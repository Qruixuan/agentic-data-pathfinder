"""Adapter letting OED consume v3alpha5 three-state safety certificates.

This does not replace the general OED controller. It is a sequential
Commit/Reveal/Stop simulation whose commit permission comes from the
workload-aware certificate instead of an interval-width heuristic, so that
the three states can be given exact operational meaning:

* ``SAFE_TO_COMMIT``      may be considered for COMMIT, subject to the normal
                          value and budget rules;
* ``UNSAFE``              must never be committed;
* ``INSUFFICIENT_EVIDENCE`` may trigger REVEAL only when another observation
                          could still reduce decision-relevant uncertainty
                          and that observation is budget-feasible.

Every non-commit path retains the safe origin design. A candidate's frozen
Oracle records are not opened before its own REVEAL action, so the
simulation cannot condition on an outcome it has not yet paid for.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from ..awm.certificate import (
    WorkloadSafetyCertificateConfig,
    certificate_cluster_observations,
    evaluate_stratum_certificate,
    load_certificate_inputs,
    load_workload_safety_certificate_config,
)
from ..awm.contracts import AWMConfigError
from ..integrations.flowmesh.pilot import _write_csv, _write_json
from ..reduced_oracle.contracts import (
    ReducedOracleConfig,
    load_reduced_oracle_config,
)
from ..reduced_oracle.snapshot import reduced_oracle_snapshot
from .contracts import OEDConfig, load_oed_config


CERTIFICATE_OED_SCHEMA_VERSION = (
    "pathfinder.oed-certificate-replay/v1alpha1"
)
CERTIFICATE_OED_POLICIES = (
    "certificate_full_oed",
    "safe_origin_baseline",
)
UNREVEALED_STATE = "UNREVEALED"
_TIER_ORDER = {
    "simultaneous-canonical": 0,
    "pair-canonical": 1,
    "fallback": 2,
}


class CertificateGateError(ValueError):
    """Raised when the certificate/OED integration is misconfigured."""


class HiddenOracleView:
    """A frozen Oracle whose candidate records stay unread until revealed.

    The safe design is always visible: it is the deployed incumbent. Every
    other design's ``runs.jsonl`` is opened only after that design has been
    revealed, and any attempt to read one earlier raises rather than quietly
    returning data the controller has not paid for.
    """

    def __init__(
        self,
        certificate_config: WorkloadSafetyCertificateConfig,
        oracle_config: ReducedOracleConfig,
        *,
        oracle_output_dir: str | Path,
    ) -> None:
        self._certificate_config = certificate_config
        self._oracle_config = oracle_config
        self._root = Path(oracle_output_dir).resolve()
        self._safe_design_id = certificate_config.safe_design_id
        self._revealed: set[str] = set()
        self._reads: list[tuple[str, ...]] = []

    @property
    def revealed_design_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._revealed))

    @property
    def read_history(self) -> tuple[tuple[str, ...], ...]:
        return tuple(self._reads)

    def visible_design_ids(self) -> tuple[str, ...]:
        return (self._safe_design_id,) + self.revealed_design_ids

    def reveal(self, design_id: str) -> None:
        if design_id == self._safe_design_id:
            raise CertificateGateError(
                "the safe design is already deployed and cannot be revealed"
            )
        if design_id in self._revealed:
            raise CertificateGateError(
                f"design already revealed: {design_id}"
            )
        self._revealed.add(design_id)

    def require_revealed(self, design_ids: Iterable[str]) -> None:
        hidden = [
            design_id
            for design_id in design_ids
            if design_id != self._safe_design_id
            and design_id not in self._revealed
        ]
        if hidden:
            raise CertificateGateError(
                "target-outcome leakage: the simulation tried to read a "
                "design before revealing it: " + ", ".join(sorted(hidden))
            )

    def inputs(self, design_ids: Iterable[str]) -> Any:
        requested = tuple(dict.fromkeys(design_ids))
        self.require_revealed(requested)
        self._reads.append(requested)
        return load_certificate_inputs(
            self._certificate_config,
            self._oracle_config,
            oracle_output_dir=self._root,
            design_ids=requested,
        )


@dataclass(frozen=True)
class StratumOutcome:
    stratum_id: str
    candidate_design_id: str
    state: str
    independent_workload_count: int
    repetition_pair_count: int
    success_difference: float | None
    success_lower_bound: float | None
    success_upper_bound: float | None
    cost_saving: float | None
    cost_lower_bound: float | None
    cost_upper_bound: float | None
    utility_gain: float | None
    success_gate: str | None
    cost_gate: str | None
    applied_design_id: str
    fallback_reason: str

    def to_row(self) -> dict[str, Any]:
        return {
            "stratum_id": self.stratum_id,
            "candidate_design_id": self.candidate_design_id,
            "certificate_state": self.state,
            "independent_workload_count": self.independent_workload_count,
            "repetition_pair_count": self.repetition_pair_count,
            "success_difference_point_estimate": self.success_difference,
            "success_difference_lower_bound": self.success_lower_bound,
            "success_difference_upper_bound": self.success_upper_bound,
            "cost_saving_point_estimate": self.cost_saving,
            "cost_saving_lower_bound": self.cost_lower_bound,
            "cost_saving_upper_bound": self.cost_upper_bound,
            "utility_gain_point_estimate": self.utility_gain,
            "success_non_inferiority_gate": self.success_gate,
            "cost_improvement_gate": self.cost_gate,
            "applied_design_id": self.applied_design_id,
            "fallback_reason": self.fallback_reason,
        }


@dataclass
class _Iteration:
    iteration: int
    action: str
    selected_design_id: str | None
    reason: str
    stopping_reason: str | None
    remaining_purse_before: float
    remaining_purse_after: float
    excursion_cost: float
    outcomes: list[StratumOutcome] = field(default_factory=list)


def _excursion_cost(
    oed_config: OEDConfig,
    design_id: str,
    transition_costs: Mapping[str, float],
) -> float:
    candidate = oed_config.reveal_candidates.get(design_id)
    probe_loss = candidate.probe_window_loss if candidate else 0.0
    return transition_costs.get(design_id, 0.0) + probe_loss


def _reveal_tier(oed_config: OEDConfig, design_id: str) -> str:
    candidate = oed_config.reveal_candidates.get(design_id)
    return candidate.reveal_tier if candidate else "fallback"


def _transition_costs(oracle_output_dir: Path) -> dict[str, float]:
    """Read design transition costs, which are not outcome observations.

    Forward and restoration transition costs are properties of the physical
    design (bytes copied, elapsed time), recorded in the Oracle table. They
    are knowable before a Reveal, so consulting them is not leakage.
    """
    path = oracle_output_dir / "oracle_table.csv"
    if not path.is_file():
        raise CertificateGateError(f"Oracle table does not exist: {path}")
    costs: dict[str, float] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            costs[row["design_id"]] = float(
                row.get("forward_transition_cost") or 0.0
            ) + float(row.get("restoration_cost") or 0.0)
    return costs


def run_oed_certificate_replay(
    oed_config: OEDConfig | str | Path,
    certificate_config: WorkloadSafetyCertificateConfig | str | Path,
    oracle_config: ReducedOracleConfig | str | Path,
    *,
    oracle_output_dir: str | Path,
    output_dir: str | Path,
    policies: tuple[str, ...] = CERTIFICATE_OED_POLICIES,
) -> dict[str, Any]:
    """Replay Commit/Reveal/Stop under v3alpha5 three-state certificates."""
    oed = (
        load_oed_config(oed_config)
        if isinstance(oed_config, (str, Path))
        else oed_config
    )
    certificate = (
        load_workload_safety_certificate_config(certificate_config)
        if isinstance(certificate_config, (str, Path))
        else certificate_config
    )
    oracle = (
        load_reduced_oracle_config(oracle_config)
        if isinstance(oracle_config, (str, Path))
        else oracle_config
    )
    source = Path(oracle_output_dir).resolve()
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise AWMConfigError(
            f"certificate OED output directory is not empty: {output}"
        )
    unknown = set(policies) - set(CERTIFICATE_OED_POLICIES)
    if unknown:
        raise CertificateGateError(
            "unknown certificate OED policy: " + ", ".join(sorted(unknown))
        )

    transition_costs = _transition_costs(source)
    runs = [
        _run_policy(
            policy,
            oed,
            certificate,
            oracle,
            oracle_output_dir=source,
            transition_costs=transition_costs,
        )
        for policy in policies
    ]

    output.mkdir(parents=True, exist_ok=True)
    trace_rows = [
        row
        for _, rows in runs
        for row in rows
    ]
    summary_rows = [summary for summary, _ in runs]
    trace_path = output / "oed_certificate_trace.csv"
    summary_path = output / "oed_certificate_summary.csv"
    evaluation_path = output / "oed_certificate_evaluation.json"
    manifest_path = output / "oed_certificate_manifest.json"
    _write_csv(trace_rows, trace_path)
    _write_csv(
        [
            {
                key: value
                for key, value in summary.items()
                if not isinstance(value, (list, dict))
            }
            for summary in summary_rows
        ],
        summary_path,
    )
    snapshot = reduced_oracle_snapshot(
        source,
        certificate.design_ids,
        scope="full-declared-design-set",
    )
    evaluation = {
        "schema_version": CERTIFICATE_OED_SCHEMA_VERSION,
        "controller_id": oed.controller_id,
        "certificate_id": certificate.certificate_id,
        "status": "COMPLETE",
        "posthoc": certificate.posthoc,
        "eligible_for_scientific_claims": (
            certificate.eligible_for_scientific_claims
        ),
        "decision_semantics": {
            "SAFE_TO_COMMIT": (
                "eligible for COMMIT subject to the value margin and the "
                "exploration budget"
            ),
            "UNSAFE": "never committed under any budget",
            "INSUFFICIENT_EVIDENCE": (
                "may trigger REVEAL only when a further observation could "
                "reduce decision-relevant uncertainty and is budget-feasible"
            ),
            "fallback": (
                "every non-COMMIT path retains "
                + certificate.safe_design_id
            ),
            "stop": (
                "certificate_limited_stop when no safe commit and no useful "
                "feasible reveal remain"
            ),
        },
        "leakage_control": (
            "a candidate's frozen Oracle records are opened only after its "
            "own REVEAL action"
        ),
        "oracle_snapshot_sha256": snapshot.sha256,
        "oracle_snapshot_algorithm": snapshot.algorithm,
        "oracle_snapshot_scope": snapshot.scope,
        "policies": summary_rows,
    }
    _write_json(evaluation, evaluation_path)
    digest = sha256()
    for path in (trace_path, summary_path, evaluation_path):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(sha256(path.read_bytes()).digest())
    manifest = {
        "schema_version": CERTIFICATE_OED_SCHEMA_VERSION,
        "controller_id": oed.controller_id,
        "certificate_id": certificate.certificate_id,
        "status": "COMPLETE",
        "posthoc": certificate.posthoc,
        "eligible_for_scientific_claims": (
            certificate.eligible_for_scientific_claims
        ),
        "deployment_mutations_performed": False,
        "secrets_recorded": False,
        "oed_config_path": str(oed.source_path),
        "oed_config_sha256": oed.source_sha256,
        "certificate_config_path": str(certificate.source_path),
        "certificate_config_sha256": certificate.source_sha256,
        "oracle_config_path": str(oracle.source_path),
        "oracle_config_sha256": oracle.source_sha256,
        "oracle_output_dir": str(source),
        **snapshot.to_manifest_fields("oracle_full_snapshot"),
        "trace_path": str(trace_path),
        "summary_path": str(summary_path),
        "evaluation_path": str(evaluation_path),
        "replay_snapshot_sha256": digest.hexdigest(),
        "policies": list(policies),
        "false_safe_commit_count": sum(
            int(summary["false_safe_commit_count"])
            for summary in summary_rows
        ),
        "fallback_design_id": certificate.safe_design_id,
    }
    _write_json(manifest, manifest_path)
    return {**manifest, "manifest_path": str(manifest_path)}


def _run_policy(
    policy_kind: str,
    oed: OEDConfig,
    certificate: WorkloadSafetyCertificateConfig,
    oracle: ReducedOracleConfig,
    *,
    oracle_output_dir: Path,
    transition_costs: Mapping[str, float],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    view = HiddenOracleView(
        certificate,
        oracle,
        oracle_output_dir=oracle_output_dir,
    )
    safe_design_id = certificate.safe_design_id
    candidates = tuple(dict.fromkeys(
        stratum.candidate_design_id for stratum in certificate.strata
    ))
    remaining = oed.exploration_budget
    committed_design_id = safe_design_id
    committed_stratum_id: str | None = None
    iterations: list[_Iteration] = []
    reveal_order: list[str] = []
    terminal_reason: str | None = None
    exhausted: set[str] = set()

    for iteration in range(oed.max_iterations):
        outcomes = _stratum_outcomes(view, certificate, candidates)
        record = _Iteration(
            iteration=iteration,
            action="STOP",
            selected_design_id=None,
            reason="",
            stopping_reason=None,
            remaining_purse_before=remaining,
            remaining_purse_after=remaining,
            excursion_cost=0.0,
            outcomes=list(outcomes),
        )

        committable = [
            outcome
            for outcome in outcomes
            if outcome.state == "SAFE_TO_COMMIT"
            and _clears_value_margin(outcome, oed, transition_costs)
        ]
        if policy_kind == "safe_origin_baseline":
            record.action = "STOP"
            record.reason = "baseline_never_reveals_or_commits"
            record.stopping_reason = "policy_limited_stop"
            iterations.append(record)
            terminal_reason = record.stopping_reason
            break

        if committable:
            selected = max(
                committable,
                key=lambda outcome: (
                    outcome.utility_gain or 0.0,
                    outcome.stratum_id,
                ),
            )
            record.action = "COMMIT"
            record.selected_design_id = selected.candidate_design_id
            record.reason = (
                "certificate_safe_to_commit_and_clears_value_margin"
            )
            committed_design_id = selected.candidate_design_id
            committed_stratum_id = selected.stratum_id
            iterations.append(record)
            terminal_reason = "commit_completed"
            break

        useful = _useful_reveals(
            certificate,
            candidates,
            revealed=set(view.revealed_design_ids),
            exhausted=exhausted,
        )
        if not useful:
            record.action = "STOP"
            record.reason = (
                "no_safe_commit_and_no_further_decision_relevant_observation"
            )
            record.stopping_reason = "certificate_limited_stop"
            iterations.append(record)
            terminal_reason = record.stopping_reason
            break

        affordable = [
            design_id
            for design_id in useful
            if _excursion_cost(oed, design_id, transition_costs)
            <= min(oed.per_excursion_cap, remaining)
        ]
        if not affordable:
            record.action = "STOP"
            record.reason = (
                "a decision-relevant reveal remains but exceeds the "
                "excursion cap or the exploration purse"
            )
            record.stopping_reason = "budget_limited_stop"
            iterations.append(record)
            terminal_reason = record.stopping_reason
            break

        # The ordering key is built only from configuration and physical
        # design costs, never from an unrevealed outcome.
        selected_design_id = min(
            affordable,
            key=lambda design_id: (
                _TIER_ORDER[_reveal_tier(oed, design_id)],
                -_stratum_span(certificate, design_id),
                _excursion_cost(oed, design_id, transition_costs),
                design_id,
            ),
        )
        cost = _excursion_cost(oed, selected_design_id, transition_costs)
        record.action = "REVEAL"
        record.selected_design_id = selected_design_id
        record.reason = (
            "insufficient_evidence_and_a_budget_feasible_observation_can "
            "_still_reduce_decision_relevant_uncertainty"
        )
        record.excursion_cost = cost
        remaining -= cost
        record.remaining_purse_after = remaining
        iterations.append(record)
        view.reveal(selected_design_id)
        reveal_order.append(selected_design_id)
        # The frozen Oracle holds exactly one observation block per design,
        # so a revealed design offers no further decision-relevant draw.
        exhausted.add(selected_design_id)
    else:
        terminal_reason = "maximum_iterations_reached"

    final_outcomes = _stratum_outcomes(view, certificate, candidates)
    false_safe = [
        outcome
        for outcome in final_outcomes
        if outcome.state == "UNSAFE"
        and outcome.candidate_design_id == committed_design_id
    ]
    rows: list[dict[str, Any]] = []
    for record in iterations:
        for outcome in record.outcomes:
            rows.append({
                "policy_kind": policy_kind,
                "controller_id": oed.controller_id,
                "iteration": record.iteration,
                "selected_action": record.action,
                "selected_design_id": record.selected_design_id,
                "action_reason": record.reason,
                "stopping_reason": record.stopping_reason,
                "revealed_design_ids": "+".join(reveal_order) or "",
                "remaining_exploration_purse_before": (
                    record.remaining_purse_before
                ),
                "remaining_exploration_purse_after": (
                    record.remaining_purse_after
                ),
                "excursion_cost": record.excursion_cost,
                **outcome.to_row(),
            })
    summary = {
        "policy_kind": policy_kind,
        "controller_id": oed.controller_id,
        "starting_safe_design_id": safe_design_id,
        "final_design_id": committed_design_id,
        "committed_stratum_id": committed_stratum_id,
        "fallback_design_id": safe_design_id,
        "fallback_applied": committed_design_id == safe_design_id,
        "terminal_reason": terminal_reason,
        "iterations": len(iterations),
        "reveal_count": sum(
            record.action == "REVEAL" for record in iterations
        ),
        "commit_count": sum(
            record.action == "COMMIT" for record in iterations
        ),
        "reveal_order": list(reveal_order),
        "total_reveal_cost": sum(
            record.excursion_cost for record in iterations
        ),
        "remaining_exploration_purse": remaining,
        "exploration_budget": oed.exploration_budget,
        "per_excursion_cap": oed.per_excursion_cap,
        "false_safe_commit_count": len(false_safe),
        "final_state_by_stratum": {
            outcome.stratum_id: outcome.state
            for outcome in final_outcomes
        },
        "unsafe_stratum_count": sum(
            outcome.state == "UNSAFE" for outcome in final_outcomes
        ),
        "safe_to_commit_stratum_count": sum(
            outcome.state == "SAFE_TO_COMMIT"
            for outcome in final_outcomes
        ),
        "insufficient_evidence_stratum_count": sum(
            outcome.state == "INSUFFICIENT_EVIDENCE"
            for outcome in final_outcomes
        ),
        "unrevealed_stratum_count": sum(
            outcome.state == UNREVEALED_STATE
            for outcome in final_outcomes
        ),
        "design_read_history": [
            "+".join(read) for read in view.read_history
        ],
    }
    return summary, rows


def _stratum_span(
    certificate: WorkloadSafetyCertificateConfig,
    design_id: str,
) -> int:
    return sum(
        1
        for stratum in certificate.strata
        if stratum.candidate_design_id == design_id
    )


def _useful_reveals(
    certificate: WorkloadSafetyCertificateConfig,
    candidates: tuple[str, ...],
    *,
    revealed: set[str],
    exhausted: set[str],
) -> tuple[str, ...]:
    """Return candidates whose reveal could still change a decision."""
    return tuple(
        design_id
        for design_id in candidates
        if design_id not in revealed
        and design_id not in exhausted
        and _stratum_span(certificate, design_id) > 0
    )


def _clears_value_margin(
    outcome: StratumOutcome,
    oed: OEDConfig,
    transition_costs: Mapping[str, float],
) -> bool:
    gain = outcome.utility_gain or 0.0
    net = gain - transition_costs.get(outcome.candidate_design_id, 0.0)
    return net > oed.commit_margin


def _stratum_outcomes(
    view: HiddenOracleView,
    certificate: WorkloadSafetyCertificateConfig,
    candidates: tuple[str, ...],
) -> tuple[StratumOutcome, ...]:
    revealed = set(view.revealed_design_ids)
    visible = tuple(
        design_id for design_id in candidates if design_id in revealed
    )
    inputs = (
        view.inputs((certificate.safe_design_id,) + visible)
        if visible
        else None
    )
    family_size = len(certificate.strata) * 4
    adjusted_alpha = certificate.alpha / family_size
    outcomes: list[StratumOutcome] = []
    for stratum in certificate.strata:
        if stratum.candidate_design_id not in revealed or inputs is None:
            outcomes.append(StratumOutcome(
                stratum_id=stratum.stratum_id,
                candidate_design_id=stratum.candidate_design_id,
                state=UNREVEALED_STATE,
                independent_workload_count=len(stratum.workload_ids),
                repetition_pair_count=0,
                success_difference=None,
                success_lower_bound=None,
                success_upper_bound=None,
                cost_saving=None,
                cost_lower_bound=None,
                cost_upper_bound=None,
                utility_gain=None,
                success_gate=None,
                cost_gate=None,
                applied_design_id=certificate.safe_design_id,
                fallback_reason="candidate_not_yet_revealed",
            ))
            continue
        observations = certificate_cluster_observations(
            inputs,
            safe_design_id=certificate.safe_design_id,
            candidate_design_id=stratum.candidate_design_id,
            workload_ids=stratum.workload_ids,
        )
        result = evaluate_stratum_certificate(
            observations,
            success_difference_support=(
                certificate.success_difference_support
            ),
            cost_saving_support=certificate.cost_saving_support,
            utility_support=inputs.utility_support,
            adjusted_alpha=adjusted_alpha,
            delta_success_margin=certificate.delta_success_margin,
            minimum_cost_saving=certificate.minimum_cost_saving,
            minimum_independent_workloads=(
                certificate.minimum_independent_workloads
            ),
            safe_design_id=certificate.safe_design_id,
            candidate_design_id=stratum.candidate_design_id,
        )
        success_gate, cost_gate = result.gates
        outcomes.append(StratumOutcome(
            stratum_id=stratum.stratum_id,
            candidate_design_id=stratum.candidate_design_id,
            state=result.certificate_state,
            independent_workload_count=result.independent_workload_count,
            repetition_pair_count=(
                result.independent_workload_count * len(inputs.repetitions)
            ),
            success_difference=result.success_bound.point_estimate,
            success_lower_bound=result.success_bound.lower_bound,
            success_upper_bound=result.success_bound.upper_bound,
            cost_saving=result.cost_bound.point_estimate,
            cost_lower_bound=result.cost_bound.lower_bound,
            cost_upper_bound=result.cost_bound.upper_bound,
            utility_gain=result.utility_point_estimate,
            success_gate=str(success_gate["result"]),
            cost_gate=str(cost_gate["result"]),
            applied_design_id=result.applied_design_id,
            fallback_reason=str(result.decision["fallback_reason"]),
        ))
    return tuple(outcomes)
