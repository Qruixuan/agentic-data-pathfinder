"""Offline OED planning for a fresh distributed-policy confirmation cohort.

This is a *prospective design planner*, not an online adaptive experiment and
not a commit engine. It answers one question: given the post-hoc pilot as
planning data and a collection budget, how many additional independent
workloads should each stratum get so that the eventual confirmation run has
the best chance of producing a decisive certificate?

Two properties keep it honest.

**The action unit is one independent workload block**, never a repetition and
never a session. Repetitions are averaged inside a workload before inference,
so buying more of them buys precision within a cluster, not more clusters.
Costing actions in sessions while counting evidence in workloads is how a
plan comes to promise power it cannot deliver.

**Allocation is fixed before outcomes exist.** Every projection uses the
pilot's plug-in point estimate held constant while only the sample size
grows. That is a planning quantity, not achieved power and not a confidence
guarantee -- the true effect is unknown and the pilot's estimate is exactly
the quantity the confirmation is supposed to test. Outcome-adaptive sampling
would need an anytime-valid or alpha-spending contract, which this version
deliberately does not implement.

Greedy rule
-----------
At each step, for every *active* stratum ``s`` holding ``n_s`` independent
workloads::

    width(n)  = normalised upper bound(n) - normalised lower bound(n)
                for the widest of the two certificate gates
    gain(s)   = width(n_s) - width(n_s + 1)
    score(s)  = gain(s) / paired_block_cost(s)

The stratum with the highest ``score`` wins; ties break by ``stratum_id``
ascending, which makes the allocation deterministic. Structural-safe strata
are never selected: their policy effect is zero by construction, so no
measurement there can narrow a gate.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Mapping

from ..awm.model import Interval, one_sided_bounded_mean_bounds
from .weighted_certificate import (
    StratumEvidence,
    distance_to_threshold,
    evaluate_weighted_policy_certificate,
)
from .confirmation import (
    ESTIMAND_KIND,
    WEIGHTS_PROVENANCE,
    ConfirmationPlanError,
    SAFE_DESIGN_ID,
    _csv_text,
    _require,
    verify_audit_snapshot,
)


OED_PLAN_SCHEMA_VERSION = (
    "pathfinder.distributed-policy-oed-plan/v1alpha1"
)
#: Planning actions this version may emit. COMMIT is deliberately absent:
#: no allocation of future work can be authorised by post-hoc evidence.
PLANNING_ACTIONS = (
    "COLLECT",
    "STOP_INSUFFICIENT_BUDGET",
    "FREEZE_CONFIRMATION_PLAN",
)
GATE_IDS = ("success_non_inferiority", "cost_improvement")


def _normalise(value: float, lower: float, upper: float) -> float:
    return (value - lower) / (upper - lower)


@dataclass
class StratumState:
    """Planning state for one stratum of the selected policy."""

    stratum_id: str
    role: str
    policy_design_id: str
    success_point_estimate: float
    cost_point_estimate: float
    success_support: tuple[float, float]
    cost_support: tuple[float, float]
    pilot_workloads: int
    allocated_workloads: int = 0
    weight: float = 0.0
    repetitions: int = 2
    _width_cache: dict[tuple[int, float], float] = field(
        default_factory=dict,
        repr=False,
    )

    @property
    def is_active(self) -> bool:
        return self.role == "active"

    @property
    def independent_workloads(self) -> int:
        """Independent clusters the *confirmation* would hold.

        Only freshly allocated blocks count. The pilot's workloads selected
        the policy and are therefore unusable as its confirmation evidence,
        so projecting precision as though they carried over would overstate
        what the fresh cohort can establish.
        """
        return self.allocated_workloads

    @property
    def paired_block_cost(self) -> int:
        """Sessions in one additional independent workload block.

        An active block buys both arms at every repetition. A structural-safe
        stratum has no candidate arm to buy.
        """
        arms = 2 if self.is_active else 1
        return arms * self.repetitions

    def gate_width(self, independent_units: int, delta: float) -> float:
        """Worst normalised gate width at a hypothetical sample size."""
        if independent_units < 1:
            return 1.0
        cached = self._width_cache.get((independent_units, delta))
        if cached is not None:
            return cached
        widths = []
        for point, (lower, upper) in (
            (self.success_point_estimate, self.success_support),
            (self.cost_point_estimate, self.cost_support),
        ):
            normalised = min(1.0, max(0.0, _normalise(point, lower, upper)))
            bounds = one_sided_bounded_mean_bounds(
                normalised,
                independent_units,
                delta,
            )
            widths.append(bounds.upper - bounds.lower)
        width = max(widths)
        self._width_cache[(independent_units, delta)] = width
        return width

    def projected_gain(self, delta: float) -> float:
        """Reduction in worst normalised gate width from one more block."""
        if not self.is_active:
            # A structurally zero effect cannot be measured more precisely.
            return 0.0
        current = self.gate_width(self.independent_workloads, delta)
        after = self.gate_width(self.independent_workloads + 1, delta)
        return max(0.0, current - after)


@dataclass
class PlanningBudget:
    """Budget in *active evidence blocks*, not benchmark cohort workloads.

    An active evidence block is one paired safe/candidate workload block in
    an active stratum. Structural-safe strata consume none, so a budget of N
    blocks is N paired comparisons -- it is emphatically not an N-workload
    benchmark cohort with the target stratum mixture.
    """

    active_evidence_blocks: int | None = None
    total_sessions: int | None = None
    consumed_blocks: int = 0
    consumed_sessions: int = 0
    allocations: list[dict[str, Any]] = field(default_factory=list)

    def can_afford(self, session_cost: int) -> bool:
        if (
            self.active_evidence_blocks is not None
            and self.consumed_blocks + 1 > self.active_evidence_blocks
        ):
            return False
        if (
            self.total_sessions is not None
            and self.consumed_sessions + session_cost > self.total_sessions
        ):
            return False
        return True

    def spend(self, stratum_id: str, session_cost: int) -> None:
        self.consumed_blocks += 1
        self.consumed_sessions += session_cost
        self.allocations.append({
            "action": "COLLECT",
            "stratum_id": stratum_id,
            "independent_workload_blocks": 1,
            "sessions": session_cost,
        })


def _stratum_states(
    source: Path,
    policy_id: str,
    *,
    weights: Mapping[str, float],
    repetitions: int,
    success_support: tuple[float, float],
    cost_support: tuple[float, float],
) -> dict[str, StratumState]:
    """Derive per-stratum planning state from the frozen policy audit."""
    rows_by_stratum: dict[str, list[dict[str, str]]] = {}
    path = source / "policy_workload_effects.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("policy_id") != policy_id:
                continue
            rows_by_stratum.setdefault(
                str(row["stratum_id"]), []
            ).append(row)
    _require(
        rows_by_stratum,
        f"the policy audit has no workload effects for {policy_id!r}",
    )
    states: dict[str, StratumState] = {}
    for stratum_id, rows in sorted(rows_by_stratum.items()):
        designs = {str(row["selected_design_id"]) for row in rows}
        _require(
            len(designs) == 1,
            f"stratum {stratum_id} maps to multiple designs: {designs}",
        )
        design = designs.pop()
        role = "structural_safe" if design == SAFE_DESIGN_ID else "active"
        successes = [
            float(row["task_success_delta_vs_safe"]) for row in rows
        ]
        savings = [float(row["cost_saving_vs_safe"]) for row in rows]
        if role == "structural_safe":
            # Zero by construction, not by measurement. Asserted rather
            # than averaged so a non-zero value in the source is a refusal.
            _require(
                all(value == 0.0 for value in successes)
                and all(value == 0.0 for value in savings),
                f"stratum {stratum_id} applies the safe design but reports "
                "a non-zero effect; that is not a structural zero",
            )
        states[stratum_id] = StratumState(
            stratum_id=stratum_id,
            role=role,
            policy_design_id=design,
            success_point_estimate=(
                sum(successes) / len(successes) if successes else 0.0
            ),
            cost_point_estimate=(
                sum(savings) / len(savings) if savings else 0.0
            ),
            success_support=success_support,
            cost_support=cost_support,
            pilot_workloads=len(rows),
            weight=float(weights.get(stratum_id, 0.0)),
            repetitions=repetitions,
        )
    return states


def plan_distributed_policy_oed(
    policy_audit_dir: str | Path,
    *,
    policy_id: str,
    stratum_weights: Mapping[str, float],
    output_dir: str | Path,
    repetitions: int = 2,
    active_evidence_block_budget: int | None = None,
    total_sessions: int | None = None,
    alpha: float = 0.05,
    delta_success_margin: float = 0.05,
    minimum_cost_saving: float = 0.25,
    target_gate_width: float | None = None,
    minimum_independent_workloads_by_active_stratum: (
        Mapping[str, int] | None
    ) = None,
    plan_id: str = "distributed-policy-oed-plan",
) -> dict[str, Any]:
    """Allocate future independent workload blocks, deterministically."""
    source = Path(policy_audit_dir).resolve()
    _require(
        source.is_dir(),
        f"policy audit directory does not exist: {source}",
    )
    audit_hashes = verify_audit_snapshot(source)
    evaluation = json.loads(
        (source / "policy_evaluation.json").read_bytes()
    )
    _require(
        evaluation.get("posthoc") is True
        and evaluation.get("eligible_for_scientific_claims") is False,
        "OED planning requires a post-hoc, non-eligible source audit",
    )
    _require(
        active_evidence_block_budget is not None
        or total_sessions is not None,
        "an active-evidence-block or session budget is required",
    )
    for stratum_id, value in stratum_weights.items():
        _require(
            isinstance(value, int) and not isinstance(value, bool),
            f"target weight for {stratum_id} must be an integer quota, not "
            "a rounded fraction",
        )
        _require(int(value) >= 0, f"target weight for {stratum_id} < 0")
    weight_total = sum(int(value) for value in stratum_weights.values())
    _require(
        weight_total > 0,
        "target stratum weights must have a positive total; without "
        "declared weights an allocation would silently change the estimand",
    )
    normalized_weights = {
        stratum_id: int(value) / weight_total
        for stratum_id, value in sorted(stratum_weights.items())
    }

    states = _stratum_states(
        source,
        policy_id,
        weights=normalized_weights,
        repetitions=repetitions,
        success_support=(-1.0, 1.0),
        cost_support=(-2.0, 2.0),
    )
    _require(
        set(states) == set(stratum_weights),
        "declared weights must cover exactly the policy's strata: "
        f"{sorted(states)}",
    )
    active = [state for state in states.values() if state.is_active]
    _require(
        active,
        "every stratum is structurally safe; there is nothing to plan",
    )

    # One family: two gates, two tails, over the active strata.
    family_size = len(active) * len(GATE_IDS) * 2
    adjusted_alpha = alpha / family_size

    minima = {
        state.stratum_id: int(
            (minimum_independent_workloads_by_active_stratum or {}).get(
                state.stratum_id,
                1,
            )
        )
        for state in active
    }
    for stratum_id, value in sorted(minima.items()):
        _require(
            value >= 1,
            f"minimum for {stratum_id} must be a positive integer",
        )

    budget = PlanningBudget(
        active_evidence_blocks=active_evidence_block_budget,
        total_sessions=total_sessions,
    )
    initial_widths = {
        state.stratum_id: state.gate_width(
            state.independent_workloads,
            adjusted_alpha,
        )
        for state in active
    }
    stop_reason = None
    steps: list[dict[str, Any]] = []
    # Feasibility before precision: a stratum below its frozen floor cannot
    # produce an admissible certificate, so optimising elsewhere first would
    # refine a quantity that is not yet usable.
    for state in sorted(active, key=lambda item: item.stratum_id):
        while (
            state.allocated_workloads < minima[state.stratum_id]
            and budget.can_afford(state.paired_block_cost)
        ):
            budget.spend(state.stratum_id, state.paired_block_cost)
            state.allocated_workloads += 1
            steps.append({
                "step": len(steps) + 1,
                "action": "COLLECT",
                "stratum_id": state.stratum_id,
                "independent_workload_blocks": 1,
                "sessions": state.paired_block_cost,
                "purpose": "meet_frozen_minimum_independent_workloads",
                "worst_gate_width_before": None,
                "worst_gate_width_after": state.gate_width(
                    state.independent_workloads,
                    adjusted_alpha,
                ),
                "score_per_session": None,
            })
    unmet = sorted(
        state.stratum_id for state in active
        if state.allocated_workloads < minima[state.stratum_id]
    )
    while True:
        if target_gate_width is not None and all(
            state.gate_width(state.independent_workloads, adjusted_alpha)
            <= target_gate_width
            for state in active
        ):
            stop_reason = "target_gate_width_reached"
            break
        affordable = [
            state for state in active
            if budget.can_afford(state.paired_block_cost)
        ]
        if not affordable:
            stop_reason = "insufficient_budget"
            break
        scored = [
            (
                state.projected_gain(adjusted_alpha)
                / state.paired_block_cost,
                state,
            )
            for state in affordable
        ]
        best_score = max(score for score, _ in scored)
        if best_score <= 0.0:
            stop_reason = "no_further_uncertainty_reduction_available"
            break
        # Deterministic: highest score, then lowest stratum_id.
        chosen = min(
            (state for score, state in scored if score == best_score),
            key=lambda state: state.stratum_id,
        )
        before = chosen.gate_width(
            chosen.independent_workloads,
            adjusted_alpha,
        )
        budget.spend(chosen.stratum_id, chosen.paired_block_cost)
        chosen.allocated_workloads += 1
        steps.append({
            "step": len(steps) + 1,
            "action": "COLLECT",
            "stratum_id": chosen.stratum_id,
            "independent_workload_blocks": 1,
            "sessions": chosen.paired_block_cost,
            "worst_gate_width_before": before,
            "worst_gate_width_after": chosen.gate_width(
                chosen.independent_workloads,
                adjusted_alpha,
            ),
            "score_per_session": best_score,
        })

    final_action = (
        "STOP_INSUFFICIENT_BUDGET"
        if stop_reason == "insufficient_budget" and not steps
        else "FREEZE_CONFIRMATION_PLAN"
    )
    feasibility = _feasibility_search(
        states,
        stratum_weights=stratum_weights,
        alpha=alpha,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        minima=minima,
        policy_id=policy_id,
    )
    projection = _weighted_projection(
        states,
        stratum_weights=stratum_weights,
        alpha=alpha,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        minima=minima,
        policy_id=policy_id,
    )
    margin_warnings = _margin_warnings(
        active,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        adjusted_alpha=adjusted_alpha,
    )
    plan = {
        "schema_version": OED_PLAN_SCHEMA_VERSION,
        "plan_id": plan_id,
        "policy_id": policy_id,
        "final_action": final_action,
        "stop_reason": stop_reason,
        "planning_actions_available": list(PLANNING_ACTIONS),
        "commit_authorised": False,
        "action_unit": "one-additional-independent-workload-block",
        "repetitions_per_workload": repetitions,
        "repetitions_increase_independent_units": False,
        "alpha": alpha,
        "family_size": family_size,
        "adjusted_alpha": adjusted_alpha,
        "estimand": {
            "kind": ESTIMAND_KIND,
            "target_stratum_weights_integer": dict(sorted(
                (key, int(value))
                for key, value in stratum_weights.items()
            )),
            "target_stratum_weights_normalized": normalized_weights,
            "target_stratum_weight_total": weight_total,
            "weights_provenance": WEIGHTS_PROVENANCE,
            "aggregation_rule": (
                "The later certificate must aggregate stratum effects using "
                "these frozen weights, NOT the empirical proportions of the "
                "collected sample."
            ),
        },
        "target_stratum_weights": normalized_weights,
        "budget": {
            "active_evidence_block_budget": active_evidence_block_budget,
            "total_sessions": total_sessions,
            "consumed_active_evidence_blocks": budget.consumed_blocks,
            "consumed_sessions": budget.consumed_sessions,
            "unit_note": (
                "The budget counts paired active evidence blocks, not "
                "benchmark cohort workloads. Structural-safe strata consume "
                "no active evidence blocks, so this budget is NOT an "
                "N-workload cohort with the target stratum mixture."
            ),
        },
        "active_evidence_blocks_by_stratum": {
            state.stratum_id: state.allocated_workloads
            for state in sorted(
                states.values(), key=lambda item: item.stratum_id
            )
            if state.is_active
        },
        "structural_zero_strata": [
            state.stratum_id
            for state in sorted(
                states.values(), key=lambda item: item.stratum_id
            )
            if not state.is_active
        ],
        "structural_zero_workloads_do_not_require_candidate_execution": (
            True
        ),
        "planned_safe_sessions_by_stratum": {
            state.stratum_id: (
                state.allocated_workloads * repetitions
            )
            for state in sorted(
                states.values(), key=lambda item: item.stratum_id
            )
        },
        "planned_candidate_sessions_by_stratum": {
            state.stratum_id: (
                state.allocated_workloads * repetitions
                if state.is_active
                else 0
            )
            for state in sorted(
                states.values(), key=lambda item: item.stratum_id
            )
        },
        "planned_total_sessions": budget.consumed_sessions,
        "repetitions_per_active_block": repetitions,
        "block_cost_formula": (
            "active block cost = 2 arms (safe + candidate) x repetitions; "
            "structural-safe block cost = 1 arm x repetitions, and no "
            "candidate execution is scheduled"
        ),
        "collection_allocation_matches_target_weights": _matches_target(
            {
                state.stratum_id: state.allocated_workloads
                for state in states.values()
            },
            normalized_weights,
        ),
        "collection_allocation_note": (
            "Allocation is chosen for precision and intentionally differs "
            "from the target weights: structural-safe strata receive zero "
            "active blocks because their effect is zero by construction. "
            "The estimand is unaffected because the frozen weights, not the "
            "sample proportions, define it."
        ),
        "allocation_by_stratum": {
            state.stratum_id: {
                "role": state.role,
                "policy_design_id": state.policy_design_id,
                "pilot_workloads": state.pilot_workloads,
                "additional_independent_workloads": (
                    state.allocated_workloads
                ),
                "sessions_per_block": state.paired_block_cost,
                "candidate_measurement_planned": state.is_active,
                "structural_zero_effect": not state.is_active,
                "projected_worst_gate_width": state.gate_width(
                    state.independent_workloads,
                    adjusted_alpha,
                ) if state.is_active else 0.0,
                "initial_worst_gate_width": initial_widths.get(
                    state.stratum_id
                ),
            }
            for state in sorted(
                states.values(),
                key=lambda item: item.stratum_id,
            )
        },
        "steps": steps,
        "minimum_independent_workloads_by_active_stratum": dict(
            sorted(minima.items())
        ),
        "unmet_minimum_strata": unmet,
        "minimum_requirements_satisfied": not unmet,
        "feasibility": feasibility,
        "projected_weighted_certificate": projection,
        "margin_warnings": margin_warnings,
        "greedy_rule": (
            "score(s) = [width(n_s) - width(n_s+1)] / paired_block_cost(s), "
            "where width is the wider of the two normalised certificate "
            "gates. Highest score wins; ties break by stratum_id ascending."
        ),
        "projection_semantics": (
            "Every projected width is a plug-in planning quantity: the "
            "pilot's post-hoc point estimate is held fixed while only the "
            "independent sample size grows. It is NOT achieved power, NOT a "
            "confidence guarantee, and NOT a prediction of the confirmation "
            "result. The true effect is unknown and is precisely what the "
            "confirmation would test."
        ),
        "posthoc_planning_data": True,
        "eligible_for_scientific_claims": False,
        "offline": True,
        "credentials_recorded": False,
        "source_policy_audit": {
            "audit_id": evaluation.get("audit_id"),
            "file_sha256": dict(audit_hashes),
        },
        "limitations": [
            "Projected widths are per-stratum plug-in proxies for the "
            "pooled policy certificate, not the pooled bound itself.",
            "Structural-safe strata are never allocated: a structurally "
            "zero effect cannot be measured more precisely.",
            "Allocation is fixed before outcomes exist; this planner "
            "implements no anytime-valid or alpha-spending contract, so it "
            "must not be used adaptively during a run.",
            "The plug-in projection holds each future stratum mean at its "
            "post-hoc point estimate. The bounded-KL interval is determined "
            "by the declared support, independent workload count, allocated "
            "alpha, and assumed mean; it does not estimate or assume an "
            "empirical within-stratum variance. A fresh cohort may shift "
            "the stratum means and therefore may produce either better or "
            "worse certificate bounds.",
        ],
    }
    allocated_total = sum(
        plan["active_evidence_blocks_by_stratum"].values()
    )
    _require(
        allocated_total == budget.consumed_blocks,
        "internal accounting error: allocated blocks do not equal consumed",
    )
    expected_sessions = sum(
        count * 2 * repetitions
        for count in plan["active_evidence_blocks_by_stratum"].values()
    )
    _require(
        plan["planned_total_sessions"] == expected_sessions,
        "internal accounting error: session total does not equal "
        "blocks x 2 arms x repetitions",
    )
    return _publish_plan(plan, output_dir=output_dir, source=source)


def _weighted_projection(
    states: Mapping[str, StratumState],
    *,
    stratum_weights: Mapping[str, int],
    alpha: float,
    delta_success_margin: float,
    minimum_cost_saving: float,
    minima: Mapping[str, int],
    policy_id: str,
) -> dict[str, Any]:
    """Project the weighted certificate under the chosen allocation.

    Uses the same aggregation core the real certificate will use, fed with
    the pilot's point estimates repeated across the projected workload
    count.

    The plug-in projection holds each future stratum mean at its post-hoc
    point estimate. The bounded-KL interval is determined by the declared
    support, independent workload count, allocated alpha, and assumed mean;
    it does not estimate or assume an empirical within-stratum variance. A
    fresh cohort may shift the stratum means and therefore may produce
    either better or worse certificate bounds.
    """
    empty = sorted(
        state.stratum_id
        for state in states.values()
        if state.is_active and state.independent_workloads < 1
    )
    if empty:
        # No fresh clusters yet in at least one active stratum, so there is
        # nothing to project. Reported rather than raised: an unaffordable
        # budget is a planning answer, not an error.
        return {
            "projection_class": "posthoc-plugin-planning-projection",
            "not_achieved_power": True,
            "not_a_confidence_guarantee": True,
            "not_a_commit_authorization": True,
            "available": False,
            "unavailable_reason": (
                "no fresh independent workloads allocated to: "
                + ", ".join(empty)
            ),
            "uses_same_weighted_aggregation_core": True,
        }
    evidence: list[StratumEvidence] = []
    for stratum_id in sorted(states):
        state = states[stratum_id]
        weight = int(stratum_weights[stratum_id])
        if not state.is_active:
            evidence.append(StratumEvidence(
                stratum_id=stratum_id,
                role="structural_safe",
                integer_weight=weight,
            ))
            continue
        count = state.independent_workloads
        evidence.append(StratumEvidence(
            stratum_id=stratum_id,
            role="active",
            integer_weight=weight,
            success_differences=(state.success_point_estimate,) * count,
            cost_savings=(state.cost_point_estimate,) * count,
        ))
    certificate = evaluate_weighted_policy_certificate(
        evidence,
        success_difference_support=Interval(-1.0, 1.0),
        cost_saving_support=Interval(-2.0, 2.0),
        alpha=alpha,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        minimum_independent_workloads_by_stratum=minima,
        safe_design_id=SAFE_DESIGN_ID,
        policy_id=policy_id,
    )
    return {
        "projection_class": "posthoc-plugin-planning-projection",
        "not_achieved_power": True,
        "not_a_confidence_guarantee": True,
        "not_a_commit_authorization": True,
        "available": True,
        "assumed_stratum_point_estimates": {
            stratum_id: {
                "success_difference": states[stratum_id]
                .success_point_estimate,
                "cost_saving": states[stratum_id].cost_point_estimate,
            }
            for stratum_id in sorted(states)
        },
        "projected_independent_workloads": {
            stratum_id: states[stratum_id].independent_workloads
            if states[stratum_id].is_active
            else 0
            for stratum_id in sorted(states)
        },
        "projected_stratum_bounds": {
            item["stratum_id"]: {
                "success_difference": item["success_difference"],
                "cost_saving": item["cost_saving"],
                "structural_zero_effect": item["structural_zero_effect"],
            }
            for item in certificate["strata"]
        },
        "projected_overall_success_difference": dict(
            certificate["overall_success_difference"]
        ),
        "projected_overall_cost_saving": dict(
            certificate["overall_cost_saving"]
        ),
        "projected_gate_results": {
            gate["gate_id"]: gate["result"]
            for gate in certificate["gates"]
        },
        "projected_certificate_state": certificate["certificate_state"],
        "projected_distance_to_threshold": distance_to_threshold(
            certificate
        ),
        "family_size": certificate["confidence"]["family_size"],
        "adjusted_alpha": certificate["confidence"]["adjusted_alpha"],
        "uses_same_weighted_aggregation_core": True,
    }


FEASIBILITY_CLASSES = (
    "POINT_ESTIMATE_BELOW_THRESHOLD",
    "PROJECTED_NOT_WITHIN_SEARCH_BUDGET",
    "PROJECTED_PASS_WITHIN_SEARCH_BUDGET",
)
#: Deterministic bounded ladder. The search never runs unbounded: an
#: arbitrarily large "passing" cohort is not a useful planning answer.
DEFAULT_SEARCH_LADDER = (
    50, 100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600,
)


def _allocate(
    states: Mapping[str, StratumState],
    blocks: int,
    adjusted_alpha: float,
    minima: Mapping[str, int],
) -> dict[str, int]:
    """Deterministic allocation of ``blocks`` active evidence blocks.

    Frozen per-stratum minima are satisfied first, as feasibility
    constraints: a stratum below its floor cannot contribute a usable
    certificate at all, so buying precision elsewhere first would be
    optimising a quantity that is not yet admissible.
    """
    active = sorted(
        (state for state in states.values() if state.is_active),
        key=lambda item: item.stratum_id,
    )
    allocation = {state.stratum_id: 0 for state in active}
    remaining = blocks
    for state in active:
        need = max(0, int(minima.get(state.stratum_id, 1)))
        take = min(need, remaining)
        allocation[state.stratum_id] += take
        remaining -= take
        if remaining <= 0:
            break
    while remaining > 0:
        scored = []
        for state in active:
            count = allocation[state.stratum_id]
            gain = max(
                0.0,
                state.gate_width(count, adjusted_alpha)
                - state.gate_width(count + 1, adjusted_alpha),
            )
            scored.append((gain / state.paired_block_cost, state))
        best = max(score for score, _ in scored)
        if best <= 0.0:
            break
        chosen = min(
            (state for score, state in scored if score == best),
            key=lambda item: item.stratum_id,
        )
        allocation[chosen.stratum_id] += 1
        remaining -= 1
    return allocation


def _certificate_at(
    states: Mapping[str, StratumState],
    allocation: Mapping[str, int],
    *,
    stratum_weights: Mapping[str, int],
    alpha: float,
    delta_success_margin: float,
    minimum_cost_saving: float,
    minima: Mapping[str, int],
    policy_id: str,
) -> dict[str, Any]:
    evidence: list[StratumEvidence] = []
    for stratum_id in sorted(states):
        state = states[stratum_id]
        weight = int(stratum_weights[stratum_id])
        if not state.is_active:
            evidence.append(StratumEvidence(
                stratum_id=stratum_id,
                role="structural_safe",
                integer_weight=weight,
            ))
            continue
        count = int(allocation.get(stratum_id, 0))
        evidence.append(StratumEvidence(
            stratum_id=stratum_id,
            role="active",
            integer_weight=weight,
            success_differences=(state.success_point_estimate,) * count,
            cost_savings=(state.cost_point_estimate,) * count,
        ))
    return evaluate_weighted_policy_certificate(
        evidence,
        success_difference_support=Interval(-1.0, 1.0),
        cost_saving_support=Interval(-2.0, 2.0),
        alpha=alpha,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        minimum_independent_workloads_by_stratum=minima,
        safe_design_id=SAFE_DESIGN_ID,
        policy_id=policy_id,
    )


def _feasibility_search(
    states: Mapping[str, StratumState],
    *,
    stratum_weights: Mapping[str, int],
    alpha: float,
    delta_success_margin: float,
    minimum_cost_saving: float,
    minima: Mapping[str, int],
    policy_id: str,
    ladder: tuple[int, ...] = DEFAULT_SEARCH_LADDER,
) -> dict[str, Any]:
    """Classify whether any bounded allocation could pass both gates.

    Distinguishes two very different failures. If the assumed point estimate
    is already on the failing side of a threshold, no sample size helps under
    the fixed-effect assumption -- the interval would have to be centred
    somewhere it is not. If the point estimate passes but every tested
    allocation still leaves the lower bound short, that is a statement about
    the search budget, not about mathematical possibility.
    """
    weight_total = sum(int(value) for value in stratum_weights.values())
    success_point = sum(
        int(stratum_weights[stratum_id]) / weight_total
        * (states[stratum_id].success_point_estimate
           if states[stratum_id].is_active else 0.0)
        for stratum_id in states
    )
    cost_point = sum(
        int(stratum_weights[stratum_id]) / weight_total
        * (states[stratum_id].cost_point_estimate
           if states[stratum_id].is_active else 0.0)
        for stratum_id in states
    )
    success_margin = success_point - (-delta_success_margin)
    cost_margin = cost_point - minimum_cost_saving
    below = [
        name for name, margin in (
            ("success_non_inferiority", success_margin),
            ("cost_improvement", cost_margin),
        )
        if margin < 0.0
    ]
    result: dict[str, Any] = {
        "overall_success_point_estimate": success_point,
        "overall_cost_saving_point_estimate": cost_point,
        "success_point_margin": success_margin,
        "cost_point_margin": cost_margin,
        "search_ladder": list(ladder),
        "largest_tested_active_block_budget": max(ladder),
        "assumption": (
            "fixed-effect plug-in: the assumed point estimates are held "
            "constant while only the sample size grows"
        ),
        "not_achieved_power": True,
        "not_a_confidence_guarantee": True,
    }
    if below:
        result.update({
            "classification": "POINT_ESTIMATE_BELOW_THRESHOLD",
            "gates_below_threshold": below,
            "first_passing_active_block_budget": None,
            "projected_state_at_largest_budget": None,
            "detail": (
                "under the fixed-effect assumption even an infinitely "
                "narrow interval centred on the assumed point estimate "
                "would fail these gates: " + ", ".join(below)
            ),
        })
        return result

    first_passing: int | None = None
    state_at_largest: str | None = None
    required = sum(minima.values())
    for blocks in ladder:
        if blocks < required:
            # Cannot seat the frozen minima, so no certificate is
            # admissible at this rung regardless of the bounds.
            continue
        allocation = _allocate(states, blocks, alpha, minima)
        certificate = _certificate_at(
            states,
            allocation,
            stratum_weights=stratum_weights,
            alpha=alpha,
            delta_success_margin=delta_success_margin,
            minimum_cost_saving=minimum_cost_saving,
            minima=minima,
            policy_id=policy_id,
        )
        state_at_largest = certificate["certificate_state"]
        if certificate["certificate_state"] == "SAFE_TO_COMMIT":
            first_passing = blocks
            break
    result.update({
        "classification": (
            "PROJECTED_PASS_WITHIN_SEARCH_BUDGET"
            if first_passing is not None
            else "PROJECTED_NOT_WITHIN_SEARCH_BUDGET"
        ),
        "gates_below_threshold": [],
        "first_passing_active_block_budget": first_passing,
        "projected_state_at_largest_budget": state_at_largest,
        "detail": (
            f"a projected allocation of {first_passing} active evidence "
            "blocks passes both weighted gates under the plug-in assumption"
            if first_passing is not None
            else "the point estimates are on the passing side, but no "
            f"tested allocation up to {max(ladder)} active evidence blocks "
            "narrows the weighted lower bounds enough; this is a statement "
            "about the search budget, not about mathematical impossibility"
        ),
    })
    return result


def _matches_target(
    allocation: Mapping[str, int],
    normalized: Mapping[str, float],
) -> bool:
    total = sum(allocation.values())
    if total <= 0:
        return False
    return all(
        abs(allocation.get(stratum_id, 0) / total - weight) < 1e-9
        for stratum_id, weight in normalized.items()
    )


def _margin_warnings(
    active: list[StratumState],
    *,
    delta_success_margin: float,
    minimum_cost_saving: float,
    adjusted_alpha: float,
) -> list[dict[str, Any]]:
    """Flag strata whose point estimate sits too near its threshold.

    A confirmation is only practical when the effect is far enough from the
    gate that a finite cohort can separate them. Saying so during planning is
    cheaper than discovering it after collection.
    """
    warnings: list[dict[str, Any]] = []
    for state in active:
        achievable = state.gate_width(
            state.independent_workloads,
            adjusted_alpha,
        )
        cost_margin = state.cost_point_estimate - minimum_cost_saving
        normalised_margin = abs(cost_margin) / (
            state.cost_support[1] - state.cost_support[0]
        )
        if normalised_margin < achievable / 2.0:
            warnings.append({
                "stratum_id": state.stratum_id,
                "gate_id": "cost_improvement",
                "point_estimate": state.cost_point_estimate,
                "threshold": minimum_cost_saving,
                "normalised_margin": normalised_margin,
                "projected_half_width": achievable / 2.0,
                "detail": (
                    "the cost-saving point estimate is close to the "
                    "threshold relative to the projected interval; a "
                    "decisive confirmation may be impractical at this "
                    "cohort size"
                ),
            })
        success_margin = (
            state.success_point_estimate + delta_success_margin
        )
        normalised_success = abs(success_margin) / (
            state.success_support[1] - state.success_support[0]
        )
        if normalised_success < achievable / 2.0:
            warnings.append({
                "stratum_id": state.stratum_id,
                "gate_id": "success_non_inferiority",
                "point_estimate": state.success_point_estimate,
                "threshold": -delta_success_margin,
                "normalised_margin": normalised_success,
                "projected_half_width": achievable / 2.0,
                "detail": (
                    "the success point estimate is close to the "
                    "non-inferiority margin relative to the projected "
                    "interval"
                ),
            })
    return warnings


def _publish_plan(
    plan: Mapping[str, Any],
    *,
    output_dir: str | Path,
    source: Path,
) -> dict[str, Any]:
    target = Path(output_dir).resolve()
    documents = {
        "oed_plan.json": json.dumps(
            plan, indent=2, sort_keys=True, ensure_ascii=False
        ) + "\n",
        "oed_allocation.csv": _csv_text([
            {
                "stratum_id": stratum_id,
                **{
                    key: value
                    for key, value in entry.items()
                    if not isinstance(value, (dict, list))
                },
            }
            for stratum_id, entry in sorted(
                plan["allocation_by_stratum"].items()
            )
        ]),
    }
    documents["SHA256SUMS"] = "".join(
        f"{sha256(content.encode('utf-8')).hexdigest()}  {name}\n"
        for name, content in sorted(documents.items())
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(
        prefix=".pathfinder-oed-plan-",
        dir=target.parent,
    ) as temp:
        staging = Path(temp) / "plan"
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_text(
                content,
                encoding="utf-8",
                newline="\n",
            )
        _require(
            not target.exists(),
            f"OED plan output directory already exists: {target}",
        )
        staging.rename(target)
    return {
        "status": "PLANNED",
        "plan_id": plan["plan_id"],
        "policy_id": plan["policy_id"],
        "final_action": plan["final_action"],
        "stop_reason": plan["stop_reason"],
        "commit_authorised": False,
        "consumed_active_evidence_blocks": plan["budget"][
            "consumed_active_evidence_blocks"
        ],
        "consumed_sessions": plan["budget"]["consumed_sessions"],
        "active_evidence_blocks_by_stratum": dict(
            plan["active_evidence_blocks_by_stratum"]
        ),
        "structural_zero_strata": list(plan["structural_zero_strata"]),
        "planned_total_sessions": plan["planned_total_sessions"],
        "target_stratum_weights": dict(plan["target_stratum_weights"]),
        "margin_warning_count": len(plan["margin_warnings"]),
        "eligible_for_scientific_claims": False,
        "plan_sha256": sha256(
            documents["oed_plan.json"].encode("utf-8")
        ).hexdigest(),
        "console_only_paths": {"output_dir": str(target)},
    }
