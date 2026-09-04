"""Post-hoc policy-level AWM audit for a frozen distributed pilot.

The distributed restricted pilot observes one safe-origin response and one
predeclared candidate response per workload.  It does *not* observe every
design on every workload.  This module therefore evaluates only policies that
select between those two observed responses.  It never fills in an unobserved
design outcome or promotes the result to a complete Reduced Oracle.
"""

from __future__ import annotations

import csv
import io
import json
import math
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from statistics import mean
from tempfile import TemporaryDirectory
from typing import Any, Mapping

from ..distributed.preregistration import (
    DistributedPilotPreregistration,
    load_distributed_pilot_preregistration,
)
from .certificate import _ClusterObservation, evaluate_stratum_certificate
from .contracts import AWMConfigError
from .model import Interval


DISTRIBUTED_POLICY_AUDIT_CONFIG_SCHEMA_VERSION = (
    "pathfinder.distributed-policy-awm-audit/v1alpha1"
)
DISTRIBUTED_POLICY_AUDIT_EVALUATION_SCHEMA_VERSION = (
    "pathfinder.distributed-policy-awm-evaluation/v1alpha1"
)
POLICY_ASSIGNMENTS = ("safe", "candidate")
POLICY_PROVENANCES = (
    "executed-preregistered-restricted-policy",
    "posthoc-selected-after-inspecting-pilot-outcomes",
)


@dataclass(frozen=True)
class DistributedPolicyDefinition:
    policy_id: str
    provenance: str
    note: str
    assignments: tuple[tuple[str, str], ...]

    def assignment_for(self, stratum_id: str) -> str:
        return dict(self.assignments)[stratum_id]


@dataclass(frozen=True)
class DistributedPolicyAuditConfig:
    schema_version: str
    audit_id: str
    source_path: Path
    source_sha256: str
    posthoc: bool
    eligible_for_scientific_claims: bool
    executed_policy_id: str
    policies: tuple[DistributedPolicyDefinition, ...]
    family_adjustment: str
    bound_method: str
    minimum_independent_workloads: int
    success_difference_support: Interval
    cost_saving_support: Interval

    @property
    def family_size(self) -> int:
        # Match the deployed AWM certificate's conservative accounting: two
        # gates and two tails for every non-baseline policy.
        return len(self.policies) * 2 * 2


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AWMConfigError(message)


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    _require(isinstance(value, str) and bool(value.strip()),
             f"{name} must be a non-empty string")
    return value.strip()


def _boolean(value: Any, name: str) -> bool:
    _require(type(value) is bool, f"{name} must be a boolean")
    return value


def _finite_number(value: Any, name: str) -> float:
    _require(type(value) in (int, float), f"{name} must be numeric")
    result = float(value)
    _require(math.isfinite(result), f"{name} must be finite")
    return result


def _read_json(path: Path) -> Any:
    def invalid_number(value: str) -> None:
        raise AWMConfigError(f"{path.name} contains non-finite JSON")

    def unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result,
                     f"{path.name} contains duplicate JSON key {key}")
            result[key] = value
        return result

    try:
        return json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=invalid_number,
            object_pairs_hook=unique_keys,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AWMConfigError(f"cannot read valid JSON from {path}") from exc


def _load_interval(value: Any, name: str) -> Interval:
    row = _mapping(value, name)
    lower = _finite_number(row.get("lower"), f"{name}.lower")
    upper = _finite_number(row.get("upper"), f"{name}.upper")
    _require(lower < upper, f"{name} must have lower < upper")
    return Interval(lower, upper)


def load_distributed_policy_audit_config(
    path: str | Path,
    *,
    stratum_ids: tuple[str, ...] | None = None,
) -> DistributedPolicyAuditConfig:
    source = Path(path).resolve()
    raw = source.read_bytes()
    payload = _mapping(_read_json(source), "policy audit config")
    schema = _string(payload.get("schema_version"), "schema_version")
    _require(schema == DISTRIBUTED_POLICY_AUDIT_CONFIG_SCHEMA_VERSION,
             f"unsupported distributed policy audit schema: {schema}")
    posthoc = _boolean(payload.get("posthoc"), "posthoc")
    eligible = _boolean(
        payload.get("eligible_for_scientific_claims"),
        "eligible_for_scientific_claims",
    )
    _require(posthoc, "distributed policy audit must remain posthoc")
    _require(not eligible,
             "posthoc distributed policy audit cannot support scientific claims")

    confidence = _mapping(payload.get("confidence"), "confidence")
    family_adjustment = _string(
        confidence.get("family_adjustment"),
        "confidence.family_adjustment",
    )
    _require(family_adjustment == "bonferroni",
             "only bonferroni family adjustment is supported")
    bound_method = _string(
        confidence.get("bound_method"),
        "confidence.bound_method",
    )
    _require(
        bound_method == "workload-cluster-one-sided-bounded-mean-kl",
        "unsupported distributed policy bound method",
    )
    minimum = confidence.get("minimum_independent_workloads")
    _require(type(minimum) is int and minimum > 0,
             "confidence.minimum_independent_workloads must be positive")

    supports = _mapping(payload.get("supports"), "supports")
    success_support = _load_interval(
        supports.get("success_difference"),
        "supports.success_difference",
    )
    cost_support = _load_interval(
        supports.get("cost_saving"),
        "supports.cost_saving",
    )
    _require(success_support.lower <= -1.0 and success_support.upper >= 1.0,
             "success_difference support must contain [-1, 1]")

    declared_strata = None if stratum_ids is None else set(stratum_ids)
    policy_rows = payload.get("policies")
    _require(isinstance(policy_rows, list) and bool(policy_rows),
             "policies must be a non-empty list")
    policies: list[DistributedPolicyDefinition] = []
    seen: set[str] = set()
    for index, value in enumerate(policy_rows):
        row = _mapping(value, f"policies[{index}]")
        policy_id = _string(row.get("policy_id"), f"policies[{index}].policy_id")
        _require(policy_id not in seen, f"duplicate policy_id: {policy_id}")
        seen.add(policy_id)
        provenance = _string(
            row.get("provenance"),
            f"policies[{index}].provenance",
        )
        _require(provenance in POLICY_PROVENANCES,
                 f"unknown policy provenance: {provenance}")
        assignments = _mapping(
            row.get("assignment_by_stratum"),
            f"policies[{index}].assignment_by_stratum",
        )
        if declared_strata is not None:
            _require(set(assignments) == declared_strata,
                     f"policy {policy_id} must assign every observed stratum exactly once")
        parsed: list[tuple[str, str]] = []
        for stratum_id, assignment in sorted(assignments.items()):
            _require(isinstance(stratum_id, str) and bool(stratum_id),
                     f"policy {policy_id} has an invalid stratum id")
            _require(assignment in POLICY_ASSIGNMENTS,
                     f"policy {policy_id} has invalid assignment for {stratum_id}")
            parsed.append((stratum_id, str(assignment)))
        policies.append(DistributedPolicyDefinition(
            policy_id=policy_id,
            provenance=provenance,
            note=_string(row.get("note"), f"policies[{index}].note"),
            assignments=tuple(parsed),
        ))

    executed = _string(payload.get("executed_policy_id"), "executed_policy_id")
    _require(executed in seen, "executed_policy_id is not declared in policies")
    executed_policy = next(policy for policy in policies
                           if policy.policy_id == executed)
    _require(all(value == "candidate" for _, value in executed_policy.assignments),
             "executed policy must select the observed candidate in every stratum")
    _require(
        executed_policy.provenance
        == "executed-preregistered-restricted-policy",
        "executed policy must carry executed-preregistered provenance",
    )
    _require(all(
        policy.policy_id == executed
        or policy.provenance == "posthoc-selected-after-inspecting-pilot-outcomes"
        for policy in policies
    ), "every derived policy must be explicitly marked posthoc")

    return DistributedPolicyAuditConfig(
        schema_version=schema,
        audit_id=_string(payload.get("audit_id"), "audit_id"),
        source_path=source,
        source_sha256=sha256(raw).hexdigest(),
        posthoc=posthoc,
        eligible_for_scientific_claims=eligible,
        executed_policy_id=executed,
        policies=tuple(policies),
        family_adjustment=family_adjustment,
        bound_method=bound_method,
        minimum_independent_workloads=minimum,
        success_difference_support=success_support,
        cost_saving_support=cost_support,
    )


def _verify_evaluation_snapshot(source: Path) -> dict[str, str]:
    checksum_path = source / "SHA256SUMS"
    _require(checksum_path.is_file(), "evaluation SHA256SUMS is missing")
    hashes: dict[str, str] = {}
    try:
        lines = checksum_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise AWMConfigError("cannot read evaluation SHA256SUMS") from exc
    _require(bool(lines), "evaluation SHA256SUMS is empty")
    for line in lines:
        parts = line.split("  ", 1)
        _require(len(parts) == 2, "malformed evaluation SHA256SUMS line")
        digest, name = parts
        _require(len(digest) == 64 and all(c in "0123456789abcdef" for c in digest),
                 "malformed evaluation SHA256 digest")
        _require(name and name not in hashes and Path(name).name == name,
                 "evaluation SHA256SUMS contains an invalid filename")
        path = source / name
        _require(path.is_file(), f"evaluation output is missing: {name}")
        _require(sha256(path.read_bytes()).hexdigest() == digest,
                 f"evaluation checksum mismatch: {name}")
        hashes[name] = digest
    for required in ("evaluation.json", "evaluation_manifest.json",
                     "paired_effects.csv", "report.md", "summary_by_design.csv"):
        _require(required in hashes,
                 f"evaluation SHA256SUMS does not bind {required}")
    return dict(sorted(hashes.items()))


def _csv(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=list(rows[0]),
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _bound_payload(bound: Any) -> dict[str, float]:
    return {
        "point_estimate": bound.point_estimate,
        "lower_bound": bound.lower_bound,
        "upper_bound": bound.upper_bound,
        "support_lower": bound.support.lower,
        "support_upper": bound.support.upper,
    }


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Distributed policy-level AWM audit",
        "",
        f"Pilot: `{report['pilot_id']}`",
        "",
        "This is a post-hoc policy diagnostic over observed safe/candidate pairs. "
        "It is not a complete design Oracle and is not eligible for scientific claims.",
        "",
        "| Policy | Success delta | Cost saving | Point thresholds | Certificate |",
        "|---|---:|---:|---|---|",
    ]
    for row in report["policy_summaries"]:
        lines.append(
            f"| {row['policy_id']} | {row['mean_task_success_delta']:.6g} | "
            f"{row['mean_cost_saving']:.6g} | "
            f"{'pass' if row['meets_point_thresholds'] else 'fail'} | "
            f"{row['certificate_state']} |"
        )
    lines.extend([
        "",
        "## Interpretation",
        "",
        "- Point-threshold checks are descriptive and are not confidence guarantees.",
        "- Certificate decisions use workload clusters; repetitions do not increase n.",
        "- Every policy derived after inspecting outcomes remains post-hoc even if a gate passes.",
        "- A fresh preregistered holdout is required before any SAFE_TO_COMMIT claim.",
    ])
    return "\n".join(lines) + "\n"


def audit_distributed_policy_awm(
    evaluation_dir: str | Path,
    *,
    preregistration: str | Path,
    audit_config: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Audit observed distributed policies without inventing counterfactuals."""
    source = Path(evaluation_dir).resolve()
    target = Path(output_dir).resolve()
    prereg_path = Path(preregistration).resolve()
    config_path = Path(audit_config).resolve()
    _require(source.is_dir(), "evaluation directory does not exist")
    _require(prereg_path.is_file(), "preregistration does not exist")
    _require(config_path.is_file(), "policy audit config does not exist")
    _require(not target.exists(), "policy audit output directory already exists")
    for protected in (source, prereg_path.parent, config_path.parent):
        _require(not target.is_relative_to(protected)
                 and not protected.is_relative_to(target),
                 "policy audit output must be separate from every input directory")

    source_hashes = _verify_evaluation_snapshot(source)
    evaluation_bytes = (source / "evaluation.json").read_bytes()
    report = _mapping(_read_json(source / "evaluation.json"), "evaluation")
    prereg = load_distributed_pilot_preregistration(prereg_path)
    stratum_ids = tuple(stratum.stratum_id for stratum in prereg.strata)
    config = load_distributed_policy_audit_config(
        config_path,
        stratum_ids=stratum_ids,
    )

    _require(report.get("pilot_id") == prereg.pilot_id,
             "evaluation pilot_id disagrees with preregistration")
    _require(report.get("status") == "COMPLETE",
             "evaluation is not complete")
    _require(report.get("eligible_for_scientific_claims") is False,
             "distributed policy audit expects pilot-only evidence")
    _require(report.get("declared_cost_basis") == "total_cost",
             "distributed policy audit requires total_cost")
    _require(report.get("independent_workloads")
             == prereg.independent_workload_count,
             "evaluation independent-workload count disagrees with preregistration")
    _require(report.get("repetitions") == prereg.repetitions,
             "evaluation repetition count disagrees with preregistration")
    pair_rows = report.get("paired_workload_effects")
    _require(isinstance(pair_rows, list),
             "evaluation paired_workload_effects is missing")
    _require(len(pair_rows) == prereg.independent_workload_count,
             "evaluation does not contain one pair per independent workload")

    by_workload: dict[str, Mapping[str, Any]] = {}
    expected = set(prereg.workload_ids)
    for index, value in enumerate(pair_rows):
        row = _mapping(value, f"paired_workload_effects[{index}]")
        workload_id = _string(row.get("workload_id"), "workload_id")
        _require(workload_id in expected and workload_id not in by_workload,
                 "paired workload ids are duplicate or unplanned")
        stratum = prereg.stratum_for(workload_id)
        _require(row.get("stratum_id") == stratum.stratum_id,
                 f"paired stratum disagrees for {workload_id}")
        _require(row.get("safe_design_id") == prereg.safe_design_id,
                 f"paired safe design disagrees for {workload_id}")
        _require(row.get("candidate_design_id") == stratum.candidate_design_id,
                 f"paired candidate design disagrees for {workload_id}")
        _require(row.get("repetitions") == prereg.repetitions,
                 f"paired repetition count disagrees for {workload_id}")
        success = _finite_number(row.get("task_success_delta"),
                                 f"{workload_id}.task_success_delta")
        cost_delta = _finite_number(row.get("total_cost_delta"),
                                    f"{workload_id}.total_cost_delta")
        _require(config.success_difference_support.contains(success),
                 f"{workload_id} success difference exceeds support")
        _require(config.cost_saving_support.contains(-cost_delta),
                 f"{workload_id} cost saving exceeds support")
        by_workload[workload_id] = row
    _require(set(by_workload) == expected,
             "evaluation paired workload set disagrees with preregistration")

    aggregate_rows = report.get("paired_aggregates")
    _require(isinstance(aggregate_rows, list) and bool(aggregate_rows),
             "evaluation paired_aggregates is missing")
    source_overall = next(
        (row for row in aggregate_rows
         if isinstance(row, Mapping) and row.get("scope") == "overall"),
        None,
    )
    _require(source_overall is not None,
             "evaluation overall paired aggregate is missing")
    source_success_mean = _finite_number(
        source_overall.get("mean_task_success_delta"),
        "evaluation overall mean_task_success_delta",
    )
    source_cost_delta = _finite_number(
        source_overall.get("mean_total_cost_delta"),
        "evaluation overall mean_total_cost_delta",
    )

    adjusted_alpha = prereg.alpha / config.family_size
    _require(0.0 < adjusted_alpha < 1.0,
             "family-adjusted alpha must be in (0, 1)")
    utility_support = Interval(
        config.success_difference_support.lower
        + config.cost_saving_support.lower,
        config.success_difference_support.upper
        + config.cost_saving_support.upper,
    )
    workload_rows: list[dict[str, Any]] = []
    policy_summaries: list[dict[str, Any]] = []
    policy_details: list[dict[str, Any]] = []

    for policy in config.policies:
        observations: list[_ClusterObservation] = []
        stratum_groups: dict[str, list[_ClusterObservation]] = {
            stratum_id: [] for stratum_id in stratum_ids
        }
        candidate_workloads = 0
        for workload_id in prereg.workload_ids:
            source_row = by_workload[workload_id]
            stratum_id = str(source_row["stratum_id"])
            assignment = policy.assignment_for(stratum_id)
            if assignment == "candidate":
                success = float(source_row["task_success_delta"])
                cost_saving = -float(source_row["total_cost_delta"])
                selected_design = str(source_row["candidate_design_id"])
                candidate_workloads += 1
            else:
                success = 0.0
                cost_saving = 0.0
                selected_design = str(source_row["safe_design_id"])
            observation = _ClusterObservation(
                workload_id=workload_id,
                success_difference=success,
                cost_saving=cost_saving,
                utility_gain=success + cost_saving,
            )
            observations.append(observation)
            stratum_groups[stratum_id].append(observation)
            workload_rows.append({
                "policy_id": policy.policy_id,
                "policy_provenance": policy.provenance,
                "workload_id": workload_id,
                "stratum_id": stratum_id,
                "selected_role": assignment,
                "selected_design_id": selected_design,
                "safe_design_id": source_row["safe_design_id"],
                "observed_candidate_design_id": source_row["candidate_design_id"],
                "task_success_delta_vs_safe": success,
                "cost_saving_vs_safe": cost_saving,
            })

        certificate = evaluate_stratum_certificate(
            tuple(observations),
            success_difference_support=config.success_difference_support,
            cost_saving_support=config.cost_saving_support,
            utility_support=utility_support,
            adjusted_alpha=adjusted_alpha,
            delta_success_margin=prereg.delta_success_margin,
            minimum_cost_saving=prereg.minimum_cost_saving,
            minimum_independent_workloads=config.minimum_independent_workloads,
            safe_design_id="all_safe",
            candidate_design_id=policy.policy_id,
            bound_method=config.bound_method,
        )
        success_mean = certificate.success_bound.point_estimate
        cost_mean = certificate.cost_bound.point_estimate
        meets_point = (
            success_mean >= -prereg.delta_success_margin
            and cost_mean >= prereg.minimum_cost_saving
        )
        summary = {
            "policy_id": policy.policy_id,
            "provenance": policy.provenance,
            "independent_workloads": len(observations),
            "candidate_workloads": candidate_workloads,
            "safe_fallback_workloads": len(observations) - candidate_workloads,
            "mean_task_success_delta": success_mean,
            "mean_cost_saving": cost_mean,
            "meets_point_thresholds": meets_point,
            "certificate_state": certificate.certificate_state,
            "success_lower_bound": certificate.success_bound.lower_bound,
            "success_upper_bound": certificate.success_bound.upper_bound,
            "cost_saving_lower_bound": certificate.cost_bound.lower_bound,
            "cost_saving_upper_bound": certificate.cost_bound.upper_bound,
            "decision_use": "posthoc-diagnostic-only",
            "eligible_for_scientific_claims": False,
        }
        policy_summaries.append(summary)
        policy_details.append({
            **summary,
            "note": policy.note,
            "assignment_by_stratum": dict(policy.assignments),
            "success_difference": _bound_payload(certificate.success_bound),
            "cost_saving": _bound_payload(certificate.cost_bound),
            "gates": list(certificate.gates),
            "certificate_decision": certificate.decision,
            "stratum_point_estimates": [
                {
                    "stratum_id": stratum_id,
                    "independent_workloads": len(group),
                    "mean_task_success_delta": mean(
                        item.success_difference for item in group
                    ),
                    "mean_cost_saving": mean(item.cost_saving for item in group),
                }
                for stratum_id, group in stratum_groups.items()
            ],
        })

        if policy.policy_id == config.executed_policy_id:
            _require(math.isclose(
                success_mean,
                source_success_mean,
                rel_tol=0.0,
                abs_tol=1e-12,
            ), "executed policy success does not reproduce evaluator output")
            _require(math.isclose(
                -cost_mean,
                source_cost_delta,
                rel_tol=0.0,
                abs_tol=1e-12,
            ), "executed policy cost does not reproduce evaluator output")

    result = {
        "schema_version": DISTRIBUTED_POLICY_AUDIT_EVALUATION_SCHEMA_VERSION,
        "status": "COMPLETE",
        "audit_id": config.audit_id,
        "pilot_id": prereg.pilot_id,
        "input_origin": "frozen-offline-distributed-evaluation",
        "source_evaluation_sha256s": source_hashes,
        "source_evaluation_json_sha256": sha256(evaluation_bytes).hexdigest(),
        "preregistration_sha256": prereg.source_sha256,
        "audit_config_sha256": config.source_sha256,
        "independent_workloads": prereg.independent_workload_count,
        "repetitions_per_workload": prereg.repetitions,
        "repetitions_increase_independent_units": False,
        "safe_baseline": prereg.safe_design_id,
        "observed_counterfactual_scope": (
            "safe-origin-versus-one-stratum-assigned-candidate-per-workload"
        ),
        "complete_design_oracle": False,
        "unobserved_design_outcomes_imputed": False,
        "thresholds": {
            "delta_success_margin": prereg.delta_success_margin,
            "minimum_cost_saving": prereg.minimum_cost_saving,
            "alpha": prereg.alpha,
            "provenance": prereg.threshold_provenance,
        },
        "confidence": {
            "family_adjustment": config.family_adjustment,
            "family_size": config.family_size,
            "adjusted_alpha": adjusted_alpha,
            "bound_method": config.bound_method,
            "minimum_independent_workloads": (
                config.minimum_independent_workloads
            ),
        },
        "policy_summaries": policy_summaries,
        "policy_details": policy_details,
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "recommended_next_step": (
            "preregister the selected policy and evaluate it on a fresh, "
            "outcome-unseen workload holdout before any commit claim"
        ),
        "limitations": [
            "The pilot observes only safe versus the assigned candidate, not a full design matrix.",
            "Derived policy selection inspected these outcomes and is necessarily post-hoc.",
            "The frozen pilot documents a runtime model deviation, so it is engineering evidence.",
            "Point thresholds are descriptive; only the bounded certificate carries uncertainty.",
        ],
    }
    documents = {
        "policy_evaluation.json": json.dumps(
            result,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        ) + "\n",
        "policy_summary.csv": _csv(policy_summaries),
        "policy_workload_effects.csv": _csv(workload_rows),
        "report.md": _markdown(result),
    }
    manifest = {
        "schema_version": DISTRIBUTED_POLICY_AUDIT_EVALUATION_SCHEMA_VERSION,
        "audit_id": config.audit_id,
        "pilot_id": prereg.pilot_id,
        "offline": True,
        "deployment_mutations_performed": False,
        "credentials_recorded": False,
        "inputs_unchanged": True,
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "input_files_sha256": {
            "audit_config": config.source_sha256,
            "preregistration": prereg.source_sha256,
            "evaluation.json": sha256(evaluation_bytes).hexdigest(),
            "evaluation/SHA256SUMS": sha256(
                (source / "SHA256SUMS").read_bytes()
            ).hexdigest(),
        },
    }
    documents["policy_manifest.json"] = json.dumps(
        manifest,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
    ) + "\n"
    documents["SHA256SUMS"] = "".join(
        f"{sha256(content.encode('utf-8')).hexdigest()}  {name}\n"
        for name, content in sorted(documents.items())
    )

    # Recheck every input immediately before the atomic publish.
    _require(sha256((source / "evaluation.json").read_bytes()).hexdigest()
             == sha256(evaluation_bytes).hexdigest(),
             "evaluation changed during policy audit")
    _require(load_distributed_pilot_preregistration(prereg_path).source_sha256
             == prereg.source_sha256,
             "preregistration changed during policy audit")
    _require(sha256(config_path.read_bytes()).hexdigest() == config.source_sha256,
             "policy audit config changed during audit")
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".pathfinder-policy-awm-", dir=target.parent) as temp:
        staging = Path(temp) / "audit"
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_text(content, encoding="utf-8", newline="\n")
        _require(not target.exists(), "policy audit output directory already exists")
        staging.rename(target)
    return result
