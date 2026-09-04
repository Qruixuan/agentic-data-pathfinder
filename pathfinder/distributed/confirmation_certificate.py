"""Evaluate a completed confirmation run against a frozen plan.

The frozen plan is a contract: one policy, one execution model, fixed
weights, declared thresholds and supports, and a fresh cohort disjoint from
the evidence that selected the policy. This module checks that contract and
then evaluates the weighted certificate. It does not relax any term of it.

Every failure here is a refusal rather than a degraded observation. Evidence
that is missing, malformed, incomplete, reused, model-mismatched, or
hash-mismatched is not weak data; it is not data, and turning it into a
statistical observation would be the whole error this layer exists to
prevent.

Hashes establish **consistency and reproducibility, not authenticity**.
Nothing here is signed: anyone who can write the evidence directory can write
matching hashes into it. What the digests buy is that a reviewer can tell
whether the artefacts they hold are the ones the certificate was computed
from.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable, Mapping

from ..awm.model import Interval
from .confirmation import (
    EXECUTION_MODEL_ID,
    SAFE_DESIGN_ID,
    ConfirmationPlanError,
    _csv_text,
    _require,
)
from .weighted_certificate import (
    StratumEvidence,
    distance_to_threshold,
    evaluate_weighted_policy_certificate,
)


CONFIRMATION_CERTIFICATE_SCHEMA_VERSION = (
    "pathfinder.distributed-policy-confirmation-certificate/v1alpha1"
)
EXECUTION_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.distributed-execution-evidence/v1alpha1"
)
#: Files a confirmation evidence directory must bind before it may be read.
REQUIRED_EVIDENCE_FILES = (
    "evaluation.json",
    "evaluation_manifest.json",
    "paired_effects.csv",
    "report.md",
    "summary_by_design.csv",
)
POLICY_SELECTION_ORIGIN = "posthoc_previous_pilot"
CONFIRMATION_EVIDENCE_ORIGIN = "fresh_preregistered_holdout"


def _verify_snapshot(
    source: Path,
    required: Iterable[str],
    label: str,
) -> dict[str, str]:
    checksum_path = source / "SHA256SUMS"
    _require(checksum_path.is_file(), f"{label} SHA256SUMS is missing")
    try:
        lines = checksum_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ConfirmationPlanError(
            f"cannot read {label} SHA256SUMS"
        ) from exc
    _require(bool(lines), f"{label} SHA256SUMS is empty")
    hashes: dict[str, str] = {}
    for line in lines:
        parts = line.split("  ", 1)
        _require(len(parts) == 2, f"malformed {label} SHA256SUMS line")
        digest, name = parts
        _require(
            len(digest) == 64
            and all(c in "0123456789abcdef" for c in digest),
            f"malformed {label} SHA256 digest",
        )
        _require(
            name and name not in hashes and Path(name).name == name,
            f"{label} SHA256SUMS contains an invalid filename",
        )
        path = source / name
        _require(path.is_file(), f"{label} file is missing: {name}")
        _require(
            sha256(path.read_bytes()).hexdigest() == digest,
            f"{label} checksum mismatch: {name}",
        )
        hashes[name] = digest
    for name in required:
        _require(
            name in hashes,
            f"{label} SHA256SUMS does not bind {name}",
        )
    return dict(sorted(hashes.items()))


@dataclass(frozen=True)
class ExecutionEvidence:
    """The smallest portable binding of a run to a plan and a model.

    The distributed evaluation format records what happened but cannot by
    itself prove which runtime model produced it, so a confirmation run must
    supply this alongside.
    """

    schema_version: str
    execution_model_id: str
    confirmation_plan_sha256: str
    evaluation_sha256: str
    pilot_id: str
    run_id: str
    evaluation_id: str
    source_sha256: str

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "execution_model_id": self.execution_model_id,
            "confirmation_plan_sha256": self.confirmation_plan_sha256,
            "evaluation_sha256": self.evaluation_sha256,
            "pilot_id": self.pilot_id,
            "run_id": self.run_id,
            "evaluation_id": self.evaluation_id,
            "execution_evidence_sha256": self.source_sha256,
            "establishes": "consistency-and-reproducibility",
            "does_not_establish": "authenticity",
        }


def load_execution_evidence(path: str | Path) -> ExecutionEvidence:
    """Load and validate the execution-evidence manifest."""
    source = Path(path).resolve()
    _require(
        source.is_file(),
        f"execution evidence manifest does not exist: {source}",
    )
    raw = source.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfirmationPlanError(
            f"invalid execution evidence manifest: {source}"
        ) from exc
    _require(
        isinstance(payload, Mapping),
        "execution evidence manifest must be an object",
    )
    for key in (
        "schema_version",
        "execution_model_id",
        "confirmation_plan_sha256",
        "evaluation_sha256",
        "pilot_id",
        "run_id",
        "evaluation_id",
    ):
        value = payload.get(key)
        _require(
            isinstance(value, str) and value.strip(),
            f"execution evidence {key} must be a non-empty string",
        )
    _require(
        payload["schema_version"] == EXECUTION_EVIDENCE_SCHEMA_VERSION,
        "unsupported execution evidence schema_version: "
        + str(payload["schema_version"]),
    )
    return ExecutionEvidence(
        schema_version=str(payload["schema_version"]),
        execution_model_id=str(payload["execution_model_id"]).strip(),
        confirmation_plan_sha256=str(
            payload["confirmation_plan_sha256"]
        ).strip(),
        evaluation_sha256=str(payload["evaluation_sha256"]).strip(),
        pilot_id=str(payload["pilot_id"]).strip(),
        run_id=str(payload["run_id"]).strip(),
        evaluation_id=str(payload["evaluation_id"]).strip(),
        source_sha256=sha256(raw).hexdigest(),
    )


def _paired_effects(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    _require(rows, "paired_effects.csv has no rows")
    return rows


def certify_distributed_policy_confirmation(
    plan_dir: str | Path,
    evidence_dir: str | Path,
    *,
    execution_evidence: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Check the frozen contract, then evaluate the weighted certificate."""
    plan_source = Path(plan_dir).resolve()
    evidence_source = Path(evidence_dir).resolve()
    _require(
        plan_source.is_dir(),
        f"confirmation plan directory does not exist: {plan_source}",
    )
    _require(
        evidence_source.is_dir(),
        f"confirmation evidence directory does not exist: {evidence_source}",
    )
    plan_hashes = _verify_snapshot(
        plan_source,
        (
            "confirmation_plan.json",
            "confirmation_strata.csv",
            "confirmation_manifest.json",
        ),
        "confirmation plan",
    )
    evidence_hashes = _verify_snapshot(
        evidence_source,
        REQUIRED_EVIDENCE_FILES,
        "confirmation evidence",
    )

    plan_bytes = (plan_source / "confirmation_plan.json").read_bytes()
    plan = json.loads(plan_bytes)
    plan_sha256 = sha256(plan_bytes).hexdigest()
    evaluation_bytes = (evidence_source / "evaluation.json").read_bytes()
    evaluation = json.loads(evaluation_bytes)
    evaluation_sha256 = sha256(evaluation_bytes).hexdigest()

    evidence = load_execution_evidence(execution_evidence)
    _require(
        evidence.confirmation_plan_sha256 == plan_sha256,
        "the execution evidence names confirmation plan "
        f"{evidence.confirmation_plan_sha256} but this plan hashes to "
        f"{plan_sha256}",
    )
    _require(
        evidence.evaluation_sha256 == evaluation_sha256,
        "the execution evidence names a different evaluation document",
    )
    _require(
        evidence.execution_model_id == EXECUTION_MODEL_ID,
        f"the run used execution model {evidence.execution_model_id!r}; the "
        f"frozen plan binds {EXECUTION_MODEL_ID!r}",
    )
    required_model = plan["future_certificate_requirements"][
        "execution_model_must_match"
    ]
    _require(
        evidence.execution_model_id == required_model,
        f"the run model does not match the plan's required {required_model!r}",
    )

    # The selecting cohort can never be the confirming cohort.
    selection_pilot_id = plan["policy_selection"][
        "selection_evidence_pilot_id"
    ]
    _require(
        evaluation.get("pilot_id") != selection_pilot_id,
        "this evaluation is the post-hoc pilot that selected the policy "
        f"({selection_pilot_id}); it can never be its own confirmation",
    )
    _require(
        evidence.pilot_id == evaluation.get("pilot_id"),
        "the execution evidence pilot id does not match the evaluation",
    )

    rows = _paired_effects(evidence_source / "paired_effects.csv")

    thresholds = plan["thresholds"]
    supports = plan["supports"]
    estimand = plan["estimand"]
    weights = estimand["target_stratum_weights_integer"]
    strata_plan = {item["stratum_id"]: item for item in plan["strata"]}
    repetitions = int(plan["repetitions"])
    fresh_ids, fresh_objects, structural_ids = _plan_cohort_identifiers(plan)

    observed_workloads: set[str] = set()
    observed_objects: set[str] = set()
    by_stratum: dict[str, list[dict[str, str]]] = {}
    for index, row in enumerate(rows):
        where = f"paired_effects.csv:{index + 2}"
        workload_id = str(row.get("workload_id") or "").strip()
        object_id = str(row.get("object_id") or "").strip()
        stratum_id = str(row.get("stratum_id") or "").strip()
        _require(workload_id, f"{where}: missing workload_id")
        _require(object_id, f"{where}: missing object_id")
        _require(
            stratum_id in strata_plan,
            f"{where}: stratum {stratum_id!r} is not in the frozen plan",
        )
        _require(
            workload_id not in observed_workloads,
            f"{where}: duplicate workload {workload_id}; one independent "
            "cluster per workload",
        )
        _require(
            object_id not in observed_objects,
            f"{where}: object {object_id} appears twice; one independent "
            "cluster per object",
        )
        observed_workloads.add(workload_id)
        observed_objects.add(object_id)
        _require(
            str(row.get("safe_design_id")) == SAFE_DESIGN_ID,
            f"{where}: safe design must be {SAFE_DESIGN_ID}",
        )
        _require(
            int(row.get("repetitions") or 0) == repetitions,
            f"{where}: expected {repetitions} repetitions",
        )
        by_stratum.setdefault(stratum_id, []).append(row)

    reused_structural = sorted(observed_workloads & structural_ids)
    _require(
        not reused_structural,
        "a structural-safe workload carries a paired record: "
        + ", ".join(reused_structural[:3])
        + "; comparing the safe design against itself is not an observation",
    )
    _require(
        observed_workloads == fresh_ids,
        "the evidence workloads do not match the frozen fresh cohort's "
        "active strata; missing="
        + ", ".join(sorted(fresh_ids - observed_workloads)[:3] or ["none"])
        + " unexpected="
        + ", ".join(sorted(observed_workloads - fresh_ids)[:3] or ["none"]),
    )
    _require(
        observed_objects == fresh_objects,
        "the evidence object/video IDs do not match the frozen fresh cohort",
    )
    _require(
        _telemetry_complete(evaluation),
        "the evaluation does not report literal telemetry and "
        "artifact-delivery completeness for every canonical record",
    )

    strata_evidence: list[StratumEvidence] = []
    workload_rows: list[dict[str, Any]] = []
    for stratum_id in sorted(strata_plan):
        entry = strata_plan[stratum_id]
        stratum_rows = by_stratum.get(stratum_id, [])
        if entry["role"] == "structural_safe":
            _require(
                not any(
                    str(row.get("candidate_design_id")) != SAFE_DESIGN_ID
                    for row in stratum_rows
                ),
                f"{stratum_id} applies the safe design, so a candidate "
                "record cannot exist for it",
            )
            strata_evidence.append(StratumEvidence(
                stratum_id=stratum_id,
                role="structural_safe",
                integer_weight=int(weights[stratum_id]),
            ))
            continue
        expected_design = str(entry["policy_design_id"])
        _require(
            stratum_rows,
            f"active stratum {stratum_id} has no safe/candidate blocks",
        )
        successes: list[float] = []
        savings: list[float] = []
        for row in stratum_rows:
            _require(
                str(row.get("candidate_design_id")) == expected_design,
                f"{stratum_id}: candidate design must be {expected_design}",
            )
            successes.append(float(row["task_success_delta"]))
            # total_cost_delta is candidate minus safe; the saving is the
            # negation, so a cheaper candidate is a positive saving.
            savings.append(-float(row["total_cost_delta"]))
            workload_rows.append({
                "workload_id": row["workload_id"],
                "object_id": row["object_id"],
                "stratum_id": stratum_id,
                "role": "active",
                "policy_design_id": expected_design,
                "safe_design_id": SAFE_DESIGN_ID,
                "repetitions": repetitions,
                "task_success_delta": successes[-1],
                "cost_saving": savings[-1],
            })
        strata_evidence.append(StratumEvidence(
            stratum_id=stratum_id,
            role="active",
            integer_weight=int(weights[stratum_id]),
            success_differences=tuple(successes),
            cost_savings=tuple(savings),
        ))

    certificate = evaluate_weighted_policy_certificate(
        strata_evidence,
        success_difference_support=Interval(
            float(supports["success_difference"][0]),
            float(supports["success_difference"][1]),
        ),
        cost_saving_support=Interval(
            float(supports["cost_saving"][0]),
            float(supports["cost_saving"][1]),
        ),
        alpha=float(thresholds["alpha"]),
        delta_success_margin=float(thresholds["delta_success_margin"]),
        minimum_cost_saving=float(thresholds["minimum_cost_saving"]),
        minimum_independent_workloads_by_stratum=_frozen_minima(plan),
        safe_design_id=SAFE_DESIGN_ID,
        policy_id=str(plan["selected_policy_id"]),
    )
    return _publish(
        certificate,
        plan=plan,
        plan_sha256=plan_sha256,
        plan_hashes=plan_hashes,
        evidence_hashes=evidence_hashes,
        evaluation=evaluation,
        evaluation_sha256=evaluation_sha256,
        execution_evidence=evidence,
        workload_rows=workload_rows,
        repetitions=repetitions,
        output_dir=output_dir,
    )


def _frozen_minima(plan: Mapping[str, Any]) -> dict[str, int]:
    """The plan's frozen per-active-stratum workload floors."""
    minima = plan.get("minimum_independent_workloads_by_active_stratum")
    _require(
        isinstance(minima, Mapping) and minima,
        "the confirmation plan does not freeze "
        "minimum_independent_workloads_by_active_stratum; it cannot be "
        "certified without one",
    )
    return {str(key): int(value) for key, value in minima.items()}


def _plan_cohort_identifiers(
    plan: Mapping[str, Any],
) -> tuple[set[str], set[str], set[str]]:
    """Split the frozen cohort into active and structural-safe workloads.

    Only active-stratum workloads produce a paired safe/candidate record. A
    structural-safe workload is part of the cohort and carries its stratum's
    weight, but comparing the safe design against itself is not an
    observation, so no row may exist for it.
    """
    cohort = plan.get("fresh_cohort")
    _require(
        isinstance(cohort, Mapping) and cohort,
        "the confirmation plan does not enumerate its fresh cohort; it "
        "cannot be checked against collected evidence",
    )
    roles = {
        item["stratum_id"]: item["role"] for item in plan["strata"]
    }
    active_ids: set[str] = set()
    active_objects: set[str] = set()
    structural_ids: set[str] = set()
    for workload_id, entry in cohort.items():
        stratum_id = str(entry["stratum_id"])
        _require(
            stratum_id in roles,
            f"cohort workload {workload_id} names unplanned stratum "
            f"{stratum_id}",
        )
        if roles[stratum_id] == "structural_safe":
            structural_ids.add(str(workload_id))
        else:
            active_ids.add(str(workload_id))
            active_objects.add(str(entry["object_id"]))
    return active_ids, active_objects, structural_ids


def _telemetry_complete(evaluation: Mapping[str, Any]) -> bool:
    """Literal completeness, not truthiness."""
    for key in (
        "telemetry_complete",
        "artifact_delivery_complete",
    ):
        if evaluation.get(key) is not True:
            return False
    return True


def _publish(
    certificate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    plan_hashes: Mapping[str, str],
    evidence_hashes: Mapping[str, str],
    evaluation: Mapping[str, Any],
    evaluation_sha256: str,
    execution_evidence: ExecutionEvidence,
    workload_rows: list[dict[str, Any]],
    repetitions: int,
    output_dir: str | Path,
) -> dict[str, Any]:
    target = Path(output_dir).resolve()
    state = str(certificate["certificate_state"])
    applied = str(certificate["decision"]["applied_design_id"])
    # Statistical state and claim eligibility are separate questions. A valid
    # engineering certificate on a fresh holdout still inherits the plan's
    # threshold and sampling provenance, which this plan does not claim meets
    # scientific-claim requirements.
    eligible = bool(plan.get("eligible_for_scientific_claims", False))
    active_workloads = int(certificate["active_independent_workloads"])
    structural = certificate["confidence"]["structural_zero_stratum_ids"]

    document = {
        "schema_version": CONFIRMATION_CERTIFICATE_SCHEMA_VERSION,
        "plan_id": plan["plan_id"],
        "policy_id": plan["selected_policy_id"],
        "execution_model_id": execution_evidence.execution_model_id,
        "policy_selection_origin": POLICY_SELECTION_ORIGIN,
        "confirmation_evidence_origin": CONFIRMATION_EVIDENCE_ORIGIN,
        "certificate": dict(certificate),
        "distance_to_threshold": distance_to_threshold(certificate),
        "minimum_independent_workloads": dict(
            certificate["minimum_independent_workloads"]
        ),
        "evidence_allocation": {
            "independent_workloads_by_stratum": {
                item["stratum_id"]: item["independent_workloads"]
                for item in certificate["strata"]
            },
            "active_independent_workloads": active_workloads,
            "structural_zero_strata": list(structural),
            "repetitions_per_workload": repetitions,
            "raw_repetition_sessions": (
                active_workloads * repetitions * 2
            ),
            "repetitions_increase_independent_units": False,
        },
        "eligible_for_scientific_claims": eligible,
        "claim_eligibility_note": (
            "Statistical state and claim eligibility are separate. A "
            "SAFE_TO_COMMIT certificate on a fresh holdout is a valid "
            "engineering result; it becomes a scientific claim only if the "
            "frozen plan's threshold and sampling provenance independently "
            "meet those requirements, which is not promoted automatically."
        ),
        "hash_semantics": (
            "Recorded digests establish consistency and reproducibility, "
            "not authenticity; nothing here is signed."
        ),
        "input_sha256": {
            "confirmation_plan": plan_sha256,
            "confirmation_plan_files": dict(plan_hashes),
            "confirmation_evidence_files": dict(evidence_hashes),
            "evaluation": evaluation_sha256,
            "execution_evidence": execution_evidence.source_sha256,
        },
        "execution_evidence": execution_evidence.to_public_dict(),
        "offline": True,
        "credentials_recorded": False,
        "authorises_commit": state == "SAFE_TO_COMMIT",
    }
    documents: dict[str, str] = {
        "confirmation_certificate.json": json.dumps(
            document, indent=2, sort_keys=True, ensure_ascii=False
        ) + "\n",
        "stratum_certificate.csv": _csv_text([
            {
                "stratum_id": item["stratum_id"],
                "role": item["role"],
                "structural_zero_effect": item["structural_zero_effect"],
                "integer_weight": item["integer_weight"],
                "normalized_weight": item["normalized_weight"],
                "independent_workloads": item["independent_workloads"],
                "alpha_consumed": item["alpha_consumed"],
                "success_point": item["success_difference"][
                    "point_estimate"
                ],
                "success_lower": item["success_difference"]["lower_bound"],
                "success_upper": item["success_difference"]["upper_bound"],
                "cost_point": item["cost_saving"]["point_estimate"],
                "cost_lower": item["cost_saving"]["lower_bound"],
                "cost_upper": item["cost_saving"]["upper_bound"],
            }
            for item in certificate["strata"]
        ]),
        "workload_effects.csv": _csv_text(workload_rows),
        "report.md": _report(
            document,
            certificate,
            plan=plan,
            repetitions=repetitions,
        ),
    }
    documents["certificate_manifest.json"] = json.dumps(
        {
            "schema_version": CONFIRMATION_CERTIFICATE_SCHEMA_VERSION,
            "plan_id": plan["plan_id"],
            "policy_id": plan["selected_policy_id"],
            "certificate_state": state,
            "applied_design_id": applied,
            "certificate_sha256": sha256(
                documents["confirmation_certificate.json"].encode("utf-8")
            ).hexdigest(),
            "inputs": {
                "confirmation_plan": {
                    "role": "frozen-confirmation-contract",
                    "sha256": plan_sha256,
                },
                "confirmation_evidence": {
                    "role": "fresh-preregistered-holdout",
                    "file_sha256": dict(evidence_hashes),
                },
                "execution_evidence": {
                    "role": "runtime-model-binding",
                    "sha256": execution_evidence.source_sha256,
                },
            },
            "portable": True,
            "absolute_paths_recorded": False,
            "offline": True,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": eligible,
        },
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
    ) + "\n"
    documents["SHA256SUMS"] = "".join(
        f"{sha256(content.encode('utf-8')).hexdigest()}  {name}\n"
        for name, content in sorted(documents.items())
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(
        prefix=".pathfinder-confirmation-certificate-",
        dir=target.parent,
    ) as temp:
        staging = Path(temp) / "certificate"
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_text(
                content,
                encoding="utf-8",
                newline="\n",
            )
        _require(
            not target.exists(),
            f"certificate output directory already exists: {target}",
        )
        staging.rename(target)
    return {
        "status": "EVALUATED",
        "plan_id": plan["plan_id"],
        "policy_id": plan["selected_policy_id"],
        "certificate_state": state,
        "applied_design_id": applied,
        "fallback_design_id": SAFE_DESIGN_ID,
        "overall_success_difference": dict(
            certificate["overall_success_difference"]
        ),
        "overall_cost_saving": dict(certificate["overall_cost_saving"]),
        "active_independent_workloads": active_workloads,
        "structural_zero_strata": list(structural),
        "policy_selection_origin": POLICY_SELECTION_ORIGIN,
        "confirmation_evidence_origin": CONFIRMATION_EVIDENCE_ORIGIN,
        "eligible_for_scientific_claims": eligible,
        "certificate_sha256": sha256(
            documents["confirmation_certificate.json"].encode("utf-8")
        ).hexdigest(),
        "console_only_paths": {"output_dir": str(target)},
    }


def _report(
    document: Mapping[str, Any],
    certificate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    repetitions: int,
) -> str:
    estimand = certificate["estimand"]
    allocation = document["evidence_allocation"]
    lines = [
        "# Distributed policy confirmation certificate",
        "",
        f"Policy: `{plan['selected_policy_id']}`  ",
        f"Execution model: `{document['execution_model_id']}`  ",
        f"Certificate state: **{certificate['certificate_state']}**  ",
        f"Applied design: `{certificate['decision']['applied_design_id']}`",
        "",
        "## Target stratum weights (frozen, not empirical)",
        "",
        "| stratum | integer quota | normalized | role |",
        "|---|---|---|---|",
    ]
    for item in certificate["strata"]:
        lines.append(
            f"| {item['stratum_id']} "
            f"| {item['integer_weight']} "
            f"| {item['normalized_weight']:.10f} "
            f"| {item['role']} |"
        )
    lines += [
        "",
        "## Collected evidence allocation",
        "",
        "| stratum | independent workloads | alpha consumed |",
        "|---|---|---|",
    ]
    for item in certificate["strata"]:
        lines.append(
            f"| {item['stratum_id']} "
            f"| {item['independent_workloads']} "
            f"| {item['alpha_consumed']} |"
        )
    success = certificate["overall_success_difference"]
    cost = certificate["overall_cost_saving"]
    points = certificate["point_thresholds"]
    lines += [
        "",
        f"Active independent workloads: {allocation['active_independent_workloads']}  ",
        f"Repetitions per workload: {repetitions} "
        "(averaged within the workload; they never add independent units)  ",
        f"Raw repetition sessions: {allocation['raw_repetition_sessions']}  ",
        "Structural-zero strata: "
        + (", ".join(allocation["structural_zero_strata"]) or "none")
        + " (exactly [0, 0]; consume no alpha)",
        "",
        "## Point thresholds versus the certificate",
        "",
        f"- weighted success point estimate: {success['point_estimate']} "
        f"(passes point threshold: {points['success_non_inferiority_point_passes']})",
        f"- weighted cost point estimate: {cost['point_estimate']} "
        f"(passes point threshold: {points['cost_improvement_point_passes']})",
        f"- weighted success bounds: [{success['lower_bound']}, "
        f"{success['upper_bound']}]",
        f"- weighted cost bounds: [{cost['lower_bound']}, "
        f"{cost['upper_bound']}]",
        "",
        "Point-threshold passage is not a certificate. The certificate state "
        "above is what the intervals can rule out.",
        "",
        "## Claim eligibility",
        "",
        f"- `eligible_for_scientific_claims`: "
        f"{str(document['eligible_for_scientific_claims']).lower()}",
        f"- `policy_selection_origin`: {document['policy_selection_origin']}",
        "- `confirmation_evidence_origin`: "
        f"{document['confirmation_evidence_origin']}",
        "",
        document["claim_eligibility_note"],
        "",
        document["hash_semantics"],
        "",
    ]
    return "\n".join(lines)
