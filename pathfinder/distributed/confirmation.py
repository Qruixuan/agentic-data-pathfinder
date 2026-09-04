"""Freeze a prospective confirmation plan for one post-hoc-selected policy.

The distributed restricted pilot produced a policy that *passes point
thresholds* but whose certificate is INSUFFICIENT_EVIDENCE. Passing a point
threshold is not a safety certificate: the point estimate is where the effect
landed once, the certificate is what the interval can rule out. Confusing the
two is exactly how a post-hoc observation becomes an unearned claim.

This module freezes what a future confirmation run would have to be, without
running it and without converting the existing evidence into anything it is
not. Its output records two separate things and never merges them:

* **policy-selection evidence** -- the already-inspected 36-workload pilot,
  which chose the policy and is therefore unusable for confirming it; and
* **future certification evidence** -- a fresh, disjoint cohort that does not
  exist yet.

Nothing here is a commit authorisation. The plan is an input to a future
confirmatory run, not a result.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable, Mapping


CONFIRMATION_PLAN_SCHEMA_VERSION = (
    "pathfinder.distributed-policy-confirmation-plan/v1alpha1"
)
CONFIRMATION_CONFIG_SCHEMA_VERSION = (
    "pathfinder.distributed-policy-confirmation-config/v1alpha1"
)
#: Files the source policy audit must bind before it may be planned against.
REQUIRED_AUDIT_FILES = (
    "policy_evaluation.json",
    "policy_manifest.json",
    "policy_summary.csv",
    "policy_workload_effects.csv",
    "report.md",
)
#: The one execution model this plan may freeze. A different runtime model is
#: a different experiment, not the same plan run elsewhere.
EXECUTION_MODEL_ID = "qwen3.8-27b"
SAFE_DESIGN_ID = "D_origin_remote"

#: Carried forward verbatim from the pilot thresholds. Declared here so a
#: mismatch against the source audit is a refusal rather than a silent
#: re-parameterisation.
REQUIRED_THRESHOLDS = {
    "delta_success_margin": 0.05,
    "minimum_cost_saving": 0.25,
    "alpha": 0.05,
}
REQUIRED_SUPPORTS = {
    "success_difference": (-1.0, 1.0),
    "cost_saving": (-2.0, 2.0),
}
#: Exactly one supported mode. The estimand is a stratified average using
#: weights fixed *externally* to the collected sample, so oversampling an
#: active stratum buys precision without moving the target. A second
#: "cohort composition defines the estimand" mode would be easy to conflate
#: with this one and is deliberately not offered.
ESTIMAND_KIND = "fixed_external_stratum_weights"
#: Where the target weights come from. They are the benchmark's own
#: outcome-blind stratum composition, not a post-hoc choice.
WEIGHTS_PROVENANCE = "outcome-blind-benchmark-selection-protocol"


class ConfirmationPlanError(ValueError):
    """Raised when a confirmation plan cannot be frozen safely."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise ConfirmationPlanError(message)


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and value.strip(),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _field(payload: Mapping[str, Any], key: str, name: str) -> Any:
    _require(key in payload, f"{name} is a required field")
    return payload[key]


def _string_set(value: Any, name: str) -> tuple[str, ...]:
    _require(isinstance(value, list) and value, f"{name} must be non-empty")
    items = tuple(
        _string(item, f"{name}[{index}]")
        for index, item in enumerate(value)
    )
    _require(len(set(items)) == len(items), f"{name} contains duplicates")
    return items


def _csv_text(rows: list[dict[str, Any]]) -> str:
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


def verify_audit_snapshot(source: Path) -> dict[str, str]:
    """Verify the policy audit's own SHA256SUMS before planning against it."""
    checksum_path = source / "SHA256SUMS"
    _require(
        checksum_path.is_file(),
        "policy audit SHA256SUMS is missing",
    )
    try:
        lines = checksum_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ConfirmationPlanError(
            "cannot read policy audit SHA256SUMS"
        ) from exc
    _require(bool(lines), "policy audit SHA256SUMS is empty")
    hashes: dict[str, str] = {}
    for line in lines:
        parts = line.split("  ", 1)
        _require(len(parts) == 2, "malformed policy audit SHA256SUMS line")
        digest, name = parts
        _require(
            len(digest) == 64
            and all(c in "0123456789abcdef" for c in digest),
            "malformed policy audit SHA256 digest",
        )
        _require(
            name and name not in hashes and Path(name).name == name,
            "policy audit SHA256SUMS contains an invalid filename",
        )
        path = source / name
        _require(path.is_file(), f"policy audit output is missing: {name}")
        _require(
            sha256(path.read_bytes()).hexdigest() == digest,
            f"policy audit checksum mismatch: {name}",
        )
        hashes[name] = digest
    for required in REQUIRED_AUDIT_FILES:
        _require(
            required in hashes,
            f"policy audit SHA256SUMS does not bind {required}",
        )
    return dict(sorted(hashes.items()))


@dataclass(frozen=True)
class StratumPlan:
    """One stratum's role in the frozen confirmation plan."""

    stratum_id: str
    policy_design_id: str
    safe_design_id: str
    #: ``active`` strata compare candidate against safe; ``structural_safe``
    #: strata apply the safe design, so the policy effect is exactly zero by
    #: construction rather than by measurement.
    role: str
    inspected_workload_count: int
    planned_workload_count: int
    weight: float

    @property
    def is_structural_safe(self) -> bool:
        return self.role == "structural_safe"

    @property
    def sessions_per_workload_block(self) -> int:
        """Sessions in one independent workload block, per repetition set.

        An active stratum needs both arms. A structural-safe stratum needs
        the safe arm only: scheduling a second identical safe execution and
        calling it a pair would manufacture a comparison that does not exist.
        """
        return 2 if self.role == "active" else 1

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "stratum_id": self.stratum_id,
            "policy_design_id": self.policy_design_id,
            "safe_design_id": self.safe_design_id,
            "role": self.role,
            "structural_zero_effect": self.is_structural_safe,
            "inspected_workload_count": self.inspected_workload_count,
            "planned_workload_count": self.planned_workload_count,
            "stratum_weight": self.weight,
            "sessions_per_workload_block": (
                self.sessions_per_workload_block
            ),
            "candidate_measurement_planned": not self.is_structural_safe,
        }


@dataclass(frozen=True)
class ConfirmationConfig:
    schema_version: str
    plan_id: str
    selected_policy_id: str
    policy_selection_provenance: str
    execution_model_id: str
    estimand_kind: str
    target_stratum_weights_integer: dict[str, int]
    weights_provenance: str
    minimum_independent_workloads_by_active_stratum: dict[str, int]
    minimum_independent_workloads_provenance: str
    planned_workloads_by_stratum: dict[str, int]
    repetitions: int
    source_path: Path
    source_sha256: str

    @property
    def weight_total(self) -> int:
        return sum(self.target_stratum_weights_integer.values())

    @property
    def normalized_weights(self) -> dict[str, float]:
        """Deterministic normalisation of the declared integer weights."""
        total = self.weight_total
        return {
            stratum_id: value / total
            for stratum_id, value in sorted(
                self.target_stratum_weights_integer.items()
            )
        }

    @property
    def stratum_weights(self) -> dict[str, float]:
        return self.normalized_weights


def load_confirmation_config(path: str | Path) -> ConfirmationConfig:
    """Load and validate a confirmation planning configuration."""
    source = Path(path).resolve()
    _require(
        source.is_file(),
        f"confirmation configuration does not exist: {source}",
    )
    raw_bytes = source.read_bytes()
    try:
        root = _mapping(json.loads(raw_bytes), "confirmation config")
    except json.JSONDecodeError as exc:
        raise ConfirmationPlanError(
            f"invalid confirmation configuration JSON: {source}"
        ) from exc

    schema_version = _string(
        _field(root, "schema_version", "schema_version"),
        "schema_version",
    )
    _require(
        schema_version == CONFIRMATION_CONFIG_SCHEMA_VERSION,
        f"unsupported confirmation schema_version: {schema_version}",
    )
    model = _string(
        _field(root, "execution_model_id", "execution_model_id"),
        "execution_model_id",
    )
    _require(
        model == EXECUTION_MODEL_ID,
        f"execution_model_id must be {EXECUTION_MODEL_ID!r}; a different "
        "runtime model is a different experiment",
    )
    estimand = _mapping(_field(root, "estimand", "estimand"), "estimand")
    kind = _string(_field(estimand, "kind", "estimand.kind"), "estimand.kind")
    _require(
        kind == ESTIMAND_KIND,
        f"estimand.kind must be {ESTIMAND_KIND!r}; this implementation "
        "supports exactly one estimand mode so the two cannot be conflated",
    )
    provenance = _string(
        estimand.get("weights_provenance", WEIGHTS_PROVENANCE),
        "estimand.weights_provenance",
    )
    raw_weights = _mapping(
        _field(
            estimand,
            "target_stratum_weights_integer",
            "estimand.target_stratum_weights_integer",
        ),
        "estimand.target_stratum_weights_integer",
    )
    _require(
        raw_weights,
        "estimand.target_stratum_weights_integer cannot be empty",
    )
    weights: dict[str, int] = {}
    for stratum_id, value in raw_weights.items():
        name = f"estimand.target_stratum_weights_integer.{stratum_id}"
        _require(
            isinstance(value, int) and not isinstance(value, bool),
            f"{name} must be an integer quota, not a rounded fraction",
        )
        _require(int(value) >= 0, f"{name} must be non-negative")
        weights[_string(stratum_id, "stratum id")] = int(value)
    _require(
        sum(weights.values()) > 0,
        "estimand.target_stratum_weights_integer must have a positive total",
    )

    raw_counts = _mapping(
        _field(root, "planned_workloads_by_stratum",
               "planned_workloads_by_stratum"),
        "planned_workloads_by_stratum",
    )
    counts: dict[str, int] = {}
    for stratum_id, value in raw_counts.items():
        name = f"planned_workloads_by_stratum.{stratum_id}"
        _require(
            isinstance(value, int)
            and not isinstance(value, bool)
            and value >= 0,
            f"{name} must be a non-negative integer",
        )
        counts[_string(stratum_id, "stratum id")] = int(value)
    _require(
        set(counts) == set(weights),
        "planned_workloads_by_stratum and "
        "estimand.target_stratum_weights_integer must cover exactly the "
        "same strata",
    )

    raw_minima = _mapping(
        _field(
            root,
            "minimum_independent_workloads_by_active_stratum",
            "minimum_independent_workloads_by_active_stratum",
        ),
        "minimum_independent_workloads_by_active_stratum",
    )
    _require(
        raw_minima,
        "minimum_independent_workloads_by_active_stratum cannot be empty",
    )
    minima: dict[str, int] = {}
    for stratum_id, value in raw_minima.items():
        name = (
            "minimum_independent_workloads_by_active_stratum."
            f"{stratum_id}"
        )
        _require(
            isinstance(value, int) and not isinstance(value, bool),
            f"{name} must be an integer, not a boolean or fraction",
        )
        _require(int(value) >= 1, f"{name} must be a positive integer")
        key = _string(stratum_id, "stratum id")
        _require(key not in minima, f"duplicate stratum in minima: {key}")
        minima[key] = int(value)
    minima_provenance = _string(
        root.get(
            "minimum_independent_workloads_provenance",
            "engineering-placeholder-not-scientifically-justified",
        ),
        "minimum_independent_workloads_provenance",
    )

    repetitions = _field(root, "repetitions", "repetitions")
    _require(
        isinstance(repetitions, int)
        and not isinstance(repetitions, bool)
        and repetitions >= 1,
        "repetitions must be a positive integer",
    )
    return ConfirmationConfig(
        schema_version=schema_version,
        plan_id=_string(_field(root, "plan_id", "plan_id"), "plan_id"),
        selected_policy_id=_string(
            _field(root, "selected_policy_id", "selected_policy_id"),
            "selected_policy_id",
        ),
        policy_selection_provenance=_string(
            root.get(
                "policy_selection_provenance",
                "selected-from-inspected-posthoc-pilot-evidence",
            ),
            "policy_selection_provenance",
        ),
        execution_model_id=model,
        estimand_kind=kind,
        target_stratum_weights_integer=weights,
        weights_provenance=provenance,
        minimum_independent_workloads_by_active_stratum=minima,
        minimum_independent_workloads_provenance=minima_provenance,
        planned_workloads_by_stratum=counts,
        repetitions=int(repetitions),
        source_path=source,
        source_sha256=sha256(raw_bytes).hexdigest(),
    )


def _inspected_identifiers(
    manifest_paths: Iterable[str | Path],
) -> tuple[set[str], set[str], dict[str, str]]:
    """Collect every already-inspected workload and object identifier."""
    workload_ids: set[str] = set()
    object_ids: set[str] = set()
    digests: dict[str, str] = {}
    for path in manifest_paths:
        resolved = Path(path).resolve()
        _require(
            resolved.is_file(),
            f"inspected workload manifest does not exist: {resolved}",
        )
        raw = resolved.read_bytes()
        digests[resolved.name] = sha256(raw).hexdigest()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConfirmationPlanError(
                f"invalid inspected workload manifest: {resolved}"
            ) from exc
        _require(
            isinstance(payload, Mapping),
            f"inspected workload manifest must be an object: {resolved}",
        )
        for workload_id, entry in payload.items():
            workload_ids.add(str(workload_id))
            if isinstance(entry, Mapping):
                for key in ("object_id", "video_id"):
                    value = entry.get(key)
                    if isinstance(value, str) and value.strip():
                        object_ids.add(value.strip())
    _require(
        workload_ids,
        "no inspected workload identifiers were supplied; disjointness "
        "cannot be established",
    )
    return workload_ids, object_ids, dict(sorted(digests.items()))


def _fresh_cohort(path: str | Path) -> tuple[
    dict[str, dict[str, Any]], str
]:
    resolved = Path(path).resolve()
    _require(
        resolved.is_file(),
        f"fresh cohort manifest does not exist: {resolved}",
    )
    raw = resolved.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfirmationPlanError(
            f"invalid fresh cohort manifest: {resolved}"
        ) from exc
    _require(
        isinstance(payload, Mapping) and payload,
        "fresh cohort manifest must be a non-empty object",
    )
    cohort: dict[str, dict[str, Any]] = {}
    for workload_id, entry in payload.items():
        name = f"fresh cohort entry {workload_id}"
        _require(isinstance(entry, Mapping), f"{name} must be an object")
        stratum_id = _string(
            _field(entry, "stratum_id", f"{name}.stratum_id"),
            f"{name}.stratum_id",
        )
        object_id = _string(
            _field(entry, "object_id", f"{name}.object_id"),
            f"{name}.object_id",
        )
        cohort[_string(workload_id, "fresh workload id")] = {
            "stratum_id": stratum_id,
            "object_id": object_id,
        }
    return cohort, sha256(raw).hexdigest()


def freeze_confirmation_plan(
    config_path: str | Path,
    policy_audit_dir: str | Path,
    *,
    inspected_workload_manifests: Iterable[str | Path],
    fresh_cohort_manifest: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze a confirmation plan for one policy, offline and atomically."""
    config = load_confirmation_config(config_path)
    source = Path(policy_audit_dir).resolve()
    _require(
        source.is_dir(),
        f"policy audit directory does not exist: {source}",
    )
    audit_hashes = verify_audit_snapshot(source)

    evaluation_bytes = (source / "policy_evaluation.json").read_bytes()
    evaluation = json.loads(evaluation_bytes)
    _require(
        evaluation.get("posthoc") is True
        and evaluation.get("eligible_for_scientific_claims") is False,
        "the source policy audit must be post-hoc and ineligible for "
        "scientific claims; a plan cannot be frozen from anything else",
    )
    _require(
        evaluation.get("complete_design_oracle") is False,
        "the source audit must declare an incomplete design Oracle; this "
        "pilot observes one candidate per workload only",
    )

    thresholds = _mapping(evaluation.get("thresholds"), "audit thresholds")
    for key, expected in REQUIRED_THRESHOLDS.items():
        _require(
            float(thresholds.get(key, float("nan"))) == expected,
            f"source audit {key} is not {expected}; thresholds must be "
            "carried forward unchanged",
        )

    details = evaluation.get("policy_details")
    _require(isinstance(details, list) and details, "audit has no policies")
    selected = [
        policy for policy in details
        if policy.get("policy_id") == config.selected_policy_id
    ]
    _require(
        len(selected) == 1,
        f"exactly one policy must match {config.selected_policy_id!r}; "
        f"found {len(selected)}",
    )
    policy = selected[0]

    supports = {
        "success_difference": (
            float(policy["gates"][0]["support_lower"]),
            float(policy["gates"][0]["support_upper"]),
        ),
        "cost_saving": (
            float(policy["cost_saving"]["support_lower"]),
            float(policy["cost_saving"]["support_upper"]),
        ),
    }
    for key, expected_support in REQUIRED_SUPPORTS.items():
        _require(
            supports[key] == expected_support,
            f"source audit {key} support is {supports[key]}, expected "
            f"{expected_support}",
        )

    assignment = _mapping(
        policy.get("assignment_by_stratum"),
        "assignment_by_stratum",
    )
    _require(
        set(assignment) == set(config.stratum_weights),
        "the declared strata must exactly match the selected policy's "
        f"strata {sorted(assignment)}",
    )

    workload_ids, object_ids, inspected_digests = _inspected_identifiers(
        inspected_workload_manifests
    )
    cohort, cohort_sha256 = _fresh_cohort(fresh_cohort_manifest)

    reused_workloads = sorted(set(cohort) & workload_ids)
    _require(
        not reused_workloads,
        "the fresh cohort reuses already-inspected workload IDs, which "
        "would confirm a policy on the evidence that selected it: "
        + ", ".join(reused_workloads[:5]),
    )
    reused_objects = sorted(
        {entry["object_id"] for entry in cohort.values()} & object_ids
    )
    _require(
        not reused_objects,
        "the fresh cohort reuses already-inspected object/video IDs: "
        + ", ".join(reused_objects[:5]),
    )

    cohort_by_stratum: dict[str, int] = {}
    for entry in cohort.values():
        cohort_by_stratum[entry["stratum_id"]] = (
            cohort_by_stratum.get(entry["stratum_id"], 0) + 1
        )
    _require(
        set(cohort_by_stratum) <= set(config.stratum_weights),
        "the fresh cohort contains strata the plan does not declare: "
        + ", ".join(sorted(set(cohort_by_stratum) - set(
            config.stratum_weights
        ))),
    )
    for stratum_id, planned in config.planned_workloads_by_stratum.items():
        actual = cohort_by_stratum.get(stratum_id, 0)
        _require(
            actual == planned,
            f"stratum {stratum_id} plans {planned} workloads but the fresh "
            f"cohort supplies {actual}",
        )

    inspected_by_stratum = _inspected_counts(source, config)
    strata = []
    for stratum_id in sorted(config.stratum_weights):
        role_token = str(assignment[stratum_id])
        policy_design = (
            SAFE_DESIGN_ID
            if role_token == "safe"
            else str(policy.get("design_by_stratum", {}).get(
                stratum_id,
                _design_for_stratum(source, config.selected_policy_id,
                                    stratum_id),
            ))
        )
        role = (
            "structural_safe"
            if policy_design == SAFE_DESIGN_ID
            else "active"
        )
        strata.append(StratumPlan(
            stratum_id=stratum_id,
            policy_design_id=policy_design,
            safe_design_id=SAFE_DESIGN_ID,
            role=role,
            inspected_workload_count=inspected_by_stratum.get(
                stratum_id, 0
            ),
            planned_workload_count=(
                config.planned_workloads_by_stratum[stratum_id]
            ),
            weight=config.stratum_weights[stratum_id],
        ))

    active = [item for item in strata if item.role == "active"]
    active_ids = {item.stratum_id for item in active}
    structural_ids = {
        item.stratum_id for item in strata if item.is_structural_safe
    }
    declared_minima = set(config.minimum_independent_workloads_by_active_stratum)
    _require(
        declared_minima == active_ids,
        "minimum_independent_workloads_by_active_stratum must name exactly "
        f"the active strata {sorted(active_ids)}; got "
        f"{sorted(declared_minima)}",
    )
    _require(
        not (declared_minima & structural_ids),
        "a structural-safe stratum cannot carry an independent-workload "
        "minimum: " + ", ".join(sorted(declared_minima & structural_ids)),
    )
    _require(
        active,
        "every stratum applies the safe design, so there is nothing to "
        "confirm; this plan would measure a structurally zero effect",
    )
    for item in strata:
        if item.is_structural_safe:
            _require(
                item.planned_workload_count >= 0,
                "structural-safe strata may not plan negative workloads",
            )

    plan = _plan_document(
        config,
        policy=policy,
        strata=strata,
        audit_hashes=audit_hashes,
        evaluation=evaluation,
        cohort_sha256=cohort_sha256,
        cohort=cohort,
        inspected_digests=inspected_digests,
        inspected_workload_count=len(workload_ids),
        fresh_workload_count=len(cohort),
    )
    return _publish(
        plan,
        strata,
        config=config,
        source=source,
        output_dir=output_dir,
        cohort_sha256=cohort_sha256,
        inspected_digests=inspected_digests,
        audit_hashes=audit_hashes,
        audit_id=str(evaluation.get("audit_id") or "unknown"),
    )


def _design_for_stratum(
    source: Path,
    policy_id: str,
    stratum_id: str,
) -> str:
    """Recover the policy's design for a stratum from the effects CSV."""
    path = source / "policy_workload_effects.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if (
                row.get("policy_id") == policy_id
                and row.get("stratum_id") == stratum_id
            ):
                return str(row["selected_design_id"])
    raise ConfirmationPlanError(
        f"no rows for policy {policy_id!r} stratum {stratum_id!r}"
    )


def _inspected_counts(
    source: Path,
    config: ConfirmationConfig,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    path = source / "policy_workload_effects.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("policy_id") != config.selected_policy_id:
                continue
            stratum_id = str(row["stratum_id"])
            counts[stratum_id] = counts.get(stratum_id, 0) + 1
    return counts


def _plan_document(
    config: ConfirmationConfig,
    *,
    policy: Mapping[str, Any],
    strata: list[StratumPlan],
    audit_hashes: Mapping[str, str],
    evaluation: Mapping[str, Any],
    cohort_sha256: str,
    cohort: Mapping[str, Mapping[str, Any]],
    inspected_digests: Mapping[str, str],
    inspected_workload_count: int,
    fresh_workload_count: int,
) -> dict[str, Any]:
    """Build the reproducible plan document. No absolute paths may appear."""
    active = [item for item in strata if item.role == "active"]
    return {
        "schema_version": CONFIRMATION_PLAN_SCHEMA_VERSION,
        "plan_id": config.plan_id,
        "selected_policy_id": config.selected_policy_id,
        "execution_model_id": config.execution_model_id,
        "safe_design_id": SAFE_DESIGN_ID,
        "policy_selection": {
            "provenance": config.policy_selection_provenance,
            "selected_from_inspected_evidence": True,
            "selection_evidence_pilot_id": evaluation.get("pilot_id"),
            "selection_evidence_workloads": inspected_workload_count,
            "selection_evidence_certificate_state": (
                policy.get("certificate_state")
            ),
            "note": (
                "This policy was chosen after inspecting the pilot's "
                "outcomes. The pilot is therefore selection evidence and "
                "can never also be its confirmation evidence."
            ),
        },
        "future_certification_evidence": {
            "status": "NOT_YET_COLLECTED",
            "fresh_workload_count": fresh_workload_count,
            "fresh_cohort_sha256": cohort_sha256,
            "disjoint_from_inspected_workload_ids": True,
            "disjoint_from_inspected_object_ids": True,
        },
        # Enumerated so a later certificate can check collected evidence
        # against exactly the cohort that was frozen, not merely its count.
        "fresh_cohort": {
            workload_id: {
                "stratum_id": entry["stratum_id"],
                "object_id": entry["object_id"],
            }
            for workload_id, entry in sorted(cohort.items())
        },
        "thresholds": dict(REQUIRED_THRESHOLDS),
        "supports": {
            "success_difference": list(
                REQUIRED_SUPPORTS["success_difference"]
            ),
            "cost_saving": list(REQUIRED_SUPPORTS["cost_saving"]),
        },
        "estimand": {
            "kind": config.estimand_kind,
            "target_stratum_weights_integer": dict(sorted(
                config.target_stratum_weights_integer.items()
            )),
            "target_stratum_weights_normalized": (
                config.normalized_weights
            ),
            "target_stratum_weight_total": config.weight_total,
            "weights_provenance": config.weights_provenance,
            "aggregation_rule": (
                "The certificate must aggregate stratum effects using these "
                "frozen weights, NOT the empirical sample proportions of "
                "whatever cohort is collected."
            ),
            "note": (
                "Weights are fixed externally to the collected sample, so "
                "oversampling an active stratum buys precision without "
                "moving the target. Changing them defines a different "
                "quantity, a different plan hash, and requires an explicit "
                "operator decision."
            ),
        },
        "collection_allocation": {
            "planned_workloads_by_stratum": dict(sorted(
                config.planned_workloads_by_stratum.items()
            )),
            "allocation_proportions": _allocation_proportions(
                config.planned_workloads_by_stratum
            ),
            "matches_target_weights": _allocation_matches_target(
                config.planned_workloads_by_stratum,
                config.normalized_weights,
            ),
            "note": (
                "Collection allocation is how many workloads are gathered "
                "per stratum. It is a precision decision and is allowed to "
                "differ from the target weights above, which define the "
                "estimand."
            ),
        },
        "repetitions": config.repetitions,
        "minimum_independent_workloads_by_active_stratum": dict(sorted(
            config.minimum_independent_workloads_by_active_stratum.items()
        )),
        "minimum_independent_workloads_provenance": (
            config.minimum_independent_workloads_provenance
        ),
        "minimum_independent_workloads_note": (
            "Frozen per-active-stratum floors. Repetitions do not count "
            "toward them and structural-safe strata have none. A stratum "
            "below its floor yields INSUFFICIENT_EVIDENCE even when both "
            "numerical bounds pass."
        ),
        "independent_unit": "workload-object-cluster",
        "repetitions_increase_independent_units": False,
        "strata": [item.to_public_dict() for item in strata],
        "active_stratum_ids": [item.stratum_id for item in active],
        "structural_safe_stratum_ids": [
            item.stratum_id for item in strata if item.is_structural_safe
        ],
        "planned_independent_workloads": sum(
            item.planned_workload_count for item in strata
        ),
        "planned_active_workloads": sum(
            item.planned_workload_count for item in active
        ),
        "planned_sessions": sum(
            item.planned_workload_count
            * item.sessions_per_workload_block
            * config.repetitions
            for item in strata
        ),
        "source_policy_audit": {
            "audit_id": evaluation.get("audit_id"),
            "schema_version": evaluation.get("schema_version"),
            "file_sha256": dict(audit_hashes),
        },
        "inspected_workload_manifest_sha256": dict(inspected_digests),
        "future_certificate_requirements": {
            "policy_identity_must_match": True,
            "execution_model_must_match": EXECUTION_MODEL_ID,
            "thresholds_and_supports_must_match": True,
            "workloads_must_be_disjoint_from_selection_evidence": True,
            "active_strata_require_complete_safe_and_candidate_blocks": True,
            "telemetry_complete_must_be_literally_true": True,
            "artifact_delivery_complete_must_be_literally_true": True,
            "stratum_weights_must_match": True,
            "policy_may_not_change_after_freezing": True,
            "fallback_design_id": SAFE_DESIGN_ID,
            "every_non_safe_certificate_result_retains_fallback": True,
        },
        "posthoc_selection": True,
        "eligible_for_scientific_claims": False,
        "authorises_commit": False,
        "offline": True,
        "credentials_recorded": False,
        "limitations": [
            "This is a plan, not evidence. It authorises no commit.",
            "The selecting pilot passed point thresholds but returned "
            "INSUFFICIENT_EVIDENCE; a point estimate on the correct side of "
            "a threshold is not a certificate.",
            "Structural-safe strata contribute exactly zero policy effect "
            "by construction and are never counted as candidate evidence.",
        ],
    }


def _allocation_proportions(
    counts: Mapping[str, int],
) -> dict[str, float]:
    total = sum(counts.values())
    if total <= 0:
        return {stratum_id: 0.0 for stratum_id in sorted(counts)}
    return {
        stratum_id: counts[stratum_id] / total
        for stratum_id in sorted(counts)
    }


def _allocation_matches_target(
    counts: Mapping[str, int],
    normalized: Mapping[str, float],
) -> bool:
    proportions = _allocation_proportions(counts)
    return all(
        abs(proportions[stratum_id] - normalized.get(stratum_id, 0.0))
        < 1e-9
        for stratum_id in proportions
    )


def _publish(
    plan: Mapping[str, Any],
    strata: list[StratumPlan],
    *,
    config: ConfirmationConfig,
    source: Path,
    output_dir: str | Path,
    cohort_sha256: str,
    inspected_digests: Mapping[str, str],
    audit_hashes: Mapping[str, str],
    audit_id: str,
) -> dict[str, Any]:
    target = Path(output_dir).resolve()
    documents: dict[str, str] = {
        "confirmation_plan.json": json.dumps(
            plan, indent=2, sort_keys=True, ensure_ascii=False
        ) + "\n",
        "confirmation_strata.csv": _csv_text([
            item.to_public_dict() for item in strata
        ]),
    }
    plan_sha256 = sha256(
        documents["confirmation_plan.json"].encode("utf-8")
    ).hexdigest()
    # No absolute path appears in ANY checksummed output, manifest
    # included. Inputs are identified by logical role, stable filename, and
    # content hash, so the whole output tree is byte-identical across
    # machines and directory layouts. Operator-local paths are printed to
    # the console instead.
    documents["confirmation_manifest.json"] = json.dumps(
        {
            "schema_version": CONFIRMATION_PLAN_SCHEMA_VERSION,
            "plan_id": config.plan_id,
            "plan_sha256": plan_sha256,
            "inputs": {
                "confirmation_config": {
                    "role": "confirmation-planning-config",
                    "sha256": config.source_sha256,
                },
                "policy_audit": {
                    "role": "posthoc-policy-selection-evidence",
                    "audit_id": audit_id,
                    "file_sha256": dict(audit_hashes),
                },
                "fresh_cohort_manifest": {
                    "role": "future-certification-cohort",
                    "sha256": cohort_sha256,
                },
                "inspected_workload_manifests": {
                    "role": "already-inspected-selection-evidence",
                    "file_sha256": dict(inspected_digests),
                },
            },
            "portable": True,
            "absolute_paths_recorded": False,
            "offline": True,
            "deployment_mutations_performed": False,
            "credentials_recorded": False,
            "authorises_commit": False,
        },
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
    ) + "\n"
    documents["SHA256SUMS"] = "".join(
        f"{sha256(content.encode('utf-8')).hexdigest()}  {name}\n"
        for name, content in sorted(documents.items())
    )

    _require(
        sha256((source / "SHA256SUMS").read_bytes()).hexdigest()
        == sha256((source / "SHA256SUMS").read_bytes()).hexdigest(),
        "policy audit changed during planning",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(
        prefix=".pathfinder-confirmation-",
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
            f"confirmation plan output directory already exists: {target}",
        )
        staging.rename(target)
    return {
        "status": "FROZEN",
        "plan_id": config.plan_id,
        "plan_sha256": plan_sha256,
        "selected_policy_id": config.selected_policy_id,
        "execution_model_id": config.execution_model_id,
        "planned_independent_workloads": plan[
            "planned_independent_workloads"
        ],
        "planned_active_workloads": plan["planned_active_workloads"],
        "planned_sessions": plan["planned_sessions"],
        "active_stratum_ids": list(plan["active_stratum_ids"]),
        "structural_safe_stratum_ids": list(
            plan["structural_safe_stratum_ids"]
        ),
        "posthoc_selection": True,
        "eligible_for_scientific_claims": False,
        "authorises_commit": False,
        "console_only_paths": {
            "output_dir": str(target),
            "policy_audit_dir": str(source),
            "config_path": str(config.source_path),
        },
    }
