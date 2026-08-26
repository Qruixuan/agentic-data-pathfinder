"""Risk-constrained, workload-aware safety certificate for AWM v3alpha5.

The certificate compares a *restricted* candidate policy against the safe
origin design with two separate one-sided gates and commits only when both
gates pass. Workload objects are the independent clusters; the complete
repetition block inside a workload is a repeated measurement and is averaged
before any bound is computed, so duplicating repetitions can never inflate the
independent sample size.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from statistics import mean
from typing import Any, Mapping

from ..config import load_config
from ..integrations.flowmesh.analysis import audit_pilot_records
from ..integrations.flowmesh.pilot import (
    _write_csv,
    _write_json,
    load_flowmesh_pilot_config,
    load_pilot_records,
)
from ..reduced_oracle.contracts import (
    ReducedOracleConfig,
    load_reduced_oracle_config,
)
from ..reduced_oracle.snapshot import reduced_oracle_snapshot
from .contracts import AWMConfigError
from .model import Interval, one_sided_bounded_mean_bounds


SAFETY_CERTIFICATE_CONFIG_SCHEMA_VERSION = (
    "pathfinder.awm-safety-certificate/v1alpha1"
)
SAFETY_CERTIFICATE_EVALUATION_SCHEMA_VERSION = (
    "pathfinder.awm-safety-certificate-evaluation/v1alpha1"
)
CERTIFICATE_MODES = ("posthoc", "confirmatory")
FAMILY_ADJUSTMENTS = ("bonferroni",)
BOUND_METHODS = ("workload-cluster-one-sided-bounded-mean-kl",)
POSTHOC_THRESHOLD_PROVENANCE = "posthoc-engineering-placeholder"
PREREGISTERED_PROVENANCE = "preregistered-before-evaluation-outcomes"
POSTHOC_RESTRICTION_PROVENANCE = "posthoc-selected-on-inspected-oracle"
THRESHOLD_PROVENANCES = (
    POSTHOC_THRESHOLD_PROVENANCE,
    PREREGISTERED_PROVENANCE,
)
CANDIDATE_RESTRICTION_PROVENANCES = (
    POSTHOC_RESTRICTION_PROVENANCE,
    PREREGISTERED_PROVENANCE,
)
ESTIMAND_TARGETS = (
    "finite-supplied-workload-set",
    "superpopulation-of-future-workloads",
)
UNDECLARED = "none-declared"
CONFIRMATORY_STATIC_REQUIREMENTS = (
    "mode_is_confirmatory",
    "posthoc_flag_cleared",
    "preregistration_declared_before_evaluation_outcomes",
    "thresholds_preregistered",
    "candidate_restriction_preregistered",
    "workload_selection_rule_preregistered",
    "selection_and_certification_sets_nonempty_and_disjoint",
    "no_posthoc_provenance_flags",
)
CONFIRMATORY_RUNTIME_REQUIREMENTS = (
    "oracle_snapshot_previously_unseen",
)
GATE_IDS = ("success_non_inferiority", "cost_improvement")
GATE_SIDES = ("lower", "upper")
GATE_RESULTS = ("PASS", "VIOLATED", "INDETERMINATE")
CERTIFICATE_STATES = (
    "SAFE_TO_COMMIT",
    "UNSAFE",
    "INSUFFICIENT_EVIDENCE",
)
_SUPPORT_TOLERANCE = 1e-9


@dataclass(frozen=True)
class CertificateStratum:
    """One tested stratum of the restricted, workload-aware policy."""

    stratum_id: str
    candidate_design_id: str
    workload_ids: tuple[str, ...]


@dataclass(frozen=True)
class WorkloadSafetyCertificateConfig:
    schema_version: str
    certificate_id: str
    source_path: Path
    source_sha256: str
    mode: str
    posthoc: bool
    eligible_for_scientific_claims: bool
    safe_design_id: str
    design_ids: tuple[str, ...]
    excluded_design_ids: tuple[str, ...]
    strata: tuple[CertificateStratum, ...]
    selection_workload_ids: tuple[str, ...]
    certificate_workload_ids: tuple[str, ...]
    alpha: float
    family_adjustment: str
    bound_method: str
    minimum_independent_workloads: int
    delta_success_margin: float
    minimum_cost_saving: float
    threshold_provenance: str
    candidate_restriction_provenance: str
    candidate_restriction_note: str
    success_difference_support: Interval
    cost_saving_support: Interval
    preregistration_declared_before_evaluation_outcomes: bool
    preregistration_declaration_id: str
    preregistration_declaration_sha256: str
    inspected_oracle_snapshot_sha256: tuple[str, ...]
    estimand_target: str
    workload_sampling_mechanism: str
    workload_selection_rule: str
    confirmatory_static_requirements: tuple[tuple[str, bool], ...]

    @property
    def confirmatory_static_requirements_met(self) -> bool:
        return all(met for _, met in self.confirmatory_static_requirements)

    @property
    def unmet_confirmatory_static_requirements(self) -> tuple[str, ...]:
        return tuple(
            name
            for name, met in self.confirmatory_static_requirements
            if not met
        )

    @property
    def generalizes_beyond_the_supplied_workloads(self) -> bool:
        return self.estimand_target == "superpopulation-of-future-workloads"

    @property
    def analysed_design_ids(self) -> tuple[str, ...]:
        ordered = [self.safe_design_id]
        for stratum in self.strata:
            if stratum.candidate_design_id not in ordered:
                ordered.append(stratum.candidate_design_id)
        return tuple(ordered)

    @property
    def family_size(self) -> int:
        return len(self.strata) * len(GATE_IDS) * len(GATE_SIDES)

    @property
    def adjusted_alpha(self) -> float:
        return self.alpha / self.family_size


@dataclass(frozen=True)
class _Response:
    success: float
    service_cost: float


@dataclass(frozen=True)
class _ClusterObservation:
    workload_id: str
    success_difference: float
    cost_saving: float
    utility_gain: float


@dataclass(frozen=True)
class _BoundedMean:
    point_estimate: float
    lower_bound: float
    upper_bound: float
    normalized_point_estimate: float
    normalized_lower_bound: float
    normalized_upper_bound: float
    support: Interval


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AWMConfigError(f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AWMConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _boolean(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise AWMConfigError(f"{name} must be a boolean")
    return value


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AWMConfigError(f"{name} must be a positive integer")
    return value


def _finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AWMConfigError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise AWMConfigError(f"{name} must be finite")
    return result


def _nonnegative_number(value: Any, name: str) -> float:
    result = _finite_number(value, name)
    if result < 0.0:
        raise AWMConfigError(f"{name} must be non-negative")
    return result


def _string_array(
    value: Any,
    name: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise AWMConfigError(f"{name} must be a non-empty array")
    result = tuple(
        _string(item, f"{name}[{index}]")
        for index, item in enumerate(value)
    )
    if len(set(result)) != len(result):
        raise AWMConfigError(f"{name} cannot contain duplicates")
    return result


def _required_choice(value: Any, name: str, choices: tuple[str, ...]) -> str:
    result = _string(value, name)
    if result not in choices:
        raise AWMConfigError(
            f"{name} must be one of {', '.join(choices)}; got {result}"
        )
    return result


def _support(value: Any, name: str) -> Interval:
    payload = _mapping(value, name)
    lower = _finite_number(payload.get("lower"), f"{name}.lower")
    upper = _finite_number(payload.get("upper"), f"{name}.upper")
    if upper <= lower:
        raise AWMConfigError(f"{name} must have a positive width")
    return Interval(lower, upper)


def _require_present(payload: Mapping[str, Any], key: str, name: str) -> Any:
    if key not in payload:
        raise AWMConfigError(f"{name} is a required configuration field")
    return payload[key]


def load_workload_safety_certificate_config(
    path: str | Path,
) -> WorkloadSafetyCertificateConfig:
    """Load and structurally validate one safety-certificate configuration."""
    source = Path(path).resolve()
    if not source.is_file():
        raise AWMConfigError(
            f"AWM safety certificate configuration does not exist: {source}"
        )
    raw_bytes = source.read_bytes()
    try:
        root = _mapping(json.loads(raw_bytes), "certificate config")
    except json.JSONDecodeError as exc:
        raise AWMConfigError(
            f"invalid AWM safety certificate JSON: {source}"
        ) from exc

    schema_version = _string(root.get("schema_version"), "schema_version")
    if schema_version != SAFETY_CERTIFICATE_CONFIG_SCHEMA_VERSION:
        raise AWMConfigError(
            "unsupported AWM safety certificate schema_version: "
            + schema_version
        )

    mode = _required_choice(root.get("mode"), "mode", CERTIFICATE_MODES)
    posthoc = _boolean(root.get("posthoc"), "posthoc")
    eligible = _boolean(
        root.get("eligible_for_scientific_claims"),
        "eligible_for_scientific_claims",
    )

    design_ids = _string_array(root.get("design_ids"), "design_ids")
    safe_design_id = _string(root.get("safe_design_id"), "safe_design_id")
    if safe_design_id not in design_ids:
        raise AWMConfigError("safe_design_id must appear in design_ids")
    excluded_design_ids = _string_array(
        root.get("excluded_design_ids", []),
        "excluded_design_ids",
        allow_empty=True,
    )
    unknown_excluded = set(excluded_design_ids) - set(design_ids)
    if unknown_excluded:
        raise AWMConfigError(
            "excluded_design_ids must appear in design_ids: "
            + ", ".join(sorted(unknown_excluded))
        )
    if safe_design_id in excluded_design_ids:
        raise AWMConfigError("safe_design_id cannot be excluded")

    raw_strata = _mapping(root.get("strata"), "strata")
    if not raw_strata:
        raise AWMConfigError("strata cannot be empty")
    strata: list[CertificateStratum] = []
    stratum_workloads: set[str] = set()
    for raw_stratum_id in sorted(raw_strata):
        stratum_id = _string(raw_stratum_id, "stratum id")
        payload = _mapping(raw_strata[raw_stratum_id], f"strata.{stratum_id}")
        candidate_design_id = _string(
            payload.get("candidate_design_id"),
            f"strata.{stratum_id}.candidate_design_id",
        )
        if candidate_design_id not in design_ids:
            raise AWMConfigError(
                f"strata.{stratum_id}.candidate_design_id must appear in "
                "design_ids"
            )
        if candidate_design_id == safe_design_id:
            raise AWMConfigError(
                f"strata.{stratum_id}.candidate_design_id cannot be the safe "
                "design"
            )
        if candidate_design_id in excluded_design_ids:
            raise AWMConfigError(
                f"strata.{stratum_id}.candidate_design_id is excluded from "
                f"the restricted search: {candidate_design_id}"
            )
        workload_ids = _string_array(
            payload.get("workload_ids"),
            f"strata.{stratum_id}.workload_ids",
        )
        overlap = stratum_workloads.intersection(workload_ids)
        if overlap:
            raise AWMConfigError(
                "strata cannot overlap: " + ", ".join(sorted(overlap))
            )
        stratum_workloads.update(workload_ids)
        strata.append(CertificateStratum(
            stratum_id=stratum_id,
            candidate_design_id=candidate_design_id,
            workload_ids=workload_ids,
        ))

    split = _mapping(root.get("workload_split"), "workload_split")
    selection_workload_ids = _string_array(
        split.get("selection_workload_ids", []),
        "workload_split.selection_workload_ids",
        allow_empty=True,
    )
    certificate_workload_ids = _string_array(
        split.get("certificate_workload_ids"),
        "workload_split.certificate_workload_ids",
    )
    leaked = set(selection_workload_ids).intersection(certificate_workload_ids)
    if leaked:
        raise AWMConfigError(
            "target-workload leakage: selection_workload_ids and "
            "certificate_workload_ids must be disjoint; shared="
            + ", ".join(sorted(leaked))
        )
    stratum_leak = set(selection_workload_ids).intersection(stratum_workloads)
    if stratum_leak:
        raise AWMConfigError(
            "target-workload leakage: a selection workload cannot be "
            "certified; shared=" + ", ".join(sorted(stratum_leak))
        )
    if stratum_workloads != set(certificate_workload_ids):
        missing = set(certificate_workload_ids) - stratum_workloads
        extra = stratum_workloads - set(certificate_workload_ids)
        raise AWMConfigError(
            "strata must partition certificate_workload_ids; "
            f"missing={sorted(missing)}, extra={sorted(extra)}"
        )

    confidence = _mapping(root.get("confidence"), "confidence")
    alpha = _finite_number(
        _require_present(confidence, "alpha", "confidence.alpha"),
        "confidence.alpha",
    )
    if not 0.0 < alpha < 1.0:
        raise AWMConfigError("confidence.alpha must be in (0, 1)")
    family_adjustment = _required_choice(
        confidence.get("family_adjustment"),
        "confidence.family_adjustment",
        FAMILY_ADJUSTMENTS,
    )
    bound_method = _required_choice(
        confidence.get("bound_method"),
        "confidence.bound_method",
        BOUND_METHODS,
    )
    minimum_independent_workloads = _positive_integer(
        confidence.get("minimum_independent_workloads"),
        "confidence.minimum_independent_workloads",
    )

    thresholds = _mapping(root.get("thresholds"), "thresholds")
    delta_success_margin = _nonnegative_number(
        _require_present(
            thresholds,
            "delta_success_margin",
            "thresholds.delta_success_margin",
        ),
        "thresholds.delta_success_margin",
    )
    minimum_cost_saving = _finite_number(
        _require_present(
            thresholds,
            "minimum_cost_saving",
            "thresholds.minimum_cost_saving",
        ),
        "thresholds.minimum_cost_saving",
    )
    threshold_provenance = _required_choice(
        thresholds.get("provenance"),
        "thresholds.provenance",
        THRESHOLD_PROVENANCES,
    )

    supports = _mapping(root.get("supports"), "supports")
    success_difference_support = _support(
        supports.get("success_difference"),
        "supports.success_difference",
    )
    cost_saving_support = _support(
        supports.get("cost_saving"),
        "supports.cost_saving",
    )

    restriction = _mapping(
        root.get("candidate_restriction"),
        "candidate_restriction",
    )
    candidate_restriction_provenance = _required_choice(
        restriction.get("provenance"),
        "candidate_restriction.provenance",
        CANDIDATE_RESTRICTION_PROVENANCES,
    )
    candidate_restriction_note = _string(
        restriction.get("note"),
        "candidate_restriction.note",
    )

    preregistration = _mapping(
        root.get("preregistration"),
        "preregistration",
    )
    declared_before = _boolean(
        preregistration.get("declared_before_evaluation_outcomes"),
        "preregistration.declared_before_evaluation_outcomes",
    )
    declaration_id = _string(
        preregistration.get("declaration_id"),
        "preregistration.declaration_id",
    )
    declaration_sha256 = _string(
        preregistration.get("declaration_sha256"),
        "preregistration.declaration_sha256",
    )
    inspected_snapshots = _string_array(
        preregistration.get("inspected_oracle_snapshot_sha256", []),
        "preregistration.inspected_oracle_snapshot_sha256",
        allow_empty=True,
    )

    estimand = _mapping(root.get("estimand"), "estimand")
    estimand_target = _required_choice(
        _require_present(estimand, "target", "estimand.target"),
        "estimand.target",
        ESTIMAND_TARGETS,
    )
    workload_sampling_mechanism = _string(
        _require_present(
            estimand,
            "workload_sampling_mechanism",
            "estimand.workload_sampling_mechanism",
        ),
        "estimand.workload_sampling_mechanism",
    )
    workload_selection_rule = _string(
        _require_present(
            estimand,
            "workload_selection_rule",
            "estimand.workload_selection_rule",
        ),
        "estimand.workload_selection_rule",
    )

    requirements: tuple[tuple[str, bool], ...] = (
        ("mode_is_confirmatory", mode == "confirmatory"),
        ("posthoc_flag_cleared", not posthoc),
        (
            "preregistration_declared_before_evaluation_outcomes",
            declared_before,
        ),
        (
            "thresholds_preregistered",
            threshold_provenance == PREREGISTERED_PROVENANCE,
        ),
        (
            "candidate_restriction_preregistered",
            candidate_restriction_provenance == PREREGISTERED_PROVENANCE,
        ),
        (
            "workload_selection_rule_preregistered",
            workload_selection_rule != UNDECLARED,
        ),
        (
            "selection_and_certification_sets_nonempty_and_disjoint",
            bool(selection_workload_ids)
            and bool(certificate_workload_ids)
            and not set(selection_workload_ids).intersection(
                certificate_workload_ids
            ),
        ),
        (
            "no_posthoc_provenance_flags",
            threshold_provenance != POSTHOC_THRESHOLD_PROVENANCE
            and candidate_restriction_provenance
            != POSTHOC_RESTRICTION_PROVENANCE,
        ),
    )
    unmet = tuple(name for name, met in requirements if not met)

    if (
        estimand_target == "superpopulation-of-future-workloads"
        and (
            workload_sampling_mechanism == UNDECLARED
            or workload_selection_rule == UNDECLARED
        )
    ):
        raise AWMConfigError(
            "a superpopulation estimand requires a declared "
            "estimand.workload_sampling_mechanism and "
            "estimand.workload_selection_rule; without one the certificate "
            "cannot claim generalization beyond the supplied workloads"
        )

    if mode == "posthoc":
        if not posthoc or eligible:
            raise AWMConfigError(
                "a post-hoc safety certificate must declare posthoc=true and "
                "eligible_for_scientific_claims=false"
            )
        if declared_before:
            raise AWMConfigError(
                "posthoc mode cannot declare "
                "preregistration.declared_before_evaluation_outcomes=true"
            )
        if threshold_provenance == PREREGISTERED_PROVENANCE:
            raise AWMConfigError(
                "posthoc mode cannot declare preregistered thresholds"
            )
        if estimand_target != "finite-supplied-workload-set":
            raise AWMConfigError(
                "posthoc mode must declare "
                "estimand.target=finite-supplied-workload-set; a post-hoc "
                "certificate cannot claim a superpopulation estimand"
            )
    else:
        if unmet:
            raise AWMConfigError(
                "confirmatory mode requires every preregistration "
                "requirement; unmet=" + ", ".join(unmet)
            )

    if eligible and unmet:
        raise AWMConfigError(
            "eligible_for_scientific_claims=true requires every "
            "confirmatory requirement; unmet=" + ", ".join(unmet)
        )

    return WorkloadSafetyCertificateConfig(
        schema_version=schema_version,
        certificate_id=_string(root.get("certificate_id"), "certificate_id"),
        source_path=source,
        source_sha256=sha256(raw_bytes).hexdigest(),
        mode=mode,
        posthoc=posthoc,
        eligible_for_scientific_claims=eligible,
        safe_design_id=safe_design_id,
        design_ids=design_ids,
        excluded_design_ids=excluded_design_ids,
        strata=tuple(strata),
        selection_workload_ids=selection_workload_ids,
        certificate_workload_ids=certificate_workload_ids,
        alpha=alpha,
        family_adjustment=family_adjustment,
        bound_method=bound_method,
        minimum_independent_workloads=minimum_independent_workloads,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        threshold_provenance=threshold_provenance,
        candidate_restriction_provenance=candidate_restriction_provenance,
        candidate_restriction_note=candidate_restriction_note,
        success_difference_support=success_difference_support,
        cost_saving_support=cost_saving_support,
        preregistration_declared_before_evaluation_outcomes=declared_before,
        preregistration_declaration_id=declaration_id,
        preregistration_declaration_sha256=declaration_sha256,
        inspected_oracle_snapshot_sha256=inspected_snapshots,
        estimand_target=estimand_target,
        workload_sampling_mechanism=workload_sampling_mechanism,
        workload_selection_rule=workload_selection_rule,
        confirmatory_static_requirements=requirements,
    )


def _service_cost(record: Mapping[str, Any]) -> float:
    return sum(
        float(event.get("realized_cost", 0.0))
        for event in record.get("access_events", [])
        if event.get("accepted")
    )


def _validate_record(record: Mapping[str, Any], design_id: str) -> None:
    trial_key = record.get("trial_key")
    if record.get("artifact_delivery_complete") is False:
        raise AWMConfigError(
            "safety certificate fails closed on an artifact-delivery "
            f"failure in {design_id}: {trial_key}"
        )
    if record.get("outcome_type") == "artifact_delivery_failure":
        raise AWMConfigError(
            "safety certificate fails closed on an artifact-delivery "
            f"failure in {design_id}: {trial_key}"
        )
    if (
        record.get("outcome_type") != "completed"
        or record.get("telemetry_complete") is not True
        or record.get("task_success") is None
    ):
        raise AWMConfigError(
            "safety certificate fails closed on incomplete telemetry in "
            f"{design_id}: {trial_key}"
        )


def _load_response_matrix(
    config: WorkloadSafetyCertificateConfig,
    oracle: ReducedOracleConfig,
    *,
    oracle_output_dir: Path,
    design_ids: tuple[str, ...] | None = None,
) -> dict[tuple[str, str, int], _Response]:
    pilot = load_flowmesh_pilot_config(oracle.workload_pilot_config_path)
    pilot_workloads = {workload.id for workload in pilot.workloads}
    declared = set(config.certificate_workload_ids).union(
        config.selection_workload_ids
    )
    unknown = declared - pilot_workloads
    if unknown:
        raise AWMConfigError(
            "certificate workloads must exist in the pilot workload set; "
            "unknown=" + ", ".join(sorted(unknown))
        )
    if config.safe_design_id != oracle.safe_design_id:
        raise AWMConfigError(
            "certificate safe_design_id disagrees with Reduced Oracle"
        )
    if config.design_ids != oracle.design_ids:
        raise AWMConfigError(
            "certificate design_ids must exactly match Oracle design order"
        )

    expected_repetitions = tuple(range(oracle.repetitions))
    certificate_workloads = set(config.certificate_workload_ids)
    loaded_design_ids = (
        config.analysed_design_ids if design_ids is None else design_ids
    )
    matrix: dict[tuple[str, str, int], _Response] = {}
    for design_id in loaded_design_ids:
        path = oracle_output_dir / "designs" / design_id / "runs.jsonl"
        if not path.is_file():
            raise AWMConfigError(
                f"Reduced Oracle design records do not exist: {path}"
            )
        records = audit_pilot_records(
            load_pilot_records(path, repair_truncated_tail=False)
        )
        for record in records:
            if record.get("design_id") != design_id:
                raise AWMConfigError(
                    f"Oracle record has wrong design_id in {design_id}"
                )
            workload_id = record.get("workload_id")
            if workload_id not in pilot_workloads:
                raise AWMConfigError(
                    f"Oracle record has unknown workload_id: {workload_id}"
                )
            if workload_id not in certificate_workloads:
                continue
            repetition = record.get("repetition")
            if (
                isinstance(repetition, bool)
                or not isinstance(repetition, int)
                or repetition not in expected_repetitions
            ):
                raise AWMConfigError(
                    f"Oracle record has invalid repetition: {repetition}"
                )
            _validate_record(record, design_id)
            key = (design_id, str(workload_id), repetition)
            if key in matrix:
                raise AWMConfigError(
                    "duplicate certificate response cell: "
                    + "|".join(map(str, key))
                )
            matrix[key] = _Response(
                success=float(int(bool(record["task_success"]))),
                service_cost=_service_cost(record),
            )

    for design_id in loaded_design_ids:
        for workload_id in config.certificate_workload_ids:
            present = tuple(
                repetition
                for repetition in expected_repetitions
                if (design_id, workload_id, repetition) in matrix
            )
            if not present:
                raise AWMConfigError(
                    "missing certificate response cell: "
                    f"{design_id}|{workload_id}"
                )
            if present != expected_repetitions:
                missing = sorted(set(expected_repetitions) - set(present))
                raise AWMConfigError(
                    "incomplete repetition block: "
                    f"{design_id}|{workload_id}; missing repetitions="
                    + ", ".join(map(str, missing))
                )
    return matrix


def _block_mean(
    matrix: Mapping[tuple[str, str, int], _Response],
    design_id: str,
    workload_id: str,
    repetitions: tuple[int, ...],
) -> _Response:
    rows = [
        matrix[(design_id, workload_id, repetition)]
        for repetition in repetitions
    ]
    return _Response(
        success=mean(row.success for row in rows),
        service_cost=mean(row.service_cost for row in rows),
    )


def _cluster_observations(
    matrix: Mapping[tuple[str, str, int], _Response],
    *,
    safe_design_id: str,
    candidate_design_id: str,
    workload_ids: tuple[str, ...],
    repetitions: tuple[int, ...],
    task_value: float,
    resource_cost_weight: float,
) -> tuple[_ClusterObservation, ...]:
    observations: list[_ClusterObservation] = []
    for workload_id in workload_ids:
        safe = _block_mean(matrix, safe_design_id, workload_id, repetitions)
        candidate = _block_mean(
            matrix,
            candidate_design_id,
            workload_id,
            repetitions,
        )
        success_difference = candidate.success - safe.success
        cost_saving = safe.service_cost - candidate.service_cost
        observations.append(_ClusterObservation(
            workload_id=workload_id,
            success_difference=success_difference,
            cost_saving=cost_saving,
            utility_gain=(
                task_value * success_difference
                + resource_cost_weight * cost_saving
            ),
        ))
    return tuple(observations)


def _bounded_mean(
    values: tuple[float, ...],
    *,
    support: Interval,
    delta: float,
    quantity: str,
    method: str = "workload-cluster-one-sided-bounded-mean-kl",
) -> _BoundedMean:
    if not values:
        raise AWMConfigError(
            f"{quantity} has no workload clusters to bound"
        )
    for value in values:
        if not support.contains(value, tolerance=_SUPPORT_TOLERANCE):
            raise AWMConfigError(
                f"{quantity} value {value} exceeds its declared support "
                f"[{support.lower}, {support.upper}]"
            )
    normalized = [
        min(1.0, max(0.0, (value - support.lower) / support.width))
        for value in values
    ]
    sample_mean = mean(normalized)
    if method == "workload-cluster-one-sided-bounded-mean-kl":
        bounds = one_sided_bounded_mean_bounds(
            sample_mean,
            len(values),
            delta,
        )
    elif method == "point-estimate-no-uncertainty":
        # Deliberately invalid negative control for calibration only: it
        # reports the sample mean as if it were a confidence bound.
        bounds = Interval(sample_mean, sample_mean)
    else:
        raise AWMConfigError(f"unknown bound method: {method}")
    return _BoundedMean(
        point_estimate=mean(values),
        lower_bound=support.lower + bounds.lower * support.width,
        upper_bound=support.lower + bounds.upper * support.width,
        normalized_point_estimate=sample_mean,
        normalized_lower_bound=bounds.lower,
        normalized_upper_bound=bounds.upper,
        support=support,
    )


@dataclass(frozen=True)
class StratumCertificate:
    """The complete certificate decision for one stratum."""

    independent_workload_count: int
    success_bound: _BoundedMean
    cost_bound: _BoundedMean
    utility_point_estimate: float
    gates: tuple[dict[str, Any], ...]
    decision: dict[str, Any]

    @property
    def certificate_state(self) -> str:
        return str(self.decision["certificate_state"])

    @property
    def applied_design_id(self) -> str:
        return str(self.decision["applied_design_id"])


def _gate(
    bound: _BoundedMean,
    threshold: float,
    *,
    gate_id: str,
    comparison: str,
) -> dict[str, Any]:
    if bound.lower_bound >= threshold:
        result = "PASS"
    elif bound.upper_bound < threshold:
        result = "VIOLATED"
    else:
        result = "INDETERMINATE"
    return {
        "gate_id": gate_id,
        "comparison": comparison,
        "threshold": threshold,
        "point_estimate": bound.point_estimate,
        "lower_bound": bound.lower_bound,
        "upper_bound": bound.upper_bound,
        "normalized_point_estimate": bound.normalized_point_estimate,
        "normalized_lower_bound": bound.normalized_lower_bound,
        "normalized_upper_bound": bound.normalized_upper_bound,
        "support_lower": bound.support.lower,
        "support_upper": bound.support.upper,
        "result": result,
    }


def _decide(
    gates: tuple[dict[str, Any], ...],
    *,
    independent_workload_count: int,
    minimum_independent_workloads: int,
    safe_design_id: str,
    candidate_design_id: str,
) -> dict[str, Any]:
    violated = tuple(
        gate["gate_id"] for gate in gates if gate["result"] == "VIOLATED"
    )
    indeterminate = tuple(
        gate["gate_id"] for gate in gates if gate["result"] == "INDETERMINATE"
    )
    if violated:
        state = "UNSAFE"
        reason = "gate_violation_established:" + "+".join(violated)
    elif independent_workload_count < minimum_independent_workloads:
        state = "INSUFFICIENT_EVIDENCE"
        reason = (
            "insufficient_independent_workloads:"
            f"{independent_workload_count}<{minimum_independent_workloads}"
        )
    elif indeterminate:
        state = "INSUFFICIENT_EVIDENCE"
        reason = "gate_not_established:" + "+".join(indeterminate)
    else:
        state = "SAFE_TO_COMMIT"
        reason = "certificate_passed_every_gate"
    safe = state == "SAFE_TO_COMMIT"
    return {
        "certificate_state": state,
        "applied_design_id": candidate_design_id if safe else safe_design_id,
        "fallback_design_id": safe_design_id,
        "fallback_applied": not safe,
        "fallback_reason": (
            "certificate_passed_fallback_not_applied" if safe else reason
        ),
        "decision_reason": reason,
    }


@dataclass(frozen=True)
class CertificateInputs:
    """Everything the statistical core needs from a frozen Oracle."""

    matrix: dict[tuple[str, str, int], _Response]
    task_value: float
    resource_cost_weight: float
    repetitions: tuple[int, ...]
    utility_support: Interval
    loaded_design_ids: tuple[str, ...]


def load_certificate_inputs(
    config: WorkloadSafetyCertificateConfig,
    oracle: ReducedOracleConfig,
    *,
    oracle_output_dir: str | Path,
    design_ids: tuple[str, ...] | None = None,
) -> CertificateInputs:
    """Read only the named designs' records from a frozen Oracle.

    ``design_ids`` exists so a sequential experiment can hold a candidate's
    records genuinely unread until it is revealed: an unnamed design's
    ``runs.jsonl`` is never opened.
    """
    source = Path(oracle_output_dir).resolve()
    matrix = _load_response_matrix(
        config,
        oracle,
        oracle_output_dir=source,
        design_ids=design_ids,
    )
    pilot = load_flowmesh_pilot_config(oracle.workload_pilot_config_path)
    system = load_config(pilot.system_config_path)
    task_class = system.task_classes[pilot.task_class_id]
    task_value = float(task_class.task_value)
    resource_cost_weight = float(system.resource_cost_weight)
    if task_value < 0.0 or resource_cost_weight < 0.0:
        raise AWMConfigError(
            "certificate requires non-negative task_value and "
            "resource_cost_weight"
        )
    return CertificateInputs(
        matrix=matrix,
        task_value=task_value,
        resource_cost_weight=resource_cost_weight,
        repetitions=tuple(range(oracle.repetitions)),
        utility_support=Interval(
            task_value * config.success_difference_support.lower
            + resource_cost_weight * config.cost_saving_support.lower,
            task_value * config.success_difference_support.upper
            + resource_cost_weight * config.cost_saving_support.upper,
        ),
        loaded_design_ids=(
            config.analysed_design_ids if design_ids is None else design_ids
        ),
    )


def certificate_cluster_observations(
    inputs: CertificateInputs,
    *,
    safe_design_id: str,
    candidate_design_id: str,
    workload_ids: tuple[str, ...],
) -> tuple[_ClusterObservation, ...]:
    """Reduce complete repetition blocks to one observation per workload."""
    return _cluster_observations(
        inputs.matrix,
        safe_design_id=safe_design_id,
        candidate_design_id=candidate_design_id,
        workload_ids=workload_ids,
        repetitions=inputs.repetitions,
        task_value=inputs.task_value,
        resource_cost_weight=inputs.resource_cost_weight,
    )


def evaluate_stratum_certificate(
    observations: tuple[_ClusterObservation, ...],
    *,
    success_difference_support: Interval,
    cost_saving_support: Interval,
    utility_support: Interval,
    adjusted_alpha: float,
    delta_success_margin: float,
    minimum_cost_saving: float,
    minimum_independent_workloads: int,
    safe_design_id: str,
    candidate_design_id: str,
    bound_method: str = "workload-cluster-one-sided-bounded-mean-kl",
) -> StratumCertificate:
    """Decide one stratum from its workload-cluster observations.

    This is the whole statistical core. The frozen-Oracle certifier and the
    Monte Carlo calibration harness both call it, so a calibration result is
    a statement about the deployed decision rule rather than about a
    re-implementation of it.
    """
    success_bound = _bounded_mean(
        tuple(value.success_difference for value in observations),
        support=success_difference_support,
        delta=adjusted_alpha,
        quantity="success_difference",
        method=bound_method,
    )
    cost_bound = _bounded_mean(
        tuple(value.cost_saving for value in observations),
        support=cost_saving_support,
        delta=adjusted_alpha,
        quantity="cost_saving",
        method=bound_method,
    )
    utility_values = tuple(value.utility_gain for value in observations)
    for value in utility_values:
        if not utility_support.contains(
            value,
            tolerance=_SUPPORT_TOLERANCE,
        ):
            raise AWMConfigError(
                f"utility_gain value {value} exceeds its derived support"
            )
    gates = (
        _gate(
            success_bound,
            -delta_success_margin,
            gate_id="success_non_inferiority",
            comparison=(
                "lower_bound(candidate_success - origin_success) >= "
                "-delta_success_margin"
            ),
        ),
        _gate(
            cost_bound,
            minimum_cost_saving,
            gate_id="cost_improvement",
            comparison=(
                "lower_bound(origin_cost - candidate_cost) >= "
                "minimum_cost_saving"
            ),
        ),
    )
    return StratumCertificate(
        independent_workload_count=len(observations),
        success_bound=success_bound,
        cost_bound=cost_bound,
        utility_point_estimate=mean(utility_values),
        gates=gates,
        decision=_decide(
            gates,
            independent_workload_count=len(observations),
            minimum_independent_workloads=minimum_independent_workloads,
            safe_design_id=safe_design_id,
            candidate_design_id=candidate_design_id,
        ),
    )


def certify_awm_restricted_policy(
    certificate_config: WorkloadSafetyCertificateConfig | str | Path,
    oracle_config: ReducedOracleConfig | str | Path,
    *,
    oracle_output_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Certify a restricted, workload-aware policy against the safe origin."""
    config = (
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
            f"certificate output directory is not empty: {output}"
        )

    analysed_snapshot = reduced_oracle_snapshot(
        source,
        config.analysed_design_ids,
        scope="analysed-design-subset",
    )
    full_snapshot = reduced_oracle_snapshot(
        source,
        config.design_ids,
        scope="full-declared-design-set",
    )
    if config.mode == "confirmatory":
        reused = tuple(
            snapshot.sha256
            for snapshot in (analysed_snapshot, full_snapshot)
            if snapshot.sha256 in config.inspected_oracle_snapshot_sha256
        )
        if reused:
            raise AWMConfigError(
                "confirmatory mode cannot reuse an Oracle snapshot that was "
                "already inspected: " + ", ".join(reused)
            )

    inputs = load_certificate_inputs(
        config,
        oracle,
        oracle_output_dir=source,
    )
    matrix = inputs.matrix
    task_value = inputs.task_value
    resource_cost_weight = inputs.resource_cost_weight
    utility_support = inputs.utility_support
    repetitions = inputs.repetitions
    adjusted_alpha = config.adjusted_alpha
    if not 0.0 < adjusted_alpha < 1.0:
        raise AWMConfigError(
            "family-adjusted alpha must remain in (0, 1)"
        )

    summary_rows: list[dict[str, Any]] = []
    stratum_details: list[dict[str, Any]] = []
    for stratum in config.strata:
        observations = _cluster_observations(
            matrix,
            safe_design_id=config.safe_design_id,
            candidate_design_id=stratum.candidate_design_id,
            workload_ids=stratum.workload_ids,
            repetitions=repetitions,
            task_value=task_value,
            resource_cost_weight=resource_cost_weight,
        )
        certificate = evaluate_stratum_certificate(
            observations,
            success_difference_support=config.success_difference_support,
            cost_saving_support=config.cost_saving_support,
            utility_support=utility_support,
            adjusted_alpha=adjusted_alpha,
            delta_success_margin=config.delta_success_margin,
            minimum_cost_saving=config.minimum_cost_saving,
            minimum_independent_workloads=(
                config.minimum_independent_workloads
            ),
            safe_design_id=config.safe_design_id,
            candidate_design_id=stratum.candidate_design_id,
            bound_method="workload-cluster-one-sided-bounded-mean-kl",
        )
        independent_workload_count = certificate.independent_workload_count
        repetition_pair_count = independent_workload_count * len(repetitions)
        success_bound = certificate.success_bound
        cost_bound = certificate.cost_bound
        success_gate, cost_gate = certificate.gates
        gates = certificate.gates
        decision = certificate.decision
        summary_rows.append({
            "certificate_id": config.certificate_id,
            "stratum_id": stratum.stratum_id,
            "candidate_design_id": stratum.candidate_design_id,
            "safe_design_id": config.safe_design_id,
            "independent_workload_count": independent_workload_count,
            "repetition_pair_count": repetition_pair_count,
            "repetitions_per_workload": len(repetitions),
            "success_difference_point_estimate": (
                success_bound.point_estimate
            ),
            "success_difference_lower_bound": success_bound.lower_bound,
            "success_difference_upper_bound": success_bound.upper_bound,
            "cost_saving_point_estimate": cost_bound.point_estimate,
            "cost_saving_lower_bound": cost_bound.lower_bound,
            "cost_saving_upper_bound": cost_bound.upper_bound,
            "utility_gain_point_estimate": (
                certificate.utility_point_estimate
            ),
            "delta_success_margin": config.delta_success_margin,
            "minimum_cost_saving": config.minimum_cost_saving,
            "success_non_inferiority_gate": success_gate["result"],
            "cost_improvement_gate": cost_gate["result"],
            "certificate_state": decision["certificate_state"],
            "applied_design_id": decision["applied_design_id"],
            "fallback_design_id": decision["fallback_design_id"],
            "fallback_applied": decision["fallback_applied"],
            "fallback_reason": decision["fallback_reason"],
            "alpha": config.alpha,
            "adjusted_alpha": adjusted_alpha,
            "unadjusted_confidence_level": 1.0 - config.alpha,
            "adjusted_confidence_level": 1.0 - adjusted_alpha,
            "family_adjustment": config.family_adjustment,
            "family_size": config.family_size,
            "posthoc": config.posthoc,
            "eligible_for_scientific_claims": (
                config.eligible_for_scientific_claims
            ),
        })
        stratum_details.append({
            "stratum_id": stratum.stratum_id,
            "candidate_design_id": stratum.candidate_design_id,
            "safe_design_id": config.safe_design_id,
            "independent_workload_count": independent_workload_count,
            "repetition_pair_count": repetition_pair_count,
            "repetitions_per_workload": len(repetitions),
            "workload_ids": list(stratum.workload_ids),
            "point_estimates": {
                "success_difference": success_bound.point_estimate,
                "cost_saving": cost_bound.point_estimate,
                "utility_gain": certificate.utility_point_estimate,
            },
            "gates": [dict(gate) for gate in gates],
            "utility_gain": {
                "point_estimate": certificate.utility_point_estimate,
                "support_lower": utility_support.lower,
                "support_upper": utility_support.upper,
                "in_confidence_family": False,
                "role": (
                    "descriptive-only; excluded from the declared "
                    "confidence family so that no gate bound is widened by "
                    "an unused quantity"
                ),
            },
            "workload_observations": [
                {
                    "workload_id": value.workload_id,
                    "success_difference": value.success_difference,
                    "cost_saving": value.cost_saving,
                    "utility_gain": value.utility_gain,
                }
                for value in observations
            ],
            **decision,
        })

    states = [row["certificate_state"] for row in summary_rows]
    safe_strata = [
        row["stratum_id"]
        for row in summary_rows
        if row["certificate_state"] == "SAFE_TO_COMMIT"
    ]
    output.mkdir(parents=True, exist_ok=True)
    summary_path = output / "certificate_summary.csv"
    evaluation_path = output / "certificate_evaluation.json"
    manifest_path = output / "certificate_manifest.json"
    _write_csv(summary_rows, summary_path)

    evaluation = {
        "schema_version": SAFETY_CERTIFICATE_EVALUATION_SCHEMA_VERSION,
        "certificate_id": config.certificate_id,
        "status": "COMPLETE",
        "mode": config.mode,
        "posthoc": config.posthoc,
        "eligible_for_scientific_claims": (
            config.eligible_for_scientific_claims
        ),
        "decision_semantics": {
            "SAFE_TO_COMMIT": (
                "every gate lower bound clears its threshold at the "
                "family-adjusted one-sided level"
            ),
            "UNSAFE": (
                "at least one gate upper bound falls strictly below its "
                "threshold, establishing the violation"
            ),
            "INSUFFICIENT_EVIDENCE": (
                "no gate violation is established and at least one gate is "
                "not established, or too few independent workloads"
            ),
            "fallback": (
                "every non-SAFE_TO_COMMIT stratum falls back to the safe "
                "origin design"
            ),
        },
        "statistical_method": {
            "independent_unit": "workload-object-cluster",
            "repeated_measurement": (
                "complete fixed repetition block averaged inside each "
                "workload before any bound is computed"
            ),
            "bound_method": config.bound_method,
            "bound_family": (
                "one-sided binary-relative-entropy inversion for the mean "
                "of independent bounded units; no normal approximation"
            ),
            "family_adjustment": config.family_adjustment,
            "family_size": config.family_size,
            "family_components": [
                {
                    "stratum_id": stratum.stratum_id,
                    "candidate_design_id": stratum.candidate_design_id,
                    "gate_id": gate_id,
                    "side": side,
                }
                for stratum in config.strata
                for gate_id in GATE_IDS
                for side in GATE_SIDES
            ],
            "alpha": config.alpha,
            "adjusted_alpha": adjusted_alpha,
            "unadjusted_confidence_level": 1.0 - config.alpha,
            "adjusted_confidence_level": 1.0 - adjusted_alpha,
            "minimum_independent_workloads": (
                config.minimum_independent_workloads
            ),
            "task_value": task_value,
            "resource_cost_weight": resource_cost_weight,
            "success_difference_support": {
                "lower": config.success_difference_support.lower,
                "upper": config.success_difference_support.upper,
            },
            "cost_saving_support": {
                "lower": config.cost_saving_support.lower,
                "upper": config.cost_saving_support.upper,
            },
            "utility_gain_support": {
                "lower": utility_support.lower,
                "upper": utility_support.upper,
            },
        },
        "thresholds": {
            "delta_success_margin": config.delta_success_margin,
            "minimum_cost_saving": config.minimum_cost_saving,
            "provenance": config.threshold_provenance,
            "selected_by_this_tool": False,
        },
        "candidate_restriction": {
            "provenance": config.candidate_restriction_provenance,
            "note": config.candidate_restriction_note,
            "excluded_design_ids": list(config.excluded_design_ids),
            "analysed_design_ids": list(config.analysed_design_ids),
            "stratum_candidates": {
                stratum.stratum_id: stratum.candidate_design_id
                for stratum in config.strata
            },
        },
        "workload_split": {
            "selection_workload_ids": list(config.selection_workload_ids),
            "certificate_workload_ids": list(
                config.certificate_workload_ids
            ),
            "leakage_checked": True,
            "independent_workload_count": len(
                config.certificate_workload_ids
            ),
            "repetition_pair_count": (
                len(config.certificate_workload_ids) * len(repetitions)
            ),
        },
        "estimand": {
            "target": config.estimand_target,
            "independent_unit": "workload-object-cluster",
            "workload_sampling_mechanism": (
                config.workload_sampling_mechanism
            ),
            "workload_selection_rule": config.workload_selection_rule,
            "generalization_claimed": (
                config.generalizes_beyond_the_supplied_workloads
            ),
            "repetitions_increase_independent_units": False,
            "note": (
                "The bound is a valid statement about a superpopulation "
                "mean only when the certified workloads are an exchangeable "
                "sample from that population under a declared sampling "
                "mechanism. With target=finite-supplied-workload-set the "
                "interval quantifies uncertainty about the mean effect over "
                "these supplied workload clusters under an unverified "
                "exchangeability assumption and is NOT a generalization "
                "claim about future workloads."
            ),
        },
        "confirmatory_eligibility": {
            "eligible_for_scientific_claims": (
                config.eligible_for_scientific_claims
            ),
            "static_requirements": {
                name: met
                for name, met in config.confirmatory_static_requirements
            },
            "unmet_static_requirements": list(
                config.unmet_confirmatory_static_requirements
            ),
            "runtime_requirements": {
                "oracle_snapshot_previously_unseen": (
                    config.mode == "confirmatory"
                ),
            },
            "note": (
                "Clearing posthoc alone never grants eligibility: every "
                "static requirement plus a previously unseen Oracle "
                "snapshot must hold."
            ),
        },
        "oracle_snapshot": {
            "algorithm": analysed_snapshot.algorithm,
            "analysed_design_subset_sha256": analysed_snapshot.sha256,
            "analysed_design_ids": list(analysed_snapshot.design_ids),
            "full_declared_design_set_sha256": full_snapshot.sha256,
            "full_declared_design_ids": list(full_snapshot.design_ids),
            "note": (
                "The two digests use one shared algorithm and differ only "
                "in scope: the analysed subset omits designs excluded from "
                "the restricted search. The full-declared-design-set digest "
                "is the value other read-only tools over the same Oracle "
                "report."
            ),
        },
        "preregistration": {
            "declared_before_evaluation_outcomes": (
                config.preregistration_declared_before_evaluation_outcomes
            ),
            "declaration_id": config.preregistration_declaration_id,
            "declaration_sha256": (
                config.preregistration_declaration_sha256
            ),
            "inspected_oracle_snapshot_count": len(
                config.inspected_oracle_snapshot_sha256
            ),
        },
        "strata": stratum_details,
        "state_counts": {
            state: states.count(state)
            for state in CERTIFICATE_STATES
        },
        "safe_to_commit_strata": safe_strata,
        "limitations": [
            "The restricted candidate set was selected post-hoc from an "
            "already inspected Reduced Oracle and requires validation on "
            "new independent workloads.",
            "delta_success_margin and minimum_cost_saving are explicit "
            "configuration inputs; this tool does not choose them and does "
            "not assert that any particular value is scientifically "
            "justified.",
            "Bounds are cluster-level over a small number of workload "
            "objects, so they are wide by construction.",
            "Conditional materialization storage and transition costs are "
            "not allocated to stratum-level policies by the current "
            "global-design Reduced Oracle.",
            "Utility gain is reported as a descriptive point estimate only "
            "and is not part of the declared confidence family.",
            "Repetitions are within-workload repeated measurements and "
            "never increase the independent workload count.",
        ],
    }
    _write_json(evaluation, evaluation_path)
    certificate_snapshot = sha256()
    for path in (summary_path, evaluation_path):
        certificate_snapshot.update(path.name.encode("utf-8"))
        certificate_snapshot.update(b"\0")
        certificate_snapshot.update(sha256(path.read_bytes()).digest())

    manifest = {
        "schema_version": SAFETY_CERTIFICATE_EVALUATION_SCHEMA_VERSION,
        "certificate_id": config.certificate_id,
        "status": "COMPLETE",
        "mode": config.mode,
        "posthoc": config.posthoc,
        "eligible_for_scientific_claims": (
            config.eligible_for_scientific_claims
        ),
        "estimand_target": config.estimand_target,
        "workload_sampling_mechanism": config.workload_sampling_mechanism,
        "workload_selection_rule": config.workload_selection_rule,
        "unmet_confirmatory_static_requirements": list(
            config.unmet_confirmatory_static_requirements
        ),
        "deployment_mutations_performed": False,
        "secrets_recorded": False,
        "certificate_config_path": str(config.source_path),
        "certificate_config_sha256": config.source_sha256,
        "oracle_config_path": str(oracle.source_path),
        "oracle_config_sha256": oracle.source_sha256,
        "oracle_output_dir": str(source),
        **analysed_snapshot.to_manifest_fields(
            "oracle_analysed_snapshot"
        ),
        **full_snapshot.to_manifest_fields("oracle_full_snapshot"),
        "certificate_snapshot_sha256": certificate_snapshot.hexdigest(),
        "summary_path": str(summary_path),
        "evaluation_path": str(evaluation_path),
        "family_adjustment": config.family_adjustment,
        "family_size": config.family_size,
        "alpha": config.alpha,
        "adjusted_alpha": adjusted_alpha,
        "unadjusted_confidence_level": 1.0 - config.alpha,
        "adjusted_confidence_level": 1.0 - adjusted_alpha,
        "independent_workload_count": len(config.certificate_workload_ids),
        "repetition_pair_count": (
            len(config.certificate_workload_ids) * len(repetitions)
        ),
        "stratum_count": len(config.strata),
        "state_counts": {
            state: states.count(state)
            for state in CERTIFICATE_STATES
        },
        "safe_to_commit_strata": safe_strata,
        "fallback_design_id": config.safe_design_id,
    }
    _write_json(manifest, manifest_path)
    return {**manifest, "manifest_path": str(manifest_path)}
