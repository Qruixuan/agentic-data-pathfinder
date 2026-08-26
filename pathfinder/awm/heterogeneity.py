from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping

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


HETEROGENEITY_CONFIG_SCHEMA_VERSION = (
    "pathfinder.awm-heterogeneity/v1alpha1"
)
HETEROGENEITY_EVALUATION_SCHEMA_VERSION = (
    "pathfinder.awm-heterogeneity-evaluation/v1alpha1"
)


@dataclass(frozen=True)
class WorkloadHeterogeneityAuditConfig:
    schema_version: str
    audit_id: str
    source_path: Path
    source_sha256: str
    posthoc: bool
    eligible_for_scientific_claims: bool
    safe_design_id: str
    design_ids: tuple[str, ...]
    training_repetitions: tuple[int, ...]
    evaluation_repetitions: tuple[int, ...]
    workload_groups: dict[str, tuple[str, ...]]
    minimum_training_workloads_per_group: int
    minimum_candidate_mean_gain: float
    minimum_candidate_positive_fraction: float

    @property
    def workload_to_group(self) -> dict[str, str]:
        return {
            workload_id: group_id
            for group_id, workload_ids in self.workload_groups.items()
            for workload_id in workload_ids
        }


@dataclass(frozen=True)
class _Response:
    success: float
    service_cost: float


@dataclass(frozen=True)
class _Effect:
    success_difference: float
    cost_saving: float
    utility_gain: float


@dataclass(frozen=True)
class _PolicyChoice:
    design_id: str
    reason: str
    training_workload_count: int
    estimated_gain: float
    positive_fraction: float


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


def _fraction(value: Any, name: str) -> float:
    result = _finite_number(value, name)
    if result < 0.0 or result > 1.0:
        raise AWMConfigError(f"{name} must be between 0 and 1")
    return result


def _string_array(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise AWMConfigError(f"{name} must be a non-empty array")
    result = tuple(
        _string(item, f"{name}[{index}]")
        for index, item in enumerate(value)
    )
    if len(set(result)) != len(result):
        raise AWMConfigError(f"{name} cannot contain duplicates")
    return result


def _repetition_array(value: Any, name: str) -> tuple[int, ...]:
    if not isinstance(value, list) or not value:
        raise AWMConfigError(f"{name} must be a non-empty array")
    result: list[int] = []
    for index, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise AWMConfigError(
                f"{name}[{index}] must be a non-negative integer"
            )
        result.append(item)
    if len(set(result)) != len(result):
        raise AWMConfigError(f"{name} cannot contain duplicates")
    return tuple(sorted(result))


def load_workload_heterogeneity_audit_config(
    path: str | Path,
) -> WorkloadHeterogeneityAuditConfig:
    source = Path(path).resolve()
    if not source.is_file():
        raise AWMConfigError(
            f"AWM heterogeneity configuration does not exist: {source}"
        )
    raw_bytes = source.read_bytes()
    try:
        root = _mapping(json.loads(raw_bytes), "heterogeneity config")
    except json.JSONDecodeError as exc:
        raise AWMConfigError(
            f"invalid AWM heterogeneity JSON: {source}"
        ) from exc

    schema_version = _string(root.get("schema_version"), "schema_version")
    if schema_version != HETEROGENEITY_CONFIG_SCHEMA_VERSION:
        raise AWMConfigError(
            "unsupported AWM heterogeneity schema_version: "
            + schema_version
        )
    posthoc = _boolean(root.get("posthoc"), "posthoc")
    eligible = _boolean(
        root.get("eligible_for_scientific_claims"),
        "eligible_for_scientific_claims",
    )
    if not posthoc or eligible:
        raise AWMConfigError(
            "the frozen-Oracle heterogeneity audit must declare "
            "posthoc=true and eligible_for_scientific_claims=false"
        )

    design_ids = _string_array(root.get("design_ids"), "design_ids")
    safe_design_id = _string(root.get("safe_design_id"), "safe_design_id")
    if safe_design_id not in design_ids:
        raise AWMConfigError("safe_design_id must appear in design_ids")
    if len(design_ids) < 2:
        raise AWMConfigError("design_ids must contain at least two designs")

    training_repetitions = _repetition_array(
        root.get("training_repetitions"),
        "training_repetitions",
    )
    evaluation_repetitions = _repetition_array(
        root.get("evaluation_repetitions"),
        "evaluation_repetitions",
    )
    overlap = set(training_repetitions).intersection(evaluation_repetitions)
    if overlap:
        raise AWMConfigError(
            "training_repetitions and evaluation_repetitions must be "
            "disjoint"
        )

    raw_groups = _mapping(root.get("workload_groups"), "workload_groups")
    if not raw_groups:
        raise AWMConfigError("workload_groups cannot be empty")
    workload_groups: dict[str, tuple[str, ...]] = {}
    seen_workloads: set[str] = set()
    for raw_group_id, raw_workloads in raw_groups.items():
        group_id = _string(raw_group_id, "workload group id")
        workloads = _string_array(
            raw_workloads,
            f"workload_groups.{group_id}",
        )
        duplicates = seen_workloads.intersection(workloads)
        if duplicates:
            raise AWMConfigError(
                "workload_groups cannot overlap: "
                + ", ".join(sorted(duplicates))
            )
        seen_workloads.update(workloads)
        workload_groups[group_id] = workloads

    policy = _mapping(root.get("policy"), "policy")
    return WorkloadHeterogeneityAuditConfig(
        schema_version=schema_version,
        audit_id=_string(root.get("audit_id"), "audit_id"),
        source_path=source,
        source_sha256=sha256(raw_bytes).hexdigest(),
        posthoc=posthoc,
        eligible_for_scientific_claims=eligible,
        safe_design_id=safe_design_id,
        design_ids=design_ids,
        training_repetitions=training_repetitions,
        evaluation_repetitions=evaluation_repetitions,
        workload_groups=workload_groups,
        minimum_training_workloads_per_group=_positive_integer(
            policy.get("minimum_training_workloads_per_group"),
            "policy.minimum_training_workloads_per_group",
        ),
        minimum_candidate_mean_gain=_finite_number(
            policy.get("minimum_candidate_mean_gain"),
            "policy.minimum_candidate_mean_gain",
        ),
        minimum_candidate_positive_fraction=_fraction(
            policy.get("minimum_candidate_positive_fraction"),
            "policy.minimum_candidate_positive_fraction",
        ),
    )


def _eligible_record(record: Mapping[str, Any]) -> bool:
    return (
        record.get("outcome_type") == "completed"
        and record.get("telemetry_complete") is True
        and record.get("task_success") is not None
    )


def _service_cost(record: Mapping[str, Any]) -> float:
    return sum(
        float(event.get("realized_cost", 0.0))
        for event in record.get("access_events", [])
        if event.get("accepted")
    )


def _load_response_matrix(
    config: WorkloadHeterogeneityAuditConfig,
    oracle: ReducedOracleConfig,
    *,
    oracle_output_dir: Path,
) -> tuple[
    dict[tuple[str, str, int], _Response],
    tuple[str, ...],
]:
    pilot = load_flowmesh_pilot_config(oracle.workload_pilot_config_path)
    pilot_workloads = tuple(workload.id for workload in pilot.workloads)
    configured_workloads = set(config.workload_to_group)
    if configured_workloads != set(pilot_workloads):
        missing = set(pilot_workloads) - configured_workloads
        extra = configured_workloads - set(pilot_workloads)
        raise AWMConfigError(
            "workload_groups must partition the complete pilot workload "
            f"set; missing={sorted(missing)}, extra={sorted(extra)}"
        )
    if config.safe_design_id != oracle.safe_design_id:
        raise AWMConfigError(
            "heterogeneity safe_design_id disagrees with Reduced Oracle"
        )
    if config.design_ids != oracle.design_ids:
        raise AWMConfigError(
            "heterogeneity design_ids must exactly match Oracle design order"
        )
    declared_repetitions = set(config.training_repetitions).union(
        config.evaluation_repetitions
    )
    expected_repetitions = set(range(oracle.repetitions))
    if declared_repetitions != expected_repetitions:
        raise AWMConfigError(
            "training and evaluation repetitions must partition every "
            "Reduced Oracle repetition"
        )

    matrix: dict[tuple[str, str, int], _Response] = {}
    for design_id in config.design_ids:
        path = oracle_output_dir / "designs" / design_id / "runs.jsonl"
        records = audit_pilot_records(
            load_pilot_records(path, repair_truncated_tail=False)
        )
        for record in records:
            if not _eligible_record(record):
                raise AWMConfigError(
                    "heterogeneity audit fails closed on an incomplete "
                    f"record in {design_id}: {record.get('trial_key')}"
                )
            if record.get("design_id") != design_id:
                raise AWMConfigError(
                    f"Oracle record has wrong design_id in {design_id}"
                )
            workload_id = record.get("workload_id")
            repetition = record.get("repetition")
            if workload_id not in configured_workloads:
                raise AWMConfigError(
                    f"Oracle record has unknown workload_id: {workload_id}"
                )
            if (
                isinstance(repetition, bool)
                or not isinstance(repetition, int)
                or repetition not in expected_repetitions
            ):
                raise AWMConfigError(
                    f"Oracle record has invalid repetition: {repetition}"
                )
            key = (design_id, str(workload_id), repetition)
            if key in matrix:
                raise AWMConfigError(
                    "duplicate heterogeneity response cell: "
                    + "|".join(map(str, key))
                )
            matrix[key] = _Response(
                success=int(bool(record["task_success"])),
                service_cost=_service_cost(record),
            )

    expected_cells = {
        (design_id, workload_id, repetition)
        for design_id in config.design_ids
        for workload_id in pilot_workloads
        for repetition in expected_repetitions
    }
    missing_cells = expected_cells - set(matrix)
    extra_cells = set(matrix) - expected_cells
    if missing_cells or extra_cells:
        raise AWMConfigError(
            "heterogeneity response matrix is not complete; "
            f"missing={len(missing_cells)}, extra={len(extra_cells)}"
        )
    return matrix, pilot_workloads


def _mean_response(
    matrix: Mapping[tuple[str, str, int], _Response],
    design_id: str,
    workload_id: str,
    repetitions: Iterable[int],
) -> _Response:
    rows = [
        matrix[(design_id, workload_id, repetition)]
        for repetition in repetitions
    ]
    return _Response(
        success=mean(row.success for row in rows),
        service_cost=mean(row.service_cost for row in rows),
    )


def _effect(
    safe: _Response,
    candidate: _Response,
    *,
    task_value: float,
    resource_cost_weight: float,
) -> _Effect:
    success_difference = candidate.success - safe.success
    cost_saving = safe.service_cost - candidate.service_cost
    return _Effect(
        success_difference=success_difference,
        cost_saving=cost_saving,
        utility_gain=(
            task_value * success_difference
            + resource_cost_weight * cost_saving
        ),
    )


def _effect_for(
    matrix: Mapping[tuple[str, str, int], _Response],
    safe_design_id: str,
    candidate_design_id: str,
    workload_id: str,
    repetitions: Iterable[int],
    *,
    task_value: float,
    resource_cost_weight: float,
) -> _Effect:
    return _effect(
        _mean_response(
            matrix,
            safe_design_id,
            workload_id,
            repetitions,
        ),
        _mean_response(
            matrix,
            candidate_design_id,
            workload_id,
            repetitions,
        ),
        task_value=task_value,
        resource_cost_weight=resource_cost_weight,
    )


def _candidate_stats(
    effects: Mapping[tuple[str, str], _Effect],
    workload_ids: Iterable[str],
    candidate_id: str,
) -> tuple[int, float, float]:
    values = [
        effects[(workload_id, candidate_id)].utility_gain
        for workload_id in workload_ids
    ]
    if not values:
        return 0, 0.0, 0.0
    return (
        len(values),
        mean(values),
        sum(value > 0.0 for value in values) / len(values),
    )


def _choose_policy_design(
    config: WorkloadHeterogeneityAuditConfig,
    effects: Mapping[tuple[str, str], _Effect],
    training_workload_ids: Iterable[str],
) -> _PolicyChoice:
    workload_ids = tuple(training_workload_ids)
    if len(workload_ids) < config.minimum_training_workloads_per_group:
        return _PolicyChoice(
            design_id=config.safe_design_id,
            reason="insufficient_training_workloads",
            training_workload_count=len(workload_ids),
            estimated_gain=0.0,
            positive_fraction=0.0,
        )
    candidates = [
        design_id
        for design_id in config.design_ids
        if design_id != config.safe_design_id
    ]
    ranked: list[tuple[float, int, str, int, float]] = []
    for index, design_id in enumerate(candidates):
        count, estimated_gain, positive_fraction = _candidate_stats(
            effects,
            workload_ids,
            design_id,
        )
        ranked.append(
            (estimated_gain, -index, design_id, count, positive_fraction)
        )
    estimated_gain, _, design_id, count, positive_fraction = max(ranked)
    if estimated_gain < config.minimum_candidate_mean_gain:
        return _PolicyChoice(
            design_id=config.safe_design_id,
            reason="candidate_mean_gain_below_threshold",
            training_workload_count=count,
            estimated_gain=estimated_gain,
            positive_fraction=positive_fraction,
        )
    if positive_fraction < config.minimum_candidate_positive_fraction:
        return _PolicyChoice(
            design_id=config.safe_design_id,
            reason="candidate_positive_fraction_below_threshold",
            training_workload_count=count,
            estimated_gain=estimated_gain,
            positive_fraction=positive_fraction,
        )
    return _PolicyChoice(
        design_id=design_id,
        reason="candidate_passed_posthoc_diagnostic_thresholds",
        training_workload_count=count,
        estimated_gain=estimated_gain,
        positive_fraction=positive_fraction,
    )


def _policy_summary(
    assignments: Iterable[Mapping[str, Any]],
    *,
    policy_prefix: str,
    safe_design_id: str,
) -> dict[str, Any]:
    rows = list(assignments)
    selected_key = f"{policy_prefix}_selected_design_id"
    gain_key = f"{policy_prefix}_evaluation_utility_gain"
    success_key = f"{policy_prefix}_evaluation_success_difference"
    cost_key = f"{policy_prefix}_evaluation_cost_saving"
    choices = Counter(str(row[selected_key]) for row in rows)
    return {
        "workload_count": len(rows),
        "selection_counts": dict(sorted(choices.items())),
        "fallback_count": choices.get(safe_design_id, 0),
        "mean_evaluation_utility_gain": mean(
            float(row[gain_key]) for row in rows
        ),
        "mean_evaluation_success_difference": mean(
            float(row[success_key]) for row in rows
        ),
        "mean_evaluation_cost_saving": mean(
            float(row[cost_key]) for row in rows
        ),
        "positive_evaluation_gain_count": sum(
            float(row[gain_key]) > 0.0 for row in rows
        ),
        "oracle_best_match_count": sum(
            row[selected_key] == row["evaluation_oracle_best_design_id"]
            for row in rows
        ),
    }


def audit_awm_workload_heterogeneity(
    audit_config: WorkloadHeterogeneityAuditConfig | str | Path,
    oracle_config: ReducedOracleConfig | str | Path,
    *,
    oracle_output_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Audit workload-conditional gains without making a safety claim."""
    config = (
        load_workload_heterogeneity_audit_config(audit_config)
        if isinstance(audit_config, (str, Path))
        else audit_config
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
            f"heterogeneity output directory is not empty: {output}"
        )
    output.mkdir(parents=True, exist_ok=True)

    matrix, workload_ids = _load_response_matrix(
        config,
        oracle,
        oracle_output_dir=source,
    )
    pilot = load_flowmesh_pilot_config(oracle.workload_pilot_config_path)
    system = load_config(pilot.system_config_path)
    task_class = system.task_classes[pilot.task_class_id]
    task_value = float(task_class.task_value)
    resource_cost_weight = float(system.resource_cost_weight)
    candidates = tuple(
        design_id
        for design_id in config.design_ids
        if design_id != config.safe_design_id
    )
    groups = config.workload_to_group

    training_effects: dict[tuple[str, str], _Effect] = {}
    evaluation_effects: dict[tuple[str, str], _Effect] = {}
    effect_rows: list[dict[str, Any]] = []
    for workload_id in workload_ids:
        for candidate_id in candidates:
            training = _effect_for(
                matrix,
                config.safe_design_id,
                candidate_id,
                workload_id,
                config.training_repetitions,
                task_value=task_value,
                resource_cost_weight=resource_cost_weight,
            )
            evaluation = _effect_for(
                matrix,
                config.safe_design_id,
                candidate_id,
                workload_id,
                config.evaluation_repetitions,
                task_value=task_value,
                resource_cost_weight=resource_cost_weight,
            )
            training_effects[(workload_id, candidate_id)] = training
            evaluation_effects[(workload_id, candidate_id)] = evaluation
            repetition_gains = [
                _effect_for(
                    matrix,
                    config.safe_design_id,
                    candidate_id,
                    workload_id,
                    (repetition,),
                    task_value=task_value,
                    resource_cost_weight=resource_cost_weight,
                ).utility_gain
                for repetition in config.training_repetitions
            ]
            sign_flip = (
                any(value > 0.0 for value in repetition_gains)
                and any(value < 0.0 for value in repetition_gains)
            )
            effect_rows.append({
                "workload_id": workload_id,
                "workload_group": groups[workload_id],
                "candidate_design_id": candidate_id,
                "training_success_difference": (
                    training.success_difference
                ),
                "training_cost_saving": training.cost_saving,
                "training_utility_gain": training.utility_gain,
                "evaluation_success_difference": (
                    evaluation.success_difference
                ),
                "evaluation_cost_saving": evaluation.cost_saving,
                "evaluation_utility_gain": evaluation.utility_gain,
                "training_repetition_sign_flip": sign_flip,
                "training_repetition_gains_json": json.dumps(
                    repetition_gains
                ),
            })

    stratum_rows: list[dict[str, Any]] = []
    for group_id, group_workloads in sorted(config.workload_groups.items()):
        for candidate_id in candidates:
            train_values = [
                training_effects[(workload_id, candidate_id)]
                for workload_id in group_workloads
            ]
            evaluation_values = [
                evaluation_effects[(workload_id, candidate_id)]
                for workload_id in group_workloads
            ]
            stratum_rows.append({
                "workload_group": group_id,
                "candidate_design_id": candidate_id,
                "workload_count": len(group_workloads),
                "mean_training_success_difference": mean(
                    value.success_difference for value in train_values
                ),
                "mean_training_cost_saving": mean(
                    value.cost_saving for value in train_values
                ),
                "mean_training_utility_gain": mean(
                    value.utility_gain for value in train_values
                ),
                "training_positive_workload_fraction": sum(
                    value.utility_gain > 0.0 for value in train_values
                ) / len(train_values),
                "mean_evaluation_success_difference": mean(
                    value.success_difference for value in evaluation_values
                ),
                "mean_evaluation_cost_saving": mean(
                    value.cost_saving for value in evaluation_values
                ),
                "mean_evaluation_utility_gain": mean(
                    value.utility_gain for value in evaluation_values
                ),
                "evaluation_positive_workload_fraction": sum(
                    value.utility_gain > 0.0
                    for value in evaluation_values
                ) / len(evaluation_values),
            })

    assignments: list[dict[str, Any]] = []
    for workload_id in workload_ids:
        group_id = groups[workload_id]
        group_training = tuple(
            candidate
            for candidate in config.workload_groups[group_id]
            if candidate != workload_id
        )
        global_training = tuple(
            candidate for candidate in workload_ids if candidate != workload_id
        )
        group_choice = _choose_policy_design(
            config,
            training_effects,
            group_training,
        )
        global_choice = _choose_policy_design(
            config,
            training_effects,
            global_training,
        )
        oracle_ranked = [
            (
                evaluation_effects[(workload_id, candidate_id)].utility_gain,
                -config.design_ids.index(candidate_id),
                candidate_id,
            )
            for candidate_id in candidates
        ] + [(
            0.0,
            -config.design_ids.index(config.safe_design_id),
            config.safe_design_id,
        )]
        oracle_best = max(oracle_ranked)[2]

        def evaluated(choice: _PolicyChoice) -> _Effect:
            if choice.design_id == config.safe_design_id:
                return _Effect(0.0, 0.0, 0.0)
            return evaluation_effects[(workload_id, choice.design_id)]

        group_evaluation = evaluated(group_choice)
        global_evaluation = evaluated(global_choice)
        assignments.append({
            "workload_id": workload_id,
            "workload_group": group_id,
            "evaluation_oracle_best_design_id": oracle_best,
            "group_policy_selected_design_id": group_choice.design_id,
            "group_policy_reason": group_choice.reason,
            "group_policy_training_workload_count": (
                group_choice.training_workload_count
            ),
            "group_policy_estimated_gain": group_choice.estimated_gain,
            "group_policy_positive_fraction": (
                group_choice.positive_fraction
            ),
            "group_policy_evaluation_success_difference": (
                group_evaluation.success_difference
            ),
            "group_policy_evaluation_cost_saving": (
                group_evaluation.cost_saving
            ),
            "group_policy_evaluation_utility_gain": (
                group_evaluation.utility_gain
            ),
            "global_policy_selected_design_id": global_choice.design_id,
            "global_policy_reason": global_choice.reason,
            "global_policy_training_workload_count": (
                global_choice.training_workload_count
            ),
            "global_policy_estimated_gain": global_choice.estimated_gain,
            "global_policy_positive_fraction": (
                global_choice.positive_fraction
            ),
            "global_policy_evaluation_success_difference": (
                global_evaluation.success_difference
            ),
            "global_policy_evaluation_cost_saving": (
                global_evaluation.cost_saving
            ),
            "global_policy_evaluation_utility_gain": (
                global_evaluation.utility_gain
            ),
        })

    workload_effects_path = output / "workload_effects.csv"
    stratum_summary_path = output / "stratum_summary.csv"
    policy_assignments_path = output / "policy_assignments.csv"
    evaluation_path = output / "heterogeneity_evaluation.json"
    manifest_path = output / "heterogeneity_manifest.json"
    _write_csv(effect_rows, workload_effects_path)
    _write_csv(stratum_rows, stratum_summary_path)
    _write_csv(assignments, policy_assignments_path)

    evaluation = {
        "schema_version": HETEROGENEITY_EVALUATION_SCHEMA_VERSION,
        "audit_id": config.audit_id,
        "status": "COMPLETE",
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "selection_uses_evaluation_repetitions": False,
        "policy_selection": (
            "leave-one-workload-out-on-training-repetitions"
        ),
        "evaluation_unit": "held-out-repetition-block-per-workload",
        "utility_scope": (
            "service-utility-only; storage and transition costs are not "
            "allocated to conditional workload policies"
        ),
        "task_value": task_value,
        "resource_cost_weight": resource_cost_weight,
        "training_repetitions": list(config.training_repetitions),
        "evaluation_repetitions": list(config.evaluation_repetitions),
        "workload_count": len(workload_ids),
        "workload_group_counts": dict(sorted(Counter(groups.values()).items())),
        "policies": {
            "safe_origin": {
                "workload_count": len(workload_ids),
                "selection_counts": {
                    config.safe_design_id: len(workload_ids)
                },
                "fallback_count": len(workload_ids),
                "mean_evaluation_utility_gain": 0.0,
                "mean_evaluation_success_difference": 0.0,
                "mean_evaluation_cost_saving": 0.0,
            },
            "global_leave_one_workload_out": _policy_summary(
                assignments,
                policy_prefix="global_policy",
                safe_design_id=config.safe_design_id,
            ),
            "groupwise_leave_one_workload_out": _policy_summary(
                assignments,
                policy_prefix="group_policy",
                safe_design_id=config.safe_design_id,
            ),
        },
        "limitations": [
            "This is post-hoc method development on an inspected Oracle.",
            "The evaluation block reuses workload identities and is not an "
            "independent-workload confirmatory split.",
            "Policy thresholds are diagnostics, not confidence bounds.",
            "Conditional materialization storage and transition costs are "
            "not identified by the current global-design Oracle.",
        ],
    }
    _write_json(evaluation, evaluation_path)
    snapshot = reduced_oracle_snapshot(
        source,
        config.design_ids,
        scope="full-declared-design-set",
    )
    manifest = {
        "schema_version": HETEROGENEITY_EVALUATION_SCHEMA_VERSION,
        "audit_id": config.audit_id,
        "status": "COMPLETE",
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "deployment_mutations_performed": False,
        "secrets_recorded": False,
        "audit_config_path": str(config.source_path),
        "audit_config_sha256": config.source_sha256,
        "oracle_config_path": str(oracle.source_path),
        "oracle_config_sha256": oracle.source_sha256,
        "oracle_output_dir": str(source),
        **snapshot.to_manifest_fields("oracle_snapshot"),
        "workload_effects_path": str(workload_effects_path),
        "stratum_summary_path": str(stratum_summary_path),
        "policy_assignments_path": str(policy_assignments_path),
        "evaluation_path": str(evaluation_path),
        "workload_count": len(workload_ids),
        "design_count": len(config.design_ids),
    }
    _write_json(manifest, manifest_path)
    return {**manifest, "manifest_path": str(manifest_path)}
