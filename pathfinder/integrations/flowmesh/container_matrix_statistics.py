"""Read-only descriptive statistics for a completed container matrix run.

This module deliberately reports only measurements already preserved by the
container matrix runner.  It does not turn component service time into
end-to-end latency, infer network throughput, calculate monetary cost, rank
designs, or evaluate semantic quality.
"""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .container_dag import (
    FlowMeshContainerDagError,
    _checksums,
    _json_bytes,
    _jsonl_bytes,
    _sha256_bytes,
    _write_documents,
)
from .container_matrix_runner import (
    _read_json,
    _read_jsonl,
    verify_flowmesh_container_matrix_run,
)


FLOWMESH_CONTAINER_MATRIX_STATISTICS_REPORT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-descriptive-report/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_STATISTICS_CELL_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-descriptive-cell/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_STATISTICS_ROUTE_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-descriptive-route/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_STATISTICS_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-descriptive-statistics/v1alpha1"
)

_REPORT_FILE = "flowmesh-container-matrix-descriptive-report.json"
_CELL_FILE = "flowmesh-container-matrix-descriptive-cells.jsonl"
_ROUTE_FILE = "flowmesh-container-matrix-descriptive-routes.jsonl"
_MANIFEST_FILE = "flowmesh-container-matrix-descriptive-manifest.json"
_OUTPUT_FILES = {_REPORT_FILE, _CELL_FILE, _ROUTE_FILE, _MANIFEST_FILE}
_EXPECTED_WORKLOADS = ("W1", "W2", "W3", "W4")
_EXPECTED_DESIGNS = tuple(f"D{index}" for index in range(8))
_EXPECTED_NODES = tuple(f"N{index}" for index in range(1, 9))
_EXPECTED_PHASES = frozenset({"A", "B", "unconditional"})
_EXPECTED_OPERATION_KINDS = frozenset(
    {
        "barrier",
        "cache_insert",
        "cache_lookup",
        "cache_read",
        "compute",
        "control",
        "index_query",
        "network_transfer",
        "storage_read",
    }
)
_PAYLOAD_KINDS = frozenset(
    {"storage_read", "cache_read", "network_transfer"}
)
_ANALYSIS_CLASS = "posthoc-descriptive-infrastructure-conformance-only"
_CACHE_LOOKUP_INTERPRETATION = (
    "counts describe the frozen cache snapshot and trace; they are "
    "not an estimate of a real cache policy hit rate"
)
_MEASUREMENT_BOUNDARIES = (
    "Only executed operations with complete recorded telemetry are "
    "included in observed numerical totals; inactive branches retain "
    "null observations and are never converted to zero.",
    "Operation component service-time sums are not trial end-to-end "
    "latency and exclude FlowMesh queueing and scheduling time.",
    "Operation byte sums are repeated physical-work accounting, not "
    "unique dataset size; network bytes are HTTP payload bytes, not "
    "complete wire bytes.",
    "Application-shaping targets are configured interventions rather "
    "than independent measurements of a physical network.",
    "No throughput, monetary cost, speedup, best-design ranking, "
    "semantic quality, uncertainty interval, or significance result "
    "is computed.",
    "Eight containers on one host do not constitute eight independent "
    "physical machines.",
)
_TOTAL_INTEGER_FIELDS = (
    "trial_count",
    "workflow_count",
    "planned_operation_count",
    "executed_operation_count",
    "inactive_operation_count",
    "planned_operation_logical_bytes_sum",
    "observed_operation_logical_bytes_sum",
    "observed_operation_physical_bytes_sum",
    "storage_read_payload_bytes_sum",
    "cache_read_payload_bytes_sum",
    "network_payload_bytes_sum",
    "cache_lookup_count",
    "cache_lookup_hit_count",
    "cache_lookup_miss_count",
    "cache_eviction_event_count",
    "cache_evicted_entry_count",
)
_TOTAL_FLOAT_FIELDS = (
    "operation_component_service_time_ms_sum",
    "network_component_service_time_ms_sum",
    "configured_application_shaping_target_ms_sum",
    "network_http_exchange_ms_sum",
    "application_shaping_sleep_ms_sum",
    "fixture_materialization_ms_sum_excluded_from_storage_measurement",
)
_AGGREGATE_ROUNDING_TOLERANCE = 0.0001
_REPORT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "analysis_class",
        "run_audit",
        "matrix_dimensions",
        "overall_totals",
        "workload_totals",
        "design_totals",
        "cache_lookup_outcome_interpretation",
        "cell_statistics_count",
        "cell_statistics_sha256",
        "route_statistics_count",
        "route_statistics_sha256",
        "source_artifact_fingerprint_before",
        "source_artifact_fingerprint_after_analysis",
        "source_artifacts_modified",
        "measurement_boundaries",
        "posthoc",
        "parameters_fitted",
        "cost_metrics_computed",
        "design_ranking_computed",
        "network_throughput_derived",
        "end_to_end_latency_measured",
        "queue_time_measured",
        "semantic_task_quality_evaluated",
        "llm_called",
        "services_started",
        "workflow_submitted",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    }
)
_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "analysis_class",
        "run_id",
        "matrix_id",
        "source_binding_checked",
        "completed_trial_count",
        "cell_count",
        "route_count",
        "source_directory_sha256",
        "parameters_fitted",
        "external_services_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
        "output_sha256",
    }
)
_RUN_AUDIT_FIELDS = frozenset(
    {
        "run_id",
        "matrix_id",
        "run_status",
        "verifier_status",
        "source_binding_checked",
        "completed_trial_count",
        "planned_operation_count",
        "executed_operation_count",
        "inactive_operation_count",
        "workflow_count",
        "flowmesh_workflow_count",
        "worker_id",
        "infrastructure_recovery_count",
        "abandoned_workflow_count",
        "replay_result_adoption_count",
        "adopted_replay_operation_count",
    }
)
_ROUTE_FIELDS = frozenset(
    {
        "schema_version",
        "workload_class",
        "workload_id",
        "design_id",
        "phase",
        "operation_kind",
        "execution_node_id",
        "destination_node_id",
        "executed_operation_count",
        "observed_operation_logical_bytes_sum",
        "observed_operation_physical_bytes_sum",
        "operation_component_service_time_ms_sum",
        "configured_application_shaping_target_ms_sum",
        "cache_lookup_outcome_counts",
        "throughput_derived",
        "cost_computed",
        "eligible_for_scientific_claims",
    }
)


class FlowMeshContainerMatrixStatisticsError(FlowMeshContainerDagError):
    """Raised when a matrix run cannot support descriptive statistics."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FlowMeshContainerMatrixStatisticsError(message)


def _number(value: Any, field: str) -> float:
    _require(
        isinstance(value, (int, float)) and not isinstance(value, bool),
        f"{field} must be numeric",
    )
    result = float(value)
    _require(math.isfinite(result) and result >= 0.0, f"{field} is invalid")
    return result


def _integer(value: Any, field: str) -> int:
    _require(type(value) is int and value >= 0, f"{field} is invalid")
    return value


def _rounded_sum(values: Iterable[float]) -> float:
    # Match the runner's preserved aggregation order exactly.  The source
    # verifier has already validated these finite values.
    return round(sum(values), 6)


def _fingerprint_directory(root: Path) -> dict[str, str]:
    _require(root.is_dir(), "statistics source directory does not exist")
    entries = sorted(path for path in root.iterdir() if path.is_file())
    _require(entries, "statistics source directory has no files")
    return {
        path.name: sha256(path.read_bytes()).hexdigest()
        for path in entries
    }


def _path_is_within(candidate: Path, parent: Path) -> bool:
    return candidate == parent or parent in candidate.parents


def _require_disjoint_paths(
    *, output_dir: Path, inputs: Mapping[str, Path]
) -> None:
    roots = list(inputs.values())
    _require(
        len(set(roots)) == len(roots),
        "matrix statistics inputs must be distinct directories",
    )
    for label, source in inputs.items():
        _require(
            not _path_is_within(output_dir, source)
            and not _path_is_within(source, output_dir),
            "matrix statistics output must be outside every input "
            f"directory ({label})",
        )


def _empty_totals() -> dict[str, Any]:
    return {
        "trial_count": 0,
        "workflow_count": 0,
        "planned_operation_count": 0,
        "executed_operation_count": 0,
        "inactive_operation_count": 0,
        "planned_operation_logical_bytes_sum": 0,
        "observed_operation_logical_bytes_sum": 0,
        "observed_operation_physical_bytes_sum": 0,
        "operation_component_service_time_ms_sum": 0.0,
        "storage_read_payload_bytes_sum": 0,
        "cache_read_payload_bytes_sum": 0,
        "network_payload_bytes_sum": 0,
        "network_component_service_time_ms_sum": 0.0,
        "configured_application_shaping_target_ms_sum": 0.0,
        "network_http_exchange_ms_sum": 0.0,
        "application_shaping_sleep_ms_sum": 0.0,
        "fixture_materialization_ms_sum_excluded_from_storage_measurement": 0.0,
        "cache_lookup_count": 0,
        "cache_lookup_hit_count": 0,
        "cache_lookup_miss_count": 0,
        "cache_eviction_event_count": 0,
        "cache_evicted_entry_count": 0,
    }


def _add_totals(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    for field in _TOTAL_INTEGER_FIELDS:
        target[field] += int(source[field])
    for field in _TOTAL_FLOAT_FIELDS:
        target[field] = round(
            float(target[field]) + float(source[field]), 6
        )


def _totals_reconcile(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> bool:
    if set(left) != set(_empty_totals()) or set(right) != set(_empty_totals()):
        return False
    if any(left[field] != right[field] for field in _TOTAL_INTEGER_FIELDS):
        return False
    return all(
        math.isclose(
            float(left[field]),
            float(right[field]),
            rel_tol=0.0,
            abs_tol=_AGGREGATE_ROUNDING_TOLERANCE,
        )
        for field in _TOTAL_FLOAT_FIELDS
    )


def _aggregate_float_reconciles(left: float, right: Any) -> bool:
    try:
        candidate = float(right)
    except (TypeError, ValueError):
        return False
    return math.isclose(
        left,
        candidate,
        rel_tol=0.0,
        abs_tol=_AGGREGATE_ROUNDING_TOLERANCE,
    )


def _trial_observation(
    trial: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    executed = [row for row in operations if row.get("executed") is True]
    inactive = [row for row in operations if row.get("executed") is False]
    _require(
        len(executed) + len(inactive) == len(operations),
        "matrix operation executed flag is invalid",
    )
    _require(
        len(operations) == trial.get("planned_operation_count")
        and len(executed) == trial.get("executed_operation_count")
        and len(inactive) == trial.get("inactive_operation_count"),
        "trial operation counts disagree with the operation ledger",
    )
    _require(
        all(
            row.get("telemetry_recorded") is True
            and row.get("telemetry_complete") is True
            and row.get("semantic_task_quality_evaluated") is False
            and row.get("credentials_recorded") is False
            and row.get("idempotent_replay") is False
            for row in executed
        ),
        "executed operation telemetry is incomplete, replayed, or unsafe",
    )
    _require(
        all(
            row.get("telemetry_recorded") is False
            and row.get("telemetry_complete") is False
            and row.get("logical_bytes") is None
            and row.get("physical_bytes") is None
            and row.get("service_time_ms") is None
            for row in inactive
        ),
        "inactive operation contains a manufactured observation",
    )

    logical_bytes = sum(
        _integer(row.get("logical_bytes"), "logical_bytes")
        for row in executed
    )
    physical_bytes = sum(
        _integer(row.get("physical_bytes"), "physical_bytes")
        for row in executed
    )
    planned_bytes = sum(
        _integer(row.get("planned_logical_bytes"), "planned_logical_bytes")
        for row in operations
    )
    service_values = [
        _number(row.get("service_time_ms"), "service_time_ms")
        for row in executed
    ]
    network_rows = [
        row for row in executed
        if row.get("operation_kind") == "network_transfer"
    ]
    lookup_rows = [
        row for row in executed
        if row.get("operation_kind") == "cache_lookup"
    ]
    lookup_outcomes = Counter(row.get("cache_result") for row in lookup_rows)
    _require(
        set(lookup_outcomes).issubset({"hit", "miss"}),
        "cache lookup outcome is invalid",
    )
    evictions = [
        row.get("cache_evictions", [])
        for row in executed
        if row.get("operation_kind") in {
            "cache_lookup", "cache_read", "cache_insert"
        }
    ]
    _require(
        all(isinstance(value, list) for value in evictions),
        "cache eviction evidence is invalid",
    )

    payload_by_kind = {
        kind: sum(
            _integer(row.get("physical_bytes"), "physical_bytes")
            for row in executed
            if row.get("operation_kind") == kind
        )
        for kind in _PAYLOAD_KINDS
    }
    observation = {
        "trial_key": trial["trial_key"],
        "repetition": trial["repetition"],
        "workflow_count": _integer(
            trial.get("workflow_count"), "workflow_count"
        ),
        "planned_operation_count": len(operations),
        "executed_operation_count": len(executed),
        "inactive_operation_count": len(inactive),
        "planned_operation_logical_bytes_sum": planned_bytes,
        "observed_operation_logical_bytes_sum": logical_bytes,
        "observed_operation_physical_bytes_sum": physical_bytes,
        "operation_component_service_time_ms_sum": _rounded_sum(
            service_values
        ),
        "storage_read_payload_bytes_sum": payload_by_kind["storage_read"],
        "cache_read_payload_bytes_sum": payload_by_kind["cache_read"],
        "network_payload_bytes_sum": payload_by_kind["network_transfer"],
        "network_component_service_time_ms_sum": _rounded_sum(
            _number(row.get("service_time_ms"), "network service_time_ms")
            for row in network_rows
        ),
        "configured_application_shaping_target_ms_sum": _rounded_sum(
            _number(
                row.get("application_shaping_target_ms"),
                "application_shaping_target_ms",
            )
            for row in network_rows
        ),
        "network_http_exchange_ms_sum": _rounded_sum(
            _number(
                row.get("network_http_exchange_ms"),
                "network_http_exchange_ms",
            )
            for row in network_rows
        ),
        "application_shaping_sleep_ms_sum": _rounded_sum(
            _number(
                row.get("application_shaping_sleep_ms"),
                "application_shaping_sleep_ms",
            )
            for row in network_rows
        ),
        "fixture_materialization_ms_sum_excluded_from_storage_measurement": (
            _rounded_sum(
                _number(
                    row.get(
                        "fixture_materialization_ms_excluded_from_storage_measurement"
                    ),
                    "fixture_materialization_ms",
                )
                for row in executed
            )
        ),
        "cache_lookup_count": len(lookup_rows),
        "cache_lookup_hit_count": lookup_outcomes.get("hit", 0),
        "cache_lookup_miss_count": lookup_outcomes.get("miss", 0),
        "cache_eviction_event_count": sum(bool(value) for value in evictions),
        "cache_evicted_entry_count": sum(len(value) for value in evictions),
        "cache_outcomes_match_frozen_plan": trial.get(
            "cache_outcomes_match_frozen_plan"
        ),
    }
    telemetry = trial.get("telemetry")
    _require(isinstance(telemetry, Mapping), "trial telemetry is missing")
    _require(
        telemetry.get("service_time_ms_sum")
        == observation["operation_component_service_time_ms_sum"]
        and telemetry.get("logical_bytes_sum")
        == observation["observed_operation_logical_bytes_sum"]
        and telemetry.get("physical_bytes_sum")
        == observation["observed_operation_physical_bytes_sum"],
        "trial telemetry disagrees with operation observations",
    )
    return observation


def _totals_for_observations(
    observations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    totals = _empty_totals()
    for observation in observations:
        source = dict(observation)
        source["trial_count"] = 1
        _add_totals(totals, source)
    return totals


def _summary_rows(
    trials: Sequence[Mapping[str, Any]],
    observations: Mapping[str, Mapping[str, Any]],
    *,
    dimension: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for trial in trials:
        grouped[str(trial[dimension])].append(observations[str(trial["trial_key"])])
    return [
        {
            dimension: value,
            "totals": _totals_for_observations(grouped[value]),
        }
        for value in sorted(grouped)
    ]


def _cell_rows(
    trials: Sequence[Mapping[str, Any]],
    observations: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    workload_ids: dict[str, set[str]] = defaultdict(set)
    for trial in trials:
        workload_class = str(trial["workload_class"])
        design_id = str(trial["design_id"])
        workload_ids[workload_class].add(str(trial["workload_id"]))
        grouped[(workload_class, design_id)].append(
            observations[str(trial["trial_key"])]
        )
    _require(
        all(len(values) == 1 for values in workload_ids.values()),
        "workload class maps to multiple workload IDs",
    )
    rows: list[dict[str, Any]] = []
    for workload_class in _EXPECTED_WORKLOADS:
        for design_id in _EXPECTED_DESIGNS:
            values = sorted(
                grouped[(workload_class, design_id)],
                key=lambda row: int(row["repetition"]),
            )
            _require(
                [row["repetition"] for row in values] == [0, 1],
                "matrix cell does not contain repetitions 0 and 1",
            )
            rows.append(
                {
                    "schema_version": (
                        FLOWMESH_CONTAINER_MATRIX_STATISTICS_CELL_SCHEMA_VERSION
                    ),
                    "workload_class": workload_class,
                    "workload_id": next(iter(workload_ids[workload_class])),
                    "design_id": design_id,
                    "trial_count": 2,
                    "repetitions_present": [0, 1],
                    "repetition_observations": values,
                    "totals": _totals_for_observations(values),
                    "cost_computed": False,
                    "design_rank_computed": False,
                    "eligible_for_scientific_claims": False,
                }
            )
    return rows


def _route_rows(
    trials: Sequence[Mapping[str, Any]],
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    dimensions = {
        str(trial["trial_key"]): (
            str(trial["workload_class"]),
            str(trial["workload_id"]),
            str(trial["design_id"]),
        )
        for trial in trials
    }
    grouped: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for operation in operations:
        if operation.get("executed") is not True:
            continue
        trial_key = str(operation["trial_key"])
        _require(trial_key in dimensions, "operation references an unknown trial")
        workload_class, workload_id, design_id = dimensions[trial_key]
        key = (
            workload_class,
            workload_id,
            design_id,
            str(operation.get("phase")),
            str(operation["operation_kind"]),
            str(operation["execution_node_id"]),
            str(operation["destination_node_id"]),
        )
        grouped[key].append(operation)

    rows: list[dict[str, Any]] = []
    for key in sorted(grouped):
        (
            workload_class,
            workload_id,
            design_id,
            phase,
            operation_kind,
            execution_node_id,
            destination_node_id,
        ) = key
        values = grouped[key]
        cache_outcomes = Counter(
            row.get("cache_result")
            for row in values
            if operation_kind == "cache_lookup"
        )
        rows.append(
            {
                "schema_version": (
                    FLOWMESH_CONTAINER_MATRIX_STATISTICS_ROUTE_SCHEMA_VERSION
                ),
                "workload_class": workload_class,
                "workload_id": workload_id,
                "design_id": design_id,
                "phase": phase,
                "operation_kind": operation_kind,
                "execution_node_id": execution_node_id,
                "destination_node_id": destination_node_id,
                "executed_operation_count": len(values),
                "observed_operation_logical_bytes_sum": sum(
                    _integer(row.get("logical_bytes"), "logical_bytes")
                    for row in values
                ),
                "observed_operation_physical_bytes_sum": sum(
                    _integer(row.get("physical_bytes"), "physical_bytes")
                    for row in values
                ),
                "operation_component_service_time_ms_sum": _rounded_sum(
                    _number(row.get("service_time_ms"), "service_time_ms")
                    for row in values
                ),
                "configured_application_shaping_target_ms_sum": (
                    _rounded_sum(
                        _number(
                            row.get("application_shaping_target_ms"),
                            "application_shaping_target_ms",
                        )
                        for row in values
                        if row.get("application_shaping_target_ms") is not None
                    )
                ),
                "cache_lookup_outcome_counts": dict(
                    sorted(
                        (str(name), count)
                        for name, count in cache_outcomes.items()
                    )
                ),
                "throughput_derived": False,
                "cost_computed": False,
                "eligible_for_scientific_claims": False,
            }
        )
    return rows


def _source_boundaries(
    summary: Mapping[str, Any], verification: Mapping[str, Any]
) -> None:
    _require(summary.get("status") == "COMPLETE", "matrix run is incomplete")
    _require(
        verification.get("status") == "VERIFIED"
        and verification.get("source_binding_checked") is True,
        "matrix run is not bound to all frozen inputs",
    )
    for field in (
        "llm_called",
        "queue_time_measured",
        "semantic_task_quality_evaluated",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(
            summary.get(field) is False,
            f"matrix statistics boundary changed: {field}",
        )
    _require(
        verification.get("adopted_replay_operation_count") == 0
        and verification.get("replay_result_adoption_count") == 0,
        "descriptive statistics refuse replay-adopted measurements",
    )


def _validate_matrix_shape(
    trials: Sequence[Mapping[str, Any]],
    operations: Sequence[Mapping[str, Any]],
) -> dict[str, list[Mapping[str, Any]]]:
    _require(len(trials) == 64, "matrix statistics require 64 trials")
    _require(len(operations) == 500, "matrix statistics require 500 operations")
    trial_keys = [str(row.get("trial_key")) for row in trials]
    _require(len(set(trial_keys)) == 64, "matrix trial keys are not unique")
    _require(
        {str(row.get("workload_class")) for row in trials}
        == set(_EXPECTED_WORKLOADS),
        "matrix workload classes changed",
    )
    _require(
        {str(row.get("design_id")) for row in trials}
        == set(_EXPECTED_DESIGNS),
        "matrix design IDs changed",
    )
    _require(
        all(
            row.get("status") == "COMPLETE"
            and row.get("semantic_task_quality_evaluated") is False
            and row.get("credentials_recorded") is False
            and row.get("eligible_for_scientific_claims") is False
            and row.get("service_time_sum_is_end_to_end_latency") is False
            and row.get("queue_time_measured") is False
            for row in trials
        ),
        "matrix trial evidence boundary changed",
    )
    operation_keys = [str(row.get("operation_key")) for row in operations]
    _require(
        len(set(operation_keys)) == 500,
        "matrix operation keys are not unique",
    )
    by_trial: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for operation in operations:
        trial_key = str(operation.get("trial_key"))
        _require(trial_key in set(trial_keys), "operation references unknown trial")
        by_trial[trial_key].append(operation)
    _require(set(by_trial) == set(trial_keys), "matrix trial has no operations")
    return by_trial


def _validate_cell_rows(cells: Sequence[Mapping[str, Any]]) -> None:
    """Recompute every cell from its two preserved trial observations."""

    expected_cell_fields = {
        "schema_version",
        "workload_class",
        "workload_id",
        "design_id",
        "trial_count",
        "repetitions_present",
        "repetition_observations",
        "totals",
        "cost_computed",
        "design_rank_computed",
        "eligible_for_scientific_claims",
    }
    expected_observation_fields = {
        "trial_key",
        "repetition",
        "workflow_count",
        "planned_operation_count",
        "executed_operation_count",
        "inactive_operation_count",
        "planned_operation_logical_bytes_sum",
        "observed_operation_logical_bytes_sum",
        "observed_operation_physical_bytes_sum",
        "operation_component_service_time_ms_sum",
        "storage_read_payload_bytes_sum",
        "cache_read_payload_bytes_sum",
        "network_payload_bytes_sum",
        "network_component_service_time_ms_sum",
        "configured_application_shaping_target_ms_sum",
        "network_http_exchange_ms_sum",
        "application_shaping_sleep_ms_sum",
        "fixture_materialization_ms_sum_excluded_from_storage_measurement",
        "cache_lookup_count",
        "cache_lookup_hit_count",
        "cache_lookup_miss_count",
        "cache_eviction_event_count",
        "cache_evicted_entry_count",
        "cache_outcomes_match_frozen_plan",
    }
    integer_fields = (
        "workflow_count",
        "planned_operation_count",
        "executed_operation_count",
        "inactive_operation_count",
        "planned_operation_logical_bytes_sum",
        "observed_operation_logical_bytes_sum",
        "observed_operation_physical_bytes_sum",
        "storage_read_payload_bytes_sum",
        "cache_read_payload_bytes_sum",
        "network_payload_bytes_sum",
        "cache_lookup_count",
        "cache_lookup_hit_count",
        "cache_lookup_miss_count",
        "cache_eviction_event_count",
        "cache_evicted_entry_count",
    )
    float_fields = (
        "operation_component_service_time_ms_sum",
        "network_component_service_time_ms_sum",
        "configured_application_shaping_target_ms_sum",
        "network_http_exchange_ms_sum",
        "application_shaping_sleep_ms_sum",
        "fixture_materialization_ms_sum_excluded_from_storage_measurement",
    )
    trial_keys: set[str] = set()
    workload_ids: dict[str, set[str]] = defaultdict(set)
    for cell in cells:
        _require(
            set(cell) == expected_cell_fields,
            "matrix cell statistics field set changed",
        )
        workload_class = str(cell["workload_class"])
        workload_id = cell.get("workload_id")
        _require(
            isinstance(workload_id, str) and bool(workload_id),
            "matrix cell workload ID is invalid",
        )
        workload_ids[workload_class].add(workload_id)
        observations = cell.get("repetition_observations")
        _require(
            isinstance(observations, list)
            and len(observations) == 2
            and all(isinstance(row, Mapping) for row in observations),
            "matrix cell repetition observations are invalid",
        )
        _require(
            [row.get("repetition") for row in observations] == [0, 1],
            "matrix cell repetition observations changed",
        )
        for observation in observations:
            _require(
                set(observation) == expected_observation_fields,
                "matrix cell observation field set changed",
            )
            trial_key = observation.get("trial_key")
            _require(
                isinstance(trial_key, str)
                and bool(trial_key)
                and trial_key not in trial_keys,
                "matrix cell trial identity is invalid or repeated",
            )
            trial_keys.add(trial_key)
            for field in integer_fields:
                _integer(observation.get(field), f"cell observation {field}")
            for field in float_fields:
                _number(observation.get(field), f"cell observation {field}")
            _require(
                observation["planned_operation_count"]
                == observation["executed_operation_count"]
                + observation["inactive_operation_count"]
                and observation["cache_lookup_count"]
                == observation["cache_lookup_hit_count"]
                + observation["cache_lookup_miss_count"]
                and (
                    observation.get("cache_outcomes_match_frozen_plan") is None
                    or observation.get("cache_outcomes_match_frozen_plan")
                    is True
                ),
                "matrix cell observation counts do not reconcile",
            )
        totals = cell.get("totals")
        _require(
            isinstance(totals, Mapping)
            and _totals_reconcile(
                _totals_for_observations(observations), totals
            ),
            "matrix cell repetition observations do not reconcile",
        )
    _require(len(trial_keys) == 64, "matrix cell trial coverage changed")
    _require(
        set(workload_ids) == set(_EXPECTED_WORKLOADS)
        and all(len(values) == 1 for values in workload_ids.values()),
        "matrix cell workload IDs do not reconcile",
    )


def _summary_rows_from_cells(
    cells: Sequence[Mapping[str, Any]],
    *,
    dimension: str,
    expected_values: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value in expected_values:
        totals = _empty_totals()
        matching = [cell for cell in cells if cell.get(dimension) == value]
        _require(matching, f"matrix {dimension} cell coverage changed")
        for cell in matching:
            _add_totals(totals, cell["totals"])
        rows.append({dimension: value, "totals": totals})
    return rows


def _validate_route_rows(
    routes: Sequence[Mapping[str, Any]],
    cells: Sequence[Mapping[str, Any]],
) -> None:
    """Validate route identities and reconcile their measured cell totals."""

    cell_workload_ids = {
        (str(cell["workload_class"]), str(cell["design_id"])): cell.get(
            "workload_id"
        )
        for cell in cells
    }
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    identities: set[tuple[Any, ...]] = set()
    for row in routes:
        _require(
            set(row) == _ROUTE_FIELDS,
            "matrix route statistics field set changed",
        )
        workload_class = row.get("workload_class")
        design_id = row.get("design_id")
        phase = row.get("phase")
        operation_kind = row.get("operation_kind")
        execution_node_id = row.get("execution_node_id")
        destination_node_id = row.get("destination_node_id")
        identity = (
            workload_class,
            row.get("workload_id"),
            design_id,
            phase,
            operation_kind,
            execution_node_id,
            destination_node_id,
        )
        _require(identity not in identities, "matrix route identity is repeated")
        identities.add(identity)
        _require(
            workload_class in _EXPECTED_WORKLOADS
            and design_id in _EXPECTED_DESIGNS
            and row.get("workload_id")
            == cell_workload_ids.get((str(workload_class), str(design_id))),
            "matrix route workload or design identity is invalid",
        )
        _require(
            phase in _EXPECTED_PHASES
            and (
                (design_id in {"D3", "D7"} and phase in {"A", "B"})
                or (design_id not in {"D3", "D7"} and phase == "unconditional")
            ),
            "matrix route phase is invalid",
        )
        _require(
            operation_kind in _EXPECTED_OPERATION_KINDS,
            "matrix route operation kind is invalid",
        )
        _require(
            execution_node_id in _EXPECTED_NODES
            and destination_node_id in _EXPECTED_NODES
            and (
                operation_kind == "network_transfer"
                or execution_node_id == destination_node_id
            ),
            "matrix route node identity is invalid",
        )
        executed_count = _integer(
            row.get("executed_operation_count"),
            "route executed_operation_count",
        )
        _require(executed_count > 0, "matrix route is empty")
        _integer(
            row.get("observed_operation_logical_bytes_sum"),
            "route observed_operation_logical_bytes_sum",
        )
        physical_bytes = _integer(
            row.get("observed_operation_physical_bytes_sum"),
            "route observed_operation_physical_bytes_sum",
        )
        _number(
            row.get("operation_component_service_time_ms_sum"),
            "route operation_component_service_time_ms_sum",
        )
        shaping_target = _number(
            row.get("configured_application_shaping_target_ms_sum"),
            "route configured_application_shaping_target_ms_sum",
        )
        _require(
            operation_kind in _PAYLOAD_KINDS or physical_bytes == 0,
            "non-payload route reports physical bytes",
        )
        _require(
            operation_kind == "network_transfer" or shaping_target == 0.0,
            "non-network route reports an application shaping target",
        )
        outcomes = row.get("cache_lookup_outcome_counts")
        _require(
            isinstance(outcomes, Mapping),
            "matrix route cache lookup outcomes are invalid",
        )
        if operation_kind == "cache_lookup":
            _require(
                set(outcomes).issubset({"hit", "miss"})
                and sum(
                    _integer(value, "route cache lookup outcome count")
                    for value in outcomes.values()
                )
                == executed_count,
                "matrix route cache lookup outcomes do not reconcile",
            )
        else:
            _require(
                not outcomes,
                "non-lookup route reports cache lookup outcomes",
            )
        grouped[(str(workload_class), str(design_id))].append(row)

    for cell in cells:
        key = (str(cell["workload_class"]), str(cell["design_id"]))
        values = grouped.get(key, [])
        totals = cell.get("totals")
        _require(
            values and isinstance(totals, Mapping),
            "matrix route cell coverage is incomplete",
        )
        lookup_outcomes = Counter()
        for row in values:
            lookup_outcomes.update(row["cache_lookup_outcome_counts"])
        network_rows = [
            row
            for row in values
            if row["operation_kind"] == "network_transfer"
        ]
        _require(
            sum(int(row["executed_operation_count"]) for row in values)
            == totals.get("executed_operation_count")
            and sum(
                int(row["observed_operation_logical_bytes_sum"])
                for row in values
            )
            == totals.get("observed_operation_logical_bytes_sum")
            and sum(
                int(row["observed_operation_physical_bytes_sum"])
                for row in values
            )
            == totals.get("observed_operation_physical_bytes_sum")
            and _aggregate_float_reconciles(
                _rounded_sum(
                    float(row["operation_component_service_time_ms_sum"])
                    for row in values
                ),
                totals.get("operation_component_service_time_ms_sum"),
            )
            and sum(
                int(row["observed_operation_physical_bytes_sum"])
                for row in values
                if row["operation_kind"] == "storage_read"
            )
            == totals.get("storage_read_payload_bytes_sum")
            and sum(
                int(row["observed_operation_physical_bytes_sum"])
                for row in values
                if row["operation_kind"] == "cache_read"
            )
            == totals.get("cache_read_payload_bytes_sum")
            and sum(
                int(row["observed_operation_physical_bytes_sum"])
                for row in network_rows
            )
            == totals.get("network_payload_bytes_sum")
            and _aggregate_float_reconciles(
                _rounded_sum(
                    float(row["operation_component_service_time_ms_sum"])
                    for row in network_rows
                ),
                totals.get("network_component_service_time_ms_sum"),
            )
            and _aggregate_float_reconciles(
                _rounded_sum(
                    float(
                        row[
                            "configured_application_shaping_target_ms_sum"
                        ]
                    )
                    for row in network_rows
                ),
                totals.get(
                    "configured_application_shaping_target_ms_sum"
                ),
            )
            and sum(lookup_outcomes.values())
            == totals.get("cache_lookup_count")
            and lookup_outcomes.get("hit", 0)
            == totals.get("cache_lookup_hit_count")
            and lookup_outcomes.get("miss", 0)
            == totals.get("cache_lookup_miss_count"),
            "matrix route cell measurements do not reconcile",
        )


def _verify_output(root: Path) -> dict[str, Any]:
    expected = _OUTPUT_FILES | {"SHA256SUMS"}
    _require(root.is_dir(), "matrix statistics output does not exist")
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected, "matrix statistics output file set changed")
    checksums: dict[str, str] = {}
    try:
        lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerMatrixStatisticsError(
            "matrix statistics checksum file is unreadable"
        ) from exc
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _OUTPUT_FILES,
            "invalid matrix statistics checksum row",
        )
        _require(name not in checksums, "duplicate matrix statistics checksum")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"matrix statistics checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(set(checksums) == _OUTPUT_FILES, "statistics checksums are incomplete")

    report = _read_json(root / _REPORT_FILE, "matrix statistics report")
    manifest = _read_json(root / _MANIFEST_FILE, "matrix statistics manifest")
    cells = _read_jsonl(root / _CELL_FILE, "matrix statistics cells")
    routes = _read_jsonl(root / _ROUTE_FILE, "matrix statistics routes")
    _require(
        set(report) == _REPORT_FIELDS,
        "matrix statistics report field set changed",
    )
    _require(
        set(manifest) == _MANIFEST_FIELDS,
        "matrix statistics manifest field set changed",
    )
    _require(
        report.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_STATISTICS_REPORT_SCHEMA_VERSION
        and report.get("status") == "COMPLETE",
        "matrix statistics report is invalid",
    )
    _require(
        manifest.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_STATISTICS_MANIFEST_SCHEMA_VERSION
        and manifest.get("status") == "COMPLETE",
        "matrix statistics manifest is invalid",
    )
    _require(
        manifest.get("output_sha256")
        == {
            _REPORT_FILE: checksums[_REPORT_FILE],
            _CELL_FILE: checksums[_CELL_FILE],
            _ROUTE_FILE: checksums[_ROUTE_FILE],
        },
        "matrix statistics manifest output digests disagree",
    )
    _require(
        report.get("cell_statistics_sha256") == checksums[_CELL_FILE]
        and report.get("route_statistics_sha256") == checksums[_ROUTE_FILE],
        "matrix statistics report ledger digests disagree",
    )
    _require(len(cells) == 32, "matrix statistics must contain 32 cells")
    _require(bool(routes), "matrix statistics route ledger is empty")
    _require(
        all(
            row.get("schema_version")
            == FLOWMESH_CONTAINER_MATRIX_STATISTICS_CELL_SCHEMA_VERSION
            and row.get("trial_count") == 2
            and row.get("repetitions_present") == [0, 1]
            and row.get("cost_computed") is False
            and row.get("design_rank_computed") is False
            and row.get("eligible_for_scientific_claims") is False
            for row in cells
        ),
        "matrix cell statistics boundary changed",
    )
    _require(
        {
            (row.get("workload_class"), row.get("design_id"))
            for row in cells
        }
        == {
            (workload, design)
            for workload in _EXPECTED_WORKLOADS
            for design in _EXPECTED_DESIGNS
        },
        "matrix statistics cell coverage changed",
    )
    _require(
        all(
            row.get("schema_version")
            == FLOWMESH_CONTAINER_MATRIX_STATISTICS_ROUTE_SCHEMA_VERSION
            and row.get("throughput_derived") is False
            and row.get("cost_computed") is False
            and row.get("eligible_for_scientific_claims") is False
            for row in routes
        ),
        "matrix route statistics boundary changed",
    )
    audit = report.get("run_audit")
    totals = report.get("overall_totals")
    _require(
        isinstance(audit, Mapping) and isinstance(totals, Mapping),
        "matrix statistics report totals are missing",
    )
    _require(
        set(audit) == _RUN_AUDIT_FIELDS,
        "matrix statistics run audit field set changed",
    )
    audit_integer_fields = (
        "completed_trial_count",
        "planned_operation_count",
        "executed_operation_count",
        "inactive_operation_count",
        "workflow_count",
        "flowmesh_workflow_count",
        "infrastructure_recovery_count",
        "abandoned_workflow_count",
        "replay_result_adoption_count",
        "adopted_replay_operation_count",
    )
    for field in audit_integer_fields:
        _integer(audit.get(field), f"matrix statistics run audit {field}")
    worker_id = audit.get("worker_id")
    _require(
        audit.get("run_status") == "COMPLETE"
        and audit.get("verifier_status") == "VERIFIED"
        and audit.get("source_binding_checked") is True
        and isinstance(audit.get("run_id"), str)
        and bool(audit.get("run_id"))
        and isinstance(audit.get("matrix_id"), str)
        and bool(audit.get("matrix_id"))
        and isinstance(worker_id, str)
        and worker_id.startswith("wkr-")
        and len(worker_id) > len("wkr-")
        and audit.get("replay_result_adoption_count") == 0
        and audit.get("adopted_replay_operation_count") == 0
        and audit.get("abandoned_workflow_count")
        == audit.get("infrastructure_recovery_count")
        and audit.get("flowmesh_workflow_count")
        == audit.get("workflow_count")
        + audit.get("infrastructure_recovery_count"),
        "matrix statistics run audit boundary changed",
    )
    _require(
        report.get("matrix_dimensions")
        == {
            "workload_classes": list(_EXPECTED_WORKLOADS),
            "design_ids": list(_EXPECTED_DESIGNS),
            "repetitions": [0, 1],
            "cell_count": 32,
        }
        and report.get("cell_statistics_count") == len(cells) == 32
        and report.get("route_statistics_count") == len(routes),
        "matrix statistics dimensions or ledger counts changed",
    )
    _require(
        manifest.get("analysis_class") == report.get("analysis_class")
        and manifest.get("run_id") == audit.get("run_id")
        and manifest.get("matrix_id") == audit.get("matrix_id")
        and manifest.get("completed_trial_count")
        == audit.get("completed_trial_count")
        == 64
        and manifest.get("cell_count") == len(cells) == 32
        and manifest.get("route_count") == len(routes),
        "matrix statistics manifest identity or counts disagree",
    )
    _require(
        manifest.get("source_binding_checked") is True
        and manifest.get("source_directory_sha256")
        == report.get("source_artifact_fingerprint_before")
        == report.get("source_artifact_fingerprint_after_analysis")
        and manifest.get("parameters_fitted") == 0
        and manifest.get("external_services_called") is False
        and manifest.get("credentials_recorded") is False
        and manifest.get("eligible_for_scientific_claims") is False,
        "matrix statistics manifest provenance boundary changed",
    )
    _validate_cell_rows(cells)
    summed = _empty_totals()
    for cell in cells:
        cell_totals = cell.get("totals")
        _require(isinstance(cell_totals, Mapping), "matrix cell totals are missing")
        _add_totals(summed, cell_totals)
    _require(
        _totals_reconcile(summed, totals),
        "matrix cell totals do not reconcile",
    )
    _validate_route_rows(routes, cells)

    workload_totals = report.get("workload_totals")
    design_totals = report.get("design_totals")
    expected_workload_totals = _summary_rows_from_cells(
        cells,
        dimension="workload_class",
        expected_values=_EXPECTED_WORKLOADS,
    )
    expected_design_totals = _summary_rows_from_cells(
        cells,
        dimension="design_id",
        expected_values=_EXPECTED_DESIGNS,
    )
    _require(
        isinstance(workload_totals, list)
        and len(workload_totals) == len(expected_workload_totals)
        and all(
            set(actual) == {"workload_class", "totals"}
            and actual.get("workload_class") == expected["workload_class"]
            and isinstance(actual.get("totals"), Mapping)
            and _totals_reconcile(actual["totals"], expected["totals"])
            for actual, expected in zip(
                workload_totals, expected_workload_totals, strict=True
            )
        ),
        "matrix workload totals do not reconcile",
    )
    _require(
        isinstance(design_totals, list)
        and len(design_totals) == len(expected_design_totals)
        and all(
            set(actual) == {"design_id", "totals"}
            and actual.get("design_id") == expected["design_id"]
            and isinstance(actual.get("totals"), Mapping)
            and _totals_reconcile(actual["totals"], expected["totals"])
            for actual, expected in zip(
                design_totals, expected_design_totals, strict=True
            )
        ),
        "matrix design totals do not reconcile",
    )

    _require(
        sum(int(row["executed_operation_count"]) for row in routes)
        == totals["executed_operation_count"],
        "matrix route operation counts do not reconcile",
    )
    _require(
        sum(int(row["observed_operation_logical_bytes_sum"]) for row in routes)
        == totals["observed_operation_logical_bytes_sum"]
        and sum(
            int(row["observed_operation_physical_bytes_sum"])
            for row in routes
        )
        == totals["observed_operation_physical_bytes_sum"]
        and _aggregate_float_reconciles(
            _rounded_sum(
                float(row["operation_component_service_time_ms_sum"])
                for row in routes
            ),
            totals["operation_component_service_time_ms_sum"],
        ),
        "matrix route measurements do not reconcile",
    )
    _require(
        audit.get("completed_trial_count") == totals["trial_count"] == 64
        and audit.get("planned_operation_count")
        == totals["planned_operation_count"]
        == 500
        and audit.get("executed_operation_count")
        == totals["executed_operation_count"]
        == 472
        and audit.get("inactive_operation_count")
        == totals["inactive_operation_count"]
        == 28
        and audit.get("workflow_count") == totals["workflow_count"] == 80,
        "matrix statistics run totals do not reconcile",
    )
    for field in (
        "cost_metrics_computed",
        "design_ranking_computed",
        "network_throughput_derived",
        "end_to_end_latency_measured",
        "queue_time_measured",
        "semantic_task_quality_evaluated",
        "llm_called",
        "services_started",
        "workflow_submitted",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(report.get(field) is False, f"statistics boundary changed: {field}")
    _require(
        report.get("source_artifacts_modified") is False
        and report.get("posthoc") is True
        and report.get("parameters_fitted") == 0
        and report.get("source_artifact_fingerprint_before")
        == report.get("source_artifact_fingerprint_after_analysis"),
        "matrix statistics provenance boundary changed",
    )
    _require(
        report.get("analysis_class") == _ANALYSIS_CLASS
        and report.get("cache_lookup_outcome_interpretation")
        == _CACHE_LOOKUP_INTERPRETATION
        and report.get("measurement_boundaries")
        == list(_MEASUREMENT_BOUNDARIES),
        "matrix statistics interpretation boundary changed",
    )
    return manifest


def summarize_flowmesh_container_matrix_run(
    *,
    run_dir: str | Path,
    matrix_plan_dir: str | Path,
    formal_execution_profile_dir: str | Path,
    coordinator_plan_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Publish path-free descriptive statistics for one verified matrix run."""

    paths = {
        "run": Path(run_dir).resolve(),
        "matrix_plan": Path(matrix_plan_dir).resolve(),
        "formal_profile": Path(formal_execution_profile_dir).resolve(),
        "coordinator_plan": Path(coordinator_plan_dir).resolve(),
    }
    target = Path(output_dir).resolve()
    _require_disjoint_paths(output_dir=target, inputs=paths)
    _require(not target.exists(), f"matrix statistics output exists: {target}")
    before = {
        label: _fingerprint_directory(path)
        for label, path in sorted(paths.items())
    }
    verification = verify_flowmesh_container_matrix_run(
        paths["run"],
        matrix_plan_dir=paths["matrix_plan"],
        formal_execution_profile_dir=paths["formal_profile"],
        coordinator_plan_dir=paths["coordinator_plan"],
    )
    summary = _read_json(
        paths["run"] / "flowmesh-container-matrix-run.json",
        "matrix run summary",
    )
    trials = _read_jsonl(
        paths["run"] / "flowmesh-container-matrix-trial-results.jsonl",
        "matrix trial results",
    )
    operations = _read_jsonl(
        paths["run"] / "flowmesh-container-matrix-operation-results.jsonl",
        "matrix operation results",
    )
    _source_boundaries(summary, verification)
    operations_by_trial = _validate_matrix_shape(trials, operations)
    observations = {
        str(trial["trial_key"]): _trial_observation(
            trial, operations_by_trial[str(trial["trial_key"])]
        )
        for trial in trials
    }
    cells = _cell_rows(trials, observations)
    routes = _route_rows(trials, operations)
    overall = _totals_for_observations(list(observations.values()))
    _require(
        overall["trial_count"] == 64
        and overall["planned_operation_count"] == 500
        and overall["executed_operation_count"] == 472
        and overall["inactive_operation_count"] == 28
        and overall["workflow_count"] == 80,
        "matrix descriptive totals do not match the formal contract",
    )
    after = {
        label: _fingerprint_directory(path)
        for label, path in sorted(paths.items())
    }
    _require(before == after, "descriptive statistics changed a source artifact")

    cell_bytes = _jsonl_bytes(cells)
    route_bytes = _jsonl_bytes(routes)
    report: dict[str, Any] = {
        "schema_version": (
            FLOWMESH_CONTAINER_MATRIX_STATISTICS_REPORT_SCHEMA_VERSION
        ),
        "status": "COMPLETE",
        "analysis_class": _ANALYSIS_CLASS,
        "run_audit": {
            "run_id": summary["run_id"],
            "matrix_id": summary["matrix_id"],
            "run_status": summary["status"],
            "verifier_status": verification["status"],
            "source_binding_checked": verification["source_binding_checked"],
            "completed_trial_count": verification["completed_trial_count"],
            "planned_operation_count": summary["planned_operation_count"],
            "executed_operation_count": verification[
                "executed_operation_count"
            ],
            "inactive_operation_count": verification[
                "inactive_operation_count"
            ],
            "workflow_count": verification["workflow_count"],
            "flowmesh_workflow_count": verification[
                "flowmesh_workflow_count"
            ],
            "worker_id": verification["worker_id"],
            "infrastructure_recovery_count": verification[
                "infrastructure_recovery_count"
            ],
            "abandoned_workflow_count": verification[
                "abandoned_workflow_count"
            ],
            "replay_result_adoption_count": verification[
                "replay_result_adoption_count"
            ],
            "adopted_replay_operation_count": verification[
                "adopted_replay_operation_count"
            ],
        },
        "matrix_dimensions": {
            "workload_classes": list(_EXPECTED_WORKLOADS),
            "design_ids": list(_EXPECTED_DESIGNS),
            "repetitions": [0, 1],
            "cell_count": 32,
        },
        "overall_totals": overall,
        "workload_totals": _summary_rows(
            trials, observations, dimension="workload_class"
        ),
        "design_totals": _summary_rows(
            trials, observations, dimension="design_id"
        ),
        "cache_lookup_outcome_interpretation": _CACHE_LOOKUP_INTERPRETATION,
        "cell_statistics_count": len(cells),
        "cell_statistics_sha256": _sha256_bytes(cell_bytes),
        "route_statistics_count": len(routes),
        "route_statistics_sha256": _sha256_bytes(route_bytes),
        "source_artifact_fingerprint_before": before,
        "source_artifact_fingerprint_after_analysis": after,
        "source_artifacts_modified": False,
        "measurement_boundaries": list(_MEASUREMENT_BOUNDARIES),
        "posthoc": True,
        "parameters_fitted": 0,
        "cost_metrics_computed": False,
        "design_ranking_computed": False,
        "network_throughput_derived": False,
        "end_to_end_latency_measured": False,
        "queue_time_measured": False,
        "semantic_task_quality_evaluated": False,
        "llm_called": False,
        "services_started": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    report_bytes = _json_bytes(report)
    manifest = {
        "schema_version": (
            FLOWMESH_CONTAINER_MATRIX_STATISTICS_MANIFEST_SCHEMA_VERSION
        ),
        "status": "COMPLETE",
        "analysis_class": report["analysis_class"],
        "run_id": summary["run_id"],
        "matrix_id": summary["matrix_id"],
        "source_binding_checked": True,
        "completed_trial_count": 64,
        "cell_count": len(cells),
        "route_count": len(routes),
        "source_directory_sha256": before,
        "parameters_fitted": 0,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            _REPORT_FILE: _sha256_bytes(report_bytes),
            _CELL_FILE: _sha256_bytes(cell_bytes),
            _ROUTE_FILE: _sha256_bytes(route_bytes),
        },
    }
    documents = {
        _REPORT_FILE: report_bytes,
        _CELL_FILE: cell_bytes,
        _ROUTE_FILE: route_bytes,
        _MANIFEST_FILE: _json_bytes(manifest),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    _write_documents(target, documents)
    _verify_output(target)
    return {
        "status": "COMPLETE",
        "analysis_class": report["analysis_class"],
        "run_id": summary["run_id"],
        "matrix_id": summary["matrix_id"],
        "completed_trial_count": 64,
        "cell_count": len(cells),
        "route_count": len(routes),
        "operation_component_service_time_ms_sum": overall[
            "operation_component_service_time_ms_sum"
        ],
        "observed_operation_physical_bytes_sum": overall[
            "observed_operation_physical_bytes_sum"
        ],
        "cost_metrics_computed": False,
        "design_ranking_computed": False,
        "source_artifacts_modified": False,
        "output_dir": str(target),
        "eligible_for_scientific_claims": False,
    }


def verify_flowmesh_container_matrix_statistics(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify a source-independent descriptive-statistics package."""

    root = Path(output_dir).resolve()
    manifest = _verify_output(root)
    return {
        "status": "VERIFIED",
        "analysis_class": manifest["analysis_class"],
        "run_id": manifest["run_id"],
        "matrix_id": manifest["matrix_id"],
        "completed_trial_count": manifest["completed_trial_count"],
        "cell_count": manifest["cell_count"],
        "route_count": manifest["route_count"],
        "parameters_fitted": 0,
        "external_services_called": False,
        "checked_files": len(_OUTPUT_FILES),
        "eligible_for_scientific_claims": False,
    }
