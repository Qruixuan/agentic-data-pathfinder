"""Backend-neutral execution plans for simulator, container, and real runs.

The discrete-event engine resolves operation templates internally.  Container
execution needs the same resolution to be frozen as a public contract rather
than reimplementing it with subtly different routing or byte arithmetic.
This module publishes that contract without running an operation or claiming
that simulated task-success values are observations.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from .admission import (
    TrialAdmissionError,
    trial_admission_contract,
    validate_trial_admission_contract,
)
from .config import Operation, SimulatorScenario, load_simulator_scenario
from .engine import SimulatorTrial, build_simulator_trials


PORTABLE_PLAN_SCHEMA_VERSION = (
    "pathfinder.backend-neutral-execution-plan/v1alpha2"
)
LEGACY_PORTABLE_PLAN_SCHEMA_VERSION = (
    "pathfinder.backend-neutral-execution-plan/v1alpha1"
)
PORTABLE_TRIAL_SCHEMA_VERSION = (
    "pathfinder.backend-neutral-execution-trial/v1alpha1"
)
PORTABLE_OPERATION_SCHEMA_VERSION = (
    "pathfinder.backend-neutral-execution-operation/v1alpha1"
)
PORTABLE_METRIC_SCHEMA_VERSION = (
    "pathfinder.backend-neutral-metric-contract/v1alpha2"
)
LEGACY_PORTABLE_METRIC_SCHEMA_VERSION = (
    "pathfinder.backend-neutral-metric-contract/v1alpha1"
)
PORTABLE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.backend-neutral-execution-plan-run/v1alpha2"
)
LEGACY_PORTABLE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.backend-neutral-execution-plan-run/v1alpha1"
)

_OUTPUT_FILES = {
    "metric_contract.json",
    "operations.jsonl",
    "portable_plan.json",
    "portable_plan_manifest.json",
    "trials.jsonl",
}

_OPERATION_SEMANTICS = {
    "barrier": "dependency-barrier",
    "cache_insert": "insert-artifact-after-complete-transfer",
    "cache_lookup": "lookup-object-representation-key",
    "cache_read": "read-exact-logical-byte-count-from-cache",
    "compute": "execute-task-operation-and-measure-service",
    "control": "execute-control-operation-and-measure-service",
    "index_query": "execute-index-query-and-measure-service",
    "network_transfer": "transfer-exact-logical-byte-count",
    "storage_read": "read-exact-logical-byte-count-from-storage",
}


class PortablePlanError(ValueError):
    """Raised when a portable execution contract is unsafe or inconsistent."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PortablePlanError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise PortablePlanError(f"non-finite JSON number: {value}")


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PortablePlanError(f"cannot read valid {name}: {path}") from exc
    return raw, value


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _array(value: Any, name: str) -> list[Any]:
    _require(isinstance(value, list), f"{name} must be an array")
    return value


def _text(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical_bytes(value) + b"\n" for value in values)


def _operation_bytes(
    scenario: SimulatorScenario,
    trial: SimulatorTrial,
    operation: Operation,
) -> int:
    if operation.kind == "cache_lookup":
        return 0
    if operation.size_bytes is not None:
        base = operation.size_bytes
    elif operation.representation_id is not None:
        obj = scenario.objects[trial.object_id]
        try:
            base = obj.representations[operation.representation_id].size_bytes
        except KeyError as exc:
            raise PortablePlanError(
                f"trial {trial.trial_key} requires unavailable representation "
                f"{operation.representation_id}"
            ) from exc
    else:
        return 0
    return int(round(base * operation.byte_multiplier))


def _resource_binding(
    scenario: SimulatorScenario,
    operation: Operation,
) -> dict[str, Any] | None:
    if operation.resource_id is None:
        return None
    resource = scenario.resources[operation.resource_id]
    return {
        "resource_id": resource.resource_id,
        "resource_kind": resource.kind,
        "node_id": resource.node_id,
        "slots": resource.slots,
    }


def _link_binding(
    scenario: SimulatorScenario,
    operation: Operation,
) -> dict[str, Any] | None:
    if operation.link_id is None:
        return None
    link = scenario.links[operation.link_id]
    return {
        "link_id": link.link_id,
        "source_node_id": link.source_node_id,
        "destination_node_id": link.destination_node_id,
        "slots": link.slots,
        "bandwidth_bytes_per_second": link.bandwidth_bytes_per_second,
        "round_trip_time_ms": link.round_trip_time_ms,
        "jitter_fraction": link.jitter_fraction,
    }


def _cache_binding(
    scenario: SimulatorScenario,
    operation: Operation,
) -> dict[str, Any] | None:
    if operation.cache_id is None:
        return None
    cache = scenario.caches[operation.cache_id]
    return {
        "cache_id": cache.cache_id,
        "node_id": cache.node_id,
        "capacity_bytes": cache.capacity_bytes,
    }


def _trial_row(trial: SimulatorTrial) -> dict[str, Any]:
    return {
        "schema_version": PORTABLE_TRIAL_SCHEMA_VERSION,
        **trial.to_public_dict(),
        "quality_result_source": "backend-measured-not-plan-assigned",
    }


def _operation_row(
    scenario: SimulatorScenario,
    trial: SimulatorTrial,
    operation: Operation,
    operation_index: int,
) -> dict[str, Any]:
    condition = None
    if operation.condition is not None:
        condition = {
            "cache_operation_id": operation.condition.cache_op_id,
            "cache_operation_key": (
                f"{trial.trial_key}|{operation.condition.cache_op_id}"
            ),
            "equals": operation.condition.equals,
        }
    return {
        "schema_version": PORTABLE_OPERATION_SCHEMA_VERSION,
        "operation_key": f"{trial.trial_key}|{operation.op_id}",
        "trial_key": trial.trial_key,
        "trial_id": trial.trial_id,
        "order_index": trial.order_index,
        "operation_index": operation_index,
        "operation_id": operation.op_id,
        "operation_kind": operation.kind,
        "execution_semantics": _OPERATION_SEMANTICS[operation.kind],
        "dependency_operation_ids": list(operation.depends_on),
        "dependency_operation_keys": [
            f"{trial.trial_key}|{dependency}"
            for dependency in operation.depends_on
        ],
        "condition": condition,
        "object_id": trial.object_id,
        "representation_id": operation.representation_id,
        "logical_bytes": _operation_bytes(scenario, trial, operation),
        "byte_multiplier": operation.byte_multiplier,
        "resource_binding": _resource_binding(scenario, operation),
        "link_binding": _link_binding(scenario, operation),
        "cache_binding": _cache_binding(scenario, operation),
        "simulation_hints": {
            "additional_service_ms": operation.service_ms,
            "not_an_instruction_to_sleep_in_measured_backends": True,
        },
    }


def _metric_contract(scenario: SimulatorScenario) -> dict[str, Any]:
    return {
        "schema_version": PORTABLE_METRIC_SCHEMA_VERSION,
        "scenario_id": scenario.scenario_id,
        "independent_unit": "workload_id",
        "trial_identity_fields": [
            "trial_key",
            "trial_id",
            "session_id",
            "order_index",
            "workload_id",
            "workload_class",
            "object_id",
            "design_id",
            "repetition",
            "seed",
        ],
        "required_trial_completion_fields": {
            "outcome_type": "completed",
            "telemetry_complete": True,
            "artifact_delivery_complete": True,
        },
        "required_measured_trial_metrics": [
            "latency_ms",
            "trial_admission_queue_ms",
            "active_execution_latency_ms",
            "logical_bytes",
            "physical_bytes",
            "network_bytes",
            "resource_service_ms",
            "resource_queue_ms",
            "task_success",
        ],
        "required_measured_event_metrics": [
            "operation_key",
            "executed",
            "ready_time_ms",
            "start_time_ms",
            "end_time_ms",
            "queue_time_ms",
            "service_time_ms",
            "logical_bytes",
            "physical_bytes",
            "resource_id",
            "source_node_id",
            "destination_node_id",
        ],
        "cross_backend_comparisons": [
            {
                "metric": "trial_identity_and_completion_set",
                "aggregation": "exact-set-equality",
            },
            {
                "metric": "network_bytes",
                "aggregation": "paired-per-trial-and-design-mean",
            },
            {
                "metric": "latency_ms",
                "aggregation": "paired-median-p95-and-design-rank",
            },
            {
                "metric": "trial_admission_queue_ms",
                "aggregation": "paired-per-trial-and-design-mean",
            },
            {
                "metric": "active_execution_latency_ms",
                "aggregation": "paired-per-trial-and-design-mean",
            },
            {
                "metric": "resource_service_ms",
                "aggregation": "paired-resource-breakdown",
            },
            {
                "metric": "resource_queue_ms",
                "aggregation": "paired-resource-breakdown",
            },
            {
                "metric": "task_success",
                "aggregation": "paired-workload-and-design",
            },
        ],
        "parity_thresholds": None,
        "parity_threshold_status": "must-be-preregistered-before-evaluation",
        "configured_rate_card_is_not_physical_money": True,
        "simulation_hints_are_not_container_measurements": True,
        "trial_admission": trial_admission_contract(
            scenario.trial_admission_slots
        ),
        "credentials_recorded": False,
    }


def _documents(scenario: SimulatorScenario) -> dict[str, bytes]:
    trials = build_simulator_trials(scenario)
    trial_rows = [_trial_row(trial) for trial in trials]
    workload_by_id = {
        workload.workload_id: workload for workload in scenario.workloads
    }
    design_by_id = {design.design_id: design for design in scenario.designs}
    operation_rows: list[dict[str, Any]] = []
    for trial in trials:
        workload = workload_by_id[trial.workload_id]
        design = design_by_id[trial.design_id]
        operations = scenario.operations_for(design, workload.workload_class)
        operation_rows.extend(
            _operation_row(scenario, trial, operation, operation_index)
            for operation_index, operation in enumerate(operations)
        )
    trial_bytes = _jsonl_bytes(trial_rows)
    operation_bytes = _jsonl_bytes(operation_rows)
    metric_bytes = _json_bytes(_metric_contract(scenario))
    plan: dict[str, Any] = {
        "schema_version": PORTABLE_PLAN_SCHEMA_VERSION,
        "scenario_id": scenario.scenario_id,
        "scenario_sha256": scenario.source_sha256,
        "seed": scenario.seed,
        "repetitions": scenario.repetitions,
        "arrival_interval_ms": scenario.arrival_interval_ms,
        "trial_admission": trial_admission_contract(
            scenario.trial_admission_slots
        ),
        "node_count": len(scenario.nodes),
        "link_count": len(scenario.links),
        "workload_count": len(scenario.workloads),
        "design_count": len(scenario.designs),
        "planned_trial_count": len(trial_rows),
        "planned_operation_count": len(operation_rows),
        "trial_sha256": _sha256_bytes(trial_bytes),
        "operation_sha256": _sha256_bytes(operation_bytes),
        "metric_contract_sha256": _sha256_bytes(metric_bytes),
        "supported_backend_classes": [
            "container-emulation",
            "discrete-event-simulation",
            "real-cluster-execution",
        ],
        "task_success_values_from_scenario_included": False,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256_bytes(_canonical_bytes(plan))
    documents = {
        "metric_contract.json": metric_bytes,
        "operations.jsonl": operation_bytes,
        "portable_plan.json": _json_bytes(plan),
        "trials.jsonl": trial_bytes,
    }
    manifest = {
        "schema_version": PORTABLE_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "scenario_id": scenario.scenario_id,
        "scenario_sha256": scenario.source_sha256,
        "plan_sha256": plan["plan_sha256"],
        "planned_trial_count": len(trial_rows),
        "planned_operation_count": len(operation_rows),
        "trial_admission": trial_admission_contract(
            scenario.trial_admission_slots
        ),
        "external_services_called": False,
        "container_started": False,
        "flowmesh_deployed": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["portable_plan_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )
    return documents


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PortablePlanError(f"cannot read {name}: {path}") from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(
                line,
                object_pairs_hook=_unique_keys,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, PortablePlanError) as exc:
            raise PortablePlanError(
                f"invalid {name} at line {line_number}: {exc}"
            ) from exc
        _require(isinstance(value, dict), f"{name} line must be an object")
        rows.append(value)
    return rows


def _verify_output(root: Path) -> dict[str, Any]:
    expected = _OUTPUT_FILES | {"SHA256SUMS"}
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected, "portable plan output file set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _OUTPUT_FILES,
            "portable plan SHA256SUMS is malformed",
        )
        _require(name not in checksums, f"duplicate checksum entry: {name}")
        actual_digest = _sha256_bytes((root / name).read_bytes())
        _require(actual_digest == digest, f"portable plan checksum mismatch: {name}")
        checksums[name] = digest
    _require(set(checksums) == _OUTPUT_FILES, "portable checksums are incomplete")
    _, manifest_value = _read_json(
        root / "portable_plan_manifest.json",
        "portable plan manifest",
    )
    manifest = _mapping(manifest_value, "portable plan manifest")
    _require(
        manifest.get("schema_version") in (
            PORTABLE_MANIFEST_SCHEMA_VERSION,
            LEGACY_PORTABLE_MANIFEST_SCHEMA_VERSION,
        ),
        "unsupported portable plan manifest schema_version",
    )
    _require(manifest.get("status") == "COMPLETE", "portable plan is incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "portable_plan_manifest.json"
        },
        "portable manifest output digests disagree",
    )
    _, plan_value = _read_json(root / "portable_plan.json", "portable plan")
    plan = _mapping(plan_value, "portable plan")
    _require(
        plan.get("schema_version") in (
            PORTABLE_PLAN_SCHEMA_VERSION,
            LEGACY_PORTABLE_PLAN_SCHEMA_VERSION,
        ),
        "unsupported portable plan schema_version",
    )
    _require(
        (manifest.get("schema_version") == PORTABLE_MANIFEST_SCHEMA_VERSION)
        is (plan.get("schema_version") == PORTABLE_PLAN_SCHEMA_VERSION),
        "portable plan and manifest schema generations differ",
    )
    plan_without_digest = dict(plan)
    claimed_plan_digest = plan_without_digest.pop("plan_sha256", None)
    _require(
        claimed_plan_digest == _sha256_bytes(_canonical_bytes(plan_without_digest)),
        "portable plan_sha256 mismatch",
    )
    metric_raw, metric_value = _read_json(
        root / "metric_contract.json",
        "portable metric contract",
    )
    metric = _mapping(metric_value, "portable metric contract")
    _require(
        _sha256_bytes(metric_raw) == plan.get("metric_contract_sha256"),
        "portable metric contract digest mismatch",
    )
    _require(
        metric.get("schema_version") in (
            PORTABLE_METRIC_SCHEMA_VERSION,
            LEGACY_PORTABLE_METRIC_SCHEMA_VERSION,
        ),
        "unsupported portable metric contract schema_version",
    )
    trials = _read_jsonl(root / "trials.jsonl", "portable trials")
    operations = _read_jsonl(root / "operations.jsonl", "portable operations")
    _require(
        len(trials) == plan.get("planned_trial_count"),
        "portable trial count mismatch",
    )
    _require(
        len(operations) == plan.get("planned_operation_count"),
        "portable operation count mismatch",
    )
    _require(
        _sha256_bytes((root / "trials.jsonl").read_bytes())
        == plan.get("trial_sha256"),
        "portable trial digest mismatch",
    )
    _require(
        _sha256_bytes((root / "operations.jsonl").read_bytes())
        == plan.get("operation_sha256"),
        "portable operation digest mismatch",
    )
    trial_keys = {_text(row.get("trial_key"), "trial_key") for row in trials}
    _require(len(trial_keys) == len(trials), "portable trials contain duplicates")
    operation_keys: set[str] = set()
    operations_by_trial: dict[str, set[str]] = {}
    for row in operations:
        _require(
            row.get("schema_version") == PORTABLE_OPERATION_SCHEMA_VERSION,
            "unsupported portable operation schema_version",
        )
        trial_key = _text(row.get("trial_key"), "operation.trial_key")
        _require(trial_key in trial_keys, "operation references unknown trial")
        operation_key = _text(row.get("operation_key"), "operation_key")
        _require(operation_key not in operation_keys, "duplicate operation_key")
        operation_keys.add(operation_key)
        operations_by_trial.setdefault(trial_key, set()).add(operation_key)
    for row in operations:
        trial_key = row["trial_key"]
        dependencies = _array(
            row.get("dependency_operation_keys"),
            "dependency_operation_keys",
        )
        _require(
            set(dependencies).issubset(operations_by_trial[trial_key]),
            "operation dependency escapes or is absent from its trial",
        )
    _require(
        set(operations_by_trial) == trial_keys,
        "one or more portable trials have no operations",
    )
    admission = plan.get("trial_admission")
    if plan.get("schema_version") == PORTABLE_PLAN_SCHEMA_VERSION:
        try:
            validate_trial_admission_contract(
                admission,
                planned_trial_count=len(trials),
            )
        except TrialAdmissionError as exc:
            raise PortablePlanError(str(exc)) from exc
        _require(
            manifest.get("trial_admission") == admission,
            "portable manifest and plan trial_admission differ",
        )
        _require(
            metric.get("schema_version") == PORTABLE_METRIC_SCHEMA_VERSION
            and metric.get("trial_admission") == admission,
            "portable metric contract and plan trial_admission differ",
        )
    else:
        _require(
            admission is None,
            "legacy portable plan cannot add trial_admission without a schema bump",
        )
        _require(
            metric.get("schema_version") == LEGACY_PORTABLE_METRIC_SCHEMA_VERSION,
            "legacy portable plan must use the legacy metric contract",
        )
    result = dict(manifest)
    result["portable_plan_schema_version"] = plan["schema_version"]
    result["trial_admission"] = admission
    return result


def build_portable_execution_plan(
    scenario_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Compile one scenario into an immutable backend-neutral operation plan."""

    scenario = load_simulator_scenario(scenario_path)
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"portable plan output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    documents = _documents(scenario)
    staging_parent = Path(tempfile.mkdtemp(prefix=".portable-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_output(staging)
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    manifest = _verify_output(target)
    return {
        **manifest,
        "output_dir": str(target),
        "plan_path": str(target / "portable_plan.json"),
        "operations_path": str(target / "operations.jsonl"),
        "metric_contract_path": str(target / "metric_contract.json"),
    }


def verify_portable_execution_plan(output_dir: str | Path) -> dict[str, Any]:
    """Read-only verification of a backend-neutral plan directory."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"portable plan output does not exist: {root}")
    manifest = _verify_output(root)
    return {
        "status": "VERIFIED",
        "scenario_id": manifest["scenario_id"],
        "plan_sha256": manifest["plan_sha256"],
        "planned_trial_count": manifest["planned_trial_count"],
        "planned_operation_count": manifest["planned_operation_count"],
        "portable_plan_schema_version": manifest[
            "portable_plan_schema_version"
        ],
        "trial_admission": manifest["trial_admission"],
        "checked_files": len(_OUTPUT_FILES),
        "eligible_for_scientific_claims": False,
    }
