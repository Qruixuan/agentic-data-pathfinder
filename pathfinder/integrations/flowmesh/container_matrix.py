"""Freeze and verify a complete 4×8 container-operation matrix.

This module is deliberately a *planning* layer.  It binds the full portable
and container ledgers, endpoint map, cache namespaces, admission contract,
and per-operation API timeout floors into one immutable package.  It does not
start containers, contact FlowMesh, or submit work.

The existing ``container_full_chain`` integration is intentionally a small,
linear, unconditional smoke.  A formal 4×8 matrix includes index-first,
parallel, and cache-conditional graphs, so treating it as 64 copies of that
smoke would silently omit real operations.  The frozen artifact below is the
contract for a later trial-wrapper coordinator: FlowMesh owns dispatch and
retry, while the coordinator owns the complete per-trial physical DAG and its
cache decisions.
"""

from __future__ import annotations

import json
import re
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from ...simulator.container_contract import (
    CONTAINER_NODE_RESULT_SCHEMA_VERSION,
    CONTAINER_OPERATION_SCHEMA_VERSION,
    verify_container_backend_plan,
)
from ...simulator.portable import verify_portable_execution_plan
from .container_dag import (
    DEFAULT_API_TASK_TIMEOUT_SECONDS,
    TELEMETRY_PROVENANCE_VERSION,
    FlowMeshContainerDagError,
    _canonical_bytes,
    _checksums,
    _document_sha256,
    _json_bytes,
    _jsonl_bytes,
    _require,
    _require_api_timeout_covers,
    _sha256_bytes,
    _text,
    _validate_api_task_timeout,
    _validate_node_api_urls,
    _write_documents,
    derive_operation_lower_bounds,
    load_container_operations,
)


FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-plan/v1alpha2"
)
FLOWMESH_CONTAINER_MATRIX_PLAN_LEGACY_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-plan/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_ADMISSION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-admission/v1alpha1"
)

_PLAN_FILES = {
    "flowmesh-container-matrix-plan.json",
    "flowmesh-container-matrix-trials.jsonl",
    "flowmesh-container-matrix-operations.jsonl",
    "flowmesh-container-matrix-admission.json",
}
_GIT_REVISION = re.compile(r"[0-9a-f]{40}")
_CACHE_OPERATION_KINDS = frozenset(
    {"cache_lookup", "cache_read", "cache_insert"}
)

_RUNTIME_INTEGRITY_REQUIREMENTS = {
    "container_operation_result_schema_version": (
        CONTAINER_NODE_RESULT_SCHEMA_VERSION
    ),
    "telemetry_provenance_version": TELEMETRY_PROVENANCE_VERSION,
    "runtime_epoch_binding_required": True,
    "pre_submit_health_required": True,
    "post_run_health_required": True,
    "all_referenced_runtime_nodes_must_be_healthy": True,
    "container_restart_during_trial_refuses_canonicalization": True,
    "legacy_v1_artifacts_not_runtime_integrity_eligible": True,
}


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError(f"{label} is invalid JSON") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError(f"cannot read {label}") from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerDagError(
                f"{label} contains invalid JSON at line {line_number}"
            ) from exc
        _require(isinstance(row, dict), f"{label} row must be an object")
        rows.append(row)
    return rows


def _sha256_path(path: Path, label: str) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise FlowMeshContainerDagError(f"cannot read {label}") from exc


def _require_git_revision(value: Any) -> str:
    revision = _text(value, "source_git_revision")
    _require(
        _GIT_REVISION.fullmatch(revision) is not None,
        "source_git_revision must be a 40-character lowercase commit ID",
    )
    return revision


def _trial_dimensions(trials: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    _require(len(trials) == 64, "formal 4x8 matrix requires exactly 64 trials")
    keys: set[str] = set()
    workload_ids: set[str] = set()
    workload_classes: set[str] = set()
    design_ids: set[str] = set()
    repetitions: set[int] = set()
    combinations: set[tuple[str, str, int]] = set()
    for row in trials:
        key = _text(row.get("trial_key"), "matrix trial_key")
        _require(key not in keys, "matrix trial keys are not unique")
        keys.add(key)
        workload_id = _text(row.get("workload_id"), "matrix workload_id")
        workload_class = _text(row.get("workload_class"), "matrix workload_class")
        design_id = _text(row.get("design_id"), "matrix design_id")
        repetition = row.get("repetition")
        _require(
            type(repetition) is int and repetition >= 0,
            "matrix repetition must be a non-negative integer",
        )
        combination = (workload_id, design_id, repetition)
        _require(
            combination not in combinations,
            "matrix workload-design-repetition cell is duplicated",
        )
        combinations.add(combination)
        workload_ids.add(workload_id)
        workload_classes.add(workload_class)
        design_ids.add(design_id)
        repetitions.add(repetition)
    _require(len(workload_ids) == 4, "formal matrix requires four workloads")
    _require(
        workload_classes == {"W1", "W2", "W3", "W4"},
        "formal matrix workload classes must be W1 through W4",
    )
    _require(len(design_ids) == 8, "formal matrix requires eight designs")
    _require(repetitions == {0, 1}, "formal matrix requires repetitions r0000 and r0001")
    expected = {
        (workload_id, design_id, repetition)
        for workload_id in workload_ids
        for design_id in design_ids
        for repetition in repetitions
    }
    _require(
        combinations == expected,
        "matrix trials do not form the exact workload-design-repetition cross product",
    )
    return {
        "workload_count": len(workload_ids),
        "workload_classes": sorted(workload_classes),
        "design_count": len(design_ids),
        "design_ids": sorted(design_ids),
        "repetitions": sorted(repetitions),
        "trial_count": len(trials),
    }


def _topology_node_ids(container_root: Path) -> list[str]:
    topology = _read_json(container_root / "container_topology.json", "container topology")
    nodes = topology.get("nodes")
    _require(isinstance(nodes, list), "container topology nodes are missing")
    node_ids = sorted(_text(row.get("node_id"), "container topology node_id") for row in nodes if isinstance(row, Mapping))
    _require(len(node_ids) == len(nodes), "container topology node is invalid")
    _require(len(node_ids) == len(set(node_ids)), "container topology node IDs are not unique")
    _require(len(node_ids) == 8, "formal matrix requires an eight-node container topology")
    return node_ids


def _validate_endpoint_map(
    node_ids: Sequence[str],
    node_api_urls: Mapping[str, str],
) -> dict[str, str]:
    placeholders = [{"execution_node_id": node_id} for node_id in node_ids]
    normalized = _validate_node_api_urls(placeholders, node_api_urls)
    _require(
        set(normalized) == set(node_ids),
        "node API URLs must cover exactly the eight frozen topology nodes",
    )
    return normalized


def _cache_scope_id(trial: Mapping[str, Any]) -> str:
    design_id = _text(trial.get("design_id"), "cache-scope design_id")
    repetition = trial.get("repetition")
    _require(
        type(repetition) is int and repetition >= 0,
        "cache-scope repetition is invalid",
    )
    return f"{design_id}|r{repetition:04d}"


def _validate_operations(
    operations: Sequence[Mapping[str, Any]],
    *,
    trials: Sequence[Mapping[str, Any]],
    portable_operations: Sequence[Mapping[str, Any]],
    portable_plan_sha256: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    _require(
        len(operations) == 500,
        "formal 4x8 matrix requires exactly 500 container operations",
    )
    trial_by_key = {
        _text(row.get("trial_key"), "matrix trial_key"): row for row in trials
    }
    _require(len(trial_by_key) == len(trials), "matrix trial keys are not unique")
    portable_by_key = {
        _text(row.get("operation_key"), "portable operation_key"): row
        for row in portable_operations
    }
    _require(
        len(portable_by_key) == len(portable_operations) == len(operations),
        "portable operation coverage is incomplete",
    )

    copied: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    by_trial: dict[str, list[dict[str, Any]]] = {key: [] for key in trial_by_key}
    cache_scopes: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in operations:
        try:
            row = json.loads(_canonical_bytes(raw).decode("utf-8"))
        except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise FlowMeshContainerDagError(
                "container operation cannot be canonicalized"
            ) from exc
        _require(isinstance(row, dict), "container operation must be an object")
        _require(
            row.get("schema_version") == CONTAINER_OPERATION_SCHEMA_VERSION,
            "formal matrix requires cache-scoped v1alpha2 container operations",
        )
        operation_key = _text(row.get("operation_key"), "matrix operation_key")
        _require(operation_key not in by_key, "matrix operation keys are not unique")
        trial_key = _text(row.get("trial_key"), "matrix operation trial_key")
        _require(trial_key in trial_by_key, "operation names a trial outside the matrix")
        _require(
            operation_key in portable_by_key,
            "container operation is absent from the portable plan",
        )
        portable = portable_by_key[operation_key]
        _require(
            row.get("logical_bytes") == portable.get("logical_bytes"),
            "container operation logical byte count differs from portable plan",
        )
        _require(
            row.get("portable_plan_sha256") == portable_plan_sha256,
            "container operation is bound to a different portable plan",
        )
        dependencies = row.get("dependency_operation_keys")
        _require(
            isinstance(dependencies, list)
            and all(isinstance(item, str) and item for item in dependencies),
            "matrix operation dependency list is invalid",
        )
        by_key[operation_key] = row
        by_trial[trial_key].append(row)
        copied.append(row)

    _require(
        set(by_key) == set(portable_by_key),
        "container and portable operation keys differ",
    )
    for row in copied:
        trial_key = row["trial_key"]
        for dependency in row["dependency_operation_keys"]:
            _require(dependency in by_key, "matrix operation has an unknown dependency")
            _require(
                by_key[dependency]["trial_key"] == trial_key,
                "matrix operation dependency crosses trial boundaries",
            )

        condition = row.get("condition")
        if condition is not None:
            _require(isinstance(condition, Mapping), "matrix condition is invalid")
            lookup_key = _text(
                condition.get("cache_operation_key"),
                "matrix condition cache_operation_key",
            )
            lookup = by_key.get(lookup_key)
            _require(lookup is not None, "matrix condition names an unknown cache lookup")
            _require(
                lookup["trial_key"] == row["trial_key"],
                "matrix condition crosses trial boundaries",
            )
            _require(
                lookup.get("operation_kind") == "cache_lookup",
                "matrix condition does not name a cache lookup",
            )
            _require(
                condition.get("equals") in {"hit", "miss"},
                "matrix condition outcome is invalid",
            )
            _require(
                isinstance(lookup.get("cache_adapter"), Mapping)
                and _text(lookup.get("cache_scope_id"), "lookup cache_scope_id")
                == _cache_scope_id(trial_by_key[row["trial_key"]]),
                "matrix condition lookup has no matching cache scope",
            )
            if row.get("operation_kind") == "cache_read":
                read_cache = row.get("cache_adapter")
                lookup_cache = lookup.get("cache_adapter")
                _require(
                    isinstance(read_cache, Mapping)
                    and isinstance(lookup_cache, Mapping),
                    "matrix cache read or its lookup has no cache adapter",
                )
                _require(
                    read_cache.get("cache_id") == lookup_cache.get("cache_id"),
                    "matrix cache read and lookup use different cache IDs",
                )
                _require(
                    _text(row.get("cache_scope_id"), "cache read cache_scope_id")
                    == _text(
                        lookup.get("cache_scope_id"),
                        "lookup cache_scope_id",
                    ),
                    "matrix cache read and lookup use different cache scopes",
                )

        cache = row.get("cache_adapter")
        kind = row.get("operation_kind")
        if cache is None:
            _require(
                row.get("cache_scope_id") is None,
                "non-cache matrix operation carries a cache scope",
            )
            continue
        _require(isinstance(cache, Mapping), "matrix cache adapter is invalid")
        _require(
            kind in _CACHE_OPERATION_KINDS,
            "only cache lookup, cache read, and insert may carry a cache adapter",
        )
        cache_id = _text(cache.get("cache_id"), "matrix cache_id")
        scope_id = _text(row.get("cache_scope_id"), "matrix cache_scope_id")
        _require(
            scope_id == _cache_scope_id(trial_by_key[row["trial_key"]]),
            "matrix cache scope does not match its design and repetition",
        )
        initial_entries = cache.get("initial_entries")
        _require(isinstance(initial_entries, list), "matrix cache initial entries missing")
        scope_key = (cache_id, scope_id)
        candidate = {
            "cache_id": cache_id,
            "cache_scope_id": scope_id,
            "node_id": _text(cache.get("node_id"), "matrix cache node_id"),
            "capacity_bytes": cache.get("capacity_bytes"),
            "initial_entries": initial_entries,
            "initial_entries_sha256": _sha256_bytes(_canonical_bytes(initial_entries)),
        }
        existing = cache_scopes.setdefault(scope_key, candidate)
        _require(
            existing == candidate,
            "matrix cache scope is bound to inconsistent cache snapshots",
        )

    _require(
        all(by_trial.values()), "a matrix trial has no container operations",
    )
    scope_rows = [cache_scopes[key] for key in sorted(cache_scopes)]
    summary = {
        "operation_count": len(copied),
        "conditional_operation_count": sum(
            1 for row in copied if row.get("condition") is not None
        ),
        "cache_scope_count": len(scope_rows),
        "cache_scopes": scope_rows,
    }
    return copied, scope_rows, summary


def _matrix_trial_rows(
    trials: Sequence[Mapping[str, Any]],
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    operation_rows: dict[str, list[Mapping[str, Any]]] = {}
    for operation in operations:
        operation_rows.setdefault(str(operation["trial_key"]), []).append(operation)
    result: list[dict[str, Any]] = []
    for trial in sorted(trials, key=lambda row: int(row["order_index"])):
        trial_key = str(trial["trial_key"])
        rows = operation_rows[trial_key]
        result.append(
            {
                "trial_key": trial_key,
                "trial_id": _text(trial.get("trial_id"), "matrix trial_id"),
                "order_index": trial["order_index"],
                "workload_id": _text(trial.get("workload_id"), "matrix workload_id"),
                "workload_class": _text(trial.get("workload_class"), "matrix workload_class"),
                "design_id": _text(trial.get("design_id"), "matrix design_id"),
                "repetition": trial["repetition"],
                "seed": trial.get("seed"),
                "object_id": _text(trial.get("object_id"), "matrix object_id"),
                "executor_node_id": _text(
                    trial.get("executor_node_id"), "matrix executor_node_id"
                ),
                "operation_count": len(rows),
                "conditional_operation_count": sum(
                    1 for row in rows if row.get("condition") is not None
                ),
                "cache_scope_ids": sorted(
                    {
                        str(row["cache_scope_id"])
                        for row in rows
                        if row.get("cache_scope_id") is not None
                    }
                ),
            }
        )
    return result


def _read_plan(plan_dir: str | Path) -> tuple[Path, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    root = Path(plan_dir).resolve()
    _require(root.is_dir(), f"container matrix plan directory does not exist: {root}")
    try:
        lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError(
            "container matrix checksum file is unreadable"
        ) from exc
    observed: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _PLAN_FILES,
            "invalid container matrix checksum row",
        )
        _require(name not in observed, "duplicate container matrix checksum")
        observed[name] = digest
    _require(
        set(observed) == _PLAN_FILES,
        "container matrix checksum set is incomplete",
    )
    for name, digest in observed.items():
        _require((root / name).is_file(), f"container matrix file is missing: {name}")
        _require(
            _sha256_path(root / name, f"container matrix {name}") == digest,
            f"container matrix checksum mismatch: {name}",
        )
    plan = _read_json(root / "flowmesh-container-matrix-plan.json", "container matrix plan")
    trials = _read_jsonl(root / "flowmesh-container-matrix-trials.jsonl", "container matrix trials")
    operations = _read_jsonl(root / "flowmesh-container-matrix-operations.jsonl", "container matrix operations")
    admission = _read_json(root / "flowmesh-container-matrix-admission.json", "container matrix admission")
    return root, plan, trials, operations, admission


def _verify_plan_contents(
    root: Path,
    plan: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
    operations: Sequence[Mapping[str, Any]],
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    schema = plan.get("schema_version")
    _require(
        schema
        in (
            FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
            FLOWMESH_CONTAINER_MATRIX_PLAN_LEGACY_SCHEMA_VERSION,
        ),
        "unsupported container matrix plan schema",
    )
    _require(plan.get("status") == "FROZEN", "container matrix plan is not frozen")
    _require(
        plan.get("plan_sha256") == _document_sha256(plan, "plan_sha256"),
        "container matrix plan digest mismatch",
    )
    dimensions = _trial_dimensions(trials)
    _require(plan.get("matrix_dimensions") == dimensions, "matrix dimensions changed")
    _require(
        plan.get("matrix_trials_sha256")
        == _sha256_path(root / "flowmesh-container-matrix-trials.jsonl", "matrix trials"),
        "matrix trial ledger digest changed",
    )
    _require(
        plan.get("matrix_operations_sha256")
        == _sha256_path(root / "flowmesh-container-matrix-operations.jsonl", "matrix operations"),
        "matrix operation ledger digest changed",
    )
    _require(
        admission.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_ADMISSION_SCHEMA_VERSION,
        "unsupported container matrix admission schema",
    )
    _require(
        plan.get("admission_contract_sha256")
        == _sha256_path(root / "flowmesh-container-matrix-admission.json", "matrix admission"),
        "matrix admission contract digest changed",
    )
    trial_by_key = {str(row["trial_key"]): row for row in trials}
    portable_proxy = [
        {
            "operation_key": row["operation_key"],
            "logical_bytes": row["logical_bytes"],
        }
        for row in operations
    ]
    checked, scope_rows, operation_summary = _validate_operations(
        operations,
        trials=trials,
        portable_operations=portable_proxy,
        portable_plan_sha256=_text(
            plan.get("portable_plan_sha256"), "matrix portable_plan_sha256"
        ),
    )
    _require(
        plan.get("operation_summary") == operation_summary,
        "matrix operation summary changed",
    )
    _require(
        plan.get("cache_scopes") == scope_rows,
        "matrix cache scope contract changed",
    )
    topology_nodes = plan.get("topology_node_ids")
    _require(
        isinstance(topology_nodes, list)
        and all(isinstance(node, str) and node for node in topology_nodes),
        "matrix topology node IDs are invalid",
    )
    _require(len(topology_nodes) == 8 and len(set(topology_nodes)) == 8, "matrix topology is not eight distinct nodes")
    urls = plan.get("node_api_urls")
    _require(isinstance(urls, Mapping), "matrix node API URLs are missing")
    _validate_endpoint_map(sorted(topology_nodes), urls)
    timeout = _validate_api_task_timeout(plan.get("api_task_timeout_seconds"))
    bounds = derive_operation_lower_bounds(checked)
    _require(
        plan.get("operation_lower_bound_seconds") == bounds,
        "matrix operation lower-bound record changed",
    )
    _require(
        plan.get("max_operation_lower_bound_seconds")
        == max((item["lower_bound_seconds"] for item in bounds), default=0.0),
        "matrix maximum lower bound changed",
    )
    _require_api_timeout_covers(bounds, timeout)
    _require(
        admission.get("trial_admission") == plan.get("trial_admission"),
        "matrix admission contract does not match the plan",
    )
    _require(
        admission.get("cache_lane_key_fields")
        == ["design_id", "repetition", "cache_id"],
        "matrix cache lane contract changed",
    )
    _require(
        admission.get("same_cache_lane_serial_execution_required") is True,
        "matrix cache lane serialization requirement changed",
    )
    _require(
        plan.get("flowmesh_execution_boundary")
        == {
            "coordinator_required": True,
            "operation_level_submission_supported": False,
            "reason": (
                "the frozen matrix contains index-first, parallel, and "
                "cache-conditional DAGs; submit through a plan-bound "
                "trial-wrapper coordinator rather than silently flattening them"
            ),
        },
        "matrix FlowMesh execution boundary changed",
    )
    if schema == FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION:
        _require(
            plan.get("runtime_integrity") == _RUNTIME_INTEGRITY_REQUIREMENTS,
            "matrix runtime-integrity contract changed",
        )
    else:
        _require(
            "runtime_integrity" not in plan,
            "legacy matrix plan unexpectedly declares runtime integrity",
        )
    _require(plan.get("workflow_submitted") is False, "matrix plan records a workflow submission")
    _require(plan.get("services_started") is False, "matrix plan records a service start")
    _require(plan.get("credentials_recorded") is False, "matrix plan records credentials")
    _require(plan.get("eligible_for_scientific_claims") is False, "matrix plan promotes scientific eligibility")
    _require(len(trial_by_key) == 64, "matrix trial count changed")
    return {
        "status": "VERIFIED",
        "schema_version": schema,
        "matrix_id": plan.get("matrix_id"),
        "scenario_id": plan.get("scenario_id"),
        "matrix_dimensions": dimensions,
        "operation_count": len(checked),
        "conditional_operation_count": operation_summary["conditional_operation_count"],
        "cache_scope_count": operation_summary["cache_scope_count"],
        "api_task_timeout_seconds": timeout,
        "max_operation_lower_bound_seconds": plan[
            "max_operation_lower_bound_seconds"
        ],
        "plan_sha256": plan["plan_sha256"],
        "runtime_integrity": (
            "required-v2"
            if schema == FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION
            else "not-recorded-v1"
        ),
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
    }


def plan_flowmesh_container_matrix(
    *,
    portable_plan_dir: str | Path,
    container_plan_dir: str | Path,
    node_api_urls: Mapping[str, str],
    worker_alias: str,
    matrix_id: str,
    source_git_revision: str,
    execution_profile_id: str,
    output_dir: str | Path,
    api_task_timeout_seconds: int = DEFAULT_API_TASK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Freeze a complete 64-trial / 500-operation container matrix.

    This accepts only the exact 4×8 development shape encoded by the current
    scenario.  It is intentionally strict: a caller who wants a different
    formal experiment needs a new scenario and a new freeze rather than an
    accidental partial reuse of this matrix.
    """

    portable_root = Path(portable_plan_dir).resolve()
    container_root = Path(container_plan_dir).resolve()
    try:
        portable_verified = verify_portable_execution_plan(portable_root)
        container_verified = verify_container_backend_plan(container_root)
    except Exception as exc:
        raise FlowMeshContainerDagError(
            "matrix source plans do not verify: " + str(exc)
        ) from exc
    portable_plan = _read_json(portable_root / "portable_plan.json", "portable plan")
    trials = _read_jsonl(portable_root / "trials.jsonl", "portable trials")
    portable_operations = _read_jsonl(
        portable_root / "operations.jsonl", "portable operations"
    )
    operations = load_container_operations(
        container_root / "container_operations.jsonl"
    )
    dimensions = _trial_dimensions(trials)
    _require(
        portable_verified["planned_trial_count"] == dimensions["trial_count"],
        "portable plan trial count changed",
    )
    _require(
        portable_verified["planned_operation_count"] == 500,
        "portable plan operation count is not the formal 4x8 count",
    )
    _require(
        container_verified["planned_trial_count"] == dimensions["trial_count"],
        "container plan trial count changed",
    )
    _require(
        container_verified["planned_operation_count"] == 500,
        "container plan operation count is not the formal 4x8 count",
    )
    _require(
        container_verified["portable_plan_sha256"] == portable_verified["plan_sha256"],
        "container and portable plans are not bound together",
    )
    copied, scope_rows, operation_summary = _validate_operations(
        operations,
        trials=trials,
        portable_operations=portable_operations,
        portable_plan_sha256=portable_verified["plan_sha256"],
    )
    node_ids = _topology_node_ids(container_root)
    urls = _validate_endpoint_map(node_ids, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    bounds = derive_operation_lower_bounds(copied)
    _require_api_timeout_covers(bounds, timeout)
    matrix_trials = _matrix_trial_rows(trials, copied)
    admission_contract = portable_plan.get("trial_admission")
    _require(isinstance(admission_contract, Mapping), "portable trial admission is missing")
    profile = _text(execution_profile_id, "execution_profile_id")
    alias = _text(worker_alias, "worker_alias")
    identifier = _text(matrix_id, "matrix_id")
    revision = _require_git_revision(source_git_revision)
    admission = {
        "schema_version": FLOWMESH_CONTAINER_MATRIX_ADMISSION_SCHEMA_VERSION,
        "execution_profile_id": profile,
        "trial_admission": dict(admission_contract),
        "cache_lane_key_fields": ["design_id", "repetition", "cache_id"],
        "same_cache_lane_serial_execution_required": True,
        "cross_lane_parallelism_not_yet_claimed": True,
        "flowmesh_dispatch_unit": "plan-bound-trial-wrapper",
        "credentials_recorded": False,
    }
    operations_bytes = _jsonl_bytes(copied)
    trials_bytes = _jsonl_bytes(matrix_trials)
    admission_bytes = _json_bytes(admission)
    boundary = {
        "coordinator_required": True,
        "operation_level_submission_supported": False,
        "reason": (
            "the frozen matrix contains index-first, parallel, and "
            "cache-conditional DAGs; submit through a plan-bound "
            "trial-wrapper coordinator rather than silently flattening them"
        ),
    }
    plan: dict[str, Any] = {
        "schema_version": FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
        "status": "FROZEN",
        "matrix_id": identifier,
        "execution_profile_id": profile,
        "source_git_revision": revision,
        "scenario_id": portable_verified["scenario_id"],
        "scenario_sha256": portable_plan.get("scenario_sha256"),
        "portable_plan_sha256": portable_verified["plan_sha256"],
        "portable_plan_file_sha256": _sha256_path(
            portable_root / "portable_plan.json", "portable plan"
        ),
        "container_plan_manifest_sha256": _sha256_path(
            container_root / "container_plan_manifest.json", "container plan manifest"
        ),
        "container_operations_source_sha256": _sha256_path(
            container_root / "container_operations.jsonl", "container operations"
        ),
        "container_topology_sha256": _sha256_path(
            container_root / "container_topology.json", "container topology"
        ),
        "matrix_dimensions": dimensions,
        "matrix_trials_sha256": _sha256_bytes(trials_bytes),
        "matrix_operations_sha256": _sha256_bytes(operations_bytes),
        "operation_summary": operation_summary,
        "cache_scopes": scope_rows,
        "topology_node_ids": node_ids,
        "node_api_urls": urls,
        "worker_alias": alias,
        "api_task_timeout_seconds": timeout,
        "operation_lower_bound_seconds": bounds,
        "max_operation_lower_bound_seconds": max(
            (item["lower_bound_seconds"] for item in bounds), default=0.0
        ),
        "trial_admission": dict(admission_contract),
        "admission_contract_sha256": _sha256_bytes(admission_bytes),
        "flowmesh_execution_boundary": boundary,
        "runtime_integrity": dict(_RUNTIME_INTEGRITY_REQUIREMENTS),
        "workflow_submitted": False,
        "services_started": False,
        "llm_called": False,
        "semantic_task_quality_evaluated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
    documents = {
        "flowmesh-container-matrix-plan.json": _json_bytes(plan),
        "flowmesh-container-matrix-trials.jsonl": trials_bytes,
        "flowmesh-container-matrix-operations.jsonl": operations_bytes,
        "flowmesh-container-matrix-admission.json": admission_bytes,
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    verified = verify_flowmesh_container_matrix_plan(target)
    return {
        "status": "FROZEN_4X8_CONTAINER_MATRIX",
        "output_dir": str(target),
        "matrix_id": identifier,
        "scenario_id": plan["scenario_id"],
        "matrix_dimensions": dimensions,
        "operation_count": len(copied),
        "conditional_operation_count": operation_summary["conditional_operation_count"],
        "cache_scope_count": operation_summary["cache_scope_count"],
        "api_task_timeout_seconds": timeout,
        "max_operation_lower_bound_seconds": plan[
            "max_operation_lower_bound_seconds"
        ],
        "plan_sha256": plan["plan_sha256"],
        "flowmesh_execution_boundary": boundary,
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
        "verification_status": verified["status"],
    }


def verify_flowmesh_container_matrix_plan(
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Verify a frozen 4×8 matrix package without contacting any service."""

    root, plan, trials, operations, admission = _read_plan(plan_dir)
    return _verify_plan_contents(root, plan, trials, operations, admission)
