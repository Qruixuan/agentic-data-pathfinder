"""FlowMesh execution of one complete, unconditional physical operation chain.

This complements, rather than replaces, :mod:`container_dag`.  The older
three-task DAG is intentionally a small transport conformance smoke.  This
module plans and runs every physical operation on one direct terminal path in
the frozen container-operation ledger, so it cannot silently report a prefix
of a retrieval workload as an end-to-end execution.

The module orchestrates already-running container nodes only.  It starts no
service, calls no LLM, and makes no scientific or semantic-quality claim.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .adapter import extract_api_executor_result
from .container_dag import (
    DEFAULT_API_TASK_TIMEOUT_SECONDS,
    TELEMETRY_DISCLAIMERS,
    TELEMETRY_FIELD_PROVENANCE,
    TELEMETRY_PROVENANCE_VERSION,
    FlowMeshContainerDagError,
    _canonical_bytes,
    _checksums,
    _copy_operation,
    _document_sha256,
    _json_bytes,
    _jsonl_bytes,
    _operation_result,
    _operation_telemetry,
    _require,
    _require_api_timeout_covers,
    _sha256_bytes,
    _task_spec,
    _text,
    _validate_api_task_timeout,
    _validate_node_api_urls,
    _validate_operation,
    _workflow_failure,
    _write_documents,
    derive_operation_lower_bounds,
    load_container_operations,
)
from .contracts import FlowMeshClientProtocol, FlowMeshSettings
from .preflight import describe_pinned_worker
from .redaction import redact_secrets


FLOWMESH_CONTAINER_FULL_CHAIN_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-full-physical-chain-plan/v1alpha1"
)
FLOWMESH_CONTAINER_FULL_CHAIN_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-full-physical-chain-run/v1alpha1"
)

_PLAN_FILES = {
    "flowmesh-container-full-chain-plan.json",
    "flowmesh-container-full-chain-workflow-template.json",
}
_RUN_FILES = {
    "flowmesh-container-full-chain-run.json",
    "flowmesh-container-full-chain-submission.json",
    "flowmesh-container-full-chain-task-results.jsonl",
}
_PHYSICAL_KINDS = frozenset({"storage_read", "network_transfer", "compute"})
_NONPHYSICAL_PREDECESSOR_KINDS = frozenset({"control", "barrier"})


def _is_unconditional(operation: Mapping[str, Any]) -> bool:
    return operation.get("condition") is None


def _validated_operations(
    operations: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    checked = [_validate_operation(row) for row in operations]
    by_key = {row["operation_key"]: row for row in checked}
    _require(len(by_key) == len(checked), "container operation keys are not unique")
    for row in checked:
        missing = [
            key for key in row["dependency_operation_keys"] if key not in by_key
        ]
        _require(
            not missing,
            "container operation names dependencies outside the frozen operation "
            "set: " + ", ".join(sorted(missing)),
        )
    return checked, by_key


def _physical_successors(
    current: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        row
        for row in operations
        if row["trial_key"] == current["trial_key"]
        and row["operation_kind"] in _PHYSICAL_KINDS
        and current["operation_key"] in row["dependency_operation_keys"]
    ]


def _chain_from_read(
    read: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
    by_key: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    """Return a complete direct path beginning at one storage read.

    Any physical branch, conditional successor, non-direct edge, or terminal
    non-compute operation is an ambiguity.  It is refused rather than chosen
    arbitrarily.  This is what distinguishes a complete chain from the older
    deliberately partial three-operation smoke.
    """

    _require(read["operation_kind"] == "storage_read", "chain must start with storage_read")
    _require(_is_unconditional(read), "full physical chain cannot start conditionally")
    predecessors = [by_key[key] for key in read["dependency_operation_keys"]]
    _require(
        all(row["operation_kind"] in _NONPHYSICAL_PREDECESSOR_KINDS for row in predecessors),
        "full physical-chain storage read has an omitted physical predecessor",
    )
    _require(
        all(_is_unconditional(row) for row in predecessors),
        "full physical-chain storage read has a conditional predecessor",
    )

    chain: list[dict[str, Any]] = [_copy_operation(read)]
    current = read
    while True:
        successors = _physical_successors(current, operations)
        if not successors:
            break
        _require(
            len(successors) == 1,
            "full physical chain branches after " + current["operation_key"],
        )
        successor = successors[0]
        _require(
            _is_unconditional(successor),
            "full physical chain has a conditional physical successor after "
            + current["operation_key"],
        )
        _require(
            successor["dependency_operation_keys"] == [current["operation_key"]],
            "full physical chain has a non-direct physical successor after "
            + current["operation_key"],
        )
        chain.append(_copy_operation(successor))
        current = successor

    _require(
        len(chain) >= 3,
        "full physical chain must contain at least storage, transfer, and compute",
    )
    _require(
        chain[-1]["operation_kind"] == "compute",
        "full physical chain does not terminate in compute",
    )
    return tuple(chain)


def select_full_physical_container_operation_chain(
    operations: Sequence[Mapping[str, Any]],
    *,
    trial_key: str | None = None,
) -> tuple[dict[str, Any], ...]:
    """Select exactly one terminal, direct, unconditional physical chain.

    Unlike ``select_linear_container_operation_dag``, this deliberately
    follows every physical successor until a terminal compute operation.  A
    five-operation retrieval path is therefore selected as five operations,
    never truncated to its first three nodes.
    """

    checked, by_key = _validated_operations(operations)
    candidates: list[tuple[dict[str, Any], ...]] = []
    for row in checked:
        if row["operation_kind"] != "storage_read" or not _is_unconditional(row):
            continue
        if trial_key is not None and row["trial_key"] != trial_key:
            continue
        try:
            candidates.append(_chain_from_read(row, checked, by_key))
        except FlowMeshContainerDagError:
            continue
    _require(bool(candidates), "no complete unconditional physical chain was found")
    candidates.sort(key=lambda rows: tuple(row["operation_key"] for row in rows))
    if trial_key is None and len(candidates) > 1:
        trial_keys = sorted({rows[0]["trial_key"] for rows in candidates})
        raise FlowMeshContainerDagError(
            "multiple complete physical chains are available; pass an exact "
            "trial_key (candidates: " + ", ".join(trial_keys) + ")"
        )
    _require(
        len(candidates) == 1,
        "the selected trial contains multiple complete physical chains; choose "
        "a trial with exactly one terminal path",
    )
    return candidates[0]


def list_full_physical_container_operation_chain_candidates(
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """List complete-chain candidates without selecting or submitting one."""

    checked, _ = _validated_operations(operations)
    candidates: list[dict[str, Any]] = []
    for trial_key in sorted({row["trial_key"] for row in checked}):
        try:
            selected = select_full_physical_container_operation_chain(
                checked, trial_key=trial_key
            )
        except FlowMeshContainerDagError:
            continue
        candidates.append(
            {
                "trial_key": trial_key,
                "operation_keys": [row["operation_key"] for row in selected],
                "operation_kinds": [row["operation_kind"] for row in selected],
                "execution_nodes": [row["execution_node_id"] for row in selected],
                "physical_operation_count": len(selected),
                "terminal_physical_operation_key": selected[-1]["operation_key"],
                "omitted_nonphysical_predecessor_operation_keys": list(
                    selected[0]["dependency_operation_keys"]
                ),
            }
        )
    return candidates


def _validate_chain_shape(operations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    copied = [_validate_operation(row) for row in operations]
    _require(len(copied) >= 3, "full physical chain must contain at least three operations")
    _require(
        all(row["operation_kind"] in _PHYSICAL_KINDS for row in copied),
        "full physical chain contains a non-physical operation",
    )
    _require(
        all(_is_unconditional(row) for row in copied),
        "full physical chain contains a conditional operation",
    )
    _require(copied[0]["operation_kind"] == "storage_read", "full physical chain must start with storage_read")
    _require(copied[-1]["operation_kind"] == "compute", "full physical chain must terminate in compute")
    _require(
        len({row["trial_key"] for row in copied}) == 1,
        "all full physical-chain operations must belong to one trial",
    )
    for previous, current in zip(copied, copied[1:]):
        _require(
            current["dependency_operation_keys"] == [previous["operation_key"]],
            "full physical-chain dependencies are not direct",
        )
    return copied


def _task_names(operations: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        f"operation-{index:02d}-{row['operation_kind'].replace('_', '-')}"
        for index, row in enumerate(operations, start=1)
    ]


def build_flowmesh_container_full_physical_chain_workflow(
    operations: Sequence[Mapping[str, Any]],
    *,
    node_api_urls: Mapping[str, str],
    selected_worker_id: str,
    smoke_id: str,
    owner: str = "pathfinder",
    api_task_timeout_seconds: int = DEFAULT_API_TASK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Build a worker-pinned FlowMesh API graph for a complete chain."""

    copied = _validate_chain_shape(operations)
    worker = _text(selected_worker_id, "selected_worker_id")
    run_name = _text(smoke_id, "smoke_id")
    urls = _validate_node_api_urls(copied, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    _require_api_timeout_covers(derive_operation_lower_bounds(copied), timeout)
    names = _task_names(copied)
    nodes: list[dict[str, Any]] = []
    for index, (name, operation) in enumerate(zip(names, copied)):
        node: dict[str, Any] = {
            "name": name,
            "spec": _task_spec(
                operation, urls[operation["execution_node_id"]], timeout
            ),
        }
        if index:
            node["dependsOn"] = [names[index - 1]]
        nodes.append(node)
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": f"pathfinder-full-chain-{run_name[:31]}",
            "owner": _text(owner, "owner"),
            "annotations": {
                "schedule_hint": {"selected_worker": worker},
                "custom": {
                    "pathfinder_smoke_id": run_name,
                    "pathfinder_evidence_class": (
                        "orchestration-transport-compute-workload-path-conformance"
                    ),
                    "pathfinder_trial_key": copied[0]["trial_key"],
                    "pathfinder_operation_keys": [
                        row["operation_key"] for row in copied
                    ],
                    "pathfinder_complete_physical_chain": True,
                    "pathfinder_semantic_quality_evaluated": False,
                },
            },
        },
        "spec": {"graph": {"nodes": nodes}},
    }


def _workflow_template(
    operations: Sequence[Mapping[str, Any]],
    node_api_urls: Mapping[str, str],
    *,
    smoke_id: str,
    worker_alias: str,
    owner: str,
    api_task_timeout_seconds: int,
) -> dict[str, Any]:
    copied = _validate_chain_shape(operations)
    urls = _validate_node_api_urls(copied, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    names = _task_names(copied)
    nodes: list[dict[str, Any]] = []
    for index, (name, operation) in enumerate(zip(names, copied)):
        node: dict[str, Any] = {
            "name": name,
            "spec": _task_spec(
                operation, urls[operation["execution_node_id"]], timeout
            ),
        }
        if index:
            node["dependsOn"] = [names[index - 1]]
        nodes.append(node)
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": f"pathfinder-full-chain-{smoke_id[:31]}",
            "owner": owner,
            "annotations": {
                "custom": {
                    "pathfinder_smoke_id": smoke_id,
                    "pathfinder_worker_alias_to_resolve_at_submission": worker_alias,
                    "pathfinder_workflow_is_not_submittable": True,
                    "pathfinder_reason": (
                        "the current worker ID is deliberately resolved immediately "
                        "before submission"
                    ),
                }
            },
        },
        "spec": {"graph": {"nodes": nodes}},
    }


def plan_flowmesh_container_full_physical_chain(
    *,
    container_operations_path: str | Path,
    node_api_urls: Mapping[str, str],
    worker_alias: str,
    smoke_id: str,
    output_dir: str | Path,
    trial_key: str | None = None,
    owner: str = "pathfinder",
    api_task_timeout_seconds: int = DEFAULT_API_TASK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Freeze one complete existing physical path as a non-submitting plan."""

    source = Path(container_operations_path).resolve()
    selected = select_full_physical_container_operation_chain(
        load_container_operations(source), trial_key=trial_key
    )
    alias = _text(worker_alias, "worker_alias")
    run_name = _text(smoke_id, "smoke_id")
    owner_name = _text(owner, "owner")
    urls = _validate_node_api_urls(selected, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    bounds = derive_operation_lower_bounds(selected)
    _require_api_timeout_covers(bounds, timeout)
    plan: dict[str, Any] = {
        "schema_version": FLOWMESH_CONTAINER_FULL_CHAIN_PLAN_SCHEMA_VERSION,
        "status": "FROZEN",
        "smoke_id": run_name,
        "worker_alias": alias,
        "owner": owner_name,
        "trial_key": selected[0]["trial_key"],
        "container_operations_source_sha256": _sha256_bytes(source.read_bytes()),
        "operations": list(selected),
        "physical_operation_count": len(selected),
        "terminal_physical_operation_key": selected[-1]["operation_key"],
        "omitted_nonphysical_predecessor_operation_keys": list(
            selected[0]["dependency_operation_keys"]
        ),
        "node_api_urls": urls,
        "api_task_timeout_seconds": timeout,
        "operation_lower_bound_seconds": bounds,
        "max_operation_lower_bound_seconds": max(
            (row["lower_bound_seconds"] for row in bounds), default=0.0
        ),
        "evidence_class": "orchestration-transport-compute-workload-path-conformance",
        "llm_called": False,
        "semantic_task_quality_evaluated": False,
        "eligible_for_scientific_claims": False,
        "credentials_recorded": False,
    }
    plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
    template = _workflow_template(
        selected,
        urls,
        smoke_id=run_name,
        worker_alias=alias,
        owner=owner_name,
        api_task_timeout_seconds=timeout,
    )
    documents = {
        "flowmesh-container-full-chain-plan.json": _json_bytes(plan),
        "flowmesh-container-full-chain-workflow-template.json": _json_bytes(template),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {
        "status": "FROZEN_FULL_PHYSICAL_CHAIN_INPUTS",
        "output_dir": str(target),
        "smoke_id": run_name,
        "trial_key": selected[0]["trial_key"],
        "operation_keys": [row["operation_key"] for row in selected],
        "execution_nodes": [row["execution_node_id"] for row in selected],
        "physical_operation_count": len(selected),
        "terminal_physical_operation_key": selected[-1]["operation_key"],
        "worker_alias": alias,
        "plan_sha256": plan["plan_sha256"],
        "api_task_timeout_seconds": timeout,
        "max_operation_lower_bound_seconds": plan[
            "max_operation_lower_bound_seconds"
        ],
        "workflow_submitted": False,
        "services_started": False,
        "llm_called": False,
        "eligible_for_scientific_claims": False,
    }


def _read_plan(plan_dir: str | Path) -> dict[str, Any]:
    root = Path(plan_dir).resolve()
    _require(root.is_dir(), f"full physical-chain plan directory does not exist: {root}")
    try:
        rows = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError("full physical-chain plan checksum file is unreadable") from exc
    observed: dict[str, str] = {}
    for line in rows:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in _PLAN_FILES, "invalid full physical-chain plan checksum row")
        _require(name not in observed, "duplicate full physical-chain plan checksum")
        observed[name] = digest
    _require(set(observed) == _PLAN_FILES, "full physical-chain plan checksum set is incomplete")
    for name, digest in observed.items():
        _require((root / name).is_file(), f"full physical-chain plan file is missing: {name}")
        _require(_sha256_bytes((root / name).read_bytes()) == digest, f"full physical-chain plan checksum mismatch: {name}")
    try:
        plan = json.loads((root / "flowmesh-container-full-chain-plan.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError("full physical-chain plan is invalid JSON") from exc
    _require(isinstance(plan, dict), "full physical-chain plan must be an object")
    _require(plan.get("schema_version") == FLOWMESH_CONTAINER_FULL_CHAIN_PLAN_SCHEMA_VERSION, "unsupported full physical-chain plan schema")
    _require(plan.get("status") == "FROZEN", "full physical-chain plan is not frozen")
    _require(plan.get("plan_sha256") == _document_sha256(plan, "plan_sha256"), "full physical-chain plan digest mismatch")
    operations = plan.get("operations")
    _require(isinstance(operations, list), "full physical-chain plan operations are missing")
    selected = _validate_chain_shape(operations)
    _require(plan.get("physical_operation_count") == len(selected), "full physical-chain plan operation count changed")
    _require(plan.get("terminal_physical_operation_key") == selected[-1]["operation_key"], "full physical-chain terminal operation changed")
    _require(plan.get("trial_key") == selected[0]["trial_key"], "full physical-chain plan trial key changed")
    _require(plan.get("omitted_nonphysical_predecessor_operation_keys") == selected[0]["dependency_operation_keys"], "full physical-chain plan omitted-predecessor record changed")
    urls = plan.get("node_api_urls")
    _require(isinstance(urls, Mapping), "full physical-chain plan has no node API URLs")
    _validate_node_api_urls(selected, urls)
    _text(plan.get("worker_alias"), "worker_alias")
    timeout = _validate_api_task_timeout(plan.get("api_task_timeout_seconds"))
    recomputed = derive_operation_lower_bounds(selected)
    _require(plan.get("operation_lower_bound_seconds") == recomputed, "full physical-chain lower-bound record does not match its operations")
    _require(plan.get("max_operation_lower_bound_seconds") == max((row["lower_bound_seconds"] for row in recomputed), default=0.0), "full physical-chain maximum lower bound does not match its operations")
    _require_api_timeout_covers(recomputed, timeout)
    return plan


def verify_flowmesh_container_full_physical_chain_plan(
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Offline verification of a frozen complete physical-chain plan."""

    plan = _read_plan(plan_dir)
    return {
        "status": "VERIFIED",
        "schema_version": plan["schema_version"],
        "smoke_id": plan["smoke_id"],
        "trial_key": plan["trial_key"],
        "operation_keys": [row["operation_key"] for row in plan["operations"]],
        "execution_nodes": [row["execution_node_id"] for row in plan["operations"]],
        "physical_operation_count": plan["physical_operation_count"],
        "terminal_physical_operation_key": plan["terminal_physical_operation_key"],
        "worker_alias": plan["worker_alias"],
        "plan_sha256": plan["plan_sha256"],
        "api_task_timeout_seconds": plan["api_task_timeout_seconds"],
        "operation_lower_bound_seconds": plan["operation_lower_bound_seconds"],
        "max_operation_lower_bound_seconds": plan["max_operation_lower_bound_seconds"],
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
    }


def _aggregate_telemetry(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate repeat-safe telemetry without making latency claims."""

    service_by_key = {
        str(row["operation_key"]): round(float(row["service_time_ms"]), 6)
        for row in rows
    }
    shaping_by_key = {
        str(row["operation_key"]): row["application_shaping_target_ms"]
        for row in rows
        if row.get("application_shaping_target_ms") is not None
    }
    sums_by_kind: dict[str, float] = {}
    for row in rows:
        kind = str(row["operation_kind"])
        sums_by_kind[kind] = sums_by_kind.get(kind, 0.0) + float(row["service_time_ms"])
    service = [float(row["service_time_ms"]) for row in rows]
    materialization = [
        float(row["fixture_materialization_ms_excluded_from_storage_measurement"])
        for row in rows
    ]
    return {
        "telemetry_provenance_version": TELEMETRY_PROVENANCE_VERSION,
        "record_count": len(rows),
        "telemetry_complete_record_count": sum(
            1 for row in rows if row.get("telemetry_complete") is True
        ),
        "service_time_ms_by_operation_key": service_by_key,
        "service_time_ms_sum_by_operation_kind": {
            key: round(value, 6) for key, value in sorted(sums_by_kind.items())
        },
        "service_time_ms_sum": round(sum(service), 6),
        "service_time_ms_max": round(max(service), 6) if service else 0.0,
        "service_time_ms_min": round(min(service), 6) if service else 0.0,
        "fixture_materialization_ms_sum_excluded_from_storage_measurement": round(sum(materialization), 6),
        "configured_application_shaping_target_ms_by_operation_key": shaping_by_key,
        "logical_bytes_sum": sum(int(row["logical_bytes"]) for row in rows),
        "physical_bytes_sum": sum(int(row["physical_bytes"]) for row in rows),
        "service_time_ms_sum_is_end_to_end_latency": False,
        "network_throughput_derived": False,
        "queue_time_measured": False,
    }


def _read_run(
    run_dir: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    root = Path(run_dir).resolve()
    _require(root.is_dir(), f"full physical-chain run directory does not exist: {root}")
    try:
        checksum_rows = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError("full physical-chain run checksum file is unreadable") from exc
    observed: dict[str, str] = {}
    for line in checksum_rows:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in _RUN_FILES, "invalid full physical-chain run checksum row")
        _require(name not in observed, "duplicate full physical-chain run checksum")
        observed[name] = digest
    _require(set(observed) == _RUN_FILES, "full physical-chain run checksum set is incomplete")
    for name, digest in observed.items():
        _require((root / name).is_file(), f"full physical-chain run file is missing: {name}")
        _require(_sha256_bytes((root / name).read_bytes()) == digest, f"full physical-chain run checksum mismatch: {name}")
    try:
        summary = json.loads((root / "flowmesh-container-full-chain-run.json").read_text(encoding="utf-8"))
        submission = json.loads((root / "flowmesh-container-full-chain-submission.json").read_text(encoding="utf-8"))
        results = [
            json.loads(line)
            for line in (root / "flowmesh-container-full-chain-task-results.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError("full physical-chain run artifact is invalid JSON") from exc
    _require(isinstance(summary, dict) and isinstance(submission, dict), "full physical-chain run documents must be objects")
    _require(all(isinstance(row, dict) for row in results), "each full physical-chain task result must be an object")
    return summary, results, submission


def verify_flowmesh_container_full_physical_chain_run(
    run_dir: str | Path,
    *,
    plan_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Offline verification of a complete-chain run artifact."""

    summary, rows, submission = _read_run(run_dir)
    _require(summary.get("schema_version") == FLOWMESH_CONTAINER_FULL_CHAIN_RUN_SCHEMA_VERSION, "unsupported full physical-chain run schema")
    _require(summary.get("status") == "COMPLETE", "full physical-chain run is not complete")
    operation_keys = summary.get("operation_keys")
    _require(isinstance(operation_keys, list) and operation_keys, "full physical-chain run summary has no operation keys")
    _require([row.get("operation_key") for row in rows] == operation_keys, "full physical-chain task result order or coverage changed")
    _require(len(rows) == len(operation_keys) == summary.get("task_result_count"), "full physical-chain task result count is wrong")
    _require(summary.get("physical_operation_count") == len(rows), "full physical-chain operation count is wrong")
    worker = summary.get("selected_worker")
    _require(isinstance(worker, Mapping), "full physical-chain run has no selected worker")
    worker_id = _text(worker.get("worker_id"), "selected worker_id")
    _require(submission.get("selected_worker_id") == worker_id, "full physical-chain submission worker does not match the run summary")
    _require(all(row.get("worker_id") == worker_id for row in rows), "a full physical-chain task names a worker other than the pin")
    _require(all(row.get("api_http_status") == 200 for row in rows), "a full physical-chain task does not report a 200 API status")
    for row in rows:
        _text(row.get("container_result_sha256"), "container_result_sha256")
        _require(row.get("telemetry_complete") is True, "full physical-chain telemetry is incomplete")
        _require(row.get("telemetry_provenance_version") == TELEMETRY_PROVENANCE_VERSION, "full physical-chain telemetry provenance changed")
        _operation_telemetry(row, {"operation_kind": row.get("operation_kind")})
    _require(summary.get("telemetry") == _aggregate_telemetry(rows), "full physical-chain telemetry aggregate does not match task results")
    _require(summary.get("telemetry_provenance", {}).get("fields") == TELEMETRY_FIELD_PROVENANCE, "full physical-chain telemetry provenance record changed")
    if plan_dir is not None:
        plan = _read_plan(plan_dir)
        _require(plan["plan_sha256"] == summary.get("plan_sha256"), "full physical-chain run is not bound to the supplied plan")
        _require([row["operation_key"] for row in plan["operations"]] == operation_keys, "full physical-chain run operations do not match the supplied plan")
        _require(plan["worker_alias"] == worker.get("alias", plan["worker_alias"]), "full physical-chain run worker alias does not match the plan")
    return {
        "status": "VERIFIED",
        "schema_version": summary["schema_version"],
        "smoke_id": summary.get("smoke_id"),
        "plan_sha256": summary.get("plan_sha256"),
        "worker_id": worker_id,
        "task_result_count": len(rows),
        "physical_operation_count": len(rows),
        "plan_binding_checked": plan_dir is not None,
        "telemetry_recording": "whitelisted-validated",
        "telemetry": summary["telemetry"],
        "eligible_for_scientific_claims": False,
    }


def run_flowmesh_container_full_physical_chain(
    *,
    plan_dir: str | Path,
    output_dir: str | Path,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
) -> dict[str, Any]:
    """Validate, submit, and record every task of one frozen full chain."""

    plan = _read_plan(plan_dir)
    _require(settings.worker_alias == plan["worker_alias"], "run worker alias must exactly match the frozen full physical-chain plan")
    identity = describe_pinned_worker(client, settings)
    workflow = build_flowmesh_container_full_physical_chain_workflow(
        plan["operations"],
        node_api_urls=plan["node_api_urls"],
        selected_worker_id=identity.worker_id,
        smoke_id=plan["smoke_id"],
        owner=plan["owner"],
        api_task_timeout_seconds=plan["api_task_timeout_seconds"],
    )
    validation = client.validate(workflow)
    _require(validation.ok, "FlowMesh rejected the full physical-chain workflow: " + "; ".join(validation.errors))
    submitted = client.submit(workflow)
    _require(len(submitted.task_ids) == len(plan["operations"]), "FlowMesh returned a task count other than the frozen full physical-chain nodes")
    terminal = client.wait(submitted.workflow_id, settings.poll_interval_seconds)
    if terminal.status != "DONE":
        raise _workflow_failure(terminal, submitted, client)

    expected_by_key = {row["operation_key"]: row for row in plan["operations"]}
    task_results: list[dict[str, Any]] = []
    for task_id in submitted.task_ids:
        raw = client.retrieve_result(task_id)
        api = extract_api_executor_result(raw)
        try:
            body = json.loads(api["text"])
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerDagError(f"FlowMesh task {task_id} returned non-JSON container output") from exc
        _require(isinstance(body, Mapping), "container result must be an object")
        operation_key = body.get("operation_key")
        _require(isinstance(operation_key, str) and operation_key in expected_by_key, "FlowMesh task returned an operation outside the frozen full physical-chain plan")
        try:
            task_detail = client.describe_task_failure(task_id)
        except Exception as exc:
            raise FlowMeshContainerDagError("cannot verify the FlowMesh worker assigned to a completed full physical-chain task: " + redact_secrets(str(exc))) from exc
        _require(isinstance(task_detail, Mapping), "FlowMesh did not return task metadata needed to verify the worker pin")
        task_results.append(_operation_result(raw, expected_by_key[operation_key], task_id=task_id, selected_worker_id=identity.worker_id, task_detail=task_detail))
    _require({row["operation_key"] for row in task_results} == set(expected_by_key), "FlowMesh task results do not cover the exact frozen full physical-chain operations")
    _require(len(task_results) == len(expected_by_key), "FlowMesh returned duplicate full physical-chain task results")
    result_by_key = {row["operation_key"]: row for row in task_results}
    task_results = [result_by_key[row["operation_key"]] for row in plan["operations"]]
    names = _task_names(plan["operations"])
    summary = {
        "schema_version": FLOWMESH_CONTAINER_FULL_CHAIN_RUN_SCHEMA_VERSION,
        "status": "COMPLETE",
        "smoke_id": plan["smoke_id"],
        "plan_sha256": plan["plan_sha256"],
        "workflow_id": submitted.workflow_id,
        "task_ids": list(submitted.task_ids),
        "selected_worker": identity.to_public_dict(),
        "operation_keys": [row["operation_key"] for row in plan["operations"]],
        "execution_nodes": [row["execution_node_id"] for row in plan["operations"]],
        "physical_operation_count": len(task_results),
        "terminal_physical_operation_key": plan["terminal_physical_operation_key"],
        "task_result_count": len(task_results),
        "flowmesh_graph_dependencies": {
            name: ([] if index == 0 else [names[index - 1]])
            for index, name in enumerate(names)
        },
        "telemetry": _aggregate_telemetry(task_results),
        "telemetry_provenance": {
            "version": TELEMETRY_PROVENANCE_VERSION,
            "fields": dict(TELEMETRY_FIELD_PROVENANCE),
            "disclaimers": list(TELEMETRY_DISCLAIMERS),
        },
        "evidence_class": "orchestration-transport-compute-workload-path-conformance",
        "llm_called": False,
        "semantic_task_quality_evaluated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    submission = {
        "workflow_id": submitted.workflow_id,
        "task_ids": list(submitted.task_ids),
        "selected_worker_id": identity.worker_id,
        "workflow_sha256": _sha256_bytes(_canonical_bytes(workflow)),
        "validated_before_submission": True,
        "credentials_recorded": False,
    }
    documents = {
        "flowmesh-container-full-chain-run.json": _json_bytes(summary),
        "flowmesh-container-full-chain-submission.json": _json_bytes(submission),
        "flowmesh-container-full-chain-task-results.jsonl": _jsonl_bytes(task_results),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {**summary, "output_dir": str(target)}
