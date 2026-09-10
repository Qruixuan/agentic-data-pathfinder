"""FlowMesh orchestration for a small, physical container-operation DAG.

This module deliberately keeps the control plane and the physical data plane
separate.  FlowMesh schedules three API tasks with explicit dependencies;
each task calls an already-running Pathfinder container node.  The container
nodes perform bounded fixture storage, HTTP transfer, and CPU work.  No LLM
is called and no semantic task-quality conclusion is produced.

The implementation is intended for an integration smoke, not for the 64-trial
simulator experiment.  It selects an existing linear chain from the frozen
container-operation contract rather than inventing new operation attributes.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlsplit

from ...simulator.container_contract import CONTAINER_OPERATION_SCHEMA_VERSION
from .adapter import FlowMeshRunError, extract_api_executor_result
from .contracts import (
    FlowMeshClientProtocol,
    FlowMeshSettings,
    SubmittedWorkflow,
    TerminalWorkflow,
)
from .preflight import describe_pinned_worker
from .redaction import redact_secrets


FLOWMESH_CONTAINER_DAG_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-operation-dag-plan/v1alpha1"
)
FLOWMESH_CONTAINER_DAG_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-operation-dag-run/v1alpha1"
)

_PLAN_FILES = {
    "flowmesh-container-dag-plan.json",
    "flowmesh-container-dag-workflow-template.json",
}
_RUN_FILES = {
    "flowmesh-container-dag-run.json",
    "flowmesh-container-dag-submission.json",
    "flowmesh-container-dag-task-results.jsonl",
}
_STEP_NAMES = ("storage-read", "network-transfer", "compute")
_STEP_KINDS = ("storage_read", "network_transfer", "compute")


class FlowMeshContainerDagError(FlowMeshRunError):
    """Raised when a container-operation DAG cannot be safely run."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FlowMeshContainerDagError(message)


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


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    copied = dict(value)
    copied.pop(field, None)
    return _sha256_bytes(_canonical_bytes(copied))


def _text(value: Any, field: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{field} must be a non-empty string",
    )
    return value.strip()


def _copy_operation(value: Mapping[str, Any]) -> dict[str, Any]:
    # JSON round-tripping prevents a caller from mutating a nested field after
    # validation and before workflow construction.
    try:
        copied = json.loads(_canonical_bytes(value).decode("utf-8"))
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError(
            "container operation cannot be canonicalized"
        ) from exc
    _require(isinstance(copied, dict), "container operation must be an object")
    return copied


def _validate_operation(value: Mapping[str, Any]) -> dict[str, Any]:
    operation = _copy_operation(value)
    _require(
        operation.get("schema_version") == CONTAINER_OPERATION_SCHEMA_VERSION,
        "unsupported container operation schema_version",
    )
    _text(operation.get("operation_key"), "operation_key")
    _text(operation.get("trial_key"), "trial_key")
    _text(operation.get("operation_id"), "operation_id")
    _text(operation.get("operation_kind"), "operation_kind")
    _text(operation.get("execution_node_id"), "execution_node_id")
    _require(
        type(operation.get("logical_bytes")) is int
        and operation["logical_bytes"] >= 0,
        "logical_bytes must be a non-negative integer",
    )
    dependencies = operation.get("dependency_operation_keys")
    _require(
        isinstance(dependencies, list)
        and all(isinstance(item, str) and item for item in dependencies),
        "dependency_operation_keys must be a list of non-empty strings",
    )
    _require(
        operation.get("condition") is None,
        "conditional container operations are not valid for a linear DAG smoke",
    )
    return operation


def load_container_operations(path: str | Path) -> list[dict[str, Any]]:
    """Load and validate one frozen ``container_operations.jsonl`` file."""

    source = Path(path).resolve()
    try:
        lines = source.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError(
            f"cannot read container operations: {source}"
        ) from exc
    operations: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerDagError(
                f"container operations contain invalid JSON at line {line_number}"
            ) from exc
        _require(
            isinstance(value, Mapping),
            f"container operation at line {line_number} is not an object",
        )
        operations.append(_validate_operation(value))
    _require(bool(operations), "container operations cannot be empty")
    keys = [row["operation_key"] for row in operations]
    _require(len(keys) == len(set(keys)), "container operation keys are not unique")
    return operations


def select_linear_container_operation_dag(
    operations: Sequence[Mapping[str, Any]],
    *,
    trial_key: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Select one exact ``storage_read -> network_transfer -> compute`` chain.

    The chain must be direct and unconditional within a single frozen trial.
    Refusing ambiguity is intentional: the smoke needs a small, inspectable
    dependency graph rather than silently choosing a path through a cache
    branch or a multi-parent operation.
    """

    checked = [_validate_operation(row) for row in operations]
    by_key = {row["operation_key"]: row for row in checked}
    _require(len(by_key) == len(checked), "container operation keys are not unique")
    candidates: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for read in checked:
        if read["operation_kind"] != "storage_read":
            continue
        if trial_key is not None and read["trial_key"] != trial_key:
            continue
        # The three-node smoke intentionally omits non-physical scheduling
        # markers, but may not skip a real I/O/compute predecessor.  Without
        # this guard a W2 index path could be made to look like a complete
        # read-transfer-compute path while silently dropping the index return.
        read_predecessors = [
            by_key.get(key) for key in read["dependency_operation_keys"]
        ]
        _require(
            all(item is not None for item in read_predecessors),
            "storage read names a dependency outside the frozen operation set",
        )
        if any(
            item["operation_kind"] not in ("control", "barrier")
            for item in read_predecessors
            if item is not None
        ):
            continue
        for transfer in checked:
            if (
                transfer["operation_kind"] != "network_transfer"
                or transfer["trial_key"] != read["trial_key"]
                or transfer["dependency_operation_keys"] != [read["operation_key"]]
            ):
                continue
            for compute in checked:
                if (
                    compute["operation_kind"] != "compute"
                    or compute["trial_key"] != read["trial_key"]
                    or compute["dependency_operation_keys"]
                    != [transfer["operation_key"]]
                ):
                    continue
                # This ensures the direct dependency keys are not merely
                # strings that happen to appear in another trial.
                _require(
                    by_key[transfer["operation_key"]]["dependency_operation_keys"]
                    == [read["operation_key"]],
                    "transfer dependency changed during validation",
                )
                candidates.append((read, transfer, compute))
    _require(
        bool(candidates),
        "no unconditional storage_read -> network_transfer -> compute chain was found",
    )
    candidates.sort(
        key=lambda items: tuple(item["operation_key"] for item in items)
    )
    if trial_key is None and len(candidates) > 1:
        trial_keys = sorted({items[0]["trial_key"] for items in candidates})
        raise FlowMeshContainerDagError(
            "multiple linear container-operation chains are available; pass an "
            "exact trial_key (candidates: " + ", ".join(trial_keys) + ")"
        )
    if trial_key is not None and len(candidates) > 1:
        raise FlowMeshContainerDagError(
            "the selected trial contains multiple linear container-operation chains; "
            "choose a trial with exactly one chain"
        )
    return tuple(_copy_operation(item) for item in candidates[0])  # type: ignore[return-value]


def list_linear_container_operation_dag_candidates(
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """List safe, direct physical DAG candidates without choosing one.

    The response is intentionally small enough to make an operator pin one
    workload/trial before submitting anything to FlowMesh.
    """

    checked = [_validate_operation(row) for row in operations]
    trial_keys = sorted({row["trial_key"] for row in checked})
    candidates: list[dict[str, Any]] = []
    for key in trial_keys:
        try:
            selected = select_linear_container_operation_dag(
                checked, trial_key=key
            )
        except FlowMeshContainerDagError:
            continue
        candidates.append({
            "trial_key": key,
            "operation_keys": [row["operation_key"] for row in selected],
            "operation_kinds": [row["operation_kind"] for row in selected],
            "execution_nodes": [row["execution_node_id"] for row in selected],
            "omitted_nonphysical_predecessor_operation_keys": list(
                selected[0]["dependency_operation_keys"]
            ),
        })
    return candidates


def _validate_node_api_urls(
    operations: Sequence[Mapping[str, Any]],
    node_api_urls: Mapping[str, str],
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for node_id, raw_url in node_api_urls.items():
        name = _text(node_id, "node API URL node_id")
        url = _text(raw_url, f"node API URL for {name}").rstrip("/")
        parsed = urlsplit(url)
        _require(
            parsed.scheme == "http" and parsed.hostname is not None,
            f"node API URL for {name} must be an absolute http URL",
        )
        _require(
            parsed.username is None and parsed.password is None,
            f"node API URL for {name} must not contain credentials",
        )
        normalized[name] = url
    required = {str(row["execution_node_id"]) for row in operations}
    missing = sorted(required - set(normalized))
    _require(
        not missing,
        "no API URL was supplied for execution node(s): " + ", ".join(missing),
    )
    return dict(sorted(normalized.items()))


def _task_spec(operation: Mapping[str, Any], url: str) -> dict[str, Any]:
    return {
        "taskType": "api",
        "api": {
            "url": url.rstrip("/") + "/v1/operations/execute",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": _copy_operation(operation),
            "timeout_sec": 120,
            "response": {
                "parse_json": False,
                "return_body": True,
                "raise_for_status": True,
                "max_body_bytes": 131072,
            },
        },
        # FlowMesh result retrieval is Root-based.  ``http`` with no URL
        # directs a current FlowMesh worker to its own Root result endpoint
        # without embedding a bearer token in this workflow.
        "output": {"destination": {"type": "http"}, "artifacts": []},
    }


def build_flowmesh_container_operation_workflow(
    operations: Sequence[Mapping[str, Any]],
    *,
    node_api_urls: Mapping[str, str],
    selected_worker_id: str,
    smoke_id: str,
    owner: str = "pathfinder",
) -> dict[str, Any]:
    """Build a worker-pinned FlowMesh API graph for an exact three-step DAG."""

    _require(len(operations) == 3, "container DAG must contain exactly three operations")
    copied = [_validate_operation(row) for row in operations]
    _require(
        tuple(row["operation_kind"] for row in copied) == _STEP_KINDS,
        "container DAG kinds must be storage_read, network_transfer, compute",
    )
    _require(
        len({row["trial_key"] for row in copied}) == 1,
        "all container DAG operations must belong to one trial",
    )
    _require(
        copied[1]["dependency_operation_keys"] == [copied[0]["operation_key"]]
        and copied[2]["dependency_operation_keys"] == [copied[1]["operation_key"]],
        "container DAG dependencies are not a direct three-step chain",
    )
    worker = _text(selected_worker_id, "selected_worker_id")
    run_name = _text(smoke_id, "smoke_id")
    resolved_urls = _validate_node_api_urls(copied, node_api_urls)
    nodes: list[dict[str, Any]] = []
    for index, (name, operation) in enumerate(zip(_STEP_NAMES, copied)):
        item: dict[str, Any] = {
            "name": name,
            "spec": _task_spec(
                operation,
                resolved_urls[operation["execution_node_id"]],
            ),
        }
        if index:
            item["dependsOn"] = [_STEP_NAMES[index - 1]]
        nodes.append(item)
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": f"pathfinder-container-{run_name[:36]}",
            "owner": _text(owner, "owner"),
            "annotations": {
                "schedule_hint": {"selected_worker": worker},
                "custom": {
                    "pathfinder_smoke_id": run_name,
                    "pathfinder_evidence_class": (
                        "orchestration-transport-compute-conformance"
                    ),
                    "pathfinder_trial_key": copied[0]["trial_key"],
                    "pathfinder_operation_keys": [
                        row["operation_key"] for row in copied
                    ],
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
) -> dict[str, Any]:
    """Build a non-submittable template that contains no volatile worker ID."""

    urls = _validate_node_api_urls(operations, node_api_urls)
    nodes: list[dict[str, Any]] = []
    for index, (name, operation) in enumerate(zip(_STEP_NAMES, operations)):
        node: dict[str, Any] = {
            "name": name,
            "spec": _task_spec(
                operation,
                urls[str(operation["execution_node_id"])],
            ),
        }
        if index:
            node["dependsOn"] = [_STEP_NAMES[index - 1]]
        nodes.append(node)
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": f"pathfinder-container-{smoke_id[:36]}",
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


def _write_documents(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".flowmesh-container-dag-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in sorted(documents.items()):
            path = staging / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )


def _read_plan(plan_dir: str | Path) -> dict[str, Any]:
    root = Path(plan_dir).resolve()
    _require(root.is_dir(), f"container DAG plan directory does not exist: {root}")
    try:
        checksums = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError("container DAG plan checksum file is unreadable") from exc
    observed: dict[str, str] = {}
    for line in checksums:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in _PLAN_FILES, "invalid container DAG plan checksum row")
        _require(name not in observed, "duplicate container DAG plan checksum")
        observed[name] = digest
    _require(set(observed) == _PLAN_FILES, "container DAG plan checksum set is incomplete")
    for name, digest in observed.items():
        _require((root / name).is_file(), f"container DAG plan file is missing: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"container DAG plan checksum mismatch: {name}",
        )
    try:
        plan = json.loads((root / "flowmesh-container-dag-plan.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError("container DAG plan is invalid JSON") from exc
    _require(isinstance(plan, dict), "container DAG plan must be an object")
    _require(plan.get("schema_version") == FLOWMESH_CONTAINER_DAG_PLAN_SCHEMA_VERSION, "unsupported container DAG plan schema")
    _require(plan.get("status") == "FROZEN", "container DAG plan is not frozen")
    _require(plan.get("plan_sha256") == _document_sha256(plan, "plan_sha256"), "container DAG plan digest mismatch")
    operations = plan.get("operations")
    _require(isinstance(operations, list), "container DAG plan operations are missing")
    selected = tuple(_validate_operation(row) for row in operations)
    _require(len(selected) == 3, "container DAG plan must contain exactly three operations")
    _require(
        tuple(row["operation_kind"] for row in selected) == _STEP_KINDS,
        "container DAG plan operation kinds changed",
    )
    _require(
        selected[1]["dependency_operation_keys"] == [selected[0]["operation_key"]]
        and selected[2]["dependency_operation_keys"] == [selected[1]["operation_key"]],
        "container DAG plan dependencies changed",
    )
    _require(
        plan.get("omitted_nonphysical_predecessor_operation_keys")
        == selected[0]["dependency_operation_keys"],
        "container DAG plan omitted-predecessor record changed",
    )
    urls = plan.get("node_api_urls")
    _require(isinstance(urls, Mapping), "container DAG plan has no node API URLs")
    _validate_node_api_urls(selected, urls)
    _text(plan.get("worker_alias"), "worker_alias")
    return plan


def plan_flowmesh_container_operation_dag(
    *,
    container_operations_path: str | Path,
    node_api_urls: Mapping[str, str],
    worker_alias: str,
    smoke_id: str,
    output_dir: str | Path,
    trial_key: str | None = None,
    owner: str = "pathfinder",
) -> dict[str, Any]:
    """Freeze an exact three-operation FlowMesh container-DAG input package.

    The package has an alias, rather than a current worker ID.  A current ID
    is intentionally resolved by the run command just before submission.
    """

    alias = _text(worker_alias, "worker_alias")
    run_name = _text(smoke_id, "smoke_id")
    owner_name = _text(owner, "owner")
    source = Path(container_operations_path).resolve()
    selected = select_linear_container_operation_dag(
        load_container_operations(source), trial_key=trial_key
    )
    urls = _validate_node_api_urls(selected, node_api_urls)
    plan = {
        "schema_version": FLOWMESH_CONTAINER_DAG_PLAN_SCHEMA_VERSION,
        "status": "FROZEN",
        "smoke_id": run_name,
        "worker_alias": alias,
        "owner": owner_name,
        "trial_key": selected[0]["trial_key"],
        "container_operations_source_sha256": _sha256_bytes(source.read_bytes()),
        "operations": list(selected),
        "omitted_nonphysical_predecessor_operation_keys": list(
            selected[0]["dependency_operation_keys"]
        ),
        "node_api_urls": urls,
        "evidence_class": "orchestration-transport-compute-conformance",
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
    )
    documents = {
        "flowmesh-container-dag-plan.json": _json_bytes(plan),
        "flowmesh-container-dag-workflow-template.json": _json_bytes(template),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {
        "status": "FROZEN_WORKFLOW_INPUTS",
        "output_dir": str(target),
        "smoke_id": run_name,
        "trial_key": selected[0]["trial_key"],
        "operation_keys": [row["operation_key"] for row in selected],
        "execution_nodes": [row["execution_node_id"] for row in selected],
        "worker_alias": alias,
        "plan_sha256": plan["plan_sha256"],
        "workflow_submitted": False,
        "services_started": False,
        "llm_called": False,
        "eligible_for_scientific_claims": False,
    }


def verify_flowmesh_container_operation_dag_plan(
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Offline verification for a frozen three-operation DAG package."""

    plan = _read_plan(plan_dir)
    return {
        "status": "VERIFIED",
        "smoke_id": plan["smoke_id"],
        "trial_key": plan["trial_key"],
        "operation_keys": [row["operation_key"] for row in plan["operations"]],
        "execution_nodes": [row["execution_node_id"] for row in plan["operations"]],
        "worker_alias": plan["worker_alias"],
        "plan_sha256": plan["plan_sha256"],
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
    }


def _workflow_failure(
    terminal: TerminalWorkflow,
    submitted: SubmittedWorkflow,
    client: FlowMeshClientProtocol,
) -> FlowMeshContainerDagError:
    details: list[str] = [terminal.detail] if terminal.detail else []
    for task_id in submitted.task_ids:
        try:
            detail = client.describe_task_failure(task_id)
        except Exception as exc:  # diagnostic retrieval must not mask failure
            details.append("task detail lookup failed: " + redact_secrets(str(exc)))
            continue
        if detail:
            details.append(
                json.dumps(detail, sort_keys=True, ensure_ascii=False)
            )
    return FlowMeshContainerDagError(
        f"FlowMesh container DAG {submitted.workflow_id} ended with {terminal.status}: "
        + ("; ".join(details) or "no task detail was reported")
    )


def _operation_result(
    raw: Mapping[str, Any],
    operation: Mapping[str, Any],
    *,
    task_id: str,
    selected_worker_id: str,
    task_detail: Mapping[str, Any] | None,
) -> dict[str, Any]:
    api = extract_api_executor_result(raw)
    try:
        result = json.loads(api["text"])
    except json.JSONDecodeError as exc:
        raise FlowMeshContainerDagError(
            f"FlowMesh task {task_id} returned non-JSON container output"
        ) from exc
    _require(isinstance(result, Mapping), "container result must be an object")
    _require(result.get("status") == "completed", "container operation did not complete")
    _require(result.get("outcome_type") == "completed", "container outcome is not completed")
    _require(result.get("telemetry_complete") is True, "container telemetry is incomplete")
    _require(result.get("credentials_recorded") is False, "container result recorded credentials")
    _require(result.get("idempotent_replay") is False, "container operation was replayed")
    for field in ("operation_key", "operation_kind", "execution_node_id"):
        _require(
            result.get(field) == operation.get(field),
            f"container result changed {field}",
        )
    _require(
        result.get("logical_bytes") == operation.get("logical_bytes"),
        "container result changed logical byte count",
    )
    if operation["operation_kind"] in ("storage_read", "network_transfer"):
        _require(
            result.get("physical_bytes") == operation["logical_bytes"],
            "container I/O result did not report exact physical bytes",
        )
    else:
        _require(
            result.get("physical_bytes") == 0,
            "container compute result must not report physical transfer bytes",
        )
    if task_detail is not None:
        assigned = task_detail.get("assigned_worker")
        _require(
            assigned == selected_worker_id,
            "FlowMesh assigned a container DAG task to a worker other than the pin",
        )
    return {
        "task_id": task_id,
        "worker_id": selected_worker_id,
        "operation_key": operation["operation_key"],
        "operation_kind": operation["operation_kind"],
        "execution_node_id": operation["execution_node_id"],
        "logical_bytes": result["logical_bytes"],
        "physical_bytes": result["physical_bytes"],
        "telemetry_complete": True,
        "idempotent_replay": False,
        "api_executor": "api",
        "api_http_status": api["status_code"],
        "container_result_sha256": _sha256_bytes(_canonical_bytes(result)),
        "task_detail_available": task_detail is not None,
    }


def run_flowmesh_container_operation_dag(
    *,
    plan_dir: str | Path,
    output_dir: str | Path,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
) -> dict[str, Any]:
    """Submit exactly one validated FlowMesh graph and verify all three tasks.

    This performs no container, Docker, tunnel, or worker lifecycle action.
    Every service must already be running and the selected worker must already
    be visible to the configured FlowMesh Root.
    """

    plan = _read_plan(plan_dir)
    _require(
        settings.worker_alias == plan["worker_alias"],
        "run worker alias must exactly match the frozen DAG plan",
    )
    identity = describe_pinned_worker(client, settings)
    workflow = build_flowmesh_container_operation_workflow(
        plan["operations"],
        node_api_urls=plan["node_api_urls"],
        selected_worker_id=identity.worker_id,
        smoke_id=plan["smoke_id"],
        owner=plan["owner"],
    )
    validation = client.validate(workflow)
    _require(
        validation.ok,
        "FlowMesh rejected the container DAG workflow: "
        + "; ".join(validation.errors),
    )
    submitted = client.submit(workflow)
    _require(
        len(submitted.task_ids) == 3,
        "FlowMesh returned a task count other than the three planned DAG nodes",
    )
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
            raise FlowMeshContainerDagError(
                f"FlowMesh task {task_id} returned non-JSON container output"
            ) from exc
        _require(isinstance(body, Mapping), "container result must be an object")
        operation_key = body.get("operation_key")
        _require(
            isinstance(operation_key, str) and operation_key in expected_by_key,
            "FlowMesh task returned an operation outside the frozen DAG plan",
        )
        try:
            task_detail = client.describe_task_failure(task_id)
        except Exception as exc:
            raise FlowMeshContainerDagError(
                "cannot verify the FlowMesh worker assigned to a completed DAG task: "
                + redact_secrets(str(exc))
            ) from exc
        _require(
            isinstance(task_detail, Mapping),
            "FlowMesh did not return task metadata needed to verify the worker pin",
        )
        task_results.append(
            _operation_result(
                raw,
                expected_by_key[operation_key],
                task_id=task_id,
                selected_worker_id=identity.worker_id,
                task_detail=task_detail,
            )
        )
    _require(
        {row["operation_key"] for row in task_results} == set(expected_by_key),
        "FlowMesh task results do not cover the exact frozen DAG operations",
    )
    task_results.sort(key=lambda row: _STEP_NAMES.index({
        "storage_read": "storage-read",
        "network_transfer": "network-transfer",
        "compute": "compute",
    }[row["operation_kind"]]))
    summary = {
        "schema_version": FLOWMESH_CONTAINER_DAG_RUN_SCHEMA_VERSION,
        "status": "COMPLETE",
        "smoke_id": plan["smoke_id"],
        "plan_sha256": plan["plan_sha256"],
        "workflow_id": submitted.workflow_id,
        "task_ids": list(submitted.task_ids),
        "selected_worker": identity.to_public_dict(),
        "operation_keys": [row["operation_key"] for row in plan["operations"]],
        "execution_nodes": [row["execution_node_id"] for row in plan["operations"]],
        "task_result_count": len(task_results),
        "flowmesh_graph_dependencies": {
            "storage-read": [],
            "network-transfer": ["storage-read"],
            "compute": ["network-transfer"],
        },
        "evidence_class": "orchestration-transport-compute-conformance",
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
        "flowmesh-container-dag-run.json": _json_bytes(summary),
        "flowmesh-container-dag-submission.json": _json_bytes(submission),
        "flowmesh-container-dag-task-results.jsonl": _jsonl_bytes(task_results),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {**summary, "output_dir": str(target)}
