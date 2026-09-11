"""Plan-bound FlowMesh execution for one cache-conditional container trial.

The container operation ledger describes both sides of cache hit/miss paths,
but FlowMesh API graphs are static.  This module therefore submits two
separate, immutable graphs:

* phase A executes only unconditional ancestors and cache lookups;
* phase B is submitted only after phase-A observations match the cache
  outcome vector frozen in the trial plan.

It is deliberately a *single-trial* runner.  The 4x8 matrix coordinator owns
cross-trial admission and must honour the matrix cache-lane serialization
contract.  This runner never starts containers, changes a worker lifecycle,
or calls an LLM.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .adapter import extract_api_executor_result
from .container_conditional_dag import resolve_conditional_container_trial
from .container_dag import (
    CONTAINER_NODE_RESULT_SCHEMA_VERSION,
    FlowMeshContainerDagError,
    _canonical_bytes,
    _checksums,
    _document_sha256,
    _json_bytes,
    _jsonl_bytes,
    _probe_container_runtime_epochs,
    _require,
    _require_api_timeout_covers,
    _runtime_epoch,
    _runtime_epoch_binding,
    _sha256_bytes,
    _task_spec,
    _text,
    _validate_api_task_timeout,
    _validate_node_api_urls,
    _validate_operation,
    _validate_runtime_epochs,
    _verify_runtime_epoch_binding,
    _workflow_failure,
    _write_documents,
    derive_operation_lower_bounds,
)
from .container_matrix import _read_plan as _read_matrix_plan
from .container_matrix import _verify_plan_contents as _verify_matrix_plan_contents
from .contracts import (
    FlowMeshClientProtocol,
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
)
from .preflight import describe_pinned_worker
from .redaction import redact_secrets


FLOWMESH_CONTAINER_CONDITIONAL_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-conditional-trial-plan/v1alpha1"
)
FLOWMESH_CONTAINER_CONDITIONAL_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-conditional-trial-run/v1alpha1"
)

_PLAN_FILES = {
    "flowmesh-container-conditional-plan.json",
    "flowmesh-container-conditional-phase-a-template.json",
    "flowmesh-container-conditional-phase-b-template.json",
}
_RUN_COMMON_FILES = {
    "flowmesh-container-conditional-run.json",
    "flowmesh-container-conditional-phase-a-submission.json",
    "flowmesh-container-conditional-phase-a-task-results.jsonl",
}
_RUN_PHASE_B_FILES = {
    "flowmesh-container-conditional-phase-b-submission.json",
    "flowmesh-container-conditional-phase-b-task-results.jsonl",
}
_IO_OPERATION_KINDS = frozenset(
    {"storage_read", "cache_read", "network_transfer"}
)


def _copy(value: Any, label: str) -> dict[str, Any]:
    try:
        copied = json.loads(_canonical_bytes(value).decode("utf-8"))
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError(
            f"{label} cannot be canonicalized"
        ) from exc
    _require(isinstance(copied, dict), f"{label} must be an object")
    return copied


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
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerDagError(
                f"{label} contains invalid JSON at line {number}"
            ) from exc
        _require(isinstance(value, dict), f"{label} row must be an object")
        rows.append(value)
    return rows


def _check_checksums(root: Path, expected: set[str], label: str) -> None:
    try:
        lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError(f"{label} checksum file is unreadable") from exc
    observed: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in expected,
            f"invalid {label} checksum row",
        )
        _require(name not in observed, f"duplicate {label} checksum")
        observed[name] = digest
    _require(set(observed) == expected, f"{label} checksum set is incomplete")
    for name, digest in observed.items():
        path = root / name
        _require(path.is_file(), f"{label} file is missing: {name}")
        _require(
            _sha256_bytes(path.read_bytes()) == digest,
            f"{label} checksum mismatch: {name}",
        )


def _phase_operations(
    plan: Mapping[str, Any],
    phase: str,
) -> list[dict[str, Any]]:
    _require(phase in {"A", "B"}, "conditional phase must be A or B")
    rows = plan.get("operations")
    _require(isinstance(rows, list), "conditional plan operations are missing")
    by_key: dict[str, dict[str, Any]] = {}
    for raw in rows:
        _require(isinstance(raw, Mapping), "conditional plan operation is invalid")
        row = _validate_operation(raw)
        key = row["operation_key"]
        _require(key not in by_key, "conditional plan operation keys are not unique")
        by_key[key] = row
    resolution = plan.get("resolution")
    _require(isinstance(resolution, Mapping), "conditional plan resolution is missing")
    raw_keys = resolution.get(
        "phase_a_operation_keys" if phase == "A" else "phase_b_operation_keys"
    )
    _require(
        isinstance(raw_keys, list)
        and all(isinstance(key, str) and key for key in raw_keys),
        f"conditional phase {phase} operation keys are invalid",
    )
    _require(len(raw_keys) == len(set(raw_keys)), f"conditional phase {phase} operation keys repeat")
    _require(all(key in by_key for key in raw_keys), f"conditional phase {phase} names an unknown operation")
    return [by_key[key] for key in raw_keys]


def _phase_dependencies(
    plan: Mapping[str, Any],
    phase: str,
    operations: Sequence[Mapping[str, Any]],
) -> dict[str, list[str]]:
    resolution = plan["resolution"]
    assert isinstance(resolution, Mapping)
    field = (
        "resolved_dependency_operation_keys"
        if phase == "A"
        else "phase_b_dependency_operation_keys"
    )
    raw = resolution.get(field)
    _require(isinstance(raw, Mapping), f"conditional phase {phase} dependencies are missing")
    keys = [str(row["operation_key"]) for row in operations]
    _require(set(raw) >= set(keys), f"conditional phase {phase} dependencies are incomplete")
    result: dict[str, list[str]] = {}
    for key in keys:
        values = raw.get(key)
        _require(
            isinstance(values, list)
            and all(isinstance(value, str) and value for value in values),
            f"conditional phase {phase} dependencies are invalid for {key}",
        )
        _require(
            all(value in keys for value in values),
            f"conditional phase {phase} contains a cross-phase dependency",
        )
        result[key] = list(values)
    return result


def _phase_node_names(operations: Sequence[Mapping[str, Any]], phase: str) -> dict[str, str]:
    names: dict[str, str] = {}
    for index, row in enumerate(operations, start=1):
        kind = str(row["operation_kind"]).replace("_", "-")
        names[str(row["operation_key"])] = f"phase-{phase.lower()}-{index:02d}-{kind}"
    return names


def build_flowmesh_container_conditional_phase_workflow(
    operations: Sequence[Mapping[str, Any]],
    *,
    dependencies: Mapping[str, Sequence[str]],
    node_api_urls: Mapping[str, str],
    smoke_id: str,
    conditional_plan_sha256: str,
    phase: str,
    owner: str = "pathfinder",
    selected_worker_id: str | None = None,
    worker_alias: str | None = None,
    api_task_timeout_seconds: int,
) -> dict[str, Any]:
    """Build one exact phase graph from a previously resolved trial plan.

    Phase-B dependencies must already have had phase-A predecessors removed by
    the frozen resolver.  This builder never accepts arbitrary condition
    choices, and a template is deliberately non-submittable when no worker ID
    is supplied.
    """

    _require(phase in {"A", "B"}, "conditional phase must be A or B")
    copied = [_validate_operation(row) for row in operations]
    _require(bool(copied), f"conditional phase {phase} cannot be empty")
    keys = [row["operation_key"] for row in copied]
    _require(len(keys) == len(set(keys)), f"conditional phase {phase} operation keys repeat")
    for row in copied:
        values = dependencies.get(row["operation_key"])
        _require(
            isinstance(values, Sequence)
            and not isinstance(values, (str, bytes))
            and all(isinstance(value, str) and value for value in values),
            f"conditional phase {phase} dependencies are invalid",
        )
        _require(
            all(value in keys for value in values),
            f"conditional phase {phase} contains a cross-phase dependency",
        )
    if phase == "A":
        _require(
            all(row.get("condition") is None for row in copied),
            "conditional phase A contains a branch operation",
        )
        _require(
            any(row["operation_kind"] == "cache_lookup" for row in copied),
            "conditional phase A has no cache lookup",
        )
    urls = _validate_node_api_urls(copied, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    _require_api_timeout_covers(derive_operation_lower_bounds(copied), timeout)
    names = _phase_node_names(copied, phase)
    nodes: list[dict[str, Any]] = []
    for row in copied:
        key = row["operation_key"]
        node: dict[str, Any] = {
            "name": names[key],
            "spec": _task_spec(
                row,
                urls[row["execution_node_id"]],
                timeout,
            ),
        }
        if dependencies[key]:
            node["dependsOn"] = [names[value] for value in dependencies[key]]
        nodes.append(node)
    custom: dict[str, Any] = {
        "pathfinder_conditional_plan_sha256": _text(
            conditional_plan_sha256, "conditional_plan_sha256"
        ),
        "pathfinder_conditional_phase": phase,
        "pathfinder_smoke_id": _text(smoke_id, "smoke_id"),
        "pathfinder_operation_keys": keys,
        "pathfinder_semantic_quality_evaluated": False,
    }
    annotations: dict[str, Any] = {"custom": custom}
    if selected_worker_id is None:
        custom.update(
            {
                "pathfinder_worker_alias_to_resolve_at_submission": _text(
                    worker_alias, "worker_alias"
                ),
                "pathfinder_workflow_is_not_submittable": True,
            }
        )
    else:
        annotations["schedule_hint"] = {
            "selected_worker": _text(selected_worker_id, "selected_worker_id")
        }
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": f"pathfinder-conditional-{phase.lower()}-{_text(smoke_id, 'smoke_id')[:27]}",
            "owner": _text(owner, "owner"),
            "annotations": annotations,
        },
        "spec": {"graph": {"nodes": nodes}},
    }


def _matrix_trial_bundle(
    matrix_plan_dir: str | Path,
    trial_key: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    root, matrix, trials, operations, admission = _read_matrix_plan(matrix_plan_dir)
    _verify_matrix_plan_contents(root, matrix, trials, operations, admission)
    requested = _text(trial_key, "trial_key")
    trial_by_key = {str(row.get("trial_key")): row for row in trials}
    _require(requested in trial_by_key, "conditional trial is absent from the frozen matrix")
    trial = _copy(trial_by_key[requested], "matrix trial")
    _require(
        trial.get("design_id") in {"D3", "D7"},
        "conditional runner supports only D3/D7 cache designs",
    )
    selected = [
        _validate_operation(row)
        for row in operations
        if row.get("trial_key") == requested
    ]
    _require(selected, "conditional trial has no frozen operations")
    resolution = resolve_conditional_container_trial(
        selected,
        trial_key=requested,
    )
    return _copy(matrix, "matrix plan"), trial, selected, resolution


def _cache_lane(
    operations: Sequence[Mapping[str, Any]],
    trial: Mapping[str, Any],
) -> dict[str, Any]:
    lookups = [row for row in operations if row["operation_kind"] == "cache_lookup"]
    _require(lookups, "conditional trial has no cache lookup")
    cache_ids: set[str] = set()
    scope_ids: set[str] = set()
    for row in lookups:
        cache = row.get("cache_adapter")
        _require(isinstance(cache, Mapping), "cache lookup has no cache adapter")
        cache_ids.add(_text(cache.get("cache_id"), "cache lookup cache_id"))
        scope_ids.add(_text(row.get("cache_scope_id"), "cache lookup cache_scope_id"))
    _require(len(cache_ids) == 1, "conditional trial uses more than one cache lane")
    _require(len(scope_ids) == 1, "conditional trial uses more than one cache scope")
    repetition = trial.get("repetition")
    _require(type(repetition) is int and repetition >= 0, "conditional trial repetition is invalid")
    return {
        "design_id": _text(trial.get("design_id"), "conditional trial design_id"),
        "repetition": repetition,
        "cache_id": next(iter(cache_ids)),
        "cache_scope_id": next(iter(scope_ids)),
        "same_cache_lane_serial_execution_required": True,
    }


def _template_from_plan(plan: Mapping[str, Any], phase: str) -> dict[str, Any]:
    operations = _phase_operations(plan, phase)
    return build_flowmesh_container_conditional_phase_workflow(
        operations,
        dependencies=_phase_dependencies(plan, phase, operations),
        node_api_urls=plan["node_api_urls"],
        smoke_id=plan["smoke_id"],
        conditional_plan_sha256=plan["plan_sha256"],
        phase=phase,
        owner=plan["owner"],
        worker_alias=plan["worker_alias"],
        api_task_timeout_seconds=plan["api_task_timeout_seconds"],
    )


def plan_flowmesh_container_conditional_trial(
    *,
    matrix_plan_dir: str | Path,
    trial_key: str,
    smoke_id: str,
    output_dir: str | Path,
    owner: str = "pathfinder",
) -> dict[str, Any]:
    """Freeze one D3/D7 cache-conditional trial before any submission."""

    matrix, trial, operations, resolution = _matrix_trial_bundle(
        matrix_plan_dir,
        trial_key,
    )
    run_name = _text(smoke_id, "smoke_id")
    owner_name = _text(owner, "owner")
    plan: dict[str, Any] = {
        "schema_version": FLOWMESH_CONTAINER_CONDITIONAL_PLAN_SCHEMA_VERSION,
        "status": "FROZEN",
        "smoke_id": run_name,
        "owner": owner_name,
        "matrix_id": matrix.get("matrix_id"),
        "matrix_plan_sha256": matrix.get("plan_sha256"),
        "matrix_operations_sha256": matrix.get("matrix_operations_sha256"),
        "topology_node_ids": matrix.get("topology_node_ids"),
        "trial": trial,
        "trial_key": trial["trial_key"],
        "worker_alias": matrix.get("worker_alias"),
        "node_api_urls": matrix.get("node_api_urls"),
        "api_task_timeout_seconds": matrix.get("api_task_timeout_seconds"),
        "operations": operations,
        "resolution": resolution,
        "cache_lane": _cache_lane(operations, trial),
        "evidence_class": "orchestration-conditional-cache-branch-conformance",
        "llm_called": False,
        "semantic_task_quality_evaluated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    _text(plan["matrix_plan_sha256"], "matrix_plan_sha256")
    _text(plan["matrix_operations_sha256"], "matrix_operations_sha256")
    _text(plan["worker_alias"], "worker_alias")
    _require(isinstance(plan["node_api_urls"], Mapping), "matrix plan has no node API URLs")
    _validate_node_api_urls(operations, plan["node_api_urls"])
    timeout = _validate_api_task_timeout(plan["api_task_timeout_seconds"])
    _require_api_timeout_covers(derive_operation_lower_bounds(operations), timeout)
    topology = plan["topology_node_ids"]
    _require(
        isinstance(topology, list)
        and len(topology) == 8
        and all(isinstance(node, str) and node for node in topology)
        and len(set(topology)) == 8
        and set(plan["node_api_urls"]) == set(topology),
        "conditional plan node API URLs do not cover the frozen topology",
    )
    plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
    templates = {
        "flowmesh-container-conditional-phase-a-template.json": _json_bytes(
            _template_from_plan(plan, "A")
        ),
        "flowmesh-container-conditional-phase-b-template.json": _json_bytes(
            _template_from_plan(plan, "B")
        ),
    }
    documents = {
        "flowmesh-container-conditional-plan.json": _json_bytes(plan),
        **templates,
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    verified = verify_flowmesh_container_conditional_trial_plan(target)
    return {
        "status": "FROZEN_CONDITIONAL_TRIAL",
        "output_dir": str(target),
        "smoke_id": run_name,
        "trial_key": plan["trial_key"],
        "cache_lane": plan["cache_lane"],
        "expected_cache_outcomes": plan["resolution"]["cache_outcomes"],
        "phase_a_operation_count": len(plan["resolution"]["phase_a_operation_keys"]),
        "phase_b_operation_count": len(plan["resolution"]["phase_b_operation_keys"]),
        "plan_sha256": plan["plan_sha256"],
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
        "verification_status": verified["status"],
    }


def _read_conditional_plan(plan_dir: str | Path) -> dict[str, Any]:
    root = Path(plan_dir).resolve()
    _require(root.is_dir(), f"conditional trial plan directory does not exist: {root}")
    _check_checksums(root, _PLAN_FILES, "conditional trial plan")
    plan = _read_json(root / "flowmesh-container-conditional-plan.json", "conditional trial plan")
    _require(
        plan.get("schema_version") == FLOWMESH_CONTAINER_CONDITIONAL_PLAN_SCHEMA_VERSION,
        "unsupported conditional trial plan schema",
    )
    _require(plan.get("status") == "FROZEN", "conditional trial plan is not frozen")
    _require(
        plan.get("plan_sha256") == _document_sha256(plan, "plan_sha256"),
        "conditional trial plan digest mismatch",
    )
    _text(plan.get("trial_key"), "conditional trial_key")
    _text(plan.get("worker_alias"), "conditional worker_alias")
    _text(plan.get("matrix_plan_sha256"), "matrix_plan_sha256")
    _text(plan.get("matrix_operations_sha256"), "matrix_operations_sha256")
    _validate_api_task_timeout(plan.get("api_task_timeout_seconds"))
    trial = plan.get("trial")
    _require(isinstance(trial, Mapping), "conditional plan trial is missing")
    _require(trial.get("trial_key") == plan["trial_key"], "conditional plan trial key changed")
    _require(trial.get("design_id") in {"D3", "D7"}, "conditional plan design is invalid")
    rows = plan.get("operations")
    _require(isinstance(rows, list), "conditional plan operations are missing")
    selected = [_validate_operation(row) for row in rows]
    _require(bool(selected), "conditional plan operation set is empty")
    _require(
        all(row["trial_key"] == plan["trial_key"] for row in selected),
        "conditional plan operations cross a trial boundary",
    )
    recomputed = resolve_conditional_container_trial(
        selected,
        trial_key=plan["trial_key"],
    )
    _require(
        plan.get("resolution") == recomputed,
        "conditional plan resolution does not match frozen operations",
    )
    _require(
        plan.get("cache_lane") == _cache_lane(selected, trial),
        "conditional plan cache lane changed",
    )
    urls = plan.get("node_api_urls")
    _require(isinstance(urls, Mapping), "conditional plan node API URLs are missing")
    _validate_node_api_urls(selected, urls)
    topology = plan.get("topology_node_ids")
    _require(
        isinstance(topology, list)
        and len(topology) == 8
        and all(isinstance(node, str) and node for node in topology)
        and len(set(topology)) == 8
        and set(urls) == set(topology),
        "conditional plan node API URLs do not cover the frozen topology",
    )
    _require_api_timeout_covers(
        derive_operation_lower_bounds(selected),
        _validate_api_task_timeout(plan["api_task_timeout_seconds"]),
    )
    for phase, filename in (
        ("A", "flowmesh-container-conditional-phase-a-template.json"),
        ("B", "flowmesh-container-conditional-phase-b-template.json"),
    ):
        observed = _read_json(root / filename, f"conditional phase {phase} template")
        _require(
            observed == _template_from_plan(plan, phase),
            f"conditional phase {phase} template does not match its frozen plan",
        )
    return plan


def verify_flowmesh_container_conditional_trial_plan(
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Verify a frozen conditional trial without contacting FlowMesh."""

    plan = _read_conditional_plan(plan_dir)
    resolution = plan["resolution"]
    assert isinstance(resolution, Mapping)
    return {
        "status": "VERIFIED",
        "schema_version": plan["schema_version"],
        "smoke_id": plan["smoke_id"],
        "trial_key": plan["trial_key"],
        "matrix_plan_sha256": plan["matrix_plan_sha256"],
        "plan_sha256": plan["plan_sha256"],
        "cache_lane": plan["cache_lane"],
        "expected_cache_outcomes": resolution["cache_outcomes"],
        "phase_a_operation_count": len(resolution["phase_a_operation_keys"]),
        "phase_b_operation_count": len(resolution["phase_b_operation_keys"]),
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
    }


def _submission_workflow(
    plan: Mapping[str, Any],
    phase: str,
    identity: FlowMeshWorkerIdentity,
) -> dict[str, Any]:
    operations = _phase_operations(plan, phase)
    return build_flowmesh_container_conditional_phase_workflow(
        operations,
        dependencies=_phase_dependencies(plan, phase, operations),
        node_api_urls=plan["node_api_urls"],
        smoke_id=plan["smoke_id"],
        conditional_plan_sha256=plan["plan_sha256"],
        phase=phase,
        owner=plan["owner"],
        selected_worker_id=identity.worker_id,
        api_task_timeout_seconds=plan["api_task_timeout_seconds"],
    )


def _phase_task_record(
    raw: Mapping[str, Any],
    operation: Mapping[str, Any],
    *,
    phase: str,
    task_id: str,
    selected_worker_id: str,
    task_detail: Mapping[str, Any] | None,
    expected_runtime_epochs: Mapping[str, str],
) -> dict[str, Any]:
    """Validate only the result facts needed for branch safety.

    Unlike the older three-step smoke telemetry artifact, this record does not
    publish a service-time aggregate: conditional orchestration is a control
    plane gate, not a latency measurement.  It retains an exact hash of the
    complete result and preserves only a lookup's hit/miss and scope needed to
    decide whether phase B may exist.
    """

    api = extract_api_executor_result(raw)
    try:
        body = json.loads(api["text"])
    except json.JSONDecodeError as exc:
        raise FlowMeshContainerDagError(
            f"FlowMesh phase {phase} task {task_id} returned non-JSON container output"
        ) from exc
    _require(isinstance(body, Mapping), "conditional container result must be an object")
    _require(
        body.get("schema_version") == CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "conditional container result does not support the v2 result contract",
    )
    _require(body.get("status") == "completed", "conditional container operation did not complete")
    _require(body.get("outcome_type") == "completed", "conditional container outcome is not completed")
    _require(body.get("telemetry_complete") is True, "conditional container telemetry is incomplete")
    _require(body.get("credentials_recorded") is False, "conditional container result recorded credentials")
    _require(body.get("idempotent_replay") is False, "conditional container operation was replayed")
    for field in ("operation_key", "operation_kind", "execution_node_id"):
        _require(
            body.get(field) == operation.get(field),
            f"conditional container result changed {field}",
        )
    _require(
        body.get("logical_bytes") == operation.get("logical_bytes"),
        "conditional container result changed logical byte count",
    )
    expected_physical = (
        operation["logical_bytes"]
        if operation["operation_kind"] in _IO_OPERATION_KINDS
        else 0
    )
    _require(
        body.get("physical_bytes") == expected_physical,
        "conditional container result physical byte count is invalid",
    )
    source = _text(operation.get("execution_node_id"), "execution_node_id")
    _require(
        source in expected_runtime_epochs,
        "conditional runtime binding lacks the execution node",
    )
    runtime_epoch = _runtime_epoch(
        body.get("runtime_epoch"), "conditional container runtime_epoch"
    )
    _require(
        runtime_epoch == expected_runtime_epochs[source],
        "conditional container result runtime epoch does not match the "
        "pre-submit health binding",
    )
    destination_runtime_epoch = body.get("destination_runtime_epoch")
    if operation["operation_kind"] == "network_transfer":
        destination = _text(
            operation.get("destination_node_id"), "destination_node_id"
        )
        _require(
            destination in expected_runtime_epochs,
            "conditional runtime binding lacks the network destination node",
        )
        destination_runtime_epoch = _runtime_epoch(
            destination_runtime_epoch,
            "conditional container destination_runtime_epoch",
        )
        _require(
            destination_runtime_epoch == expected_runtime_epochs[destination],
            "conditional network sink runtime epoch does not match the "
            "pre-submit health binding",
        )
    else:
        _require(
            destination_runtime_epoch is None,
            "non-network conditional result names a destination runtime epoch",
        )
    if task_detail is not None:
        _require(
            task_detail.get("assigned_worker") == selected_worker_id,
            "FlowMesh assigned a conditional task to a worker other than the pin",
        )

    cache_outcome: str | None = None
    cache_scope_id: str | None = None
    if operation["operation_kind"] == "cache_lookup":
        cache_outcome = body.get("cache_result")
        _require(
            cache_outcome in {"hit", "miss"},
            "cache lookup did not return the literal hit or miss outcome",
        )
        cache_scope_id = _text(body.get("cache_scope_id"), "cache lookup result cache_scope_id")
        _require(
            cache_scope_id == operation.get("cache_scope_id"),
            "cache lookup result changed cache scope",
        )
    return {
        "phase": phase,
        "task_id": task_id,
        "worker_id": selected_worker_id,
        "operation_key": operation["operation_key"],
        "operation_kind": operation["operation_kind"],
        "execution_node_id": operation["execution_node_id"],
        "destination_node_id": operation["destination_node_id"],
        "runtime_epoch": runtime_epoch,
        "destination_runtime_epoch": destination_runtime_epoch,
        "container_result_schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "logical_bytes": operation["logical_bytes"],
        "physical_bytes": expected_physical,
        "cache_outcome": cache_outcome,
        "cache_scope_id": cache_scope_id,
        "telemetry_complete": True,
        "api_executor": api["executor"],
        "api_http_status": api["status_code"],
        "container_result_sha256": _sha256_bytes(_canonical_bytes(body)),
        "task_detail_available": task_detail is not None,
    }


def _collect_phase_results(
    client: FlowMeshClientProtocol,
    submitted: SubmittedWorkflow,
    *,
    plan: Mapping[str, Any],
    phase: str,
    selected_worker_id: str,
    expected_runtime_epochs: Mapping[str, str],
) -> list[dict[str, Any]]:
    operations = _phase_operations(plan, phase)
    expected = {row["operation_key"]: row for row in operations}
    _require(
        len(submitted.task_ids) == len(expected),
        f"FlowMesh returned a task count other than the frozen conditional phase {phase} nodes",
    )
    records: list[dict[str, Any]] = []
    for task_id in submitted.task_ids:
        raw = client.retrieve_result(task_id)
        api = extract_api_executor_result(raw)
        try:
            body = json.loads(api["text"])
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerDagError(
                f"FlowMesh phase {phase} task {task_id} returned non-JSON container output"
            ) from exc
        _require(isinstance(body, Mapping), "conditional container result must be an object")
        key = body.get("operation_key")
        _require(
            isinstance(key, str) and key in expected,
            f"FlowMesh phase {phase} returned an operation outside the frozen plan",
        )
        try:
            detail = client.describe_task_failure(task_id)
        except Exception as exc:
            raise FlowMeshContainerDagError(
                "cannot verify the FlowMesh worker assigned to a completed "
                f"conditional phase {phase} task: {redact_secrets(str(exc))}"
            ) from exc
        _require(
            isinstance(detail, Mapping),
            "FlowMesh did not return task metadata needed to verify the worker pin",
        )
        records.append(
            _phase_task_record(
                raw,
                expected[key],
                phase=phase,
                task_id=task_id,
                selected_worker_id=selected_worker_id,
                task_detail=detail,
                expected_runtime_epochs=expected_runtime_epochs,
            )
        )
    _require(
        {row["operation_key"] for row in records} == set(expected),
        f"FlowMesh phase {phase} results do not cover the frozen operations",
    )
    _require(
        len(records) == len(expected),
        f"FlowMesh phase {phase} returned duplicate operation results",
    )
    by_key = {row["operation_key"]: row for row in records}
    return [by_key[row["operation_key"]] for row in operations]


def _observed_cache_outcomes(
    records: Sequence[Mapping[str, Any]],
    *,
    expected: Mapping[str, Any],
) -> dict[str, str]:
    observed: dict[str, str] = {}
    for row in records:
        if row.get("operation_kind") != "cache_lookup":
            continue
        key = _text(row.get("operation_key"), "cache lookup result operation_key")
        value = row.get("cache_outcome")
        _require(value in {"hit", "miss"}, "cache lookup record outcome is invalid")
        observed[key] = str(value)
    _require(
        set(observed) == set(expected),
        "phase A cache lookup results do not cover the frozen outcome vector",
    )
    return dict(sorted(observed.items()))


def _submission_record(
    submitted: SubmittedWorkflow,
    workflow: Mapping[str, Any],
    *,
    phase: str,
    selected_worker_id: str,
) -> dict[str, Any]:
    return {
        "phase": phase,
        "workflow_id": submitted.workflow_id,
        "task_ids": list(submitted.task_ids),
        "selected_worker_id": selected_worker_id,
        "workflow_sha256": _sha256_bytes(_canonical_bytes(workflow)),
        "validated_before_submission": True,
        "credentials_recorded": False,
    }


def _current_worker_is_unchanged(
    client: FlowMeshClientProtocol,
    identity: FlowMeshWorkerIdentity,
) -> None:
    """Refuse phase B if the Root no longer reports the phase-A worker."""

    try:
        current = client.describe_current_worker(worker_id=identity.worker_id)
    except Exception as exc:
        raise FlowMeshContainerDagError(
            "cannot verify the phase-A worker remains current before phase B: "
            + redact_secrets(str(exc))
        ) from exc
    _require(
        current.worker_id == identity.worker_id,
        "the pinned FlowMesh worker changed between conditional phases",
    )


def _run_summary(
    plan: Mapping[str, Any],
    identity: FlowMeshWorkerIdentity,
    *,
    phase_a_submission: Mapping[str, Any],
    phase_a_records: Sequence[Mapping[str, Any]],
    observed_outcomes: Mapping[str, str],
    runtime_epoch_binding: Mapping[str, Any],
    status: str,
    phase_b_submission: Mapping[str, Any] | None = None,
    phase_b_records: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    resolution = plan["resolution"]
    assert isinstance(resolution, Mapping)
    expected = resolution["cache_outcomes"]
    _require(isinstance(expected, Mapping), "conditional plan cache outcomes are missing")
    complete = status == "COMPLETE"
    return {
        "schema_version": FLOWMESH_CONTAINER_CONDITIONAL_RUN_SCHEMA_VERSION,
        "status": status,
        "smoke_id": plan["smoke_id"],
        "trial_key": plan["trial_key"],
        "plan_sha256": plan["plan_sha256"],
        "matrix_plan_sha256": plan["matrix_plan_sha256"],
        "selected_worker": identity.to_public_dict(),
        "cache_lane": plan["cache_lane"],
        "expected_cache_outcomes": dict(sorted(expected.items())),
        "observed_cache_outcomes": dict(sorted(observed_outcomes.items())),
        "cache_outcomes_match_frozen_plan": observed_outcomes == expected,
        "phase_a": {
            "workflow_id": phase_a_submission["workflow_id"],
            "task_ids": phase_a_submission["task_ids"],
            "task_result_count": len(phase_a_records),
            "operation_keys": [row["operation_key"] for row in phase_a_records],
        },
        "phase_b": {
            "submitted": complete,
            "workflow_id": (
                phase_b_submission["workflow_id"] if phase_b_submission else None
            ),
            "task_ids": phase_b_submission["task_ids"] if phase_b_submission else [],
            "task_result_count": len(phase_b_records or []),
            "operation_keys": [
                row["operation_key"] for row in (phase_b_records or [])
            ],
        },
        "phase_b_submission_refused": not complete,
        "runtime_epoch_binding": dict(runtime_epoch_binding),
        "evidence_class": "orchestration-conditional-cache-branch-conformance",
        "llm_called": False,
        "semantic_task_quality_evaluated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _write_run_artifact(
    output_dir: str | Path,
    *,
    summary: Mapping[str, Any],
    phase_a_submission: Mapping[str, Any],
    phase_a_records: Sequence[Mapping[str, Any]],
    phase_b_submission: Mapping[str, Any] | None = None,
    phase_b_records: Sequence[Mapping[str, Any]] | None = None,
) -> Path:
    documents: dict[str, bytes] = {
        "flowmesh-container-conditional-run.json": _json_bytes(summary),
        "flowmesh-container-conditional-phase-a-submission.json": _json_bytes(
            phase_a_submission
        ),
        "flowmesh-container-conditional-phase-a-task-results.jsonl": _jsonl_bytes(
            phase_a_records
        ),
    }
    if phase_b_submission is not None and phase_b_records is not None:
        documents["flowmesh-container-conditional-phase-b-submission.json"] = _json_bytes(
            phase_b_submission
        )
        documents["flowmesh-container-conditional-phase-b-task-results.jsonl"] = _jsonl_bytes(
            phase_b_records
        )
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return target


def run_flowmesh_container_conditional_trial(
    *,
    plan_dir: str | Path,
    output_dir: str | Path,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    runtime_epoch_probe: Callable[
        [Mapping[str, str], Sequence[Mapping[str, Any]]], Mapping[str, str]
    ] | None = None,
) -> dict[str, Any]:
    """Submit phase A, gate phase B on literal lookup agreement, and record it.

    A mismatch is not treated as a successful run.  It creates an auditable
    ``REFUSED_CACHE_OUTCOME_MISMATCH`` artifact containing only phase-A
    evidence, and does not contact FlowMesh again for phase B.
    """

    plan = _read_conditional_plan(plan_dir)
    _require(
        settings.worker_alias == plan["worker_alias"],
        "run worker alias must exactly match the frozen conditional plan",
    )
    identity = describe_pinned_worker(client, settings)
    phase_a_workflow = _submission_workflow(plan, "A", identity)
    validation = client.validate(phase_a_workflow)
    _require(
        validation.ok,
        "FlowMesh rejected conditional phase A workflow: "
        + "; ".join(validation.errors),
    )
    # Bind every node which either cache branch could touch before phase A.
    # The phase-B decision is meaningful only while that complete physical
    # environment remains the same; probing merely the initially selected
    # phase-A nodes would miss a restart of a prospective branch endpoint.
    probe = runtime_epoch_probe or _probe_container_runtime_epochs
    runtime_epochs_before = _validate_runtime_epochs(
        probe(plan["node_api_urls"], plan["operations"]),
        plan["operations"],
    )
    submitted_a = client.submit(phase_a_workflow)
    terminal_a = client.wait(submitted_a.workflow_id, settings.poll_interval_seconds)
    if terminal_a.status != "DONE":
        raise _workflow_failure(terminal_a, submitted_a, client)
    records_a = _collect_phase_results(
        client,
        submitted_a,
        plan=plan,
        phase="A",
        selected_worker_id=identity.worker_id,
        expected_runtime_epochs=runtime_epochs_before,
    )
    submission_a = _submission_record(
        submitted_a,
        phase_a_workflow,
        phase="A",
        selected_worker_id=identity.worker_id,
    )
    resolution = plan["resolution"]
    assert isinstance(resolution, Mapping)
    expected = resolution["cache_outcomes"]
    _require(isinstance(expected, Mapping), "conditional plan cache outcomes are missing")
    observed = _observed_cache_outcomes(records_a, expected=expected)
    if observed != expected:
        runtime_epochs_after = _validate_runtime_epochs(
            probe(plan["node_api_urls"], plan["operations"]),
            plan["operations"],
        )
        runtime_binding = _runtime_epoch_binding(
            plan_sha256=plan["plan_sha256"],
            node_api_urls=plan["node_api_urls"],
            operations=plan["operations"],
            before=runtime_epochs_before,
            after=runtime_epochs_after,
        )
        summary = _run_summary(
            plan,
            identity,
            phase_a_submission=submission_a,
            phase_a_records=records_a,
            observed_outcomes=observed,
            runtime_epoch_binding=runtime_binding,
            status="REFUSED_CACHE_OUTCOME_MISMATCH",
        )
        target = _write_run_artifact(
            output_dir,
            summary=summary,
            phase_a_submission=submission_a,
            phase_a_records=records_a,
        )
        return {**summary, "output_dir": str(target)}

    _current_worker_is_unchanged(client, identity)
    phase_b_workflow = _submission_workflow(plan, "B", identity)
    validation = client.validate(phase_b_workflow)
    _require(
        validation.ok,
        "FlowMesh rejected conditional phase B workflow: "
        + "; ".join(validation.errors),
    )
    submitted_b = client.submit(phase_b_workflow)
    terminal_b = client.wait(submitted_b.workflow_id, settings.poll_interval_seconds)
    if terminal_b.status != "DONE":
        raise _workflow_failure(terminal_b, submitted_b, client)
    records_b = _collect_phase_results(
        client,
        submitted_b,
        plan=plan,
        phase="B",
        selected_worker_id=identity.worker_id,
        expected_runtime_epochs=runtime_epochs_before,
    )
    submission_b = _submission_record(
        submitted_b,
        phase_b_workflow,
        phase="B",
        selected_worker_id=identity.worker_id,
    )
    runtime_epochs_after = _validate_runtime_epochs(
        probe(plan["node_api_urls"], plan["operations"]),
        plan["operations"],
    )
    runtime_binding = _runtime_epoch_binding(
        plan_sha256=plan["plan_sha256"],
        node_api_urls=plan["node_api_urls"],
        operations=plan["operations"],
        before=runtime_epochs_before,
        after=runtime_epochs_after,
    )
    summary = _run_summary(
        plan,
        identity,
        phase_a_submission=submission_a,
        phase_a_records=records_a,
        observed_outcomes=observed,
        runtime_epoch_binding=runtime_binding,
        status="COMPLETE",
        phase_b_submission=submission_b,
        phase_b_records=records_b,
    )
    target = _write_run_artifact(
        output_dir,
        summary=summary,
        phase_a_submission=submission_a,
        phase_a_records=records_a,
        phase_b_submission=submission_b,
        phase_b_records=records_b,
    )
    return {**summary, "output_dir": str(target)}


def _read_conditional_run(
    run_dir: str | Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, Any] | None,
    list[dict[str, Any]] | None,
]:
    root = Path(run_dir).resolve()
    _require(root.is_dir(), f"conditional trial run directory does not exist: {root}")
    summary = _read_json(root / "flowmesh-container-conditional-run.json", "conditional trial run")
    status = summary.get("status")
    _require(
        status in {"COMPLETE", "REFUSED_CACHE_OUTCOME_MISMATCH"},
        "conditional trial run has an unsupported status",
    )
    expected = set(_RUN_COMMON_FILES)
    if status == "COMPLETE":
        expected |= _RUN_PHASE_B_FILES
    _check_checksums(root, expected, "conditional trial run")
    phase_a_submission = _read_json(
        root / "flowmesh-container-conditional-phase-a-submission.json",
        "conditional phase A submission",
    )
    phase_a_rows = _read_jsonl(
        root / "flowmesh-container-conditional-phase-a-task-results.jsonl",
        "conditional phase A results",
    )
    if status != "COMPLETE":
        return summary, phase_a_submission, phase_a_rows, None, None
    phase_b_submission = _read_json(
        root / "flowmesh-container-conditional-phase-b-submission.json",
        "conditional phase B submission",
    )
    phase_b_rows = _read_jsonl(
        root / "flowmesh-container-conditional-phase-b-task-results.jsonl",
        "conditional phase B results",
    )
    return (
        summary,
        phase_a_submission,
        phase_a_rows,
        phase_b_submission,
        phase_b_rows,
    )


def _verify_phase_records(
    records: Sequence[Mapping[str, Any]],
    *,
    operations: Sequence[Mapping[str, Any]],
    phase: str,
    selected_worker_id: str,
) -> None:
    _require(
        len(records) == len(operations),
        f"conditional phase {phase} result count is wrong",
    )
    expected_keys = [str(row["operation_key"]) for row in operations]
    _require(
        [row.get("operation_key") for row in records] == expected_keys,
        f"conditional phase {phase} result order or coverage changed",
    )
    seen_tasks: set[str] = set()
    for row, operation in zip(records, operations):
        _require(row.get("phase") == phase, f"conditional phase {phase} record phase changed")
        task_id = _text(row.get("task_id"), "conditional task_id")
        _require(task_id not in seen_tasks, "conditional phase task IDs are duplicated")
        seen_tasks.add(task_id)
        _require(row.get("worker_id") == selected_worker_id, "conditional task worker changed")
        for field in (
            "operation_key",
            "operation_kind",
            "execution_node_id",
            "destination_node_id",
            "logical_bytes",
        ):
            _require(
                row.get(field) == operation.get(field),
                f"conditional phase {phase} record changed {field}",
            )
        expected_physical = (
            operation["logical_bytes"]
            if operation["operation_kind"] in _IO_OPERATION_KINDS
            else 0
        )
        _require(
            row.get("physical_bytes") == expected_physical,
            f"conditional phase {phase} physical bytes changed",
        )
        _require(row.get("telemetry_complete") is True, "conditional telemetry is incomplete")
        _require(row.get("api_executor") == "api", "conditional task did not use API executor")
        _require(
            row.get("container_result_schema_version")
            == CONTAINER_NODE_RESULT_SCHEMA_VERSION,
            "conditional task result does not support the v2 result contract",
        )
        _runtime_epoch(row.get("runtime_epoch"), "conditional task runtime_epoch")
        if operation["operation_kind"] == "network_transfer":
            _runtime_epoch(
                row.get("destination_runtime_epoch"),
                "conditional task destination_runtime_epoch",
            )
        else:
            _require(
                row.get("destination_runtime_epoch") is None,
                "non-network conditional task names a destination runtime epoch",
            )
        _require(
            row.get("task_detail_available") is True,
            "conditional task has no worker-assignment evidence",
        )
        status = row.get("api_http_status")
        _require(type(status) is int and 200 <= status < 300, "conditional task HTTP status is invalid")
        digest = row.get("container_result_sha256")
        _require(
            isinstance(digest, str)
            and len(digest) == 64
            and all(character in "0123456789abcdef" for character in digest),
            "conditional task result digest is invalid",
        )
        if operation["operation_kind"] == "cache_lookup":
            _require(row.get("cache_outcome") in {"hit", "miss"}, "conditional lookup outcome is invalid")
            _require(
                row.get("cache_scope_id") == operation.get("cache_scope_id"),
                "conditional lookup cache scope changed",
            )
        else:
            _require(row.get("cache_outcome") is None, "non-lookup operation has a cache outcome")
            _require(row.get("cache_scope_id") is None, "non-lookup operation has a cache scope")


def _verify_submission(
    submission: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    phase: str,
    selected_worker_id: str,
) -> None:
    _require(submission.get("phase") == phase, f"conditional phase {phase} submission changed")
    _text(submission.get("workflow_id"), f"conditional phase {phase} workflow_id")
    task_ids = submission.get("task_ids")
    _require(
        isinstance(task_ids, list)
        and all(isinstance(item, str) and item for item in task_ids),
        f"conditional phase {phase} submission task IDs are invalid",
    )
    _require(
        task_ids == [row.get("task_id") for row in records],
        f"conditional phase {phase} submission task IDs do not match results",
    )
    _require(
        submission.get("selected_worker_id") == selected_worker_id,
        f"conditional phase {phase} submission worker changed",
    )
    _require(submission.get("validated_before_submission") is True, "conditional submission was not validated")
    _require(submission.get("credentials_recorded") is False, "conditional submission recorded credentials")
    digest = submission.get("workflow_sha256")
    _require(
        isinstance(digest, str)
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest),
        "conditional workflow digest is invalid",
    )


def verify_flowmesh_container_conditional_trial_run(
    run_dir: str | Path,
    *,
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Offline-verify a complete or deliberately refused conditional run."""

    plan = _read_conditional_plan(plan_dir)
    summary, submission_a, records_a, submission_b, records_b = _read_conditional_run(
        run_dir
    )
    _require(
        summary.get("schema_version") == FLOWMESH_CONTAINER_CONDITIONAL_RUN_SCHEMA_VERSION,
        "unsupported conditional trial run schema",
    )
    for field in ("smoke_id", "trial_key", "plan_sha256", "matrix_plan_sha256", "cache_lane"):
        _require(
            summary.get(field) == plan.get(field),
            f"conditional run is not bound to the supplied plan: {field}",
        )
    worker = summary.get("selected_worker")
    _require(isinstance(worker, Mapping), "conditional run selected worker is missing")
    worker_id = _text(worker.get("worker_id"), "conditional selected worker_id")
    _require(
        worker.get("alias") == plan["worker_alias"],
        "conditional run worker alias changed",
    )
    phase_a_ops = _phase_operations(plan, "A")
    _verify_phase_records(
        records_a,
        operations=phase_a_ops,
        phase="A",
        selected_worker_id=worker_id,
    )
    _verify_submission(
        submission_a,
        records_a,
        phase="A",
        selected_worker_id=worker_id,
    )
    resolution = plan["resolution"]
    assert isinstance(resolution, Mapping)
    expected = resolution["cache_outcomes"]
    _require(isinstance(expected, Mapping), "conditional plan outcomes are missing")
    observed = _observed_cache_outcomes(records_a, expected=expected)
    _require(
        summary.get("expected_cache_outcomes") == dict(sorted(expected.items())),
        "conditional run expected cache outcomes changed",
    )
    _require(
        summary.get("observed_cache_outcomes") == observed,
        "conditional run observed cache outcomes changed",
    )
    _require(
        summary.get("cache_outcomes_match_frozen_plan") == (observed == expected),
        "conditional run cache-outcome match flag changed",
    )
    status = summary["status"]
    phase_b = summary.get("phase_b")
    _require(isinstance(phase_b, Mapping), "conditional run phase B summary is missing")
    if status == "REFUSED_CACHE_OUTCOME_MISMATCH":
        _require(observed != expected, "conditional refusal has matching cache outcomes")
        _require(summary.get("phase_b_submission_refused") is True, "conditional refusal did not record phase-B refusal")
        _require(phase_b.get("submitted") is False, "conditional refusal records phase-B submission")
        _require(phase_b.get("workflow_id") is None, "conditional refusal has a phase-B workflow")
        _require(phase_b.get("task_ids") == [], "conditional refusal has phase-B tasks")
        _require(phase_b.get("task_result_count") == 0, "conditional refusal has phase-B results")
        _require(phase_b.get("operation_keys") == [], "conditional refusal has phase-B operation keys")
        _require(submission_b is None and records_b is None, "conditional refusal contains phase-B files")
    else:
        _require(observed == expected, "conditional complete run cache outcomes do not match its plan")
        _require(summary.get("phase_b_submission_refused") is False, "conditional complete run refused phase B")
        _require(phase_b.get("submitted") is True, "conditional complete run did not submit phase B")
        _require(submission_b is not None and records_b is not None, "conditional complete run phase-B files are missing")
        phase_b_ops = _phase_operations(plan, "B")
        _verify_phase_records(
            records_b,
            operations=phase_b_ops,
            phase="B",
            selected_worker_id=worker_id,
        )
        _verify_submission(
            submission_b,
            records_b,
            phase="B",
            selected_worker_id=worker_id,
        )
        _require(
            phase_b.get("workflow_id") == submission_b["workflow_id"],
            "conditional complete run phase-B workflow changed",
        )
        _require(
            phase_b.get("task_ids") == submission_b["task_ids"],
            "conditional complete run phase-B task IDs changed",
        )
        _require(
            phase_b.get("task_result_count") == len(records_b),
            "conditional complete run phase-B result count changed",
        )
        _require(
            phase_b.get("operation_keys") == [row["operation_key"] for row in records_b],
            "conditional complete run phase-B operation keys changed",
        )
    phase_a = summary.get("phase_a")
    _require(isinstance(phase_a, Mapping), "conditional run phase A summary is missing")
    _require(phase_a.get("workflow_id") == submission_a["workflow_id"], "conditional phase-A workflow changed")
    _require(phase_a.get("task_ids") == submission_a["task_ids"], "conditional phase-A task IDs changed")
    _require(phase_a.get("task_result_count") == len(records_a), "conditional phase-A result count changed")
    _require(
        phase_a.get("operation_keys") == [row["operation_key"] for row in records_a],
        "conditional phase-A operation keys changed",
    )
    _verify_runtime_epoch_binding(
        summary.get("runtime_epoch_binding"),
        plan_sha256=plan["plan_sha256"],
        node_api_urls=plan["node_api_urls"],
        operations=plan["operations"],
        rows=[*records_a, *(records_b or [])],
    )
    _require(summary.get("llm_called") is False, "conditional run records an LLM call")
    _require(
        summary.get("semantic_task_quality_evaluated") is False,
        "conditional run records semantic quality evaluation",
    )
    _require(summary.get("credentials_recorded") is False, "conditional run records credentials")
    _require(
        summary.get("eligible_for_scientific_claims") is False,
        "conditional run promotes scientific eligibility",
    )
    return {
        "status": "VERIFIED",
        "run_status": status,
        "smoke_id": summary["smoke_id"],
        "trial_key": summary["trial_key"],
        "plan_sha256": summary["plan_sha256"],
        "worker_id": worker_id,
        "phase_a_task_result_count": len(records_a),
        "phase_b_task_result_count": len(records_b or []),
        "phase_b_submitted": status == "COMPLETE",
        "cache_outcomes_match_frozen_plan": observed == expected,
        "eligible_for_scientific_claims": False,
    }
