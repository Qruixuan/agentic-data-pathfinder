"""FlowMesh wrapper for the public candidate-wide W4 retrieval matrix.

The frozen route package contains hundreds of data-dependent operations.  A
FlowMesh API graph cannot safely splice one task response into the next task
body, so this adapter uses the same deployment shape as the semantic matrix:
one worker-pinned API task calls an N7 or N8 route coordinator for each W4
trial.  The coordinator performs the N2/N3/N4/cache/N6 handoffs internally.

The plan is endpoint-free.  N7/N8 URLs and request authentication exist only
while rendering the submitted workflow.  Durable output contains verified
public rankings, content identities, component evidence, and FlowMesh task
bindings; it contains no hidden relevance labels, endpoint values, secrets,
artifact payloads, or model reasoning.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from ...simulator.full_flow_w4_candidate_coordinator import (
    OBSERVATIONS_NAME,
    OPERATION_EVIDENCE_NAME,
    RUN_NAME as COORDINATOR_RUN_NAME,
    TRIAL_RESULTS_NAME,
    publish_full_flow_w4_candidate_coordinator_results,
    verify_full_flow_w4_candidate_coordinator_run,
)
from ...simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    full_flow_request_hmac_sha256,
)
from ...simulator.full_flow_w4_candidate_routes import (
    load_full_flow_w4_candidate_route_inputs,
)
from ...simulator.full_flow_w4_live_executor import (
    EVENTS_NAME as COMPONENT_EVENTS_NAME,
    RECEIPT_NAME as COMPONENT_RECEIPT_NAME,
    freeze_full_flow_w4_component_execution_receipt,
    verify_full_flow_w4_component_execution_receipt,
)
from .adapter import extract_api_executor_result
from .contracts import FlowMeshClientProtocol, FlowMeshSettings, TerminalWorkflow
from .preflight import describe_pinned_worker
from .redaction import redact_secrets


FLOWMESH_W4_REQUEST_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-candidate-trial-request/v1alpha1"
)
FLOWMESH_W4_RESPONSE_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-candidate-trial-response/v1alpha1"
)
FLOWMESH_W4_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-candidate-matrix-plan/v1alpha1"
)
FLOWMESH_W4_ENVELOPE_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-candidate-matrix-envelope/v1alpha1"
)
FLOWMESH_W4_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-candidate-matrix-run/v1alpha1"
)
FLOWMESH_W4_SUBMISSION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-candidate-matrix-submission/v1alpha1"
)
FLOWMESH_W4_TASK_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-candidate-matrix-task/v1alpha1"
)

W4_COORDINATOR_ENDPOINT_PATH = "/v1/full-flow/w4-retrieval/execute"
PLAN_NAME = "flowmesh-w4-candidate-matrix-plan.json"
REQUESTS_NAME = "flowmesh-w4-candidate-trial-requests.jsonl"
ENVELOPE_NAME = "flowmesh-w4-candidate-workflow-envelope.json"
RUN_NAME = "flowmesh-w4-candidate-matrix-run.json"
SUBMISSION_NAME = "flowmesh-w4-candidate-matrix-submission.json"
TASKS_NAME = "flowmesh-w4-candidate-matrix-tasks.jsonl"
RESPONSES_NAME = "flowmesh-w4-candidate-trial-responses.jsonl"
FLOWMESH_COMPONENT_EVENTS_NAME = "flowmesh-w4-component-events.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"
CANDIDATE_RUN_DIR_NAME = "candidate-run"
COMPONENT_RECEIPT_DIR_NAME = "component-receipt"

_PLAN_FILES = frozenset({PLAN_NAME, REQUESTS_NAME, ENVELOPE_NAME})
_RUN_FILES = frozenset({
    RUN_NAME,
    SUBMISSION_NAME,
    TASKS_NAME,
    RESPONSES_NAME,
    FLOWMESH_COMPONENT_EVENTS_NAME,
})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,255}\Z")
_PRIVATE_KEYS = frozenset({
    "api_key",
    "authorization",
    "bearer_token",
    "correct_answer",
    "correct_answer_id",
    "credential",
    "hidden_label",
    "password",
    "reasoning",
    "secret",
    "token",
})


class FlowMeshW4CandidateMatrixError(RuntimeError):
    """Raised when W4 cannot cross FlowMesh without weakening its binding."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FlowMeshW4CandidateMatrixError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FlowMeshW4CandidateMatrixError(
            "W4 FlowMesh value is not canonical JSON"
        ) from exc


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _strict_fields(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    _require(set(value) == expected, f"{name} fields changed")


def _assert_public(value: Any, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).casefold()
            safety_declaration = lowered in {
                "credentials_recorded",
                "endpoint_values_included",
                "hidden_relevance_values_included",
                "hidden_relevance_values_read",
            }
            if safety_declaration:
                _require(child is False, f"unsafe declaration at {path}.{key}")
                continue
            if lowered == "ready_for_n1_hidden_relevance_evaluation":
                _require(child is True, f"invalid declaration at {path}.{key}")
                continue
            _require(
                lowered not in _PRIVATE_KEYS
                and not any(part in lowered for part in (
                    "credential_value",
                    "hidden_relevance",
                    "model_reasoning",
                )),
                f"private field crossed W4 FlowMesh boundary at {path}.{key}",
            )
            _assert_public(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_public(child, f"{path}[{index}]")


def _strict_json_bytes(raw: bytes, name: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, child in pairs:
            _require(key not in result, f"{name} repeats key {key}")
            result[key] = child
        return result

    try:
        value = json.loads(
            raw,
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FlowMeshW4CandidateMatrixError(
                    f"{name} contains non-finite number {token}"
                )
            ),
        )
    except FlowMeshW4CandidateMatrixError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshW4CandidateMatrixError(f"{name} is invalid JSON") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _read_json(path: Path, name: str) -> tuple[bytes, dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise FlowMeshW4CandidateMatrixError(f"cannot read {name}") from exc
    return raw, _strict_json_bytes(raw, name)


def _read_jsonl(path: Path, name: str) -> tuple[bytes, list[dict[str, Any]]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        raw = path.read_bytes()
        lines = raw.splitlines()
    except OSError as exc:
        raise FlowMeshW4CandidateMatrixError(f"cannot read {name}") from exc
    _require(
        lines and all(line.strip() for line in lines),
        f"{name} is empty or sparse",
    )
    return raw, [
        _strict_json_bytes(line, f"{name} line {index}")
        for index, line in enumerate(lines, start=1)
    ]


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _publish_directory(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(
        not target.exists() and not target.is_symlink(),
        f"output exists: {target}",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".flowmesh-w4-", dir=target.parent))
    try:
        for name, content in documents.items():
            (temporary / name).write_bytes(content)
        (temporary / CHECKSUMS_NAME).write_bytes(_checksums(documents))
        os.replace(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _require_disjoint_output(target: Path, sources: Sequence[Path]) -> None:
    for source in sources:
        _require(target != source, "output directory overlaps a frozen source")
        for child, parent in ((target, source), (source, target)):
            try:
                child.relative_to(parent)
            except ValueError:
                continue
            raise FlowMeshW4CandidateMatrixError(
                "output directory overlaps a frozen source"
            )


def _verify_flat_directory(
    root: Path,
    files: frozenset[str],
    name: str,
) -> dict[str, bytes]:
    _require(root.is_dir() and not root.is_symlink(), f"{name} directory is missing")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries)
        and {path.name for path in entries} == files | {CHECKSUMS_NAME},
        f"{name} file set changed",
    )
    documents = {file_name: (root / file_name).read_bytes() for file_name in files}
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(documents),
        f"{name} checksums failed",
    )
    return documents


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    core = dict(value)
    supplied = core.pop(field, None)
    _digest(supplied, field)
    _require(supplied == _sha256(_canonical(core)), f"{field} changed")
    return str(supplied)


def _request_core(
    *,
    run_id: str,
    route_plan: Mapping[str, Any],
    public_task: Mapping[str, Any],
    trial: Mapping[str, Any],
) -> dict[str, Any]:
    run = _identifier(run_id, "run_id")
    trial_key = _identifier(trial.get("trial_key"), "trial_key")
    coordinator = trial.get("executor_node_id")
    _require(coordinator in {"N7", "N8"}, "W4 coordinator node must be N7 or N8")
    idempotency_key = _sha256(_canonical({
        "domain": "pathfinder.flowmesh-w4-trial-idempotency/v1",
        "run_id": run,
        "route_plan_sha256": route_plan["plan_sha256"],
        "trial_key": trial_key,
    }))
    return {
        "schema_version": FLOWMESH_W4_REQUEST_SCHEMA_VERSION,
        "run_id": run,
        "idempotency_key": idempotency_key,
        "physical_plan_id": route_plan["physical_plan_id"],
        "route_plan_sha256": route_plan["plan_sha256"],
        "retrieval_task_binding_sha256": public_task["task_binding_sha256"],
        "candidate_set_sha256": public_task["candidate_set_sha256"],
        "trial_key": trial_key,
        "order_index": trial["order_index"],
        "design_id": trial["design_id"],
        "repetition": trial["repetition"],
        "coordinator_node_id": coordinator,
        "hidden_relevance_values_included": False,
        "credentials_recorded": False,
    }


def build_flowmesh_w4_trial_request(
    *,
    run_id: str,
    route_plan: Mapping[str, Any],
    public_task: Mapping[str, Any],
    trial: Mapping[str, Any],
) -> dict[str, Any]:
    request = _request_core(
        run_id=run_id,
        route_plan=route_plan,
        public_task=public_task,
        trial=trial,
    )
    request["request_sha256"] = _sha256(_canonical(request))
    return request


def validate_flowmesh_w4_trial_request(
    value: Mapping[str, Any],
    *,
    route_package_dir: str | Path | None = None,
) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "W4 FlowMesh request is not an object")
    request = json.loads(_canonical(value).decode("utf-8"))
    _strict_fields(request, {
        "schema_version", "run_id", "idempotency_key", "physical_plan_id",
        "route_plan_sha256", "retrieval_task_binding_sha256",
        "candidate_set_sha256", "trial_key", "order_index", "design_id",
        "repetition", "coordinator_node_id",
        "hidden_relevance_values_included", "credentials_recorded",
        "request_sha256",
    }, "W4 FlowMesh request")
    _require(
        request["schema_version"] == FLOWMESH_W4_REQUEST_SCHEMA_VERSION
        and request["coordinator_node_id"] in {"N7", "N8"}
        and request["design_id"] in {f"D{index}" for index in range(8)}
        and type(request["repetition"]) is int
        and request["repetition"] in {0, 1}
        and type(request["order_index"]) is int
        and request["order_index"] >= 0
        and request["hidden_relevance_values_included"] is False
        and request["credentials_recorded"] is False,
        "W4 FlowMesh request contract changed",
    )
    _identifier(request["run_id"], "run_id")
    _identifier(request["physical_plan_id"], "physical_plan_id")
    _identifier(request["trial_key"], "trial_key")
    for name in (
        "idempotency_key", "route_plan_sha256",
        "retrieval_task_binding_sha256", "candidate_set_sha256",
    ):
        _digest(request[name], name)
    expected_idempotency = _sha256(_canonical({
        "domain": "pathfinder.flowmesh-w4-trial-idempotency/v1",
        "run_id": request["run_id"],
        "route_plan_sha256": request["route_plan_sha256"],
        "trial_key": request["trial_key"],
    }))
    _require(
        request["idempotency_key"] == expected_idempotency,
        "W4 request idempotency key changed",
    )
    supplied = request.pop("request_sha256")
    _require(supplied == _sha256(_canonical(request)), "W4 request digest changed")
    request["request_sha256"] = supplied
    _assert_public(request)
    if route_package_dir is not None:
        source = load_full_flow_w4_candidate_route_inputs(route_package_dir)
        by_key = {str(row["trial_key"]): row for row in source.trials}
        trial = by_key.get(request["trial_key"])
        _require(trial is not None, "W4 request trial is absent from route package")
        expected = build_flowmesh_w4_trial_request(
            run_id=request["run_id"],
            route_plan=source.plan,
            public_task=source.public_task,
            trial=trial,
        )
        _require(request == expected, "W4 request differs from frozen route trial")
    return request


def _compile_requests(
    route_package_dir: str | Path,
    run_id: str,
) -> list[dict[str, Any]]:
    source = load_full_flow_w4_candidate_route_inputs(route_package_dir)
    requests = [
        build_flowmesh_w4_trial_request(
            run_id=run_id,
            route_plan=source.plan,
            public_task=source.public_task,
            trial=trial,
        )
        for trial in sorted(source.trials, key=lambda row: int(row["order_index"]))
    ]
    _require(
        len(requests) == 16
        and {(row["design_id"], row["repetition"]) for row in requests}
        == {(f"D{index}", repetition) for index in range(8) for repetition in (0, 1)},
        "W4 FlowMesh request coverage changed",
    )
    return requests


def _workflow_envelope(
    requests: Sequence[Mapping[str, Any]],
    *,
    worker_alias: str,
    owner: str,
    api_task_timeout_seconds: int,
) -> dict[str, Any]:
    tasks = []
    previous: str | None = None
    for request in requests:
        name = f"w4-trial-{int(request['order_index']):04d}"
        row = {
            "task_name": name,
            "trial_key": request["trial_key"],
            "request_sha256": request["request_sha256"],
            "coordinator_service_contract_id": (
                f"{request['coordinator_node_id']}.w4-candidate-coordinator"
            ),
            "depends_on": [] if previous is None else [previous],
        }
        tasks.append(row)
        previous = name
    return {
        "schema_version": FLOWMESH_W4_ENVELOPE_SCHEMA_VERSION,
        "worker_alias_to_resolve_at_submission": worker_alias,
        "owner": owner,
        "api_task_timeout_seconds": api_task_timeout_seconds,
        "execution_order": "global-serial-frozen-order",
        "cache_semantics": "D3-and-D7-r0-miss-before-r1-hit",
        "task_count": len(tasks),
        "tasks": tasks,
        "coordinator_endpoints_bound_at_submission": True,
        "submittable": False,
        "credentials_recorded": False,
    }


def plan_flowmesh_w4_candidate_matrix(
    *,
    route_package_dir: str | Path,
    run_id: str,
    worker_alias: str,
    output_dir: str | Path,
    owner: str = "pathfinder",
    api_task_timeout_seconds: int = 900,
) -> dict[str, Any]:
    source_root = Path(route_package_dir).resolve()
    source = load_full_flow_w4_candidate_route_inputs(source_root)
    run = _identifier(run_id, "run_id")
    alias = _identifier(worker_alias, "worker_alias")
    owner_name = _identifier(owner, "owner")
    _require(
        type(api_task_timeout_seconds) is int and api_task_timeout_seconds > 0,
        "api_task_timeout_seconds must be a positive integer",
    )
    requests = _compile_requests(source_root, run)
    requests_bytes = _jsonl_bytes(requests)
    envelope = _workflow_envelope(
        requests,
        worker_alias=alias,
        owner=owner_name,
        api_task_timeout_seconds=api_task_timeout_seconds,
    )
    envelope_bytes = _json_bytes(envelope)
    plan: dict[str, Any] = {
        "schema_version": FLOWMESH_W4_PLAN_SCHEMA_VERSION,
        "status": "FROZEN_W4_FLOWMESH_MATRIX_PLAN",
        "run_id": run,
        "worker_alias": alias,
        "owner": owner_name,
        "api_task_timeout_seconds": api_task_timeout_seconds,
        "physical_plan_id": source.plan["physical_plan_id"],
        "route_plan_sha256": source.plan["plan_sha256"],
        "route_package_checksums_sha256": _sha256(
            (source_root / CHECKSUMS_NAME).read_bytes()
        ),
        "retrieval_task_binding_sha256": source.public_task[
            "task_binding_sha256"
        ],
        "candidate_set_sha256": source.public_task["candidate_set_sha256"],
        "trial_count": len(requests),
        "flowmesh_api_task_count": len(requests),
        "workflow_count": 1,
        "execution_order": "global-serial-frozen-order",
        "cache_semantics": "D3-and-D7-r0-miss-before-r1-hit",
        "coordinator_service_contract_ids": [
            "N7.w4-candidate-coordinator",
            "N8.w4-candidate-coordinator",
        ],
        "requests_sha256": _sha256(requests_bytes),
        "workflow_envelope_sha256": _sha256(envelope_bytes),
        "endpoint_values_included": False,
        "hidden_relevance_values_included": False,
        "workflow_submitted": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256(_canonical(plan))
    documents = {
        PLAN_NAME: _json_bytes(plan),
        REQUESTS_NAME: requests_bytes,
        ENVELOPE_NAME: envelope_bytes,
    }
    target = Path(output_dir).resolve()
    _require_disjoint_output(target, [source_root])
    _publish_directory(target, documents)
    verified = verify_flowmesh_w4_candidate_matrix_plan(
        target, route_package_dir=source_root
    )
    return {**verified, "status": "FROZEN", "output_dir": str(target)}


def _load_plan(
    plan_dir: str | Path,
    *,
    route_package_dir: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    root = Path(plan_dir).resolve()
    documents = _verify_flat_directory(root, _PLAN_FILES, "W4 FlowMesh plan")
    plan = _strict_json_bytes(documents[PLAN_NAME], "W4 FlowMesh plan")
    requests = [
        _strict_json_bytes(line, f"W4 request line {index}")
        for index, line in enumerate(documents[REQUESTS_NAME].splitlines(), start=1)
    ]
    envelope = _strict_json_bytes(documents[ENVELOPE_NAME], "W4 workflow envelope")
    _require(
        documents[PLAN_NAME] == _json_bytes(plan)
        and documents[REQUESTS_NAME] == _jsonl_bytes(requests)
        and documents[ENVELOPE_NAME] == _json_bytes(envelope),
        "W4 FlowMesh plan is not canonical",
    )
    _strict_fields(plan, {
        "schema_version", "status", "run_id", "worker_alias", "owner",
        "api_task_timeout_seconds", "physical_plan_id", "route_plan_sha256",
        "route_package_checksums_sha256", "retrieval_task_binding_sha256",
        "candidate_set_sha256", "trial_count", "flowmesh_api_task_count",
        "workflow_count", "execution_order", "cache_semantics",
        "coordinator_service_contract_ids", "requests_sha256",
        "workflow_envelope_sha256", "endpoint_values_included",
        "hidden_relevance_values_included", "workflow_submitted", "llm_called",
        "credentials_recorded", "eligible_for_scientific_claims", "plan_sha256",
    }, "W4 FlowMesh plan")
    _require(
        plan["schema_version"] == FLOWMESH_W4_PLAN_SCHEMA_VERSION
        and plan["status"] == "FROZEN_W4_FLOWMESH_MATRIX_PLAN"
        and type(plan["api_task_timeout_seconds"]) is int
        and plan["api_task_timeout_seconds"] > 0
        and plan["trial_count"] == plan["flowmesh_api_task_count"] == 16
        and plan["workflow_count"] == 1
        and plan["execution_order"] == "global-serial-frozen-order"
        and plan["cache_semantics"] == "D3-and-D7-r0-miss-before-r1-hit"
        and plan["coordinator_service_contract_ids"]
        == ["N7.w4-candidate-coordinator", "N8.w4-candidate-coordinator"]
        and plan["requests_sha256"] == _sha256(documents[REQUESTS_NAME])
        and plan["workflow_envelope_sha256"] == _sha256(documents[ENVELOPE_NAME])
        and plan["endpoint_values_included"] is False
        and plan["hidden_relevance_values_included"] is False
        and plan["workflow_submitted"] is False
        and plan["llm_called"] is False
        and plan["credentials_recorded"] is False
        and plan["eligible_for_scientific_claims"] is False,
        "W4 FlowMesh plan claims or dimensions changed",
    )
    _identifier(plan["run_id"], "run_id")
    _identifier(plan["worker_alias"], "worker_alias")
    _identifier(plan["owner"], "owner")
    _document_sha256(plan, "plan_sha256")
    source_root = Path(route_package_dir).resolve()
    source = load_full_flow_w4_candidate_route_inputs(source_root)
    _require(
        plan["physical_plan_id"] == source.plan["physical_plan_id"]
        and plan["route_plan_sha256"] == source.plan["plan_sha256"]
        and plan["route_package_checksums_sha256"]
        == _sha256((source_root / CHECKSUMS_NAME).read_bytes())
        and plan["retrieval_task_binding_sha256"]
        == source.public_task["task_binding_sha256"]
        and plan["candidate_set_sha256"]
        == source.public_task["candidate_set_sha256"],
        "W4 FlowMesh plan source binding changed",
    )
    expected_requests = _compile_requests(source_root, plan["run_id"])
    expected_envelope = _workflow_envelope(
        expected_requests,
        worker_alias=plan["worker_alias"],
        owner=plan["owner"],
        api_task_timeout_seconds=plan["api_task_timeout_seconds"],
    )
    _require(
        requests == expected_requests and envelope == expected_envelope,
        "W4 FlowMesh plan differs from deterministic recompilation",
    )
    return plan, requests, envelope


def verify_flowmesh_w4_candidate_matrix_plan(
    plan_dir: str | Path,
    *,
    route_package_dir: str | Path,
) -> dict[str, Any]:
    plan, requests, _ = _load_plan(
        plan_dir, route_package_dir=route_package_dir
    )
    return {
        "status": "VERIFIED",
        "run_id": plan["run_id"],
        "physical_plan_id": plan["physical_plan_id"],
        "trial_count": len(requests),
        "flowmesh_api_task_count": len(requests),
        "workflow_count": 1,
        "worker_alias": plan["worker_alias"],
        "api_task_timeout_seconds": plan["api_task_timeout_seconds"],
        "plan_sha256": plan["plan_sha256"],
        "endpoint_binding_required_at_submission": True,
        "workflow_submitted": False,
        "eligible_for_scientific_claims": False,
    }


def _origin(value: Any, name: str, private_hosts: Sequence[str]) -> str:
    _require(isinstance(value, str) and value == value.strip(), f"{name} is invalid")
    parsed = urlsplit(str(value))
    _require(
        parsed.scheme in {"http", "https"}
        and parsed.hostname is not None
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
        and parsed.path in {"", "/"},
        f"{name} must be an origin without credentials, path, query, or fragment",
    )
    host = parsed.hostname.casefold()
    allowed = {item.casefold() for item in private_hosts}
    _require(
        parsed.scheme == "https"
        or host in {"127.0.0.1", "localhost", "::1"}
        or host in allowed,
        f"{name} plain HTTP is restricted to explicitly allowed simulator hosts",
    )
    return str(value).rstrip("/")


def build_flowmesh_w4_candidate_matrix_workflow(
    requests: Sequence[Mapping[str, Any]],
    *,
    coordinator_base_urls: Mapping[str, str],
    selected_worker_id: str,
    owner: str,
    api_task_timeout_seconds: int,
    runtime_header_provider: Callable[
        [Mapping[str, Any]], Mapping[str, str]
    ] | None = None,
    simulator_private_http_hosts: Sequence[str] = (),
) -> dict[str, Any]:
    _require(len(requests) == 16, "W4 workflow requires exactly sixteen requests")
    validated_requests = [
        validate_flowmesh_w4_trial_request(raw) for raw in requests
    ]
    _require(
        [row["order_index"] for row in validated_requests] == list(range(16))
        and len({row["trial_key"] for row in validated_requests}) == 16
        and len({row["request_sha256"] for row in validated_requests}) == 16
        and len({row["idempotency_key"] for row in validated_requests}) == 16
        and {
            (row["design_id"], row["repetition"])
            for row in validated_requests
        }
        == {
            (f"D{design}", repetition)
            for design in range(8)
            for repetition in (0, 1)
        }
        and all(
            row["coordinator_node_id"]
            == ("N7" if int(row["design_id"][1:]) < 4 else "N8")
            for row in validated_requests
        )
        and len({
            (
                row["run_id"],
                row["physical_plan_id"],
                row["route_plan_sha256"],
                row["retrieval_task_binding_sha256"],
                row["candidate_set_sha256"],
            )
            for row in validated_requests
        })
        == 1,
        "W4 workflow requests do not form one complete frozen matrix",
    )
    worker = _identifier(selected_worker_id, "selected_worker_id")
    owner_name = _identifier(owner, "owner")
    _require(
        type(api_task_timeout_seconds) is int and api_task_timeout_seconds > 0,
        "api_task_timeout_seconds must be a positive integer",
    )
    _require(
        isinstance(coordinator_base_urls, Mapping)
        and set(coordinator_base_urls) == {"N7", "N8"},
        "coordinator_base_urls must bind exactly N7 and N8",
    )
    urls = {
        node: _origin(value, f"{node} coordinator URL", simulator_private_http_hosts)
        for node, value in coordinator_base_urls.items()
    }
    nodes = []
    previous: str | None = None
    run_ids: set[str] = set()
    for request in validated_requests:
        run_ids.add(request["run_id"])
        headers = {"Content-Type": "application/json"}
        if runtime_header_provider is not None:
            runtime_headers = runtime_header_provider(
                json.loads(_canonical(request).decode("utf-8"))
            )
            _require(
                isinstance(runtime_headers, Mapping),
                "runtime headers are invalid",
            )
            _require(
                set(runtime_headers) == {FULL_FLOW_INGRESS_SIGNATURE_HEADER}
                and _SHA256.fullmatch(
                    str(runtime_headers.get(FULL_FLOW_INGRESS_SIGNATURE_HEADER))
                )
                is not None,
                "runtime header provider must return one full-flow HMAC digest",
            )
            for name, value in runtime_headers.items():
                _require(
                    isinstance(name, str) and name.casefold() != "content-type"
                    and isinstance(value, str) and bool(value),
                    "runtime header is invalid",
                )
                headers[name] = value
        name = f"w4-trial-{int(request['order_index']):04d}"
        node: dict[str, Any] = {
            "name": name,
            "spec": {
                "taskType": "api",
                "api": {
                    "url": urls[request["coordinator_node_id"]]
                    + W4_COORDINATOR_ENDPOINT_PATH,
                    "method": "POST",
                    "headers": headers,
                    "body": request,
                    "timeout_sec": api_task_timeout_seconds,
                    "response": {
                        "parse_json": False,
                        "return_body": True,
                        "raise_for_status": True,
                        "max_body_bytes": 16 * 1024 * 1024,
                    },
                },
                "output": {"destination": {"type": "http"}, "artifacts": []},
            },
        }
        if previous is not None:
            node["dependsOn"] = [previous]
        nodes.append(node)
        previous = name
    _require(len(run_ids) == 1, "W4 workflow requests mix run identities")
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": "pathfinder-w4-" + next(iter(run_ids))[:40],
            "owner": owner_name,
            "annotations": {
                "schedule_hint": {"selected_worker": worker},
                "custom": {
                    "pathfinder_w4_run_id": next(iter(run_ids)),
                    "pathfinder_w4_task_count": 16,
                    "pathfinder_w4_global_serial_order": True,
                    "pathfinder_hidden_relevance_values_included": False,
                },
            },
        },
        "spec": {"graph": {"nodes": nodes}},
    }


def full_flow_w4_hmac_header_provider(
    secret: str,
) -> Callable[[Mapping[str, Any]], Mapping[str, str]]:
    """Build a runtime-only signer for the dedicated W4 coordinator route."""

    def provide(request: Mapping[str, Any]) -> Mapping[str, str]:
        return {
            FULL_FLOW_INGRESS_SIGNATURE_HEADER: full_flow_request_hmac_sha256(
                request,
                secret,
            )
        }

    return provide


def validate_flowmesh_w4_trial_response(
    value: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    request = validate_flowmesh_w4_trial_request(request)
    _require(isinstance(value, Mapping), "W4 coordinator response is invalid")
    response = json.loads(_canonical(value).decode("utf-8"))
    _strict_fields(response, {
        "schema_version", "status", "request_sha256", "coordinator_node_id",
        "route_plan_sha256", "evidence_class", "trial_result", "operation_evidence",
        "observation", "component_events", "component_events_sha256",
        "hidden_relevance_values_read", "endpoint_values_included",
        "credentials_recorded", "eligible_for_scientific_claims",
        "response_sha256",
    }, "W4 coordinator response")
    _require(
        response["schema_version"] == FLOWMESH_W4_RESPONSE_SCHEMA_VERSION
        and response["status"] == "COMPLETE"
        and response["request_sha256"] == request["request_sha256"]
        and response["coordinator_node_id"] == request["coordinator_node_id"]
        and response["route_plan_sha256"] == request["route_plan_sha256"]
        and response["evidence_class"] in {
            "live-local-component-execution",
            "strict-fake-component-conformance",
        }
        and response["hidden_relevance_values_read"] is False
        and response["endpoint_values_included"] is False
        and response["credentials_recorded"] is False
        and response["eligible_for_scientific_claims"] is False,
        "W4 coordinator response binding or safety claim changed",
    )
    trial = response["trial_result"]
    evidence = response["operation_evidence"]
    observation = response["observation"]
    events = response["component_events"]
    _require(
        isinstance(trial, Mapping)
        and trial.get("trial_key") == request["trial_key"]
        and trial.get("run_id") == request["run_id"]
        and trial.get("order_index") == request["order_index"]
        and trial.get("design_id") == request["design_id"]
        and trial.get("repetition") == request["repetition"]
        and isinstance(trial.get("ranked_object_ids"), list)
        and isinstance(evidence, list)
        and evidence
        and all(
            isinstance(row, Mapping)
            and row.get("trial_key") == request["trial_key"]
            and row.get("run_id") == request["run_id"]
            for row in evidence
        )
        and isinstance(observation, Mapping)
        and observation.get("trial_key") == request["trial_key"]
        and observation.get("ranked_object_ids") == trial["ranked_object_ids"]
        and isinstance(events, list)
        and len(events)
        == sum(row.get("execution_status") == "COMPLETED" for row in evidence)
        and response["component_events_sha256"] == _sha256(_jsonl_bytes(events)),
        "W4 coordinator response trial evidence changed",
    )
    supplied = response.pop("response_sha256")
    _digest(supplied, "response_sha256")
    _require(supplied == _sha256(_canonical(response)), "W4 response digest changed")
    response["response_sha256"] = supplied
    _assert_public(response)
    return response


def _terminal_error(
    terminal: TerminalWorkflow,
    task_ids: Sequence[str],
    client: FlowMeshClientProtocol,
) -> FlowMeshW4CandidateMatrixError:
    details: list[str] = []
    if terminal.detail:
        details.append(redact_secrets(terminal.detail))
    for task_id in task_ids:
        try:
            detail = client.describe_task_failure(task_id)
        except Exception:
            continue
        if isinstance(detail, Mapping) and detail.get("detail"):
            details.append(redact_secrets(str(detail["detail"])))
    suffix = "" if not details else ": " + "; ".join(details[:3])
    return FlowMeshW4CandidateMatrixError(
        f"W4 FlowMesh workflow {terminal.workflow_id} ended with "
        f"{terminal.status}{suffix}"
    )


def _inner_report(path: Path) -> dict[str, Any]:
    return _strict_json_bytes(path.read_bytes(), path.name)


def run_flowmesh_w4_candidate_matrix(
    *,
    plan_dir: str | Path,
    route_package_dir: str | Path,
    crosswalk_dir: str | Path,
    index_package_dir: str | Path,
    output_dir: str | Path,
    coordinator_base_urls: Mapping[str, str],
    runtime_header_provider: Callable[[Mapping[str, Any]], Mapping[str, str]],
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    simulator_private_http_hosts: Sequence[str] = (),
) -> dict[str, Any]:
    plan, requests, _ = _load_plan(
        plan_dir, route_package_dir=route_package_dir
    )
    _require(
        settings.worker_alias == plan["worker_alias"]
        and settings.worker_id is None,
        "W4 run requires the frozen stable worker alias",
    )
    _require(callable(runtime_header_provider), "runtime header provider is required")
    target = Path(output_dir).resolve()
    _require(
        not target.exists() and not target.is_symlink(),
        f"output exists: {target}",
    )
    _require_disjoint_output(
        target,
        [
            Path(plan_dir).resolve(),
            Path(route_package_dir).resolve(),
            Path(crosswalk_dir).resolve(),
            Path(index_package_dir).resolve(),
        ],
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    identity = describe_pinned_worker(client, settings)
    _require(identity.alias in {None, plan["worker_alias"]}, "worker alias changed")
    workflow = build_flowmesh_w4_candidate_matrix_workflow(
        requests,
        coordinator_base_urls=coordinator_base_urls,
        selected_worker_id=identity.worker_id,
        owner=plan["owner"],
        api_task_timeout_seconds=plan["api_task_timeout_seconds"],
        runtime_header_provider=runtime_header_provider,
        simulator_private_http_hosts=simulator_private_http_hosts,
    )
    validation = client.validate(workflow)
    _require(validation.ok, "W4 FlowMesh workflow validation failed")
    submitted = client.submit(workflow)
    _require(
        len(submitted.task_ids) == len(requests) == 16,
        "FlowMesh returned the wrong W4 task count",
    )
    terminal = client.wait(
        submitted.workflow_id, settings.poll_interval_seconds
    )
    if terminal.status != "DONE" or terminal.workflow_id != submitted.workflow_id:
        raise _terminal_error(terminal, submitted.task_ids, client)
    request_by_sha = {row["request_sha256"]: row for row in requests}
    response_by_sha: dict[str, dict[str, Any]] = {}
    task_by_sha: dict[str, dict[str, Any]] = {}
    for task_id in submitted.task_ids:
        api = extract_api_executor_result(client.retrieve_result(task_id))
        raw_response = _strict_json_bytes(
            api["text"].encode("utf-8"), "W4 API result"
        )
        request_sha = raw_response.get("request_sha256")
        _require(
            isinstance(request_sha, str)
            and request_sha in request_by_sha
            and request_sha not in response_by_sha,
            "W4 task response coverage is duplicated or unknown",
        )
        request = request_by_sha[request_sha]
        response = validate_flowmesh_w4_trial_response(
            raw_response, request=request
        )
        detail = client.describe_task_failure(task_id)
        _require(
            isinstance(detail, Mapping)
            and detail.get("assigned_worker") == identity.worker_id,
            "W4 task was not assigned to the pinned worker",
        )
        response_by_sha[request_sha] = response
        task_by_sha[request_sha] = {
            "schema_version": FLOWMESH_W4_TASK_SCHEMA_VERSION,
            "task_id": _identifier(task_id, "task_id"),
            "trial_key": request["trial_key"],
            "request_sha256": request["request_sha256"],
            "response_sha256": response["response_sha256"],
            "assigned_worker_id": identity.worker_id,
            "api_executor": api["executor"],
            "api_http_status": api["status_code"],
            "credentials_recorded": False,
        }
    _require(
        set(response_by_sha) == set(request_by_sha)
        and set(task_by_sha) == set(request_by_sha),
        "W4 task responses do not cover the frozen requests",
    )
    responses = [response_by_sha[row["request_sha256"]] for row in requests]
    task_rows = [task_by_sha[row["request_sha256"]] for row in requests]

    staging_parent = Path(
        tempfile.mkdtemp(prefix=".flowmesh-w4-run-", dir=target.parent)
    )
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        candidate_dir = staging / CANDIDATE_RUN_DIR_NAME
        receipt_dir = staging / COMPONENT_RECEIPT_DIR_NAME
        trial_results = [row["trial_result"] for row in responses]
        operation_evidence = [
            item for row in responses for item in row["operation_evidence"]
        ]
        observations = [row["observation"] for row in responses]
        component_events = [
            item for row in responses for item in row["component_events"]
        ]
        evidence_classes = {row["evidence_class"] for row in responses}
        _require(
            len(evidence_classes) == 1,
            "W4 coordinator responses mix component evidence classes",
        )
        evidence_class = next(iter(evidence_classes))
        publish_full_flow_w4_candidate_coordinator_results(
            route_package_dir,
            run_id=plan["run_id"],
            trial_results=trial_results,
            operation_evidence=operation_evidence,
            observations=observations,
            output_dir=candidate_dir,
        )
        component_event_bytes = _jsonl_bytes(component_events)
        component_event_path = staging / FLOWMESH_COMPONENT_EVENTS_NAME
        component_event_path.write_bytes(component_event_bytes)
        freeze_full_flow_w4_component_execution_receipt(
            candidate_dir,
            route_package_dir=route_package_dir,
            crosswalk_dir=crosswalk_dir,
            index_package_dir=index_package_dir,
            component_events_path=component_event_path,
            evidence_class=evidence_class,
            output_dir=receipt_dir,
        )
        candidate_report = _inner_report(candidate_dir / COORDINATOR_RUN_NAME)
        receipt_report = _inner_report(receipt_dir / COMPONENT_RECEIPT_NAME)
        response_bytes = _jsonl_bytes(responses)
        task_bytes = _jsonl_bytes(task_rows)
        submission = {
            "schema_version": FLOWMESH_W4_SUBMISSION_SCHEMA_VERSION,
            "workflow_id": _identifier(submitted.workflow_id, "workflow_id"),
            "task_ids": [row["task_id"] for row in task_rows],
            "worker": identity.to_public_dict(),
            "validated_before_submission": True,
            "trial_count": 16,
            "endpoint_values_included": False,
            "credentials_recorded": False,
        }
        submission_bytes = _json_bytes(submission)
        report: dict[str, Any] = {
            "schema_version": FLOWMESH_W4_RUN_SCHEMA_VERSION,
            "status": "COMPLETE",
            "run_id": plan["run_id"],
            "physical_plan_id": plan["physical_plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "route_plan_sha256": plan["route_plan_sha256"],
            "workflow_id": submitted.workflow_id,
            "worker_id": identity.worker_id,
            "worker_alias": plan["worker_alias"],
            "workflow_count": 1,
            "flowmesh_api_task_count": 16,
            "completed_trial_count": 16,
            "candidate_coordinator_run_sha256": candidate_report["run_sha256"],
            "component_receipt_sha256": receipt_report["receipt_sha256"],
            "component_evidence_class": evidence_class,
            "candidate_run_checksums_sha256": _sha256(
                (candidate_dir / CHECKSUMS_NAME).read_bytes()
            ),
            "component_receipt_checksums_sha256": _sha256(
                (receipt_dir / CHECKSUMS_NAME).read_bytes()
            ),
            "task_records_sha256": _sha256(task_bytes),
            "responses_sha256": _sha256(response_bytes),
            "component_events_sha256": _sha256(component_event_bytes),
            "global_serial_execution_declared": True,
            "independent_n7_n8_cache_miss_then_hit_verified": True,
            "ready_for_n1_hidden_relevance_evaluation": True,
            "llm_called": receipt_report["llm_called"],
            "flowmesh_workflow_submitted": True,
            "hidden_relevance_values_read": False,
            "endpoint_values_included": False,
            "credentials_recorded": False,
            "real_cloud_performance_measured": False,
            "eligible_for_scientific_claims": False,
        }
        report["run_sha256"] = _sha256(_canonical(report))
        documents = {
            RUN_NAME: _json_bytes(report),
            SUBMISSION_NAME: submission_bytes,
            TASKS_NAME: task_bytes,
            RESPONSES_NAME: response_bytes,
            FLOWMESH_COMPONENT_EVENTS_NAME: component_event_bytes,
        }
        for name, content in documents.items():
            if name == FLOWMESH_COMPONENT_EVENTS_NAME:
                _require(
                    component_event_path.read_bytes() == content,
                    "component event source changed during freeze",
                )
            else:
                (staging / name).write_bytes(content)
        (staging / CHECKSUMS_NAME).write_bytes(_checksums(documents))
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging_parent, ignore_errors=True)
        raise
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    verified = verify_flowmesh_w4_candidate_matrix_run(
        target,
        plan_dir=plan_dir,
        route_package_dir=route_package_dir,
        crosswalk_dir=crosswalk_dir,
        index_package_dir=index_package_dir,
    )
    return {**verified, "status": "COMPLETE", "output_dir": str(target)}


def verify_flowmesh_w4_candidate_matrix_run(
    run_dir: str | Path,
    *,
    plan_dir: str | Path,
    route_package_dir: str | Path,
    crosswalk_dir: str | Path,
    index_package_dir: str | Path,
) -> dict[str, Any]:
    root = Path(run_dir).resolve()
    _require(root.is_dir() and not root.is_symlink(), "W4 FlowMesh run is missing")
    entries = {path.name: path for path in root.iterdir()}
    _require(
        set(entries) == _RUN_FILES | {
            CHECKSUMS_NAME, CANDIDATE_RUN_DIR_NAME, COMPONENT_RECEIPT_DIR_NAME,
        }
        and all(
            entries[name].is_file() and not entries[name].is_symlink()
            for name in _RUN_FILES | {CHECKSUMS_NAME}
        )
        and all(
            entries[name].is_dir() and not entries[name].is_symlink()
            for name in {CANDIDATE_RUN_DIR_NAME, COMPONENT_RECEIPT_DIR_NAME}
        ),
        "W4 FlowMesh run file set changed",
    )
    documents = {name: (root / name).read_bytes() for name in _RUN_FILES}
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(documents),
        "W4 FlowMesh run checksums failed",
    )
    report = _strict_json_bytes(documents[RUN_NAME], "W4 FlowMesh run")
    submission = _strict_json_bytes(documents[SUBMISSION_NAME], "W4 submission")
    tasks = [
        _strict_json_bytes(line, f"W4 task line {index}")
        for index, line in enumerate(documents[TASKS_NAME].splitlines(), start=1)
    ]
    responses = [
        _strict_json_bytes(line, f"W4 response line {index}")
        for index, line in enumerate(documents[RESPONSES_NAME].splitlines(), start=1)
    ]
    events = [
        _strict_json_bytes(line, f"W4 component event line {index}")
        for index, line in enumerate(
            documents[FLOWMESH_COMPONENT_EVENTS_NAME].splitlines(), start=1
        )
    ]
    _require(
        documents[RUN_NAME] == _json_bytes(report)
        and documents[SUBMISSION_NAME] == _json_bytes(submission)
        and documents[TASKS_NAME] == _jsonl_bytes(tasks)
        and documents[RESPONSES_NAME] == _jsonl_bytes(responses)
        and documents[FLOWMESH_COMPONENT_EVENTS_NAME] == _jsonl_bytes(events),
        "W4 FlowMesh run is not canonical",
    )
    plan, requests, _ = _load_plan(plan_dir, route_package_dir=route_package_dir)
    _require(
        len(tasks) == len(responses) == len(requests) == 16,
        "W4 run coverage changed",
    )
    for request, response, task in zip(requests, responses, tasks, strict=True):
        validated = validate_flowmesh_w4_trial_response(response, request=request)
        _strict_fields(task, {
            "schema_version", "task_id", "trial_key", "request_sha256",
            "response_sha256", "assigned_worker_id", "api_executor",
            "api_http_status", "credentials_recorded",
        }, "W4 FlowMesh task")
        _require(
            task["schema_version"] == FLOWMESH_W4_TASK_SCHEMA_VERSION
            and task["trial_key"] == request["trial_key"]
            and task["request_sha256"] == request["request_sha256"]
            and task["response_sha256"] == validated["response_sha256"]
            and task["api_executor"] == "api"
            and type(task["api_http_status"]) is int
            and 200 <= task["api_http_status"] < 300
            and task["credentials_recorded"] is False,
            "W4 FlowMesh task binding changed",
        )
        _identifier(task["task_id"], "task_id")
        _identifier(task["assigned_worker_id"], "assigned_worker_id")
    flattened_events = [item for row in responses for item in row["component_events"]]
    _require(events == flattened_events, "W4 component event aggregation changed")
    candidate = verify_full_flow_w4_candidate_coordinator_run(
        root / CANDIDATE_RUN_DIR_NAME,
        route_package_dir=route_package_dir,
    )
    receipt = verify_full_flow_w4_component_execution_receipt(
        root / COMPONENT_RECEIPT_DIR_NAME,
        coordinator_run_dir=root / CANDIDATE_RUN_DIR_NAME,
        route_package_dir=route_package_dir,
        crosswalk_dir=crosswalk_dir,
        index_package_dir=index_package_dir,
    )
    candidate_report = _inner_report(
        root / CANDIDATE_RUN_DIR_NAME / COORDINATOR_RUN_NAME
    )
    receipt_report = _inner_report(
        root / COMPONENT_RECEIPT_DIR_NAME / COMPONENT_RECEIPT_NAME
    )
    _strict_fields(submission, {
        "schema_version", "workflow_id", "task_ids", "worker",
        "validated_before_submission", "trial_count",
        "endpoint_values_included", "credentials_recorded",
    }, "W4 FlowMesh submission")
    worker = submission.get("worker")
    _require(
        submission["schema_version"] == FLOWMESH_W4_SUBMISSION_SCHEMA_VERSION
        and isinstance(submission["task_ids"], list)
        and len(submission["task_ids"]) == 16
        and len(set(submission["task_ids"])) == 16
        and submission["task_ids"] == [row["task_id"] for row in tasks]
        and submission["trial_count"] == 16
        and isinstance(worker, Mapping)
        and worker.get("worker_id") == tasks[0]["assigned_worker_id"]
        and all(row["assigned_worker_id"] == worker["worker_id"] for row in tasks)
        and submission["validated_before_submission"] is True
        and submission["endpoint_values_included"] is False
        and submission["credentials_recorded"] is False,
        "W4 FlowMesh submission binding changed",
    )
    _require(
        set(worker) == {
            "worker_id", "alias", "status", "namespace", "cluster", "node_alias",
        }
        and all(
            value is None or isinstance(value, str)
            for value in worker.values()
        ),
        "W4 public worker identity fields changed",
    )
    _identifier(submission["workflow_id"], "workflow_id")
    for task_id in submission["task_ids"]:
        _identifier(task_id, "task_id")
    _identifier(worker["worker_id"], "worker_id")
    _require(
        worker["alias"] in {None, plan["worker_alias"]},
        "W4 public worker alias differs from the frozen alias",
    )
    _assert_public(submission)
    _assert_public(tasks)
    _assert_public(responses)
    _strict_fields(report, {
        "schema_version", "status", "run_id", "physical_plan_id",
        "plan_sha256", "route_plan_sha256", "workflow_id", "worker_id",
        "worker_alias", "workflow_count", "flowmesh_api_task_count",
        "completed_trial_count", "candidate_coordinator_run_sha256",
        "component_receipt_sha256", "component_evidence_class",
        "candidate_run_checksums_sha256",
        "component_receipt_checksums_sha256", "task_records_sha256",
        "responses_sha256", "component_events_sha256",
        "global_serial_execution_declared",
        "independent_n7_n8_cache_miss_then_hit_verified",
        "ready_for_n1_hidden_relevance_evaluation", "llm_called",
        "flowmesh_workflow_submitted", "hidden_relevance_values_read",
        "endpoint_values_included", "credentials_recorded",
        "real_cloud_performance_measured", "eligible_for_scientific_claims",
        "run_sha256",
    }, "W4 FlowMesh run")
    _require(
        report["schema_version"] == FLOWMESH_W4_RUN_SCHEMA_VERSION
        and report["status"] == "COMPLETE"
        and report["run_id"] == plan["run_id"]
        and report["physical_plan_id"] == plan["physical_plan_id"]
        and report["plan_sha256"] == plan["plan_sha256"]
        and report["route_plan_sha256"] == plan["route_plan_sha256"]
        and report["workflow_id"] == submission["workflow_id"]
        and report["worker_id"] == worker["worker_id"]
        and report["worker_alias"] == plan["worker_alias"]
        and report["workflow_count"] == 1
        and report["flowmesh_api_task_count"] == 16
        and report["completed_trial_count"] == candidate["trial_count"] == 16
        and report["candidate_coordinator_run_sha256"]
        == candidate_report["run_sha256"]
        and report["component_receipt_sha256"] == receipt_report["receipt_sha256"]
        and report["component_evidence_class"] == receipt["evidence_class"]
        and report["candidate_run_checksums_sha256"]
        == _sha256((root / CANDIDATE_RUN_DIR_NAME / CHECKSUMS_NAME).read_bytes())
        and report["component_receipt_checksums_sha256"]
        == _sha256((root / COMPONENT_RECEIPT_DIR_NAME / CHECKSUMS_NAME).read_bytes())
        and report["task_records_sha256"] == _sha256(documents[TASKS_NAME])
        and report["responses_sha256"] == _sha256(documents[RESPONSES_NAME])
        and report["component_events_sha256"]
        == _sha256(documents[FLOWMESH_COMPONENT_EVENTS_NAME])
        and report["global_serial_execution_declared"] is True
        and report["independent_n7_n8_cache_miss_then_hit_verified"] is True
        and report["ready_for_n1_hidden_relevance_evaluation"] is True
        and report["llm_called"] == receipt["llm_called"]
        and report["flowmesh_workflow_submitted"] is True
        and report["hidden_relevance_values_read"] is False
        and report["endpoint_values_included"] is False
        and report["credentials_recorded"] is False
        and report["real_cloud_performance_measured"] is False
        and report["eligible_for_scientific_claims"] is False,
        "W4 FlowMesh run claims or source bindings changed",
    )
    _document_sha256(report, "run_sha256")
    _identifier(report["workflow_id"], "workflow_id")
    _identifier(report["worker_id"], "worker_id")
    _assert_public(report)
    return {
        "status": "VERIFIED",
        "run_id": report["run_id"],
        "physical_plan_id": report["physical_plan_id"],
        "plan_sha256": report["plan_sha256"],
        "completed_trial_count": 16,
        "flowmesh_api_task_count": 16,
        "workflow_count": 1,
        "worker_id": report["worker_id"],
        "source_binding_checked": True,
        "component_evidence_class": report["component_evidence_class"],
        "llm_called": report["llm_called"],
        "ready_for_n1_hidden_relevance_evaluation": True,
        "real_cloud_performance_measured": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CANDIDATE_RUN_DIR_NAME",
    "COMPONENT_RECEIPT_DIR_NAME",
    "FLOWMESH_W4_PLAN_SCHEMA_VERSION",
    "FLOWMESH_W4_REQUEST_SCHEMA_VERSION",
    "FLOWMESH_W4_RESPONSE_SCHEMA_VERSION",
    "FLOWMESH_W4_RUN_SCHEMA_VERSION",
    "FlowMeshW4CandidateMatrixError",
    "W4_COORDINATOR_ENDPOINT_PATH",
    "build_flowmesh_w4_candidate_matrix_workflow",
    "build_flowmesh_w4_trial_request",
    "full_flow_w4_hmac_header_provider",
    "plan_flowmesh_w4_candidate_matrix",
    "run_flowmesh_w4_candidate_matrix",
    "validate_flowmesh_w4_trial_request",
    "validate_flowmesh_w4_trial_response",
    "verify_flowmesh_w4_candidate_matrix_plan",
    "verify_flowmesh_w4_candidate_matrix_run",
]
