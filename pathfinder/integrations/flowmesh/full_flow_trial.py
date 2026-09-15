"""One worker-pinned FlowMesh task for a complete Pathfinder trial.

The logical plan freezes the exact request accepted by N7, while remaining
free of endpoints, worker identities, credentials, host paths, and artifact
bytes.  A separate deployment binding supplies the current N7 origin and
stable FlowMesh worker alias.  FlowMesh submits one API task; N7 performs the
N4 Data Agent -> N7 executor -> N6 inference -> N7 scoring route internally.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from ...distributed.scoring import (
    AnswerOption,
    WorkloadScoringContract,
    evaluate_workload_answer,
    load_workload_scoring_contract,
    render_workload_question,
)
from ...frame_bundle_ingest import FRAME_BUNDLE_MEDIA_TYPE
from ...simulator.data_agent_semantic_vertical import (
    load_data_agent_frame_bundle_semantic_spec,
)
from ...simulator.container_node import (
    FULL_FLOW_INGRESS_HMAC_SECRET_ENV,
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    ContainerNodeRuntime,
    full_flow_request_hmac_sha256,
)
from ...simulator.full_flow_data_plane import (
    FULL_FLOW_DATA_PLANE_SCHEMA_VERSION,
    PACKAGE_MANIFEST_NAME,
    verify_full_flow_data_plane_package,
)
from ...simulator.full_flow_runtime import (
    FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
    FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
    FULL_FLOW_REQUEST_SCHEMA_VERSION,
    FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
    FullFlowRouteConfig,
    build_full_flow_trial_request,
    full_flow_n1_score_request_id,
    validate_full_flow_trial_request_v2,
)
from ...simulator.hidden_oracle import (
    N1_NODE_ID,
    N1_SCORE_RESULT_SCHEMA_VERSION,
    assert_hidden_oracle_fields_absent,
    build_n1_score_request,
    verify_n1_score_result,
)
from .adapter import extract_api_executor_result
from .contracts import (
    FlowMeshClientProtocol,
    FlowMeshSettings,
    SubmittedWorkflow,
    TerminalWorkflow,
)
from .preflight import describe_pinned_worker
from .redaction import redact_secrets


# The data-plane input is the existing portable package, not another parallel
# binding schema.  This alias makes that fact explicit to callers.
FULL_FLOW_DATA_PLANE_BINDING_SCHEMA_VERSION = FULL_FLOW_DATA_PLANE_SCHEMA_VERSION
FULL_FLOW_DEPLOYMENT_BINDING_SCHEMA_VERSION = (
    "pathfinder.full-flow-deployment-binding/v1alpha1"
)
FULL_FLOW_LOGICAL_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-full-flow-logical-plan/v1alpha1"
)
FULL_FLOW_TASK_RECORD_SCHEMA_VERSION = (
    "pathfinder.flowmesh-full-flow-task-record/v1alpha1"
)
FULL_FLOW_RUN_SCHEMA_VERSION = "pathfinder.flowmesh-full-flow-run/v1alpha1"
FULL_FLOW_SUBMISSION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-full-flow-submission/v1alpha1"
)
FULL_FLOW_SERVICE_RESULT_SCHEMA_VERSION = FULL_FLOW_EVIDENCE_SCHEMA_VERSION
FULL_FLOW_LOGICAL_PLAN_V2_SCHEMA_VERSION = (
    "pathfinder.flowmesh-full-flow-logical-plan/v2alpha1"
)
FULL_FLOW_TASK_RECORD_V2_SCHEMA_VERSION = (
    "pathfinder.flowmesh-full-flow-task-record/v2alpha1"
)
FULL_FLOW_RUN_V2_SCHEMA_VERSION = "pathfinder.flowmesh-full-flow-run/v2alpha1"
FULL_FLOW_SUBMISSION_V2_SCHEMA_VERSION = (
    "pathfinder.flowmesh-full-flow-submission/v2alpha1"
)
FULL_FLOW_SERVICE_RESULT_V2_SCHEMA_VERSION = FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION

_LOGICAL_FILE = "flowmesh-full-flow-logical-plan.json"
_DEPLOYMENT_FILE = "flowmesh-full-flow-deployment-binding.json"
_TEMPLATE_FILE = "flowmesh-full-flow-workflow-template.json"
_PLAN_FILES = frozenset({_LOGICAL_FILE, _DEPLOYMENT_FILE, _TEMPLATE_FILE})
_RUN_FILE = "flowmesh-full-flow-run.json"
_SUBMISSION_FILE = "flowmesh-full-flow-submission.json"
_TASK_RECORD_FILE = "flowmesh-full-flow-task-record.json"
_RUN_FILES = frozenset({_RUN_FILE, _SUBMISSION_FILE, _TASK_RECORD_FILE})
_LOGICAL_V2_FILE = "flowmesh-full-flow-v2-logical-plan.json"
_DEPLOYMENT_V2_FILE = "flowmesh-full-flow-v2-deployment-binding.json"
_TEMPLATE_V2_FILE = "flowmesh-full-flow-v2-workflow-template.json"
_PLAN_V2_FILES = frozenset(
    {_LOGICAL_V2_FILE, _DEPLOYMENT_V2_FILE, _TEMPLATE_V2_FILE}
)
_RUN_V2_FILE = "flowmesh-full-flow-v2-run.json"
_SUBMISSION_V2_FILE = "flowmesh-full-flow-v2-submission.json"
_TASK_RECORD_V2_FILE = "flowmesh-full-flow-v2-task-record.json"
_RUN_V2_FILES = frozenset(
    {_RUN_V2_FILE, _SUBMISSION_V2_FILE, _TASK_RECORD_V2_FILE}
)
_V2_EVIDENCE_CLASS = "flowmesh-unified-pathfinder-hidden-oracle-v2"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:-]{0,511}\Z")
_RUNTIME_EPOCH = re.compile(r"[0-9a-f]{32}\Z")

_DEPLOYMENT_KEYS = frozenset({
    "schema_version",
    "deployment_binding_id",
    "coordinator_node_id",
    "coordinator_api_url",
    "worker_alias",
    "api_task_timeout_seconds",
    "credentials_recorded",
})
_REQUEST_KEYS = frozenset({
    "schema_version",
    "full_flow_request_id",
    "run_id",
    "trial_id",
    "trial_key",
    "workload_id",
    "task_class_id",
    "route_id",
    "event_index",
    "object_id",
    "representation_id",
    "artifact_sha256",
    "artifact_size_bytes",
    "object_catalog_version",
    "expected_model",
    "question",
    "success_scoring_rule",
    "answer_options",
    "correct_answer_id",
    "frozen_binding_sha256",
    "credentials_recorded",
})
_EXPECTED_ARTIFACT_KEYS = frozenset({
    "object_id",
    "representation_id",
    "artifact_sha256",
    "artifact_size_bytes",
    "object_catalog_version",
    "frame_count",
    "manifest_sha256",
})
_ROUTE_BINDING_KEYS = frozenset({
    "route_id",
    "source_node_id",
    "executor_node_id",
    "inference_node_id",
    "scoring_node_id",
    "requested_location",
    "data_agent_plan_id",
    "data_agent_plan_epoch",
    "quiescence_timeout_seconds",
})
_REQUIRED_RESULT_CLAIMS = {
    "real_object_identity_verified": True,
    "data_agent_source_identity_verified": True,
    "data_agent_artifact_delivery_verified": True,
    "semantic_frame_payload_integrity_verified": True,
    "container_semantic_response_consistency_verified": True,
    "semantic_health_verified": True,
    "scoring_verified": True,
    "route_unified": True,
    "llm_called": True,
    "telemetry_complete": True,
    "credentials_recorded": False,
    "eligible_for_scientific_claims": False,
}
_LOGICAL_PLAN_KEYS = frozenset({
    "schema_version",
    "status",
    "logical_plan_id",
    "owner",
    "semantic_spec_sha256",
    "data_plane_manifest_sha256",
    "request_body",
    "route_binding",
    "expected_artifact",
    "required_result_claims",
    "evidence_class",
    "workflow_submitted",
    "credentials_recorded",
    "eligible_for_awm_oed",
    "eligible_for_scientific_claims",
    "plan_sha256",
})
_LOGICAL_PLAN_V2_KEYS = frozenset({
    "schema_version",
    "status",
    "logical_plan_id",
    "owner",
    "data_plane_manifest_sha256",
    "public_task_binding_sha256",
    "oracle_id",
    "request_body",
    "route_binding",
    "expected_artifact",
    "required_result_claims",
    "evidence_class",
    "workflow_submitted",
    "credentials_recorded",
    "eligible_for_awm_oed",
    "eligible_for_scientific_claims",
    "plan_sha256",
})
_EVIDENCE_KEYS = frozenset({
    "schema_version",
    "status",
    "full_flow_request_id",
    "request_sha256",
    "frozen_binding_sha256",
    "route_config_sha256",
    "idempotent_replay",
    "run_id",
    "trial_id",
    "trial_key",
    "workload_id",
    "task_class_id",
    "object_id",
    "representation_id",
    "route",
    "data_agent",
    "semantic",
    "scoring",
    "real_object_identity_verified",
    "data_agent_source_identity_verified",
    "data_agent_artifact_delivery_verified",
    "semantic_frame_payload_integrity_verified",
    "container_semantic_response_consistency_verified",
    "semantic_health_verified",
    "scoring_verified",
    "route_unified",
    "llm_called",
    "telemetry_complete",
    "credentials_recorded",
    "eligible_for_scientific_claims",
})
_EVIDENCE_ROUTE_KEYS = frozenset({
    "route_id",
    "source_node_id",
    "executor_node_id",
    "inference_node_id",
    "requested_location",
})
_EVIDENCE_DATA_AGENT_KEYS = frozenset({
    "access_id",
    "source_node_id",
    "source_identity_basis",
    "source_identity_verified",
    "health_sha256_before",
    "health_sha256_after",
    "plan_id",
    "plan_epoch",
    "object_catalog_version",
    "artifact_media_type",
    "artifact_size_bytes",
    "artifact_sha256",
    "manifest_sha256",
    "frame_count",
    "total_jpeg_bytes",
    "delivery",
    "latency_ms",
})
_DELIVERY_KEYS = frozenset({
    "telemetry_supported",
    "telemetry_complete",
    "in_flight_request_count",
    "download_request_count",
    "completed_request_count",
    "full_download_count",
    "bytes_sent",
    "artifact_size_bytes",
    "server_reported_transfer_latency_ms",
    "exactly_one_full_download",
    "bytes_sent_equals_artifact_size",
    "telemetry_object_id",
    "telemetry_object_catalog_version",
    "expected_object_catalog_version",
})
_LATENCY_KEYS = frozenset({
    "data_agent_service",
    "client_access_round_trip",
    "artifact_download_elapsed",
    "server_reported_transfer",
})
_SEMANTIC_KEYS = frozenset({
    "semantic_request_id",
    "request_sha256",
    "question_sha256",
    "prompt_sha256",
    "frame_sequence_sha256",
    "frame_metadata",
    "representation_delivery_bytes",
    "model",
    "runtime_epoch",
    "service_time_ms",
    "adapter_idempotent_replay",
})
_FRAME_METADATA_KEYS = frozenset({
    "frame_index",
    "timestamp_seconds",
    "width",
    "height",
    "jpeg_size_bytes",
    "jpeg_sha256",
})
_SCORING_KEYS = frozenset({
    "success_scoring_rule",
    "answer_option_ids",
    "answer_options_sha256",
    "correct_answer_id",
    "final_answer",
    "final_answer_sha256",
    "task_success",
})
_SCORING_V2_KEYS = frozenset({
    "success_scoring_rule",
    "answer_option_ids",
    "answer_options_sha256",
    "task_binding_sha256",
    "final_answer",
    "final_answer_sha256",
    "task_success",
    "score",
    "oracle_health_sha256",
    "oracle_result",
})
_ORACLE_RESULT_KEYS = frozenset({
    "schema_version",
    "status",
    "score_request_id",
    "evaluation_unit_id",
    "oracle_id",
    "node_id",
    "run_id",
    "trial_id",
    "object_id",
    "task_binding_sha256",
    "request_sha256",
    "prediction_sha256",
    "success_scoring_rule",
    "correct",
    "score",
    "public_task_set_sha256",
    "oracle_instance_hmac_sha256",
    "score_evidence_hmac_sha256",
    "result_content_sha256",
    "idempotent_replay",
    "hidden_answer_returned",
    "credentials_recorded",
    "eligible_for_scientific_claims",
})
_SUMMARY_KEYS = frozenset({
    "schema_version",
    "status",
    "logical_plan_id",
    "plan_sha256",
    "deployment_binding_sha256",
    "workflow_id",
    "task_id",
    "selected_worker",
    "trial_key",
    "object_id",
    "representation_id",
    "source_node_id",
    "execution_node_id",
    "semantic_executor_node_id",
    "scoring_node_id",
    "task_success",
    "idempotent_replay",
    "route_unified",
    "flowmesh_semantic_execution_verified",
    "host_artifact_materialized",
    "telemetry_complete",
    "llm_called",
    "evidence_class",
    "credentials_recorded",
    "eligible_for_awm_oed",
    "eligible_for_scientific_claims",
})
_SUBMISSION_KEYS = frozenset({
    "schema_version",
    "workflow_id",
    "task_id",
    "selected_worker_id",
    "logical_plan_sha256",
    "deployment_binding_sha256",
    "workflow_sha256",
    "validated_before_submission",
    "credentials_recorded",
})
_TASK_RECORD_KEYS = frozenset({
    "schema_version",
    "task_id",
    "worker_id",
    "api_executor",
    "api_http_status",
    "service_result_sha256",
    "service_result",
    "credentials_recorded",
})


class FlowMeshFullFlowTrialError(RuntimeError):
    """Raised when a full-flow plan or result is untrustworthy."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FlowMeshFullFlowTrialError(message)


def _text(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return str(value).strip()


def _identifier(value: Any, name: str) -> str:
    result = _text(value, name)
    _require(
        _IDENTIFIER.fullmatch(result) is not None,
        f"{name} contains unsupported characters",
    )
    return result


def _runtime_full_flow_hmac_secret(value: str | None) -> str:
    secret = (
        os.environ.get(FULL_FLOW_INGRESS_HMAC_SECRET_ENV)
        if value is None
        else value
    )
    _require(
        isinstance(secret, str)
        and bool(secret)
        and secret == secret.strip()
        and secret.isascii()
        and len(secret.encode("ascii")) >= 32
        and len(secret.encode("ascii")) <= 8192
        and all(33 <= ord(character) <= 126 for character in secret),
        f"{FULL_FLOW_INGRESS_HMAC_SECRET_ENV} is required and must be "
        "at least 32 bytes of printable ASCII",
    )
    return secret


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    _require(
        type(value) is int and value >= minimum,
        f"{name} must be an integer >= {minimum}",
    )
    return int(value)


def _number(value: Any, name: str) -> float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0.0,
        f"{name} must be a finite non-negative number",
    )
    return float(value)


def _digest(value: Any, name: str) -> str:
    result = _text(value, name)
    _require(_SHA256.fullmatch(result) is not None, f"{name} is not SHA-256")
    return result


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FlowMeshFullFlowTrialError(
            "full-flow value is not canonical JSON"
        ) from exc


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


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _document_sha256(document: Mapping[str, Any], field: str) -> str:
    copied = dict(document)
    copied.pop(field, None)
    return _sha256_bytes(_canonical_bytes(copied))


def _strict_json(raw: bytes, name: str) -> dict[str, Any]:
    def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{name} contains a duplicate key")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise FlowMeshFullFlowTrialError(
            f"{name} contains a non-finite number"
        )

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except FlowMeshFullFlowTrialError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshFullFlowTrialError(f"{name} is not valid JSON") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )


def _write_documents(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    stage_parent = Path(
        tempfile.mkdtemp(prefix=".flowmesh-full-flow-", dir=target.parent)
    )
    stage = stage_parent / "output"
    try:
        stage.mkdir()
        for name, content in sorted(documents.items()):
            path = stage / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(stage, target)
    finally:
        shutil.rmtree(stage_parent, ignore_errors=True)


def _write_one_file(path: Path, content: bytes) -> None:
    _require(not path.exists(), f"output file already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    stage_parent = Path(
        tempfile.mkdtemp(prefix=".full-flow-binding-", dir=path.parent)
    )
    stage = stage_parent / path.name
    try:
        stage.write_bytes(content)
        os.replace(stage, path)
    finally:
        shutil.rmtree(stage_parent, ignore_errors=True)


def _read_checksums(
    root: Path,
    expected: frozenset[str],
    label: str,
) -> dict[str, bytes]:
    _require(root.is_dir(), f"{label} directory does not exist: {root}")
    actual = {
        path.name
        for path in root.iterdir()
        if path.is_file() and path.name != "SHA256SUMS"
    }
    _require(actual == expected, f"{label} file set changed")
    try:
        rows = (root / "SHA256SUMS").read_text(
            encoding="utf-8"
        ).splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshFullFlowTrialError(
            f"{label} checksum file is unreadable"
        ) from exc
    observed: dict[str, str] = {}
    for row in rows:
        digest, separator, name = row.partition("  ")
        _require(
            separator == "  "
            and name in expected
            and _SHA256.fullmatch(digest) is not None,
            f"invalid {label} checksum row",
        )
        _require(name not in observed, f"duplicate {label} checksum row")
        observed[name] = digest
    _require(set(observed) == expected, f"{label} checksums are incomplete")
    documents: dict[str, bytes] = {}
    for name, digest in observed.items():
        try:
            content = (root / name).read_bytes()
        except OSError as exc:
            raise FlowMeshFullFlowTrialError(
                f"{label} file is unreadable: {name}"
            ) from exc
        _require(
            _sha256_bytes(content) == digest,
            f"{label} checksum mismatch: {name}",
        )
        documents[name] = content
    return documents


def _assert_endpoint_free(value: Any, *, path: str = "$") -> None:
    forbidden = {
        "api_key",
        "authorization",
        "base_url",
        "password",
        "secret",
        "token",
        "url",
        "uri",
    }
    if isinstance(value, Mapping):
        for key, child in value.items():
            _require(
                str(key).casefold() not in forbidden,
                f"logical plan contains forbidden field {path}.{key}",
            )
            _assert_endpoint_free(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_endpoint_free(child, path=f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.casefold()
        _require(
            "http://" not in lowered
            and "https://" not in lowered
            and not lowered.startswith("bearer "),
            f"logical plan contains endpoint or bearer data at {path}",
        )


def _assert_safe_result(value: Any, *, path: str = "$") -> None:
    forbidden_parts = (
        "api_key",
        "authorization",
        "bearer",
        "password",
        "secret",
        "token",
        "url",
        "endpoint",
        "jpeg_base64",
        "frame_bytes",
        "artifact_payload",
        "artifact_path",
    )
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).casefold()
            _require(
                not any(part in lowered for part in forbidden_parts),
                f"full-flow result contains unsafe field {path}.{key}",
            )
            _assert_safe_result(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_safe_result(child, path=f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.casefold()
        _require(
            "http://" not in lowered
            and "https://" not in lowered
            and not lowered.startswith("bearer "),
            f"full-flow result contains unsafe value at {path}",
        )


def _validate_deployment_binding(value: Any) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "deployment binding must be an object")
    document = dict(value)
    _require(
        set(document) == _DEPLOYMENT_KEYS,
        "full-flow deployment binding field set changed",
    )
    _require(
        document.get("schema_version")
        == FULL_FLOW_DEPLOYMENT_BINDING_SCHEMA_VERSION,
        "unsupported full-flow deployment binding schema",
    )
    _identifier(document.get("deployment_binding_id"), "deployment_binding_id")
    _require(
        document.get("coordinator_node_id") == "N7",
        "full-flow deployment must bind coordinator N7",
    )
    document["worker_alias"] = _identifier(
        document.get("worker_alias"), "worker_alias"
    )
    document["api_task_timeout_seconds"] = _integer(
        document.get("api_task_timeout_seconds"),
        "api_task_timeout_seconds",
        minimum=1,
    )
    origin = _text(document.get("coordinator_api_url"), "coordinator_api_url")
    parsed = urlsplit(origin)
    _require(
        parsed.scheme in {"http", "https"} and parsed.hostname is not None,
        "coordinator_api_url must be an absolute HTTP(S) URL",
    )
    _require(
        parsed.username is None and parsed.password is None,
        "coordinator_api_url must not contain credentials",
    )
    _require(
        parsed.path in {"", "/"} and not parsed.query and not parsed.fragment,
        "coordinator_api_url must be an origin without path/query/fragment",
    )
    _require(
        parsed.scheme == "https"
        or parsed.hostname in {"127.0.0.1", "localhost", "::1"},
        "non-loopback coordinator_api_url must use HTTPS",
    )
    _require(
        document.get("credentials_recorded") is False,
        "full-flow deployment binding records credentials",
    )
    document["coordinator_api_url"] = origin.rstrip("/")
    return document


def build_full_flow_deployment_binding(
    *,
    deployment_binding_id: str,
    coordinator_api_url: str,
    worker_alias: str,
    api_task_timeout_seconds: int,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build, validate, and optionally atomically freeze a safe binding."""

    document = _validate_deployment_binding({
        "schema_version": FULL_FLOW_DEPLOYMENT_BINDING_SCHEMA_VERSION,
        "deployment_binding_id": deployment_binding_id,
        "coordinator_node_id": "N7",
        "coordinator_api_url": coordinator_api_url,
        "worker_alias": worker_alias,
        "api_task_timeout_seconds": api_task_timeout_seconds,
        "credentials_recorded": False,
    })
    if output_path is not None:
        _write_one_file(Path(output_path).resolve(), _json_bytes(document))
    return document


def _load_deployment_binding(path: str | Path) -> dict[str, Any]:
    try:
        raw = Path(path).resolve().read_bytes()
    except OSError as exc:
        raise FlowMeshFullFlowTrialError(
            "full-flow deployment binding is unreadable"
        ) from exc
    return _validate_deployment_binding(
        _strict_json(raw, "full-flow deployment binding")
    )


def _route_config(
    semantic: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> tuple[FullFlowRouteConfig, dict[str, Any]]:
    route = manifest["route"]
    config = FullFlowRouteConfig(
        route_id=str(semantic["data_agent_route_design_id"]),
        requested_location=str(route["source_location"]),
        data_agent_plan_id=str(semantic["data_agent_plan_id"]),
        data_agent_plan_epoch=int(semantic["data_agent_plan_epoch"]),
        source_node_id=str(route["source_node_id"]),
        executor_node_id=str(route["executor_node_id"]),
        inference_node_id=str(route["inference_node_id"]),
    )
    binding = {**config.public_binding(), "scoring_node_id": "N7"}
    _require(set(binding) == _ROUTE_BINDING_KEYS, "route binding fields changed")
    return config, binding


def _load_bound_artifact(
    package_dir: str | Path,
    semantic: Any,
) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    root = Path(package_dir).resolve()
    try:
        verified = verify_full_flow_data_plane_package(root)
        raw = (root / PACKAGE_MANIFEST_NAME).read_bytes()
        manifest = _strict_json(raw, PACKAGE_MANIFEST_NAME)
    except Exception as exc:
        raise FlowMeshFullFlowTrialError(
            "full-flow data-plane package is invalid"
        ) from exc
    _require(
        verified.get("source_node_id") == "N4"
        and verified.get("executor_node_id") == "N7"
        and verified.get("inference_node_id") == "N6",
        "full-flow data-plane route is not N4 -> N7 -> N6",
    )
    rows = [
        row
        for row in manifest.get("objects", [])
        if isinstance(row, Mapping)
        and row.get("object_id") == semantic.document["artifact_object_id"]
    ]
    _require(len(rows) == 1, "semantic object is not uniquely bound in data plane")
    row = dict(rows[0])
    # The portable data-plane package deliberately canonicalizes a semantic
    # spec before storing it.  In particular, a source file written with
    # Windows newlines has a different byte digest from the package's LF-only
    # canonical copy even though the frozen JSON document is identical.  Bind
    # the exact packaged document (and its checked digest), rather than
    # incorrectly requiring the operator's source-file encoding to survive
    # package construction byte for byte.
    matches: list[tuple[Mapping[str, Any], str]] = []
    for item in row.get("semantic_specs", []):
        if not isinstance(item, Mapping):
            continue
        if (
            item.get("semantic_run_id")
            != semantic.document["semantic_run_id"]
            or item.get("trial_key") != semantic.document["trial_key"]
            or item.get("workload_id") != semantic.document["workload_id"]
            or item.get("data_agent_plan_id")
            != semantic.document["data_agent_plan_id"]
        ):
            continue
        package_path = item.get("package_path")
        if not isinstance(package_path, str):
            continue
        try:
            packaged_raw = (root / package_path).read_bytes()
            packaged = _strict_json(packaged_raw, "packaged semantic spec")
        except (OSError, FlowMeshFullFlowTrialError):
            continue
        packaged_sha256 = _sha256_bytes(packaged_raw)
        if (
            item.get("sha256") == packaged_sha256
            and packaged == semantic.document
        ):
            matches.append((item, packaged_sha256))
    _require(
        len(matches) == 1,
        "semantic spec is not uniquely bound to the data-plane object",
    )
    expected = {
        "object_id": row["object_id"],
        "representation_id": row["representation_id"],
        "artifact_sha256": row["artifact_sha256"],
        "artifact_size_bytes": row["artifact_size_bytes"],
        "object_catalog_version": row["catalog_version"],
        "frame_count": row["frame_count"],
        "manifest_sha256": row["manifest_sha256"],
    }
    return manifest, expected, _sha256_bytes(raw), matches[0][1]


def _validate_request(value: Any) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "full-flow request must be an object")
    request = dict(value)
    _require(set(request) == _REQUEST_KEYS, "full-flow request fields changed")
    _require(
        request.get("schema_version") == FULL_FLOW_REQUEST_SCHEMA_VERSION,
        "unsupported N7 full-flow request schema",
    )
    _require(
        request.get("credentials_recorded") is False,
        "request records credentials",
    )
    _require(
        request.get("representation_id") == "sampled_frame_bundle",
        "full-flow request has the wrong representation",
    )
    for name in ("artifact_sha256", "frozen_binding_sha256"):
        _digest(request.get(name), name)
    _integer(request.get("artifact_size_bytes"), "artifact_size_bytes", minimum=1)
    _integer(request.get("event_index"), "event_index")
    _assert_endpoint_free(request)
    return request


def _logical_plan(
    *,
    semantic: Any,
    semantic_spec_sha256: str,
    manifest: Mapping[str, Any],
    manifest_sha256: str,
    expected_artifact: Mapping[str, Any],
    owner: str,
) -> dict[str, Any]:
    route_config, route_binding = _route_config(semantic.document, manifest)
    source = semantic.document
    request = build_full_flow_trial_request(
        route_config=route_config,
        full_flow_request_id=source["semantic_run_id"],
        run_id=source["semantic_run_id"],
        trial_id=source["semantic_run_id"],
        trial_key=source["trial_key"],
        workload_id=source["workload_id"],
        task_class_id=source["task_class_id"],
        object_id=source["artifact_object_id"],
        artifact_sha256=source["artifact_sha256"],
        artifact_size_bytes=source["artifact_size_bytes"],
        object_catalog_version=source["object_catalog_version"],
        expected_model=source["expected_model"],
        question=source["question"],
        answer_options=source["answer_options"],
        correct_answer_id=source["correct_answer_id"],
        success_scoring_rule=source["success_scoring_rule"],
        event_index=0,
    )
    _validate_request(request)
    for request_name, artifact_name in (
        ("object_id", "object_id"),
        ("representation_id", "representation_id"),
        ("artifact_sha256", "artifact_sha256"),
        ("artifact_size_bytes", "artifact_size_bytes"),
        ("object_catalog_version", "object_catalog_version"),
    ):
        _require(
            request[request_name] == expected_artifact[artifact_name],
            f"semantic request and data plane disagree on {request_name}",
        )
    plan = {
        "schema_version": FULL_FLOW_LOGICAL_PLAN_SCHEMA_VERSION,
        "status": "FROZEN",
        "logical_plan_id": request["full_flow_request_id"],
        "owner": _identifier(owner, "owner"),
        # This is the digest of the checked, canonical spec inside the
        # data-plane package.  The original host file is not a runtime input.
        "semantic_spec_sha256": semantic_spec_sha256,
        "data_plane_manifest_sha256": manifest_sha256,
        "request_body": request,
        "route_binding": route_binding,
        "expected_artifact": dict(expected_artifact),
        "required_result_claims": dict(_REQUIRED_RESULT_CLAIMS),
        "evidence_class": "flowmesh-unified-pathfinder-full-flow-smoke",
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
    return _validate_logical_plan(plan)


def _validate_logical_plan(value: Any) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "logical plan must be an object")
    plan = dict(value)
    _require(set(plan) == _LOGICAL_PLAN_KEYS, "logical plan fields changed")
    _require(
        plan.get("schema_version") == FULL_FLOW_LOGICAL_PLAN_SCHEMA_VERSION,
        "unsupported full-flow logical plan schema",
    )
    _require(plan.get("status") == "FROZEN", "logical plan is not frozen")
    _identifier(plan.get("logical_plan_id"), "logical_plan_id")
    _identifier(plan.get("owner"), "owner")
    _digest(plan.get("semantic_spec_sha256"), "semantic_spec_sha256")
    _digest(plan.get("data_plane_manifest_sha256"), "data_plane_manifest_sha256")
    request = _validate_request(plan.get("request_body"))
    _require(
        request["full_flow_request_id"] == plan["logical_plan_id"],
        "logical plan ID differs from its N7 request",
    )
    route = plan.get("route_binding")
    _require(
        isinstance(route, Mapping) and set(route) == _ROUTE_BINDING_KEYS,
        "logical route binding fields changed",
    )
    route_config = FullFlowRouteConfig(
        route_id=route["route_id"],
        requested_location=route["requested_location"],
        data_agent_plan_id=route["data_agent_plan_id"],
        data_agent_plan_epoch=route["data_agent_plan_epoch"],
        source_node_id=route["source_node_id"],
        executor_node_id=route["executor_node_id"],
        inference_node_id=route["inference_node_id"],
        quiescence_timeout_seconds=route["quiescence_timeout_seconds"],
    )
    _require(route.get("scoring_node_id") == "N7", "scoring node must be N7")
    expected_request = build_full_flow_trial_request(
        route_config=route_config,
        full_flow_request_id=request["full_flow_request_id"],
        run_id=request["run_id"],
        trial_id=request["trial_id"],
        trial_key=request["trial_key"],
        workload_id=request["workload_id"],
        task_class_id=request["task_class_id"],
        object_id=request["object_id"],
        artifact_sha256=request["artifact_sha256"],
        artifact_size_bytes=request["artifact_size_bytes"],
        object_catalog_version=request["object_catalog_version"],
        expected_model=request["expected_model"],
        question=request["question"],
        answer_options=request["answer_options"],
        correct_answer_id=request["correct_answer_id"],
        success_scoring_rule=request["success_scoring_rule"],
        event_index=request["event_index"],
    )
    _require(request == expected_request, "N7 request frozen binding changed")
    artifact = plan.get("expected_artifact")
    _require(
        isinstance(artifact, Mapping)
        and set(artifact) == _EXPECTED_ARTIFACT_KEYS,
        "expected artifact fields changed",
    )
    _require(
        artifact["object_id"] == request["object_id"]
        and artifact["representation_id"] == request["representation_id"]
        and artifact["artifact_sha256"] == request["artifact_sha256"]
        and artifact["artifact_size_bytes"] == request["artifact_size_bytes"]
        and artifact["object_catalog_version"]
        == request["object_catalog_version"],
        "expected artifact differs from N7 request",
    )
    _digest(artifact["manifest_sha256"], "manifest_sha256")
    _integer(artifact["frame_count"], "frame_count", minimum=1)
    _require(
        plan.get("required_result_claims") == _REQUIRED_RESULT_CLAIMS,
        "required result claims changed",
    )
    for name in (
        "workflow_submitted",
        "credentials_recorded",
        "eligible_for_awm_oed",
        "eligible_for_scientific_claims",
    ):
        _require(plan.get(name) is False, f"logical plan {name} must be false")
    _require(
        plan.get("evidence_class")
        == "flowmesh-unified-pathfinder-full-flow-smoke",
        "logical plan evidence class changed",
    )
    _require(
        plan.get("plan_sha256") == _document_sha256(plan, "plan_sha256"),
        "logical plan digest mismatch",
    )
    _assert_endpoint_free(plan)
    return plan


def _task_spec(
    request: Mapping[str, Any],
    deployment: Mapping[str, Any],
    *,
    full_flow_hmac_secret: str | None = None,
) -> dict[str, Any]:
    if request.get("schema_version") == FULL_FLOW_REQUEST_V2_SCHEMA_VERSION:
        # This check sits immediately at the FlowMesh serialization boundary.
        # A caller cannot bypass it by constructing a request mapping directly.
        assert_hidden_oracle_fields_absent(request)
    headers = {"Content-Type": "application/json"}
    if full_flow_hmac_secret is not None:
        headers[FULL_FLOW_INGRESS_SIGNATURE_HEADER] = (
            full_flow_request_hmac_sha256(request, full_flow_hmac_secret)
        )
    return {
        "taskType": "api",
        "api": {
            "url": (
                deployment["coordinator_api_url"]
                + "/v1/pathfinder/trials/execute"
            ),
            "method": "POST",
            "headers": headers,
            "body": json.loads(json.dumps(request)),
            "timeout_sec": deployment["api_task_timeout_seconds"],
            "response": {
                "parse_json": False,
                "return_body": True,
                "raise_for_status": True,
                "max_body_bytes": 2 * 1024 * 1024,
            },
        },
        "output": {"destination": {"type": "http"}, "artifacts": []},
    }


def build_flowmesh_full_flow_trial_workflow(
    logical_plan: Mapping[str, Any],
    deployment_binding: Mapping[str, Any],
    *,
    selected_worker_id: str | None,
    full_flow_hmac_secret: str | None = None,
) -> dict[str, Any]:
    """Build the one-node APITask template or worker-pinned workflow."""

    plan = _validate_logical_plan(logical_plan)
    deployment = _validate_deployment_binding(deployment_binding)
    template = selected_worker_id is None
    custom: dict[str, Any] = {
        "pathfinder_full_flow_request_id": plan["logical_plan_id"],
        "pathfinder_logical_plan_sha256": plan["plan_sha256"],
        "pathfinder_deployment_binding_sha256": _sha256_bytes(
            _canonical_bytes(deployment)
        ),
        "pathfinder_route_unified_required": True,
        "pathfinder_host_artifact_materialization_forbidden": True,
    }
    annotations: dict[str, Any] = {"custom": custom}
    if template:
        custom.update({
            "pathfinder_worker_alias_to_resolve_at_submission": deployment[
                "worker_alias"
            ],
            "pathfinder_workflow_is_not_submittable": True,
        })
    else:
        annotations["schedule_hint"] = {
            "selected_worker": _identifier(
                selected_worker_id,
                "selected_worker_id",
            )
        }
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": "pathfinder-full-flow-" + plan["logical_plan_id"][:36],
            "owner": plan["owner"],
            "annotations": annotations,
        },
        "spec": {
            "graph": {
                "nodes": [{
                    "name": "pathfinder-full-flow-trial",
                    "spec": _task_spec(
                        plan["request_body"],
                        deployment,
                        full_flow_hmac_secret=full_flow_hmac_secret,
                    ),
                }]
            }
        },
    }


def build_flowmesh_full_flow_trial_v2_workflow(
    public_request: Mapping[str, Any],
    route_config: FullFlowRouteConfig,
    deployment_binding: Mapping[str, Any],
    *,
    owner: str = "pathfinder",
    selected_worker_id: str | None,
    full_flow_hmac_secret: str | None = None,
) -> dict[str, Any]:
    """Build a label-free v2 APITask without creating a legacy v1 plan."""

    request = validate_full_flow_trial_request_v2(
        public_request,
        route_config=route_config,
    )
    deployment = _validate_deployment_binding(deployment_binding)
    # Deliberately repeat this at the last public boundary.  Future request
    # validation changes must not make hidden-label admission implicit.
    assert_hidden_oracle_fields_absent(request)
    template = selected_worker_id is None
    custom: dict[str, Any] = {
        "pathfinder_full_flow_request_id": request["full_flow_request_id"],
        "pathfinder_public_request_sha256": _sha256_bytes(
            _canonical_bytes(request)
        ),
        "pathfinder_task_binding_sha256": request["task_binding_sha256"],
        "pathfinder_hidden_oracle_required": True,
        "pathfinder_host_artifact_materialization_forbidden": True,
    }
    annotations: dict[str, Any] = {"custom": custom}
    if template:
        custom.update({
            "pathfinder_worker_alias_to_resolve_at_submission": deployment[
                "worker_alias"
            ],
            "pathfinder_workflow_is_not_submittable": True,
        })
    else:
        annotations["schedule_hint"] = {
            "selected_worker": _identifier(
                selected_worker_id,
                "selected_worker_id",
            )
        }
    workflow = {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": "pathfinder-full-flow-v2-"
            + request["full_flow_request_id"][:33],
            "owner": _identifier(owner, "owner"),
            "annotations": annotations,
        },
        "spec": {
            "graph": {
                "nodes": [{
                    "name": "pathfinder-full-flow-v2-trial",
                    "spec": _task_spec(
                        request,
                        deployment,
                        full_flow_hmac_secret=full_flow_hmac_secret,
                    ),
                }]
            }
        },
    }
    assert_hidden_oracle_fields_absent(workflow)
    return workflow


def _load_public_artifact_v2(
    package_dir: str | Path,
    request: Mapping[str, Any],
    route_config: FullFlowRouteConfig,
) -> tuple[dict[str, Any], str]:
    """Load one public artifact binding without reading a semantic label."""

    root = Path(package_dir).resolve()
    try:
        verified = verify_full_flow_data_plane_package(root)
        raw = (root / PACKAGE_MANIFEST_NAME).read_bytes()
        manifest = _strict_json(raw, PACKAGE_MANIFEST_NAME)
    except Exception as exc:
        raise FlowMeshFullFlowTrialError(
            "full-flow data-plane package is invalid"
        ) from exc
    _require(
        verified.get("source_node_id") == "N4"
        and verified.get("executor_node_id") == "N7"
        and verified.get("inference_node_id") == "N6",
        "full-flow data-plane route is not N4 -> N7 -> N6",
    )
    manifest_route = manifest.get("route")
    _require(isinstance(manifest_route, Mapping), "data-plane route is missing")
    _require(
        manifest_route.get("source_node_id") == route_config.source_node_id
        and manifest_route.get("executor_node_id")
        == route_config.executor_node_id
        and manifest_route.get("inference_node_id")
        == route_config.inference_node_id
        and manifest_route.get("source_location")
        == route_config.requested_location,
        "public request route differs from the data-plane package",
    )
    rows = [
        row
        for row in manifest.get("objects", [])
        if isinstance(row, Mapping)
        and row.get("object_id") == request["object_id"]
        and row.get("representation_id") == request["representation_id"]
    ]
    _require(len(rows) == 1, "public object is not uniquely bound in data plane")
    row = rows[0]
    expected = {
        "object_id": row["object_id"],
        "representation_id": row["representation_id"],
        "artifact_sha256": row["artifact_sha256"],
        "artifact_size_bytes": row["artifact_size_bytes"],
        "object_catalog_version": row["catalog_version"],
        "frame_count": row["frame_count"],
        "manifest_sha256": row["manifest_sha256"],
    }
    for request_name in (
        "object_id",
        "representation_id",
        "artifact_sha256",
        "artifact_size_bytes",
        "object_catalog_version",
    ):
        _require(
            request[request_name] == expected[request_name],
            f"public request and data plane disagree on {request_name}",
        )
    return expected, _sha256_bytes(raw)


def _validate_logical_plan_v2(value: Any) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "v2 logical plan must be an object")
    plan = dict(value)
    _require(
        set(plan) == _LOGICAL_PLAN_V2_KEYS,
        "v2 logical plan fields changed",
    )
    _require(
        plan.get("schema_version") == FULL_FLOW_LOGICAL_PLAN_V2_SCHEMA_VERSION,
        "unsupported v2 full-flow logical plan schema",
    )
    _require(plan.get("status") == "FROZEN", "v2 logical plan is not frozen")
    _identifier(plan.get("logical_plan_id"), "logical_plan_id")
    _identifier(plan.get("owner"), "owner")
    _digest(plan.get("data_plane_manifest_sha256"), "data plane manifest")
    task_binding = _digest(
        plan.get("public_task_binding_sha256"),
        "public_task_binding_sha256",
    )
    oracle_id = _identifier(plan.get("oracle_id"), "oracle_id")
    route = plan.get("route_binding")
    _require(
        isinstance(route, Mapping) and set(route) == _ROUTE_BINDING_KEYS,
        "v2 logical route binding fields changed",
    )
    route_config = FullFlowRouteConfig(
        route_id=route["route_id"],
        requested_location=route["requested_location"],
        data_agent_plan_id=route["data_agent_plan_id"],
        data_agent_plan_epoch=route["data_agent_plan_epoch"],
        source_node_id=route["source_node_id"],
        executor_node_id=route["executor_node_id"],
        inference_node_id=route["inference_node_id"],
        quiescence_timeout_seconds=route["quiescence_timeout_seconds"],
    )
    _require(route.get("scoring_node_id") == N1_NODE_ID, "v2 scoring node is not N1")
    request = validate_full_flow_trial_request_v2(
        plan.get("request_body"),
        route_config=route_config,
    )
    _require(
        request["full_flow_request_id"] == plan["logical_plan_id"]
        and request["task_binding_sha256"] == task_binding
        and request["oracle_id"] == oracle_id,
        "v2 plan differs from its public task or oracle binding",
    )
    artifact = plan.get("expected_artifact")
    _require(
        isinstance(artifact, Mapping)
        and set(artifact) == _EXPECTED_ARTIFACT_KEYS,
        "v2 expected artifact fields changed",
    )
    for name in (
        "object_id",
        "representation_id",
        "artifact_sha256",
        "artifact_size_bytes",
        "object_catalog_version",
    ):
        _require(
            artifact.get(name) == request[name],
            f"v2 expected artifact changed {name}",
        )
    _digest(artifact.get("manifest_sha256"), "manifest_sha256")
    _integer(artifact.get("frame_count"), "frame_count", minimum=1)
    _require(
        plan.get("required_result_claims") == _REQUIRED_RESULT_CLAIMS,
        "v2 required result claims changed",
    )
    for name in (
        "workflow_submitted",
        "credentials_recorded",
        "eligible_for_awm_oed",
        "eligible_for_scientific_claims",
    ):
        _require(plan.get(name) is False, f"v2 logical plan {name} must be false")
    _require(
        plan.get("evidence_class") == _V2_EVIDENCE_CLASS,
        "v2 logical plan evidence class changed",
    )
    _require(
        plan.get("plan_sha256") == _document_sha256(plan, "plan_sha256"),
        "v2 logical plan digest mismatch",
    )
    assert_hidden_oracle_fields_absent(plan)
    _assert_endpoint_free(plan)
    return plan


def plan_flowmesh_full_flow_trial_v2(
    *,
    public_request: Mapping[str, Any],
    route_config: FullFlowRouteConfig,
    data_plane_package: str | Path,
    deployment_binding: Mapping[str, Any] | str | Path,
    output_dir: str | Path,
    owner: str = "pathfinder",
) -> dict[str, Any]:
    """Freeze endpoint-free public v2 inputs and a separate deployment binding."""

    request = validate_full_flow_trial_request_v2(
        public_request,
        route_config=route_config,
    )
    expected_artifact, manifest_sha256 = _load_public_artifact_v2(
        data_plane_package,
        request,
        route_config,
    )
    if isinstance(deployment_binding, Mapping):
        deployment = _validate_deployment_binding(deployment_binding)
    else:
        deployment = _load_deployment_binding(deployment_binding)
    route_binding = {
        **route_config.public_binding(),
        "scoring_node_id": N1_NODE_ID,
    }
    plan = {
        "schema_version": FULL_FLOW_LOGICAL_PLAN_V2_SCHEMA_VERSION,
        "status": "FROZEN",
        "logical_plan_id": request["full_flow_request_id"],
        "owner": _identifier(owner, "owner"),
        "data_plane_manifest_sha256": manifest_sha256,
        "public_task_binding_sha256": request["task_binding_sha256"],
        "oracle_id": request["oracle_id"],
        "request_body": request,
        "route_binding": route_binding,
        "expected_artifact": expected_artifact,
        "required_result_claims": dict(_REQUIRED_RESULT_CLAIMS),
        "evidence_class": _V2_EVIDENCE_CLASS,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
    plan = _validate_logical_plan_v2(plan)
    template = build_flowmesh_full_flow_trial_v2_workflow(
        request,
        route_config,
        deployment,
        owner=plan["owner"],
        selected_worker_id=None,
    )
    documents = {
        _LOGICAL_V2_FILE: _json_bytes(plan),
        _DEPLOYMENT_V2_FILE: _json_bytes(deployment),
        _TEMPLATE_V2_FILE: _json_bytes(template),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {
        "status": "FROZEN_FULL_FLOW_TRIAL_V2",
        "output_dir": str(target),
        "logical_plan_id": plan["logical_plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "public_task_binding_sha256": plan["public_task_binding_sha256"],
        "oracle_id": plan["oracle_id"],
        "worker_alias": deployment["worker_alias"],
        "source_node_id": "N4",
        "execution_node_id": "N7",
        "semantic_executor_node_id": "N6",
        "scoring_node_id": N1_NODE_ID,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }


def _read_plan_v2(
    plan_dir: str | Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    documents = _read_checksums(
        Path(plan_dir).resolve(),
        _PLAN_V2_FILES,
        "v2 full-flow plan",
    )
    plan = _validate_logical_plan_v2(
        _strict_json(documents[_LOGICAL_V2_FILE], "v2 logical plan")
    )
    deployment = _validate_deployment_binding(
        _strict_json(documents[_DEPLOYMENT_V2_FILE], "deployment binding")
    )
    route = plan["route_binding"]
    route_config = FullFlowRouteConfig(
        route_id=route["route_id"],
        requested_location=route["requested_location"],
        data_agent_plan_id=route["data_agent_plan_id"],
        data_agent_plan_epoch=route["data_agent_plan_epoch"],
        source_node_id=route["source_node_id"],
        executor_node_id=route["executor_node_id"],
        inference_node_id=route["inference_node_id"],
        quiescence_timeout_seconds=route["quiescence_timeout_seconds"],
    )
    template = _strict_json(documents[_TEMPLATE_V2_FILE], "workflow template")
    _require(
        template
        == build_flowmesh_full_flow_trial_v2_workflow(
            plan["request_body"],
            route_config,
            deployment,
            owner=plan["owner"],
            selected_worker_id=None,
        ),
        "v2 full-flow workflow template changed",
    )
    assert_hidden_oracle_fields_absent(documents)
    return plan, deployment


def verify_flowmesh_full_flow_trial_v2_plan(
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Offline-verify a label-free v2 plan without contacting N1."""

    plan, deployment = _read_plan_v2(plan_dir)
    route = plan["route_binding"]
    return {
        "status": "VERIFIED",
        "schema_version": plan["schema_version"],
        "logical_plan_id": plan["logical_plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "public_task_binding_sha256": plan["public_task_binding_sha256"],
        "oracle_id": plan["oracle_id"],
        "deployment_binding_sha256": _sha256_bytes(
            _canonical_bytes(deployment)
        ),
        "worker_alias": deployment["worker_alias"],
        "source_node_id": route["source_node_id"],
        "execution_node_id": route["executor_node_id"],
        "semantic_executor_node_id": route["inference_node_id"],
        "scoring_node_id": route["scoring_node_id"],
        "object_id": plan["expected_artifact"]["object_id"],
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }


def plan_flowmesh_full_flow_trial(
    *,
    semantic_spec: str | Path,
    data_plane_package: str | Path,
    deployment_binding: str | Path,
    output_dir: str | Path,
    owner: str = "pathfinder",
) -> dict[str, Any]:
    """Freeze an exact N7 request plus a separate submission binding."""

    try:
        semantic = load_data_agent_frame_bundle_semantic_spec(semantic_spec)
    except Exception as exc:
        raise FlowMeshFullFlowTrialError("semantic spec is invalid") from exc
    (
        manifest,
        expected_artifact,
        manifest_sha256,
        semantic_spec_sha256,
    ) = _load_bound_artifact(data_plane_package, semantic)
    deployment = _load_deployment_binding(deployment_binding)
    plan = _logical_plan(
        semantic=semantic,
        semantic_spec_sha256=semantic_spec_sha256,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        expected_artifact=expected_artifact,
        owner=owner,
    )
    template = build_flowmesh_full_flow_trial_workflow(
        plan,
        deployment,
        selected_worker_id=None,
    )
    documents = {
        _LOGICAL_FILE: _json_bytes(plan),
        _DEPLOYMENT_FILE: _json_bytes(deployment),
        _TEMPLATE_FILE: _json_bytes(template),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {
        "status": "FROZEN_FULL_FLOW_TRIAL",
        "output_dir": str(target),
        "logical_plan_id": plan["logical_plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "deployment_binding_sha256": _sha256_bytes(
            _canonical_bytes(deployment)
        ),
        "worker_alias": deployment["worker_alias"],
        "source_node_id": "N4",
        "execution_node_id": "N7",
        "semantic_executor_node_id": "N6",
        "scoring_node_id": "N7",
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }


def _read_plan(
    plan_dir: str | Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    documents = _read_checksums(
        Path(plan_dir).resolve(),
        _PLAN_FILES,
        "full-flow plan",
    )
    plan = _validate_logical_plan(
        _strict_json(documents[_LOGICAL_FILE], "logical plan")
    )
    deployment = _validate_deployment_binding(
        _strict_json(documents[_DEPLOYMENT_FILE], "deployment binding")
    )
    template = _strict_json(documents[_TEMPLATE_FILE], "workflow template")
    _require(
        template
        == build_flowmesh_full_flow_trial_workflow(
            plan,
            deployment,
            selected_worker_id=None,
        ),
        "full-flow workflow template changed",
    )
    return plan, deployment


def verify_flowmesh_full_flow_trial_plan(
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Verify a plan offline without calling FlowMesh or a service."""

    plan, deployment = _read_plan(plan_dir)
    route = plan["route_binding"]
    return {
        "status": "VERIFIED",
        "schema_version": plan["schema_version"],
        "logical_plan_id": plan["logical_plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "deployment_binding_sha256": _sha256_bytes(
            _canonical_bytes(deployment)
        ),
        "worker_alias": deployment["worker_alias"],
        "source_node_id": route["source_node_id"],
        "execution_node_id": route["executor_node_id"],
        "semantic_executor_node_id": route["inference_node_id"],
        "scoring_node_id": route["scoring_node_id"],
        "object_id": plan["expected_artifact"]["object_id"],
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }


def _validate_delivery(value: Any, request: Mapping[str, Any]) -> None:
    _require(
        isinstance(value, Mapping) and set(value) == _DELIVERY_KEYS,
        "Data Agent delivery fields changed",
    )
    for name in (
        "telemetry_supported",
        "telemetry_complete",
        "exactly_one_full_download",
        "bytes_sent_equals_artifact_size",
    ):
        _require(value.get(name) is True, f"Data Agent delivery {name} is false")
    _require(
        value.get("download_request_count") == 1
        and value.get("completed_request_count") == 1
        and value.get("full_download_count") == 1,
        "Data Agent delivery is not exactly one completed full download",
    )
    _require(
        value.get("in_flight_request_count") == 0,
        "Data Agent delivery still has an in-flight request",
    )
    _require(
        value.get("bytes_sent") == request["artifact_size_bytes"]
        and value.get("artifact_size_bytes") == request["artifact_size_bytes"],
        "Data Agent byte accounting differs from the artifact",
    )
    _require(
        value.get("telemetry_object_id") == request["object_id"]
        and value.get("telemetry_object_catalog_version")
        == request["object_catalog_version"]
        and value.get("expected_object_catalog_version")
        == request["object_catalog_version"],
        "Data Agent telemetry identity changed",
    )
    server_latency = value.get("server_reported_transfer_latency_ms")
    if server_latency is not None:
        _number(server_latency, "server_reported_transfer_latency_ms")


def _validate_evidence(
    value: Any,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "N7 full-flow result is not an object")
    evidence = dict(value)
    _require(set(evidence) == _EVIDENCE_KEYS, "N7 evidence fields changed")
    _require(
        evidence.get("schema_version") == FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
        "unsupported N7 full-flow evidence schema",
    )
    _require(evidence.get("status") == "COMPLETE", "N7 trial did not complete")
    request = plan["request_body"]
    for name in (
        "full_flow_request_id",
        "frozen_binding_sha256",
        "run_id",
        "trial_id",
        "trial_key",
        "workload_id",
        "task_class_id",
        "object_id",
        "representation_id",
    ):
        _require(evidence.get(name) == request[name], f"N7 changed {name}")
    _require(
        evidence.get("request_sha256")
        == _sha256_bytes(_canonical_bytes(request)),
        "N7 request digest changed",
    )
    for name, expected in _REQUIRED_RESULT_CLAIMS.items():
        _require(
            evidence.get(name) is expected,
            f"N7 evidence does not verify {name}",
        )
    _require(
        type(evidence.get("idempotent_replay")) is bool,
        "N7 idempotent replay flag is invalid",
    )
    route = evidence.get("route")
    _require(
        isinstance(route, Mapping) and set(route) == _EVIDENCE_ROUTE_KEYS,
        "N7 route evidence fields changed",
    )
    expected_route = plan["route_binding"]
    for result_name, plan_name in (
        ("route_id", "route_id"),
        ("source_node_id", "source_node_id"),
        ("executor_node_id", "executor_node_id"),
        ("inference_node_id", "inference_node_id"),
        ("requested_location", "requested_location"),
    ):
        _require(
            route.get(result_name) == expected_route[plan_name],
            f"N7 route changed {result_name}",
        )
    route_config = FullFlowRouteConfig(
        route_id=expected_route["route_id"],
        requested_location=expected_route["requested_location"],
        data_agent_plan_id=expected_route["data_agent_plan_id"],
        data_agent_plan_epoch=expected_route["data_agent_plan_epoch"],
        source_node_id=expected_route["source_node_id"],
        executor_node_id=expected_route["executor_node_id"],
        inference_node_id=expected_route["inference_node_id"],
        quiescence_timeout_seconds=expected_route[
            "quiescence_timeout_seconds"
        ],
    )
    _require(
        evidence.get("route_config_sha256") == route_config.sha256,
        "N7 route configuration digest changed",
    )
    data_agent = evidence.get("data_agent")
    _require(
        isinstance(data_agent, Mapping)
        and set(data_agent) == _EVIDENCE_DATA_AGENT_KEYS,
        "Data Agent evidence fields changed",
    )
    expected_artifact = plan["expected_artifact"]
    expected_data = {
        "source_node_id": expected_route["source_node_id"],
        "source_identity_basis": "data-agent-health-before-and-after",
        "source_identity_verified": True,
        "plan_id": expected_route["data_agent_plan_id"],
        "plan_epoch": expected_route["data_agent_plan_epoch"],
        "object_catalog_version": request["object_catalog_version"],
        "artifact_media_type": FRAME_BUNDLE_MEDIA_TYPE,
        "artifact_size_bytes": request["artifact_size_bytes"],
        "artifact_sha256": request["artifact_sha256"],
        "manifest_sha256": expected_artifact["manifest_sha256"],
        "frame_count": expected_artifact["frame_count"],
    }
    for name, expected in expected_data.items():
        _require(
            data_agent.get(name) == expected,
            f"Data Agent evidence changed {name}",
        )
    for name in ("health_sha256_before", "health_sha256_after"):
        _digest(data_agent.get(name), f"Data Agent {name}")
    _require(
        data_agent["health_sha256_before"]
        == data_agent["health_sha256_after"],
        "Data Agent health changed during artifact delivery",
    )
    _text(data_agent.get("access_id"), "Data Agent access_id")
    _integer(data_agent.get("total_jpeg_bytes"), "total_jpeg_bytes", minimum=1)
    _validate_delivery(data_agent.get("delivery"), request)
    latency = data_agent.get("latency_ms")
    _require(
        isinstance(latency, Mapping) and set(latency) == _LATENCY_KEYS,
        "Data Agent latency fields changed",
    )
    for name, number in latency.items():
        if number is not None:
            _number(number, f"Data Agent latency {name}")
    try:
        contract = load_workload_scoring_contract(
            {
                "object_id": request["object_id"],
                "question": request["question"],
                "answer_options": request["answer_options"],
                "correct_answer_id": request["correct_answer_id"],
            },
            request["success_scoring_rule"],
            name="full-flow offline verifier",
        )
        rendered_question = render_workload_question(
            {"question": request["question"]},
            contract,
        )
    except Exception as exc:
        raise FlowMeshFullFlowTrialError(
            "cannot reconstruct frozen semantic question"
        ) from exc
    semantic = evidence.get("semantic")
    _require(
        isinstance(semantic, Mapping) and set(semantic) == _SEMANTIC_KEYS,
        "semantic evidence fields changed",
    )
    for name in ("request_sha256", "frame_sequence_sha256"):
        _digest(semantic.get(name), f"semantic {name}")
    question_sha256 = _sha256_bytes(rendered_question.encode("utf-8"))
    _require(
        semantic.get("question_sha256") == question_sha256,
        "semantic question digest differs from frozen scoring input",
    )
    _require(
        semantic.get("model") == request["expected_model"],
        "semantic model differs from frozen model",
    )
    _require(
        isinstance(semantic.get("runtime_epoch"), str)
        and _RUNTIME_EPOCH.fullmatch(semantic["runtime_epoch"]) is not None,
        "semantic runtime epoch is invalid",
    )
    _require(
        type(semantic.get("adapter_idempotent_replay")) is bool,
        "semantic replay flag is invalid",
    )
    _number(semantic.get("service_time_ms"), "semantic service_time_ms")
    frame_metadata = semantic.get("frame_metadata")
    _require(isinstance(frame_metadata, list), "semantic frame metadata changed")
    _require(
        len(frame_metadata) == expected_artifact["frame_count"],
        "semantic frame metadata count changed",
    )
    total_frame_bytes = 0
    previous_timestamp = -1.0
    for index, raw_frame in enumerate(frame_metadata):
        _require(
            isinstance(raw_frame, Mapping)
            and set(raw_frame) == _FRAME_METADATA_KEYS,
            f"semantic frame metadata {index} fields changed",
        )
        _require(
            raw_frame.get("frame_index") == index,
            "semantic frame indexes are not contiguous",
        )
        timestamp = _number(
            raw_frame.get("timestamp_seconds"),
            f"semantic frame {index} timestamp_seconds",
        )
        _require(
            timestamp >= previous_timestamp,
            "semantic frame timestamps are not monotonic",
        )
        previous_timestamp = timestamp
        _integer(raw_frame.get("width"), "semantic frame width", minimum=1)
        _integer(raw_frame.get("height"), "semantic frame height", minimum=1)
        total_frame_bytes += _integer(
            raw_frame.get("jpeg_size_bytes"),
            "semantic frame jpeg_size_bytes",
            minimum=1,
        )
        _digest(raw_frame.get("jpeg_sha256"), "semantic frame jpeg_sha256")
    _require(
        semantic.get("representation_delivery_bytes") == total_frame_bytes
        == data_agent["total_jpeg_bytes"],
        "semantic representation byte count differs from Data Agent bundle",
    )
    prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
        request["representation_id"],
        len(frame_metadata),
        rendered_question,
    )
    prompt_sha256 = _sha256_bytes(prompt.encode("utf-8"))
    _require(
        semantic.get("prompt_sha256") == prompt_sha256,
        "semantic prompt digest differs from frozen request",
    )
    semantic_request_id = _sha256_bytes(_canonical_bytes({
        "full_flow_request_id": request["full_flow_request_id"],
        "trial_key": request["trial_key"],
        "execution_node_id": expected_route["inference_node_id"],
        "representation_id": request["representation_id"],
        "representation_sha256": request["artifact_sha256"],
        "question_sha256": question_sha256,
        "prompt_sha256": prompt_sha256,
        "frame_sequence_sha256": semantic["frame_sequence_sha256"],
    }))
    _require(
        semantic.get("semantic_request_id") == semantic_request_id,
        "semantic request identity differs from frozen route and inputs",
    )
    scoring = evidence.get("scoring")
    _require(
        isinstance(scoring, Mapping) and set(scoring) == _SCORING_KEYS,
        "scoring evidence fields changed",
    )
    _require(
        scoring.get("success_scoring_rule") == request["success_scoring_rule"]
        and scoring.get("correct_answer_id") == request["correct_answer_id"],
        "scoring contract changed",
    )
    option_ids = [item["option_id"] for item in request["answer_options"]]
    _require(scoring.get("answer_option_ids") == option_ids, "option IDs changed")
    _require(
        scoring.get("answer_options_sha256")
        == _sha256_bytes(_canonical_bytes(request["answer_options"])),
        "answer options digest changed",
    )
    answer = _text(scoring.get("final_answer"), "final_answer")
    _require(
        scoring.get("final_answer_sha256")
        == _sha256_bytes(answer.encode("utf-8")),
        "final answer digest changed",
    )
    try:
        success = evaluate_workload_answer(answer, contract)
    except Exception as exc:
        raise FlowMeshFullFlowTrialError(
            "cannot re-evaluate frozen scoring"
        ) from exc
    _require(
        scoring.get("task_success") is success,
        "N7 task_success disagrees with frozen scoring",
    )
    _assert_safe_result(evidence)
    return evidence


def _public_contract_v2(
    request: Mapping[str, Any],
) -> tuple[WorkloadScoringContract, str]:
    contract = WorkloadScoringContract(
        rule=request["success_scoring_rule"],
        answer_options=tuple(
            AnswerOption(
                option_id=option["option_id"],
                text=option["text"],
            )
            for option in request["answer_options"]
        ),
        correct_answer_id=None,
    )
    return contract, render_workload_question(request, contract)


def _validate_evidence_v2(
    value: Any,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify public v2 evidence without access to the hidden answer."""

    _require(isinstance(value, Mapping), "N7 v2 result is not an object")
    evidence = dict(value)
    _require(set(evidence) == _EVIDENCE_KEYS, "N7 v2 evidence fields changed")
    _require(
        evidence.get("schema_version") == FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
        "unsupported N7 v2 full-flow evidence schema",
    )
    _require(evidence.get("status") == "COMPLETE", "N7 v2 trial did not complete")
    request = plan["request_body"]
    for name in (
        "full_flow_request_id",
        "frozen_binding_sha256",
        "run_id",
        "trial_id",
        "trial_key",
        "workload_id",
        "task_class_id",
        "object_id",
        "representation_id",
    ):
        _require(evidence.get(name) == request[name], f"N7 v2 changed {name}")
    _require(
        evidence.get("request_sha256")
        == _sha256_bytes(_canonical_bytes(request)),
        "N7 v2 request digest changed",
    )
    for name, expected in _REQUIRED_RESULT_CLAIMS.items():
        _require(evidence.get(name) is expected, f"N7 v2 did not verify {name}")
    _require(
        type(evidence.get("idempotent_replay")) is bool,
        "N7 v2 replay flag is invalid",
    )
    route = evidence.get("route")
    expected_route = plan["route_binding"]
    _require(
        isinstance(route, Mapping) and set(route) == _EVIDENCE_ROUTE_KEYS,
        "N7 v2 route evidence fields changed",
    )
    for result_name, plan_name in (
        ("route_id", "route_id"),
        ("source_node_id", "source_node_id"),
        ("executor_node_id", "executor_node_id"),
        ("inference_node_id", "inference_node_id"),
        ("requested_location", "requested_location"),
    ):
        _require(
            route.get(result_name) == expected_route[plan_name],
            f"N7 v2 route changed {result_name}",
        )
    route_config = FullFlowRouteConfig(
        route_id=expected_route["route_id"],
        requested_location=expected_route["requested_location"],
        data_agent_plan_id=expected_route["data_agent_plan_id"],
        data_agent_plan_epoch=expected_route["data_agent_plan_epoch"],
        source_node_id=expected_route["source_node_id"],
        executor_node_id=expected_route["executor_node_id"],
        inference_node_id=expected_route["inference_node_id"],
        quiescence_timeout_seconds=expected_route[
            "quiescence_timeout_seconds"
        ],
    )
    _require(
        evidence.get("route_config_sha256") == route_config.sha256,
        "N7 v2 route configuration digest changed",
    )
    data_agent = evidence.get("data_agent")
    _require(
        isinstance(data_agent, Mapping)
        and set(data_agent) == _EVIDENCE_DATA_AGENT_KEYS,
        "N7 v2 Data Agent evidence fields changed",
    )
    expected_artifact = plan["expected_artifact"]
    expected_data = {
        "source_node_id": expected_route["source_node_id"],
        "source_identity_basis": "data-agent-health-before-and-after",
        "source_identity_verified": True,
        "plan_id": expected_route["data_agent_plan_id"],
        "plan_epoch": expected_route["data_agent_plan_epoch"],
        "object_catalog_version": request["object_catalog_version"],
        "artifact_media_type": FRAME_BUNDLE_MEDIA_TYPE,
        "artifact_size_bytes": request["artifact_size_bytes"],
        "artifact_sha256": request["artifact_sha256"],
        "manifest_sha256": expected_artifact["manifest_sha256"],
        "frame_count": expected_artifact["frame_count"],
    }
    for name, expected in expected_data.items():
        _require(data_agent.get(name) == expected, f"N7 v2 changed Data Agent {name}")
    for name in ("health_sha256_before", "health_sha256_after"):
        _digest(data_agent.get(name), f"Data Agent {name}")
    _require(
        data_agent["health_sha256_before"]
        == data_agent["health_sha256_after"],
        "Data Agent health changed during v2 delivery",
    )
    _text(data_agent.get("access_id"), "Data Agent access_id")
    _integer(data_agent.get("total_jpeg_bytes"), "total_jpeg_bytes", minimum=1)
    _validate_delivery(data_agent.get("delivery"), request)
    latency = data_agent.get("latency_ms")
    _require(
        isinstance(latency, Mapping) and set(latency) == _LATENCY_KEYS,
        "N7 v2 Data Agent latency fields changed",
    )
    for name, number in latency.items():
        if number is not None:
            _number(number, f"Data Agent latency {name}")

    contract, rendered_question = _public_contract_v2(request)
    semantic = evidence.get("semantic")
    _require(
        isinstance(semantic, Mapping) and set(semantic) == _SEMANTIC_KEYS,
        "N7 v2 semantic evidence fields changed",
    )
    for name in (
        "request_sha256",
        "prompt_sha256",
        "frame_sequence_sha256",
    ):
        _digest(semantic.get(name), f"semantic {name}")
    question_sha256 = _sha256_bytes(rendered_question.encode("utf-8"))
    _require(
        semantic.get("question_sha256") == question_sha256
        and semantic.get("model") == request["expected_model"],
        "N7 v2 semantic task or model binding changed",
    )
    _require(
        isinstance(semantic.get("runtime_epoch"), str)
        and _RUNTIME_EPOCH.fullmatch(semantic["runtime_epoch"]) is not None,
        "N7 v2 semantic runtime epoch is invalid",
    )
    _require(
        type(semantic.get("adapter_idempotent_replay")) is bool,
        "N7 v2 semantic replay flag is invalid",
    )
    _number(semantic.get("service_time_ms"), "semantic service_time_ms")
    frame_metadata = semantic.get("frame_metadata")
    _require(isinstance(frame_metadata, list), "N7 v2 frame metadata changed")
    _require(
        len(frame_metadata) == expected_artifact["frame_count"],
        "N7 v2 frame metadata count changed",
    )
    total_frame_bytes = 0
    previous_timestamp = -1.0
    for index, raw_frame in enumerate(frame_metadata):
        _require(
            isinstance(raw_frame, Mapping)
            and set(raw_frame) == _FRAME_METADATA_KEYS,
            f"N7 v2 frame metadata {index} fields changed",
        )
        _require(
            raw_frame.get("frame_index") == index,
            "N7 v2 frame indexes are not contiguous",
        )
        timestamp = _number(
            raw_frame.get("timestamp_seconds"),
            f"N7 v2 frame {index} timestamp",
        )
        _require(timestamp >= previous_timestamp, "N7 v2 timestamps regress")
        previous_timestamp = timestamp
        _integer(raw_frame.get("width"), "frame width", minimum=1)
        _integer(raw_frame.get("height"), "frame height", minimum=1)
        total_frame_bytes += _integer(
            raw_frame.get("jpeg_size_bytes"),
            "frame jpeg_size_bytes",
            minimum=1,
        )
        _digest(raw_frame.get("jpeg_sha256"), "frame jpeg_sha256")
    _require(
        semantic.get("representation_delivery_bytes") == total_frame_bytes
        == data_agent["total_jpeg_bytes"],
        "N7 v2 semantic bytes differ from the Data Agent bundle",
    )
    prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
        request["representation_id"],
        len(frame_metadata),
        rendered_question,
    )
    prompt_sha256 = _sha256_bytes(prompt.encode("utf-8"))
    _require(
        semantic.get("prompt_sha256") == prompt_sha256,
        "N7 v2 semantic prompt digest changed",
    )
    semantic_request_id = _sha256_bytes(_canonical_bytes({
        "full_flow_request_id": request["full_flow_request_id"],
        "trial_key": request["trial_key"],
        "execution_node_id": expected_route["inference_node_id"],
        "representation_id": request["representation_id"],
        "representation_sha256": request["artifact_sha256"],
        "question_sha256": question_sha256,
        "prompt_sha256": prompt_sha256,
        "frame_sequence_sha256": semantic["frame_sequence_sha256"],
    }))
    _require(
        semantic.get("semantic_request_id") == semantic_request_id,
        "N7 v2 semantic request identity changed",
    )

    scoring = evidence.get("scoring")
    _require(
        isinstance(scoring, Mapping) and set(scoring) == _SCORING_V2_KEYS,
        "N7 v2 scoring evidence fields changed",
    )
    option_ids = [option.option_id for option in contract.answer_options]
    _require(
        scoring.get("success_scoring_rule") == request["success_scoring_rule"]
        and scoring.get("answer_option_ids") == option_ids
        and scoring.get("task_binding_sha256")
        == request["task_binding_sha256"],
        "N7 v2 public scoring binding changed",
    )
    _require(
        scoring.get("answer_options_sha256")
        == _sha256_bytes(_canonical_bytes(request["answer_options"])),
        "N7 v2 answer-options digest changed",
    )
    final_answer = _text(scoring.get("final_answer"), "final_answer")
    _require(
        scoring.get("final_answer_sha256")
        == _sha256_bytes(final_answer.encode("utf-8")),
        "N7 v2 final-answer digest changed",
    )
    declared_matches = [
        option.option_id
        for option in contract.answer_options
        if evaluate_workload_answer(
            final_answer,
            WorkloadScoringContract(
                rule=contract.rule,
                answer_options=contract.answer_options,
                correct_answer_id=option.option_id,
            ),
        )
        is True
    ]
    _require(
        len(declared_matches) == 1,
        "N7 v2 final answer is not one declared option",
    )
    _digest(scoring.get("oracle_health_sha256"), "N1 health digest")
    oracle = scoring.get("oracle_result")
    _require(
        isinstance(oracle, Mapping) and set(oracle) == _ORACLE_RESULT_KEYS,
        "N1 public result fields changed",
    )
    _require(
        oracle.get("schema_version") == N1_SCORE_RESULT_SCHEMA_VERSION
        and oracle.get("status") == "SCORED"
        and oracle.get("node_id") == N1_NODE_ID
        and oracle.get("oracle_id") == request["oracle_id"]
        == plan["oracle_id"]
        and oracle.get("run_id") == request["run_id"]
        and oracle.get("trial_id") == request["trial_id"]
        and oracle.get("object_id") == request["object_id"]
        and oracle.get("task_binding_sha256")
        == request["task_binding_sha256"]
        and oracle.get("success_scoring_rule")
        == request["success_scoring_rule"],
        "N1 result differs from the frozen public task or oracle",
    )
    expected_score_id = full_flow_n1_score_request_id(
        request,
        final_answer=final_answer,
    )
    _require(
        oracle.get("score_request_id") == expected_score_id,
        "N1 score request identity changed",
    )
    score_request = build_n1_score_request(
        score_request_id=expected_score_id,
        oracle_id=request["oracle_id"],
        run_id=request["run_id"],
        trial_id=request["trial_id"],
        object_id=request["object_id"],
        task_binding_sha256=request["task_binding_sha256"],
        predicted_answer=final_answer,
    )
    _require(
        oracle.get("evaluation_unit_id")
        == score_request["evaluation_unit_id"]
        and oracle.get("request_sha256")
        == _sha256_bytes(_canonical_bytes(score_request))
        and oracle.get("prediction_sha256")
        == _sha256_bytes(final_answer.encode("utf-8")),
        "N1 request or prediction digest changed",
    )
    _require(type(oracle.get("correct")) is bool, "N1 correctness is invalid")
    expected_score = 1.0 if oracle["correct"] else 0.0
    _require(
        oracle.get("score") == expected_score
        and scoring.get("score") == expected_score
        and scoring.get("task_success") is oracle["correct"],
        "N1 score, correctness, and N7 task success disagree",
    )
    for name in (
        "public_task_set_sha256",
        "oracle_instance_hmac_sha256",
        "score_evidence_hmac_sha256",
        "result_content_sha256",
    ):
        _digest(oracle.get(name), f"N1 {name}")
    oracle_core = dict(oracle)
    oracle_core.pop("result_content_sha256")
    _require(
        oracle["result_content_sha256"]
        == _sha256_bytes(_canonical_bytes(oracle_core)),
        "N1 result content digest changed",
    )
    _require(
        type(oracle.get("idempotent_replay")) is bool
        and oracle.get("hidden_answer_returned") is False
        and oracle.get("credentials_recorded") is False
        and oracle.get("eligible_for_scientific_claims") is False,
        "N1 public provenance flags are invalid",
    )
    assert_hidden_oracle_fields_absent(evidence)
    _assert_safe_result(evidence)
    return evidence


def _workflow_failure(
    terminal: TerminalWorkflow,
    submitted: SubmittedWorkflow,
    client: FlowMeshClientProtocol,
) -> FlowMeshFullFlowTrialError:
    details = [terminal.detail] if terminal.detail else []
    for task_id in submitted.task_ids:
        try:
            detail = client.describe_task_failure(task_id)
        except Exception as exc:
            details.append("task detail unavailable: " + redact_secrets(str(exc)))
        else:
            if detail:
                details.append(
                    redact_secrets(
                        json.dumps(detail, sort_keys=True, ensure_ascii=False)
                    )
                )
    return FlowMeshFullFlowTrialError(
        f"FlowMesh full-flow trial {submitted.workflow_id} ended with "
        f"{terminal.status}: "
        + ("; ".join(details) or "no task detail was reported")
    )


def run_flowmesh_full_flow_trial_v2(
    *,
    plan_dir: str | Path,
    output_dir: str | Path,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    full_flow_ingress_hmac_secret: str | None = None,
) -> dict[str, Any]:
    """Submit one label-free v2 task and freeze N1-scored public evidence."""

    plan, deployment = _read_plan_v2(plan_dir)
    _require(
        settings.worker_alias == deployment["worker_alias"]
        and settings.worker_id is None,
        "v2 run settings must carry exactly the deployment worker alias",
    )
    # Resolve runtime-only authentication after all local frozen-input checks,
    # but before the first FlowMesh read or mutation.
    ingress_secret = _runtime_full_flow_hmac_secret(
        full_flow_ingress_hmac_secret
    )
    identity = describe_pinned_worker(client, settings)
    _require(
        identity.alias in {None, deployment["worker_alias"]},
        "Root returned a different worker alias",
    )
    route = plan["route_binding"]
    route_config = FullFlowRouteConfig(
        route_id=route["route_id"],
        requested_location=route["requested_location"],
        data_agent_plan_id=route["data_agent_plan_id"],
        data_agent_plan_epoch=route["data_agent_plan_epoch"],
        source_node_id=route["source_node_id"],
        executor_node_id=route["executor_node_id"],
        inference_node_id=route["inference_node_id"],
        quiescence_timeout_seconds=route["quiescence_timeout_seconds"],
    )
    public_workflow = build_flowmesh_full_flow_trial_v2_workflow(
        plan["request_body"],
        route_config,
        deployment,
        owner=plan["owner"],
        selected_worker_id=identity.worker_id,
    )
    workflow = build_flowmesh_full_flow_trial_v2_workflow(
        plan["request_body"],
        route_config,
        deployment,
        owner=plan["owner"],
        selected_worker_id=identity.worker_id,
        full_flow_hmac_secret=ingress_secret,
    )
    assert_hidden_oracle_fields_absent(workflow)
    validation = client.validate(workflow)
    _require(
        validation.ok,
        "FlowMesh rejected the v2 full-flow workflow: "
        + "; ".join(validation.errors),
    )
    submitted = client.submit(workflow)
    _require(len(submitted.task_ids) == 1, "FlowMesh returned != 1 v2 task")
    terminal = client.wait(
        submitted.workflow_id,
        settings.poll_interval_seconds,
    )
    if terminal.status != "DONE":
        raise _workflow_failure(terminal, submitted, client)
    task_id = submitted.task_ids[0]
    try:
        api = extract_api_executor_result(client.retrieve_result(task_id))
    except Exception as exc:
        raise FlowMeshFullFlowTrialError(
            "cannot validate FlowMesh v2 API result: "
            + redact_secrets(str(exc))
        ) from exc
    evidence = _validate_evidence_v2(
        _strict_json(api["text"].encode("utf-8"), "N7 v2 evidence"),
        plan,
    )
    try:
        detail = client.describe_task_failure(task_id)
    except Exception as exc:
        raise FlowMeshFullFlowTrialError(
            "cannot verify completed v2 task worker: "
            + redact_secrets(str(exc))
        ) from exc
    _require(
        isinstance(detail, Mapping)
        and detail.get("assigned_worker") == identity.worker_id,
        "completed v2 task was not assigned to the pinned worker",
    )
    deployment_sha256 = _sha256_bytes(_canonical_bytes(deployment))
    record = {
        "schema_version": FULL_FLOW_TASK_RECORD_V2_SCHEMA_VERSION,
        "task_id": task_id,
        "worker_id": identity.worker_id,
        "api_executor": api["executor"],
        "api_http_status": api["status_code"],
        "service_result_sha256": _sha256_bytes(_canonical_bytes(evidence)),
        "service_result": evidence,
        "credentials_recorded": False,
    }
    summary = {
        "schema_version": FULL_FLOW_RUN_V2_SCHEMA_VERSION,
        "status": "COMPLETE",
        "logical_plan_id": plan["logical_plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "deployment_binding_sha256": deployment_sha256,
        "workflow_id": submitted.workflow_id,
        "task_id": task_id,
        "selected_worker": identity.to_public_dict(),
        "trial_key": evidence["trial_key"],
        "object_id": evidence["object_id"],
        "representation_id": evidence["representation_id"],
        "source_node_id": route["source_node_id"],
        "execution_node_id": route["executor_node_id"],
        "semantic_executor_node_id": route["inference_node_id"],
        "scoring_node_id": N1_NODE_ID,
        "task_success": evidence["scoring"]["task_success"],
        "idempotent_replay": evidence["idempotent_replay"],
        "route_unified": True,
        "flowmesh_semantic_execution_verified": True,
        "host_artifact_materialized": False,
        "telemetry_complete": True,
        "llm_called": True,
        "evidence_class": _V2_EVIDENCE_CLASS,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }
    submission = {
        "schema_version": FULL_FLOW_SUBMISSION_V2_SCHEMA_VERSION,
        "workflow_id": submitted.workflow_id,
        "task_id": task_id,
        "selected_worker_id": identity.worker_id,
        "logical_plan_sha256": plan["plan_sha256"],
        "deployment_binding_sha256": deployment_sha256,
        # Bind the durable record to the reproducible credential-free workflow.
        # The submitted variant additionally carries a request-bound HMAC that
        # may transit the FlowMesh control plane but is never written here.
        "workflow_sha256": _sha256_bytes(_canonical_bytes(public_workflow)),
        "validated_before_submission": True,
        "credentials_recorded": False,
    }
    assert_hidden_oracle_fields_absent(summary)
    assert_hidden_oracle_fields_absent(record)
    documents = {
        _RUN_V2_FILE: _json_bytes(summary),
        _SUBMISSION_V2_FILE: _json_bytes(submission),
        _TASK_RECORD_V2_FILE: _json_bytes(record),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {**summary, "output_dir": str(target)}


def verify_flowmesh_full_flow_trial_v2_run(
    run_dir: str | Path,
    *,
    plan_dir: str | Path,
    n1_oracle_package_dir: str | Path | None = None,
    n1_evidence_secret: bytes | None = None,
) -> dict[str, Any]:
    """Verify a durable v2 run, optionally authenticating the N1 HMAC.

    Public artifacts alone establish structural consistency but cannot prove
    that the reported score came from the frozen hidden oracle.  Supplying
    both runtime-only N1 inputs upgrades the result to authenticated
    ``VERIFIED`` without persisting either input.
    """

    _require(
        (n1_oracle_package_dir is None) == (n1_evidence_secret is None),
        "N1 package and evidence secret must be supplied together",
    )

    documents = _read_checksums(
        Path(run_dir).resolve(),
        _RUN_V2_FILES,
        "v2 full-flow run",
    )
    summary = _strict_json(documents[_RUN_V2_FILE], "v2 run summary")
    submission = _strict_json(documents[_SUBMISSION_V2_FILE], "v2 submission")
    record = _strict_json(documents[_TASK_RECORD_V2_FILE], "v2 task record")
    _require(set(summary) == _SUMMARY_KEYS, "v2 summary fields changed")
    _require(set(submission) == _SUBMISSION_KEYS, "v2 submission fields changed")
    _require(set(record) == _TASK_RECORD_KEYS, "v2 task record fields changed")
    _require(
        summary.get("schema_version") == FULL_FLOW_RUN_V2_SCHEMA_VERSION
        and summary.get("status") == "COMPLETE",
        "v2 full-flow run is not complete",
    )
    _require(
        submission.get("schema_version")
        == FULL_FLOW_SUBMISSION_V2_SCHEMA_VERSION
        and record.get("schema_version")
        == FULL_FLOW_TASK_RECORD_V2_SCHEMA_VERSION,
        "v2 durable schema changed",
    )
    plan, deployment = _read_plan_v2(plan_dir)
    evidence = _validate_evidence_v2(record.get("service_result"), plan)
    _require(
        record.get("service_result_sha256")
        == _sha256_bytes(_canonical_bytes(evidence)),
        "v2 service result digest changed",
    )
    worker = summary.get("selected_worker")
    _require(isinstance(worker, Mapping), "v2 selected worker is missing")
    worker_id = _identifier(worker.get("worker_id"), "selected worker_id")
    _require(
        record.get("worker_id") == worker_id
        == submission.get("selected_worker_id"),
        "v2 worker binding changed",
    )
    _require(
        record.get("task_id") == summary.get("task_id")
        == submission.get("task_id"),
        "v2 task identity changed",
    )
    _require(
        summary.get("workflow_id") == submission.get("workflow_id"),
        "v2 workflow identity changed",
    )
    _require(
        record.get("api_executor") == "api"
        and type(record.get("api_http_status")) is int
        and 200 <= record["api_http_status"] < 300,
        "v2 API executor evidence changed",
    )
    deployment_sha256 = _sha256_bytes(_canonical_bytes(deployment))
    _require(
        summary.get("plan_sha256") == plan["plan_sha256"]
        == submission.get("logical_plan_sha256"),
        "v2 run is not bound to its logical plan",
    )
    _require(
        summary.get("deployment_binding_sha256") == deployment_sha256
        == submission.get("deployment_binding_sha256"),
        "v2 run is not bound to its deployment",
    )
    route = plan["route_binding"]
    route_config = FullFlowRouteConfig(
        route_id=route["route_id"],
        requested_location=route["requested_location"],
        data_agent_plan_id=route["data_agent_plan_id"],
        data_agent_plan_epoch=route["data_agent_plan_epoch"],
        source_node_id=route["source_node_id"],
        executor_node_id=route["executor_node_id"],
        inference_node_id=route["inference_node_id"],
        quiescence_timeout_seconds=route["quiescence_timeout_seconds"],
    )
    workflow = build_flowmesh_full_flow_trial_v2_workflow(
        plan["request_body"],
        route_config,
        deployment,
        owner=plan["owner"],
        selected_worker_id=worker_id,
    )
    _require(
        submission.get("workflow_sha256")
        == _sha256_bytes(_canonical_bytes(workflow)),
        "v2 submitted workflow digest changed",
    )
    expected_summary = {
        "logical_plan_id": plan["logical_plan_id"],
        "trial_key": evidence["trial_key"],
        "object_id": evidence["object_id"],
        "representation_id": evidence["representation_id"],
        "source_node_id": route["source_node_id"],
        "execution_node_id": route["executor_node_id"],
        "semantic_executor_node_id": route["inference_node_id"],
        "scoring_node_id": N1_NODE_ID,
        "task_success": evidence["scoring"]["task_success"],
        "idempotent_replay": evidence["idempotent_replay"],
        "route_unified": True,
        "flowmesh_semantic_execution_verified": True,
        "host_artifact_materialized": False,
        "telemetry_complete": True,
        "llm_called": True,
        "evidence_class": _V2_EVIDENCE_CLASS,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }
    for name, expected in expected_summary.items():
        _require(summary.get(name) == expected, f"v2 summary changed {name}")
    oracle_hmac_verified = False
    if n1_oracle_package_dir is not None:
        scoring = evidence["scoring"]
        oracle_result = scoring["oracle_result"]
        final_answer = scoring["final_answer"]
        score_request = build_n1_score_request(
        score_request_id=oracle_result["score_request_id"],
        oracle_id=plan["oracle_id"],
        run_id=evidence["run_id"],
        trial_id=evidence["trial_id"],
        object_id=evidence["object_id"],
            task_binding_sha256=plan["public_task_binding_sha256"],
            predicted_answer=final_answer,
        )
        try:
            authenticated_score = verify_n1_score_result(
                package_dir=n1_oracle_package_dir,
                request=score_request,
                result=oracle_result,
                evidence_secret=n1_evidence_secret,
            )
        except Exception as exc:
            raise FlowMeshFullFlowTrialError(
                "N1 hidden-oracle evidence authentication failed"
            ) from exc
        _require(
            authenticated_score["correct"]
            is evidence["scoring"]["task_success"],
            "authenticated N1 score differs from the v2 evidence",
        )
        oracle_hmac_verified = True
    _require(
        submission.get("validated_before_submission") is True
        and submission.get("credentials_recorded") is False
        and record.get("credentials_recorded") is False,
        "v2 submission provenance changed",
    )
    assert_hidden_oracle_fields_absent(summary)
    assert_hidden_oracle_fields_absent(record)
    _assert_safe_result(summary)
    _assert_safe_result(record)
    return {
        "status": (
            "VERIFIED" if oracle_hmac_verified else "STRUCTURALLY_VERIFIED"
        ),
        "schema_version": summary["schema_version"],
        "logical_plan_id": summary["logical_plan_id"],
        "plan_sha256": summary["plan_sha256"],
        "workflow_id": summary["workflow_id"],
        "task_id": summary["task_id"],
        "worker_id": worker_id,
        "trial_key": summary["trial_key"],
        "object_id": summary["object_id"],
        "oracle_id": plan["oracle_id"],
        "public_task_binding_sha256": plan["public_task_binding_sha256"],
        "task_success": (
            summary["task_success"] if oracle_hmac_verified else None
        ),
        "reported_task_success": summary["task_success"],
        "oracle_hmac_verified": oracle_hmac_verified,
        "authenticity_verified": oracle_hmac_verified,
        "route_unified": True,
        "flowmesh_semantic_execution_verified": True,
        "host_artifact_materialized": False,
        "plan_binding_checked": True,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }


def run_flowmesh_full_flow_trial(
    *,
    plan_dir: str | Path,
    output_dir: str | Path,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    full_flow_ingress_hmac_secret: str | None = None,
) -> dict[str, Any]:
    """Submit exactly one pinned N7 API task and freeze safe evidence."""

    plan, deployment = _read_plan(plan_dir)
    _require(
        settings.worker_alias == deployment["worker_alias"]
        and settings.worker_id is None,
        "run settings must carry exactly the deployment worker alias",
    )
    # Resolve runtime-only authentication after all local frozen-input checks,
    # but before the first FlowMesh read or mutation.
    ingress_secret = _runtime_full_flow_hmac_secret(
        full_flow_ingress_hmac_secret
    )
    identity = describe_pinned_worker(client, settings)
    _require(
        identity.alias in {None, deployment["worker_alias"]},
        "Root returned a different worker alias",
    )
    public_workflow = build_flowmesh_full_flow_trial_workflow(
        plan,
        deployment,
        selected_worker_id=identity.worker_id,
    )
    workflow = build_flowmesh_full_flow_trial_workflow(
        plan,
        deployment,
        selected_worker_id=identity.worker_id,
        full_flow_hmac_secret=ingress_secret,
    )
    validation = client.validate(workflow)
    _require(
        validation.ok,
        "FlowMesh rejected the full-flow workflow: "
        + "; ".join(validation.errors),
    )
    submitted = client.submit(workflow)
    _require(len(submitted.task_ids) == 1, "FlowMesh returned != 1 task")
    terminal = client.wait(
        submitted.workflow_id,
        settings.poll_interval_seconds,
    )
    if terminal.status != "DONE":
        raise _workflow_failure(terminal, submitted, client)
    task_id = submitted.task_ids[0]
    try:
        api = extract_api_executor_result(client.retrieve_result(task_id))
    except Exception as exc:
        raise FlowMeshFullFlowTrialError(
            "cannot validate FlowMesh API result: " + redact_secrets(str(exc))
        ) from exc
    evidence = _validate_evidence(
        _strict_json(api["text"].encode("utf-8"), "N7 full-flow evidence"),
        plan,
    )
    try:
        detail = client.describe_task_failure(task_id)
    except Exception as exc:
        raise FlowMeshFullFlowTrialError(
            "cannot verify completed task worker: " + redact_secrets(str(exc))
        ) from exc
    _require(
        isinstance(detail, Mapping)
        and detail.get("assigned_worker") == identity.worker_id,
        "completed full-flow task was not assigned to the pinned worker",
    )
    deployment_sha256 = _sha256_bytes(_canonical_bytes(deployment))
    record = {
        "schema_version": FULL_FLOW_TASK_RECORD_SCHEMA_VERSION,
        "task_id": task_id,
        "worker_id": identity.worker_id,
        "api_executor": api["executor"],
        "api_http_status": api["status_code"],
        "service_result_sha256": _sha256_bytes(_canonical_bytes(evidence)),
        "service_result": evidence,
        "credentials_recorded": False,
    }
    route = plan["route_binding"]
    summary = {
        "schema_version": FULL_FLOW_RUN_SCHEMA_VERSION,
        "status": "COMPLETE",
        "logical_plan_id": plan["logical_plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "deployment_binding_sha256": deployment_sha256,
        "workflow_id": submitted.workflow_id,
        "task_id": task_id,
        "selected_worker": identity.to_public_dict(),
        "trial_key": evidence["trial_key"],
        "object_id": evidence["object_id"],
        "representation_id": evidence["representation_id"],
        "source_node_id": route["source_node_id"],
        "execution_node_id": route["executor_node_id"],
        "semantic_executor_node_id": route["inference_node_id"],
        "scoring_node_id": route["scoring_node_id"],
        "task_success": evidence["scoring"]["task_success"],
        "idempotent_replay": evidence["idempotent_replay"],
        "route_unified": True,
        "flowmesh_semantic_execution_verified": True,
        "host_artifact_materialized": False,
        "telemetry_complete": True,
        "llm_called": True,
        "evidence_class": "flowmesh-unified-pathfinder-full-flow-smoke",
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }
    submission = {
        "schema_version": FULL_FLOW_SUBMISSION_SCHEMA_VERSION,
        "workflow_id": submitted.workflow_id,
        "task_id": task_id,
        "selected_worker_id": identity.worker_id,
        "logical_plan_sha256": plan["plan_sha256"],
        "deployment_binding_sha256": deployment_sha256,
        "workflow_sha256": _sha256_bytes(_canonical_bytes(public_workflow)),
        "validated_before_submission": True,
        "credentials_recorded": False,
    }
    documents = {
        _RUN_FILE: _json_bytes(summary),
        _SUBMISSION_FILE: _json_bytes(submission),
        _TASK_RECORD_FILE: _json_bytes(record),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {**summary, "output_dir": str(target)}


def verify_flowmesh_full_flow_trial_run(
    run_dir: str | Path,
    *,
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Offline-verify exact object, route, score, worker, and plan binding."""

    documents = _read_checksums(
        Path(run_dir).resolve(),
        _RUN_FILES,
        "full-flow run",
    )
    summary = _strict_json(documents[_RUN_FILE], "full-flow run summary")
    submission = _strict_json(documents[_SUBMISSION_FILE], "submission")
    record = _strict_json(documents[_TASK_RECORD_FILE], "task record")
    _require(set(summary) == _SUMMARY_KEYS, "full-flow summary fields changed")
    _require(set(submission) == _SUBMISSION_KEYS, "submission fields changed")
    _require(set(record) == _TASK_RECORD_KEYS, "task record fields changed")
    _require(
        summary.get("schema_version") == FULL_FLOW_RUN_SCHEMA_VERSION
        and summary.get("status") == "COMPLETE",
        "full-flow run is not complete",
    )
    _require(
        submission.get("schema_version") == FULL_FLOW_SUBMISSION_SCHEMA_VERSION
        and record.get("schema_version") == FULL_FLOW_TASK_RECORD_SCHEMA_VERSION,
        "full-flow durable schema changed",
    )
    plan, deployment = _read_plan(plan_dir)
    evidence = _validate_evidence(record.get("service_result"), plan)
    _require(
        record.get("service_result_sha256")
        == _sha256_bytes(_canonical_bytes(evidence)),
        "service result digest changed",
    )
    worker = summary.get("selected_worker")
    _require(isinstance(worker, Mapping), "selected worker is missing")
    worker_id = _identifier(worker.get("worker_id"), "selected worker_id")
    _require(
        record.get("worker_id") == worker_id
        == submission.get("selected_worker_id"),
        "worker binding changed",
    )
    _require(
        record.get("task_id") == summary.get("task_id")
        == submission.get("task_id"),
        "task identity changed",
    )
    _require(
        summary.get("workflow_id") == submission.get("workflow_id"),
        "workflow identity changed",
    )
    _require(
        record.get("api_executor") == "api"
        and type(record.get("api_http_status")) is int
        and 200 <= record["api_http_status"] < 300,
        "API executor evidence changed",
    )
    deployment_sha256 = _sha256_bytes(_canonical_bytes(deployment))
    _require(
        summary.get("plan_sha256") == plan["plan_sha256"]
        == submission.get("logical_plan_sha256"),
        "run is not bound to supplied logical plan",
    )
    _require(
        summary.get("deployment_binding_sha256") == deployment_sha256
        == submission.get("deployment_binding_sha256"),
        "run is not bound to deployment binding",
    )
    workflow = build_flowmesh_full_flow_trial_workflow(
        plan,
        deployment,
        selected_worker_id=worker_id,
    )
    _require(
        submission.get("workflow_sha256")
        == _sha256_bytes(_canonical_bytes(workflow)),
        "submitted workflow digest changed",
    )
    route = plan["route_binding"]
    exact_summary = {
        "logical_plan_id": plan["logical_plan_id"],
        "trial_key": evidence["trial_key"],
        "object_id": evidence["object_id"],
        "representation_id": evidence["representation_id"],
        "source_node_id": route["source_node_id"],
        "execution_node_id": route["executor_node_id"],
        "semantic_executor_node_id": route["inference_node_id"],
        "scoring_node_id": route["scoring_node_id"],
        "task_success": evidence["scoring"]["task_success"],
        "idempotent_replay": evidence["idempotent_replay"],
        "route_unified": True,
        "flowmesh_semantic_execution_verified": True,
        "host_artifact_materialized": False,
        "telemetry_complete": True,
        "llm_called": True,
        "evidence_class": "flowmesh-unified-pathfinder-full-flow-smoke",
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }
    for name, expected in exact_summary.items():
        _require(summary.get(name) == expected, f"summary changed {name}")
    _require(
        submission.get("validated_before_submission") is True
        and submission.get("credentials_recorded") is False
        and record.get("credentials_recorded") is False,
        "submission provenance changed",
    )
    _assert_safe_result(summary)
    _assert_safe_result(record)
    return {
        "status": "VERIFIED",
        "schema_version": summary["schema_version"],
        "logical_plan_id": summary["logical_plan_id"],
        "plan_sha256": summary["plan_sha256"],
        "workflow_id": summary["workflow_id"],
        "task_id": summary["task_id"],
        "worker_id": worker_id,
        "trial_key": summary["trial_key"],
        "object_id": summary["object_id"],
        "task_success": summary["task_success"],
        "route_unified": True,
        "flowmesh_semantic_execution_verified": True,
        "host_artifact_materialized": False,
        "plan_binding_checked": True,
        "credentials_recorded": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "FULL_FLOW_DATA_PLANE_BINDING_SCHEMA_VERSION",
    "FULL_FLOW_DEPLOYMENT_BINDING_SCHEMA_VERSION",
    "FULL_FLOW_LOGICAL_PLAN_SCHEMA_VERSION",
    "FULL_FLOW_LOGICAL_PLAN_V2_SCHEMA_VERSION",
    "FULL_FLOW_REQUEST_SCHEMA_VERSION",
    "FULL_FLOW_RUN_SCHEMA_VERSION",
    "FULL_FLOW_RUN_V2_SCHEMA_VERSION",
    "FULL_FLOW_SERVICE_RESULT_SCHEMA_VERSION",
    "FULL_FLOW_SERVICE_RESULT_V2_SCHEMA_VERSION",
    "FlowMeshFullFlowTrialError",
    "build_flowmesh_full_flow_trial_workflow",
    "build_flowmesh_full_flow_trial_v2_workflow",
    "build_full_flow_deployment_binding",
    "plan_flowmesh_full_flow_trial",
    "plan_flowmesh_full_flow_trial_v2",
    "run_flowmesh_full_flow_trial",
    "run_flowmesh_full_flow_trial_v2",
    "verify_flowmesh_full_flow_trial_plan",
    "verify_flowmesh_full_flow_trial_v2_plan",
    "verify_flowmesh_full_flow_trial_run",
    "verify_flowmesh_full_flow_trial_v2_run",
]
