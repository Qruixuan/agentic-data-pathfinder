"""Audited Data Agent to container-vision semantic vertical slice.

This module closes one deliberately narrow integration seam.  It binds one
frozen container-matrix trial to an exact Data Agent route, downloads and
validates a canonical sampled-frame bundle, presents only its verified ordered
frames to the container semantic-v2 adapter, applies the frozen answer-scoring
rule, and writes a checksum-bound evidence package.

The host coordinator performs the Data Agent fetch and then invokes N6
directly.  This vertical slice is deliberately not represented as a
FlowMesh-scheduled semantic workflow.

The infrastructure matrix and the semantic artifact are not yet the same data
plane.  In particular, the current matrix uses deterministic synthetic objects
while the Data Agent serves real benchmark objects.  The evidence therefore
keeps both object identities and explicitly refuses to claim route unification.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Protocol

from ..distributed.registry import EndpointRegistry
from ..distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    evaluate_workload_answer,
    load_workload_scoring_contract,
    render_workload_question,
)
from ..frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    FrameBundleLimits,
)
from ..frame_bundle_transfer import (
    FrameBundleTransfer,
    build_frame_bundle_access_request,
    fetch_validated_frame_bundle,
)
from .container_node import CONTAINER_NODE_BEARER_TOKEN_ENV


DATA_AGENT_SEMANTIC_RECORD_SCHEMA_VERSION = (
    "pathfinder.data-agent-frame-bundle-semantic-record/v1alpha1"
)
DATA_AGENT_SEMANTIC_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.data-agent-frame-bundle-semantic-manifest/v1alpha1"
)
DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION = (
    "pathfinder.data-agent-frame-bundle-semantic-spec/v1alpha1"
)
CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION_V2 = (
    "pathfinder.container-node-semantic-request/v1alpha2"
)
CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION_V2 = (
    "pathfinder.container-node-semantic-result/v1alpha2"
)
DATA_AGENT_SEMANTIC_EXECUTOR_NODE_ID = "N6"

RECORD_NAME = "data-agent-frame-bundle-semantic-record.json"
MANIFEST_NAME = "data-agent-frame-bundle-semantic-manifest.json"
_OUTPUT_FILES = frozenset({RECORD_NAME, MANIFEST_NAME})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_PERSISTED_OPTION_ANSWER_BYTES = 64

DATA_AGENT_SEMANTIC_RECORD_REQUIRED_KEYS = frozenset({
    "schema_version",
    "status",
    "semantic_run_id",
    "semantic_request_id",
    "matrix_id",
    "matrix_plan_sha256",
    "trial_key",
    "trial_id",
    "workload_id",
    "workload_class",
    "matrix_design_id",
    "repetition",
    "matrix_object_id",
    "artifact_object_id",
    "matrix_executor_node_id",
    "semantic_executor_node_id",
    "representation_id",
    "endpoint_registry_id",
    "endpoint_registry_sha256",
    "semantic_spec_sha256",
    "route",
    "data_agent_route_design_id",
    "data_agent_plan_id",
    "data_agent_plan_epoch",
    "event_index",
    "data_agent_access_id",
    "artifact",
    "frame_bundle",
    "delivery",
    "latency_ms",
    "question_sha256",
    "prompt_sha256",
    "frame_sequence_sha256",
    "container_request_sha256",
    "container_result_schema_version",
    "semantic_input_kind",
    "representation_delivery_bytes",
    "success_scoring_rule",
    "answer_option_ids",
    "correct_answer_id",
    "final_answer",
    "final_answer_sha256",
    "task_success",
    "expected_model",
    "model",
    "semantic_service_time_ms",
    "semantic_runtime_epoch",
    "semantic_health_verified",
    "idempotent_replay",
    "container_data_plane_artifact_delivery_verified",
    "semantic_frame_payload_integrity_verified",
    "data_agent_artifact_delivery_verified",
    "container_semantic_response_consistency_verified",
    "container_runtime_code_provenance_verified",
    "scoring_verified",
    "execution_and_semantic_route_unified",
    "llm_called",
    "semantic_telemetry_complete",
    "credentials_recorded",
    "eligible_for_scientific_claims",
})

DATA_AGENT_SEMANTIC_MANIFEST_REQUIRED_KEYS = frozenset({
    "schema_version",
    "status",
    "semantic_run_id",
    "matrix_id",
    "matrix_plan_sha256",
    "matrix_plan_file_sha256",
    "trial_key",
    "event_index",
    "endpoint_registry_id",
    "endpoint_registry_sha256",
    "semantic_spec_sha256",
    "record_count",
    "record_sha256",
    "artifact_size_bytes",
    "artifact_sha256",
    "frame_count",
    "task_success_count",
    "expected_model",
    "models",
    "semantic_runtime_epoch",
    "semantic_health_verified",
    "data_agent_artifact_delivery_verified",
    "container_semantic_response_consistency_verified",
    "container_runtime_code_provenance_verified",
    "scoring_verified",
    "execution_and_semantic_route_unified",
    "llm_called",
    "credentials_recorded",
    "eligible_for_scientific_claims",
    "limitations",
})

_CONTAINER_RESULT_KEYS = frozenset({
    "schema_version",
    "api_version",
    "status",
    "outcome_type",
    "telemetry_complete",
    "semantic_request_id",
    "execution_node_id",
    "started_monotonic_ns",
    "finished_monotonic_ns",
    "service_time_ms",
    "request_sha256",
    "prompt_sha256",
    "representation_sha256",
    "frame_sequence_sha256",
    "frame_count",
    "representation_delivery_bytes",
    "semantic_input_kind",
    "data_plane_artifact_delivery_verified",
    "semantic_frame_payload_integrity_verified",
    "source_node_id",
    "model",
    "final_answer",
    "final_answer_sha256",
    "llm_called",
    "credentials_recorded",
    "idempotent_replay",
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

DATA_AGENT_SEMANTIC_SPEC_REQUIRED_KEYS = frozenset({
    "schema_version",
    "semantic_run_id",
    "trial_key",
    "semantic_executor_node_id",
    "representation_id",
    "data_agent_route_design_id",
    "data_agent_plan_id",
    "data_agent_plan_epoch",
    "workload_id",
    "task_class_id",
    "artifact_object_id",
    "artifact_sha256",
    "artifact_size_bytes",
    "object_catalog_version",
    "question",
    "success_scoring_rule",
    "answer_options",
    "correct_answer_id",
    "expected_model",
    "credentials_recorded",
})

_RECORD_KEYS = DATA_AGENT_SEMANTIC_RECORD_REQUIRED_KEYS
_MANIFEST_KEYS = DATA_AGENT_SEMANTIC_MANIFEST_REQUIRED_KEYS
_SPEC_KEYS = DATA_AGENT_SEMANTIC_SPEC_REQUIRED_KEYS


class DataAgentSemanticVerticalError(RuntimeError):
    """Raised when the vertical slice cannot produce trusted evidence."""


@dataclass(frozen=True)
class DataAgentFrameBundleSemanticSpec:
    """Validated immutable-file binding for one semantic vertical trial."""

    source_path: Path
    source_sha256: str
    document: dict[str, Any]

    def workload(self) -> dict[str, Any]:
        """Project the frozen document into the shared scoring contract."""
        return {
            "workload_id": self.document["workload_id"],
            "object_id": self.document["artifact_object_id"],
            "task_class_id": self.document["task_class_id"],
            "question": self.document["question"],
            "answer_options": json.loads(
                json.dumps(self.document["answer_options"])
            ),
            "correct_answer_id": self.document["correct_answer_id"],
            "success_scoring_rule": self.document[
                "success_scoring_rule"
            ],
            "data_agent_route_design_id": self.document[
                "data_agent_route_design_id"
            ],
            "data_agent_plan_id": self.document["data_agent_plan_id"],
            "data_agent_plan_epoch": self.document[
                "data_agent_plan_epoch"
            ],
            "expected_artifact_sha256": self.document["artifact_sha256"],
            "expected_artifact_size_bytes": self.document[
                "artifact_size_bytes"
            ],
            "expected_object_catalog_version": self.document[
                "object_catalog_version"
            ],
        }


class ContainerSemanticVisionAdapter(Protocol):
    """Trusted adapter for one container semantic-v2 request.

    The request contains JPEG base64 only in memory.  Implementations must not
    persist it, and this module never copies it into the evidence package.
    """

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        """Execute one already-bound semantic request."""

    @property
    def health_verified(self) -> bool:
        """Whether pre/post semantic-v2 health checks succeeded."""

    @property
    def last_runtime_epoch(self) -> str | None:
        """The stable container runtime epoch observed around execution."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Any,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> None:
        del request, file_pointer, code, message, headers, new_url
        return None


class HttpContainerSemanticVisionAdapter:
    """Bounded HTTP adapter for the container semantic-v2 endpoint.

    The endpoint carries no credential in its URL, follows no redirects, and
    is health-gated before and after the request.  The stable runtime epoch is
    exposed as metadata for the outer evidence record; neither endpoint URL
    is ever returned or persisted.
    """

    def __init__(
        self,
        *,
        semantic_url: str,
        health_url: str,
        expected_execution_node_id: str,
        bearer_token: str | None = None,
        timeout_seconds: float = 240.0,
        max_request_bytes: int = 16 * 1024 * 1024,
        max_response_bytes: int = 2 * 1024 * 1024,
    ) -> None:
        self._semantic_url = self._endpoint(
            semantic_url,
            "semantic_url",
            "/v1/semantic/chat-completions",
        )
        self._health_url = self._endpoint(
            health_url,
            "health_url",
            "/healthz",
        )
        _require(
            self._origin(self._semantic_url) == self._origin(self._health_url),
            "semantic_url and health_url must have the same origin",
        )
        _require(
            isinstance(bearer_token, str)
            and bool(bearer_token)
            and bearer_token == bearer_token.strip()
            and bearer_token.isascii()
            and len(bearer_token.encode("ascii")) <= 8192
            and all(33 <= ord(character) <= 126 for character in bearer_token),
            f"{CONTAINER_NODE_BEARER_TOKEN_ENV} is required and must be "
            "printable ASCII",
        )
        self._authorization = "Bearer " + bearer_token
        self._expected_execution_node_id = _text(
            expected_execution_node_id,
            "expected_execution_node_id",
        )
        _require(
            isinstance(timeout_seconds, (int, float))
            and not isinstance(timeout_seconds, bool)
            and math.isfinite(float(timeout_seconds))
            and float(timeout_seconds) > 0.0,
            "timeout_seconds is invalid",
        )
        self._timeout_seconds = float(timeout_seconds)
        self._max_request_bytes = _integer(
            max_request_bytes, "max_request_bytes", minimum=1
        )
        self._max_response_bytes = _integer(
            max_response_bytes, "max_response_bytes", minimum=1
        )
        # N6 is a loopback-only container endpoint carrying the rendered
        # question and base64 JPEGs.  Ambient HTTP_PROXY/ALL_PROXY settings
        # must not route that private request through another process.
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _NoRedirect(),
        )
        self._health_verified = False
        self._last_runtime_epoch: str | None = None

    @staticmethod
    def _endpoint(value: Any, name: str, expected_path: str) -> str:
        endpoint = _text(value, name)
        parsed = urllib.parse.urlsplit(endpoint)
        _require(
            parsed.scheme == "http" and parsed.hostname == "127.0.0.1",
            f"{name} must use literal http://127.0.0.1",
        )
        _require(
            parsed.username is None and parsed.password is None,
            f"{name} must not contain credentials",
        )
        try:
            port = parsed.port
        except ValueError as exc:
            raise DataAgentSemanticVerticalError(
                f"{name} port is invalid"
            ) from exc
        _require(port is not None, f"{name} must include an explicit port")
        _require(
            parsed.netloc == f"127.0.0.1:{port}",
            f"{name} authority is not canonical",
        )
        _require(
            not parsed.query and not parsed.fragment,
            f"{name} contains unsafe metadata",
        )
        _require(parsed.path == expected_path, f"{name} path is invalid")
        return endpoint

    @staticmethod
    def _origin(url: str) -> tuple[str, str, int]:
        parsed = urllib.parse.urlsplit(url)
        try:
            port = parsed.port
        except ValueError as exc:
            raise DataAgentSemanticVerticalError(
                "container endpoint port is invalid"
            ) from exc
        return (
            parsed.scheme,
            str(parsed.hostname).casefold(),
            port if port is not None else (443 if parsed.scheme == "https" else 80),
        )

    @property
    def health_verified(self) -> bool:
        return self._health_verified

    @property
    def last_runtime_epoch(self) -> str | None:
        return self._last_runtime_epoch

    def _request_json(
        self,
        *,
        url: str,
        method: str,
        payload: Mapping[str, Any] | None = None,
        max_bytes: int,
        authenticated: bool = False,
    ) -> dict[str, Any]:
        body = None if payload is None else _canonical_bytes(payload)
        if body is not None:
            _require(
                len(body) <= self._max_request_bytes,
                "container semantic request exceeds its byte limit",
            )
        headers = {
            "Accept": "application/json",
            "User-Agent": "pathfinder-data-agent-semantic-vertical/1",
        }
        if authenticated:
            headers["Authorization"] = self._authorization
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with self._opener.open(
                request,
                timeout=self._timeout_seconds,
            ) as response:
                _require(
                    response.status == 200,
                    "container endpoint returned a non-200 response",
                )
                content_type = response.headers.get_content_type()
                _require(
                    content_type == "application/json",
                    "container endpoint returned non-JSON content",
                )
                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    try:
                        declared = int(content_length)
                    except ValueError as exc:
                        raise DataAgentSemanticVerticalError(
                            "container response Content-Length is invalid"
                        ) from exc
                    _require(
                        0 <= declared <= max_bytes,
                        "container response exceeds its byte limit",
                    )
                raw = response.read(max_bytes + 1)
                _require(
                    len(raw) <= max_bytes,
                    "container response exceeds its byte limit",
                )
        except urllib.error.HTTPError as exc:
            raise DataAgentSemanticVerticalError(
                f"container endpoint returned HTTP {exc.code}"
            ) from exc
        except urllib.error.URLError as exc:
            raise DataAgentSemanticVerticalError(
                "container endpoint is unreachable"
            ) from exc
        except OSError as exc:
            raise DataAgentSemanticVerticalError(
                "container endpoint transport failed"
            ) from exc
        value = _strict_json_value(raw, "container endpoint response")
        _require(isinstance(value, dict), "container endpoint JSON is not an object")
        return value

    def _health(self) -> str:
        health = self._request_json(
            url=self._health_url,
            method="GET",
            max_bytes=self._max_response_bytes,
        )
        _require(health.get("status") == "ok", "container semantic health is not ok")
        _require(
            health.get("node_id") == self._expected_execution_node_id,
            "container semantic health node changed",
        )
        _require(
            health.get("credentials_recorded") is False,
            "container semantic health reports recorded credentials",
        )
        _require(
            health.get("semantic_quality_enabled") is True,
            "container semantic execution is disabled",
        )
        _require(
            health.get("semantic_llm_configured") is True,
            "container semantic LLM is not configured",
        )
        _require(
            health.get("semantic_vision_request_adapter_supported") is True,
            "container semantic vision request adapter is unsupported",
        )
        _require(
            health.get("semantic_vision_request_schema_version")
            == CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION_V2,
            "container semantic vision request schema changed",
        )
        epoch = _text(health.get("runtime_epoch"), "container runtime_epoch")
        _require(
            re.fullmatch(r"[0-9a-f]{32}", epoch) is not None,
            "container runtime_epoch is invalid",
        )
        return epoch

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        self._health_verified = False
        self._last_runtime_epoch = None
        before = self._health()
        result = self._request_json(
            url=self._semantic_url,
            method="POST",
            payload=request,
            max_bytes=self._max_response_bytes,
            authenticated=True,
        )
        after = self._health()
        _require(
            before == after,
            "container runtime epoch changed during semantic execution",
        )
        self._last_runtime_epoch = before
        self._health_verified = True
        return result


def _require(condition: object, message: str) -> None:
    if not condition:
        raise DataAgentSemanticVerticalError(message)


def _text(value: Any, name: str) -> str:
    _require(isinstance(value, str) and bool(value.strip()), f"{name} is invalid")
    return str(value).strip()


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value >= minimum,
        f"{name} is invalid",
    )
    return int(value)


def _number(value: Any, name: str) -> float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0.0,
        f"{name} is invalid",
    )
    return float(value)


def _digest(value: Any, name: str) -> str:
    text = _text(value, name)
    _require(_SHA256.fullmatch(text) is not None, f"{name} is invalid")
    return text


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_path(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256_bytes(payload)}  {name}\n".encode("utf-8")
        for name, payload in sorted(documents.items())
    )


def _strict_json_value(raw: bytes | str, name: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, child in pairs:
            if key in value:
                raise DataAgentSemanticVerticalError(
                    f"{name} contains a duplicate key"
                )
            value[key] = child
        return value

    def reject_constant(value: str) -> None:
        del value
        raise DataAgentSemanticVerticalError(
            f"{name} contains a non-finite JSON number"
        )

    try:
        return json.loads(
            raw,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except DataAgentSemanticVerticalError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise DataAgentSemanticVerticalError(f"{name} is unreadable") from exc


def _read_json(path: Path, name: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise DataAgentSemanticVerticalError(f"{name} is unreadable") from exc
    value = _strict_json_value(raw, name)
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise DataAgentSemanticVerticalError(f"{name} is unreadable") from exc
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        _require(bool(line.strip()), f"{name} contains a blank row")
        row = _strict_json_value(line, f"{name} row {index}")
        _require(isinstance(row, dict), f"{name} row {index} is not an object")
        rows.append(row)
    return rows


def load_data_agent_frame_bundle_semantic_spec(
    path: str | Path,
) -> DataAgentFrameBundleSemanticSpec:
    """Load and strictly validate one frozen semantic vertical spec."""

    source = Path(path).resolve()
    raw = source.read_bytes() if source.is_file() else b""
    _require(bool(raw), "semantic vertical spec is missing or empty")
    document = _read_json(source, "semantic vertical spec")
    _require(set(document) == _SPEC_KEYS, "semantic vertical spec field set changed")
    _require(
        document.get("schema_version")
        == DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
        "unsupported semantic vertical spec schema",
    )
    for name in (
        "semantic_run_id",
        "trial_key",
        "semantic_executor_node_id",
        "representation_id",
        "data_agent_route_design_id",
        "data_agent_plan_id",
        "workload_id",
        "task_class_id",
        "artifact_object_id",
        "object_catalog_version",
        "question",
        "expected_model",
    ):
        _text(document.get(name), f"semantic spec {name}")
    _require(
        document.get("semantic_executor_node_id")
        == DATA_AGENT_SEMANTIC_EXECUTOR_NODE_ID,
        "semantic spec semantic_executor_node_id must be N6",
    )
    _integer(
        document.get("data_agent_plan_epoch"),
        "semantic spec data_agent_plan_epoch",
    )
    _digest(document.get("artifact_sha256"), "semantic spec artifact_sha256")
    _integer(
        document.get("artifact_size_bytes"),
        "semantic spec artifact_size_bytes",
        minimum=1,
    )
    _require(
        document.get("credentials_recorded") is False,
        "semantic vertical spec records credentials",
    )
    try:
        contract = load_workload_scoring_contract(
            {
                "object_id": document["artifact_object_id"],
                "question": document["question"],
                "answer_options": document["answer_options"],
                "correct_answer_id": document["correct_answer_id"],
            },
            str(document.get("success_scoring_rule")),
            name="semantic vertical spec",
        )
    except Exception as exc:
        raise DataAgentSemanticVerticalError(
            "semantic vertical spec scoring contract is invalid"
        ) from exc
    _require(
        contract.rule
        in {
            MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        },
        "semantic vertical spec requires a deterministic multiple-choice rule",
    )
    return DataAgentFrameBundleSemanticSpec(
        source_path=source,
        source_sha256=_sha256_bytes(raw),
        document=json.loads(json.dumps(document)),
    )


def _data_agent_access_id(
    *,
    semantic_run_id: str,
    trial_key: str,
    artifact_object_id: str,
    representation_id: str,
    event_index: int,
) -> str:
    """Derive one retry-safe Data Agent access identity.

    A semantic request remains idempotent across infrastructure retries, but
    each Data Agent delivery is a new measured event.  Including the frozen
    matrix trial and explicit event index prevents telemetry from two bundle
    downloads being silently accumulated under one access ID.
    """

    return str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        (
            "pathfinder-data-agent-semantic-access:"
            f"{semantic_run_id}:{trial_key}:{artifact_object_id}:"
            f"{representation_id}:{event_index}"
        ),
    ))


def _semantic_request_id(
    *,
    semantic_run_id: str,
    trial_key: str,
    execution_node_id: str,
    representation_id: str,
    representation_sha256: str,
    question_sha256: str,
    prompt_sha256: str,
    frame_sequence_sha256: str,
) -> str:
    """Bind the semantic request identity to all frozen semantic inputs."""

    return _sha256_bytes(
        _canonical_bytes({
            "semantic_run_id": semantic_run_id,
            "trial_key": trial_key,
            "execution_node_id": execution_node_id,
            "representation_id": representation_id,
            "representation_sha256": representation_sha256,
            "question_sha256": question_sha256,
            "prompt_sha256": prompt_sha256,
            "frame_sequence_sha256": frame_sequence_sha256,
        })
    )


def _validated_declared_option_answer(answer: Any, contract: Any) -> str:
    """Accept only one short option response under the frozen grammar.

    This is an evidence-redaction boundary, not a scorer: a declared but
    incorrect option remains a valid response and is subsequently scored
    false.  Reusing ``evaluate_workload_answer`` with each declared option as
    the provisional correct ID keeps this gate exactly aligned with the
    shared exact/canonical marker grammar without extracting from prose.
    """

    _require(
        isinstance(answer, str) and bool(answer.strip()),
        "container final_answer is invalid",
    )
    _require(
        len(answer.encode("utf-8")) <= _MAX_PERSISTED_OPTION_ANSWER_BYTES,
        "container final_answer exceeds the option-response byte limit",
    )
    value = answer.strip()
    matches = [
        option.option_id
        for option in contract.answer_options
        if evaluate_workload_answer(
            value,
            replace(contract, correct_answer_id=option.option_id),
        )
        is True
    ]
    _require(
        len(matches) == 1,
        "container final_answer is not one declared option response",
    )
    return value


def verify_flowmesh_container_matrix_plan(
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Resolve the FlowMesh verifier lazily while preserving the test seam."""
    from ..integrations.flowmesh.container_matrix import (
        verify_flowmesh_container_matrix_plan as verify,
    )

    return verify(plan_dir)


def _matrix_binding(
    matrix_plan_dir: str | Path,
    trial_key: str,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    root = Path(matrix_plan_dir).resolve()
    try:
        verified = verify_flowmesh_container_matrix_plan(root)
    except Exception as exc:
        raise DataAgentSemanticVerticalError(
            "container matrix plan does not verify"
        ) from exc
    _require(verified.get("status") == "VERIFIED", "matrix plan is not verified")
    plan = _read_json(
        root / "flowmesh-container-matrix-plan.json",
        "container matrix plan",
    )
    trials = _read_jsonl(
        root / "flowmesh-container-matrix-trials.jsonl",
        "container matrix trials",
    )
    key = _text(trial_key, "trial_key")
    matches = [row for row in trials if row.get("trial_key") == key]
    _require(len(matches) == 1, "trial_key does not name exactly one matrix trial")
    _require(
        plan.get("plan_sha256") == verified.get("plan_sha256"),
        "matrix verifier and plan digest differ",
    )
    return root, plan, matches[0]


def _validate_workload(
    workload: Mapping[str, Any],
    trial: Mapping[str, Any],
) -> tuple[dict[str, Any], Any, str]:
    _require(isinstance(workload, Mapping), "semantic workload must be an object")
    copied = dict(workload)
    workload_id = _text(copied.get("workload_id"), "workload.workload_id")
    _require(
        workload_id == trial.get("workload_id"),
        "semantic workload_id differs from the bound matrix trial",
    )
    artifact_object_id = _text(
        copied.get("object_id"), "workload.object_id"
    )
    rule = _text(
        copied.get("success_scoring_rule"),
        "workload.success_scoring_rule",
    )
    _require(
        rule
        in {
            MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        },
        "vertical slice requires a deterministic multiple-choice scoring rule",
    )
    try:
        contract = load_workload_scoring_contract(
            copied,
            rule,
            name="semantic workload",
        )
    except Exception as exc:
        raise DataAgentSemanticVerticalError(
            "semantic workload scoring contract is invalid"
        ) from exc
    question = render_workload_question(copied, contract)
    _text(question, "rendered question")
    return copied, contract, artifact_object_id


def _semantic_helpers() -> tuple[Any, Any, str]:
    # Imported lazily so this isolated module does not change the container
    # runtime's import graph.  Both helpers are the runtime contract: copying
    # their algorithms here would allow the runner and node to drift.
    try:
        from .container_node import (
            CONTAINER_NODE_API_VERSION,
            ContainerNodeRuntime,
            semantic_frame_sequence_sha256,
        )
    except ImportError as exc:
        raise DataAgentSemanticVerticalError(
            "container semantic-v2 helpers are unavailable"
        ) from exc
    return (
        ContainerNodeRuntime,
        semantic_frame_sequence_sha256,
        CONTAINER_NODE_API_VERSION,
    )


def _vision_request(
    *,
    semantic_run_id: str,
    trial_key: str,
    semantic_executor_node_id: str,
    representation_id: str,
    representation_sha256: str,
    question: str,
    transfer: FrameBundleTransfer,
) -> tuple[dict[str, Any], str, str]:
    runtime, sequence_digest, _api_version = _semantic_helpers()
    frames: list[dict[str, Any]] = []
    for expected_index, frame in enumerate(transfer.bundle.vision_frames()):
        _require(
            frame.frame_index == expected_index,
            "validated frame sequence is not zero-based and contiguous",
        )
        frames.append({
            "frame_index": frame.frame_index,
            "timestamp_seconds": frame.timestamp_seconds,
            "width": frame.width,
            "height": frame.height,
            "jpeg_size_bytes": len(frame.jpeg_bytes),
            "jpeg_sha256": _sha256_bytes(frame.jpeg_bytes),
            "jpeg_base64": base64.b64encode(frame.jpeg_bytes).decode("ascii"),
        })
    _require(bool(frames), "validated frame bundle contains no frames")
    frame_sequence_sha256 = _digest(
        sequence_digest(frames), "frame_sequence_sha256"
    )
    prompt = runtime.build_semantic_vision_prompt(
        representation_id,
        len(frames),
        question,
    )
    prompt_sha256 = _sha256_bytes(prompt.encode("utf-8"))
    semantic_request_id = _semantic_request_id(
        semantic_run_id=semantic_run_id,
        trial_key=trial_key,
        execution_node_id=semantic_executor_node_id,
        representation_id=representation_id,
        representation_sha256=representation_sha256,
        question_sha256=_sha256_bytes(question.encode("utf-8")),
        prompt_sha256=prompt_sha256,
        frame_sequence_sha256=frame_sequence_sha256,
    )
    request = {
        "schema_version": CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION_V2,
        "semantic_request_id": semantic_request_id,
        "execution_node_id": semantic_executor_node_id,
        "representation_id": representation_id,
        "representation_sha256": representation_sha256,
        "question": question,
        "prompt_sha256": prompt_sha256,
        "frame_sequence_sha256": frame_sequence_sha256,
        "frames": frames,
    }
    return request, prompt_sha256, frame_sequence_sha256


def _validate_container_result(
    result: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    expected_model: str,
) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "container semantic result is invalid")
    copied = dict(result)
    _require(
        set(copied) == _CONTAINER_RESULT_KEYS,
        "container semantic result field set changed",
    )
    _require(
        copied.get("schema_version")
        == CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION_V2,
        "container semantic result schema changed",
    )
    _runtime, _sequence_digest, api_version = _semantic_helpers()
    _require(
        copied.get("api_version") == api_version,
        "container semantic API version changed",
    )
    _require(copied.get("status") == "completed", "container semantic call failed")
    _require(
        copied.get("outcome_type") == "completed",
        "container semantic outcome is not completed",
    )
    for field in (
        "semantic_request_id",
        "execution_node_id",
        "prompt_sha256",
        "representation_sha256",
        "frame_sequence_sha256",
    ):
        _require(copied.get(field) == request[field], f"container changed {field}")
    request_sha256 = _sha256_bytes(_canonical_bytes(request))
    _require(
        copied.get("request_sha256") == request_sha256,
        "container request digest changed",
    )
    _require(
        copied.get("frame_count") == len(request["frames"]),
        "container frame count changed",
    )
    delivered = sum(frame["jpeg_size_bytes"] for frame in request["frames"])
    _require(
        copied.get("representation_delivery_bytes") == delivered,
        "container representation byte count changed",
    )
    _require(
        copied.get("semantic_input_kind") == "ordered-jpeg-frames",
        "container semantic input kind changed",
    )
    _require(
        copied.get("data_plane_artifact_delivery_verified") is False,
        "container overclaimed Data Agent provenance",
    )
    _require(
        copied.get("semantic_frame_payload_integrity_verified") is True,
        "container did not verify the received frame payload",
    )
    _require(
        copied.get("source_node_id") is None,
        "container unexpectedly claims a direct source-node fetch",
    )
    _require(
        copied.get("telemetry_complete") is True,
        "semantic telemetry is incomplete",
    )
    _require(copied.get("llm_called") is True, "semantic adapter did not call the LLM")
    _require(
        copied.get("credentials_recorded") is False,
        "semantic adapter recorded credentials",
    )
    _require(
        type(copied.get("idempotent_replay")) is bool,
        "idempotent replay flag is invalid",
    )
    started = _integer(
        copied.get("started_monotonic_ns"), "started_monotonic_ns"
    )
    finished = _integer(
        copied.get("finished_monotonic_ns"), "finished_monotonic_ns"
    )
    _require(finished >= started, "container monotonic interval is invalid")
    _number(copied.get("service_time_ms"), "semantic service_time_ms")
    answer = copied.get("final_answer")
    _require(
        isinstance(answer, str) and bool(answer.strip()),
        "container final_answer is invalid",
    )
    _require(
        copied.get("final_answer_sha256")
        == _sha256_bytes(answer.encode("utf-8")),
        "container answer digest changed",
    )
    _require(
        copied.get("model") == expected_model,
        "container model differs from the frozen semantic spec",
    )
    return copied


def _frame_bundle_evidence(
    transfer: FrameBundleTransfer,
    frame_sequence_sha256: str,
) -> dict[str, Any]:
    frames = [
        {
            "frame_index": frame.frame_index,
            "timestamp_seconds": frame.timestamp_seconds,
            "width": frame.width,
            "height": frame.height,
            "jpeg_size_bytes": len(frame.jpeg_bytes),
            "jpeg_sha256": _sha256_bytes(frame.jpeg_bytes),
            "media_type": frame.media_type,
        }
        for frame in transfer.bundle.vision_frames()
    ]
    return {
        "representation_id": transfer.bundle.representation_id,
        "representation_sha256": transfer.artifact.sha256,
        "artifact_media_type": transfer.bundle.artifact_media_type,
        "artifact_size_bytes": transfer.bundle.artifact_size_bytes,
        "artifact_sha256": transfer.bundle.artifact_sha256,
        "manifest_sha256": transfer.bundle.manifest_sha256,
        "frame_count": transfer.bundle.frame_count,
        "total_jpeg_bytes": transfer.bundle.total_jpeg_bytes,
        "frame_sequence_sha256": frame_sequence_sha256,
        "frames": frames,
    }


def execute_data_agent_frame_bundle_semantic_trial(
    *,
    matrix_plan_dir: str | Path,
    semantic_spec: str | Path,
    endpoint_registry: EndpointRegistry,
    clients_by_endpoint_id: Mapping[str, Any],
    adapter: ContainerSemanticVisionAdapter,
    output_dir: str | Path,
    event_index: int = 0,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
    quiescence_timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    """Execute and atomically record one bound semantic vertical trial."""

    target = Path(output_dir).resolve()
    _require(not target.exists(), "semantic vertical output directory already exists")
    spec = load_data_agent_frame_bundle_semantic_spec(semantic_spec)
    _text(endpoint_registry.registry_id, "endpoint_registry.registry_id")
    _digest(
        endpoint_registry.source_sha256,
        "endpoint_registry.source_sha256",
    )
    _require(
        isinstance(clients_by_endpoint_id, Mapping),
        "clients_by_endpoint_id must be a mapping",
    )
    run_id = str(spec.document["semantic_run_id"])
    executor = str(spec.document["semantic_executor_node_id"])
    rep_id = str(spec.document["representation_id"])
    event = _integer(event_index, "event_index")
    matrix_root, matrix_plan, trial = _matrix_binding(
        matrix_plan_dir,
        str(spec.document["trial_key"]),
    )
    semantic_workload, contract, artifact_object_id = _validate_workload(
        spec.workload(), trial
    )
    route_design_id = _text(
        semantic_workload.get("data_agent_route_design_id"),
        "workload.data_agent_route_design_id",
    )
    data_agent_plan_id = _text(
        semantic_workload.get("data_agent_plan_id"),
        "workload.data_agent_plan_id",
    )
    data_agent_plan_epoch = _integer(
        semantic_workload.get("data_agent_plan_epoch", 0),
        "workload.data_agent_plan_epoch",
    )
    expected_model = _text(
        spec.document.get("expected_model"),
        "semantic spec expected_model",
    )
    route = endpoint_registry.route(
        design_id=route_design_id,
        representation_id=rep_id,
    )
    _require(
        route.endpoint_id in clients_by_endpoint_id,
        "no Data Agent client is configured for the exact route",
    )
    request = build_frame_bundle_access_request(
        object_id=artifact_object_id,
        plan_id=data_agent_plan_id,
        requested_location=route.source_location,
        session_id=run_id,
        trial_id=_text(trial.get("trial_id"), "matrix trial_id"),
        task_class_id=_text(
            semantic_workload.get("task_class_id", trial.get("workload_class")),
            "semantic task_class_id",
        ),
        representation_id=rep_id,
        access_id=_data_agent_access_id(
            semantic_run_id=run_id,
            trial_key=str(trial["trial_key"]),
            artifact_object_id=artifact_object_id,
            representation_id=rep_id,
            event_index=event,
        ),
        plan_epoch=data_agent_plan_epoch,
        event_index=event,
    )
    expected_sha256 = _digest(
        semantic_workload.get("expected_artifact_sha256"),
        "workload.expected_artifact_sha256",
    )
    expected_size = _integer(
        semantic_workload.get("expected_artifact_size_bytes"),
        "workload.expected_artifact_size_bytes",
        minimum=1,
    )
    expected_catalog = _text(
        semantic_workload.get("expected_object_catalog_version"),
        "workload.expected_object_catalog_version",
    )
    transfer = fetch_validated_frame_bundle(
        clients_by_endpoint_id[route.endpoint_id],
        request,
        expected_object_id=artifact_object_id,
        expected_artifact_sha256=expected_sha256,
        expected_artifact_size_bytes=expected_size,
        expected_object_catalog_version=expected_catalog,
        limits=limits,
        quiescence_timeout_seconds=quiescence_timeout_seconds,
    )
    _require(
        transfer.access_request == request,
        "frame-bundle transfer returned a different access request",
    )
    _require(
        transfer.artifact.access_id == request.access_id,
        "Data Agent access ID changed during bundle transfer",
    )
    _require(
        transfer.artifact.location == route.source_location,
        "Data Agent artifact location differs from the exact route",
    )
    _require(
        transfer.bundle.representation_id == rep_id,
        "frame bundle representation_id changed",
    )
    _require(
        transfer.delivery.exactly_one_full_download,
        "semantic evidence requires exactly one complete artifact download",
    )
    _require(
        transfer.delivery.bytes_sent_equals_artifact_size,
        "semantic evidence requires exact artifact-byte accounting",
    )
    question = render_workload_question(semantic_workload, contract)
    semantic_request, prompt_sha256, frame_sequence_sha256 = _vision_request(
        semantic_run_id=run_id,
        trial_key=str(trial["trial_key"]),
        semantic_executor_node_id=executor,
        representation_id=rep_id,
        representation_sha256=transfer.artifact.sha256,
        question=question,
        transfer=transfer,
    )
    result = _validate_container_result(
        adapter.execute(json.loads(json.dumps(semantic_request))),
        request=semantic_request,
        expected_model=expected_model,
    )
    _require(
        getattr(adapter, "health_verified", False) is True,
        "container semantic adapter did not verify pre/post health",
    )
    semantic_runtime_epoch = _text(
        getattr(adapter, "last_runtime_epoch", None),
        "container semantic runtime_epoch",
    )
    _require(
        re.fullmatch(r"[0-9a-f]{32}", semantic_runtime_epoch) is not None,
        "container semantic runtime_epoch is invalid",
    )
    final_answer = _validated_declared_option_answer(
        result["final_answer"],
        contract,
    )
    task_success = evaluate_workload_answer(final_answer, contract)
    _require(type(task_success) is bool, "semantic score is not boolean")
    frame_bundle = _frame_bundle_evidence(transfer, frame_sequence_sha256)
    record = {
        "schema_version": DATA_AGENT_SEMANTIC_RECORD_SCHEMA_VERSION,
        "status": "COMPLETE",
        "semantic_run_id": run_id,
        "semantic_request_id": semantic_request["semantic_request_id"],
        "matrix_id": matrix_plan["matrix_id"],
        "matrix_plan_sha256": matrix_plan["plan_sha256"],
        "trial_key": trial["trial_key"],
        "trial_id": trial["trial_id"],
        "workload_id": trial["workload_id"],
        "workload_class": trial["workload_class"],
        "matrix_design_id": trial["design_id"],
        "repetition": trial["repetition"],
        "matrix_object_id": trial["object_id"],
        "artifact_object_id": artifact_object_id,
        "matrix_executor_node_id": trial["executor_node_id"],
        "semantic_executor_node_id": executor,
        "representation_id": rep_id,
        "endpoint_registry_id": endpoint_registry.registry_id,
        "endpoint_registry_sha256": endpoint_registry.source_sha256,
        "semantic_spec_sha256": spec.source_sha256,
        "route": route.to_public_dict(),
        "data_agent_route_design_id": route_design_id,
        "data_agent_plan_id": request.plan_id,
        "data_agent_plan_epoch": request.plan_epoch,
        "event_index": request.event_index,
        "data_agent_access_id": request.access_id,
        "artifact": transfer.artifact.to_metadata_dict(),
        "frame_bundle": frame_bundle,
        "delivery": transfer.delivery.to_dict(),
        "latency_ms": transfer.latency_ms(),
        "question_sha256": _sha256_bytes(question.encode("utf-8")),
        "prompt_sha256": prompt_sha256,
        "frame_sequence_sha256": frame_sequence_sha256,
        "container_request_sha256": result["request_sha256"],
        "container_result_schema_version": result["schema_version"],
        "semantic_input_kind": result["semantic_input_kind"],
        "representation_delivery_bytes": result[
            "representation_delivery_bytes"
        ],
        "success_scoring_rule": contract.rule,
        "answer_option_ids": [
            option.option_id for option in contract.answer_options
        ],
        "correct_answer_id": contract.correct_answer_id,
        "final_answer": final_answer,
        "final_answer_sha256": _sha256_bytes(
            final_answer.encode("utf-8")
        ),
        "task_success": task_success,
        "expected_model": expected_model,
        "model": result["model"],
        "semantic_service_time_ms": result["service_time_ms"],
        "semantic_runtime_epoch": semantic_runtime_epoch,
        "semantic_health_verified": True,
        "idempotent_replay": result["idempotent_replay"],
        "container_data_plane_artifact_delivery_verified": False,
        "semantic_frame_payload_integrity_verified": True,
        "data_agent_artifact_delivery_verified": True,
        "container_semantic_response_consistency_verified": True,
        "container_runtime_code_provenance_verified": False,
        "scoring_verified": True,
        "execution_and_semantic_route_unified": False,
        "llm_called": True,
        "semantic_telemetry_complete": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    _require(set(record) == _RECORD_KEYS, "internal semantic record field drift")
    record_bytes = _json_bytes(record)
    manifest = {
        "schema_version": DATA_AGENT_SEMANTIC_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "semantic_run_id": run_id,
        "matrix_id": matrix_plan["matrix_id"],
        "matrix_plan_sha256": matrix_plan["plan_sha256"],
        "matrix_plan_file_sha256": _sha256_path(
            matrix_root / "flowmesh-container-matrix-plan.json"
        ),
        "trial_key": trial["trial_key"],
        "event_index": request.event_index,
        "endpoint_registry_id": endpoint_registry.registry_id,
        "endpoint_registry_sha256": endpoint_registry.source_sha256,
        "semantic_spec_sha256": spec.source_sha256,
        "record_count": 1,
        "record_sha256": _sha256_bytes(record_bytes),
        "artifact_size_bytes": transfer.artifact.size_bytes,
        "artifact_sha256": transfer.artifact.sha256,
        "frame_count": transfer.bundle.frame_count,
        "task_success_count": int(task_success),
        "expected_model": expected_model,
        "models": [result["model"]],
        "semantic_runtime_epoch": semantic_runtime_epoch,
        "semantic_health_verified": True,
        "data_agent_artifact_delivery_verified": True,
        "container_semantic_response_consistency_verified": True,
        "container_runtime_code_provenance_verified": False,
        "scoring_verified": True,
        "execution_and_semantic_route_unified": False,
        "llm_called": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "limitations": [
            (
                "The current infrastructure matrix uses a synthetic object "
                "while the semantic path uses a separately frozen benchmark "
                "artifact."
            ),
            (
                "The Data Agent delivery and container semantic request are "
                "bound in one audited trial, but are not yet the same route "
                "as every synthetic infrastructure operation."
            ),
            (
                "The host coordinator fetches from the Data Agent and invokes "
                "N6 directly; this is not a FlowMesh-scheduled semantic "
                "workflow."
            ),
            (
                "This single-trial vertical slice is integration evidence, "
                "not a performance or policy-effectiveness result."
            ),
            (
                "The mutable container image was not bound to a frozen image "
                "digest or source revision, so runtime code provenance is "
                "not verified."
            ),
        ],
    }
    _require(set(manifest) == _MANIFEST_KEYS, "internal semantic manifest field drift")
    documents = {
        RECORD_NAME: record_bytes,
        MANIFEST_NAME: _json_bytes(manifest),
    }
    documents["SHA256SUMS"] = _checksum_bytes(documents)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(
        tempfile.mkdtemp(prefix=".data-agent-semantic-", dir=target.parent)
    )
    staging = temporary_root / "output"
    try:
        staging.mkdir()
        for name, payload in documents.items():
            (staging / name).write_bytes(payload)
        verify_data_agent_frame_bundle_semantic_trial(
            output_dir=staging,
            matrix_plan_dir=matrix_root,
            endpoint_registry=endpoint_registry,
            semantic_spec=spec.source_path,
        )
        os.replace(staging, target)
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)
    return {**manifest, "output_dir": str(target)}


def _verify_checksum_set(root: Path) -> None:
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(
        actual == _OUTPUT_FILES | {"SHA256SUMS"},
        "semantic output file set changed",
    )
    rows: dict[str, str] = {}
    try:
        lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise DataAgentSemanticVerticalError(
            "semantic checksum file is unreadable"
        ) from exc
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _OUTPUT_FILES,
            "invalid semantic checksum row",
        )
        _digest(digest, "semantic checksum")
        _require(name not in rows, "duplicate semantic checksum row")
        _require(_sha256_path(root / name) == digest, "semantic checksum mismatch")
        rows[name] = digest
    _require(set(rows) == _OUTPUT_FILES, "semantic checksum set is incomplete")


def _no_binary_evidence(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key in {"jpeg_base64", "question", "prompt", "api_key", "token"}:
                return False
            if not _no_binary_evidence(child):
                return False
    elif isinstance(value, list):
        return all(_no_binary_evidence(item) for item in value)
    return True


def verify_data_agent_frame_bundle_semantic_trial(
    *,
    output_dir: str | Path,
    matrix_plan_dir: str | Path,
    endpoint_registry: EndpointRegistry,
    semantic_spec: str | Path,
) -> dict[str, Any]:
    """Verify a vertical-slice evidence package without network or LLM use."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), "semantic vertical output directory is missing")
    _verify_checksum_set(root)
    record = _read_json(root / RECORD_NAME, "semantic record")
    manifest = _read_json(root / MANIFEST_NAME, "semantic manifest")
    spec = load_data_agent_frame_bundle_semantic_spec(semantic_spec)
    _require(set(record) == _RECORD_KEYS, "semantic record field set changed")
    _require(set(manifest) == _MANIFEST_KEYS, "semantic manifest field set changed")
    _require(
        _no_binary_evidence(record),
        "semantic evidence contains private request content",
    )
    _require(
        record.get("schema_version") == DATA_AGENT_SEMANTIC_RECORD_SCHEMA_VERSION,
        "unsupported semantic record schema",
    )
    _require(
        manifest.get("schema_version")
        == DATA_AGENT_SEMANTIC_MANIFEST_SCHEMA_VERSION,
        "unsupported semantic manifest schema",
    )
    _require(record.get("status") == "COMPLETE", "semantic record is incomplete")
    _require(manifest.get("status") == "COMPLETE", "semantic manifest is incomplete")
    _digest(record.get("semantic_request_id"), "semantic_request_id")
    _digest(record.get("matrix_plan_sha256"), "matrix_plan_sha256")
    _digest(record.get("endpoint_registry_sha256"), "endpoint_registry_sha256")
    matrix_root, matrix_plan, trial = _matrix_binding(
        matrix_plan_dir,
        _text(record.get("trial_key"), "record.trial_key"),
    )
    expected_trial = {
        "trial_key": trial.get("trial_key"),
        "trial_id": trial.get("trial_id"),
        "workload_id": trial.get("workload_id"),
        "workload_class": trial.get("workload_class"),
        "matrix_design_id": trial.get("design_id"),
        "repetition": trial.get("repetition"),
        "matrix_object_id": trial.get("object_id"),
        "matrix_executor_node_id": trial.get("executor_node_id"),
    }
    for field, expected in expected_trial.items():
        _require(record.get(field) == expected, f"record {field} changed")
    spec_bindings = {
        "semantic_run_id": spec.document["semantic_run_id"],
        "trial_key": spec.document["trial_key"],
        "semantic_executor_node_id": spec.document[
            "semantic_executor_node_id"
        ],
        "representation_id": spec.document["representation_id"],
        "data_agent_route_design_id": spec.document[
            "data_agent_route_design_id"
        ],
        "data_agent_plan_id": spec.document["data_agent_plan_id"],
        "data_agent_plan_epoch": spec.document["data_agent_plan_epoch"],
        "workload_id": spec.document["workload_id"],
        "artifact_object_id": spec.document["artifact_object_id"],
        "expected_model": spec.document["expected_model"],
    }
    for field, expected in spec_bindings.items():
        _require(record.get(field) == expected, f"record {field} differs from spec")
    _require(
        record.get("semantic_spec_sha256") == spec.source_sha256,
        "semantic spec digest changed",
    )
    _require(
        record.get("matrix_id") == matrix_plan.get("matrix_id"),
        "matrix_id changed",
    )
    _require(
        record.get("matrix_plan_sha256") == matrix_plan.get("plan_sha256"),
        "matrix plan binding changed",
    )
    route = endpoint_registry.route(
        design_id=_text(
            record.get("data_agent_route_design_id"),
            "data_agent_route_design_id",
        ),
        representation_id=str(record["representation_id"]),
    )
    _require(record.get("route") == route.to_public_dict(), "Data Agent route changed")
    _require(
        record.get("endpoint_registry_id") == endpoint_registry.registry_id,
        "endpoint registry ID changed",
    )
    _require(
        record.get("endpoint_registry_sha256") == endpoint_registry.source_sha256,
        "endpoint registry digest changed",
    )
    _text(record.get("data_agent_plan_id"), "data_agent_plan_id")
    _integer(record.get("data_agent_plan_epoch"), "data_agent_plan_epoch")
    event_index = _integer(record.get("event_index"), "event_index")
    access_id = _text(record.get("data_agent_access_id"), "data_agent_access_id")
    _require(
        access_id
        == _data_agent_access_id(
            semantic_run_id=str(record["semantic_run_id"]),
            trial_key=str(record["trial_key"]),
            artifact_object_id=str(record["artifact_object_id"]),
            representation_id=str(record["representation_id"]),
            event_index=event_index,
        ),
        "Data Agent access ID is not bound to the trial event",
    )
    _text(record.get("artifact_object_id"), "artifact_object_id")
    artifact = record.get("artifact")
    _require(isinstance(artifact, Mapping), "artifact metadata is invalid")
    _require(
        set(artifact)
        == {
            "access_id",
            "media_type",
            "size_bytes",
            "sha256",
            "object_id",
            "object_catalog_version",
            "location",
        },
        "artifact metadata field set changed",
    )
    _require(
        artifact.get("access_id") == record.get("data_agent_access_id"),
        "artifact access ID changed",
    )
    _require(
        artifact.get("object_id") == record.get("artifact_object_id"),
        "artifact object ID changed",
    )
    _require(
        artifact.get("media_type") == "application/x-tar",
        "artifact media type changed",
    )
    _require(
        artifact.get("location") == route.source_location,
        "artifact route changed",
    )
    artifact_size = _integer(artifact.get("size_bytes"), "artifact size", minimum=1)
    artifact_sha256 = _digest(artifact.get("sha256"), "artifact sha256")
    _require(
        artifact_size == spec.document["artifact_size_bytes"],
        "artifact size differs from spec",
    )
    _require(
        artifact_sha256 == spec.document["artifact_sha256"],
        "artifact digest differs from spec",
    )
    _require(
        artifact.get("object_catalog_version")
        == spec.document["object_catalog_version"],
        "artifact catalog differs from spec",
    )
    frame_bundle = record.get("frame_bundle")
    _require(isinstance(frame_bundle, Mapping), "frame bundle evidence is invalid")
    _require(
        set(frame_bundle)
        == {
            "representation_id",
            "representation_sha256",
            "artifact_media_type",
            "artifact_size_bytes",
            "artifact_sha256",
            "manifest_sha256",
            "frame_count",
            "total_jpeg_bytes",
            "frame_sequence_sha256",
            "frames",
        },
        "frame bundle evidence field set changed",
    )
    _require(
        frame_bundle.get("representation_id") == record.get("representation_id"),
        "bundle representation changed",
    )
    _require(
        frame_bundle.get("representation_sha256") == artifact_sha256,
        "representation digest changed",
    )
    _require(
        frame_bundle.get("artifact_size_bytes") == artifact_size,
        "bundle size changed",
    )
    _require(
        frame_bundle.get("artifact_sha256") == artifact_sha256,
        "bundle digest changed",
    )
    _require(
        frame_bundle.get("artifact_media_type") == "application/x-tar",
        "bundle media type changed",
    )
    _digest(frame_bundle.get("manifest_sha256"), "bundle manifest sha256")
    frame_count = _integer(frame_bundle.get("frame_count"), "frame count", minimum=1)
    frames = frame_bundle.get("frames")
    _require(
        isinstance(frames, list) and len(frames) == frame_count,
        "frame evidence count changed",
    )
    total_jpeg_bytes = 0
    previous_timestamp = -1.0
    for index, frame in enumerate(frames):
        _require(isinstance(frame, Mapping), "frame metadata is invalid")
        _require(
            set(frame)
            == {
                "frame_index",
                "timestamp_seconds",
                "width",
                "height",
                "jpeg_size_bytes",
                "jpeg_sha256",
                "media_type",
            },
            "frame metadata field set changed",
        )
        _require(frame.get("frame_index") == index, "frame order changed")
        timestamp = _number(frame.get("timestamp_seconds"), "frame timestamp")
        _require(timestamp > previous_timestamp, "frame timestamps are not increasing")
        previous_timestamp = timestamp
        _integer(frame.get("width"), "frame width", minimum=1)
        _integer(frame.get("height"), "frame height", minimum=1)
        total_jpeg_bytes += _integer(
            frame.get("jpeg_size_bytes"), "frame JPEG size", minimum=1
        )
        _digest(frame.get("jpeg_sha256"), "frame JPEG sha256")
        _require(frame.get("media_type") == "image/jpeg", "frame media type changed")
    _require(
        frame_bundle.get("total_jpeg_bytes") == total_jpeg_bytes,
        "total JPEG byte count changed",
    )
    sequence_sha256 = _digest(
        frame_bundle.get("frame_sequence_sha256"),
        "frame sequence sha256",
    )
    _require(
        record.get("frame_sequence_sha256") == sequence_sha256,
        "frame sequence binding changed",
    )
    delivery = record.get("delivery")
    _require(isinstance(delivery, Mapping), "delivery evidence is invalid")
    _require(set(delivery) == _DELIVERY_KEYS, "delivery evidence field set changed")
    _require(
        delivery.get("telemetry_supported") is True,
        "Data Agent telemetry is unsupported",
    )
    _require(
        delivery.get("telemetry_complete") is True,
        "Data Agent telemetry is incomplete",
    )
    _require(
        delivery.get("exactly_one_full_download") is True,
        "artifact download count changed",
    )
    _require(
        delivery.get("bytes_sent_equals_artifact_size") is True,
        "artifact bytes are not exact",
    )
    _require(
        delivery.get("artifact_size_bytes") == artifact_size,
        "delivery artifact size changed",
    )
    _require(
        delivery.get("in_flight_request_count") == 0,
        "Data Agent transfer was not quiescent",
    )
    for name in (
        "download_request_count",
        "completed_request_count",
        "full_download_count",
    ):
        _require(delivery.get(name) == 1, f"delivery {name} changed")
    _require(delivery.get("bytes_sent") == artifact_size, "delivery byte count changed")
    _number(
        delivery.get("server_reported_transfer_latency_ms"),
        "delivery transfer latency",
    )
    _require(
        delivery.get("telemetry_object_id")
        == record.get("artifact_object_id"),
        "delivery object identity changed",
    )
    _require(
        delivery.get("telemetry_object_catalog_version")
        == spec.document["object_catalog_version"],
        "delivery catalog identity changed",
    )
    _require(
        delivery.get("expected_object_catalog_version")
        == spec.document["object_catalog_version"],
        "delivery expected catalog changed",
    )
    latency = record.get("latency_ms")
    _require(
        isinstance(latency, Mapping)
        and set(latency)
        == {
            "data_agent_service",
            "client_access_round_trip",
            "artifact_download_elapsed",
            "server_reported_transfer",
        },
        "Data Agent latency evidence changed",
    )
    for name, value in latency.items():
        _number(value, f"latency_ms.{name}")
    _digest(record.get("question_sha256"), "question sha256")
    spec_workload = spec.workload()
    try:
        spec_contract = load_workload_scoring_contract(
            spec_workload,
            str(spec_workload["success_scoring_rule"]),
            name="semantic spec",
        )
    except Exception as exc:
        raise DataAgentSemanticVerticalError(
            "semantic spec scoring contract is invalid"
        ) from exc
    rendered_question = render_workload_question(spec_workload, spec_contract)
    rendered_question_sha256 = _sha256_bytes(
        rendered_question.encode("utf-8")
    )
    _require(
        record.get("question_sha256")
        == rendered_question_sha256,
        "question digest differs from spec",
    )
    runtime, _sequence_digest, _api_version = _semantic_helpers()
    expected_prompt = runtime.build_semantic_vision_prompt(
        str(record["representation_id"]),
        frame_count,
        rendered_question,
    )
    _require(
        record.get("prompt_sha256")
        == _sha256_bytes(expected_prompt.encode("utf-8")),
        "prompt digest differs from spec",
    )
    _require(
        record.get("semantic_request_id")
        == _semantic_request_id(
            semantic_run_id=str(spec.document["semantic_run_id"]),
            trial_key=str(trial["trial_key"]),
            execution_node_id=str(
                spec.document["semantic_executor_node_id"]
            ),
            representation_id=str(spec.document["representation_id"]),
            representation_sha256=artifact_sha256,
            question_sha256=rendered_question_sha256,
            prompt_sha256=str(record["prompt_sha256"]),
            frame_sequence_sha256=sequence_sha256,
        ),
        "semantic request ID is not bound to the frozen semantic inputs",
    )
    _digest(record.get("container_request_sha256"), "container request sha256")
    _require(
        record.get("container_result_schema_version")
        == CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION_V2,
        "container result schema changed",
    )
    _require(
        record.get("semantic_input_kind") == "ordered-jpeg-frames",
        "semantic input kind changed",
    )
    _require(
        record.get("representation_delivery_bytes") == total_jpeg_bytes,
        "semantic delivery bytes changed",
    )
    _require(
        record.get("success_scoring_rule")
        == spec.document["success_scoring_rule"],
        "scoring rule differs from spec",
    )
    expected_option_ids = [
        option.option_id for option in spec_contract.answer_options
    ]
    _require(
        record.get("answer_option_ids") == expected_option_ids,
        "answer option IDs differ from spec",
    )
    _require(
        record.get("correct_answer_id")
        == spec.document["correct_answer_id"],
        "correct answer differs from spec",
    )
    answer = _validated_declared_option_answer(
        record.get("final_answer"),
        spec_contract,
    )
    _require(
        record.get("final_answer_sha256")
        == _sha256_bytes(answer.encode("utf-8")),
        "final answer digest changed",
    )
    _require(
        evaluate_workload_answer(answer, spec_contract)
        is record.get("task_success"),
        "semantic score changed",
    )
    _require(
        record.get("model") == spec.document["expected_model"],
        "record model differs from the frozen semantic spec",
    )
    _number(record.get("semantic_service_time_ms"), "semantic service time")
    runtime_epoch = _text(
        record.get("semantic_runtime_epoch"),
        "semantic runtime_epoch",
    )
    _require(
        re.fullmatch(r"[0-9a-f]{32}", runtime_epoch) is not None,
        "semantic runtime_epoch is invalid",
    )
    _require(
        record.get("semantic_health_verified") is True,
        "semantic health was not verified",
    )
    _require(
        type(record.get("idempotent_replay")) is bool,
        "idempotent replay flag changed",
    )
    for field in (
        "semantic_frame_payload_integrity_verified",
        "data_agent_artifact_delivery_verified",
        "container_semantic_response_consistency_verified",
        "scoring_verified",
        "llm_called",
        "semantic_telemetry_complete",
    ):
        _require(record.get(field) is True, f"record {field} is not true")
    _require(
        record.get("container_runtime_code_provenance_verified") is False,
        "record overclaims container runtime code provenance",
    )
    _require(
        record.get("container_data_plane_artifact_delivery_verified") is False,
        "container overclaimed Data Agent provenance",
    )
    _require(
        record.get("execution_and_semantic_route_unified") is False,
        "route unification was overclaimed",
    )
    _require(record.get("credentials_recorded") is False, "record contains credentials")
    _require(
        record.get("eligible_for_scientific_claims") is False,
        "record overclaims scientific eligibility",
    )
    _require(
        manifest.get("semantic_run_id") == record.get("semantic_run_id"),
        "manifest run ID changed",
    )
    _require(
        manifest.get("matrix_id") == record.get("matrix_id"),
        "manifest matrix ID changed",
    )
    _require(
        manifest.get("matrix_plan_sha256")
        == record.get("matrix_plan_sha256"),
        "manifest matrix digest changed",
    )
    _require(
        manifest.get("matrix_plan_file_sha256")
        == _sha256_path(
            matrix_root / "flowmesh-container-matrix-plan.json"
        ),
        "matrix plan file binding changed",
    )
    _require(
        manifest.get("trial_key") == record.get("trial_key"),
        "manifest trial binding changed",
    )
    _require(
        manifest.get("event_index") == event_index,
        "manifest event binding changed",
    )
    _require(
        manifest.get("endpoint_registry_id") == endpoint_registry.registry_id,
        "manifest registry ID changed",
    )
    _require(
        manifest.get("endpoint_registry_sha256")
        == endpoint_registry.source_sha256,
        "manifest registry digest changed",
    )
    _require(
        manifest.get("semantic_spec_sha256") == spec.source_sha256,
        "manifest semantic spec digest changed",
    )
    _require(manifest.get("record_count") == 1, "manifest record count changed")
    _require(
        manifest.get("record_sha256") == _sha256_path(root / RECORD_NAME),
        "manifest record digest changed",
    )
    _require(
        manifest.get("artifact_size_bytes") == artifact_size,
        "manifest artifact size changed",
    )
    _require(
        manifest.get("artifact_sha256") == artifact_sha256,
        "manifest artifact digest changed",
    )
    _require(manifest.get("frame_count") == frame_count, "manifest frame count changed")
    _require(
        manifest.get("task_success_count") == int(record["task_success"]),
        "manifest task score changed",
    )
    _require(
        manifest.get("expected_model") == spec.document["expected_model"],
        "manifest expected model differs from spec",
    )
    _require(manifest.get("models") == [record["model"]], "manifest model set changed")
    _require(
        manifest.get("semantic_runtime_epoch") == runtime_epoch,
        "manifest runtime epoch changed",
    )
    _require(
        manifest.get("semantic_health_verified") is True,
        "manifest semantic health was not verified",
    )
    for field in (
        "data_agent_artifact_delivery_verified",
        "container_semantic_response_consistency_verified",
        "scoring_verified",
        "llm_called",
    ):
        _require(manifest.get(field) is True, f"manifest {field} is not true")
    _require(
        manifest.get("container_runtime_code_provenance_verified") is False,
        "manifest overclaims container runtime code provenance",
    )
    _require(
        manifest.get("execution_and_semantic_route_unified") is False,
        "manifest overclaims route unification",
    )
    _require(
        manifest.get("credentials_recorded") is False,
        "manifest contains credentials",
    )
    _require(
        manifest.get("eligible_for_scientific_claims") is False,
        "manifest overclaims scientific eligibility",
    )
    limitations = manifest.get("limitations")
    _require(
        isinstance(limitations, list)
        and len(limitations) >= 3
        and all(isinstance(item, str) and item for item in limitations),
        "semantic limitations are incomplete",
    )
    return {
        "status": "VERIFIED",
        "semantic_run_id": record["semantic_run_id"],
        "matrix_id": record["matrix_id"],
        "matrix_plan_sha256": record["matrix_plan_sha256"],
        "trial_key": record["trial_key"],
        "event_index": event_index,
        "matrix_object_id": record["matrix_object_id"],
        "artifact_object_id": record["artifact_object_id"],
        "representation_id": record["representation_id"],
        "frame_count": frame_count,
        "task_success": record["task_success"],
        "model": record["model"],
        "data_agent_artifact_delivery_verified": True,
        "container_semantic_response_consistency_verified": True,
        "container_runtime_code_provenance_verified": False,
        "execution_and_semantic_route_unified": False,
        "llm_called": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION_V2",
    "CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION_V2",
    "ContainerSemanticVisionAdapter",
    "DATA_AGENT_SEMANTIC_EXECUTOR_NODE_ID",
    "DATA_AGENT_SEMANTIC_MANIFEST_REQUIRED_KEYS",
    "DATA_AGENT_SEMANTIC_MANIFEST_SCHEMA_VERSION",
    "DATA_AGENT_SEMANTIC_RECORD_REQUIRED_KEYS",
    "DATA_AGENT_SEMANTIC_RECORD_SCHEMA_VERSION",
    "DATA_AGENT_SEMANTIC_SPEC_REQUIRED_KEYS",
    "DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION",
    "DataAgentFrameBundleSemanticSpec",
    "DataAgentSemanticVerticalError",
    "HttpContainerSemanticVisionAdapter",
    "execute_data_agent_frame_bundle_semantic_trial",
    "load_data_agent_frame_bundle_semantic_spec",
    "verify_data_agent_frame_bundle_semantic_trial",
]
