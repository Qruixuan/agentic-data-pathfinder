"""One-call runtime for a real-object Pathfinder simulator trial.

The runtime is intended to sit behind the N7 container API.  One invocation
binds and performs the whole data path: N7 requests one exact N4 Data Agent
artifact, validates the canonical frame bundle, sends its ordered JPEG frames
to N6's semantic-v2 endpoint, and applies the frozen scoring rule.  The
returned document is evidence, not a transport envelope: it never contains
endpoint URLs, bearer tokens, prompts, questions, option text, or image bytes.

Network configuration is constructor state rather than request content.  This
is important for FlowMesh: a frozen task names a route, while the deployment
maps that route to current service endpoints without putting capabilities or
credentials in a workflow document.
"""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import math
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from ..data_agent_client import (
    DATA_AGENT_API_VERSION,
    DataAgentClientSettings,
    HttpDataAgentClient,
)
from ..distributed.scoring import (
    AnswerOption,
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    WorkloadScoringContract,
    evaluate_workload_answer,
    load_workload_scoring_contract,
    render_workload_question,
)
from ..frame_bundle import REPRESENTATION_ID
from ..frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    FrameBundleLimits,
)
from ..frame_bundle_transfer import (
    FrameBundleTransfer,
    build_frame_bundle_access_request,
    fetch_validated_frame_bundle,
    transfer_audit_of,
)
from .container_node import (
    CONTAINER_NODE_API_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION,
    ContainerNodeRuntime,
    semantic_frame_sequence_sha256,
)
from .hidden_oracle import (
    N1_NODE_ID,
    N1_SCORE_RESULT_SCHEMA_VERSION,
    N1OracleHTTPClient,
    assert_hidden_oracle_fields_absent,
    build_n1_evaluation_unit_id,
    build_n1_public_task_binding,
    build_n1_score_request,
)


FULL_FLOW_REQUEST_SCHEMA_VERSION = (
    "pathfinder.simulator-full-flow-trial-request/v1alpha1"
)
FULL_FLOW_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.simulator-full-flow-trial-evidence/v1alpha1"
)
FULL_FLOW_REQUEST_V2_SCHEMA_VERSION = (
    "pathfinder.simulator-full-flow-trial-request/v2alpha1"
)
FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION = (
    "pathfinder.simulator-full-flow-trial-evidence/v2alpha1"
)

FULL_FLOW_SOURCE_NODE_ID = "N4"
FULL_FLOW_EXECUTOR_NODE_ID = "N7"
FULL_FLOW_INFERENCE_NODE_ID = "N6"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_RUNTIME_EPOCH = re.compile(r"[0-9a-f]{32}\Z")
_SIMULATOR_PRIVATE_HOST = re.compile(
    r"pathfinder-sim-[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z"
)
_MAX_IDENTIFIER_BYTES = 512
_MAX_QUESTION_BYTES = 64 * 1024
_MAX_OPTION_TEXT_BYTES = 16 * 1024
_MAX_OPTION_ANSWER_BYTES = 64
_UNSAFE_EVIDENCE_VALUE = re.compile(
    r"(?:"
    r"[a-z][a-z0-9+.-]*://|"
    r"\bbearer\s+\S|"
    r"\b(?:api[_ -]?key|password|secret|token)\s*[:=]\s*\S+|"
    r"\bsk-[a-z0-9_-]{16,}\b|"
    r"\bAKIA[0-9A-Z]{16}\b|"
    r"\beyJ[a-z0-9_-]{10,}\.[a-z0-9_-]{10,}\.[a-z0-9_-]{10,}\b|"
    r"\b(?:localhost|(?:\d{1,3}\.){3}\d{1,3}|"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}|"
    r"pathfinder-sim-[a-z0-9-]+):\d{1,5}\b"
    r")",
    re.IGNORECASE,
)

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

_REQUEST_V2_KEYS = frozenset(
    (_REQUEST_KEYS - {"correct_answer_id"})
    | {"oracle_id", "task_binding_sha256"}
)

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

_SCORING_RULES = frozenset({
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
})


class FullFlowRuntimeError(RuntimeError):
    """Raised when a full-flow request cannot produce trusted evidence."""


class FullFlowIdempotencyConflict(FullFlowRuntimeError):
    """Raised when one request ID is reused for different request bytes."""


class SemanticVisionAdapter(Protocol):
    """Minimal N6 semantic-v2 adapter consumed by the N7 runtime."""

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        """Execute one already-bound semantic-v2 request."""

    @property
    def health_verified(self) -> bool:
        """Whether stable pre/post N6 health checks succeeded."""

    @property
    def last_runtime_epoch(self) -> str | None:
        """The N6 runtime epoch stable across this semantic execution."""


class N1OracleClient(Protocol):
    """Deployment-injected N7 client for the hidden N1 scoring boundary."""

    def health(self) -> Mapping[str, Any]:
        """Return the public, label-free N1 identity binding."""

    def score(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        """Score one declared prediction without returning the label."""


@dataclass(frozen=True)
class DataAgentSourceAttestation:
    """Stable, credential-free N4 identity observed around one transfer."""

    access_id: str
    source_node_id: str
    object_catalog_version: str
    representations: tuple[str, ...]
    health_sha256_before: str
    health_sha256_after: str
    stable_health_verified: bool


class DataAgentSourceAttestationProvider(Protocol):
    """Binary Data Agent client that can attest the serving source."""

    def get_source_attestation(
        self,
        access_id: str,
    ) -> DataAgentSourceAttestation:
        """Return the completed pre/post health attestation for an access."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowRuntimeError(message)


def _text(value: Any, name: str, *, max_bytes: int = _MAX_IDENTIFIER_BYTES) -> str:
    _require(isinstance(value, str) and bool(value.strip()), f"{name} is invalid")
    normalized = str(value).strip()
    _require(
        len(normalized.encode("utf-8")) <= max_bytes,
        f"{name} exceeds its byte limit",
    )
    return normalized


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    _require(
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= minimum,
        f"{name} is invalid",
    )
    return int(value)


def _public_text(
    value: Any,
    name: str,
    *,
    max_bytes: int = _MAX_IDENTIFIER_BYTES,
) -> str:
    result = _text(value, name, max_bytes=max_bytes)
    _require(
        _UNSAFE_EVIDENCE_VALUE.search(result) is None,
        f"{name} contains endpoint or authorization material",
    )
    _require(
        all(ord(character) >= 32 for character in result),
        f"{name} contains control characters",
    )
    return result


def _number(value: Any, name: str, *, positive: bool = False) -> float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value)),
        f"{name} is invalid",
    )
    result = float(value)
    _require(result > 0.0 if positive else result >= 0.0, f"{name} is invalid")
    return result


def _digest(value: Any, name: str) -> str:
    normalized = _text(value, name, max_bytes=64)
    _require(_SHA256.fullmatch(normalized) is not None, f"{name} is invalid")
    return normalized


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowRuntimeError("request is not canonical JSON") from exc


def _copy_json(value: Any) -> Any:
    return json.loads(_canonical_bytes(value).decode("utf-8"))


def _strict_json_bytes(raw: bytes, name: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise FullFlowRuntimeError(f"{name} contains a duplicate key")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        del value
        raise FullFlowRuntimeError(f"{name} contains a non-finite number")

    try:
        return json.loads(
            raw,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except FullFlowRuntimeError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowRuntimeError(f"{name} is unreadable") from exc


@dataclass(frozen=True)
class FullFlowRouteConfig:
    """Non-secret deployment binding for the N4 -> N7 -> N6 route."""

    route_id: str
    requested_location: str
    data_agent_plan_id: str
    data_agent_plan_epoch: int = 0
    source_node_id: str = FULL_FLOW_SOURCE_NODE_ID
    executor_node_id: str = FULL_FLOW_EXECUTOR_NODE_ID
    inference_node_id: str = FULL_FLOW_INFERENCE_NODE_ID
    quiescence_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "route_id",
            _public_text(self.route_id, "route_id"),
        )
        object.__setattr__(
            self,
            "requested_location",
            _public_text(self.requested_location, "requested_location"),
        )
        object.__setattr__(
            self,
            "data_agent_plan_id",
            _public_text(self.data_agent_plan_id, "data_agent_plan_id"),
        )
        _integer(self.data_agent_plan_epoch, "data_agent_plan_epoch")
        _require(
            self.source_node_id == FULL_FLOW_SOURCE_NODE_ID,
            "full-flow source_node_id must be N4",
        )
        _require(
            self.executor_node_id == FULL_FLOW_EXECUTOR_NODE_ID,
            "full-flow executor_node_id must be N7",
        )
        _require(
            self.inference_node_id == FULL_FLOW_INFERENCE_NODE_ID,
            "full-flow inference_node_id must be N6",
        )
        _number(
            self.quiescence_timeout_seconds,
            "quiescence_timeout_seconds",
        )

    def public_binding(self) -> dict[str, Any]:
        """Return route semantics only; endpoint configuration is excluded."""
        return {
            "route_id": self.route_id,
            "source_node_id": self.source_node_id,
            "executor_node_id": self.executor_node_id,
            "inference_node_id": self.inference_node_id,
            "requested_location": self.requested_location,
            "data_agent_plan_id": self.data_agent_plan_id,
            "data_agent_plan_epoch": self.data_agent_plan_epoch,
            "quiescence_timeout_seconds": float(
                self.quiescence_timeout_seconds
            ),
        }

    @property
    def sha256(self) -> str:
        return _sha256_bytes(_canonical_bytes(self.public_binding()))


def full_flow_binding_sha256(
    request: Mapping[str, Any],
    route_config: FullFlowRouteConfig,
) -> str:
    """Hash every frozen request field plus the endpoint-free route binding."""
    copied = dict(request)
    copied.pop("frozen_binding_sha256", None)
    return _sha256_bytes(
        _canonical_bytes({
            "request": copied,
            "route": route_config.public_binding(),
        })
    )


def full_flow_n1_score_request_id(
    request: Mapping[str, Any],
    *,
    final_answer: str,
) -> str:
    """Bind one N1 score call to the frozen run/trial evaluation unit."""

    _require(
        request.get("schema_version") == FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
        "N1 score identity requires a v2 full-flow request",
    )
    evaluation_unit_id = build_n1_evaluation_unit_id(
        oracle_id=_public_text(request.get("oracle_id"), "oracle_id"),
        run_id=_public_text(request.get("run_id"), "run_id"),
        trial_id=_public_text(request.get("trial_id"), "trial_id"),
    )
    answer = _public_text(
        final_answer,
        "final_answer",
        max_bytes=_MAX_OPTION_ANSWER_BYTES,
    )
    return _sha256_bytes(
        _canonical_bytes(
            {
                "domain": "pathfinder.full-flow-n1-score/v2",
                "evaluation_unit_id": evaluation_unit_id,
                "full_flow_request_id": _public_text(
                    request.get("full_flow_request_id"),
                    "full_flow_request_id",
                ),
                "request_binding_sha256": _digest(
                    request.get("frozen_binding_sha256"),
                    "frozen_binding_sha256",
                ),
                "task_binding_sha256": _digest(
                    request.get("task_binding_sha256"),
                    "task_binding_sha256",
                ),
                "prediction_sha256": _sha256_bytes(answer.encode("utf-8")),
            }
        )
    )


def build_full_flow_trial_request(
    *,
    route_config: FullFlowRouteConfig,
    full_flow_request_id: str,
    run_id: str,
    trial_id: str,
    trial_key: str,
    workload_id: str,
    object_id: str,
    artifact_sha256: str,
    artifact_size_bytes: int,
    object_catalog_version: str,
    expected_model: str,
    question: str,
    answer_options: list[Mapping[str, Any]],
    correct_answer_id: str,
    success_scoring_rule: str,
    task_class_id: str = "video_qa",
    event_index: int = 0,
) -> dict[str, Any]:
    """Build a request whose immutable scientific bindings are self-hashed."""
    request: dict[str, Any] = {
        "schema_version": FULL_FLOW_REQUEST_SCHEMA_VERSION,
        "full_flow_request_id": full_flow_request_id,
        "run_id": run_id,
        "trial_id": trial_id,
        "trial_key": trial_key,
        "workload_id": workload_id,
        "task_class_id": task_class_id,
        "route_id": route_config.route_id,
        "event_index": event_index,
        "object_id": object_id,
        "representation_id": REPRESENTATION_ID,
        "artifact_sha256": artifact_sha256,
        "artifact_size_bytes": artifact_size_bytes,
        "object_catalog_version": object_catalog_version,
        "expected_model": expected_model,
        "question": question,
        "success_scoring_rule": success_scoring_rule,
        "answer_options": [dict(option) for option in answer_options],
        "correct_answer_id": correct_answer_id,
        "credentials_recorded": False,
    }
    request["frozen_binding_sha256"] = full_flow_binding_sha256(
        request,
        route_config,
    )
    return request


def build_full_flow_trial_request_v2(
    *,
    route_config: FullFlowRouteConfig,
    full_flow_request_id: str,
    run_id: str,
    trial_id: str,
    trial_key: str,
    workload_id: str,
    object_id: str,
    artifact_sha256: str,
    artifact_size_bytes: int,
    object_catalog_version: str,
    expected_model: str,
    question: str,
    answer_options: list[Mapping[str, Any]],
    success_scoring_rule: str,
    oracle_id: str,
    task_binding_sha256: str,
    task_class_id: str = "video_qa",
    event_index: int = 0,
) -> dict[str, Any]:
    """Build a label-free v2 request safe to freeze in a FlowMesh plan."""

    full_flow_request_id = _public_text(
        full_flow_request_id,
        "full_flow_request_id",
    )
    run_id = _public_text(run_id, "run_id")
    trial_id = _public_text(trial_id, "trial_id")
    trial_key = _public_text(trial_key, "trial_key")
    workload_id = _public_text(workload_id, "workload_id")
    object_id = _public_text(object_id, "object_id")
    task_class_id = _public_text(task_class_id, "task_class_id")
    object_catalog_version = _public_text(
        object_catalog_version,
        "object_catalog_version",
    )
    expected_model = _public_text(expected_model, "expected_model")
    oracle_id = _public_text(oracle_id, "oracle_id")
    artifact_sha256 = _digest(artifact_sha256, "artifact_sha256")
    artifact_size_bytes = _integer(
        artifact_size_bytes,
        "artifact_size_bytes",
        minimum=1,
    )
    event_index = _integer(event_index, "event_index")
    try:
        public_task = build_n1_public_task_binding(
            workload_id=workload_id,
            object_id=object_id,
            task_class_id=task_class_id,
            question=question,
            answer_options=answer_options,
            success_scoring_rule=success_scoring_rule,
        )
    except Exception as exc:
        raise FullFlowRuntimeError("public N1 task binding is invalid") from exc
    _require(
        public_task["task_binding_sha256"] == task_binding_sha256,
        "task_binding_sha256 differs from the public N1 task",
    )
    request: dict[str, Any] = {
        "schema_version": FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
        "full_flow_request_id": full_flow_request_id,
        "run_id": run_id,
        "trial_id": trial_id,
        "trial_key": trial_key,
        "workload_id": workload_id,
        "task_class_id": task_class_id,
        "route_id": route_config.route_id,
        "event_index": event_index,
        "object_id": object_id,
        "representation_id": REPRESENTATION_ID,
        "artifact_sha256": artifact_sha256,
        "artifact_size_bytes": artifact_size_bytes,
        "object_catalog_version": object_catalog_version,
        "expected_model": expected_model,
        "question": question,
        "success_scoring_rule": success_scoring_rule,
        "answer_options": [dict(option) for option in answer_options],
        "oracle_id": oracle_id,
        "task_binding_sha256": task_binding_sha256,
        "credentials_recorded": False,
    }
    assert_hidden_oracle_fields_absent(request)
    request["frozen_binding_sha256"] = full_flow_binding_sha256(
        request,
        route_config,
    )
    return request


def validate_full_flow_trial_request_v2(
    request: Mapping[str, Any],
    *,
    route_config: FullFlowRouteConfig,
) -> dict[str, Any]:
    """Verify a frozen public v2 request without requiring runtime clients."""

    _require(isinstance(request, Mapping), "full-flow request must be an object")
    copied = _copy_json(dict(request))
    _require(
        set(copied) == _REQUEST_V2_KEYS,
        "full-flow v2 request field set changed",
    )
    _require(
        copied.get("schema_version") == FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
        "full-flow v2 request schema changed",
    )
    assert_hidden_oracle_fields_absent(copied)
    _digest(copied.get("artifact_sha256"), "artifact_sha256")
    _digest(copied.get("task_binding_sha256"), "task_binding_sha256")
    _digest(copied.get("frozen_binding_sha256"), "frozen_binding_sha256")
    _integer(copied.get("artifact_size_bytes"), "artifact_size_bytes", minimum=1)
    _integer(copied.get("event_index"), "event_index")
    _require(
        copied.get("credentials_recorded") is False,
        "full-flow request records credentials",
    )
    try:
        expected = build_full_flow_trial_request_v2(
            route_config=route_config,
            full_flow_request_id=copied["full_flow_request_id"],
            run_id=copied["run_id"],
            trial_id=copied["trial_id"],
            trial_key=copied["trial_key"],
            workload_id=copied["workload_id"],
            task_class_id=copied["task_class_id"],
            object_id=copied["object_id"],
            artifact_sha256=copied["artifact_sha256"],
            artifact_size_bytes=copied["artifact_size_bytes"],
            object_catalog_version=copied["object_catalog_version"],
            expected_model=copied["expected_model"],
            question=copied["question"],
            answer_options=copied["answer_options"],
            success_scoring_rule=copied["success_scoring_rule"],
            oracle_id=copied["oracle_id"],
            task_binding_sha256=copied["task_binding_sha256"],
            event_index=copied["event_index"],
        )
    except (KeyError, TypeError) as exc:
        raise FullFlowRuntimeError("full-flow v2 request is incomplete") from exc
    _require(copied == expected, "full-flow v2 frozen binding changed")
    return copied


@dataclass
class _RequestState:
    request_sha256: str
    running: bool = False
    access_consumed: bool = False
    evidence: dict[str, Any] | None = None


class FullFlowTrialRuntime:
    """Thread-safe N7 executor for one-call real-object full-flow trials."""

    def __init__(
        self,
        *,
        route_config: FullFlowRouteConfig,
        data_agent_client: Any,
        semantic_adapter: SemanticVisionAdapter,
        oracle_client: N1OracleClient | None = None,
        limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
        forbidden_evidence_values: tuple[str, ...] = (),
    ) -> None:
        _require(
            hasattr(data_agent_client, "fetch_binary_artifact")
            and hasattr(data_agent_client, "get_access_telemetry")
            and hasattr(data_agent_client, "get_source_attestation"),
            "data_agent_client lacks the binary transfer and source "
            "attestation contract",
        )
        _require(
            hasattr(semantic_adapter, "execute"),
            "semantic_adapter lacks execute()",
        )
        self._route = route_config
        self._data_agent = data_agent_client
        self._semantic = semantic_adapter
        if oracle_client is not None:
            _require(
                hasattr(oracle_client, "health")
                and hasattr(oracle_client, "score"),
                "oracle_client lacks health() or score()",
            )
        self._oracle = oracle_client
        self._limits = limits
        self._forbidden_evidence_values = tuple(
            _text(value, "forbidden_evidence_value", max_bytes=8192)
            for value in forbidden_evidence_values
        )
        self._condition = threading.Condition()
        # Adapter health/epoch metadata is deliberately part of the adapter
        # protocol rather than its return body.  Keep execute + metadata read
        # atomic across different full-flow IDs so those shared properties
        # cannot be overwritten by a concurrent N6 call.
        self._semantic_lock = threading.Lock()
        self._states: dict[str, _RequestState] = {}

    @property
    def route_config_sha256(self) -> str:
        return self._route.sha256

    def execute(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Execute once, wait for an identical in-flight call, or replay."""
        validated, contract, rendered_question = self._validated_request(request)
        request_bytes = _canonical_bytes(validated)
        request_sha256 = _sha256_bytes(request_bytes)
        request_id = validated["full_flow_request_id"]

        with self._condition:
            state = self._states.get(request_id)
            if state is None:
                state = _RequestState(request_sha256=request_sha256)
                self._states[request_id] = state
            elif state.request_sha256 != request_sha256:
                raise FullFlowIdempotencyConflict(
                    "full_flow_request_id was reused for different request bytes"
                )

            while state.running:
                self._condition.wait()
            if state.evidence is not None:
                replay = _copy_json(state.evidence)
                replay["idempotent_replay"] = True
                return replay
            if state.access_consumed:
                raise FullFlowRuntimeError(
                    "a previous attempt consumed the Data Agent access "
                    "without committing complete evidence; same-ID retry "
                    "is refused to prevent an unaccounted second download"
                )
            state.running = True

        try:
            evidence = self._execute_once(
                validated,
                contract=contract,
                rendered_question=rendered_question,
                request_sha256=request_sha256,
                state=state,
            )
        except BaseException:
            with self._condition:
                state.running = False
                self._condition.notify_all()
            raise

        with self._condition:
            state.evidence = _copy_json(evidence)
            state.running = False
            self._condition.notify_all()
        return _copy_json(evidence)

    def _validated_request(
        self,
        request: Mapping[str, Any],
    ) -> tuple[dict[str, Any], Any, str]:
        _require(isinstance(request, Mapping), "full-flow request must be an object")
        copied = _copy_json(dict(request))
        if copied.get("schema_version") == FULL_FLOW_REQUEST_V2_SCHEMA_VERSION:
            return self._validated_request_v2(copied)
        _require(set(copied) == _REQUEST_KEYS, "full-flow request field set changed")
        _require(
            copied.get("schema_version") == FULL_FLOW_REQUEST_SCHEMA_VERSION,
            "full-flow request schema changed",
        )
        for field_name in (
            "full_flow_request_id",
            "run_id",
            "trial_id",
            "trial_key",
            "workload_id",
            "object_id",
            "object_catalog_version",
            "expected_model",
            "correct_answer_id",
        ):
            copied[field_name] = _public_text(
                copied.get(field_name), field_name
            )
        copied["task_class_id"] = _public_text(
            copied.get("task_class_id"), "task_class_id"
        )
        _require(
            copied["task_class_id"] == "video_qa",
            "full-flow runtime supports only video_qa",
        )
        _require(
            copied.get("route_id") == self._route.route_id,
            "full-flow request route_id differs from runtime configuration",
        )
        _require(
            copied.get("representation_id") == REPRESENTATION_ID,
            "full-flow request requires sampled_frame_bundle",
        )
        copied["event_index"] = _integer(
            copied.get("event_index"), "event_index"
        )
        copied["artifact_size_bytes"] = _integer(
            copied.get("artifact_size_bytes"),
            "artifact_size_bytes",
            minimum=1,
        )
        _require(
            copied["artifact_size_bytes"] <= self._limits.max_artifact_bytes,
            "artifact_size_bytes exceeds the frame-bundle limit",
        )
        copied["artifact_sha256"] = _digest(
            copied.get("artifact_sha256"), "artifact_sha256"
        )
        question = _text(
            copied.get("question"),
            "question",
            max_bytes=_MAX_QUESTION_BYTES,
        )
        copied["question"] = question
        rule = _text(copied.get("success_scoring_rule"), "success_scoring_rule")
        _require(rule in _SCORING_RULES, "full-flow scoring rule is unsupported")
        raw_options = copied.get("answer_options")
        _require(
            isinstance(raw_options, list) and 2 <= len(raw_options) <= 32,
            "answer_options must contain between 2 and 32 options",
        )
        for index, option in enumerate(raw_options):
            _require(
                isinstance(option, dict)
                and set(option) == {"option_id", "text"},
                f"answer_options[{index}] field set changed",
            )
            option["option_id"] = _text(
                option.get("option_id"),
                f"answer_options[{index}].option_id",
                max_bytes=16,
            )
            option["text"] = _text(
                option.get("text"),
                f"answer_options[{index}].text",
                max_bytes=_MAX_OPTION_TEXT_BYTES,
            )
        _require(
            copied.get("credentials_recorded") is False,
            "full-flow request records credentials",
        )
        frozen_digest = _digest(
            copied.get("frozen_binding_sha256"),
            "frozen_binding_sha256",
        )
        _require(
            frozen_digest == full_flow_binding_sha256(copied, self._route),
            "full-flow frozen binding digest does not match request and route",
        )
        try:
            contract = load_workload_scoring_contract(
                {
                    "object_id": copied["object_id"],
                    "question": copied["question"],
                    "answer_options": copied["answer_options"],
                    "correct_answer_id": copied["correct_answer_id"],
                },
                rule,
                name="full-flow request",
            )
            rendered_question = render_workload_question(copied, contract)
        except Exception as exc:
            raise FullFlowRuntimeError(
                "full-flow scoring contract is invalid"
            ) from exc
        return copied, contract, rendered_question

    def _validated_request_v2(
        self,
        copied: dict[str, Any],
    ) -> tuple[dict[str, Any], WorkloadScoringContract, str]:
        """Validate public v2 inputs without manufacturing a label."""

        _require(
            set(copied) == _REQUEST_V2_KEYS,
            "full-flow v2 request field set changed",
        )
        assert_hidden_oracle_fields_absent(copied)
        for field_name in (
            "full_flow_request_id",
            "run_id",
            "trial_id",
            "trial_key",
            "workload_id",
            "object_id",
            "object_catalog_version",
            "expected_model",
            "oracle_id",
        ):
            copied[field_name] = _public_text(
                copied.get(field_name), field_name
            )
        copied["task_class_id"] = _public_text(
            copied.get("task_class_id"), "task_class_id"
        )
        _require(
            copied["task_class_id"] == "video_qa",
            "full-flow runtime supports only video_qa",
        )
        _require(
            copied.get("route_id") == self._route.route_id,
            "full-flow request route_id differs from runtime configuration",
        )
        _require(
            copied.get("representation_id") == REPRESENTATION_ID,
            "full-flow request requires sampled_frame_bundle",
        )
        copied["event_index"] = _integer(
            copied.get("event_index"), "event_index"
        )
        copied["artifact_size_bytes"] = _integer(
            copied.get("artifact_size_bytes"),
            "artifact_size_bytes",
            minimum=1,
        )
        _require(
            copied["artifact_size_bytes"] <= self._limits.max_artifact_bytes,
            "artifact_size_bytes exceeds the frame-bundle limit",
        )
        copied["artifact_sha256"] = _digest(
            copied.get("artifact_sha256"), "artifact_sha256"
        )
        copied["task_binding_sha256"] = _digest(
            copied.get("task_binding_sha256"), "task_binding_sha256"
        )
        copied["question"] = _text(
            copied.get("question"),
            "question",
            max_bytes=_MAX_QUESTION_BYTES,
        )
        rule = _text(copied.get("success_scoring_rule"), "success_scoring_rule")
        _require(rule in _SCORING_RULES, "full-flow scoring rule is unsupported")
        raw_options = copied.get("answer_options")
        _require(
            isinstance(raw_options, list) and 2 <= len(raw_options) <= 32,
            "answer_options must contain between 2 and 32 options",
        )
        option_rows: list[dict[str, str]] = []
        for index, option in enumerate(raw_options):
            _require(
                isinstance(option, dict)
                and set(option) == {"option_id", "text"},
                f"answer_options[{index}] field set changed",
            )
            option_id = _text(
                option.get("option_id"),
                f"answer_options[{index}].option_id",
                max_bytes=16,
            )
            option_text = _text(
                option.get("text"),
                f"answer_options[{index}].text",
                max_bytes=_MAX_OPTION_TEXT_BYTES,
            )
            option_rows.append({"option_id": option_id, "text": option_text})
        copied["answer_options"] = option_rows
        _require(
            copied.get("credentials_recorded") is False,
            "full-flow request records credentials",
        )
        try:
            public_task = build_n1_public_task_binding(
                workload_id=copied["workload_id"],
                object_id=copied["object_id"],
                task_class_id=copied["task_class_id"],
                question=copied["question"],
                answer_options=copied["answer_options"],
                success_scoring_rule=rule,
            )
        except Exception as exc:
            raise FullFlowRuntimeError("public N1 task binding is invalid") from exc
        _require(
            copied["task_binding_sha256"]
            == public_task["task_binding_sha256"],
            "task_binding_sha256 differs from public task contents",
        )
        frozen_digest = _digest(
            copied.get("frozen_binding_sha256"),
            "frozen_binding_sha256",
        )
        _require(
            frozen_digest == full_flow_binding_sha256(copied, self._route),
            "full-flow frozen binding digest does not match request and route",
        )
        contract = WorkloadScoringContract(
            rule=rule,
            answer_options=tuple(
                AnswerOption(
                    option_id=option["option_id"],
                    text=option["text"],
                )
                for option in option_rows
            ),
            correct_answer_id=None,
        )
        try:
            rendered_question = render_workload_question(copied, contract)
        except Exception as exc:
            raise FullFlowRuntimeError(
                "full-flow public scoring contract is invalid"
            ) from exc
        _require(self._oracle is not None, "v2 full-flow request requires N1 oracle")
        return copied, contract, rendered_question

    def _execute_once(
        self,
        request: dict[str, Any],
        *,
        contract: Any,
        rendered_question: str,
        request_sha256: str,
        state: _RequestState,
    ) -> dict[str, Any]:
        access_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            (
                "pathfinder-full-flow-access:"
                f"{request['full_flow_request_id']}:"
                f"{request['trial_key']}:{request['object_id']}:"
                f"{request['representation_id']}:{request['event_index']}"
            ),
        ))
        access_request = build_frame_bundle_access_request(
            object_id=request["object_id"],
            plan_id=self._route.data_agent_plan_id,
            requested_location=self._route.requested_location,
            session_id=request["run_id"],
            trial_id=request["trial_id"],
            task_class_id=request["task_class_id"],
            representation_id=request["representation_id"],
            access_id=access_id,
            plan_epoch=self._route.data_agent_plan_epoch,
            event_index=request["event_index"],
        )
        try:
            transfer = fetch_validated_frame_bundle(
                self._data_agent,
                access_request,
                expected_object_id=request["object_id"],
                expected_artifact_sha256=request["artifact_sha256"],
                expected_artifact_size_bytes=request["artifact_size_bytes"],
                expected_object_catalog_version=request[
                    "object_catalog_version"
                ],
                limits=self._limits,
                quiescence_timeout_seconds=(
                    self._route.quiescence_timeout_seconds
                ),
            )
        except Exception as exc:
            audit = transfer_audit_of(exc)
            if (
                audit is not None
                and audit.execution_phase.artifact_download_started
            ):
                self._mark_access_consumed(state)
            raise FullFlowRuntimeError(
                "Data Agent frame-bundle delivery failed"
            ) from exc
        self._mark_access_consumed(state)
        source_attestation = self._validate_transfer(
            transfer,
            request,
            access_id,
        )

        semantic_request, frame_metadata = self._semantic_request(
            request,
            rendered_question=rendered_question,
            transfer=transfer,
        )
        with self._semantic_lock:
            try:
                raw_result = self._semantic.execute(semantic_request)
            except Exception as exc:
                raise FullFlowRuntimeError(
                    "N6 semantic execution failed"
                ) from exc
            result = self._validate_semantic_result(
                raw_result,
                request=semantic_request,
                expected_model=request["expected_model"],
            )
            _require(
                self._semantic.health_verified is True,
                "N6 semantic adapter did not verify stable health",
            )
            runtime_epoch = _text(
                self._semantic.last_runtime_epoch,
                "semantic runtime_epoch",
                max_bytes=32,
            )
            _require(
                _RUNTIME_EPOCH.fullmatch(runtime_epoch) is not None,
                "semantic runtime_epoch is invalid",
            )

        final_answer = self._declared_option_answer(
            result["final_answer"], contract
        )
        if request["schema_version"] == FULL_FLOW_REQUEST_V2_SCHEMA_VERSION:
            oracle_result, oracle_health_sha256 = self._score_with_n1_oracle(
                request,
                final_answer=final_answer,
            )
            task_success = oracle_result["correct"]
            scoring_evidence = {
                "success_scoring_rule": request["success_scoring_rule"],
                "answer_option_ids": [
                    option.option_id for option in contract.answer_options
                ],
                "answer_options_sha256": _sha256_bytes(
                    _canonical_bytes(request["answer_options"])
                ),
                "task_binding_sha256": request["task_binding_sha256"],
                "final_answer": final_answer,
                "final_answer_sha256": _sha256_bytes(
                    final_answer.encode("utf-8")
                ),
                "task_success": task_success,
                "score": oracle_result["score"],
                "oracle_health_sha256": oracle_health_sha256,
                "oracle_result": oracle_result,
            }
            evidence_schema = FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION
        else:
            task_success = evaluate_workload_answer(final_answer, contract)
            _require(type(task_success) is bool, "full-flow score is not boolean")
            evidence_schema = FULL_FLOW_EVIDENCE_SCHEMA_VERSION
            scoring_evidence = {
                "success_scoring_rule": contract.rule,
                "answer_option_ids": [
                    option.option_id for option in contract.answer_options
                ],
                "answer_options_sha256": _sha256_bytes(
                    _canonical_bytes(request["answer_options"])
                ),
                "correct_answer_id": contract.correct_answer_id,
                "final_answer": final_answer,
                "final_answer_sha256": _sha256_bytes(
                    final_answer.encode("utf-8")
                ),
                "task_success": task_success,
            }

        latency_ms = transfer.latency_ms()
        for name, value in latency_ms.items():
            if value is not None:
                _number(value, f"Data Agent {name} latency")

        evidence = {
            "schema_version": evidence_schema,
            "status": "COMPLETE",
            "full_flow_request_id": request["full_flow_request_id"],
            "request_sha256": request_sha256,
            "frozen_binding_sha256": request["frozen_binding_sha256"],
            "route_config_sha256": self._route.sha256,
            "idempotent_replay": False,
            "run_id": request["run_id"],
            "trial_id": request["trial_id"],
            "trial_key": request["trial_key"],
            "workload_id": request["workload_id"],
            "task_class_id": request["task_class_id"],
            "object_id": request["object_id"],
            "representation_id": request["representation_id"],
            "route": {
                "route_id": self._route.route_id,
                "source_node_id": self._route.source_node_id,
                "executor_node_id": self._route.executor_node_id,
                "inference_node_id": self._route.inference_node_id,
                "requested_location": self._route.requested_location,
            },
            "data_agent": {
                "access_id": access_id,
                "source_node_id": source_attestation.source_node_id,
                "source_identity_basis": (
                    "data-agent-health-before-and-after"
                ),
                "source_identity_verified": (
                    source_attestation.stable_health_verified
                ),
                "health_sha256_before": (
                    source_attestation.health_sha256_before
                ),
                "health_sha256_after": (
                    source_attestation.health_sha256_after
                ),
                "plan_id": self._route.data_agent_plan_id,
                "plan_epoch": self._route.data_agent_plan_epoch,
                "object_catalog_version": request[
                    "object_catalog_version"
                ],
                "artifact_media_type": transfer.artifact.media_type,
                "artifact_size_bytes": transfer.artifact.size_bytes,
                "artifact_sha256": transfer.artifact.sha256,
                "manifest_sha256": transfer.bundle.manifest_sha256,
                "frame_count": transfer.bundle.frame_count,
                "total_jpeg_bytes": transfer.bundle.total_jpeg_bytes,
                "delivery": transfer.delivery.to_dict(),
                "latency_ms": latency_ms,
            },
            "semantic": {
                "semantic_request_id": result["semantic_request_id"],
                "request_sha256": result["request_sha256"],
                "question_sha256": _sha256_bytes(
                    rendered_question.encode("utf-8")
                ),
                "prompt_sha256": result["prompt_sha256"],
                "frame_sequence_sha256": result[
                    "frame_sequence_sha256"
                ],
                "frame_metadata": frame_metadata,
                "representation_delivery_bytes": result[
                    "representation_delivery_bytes"
                ],
                "model": result["model"],
                "runtime_epoch": runtime_epoch,
                "service_time_ms": result["service_time_ms"],
                "adapter_idempotent_replay": result[
                    "idempotent_replay"
                ],
            },
            "scoring": scoring_evidence,
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
        if request["schema_version"] == FULL_FLOW_REQUEST_V2_SCHEMA_VERSION:
            assert_hidden_oracle_fields_absent(evidence)
        self._assert_safe_evidence(evidence)
        return evidence

    def _mark_access_consumed(self, state: _RequestState) -> None:
        """Make retries fail closed once remote transfer work has begun."""
        with self._condition:
            state.access_consumed = True

    def _validate_transfer(
        self,
        transfer: FrameBundleTransfer,
        request: Mapping[str, Any],
        access_id: str,
    ) -> DataAgentSourceAttestation:
        attestation = self._data_agent.get_source_attestation(access_id)
        _require(
            isinstance(attestation, DataAgentSourceAttestation),
            "Data Agent source attestation is invalid",
        )
        _require(
            attestation.access_id == access_id
            and attestation.source_node_id == self._route.source_node_id
            and attestation.object_catalog_version
            == request["object_catalog_version"]
            and request["representation_id"] in attestation.representations
            and attestation.stable_health_verified is True,
            "Data Agent source identity is not stably attested as N4",
        )
        _digest(
            attestation.health_sha256_before,
            "Data Agent pre-transfer health digest",
        )
        _digest(
            attestation.health_sha256_after,
            "Data Agent post-transfer health digest",
        )
        _require(
            attestation.health_sha256_before
            == attestation.health_sha256_after,
            "Data Agent health changed during artifact delivery",
        )
        _require(
            transfer.access_request.access_id == access_id
            and transfer.artifact.access_id == access_id
            and transfer.telemetry.access_id == access_id,
            "Data Agent access identity changed",
        )
        _require(
            transfer.bundle.object_id == request["object_id"]
            and transfer.artifact.object_id == request["object_id"]
            and transfer.telemetry.object_id == request["object_id"],
            "Data Agent object identity changed",
        )
        _require(
            transfer.bundle.representation_id == request["representation_id"]
            and transfer.telemetry.representation_id
            == request["representation_id"],
            "Data Agent representation identity changed",
        )
        _require(
            transfer.artifact.object_catalog_version
            == request["object_catalog_version"]
            and transfer.telemetry.object_catalog_version
            == request["object_catalog_version"],
            "Data Agent catalog binding changed",
        )
        _require(
            transfer.artifact.location == self._route.requested_location,
            "Data Agent source location changed",
        )
        _require(
            transfer.delivery.telemetry_complete
            and transfer.delivery.exactly_one_full_download
            and transfer.delivery.bytes_sent_equals_artifact_size,
            "Data Agent delivery is not exactly one accounted full download",
        )
        _require(
            transfer.artifact.sha256 == request["artifact_sha256"]
            and transfer.bundle.artifact_sha256
            == request["artifact_sha256"],
            "Data Agent artifact digest changed",
        )
        _require(
            transfer.artifact.size_bytes == request["artifact_size_bytes"]
            and transfer.bundle.artifact_size_bytes
            == request["artifact_size_bytes"],
            "Data Agent artifact size changed",
        )
        return attestation

    def _semantic_request(
        self,
        request: Mapping[str, Any],
        *,
        rendered_question: str,
        transfer: FrameBundleTransfer,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        frames: list[dict[str, Any]] = []
        metadata: list[dict[str, Any]] = []
        for expected_index, frame in enumerate(transfer.bundle.vision_frames()):
            _require(
                frame.frame_index == expected_index,
                "validated frame sequence is not zero-based and contiguous",
            )
            digest = _sha256_bytes(frame.jpeg_bytes)
            frame_value = {
                "frame_index": frame.frame_index,
                "timestamp_seconds": frame.timestamp_seconds,
                "width": frame.width,
                "height": frame.height,
                "jpeg_size_bytes": len(frame.jpeg_bytes),
                "jpeg_sha256": digest,
                "jpeg_base64": base64.b64encode(frame.jpeg_bytes).decode(
                    "ascii"
                ),
            }
            frames.append(frame_value)
            metadata.append({
                key: value
                for key, value in frame_value.items()
                if key != "jpeg_base64"
            })
        _require(bool(frames), "validated frame bundle contains no frames")
        sequence_sha256 = _digest(
            semantic_frame_sequence_sha256(frames),
            "frame_sequence_sha256",
        )
        prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
            request["representation_id"],
            len(frames),
            rendered_question,
        )
        prompt_sha256 = _sha256_bytes(prompt.encode("utf-8"))
        semantic_id = _sha256_bytes(_canonical_bytes({
            "full_flow_request_id": request["full_flow_request_id"],
            "trial_key": request["trial_key"],
            "execution_node_id": self._route.inference_node_id,
            "representation_id": request["representation_id"],
            "representation_sha256": request["artifact_sha256"],
            "question_sha256": _sha256_bytes(
                rendered_question.encode("utf-8")
            ),
            "prompt_sha256": prompt_sha256,
            "frame_sequence_sha256": sequence_sha256,
        }))
        return ({
            "schema_version": (
                CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION
            ),
            "semantic_request_id": semantic_id,
            "execution_node_id": self._route.inference_node_id,
            "representation_id": request["representation_id"],
            "representation_sha256": request["artifact_sha256"],
            "question": rendered_question,
            "prompt_sha256": prompt_sha256,
            "frame_sequence_sha256": sequence_sha256,
            "frames": frames,
        }, metadata)

    def _validate_semantic_result(
        self,
        raw: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
        expected_model: str,
    ) -> dict[str, Any]:
        _require(isinstance(raw, Mapping), "N6 semantic result is invalid")
        result = _copy_json(dict(raw))
        _require(
            set(result) == _CONTAINER_RESULT_KEYS,
            "N6 semantic result field set changed",
        )
        _require(
            result.get("schema_version")
            == CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION,
            "N6 semantic result schema changed",
        )
        _require(
            result.get("api_version") == CONTAINER_NODE_API_VERSION,
            "N6 semantic API version changed",
        )
        _require(
            result.get("status") == "completed"
            and result.get("outcome_type") == "completed",
            "N6 semantic call did not complete",
        )
        for name in (
            "semantic_request_id",
            "execution_node_id",
            "prompt_sha256",
            "representation_sha256",
            "frame_sequence_sha256",
        ):
            _require(result.get(name) == request[name], f"N6 changed {name}")
        _require(
            result.get("request_sha256")
            == _sha256_bytes(_canonical_bytes(request)),
            "N6 semantic request digest changed",
        )
        frame_count = _integer(result.get("frame_count"), "N6 frame_count")
        _require(
            frame_count == len(request["frames"]),
            "N6 semantic frame count changed",
        )
        delivered = sum(
            frame["jpeg_size_bytes"] for frame in request["frames"]
        )
        delivery_bytes = _integer(
            result.get("representation_delivery_bytes"),
            "N6 representation_delivery_bytes",
        )
        _require(
            delivery_bytes == delivered,
            "N6 representation byte count changed",
        )
        _require(
            result.get("semantic_input_kind") == "ordered-jpeg-frames",
            "N6 semantic input kind changed",
        )
        _require(
            result.get("data_plane_artifact_delivery_verified") is False,
            "N6 overclaimed direct Data Agent artifact delivery",
        )
        _require(
            result.get("semantic_frame_payload_integrity_verified") is True,
            "N6 did not verify frame payload integrity",
        )
        _require(
            result.get("source_node_id") is None,
            "N6 unexpectedly claims a direct source-node fetch",
        )
        _require(
            result.get("telemetry_complete") is True
            and result.get("llm_called") is True
            and result.get("credentials_recorded") is False,
            "N6 semantic provenance flags are invalid",
        )
        _require(
            type(result.get("idempotent_replay")) is bool,
            "N6 idempotent_replay is invalid",
        )
        started = _integer(
            result.get("started_monotonic_ns"), "started_monotonic_ns"
        )
        finished = _integer(
            result.get("finished_monotonic_ns"), "finished_monotonic_ns"
        )
        _require(finished >= started, "N6 semantic interval is invalid")
        _number(result.get("service_time_ms"), "semantic service_time_ms")
        answer = result.get("final_answer")
        _require(
            isinstance(answer, str) and bool(answer.strip()),
            "N6 final_answer is invalid",
        )
        _require(
            len(answer.encode("utf-8")) <= _MAX_OPTION_ANSWER_BYTES,
            "N6 final_answer exceeds its byte limit",
        )
        _require(
            result.get("final_answer_sha256")
            == _sha256_bytes(answer.encode("utf-8")),
            "N6 final-answer digest changed",
        )
        _require(
            result.get("model") == expected_model,
            "N6 model differs from the frozen binding",
        )
        return result

    @staticmethod
    def _declared_option_answer(answer: Any, contract: Any) -> str:
        value = _text(
            answer,
            "N6 final_answer",
            max_bytes=_MAX_OPTION_ANSWER_BYTES,
        )
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
            "N6 final_answer is not one declared option response",
        )
        return value

    def _score_with_n1_oracle(
        self,
        request: Mapping[str, Any],
        *,
        final_answer: str,
    ) -> tuple[dict[str, Any], str]:
        """Call N1 and bind its public, label-free evidence to this request."""

        _require(self._oracle is not None, "N1 oracle is not configured")
        score_request_id = full_flow_n1_score_request_id(
            request,
            final_answer=final_answer,
        )
        try:
            score_request = build_n1_score_request(
                score_request_id=score_request_id,
                oracle_id=request["oracle_id"],
                run_id=request["run_id"],
                trial_id=request["trial_id"],
                object_id=request["object_id"],
                task_binding_sha256=request["task_binding_sha256"],
                predicted_answer=final_answer,
            )
            before = _copy_json(dict(self._oracle.health()))
            result = _copy_json(dict(self._oracle.score(score_request)))
            after = _copy_json(dict(self._oracle.health()))
        except Exception as exc:
            raise FullFlowRuntimeError("N1 hidden scoring failed") from exc
        assert_hidden_oracle_fields_absent(before)
        assert_hidden_oracle_fields_absent(result)
        assert_hidden_oracle_fields_absent(after)
        _require(before == after, "N1 public identity changed during scoring")
        _require(
            before.get("status") == "ok"
            and before.get("node_id") == N1_NODE_ID
            and before.get("oracle_id") == request["oracle_id"]
            and before.get("hidden_answer_returned") is False
            and before.get("credentials_recorded") is False,
            "N1 health identity is invalid",
        )
        _require(
            result.get("schema_version") == N1_SCORE_RESULT_SCHEMA_VERSION
            and result.get("status") == "SCORED"
            and result.get("score_request_id") == score_request_id
            and result.get("evaluation_unit_id")
            == score_request["evaluation_unit_id"]
            and result.get("oracle_id") == request["oracle_id"]
            and result.get("node_id") == N1_NODE_ID
            and result.get("run_id") == request["run_id"]
            and result.get("trial_id") == request["trial_id"]
            and result.get("object_id") == request["object_id"]
            and result.get("task_binding_sha256")
            == request["task_binding_sha256"]
            and result.get("success_scoring_rule")
            == request["success_scoring_rule"],
            "N1 score identity differs from the v2 task binding",
        )
        _require(
            result.get("request_sha256")
            == _sha256_bytes(_canonical_bytes(score_request)),
            "N1 score request digest changed",
        )
        _require(
            result.get("prediction_sha256")
            == _sha256_bytes(final_answer.encode("utf-8")),
            "N1 prediction digest changed",
        )
        _require(type(result.get("correct")) is bool, "N1 correctness is invalid")
        _require(
            result.get("score") == (1.0 if result["correct"] else 0.0),
            "N1 score differs from correctness",
        )
        _require(
            result.get("public_task_set_sha256")
            == before.get("public_task_set_sha256"),
            "N1 public task set changed during scoring",
        )
        for name in (
            "oracle_instance_hmac_sha256",
            "score_evidence_hmac_sha256",
            "result_content_sha256",
        ):
            _digest(result.get(name), f"N1 {name}")
        _require(
            type(result.get("idempotent_replay")) is bool
            and result.get("hidden_answer_returned") is False
            and result.get("credentials_recorded") is False
            and result.get("eligible_for_scientific_claims") is False,
            "N1 public score provenance flags are invalid",
        )
        return result, _sha256_bytes(_canonical_bytes(before))

    def _assert_safe_evidence(self, evidence: Mapping[str, Any]) -> None:
        raw = _canonical_bytes(evidence)
        _require(b"jpeg_base64" not in raw, "evidence contains frame payloads")
        forbidden_keys = (
            "api_key",
            "authorization",
            "bearer",
            "password",
            "token",
            "url",
            "endpoint",
        )

        def visit(value: Any) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    normalized = str(key).casefold()
                    _require(
                        not any(part in normalized for part in forbidden_keys),
                        "evidence contains transport or credential fields",
                    )
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)
            elif isinstance(value, str):
                _require(
                    _UNSAFE_EVIDENCE_VALUE.search(value) is None,
                    "evidence contains endpoint or authorization material",
                )
                for secret in self._forbidden_evidence_values:
                    _require(
                        secret not in value,
                        "evidence contains configured credential material",
                    )

        visit(evidence)


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
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


def _is_loopback(hostname: str | None) -> bool:
    if hostname is None:
        return False
    if hostname.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _private_hosts(value: tuple[str, ...]) -> tuple[str, ...]:
    hosts = tuple(str(host).casefold() for host in value)
    _require(
        len(hosts) == len(set(hosts))
        and all(_SIMULATOR_PRIVATE_HOST.fullmatch(host) for host in hosts),
        "simulator_private_http_hosts is invalid",
    )
    return hosts


def _validated_base_url(
    value: Any,
    name: str,
    private_hosts: tuple[str, ...],
) -> str:
    endpoint = _text(value, name, max_bytes=2048).rstrip("/")
    parsed = urllib.parse.urlsplit(endpoint)
    _require(
        parsed.scheme in {"http", "https"} and parsed.hostname is not None,
        f"{name} must be an absolute HTTP(S) URL",
    )
    _require(
        parsed.username is None and parsed.password is None,
        f"{name} must not contain credentials",
    )
    try:
        port = parsed.port
    except ValueError as exc:
        raise FullFlowRuntimeError(f"{name} port is invalid") from exc
    _require(
        not parsed.query and not parsed.fragment,
        f"{name} contains unsafe metadata",
    )
    hostname = str(parsed.hostname).casefold()
    _require(
        parsed.scheme == "https"
        or _is_loopback(hostname)
        or hostname in private_hosts,
        f"{name} must use HTTPS, loopback, or an explicitly bound "
        "Pathfinder simulator-private host",
    )
    if hostname.startswith("pathfinder-sim-"):
        _require(
            hostname in private_hosts,
            f"{name} simulator-private host is not explicitly bound",
        )
    _require(port is not None or parsed.scheme == "https", f"{name} needs a port")
    return endpoint


@dataclass(frozen=True)
class FullFlowHttpConfig:
    """Ephemeral endpoint/credential configuration; never put in evidence."""

    data_agent_base_url: str = field(repr=False)
    semantic_base_url: str = field(repr=False)
    data_agent_token: str | None = field(default=None, repr=False, compare=False)
    semantic_bearer_token: str | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    oracle_base_url: str | None = field(default=None, repr=False, compare=False)
    oracle_id: str | None = None
    oracle_public_task_set_sha256: str | None = None
    oracle_token: str | None = field(default=None, repr=False, compare=False)
    simulator_private_http_hosts: tuple[str, ...] = ()
    data_agent_timeout_seconds: float = 30.0
    semantic_timeout_seconds: float = 240.0
    oracle_timeout_seconds: float = 30.0
    max_retries: int = 1
    max_response_bytes: int = 2 * 1024 * 1024
    max_semantic_request_bytes: int = 16 * 1024 * 1024

    def __post_init__(self) -> None:
        hosts = _private_hosts(tuple(self.simulator_private_http_hosts))
        object.__setattr__(self, "simulator_private_http_hosts", hosts)
        object.__setattr__(
            self,
            "data_agent_base_url",
            _validated_base_url(
                self.data_agent_base_url,
                "data_agent_base_url",
                hosts,
            ),
        )
        semantic = _validated_base_url(
            self.semantic_base_url,
            "semantic_base_url",
            hosts,
        )
        parsed = urllib.parse.urlsplit(semantic)
        _require(
            parsed.path in {"", "/"},
            "semantic_base_url must name an origin without a path",
        )
        object.__setattr__(self, "semantic_base_url", semantic)
        if self.data_agent_token is not None:
            token = _text(
                self.data_agent_token,
                "data_agent_token",
                max_bytes=8192,
            )
            _require(
                token == self.data_agent_token,
                "data_agent_token must not have surrounding whitespace",
            )
        if self.semantic_bearer_token is not None:
            semantic_token = _text(
                self.semantic_bearer_token,
                "semantic_bearer_token",
                max_bytes=8192,
            )
            _require(
                semantic_token == self.semantic_bearer_token
                and semantic_token.isascii()
                and all(33 <= ord(character) <= 126 for character in semantic_token),
                "semantic_bearer_token must be printable ASCII without whitespace",
            )
        oracle_values = (
            self.oracle_base_url,
            self.oracle_id,
            self.oracle_public_task_set_sha256,
            self.oracle_token,
        )
        _require(
            all(value is None for value in oracle_values)
            or all(value is not None for value in oracle_values),
            "N1 oracle runtime configuration must be supplied as one set",
        )
        if self.oracle_base_url is not None:
            oracle_base = _validated_base_url(
                self.oracle_base_url,
                "oracle_base_url",
                hosts,
            )
            _require(
                urllib.parse.urlsplit(oracle_base).path in {"", "/"},
                "oracle_base_url must name an origin without a path",
            )
            object.__setattr__(self, "oracle_base_url", oracle_base)
            _public_text(self.oracle_id, "oracle_id")
            _digest(
                self.oracle_public_task_set_sha256,
                "oracle_public_task_set_sha256",
            )
            oracle_token = _text(
                self.oracle_token,
                "oracle_token",
                max_bytes=8192,
            )
            _require(
                oracle_token == self.oracle_token,
                "oracle_token must not have surrounding whitespace",
            )
        _number(
            self.data_agent_timeout_seconds,
            "data_agent_timeout_seconds",
            positive=True,
        )
        _number(
            self.semantic_timeout_seconds,
            "semantic_timeout_seconds",
            positive=True,
        )
        _number(
            self.oracle_timeout_seconds,
            "oracle_timeout_seconds",
            positive=True,
        )
        _integer(self.max_retries, "max_retries")
        _integer(self.max_response_bytes, "max_response_bytes", minimum=1)
        _integer(
            self.max_semantic_request_bytes,
            "max_semantic_request_bytes",
            minimum=1,
        )


class SourceAttestedDataAgentClient:
    """Add stable N4 health attestation to a binary Data Agent client.

    Health is sampled immediately before artifact access and after the
    quiescent telemetry read.  The attestation is keyed by access ID so
    concurrent independent full-flow requests cannot exchange identity
    state.  Endpoint values remain private process state.
    """

    _HEALTH_KEYS = frozenset({
        "status",
        "api_version",
        "node_id",
        "representations",
        "object_catalog_version",
        "object_count",
        "credentials_recorded",
    })

    def __init__(
        self,
        client: Any,
        *,
        health_url: str,
        opener: Any,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> None:
        _require(
            hasattr(client, "fetch_binary_artifact")
            and hasattr(client, "get_access_telemetry"),
            "wrapped Data Agent client lacks binary transfer support",
        )
        self._client = client
        self._health_url = health_url
        self._opener = opener
        self._timeout = float(timeout_seconds)
        self._max_response = max_response_bytes
        self._lock = threading.Lock()
        self._before: dict[str, dict[str, Any]] = {}
        self._attestations: dict[str, DataAgentSourceAttestation] = {}

    @property
    def wrapped_client(self) -> Any:
        """The transport client, exposed for type inspection only."""
        return self._client

    def _health(self, representation_id: str) -> dict[str, Any]:
        request = urllib.request.Request(
            self._health_url,
            headers={
                "Accept": "application/json",
                "User-Agent": "pathfinder-full-flow-runtime/1",
            },
            method="GET",
        )
        try:
            with self._opener(request, timeout=self._timeout) as response:
                _require(
                    response.status == 200,
                    "Data Agent health endpoint is not ready",
                )
                _require(
                    response.headers.get_content_type()
                    == "application/json",
                    "Data Agent health endpoint returned non-JSON content",
                )
                raw = response.read(self._max_response + 1)
                _require(
                    len(raw) <= self._max_response,
                    "Data Agent health response exceeds its byte limit",
                )
        except urllib.error.HTTPError as exc:
            raise FullFlowRuntimeError(
                f"Data Agent health endpoint returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowRuntimeError(
                "Data Agent health endpoint is unreachable"
            ) from exc
        value = _strict_json_bytes(raw, "Data Agent health response")
        _require(isinstance(value, dict), "Data Agent health is not an object")
        _require(
            set(value) == self._HEALTH_KEYS,
            "Data Agent health field set changed",
        )
        _require(value.get("status") == "ok", "Data Agent health is not ok")
        _require(
            value.get("credentials_recorded") is False,
            "Data Agent health records credentials",
        )
        _require(
            value.get("api_version") == DATA_AGENT_API_VERSION,
            "Data Agent health API version changed",
        )
        _require(
            value.get("node_id") == FULL_FLOW_SOURCE_NODE_ID,
            "Data Agent health did not attest source N4",
        )
        catalog = _public_text(
            value.get("object_catalog_version"),
            "Data Agent health object_catalog_version",
        )
        _integer(
            value.get("object_count"),
            "Data Agent health object_count",
            minimum=1,
        )
        representations = value.get("representations")
        _require(
            isinstance(representations, list)
            and bool(representations)
            and all(isinstance(item, str) for item in representations),
            "Data Agent health representations are invalid",
        )
        normalized_representations = tuple(
            _public_text(item, "Data Agent health representation")
            for item in representations
        )
        _require(
            len(normalized_representations)
            == len(set(normalized_representations)),
            "Data Agent health representations contain duplicates",
        )
        _require(
            representation_id in normalized_representations,
            "Data Agent health does not advertise the requested representation",
        )
        return {
            "status": "ok",
            "api_version": DATA_AGENT_API_VERSION,
            "node_id": FULL_FLOW_SOURCE_NODE_ID,
            "representations": list(normalized_representations),
            "object_catalog_version": catalog,
            "object_count": value["object_count"],
        }

    def fetch_binary_artifact(
        self,
        request: Any,
        *,
        allowed_media_types: Any,
        on_phase: Any = None,
    ) -> Any:
        before = self._health(request.representation_id)
        with self._lock:
            self._before[request.access_id] = before
            self._attestations.pop(request.access_id, None)
        return self._client.fetch_binary_artifact(
            request,
            allowed_media_types=allowed_media_types,
            on_phase=on_phase,
        )

    def get_access_telemetry(
        self,
        access_id: str,
        *,
        wait_for_quiescence: bool = False,
        quiescence_timeout_seconds: float = 5.0,
        quiescence_poll_seconds: float = 0.02,
    ) -> Any:
        telemetry = self._client.get_access_telemetry(
            access_id,
            wait_for_quiescence=wait_for_quiescence,
            quiescence_timeout_seconds=quiescence_timeout_seconds,
            quiescence_poll_seconds=quiescence_poll_seconds,
        )
        representation_id = _public_text(
            telemetry.representation_id,
            "Data Agent telemetry representation_id",
        )
        after = self._health(representation_id)
        with self._lock:
            before = self._before.pop(access_id, None)
        _require(before is not None, "Data Agent pre-transfer health is absent")
        before_sha256 = _sha256_bytes(_canonical_bytes(before))
        after_sha256 = _sha256_bytes(_canonical_bytes(after))
        _require(
            before_sha256 == after_sha256,
            "Data Agent health changed during artifact delivery",
        )
        attestation = DataAgentSourceAttestation(
            access_id=access_id,
            source_node_id=FULL_FLOW_SOURCE_NODE_ID,
            object_catalog_version=after["object_catalog_version"],
            representations=tuple(after["representations"]),
            health_sha256_before=before_sha256,
            health_sha256_after=after_sha256,
            stable_health_verified=True,
        )
        with self._lock:
            self._attestations[access_id] = attestation
        return telemetry

    def get_source_attestation(
        self,
        access_id: str,
    ) -> DataAgentSourceAttestation:
        with self._lock:
            attestation = self._attestations.pop(access_id, None)
        _require(
            attestation is not None,
            "Data Agent source attestation was not completed",
        )
        return attestation


class HttpSemanticVisionAdapter:
    """Proxy-free, redirect-free adapter for an exact configured N6 origin."""

    def __init__(self, config: FullFlowHttpConfig) -> None:
        _require(
            config.semantic_bearer_token is not None,
            "semantic_bearer_token is required",
        )
        base = config.semantic_base_url.rstrip("/")
        self._semantic_url = base + "/v1/semantic/chat-completions"
        self._health_url = base + "/healthz"
        self._timeout = float(config.semantic_timeout_seconds)
        self._max_request = config.max_semantic_request_bytes
        self._max_response = config.max_response_bytes
        self._authorization = "Bearer " + str(config.semantic_bearer_token)
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _RejectRedirects(),
        ).open
        self._health_verified = False
        self._last_runtime_epoch: str | None = None

    @property
    def health_verified(self) -> bool:
        return self._health_verified

    @property
    def last_runtime_epoch(self) -> str | None:
        return self._last_runtime_epoch

    def _json(
        self,
        url: str,
        *,
        method: str,
        payload: Mapping[str, Any] | None = None,
        authenticated: bool = False,
    ) -> dict[str, Any]:
        body = None if payload is None else _canonical_bytes(payload)
        if body is not None:
            _require(len(body) <= self._max_request, "semantic request too large")
        headers = {
            "Accept": "application/json",
            "User-Agent": "pathfinder-full-flow-runtime/1",
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
            with self._opener(request, timeout=self._timeout) as response:
                _require(response.status == 200, "semantic endpoint is not ready")
                _require(
                    response.headers.get_content_type() == "application/json",
                    "semantic endpoint returned non-JSON content",
                )
                raw = response.read(self._max_response + 1)
                _require(
                    len(raw) <= self._max_response,
                    "semantic response exceeds its byte limit",
                )
        except urllib.error.HTTPError as exc:
            raise FullFlowRuntimeError(
                f"semantic endpoint returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowRuntimeError("semantic endpoint is unreachable") from exc
        value = _strict_json_bytes(raw, "semantic endpoint response")
        _require(isinstance(value, dict), "semantic response must be an object")
        return value

    def _health(self) -> str:
        value = self._json(self._health_url, method="GET")
        _require(value.get("status") == "ok", "N6 health status is not ok")
        _require(value.get("node_id") == FULL_FLOW_INFERENCE_NODE_ID, "wrong N6")
        _require(
            value.get("semantic_quality_enabled") is True
            and value.get("semantic_llm_configured") is True
            and value.get("semantic_vision_request_adapter_supported") is True,
            "N6 semantic vision capability is not ready",
        )
        _require(
            value.get("semantic_vision_request_schema_version")
            == CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
            "N6 semantic vision request schema changed",
        )
        _require(
            value.get("credentials_recorded") is False,
            "N6 health reports recorded credentials",
        )
        epoch = _text(value.get("runtime_epoch"), "N6 runtime_epoch", max_bytes=32)
        _require(_RUNTIME_EPOCH.fullmatch(epoch) is not None, "N6 epoch is invalid")
        return epoch

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        self._health_verified = False
        self._last_runtime_epoch = None
        before = self._health()
        result = self._json(
            self._semantic_url,
            method="POST",
            payload=request,
            authenticated=True,
        )
        after = self._health()
        _require(before == after, "N6 runtime changed during semantic execution")
        self._last_runtime_epoch = before
        self._health_verified = True
        return result


def build_http_full_flow_runtime(
    *,
    route_config: FullFlowRouteConfig,
    http_config: FullFlowHttpConfig,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> FullFlowTrialRuntime:
    """Build production clients without inheriting ambient proxy settings."""
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _RejectRedirects(),
    ).open
    settings = DataAgentClientSettings(
        base_url=http_config.data_agent_base_url,
        token=http_config.data_agent_token,
        timeout_seconds=http_config.data_agent_timeout_seconds,
        max_retries=http_config.max_retries,
        max_response_bytes=http_config.max_response_bytes,
        max_artifact_bytes=limits.max_artifact_bytes,
        simulator_private_http_hosts=(
            http_config.simulator_private_http_hosts
        ),
    )
    client = HttpDataAgentClient(
        settings,
        opener=opener,
        artifact_opener=opener,
    )
    source_attested_client = SourceAttestedDataAgentClient(
        client,
        health_url=(http_config.data_agent_base_url.rstrip("/") + "/healthz"),
        opener=opener,
        timeout_seconds=http_config.data_agent_timeout_seconds,
        max_response_bytes=http_config.max_response_bytes,
    )
    oracle_client: N1OracleHTTPClient | None = None
    if http_config.oracle_base_url is not None:
        oracle_client = N1OracleHTTPClient(
            base_url=http_config.oracle_base_url,
            expected_oracle_id=str(http_config.oracle_id),
            expected_public_task_set_sha256=str(
                http_config.oracle_public_task_set_sha256
            ),
            bearer_token=str(http_config.oracle_token),
            timeout_seconds=http_config.oracle_timeout_seconds,
            simulator_private_http_hosts=(
                http_config.simulator_private_http_hosts
            ),
        )
    forbidden_values = tuple(
        value
        for value in (
            http_config.data_agent_token,
            http_config.oracle_token,
        )
        if value is not None
    )
    return FullFlowTrialRuntime(
        route_config=route_config,
        data_agent_client=source_attested_client,
        semantic_adapter=HttpSemanticVisionAdapter(http_config),
        oracle_client=oracle_client,
        limits=limits,
        forbidden_evidence_values=forbidden_values,
    )


__all__ = [
    "FULL_FLOW_EVIDENCE_SCHEMA_VERSION",
    "FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION",
    "FULL_FLOW_EXECUTOR_NODE_ID",
    "FULL_FLOW_INFERENCE_NODE_ID",
    "FULL_FLOW_REQUEST_SCHEMA_VERSION",
    "FULL_FLOW_REQUEST_V2_SCHEMA_VERSION",
    "FULL_FLOW_SOURCE_NODE_ID",
    "DataAgentSourceAttestation",
    "DataAgentSourceAttestationProvider",
    "FullFlowHttpConfig",
    "FullFlowIdempotencyConflict",
    "FullFlowRouteConfig",
    "FullFlowRuntimeError",
    "FullFlowTrialRuntime",
    "HttpSemanticVisionAdapter",
    "N1OracleClient",
    "SemanticVisionAdapter",
    "SourceAttestedDataAgentClient",
    "build_full_flow_trial_request",
    "build_full_flow_trial_request_v2",
    "build_http_full_flow_runtime",
    "full_flow_binding_sha256",
    "full_flow_n1_score_request_id",
    "validate_full_flow_trial_request_v2",
]
