"""Dependency-injected runtime for one frozen full-flow semantic route.

The semantic matrix and its deployment admission package deliberately stop
before execution: FlowMesh's static API-task graph cannot carry a predecessor
response into a successor request.  This module is the small coordinator that
does carry those values.  It consumes one *already verified* bound trial and
its exact bound stage DAG; deployment clients are supplied as Protocol
adapters so the same coordinator can run against in-memory tests, the local
eight-container stack, or later VM services.

No endpoint or credential is written to the returned evidence.  In
particular, an indexed-raw route is executable only when N2 supplies exact
inclusive byte offsets plus a digest for those exact bytes, all bound to the
full N3 artifact identity.  A percentage, timestamp guess, or synthetic byte
fraction is not accepted as a range descriptor.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

from .full_flow_semantic_execution_admission import (
    BOUND_STAGE_SCHEMA_VERSION,
    BOUND_TRIAL_SCHEMA_VERSION,
)
from .full_flow_semantic_route_evidence import (
    SEMANTIC_ROUTE_EPISODE_EVIDENCE_SCHEMA_VERSION,
    SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
    verify_public_semantic_route_evidence,
)
from .full_flow_semantic_input_profiles import (
    SemanticInputProfileError,
    model_input_frontier_representation_ids,
    profile_sha256,
    validate_semantic_input_profile,
)
from .hidden_oracle import (
    N1_SCORE_REQUEST_SCHEMA_VERSION,
    N1_SCORE_RESULT_SCHEMA_VERSION,
    build_n1_public_task_binding,
    build_n1_score_request,
)


NEUTRAL_OBSERVATION_CANDIDATE_SCHEMA_VERSION = (
    "pathfinder.simulator-neutral-observation-candidate/v1alpha1"
)
EXACT_CONTENT_RANGE_SCHEMA_VERSION = (
    "pathfinder.exact-content-range/v1alpha1"
)
EXACT_TEMPORAL_FRAME_SELECTION_SCHEMA_VERSION = (
    "pathfinder.exact-temporal-frame-selection/v1alpha1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_ROUTE_FAMILIES = {
    "raw",
    "indexed-raw",
    "indexed-derived",
    "remote-derived",
    "local-cache-derived",
}
_INPUT_MODES = {
    "raw-prepared-frames",
    "direct-video",
    "digest",
    "frame-bundle",
    "digest+frames-fusion",
    "digest+indexed-frames-fusion",
}
_SCORE_RESULT_FIELDS = {
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
}


class SemanticRouteRuntimeError(RuntimeError):
    """Raised before incomplete or ambiguous route evidence is accepted."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise SemanticRouteRuntimeError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SemanticRouteRuntimeError("value is not canonical JSON") from exc


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


def _text(value: Any, name: str, *, maximum: int = 16_384) -> str:
    _require(isinstance(value, str) and bool(value), f"{name} must be text")
    _require(len(value.encode("utf-8")) <= maximum, f"{name} is too large")
    return str(value)


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    _require(
        type(value) is int and value >= minimum,
        f"{name} must be an integer >= {minimum}",
    )
    return int(value)


def _number(value: Any, name: str) -> float:
    _require(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) >= 0,
        f"{name} must be a finite non-negative number",
    )
    return float(value)


def _copy_json(value: Any) -> Any:
    return json.loads(_canonical(value))


@dataclass(frozen=True)
class ArtifactIdentity:
    """Content identity frozen by the semantic artifact binding set."""

    object_id: str
    representation_id: str
    artifact_sha256: str
    artifact_size_bytes: int
    object_catalog_version: str

    def __post_init__(self) -> None:
        _identifier(self.object_id, "artifact object_id")
        _identifier(self.representation_id, "representation_id")
        _digest(self.artifact_sha256, "artifact_sha256")
        _integer(
            self.artifact_size_bytes,
            "artifact_size_bytes",
            minimum=1,
        )
        _identifier(self.object_catalog_version, "object_catalog_version")

    @property
    def commitment(self) -> str:
        return _sha256(_canonical(self.to_dict()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "object_id": self.object_id,
            "representation_id": self.representation_id,
            "artifact_sha256": self.artifact_sha256,
            "artifact_size_bytes": self.artifact_size_bytes,
            "object_catalog_version": self.object_catalog_version,
        }


@dataclass(frozen=True)
class ExactContentRange:
    """An exact, content-bound N2 selection over one N3 raw artifact.

    ``range_end`` is inclusive, matching HTTP ``Range`` and
    :meth:`HttpDataAgentClient.fetch_binary_artifact_range`.
    """

    object_id: str
    representation_id: str
    object_catalog_version: str
    full_artifact_size_bytes: int
    full_artifact_sha256: str
    range_start: int
    range_end: int
    range_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.object_id, "range object_id")
        _require(
            self.representation_id == "raw_video",
            "an exact content range must describe raw_video",
        )
        _identifier(self.object_catalog_version, "range catalog version")
        full_size = _integer(
            self.full_artifact_size_bytes,
            "full_artifact_size_bytes",
            minimum=1,
        )
        _digest(self.full_artifact_sha256, "full_artifact_sha256")
        start = _integer(self.range_start, "range_start")
        end = _integer(self.range_end, "range_end")
        _require(end >= start, "range_end precedes range_start")
        _require(end < full_size, "exact content range exceeds its artifact")
        _digest(self.range_sha256, "range_sha256")

    @property
    def range_size_bytes(self) -> int:
        return self.range_end - self.range_start + 1

    @property
    def descriptor_sha256(self) -> str:
        return _sha256(_canonical(self.to_dict()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": EXACT_CONTENT_RANGE_SCHEMA_VERSION,
            "object_id": self.object_id,
            "representation_id": self.representation_id,
            "object_catalog_version": self.object_catalog_version,
            "full_artifact_size_bytes": self.full_artifact_size_bytes,
            "full_artifact_sha256": self.full_artifact_sha256,
            "range_start": self.range_start,
            "range_end": self.range_end,
            "range_size_bytes": self.range_size_bytes,
            "range_sha256": self.range_sha256,
            "selection_semantics": "exact-inclusive-content-bound-range",
            "fractional_or_estimated_range": False,
        }

    def matches(self, identity: ArtifactIdentity) -> bool:
        return (
            self.object_id == identity.object_id
            and self.representation_id == identity.representation_id
            and self.object_catalog_version == identity.object_catalog_version
            and self.full_artifact_size_bytes == identity.artifact_size_bytes
            and self.full_artifact_sha256 == identity.artifact_sha256
        )


@dataclass(frozen=True)
class ExactTemporalFrameSelection:
    """A real N3-side temporal projection bound to its authoritative MP4."""

    object_id: str
    representation_id: str
    object_catalog_version: str
    full_artifact_size_bytes: int
    full_artifact_sha256: str
    selected_representation_id: str
    selected_artifact_size_bytes: int
    selected_artifact_sha256: str
    frame_count: int
    temporal_start_fraction: float
    temporal_end_fraction: float
    selection_policy_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.object_id, "temporal selection object_id")
        _require(
            self.representation_id == "raw_video",
            "a temporal selection must bind an authoritative raw_video",
        )
        _identifier(
            self.object_catalog_version,
            "temporal selection catalog version",
        )
        source_size = _integer(
            self.full_artifact_size_bytes,
            "full_artifact_size_bytes",
            minimum=1,
        )
        _digest(self.full_artifact_sha256, "full_artifact_sha256")
        _require(
            self.selected_representation_id
            == "indexed_temporal_frame_bundle",
            "temporal selection has the wrong execution representation",
        )
        selected_size = _integer(
            self.selected_artifact_size_bytes,
            "selected_artifact_size_bytes",
            minimum=1,
        )
        _require(
            selected_size < source_size,
            "temporal selection does not reduce source transfer bytes",
        )
        _digest(self.selected_artifact_sha256, "selected_artifact_sha256")
        _integer(self.frame_count, "frame_count", minimum=1)
        start = _number(
            self.temporal_start_fraction,
            "temporal_start_fraction",
        )
        end = _number(
            self.temporal_end_fraction,
            "temporal_end_fraction",
        )
        _require(
            0.0 <= start < end <= 1.0,
            "temporal selection window is invalid",
        )
        _digest(self.selection_policy_sha256, "selection_policy_sha256")

    @property
    def descriptor_sha256(self) -> str:
        return _sha256(_canonical(self.to_dict()))

    @property
    def selected_identity(self) -> ArtifactIdentity:
        return ArtifactIdentity(
            object_id=self.object_id,
            representation_id=self.selected_representation_id,
            artifact_sha256=self.selected_artifact_sha256,
            artifact_size_bytes=self.selected_artifact_size_bytes,
            object_catalog_version=self.object_catalog_version,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": EXACT_TEMPORAL_FRAME_SELECTION_SCHEMA_VERSION,
            "object_id": self.object_id,
            "representation_id": self.representation_id,
            "object_catalog_version": self.object_catalog_version,
            "full_artifact_size_bytes": self.full_artifact_size_bytes,
            "full_artifact_sha256": self.full_artifact_sha256,
            "selected_representation_id": self.selected_representation_id,
            "selected_artifact_size_bytes": self.selected_artifact_size_bytes,
            "selected_artifact_sha256": self.selected_artifact_sha256,
            "frame_count": self.frame_count,
            "temporal_window_fraction": [
                self.temporal_start_fraction,
                self.temporal_end_fraction,
            ],
            "selection_policy_sha256": self.selection_policy_sha256,
            "selection_semantics": "source-decoded-temporal-frame-bundle",
            "partial_mp4_byte_range_claimed": False,
            "source_side_projection_executed": True,
        }

    def matches(self, identity: ArtifactIdentity) -> bool:
        return (
            self.object_id == identity.object_id
            and self.representation_id == identity.representation_id
            and self.object_catalog_version == identity.object_catalog_version
            and self.full_artifact_size_bytes == identity.artifact_size_bytes
            and self.full_artifact_sha256 == identity.artifact_sha256
        )


ExactSourceSelection = ExactContentRange | ExactTemporalFrameSelection


@dataclass(frozen=True)
class AdapterTelemetry:
    service_time_ms: float = 0.0
    bytes_read: int = 0
    bytes_sent: int = 0

    def __post_init__(self) -> None:
        _number(self.service_time_ms, "service_time_ms")
        _integer(self.bytes_read, "bytes_read")
        _integer(self.bytes_sent, "bytes_sent")


@dataclass(frozen=True)
class ControlAdmission:
    admission_sha256: str
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _digest(self.admission_sha256, "control admission digest")


@dataclass(frozen=True)
class IndexSelection:
    selected_object_id: str
    index_result_sha256: str
    segment: ExactSourceSelection | None = None
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _identifier(self.selected_object_id, "selected_object_id")
        _digest(self.index_result_sha256, "index_result_sha256")


@dataclass(frozen=True)
class ArtifactAccess:
    source_identity: ArtifactIdentity
    payload: bytes
    segment: ExactSourceSelection | None = None
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _require(isinstance(self.payload, bytes), "artifact payload must be bytes")

    @property
    def payload_sha256(self) -> str:
        return _sha256(self.payload)


@dataclass(frozen=True)
class TransferResult:
    value: Any
    transfer_sha256: str
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)
    application_shaping_profile_id: str | None = None
    configured_application_shaping_target_ms: float = 0.0

    def __post_init__(self) -> None:
        _digest(self.transfer_sha256, "transfer_sha256")
        if self.application_shaping_profile_id is not None:
            _identifier(
                self.application_shaping_profile_id,
                "application_shaping_profile_id",
            )
        _number(
            self.configured_application_shaping_target_ms,
            "configured_application_shaping_target_ms",
        )
        _require(
            self.application_shaping_profile_id is not None
            or self.configured_application_shaping_target_ms == 0.0,
            "configured shaping target lacks a profile identity",
        )


@dataclass(frozen=True)
class CacheLookupResult:
    node_id: str
    cache_id: str
    branch: str
    runtime_epoch: str
    lookup_sha256: str
    source_insert_trial_key: str | None = None
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _require(self.node_id in {"N7", "N8"}, "cache must run on N7 or N8")
        _identifier(self.cache_id, "cache_id")
        _require(self.branch in {"hit", "miss"}, "cache branch is invalid")
        _identifier(self.runtime_epoch, "cache runtime_epoch")
        _digest(self.lookup_sha256, "lookup_sha256")
        if self.branch == "hit":
            _require(
                isinstance(self.source_insert_trial_key, str)
                and bool(self.source_insert_trial_key),
                "cache hit lacks its prerequisite insertion trial",
            )
        else:
            _require(
                self.source_insert_trial_key is None,
                "cache miss cannot claim a prerequisite insertion",
            )


@dataclass(frozen=True)
class CacheInsertResult:
    node_id: str
    cache_id: str
    runtime_epoch: str
    insert_sha256: str
    artifact: ArtifactAccess
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _require(self.node_id in {"N7", "N8"}, "cache must run on N7 or N8")
        _identifier(self.cache_id, "cache_id")
        _identifier(self.runtime_epoch, "cache runtime_epoch")
        _digest(self.insert_sha256, "insert_sha256")


@dataclass(frozen=True)
class BranchJoinResult:
    artifact: ArtifactAccess
    branch: str
    join_sha256: str
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _require(self.branch in {"hit", "miss"}, "joined cache branch is invalid")
        _digest(self.join_sha256, "join_sha256")


@dataclass(frozen=True)
class PreparedSemanticInput:
    mode: str
    payload: bytes
    component_identities: tuple[ArtifactIdentity, ...]
    preparation_sha256: str
    request_binding_stage_key: str
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _require(self.mode in _INPUT_MODES, "semantic input mode is unsupported")
        _require(isinstance(self.payload, bytes) and self.payload, "model input is empty")
        _require(bool(self.component_identities), "model input has no components")
        _digest(self.preparation_sha256, "preparation_sha256")
        # The stage key the semantic request ID was derived from. A later
        # stage (``infer``) must revalidate that ID against *this* key, not
        # against whichever stage happens to be executing, or a legitimate
        # two-stage route fails its own binding check.
        _text(
            self.request_binding_stage_key,
            "request_binding_stage_key",
            maximum=2048,
        )

    @property
    def payload_sha256(self) -> str:
        return _sha256(self.payload)


@dataclass(frozen=True)
class SemanticInferenceResult:
    final_answer: str
    model: str
    input_sha256: str
    request_sha256: str
    result_sha256: str
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _text(self.final_answer, "final_answer", maximum=64 * 1024)
        _identifier(self.model, "model")
        _digest(self.input_sha256, "semantic input digest")
        _digest(self.request_sha256, "semantic request digest")
        _digest(self.result_sha256, "semantic result digest")


@dataclass(frozen=True)
class AuthenticatedN1Score:
    result: Mapping[str, Any]
    authentication_verified: bool
    verification_sha256: str
    telemetry: AdapterTelemetry = field(default_factory=AdapterTelemetry)

    def __post_init__(self) -> None:
        _require(
            self.authentication_verified is True,
            "N1 score was not authenticated",
        )
        _digest(self.verification_sha256, "N1 verification digest")


@dataclass(frozen=True)
class ProvisioningReference:
    chain_id: str
    logical_object_id: str
    artifact_identity: ArtifactIdentity
    n5_evidence_sha256: str
    n4_publication_sha256: str
    available: bool

    def __post_init__(self) -> None:
        _text(self.chain_id, "provisioning chain_id", maximum=512)
        _identifier(self.logical_object_id, "provisioning logical_object_id")
        _digest(self.n5_evidence_sha256, "N5 evidence digest")
        _digest(self.n4_publication_sha256, "N4 publication digest")
        _require(self.available is True, "provisioned artifact is unavailable")


@runtime_checkable
class TrialControlAdapter(Protocol):
    def admit(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
    ) -> ControlAdmission: ...


@runtime_checkable
class IndexQueryAdapter(Protocol):
    def query(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        expected_object_id: str,
    ) -> IndexSelection: ...


@runtime_checkable
class ArtifactSourceAdapter(Protocol):
    def fetch_full(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        upstream_values: Sequence[Any],
    ) -> ArtifactAccess: ...

    def fetch_selected(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        source_identity: ArtifactIdentity,
        selection: ExactTemporalFrameSelection,
    ) -> ArtifactAccess: ...


@runtime_checkable
class RangeFetcher(Protocol):
    """Structural subset of ``HttpDataAgentClient`` used by indexed raw."""

    def fetch_binary_artifact_range(
        self,
        request: Any,
        *,
        range_start: int,
        range_end: int,
        expected_range_sha256: str,
        allowed_media_types: frozenset[str] | set[str] | tuple[str, ...],
        on_phase: Callable[[str], None] | None = None,
    ) -> Any: ...


@runtime_checkable
class N3RangeRequestFactory(Protocol):
    def build_request(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        selection: IndexSelection,
    ) -> Any: ...


@runtime_checkable
class ByteTransferAdapter(Protocol):
    def transfer(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        value: Any,
    ) -> TransferResult: ...


@runtime_checkable
class ArtifactCacheAdapter(Protocol):
    def lookup(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
    ) -> CacheLookupResult: ...

    def read(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        lookup: CacheLookupResult,
    ) -> ArtifactAccess: ...

    def insert(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        lookup: CacheLookupResult,
        artifact: ArtifactAccess,
    ) -> CacheInsertResult: ...


@runtime_checkable
class ModelInputAdapter(Protocol):
    def prepare(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        mode: str,
        artifacts: Sequence[ArtifactAccess],
    ) -> PreparedSemanticInput: ...


@runtime_checkable
class SemanticInferenceAdapter(Protocol):
    def infer(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        model_input: PreparedSemanticInput,
    ) -> SemanticInferenceResult: ...


@runtime_checkable
class AuthenticatedN1ScoringAdapter(Protocol):
    def score_once_and_verify(
        self,
        request: Mapping[str, Any],
    ) -> AuthenticatedN1Score: ...


@runtime_checkable
class ProvisioningReferenceAdapter(Protocol):
    def resolve(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        chain_id: str,
        logical_object_id: str,
        identity: ArtifactIdentity,
    ) -> ProvisioningReference: ...


@runtime_checkable
class RouteExecutionStore(Protocol):
    def begin(self, execution_id: str, request_sha256: str) -> Mapping[str, Any] | None: ...

    def complete(
        self,
        execution_id: str,
        request_sha256: str,
        evidence: Mapping[str, Any],
    ) -> None: ...

    def fail(self, execution_id: str, request_sha256: str, reason: str) -> None: ...


@dataclass(frozen=True)
class SemanticRouteAdapters:
    control: TrialControlAdapter
    index: IndexQueryAdapter
    artifacts: ArtifactSourceAdapter
    range_fetcher: RangeFetcher
    range_request_factory: N3RangeRequestFactory
    transport: ByteTransferAdapter
    cache: ArtifactCacheAdapter
    model_input: ModelInputAdapter
    semantic: SemanticInferenceAdapter
    scorer: AuthenticatedN1ScoringAdapter
    provisioning: ProvisioningReferenceAdapter
    raw_range_allowed_media_types: tuple[str, ...] = ("video/mp4",)

    def __post_init__(self) -> None:
        _require(
            bool(self.raw_range_allowed_media_types)
            and len(self.raw_range_allowed_media_types)
            == len(set(self.raw_range_allowed_media_types))
            and all(
                isinstance(value, str) and "/" in value
                for value in self.raw_range_allowed_media_types
            ),
            "raw range media type allowlist is invalid",
        )


class InMemoryRouteExecutionStore:
    """Thread-safe exactly-once store for tests and one-process local smokes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._rows: dict[str, dict[str, Any]] = {}

    def begin(
        self,
        execution_id: str,
        request_sha256: str,
    ) -> Mapping[str, Any] | None:
        _identifier(execution_id, "execution_id")
        _digest(request_sha256, "request_sha256")
        with self._lock:
            current = self._rows.get(execution_id)
            if current is None:
                self._rows[execution_id] = {
                    "request_sha256": request_sha256,
                    "state": "RUNNING",
                }
                return None
            _require(
                current["request_sha256"] == request_sha256,
                "execution_id was reused for different frozen input",
            )
            _require(
                current["state"] != "RUNNING",
                "route execution is already in progress",
            )
            _require(
                current["state"] == "COMPLETE",
                "failed route execution cannot be replayed ambiguously",
            )
            return _copy_json(current["evidence"])

    def complete(
        self,
        execution_id: str,
        request_sha256: str,
        evidence: Mapping[str, Any],
    ) -> None:
        with self._lock:
            current = self._rows.get(execution_id)
            _require(
                current == {
                    "request_sha256": request_sha256,
                    "state": "RUNNING",
                },
                "route execution store completion state changed",
            )
            self._rows[execution_id] = {
                "request_sha256": request_sha256,
                "state": "COMPLETE",
                "evidence": _copy_json(evidence),
            }

    def fail(self, execution_id: str, request_sha256: str, reason: str) -> None:
        with self._lock:
            current = self._rows.get(execution_id)
            if current == {
                "request_sha256": request_sha256,
                "state": "RUNNING",
            }:
                self._rows[execution_id] = {
                    "request_sha256": request_sha256,
                    "state": "FAILED",
                    "reason_sha256": _sha256(reason.encode("utf-8")),
                }


def _identity_from_row(row: Mapping[str, Any]) -> tuple[str, ArtifactIdentity]:
    logical_object_id = _identifier(
        row.get("logical_object_id"), "logical_object_id"
    )
    artifact_object_id = _identifier(
        row.get("artifact_object_id"), "artifact_object_id"
    )
    representation_id = _identifier(
        row.get("representation_id"), "representation_id"
    )
    binding = row.get("representation_binding")
    _require(isinstance(binding, Mapping), "representation binding is missing")
    _require(
        binding.get("representation_id") == representation_id,
        "representation binding identity changed",
    )
    return logical_object_id, ArtifactIdentity(
        object_id=artifact_object_id,
        representation_id=representation_id,
        artifact_sha256=_digest(
            binding.get("artifact_sha256"), "artifact_sha256"
        ),
        artifact_size_bytes=_integer(
            binding.get("artifact_size_bytes"),
            "artifact_size_bytes",
            minimum=1,
        ),
        object_catalog_version=_identifier(
            binding.get("object_catalog_version"),
            "object_catalog_version",
        ),
    )


def _validate_public_task(trial: Mapping[str, Any]) -> dict[str, Any]:
    task = trial.get("public_task_binding")
    _require(isinstance(task, Mapping), "public task binding is missing")
    try:
        rebuilt = build_n1_public_task_binding(
            workload_id=task.get("workload_id"),
            object_id=task.get("object_id"),
            task_class_id=task.get("task_class_id"),
            question=task.get("question"),
            answer_options=task.get("answer_options"),
            success_scoring_rule=task.get("success_scoring_rule"),
        )
    except Exception as exc:
        raise SemanticRouteRuntimeError("public task binding is invalid") from exc
    _require(
        _canonical(rebuilt) == _canonical(task),
        "public task binding is not canonical",
    )
    _require(
        rebuilt["task_binding_sha256"]
        == trial.get("public_task_binding_sha256"),
        "public task digest differs from bound trial",
    )
    _require(
        rebuilt["workload_id"] == trial.get("workload_id")
        and rebuilt["object_id"] == trial.get("artifact_object_id"),
        "public task workload or object differs from bound trial",
    )
    return rebuilt


def _validate_trial_and_stages(
    trial_value: Mapping[str, Any],
    stage_values: Sequence[Mapping[str, Any]],
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, tuple[str, ArtifactIdentity]],
    dict[str, Any],
]:
    _require(isinstance(trial_value, Mapping), "bound trial must be an object")
    trial = _copy_json(trial_value)
    _require(
        trial.get("schema_version") == BOUND_TRIAL_SCHEMA_VERSION,
        "bound trial schema changed",
    )
    trial_key = _text(trial.get("trial_key"), "trial_key", maximum=1024)
    route_family = trial.get("route_family")
    _require(route_family in _ROUTE_FAMILIES, "route family is unsupported")
    _require(
        trial.get("executor_node_id") in {"N7", "N8"},
        "route executor must be N7 or N8",
    )
    public_task = _validate_public_task(trial)

    raw_identities = trial.get("representation_identities")
    _require(
        isinstance(raw_identities, list) and bool(raw_identities),
        "bound trial has no representation identities",
    )
    identities: dict[str, tuple[str, ArtifactIdentity]] = {}
    for raw in raw_identities:
        _require(isinstance(raw, Mapping), "representation identity is invalid")
        logical_id, identity = _identity_from_row(raw)
        _require(
            identity.representation_id not in identities,
            "bound trial repeats a representation identity",
        )
        _require(
            identity.object_id == trial.get("artifact_object_id"),
            "bound artifact object identity changed",
        )
        identities[identity.representation_id] = (logical_id, identity)
    if route_family in {"raw", "indexed-raw"}:
        _require(
            set(identities) == {"raw_video"},
            "raw route must bind only raw_video",
        )
    elif route_family == "indexed-derived":
        _require(
            set(identities) == {"raw_video", "multimodal_digest"},
            "indexed-derived route requires raw_video and multimodal_digest",
        )
    else:
        _require(
            set(identities) <= {"multimodal_digest", "sampled_frame_bundle"},
            "derived route binds an unsupported representation",
        )

    keys = trial.get("semantic_stage_keys")
    hashes = trial.get("bound_stage_sha256")
    _require(
        isinstance(keys, list)
        and isinstance(hashes, list)
        and len(keys) == len(hashes)
        and bool(keys),
        "bound stage identity list is invalid",
    )
    _require(len(keys) == len(set(keys)), "bound trial repeats a stage key")
    _require(
        len(stage_values) == len(keys),
        "bound stage set is missing or contains unused results",
    )
    by_key: dict[str, dict[str, Any]] = {}
    for raw_stage in stage_values:
        _require(isinstance(raw_stage, Mapping), "bound stage is invalid")
        stage = _copy_json(raw_stage)
        key = _text(stage.get("stage_key"), "stage_key", maximum=2048)
        _require(key not in by_key, "bound stage key repeats")
        by_key[key] = stage
    _require(set(by_key) == set(keys), "bound stages do not exactly match trial")
    stages = [by_key[key] for key in keys]
    for position, (stage, expected_hash) in enumerate(zip(stages, hashes)):
        _require(
            stage.get("schema_version") == BOUND_STAGE_SCHEMA_VERSION,
            "bound stage schema changed",
        )
        _require(stage.get("trial_key") == trial_key, "stage trial binding changed")
        _require(stage.get("stage_index") == position, "stage order changed")
        _require(
            _sha256(_canonical(stage))
            == _digest(expected_hash, "bound stage SHA-256"),
            f"bound stage content changed: {stage['stage_key']}",
        )
        dependencies = stage.get("dependency_stage_keys")
        _require(
            isinstance(dependencies, list)
            and len(dependencies) == len(set(dependencies)),
            "stage dependency list is invalid",
        )
        prior = set(keys[:position])
        _require(
            all(value in prior for value in dependencies),
            f"stage has a missing or forward dependency: {stage['stage_key']}",
        )
        condition = stage.get("condition")
        if condition is not None:
            _require(
                isinstance(condition, dict)
                and set(condition)
                == {"cache_operation_id", "cache_operation_key", "equals"}
                and condition.get("equals") in {"hit", "miss"}
                and condition.get("cache_operation_key") in prior,
                f"stage cache condition changed: {stage['stage_key']}",
            )
            lookup = by_key[condition["cache_operation_key"]]
            _require(
                lookup.get("action") == "lookup",
                "cache condition does not refer to a lookup stage",
            )
        identity_row = stage.get("object_representation_identity")
        _require(
            isinstance(identity_row, Mapping),
            "stage object representation identity is missing",
        )
        representation_id = identity_row.get("representation_id")
        binding = identity_row.get("representation_binding")
        if representation_id is not None:
            _require(
                representation_id in identities and isinstance(binding, Mapping),
                "stage representation is not bound by its trial",
            )
            logical_id, identity = identities[representation_id]
            _require(
                identity_row.get("logical_object_id") == logical_id
                and identity_row.get("artifact_object_id") == identity.object_id
                and _canonical(binding)
                == _canonical({
                    "representation_id": identity.representation_id,
                    "artifact_sha256": identity.artifact_sha256,
                    "artifact_size_bytes": identity.artifact_size_bytes,
                    "object_catalog_version": identity.object_catalog_version,
                }),
                "stage representation identity differs from bound trial",
            )

    actions = [stage.get("action") for stage in stages]
    _require(actions.count("infer") == 1, "trial must contain one inference")
    _require(
        actions.count("score-hidden-answer") == 1
        and actions[-1] == "score-hidden-answer",
        "trial must end in exactly one hidden score",
    )
    _require(actions[0] == "admit-trial", "trial must begin with N1 admission")
    if route_family in {"indexed-raw", "indexed-derived"}:
        _require(
            any(
                stage.get("action") == "query-index"
                and stage.get("logical_node_ids") == ["N2"]
                for stage in stages
            ),
            "indexed route lacks its N2 query",
        )
    if route_family == "local-cache-derived":
        _require("lookup" in actions, "cache route lacks a lookup")
    else:
        _require(
            not any(
                action in {"lookup", "read", "insert", "join-hit-or-miss-branch"}
                for action in actions
            ),
            "non-cache route contains cache stages",
        )
    profile = trial.get("semantic_input_profile")
    if route_family == "indexed-derived":
        _require(
            isinstance(profile, Mapping),
            "indexed-derived route requires a frozen fusion profile",
        )
    if profile is not None:
        frontier = model_input_frontier_representation_ids(stages)
        try:
            validate_semantic_input_profile(
                profile,
                route_family=str(route_family),
                model_input_representation_ids=frontier,
            )
        except SemanticInputProfileError as exc:
            raise SemanticRouteRuntimeError(str(exc)) from exc
    return trial, stages, identities, public_task


def _prepared_request(value: PreparedSemanticInput) -> dict[str, Any]:
    try:
        request = json.loads(value.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SemanticRouteRuntimeError(
            "prepared N6 input is not UTF-8 JSON"
        ) from exc
    _require(isinstance(request, dict), "prepared N6 input must be an object")
    _require(
        _canonical(request) == value.payload,
        "prepared N6 input is not canonical JSON",
    )
    return request


def _semantic_input_evidence(
    prepared: PreparedSemanticInput,
    profile_value: Any,
) -> dict[str, Any]:
    request = _prepared_request(prepared)
    content = dict(request)
    request_id = content.pop("semantic_request_id", None)
    if profile_value is not None:
        _require(
            isinstance(request_id, str),
            "profiled N6 input has no request identity",
        )
    direct_video_sha256: str | None = None
    direct_video_size_bytes: int | None = None
    if prepared.mode == "direct-video":
        encoded = request.get("video_base64")
        _require(
            isinstance(encoded, str) and bool(encoded),
            "prepared N6 direct video payload is missing",
        )
        try:
            video_payload = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise SemanticRouteRuntimeError(
                "prepared N6 direct video payload is invalid"
            ) from exc
        _require(bool(video_payload), "prepared N6 direct video payload is empty")
        direct_video_sha256 = hashlib.sha256(video_payload).hexdigest()
        direct_video_size_bytes = len(video_payload)
        # Evidence must commit to the bytes actually sent for inference, not
        # merely to a telemetry flag asserting that video was used.
        _require(
            request.get("video_sha256") == direct_video_sha256
            and request.get("video_size_bytes") == direct_video_size_bytes
            and request.get("representation_sha256") == direct_video_sha256,
            "prepared N6 direct video does not bind its delivered bytes",
        )
        _require(
            prepared.component_identities[0].artifact_sha256
            == direct_video_sha256,
            "prepared N6 direct video differs from its routed artifact",
        )

    frames_value = request.get("frames", [])
    _require(isinstance(frames_value, list), "prepared N6 frames are invalid")
    frame_timestamps: list[float] = []
    frame_dimensions: list[dict[str, int]] = []
    frame_payload_bytes = 0
    for frame in frames_value:
        _require(isinstance(frame, Mapping), "prepared N6 frame is invalid")
        timestamp = frame.get("timestamp_seconds")
        width = frame.get("width")
        height = frame.get("height")
        encoded = frame.get("jpeg_base64")
        _require(
            not isinstance(timestamp, bool)
            and isinstance(timestamp, (int, float))
            and math.isfinite(float(timestamp))
            and float(timestamp) >= 0.0,
            "prepared N6 frame timestamp is invalid",
        )
        _require(
            type(width) is int
            and width > 0
            and type(height) is int
            and height > 0
            and isinstance(encoded, str)
            and bool(encoded),
            "prepared N6 frame shape is invalid",
        )
        try:
            payload = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise SemanticRouteRuntimeError(
                "prepared N6 frame payload is invalid"
            ) from exc
        _require(bool(payload), "prepared N6 frame payload is empty")
        frame_timestamps.append(float(timestamp))
        frame_dimensions.append({"width": width, "height": height})
        frame_payload_bytes += len(payload)

    profile: Mapping[str, Any] | None = None
    if profile_value is not None:
        _require(
            isinstance(profile_value, Mapping),
            "semantic input profile is invalid",
        )
        profile = profile_value
        selection = profile.get("frame_selection")
        expected_count = 0 if selection is None else selection.get("frame_count")
        _require(
            expected_count == len(frames_value)
            and profile.get("input_mode") == prepared.mode,
            "prepared N6 input differs from its semantic profile",
        )
        temporal_window = (
            None
            if selection is None
            else selection.get("temporal_window_fraction")
        )
        profile_id = profile.get("profile_id")
        profile_digest = profile_sha256(profile)
        profile_verified = True
    else:
        temporal_window = None
        profile_id = None
        profile_digest = None
        profile_verified = False

    frame_sequence = request.get("frame_sequence_sha256")
    if frames_value:
        _digest(frame_sequence, "frame_sequence_sha256")
    else:
        _require(frame_sequence is None, "digest input names a frame sequence")
    digest_input = request.get("digest_sha256")
    if prepared.mode == "digest":
        digest_input = prepared.component_identities[0].artifact_sha256
    if digest_input is not None:
        _digest(digest_input, "digest_input_sha256")
    return {
        "semantic_input_profile_id": profile_id,
        "semantic_input_profile_sha256": profile_digest,
        "semantic_input_profile_verified": profile_verified,
        "semantic_content_sha256": _sha256(_canonical(content)),
        "frame_count": len(frames_value),
        "frame_timestamps_seconds": frame_timestamps,
        "frame_dimensions": frame_dimensions,
        "frame_payload_bytes": frame_payload_bytes,
        "frame_sequence_sha256": frame_sequence,
        "digest_input_sha256": digest_input,
        "temporal_window_fraction": temporal_window,
        "direct_video_input": prepared.mode == "direct-video",
        "direct_video_sha256": direct_video_sha256,
        "direct_video_size_bytes": direct_video_size_bytes,
    }


def _unwrap(value: Any) -> Any:
    while isinstance(value, TransferResult):
        value = value.value
    if isinstance(value, CacheInsertResult):
        return value.artifact
    if isinstance(value, BranchJoinResult):
        return value.artifact
    return value


def _find_values(value: Any, kind: type[Any]) -> list[Any]:
    value = _unwrap(value)
    if isinstance(value, kind):
        return [value]
    if isinstance(value, (tuple, list)):
        result: list[Any] = []
        for child in value:
            result.extend(_find_values(child, kind))
        return result
    return []


def _primary_value(values: Sequence[Any]) -> Any:
    meaningful = [
        _unwrap(value)
        for value in values
        if not isinstance(_unwrap(value), (ControlAdmission, CacheLookupResult))
    ]
    if not meaningful:
        meaningful = [_unwrap(value) for value in values]
    _require(len(meaningful) == 1, "transfer has ambiguous predecessor values")
    return meaningful[0]


def _value_commitment(value: Any) -> dict[str, Any]:
    if isinstance(value, TransferResult):
        return {
            "kind": "transfer",
            "transfer_sha256": value.transfer_sha256,
            "forwarded": _value_commitment(value.value),
        }
    if isinstance(value, CacheInsertResult):
        return {
            "kind": "cache-insert",
            "node_id": value.node_id,
            "cache_id": value.cache_id,
            "runtime_epoch": value.runtime_epoch,
            "insert_sha256": value.insert_sha256,
            "artifact": _value_commitment(value.artifact),
        }
    if isinstance(value, BranchJoinResult):
        return {
            "kind": "cache-branch-join",
            "branch": value.branch,
            "join_sha256": value.join_sha256,
            "artifact": _value_commitment(value.artifact),
        }
    value = _unwrap(value)
    if isinstance(value, ArtifactAccess):
        return {
            "kind": "artifact",
            "identity_sha256": value.source_identity.commitment,
            "payload_sha256": value.payload_sha256,
            "payload_size_bytes": len(value.payload),
            "range_descriptor_sha256": (
                None if value.segment is None else value.segment.descriptor_sha256
            ),
        }
    if isinstance(value, IndexSelection):
        return {
            "kind": "index-selection",
            "selected_object_id": value.selected_object_id,
            "index_result_sha256": value.index_result_sha256,
            "range_descriptor_sha256": (
                None if value.segment is None else value.segment.descriptor_sha256
            ),
        }
    if isinstance(value, PreparedSemanticInput):
        return {
            "kind": "semantic-input",
            "mode": value.mode,
            "payload_sha256": value.payload_sha256,
            "payload_size_bytes": len(value.payload),
            "preparation_sha256": value.preparation_sha256,
        }
    if isinstance(value, SemanticInferenceResult):
        return {
            "kind": "semantic-answer",
            "result_sha256": value.result_sha256,
            "answer_sha256": _sha256(value.final_answer.encode("utf-8")),
        }
    if isinstance(value, ControlAdmission):
        return {"kind": "control", "sha256": value.admission_sha256}
    if isinstance(value, CacheLookupResult):
        return {
            "kind": "cache-lookup",
            "node_id": value.node_id,
            "branch": value.branch,
            "runtime_epoch": value.runtime_epoch,
            "lookup_sha256": value.lookup_sha256,
        }
    if isinstance(value, AuthenticatedN1Score):
        return {
            "kind": "authenticated-score",
            "verification_sha256": value.verification_sha256,
        }
    raise SemanticRouteRuntimeError(
        f"unsupported stage result type: {type(value).__name__}"
    )


def _validate_artifact(
    artifact: ArtifactAccess,
    expected: ArtifactIdentity,
    *,
    segment: ExactSourceSelection | None,
) -> None:
    _require(
        artifact.source_identity == expected,
        "N3/N4 source artifact identity changed",
    )
    if segment is None:
        _require(artifact.segment is None, "full artifact unexpectedly became a range")
        _require(
            len(artifact.payload) == expected.artifact_size_bytes
            and artifact.payload_sha256 == expected.artifact_sha256,
            "N3/N4 full artifact bytes differ from frozen identity",
        )
    elif isinstance(segment, ExactContentRange):
        _require(segment.matches(expected), "N2 range does not bind the N3 artifact")
        _require(
            artifact.segment == segment
            and len(artifact.payload) == segment.range_size_bytes
            and artifact.payload_sha256 == segment.range_sha256,
            "N3 exact range differs from the N2 descriptor",
        )
    else:
        _require(
            isinstance(segment, ExactTemporalFrameSelection)
            and segment.matches(expected),
            "N2 temporal selection does not bind the N3 source artifact",
        )
        _require(
            artifact.segment == segment
            and len(artifact.payload) == segment.selected_artifact_size_bytes
            and artifact.payload_sha256 == segment.selected_artifact_sha256,
            "N3 temporal projection differs from the N2 descriptor",
        )


def _range_artifact(
    raw: Any,
    *,
    expected: ArtifactIdentity,
    segment: ExactContentRange,
) -> ArtifactAccess:
    fields = {
        "data": getattr(raw, "data", None),
        "range_start": getattr(raw, "range_start", None),
        "range_end": getattr(raw, "range_end", None),
        "range_size_bytes": getattr(raw, "range_size_bytes", None),
        "range_sha256": getattr(raw, "range_sha256", None),
        "full_artifact_size_bytes": getattr(raw, "full_artifact_size_bytes", None),
        "full_artifact_sha256": getattr(raw, "full_artifact_sha256", None),
        "object_id": getattr(raw, "object_id", None),
        "object_catalog_version": getattr(raw, "object_catalog_version", None),
    }
    _require(isinstance(fields["data"], bytes), "range fetcher returned no bytes")
    _require(
        fields["range_start"] == segment.range_start
        and fields["range_end"] == segment.range_end
        and fields["range_size_bytes"] == segment.range_size_bytes
        and fields["range_sha256"] == segment.range_sha256,
        "range fetcher changed the exact selected range",
    )
    _require(
        fields["full_artifact_size_bytes"] == expected.artifact_size_bytes
        and fields["full_artifact_sha256"] == expected.artifact_sha256
        and fields["object_id"] == expected.object_id
        and fields["object_catalog_version"] == expected.object_catalog_version,
        "range fetcher changed the full N3 source identity",
    )
    elapsed = getattr(raw, "download_elapsed_ms", 0.0)
    artifact = ArtifactAccess(
        source_identity=expected,
        payload=fields["data"],
        segment=segment,
        telemetry=AdapterTelemetry(
            service_time_ms=_number(elapsed, "range download elapsed"),
            bytes_read=segment.range_size_bytes,
        ),
    )
    _validate_artifact(artifact, expected, segment=segment)
    return artifact


def _input_mode(
    artifacts: Sequence[ArtifactAccess],
    route_family: str,
) -> str:
    representations = [value.source_identity.representation_id for value in artifacts]
    _require(
        len(representations) == len(set(representations)),
        "semantic input repeats a representation",
    )
    _require(route_family in _ROUTE_FAMILIES, "route family is unsupported")
    values = set(representations)
    if values == {"raw_video"}:
        # The raw family is the high-fidelity alternative and delivers the
        # complete encoded video.  Indexed raw stays a selective temporal
        # frame projection, so the two must not collapse to one mode.
        if route_family == "raw":
            return "direct-video"
        return "raw-prepared-frames"
    if values == {"raw_video", "multimodal_digest"}:
        _require(
            route_family == "indexed-derived",
            "raw and digest fusion requires indexed-derived route",
        )
        return "digest+indexed-frames-fusion"
    if values == {"multimodal_digest"}:
        return "digest"
    if values == {"sampled_frame_bundle"}:
        return "frame-bundle"
    if values == {"multimodal_digest", "sampled_frame_bundle"}:
        return "digest+frames-fusion"
    raise SemanticRouteRuntimeError(
        f"cannot form an N6 semantic input from {sorted(values)}"
    )


def _validate_prepared(
    prepared: PreparedSemanticInput,
    artifacts: Sequence[ArtifactAccess],
    expected_mode: str,
) -> None:
    _require(prepared.mode == expected_mode, "model input adapter changed input mode")
    expected = tuple(value.source_identity for value in artifacts)
    _require(
        prepared.component_identities == expected,
        "model input components differ from routed artifacts",
    )
    expected_preparation = _sha256(_canonical({
        "mode": prepared.mode,
        "payload_sha256": prepared.payload_sha256,
        "payload_size_bytes": len(prepared.payload),
        "component_identity_sha256": [value.commitment for value in expected],
        "request_binding_stage_key": prepared.request_binding_stage_key,
    }))
    _require(
        prepared.preparation_sha256 == expected_preparation,
        "model input preparation commitment is invalid",
    )


def _validate_authenticated_score(
    authenticated: AuthenticatedN1Score,
    request: Mapping[str, Any],
    *,
    oracle_id: str,
    public_task_set_sha256: str,
    success_scoring_rule: str,
) -> dict[str, Any]:
    _require(
        isinstance(authenticated, AuthenticatedN1Score),
        "N1 adapter returned an unauthenticated result type",
    )
    result = _copy_json(authenticated.result)
    _require(set(result) == _SCORE_RESULT_FIELDS, "N1 score result fields changed")
    _require(
        result.get("schema_version") == N1_SCORE_RESULT_SCHEMA_VERSION
        and result.get("status") == "SCORED"
        and result.get("node_id") == "N1",
        "N1 score result schema, status, or node changed",
    )
    for name in (
        "score_request_id",
        "evaluation_unit_id",
        "run_id",
        "trial_id",
        "object_id",
        "task_binding_sha256",
    ):
        _require(result.get(name) == request.get(name), f"N1 {name} changed")
    _require(result.get("oracle_id") == oracle_id, "N1 oracle_id changed")
    _require(
        result.get("request_sha256") == _sha256(_canonical(request)),
        "N1 score request digest changed",
    )
    _require(
        result.get("prediction_sha256")
        == _sha256(str(request["predicted_answer"]).encode("utf-8")),
        "N1 prediction digest changed",
    )
    _require(
        type(result.get("correct")) is bool
        and result.get("score") == (1.0 if result["correct"] else 0.0),
        "N1 correctness and score disagree",
    )
    _require(
        result.get("success_scoring_rule") == success_scoring_rule,
        "N1 success scoring rule changed",
    )
    _require(
        result.get("public_task_set_sha256") == public_task_set_sha256,
        "N1 public task set binding changed",
    )
    for name in (
        "oracle_instance_hmac_sha256",
        "score_evidence_hmac_sha256",
    ):
        _digest(result.get(name), name)
    _require(
        type(result.get("idempotent_replay")) is bool
        and
        result.get("hidden_answer_returned") is False
        and result.get("credentials_recorded") is False
        and result.get("eligible_for_scientific_claims") is False,
        "N1 score result has unsafe provenance flags",
    )
    core = dict(result)
    content = core.pop("result_content_sha256", None)
    _require(
        content == _sha256(_canonical(core)),
        "N1 result content digest changed",
    )
    expected_verification = _sha256(_canonical({
        "domain": "pathfinder.authenticated-n1-score-verification/v1",
        "request_sha256": result["request_sha256"],
        "result_content_sha256": result["result_content_sha256"],
        "score_evidence_hmac_sha256": result["score_evidence_hmac_sha256"],
    }))
    _require(
        authenticated.verification_sha256 == expected_verification,
        "N1 authentication verification commitment changed",
    )
    return result


def _telemetry(value: Any) -> AdapterTelemetry:
    telemetry = getattr(value, "telemetry", None)
    _require(isinstance(telemetry, AdapterTelemetry), "adapter telemetry is missing")
    return telemetry


def _stage_public_result(value: Any) -> dict[str, Any]:
    commitment = _value_commitment(value)
    telemetry = _telemetry(value)
    return {
        "outcome_kind": commitment["kind"],
        "outcome_sha256": _sha256(_canonical(commitment)),
        "service_time_ms": telemetry.service_time_ms,
        "bytes_read": telemetry.bytes_read,
        "bytes_sent": telemetry.bytes_sent,
    }


def _assert_credential_free(value: Any, path: str = "evidence") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).casefold().replace("-", "_")
            _require(
                key not in {
                    "api_key",
                    "authorization",
                    "bearer_token",
                    "credential_value",
                    "password",
                    "secret",
                    "token",
                    "correct_answer_id",
                    "hidden_labels",
                    "labels",
                },
                f"private field entered route evidence at {path}.{raw_key}",
            )
            _assert_credential_free(child, f"{path}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_credential_free(child, f"{path}[{index}]")
    else:
        _require(not isinstance(value, bytes), f"raw bytes entered route evidence at {path}")


class GenericSemanticRouteCoordinator:
    """Execute exactly one frozen semantic trial through injected adapters."""

    def __init__(
        self,
        *,
        adapters: SemanticRouteAdapters,
        store: RouteExecutionStore,
        oracle_id: str,
        oracle_public_task_set_sha256: str,
    ) -> None:
        self._adapters = adapters
        self._store = store
        self._oracle_id = _identifier(oracle_id, "oracle_id")
        self._public_task_set_sha256 = _digest(
            oracle_public_task_set_sha256,
            "oracle_public_task_set_sha256",
        )

    def execute(
        self,
        *,
        run_id: str,
        bound_trial: Mapping[str, Any],
        bound_stages: Sequence[Mapping[str, Any]],
        cache_episode_id: str | None = None,
    ) -> dict[str, Any]:
        run_id = _text(run_id, "run_id", maximum=256)
        if cache_episode_id is not None:
            cache_episode_id = _identifier(cache_episode_id, "cache_episode_id")
        trial, stages, identities, public_task = _validate_trial_and_stages(
            bound_trial,
            bound_stages,
        )
        trial_key = trial["trial_key"]
        _require(
            cache_episode_id is None
            or trial["route_family"] == "local-cache-derived",
            "cache episode is only valid for a cache route",
        )
        request_core = {
            "domain": "pathfinder.generic-semantic-route-execution/v1",
            "run_id": run_id,
            "trial_key": trial_key,
            "trial_sha256": _sha256(_canonical(trial)),
            "stage_dag_sha256": _sha256(_canonical(stages)),
            "oracle_id": self._oracle_id,
            "oracle_public_task_set_sha256": self._public_task_set_sha256,
        }
        if cache_episode_id is not None:
            request_core["cache_episode_id"] = cache_episode_id
        request_sha256 = _sha256(_canonical(request_core))
        execution_id = _sha256(_canonical({
            "domain": "pathfinder.generic-semantic-route-id/v1",
            "run_id": run_id,
            "trial_key": trial_key,
        }))
        replay = self._store.begin(execution_id, request_sha256)
        if replay is not None:
            result = _copy_json(replay)
            _require(result.get("status") == "COMPLETE", "stored evidence is incomplete")
            result["idempotent_replay"] = True
            return result

        try:
            evidence = self._execute_once(
                run_id=run_id,
                execution_id=execution_id,
                request_sha256=request_sha256,
                trial=trial,
                stages=stages,
                identities=identities,
                public_task=public_task,
                cache_episode_id=cache_episode_id,
            )
            self._store.complete(execution_id, request_sha256, evidence)
            return evidence
        except Exception as exc:
            self._store.fail(execution_id, request_sha256, str(exc))
            if isinstance(exc, SemanticRouteRuntimeError):
                raise
            raise SemanticRouteRuntimeError(
                f"semantic route adapter failed: {type(exc).__name__}"
            ) from exc

    def _execute_once(
        self,
        *,
        run_id: str,
        execution_id: str,
        request_sha256: str,
        trial: dict[str, Any],
        stages: list[dict[str, Any]],
        identities: dict[str, tuple[str, ArtifactIdentity]],
        public_task: dict[str, Any],
        cache_episode_id: str | None = None,
    ) -> dict[str, Any]:
        provisioning: list[ProvisioningReference] = []
        for chain_id in trial.get("required_provisioning_chain_ids", []):
            _text(chain_id, "provisioning chain_id", maximum=512)
            matches = [
                (logical, identity)
                for logical, identity in identities.values()
                if chain_id == f"artifact|{logical}|{identity.representation_id}"
            ]
            _require(
                len(matches) == 1,
                "N5 provisioning reference does not bind one trial artifact",
            )
            logical, identity = matches[0]
            value = self._adapters.provisioning.resolve(
                run_id=run_id,
                trial=trial,
                chain_id=chain_id,
                logical_object_id=logical,
                identity=identity,
            )
            _require(
                isinstance(value, ProvisioningReference)
                and value.chain_id == chain_id
                and value.logical_object_id == logical
                and value.artifact_identity == identity,
                "N5/N4 provisioning reference changed",
            )
            provisioning.append(value)

        results: dict[str, Any] = {}
        active: set[str] = set()
        inactive: set[str] = set()
        consumed: set[str] = set()
        stage_evidence: list[dict[str, Any]] = []
        cache_evidence: list[dict[str, Any]] = []
        cache_epochs: set[tuple[str, str, str]] = set()
        prepared_input: PreparedSemanticInput | None = None
        semantic_result: SemanticInferenceResult | None = None
        score_request: dict[str, Any] | None = None
        score_result: dict[str, Any] | None = None
        score_calls = 0

        by_key = {stage["stage_key"]: stage for stage in stages}
        for stage in stages:
            key = stage["stage_key"]
            condition = stage.get("condition")
            if condition is not None:
                lookup_key = condition["cache_operation_key"]
                _require(lookup_key in results, "cache condition result is missing")
                lookup = _unwrap(results[lookup_key])
                _require(
                    isinstance(lookup, CacheLookupResult),
                    "cache condition predecessor is not a lookup result",
                )
                consumed.add(lookup_key)
                if lookup.branch != condition["equals"]:
                    inactive.add(key)
                    stage_evidence.append({
                        "stage_key": key,
                        "stage_index": stage["stage_index"],
                        "action": stage["action"],
                        "condition": condition,
                        "state": "SKIPPED_INACTIVE_CONDITION",
                        "outcome_kind": None,
                        "outcome_sha256": None,
                        "service_time_ms": 0.0,
                        "bytes_read": 0,
                        "bytes_sent": 0,
                    })
                    continue

            dependency_keys = stage["dependency_stage_keys"]
            dependency_values: list[Any] = []
            if stage["action"] == "join-hit-or-miss-branch":
                active_dependencies = [
                    dependency
                    for dependency in dependency_keys
                    if dependency in results
                ]
                _require(
                    len(active_dependencies) == 1,
                    "cache join did not receive exactly one exclusive branch",
                )
                _require(
                    all(
                        dependency in results or dependency in inactive
                        for dependency in dependency_keys
                    ),
                    "cache join has an unresolved branch",
                )
                dependency_keys_used = active_dependencies
            else:
                _require(
                    all(dependency in results for dependency in dependency_keys),
                    f"active stage has a missing result: {key}",
                )
                dependency_keys_used = list(dependency_keys)
            for dependency in dependency_keys_used:
                dependency_values.append(results[dependency])
                consumed.add(dependency)

            action = stage["action"]
            identity = None
            representation = stage["object_representation_identity"].get(
                "representation_id"
            )
            if representation is not None:
                identity = identities[representation][1]

            if action == "admit-trial":
                _require(
                    stage.get("logical_node_ids") == ["N1"],
                    "trial admission is not on N1",
                )
                outcome = self._adapters.control.admit(
                    run_id=run_id,
                    trial=trial,
                    stage=stage,
                )
                _require(isinstance(outcome, ControlAdmission), "N1 admission result is invalid")
            elif action == "query-index":
                node = stage.get("logical_node_ids")
                _require(
                    node in [["N2"], [trial["executor_node_id"]]],
                    "index query is not on N2 or the selected executor",
                )
                outcome = self._adapters.index.query(
                    run_id=run_id,
                    trial=trial,
                    stage=stage,
                    public_task=public_task,
                    expected_object_id=trial["artifact_object_id"],
                )
                _require(isinstance(outcome, IndexSelection), "index result is invalid")
                _require(
                    outcome.selected_object_id == trial["artifact_object_id"],
                    "N2/local index selected a different artifact object",
                )
                if trial["route_family"] in {"indexed-raw", "indexed-derived"}:
                    _require(
                        stage.get("logical_node_ids") == ["N2"],
                        "indexed selection must come from N2",
                    )
                    _require(
                        outcome.segment is not None,
                        "indexed route requires an exact content-bound N2 range; "
                        "estimated fractions are not executable",
                    )
                    _require(
                        outcome.segment.matches(identities["raw_video"][1]),
                        "N2 range does not bind the frozen N3 raw artifact",
                    )
                    if trial["route_family"] == "indexed-derived":
                        _require(
                            isinstance(
                                outcome.segment, ExactTemporalFrameSelection
                            ),
                            "indexed-derived requires an N3 temporal projection",
                        )
            elif action in {"access-raw-artifact", "access-derived-artifact"}:
                _require(identity is not None, "artifact access lacks an identity")
                expected_node = "N3" if action == "access-raw-artifact" else "N4"
                _require(
                    stage.get("logical_node_ids") == [expected_node],
                    f"artifact access is not on {expected_node}",
                )
                selections = [
                    selection
                    for value in dependency_values
                    for selection in _find_values(value, IndexSelection)
                ]
                if (
                    action == "access-raw-artifact"
                    and trial["route_family"]
                    in {"indexed-raw", "indexed-derived"}
                ):
                    _require(
                        len(selections) == 1 and selections[0].segment is not None,
                        "N2 exact range was not handed to N3",
                    )
                    selection = selections[0]
                    segment = selection.segment
                    assert segment is not None
                    if isinstance(segment, ExactContentRange):
                        request = (
                            self._adapters.range_request_factory.build_request(
                                run_id=run_id,
                                trial=trial,
                                stage=stage,
                                identity=identity,
                                selection=selection,
                            )
                        )
                        raw = (
                            self._adapters.range_fetcher
                            .fetch_binary_artifact_range(
                                request,
                                range_start=segment.range_start,
                                range_end=segment.range_end,
                                expected_range_sha256=segment.range_sha256,
                                allowed_media_types=(
                                    self._adapters
                                    .raw_range_allowed_media_types
                                ),
                            )
                        )
                        outcome = _range_artifact(
                            raw,
                            expected=identity,
                            segment=segment,
                        )
                    else:
                        _require(
                            isinstance(segment, ExactTemporalFrameSelection),
                            "N2 returned an unsupported exact selection",
                        )
                        outcome = self._adapters.artifacts.fetch_selected(
                            run_id=run_id,
                            trial=trial,
                            stage=stage,
                            source_identity=identity,
                            selection=segment,
                        )
                        _validate_artifact(
                            outcome,
                            identity,
                            segment=segment,
                        )
                else:
                    outcome = self._adapters.artifacts.fetch_full(
                        run_id=run_id,
                        trial=trial,
                        stage=stage,
                        identity=identity,
                        upstream_values=dependency_values,
                    )
                    _require(isinstance(outcome, ArtifactAccess), "artifact result is invalid")
                    _validate_artifact(outcome, identity, segment=None)
            elif action == "transfer-bytes":
                value = _primary_value(dependency_values)
                before = _value_commitment(value)
                outcome = self._adapters.transport.transfer(
                    run_id=run_id,
                    trial=trial,
                    stage=stage,
                    value=value,
                )
                _require(isinstance(outcome, TransferResult), "transfer result is invalid")
                _require(
                    _value_commitment(outcome.value) == before,
                    "transport substituted its predecessor value",
                )
            elif action == "lookup":
                _require(identity is not None, "cache lookup lacks an identity")
                _require(
                    stage.get("logical_node_ids") == [trial["executor_node_id"]],
                    "cache lookup is not on the selected executor",
                )
                cache_episode_kwargs = (
                    {"cache_episode_id": cache_episode_id}
                    if cache_episode_id is not None else {}
                )
                outcome = self._adapters.cache.lookup(
                    run_id=run_id,
                    trial=trial,
                    stage=stage,
                    identity=identity,
                    **cache_episode_kwargs,
                )
                _require(isinstance(outcome, CacheLookupResult), "cache lookup result is invalid")
                _require(
                    outcome.node_id == trial["executor_node_id"],
                    "cache lookup came from the wrong executor",
                )
                if cache_episode_id is None:
                    repetition = _integer(trial.get("repetition"), "repetition")
                    expected_branch = "miss" if repetition == 0 else "hit"
                    _require(
                        outcome.branch == expected_branch,
                        "cache branch differs from the frozen repetition lifecycle",
                    )
                if outcome.branch == "hit" and cache_episode_id is None:
                    prefix, separator, suffix = trial["trial_key"].rpartition("|")
                    _require(
                        bool(separator)
                        and suffix == f"r{repetition:04d}"
                        and outcome.source_insert_trial_key
                        == f"{prefix}|r{repetition - 1:04d}",
                        "cache hit does not bind the immediately preceding "
                        "paired insertion trial",
                    )
                if outcome.branch == "hit" and cache_episode_id is not None:
                    _require(
                        outcome.source_insert_trial_key is not None
                        and outcome.source_insert_trial_key != trial["trial_key"],
                        "episode cache hit lacks a prior insertion trial",
                    )
                cache_epochs.add((outcome.node_id, outcome.cache_id, outcome.runtime_epoch))
                cache_evidence.append({
                    "lookup_stage_key": key,
                    "representation_id": identity.representation_id,
                    "branch": outcome.branch,
                    "cache_node_id": outcome.node_id,
                    "cache_id": outcome.cache_id,
                    "runtime_epoch": outcome.runtime_epoch,
                    "source_insert_trial_key": outcome.source_insert_trial_key,
                    "lookup_sha256": outcome.lookup_sha256,
                })
            elif action == "read":
                _require(identity is not None, "cache read lacks an identity")
                lookups = [
                    value for value in map(_unwrap, dependency_values)
                    if isinstance(value, CacheLookupResult)
                ]
                _require(
                    len(lookups) == 1 and lookups[0].branch == "hit",
                    "cache read did not follow one hit",
                )
                outcome = self._adapters.cache.read(
                    run_id=run_id,
                    trial=trial,
                    stage=stage,
                    identity=identity,
                    lookup=lookups[0],
                )
                _require(isinstance(outcome, ArtifactAccess), "cache read result is invalid")
                _validate_artifact(outcome, identity, segment=None)
            elif action == "insert":
                _require(identity is not None, "cache insert lacks an identity")
                lookups = [
                    value for value in map(_unwrap, dependency_values)
                    if isinstance(value, CacheLookupResult)
                ]
                artifacts = [
                    value for value in map(_unwrap, dependency_values)
                    if isinstance(value, ArtifactAccess)
                ]
                _require(
                    len(lookups) == 1
                    and lookups[0].branch == "miss"
                    and len(artifacts) == 1,
                    "cache insert did not follow one miss artifact",
                )
                cache_episode_kwargs = (
                    {"cache_episode_id": cache_episode_id}
                    if cache_episode_id is not None else {}
                )
                outcome = self._adapters.cache.insert(
                    run_id=run_id,
                    trial=trial,
                    stage=stage,
                    identity=identity,
                    lookup=lookups[0],
                    artifact=artifacts[0],
                    **cache_episode_kwargs,
                )
                _require(isinstance(outcome, CacheInsertResult), "cache insert result is invalid")
                _require(
                    outcome.node_id == lookups[0].node_id
                    and outcome.cache_id == lookups[0].cache_id
                    and outcome.runtime_epoch == lookups[0].runtime_epoch,
                    "cache runtime identity changed between lookup and insert",
                )
                _validate_artifact(outcome.artifact, identity, segment=None)
            elif action == "join-hit-or-miss-branch":
                _require(
                    stage.get("logical_node_ids") == [trial["executor_node_id"]],
                    "cache join is not on the selected executor",
                )
                artifact = _unwrap(dependency_values[0])
                _require(isinstance(artifact, ArtifactAccess), "cache branch did not yield an artifact")
                branch_conditions = [
                    by_key[dependency].get("condition")
                    for dependency in stage["dependency_stage_keys"]
                ]
                active_condition = by_key[dependency_keys_used[0]].get("condition")
                _require(
                    {value.get("equals") for value in branch_conditions if isinstance(value, dict)}
                    == {"hit", "miss"}
                    and isinstance(active_condition, dict),
                    "cache join does not preserve an exact hit/miss pair",
                )
                join_sha = _sha256(_canonical({
                    "stage_key": key,
                    "active_dependency": dependency_keys_used[0],
                    "branch": active_condition["equals"],
                    "artifact_payload_sha256": artifact.payload_sha256,
                }))
                outcome = BranchJoinResult(
                    artifact=artifact,
                    branch=active_condition["equals"],
                    join_sha256=join_sha,
                )
            elif action == "prepare-model-input":
                _require(
                    stage.get("logical_node_ids") == [trial["executor_node_id"]],
                    "model input preparation is not on the selected executor",
                )
                artifacts = [
                    artifact
                    for value in dependency_values
                    for artifact in _find_values(value, ArtifactAccess)
                ]
                mode = _input_mode(artifacts, str(trial["route_family"]))
                outcome = self._adapters.model_input.prepare(
                    run_id=run_id,
                    trial=trial,
                    stage=stage,
                    public_task=public_task,
                    mode=mode,
                    artifacts=artifacts,
                )
                _require(isinstance(outcome, PreparedSemanticInput), "prepared input is invalid")
                _validate_prepared(outcome, artifacts, mode)
                _require(prepared_input is None, "model input was prepared more than once")
                prepared_input = outcome
            elif action == "infer":
                _require(stage.get("logical_node_ids") == ["N6"], "inference is not on N6")
                prepared = [
                    value for value in map(_unwrap, dependency_values)
                    if isinstance(value, PreparedSemanticInput)
                ]
                if prepared:
                    _require(len(prepared) == 1, "inference has multiple prepared inputs")
                    model_input = prepared[0]
                else:
                    artifacts = [
                        artifact
                        for value in dependency_values
                        for artifact in _find_values(value, ArtifactAccess)
                    ]
                    mode = _input_mode(artifacts, str(trial["route_family"]))
                    model_input = self._adapters.model_input.prepare(
                        run_id=run_id,
                        trial=trial,
                        stage=stage,
                        public_task=public_task,
                        mode=mode,
                        artifacts=artifacts,
                    )
                    _require(isinstance(model_input, PreparedSemanticInput), "prepared input is invalid")
                    _validate_prepared(model_input, artifacts, mode)
                    _require(prepared_input is None, "model input was prepared more than once")
                    prepared_input = model_input
                outcome = self._adapters.semantic.infer(
                    run_id=run_id,
                    trial=trial,
                    stage=stage,
                    public_task=public_task,
                    model_input=model_input,
                )
                _require(isinstance(outcome, SemanticInferenceResult), "semantic result is invalid")
                _require(
                    outcome.input_sha256 == model_input.payload_sha256,
                    "N6 semantic result binds a different model input",
                )
                _require(semantic_result is None, "semantic inference ran more than once")
                semantic_result = outcome
            elif action == "score-hidden-answer":
                answers = [
                    value for value in map(_unwrap, dependency_values)
                    if isinstance(value, SemanticInferenceResult)
                ]
                _require(len(answers) == 1, "N1 scoring lacks one N6 answer")
                score_request_id = _sha256(_canonical({
                    "domain": "pathfinder.generic-semantic-n1-score/v1",
                    "run_id": run_id,
                    "trial_id": trial["trial_key"],
                    "task_binding_sha256": public_task["task_binding_sha256"],
                    "predicted_answer_sha256": _sha256(
                        answers[0].final_answer.encode("utf-8")
                    ),
                }))
                score_request = build_n1_score_request(
                    score_request_id=score_request_id,
                    oracle_id=self._oracle_id,
                    run_id=run_id,
                    trial_id=trial["trial_key"],
                    object_id=trial["artifact_object_id"],
                    task_binding_sha256=public_task["task_binding_sha256"],
                    predicted_answer=answers[0].final_answer,
                )
                _require(
                    score_request["schema_version"]
                    == N1_SCORE_REQUEST_SCHEMA_VERSION,
                    "N1 v1alpha2 score request was not constructed",
                )
                score_calls += 1
                authenticated = self._adapters.scorer.score_once_and_verify(
                    score_request
                )
                score_result = _validate_authenticated_score(
                    authenticated,
                    score_request,
                    oracle_id=self._oracle_id,
                    public_task_set_sha256=self._public_task_set_sha256,
                    success_scoring_rule=public_task[
                        "success_scoring_rule"
                    ],
                )
                outcome = authenticated
            else:
                raise SemanticRouteRuntimeError(
                    f"unsupported frozen stage action: {action}"
                )

            _telemetry(outcome)
            results[key] = outcome
            active.add(key)
            stage_evidence.append({
                "stage_key": key,
                "stage_index": stage["stage_index"],
                "action": action,
                "condition": condition,
                "state": "EXECUTED",
                **_stage_public_result(outcome),
            })

        _require(
            score_calls == 1
            and score_request is not None
            and score_result is not None,
            "N1 was not scored exactly once",
        )
        _require(prepared_input is not None, "semantic model input is missing")
        _require(semantic_result is not None, "semantic result is missing")
        terminal_key = stages[-1]["stage_key"]
        _require(
            set(results) == active
            and active | inactive == {stage["stage_key"] for stage in stages},
            "stage results are missing or unused",
        )
        unused = active - consumed - {terminal_key}
        _require(not unused, f"stage results were not consumed: {sorted(unused)}")
        if cache_epochs:
            _require(
                len(cache_epochs) == 1,
                "one trial observed multiple cache runtime identities",
            )

        component_ms: dict[str, float] = {}
        bytes_read = 0
        bytes_sent = 0
        for row in stage_evidence:
            if row["state"] != "EXECUTED":
                continue
            action = row["action"]
            component_ms[action] = component_ms.get(action, 0.0) + float(
                row["service_time_ms"]
            )
            bytes_read += int(row["bytes_read"])
            bytes_sent += int(row["bytes_sent"])

        provisioning_rows = [
            {
                "chain_id": value.chain_id,
                "logical_object_id": value.logical_object_id,
                "artifact_identity_sha256": value.artifact_identity.commitment,
                "n5_evidence_sha256": value.n5_evidence_sha256,
                "n4_publication_sha256": value.n4_publication_sha256,
                "available": True,
            }
            for value in provisioning
        ]
        observation = {
            "schema_version": NEUTRAL_OBSERVATION_CANDIDATE_SCHEMA_VERSION,
            "trial_key": trial["trial_key"],
            "order_index": trial["order_index"],
            "workload_id": trial["workload_id"],
            "workload_class": trial["workload_class"],
            "design_id": trial["design_id"],
            "repetition": trial["repetition"],
            "object_id": trial["artifact_object_id"],
            "route_family": trial["route_family"],
            "executor_node_id": trial["executor_node_id"],
            "task_success": score_result["correct"],
            "score": score_result["score"],
            "score_authenticity_verified": True,
            "score_authentication": "n1-hmac-verified",
            "component_service_time_ms": dict(sorted(component_ms.items())),
            "byte_measurements": {
                "adapter_bytes_read": bytes_read,
                "adapter_bytes_sent": bytes_sent,
                "semantic_input_bytes": len(prepared_input.payload),
            },
            "monetary_measurement_available": False,
            "monetary_values_included": False,
            "synthetic_monetary_inputs_consumed": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        input_evidence = _semantic_input_evidence(
            prepared_input,
            trial.get("semantic_input_profile"),
        )
        evidence: dict[str, Any] = {
            "schema_version": (
                SEMANTIC_ROUTE_EPISODE_EVIDENCE_SCHEMA_VERSION
                if cache_episode_id is not None
                else SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION
            ),
            "status": "COMPLETE",
            "execution_id": execution_id,
            "request_sha256": request_sha256,
            "run_id": run_id,
            "trial_id": trial["trial_key"],
            "trial_key": trial["trial_key"],
            "trial_sha256": _sha256(_canonical(trial)),
            "stage_dag_sha256": _sha256(_canonical(stages)),
            "workload_id": trial["workload_id"],
            "workload_class": trial["workload_class"],
            "design_id": trial["design_id"],
            "repetition": trial["repetition"],
            "artifact_object_id": trial["artifact_object_id"],
            "public_task_binding_sha256": public_task["task_binding_sha256"],
            "route": {
                "route_family": trial["route_family"],
                "executor_node_id": trial["executor_node_id"],
                "inference_node_id": "N6",
                "score_node_id": "N1",
            },
            "artifact_identities": [
                {
                    "logical_object_id": logical,
                    **identity.to_dict(),
                    "identity_sha256": identity.commitment,
                }
                for logical, identity in sorted(
                    identities.values(),
                    key=lambda value: value[1].representation_id,
                )
            ],
            "provisioning_references": provisioning_rows,
            "stage_results": stage_evidence,
            "cache_branches": cache_evidence,
            "model_input": {
                "mode": prepared_input.mode,
                "payload_sha256": prepared_input.payload_sha256,
                "payload_size_bytes": len(prepared_input.payload),
                "component_identity_sha256": [
                    value.commitment
                    for value in prepared_input.component_identities
                ],
                "preparation_sha256": prepared_input.preparation_sha256,
                **input_evidence,
            },
            "semantic": {
                "model": semantic_result.model,
                "input_sha256": semantic_result.input_sha256,
                "request_sha256": semantic_result.request_sha256,
                "result_sha256": semantic_result.result_sha256,
                "final_answer_sha256": _sha256(
                    semantic_result.final_answer.encode("utf-8")
                ),
                "service_time_ms": semantic_result.telemetry.service_time_ms,
            },
            # These are public N1 documents. The request retains the model's
            # prediction, never the hidden answer, and the result asserts
            # hidden_answer_returned=false. A privileged offline consumer can
            # therefore replay the N1 HMAC instead of trusting task_success.
            "n1_score_request": _copy_json(score_request),
            "n1_score_result": _copy_json(score_result),
            "scoring": {
                "oracle_id": score_result["oracle_id"],
                "score_request_id": score_result["score_request_id"],
                "evaluation_unit_id": score_result["evaluation_unit_id"],
                "task_binding_sha256": score_result["task_binding_sha256"],
                "task_success": score_result["correct"],
                "score": score_result["score"],
                "score_evidence_hmac_sha256": score_result[
                    "score_evidence_hmac_sha256"
                ],
                "result_content_sha256": score_result[
                    "result_content_sha256"
                ],
                "authentication_verification_sha256": results[terminal_key].verification_sha256,
                "authenticated_n1_v1alpha2": True,
            },
            "neutral_observation_candidate": observation,
            "all_frozen_stages_accounted_for": True,
            "exclusive_cache_branches_verified": True,
            "n2_exact_range_required_for_indexed_raw": True,
            "n3_n4_artifact_identity_verified": True,
            "n5_provisioning_references_verified": True,
            "n6_input_mode_verified": True,
            "semantic_input_profile_verified": input_evidence[
                "semantic_input_profile_verified"
            ],
            "n1_exactly_once_authenticated_score_verified": True,
            "idempotent_replay": False,
            "endpoint_values_included": False,
            "credential_values_included": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        if cache_episode_id is not None:
            evidence["cache_episode_id"] = cache_episode_id
        _assert_credential_free(evidence)
        evidence["evidence_sha256"] = _sha256(_canonical(evidence))
        return verify_public_semantic_route_evidence(evidence)


__all__ = [
    "AdapterTelemetry",
    "ArtifactAccess",
    "ArtifactCacheAdapter",
    "ArtifactIdentity",
    "ArtifactSourceAdapter",
    "AuthenticatedN1Score",
    "AuthenticatedN1ScoringAdapter",
    "ByteTransferAdapter",
    "BranchJoinResult",
    "CacheInsertResult",
    "CacheLookupResult",
    "ControlAdmission",
    "EXACT_CONTENT_RANGE_SCHEMA_VERSION",
    "EXACT_TEMPORAL_FRAME_SELECTION_SCHEMA_VERSION",
    "ExactContentRange",
    "ExactSourceSelection",
    "ExactTemporalFrameSelection",
    "GenericSemanticRouteCoordinator",
    "InMemoryRouteExecutionStore",
    "IndexQueryAdapter",
    "IndexSelection",
    "ModelInputAdapter",
    "N3RangeRequestFactory",
    "NEUTRAL_OBSERVATION_CANDIDATE_SCHEMA_VERSION",
    "PreparedSemanticInput",
    "ProvisioningReference",
    "ProvisioningReferenceAdapter",
    "RangeFetcher",
    "RouteExecutionStore",
    "SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION",
    "SemanticInferenceAdapter",
    "SemanticInferenceResult",
    "SemanticRouteAdapters",
    "SemanticRouteRuntimeError",
    "TransferResult",
    "TrialControlAdapter",
]
