"""Content-bound N6 model-input and semantic inference adapters.

The generic full-flow route coordinator hands this module opaque artifact
bytes plus their frozen identities.  Preparation fails closed unless those
bytes still match the identity (or the exact N2 byte-range descriptor), then
builds one canonical request accepted by :mod:`pathfinder.simulator.container_node`.

Raw video decoding is intentionally outside this module.  A caller must
inject a bounded sampler which consumes the exact routed bytes and returns
ordered JPEG frames.  Tests can inject a deterministic sampler; production
can inject a real decoder without weakening the content-binding checks here.
No endpoint, credential, hidden answer, or label is accepted or retained.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol, Sequence

from ..frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    FRAME_BUNDLE_MEDIA_TYPE,
    FrameBundleLimits,
    validate_frame_bundle_bytes,
)
from .container_node import (
    CONTAINER_NODE_API_VERSION,
    CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_FUSION_RESULT_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION,
    ContainerNodeRuntime,
    semantic_frame_sequence_sha256,
    semantic_fusion_representation_sha256,
)
from .full_flow_semantic_route_runtime import (
    AdapterTelemetry,
    ArtifactAccess,
    ArtifactIdentity,
    ExactContentRange,
    ExactTemporalFrameSelection,
    PreparedSemanticInput,
    SemanticInferenceResult,
)
from .hidden_oracle import build_n1_public_task_binding
from .full_flow_semantic_input_profiles import (
    SemanticInputProfileError,
    validate_semantic_input_profile,
)


RAW_PREPARED_REPRESENTATION_DOMAIN = (
    b"pathfinder.raw-video-prepared-frame-representation/v1\x00"
)
SEMANTIC_REQUEST_ID_DOMAIN = "pathfinder.n6-semantic-request-id/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_INPUT_MODES = {
    "raw-prepared-frames",
    "digest",
    "frame-bundle",
    "digest+frames-fusion",
}
_PRIVATE_KEYS = {
    "api_key",
    "authorization",
    "bearer_token",
    "correct_answer_id",
    "credential_value",
    "hidden_answer",
    "hidden_answers",
    "hidden_label",
    "hidden_labels",
    "labels",
    "password",
    "secret",
    "token",
}


class N6AdapterError(RuntimeError):
    """Raised before unbound input or output can enter route evidence."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N6AdapterError(message)


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
        raise N6AdapterError("value is not canonical JSON") from exc


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


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


def _text(value: Any, name: str, *, max_bytes: int) -> str:
    _require(isinstance(value, str) and bool(value), f"{name} must be text")
    _require(len(value.encode("utf-8")) <= max_bytes, f"{name} is too large")
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
        and float(value) >= 0.0,
        f"{name} must be a finite non-negative number",
    )
    return float(value)


def _assert_no_private_fields(value: Any, path: str = "public_task") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).casefold().replace("-", "_")
            _require(
                key not in _PRIVATE_KEYS,
                f"private field entered N6 input at {path}.{raw_key}",
            )
            _assert_no_private_fields(child, f"{path}.{raw_key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _assert_no_private_fields(child, f"{path}[{index}]")


@dataclass(frozen=True)
class N6SampledFrame:
    """One real-decoder result returned by the injected raw-video sampler."""

    frame_index: int
    timestamp_seconds: float
    width: int
    height: int
    jpeg_bytes: bytes


class RawVideoFrameSampler(Protocol):
    """Injected decoding boundary; implementations must decode supplied bytes."""

    def __call__(
        self,
        payload: bytes,
        *,
        object_id: str,
        source_payload_sha256: str,
        frame_count: int,
        jpeg_max_dimension: int,
        temporal_start_fraction: float,
        temporal_end_fraction: float,
    ) -> Sequence[N6SampledFrame]: ...


class SemanticRequestExecutor(Protocol):
    """In-process or HTTP adapter for one already-bound N6 request."""

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class N6PreparationLimits:
    max_raw_video_bytes: int = 1024 * 1024 * 1024
    max_digest_bytes: int = 256 * 1024
    max_question_bytes: int = 64 * 1024
    raw_frame_count: int = 16
    jpeg_max_dimension: int = 768
    max_frame_count: int = 32
    max_frame_bytes: int = 512 * 1024
    max_total_frame_bytes: int = 2 * 1024 * 1024
    frame_bundle_limits: FrameBundleLimits = field(
        default_factory=lambda: DEFAULT_FRAME_BUNDLE_LIMITS
    )

    def __post_init__(self) -> None:
        for name in (
            "max_raw_video_bytes",
            "max_digest_bytes",
            "max_question_bytes",
            "raw_frame_count",
            "jpeg_max_dimension",
            "max_frame_count",
            "max_frame_bytes",
            "max_total_frame_bytes",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.raw_frame_count > self.max_frame_count:
            raise ValueError("raw_frame_count exceeds max_frame_count")


def _validate_artifact(access: ArtifactAccess) -> None:
    _require(isinstance(access, ArtifactAccess), "artifact input is invalid")
    identity = access.source_identity
    _require(
        isinstance(identity, ArtifactIdentity),
        "artifact source identity is invalid",
    )
    _require(isinstance(access.payload, bytes), "artifact payload must be bytes")
    if access.segment is None:
        _require(
            len(access.payload) == identity.artifact_size_bytes,
            "artifact payload size differs from its frozen identity",
        )
        _require(
            _sha256(access.payload) == identity.artifact_sha256,
            "artifact payload digest differs from its frozen identity",
        )
        return
    segment = access.segment
    _require(
        segment.matches(identity),
        "exact selection descriptor differs from its source identity",
    )
    if isinstance(segment, ExactContentRange):
        _require(
            len(access.payload) == segment.range_size_bytes,
            "range payload size differs from its exact descriptor",
        )
        _require(
            _sha256(access.payload) == segment.range_sha256,
            "range payload digest differs from its exact descriptor",
        )
        return
    _require(
        isinstance(segment, ExactTemporalFrameSelection)
        and len(access.payload) == segment.selected_artifact_size_bytes
        and _sha256(access.payload) == segment.selected_artifact_sha256,
        "temporal projection payload differs from its exact descriptor",
    )


def _render_public_question(public_task: Mapping[str, Any], limit: int) -> str:
    _require(isinstance(public_task, Mapping), "public task must be an object")
    _assert_no_private_fields(public_task)
    try:
        rebuilt = build_n1_public_task_binding(
            workload_id=public_task.get("workload_id"),
            object_id=public_task.get("object_id"),
            task_class_id=public_task.get("task_class_id"),
            question=public_task.get("question"),
            answer_options=public_task.get("answer_options"),
            success_scoring_rule=public_task.get("success_scoring_rule"),
        )
    except Exception as exc:
        raise N6AdapterError("public task binding is invalid") from exc
    _require(
        _canonical(rebuilt) == _canonical(public_task),
        "public task binding is not canonical or content-bound",
    )
    question = _text(
        public_task.get("question"),
        "public task question",
        max_bytes=limit,
    )
    options = public_task.get("answer_options")
    _require(isinstance(options, list), "public task answer_options must be an array")
    _require(2 <= len(options) <= 32, "public task must carry 2 to 32 options")
    rendered: list[str] = []
    seen: set[str] = set()
    for position, raw in enumerate(options):
        _require(isinstance(raw, Mapping), f"answer_options[{position}] is invalid")
        _require(
            set(raw) == {"option_id", "text"},
            f"answer_options[{position}] fields changed",
        )
        option_id = _identifier(raw.get("option_id"), "option_id")
        _require(option_id not in seen, "public task repeats an option_id")
        seen.add(option_id)
        option_text = _text(
            raw.get("text"),
            "option text",
            max_bytes=limit,
        )
        rendered.append(f"[{option_id}] {option_text}")
    value = (
        f"{question}\n\nOptions:\n"
        + "\n".join(rendered)
        + "\n\nReturn exactly one option ID and no other text."
    )
    return _text(value, "rendered public question", max_bytes=limit)


def _wire_frames(frames: Sequence[N6SampledFrame]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for frame in frames:
        _require(isinstance(frame, N6SampledFrame), "sampler frame type changed")
        payload = frame.jpeg_bytes
        _require(isinstance(payload, bytes), "sampler JPEG payload is not bytes")
        values.append({
            "frame_index": frame.frame_index,
            "timestamp_seconds": frame.timestamp_seconds,
            "width": frame.width,
            "height": frame.height,
            "jpeg_size_bytes": len(payload),
            "jpeg_sha256": _sha256(payload),
            "jpeg_base64": base64.b64encode(payload).decode("ascii"),
        })
    return values


def _validate_frame_limits(
    frames: Sequence[N6SampledFrame],
    limits: N6PreparationLimits,
) -> None:
    _require(bool(frames), "prepared frame sequence is empty")
    _require(
        len(frames) <= limits.max_frame_count,
        "prepared frame count exceeds its N6 bound",
    )
    total = 0
    for position, frame in enumerate(frames):
        _require(
            frame.frame_index == position,
            "prepared frames are not zero-based and contiguous",
        )
        _number(frame.timestamp_seconds, "frame timestamp")
        _integer(frame.width, "frame width", minimum=1)
        _integer(frame.height, "frame height", minimum=1)
        _require(
            frame.width <= limits.jpeg_max_dimension
            and frame.height <= limits.jpeg_max_dimension,
            "prepared frame dimensions exceed the configured bound",
        )
        _require(
            0 < len(frame.jpeg_bytes) <= limits.max_frame_bytes,
            "prepared JPEG exceeds its per-frame byte bound",
        )
        total += len(frame.jpeg_bytes)
    _require(
        total <= limits.max_total_frame_bytes,
        "prepared JPEG sequence exceeds its total byte bound",
    )


def _sparse_uniform_frames(
    frames: Sequence[N6SampledFrame],
    frame_count: int,
) -> tuple[N6SampledFrame, ...]:
    """Select deterministic midpoint positions while preserving timestamps."""

    _require(
        type(frame_count) is int and 0 < frame_count <= len(frames),
        "semantic input profile requests an invalid frame count",
    )
    if frame_count == len(frames):
        selected = tuple(frames)
    else:
        indexes = [
            ((2 * index + 1) * len(frames)) // (2 * frame_count)
            for index in range(frame_count)
        ]
        _require(
            len(indexes) == len(set(indexes)),
            "semantic input profile produced duplicate frame positions",
        )
        selected = tuple(frames[index] for index in indexes)
    return tuple(
        N6SampledFrame(
            frame_index=index,
            timestamp_seconds=value.timestamp_seconds,
            width=value.width,
            height=value.height,
            jpeg_bytes=value.jpeg_bytes,
        )
        for index, value in enumerate(selected)
    )


def _frozen_profile(
    trial: Mapping[str, Any],
    mode: str,
    identities: Sequence[ArtifactIdentity],
) -> Mapping[str, Any] | None:
    """Validate a new frozen profile while retaining legacy package support."""

    supplied = trial.get("semantic_input_profile")
    if supplied is None:
        return None
    representations = [value.representation_id for value in identities]
    try:
        expected = validate_semantic_input_profile(
            supplied,
            route_family=str(trial.get("route_family")),
            model_input_representation_ids=representations,
        )
    except SemanticInputProfileError as exc:
        raise N6AdapterError(str(exc)) from exc
    _require(
        expected["input_mode"] == mode,
        "semantic input profile mode differs from routed artifacts",
    )
    return expected


def _frame_selection(
    profile: Mapping[str, Any] | None,
    *,
    legacy_count: int,
) -> tuple[int, float, float]:
    if profile is None:
        return legacy_count, 0.0, 1.0
    selection = profile.get("frame_selection")
    _require(isinstance(selection, Mapping), "frame profile selection is missing")
    count = selection.get("frame_count")
    window = selection.get("temporal_window_fraction")
    _require(
        type(count) is int
        and isinstance(window, list)
        and len(window) == 2
        and all(isinstance(value, (int, float)) for value in window),
        "frame profile selection is invalid",
    )
    return int(count), float(window[0]), float(window[1])


def raw_prepared_representation_sha256(
    identity: ArtifactIdentity,
    access: ArtifactAccess,
    frame_sequence_sha256: str,
) -> str:
    """Bind source identity, exact routed bytes, and decoded frame sequence."""

    _validate_artifact(access)
    _require(
        identity == access.source_identity,
        "raw prepared identity differs from the routed artifact",
    )
    sequence = _digest(frame_sequence_sha256, "frame_sequence_sha256")
    core = {
        "source_identity_sha256": identity.commitment,
        "source_payload_sha256": access.payload_sha256,
        "source_payload_size_bytes": len(access.payload),
        "exact_range_descriptor_sha256": (
            None if access.segment is None else access.segment.descriptor_sha256
        ),
        "frame_sequence_sha256": sequence,
    }
    value = hashlib.sha256()
    value.update(RAW_PREPARED_REPRESENTATION_DOMAIN)
    value.update(_canonical(core))
    return value.hexdigest()


def _request_id(
    *,
    run_id: str,
    trial: Mapping[str, Any],
    request_binding_stage_key: str,
    public_task: Mapping[str, Any],
    mode: str,
    identities: Sequence[ArtifactIdentity],
    request_without_id: Mapping[str, Any],
) -> str:
    core = {
        "domain": SEMANTIC_REQUEST_ID_DOMAIN,
        "run_id": _text(run_id, "run_id", max_bytes=512),
        "trial_key": _text(
            trial.get("trial_key"), "trial_key", max_bytes=2048
        ),
        "stage_key": _text(
            request_binding_stage_key, "stage_key", max_bytes=2048
        ),
        "task_binding_sha256": _digest(
            public_task.get("task_binding_sha256"),
            "task_binding_sha256",
        ),
        "mode": mode,
        "component_identity_sha256": [value.commitment for value in identities],
        "request_without_id_sha256": _sha256(_canonical(request_without_id)),
    }
    return _sha256(_canonical(core))


def _prepared(
    mode: str,
    request: Mapping[str, Any],
    identities: tuple[ArtifactIdentity, ...],
    *,
    bytes_read: int,
    service_time_ms: float,
    request_binding_stage_key: str,
) -> PreparedSemanticInput:
    payload = _canonical(request)
    preparation = _sha256(_canonical({
        "mode": mode,
        "payload_sha256": _sha256(payload),
        "payload_size_bytes": len(payload),
        "component_identity_sha256": [value.commitment for value in identities],
        "request_binding_stage_key": request_binding_stage_key,
    }))
    return PreparedSemanticInput(
        mode=mode,
        payload=payload,
        component_identities=identities,
        preparation_sha256=preparation,
        request_binding_stage_key=request_binding_stage_key,
        telemetry=AdapterTelemetry(
            service_time_ms=service_time_ms,
            bytes_read=bytes_read,
        ),
    )


class N6ModelInputAdapter:
    """Prepare canonical container-node v1/v2/v3 requests from routed bytes."""

    def __init__(
        self,
        *,
        raw_sampler: RawVideoFrameSampler,
        limits: N6PreparationLimits = N6PreparationLimits(),
        clock_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        self._raw_sampler = raw_sampler
        self._limits = limits
        self._clock_ns = clock_ns

    def prepare(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        mode: str,
        artifacts: Sequence[ArtifactAccess],
    ) -> PreparedSemanticInput:
        _require(mode in _INPUT_MODES, "N6 input mode is unsupported")
        _require(bool(artifacts), "N6 received no routed artifact")
        _require(
            all(isinstance(value, ArtifactAccess) for value in artifacts),
            "N6 received an invalid routed artifact",
        )
        started = self._clock_ns()
        for access in artifacts:
            _validate_artifact(access)
        identities = tuple(value.source_identity for value in artifacts)
        _require(
            len({value.representation_id for value in identities})
            == len(identities),
            "N6 received duplicate representation identities",
        )
        _require(
            len({value.object_id for value in identities}) == 1,
            "N6 fusion components name different objects",
        )
        artifact_object_id = identities[0].object_id
        _require(
            public_task.get("object_id") == artifact_object_id,
            "N6 public task and artifact name different objects",
        )
        declared_trial_object = trial.get("artifact_object_id")
        _require(
            declared_trial_object in (None, artifact_object_id),
            "N6 trial and artifact name different objects",
        )
        question = _render_public_question(
            public_task, self._limits.max_question_bytes
        )
        profile = _frozen_profile(trial, mode, identities)
        # Captured once here so the request ID and the value stored on the
        # prepared input are derived from exactly the same stage key.
        binding_stage_key = _text(
            stage.get("stage_key"), "stage_key", max_bytes=2048
        )

        if mode == "digest":
            request = self._digest_request(
                run_id,
                trial,
                stage,
                public_task,
                question,
                artifacts,
                identities,
            )
        elif mode == "raw-prepared-frames":
            request = self._raw_request(
                run_id,
                trial,
                stage,
                public_task,
                question,
                artifacts,
                identities,
                profile,
            )
        elif mode == "frame-bundle":
            request = self._bundle_request(
                run_id,
                trial,
                stage,
                public_task,
                question,
                artifacts,
                identities,
                profile,
            )
        else:
            request = self._fusion_request(
                run_id,
                trial,
                stage,
                public_task,
                question,
                artifacts,
                identities,
                profile,
            )
        finished = self._clock_ns()
        _require(finished >= started, "N6 preparation clock moved backwards")
        return _prepared(
            mode,
            request,
            identities,
            bytes_read=sum(len(value.payload) for value in artifacts),
            service_time_ms=(finished - started) / 1_000_000.0,
            request_binding_stage_key=binding_stage_key,
        )

    def _digest_request(
        self,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        question: str,
        artifacts: Sequence[ArtifactAccess],
        identities: tuple[ArtifactIdentity, ...],
    ) -> dict[str, Any]:
        _require(
            len(artifacts) == 1
            and identities[0].representation_id == "multimodal_digest"
            and artifacts[0].segment is None,
            "digest mode requires one complete multimodal_digest artifact",
        )
        payload = artifacts[0].payload
        _require(
            0 < len(payload) <= self._limits.max_digest_bytes,
            "digest payload exceeds its N6 byte bound",
        )
        try:
            digest_text = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise N6AdapterError("digest payload is not valid UTF-8") from exc
        _require(bool(digest_text), "digest payload is empty")
        prompt = ContainerNodeRuntime.build_semantic_prompt(
            "multimodal_digest", digest_text, question
        )
        prompt_sha = _sha256(prompt.encode("utf-8"))
        without_id = {
            "schema_version": CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
            "execution_node_id": "N6",
            "prompt": prompt,
            "prompt_sha256": prompt_sha,
            "representation_sha256": identities[0].artifact_sha256,
        }
        request_id = _request_id(
            run_id=run_id,
            trial=trial,
            request_binding_stage_key=_text(
                stage.get("stage_key"), "stage_key", max_bytes=2048
            ),
            public_task=public_task,
            mode="digest",
            identities=identities,
            request_without_id=without_id,
        )
        return {**without_id, "semantic_request_id": request_id}

    def _raw_request(
        self,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        question: str,
        artifacts: Sequence[ArtifactAccess],
        identities: tuple[ArtifactIdentity, ...],
        profile: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        _require(
            len(artifacts) == 1
            and identities[0].representation_id == "raw_video",
            "raw mode requires one raw_video artifact",
        )
        access = artifacts[0]
        _require(
            len(access.payload) <= self._limits.max_raw_video_bytes,
            "raw video payload exceeds its N6 byte bound",
        )
        frame_count, window_start, window_end = _frame_selection(
            profile,
            legacy_count=self._limits.raw_frame_count,
        )
        if (
            profile is not None
            and profile.get("source_byte_range_kind")
            == "source-decoded-temporal-frame-bundle"
        ):
            _require(
                isinstance(access.segment, ExactTemporalFrameSelection),
                "indexed semantic profile requires a real N3 projection",
            )
        if isinstance(access.segment, ExactTemporalFrameSelection):
            selection = access.segment
            _require(
                frame_count == selection.frame_count
                and window_start == selection.temporal_start_fraction
                and window_end == selection.temporal_end_fraction,
                "semantic profile differs from the N3 temporal selection",
            )
            frames, sequence_sha = self._validated_temporal_frames(
                access,
                selection,
            )
        else:
            sampled = tuple(self._raw_sampler(
                access.payload,
                object_id=identities[0].object_id,
                source_payload_sha256=access.payload_sha256,
                frame_count=frame_count,
                jpeg_max_dimension=self._limits.jpeg_max_dimension,
                temporal_start_fraction=window_start,
                temporal_end_fraction=window_end,
            ))
            _require(
                len(sampled) == frame_count,
                "raw sampler returned a different frame count",
            )
            _validate_frame_limits(sampled, self._limits)
            frames = _wire_frames(sampled)
            sequence_sha = _digest(
                semantic_frame_sequence_sha256(frames),
                "frame_sequence_sha256",
            )
        representation_sha = raw_prepared_representation_sha256(
            identities[0], access, sequence_sha
        )
        return self._vision_request(
            run_id=run_id,
            trial=trial,
            stage=stage,
            public_task=public_task,
            mode="raw-prepared-frames",
            identities=identities,
            representation_id="raw_video_prepared_frames",
            representation_sha256=representation_sha,
            question=question,
            frames=frames,
            frame_sequence_sha256=sequence_sha,
        )

    def _validated_temporal_frames(
        self,
        access: ArtifactAccess,
        selection: ExactTemporalFrameSelection,
    ) -> tuple[list[dict[str, Any]], str]:
        """Consume the exact N3 projection; never decode the MP4 again."""

        bundle = validate_frame_bundle_bytes(
            access.payload,
            expected_object_id=access.source_identity.object_id,
            expected_sha256=selection.selected_artifact_sha256,
            expected_size_bytes=selection.selected_artifact_size_bytes,
            artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
            limits=self._limits.frame_bundle_limits,
        )
        _require(
            bundle.source.source_video_sha256
            == access.source_identity.artifact_sha256
            and bundle.source.source_video_size_bytes
            == access.source_identity.artifact_size_bytes
            and bundle.source.declared_frame_count == selection.frame_count
            and bundle.source.sampling_method
            == "uniform-midpoint-temporal-window",
            "N3 temporal bundle does not bind its raw source and policy",
        )
        sampled = tuple(
            N6SampledFrame(
                frame_index=value.frame_index,
                timestamp_seconds=value.timestamp_seconds,
                width=value.width,
                height=value.height,
                jpeg_bytes=value.jpeg_bytes,
            )
            for value in bundle.vision_frames()
        )
        _require(
            len(sampled) == selection.frame_count,
            "N3 temporal projection has the wrong frame count",
        )
        lower = (
            bundle.source.source_duration_seconds
            * selection.temporal_start_fraction
        )
        upper = (
            bundle.source.source_duration_seconds
            * selection.temporal_end_fraction
        )
        _require(
            all(lower <= value.timestamp_seconds <= upper for value in sampled),
            "N3 temporal projection contains a frame outside its window",
        )
        _validate_frame_limits(sampled, self._limits)
        frames = _wire_frames(sampled)
        return frames, _digest(
            semantic_frame_sequence_sha256(frames),
            "frame_sequence_sha256",
        )

    def _validated_bundle_frames(
        self,
        access: ArtifactAccess,
        *,
        frame_count: int | None = None,
    ) -> tuple[list[dict[str, Any]], str]:
        _require(
            access.source_identity.representation_id == "sampled_frame_bundle"
            and access.segment is None,
            "frame-bundle mode requires a complete sampled_frame_bundle",
        )
        bundle = validate_frame_bundle_bytes(
            access.payload,
            expected_object_id=access.source_identity.object_id,
            expected_sha256=access.source_identity.artifact_sha256,
            expected_size_bytes=access.source_identity.artifact_size_bytes,
            artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
            limits=self._limits.frame_bundle_limits,
        )
        sampled = tuple(
            N6SampledFrame(
                frame_index=value.frame_index,
                timestamp_seconds=value.timestamp_seconds,
                width=value.width,
                height=value.height,
                jpeg_bytes=value.jpeg_bytes,
            )
            for value in bundle.vision_frames()
        )
        if frame_count is not None:
            sampled = _sparse_uniform_frames(sampled, frame_count)
        _validate_frame_limits(sampled, self._limits)
        frames = _wire_frames(sampled)
        sequence_sha = _digest(
            semantic_frame_sequence_sha256(frames),
            "frame_sequence_sha256",
        )
        return frames, sequence_sha

    def _vision_request(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        mode: str,
        identities: tuple[ArtifactIdentity, ...],
        representation_id: str,
        representation_sha256: str,
        question: str,
        frames: list[dict[str, Any]],
        frame_sequence_sha256: str,
    ) -> dict[str, Any]:
        prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
            representation_id, len(frames), question
        )
        without_id = {
            "schema_version": (
                CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION
            ),
            "execution_node_id": "N6",
            "representation_id": representation_id,
            "representation_sha256": _digest(
                representation_sha256, "representation_sha256"
            ),
            "question": question,
            "prompt_sha256": _sha256(prompt.encode("utf-8")),
            "frame_sequence_sha256": frame_sequence_sha256,
            "frames": frames,
        }
        request_id = _request_id(
            run_id=run_id,
            trial=trial,
            request_binding_stage_key=_text(
                stage.get("stage_key"), "stage_key", max_bytes=2048
            ),
            public_task=public_task,
            mode=mode,
            identities=identities,
            request_without_id=without_id,
        )
        return {**without_id, "semantic_request_id": request_id}

    def _bundle_request(
        self,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        question: str,
        artifacts: Sequence[ArtifactAccess],
        identities: tuple[ArtifactIdentity, ...],
        profile: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        _require(len(artifacts) == 1, "frame-bundle mode requires one artifact")
        frame_count = None
        if profile is not None:
            frame_count, _start, _end = _frame_selection(
                profile,
                legacy_count=self._limits.raw_frame_count,
            )
        frames, sequence_sha = self._validated_bundle_frames(
            artifacts[0],
            frame_count=frame_count,
        )
        return self._vision_request(
            run_id=run_id,
            trial=trial,
            stage=stage,
            public_task=public_task,
            mode="frame-bundle",
            identities=identities,
            representation_id="sampled_frame_bundle",
            representation_sha256=identities[0].artifact_sha256,
            question=question,
            frames=frames,
            frame_sequence_sha256=sequence_sha,
        )

    def _fusion_request(
        self,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        question: str,
        artifacts: Sequence[ArtifactAccess],
        identities: tuple[ArtifactIdentity, ...],
        profile: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        by_representation = {
            value.source_identity.representation_id: value for value in artifacts
        }
        _require(
            set(by_representation)
            == {"multimodal_digest", "sampled_frame_bundle"},
            "fusion requires one digest and one frame bundle",
        )
        digest_access = by_representation["multimodal_digest"]
        _require(digest_access.segment is None, "fusion digest cannot be ranged")
        _require(
            0 < len(digest_access.payload) <= self._limits.max_digest_bytes,
            "fusion digest exceeds its N6 byte bound",
        )
        try:
            digest_text = digest_access.payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise N6AdapterError("fusion digest is not valid UTF-8") from exc
        _require(bool(digest_text), "fusion digest is empty")
        digest_sha = digest_access.payload_sha256
        frame_count = None
        if profile is not None:
            frame_count, _start, _end = _frame_selection(
                profile,
                legacy_count=self._limits.raw_frame_count,
            )
        frames, sequence_sha = self._validated_bundle_frames(
            by_representation["sampled_frame_bundle"],
            frame_count=frame_count,
        )
        representation_id = "multimodal_digest+sampled_frame_bundle"
        representation_sha = semantic_fusion_representation_sha256(
            digest_sha, sequence_sha
        )
        prompt = ContainerNodeRuntime.build_semantic_fusion_prompt(
            representation_id, digest_text, len(frames), question
        )
        without_id = {
            "schema_version": (
                CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION
            ),
            "execution_node_id": "N6",
            "representation_id": representation_id,
            "representation_sha256": representation_sha,
            "digest_text": digest_text,
            "digest_sha256": digest_sha,
            "question": question,
            "prompt_sha256": _sha256(prompt.encode("utf-8")),
            "frame_sequence_sha256": sequence_sha,
            "frames": frames,
        }
        request_id = _request_id(
            run_id=run_id,
            trial=trial,
            request_binding_stage_key=_text(
                stage.get("stage_key"), "stage_key", max_bytes=2048
            ),
            public_task=public_task,
            mode="digest+frames-fusion",
            identities=identities,
            request_without_id=without_id,
        )
        return {**without_id, "semantic_request_id": request_id}


def decode_prepared_semantic_request(
    model_input: PreparedSemanticInput,
) -> dict[str, Any]:
    """Decode a canonical prepared request and recheck its outer commitment."""

    _require(
        isinstance(model_input, PreparedSemanticInput),
        "model input has the wrong type",
    )
    try:
        value = json.loads(model_input.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise N6AdapterError("prepared model input is not UTF-8 JSON") from exc
    _require(isinstance(value, dict), "prepared model input must be an object")
    _require(
        _canonical(value) == model_input.payload,
        "prepared model input is not canonical JSON",
    )
    expected = _sha256(_canonical({
        "mode": model_input.mode,
        "payload_sha256": model_input.payload_sha256,
        "payload_size_bytes": len(model_input.payload),
        "component_identity_sha256": [
            value.commitment for value in model_input.component_identities
        ],
        "request_binding_stage_key": model_input.request_binding_stage_key,
    }))
    _require(
        model_input.preparation_sha256 == expected,
        "prepared model input commitment changed",
    )
    return value


def _validate_prepared_request_binding(
    request: Mapping[str, Any],
    model_input: PreparedSemanticInput,
    *,
    run_id: str,
    trial: Mapping[str, Any],
    stage: Mapping[str, Any],
    public_task: Mapping[str, Any],
) -> None:
    """Re-derive the wire request before an injected executor sees it."""

    mode = model_input.mode
    identities = model_input.component_identities
    question = _render_public_question(public_task, 64 * 1024)
    schema_by_mode = {
        "digest": CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
        "raw-prepared-frames": (
            CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION
        ),
        "frame-bundle": CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
        "digest+frames-fusion": (
            CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION
        ),
    }
    _require(
        request.get("schema_version") == schema_by_mode.get(mode),
        "prepared request schema differs from its input mode",
    )
    _require(
        request.get("execution_node_id") == "N6",
        "prepared request is assigned to a different node",
    )
    request_id = _digest(
        request.get("semantic_request_id"), "semantic_request_id"
    )
    without_id = dict(request)
    del without_id["semantic_request_id"]
    # The ID was derived during preparation, so it must be revalidated
    # against the stage key recorded at that time. ``stage`` remains the
    # currently executing (infer) stage and is deliberately not substituted
    # here; using it would break every legitimate two-stage route.
    _require(
        request_id
        == _request_id(
            run_id=run_id,
            trial=trial,
            request_binding_stage_key=model_input.request_binding_stage_key,
            public_task=public_task,
            mode=mode,
            identities=identities,
            request_without_id=without_id,
        ),
        "prepared semantic request ID binding changed",
    )
    if mode == "digest":
        _require(
            set(request)
            == {
                "schema_version",
                "semantic_request_id",
                "execution_node_id",
                "prompt",
                "prompt_sha256",
                "representation_sha256",
            },
            "prepared digest request fields changed",
        )
        _require(
            len(identities) == 1
            and identities[0].representation_id == "multimodal_digest"
            and request.get("representation_sha256")
            == identities[0].artifact_sha256,
            "prepared digest identity binding changed",
        )
        prompt = _text(
            request.get("prompt"), "digest prompt", max_bytes=1024 * 1024
        )
        prefix = ContainerNodeRuntime.build_semantic_prompt(
            "multimodal_digest", "", question
        )
        marker = "\n--- representation ends ---\n\n" + question
        start, separator, _suffix = prefix.partition(marker)
        _require(bool(separator), "digest prompt contract is unavailable")
        _require(
            prompt.startswith(start) and prompt.endswith(marker),
            "prepared digest prompt structure changed",
        )
        digest_text = prompt[len(start) : -len(marker)]
        digest_bytes = digest_text.encode("utf-8")
        _require(
            len(digest_bytes) == identities[0].artifact_size_bytes
            and _sha256(digest_bytes) == identities[0].artifact_sha256,
            "prepared digest prompt differs from its artifact identity",
        )
        _require(
            request.get("prompt_sha256") == _sha256(prompt.encode("utf-8")),
            "prepared digest prompt hash changed",
        )
        return

    if mode in {"raw-prepared-frames", "frame-bundle"}:
        _require(
            set(request)
            == {
                "schema_version",
                "semantic_request_id",
                "execution_node_id",
                "representation_id",
                "representation_sha256",
                "question",
                "prompt_sha256",
                "frame_sequence_sha256",
                "frames",
            },
            "prepared vision request fields changed",
        )
        _require(request.get("question") == question, "public question changed")
        frames = request.get("frames")
        sequence = semantic_frame_sequence_sha256(frames)
        _require(
            request.get("frame_sequence_sha256") == sequence,
            "prepared frame sequence hash changed",
        )
        representation_id = request.get("representation_id")
        prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
            str(representation_id), len(frames), question
        )
        _require(
            request.get("prompt_sha256") == _sha256(prompt.encode("utf-8")),
            "prepared vision prompt hash changed",
        )
        if mode == "frame-bundle":
            _require(
                len(identities) == 1
                and identities[0].representation_id == "sampled_frame_bundle"
                and representation_id == "sampled_frame_bundle"
                and request.get("representation_sha256")
                == identities[0].artifact_sha256,
                "prepared frame bundle identity binding changed",
            )
        else:
            _require(
                len(identities) == 1
                and identities[0].representation_id == "raw_video"
                and representation_id == "raw_video_prepared_frames",
                "prepared raw frame identity binding changed",
            )
            _digest(
                request.get("representation_sha256"),
                "raw prepared representation_sha256",
            )
        return

    _require(
        set(request)
        == {
            "schema_version",
            "semantic_request_id",
            "execution_node_id",
            "representation_id",
            "representation_sha256",
            "digest_text",
            "digest_sha256",
            "question",
            "prompt_sha256",
            "frame_sequence_sha256",
            "frames",
        },
        "prepared fusion request fields changed",
    )
    identity_by_representation = {
        value.representation_id: value for value in identities
    }
    _require(
        set(identity_by_representation)
        == {"multimodal_digest", "sampled_frame_bundle"},
        "prepared fusion component identities changed",
    )
    digest_text = _text(
        request.get("digest_text"), "fusion digest", max_bytes=256 * 1024
    )
    digest_bytes = digest_text.encode("utf-8")
    digest_identity = identity_by_representation["multimodal_digest"]
    _require(
        len(digest_bytes) == digest_identity.artifact_size_bytes
        and _sha256(digest_bytes) == digest_identity.artifact_sha256
        and request.get("digest_sha256") == digest_identity.artifact_sha256,
        "prepared fusion digest identity binding changed",
    )
    _require(request.get("question") == question, "public question changed")
    frames = request.get("frames")
    sequence = semantic_frame_sequence_sha256(frames)
    _require(
        request.get("frame_sequence_sha256") == sequence,
        "prepared fusion frame sequence hash changed",
    )
    _require(
        request.get("representation_sha256")
        == semantic_fusion_representation_sha256(
            digest_identity.artifact_sha256, sequence
        ),
        "prepared fusion composite hash changed",
    )
    representation_id = "multimodal_digest+sampled_frame_bundle"
    _require(
        request.get("representation_id") == representation_id,
        "prepared fusion representation ID changed",
    )
    prompt = ContainerNodeRuntime.build_semantic_fusion_prompt(
        representation_id, digest_text, len(frames), question
    )
    _require(
        request.get("prompt_sha256") == _sha256(prompt.encode("utf-8")),
        "prepared fusion prompt hash changed",
    )


class BoundN6SemanticInferenceAdapter:
    """Execute and validate one prepared request without retaining its payload."""

    def __init__(
        self,
        *,
        executor: SemanticRequestExecutor,
        health_probe: Callable[[], Mapping[str, Any]],
        expected_model: str,
    ) -> None:
        self._executor = executor
        self._health_probe = health_probe
        self._expected_model = _identifier(expected_model, "expected_model")

    def infer(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        model_input: PreparedSemanticInput,
    ) -> SemanticInferenceResult:
        request = decode_prepared_semantic_request(model_input)
        _validate_prepared_request_binding(
            request,
            model_input,
            run_id=run_id,
            trial=trial,
            stage=stage,
            public_task=public_task,
        )
        before = self._health(request)
        try:
            raw = self._executor.execute(json.loads(model_input.payload))
        except Exception as exc:
            raise N6AdapterError(
                f"N6 semantic executor failed: {type(exc).__name__}"
            ) from exc
        after = self._health(request)
        _require(before == after, "N6 runtime epoch changed during inference")
        result = self._validate_result(raw, request, model_input.mode)
        answer = str(result["final_answer"])
        return SemanticInferenceResult(
            final_answer=answer,
            model=self._expected_model,
            input_sha256=model_input.payload_sha256,
            request_sha256=str(result["request_sha256"]),
            result_sha256=_sha256(_canonical(result)),
            telemetry=AdapterTelemetry(
                service_time_ms=float(result["service_time_ms"]),
                bytes_read=len(model_input.payload),
                bytes_sent=len(answer.encode("utf-8")),
            ),
        )

    def _health(self, request: Mapping[str, Any]) -> str:
        value = self._health_probe()
        _require(isinstance(value, Mapping), "N6 health response is invalid")
        _require(
            value.get("api_version") == CONTAINER_NODE_API_VERSION
            and value.get("status") == "ok"
            and value.get("node_id") == "N6",
            "N6 health identity is not ready",
        )
        _require(
            value.get("semantic_quality_enabled") is True
            and value.get("semantic_llm_configured") is True
            and value.get("credentials_recorded") is False,
            "N6 semantic service is not safely configured",
        )
        schema = request.get("schema_version")
        if schema == CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION:
            _require(
                value.get("semantic_vision_request_adapter_supported") is True
                and value.get("semantic_vision_request_schema_version") == schema,
                "N6 semantic vision adapter is unavailable",
            )
        elif schema == CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION:
            _require(
                value.get("semantic_fusion_request_adapter_supported") is True
                and value.get("semantic_fusion_request_schema_version") == schema,
                "N6 semantic fusion adapter is unavailable",
            )
        else:
            _require(
                schema == CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
                "N6 semantic request schema is unsupported",
            )
        epoch = value.get("runtime_epoch")
        _require(
            isinstance(epoch, str) and re.fullmatch(r"[0-9a-f]{32}", epoch),
            "N6 runtime epoch is invalid",
        )
        return str(epoch)

    def _validate_result(
        self,
        raw: Mapping[str, Any],
        request: Mapping[str, Any],
        mode: str,
    ) -> dict[str, Any]:
        _require(isinstance(raw, Mapping), "N6 result is not an object")
        result = json.loads(_canonical(raw))
        schema_by_request = {
            CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION: (
                CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION
            ),
            CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION: (
                CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION
            ),
            CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION: (
                CONTAINER_NODE_SEMANTIC_FUSION_RESULT_SCHEMA_VERSION
            ),
        }
        expected_schema = schema_by_request.get(request.get("schema_version"))
        _require(
            result.get("schema_version") == expected_schema,
            "N6 result schema changed",
        )
        _require(
            result.get("api_version") == CONTAINER_NODE_API_VERSION
            and result.get("status") == "completed"
            and result.get("outcome_type") == "completed"
            and result.get("telemetry_complete") is True,
            "N6 semantic execution did not complete",
        )
        _require(
            result.get("credentials_recorded") is False
            and result.get("llm_called") is True
            and type(result.get("idempotent_replay")) is bool,
            "N6 semantic result has unsafe provenance",
        )
        for name in (
            "semantic_request_id",
            "execution_node_id",
            "prompt_sha256",
            "representation_sha256",
        ):
            _require(result.get(name) == request.get(name), f"N6 changed {name}")
        _require(
            result.get("request_sha256") == _sha256(_canonical(request)),
            "N6 result binds a different request",
        )
        _require(result.get("model") == self._expected_model, "N6 model changed")
        answer = _text(
            result.get("final_answer"), "N6 final_answer", max_bytes=16 * 1024
        )
        _require(
            result.get("final_answer_sha256") == _sha256(answer.encode("utf-8")),
            "N6 final answer digest changed",
        )
        service_time = _number(
            result.get("service_time_ms"), "N6 service_time_ms"
        )
        started = _integer(result.get("started_monotonic_ns"), "N6 started time")
        finished = _integer(result.get("finished_monotonic_ns"), "N6 finished time")
        _require(finished >= started, "N6 monotonic interval is invalid")
        _require(
            service_time == (finished - started) / 1_000_000.0,
            "N6 service time differs from its monotonic interval",
        )
        if mode in {"raw-prepared-frames", "frame-bundle"}:
            self._validate_vision_result(result, request)
        elif mode == "digest+frames-fusion":
            self._validate_vision_result(result, request)
            _require(
                result.get("digest_sha256") == request.get("digest_sha256")
                and result.get("digest_bytes")
                == len(str(request["digest_text"]).encode("utf-8"))
                and result.get("semantic_digest_payload_integrity_verified") is True,
                "N6 fusion digest binding changed",
            )
        else:
            _require(
                result.get("data_plane_artifact_delivery_verified") is False
                and result.get("source_node_id") is None
                and result.get("representation_delivery_bytes") is None,
                "N6 direct digest result claims an external artifact fetch",
            )
        return result

    @staticmethod
    def _validate_vision_result(
        result: Mapping[str, Any], request: Mapping[str, Any]
    ) -> None:
        _require(
            result.get("frame_sequence_sha256")
            == request.get("frame_sequence_sha256")
            and result.get("frame_count") == len(request.get("frames", [])),
            "N6 frame sequence binding changed",
        )
        delivery = sum(
            int(frame["jpeg_size_bytes"]) for frame in request["frames"]
        )
        if request.get("schema_version") == (
            CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION
        ):
            delivery += len(str(request["digest_text"]).encode("utf-8"))
            expected_kind = "digest-and-ordered-jpeg-frames"
        else:
            expected_kind = "ordered-jpeg-frames"
        _require(
            result.get("representation_delivery_bytes") == delivery
            and result.get("semantic_input_kind") == expected_kind
            and result.get("semantic_frame_payload_integrity_verified") is True
            and result.get("data_plane_artifact_delivery_verified") is False
            and result.get("source_node_id") is None,
            "N6 frame delivery attestation changed",
        )


__all__ = [
    "BoundN6SemanticInferenceAdapter",
    "N6AdapterError",
    "N6ModelInputAdapter",
    "N6PreparationLimits",
    "N6SampledFrame",
    "RAW_PREPARED_REPRESENTATION_DOMAIN",
    "RawVideoFrameSampler",
    "SEMANTIC_REQUEST_ID_DOMAIN",
    "SemanticRequestExecutor",
    "decode_prepared_semantic_request",
    "raw_prepared_representation_sha256",
]
