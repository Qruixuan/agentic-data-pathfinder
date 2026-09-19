"""Offline tests for content-bound N6 model-input adapters."""

from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping, Sequence
from unittest import mock

from pathfinder.frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    REPRESENTATION_ID,
    deterministic_frame_bundle_tar,
)
from pathfinder.simulator.container_node import (
    CONTAINER_NODE_API_VERSION,
    CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_FUSION_RESULT_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VIDEO_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VIDEO_RESULT_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION,
    ContainerNodeRuntime,
    semantic_fusion_representation_sha256,
)
from pathfinder.simulator.full_flow_n6_adapters import (
    DIRECT_VIDEO_MEDIA_TYPE,
    DIRECT_VIDEO_REPRESENTATION_ID,
    BoundN6SemanticInferenceAdapter,
    N6AdapterError,
    N6ModelInputAdapter,
    N6PreparationLimits,
    N6SampledFrame,
    decode_prepared_semantic_request,
    raw_prepared_representation_sha256,
)
from pathfinder.simulator.full_flow_semantic_input_profiles import (
    build_semantic_input_profile,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    ArtifactAccess,
    ArtifactIdentity,
    ExactContentRange,
    ExactTemporalFrameSelection,
    PreparedSemanticInput,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding


OBJECT_ID = "nextqa-val-0000000001"
CATALOG_VERSION = "n6-adapter-catalog-v1"
MODEL = "qwen3.8-27b"
JPEG = base64.b64decode(
    "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoHBwYI"
    "DAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/2wBDAQMEBAUEBQkF"
    "BQkUDQsNFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQU"
    "FBQUFBT/wAARCAACAAIDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQF"
    "BgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEI"
    "I0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNk"
    "ZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLD"
    "xMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEB"
    "AQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJB"
    "UQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZH"
    "SElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaan"
    "qKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oA"
    "DAMBAAIRAxEAPwD7V+C37O3wp1v4OeBNR1H4ZeDr/ULzQbC4ubu60C0klnle3Rnd3aMl"
    "mYkkknJJJNFFFf0xln+40P8ABH8keXiP40/V/mf/2Q==",
    validate=True,
)


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _identity(representation_id: str, payload: bytes) -> ArtifactIdentity:
    return ArtifactIdentity(
        object_id=OBJECT_ID,
        representation_id=representation_id,
        artifact_sha256=_sha(payload),
        artifact_size_bytes=len(payload),
        object_catalog_version=CATALOG_VERSION,
    )


def _access(representation_id: str, payload: bytes) -> ArtifactAccess:
    return ArtifactAccess(_identity(representation_id, payload), payload)


def _public_task() -> dict[str, Any]:
    return build_n1_public_task_binding(
        workload_id="W1",
        object_id=OBJECT_ID,
        task_class_id="descriptive",
        question="What is the main action?",
        answer_options=[
            {"option_id": "A", "text": "A vehicle crosses a river."},
            {"option_id": "B", "text": "Two people perform music."},
        ],
        success_scoring_rule="multiple-choice-option-id-exact-match-v1",
    )


def _frame(index: int) -> N6SampledFrame:
    marker = b"frame-" + str(index).encode("ascii")
    payload = (
        JPEG[:2]
        + b"\xff\xfe"
        + (len(marker) + 2).to_bytes(2, "big")
        + marker
        + JPEG[2:]
    )
    return N6SampledFrame(
        frame_index=index,
        timestamp_seconds=0.5 + index,
        width=2,
        height=2,
        jpeg_bytes=payload,
    )


# One N3 selection-policy document digest, written into both bundle manifest
# fields and into the exact-range row, exactly as the real freezer does.
SELECTION_POLICY_SHA = "e" * 64


def _bundle_bytes(
    frame_count: int = 2,
    *,
    source_payload: bytes | None = None,
    sampling_method: str = "uniform-midpoint",
    source_duration_seconds: float | None = None,
    timestamp_start: float = 0.5,
    selection_policy_sha256: str = SELECTION_POLICY_SHA,
) -> bytes:
    frames = [
        N6SampledFrame(
            frame_index=index,
            timestamp_seconds=timestamp_start + index,
            width=_frame(index).width,
            height=_frame(index).height,
            jpeg_bytes=_frame(index).jpeg_bytes,
        )
        for index in range(frame_count)
    ]
    source = source_payload if source_payload is not None else b"x" * 100
    rows = [
        {
            "frame_index": frame.frame_index,
            "timestamp_seconds": frame.timestamp_seconds,
            "width": frame.width,
            "height": frame.height,
            "path": f"frames/{frame.frame_index:03d}.jpg",
            "jpeg_size_bytes": len(frame.jpeg_bytes),
            "jpeg_sha256": _sha(frame.jpeg_bytes),
        }
        for frame in frames
    ]
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": REPRESENTATION_ID,
        "object_id": OBJECT_ID,
        "source_video_id": "0000000001",
        "source_video_filename": "0000000001.mp4",
        "source_video_size_bytes": len(source),
        "source_video_sha256": _sha(source),
        "source_duration_seconds": (
            max(4.0, float(frame_count) + 1.0)
            if source_duration_seconds is None
            else source_duration_seconds
        ),
        "sampling": {
            "method": sampling_method,
            "frame_count": frame_count,
            "jpeg_max_dimension": 768,
            "jpeg_quality": 82,
            "jpeg_optimize": True,
        },
        "source_frame_descriptions": {
            "representation_id": "sampled_frames",
            "path": f"{OBJECT_ID}/sampled_frames.json",
            "sha256": selection_policy_sha256,
        },
        "generation_manifest_sha256": selection_policy_sha256,
        "frames": rows,
        "frame_count": frame_count,
        "total_jpeg_bytes": sum(len(frame.jpeg_bytes) for frame in frames),
        "software_versions": {"av": "17.0.1", "Pillow": "12.3.0"},
        "historical_visual_bytes_retained": False,
        "sampling_alignment_statement": (
            "These JPEG frames were regenerated and are aligned with the frozen "
            "sampling metadata. They do not claim byte identity with the "
            "historical visual input."
        ),
        "claims_byte_identity_with_historical_visual_input": False,
        "credentials_recorded": False,
        "llm_called": False,
        "network_calls_performed": False,
    }
    manifest_raw = (
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    ).encode("utf-8")
    members = [(OBJECT_MANIFEST_NAME, manifest_raw)] + [
        (f"frames/{frame.frame_index:03d}.jpg", frame.jpeg_bytes)
        for frame in frames
    ]
    return deterministic_frame_bundle_tar(members)


class RecordingSampler:
    def __init__(self, frames: Sequence[N6SampledFrame] | None = None) -> None:
        self.frames = tuple(frames or (_frame(0), _frame(1)))
        self.calls: list[dict[str, Any]] = []

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
    ) -> Sequence[N6SampledFrame]:
        self.calls.append({
            "payload": payload,
            "object_id": object_id,
            "source_payload_sha256": source_payload_sha256,
            "frame_count": frame_count,
            "jpeg_max_dimension": jpeg_max_dimension,
            "temporal_start_fraction": temporal_start_fraction,
            "temporal_end_fraction": temporal_end_fraction,
        })
        return self.frames


class N6PreparationTest(unittest.TestCase):
    def _adapter(
        self,
        sampler: RecordingSampler | None = None,
        **overrides: Any,
    ) -> N6ModelInputAdapter:
        limits = N6PreparationLimits(raw_frame_count=2, **overrides)
        return N6ModelInputAdapter(
            raw_sampler=sampler or RecordingSampler(),
            limits=limits,
            clock_ns=lambda: 100,
        )

    def _prepare(
        self,
        adapter: N6ModelInputAdapter,
        mode: str,
        artifacts: Sequence[ArtifactAccess],
    ) -> PreparedSemanticInput:
        return adapter.prepare(
            run_id="n6-offline-run-v1",
            trial={"trial_key": "scenario|W1|D0|r0000"},
            stage={"stage_key": "scenario|W1|D0|r0000|infer"},
            public_task=_public_task(),
            mode=mode,
            artifacts=artifacts,
        )

    def _prepare_profiled(
        self,
        adapter: N6ModelInputAdapter,
        *,
        route_family: str,
        mode: str,
        artifacts: Sequence[ArtifactAccess],
    ) -> PreparedSemanticInput:
        profile = build_semantic_input_profile(
            route_family=route_family,
            model_input_representation_ids=[
                value.source_identity.representation_id for value in artifacts
            ],
        )
        return adapter.prepare(
            run_id="n6-profile-run-v1",
            trial={
                "trial_key": f"scenario|W1|{route_family}|r0000",
                "route_family": route_family,
                "artifact_object_id": OBJECT_ID,
                "semantic_input_profile": profile,
            },
            stage={"stage_key": f"scenario|W1|{route_family}|prepare"},
            public_task=_public_task(),
            mode=mode,
            artifacts=artifacts,
        )

    def test_digest_builds_canonical_v1_request(self) -> None:
        digest = b"Two people perform music."
        prepared = self._prepare(
            self._adapter(),
            "digest",
            [_access("multimodal_digest", digest)],
        )
        request = decode_prepared_semantic_request(prepared)
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
            request["schema_version"],
        )
        self.assertEqual("N6", request["execution_node_id"])
        self.assertEqual(_sha(digest), request["representation_sha256"])
        self.assertEqual(_sha(request["prompt"].encode()), request["prompt_sha256"])
        self.assertNotIn("correct_answer_id", prepared.payload.decode())
        self.assertEqual(_canonical(request), prepared.payload)

    def test_digest_rejects_invalid_utf8_size_and_identity_tampering(self) -> None:
        with self.assertRaisesRegex(N6AdapterError, "UTF-8"):
            self._prepare(
                self._adapter(),
                "digest",
                [_access("multimodal_digest", b"\xff")],
            )
        with self.assertRaisesRegex(N6AdapterError, "byte bound"):
            self._prepare(
                self._adapter(max_digest_bytes=3),
                "digest",
                [_access("multimodal_digest", b"four")],
            )
        valid = _access("multimodal_digest", b"valid")
        altered = ArtifactAccess(valid.source_identity, b"other")
        with self.assertRaisesRegex(N6AdapterError, "size differs|digest differs"):
            self._prepare(self._adapter(), "digest", [altered])

    def test_raw_sampler_receives_the_exact_bounded_payload(self) -> None:
        payload = b"\x00\x00\x00\x18ftypisom-real-routed-video"
        sampler = RecordingSampler()
        access = _access("raw_video", payload)
        prepared = self._prepare(
            self._adapter(sampler), "raw-prepared-frames", [access]
        )
        request = decode_prepared_semantic_request(prepared)
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
            request["schema_version"],
        )
        self.assertEqual(payload, sampler.calls[0]["payload"])
        self.assertEqual(_sha(payload), sampler.calls[0]["source_payload_sha256"])
        self.assertEqual(2, sampler.calls[0]["frame_count"])
        self.assertEqual(
            raw_prepared_representation_sha256(
                access.source_identity,
                access,
                request["frame_sequence_sha256"],
            ),
            request["representation_sha256"],
        )

    def test_frozen_raw_and_indexed_profiles_use_distinct_representations(
        self,
    ) -> None:
        payload = b"real-routed-video" * 10_000
        raw_sampler = RecordingSampler(tuple(_frame(index) for index in range(24)))
        raw = self._prepare_profiled(
            self._adapter(raw_sampler),
            route_family="raw",
            mode="direct-video",
            artifacts=[_access("raw_video", payload)],
        )
        selected = _bundle_bytes(
            8,
            source_payload=payload,
            sampling_method="uniform-midpoint-temporal-window",
            source_duration_seconds=40.0,
            timestamp_start=10.0,
        )
        segment = ExactTemporalFrameSelection(
            object_id=OBJECT_ID,
            representation_id="raw_video",
            object_catalog_version=CATALOG_VERSION,
            full_artifact_size_bytes=len(payload),
            full_artifact_sha256=_sha(payload),
            selected_representation_id="indexed_temporal_frame_bundle",
            selected_artifact_size_bytes=len(selected),
            selected_artifact_sha256=_sha(selected),
            frame_count=8,
            temporal_start_fraction=0.25,
            temporal_end_fraction=0.75,
            selection_policy_sha256=SELECTION_POLICY_SHA,
        )
        indexed_sampler = RecordingSampler()
        indexed = self._prepare_profiled(
            self._adapter(indexed_sampler),
            route_family="indexed-raw",
            mode="raw-prepared-frames",
            artifacts=[ArtifactAccess(
                _identity("raw_video", payload),
                selected,
                segment=segment,
            )],
        )
        raw_request = decode_prepared_semantic_request(raw)
        # The raw family carries the complete encoded video and no frames at
        # all; nothing is decoded on the execution side.
        self.assertNotIn("frames", raw_request)
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_VIDEO_REQUEST_SCHEMA_VERSION,
            raw_request["schema_version"],
        )
        self.assertEqual(
            payload,
            base64.b64decode(raw_request["video_base64"], validate=True),
        )
        self.assertEqual(_sha(payload), raw_request["video_sha256"])
        self.assertEqual([], raw_sampler.calls)
        # Indexed raw remains a selective temporal frame projection.
        self.assertEqual(8, len(decode_prepared_semantic_request(indexed)["frames"]))
        self.assertEqual([], indexed_sampler.calls)
        self.assertLess(len(selected), len(payload))
        self.assertNotEqual(raw.payload_sha256, indexed.payload_sha256)

    def test_indexed_profile_refuses_an_n6_only_crop(self) -> None:
        payload = b"real-routed-video" * 10_000
        with self.assertRaisesRegex(N6AdapterError, "real N3 projection"):
            self._prepare_profiled(
                self._adapter(RecordingSampler()),
                route_family="indexed-raw",
                mode="raw-prepared-frames",
                artifacts=[_access("raw_video", payload)],
            )

    def test_raw_range_binds_both_full_identity_and_exact_segment(self) -> None:
        full = b"0123456789abcdefghij"
        identity = _identity("raw_video", full)
        selected = full[4:12]
        segment = ExactContentRange(
            object_id=OBJECT_ID,
            representation_id="raw_video",
            object_catalog_version=CATALOG_VERSION,
            full_artifact_size_bytes=len(full),
            full_artifact_sha256=_sha(full),
            range_start=4,
            range_end=11,
            range_sha256=_sha(selected),
        )
        access = ArtifactAccess(identity, selected, segment=segment)
        prepared = self._prepare(self._adapter(), "raw-prepared-frames", [access])
        request = decode_prepared_semantic_request(prepared)
        digest = raw_prepared_representation_sha256(
            identity, access, request["frame_sequence_sha256"]
        )
        self.assertEqual(digest, request["representation_sha256"])
        tampered = ArtifactAccess(identity, b"tampered", segment=segment)
        with self.assertRaisesRegex(N6AdapterError, "range payload"):
            self._prepare(self._adapter(), "raw-prepared-frames", [tampered])

    def test_raw_sampler_bounds_and_determinism_are_enforced(self) -> None:
        raw = _access("raw_video", b"raw-video")
        short = RecordingSampler((_frame(0),))
        with self.assertRaisesRegex(N6AdapterError, "frame count"):
            self._prepare(self._adapter(short), "raw-prepared-frames", [raw])
        large = _frame(1)
        large = N6SampledFrame(1, 1.5, 769, 2, large.jpeg_bytes)
        with self.assertRaisesRegex(N6AdapterError, "dimensions"):
            self._prepare(
                self._adapter(RecordingSampler((_frame(0), large))),
                "raw-prepared-frames",
                [raw],
            )
        first = self._prepare(self._adapter(), "raw-prepared-frames", [raw])
        second = self._prepare(self._adapter(), "raw-prepared-frames", [raw])
        self.assertEqual(first.payload, second.payload)
        self.assertEqual(first.preparation_sha256, second.preparation_sha256)

    def test_bundle_uses_strict_non_extracting_ingestion_and_v2(self) -> None:
        bundle = _bundle_bytes()
        access = _access("sampled_frame_bundle", bundle)
        prepared = self._prepare(self._adapter(), "frame-bundle", [access])
        request = decode_prepared_semantic_request(prepared)
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
            request["schema_version"],
        )
        self.assertEqual(_sha(bundle), request["representation_sha256"])
        self.assertEqual([0, 1], [row["frame_index"] for row in request["frames"]])
        changed = bytearray(bundle)
        changed[1024] ^= 1
        with self.assertRaisesRegex(N6AdapterError, "digest differs"):
            self._prepare(
                self._adapter(),
                "frame-bundle",
                [ArtifactAccess(access.source_identity, bytes(changed))],
            )

    def test_fusion_preserves_separate_component_identities_and_v3_hash(self) -> None:
        digest = _access("multimodal_digest", b"Two people perform music.")
        bundle = _access("sampled_frame_bundle", _bundle_bytes())
        prepared = self._prepare(
            self._adapter(),
            "digest+frames-fusion",
            [digest, bundle],
        )
        request = decode_prepared_semantic_request(prepared)
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION,
            request["schema_version"],
        )
        self.assertEqual(
            (digest.source_identity, bundle.source_identity),
            prepared.component_identities,
        )
        self.assertEqual(digest.payload_sha256, request["digest_sha256"])
        self.assertEqual(
            semantic_fusion_representation_sha256(
                request["digest_sha256"], request["frame_sequence_sha256"]
            ),
            request["representation_sha256"],
        )

    def test_derived_profiles_sparse_frames_and_preserve_digest_fusion(self) -> None:
        bundle = _access("sampled_frame_bundle", _bundle_bytes(8))
        frames = self._prepare_profiled(
            self._adapter(),
            route_family="remote-derived",
            mode="frame-bundle",
            artifacts=[bundle],
        )
        frame_request = decode_prepared_semantic_request(frames)
        self.assertEqual(4, len(frame_request["frames"]))
        self.assertEqual(
            [1.5, 3.5, 5.5, 7.5],
            [row["timestamp_seconds"] for row in frame_request["frames"]],
        )

        digest = _access("multimodal_digest", b"semantic digest")
        fusion = self._prepare_profiled(
            self._adapter(),
            route_family="remote-derived",
            mode="digest+frames-fusion",
            artifacts=[digest, bundle],
        )
        fusion_request = decode_prepared_semantic_request(fusion)
        self.assertEqual(4, len(fusion_request["frames"]))
        self.assertEqual(digest.payload_sha256, fusion_request["digest_sha256"])
        self.assertEqual("semantic digest", fusion_request["digest_text"])

    def test_tampered_semantic_input_profile_fails_closed(self) -> None:
        profile = build_semantic_input_profile(
            route_family="raw",
            model_input_representation_ids=["raw_video"],
        )
        tampered = {**profile, "direct_video_input": False}
        with self.assertRaisesRegex(
            N6AdapterError,
            "profile differs",
        ):
            self._adapter().prepare(
                run_id="run",
                trial={
                    "trial_key": "trial",
                    "route_family": "raw",
                    "artifact_object_id": OBJECT_ID,
                    "semantic_input_profile": tampered,
                },
                stage={"stage_key": "stage"},
                public_task=_public_task(),
                mode="direct-video",
                artifacts=[_access("raw_video", b"raw")],
            )

    def test_raw_family_profile_cannot_drive_the_sampled_frame_path(self) -> None:
        """A direct-video profile must never be served as sampled frames."""

        profile = build_semantic_input_profile(
            route_family="raw",
            model_input_representation_ids=["raw_video"],
        )
        with self.assertRaisesRegex(N6AdapterError, "profile mode differs"):
            self._adapter(RecordingSampler()).prepare(
                run_id="run",
                trial={
                    "trial_key": "trial",
                    "route_family": "raw",
                    "artifact_object_id": OBJECT_ID,
                    "semantic_input_profile": profile,
                },
                stage={"stage_key": "stage"},
                public_task=_public_task(),
                mode="raw-prepared-frames",
                artifacts=[_access("raw_video", b"raw")],
            )

    def test_hidden_label_fields_are_rejected(self) -> None:
        task = _public_task()
        task["correct_answer_id"] = "B"
        with self.assertRaisesRegex(N6AdapterError, "private field"):
            self._adapter().prepare(
                run_id="run",
                trial={"trial_key": "trial"},
                stage={"stage_key": "stage"},
                public_task=task,
                mode="digest",
                artifacts=[_access("multimodal_digest", b"digest")],
            )

    def test_requests_are_accepted_by_the_real_container_node_contract(self) -> None:
        cases = {
            "digest": [_access("multimodal_digest", b"digest")],
            "raw-prepared-frames": [_access("raw_video", b"raw")],
            "frame-bundle": [
                _access("sampled_frame_bundle", _bundle_bytes())
            ],
            "digest+frames-fusion": [
                _access("multimodal_digest", b"digest"),
                _access("sampled_frame_bundle", _bundle_bytes()),
            ],
        }
        with tempfile.TemporaryDirectory() as temporary:
            runtime = ContainerNodeRuntime(
                "N6", Path(temporary), enable_semantic_llm=True
            )
            for mode, artifacts in cases.items():
                with self.subTest(mode=mode):
                    prepared = self._prepare(
                        self._adapter(), mode, artifacts
                    )
                    request = decode_prepared_semantic_request(prepared)
                    with mock.patch.object(
                        runtime,
                        "_call_semantic_llm",
                        return_value=("B", MODEL),
                    ) as call:
                        result = runtime.semantic_complete(request)
                    self.assertEqual("completed", result["status"])
                    self.assertEqual(
                        prepared.payload_sha256,
                        result["request_sha256"],
                    )
                    call.assert_called_once()


class FakeExecutor:
    def __init__(self, *, mutate: str | None = None) -> None:
        self.mutate = mutate
        self.calls: list[dict[str, Any]] = []

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        request = json.loads(_canonical(request))
        self.calls.append(request)
        schema = request["schema_version"]
        result_schema = {
            CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION: (
                CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION
            ),
            CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION: (
                CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION
            ),
            CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION: (
                CONTAINER_NODE_SEMANTIC_FUSION_RESULT_SCHEMA_VERSION
            ),
        }[schema]
        result: dict[str, Any] = {
            "schema_version": result_schema,
            "api_version": CONTAINER_NODE_API_VERSION,
            "status": "completed",
            "outcome_type": "completed",
            "telemetry_complete": True,
            "semantic_request_id": request["semantic_request_id"],
            "execution_node_id": "N6",
            "started_monotonic_ns": 100,
            "finished_monotonic_ns": 200,
            "service_time_ms": 0.0001,
            "request_sha256": _sha(_canonical(request)),
            "prompt_sha256": request["prompt_sha256"],
            "representation_sha256": request["representation_sha256"],
            "data_plane_artifact_delivery_verified": False,
            "source_node_id": None,
            "representation_delivery_bytes": None,
            "model": MODEL,
            "final_answer": "B",
            "final_answer_sha256": _sha(b"B"),
            "llm_called": True,
            "credentials_recorded": False,
            "idempotent_replay": False,
        }
        if schema != CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION:
            frame_bytes = sum(row["jpeg_size_bytes"] for row in request["frames"])
            result.update({
                "frame_sequence_sha256": request["frame_sequence_sha256"],
                "frame_count": len(request["frames"]),
                "representation_delivery_bytes": frame_bytes,
                "semantic_input_kind": "ordered-jpeg-frames",
                "semantic_frame_payload_integrity_verified": True,
            })
        if schema == CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION:
            result.update({
                "digest_sha256": request["digest_sha256"],
                "digest_bytes": len(request["digest_text"].encode("utf-8")),
                "representation_delivery_bytes": (
                    result["representation_delivery_bytes"]
                    + len(request["digest_text"].encode("utf-8"))
                ),
                "semantic_input_kind": "digest-and-ordered-jpeg-frames",
                "semantic_digest_payload_integrity_verified": True,
            })
        if self.mutate is not None:
            result[self.mutate] = "0" * 64
        return result


def _health(epoch: str = "a" * 32) -> dict[str, Any]:
    return {
        "api_version": CONTAINER_NODE_API_VERSION,
        "status": "ok",
        "node_id": "N6",
        "runtime_epoch": epoch,
        "semantic_quality_enabled": True,
        "semantic_llm_configured": True,
        "semantic_vision_request_adapter_supported": True,
        "semantic_vision_request_schema_version": (
            CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION
        ),
        "semantic_fusion_request_adapter_supported": True,
        "semantic_fusion_request_schema_version": (
            CONTAINER_NODE_SEMANTIC_FUSION_REQUEST_SCHEMA_VERSION
        ),
        "credentials_recorded": False,
    }


class N6CrossStageRequestBindingTest(unittest.TestCase):
    """A real route prepares on one stage and infers on another.

    The semantic request ID is derived during ``prepare-model-input`` but was
    previously revalidated against whichever stage was executing, so the
    genuine two-stage D0 route failed its own binding check before N6 was
    ever contacted. Every prior test prepared and inferred with the same
    stage key, which is why none of them caught it.
    """

    PREPARE_STAGE = {"stage_key": "scenario|W1|D0|r0000|prepare-model-input"}
    INFER_STAGE = {"stage_key": "scenario|W1|D0|r0000|infer"}

    def _prepare(self, stage=None):
        adapter = N6ModelInputAdapter(
            raw_sampler=RecordingSampler(),
            limits=N6PreparationLimits(raw_frame_count=2),
            clock_ns=lambda: 1,
        )
        return adapter.prepare(
            run_id="run",
            trial={"trial_key": "trial"},
            stage=stage or self.PREPARE_STAGE,
            public_task=_public_task(),
            mode="digest",
            artifacts=[_access("multimodal_digest", b"digest")],
        )

    def _infer(self, prepared, executor, stage=None):
        return BoundN6SemanticInferenceAdapter(
            executor=executor, health_probe=_health, expected_model=MODEL,
        ).infer(
            run_id="run",
            trial={"trial_key": "trial"},
            stage=stage or self.INFER_STAGE,
            public_task=_public_task(),
            model_input=prepared,
        )

    def test_distinct_prepare_and_infer_stages_succeed_once(self) -> None:
        prepared = self._prepare()
        self.assertEqual(
            self.PREPARE_STAGE["stage_key"], prepared.request_binding_stage_key
        )
        executor = FakeExecutor()
        result = self._infer(prepared, executor)
        self.assertEqual(MODEL, result.model)
        self.assertEqual(1, len(executor.calls))

    def test_tampered_binding_stage_key_fails_before_execution(self) -> None:
        prepared = self._prepare()
        forged = PreparedSemanticInput(
            mode=prepared.mode,
            payload=prepared.payload,
            component_identities=prepared.component_identities,
            preparation_sha256=prepared.preparation_sha256,
            request_binding_stage_key=self.INFER_STAGE["stage_key"],
        )
        executor = FakeExecutor()
        with self.assertRaises(N6AdapterError):
            self._infer(forged, executor)
        self.assertEqual(0, len(executor.calls))

    def test_id_derived_from_infer_stage_still_fails(self) -> None:
        # Outer commitment recomputed so only the request-ID binding can
        # reject this: the ID was derived from the infer stage key.
        wrong = self._prepare(stage=self.INFER_STAGE)
        forged = PreparedSemanticInput(
            mode=wrong.mode,
            payload=wrong.payload,
            component_identities=wrong.component_identities,
            preparation_sha256=_sha(_canonical({
                "mode": wrong.mode,
                "payload_sha256": wrong.payload_sha256,
                "payload_size_bytes": len(wrong.payload),
                "component_identity_sha256": [
                    v.commitment for v in wrong.component_identities
                ],
                "request_binding_stage_key": self.PREPARE_STAGE["stage_key"],
            })),
            request_binding_stage_key=self.PREPARE_STAGE["stage_key"],
        )
        executor = FakeExecutor()
        with self.assertRaisesRegex(N6AdapterError, "request ID binding"):
            self._infer(forged, executor)
        self.assertEqual(0, len(executor.calls))

    def test_same_stage_inline_inference_still_works(self) -> None:
        prepared = self._prepare(stage=self.INFER_STAGE)
        executor = FakeExecutor()
        result = self._infer(prepared, executor, stage=self.INFER_STAGE)
        self.assertEqual(MODEL, result.model)
        self.assertEqual(1, len(executor.calls))


class N6InferenceTest(unittest.TestCase):
    def _prepared(self, mode: str) -> PreparedSemanticInput:
        adapter = N6ModelInputAdapter(
            raw_sampler=RecordingSampler(),
            limits=N6PreparationLimits(raw_frame_count=2),
            clock_ns=lambda: 1,
        )
        artifacts = {
            "digest": [_access("multimodal_digest", b"digest")],
            "raw-prepared-frames": [_access("raw_video", b"raw")],
            "frame-bundle": [_access("sampled_frame_bundle", _bundle_bytes())],
            "digest+frames-fusion": [
                _access("multimodal_digest", b"digest"),
                _access("sampled_frame_bundle", _bundle_bytes()),
            ],
        }[mode]
        return adapter.prepare(
            run_id="run",
            trial={"trial_key": "trial"},
            stage={"stage_key": "stage"},
            public_task=_public_task(),
            mode=mode,
            artifacts=artifacts,
        )

    def test_all_three_container_schemas_are_result_bound(self) -> None:
        for mode in (
            "digest",
            "raw-prepared-frames",
            "frame-bundle",
            "digest+frames-fusion",
        ):
            with self.subTest(mode=mode):
                prepared = self._prepared(mode)
                executor = FakeExecutor()
                adapter = BoundN6SemanticInferenceAdapter(
                    executor=executor,
                    health_probe=_health,
                    expected_model=MODEL,
                )
                result = adapter.infer(
                    run_id="run",
                    trial={"trial_key": "trial"},
                    stage={"stage_key": "stage"},
                    public_task=_public_task(),
                    model_input=prepared,
                )
                self.assertEqual("B", result.final_answer)
                self.assertEqual(prepared.payload_sha256, result.input_sha256)
                self.assertEqual(prepared.payload_sha256, result.request_sha256)
                self.assertEqual(1, len(executor.calls))

    def test_result_and_health_tampering_fail_closed(self) -> None:
        prepared = self._prepared("frame-bundle")
        for field in ("request_sha256", "prompt_sha256", "representation_sha256"):
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(
                    N6AdapterError, "different request|changed"
                ),
            ):
                BoundN6SemanticInferenceAdapter(
                    executor=FakeExecutor(mutate=field),
                    health_probe=_health,
                    expected_model=MODEL,
                ).infer(
                    run_id="run",
                    trial={"trial_key": "trial"},
                    stage={"stage_key": "stage"},
                    public_task=_public_task(),
                    model_input=prepared,
                )
        with self.assertRaisesRegex(N6AdapterError, "health identity"):
            BoundN6SemanticInferenceAdapter(
                executor=FakeExecutor(),
                health_probe=lambda: {**_health(), "node_id": "N5"},
                expected_model=MODEL,
            ).infer(
                run_id="run",
                trial={"trial_key": "trial"},
                stage={"stage_key": "stage"},
                public_task=_public_task(),
                model_input=prepared,
            )

    def test_runtime_epoch_change_is_rejected(self) -> None:
        values = iter((_health("a" * 32), _health("b" * 32)))
        with self.assertRaisesRegex(N6AdapterError, "runtime epoch changed"):
            BoundN6SemanticInferenceAdapter(
                executor=FakeExecutor(),
                health_probe=lambda: next(values),
                expected_model=MODEL,
            ).infer(
                run_id="run",
                trial={"trial_key": "trial"},
                stage={"stage_key": "stage"},
                public_task=_public_task(),
                model_input=self._prepared("digest"),
            )

    def test_outer_preparation_tamper_is_rejected_before_execute(self) -> None:
        prepared = self._prepared("digest")
        changed = PreparedSemanticInput(
            mode=prepared.mode,
            payload=prepared.payload + b" ",
            component_identities=prepared.component_identities,
            preparation_sha256=prepared.preparation_sha256,
            request_binding_stage_key=prepared.request_binding_stage_key,
        )
        executor = FakeExecutor()
        with self.assertRaisesRegex(N6AdapterError, "JSON|canonical|commitment"):
            BoundN6SemanticInferenceAdapter(
                executor=executor,
                health_probe=_health,
                expected_model=MODEL,
            ).infer(
                run_id="run",
                trial={},
                stage={},
                public_task={},
                model_input=changed,
            )
        self.assertEqual([], executor.calls)


if __name__ == "__main__":
    unittest.main()
