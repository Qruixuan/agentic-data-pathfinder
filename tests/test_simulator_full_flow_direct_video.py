"""Focused coverage for the real direct-encoded-video path used by D0/D4.

These tests prove that the raw family delivers the complete original encoded
video to N6, that every other family keeps its own representation, that the
outbound backend request carries a real video content block, and that a
falsified direct-video claim is rejected rather than trusted.
"""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
import tempfile
import unittest
from typing import Any, Mapping
from unittest import mock

from pathfinder.simulator.container_node import (
    CONTAINER_NODE_SEMANTIC_VIDEO_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VIDEO_RESULT_SCHEMA_VERSION,
    ContainerNodeError,
    ContainerNodeRuntime,
)
from pathfinder.simulator.full_flow_n6_adapters import (
    DIRECT_VIDEO_MEDIA_TYPE,
    DIRECT_VIDEO_REPRESENTATION_ID,
    N6AdapterError,
    N6ModelInputAdapter,
    N6PreparationLimits,
    decode_prepared_semantic_request,
)
from pathfinder.simulator.full_flow_semantic_input_profiles import (
    RAW_DIRECT_VIDEO_PROFILE_ID,
    build_semantic_input_profile,
)
from pathfinder.simulator.full_flow_semantic_route_evidence import (
    SemanticRouteEvidenceValidationError,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    ArtifactAccess,
    ArtifactIdentity,
)

OBJECT_ID = "nextqa-val-4010069381"
CATALOG_VERSION = "catalog-v1"
# A small but structurally real MP4 header prefix; the adapters never decode
# it, they only bind its identity and hand it to the backend.
VIDEO = b"\x00\x00\x00\x18ftypisom" + b"direct-video-payload" * 64


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _identity(representation_id: str, payload: bytes) -> ArtifactIdentity:
    return ArtifactIdentity(
        object_id=OBJECT_ID,
        representation_id=representation_id,
        object_catalog_version=CATALOG_VERSION,
        artifact_size_bytes=len(payload),
        artifact_sha256=_sha(payload),
    )


def _access(representation_id: str, payload: bytes) -> ArtifactAccess:
    return ArtifactAccess(_identity(representation_id, payload), payload)


def _public_task() -> dict[str, Any]:
    from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding

    return build_n1_public_task_binding(
        workload_id="smoke-retrieval",
        object_id=OBJECT_ID,
        task_class_id="W3",
        question="Why did the person move the object?",
        answer_options=[
            {"option_id": "A", "text": "to clean underneath"},
            {"option_id": "B", "text": "to reach the switch"},
        ],
        success_scoring_rule="multiple-choice-option-id-canonical-match-v1",
    )


def _prepare(payload: bytes = VIDEO, **limit_kwargs: Any):
    adapter = N6ModelInputAdapter(
        raw_sampler=_forbidden_sampler,
        limits=N6PreparationLimits(**limit_kwargs),
    )
    profile = build_semantic_input_profile(
        route_family="raw",
        model_input_representation_ids=["raw_video"],
    )
    return adapter.prepare(
        run_id="direct-video-run-v1",
        trial={
            "trial_key": "scenario|W3|D0|r0000",
            "route_family": "raw",
            "artifact_object_id": OBJECT_ID,
            "semantic_input_profile": profile,
        },
        stage={"stage_key": "scenario|W3|D0|prepare"},
        public_task=_public_task(),
        mode="direct-video",
        artifacts=[_access("raw_video", payload)],
    )


def _forbidden_sampler(*args: Any, **kwargs: Any):
    raise AssertionError("the direct-video path must never decode frames")


class DirectVideoPreparationTest(unittest.TestCase):
    def test_d0_carries_the_complete_encoded_video_and_its_identity(self) -> None:
        prepared = _prepare()
        request = decode_prepared_semantic_request(prepared)
        self.assertEqual("direct-video", prepared.mode)
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_VIDEO_REQUEST_SCHEMA_VERSION,
            request["schema_version"],
        )
        # The exact routed bytes are what will reach the backend.
        self.assertEqual(
            VIDEO, base64.b64decode(request["video_base64"], validate=True)
        )
        self.assertEqual(_sha(VIDEO), request["video_sha256"])
        self.assertEqual(len(VIDEO), request["video_size_bytes"])
        self.assertEqual(DIRECT_VIDEO_MEDIA_TYPE, request["video_media_type"])
        self.assertEqual(
            DIRECT_VIDEO_REPRESENTATION_ID, request["representation_id"]
        )
        # Public evidence commits to the actual inference bytes.
        self.assertEqual(_sha(VIDEO), request["representation_sha256"])
        self.assertNotIn("frames", request)
        self.assertNotIn("frame_sequence_sha256", request)

    def test_d0_no_longer_uses_raw_prepared_frames(self) -> None:
        profile = build_semantic_input_profile(
            route_family="raw",
            model_input_representation_ids=["raw_video"],
        )
        self.assertEqual(RAW_DIRECT_VIDEO_PROFILE_ID, profile["profile_id"])
        self.assertEqual("direct-video", profile["input_mode"])
        self.assertIsNone(profile["frame_selection"])
        self.assertTrue(profile["direct_video_input"])
        # The sampler is wired to explode; reaching preparation without it
        # proves no frame decoding occurs on this path.
        self.assertEqual("direct-video", _prepare().mode)

    def test_other_families_keep_their_own_representations(self) -> None:
        indexed = build_semantic_input_profile(
            route_family="indexed-raw",
            model_input_representation_ids=["raw_video"],
        )
        self.assertEqual("raw-prepared-frames", indexed["input_mode"])
        self.assertEqual(8, indexed["frame_selection"]["frame_count"])
        self.assertFalse(indexed["direct_video_input"])
        for family in ("remote-derived", "local-cache-derived"):
            derived = build_semantic_input_profile(
                route_family=family,
                model_input_representation_ids=["sampled_frame_bundle"],
            )
            self.assertEqual("frame-bundle", derived["input_mode"])
            self.assertEqual(4, derived["frame_selection"]["frame_count"])
            self.assertFalse(derived["direct_video_input"])

    def test_oversized_video_and_partial_reads_fail_closed(self) -> None:
        with self.assertRaisesRegex(N6AdapterError, "byte bound"):
            _prepare(max_direct_video_bytes=16)
        identity = _identity("raw_video", VIDEO)
        mismatched = ArtifactAccess(identity, VIDEO[:32])
        adapter = N6ModelInputAdapter(raw_sampler=_forbidden_sampler)
        with self.assertRaisesRegex(N6AdapterError, "size differs|digest differs"):
            adapter.prepare(
                run_id="run",
                trial={
                    "trial_key": "t",
                    "route_family": "raw",
                    "artifact_object_id": OBJECT_ID,
                },
                stage={"stage_key": "s"},
                public_task=_public_task(),
                mode="direct-video",
                artifacts=[mismatched],
            )


class DirectVideoBackendRequestTest(unittest.TestCase):
    """The outbound model request must contain a real video content block."""

    def _runtime(self) -> ContainerNodeRuntime:
        state_dir = tempfile.mkdtemp(prefix="pf-direct-video-")
        self.addCleanup(shutil.rmtree, state_dir, True)
        return ContainerNodeRuntime(
            "N6",
            state_dir,
            enable_semantic_llm=True,
        )

    def test_backend_request_uses_a_video_url_data_url_block(self) -> None:
        runtime = self._runtime()
        captured: dict[str, Any] = {}

        class _Response:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *args: Any) -> None:
                return None

            def read(self_inner, _limit: int) -> bytes:
                return json.dumps({
                    "model": "qwen3.8-27b",
                    "choices": [{"message": {"content": "B"}}],
                }).encode("utf-8")

        def _fake_opener(_base_url: str):
            class _Opener:
                def open(self_inner, request: Any, timeout: float) -> Any:
                    captured["body"] = json.loads(request.data.decode("utf-8"))
                    return _Response()

            return _Opener()

        env = {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": "https://example.invalid/v1",
            "PATHFINDER_SEMANTIC_LLM_MODEL": "qwen3.8-27b",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "unit-test-placeholder",
        }
        with mock.patch.dict("os.environ", env, clear=False), mock.patch(
            "pathfinder.simulator.container_node._semantic_llm_opener",
            _fake_opener,
        ):
            answer, model = runtime._call_semantic_llm(
                "prompt",
                video_payload=VIDEO,
                video_media_type=DIRECT_VIDEO_MEDIA_TYPE,
                video_frames_per_second=2.0,
            )

        self.assertEqual("B", answer)
        self.assertEqual("qwen3.8-27b", model)
        content = captured["body"]["messages"][0]["content"]
        block = content[0]
        self.assertEqual("video_url", block["type"])
        self.assertEqual(2.0, block["fps"])
        url = block["video_url"]["url"]
        prefix = f"data:{DIRECT_VIDEO_MEDIA_TYPE};base64,"
        self.assertTrue(url.startswith(prefix))
        # The real encoded video is what crosses the wire, not a frame list.
        self.assertEqual(
            VIDEO, base64.b64decode(url[len(prefix):], validate=True)
        )
        self.assertNotIn(
            "image_url", json.dumps(captured["body"]["messages"])
        )

    def test_unsupported_video_schemas_fail_closed(self) -> None:
        runtime = self._runtime()
        env = {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": "https://example.invalid/v1",
            "PATHFINDER_SEMANTIC_LLM_MODEL": "qwen3.8-27b",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "unit-test-placeholder",
        }
        with mock.patch.dict("os.environ", env, clear=False):
            # A media type whose direct-video schema was never established.
            with self.assertRaisesRegex(ContainerNodeError, "media type"):
                runtime._call_semantic_llm(
                    "prompt",
                    video_payload=VIDEO,
                    video_media_type="video/x-matroska",
                    video_frames_per_second=2.0,
                )
            # A frame rate outside the documented backend range.
            with self.assertRaisesRegex(ContainerNodeError, "frames_per_second"):
                runtime._call_semantic_llm(
                    "prompt",
                    video_payload=VIDEO,
                    video_media_type=DIRECT_VIDEO_MEDIA_TYPE,
                    video_frames_per_second=99.0,
                )
            # Video and frames are mutually exclusive representations.
            with self.assertRaisesRegex(ContainerNodeError, "both video"):
                runtime._call_semantic_llm(
                    "prompt",
                    jpeg_frames=(b"jpeg",),
                    video_payload=VIDEO,
                    video_media_type=DIRECT_VIDEO_MEDIA_TYPE,
                    video_frames_per_second=2.0,
                )

    def test_node_rejects_a_video_request_that_misstates_its_payload(self) -> None:
        runtime = self._runtime()
        prepared = _prepare()
        request = dict(decode_prepared_semantic_request(prepared))
        tampered = {
            **request,
            "video_base64": base64.b64encode(b"a different video").decode("ascii"),
        }
        with self.assertRaisesRegex(ContainerNodeError, "video_size_bytes"):
            runtime.semantic_complete(tampered)
        swapped = {
            **request,
            "semantic_request_id": _sha(b"distinct-request-identity"),
            "video_sha256": _sha(b"unrelated"),
        }
        with self.assertRaisesRegex(
            ContainerNodeError, "video_sha256|representation"
        ):
            runtime.semantic_complete(swapped)


class DirectVideoEvidenceTest(unittest.TestCase):
    """The public verifier must reject a falsified direct-video claim."""

    @staticmethod
    def _model_input(**overrides: Any) -> dict[str, Any]:
        base = {
            "mode": "direct-video",
            "payload_sha256": _sha(b"payload"),
            "payload_size_bytes": 10,
            "component_identity_sha256": [_sha(b"identity")],
            "preparation_sha256": _sha(b"preparation"),
            "semantic_input_profile_id": RAW_DIRECT_VIDEO_PROFILE_ID,
            "semantic_input_profile_sha256": _sha(b"profile"),
            "semantic_input_profile_verified": True,
            "semantic_content_sha256": _sha(b"content"),
            "frame_count": 0,
            "frame_timestamps_seconds": [],
            "frame_dimensions": [],
            "frame_payload_bytes": 0,
            "frame_sequence_sha256": None,
            "digest_input_sha256": None,
            "temporal_window_fraction": None,
            "direct_video_input": True,
            "direct_video_sha256": _sha(VIDEO),
            "direct_video_size_bytes": len(VIDEO),
        }
        base.update(overrides)
        return base

    def _check(self, **overrides: Any) -> None:
        from pathfinder.simulator import full_flow_semantic_route_evidence as ev

        ev._verify_direct_video_claim(
            self._model_input(**overrides),
            overrides.get(
                "semantic_input_profile_id", RAW_DIRECT_VIDEO_PROFILE_ID
            ),
            True,
        )

    def test_honest_direct_video_evidence_is_accepted(self) -> None:
        self._check()

    def test_claim_without_delivered_video_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            SemanticRouteEvidenceValidationError, "direct_video_sha256"
        ):
            self._check(direct_video_sha256=None)

    def test_claim_on_a_sampled_representation_is_rejected(self) -> None:
        # Setting the flag while actually shipping frames must fail.
        with self.assertRaisesRegex(
            SemanticRouteEvidenceValidationError, "input mode"
        ):
            self._check(mode="raw-prepared-frames")
        with self.assertRaisesRegex(
            SemanticRouteEvidenceValidationError, "sampled representation"
        ):
            self._check(
                frame_count=24,
                frame_timestamps_seconds=[float(i) for i in range(24)],
                frame_dimensions=[{"width": 2, "height": 2}] * 24,
                frame_payload_bytes=240,
                frame_sequence_sha256=_sha(b"frames"),
            )

    def test_claim_disagreeing_with_the_frozen_profile_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            SemanticRouteEvidenceValidationError, "frozen profile"
        ):
            self._check(semantic_input_profile_id="derived-sparse-frames-4-v1")

    def test_non_video_input_cannot_carry_a_video_commitment(self) -> None:
        with self.assertRaisesRegex(
            SemanticRouteEvidenceValidationError, "video commitment"
        ):
            self._check(
                mode="frame-bundle",
                direct_video_input=False,
                semantic_input_profile_id="derived-sparse-frames-4-v1",
                frame_count=4,
                frame_timestamps_seconds=[0.0, 1.0, 2.0, 3.0],
                frame_dimensions=[{"width": 2, "height": 2}] * 4,
                frame_payload_bytes=40,
                frame_sequence_sha256=_sha(b"frames"),
            )


class DirectVideoConfidentialityTest(unittest.TestCase):
    def test_no_hidden_label_or_credential_enters_the_video_request(self) -> None:
        request = decode_prepared_semantic_request(_prepare())
        text = _canonical(request).decode("utf-8")
        for forbidden in (
            "correct_answer_id",
            "hidden_label",
            "api_key",
            "authorization",
            "bearer",
        ):
            self.assertNotIn(forbidden, text.casefold())
        # The public task contributes only its question and option texts.
        self.assertIn("Why did the person move the object?", request["question"])

    def test_scoring_rule_is_unchanged_by_the_video_path(self) -> None:
        task = _public_task()
        self.assertEqual(
            "multiple-choice-option-id-canonical-match-v1",
            task["success_scoring_rule"],
        )
        request = decode_prepared_semantic_request(_prepare())
        self.assertTrue(
            request["question"].rstrip().endswith(
                "Return exactly one option ID and no other text."
            )
        )


if __name__ == "__main__":
    unittest.main()
