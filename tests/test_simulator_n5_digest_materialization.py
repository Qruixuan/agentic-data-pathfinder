from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from pathfinder.simulator.n5_digest_materialization import (
    CHECKSUMS_NAME,
    DIGEST_NAME,
    EVIDENCE_NAME,
    PLAN_NAME,
    N5DigestMaterializationError,
    OpenAICompatibleVisionDigestAdapter,
    VisionDigestResult,
    freeze_n5_multimodal_digest_plan,
    materialize_n5_multimodal_digest,
    verify_n5_multimodal_digest_materialization,
    verify_n5_multimodal_digest_plan,
)
from pathfinder.video_prep import SampledImage


MODEL_ID = "qwen3.8-27b"
OBJECT_ID = "video-descriptive"
DURATION = 12.0


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _mp4_bytes(seed: int = 11) -> bytes:
    brands = b"isom" + struct.pack(">I", 512) + b"isomiso2mp41"
    ftyp = struct.pack(">I", len(brands) + 8) + b"ftyp" + brands
    body = bytes((seed + index) % 256 for index in range(128))
    return ftyp + struct.pack(">I", len(body) + 8) + b"mdat" + body


def _jpeg(seed: int) -> bytes:
    body = bytes((seed + index) % 256 for index in range(48))
    return b"\xff\xd8" + body + b"\xff\xd9"


def _frames() -> list[SampledImage]:
    return [
        SampledImage(
            frame_index=index,
            timestamp_seconds=float(index * 3 + 1),
            width=320,
            height=180,
            jpeg_bytes=_jpeg(index * 13),
        )
        for index in range(4)
    ]


def _digest() -> dict:
    return {
        "events": [
            {
                "start_seconds": 1.0,
                "end_seconds": 4.0,
                "description": "A person enters the visible room.",
            },
            {
                "start_seconds": 7.0,
                "end_seconds": None,
                "description": "The person sits beside a table.",
            },
        ],
        "summary": "A person enters a room and later sits by a table.",
    }


class FakeVisionAdapter:
    def __init__(
        self,
        *,
        model_id: str = MODEL_ID,
        digest: dict | None = None,
        llm_called: bool = True,
        error: Exception | None = None,
    ) -> None:
        self.model_id = model_id
        self.digest = digest if digest is not None else _digest()
        self.llm_called = llm_called
        self.error = error
        self.calls: list[dict] = []

    def generate_digest(self, **kwargs) -> VisionDigestResult:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return VisionDigestResult(
            model_id=self.model_id,
            digest=self.digest,
            response_sha256=_sha256(b"fake bounded model response"),
            protocol_attempts=1,
            llm_called=self.llm_called,
            adapter_id="fake-vision-model-v1",
        )


class FakeResponse:
    def __init__(self, payload: bytes, *, status: int = 200) -> None:
        self.payload = payload
        self.status = status

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args) -> None:
        return None

    def read(self, maximum: int = -1) -> bytes:
        if maximum < 0:
            return self.payload
        return self.payload[:maximum]


class FakeOpener:
    def __init__(self, response: FakeResponse | Exception) -> None:
        self.response = response
        self.calls: list[tuple] = []

    def open(self, request, *, timeout):
        self.calls.append((request, timeout))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class N5DigestMaterializationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "operator-video.mp4"
        self.source.write_bytes(_mp4_bytes())
        self.frames = _frames()

    def _freeze(self, name: str = "plan") -> Path:
        output = self.root / name
        freeze_n5_multimodal_digest_plan(
            self.source,
            self.frames,
            source_duration_seconds=DURATION,
            object_id=OBJECT_ID,
            model_id=MODEL_ID,
            output_dir=output,
            plan_id="n5-digest-test-v1",
            jpeg_max_dimension=768,
            seed=17,
        )
        return output

    def _sampler(self, frames: list[SampledImage] | None = None):
        selected = self.frames if frames is None else frames

        def sample(path, *, frame_count, jpeg_max_dimension):
            self.assertEqual(self.source.resolve(), path)
            self.assertEqual(4, frame_count)
            self.assertEqual(768, jpeg_max_dimension)
            return list(selected), DURATION

        return sample

    def test_freezes_exact_endpoint_free_plan_deterministically(self) -> None:
        first = self._freeze("plan-a")
        second = self._freeze("plan-b")
        self.assertEqual(
            {path.name: path.read_bytes() for path in first.iterdir()},
            {path.name: path.read_bytes() for path in second.iterdir()},
        )
        plan = json.loads((first / PLAN_NAME).read_text(encoding="utf-8"))
        self.assertEqual(_sha256(self.source.read_bytes()), plan["source"]["sha256"])
        self.assertEqual(len(self.source.read_bytes()), plan["source"]["size_bytes"])
        self.assertEqual(MODEL_ID, plan["generation"]["model_id"])
        self.assertTrue(plan["generation"]["semantic_model_call_required"])
        self.assertFalse(plan["generation"]["synthetic_or_hash_digest_permitted"])
        self.assertEqual(4, plan["sampling"]["frame_count"])
        self.assertEqual(
            [_sha256(frame.jpeg_bytes) for frame in self.frames],
            [row["jpeg_sha256"] for row in plan["sampling"]["frames"]],
        )
        text = (first / PLAN_NAME).read_text(encoding="utf-8").lower()
        self.assertNotIn("http://", text)
        self.assertNotIn("https://", text)
        self.assertNotIn(str(self.root).lower(), text)
        self.assertNotIn("operator-video.mp4", text)
        self.assertEqual(
            "VERIFIED",
            verify_n5_multimodal_digest_plan(first, self.source)["status"],
        )

    def test_materializes_canonical_digest_and_verifies_provenance(self) -> None:
        plan = self._freeze()
        output = self.root / "output"
        adapter = FakeVisionAdapter()
        report = materialize_n5_multimodal_digest(
            plan,
            self.source,
            output_dir=output,
            vision_adapter=adapter,
            sampler=self._sampler(),
        )
        self.assertEqual("VERIFIED", report["status"])
        self.assertTrue(report["llm_called"])
        self.assertEqual(MODEL_ID, report["model_id"])
        self.assertEqual(1, len(adapter.calls))
        self.assertEqual(MODEL_ID, adapter.calls[0]["expected_model_id"])
        self.assertEqual(17, adapter.calls[0]["seed"])
        digest = (output / DIGEST_NAME).read_text(encoding="utf-8")
        self.assertTrue(digest.startswith(
            "PATHFINDER QUESTION-INDEPENDENT MULTIMODAL DIGEST\n"
        ))
        self.assertIn("Object: video-descriptive", digest)
        self.assertIn("- [1.000s-4.000s] A person enters", digest)
        evidence = json.loads(
            (output / EVIDENCE_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual(_sha256(digest.encode("utf-8")), evidence[
            "output_binding"
        ]["sha256"])
        self.assertEqual(len(digest.encode("utf-8")), evidence[
            "output_binding"
        ]["size_bytes"])
        self.assertTrue(evidence["sampling_alignment_verified"])
        self.assertFalse(evidence["synthetic_or_hash_digest_used"])
        self.assertEqual(
            "VERIFIED",
            verify_n5_multimodal_digest_materialization(
                output,
                plan,
                self.source,
            )["status"],
        )

    def test_model_identity_and_llm_attestation_are_mandatory(self) -> None:
        plan = self._freeze()
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "model ID differs",
        ):
            materialize_n5_multimodal_digest(
                plan,
                self.source,
                output_dir=self.root / "wrong-model",
                vision_adapter=FakeVisionAdapter(model_id="other-model"),
                sampler=self._sampler(),
            )
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "did not attest",
        ):
            materialize_n5_multimodal_digest(
                plan,
                self.source,
                output_dir=self.root / "no-llm",
                vision_adapter=FakeVisionAdapter(llm_called=False),
                sampler=self._sampler(),
            )
        self.assertFalse((self.root / "wrong-model").exists())
        self.assertFalse((self.root / "no-llm").exists())

    def test_sampling_alignment_is_checked_before_model_call(self) -> None:
        plan = self._freeze()
        changed = list(self.frames)
        original = changed[2]
        changed[2] = SampledImage(
            frame_index=original.frame_index,
            timestamp_seconds=original.timestamp_seconds + 0.001,
            width=original.width,
            height=original.height,
            jpeg_bytes=original.jpeg_bytes,
        )
        adapter = FakeVisionAdapter()
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "alignment changed",
        ):
            materialize_n5_multimodal_digest(
                plan,
                self.source,
                output_dir=self.root / "misaligned",
                vision_adapter=adapter,
                sampler=self._sampler(changed),
            )
        self.assertEqual([], adapter.calls)

    def test_adapter_failure_never_creates_a_fallback_digest(self) -> None:
        plan = self._freeze()
        output = self.root / "failed"
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "no fallback digest was generated",
        ):
            materialize_n5_multimodal_digest(
                plan,
                self.source,
                output_dir=output,
                vision_adapter=FakeVisionAdapter(error=RuntimeError("down")),
                sampler=self._sampler(),
            )
        self.assertFalse(output.exists())

    def test_invalid_semantic_digest_is_rejected(self) -> None:
        plan = self._freeze()
        invalid = _digest()
        invalid["events"][0]["description"] = _sha256(b"not semantics") + "\n"
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "bounded, non-empty, trimmed string",
        ):
            materialize_n5_multimodal_digest(
                plan,
                self.source,
                output_dir=self.root / "invalid-digest",
                vision_adapter=FakeVisionAdapter(digest=invalid),
                sampler=self._sampler(),
            )

    def test_source_and_output_tampering_are_rejected(self) -> None:
        plan = self._freeze()
        changed_source = self.root / "changed.mp4"
        changed_source.write_bytes(_mp4_bytes(99))
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "content binding mismatch",
        ):
            verify_n5_multimodal_digest_plan(plan, changed_source)

        output = self.root / "valid-output"
        materialize_n5_multimodal_digest(
            plan,
            self.source,
            output_dir=output,
            vision_adapter=FakeVisionAdapter(),
            sampler=self._sampler(),
        )
        with (output / DIGEST_NAME).open("ab") as handle:
            handle.write(b"tamper")
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "checksum mismatch",
        ):
            verify_n5_multimodal_digest_materialization(
                output,
                plan,
                self.source,
            )


class OpenAICompatibleVisionDigestAdapterTest(unittest.TestCase):
    def test_http_is_limited_to_loopback_or_explicit_simulator_hosts(self) -> None:
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "plain HTTP is limited",
        ):
            OpenAICompatibleVisionDigestAdapter(
                base_url="http://example.com/v1",
                api_key="not-recorded",
                model_id=MODEL_ID,
            )
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "plain HTTP is limited",
        ):
            OpenAICompatibleVisionDigestAdapter(
                base_url="http://pathfinder-sim-n6/v1",
                api_key="not-recorded",
                model_id=MODEL_ID,
            )
        OpenAICompatibleVisionDigestAdapter(
            base_url="http://127.0.0.1:8010/v1",
            api_key="not-recorded",
            model_id=MODEL_ID,
        )
        OpenAICompatibleVisionDigestAdapter(
            base_url="http://pathfinder-sim-n6:8000/v1",
            api_key="not-recorded",
            model_id=MODEL_ID,
            allowed_http_simulator_hosts=("pathfinder-sim-n6",),
        )
        OpenAICompatibleVisionDigestAdapter(
            base_url="https://model.example.test/v1",
            api_key="not-recorded",
            model_id=MODEL_ID,
        )

    def test_default_transport_disables_proxy_and_redirect_handlers(self) -> None:
        fake = FakeOpener(FakeResponse(b"{}"))
        with patch(
            "pathfinder.simulator.n5_digest_materialization.build_opener",
            return_value=fake,
        ) as builder:
            OpenAICompatibleVisionDigestAdapter(
                base_url="https://model.example.test/v1",
                api_key="not-recorded",
                model_id=MODEL_ID,
            )
        handlers = builder.call_args.args
        self.assertEqual({}, handlers[0].proxies)
        self.assertEqual("_NoRedirectHandler", type(handlers[1]).__name__)

    def test_bounded_request_returns_exact_model_and_semantic_payload(self) -> None:
        content = json.dumps(_digest(), separators=(",", ":"))
        envelope = json.dumps({
            "model": MODEL_ID,
            "choices": [{"message": {"content": content}}],
        }).encode("utf-8")
        opener = FakeOpener(FakeResponse(envelope))
        adapter = OpenAICompatibleVisionDigestAdapter(
            base_url="http://127.0.0.1:8010/v1",
            api_key="runtime-only-key",
            model_id=MODEL_ID,
            opener=opener,
        )
        result = adapter.generate_digest(
            object_id=OBJECT_ID,
            frames=_frames(),
            duration_seconds=DURATION,
            expected_model_id=MODEL_ID,
            seed=17,
        )
        self.assertTrue(result.llm_called)
        self.assertEqual(MODEL_ID, result.model_id)
        self.assertEqual(_digest(), result.digest)
        request, timeout = opener.calls[0]
        self.assertEqual(180.0, timeout)
        self.assertEqual(
            "http://127.0.0.1:8010/v1/chat/completions",
            request.full_url,
        )
        payload = json.loads(request.data)
        self.assertEqual(MODEL_ID, payload["model"])
        images = [
            item for item in payload["messages"][0]["content"]
            if item["type"] == "image_url"
        ]
        self.assertEqual(4, len(images))
        self.assertTrue(all(
            item["image_url"]["url"].startswith("data:image/jpeg;base64,")
            for item in images
        ))

    def test_response_model_mismatch_and_redirect_are_rejected(self) -> None:
        content = json.dumps(_digest())
        wrong = json.dumps({
            "model": "other-model",
            "choices": [{"message": {"content": content}}],
        }).encode("utf-8")
        adapter = OpenAICompatibleVisionDigestAdapter(
            base_url="https://model.example.test/v1",
            api_key="runtime-only-key",
            model_id=MODEL_ID,
            opener=FakeOpener(FakeResponse(wrong)),
        )
        with self.assertRaisesRegex(
            N5DigestMaterializationError,
            "response model ID changed",
        ):
            adapter.generate_digest(
                object_id=OBJECT_ID,
                frames=_frames(),
                duration_seconds=DURATION,
                expected_model_id=MODEL_ID,
                seed=17,
            )

        redirect = HTTPError(
            "https://model.example.test/v1/chat/completions",
            302,
            "redirect",
            {},
            None,
        )
        redirected = OpenAICompatibleVisionDigestAdapter(
            base_url="https://model.example.test/v1",
            api_key="runtime-only-key",
            model_id=MODEL_ID,
            opener=FakeOpener(redirect),
        )
        with self.assertRaisesRegex(N5DigestMaterializationError, "HTTP 302"):
            redirected.generate_digest(
                object_id=OBJECT_ID,
                frames=_frames(),
                duration_seconds=DURATION,
                expected_model_id=MODEL_ID,
                seed=17,
            )

    def test_deployment_address_and_key_never_enter_runtime_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "video.mp4"
            source.write_bytes(_mp4_bytes())
            frames = _frames()
            plan = root / "plan"
            freeze_n5_multimodal_digest_plan(
                source,
                frames,
                source_duration_seconds=DURATION,
                object_id=OBJECT_ID,
                model_id=MODEL_ID,
                output_dir=plan,
                plan_id="deployment-redaction-v1",
                seed=17,
            )
            content = json.dumps(_digest(), separators=(",", ":"))
            response = json.dumps({
                "model": MODEL_ID,
                "choices": [{"message": {"content": content}}],
            }).encode("utf-8")
            deployment_address = "https://private-model.example.test/v1"
            runtime_key = "key-that-must-never-be-persisted"
            adapter = OpenAICompatibleVisionDigestAdapter(
                base_url=deployment_address,
                api_key=runtime_key,
                model_id=MODEL_ID,
                opener=FakeOpener(FakeResponse(response)),
            )

            def sampler(path, *, frame_count, jpeg_max_dimension):
                return list(frames), DURATION

            output = root / "output"
            materialize_n5_multimodal_digest(
                plan,
                source,
                output_dir=output,
                vision_adapter=adapter,
                sampler=sampler,
            )
            persisted = b"".join(
                path.read_bytes() for path in (*plan.iterdir(), *output.iterdir())
            ).decode("utf-8")
            self.assertNotIn(deployment_address, persisted)
            self.assertNotIn(runtime_key, persisted)


if __name__ == "__main__":
    unittest.main()
