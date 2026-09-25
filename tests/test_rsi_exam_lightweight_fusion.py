"""A light D summary is generated once per video, not once per question."""

from __future__ import annotations

import hashlib
from io import BytesIO
import json
from pathlib import Path
import tempfile
import unittest

from PIL import Image

from pathfinder.rsi_exam.lightweight_fusion import (
    PROMPT_SHA256, ProviderResponse, freeze_lightweight_fusion,
    materialize_video_summaries, verify_lightweight_fusion,
)
from pathfinder.rsi_exam.formal_foundation import _frame_bundle
from pathfinder.simulator.n4_derived_data_plane import (
    N4ArtifactProvenance, N4DerivedArtifactInput,
    build_n4_derived_data_package,
)


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _bundle(object_id: str, root: Path) -> bytes:
    rows = []
    for index in range(24):
        buffer = BytesIO()
        Image.new("RGB", (16, 16), (index * 10, 1, 2)).save(
            buffer, format="JPEG", quality=82, optimize=True,
        )
        payload = buffer.getvalue()
        name = f"frames/{index:03d}.jpg"
        path = root / object_id / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        rows.append({
            "frame_index": index,
            "object_id": object_id,
            "package_path": path.relative_to(root).as_posix(),
            "jpeg_size_bytes": len(payload),
            "jpeg_sha256": _sha(payload),
            "timestamp_seconds": float(index) + 0.5,
            "width": 16,
            "height": 16,
        })
    return _frame_bundle(
        object_id=object_id,
        object_row={"source_video_size_bytes": 100,
                    "source_video_sha256": "a" * 64,
                    "duration_seconds": 24.0},
        frame_rows=rows, preparation_root=root,
        preparation_sha256="b" * 64,
        frame_description_path="source-decoded-frames.jsonl",
    )


class LightweightFusionTests(unittest.TestCase):
    def test_one_summary_call_per_video_then_fuse_existing_frames(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = []
            for index in range(8):
                object_id = f"video-{index}"
                inputs.append(N4DerivedArtifactInput(
                    object_id=object_id,
                    representation_id="sampled_frame_bundle",
                    artifact_bytes=_bundle(object_id, root / "prep"),
                    plan_ids=tuple(f"D{i}" for i in range(8)),
                    provenance=N4ArtifactProvenance(
                        producer_node_id="N5",
                        publication_source_id=f"test-{object_id}",
                        source_representation_id="raw_video",
                        source_content_sha256="a" * 64,
                        derivation_id="test-frame-only",
                        derivation_sha256="b" * 64,
                    ),
                ))
            build_n4_derived_data_package(
                inputs, output_dir=root / "frames", package_id="test-frames",
                catalog_version="test-frames-catalog",
            )
            calls = []

            def provider(request, _timeout):
                body = json.loads(request.data)
                self.assertEqual(body["model"], "test-model")
                content = body["messages"][0]["content"]
                self.assertEqual(sum(part["type"] == "image_url"
                                     for part in content), 8)
                self.assertIn("question-independent", content[0]["text"])
                self.assertEqual(len(content), 17)
                calls.append(request.full_url)
                payload = json.dumps({
                    "choices": [{"message": {"content": json.dumps({
                        "summary": "A person moves across the room.",
                    })}}],
                    "usage": {"prompt_tokens": 12,
                              "completion_tokens": 8,
                              "total_tokens": 20},
                }).encode()
                return ProviderResponse(
                    payload, f"00000000-0000-0000-0000-{len(calls):012d}",
                )

            report = materialize_video_summaries(
                root / "frames", cache_dir=root / "cache",
                output_dir=root / "summaries", model_id="test-model",
                base_url="https://example.invalid/v1", api_key="test-key",
                transport=provider,
            )
            self.assertEqual(report["provider_requests_this_run"], 8)
            self.assertEqual(len(calls), 8)
            self.assertEqual(report["prompt_tokens"], 96)
            self.assertEqual(report["completion_tokens"], 64)
            self.assertEqual(len(PROMPT_SHA256), 64)
            summaries = [json.loads(line) for line in
                         (root / "summaries/video-summaries.jsonl")
                         .read_bytes().splitlines()]
            self.assertEqual(len({row["provider_request_id_sha256"]
                                  for row in summaries}), 8)
            self.assertTrue(all(row["provider_request_id_sha256"]
                                for row in summaries))
            for path in (root / "cache").iterdir():
                self.assertNotIn(b"test-key", path.read_bytes())

            def no_provider(_request, _timeout):
                self.fail("cached response must not spend another call")

            again = materialize_video_summaries(
                root / "frames", cache_dir=root / "cache",
                output_dir=root / "summaries-again", model_id="test-model",
                base_url="https://example.invalid/v1", api_key="test-key",
                transport=no_provider,
            )
            self.assertEqual(again["provider_requests_this_run"], 0)
            fused = freeze_lightweight_fusion(
                root / "frames", root / "summaries",
                output_dir=root / "fusion", package_id="test-fusion",
            )
            self.assertEqual(fused["object_count"], 8)
            self.assertEqual(fused, verify_lightweight_fusion(
                root / "fusion", root / "frames", root / "summaries",
            ))
            document = json.loads((root / "fusion/n4/n4-derived-data-package.json")
                                  .read_bytes())
            self.assertEqual(len(document["objects"]), 16)
            self.assertEqual({r["representation_id"] for r in
                              document["objects"]},
                             {"sampled_frame_bundle", "multimodal_digest"})


if __name__ == "__main__":
    unittest.main()
