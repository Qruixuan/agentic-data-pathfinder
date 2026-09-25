"""A frame-only N4 build needs no caption or provider input."""

from __future__ import annotations

from io import BytesIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from PIL import Image

from pathfinder.rsi_exam.lightweight_derived import (
    freeze_lightweight_derived, verify_lightweight_derived,
)
from pathfinder.video_prep import SampledImage


class LightweightDerivedTests(unittest.TestCase):
    def test_frame_only_package_is_source_bound_and_needs_no_caption(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw_root = root / "raw"
            source = raw_root / "artifacts/nextqa-val-123/raw_video.mp4"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"test-video")
            import hashlib
            source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
            row = {
                "object_id": "nextqa-val-123",
                "artifact_package_path": source.relative_to(raw_root).as_posix(),
                "artifact_sha256": source_sha,
                "artifact_size_bytes": source.stat().st_size,
            }
            (raw_root / "raw-cold-data-plane.json").write_text(
                json.dumps({"objects": [row]}), encoding="utf-8",
            )

            def sampler(_source, **options):
                self.assertEqual(options["frame_count"], 24)
                self.assertEqual(options["jpeg_max_dimension"], 768)
                frames = []
                for index in range(24):
                    buffer = BytesIO()
                    Image.new("RGB", (16, 16), (index * 10, 1, 2)).save(
                        buffer, format="JPEG", quality=82, optimize=True,
                    )
                    frames.append(SampledImage(
                        frame_index=index, timestamp_seconds=index + 0.5,
                        width=16, height=16, jpeg_bytes=buffer.getvalue(),
                    ))
                return frames, 24.0

            with patch(
                "pathfinder.rsi_exam.lightweight_derived."
                "verify_raw_cold_data_plane_package",
            ):
                report = freeze_lightweight_derived(
                    raw_root, output_dir=root / "light", package_id="light-test",
                    sampler=sampler,
                )
                self.assertEqual(report["object_count"], 1)
                self.assertEqual(report["provider_requests_made"], 0)
                self.assertEqual(report, verify_lightweight_derived(
                    root / "light", raw_root,
                ))
                n4 = json.loads((root / "light/n4/n4-derived-data-package.json")
                                .read_bytes())
                self.assertEqual([item["representation_id"]
                                  for item in n4["objects"]],
                                 ["sampled_frame_bundle"])
                receipt = json.loads((root / "light/build-receipt.json")
                                     .read_bytes())
                self.assertFalse(receipt["caption_or_embedding_required"])
                with self.assertRaisesRegex(ValueError, "output exists"):
                    freeze_lightweight_derived(
                        raw_root, output_dir=root / "light",
                        package_id="light-test", sampler=sampler,
                    )


if __name__ == "__main__":
    unittest.main()
