from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

from pathfinder.data_agent_manifest import load_data_agent_manifest
from pathfinder.frame_bundle_ingest import (
    FRAME_BUNDLE_MEDIA_TYPE,
    validate_frame_bundle_bytes,
)
from pathfinder.simulator.full_flow_exact_range_catalog import (
    ExactFullObjectRangeCatalog,
    build_full_flow_exact_range_catalog,
    verify_full_flow_exact_range_catalog,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    ArtifactIdentity,
    ExactTemporalFrameSelection,
)
from pathfinder.simulator.n3_indexed_data_plane import (
    INDEXED_REPRESENTATION_ID,
    N3TemporalSelectionPolicy,
    build_n3_indexed_data_plane_package,
    verify_n3_indexed_data_plane_package,
    verify_n3_semantic_data_plane_package,
)
from pathfinder.simulator.raw_cold_data_plane import (
    DATA_AGENT_MANIFEST_PATH,
    PACKAGE_MANIFEST_NAME,
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)
from pathfinder.video_prep import SampledImage


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _mp4() -> bytes:
    compatible = b"isom" + b"iso2" + b"mp41"
    payload = b"isom" + struct.pack(">I", 512) + compatible
    ftyp = struct.pack(">I", len(payload) + 8) + b"ftyp" + payload
    body = bytes(index % 251 for index in range(1024 * 1024))
    return ftyp + struct.pack(">I", len(body) + 8) + b"mdat" + body


def _jpeg(index: int) -> bytes:
    app0 = b"\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    sof0 = (
        b"\xff\xc0\x00\x0b\x08"
        + (2).to_bytes(2, "big")
        + (2).to_bytes(2, "big")
        + b"\x01\x01\x11\x00"
    )
    sos = b"\xff\xda\x00\x08\x01\x01\x00\x00\x3f\x00"
    return b"\xff\xd8" + app0 + sof0 + sos + bytes([index, 0x22]) + b"\xff\xd9"


class _Sampler:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int, float, float]] = []

    def __call__(
        self,
        path: Path,
        *,
        frame_count: int,
        jpeg_max_dimension: int,
        temporal_start_fraction: float,
        temporal_end_fraction: float,
    ) -> tuple[list[SampledImage], float]:
        self.calls.append((
            frame_count,
            jpeg_max_dimension,
            temporal_start_fraction,
            temporal_end_fraction,
        ))
        self.source_sha256 = _sha256(path.read_bytes())
        return ([
            SampledImage(
                frame_index=index,
                timestamp_seconds=10.0 + index,
                width=2,
                height=2,
                jpeg_bytes=_jpeg(index),
            )
            for index in range(frame_count)
        ], 40.0)


class N3IndexedDataPlaneTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.object_id = "nextqa-val-2435100235"
        self.catalog_version = "n3-real-one-case-v1"
        self.payload = _mp4()
        source = self.root / "source.mp4"
        source.write_bytes(self.payload)
        self.raw = self.root / "raw"
        build_raw_cold_data_plane_package(
            [RawColdObjectBinding(
                object_id=self.object_id,
                artifact_path=source,
                catalog_version=self.catalog_version,
                plan_ids=tuple(f"D{index}" for index in range(8)),
                dataset_id="nextqa",
                dataset_revision="one-case-v1",
                source_object_id="2435100235",
                artifact_sha256=_sha256(self.payload),
                artifact_size_bytes=len(self.payload),
            )],
            output_dir=self.raw,
            package_id="n3-raw-one-case-v1",
        )
        self.sampler = _Sampler()
        self.indexed = self.root / "indexed"
        build_n3_indexed_data_plane_package(
            self.raw,
            output_dir=self.indexed,
            package_id="n3-indexed-one-case-v1",
            sampler=self.sampler,
        )

    def test_freezes_smaller_real_projection_and_data_agent_binding(self) -> None:
        result = verify_n3_indexed_data_plane_package(self.indexed)
        self.assertEqual("VERIFIED_N3_INDEXED_DATA_PLANE", result["status"])
        self.assertTrue(result["source_side_projection_verified"])
        self.assertTrue(result["byte_reduction_verified"])
        self.assertEqual([(8, 768, 0.25, 0.75)], self.sampler.calls)

        report = json.loads(
            (self.indexed / PACKAGE_MANIFEST_NAME).read_text(encoding="utf-8")
        )
        rows = {
            row["representation_id"]: row for row in report["objects"]
        }
        self.assertLess(
            rows[INDEXED_REPRESENTATION_ID]["artifact_size_bytes"],
            rows["raw_video"]["artifact_size_bytes"],
        )
        selected = rows[INDEXED_REPRESENTATION_ID]
        bundle = (
            self.indexed / selected["artifact_package_path"]
        ).read_bytes()
        validated = validate_frame_bundle_bytes(
            bundle,
            expected_object_id=self.object_id,
            expected_sha256=selected["artifact_sha256"],
            expected_size_bytes=selected["artifact_size_bytes"],
            artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
        )
        self.assertEqual(8, validated.frame_count)

        manifest = load_data_agent_manifest(
            self.indexed / DATA_AGENT_MANIFEST_PATH
        )
        resolved = manifest.resolve(
            plan_id="D1",
            object_id=self.object_id,
            representation_id=INDEXED_REPRESENTATION_ID,
            requested_location="origin-cold",
        )
        self.assertEqual(bundle, resolved.path.read_bytes())

    def test_exact_catalog_resolves_temporal_projection(self) -> None:
        ranges = self.root / "selections"
        built = build_full_flow_exact_range_catalog(
            self.indexed,
            catalog_id="one-case-real-selection-v1",
            output_dir=ranges,
        )
        self.assertEqual("FROZEN_EXACT_TEMPORAL_SELECTIONS", built["status"])
        self.assertTrue(built["index_selectivity_claimed"])
        self.assertTrue(built["byte_reduction_claimed"])
        verified = verify_full_flow_exact_range_catalog(ranges, self.indexed)
        self.assertTrue(verified["source_side_projection_executed"])

        selection = ExactFullObjectRangeCatalog(
            ranges, self.indexed
        ).resolve(ArtifactIdentity(
            object_id=self.object_id,
            representation_id="raw_video",
            artifact_sha256=_sha256(self.payload),
            artifact_size_bytes=len(self.payload),
            object_catalog_version=self.catalog_version,
        ))
        self.assertIsInstance(selection, ExactTemporalFrameSelection)
        self.assertEqual(8, selection.frame_count)
        self.assertEqual((0.25, 0.75), (
            selection.temporal_start_fraction,
            selection.temporal_end_fraction,
        ))
        self.assertLess(
            selection.selected_artifact_size_bytes,
            selection.full_artifact_size_bytes,
        )

    def test_flexible_verifier_accepts_legacy_and_upgraded_packages(self) -> None:
        self.assertEqual(
            "VERIFIED_RAW_COLD_DATA_PLANE",
            verify_n3_semantic_data_plane_package(self.raw)["status"],
        )
        self.assertEqual(
            "VERIFIED_N3_INDEXED_DATA_PLANE",
            verify_n3_semantic_data_plane_package(self.indexed)["status"],
        )


if __name__ == "__main__":
    unittest.main()
