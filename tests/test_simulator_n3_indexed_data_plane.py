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
    N3IndexedDataPlaneError,
    N3TemporalSelectionPolicy,
    TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
    build_n3_indexed_data_plane_package,
    load_n3_temporal_selection_policy_manifest,
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


class N3MultiPolicyIndexedDataPlaneTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / "raw"
        bindings = []
        for index, source_id in enumerate(("111", "222")):
            payload = _mp4() + bytes([index])
            source = self.root / f"{source_id}.mp4"
            source.write_bytes(payload)
            bindings.append(RawColdObjectBinding(
                object_id=f"nextqa-val-{source_id}",
                artifact_path=source,
                catalog_version="multi-policy-v1",
                plan_ids=("D1", "D5"),
                dataset_id="nextqa",
                dataset_revision="formal-v1",
                source_object_id=source_id,
                artifact_sha256=_sha256(payload),
                artifact_size_bytes=len(payload),
            ))
        build_raw_cold_data_plane_package(
            bindings,
            output_dir=self.raw,
            package_id="n3-raw-multi-policy-v1",
        )

    @staticmethod
    def _policy(start: float, end: float) -> N3TemporalSelectionPolicy:
        provenance = {
            "action_id": "temporal-index-formal-v1",
            "anchor_top_k": 2,
            "anchor_window_ordinals": [1, 2],
            "expansion_basis": "timestamp",
            "fallback_used": False,
            "max_selected_windows": 4,
            "merged_intervals_seconds": [[start * 40.0, end * 40.0]],
            "public_question_sha256": "1" * 64,
            "relation": "none",
            "selected_window_ordinals": [1, 2],
            "temporal_index_package_sha256": "2" * 64,
        }
        return N3TemporalSelectionPolicy(
            frame_count=2,
            temporal_start_fraction=start,
            temporal_end_fraction=end,
            sampling_method=TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
            selection_provenance=provenance,
        )

    def test_object_specific_policies_are_frozen_and_verified(self) -> None:
        policies = {
            "nextqa-val-111": self._policy(0.1, 0.4),
            "nextqa-val-222": self._policy(0.6, 0.9),
        }
        sampler = _Sampler()
        output = self.root / "indexed"
        build_n3_indexed_data_plane_package(
            self.raw,
            output_dir=output,
            package_id="n3-indexed-multi-policy-v1",
            policies=policies,
            sampler=sampler,
        )
        verified = verify_n3_indexed_data_plane_package(output)
        self.assertEqual(
            "pathfinder.simulator-n3-indexed-data-plane/v1alpha2",
            verified["schema_version"],
        )
        self.assertEqual(
            [(2, 768, 0.1, 0.4), (2, 768, 0.6, 0.9)],
            sampler.calls,
        )
        report = json.loads(
            (output / PACKAGE_MANIFEST_NAME).read_text(encoding="utf-8")
        )
        self.assertNotIn("selection_policy", report)
        self.assertEqual(
            {
                object_id: policy.to_dict()
                for object_id, policy in policies.items()
            },
            report["selection_policies"],
        )

    def test_exact_catalog_resolves_multi_policy_temporal_projections(
        self,
    ) -> None:
        policies = {
            "nextqa-val-111": self._policy(0.1, 0.4),
            "nextqa-val-222": self._policy(0.6, 0.9),
        }
        indexed = self.root / "indexed-for-catalog"
        build_n3_indexed_data_plane_package(
            self.raw,
            output_dir=indexed,
            package_id="n3-indexed-multi-policy-catalog-v1",
            policies=policies,
            sampler=_Sampler(),
        )
        selections = self.root / "selections"
        built = build_full_flow_exact_range_catalog(
            indexed,
            catalog_id="multi-policy-real-selections-v1",
            output_dir=selections,
        )
        self.assertEqual("FROZEN_EXACT_TEMPORAL_SELECTIONS", built["status"])
        verified = verify_full_flow_exact_range_catalog(selections, indexed)
        self.assertTrue(verified["source_side_projection_executed"])

        manifest = json.loads(
            (indexed / PACKAGE_MANIFEST_NAME).read_text(encoding="utf-8")
        )
        raw_rows = {
            row["object_id"]: row
            for row in manifest["objects"]
            if row["representation_id"] == "raw_video"
        }
        catalog = ExactFullObjectRangeCatalog(selections, indexed)
        for object_id, policy in policies.items():
            row = raw_rows[object_id]
            selection = catalog.resolve(ArtifactIdentity(
                object_id=object_id,
                representation_id="raw_video",
                artifact_sha256=row["artifact_sha256"],
                artifact_size_bytes=row["artifact_size_bytes"],
                object_catalog_version=row["catalog_version"],
            ))
            self.assertIsInstance(selection, ExactTemporalFrameSelection)
            self.assertEqual(
                (
                    policy.temporal_start_fraction,
                    policy.temporal_end_fraction,
                ),
                (
                    selection.temporal_start_fraction,
                    selection.temporal_end_fraction,
                ),
            )

    def test_policy_manifest_requires_exact_canonical_documents(self) -> None:
        path = self.root / "policies.json"
        document = {
            "schema_version": (
                "pathfinder.n3-temporal-selection-policy-manifest/v1alpha1"
            ),
            "policies": {
                "nextqa-val-111": self._policy(0.1, 0.4).to_dict(),
            },
        }
        path.write_text(
            json.dumps(document, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        loaded = load_n3_temporal_selection_policy_manifest(path)
        self.assertEqual(document["policies"]["nextqa-val-111"],
                         loaded["nextqa-val-111"].to_dict())

        document["policies"]["nextqa-val-111"]["extra"] = True
        path.write_text(
            json.dumps(document, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        with self.assertRaises(N3IndexedDataPlaneError):
            load_n3_temporal_selection_policy_manifest(path)

    def test_policy_keys_must_exactly_cover_raw_objects(self) -> None:
        with self.assertRaisesRegex(N3IndexedDataPlaneError, "exactly cover"):
            build_n3_indexed_data_plane_package(
                self.raw,
                output_dir=self.root / "invalid",
                package_id="n3-indexed-invalid-v1",
                policies={"nextqa-val-111": self._policy(0.1, 0.4)},
                sampler=_Sampler(),
            )


if __name__ == "__main__":
    unittest.main()
