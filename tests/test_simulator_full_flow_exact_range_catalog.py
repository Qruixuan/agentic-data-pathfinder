from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_exact_range_catalog import (
    CATALOG_NAME,
    ExactFullObjectRangeCatalog,
    FullFlowExactRangeCatalogError,
    build_full_flow_exact_range_catalog,
    verify_full_flow_exact_range_catalog,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import ArtifactIdentity
from pathfinder.simulator.raw_cold_data_plane import (
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _mp4(seed: int) -> bytes:
    compatible = b"isom" + b"iso2" + b"mp41"
    payload = b"isom" + struct.pack(">I", 512) + compatible
    ftyp = struct.pack(">I", len(payload) + 8) + b"ftyp" + payload
    body = bytes((seed + index) % 256 for index in range(128))
    return ftyp + struct.pack(">I", len(body) + 8) + b"mdat" + body


class FullFlowExactRangeCatalogTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "source.mp4"
        self.payload = _mp4(17)
        self.source.write_bytes(self.payload)
        self.object_id = "nextqa-val-1000000017"
        self.catalog_version = "n3-test-catalog-v1"
        self.n3 = self.root / "n3"
        build_raw_cold_data_plane_package(
            [RawColdObjectBinding(
                object_id=self.object_id,
                artifact_path=self.source,
                catalog_version=self.catalog_version,
                plan_ids=tuple(f"D{index}" for index in range(8)),
                dataset_id="test-dataset",
                dataset_revision="test-revision-v1",
                source_object_id="1000000017",
                artifact_sha256=_sha256(self.payload),
                artifact_size_bytes=len(self.payload),
            )],
            package_id="n3-test-package-v1",
            output_dir=self.n3,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _build(self, name: str = "ranges") -> Path:
        output = self.root / name
        result = build_full_flow_exact_range_catalog(
            self.n3,
            catalog_id="exact-full-object-fallback-v1",
            output_dir=output,
        )
        self.assertEqual("FROZEN_EXACT_FULL_OBJECT_FALLBACK", result["status"])
        self.assertEqual(1, result["entry_count"])
        return output

    def test_builds_and_resolves_content_bound_full_object_range(self) -> None:
        output = self._build()
        verified = verify_full_flow_exact_range_catalog(output, self.n3)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertFalse(verified["partial_mp4_ranges_supported"])
        self.assertFalse(verified["byte_reduction_claimed"])

        catalog = ExactFullObjectRangeCatalog(output, self.n3)
        segment = catalog.resolve(ArtifactIdentity(
            object_id=self.object_id,
            representation_id="raw_video",
            artifact_sha256=_sha256(self.payload),
            artifact_size_bytes=len(self.payload),
            object_catalog_version=self.catalog_version,
        ))
        self.assertEqual(0, segment.range_start)
        self.assertEqual(len(self.payload) - 1, segment.range_end)
        self.assertEqual(_sha256(self.payload), segment.range_sha256)
        self.assertEqual(len(self.payload), segment.range_size_bytes)

    def test_output_contains_no_artifact_or_endpoint(self) -> None:
        output = self._build()
        self.assertEqual(
            {"SHA256SUMS", CATALOG_NAME},
            {path.name for path in output.iterdir()},
        )
        text = (output / CATALOG_NAME).read_text(encoding="utf-8")
        self.assertNotIn("http://", text)
        self.assertNotIn("https://", text)
        self.assertNotIn(str(self.source), text)
        self.assertNotIn("api_key", text.casefold())
        self.assertNotIn("bearer", text.casefold())

    def test_tampering_fails_even_when_checksum_is_rewritten(self) -> None:
        output = self._build()
        path = output / CATALOG_NAME
        document = json.loads(path.read_text(encoding="utf-8"))
        document["entries"][0]["range_end"] -= 1
        path.write_text(
            json.dumps(document, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (output / "SHA256SUMS").write_text(
            f"{_sha256(path.read_bytes())}  {CATALOG_NAME}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            FullFlowExactRangeCatalogError,
            "catalog digest failed|full-object fallback",
        ):
            verify_full_flow_exact_range_catalog(output, self.n3)

    def test_source_drift_is_detected(self) -> None:
        output = self._build()
        artifact = next((self.n3 / "artifacts").rglob("*.mp4"))
        changed = bytearray(artifact.read_bytes())
        changed[-1] ^= 1
        artifact.write_bytes(bytes(changed))
        with self.assertRaisesRegex(
            FullFlowExactRangeCatalogError,
            "N3 semantic data-plane package verification failed",
        ):
            verify_full_flow_exact_range_catalog(output, self.n3)

    def test_identity_mismatch_fails_closed(self) -> None:
        output = self._build()
        catalog = ExactFullObjectRangeCatalog(output, self.n3)
        with self.assertRaisesRegex(
            FullFlowExactRangeCatalogError,
            "differs from trial identity",
        ):
            catalog.resolve(ArtifactIdentity(
                object_id=self.object_id,
                representation_id="raw_video",
                artifact_sha256="f" * 64,
                artifact_size_bytes=len(self.payload),
                object_catalog_version=self.catalog_version,
            ))


if __name__ == "__main__":
    unittest.main()
