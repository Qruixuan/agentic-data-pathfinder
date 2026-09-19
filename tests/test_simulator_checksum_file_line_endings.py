"""A frozen SHA256SUMS must be byte-identical on every operating system.

``Path.write_text`` opens the file in text mode, so on Windows Python rewrites
every ``\\n`` as ``\\r\\n``.  A checksum file frozen on a Windows workstation
then could not be checked by ``sha256sum -c`` on the Linux hosts it is
deployed to, and the same artifact frozen on two machines was not byte
identical -- the same class of defect as the source-digest normalisation in
``test_simulator_admission_source_digest_portability``.

Most freezers in the package already write bytes.  These tests pin that for
the ones that did not, and guard the whole package against a relapse.
"""

from __future__ import annotations

import ast
import hashlib
import pathlib
import struct
import tempfile
import unittest

from pathfinder.simulator.full_flow_exact_range_catalog import (
    CHECKSUMS_NAME,
    build_full_flow_exact_range_catalog,
)
from pathfinder.simulator.n3_indexed_data_plane import (
    build_n3_indexed_data_plane_package,
)
from pathfinder.simulator.raw_cold_data_plane import (
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)
from pathfinder.video_prep import SampledImage

PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1] / "pathfinder"


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _mp4() -> bytes:
    compatible = b"isom" + b"iso2" + b"mp41"
    payload = b"isom" + struct.pack(">I", 512) + compatible
    ftyp = struct.pack(">I", len(payload) + 8) + b"ftyp" + payload
    body = bytes(index % 251 for index in range(64 * 1024))
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


def _sampler(
    path: pathlib.Path,
    *,
    frame_count: int,
    jpeg_max_dimension: int,
    temporal_start_fraction: float,
    temporal_end_fraction: float,
) -> tuple[list[SampledImage], float]:
    del path, jpeg_max_dimension, temporal_start_fraction, temporal_end_fraction
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


class ChecksumFileLineEndingTest(unittest.TestCase):
    def test_a_frozen_catalog_checksum_file_uses_lf(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = pathlib.Path(name)
            payload = _mp4()
            source = root / "source.mp4"
            source.write_bytes(payload)
            raw = root / "raw"
            build_raw_cold_data_plane_package(
                [RawColdObjectBinding(
                    object_id="nextqa-val-3429509208",
                    artifact_path=source,
                    catalog_version="checksum-line-ending-v1",
                    plan_ids=("D0", "D1"),
                    dataset_id="nextqa",
                    dataset_revision="line-ending-v1",
                    source_object_id="3429509208",
                    artifact_sha256=_sha(payload),
                    artifact_size_bytes=len(payload),
                )],
                output_dir=raw,
                package_id="n3-raw-line-ending-v1",
            )
            indexed = root / "indexed"
            build_n3_indexed_data_plane_package(
                raw,
                output_dir=indexed,
                package_id="n3-indexed-line-ending-v1",
                sampler=_sampler,
            )
            catalog = root / "selections"
            build_full_flow_exact_range_catalog(
                indexed,
                catalog_id="line-ending-selection-v1",
                output_dir=catalog,
            )

            written = (catalog / CHECKSUMS_NAME).read_bytes()
            self.assertNotIn(b"\r", written)
            self.assertTrue(written.endswith(b"\n"))
            # The N3 package it was built from must be clean too.
            self.assertNotIn(b"\r", (indexed / "SHA256SUMS").read_bytes())

    def test_no_checksum_writer_opens_the_file_in_text_mode(self) -> None:
        """Guard the whole package, not just the freezers fixed so far."""

        offenders: list[str] = []
        for path in sorted(PACKAGE_ROOT.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                target = node.func
                if not isinstance(target, ast.Attribute):
                    continue
                if target.attr != "write_text":
                    continue
                if not _writes_a_checksum_file(target.value):
                    continue
                # Text mode is fine only when the newline is pinned.
                if any(kw.arg == "newline" for kw in node.keywords):
                    continue
                relative = path.relative_to(PACKAGE_ROOT.parent).as_posix()
                offenders.append(f"{relative}:{node.lineno}")
        self.assertEqual([], offenders, "checksum file written in text mode")


def _writes_a_checksum_file(destination: ast.expr) -> bool:
    """True when the write target is built from a CHECKSUM* name constant."""

    for node in ast.walk(destination):
        if isinstance(node, ast.Name) and "CHECKSUM" in node.id.upper():
            return True
        if isinstance(node, ast.Attribute) and "CHECKSUM" in node.attr.upper():
            return True
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if "SHA256SUMS" in node.value:
                return True
    return False


if __name__ == "__main__":
    unittest.main()
