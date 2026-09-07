"""Native download, validation, and telemetry reconciliation for bundles.

The fixtures build canonical bundles in memory. Nothing here needs a codec,
a real video, or a binary fixture checked into Git: the JPEGs are minimal
but structurally valid, because this layer verifies headers, sizes, and
digests rather than pixels.
"""

from __future__ import annotations

import io
import json
import os
import tarfile
import tempfile
import threading
import unittest
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Sequence
from unittest import mock

from pathfinder.data_agent_client import (
    DATA_AGENT_API_VERSION,
    DataAgentTelemetryUnsupportedError,
    DataAgentAccessRequest,
    DataAgentAccessTelemetry,
    DataAgentArtifactIntegrityError,
    DataAgentArtifactRedirectError,
    DataAgentArtifactSecurityError,
    DataAgentArtifactTooLargeError,
    DataAgentArtifactUnsupportedError,
    DataAgentClientSettings,
    DataAgentHTTPError,
    DataAgentProtocolError,
    HttpDataAgentClient,
)
from pathfinder.frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    REPRESENTATION_ID,
    deterministic_frame_bundle_tar,
)
from pathfinder.frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    FRAME_BUNDLE_MEDIA_TYPE,
    FrameBundleArchiveError,
    FrameBundleCanonicalizationError,
    FrameBundleIdentityError,
    FrameBundleIngestError,
    FrameBundleLimitError,
    FrameBundleLimits,
    FrameBundleManifestError,
    ValidatedFrameBundle,
    validate_frame_bundle_bytes,
)
from pathfinder.frame_bundle_transfer import (
    FrameBundleDeliveryError,
    FrameBundleTransferError,
    SMOKE_FAILURE_NAME,
    SMOKE_REPORT_NAME,
    build_frame_bundle_access_request,
    classify_frame_bundle_failure,
    fetch_validated_frame_bundle,
    run_frame_bundle_transfer_smoke,
    transfer_audit_of,
)

OBJECT_ID = "nextqa-val-0000000001"
ALIGNMENT_STATEMENT = (
    "These JPEG frames were regenerated from the same source video using "
    "the same sampling algorithm and are aligned with the frozen sampling "
    "metadata: identical frame count, frame indices, timestamps, widths, "
    "heights, and declared encoder settings. The JPEG bytes supplied to the "
    "historical description model were not retained, so this artifact does "
    "NOT claim byte identity with the historical visual input."
)


def make_jpeg(width: int, height: int, *, filler: bytes = b"\x11\x22") -> bytes:
    """A minimal, structurally valid JPEG carrying declared dimensions.

    Header segments only, plus a short scan. It is never decoded here: the
    ingestion layer reads the start-of-frame segment and refuses to
    reconstruct pixels, which is exactly what this fixture exercises.
    """
    app0 = (
        b"\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    )
    sof0 = (
        b"\xff\xc0\x00\x0b\x08"
        + height.to_bytes(2, "big")
        + width.to_bytes(2, "big")
        + b"\x01\x01\x11\x00"
    )
    sos = b"\xff\xda\x00\x08\x01\x01\x00\x00\x3f\x00"
    return b"\xff\xd8" + app0 + sof0 + sos + filler + b"\xff\xd9"


def frame_member(index: int) -> str:
    return f"frames/{index:03d}.jpg"


def tar_info(name: str, size: int, **overrides: Any) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name=name)
    info.size = size
    info.mtime = 0
    info.mode = 0o644
    info.type = tarfile.REGTYPE
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    for key, value in overrides.items():
        setattr(info, key, value)
    return info


def pack(
    members: Sequence[tuple[tarfile.TarInfo, bytes | None]],
    *,
    mode: str = "w",
) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(
        fileobj=buffer, mode=mode, format=tarfile.USTAR_FORMAT
    ) as archive:
        for info, payload in members:
            if payload is None:
                archive.addfile(info)
            else:
                archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


def canonical_manifest(
    frames: Sequence[bytes],
    *,
    object_id: str = OBJECT_ID,
    width: int = 32,
    height: int = 24,
) -> dict[str, Any]:
    entries = []
    for index, payload in enumerate(frames):
        entries.append({
            "frame_index": index,
            "timestamp_seconds": round(0.5 + index * 1.25, 6),
            "width": width,
            "height": height,
            "path": frame_member(index),
            "jpeg_size_bytes": len(payload),
            "jpeg_sha256": sha256(payload).hexdigest(),
        })
    return {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": REPRESENTATION_ID,
        "object_id": object_id,
        "source_video_id": "0000000001",
        "source_video_filename": "0000000001.mp4",
        "source_video_size_bytes": 123456,
        "source_video_sha256": "a" * 64,
        "source_duration_seconds": 42.5,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": len(frames),
            "jpeg_max_dimension": 768,
            "jpeg_quality": 82,
            "jpeg_optimize": True,
        },
        "source_frame_descriptions": {
            "representation_id": "sampled_frames",
            "path": f"{object_id}/sampled_frames.json",
            "sha256": "b" * 64,
        },
        "generation_manifest_sha256": "c" * 64,
        "frames": entries,
        "frame_count": len(entries),
        "total_jpeg_bytes": sum(len(payload) for payload in frames),
        "software_versions": {"av": "17.0.1", "Pillow": "12.3.0"},
        "historical_visual_bytes_retained": False,
        "sampling_alignment_statement": ALIGNMENT_STATEMENT,
        "claims_byte_identity_with_historical_visual_input": False,
        "credentials_recorded": False,
        "llm_called": False,
        "network_calls_performed": False,
    }


def manifest_bytes(manifest: dict[str, Any]) -> bytes:
    return (
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def build_bundle(
    *,
    object_id: str = OBJECT_ID,
    frame_count: int = 3,
    width: int = 32,
    height: int = 24,
    mutate_manifest: Callable[[dict[str, Any]], None] | None = None,
    mutate_members: Callable[
        [list[tuple[tarfile.TarInfo, bytes | None]]], None
    ] | None = None,
    raw_manifest: bytes | None = None,
    mode: str = "w",
) -> bytes:
    frames = [
        make_jpeg(width, height, filler=bytes([0x10 + index, 0x20]))
        for index in range(frame_count)
    ]
    manifest = canonical_manifest(
        frames, object_id=object_id, width=width, height=height
    )
    if mutate_manifest is not None:
        mutate_manifest(manifest)
    payload = (
        raw_manifest if raw_manifest is not None else manifest_bytes(manifest)
    )
    if mutate_members is None and mode == "w":
        # The canonical path goes through the generator's own serializer:
        # there is exactly one definition of a canonical bundle archive, and
        # the tests must not invent a second one.
        return deterministic_frame_bundle_tar(
            [(OBJECT_MANIFEST_NAME, payload)]
            + [
                (frame_member(index), frame)
                for index, frame in enumerate(frames)
            ]
        )
    members: list[tuple[tarfile.TarInfo, bytes | None]] = [
        (tar_info(OBJECT_MANIFEST_NAME, len(payload)), payload)
    ]
    for index, frame in enumerate(frames):
        members.append((tar_info(frame_member(index), len(frame)), frame))
    if mutate_members is not None:
        mutate_members(members)
    return pack(members, mode=mode)


def validate(raw: bytes, **kwargs: Any) -> ValidatedFrameBundle:
    kwargs.setdefault("expected_object_id", OBJECT_ID)
    return validate_frame_bundle_bytes(raw, **kwargs)


class BundleAcceptanceTest(unittest.TestCase):
    def test_a_canonical_bundle_is_accepted(self) -> None:
        raw = build_bundle()
        bundle = validate(raw)

        self.assertEqual(OBJECT_ID, bundle.object_id)
        self.assertEqual(REPRESENTATION_ID, bundle.representation_id)
        self.assertEqual(FRAME_BUNDLE_SCHEMA_VERSION, bundle.schema_version)
        self.assertEqual(3, bundle.frame_count)
        self.assertEqual(4, bundle.member_count)
        self.assertEqual(len(raw), bundle.artifact_size_bytes)
        self.assertEqual(sha256(raw).hexdigest(), bundle.artifact_sha256)
        self.assertEqual("uniform-midpoint", bundle.source.sampling_method)
        self.assertEqual(768, bundle.source.jpeg_max_dimension)

    def test_frames_are_ordered_and_byte_exact(self) -> None:
        raw = build_bundle(frame_count=4)
        bundle = validate(raw)

        self.assertEqual(
            [0, 1, 2, 3], [frame.frame_index for frame in bundle.frames]
        )
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
            for frame in bundle.frames:
                member = archive.extractfile(frame.member_path)
                assert member is not None
                self.assertEqual(member.read(), frame.jpeg_bytes)
        self.assertEqual(
            sum(frame.size_bytes for frame in bundle.frames),
            bundle.total_jpeg_bytes,
        )

    def test_exact_size_and_digest_are_enforced(self) -> None:
        raw = build_bundle()
        bundle = validate(
            raw,
            expected_sha256=sha256(raw).hexdigest(),
            expected_size_bytes=len(raw),
        )
        self.assertEqual(len(raw), bundle.artifact_size_bytes)

        with self.assertRaises(FrameBundleIdentityError):
            validate(raw, expected_sha256="d" * 64)
        with self.assertRaises(FrameBundleIdentityError):
            validate(raw, expected_size_bytes=len(raw) + 1)

    def test_the_ordered_vision_handoff_carries_verified_frames(self) -> None:
        bundle = validate(build_bundle(frame_count=3))
        frames = bundle.vision_frames()

        self.assertEqual(3, len(frames))
        self.assertEqual([0, 1, 2], [frame.frame_index for frame in frames])
        for handoff, validated in zip(frames, bundle.frames):
            self.assertEqual("image/jpeg", handoff.media_type)
            self.assertEqual(validated.jpeg_bytes, handoff.jpeg_bytes)
            self.assertEqual(validated.width, handoff.width)
            self.assertEqual(validated.height, handoff.height)
            self.assertEqual(
                validated.timestamp_seconds, handoff.timestamp_seconds
            )

    def test_no_method_renders_binary_content_into_agent_text(self) -> None:
        bundle = validate(build_bundle())
        # The Agent-facing failure this representation exists to avoid is a
        # tar or a base64 blob arriving in a tool response.
        self.assertFalse(hasattr(bundle, "to_agent_content"))
        summary = bundle.agent_visible_summary()
        rendered = json.dumps(summary)
        self.assertNotIn("\\u00ff", rendered)
        for frame in bundle.frames:
            self.assertNotIn(frame.jpeg_bytes.hex(), rendered)
        self.assertFalse(summary["pixel_decoding_performed"])
        self.assertFalse(
            summary["claims_byte_identity_with_historical_visual_input"]
        )
        for entry in summary["frames"]:
            self.assertNotIn("jpeg_bytes", entry)

    def test_the_bundle_value_is_immutable(self) -> None:
        bundle = validate(build_bundle())
        with self.assertRaises(Exception):
            bundle.object_id = "other"  # type: ignore[misc]
        self.assertIsInstance(bundle.frames, tuple)


class TarRejectionTest(unittest.TestCase):
    def assert_rejected(
        self,
        raw: bytes,
        error: type = FrameBundleIngestError,
        pattern: str | None = None,
        **kwargs: Any,
    ) -> None:
        if pattern is None:
            with self.assertRaises(error):
                validate(raw, **kwargs)
        else:
            with self.assertRaisesRegex(error, pattern):
                validate(raw, **kwargs)

    def test_an_absolute_member_path_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            members[1] = (tar_info("/etc/passwd", 3), b"abc")

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            "absolute|empty path component",
        )

    def test_a_traversal_member_path_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            members[1] = (tar_info("../escape.jpg", 3), b"abc")

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            "traverse upwards",
        )

    def test_a_backslash_member_path_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            members[1] = (tar_info("frames\\000.jpg", 3), b"abc")

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            "backslash",
        )

    def test_an_empty_path_component_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            members[1] = (tar_info("frames//000.jpg", 3), b"abc")

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            "empty path component",
        )

    def test_a_dot_component_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            members[1] = (tar_info("./frames/000.jpg", 3), b"abc")

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            r"'\.' component",
        )

    def test_a_duplicate_member_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            members.append(members[1])

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            "duplicate member|lexicographic order",
        )

    def test_non_regular_members_are_refused(self) -> None:
        cases = {
            "directory": tarfile.DIRTYPE,
            "symlink": tarfile.SYMTYPE,
            "hardlink": tarfile.LNKTYPE,
            "fifo": tarfile.FIFOTYPE,
            "chardev": tarfile.CHRTYPE,
            "blockdev": tarfile.BLKTYPE,
        }
        for label, member_type in cases.items():
            with self.subTest(member=label):
                def mutate(members: list[Any], t=member_type) -> None:
                    members.append(
                        (tar_info("zz-extra", 0, type=t, linkname="x"), None)
                    )

                self.assert_rejected(
                    build_bundle(mutate_members=mutate),
                    FrameBundleArchiveError,
                    "not a regular file",
                )

    def test_a_compressed_tar_is_refused(self) -> None:
        self.assert_rejected(
            build_bundle(mode="w:gz"),
            FrameBundleArchiveError,
            "uncompressed tar",
        )

    def test_noncanonical_member_order_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            members.reverse()

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            "lexicographic order",
        )

    def test_noncanonical_member_metadata_is_refused(self) -> None:
        cases = {
            "mtime": ({"mtime": 1_700_000_000}, "mtime=0"),
            "uid": ({"uid": 1000}, "uid=0 and gid=0"),
            "gid": ({"gid": 1000}, "uid=0 and gid=0"),
            "uname": ({"uname": "ruixuan"}, "empty uname and gname"),
            "gname": ({"gname": "staff"}, "empty uname and gname"),
            "mode": ({"mode": 0o777}, "mode 0644"),
        }
        for label, (overrides, pattern) in cases.items():
            with self.subTest(field=label):
                def mutate(members: list[Any], o=overrides) -> None:
                    info, payload = members[1]
                    assert payload is not None
                    members[1] = (
                        tar_info(info.name, info.size, **o),
                        payload,
                    )

                self.assert_rejected(
                    build_bundle(mutate_members=mutate),
                    FrameBundleArchiveError,
                    pattern,
                )

    def test_a_missing_embedded_manifest_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            del members[0]

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            f"no {OBJECT_MANIFEST_NAME} member",
        )

    def test_a_second_manifest_copy_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            info, payload = members[0]
            assert payload is not None
            members.append(
                (tar_info(f"frames/{OBJECT_MANIFEST_NAME}", len(payload)),
                 payload)
            )

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            "unexpected member",
        )

    def test_an_unexpected_member_is_refused(self) -> None:
        def mutate(members: list[Any]) -> None:
            members.append((tar_info("zz-README.txt", 2), b"hi"))

        self.assert_rejected(
            build_bundle(mutate_members=mutate),
            FrameBundleArchiveError,
            "unexpected member",
        )

    def test_an_empty_archive_is_refused(self) -> None:
        self.assert_rejected(
            pack([]), FrameBundleArchiveError, "contains no members"
        )

    def test_an_empty_payload_is_refused(self) -> None:
        self.assert_rejected(b"", FrameBundleArchiveError, "empty")

    def test_arbitrary_bytes_are_refused(self) -> None:
        self.assert_rejected(
            b"not a tar at all" * 40,
            FrameBundleArchiveError,
            "readable uncompressed tar",
        )

    def test_a_truncated_archive_is_refused(self) -> None:
        raw = build_bundle()
        self.assert_rejected(raw[: len(raw) // 2], FrameBundleIngestError)

    def test_a_frame_that_is_not_a_jpeg_is_refused(self) -> None:
        def mutate(manifest: dict[str, Any]) -> None:
            pass

        frames = [make_jpeg(32, 24), b"NOT-A-JPEG-AT-ALL"]
        manifest = canonical_manifest(frames)
        payload = manifest_bytes(manifest)
        members = [(tar_info(OBJECT_MANIFEST_NAME, len(payload)), payload)]
        for index, frame in enumerate(frames):
            members.append((tar_info(frame_member(index), len(frame)), frame))
        self.assert_rejected(
            pack(members),
            FrameBundleArchiveError,
            "start-of-image",
        )


BLOCK = 512
RECORD = 10240


def repair_checksum(header: bytes) -> bytes:
    """Recompute a USTAR header checksum after editing raw bytes.

    Without this, a hand-edited header is rejected for a bad checksum and the
    test would prove nothing about canonicalization.
    """
    blank = header[:148] + b" " * 8 + header[156:]
    total = sum(blank)
    return (
        header[:148] + ("%06o\0 " % total).encode("ascii") + header[156:]
    )


def edit_header(raw: bytes, block_index: int, offset: int, value: bytes) -> bytes:
    start = block_index * BLOCK
    header = bytearray(raw[start:start + BLOCK])
    header[offset:offset + len(value)] = value
    repaired = repair_checksum(bytes(header))
    return raw[:start] + repaired + raw[start + BLOCK:]


def repack(
    raw: bytes,
    *,
    fmt: int,
    pax_headers: dict[str, str] | None = None,
) -> bytes:
    """Re-serialize a canonical bundle in a different archive format.

    ``PAX_FORMAT`` alone is not enough to produce different bytes: tarfile
    only emits an extended header when a field cannot be expressed in a
    plain USTAR header, so a bundle with short ASCII names and small sizes
    round-trips byte-identically. ``pax_headers`` forces the extension that
    the canonical form must reject.
    """
    members: list[tuple[tarfile.TarInfo, bytes]] = []
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as source:
        for member in source.getmembers():
            stream = source.extractfile(member)
            assert stream is not None
            members.append((member, stream.read()))
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=fmt) as archive:
        for index, (info, payload) in enumerate(members):
            if pax_headers is not None and index == 0:
                info.pax_headers = dict(pax_headers)
            archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


class CanonicalArchiveTest(unittest.TestCase):
    """Raw bytes must be the generator's archive, not merely equivalent.

    ``tarfile`` shows a logical view: it follows PAX and GNU extensions,
    normalizes header fields, stops at the first end-of-archive marker, and
    ignores anything after it. Each case below is logically indistinguishable
    from a valid bundle through that view and must still be refused.
    """

    def setUp(self) -> None:
        self.raw = build_bundle(frame_count=2)

    def assert_not_canonical(self, raw: bytes) -> None:
        with self.assertRaises(FrameBundleCanonicalizationError):
            validate(raw)

    def assert_logically_equivalent(self, raw: bytes) -> None:
        """Confirm the case really does fool the logical member view."""
        def view(blob: bytes) -> list[tuple[str, bytes]]:
            with tarfile.open(fileobj=io.BytesIO(blob), mode="r:") as archive:
                result = []
                for member in archive.getmembers():
                    stream = archive.extractfile(member)
                    assert stream is not None
                    result.append((member.name, stream.read()))
                return result

        self.assertEqual(view(self.raw), view(raw))

    def test_the_generator_archive_is_accepted(self) -> None:
        bundle = validate(self.raw)
        self.assertEqual(2, bundle.frame_count)
        self.assertEqual(0, len(self.raw) % RECORD)

    def test_a_pax_extended_header_archive_is_refused(self) -> None:
        pax = repack(
            self.raw,
            fmt=tarfile.PAX_FORMAT,
            pax_headers={"comment": "smuggled metadata"},
        )
        self.assertNotEqual(self.raw, pax)
        self.assert_logically_equivalent(pax)
        self.assert_not_canonical(pax)

    def test_a_plain_pax_archive_matching_ustar_byte_for_byte_is_accepted(
        self,
    ) -> None:
        # Honest boundary: with no field needing an extension, PAX_FORMAT
        # emits exactly the canonical USTAR bytes. The rule enforced here is
        # about bytes, not about the writer's declared format constant, so
        # this case is accepted and the test says so plainly.
        plain = repack(self.raw, fmt=tarfile.PAX_FORMAT)
        self.assertEqual(self.raw, plain)
        self.assertEqual(2, validate(plain).frame_count)

    def test_a_gnu_archive_is_refused(self) -> None:
        gnu = repack(self.raw, fmt=tarfile.GNU_FORMAT)
        self.assertNotEqual(self.raw, gnu)
        self.assert_logically_equivalent(gnu)
        self.assert_not_canonical(gnu)

    def test_a_pax_extended_header_member_is_refused(self) -> None:
        for header_type in (tarfile.XHDTYPE, tarfile.XGLTYPE):
            with self.subTest(header=header_type):
                def mutate(members: list[Any], t=header_type) -> None:
                    members.insert(
                        0,
                        (
                            tar_info("PaxHeaders/0", 4, type=t),
                            b"junk",
                        ),
                    )

                with self.assertRaises(FrameBundleArchiveError):
                    validate(build_bundle(frame_count=2, mutate_members=mutate))

    def test_a_gnu_long_name_member_is_refused(self) -> None:
        long_name = "frames/" + "l" * 120 + ".jpg"
        buffer = io.BytesIO()
        with tarfile.open(
            fileobj=buffer, mode="w", format=tarfile.GNU_FORMAT
        ) as archive:
            with tarfile.open(
                fileobj=io.BytesIO(self.raw), mode="r:"
            ) as source:
                for member in source.getmembers():
                    stream = source.extractfile(member)
                    assert stream is not None
                    archive.addfile(member, io.BytesIO(stream.read()))
            archive.addfile(tar_info(long_name, 3), io.BytesIO(b"abc"))
        raw = buffer.getvalue()
        self.assertIn(b"@LongLink", raw)
        with self.assertRaises(FrameBundleArchiveError):
            validate(raw)

    def test_trailing_nonzero_data_is_refused(self) -> None:
        appended = self.raw + b"stowaway payload"
        self.assert_logically_equivalent(appended)
        self.assert_not_canonical(appended)

    def test_extra_record_padding_is_refused(self) -> None:
        padded = self.raw + b"\0" * RECORD
        self.assert_logically_equivalent(padded)
        self.assert_not_canonical(padded)

    def test_a_concatenated_archive_is_refused(self) -> None:
        concatenated = self.raw + build_bundle(
            frame_count=2, object_id="nextqa-val-0000000002"
        )
        self.assert_logically_equivalent(concatenated)
        self.assert_not_canonical(concatenated)

    def test_altered_end_of_archive_blocks_are_refused(self) -> None:
        # Two zero blocks mark the end. Replace the trailing padding so the
        # archive still parses but no longer matches byte for byte.
        trimmed = self.raw.rstrip(b"\0")
        blocks = -(-len(trimmed) // BLOCK) + 2
        shortened = self.raw[: blocks * BLOCK]
        self.assertNotEqual(self.raw, shortened)
        self.assert_logically_equivalent(shortened)
        self.assert_not_canonical(shortened)

    def test_a_header_field_tarfile_normalizes_away_is_refused(self) -> None:
        # uname is 32 bytes at offset 265 and is empty in a canonical bundle.
        # tarfile reads up to the first NUL, so bytes after it are invisible
        # in the logical view -- and are exactly the kind of smuggled content
        # a byte-level comparison exists to catch.
        forged = edit_header(self.raw, 0, 266, b"smuggled")
        self.assertNotEqual(self.raw, forged)
        with tarfile.open(fileobj=io.BytesIO(forged), mode="r:") as archive:
            self.assertEqual("", archive.getmembers()[0].uname)
        self.assert_logically_equivalent(forged)
        self.assert_not_canonical(forged)

    def test_a_normalized_numeric_field_is_refused(self) -> None:
        # devmajor/devminor are all-NUL in a canonical bundle; tarfile treats
        # an octal-zero encoding there as the same value.
        forged = edit_header(self.raw, 0, 329, b"0000000\0")
        self.assertNotEqual(self.raw, forged)
        self.assert_logically_equivalent(forged)
        self.assert_not_canonical(forged)

    def test_canonicalization_maps_to_its_own_failure_class(self) -> None:
        self.assertEqual(
            "bundle_not_canonical",
            classify_frame_bundle_failure(
                FrameBundleCanonicalizationError("x")
            ),
        )


class ManifestRejectionTest(unittest.TestCase):
    def assert_rejected(
        self,
        pattern: str,
        error: type = FrameBundleManifestError,
        **build_kwargs: Any,
    ) -> None:
        with self.assertRaisesRegex(error, pattern):
            validate(build_bundle(**build_kwargs))

    def mutation(
        self, mutate: Callable[[dict[str, Any]], None]
    ) -> dict[str, Any]:
        return {"mutate_manifest": mutate}

    def test_malformed_manifest_json_is_refused(self) -> None:
        self.assert_rejected(
            "not valid UTF-8 JSON",
            raw_manifest=b"{not json",
        )

    def test_a_non_object_manifest_is_refused(self) -> None:
        self.assert_rejected(
            "must be a JSON object",
            raw_manifest=b"[1, 2, 3]",
        )

    def test_an_unsupported_schema_version_is_refused(self) -> None:
        self.assert_rejected(
            "unsupported frame bundle schema_version",
            **self.mutation(
                lambda m: m.__setitem__(
                    "schema_version", "pathfinder.sampled-frame-bundle/v9.9"
                )
            ),
        )

    def test_a_wrong_representation_id_is_refused(self) -> None:
        self.assert_rejected(
            "representation_id must be",
            **self.mutation(
                lambda m: m.__setitem__("representation_id", "sampled_frames")
            ),
        )

    def test_a_wrong_object_id_is_refused(self) -> None:
        self.assert_rejected(
            "expected",
            FrameBundleIdentityError,
            **self.mutation(
                lambda m: m.__setitem__("object_id", "some-other-object")
            ),
        )

    def test_a_missing_manifest_field_is_refused(self) -> None:
        self.assert_rejected(
            "missing required field",
            **self.mutation(lambda m: m.pop("total_jpeg_bytes")),
        )

    def test_an_unexpected_manifest_field_is_refused(self) -> None:
        self.assert_rejected(
            "unexpected field",
            **self.mutation(lambda m: m.__setitem__("extra_claim", True)),
        )

    def test_a_missing_frame_member_is_refused(self) -> None:
        def drop_last_member(members: list[Any]) -> None:
            del members[-1]

        with self.assertRaisesRegex(
            FrameBundleManifestError, "frame member"
        ):
            validate(build_bundle(mutate_members=drop_last_member))

    def test_a_duplicate_frame_index_is_refused(self) -> None:
        self.assert_rejected(
            "is duplicated",
            **self.mutation(
                lambda m: m["frames"][1].__setitem__("frame_index", 0)
            ),
        )

    def test_a_noncanonical_frame_index_is_refused(self) -> None:
        self.assert_rejected(
            "not canonical",
            **self.mutation(
                lambda m: m["frames"][2].__setitem__("frame_index", 5)
            ),
        )

    def test_a_frame_path_mismatch_is_refused(self) -> None:
        self.assert_rejected(
            r"\.path is",
            **self.mutation(
                lambda m: m["frames"][1].__setitem__(
                    "path", "frames/002.jpg"
                )
            ),
        )

    def test_a_frame_size_mismatch_is_refused(self) -> None:
        self.assert_rejected(
            "jpeg_size_bytes",
            **self.mutation(
                lambda m: m["frames"][0].__setitem__("jpeg_size_bytes", 7)
            ),
        )

    def test_a_frame_digest_mismatch_is_refused(self) -> None:
        self.assert_rejected(
            "does not match the member bytes",
            **self.mutation(
                lambda m: m["frames"][0].__setitem__("jpeg_sha256", "e" * 64)
            ),
        )

    def test_a_total_byte_mismatch_is_refused(self) -> None:
        self.assert_rejected(
            "total_jpeg_bytes",
            **self.mutation(
                lambda m: m.__setitem__("total_jpeg_bytes", 999_999)
            ),
        )

    def test_a_frame_count_mismatch_is_refused(self) -> None:
        self.assert_rejected(
            "frame_count",
            **self.mutation(lambda m: m.__setitem__("frame_count", 99)),
        )

    def test_invalid_timestamps_are_refused(self) -> None:
        for label, value in (
            ("negative", -1.0),
            ("nan", float("nan")),
            ("infinite", float("inf")),
            ("string", "0.5"),
            ("boolean", True),
            ("null", None),
        ):
            with self.subTest(timestamp=label):
                self.assert_rejected(
                    "timestamp_seconds",
                    **self.mutation(
                        lambda m, v=value: m["frames"][0].__setitem__(
                            "timestamp_seconds", v
                        )
                    ),
                )

    def test_invalid_dimensions_are_refused(self) -> None:
        for field in ("width", "height"):
            for label, value in (
                ("zero", 0),
                ("negative", -8),
                ("float", 32.0),
                ("boolean", True),
                ("string", "32"),
            ):
                with self.subTest(field=field, value=label):
                    self.assert_rejected(
                        field,
                        **self.mutation(
                            lambda m, f=field, v=value: m["frames"][
                                0
                            ].__setitem__(f, v)
                        ),
                    )

    def test_dimensions_disagreeing_with_the_jpeg_are_refused(self) -> None:
        self.assert_rejected(
            "the JPEG itself",
            **self.mutation(
                lambda m: m["frames"][0].__setitem__("width", 31)
            ),
        )

    def test_required_booleans_reject_truthy_substitutes(self) -> None:
        fields = (
            "historical_visual_bytes_retained",
            "claims_byte_identity_with_historical_visual_input",
            "llm_called",
            "credentials_recorded",
            "network_calls_performed",
        )
        substitutes = (0, 1, "false", "true", None, [], True)
        for field in fields:
            for value in substitutes:
                with self.subTest(field=field, value=repr(value)):
                    self.assert_rejected(
                        "literal boolean false",
                        **self.mutation(
                            lambda m, f=field, v=value: m.__setitem__(f, v)
                        ),
                    )

    def test_an_alignment_statement_without_the_disclaimer_is_refused(
        self,
    ) -> None:
        self.assert_rejected(
            "sampling_alignment_statement",
            **self.mutation(
                lambda m: m.__setitem__(
                    "sampling_alignment_statement",
                    "These frames are aligned with the frozen sampling "
                    "metadata.",
                )
            ),
        )

    def test_an_alignment_statement_claiming_identity_is_refused(
        self,
    ) -> None:
        self.assert_rejected(
            "sampling_alignment_statement",
            **self.mutation(
                lambda m: m.__setitem__(
                    "sampling_alignment_statement",
                    "These JPEG frames are identical to the historical "
                    "visual input.",
                )
            ),
        )

    def test_invalid_source_metadata_is_refused(self) -> None:
        cases = (
            ("source_video_sha256", "not-a-digest"),
            ("source_video_size_bytes", 0),
            ("source_duration_seconds", float("nan")),
            ("source_video_filename", ""),
            ("generation_manifest_sha256", "C" * 64),
            ("software_versions", ["av"]),
            ("sampling", {"method": "x"}),
        )
        for field, value in cases:
            with self.subTest(field=field):
                self.assert_rejected(
                    field.split(".")[0],
                    **self.mutation(
                        lambda m, f=field, v=value: m.__setitem__(f, v)
                    ),
                )

    def test_a_wrong_source_representation_id_is_refused(self) -> None:
        self.assert_rejected(
            "source_frame_descriptions.representation_id",
            **self.mutation(
                lambda m: m["source_frame_descriptions"].__setitem__(
                    "representation_id", "sampled_frame_bundle"
                )
            ),
        )


class LimitTest(unittest.TestCase):
    def assert_limited(self, limits: FrameBundleLimits, **kwargs: Any) -> None:
        with self.assertRaises(FrameBundleLimitError):
            validate(build_bundle(**kwargs), limits=limits)

    def test_the_member_count_bound_is_enforced(self) -> None:
        self.assert_limited(
            FrameBundleLimits(max_member_count=2), frame_count=4
        )

    def test_the_frame_count_bound_is_enforced(self) -> None:
        self.assert_limited(
            FrameBundleLimits(max_frame_count=2), frame_count=4
        )

    def test_the_frame_byte_bound_is_enforced(self) -> None:
        self.assert_limited(FrameBundleLimits(max_frame_bytes=8))

    def test_the_total_contained_byte_bound_is_enforced(self) -> None:
        self.assert_limited(
            FrameBundleLimits(max_total_contained_bytes=64)
        )

    def test_the_manifest_byte_bound_is_enforced(self) -> None:
        self.assert_limited(FrameBundleLimits(max_manifest_bytes=32))

    def test_the_artifact_byte_bound_is_enforced(self) -> None:
        raw = build_bundle()
        with self.assertRaises(FrameBundleLimitError):
            validate(
                raw,
                limits=FrameBundleLimits(max_artifact_bytes=len(raw) - 1),
            )

    def test_the_frame_dimension_bound_is_enforced(self) -> None:
        self.assert_limited(FrameBundleLimits(max_frame_dimension=16))

    def test_limits_reject_nonpositive_and_boolean_values(self) -> None:
        for field in DEFAULT_FRAME_BUNDLE_LIMITS.to_dict():
            for value in (0, -1, True):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        FrameBundleLimits(**{field: value})


class FakeHTTPResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self, amount: int | None = None) -> bytes:
        return self._body if amount is None else self._body[:amount]

    def __enter__(self) -> "FakeHTTPResponse":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


class ArtifactHTTPResponse:
    def __init__(
        self,
        body: bytes,
        media_type: str,
        *,
        declared_length: int | None = None,
        status: int = 200,
    ) -> None:
        self._body = body
        self.status = status
        self.headers = {
            "Content-Type": media_type,
            "Content-Length": str(
                len(body) if declared_length is None else declared_length
            ),
        }

    def read(self, amount: int | None = None) -> bytes:
        return self._body if amount is None else self._body[:amount]

    def __enter__(self) -> "ArtifactHTTPResponse":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


class RecordingArtifactOpener:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.requests: list[Any] = []

    def __call__(self, request: Any, timeout: float | None = None) -> Any:
        self.requests.append(request)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class BinaryDownloadSecurityTest(unittest.TestCase):
    """The binary path must inherit the artifact trust boundary intact."""

    ACCESS_ID = "frame-bundle-access"
    BASE_URL = "http://data-agent.test"
    SIGNED_QUERY = "?expires=9999999999&signature=do-not-expose"

    def request(self) -> DataAgentAccessRequest:
        return build_frame_bundle_access_request(
            object_id=OBJECT_ID,
            plan_id="D_bundle",
            requested_location="node-1/nvme",
            session_id="session-1",
            trial_id="trial-1",
            access_id=self.ACCESS_ID,
        )

    def build(
        self,
        body: bytes,
        *,
        declared_media_type: str = FRAME_BUNDLE_MEDIA_TYPE,
        artifact_url: str | None = None,
        artifact_response: Any | None = None,
        max_artifact_bytes: int = 1024 * 1024,
        declared_digest: str | None = None,
        response_media_type: str | None = None,
        token: str | None = "test-token",
    ) -> tuple[HttpDataAgentClient, RecordingArtifactOpener]:
        signed_url = artifact_url or (
            f"{self.BASE_URL}/v1/artifacts/{self.ACCESS_ID}"
            + self.SIGNED_QUERY
        )
        access_response = {
            "api_version": DATA_AGENT_API_VERSION,
            "status": "succeeded",
            "access_id": self.ACCESS_ID,
            "object_id": OBJECT_ID,
            "object_catalog_version": "test-catalog-v1",
            "payload": {
                "kind": "artifact_uri",
                "media_type": declared_media_type,
                "value": signed_url,
                "sha256": declared_digest or sha256(body).hexdigest(),
            },
            "telemetry": {
                "service_latency_ms": 2.0,
                "realized_cost": 0.5,
                "bytes_read": len(body),
                "location": "node-1/nvme",
            },
        }
        opener = RecordingArtifactOpener(
            artifact_response
            if artifact_response is not None
            else ArtifactHTTPResponse(
                body, response_media_type or declared_media_type
            )
        )
        client = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url=self.BASE_URL,
                token=token,
                timeout_seconds=2.0,
                max_retries=0,
                max_artifact_bytes=max_artifact_bytes,
            ),
            opener=lambda request, timeout=None: FakeHTTPResponse(
                json.dumps(access_response).encode("utf-8")
            ),
            artifact_opener=opener,
            sleep=lambda _seconds: None,
        )
        return client, opener

    def fetch(self, client: HttpDataAgentClient) -> Any:
        return client.fetch_binary_artifact(
            self.request(),
            allowed_media_types=frozenset({FRAME_BUNDLE_MEDIA_TYPE}),
        )

    def test_a_bounded_tar_is_downloaded_and_verified(self) -> None:
        raw = build_bundle()
        client, opener = self.build(raw)

        artifact = self.fetch(client)

        self.assertEqual(raw, artifact.data)
        self.assertEqual(len(raw), artifact.size_bytes)
        self.assertEqual(sha256(raw).hexdigest(), artifact.sha256)
        self.assertEqual(FRAME_BUNDLE_MEDIA_TYPE, artifact.media_type)
        self.assertEqual(OBJECT_ID, artifact.object_id)
        self.assertEqual("test-catalog-v1", artifact.object_catalog_version)
        self.assertEqual("node-1/nvme", artifact.location)
        self.assertIsNotNone(artifact.download_elapsed_ms)
        self.assertFalse(hasattr(artifact, "url"))
        # The bounded read is what caps the body, not a trusting read().
        self.assertNotIn(
            "do-not-expose", json.dumps(artifact.to_metadata_dict())
        )
        self.assertEqual(
            FRAME_BUNDLE_MEDIA_TYPE,
            opener.requests[0].get_header("Accept"),
        )

    def test_redirects_are_refused_without_exposing_the_signed_url(
        self,
    ) -> None:
        signed_url = (
            f"{self.BASE_URL}/v1/artifacts/{self.ACCESS_ID}"
            "?signature=secret-value"
        )
        client, _ = self.build(
            b"body",
            artifact_url=signed_url,
            artifact_response=DataAgentHTTPErrorFactory.redirect(signed_url),
        )
        with self.assertRaises(DataAgentArtifactRedirectError) as context:
            self.fetch(client)
        self.assertNotIn("secret-value", str(context.exception))
        self.assertNotIn("attacker.invalid", str(context.exception))

    def test_a_different_origin_artifact_url_is_refused(self) -> None:
        client, opener = self.build(
            b"body",
            artifact_url=(
                f"http://attacker.invalid/v1/artifacts/{self.ACCESS_ID}"
            ),
        )
        with self.assertRaises(DataAgentArtifactSecurityError):
            self.fetch(client)
        self.assertEqual([], opener.requests)

    def test_a_wrong_artifact_route_is_refused(self) -> None:
        client, opener = self.build(
            b"body",
            artifact_url=f"{self.BASE_URL}/v1/artifacts/other-access",
        )
        with self.assertRaises(DataAgentArtifactSecurityError):
            self.fetch(client)
        self.assertEqual([], opener.requests)

    def test_declared_and_streamed_oversize_are_both_refused(self) -> None:
        cases = {
            "declared": ArtifactHTTPResponse(
                b"x" * 8,
                FRAME_BUNDLE_MEDIA_TYPE,
                declared_length=10_000,
            ),
            "streamed": ArtifactHTTPResponse(
                b"x" * 17,
                FRAME_BUNDLE_MEDIA_TYPE,
                declared_length=16,
            ),
        }
        for label, response in cases.items():
            with self.subTest(label):
                client, _ = self.build(
                    b"x" * 8,
                    artifact_response=response,
                    max_artifact_bytes=16,
                )
                with self.assertRaises(DataAgentArtifactTooLargeError):
                    self.fetch(client)

    def test_a_disallowed_declared_media_type_is_refused_before_download(
        self,
    ) -> None:
        client, opener = self.build(
            b"body", declared_media_type="application/octet-stream"
        )
        with self.assertRaises(DataAgentArtifactUnsupportedError) as context:
            self.fetch(client)
        self.assertIn("application/octet-stream", str(context.exception))
        self.assertEqual([], opener.requests)

    def test_a_response_media_type_that_contradicts_the_access_is_refused(
        self,
    ) -> None:
        raw = build_bundle()
        client, _ = self.build(
            raw,
            artifact_response=ArtifactHTTPResponse(raw, "application/json"),
        )
        with self.assertRaises(DataAgentProtocolError):
            self.fetch(client)

    def test_a_payload_digest_mismatch_is_refused(self) -> None:
        raw = build_bundle()
        client, _ = self.build(raw, declared_digest="f" * 64)
        with self.assertRaises(DataAgentArtifactIntegrityError):
            self.fetch(client)

    def test_a_truncated_body_is_refused_by_the_digest_check(self) -> None:
        raw = build_bundle()
        client, _ = self.build(
            raw,
            artifact_response=ArtifactHTTPResponse(
                raw[: len(raw) // 2],
                FRAME_BUNDLE_MEDIA_TYPE,
                declared_length=len(raw),
            ),
        )
        with self.assertRaises(DataAgentArtifactIntegrityError):
            self.fetch(client)

    def test_a_non_200_artifact_response_is_refused(self) -> None:
        raw = build_bundle()
        client, _ = self.build(
            raw,
            artifact_response=ArtifactHTTPResponse(
                raw, FRAME_BUNDLE_MEDIA_TYPE, status=206
            ),
        )
        with self.assertRaises(DataAgentProtocolError):
            self.fetch(client)

    def test_the_allow_list_must_be_an_explicit_nonempty_set(self) -> None:
        client, _ = self.build(build_bundle())
        for value in (
            frozenset(),
            set(),
            (),
            "application/x-tar",
            ("*/*",),
            ("application/*",),
            ("",),
            (None,),
        ):
            with self.subTest(allowed=repr(value)):
                with self.assertRaises(ValueError):
                    client.fetch_binary_artifact(
                        self.request(), allowed_media_types=value
                    )

    def test_the_agent_facing_fetch_still_refuses_a_tar(self) -> None:
        raw = build_bundle()
        client, _ = self.build(raw)
        with self.assertRaises(DataAgentArtifactUnsupportedError) as context:
            client.fetch_artifact(self.request())
        self.assertIn(FRAME_BUNDLE_MEDIA_TYPE, str(context.exception))


class DataAgentHTTPErrorFactory:
    @staticmethod
    def redirect(url: str) -> Exception:
        from urllib.error import HTTPError

        return HTTPError(
            url,
            302,
            "Found",
            {"Location": "https://attacker.invalid/steal"},
            io.BytesIO(b""),
        )


class FakeSettings:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url


class FakeBundleClient:
    """A client stub for the reconciliation layer only."""

    def __init__(
        self,
        artifact: Any,
        telemetry: Any,
        *,
        base_url: str = "http://data-agent.test",
    ) -> None:
        self.artifact = artifact
        self.telemetry = telemetry
        self.settings = FakeSettings(base_url)
        self.allowed_media_types: Any = None
        self.quiescence_waits: list[bool] = []

    def fetch_binary_artifact(
        self,
        request: DataAgentAccessRequest,
        *,
        allowed_media_types: Any,
        on_phase: Any = None,
    ) -> Any:
        self.allowed_media_types = allowed_media_types
        if on_phase is not None:
            on_phase("access_completed")
        if isinstance(self.artifact, Exception):
            # Modelled as a pre-transfer refusal: no download was started.
            raise self.artifact
        if on_phase is not None:
            on_phase("artifact_download_started")
            on_phase("artifact_download_completed")
        return self.artifact

    def get_access_telemetry(
        self,
        access_id: str,
        *,
        wait_for_quiescence: bool = False,
        quiescence_timeout_seconds: float = 5.0,
    ) -> Any:
        self.quiescence_waits.append(wait_for_quiescence)
        if isinstance(self.telemetry, Exception):
            raise self.telemetry
        return self.telemetry


def binary_artifact(raw: bytes, **overrides: Any) -> Any:
    from pathfinder.data_agent_client import DataAgentBinaryArtifact

    fields: dict[str, Any] = {
        "access_id": "frame-bundle-access",
        "media_type": FRAME_BUNDLE_MEDIA_TYPE,
        "data": raw,
        "size_bytes": len(raw),
        "sha256": sha256(raw).hexdigest(),
        "object_id": OBJECT_ID,
        "object_catalog_version": "test-catalog-v1",
        "location": "node-1/nvme",
        "service_latency_ms": 3.0,
        "client_round_trip_ms": 5.0,
        "download_elapsed_ms": 1.0,
    }
    fields.update(overrides)
    return DataAgentBinaryArtifact(**fields)


def telemetry_for(size_bytes: int, **overrides: Any) -> DataAgentAccessTelemetry:
    fields: dict[str, Any] = {
        "access_id": "frame-bundle-access",
        "object_id": OBJECT_ID,
        "representation_id": REPRESENTATION_ID,
        "object_catalog_version": "test-catalog-v1",
        "download_request_count": 1,
        "completed_request_count": 1,
        "full_download_count": 1,
        "bytes_sent": size_bytes,
        "transfer_latency_ms": 2.5,
        "latest_completed_at": 1.0,
        "in_flight_request_count": 0,
        "server_reported_complete": True,
    }
    fields.update(overrides)
    return DataAgentAccessTelemetry(**fields)


class TelemetryReconciliationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = build_bundle()
        self.request = build_frame_bundle_access_request(
            object_id=OBJECT_ID,
            plan_id="D_bundle",
            requested_location="node-1/nvme",
            session_id="session-1",
            trial_id="trial-1",
            access_id="frame-bundle-access",
        )

    def run_transfer(
        self,
        *,
        artifact: Any = None,
        telemetry: Any = None,
        **kwargs: Any,
    ) -> Any:
        client = FakeBundleClient(
            artifact if artifact is not None else binary_artifact(self.raw),
            telemetry
            if telemetry is not None
            else telemetry_for(len(self.raw)),
        )
        self.client = client
        return fetch_validated_frame_bundle(client, self.request, **kwargs)

    def assert_failure_class(self, expected: str, **kwargs: Any) -> None:
        with self.assertRaises(FrameBundleDeliveryError) as context:
            self.run_transfer(**kwargs)
        self.assertEqual(expected, context.exception.failure_class)

    def test_a_complete_delivery_is_accepted(self) -> None:
        transfer = self.run_transfer(
            expected_artifact_sha256=sha256(self.raw).hexdigest(),
            expected_artifact_size_bytes=len(self.raw),
            expected_object_catalog_version="test-catalog-v1",
        )

        self.assertEqual(OBJECT_ID, transfer.object_id)
        self.assertEqual(3, transfer.bundle.frame_count)
        self.assertTrue(transfer.delivery.telemetry_complete)
        self.assertEqual(0, transfer.delivery.in_flight_request_count)
        self.assertTrue(transfer.delivery.exactly_one_full_download)
        self.assertTrue(transfer.delivery.bytes_sent_equals_artifact_size)
        self.assertEqual([True], self.client.quiescence_waits)
        self.assertEqual(
            frozenset({FRAME_BUNDLE_MEDIA_TYPE}),
            self.client.allowed_media_types,
        )
        self.assertEqual(
            {
                "data_agent_service",
                "client_access_round_trip",
                "artifact_download_elapsed",
                "server_reported_transfer",
            },
            set(transfer.latency_ms()),
        )

    def test_more_than_one_download_is_reported_but_not_refused(self) -> None:
        transfer = self.run_transfer(
            telemetry=telemetry_for(
                len(self.raw) * 2,
                download_request_count=2,
                completed_request_count=2,
                full_download_count=2,
            )
        )
        self.assertFalse(transfer.delivery.exactly_one_full_download)
        self.assertFalse(transfer.delivery.bytes_sent_equals_artifact_size)

    def test_unsupported_completeness_fields_are_refused(self) -> None:
        for label, overrides in (
            ("counter", {"in_flight_request_count": None}),
            ("verdict", {"server_reported_complete": None}),
            (
                "both",
                {
                    "in_flight_request_count": None,
                    "server_reported_complete": None,
                },
            ),
        ):
            with self.subTest(missing=label):
                self.assert_failure_class(
                    "telemetry_unsupported",
                    telemetry=telemetry_for(len(self.raw), **overrides),
                )

    def test_an_incomplete_verdict_is_refused(self) -> None:
        self.assert_failure_class(
            "telemetry_incomplete",
            telemetry=telemetry_for(
                len(self.raw), server_reported_complete=False
            ),
        )

    def test_a_nonzero_in_flight_count_is_refused(self) -> None:
        self.assert_failure_class(
            "telemetry_not_quiescent",
            telemetry=telemetry_for(
                len(self.raw), in_flight_request_count=1
            ),
        )

    def test_zero_download_requests_are_refused(self) -> None:
        self.assert_failure_class(
            "no_download_recorded",
            telemetry=telemetry_for(
                len(self.raw),
                download_request_count=0,
                completed_request_count=0,
                full_download_count=0,
            ),
        )

    def test_an_incomplete_request_is_refused(self) -> None:
        self.assert_failure_class(
            "no_completed_request",
            telemetry=telemetry_for(
                len(self.raw),
                completed_request_count=0,
                full_download_count=0,
            ),
        )

    def test_a_partial_only_download_is_refused(self) -> None:
        self.assert_failure_class(
            "partial_download_only",
            telemetry=telemetry_for(len(self.raw), full_download_count=0),
        )

    def test_a_byte_count_below_the_artifact_is_refused(self) -> None:
        self.assert_failure_class(
            "bytes_sent_below_artifact_size",
            telemetry=telemetry_for(len(self.raw) - 1),
        )

    def test_a_telemetry_object_mismatch_is_refused(self) -> None:
        self.assert_failure_class(
            "object_id_mismatch",
            telemetry=telemetry_for(len(self.raw), object_id="other-object"),
        )

    def test_a_telemetry_representation_mismatch_is_refused(self) -> None:
        self.assert_failure_class(
            "representation_id_mismatch",
            telemetry=telemetry_for(
                len(self.raw), representation_id="sampled_frames"
            ),
        )

    def test_a_catalog_version_mismatch_is_refused(self) -> None:
        self.assert_failure_class(
            "catalog_version_mismatch",
            telemetry=telemetry_for(
                len(self.raw), object_catalog_version="other-catalog"
            ),
            expected_object_catalog_version="test-catalog-v1",
        )

    def test_an_access_level_object_mismatch_is_refused(self) -> None:
        self.assert_failure_class(
            "object_id_mismatch",
            artifact=binary_artifact(self.raw, object_id="other-object"),
        )

    def test_an_access_level_catalog_mismatch_is_refused(self) -> None:
        self.assert_failure_class(
            "catalog_version_mismatch",
            artifact=binary_artifact(
                self.raw, object_catalog_version="other-catalog"
            ),
            expected_object_catalog_version="test-catalog-v1",
        )

    def test_a_malformed_bundle_still_reconciles_telemetry_for_audit(
        self,
    ) -> None:
        junk = b"not a tar" * 30
        client = FakeBundleClient(
            binary_artifact(junk), telemetry_for(len(junk))
        )
        with self.assertRaises(FrameBundleArchiveError) as context:
            fetch_validated_frame_bundle(client, self.request)

        # The bundle failure stays primary; telemetry is secondary evidence.
        audit = transfer_audit_of(context.exception)
        self.assertIsNotNone(audit)
        self.assertEqual("bundle_archive_invalid", audit.primary_failure_class)
        self.assertEqual([True], client.quiescence_waits)
        phase = audit.execution_phase
        self.assertTrue(phase.access_completed)
        self.assertTrue(phase.artifact_download_completed)
        self.assertFalse(phase.bundle_validation_completed)
        self.assertTrue(phase.telemetry_reconciliation_attempted)
        evidence = audit.telemetry_reconciliation
        self.assertEqual("complete", evidence.status)
        self.assertEqual(len(junk), evidence.bytes_sent)
        self.assertEqual(1, evidence.full_download_count)
        self.assertEqual(1, evidence.download_request_count)

    def test_a_failure_before_any_transfer_does_not_call_telemetry(
        self,
    ) -> None:
        refused = DataAgentArtifactUnsupportedError(
            "Data Agent artifact media type is not permitted"
        )
        client = FakeBundleClient(refused, telemetry_for(0))
        with self.assertRaises(DataAgentArtifactUnsupportedError) as context:
            fetch_validated_frame_bundle(client, self.request)

        self.assertEqual([], client.quiescence_waits)
        audit = transfer_audit_of(context.exception)
        self.assertEqual(
            "artifact_media_type_rejected", audit.primary_failure_class
        )
        self.assertFalse(
            audit.execution_phase.artifact_download_started
        )
        self.assertFalse(
            audit.execution_phase.telemetry_reconciliation_attempted
        )
        self.assertEqual(
            "not_attempted", audit.telemetry_reconciliation.status
        )
        self.assertIsNone(audit.telemetry_reconciliation.bytes_sent)

    def test_a_secondary_telemetry_failure_never_displaces_the_primary(
        self,
    ) -> None:
        junk = b"not a tar" * 30
        client = FakeBundleClient(
            binary_artifact(junk),
            DataAgentTelemetryUnsupportedError(
                "frame-bundle-access", ("telemetry_complete",)
            ),
        )
        with self.assertRaises(FrameBundleArchiveError) as context:
            fetch_validated_frame_bundle(client, self.request)

        audit = transfer_audit_of(context.exception)
        self.assertEqual("bundle_archive_invalid", audit.primary_failure_class)
        evidence = audit.telemetry_reconciliation
        self.assertEqual("unavailable", evidence.status)
        self.assertEqual(
            "DataAgentTelemetryUnsupportedError", evidence.error_class
        )
        # Counters are absent, not zero: nothing was reported.
        for counter in (
            evidence.bytes_sent,
            evidence.download_request_count,
            evidence.full_download_count,
            evidence.completed_request_count,
            evidence.telemetry_complete,
        ):
            self.assertIsNone(counter)

    def test_incomplete_telemetry_is_recorded_as_incomplete_evidence(
        self,
    ) -> None:
        junk = b"not a tar" * 30
        client = FakeBundleClient(
            binary_artifact(junk),
            telemetry_for(len(junk), server_reported_complete=False),
        )
        with self.assertRaises(FrameBundleArchiveError) as context:
            fetch_validated_frame_bundle(client, self.request)
        evidence = transfer_audit_of(
            context.exception
        ).telemetry_reconciliation
        self.assertEqual("incomplete", evidence.status)
        self.assertFalse(evidence.telemetry_complete)
        self.assertEqual(len(junk), evidence.bytes_sent)

    def test_a_delivery_failure_carries_the_reconciled_counters(self) -> None:
        with self.assertRaises(FrameBundleDeliveryError) as context:
            self.run_transfer(
                telemetry=telemetry_for(len(self.raw), full_download_count=0)
            )
        audit = transfer_audit_of(context.exception)
        self.assertEqual("partial_download_only", audit.primary_failure_class)
        self.assertTrue(audit.execution_phase.bundle_validation_completed)
        self.assertEqual(
            "complete", audit.telemetry_reconciliation.status
        )
        self.assertEqual(
            0, audit.telemetry_reconciliation.full_download_count
        )

    def test_the_audit_record_is_deterministic_and_json_safe(self) -> None:
        junk = b"not a tar" * 30
        client = FakeBundleClient(
            binary_artifact(junk), telemetry_for(len(junk))
        )
        with self.assertRaises(FrameBundleArchiveError) as context:
            fetch_validated_frame_bundle(client, self.request)
        payload = transfer_audit_of(context.exception).to_dict()
        self.assertEqual(
            json.dumps(payload, sort_keys=True),
            json.dumps(json.loads(json.dumps(payload)), sort_keys=True),
        )
        self.assertEqual(
            {
                "primary_failure_class",
                "primary_error_class",
                "primary_message",
                "execution_phase",
                "telemetry_reconciliation",
            },
            set(payload),
        )

    def test_every_failure_maps_to_a_declared_class(self) -> None:
        from pathfinder.frame_bundle_transfer import (
            FRAME_BUNDLE_FAILURE_CLASSES,
        )

        samples = [
            FrameBundleLimitError("x"),
            FrameBundleIdentityError("x"),
            FrameBundleManifestError("x"),
            FrameBundleArchiveError("x"),
            DataAgentArtifactRedirectError("x"),
            DataAgentArtifactSecurityError("x"),
            DataAgentArtifactTooLargeError("x"),
            DataAgentArtifactUnsupportedError("x"),
            DataAgentArtifactIntegrityError("x"),
            DataAgentProtocolError("x"),
            FrameBundleDeliveryError("telemetry_unsupported", "x"),
        ]
        for error in samples:
            with self.subTest(error=type(error).__name__):
                self.assertIn(
                    classify_frame_bundle_failure(error),
                    FRAME_BUNDLE_FAILURE_CLASSES,
                )


class SmokeReportTest(unittest.TestCase):
    """Report generation, retention, and refusal, driven by a stub client."""

    def setUp(self) -> None:
        self.raw = build_bundle(frame_count=2)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def client(self, **overrides: Any) -> FakeBundleClient:
        return FakeBundleClient(
            overrides.get("artifact", binary_artifact(self.raw)),
            overrides.get("telemetry", telemetry_for(len(self.raw))),
        )

    def run_smoke(self, name: str = "run", **kwargs: Any) -> dict[str, Any]:
        options: dict[str, Any] = {
            "object_id": OBJECT_ID,
            "plan_id": "D_bundle",
            "requested_location": "node-1/nvme",
            "output_dir": self.root / name,
            "session_id": "smoke-session",
            "access_id": "frame-bundle-access",
            "client": kwargs.pop("client", None) or self.client(),
        }
        options.update(kwargs)
        return run_frame_bundle_transfer_smoke(**options)

    def test_a_successful_smoke_writes_a_canonical_report(self) -> None:
        report = self.run_smoke(
            expected_artifact_sha256=sha256(self.raw).hexdigest(),
            expected_artifact_size_bytes=len(self.raw),
            retain_artifact=False,
        )
        path = self.root / "run" / SMOKE_REPORT_NAME
        self.assertTrue(path.exists())
        text = path.read_text(encoding="utf-8")
        self.assertTrue(text.endswith("\n"))
        stored = json.loads(text)
        self.assertEqual(report, stored)
        self.assertEqual(
            text,
            json.dumps(stored, indent=2, sort_keys=True, ensure_ascii=False)
            + "\n",
        )
        self.assertEqual("succeeded", stored["status"])
        self.assertEqual("transfer_conformance", stored["evidence_class"])
        self.assertFalse(stored["eligible_for_scientific_claims"])
        self.assertFalse(stored["is_performance_measurement"])
        self.assertFalse(stored["llm_called"])
        self.assertFalse(stored["credentials_recorded"])
        self.assertEqual(2, stored["bundle"]["frame_count"])
        self.assertEqual(3, stored["bundle"]["tar_member_count"])
        self.assertIsNone(stored["outputs"]["retained_artifact"])
        self.assertFalse(stored["bundle"]["pixel_decoding_performed"])
        self.assertFalse(
            stored["bundle"][
                "claims_byte_identity_with_historical_visual_input"
            ]
        )
        self.assertEqual(
            {
                "data_agent_service",
                "client_access_round_trip",
                "artifact_download_elapsed",
                "server_reported_transfer",
            },
            set(stored["latency_ms"]),
        )

    def test_the_report_describes_transfer_evidence_not_performance(
        self,
    ) -> None:
        report = self.run_smoke()
        statement = report["evidence_statement"].lower()
        self.assertIn("transfer and conformance evidence only", statement)
        self.assertIn("not a performance measurement", statement)
        self.assertIn("not confirmatory scientific evidence", statement)

    def test_the_verified_tar_is_retained_only_on_request(self) -> None:
        report = self.run_smoke(name="kept", retain_artifact=True)
        retained = report["outputs"]["retained_artifact"]
        self.assertIsNotNone(retained)
        stored = self.root / "kept" / retained["filename"]
        self.assertEqual(self.raw, stored.read_bytes())
        self.assertEqual(sha256(self.raw).hexdigest(), retained["sha256"])

        plain = self.run_smoke(name="not-kept")
        self.assertIsNone(plain["outputs"]["retained_artifact"])
        self.assertEqual(
            [SMOKE_REPORT_NAME],
            sorted(p.name for p in (self.root / "not-kept").iterdir()),
        )

    def test_no_token_or_signed_url_reaches_the_output(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": "super-secret-token"},
            clear=False,
        ):
            self.run_smoke(name="secrets", retain_artifact=True)
        for path in (self.root / "secrets").iterdir():
            blob = path.read_bytes()
            self.assertNotIn(b"super-secret-token", blob)
            self.assertNotIn(b"signature=", blob)
            self.assertNotIn(b"Authorization", blob)

    def test_an_existing_output_directory_is_refused(self) -> None:
        (self.root / "taken").mkdir()
        with self.assertRaisesRegex(
            FrameBundleTransferError, "refusing to overwrite"
        ):
            self.run_smoke(name="taken")

    def test_a_delivery_failure_writes_no_success_report(self) -> None:
        failing = self.client(
            telemetry=telemetry_for(
                len(self.raw), server_reported_complete=False
            )
        )
        with self.assertRaises(FrameBundleDeliveryError):
            self.run_smoke(name="broken", client=failing)

        directory = self.root / "broken"
        self.assertFalse((directory / SMOKE_REPORT_NAME).exists())
        failure = json.loads(
            (directory / SMOKE_FAILURE_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual("failed", failure["status"])
        self.assertEqual("telemetry_incomplete", failure["failure_class"])
        self.assertFalse(failure["eligible_for_scientific_claims"])
        self.assertFalse(failure["credentials_recorded"])

    def test_a_malformed_bundle_writes_no_success_report(self) -> None:
        broken = self.client(artifact=binary_artifact(b"junk" * 50))
        with self.assertRaises(FrameBundleIngestError):
            self.run_smoke(name="malformed", client=broken)
        directory = self.root / "malformed"
        self.assertFalse((directory / SMOKE_REPORT_NAME).exists())
        failure = json.loads(
            (directory / SMOKE_FAILURE_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual("bundle_archive_invalid", failure["failure_class"])

    def test_no_partial_file_survives_a_write(self) -> None:
        self.run_smoke(name="atomic", retain_artifact=True)
        names = sorted(p.name for p in (self.root / "atomic").iterdir())
        self.assertTrue(all(not name.startswith(".") for name in names))


class LoopbackTransferTest(unittest.TestCase):
    """End to end against a real Data Agent HTTP server on loopback."""

    TOKEN = "control-token"
    PLAN_ID = "D_bundle"
    LOCATION = "node-1/nvme"
    CATALOG_VERSION = "frame-bundle-catalog-v1"

    def setUp(self) -> None:
        from pathfinder.data_agent_manifest import (
            DATA_AGENT_MANIFEST_VERSION,
            DATA_OBJECT_CATALOG_VERSION,
        )
        from pathfinder.data_agent_server import (
            DataAgentServerSettings,
            create_data_agent_http_server,
        )

        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = build_bundle(frame_count=3)
        (self.root / "bundle.tar").write_bytes(self.raw)
        binding = {
            "location": self.LOCATION,
            "minimum_latency_ms": 0,
            "realized_cost": 0.5,
            "cache_hit": False,
        }
        (self.root / "object-catalog.json").write_text(
            json.dumps({
                "schema_version": DATA_OBJECT_CATALOG_VERSION,
                "catalog_version": self.CATALOG_VERSION,
                "objects": {
                    OBJECT_ID: {
                        "representations": {
                            REPRESENTATION_ID: {"path": "bundle.tar"}
                        }
                    }
                },
            }),
            encoding="utf-8",
        )
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text(
            json.dumps({
                "schema_version": DATA_AGENT_MANIFEST_VERSION,
                "node_id": "frame-bundle-test-node",
                "require_plan_binding": True,
                "object_catalog_path": "object-catalog.json",
                "representations": {
                    REPRESENTATION_ID: {
                        "kind": "artifact_uri",
                        "media_type": FRAME_BUNDLE_MEDIA_TYPE,
                        "path": "bundle.tar",
                        "default_binding": dict(binding),
                        "plan_bindings": {self.PLAN_ID: dict(binding)},
                    }
                },
            }),
            encoding="utf-8",
        )
        self.server = create_data_agent_http_server(
            manifest_path=manifest_path,
            operation_db=self.root / "operations.sqlite3",
            settings=DataAgentServerSettings(
                host="127.0.0.1",
                port=0,
                token=self.TOKEN,
                artifact_secret="artifact-secret",
            ),
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        self.thread.start()
        self.addCleanup(self._stop_server)
        host, port = self.server.server_address[:2]
        self.base_url = f"http://{host}:{port}"

    def _stop_server(self) -> None:
        try:
            self.server.shutdown()
        finally:
            self.server.server_close()
            self.thread.join(timeout=5)

    def smoke_options(self, name: str, **overrides: Any) -> dict[str, Any]:
        options: dict[str, Any] = {
            "base_url": self.base_url,
            "object_id": OBJECT_ID,
            "plan_id": self.PLAN_ID,
            "requested_location": self.LOCATION,
            "output_dir": Path(self.temporary.name) / name,
            "session_id": f"loopback-{name}",
            "timeout_seconds": 10.0,
            "quiescence_timeout_seconds": 10.0,
        }
        options.update(overrides)
        return options

    def test_a_real_transfer_validates_and_reconciles(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": self.TOKEN},
            clear=False,
        ):
            report = run_frame_bundle_transfer_smoke(
                **self.smoke_options(
                    "live",
                    expected_artifact_sha256=sha256(self.raw).hexdigest(),
                    expected_artifact_size_bytes=len(self.raw),
                    expected_object_catalog_version=self.CATALOG_VERSION,
                    retain_artifact=True,
                )
            )

        self.assertEqual("succeeded", report["status"])
        self.assertEqual(len(self.raw), report["artifact"]["size_bytes"])
        self.assertEqual(
            sha256(self.raw).hexdigest(), report["artifact"]["sha256"]
        )
        self.assertEqual(
            FRAME_BUNDLE_MEDIA_TYPE, report["artifact"]["media_type"]
        )
        self.assertEqual(3, report["bundle"]["frame_count"])
        self.assertEqual(4, report["bundle"]["tar_member_count"])
        delivery = report["delivery"]
        self.assertTrue(delivery["telemetry_complete"])
        self.assertEqual(0, delivery["in_flight_request_count"])
        self.assertGreaterEqual(delivery["download_request_count"], 1)
        self.assertGreaterEqual(delivery["completed_request_count"], 1)
        self.assertGreaterEqual(delivery["full_download_count"], 1)
        self.assertGreaterEqual(delivery["bytes_sent"], len(self.raw))
        self.assertTrue(delivery["exactly_one_full_download"])
        self.assertTrue(delivery["bytes_sent_equals_artifact_size"])
        self.assertEqual(
            self.base_url.replace("127.0.0.1", "127.0.0.1"),
            report["data_agent_origin"],
        )
        retained = report["outputs"]["retained_artifact"]
        stored = Path(self.temporary.name) / "live" / retained["filename"]
        self.assertEqual(self.raw, stored.read_bytes())
        for path in (Path(self.temporary.name) / "live").iterdir():
            self.assertNotIn(self.TOKEN.encode(), path.read_bytes())
            self.assertNotIn(b"signature=", path.read_bytes())

    def test_a_missing_bearer_token_is_refused(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PATHFINDER_DATA_AGENT_TOKEN", None)
            with self.assertRaises(DataAgentHTTPError) as context:
                run_frame_bundle_transfer_smoke(
                    **self.smoke_options("unauthorized")
                )
        self.assertEqual(401, context.exception.status_code)
        directory = Path(self.temporary.name) / "unauthorized"
        self.assertFalse((directory / SMOKE_REPORT_NAME).exists())
        self.assertTrue((directory / SMOKE_FAILURE_NAME).exists())

    def test_a_wrong_expected_digest_is_refused(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": self.TOKEN},
            clear=False,
        ):
            with self.assertRaises(FrameBundleIdentityError):
                run_frame_bundle_transfer_smoke(
                    **self.smoke_options(
                        "wrong-digest", expected_artifact_sha256="a" * 64
                    )
                )
        self.assertFalse(
            (
                Path(self.temporary.name) / "wrong-digest"
                / SMOKE_REPORT_NAME
            ).exists()
        )

    def serve_bytes(self, name: str, payload: bytes) -> None:
        """Replace the served artifact, keeping everything else identical."""
        (self.root / name).write_bytes(payload)

    def test_a_downloaded_malformed_bundle_still_reconciles_telemetry(
        self,
    ) -> None:
        # The archive is a real, fully transferred tar whose bytes are not
        # the canonical archive. The transfer genuinely consumed remote
        # resources, so the audit must account for it even though the bundle
        # is refused.
        noncanonical = build_bundle(frame_count=3) + b"stowaway"
        self.serve_bytes("bundle.tar", noncanonical)

        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": self.TOKEN},
            clear=False,
        ):
            with self.assertRaises(
                FrameBundleCanonicalizationError
            ) as context:
                run_frame_bundle_transfer_smoke(
                    **self.smoke_options("audited")
                )

        audit = transfer_audit_of(context.exception)
        self.assertIsNotNone(audit)
        # 4. the primary classification is the bundle failure
        self.assertEqual("bundle_not_canonical", audit.primary_failure_class)
        # 1 + 2. downloaded in full, then rejected
        phase = audit.execution_phase
        self.assertTrue(phase.access_completed)
        self.assertTrue(phase.artifact_download_completed)
        self.assertFalse(phase.bundle_validation_completed)
        # 3. telemetry reconciled afterwards
        self.assertTrue(phase.telemetry_reconciliation_attempted)
        evidence = audit.telemetry_reconciliation
        self.assertEqual("complete", evidence.status)
        self.assertTrue(evidence.telemetry_complete)
        self.assertEqual(0, evidence.in_flight_request_count)
        # 5. the real transfer counters survive into the audit
        self.assertEqual(1, evidence.download_request_count)
        self.assertEqual(1, evidence.full_download_count)
        self.assertEqual(len(noncanonical), evidence.bytes_sent)
        self.assertEqual(OBJECT_ID, evidence.object_id)

        # 6. a failure report only, and no completed observation
        directory = Path(self.temporary.name) / "audited"
        self.assertFalse((directory / SMOKE_REPORT_NAME).exists())
        failure = json.loads(
            (directory / SMOKE_FAILURE_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual("failed", failure["status"])
        self.assertEqual("bundle_not_canonical", failure["failure_class"])
        self.assertEqual(
            "bundle_not_canonical",
            failure["audit"]["primary_failure_class"],
        )
        self.assertEqual(
            len(noncanonical),
            failure["audit"]["telemetry_reconciliation"]["bytes_sent"],
        )
        self.assertTrue(
            failure["audit"]["execution_phase"][
                "artifact_download_completed"
            ]
        )
        self.assertFalse(
            failure["audit"]["execution_phase"][
                "bundle_validation_completed"
            ]
        )
        self.assertFalse(failure["eligible_for_scientific_claims"])
        for path in directory.iterdir():
            blob = path.read_bytes()
            self.assertNotIn(self.TOKEN.encode(), blob)
            self.assertNotIn(b"signature=", blob)

    def test_a_bundle_failure_outranks_a_telemetry_failure(self) -> None:
        self.serve_bytes("bundle.tar", build_bundle(frame_count=2) + b"junk")

        # Force the secondary telemetry read to fail while leaving the
        # download path untouched.
        service = self.server.service
        original = service.access_telemetry

        def broken(access_id: str) -> dict:
            payload = original(access_id)
            payload["artifact_download"].pop("telemetry_complete", None)
            return payload

        service.access_telemetry = broken
        self.addCleanup(setattr, service, "access_telemetry", original)

        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": self.TOKEN},
            clear=False,
        ):
            with self.assertRaises(
                FrameBundleCanonicalizationError
            ) as context:
                run_frame_bundle_transfer_smoke(
                    **self.smoke_options("both-fail")
                )

        audit = transfer_audit_of(context.exception)
        # The bundle failure stays primary; the telemetry problem is
        # recorded beside it rather than replacing it.
        self.assertEqual("bundle_not_canonical", audit.primary_failure_class)
        self.assertEqual(
            "unavailable", audit.telemetry_reconciliation.status
        )
        self.assertEqual(
            "DataAgentTelemetryUnsupportedError",
            audit.telemetry_reconciliation.error_class,
        )
        self.assertIsNone(audit.telemetry_reconciliation.bytes_sent)
        self.assertTrue(
            audit.execution_phase.telemetry_reconciliation_attempted
        )

        directory = Path(self.temporary.name) / "both-fail"
        self.assertFalse((directory / SMOKE_REPORT_NAME).exists())
        failure = json.loads(
            (directory / SMOKE_FAILURE_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual("bundle_not_canonical", failure["failure_class"])
        self.assertEqual(
            "unavailable",
            failure["audit"]["telemetry_reconciliation"]["status"],
        )

    def test_a_pre_transfer_refusal_records_no_telemetry_attempt(
        self,
    ) -> None:
        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": self.TOKEN},
            clear=False,
        ):
            with self.assertRaises(Exception):
                run_frame_bundle_transfer_smoke(
                    **self.smoke_options(
                        "no-transfer", plan_id="D_not_configured"
                    )
                )
        failure = json.loads(
            (
                Path(self.temporary.name) / "no-transfer"
                / SMOKE_FAILURE_NAME
            ).read_text(encoding="utf-8")
        )
        phase = failure["audit"]["execution_phase"]
        self.assertFalse(phase["artifact_download_started"])
        self.assertFalse(phase["telemetry_reconciliation_attempted"])
        self.assertEqual(
            "not_attempted",
            failure["audit"]["telemetry_reconciliation"]["status"],
        )
        self.assertIsNone(
            failure["audit"]["telemetry_reconciliation"]["bytes_sent"]
        )

    def test_the_cli_runs_the_same_smoke(self) -> None:
        import contextlib

        from pathfinder.cli import main

        output = Path(self.temporary.name) / "cli"
        buffer = io.StringIO()
        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": self.TOKEN},
            clear=False,
        ):
            with contextlib.redirect_stdout(buffer):
                code = main([
                    "run-frame-bundle-transfer-smoke",
                    "--data-agent-url", self.base_url,
                    "--object-id", OBJECT_ID,
                    "--plan-id", self.PLAN_ID,
                    "--location", self.LOCATION,
                    "--output-dir", str(output),
                    "--expected-sha256", sha256(self.raw).hexdigest(),
                    "--expected-size-bytes", str(len(self.raw)),
                    "--expected-catalog-version", self.CATALOG_VERSION,
                    "--session-id", "cli-session",
                    "--timeout", "10",
                    "--telemetry-quiescence-timeout", "10",
                    "--retain-artifact",
                ])

        self.assertEqual(0, code)
        summary = json.loads(buffer.getvalue())
        self.assertEqual("succeeded", summary["status"])
        self.assertFalse(summary["eligible_for_scientific_claims"])
        self.assertEqual(3, summary["frame_count"])
        self.assertEqual(
            str(output / SMOKE_REPORT_NAME), summary["report_path"]
        )
        report = json.loads(
            (output / SMOKE_REPORT_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual("transfer_conformance", report["evidence_class"])
        self.assertNotIn(self.TOKEN, buffer.getvalue())

    def test_the_cli_returns_nonzero_on_a_validation_failure(self) -> None:
        import contextlib

        from pathfinder.cli import main

        output = Path(self.temporary.name) / "cli-fail"
        buffer = io.StringIO()
        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": self.TOKEN},
            clear=False,
        ):
            with contextlib.redirect_stdout(buffer):
                code = main([
                    "run-frame-bundle-transfer-smoke",
                    "--data-agent-url", self.base_url,
                    "--object-id", OBJECT_ID,
                    "--plan-id", self.PLAN_ID,
                    "--location", self.LOCATION,
                    "--output-dir", str(output),
                    "--expected-size-bytes", str(len(self.raw) + 1),
                    "--session-id", "cli-fail-session",
                    "--timeout", "10",
                ])

        self.assertNotEqual(0, code)
        self.assertEqual("error", json.loads(buffer.getvalue())["status"])
        self.assertFalse((output / SMOKE_REPORT_NAME).exists())
        self.assertTrue((output / SMOKE_FAILURE_NAME).exists())

    def test_the_cli_enforces_configured_limits(self) -> None:
        import contextlib

        from pathfinder.cli import main

        output = Path(self.temporary.name) / "cli-limits"
        buffer = io.StringIO()
        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_DATA_AGENT_TOKEN": self.TOKEN},
            clear=False,
        ):
            with contextlib.redirect_stdout(buffer):
                code = main([
                    "run-frame-bundle-transfer-smoke",
                    "--data-agent-url", self.base_url,
                    "--object-id", OBJECT_ID,
                    "--plan-id", self.PLAN_ID,
                    "--location", self.LOCATION,
                    "--output-dir", str(output),
                    "--max-frame-count", "1",
                    "--session-id", "cli-limit-session",
                    "--timeout", "10",
                ])

        self.assertNotEqual(0, code)
        failure = json.loads(
            (output / SMOKE_FAILURE_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual("bundle_limit_exceeded", failure["failure_class"])


class SeamContractTest(unittest.TestCase):
    """The narrow boundaries this feature is supposed to keep narrow."""

    def test_an_agent_facing_client_cannot_pull_bundle_bytes(self) -> None:
        class AgentOnlyClient:
            def fetch_artifact(self, request: Any) -> Any:
                raise AssertionError("must not be reached")

        request = build_frame_bundle_access_request(
            object_id=OBJECT_ID,
            plan_id="D_bundle",
            requested_location="node-1/nvme",
            session_id="s",
            trial_id="t",
        )
        with self.assertRaisesRegex(TypeError, "fetch_binary_artifact"):
            fetch_validated_frame_bundle(AgentOnlyClient(), request)

    def test_a_non_bundle_media_type_cannot_be_recorded(self) -> None:
        with self.assertRaises(FrameBundleIdentityError):
            validate_frame_bundle_bytes(
                build_bundle(),
                expected_object_id=OBJECT_ID,
                artifact_media_type="application/json",
            )

    def test_the_access_request_is_idempotent_per_session(self) -> None:
        def make(session: str) -> str:
            return build_frame_bundle_access_request(
                object_id=OBJECT_ID,
                plan_id="D_bundle",
                requested_location="node-1/nvme",
                session_id=session,
                trial_id="t",
            ).access_id

        self.assertEqual(make("session-a"), make("session-a"))
        self.assertNotEqual(make("session-a"), make("session-b"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
