"""Strict, bounded, non-extracting validation of ``sampled_frame_bundle``.

A frame bundle arrives as an opaque uncompressed tar downloaded from a
remote Data Agent. Everything inside it is untrusted input: member names,
member metadata, the embedded manifest, and the JPEG payloads. This module
turns those bytes into a typed, immutable value or refuses them, and it
never writes a member path to the filesystem and never calls
``extractall()``.

Scope of the guarantee
----------------------
Validation here is *structural and cryptographic*, not perceptual. Every
JPEG is checked for its size, its SHA-256, a canonical name, a well-formed
JFIF/EXIF byte structure, and the width and height declared in its own
start-of-frame segment. Actual pixel decoding is deliberately **deferred to
the future vision adapter**, because decoding would add a mandatory runtime
image dependency to the ingestion path. The limits a decoding adapter must
still enforce are listed in :data:`DEFERRED_DECODE_REQUIREMENTS`.

What this layer never claims
----------------------------
The frames in a bundle are *aligned with the frozen sampling metadata*: the
same source video, the same sampling algorithm, identical frame indices,
timestamps, and dimensions. They are **not** the JPEG bytes that were
historically shown to a description model, which were never retained. This
module refuses any bundle whose manifest asserts otherwise.
"""

from __future__ import annotations

import io
import json
import math
import re
import tarfile
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Mapping, Protocol

from .frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    REPRESENTATION_ID,
    SOURCE_REPRESENTATION_ID,
    deterministic_frame_bundle_tar,
)


FRAME_BUNDLE_MEDIA_TYPE = "application/x-tar"
FRAME_MEDIA_TYPE = "image/jpeg"

#: Media types this ingestion path will accept for a frame bundle. Kept as a
#: frozenset so a caller cannot widen it in place.
FRAME_BUNDLE_ALLOWED_MEDIA_TYPES = frozenset({FRAME_BUNDLE_MEDIA_TYPE})

#: Obligations that move to the vision adapter because this layer refuses to
#: decode pixels. Documented as data so an adapter can assert against it.
DEFERRED_DECODE_REQUIREMENTS: Mapping[str, Any] = {
    "pixel_decoding_performed_here": False,
    "adapter_must_enforce": (
        "bounded total pixels per frame (width * height)",
        "bounded total pixels per bundle across all frames",
        "a decoder-level decompression-bomb guard",
        "a decode timeout per frame",
        "agreement between decoded dimensions and manifest dimensions",
        "refusal of any frame whose decoder emits a warning it cannot "
        "classify",
    ),
}

_HEX64 = re.compile(r"\A[0-9a-f]{64}\Z")
_FRAME_MEMBER = re.compile(r"\Aframes/(\d+)\.jpg\Z")
_DRIVE_LETTER = re.compile(r"\A[A-Za-z]:")

_JPEG_SOI = b"\xff\xd8"
_JPEG_EOI = b"\xff\xd9"
# Start-of-frame markers carry the declared dimensions. 0xC4 (DHT), 0xC8
# (reserved JPG), and 0xCC (DAC) share the 0xCn range but are not frames.
_SOF_MARKERS = frozenset(
    set(range(0xC0, 0xD0)) - {0xC4, 0xC8, 0xCC}
)
_STANDALONE_MARKERS = frozenset({0x01} | set(range(0xD0, 0xD8)))

_REQUIRED_FALSE_FIELDS = (
    "historical_visual_bytes_retained",
    "claims_byte_identity_with_historical_visual_input",
    "llm_called",
    "credentials_recorded",
    "network_calls_performed",
)

_MANIFEST_KEYS = frozenset({
    "schema_version",
    "representation_id",
    "object_id",
    "source_video_id",
    "source_video_filename",
    "source_video_size_bytes",
    "source_video_sha256",
    "source_duration_seconds",
    "sampling",
    "source_frame_descriptions",
    "generation_manifest_sha256",
    "frames",
    "frame_count",
    "total_jpeg_bytes",
    "software_versions",
    "sampling_alignment_statement",
} | set(_REQUIRED_FALSE_FIELDS))

_FRAME_KEYS = frozenset({
    "frame_index",
    "timestamp_seconds",
    "width",
    "height",
    "path",
    "jpeg_size_bytes",
    "jpeg_sha256",
})

_SAMPLING_KEYS = frozenset({
    "method",
    "frame_count",
    "jpeg_max_dimension",
    "jpeg_quality",
    "jpeg_optimize",
})

_SOURCE_DESCRIPTION_KEYS = frozenset({
    "representation_id",
    "path",
    "sha256",
})

#: The statement must carry both an affirmative alignment claim and an
#: explicit non-identity disclaimer. Both are matched as positive evidence.
#: Scanning for *forbidden* phrases instead would be wrong here: the
#: canonical disclaimer itself contains "byte identity with the historical
#: visual input", negated, so a substring blocklist rejects the correct text
#: and passes anything that simply omits the disclaimer.
_ALIGNMENT_REQUIRED_PHRASES = (
    "aligned with the frozen sampling metadata",
    "not claim byte identity with the historical visual input",
)


class FrameBundleIngestError(RuntimeError):
    """Base error for a rejected ``sampled_frame_bundle`` payload."""


class FrameBundleLimitError(FrameBundleIngestError):
    """Raised when a configured ingestion bound is exceeded."""


class FrameBundleArchiveError(FrameBundleIngestError):
    """Raised when the tar container violates the bundle contract."""


class FrameBundleCanonicalizationError(FrameBundleArchiveError):
    """Raised when raw bundle bytes are not the canonical USTAR archive.

    ``tarfile`` presents a *logical* view: it follows PAX and GNU extension
    headers, normalizes header fields, stops at the first end-of-archive
    marker, and silently ignores whatever follows. Two archives with an
    identical logical member list can therefore have very different bytes,
    and only one of them is what Pathfinder's generator produces. Because
    the whole point of this representation is a byte-reproducible artifact,
    anything else is refused here even when its contents look equivalent.
    """


class FrameBundleManifestError(FrameBundleIngestError):
    """Raised when the embedded manifest is malformed or inconsistent."""


class FrameBundleIdentityError(FrameBundleIngestError):
    """Raised when the bundle is not the artifact the caller asked for."""


@dataclass(frozen=True)
class FrameBundleLimits:
    """Bounds applied before any untrusted structure is interpreted."""

    max_artifact_bytes: int = 8 * 1024 * 1024
    max_member_count: int = 128
    max_frame_count: int = 64
    max_frame_bytes: int = 4 * 1024 * 1024
    max_total_contained_bytes: int = 8 * 1024 * 1024
    max_manifest_bytes: int = 1024 * 1024
    max_frame_dimension: int = 8192

    def __post_init__(self) -> None:
        for name in (
            "max_artifact_bytes",
            "max_member_count",
            "max_frame_count",
            "max_frame_bytes",
            "max_total_contained_bytes",
            "max_manifest_bytes",
            "max_frame_dimension",
        ):
            value = getattr(self, name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a positive integer")

    def to_dict(self) -> dict[str, int]:
        return {
            "max_artifact_bytes": self.max_artifact_bytes,
            "max_member_count": self.max_member_count,
            "max_frame_count": self.max_frame_count,
            "max_frame_bytes": self.max_frame_bytes,
            "max_total_contained_bytes": self.max_total_contained_bytes,
            "max_manifest_bytes": self.max_manifest_bytes,
            "max_frame_dimension": self.max_frame_dimension,
        }


DEFAULT_FRAME_BUNDLE_LIMITS = FrameBundleLimits()


@dataclass(frozen=True)
class VisionFrame:
    """One validated frame, in the shape a vision adapter consumes.

    This is the entire handoff surface. It is provider neutral on purpose:
    no adapter-specific encoding, no prompt text, no ordering policy.
    """

    frame_index: int
    timestamp_seconds: float
    width: int
    height: int
    media_type: str
    jpeg_bytes: bytes


class FrameBundleVisionSource(Protocol):
    """The seam a future vision adapter is written against.

    An adapter receives ordered, already-verified frames and nothing else.
    It must not be given the tar, the manifest, or a Data Agent URL.
    """

    def vision_frames(self) -> tuple[VisionFrame, ...]:
        """Return validated frames in ascending frame-index order."""


@dataclass(frozen=True)
class ValidatedFrame:
    """One frame member whose bytes matched its manifest entry exactly."""

    frame_index: int
    timestamp_seconds: float
    width: int
    height: int
    member_path: str
    size_bytes: int
    sha256: str
    jpeg_bytes: bytes
    media_type: str = FRAME_MEDIA_TYPE

    def to_metadata_dict(self) -> dict[str, Any]:
        """Metadata only. Deliberately never carries the JPEG bytes."""
        return {
            "frame_index": self.frame_index,
            "timestamp_seconds": self.timestamp_seconds,
            "width": self.width,
            "height": self.height,
            "member_path": self.member_path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "media_type": self.media_type,
        }


@dataclass(frozen=True)
class FrameBundleSource:
    """Provenance of the source video and the frozen sampling metadata."""

    source_video_id: str
    source_video_filename: str
    source_video_size_bytes: int
    source_video_sha256: str
    source_duration_seconds: float
    sampling_method: str
    declared_frame_count: int
    jpeg_max_dimension: int
    jpeg_quality: int
    jpeg_optimize: bool
    frame_descriptions_representation_id: str
    frame_descriptions_path: str
    frame_descriptions_sha256: str
    generation_manifest_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_video_id": self.source_video_id,
            "source_video_filename": self.source_video_filename,
            "source_video_size_bytes": self.source_video_size_bytes,
            "source_video_sha256": self.source_video_sha256,
            "source_duration_seconds": self.source_duration_seconds,
            "sampling_method": self.sampling_method,
            "declared_frame_count": self.declared_frame_count,
            "jpeg_max_dimension": self.jpeg_max_dimension,
            "jpeg_quality": self.jpeg_quality,
            "jpeg_optimize": self.jpeg_optimize,
            "frame_descriptions_representation_id": (
                self.frame_descriptions_representation_id
            ),
            "frame_descriptions_path": self.frame_descriptions_path,
            "frame_descriptions_sha256": self.frame_descriptions_sha256,
            "generation_manifest_sha256": self.generation_manifest_sha256,
        }


@dataclass(frozen=True)
class ValidatedFrameBundle:
    """An accepted bundle: immutable, ordered, and byte-verified.

    Implements :class:`FrameBundleVisionSource`. It intentionally offers no
    method that renders binary content into Agent-visible text; the only
    text projection, :meth:`agent_visible_summary`, is metadata only.
    """

    object_id: str
    representation_id: str
    schema_version: str
    artifact_media_type: str
    artifact_size_bytes: int
    artifact_sha256: str
    manifest_sha256: str
    member_count: int
    frames: tuple[ValidatedFrame, ...]
    total_jpeg_bytes: int
    source: FrameBundleSource
    software_versions: tuple[tuple[str, str | None], ...]
    sampling_alignment_statement: str
    claims_byte_identity_with_historical_visual_input: bool = False
    historical_visual_bytes_retained: bool = False

    @property
    def frame_count(self) -> int:
        return len(self.frames)

    def vision_frames(self) -> tuple[VisionFrame, ...]:
        """Ordered, verified frames for a future vision adapter."""
        return tuple(
            VisionFrame(
                frame_index=frame.frame_index,
                timestamp_seconds=frame.timestamp_seconds,
                width=frame.width,
                height=frame.height,
                media_type=frame.media_type,
                jpeg_bytes=frame.jpeg_bytes,
            )
            for frame in self.frames
        )

    def agent_visible_summary(self) -> dict[str, Any]:
        """A JSON-safe description with no image bytes and no base64.

        Returning frame bytes through an Agent text channel is the failure
        this representation exists to avoid, so the projection is metadata
        only and there is no opt-out.
        """
        return {
            "representation_id": self.representation_id,
            "schema_version": self.schema_version,
            "object_id": self.object_id,
            "artifact_media_type": self.artifact_media_type,
            "artifact_size_bytes": self.artifact_size_bytes,
            "artifact_sha256": self.artifact_sha256,
            "frame_count": self.frame_count,
            "total_jpeg_bytes": self.total_jpeg_bytes,
            "frames": [frame.to_metadata_dict() for frame in self.frames],
            "sampling_alignment_statement": (
                self.sampling_alignment_statement
            ),
            "claims_byte_identity_with_historical_visual_input": False,
            "historical_visual_bytes_retained": False,
            "pixel_decoding_performed": False,
        }


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FrameBundleManifestError(
            f"{label} must be a non-empty string"
        )
    return value


def _integer(value: Any, label: str, *, minimum: int) -> int:
    # bool is an int subclass; a manifest that puts True where a count
    # belongs is malformed, not a one.
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
    ):
        raise FrameBundleManifestError(
            f"{label} must be an integer >= {minimum}"
        )
    return value


def _number(value: Any, label: str, *, minimum: float) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        or float(value) < minimum
    ):
        raise FrameBundleManifestError(
            f"{label} must be a finite number >= {minimum}"
        )
    return float(value)


def _literal_false(value: Any, label: str) -> bool:
    # ``is False`` and not ``not value``: 0, "", and "false" are all falsey
    # but none of them is the boolean the contract requires.
    if value is not False:
        raise FrameBundleManifestError(
            f"{label} must be the literal boolean false, not {value!r}"
        )
    return False


def _hex_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _HEX64.fullmatch(value):
        raise FrameBundleManifestError(
            f"{label} must be a lowercase 64-character hex SHA-256"
        )
    return value


def _exact_keys(
    mapping: Mapping[str, Any],
    expected: frozenset[str],
    label: str,
) -> None:
    present = set(mapping)
    missing = sorted(expected - present)
    unexpected = sorted(present - expected)
    if missing:
        raise FrameBundleManifestError(
            f"{label} is missing required field(s): {', '.join(missing)}"
        )
    if unexpected:
        raise FrameBundleManifestError(
            f"{label} has unexpected field(s): {', '.join(unexpected)}"
        )


def _validate_member_name(name: Any) -> str:
    if not isinstance(name, str) or not name:
        raise FrameBundleArchiveError(
            "tar member name must be a non-empty string"
        )
    if len(name) > 256:
        raise FrameBundleLimitError(
            f"tar member name is unreasonably long: {len(name)} characters"
        )
    if "\\" in name:
        raise FrameBundleArchiveError(
            f"tar member name contains a backslash: {name!r}"
        )
    if any(character < " " or character == "\x7f" for character in name):
        raise FrameBundleArchiveError(
            "tar member name contains a control character"
        )
    if name.startswith("/"):
        raise FrameBundleArchiveError(
            f"tar member name must not be absolute: {name!r}"
        )
    if _DRIVE_LETTER.match(name):
        raise FrameBundleArchiveError(
            f"tar member name must not carry a drive letter: {name!r}"
        )
    parts = name.split("/")
    if any(part == "" for part in parts):
        raise FrameBundleArchiveError(
            f"tar member name has an empty path component: {name!r}"
        )
    if any(part == ".." for part in parts):
        raise FrameBundleArchiveError(
            f"tar member name must not traverse upwards: {name!r}"
        )
    if any(part == "." for part in parts):
        raise FrameBundleArchiveError(
            f"tar member name must not contain a '.' component: {name!r}"
        )
    return name


def _validate_member_metadata(member: tarfile.TarInfo) -> None:
    name = member.name
    if member.type != tarfile.REGTYPE:
        raise FrameBundleArchiveError(
            f"tar member {name!r} is not a regular file "
            f"(type {member.type!r})"
        )
    for predicate, description in (
        (member.isdir(), "a directory"),
        (member.issym(), "a symbolic link"),
        (member.islnk(), "a hard link"),
        (member.ischr(), "a character device"),
        (member.isblk(), "a block device"),
        (member.isfifo(), "a FIFO"),
        (member.isdev(), "a device"),
        (member.issparse(), "sparse"),
    ):
        if predicate:
            raise FrameBundleArchiveError(
                f"tar member {name!r} is {description}"
            )
    if member.mtime != 0:
        raise FrameBundleArchiveError(
            f"tar member {name!r} must have mtime=0, not {member.mtime}"
        )
    if member.uid != 0 or member.gid != 0:
        raise FrameBundleArchiveError(
            f"tar member {name!r} must have uid=0 and gid=0"
        )
    if member.uname != "" or member.gname != "":
        raise FrameBundleArchiveError(
            f"tar member {name!r} must have empty uname and gname"
        )
    if member.mode != 0o644:
        raise FrameBundleArchiveError(
            f"tar member {name!r} must have mode 0644, not "
            f"{member.mode:04o}"
        )


def _jpeg_declared_dimensions(payload: bytes) -> tuple[int, int]:
    """Read width and height from the start-of-frame segment.

    This walks segment headers only. No entropy-coded data is interpreted
    and no pixel is reconstructed, so a malformed image cannot turn into a
    decode of arbitrary size here.
    """
    if not payload.startswith(_JPEG_SOI):
        raise FrameBundleArchiveError(
            "frame does not begin with a JPEG start-of-image marker"
        )
    if not payload.endswith(_JPEG_EOI):
        raise FrameBundleArchiveError(
            "frame does not end with a JPEG end-of-image marker"
        )
    offset = 2
    length = len(payload)
    # Segment headers are at least four bytes, so this cannot loop more
    # times than the payload has quarter-bytes.
    for _ in range(length):
        while offset < length and payload[offset] == 0xFF:
            offset += 1
        if offset >= length:
            break
        marker = payload[offset]
        offset += 1
        if marker in _STANDALONE_MARKERS:
            continue
        if marker == 0xD9 or marker == 0xDA:
            break
        if offset + 2 > length:
            break
        segment_length = int.from_bytes(payload[offset:offset + 2], "big")
        if segment_length < 2:
            raise FrameBundleArchiveError(
                "frame has an invalid JPEG segment length"
            )
        if marker in _SOF_MARKERS:
            if offset + 7 > length:
                raise FrameBundleArchiveError(
                    "frame has a truncated JPEG start-of-frame segment"
                )
            height = int.from_bytes(payload[offset + 3:offset + 5], "big")
            width = int.from_bytes(payload[offset + 5:offset + 7], "big")
            if width <= 0 or height <= 0:
                raise FrameBundleArchiveError(
                    "frame declares non-positive JPEG dimensions"
                )
            return width, height
        offset += segment_length
        if offset > length:
            raise FrameBundleArchiveError(
                "frame has a JPEG segment that runs past its end"
            )
        # A marker must follow the segment we just skipped.
        if offset < length and payload[offset] != 0xFF:
            raise FrameBundleArchiveError(
                "frame has a misaligned JPEG segment boundary"
            )
    raise FrameBundleArchiveError(
        "frame has no JPEG start-of-frame segment"
    )


def _read_members(
    raw: bytes,
    limits: FrameBundleLimits,
) -> tuple[list[str], dict[str, bytes]]:
    """Return member names in archive order plus their verified payloads.

    The archive is read from memory. ``mode="r:"`` refuses any compressed
    stream, which is what makes "uncompressed tar" an enforced property
    rather than a convention.
    """
    try:
        archive = tarfile.open(fileobj=io.BytesIO(raw), mode="r:")
    except tarfile.TarError as exc:
        raise FrameBundleArchiveError(
            f"frame bundle is not a readable uncompressed tar: {exc}"
        ) from exc

    order: list[str] = []
    payloads: dict[str, bytes] = {}
    contained_bytes = 0
    try:
        for member in archive:
            if len(order) >= limits.max_member_count:
                raise FrameBundleLimitError(
                    "frame bundle exceeds max_member_count="
                    f"{limits.max_member_count}"
                )
            name = _validate_member_name(member.name)
            _validate_member_metadata(member)
            if name in payloads:
                raise FrameBundleArchiveError(
                    f"frame bundle contains a duplicate member: {name!r}"
                )
            if order and name <= order[-1]:
                raise FrameBundleArchiveError(
                    "frame bundle members are not in lexicographic order: "
                    f"{name!r} follows {order[-1]!r}"
                )
            member_limit = (
                limits.max_manifest_bytes
                if name == OBJECT_MANIFEST_NAME
                else limits.max_frame_bytes
            )
            if member.size > member_limit:
                raise FrameBundleLimitError(
                    f"tar member {name!r} is {member.size} bytes, above its "
                    f"{member_limit}-byte bound"
                )
            contained_bytes += member.size
            if contained_bytes > limits.max_total_contained_bytes:
                raise FrameBundleLimitError(
                    "frame bundle exceeds max_total_contained_bytes="
                    f"{limits.max_total_contained_bytes}"
                )
            stream = archive.extractfile(member)
            if stream is None:
                raise FrameBundleArchiveError(
                    f"tar member {name!r} has no readable content"
                )
            payload = stream.read(member.size + 1)
            if len(payload) != member.size:
                raise FrameBundleArchiveError(
                    f"tar member {name!r} does not match its declared size"
                )
            order.append(name)
            payloads[name] = payload
    except tarfile.TarError as exc:
        raise FrameBundleArchiveError(
            f"frame bundle tar is malformed: {exc}"
        ) from exc
    finally:
        archive.close()

    if not order:
        raise FrameBundleArchiveError("frame bundle contains no members")
    return order, payloads


def _validate_sampling(value: Any, frame_count: int) -> tuple[
    str, int, int, int, bool
]:
    if not isinstance(value, Mapping):
        raise FrameBundleManifestError("manifest sampling must be an object")
    _exact_keys(value, _SAMPLING_KEYS, "manifest sampling")
    method = _text(value["method"], "manifest sampling.method")
    declared = _integer(
        value["frame_count"], "manifest sampling.frame_count", minimum=1
    )
    if declared != frame_count:
        raise FrameBundleManifestError(
            f"manifest sampling.frame_count={declared} disagrees with "
            f"frame_count={frame_count}"
        )
    max_dimension = _integer(
        value["jpeg_max_dimension"],
        "manifest sampling.jpeg_max_dimension",
        minimum=1,
    )
    quality = _integer(
        value["jpeg_quality"], "manifest sampling.jpeg_quality", minimum=1
    )
    if quality > 100:
        raise FrameBundleManifestError(
            "manifest sampling.jpeg_quality must be <= 100"
        )
    optimize = value["jpeg_optimize"]
    if not isinstance(optimize, bool):
        raise FrameBundleManifestError(
            "manifest sampling.jpeg_optimize must be a boolean"
        )
    return method, declared, max_dimension, quality, optimize


def _validate_source(
    manifest: Mapping[str, Any],
    frame_count: int,
) -> FrameBundleSource:
    method, declared, max_dimension, quality, optimize = _validate_sampling(
        manifest["sampling"], frame_count
    )
    descriptions = manifest["source_frame_descriptions"]
    if not isinstance(descriptions, Mapping):
        raise FrameBundleManifestError(
            "manifest source_frame_descriptions must be an object"
        )
    _exact_keys(
        descriptions,
        _SOURCE_DESCRIPTION_KEYS,
        "manifest source_frame_descriptions",
    )
    descriptions_representation = _text(
        descriptions["representation_id"],
        "manifest source_frame_descriptions.representation_id",
    )
    if descriptions_representation != SOURCE_REPRESENTATION_ID:
        raise FrameBundleManifestError(
            "manifest source_frame_descriptions.representation_id must be "
            f"{SOURCE_REPRESENTATION_ID!r}"
        )
    return FrameBundleSource(
        source_video_id=_text(
            manifest["source_video_id"], "manifest source_video_id"
        ),
        source_video_filename=_text(
            manifest["source_video_filename"],
            "manifest source_video_filename",
        ),
        source_video_size_bytes=_integer(
            manifest["source_video_size_bytes"],
            "manifest source_video_size_bytes",
            minimum=1,
        ),
        source_video_sha256=_hex_digest(
            manifest["source_video_sha256"], "manifest source_video_sha256"
        ),
        source_duration_seconds=_number(
            manifest["source_duration_seconds"],
            "manifest source_duration_seconds",
            minimum=0.0,
        ),
        sampling_method=method,
        declared_frame_count=declared,
        jpeg_max_dimension=max_dimension,
        jpeg_quality=quality,
        jpeg_optimize=optimize,
        frame_descriptions_representation_id=descriptions_representation,
        frame_descriptions_path=_text(
            descriptions["path"], "manifest source_frame_descriptions.path"
        ),
        frame_descriptions_sha256=_hex_digest(
            descriptions["sha256"],
            "manifest source_frame_descriptions.sha256",
        ),
        generation_manifest_sha256=_hex_digest(
            manifest["generation_manifest_sha256"],
            "manifest generation_manifest_sha256",
        ),
    )


def _validate_software_versions(
    value: Any,
) -> tuple[tuple[str, str | None], ...]:
    if not isinstance(value, Mapping):
        raise FrameBundleManifestError(
            "manifest software_versions must be an object"
        )
    entries: list[tuple[str, str | None]] = []
    for name, version in value.items():
        if not isinstance(name, str) or not name.strip():
            raise FrameBundleManifestError(
                "manifest software_versions keys must be non-empty strings"
            )
        if version is not None and not isinstance(version, str):
            raise FrameBundleManifestError(
                f"manifest software_versions[{name!r}] must be a string "
                "or null"
            )
        entries.append((name, version))
    return tuple(sorted(entries, key=lambda item: item[0]))


def _validate_alignment_statement(value: Any) -> str:
    statement = _text(value, "manifest sampling_alignment_statement")
    lowered = statement.lower()
    for phrase in _ALIGNMENT_REQUIRED_PHRASES:
        if phrase not in lowered:
            raise FrameBundleManifestError(
                "manifest sampling_alignment_statement must state that the "
                f"frames are {_ALIGNMENT_REQUIRED_PHRASES[0]} and that the "
                "artifact does "
                f"{_ALIGNMENT_REQUIRED_PHRASES[1]}; it is missing "
                f"{phrase!r}"
            )
    return statement


def _canonical_frame_member(index: int) -> str:
    return f"frames/{index:03d}.jpg"


def validate_frame_bundle_bytes(
    raw: bytes,
    *,
    expected_object_id: str,
    expected_sha256: str | None = None,
    expected_size_bytes: int | None = None,
    artifact_media_type: str = FRAME_BUNDLE_MEDIA_TYPE,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> ValidatedFrameBundle:
    """Validate downloaded bundle bytes and return an immutable value.

    Every rejection is fail-closed: there is no partial result and no
    "best effort" bundle. ``expected_object_id`` is required because a
    structurally perfect bundle for the wrong object is still the wrong
    evidence.
    """
    if not isinstance(raw, (bytes, bytearray)):
        raise FrameBundleIngestError(
            "frame bundle payload must be raw bytes"
        )
    raw = bytes(raw)
    if not isinstance(expected_object_id, str) or not expected_object_id:
        raise ValueError("expected_object_id is required")
    if artifact_media_type not in FRAME_BUNDLE_ALLOWED_MEDIA_TYPES:
        raise FrameBundleIdentityError(
            f"frame bundle media type {artifact_media_type!r} is not one of "
            f"{', '.join(sorted(FRAME_BUNDLE_ALLOWED_MEDIA_TYPES))}"
        )
    if len(raw) > limits.max_artifact_bytes:
        raise FrameBundleLimitError(
            f"frame bundle is {len(raw)} bytes, above max_artifact_bytes="
            f"{limits.max_artifact_bytes}"
        )
    if not raw:
        raise FrameBundleArchiveError("frame bundle payload is empty")

    artifact_sha256 = sha256(raw).hexdigest()
    if expected_sha256 is not None:
        normalized = _hex_digest(
            str(expected_sha256).lower(), "expected_sha256"
        )
        if artifact_sha256 != normalized:
            raise FrameBundleIdentityError(
                "frame bundle SHA-256 does not match the expected digest"
            )
    if expected_size_bytes is not None:
        if (
            not isinstance(expected_size_bytes, int)
            or isinstance(expected_size_bytes, bool)
            or expected_size_bytes < 0
        ):
            raise ValueError(
                "expected_size_bytes must be a non-negative integer"
            )
        if len(raw) != expected_size_bytes:
            raise FrameBundleIdentityError(
                f"frame bundle is {len(raw)} bytes, expected "
                f"{expected_size_bytes}"
            )

    order, payloads = _read_members(raw, limits)

    if OBJECT_MANIFEST_NAME not in payloads:
        raise FrameBundleArchiveError(
            f"frame bundle has no {OBJECT_MANIFEST_NAME} member"
        )
    manifest_bytes = payloads[OBJECT_MANIFEST_NAME]
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrameBundleManifestError(
            f"embedded frame bundle manifest is not valid UTF-8 JSON: {exc}"
        ) from exc
    if not isinstance(manifest, Mapping):
        raise FrameBundleManifestError(
            "embedded frame bundle manifest must be a JSON object"
        )
    _exact_keys(manifest, _MANIFEST_KEYS, "manifest")

    schema_version = _text(manifest["schema_version"], "manifest schema_version")
    if schema_version != FRAME_BUNDLE_SCHEMA_VERSION:
        raise FrameBundleManifestError(
            f"unsupported frame bundle schema_version: {schema_version!r} "
            f"(expected {FRAME_BUNDLE_SCHEMA_VERSION!r})"
        )
    representation_id = _text(
        manifest["representation_id"], "manifest representation_id"
    )
    if representation_id != REPRESENTATION_ID:
        raise FrameBundleManifestError(
            f"manifest representation_id must be {REPRESENTATION_ID!r}, "
            f"not {representation_id!r}"
        )
    object_id = _text(manifest["object_id"], "manifest object_id")
    if object_id != expected_object_id:
        raise FrameBundleIdentityError(
            f"frame bundle is for object {object_id!r}, expected "
            f"{expected_object_id!r}"
        )
    for field_name in _REQUIRED_FALSE_FIELDS:
        _literal_false(manifest[field_name], f"manifest {field_name}")
    statement = _validate_alignment_statement(
        manifest["sampling_alignment_statement"]
    )

    entries = manifest["frames"]
    if not isinstance(entries, list):
        raise FrameBundleManifestError("manifest frames must be an array")
    if not entries:
        raise FrameBundleManifestError("manifest frames must not be empty")
    if len(entries) > limits.max_frame_count:
        raise FrameBundleLimitError(
            f"frame bundle declares {len(entries)} frames, above "
            f"max_frame_count={limits.max_frame_count}"
        )
    declared_count = _integer(
        manifest["frame_count"], "manifest frame_count", minimum=1
    )
    if declared_count != len(entries):
        raise FrameBundleManifestError(
            f"manifest frame_count={declared_count} disagrees with "
            f"{len(entries)} frame entries"
        )

    frame_members = [
        name for name in order if name != OBJECT_MANIFEST_NAME
    ]
    unexpected = [
        name for name in frame_members if not _FRAME_MEMBER.fullmatch(name)
    ]
    if unexpected:
        raise FrameBundleArchiveError(
            "frame bundle has unexpected member(s): "
            + ", ".join(repr(name) for name in unexpected)
        )
    if len(frame_members) != len(entries):
        raise FrameBundleManifestError(
            f"frame bundle carries {len(frame_members)} frame member(s) but "
            f"declares {len(entries)}"
        )

    frames: list[ValidatedFrame] = []
    seen_indices: set[int] = set()
    previous_index = -1
    total_declared = 0
    for position, entry in enumerate(entries):
        label = f"manifest frames[{position}]"
        if not isinstance(entry, Mapping):
            raise FrameBundleManifestError(f"{label} must be an object")
        _exact_keys(entry, _FRAME_KEYS, label)
        frame_index = _integer(
            entry["frame_index"], f"{label}.frame_index", minimum=0
        )
        if frame_index in seen_indices:
            raise FrameBundleManifestError(
                f"{label}.frame_index={frame_index} is duplicated"
            )
        if frame_index <= previous_index:
            raise FrameBundleManifestError(
                f"{label}.frame_index={frame_index} is not strictly "
                "ascending"
            )
        if frame_index != position:
            raise FrameBundleManifestError(
                f"{label}.frame_index={frame_index} is not canonical; "
                f"expected {position}"
            )
        seen_indices.add(frame_index)
        previous_index = frame_index

        member_path = _text(entry["path"], f"{label}.path")
        canonical = _canonical_frame_member(frame_index)
        if member_path != canonical:
            raise FrameBundleManifestError(
                f"{label}.path is {member_path!r}, expected {canonical!r}"
            )
        if member_path not in payloads:
            raise FrameBundleManifestError(
                f"{label}.path {member_path!r} has no tar member"
            )
        payload = payloads[member_path]

        size_bytes = _integer(
            entry["jpeg_size_bytes"], f"{label}.jpeg_size_bytes", minimum=1
        )
        if size_bytes != len(payload):
            raise FrameBundleManifestError(
                f"{label}.jpeg_size_bytes={size_bytes} disagrees with the "
                f"{len(payload)}-byte member"
            )
        digest = _hex_digest(entry["jpeg_sha256"], f"{label}.jpeg_sha256")
        actual_digest = sha256(payload).hexdigest()
        if digest != actual_digest:
            raise FrameBundleManifestError(
                f"{label}.jpeg_sha256 does not match the member bytes"
            )
        total_declared += size_bytes

        timestamp = _number(
            entry["timestamp_seconds"],
            f"{label}.timestamp_seconds",
            minimum=0.0,
        )
        width = _integer(entry["width"], f"{label}.width", minimum=1)
        height = _integer(entry["height"], f"{label}.height", minimum=1)
        for name, value in (("width", width), ("height", height)):
            if value > limits.max_frame_dimension:
                raise FrameBundleLimitError(
                    f"{label}.{name}={value} exceeds max_frame_dimension="
                    f"{limits.max_frame_dimension}"
                )
        declared_width, declared_height = _jpeg_declared_dimensions(payload)
        if (declared_width, declared_height) != (width, height):
            raise FrameBundleManifestError(
                f"{label} declares {width}x{height} but the JPEG itself "
                f"declares {declared_width}x{declared_height}"
            )
        frames.append(
            ValidatedFrame(
                frame_index=frame_index,
                timestamp_seconds=timestamp,
                width=width,
                height=height,
                member_path=member_path,
                size_bytes=size_bytes,
                sha256=digest,
                jpeg_bytes=payload,
            )
        )

    total_jpeg_bytes = _integer(
        manifest["total_jpeg_bytes"], "manifest total_jpeg_bytes", minimum=1
    )
    if total_jpeg_bytes != total_declared:
        raise FrameBundleManifestError(
            f"manifest total_jpeg_bytes={total_jpeg_bytes} disagrees with "
            f"the {total_declared} bytes its frame entries declare"
        )
    actual_total = sum(len(payloads[frame.member_path]) for frame in frames)
    if total_jpeg_bytes != actual_total:
        raise FrameBundleManifestError(
            f"manifest total_jpeg_bytes={total_jpeg_bytes} disagrees with "
            f"the {actual_total} bytes actually carried"
        )

    # Final gate: rebuild the archive from the members just verified, using
    # the generator's own serializer, and require the downloaded bytes to be
    # exactly that. This is what rejects PAX and GNU variants, concatenated
    # archives, trailing data, altered end blocks or record padding, and raw
    # header fields that tarfile normalized away before we ever saw them.
    try:
        canonical = deterministic_frame_bundle_tar(
            [(name, payloads[name]) for name in order]
        )
    except (ValueError, tarfile.TarError) as exc:
        # Reaching here means the members passed every logical check yet
        # cannot be expressed as a canonical USTAR archive at all -- which is
        # itself proof the input was never one.
        raise FrameBundleCanonicalizationError(
            f"frame bundle members cannot be re-serialized canonically: {exc}"
        ) from exc
    if canonical != raw:
        raise FrameBundleCanonicalizationError(
            "frame bundle bytes are not the canonical USTAR archive for "
            f"their contents ({len(raw)} bytes received, {len(canonical)} "
            "bytes when re-serialized); the logical members may match while "
            "the archive encoding does not"
        )

    return ValidatedFrameBundle(
        object_id=object_id,
        representation_id=representation_id,
        schema_version=schema_version,
        artifact_media_type=artifact_media_type,
        artifact_size_bytes=len(raw),
        artifact_sha256=artifact_sha256,
        manifest_sha256=sha256(manifest_bytes).hexdigest(),
        member_count=len(order),
        frames=tuple(frames),
        total_jpeg_bytes=total_jpeg_bytes,
        source=_validate_source(manifest, len(frames)),
        software_versions=_validate_software_versions(
            manifest["software_versions"]
        ),
        sampling_alignment_statement=statement,
    )
