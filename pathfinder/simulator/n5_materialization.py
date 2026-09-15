"""Portable deterministic N5 video-to-frame-bundle materialization.

The contract in this module is deliberately independent of HTTP, Docker,
FlowMesh, and host paths.  A frozen plan binds one exact source video, the
existing sampled-frame provenance used by Pathfinder's canonical bundle
format, the deterministic sampling/encoding contract, and the expected
output bytes.  The N5 runtime receives the raw bytes out of band, verifies
their content identity, regenerates the canonical bundle, and refuses any
output that differs from the frozen artifact binding.

Frame extraction does not call an LLM.  Semantic descriptions and digests are
not generated here; the sampled-frame description document is consumed only
as frozen alignment provenance required by the existing frame-bundle schema.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import math
import re
import sqlite3
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import metadata
from pathlib import Path, PurePosixPath, PureWindowsPath
from tempfile import TemporaryDirectory
from typing import Any, Protocol

from ..frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    JPEG_OPTIMIZE,
    JPEG_QUALITY,
    OBJECT_MANIFEST_NAME,
    REPRESENTATION_ID,
    SAMPLING_METHOD,
    SOURCE_REPRESENTATION_ID,
    deterministic_frame_bundle_tar,
)
from ..frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    FRAME_BUNDLE_MEDIA_TYPE,
    FrameBundleLimits,
    validate_frame_bundle_bytes,
)
from ..video_prep import (
    FRAME_SCHEMA_VERSION,
    PREP_SCHEMA_VERSION,
    SampledImage,
    sample_video,
)


N5_MATERIALIZATION_PLAN_SCHEMA_VERSION = (
    "pathfinder.simulator-n5-materialization-plan/v1alpha1"
)
N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.simulator-n5-materialization-evidence/v1alpha1"
)
N5_MATERIALIZATION_PLAN_STATUS = "FROZEN_N5_MATERIALIZATION_PLAN"
N5_MATERIALIZATION_STATUS = "COMPLETE"

N5_SOURCE_NODE_ID = "N3"
N5_MATERIALIZER_NODE_ID = "N5"
N5_SOURCE_MEDIA_TYPE = "video/mp4"
N5_TRANSFORMATION_ID = (
    "pathfinder.uniform-midpoint-jpeg-frame-bundle/v1"
)
N5_MATERIALIZATION_HTTP_API_VERSION = (
    "pathfinder.simulator-n5-materialization-http/v1alpha1"
)
N5_MATERIALIZATION_HTTP_EXECUTE_SCHEMA_VERSION = (
    "pathfinder.simulator-n5-materialization-http-execute/v1alpha1"
)
N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION = (
    "pathfinder.simulator-n5-materialization-http-result/v1alpha1"
)

N4_ATOMIC_PUBLICATION_REQUIREMENTS = (
    "stage the complete canonical bundle under its expected SHA-256 and size",
    "verify the N5 plan, object, representation, and manifest bindings",
    "compare-and-swap the expected N4 object-catalog version",
    "make artifact bytes and the catalog entry visible in one commit",
    "return the committed catalog version and publication receipt digest",
    "make retries idempotent and leave no partially visible artifact",
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:-]{0,511}\Z")
_HANDLE = re.compile(r"[0-9a-f]{64}\Z")
_PRIVATE_HOST = re.compile(
    r"pathfinder-sim-[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z"
)
_MAX_SOURCE_BYTES = 4 * 1024 * 1024 * 1024
_ALIGNMENT_STATEMENT = (
    "These JPEG frames were regenerated from the same source video using "
    "the same sampling algorithm and are aligned with the frozen sampling "
    "metadata: identical frame count, frame indices, timestamps, widths, "
    "heights, and declared encoder settings. The JPEG bytes supplied to the "
    "historical description model were not retained, so this artifact does "
    "NOT claim byte identity with the historical visual input."
)

_PLAN_KEYS = frozenset({
    "schema_version",
    "status",
    "plan_id",
    "idempotency_key",
    "route",
    "input",
    "transformation",
    "expected_output",
    "transformation_contract_sha256",
    "plan_sha256",
    "endpoint_free",
    "llm_required",
    "credentials_recorded",
    "eligible_for_scientific_claims",
})
_ROUTE_KEYS = frozenset({"source_node_id", "materializer_node_id"})
_INPUT_KEYS = frozenset({
    "object_id",
    "source_video_id",
    "source_video_filename",
    "media_type",
    "size_bytes",
    "sha256",
})
_TRANSFORMATION_KEYS = frozenset({
    "transformation_id",
    "sampling_method",
    "frame_count",
    "jpeg_max_dimension",
    "jpeg_quality",
    "jpeg_optimize",
    "source_duration_seconds",
    "source_frame_descriptions",
    "generation_manifest_sha256",
    "software_versions",
    "canonical_archive",
    "sampling_alignment_statement",
})
_SOURCE_DESCRIPTION_KEYS = frozenset({
    "representation_id",
    "portable_path",
    "size_bytes",
    "sha256",
})
_CANONICAL_ARCHIVE_KEYS = frozenset({
    "format",
    "mtime",
    "mode",
    "uid",
    "gid",
    "uname",
    "gname",
    "member_order",
})
_OUTPUT_KEYS = frozenset({
    "object_id",
    "representation_id",
    "media_type",
    "artifact_size_bytes",
    "artifact_sha256",
    "manifest_sha256",
    "frame_count",
    "total_jpeg_bytes",
    "member_count",
})


class N5MaterializationError(RuntimeError):
    """Raised when N5 cannot reproduce the frozen transformation."""


class N5MaterializationConflict(N5MaterializationError):
    """Raised when an idempotency key is reused for another plan."""


class FrameSampler(Protocol):
    def __call__(
        self,
        path: Path,
        *,
        frame_count: int,
        jpeg_max_dimension: int,
    ) -> tuple[list[SampledImage], float]:
        """Decode one content-verified source into ordered JPEG frames."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N5MaterializationError(message)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise N5MaterializationError("value is not canonical JSON") from exc


def _copy_json(value: Any) -> Any:
    return json.loads(_canonical_bytes(value).decode("utf-8"))


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not a lowercase SHA-256",
    )
    return value


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    _require(
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= minimum,
        f"{name} is invalid",
    )
    return value


def _number(value: Any, name: str, *, positive: bool = False) -> float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value)),
        f"{name} is invalid",
    )
    result = float(value)
    _require(result > 0 if positive else result >= 0, f"{name} is invalid")
    return result


def _portable_path(value: Any, name: str) -> str:
    _require(isinstance(value, str) and bool(value), f"{name} is invalid")
    posix = PurePosixPath(value)
    windows = PureWindowsPath(value)
    _require(
        not posix.is_absolute()
        and not windows.is_absolute()
        and not windows.drive
        and ".." not in posix.parts
        and ".." not in windows.parts
        and "\\" not in value,
        f"{name} must be a portable relative path",
    )
    return posix.as_posix()


def _strict_json(raw: bytes, name: str) -> dict[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            _require(key not in result, f"{name} contains duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(
                N5MaterializationError(
                    f"{name} contains non-finite number {item}"
                )
            ),
        )
    except N5MaterializationError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise N5MaterializationError(f"{name} is not valid JSON") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def current_materializer_software_versions() -> dict[str, str | None]:
    """Return decoder/encoder versions that affect deterministic output."""
    versions: dict[str, str | None] = {}
    for name in ("av", "Pillow", "pathfinder-minimal"):
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = None
    return versions


def _software_versions(value: Mapping[str, Any]) -> dict[str, str | None]:
    _require(isinstance(value, Mapping) and bool(value), "software_versions invalid")
    result: dict[str, str | None] = {}
    for raw_name, raw_version in value.items():
        name = _identifier(raw_name, "software version name")
        _require(
            raw_version is None
            or (isinstance(raw_version, str) and bool(raw_version)),
            f"software_versions[{name}] is invalid",
        )
        if isinstance(raw_version, str):
            _require(
                "://" not in raw_version,
                f"software_versions[{name}] contains an endpoint",
            )
        result[name] = raw_version
    return dict(sorted(result.items()))


def _source_description(
    raw: bytes,
    *,
    object_id: str,
    source_video_id: str,
    source_sha256: str,
    frame_count: int,
    jpeg_max_dimension: int,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    value = _strict_json(raw, "sampled-frame description")
    _require(
        value.get("schema_version") == FRAME_SCHEMA_VERSION,
        "sampled-frame description schema changed",
    )
    _require(value.get("object_id") == object_id, "description object changed")
    _require(
        value.get("source_video_id") == source_video_id,
        "description source video changed",
    )
    _require(
        value.get("source_video_sha256") == source_sha256,
        "description source video digest changed",
    )
    sampling = value.get("sampling")
    _require(isinstance(sampling, Mapping), "description sampling is absent")
    _require(
        sampling.get("method") == SAMPLING_METHOD
        and _integer(
            sampling.get("frame_count"),
            "description sampling.frame_count",
            minimum=1,
        )
        == frame_count
        and _integer(
            sampling.get("jpeg_max_dimension"),
            "description sampling.jpeg_max_dimension",
            minimum=1,
        )
        == jpeg_max_dimension,
        "description sampling contract changed",
    )
    duration = _number(
        value.get("source_duration_seconds"),
        "description source_duration_seconds",
        positive=True,
    )
    frames = value.get("frames")
    _require(
        isinstance(frames, list) and len(frames) == frame_count,
        "description frame count changed",
    )
    normalized: list[dict[str, Any]] = []
    for index, frame in enumerate(frames):
        _require(isinstance(frame, Mapping), "description frame is invalid")
        normalized.append({
            "frame_index": _integer(
                frame.get("frame_index"),
                f"description frames[{index}].frame_index",
            ),
            "timestamp_seconds": _number(
                frame.get("timestamp_seconds"),
                f"description frames[{index}].timestamp_seconds",
            ),
            "width": _integer(
                frame.get("width"),
                f"description frames[{index}].width",
                minimum=1,
            ),
            "height": _integer(
                frame.get("height"),
                f"description frames[{index}].height",
                minimum=1,
            ),
        })
        _require(
            normalized[-1]["frame_index"] == index,
            "description frame indices are not contiguous",
        )
    return ({"source_duration_seconds": duration}, tuple(normalized))


def _validate_generation_manifest(
    raw: bytes,
    *,
    object_id: str,
    source_video_filename: str,
    source_video_size_bytes: int,
    source_video_sha256: str,
    description_path: str,
    description_size_bytes: int,
    description_sha256: str,
    frame_count: int,
    jpeg_max_dimension: int,
) -> None:
    value = _strict_json(raw, "generation manifest")
    _require(
        value.get("schema_version") == PREP_SCHEMA_VERSION,
        "generation manifest schema changed",
    )
    _require(
        _integer(
            value.get("frame_count"),
            "generation manifest frame_count",
            minimum=1,
        )
        == frame_count
        and _integer(
            value.get("jpeg_max_dimension"),
            "generation manifest jpeg_max_dimension",
            minimum=1,
        )
        == jpeg_max_dimension,
        "generation manifest sampling contract changed",
    )
    _require(
        value.get("credentials_recorded") is False,
        "generation manifest records credentials",
    )
    objects = value.get("objects")
    _require(isinstance(objects, list), "generation manifest objects missing")
    matches = [
        item
        for item in objects
        if isinstance(item, Mapping) and item.get("object_id") == object_id
    ]
    _require(
        len(matches) == 1,
        "generation manifest does not bind exactly one requested object",
    )
    entry = matches[0]
    source = entry.get("source_video")
    _require(
        isinstance(source, Mapping)
        and source.get("filename") == source_video_filename
        and source.get("size_bytes") == source_video_size_bytes
        and source.get("sha256") == source_video_sha256,
        "generation manifest source-video binding changed",
    )
    representations = entry.get("representations")
    _require(
        isinstance(representations, Mapping),
        "generation manifest representations missing",
    )
    description = representations.get(SOURCE_REPRESENTATION_ID)
    _require(
        isinstance(description, Mapping)
        and description.get("path") == description_path
        and description.get("size_bytes") == description_size_bytes
        and description.get("sha256") == description_sha256,
        "generation manifest sampled-frame binding changed",
    )


def _sample_source(
    source: bytes,
    *,
    filename: str,
    frame_count: int,
    jpeg_max_dimension: int,
    sampler: FrameSampler,
) -> tuple[list[SampledImage], float]:
    with TemporaryDirectory(prefix="pathfinder-n5-materialization-") as root:
        path = Path(root) / filename
        path.write_bytes(source)
        try:
            sampled, duration = sampler(
                path,
                frame_count=frame_count,
                jpeg_max_dimension=jpeg_max_dimension,
            )
        except Exception as exc:
            raise N5MaterializationError("N5 frame sampling failed") from exc
    _require(
        isinstance(sampled, list) and len(sampled) == frame_count,
        "N5 sampler returned the wrong frame count",
    )
    _number(duration, "N5 decoder duration", positive=True)
    for index, frame in enumerate(sampled):
        _require(
            isinstance(frame, SampledImage)
            and frame.frame_index == index
            and isinstance(frame.jpeg_bytes, bytes)
            and bool(frame.jpeg_bytes),
            f"N5 sampled frame {index} is invalid",
        )
        _number(frame.timestamp_seconds, f"N5 frame {index} timestamp")
        _integer(frame.width, f"N5 frame {index} width", minimum=1)
        _integer(frame.height, f"N5 frame {index} height", minimum=1)
    return sampled, float(duration)


def _compare_alignment(
    sampled: Sequence[SampledImage],
    expected: Sequence[Mapping[str, Any]],
) -> None:
    _require(len(sampled) == len(expected), "sampled-frame alignment changed")
    for index, (image, frame) in enumerate(zip(sampled, expected)):
        _require(
            image.frame_index == frame["frame_index"]
            and image.timestamp_seconds == frame["timestamp_seconds"]
            and image.width == frame["width"]
            and image.height == frame["height"],
            f"sampled-frame alignment changed at frame {index}",
        )


def _frame_manifest(
    *,
    source: Mapping[str, Any],
    transformation: Mapping[str, Any],
    sampled: Sequence[SampledImage],
) -> dict[str, Any]:
    frames = []
    for image in sampled:
        member = f"frames/{image.frame_index:03d}.jpg"
        frames.append({
            "frame_index": image.frame_index,
            "timestamp_seconds": image.timestamp_seconds,
            "width": image.width,
            "height": image.height,
            "path": member,
            "jpeg_size_bytes": len(image.jpeg_bytes),
            "jpeg_sha256": _sha256(image.jpeg_bytes),
        })
    description = transformation["source_frame_descriptions"]
    return {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": REPRESENTATION_ID,
        "object_id": source["object_id"],
        "source_video_id": source["source_video_id"],
        "source_video_filename": source["source_video_filename"],
        "source_video_size_bytes": source["size_bytes"],
        "source_video_sha256": source["sha256"],
        "source_duration_seconds": transformation["source_duration_seconds"],
        "sampling": {
            "method": transformation["sampling_method"],
            "frame_count": transformation["frame_count"],
            "jpeg_max_dimension": transformation["jpeg_max_dimension"],
            "jpeg_quality": transformation["jpeg_quality"],
            "jpeg_optimize": transformation["jpeg_optimize"],
        },
        "source_frame_descriptions": {
            "representation_id": description["representation_id"],
            "path": description["portable_path"],
            "sha256": description["sha256"],
        },
        "generation_manifest_sha256": transformation[
            "generation_manifest_sha256"
        ],
        "frames": frames,
        "frame_count": len(frames),
        "total_jpeg_bytes": sum(len(image.jpeg_bytes) for image in sampled),
        "software_versions": transformation["software_versions"],
        "historical_visual_bytes_retained": False,
        "sampling_alignment_statement": transformation[
            "sampling_alignment_statement"
        ],
        "claims_byte_identity_with_historical_visual_input": False,
        "credentials_recorded": False,
        "llm_called": False,
        "network_calls_performed": False,
    }


def _materialize(
    source_bytes: bytes,
    *,
    source: Mapping[str, Any],
    transformation: Mapping[str, Any],
    sampler: FrameSampler,
) -> tuple[bytes, Any]:
    sampled, _duration = _sample_source(
        source_bytes,
        filename=source["source_video_filename"],
        frame_count=transformation["frame_count"],
        jpeg_max_dimension=transformation["jpeg_max_dimension"],
        sampler=sampler,
    )
    manifest = _frame_manifest(
        source=source,
        transformation=transformation,
        sampled=sampled,
    )
    manifest_bytes = (
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False)
        + "\n"
    ).encode("utf-8")
    members = [(OBJECT_MANIFEST_NAME, manifest_bytes)] + [
        (f"frames/{image.frame_index:03d}.jpg", image.jpeg_bytes)
        for image in sampled
    ]
    artifact = deterministic_frame_bundle_tar(members)
    return artifact, manifest


@dataclass(frozen=True)
class FrozenN5Materialization:
    plan: dict[str, Any]
    artifact_bytes: bytes = field(repr=False)


@dataclass(frozen=True)
class N5MaterializationExecution:
    evidence: dict[str, Any]
    artifact_bytes: bytes = field(repr=False)


def freeze_n5_materialization_plan(
    *,
    plan_id: str,
    idempotency_key: str,
    object_id: str,
    source_video_id: str,
    source_video_filename: str,
    source_video_bytes: bytes,
    source_frame_descriptions_path: str,
    source_frame_descriptions_bytes: bytes,
    generation_manifest_bytes: bytes,
    frame_count: int,
    jpeg_max_dimension: int,
    sampler: FrameSampler = sample_video,
    software_versions: Mapping[str, str | None] | None = None,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> FrozenN5Materialization:
    """Materialize once and freeze its exact portable replay contract."""
    plan_id = _identifier(plan_id, "plan_id")
    idempotency_key = _identifier(idempotency_key, "idempotency_key")
    object_id = _identifier(object_id, "object_id")
    source_video_id = _identifier(source_video_id, "source_video_id")
    _require(
        isinstance(source_video_filename, str)
        and Path(source_video_filename).name == source_video_filename
        and PureWindowsPath(source_video_filename).name == source_video_filename,
        "source_video_filename must be a bare filename",
    )
    _require(
        source_video_filename.casefold().endswith(".mp4"),
        "source_video_filename must identify an MP4 file",
    )
    _require(
        isinstance(source_video_bytes, bytes)
        and 0 < len(source_video_bytes) <= _MAX_SOURCE_BYTES,
        "source_video_bytes is invalid",
    )
    _require(
        isinstance(source_frame_descriptions_bytes, bytes)
        and bool(source_frame_descriptions_bytes),
        "source_frame_descriptions_bytes is invalid",
    )
    _require(
        isinstance(generation_manifest_bytes, bytes)
        and bool(generation_manifest_bytes),
        "generation_manifest_bytes is invalid",
    )
    frame_count = _integer(frame_count, "frame_count", minimum=1)
    jpeg_max_dimension = _integer(
        jpeg_max_dimension,
        "jpeg_max_dimension",
        minimum=1,
    )
    _require(
        frame_count <= limits.max_frame_count
        and jpeg_max_dimension <= limits.max_frame_dimension,
        "materialization sampling exceeds the frame-bundle limits",
    )
    source_sha256 = _sha256(source_video_bytes)
    description_path = _portable_path(
        source_frame_descriptions_path,
        "source_frame_descriptions_path",
    )
    description_sha256 = _sha256(source_frame_descriptions_bytes)
    description_summary, expected_frames = _source_description(
        source_frame_descriptions_bytes,
        object_id=object_id,
        source_video_id=source_video_id,
        source_sha256=source_sha256,
        frame_count=frame_count,
        jpeg_max_dimension=jpeg_max_dimension,
    )
    _validate_generation_manifest(
        generation_manifest_bytes,
        object_id=object_id,
        source_video_filename=source_video_filename,
        source_video_size_bytes=len(source_video_bytes),
        source_video_sha256=source_sha256,
        description_path=description_path,
        description_size_bytes=len(source_frame_descriptions_bytes),
        description_sha256=description_sha256,
        frame_count=frame_count,
        jpeg_max_dimension=jpeg_max_dimension,
    )
    source = {
        "object_id": object_id,
        "source_video_id": source_video_id,
        "source_video_filename": source_video_filename,
        "media_type": N5_SOURCE_MEDIA_TYPE,
        "size_bytes": len(source_video_bytes),
        "sha256": source_sha256,
    }
    transformation = {
        "transformation_id": N5_TRANSFORMATION_ID,
        "sampling_method": SAMPLING_METHOD,
        "frame_count": frame_count,
        "jpeg_max_dimension": jpeg_max_dimension,
        "jpeg_quality": JPEG_QUALITY,
        "jpeg_optimize": JPEG_OPTIMIZE,
        "source_duration_seconds": description_summary[
            "source_duration_seconds"
        ],
        "source_frame_descriptions": {
            "representation_id": SOURCE_REPRESENTATION_ID,
            "portable_path": description_path,
            "size_bytes": len(source_frame_descriptions_bytes),
            "sha256": description_sha256,
        },
        "generation_manifest_sha256": _sha256(generation_manifest_bytes),
        "software_versions": _software_versions(
            software_versions
            if software_versions is not None
            else current_materializer_software_versions()
        ),
        "canonical_archive": {
            "format": "USTAR",
            "mtime": 0,
            "mode": 420,
            "uid": 0,
            "gid": 0,
            "uname": "",
            "gname": "",
            "member_order": "lexicographic",
        },
        "sampling_alignment_statement": _ALIGNMENT_STATEMENT,
    }
    artifact, _manifest = _materialize(
        source_video_bytes,
        source=source,
        transformation=transformation,
        sampler=sampler,
    )
    sampled_bundle = validate_frame_bundle_bytes(
        artifact,
        expected_object_id=object_id,
        expected_sha256=_sha256(artifact),
        expected_size_bytes=len(artifact),
        artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
        limits=limits,
    )
    _compare_alignment(sampled_bundle.vision_frames(), expected_frames)
    output = {
        "object_id": object_id,
        "representation_id": REPRESENTATION_ID,
        "media_type": FRAME_BUNDLE_MEDIA_TYPE,
        "artifact_size_bytes": len(artifact),
        "artifact_sha256": _sha256(artifact),
        "manifest_sha256": sampled_bundle.manifest_sha256,
        "frame_count": sampled_bundle.frame_count,
        "total_jpeg_bytes": sampled_bundle.total_jpeg_bytes,
        "member_count": sampled_bundle.member_count,
    }
    plan: dict[str, Any] = {
        "schema_version": N5_MATERIALIZATION_PLAN_SCHEMA_VERSION,
        "status": N5_MATERIALIZATION_PLAN_STATUS,
        "plan_id": plan_id,
        "idempotency_key": idempotency_key,
        "route": {
            "source_node_id": N5_SOURCE_NODE_ID,
            "materializer_node_id": N5_MATERIALIZER_NODE_ID,
        },
        "input": source,
        "transformation": transformation,
        "expected_output": output,
        "transformation_contract_sha256": _sha256(
            _canonical_bytes(transformation)
        ),
        "endpoint_free": True,
        "llm_required": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256(_canonical_bytes(plan))
    verified = verify_n5_materialization_plan(plan, limits=limits)
    return FrozenN5Materialization(
        plan=verified,
        artifact_bytes=artifact,
    )


def verify_n5_materialization_plan(
    plan: Mapping[str, Any],
    *,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> dict[str, Any]:
    """Validate and normalize an endpoint-free frozen N5 plan."""
    _require(isinstance(plan, Mapping), "N5 plan must be an object")
    value = _copy_json(dict(plan))
    _require(set(value) == _PLAN_KEYS, "N5 plan field set changed")
    _require(
        value.get("schema_version") == N5_MATERIALIZATION_PLAN_SCHEMA_VERSION
        and value.get("status") == N5_MATERIALIZATION_PLAN_STATUS,
        "N5 plan schema or status changed",
    )
    _identifier(value.get("plan_id"), "plan_id")
    _identifier(value.get("idempotency_key"), "idempotency_key")
    route = value.get("route")
    _require(
        isinstance(route, dict)
        and set(route) == _ROUTE_KEYS
        and route.get("source_node_id") == N5_SOURCE_NODE_ID
        and route.get("materializer_node_id") == N5_MATERIALIZER_NODE_ID,
        "N5 route binding changed",
    )
    source = value.get("input")
    _require(
        isinstance(source, dict) and set(source) == _INPUT_KEYS,
        "N5 input binding changed",
    )
    _identifier(source.get("object_id"), "input.object_id")
    _identifier(source.get("source_video_id"), "input.source_video_id")
    _require(
        isinstance(source.get("source_video_filename"), str)
        and Path(source["source_video_filename"]).name
        == source["source_video_filename"]
        and PureWindowsPath(source["source_video_filename"]).name
        == source["source_video_filename"],
        "input source_video_filename is invalid",
    )
    _require(
        source["source_video_filename"].casefold().endswith(".mp4"),
        "input source_video_filename must identify an MP4 file",
    )
    _require(
        source.get("media_type") == N5_SOURCE_MEDIA_TYPE,
        "N5 input media type changed",
    )
    size = _integer(source.get("size_bytes"), "input.size_bytes", minimum=1)
    _require(size <= _MAX_SOURCE_BYTES, "N5 input exceeds its safety limit")
    _digest(source.get("sha256"), "input.sha256")

    transform = value.get("transformation")
    _require(
        isinstance(transform, dict)
        and set(transform) == _TRANSFORMATION_KEYS,
        "N5 transformation contract changed",
    )
    _require(
        transform.get("transformation_id") == N5_TRANSFORMATION_ID
        and transform.get("sampling_method") == SAMPLING_METHOD
        and transform.get("jpeg_quality") == JPEG_QUALITY
        and transform.get("jpeg_optimize") is JPEG_OPTIMIZE,
        "N5 deterministic transformation changed",
    )
    frames = _integer(
        transform.get("frame_count"),
        "transformation.frame_count",
        minimum=1,
    )
    dimension = _integer(
        transform.get("jpeg_max_dimension"),
        "transformation.jpeg_max_dimension",
        minimum=1,
    )
    _require(
        frames <= limits.max_frame_count
        and dimension <= limits.max_frame_dimension,
        "N5 transformation exceeds frame-bundle limits",
    )
    _number(
        transform.get("source_duration_seconds"),
        "transformation.source_duration_seconds",
        positive=True,
    )
    description = transform.get("source_frame_descriptions")
    _require(
        isinstance(description, dict)
        and set(description) == _SOURCE_DESCRIPTION_KEYS
        and description.get("representation_id")
        == SOURCE_REPRESENTATION_ID,
        "N5 source description binding changed",
    )
    _portable_path(description.get("portable_path"), "description path")
    _integer(description.get("size_bytes"), "description size", minimum=1)
    _digest(description.get("sha256"), "description sha256")
    _digest(
        transform.get("generation_manifest_sha256"),
        "generation_manifest_sha256",
    )
    versions = _software_versions(transform.get("software_versions"))
    _require(
        versions == transform["software_versions"],
        "software_versions are not canonical",
    )
    archive = transform.get("canonical_archive")
    _require(
        isinstance(archive, dict)
        and set(archive) == _CANONICAL_ARCHIVE_KEYS
        and archive
        == {
            "format": "USTAR",
            "mtime": 0,
            "mode": 420,
            "uid": 0,
            "gid": 0,
            "uname": "",
            "gname": "",
            "member_order": "lexicographic",
        },
        "N5 canonical archive contract changed",
    )
    _require(
        transform.get("sampling_alignment_statement") == _ALIGNMENT_STATEMENT,
        "N5 alignment statement changed",
    )
    _require(
        value.get("transformation_contract_sha256")
        == _sha256(_canonical_bytes(transform)),
        "N5 transformation contract digest changed",
    )

    output = value.get("expected_output")
    _require(
        isinstance(output, dict) and set(output) == _OUTPUT_KEYS,
        "N5 expected output binding changed",
    )
    _require(
        output.get("object_id") == source["object_id"]
        and output.get("representation_id") == REPRESENTATION_ID
        and output.get("media_type") == FRAME_BUNDLE_MEDIA_TYPE
        and output.get("frame_count") == frames,
        "N5 expected output identity changed",
    )
    _integer(
        output.get("artifact_size_bytes"),
        "expected_output.artifact_size_bytes",
        minimum=1,
    )
    _digest(output.get("artifact_sha256"), "expected_output.artifact_sha256")
    _digest(output.get("manifest_sha256"), "expected_output.manifest_sha256")
    _integer(
        output.get("total_jpeg_bytes"),
        "expected_output.total_jpeg_bytes",
        minimum=1,
    )
    _integer(
        output.get("member_count"),
        "expected_output.member_count",
        minimum=2,
    )
    _require(
        value.get("endpoint_free") is True
        and value.get("llm_required") is False
        and value.get("credentials_recorded") is False
        and value.get("eligible_for_scientific_claims") is False,
        "N5 plan provenance flags changed",
    )
    recorded = _digest(value.get("plan_sha256"), "plan_sha256")
    unsigned = dict(value)
    unsigned.pop("plan_sha256")
    _require(
        recorded == _sha256(_canonical_bytes(unsigned)),
        "N5 plan digest changed",
    )
    raw = _canonical_bytes(value)
    _require(
        b"://" not in raw
        and b"api_key" not in raw.lower()
        and b"password" not in raw.lower()
        and b"bearer" not in raw.lower(),
        "N5 portable plan contains endpoint or credential material",
    )
    return value


@dataclass
class _ExecutionState:
    plan_sha256: str
    running: bool = False
    artifact_bytes: bytes | None = None
    evidence: dict[str, Any] | None = None


class N5MaterializationRuntime:
    """Thread-safe local N5 executor for frozen deterministic plans."""

    def __init__(
        self,
        *,
        sampler: FrameSampler = sample_video,
        software_versions: Mapping[str, str | None] | None = None,
        limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
        max_source_bytes: int = _MAX_SOURCE_BYTES,
        node_id: str = N5_MATERIALIZER_NODE_ID,
    ) -> None:
        _require(node_id == N5_MATERIALIZER_NODE_ID, "runtime node must be N5")
        self._sampler = sampler
        self._software_versions = _software_versions(
            software_versions
            if software_versions is not None
            else current_materializer_software_versions()
        )
        self._limits = limits
        self._max_source_bytes = _integer(
            max_source_bytes,
            "max_source_bytes",
            minimum=1,
        )
        self._condition = threading.Condition()
        self._states: dict[str, _ExecutionState] = {}

    def execute(
        self,
        plan: Mapping[str, Any],
        source_video_bytes: bytes,
    ) -> N5MaterializationExecution:
        frozen = verify_n5_materialization_plan(plan, limits=self._limits)
        source = frozen["input"]
        _require(
            isinstance(source_video_bytes, bytes)
            and 0 < len(source_video_bytes) <= self._max_source_bytes,
            "runtime source bytes exceed their safety limit",
        )
        _require(
            len(source_video_bytes) == source["size_bytes"]
            and _sha256(source_video_bytes) == source["sha256"],
            "runtime source bytes do not match the frozen input binding",
        )
        _require(
            self._software_versions
            == frozen["transformation"]["software_versions"],
            "runtime decoder/encoder software differs from the frozen plan",
        )
        key = frozen["idempotency_key"]
        digest = frozen["plan_sha256"]
        with self._condition:
            state = self._states.get(key)
            if state is None:
                state = _ExecutionState(plan_sha256=digest)
                self._states[key] = state
            elif state.plan_sha256 != digest:
                raise N5MaterializationConflict(
                    "N5 idempotency_key was reused for a different plan"
                )
            while state.running:
                self._condition.wait()
            if state.evidence is not None and state.artifact_bytes is not None:
                evidence = _copy_json(state.evidence)
                evidence["idempotent_replay"] = True
                return N5MaterializationExecution(
                    evidence=evidence,
                    artifact_bytes=state.artifact_bytes,
                )
            state.running = True
        try:
            execution = self._execute_once(frozen, source_video_bytes)
        except BaseException:
            with self._condition:
                state.running = False
                self._condition.notify_all()
            raise
        with self._condition:
            state.artifact_bytes = execution.artifact_bytes
            state.evidence = _copy_json(execution.evidence)
            state.running = False
            self._condition.notify_all()
        return N5MaterializationExecution(
            evidence=_copy_json(execution.evidence),
            artifact_bytes=execution.artifact_bytes,
        )

    def _execute_once(
        self,
        plan: Mapping[str, Any],
        source_video_bytes: bytes,
    ) -> N5MaterializationExecution:
        artifact, _manifest = _materialize(
            source_video_bytes,
            source=plan["input"],
            transformation=plan["transformation"],
            sampler=self._sampler,
        )
        expected = plan["expected_output"]
        _require(
            len(artifact) == expected["artifact_size_bytes"]
            and _sha256(artifact) == expected["artifact_sha256"],
            "N5 output differs from the frozen artifact binding",
        )
        bundle = validate_frame_bundle_bytes(
            artifact,
            expected_object_id=plan["input"]["object_id"],
            expected_sha256=expected["artifact_sha256"],
            expected_size_bytes=expected["artifact_size_bytes"],
            artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
            limits=self._limits,
        )
        _require(
            bundle.manifest_sha256 == expected["manifest_sha256"]
            and bundle.frame_count == expected["frame_count"]
            and bundle.total_jpeg_bytes == expected["total_jpeg_bytes"]
            and bundle.member_count == expected["member_count"],
            "N5 validated output metadata differs from the frozen binding",
        )
        evidence = {
            "schema_version": N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION,
            "status": N5_MATERIALIZATION_STATUS,
            "plan_id": plan["plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "idempotency_key": plan["idempotency_key"],
            "idempotent_replay": False,
            "route": dict(plan["route"]),
            "input": {
                "object_id": plan["input"]["object_id"],
                "source_video_id": plan["input"]["source_video_id"],
                "media_type": plan["input"]["media_type"],
                "size_bytes": plan["input"]["size_bytes"],
                "sha256": plan["input"]["sha256"],
            },
            "transformation_contract_sha256": plan[
                "transformation_contract_sha256"
            ],
            "output": dict(expected),
            "input_content_binding_verified": True,
            "canonical_frame_bundle_verified": True,
            "output_binding_verified": True,
            "logical_n5_execution_binding_verified": True,
            "physical_host_identity_verified": False,
            "source_delivery_telemetry_verified": False,
            "llm_called": False,
            "semantic_digest_generated": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        return N5MaterializationExecution(
            evidence=evidence,
            artifact_bytes=artifact,
        )


class N5MaterializationHttpError(N5MaterializationError):
    """Raised when the deployment HTTP protocol fails closed."""


def _bearer_token(value: Any, name: str = "bearer_token") -> str:
    _require(isinstance(value, str), f"{name} must be a string")
    encoded = value.encode("utf-8")
    _require(1 <= len(encoded) <= 8192, f"{name} has an invalid length")
    _require(value == value.strip(), f"{name} has surrounding whitespace")
    _require(
        all(character not in value for character in "\r\n\x00"),
        f"{name} contains a control character",
    )
    return value


def _is_loopback(hostname: str | None) -> bool:
    if hostname is None:
        return False
    if hostname.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _private_http_hosts(value: Sequence[str]) -> tuple[str, ...]:
    hosts = tuple(str(host).casefold() for host in value)
    _require(
        len(hosts) == len(set(hosts))
        and all(_PRIVATE_HOST.fullmatch(host) is not None for host in hosts),
        "simulator_private_http_hosts is invalid",
    )
    return hosts


def _http_origin(value: Any, private_hosts: tuple[str, ...]) -> str:
    _require(isinstance(value, str) and value, "base_url must be a string")
    _require(value == value.strip(), "base_url has surrounding whitespace")
    parsed = urllib.parse.urlsplit(value)
    _require(
        parsed.scheme in {"http", "https"} and parsed.hostname is not None,
        "base_url must be an absolute HTTP(S) origin",
    )
    _require(
        parsed.username is None and parsed.password is None,
        "base_url must not contain credentials",
    )
    _require(
        parsed.path in {"", "/"} and not parsed.query and not parsed.fragment,
        "base_url must name an origin without path, query, or fragment",
    )
    try:
        port = parsed.port
    except ValueError as exc:
        raise N5MaterializationHttpError("base_url port is invalid") from exc
    hostname = str(parsed.hostname).casefold()
    simulator_private = hostname.startswith("pathfinder-sim-")
    _require(
        not simulator_private or hostname in private_hosts,
        "base_url simulator-private host is not explicitly bound",
    )
    _require(
        parsed.scheme == "https"
        or _is_loopback(hostname)
        or hostname in private_hosts,
        "base_url must use HTTPS, loopback, or an explicitly bound "
        "Pathfinder simulator-private host",
    )
    _require(
        parsed.scheme == "https" or port is not None,
        "an HTTP base_url must include an explicit port",
    )
    return value.rstrip("/")


@dataclass(frozen=True)
class N5MaterializationHttpClientConfig:
    """Ephemeral N5 endpoint and credential configuration.

    The token is intentionally excluded from repr, equality, plans, results,
    and evidence.  This configuration is process state, not a frozen input.
    """

    base_url: str = field(repr=False)
    bearer_token: str = field(repr=False, compare=False)
    simulator_private_http_hosts: tuple[str, ...] = ()
    timeout_seconds: float = 300.0
    max_json_bytes: int = 8 * 1024 * 1024
    max_source_bytes: int = _MAX_SOURCE_BYTES
    max_result_bytes: int = DEFAULT_FRAME_BUNDLE_LIMITS.max_artifact_bytes

    def __post_init__(self) -> None:
        hosts = _private_http_hosts(self.simulator_private_http_hosts)
        object.__setattr__(self, "simulator_private_http_hosts", hosts)
        object.__setattr__(self, "base_url", _http_origin(self.base_url, hosts))
        _bearer_token(self.bearer_token)
        _number(self.timeout_seconds, "timeout_seconds", positive=True)
        _integer(self.max_json_bytes, "max_json_bytes", minimum=1)
        _integer(self.max_source_bytes, "max_source_bytes", minimum=1)
        _integer(self.max_result_bytes, "max_result_bytes", minimum=1)


@dataclass(frozen=True)
class N5MaterializationHttpExecution:
    """Verified client-side result without endpoint or credential material."""

    evidence: dict[str, Any]
    transport_receipt: dict[str, Any]
    artifact_bytes: bytes


@dataclass
class _StoredResult:
    payload: bytes
    output: dict[str, Any]


class N5MaterializationHttpService:
    """Deployment-only, restart-safe handle service around the runtime.

    Source bytes, result bytes, evidence, and request/plan bindings live in a
    dedicated SQLite database.  A completed request therefore survives a
    service restart.  A request interrupted before its atomic completion is
    deterministically recomputed.  The state directory is deployment state;
    it is never embedded in a portable plan, result, or evidence document.

    One running service owns a state directory at a time.  SQLite serializes
    writes and the in-process condition serializes duplicate live requests;
    coordinating multiple live service processes over one directory is out of
    scope for this adapter.
    """

    def __init__(
        self,
        *,
        runtime: N5MaterializationRuntime,
        bearer_token: str,
        state_dir: Path,
        max_source_bytes: int = _MAX_SOURCE_BYTES,
        max_json_bytes: int = 8 * 1024 * 1024,
        max_result_bytes: int = DEFAULT_FRAME_BUNDLE_LIMITS.max_artifact_bytes,
    ) -> None:
        self._runtime = runtime
        self._bearer_token = _bearer_token(bearer_token)
        self.max_source_bytes = _integer(
            max_source_bytes,
            "max_source_bytes",
            minimum=1,
        )
        self.max_json_bytes = _integer(
            max_json_bytes,
            "max_json_bytes",
            minimum=1,
        )
        self.max_result_bytes = _integer(
            max_result_bytes,
            "max_result_bytes",
            minimum=1,
        )
        self.runtime_epoch = uuid.uuid4().hex
        _require(isinstance(state_dir, Path), "state_dir must be a Path")
        state_dir.mkdir(parents=True, exist_ok=True)
        _require(state_dir.is_dir(), "state_dir must be a directory")
        self._database_path = state_dir / "n5-materialization.sqlite3"
        self._condition = threading.Condition(threading.RLock())
        self._inflight: set[str] = set()
        self._initialize_database()

    def _database(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database_path,
            timeout=30.0,
            isolation_level=None,
        )
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize_database(self) -> None:
        with closing(self._database()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            _require(version in {0, 1}, "N5 state database version is unsupported")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS staged_sources (
                    source_handle TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS materialized_results (
                    result_handle TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    output_json BLOB NOT NULL,
                    payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS materialization_requests (
                    request_id TEXT PRIMARY KEY,
                    plan_sha256 TEXT NOT NULL,
                    source_handle TEXT NOT NULL,
                    status TEXT NOT NULL,
                    owner_epoch TEXT NOT NULL,
                    result_handle TEXT,
                    evidence_json BLOB,
                    FOREIGN KEY(source_handle)
                        REFERENCES staged_sources(source_handle),
                    FOREIGN KEY(result_handle)
                        REFERENCES materialized_results(result_handle)
                );
                """
            )
            connection.execute("PRAGMA user_version = 1")
            integrity = connection.execute("PRAGMA quick_check").fetchone()[0]
            _require(integrity == "ok", "N5 state database integrity failed")

    def authorized(self, authorization: str | None) -> bool:
        expected = "Bearer " + self._bearer_token
        return (
            isinstance(authorization, str)
            and hmac.compare_digest(authorization, expected)
        )

    def health(self) -> dict[str, Any]:
        return {
            "api_version": N5_MATERIALIZATION_HTTP_API_VERSION,
            "status": "ok",
            "node_id": N5_MATERIALIZER_NODE_ID,
            "runtime_epoch": self.runtime_epoch,
            "plan_schema_version": N5_MATERIALIZATION_PLAN_SCHEMA_VERSION,
            "evidence_schema_version": (
                N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION
            ),
            "input_mode": "authenticated-staged-binary-handle",
            "output_mode": "authenticated-binary-handle",
            "handle_durability": "sqlite-restart-safe",
            "request_idempotency": "request-id-and-plan-sha256",
            "credentials_recorded": False,
        }

    def stage_source(
        self,
        *,
        source_handle: str,
        payload: bytes,
        declared_sha256: str,
    ) -> dict[str, Any]:
        _require(
            isinstance(source_handle, str)
            and _HANDLE.fullmatch(source_handle) is not None,
            "source handle is invalid",
        )
        _digest(declared_sha256, "declared source SHA-256")
        _require(isinstance(payload, bytes), "source payload must be bytes")
        _require(
            1 <= len(payload) <= self.max_source_bytes,
            "source payload size is outside the configured limit",
        )
        actual = _sha256(payload)
        _require(
            actual == source_handle and actual == declared_sha256,
            "source payload content binding failed",
        )
        replay = False
        with closing(self._database()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT size_bytes, content_sha256, payload
                FROM staged_sources
                WHERE source_handle = ?
                """,
                (source_handle,),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO staged_sources (
                        source_handle,
                        size_bytes,
                        content_sha256,
                        payload
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        source_handle,
                        len(payload),
                        actual,
                        sqlite3.Binary(payload),
                    ),
                )
            else:
                replay = True
                _require(
                    row[0] == len(payload)
                    and row[1] == actual
                    and bytes(row[2]) == payload,
                    "source handle conflicts with durable content",
                )
            connection.commit()
        return {
            "schema_version": N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION,
            "status": "STAGED",
            "source_handle": source_handle,
            "content_sha256": actual,
            "size_bytes": len(payload),
            "idempotent_replay": replay,
            "credentials_recorded": False,
        }

    def execute(
        self,
        *,
        request_id: str,
        source_handle: str,
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        verified = verify_n5_materialization_plan(plan)
        _require(
            self._bearer_token.encode("utf-8")
            not in _canonical_bytes(verified),
            "frozen plan contains configured credential material",
        )
        expected_source = verified["input"]
        _require(
            source_handle == expected_source["sha256"],
            "source handle does not match the frozen input",
        )
        _identifier(request_id, "request_id")
        _require(
            request_id == verified["idempotency_key"],
            "request ID does not match the frozen idempotency key",
        )
        plan_sha256 = verified["plan_sha256"]
        while True:
            with self._condition:
                with closing(self._database()) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    source_row = connection.execute(
                        """
                        SELECT size_bytes, content_sha256, payload
                        FROM staged_sources
                        WHERE source_handle = ?
                        """,
                        (source_handle,),
                    ).fetchone()
                    _require(source_row is not None, "source handle was not staged")
                    _require(
                        source_row[0] == expected_source["size_bytes"]
                        and source_row[1] == expected_source["sha256"]
                        and _sha256(bytes(source_row[2]))
                        == expected_source["sha256"],
                        "durable source content binding failed",
                    )
                    request_row = connection.execute(
                        """
                        SELECT
                            plan_sha256,
                            source_handle,
                            status,
                            result_handle,
                            evidence_json
                        FROM materialization_requests
                        WHERE request_id = ?
                        """,
                        (request_id,),
                    ).fetchone()
                    if request_row is not None:
                        if (
                            request_row[0] != plan_sha256
                            or request_row[1] != source_handle
                        ):
                            connection.rollback()
                            raise N5MaterializationConflict(
                                "request ID was reused for another plan"
                            )
                        if request_row[2] == "COMPLETE":
                            _require(
                                request_row[3]
                                == verified["expected_output"]["artifact_sha256"]
                                and isinstance(request_row[4], bytes),
                                "durable completed request binding failed",
                            )
                            evidence = _strict_json(
                                bytes(request_row[4]),
                                "durable N5 evidence",
                            )
                            result_handle = request_row[3]
                            connection.commit()
                            stored = self.result(result_handle)
                            _require(
                                stored.output == verified["expected_output"]
                                and evidence.get("schema_version")
                                == N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION
                                and evidence.get("status")
                                == N5_MATERIALIZATION_STATUS
                                and evidence.get("plan_sha256") == plan_sha256
                                and evidence.get("idempotency_key") == request_id
                                and evidence.get("output")
                                == verified["expected_output"]
                                and evidence.get("credentials_recorded") is False,
                                "durable N5 evidence binding failed",
                            )
                            return self._response(
                                verified=verified,
                                source_handle=source_handle,
                                result_handle=result_handle,
                                evidence=evidence,
                                replay=True,
                            )
                        if request_id in self._inflight:
                            connection.commit()
                            self._condition.wait()
                            continue
                        connection.execute(
                            """
                            UPDATE materialization_requests
                            SET owner_epoch = ?
                            WHERE request_id = ?
                            """,
                            (self.runtime_epoch, request_id),
                        )
                    else:
                        connection.execute(
                            """
                            INSERT INTO materialization_requests (
                                request_id,
                                plan_sha256,
                                source_handle,
                                status,
                                owner_epoch
                            ) VALUES (?, ?, ?, 'RUNNING', ?)
                            """,
                            (
                                request_id,
                                plan_sha256,
                                source_handle,
                                self.runtime_epoch,
                            ),
                        )
                    connection.commit()
                self._inflight.add(request_id)
                source_payload = bytes(source_row[2])
                break
        try:
            execution = self._runtime.execute(verified, source_payload)
            output = dict(verified["expected_output"])
            result_handle = output["artifact_sha256"]
            _require(
                len(execution.artifact_bytes) <= self.max_result_bytes,
                "materialized result exceeds the configured limit",
            )
            with self._condition:
                with closing(self._database()) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    current = connection.execute(
                        """
                        SELECT size_bytes, content_sha256, output_json, payload
                        FROM materialized_results
                        WHERE result_handle = ?
                        """,
                        (result_handle,),
                    ).fetchone()
                    output_bytes = _canonical_bytes(output)
                    if current is None:
                        connection.execute(
                            """
                            INSERT INTO materialized_results (
                                result_handle,
                                size_bytes,
                                content_sha256,
                                output_json,
                                payload
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                result_handle,
                                len(execution.artifact_bytes),
                                result_handle,
                                sqlite3.Binary(output_bytes),
                                sqlite3.Binary(execution.artifact_bytes),
                            ),
                        )
                    else:
                        _require(
                            current[0] == len(execution.artifact_bytes)
                            and current[1] == result_handle
                            and bytes(current[2]) == output_bytes
                            and bytes(current[3]) == execution.artifact_bytes,
                            "result handle conflicts with durable output",
                        )
                    evidence_bytes = _canonical_bytes(execution.evidence)
                    _require(
                        self._bearer_token.encode("utf-8")
                        not in evidence_bytes,
                        "N5 evidence contains configured credential material",
                    )
                    updated = connection.execute(
                        """
                        UPDATE materialization_requests
                        SET
                            status = 'COMPLETE',
                            result_handle = ?,
                            evidence_json = ?
                        WHERE request_id = ?
                          AND plan_sha256 = ?
                          AND status = 'RUNNING'
                          AND owner_epoch = ?
                        """,
                        (
                            result_handle,
                            sqlite3.Binary(evidence_bytes),
                            request_id,
                            plan_sha256,
                            self.runtime_epoch,
                        ),
                    ).rowcount
                    _require(updated == 1, "durable request ownership changed")
                    connection.commit()
                self._inflight.discard(request_id)
                self._condition.notify_all()
        except BaseException:
            with self._condition:
                with closing(self._database()) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        """
                        DELETE FROM materialization_requests
                        WHERE request_id = ?
                          AND plan_sha256 = ?
                          AND status = 'RUNNING'
                          AND owner_epoch = ?
                        """,
                        (request_id, plan_sha256, self.runtime_epoch),
                    )
                    connection.commit()
                self._inflight.discard(request_id)
                self._condition.notify_all()
            raise
        return self._response(
            verified=verified,
            source_handle=source_handle,
            result_handle=result_handle,
            evidence=execution.evidence,
            replay=False,
        )

    def _response(
        self,
        *,
        verified: Mapping[str, Any],
        source_handle: str,
        result_handle: str,
        evidence: Mapping[str, Any],
        replay: bool,
    ) -> dict[str, Any]:
        safe_evidence = _copy_json(evidence)
        safe_evidence["idempotent_replay"] = replay
        return {
            "schema_version": N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION,
            "status": N5_MATERIALIZATION_STATUS,
            "node_id": N5_MATERIALIZER_NODE_ID,
            "runtime_epoch": self.runtime_epoch,
            "source_handle": source_handle,
            "result_handle": result_handle,
            "plan_sha256": verified["plan_sha256"],
            "request_id": verified["idempotency_key"],
            "output": dict(verified["expected_output"]),
            "evidence": safe_evidence,
            "idempotent_replay": replay,
            "credentials_recorded": False,
        }

    def result(self, result_handle: str) -> _StoredResult:
        _require(
            isinstance(result_handle, str)
            and _HANDLE.fullmatch(result_handle) is not None,
            "result handle is invalid",
        )
        with closing(self._database()) as connection:
            row = connection.execute(
                """
                SELECT size_bytes, content_sha256, output_json, payload
                FROM materialized_results
                WHERE result_handle = ?
                """,
                (result_handle,),
            ).fetchone()
        _require(row is not None, "result handle was not found")
        output = _strict_json(bytes(row[2]), "durable N5 output binding")
        payload = bytes(row[3])
        _require(
            row[0] == len(payload)
            and row[1] == result_handle
            and _sha256(payload) == result_handle,
            "durable result content binding failed",
        )
        return _StoredResult(
            payload=payload,
            output=output,
        )


class _N5RequestError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class _N5HttpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "PathfinderN5/1"
    sys_version = ""

    @property
    def _service(self) -> N5MaterializationHttpService:
        return self.server.materialization_service  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        del format, args

    def _json_response(
        self,
        status: int,
        value: Mapping[str, Any],
        *,
        extra_headers: Mapping[str, str] | None = None,
    ) -> None:
        payload = _canonical_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if extra_headers is not None:
            for name, header_value in extra_headers.items():
                self.send_header(name, header_value)
        self.end_headers()
        self.wfile.write(payload)

    def _error(self, status: int, code: str, message: str) -> None:
        headers = {"WWW-Authenticate": "Bearer"} if status == 401 else None
        self._json_response(
            status,
            {
                "api_version": N5_MATERIALIZATION_HTTP_API_VERSION,
                "status": "error",
                "error": {"code": code, "message": message},
                "credentials_recorded": False,
            },
            extra_headers=headers,
        )

    def _authorized(self) -> None:
        values = self.headers.get_all("Authorization") or []
        if len(values) != 1 or not self._service.authorized(values[0]):
            raise _N5RequestError(
                401,
                "missing or invalid N5 materialization bearer token",
            )

    def _body(self, *, limit: int, media_type: str) -> bytes:
        if self.headers.get("Transfer-Encoding") is not None:
            raise _N5RequestError(400, "transfer encoding is not supported")
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1 or re.fullmatch(r"[0-9]+", lengths[0]) is None:
            raise _N5RequestError(411, "one valid Content-Length is required")
        length = int(lengths[0])
        if not 1 <= length <= limit:
            raise _N5RequestError(413, "request body size is outside its limit")
        content_type = self.headers.get_content_type()
        if content_type != media_type:
            raise _N5RequestError(415, "request media type is not supported")
        payload = self.rfile.read(length)
        if len(payload) != length:
            raise _N5RequestError(400, "request body was truncated")
        return payload

    def _dispatch(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
            raise _N5RequestError(404, "route not found")
        path = parsed.path
        if self.command == "GET" and path == "/healthz":
            self._json_response(200, self._service.health())
            return
        if path.startswith("/v1/materialization-inputs/"):
            self._authorized()
            if self.command != "PUT":
                raise _N5RequestError(405, "method not allowed")
            handle = path.removeprefix("/v1/materialization-inputs/")
            payload = self._body(
                limit=self._service.max_source_bytes,
                media_type=N5_SOURCE_MEDIA_TYPE,
            )
            digests = self.headers.get_all(
                "X-Pathfinder-Content-SHA256"
            ) or []
            if len(digests) != 1:
                raise _N5RequestError(400, "one content digest is required")
            declared = digests[0]
            try:
                result = self._service.stage_source(
                    source_handle=handle,
                    payload=payload,
                    declared_sha256=declared,
                )
            except N5MaterializationConflict as exc:
                raise _N5RequestError(409, "source handle conflict") from exc
            except N5MaterializationError as exc:
                raise _N5RequestError(400, "invalid source binding") from exc
            self._json_response(
                200 if result["idempotent_replay"] else 201,
                result,
            )
            return
        if path == "/v1/materializations/execute":
            self._authorized()
            if self.command != "POST":
                raise _N5RequestError(405, "method not allowed")
            payload = self._body(
                limit=self._service.max_json_bytes,
                media_type="application/json",
            )
            try:
                request = _strict_json(payload, "N5 HTTP execute request")
                _require(
                    set(request)
                    == {"schema_version", "request_id", "source_handle", "plan"},
                    "execute request fields changed",
                )
                _require(
                    request["schema_version"]
                    == N5_MATERIALIZATION_HTTP_EXECUTE_SCHEMA_VERSION,
                    "execute request schema changed",
                )
                _require(
                    isinstance(request["plan"], dict),
                    "execute plan must be an object",
                )
                result = self._service.execute(
                    request_id=request["request_id"],
                    source_handle=request["source_handle"],
                    plan=request["plan"],
                )
            except N5MaterializationConflict as exc:
                raise _N5RequestError(409, "materialization conflict") from exc
            except N5MaterializationError as exc:
                raise _N5RequestError(
                    400,
                    "invalid materialization request",
                ) from exc
            self._json_response(200, result)
            return
        if path.startswith("/v1/materialization-results/"):
            self._authorized()
            if self.command != "GET":
                raise _N5RequestError(405, "method not allowed")
            handle = path.removeprefix("/v1/materialization-results/")
            try:
                stored = self._service.result(handle)
            except N5MaterializationError as exc:
                raise _N5RequestError(404, "result handle not found") from exc
            self.send_response(200)
            self.send_header("Content-Type", FRAME_BUNDLE_MEDIA_TYPE)
            self.send_header("Content-Length", str(len(stored.payload)))
            self.send_header("X-Pathfinder-Content-SHA256", handle)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(stored.payload)
            return
        raise _N5RequestError(404, "route not found")

    def _handle(self) -> None:
        try:
            self._dispatch()
        except _N5RequestError as exc:
            codes = {
                400: "bad_request",
                401: "unauthorized",
                404: "not_found",
                405: "method_not_allowed",
                409: "conflict",
                411: "length_required",
                413: "payload_too_large",
                415: "unsupported_media_type",
            }
            self._error(
                exc.status,
                codes.get(exc.status, "request_failed"),
                exc.message,
            )
        except Exception:
            self._error(500, "internal_error", "N5 materialization failed")

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def do_PUT(self) -> None:  # noqa: N802
        self._handle()

    def do_DELETE(self) -> None:  # noqa: N802
        self._handle()


class N5MaterializationHttpServer(ThreadingHTTPServer):
    """Threaded deployment adapter exposing one N5 service."""

    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        service: N5MaterializationHttpService,
    ) -> None:
        self.materialization_service = service
        super().__init__(server_address, _N5HttpHandler)


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Any,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> None:
        del request, file_pointer, code, message, headers, new_url
        return None


class HttpN5MaterializationClient:
    """Proxy-free, redirect-free client for one exact configured N5 origin."""

    def __init__(self, config: N5MaterializationHttpClientConfig) -> None:
        self._config = config
        self._base_url = config.base_url
        self._authorization = "Bearer " + config.bearer_token
        self._open = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _RejectRedirects(),
        ).open

    def _request(
        self,
        path: str,
        *,
        method: str,
        body: bytes | None = None,
        content_type: str | None = None,
        authenticated: bool = True,
        max_bytes: int,
        extra_headers: Mapping[str, str] | None = None,
    ) -> tuple[bytes, Any, int]:
        _require(path.startswith("/") and "://" not in path, "unsafe HTTP path")
        headers = {
            "Accept-Encoding": "identity",
            "User-Agent": "pathfinder-n5-materialization/1",
        }
        if authenticated:
            headers["Authorization"] = self._authorization
        if content_type is not None:
            headers["Content-Type"] = content_type
        if extra_headers is not None:
            headers.update(extra_headers)
        request = urllib.request.Request(
            self._base_url + path,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with self._open(
                request,
                timeout=float(self._config.timeout_seconds),
            ) as response:
                lengths = response.headers.get_all("Content-Length") or []
                _require(
                    len(lengths) == 1
                    and re.fullmatch(r"[0-9]+", lengths[0]) is not None,
                    "N5 response lacks a valid Content-Length",
                )
                declared_length = int(lengths[0])
                _require(
                    declared_length <= max_bytes,
                    "N5 response exceeds its byte limit",
                )
                _require(
                    response.headers.get("Content-Encoding") in {None, "identity"},
                    "N5 response used unexpected content encoding",
                )
                payload = response.read(max_bytes + 1)
                _require(
                    len(payload) == declared_length and len(payload) <= max_bytes,
                    "N5 response length binding failed",
                )
                return payload, response.headers, response.status
        except urllib.error.HTTPError as exc:
            raise N5MaterializationHttpError(
                f"N5 endpoint returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise N5MaterializationHttpError(
                "N5 endpoint is unreachable"
            ) from exc

    def _json_request(
        self,
        path: str,
        *,
        method: str,
        value: Mapping[str, Any] | None = None,
        authenticated: bool = True,
    ) -> tuple[dict[str, Any], int]:
        body = None if value is None else _canonical_bytes(value)
        if body is not None:
            _require(
                len(body) <= self._config.max_json_bytes,
                "N5 JSON request exceeds its byte limit",
            )
        payload, headers, status = self._request(
            path,
            method=method,
            body=body,
            content_type="application/json" if body is not None else None,
            authenticated=authenticated,
            max_bytes=self._config.max_json_bytes,
        )
        _require(
            headers.get_content_type() == "application/json",
            "N5 endpoint returned non-JSON content",
        )
        _require(
            self._config.bearer_token.encode("utf-8") not in payload,
            "N5 response contains configured credential material",
        )
        return _strict_json(payload, "N5 HTTP response"), status

    def health(self) -> dict[str, Any]:
        value, status = self._json_request(
            "/healthz",
            method="GET",
            authenticated=False,
        )
        _require(status == 200, "N5 health returned an unexpected status")
        expected_keys = {
            "api_version",
            "status",
            "node_id",
            "runtime_epoch",
            "plan_schema_version",
            "evidence_schema_version",
            "input_mode",
            "output_mode",
            "handle_durability",
            "request_idempotency",
            "credentials_recorded",
        }
        _require(set(value) == expected_keys, "N5 health fields changed")
        _require(
            value["api_version"] == N5_MATERIALIZATION_HTTP_API_VERSION
            and value["status"] == "ok"
            and value["node_id"] == N5_MATERIALIZER_NODE_ID,
            "N5 health identity is invalid",
        )
        _require(
            isinstance(value["runtime_epoch"], str)
            and re.fullmatch(r"[0-9a-f]{32}", value["runtime_epoch"])
            is not None,
            "N5 runtime epoch is invalid",
        )
        _require(
            value["plan_schema_version"]
            == N5_MATERIALIZATION_PLAN_SCHEMA_VERSION
            and value["evidence_schema_version"]
            == N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION,
            "N5 portable contract version changed",
        )
        _require(
            value["input_mode"] == "authenticated-staged-binary-handle"
            and value["output_mode"] == "authenticated-binary-handle"
            and value["handle_durability"] == "sqlite-restart-safe"
            and value["request_idempotency"]
            == "request-id-and-plan-sha256"
            and value["credentials_recorded"] is False,
            "N5 deployment mode is invalid",
        )
        return value

    def stage_source(
        self,
        plan: Mapping[str, Any],
        source_video_bytes: bytes,
    ) -> str:
        verified = verify_n5_materialization_plan(plan)
        _require(
            isinstance(source_video_bytes, bytes),
            "source_video_bytes must be bytes",
        )
        expected = verified["input"]
        _require(
            len(source_video_bytes) == expected["size_bytes"]
            and _sha256(source_video_bytes) == expected["sha256"],
            "local source bytes do not match the frozen input binding",
        )
        _require(
            len(source_video_bytes) <= self._config.max_source_bytes,
            "source bytes exceed the client limit",
        )
        handle = expected["sha256"]
        payload, headers, status = self._request(
            "/v1/materialization-inputs/" + handle,
            method="PUT",
            body=source_video_bytes,
            content_type=N5_SOURCE_MEDIA_TYPE,
            max_bytes=self._config.max_json_bytes,
            extra_headers={"X-Pathfinder-Content-SHA256": handle},
        )
        _require(status in {200, 201}, "N5 source stage status is invalid")
        _require(
            headers.get_content_type() == "application/json",
            "N5 source stage returned non-JSON content",
        )
        result = _strict_json(payload, "N5 source stage response")
        _require(
            result.get("status") == "STAGED"
            and result.get("source_handle") == handle
            and result.get("content_sha256") == handle
            and result.get("size_bytes") == len(source_video_bytes)
            and result.get("credentials_recorded") is False,
            "N5 source stage response binding failed",
        )
        return handle

    def submit(
        self,
        plan: Mapping[str, Any],
        source_handle: str,
    ) -> dict[str, Any]:
        verified = verify_n5_materialization_plan(plan)
        _require(
            source_handle == verified["input"]["sha256"],
            "source handle does not match the frozen plan",
        )
        value, status = self._json_request(
            "/v1/materializations/execute",
            method="POST",
            value={
                "schema_version": (
                    N5_MATERIALIZATION_HTTP_EXECUTE_SCHEMA_VERSION
                ),
                "request_id": verified["idempotency_key"],
                "source_handle": source_handle,
                "plan": verified,
            },
        )
        _require(status == 200, "N5 execute status is invalid")
        expected_keys = {
            "schema_version",
            "status",
            "node_id",
            "runtime_epoch",
            "source_handle",
            "result_handle",
            "plan_sha256",
            "request_id",
            "output",
            "evidence",
            "idempotent_replay",
            "credentials_recorded",
        }
        _require(set(value) == expected_keys, "N5 execute response fields changed")
        output = verified["expected_output"]
        _require(
            value["schema_version"]
            == N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION
            and value["status"] == N5_MATERIALIZATION_STATUS
            and value["node_id"] == N5_MATERIALIZER_NODE_ID
            and value["source_handle"] == source_handle
            and value["result_handle"] == output["artifact_sha256"]
            and value["plan_sha256"] == verified["plan_sha256"]
            and value["request_id"] == verified["idempotency_key"]
            and value["output"] == output
            and isinstance(value["idempotent_replay"], bool)
            and value["credentials_recorded"] is False,
            "N5 execute response binding failed",
        )
        evidence = value["evidence"]
        _require(
            isinstance(evidence, dict)
            and evidence.get("schema_version")
            == N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION
            and evidence.get("plan_sha256") == verified["plan_sha256"]
            and evidence.get("output") == output
            and evidence.get("idempotent_replay")
            is value["idempotent_replay"]
            and evidence.get("credentials_recorded") is False,
            "N5 materialization evidence binding failed",
        )
        return value

    def fetch_result(
        self,
        result: Mapping[str, Any],
    ) -> bytes:
        _require(isinstance(result, Mapping), "N5 result must be an object")
        handle = result.get("result_handle")
        output = result.get("output")
        _require(
            isinstance(handle, str) and _HANDLE.fullmatch(handle) is not None,
            "N5 result handle is invalid",
        )
        _require(isinstance(output, dict), "N5 result output is invalid")
        expected_size = _integer(
            output.get("artifact_size_bytes"),
            "result artifact_size_bytes",
            minimum=1,
        )
        expected_sha256 = _digest(
            output.get("artifact_sha256"),
            "result artifact_sha256",
        )
        _require(handle == expected_sha256, "N5 result handle binding failed")
        _require(
            expected_size <= self._config.max_result_bytes,
            "N5 result exceeds the client limit",
        )
        payload, headers, status = self._request(
            "/v1/materialization-results/" + handle,
            method="GET",
            max_bytes=self._config.max_result_bytes,
        )
        _require(status == 200, "N5 result download status is invalid")
        _require(
            headers.get_content_type() == FRAME_BUNDLE_MEDIA_TYPE,
            "N5 result media type changed",
        )
        response_digests = headers.get_all(
            "X-Pathfinder-Content-SHA256"
        ) or []
        _require(
            response_digests == [handle]
            and len(payload) == expected_size
            and _sha256(payload) == expected_sha256,
            "N5 result content binding failed",
        )
        return payload

    def execute(
        self,
        plan: Mapping[str, Any],
        source_video_bytes: bytes,
    ) -> N5MaterializationHttpExecution:
        verified = verify_n5_materialization_plan(plan)
        before = self.health()
        source_handle = self.stage_source(verified, source_video_bytes)
        result = self.submit(verified, source_handle)
        _require(
            result["runtime_epoch"] == before["runtime_epoch"],
            "N5 runtime changed before execution completed",
        )
        artifact = self.fetch_result(result)
        after = self.health()
        _require(
            after["runtime_epoch"] == before["runtime_epoch"],
            "N5 runtime changed during materialization",
        )
        output = verified["expected_output"]
        bundle = validate_frame_bundle_bytes(
            artifact,
            expected_object_id=verified["input"]["object_id"],
            expected_sha256=output["artifact_sha256"],
            expected_size_bytes=output["artifact_size_bytes"],
            artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
        )
        _require(
            bundle.manifest_sha256 == output["manifest_sha256"]
            and bundle.frame_count == output["frame_count"]
            and bundle.total_jpeg_bytes == output["total_jpeg_bytes"]
            and bundle.member_count == output["member_count"],
            "downloaded canonical bundle metadata binding failed",
        )
        receipt = {
            "schema_version": N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION,
            "status": "VERIFIED",
            "node_id": N5_MATERIALIZER_NODE_ID,
            "runtime_epoch": before["runtime_epoch"],
            "source_handle": source_handle,
            "result_handle": result["result_handle"],
            "plan_sha256": verified["plan_sha256"],
            "artifact_size_bytes": len(artifact),
            "artifact_sha256": _sha256(artifact),
            "source_content_binding_verified": True,
            "result_content_binding_verified": True,
            "stable_runtime_epoch_verified": True,
            "redirects_followed": False,
            "ambient_proxies_used": False,
            "credentials_recorded": False,
        }
        return N5MaterializationHttpExecution(
            evidence=_copy_json(result["evidence"]),
            transport_receipt=receipt,
            artifact_bytes=artifact,
        )


__all__ = [
    "N4_ATOMIC_PUBLICATION_REQUIREMENTS",
    "N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION",
    "N5_MATERIALIZATION_HTTP_API_VERSION",
    "N5_MATERIALIZATION_HTTP_EXECUTE_SCHEMA_VERSION",
    "N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION",
    "N5_MATERIALIZATION_PLAN_SCHEMA_VERSION",
    "N5_MATERIALIZATION_PLAN_STATUS",
    "N5_MATERIALIZATION_STATUS",
    "N5_MATERIALIZER_NODE_ID",
    "N5_SOURCE_MEDIA_TYPE",
    "N5_SOURCE_NODE_ID",
    "N5_TRANSFORMATION_ID",
    "FrameSampler",
    "FrozenN5Materialization",
    "N5MaterializationConflict",
    "N5MaterializationError",
    "N5MaterializationExecution",
    "N5MaterializationHttpClientConfig",
    "N5MaterializationHttpError",
    "N5MaterializationHttpExecution",
    "N5MaterializationHttpServer",
    "N5MaterializationHttpService",
    "N5MaterializationRuntime",
    "HttpN5MaterializationClient",
    "current_materializer_software_versions",
    "freeze_n5_materialization_plan",
    "verify_n5_materialization_plan",
]
