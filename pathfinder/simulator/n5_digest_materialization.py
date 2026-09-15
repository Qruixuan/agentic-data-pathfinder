"""Auditable N5 materialization of question-independent video digests.

The frozen plan contains content identities, a frame-sampling contract, and
the exact model identifier, but no deployment address or secret.  Runtime
generation accepts an injected vision adapter and never manufactures a
semantic digest from hashes, filenames, or synthetic fixtures.

The OpenAI-compatible adapter in this module is a deployment binding only.
Its address and key live in memory and are intentionally absent from plans,
evidence, and the materialized representation.
"""

from __future__ import annotations

import base64
import ipaddress
import json
import math
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

from ..video_prep import SampledImage, sample_video


N5_DIGEST_PLAN_SCHEMA_VERSION = (
    "pathfinder.simulator-n5-digest-materialization-plan/v1alpha1"
)
N5_DIGEST_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.simulator-n5-digest-materialization-evidence/v1alpha1"
)
VISION_DIGEST_ADAPTER_PROTOCOL = (
    "pathfinder.simulator-vision-digest-adapter/v1alpha1"
)
OPENAI_VISION_DIGEST_ADAPTER_ID = "openai-compatible-vision-digest-v1"
REPRESENTATION_ID = "multimodal_digest"
SOURCE_REPRESENTATION_ID = "raw_video"
MEDIA_TYPE = "text/plain; charset=utf-8"
PLAN_NAME = "n5-digest-materialization-plan.json"
EVIDENCE_NAME = "n5-digest-materialization-evidence.json"
DIGEST_NAME = "multimodal_digest.txt"
CHECKSUMS_NAME = "SHA256SUMS"

SAMPLING_METHOD = "uniform-midpoint-video-frame-sampling-v1"
JPEG_QUALITY = 82
JPEG_OPTIMIZE = True
DEFAULT_MAX_DIGEST_BYTES = 256 * 1024
MAX_FRAME_COUNT = 32
MAX_FRAME_BYTES = 512 * 1024
MAX_TOTAL_FRAME_BYTES = 8 * 1024 * 1024
MAX_IMAGE_DIMENSION = 8192
MAX_TOTAL_IMAGE_PIXELS = 64 * 1024 * 1024
MAX_REQUEST_BYTES = 12 * 1024 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024

DIGEST_VISION_PROMPT = """Create a question-independent temporal digest from
the chronologically ordered sampled video frames. Use only visible facts. Do
not infer an evaluation question, intended answer, hidden event, motivation,
or event outside the supplied frames.

Return exactly one JSON object with this shape:
{"events":[{"start_seconds":0.0,"end_seconds":null,
"description":"factual event"}],"summary":"concise factual summary"}

Events must be chronological. Return JSON only, without Markdown fences."""

_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/+-]*")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SIMULATOR_HOST = re.compile(r"pathfinder-sim-[a-z0-9][a-z0-9.-]*")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
_URL_PREFIXES = ("http://", "https://", "file://", "ssh://")


class N5DigestMaterializationError(ValueError):
    """Raised when a digest plan, adapter response, or artifact is invalid."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise N5DigestMaterializationError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise N5DigestMaterializationError(f"non-finite JSON number: {value}")


def _decode_json(value: bytes, label: str) -> Any:
    try:
        return json.loads(
            value.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise N5DigestMaterializationError(
            f"{label} is not canonical UTF-8 JSON"
        ) from exc


def _read_json(path: Path, label: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise N5DigestMaterializationError(f"cannot read {label}") from exc
    return raw, _decode_json(raw, label)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{label} must be an object")
    return value


def _strict_keys(
    value: Mapping[str, Any],
    expected: set[str],
    label: str,
) -> None:
    _require(
        set(value) == expected,
        f"{label} fields changed: expected {sorted(expected)}, got "
        f"{sorted(value)}",
    )


def _string(value: Any, label: str, *, maximum: int = 4096) -> str:
    _require(
        isinstance(value, str)
        and value == value.strip()
        and bool(value)
        and len(value.encode("utf-8")) <= maximum,
        f"{label} must be a bounded, non-empty, trimmed string",
    )
    _require(
        all(character >= " " for character in value),
        f"{label} contains a control character",
    )
    return value


def _identifier(value: Any, label: str) -> str:
    result = _string(value, label, maximum=256)
    _require(bool(_SAFE_ID.fullmatch(result)), f"{label} is not portable")
    return result


def _model_id(value: Any) -> str:
    result = _string(value, "model_id", maximum=256)
    _require(bool(_MODEL_ID.fullmatch(result)), "model_id is not portable")
    _require(
        not result.startswith(("/", "\\"))
        and not _WINDOWS_ABSOLUTE.match(result),
        "model_id cannot be an absolute path",
    )
    return result


def _integer(
    value: Any,
    label: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    _require(type(value) is int and value >= minimum, f"{label} is invalid")
    if maximum is not None:
        _require(value <= maximum, f"{label} exceeds its limit")
    return value


def _number(value: Any, label: str, *, positive: bool = False) -> float:
    _require(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value)),
        f"{label} must be finite",
    )
    result = float(value)
    _require(
        result > 0 if positive else result >= 0,
        f"{label} has an invalid sign",
    )
    return result


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _sha256_file(path: Path) -> tuple[int, str]:
    digest = sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while block := handle.read(1024 * 1024):
                size += len(block)
                digest.update(block)
    except OSError as exc:
        raise N5DigestMaterializationError("cannot read source video") from exc
    return size, digest.hexdigest()


def _validate_mp4(path: Path) -> tuple[int, str]:
    _require(path.is_file() and not path.is_symlink(), "source video is invalid")
    size, digest = _sha256_file(path)
    _require(size >= 16, "source video is too short")
    try:
        with path.open("rb") as handle:
            prefix = handle.read(64)
    except OSError as exc:
        raise N5DigestMaterializationError("cannot inspect source video") from exc
    _require(b"ftyp" in prefix[4:32], "source video is not an MP4 artifact")
    return size, digest


def _assert_safe_document(value: Any, label: str = "document") -> None:
    if isinstance(value, Mapping):
        forbidden = {
            "api_key",
            "authorization",
            "base_url",
            "bearer",
            "credential",
            "endpoint",
            "host_path",
            "password",
            "secret",
            "token",
            "url",
        }
        for key, child in value.items():
            _require(
                key.lower() not in forbidden,
                f"{label} contains a deployment-only field: {key}",
            )
            _assert_safe_document(child, f"{label}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_safe_document(child, f"{label}[{index}]")
        return
    if isinstance(value, str):
        lowered = value.lower()
        _require(
            not lowered.startswith(_URL_PREFIXES),
            f"{label} contains a deployment address",
        )
        _require(
            not value.startswith(("/", "\\\\"))
            and not _WINDOWS_ABSOLUTE.match(value),
            f"{label} contains an absolute host path",
        )


@dataclass(frozen=True)
class DigestFrameBinding:
    """Frozen identity and alignment metadata for one sampled JPEG."""

    frame_index: int
    timestamp_seconds: float
    width: int
    height: int
    jpeg_size_bytes: int
    jpeg_sha256: str

    @classmethod
    def from_sampled_image(cls, image: SampledImage) -> "DigestFrameBinding":
        _validate_sampled_image(image)
        return cls(
            frame_index=image.frame_index,
            timestamp_seconds=image.timestamp_seconds,
            width=image.width,
            height=image.height,
            jpeg_size_bytes=len(image.jpeg_bytes),
            jpeg_sha256=_sha256_bytes(image.jpeg_bytes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "frame_index": self.frame_index,
            "timestamp_seconds": self.timestamp_seconds,
            "width": self.width,
            "height": self.height,
            "jpeg_size_bytes": self.jpeg_size_bytes,
            "jpeg_sha256": self.jpeg_sha256,
        }


@dataclass(frozen=True)
class VisionDigestResult:
    """Normalized semantic result returned by an injected vision adapter."""

    model_id: str
    digest: Mapping[str, Any]
    response_sha256: str
    protocol_attempts: int
    llm_called: bool
    adapter_id: str


class VisionDigestAdapter(Protocol):
    """Runtime-only interface; implementations must not enter frozen plans."""

    def generate_digest(
        self,
        *,
        object_id: str,
        frames: Sequence[SampledImage],
        duration_seconds: float,
        expected_model_id: str,
        seed: int,
    ) -> VisionDigestResult:
        """Generate one semantic digest from bounded chronological frames."""


def _validate_sampled_image(image: SampledImage) -> None:
    _require(isinstance(image, SampledImage), "sampled frame type is invalid")
    _integer(image.frame_index, "frame_index")
    _number(image.timestamp_seconds, "timestamp_seconds")
    _integer(image.width, "frame width", minimum=1, maximum=MAX_IMAGE_DIMENSION)
    _integer(image.height, "frame height", minimum=1, maximum=MAX_IMAGE_DIMENSION)
    _require(
        isinstance(image.jpeg_bytes, bytes)
        and 4 <= len(image.jpeg_bytes) <= MAX_FRAME_BYTES
        and image.jpeg_bytes.startswith(b"\xff\xd8")
        and image.jpeg_bytes.endswith(b"\xff\xd9"),
        "sampled frame is not a bounded JPEG",
    )


def _frame_bindings(
    frames: Sequence[SampledImage],
    *,
    duration_seconds: float,
    jpeg_max_dimension: int,
) -> list[dict[str, Any]]:
    _require(
        isinstance(frames, Sequence)
        and 1 <= len(frames) <= MAX_FRAME_COUNT,
        "sampled frame count is invalid",
    )
    total_bytes = 0
    total_pixels = 0
    rows: list[dict[str, Any]] = []
    previous_timestamp = -math.inf
    for index, image in enumerate(frames):
        _validate_sampled_image(image)
        _require(image.frame_index == index, "sampled frame indexes are not dense")
        _require(
            image.timestamp_seconds >= previous_timestamp
            and image.timestamp_seconds <= duration_seconds,
            "sampled frame timestamps are not aligned with source duration",
        )
        _require(
            image.width <= jpeg_max_dimension
            and image.height <= jpeg_max_dimension,
            "sampled frame exceeds the frozen dimension limit",
        )
        previous_timestamp = image.timestamp_seconds
        total_bytes += len(image.jpeg_bytes)
        total_pixels += image.width * image.height
        rows.append(DigestFrameBinding.from_sampled_image(image).to_dict())
    _require(total_bytes <= MAX_TOTAL_FRAME_BYTES, "sampled JPEG bytes exceed limit")
    _require(
        total_pixels <= MAX_TOTAL_IMAGE_PIXELS,
        "sampled image pixels exceed limit",
    )
    return rows


def _write_directory_atomic(
    target: Path,
    documents: Mapping[str, bytes],
    *,
    prefix: str,
) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=prefix, dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256_bytes(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def freeze_n5_multimodal_digest_plan(
    source_video_path: str | Path,
    sampled_frames: Sequence[SampledImage],
    *,
    source_duration_seconds: float,
    object_id: str,
    model_id: str,
    output_dir: str | Path,
    plan_id: str,
    jpeg_max_dimension: int = 768,
    seed: int = 0,
    maximum_digest_bytes: int = DEFAULT_MAX_DIGEST_BYTES,
) -> dict[str, Any]:
    """Freeze exact source, sampling, model, and output-bound contracts."""

    source = Path(source_video_path).resolve()
    source_size, source_sha = _validate_mp4(source)
    object_id = _identifier(object_id, "object_id")
    plan_id = _identifier(plan_id, "plan_id")
    model_id = _model_id(model_id)
    duration = round(
        _number(
            source_duration_seconds,
            "source_duration_seconds",
            positive=True,
        ),
        6,
    )
    jpeg_max_dimension = _integer(
        jpeg_max_dimension,
        "jpeg_max_dimension",
        minimum=1,
        maximum=MAX_IMAGE_DIMENSION,
    )
    seed = _integer(seed, "seed")
    maximum_digest_bytes = _integer(
        maximum_digest_bytes,
        "maximum_digest_bytes",
        minimum=1,
        maximum=DEFAULT_MAX_DIGEST_BYTES,
    )
    frames = _frame_bindings(
        sampled_frames,
        duration_seconds=duration,
        jpeg_max_dimension=jpeg_max_dimension,
    )
    sampling_metadata_sha = _sha256_bytes(_canonical_bytes(frames))
    plan: dict[str, Any] = {
        "schema_version": N5_DIGEST_PLAN_SCHEMA_VERSION,
        "status": "FROZEN_N5_MULTIMODAL_DIGEST_PLAN",
        "plan_id": plan_id,
        "node_id": "N5",
        "object_id": object_id,
        "source": {
            "representation_id": SOURCE_REPRESENTATION_ID,
            "media_type": "video/mp4",
            "size_bytes": source_size,
            "sha256": source_sha,
        },
        "sampling": {
            "method": SAMPLING_METHOD,
            "frame_count": len(frames),
            "source_duration_seconds": duration,
            "jpeg_max_dimension": jpeg_max_dimension,
            "jpeg_quality": JPEG_QUALITY,
            "jpeg_optimize": JPEG_OPTIMIZE,
            "frames": frames,
            "metadata_sha256": sampling_metadata_sha,
        },
        "generation": {
            "representation_id": REPRESENTATION_ID,
            "media_type": MEDIA_TYPE,
            "adapter_protocol": VISION_DIGEST_ADAPTER_PROTOCOL,
            "model_id": model_id,
            "seed": seed,
            "prompt_sha256": _sha256_bytes(
                DIGEST_VISION_PROMPT.encode("utf-8")
            ),
            "maximum_digest_bytes": maximum_digest_bytes,
            "semantic_model_call_required": True,
            "synthetic_or_hash_digest_permitted": False,
        },
        "deployment_binding_included": False,
        "source_artifact_included": False,
        "sampled_jpeg_bytes_included": False,
        "expected_digest_content_included": False,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256_bytes(_canonical_bytes(plan))
    _assert_safe_document(plan)
    plan_bytes = _json_bytes(plan)
    documents = {PLAN_NAME: plan_bytes}
    documents[CHECKSUMS_NAME] = _checksum_bytes(documents)
    target = Path(output_dir).resolve()
    _write_directory_atomic(target, documents, prefix=".n5-digest-plan-")
    verified = verify_n5_multimodal_digest_plan(target, source)
    return {**verified, "output_dir": str(target)}


_PLAN_KEYS = {
    "credentials_recorded",
    "deployment_binding_included",
    "eligible_for_scientific_claims",
    "expected_digest_content_included",
    "external_services_called",
    "generation",
    "node_id",
    "object_id",
    "plan_id",
    "plan_sha256",
    "sampled_jpeg_bytes_included",
    "sampling",
    "schema_version",
    "source",
    "source_artifact_included",
    "status",
}


def _verify_checksums(
    root: Path,
    content_names: set[str],
    *,
    label: str,
) -> None:
    expected = content_names | {CHECKSUMS_NAME}
    _require(root.is_dir(), f"{label} directory does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        f"{label} must contain regular files only",
    )
    _require(
        {path.name for path in entries} == expected,
        f"{label} file set changed",
    )
    try:
        lines = (root / CHECKSUMS_NAME).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise N5DigestMaterializationError(
            f"cannot read {label} checksums"
        ) from exc
    _require(len(lines) == len(content_names), f"{label} checksums incomplete")
    seen: set[str] = set()
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  "
            and bool(_SHA256.fullmatch(digest))
            and name in content_names,
            f"{label} SHA256SUMS is malformed",
        )
        _require(name not in seen, f"duplicate {label} checksum: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"{label} checksum mismatch: {name}",
        )
        seen.add(name)
    _require(
        [line.partition("  ")[2] for line in lines] == sorted(content_names),
        f"{label} checksums are not canonical",
    )


def _parse_frame_binding(value: Any, index: int) -> dict[str, Any]:
    row = _mapping(value, f"sampling.frames[{index}]")
    _strict_keys(
        row,
        {
            "frame_index",
            "height",
            "jpeg_sha256",
            "jpeg_size_bytes",
            "timestamp_seconds",
            "width",
        },
        f"sampling.frames[{index}]",
    )
    _require(
        _integer(row["frame_index"], "frame_index") == index,
        "sampling frame indexes are not dense",
    )
    timestamp = _number(row["timestamp_seconds"], "timestamp_seconds")
    width = _integer(
        row["width"], "frame width", minimum=1, maximum=MAX_IMAGE_DIMENSION
    )
    height = _integer(
        row["height"], "frame height", minimum=1, maximum=MAX_IMAGE_DIMENSION
    )
    size = _integer(
        row["jpeg_size_bytes"],
        "jpeg_size_bytes",
        minimum=4,
        maximum=MAX_FRAME_BYTES,
    )
    digest = _string(row["jpeg_sha256"], "jpeg_sha256", maximum=64)
    _require(bool(_SHA256.fullmatch(digest)), "jpeg_sha256 is invalid")
    return {
        "frame_index": index,
        "timestamp_seconds": timestamp,
        "width": width,
        "height": height,
        "jpeg_size_bytes": size,
        "jpeg_sha256": digest,
    }


def _verify_plan_document(value: Any) -> dict[str, Any]:
    plan = dict(_mapping(value, "N5 digest plan"))
    _strict_keys(plan, _PLAN_KEYS, "N5 digest plan")
    _require(
        plan["schema_version"] == N5_DIGEST_PLAN_SCHEMA_VERSION,
        "unsupported N5 digest plan schema_version",
    )
    _require(
        plan["status"] == "FROZEN_N5_MULTIMODAL_DIGEST_PLAN",
        "N5 digest plan is not frozen",
    )
    _identifier(plan["plan_id"], "plan_id")
    _require(plan["node_id"] == "N5", "digest plan node is not N5")
    _identifier(plan["object_id"], "object_id")
    recorded_sha = plan.pop("plan_sha256")
    _require(
        recorded_sha == _sha256_bytes(_canonical_bytes(plan)),
        "N5 digest plan_sha256 mismatch",
    )
    plan["plan_sha256"] = recorded_sha

    source = _mapping(plan["source"], "source")
    _strict_keys(
        source,
        {"media_type", "representation_id", "sha256", "size_bytes"},
        "source",
    )
    _require(
        source["representation_id"] == SOURCE_REPRESENTATION_ID
        and source["media_type"] == "video/mp4",
        "digest source contract changed",
    )
    _integer(source["size_bytes"], "source size", minimum=16)
    _require(
        isinstance(source["sha256"], str)
        and bool(_SHA256.fullmatch(source["sha256"])),
        "source sha256 is invalid",
    )

    sampling = _mapping(plan["sampling"], "sampling")
    _strict_keys(
        sampling,
        {
            "frame_count",
            "frames",
            "jpeg_max_dimension",
            "jpeg_optimize",
            "jpeg_quality",
            "metadata_sha256",
            "method",
            "source_duration_seconds",
        },
        "sampling",
    )
    _require(sampling["method"] == SAMPLING_METHOD, "sampling method changed")
    frame_count = _integer(
        sampling["frame_count"],
        "frame_count",
        minimum=1,
        maximum=MAX_FRAME_COUNT,
    )
    duration = _number(
        sampling["source_duration_seconds"],
        "source_duration_seconds",
        positive=True,
    )
    maximum_dimension = _integer(
        sampling["jpeg_max_dimension"],
        "jpeg_max_dimension",
        minimum=1,
        maximum=MAX_IMAGE_DIMENSION,
    )
    _require(
        sampling["jpeg_quality"] == JPEG_QUALITY
        and sampling["jpeg_optimize"] is JPEG_OPTIMIZE,
        "JPEG sampling contract changed",
    )
    frames_value = sampling["frames"]
    _require(
        isinstance(frames_value, list) and len(frames_value) == frame_count,
        "sampling frame count disagrees",
    )
    frames = [
        _parse_frame_binding(row, index)
        for index, row in enumerate(frames_value)
    ]
    _require(
        all(row["timestamp_seconds"] <= duration for row in frames)
        and all(
            current["timestamp_seconds"] >= previous["timestamp_seconds"]
            for previous, current in zip(frames, frames[1:])
        ),
        "sampling timestamps are not chronological",
    )
    _require(
        all(
            row["width"] <= maximum_dimension
            and row["height"] <= maximum_dimension
            for row in frames
        ),
        "sampling dimensions exceed the plan limit",
    )
    _require(
        sum(row["jpeg_size_bytes"] for row in frames)
        <= MAX_TOTAL_FRAME_BYTES,
        "sampling JPEG bytes exceed the total limit",
    )
    _require(
        sum(row["width"] * row["height"] for row in frames)
        <= MAX_TOTAL_IMAGE_PIXELS,
        "sampling pixels exceed the total limit",
    )
    _require(
        sampling["metadata_sha256"]
        == _sha256_bytes(_canonical_bytes(frames)),
        "sampling metadata digest mismatch",
    )

    generation = _mapping(plan["generation"], "generation")
    _strict_keys(
        generation,
        {
            "adapter_protocol",
            "maximum_digest_bytes",
            "media_type",
            "model_id",
            "prompt_sha256",
            "representation_id",
            "seed",
            "semantic_model_call_required",
            "synthetic_or_hash_digest_permitted",
        },
        "generation",
    )
    _require(
        generation["representation_id"] == REPRESENTATION_ID
        and generation["media_type"] == MEDIA_TYPE
        and generation["adapter_protocol"] == VISION_DIGEST_ADAPTER_PROTOCOL,
        "digest generation contract changed",
    )
    _model_id(generation["model_id"])
    _integer(generation["seed"], "seed")
    _integer(
        generation["maximum_digest_bytes"],
        "maximum_digest_bytes",
        minimum=1,
        maximum=DEFAULT_MAX_DIGEST_BYTES,
    )
    _require(
        generation["prompt_sha256"]
        == _sha256_bytes(DIGEST_VISION_PROMPT.encode("utf-8")),
        "digest prompt binding changed",
    )
    _require(
        generation["semantic_model_call_required"] is True
        and generation["synthetic_or_hash_digest_permitted"] is False,
        "semantic generation safety contract changed",
    )
    for key in (
        "deployment_binding_included",
        "source_artifact_included",
        "sampled_jpeg_bytes_included",
        "expected_digest_content_included",
        "external_services_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(plan[key] is False, f"unsafe N5 plan flag: {key}")
    _assert_safe_document(plan)
    return plan


def verify_n5_multimodal_digest_plan(
    plan_dir: str | Path,
    source_video_path: str | Path,
) -> dict[str, Any]:
    """Verify one frozen plan and its exact raw-video content binding."""

    root = Path(plan_dir).resolve()
    _verify_checksums(root, {PLAN_NAME}, label="N5 digest plan")
    _, value = _read_json(root / PLAN_NAME, "N5 digest plan")
    plan = _verify_plan_document(value)
    source_size, source_sha = _validate_mp4(Path(source_video_path).resolve())
    _require(
        source_size == plan["source"]["size_bytes"]
        and source_sha == plan["source"]["sha256"],
        "source video content binding mismatch",
    )
    return {
        "status": "VERIFIED",
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "object_id": plan["object_id"],
        "model_id": plan["generation"]["model_id"],
        "frame_count": plan["sampling"]["frame_count"],
        "source_video_sha256": plan["source"]["sha256"],
        "deployment_binding_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _validate_digest_payload(
    value: Any,
    *,
    duration_seconds: float,
) -> dict[str, Any]:
    digest = _mapping(value, "vision digest")
    _strict_keys(digest, {"events", "summary"}, "vision digest")
    events_value = digest["events"]
    _require(
        isinstance(events_value, list) and 1 <= len(events_value) <= 256,
        "vision digest events must be a bounded non-empty list",
    )
    events: list[dict[str, Any]] = []
    previous_start = -math.inf
    for index, value in enumerate(events_value):
        event = _mapping(value, f"vision digest event {index}")
        _strict_keys(
            event,
            {"description", "end_seconds", "start_seconds"},
            f"vision digest event {index}",
        )
        start = round(
            _number(event["start_seconds"], "event start_seconds"),
            6,
        )
        _require(
            start >= previous_start and start <= duration_seconds,
            "vision digest events are not chronological",
        )
        previous_start = start
        raw_end = event["end_seconds"]
        end = None
        if raw_end is not None:
            end = round(_number(raw_end, "event end_seconds"), 6)
            _require(
                start <= end <= duration_seconds,
                "vision digest event end is outside the source duration",
            )
        description = _string(
            event["description"],
            "event description",
            maximum=4096,
        )
        events.append({
            "start_seconds": start,
            "end_seconds": end,
            "description": description,
        })
    summary = _string(digest["summary"], "digest summary", maximum=8192)
    return {"events": events, "summary": summary}


def _canonical_digest_bytes(
    object_id: str,
    digest: Mapping[str, Any],
) -> bytes:
    lines = [
        "PATHFINDER QUESTION-INDEPENDENT MULTIMODAL DIGEST",
        f"Object: {object_id}",
        "Timeline:",
    ]
    for event in digest["events"]:
        interval = f"{event['start_seconds']:.3f}s"
        if event["end_seconds"] is not None:
            interval += f"-{event['end_seconds']:.3f}s"
        lines.append(f"- [{interval}] {event['description']}")
    lines.extend(("Summary:", digest["summary"]))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _compare_sampled_frames(
    frames: Sequence[SampledImage],
    *,
    duration_seconds: float,
    plan: Mapping[str, Any],
) -> None:
    sampling = plan["sampling"]
    actual_duration = round(
        _number(duration_seconds, "sampled source duration", positive=True),
        6,
    )
    _require(
        actual_duration == sampling["source_duration_seconds"],
        "sampled source duration differs from the frozen plan",
    )
    actual = _frame_bindings(
        frames,
        duration_seconds=actual_duration,
        jpeg_max_dimension=sampling["jpeg_max_dimension"],
    )
    _require(
        actual == sampling["frames"],
        "sampled JPEG identity or timestamp/dimension alignment changed",
    )


def materialize_n5_multimodal_digest(
    plan_dir: str | Path,
    source_video_path: str | Path,
    *,
    output_dir: str | Path,
    vision_adapter: VisionDigestAdapter,
    sampler: Any = sample_video,
) -> dict[str, Any]:
    """Execute a frozen digest plan through an injected semantic adapter."""

    plan_root = Path(plan_dir).resolve()
    source = Path(source_video_path).resolve()
    verify_n5_multimodal_digest_plan(plan_root, source)
    _, plan_value = _read_json(plan_root / PLAN_NAME, "N5 digest plan")
    plan = _verify_plan_document(plan_value)
    sampling = plan["sampling"]
    try:
        frames, duration = sampler(
            source,
            frame_count=sampling["frame_count"],
            jpeg_max_dimension=sampling["jpeg_max_dimension"],
        )
    except N5DigestMaterializationError:
        raise
    except Exception as exc:
        raise N5DigestMaterializationError("N5 video sampling failed") from exc
    _require(isinstance(frames, list), "N5 sampler returned an invalid frame list")
    _compare_sampled_frames(frames, duration_seconds=duration, plan=plan)

    generation = plan["generation"]
    try:
        result = vision_adapter.generate_digest(
            object_id=plan["object_id"],
            frames=tuple(frames),
            duration_seconds=sampling["source_duration_seconds"],
            expected_model_id=generation["model_id"],
            seed=generation["seed"],
        )
    except N5DigestMaterializationError:
        raise
    except Exception as exc:
        raise N5DigestMaterializationError(
            "vision-digest adapter failed; no fallback digest was generated"
        ) from exc
    _require(
        isinstance(result, VisionDigestResult),
        "vision-digest adapter returned an invalid result type",
    )
    _require(
        result.llm_called is True,
        "vision-digest adapter did not attest a semantic model call",
    )
    _require(
        _model_id(result.model_id) == generation["model_id"],
        "vision-digest model ID differs from the frozen plan",
    )
    _require(
        isinstance(result.response_sha256, str)
        and bool(_SHA256.fullmatch(result.response_sha256)),
        "vision-digest response SHA-256 is invalid",
    )
    attempts = _integer(
        result.protocol_attempts,
        "vision-digest protocol_attempts",
        minimum=1,
        maximum=10,
    )
    adapter_id = _identifier(result.adapter_id, "vision adapter_id")
    digest = _validate_digest_payload(
        result.digest,
        duration_seconds=sampling["source_duration_seconds"],
    )
    digest_bytes = _canonical_digest_bytes(plan["object_id"], digest)
    _require(
        len(digest_bytes) <= generation["maximum_digest_bytes"],
        "canonical multimodal digest exceeds the frozen output limit",
    )
    digest_sha = _sha256_bytes(digest_bytes)
    evidence = {
        "schema_version": N5_DIGEST_EVIDENCE_SCHEMA_VERSION,
        "status": "COMPLETE",
        "evidence_class": "runtime-vision-model-materialization",
        "node_id": "N5",
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "object_id": plan["object_id"],
        "source_video_size_bytes": plan["source"]["size_bytes"],
        "source_video_sha256": plan["source"]["sha256"],
        "sampling_metadata_sha256": sampling["metadata_sha256"],
        "sampling_alignment_verified": True,
        "frame_count": sampling["frame_count"],
        "adapter_protocol": generation["adapter_protocol"],
        "adapter_id": adapter_id,
        "model_id": result.model_id,
        "prompt_sha256": generation["prompt_sha256"],
        "protocol_attempts": attempts,
        "completion_response_sha256": result.response_sha256,
        "digest_document": digest,
        "output_binding": {
            "artifact_name": DIGEST_NAME,
            "representation_id": REPRESENTATION_ID,
            "media_type": MEDIA_TYPE,
            "size_bytes": len(digest_bytes),
            "sha256": digest_sha,
        },
        "llm_called": True,
        "synthetic_or_hash_digest_used": False,
        "deployment_binding_included": False,
        "source_artifact_included": False,
        "sampled_jpeg_bytes_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    _assert_safe_document(evidence)
    evidence_bytes = _json_bytes(evidence)
    documents = {
        DIGEST_NAME: digest_bytes,
        EVIDENCE_NAME: evidence_bytes,
    }
    documents[CHECKSUMS_NAME] = _checksum_bytes(documents)
    target = Path(output_dir).resolve()
    _write_directory_atomic(target, documents, prefix=".n5-digest-output-")
    verified = verify_n5_multimodal_digest_materialization(
        target,
        plan_root,
        source,
    )
    return {**verified, "output_dir": str(target)}


_EVIDENCE_KEYS = {
    "adapter_id",
    "adapter_protocol",
    "completion_response_sha256",
    "credentials_recorded",
    "deployment_binding_included",
    "digest_document",
    "eligible_for_scientific_claims",
    "evidence_class",
    "frame_count",
    "llm_called",
    "model_id",
    "node_id",
    "object_id",
    "output_binding",
    "plan_id",
    "plan_sha256",
    "prompt_sha256",
    "protocol_attempts",
    "sampled_jpeg_bytes_included",
    "sampling_alignment_verified",
    "sampling_metadata_sha256",
    "schema_version",
    "source_artifact_included",
    "source_video_sha256",
    "source_video_size_bytes",
    "status",
    "synthetic_or_hash_digest_used",
}


def verify_n5_multimodal_digest_materialization(
    output_dir: str | Path,
    plan_dir: str | Path,
    source_video_path: str | Path,
) -> dict[str, Any]:
    """Verify output bytes, semantic evidence, and all frozen bindings."""

    root = Path(output_dir).resolve()
    _verify_checksums(
        root,
        {DIGEST_NAME, EVIDENCE_NAME},
        label="N5 digest materialization",
    )
    plan_root = Path(plan_dir).resolve()
    verify_n5_multimodal_digest_plan(plan_root, source_video_path)
    _, plan_value = _read_json(plan_root / PLAN_NAME, "N5 digest plan")
    plan = _verify_plan_document(plan_value)
    _, evidence_value = _read_json(
        root / EVIDENCE_NAME,
        "N5 digest evidence",
    )
    evidence = _mapping(evidence_value, "N5 digest evidence")
    _strict_keys(evidence, _EVIDENCE_KEYS, "N5 digest evidence")
    _require(
        evidence["schema_version"] == N5_DIGEST_EVIDENCE_SCHEMA_VERSION
        and evidence["status"] == "COMPLETE"
        and evidence["evidence_class"]
        == "runtime-vision-model-materialization",
        "N5 digest evidence status or schema changed",
    )
    _require(
        evidence["node_id"] == "N5"
        and evidence["plan_id"] == plan["plan_id"]
        and evidence["plan_sha256"] == plan["plan_sha256"]
        and evidence["object_id"] == plan["object_id"],
        "N5 digest evidence plan identity changed",
    )
    _require(
        evidence["source_video_size_bytes"] == plan["source"]["size_bytes"]
        and evidence["source_video_sha256"] == plan["source"]["sha256"]
        and evidence["sampling_metadata_sha256"]
        == plan["sampling"]["metadata_sha256"]
        and evidence["frame_count"] == plan["sampling"]["frame_count"],
        "N5 digest source or sampling evidence changed",
    )
    generation = plan["generation"]
    _require(
        evidence["adapter_protocol"] == VISION_DIGEST_ADAPTER_PROTOCOL
        and evidence["model_id"] == generation["model_id"]
        and evidence["prompt_sha256"] == generation["prompt_sha256"],
        "N5 digest model or adapter binding changed",
    )
    _identifier(evidence["adapter_id"], "adapter_id")
    _integer(
        evidence["protocol_attempts"],
        "protocol_attempts",
        minimum=1,
        maximum=10,
    )
    _require(
        isinstance(evidence["completion_response_sha256"], str)
        and bool(_SHA256.fullmatch(evidence[
            "completion_response_sha256"
        ])),
        "completion response digest is invalid",
    )
    for key, expected in (
        ("sampling_alignment_verified", True),
        ("llm_called", True),
        ("synthetic_or_hash_digest_used", False),
        ("deployment_binding_included", False),
        ("source_artifact_included", False),
        ("sampled_jpeg_bytes_included", False),
        ("credentials_recorded", False),
        ("eligible_for_scientific_claims", False),
    ):
        _require(evidence[key] is expected, f"unsafe evidence flag: {key}")
    digest = _validate_digest_payload(
        evidence["digest_document"],
        duration_seconds=plan["sampling"]["source_duration_seconds"],
    )
    expected_digest = _canonical_digest_bytes(plan["object_id"], digest)
    try:
        actual_digest = (root / DIGEST_NAME).read_bytes()
    except OSError as exc:
        raise N5DigestMaterializationError("cannot read digest artifact") from exc
    _require(
        actual_digest == expected_digest,
        "multimodal digest is not the canonical evidence representation",
    )
    _require(
        len(actual_digest) <= generation["maximum_digest_bytes"],
        "multimodal digest exceeds its frozen size bound",
    )
    output = _mapping(evidence["output_binding"], "output binding")
    _strict_keys(
        output,
        {
            "artifact_name",
            "media_type",
            "representation_id",
            "sha256",
            "size_bytes",
        },
        "output binding",
    )
    _require(
        output["artifact_name"] == DIGEST_NAME
        and output["representation_id"] == REPRESENTATION_ID
        and output["media_type"] == MEDIA_TYPE
        and output["size_bytes"] == len(actual_digest)
        and output["sha256"] == _sha256_bytes(actual_digest),
        "multimodal digest output binding mismatch",
    )
    _assert_safe_document(evidence)
    return {
        "status": "VERIFIED",
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "object_id": plan["object_id"],
        "representation_id": REPRESENTATION_ID,
        "model_id": evidence["model_id"],
        "frame_count": evidence["frame_count"],
        "digest_size_bytes": len(actual_digest),
        "digest_sha256": _sha256_bytes(actual_digest),
        "llm_called": True,
        "sampling_alignment_verified": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Request,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> None:
        return None


def _normalise_deployment_base(
    value: str,
    allowed_http_simulator_hosts: Sequence[str],
) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        "vision service base address is required",
    )
    try:
        parsed = urlsplit(value.strip())
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise N5DigestMaterializationError(
            "vision service base address is invalid"
        ) from exc
    _require(
        parsed.scheme in {"http", "https"} and hostname is not None,
        "vision service base address must use HTTP(S)",
    )
    _require(
        parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment,
        "vision service base address cannot contain credentials or parameters",
    )
    _require(port is None or 1 <= port <= 65535, "vision service port is invalid")
    allowed: set[str] = set()
    for item in allowed_http_simulator_hosts:
        _require(isinstance(item, str), "allowed simulator host is invalid")
        host = item.strip().lower()
        _require(
            bool(_SIMULATOR_HOST.fullmatch(host)),
            "allowed HTTP host is not a pathfinder-sim-* host",
        )
        allowed.add(host)
    if parsed.scheme == "http":
        host = hostname.lower()
        loopback = host == "localhost"
        try:
            loopback = loopback or ipaddress.ip_address(host).is_loopback
        except ValueError:
            pass
        simulator = bool(_SIMULATOR_HOST.fullmatch(host)) and host in allowed
        _require(
            loopback or simulator,
            "plain HTTP is limited to loopback or an explicitly bound "
            "pathfinder-sim-* host",
        )
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


class OpenAICompatibleVisionDigestAdapter:
    """Deployment-only, proxy-free and redirect-free vision model binding."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model_id: str,
        allowed_http_simulator_hosts: Sequence[str] = (),
        timeout_seconds: float = 180.0,
        max_attempts: int = 3,
        opener: Any | None = None,
    ) -> None:
        self._base_url = _normalise_deployment_base(
            base_url,
            allowed_http_simulator_hosts,
        )
        _require(
            isinstance(api_key, str) and bool(api_key.strip()),
            "vision service API key is required",
        )
        self._api_key = api_key.strip()
        self._model_id = _model_id(model_id)
        self._timeout_seconds = _number(
            timeout_seconds,
            "vision service timeout",
            positive=True,
        )
        self._max_attempts = _integer(
            max_attempts,
            "vision service max_attempts",
            minimum=1,
            maximum=10,
        )
        self._opener = opener or build_opener(
            ProxyHandler({}),
            _NoRedirectHandler(),
        )

    def _request_body(
        self,
        *,
        object_id: str,
        frames: Sequence[SampledImage],
        duration_seconds: float,
        seed: int,
    ) -> bytes:
        object_id = _identifier(object_id, "object_id")
        duration = _number(
            duration_seconds,
            "source duration",
            positive=True,
        )
        _integer(seed, "seed")
        frame_rows = _frame_bindings(
            frames,
            duration_seconds=duration,
            jpeg_max_dimension=MAX_IMAGE_DIMENSION,
        )
        content: list[dict[str, Any]] = [{
            "type": "text",
            "text": (
                DIGEST_VISION_PROMPT
                + f"\n\nObject ID: {object_id}\n"
                + f"Source duration: {duration:.6f} seconds"
            ),
        }]
        for image, metadata in zip(frames, frame_rows, strict=True):
            content.append({
                "type": "text",
                "text": (
                    f"frame_index={metadata['frame_index']}; "
                    "timestamp_seconds="
                    f"{metadata['timestamp_seconds']:.6f}"
                ),
            })
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": "data:image/jpeg;base64,"
                    + base64.b64encode(image.jpeg_bytes).decode("ascii"),
                    "detail": "low",
                },
            })
        payload = {
            "model": self._model_id,
            "messages": [{"role": "user", "content": content}],
            "temperature": 0,
            "seed": seed,
        }
        body = json.dumps(
            payload,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        _require(
            len(body) <= MAX_REQUEST_BYTES,
            "vision service request exceeds its byte limit",
        )
        return body

    def generate_digest(
        self,
        *,
        object_id: str,
        frames: Sequence[SampledImage],
        duration_seconds: float,
        expected_model_id: str,
        seed: int,
    ) -> VisionDigestResult:
        expected_model = _model_id(expected_model_id)
        _require(
            expected_model == self._model_id,
            "deployment model differs from the frozen model ID",
        )
        body = self._request_body(
            object_id=object_id,
            frames=frames,
            duration_seconds=duration_seconds,
            seed=seed,
        )
        request = Request(
            self._base_url + "/chat/completions",
            data=body,
            method="POST",
            headers={
                "Authorization": "Bearer " + self._api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        last_error: BaseException | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                with self._opener.open(
                    request,
                    timeout=self._timeout_seconds,
                ) as response:
                    status = getattr(response, "status", None)
                    if status is None and hasattr(response, "getcode"):
                        status = response.getcode()
                    _require(status == 200, "vision service returned non-200")
                    raw = response.read(MAX_RESPONSE_BYTES + 1)
                _require(
                    len(raw) <= MAX_RESPONSE_BYTES,
                    "vision service response exceeds its byte limit",
                )
                envelope = _mapping(
                    _decode_json(raw, "vision service response"),
                    "vision service response",
                )
                _require(
                    envelope.get("model") == self._model_id,
                    "vision service response model ID changed",
                )
                choices = envelope.get("choices")
                _require(
                    isinstance(choices, list) and len(choices) == 1,
                    "vision service response choices are invalid",
                )
                choice = _mapping(choices[0], "vision service choice")
                message = _mapping(
                    choice.get("message"),
                    "vision service message",
                )
                content = message.get("content")
                _require(
                    isinstance(content, str)
                    and len(content.encode("utf-8")) <= MAX_RESPONSE_BYTES,
                    "vision service message content is invalid",
                )
                digest = _validate_digest_payload(
                    _decode_json(
                        content.encode("utf-8"),
                        "vision digest content",
                    ),
                    duration_seconds=duration_seconds,
                )
                return VisionDigestResult(
                    model_id=self._model_id,
                    digest=digest,
                    response_sha256=_sha256_bytes(raw),
                    protocol_attempts=attempt,
                    llm_called=True,
                    adapter_id=OPENAI_VISION_DIGEST_ADAPTER_ID,
                )
            except HTTPError as exc:
                last_error = exc
                if exc.code not in {408, 409, 425, 429, 500, 502, 503, 504}:
                    break
            except (URLError, TimeoutError, OSError) as exc:
                last_error = exc
            if attempt == self._max_attempts:
                break
        _require(last_error is not None, "vision service failed without detail")
        detail = (
            f"HTTP {last_error.code}"
            if isinstance(last_error, HTTPError)
            else type(last_error).__name__
        )
        raise N5DigestMaterializationError(
            f"vision service request failed after bounded attempts: {detail}"
        ) from last_error


__all__ = [
    "CHECKSUMS_NAME",
    "DIGEST_NAME",
    "DIGEST_VISION_PROMPT",
    "DigestFrameBinding",
    "EVIDENCE_NAME",
    "MEDIA_TYPE",
    "N5_DIGEST_EVIDENCE_SCHEMA_VERSION",
    "N5_DIGEST_PLAN_SCHEMA_VERSION",
    "N5DigestMaterializationError",
    "OPENAI_VISION_DIGEST_ADAPTER_ID",
    "OpenAICompatibleVisionDigestAdapter",
    "PLAN_NAME",
    "REPRESENTATION_ID",
    "SOURCE_REPRESENTATION_ID",
    "VISION_DIGEST_ADAPTER_PROTOCOL",
    "VisionDigestAdapter",
    "VisionDigestResult",
    "freeze_n5_multimodal_digest_plan",
    "materialize_n5_multimodal_digest",
    "verify_n5_multimodal_digest_materialization",
    "verify_n5_multimodal_digest_plan",
]
