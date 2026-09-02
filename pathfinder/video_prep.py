from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Sequence, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


PREP_SCHEMA_VERSION = "pathfinder.video-representation-prep/v1alpha1"
FRAME_SCHEMA_VERSION = "pathfinder.sampled-frame-observations/v1alpha1"
FORMAL_VIDEO_IDS = (
    "2834146886",
    "2976913210",
    "3441428429",
    "4130504920",
    "4260763967",
    "4882821564",
    "8547321641",
    "9088819598",
)
DEFAULT_FRAME_COUNT = 16
DEFAULT_JPEG_MAX_DIMENSION = 768
DEFAULT_PROTOCOL_MAX_ATTEMPTS = 3
INTERRUPTION_SCHEMA_VERSION = (
    "pathfinder.interrupted-representation-prep/v0.1"
)
RECOVERY_SCHEMA_VERSION = (
    "pathfinder.video-representation-recovery/v1alpha1"
)
INTERRUPTION_MANIFEST_NAME = "INTERRUPTION.json"
INTERRUPTION_CHECKSUMS_NAME = "INTERRUPTED_SHA256SUMS"

_PROTOCOL_REPAIR_PROMPT = """Your previous response did not satisfy the required
JSON schema. Return the complete response again as exactly one valid JSON
object. Preserve the requested facts, entry count, ordering, and field names.
Do not use Markdown fences and do not add explanatory text."""

ValidatedJson = TypeVar("ValidatedJson")

FRAME_PROMPT = """You are preparing a question-independent video data representation.
You receive chronologically ordered frames sampled from one video. Describe only
what is visibly supported by each frame. Do not guess the evaluation question,
the intended answer, hidden events, motivations, or events outside the frame.

Return one JSON object with exactly this shape:
{
  "frames": [
    {
      "frame_index": 0,
      "description": "concise factual visual description",
      "visible_text": null
    }
  ]
}

Return exactly one entry for every supplied frame_index, in ascending order.
visible_text must be a string only when text is legible; otherwise use null.
Return JSON only, without Markdown fences."""

DIGEST_PROMPT = """Build a question-independent temporal digest from the supplied
time-indexed frame observations. Use only facts in the observations. Do not
infer the evaluation question or intended answer, and do not add motivations
or events that are not supported.

Return one JSON object with exactly this shape:
{
  "events": [
    {
      "start_seconds": 0.0,
      "end_seconds": null,
      "description": "factual event description"
    }
  ],
  "summary": "concise factual summary"
}

Events must be chronological. Return JSON only, without Markdown fences."""


class VideoPreparationError(RuntimeError):
    """Raised when a representation cannot be prepared safely."""


class PreparationProtocolError(VideoPreparationError):
    """Raised when the inference gateway returns an invalid response."""


@dataclass(frozen=True)
class SampledImage:
    frame_index: int
    timestamp_seconds: float
    width: int
    height: int
    jpeg_bytes: bytes


@dataclass(frozen=True)
class AuditedRecoveryCheckpoint:
    checkpoint_name: str
    interruption_manifest_sha256: str
    checksum_manifest_sha256: str
    file_hashes: Mapping[str, str]
    entries: Mapping[str, Mapping[str, Any]]


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")


def _decode_json_layers(raw: bytes | str) -> Any:
    value: Any = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    for _ in range(3):
        if not isinstance(value, str):
            return value
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _extract_json_document(text: str, name: str) -> Mapping[str, Any]:
    candidate = text.strip()
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            candidate = "\n".join(lines[1:-1]).strip()
            if candidate.casefold().startswith("json\n"):
                candidate = candidate[5:].strip()
    try:
        value = json.loads(candidate)
    except json.JSONDecodeError as exc:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start < 0 or end <= start:
            raise PreparationProtocolError(
                f"{name} response is not a JSON object"
            ) from exc
        try:
            value = json.loads(candidate[start : end + 1])
        except json.JSONDecodeError as nested_exc:
            raise PreparationProtocolError(
                f"{name} response contains invalid JSON"
            ) from nested_exc
    if not isinstance(value, Mapping):
        raise PreparationProtocolError(f"{name} response must be an object")
    return value


def _extract_message_text(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        raise PreparationProtocolError("chat response must be an object")
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise PreparationProtocolError("chat response has no choices")
    choice = choices[0]
    if not isinstance(choice, Mapping):
        raise PreparationProtocolError("chat response choice is invalid")
    message = choice.get("message")
    if not isinstance(message, Mapping):
        raise PreparationProtocolError("chat response message is invalid")
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, Mapping) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        combined = "\n".join(parts).strip()
        if combined:
            return combined
    raise PreparationProtocolError("chat response content is empty")


def _normalize_base_url(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise VideoPreparationError("LLM base URL is required")
    parsed = urlsplit(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise VideoPreparationError("LLM base URL must be HTTP(S)")
    if parsed.username is not None or parsed.password is not None:
        raise VideoPreparationError("LLM base URL cannot contain credentials")
    if parsed.query or parsed.fragment:
        raise VideoPreparationError(
            "LLM base URL cannot contain a query or fragment"
        )
    if parsed.scheme == "http" and parsed.hostname not in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        raise VideoPreparationError(
            "non-local LLM base URL must use HTTPS"
        )
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


class OpenAICompatibleVisionClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 180.0,
        max_attempts: int = 3,
    ) -> None:
        self.base_url = _normalize_base_url(base_url)
        if not isinstance(api_key, str) or not api_key.strip():
            raise VideoPreparationError("LLM API key is required")
        if not isinstance(model, str) or not model.strip():
            raise VideoPreparationError("LLM model is required")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise VideoPreparationError("timeout must be positive and finite")
        if max_attempts <= 0:
            raise VideoPreparationError("max_attempts must be positive")
        self._api_key = api_key.strip()
        self.model = model.strip()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts

    def chat(self, *, messages: list[dict[str, Any]], seed: int) -> str:
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0,
            "seed": seed,
        }
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = Request(
            self.base_url + "/chat/completions",
            data=body,
            method="POST",
            headers={
                "Authorization": "Bearer " + self._api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        last_error: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    decoded = _decode_json_layers(response.read())
                return _extract_message_text(decoded)
            except HTTPError as exc:
                last_error = exc
                if exc.code not in {408, 409, 425, 429, 500, 502, 503, 504}:
                    break
            except (URLError, TimeoutError) as exc:
                last_error = exc
            if attempt < self.max_attempts:
                time.sleep(float(2 ** (attempt - 1)))
        assert last_error is not None
        if isinstance(last_error, HTTPError):
            detail = f"HTTP {last_error.code}"
        else:
            detail = type(last_error).__name__
        raise VideoPreparationError(
            "LLM request failed after "
            f"{self.max_attempts} attempt(s): {detail}"
        ) from last_error


def _validated_json_chat(
    *,
    client: OpenAICompatibleVisionClient,
    messages: list[dict[str, Any]],
    seed: int,
    response_name: str,
    validator: Callable[[Mapping[str, Any]], ValidatedJson],
    maximum_attempts: int = DEFAULT_PROTOCOL_MAX_ATTEMPTS,
) -> tuple[ValidatedJson, int]:
    """Run a bounded JSON exchange without changing the initial request.

    Transport retries remain the client's responsibility. Later attempts here
    occur only after a successful response violates the requested JSON
    protocol. The invalid response is returned to the model for repair but is
    neither logged nor persisted by this module.
    """

    if (
        isinstance(maximum_attempts, bool)
        or not isinstance(maximum_attempts, int)
        or maximum_attempts <= 0
    ):
        raise VideoPreparationError(
            "protocol maximum attempts must be a positive integer"
        )

    original_messages = list(messages)
    current_messages = original_messages
    last_error: PreparationProtocolError | None = None
    for attempt in range(1, maximum_attempts + 1):
        response = client.chat(messages=current_messages, seed=seed)
        try:
            payload = _extract_json_document(response, response_name)
            return validator(payload), attempt
        except PreparationProtocolError as exc:
            last_error = exc
            if attempt < maximum_attempts:
                current_messages = [
                    *original_messages,
                    {"role": "assistant", "content": response},
                    {"role": "user", "content": _PROTOCOL_REPAIR_PROMPT},
                ]

    assert last_error is not None
    raise PreparationProtocolError(
        f"{response_name} response remained invalid after "
        f"{maximum_attempts} protocol attempt(s): {last_error}"
    ) from last_error


def _load_video_dependencies() -> Any:
    try:
        import av  # type: ignore[import-not-found]
        from PIL import Image  # noqa: F401
    except ImportError as exc:
        raise VideoPreparationError(
            "video preparation dependencies are missing; install with "
            "`python -m pip install -e .[data-prep]`"
        ) from exc
    return av


def sample_video(
    path: Path,
    *,
    frame_count: int,
    jpeg_max_dimension: int,
) -> tuple[list[SampledImage], float]:
    if frame_count <= 0:
        raise VideoPreparationError("frame_count must be positive")
    if jpeg_max_dimension <= 0:
        raise VideoPreparationError("jpeg_max_dimension must be positive")
    av = _load_video_dependencies()
    with av.open(str(path)) as container:
        streams = list(container.streams.video)
        if len(streams) != 1:
            raise VideoPreparationError(
                f"{path.name} must contain exactly one video stream"
            )
        stream = streams[0]
        if stream.duration is not None and stream.time_base is not None:
            duration = float(stream.duration * stream.time_base)
        elif container.duration is not None:
            duration = float(container.duration / av.time_base)
        else:
            raise VideoPreparationError(
                f"{path.name} has no usable duration metadata"
            )
        if not math.isfinite(duration) or duration <= 0:
            raise VideoPreparationError(f"{path.name} has invalid duration")

        targets = [
            duration * (index + 0.5) / frame_count
            for index in range(frame_count)
        ]
        sampled: list[SampledImage] = []
        target_index = 0
        for frame in container.decode(video=stream.index):
            if target_index >= frame_count:
                break
            if frame.time is None or float(frame.time) < targets[target_index]:
                continue
            image = frame.to_image().convert("RGB")
            image.thumbnail((jpeg_max_dimension, jpeg_max_dimension))
            buffer = BytesIO()
            image.save(buffer, format="JPEG", quality=82, optimize=True)
            sampled.append(
                SampledImage(
                    frame_index=target_index,
                    timestamp_seconds=round(float(frame.time), 6),
                    width=image.width,
                    height=image.height,
                    jpeg_bytes=buffer.getvalue(),
                )
            )
            target_index += 1
        if len(sampled) != frame_count:
            raise VideoPreparationError(
                f"{path.name} produced {len(sampled)} of {frame_count} frames"
            )
        return sampled, duration


def _frame_messages(frames: Sequence[SampledImage]) -> list[dict[str, Any]]:
    content: list[dict[str, Any]] = [{"type": "text", "text": FRAME_PROMPT}]
    for frame in frames:
        content.append(
            {
                "type": "text",
                "text": (
                    f"frame_index={frame.frame_index}; "
                    f"timestamp_seconds={frame.timestamp_seconds:.6f}"
                ),
            }
        )
        content.append(
            {
                "type": "image_url",
                "image_url": {
                    "url": (
                        "data:image/jpeg;base64,"
                        + base64.b64encode(frame.jpeg_bytes).decode("ascii")
                    ),
                    "detail": "low",
                },
            }
        )
    return [{"role": "user", "content": content}]


def _validate_frame_descriptions(
    payload: Mapping[str, Any],
    frames: Sequence[SampledImage],
) -> list[dict[str, Any]]:
    raw_frames = payload.get("frames")
    if not isinstance(raw_frames, list) or len(raw_frames) != len(frames):
        raise PreparationProtocolError(
            "frame-description response has the wrong number of frames"
        )
    result: list[dict[str, Any]] = []
    for expected, raw in zip(frames, raw_frames, strict=True):
        if not isinstance(raw, Mapping):
            raise PreparationProtocolError("frame entry must be an object")
        if raw.get("frame_index") != expected.frame_index:
            raise PreparationProtocolError("frame indexes are not aligned")
        description = raw.get("description")
        if not isinstance(description, str) or not description.strip():
            raise PreparationProtocolError("frame description is empty")
        visible_text = raw.get("visible_text")
        if visible_text is not None and not isinstance(visible_text, str):
            raise PreparationProtocolError(
                "frame visible_text must be a string or null"
            )
        result.append(
            {
                "frame_index": expected.frame_index,
                "timestamp_seconds": expected.timestamp_seconds,
                "width": expected.width,
                "height": expected.height,
                "description": description.strip(),
                "visible_text": (
                    visible_text.strip()
                    if isinstance(visible_text, str) and visible_text.strip()
                    else None
                ),
            }
        )
    return result


def _validate_digest(
    payload: Mapping[str, Any],
    *,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    raw_events = payload.get("events")
    summary = payload.get("summary")
    if not isinstance(raw_events, list) or not raw_events:
        raise PreparationProtocolError("digest events must be a non-empty list")
    if not isinstance(summary, str) or not summary.strip():
        raise PreparationProtocolError("digest summary is empty")
    events = []
    previous_start = -math.inf
    for raw in raw_events:
        if not isinstance(raw, Mapping):
            raise PreparationProtocolError("digest event must be an object")
        start = raw.get("start_seconds")
        end = raw.get("end_seconds")
        description = raw.get("description")
        if (
            isinstance(start, bool)
            or not isinstance(start, (int, float))
            or not math.isfinite(float(start))
            or float(start) < 0
        ):
            raise PreparationProtocolError("digest event start is invalid")
        start_value = float(start)
        if start_value < previous_start:
            raise PreparationProtocolError("digest events are not chronological")
        if (
            duration_seconds is not None
            and start_value > duration_seconds
        ):
            raise PreparationProtocolError(
                "digest event starts after the source video ends"
            )
        previous_start = start_value
        if end is not None and (
            isinstance(end, bool)
            or not isinstance(end, (int, float))
            or not math.isfinite(float(end))
            or float(end) < start_value
        ):
            raise PreparationProtocolError("digest event end is invalid")
        if (
            duration_seconds is not None
            and end is not None
            and float(end) > duration_seconds
        ):
            raise PreparationProtocolError(
                "digest event ends after the source video ends"
            )
        if not isinstance(description, str) or not description.strip():
            raise PreparationProtocolError("digest event description is empty")
        events.append(
            {
                "start_seconds": round(start_value, 6),
                "end_seconds": (
                    round(float(end), 6) if end is not None else None
                ),
                "description": description.strip(),
            }
        )
    return {"events": events, "summary": summary.strip()}


def _digest_text(object_id: str, digest: Mapping[str, Any]) -> str:
    lines = [
        "PATHFINDER QUESTION-INDEPENDENT MULTIMODAL DIGEST",
        f"Object: {object_id}",
        "Timeline:",
    ]
    for event in digest["events"]:
        end = event["end_seconds"]
        interval = f"{event['start_seconds']:.3f}s"
        if end is not None:
            interval += f"-{end:.3f}s"
        lines.append(f"- [{interval}] {event['description']}")
    lines.extend(("Summary:", str(digest["summary"])))
    return "\n".join(lines).strip() + "\n"


def _package_version(name: str) -> str | None:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _write_atomic(path: Path, payload: bytes) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _normalize_video_ids(values: Sequence[str]) -> tuple[str, ...]:
    video_ids: list[str] = []
    seen: set[str] = set()
    for raw in values:
        if not isinstance(raw, str) or not raw.strip():
            raise VideoPreparationError("video IDs must be non-empty strings")
        video_id = raw.strip()
        if not video_id.isdecimal():
            raise VideoPreparationError(
                f"video ID must contain decimal digits only: {video_id!r}"
            )
        if video_id in seen:
            raise VideoPreparationError(f"duplicate video ID: {video_id}")
        seen.add(video_id)
        video_ids.append(video_id)
    if not video_ids:
        raise VideoPreparationError("at least one video ID is required")
    return tuple(video_ids)


def load_selection_video_ids(path: Path) -> tuple[str, ...]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VideoPreparationError(
            f"cannot read workload selection config: {path}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise VideoPreparationError("workload selection must be an object")
    if payload.get("schema_version") != (
        "pathfinder.workload-expansion-selection/v1alpha1"
    ):
        raise VideoPreparationError(
            "unsupported workload selection schema_version"
        )
    rows = payload.get("added_workloads")
    if not isinstance(rows, list) or not rows:
        raise VideoPreparationError(
            "workload selection has no added_workloads"
        )
    video_ids: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise VideoPreparationError(
                "workload selection entries must be objects"
            )
        video_id = row.get("video_id")
        object_id = row.get("object_id")
        if not isinstance(video_id, str):
            raise VideoPreparationError(
                "workload selection entry has no video_id"
            )
        if object_id != f"nextqa-val-{video_id}":
            raise VideoPreparationError(
                "workload selection object_id does not match video_id"
            )
        video_ids.append(video_id)
    return _normalize_video_ids(video_ids)


def _load_json_mapping(path: Path, name: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VideoPreparationError(f"cannot read {name}: {path}") from exc
    if not isinstance(payload, Mapping):
        raise VideoPreparationError(f"{name} must be a JSON object")
    return payload


def _verify_interrupted_checksums(root: Path) -> dict[str, str]:
    checksum_path = root / INTERRUPTION_CHECKSUMS_NAME
    try:
        lines = checksum_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise VideoPreparationError(
            f"cannot read interrupted checksum manifest: {checksum_path}"
        ) from exc
    if not lines:
        raise VideoPreparationError("interrupted checksum manifest is empty")

    recorded: dict[str, str] = {}
    for line in lines:
        if "  " not in line:
            raise VideoPreparationError(
                "interrupted checksum entry is malformed"
            )
        expected_hash, relative_text = line.split("  ", 1)
        if len(expected_hash) != 64 or any(
            character not in "0123456789abcdef"
            for character in expected_hash
        ):
            raise VideoPreparationError(
                "interrupted checksum entry has an invalid SHA-256"
            )
        relative = PurePosixPath(relative_text)
        if (
            not relative_text
            or "\\" in relative_text
            or relative.is_absolute()
            or relative.as_posix() != relative_text
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise VideoPreparationError(
                "interrupted checksum entry has an unsafe path"
            )
        if relative_text == INTERRUPTION_CHECKSUMS_NAME:
            raise VideoPreparationError(
                "interrupted checksum manifest must not hash itself"
            )
        if relative_text in recorded:
            raise VideoPreparationError(
                f"duplicate interrupted checksum path: {relative_text}"
            )
        recorded[relative_text] = expected_hash

    discovered: dict[str, Path] = {}
    for path in root.rglob("*"):
        if path.is_symlink():
            raise VideoPreparationError(
                f"interrupted checkpoint contains a symbolic link: {path.name}"
            )
        if not path.is_file() or path == checksum_path:
            continue
        relative_text = path.relative_to(root).as_posix()
        discovered[relative_text] = path

    if set(recorded) != set(discovered):
        missing = sorted(set(discovered) - set(recorded))
        unexpected = sorted(set(recorded) - set(discovered))
        raise VideoPreparationError(
            "interrupted checkpoint checksum coverage is incomplete; "
            f"unrecorded={missing}, missing_files={unexpected}"
        )

    for relative_text, expected_hash in recorded.items():
        if _sha256_file(discovered[relative_text]) != expected_hash:
            raise VideoPreparationError(
                "interrupted checkpoint checksum mismatch: "
                f"{relative_text}"
            )
    return recorded


def _validate_recovered_frame_payload(
    *,
    payload: Mapping[str, Any],
    object_id: str,
    video_id: str,
    source: Path,
    model: str,
    frame_count: int,
    jpeg_max_dimension: int,
) -> None:
    if payload.get("schema_version") != FRAME_SCHEMA_VERSION:
        raise VideoPreparationError(
            f"recovered frame schema is invalid: {object_id}"
        )
    if payload.get("object_id") != object_id:
        raise VideoPreparationError(
            f"recovered frame object ID is invalid: {object_id}"
        )
    if payload.get("source_video_id") != video_id:
        raise VideoPreparationError(
            f"recovered frame video ID is invalid: {object_id}"
        )
    if payload.get("source_video_sha256") != _sha256_file(source):
        raise VideoPreparationError(
            f"recovered frame source hash is invalid: {object_id}"
        )

    duration = payload.get("source_duration_seconds")
    if (
        isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(float(duration))
        or float(duration) <= 0
    ):
        raise VideoPreparationError(
            f"recovered frame duration is invalid: {object_id}"
        )

    sampling = payload.get("sampling")
    if (
        not isinstance(sampling, Mapping)
        or set(sampling) != {
            "method",
            "frame_count",
            "jpeg_max_dimension",
        }
        or sampling.get("method") != "uniform-midpoint"
        or type(sampling.get("frame_count")) is not int
        or sampling.get("frame_count") != frame_count
        or type(sampling.get("jpeg_max_dimension")) is not int
        or sampling.get("jpeg_max_dimension") != jpeg_max_dimension
    ):
        raise VideoPreparationError(
            f"recovered frame sampling contract is invalid: {object_id}"
        )

    generator = payload.get("generator")
    if (
        not isinstance(generator, Mapping)
        or set(generator) != {"model", "temperature", "prompt_sha256"}
        or generator.get("model") != model
        or type(generator.get("temperature")) is not int
        or generator.get("temperature") != 0
        or generator.get("prompt_sha256")
        != _sha256_bytes(FRAME_PROMPT.encode("utf-8"))
    ):
        raise VideoPreparationError(
            f"recovered frame generator contract is invalid: {object_id}"
        )

    frames = payload.get("frames")
    if not isinstance(frames, list) or len(frames) != frame_count:
        raise VideoPreparationError(
            f"recovered frame count is invalid: {object_id}"
        )
    for index, frame in enumerate(frames):
        if (
            not isinstance(frame, Mapping)
            or type(frame.get("frame_index")) is not int
            or frame.get("frame_index") != index
        ):
            raise VideoPreparationError(
                f"recovered frame ordering is invalid: {object_id}"
            )
        timestamp = frame.get("timestamp_seconds")
        width = frame.get("width")
        height = frame.get("height")
        description = frame.get("description")
        visible_text = frame.get("visible_text")
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or not math.isfinite(float(timestamp))
            or float(timestamp) < 0
            or float(timestamp) > float(duration) + 0.001
        ):
            raise VideoPreparationError(
                f"recovered frame timestamp is invalid: {object_id}"
            )
        if (
            isinstance(width, bool)
            or not isinstance(width, int)
            or isinstance(height, bool)
            or not isinstance(height, int)
            or width <= 0
            or height <= 0
            or max(width, height) > jpeg_max_dimension
        ):
            raise VideoPreparationError(
                f"recovered frame dimensions are invalid: {object_id}"
            )
        if not isinstance(description, str) or not description.strip():
            raise VideoPreparationError(
                f"recovered frame description is invalid: {object_id}"
            )
        if visible_text is not None and (
            not isinstance(visible_text, str) or not visible_text.strip()
        ):
            raise VideoPreparationError(
                f"recovered visible text is invalid: {object_id}"
            )


def _validate_recovered_digest(text: str, object_id: str) -> None:
    lines = text.splitlines()
    if (
        not text.endswith("\n")
        or len(lines) < 6
        or lines[0] != "PATHFINDER QUESTION-INDEPENDENT MULTIMODAL DIGEST"
        or lines[1] != f"Object: {object_id}"
        or lines[2] != "Timeline:"
        or "Summary:" not in lines[3:]
    ):
        raise VideoPreparationError(
            f"recovered digest structure is invalid: {object_id}"
        )
    summary_index = lines.index("Summary:", 3)
    if (
        summary_index == 3
        or any(not line.startswith("- [") for line in lines[3:summary_index])
        or not any(line.strip() for line in lines[summary_index + 1 :])
    ):
        raise VideoPreparationError(
            f"recovered digest content is invalid: {object_id}"
        )


def _load_audited_recovery_checkpoint(
    *,
    root: Path,
    video_directory: Path,
    ordered_video_ids: Sequence[str],
    model: str,
    frame_count: int,
    jpeg_max_dimension: int,
) -> AuditedRecoveryCheckpoint:
    if not root.is_dir() or root.is_symlink():
        raise VideoPreparationError(
            f"interrupted checkpoint is not a regular directory: {root}"
        )
    file_hashes = _verify_interrupted_checksums(root)
    interruption_path = root / INTERRUPTION_MANIFEST_NAME
    interruption = _load_json_mapping(
        interruption_path,
        "interruption manifest",
    )
    if interruption.get("schema_version") != INTERRUPTION_SCHEMA_VERSION:
        raise VideoPreparationError("unsupported interruption schema_version")
    if interruption.get("status") != "INTERRUPTED":
        raise VideoPreparationError("interruption status must be INTERRUPTED")
    if interruption.get("model") != model:
        raise VideoPreparationError(
            "interruption model does not match the requested model"
        )
    if (
        type(interruption.get("expected_object_count")) is not int
        or interruption.get("expected_object_count")
        != len(ordered_video_ids)
    ):
        raise VideoPreparationError(
            "interruption expected object count does not match the selection"
        )
    if interruption.get("final_output_created") is not False:
        raise VideoPreparationError(
            "interruption manifest must explicitly record no final output"
        )
    if interruption.get("incomplete_object_directories") != []:
        raise VideoPreparationError(
            "interruption checkpoint contains incomplete object directories"
        )
    if interruption.get("protocol_attempts_for_recovered_objects") != (
        "not persisted before interruption"
    ):
        raise VideoPreparationError(
            "interruption manifest must disclose missing protocol attempts"
        )

    expected_objects = {
        f"nextqa-val-{video_id}": video_id
        for video_id in ordered_video_ids
    }
    object_directories = {
        path.name: path
        for path in root.iterdir()
        if path.is_dir()
    }
    unexpected_objects = sorted(set(object_directories) - set(expected_objects))
    if unexpected_objects:
        raise VideoPreparationError(
            "interruption checkpoint has unexpected objects: "
            f"{unexpected_objects}"
        )
    if not object_directories:
        raise VideoPreparationError("interruption checkpoint has no objects")
    if (
        type(interruption.get("complete_object_count")) is not int
        or interruption.get("complete_object_count")
        != len(object_directories)
    ):
        raise VideoPreparationError(
            "interruption complete object count does not match the checkpoint"
        )

    allowed_top_level_files = {
        INTERRUPTION_MANIFEST_NAME,
        INTERRUPTION_CHECKSUMS_NAME,
    }
    unexpected_top_level = sorted(
        path.name
        for path in root.iterdir()
        if path.is_file() and path.name not in allowed_top_level_files
    )
    if unexpected_top_level:
        raise VideoPreparationError(
            "interruption checkpoint has unexpected top-level files: "
            f"{unexpected_top_level}"
        )

    entries: dict[str, Mapping[str, Any]] = {}
    for object_id, object_directory in object_directories.items():
        video_id = expected_objects[object_id]
        source = video_directory / f"{video_id}.mp4"
        expected_files = {"sampled_frames.json", "multimodal_digest.txt"}
        actual_files = {
            path.name
            for path in object_directory.iterdir()
            if path.is_file()
        }
        if actual_files != expected_files or any(
            path.is_dir() for path in object_directory.iterdir()
        ):
            raise VideoPreparationError(
                f"recovered object files are invalid: {object_id}"
            )

        frames_path = object_directory / "sampled_frames.json"
        digest_path = object_directory / "multimodal_digest.txt"
        frame_payload = _load_json_mapping(
            frames_path,
            f"recovered frames for {object_id}",
        )
        _validate_recovered_frame_payload(
            payload=frame_payload,
            object_id=object_id,
            video_id=video_id,
            source=source,
            model=model,
            frame_count=frame_count,
            jpeg_max_dimension=jpeg_max_dimension,
        )
        try:
            digest_text = digest_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise VideoPreparationError(
                f"cannot read recovered digest: {object_id}"
            ) from exc
        _validate_recovered_digest(digest_text, object_id)

        frame_bytes = frames_path.read_bytes()
        digest_bytes = digest_path.read_bytes()
        entries[object_id] = {
            "object_id": object_id,
            "source_video": {
                "filename": source.name,
                "size_bytes": source.stat().st_size,
                "sha256": _sha256_file(source),
            },
            "protocol_attempts": {
                "frame_descriptions": None,
                "multimodal_digest": None,
            },
            "recovery": {
                "kind": "audited-interrupted-checkpoint",
                "historical_protocol_attempts_recorded": False,
            },
            "representations": {
                "sampled_frames": {
                    "path": f"{object_id}/sampled_frames.json",
                    "size_bytes": len(frame_bytes),
                    "sha256": _sha256_bytes(frame_bytes),
                },
                "multimodal_digest": {
                    "path": f"{object_id}/multimodal_digest.txt",
                    "size_bytes": len(digest_bytes),
                    "sha256": _sha256_bytes(digest_bytes),
                },
            },
        }

    return AuditedRecoveryCheckpoint(
        checkpoint_name=root.name,
        interruption_manifest_sha256=_sha256_file(interruption_path),
        checksum_manifest_sha256=_sha256_file(
            root / INTERRUPTION_CHECKSUMS_NAME
        ),
        file_hashes=file_hashes,
        entries=entries,
    )


def audit_recovery_checkpoint(
    *,
    root: Path,
    video_directory: Path,
    model: str,
    frame_count: int,
    jpeg_max_dimension: int,
    video_ids: Sequence[str],
) -> dict[str, Any]:
    ordered_video_ids = _normalize_video_ids(video_ids)
    paths = {path.stem: path for path in video_directory.glob("*.mp4")}
    expected = set(ordered_video_ids)
    if set(paths) != expected:
        missing = sorted(expected - set(paths))
        unexpected = sorted(set(paths) - expected)
        raise VideoPreparationError(
            "video directory does not match the frozen video IDs; "
            f"missing={missing}, unexpected={unexpected}"
        )
    if not isinstance(model, str) or not model.strip():
        raise VideoPreparationError("LLM model is required")
    checkpoint = _load_audited_recovery_checkpoint(
        root=root,
        video_directory=video_directory,
        ordered_video_ids=ordered_video_ids,
        model=model.strip(),
        frame_count=frame_count,
        jpeg_max_dimension=jpeg_max_dimension,
    )
    recovered = set(checkpoint.entries)
    ordered_objects = [
        f"nextqa-val-{video_id}" for video_id in ordered_video_ids
    ]
    return {
        "status": "recovery_audit_ok",
        "checkpoint_name": checkpoint.checkpoint_name,
        "model": model.strip(),
        "expected_object_count": len(ordered_objects),
        "recovered_object_count": len(recovered),
        "missing_object_count": len(ordered_objects) - len(recovered),
        "recovered_object_ids": [
            object_id for object_id in ordered_objects if object_id in recovered
        ],
        "missing_object_ids": [
            object_id
            for object_id in ordered_objects
            if object_id not in recovered
        ],
        "interruption_manifest_sha256": (
            checkpoint.interruption_manifest_sha256
        ),
        "checksum_manifest_sha256": checkpoint.checksum_manifest_sha256,
        "historical_protocol_attempts_recorded": False,
        "inference_requests_made": False,
        "credentials_recorded": False,
    }


def prepare_representations(
    *,
    video_directory: Path,
    output_directory: Path,
    client: OpenAICompatibleVisionClient,
    frame_count: int = DEFAULT_FRAME_COUNT,
    jpeg_max_dimension: int = DEFAULT_JPEG_MAX_DIMENSION,
    base_seed: int = 7301,
    video_ids: Sequence[str] = FORMAL_VIDEO_IDS,
    protocol_max_attempts: int = DEFAULT_PROTOCOL_MAX_ATTEMPTS,
    resume_from: Path | None = None,
) -> dict[str, Any]:
    ordered_video_ids = _normalize_video_ids(video_ids)
    paths = {path.stem: path for path in video_directory.glob("*.mp4")}
    expected = set(ordered_video_ids)
    if set(paths) != expected:
        missing = sorted(expected - set(paths))
        unexpected = sorted(set(paths) - expected)
        raise VideoPreparationError(
            "video directory does not match the frozen video IDs; "
            f"missing={missing}, unexpected={unexpected}"
        )
    if output_directory.exists():
        raise VideoPreparationError(
            f"output directory already exists: {output_directory}"
        )
    recovery = (
        _load_audited_recovery_checkpoint(
            root=resume_from,
            video_directory=video_directory,
            ordered_video_ids=ordered_video_ids,
            model=client.model,
            frame_count=frame_count,
            jpeg_max_dimension=jpeg_max_dimension,
        )
        if resume_from is not None
        else None
    )
    recovered_entries = recovery.entries if recovery is not None else {}
    output_directory.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=".pathfinder-video-prep-",
            dir=output_directory.parent,
        )
    )
    generated: list[dict[str, Any]] = []
    try:
        for position, video_id in enumerate(ordered_video_ids):
            source = paths[video_id]
            object_id = f"nextqa-val-{video_id}"
            if object_id in recovered_entries:
                assert resume_from is not None
                shutil.copytree(
                    resume_from / object_id,
                    staging / object_id,
                )
                generated.append(dict(recovered_entries[object_id]))
                print(f"recovered {object_id}", flush=True)
                continue
            print(f"sampling {object_id}", flush=True)
            images, duration = sample_video(
                source,
                frame_count=frame_count,
                jpeg_max_dimension=jpeg_max_dimension,
            )
            descriptions, frame_protocol_attempts = _validated_json_chat(
                client=client,
                messages=_frame_messages(images),
                seed=base_seed + position * 2,
                response_name="frame-description",
                validator=lambda payload: _validate_frame_descriptions(
                    payload,
                    images,
                ),
                maximum_attempts=protocol_max_attempts,
            )
            frame_payload = {
                "schema_version": FRAME_SCHEMA_VERSION,
                "object_id": object_id,
                "source_video_id": video_id,
                "source_video_sha256": _sha256_file(source),
                "source_duration_seconds": round(duration, 6),
                "sampling": {
                    "method": "uniform-midpoint",
                    "frame_count": frame_count,
                    "jpeg_max_dimension": jpeg_max_dimension,
                },
                "generator": {
                    "model": client.model,
                    "temperature": 0,
                    "prompt_sha256": _sha256_bytes(
                        FRAME_PROMPT.encode("utf-8")
                    ),
                },
                "frames": descriptions,
            }
            digest_input = json.dumps(
                {
                    "object_id": object_id,
                    "duration_seconds": round(duration, 6),
                    "frames": descriptions,
                },
                separators=(",", ":"),
                ensure_ascii=False,
            )
            digest, digest_protocol_attempts = _validated_json_chat(
                client=client,
                messages=[
                    {
                        "role": "user",
                        "content": DIGEST_PROMPT + "\n\nObservations:\n" + digest_input,
                    }
                ],
                seed=base_seed + position * 2 + 1,
                response_name="digest",
                validator=lambda payload: _validate_digest(
                    payload,
                    duration_seconds=duration,
                ),
                maximum_attempts=protocol_max_attempts,
            )

            object_directory = staging / object_id
            object_directory.mkdir(parents=True)
            frames_path = object_directory / "sampled_frames.json"
            digest_path = object_directory / "multimodal_digest.txt"
            frame_bytes = _json_bytes(frame_payload)
            digest_bytes = _digest_text(object_id, digest).encode("utf-8")
            _write_atomic(frames_path, frame_bytes)
            _write_atomic(digest_path, digest_bytes)
            generated.append(
                {
                    "object_id": object_id,
                    "source_video": {
                        "filename": source.name,
                        "size_bytes": source.stat().st_size,
                        "sha256": _sha256_file(source),
                    },
                    "protocol_attempts": {
                        "frame_descriptions": frame_protocol_attempts,
                        "multimodal_digest": digest_protocol_attempts,
                    },
                    "representations": {
                        "sampled_frames": {
                            "path": f"{object_id}/sampled_frames.json",
                            "size_bytes": len(frame_bytes),
                            "sha256": _sha256_bytes(frame_bytes),
                        },
                        "multimodal_digest": {
                            "path": f"{object_id}/multimodal_digest.txt",
                            "size_bytes": len(digest_bytes),
                            "sha256": _sha256_bytes(digest_bytes),
                        },
                    },
                }
            )
            print(f"prepared {object_id}", flush=True)

        if recovery is not None:
            current_hashes = _verify_interrupted_checksums(resume_from)
            current_interruption_sha256 = _sha256_file(
                resume_from / INTERRUPTION_MANIFEST_NAME
            )
            current_checksum_sha256 = _sha256_file(
                resume_from / INTERRUPTION_CHECKSUMS_NAME
            )
            if (
                current_hashes != recovery.file_hashes
                or current_interruption_sha256
                != recovery.interruption_manifest_sha256
                or current_checksum_sha256 != recovery.checksum_manifest_sha256
            ):
                raise VideoPreparationError(
                    "interrupted checkpoint changed during recovery"
                )

        manifest = {
            "schema_version": PREP_SCHEMA_VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "question_blind": True,
            "input_contract": "video bytes and frozen video IDs only",
            "model": client.model,
            "llm_base_url": client.base_url,
            "temperature": 0,
            "base_seed": base_seed,
            "protocol_max_attempts": protocol_max_attempts,
            "video_ids": list(ordered_video_ids),
            "frame_count": frame_count,
            "jpeg_max_dimension": jpeg_max_dimension,
            "frame_prompt_sha256": _sha256_bytes(
                FRAME_PROMPT.encode("utf-8")
            ),
            "digest_prompt_sha256": _sha256_bytes(
                DIGEST_PROMPT.encode("utf-8")
            ),
            "python_packages": {
                "av": _package_version("av"),
                "Pillow": _package_version("Pillow"),
            },
            "objects": generated,
            "credentials_recorded": False,
        }
        if recovery is not None:
            manifest["recovery"] = {
                "schema_version": RECOVERY_SCHEMA_VERSION,
                "mode": "audited-interrupted-checkpoint",
                "checkpoint_name": recovery.checkpoint_name,
                "interruption_manifest_sha256": (
                    recovery.interruption_manifest_sha256
                ),
                "checksum_manifest_sha256": (
                    recovery.checksum_manifest_sha256
                ),
                "recovered_object_count": len(recovered_entries),
                "newly_generated_object_count": (
                    len(ordered_video_ids) - len(recovered_entries)
                ),
                "historical_protocol_attempts_recorded": False,
                "checkpoint_integrity_reverified_before_finalization": True,
                "source_checkpoint_mutated": False,
            }
        _write_atomic(
            staging / "generation-manifest.json",
            _json_bytes(manifest),
        )
        staging.replace(output_directory)
        return manifest
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def probe_vision_endpoint(
    *,
    video_directory: Path,
    client: OpenAICompatibleVisionClient,
    jpeg_max_dimension: int = DEFAULT_JPEG_MAX_DIMENSION,
    seed: int = 7301,
    video_ids: Sequence[str] = FORMAL_VIDEO_IDS,
    protocol_max_attempts: int = DEFAULT_PROTOCOL_MAX_ATTEMPTS,
) -> dict[str, Any]:
    video_id = _normalize_video_ids(video_ids)[0]
    path = video_directory / f"{video_id}.mp4"
    if not path.is_file():
        raise VideoPreparationError(f"probe video is missing: {path}")
    frames, _ = sample_video(
        path,
        frame_count=1,
        jpeg_max_dimension=jpeg_max_dimension,
    )
    descriptions, protocol_attempts = _validated_json_chat(
        client=client,
        messages=_frame_messages(frames),
        seed=seed,
        response_name="vision probe",
        validator=lambda payload: _validate_frame_descriptions(
            payload,
            frames,
        ),
        maximum_attempts=protocol_max_attempts,
    )
    return {
        "status": "vision_probe_ok",
        "model": client.model,
        "video_id": video_id,
        "described_frames": len(descriptions),
        "protocol_attempts": protocol_attempts,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate question-blind frame observations and temporal digests "
            "for a frozen video corpus."
        )
    )
    parser.add_argument("--video-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--selection-config",
        type=Path,
        help=(
            "load added video IDs in frozen order from a Pathfinder workload "
            "selection config"
        ),
    )
    scope.add_argument(
        "--video-id",
        action="append",
        dest="video_ids",
        help="explicit frozen video ID; repeat to provide more than one",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("PATHFINDER_PREP_LLM_BASE_URL"),
    )
    parser.add_argument(
        "--model",
        default=os.environ.get("PATHFINDER_PREP_LLM_MODEL"),
    )
    parser.add_argument("--frame-count", type=int, default=DEFAULT_FRAME_COUNT)
    parser.add_argument(
        "--jpeg-max-dimension",
        type=int,
        default=DEFAULT_JPEG_MAX_DIMENSION,
    )
    parser.add_argument("--base-seed", type=int, default=7301)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument(
        "--protocol-max-attempts",
        type=int,
        default=DEFAULT_PROTOCOL_MAX_ATTEMPTS,
        help=(
            "maximum JSON-protocol attempts per frame or digest response; "
            "transport retries are configured separately"
        ),
    )
    parser.add_argument(
        "--resume-from",
        type=Path,
        help=(
            "reuse strictly validated objects from an audited interrupted "
            "checkpoint and call the LLM only for missing objects"
        ),
    )
    parser.add_argument(
        "--audit-resume-only",
        action="store_true",
        help=(
            "validate the interrupted checkpoint and report reusable and "
            "missing objects without making an inference request"
        ),
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="validate one image request without writing representations",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    api_key = os.environ.get("PATHFINDER_PREP_LLM_API_KEY")
    try:
        if args.selection_config is not None:
            video_ids = load_selection_video_ids(
                args.selection_config.resolve()
            )
        elif args.video_ids is not None:
            video_ids = _normalize_video_ids(args.video_ids)
        else:
            video_ids = FORMAL_VIDEO_IDS
        if args.audit_resume_only:
            if args.resume_from is None:
                raise VideoPreparationError(
                    "--audit-resume-only requires --resume-from"
                )
            if args.probe_only:
                raise VideoPreparationError(
                    "--audit-resume-only cannot be combined with --probe-only"
                )
            print(
                json.dumps(
                    audit_recovery_checkpoint(
                        root=args.resume_from.resolve(),
                        video_directory=args.video_dir.resolve(),
                        model=args.model,
                        frame_count=args.frame_count,
                        jpeg_max_dimension=args.jpeg_max_dimension,
                        video_ids=video_ids,
                    ),
                    indent=2,
                )
            )
            return 0
        client = OpenAICompatibleVisionClient(
            base_url=args.base_url,
            api_key=api_key,
            model=args.model,
            timeout_seconds=args.timeout,
        )
        if args.probe_only:
            print(
                json.dumps(
                    probe_vision_endpoint(
                        video_directory=args.video_dir.resolve(),
                        client=client,
                        jpeg_max_dimension=args.jpeg_max_dimension,
                        seed=args.base_seed,
                        video_ids=video_ids,
                        protocol_max_attempts=args.protocol_max_attempts,
                    ),
                    indent=2,
                )
            )
            return 0
        manifest = prepare_representations(
            video_directory=args.video_dir.resolve(),
            output_directory=args.output_dir.resolve(),
            client=client,
            frame_count=args.frame_count,
            jpeg_max_dimension=args.jpeg_max_dimension,
            base_seed=args.base_seed,
            video_ids=video_ids,
            protocol_max_attempts=args.protocol_max_attempts,
            resume_from=(
                args.resume_from.resolve()
                if args.resume_from is not None
                else None
            ),
        )
    except VideoPreparationError as exc:
        print(json.dumps({"status": "error", "message": str(exc)}))
        return 1
    print(
        json.dumps(
            {
                "status": "complete",
                "objects": len(manifest["objects"]),
                "representations": len(manifest["objects"]) * 2,
                "output_dir": str(args.output_dir.resolve()),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
