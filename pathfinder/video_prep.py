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
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
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


def prepare_representations(
    *,
    video_directory: Path,
    output_directory: Path,
    client: OpenAICompatibleVisionClient,
    frame_count: int = DEFAULT_FRAME_COUNT,
    jpeg_max_dimension: int = DEFAULT_JPEG_MAX_DIMENSION,
    base_seed: int = 7301,
) -> dict[str, Any]:
    paths = {path.stem: path for path in video_directory.glob("*.mp4")}
    expected = set(FORMAL_VIDEO_IDS)
    if set(paths) != expected:
        raise VideoPreparationError(
            "video directory must contain exactly the frozen eight video IDs"
        )
    if output_directory.exists():
        raise VideoPreparationError(
            f"output directory already exists: {output_directory}"
        )
    output_directory.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=".phase-b-confirmatory-small-",
            dir=output_directory.parent,
        )
    )
    generated: list[dict[str, Any]] = []
    try:
        for position, video_id in enumerate(FORMAL_VIDEO_IDS):
            source = paths[video_id]
            object_id = f"nextqa-val-{video_id}"
            print(f"sampling {object_id}", flush=True)
            images, duration = sample_video(
                source,
                frame_count=frame_count,
                jpeg_max_dimension=jpeg_max_dimension,
            )
            frame_response = client.chat(
                messages=_frame_messages(images),
                seed=base_seed + position * 2,
            )
            descriptions = _validate_frame_descriptions(
                _extract_json_document(frame_response, "frame-description"),
                images,
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
            digest_response = client.chat(
                messages=[
                    {
                        "role": "user",
                        "content": DIGEST_PROMPT + "\n\nObservations:\n" + digest_input,
                    }
                ],
                seed=base_seed + position * 2 + 1,
            )
            digest = _validate_digest(
                _extract_json_document(digest_response, "digest"),
                duration_seconds=duration,
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

        manifest = {
            "schema_version": PREP_SCHEMA_VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "question_blind": True,
            "input_contract": "video bytes and frozen video IDs only",
            "model": client.model,
            "llm_base_url": client.base_url,
            "temperature": 0,
            "base_seed": base_seed,
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
) -> dict[str, Any]:
    video_id = FORMAL_VIDEO_IDS[0]
    path = video_directory / f"{video_id}.mp4"
    if not path.is_file():
        raise VideoPreparationError(f"probe video is missing: {path}")
    frames, _ = sample_video(
        path,
        frame_count=1,
        jpeg_max_dimension=jpeg_max_dimension,
    )
    response = client.chat(messages=_frame_messages(frames), seed=seed)
    descriptions = _validate_frame_descriptions(
        _extract_json_document(response, "vision probe"),
        frames,
    )
    return {
        "status": "vision_probe_ok",
        "model": client.model,
        "video_id": video_id,
        "described_frames": len(descriptions),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate question-blind frame observations and temporal digests "
            "for the frozen Phase B small confirmatory corpus."
        )
    )
    parser.add_argument("--video-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
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
        "--probe-only",
        action="store_true",
        help="validate one image request without writing representations",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    api_key = os.environ.get("PATHFINDER_PREP_LLM_API_KEY")
    try:
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
