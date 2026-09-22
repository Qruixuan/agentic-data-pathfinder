"""Freeze a multi-case, outcome-blind temporal index for RSI-Exam traces.

The pipeline is deliberately split into three fail-closed phases:

1. sample public source videos and freeze question-independent windows;
2. materialize one durable action caption per window; and
3. embed captions and public-question anchor clauses, then freeze one N3
   temporal selection policy per object.

Caption calls never receive questions, options, labels, predictions, or task
outcomes.  The finalizer reads public questions only after captions are frozen.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import tempfile
import tarfile
import time
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction
from pathlib import Path, PurePosixPath
from typing import Any

from ..simulator.full_flow_fine_temporal_windows import (
    CAPTION_PROMPT,
    CAPTION_PROMPT_SHA256,
    assert_caption_request_is_question_independent,
    build_raw_response_record,
    build_windows,
    cache_entry_bindings,
    caption_search_text,
    extract_single_json_object,
    validate_structured_caption,
)
from ..simulator.full_flow_temporal_embeddings import (
    VECTOR_POLICY_ID,
    normalize_and_quantize,
    rank_segments_semantic,
)
from ..simulator.full_flow_temporal_index_v2 import (
    TEMPORAL_INDEX_V2_ACTION_ID,
    contextualize_anchor_clause,
    select_v2,
)
from ..simulator.full_flow_runtime_frame_plan import (
    RUNTIME_REPRESENTATION_LABEL,
    build_runtime_frame_plan,
    verify_runtime_frames,
)
from ..simulator.n3_indexed_data_plane import (
    TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
    N3TemporalSelectionPolicy,
    build_n3_indexed_data_plane_package,
)
from ..simulator.raw_cold_data_plane import (
    CHECKSUMS_NAME,
    PACKAGE_MANIFEST_NAME as N3_MANIFEST_NAME,
    verify_raw_cold_data_plane_package,
)
from ..video_prep import SampledImage, sample_video
from .collection_plan import (
    CASES_NAME,
    verify_collection_plan,
)


PREPARATION_SCHEMA_VERSION = (
    "pathfinder.rsi-exam-temporal-index-preparation/v1alpha1"
)
CAPTION_PACKAGE_SCHEMA_VERSION = (
    "pathfinder.rsi-exam-temporal-caption-package/v1alpha1"
)
INDEX_PACKAGE_SCHEMA_VERSION = (
    "pathfinder.rsi-exam-temporal-index-package/v1alpha1"
)
POLICY_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.n3-temporal-selection-policy-manifest/v1alpha1"
)
PREPARATION_MANIFEST = "temporal-index-preparation.json"
FRAMES_NAME = "caption-frames.jsonl"
WINDOWS_NAME = "fine-windows.jsonl"
CAPTION_MANIFEST = "temporal-caption-package.json"
CAPTIONS_NAME = "fine-captions.jsonl"
BUILD_COST_NAME = "temporal-index-build-cost.json"
INDEX_MANIFEST = "temporal-index-package.json"
VECTORS_NAME = "temporal-index-vectors.jsonl"
POLICY_MANIFEST = "n3-temporal-selection-policies.json"
DEFAULT_CAPTION_FRAME_COUNT = 24
DEFAULT_RUNTIME_FRAME_COUNT = 10
DEFAULT_JPEG_MAX_DIMENSION = 768
DEFAULT_EMBEDDING_DIMENSION = 1024
FORMAL_CAPTION_CACHE_SCHEMA_VERSION = (
    "pathfinder.rsi-exam-formal-temporal-caption-cache/v1alpha2"
)
LEGACY_CAPTION_CACHE_SCHEMA_VERSION = (
    "pathfinder.full-flow-fine-caption-cache/v1alpha1"
)


class FormalTemporalIndexError(RuntimeError):
    """Raised when a formal temporal-index input or result is not bound."""


Transport = Callable[[urllib.request.Request, float], bytes]


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FormalTemporalIndexError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FormalTemporalIndexError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise FormalTemporalIndexError(f"cannot read {label}") from exc
    _require(b"\r" not in raw and raw.endswith(b"\n"),
             f"{label} is not canonical LF JSONL")
    rows = [json.loads(line) for line in raw.splitlines() if line]
    _require(all(isinstance(row, dict) for row in rows),
             f"{label} contains a non-object row")
    _require(raw == _jsonl_bytes(rows), f"{label} is not canonical")
    return rows


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def _checksums(root: Path) -> bytes:
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != CHECKSUMS_NAME:
            digest, _ = _hash_file(path)
            rows.append(f"{digest}  {path.relative_to(root).as_posix()}\n")
    return "".join(rows).encode("utf-8")


def _verify_checksums(root: Path) -> None:
    expected = (root / CHECKSUMS_NAME).read_bytes()
    _require(b"\r" not in expected, "checksum manifest contains CR bytes")
    _require(expected == _checksums(root), "checksum manifest differs")


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_bytes(value)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _default_transport(request: urllib.request.Request, timeout: float) -> bytes:
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def _selected_case_ids(plan_dir: Path) -> list[str]:
    rows = _read_jsonl(plan_dir / CASES_NAME, "selected cases")
    result = [str(row["object_id"]) for row in rows]
    _require(len(result) == len(set(result)) and bool(result),
             "selected cases are not distinct")
    return result


def prepare_formal_temporal_index(
    collection_plan_dir: str | Path,
    n3_raw_package_dir: str | Path,
    *,
    output_dir: str | Path,
    package_id: str,
    caption_frame_count: int = DEFAULT_CAPTION_FRAME_COUNT,
    jpeg_max_dimension: int = DEFAULT_JPEG_MAX_DIMENSION,
    sampler: Callable[..., tuple[Sequence[SampledImage], float]] = sample_video,
) -> dict[str, Any]:
    """Freeze caption frames and fine windows before any provider call."""

    plan_root = Path(collection_plan_dir).resolve()
    raw_root = Path(n3_raw_package_dir).resolve()
    plan = verify_collection_plan(plan_root)
    raw = verify_raw_cold_data_plane_package(raw_root)
    _require(type(caption_frame_count) is int and caption_frame_count >= 12,
             "caption_frame_count must be at least 12")
    _require(type(jpeg_max_dimension) is int and jpeg_max_dimension > 0,
             "jpeg_max_dimension must be positive")
    case_ids = _selected_case_ids(plan_root)
    raw_manifest = _read_json(raw_root / N3_MANIFEST_NAME, "N3 raw package")
    raw_rows = {
        str(row["object_id"]): row for row in raw_manifest["objects"]
    }
    _require(set(raw_rows) == set(case_ids),
             "N3 raw package does not exactly cover selected cases")

    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        frame_rows: list[dict[str, Any]] = []
        window_rows: list[dict[str, Any]] = []
        object_rows: list[dict[str, Any]] = []
        for object_id in sorted(case_ids):
            raw_row = raw_rows[object_id]
            relative = Path(*PurePosixPath(
                str(raw_row["artifact_package_path"])
            ).parts)
            source = raw_root / relative
            sampled, duration = sampler(
                source,
                frame_count=caption_frame_count,
                jpeg_max_dimension=jpeg_max_dimension,
                temporal_start_fraction=0.0,
                temporal_end_fraction=1.0,
            )
            frames = []
            for frame in sampled:
                frame_path = (
                    Path("frames") / object_id / f"{frame.frame_index:03d}.jpg"
                )
                (staging / frame_path).parent.mkdir(parents=True, exist_ok=True)
                (staging / frame_path).write_bytes(frame.jpeg_bytes)
                row = {
                    "frame_index": frame.frame_index,
                    "height": frame.height,
                    "jpeg_sha256": _sha256(frame.jpeg_bytes),
                    "jpeg_size_bytes": len(frame.jpeg_bytes),
                    "object_id": object_id,
                    "package_path": frame_path.as_posix(),
                    "timestamp_seconds": frame.timestamp_seconds,
                    "width": frame.width,
                }
                frames.append(row)
                frame_rows.append(row)
            windows = build_windows(
                object_id=object_id,
                duration_seconds=float(duration),
                frames=frames,
                source_video_sha256=str(raw_row["artifact_sha256"]),
                source_video_size_bytes=int(raw_row["artifact_size_bytes"]),
            )
            window_rows.extend(windows)
            object_rows.append({
                "duration_seconds": float(duration),
                "object_id": object_id,
                "source_video_sha256": raw_row["artifact_sha256"],
                "source_video_size_bytes": raw_row["artifact_size_bytes"],
                "window_count": len(windows),
            })

        frame_rows.sort(key=lambda row: (row["object_id"], row["frame_index"]))
        window_rows.sort(key=lambda row: (row["object_id"], row["ordinal"]))
        (staging / FRAMES_NAME).write_bytes(_jsonl_bytes(frame_rows))
        (staging / WINDOWS_NAME).write_bytes(_jsonl_bytes(window_rows))
        manifest = {
            "schema_version": PREPARATION_SCHEMA_VERSION,
            "status": "FROZEN_OUTCOME_BLIND_TEMPORAL_INDEX_PREPARATION",
            "package_id": package_id,
            "collection_plan_sha256": plan["plan_sha256"],
            "n3_raw_package_id": raw["package_id"],
            "n3_raw_manifest_sha256": _sha256(
                (raw_root / N3_MANIFEST_NAME).read_bytes()
            ),
            "n3_raw_checksums_sha256": _sha256(
                (raw_root / CHECKSUMS_NAME).read_bytes()
            ),
            "caption_prompt_sha256": CAPTION_PROMPT_SHA256,
            "caption_frame_count_per_object": caption_frame_count,
            "jpeg_max_dimension": jpeg_max_dimension,
            "object_count": len(object_rows),
            "window_count": len(window_rows),
            "frame_count": len(frame_rows),
            "objects": object_rows,
            "question_independent_preparation": True,
            "provider_requests_made": 0,
            "task_outcomes_read": False,
            "hidden_label_values_read": False,
            "credentials_recorded": False,
        }
        manifest["preparation_sha256"] = _sha256(_canonical(manifest))
        (staging / PREPARATION_MANIFEST).write_bytes(_json_bytes(manifest))
        (staging / CHECKSUMS_NAME).write_bytes(_checksums(staging))
        verify_formal_temporal_index_preparation(staging)
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {
        "status": "FROZEN_OUTCOME_BLIND_TEMPORAL_INDEX_PREPARATION",
        "output_dir": str(target),
        "package_id": package_id,
        "object_count": len(object_rows),
        "window_count": len(window_rows),
        "provider_requests_made": 0,
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }


def verify_formal_temporal_index_preparation(
    preparation_dir: str | Path,
) -> dict[str, Any]:
    root = Path(preparation_dir).resolve()
    _require(root.is_dir(), "temporal-index preparation is missing")
    _verify_checksums(root)
    manifest = _read_json(root / PREPARATION_MANIFEST, "preparation manifest")
    supplied = manifest.pop("preparation_sha256", None)
    _require(supplied == _sha256(_canonical(manifest)),
             "preparation manifest digest differs")
    manifest["preparation_sha256"] = supplied
    _require(manifest.get("schema_version") == PREPARATION_SCHEMA_VERSION,
             "preparation schema differs")
    for key in (
        "question_independent_preparation",
    ):
        _require(manifest.get(key) is True, f"{key} must be true")
    for key in (
        "task_outcomes_read",
        "hidden_label_values_read",
        "credentials_recorded",
    ):
        _require(manifest.get(key) is False, f"{key} must be false")
    frames = _read_jsonl(root / FRAMES_NAME, "caption frames")
    windows = _read_jsonl(root / WINDOWS_NAME, "fine windows")
    _require(manifest.get("frame_count") == len(frames), "frame count differs")
    _require(manifest.get("window_count") == len(windows), "window count differs")
    frame_keys: set[tuple[str, str]] = set()
    for row in frames:
        path = root / Path(*PurePosixPath(str(row["package_path"])).parts)
        digest, size = _hash_file(path)
        _require(
            digest == row.get("jpeg_sha256")
            and size == row.get("jpeg_size_bytes"),
            "caption frame identity differs",
        )
        frame_keys.add((str(row["object_id"]), digest))
    for window in windows:
        _require(
            all(
                (str(window["object_id"]), str(digest)) in frame_keys
                for digest in window["frame_sha256"]
            ),
            "fine window references an unknown frame",
        )
    return {
        "status": "VERIFIED_OUTCOME_BLIND_TEMPORAL_INDEX_PREPARATION",
        "package_id": manifest["package_id"],
        "preparation_sha256": supplied,
        "object_count": manifest["object_count"],
        "window_count": len(windows),
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }


def _caption_cache_path(cache_root: Path, window: Mapping[str, Any]) -> Path:
    return (
        cache_root
        / "validated"
        / str(window["object_id"])
        / f"{int(window['ordinal']):02d}.json"
    )


def _raw_cache_path(
    cache_root: Path,
    window: Mapping[str, Any],
    attempt: int,
) -> Path:
    return (
        cache_root
        / "raw"
        / str(window["object_id"])
        / f"{int(window['ordinal']):02d}.attempt-{attempt:02d}.json"
    )


def _next_raw_attempt(cache_root: Path, window: Mapping[str, Any]) -> int:
    directory = cache_root / "raw" / str(window["object_id"])
    prefix = f"{int(window['ordinal']):02d}.attempt-"
    attempts = []
    for path in directory.glob(prefix + "*.json"):
        suffix = path.stem.removeprefix(prefix)
        if suffix.isdigit():
            attempts.append(int(suffix))
    return max(attempts, default=0) + 1


def _load_valid_caption(
    path: Path,
    *,
    window: Mapping[str, Any],
    model_id: str,
    preparation_sha256: str,
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        entry = _read_json(path, "caption cache entry")
        expected = cache_entry_bindings(
            window=window,
            model_id=model_id,
            prompt_sha256=CAPTION_PROMPT_SHA256,
            segmentation_package_sha256=preparation_sha256,
        )
        # The preparation digest binds the complete collection.  It must not
        # invalidate an unchanged window merely because another object enters
        # or leaves the cohort.  Reuse is instead authorized by the complete
        # canonical window descriptor, its exact frame identities, the model,
        # prompt, and response schema.  The request digest is retained and
        # validated as an identity, but is not itself the reuse key: adding a
        # provider-side JSON-mode hint changes those bytes without changing
        # the caption contract or any of the image/prompt inputs.
        content_keys = set(expected) - {
            "cache_schema_version",
            "segmentation_package_sha256",
        }
        _require(
            entry.get("cache_schema_version") in {
                LEGACY_CAPTION_CACHE_SCHEMA_VERSION,
                FORMAL_CAPTION_CACHE_SCHEMA_VERSION,
            }
            and all(
                entry.get(key) == expected[key] for key in content_keys
            ),
            "caption cache binding differs",
        )
        caption = validate_structured_caption(entry.get("structured_caption"))
        request_input_sha256 = entry.get("request_input_sha256")
        _require(
            isinstance(request_input_sha256, str)
            and len(request_input_sha256) == 64
            and all(
                character in "0123456789abcdef"
                for character in request_input_sha256
            ),
            "caption cache request identity is invalid",
        )
        _require(
            entry.get("caption_sha256") == _sha256(_canonical(caption))
            and entry.get("search_text_sha256")
            == _sha256(caption_search_text(caption).encode("utf-8")),
            "caption cache content identity differs",
        )
        response_sha256 = entry.get("response_sha256")
        _require(
            isinstance(response_sha256, str)
            and len(response_sha256) == 64
            and all(character in "0123456789abcdef" for character in response_sha256),
            "caption cache response identity is invalid",
        )
        normalized = dict(entry)
        normalized.update(expected)
        normalized.update({
            "cache_schema_version": FORMAL_CAPTION_CACHE_SCHEMA_VERSION,
            "cache_reuse_scope": "canonical-window-content-v1",
            "structured_caption": caption,
        })
        return normalized
    except (FormalTemporalIndexError, ValueError, KeyError, OSError):
        return None


def _caption_request(
    *,
    window: Mapping[str, Any],
    frames_by_sha: Mapping[str, bytes],
    model_id: str,
) -> bytes:
    content: list[dict[str, Any]] = [{"type": "text", "text": CAPTION_PROMPT}]
    for timestamp, digest in zip(
        window["frame_timestamps_seconds"],
        window["frame_sha256"],
        strict=True,
    ):
        content.append({
            "type": "text",
            "text": f"timestamp_seconds={float(timestamp):.6f}",
        })
        content.append({
            "type": "image_url",
            "image_url": {
                "url": "data:image/jpeg;base64,"
                + base64.b64encode(frames_by_sha[str(digest)]).decode("ascii"),
                "detail": "low",
            },
        })
    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": content}],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    assert_caption_request_is_question_independent(payload)
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def materialize_formal_temporal_captions(
    preparation_dir: str | Path,
    *,
    output_dir: str | Path,
    cache_dir: str | Path,
    package_id: str,
    model_id: str,
    base_url: str,
    api_key: str,
    max_attempts_per_window: int = 2,
    parallelism: int = 1,
    timeout_seconds: float = 180.0,
    transport: Transport = _default_transport,
) -> dict[str, Any]:
    """Materialize durable question-independent captions and freeze them."""

    prep_root = Path(preparation_dir).resolve()
    prep = verify_formal_temporal_index_preparation(prep_root)
    _require(bool(model_id), "caption model_id is required")
    _require(bool(base_url) and bool(api_key), "caption provider is not configured")
    _require(
        type(max_attempts_per_window) is int
        and 1 <= max_attempts_per_window <= 3,
        "max_attempts_per_window must be within 1..3",
    )
    _require(
        type(parallelism) is int and 1 <= parallelism <= 8,
        "caption parallelism must be within 1..8",
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    cache_root = Path(cache_dir).resolve()
    cache_root.mkdir(parents=True, exist_ok=True)
    windows = _read_jsonl(prep_root / WINDOWS_NAME, "fine windows")
    frames = _read_jsonl(prep_root / FRAMES_NAME, "caption frames")
    frames_by_sha: dict[str, bytes] = {}
    for row in frames:
        path = prep_root / Path(*PurePosixPath(str(row["package_path"])).parts)
        data = path.read_bytes()
        _require(_sha256(data) == row["jpeg_sha256"],
                 "caption frame changed before materialization")
        frames_by_sha[str(row["jpeg_sha256"])] = data

    provider_requests = validation_failures = reused = 0
    entries: list[dict[str, Any]] = []
    todo: list[dict[str, Any]] = []
    for window in windows:
        cache_path = _caption_cache_path(cache_root, window)
        cached = _load_valid_caption(
            cache_path,
            window=window,
            model_id=model_id,
            preparation_sha256=prep["preparation_sha256"],
        )
        if cached is not None:
            entries.append(cached)
            reused += 1
            continue
        todo.append(window)

    def materialize_one(
        window: Mapping[str, Any],
    ) -> tuple[dict[str, Any], int, int, dict[str, Any]]:
        cache_path = _caption_cache_path(cache_root, window)
        body = _caption_request(
            window=window,
            frames_by_sha=frames_by_sha,
            model_id=model_id,
        )
        completed = None
        last_error = None
        request_count = 0
        failure_count = 0
        usage = None
        first_attempt = _next_raw_attempt(cache_root, window)
        for offset in range(max_attempts_per_window):
            attempt = first_attempt + offset
            request = urllib.request.Request(
                base_url.rstrip("/") + "/chat/completions",
                data=body,
                method="POST",
                headers={
                    "Authorization": "Bearer " + api_key,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            started = time.monotonic()
            try:
                raw = transport(request, timeout_seconds)
                request_count += 1
                response = json.loads(raw.decode("utf-8"))
            except Exception as exc:
                raise FormalTemporalIndexError(
                    f"caption transport failed for {window['window_id']}: "
                    f"{type(exc).__name__}"
                ) from exc
            elapsed = time.monotonic() - started
            record = build_raw_response_record(
                window=window,
                model_id=model_id,
                prompt_sha256=CAPTION_PROMPT_SHA256,
                segmentation_package_sha256=prep["preparation_sha256"],
                request_input_sha256=_sha256(body),
                response_bytes=raw,
                document=response,
            )
            _atomic_json(_raw_cache_path(cache_root, window, attempt), record)
            try:
                parsed = extract_single_json_object(record["raw_content"] or "")
                caption = validate_structured_caption(parsed)
            except Exception as exc:
                failure_count += 1
                last_error = type(exc).__name__
                continue
            entry = dict(cache_entry_bindings(
                window=window,
                model_id=model_id,
                prompt_sha256=CAPTION_PROMPT_SHA256,
                segmentation_package_sha256=prep["preparation_sha256"],
            ))
            entry.update({
                "cache_schema_version": FORMAL_CAPTION_CACHE_SCHEMA_VERSION,
                "cache_reuse_scope": "canonical-window-content-v1",
                "ordinal": window["ordinal"],
                "object_id": window["object_id"],
                "start_seconds": window["start_seconds"],
                "end_seconds": window["end_seconds"],
                "source_video_sha256": window["source_video_sha256"],
                "structured_caption": caption,
                "caption_sha256": _sha256(_canonical(caption)),
                "search_text_sha256": _sha256(
                    caption_search_text(caption).encode("utf-8")
                ),
                "request_input_sha256": _sha256(body),
                "response_sha256": _sha256(raw),
                "provider_attempt": attempt,
                "call_status": "completed",
                "credentials_recorded": False,
            })
            _atomic_json(cache_path, entry)
            usage = {
                "window_id": window["window_id"],
                "attempt": attempt,
                "request_body_bytes": len(body),
                "response_bytes": len(raw),
                "service_time_seconds": round(elapsed, 6),
                "provider_usage": record.get("usage"),
            }
            completed = entry
            break
        _require(
            completed is not None,
            f"caption validation failed for {window['window_id']} after "
            f"{max_attempts_per_window} attempts ({last_error})",
        )
        return completed, request_count, failure_count, usage

    usage_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        for completed, requests, failures, usage in executor.map(
            materialize_one, todo
        ):
            entries.append(completed)
            provider_requests += requests
            validation_failures += failures
            usage_rows.append(usage)

    entries.sort(key=lambda row: (row["object_id"], row["ordinal"]))
    _require(len(entries) == len(windows), "caption package is incomplete")
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        caption_bytes = _jsonl_bytes(entries)
        (staging / CAPTIONS_NAME).write_bytes(caption_bytes)
        cost = {
            "schema_version": "pathfinder.rsi-exam-index-build-cost/v1alpha1",
            "caption_model_id": model_id,
            "caption_window_count": len(entries),
            "provider_request_count_this_run": provider_requests,
            "validated_caption_reuse_count": reused,
            "response_validation_failure_count": validation_failures,
            "parallelism": parallelism,
            "per_request_usage": usage_rows,
            "monetary_cost_measured": False,
            "credentials_recorded": False,
        }
        (staging / BUILD_COST_NAME).write_bytes(_json_bytes(cost))
        manifest = {
            "schema_version": CAPTION_PACKAGE_SCHEMA_VERSION,
            "status": "FROZEN_QUESTION_INDEPENDENT_TEMPORAL_CAPTIONS",
            "package_id": package_id,
            "preparation_package_id": prep["package_id"],
            "preparation_sha256": prep["preparation_sha256"],
            "caption_prompt_sha256": CAPTION_PROMPT_SHA256,
            "model_id": model_id,
            "caption_count": len(entries),
            "object_ids": sorted({str(row["object_id"]) for row in entries}),
            "captions_sha256": _sha256(caption_bytes),
            "question_independent": True,
            "task_outcomes_read": False,
            "hidden_label_values_read": False,
            "credentials_recorded": False,
        }
        manifest["package_sha256"] = _sha256(_canonical(manifest))
        (staging / CAPTION_MANIFEST).write_bytes(_json_bytes(manifest))
        (staging / CHECKSUMS_NAME).write_bytes(_checksums(staging))
        verify_formal_temporal_caption_package(staging, prep_root)
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {
        "status": "FROZEN_QUESTION_INDEPENDENT_TEMPORAL_CAPTIONS",
        "output_dir": str(target),
        "caption_count": len(entries),
        "provider_request_count_this_run": provider_requests,
        "validated_caption_reuse_count": reused,
        "response_validation_failure_count": validation_failures,
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }


def verify_formal_temporal_caption_package(
    caption_dir: str | Path,
    preparation_dir: str | Path,
) -> dict[str, Any]:
    root = Path(caption_dir).resolve()
    prep = verify_formal_temporal_index_preparation(preparation_dir)
    _verify_checksums(root)
    manifest = _read_json(root / CAPTION_MANIFEST, "caption manifest")
    supplied = manifest.pop("package_sha256", None)
    _require(supplied == _sha256(_canonical(manifest)),
             "caption package digest differs")
    manifest["package_sha256"] = supplied
    captions = _read_jsonl(root / CAPTIONS_NAME, "fine captions")
    _require(
        manifest.get("schema_version") == CAPTION_PACKAGE_SCHEMA_VERSION
        and manifest.get("preparation_sha256") == prep["preparation_sha256"]
        and manifest.get("caption_count") == len(captions)
        and manifest.get("captions_sha256") == _sha256(_jsonl_bytes(captions)),
        "caption package binding differs",
    )
    for row in captions:
        validate_structured_caption(row.get("structured_caption"))
        _require(row.get("credentials_recorded") is False,
                 "caption entry records credentials")
    return {
        "status": "VERIFIED_QUESTION_INDEPENDENT_TEMPORAL_CAPTIONS",
        "package_id": manifest["package_id"],
        "package_sha256": supplied,
        "caption_count": len(captions),
        "object_count": len(manifest["object_ids"]),
        "credentials_recorded": False,
    }


def _public_tasks(path: Path) -> tuple[bytes, dict[str, dict[str, Any]]]:
    raw = path.read_bytes()
    _require(b"\r" not in raw, "public task set contains CR bytes")
    document = json.loads(raw.decode("utf-8"))
    _require(
        isinstance(document, dict)
        and document.get("label_values_included") is False
        and document.get("credentials_recorded") is False,
        "public task set is not safe for temporal-index finalization",
    )
    tasks = {
        str(row["object_id"]): row for row in document.get("tasks", [])
    }
    _require(bool(tasks), "public task set is empty")
    return raw, tasks


def _embedding_batches(
    *,
    texts: Sequence[str],
    model_id: str,
    dimension: int,
    base_url: str,
    api_key: str,
    batch_size: int,
    timeout_seconds: float,
    transport: Transport,
) -> tuple[list[list[int]], list[dict[str, Any]]]:
    _require(bool(texts), "no embedding inputs were supplied")
    model_batch_limits = {
        "text-embedding-v4": 10,
        "qwen3.7-text-embedding": 20,
    }
    batch_limit = model_batch_limits.get(model_id, 128)
    _require(
        1 <= batch_size <= batch_limit,
        f"embedding batch size exceeds the {model_id} limit of {batch_limit}",
    )
    vectors: list[list[int]] = []
    receipts: list[dict[str, Any]] = []
    for offset in range(0, len(texts), batch_size):
        batch = list(texts[offset:offset + batch_size])
        body = json.dumps({
            "model": model_id,
            "input": batch,
            "dimensions": dimension,
            "encoding_format": "float",
        }, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            base_url.rstrip("/") + "/embeddings",
            data=body,
            method="POST",
            headers={
                "Authorization": "Bearer " + api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        started = time.monotonic()
        try:
            raw = transport(request, timeout_seconds)
            response = json.loads(raw.decode("utf-8"))
            rows = sorted(response["data"], key=lambda row: row["index"])
        except Exception as exc:
            raise FormalTemporalIndexError(
                f"embedding request failed at input offset {offset}: "
                f"{type(exc).__name__}"
            ) from exc
        _require(len(rows) == len(batch), "embedding response count differs")
        vectors.extend([
            list(normalize_and_quantize(
                row["embedding"], dimension=dimension
            ))
            for row in rows
        ])
        receipts.append({
            "input_count": len(batch),
            "request_sha256": _sha256(body),
            "response_sha256": _sha256(raw),
            "service_time_seconds": round(time.monotonic() - started, 6),
            "usage": response.get("usage"),
        })
    return vectors, receipts


def _fraction_pair(value: float, duration: float, *, is_end: bool) -> list[int]:
    if is_end and value >= round(duration, 6):
        return [1, 1]
    fraction = Fraction(str(value)) / Fraction(str(duration))
    _require(0 <= fraction <= 1, "selected interval fraction is outside video")
    return [fraction.numerator, fraction.denominator]


def _runtime_manifests(
    *,
    n3_output: Path,
    runtime_root: Path,
    plans: Mapping[str, Mapping[str, Any]],
    policies: Mapping[str, N3TemporalSelectionPolicy],
) -> None:
    report = _read_json(n3_output / N3_MANIFEST_NAME, "N3 indexed package")
    rows = {
        str(row["object_id"]): row
        for row in report["objects"]
        if row["representation_id"] == "indexed_temporal_frame_bundle"
    }
    _require(set(rows) == set(plans), "N3 indexed rows differ from runtime plans")
    for object_id, plan in sorted(plans.items()):
        row = rows[object_id]
        bundle = n3_output / Path(*PurePosixPath(
            str(row["artifact_package_path"])
        ).parts)
        with tarfile.open(bundle) as archive:
            member = archive.extractfile("frame_bundle_manifest.json")
            _require(member is not None, "frame bundle manifest is missing")
            embedded = json.loads(member.read().decode("utf-8"))
        manifest = verify_runtime_frames(
            plan=plan,
            frames=embedded["frames"],
            source_video_sha256=embedded["source_video_sha256"],
        )
        manifest.update({
            "representation_label": RUNTIME_REPRESENTATION_LABEL,
            "n3_package_id": report["package_id"],
            "frame_bundle_sha256": row["artifact_sha256"],
            "frame_bundle_size_bytes": row["artifact_size_bytes"],
            "selection_policy": policies[object_id].to_dict(),
        })
        manifest["manifest_sha256"] = _sha256(_canonical(manifest))
        directory = runtime_root / object_id
        directory.mkdir(parents=True, exist_ok=False)
        (directory / "runtime-frame-plan.json").write_bytes(_json_bytes(plan))
        (directory / "runtime-frame-manifest.json").write_bytes(
            _json_bytes(manifest)
        )
        (directory / CHECKSUMS_NAME).write_bytes(_checksums(directory))


def finalize_formal_temporal_index(
    preparation_dir: str | Path,
    caption_dir: str | Path,
    collection_plan_dir: str | Path,
    public_task_set: str | Path,
    n3_raw_package_dir: str | Path,
    *,
    output_dir: str | Path,
    n3_output_dir: str | Path,
    runtime_frame_manifest_dir: str | Path,
    package_id: str,
    n3_package_id: str,
    embedding_model_id: str,
    base_url: str,
    api_key: str,
    dimension: int = DEFAULT_EMBEDDING_DIMENSION,
    batch_size: int = 64,
    runtime_frame_count: int = DEFAULT_RUNTIME_FRAME_COUNT,
    jpeg_max_dimension: int = DEFAULT_JPEG_MAX_DIMENSION,
    timeout_seconds: float = 180.0,
    transport: Transport = _default_transport,
    n3_sampler: Callable[
        ..., tuple[Sequence[SampledImage], float]
    ] = sample_video,
) -> dict[str, Any]:
    """Freeze per-object policies and materialize their exact N3 bundles."""

    prep_root = Path(preparation_dir).resolve()
    caption_root = Path(caption_dir).resolve()
    plan_root = Path(collection_plan_dir).resolve()
    raw_root = Path(n3_raw_package_dir).resolve()
    prep = verify_formal_temporal_index_preparation(prep_root)
    captions = verify_formal_temporal_caption_package(caption_root, prep_root)
    plan = verify_collection_plan(plan_root)
    verify_raw_cold_data_plane_package(raw_root)
    _require(bool(base_url) and bool(api_key), "embedding provider is not configured")
    _require(type(dimension) is int and dimension > 0,
             "embedding dimension must be positive")
    _require(type(runtime_frame_count) is int and runtime_frame_count > 0,
             "runtime frame count must be positive")

    task_raw, tasks = _public_tasks(Path(public_task_set).resolve())
    case_ids = _selected_case_ids(plan_root)
    _require(set(case_ids) <= set(tasks), "selected case has no public task")
    caption_rows = _read_jsonl(caption_root / CAPTIONS_NAME, "fine captions")
    window_rows = _read_jsonl(prep_root / WINDOWS_NAME, "fine windows")
    windows_by_object: dict[str, dict[int, dict[str, Any]]] = {}
    for row in window_rows:
        windows_by_object.setdefault(str(row["object_id"]), {})[
            int(row["ordinal"])
        ] = row
    captions_by_object: dict[str, list[dict[str, Any]]] = {}
    for row in caption_rows:
        captions_by_object.setdefault(str(row["object_id"]), []).append(row)
    _require(
        set(captions_by_object) == set(case_ids) == set(windows_by_object),
        "temporal-index objects differ from collection cases",
    )

    caption_texts: list[str] = []
    caption_keys: list[tuple[str, int]] = []
    anchor_texts: list[str] = []
    anchors: dict[str, dict[str, Any]] = {}
    for object_id in sorted(case_ids):
        for row in sorted(
            captions_by_object[object_id], key=lambda item: item["ordinal"]
        ):
            caption_texts.append(caption_search_text(row["structured_caption"]))
            caption_keys.append((object_id, int(row["ordinal"])))
        contextualized = contextualize_anchor_clause(tasks[object_id]["question"])
        anchors[object_id] = contextualized
        anchor_texts.append(contextualized["contextualized_anchor_text"])
    all_texts = caption_texts + anchor_texts
    vectors, embedding_receipts = _embedding_batches(
        texts=all_texts,
        model_id=embedding_model_id,
        dimension=dimension,
        base_url=base_url,
        api_key=api_key,
        batch_size=batch_size,
        timeout_seconds=timeout_seconds,
        transport=transport,
    )
    caption_vectors = vectors[:len(caption_texts)]
    anchor_vectors = vectors[len(caption_texts):]
    segment_vectors: dict[str, list[dict[str, Any]]] = {
        object_id: [] for object_id in case_ids
    }
    for (object_id, ordinal), text, vector in zip(
        caption_keys, caption_texts, caption_vectors, strict=True
    ):
        window = windows_by_object[object_id][ordinal]
        segment_vectors[object_id].append({
            "kind": "fine_window_caption",
            "object_id": object_id,
            "ordinal": ordinal,
            "segment_ordinal": ordinal,
            "segment_id": window["window_id"],
            "window_id": window["window_id"],
            "start_seconds": window["start_seconds"],
            "end_seconds": window["end_seconds"],
            "input_sha256": _sha256(text.encode("utf-8")),
            "model_id": embedding_model_id,
            "dimension": dimension,
            "vector_policy_id": VECTOR_POLICY_ID,
            "vector": vector,
        })

    selections: dict[str, dict[str, Any]] = {}
    anchor_rows: list[dict[str, Any]] = []
    vector_rows: list[dict[str, Any]] = []
    for object_id, anchor_vector in zip(
        sorted(case_ids), anchor_vectors, strict=True
    ):
        contextualized = anchors[object_id]
        anchor_row = {
            **contextualized,
            "kind": "contextualized_anchor_clause",
            "object_id": object_id,
            "model_id": embedding_model_id,
            "dimension": dimension,
            "vector_policy_id": VECTOR_POLICY_ID,
            "vector": anchor_vector,
        }
        anchor_rows.append(anchor_row)
        ranked = rank_segments_semantic(
            question_vector=anchor_vector,
            segment_vectors=segment_vectors[object_id],
        )
        selection = select_v2(
            question=tasks[object_id]["question"],
            ranked=ranked,
            windows_by_ordinal=windows_by_object[object_id],
        )
        _require(selection["fallback_used"] is False,
                 "temporal selection used a fallback")
        selections[object_id] = {
            **selection,
            "ranking": ranked,
            "public_question_sha256": contextualized["question_sha256"],
        }
        vector_rows.extend(segment_vectors[object_id])
    vector_rows.extend(anchor_rows)
    vector_rows.sort(key=lambda row: (
        row["object_id"], row["kind"], int(row.get("ordinal", -1))
    ))

    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        vectors_bytes = _jsonl_bytes(vector_rows)
        selections_bytes = _json_bytes(selections)
        (staging / VECTORS_NAME).write_bytes(vectors_bytes)
        (staging / "temporal-index-selections.json").write_bytes(
            selections_bytes
        )
        manifest = {
            "schema_version": INDEX_PACKAGE_SCHEMA_VERSION,
            "status": "FROZEN_OUTCOME_BLIND_MULTI_OBJECT_TEMPORAL_INDEX",
            "package_id": package_id,
            "collection_plan_sha256": plan["plan_sha256"],
            "preparation_sha256": prep["preparation_sha256"],
            "caption_package_sha256": captions["package_sha256"],
            "public_task_set_sha256": _sha256(task_raw),
            "embedding_model_id": embedding_model_id,
            "embedding_dimension": dimension,
            "embedding_request_count": len(embedding_receipts),
            "embedding_input_count": len(all_texts),
            "embedding_receipts": embedding_receipts,
            "object_count": len(case_ids),
            "window_vector_count": len(caption_vectors),
            "anchor_vector_count": len(anchor_vectors),
            "vectors_sha256": _sha256(vectors_bytes),
            "selections_sha256": _sha256(selections_bytes),
            "runtime_embedding_calls_required": False,
            "task_outcomes_read": False,
            "hidden_label_values_read": False,
            "credentials_recorded": False,
        }
        manifest["package_sha256"] = _sha256(_canonical(manifest))
        (staging / INDEX_MANIFEST).write_bytes(_json_bytes(manifest))

        policies: dict[str, N3TemporalSelectionPolicy] = {}
        policy_documents: dict[str, dict[str, Any]] = {}
        plans: dict[str, dict[str, Any]] = {}
        prep_manifest = _read_json(
            prep_root / PREPARATION_MANIFEST, "preparation manifest"
        )
        objects = {row["object_id"]: row for row in prep_manifest["objects"]}
        for object_id in sorted(case_ids):
            selection = selections[object_id]
            start, end = selection["selected_span_seconds"]
            duration = float(objects[object_id]["duration_seconds"])
            start_pair = _fraction_pair(float(start), duration, is_end=False)
            end_pair = _fraction_pair(float(end), duration, is_end=True)
            start_fraction = float(Fraction(*start_pair))
            end_fraction = float(Fraction(*end_pair))
            provenance = {
                "action_id": TEMPORAL_INDEX_V2_ACTION_ID,
                "anchor_top_k": selection["anchor_top_k"],
                "anchor_window_ordinals": selection[
                    "anchor_window_ordinals"
                ],
                "expansion_basis": selection["expansion_basis"],
                "fallback_used": False,
                "max_selected_windows": selection["max_selected_windows"],
                "merged_intervals_seconds": selection[
                    "merged_intervals_seconds"
                ],
                "public_question_sha256": selection[
                    "public_question_sha256"
                ],
                "relation": selection["relation"],
                "selected_window_ordinals": selection[
                    "selected_window_ordinals"
                ],
                "temporal_index_package_sha256": manifest["package_sha256"],
            }
            policy = N3TemporalSelectionPolicy(
                frame_count=runtime_frame_count,
                jpeg_max_dimension=jpeg_max_dimension,
                temporal_start_fraction=start_fraction,
                temporal_end_fraction=end_fraction,
                sampling_method=TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
                selection_provenance=provenance,
            )
            policies[object_id] = policy
            policy_documents[object_id] = policy.to_dict()
            plans[object_id] = build_runtime_frame_plan(
                plan_id=f"rsi-formal-runtime-frames-{object_id}-v1",
                object_id=object_id,
                source_video_sha256=objects[object_id]["source_video_sha256"],
                source_video_size_bytes=objects[object_id][
                    "source_video_size_bytes"
                ],
                duration_seconds=duration,
                start_fraction=start_pair,
                end_fraction=end_pair,
                frame_count=runtime_frame_count,
                jpeg_max_dimension=jpeg_max_dimension,
                selection_provenance=provenance,
                caption_index_window_ordinals=selection[
                    "selected_window_ordinals"
                ],
                bindings={
                    "collection_plan_sha256": plan["plan_sha256"],
                    "temporal_index_package_sha256": manifest[
                        "package_sha256"
                    ],
                    "preparation_sha256": prep["preparation_sha256"],
                },
            )
        policy_manifest = {
            "schema_version": POLICY_MANIFEST_SCHEMA_VERSION,
            "policies": policy_documents,
        }
        (staging / POLICY_MANIFEST).write_bytes(_json_bytes(policy_manifest))
        (staging / CHECKSUMS_NAME).write_bytes(_checksums(staging))
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)

    n3_output = Path(n3_output_dir).resolve()
    runtime_root = Path(runtime_frame_manifest_dir).resolve()
    _require(not n3_output.exists(), f"N3 output already exists: {n3_output}")
    _require(not runtime_root.exists(),
             f"runtime manifest output already exists: {runtime_root}")
    build_n3_indexed_data_plane_package(
        raw_root,
        output_dir=n3_output,
        package_id=n3_package_id,
        policies=policies,
        sampler=n3_sampler,
    )
    runtime_root.mkdir(parents=True)
    _runtime_manifests(
        n3_output=n3_output,
        runtime_root=runtime_root,
        plans=plans,
        policies=policies,
    )
    return {
        "status": "FROZEN_MULTI_OBJECT_TEMPORAL_INDEX_AND_N3_PACKAGE",
        "output_dir": str(target),
        "n3_output_dir": str(n3_output),
        "runtime_frame_manifest_dir": str(runtime_root),
        "object_count": len(case_ids),
        "window_vector_count": len(caption_vectors),
        "embedding_request_count": len(embedding_receipts),
        "embedding_input_count": len(all_texts),
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }


__all__ = [
    "FormalTemporalIndexError",
    "finalize_formal_temporal_index",
    "materialize_formal_temporal_captions",
    "prepare_formal_temporal_index",
    "verify_formal_temporal_caption_package",
    "verify_formal_temporal_index_preparation",
]
