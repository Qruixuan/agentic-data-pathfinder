"""Generic fine-grained overlapping temporal windows and action captions.

The coarse question-independent digest describes camera framing across a few
long spans, which cannot separate an anchor event from what follows it.  This
module defines a duration-based, overlapping windowing policy that applies
identically to any video, and a single question-independent caption contract
that asks for actor motion and state transitions rather than framing.

Window boundaries derive only from the video duration and frozen constants.
No question, answer, outcome, prediction or known timestamp participates.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

FINE_WINDOW_SCHEMA_VERSION = "pathfinder.full-flow-fine-temporal-windows/v1alpha1"
FINE_CAPTION_SCHEMA_VERSION = "pathfinder.full-flow-fine-action-caption/v1alpha1"
SEGMENTATION_POLICY_ID = "duration-fraction-overlapping-windows-v1"

# Frozen generic constants.  The window is a fixed fraction of the duration and
# successive windows overlap by half a window, so an event near a boundary is
# still wholly contained in some window.
WINDOW_DURATION_FRACTION = 0.20
WINDOW_OVERLAP_FRACTION = 0.50
MIN_WINDOW_SECONDS = 1.0
MAX_WINDOWS = 12
MIN_FRAMES_PER_WINDOW = 2

# One prompt for every window of every video.  It is question-independent and
# forbids guessing beyond what the frames show.
CAPTION_PROMPT = (
    "You are building a question-independent temporal index for a video.\n"
    "You are shown consecutive JPEG frames from ONE short time window, in "
    "chronological order.\n"
    "Describe ONLY what is visibly supported by these frames.\n"
    "Emphasise what the subjects DO: their motion, and how their position or "
    "state changes from the first frame to the last. Do not describe only the "
    "camera framing.\n"
    "Do not guess intent, do not infer events outside this window, do not "
    "answer any question, and do not speculate about what happens next.\n"
    "If something is occluded or unclear, say so in 'uncertainty'.\n\n"
    "Return ONLY a JSON object with exactly these keys:\n"
    '{"subjects": [string], "subject_actions": [string], '
    '"objects_interacted_with": [string], "camera_relation": string, '
    '"initial_visible_state": string, "final_visible_state": string, '
    '"observable_transition": string, "uncertainty": string}\n'
)
CAPTION_PROMPT_SHA256 = hashlib.sha256(CAPTION_PROMPT.encode("utf-8")).hexdigest()

_CAPTION_KEYS = {
    "subjects", "subject_actions", "objects_interacted_with", "camera_relation",
    "initial_visible_state", "final_visible_state", "observable_transition",
    "uncertainty",
}
_LIST_KEYS = {"subjects", "subject_actions", "objects_interacted_with"}
# A caption request or response must never carry task material.
_FORBIDDEN_CAPTION_KEYS = frozenset({
    "question", "answer", "answer_options", "correct_answer_id", "option_id",
    "task_success", "prediction", "predicted_answer", "hidden_label", "score",
})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class FineWindowError(ValueError):
    """Raised before an unbound window or caption can be frozen."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FineWindowError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def window_geometry(duration_seconds: float) -> tuple[float, float]:
    """Window length and stride, from duration and frozen constants only."""

    _require(
        isinstance(duration_seconds, float) and duration_seconds > 0.0,
        "duration_seconds must be positive",
    )
    window = max(MIN_WINDOW_SECONDS, duration_seconds * WINDOW_DURATION_FRACTION)
    stride = max(MIN_WINDOW_SECONDS / 2.0, window * (1.0 - WINDOW_OVERLAP_FRACTION))
    return window, stride


def build_windows(
    *,
    object_id: str,
    duration_seconds: float,
    frames: Sequence[Mapping[str, Any]],
    source_video_sha256: str,
    source_video_size_bytes: int,
) -> list[dict[str, Any]]:
    """Build bounded overlapping windows bound to exact frozen frames.

    ``frames`` are the already-frozen sampled frames; each window binds those
    whose timestamps fall inside it.  Nothing is decoded and no frame is
    invented.
    """

    _require(bool(object_id), "object_id is required")
    _digest(source_video_sha256, "source_video_sha256")
    _require(bool(frames), "no frames were supplied")
    window, stride = window_geometry(duration_seconds)

    windows: list[dict[str, Any]] = []
    start = 0.0
    ordinal = 0
    while start < duration_seconds and len(windows) < MAX_WINDOWS:
        end = min(start + window, duration_seconds)
        inside = [
            f for f in frames
            if start <= float(f["timestamp_seconds"]) <= end
        ]
        if len(inside) >= MIN_FRAMES_PER_WINDOW:
            windows.append({
                "window_id": f"{object_id}#win{ordinal:02d}",
                "ordinal": ordinal,
                "object_id": object_id,
                "start_seconds": round(start, 6),
                "end_seconds": round(end, 6),
                "duration_seconds": duration_seconds,
                "frame_count": len(inside),
                "frame_timestamps_seconds": [
                    float(f["timestamp_seconds"]) for f in inside
                ],
                "frame_sha256": [str(f["jpeg_sha256"]) for f in inside],
                "source_video_sha256": source_video_sha256,
                "source_video_size_bytes": source_video_size_bytes,
                "segmentation_policy_id": SEGMENTATION_POLICY_ID,
                "window_seconds": round(window, 6),
                "stride_seconds": round(stride, 6),
            })
            ordinal += 1
        if end >= duration_seconds:
            break
        start += stride
    _require(len(windows) >= 2, "a fine temporal index needs at least two windows")
    _require(len(windows) <= MAX_WINDOWS, "window count exceeds the frozen cap")
    return windows


def assert_caption_request_is_question_independent(request: Any) -> None:
    """Refuse any caption request carrying task or outcome material."""

    if isinstance(request, Mapping):
        for key, nested in request.items():
            _require(
                str(key).casefold().replace("-", "_") not in _FORBIDDEN_CAPTION_KEYS,
                f"caption request carries forbidden field {key!r}",
            )
            assert_caption_request_is_question_independent(nested)
    elif isinstance(request, (list, tuple)):
        for nested in request:
            assert_caption_request_is_question_independent(nested)


def validate_structured_caption(caption: Any) -> dict[str, Any]:
    """Validate the frozen structured caption schema."""

    _require(isinstance(caption, Mapping), "structured caption must be an object")
    _require(set(caption) == _CAPTION_KEYS, "structured caption fields changed")
    result: dict[str, Any] = {}
    for key in sorted(_CAPTION_KEYS):
        value = caption[key]
        if key in _LIST_KEYS:
            _require(
                isinstance(value, list)
                and all(isinstance(item, str) and item.strip() for item in value),
                f"{key} must be a list of non-empty strings",
            )
            result[key] = [item.strip() for item in value]
        elif isinstance(value, list):
            # Providers legitimately answer a scalar descriptive field with a
            # list. Join deterministically rather than discarding the caption;
            # this normalizes shape only and never changes what was asked.
            _require(
                all(isinstance(item, str) for item in value),
                f"{key} list must contain only strings",
            )
            result[key] = ", ".join(item.strip() for item in value if item.strip())
        elif value is None:
            result[key] = ""
        else:
            _require(isinstance(value, str), f"{key} must be a string")
            result[key] = value.strip()
    _require(
        bool(result["observable_transition"]) or bool(result["subject_actions"]),
        "a caption must describe some action or transition",
    )
    return result


def caption_search_text(caption: Mapping[str, Any]) -> str:
    """Flatten a structured caption into the text that gets embedded.

    Action and transition fields lead, so motion dominates the vector rather
    than camera framing.
    """

    validated = validate_structured_caption(caption)
    parts = [
        " ".join(validated["subject_actions"]),
        validated["observable_transition"],
        validated["initial_visible_state"],
        validated["final_visible_state"],
        " ".join(validated["subjects"]),
        " ".join(validated["objects_interacted_with"]),
        validated["camera_relation"],
    ]
    return " ".join(part for part in parts if part).strip()


def freeze_window_package(
    *,
    package_id: str,
    windows: Sequence[Mapping[str, Any]],
    source_bindings: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze the window list before any caption call is made."""

    _require(bool(windows), "no windows to freeze")
    rows = [json.loads(_canonical(w)) for w in windows]
    window_bytes = b"".join(_canonical(row) + b"\n" for row in rows)
    package = {
        "schema_version": FINE_WINDOW_SCHEMA_VERSION,
        "package_id": package_id,
        "segmentation_policy_id": SEGMENTATION_POLICY_ID,
        "window_duration_fraction": WINDOW_DURATION_FRACTION,
        "window_overlap_fraction": WINDOW_OVERLAP_FRACTION,
        "min_window_seconds": MIN_WINDOW_SECONDS,
        "max_windows": MAX_WINDOWS,
        "window_count": len(rows),
        "object_ids": sorted({r["object_id"] for r in rows}),
        "windows_sha256": _sha256(window_bytes),
        "caption_prompt_sha256": CAPTION_PROMPT_SHA256,
        "source_bindings": json.loads(_canonical(source_bindings)),
        "captions_materialized": False,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "task_outcomes_included": False,
        "eligible_for_scientific_claims": False,
        "status": "FROZEN_FINE_TEMPORAL_WINDOWS",
    }
    package["package_sha256"] = _sha256(_canonical(package))
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.mkdir(parents=True)
    (target / "fine-window-package.json").write_bytes(_canonical(package))
    (target / "fine-windows.jsonl").write_bytes(window_bytes)
    manifest = "\n".join(
        f"{_sha256((target / name).read_bytes())}  {name}"
        for name in sorted(("fine-window-package.json", "fine-windows.jsonl"))
    )
    (target / "SHA256SUMS").write_bytes((manifest + "\n").encode("utf-8"))
    return package


__all__ = [
    "CAPTION_PROMPT",
    "CAPTION_PROMPT_SHA256",
    "FINE_CAPTION_SCHEMA_VERSION",
    "FINE_WINDOW_SCHEMA_VERSION",
    "MAX_WINDOWS",
    "MIN_FRAMES_PER_WINDOW",
    "MIN_WINDOW_SECONDS",
    "SEGMENTATION_POLICY_ID",
    "WINDOW_DURATION_FRACTION",
    "WINDOW_OVERLAP_FRACTION",
    "FineWindowError",
    "assert_caption_request_is_question_independent",
    "build_windows",
    "caption_search_text",
    "freeze_window_package",
    "validate_structured_caption",
    "window_geometry",
]
