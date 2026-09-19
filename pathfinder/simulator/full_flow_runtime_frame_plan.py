"""Freeze and verify the runtime frame plan for a query-aware N3 projection.

A temporal index selects an *interval*. N3 then decodes its own runtime frames
from the bound source MP4 inside that interval; it does not reuse the frame
timestamps the caption pilot happened to sample. Four things are therefore
distinct and are named distinctly throughout:

``caption_index_windows``
    the overlapping caption windows the index ranked in order to choose an
    anchor. They exist only to select the interval.
``selected_interval``
    the merged interval the selection produced, bound as an exact rational
    fraction pair of the object duration rather than a display-rounded second
    count.
``runtime_frame_timestamps``
    the timestamps N3 actually decoded from the source MP4 inside that
    interval, frozen before any task outcome is observable.
``runtime_frame_artifacts``
    the JPEG payloads delivered to N6, each with its own digest and size.

The plan is pure arithmetic over the bound duration, so the sampler's target
times are predictable before decoding. The decoded times are whatever the
container yields at or after each target, so they are recorded rather than
predicted, and then verified against the interval.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from fractions import Fraction
from typing import Any

RUNTIME_FRAME_PLAN_SCHEMA_VERSION = (
    "pathfinder.full-flow-runtime-frame-plan/v1alpha1"
)
RUNTIME_FRAME_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.full-flow-runtime-frame-manifest/v1alpha1"
)

# The public name for what this path actually transports.
RUNTIME_REPRESENTATION_LABEL = "query-aware temporal-index-selected frames"


class RuntimeFramePlanError(ValueError):
    """Raised when a runtime frame plan or manifest is not exact."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise RuntimeFramePlanError(message)


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _fraction(pair: Sequence[int], label: str) -> Fraction:
    _require(
        isinstance(pair, Sequence) and not isinstance(pair, (str, bytes))
        and len(pair) == 2
        and all(type(v) is int for v in pair)
        and pair[1] > 0,
        f"{label} must be an integer numerator/denominator pair",
    )
    return Fraction(int(pair[0]), int(pair[1]))


def target_timestamps(
    *,
    duration_seconds: float,
    start_fraction: Fraction,
    end_fraction: Fraction,
    frame_count: int,
) -> list[float]:
    """Reproduce the sampler's target times exactly, without decoding.

    Mirrors ``sample_video``: ``start + span * (i + 0.5) / frame_count``.
    """

    _require(frame_count > 0, "frame_count must be positive")
    _require(
        0 <= start_fraction < end_fraction <= 1,
        "the selected interval is outside the object",
    )
    start = duration_seconds * float(start_fraction)
    span = duration_seconds * (float(end_fraction) - float(start_fraction))
    return [
        start + span * (index + 0.5) / frame_count
        for index in range(frame_count)
    ]


def build_runtime_frame_plan(
    *,
    plan_id: str,
    object_id: str,
    source_video_sha256: str,
    source_video_size_bytes: int,
    duration_seconds: float,
    start_fraction: Sequence[int],
    end_fraction: Sequence[int],
    frame_count: int,
    jpeg_max_dimension: int,
    selection_provenance: Mapping[str, Any],
    caption_index_window_ordinals: Sequence[int],
    bindings: Mapping[str, str],
) -> dict[str, Any]:
    """Freeze everything the runtime projection must honour."""

    start = _fraction(start_fraction, "start_fraction")
    end = _fraction(end_fraction, "end_fraction")
    _require(bool(object_id), "object_id is required")
    _require(len(source_video_sha256) == 64, "source_video_sha256 is not sha256")
    _require(
        type(source_video_size_bytes) is int and source_video_size_bytes > 0,
        "source_video_size_bytes must be positive",
    )
    _require(
        isinstance(duration_seconds, float) and duration_seconds > 0,
        "duration_seconds must be positive",
    )
    _require(
        selection_provenance.get("fallback_used") is False,
        "a runtime plan must not be built from a fallback selection",
    )

    interval_start = duration_seconds * float(start)
    interval_end = duration_seconds * float(end)
    targets = target_timestamps(
        duration_seconds=duration_seconds, start_fraction=start,
        end_fraction=end, frame_count=frame_count,
    )
    provenance_sha = sha256_hex(canonical(dict(selection_provenance)))

    plan = {
        "schema_version": RUNTIME_FRAME_PLAN_SCHEMA_VERSION,
        "plan_id": plan_id,
        "representation_label": RUNTIME_REPRESENTATION_LABEL,
        "object_id": object_id,

        # --- exact source binding ---
        "source_video_sha256": source_video_sha256,
        "source_video_size_bytes": source_video_size_bytes,
        "duration_seconds": duration_seconds,

        # --- the interval, bound exactly, not display-rounded ---
        "selected_interval": {
            "start_fraction": [start.numerator, start.denominator],
            "end_fraction": [end.numerator, end.denominator],
            "start_seconds_exact": interval_start,
            "end_seconds_exact": interval_end,
            "boundaries_are_display_rounded": False,
        },

        # --- what selected it, kept distinct from what runs ---
        "caption_index_window_ordinals": [int(o) for o in caption_index_window_ordinals],
        "caption_windows_are_runtime_frames": False,
        "selection_provenance": json.loads(
            canonical(dict(selection_provenance)).decode("utf-8")
        ),
        "selection_provenance_sha256": provenance_sha,

        # --- what N3 will decode ---
        "runtime_frame_count": frame_count,
        "runtime_jpeg_max_dimension": jpeg_max_dimension,
        "runtime_target_timestamps_seconds": targets,
        "runtime_timestamps_reuse_caption_frames": False,

        "bindings": dict(bindings),
        "frozen_before_task_success_observable": True,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "task_outcomes_included": False,
    }
    plan["plan_sha256"] = sha256_hex(canonical(plan))
    return plan


def verify_runtime_frames(
    *,
    plan: Mapping[str, Any],
    frames: Sequence[Mapping[str, Any]],
    source_video_sha256: str,
) -> dict[str, Any]:
    """Check every decoded runtime frame against the frozen plan."""

    _require(
        source_video_sha256 == plan["source_video_sha256"],
        "runtime frames were decoded from a different source video",
    )
    _require(
        len(frames) == plan["runtime_frame_count"],
        "runtime frame count does not match the frozen plan",
    )
    interval = plan["selected_interval"]
    start = float(interval["start_seconds_exact"])
    end = float(interval["end_seconds_exact"])

    seen_digests: set[str] = set()
    previous = None
    timestamps: list[float] = []
    total = 0
    for index, frame in enumerate(frames):
        _require(
            int(frame["frame_index"]) == index,
            f"runtime frame {index} is out of order",
        )
        stamp = float(frame["timestamp_seconds"])
        _require(
            start <= stamp <= end,
            f"runtime frame {index} lies outside the selected interval",
        )
        _require(
            previous is None or stamp > previous,
            f"runtime frame {index} is not strictly after its predecessor",
        )
        digest = str(frame["jpeg_sha256"])
        _require(len(digest) == 64, f"runtime frame {index} digest is not sha256")
        _require(
            digest not in seen_digests,
            f"runtime frame {index} duplicates an earlier frame payload",
        )
        size = int(frame["jpeg_size_bytes"])
        _require(size > 0, f"runtime frame {index} is empty")
        seen_digests.add(digest)
        timestamps.append(stamp)
        total += size
        previous = stamp

    return {
        "schema_version": RUNTIME_FRAME_MANIFEST_SCHEMA_VERSION,
        "representation_label": RUNTIME_REPRESENTATION_LABEL,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "object_id": plan["object_id"],
        "source_video_sha256": source_video_sha256,
        "runtime_frame_count": len(frames),
        "runtime_frame_timestamps_seconds": timestamps,
        "runtime_frame_digests": [str(f["jpeg_sha256"]) for f in frames],
        "selected_artifact_bytes": total,
        "all_frames_inside_selected_interval": True,
        "all_frames_strictly_ordered": True,
        "all_frame_payloads_distinct": True,
        "all_frames_bound_to_source_video": True,
        "partial_mp4_byte_range_claimed": False,
        "reduced_source_storage_io_claimed": False,
        "original_object_bytes_read": plan["source_video_size_bytes"],
        "original_object_read_mode": "complete-object-read-then-source-side-decode",
        "credentials_recorded": False,
    }


__all__ = [
    "RUNTIME_FRAME_MANIFEST_SCHEMA_VERSION",
    "RUNTIME_FRAME_PLAN_SCHEMA_VERSION",
    "RUNTIME_REPRESENTATION_LABEL",
    "RuntimeFramePlanError",
    "build_runtime_frame_plan",
    "canonical",
    "sha256_hex",
    "target_timestamps",
    "verify_runtime_frames",
]
