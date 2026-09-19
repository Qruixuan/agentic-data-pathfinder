"""Focused coverage for generic fine-grained overlapping temporal windows."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_fine_temporal_windows import (
    CAPTION_PROMPT,
    CAPTION_PROMPT_SHA256,
    MAX_WINDOWS,
    SEGMENTATION_POLICY_ID,
    FineWindowError,
    assert_caption_request_is_question_independent,
    build_windows,
    caption_search_text,
    freeze_window_package,
    validate_structured_caption,
    window_geometry,
)

SRC = "c" * 64


def _frames(count: int, duration: float):
    step = duration / count
    return [
        {
            "timestamp_seconds": round(step * (2 * i + 1) / 2, 6),
            "jpeg_sha256": hashlib.sha256(f"f{i}".encode()).hexdigest(),
        }
        for i in range(count)
    ]


def _caption(**overrides):
    base = {
        "subjects": ["a person"],
        "subject_actions": ["walks forward"],
        "objects_interacted_with": ["a door"],
        "camera_relation": "moves closer to the camera",
        "initial_visible_state": "standing at the far side",
        "final_visible_state": "close to the lens",
        "observable_transition": "moves from far to near across the frames",
        "uncertainty": "face partly occluded",
    }
    base.update(overrides)
    return base


class WindowConstructionTest(unittest.TestCase):
    def test_geometry_is_duration_based_and_overlapping(self) -> None:
        window, stride = window_geometry(20.0)
        self.assertAlmostEqual(4.0, window)
        self.assertAlmostEqual(2.0, stride)
        # Overlap means the stride is strictly shorter than the window.
        self.assertLess(stride, window)

    def test_windows_are_deterministic_and_capped(self) -> None:
        frames = _frames(16, 15.6)
        first = build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=frames,
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        again = build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=frames,
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        self.assertEqual(first, again)
        self.assertGreater(len(first), 1)
        self.assertLessEqual(len(first), MAX_WINDOWS)
        self.assertEqual(list(range(len(first))), [w["ordinal"] for w in first])

    def test_windows_overlap_and_cover_the_tail(self) -> None:
        windows = build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=_frames(16, 15.6),
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        for earlier, later in zip(windows, windows[1:]):
            self.assertLess(later["start_seconds"], earlier["end_seconds"])
        self.assertAlmostEqual(15.6, max(w["end_seconds"] for w in windows), places=3)

    def test_every_window_binds_source_and_exact_frames(self) -> None:
        frames = _frames(16, 15.6)
        by_sha = {f["jpeg_sha256"] for f in frames}
        for window in build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=frames,
            source_video_sha256=SRC, source_video_size_bytes=100,
        ):
            self.assertEqual(SRC, window["source_video_sha256"])
            self.assertEqual(SEGMENTATION_POLICY_ID, window["segmentation_policy_id"])
            self.assertEqual(window["frame_count"], len(window["frame_sha256"]))
            self.assertTrue(set(window["frame_sha256"]) <= by_sha)
            for timestamp in window["frame_timestamps_seconds"]:
                self.assertGreaterEqual(timestamp, window["start_seconds"])
                self.assertLessEqual(timestamp, window["end_seconds"])

    def test_too_few_frames_cannot_form_an_index(self) -> None:
        with self.assertRaisesRegex(FineWindowError, "at least two windows"):
            build_windows(
                object_id="obj-x", duration_seconds=10.0, frames=_frames(1, 10.0),
                source_video_sha256=SRC, source_video_size_bytes=100,
            )

    def test_different_durations_yield_different_windowing(self) -> None:
        short = build_windows(
            object_id="obj-a", duration_seconds=10.0, frames=_frames(16, 10.0),
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        long = build_windows(
            object_id="obj-b", duration_seconds=40.0, frames=_frames(16, 40.0),
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        self.assertNotEqual(short[0]["end_seconds"], long[0]["end_seconds"])


class CaptionContractTest(unittest.TestCase):
    def test_prompt_is_single_and_question_independent(self) -> None:
        self.assertEqual(
            hashlib.sha256(CAPTION_PROMPT.encode()).hexdigest(), CAPTION_PROMPT_SHA256
        )
        lowered = CAPTION_PROMPT.lower()
        for banned in ("option", "correct", "task_success", "answer the"):
            self.assertNotIn(banned, lowered)
        # It must steer toward action rather than framing alone.
        self.assertIn("motion", lowered)
        self.assertIn("do not guess intent", lowered)

    def test_caption_request_rejects_task_material(self) -> None:
        for bad in (
            {"question": "what happened"},
            {"messages": [{"answer_options": ["A"]}]},
            {"meta": {"task_success": True}},
            {"meta": {"predicted_answer": "C"}},
        ):
            with self.subTest(payload=bad):
                with self.assertRaisesRegex(FineWindowError, "forbidden field"):
                    assert_caption_request_is_question_independent(bad)

    def test_clean_request_is_accepted(self) -> None:
        assert_caption_request_is_question_independent(
            {"model": "m", "messages": [{"role": "user", "content": "frames"}]}
        )

    def test_structured_caption_validation(self) -> None:
        validated = validate_structured_caption(_caption())
        self.assertEqual(["walks forward"], validated["subject_actions"])
        with self.assertRaisesRegex(FineWindowError, "fields changed"):
            validate_structured_caption({"subjects": ["x"]})
        with self.assertRaisesRegex(FineWindowError, "must be a list"):
            validate_structured_caption(_caption(subjects="not a list"))
        with self.assertRaisesRegex(FineWindowError, "action or transition"):
            validate_structured_caption(
                _caption(subject_actions=[], observable_transition="")
            )

    def test_search_text_leads_with_action_not_framing(self) -> None:
        text = caption_search_text(_caption())
        self.assertTrue(text.startswith("walks forward"))
        # Framing is present but last, so motion dominates the vector.
        self.assertLess(text.index("walks forward"), text.index("moves closer"))


class PackageTest(unittest.TestCase):
    def test_window_package_freezes_before_captions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            windows = build_windows(
                object_id="obj-x", duration_seconds=15.6, frames=_frames(16, 15.6),
                source_video_sha256=SRC, source_video_size_bytes=100,
            )
            package = freeze_window_package(
                package_id="pilot", windows=windows,
                source_bindings={"source_video_sha256": SRC},
                output_dir=Path(tmp) / "pkg",
            )
            self.assertFalse(package["captions_materialized"])
            self.assertEqual(CAPTION_PROMPT_SHA256, package["caption_prompt_sha256"])
            root = Path(tmp) / "pkg"
            for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
                expected, _, name = line.partition("  ")
                actual = hashlib.sha256((root / name).read_bytes()).hexdigest()
                self.assertEqual(expected, actual)

    def test_package_refuses_to_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            windows = build_windows(
                object_id="obj-x", duration_seconds=15.6, frames=_frames(16, 15.6),
                source_video_sha256=SRC, source_video_size_bytes=100,
            )
            freeze_window_package(
                package_id="pilot", windows=windows, source_bindings={},
                output_dir=Path(tmp) / "pkg",
            )
            with self.assertRaisesRegex(FineWindowError, "already exists"):
                freeze_window_package(
                    package_id="pilot", windows=windows, source_bindings={},
                    output_dir=Path(tmp) / "pkg",
                )


class SourceHygieneTest(unittest.TestCase):
    def test_no_visible_set_literals_and_no_control_bytes(self) -> None:
        import pathfinder.simulator.full_flow_fine_temporal_windows as module

        raw = Path(module.__file__).read_bytes()
        stray = {b for b in raw if b < 0x20 and b not in (0x09, 0x0A, 0x0D)}
        self.assertEqual(set(), stray, f"stray control bytes: {sorted(stray)}")
        source = raw.decode("utf-8")
        for forbidden in (
            "nextqa", "3429509208", "2435100235", "2461993294", "4010069381",
            "smoke-temporal", "baby", "vacuum", "13.68", "9.76",
        ):
            self.assertNotIn(forbidden, source, f"hard-coded token {forbidden!r}")


if __name__ == "__main__":
    unittest.main()
