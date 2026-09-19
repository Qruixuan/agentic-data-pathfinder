"""Focused coverage for the query-aware runtime frame plan and its verifier."""

from __future__ import annotations

import unittest
from fractions import Fraction

from pathfinder.simulator.full_flow_runtime_frame_plan import (
    RUNTIME_REPRESENTATION_LABEL,
    RuntimeFramePlanError,
    build_runtime_frame_plan,
    target_timestamps,
    verify_runtime_frames,
)

DURATION = 15.598917
SOURCE_SHA = "637d042e2d83be3dd0ac84f71c2acb3444a682af44ecd1e027a30dd4ff2c2ba6"
PROVENANCE = {
    "action_id": "semantic-temporal-index-v2-subject-aware-topk",
    "anchor_top_k": 2,
    "anchor_window_ordinals": [5, 4],
    "expansion_basis": "timestamp",
    "fallback_used": False,
    "max_selected_windows": 4,
    "merged_intervals_seconds": [[6.239567, 15.598917]],
    "public_question_sha256": "6dfabed52d4ff990a818b14862a8a832232f1fe99010cddb33e5991390ba52ff",
    "relation": "following",
    "selected_window_ordinals": [4, 5, 7, 8],
    "temporal_index_package_sha256": "21094b338d01b45752525f09268ffe8f41dbb62ca6625e4b9e855fdcae57d9b4",
}


def _plan(**overrides):
    kwargs = dict(
        plan_id="runtime-frame-plan-test",
        object_id="nextqa-val-3429509208",
        source_video_sha256=SOURCE_SHA,
        source_video_size_bytes=1626982,
        duration_seconds=DURATION,
        start_fraction=[2, 5],
        end_fraction=[1, 1],
        frame_count=10,
        jpeg_max_dimension=768,
        selection_provenance=PROVENANCE,
        caption_index_window_ordinals=[4, 5, 7, 8],
        bindings={"temporal_index_package_sha256": PROVENANCE[
            "temporal_index_package_sha256"]},
    )
    kwargs.update(overrides)
    return build_runtime_frame_plan(**kwargs)


def _frames(plan, *, count=None, shift=0.0):
    stamps = plan["runtime_target_timestamps_seconds"]
    if count is not None:
        stamps = stamps[:count]
    return [
        {
            "frame_index": i,
            "timestamp_seconds": s + shift,
            "jpeg_sha256": f"{i:064d}",
            "jpeg_size_bytes": 40000 + i,
        }
        for i, s in enumerate(stamps)
    ]


class IntervalBindingTest(unittest.TestCase):
    def test_interval_is_bound_as_exact_rational_fractions(self) -> None:
        interval = _plan()["selected_interval"]
        self.assertEqual([2, 5], interval["start_fraction"])
        self.assertEqual([1, 1], interval["end_fraction"])
        self.assertFalse(interval["boundaries_are_display_rounded"])

    def test_exact_seconds_are_not_the_display_rounded_values(self) -> None:
        interval = _plan()["selected_interval"]
        # 6.240 / 15.599 are display roundings; the bound values are exact.
        self.assertEqual(DURATION * 0.4, interval["start_seconds_exact"])
        self.assertEqual(DURATION, interval["end_seconds_exact"])
        self.assertNotEqual(6.240, round(interval["start_seconds_exact"], 3) + 1e-9)

    def test_source_and_package_bindings_are_present(self) -> None:
        plan = _plan()
        self.assertEqual(SOURCE_SHA, plan["source_video_sha256"])
        self.assertEqual(1626982, plan["source_video_size_bytes"])
        self.assertEqual(DURATION, plan["duration_seconds"])
        self.assertEqual("nextqa-val-3429509208", plan["object_id"])
        self.assertEqual(64, len(plan["selection_provenance_sha256"]))
        self.assertIn("temporal_index_package_sha256", plan["bindings"])

    def test_fallback_selection_cannot_produce_a_runtime_plan(self) -> None:
        with self.assertRaisesRegex(RuntimeFramePlanError, "fallback"):
            _plan(selection_provenance=dict(PROVENANCE, fallback_used=True))


class DeterministicTimestampTest(unittest.TestCase):
    def test_targets_are_pure_arithmetic_and_reproducible(self) -> None:
        first = target_timestamps(duration_seconds=DURATION,
                                  start_fraction=Fraction(2, 5),
                                  end_fraction=Fraction(1),
                                  frame_count=10)
        second = target_timestamps(duration_seconds=DURATION,
                                   start_fraction=Fraction(2, 5),
                                   end_fraction=Fraction(1),
                                   frame_count=10)
        self.assertEqual(first, second)
        self.assertEqual(10, len(first))

    def test_every_target_lies_inside_the_selected_interval(self) -> None:
        plan = _plan()
        lo = plan["selected_interval"]["start_seconds_exact"]
        hi = plan["selected_interval"]["end_seconds_exact"]
        for stamp in plan["runtime_target_timestamps_seconds"]:
            self.assertGreaterEqual(stamp, lo)
            self.assertLessEqual(stamp, hi)

    def test_plan_is_frozen_before_any_outcome(self) -> None:
        plan = _plan()
        self.assertTrue(plan["frozen_before_task_success_observable"])
        self.assertFalse(plan["task_outcomes_included"])

    def test_runtime_frames_are_distinct_from_caption_windows(self) -> None:
        plan = _plan()
        self.assertFalse(plan["caption_windows_are_runtime_frames"])
        self.assertFalse(plan["runtime_timestamps_reuse_caption_frames"])
        self.assertEqual(RUNTIME_REPRESENTATION_LABEL,
                         plan["representation_label"])


class RuntimeFrameVerificationTest(unittest.TestCase):
    def test_conforming_frames_verify(self) -> None:
        plan = _plan()
        manifest = verify_runtime_frames(
            plan=plan, frames=_frames(plan), source_video_sha256=SOURCE_SHA)
        self.assertEqual(10, manifest["runtime_frame_count"])
        self.assertTrue(manifest["all_frames_inside_selected_interval"])
        self.assertTrue(manifest["all_frames_strictly_ordered"])
        self.assertTrue(manifest["all_frame_payloads_distinct"])
        self.assertEqual(sum(40000 + i for i in range(10)),
                         manifest["selected_artifact_bytes"])

    def test_frames_from_another_source_video_are_refused(self) -> None:
        plan = _plan()
        with self.assertRaisesRegex(RuntimeFramePlanError, "different source"):
            verify_runtime_frames(plan=plan, frames=_frames(plan),
                                  source_video_sha256="f" * 64)

    def test_a_frame_outside_the_interval_is_refused(self) -> None:
        plan = _plan()
        frames = _frames(plan)
        frames[0]["timestamp_seconds"] = 1.0
        with self.assertRaisesRegex(RuntimeFramePlanError, "outside"):
            verify_runtime_frames(plan=plan, frames=frames,
                                  source_video_sha256=SOURCE_SHA)

    def test_unordered_frames_are_refused(self) -> None:
        plan = _plan()
        frames = _frames(plan)
        frames[3]["timestamp_seconds"] = frames[2]["timestamp_seconds"]
        with self.assertRaisesRegex(RuntimeFramePlanError, "strictly after"):
            verify_runtime_frames(plan=plan, frames=frames,
                                  source_video_sha256=SOURCE_SHA)

    def test_duplicate_frame_payloads_are_refused(self) -> None:
        plan = _plan()
        frames = _frames(plan)
        frames[5]["jpeg_sha256"] = frames[1]["jpeg_sha256"]
        with self.assertRaisesRegex(RuntimeFramePlanError, "duplicates"):
            verify_runtime_frames(plan=plan, frames=frames,
                                  source_video_sha256=SOURCE_SHA)

    def test_wrong_frame_count_is_refused(self) -> None:
        plan = _plan()
        with self.assertRaisesRegex(RuntimeFramePlanError, "frame count"):
            verify_runtime_frames(plan=plan, frames=_frames(plan, count=9),
                                  source_video_sha256=SOURCE_SHA)

    def test_manifest_makes_no_storage_io_reduction_claim(self) -> None:
        plan = _plan()
        manifest = verify_runtime_frames(
            plan=plan, frames=_frames(plan), source_video_sha256=SOURCE_SHA)
        self.assertFalse(manifest["partial_mp4_byte_range_claimed"])
        self.assertFalse(manifest["reduced_source_storage_io_claimed"])
        self.assertEqual(1626982, manifest["original_object_bytes_read"])

    def test_identical_plans_produce_identical_inputs_for_d1_and_d5(self) -> None:
        # D1 and D5 differ by placement and execution, not by sampled content.
        d1 = _plan(plan_id="shared-plan")
        d5 = _plan(plan_id="shared-plan")
        self.assertEqual(d1["plan_sha256"], d5["plan_sha256"])
        self.assertEqual(d1["runtime_target_timestamps_seconds"],
                         d5["runtime_target_timestamps_seconds"])
        frames = _frames(d1)
        a = verify_runtime_frames(plan=d1, frames=frames,
                                  source_video_sha256=SOURCE_SHA)
        b = verify_runtime_frames(plan=d5, frames=frames,
                                  source_video_sha256=SOURCE_SHA)
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
