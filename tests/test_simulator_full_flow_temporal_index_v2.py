"""Focused coverage for subject-aware, timestamp-expanded temporal index v2."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_temporal_index import TemporalIndexError
from pathfinder.simulator.full_flow_temporal_index_v2 import (
    DEFAULT_ANCHOR_TOP_K,
    TEMPORAL_INDEX_V2_ACTION_ID,
    contextualize_anchor_clause,
    merge_intervals,
    public_question_subject,
    select_v2,
    strictly_after,
    strictly_before,
)


def _window(ordinal, start, end):
    return {
        "ordinal": ordinal,
        "window_id": f"obj#win{ordinal:02d}",
        "start_seconds": float(start),
        "end_seconds": float(end),
    }


# Overlapping windows, deliberately shaped like the real policy.
WINDOWS = {
    0: _window(0, 0.0, 3.0),
    1: _window(1, 1.5, 4.5),
    2: _window(2, 3.0, 6.0),
    3: _window(3, 4.5, 7.5),
    4: _window(4, 6.0, 9.0),
    5: _window(5, 7.5, 10.5),
}


def _ranked(order):
    return [{"segment_id": f"obj#win{o:02d}", "ordinal": o,
             "similarity_score_units": 1000 - index}
            for index, o in enumerate(order)]


class SubjectContextualizationTest(unittest.TestCase):
    def test_pronoun_is_replaced_by_the_public_question_subject(self) -> None:
        result = contextualize_anchor_clause(
            "what did the baby do after he approached near the camera"
        )
        self.assertEqual("the baby", result["public_question_subject"])
        self.assertEqual("the baby approached near the camera",
                         result["contextualized_anchor_text"])
        self.assertTrue(result["pronoun_substituted"])

    def test_resolution_is_not_specific_to_any_one_subject(self) -> None:
        cases = {
            "what did the man do after he opened the door": "the man opened the door",
            "what did the cyclist do after she stopped": "the cyclist stopped",
            "what did the machine do after it started": "the machine started",
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(
                    expected,
                    contextualize_anchor_clause(question)["contextualized_anchor_text"],
                )

    def test_clause_with_explicit_subject_is_left_alone(self) -> None:
        result = contextualize_anchor_clause(
            "what happened before the vehicle stopped moving"
        )
        self.assertFalse(result["pronoun_substituted"])
        self.assertEqual("the vehicle stopped moving",
                         result["contextualized_anchor_text"])

    def test_bare_pronoun_is_not_accepted_as_a_subject(self) -> None:
        self.assertIsNone(
            public_question_subject("what did they do after it fell over")
        )

    def test_no_object_or_answer_literal_in_source(self) -> None:
        import pathfinder.simulator.full_flow_temporal_index_v2 as module

        raw = Path(module.__file__).read_bytes()
        stray = {b for b in raw if b < 0x20 and b not in (0x09, 0x0A, 0x0D)}
        self.assertEqual(set(), stray)
        source = raw.decode("utf-8")
        for forbidden in (
            "nextqa", "3429509208", "baby", "vacuum", "camera", "smoke-temporal",
            "task_success", "correct_answer",
        ):
            self.assertNotIn(forbidden, source, f"hard-coded token {forbidden!r}")


class TimestampExpansionTest(unittest.TestCase):
    def test_strictly_after_uses_timestamps_not_ordinals(self) -> None:
        # win03 has a higher ordinal than win02 but starts inside it.
        self.assertFalse(strictly_after(WINDOWS[3], WINDOWS[2]["end_seconds"]))
        self.assertTrue(strictly_after(WINDOWS[4], WINDOWS[2]["end_seconds"]))
        self.assertTrue(strictly_before(WINDOWS[0], WINDOWS[2]["start_seconds"]))
        self.assertFalse(strictly_before(WINDOWS[1], WINDOWS[2]["start_seconds"]))

    def test_following_expansion_excludes_overlapping_windows(self) -> None:
        result = select_v2(
            question="what did the person do after he moved",
            ranked=_ranked([2, 1, 0, 3, 4, 5]),
            windows_by_ordinal=WINDOWS, anchor_top_k=1, max_selected_windows=3,
        )
        self.assertEqual([2], result["anchor_window_ordinals"])
        # win03 overlaps the anchor and must not be treated as later.
        self.assertNotIn(3, result["relation_expanded_ordinals"])
        for ordinal in result["relation_expanded_ordinals"]:
            self.assertGreaterEqual(
                WINDOWS[ordinal]["start_seconds"], WINDOWS[2]["end_seconds"]
            )

    def test_top_k_anchors_are_retained(self) -> None:
        result = select_v2(
            question="what did the person do after he moved",
            ranked=_ranked([2, 5, 0, 1, 3, 4]),
            windows_by_ordinal=WINDOWS, anchor_top_k=2, max_selected_windows=4,
        )
        self.assertEqual([2, 5], result["anchor_window_ordinals"])
        self.assertIn(2, result["selected_window_ordinals"])
        self.assertIn(5, result["selected_window_ordinals"])

    def test_preceding_expansion_is_also_timestamp_based(self) -> None:
        result = select_v2(
            question="what happened before the person moved",
            ranked=_ranked([4, 3, 2, 1, 0, 5]),
            windows_by_ordinal=WINDOWS, anchor_top_k=1, max_selected_windows=3,
        )
        self.assertEqual("preceding", result["relation"])
        for ordinal in result["relation_expanded_ordinals"]:
            self.assertLessEqual(
                WINDOWS[ordinal]["end_seconds"], WINDOWS[4]["start_seconds"]
            )

    def test_evidence_budget_is_enforced(self) -> None:
        result = select_v2(
            question="what did the person do after he moved",
            ranked=_ranked([0, 1, 2, 3, 4, 5]),
            windows_by_ordinal=WINDOWS, anchor_top_k=2, max_selected_windows=3,
        )
        self.assertLessEqual(len(result["selected_window_ordinals"]), 3)

    def test_budget_smaller_than_anchors_is_refused(self) -> None:
        with self.assertRaisesRegex(TemporalIndexError, "budget"):
            select_v2(
                question="what did the person do after he moved",
                ranked=_ranked([0, 1, 2]), windows_by_ordinal=WINDOWS,
                anchor_top_k=2, max_selected_windows=1,
            )

    def test_selection_is_ordered_deduplicated_and_never_falls_back(self) -> None:
        result = select_v2(
            question="what did the person do after he moved",
            ranked=_ranked([2, 4, 0, 1, 3, 5]),
            windows_by_ordinal=WINDOWS, anchor_top_k=2, max_selected_windows=4,
        )
        ordinals = result["selected_window_ordinals"]
        self.assertEqual(sorted(set(ordinals)), ordinals)
        self.assertFalse(result["fallback_used"])
        self.assertEqual("timestamp", result["expansion_basis"])
        self.assertEqual(TEMPORAL_INDEX_V2_ACTION_ID, result["action_id"])


class IntervalMergeTest(unittest.TestCase):
    def test_overlapping_intervals_merge_deterministically(self) -> None:
        merged = merge_intervals([WINDOWS[2], WINDOWS[3]])
        self.assertEqual([(3.0, 7.5)], merged)
        self.assertEqual(merged, merge_intervals([WINDOWS[3], WINDOWS[2]]))

    def test_disjoint_intervals_stay_separate(self) -> None:
        self.assertEqual([(0.0, 3.0), (6.0, 9.0)],
                         merge_intervals([WINDOWS[0], WINDOWS[4]]))

    def test_touching_intervals_merge(self) -> None:
        self.assertEqual([(0.0, 6.0)], merge_intervals([WINDOWS[0], WINDOWS[2]]))


class ClaimBoundaryTest(unittest.TestCase):
    def test_result_declares_no_hidden_or_credential_content(self) -> None:
        result = select_v2(
            question="what did the person do after he moved",
            ranked=_ranked([2, 4, 0, 1, 3, 5]), windows_by_ordinal=WINDOWS,
        )
        self.assertFalse(result["credentials_recorded"])
        self.assertFalse(result["hidden_label_values_included"])
        blob = json.dumps(result).casefold()
        for forbidden in ("task_success", "correct_answer", "token", "secret"):
            self.assertNotIn(forbidden, blob)

    def test_defaults_are_frozen_constants(self) -> None:
        self.assertEqual(2, DEFAULT_ANCHOR_TOP_K)


if __name__ == "__main__":
    unittest.main()
