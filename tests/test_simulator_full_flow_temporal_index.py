"""Focused coverage for the query-aware temporal index.

Every fixture here is synthetic. No real object ID, workload ID, question,
timestamp or answer from the visible development set appears, so passing these
tests cannot depend on the observed demo outcome.
"""

from __future__ import annotations

import hashlib
import json
import unittest

from pathfinder.simulator.full_flow_temporal_index import (
    TEMPORAL_SELECTION_SCHEMA_VERSION,
    TemporalIndexError,
    TemporalSegment,
    anchor_query_tokens,
    build_segments,
    detect_relation,
    expand_relation,
    parse_question_independent_timeline,
    rank_segments,
    search_temporal_index,
    verify_selection,
)

INDEX_SHA = "a" * 64


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _segment(obj: str, ordinal: int, start: float, end: float, caption: str) -> TemporalSegment:
    return TemporalSegment(
        segment_id=f"{obj}#seg{ordinal:02d}",
        ordinal=ordinal,
        object_id=obj,
        start_seconds=start,
        end_seconds=end,
        caption=caption,
        source_video_sha256=_sha(f"source-{obj}"),
        source_video_size_bytes=1000 + ordinal,
        projection_sha256=_sha(f"proj-{obj}-{ordinal}"),
        projection_size_bytes=500 + ordinal,
        projection_frame_count=3,
        projection_frame_timestamps=(start, (start + end) / 2, end),
    )


# Two synthetic videos with deliberately different content.
VIDEO_A = (
    _segment("obj-alpha", 0, 0.0, 10.0, "a dog sleeps on a rug in a quiet room"),
    _segment("obj-alpha", 1, 10.0, 20.0, "a person opens the front door holding keys"),
    _segment("obj-alpha", 2, 20.0, 30.0, "the dog jumps onto the sofa and barks"),
    _segment("obj-alpha", 3, 30.0, 40.0, "the person sits and reads a newspaper"),
)
VIDEO_B = (
    _segment("obj-beta", 0, 0.0, 12.0, "a person opens the front door holding keys"),
    _segment("obj-beta", 1, 12.0, 24.0, "a cyclist rides past a brick wall"),
    _segment("obj-beta", 2, 24.0, 36.0, "the cyclist stops and points forward"),
)


class SegmentConstructionTest(unittest.TestCase):
    DIGEST = (
        "PATHFINDER QUESTION-INDEPENDENT MULTIMODAL DIGEST\n"
        "Object: obj-synthetic\n"
        "Timeline:\n"
        "- [0.000s-5.000s] first described scene\n"
        "- [5.000s-9.000s] second described scene\n"
        "- [9.000s] trailing open ended scene\n"
        "Summary:\nsomething\n"
    )

    @staticmethod
    def _projection(start, end):
        return (_sha(f"p{start}-{end}"), 100, 2, (start, end))

    def test_multi_segment_construction_is_deterministic(self) -> None:
        first = build_segments(
            object_id="obj-synthetic", digest_text=self.DIGEST, duration_seconds=12.0,
            source_video_sha256=_sha("src"), source_video_size_bytes=999,
            projection_for_window=self._projection,
        )
        again = build_segments(
            object_id="obj-synthetic", digest_text=self.DIGEST, duration_seconds=12.0,
            source_video_sha256=_sha("src"), source_video_size_bytes=999,
            projection_for_window=self._projection,
        )
        self.assertEqual(3, len(first))
        self.assertEqual(
            [s.to_public_dict() for s in first], [s.to_public_dict() for s in again]
        )
        # The trailing open-ended caption runs to the end of the video.
        self.assertEqual(12.0, first[-1].end_seconds)
        self.assertEqual(
            ["obj-synthetic#seg00", "obj-synthetic#seg01", "obj-synthetic#seg02"],
            [s.segment_id for s in first],
        )

    def test_index_contents_are_question_independent(self) -> None:
        segments = build_segments(
            object_id="obj-synthetic", digest_text=self.DIGEST, duration_seconds=12.0,
            source_video_sha256=_sha("src"), source_video_size_bytes=999,
            projection_for_window=self._projection,
        )
        # Building the index takes no question argument at all, and nothing in
        # the frozen output mentions one.
        blob = json.dumps([s.to_public_dict() for s in segments])
        for probe in ("question", "answer", "option", "correct"):
            self.assertNotIn(probe, blob.lower())

    def test_digest_must_declare_question_independence(self) -> None:
        with self.assertRaisesRegex(TemporalIndexError, "question-independent"):
            parse_question_independent_timeline("Timeline:\n- [0.0s-1.0s] x\n- [1.0s] y\n")

    def test_single_segment_object_is_refused(self) -> None:
        with self.assertRaisesRegex(TemporalIndexError, "at least two segments"):
            parse_question_independent_timeline(
                "PATHFINDER QUESTION-INDEPENDENT MULTIMODAL DIGEST\n"
                "Timeline:\n- [0.000s-5.000s] only one scene\n"
            )


class RelationDetectionTest(unittest.TestCase):
    def test_generic_relations(self) -> None:
        cases = {
            "what did the animal do after it entered": "following",
            "what happened before the vehicle stopped": "preceding",
            "what is the person doing while standing": "during",
            "what occurs at the start of the clip": "start",
            "what happens at the end of the clip": "end",
            "what colour is the object": "none",
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(expected, detect_relation(question))

    def test_anchor_clause_keeps_the_event_side_of_the_relation(self) -> None:
        from pathfinder.simulator.full_flow_temporal_index import anchor_clause

        cases = {
            "what did the animal do after it entered the room":
                "it entered the room",
            "what happened before the vehicle stopped moving":
                "the vehicle stopped moving",
            "what is the person doing while standing by the door":
                "standing by the door",
            # No relation: the whole question is the anchor clause.
            "what colour is the large object":
                "what colour is the large object",
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(expected, anchor_clause(question))

    def test_cue_inside_a_longer_word_is_not_a_relation(self) -> None:
        from pathfinder.simulator.full_flow_temporal_index import anchor_clause

        # "rafters" contains "after"; a missing word boundary would split here.
        question = "the rafters collapsed onto the floor"
        self.assertEqual("none", detect_relation(question))
        self.assertEqual(question, anchor_clause(question))

    def test_cue_with_no_usable_event_clause_falls_back_to_the_question(self) -> None:
        from pathfinder.simulator.full_flow_temporal_index import anchor_clause

        self.assertEqual("what happened after", anchor_clause("what happened after"))

    def test_relation_words_do_not_bias_anchor_matching(self) -> None:
        tokens = anchor_query_tokens("what did the dog do after it barks")
        self.assertNotIn("after", tokens)
        self.assertNotIn("what", tokens)
        self.assertIn("dog", tokens)
        self.assertIn("barks", tokens)


class AnchorRetrievalTest(unittest.TestCase):
    def test_anchor_is_retrieved_from_segment_content(self) -> None:
        ranked = rank_segments("what did the dog do after it jumps onto the sofa", VIDEO_A)
        self.assertEqual("obj-alpha#seg02", ranked[0].segment_id)
        self.assertGreater(ranked[0].score_milli, 0)
        self.assertEqual(len(VIDEO_A), len(ranked))

    def test_different_questions_select_different_segments(self) -> None:
        door = search_temporal_index(
            question="what did the person do while opening the front door",
            object_id="obj-alpha", segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
        )
        sofa = search_temporal_index(
            question="what did the dog do while it jumps onto the sofa",
            object_id="obj-alpha", segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
        )
        self.assertEqual("obj-alpha#seg01", door.anchor_segment_id)
        self.assertEqual("obj-alpha#seg02", sofa.anchor_segment_id)
        self.assertNotEqual(
            [s.segment_id for s in door.selected_segments],
            [s.segment_id for s in sofa.selected_segments],
        )

    def test_same_question_selects_different_segments_per_video(self) -> None:
        question = "what happened after the person opened the front door"
        a = search_temporal_index(
            question=question, object_id="obj-alpha", segments=VIDEO_A,
            temporal_index_sha256=INDEX_SHA,
        )
        b = search_temporal_index(
            question=question, object_id="obj-beta", segments=VIDEO_B,
            temporal_index_sha256=INDEX_SHA,
        )
        # Same query, different videos, different anchors and windows.
        self.assertEqual("obj-alpha#seg01", a.anchor_segment_id)
        self.assertEqual("obj-beta#seg00", b.anchor_segment_id)
        self.assertNotEqual(a.selected_timestamp_range, b.selected_timestamp_range)

    def test_deterministic_tie_breaking(self) -> None:
        tied = (
            _segment("obj-tie", 0, 0.0, 5.0, "identical wording here"),
            _segment("obj-tie", 1, 5.0, 10.0, "identical wording here"),
        )
        for _ in range(5):
            ranked = rank_segments("what about identical wording", tied)
            self.assertEqual("obj-tie#seg00", ranked[0].segment_id)


class RelationExpansionTest(unittest.TestCase):
    def test_after_selects_following_segments_not_a_fixed_window(self) -> None:
        result = search_temporal_index(
            question="what did the dog do after it jumps onto the sofa",
            object_id="obj-alpha", segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
        )
        self.assertEqual("following", result.relation)
        self.assertEqual("obj-alpha#seg02", result.anchor_segment_id)
        self.assertEqual(["obj-alpha#seg03"], [s.segment_id for s in result.selected_segments])
        # Strictly after the anchor, and not the globally fixed middle window.
        self.assertGreaterEqual(result.selected_timestamp_range[0], 30.0)

    def test_before_selects_preceding_segments(self) -> None:
        result = search_temporal_index(
            question="what happened before the dog jumps onto the sofa",
            object_id="obj-alpha", segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
            max_selected_segments=1,
        )
        self.assertEqual("preceding", result.relation)
        self.assertEqual(["obj-alpha#seg01"], [s.segment_id for s in result.selected_segments])

    def test_during_start_end_and_none(self) -> None:
        during = expand_relation("during", 2, 4, max_selected_segments=2)
        self.assertEqual((2,), during)
        self.assertEqual((0, 1), expand_relation("start", 3, 4, max_selected_segments=2))
        self.assertEqual((2, 3), expand_relation("end", 0, 4, max_selected_segments=2))
        self.assertEqual((1, 2), expand_relation("none", 2, 4, max_selected_segments=2))

    def test_anchor_at_boundary_still_yields_non_empty_selection(self) -> None:
        self.assertEqual((3,), expand_relation("following", 3, 4, max_selected_segments=2))
        self.assertEqual((0,), expand_relation("preceding", 0, 4, max_selected_segments=2))

    def test_selection_is_bounded_ordered_and_deduplicated(self) -> None:
        result = search_temporal_index(
            question="what happened after the dog sleeps on a rug",
            object_id="obj-alpha", segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
            max_selected_segments=2,
        )
        ordinals = [s.ordinal for s in result.selected_segments]
        self.assertLessEqual(len(ordinals), 2)
        self.assertEqual(sorted(set(ordinals)), ordinals)


class BindingAndEvidenceTest(unittest.TestCase):
    def _selection(self):
        return search_temporal_index(
            question="what did the dog do after it jumps onto the sofa",
            object_id="obj-alpha", segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
        )

    def test_evidence_binds_source_and_projection_identities(self) -> None:
        ev = self._selection().to_public_evidence()
        self.assertEqual(TEMPORAL_SELECTION_SCHEMA_VERSION, ev["schema_version"])
        self.assertEqual(len(VIDEO_A), ev["candidate_segment_count"])
        self.assertGreater(len(ev["ranked_segments"]), 1)
        for seg in ev["selected_segments"]:
            self.assertEqual(_sha("source-obj-alpha"), seg["source_video_sha256"])
            self.assertTrue(seg["projection_sha256"])
            self.assertGreater(seg["projection_frame_count"], 0)
        self.assertFalse(ev["fixed_window_fallback_used"])
        self.assertTrue(ev["query_aware_selection"])

    def test_verifier_accepts_honest_evidence(self) -> None:
        ev = self._selection().to_public_evidence()
        report = verify_selection(
            ev, question="what did the dog do after it jumps onto the sofa",
            segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
        )
        self.assertEqual("VERIFIED", report["status"])

    def test_verifier_rejects_tampered_evidence(self) -> None:
        question = "what did the dog do after it jumps onto the sofa"
        base = self._selection().to_public_evidence()
        tampers = {
            "forged segment": {"selected_segment_ids": ["obj-alpha#seg00"]},
            "reordered ranking": {"ranked_segments": list(reversed(base["ranked_segments"]))},
            "omitted candidates": {"candidate_segment_count": 1},
            "relabelled relation": {"relation": "preceding"},
            "moved anchor": {"anchor_segment_id": "obj-alpha#seg00"},
            "inflated bytes": {"total_selected_artifact_bytes": 99999},
        }
        for label, patch in tampers.items():
            with self.subTest(tamper=label):
                with self.assertRaises(TemporalIndexError):
                    verify_selection(
                        {**base, **patch}, question=question,
                        segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
                    )

    def test_fixed_window_fallback_cannot_pose_as_query_aware(self) -> None:
        ev = {**self._selection().to_public_evidence(), "fixed_window_fallback_used": True}
        with self.assertRaisesRegex(TemporalIndexError, "query-aware"):
            verify_selection(
                ev, question="what did the dog do after it jumps onto the sofa",
                segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
            )

    def test_single_candidate_shortcut_is_refused(self) -> None:
        with self.assertRaisesRegex(TemporalIndexError, "at least two candidate"):
            search_temporal_index(
                question="anything at all", object_id="obj-alpha",
                segments=VIDEO_A[:1], temporal_index_sha256=INDEX_SHA,
            )

    def test_no_hidden_label_or_credential_can_enter_evidence(self) -> None:
        evidence = self._selection().to_public_evidence()
        # The two safety declarations are expected; what must never appear is a
        # field that could *carry* a label or credential value.
        self.assertIs(False, evidence["hidden_label_values_included"])
        self.assertIs(False, evidence["credentials_recorded"])
        forbidden_keys = {
            "correct_answer_id", "hidden_label", "hidden_labels", "answer",
            "token", "bearer_token", "secret", "api_key", "authorization",
            "password", "signed_url",
        }

        def walk(node):
            if isinstance(node, dict):
                for key, value in node.items():
                    self.assertNotIn(str(key).casefold(), forbidden_keys)
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(evidence)
        # Captions are retained only as digests, never as text.
        blob = json.dumps(evidence)
        for segment in VIDEO_A:
            self.assertNotIn(segment.caption, blob)


class FixedWindowRegressionTest(unittest.TestCase):
    """A fixed-window implementation must not be able to pass these tests."""

    def test_fixed_middle_window_would_fail_after_relation(self) -> None:
        result = search_temporal_index(
            question="what did the person do after reading a newspaper",
            object_id="obj-alpha", segments=VIDEO_A, temporal_index_sha256=INDEX_SHA,
        )
        # A constant 25%-75% window over 0-40s would be 10s-30s (seg01..seg02).
        fixed_window_ids = {"obj-alpha#seg01", "obj-alpha#seg02"}
        selected = {s.segment_id for s in result.selected_segments}
        self.assertNotEqual(fixed_window_ids, selected)

    def test_selection_varies_with_query_which_a_fixed_window_cannot_do(self) -> None:
        seen = set()
        for question in (
            "what happened after the dog sleeps on a rug",
            "what happened after the person opens the front door",
            "what happened before the person sits and reads",
            "what is shown at the start",
            "what is shown at the end",
        ):
            result = search_temporal_index(
                question=question, object_id="obj-alpha", segments=VIDEO_A,
                temporal_index_sha256=INDEX_SHA, max_selected_segments=1,
            )
            seen.add(tuple(s.segment_id for s in result.selected_segments))
        # A fixed window would yield exactly one distinct selection.
        self.assertGreater(len(seen), 1)


class NoHardCodingTest(unittest.TestCase):
    def test_module_contains_no_visible_set_identifiers(self) -> None:
        from pathlib import Path

        import pathfinder.simulator.full_flow_temporal_index as module

        source = Path(module.__file__).read_text(encoding="utf-8")
        for forbidden in (
            "nextqa", "3429509208", "2435100235", "2461993294", "4010069381",
            "smoke-temporal", "smoke-causal", "baby", "camera", "vacuum",
            "0.25", "0.75",
        ):
            self.assertNotIn(forbidden, source, f"hard-coded token {forbidden!r}")

    def test_source_contains_no_stray_control_characters(self) -> None:
        """A control byte inside a regex silently disables it; catch it here."""

        from pathlib import Path

        import pathfinder.simulator.full_flow_temporal_index as module

        raw = Path(module.__file__).read_bytes()
        stray = {
            byte for byte in raw
            if byte < 0x20 and byte not in (0x09, 0x0A, 0x0D)
        }
        self.assertEqual(set(), stray, f"stray control bytes: {sorted(stray)}")


if __name__ == "__main__":
    unittest.main()
