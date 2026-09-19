"""Focused coverage for the query-aware N3 temporal selection policy.

v1's fixed middle-window projection must keep producing a byte-identical
policy document, so already-frozen N3 packages stay verifiable.
"""

from __future__ import annotations

import unittest

from pathfinder.simulator.n3_indexed_data_plane import (
    N3IndexedDataPlaneError,
    N3TemporalSelectionPolicy,
    TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
    UNIFORM_MIDPOINT_SAMPLING_METHOD,
)

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
    "temporal_index_package_sha256": "21094b338d01" + "0" * 52,
}


def _query_aware(**overrides):
    kwargs = {
        "frame_count": 10,
        "temporal_start_fraction": 0.4,
        "temporal_end_fraction": 1.0,
        "sampling_method": TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
        "selection_provenance": PROVENANCE,
    }
    kwargs.update(overrides)
    return N3TemporalSelectionPolicy(**kwargs)


class LegacyPolicyIsUnchangedTest(unittest.TestCase):
    def test_default_policy_document_is_byte_stable(self) -> None:
        self.assertEqual(
            {
                "selection_semantics": "source-decoded-temporal-frame-bundle",
                "sampling_method": "uniform-midpoint-temporal-window",
                "frame_count": 8,
                "jpeg_max_dimension": 768,
                "temporal_window_fraction": [0.25, 0.75],
                "partial_mp4_byte_range_claimed": False,
                "source_side_projection_executed": True,
            },
            N3TemporalSelectionPolicy().to_dict(),
        )

    def test_default_policy_carries_no_query_aware_marker(self) -> None:
        document = N3TemporalSelectionPolicy().to_dict()
        self.assertNotIn("query_aware_selection", document)
        self.assertNotIn("temporal_index_selection", document)
        self.assertEqual(UNIFORM_MIDPOINT_SAMPLING_METHOD,
                         document["sampling_method"])


class QueryAwarePolicyTest(unittest.TestCase):
    def test_selection_provenance_is_frozen_into_the_document(self) -> None:
        document = _query_aware().to_dict()
        self.assertTrue(document["query_aware_selection"])
        self.assertEqual(TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
                         document["sampling_method"])
        self.assertEqual(PROVENANCE, document["temporal_index_selection"])
        self.assertEqual([0.4, 1.0], document["temporal_window_fraction"])
        self.assertEqual(10, document["frame_count"])

    def test_no_mp4_byte_range_is_ever_claimed(self) -> None:
        # The wire path decodes frames source-side; it does not range-read MP4.
        for policy in (N3TemporalSelectionPolicy(), _query_aware()):
            with self.subTest(method=policy.sampling_method):
                document = policy.to_dict()
                self.assertFalse(document["partial_mp4_byte_range_claimed"])
                self.assertTrue(document["source_side_projection_executed"])

    def test_incomplete_provenance_is_refused(self) -> None:
        for missing in sorted(PROVENANCE):
            partial = {k: v for k, v in PROVENANCE.items() if k != missing}
            with self.subTest(missing=missing):
                with self.assertRaises(N3IndexedDataPlaneError):
                    _query_aware(selection_provenance=partial)

    def test_fallback_selection_is_refused(self) -> None:
        with self.assertRaisesRegex(N3IndexedDataPlaneError, "fallback"):
            _query_aware(selection_provenance=dict(PROVENANCE, fallback_used=True))

    def test_query_aware_method_requires_provenance(self) -> None:
        with self.assertRaises(N3IndexedDataPlaneError):
            _query_aware(selection_provenance=None)

    def test_fixed_window_policy_cannot_carry_provenance(self) -> None:
        with self.assertRaisesRegex(N3IndexedDataPlaneError, "fixed-window"):
            N3TemporalSelectionPolicy(selection_provenance=PROVENANCE)

    def test_unsupported_sampling_method_is_refused(self) -> None:
        with self.assertRaisesRegex(N3IndexedDataPlaneError, "sampling method"):
            _query_aware(sampling_method="fixed-late-window")

    def test_selected_interval_matches_the_frozen_v2_decision(self) -> None:
        # The frozen pre-outcome decision: 6.240-15.599s of a 15.598917s object.
        duration = 15.598917
        start, end = PROVENANCE["merged_intervals_seconds"][0]
        document = _query_aware().to_dict()
        self.assertAlmostEqual(start / duration,
                               document["temporal_window_fraction"][0], places=6)
        self.assertAlmostEqual(end / duration,
                               document["temporal_window_fraction"][1], places=6)


if __name__ == "__main__":
    unittest.main()
