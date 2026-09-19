"""Focused coverage for the query-aware indexed semantic input profile."""

from __future__ import annotations

import unittest

from pathfinder.simulator.full_flow_semantic_input_profiles import (
    INDEXED_QUERY_AWARE_PROFILE_ID,
    INDEXED_WINDOW_PROFILE_ID,
    QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
    SemanticInputProfileError,
    build_semantic_input_profile,
)


def _query_aware(**overrides):
    kwargs = {
        "route_family": "indexed-raw",
        "model_input_representation_ids": ["raw_video"],
        "indexed_selection_kind": QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
        "indexed_frame_count": 10,
        "indexed_temporal_window_fraction": (0.4, 1.0),
    }
    kwargs.update(overrides)
    return build_semantic_input_profile(**kwargs)


class LegacyIndexedProfileTest(unittest.TestCase):
    def test_default_indexed_profile_is_unchanged(self) -> None:
        profile = build_semantic_input_profile(
            route_family="indexed-raw",
            model_input_representation_ids=["raw_video"],
        )
        self.assertEqual(INDEXED_WINDOW_PROFILE_ID, profile["profile_id"])
        self.assertEqual(
            {"method": "uniform-midpoint", "frame_count": 8,
             "temporal_window_fraction": [0.25, 0.75]},
            profile["frame_selection"],
        )

    def test_fixed_profile_refuses_a_derived_selection(self) -> None:
        with self.assertRaises(SemanticInputProfileError):
            build_semantic_input_profile(
                route_family="indexed-raw",
                model_input_representation_ids=["raw_video"],
                indexed_frame_count=10,
            )


class QueryAwareProfileTest(unittest.TestCase):
    def test_profile_id_is_new_and_not_the_legacy_one(self) -> None:
        profile = _query_aware()
        self.assertEqual(INDEXED_QUERY_AWARE_PROFILE_ID, profile["profile_id"])
        self.assertNotEqual(INDEXED_WINDOW_PROFILE_ID, profile["profile_id"])

    def test_selection_is_named_as_index_derived(self) -> None:
        selection = _query_aware()["frame_selection"]
        self.assertEqual("temporal-index-selected-interval", selection["method"])
        self.assertEqual(10, selection["frame_count"])
        self.assertEqual([0.4, 1.0], selection["temporal_window_fraction"])

    def test_it_never_claims_direct_video(self) -> None:
        profile = _query_aware()
        self.assertFalse(profile["direct_video_input"])
        self.assertEqual("raw-prepared-frames", profile["input_mode"])
        self.assertEqual("source-decoded-temporal-frame-bundle",
                         profile["source_byte_range_kind"])

    def test_only_the_indexed_family_has_a_query_aware_projection(self) -> None:
        with self.assertRaisesRegex(SemanticInputProfileError, "indexed-raw"):
            _query_aware(route_family="raw",
                         model_input_representation_ids=["raw_video"])

    def test_frame_count_and_interval_are_required(self) -> None:
        with self.assertRaises(SemanticInputProfileError):
            _query_aware(indexed_frame_count=None)
        with self.assertRaises(SemanticInputProfileError):
            _query_aware(indexed_temporal_window_fraction=None)

    def test_inverted_or_out_of_range_intervals_are_refused(self) -> None:
        for bad in ((1.0, 0.4), (-0.1, 1.0), (0.4, 1.1), (0.5, 0.5)):
            with self.subTest(interval=bad):
                with self.assertRaises(SemanticInputProfileError):
                    _query_aware(indexed_temporal_window_fraction=bad)

    def test_d1_and_d5_share_one_profile(self) -> None:
        # Both designs build the profile from the same plan; only placement
        # and execution differ between them.
        self.assertEqual(_query_aware(), _query_aware())


if __name__ == "__main__":
    unittest.main()
