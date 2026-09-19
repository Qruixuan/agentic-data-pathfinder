"""Focused coverage for validating an already-frozen query-aware profile.

A query-aware profile is derived from the N3 package that produced the
projection.  Validation must rebuild it rather than accept it, and every
fixed-window profile must keep validating exactly as before.
"""

from __future__ import annotations

import unittest

from pathfinder.simulator.full_flow_semantic_input_profiles import (
    INDEXED_QUERY_AWARE_PROFILE_ID,
    INDEXED_WINDOW_PROFILE_ID,
    QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
    SemanticInputProfileError,
    build_semantic_input_profile,
    indexed_selection_from_profile,
    validate_semantic_input_profile,
)

SELECTION = {
    "indexed_selection_kind": QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
    "indexed_frame_count": 10,
    "indexed_temporal_window_fraction": (0.4, 1.0),
}


def _query_aware():
    return build_semantic_input_profile(
        route_family="indexed-raw",
        model_input_representation_ids=["raw_video"],
        **SELECTION,
    )


def _fixed():
    return build_semantic_input_profile(
        route_family="indexed-raw",
        model_input_representation_ids=["raw_video"],
    )


class SelectionRecoveryTest(unittest.TestCase):
    def test_a_query_aware_profile_declares_its_own_selection(self) -> None:
        self.assertEqual(SELECTION, indexed_selection_from_profile(_query_aware()))

    def test_fixed_window_and_frameless_profiles_declare_nothing(self) -> None:
        raw = build_semantic_input_profile(
            route_family="raw", model_input_representation_ids=["raw_video"])
        for profile in (_fixed(), raw):
            with self.subTest(profile=profile["profile_id"]):
                self.assertIsNone(indexed_selection_from_profile(profile))


class ValidationTest(unittest.TestCase):
    def test_a_query_aware_profile_validates_without_an_authority(self) -> None:
        profile = _query_aware()
        self.assertEqual(
            profile,
            validate_semantic_input_profile(
                profile, route_family="indexed-raw",
                model_input_representation_ids=["raw_video"]),
        )

    def test_the_authority_overrides_what_the_profile_declares(self) -> None:
        with self.assertRaises(SemanticInputProfileError):
            validate_semantic_input_profile(
                _query_aware(), route_family="indexed-raw",
                model_input_representation_ids=["raw_video"],
                indexed_selection=dict(SELECTION, indexed_frame_count=8),
            )

    def test_a_fixed_window_authority_refuses_a_query_aware_profile(self) -> None:
        with self.assertRaises(SemanticInputProfileError):
            validate_semantic_input_profile(
                _query_aware(), route_family="indexed-raw",
                model_input_representation_ids=["raw_video"],
                indexed_selection={},
            )

    def test_a_tampered_interval_is_refused(self) -> None:
        profile = _query_aware()
        profile["frame_selection"]["temporal_window_fraction"] = [0.1, 1.0]
        with self.assertRaises(SemanticInputProfileError):
            validate_semantic_input_profile(
                profile, route_family="indexed-raw",
                model_input_representation_ids=["raw_video"],
                indexed_selection=SELECTION,
            )

    def test_only_indexed_raw_may_carry_a_query_aware_selection(self) -> None:
        profile = _query_aware()
        with self.assertRaisesRegex(SemanticInputProfileError, "indexed-raw"):
            validate_semantic_input_profile(
                profile, route_family="raw",
                model_input_representation_ids=["raw_video"])

    def test_legacy_profiles_validate_exactly_as_before(self) -> None:
        for family, reps in (
            ("raw", ["raw_video"]),
            ("indexed-raw", ["raw_video"]),
            ("remote-derived", ["sampled_frame_bundle"]),
        ):
            with self.subTest(route_family=family):
                profile = build_semantic_input_profile(
                    route_family=family, model_input_representation_ids=reps)
                self.assertEqual(
                    profile,
                    validate_semantic_input_profile(
                        profile, route_family=family,
                        model_input_representation_ids=reps),
                )

    def test_a_whole_number_bound_survives_a_json_boundary(self) -> None:
        # A signature is taken over these bytes and the profile then crosses a
        # process boundary.  1.0 and 1 are the same number but not the same
        # bytes, and encoders that drop the redundant fraction would break the
        # signature, so a whole bound is written as an int here.
        import json

        selection = _query_aware()["frame_selection"]
        self.assertEqual([0.4, 1], selection["temporal_window_fraction"])
        self.assertIsInstance(selection["temporal_window_fraction"][1], int)
        encoded = json.dumps(selection, sort_keys=True, separators=(",", ":"))
        self.assertNotIn("1.0", encoded)
        self.assertEqual(selection, json.loads(encoded))

    def test_a_fractional_bound_is_left_alone(self) -> None:
        profile = build_semantic_input_profile(
            route_family="indexed-raw",
            model_input_representation_ids=["raw_video"],
            indexed_selection_kind=QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
            indexed_frame_count=10,
            indexed_temporal_window_fraction=(0.25, 0.75),
        )
        self.assertEqual([0.25, 0.75],
                         profile["frame_selection"]["temporal_window_fraction"])

    def test_the_normalised_profile_still_validates(self) -> None:
        profile = _query_aware()
        self.assertEqual(
            profile,
            validate_semantic_input_profile(
                profile, route_family="indexed-raw",
                model_input_representation_ids=["raw_video"],
                indexed_selection=SELECTION),
        )

    def test_the_two_indexed_profiles_stay_distinct(self) -> None:
        self.assertEqual(INDEXED_WINDOW_PROFILE_ID, _fixed()["profile_id"])
        self.assertEqual(
            INDEXED_QUERY_AWARE_PROFILE_ID, _query_aware()["profile_id"])


if __name__ == "__main__":
    unittest.main()
