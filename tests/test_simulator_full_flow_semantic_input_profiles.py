from __future__ import annotations

import unittest

from pathfinder.simulator.full_flow_semantic_input_profiles import (
    DERIVED_SPARSE_FRAMES_PROFILE_ID,
    DERIVED_SPARSE_FUSION_PROFILE_ID,
    INDEXED_WINDOW_PROFILE_ID,
    RAW_DENSE_PROFILE_ID,
    SemanticInputProfileError,
    build_semantic_input_profile,
    profile_sha256,
    validate_semantic_input_profile,
)


class SemanticInputProfileTest(unittest.TestCase):
    def test_raw_and_indexed_profiles_freeze_different_visual_inputs(self) -> None:
        raw = build_semantic_input_profile(
            route_family="raw",
            model_input_representation_ids=["raw_video"],
        )
        indexed = build_semantic_input_profile(
            route_family="indexed-raw",
            model_input_representation_ids=["raw_video"],
        )
        self.assertEqual(RAW_DENSE_PROFILE_ID, raw["profile_id"])
        self.assertEqual(24, raw["frame_selection"]["frame_count"])
        self.assertEqual(
            [0.0, 1.0], raw["frame_selection"]["temporal_window_fraction"]
        )
        self.assertEqual(INDEXED_WINDOW_PROFILE_ID, indexed["profile_id"])
        self.assertEqual(8, indexed["frame_selection"]["frame_count"])
        self.assertEqual(
            [0.25, 0.75],
            indexed["frame_selection"]["temporal_window_fraction"],
        )
        self.assertEqual(
            "source-decoded-temporal-frame-bundle",
            indexed["source_byte_range_kind"],
        )
        self.assertTrue(indexed["source_byte_selectivity_claimed"])
        self.assertFalse(indexed["direct_video_input"])
        self.assertNotEqual(profile_sha256(raw), profile_sha256(indexed))

    def test_derived_profile_distinguishes_frames_from_fusion(self) -> None:
        frames = build_semantic_input_profile(
            route_family="remote-derived",
            model_input_representation_ids=["sampled_frame_bundle"],
        )
        fusion = build_semantic_input_profile(
            route_family="local-cache-derived",
            model_input_representation_ids=[
                "sampled_frame_bundle",
                "multimodal_digest",
            ],
        )
        self.assertEqual(DERIVED_SPARSE_FRAMES_PROFILE_ID, frames["profile_id"])
        self.assertFalse(frames["digest_included"])
        self.assertEqual(4, frames["frame_selection"]["frame_count"])
        self.assertEqual(DERIVED_SPARSE_FUSION_PROFILE_ID, fusion["profile_id"])
        self.assertTrue(fusion["digest_included"])
        self.assertEqual(4, fusion["frame_selection"]["frame_count"])

    def test_validation_rederives_instead_of_trusting_parameters(self) -> None:
        profile = build_semantic_input_profile(
            route_family="raw",
            model_input_representation_ids=["raw_video"],
        )
        profile["frame_selection"]["frame_count"] = 8
        with self.assertRaisesRegex(
            SemanticInputProfileError,
            "differs from its frozen route frontier",
        ):
            validate_semantic_input_profile(
                profile,
                route_family="raw",
                model_input_representation_ids=["raw_video"],
            )


if __name__ == "__main__":
    unittest.main()
