from __future__ import annotations

import json
import unittest

from pathfinder.simulator.full_flow_semantic_input_profiles import (
    DERIVED_SPARSE_FRAMES_PROFILE_ID,
    DERIVED_SPARSE_FUSION_PROFILE_ID,
    INDEXED_WINDOW_PROFILE_ID,
    INDEXED_DERIVED_FUSION_PROFILE_ID,
    RAW_DIRECT_VIDEO_PROFILE_ID,
    SemanticInputProfileError,
    build_semantic_input_profile,
    profile_sha256,
    validate_semantic_input_profile,
)


class SemanticInputProfileTest(unittest.TestCase):
    def test_indexed_derived_requires_question_selected_frames_and_digest(
        self,
    ) -> None:
        profile = build_semantic_input_profile(
            route_family="indexed-derived",
            model_input_representation_ids=[
                "raw_video", "multimodal_digest",
            ],
            indexed_selection_kind="query-aware-temporal-index",
            indexed_frame_count=4,
            indexed_temporal_window_fraction=(0.25, 0.75),
        )
        self.assertEqual(
            INDEXED_DERIVED_FUSION_PROFILE_ID, profile["profile_id"]
        )
        self.assertEqual(
            "digest+indexed-frames-fusion", profile["input_mode"]
        )
        self.assertTrue(profile["digest_included"])
        self.assertEqual(4, profile["frame_selection"]["frame_count"])
        self.assertEqual(
            profile,
            validate_semantic_input_profile(
                profile,
                route_family="indexed-derived",
                model_input_representation_ids=[
                    "raw_video", "multimodal_digest",
                ],
            ),
        )
        with self.assertRaisesRegex(
            SemanticInputProfileError, "query-aware raw projection"
        ):
            build_semantic_input_profile(
                route_family="indexed-derived",
                model_input_representation_ids=["raw_video"],
                indexed_selection_kind="query-aware-temporal-index",
                indexed_frame_count=4,
                indexed_temporal_window_fraction=(0.25, 0.75),
            )

    def test_raw_and_indexed_profiles_freeze_different_visual_inputs(self) -> None:
        raw = build_semantic_input_profile(
            route_family="raw",
            model_input_representation_ids=["raw_video"],
        )
        indexed = build_semantic_input_profile(
            route_family="indexed-raw",
            model_input_representation_ids=["raw_video"],
        )
        # The raw family is the direct-video alternative: it delivers the
        # complete encoded object and freezes no sampling policy at all.
        self.assertEqual(RAW_DIRECT_VIDEO_PROFILE_ID, raw["profile_id"])
        self.assertEqual("direct-video", raw["input_mode"])
        self.assertIsNone(raw["frame_selection"])
        self.assertTrue(raw["direct_video_input"])
        self.assertEqual("complete-artifact", raw["source_byte_range_kind"])
        self.assertFalse(raw["source_byte_selectivity_claimed"])
        self.assertNotIn(
            "0.0",
            json.dumps(raw, sort_keys=True, separators=(",", ":")),
        )
        # Indexed raw must stay a selective temporal frame projection.
        self.assertEqual(INDEXED_WINDOW_PROFILE_ID, indexed["profile_id"])
        self.assertEqual("raw-prepared-frames", indexed["input_mode"])
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
        # Re-labelling a sampled frame policy as the raw family, or dropping
        # the direct-video claim, must both be rebuilt away rather than
        # trusted as recorded parameters.
        for tamper in (
            {"frame_selection": {
                "method": "uniform-midpoint",
                "frame_count": 24,
                "temporal_window_fraction": [0, 1],
            }},
            {"input_mode": "raw-prepared-frames"},
            {"direct_video_input": False},
            {"profile_id": "raw-dense-uniform-24-v1"},
        ):
            with self.subTest(tamper=sorted(tamper)[0]):
                tampered = {**profile, **tamper}
                with self.assertRaisesRegex(
                    SemanticInputProfileError,
                    "differs from its frozen route frontier",
                ):
                    validate_semantic_input_profile(
                        tampered,
                        route_family="raw",
                        model_input_representation_ids=["raw_video"],
                    )


if __name__ == "__main__":
    unittest.main()
