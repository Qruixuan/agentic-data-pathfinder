from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.config import ConfigError, load_config
from pathfinder.experiment import run_pilot, run_session
from pathfinder.resolver import AccessResolver


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "minimal_system.json"


class MinimalSystemTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_config(CONFIG_PATH)

    def test_configuration_declares_expected_minimal_domain(self) -> None:
        self.assertEqual(4, len(self.config.representations))
        self.assertEqual(2, len(self.config.task_classes))
        self.assertEqual(4, len(self.config.designs))
        self.assertEqual(
            "minimal-video-v1",
            self.config.price_universe_version,
        )
        self.assertIn(
            8.0,
            self.config.price_universe("video_qa", "multimodal_digest"),
        )

    def test_resolver_separates_quote_from_physical_path(self) -> None:
        design = self.config.designs["D_structured_digest"]
        task = self.config.task_classes["video_qa"]
        resolver = AccessResolver(self.config)
        low_offers, _ = resolver.build_offers(
            design,
            task,
            self.config.quote_profiles["digest_low"],
        )
        high_offers, _ = resolver.build_offers(
            design,
            task,
            self.config.quote_profiles["digest_high"],
        )
        low_digest = next(
            offer
            for offer in low_offers
            if offer.representation_id == "multimodal_digest"
        )
        high_digest = next(
            offer
            for offer in high_offers
            if offer.representation_id == "multimodal_digest"
        )
        self.assertEqual(2.0, low_digest.quoted_price)
        self.assertTrue(low_digest.affordable)
        self.assertEqual(8.0, high_digest.quoted_price)
        self.assertFalse(high_digest.affordable)
        self.assertEqual(
            low_digest.expected_latency_ms,
            high_digest.expected_latency_ms,
        )
        self.assertEqual(low_digest.location, high_digest.location)

    def test_quote_intervention_changes_digest_access(self) -> None:
        low_accesses = 0
        high_accesses = 0
        for seed in range(100):
            low = run_session(
                self.config,
                "D_structured_digest",
                "video_qa",
                "digest_low",
                seed=seed,
            )
            high = run_session(
                self.config,
                "D_structured_digest",
                "video_qa",
                "digest_high",
                seed=seed,
            )
            low_accesses += (
                low.selected_representation_id == "multimodal_digest"
            )
            high_accesses += (
                high.selected_representation_id == "multimodal_digest"
            )
        self.assertGreater(low_accesses, 80)
        self.assertEqual(0, high_accesses)

    def test_latency_intervention_does_not_change_quote_or_choice(self) -> None:
        normal = run_session(
            self.config,
            "D_structured_digest",
            "video_qa",
            "digest_low",
            latency_multiplier=1.0,
            seed=42,
        )
        slow = run_session(
            self.config,
            "D_structured_digest",
            "video_qa",
            "digest_low",
            latency_multiplier=2.0,
            seed=42,
        )
        self.assertEqual(
            normal.selected_representation_id,
            slow.selected_representation_id,
        )
        self.assertEqual(
            [offer.quoted_price for offer in normal.offers],
            [offer.quoted_price for offer in slow.offers],
        )
        self.assertIsNotNone(normal.felt_latency_ms)
        self.assertIsNotNone(slow.felt_latency_ms)
        self.assertAlmostEqual(
            normal.felt_latency_ms * 2.0,
            slow.felt_latency_ms,
            places=6,
        )
        self.assertAlmostEqual(
            normal.realized_cost,
            slow.realized_cost,
            places=6,
        )

    def test_pilot_writes_replayable_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = run_pilot(
                self.config,
                temporary_directory,
                design_ids=["A_origin", "D_structured_digest"],
                task_class_ids=["video_qa"],
                quote_profile_ids=["digest_low", "digest_high"],
                latency_multipliers=[1.0],
                trials_per_cell=3,
            )
            self.assertEqual(12, result["session_count"])
            self.assertTrue(result["paired_seeds_across_cells"])
            sessions_path = Path(result["sessions_path"])
            summary_path = Path(result["summary_path"])
            manifest_path = Path(result["manifest_path"])
            self.assertTrue(sessions_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertTrue(manifest_path.exists())
            lines = sessions_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(12, len(lines))
            first = json.loads(lines[0])
            self.assertIn("offers", first)
            self.assertIn("felt_latency_ms", first)
            self.assertIn("realized_cost", first)
            self.assertIn("terminal_success", first)
            self.assertEqual(
                "minimal-video-v1",
                first["price_universe_version"],
            )
            seeds = {json.loads(line)["seed"] for line in lines}
            self.assertEqual({1000, 1001, 1002}, seeds)

    def test_config_rejects_quote_outside_predeclared_universe(self) -> None:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        raw["quote_profiles"][1]["overrides"]["video_qa"][
            "multimodal_digest"
        ] = 3
        with tempfile.TemporaryDirectory() as temporary_directory:
            invalid_path = Path(temporary_directory) / "invalid.json"
            invalid_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaises(ConfigError):
                load_config(invalid_path)


if __name__ == "__main__":
    unittest.main()
