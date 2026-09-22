from __future__ import annotations

import unittest
from pathlib import Path

from pathfinder.rsi_exam.offline_replay import (
    run_offline_replay_policy,
    verify_offline_replay_package,
)


FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "fixtures"
    / "rsi_exam_offline_replay"
    / "pathfinder-one-case-v1"
)


class OfflineReplayFixtureTest(unittest.TestCase):
    def test_committed_one_case_fixture_verifies_and_replays(self) -> None:
        receipt = verify_offline_replay_package(FIXTURE)
        self.assertEqual("VERIFIED_OFFLINE_REPLAY", receipt["status"])
        self.assertEqual(
            "461150e2690e5f4e540a1ec0d5a3b3f7b7913c5598459a8ec90240065039d009",
            receipt["package_sha256"],
        )
        replay = run_offline_replay_policy(
            FIXTURE,
            policy_name="always-indexed",
            mode="shared-dataset-sequence",
            query_count=10,
            seed=7,
        )
        self.assertEqual("COMPLETE", replay["status"])
        self.assertEqual(5_518_182, replay["metrics"]["total_source_bytes"])
        self.assertEqual(1, replay["metrics"]["index_builds"])
        self.assertEqual(1.0, replay["metrics"]["success_rate"])
        self.assertFalse(replay["external_calls_made"])


if __name__ == "__main__":
    unittest.main()
