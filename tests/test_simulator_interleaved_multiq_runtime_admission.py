"""The interleaved runtime admission is frozen, exact and never self-submits."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pathfinder.simulator.interleaved_multiq_runtime_admission import (
    InterleavedRuntimeAdmissionError,
    _cache_episode_matches,
    freeze_interleaved_runtime_admission,
    verify_interleaved_runtime_admission,
)


class InterleavedRuntimeAdmissionTests(unittest.TestCase):
    def test_cache_pair_binds_arm_for_legacy_and_ten_route_designs(self):
        cache = {("run", "trial"): "episode"}
        for design in ("DC", "D3", "D7"):
            route = {"design_id": design, "arm_id": "DC",
                     "run_id": "run", "trial_key": "trial",
                     "cache_episode_id": "episode"}
            self.assertTrue(_cache_episode_matches(route, cache))
            self.assertFalse(_cache_episode_matches(
                {**route, "cache_episode_id": "wrong"}, cache,
            ))
        self.assertTrue(_cache_episode_matches({
            "design_id": "D2", "arm_id": "D", "run_id": "run",
            "trial_key": "trial", "cache_episode_id": None,
        }, cache))
        self.assertFalse(_cache_episode_matches({
            "design_id": "D2", "arm_id": "D", "run_id": "run",
            "trial_key": "trial", "cache_episode_id": "episode",
        }, cache))

    @staticmethod
    def _documents(**_sources):
        return {
            "interleaved-runtime-admission.json": (
                b'{"admission_sha256":"' + b"a" * 64 + b'",'
                b'"trial_count":24,"stage_count":276,'
                b'"index_query_plan_count":6,'
                b'"data_agent_plan_binding_count":42,'
                b'"cache_episode_binding_count":6}'
            ),
            "admitted-trials.jsonl": b'{"trial_key":"trial-1"}\n',
            "admitted-stages.jsonl": b'{"stage_key":"stage-1"}\n',
            "index-query-plans.jsonl": b'{"query_id":"query-1"}\n',
            "data-agent-plan-bindings.jsonl": b'{"plan_id":"plan-1"}\n',
            "cache-episode-bindings.jsonl": b'{"run_id":"run-1"}\n',
        }

    def test_freeze_verify_rejects_reuse_and_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "admission"
            with patch(
                "pathfinder.simulator.interleaved_multiq_runtime_admission."
                "_expected", side_effect=self._documents,
            ):
                report = freeze_interleaved_runtime_admission(
                    output_dir=output,
                )
                self.assertEqual(report["trial_count"], 24)
                self.assertEqual(report["index_query_plan_count"], 6)
                self.assertFalse(report["workflow_submitted"])
                with self.assertRaises(InterleavedRuntimeAdmissionError):
                    freeze_interleaved_runtime_admission(output_dir=output)
                (output / "admitted-trials.jsonl").write_bytes(b"changed")
                with self.assertRaises(InterleavedRuntimeAdmissionError):
                    verify_interleaved_runtime_admission(output)


if __name__ == "__main__":
    unittest.main()
