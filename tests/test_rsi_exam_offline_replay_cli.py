from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from pathfinder.cli import main as cli_main


class OfflineReplayCliTest(unittest.TestCase):
    def _invoke(self, arguments: list[str]) -> tuple[int, dict]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        lines = stdout.getvalue().splitlines()
        self.assertEqual(1, len(lines), stdout.getvalue())
        return status, json.loads(lines[0])

    def test_build_and_verify_commands_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.rsi_exam.offline_replay.build_offline_replay_package",
            return_value={"status": "FROZEN_OFFLINE_REPLAY"},
        ) as build:
            status, payload = self._invoke([
                "build-rsi-exam-offline-replay",
                "--accounting-dir",
                "accounting-a",
                "--accounting-dir",
                "accounting-b",
                "--source-commit",
                "5" * 40,
                "--builder-commit",
                "6" * 40,
                "--package-id",
                "replay-v1",
                "--output-dir",
                "replay",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN_OFFLINE_REPLAY", payload["status"])
        build.assert_called_once_with(
            [Path("accounting-a"), Path("accounting-b")],
            output_dir=Path("replay"),
            source_commit="5" * 40,
            builder_commit="6" * 40,
            package_id="replay-v1",
            split_manifest=None,
        )

        with mock.patch(
            "pathfinder.rsi_exam.offline_replay.verify_offline_replay_package",
            return_value={"status": "VERIFIED_OFFLINE_REPLAY"},
        ) as verify:
            status, payload = self._invoke([
                "verify-rsi-exam-offline-replay",
                "--package-dir",
                "replay",
                "--accounting-dir",
                "accounting-a",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED_OFFLINE_REPLAY", payload["status"])
        verify.assert_called_once_with(
            Path("replay"),
            source_accounting_dirs=[Path("accounting-a")],
        )

    def test_collection_plan_commands_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.rsi_exam.collection_plan.audit_collection_candidates",
            return_value={"status": "READY_FOR_OUTCOME_BLIND_SELECTION"},
        ) as audit:
            status, payload = self._invoke([
                "audit-rsi-exam-trace-collection-candidates",
                "--public-task-set",
                "public-tasks.json",
                "--cohort-spec",
                "cohort-spec.json",
            ])
        self.assertEqual(0, status)
        self.assertEqual("READY_FOR_OUTCOME_BLIND_SELECTION", payload["status"])
        audit.assert_called_once_with(
            Path("public-tasks.json"),
            Path("cohort-spec.json"),
            None,
        )

        with mock.patch(
            "pathfinder.rsi_exam.collection_plan.freeze_collection_plan",
            return_value={"status": "FROZEN_OUTCOME_BLIND_COLLECTION_PLAN"},
        ) as freeze:
            status, _ = self._invoke([
                "freeze-rsi-exam-trace-collection-plan",
                "--public-task-set",
                "public-tasks.json",
                "--cohort-spec",
                "cohort-spec.json",
                "--builder-commit",
                "7" * 40,
                "--output-dir",
                "plan",
            ])
        self.assertEqual(0, status)
        freeze.assert_called_once_with(
            Path("public-tasks.json"),
            Path("cohort-spec.json"),
            builder_commit="7" * 40,
            output_dir=Path("plan"),
            raw_candidate_bindings=None,
        )

        with mock.patch(
            "pathfinder.rsi_exam.collection_plan.verify_collection_plan",
            return_value={"status": "VERIFIED_OUTCOME_BLIND_COLLECTION_PLAN"},
        ) as verify:
            status, _ = self._invoke([
                "verify-rsi-exam-trace-collection-plan",
                "--plan-dir",
                "plan",
                "--public-task-set",
                "public-tasks.json",
                "--cohort-spec",
                "cohort-spec.json",
                "--builder-commit",
                "7" * 40,
            ])
        self.assertEqual(0, status)
        verify.assert_called_once_with(
            Path("plan"),
            public_task_set=Path("public-tasks.json"),
            cohort_spec=Path("cohort-spec.json"),
            builder_commit="7" * 40,
            raw_candidate_bindings=None,
        )

    def test_blocked_collection_audit_returns_nonzero(self) -> None:
        with mock.patch(
            "pathfinder.rsi_exam.collection_plan.audit_collection_candidates",
            return_value={"status": "BLOCKED_INSUFFICIENT_PUBLIC_CANDIDATES"},
        ):
            status, payload = self._invoke([
                "audit-rsi-exam-trace-collection-candidates",
                "--public-task-set",
                "public-tasks.json",
                "--cohort-spec",
                "cohort-spec.json",
            ])
        self.assertEqual(2, status)
        self.assertEqual(
            "BLOCKED_INSUFFICIENT_PUBLIC_CANDIDATES",
            payload["status"],
        )

    def test_run_and_compare_commands_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.rsi_exam.offline_replay.run_offline_replay_policy",
            return_value={"status": "COMPLETE"},
        ) as run:
            status, _ = self._invoke([
                "run-rsi-exam-offline-replay",
                "--package-dir",
                "replay",
                "--policy",
                "amortization-aware",
                "--mode",
                "shared-dataset-sequence",
                "--queries",
                "10",
                "--seed",
                "7",
            ])
        self.assertEqual(0, status)
        run.assert_called_once_with(
            Path("replay"),
            policy_name="amortization-aware",
            mode="shared-dataset-sequence",
            query_count=10,
            seed=7,
            case_id=None,
        )

        with mock.patch(
            "pathfinder.rsi_exam.offline_replay."
            "compare_offline_replay_baselines",
            return_value={"status": "COMPLETE"},
        ) as compare:
            status, _ = self._invoke([
                "compare-rsi-exam-offline-replay-baselines",
                "--package-dir",
                "replay",
                "--mode",
                "independent-query",
                "--queries",
                "1",
            ])
        self.assertEqual(0, status)
        compare.assert_called_once_with(
            Path("replay"),
            mode="independent-query",
            query_count=1,
            seed=0,
            case_id=None,
        )


if __name__ == "__main__":
    unittest.main()
