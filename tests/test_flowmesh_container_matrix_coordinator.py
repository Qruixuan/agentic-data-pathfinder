from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from pathfinder.cli import main as cli_main
from pathfinder.integrations.flowmesh.container_formal_profile import (
    freeze_flowmesh_container_formal_execution_profile,
)
from pathfinder.integrations.flowmesh.container_full_chain_calibration import (
    audit_flowmesh_container_full_chain_calibration,
)
from pathfinder.integrations.flowmesh.container_matrix import (
    plan_flowmesh_container_matrix,
)
from pathfinder.integrations.flowmesh.container_matrix_coordinator import (
    FlowMeshContainerMatrixCoordinatorError,
    _trial_wrappers,
    plan_flowmesh_container_matrix_coordinator_dry_run,
    verify_flowmesh_container_matrix_coordinator_dry_run,
)
from pathfinder.simulator import build_portable_execution_plan, plan_container_backend
from tests.test_flowmesh_container_full_chain_calibration import (
    FullChainCalibrationAuditTest,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"


def _urls() -> dict[str, str]:
    return {
        f"N{number}": f"http://127.0.0.1:{19080 + number}"
        for number in range(1, 9)
    }


class FlowMeshContainerMatrixCoordinatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.profile_id = "formal-infrastructure-v1"
        self.portable = self.root / "portable"
        self.container = self.root / "container"
        build_portable_execution_plan(SCENARIO, output_dir=self.portable)
        plan_container_backend(
            SCENARIO,
            self.portable,
            CONTAINER_SPEC,
            output_dir=self.container,
        )
        self.matrix = self._matrix("matrix", "matrix-v1")

        calibration_fixture = FullChainCalibrationAuditTest("runTest")
        calibration_fixture.setUp()
        self.addCleanup(calibration_fixture.temporary.cleanup)
        fast_plan, fast_run, slow_plan, slow_run = calibration_fixture._make_pair()
        self.audit = self.root / "audit"
        audit_flowmesh_container_full_chain_calibration(
            fast_plan_dir=fast_plan,
            fast_run_dir=fast_run,
            slow_plan_dir=slow_plan,
            slow_run_dir=slow_run,
            output_dir=self.audit,
        )
        self.profile = self.root / "profile"
        freeze_flowmesh_container_formal_execution_profile(
            matrix_plan_dir=self.matrix,
            calibration_audit_dir=self.audit,
            execution_profile_id=self.profile_id,
            output_dir=self.profile,
        )

    def _matrix(self, name: str, matrix_id: str) -> Path:
        output = self.root / name
        plan_flowmesh_container_matrix(
            portable_plan_dir=self.portable,
            container_plan_dir=self.container,
            node_api_urls=_urls(),
            worker_alias="matrix-coordinator-test-worker",
            matrix_id=matrix_id,
            source_git_revision="a" * 40,
            execution_profile_id=self.profile_id,
            api_task_timeout_seconds=900,
            output_dir=output,
        )
        return output

    def _freeze(self, name: str = "coordinator") -> dict[str, object]:
        return plan_flowmesh_container_matrix_coordinator_dry_run(
            matrix_plan_dir=self.matrix,
            formal_execution_profile_dir=self.profile,
            coordinator_id="formal-4x8-coordinator-v1",
            output_dir=self.root / name,
        )

    def test_freezes_and_source_verifies_a_serial_64_wrapper_dry_run(self) -> None:
        report = self._freeze()
        self.assertEqual("FROZEN_MATRIX_COORDINATOR_DRY_RUN", report["status"])
        self.assertEqual(64, report["trial_wrapper_count"])
        self.assertEqual(16, report["conditional_trial_wrapper_count"])
        self.assertEqual(1, report["primary_trial_wrapper_max_concurrency"])
        self.assertFalse(report["workflow_submitted"])
        self.assertFalse(report["eligible_for_scientific_claims"])

        verified = verify_flowmesh_container_matrix_coordinator_dry_run(
            self.root / "coordinator",
            matrix_plan_dir=self.matrix,
            formal_execution_profile_dir=self.profile,
        )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertTrue(verified["source_binding_checked"])

        wrappers = [
            json.loads(line)
            for line in (
                self.root
                / "coordinator"
                / "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl"
            ).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(list(range(64)), [row["sequence_index"] for row in wrappers])
        self.assertEqual(
            [None] + [row["trial_key"] for row in wrappers[:-1]],
            [row["global_serial_predecessor_trial_key"] for row in wrappers],
        )
        self.assertTrue(
            all(
                row["maximum_concurrent_primary_trial_wrappers"] == 1
                and row["primary_trial_wrapper_slot"] == 0
                for row in wrappers
            )
        )
        conditional = [
            row for row in wrappers if row["conditional_operation_count"] > 0
        ]
        self.assertEqual(16, len(conditional))
        self.assertTrue(
            all(
                row["conditional_protocol"]["strategy"]
                == "two-phase-live-cache-observation-then-frozen-branch"
                and row["conditional_protocol"][
                    "operation_level_submission_permitted"
                ]
                is False
                for row in conditional
            )
        )
        package_text = "".join(
            path.read_text(encoding="utf-8")
            for path in (self.root / "coordinator").iterdir()
            if path.is_file()
        )
        self.assertNotIn("127.0.0.1", package_text)

    def test_portable_verification_does_not_require_source_directories(self) -> None:
        self._freeze()
        verified = verify_flowmesh_container_matrix_coordinator_dry_run(
            self.root / "coordinator"
        )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertFalse(verified["source_binding_checked"])

    def test_refuses_only_one_source_directory_for_cross_binding(self) -> None:
        self._freeze()
        with self.assertRaisesRegex(
            FlowMeshContainerMatrixCoordinatorError,
            "supplied together",
        ):
            verify_flowmesh_container_matrix_coordinator_dry_run(
                self.root / "coordinator", matrix_plan_dir=self.matrix
            )

    def test_refuses_a_profile_bound_to_another_matrix(self) -> None:
        other_matrix = self._matrix("other-matrix", "matrix-v2")
        output = self.root / "wrong-profile"
        with self.assertRaisesRegex(
            FlowMeshContainerMatrixCoordinatorError,
            "different matrix plan",
        ):
            plan_flowmesh_container_matrix_coordinator_dry_run(
                matrix_plan_dir=other_matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_id="formal-4x8-coordinator-v1",
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_refuses_to_flatten_an_unsupported_conditional_design(self) -> None:
        trial_rows = [
            json.loads(line)
            for line in (
                self.matrix / "flowmesh-container-matrix-trials.jsonl"
            ).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        operation_rows = [
            json.loads(line)
            for line in (
                self.matrix / "flowmesh-container-matrix-operations.jsonl"
            ).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        d0_key = next(row["trial_key"] for row in trial_rows if row["design_id"] == "D0")
        d0_operation = next(
            row for row in operation_rows if row["trial_key"] == d0_key
        )
        d0_operation["condition"] = {
            "cache_operation_key": "not-used",
            "equals": "hit",
        }
        with self.assertRaisesRegex(
            FlowMeshContainerMatrixCoordinatorError,
            "unsupported for design D0",
        ):
            _trial_wrappers(trials=trial_rows, operations=operation_rows)

    def test_verifier_refuses_tampered_wrapper_ledger(self) -> None:
        self._freeze()
        target = (
            self.root
            / "coordinator"
            / "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl"
        )
        target.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            FlowMeshContainerMatrixCoordinatorError,
            "checksum mismatch",
        ):
            verify_flowmesh_container_matrix_coordinator_dry_run(
                self.root / "coordinator"
            )

    def test_cli_freezes_and_source_verifies_the_dry_run_package(self) -> None:
        output = self.root / "cli-coordinator"
        stream = io.StringIO()
        with redirect_stdout(stream):
            code = cli_main(
                [
                    "plan-flowmesh-container-matrix-coordinator-dry-run",
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--formal-execution-profile-dir",
                    str(self.profile),
                    "--coordinator-id",
                    "formal-4x8-cli-coordinator-v1",
                    "--output-dir",
                    str(output),
                    "--compact",
                ]
            )
        self.assertEqual(0, code)
        self.assertEqual(
            "FROZEN_MATRIX_COORDINATOR_DRY_RUN",
            json.loads(stream.getvalue())["status"],
        )

        stream = io.StringIO()
        with redirect_stdout(stream):
            code = cli_main(
                [
                    "verify-flowmesh-container-matrix-coordinator-dry-run",
                    "--plan-dir",
                    str(output),
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--formal-execution-profile-dir",
                    str(self.profile),
                    "--compact",
                ]
            )
        self.assertEqual(0, code)
        payload = json.loads(stream.getvalue())
        self.assertEqual("VERIFIED", payload["status"])
        self.assertTrue(payload["source_binding_checked"])


if __name__ == "__main__":
    unittest.main()
