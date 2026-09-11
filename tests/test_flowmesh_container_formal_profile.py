from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from pathfinder.cli import main as cli_main
from pathfinder.integrations.flowmesh.container_dag import FlowMeshContainerDagError
from pathfinder.integrations.flowmesh.container_formal_profile import (
    FlowMeshContainerFormalProfileError,
    freeze_flowmesh_container_formal_execution_profile,
    verify_flowmesh_container_formal_execution_profile,
)
from pathfinder.integrations.flowmesh.container_matrix import (
    plan_flowmesh_container_matrix,
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


class FlowMeshContainerFormalProfileTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.portable = self.root / "portable"
        self.container = self.root / "container"
        build_portable_execution_plan(SCENARIO, output_dir=self.portable)
        plan_container_backend(
            SCENARIO,
            self.portable,
            CONTAINER_SPEC,
            output_dir=self.container,
        )
        self.profile_id = "formal-infrastructure-v1"
        self.matrix = self.root / "matrix"
        plan_flowmesh_container_matrix(
            portable_plan_dir=self.portable,
            container_plan_dir=self.container,
            node_api_urls=_urls(),
            worker_alias="matrix-test-worker",
            matrix_id="flowmesh-4x8-formal-profile-test-v1",
            source_git_revision="a" * 40,
            execution_profile_id=self.profile_id,
            api_task_timeout_seconds=900,
            output_dir=self.matrix,
        )

        # Reuse the calibration test fixture, which constructs two complete
        # v2 FlowMesh artifacts through a fake client and audits them without
        # contacting a live service.
        calibration_fixture = FullChainCalibrationAuditTest("runTest")
        calibration_fixture.setUp()
        self.addCleanup(calibration_fixture.temporary.cleanup)
        fast_plan, fast_run, slow_plan, slow_run = calibration_fixture._make_pair()
        from pathfinder.integrations.flowmesh.container_full_chain_calibration import (
            audit_flowmesh_container_full_chain_calibration,
        )

        self.audit = self.root / "audit"
        audit_flowmesh_container_full_chain_calibration(
            fast_plan_dir=fast_plan,
            fast_run_dir=fast_run,
            slow_plan_dir=slow_plan,
            slow_run_dir=slow_run,
            output_dir=self.audit,
        )

    def _freeze(self, name: str = "profile") -> dict[str, object]:
        return freeze_flowmesh_container_formal_execution_profile(
            matrix_plan_dir=self.matrix,
            calibration_audit_dir=self.audit,
            execution_profile_id=self.profile_id,
            output_dir=self.root / name,
        )

    def test_freezes_a_serial_infrastructure_only_profile(self) -> None:
        report = self._freeze()
        self.assertEqual("FROZEN_FORMAL_INFRASTRUCTURE_PROFILE", report["status"])
        self.assertEqual(1, report["primary_trial_wrapper_max_concurrency"])
        self.assertFalse(report["cross_lane_parallelism_authorized"])
        self.assertEqual(0, report["parameters_fitted"])
        self.assertFalse(report["eligible_for_scientific_claims"])

        verified = verify_flowmesh_container_formal_execution_profile(
            self.root / "profile"
        )
        self.assertEqual("VERIFIED", verified["status"])
        profile = json.loads(
            (self.root / "profile" / "flowmesh-container-formal-execution-profile.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual("infrastructure-conformance-only", profile["measurement_scope"]["evidence_class"])
        self.assertFalse(profile["measurement_scope"]["physical_monetary_cost_evaluated"])
        self.assertNotIn("127.0.0.1", json.dumps(profile))

    def test_refuses_any_nonserial_primary_concurrency(self) -> None:
        with self.assertRaisesRegex(
            FlowMeshContainerFormalProfileError,
            "primary_trial_wrapper_max_concurrency=1",
        ):
            freeze_flowmesh_container_formal_execution_profile(
                matrix_plan_dir=self.matrix,
                calibration_audit_dir=self.audit,
                execution_profile_id=self.profile_id,
                output_dir=self.root / "parallel",
                primary_trial_wrapper_max_concurrency=2,
            )
        self.assertFalse((self.root / "parallel").exists())

    def test_refuses_profile_identity_drift_from_the_matrix(self) -> None:
        with self.assertRaisesRegex(
            FlowMeshContainerFormalProfileError,
            "does not match",
        ):
            freeze_flowmesh_container_formal_execution_profile(
                matrix_plan_dir=self.matrix,
                calibration_audit_dir=self.audit,
                execution_profile_id="another-profile",
                output_dir=self.root / "wrong-id",
            )
        self.assertFalse((self.root / "wrong-id").exists())

    def test_verifier_refuses_tampering(self) -> None:
        self._freeze()
        profile = self.root / "profile" / "flowmesh-container-formal-execution-profile.json"
        profile.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            FlowMeshContainerFormalProfileError,
            "checksum mismatch",
        ):
            verify_flowmesh_container_formal_execution_profile(self.root / "profile")

    def test_cli_freezes_and_verifies_the_profile_offline(self) -> None:
        profile_dir = self.root / "cli-profile"
        stream = io.StringIO()
        with redirect_stdout(stream):
            code = cli_main(
                [
                    "freeze-flowmesh-container-formal-execution-profile",
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--calibration-audit-dir",
                    str(self.audit),
                    "--execution-profile-id",
                    self.profile_id,
                    "--output-dir",
                    str(profile_dir),
                    "--compact",
                ]
            )
        self.assertEqual(0, code)
        frozen = json.loads(stream.getvalue())
        self.assertEqual("FROZEN_FORMAL_INFRASTRUCTURE_PROFILE", frozen["status"])

        stream = io.StringIO()
        with redirect_stdout(stream):
            code = cli_main(
                [
                    "verify-flowmesh-container-formal-execution-profile",
                    "--profile-dir",
                    str(profile_dir),
                    "--compact",
                ]
            )
        self.assertEqual(0, code)
        self.assertEqual("VERIFIED", json.loads(stream.getvalue())["status"])


if __name__ == "__main__":
    unittest.main()
