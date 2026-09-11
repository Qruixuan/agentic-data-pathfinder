from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.integrations.flowmesh.container_dag import FlowMeshContainerDagError
from pathfinder.integrations.flowmesh.container_matrix import (
    plan_flowmesh_container_matrix,
    verify_flowmesh_container_matrix_plan,
)
from pathfinder.simulator.container_contract import (
    CONTAINER_NODE_RESULT_SCHEMA_VERSION,
)
from pathfinder.simulator import (
    build_portable_execution_plan,
    plan_container_backend,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"


def _urls() -> dict[str, str]:
    return {
        f"N{number}": f"http://127.0.0.1:{19080 + number}"
        for number in range(1, 9)
    }


class FlowMeshContainerMatrixPlanTest(unittest.TestCase):
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

    def _plan(self, name: str = "matrix") -> dict[str, object]:
        return plan_flowmesh_container_matrix(
            portable_plan_dir=self.portable,
            container_plan_dir=self.container,
            node_api_urls=_urls(),
            worker_alias="matrix-test-worker",
            matrix_id="flowmesh-4x8-test-v1",
            source_git_revision="a" * 40,
            execution_profile_id="development-load-v1",
            api_task_timeout_seconds=900,
            output_dir=self.root / name,
        )

    def test_freezes_and_verifies_the_exact_64_trial_500_operation_matrix(self) -> None:
        report = self._plan()
        self.assertEqual("FROZEN_4X8_CONTAINER_MATRIX", report["status"])
        self.assertEqual(64, report["matrix_dimensions"]["trial_count"])
        self.assertEqual(500, report["operation_count"])
        self.assertEqual(80, report["conditional_operation_count"])
        self.assertEqual(4, report["cache_scope_count"])
        self.assertEqual(576.03, report["max_operation_lower_bound_seconds"])
        self.assertFalse(report["workflow_submitted"])
        self.assertFalse(report["services_started"])

        matrix_root = self.root / "matrix"
        verified = verify_flowmesh_container_matrix_plan(matrix_root)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(64, verified["matrix_dimensions"]["trial_count"])
        self.assertEqual(500, verified["operation_count"])
        self.assertEqual("required-v2", verified["runtime_integrity"])

        plan = json.loads(
            (matrix_root / "flowmesh-container-matrix-plan.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertTrue(plan["flowmesh_execution_boundary"]["coordinator_required"])
        self.assertFalse(
            plan["flowmesh_execution_boundary"]["operation_level_submission_supported"]
        )
        self.assertEqual(
            CONTAINER_NODE_RESULT_SCHEMA_VERSION,
            plan["runtime_integrity"]["container_operation_result_schema_version"],
        )
        self.assertTrue(plan["runtime_integrity"]["runtime_epoch_binding_required"])
        self.assertTrue(plan["runtime_integrity"]["pre_submit_health_required"])
        self.assertTrue(plan["runtime_integrity"]["post_run_health_required"])
        self.assertEqual(
            {"D3|r0000", "D3|r0001", "D7|r0000", "D7|r0001"},
            {row["cache_scope_id"] for row in plan["cache_scopes"]},
        )

    def test_refuses_an_incomplete_eight_node_endpoint_map_before_writing(self) -> None:
        urls = _urls()
        urls.pop("N5")
        output = self.root / "missing-node"
        with self.assertRaisesRegex(
            FlowMeshContainerDagError,
            "no API URL was supplied for execution node",
        ):
            plan_flowmesh_container_matrix(
                portable_plan_dir=self.portable,
                container_plan_dir=self.container,
                node_api_urls=urls,
                worker_alias="matrix-test-worker",
                matrix_id="flowmesh-4x8-test-v1",
                source_git_revision="a" * 40,
                execution_profile_id="development-load-v1",
                api_task_timeout_seconds=900,
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_refuses_a_timeout_below_the_frozen_slow_link_floor(self) -> None:
        output = self.root / "too-short"
        with self.assertRaisesRegex(
            FlowMeshContainerDagError,
            "required at least 576.03s",
        ):
            plan_flowmesh_container_matrix(
                portable_plan_dir=self.portable,
                container_plan_dir=self.container,
                node_api_urls=_urls(),
                worker_alias="matrix-test-worker",
                matrix_id="flowmesh-4x8-test-v1",
                source_git_revision="a" * 40,
                execution_profile_id="development-load-v1",
                api_task_timeout_seconds=300,
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_verifier_rejects_a_tampered_plan_without_relying_on_a_source_directory(self) -> None:
        self._plan()
        target = self.root / "matrix" / "flowmesh-container-matrix-plan.json"
        target.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            FlowMeshContainerDagError,
            "checksum mismatch",
        ):
            verify_flowmesh_container_matrix_plan(self.root / "matrix")


if __name__ == "__main__":
    unittest.main()
