from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pathfinder.integrations.flowmesh.container_dag import FlowMeshContainerDagError
from pathfinder.integrations.flowmesh.container_full_chain import (
    build_flowmesh_container_full_physical_chain_workflow,
    list_full_physical_container_operation_chain_candidates,
    plan_flowmesh_container_full_physical_chain,
    run_flowmesh_container_full_physical_chain,
    select_full_physical_container_operation_chain,
    verify_flowmesh_container_full_physical_chain_plan,
    verify_flowmesh_container_full_physical_chain_run,
)
from pathfinder.integrations.flowmesh.contracts import FlowMeshSettings
from tests.test_flowmesh_container_dag import (
    FakeFlowMeshClient,
    _RUN_URLS,
    _link,
    _operation,
    _runtime_epoch_probe,
)


def _full_retrieval_chain() -> list[dict[str, Any]]:
    """A D0-like five-operation retrieval path ending at N6."""

    control = _operation("retrieval|control", "control", "N1", [], logical_bytes=0)
    read = _operation(
        "retrieval|read-raw", "storage_read", "N3", [control["operation_key"]],
        logical_bytes=720_000_000, trial_key="retrieval",
    )
    transfer = _operation(
        "retrieval|transfer-raw", "network_transfer", "N7",
        [read["operation_key"]], logical_bytes=720_000_000, trial_key="retrieval",
        link_adapter=_link(3_125_000_000, 2.0),
    )
    decode = _operation(
        "retrieval|decode", "compute", "N7", [transfer["operation_key"]],
        logical_bytes=0, trial_key="retrieval",
    )
    return_transfer = _operation(
        "retrieval|return", "network_transfer", "N6", [decode["operation_key"]],
        logical_bytes=262_144, trial_key="retrieval",
        link_adapter=_link(12_500_000_000, 0.2),
    )
    score = _operation(
        "retrieval|score", "compute", "N6", [return_transfer["operation_key"]],
        logical_bytes=0, trial_key="retrieval",
    )
    return [control, read, transfer, decode, return_transfer, score]


def _write_ledger(root: Path) -> Path:
    path = root / "container_operations.jsonl"
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in _full_retrieval_chain()),
        encoding="utf-8",
    )
    return path


class FullPhysicalChainSelectionTest(unittest.TestCase):
    def test_selects_every_physical_operation_to_the_terminal_compute(self) -> None:
        selected = select_full_physical_container_operation_chain(
            _full_retrieval_chain(), trial_key="retrieval"
        )
        self.assertEqual(
            [
                "retrieval|read-raw",
                "retrieval|transfer-raw",
                "retrieval|decode",
                "retrieval|return",
                "retrieval|score",
            ],
            [row["operation_key"] for row in selected],
        )
        self.assertEqual("compute", selected[-1]["operation_kind"])

    def test_candidate_discloses_a_five_operation_terminal_path(self) -> None:
        candidates = list_full_physical_container_operation_chain_candidates(
            _full_retrieval_chain()
        )
        self.assertEqual(1, len(candidates))
        self.assertEqual(5, candidates[0]["physical_operation_count"])
        self.assertEqual("retrieval|score", candidates[0]["terminal_physical_operation_key"])
        self.assertEqual(["N3", "N7", "N7", "N6", "N6"], candidates[0]["execution_nodes"])

    def test_conditional_physical_successor_cannot_be_silently_omitted(self) -> None:
        ledger = _full_retrieval_chain()
        ledger[-1]["condition"] = {
            "cache_operation_key": "retrieval|cache",
            "cache_operation_id": "cache",
            "equals": "hit",
        }
        ledger.append(_operation("retrieval|cache", "cache_read", "N6", [], trial_key="retrieval", logical_bytes=0))
        with self.assertRaises(FlowMeshContainerDagError):
            select_full_physical_container_operation_chain(
                ledger, trial_key="retrieval"
            )


class FullPhysicalChainPlanAndRunTest(unittest.TestCase):
    def test_plan_freezes_five_nodes_with_direct_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = plan_flowmesh_container_full_physical_chain(
                container_operations_path=_write_ledger(root),
                node_api_urls=_RUN_URLS,
                worker_alias="container-smoke-worker",
                smoke_id="full-retrieval-smoke",
                trial_key="retrieval",
                output_dir=root / "plan",
                api_task_timeout_seconds=300,
            )
            self.assertEqual("FROZEN_FULL_PHYSICAL_CHAIN_INPUTS", payload["status"])
            self.assertEqual(5, payload["physical_operation_count"])
            template = json.loads(
                (root / "plan" / "flowmesh-container-full-chain-workflow-template.json").read_text(encoding="utf-8")
            )
            nodes = template["spec"]["graph"]["nodes"]
            self.assertEqual(5, len(nodes))
            self.assertEqual(
                [
                    "operation-01-storage-read",
                    "operation-02-network-transfer",
                    "operation-03-compute",
                    "operation-04-network-transfer",
                    "operation-05-compute",
                ],
                [node["name"] for node in nodes],
            )
            self.assertEqual([300] * 5, [node["spec"]["api"]["timeout_sec"] for node in nodes])
            self.assertEqual("VERIFIED", verify_flowmesh_container_full_physical_chain_plan(root / "plan")["status"])

    def test_fake_flowmesh_run_covers_all_five_operations_and_verifies(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan_flowmesh_container_full_physical_chain(
                container_operations_path=_write_ledger(root),
                node_api_urls=_RUN_URLS,
                worker_alias="container-smoke-worker",
                smoke_id="full-retrieval-smoke",
                trial_key="retrieval",
                output_dir=root / "plan",
                api_task_timeout_seconds=300,
            )
            client = FakeFlowMeshClient()
            result = run_flowmesh_container_full_physical_chain(
                plan_dir=root / "plan",
                output_dir=root / "run",
                client=client,
                settings=FlowMeshSettings(
                    worker_alias="container-smoke-worker",
                    validate_before_submit=True,
                ),
                runtime_epoch_probe=_runtime_epoch_probe,
            )
            self.assertEqual("COMPLETE", result["status"])
            self.assertEqual(5, result["task_result_count"])
            self.assertEqual(5, len(client.submitted["spec"]["graph"]["nodes"]))
            self.assertIn("retrieval|return", result["telemetry"]["service_time_ms_by_operation_key"])
            self.assertEqual(
                "VERIFIED",
                verify_flowmesh_container_full_physical_chain_run(
                    root / "run", plan_dir=root / "plan"
                )["status"],
            )

    def test_builder_rejects_a_prefix_without_a_terminal_compute(self) -> None:
        prefix = _full_retrieval_chain()[1:3]
        with self.assertRaises(FlowMeshContainerDagError):
            build_flowmesh_container_full_physical_chain_workflow(
                prefix,
                node_api_urls=_RUN_URLS,
                selected_worker_id="wkr-77",
                smoke_id="prefix",
            )


if __name__ == "__main__":
    unittest.main()
