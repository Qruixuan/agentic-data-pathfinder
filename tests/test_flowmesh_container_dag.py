from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from pathfinder.integrations.flowmesh.container_dag import (
    FlowMeshContainerDagError,
    build_flowmesh_container_operation_workflow,
    list_linear_container_operation_dag_candidates,
    plan_flowmesh_container_operation_dag,
    run_flowmesh_container_operation_dag,
    select_linear_container_operation_dag,
    verify_flowmesh_container_operation_dag_plan,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.simulator.container_contract import (
    CONTAINER_OPERATION_SCHEMA_VERSION,
)


def _operation(
    key: str,
    kind: str,
    node: str,
    dependencies: list[str],
    *,
    trial_key: str = "smoke-trial",
    logical_bytes: int = 4096,
) -> dict[str, Any]:
    return {
        "schema_version": CONTAINER_OPERATION_SCHEMA_VERSION,
        "backend_id": "container-smoke",
        "portable_plan_sha256": "a" * 64,
        "operation_key": key,
        "trial_key": trial_key,
        "operation_id": key.rsplit("|", 1)[-1],
        "operation_kind": kind,
        "dependency_operation_keys": dependencies,
        "condition": None,
        "object_id": "fixture-001",
        "representation_id": "raw_video",
        "logical_bytes": logical_bytes,
        "operation_adapter": "fixture-v1",
        "resource_adapter": None,
        "link_adapter": None,
        "cache_adapter": None,
        "task_executor": {"semantic_quality_enabled": False},
        "execution_node_id": node,
        "execution_container": f"node-{node.lower()}",
        "destination_node_id": node,
        "destination_container": f"node-{node.lower()}",
        "measure_actual_duration": True,
        "simulation_hint_used_as_measured_duration": False,
    }


def _chain() -> list[dict[str, Any]]:
    read = _operation("smoke-trial|read", "storage_read", "N3", [])
    transfer = _operation(
        "smoke-trial|transfer",
        "network_transfer",
        "N7",
        [read["operation_key"]],
    )
    compute = _operation(
        "smoke-trial|compute",
        "compute",
        "N6",
        [transfer["operation_key"]],
        logical_bytes=0,
    )
    return [read, transfer, compute]


class FakeFlowMeshClient:
    def __init__(self, *, assigned_worker: str = "wkr-77") -> None:
        self.assigned_worker = assigned_worker
        self.validated: list[dict[str, Any]] = []
        self.submitted: dict[str, Any] | None = None
        self.results: dict[str, dict[str, Any]] = {}

    def describe_current_worker(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> FlowMeshWorkerIdentity:
        if alias != "container-smoke-worker" or worker_id is not None:
            raise RuntimeError("unexpected worker selector")
        return FlowMeshWorkerIdentity(
            worker_id=self.assigned_worker,
            alias=alias,
            status="IDLE",
        )

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        self.validated.append(dict(workflow))
        return WorkflowValidation(ok=True)

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        self.submitted = dict(workflow)
        nodes = workflow["spec"]["graph"]["nodes"]
        task_ids = tuple(f"tsk-{index}" for index in range(len(nodes)))
        for task_id, node in zip(task_ids, nodes):
            operation = node["spec"]["api"]["body"]
            physical_bytes = (
                operation["logical_bytes"]
                if operation["operation_kind"]
                in ("storage_read", "network_transfer")
                else 0
            )
            body = {
                "status": "completed",
                "outcome_type": "completed",
                "telemetry_complete": True,
                "credentials_recorded": False,
                "idempotent_replay": False,
                "operation_key": operation["operation_key"],
                "operation_kind": operation["operation_kind"],
                "execution_node_id": operation["execution_node_id"],
                "logical_bytes": operation["logical_bytes"],
                "physical_bytes": physical_bytes,
            }
            self.results[task_id] = {
                "executor": "api",
                "ok": True,
                "status_code": 200,
                "text": json.dumps(body),
            }
        return SubmittedWorkflow("wfl-container-smoke", task_ids)

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        return TerminalWorkflow(workflow_id, "DONE")

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        return self.results[task_id]

    def describe_task_failure(self, task_id: str) -> dict[str, Any]:
        return {"task_status": "DONE", "assigned_worker": self.assigned_worker}


class FlowMeshContainerDagTest(unittest.TestCase):
    def test_selects_exact_linear_chain(self) -> None:
        selected = select_linear_container_operation_dag(
            _chain(), trial_key="smoke-trial"
        )
        self.assertEqual(
            ["storage_read", "network_transfer", "compute"],
            [row["operation_kind"] for row in selected],
        )

    def test_refuses_ambiguous_chain_without_trial_pin(self) -> None:
        alternative = _chain()
        for item in alternative:
            item["trial_key"] = "second-trial"
            item["operation_key"] = item["operation_key"].replace(
                "smoke-trial", "second-trial"
            )
        alternative[1]["dependency_operation_keys"] = [
            alternative[0]["operation_key"]
        ]
        alternative[2]["dependency_operation_keys"] = [
            alternative[1]["operation_key"]
        ]
        with self.assertRaises(FlowMeshContainerDagError) as context:
            select_linear_container_operation_dag(_chain() + alternative)
        self.assertIn("multiple linear", str(context.exception))

    def test_workflow_is_a_pinned_three_node_flowmesh_graph(self) -> None:
        workflow = build_flowmesh_container_operation_workflow(
            _chain(),
            node_api_urls={
                "N3": "http://127.0.0.1:29083",
                "N7": "http://127.0.0.1:29087",
                "N6": "http://127.0.0.1:29086",
            },
            selected_worker_id="wkr-77",
            smoke_id="dag-smoke-001",
        )
        self.assertEqual("wkr-77", workflow["metadata"]["annotations"]["schedule_hint"]["selected_worker"])
        nodes = workflow["spec"]["graph"]["nodes"]
        self.assertEqual(["storage-read", "network-transfer", "compute"], [node["name"] for node in nodes])
        self.assertNotIn("dependsOn", nodes[0])
        self.assertEqual(["storage-read"], nodes[1]["dependsOn"])
        self.assertEqual(["network-transfer"], nodes[2]["dependsOn"])
        self.assertTrue(
            nodes[0]["spec"]["api"]["url"].endswith(
                "/v1/operations/execute"
            )
        )

    def test_control_predecessor_is_disclosed_but_index_predecessor_is_not_skipped(self) -> None:
        schedule = _operation("smoke-trial|schedule", "control", "N1", [])
        chain = _chain()
        chain[0]["dependency_operation_keys"] = [schedule["operation_key"]]
        selected = select_linear_container_operation_dag(
            [schedule] + chain, trial_key="smoke-trial"
        )
        self.assertEqual("smoke-trial|read", selected[0]["operation_key"])
        candidates = list_linear_container_operation_dag_candidates(
            [schedule] + chain
        )
        self.assertEqual([schedule["operation_key"]], candidates[0]["omitted_nonphysical_predecessor_operation_keys"])

        index = _operation("smoke-trial|index", "index_query", "N2", [])
        chain[0]["dependency_operation_keys"] = [index["operation_key"]]
        self.assertEqual(
            [],
            list_linear_container_operation_dag_candidates([index] + chain),
        )

    def test_plan_and_fake_flowmesh_run_are_bound_and_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            operations_path = root / "container_operations.jsonl"
            operations_path.write_text(
                "".join(json.dumps(row) + "\n" for row in _chain()),
                encoding="utf-8",
            )
            plan_dir = root / "plan"
            result = plan_flowmesh_container_operation_dag(
                container_operations_path=operations_path,
                node_api_urls={
                    "N3": "http://127.0.0.1:29083",
                    "N7": "http://127.0.0.1:29087",
                    "N6": "http://127.0.0.1:29086",
                },
                worker_alias="container-smoke-worker",
                smoke_id="dag-smoke-001",
                trial_key="smoke-trial",
                output_dir=plan_dir,
            )
            self.assertEqual("FROZEN_WORKFLOW_INPUTS", result["status"])
            self.assertEqual(
                "VERIFIED",
                verify_flowmesh_container_operation_dag_plan(plan_dir)["status"],
            )
            client = FakeFlowMeshClient()
            run = run_flowmesh_container_operation_dag(
                plan_dir=plan_dir,
                output_dir=root / "run",
                client=client,
                settings=FlowMeshSettings(
                    worker_alias="container-smoke-worker",
                    validate_before_submit=True,
                ),
            )
            self.assertEqual("COMPLETE", run["status"])
            self.assertEqual(3, run["task_result_count"])
            self.assertFalse(run["llm_called"])
            self.assertFalse(run["semantic_task_quality_evaluated"])
            self.assertTrue(client.validated)
            self.assertEqual("wkr-77", client.submitted["metadata"]["annotations"]["schedule_hint"]["selected_worker"])

    def test_run_refuses_wrong_assigned_worker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            operations_path = root / "container_operations.jsonl"
            operations_path.write_text(
                "".join(json.dumps(row) + "\n" for row in _chain()),
                encoding="utf-8",
            )
            plan_flowmesh_container_operation_dag(
                container_operations_path=operations_path,
                node_api_urls={
                    "N3": "http://127.0.0.1:29083",
                    "N7": "http://127.0.0.1:29087",
                    "N6": "http://127.0.0.1:29086",
                },
                worker_alias="container-smoke-worker",
                smoke_id="dag-smoke-001",
                trial_key="smoke-trial",
                output_dir=root / "plan",
            )
            client = FakeFlowMeshClient(assigned_worker="wkr-wrong")
            # The client reports wkr-wrong at pin resolution too, so alter the
            # finished task metadata after construction to simulate a scheduler
            # pin violation rather than a Root-resolution failure.
            client.describe_task_failure = lambda task_id: {
                "task_status": "DONE",
                "assigned_worker": "wkr-other",
            }
            with self.assertRaises(FlowMeshContainerDagError) as context:
                run_flowmesh_container_operation_dag(
                    plan_dir=root / "plan",
                    output_dir=root / "run",
                    client=client,
                    settings=FlowMeshSettings(
                        worker_alias="container-smoke-worker",
                        validate_before_submit=True,
                    ),
                )
            self.assertIn("other than the pin", str(context.exception))
            self.assertFalse((root / "run").exists())


if __name__ == "__main__":
    unittest.main()
