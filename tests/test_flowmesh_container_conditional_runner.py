from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Mapping

from pathfinder.cli import main as cli_main
from pathfinder.integrations.flowmesh.container_conditional_runner import (
    plan_flowmesh_container_conditional_trial,
    run_flowmesh_container_conditional_trial,
    verify_flowmesh_container_conditional_trial_plan,
    verify_flowmesh_container_conditional_trial_run,
)
from pathfinder.integrations.flowmesh.container_dag import FlowMeshContainerDagError
from pathfinder.integrations.flowmesh.container_matrix import (
    plan_flowmesh_container_matrix,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.simulator import build_portable_execution_plan, plan_container_backend
from pathfinder.simulator.container_contract import (
    CONTAINER_NODE_RESULT_SCHEMA_VERSION,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
SCENARIO_ID = "flowmesh-infra-4x8-local-smoke-v1"


def _urls() -> dict[str, str]:
    return {
        f"N{number}": f"http://127.0.0.1:{19080 + number}"
        for number in range(1, 9)
    }


def _trial_key(workload: str, design: str = "D3", repetition: int = 0) -> str:
    return f"{SCENARIO_ID}|smoke-{workload}|{design}|r{repetition:04d}"


class FakeConditionalFlowMeshClient:
    """Offline FlowMesh double that exposes the actual API task bodies."""

    def __init__(
        self,
        outcomes: Mapping[str, str],
        *,
        worker_changes: bool = False,
        wrong_result_epoch: bool = False,
    ) -> None:
        self.outcomes = dict(outcomes)
        self.worker_changes = worker_changes
        self.wrong_result_epoch = wrong_result_epoch
        self.workflows: list[dict[str, Any]] = []
        self.results: dict[str, dict[str, Any]] = {}
        self.task_workers: dict[str, str] = {}
        self._phase_a_submitted = False

    def describe_current_worker(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> FlowMeshWorkerIdentity:
        if alias is not None:
            if alias != "conditional-test-worker" or worker_id is not None:
                raise RuntimeError("unexpected alias lookup")
            return FlowMeshWorkerIdentity(
                worker_id="wkr-conditional",
                alias=alias,
                status="IDLE",
            )
        if worker_id != "wkr-conditional":
            raise RuntimeError("unexpected worker ID lookup")
        if self.worker_changes:
            return FlowMeshWorkerIdentity(
                worker_id="wkr-replaced",
                alias="conditional-test-worker",
                status="IDLE",
            )
        return FlowMeshWorkerIdentity(
            worker_id=worker_id,
            alias="conditional-test-worker",
            status="IDLE",
        )

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        return WorkflowValidation(ok=True)

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        copied = json.loads(json.dumps(workflow))
        self.workflows.append(copied)
        phase = copied["metadata"]["annotations"]["custom"][
            "pathfinder_conditional_phase"
        ]
        if phase == "A":
            self._phase_a_submitted = True
        task_ids: list[str] = []
        for index, node in enumerate(copied["spec"]["graph"]["nodes"], start=1):
            task_id = f"tsk-{phase.lower()}-{len(self.workflows)}-{index}"
            task_ids.append(task_id)
            operation = node["spec"]["api"]["body"]
            kind = operation["operation_kind"]
            body: dict[str, Any] = {
                "schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
                "status": "completed",
                "outcome_type": "completed",
                "telemetry_complete": True,
                "credentials_recorded": False,
                "idempotent_replay": False,
                "operation_key": operation["operation_key"],
                "operation_kind": kind,
                "execution_node_id": operation["execution_node_id"],
                "destination_runtime_epoch": (
                    "a" * 32 if kind == "network_transfer" else None
                ),
                "runtime_epoch": (
                    "b" * 32 if self.wrong_result_epoch else "a" * 32
                ),
                "logical_bytes": operation["logical_bytes"],
                "physical_bytes": (
                    operation["logical_bytes"]
                    if kind in {"storage_read", "cache_read", "network_transfer"}
                    else 0
                ),
                "cache_result": (
                    self.outcomes.get(operation["operation_key"])
                    if kind == "cache_lookup"
                    else None
                ),
                "cache_scope_id": (
                    operation.get("cache_scope_id")
                    if kind == "cache_lookup"
                    else None
                ),
            }
            self.results[task_id] = {
                "executor": "api",
                "ok": True,
                "status_code": 200,
                "text": json.dumps(body, sort_keys=True),
            }
            self.task_workers[task_id] = "wkr-conditional"
        return SubmittedWorkflow(f"wfl-{phase.lower()}-{len(self.workflows)}", tuple(task_ids))

    def wait(self, workflow_id: str, poll_interval_seconds: float) -> TerminalWorkflow:
        return TerminalWorkflow(workflow_id, "DONE")

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        return self.results[task_id]

    def describe_task_failure(self, task_id: str) -> dict[str, Any]:
        return {"task_status": "DONE", "assigned_worker": self.task_workers[task_id]}


class ConditionalFlowMeshTrialRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        portable = self.root / "portable"
        container = self.root / "container"
        build_portable_execution_plan(SCENARIO, output_dir=portable)
        plan_container_backend(
            SCENARIO,
            portable,
            CONTAINER_SPEC,
            output_dir=container,
        )
        self.matrix = self.root / "matrix"
        plan_flowmesh_container_matrix(
            portable_plan_dir=portable,
            container_plan_dir=container,
            node_api_urls=_urls(),
            worker_alias="conditional-test-worker",
            matrix_id="conditional-test-matrix-v1",
            source_git_revision="a" * 40,
            execution_profile_id="development-load-v1",
            api_task_timeout_seconds=900,
            output_dir=self.matrix,
        )

    def _freeze(self, trial_key: str, name: str = "conditional-plan") -> Path:
        output = self.root / name
        result = plan_flowmesh_container_conditional_trial(
            matrix_plan_dir=self.matrix,
            trial_key=trial_key,
            smoke_id="conditional-runner-test",
            output_dir=output,
        )
        self.assertEqual("FROZEN_CONDITIONAL_TRIAL", result["status"])
        self.assertEqual(
            "VERIFIED",
            verify_flowmesh_container_conditional_trial_plan(output)["status"],
        )
        return output

    @staticmethod
    def _runtime_epoch_probe(
        _urls: Mapping[str, str],
        operations: list[Mapping[str, Any]],
    ) -> dict[str, str]:
        node_ids = {str(row["execution_node_id"]) for row in operations}
        node_ids.update(
            str(row["destination_node_id"])
            for row in operations
            if row["operation_kind"] == "network_transfer"
        )
        return {node_id: "a" * 32 for node_id in node_ids}

    def _run(self, plan: Path, outcomes: Mapping[str, str], name: str = "run") -> tuple[dict[str, Any], FakeConditionalFlowMeshClient]:
        client = FakeConditionalFlowMeshClient(outcomes)
        result = run_flowmesh_container_conditional_trial(
            plan_dir=plan,
            output_dir=self.root / name,
            client=client,
            settings=FlowMeshSettings(worker_alias="conditional-test-worker"),
            runtime_epoch_probe=self._runtime_epoch_probe,
        )
        return result, client

    def test_hit_submits_only_phase_a_then_the_local_phase_b_branch(self) -> None:
        trial_key = _trial_key("descriptive")
        plan = self._freeze(trial_key)
        lookup_key = f"{trial_key}|lookup"
        result, client = self._run(plan, {lookup_key: "hit"})

        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(2, len(client.workflows))
        phase_a_bodies = [
            node["spec"]["api"]["body"]
            for node in client.workflows[0]["spec"]["graph"]["nodes"]
        ]
        self.assertEqual({"schedule", "lookup"}, {row["operation_id"] for row in phase_a_bodies})
        phase_b_bodies = [
            node["spec"]["api"]["body"]
            for node in client.workflows[1]["spec"]["graph"]["nodes"]
        ]
        phase_b_ids = {row["operation_id"] for row in phase_b_bodies}
        self.assertIn("read-local", phase_b_ids)
        self.assertFalse({"read-remote", "transfer-remote", "insert"} & phase_b_ids)
        verified = verify_flowmesh_container_conditional_trial_run(
            self.root / "run", plan_dir=plan
        )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertTrue(verified["phase_b_submitted"])

    def test_miss_submits_only_the_remote_insert_branch(self) -> None:
        trial_key = _trial_key("retrieval")
        plan = self._freeze(trial_key)
        lookup_key = f"{trial_key}|lookup"
        result, client = self._run(plan, {lookup_key: "miss"})

        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(2, len(client.workflows))
        phase_b_ids = {
            node["spec"]["api"]["body"]["operation_id"]
            for node in client.workflows[1]["spec"]["graph"]["nodes"]
        }
        self.assertNotIn("read-local", phase_b_ids)
        self.assertTrue({"read-remote", "transfer-remote", "insert"} <= phase_b_ids)

    def test_observed_mismatch_writes_refusal_and_never_submits_phase_b(self) -> None:
        trial_key = _trial_key("descriptive")
        plan = self._freeze(trial_key)
        lookup_key = f"{trial_key}|lookup"
        result, client = self._run(plan, {lookup_key: "miss"})

        self.assertEqual("REFUSED_CACHE_OUTCOME_MISMATCH", result["status"])
        self.assertEqual(1, len(client.workflows))
        self.assertTrue(result["phase_b_submission_refused"])
        self.assertFalse((self.root / "run" / "flowmesh-container-conditional-phase-b-submission.json").exists())
        verified = verify_flowmesh_container_conditional_trial_run(
            self.root / "run", plan_dir=plan
        )
        self.assertEqual("REFUSED_CACHE_OUTCOME_MISMATCH", verified["run_status"])
        self.assertFalse(verified["phase_b_submitted"])

    def test_worker_replacement_after_phase_a_refuses_to_submit_phase_b(self) -> None:
        trial_key = _trial_key("descriptive")
        plan = self._freeze(trial_key)
        lookup_key = f"{trial_key}|lookup"
        client = FakeConditionalFlowMeshClient(
            {lookup_key: "hit"},
            worker_changes=True,
        )
        with self.assertRaisesRegex(
            FlowMeshContainerDagError,
            "worker changed between conditional phases",
        ):
            run_flowmesh_container_conditional_trial(
                plan_dir=plan,
                output_dir=self.root / "worker-change",
                client=client,
                settings=FlowMeshSettings(worker_alias="conditional-test-worker"),
                runtime_epoch_probe=self._runtime_epoch_probe,
            )
        self.assertEqual(1, len(client.workflows))
        self.assertFalse((self.root / "worker-change").exists())

    def test_phase_a_refuses_a_result_from_an_unbound_runtime_epoch(self) -> None:
        trial_key = _trial_key("descriptive")
        plan = self._freeze(trial_key)
        lookup_key = f"{trial_key}|lookup"
        client = FakeConditionalFlowMeshClient(
            {lookup_key: "hit"},
            wrong_result_epoch=True,
        )
        with self.assertRaisesRegex(
            FlowMeshContainerDagError,
            "runtime epoch does not match the pre-submit health binding",
        ):
            run_flowmesh_container_conditional_trial(
                plan_dir=plan,
                output_dir=self.root / "wrong-epoch",
                client=client,
                settings=FlowMeshSettings(worker_alias="conditional-test-worker"),
                runtime_epoch_probe=self._runtime_epoch_probe,
            )
        self.assertEqual(1, len(client.workflows))
        self.assertFalse((self.root / "wrong-epoch").exists())

    def test_refused_cache_outcome_still_binds_pre_and_post_runtime_epochs(self) -> None:
        trial_key = _trial_key("descriptive")
        plan = self._freeze(trial_key)
        lookup_key = f"{trial_key}|lookup"
        calls: list[dict[str, str]] = []

        def probe(
            urls: Mapping[str, str],
            operations: list[Mapping[str, Any]],
        ) -> dict[str, str]:
            result = self._runtime_epoch_probe(urls, operations)
            calls.append(result)
            return result

        client = FakeConditionalFlowMeshClient({lookup_key: "miss"})
        result = run_flowmesh_container_conditional_trial(
            plan_dir=plan,
            output_dir=self.root / "refused-bound",
            client=client,
            settings=FlowMeshSettings(worker_alias="conditional-test-worker"),
            runtime_epoch_probe=probe,
        )

        self.assertEqual("REFUSED_CACHE_OUTCOME_MISMATCH", result["status"])
        self.assertEqual(2, len(calls))
        binding = result["runtime_epoch_binding"]
        self.assertTrue(binding["runtime_epoch_binding_required"])
        self.assertTrue(binding["all_runtime_epochs_stable"])
        self.assertEqual(
            binding["node_runtime_epochs_before"],
            binding["node_runtime_epochs_after"],
        )

    def test_cli_freezes_and_verifies_a_conditional_plan_offline(self) -> None:
        output = self.root / "cli-conditional-plan"
        stream = io.StringIO()
        with redirect_stdout(stream):
            code = cli_main(
                [
                    "plan-flowmesh-container-conditional-trial",
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--trial-key",
                    _trial_key("descriptive"),
                    "--smoke-id",
                    "conditional-cli-test",
                    "--output-dir",
                    str(output),
                    "--compact",
                ]
            )
        self.assertEqual(0, code)
        self.assertEqual(
            "FROZEN_CONDITIONAL_TRIAL",
            json.loads(stream.getvalue())["status"],
        )

        stream = io.StringIO()
        with redirect_stdout(stream):
            code = cli_main(
                [
                    "verify-flowmesh-container-conditional-trial-plan",
                    "--plan-dir",
                    str(output),
                    "--compact",
                ]
            )
        self.assertEqual(0, code)
        self.assertEqual("VERIFIED", json.loads(stream.getvalue())["status"])


if __name__ == "__main__":
    unittest.main()
