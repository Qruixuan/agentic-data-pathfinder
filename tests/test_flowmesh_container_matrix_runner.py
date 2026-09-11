from __future__ import annotations

import hashlib
import io
import itertools
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from itertools import count
from pathlib import Path
from typing import Any, Mapping, Sequence
from unittest import mock

from pathfinder.cli import _parser as cli_parser
from pathfinder.cli import main as cli_main
from pathfinder.integrations.flowmesh.container_conditional_dag import (
    resolve_conditional_container_trial,
)
from pathfinder.integrations.flowmesh.container_full_chain_calibration import (
    audit_flowmesh_container_full_chain_calibration,
)
from pathfinder.integrations.flowmesh.container_formal_profile import (
    freeze_flowmesh_container_formal_execution_profile,
)
from pathfinder.integrations.flowmesh.container_matrix import (
    plan_flowmesh_container_matrix,
)
from pathfinder.integrations.flowmesh.container_matrix_coordinator import (
    plan_flowmesh_container_matrix_coordinator_dry_run,
)
from pathfinder.integrations.flowmesh import (
    container_matrix_runner as matrix_runner_module,
)
from pathfinder.integrations.flowmesh.container_matrix_runner import (
    adopt_flowmesh_container_matrix_replay_results,
    build_flowmesh_container_matrix_trial_workflow,
    run_flowmesh_container_matrix,
    verify_flowmesh_container_matrix_run,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.simulator import (
    build_portable_execution_plan,
    plan_container_backend,
)
from pathfinder.simulator.container_contract import (
    CONTAINER_NODE_RESULT_SCHEMA_VERSION,
)
from tests import (
    test_flowmesh_container_full_chain_calibration as calibration_test_module,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)
WORKER_ALIAS = "matrix-runner-test-worker"
WORKER_ID = "wkr-matrix-runner"
PROFILE_ID = "formal-matrix-runner-v1"
MATRIX_ID = "flowmesh-4x8-matrix-runner-test-v1"
COORDINATOR_ID = "flowmesh-4x8-matrix-runner-coordinator-v1"
SOURCE_REVISION = "a" * 40


def _urls() -> dict[str, str]:
    return {
        f"N{number}": f"http://127.0.0.1:{19080 + number}"
        for number in range(1, 9)
    }


def _epochs(suffix: str = "stable") -> dict[str, str]:
    return {
        f"N{number}": hashlib.sha256(
            f"N{number}-{suffix}".encode("utf-8")
        ).hexdigest()[:32]
        for number in range(1, 9)
    }


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"expected a JSON object in {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _copy_tree(source: Path, destination: Path) -> Path:
    shutil.copytree(source, destination)
    return destination


def _restamp_checksums(root: Path) -> None:
    checksum_path = root / "SHA256SUMS"
    names = [
        line.partition("  ")[2]
        for line in checksum_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    checksum_path.write_text(
        "".join(
            f"{hashlib.sha256((root / name).read_bytes()).hexdigest()}  {name}\n"
            for name in names
        ),
        encoding="utf-8",
    )


def _document_digest(value: Mapping[str, Any], field: str) -> str:
    document = dict(value)
    document.pop(field, None)
    return hashlib.sha256(
        json.dumps(
            document,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _restamp_document(value: dict[str, Any], field: str) -> None:
    value[field] = _document_digest(value, field)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text(
        "".join(
            json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )


def _verify_checksum_file(root: Path) -> None:
    for line in (root / "SHA256SUMS").read_text(
        encoding="utf-8"
    ).splitlines():
        digest, separator, name = line.partition("  ")
        if separator != "  ":
            raise AssertionError("invalid SHA256SUMS row")
        actual = hashlib.sha256((root / name).read_bytes()).hexdigest()
        if actual != digest:
            raise AssertionError(f"checksum mismatch: {name}")


def _trial_key(
    workload: str,
    design: str,
    repetition: int = 0,
) -> str:
    return (
        "flowmesh-infra-4x8-local-smoke-v1|"
        f"smoke-{workload}|{design}|r{repetition:04d}"
    )


def _network_target_ms(operation: Mapping[str, Any]) -> float:
    adapter = operation.get("link_adapter")
    if not isinstance(adapter, Mapping):
        raise AssertionError("network operation has no link adapter")
    bandwidth = int(adapter["bandwidth_bytes_per_second"])
    return (
        int(operation["logical_bytes"]) / bandwidth * 1000.0
        + float(adapter["round_trip_time_ms"])
    )


class _EpochProbe:
    def __init__(self, epochs: Mapping[str, str] | None = None) -> None:
        self.current = dict(epochs or _epochs())
        self.calls: list[tuple[str, ...]] = []

    def __call__(
        self,
        _node_api_urls: Mapping[str, str],
        operations: Sequence[Mapping[str, Any]],
    ) -> dict[str, str]:
        referenced = {
            str(operation["execution_node_id"]) for operation in operations
        }
        referenced.update(
            str(operation["destination_node_id"])
            for operation in operations
            if operation["operation_kind"] == "network_transfer"
        )
        self.calls.append(tuple(sorted(referenced)))
        return {node_id: self.current[node_id] for node_id in referenced}


class FakeMatrixFlowMeshClient:
    """Stateful offline double for arbitrary trial graphs and two phases."""

    _workflow_numbers = count(1)

    _instance_numbers = itertools.count(1)

    def __init__(
        self,
        *,
        epochs: Mapping[str, str] | None = None,
        cache_outcomes: Mapping[str, str] | None = None,
        worker_id: str = WORKER_ID,
        worker_status: str = "IDLE",
        interrupt_before_trial: str | None = None,
        interrupt_submit_trial: str | None = None,
        interrupt_wait_trial: str | None = None,
        terminal_failure_trial: str | None = None,
        recoverable_terminal_failure_trial: str | None = None,
        recoverable_primary_task_index: int = 0,
        recoverable_dispatched_task_ids: tuple[str, ...] | None = (),
        replay_operation_key: str | None = None,
        wrong_epoch_operation_key: str | None = None,
        wrong_assigned_worker_operation_key: str | None = None,
    ) -> None:
        self.instance_number = next(self._instance_numbers)
        self.epochs = dict(epochs or _epochs())
        self.cache_outcomes = dict(cache_outcomes or {})
        self.worker_id = worker_id
        self.worker_status = worker_status
        self.interrupt_before_trial = interrupt_before_trial
        self.interrupt_submit_trial = interrupt_submit_trial
        self.interrupt_wait_trial = interrupt_wait_trial
        self.terminal_failure_trial = terminal_failure_trial
        self.recoverable_terminal_failure_trial = (
            recoverable_terminal_failure_trial
        )
        self.recoverable_primary_task_index = recoverable_primary_task_index
        self.recoverable_dispatched_task_ids = (
            recoverable_dispatched_task_ids
        )
        self.replay_operation_key = replay_operation_key
        self.wrong_epoch_operation_key = wrong_epoch_operation_key
        self.wrong_assigned_worker_operation_key = (
            wrong_assigned_worker_operation_key
        )
        self.validated: list[dict[str, Any]] = []
        self.workflows: list[dict[str, Any]] = []
        self.results: dict[str, dict[str, Any]] = {}
        self.task_workers: dict[str, str] = {}
        self.workflow_trials: dict[str, str] = {}
        self.workflow_task_ids: dict[str, tuple[str, ...]] = {}
        self.recoverable_terminals: dict[str, TerminalWorkflow] = {}
        self.failure_details: dict[str, dict[str, Any]] = {}
        self.submit_attempts: list[str] = []
        self.wait_calls: list[str] = []
        self.retrieve_calls: list[str] = []
        self.active_workflow: str | None = None
        self.maximum_active_workflows = 0
        self._interrupted = False
        self._submit_interrupted = False
        self._wait_interrupted = False
        self._recoverable_failure_emitted = False

    @staticmethod
    def _workflow_operations(
        workflow: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        return [
            dict(node["spec"]["api"]["body"])
            for node in workflow["spec"]["graph"]["nodes"]
        ]

    def describe_current_worker(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> FlowMeshWorkerIdentity:
        if alias is not None:
            if alias != WORKER_ALIAS or worker_id is not None:
                raise RuntimeError("unexpected worker alias lookup")
        elif worker_id != self.worker_id:
            raise RuntimeError("worker is no longer current")
        return FlowMeshWorkerIdentity(
            worker_id=self.worker_id,
            alias=WORKER_ALIAS,
            status=self.worker_status,
            namespace="test",
            cluster="test",
            node_alias="test-node",
        )

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        copied = json.loads(json.dumps(workflow))
        operations = self._workflow_operations(copied)
        trial_key = str(operations[0]["trial_key"])
        if (
            self.interrupt_before_trial == trial_key
            and not self._interrupted
        ):
            self._interrupted = True
            raise KeyboardInterrupt("injected clean-boundary interruption")
        self.validated.append(copied)
        return WorkflowValidation(ok=True)

    def _body(self, operation: Mapping[str, Any]) -> dict[str, Any]:
        kind = str(operation["operation_kind"])
        operation_key = str(operation["operation_key"])
        logical_bytes = int(operation["logical_bytes"])
        physical_bytes = (
            logical_bytes
            if kind in {"storage_read", "cache_read", "network_transfer"}
            else 0
        )
        shaping_target = (
            _network_target_ms(operation)
            if kind == "network_transfer"
            else None
        )
        exchange_ms = 0.1 if kind == "network_transfer" else None
        shaping_sleep_ms = shaping_target if kind == "network_transfer" else None
        service_time_ms = (
            float(shaping_target) + 1.0
            if shaping_target is not None
            else 1.0
        )
        started_ns = 1_000_000_000
        elapsed_ns = int(round(service_time_ms * 1_000_000))
        service_time_ms = elapsed_ns / 1_000_000.0
        source_node = str(operation["execution_node_id"])
        destination_node = str(operation["destination_node_id"])
        cache_result = (
            self.cache_outcomes.get(operation_key)
            if kind == "cache_lookup"
            else ("hit" if kind == "cache_read" else None)
        )
        body = {
            "schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
            "api_version": "pathfinder.container-node/v1alpha1",
            "status": "completed",
            "outcome_type": "completed",
            "telemetry_complete": True,
            "credentials_recorded": False,
            "idempotent_replay": operation_key
            == self.replay_operation_key,
            "operation_key": operation_key,
            "operation_kind": kind,
            "execution_node_id": source_node,
            "runtime_epoch": (
                "f" * 32
                if operation_key == self.wrong_epoch_operation_key
                else self.epochs[source_node]
            ),
            "destination_runtime_epoch": (
                self.epochs[destination_node]
                if kind == "network_transfer"
                else None
            ),
            "started_monotonic_ns": started_ns,
            "finished_monotonic_ns": started_ns + elapsed_ns,
            "service_time_ms": service_time_ms,
            "fixture_materialization_ms_excluded_from_storage_measurement": 0.0,
            "application_shaping_target_ms": shaping_target,
            "network_http_exchange_ms": exchange_ms,
            "application_shaping_sleep_ms": shaping_sleep_ms,
            "logical_bytes": logical_bytes,
            "physical_bytes": physical_bytes,
            "payload_sha256": None,
            "cache_result": cache_result,
            "cache_evictions": [],
            "cache_scope_id": (
                operation.get("cache_scope_id")
                if kind in {"cache_lookup", "cache_read", "cache_insert"}
                else None
            ),
            "infrastructure_operation_success": True,
            "semantic_task_quality_evaluated": False,
        }
        return body

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        if self.active_workflow is not None:
            raise AssertionError("matrix runner submitted concurrent workflows")
        copied = json.loads(json.dumps(workflow))
        operations = self._workflow_operations(copied)
        trial_key = str(operations[0]["trial_key"])
        self.submit_attempts.append(trial_key)
        if (
            self.interrupt_submit_trial == trial_key
            and not self._submit_interrupted
        ):
            self._submit_interrupted = True
            raise KeyboardInterrupt("injected ambiguous submission window")
        workflow_number = next(self._workflow_numbers)
        workflow_id = (
            f"wfl-matrix-{self.instance_number:03d}-{workflow_number:03d}"
        )
        task_ids = tuple(
            f"tsk-matrix-{self.instance_number:03d}-"
            f"{workflow_number:03d}-{index:03d}"
            for index in range(1, len(operations) + 1)
        )
        self.workflows.append(copied)
        self.workflow_trials[workflow_id] = trial_key
        self.workflow_task_ids[workflow_id] = task_ids
        self.active_workflow = workflow_id
        self.maximum_active_workflows = max(
            self.maximum_active_workflows,
            1 if self.active_workflow is not None else 0,
        )
        for task_id, operation in zip(task_ids, operations):
            body = self._body(operation)
            self.results[task_id] = {
                "executor": "api",
                "ok": True,
                "status_code": 200,
                "text": json.dumps(body, sort_keys=True),
            }
            assigned = (
                "wkr-wrong"
                if operation["operation_key"]
                == self.wrong_assigned_worker_operation_key
                else self.worker_id
            )
            self.task_workers[task_id] = assigned
        return SubmittedWorkflow(workflow_id, task_ids)

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        self.wait_calls.append(workflow_id)
        if workflow_id in self.recoverable_terminals:
            return self.recoverable_terminals[workflow_id]
        if workflow_id != self.active_workflow:
            raise AssertionError("wait did not target the active workflow")
        trial_key = self.workflow_trials[workflow_id]
        if (
            self.interrupt_wait_trial == trial_key
            and not self._wait_interrupted
        ):
            self._wait_interrupted = True
            raise KeyboardInterrupt("injected bound-workflow interruption")
        self.active_workflow = None
        if trial_key == self.terminal_failure_trial:
            return TerminalWorkflow(
                workflow_id,
                "FAILED",
                detail="Bearer secret-must-be-redacted",
            )
        if (
            trial_key == self.recoverable_terminal_failure_trial
            and not self._recoverable_failure_emitted
        ):
            self._recoverable_failure_emitted = True
            task_ids = self.workflow_task_ids[workflow_id]
            primary_index = self.recoverable_primary_task_index
            primary = task_ids[primary_index]
            failed_ids = [primary]
            self.failure_details[primary] = {
                "task_status": "FAILED",
                "attempts": 1,
                "max_attempts": 3,
                "assigned_worker": self.worker_id,
                "last_failed_worker": self.worker_id,
                "detail": (
                    f"HTTP delivery for task {primary} returned status 503: "
                    '{"detail":"Identity provider unavailable"}'
                ),
            }
            for index, task_id in enumerate(task_ids):
                if task_id == primary:
                    continue
                if index == primary_index + 1:
                    failed_ids.append(task_id)
                    self.failure_details[task_id] = {
                        "task_status": "FAILED",
                        "attempts": 0,
                        "max_attempts": 3,
                        "assigned_worker": None,
                        "last_failed_worker": None,
                        "detail": f"Dependency {primary} failed",
                    }
                else:
                    self.failure_details[task_id] = {
                        "task_status": "PENDING",
                        "attempts": 0,
                        "max_attempts": 3,
                        "assigned_worker": None,
                        "last_failed_worker": None,
                        "detail": None,
                    }
            terminal = TerminalWorkflow(
                workflow_id=workflow_id,
                status="FAILED",
                failed_task_ids=tuple(failed_ids),
                cancelled_task_ids=(),
                detail="root-reported identity-provider failure",
                dispatched_task_ids=self.recoverable_dispatched_task_ids,
            )
            self.recoverable_terminals[workflow_id] = terminal
            return terminal
        return TerminalWorkflow(workflow_id, "DONE")

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        self.retrieve_calls.append(task_id)
        return self.results[task_id]

    def describe_task_failure(self, task_id: str) -> dict[str, Any]:
        if task_id in self.failure_details:
            return dict(self.failure_details[task_id])
        return {
            "task_status": "DONE",
            "assigned_worker": self.task_workers[task_id],
        }


class FlowMeshContainerMatrixRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture_tmp = tempfile.TemporaryDirectory()
        root = Path(cls.fixture_tmp.name)
        cls.fixture_root = root
        cls.portable = root / "portable"
        cls.container = root / "container"
        cls.matrix = root / "matrix"
        cls.audit = root / "audit"
        cls.profile = root / "profile"
        cls.coordinator = root / "coordinator"
        build_portable_execution_plan(SCENARIO, output_dir=cls.portable)
        plan_container_backend(
            SCENARIO,
            cls.portable,
            CONTAINER_SPEC,
            output_dir=cls.container,
        )
        plan_flowmesh_container_matrix(
            portable_plan_dir=cls.portable,
            container_plan_dir=cls.container,
            node_api_urls=_urls(),
            worker_alias=WORKER_ALIAS,
            matrix_id=MATRIX_ID,
            source_git_revision=SOURCE_REVISION,
            execution_profile_id=PROFILE_ID,
            api_task_timeout_seconds=900,
            output_dir=cls.matrix,
        )
        calibration_fixture = calibration_test_module.FullChainCalibrationAuditTest(
            "runTest"
        )
        calibration_fixture.setUp()
        try:
            fast_plan, fast_run, slow_plan, slow_run = (
                calibration_fixture._make_pair()
            )
            audit_flowmesh_container_full_chain_calibration(
                fast_plan_dir=fast_plan,
                fast_run_dir=fast_run,
                slow_plan_dir=slow_plan,
                slow_run_dir=slow_run,
                output_dir=cls.audit,
            )
        finally:
            calibration_fixture.temporary.cleanup()
        freeze_flowmesh_container_formal_execution_profile(
            matrix_plan_dir=cls.matrix,
            calibration_audit_dir=cls.audit,
            execution_profile_id=PROFILE_ID,
            primary_trial_wrapper_max_concurrency=1,
            output_dir=cls.profile,
        )
        plan_flowmesh_container_matrix_coordinator_dry_run(
            matrix_plan_dir=cls.matrix,
            formal_execution_profile_dir=cls.profile,
            coordinator_id=COORDINATOR_ID,
            output_dir=cls.coordinator,
        )
        cls.operations = _read_jsonl(
            cls.matrix / "flowmesh-container-matrix-operations.jsonl"
        )
        cls.trials = _read_jsonl(
            cls.matrix / "flowmesh-container-matrix-trials.jsonl"
        )
        cls.wrappers = _read_jsonl(
            cls.coordinator
            / "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl"
        )
        cls.cache_outcomes: dict[str, str] = {}
        conditional_trials = [
            trial
            for trial in cls.trials
            if trial["design_id"] in {"D3", "D7"}
        ]
        for trial in conditional_trials:
            resolved = resolve_conditional_container_trial(
                cls.operations,
                trial_key=trial["trial_key"],
            )
            cls.cache_outcomes.update(resolved["cache_outcomes"])
        cls.golden_run = root / "golden-run"
        golden_settings = FlowMeshSettings(
            base_url="https://flowmesh.test/fm/root-a",
            worker_alias=WORKER_ALIAS,
            validate_before_submit=True,
            poll_interval_seconds=0.01,
        )
        golden_client = FakeMatrixFlowMeshClient(
            cache_outcomes=cls.cache_outcomes
        )
        run_flowmesh_container_matrix(
            matrix_plan_dir=cls.matrix,
            formal_execution_profile_dir=cls.profile,
            coordinator_plan_dir=cls.coordinator,
            output_dir=cls.golden_run,
            run_id="formal-matrix-runner-golden-v1",
            client=golden_client,
            settings=golden_settings,
            runtime_epoch_probe=_EpochProbe(golden_client.epochs),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.fixture_tmp.cleanup()

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.settings = FlowMeshSettings(
            worker_alias=WORKER_ALIAS,
            validate_before_submit=True,
            poll_interval_seconds=0.01,
        )

    def _run(
        self,
        client: FakeMatrixFlowMeshClient,
        *,
        output: Path | None = None,
        probe: _EpochProbe | None = None,
        run_id: str = "formal-matrix-runner-test-v1",
        recovery_id: str | None = None,
        recovery_reason: str | None = None,
        recover_failed_entry_sha256: str | None = None,
    ) -> dict[str, Any]:
        return run_flowmesh_container_matrix(
            matrix_plan_dir=self.matrix,
            formal_execution_profile_dir=self.profile,
            coordinator_plan_dir=self.coordinator,
            output_dir=output or self.root / "run",
            run_id=run_id,
            client=client,
            settings=self.settings,
            runtime_epoch_probe=probe or _EpochProbe(client.epochs),
            recovery_id=recovery_id,
            recovery_reason=recovery_reason,
            recover_failed_entry_sha256=recover_failed_entry_sha256,
        )

    def _completed_run_copy(self, name: str) -> Path:
        return _copy_tree(self.golden_run, self.root / name)

    def _fresh_recovery_client(
        self,
        failed_client: FakeMatrixFlowMeshClient,
    ) -> FakeMatrixFlowMeshClient:
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        client.recoverable_terminals = dict(
            failed_client.recoverable_terminals
        )
        client.failure_details = {
            key: dict(value)
            for key, value in failed_client.failure_details.items()
        }
        return client

    def _replay_failed_recovery(
        self,
        *,
        output: Path,
        replay_physical_operation: bool = False,
    ) -> tuple[FakeMatrixFlowMeshClient, str, str]:
        target_wrapper = next(
            row
            for row in self.wrappers
            if row["design_id"] not in {"D3", "D7"}
        )
        target_trial = str(target_wrapper["trial_key"])
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=target_trial,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)
        first_failure = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]

        target_operations = [
            row for row in self.operations if row["trial_key"] == target_trial
        ]
        if replay_physical_operation:
            replay_key = next(
                str(row["operation_key"])
                for row in target_operations
                if row["operation_kind"]
                in {"storage_read", "cache_read", "network_transfer"}
            )
        else:
            replay_key = next(
                str(row["operation_key"])
                for row in target_operations
                if row["operation_id"] == "schedule"
            )
        recovery_client = self._fresh_recovery_client(first_client)
        recovery_client.replay_operation_key = replay_key
        with self.assertRaisesRegex(Exception, "operation was replayed"):
            self._run(
                recovery_client,
                output=output,
                recovery_id="idp-recovery-replay-adoption-test",
                recovery_reason="Authorize the one infrastructure retry.",
                recover_failed_entry_sha256=first_failure,
            )
        journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        second_failure = str(journal[-1]["entry_sha256"])
        bound = journal[-2]["payload"]
        recovery_client.recoverable_terminals[str(bound["workflow_id"])] = (
            TerminalWorkflow(
                workflow_id=str(bound["workflow_id"]),
                status="DONE",
                dispatched_task_ids=(),
            )
        )
        return recovery_client, second_failure, target_trial

    @staticmethod
    def _workflow_operation_keys(
        client: FakeMatrixFlowMeshClient,
    ) -> list[str]:
        return [
            operation["operation_key"]
            for workflow in client.workflows
            for operation in client._workflow_operations(workflow)
        ]

    def test_unconditional_builder_preserves_parallel_and_index_dags(self) -> None:
        for trial_key, expected_count in (
            (_trial_key("causal", "D2"), 8),
            (_trial_key("retrieval", "D2"), 9),
        ):
            operations = [
                row for row in self.operations if row["trial_key"] == trial_key
            ]
            dependencies = {
                row["operation_key"]: list(row["dependency_operation_keys"])
                for row in operations
            }
            workflow = build_flowmesh_container_matrix_trial_workflow(
                operations,
                dependencies=dependencies,
                node_api_urls=_urls(),
                selected_worker_id=WORKER_ID,
                run_id="builder-test-v1",
                trial_key=trial_key,
                phase="unconditional",
                owner="pathfinder",
                api_task_timeout_seconds=900,
            )
            nodes = workflow["spec"]["graph"]["nodes"]
            self.assertEqual(expected_count, len(nodes))
            by_key = {
                node["spec"]["api"]["body"]["operation_key"]: node
                for node in nodes
            }
            self.assertEqual(
                {row["operation_key"] for row in operations},
                set(by_key),
            )
            name_by_key = {
                operation_key: node["name"]
                for operation_key, node in by_key.items()
            }
            for operation in operations:
                node = by_key[operation["operation_key"]]
                expected_dependencies = [
                    name_by_key[key]
                    for key in operation["dependency_operation_keys"]
                ]
                self.assertEqual(
                    expected_dependencies,
                    node.get("dependsOn", []),
                )
                self.assertEqual(
                    operation,
                    node["spec"]["api"]["body"],
                )
                self.assertEqual(900, node["spec"]["api"]["timeout_sec"])
            if "|smoke-causal|" in trial_key:
                infer = next(
                    row for row in operations if row["operation_id"] == "infer"
                )
                self.assertEqual(2, len(infer["dependency_operation_keys"]))
            else:
                self.assertTrue(
                    any(row["operation_kind"] == "index_query" for row in operations)
                )

    def test_matrix_plan_rejects_node_urls_with_query_or_fragment(self) -> None:
        for label, suffix in (("query", "?route=alternate"), ("fragment", "#n1")):
            with self.subTest(label=label):
                urls = _urls()
                urls["N1"] += suffix
                with self.assertRaisesRegex(Exception, "(?i)(query|fragment)"):
                    plan_flowmesh_container_matrix(
                        portable_plan_dir=self.portable,
                        container_plan_dir=self.container,
                        node_api_urls=urls,
                        worker_alias=WORKER_ALIAS,
                        matrix_id=f"matrix-invalid-url-{label}",
                        source_git_revision=SOURCE_REVISION,
                        execution_profile_id=PROFILE_ID,
                        api_task_timeout_seconds=900,
                        output_dir=self.root / f"matrix-invalid-url-{label}",
                    )

    def test_full_64_trial_run_has_exact_workflow_and_operation_coverage(self) -> None:
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        result = self._run(client)

        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(64, result["completed_trial_count"])
        self.assertEqual(80, len(client.workflows))
        executed_keys = self._workflow_operation_keys(client)
        self.assertEqual(472, len(executed_keys))
        self.assertEqual(472, len(set(executed_keys)))
        self.assertEqual(1, client.maximum_active_workflows)

        workflow_trials = [
            client._workflow_operations(workflow)[0]["trial_key"]
            for workflow in client.workflows
        ]
        collapsed_trials = [
            key
            for index, key in enumerate(workflow_trials)
            if index == 0 or key != workflow_trials[index - 1]
        ]
        self.assertEqual(
            [row["trial_key"] for row in self.wrappers],
            collapsed_trials,
        )

        run_root = self.root / "run"
        _verify_checksum_file(run_root)
        operation_rows = _read_jsonl(
            run_root / "flowmesh-container-matrix-operation-results.jsonl"
        )
        self.assertEqual(500, len(operation_rows))
        self.assertEqual(472, sum(row["executed"] is True for row in operation_rows))
        inactive = [row for row in operation_rows if row["executed"] is False]
        self.assertEqual(28, len(inactive))
        self.assertTrue(
            all(row["skip_reason"] == "inactive-conditional-branch" for row in inactive)
        )
        self.assertEqual(
            "VERIFIED",
            verify_flowmesh_container_matrix_run(run_root)["status"],
        )

    def test_conditional_wrappers_submit_only_the_frozen_hit_or_miss_branch(
        self,
    ) -> None:
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        self._run(client)
        by_trial: dict[str, list[dict[str, Any]]] = {}
        for workflow in client.workflows:
            rows = client._workflow_operations(workflow)
            by_trial.setdefault(rows[0]["trial_key"], []).extend(rows)

        hit_key = _trial_key("descriptive", "D3")
        hit_ids = {row["operation_id"] for row in by_trial[hit_key]}
        self.assertIn("read-local", hit_ids)
        self.assertFalse({"read-remote", "transfer-remote", "insert"} & hit_ids)

        miss_key = _trial_key("retrieval", "D3")
        miss_ids = {row["operation_id"] for row in by_trial[miss_key]}
        self.assertNotIn("read-local", miss_ids)
        self.assertTrue({"read-remote", "transfer-remote", "insert"} <= miss_ids)

        causal_key = _trial_key("causal", "D3")
        causal_ids = {row["operation_id"] for row in by_trial[causal_key]}
        self.assertFalse(
            {"read-local-digest", "read-local-frames"} & causal_ids
        )
        self.assertTrue(
            {
                "read-remote-digest",
                "insert-digest",
                "read-remote-frames",
                "insert-frames",
            }
            <= causal_ids
        )

    def test_replay_result_fails_closed_and_stops_later_trials(self) -> None:
        target_trial = self.wrappers[2]["trial_key"]
        target_operation = next(
            row["operation_key"]
            for row in self.operations
            if row["trial_key"] == target_trial
        )
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            replay_operation_key=target_operation,
        )
        with self.assertRaisesRegex(Exception, "replayed"):
            self._run(client)

        submitted_trials = {
            workflow["spec"]["graph"]["nodes"][0]["spec"]["api"]["body"][
                "trial_key"
            ]
            for workflow in client.workflows
        }
        self.assertIn(target_trial, submitted_trials)
        self.assertNotIn(self.wrappers[3]["trial_key"], submitted_trials)
        failure = _read_json(
            self.root / "run" / "flowmesh-container-matrix-failure.json"
        )
        self.assertEqual(target_trial, failure["trial_key"])
        self.assertIn(target_operation, json.dumps(failure))
        self.assertNotIn(
            target_trial,
            {
                row["trial_key"]
                for row in _read_jsonl(
                    self.root
                    / "run"
                    / "flowmesh-container-matrix-trial-checkpoints.jsonl"
                )
            },
        )

    def test_wrong_worker_assignment_fails_before_next_wrapper(self) -> None:
        target_trial = self.wrappers[1]["trial_key"]
        target_operation = next(
            row["operation_key"]
            for row in self.operations
            if row["trial_key"] == target_trial
        )
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            wrong_assigned_worker_operation_key=target_operation,
        )
        with self.assertRaisesRegex(Exception, "worker"):
            self._run(client)
        submitted = {
            client._workflow_operations(workflow)[0]["trial_key"]
            for workflow in client.workflows
        }
        self.assertNotIn(self.wrappers[2]["trial_key"], submitted)

    def test_wrong_result_epoch_fails_before_next_wrapper(self) -> None:
        target_trial = self.wrappers[1]["trial_key"]
        target_operation = next(
            row["operation_key"]
            for row in self.operations
            if row["trial_key"] == target_trial
        )
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            wrong_epoch_operation_key=target_operation,
        )
        with self.assertRaisesRegex(Exception, "runtime epoch"):
            self._run(client)
        submitted = {
            client._workflow_operations(workflow)[0]["trial_key"]
            for workflow in client.workflows
        }
        self.assertNotIn(self.wrappers[2]["trial_key"], submitted)

    def test_terminal_failure_is_redacted_and_preserves_completed_prefix(self) -> None:
        target_trial = self.wrappers[2]["trial_key"]
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            terminal_failure_trial=target_trial,
        )
        with self.assertRaises(Exception) as context:
            self._run(client)
        self.assertNotIn("secret-must-be-redacted", str(context.exception))
        failure_path = (
            self.root / "run" / "flowmesh-container-matrix-failure.json"
        )
        failure_text = failure_path.read_text(encoding="utf-8")
        self.assertNotIn("secret-must-be-redacted", failure_text)
        self.assertEqual(target_trial, _read_json(failure_path)["trial_key"])
        checkpoints = _read_jsonl(
            self.root
            / "run"
            / "flowmesh-container-matrix-trial-checkpoints.jsonl"
        )
        self.assertEqual(
            [row["trial_key"] for row in self.wrappers[:2]],
            [row["trial_key"] for row in checkpoints],
        )

    def test_explicit_identity_provider_recovery_continues_same_run(
        self,
    ) -> None:
        target_wrapper = next(
            row
            for row in self.wrappers[2:]
            if row["design_id"] not in {"D3", "D7"}
        )
        target_trial = target_wrapper["trial_key"]
        output = self.root / "recover-idp-failure"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=target_trial,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)

        failure_path = (
            output / "flowmesh-container-matrix-failure.json"
        )
        original_failure_bytes = failure_path.read_bytes()
        failed_journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        self.assertEqual("RUN_FAILED", failed_journal[-1]["state"])
        failed_entry_sha256 = failed_journal[-1]["entry_sha256"]
        failed_workflow_id = next(
            reversed(first_client.recoverable_terminals)
        )

        recovery_client = self._fresh_recovery_client(first_client)
        recovery_reason = "environment-sensitive-verification-sentinel"
        with mock.patch.dict(os.environ, {"FLOWMESH_API_KEY": ""}):
            result = self._run(
                recovery_client,
                output=output,
                recovery_id="idp-recovery-001",
                recovery_reason=recovery_reason,
                recover_failed_entry_sha256=failed_entry_sha256,
            )

        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(1, result["infrastructure_recovery_count"])
        self.assertEqual(1, result["abandoned_workflow_count"])
        self.assertEqual(80, result["canonical_workflow_count"])
        self.assertEqual(81, result["flowmesh_workflow_count"])
        implementation_bindings = result[
            "recovery_runner_module_sha256_by_recovery_id"
        ]
        self.assertEqual({"idp-recovery-001"}, set(implementation_bindings))
        self.assertRegex(
            implementation_bindings["idp-recovery-001"],
            r"^[0-9a-f]{64}$",
        )
        self.assertEqual(original_failure_bytes, failure_path.read_bytes())
        self.assertNotIn(
            failed_workflow_id,
            {
                row["workflow_id"]
                for row in _read_jsonl(
                    output / "flowmesh-container-matrix-submissions.jsonl"
                )
            },
        )
        self.assertEqual(
            target_trial,
            recovery_client.submit_attempts[0],
        )
        earlier_trials = {
            row["trial_key"]
            for row in self.wrappers[: target_wrapper["sequence_index"]]
        }
        self.assertTrue(
            earlier_trials.isdisjoint(recovery_client.submit_attempts)
        )
        checksums = (
            output / "SHA256SUMS"
        ).read_text(encoding="utf-8")
        self.assertIn("flowmesh-container-matrix-failure.json", checksums)
        journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        failure_index = next(
            index
            for index, row in enumerate(journal)
            if row["entry_sha256"] == failed_entry_sha256
        )
        self.assertEqual(
            "INFRASTRUCTURE_RECOVERY_AUTHORIZED",
            journal[failure_index + 1]["state"],
        )
        authorization = journal[failure_index + 1]
        recovery_payload = authorization["payload"]
        self.assertEqual(
            "pathfinder.flowmesh-container-matrix-infrastructure-recovery/"
            "v1alpha2",
            recovery_payload["recovery_implementation_schema"],
        )
        self.assertEqual(
            "flowmesh-identity-provider-unavailable-at-safe-schedule-root",
            recovery_payload["failure_class"],
        )
        self.assertEqual(
            "non-historical-diagnostic-only",
            recovery_payload["root_dispatch_history_interpretation"],
        )
        safety = recovery_payload["schedule_root_safety_evidence"]
        self.assertEqual(
            recovery_payload["bound_task_ids"],
            [row["task_id"] for row in safety["task_to_operation_bindings"]],
        )
        self.assertEqual(
            safety["phase_operation_keys"],
            [
                row["operation_key"]
                for row in safety["frozen_phase_operations"]
            ],
        )
        self.assertEqual(
            "schedule",
            safety["schedule_operation_evidence"]["operation_id"],
        )
        self.assertTrue(
            safety["all_other_phase_operations_transitively_downstream"]
        )

        # Historical v1alpha1 authorization entries remain readable by the
        # offline journal verifier even though new authorizations are v1alpha2.
        legacy_journal = json.loads(json.dumps(journal))
        legacy_authorization = legacy_journal[failure_index + 1]
        legacy_payload = legacy_authorization["payload"]
        legacy_payload.pop("root_dispatch_history_interpretation")
        legacy_payload.pop("schedule_root_safety_evidence")
        legacy_payload["failure_class"] = (
            "flowmesh-identity-provider-unavailable-before-dispatch"
        )
        legacy_payload["recovery_implementation_schema"] = (
            "pathfinder.flowmesh-container-matrix-infrastructure-recovery/"
            "v1alpha1"
        )
        legacy_authorization["entry_sha256"] = (
            matrix_runner_module._entry_sha256(
                legacy_authorization, "entry_sha256"
            )
        )
        contract = _read_json(
            output / "flowmesh-container-matrix-run-contract.json"
        )
        checkpoints = _read_jsonl(
            output / "flowmesh-container-matrix-trial-checkpoints.jsonl"
        )
        sources = matrix_runner_module._load_sources(
            self.matrix, self.profile, self.coordinator
        )
        matrix_runner_module._validate_journal(
            legacy_journal,
            contract,
            checkpoints,
            sources=sources,
            failure_document=_read_json(failure_path),
            require_complete=True,
        )
        # Offline verification must depend only on frozen evidence.  A later
        # process may have different credential-shaped environment values,
        # which must not cause stored recovery prose to be re-redacted and
        # rejected.
        with mock.patch.dict(
            os.environ,
            {"FLOWMESH_API_KEY": recovery_reason},
        ):
            verified = verify_flowmesh_container_matrix_run(
                output,
                matrix_plan_dir=self.matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
            )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(1, verified["infrastructure_recovery_count"])

    def test_durable_failure_requires_exact_explicit_authorization(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers[1:]
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "recovery-authorization"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=target_trial,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)
        failed_digest = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]

        without_authorization = self._fresh_recovery_client(first_client)
        with self.assertRaisesRegex(Exception, "explicit audited recovery"):
            self._run(without_authorization, output=output)
        self.assertEqual([], without_authorization.wait_calls)
        self.assertEqual([], without_authorization.workflows)

        wrong_digest_client = self._fresh_recovery_client(first_client)
        probe = _EpochProbe(wrong_digest_client.epochs)
        with self.assertRaisesRegex(Exception, "does not bind"):
            self._run(
                wrong_digest_client,
                output=output,
                probe=probe,
                recovery_id="idp-recovery-wrong-digest",
                recovery_reason="Attempt with a deliberately wrong digest.",
                recover_failed_entry_sha256="0" * 64,
            )
        self.assertNotEqual("0" * 64, failed_digest)
        self.assertEqual([], probe.calls)
        self.assertEqual([], wrong_digest_client.wait_calls)
        self.assertEqual([], wrong_digest_client.workflows)

    def test_adopts_one_schedule_replay_without_resubmission(self) -> None:
        output = self.root / "schedule-replay-adoption"
        client, failed_digest, target_trial = self._replay_failed_recovery(
            output=output
        )
        validation_count = len(client.validated)
        workflow_count = len(client.workflows)
        submit_attempts = list(client.submit_attempts)

        adopted = adopt_flowmesh_container_matrix_replay_results(
            matrix_plan_dir=self.matrix,
            formal_execution_profile_dir=self.profile,
            coordinator_plan_dir=self.coordinator,
            run_dir=output,
            run_id="formal-matrix-runner-test-v1",
            client=client,
            settings=self.settings,
            adoption_id="schedule-replay-adoption-001",
            adoption_reason=(
                "Adopt the exact same-epoch zero-byte schedule result only."
            ),
            adopt_failed_entry_sha256=failed_digest,
            runtime_epoch_probe=_EpochProbe(client.epochs),
        )
        self.assertEqual("REPLAY_RESULTS_ADOPTED", adopted["status"])
        self.assertEqual(target_trial, adopted["trial_key"])
        self.assertFalse(adopted["workflow_submitted"])
        self.assertFalse(adopted["workflow_validated"])
        self.assertEqual(validation_count, len(client.validated))
        self.assertEqual(workflow_count, len(client.workflows))
        self.assertEqual(submit_attempts, client.submit_attempts)

        checkpoint = _read_jsonl(
            output / "flowmesh-container-matrix-trial-checkpoints.jsonl"
        )[0]
        trial = checkpoint["trial_result"]
        replayed = [
            row
            for row in checkpoint["operation_results"]
            if row.get("idempotent_replay") is True
        ]
        self.assertEqual(1, len(replayed))
        self.assertEqual("schedule", replayed[0]["operation_id"])
        self.assertEqual(
            trial["executed_operation_count"],
            trial["telemetry"]["record_count"],
        )
        self.assertEqual(
            trial["executed_operation_count"] - 1,
            trial["non_replayed_result_telemetry"]["record_count"],
        )
        self.assertFalse(replayed[0]["original_flowmesh_task_id_known"])
        self.assertFalse(replayed[0]["original_flowmesh_workflow_id_known"])
        self.assertFalse(replayed[0]["measurement_freshness_established"])
        self.assertEqual(replayed[0]["task_id"], replayed[0]["result_carrier_task_id"])
        self.assertEqual(
            checkpoint["submissions"][0]["workflow_id"],
            replayed[0]["result_carrier_workflow_id"],
        )
        self.assertEqual(
            "pathfinder.flowmesh-container-matrix-operation-result/v1alpha2",
            replayed[0]["schema_version"],
        )
        self.assertEqual(
            "container-node-idempotency-ledger",
            replayed[0]["measurement_origin"],
        )

        resumed = self._run(
            FakeMatrixFlowMeshClient(cache_outcomes=self.cache_outcomes),
            output=output,
        )
        self.assertEqual("COMPLETE", resumed["status"])
        self.assertEqual(1, resumed["replay_result_adoption_count"])
        self.assertEqual(1, resumed["adopted_replay_operation_count"])
        self.assertTrue(
            resumed["adopted_prior_node_measurements_in_canonical_results"]
        )
        self.assertEqual(
            "VERIFIED",
            verify_flowmesh_container_matrix_run(
                output,
                matrix_plan_dir=self.matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
            )["status"],
        )

    def test_replay_adoption_resumes_every_durable_prefix_without_submit(
        self,
    ) -> None:
        for stage in ("adoption", "results", "checkpoint", "completed"):
            with self.subTest(stage=stage):
                output = self.root / f"adoption-crash-{stage}"
                client, failed_digest, _trial = self._replay_failed_recovery(
                    output=output
                )
                real_journal_entry = matrix_runner_module._journal_entry
                real_append = matrix_runner_module._append_digest_entry

                def interrupted_journal(*args: Any, **kwargs: Any) -> Any:
                    row = real_journal_entry(*args, **kwargs)
                    state = kwargs.get("state")
                    if (
                        stage == "adoption"
                        and state == "REPLAY_RESULTS_ADOPTION_AUTHORIZED"
                    ) or (
                        stage == "results" and state == "RESULTS_OBTAINED"
                    ) or (
                        stage == "completed" and state == "TRIAL_COMPLETED"
                    ):
                        raise KeyboardInterrupt(
                            f"injected crash after durable {stage} append"
                        )
                    return row

                def interrupted_append(*args: Any, **kwargs: Any) -> Any:
                    row = real_append(*args, **kwargs)
                    path = Path(args[0])
                    if (
                        stage == "checkpoint"
                        and path.name
                        == "flowmesh-container-matrix-trial-checkpoints.jsonl"
                    ):
                        raise KeyboardInterrupt(
                            "injected crash after durable checkpoint append"
                        )
                    return row

                with (
                    mock.patch.object(
                        matrix_runner_module,
                        "_journal_entry",
                        side_effect=interrupted_journal,
                    ),
                    mock.patch.object(
                        matrix_runner_module,
                        "_append_digest_entry",
                        side_effect=interrupted_append,
                    ),
                    self.assertRaises(KeyboardInterrupt),
                ):
                    adopt_flowmesh_container_matrix_replay_results(
                        matrix_plan_dir=self.matrix,
                        formal_execution_profile_dir=self.profile,
                        coordinator_plan_dir=self.coordinator,
                        run_dir=output,
                        run_id="formal-matrix-runner-test-v1",
                        client=client,
                        settings=self.settings,
                        adoption_id=f"schedule-adoption-crash-{stage}",
                        adoption_reason=(
                            "Exercise deterministic adoption crash recovery."
                        ),
                        adopt_failed_entry_sha256=failed_digest,
                        runtime_epoch_probe=_EpochProbe(client.epochs),
                    )

                live_counts = (
                    len(client.validated),
                    len(client.workflows),
                    len(client.submit_attempts),
                    len(client.retrieve_calls),
                )
                resumed = adopt_flowmesh_container_matrix_replay_results(
                    matrix_plan_dir=self.matrix,
                    formal_execution_profile_dir=self.profile,
                    coordinator_plan_dir=self.coordinator,
                    run_dir=output,
                    run_id="formal-matrix-runner-test-v1",
                    client=client,
                    settings=self.settings,
                    adoption_id=f"schedule-adoption-crash-{stage}",
                    adoption_reason=(
                        "Exercise deterministic adoption crash recovery."
                    ),
                    adopt_failed_entry_sha256=failed_digest,
                    runtime_epoch_probe=_EpochProbe(client.epochs),
                )
                self.assertEqual("REPLAY_RESULTS_ADOPTED", resumed["status"])
                self.assertEqual(
                    live_counts,
                    (
                        len(client.validated),
                        len(client.workflows),
                        len(client.submit_attempts),
                        len(client.retrieve_calls),
                    ),
                )
                journal_path = (
                    output / "flowmesh-container-matrix-journal.jsonl"
                )
                checkpoint_path = (
                    output
                    / "flowmesh-container-matrix-trial-checkpoints.jsonl"
                )
                durable_bytes = (journal_path.read_bytes(), checkpoint_path.read_bytes())
                repeated = adopt_flowmesh_container_matrix_replay_results(
                    matrix_plan_dir=self.matrix,
                    formal_execution_profile_dir=self.profile,
                    coordinator_plan_dir=self.coordinator,
                    run_dir=output,
                    run_id="formal-matrix-runner-test-v1",
                    client=client,
                    settings=self.settings,
                    adoption_id=f"schedule-adoption-crash-{stage}",
                    adoption_reason=(
                        "Exercise deterministic adoption crash recovery."
                    ),
                    adopt_failed_entry_sha256=failed_digest,
                    runtime_epoch_probe=_EpochProbe(client.epochs),
                )
                self.assertEqual("REPLAY_RESULTS_ADOPTED", repeated["status"])
                self.assertEqual(
                    durable_bytes,
                    (journal_path.read_bytes(), checkpoint_path.read_bytes()),
                )

    def test_replay_adoption_refuses_a_physical_operation(self) -> None:
        output = self.root / "physical-replay-adoption"
        client, failed_digest, _target_trial = self._replay_failed_recovery(
            output=output,
            replay_physical_operation=True,
        )
        validation_count = len(client.validated)
        workflow_count = len(client.workflows)
        with self.assertRaisesRegex(Exception, "schedule control"):
            adopt_flowmesh_container_matrix_replay_results(
                matrix_plan_dir=self.matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
                run_dir=output,
                run_id="formal-matrix-runner-test-v1",
                client=client,
                settings=self.settings,
                adoption_id="physical-replay-must-fail",
                adoption_reason="A physical replay must remain non-canonical.",
                adopt_failed_entry_sha256=failed_digest,
                runtime_epoch_probe=_EpochProbe(client.epochs),
            )
        self.assertEqual(validation_count, len(client.validated))
        self.assertEqual(workflow_count, len(client.workflows))
        self.assertEqual(
            "RUN_FAILED",
            _read_jsonl(
                output / "flowmesh-container-matrix-journal.jsonl"
            )[-1]["state"],
        )

    def test_source_less_checkpoint_rejects_restamped_replay_provenance(
        self,
    ) -> None:
        output = self.root / "restamped-replay-provenance"
        client, failed_digest, _target_trial = self._replay_failed_recovery(
            output=output
        )
        adopt_flowmesh_container_matrix_replay_results(
            matrix_plan_dir=self.matrix,
            formal_execution_profile_dir=self.profile,
            coordinator_plan_dir=self.coordinator,
            run_dir=output,
            run_id="formal-matrix-runner-test-v1",
            client=client,
            settings=self.settings,
            adoption_id="restamped-replay-provenance-001",
            adoption_reason="Create evidence for a provenance tamper test.",
            adopt_failed_entry_sha256=failed_digest,
            runtime_epoch_probe=_EpochProbe(client.epochs),
        )
        checkpoint_path = (
            output / "flowmesh-container-matrix-trial-checkpoints.jsonl"
        )
        original_checkpoints = _read_jsonl(checkpoint_path)
        contract = _read_json(
            output / "flowmesh-container-matrix-run-contract.json"
        )
        for name, field, value, expected_error in (
            (
                "known-field",
                "measurement_origin",
                "fabricated-fresh-live-execution",
                "provenance is invalid",
            ),
            (
                "unknown-origin-id-field",
                "original_flowmesh_task_id",
                "tsk-claimed-original",
                "fields changed",
            ),
        ):
            with self.subTest(name=name):
                checkpoints = json.loads(json.dumps(original_checkpoints))
                replayed = next(
                    row
                    for row in checkpoints[0]["operation_results"]
                    if row.get("idempotent_replay") is True
                )
                replayed[field] = value
                _restamp_document(checkpoints[0], "entry_sha256")
                _write_jsonl(checkpoint_path, checkpoints)
                with self.assertRaisesRegex(Exception, expected_error):
                    matrix_runner_module._load_checkpoints(
                        checkpoint_path,
                        contract,
                        sources=None,
                    )

    def test_recovery_treats_missing_root_dispatch_as_diagnostic(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers[1:]
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "missing-dispatch-evidence"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=target_trial,
            recoverable_dispatched_task_ids=None,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)
        failed_digest = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]
        recovery_client = self._fresh_recovery_client(first_client)
        result = self._run(
            recovery_client,
            output=output,
            recovery_id="idp-recovery-missing-dispatch",
            recovery_reason="Dispatch evidence is deliberately absent.",
            recover_failed_entry_sha256=failed_digest,
        )
        self.assertEqual("COMPLETE", result["status"])
        authorization = next(
            row
            for row in _read_jsonl(
                output / "flowmesh-container-matrix-journal.jsonl"
            )
            if row["state"] == "INFRASTRUCTURE_RECOVERY_AUTHORIZED"
        )
        self.assertIsNone(authorization["payload"]["dispatched_task_ids"])
        self.assertEqual(
            "non-historical-diagnostic-only",
            authorization["payload"][
                "root_dispatch_history_interpretation"
            ],
        )

    def test_recovery_treats_nonempty_root_dispatch_as_diagnostic(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers[1:]
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "nonempty-dispatch-diagnostic"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=target_trial,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)
        journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        failed_digest = journal[-1]["entry_sha256"]
        workflow_id = next(reversed(first_client.recoverable_terminals))
        original = first_client.recoverable_terminals[workflow_id]
        diagnostic_task_id = original.failed_task_ids[0]
        first_client.recoverable_terminals[workflow_id] = TerminalWorkflow(
            workflow_id=original.workflow_id,
            status=original.status,
            failed_task_ids=original.failed_task_ids,
            cancelled_task_ids=original.cancelled_task_ids,
            detail=original.detail,
            dispatched_task_ids=(diagnostic_task_id,),
        )
        recovery_client = self._fresh_recovery_client(first_client)
        result = self._run(
            recovery_client,
            output=output,
            recovery_id="idp-recovery-nonempty-dispatch",
            recovery_reason="Preserve the Root snapshot only as diagnostics.",
            recover_failed_entry_sha256=failed_digest,
        )
        self.assertEqual("COMPLETE", result["status"])
        authorization = next(
            row
            for row in _read_jsonl(
                output / "flowmesh-container-matrix-journal.jsonl"
            )
            if row["state"] == "INFRASTRUCTURE_RECOVERY_AUTHORIZED"
        )
        self.assertEqual(
            [diagnostic_task_id],
            authorization["payload"]["dispatched_task_ids"],
        )

    def test_recovery_refuses_physical_primary_before_resubmission(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers[1:]
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "physical-primary-recovery-refusal"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=target_trial,
            recoverable_primary_task_index=1,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)
        failed_digest = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]
        recovery_client = self._fresh_recovery_client(first_client)
        with self.assertRaisesRegex(Exception, "schedule control"):
            self._run(
                recovery_client,
                output=output,
                recovery_id="physical-primary-must-not-recover",
                recovery_reason="A physical attempted task is not retry-safe.",
                recover_failed_entry_sha256=failed_digest,
            )
        self.assertEqual([], recovery_client.validated)
        self.assertEqual([], recovery_client.workflows)
        self.assertEqual([], recovery_client.submit_attempts)
        self.assertFalse(
            any(
                row["state"] == "INFRASTRUCTURE_RECOVERY_AUTHORIZED"
                for row in _read_jsonl(
                    output / "flowmesh-container-matrix-journal.jsonl"
                )
            )
        )

    def test_recovery_refuses_an_independent_phase_root(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers
            if row["design_id"] not in {"D3", "D7"}
        )
        phase_operations = [
            json.loads(json.dumps(row))
            for row in self.operations
            if row["trial_key"] == target_trial
        ]
        schedule_index = next(
            index
            for index, row in enumerate(phase_operations)
            if row["operation_id"] == "schedule"
        )
        independent_index = next(
            index
            for index, row in enumerate(phase_operations)
            if row["operation_id"] != "schedule"
        )
        phase_operations[independent_index]["dependency_operation_keys"] = []
        task_ids = [
            f"tsk-structural-{index}"
            for index in range(len(phase_operations))
        ]
        evidence = [
            {
                "task_id": task_id,
                "task_status": "PENDING",
                "attempts": 0,
                "max_attempts": 3,
                "assigned_worker": None,
                "last_failed_worker": None,
                "detail": None,
            }
            for task_id in task_ids
        ]
        primary_task_id = task_ids[schedule_index]
        evidence[schedule_index] = {
            "task_id": primary_task_id,
            "task_status": "FAILED",
            "attempts": 1,
            "max_attempts": 3,
            "assigned_worker": WORKER_ID,
            "last_failed_worker": WORKER_ID,
            "detail": (
                f"HTTP delivery for task {primary_task_id} returned status "
                '503: {"detail":"Identity provider unavailable"}'
            ),
        }
        with self.assertRaisesRegex(Exception, "transitively downstream"):
            matrix_runner_module._recovery_schedule_safety_evidence(
                phase_operations,
                bound_task_ids=task_ids,
                task_evidence=evidence,
            )

    def test_crash_after_recovery_authorization_resumes_without_reauthorization(
        self,
    ) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers[1:]
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "authorized-crash-resume"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=target_trial,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)
        failed_digest = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]

        recovery_client = self._fresh_recovery_client(first_client)
        real_journal_entry = matrix_runner_module._journal_entry

        def interrupt_after_authorization(*args: Any, **kwargs: Any) -> Any:
            entry = real_journal_entry(*args, **kwargs)
            if kwargs.get("state") == "INFRASTRUCTURE_RECOVERY_AUTHORIZED":
                raise KeyboardInterrupt("injected post-authorization crash")
            return entry

        with (
            mock.patch.object(
                matrix_runner_module,
                "_journal_entry",
                side_effect=interrupt_after_authorization,
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            self._run(
                recovery_client,
                output=output,
                recovery_id="idp-recovery-crash-window",
                recovery_reason="Test the durable authorization crash window.",
                recover_failed_entry_sha256=failed_digest,
            )
        journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        self.assertEqual(
            "INFRASTRUCTURE_RECOVERY_AUTHORIZED",
            journal[-1]["state"],
        )

        resumed_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        result = self._run(resumed_client, output=output)
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(1, result["infrastructure_recovery_count"])
        final_journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        self.assertEqual(
            1,
            sum(
                row["state"] == "INFRASTRUCTURE_RECOVERY_AUTHORIZED"
                for row in final_journal
            ),
        )

    def test_same_phase_cannot_be_recovered_twice(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers[1:]
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "one-recovery-per-phase"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=target_trial,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)
        first_failed_digest = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]
        original_failure_bytes = (
            output / "flowmesh-container-matrix-failure.json"
        ).read_bytes()

        retry_client = self._fresh_recovery_client(first_client)
        retry_client.terminal_failure_trial = target_trial
        with self.assertRaises(Exception):
            self._run(
                retry_client,
                output=output,
                recovery_id="idp-recovery-only-once",
                recovery_reason="Authorize the sole retry for this phase.",
                recover_failed_entry_sha256=first_failed_digest,
            )
        second_failed_digest = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]

        second_retry_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        with self.assertRaisesRegex(Exception, "at most one"):
            self._run(
                second_retry_client,
                output=output,
                recovery_id="idp-recovery-disallowed-second",
                recovery_reason="A second retry must be refused.",
                recover_failed_entry_sha256=second_failed_digest,
            )
        self.assertEqual([], second_retry_client.wait_calls)
        self.assertEqual([], second_retry_client.workflows)
        self.assertEqual(
            original_failure_bytes,
            (
                output / "flowmesh-container-matrix-failure.json"
            ).read_bytes(),
        )

    def test_recovery_id_cannot_be_reused_for_a_later_phase(self) -> None:
        unconditional = [
            row
            for row in self.wrappers
            if row["design_id"] not in {"D3", "D7"}
        ]
        first_target = unconditional[1]["trial_key"]
        second_target = unconditional[2]["trial_key"]
        output = self.root / "unique-recovery-ids"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            recoverable_terminal_failure_trial=first_target,
        )
        with self.assertRaises(Exception):
            self._run(first_client, output=output)
        first_digest = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]

        first_retry = self._fresh_recovery_client(first_client)
        first_retry.recoverable_terminal_failure_trial = second_target
        with self.assertRaises(Exception):
            self._run(
                first_retry,
                output=output,
                recovery_id="idp-recovery-unique-001",
                recovery_reason="Authorize recovery of the first phase.",
                recover_failed_entry_sha256=first_digest,
            )
        second_digest = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )[-1]["entry_sha256"]

        duplicate_id_client = self._fresh_recovery_client(first_retry)
        probe = _EpochProbe(duplicate_id_client.epochs)
        with self.assertRaisesRegex(Exception, "recovery_id was already used"):
            self._run(
                duplicate_id_client,
                output=output,
                probe=probe,
                recovery_id="idp-recovery-unique-001",
                recovery_reason="This duplicate ID must be refused.",
                recover_failed_entry_sha256=second_digest,
            )
        self.assertEqual([], probe.calls)
        self.assertEqual([], duplicate_id_client.wait_calls)
        self.assertEqual([], duplicate_id_client.workflows)

    def test_recovery_refuses_reused_flowmesh_identifiers_before_binding(
        self,
    ) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers[1:]
            if row["design_id"] not in {"D3", "D7"}
        )
        for reuse_kind in ("workflow", "task"):
            with self.subTest(reuse_kind=reuse_kind):
                output = self.root / f"reused-flowmesh-{reuse_kind}-id"
                first_client = FakeMatrixFlowMeshClient(
                    cache_outcomes=self.cache_outcomes,
                    recoverable_terminal_failure_trial=target_trial,
                )
                with self.assertRaises(Exception):
                    self._run(first_client, output=output)
                journal_path = (
                    output / "flowmesh-container-matrix-journal.jsonl"
                )
                failed_digest = _read_jsonl(journal_path)[-1][
                    "entry_sha256"
                ]
                old_bound = next(
                    row
                    for row in reversed(_read_jsonl(journal_path))
                    if row["state"] == "WORKFLOW_BOUND"
                )
                old_workflow_id = old_bound["payload"]["workflow_id"]
                old_task_ids = tuple(old_bound["payload"]["task_ids"])
                original_failure = (
                    output / "flowmesh-container-matrix-failure.json"
                ).read_bytes()
                fresh_task_ids = tuple(
                    f"tsk-fresh-{reuse_kind}-{index:03d}"
                    for index in range(len(old_task_ids))
                )
                returned_workflow_id = (
                    old_workflow_id
                    if reuse_kind == "workflow"
                    else f"wfl-fresh-{reuse_kind}"
                )
                returned_task_ids = (
                    fresh_task_ids
                    if reuse_kind == "workflow"
                    else (old_task_ids[0], *fresh_task_ids[1:])
                )

                retry_client = self._fresh_recovery_client(first_client)

                def reuse_identifiers(
                    workflow: Mapping[str, Any],
                ) -> SubmittedWorkflow:
                    retry_client.workflows.append(
                        json.loads(json.dumps(workflow))
                    )
                    return SubmittedWorkflow(
                        returned_workflow_id, returned_task_ids
                    )

                retry_client.submit = (  # type: ignore[method-assign]
                    reuse_identifiers
                )
                with self.assertRaisesRegex(
                    Exception, "reused a workflow or task ID"
                ):
                    self._run(
                        retry_client,
                        output=output,
                        recovery_id=f"idp-recovery-reused-{reuse_kind}-id",
                        recovery_reason=(
                            "Exercise Root identifier reuse refusal."
                        ),
                        recover_failed_entry_sha256=failed_digest,
                    )
                final_journal = _read_jsonl(journal_path)
                self.assertEqual("RUN_FAILED", final_journal[-1]["state"])
                self.assertEqual(
                    1,
                    sum(
                        row["state"] == "WORKFLOW_BOUND"
                        and row["payload"]["workflow_id"]
                        == old_workflow_id
                        for row in final_journal
                    ),
                )
                self.assertEqual([], retry_client.retrieve_calls)
                self.assertEqual(
                    original_failure,
                    (
                        output
                        / "flowmesh-container-matrix-failure.json"
                    ).read_bytes(),
                )

    def test_clean_boundary_interrupt_resumes_only_the_missing_suffix(self) -> None:
        interrupted_trial = self.wrappers[5]["trial_key"]
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            interrupt_before_trial=interrupted_trial,
        )
        output = self.root / "resume"
        with self.assertRaises(KeyboardInterrupt):
            self._run(first_client, output=output)
        first_keys = set(self._workflow_operation_keys(first_client))
        self.assertTrue(first_keys)

        second_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        resumed = self._run(second_client, output=output)
        second_keys = set(self._workflow_operation_keys(second_client))
        self.assertEqual("COMPLETE", resumed["status"])
        self.assertTrue(resumed["resume_performed"])
        self.assertEqual(5, resumed["checkpoint_reused_trial_count"])
        self.assertTrue(first_keys.isdisjoint(second_keys))
        self.assertEqual(472, len(first_keys | second_keys))
        self.assertEqual(
            "VERIFIED",
            verify_flowmesh_container_matrix_run(output)["status"],
        )

    def test_resume_fails_closed_after_unbound_submission_intent(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "ambiguous-submission"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            interrupt_submit_trial=target_trial,
        )
        with self.assertRaises(KeyboardInterrupt):
            self._run(first_client, output=output)
        self.assertEqual([target_trial], first_client.submit_attempts)
        journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        self.assertEqual("SUBMISSION_INTENT", journal[-1]["state"])

        resume_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        with self.assertRaisesRegex(Exception, "(?i)ambiguous"):
            self._run(resume_client, output=output)
        self.assertEqual([], resume_client.validated)
        self.assertEqual([], resume_client.submit_attempts)
        self.assertEqual([], resume_client.workflows)

    def test_resume_waits_for_a_bound_workflow_without_resubmitting(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "bound-workflow-resume"
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            interrupt_wait_trial=target_trial,
        )
        with self.assertRaises(KeyboardInterrupt):
            self._run(client, output=output)
        self.assertEqual(1, client.submit_attempts.count(target_trial))
        journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        self.assertEqual("WORKFLOW_BOUND", journal[-1]["state"])

        resumed = self._run(client, output=output)
        self.assertEqual("COMPLETE", resumed["status"])
        self.assertEqual(1, client.submit_attempts.count(target_trial))
        self.assertEqual(80, len(client.workflows))

    def test_resume_reuses_persisted_results_without_live_retrieval(self) -> None:
        target_trial = next(
            row["trial_key"]
            for row in self.wrappers
            if row["design_id"] not in {"D3", "D7"}
        )
        output = self.root / "results-obtained-resume"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        real_journal_entry = matrix_runner_module._journal_entry
        interrupted = False

        def interrupt_after_results(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal interrupted
            row = real_journal_entry(*args, **kwargs)
            if (
                kwargs.get("state") == "RESULTS_OBTAINED"
                and kwargs.get("trial_key") == target_trial
                and not interrupted
            ):
                interrupted = True
                raise KeyboardInterrupt("injected post-results interruption")
            return row

        with (
            mock.patch.object(
                matrix_runner_module,
                "_journal_entry",
                side_effect=interrupt_after_results,
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            self._run(first_client, output=output)
        self.assertTrue(interrupted)
        journal = _read_jsonl(
            output / "flowmesh-container-matrix-journal.jsonl"
        )
        self.assertEqual("RESULTS_OBTAINED", journal[-1]["state"])

        resume_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        resumed = self._run(resume_client, output=output)
        self.assertEqual("COMPLETE", resumed["status"])
        self.assertNotIn(target_trial, resume_client.submit_attempts)
        validated_trials = {
            resume_client._workflow_operations(workflow)[0]["trial_key"]
            for workflow in resume_client.validated
        }
        self.assertNotIn(target_trial, validated_trials)

    def test_completed_output_is_reused_without_live_calls(self) -> None:
        output = self.root / "complete"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        self._run(first_client, output=output)

        second_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            interrupt_before_trial=self.wrappers[0]["trial_key"],
        )
        reused = self._run(second_client, output=output)
        self.assertEqual("COMPLETE", reused["status"])
        self.assertTrue(reused["completed_output_reused"])
        self.assertEqual(0, reused["executed_this_invocation"])
        self.assertEqual([], second_client.validated)
        self.assertEqual([], second_client.workflows)

    def test_completed_output_reuse_rejects_a_different_worker_alias(
        self,
    ) -> None:
        output = self._completed_run_copy("completed-wrong-alias")
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        wrong_settings = FlowMeshSettings(
            base_url="https://flowmesh.test/fm/root-a",
            worker_alias="another-worker-alias",
            validate_before_submit=True,
            poll_interval_seconds=0.01,
        )
        with self.assertRaisesRegex(Exception, "(?i)worker alias"):
            run_flowmesh_container_matrix(
                matrix_plan_dir=self.matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
                output_dir=output,
                run_id="formal-matrix-runner-golden-v1",
                client=client,
                settings=wrong_settings,
                runtime_epoch_probe=_EpochProbe(client.epochs),
            )
        self.assertEqual([], client.validated)
        self.assertEqual([], client.workflows)

    def test_resume_rejects_runtime_epoch_drift_before_submission(self) -> None:
        interrupted_trial = self.wrappers[3]["trial_key"]
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            interrupt_before_trial=interrupted_trial,
        )
        output = self.root / "epoch-drift"
        with self.assertRaises(KeyboardInterrupt):
            self._run(first_client, output=output)

        restarted_epochs = _epochs("restarted")
        second_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            epochs=restarted_epochs,
        )
        with self.assertRaisesRegex(Exception, "runtime epoch"):
            self._run(
                second_client,
                output=output,
                probe=_EpochProbe(restarted_epochs),
            )
        self.assertEqual([], second_client.workflows)

    def test_runner_rejects_duplicate_runtime_epochs_before_submission(
        self,
    ) -> None:
        duplicate_epochs = _epochs("duplicate")
        duplicate_epochs["N8"] = duplicate_epochs["N7"]
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            epochs=duplicate_epochs,
        )
        with self.assertRaisesRegex(Exception, "(?i)(duplicate|distinct|unique)"):
            self._run(
                client,
                output=self.root / "duplicate-runtime-epochs",
                probe=_EpochProbe(duplicate_epochs),
            )
        self.assertEqual([], client.validated)
        self.assertEqual([], client.workflows)

    def test_root_endpoint_identity_rejects_sensitive_url_components(
        self,
    ) -> None:
        endpoints = (
            "https://user:password@flowmesh.test/fm/root",
            "https://flowmesh.test/fm/root?tenant=private",
            "https://flowmesh.test/fm/root#private",
        )
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                with self.assertRaisesRegex(
                    Exception, "(?i)(credentials|query|fragment)"
                ):
                    matrix_runner_module._root_endpoint_identity_sha256(
                        endpoint
                    )

    def test_sensitive_root_url_is_rejected_before_external_reads(self) -> None:
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        client.describe_current_worker = mock.Mock(
            wraps=client.describe_current_worker
        )
        probe = mock.Mock(return_value=client.epochs)
        settings = FlowMeshSettings(
            base_url="https://flowmesh.test/fm/root?tenant=private",
            worker_alias=WORKER_ALIAS,
            validate_before_submit=True,
            poll_interval_seconds=0.01,
        )
        with self.assertRaisesRegex(Exception, "(?i)(query|fragment)"):
            run_flowmesh_container_matrix(
                matrix_plan_dir=self.matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
                output_dir=self.root / "sensitive-root-url",
                run_id="sensitive-root-url-v1",
                client=client,
                settings=settings,
                runtime_epoch_probe=probe,
            )
        client.describe_current_worker.assert_not_called()
        probe.assert_not_called()
        self.assertEqual([], client.validated)
        self.assertEqual([], client.workflows)

    def test_output_directory_lock_rejects_a_second_handle_and_releases(
        self,
    ) -> None:
        target = self.root / "locked-output"
        with matrix_runner_module._exclusive_run_lock(target):
            with self.assertRaisesRegex(Exception, "another process"):
                with matrix_runner_module._exclusive_run_lock(target):
                    self.fail("a competing handle acquired the run lock")

        reacquired = False
        with matrix_runner_module._exclusive_run_lock(target):
            reacquired = True
        self.assertTrue(reacquired)

    def test_resume_allows_dynamic_worker_status_change(self) -> None:
        interrupted_trial = self.wrappers[3]["trial_key"]
        output = self.root / "worker-status-change"
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            worker_status="IDLE",
            interrupt_before_trial=interrupted_trial,
        )
        with self.assertRaises(KeyboardInterrupt):
            self._run(first_client, output=output)

        second_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            worker_status="RUNNING",
        )
        resumed = self._run(second_client, output=output)
        self.assertEqual("COMPLETE", resumed["status"])
        self.assertTrue(resumed["resume_performed"])
        self.assertEqual(3, resumed["checkpoint_reused_trial_count"])
        self.assertTrue(second_client.workflows)

    def test_resume_rejects_a_different_flowmesh_root_endpoint(self) -> None:
        interrupted_trial = self.wrappers[2]["trial_key"]
        output = self.root / "root-endpoint-change"
        initial_settings = FlowMeshSettings(
            base_url="https://flowmesh.test/fm/root-a",
            worker_alias=WORKER_ALIAS,
            validate_before_submit=True,
            poll_interval_seconds=0.01,
        )
        first_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes,
            interrupt_before_trial=interrupted_trial,
        )
        with self.assertRaises(KeyboardInterrupt):
            run_flowmesh_container_matrix(
                matrix_plan_dir=self.matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
                output_dir=output,
                run_id="root-endpoint-binding-v1",
                client=first_client,
                settings=initial_settings,
                runtime_epoch_probe=_EpochProbe(first_client.epochs),
            )

        changed_settings = FlowMeshSettings(
            base_url="https://flowmesh.test/fm/root-b",
            worker_alias=WORKER_ALIAS,
            validate_before_submit=True,
            poll_interval_seconds=0.01,
        )
        second_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        with self.assertRaisesRegex(
            Exception,
            "(?i)(root|endpoint|contract)",
        ):
            run_flowmesh_container_matrix(
                matrix_plan_dir=self.matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
                output_dir=output,
                run_id="root-endpoint-binding-v1",
                client=second_client,
                settings=changed_settings,
                runtime_epoch_probe=_EpochProbe(second_client.epochs),
            )
        self.assertEqual([], second_client.workflows)

    def test_resume_rebuilds_a_partially_written_finalization(self) -> None:
        output = self.root / "partial-finalization"
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        real_atomic_write = matrix_runner_module._atomic_write
        interrupted = False

        def interrupt_finalization(path: Path, content: bytes) -> None:
            nonlocal interrupted
            if (
                Path(path).name
                == "flowmesh-container-matrix-trial-results.jsonl"
                and not interrupted
            ):
                interrupted = True
                raise OSError("injected finalization interruption")
            real_atomic_write(path, content)

        with (
            mock.patch.object(
                matrix_runner_module,
                "_atomic_write",
                side_effect=interrupt_finalization,
            ),
            self.assertRaisesRegex(OSError, "finalization interruption"),
        ):
            self._run(client, output=output)
        self.assertTrue(interrupted)
        self.assertTrue(
            (output / "flowmesh-container-matrix-run.json").is_file()
        )
        self.assertFalse((output / "SHA256SUMS").exists())
        self.assertEqual(
            64,
            len(
                _read_jsonl(
                    output
                    / "flowmesh-container-matrix-trial-checkpoints.jsonl"
                )
            ),
        )

        resume_client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        resumed = self._run(resume_client, output=output)
        self.assertEqual("COMPLETE", resumed["status"])
        self.assertTrue(resumed["resume_performed"])
        self.assertEqual(64, resumed["checkpoint_reused_trial_count"])
        self.assertEqual(0, resumed["executed_this_invocation"])
        self.assertEqual([], resume_client.validated)
        self.assertEqual([], resume_client.workflows)
        self.assertEqual(
            "VERIFIED",
            verify_flowmesh_container_matrix_run(output)["status"],
        )

    def test_offline_and_source_bound_verification(self) -> None:
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        self._run(client)
        run_root = self.root / "run"
        portable = verify_flowmesh_container_matrix_run(run_root)
        self.assertEqual("VERIFIED", portable["status"])
        self.assertFalse(portable["source_binding_checked"])

        bound = verify_flowmesh_container_matrix_run(
            run_root,
            matrix_plan_dir=self.matrix,
            formal_execution_profile_dir=self.profile,
            coordinator_plan_dir=self.coordinator,
        )
        self.assertEqual("VERIFIED", bound["status"])
        self.assertTrue(bound["source_binding_checked"])
        with self.assertRaisesRegex(Exception, "supplied together"):
            verify_flowmesh_container_matrix_run(
                run_root,
                matrix_plan_dir=self.matrix,
            )

    def test_completed_verifier_rejects_an_extra_subdirectory(self) -> None:
        run_root = self._completed_run_copy("extra-directory")
        (run_root / "unexpected-evidence").mkdir()
        with self.assertRaises(Exception):
            verify_flowmesh_container_matrix_run(run_root)

    def test_completed_verifier_rejects_a_symlinked_artifact(self) -> None:
        run_root = self._completed_run_copy("symlinked-artifact")
        artifact = run_root / "flowmesh-container-matrix-run.json"
        backing = self.root / "symlinked-run-summary.json"
        backing.write_bytes(artifact.read_bytes())
        artifact.unlink()
        try:
            artifact.symlink_to(backing)
        except (NotImplementedError, OSError) as exc:
            self.skipTest(f"symlink creation is unavailable: {exc}")
        with self.assertRaises(Exception):
            verify_flowmesh_container_matrix_run(run_root)

    def test_source_binding_rejects_another_valid_matrix(self) -> None:
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        self._run(client)
        other_matrix = _copy_tree(self.matrix, self.root / "other-matrix")
        plan_path = other_matrix / "flowmesh-container-matrix-plan.json"
        plan = _read_json(plan_path)
        plan["matrix_id"] = "another-valid-looking-matrix"
        plan_without_digest = dict(plan)
        plan_without_digest.pop("plan_sha256", None)
        plan["plan_sha256"] = hashlib.sha256(
            json.dumps(
                plan_without_digest,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        plan_path.write_text(
            json.dumps(plan, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _restamp_checksums(other_matrix)
        with self.assertRaises(Exception):
            verify_flowmesh_container_matrix_run(
                self.root / "run",
                matrix_plan_dir=other_matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
            )

    def test_semantically_tampered_operation_ledger_is_rejected_after_restamp(
        self,
    ) -> None:
        client = FakeMatrixFlowMeshClient(
            cache_outcomes=self.cache_outcomes
        )
        self._run(client)
        run_root = self.root / "run"
        result_path = (
            run_root / "flowmesh-container-matrix-operation-results.jsonl"
        )
        rows = _read_jsonl(result_path)
        rows[0]["executed"] = not rows[0]["executed"]
        result_path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
        _restamp_checksums(run_root)
        with self.assertRaises(Exception):
            verify_flowmesh_container_matrix_run(run_root)

    def test_checkpoint_and_final_coordinated_tamper_is_rejected(self) -> None:
        run_root = self._completed_run_copy("coordinated-result-tamper")
        checkpoint_path = (
            run_root / "flowmesh-container-matrix-trial-checkpoints.jsonl"
        )
        checkpoints = _read_jsonl(checkpoint_path)
        operation = next(
            row
            for row in checkpoints[0]["operation_results"]
            if row["executed"] is True
            and row["operation_kind"]
            in {"storage_read", "cache_read", "network_transfer"}
        )
        operation_key = operation["operation_key"]
        operation["physical_bytes"] += 1
        checkpoints[0]["trial_result"]["telemetry"][
            "physical_bytes_sum"
        ] += 1
        _restamp_document(checkpoints[0], "entry_sha256")
        _write_jsonl(checkpoint_path, checkpoints)

        operation_path = (
            run_root / "flowmesh-container-matrix-operation-results.jsonl"
        )
        operation_rows = _read_jsonl(operation_path)
        final_operation = next(
            row for row in operation_rows if row["operation_key"] == operation_key
        )
        final_operation["physical_bytes"] += 1
        _write_jsonl(operation_path, operation_rows)

        trial_path = run_root / "flowmesh-container-matrix-trial-results.jsonl"
        trial_rows = _read_jsonl(trial_path)
        trial_rows[0] = checkpoints[0]["trial_result"]
        _write_jsonl(trial_path, trial_rows)
        summary_path = run_root / "flowmesh-container-matrix-run.json"
        summary = _read_json(summary_path)
        _restamp_document(summary, "run_sha256")
        _write_json(summary_path, summary)
        _restamp_checksums(run_root)

        with self.assertRaises(Exception):
            verify_flowmesh_container_matrix_run(run_root)

    def test_restamped_node_api_url_digest_tamper_is_rejected(self) -> None:
        run_root = self._completed_run_copy("node-url-digest-tamper")
        contract_path = (
            run_root / "flowmesh-container-matrix-run-contract.json"
        )
        contract = _read_json(contract_path)
        contract["node_api_urls_sha256"] = "0" * 64
        _restamp_document(contract, "contract_sha256")
        _write_json(contract_path, contract)

        summary_path = run_root / "flowmesh-container-matrix-run.json"
        summary = _read_json(summary_path)
        summary["contract_sha256"] = contract["contract_sha256"]
        _restamp_document(summary, "run_sha256")
        _write_json(summary_path, summary)
        _restamp_checksums(run_root)

        with self.assertRaises(Exception):
            verify_flowmesh_container_matrix_run(run_root)

    def test_completed_run_rejects_invalid_journal_histories(self) -> None:
        cases = ("empty", "out-of-order", "checkpoint-unlinked")
        for case in cases:
            with self.subTest(case=case):
                run_root = self._completed_run_copy(f"journal-{case}")
                journal_path = (
                    run_root / "flowmesh-container-matrix-journal.jsonl"
                )
                rows = _read_jsonl(journal_path)
                if case == "empty":
                    rows = []
                elif case == "out-of-order":
                    rows[0], rows[1] = rows[1], rows[0]
                    for index, row in enumerate(rows):
                        row["journal_sequence"] = index
                        _restamp_document(row, "entry_sha256")
                else:
                    completed = next(
                        row for row in rows if row["state"] == "TRIAL_COMPLETED"
                    )
                    completed["payload"]["checkpoint_entry_sha256"] = "0" * 64
                    _restamp_document(completed, "entry_sha256")
                _write_jsonl(journal_path, rows)
                _restamp_checksums(run_root)
                with self.assertRaises(Exception):
                    verify_flowmesh_container_matrix_run(run_root)

    def test_source_bound_verifier_rederives_wrapper_and_resolution_maps(
        self,
    ) -> None:
        run_root = self._completed_run_copy("source-map-tamper")
        contract_path = (
            run_root / "flowmesh-container-matrix-run-contract.json"
        )
        contract = _read_json(contract_path)
        wrapper_trial = contract["ordered_trial_keys"][0]
        conditional_trial = next(
            key
            for key, digest in contract[
                "resolution_sha256_by_trial_key"
            ].items()
            if digest is not None
        )
        contract["trial_wrapper_sha256_by_trial_key"][wrapper_trial] = (
            "1" * 64
        )
        contract["resolution_sha256_by_trial_key"][conditional_trial] = (
            "2" * 64
        )
        _restamp_document(contract, "contract_sha256")
        _write_json(contract_path, contract)

        checkpoint_path = (
            run_root / "flowmesh-container-matrix-trial-checkpoints.jsonl"
        )
        checkpoints = _read_jsonl(checkpoint_path)
        checkpoints[0]["wrapper_sha256"] = "1" * 64
        _restamp_document(checkpoints[0], "entry_sha256")
        _write_jsonl(checkpoint_path, checkpoints)

        journal_path = run_root / "flowmesh-container-matrix-journal.jsonl"
        journal = _read_jsonl(journal_path)
        completion = next(
            row
            for row in journal
            if row["state"] == "TRIAL_COMPLETED"
            and row["sequence_index"] == 0
        )
        completion["payload"]["checkpoint_entry_sha256"] = checkpoints[0][
            "entry_sha256"
        ]
        _restamp_document(completion, "entry_sha256")
        _write_jsonl(journal_path, journal)

        summary_path = run_root / "flowmesh-container-matrix-run.json"
        summary = _read_json(summary_path)
        summary["contract_sha256"] = contract["contract_sha256"]
        _restamp_document(summary, "run_sha256")
        _write_json(summary_path, summary)
        _restamp_checksums(run_root)

        portable = verify_flowmesh_container_matrix_run(run_root)
        self.assertEqual("VERIFIED", portable["status"])
        self.assertFalse(portable["source_binding_checked"])
        with self.assertRaises(Exception):
            verify_flowmesh_container_matrix_run(
                run_root,
                matrix_plan_dir=self.matrix,
                formal_execution_profile_dir=self.profile,
                coordinator_plan_dir=self.coordinator,
            )

    def test_run_cli_passes_the_frozen_sources_and_runtime_settings(self) -> None:
        output = self.root / "cli-run"
        expected = {
            "status": "COMPLETE",
            "run_id": "matrix-cli-run-v1",
            "completed_trial_count": 64,
        }
        fake_sdk_client = mock.Mock()
        stream = io.StringIO()
        with (
            mock.patch(
                "pathfinder.integrations.flowmesh.SdkFlowMeshClient",
                return_value=fake_sdk_client,
            ) as client_factory,
            mock.patch(
                "pathfinder.integrations.flowmesh.container_matrix_runner."
                "run_flowmesh_container_matrix",
                return_value=expected,
            ) as run_matrix,
            redirect_stdout(stream),
        ):
            code = cli_main(
                [
                    "run-flowmesh-container-matrix",
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--formal-execution-profile-dir",
                    str(self.profile),
                    "--coordinator-plan-dir",
                    str(self.coordinator),
                    "--output-dir",
                    str(output),
                    "--run-id",
                    "matrix-cli-run-v1",
                    "--worker-alias",
                    WORKER_ALIAS,
                    "--flowmesh-base-url",
                    "https://flowmesh.test/root",
                    "--poll-interval",
                    "0.25",
                    "--recovery-id",
                    "idp-recovery-cli-001",
                    "--recovery-reason",
                    "Operator restored the Root identity provider.",
                    "--recover-failed-entry-sha256",
                    "a" * 64,
                    "--compact",
                ]
            )
        self.assertEqual(0, code)
        self.assertEqual(expected, json.loads(stream.getvalue()))
        settings = client_factory.call_args.args[0]
        self.assertEqual(WORKER_ALIAS, settings.worker_alias)
        self.assertEqual("https://flowmesh.test/root", settings.base_url)
        self.assertEqual(600, settings.task_timeout_seconds)
        self.assertEqual(0.25, settings.poll_interval_seconds)
        self.assertTrue(settings.validate_before_submit)
        kwargs = run_matrix.call_args.kwargs
        self.assertEqual(self.matrix, kwargs["matrix_plan_dir"])
        self.assertEqual(self.profile, kwargs["formal_execution_profile_dir"])
        self.assertEqual(self.coordinator, kwargs["coordinator_plan_dir"])
        self.assertEqual(output, kwargs["output_dir"])
        self.assertEqual("matrix-cli-run-v1", kwargs["run_id"])
        self.assertIs(fake_sdk_client, kwargs["client"])
        self.assertIs(settings, kwargs["settings"])
        self.assertEqual(
            "idp-recovery-cli-001", kwargs["recovery_id"]
        )
        self.assertEqual(
            "Operator restored the Root identity provider.",
            kwargs["recovery_reason"],
        )
        self.assertEqual(
            "a" * 64, kwargs["recover_failed_entry_sha256"]
        )
        fake_sdk_client.close.assert_called_once_with()

    def test_run_cli_rejects_non_finite_poll_interval(self) -> None:
        with (
            self.assertRaises(SystemExit),
            redirect_stderr(io.StringIO()),
        ):
            cli_parser().parse_args(
                [
                    "run-flowmesh-container-matrix",
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--formal-execution-profile-dir",
                    str(self.profile),
                    "--coordinator-plan-dir",
                    str(self.coordinator),
                    "--output-dir",
                    str(self.root / "bad-poll"),
                    "--run-id",
                    "matrix-cli-run-v1",
                    "--worker-alias",
                    WORKER_ALIAS,
                    "--poll-interval",
                    "nan",
                ]
            )

    def test_run_cli_rejects_blank_worker_alias_before_client_creation(
        self,
    ) -> None:
        stream = io.StringIO()
        with (
            mock.patch(
                "pathfinder.integrations.flowmesh.SdkFlowMeshClient"
            ) as client_factory,
            redirect_stdout(stream),
        ):
            code = cli_main(
                [
                    "run-flowmesh-container-matrix",
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--formal-execution-profile-dir",
                    str(self.profile),
                    "--coordinator-plan-dir",
                    str(self.coordinator),
                    "--output-dir",
                    str(self.root / "blank-worker"),
                    "--run-id",
                    "matrix-cli-run-v1",
                    "--worker-alias",
                    "   ",
                    "--compact",
                ]
            )
        self.assertEqual(2, code)
        self.assertIn("non-empty", json.loads(stream.getvalue())["message"])
        client_factory.assert_not_called()

    def test_cli_top_level_error_output_redacts_bearer_credentials(self) -> None:
        fake_sdk_client = mock.Mock()
        stream = io.StringIO()
        with (
            mock.patch(
                "pathfinder.integrations.flowmesh.SdkFlowMeshClient",
                return_value=fake_sdk_client,
            ),
            mock.patch(
                "pathfinder.integrations.flowmesh.container_matrix_runner."
                "run_flowmesh_container_matrix",
                side_effect=RuntimeError(
                    "delivery failed: Authorization: Bearer secret-cli-token"
                ),
            ),
            redirect_stdout(stream),
        ):
            code = cli_main(
                [
                    "run-flowmesh-container-matrix",
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--formal-execution-profile-dir",
                    str(self.profile),
                    "--coordinator-plan-dir",
                    str(self.coordinator),
                    "--output-dir",
                    str(self.root / "cli-redaction"),
                    "--run-id",
                    "matrix-cli-redaction-v1",
                    "--worker-alias",
                    WORKER_ALIAS,
                    "--compact",
                ]
            )
        text = stream.getvalue()
        self.assertEqual(2, code)
        self.assertNotIn("secret-cli-token", text)
        self.assertIn("<redacted>", json.loads(text)["message"])
        fake_sdk_client.close.assert_called_once_with()

    def test_verify_cli_supports_portable_and_source_bound_modes(self) -> None:
        run_root = self.root / "cli-verify"
        response = {
            "status": "VERIFIED",
            "run_id": "matrix-cli-run-v1",
            "source_binding_checked": True,
        }
        stream = io.StringIO()
        with (
            mock.patch(
                "pathfinder.integrations.flowmesh.container_matrix_runner."
                "verify_flowmesh_container_matrix_run",
                return_value=response,
            ) as verify_run,
            redirect_stdout(stream),
        ):
            code = cli_main(
                [
                    "verify-flowmesh-container-matrix-run",
                    "--run-dir",
                    str(run_root),
                    "--matrix-plan-dir",
                    str(self.matrix),
                    "--formal-execution-profile-dir",
                    str(self.profile),
                    "--coordinator-plan-dir",
                    str(self.coordinator),
                    "--compact",
                ]
            )
        self.assertEqual(0, code)
        self.assertEqual(response, json.loads(stream.getvalue()))
        verify_run.assert_called_once_with(
            run_root,
            matrix_plan_dir=self.matrix,
            formal_execution_profile_dir=self.profile,
            coordinator_plan_dir=self.coordinator,
        )


if __name__ == "__main__":
    unittest.main()
