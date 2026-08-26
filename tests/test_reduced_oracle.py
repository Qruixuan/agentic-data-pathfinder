from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.config import load_config
from pathfinder.data_agent_manifest import load_data_agent_manifest
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshAgentRun,
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
)
from pathfinder.integrations.flowmesh.pilot import (
    load_flowmesh_pilot_config,
)
from pathfinder.reduced_oracle import (
    FilesystemTransitionExecutor,
    RecoveryCircuitOpenError,
    ReducedOracleRecoveryError,
    load_reduced_oracle_config,
    plan_reduced_oracle_recovery,
    run_reduced_oracle,
    run_reduced_oracle_recovery,
)


ROOT = Path(__file__).resolve().parents[1]
ORACLE_CONFIG_PATH = ROOT / "configs" / "reduced_oracle_mvp.json"
ORACLE_MANIFEST_PATH = (
    ROOT / "configs" / "reduced_oracle_mvp_data_agent_manifest.json"
)
PHASE_B_PILOT_PATH = (
    ROOT / "configs" / "phase_b_confirmatory_small.json"
)


class DesignAwareOracleAdapter:
    def __init__(self) -> None:
        self.settings = FlowMeshSettings(worker_alias="pathfinder-oracle-test")
        self.requests: list[FlowMeshAgentRunRequest] = []

    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        self.requests.append(request)
        local = request.design_id == "D_local_digest"
        representation = "multimodal_digest" if local else "sampled_frames"
        event = {
            "accepted": True,
            "representation_id": representation,
            "quoted_price": 2.0 if local else 1.0,
            "realized_cost": 0.2 if local else 0.35,
            "felt_latency_ms": 25.0 if local else 75.0,
            "data_agent_service_latency_ms": 25.0 if local else 75.0,
            "data_agent_fetch_latency_ms": 0.1,
            "data_agent_controlled_delay_ms": 24.9 if local else 74.9,
            "artifact_bytes_sent": 0,
            "artifact_transfer_latency_ms": 0.0,
            "artifact_download_request_count": 0,
            "artifact_full_download_count": 0,
        }
        # This contains every accepted answer token in the fixed workload.
        correct = (
            "smells the black dog; continue skating; kept trying; unwrap it; "
            "assemble parts to build a toy; lick it; one; purple"
        )
        suffix = len(self.requests)
        return FlowMeshAgentRun(
            session_id=request.session_id or f"session-{suffix}",
            workflow_id=f"wfl-{suffix}",
            task_id=f"tsk-{suffix}",
            status="DONE",
            final_answer=correct if local else "none of the listed answers",
            access_events=(event,),
            raw_result={},
        )


class InterruptingOracleAdapter(DesignAwareOracleAdapter):
    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        if request.design_id == "D_local_digest":
            raise KeyboardInterrupt("simulated abrupt probe interruption")
        return super().run(request)


class FailingRecoveryAdapter(DesignAwareOracleAdapter):
    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        self.requests.append(request)
        raise RuntimeError("simulated unavailable inference dependency")


def _temporary_oracle_config(
    root: Path,
    *,
    repetitions: int = 1,
) -> Path:
    pilot = load_flowmesh_pilot_config(PHASE_B_PILOT_PATH)
    for workload in pilot.workloads:
        source = root / "sources" / workload.object_id / "digest.txt"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(
            f"digest for {workload.object_id}\n",
            encoding="utf-8",
        )
    payload = {
        "schema_version": "pathfinder.reduced-oracle/v1alpha1",
        "oracle_id": "test-reduced-oracle",
        "workload_pilot_config": str(PHASE_B_PILOT_PATH),
        "safe_design_id": "D_remote_digest",
        "design_order": ["D_remote_digest", "D_local_digest"],
        "quote_profile_id": "as_designed",
        "latency_multiplier": 1.0,
        "repetitions": repetitions,
        "base_seed": 101,
        "randomization_seed": 201,
        "horizon_sessions": 100,
        "horizon_hours": 1,
        "minimum_completion_rate": 1.0,
        "materialization_root": "materialized",
        "cost_model_status": "test-only",
        "transition_cost": {
            "copy_cost_per_gib": 0,
            "elapsed_time_cost_per_second": 0,
            "foreground_loss_per_transition": 0,
            "storage_cost_per_gib_hour": 0,
        },
        "naive_baseline": {
            "candidate_design_id": "D_local_digest",
            "representation_id": "multimodal_digest",
            "decision_margin": 0,
        },
        "designs": [
            {
                "design_id": "D_remote_digest",
                "materialization_decision": "reuse",
                "placement_decision": "remote",
                "execution_decision": "flowmesh",
                "materializations": [],
            },
            {
                "design_id": "D_local_digest",
                "materialization_decision": "copy",
                "placement_decision": "local",
                "execution_decision": "flowmesh",
                "materializations": [
                    {
                        "representation_id": "multimodal_digest",
                        "source_template": (
                            "sources/{object_id}/digest.txt"
                        ),
                        "target_template": (
                            "materialized/{object_id}/digest.txt"
                        ),
                    }
                ],
            },
        ],
    }
    path = root / "oracle.json"
    path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    return path


def _rewrite_incident_as_interrupted(
    output: Path,
    *,
    infrastructure_failures: int,
    missing_trials: int,
) -> list[dict[str, object]]:
    changed: list[dict[str, object]] = []
    remote_path = (
        output / "designs" / "D_remote_digest" / "runs.jsonl"
    )
    remote = [
        json.loads(line)
        for line in remote_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for record in remote[:infrastructure_failures]:
        record["outcome_type"] = "infrastructure_failure"
        record["telemetry_complete"] = None
        record["artifact_delivery_complete"] = None
        record["task_success"] = None
        record["workflow_id"] = None
        record["task_id"] = None
        record["flowmesh_status"] = "FAILED"
        record["final_answer"] = None
        record["error_type"] = "RuntimeError"
        record["error_message"] = "simulated incident"
        changed.append(record)
    remote_path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in remote),
        encoding="utf-8",
    )

    local_path = output / "designs" / "D_local_digest" / "runs.jsonl"
    local = [
        json.loads(line)
        for line in local_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if missing_trials:
        changed.extend(local[-missing_trials:])
        local = local[:-missing_trials]
    local_path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in local),
        encoding="utf-8",
    )
    return changed


class ReducedOracleCommittedContractTest(unittest.TestCase):
    def test_committed_plan_and_catalog_are_consistent(self) -> None:
        oracle = load_reduced_oracle_config(ORACLE_CONFIG_PATH)
        pilot = load_flowmesh_pilot_config(
            oracle.workload_pilot_config_path
        )
        manifest = load_data_agent_manifest(ORACLE_MANIFEST_PATH)

        self.assertEqual(
            ("D_remote_digest", "D_local_digest"),
            oracle.design_ids,
        )
        self.assertEqual(48, len(pilot.workloads) * oracle.repetitions * 2)
        self.assertIsNotNone(manifest.object_catalog)
        assert manifest.object_catalog is not None
        self.assertEqual(
            {workload.object_id for workload in pilot.workloads},
            set(manifest.object_catalog.objects),
        )
        for object_id, representations in manifest.object_catalog.objects.items():
            digest = representations["multimodal_digest"]
            local_path = digest.plan_paths["D_local_digest"]
            self.assertIn("phase_b_confirmatory_small", str(digest.path))
            self.assertIn("reduced_oracle_mvp", str(local_path))
            self.assertIn(object_id, str(local_path))
            local_path.relative_to(oracle.materialization_root)


class FilesystemTransitionExecutorTest(unittest.TestCase):
    def test_forward_copy_and_safe_restore_are_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_reduced_oracle_config(
                _temporary_oracle_config(root)
            )
            executor = FilesystemTransitionExecutor(
                config,
                object_ids=("object-a",),
                runtime_dir=root / "runtime",
            )
            source = root / "sources" / "object-a" / "digest.txt"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("stable digest\n", encoding="utf-8")

            executor.transition("D_remote_digest", transition_type="activate")
            forward = executor.transition(
                "D_local_digest",
                transition_type="forward",
            )
            target = root / "materialized" / "object-a" / "digest.txt"
            self.assertEqual(source.read_bytes(), target.read_bytes())
            self.assertEqual(source.stat().st_size, forward.copied_bytes)
            self.assertEqual(1, forward.files_created)

            restore = executor.transition(
                "D_remote_digest",
                transition_type="restore",
            )
            self.assertFalse(target.exists())
            self.assertEqual(1, restore.files_removed)
            self.assertEqual("D_remote_digest", executor.active_design_id())

    def test_reused_file_is_not_claimed_or_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_reduced_oracle_config(
                _temporary_oracle_config(root)
            )
            source = root / "sources" / "object-a" / "digest.txt"
            target = root / "materialized" / "object-a" / "digest.txt"
            source.parent.mkdir(parents=True, exist_ok=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("preexisting\n", encoding="utf-8")
            target.write_bytes(source.read_bytes())
            executor = FilesystemTransitionExecutor(
                config,
                object_ids=("object-a",),
                runtime_dir=root / "runtime",
            )

            forward = executor.transition(
                "D_local_digest",
                transition_type="forward",
            )
            self.assertEqual(1, forward.files_reused)
            executor.transition("D_remote_digest", transition_type="restore")
            self.assertTrue(target.exists())

    def test_modified_owned_file_is_never_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_reduced_oracle_config(
                _temporary_oracle_config(root)
            )
            source = root / "sources" / "object-a" / "digest.txt"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("original\n", encoding="utf-8")
            executor = FilesystemTransitionExecutor(
                config,
                object_ids=("object-a",),
                runtime_dir=root / "runtime",
            )
            executor.transition("D_local_digest", transition_type="forward")
            target = root / "materialized" / "object-a" / "digest.txt"
            target.write_text("operator changed this\n", encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeError,
                "refusing to remove a modified materialization",
            ):
                executor.transition(
                    "D_remote_digest",
                    transition_type="restore",
                )
            self.assertTrue(target.exists())

    def test_target_outside_materialization_root_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = _temporary_oracle_config(root)
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            payload["designs"][1]["materializations"][0][
                "target_template"
            ] = "outside/{object_id}/digest.txt"
            config_path.write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            config = load_reduced_oracle_config(config_path)
            executor = FilesystemTransitionExecutor(
                config,
                object_ids=("object-a",),
                runtime_dir=root / "runtime",
            )
            source = root / "sources" / "object-a" / "digest.txt"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source\n", encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeError,
                "escapes materialization_root",
            ):
                executor.transition(
                    "D_local_digest",
                    transition_type="forward",
                )
            self.assertFalse((root / "outside").exists())


class ReducedOracleRunnerTest(unittest.TestCase):
    def test_run_finds_oracle_lock_in_and_resumes_without_new_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_reduced_oracle_config(
                _temporary_oracle_config(root)
            )
            pilot = load_flowmesh_pilot_config(
                config.workload_pilot_config_path
            )
            system = load_config(pilot.system_config_path)
            output = root / "output"
            adapter = DesignAwareOracleAdapter()

            summary = run_reduced_oracle(
                config=config,
                system=system,
                adapter=adapter,
                output_dir=output,
            )
            self.assertEqual(16, len(adapter.requests))
            self.assertTrue(summary["safe_design_restored"])
            self.assertEqual(
                "D_local_digest",
                summary["steady_state_oracle_design_id"],
            )
            self.assertEqual(
                "D_local_digest",
                summary["transition_aware_oracle_design_id"],
            )
            self.assertTrue(summary["self_confirming_lock_in_observed"])
            self.assertFalse(any((root / "materialized").rglob("*.txt")))

            with (output / "oracle_table.csv").open(
                encoding="utf-8",
                newline="",
            ) as handle:
                table = {row["design_id"]: row for row in csv.DictReader(handle)}
            self.assertEqual("True", table["D_local_digest"]["objective_evaluable"])
            self.assertGreater(
                float(table["D_local_digest"]["phi"]),
                float(table["D_remote_digest"]["phi"]),
            )
            lock_in = json.loads(
                (output / "lock_in_trace.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                "D_remote_digest",
                lock_in["naive_final_design_id"],
            )

            resumed = DesignAwareOracleAdapter()
            second = run_reduced_oracle(
                config=config,
                system=system,
                adapter=resumed,
                output_dir=output,
            )
            self.assertEqual([], resumed.requests)
            self.assertEqual(
                summary["transition_aware_oracle_design_id"],
                second["transition_aware_oracle_design_id"],
            )

    def test_abrupt_probe_interruption_restores_safe_design(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_reduced_oracle_config(
                _temporary_oracle_config(root)
            )
            pilot = load_flowmesh_pilot_config(
                config.workload_pilot_config_path
            )
            system = load_config(pilot.system_config_path)
            output = root / "output"

            with self.assertRaisesRegex(
                KeyboardInterrupt,
                "simulated abrupt probe interruption",
            ):
                run_reduced_oracle(
                    config=config,
                    system=system,
                    adapter=InterruptingOracleAdapter(),
                    output_dir=output,
                )

            active = json.loads(
                (output / "runtime" / "active_plan.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual("D_remote_digest", active["active_design_id"])
            self.assertFalse(any((root / "materialized").rglob("*.txt")))


class ReducedOracleRecoveryTest(unittest.TestCase):
    def _incident(
        self,
        root: Path,
        *,
        infrastructure_failures: int,
        missing_trials: int,
    ) -> tuple[object, object, Path, list[dict[str, object]]]:
        config = load_reduced_oracle_config(
            _temporary_oracle_config(root)
        )
        pilot = load_flowmesh_pilot_config(
            config.workload_pilot_config_path
        )
        system = load_config(pilot.system_config_path)
        incident = root / "incident"
        run_reduced_oracle(
            config=config,
            system=system,
            adapter=DesignAwareOracleAdapter(),
            output_dir=incident,
        )
        changed = _rewrite_incident_as_interrupted(
            incident,
            infrastructure_failures=infrastructure_failures,
            missing_trials=missing_trials,
        )
        return config, system, incident, changed

    def test_recovery_preserves_incident_and_builds_complete_canonical_oracle(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, system, incident, changed = self._incident(
                root,
                infrastructure_failures=1,
                missing_trials=2,
            )
            recovery = root / "recovery"
            plan = plan_reduced_oracle_recovery(
                config=config,
                system=system,
                incident_dir=incident,
                recovery_dir=recovery,
            )
            self.assertEqual(16, plan["expected_trial_count"])
            self.assertEqual(3, plan["recoverable_trial_count"])
            before = {
                path.relative_to(incident): path.read_bytes()
                for path in incident.rglob("*")
                if path.is_file() and path.name not in {
                    ".oracle.lock",
                    ".pilot.lock",
                }
            }

            adapter = DesignAwareOracleAdapter()
            manifest = run_reduced_oracle_recovery(
                config=config,
                system=system,
                adapter=adapter,
                incident_dir=incident,
                recovery_dir=recovery,
            )

            self.assertEqual("COMPLETE", manifest["status"])
            self.assertEqual(3, len(adapter.requests))
            original_sessions = {
                str(record["session_id"]) for record in changed
            }
            self.assertTrue(
                original_sessions.isdisjoint(
                    request.session_id for request in adapter.requests
                )
            )
            after = {
                path.relative_to(incident): path.read_bytes()
                for path in incident.rglob("*")
                if path.is_file() and path.name not in {
                    ".oracle.lock",
                    ".pilot.lock",
                }
            }
            self.assertEqual(before, after)

            canonical = recovery / "canonical-oracle"
            all_records = []
            for design_id in config.design_ids:
                records = [
                    json.loads(line)
                    for line in (
                        canonical / "designs" / design_id / "runs.jsonl"
                    ).read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]
                self.assertEqual(8, len(records))
                self.assertTrue(
                    all(record["outcome_type"] == "completed" for record in records)
                )
                self.assertTrue(
                    all(record["telemetry_complete"] is True for record in records)
                )
                all_records.extend(records)
            self.assertEqual(16, len(all_records))
            self.assertEqual(
                3,
                sum(
                    record["canonical_source"] == "recovery_attempt"
                    for record in all_records
                ),
            )
            oracle_manifest = json.loads(
                (canonical / "oracle_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual("COMPLETE", oracle_manifest["status"])
            self.assertTrue(oracle_manifest["safe_design_restored"])

            no_op = DesignAwareOracleAdapter()
            resumed_manifest = run_reduced_oracle_recovery(
                config=config,
                system=system,
                adapter=no_op,
                incident_dir=incident,
                recovery_dir=recovery,
            )
            self.assertEqual("COMPLETE", resumed_manifest["status"])
            self.assertEqual([], no_op.requests)

    def test_consecutive_infrastructure_failures_open_circuit_and_restore_safe(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, system, incident, _ = self._incident(
                root,
                infrastructure_failures=4,
                missing_trials=0,
            )
            recovery = root / "recovery"
            adapter = FailingRecoveryAdapter()

            with self.assertRaisesRegex(
                RecoveryCircuitOpenError,
                "consecutive infrastructure failures",
            ):
                run_reduced_oracle_recovery(
                    config=config,
                    system=system,
                    adapter=adapter,
                    incident_dir=incident,
                    recovery_dir=recovery,
                    max_consecutive_infrastructure_failures=2,
                )

            self.assertEqual(2, len(adapter.requests))
            manifest = json.loads(
                (recovery / "recovery_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual("CIRCUIT_OPEN", manifest["status"])
            self.assertTrue(manifest["safe_design_restored"])
            self.assertFalse((recovery / "canonical-oracle").exists())

            resumed = DesignAwareOracleAdapter()
            completed = run_reduced_oracle_recovery(
                config=config,
                system=system,
                adapter=resumed,
                incident_dir=incident,
                recovery_dir=recovery,
                max_consecutive_infrastructure_failures=2,
            )
            self.assertEqual("COMPLETE", completed["status"])
            self.assertEqual(4, len(resumed.requests))
            attempts = [
                json.loads(line)
                for line in (recovery / "attempts.jsonl").read_text(
                    encoding="utf-8"
                ).splitlines()
                if line.strip()
            ]
            self.assertEqual(6, len(attempts))
            self.assertEqual(
                len({record["session_id"] for record in attempts}),
                len(attempts),
            )

    def test_incident_change_after_plan_is_rejected_before_submission(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, system, incident, _ = self._incident(
                root,
                infrastructure_failures=1,
                missing_trials=0,
            )
            recovery = root / "recovery"
            plan_reduced_oracle_recovery(
                config=config,
                system=system,
                incident_dir=incident,
                recovery_dir=recovery,
            )
            records_path = (
                incident / "designs" / "D_remote_digest" / "runs.jsonl"
            )
            records_path.write_bytes(records_path.read_bytes() + b"\n")
            adapter = DesignAwareOracleAdapter()

            with self.assertRaisesRegex(
                ReducedOracleRecoveryError,
                "incident evidence changed",
            ):
                run_reduced_oracle_recovery(
                    config=config,
                    system=system,
                    adapter=adapter,
                    incident_dir=incident,
                    recovery_dir=recovery,
                )
            self.assertEqual([], adapter.requests)

    def test_attempt_ledger_tampering_is_rejected_before_resumption(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, system, incident, _ = self._incident(
                root,
                infrastructure_failures=2,
                missing_trials=0,
            )
            recovery = root / "recovery"
            with self.assertRaises(RecoveryCircuitOpenError):
                run_reduced_oracle_recovery(
                    config=config,
                    system=system,
                    adapter=FailingRecoveryAdapter(),
                    incident_dir=incident,
                    recovery_dir=recovery,
                    max_consecutive_infrastructure_failures=1,
                )
            attempts_path = recovery / "attempts.jsonl"
            attempts = [
                json.loads(line)
                for line in attempts_path.read_text(
                    encoding="utf-8"
                ).splitlines()
                if line.strip()
            ]
            attempts[0]["error_message"] = "tampered after the fact"
            attempts_path.write_text(
                json.dumps(attempts[0], sort_keys=True) + "\n",
                encoding="utf-8",
            )
            adapter = DesignAwareOracleAdapter()

            with self.assertRaisesRegex(
                ReducedOracleRecoveryError,
                "payload digest is invalid",
            ):
                run_reduced_oracle_recovery(
                    config=config,
                    system=system,
                    adapter=adapter,
                    incident_dir=incident,
                    recovery_dir=recovery,
                )
            self.assertEqual([], adapter.requests)

    def test_non_retryable_research_failure_blocks_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, system, incident, _ = self._incident(
                root,
                infrastructure_failures=1,
                missing_trials=0,
            )
            path = incident / "designs" / "D_remote_digest" / "runs.jsonl"
            records = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            records[0]["outcome_type"] = "telemetry_failure"
            records[0]["telemetry_complete"] = False
            path.write_text(
                "".join(
                    json.dumps(record, sort_keys=True) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ReducedOracleRecoveryError,
                "automatic recovery is forbidden",
            ):
                plan_reduced_oracle_recovery(
                    config=config,
                    system=system,
                    incident_dir=incident,
                    recovery_dir=root / "recovery",
                )


if __name__ == "__main__":
    unittest.main()
