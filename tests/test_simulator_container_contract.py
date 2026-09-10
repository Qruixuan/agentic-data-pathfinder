from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.cli import main as cli_main
from pathfinder.simulator import (
    BackendParityError,
    ContainerContractError,
    PortablePlanError,
    build_portable_execution_plan,
    evaluate_backend_parity,
    plan_container_backend,
    run_simulator_scenario,
    verify_backend_parity,
    verify_container_backend_plan,
    verify_portable_execution_plan,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


class PortableExecutionPlanTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_plan_is_complete_deterministic_and_backend_neutral(self) -> None:
        first = self.root / "portable-a"
        second = self.root / "portable-b"
        report = build_portable_execution_plan(SCENARIO, output_dir=first)
        build_portable_execution_plan(SCENARIO, output_dir=second)
        self.assertEqual(64, report["planned_trial_count"])
        self.assertGreater(report["planned_operation_count"], 64)
        self.assertEqual(
            sorted(path.name for path in first.iterdir()),
            sorted(path.name for path in second.iterdir()),
        )
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        verified = verify_portable_execution_plan(first)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(64, verified["planned_trial_count"])
        self.assertEqual(4, verified["trial_admission"]["slots"])
        self.assertEqual(
            "fifo-by-arrival-time-then-order-index",
            verified["trial_admission"]["algorithm"],
        )

    def test_plan_resolves_bytes_dependencies_and_exact_bindings(self) -> None:
        output = self.root / "portable"
        build_portable_execution_plan(SCENARIO, output_dir=output)
        trials = _jsonl(output / "trials.jsonl")
        trial = next(
            row for row in trials
            if row["workload_id"] == "smoke-descriptive"
            and row["design_id"] == "D0"
            and row["repetition"] == 0
        )
        rows = {
            row["operation_id"]: row
            for row in _jsonl(output / "operations.jsonl")
            if row["trial_key"] == trial["trial_key"]
        }
        self.assertEqual(120_000_000, rows["read-raw"]["logical_bytes"])
        self.assertEqual("N3.hdd", rows["read-raw"]["resource_binding"]["resource_id"])
        self.assertEqual(
            "N3-N7-cold",
            rows["transfer-raw"]["link_binding"]["link_id"],
        )
        self.assertEqual(
            [f"{trial['trial_key']}|read-raw"],
            rows["transfer-raw"]["dependency_operation_keys"],
        )
        self.assertTrue(
            rows["infer"]["simulation_hints"][
                "not_an_instruction_to_sleep_in_measured_backends"
            ]
        )

    def test_simulated_success_is_not_promoted_into_portable_plan(self) -> None:
        output = self.root / "portable"
        build_portable_execution_plan(SCENARIO, output_dir=output)
        plan_text = (output / "portable_plan.json").read_text(encoding="utf-8")
        trial_text = (output / "trials.jsonl").read_text(encoding="utf-8")
        operation_text = (output / "operations.jsonl").read_text(encoding="utf-8")
        self.assertNotIn("task_success_by_design", plan_text)
        self.assertNotIn('"task_success":', trial_text)
        self.assertNotIn('"task_success":', operation_text)
        plan = json.loads(plan_text)
        self.assertFalse(plan["task_success_values_from_scenario_included"])
        metric = json.loads(
            (output / "metric_contract.json").read_text(encoding="utf-8")
        )
        self.assertIsNone(metric["parity_thresholds"])
        self.assertEqual(
            "must-be-preregistered-before-evaluation",
            metric["parity_threshold_status"],
        )

    def test_checksum_tampering_is_rejected(self) -> None:
        output = self.root / "portable"
        build_portable_execution_plan(SCENARIO, output_dir=output)
        path = output / "metric_contract.json"
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(PortablePlanError, "checksum mismatch"):
            verify_portable_execution_plan(output)


class ContainerBackendContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.portable = self.root / "portable"
        build_portable_execution_plan(SCENARIO, output_dir=self.portable)

    def _mutated_spec(self, mutate) -> Path:
        payload = json.loads(CONTAINER_SPEC.read_text(encoding="utf-8"))
        mutate(payload)
        path = self.root / f"spec-{len(list(self.root.iterdir()))}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_complete_contract_maps_all_portable_operations(self) -> None:
        output = self.root / "container-plan"
        report = plan_container_backend(
            SCENARIO,
            self.portable,
            CONTAINER_SPEC,
            output_dir=output,
        )
        portable_operations = _jsonl(self.portable / "operations.jsonl")
        container_operations = _jsonl(output / "container_operations.jsonl")
        self.assertEqual(len(portable_operations), len(container_operations))
        self.assertEqual(
            [row["operation_key"] for row in portable_operations],
            [row["operation_key"] for row in container_operations],
        )
        self.assertEqual(
            [row["logical_bytes"] for row in portable_operations],
            [row["logical_bytes"] for row in container_operations],
        )
        infer = next(
            row for row in container_operations
            if row["operation_id"] == "infer"
        )
        self.assertEqual("N6", infer["execution_node_id"])
        transfer = next(
            row for row in container_operations
            if row["operation_id"] == "transfer-raw"
        )
        self.assertEqual("N3", transfer["execution_node_id"])
        self.assertEqual("N7", transfer["destination_node_id"])
        self.assertEqual("CONTRACT_READY_LAUNCH_UNVERIFIED", report["readiness_status"])
        self.assertEqual(2, report["launch_blocker_count"])
        verified = verify_container_backend_plan(output)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertFalse(report["launch_authorized"])
        self.assertFalse(report["container_started"])

    def test_readiness_separates_contract_from_live_host_evidence(self) -> None:
        output = self.root / "container-plan"
        plan_container_backend(
            SCENARIO,
            self.portable,
            CONTAINER_SPEC,
            output_dir=output,
        )
        readiness = json.loads(
            (output / "container_readiness.json").read_text(encoding="utf-8")
        )
        self.assertTrue(readiness["contract_complete"])
        self.assertFalse(readiness["launch_authorized"])
        self.assertFalse(readiness["docker_or_host_probed"])
        blocker_ids = {
            row["check_id"] for row in readiness["launch_blockers"]
        }
        self.assertEqual(
            {"host_runtime_capabilities", "immutable_container_images"},
            blocker_ids,
        )
        warning_ids = {
            row["check_id"] for row in readiness["advisory_warnings"]
        }
        self.assertEqual(
            {"semantic_quality_disabled", "synthetic_payload_content"},
            warning_ids,
        )

    def test_missing_or_wrong_exact_mapping_is_rejected_before_output(self) -> None:
        cases = (
            (
                lambda payload: payload["link_adapters"].pop(),
                "link adapter coverage mismatch",
            ),
            (
                lambda payload: payload["resource_adapters"][0].__setitem__(
                    "node_id", "N8"
                ),
                "resource node mismatch",
            ),
            (
                lambda payload: payload["operation_adapters"].pop("storage_read"),
                "operation adapter coverage mismatch",
            ),
            (
                lambda payload: payload["task_executors"].pop(),
                "task executor coverage mismatch",
            ),
        )
        for index, (mutate, message) in enumerate(cases):
            with self.subTest(index=index):
                spec = self._mutated_spec(mutate)
                output = self.root / f"bad-{index}"
                with self.assertRaisesRegex(ContainerContractError, message):
                    plan_container_backend(
                        SCENARIO,
                        self.portable,
                        spec,
                        output_dir=output,
                    )
                self.assertFalse(output.exists())

    def test_credentials_and_unbound_scenario_are_rejected(self) -> None:
        sensitive = self._mutated_spec(
            lambda payload: payload.__setitem__("api_key", "must-not-be-here")
        )
        with self.assertRaisesRegex(ContainerContractError, "credential-like"):
            plan_container_backend(
                SCENARIO,
                self.portable,
                sensitive,
                output_dir=self.root / "sensitive",
            )
        wrong_scenario = self._mutated_spec(
            lambda payload: payload.__setitem__("scenario_id", "other")
        )
        with self.assertRaisesRegex(ContainerContractError, "scenario_id mismatch"):
            plan_container_backend(
                SCENARIO,
                self.portable,
                wrong_scenario,
                output_dir=self.root / "wrong-scenario",
            )

    def test_cli_builds_both_layers_without_launching_services(self) -> None:
        portable = self.root / "cli-portable"
        container = self.root / "cli-container"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "build-portable-execution-plan",
                "--scenario",
                str(SCENARIO),
                "--output-dir",
                str(portable),
                "--compact",
            ])
        self.assertEqual(0, status)
        self.assertFalse(json.loads(stdout.getvalue())["container_started"])
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "plan-container-simulation",
                "--scenario",
                str(SCENARIO),
                "--portable-plan-dir",
                str(portable),
                "--container-spec",
                str(CONTAINER_SPEC),
                "--output-dir",
                str(container),
                "--compact",
            ])
        self.assertEqual(0, status)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["container_started"])
        self.assertFalse(payload["launch_authorized"])


class BackendParityEvaluationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.portable = self.root / "portable"
        self.simulation = self.root / "simulation"
        build_portable_execution_plan(SCENARIO, output_dir=self.portable)
        run_simulator_scenario(SCENARIO, output_dir=self.simulation)
        self.reference = self.simulation / "canonical_records.jsonl"

    def _mutated_records(self, mutate) -> Path:
        rows = _jsonl(self.reference)
        mutate(rows)
        path = self.root / f"records-{len(list(self.root.iterdir()))}.jsonl"
        path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
        return path

    def _infrastructure_records(self) -> Path:
        def mutate(rows: list[dict]) -> None:
            for row in rows:
                row["task_success"] = None
                row["semantic_task_quality_evaluated"] = False
                row["quality_provenance"] = (
                    "unavailable-in-infrastructure-only-container-run"
                )

        return self._mutated_records(mutate)

    def test_identical_ledgers_produce_zero_descriptive_differences(self) -> None:
        output = self.root / "parity"
        report = evaluate_backend_parity(
            self.portable,
            self.reference,
            self.reference,
            reference_label="discrete-event",
            candidate_label="container-fixture",
            output_dir=output,
        )
        self.assertEqual(
            "DESCRIPTIVE_ONLY_THRESHOLDS_UNSET",
            report["evaluation_status"],
        )
        self.assertTrue(report["design_latency_rank_exact_match"])
        payload = json.loads(
            (output / "parity_report.json").read_text(encoding="utf-8")
        )
        overall = payload["aggregates"][0]
        self.assertEqual(0.0, overall["mean_latency_delta_ms"])
        self.assertEqual(0.0, overall["mean_network_bytes_delta"])
        self.assertFalse(payload["parity_claim_made"])
        self.assertEqual("VERIFIED", verify_backend_parity(output)["status"])

    def test_measured_difference_is_reported_without_inventing_a_threshold(self) -> None:
        def add_latency(rows: list[dict]) -> None:
            rows[0]["latency_ms"] += 25.0
            rows[0]["active_execution_latency_ms"] += 25.0

        candidate = self._mutated_records(add_latency)
        output = self.root / "parity"
        evaluate_backend_parity(
            self.portable,
            self.reference,
            candidate,
            reference_label="discrete-event",
            candidate_label="container-fixture",
            output_dir=output,
        )
        payload = json.loads(
            (output / "parity_report.json").read_text(encoding="utf-8")
        )
        self.assertGreater(
            payload["aggregates"][0]["mean_latency_delta_ms"],
            0.0,
        )
        self.assertIsNone(payload["parity_thresholds"])
        self.assertFalse(payload["parity_claim_made"])

    def test_infrastructure_only_scope_excludes_semantic_quality(self) -> None:
        candidate = self._infrastructure_records()
        output = self.root / "infrastructure-parity"
        report = evaluate_backend_parity(
            self.portable,
            self.reference,
            candidate,
            reference_label="discrete-event",
            candidate_label="container-infrastructure",
            output_dir=output,
            comparison_scope="infrastructure-only",
        )
        self.assertEqual("infrastructure-only", report["comparison_scope"])
        payload = json.loads(
            (output / "parity_report.json").read_text(encoding="utf-8")
        )
        self.assertFalse(payload["semantic_task_quality_compared"])
        self.assertTrue(
            payload["infrastructure_only_does_not_establish_task_quality"]
        )
        self.assertIn("task_success", payload["excluded_metrics"])
        overall = payload["aggregates"][0]
        self.assertIsNone(overall["task_success_change_count"])
        self.assertFalse(overall["semantic_task_quality_compared"])
        self.assertIn("network", overall["mean_resource_service_ms_delta"])
        pairs = _jsonl(output / "parity_pairs.jsonl")
        self.assertTrue(all(
            row["reference_task_success"] is None
            and row["candidate_task_success"] is None
            and row["task_success_changed"] is None
            and row["task_success_comparison_status"]
            == "EXCLUDED_BY_INFRASTRUCTURE_ONLY_SCOPE"
            for row in pairs
        ))
        verified = verify_backend_parity(output)
        self.assertEqual("infrastructure-only", verified["comparison_scope"])
        self.assertFalse(verified["semantic_task_quality_compared"])

    def test_full_scope_still_rejects_missing_semantic_result(self) -> None:
        candidate = self._infrastructure_records()
        with self.assertRaisesRegex(
            BackendParityError,
            "task_success must be literal boolean",
        ):
            evaluate_backend_parity(
                self.portable,
                self.reference,
                candidate,
                reference_label="discrete-event",
                candidate_label="container-infrastructure",
                output_dir=self.root / "full-rejects-null",
            )

    def test_infrastructure_null_requires_explicit_quality_provenance(self) -> None:
        candidate = self._infrastructure_records()
        rows = _jsonl(candidate)
        rows[0].pop("semantic_task_quality_evaluated")
        candidate.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            BackendParityError,
            "requires explicit semantic_task_quality_evaluated=false",
        ):
            evaluate_backend_parity(
                self.portable,
                self.reference,
                candidate,
                reference_label="discrete-event",
                candidate_label="container-infrastructure",
                output_dir=self.root / "missing-quality-provenance",
                comparison_scope="infrastructure-only",
            )

    def test_cli_exposes_infrastructure_only_scope(self) -> None:
        candidate = self._infrastructure_records()
        output = self.root / "cli-infrastructure-parity"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "evaluate-backend-parity",
                "--portable-plan-dir",
                str(self.portable),
                "--reference-records",
                str(self.reference),
                "--candidate-records",
                str(candidate),
                "--reference-label",
                "discrete-event",
                "--candidate-label",
                "container-infrastructure",
                "--comparison-scope",
                "infrastructure-only",
                "--output-dir",
                str(output),
                "--compact",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "infrastructure-only",
            json.loads(stdout.getvalue())["comparison_scope"],
        )

    def test_incomplete_or_identity_changed_candidate_is_rejected(self) -> None:
        cases = (
            (lambda rows: rows.pop(), "exact frozen trial set"),
            (
                lambda rows: rows[0].__setitem__("design_id", "other"),
                "identity field design_id",
            ),
            (
                lambda rows: rows[0].__setitem__("telemetry_complete", False),
                "telemetry is incomplete",
            ),
        )
        for index, (mutate, message) in enumerate(cases):
            with self.subTest(index=index):
                candidate = self._mutated_records(mutate)
                output = self.root / f"bad-parity-{index}"
                with self.assertRaisesRegex(BackendParityError, message):
                    evaluate_backend_parity(
                        self.portable,
                        self.reference,
                        candidate,
                        reference_label="discrete-event",
                        candidate_label="container-fixture",
                        output_dir=output,
                    )
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
