from __future__ import annotations

import contextlib
import io
import json
import socket
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from pathfinder.cli import main
from pathfinder.evaluation import EvaluationError, evaluate_distributed_pilot
from pathfinder.evaluation.example import create_evaluation_example
from pathfinder.distributed.execution import (
    TrialExecution, build_frozen_plan_document, run_distributed_pilot,
)
from pathfinder.distributed.measurements import (
    build_measured_cost_ledger, load_measurement_manifest,
)
from pathfinder.distributed.preregistration import load_distributed_pilot_preregistration
from pathfinder.distributed.registry import load_endpoint_registry


EXPECTED = Path(__file__).parent / "fixtures/workload_evaluation/expected.json"


def write(path: Path, payload: object) -> None:
    content = ("".join(json.dumps(row) + "\n" for row in payload)
               if path.suffix == ".jsonl" else json.dumps(payload))
    path.write_text(content, encoding="utf-8")


def read(path: Path):
    content = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in content.splitlines() if line.strip()]
    return json.loads(content)


def snapshot(root: Path) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes()
            for p in root.rglob("*") if p.is_file()}


class WorkloadEvaluationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "example"
        create_evaluation_example(self.source)
        self.config, self.run = self.source / "config", self.source / "run"
        self.output = self.root / "evaluation"
        self.records_path = self.run / "canonical_records.jsonl"
        self.attempts_path = self.run / "attempt_ledger.jsonl"

    def evaluate(self, output=None, run=None):
        return evaluate_distributed_pilot(
            run or self.run, preregistration=self.config / "preregistration.json",
            endpoint_registry=self.config / "endpoint-registry.json",
            workload_manifest=self.config / "workloads.json",
            measurement_manifest=self.config / "measurements.json",
            output_dir=output or self.output,
        )

    def expect_refusal(self, pattern=None):
        before = snapshot(self.source)
        with self.assertRaises((ValueError, RuntimeError, OSError)) as caught:
            self.evaluate()
        if pattern:
            self.assertIn(pattern, str(caught.exception))
        self.assertEqual(snapshot(self.source), before)
        self.assertFalse(self.output.exists())

    def test_hand_calculated_reference(self):
        result, expected = self.evaluate(), read(EXPECTED)
        for key in ("planned_trials", "independent_workloads", "attempt_records",
                    "attempt_classes"):
            self.assertEqual(result[key], expected[key])
        safe, candidate = (result["by_role"][key]
                           for key in ("safe", "restricted_candidate"))
        self.assertEqual(safe["task_successes"], expected["safe_successes"])
        self.assertEqual(candidate["task_successes"], expected["candidate_successes"])
        for label, group in (("safe", safe), ("candidate", candidate)):
            self.assertAlmostEqual(group["mean_total_cost_per_session"],
                                   expected[label + "_mean_total_cost"])
            self.assertAlmostEqual(group["mean_cross_node_payload_bytes_per_session"],
                                   expected[label + "_cross_node_bytes"])
        aggregate = result["paired_aggregates"][0]
        self.assertAlmostEqual(aggregate["mean_task_success_delta"], expected["mean_success_delta"])
        self.assertAlmostEqual(aggregate["mean_total_cost_delta"], expected["mean_total_cost_delta"])
        for pair in result["paired_workload_effects"]:
            self.assertAlmostEqual(pair["total_cost_delta"],
                                   expected["per_workload_total_cost_delta"][pair["workload_id"]])
        self.assertEqual(result["failure_classes"], {"infrastructure_failure": 1})
        self.assertEqual(result["input_origin"], "synthetic-format-example")
        self.assertIs(result["eligible_for_scientific_claims"], False)

    def test_routing_uses_actual_representation_not_design_name(self):
        result = self.evaluate()
        causal = next(group for group in result["by_stratum_design"]
                      if group["stratum_id"] == "causal" and group["design_id"] == "D_local_frames")
        self.assertEqual(causal["routes"][0]["endpoint_id"], "origin_remote")
        self.assertEqual(causal["routes"][0]["destination_execution_node_id"], "example-execution")
        self.assertEqual(causal["routes"][0]["destination_basis"], "recorded")

    def test_missing_destination_is_labeled_inferred_not_measured(self):
        records = read(self.records_path)
        del records[0]["access_events"][0]["destination_execution_node_id"]
        write(self.records_path, records)
        result = self.evaluate()
        self.assertTrue(any("1 access destinations" in text for text in result["limitations"]))
        self.assertTrue(any(route["destination_basis"] == "inferred-from-frozen-registry"
                            for group in result["by_stratum_design"] for route in group["routes"]))

    def test_artifact_bytes_are_not_double_counted_or_charged_for_local_traffic(self):
        result = self.evaluate()
        groups = {r["design_id"]: r for r in result["by_stratum_design"]
                  if r["stratum_id"] == "descriptive"}
        remote, local = groups["D_origin_remote"], groups["D_local_frames"]
        self.assertEqual(remote["mean_cross_node_payload_bytes_per_session"], 400)
        self.assertEqual(local["mean_cross_node_payload_bytes_per_session"], 0)
        self.assertEqual(local["mean_payload_bytes_per_session"], 400)
        self.assertEqual(remote["mean_component_cost_per_session"]["network"], 400 / 1024)

    def test_missing_latency_is_not_zero_and_percentiles_are_declared(self):
        records = read(self.records_path)
        del records[0]["access_events"][0]["felt_latency_ms"]
        write(self.records_path, records)
        stats = self.evaluate()["by_role"]["safe"]["latency_ms_per_access"]["felt_latency_ms"]
        self.assertEqual(stats, {"count": 5, "missing_count": 1,
                                 "mean": 30, "median": 30, "p95": 30})

    def test_configured_quantities_remain_distinguishable(self):
        result = self.evaluate()
        self.assertEqual(result["declared_measurement_kinds"]["storage"],
                         {"configured": 4, "not_applicable": 2})
        self.assertIn("Synthetic", result["cost_rate_provenance"])
        self.assertEqual(result["by_role"]["safe"]["latency_ms_per_access"]
                         ["data_agent_controlled_delay_ms"]["mean"], 10)

    def test_no_network_or_execution_and_inputs_byte_identical(self):
        before = snapshot(self.source)
        with patch.object(socket.socket, "connect", side_effect=AssertionError("network")), \
             patch("urllib.request.urlopen", side_effect=AssertionError("network")), \
             patch("pathfinder.distributed.execution.run_distributed_pilot",
                   side_effect=AssertionError("runner")):
            self.evaluate()
        self.assertEqual(snapshot(self.source), before)

    def test_repeated_evaluation_is_byte_reproducible_and_hashes_verify(self):
        self.evaluate()
        other = self.root / "second-report"
        self.evaluate(output=other)
        self.assertEqual(snapshot(self.output), snapshot(other))
        for line in (self.output / "SHA256SUMS").read_text().splitlines():
            digest, name = line.split("  ")
            self.assertEqual(digest, sha256((self.output / name).read_bytes()).hexdigest())
        manifest = read(self.output / "evaluation_manifest.json")
        self.assertEqual(len(manifest["input_files_sha256"]), 9)
        self.assertEqual(manifest["input_files_sha256"]["canonical_records.jsonl"],
                         sha256(self.records_path.read_bytes()).hexdigest())

    def test_example_is_deterministic_and_refuses_overwrite(self):
        second = self.root / "second-example"
        create_evaluation_example(second)
        self.assertEqual(snapshot(self.source), snapshot(second))
        before = snapshot(self.source)
        with self.assertRaises(EvaluationError):
            create_evaluation_example(self.source)
        self.assertEqual(snapshot(self.source), before)

    def test_output_overwrite_and_overlap_are_refused(self):
        self.evaluate()
        before = snapshot(self.output)
        with self.assertRaises(EvaluationError):
            self.evaluate()
        self.assertEqual(snapshot(self.output), before)
        for path in (self.run / "reports", self.config / "reports", self.source / "run/reports"):
            with self.subTest(path=path), self.assertRaises(EvaluationError):
                self.evaluate(output=path)
            self.assertFalse(path.exists())

    def test_incomplete_snapshot_and_duplicate_records_refused(self):
        records = read(self.records_path)
        for malformed in (records[:-1], records + [records[0]]):
            with self.subTest(length=len(malformed)):
                write(self.records_path, malformed)
                self.expect_refusal()

    def test_strict_completeness_and_identity(self):
        records = read(self.records_path)
        cases = [(field, value) for field in ("telemetry_complete", "artifact_delivery_complete")
                 for value in (None, False, 1, "true")]
        cases += [("outcome_type", "artifact_delivery_failure"), ("task_success", "true"),
                  ("is_safe_design", 1), ("repetition", False), ("order_index", 1),
                  ("schema_version", "foreign"), ("experiment_id", "foreign")]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                changed = json.loads(json.dumps(records))
                changed[0][field] = value
                write(self.records_path, changed)
                self.expect_refusal()

    def test_artifact_missing_download_is_not_task_failure_or_dropped(self):
        records = read(self.records_path)
        index = next(i for i, row in enumerate(records) if row["artifact_delivery_required"])
        for field in ("artifact_bytes_sent", "artifact_download_request_count", "artifact_full_download_count"):
            for value in (0, None, "1"):
                with self.subTest(field=field, value=value):
                    changed = json.loads(json.dumps(records))
                    changed[index]["access_events"][0][field] = value
                    write(self.records_path, changed)
                    self.expect_refusal()

    def test_changed_scoring_labels_questions_or_objects_are_refused(self):
        records = read(self.records_path)
        for field, value in (("task_success", False), ("question", "different"),
                             ("accepted_answer_substrings", ["red"]), ("object_id", "other")):
            with self.subTest(field=field):
                changed = json.loads(json.dumps(records))
                changed[0][field] = value
                write(self.records_path, changed)
                self.expect_refusal()

    def test_cost_tampering_and_missing_components_are_refused(self):
        records = read(self.records_path)
        for mutation in ("total", "component", "missing", "availability", "negative", "bool"):
            with self.subTest(mutation=mutation):
                changed = json.loads(json.dumps(records))
                ledger = changed[0]["cost_ledger"]
                if mutation == "total":
                    ledger["total_cost"] += 1
                elif mutation == "component":
                    ledger["components"]["service"]["value"] += 1
                    ledger["total_cost"] += 1
                elif mutation == "missing":
                    del ledger["components"]["storage"]
                elif mutation == "availability":
                    ledger["total_cost_available"] = "true"
                else:
                    ledger["components"]["service"]["value"] = -1 if mutation == "negative" else True
                write(self.records_path, changed)
                self.expect_refusal()

    def test_invalid_event_routes_and_quantities_are_refused(self):
        records = read(self.records_path)
        for field, value in (("accepted", 1), ("source_node_id", "wrong-node"),
                             ("endpoint_id", "wrong"), ("destination_execution_node_id", "wrong"),
                             ("session_id", "foreign"), ("realized_cost", -1),
                             ("bytes_read", True), ("felt_latency_ms", "30")):
            with self.subTest(field=field):
                changed = json.loads(json.dumps(records))
                changed[0]["access_events"][0][field] = value
                write(self.records_path, changed)
                self.expect_refusal()

    def test_missing_attempts_and_journal_crash_window_are_not_repaired(self):
        attempts = read(self.attempts_path)
        write(self.attempts_path, attempts[:-1])
        self.expect_refusal("missing planned cells")
        write(self.attempts_path, attempts)
        journal_path = self.run / "cell_journal.jsonl"
        journal = read(journal_path)
        journal[-1]["state"] = "CANONICAL_WRITTEN"
        write(journal_path, journal)
        self.expect_refusal("not fully COMPLETED")

    def test_attempt_number_and_failure_class_audited(self):
        attempts = read(self.attempts_path)
        for field, value in (("attempt", 3), ("succeeded", True),
                             ("failure_class", None), ("telemetry_complete", "true")):
            with self.subTest(field=field):
                changed = json.loads(json.dumps(attempts))
                changed[0][field] = value
                write(self.attempts_path, changed)
                self.expect_refusal()

    def test_workload_or_registry_binding_cannot_be_changed(self):
        path = self.config / "workloads.json"
        workloads = read(path)
        workloads["example-causal"]["question"] = "New question"
        write(path, workloads)
        self.expect_refusal("frozen plan disagrees")

    def test_stale_measurement_binding_is_refused(self):
        path = self.config / "measurements.json"
        payload = read(path)
        payload["endpoint_registry_sha256"] = "a" * 64
        write(path, payload)
        self.expect_refusal()

    def test_measurement_provenance_change_cannot_be_substituted(self):
        path = self.config / "measurements.json"
        payload = read(path)
        # This leaves every numeric ledger unchanged but must not be allowed
        # to upgrade invented configured quantities to real measurements.
        for entry in payload["measurements"]:
            if entry["storage"]["kind"] == "configured":
                entry["storage"]["kind"] = "measured"
        write(path, payload)
        self.expect_refusal("measurement_manifest_sha256")

    def test_final_summary_must_agree_with_the_complete_ledgers(self):
        path = self.run / "run_summary.jsonl"
        summaries = read(path)
        for field, value in (("complete", "true"), ("pilot_id", "foreign"),
                             ("completed_canonical_count", 13),
                             ("workload_content_sha256", "a" * 64)):
            with self.subTest(field=field):
                changed = json.loads(json.dumps(summaries))
                changed[-1][field] = value
                write(path, changed)
                self.expect_refusal("run summary")

    def test_shared_video_not_counted_as_independent_workloads(self):
        path = self.config / "workloads.json"
        workloads = read(path)
        workloads["example-causal"]["object_id"] = workloads["example-temporal"]["object_id"]
        write(path, workloads)
        self.expect_refusal("independent-unit count")

    def test_bad_json_duplicate_key_and_nonfinite_values_refused(self):
        for content in ('{"status":1,"status":2}', '{"x":NaN}', '{'):
            with self.subTest(content=content):
                (self.run / "distributed_pilot_plan.json").write_text(content)
                self.expect_refusal()

    def test_reports_exclude_answers_handles_and_runtime_metadata(self):
        records = read(self.records_path)
        row = next(r for r in records if r["artifact_delivery_required"])
        row["access_events"][0]["artifact_handle"] = "PRIVATE-HANDLE-DO-NOT-PRINT"
        row["access_events"][0]["artifact_handle_sha256"] = sha256(
            b"PRIVATE-HANDLE-DO-NOT-PRINT").hexdigest()
        row["final_answer"] = "PRIVATE-ANSWER blue"
        row["private_debug"] = "PRIVATE-DEBUG"
        write(self.records_path, records)
        self.evaluate()
        for content in snapshot(self.output).values():
            self.assertNotIn(b"PRIVATE-", content)

    def test_no_accepted_access_remains_visible(self):
        records = read(self.records_path)
        row = records[0]
        row["access_events"][0]["accepted"] = False
        row["accepted_access_count"] = 0
        row["selected_representations"] = []
        prereg = load_distributed_pilot_preregistration(self.config / "preregistration.json")
        registry = load_endpoint_registry(self.config / "endpoint-registry.json")
        row["cost_ledger"] = build_measured_cost_ledger(
            row, model=prereg.cost_model,
            provider=load_measurement_manifest(self.config / "measurements.json"),
            design_id=row["design_id"], object_id=row["object_id"],
            node_id=row["access_events"][0]["source_node_id"],
            network_transport_for={key: e.network_transport for key, e in registry.endpoints.items()},
        ).to_public_dict()
        write(self.records_path, records)
        result = self.evaluate()
        self.assertEqual(result["by_role"]["safe"]["no_access_sessions"], 1)
        self.assertEqual(result["by_role"]["safe"]["sessions"], 6)
        self.assertEqual(result["by_role"]["safe"]["accepted_accesses"], 5)

    def test_unscored_tasks_are_unavailable_not_failures(self):
        path = self.config / "workloads.json"
        workloads = read(path)
        workloads["example-causal"]["accepted_answer_substrings"] = []
        write(path, workloads)
        prereg = load_distributed_pilot_preregistration(self.config / "preregistration.json")
        registry = load_endpoint_registry(self.config / "endpoint-registry.json")
        plan = build_frozen_plan_document(prereg, registry, workloads=workloads)
        write(self.run / "distributed_pilot_plan.json", plan)
        summaries = read(self.run / "run_summary.jsonl")
        summaries[-1]["workload_content_sha256"] = plan["workload_content_sha256"]
        write(self.run / "run_summary.jsonl", summaries)
        records = read(self.records_path)
        for row in records:
            if row["workload_id"] == "example-causal":
                row["accepted_answer_substrings"] = []
                row["task_success"] = None
        write(self.records_path, records)
        result = self.evaluate()
        self.assertEqual(result["by_role"]["safe"]["evaluable_tasks"], 4)
        self.assertEqual(result["paired_aggregates"][0]["independent_workloads"], 3)
        self.assertEqual(result["paired_aggregates"][0]["evaluable_pairs"], 2)

    def test_current_runtime_writer_interoperability(self):
        prereg = load_distributed_pilot_preregistration(self.config / "preregistration.json")
        registry = load_endpoint_registry(self.config / "endpoint-registry.json")
        workloads = read(self.config / "workloads.json")
        provider = load_measurement_manifest(self.config / "measurements.json")
        records = {r["trial_key"]: r for r in read(self.records_path)}

        class LocalFixtureExecutor:
            def execute(self, trial, **kwargs):
                row = records[trial.trial_key]
                event = dict(row["access_events"][0], session_id=trial.session_id)
                return TrialExecution(final_answer=row["final_answer"],
                                      access_events=(event,), status="DONE")

        output = self.root / "actual-writer"
        run_distributed_pilot(prereg, registry, LocalFixtureExecutor(), output_dir=output,
                              workloads=workloads, provider=provider, require_preflight=False)
        result = self.evaluate(run=output)
        self.assertEqual(result["attempt_classes"], {"canonical": 12})
        self.assertEqual(result["latest_journal_states"], {"COMPLETED": 12})

    def test_cli_success_and_actionable_failure_exit_codes(self):
        args = ["evaluate-distributed-pilot", "--run-dir", str(self.run),
                "--preregistration", str(self.config / "preregistration.json"),
                "--endpoint-registry", str(self.config / "endpoint-registry.json"),
                "--workload-manifest", str(self.config / "workloads.json"),
                "--measurement-manifest", str(self.config / "measurements.json"),
                "--output-dir", str(self.output)]
        with contextlib.redirect_stdout(io.StringIO()) as captured:
            self.assertEqual(main(args), 0)
        self.assertEqual(json.loads(captured.getvalue())["status"], "COMPLETE")
        with contextlib.redirect_stdout(io.StringIO()) as captured:
            self.assertEqual(main(args), 2)
        self.assertIn("already exists", captured.getvalue())


if __name__ == "__main__":
    unittest.main()
