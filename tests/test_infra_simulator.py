from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.cli import main as cli_main
from pathfinder.simulator import (
    SimulatorConfigError,
    build_simulator_trials,
    load_simulator_scenario,
    run_discrete_event_simulation,
    run_simulator_scenario,
    verify_simulator_run,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO_PATH = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"


class SimulatorConfigurationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scenario = load_simulator_scenario(SCENARIO_PATH)

    def test_reference_scenario_declares_the_complete_4x8_smoke(self) -> None:
        self.assertEqual("flowmesh-infra-4x8-local-smoke-v1", self.scenario.scenario_id)
        self.assertEqual(8, len(self.scenario.nodes))
        self.assertEqual(4, len(self.scenario.workloads))
        self.assertEqual(8, len(self.scenario.designs))
        self.assertEqual(2, self.scenario.repetitions)
        self.assertEqual(4, self.scenario.trial_admission_slots)
        self.assertEqual(64, self.scenario.planned_trial_count)
        self.assertEqual(
            {f"N{index}" for index in range(1, 9)},
            set(self.scenario.nodes),
        )

    def test_trial_plan_is_complete_paired_and_deterministic(self) -> None:
        first = build_simulator_trials(self.scenario)
        second = build_simulator_trials(self.scenario)
        self.assertEqual(first, second)
        self.assertEqual(64, len(first))
        self.assertEqual(64, len({trial.trial_key for trial in first}))
        counts: dict[tuple[str, str], int] = {}
        for trial in first:
            key = (trial.workload_id, trial.design_id)
            counts[key] = counts.get(key, 0) + 1
        self.assertEqual({2}, set(counts.values()))

    def _mutated_scenario(self, mutate) -> Path:
        payload = json.loads(SCENARIO_PATH.read_text(encoding="utf-8"))
        mutate(payload)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "scenario.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_every_workload_must_freeze_quality_for_every_design(self) -> None:
        path = self._mutated_scenario(
            lambda payload: payload["workloads"][0][
                "task_success_by_design"
            ].pop("D7")
        )
        with self.assertRaisesRegex(
            SimulatorConfigError,
            "must declare a frozen success outcome for every design",
        ):
            load_simulator_scenario(path)

    def test_unresolved_resource_binding_is_refused(self) -> None:
        path = self._mutated_scenario(
            lambda payload: payload["designs"][0]["bindings"].__setitem__(
                "executor_cpu", "not-a-resource"
            )
        )
        with self.assertRaisesRegex(SimulatorConfigError, "unknown resource"):
            load_simulator_scenario(path)

    def test_operation_cycle_is_refused(self) -> None:
        def mutate(payload) -> None:
            payload["operation_templates"][0]["operations"][0][
                "depends_on"
            ] = ["infer"]

        path = self._mutated_scenario(mutate)
        with self.assertRaisesRegex(SimulatorConfigError, "contains a cycle"):
            load_simulator_scenario(path)

    def test_invalid_jitter_is_refused(self) -> None:
        path = self._mutated_scenario(
            lambda payload: payload["nodes"][0]["resources"][0].__setitem__(
                "jitter_fraction", 1.0
            )
        )
        with self.assertRaisesRegex(SimulatorConfigError, "less than 1"):
            load_simulator_scenario(path)

    def test_trial_admission_slots_must_fit_the_frozen_trial_plan(self) -> None:
        path = self._mutated_scenario(
            lambda payload: payload.__setitem__("trial_admission_slots", 65)
        )
        with self.assertRaisesRegex(
            SimulatorConfigError,
            "cannot exceed planned trial count",
        ):
            load_simulator_scenario(path)

    def test_missing_representation_is_refused_before_simulation(self) -> None:
        def mutate(payload) -> None:
            del payload["objects"][0]["representations"]["raw_video"]

        path = self._mutated_scenario(mutate)
        with self.assertRaisesRegex(
            SimulatorConfigError,
            "requires missing representation raw_video",
        ):
            load_simulator_scenario(path)


class DiscreteEventEngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scenario = load_simulator_scenario(SCENARIO_PATH)
        cls.result = run_discrete_event_simulation(cls.scenario)

    def test_simulation_completes_every_planned_trial(self) -> None:
        self.assertEqual("COMPLETE", self.result.summary["status"])
        self.assertEqual(64, len(self.result.canonical_records))
        self.assertEqual(64, self.result.summary["planned_trials"])
        self.assertGreater(len(self.result.events), 64)
        self.assertEqual(
            len(self.result.events),
            {event.event_index for event in self.result.events}.__len__(),
        )

    def test_records_are_explicitly_simulated_not_live_evidence(self) -> None:
        for row in self.result.canonical_records:
            self.assertTrue(row["simulated"])
            self.assertFalse(row["flowmesh_deployed"])
            self.assertFalse(row["llm_called"])
            self.assertFalse(row["credentials_recorded"])
            self.assertFalse(row["eligible_for_scientific_claims"])
            self.assertEqual("completed", row["outcome_type"])
            self.assertTrue(row["telemetry_complete"])
            self.assertTrue(row["artifact_delivery_complete"])
            self.assertTrue(row["quality_provenance"].endswith(
                "not-scientific-evidence"
            ))

    def test_resource_contention_creates_queue_time(self) -> None:
        queued = [
            event for event in self.result.events
            if event.executed and event.queue_time_ms > 0
        ]
        self.assertTrue(queued)
        self.assertTrue(any(event.resource_kind == "gpu" for event in queued))

    def test_global_fifo_admission_is_bounded_and_latency_is_decomposed(self) -> None:
        records = self.result.canonical_records
        self.assertTrue(any(row["trial_admission_queue_ms"] > 0 for row in records))
        for row in records:
            self.assertAlmostEqual(
                row["latency_ms"],
                row["trial_admission_queue_ms"]
                + row["active_execution_latency_ms"],
            )
            self.assertEqual("planned-trial-arrival", row["latency_origin"])
            self.assertEqual(4, row["trial_admission_slots"])

        admission_times = [
            row["arrival_time_ms"] + row["trial_admission_queue_ms"]
            for row in records
        ]
        self.assertEqual(sorted(admission_times), admission_times)

        boundaries: list[tuple[float, int]] = []
        for row in records:
            admitted = row["arrival_time_ms"] + row["trial_admission_queue_ms"]
            finished = admitted + row["active_execution_latency_ms"]
            boundaries.append((admitted, 1))
            boundaries.append((finished, -1))
        active = 0
        peak = 0
        for _, delta in sorted(boundaries, key=lambda item: (item[0], item[1])):
            active += delta
            peak = max(peak, active)
        self.assertEqual(4, peak)

    def test_cache_hit_and_miss_branches_are_both_exercised(self) -> None:
        lookups = [
            event for event in self.result.events
            if event.operation_kind == "cache_lookup"
        ]
        self.assertIn("hit", {event.cache_result for event in lookups})
        self.assertIn("miss", {event.cache_result for event in lookups})
        self.assertGreater(self.result.summary["skipped_branch_events"], 0)
        hit_trials = {
            event.trial_key for event in lookups if event.cache_result == "hit"
        }
        for trial_key in hit_trials:
            remote = [
                event for event in self.result.events
                if event.trial_key == trial_key
                and event.operation_id == "transfer-remote"
            ]
            self.assertEqual(1, len(remote))
            self.assertFalse(remote[0].executed)

    def test_network_byte_totals_are_conserved(self) -> None:
        by_trial: dict[str, int] = {}
        for event in self.result.events:
            if event.executed and event.resource_kind == "network":
                by_trial[event.trial_key] = (
                    by_trial.get(event.trial_key, 0) + event.physical_bytes
                )
        for row in self.result.canonical_records:
            self.assertEqual(
                by_trial.get(row["trial_key"], 0),
                row["network_bytes"],
            )

    def test_representative_design_relations_are_visible(self) -> None:
        summaries = {
            row["design_id"]: row
            for row in self.result.summary["design_summaries"]
        }
        self.assertGreater(
            summaries["D0"]["mean_active_execution_latency_ms"],
            summaries["D1"]["mean_active_execution_latency_ms"],
        )
        self.assertGreater(
            summaries["D4"]["mean_active_execution_latency_ms"],
            summaries["D5"]["mean_active_execution_latency_ms"],
        )
        self.assertGreater(
            summaries["D6"]["mean_active_execution_latency_ms"],
            summaries["D2"]["mean_active_execution_latency_ms"],
        )
        self.assertGreater(summaries["D3"]["cache_hits"], 0)
        self.assertGreater(summaries["D7"]["cache_hits"], 0)

    def test_a_second_engine_run_is_identical(self) -> None:
        second = run_discrete_event_simulation(self.scenario)
        self.assertEqual(self.result.plan, second.plan)
        self.assertEqual(self.result.events, second.events)
        self.assertEqual(self.result.canonical_records, second.canonical_records)
        self.assertEqual(self.result.summary, second.summary)


class SimulatorPublicationTest(unittest.TestCase):
    def test_atomic_outputs_are_byte_identical_and_verifiable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first = root / "run-a"
            second = root / "run-b"
            report = run_simulator_scenario(SCENARIO_PATH, output_dir=first)
            run_simulator_scenario(SCENARIO_PATH, output_dir=second)
            self.assertEqual("COMPLETE", report["status"])
            self.assertEqual(64, report["canonical_records"])
            first_files = sorted(path.name for path in first.iterdir())
            self.assertEqual(
                first_files,
                sorted(path.name for path in second.iterdir()),
            )
            for name in first_files:
                self.assertEqual(
                    (first / name).read_bytes(),
                    (second / name).read_bytes(),
                    name,
                )
            verified = verify_simulator_run(first)
            self.assertEqual("VERIFIED", verified["status"])
            self.assertEqual(5, verified["checked_files"])

    def test_existing_output_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "existing"
            output.mkdir()
            marker = output / "operator-file.txt"
            marker.write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                run_simulator_scenario(SCENARIO_PATH, output_dir=output)
            self.assertEqual("keep", marker.read_text(encoding="utf-8"))

    def test_cli_runs_without_flowmesh_or_external_services(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "cli-run"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                status = cli_main([
                    "simulate-flowmesh-infra",
                    "--scenario",
                    str(SCENARIO_PATH),
                    "--output-dir",
                    str(output),
                    "--compact",
                ])
            self.assertEqual(0, status)
            payload = json.loads(stdout.getvalue())
            self.assertEqual("COMPLETE", payload["status"])
            self.assertFalse(payload["flowmesh_deployed"])
            self.assertFalse(payload["external_services_called"])


if __name__ == "__main__":
    unittest.main()
