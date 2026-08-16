from __future__ import annotations

import csv
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from pathfinder.config import load_config
from pathfinder.data_agent_manifest import load_data_agent_manifest
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshAgentRun,
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
)
from pathfinder.integrations.flowmesh.gateway import TelemetryIncompleteError
from pathfinder.integrations.flowmesh.pilot import (
    _exclusive_pilot_lock,
    FlowMeshPilotConfigError,
    build_trial_plan,
    load_flowmesh_pilot_config,
    load_pilot_records,
    run_flowmesh_pilot,
    summarize_paired_contrasts,
    validate_flowmesh_pilot_config,
)


ROOT = Path(__file__).resolve().parents[1]
PILOT_CONFIG_PATH = ROOT / "configs" / "phase_a_quote_pilot.json"
SYSTEM_CONFIG_PATH = (
    ROOT / "configs" / "phase_a_quote_pilot_system.json"
)
COST_AWARE_PILOT_CONFIG_PATH = (
    ROOT / "configs" / "phase_a_cost_aware_quote_v2.json"
)
COST_AWARE_DRY_RUN_CONFIG_PATH = (
    ROOT / "configs" / "phase_a_cost_aware_quote_v2_dry_run.json"
)
COST_AWARE_SYSTEM_CONFIG_PATH = (
    ROOT / "configs" / "phase_a_cost_aware_quote_v2_system.json"
)
PHASE_B_PILOT_CONFIG_PATH = ROOT / "configs" / "phase_b_causal_gate.json"
PHASE_B_DRY_RUN_CONFIG_PATH = (
    ROOT / "configs" / "phase_b_causal_gate_dry_run.json"
)
PHASE_B_SYSTEM_CONFIG_PATH = (
    ROOT / "configs" / "phase_b_causal_gate_system.json"
)
PHASE_B_DATA_AGENT_MANIFEST_PATH = (
    ROOT / "configs" / "phase_b_data_agent_manifest.json"
)
PHASE_B_CONFIRMATORY_SMALL_CONFIG_PATH = (
    ROOT / "configs" / "phase_b_confirmatory_small.json"
)
PHASE_B_CONFIRMATORY_SMALL_MANIFEST_PATH = (
    ROOT
    / "configs"
    / "phase_b_confirmatory_small_data_agent_manifest.json"
)
PHASE_B_CONFIRMATORY_SMALL_SYSTEM_PATH = (
    ROOT / "configs" / "phase_b_confirmatory_small_system.json"
)


class FakePilotAdapter:
    def __init__(self) -> None:
        self.settings = FlowMeshSettings(worker_alias="pathfinder-test")
        self.requests: list[FlowMeshAgentRunRequest] = []

    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        self.requests.append(request)
        representation = (
            "multimodal_digest"
            if request.quote_profile_id == "digest_low"
            else "sampled_frames"
        )
        event = {
            "accepted": True,
            "representation_id": representation,
            "quoted_price": 2.0,
            "realized_cost": (
                1.6 if representation == "multimodal_digest" else 0.35
            ),
            "felt_latency_ms": (
                210.0 if representation == "multimodal_digest" else 75.0
            ),
            "data_agent_service_latency_ms": (
                210.0 if representation == "multimodal_digest" else 75.0
            ),
            "data_agent_fetch_latency_ms": 0.2,
            "data_agent_controlled_delay_ms": (
                209.8 if representation == "multimodal_digest" else 74.8
            ),
            "artifact_bytes_sent": (
                0 if representation == "multimodal_digest" else 121
            ),
            "artifact_transfer_latency_ms": (
                0.0 if representation == "multimodal_digest" else 0.3
            ),
        }
        suffix = len(self.requests)
        return FlowMeshAgentRun(
            session_id=request.session_id or f"session-{suffix}",
            workflow_id=f"wfl-{suffix}",
            task_id=f"tsk-{suffix}",
            status="DONE",
            final_answer="The red car enters at 00:17.",
            access_events=(event,),
            raw_result={},
        )


class InterruptingPilotAdapter(FakePilotAdapter):
    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        if len(self.requests) == 1:
            raise KeyboardInterrupt("simulated operator interruption")
        return super().run(request)


class FailingPilotAdapter(FakePilotAdapter):
    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        self.requests.append(request)
        if request.quote_profile_id == "digest_low":
            raise TelemetryIncompleteError("access-test", 1)
        raise RuntimeError("simulated FlowMesh failure")


class SecretEchoPilotAdapter(FakePilotAdapter):
    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        self.requests.append(request)
        raise RuntimeError(
            "Authorization: Bearer secret-test-value at "
            "https://data-agent.invalid/v1/artifacts/a?signature=private"
        )


class RecoveringPilotAdapter(FakePilotAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.recovered_session_ids: list[str] = []

    def recover(self, session_id: str) -> FlowMeshAgentRun:
        self.recovered_session_ids.append(session_id)
        return FlowMeshAgentRun(
            session_id=session_id,
            workflow_id=f"wfl-recovered-{len(self.recovered_session_ids)}",
            task_id=f"tsk-recovered-{len(self.recovered_session_ids)}",
            status="DONE",
            final_answer="The red car enters at 00:17.",
            access_events=(),
            raw_result={"recovered_from_gateway_state": True},
        )

    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        raise AssertionError("a recovered session must not be resubmitted")


class FlowMeshPilotConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.pilot = load_flowmesh_pilot_config(PILOT_CONFIG_PATH)
        cls.system = load_config(SYSTEM_CONFIG_PATH)

    def test_dedicated_configuration_excludes_unreadable_video(self) -> None:
        validate_flowmesh_pilot_config(self.pilot, self.system)
        self.assertEqual(
            {
                "sampled_frames",
                "embeddings",
                "multimodal_digest",
            },
            set(self.system.representations),
        )
        self.assertNotIn("compressed_video", self.system.representations)
        self.assertEqual(
            ("digest_low", "as_designed"),
            self.pilot.quote_profile_ids,
        )

    def test_pilot_question_does_not_force_a_representation(self) -> None:
        question = self.pilot.workloads[0].question.casefold()
        for representation in self.system.representations:
            self.assertNotIn(representation.casefold(), question)
        self.assertNotIn("must use", question)

    def test_trial_plan_is_deterministic_balanced_and_paired(self) -> None:
        self.assertEqual("D_structured_digest", self.pilot.design_id)
        first = build_trial_plan(self.pilot)
        second = build_trial_plan(self.pilot)
        self.assertEqual(first, second)
        self.assertEqual(20, len(first))
        counts = Counter(trial.quote_profile_id for trial in first)
        self.assertEqual(
            {"digest_low": 10, "as_designed": 10},
            dict(counts),
        )
        seeds: dict[int, set[int]] = {}
        for trial in first:
            seeds.setdefault(trial.repetition, set()).add(trial.seed)
        self.assertTrue(all(len(values) == 1 for values in seeds.values()))
        self.assertEqual(len(first), len({trial.session_id for trial in first}))

    def test_invalid_cross_reference_fails_before_submission(self) -> None:
        broken = self.pilot.__class__(
            **{
                **self.pilot.__dict__,
                "design_ids": ("unknown-design",),
            }
        )
        with self.assertRaises(FlowMeshPilotConfigError):
            validate_flowmesh_pilot_config(broken, self.system)


class CostAwareQuotePilotConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.pilot = load_flowmesh_pilot_config(
            COST_AWARE_PILOT_CONFIG_PATH
        )
        cls.dry_run = load_flowmesh_pilot_config(
            COST_AWARE_DRY_RUN_CONFIG_PATH
        )
        cls.system = load_config(COST_AWARE_SYSTEM_CONFIG_PATH)

    def test_v2_configs_validate_and_use_three_distinct_quote_levels(
        self,
    ) -> None:
        validate_flowmesh_pilot_config(self.pilot, self.system)
        validate_flowmesh_pilot_config(self.dry_run, self.system)

        task = self.system.task_classes["video_qa"]
        path = self.system.designs["D_structured_digest"].paths[
            "multimodal_digest"
        ]
        quotes = {
            profile_id: self.system.quote_profiles[profile_id].quote_for(
                task.id,
                "multimodal_digest",
                path.quotes[task.id],
            )
            for profile_id in self.pilot.quote_profile_ids
        }
        self.assertEqual(
            {
                "digest_low": 2.0,
                "digest_high": 6.0,
                "digest_unaffordable": 8.0,
            },
            quotes,
        )
        self.assertLessEqual(quotes["digest_high"], task.access_budget)
        self.assertGreater(
            quotes["digest_unaffordable"],
            task.access_budget,
        )

    def test_v2_changes_only_the_digest_quote(self) -> None:
        task = self.system.task_classes["video_qa"]
        design = self.system.designs["D_structured_digest"]
        for representation_id in ("sampled_frames", "embeddings"):
            default = design.paths[representation_id].quotes[task.id]
            observed = {
                self.system.quote_profiles[profile_id].quote_for(
                    task.id,
                    representation_id,
                    default,
                )
                for profile_id in self.pilot.quote_profile_ids
            }
            self.assertEqual({default}, observed)

    def test_v2_plans_are_balanced_paired_and_do_not_force_a_choice(
        self,
    ) -> None:
        dry_trials = build_trial_plan(self.dry_run)
        full_trials = build_trial_plan(self.pilot)
        self.assertEqual(9, len(dry_trials))
        self.assertEqual(45, len(full_trials))

        counts = Counter(trial.quote_profile_id for trial in full_trials)
        self.assertEqual(
            {
                "digest_low": 15,
                "digest_high": 15,
                "digest_unaffordable": 15,
            },
            dict(counts),
        )
        seeds: dict[tuple[str, int], set[int]] = {}
        for trial in full_trials:
            block = (trial.workload_id, trial.repetition)
            seeds.setdefault(block, set()).add(trial.seed)
        self.assertTrue(all(len(values) == 1 for values in seeds.values()))

        for workload in self.pilot.workloads:
            question = workload.question.casefold()
            self.assertNotIn("must use", question)
            for representation in self.system.representations:
                self.assertNotIn(representation.casefold(), question)


class PhaseBCausalGateConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.pilot = load_flowmesh_pilot_config(PHASE_B_PILOT_CONFIG_PATH)
        cls.dry_run = load_flowmesh_pilot_config(
            PHASE_B_DRY_RUN_CONFIG_PATH
        )
        cls.system = load_config(PHASE_B_SYSTEM_CONFIG_PATH)

    def test_phase_b_factorial_is_balanced_and_matched(self) -> None:
        validate_flowmesh_pilot_config(self.pilot, self.system)
        validate_flowmesh_pilot_config(self.dry_run, self.system)
        self.assertEqual(
            ("D_remote_digest", "D_local_digest"),
            self.pilot.design_ids,
        )
        with self.assertRaisesRegex(FlowMeshPilotConfigError, "multiple"):
            _ = self.pilot.design_id
        dry_trials = build_trial_plan(self.dry_run)
        full_trials = build_trial_plan(self.pilot)
        self.assertEqual(54, len(dry_trials))
        self.assertEqual(540, len(full_trials))

        cells = Counter(
            (
                trial.design_id,
                trial.quote_profile_id,
                trial.latency_multiplier,
            )
            for trial in full_trials
        )
        self.assertEqual(18, len(cells))
        self.assertEqual({30}, set(cells.values()))
        blocks: dict[tuple[str, int], set[tuple[str, str, float]]] = {}
        for trial in full_trials:
            blocks.setdefault(
                (trial.workload_id, trial.repetition), set()
            ).add(
                (
                    trial.design_id,
                    trial.quote_profile_id,
                    trial.latency_multiplier,
                )
            )
        self.assertEqual({18}, {len(cells) for cells in blocks.values()})

    def test_matched_quote_and_physical_cells_are_declared(self) -> None:
        task = self.system.task_classes["video_qa"]
        remote = self.system.designs["D_remote_digest"]
        local = self.system.designs["D_local_digest"]
        low = self.system.quote_profiles["digest_low"]
        remote_quote = low.quote_for(
            task.id,
            "multimodal_digest",
            remote.paths["multimodal_digest"].quotes[task.id],
        )
        local_quote = low.quote_for(
            task.id,
            "multimodal_digest",
            local.paths["multimodal_digest"].quotes[task.id],
        )
        self.assertEqual(remote_quote, local_quote)
        self.assertNotEqual(
            remote.paths["multimodal_digest"].location,
            local.paths["multimodal_digest"].location,
        )
        self.assertNotEqual(
            remote.paths["multimodal_digest"].realized_cost,
            local.paths["multimodal_digest"].realized_cost,
        )

    def test_data_agent_bindings_match_every_phase_b_design_path(self) -> None:
        manifest = load_data_agent_manifest(
            PHASE_B_DATA_AGENT_MANIFEST_PATH
        )
        for design_id in self.pilot.design_ids:
            design = self.system.designs[design_id]
            for representation_id, path in design.paths.items():
                binding = manifest.representations[
                    representation_id
                ].plan_bindings[design_id]
                self.assertEqual(path.location, binding.location)
                self.assertEqual(path.latency_ms, binding.minimum_latency_ms)
                self.assertEqual(path.realized_cost, binding.realized_cost)

    def test_paired_contrasts_cover_each_single_factor(self) -> None:
        trials = build_trial_plan(self.dry_run)
        rows = summarize_paired_contrasts([], trials)
        self.assertEqual(
            {"physical", "quote", "latency"},
            {row["contrast_type"] for row in rows},
        )
        self.assertTrue(all(row["planned_pairs"] > 0 for row in rows))
        self.assertTrue(all(row["complete_pairs"] == 0 for row in rows))

    def test_phase_b_dry_run_executes_all_multi_design_cells(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            adapter = FakePilotAdapter()
            manifest = run_flowmesh_pilot(
                pilot=self.dry_run,
                system=self.system,
                adapter=adapter,
                output_dir=Path(directory) / "phase-b",
            )
            output = Path(directory) / "phase-b"
            self.assertEqual(54, manifest["planned_trials"])
            self.assertEqual(54, len(adapter.requests))
            self.assertEqual(
                set(self.pilot.design_ids),
                {request.design_id for request in adapter.requests},
            )
            with (output / "summary.csv").open(
                "r", encoding="utf-8", newline=""
            ) as handle:
                self.assertEqual(18, len(list(csv.DictReader(handle))))
            with (output / "summary_by_workload.csv").open(
                "r", encoding="utf-8", newline=""
            ) as handle:
                self.assertEqual(54, len(list(csv.DictReader(handle))))
            with (output / "paired_contrasts.csv").open(
                "r", encoding="utf-8", newline=""
            ) as handle:
                contrasts = list(csv.DictReader(handle))
            self.assertEqual(
                {"physical", "quote", "latency"},
                {row["contrast_type"] for row in contrasts},
            )
            self.assertTrue(
                all(int(row["complete_pairs"]) > 0 for row in contrasts)
            )


class PhaseBConfirmatorySmallConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.pilot = load_flowmesh_pilot_config(
            PHASE_B_CONFIRMATORY_SMALL_CONFIG_PATH
        )
        cls.system = load_config(PHASE_B_CONFIRMATORY_SMALL_SYSTEM_PATH)
        cls.manifest = load_data_agent_manifest(
            PHASE_B_CONFIRMATORY_SMALL_MANIFEST_PATH
        )

    def test_plan_has_48_balanced_matched_sessions(self) -> None:
        validate_flowmesh_pilot_config(self.pilot, self.system)
        trials = build_trial_plan(self.pilot)
        self.assertEqual(48, len(trials))
        self.assertEqual((1.0,), self.pilot.latency_multipliers)
        self.assertEqual(8, len(self.pilot.workloads))
        self.assertEqual(
            8,
            len({workload.object_id for workload in self.pilot.workloads}),
        )

        cells = Counter(
            (trial.design_id, trial.quote_profile_id)
            for trial in trials
        )
        self.assertEqual(6, len(cells))
        self.assertEqual({8}, set(cells.values()))

        per_object: dict[str, set[tuple[str, str]]] = {}
        for trial in trials:
            per_object.setdefault(trial.object_id, set()).add(
                (trial.design_id, trial.quote_profile_id)
            )
        self.assertEqual({6}, {len(cells) for cells in per_object.values()})

    def test_workload_strata_and_questions_are_frozen_without_path_hints(
        self,
    ) -> None:
        strata = Counter(
            workload.id.split("-", 1)[0]
            for workload in self.pilot.workloads
        )
        self.assertEqual(
            {"temporal": 3, "causal": 3, "descriptive": 2},
            dict(strata),
        )
        for workload in self.pilot.workloads:
            question = workload.question.casefold()
            self.assertNotIn("must use", question)
            for representation in self.system.representations:
                self.assertNotIn(representation.casefold(), question)

    def test_object_catalog_covers_every_workload_and_representation(
        self,
    ) -> None:
        catalog = self.manifest.object_catalog
        self.assertIsNotNone(catalog)
        assert catalog is not None
        self.assertEqual(
            {workload.object_id for workload in self.pilot.workloads},
            set(catalog.objects),
        )
        expected_representations = set(self.system.representations)
        for representations in catalog.objects.values():
            self.assertEqual(expected_representations, set(representations))
            for specification in representations.values():
                self.assertIn(
                    "phase_b_confirmatory_small",
                    str(specification.path),
                )

    def test_manifest_bindings_match_every_design_path(self) -> None:
        for design_id in self.pilot.design_ids:
            design = self.system.designs[design_id]
            for representation_id, path in design.paths.items():
                binding = self.manifest.representations[
                    representation_id
                ].plan_bindings[design_id]
                self.assertEqual(path.location, binding.location)
                self.assertEqual(path.latency_ms, binding.minimum_latency_ms)
                self.assertEqual(path.realized_cost, binding.realized_cost)


class FlowMeshPilotBatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pilot = load_flowmesh_pilot_config(PILOT_CONFIG_PATH)
        self.system = load_config(SYSTEM_CONFIG_PATH)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temporary_directory.name) / "pilot"

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_batch_writes_records_summary_manifest_and_frozen_plan(self) -> None:
        adapter = FakePilotAdapter()
        progress: list[dict[str, object]] = []
        manifest = run_flowmesh_pilot(
            pilot=self.pilot,
            system=self.system,
            adapter=adapter,
            output_dir=self.output_dir,
            repetitions=2,
            progress_callback=progress.append,
        )

        self.assertEqual("COMPLETE", manifest["status"])
        self.assertEqual(4, manifest["planned_trials"])
        self.assertEqual(4, manifest["recorded_trials"])
        self.assertEqual(4, len(adapter.requests))
        self.assertEqual(4, len(progress))
        self.assertEqual(4, progress[-1]["recorded_trials"])
        records = load_pilot_records(self.output_dir / "runs.jsonl")
        self.assertEqual(4, len(records))
        self.assertTrue(all(record["telemetry_complete"] for record in records))
        self.assertTrue(all(record["task_success"] for record in records))
        self.assertTrue(all("raw_result" not in record for record in records))

        plan = json.loads(
            (self.output_dir / "trial_plan.json").read_text(encoding="utf-8")
        )
        self.assertEqual(4, plan["trial_count"])
        with (self.output_dir / "summary.csv").open(
            "r", encoding="utf-8", newline=""
        ) as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(2, len(rows))
        rates = {
            row["quote_profile_id"]: row["multimodal_digest_access_rate"]
            for row in rows
        }
        self.assertEqual("1.0", rates["digest_low"])
        self.assertEqual("0.0", rates["as_designed"])
        self.assertTrue((self.output_dir / "summary_by_workload.csv").exists())
        self.assertTrue((self.output_dir / "paired_contrasts.csv").exists())
        self.assertIn("workload_summary_path", manifest)
        self.assertIn("paired_contrasts_path", manifest)
        self.assertFalse(manifest["secrets_recorded"])

    def test_interrupted_batch_resumes_only_missing_trials(self) -> None:
        interrupted = InterruptingPilotAdapter()
        with self.assertRaises(KeyboardInterrupt):
            run_flowmesh_pilot(
                pilot=self.pilot,
                system=self.system,
                adapter=interrupted,
                output_dir=self.output_dir,
                repetitions=2,
            )
        self.assertEqual(1, len(load_pilot_records(self.output_dir / "runs.jsonl")))

        resumed = FakePilotAdapter()
        manifest = run_flowmesh_pilot(
            pilot=self.pilot,
            system=self.system,
            adapter=resumed,
            output_dir=self.output_dir,
            repetitions=2,
        )
        self.assertEqual(3, len(resumed.requests))
        self.assertEqual(4, manifest["recorded_trials"])

        third = FakePilotAdapter()
        run_flowmesh_pilot(
            pilot=self.pilot,
            system=self.system,
            adapter=third,
            output_dir=self.output_dir,
            repetitions=2,
        )
        self.assertEqual([], third.requests)

    def test_failures_are_recorded_once_and_classified(self) -> None:
        adapter = FailingPilotAdapter()
        manifest = run_flowmesh_pilot(
            pilot=self.pilot,
            system=self.system,
            adapter=adapter,
            output_dir=self.output_dir,
            repetitions=1,
        )
        self.assertEqual(
            {"infrastructure_failure": 1, "telemetry_failure": 1},
            manifest["outcome_counts"],
        )
        records = load_pilot_records(self.output_dir / "runs.jsonl")
        by_outcome = {record["outcome_type"]: record for record in records}
        self.assertFalse(by_outcome["telemetry_failure"]["telemetry_complete"])
        self.assertIsNone(
            by_outcome["infrastructure_failure"]["telemetry_complete"]
        )

        retry = FakePilotAdapter()
        run_flowmesh_pilot(
            pilot=self.pilot,
            system=self.system,
            adapter=retry,
            output_dir=self.output_dir,
            repetitions=1,
        )
        self.assertEqual([], retry.requests)

    def test_changed_plan_cannot_be_mixed_into_existing_output(self) -> None:
        run_flowmesh_pilot(
            pilot=self.pilot,
            system=self.system,
            adapter=FakePilotAdapter(),
            output_dir=self.output_dir,
            repetitions=1,
        )
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            run_flowmesh_pilot(
                pilot=self.pilot,
                system=self.system,
                adapter=FakePilotAdapter(),
                output_dir=self.output_dir,
                repetitions=2,
            )

    def test_unpinned_batches_are_rejected(self) -> None:
        adapter = FakePilotAdapter()
        adapter.settings = FlowMeshSettings()
        with self.assertRaisesRegex(FlowMeshPilotConfigError, "worker"):
            run_flowmesh_pilot(
                pilot=self.pilot,
                system=self.system,
                adapter=adapter,
                output_dir=self.output_dir,
                repetitions=1,
            )
        self.assertEqual([], adapter.requests)

    def test_same_output_directory_cannot_run_concurrently(self) -> None:
        lock_path = self.output_dir / ".pilot.lock"
        with _exclusive_pilot_lock(lock_path):
            with self.assertRaisesRegex(RuntimeError, "already holds"):
                run_flowmesh_pilot(
                    pilot=self.pilot,
                    system=self.system,
                    adapter=FakePilotAdapter(),
                    output_dir=self.output_dir,
                    repetitions=1,
                )

    def test_truncated_final_jsonl_append_is_discarded(self) -> None:
        records_path = self.output_dir / "runs.jsonl"
        records_path.parent.mkdir(parents=True)
        records_path.write_text(
            json.dumps({"trial_key": "complete"})
            + "\n"
            + '{"trial_key":"partial',
            encoding="utf-8",
        )
        records = load_pilot_records(records_path)
        self.assertEqual([{"trial_key": "complete"}], records)
        repaired = records_path.read_text(encoding="utf-8")
        self.assertEqual(json.dumps({"trial_key": "complete"}) + "\n", repaired)

    def test_failure_records_redact_credentials_and_signed_urls(self) -> None:
        with patch.dict(
            "os.environ",
            {"FLOWMESH_API_KEY": "secret-test-value"},
        ):
            run_flowmesh_pilot(
                pilot=self.pilot,
                system=self.system,
                adapter=SecretEchoPilotAdapter(),
                output_dir=self.output_dir,
                repetitions=1,
            )
        raw = (self.output_dir / "runs.jsonl").read_text(encoding="utf-8")
        self.assertNotIn("secret-test-value", raw)
        self.assertNotIn("signature=private", raw)
        self.assertIn("<redacted>", raw)

    def test_missing_jsonl_record_recovers_gateway_state_before_submit(
        self,
    ) -> None:
        adapter = RecoveringPilotAdapter()
        run_flowmesh_pilot(
            pilot=self.pilot,
            system=self.system,
            adapter=adapter,
            output_dir=self.output_dir,
            repetitions=1,
        )
        records = load_pilot_records(self.output_dir / "runs.jsonl")
        self.assertEqual(2, len(adapter.recovered_session_ids))
        self.assertTrue(
            all(record["recovered_from_gateway_state"] for record in records)
        )


if __name__ == "__main__":
    unittest.main()
