from __future__ import annotations

import csv
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from pathfinder.config import load_config
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
    validate_flowmesh_pilot_config,
)


ROOT = Path(__file__).resolve().parents[1]
PILOT_CONFIG_PATH = ROOT / "configs" / "phase_a_quote_pilot.json"
SYSTEM_CONFIG_PATH = (
    ROOT / "configs" / "phase_a_quote_pilot_system.json"
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
                "design_id": "unknown-design",
            }
        )
        with self.assertRaises(FlowMeshPilotConfigError):
            validate_flowmesh_pilot_config(broken, self.system)


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
