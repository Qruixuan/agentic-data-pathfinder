from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.cli import main as cli_main
from pathfinder.simulator import (
    FlowMeshTraceImportError,
    import_flowmesh_trace,
    verify_flowmesh_trace_import,
)


def _artifact_event() -> dict[str, object]:
    return {
        "accepted": True,
        "event_id": 7,
        "representation_id": "sampled_frame_bundle",
        "endpoint_id": "origin_remote",
        "source_node_id": "origin-0",
        "destination_execution_node_id": "executor-0",
        "source_location": "origin-remote",
        "bytes_read": 512,
        "artifact_handle_sha256": "a" * 64,
        "artifact_bytes_sent": 4096,
        "artifact_download_request_count": 1,
        "artifact_full_download_count": 1,
        "felt_latency_ms": 18.0,
        "data_agent_service_latency_ms": 10.0,
        "data_agent_fetch_latency_ms": 2.0,
        "data_agent_controlled_delay_ms": 4.0,
        "artifact_transfer_latency_ms": 3.0,
        "realized_cost": 0.9,
    }


def _completed_record() -> dict[str, object]:
    return {
        "schema_version": "pathfinder.distributed-pilot-record/v1alpha1",
        "experiment_id": "pilot-real-1",
        "trial_key": "pilot-real-1|w1|D0|r0",
        "trial_id": "trial-secret-shaped-id",
        "session_id": "session-1",
        "workflow_id": "workflow-1",
        "task_id": "task-1",
        "workload_id": "w1",
        "object_id": "video-1",
        "design_id": "D0",
        "task_class_id": "temporal",
        "repetition": 0,
        "started_at": "2026-09-01T00:00:00+00:00",
        "finished_at": "2026-09-01T00:00:00.025000+00:00",
        "outcome_type": "completed",
        "telemetry_complete": True,
        "artifact_delivery_complete": True,
        "task_success": True,
        "question": "must not be copied",
        "final_answer": "must not be copied either",
        "access_event_count": 1,
        "accepted_access_count": 1,
        "access_events": [_artifact_event()],
    }


def _failure_record() -> dict[str, object]:
    return {
        "schema_version": "pathfinder.flowmesh-pilot-record/v1alpha1",
        "experiment_id": "pilot-real-1",
        "trial_key": "pilot-real-1|w2|D1|r0",
        "trial_id": "trial-2",
        "session_id": "session-2",
        "workflow_id": None,
        "task_id": None,
        "workload_id": "w2",
        "object_id": "video-2",
        "design_id": "D1",
        "task_class_id": "causal",
        "repetition": 0,
        "duration_seconds": 1.5,
        "outcome_type": "infrastructure_failure",
        "telemetry_complete": None,
        "artifact_delivery_complete": None,
        "task_success": None,
        "access_event_count": 0,
        "accepted_access_count": 0,
        "access_events": [],
    }


class FlowMeshTraceImportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def _write(self, *records: dict[str, object]) -> Path:
        path = self.root / "canonical_records.jsonl"
        path.write_text(
            "".join(
                json.dumps(record, sort_keys=True) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )
        return path

    def test_completed_and_failed_records_are_kept_separate(self) -> None:
        source = self._write(_completed_record(), _failure_record())
        output = self.root / "import"
        report = import_flowmesh_trace(source, output_dir=output)

        self.assertEqual("COMPLETE", report["status"])
        self.assertEqual(2, report["record_count"])
        self.assertEqual(1, report["completed_record_count"])
        self.assertEqual(1, report["failed_record_count"])
        self.assertEqual(1, report["calibration_access_observation_count"])

        trials = [
            json.loads(line)
            for line in (output / "trial_observations.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertTrue(trials[0]["included_in_calibration"])
        self.assertFalse(trials[1]["included_in_calibration"])
        self.assertIn("infrastructure_failure", trials[1]["calibration_exclusion_reason"])

        accesses = [
            json.loads(line)
            for line in (output / "access_observations.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertEqual(4096, accesses[0]["payload_bytes"])
        self.assertTrue(accesses[0]["route_identity_complete"])
        self.assertTrue(
            accesses[0]["runtime_cost_is_not_a_physical_rate_calibration"]
        )

    def test_private_text_and_raw_identifiers_are_not_copied(self) -> None:
        output = self.root / "import"
        import_flowmesh_trace(self._write(_completed_record()), output_dir=output)
        combined = b"".join(path.read_bytes() for path in output.iterdir())
        self.assertNotIn(b"must not be copied", combined)
        self.assertNotIn(b"trial-secret-shaped-id", combined)
        self.assertNotIn(b"session-1", combined)
        self.assertNotIn(b"workflow-1", combined)
        self.assertNotIn(b"task-1", combined)
        self.assertNotIn(b"pilot-real-1|w1|D0|r0", combined)
        self.assertNotIn(("a" * 64).encode("ascii"), combined)
        self.assertIn(b"trial_id_sha256", combined)

    def test_completed_records_require_literal_completeness(self) -> None:
        for field, value in (
            ("telemetry_complete", "true"),
            ("artifact_delivery_complete", None),
            ("task_success", 1),
        ):
            with self.subTest(field=field, value=value):
                record = _completed_record()
                record[field] = value
                with self.assertRaisesRegex(FlowMeshTraceImportError, field):
                    import_flowmesh_trace(
                        self._write(record),
                        output_dir=self.root / f"bad-{field}",
                    )

    def test_incomplete_artifact_delivery_is_refused(self) -> None:
        record = _completed_record()
        record["access_events"][0]["artifact_full_download_count"] = 0
        with self.assertRaisesRegex(
            FlowMeshTraceImportError,
            "without a completed full download",
        ):
            import_flowmesh_trace(
                self._write(record), output_dir=self.root / "bad-artifact"
            )

    def test_artifact_delivery_failure_is_retained_but_not_calibrated(self) -> None:
        record = _completed_record()
        record["outcome_type"] = "artifact_delivery_failure"
        record["artifact_delivery_complete"] = False
        record["task_success"] = None
        record["access_events"][0]["artifact_full_download_count"] = 0
        output = self.root / "delivery-failure"
        report = import_flowmesh_trace(self._write(record), output_dir=output)
        self.assertEqual(1, report["failed_record_count"])
        self.assertEqual(0, report["calibration_access_observation_count"])
        trial = json.loads(
            (output / "trial_observations.jsonl").read_text(encoding="utf-8")
        )
        self.assertEqual("artifact_delivery_failure", trial["outcome_type"])
        self.assertFalse(trial["included_in_calibration"])

    def test_raw_artifact_capabilities_are_refused(self) -> None:
        record = _completed_record()
        record["access_events"][0]["artifact_handle"] = "signed-secret"
        with self.assertRaisesRegex(FlowMeshTraceImportError, "raw artifact handle"):
            import_flowmesh_trace(
                self._write(record), output_dir=self.root / "raw-handle"
            )

    def test_publication_is_deterministic_and_verifiable(self) -> None:
        source = self._write(_completed_record(), _failure_record())
        first = self.root / "first"
        second = self.root / "second"
        import_flowmesh_trace(source, output_dir=first)
        import_flowmesh_trace(source, output_dir=second)
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        verified = verify_flowmesh_trace_import(first)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(4, verified["checked_files"])

    def test_existing_output_and_tampering_are_refused(self) -> None:
        source = self._write(_completed_record())
        output = self.root / "import"
        import_flowmesh_trace(source, output_dir=output)
        with self.assertRaisesRegex(FlowMeshTraceImportError, "already exists"):
            import_flowmesh_trace(source, output_dir=output)
        summary = output / "calibration_summary.json"
        summary.write_bytes(summary.read_bytes() + b" ")
        with self.assertRaisesRegex(FlowMeshTraceImportError, "checksum mismatch"):
            verify_flowmesh_trace_import(output)

    def test_cli_imports_without_external_services(self) -> None:
        source = self._write(_completed_record())
        output = self.root / "cli"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "import-flowmesh-infra-trace",
                "--records",
                str(source),
                "--output-dir",
                str(output),
                "--compact",
            ])
        self.assertEqual(0, status)
        report = json.loads(stdout.getvalue())
        self.assertEqual("COMPLETE", report["status"])
        self.assertFalse(report["external_services_called"])


if __name__ == "__main__":
    unittest.main()
