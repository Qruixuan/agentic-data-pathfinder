from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.cli import main as cli_main
from pathfinder.simulator import (
    EVIDENCE_MANIFEST_SCHEMA_VERSION,
    EVIDENCE_SPEC_SCHEMA_VERSION,
    MODEL_TIMING_SCHEMA_VERSION,
    SimulatorEvidenceError,
    SimulatorFitError,
    build_simulator_evidence_bundle,
    fit_simulator_scenario,
    import_flowmesh_trace,
    load_simulator_scenario,
    verify_simulator_evidence_bundle,
    verify_simulator_fit,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"


def _flowmesh_record() -> dict:
    return {
        "schema_version": "pathfinder.distributed-pilot-record/v1alpha1",
        "experiment_id": "pilot-1",
        "trial_key": "pilot-1|workload-1|D0|r0",
        "trial_id": "trial-1",
        "session_id": "session-1",
        "workflow_id": "workflow-1",
        "task_id": "task-1",
        "workload_id": "workload-1",
        "object_id": "video-1",
        "design_id": "D0",
        "task_class_id": "temporal",
        "repetition": 0,
        "duration_seconds": 0.02,
        "outcome_type": "completed",
        "telemetry_complete": True,
        "artifact_delivery_complete": True,
        "task_success": True,
        "access_event_count": 1,
        "accepted_access_count": 1,
        "access_events": [{
            "accepted": True,
            "representation_id": "sampled_frame_bundle",
            "endpoint_id": "origin_remote",
            "source_node_id": "N4",
            "destination_execution_node_id": "N7",
            "source_location": "origin-remote",
            "bytes_read": 512,
            "artifact_handle_sha256": "a" * 64,
            "artifact_bytes_sent": 4096,
            "artifact_download_request_count": 1,
            "artifact_full_download_count": 1,
            "felt_latency_ms": 18.0,
            "realized_cost": 0.9,
        }],
    }


class SimulatorEvidenceFitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.fio = self.root / "fio.json"
        self.fio.write_text(json.dumps({
            "jobs": [
                {"read": {"bw_bytes": value, "clat_ns": {"mean": latency}}}
                for value, latency in (
                    (100_000_000, 1_000_000),
                    (110_000_000, 2_000_000),
                    (120_000_000, 3_000_000),
                )
            ]
        }), encoding="utf-8")
        self.iperf = self.root / "iperf.json"
        self.iperf.write_text(json.dumps({
            "intervals": [
                {"sum": {"bits_per_second": value}}
                for value in (800_000_000, 880_000_000, 960_000_000)
            ]
        }), encoding="utf-8")
        self.model = self.root / "model.jsonl"
        self.model.write_text("".join(
            json.dumps({
                "schema_version": MODEL_TIMING_SCHEMA_VERSION,
                "observation_id": f"timing-{index}",
                "node_id": "N6",
                "resource_id": "N6.gpu",
                "template_id": "remote-digest",
                "op_id": "infer",
                "service_time_ms": value,
                "outcome_type": "completed",
                "telemetry_complete": True,
                "real_measurement": True,
                "credentials_recorded": False,
            }, sort_keys=True) + "\n"
            for index, value in enumerate((40, 50, 60))
        ), encoding="utf-8")
        records = self.root / "canonical_records.jsonl"
        records.write_text(
            json.dumps(_flowmesh_record()) + "\n",
            encoding="utf-8",
        )
        self.trace = self.root / "trace"
        import_flowmesh_trace(records, output_dir=self.trace)
        self.spec = self.root / "evidence-spec.json"
        self.spec_payload = {
            "schema_version": EVIDENCE_SPEC_SCHEMA_VERSION,
            "evidence_id": "measured-fixture-v1",
            "credentials_recorded": False,
            "sources": [
                {
                    "source_id": "fio-n3",
                    "kind": "fio",
                    "path": "fio.json",
                    "node_id": "N3",
                    "resource_id": "N3.hdd",
                    "operation": "read",
                },
                {
                    "source_id": "iperf-n3-n7",
                    "kind": "iperf3",
                    "path": "iperf.json",
                    "link_id": "N3-N7-cold",
                    "source_node_id": "N3",
                    "destination_node_id": "N7",
                    "round_trip_time_ms_samples": [1, 2, 3],
                },
                {
                    "source_id": "model-remote-digest",
                    "kind": "model-timing-jsonl",
                    "path": "model.jsonl",
                },
                {
                    "source_id": "flowmesh-route",
                    "kind": "flowmesh-trace-import",
                    "path": "trace",
                },
            ],
        }
        self._write_spec()

    def _write_spec(self) -> None:
        self.spec.write_text(
            json.dumps(self.spec_payload),
            encoding="utf-8",
        )

    def test_builds_unified_bundle_without_decomposing_flowmesh_latency(self) -> None:
        first = self.root / "evidence-a"
        second = self.root / "evidence-b"
        report = build_simulator_evidence_bundle(self.spec, output_dir=first)
        build_simulator_evidence_bundle(self.spec, output_dir=second)
        self.assertEqual(4, report["source_count"])
        self.assertEqual(4, report["observation_count"])
        self.assertEqual(3, report["direct_calibration_observation_count"])
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        observations = [
            json.loads(line)
            for line in (first / "evidence_observations.jsonl").read_text().splitlines()
        ]
        route = next(
            row for row in observations
            if row["measurement_kind"] == "flowmesh-route"
        )
        self.assertEqual("validation-only", route["calibration_role"])
        self.assertIn("end_to_end_latency_ms", route["metrics"])
        verified = verify_simulator_evidence_bundle(first)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(
            EVIDENCE_MANIFEST_SCHEMA_VERSION,
            verified["schema_version"],
        )

    def test_fit_updates_only_named_direct_parameters(self) -> None:
        evidence = self.root / "evidence"
        build_simulator_evidence_bundle(self.spec, output_dir=evidence)
        output = self.root / "fit"
        report = fit_simulator_scenario(
            SCENARIO,
            evidence,
            output_scenario_id="fitted-test-v1",
            output_dir=output,
        )
        self.assertEqual(3, report["fitted_target_count"])
        self.assertEqual(1, report["validation_only_observation_count"])
        scenario = json.loads(
            (output / "calibrated_scenario.json").read_text(encoding="utf-8")
        )
        resources = {
            resource["resource_id"]: resource
            for node in scenario["nodes"]
            for resource in node.get("resources", [])
        }
        self.assertEqual(2.0, resources["N3.hdd"]["base_latency_ms"])
        self.assertEqual(
            110_000_000,
            resources["N3.hdd"]["throughput_bytes_per_second"],
        )
        links = {link["link_id"]: link for link in scenario["links"]}
        self.assertEqual(
            110_000_000,
            links["N3-N7-cold"]["bandwidth_bytes_per_second"],
        )
        self.assertEqual(2.0, links["N3-N7-cold"]["round_trip_time_ms"])
        template = next(
            item for item in scenario["operation_templates"]
            if item["template_id"] == "remote-digest"
        )
        operation = next(
            item for item in template["operations"] if item["op_id"] == "infer"
        )
        self.assertEqual(50.0, operation["service_ms"])
        self.assertEqual(
            "provisional-local-simulator-units-v1",
            scenario["rate_card"]["rate_card_id"],
        )
        load_simulator_scenario(output / "calibrated_scenario.json")
        self.assertEqual("VERIFIED", verify_simulator_fit(output)["status"])

    def test_refuses_sensitive_input_and_path_escape(self) -> None:
        for mutation, message in (
            ("secret", "credential-like"),
            ("escape", "contained relative"),
        ):
            with self.subTest(mutation=mutation):
                payload = json.loads(json.dumps(self.spec_payload))
                if mutation == "secret":
                    payload["api_key"] = "do-not-copy"
                else:
                    payload["sources"][0]["path"] = "../fio.json"
                self.spec_payload = payload
                self._write_spec()
                with self.assertRaisesRegex(SimulatorEvidenceError, message):
                    build_simulator_evidence_bundle(
                        self.spec,
                        output_dir=self.root / f"bad-{mutation}",
                    )
                self.spec_payload.pop("api_key", None)
                self.spec_payload["sources"][0]["path"] = "fio.json"
                self._write_spec()

    def test_refuses_wrong_units_duplicate_targets_and_existing_outputs(self) -> None:
        evidence = self.root / "evidence"
        build_simulator_evidence_bundle(self.spec, output_dir=evidence)
        observations = [
            json.loads(line)
            for line in (evidence / "evidence_observations.jsonl").read_text().splitlines()
        ]
        storage = next(
            row for row in observations if row["measurement_kind"] == "storage"
        )
        storage["metrics"]["base_latency_ms"]["unit"] = "seconds"
        (evidence / "evidence_observations.jsonl").write_text(
            "\n".join(json.dumps(row) for row in observations) + "\n",
            encoding="utf-8",
        )
        # Updating an evidence file invalidates its checksum before fitting.
        with self.assertRaisesRegex(SimulatorEvidenceError, "checksum mismatch"):
            fit_simulator_scenario(
                SCENARIO,
                evidence,
                output_scenario_id="bad-units",
                output_dir=self.root / "bad-fit",
            )
        existing = self.root / "existing"
        existing.mkdir()
        with self.assertRaisesRegex(SimulatorEvidenceError, "already exists"):
            build_simulator_evidence_bundle(self.spec, output_dir=existing)

    def test_fit_is_deterministic_and_refuses_same_scenario_id(self) -> None:
        evidence = self.root / "evidence"
        build_simulator_evidence_bundle(self.spec, output_dir=evidence)
        first = self.root / "fit-a"
        second = self.root / "fit-b"
        fit_simulator_scenario(
            SCENARIO,
            evidence,
            output_scenario_id="fitted-test-v1",
            output_dir=first,
        )
        fit_simulator_scenario(
            SCENARIO,
            evidence,
            output_scenario_id="fitted-test-v1",
            output_dir=second,
        )
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        with self.assertRaisesRegex(SimulatorFitError, "must change"):
            fit_simulator_scenario(
                SCENARIO,
                evidence,
                output_scenario_id="flowmesh-infra-4x8-local-smoke-v1",
                output_dir=self.root / "same-id",
            )

    def test_cli_builds_evidence_and_fit(self) -> None:
        evidence = self.root / "cli-evidence"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "build-flowmesh-infra-evidence",
                "--spec",
                str(self.spec),
                "--output-dir",
                str(evidence),
                "--compact",
            ])
        self.assertEqual(0, status)
        self.assertEqual("COMPLETE", json.loads(stdout.getvalue())["status"])
        fitted = self.root / "cli-fit"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "fit-flowmesh-infra-scenario",
                "--scenario",
                str(SCENARIO),
                "--evidence-dir",
                str(evidence),
                "--output-scenario-id",
                "cli-fit-v1",
                "--output-dir",
                str(fitted),
                "--compact",
            ])
        self.assertEqual(0, status)
        self.assertEqual("COMPLETE", json.loads(stdout.getvalue())["status"])


if __name__ == "__main__":
    unittest.main()
