"""Read-only fast/slow full-chain application-shaping audit tests."""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from pathfinder.integrations.flowmesh.container_dag import FlowMeshContainerDagError
from pathfinder.integrations.flowmesh.container_full_chain import (
    FLOWMESH_CONTAINER_FULL_CHAIN_RUN_LEGACY_SCHEMA_VERSION,
    _aggregate_telemetry,
    plan_flowmesh_container_full_physical_chain,
    run_flowmesh_container_full_physical_chain,
)
from pathfinder.integrations.flowmesh.container_dag import (
    TELEMETRY_DISCLAIMERS_V1,
    TELEMETRY_FIELD_PROVENANCE_V1,
    TELEMETRY_PROVENANCE_LEGACY_VERSION,
)
from pathfinder.integrations.flowmesh.container_full_chain_calibration import (
    FullChainCalibrationAuditError,
    audit_flowmesh_container_full_chain_calibration,
    verify_flowmesh_container_full_chain_calibration,
)
from pathfinder.integrations.flowmesh.contracts import FlowMeshSettings
from pathfinder.simulator.container_contract import CONTAINER_OPERATION_SCHEMA_VERSION
from pathfinder.cli import main as cli_main
from tests.test_flowmesh_container_dag import (
    FakeFlowMeshClient,
    _runtime_epoch_probe,
)


_URLS = {
    "N3": "http://127.0.0.1:19083",
    "N6": "http://127.0.0.1:19086",
    "N8": "http://127.0.0.1:19088",
}


def _operation(
    key: str,
    kind: str,
    node: str,
    dependencies: list[str],
    *,
    trial_key: str,
    logical_bytes: int,
    link_adapter: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": CONTAINER_OPERATION_SCHEMA_VERSION,
        "backend_id": "container-calibration-test",
        "portable_plan_sha256": "a" * 64,
        "operation_key": key,
        "trial_key": trial_key,
        "operation_id": key.rsplit("|", 1)[-1],
        "operation_kind": kind,
        "dependency_operation_keys": dependencies,
        "condition": None,
        "object_id": "same-object",
        "representation_id": "raw-video",
        "logical_bytes": logical_bytes,
        "operation_adapter": "test-adapter-v1",
        "resource_adapter": None,
        "link_adapter": None if link_adapter is None else dict(link_adapter),
        "cache_adapter": None,
        "cache_scope_id": None,
        "task_executor": {"semantic_quality_enabled": False},
        "execution_node_id": node,
        "execution_container": f"node-{node.lower()}",
        "destination_node_id": node,
        "destination_container": f"node-{node.lower()}",
        "measure_actual_duration": True,
        "simulation_hint_used_as_measured_duration": False,
    }


def _link(
    source: str,
    destination: str,
    bandwidth_bytes_per_second: int,
    round_trip_time_ms: float,
) -> dict[str, Any]:
    return {
        "adapter": "application-rate-rtt-shaper-v1",
        "link_id": f"{source}-{destination}-test",
        "source_node_id": source,
        "destination_node_id": destination,
        "bandwidth_bytes_per_second": bandwidth_bytes_per_second,
        "round_trip_time_ms": round_trip_time_ms,
        "required_capabilities": [],
    }


def _chain(
    trial_key: str,
    *,
    primary_bandwidth_bytes_per_second: int,
    primary_round_trip_time_ms: float,
) -> list[dict[str, Any]]:
    control = _operation(
        f"{trial_key}|control", "control", "N3", [],
        trial_key=trial_key, logical_bytes=0,
    )
    read = _operation(
        f"{trial_key}|scan-raw", "storage_read", "N3", [control["operation_key"]],
        trial_key=trial_key, logical_bytes=720_000_000,
    )
    transfer = _operation(
        f"{trial_key}|transfer-scan", "network_transfer", "N3",
        [read["operation_key"]], trial_key=trial_key, logical_bytes=720_000_000,
        link_adapter=_link(
            "N3", "N8", primary_bandwidth_bytes_per_second,
            primary_round_trip_time_ms,
        ),
    )
    decode = _operation(
        f"{trial_key}|scan-compute", "compute", "N8", [transfer["operation_key"]],
        trial_key=trial_key, logical_bytes=0,
    )
    send = _operation(
        f"{trial_key}|send-model-input", "network_transfer", "N8",
        [decode["operation_key"]], trial_key=trial_key, logical_bytes=262_144,
        link_adapter=_link("N8", "N6", 12_500_000_000, 0.2),
    )
    infer = _operation(
        f"{trial_key}|infer", "compute", "N6", [send["operation_key"]],
        trial_key=trial_key, logical_bytes=0,
    )
    return [control, read, transfer, decode, send, infer]


def _target_ms(operation: Mapping[str, Any]) -> float:
    link = operation["link_adapter"]
    assert isinstance(link, Mapping)
    return (
        float(operation["logical_bytes"])
        / float(link["bandwidth_bytes_per_second"])
        * 1000.0
        + float(link["round_trip_time_ms"])
    )


def _fingerprint(root: Path) -> dict[str, str]:
    return {
        path.name: sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.iterdir())
        if path.is_file()
    }


def _restamp_legacy_v1_run(run_dir: Path) -> None:
    """Downgrade a test fixture into a verifier-accepted v1 artifact.

    This imitates a historical timing-bearing full-chain record without
    changing its observed service values.  The calibration audit must accept
    it through the public verifier and label, rather than upgrade, its legacy
    provenance.
    """

    summary_path = run_dir / "flowmesh-container-full-chain-run.json"
    rows_path = run_dir / "flowmesh-container-full-chain-task-results.jsonl"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    task_rows = [
        json.loads(line)
        for line in rows_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for row in task_rows:
        row["telemetry_provenance_version"] = TELEMETRY_PROVENANCE_LEGACY_VERSION
        for field in (
            "container_result_schema_version",
            "runtime_epoch",
            "destination_runtime_epoch",
            "network_http_exchange_ms",
            "application_shaping_sleep_ms",
        ):
            row.pop(field, None)
    summary["schema_version"] = FLOWMESH_CONTAINER_FULL_CHAIN_RUN_LEGACY_SCHEMA_VERSION
    summary.pop("node_api_urls", None)
    summary.pop("runtime_epoch_binding", None)
    summary["telemetry"] = _aggregate_telemetry(
        task_rows,
        provenance_version=TELEMETRY_PROVENANCE_LEGACY_VERSION,
    )
    summary["telemetry_provenance"] = {
        "version": TELEMETRY_PROVENANCE_LEGACY_VERSION,
        "fields": dict(TELEMETRY_FIELD_PROVENANCE_V1),
        "disclaimers": list(TELEMETRY_DISCLAIMERS_V1),
    }
    summary_body = json.dumps(
        summary, indent=2, sort_keys=True, ensure_ascii=False
    ).encode("utf-8") + b"\n"
    results_body = "".join(
        json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n"
        for row in task_rows
    ).encode("utf-8")
    summary_path.write_bytes(summary_body)
    rows_path.write_bytes(results_body)
    checksum_path = run_dir / "SHA256SUMS"
    checksums = []
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        digest, _, name = line.partition("  ")
        if name == summary_path.name:
            digest = sha256(summary_body).hexdigest()
        elif name == rows_path.name:
            digest = sha256(results_body).hexdigest()
        checksums.append(f"{digest}  {name}")
    checksum_path.write_text("\n".join(checksums) + "\n", encoding="utf-8")


def _restamp_calibration_report(output_dir: Path, mutate: Any) -> None:
    """Re-stamp an audit fixture to test structural checks beyond hashes."""

    report_path = output_dir / "full-chain-calibration-report.json"
    manifest_path = output_dir / "full-chain-calibration-manifest.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    mutate(report)
    report_body = json.dumps(
        report, indent=2, sort_keys=True, ensure_ascii=False
    ).encode("utf-8") + b"\n"
    report_path.write_bytes(report_body)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["output_sha256"][report_path.name] = sha256(report_body).hexdigest()
    manifest_body = json.dumps(
        manifest, indent=2, sort_keys=True, ensure_ascii=False
    ).encode("utf-8") + b"\n"
    manifest_path.write_bytes(manifest_body)

    checksums_path = output_dir / "SHA256SUMS"
    checksums = []
    for line in checksums_path.read_text(encoding="utf-8").splitlines():
        digest, _, name = line.partition("  ")
        if name == report_path.name:
            digest = sha256(report_body).hexdigest()
        elif name == manifest_path.name:
            digest = sha256(manifest_body).hexdigest()
        checksums.append(f"{digest}  {name}")
    checksums_path.write_text("\n".join(checksums) + "\n", encoding="utf-8")


class FullChainCalibrationAuditTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def _make_pair(
        self,
        *,
        wrong_recorded_target: bool = False,
        service_below_target: bool = False,
    ) -> tuple[Path, Path, Path, Path]:
        fast_key = "matrix|W2|D0|r0000"
        slow_key = "matrix|W2|D4|r0000"
        operations = [
            *_chain(
                fast_key,
                primary_bandwidth_bytes_per_second=3_125_000_000,
                primary_round_trip_time_ms=2.0,
            ),
            *_chain(
                slow_key,
                primary_bandwidth_bytes_per_second=1_250_000,
                primary_round_trip_time_ms=30.0,
            ),
        ]
        source = self.root / "container_operations.jsonl"
        source.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in operations),
            encoding="utf-8",
        )

        def plan_and_run(
            name: str,
            trial_key: str,
        ) -> tuple[Path, Path]:
            plan = self.root / f"{name}-plan"
            plan_flowmesh_container_full_physical_chain(
                container_operations_path=source,
                node_api_urls=_URLS,
                worker_alias="container-smoke-worker",
                smoke_id=f"{name}-smoke",
                trial_key=trial_key,
                api_task_timeout_seconds=900,
                output_dir=plan,
            )
            frozen = json.loads(
                (plan / "flowmesh-container-full-chain-plan.json").read_text(
                    encoding="utf-8"
                )
            )
            targets = {
                row["operation_key"]: _target_ms(row)
                for row in frozen["operations"]
                if row["operation_kind"] == "network_transfer"
            }

            def set_consistent_network_timing(body: dict[str, Any]) -> None:
                target = targets.get(body["operation_key"])
                if target is None:
                    return
                reported_target = target
                if wrong_recorded_target and name == "fast":
                    reported_target += 1.0
                below_target = (
                    service_below_target
                    and name == "fast"
                    and body["operation_key"].endswith("|transfer-scan")
                )
                service = target - 1.0 if below_target else target + 5.0
                body["application_shaping_target_ms"] = reported_target
                body["service_time_ms"] = service
                # Keep the v2 timing decomposition internally consistent.
                # The audit does not treat either component as a network
                # measurement; this only makes the fake container record
                # satisfy the runtime's own same-record validation.
                http_exchange = 0.0 if below_target else 5.0
                body["network_http_exchange_ms"] = http_exchange
                body["application_shaping_sleep_ms"] = service - http_exchange
                body["finished_monotonic_ns"] = (
                    body["started_monotonic_ns"] + int(service * 1_000_000)
                )

            client = FakeFlowMeshClient()
            client.mutate_result = set_consistent_network_timing
            run = self.root / f"{name}-run"
            run_flowmesh_container_full_physical_chain(
                plan_dir=plan,
                output_dir=run,
                client=client,
                settings=FlowMeshSettings(
                    worker_alias="container-smoke-worker",
                    validate_before_submit=True,
                ),
                runtime_epoch_probe=_runtime_epoch_probe,
            )
            return plan, run

        fast_plan, fast_run = plan_and_run("fast", fast_key)
        slow_plan, slow_run = plan_and_run("slow", slow_key)
        return fast_plan, fast_run, slow_plan, slow_run

    def test_audits_fast_and_slow_paths_without_fitting_or_throughput_claims(
        self,
    ) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair()
        before = {
            name: _fingerprint(path)
            for name, path in {
                "fast-plan": fast_plan,
                "fast-run": fast_run,
                "slow-plan": slow_plan,
                "slow-run": slow_run,
            }.items()
        }
        output = self.root / "audit"
        report = audit_flowmesh_container_full_chain_calibration(
            fast_plan_dir=fast_plan,
            fast_run_dir=fast_run,
            slow_plan_dir=slow_plan,
            slow_run_dir=slow_run,
            output_dir=output,
        )
        self.assertEqual("COMPLETE", report["status"])
        self.assertEqual(0, report["parameters_fitted"])
        self.assertFalse(report["network_throughput_derived"])
        self.assertFalse(report["source_artifacts_modified"])
        self.assertEqual(4, report["network_transfer_observation_count"])
        self.assertGreater(report["primary_target_delta_ms"], 0.0)
        self.assertEqual(
            "VERIFIED",
            verify_flowmesh_container_full_chain_calibration(output)["status"],
        )
        after = {
            name: _fingerprint(path)
            for name, path in {
                "fast-plan": fast_plan,
                "fast-run": fast_run,
                "slow-plan": slow_plan,
                "slow-run": slow_run,
            }.items()
        }
        self.assertEqual(before, after)
        payload = json.loads(
            (output / "full-chain-calibration-report.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertFalse(payload["calibration"]["physical_network_rate_inferred"])
        self.assertFalse(payload["eligible_for_scientific_claims"])
        self.assertIn(
            "not an independent network measurement",
            " ".join(payload["measurement_boundaries"]),
        )
        self.assertNotIn("throughput_bytes_per_second", json.dumps(payload))

    def test_accepts_verified_legacy_v1_pair_and_labels_it(self) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair()
        _restamp_legacy_v1_run(fast_run)
        _restamp_legacy_v1_run(slow_run)

        output = self.root / "legacy-audit"
        audit_flowmesh_container_full_chain_calibration(
            fast_plan_dir=fast_plan,
            fast_run_dir=fast_run,
            slow_plan_dir=slow_plan,
            slow_run_dir=slow_run,
            output_dir=output,
        )
        report = json.loads(
            (output / "full-chain-calibration-report.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertTrue(
            report["source_schema_compatibility"][
                "configured_fast_run_is_legacy_v1alpha1"
            ]
        )
        self.assertTrue(
            report["source_schema_compatibility"][
                "configured_slow_run_is_legacy_v1alpha1"
            ]
        )
        self.assertEqual(
            TELEMETRY_PROVENANCE_LEGACY_VERSION,
            report["paths"]["configured_fast"][
                "source_telemetry_provenance_version"
            ],
        )
        self.assertEqual(
            "VERIFIED",
            verify_flowmesh_container_full_chain_calibration(output)["status"],
        )

    def test_cli_audits_and_verifies_without_a_service_client(self) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair()
        output = self.root / "cli-audit"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = cli_main([
                "audit-flowmesh-container-full-chain-calibration",
                "--fast-plan-dir", str(fast_plan),
                "--fast-run-dir", str(fast_run),
                "--slow-plan-dir", str(slow_plan),
                "--slow-run-dir", str(slow_run),
                "--output-dir", str(output),
                "--compact",
            ])
        self.assertEqual(0, exit_code)
        self.assertEqual("COMPLETE", json.loads(stdout.getvalue())["status"])
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = cli_main([
                "verify-flowmesh-container-full-chain-calibration",
                "--output-dir", str(output),
                "--compact",
            ])
        self.assertEqual(0, exit_code)
        self.assertEqual("VERIFIED", json.loads(stdout.getvalue())["status"])

    def test_rejects_a_result_target_that_does_not_match_the_frozen_link(self) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair(
            wrong_recorded_target=True
        )
        output = self.root / "rejected"
        with self.assertRaisesRegex(
            FullChainCalibrationAuditError,
            "recorded shaping target does not match",
        ):
            audit_flowmesh_container_full_chain_calibration(
                fast_plan_dir=fast_plan,
                fast_run_dir=fast_run,
                slow_plan_dir=slow_plan,
                slow_run_dir=slow_run,
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_rejects_a_service_duration_below_the_configured_target(self) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair(
            service_below_target=True
        )
        with self.assertRaisesRegex(
            FullChainCalibrationAuditError,
            "below the configured shaping target",
        ):
            audit_flowmesh_container_full_chain_calibration(
                fast_plan_dir=fast_plan,
                fast_run_dir=fast_run,
                slow_plan_dir=slow_plan,
                slow_run_dir=slow_run,
                output_dir=self.root / "rejected",
            )

    def test_refuses_to_write_inside_a_frozen_input_directory(self) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair()
        with self.assertRaisesRegex(
            FullChainCalibrationAuditError,
            "outside every frozen input artifact",
        ):
            audit_flowmesh_container_full_chain_calibration(
                fast_plan_dir=fast_plan,
                fast_run_dir=fast_run,
                slow_plan_dir=slow_plan,
                slow_run_dir=slow_run,
                output_dir=fast_plan / "audit",
            )

    def test_verifier_rejects_tampered_audit_output(self) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair()
        output = self.root / "audit"
        audit_flowmesh_container_full_chain_calibration(
            fast_plan_dir=fast_plan,
            fast_run_dir=fast_run,
            slow_plan_dir=slow_plan,
            slow_run_dir=slow_run,
            output_dir=output,
        )
        report = output / "full-chain-calibration-report.json"
        report.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            FullChainCalibrationAuditError, "checksum mismatch"
        ):
            verify_flowmesh_container_full_chain_calibration(output)

    def test_verifier_rejects_a_restamped_physical_rate_claim(self) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair()
        output = self.root / "audit"
        audit_flowmesh_container_full_chain_calibration(
            fast_plan_dir=fast_plan,
            fast_run_dir=fast_run,
            slow_plan_dir=slow_plan,
            slow_run_dir=slow_run,
            output_dir=output,
        )

        def add_claim(report: dict[str, Any]) -> None:
            report["calibration"]["physical_network_rate_inferred"] = True

        _restamp_calibration_report(output, add_claim)
        with self.assertRaisesRegex(
            FullChainCalibrationAuditError,
            "physical_network_rate_inferred",
        ):
            verify_flowmesh_container_full_chain_calibration(output)

    def test_fast_and_slow_must_have_the_same_frozen_workload_shape(self) -> None:
        fast_plan, fast_run, slow_plan, slow_run = self._make_pair()
        plan_path = slow_plan / "flowmesh-container-full-chain-plan.json"
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        plan["operations"][0]["object_id"] = "different-object"
        # This intentionally re-stamps the plan and its checksum so the
        # pairing guard, rather than a superficial checksum error, is tested.
        from pathfinder.integrations.flowmesh.container_dag import _document_sha256

        plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
        body = json.dumps(
            plan, indent=2, sort_keys=True, ensure_ascii=False
        ).encode("utf-8") + b"\n"
        plan_path.write_bytes(body)
        rows = []
        for line in (slow_plan / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
            digest, _, name = line.partition("  ")
            if name == plan_path.name:
                digest = sha256(body).hexdigest()
            rows.append(f"{digest}  {name}")
        (slow_plan / "SHA256SUMS").write_text("\n".join(rows) + "\n", encoding="utf-8")

        run_path = slow_run / "flowmesh-container-full-chain-run.json"
        run = json.loads(run_path.read_text(encoding="utf-8"))
        run["plan_sha256"] = plan["plan_sha256"]
        # The v2 runtime-epoch binding is part of the run's plan binding;
        # re-stamp it too so the calibration pairing check receives an
        # otherwise valid, deliberately workload-mismatched source pair.
        run["runtime_epoch_binding"]["plan_sha256"] = plan["plan_sha256"]
        run_body = json.dumps(
            run, indent=2, sort_keys=True, ensure_ascii=False
        ).encode("utf-8") + b"\n"
        run_path.write_bytes(run_body)
        rows = []
        for line in (slow_run / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
            digest, _, name = line.partition("  ")
            if name == run_path.name:
                digest = sha256(run_body).hexdigest()
            rows.append(f"{digest}  {name}")
        (slow_run / "SHA256SUMS").write_text("\n".join(rows) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(
            FullChainCalibrationAuditError,
            "frozen workload shape",
        ):
            audit_flowmesh_container_full_chain_calibration(
                fast_plan_dir=fast_plan,
                fast_run_dir=fast_run,
                slow_plan_dir=slow_plan,
                slow_run_dir=slow_run,
                output_dir=self.root / "rejected",
            )


if __name__ == "__main__":
    unittest.main()
