from __future__ import annotations

import contextlib
import errno
import io
import json
import os
import signal
import tempfile
import threading
import time
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest
from pathlib import Path
from unittest import mock

import pathfinder.simulator.container_execution as container_execution
from pathfinder.cli import main as cli_main
from pathfinder.simulator import (
    CONTAINER_OPERATION_SCHEMA_VERSION,
    CONTAINER_NODE_RESULT_SCHEMA_VERSION,
    ContainerExecutionError,
    ContainerNodeError,
    SemanticExecutionError,
    ContainerNodeRuntime,
    LocalContainerError,
    build_local_container_compose,
    align_local_container_semantic_scores,
    build_portable_execution_plan,
    create_container_node_server,
    execute_local_container_plan,
    execute_local_container_semantic_run,
    plan_container_backend,
    preflight_local_container_host,
    serve_container_node,
    verify_local_container_compose,
    verify_container_execution,
    verify_local_container_semantic_run,
    verify_local_container_semantic_score_alignment,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)


class AtomicCheckpointWriteTest(unittest.TestCase):
    def test_transient_windows_replace_lock_is_retried(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            target = Path(temporary_directory) / "trial_checkpoint.jsonl"
            real_replace = container_execution.os.replace
            replace_attempts = 0

            def transient_then_replace(source: Path, destination: Path) -> None:
                nonlocal replace_attempts
                replace_attempts += 1
                if replace_attempts < 3:
                    raise PermissionError(
                        errno.EACCES,
                        "simulated Windows sharing violation",
                        str(destination),
                    )
                real_replace(source, destination)

            with (
                mock.patch.object(
                    container_execution.os,
                    "replace",
                    side_effect=transient_then_replace,
                ),
                mock.patch.object(container_execution.time, "sleep") as sleep,
            ):
                container_execution._atomic_write(target, b"durable\n")

            self.assertEqual(target.read_bytes(), b"durable\n")
            self.assertEqual(replace_attempts, 3)
            self.assertEqual(sleep.call_count, 2)

    def test_persistent_replace_lock_is_not_hidden(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            target = Path(temporary_directory) / "trial_checkpoint.jsonl"
            locked = PermissionError(
                errno.EACCES,
                "simulated persistent Windows sharing violation",
                str(target),
            )

            with (
                mock.patch.object(
                    container_execution.os,
                    "replace",
                    side_effect=locked,
                ) as replace,
                mock.patch.object(container_execution.time, "sleep") as sleep,
            ):
                with self.assertRaises(PermissionError):
                    container_execution._atomic_write(target, b"durable\n")

            self.assertEqual(
                replace.call_count,
                container_execution._ATOMIC_REPLACE_MAX_ATTEMPTS,
            )
            self.assertEqual(
                sleep.call_count,
                container_execution._ATOMIC_REPLACE_MAX_ATTEMPTS - 1,
            )
            self.assertFalse(target.exists())
            self.assertEqual(
                list(Path(temporary_directory).glob("*.tmp")),
                [],
            )

    def test_non_permission_replace_failure_is_not_retried(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            target = Path(temporary_directory) / "trial_checkpoint.jsonl"

            with (
                mock.patch.object(
                    container_execution.os,
                    "replace",
                    side_effect=FileNotFoundError("simulated invalid path"),
                ) as replace,
                mock.patch.object(container_execution.time, "sleep") as sleep,
            ):
                with self.assertRaises(FileNotFoundError):
                    container_execution._atomic_write(target, b"durable\n")

            replace.assert_called_once()
            sleep.assert_not_called()


def _operation(kind: str, node_id: str = "N3", size: int = 4096) -> dict:
    return {
        "schema_version": CONTAINER_OPERATION_SCHEMA_VERSION,
        "operation_key": f"trial-1|{kind}",
        "operation_kind": kind,
        "execution_node_id": node_id,
        "destination_container": "unused",
        "object_id": "fixture-object",
        "representation_id": "raw-video",
        "logical_bytes": size,
        "resource_adapter": {
            "resource_id": f"{node_id}.hdd",
            "node_id": node_id,
            "resource_kind": "storage",
        },
    }


def _mock_endpoints() -> dict[str, dict[str, str]]:
    return {
        f"N{number}": {
            "container_name": f"test-n{number}",
            "container_url": f"http://N{number}.test",
            "host_health_url": f"http://N{number}.test/healthz",
            "host_operation_url": f"http://N{number}.test/execute",
        }
        for number in range(1, 9)
    }


def _mock_node_response(
    url: str,
    payload: dict | None,
    *,
    epoch_suffix: str = "stable",
) -> dict:
    if payload is None:
        node_id = url.split("//", 1)[1].split(".", 1)[0]
        return {
            "status": "ok",
            "node_id": node_id,
            "runtime_epoch": f"epoch-{node_id}-{epoch_suffix}",
            "semantic_quality_enabled": False,
        }
    kind = payload["operation_kind"]
    return {
        "schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "operation_key": payload["operation_key"],
        "execution_node_id": payload["execution_node_id"],
        "outcome_type": "completed",
        "telemetry_complete": True,
        "credentials_recorded": False,
        "semantic_task_quality_evaluated": False,
        "service_time_ms": 0.01,
        "logical_bytes": payload["logical_bytes"],
        "physical_bytes": (
            payload["logical_bytes"]
            if kind in ("storage_read", "cache_read", "network_transfer")
            else 0
        ),
        "cache_result": "miss" if kind == "cache_lookup" else None,
        "cache_evictions": [],
        "payload_sha256": None,
        "application_shaping_target_ms": None,
        "idempotent_replay": False,
    }


class ContainerNodeRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_storage_read_uses_real_bounded_bytes_and_stable_digest(self) -> None:
        runtime = ContainerNodeRuntime("N3", self.root / "state")
        operation = _operation("storage_read")
        first = runtime.execute(operation)
        second = runtime.execute(operation)
        self.assertEqual("completed", first["outcome_type"])
        self.assertTrue(first["telemetry_complete"])
        self.assertEqual(4096, first["logical_bytes"])
        self.assertEqual(4096, first["physical_bytes"])
        self.assertEqual(first["payload_sha256"], second["payload_sha256"])
        self.assertGreaterEqual(
            first["fixture_materialization_ms_excluded_from_storage_measurement"],
            0.0,
        )
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(
            first["fixture_materialization_ms_excluded_from_storage_measurement"],
            second["fixture_materialization_ms_excluded_from_storage_measurement"],
        )
        self.assertFalse(first["semantic_task_quality_evaluated"])

    def test_runtime_epoch_and_operation_key_idempotency_are_explicit(self) -> None:
        runtime = ContainerNodeRuntime("N3", self.root / "state")
        self.assertRegex(runtime.health()["runtime_epoch"], r"^[0-9a-f]{32}$")
        operation = _operation("storage_read")
        first = runtime.execute(operation)
        replay = runtime.execute(operation)
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(replay["idempotent_replay"])
        changed = dict(operation)
        changed["logical_bytes"] += 1
        with self.assertRaisesRegex(ContainerNodeError, "different input"):
            runtime.execute(changed)

    def test_wrong_node_and_oversize_operation_are_rejected(self) -> None:
        runtime = ContainerNodeRuntime(
            "N3",
            self.root / "state",
            max_operation_bytes=1024,
        )
        with self.assertRaisesRegex(ContainerNodeError, "different node"):
            runtime.execute(_operation("storage_read", node_id="N4"))
        oversized = _operation("storage_read", size=1025)
        oversized["operation_key"] = "trial-2|storage_read"
        with self.assertRaisesRegex(ContainerNodeError, "safety limit"):
            runtime.execute(oversized)

    def test_network_transfer_sends_exact_bytes_to_another_node(self) -> None:
        destination = create_container_node_server(
            "N7",
            self.root / "destination",
            max_operation_bytes=8192,
        )
        thread = threading.Thread(target=destination.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(destination.server_close)
        self.addCleanup(destination.shutdown)
        operation = _operation("network_transfer", size=8192)
        operation["destination_url"] = (
            f"http://127.0.0.1:{destination.server_address[1]}"
        )
        operation["destination_container"] = "pathfinder-sim-n7"
        operation["link_adapter"] = {
            "link_id": "N3-N7",
            "bandwidth_bytes_per_second": 100_000_000,
            "round_trip_time_ms": 1.0,
        }
        source = ContainerNodeRuntime("N3", self.root / "source")
        result = source.execute(operation)
        self.assertEqual(8192, result["physical_bytes"])
        self.assertEqual(1.08192, result["application_shaping_target_ms"])
        self.assertRegex(result["payload_sha256"], r"^[0-9a-f]{64}$")
        self.assertGreaterEqual(result["service_time_ms"], 1.0)

    def test_cache_insert_does_not_claim_payload_io(self) -> None:
        runtime = ContainerNodeRuntime("N7", self.root / "state")
        operation = _operation("cache_insert", node_id="N7")
        operation["cache_adapter"] = {
            "cache_id": "N7.cache",
            "capacity_bytes": 10_000,
            "initial_entries": [],
        }
        result = runtime.execute(operation)
        self.assertEqual(0, result["physical_bytes"])

    def test_frozen_initial_cache_entry_is_observed_as_a_hit(self) -> None:
        runtime = ContainerNodeRuntime("N7", self.root / "state")
        operation = _operation("cache_lookup", node_id="N7", size=0)
        operation["object_id"] = "video-descriptive"
        operation["representation_id"] = "multimodal_digest"
        operation["cache_adapter"] = {
            "cache_id": "N7.cache",
            "capacity_bytes": 10_000,
            "initial_entries": [{
                "object_id": "video-descriptive",
                "representation_id": "multimodal_digest",
                "size_bytes": 100,
            }],
        }
        result = runtime.execute(operation)
        self.assertEqual("hit", result["cache_result"])

    def test_service_handles_sigterm_without_deadlocking_main_loop(self) -> None:
        installed_handlers: dict[int, object] = {}
        restored_handlers: dict[int, object] = {}
        old_handlers = {
            signal.SIGTERM: object(),
            signal.SIGINT: object(),
        }

        class FakeServer:
            def __init__(self) -> None:
                self.shutdown_count = 0
                self.closed = False
                self.poll_interval = None
                self.shutdown_observed = threading.Event()

            def serve_forever(self, *, poll_interval: float) -> None:
                self.poll_interval = poll_interval
                handler = installed_handlers[signal.SIGTERM]
                assert callable(handler)
                handler(signal.SIGTERM, None)
                if not self.shutdown_observed.wait(timeout=1.0):
                    raise AssertionError("SIGTERM did not trigger server shutdown")

            def shutdown(self) -> None:
                self.shutdown_count += 1
                self.shutdown_observed.set()

            def server_close(self) -> None:
                self.closed = True

        server = FakeServer()

        def install_handler(signal_number: int, handler: object) -> None:
            if handler in old_handlers.values():
                restored_handlers[signal_number] = handler
            else:
                installed_handlers[signal_number] = handler

        with (
            mock.patch(
                "pathfinder.simulator.container_node.create_container_node_server",
                return_value=server,
            ),
            mock.patch(
                "pathfinder.simulator.container_node.signal.getsignal",
                side_effect=lambda number: old_handlers[number],
            ),
            mock.patch(
                "pathfinder.simulator.container_node.signal.signal",
                side_effect=install_handler,
            ),
        ):
            serve_container_node("N1", self.root / "state")

        self.assertEqual(1, server.shutdown_count)
        self.assertTrue(server.closed)
        self.assertEqual(0.1, server.poll_interval)
        self.assertEqual(old_handlers, restored_handlers)


class LocalComposePackageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.portable = self.root / "portable"
        self.container_plan = self.root / "container-plan"
        build_portable_execution_plan(SCENARIO, output_dir=self.portable)
        plan_container_backend(
            SCENARIO,
            self.portable,
            CONTAINER_SPEC,
            output_dir=self.container_plan,
        )

    def test_compose_package_is_deterministic_complete_and_nonlaunching(self) -> None:
        first = self.root / "compose-a"
        second = self.root / "compose-b"
        report = build_local_container_compose(
            self.container_plan,
            output_dir=first,
        )
        build_local_container_compose(self.container_plan, output_dir=second)
        self.assertEqual(8, report["service_count"])
        self.assertFalse(report["docker_called"])
        self.assertFalse(report["container_started"])
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        verified = verify_local_container_compose(first)
        self.assertEqual("VERIFIED_NOT_LAUNCHED", verified["status"])
        compose = (first / "compose.yaml").read_text(encoding="utf-8")
        self.assertEqual(8, compose.count("    container_name:"))
        self.assertEqual(8, compose.count("    build:"))
        self.assertEqual(0, report["pinned_image_count"])
        self.assertFalse(report["image_pinning_enforced"])
        self.assertTrue(report["build_context_included"])
        self.assertIn("read_only: true", compose)
        self.assertNotIn("NET_ADMIN", compose)
        self.assertNotIn("token", compose.casefold())

    def test_semantic_executor_is_explicit_and_contains_no_credential_value(self) -> None:
        output = self.root / "semantic-compose"
        report = build_local_container_compose(
            self.container_plan,
            output_dir=output,
            semantic_executor_node_id="N6",
            semantic_artifact_source_node_ids=("N3",),
        )
        self.assertTrue(report["semantic_quality_enabled"])
        self.assertEqual("N6", report["semantic_executor_node_id"])
        self.assertTrue(report["semantic_runtime_build"])
        self.assertEqual(
            "pathfinder-simulator-node:semantic-local",
            report["semantic_runtime_image"],
        )
        self.assertFalse(report["image_pinning_enforced"])
        self.assertTrue(report["build_context_included"])
        self.assertFalse(report["semantic_llm_credentials_bound"])
        self.assertEqual(["N3"], report["semantic_artifact_source_node_ids"])
        compose = (output / "compose.yaml").read_text(encoding="utf-8")
        self.assertEqual(1, compose.count('"--enable-semantic-llm"'))
        self.assertIn("PATHFINDER_SEMANTIC_LLM_API_KEY", compose)
        self.assertIn("PATHFINDER_SEMANTIC_ARTIFACT_ROOT", compose)
        self.assertIn('"pathfinder-sim-n3-origin-cold"', compose)
        self.assertEqual(8, compose.count("    build:"))
        self.assertEqual(
            8,
            compose.count('    image: "pathfinder-simulator-node:semantic-local"'),
        )
        self.assertNotIn("not-a-real-secret", compose)
        verified = verify_local_container_compose(output)
        self.assertTrue(verified["semantic_quality_enabled"])
        self.assertTrue(verified["semantic_runtime_build"])
        self.assertEqual("N6", verified["semantic_executor_node_id"])
        self.assertEqual(["N3"], verified["semantic_artifact_source_node_ids"])

    def test_pinned_images_are_enforced_and_cannot_be_rebuilt(self) -> None:
        digest = "sha256:" + "a" * 64
        spec = json.loads(CONTAINER_SPEC.read_text(encoding="utf-8"))
        for node in spec["nodes"]:
            node["image_digest"] = digest
        spec_path = self.root / "pinned-container-spec.json"
        spec_path.write_text(json.dumps(spec), encoding="utf-8")
        container_plan = self.root / "pinned-container-plan"
        plan_container_backend(
            SCENARIO,
            self.portable,
            spec_path,
            output_dir=container_plan,
        )
        output = self.root / "pinned-compose"
        report = build_local_container_compose(container_plan, output_dir=output)
        compose = (output / "compose.yaml").read_text(encoding="utf-8")

        self.assertEqual(8, report["pinned_image_count"])
        self.assertTrue(report["image_pinning_enforced"])
        self.assertFalse(report["build_context_included"])
        self.assertNotIn("    build:", compose)
        self.assertEqual(
            8,
            compose.count(
                '    image: "pathfinder-infra-node:development@'
                + digest
                + '"'
            ),
        )
        verified = verify_local_container_compose(output)
        self.assertTrue(verified["image_pinning_enforced"])

    def test_compose_checksum_tampering_is_rejected(self) -> None:
        output = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=output)
        path = output / "compose.yaml"
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(LocalContainerError, "checksum mismatch"):
            verify_local_container_compose(output)

    def test_cli_generates_package_without_launching(self) -> None:
        output = self.root / "cli-compose"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "build-local-container-compose",
                "--container-plan-dir",
                str(self.container_plan),
                "--output-dir",
                str(output),
                "--compact",
            ])
        self.assertEqual(0, status)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["docker_called"])
        self.assertEqual(8, payload["service_count"])

    def test_read_only_preflight_reports_missing_docker(self) -> None:
        with mock.patch(
            "pathfinder.simulator.local_container.shutil.which",
            return_value=None,
        ):
            report = preflight_local_container_host()
        self.assertEqual("BLOCKED", report["status"])
        self.assertIn("docker_cli_missing", report["blockers"])
        self.assertTrue(report["read_only_probe"])
        self.assertFalse(report["service_started"])

    def test_serial_driver_executes_one_small_frozen_trial(self) -> None:
        compose = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        endpoints = {}
        for node_number in range(1, 9):
            node_id = f"N{node_number}"
            server = create_container_node_server(
                node_id,
                self.root / f"state-{node_id}",
                max_operation_bytes=1024 * 1024,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
            base = f"http://127.0.0.1:{server.server_address[1]}"
            endpoints[node_id] = {
                "container_name": f"test-{node_id.casefold()}",
                "container_url": base,
                "host_health_url": base + "/healthz",
                "host_operation_url": base + "/v1/operations/execute",
            }
        trial_key = (
            "flowmesh-infra-4x8-local-smoke-v1|"
            "smoke-descriptive|D2|r0000"
        )
        output = self.root / "execution"
        report = execute_local_container_plan(
            compose,
            self.portable,
            output_dir=output,
            trial_key=trial_key,
            endpoint_override=endpoints,
            request_timeout_seconds=10.0,
        )
        self.assertEqual("PARTIAL_SMOKE", report["status"])
        self.assertEqual(1, report["executed_trial_count"])
        self.assertFalse(report["semantic_task_quality_evaluated"])
        records = [
            json.loads(line)
            for line in (output / "infrastructure_records.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertEqual(1, len(records))
        self.assertIsNone(records[0]["task_success"])
        self.assertGreater(records[0]["network_bytes"], 0)
        self.assertEqual("VERIFIED", verify_container_execution(output)["status"])

    def test_concurrent_driver_enforces_frozen_slots_and_measures_queue(self) -> None:
        compose = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        endpoints = {
            f"N{number}": {
                "container_name": f"test-n{number}",
                "container_url": f"http://N{number}.test",
                "host_health_url": f"http://N{number}.test/healthz",
                "host_operation_url": f"http://N{number}.test/execute",
            }
            for number in range(1, 9)
        }

        def request(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            self.assertGreater(timeout_seconds, 0.0)
            if payload is None:
                node_id = url.split("//", 1)[1].split(".", 1)[0]
                return {
                    "status": "ok",
                    "node_id": node_id,
                    "runtime_epoch": f"epoch-{node_id}",
                    "semantic_quality_enabled": False,
                }
            if payload["operation_kind"] == "control":
                # Four trials reach a two-slot frozen control resource almost
                # together.  Holding it makes admission queueing observable.
                time.sleep(0.04)
            physical = (
                payload["logical_bytes"]
                if payload["operation_kind"]
                in ("storage_read", "cache_read", "network_transfer")
                else 0
            )
            return {
                "schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
                "operation_key": payload["operation_key"],
                "execution_node_id": payload["execution_node_id"],
                "outcome_type": "completed",
                "telemetry_complete": True,
                "credentials_recorded": False,
                "semantic_task_quality_evaluated": False,
                "service_time_ms": 40.0,
                "logical_bytes": payload["logical_bytes"],
                "physical_bytes": physical,
                "cache_result": None,
                "cache_evictions": [],
                "payload_sha256": None,
                "application_shaping_target_ms": None,
            }

        output = self.root / "concurrent-execution"
        with mock.patch(
            "pathfinder.simulator.container_execution._request_json",
            side_effect=request,
        ):
            report = execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=8,
                max_concurrency=4,
                endpoint_override=endpoints,
                request_timeout_seconds=10.0,
            )
        self.assertEqual("concurrent", report["execution_mode"])
        self.assertFalse(report["serial_driver"])
        self.assertTrue(report["contention_measured"])
        self.assertTrue(report["queue_time_measured"])
        self.assertTrue(report["observed_contention"])
        self.assertGreaterEqual(report["peak_active_trials"], 3)
        self.assertGreaterEqual(report["queued_operation_count"], 2)
        self.assertGreater(report["total_queue_time_ms"], 20.0)
        self.assertTrue(report["trial_admission_queue_measured"])
        self.assertTrue(report["latency_includes_trial_admission_queue"])
        self.assertEqual("planned-trial-arrival", report["latency_origin"])

        events = [
            json.loads(line)
            for line in (output / "operation_results.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        queued_controls = [
            row
            for row in events
            if row["operation_kind"] == "control"
            and row["queue_observed"] is True
        ]
        self.assertGreaterEqual(len(queued_controls), 2)
        self.assertTrue(all(
            row["contention_key"] == "resource:N1.control"
            and row["resource_capacity_slots"] == 2
            and row["queue_time_measurement"]
            == "driver-enforced-frozen-slot-admission"
            for row in queued_controls
        ))
        self.assertEqual(list(range(len(events))), [
            row["event_index"] for row in events
        ])
        records = [
            json.loads(line)
            for line in (output / "infrastructure_records.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertTrue(any(
            row["trial_admission_queue_ms"] > 0 for row in records
        ))
        for row in records:
            self.assertAlmostEqual(
                row["latency_ms"],
                row["trial_admission_queue_ms"]
                + row["active_execution_latency_ms"],
            )
            self.assertEqual(4, row["trial_admission_slots"])
        admission_times = [
            row["arrival_time_ms"] + row["trial_admission_queue_ms"]
            for row in records
        ]
        self.assertEqual(sorted(admission_times), admission_times)
        verified = verify_container_execution(output)
        self.assertEqual("VERIFIED", verified["status"])

    def test_concurrent_trial_key_is_rejected_as_misleading(self) -> None:
        with self.assertRaisesRegex(
            ContainerExecutionError,
            "single trial_key requires max_concurrency=1",
        ):
            execute_local_container_plan(
                self.root / "unused-compose",
                self.root / "unused-portable",
                output_dir=self.root / "unused-output",
                trial_key="one-trial",
                max_concurrency=2,
            )

    def test_complete_run_cannot_override_frozen_admission_slots(self) -> None:
        compose = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        with self.assertRaisesRegex(
            ContainerExecutionError,
            "complete execution requires max_concurrency",
        ):
            execute_local_container_plan(
                compose,
                self.portable,
                output_dir=self.root / "wrong-admission-width",
                max_concurrency=2,
            )

    def test_concurrent_exact_trial_subset_retains_frozen_order(self) -> None:
        compose = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        selected = [
            "flowmesh-infra-4x8-local-smoke-v1|smoke-causal|D7|r0000",
            "flowmesh-infra-4x8-local-smoke-v1|smoke-descriptive|D7|r0000",
        ]
        endpoints = {
            f"N{number}": {
                "container_name": f"test-n{number}",
                "container_url": f"http://N{number}.test",
                "host_health_url": f"http://N{number}.test/healthz",
                "host_operation_url": f"http://N{number}.test/execute",
            }
            for number in range(1, 9)
        }

        def request(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            if payload is None:
                node_id = url.split("//", 1)[1].split(".", 1)[0]
                return {
                    "status": "ok",
                    "node_id": node_id,
                    "runtime_epoch": f"epoch-{node_id}",
                    "semantic_quality_enabled": False,
                }
            kind = payload["operation_kind"]
            return {
                "schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
                "operation_key": payload["operation_key"],
                "execution_node_id": payload["execution_node_id"],
                "outcome_type": "completed",
                "telemetry_complete": True,
                "credentials_recorded": False,
                "semantic_task_quality_evaluated": False,
                "service_time_ms": 0.01,
                "logical_bytes": payload["logical_bytes"],
                "physical_bytes": (
                    payload["logical_bytes"]
                    if kind in ("storage_read", "cache_read", "network_transfer")
                    else 0
                ),
                "cache_result": "hit" if kind == "cache_lookup" else None,
                "cache_evictions": [],
                "payload_sha256": None,
                "application_shaping_target_ms": None,
            }

        output = self.root / "exact-subset"
        with mock.patch(
            "pathfinder.simulator.container_execution._request_json",
            side_effect=request,
        ):
            report = execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_key=selected,
                max_concurrency=2,
                endpoint_override=endpoints,
            )
        self.assertEqual(2, report["executed_trial_count"])
        records = [
            json.loads(line)
            for line in (output / "infrastructure_records.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        # Caller order is deliberately reversed; frozen order wins.
        self.assertEqual(
            [
                "flowmesh-infra-4x8-local-smoke-v1|smoke-descriptive|D7|r0000",
                "flowmesh-infra-4x8-local-smoke-v1|smoke-causal|D7|r0000",
            ],
            [row["trial_key"] for row in records],
        )

    def test_interrupted_run_resumes_only_from_complete_trial_checkpoint(self) -> None:
        compose = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        endpoints = _mock_endpoints()
        output = self.root / "recoverable-execution"
        operation_trials: list[str] = []
        first_trial: str | None = None

        def interrupted_request(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            nonlocal first_trial
            self.assertGreater(timeout_seconds, 0.0)
            if payload is None:
                return _mock_node_response(url, payload)
            trial = str(payload["trial_key"])
            if first_trial is None:
                first_trial = trial
            if trial != first_trial:
                raise ContainerExecutionError("injected interruption")
            operation_trials.append(trial)
            return _mock_node_response(url, payload)

        with (
            mock.patch(
                "pathfinder.simulator.container_execution._request_json",
                side_effect=interrupted_request,
            ),
            self.assertRaisesRegex(ContainerExecutionError, "injected interruption"),
        ):
            execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=2,
                endpoint_override=endpoints,
                request_timeout_seconds=10.0,
            )

        self.assertTrue((output / "container_run_checkpoint.json").is_file())
        self.assertEqual(
            1,
            len((output / "trial_checkpoint.jsonl").read_text().splitlines()),
        )
        self.assertFalse((output / "SHA256SUMS").exists())
        self.assertEqual({first_trial}, set(operation_trials))

        resumed_operation_trials: list[str] = []

        def resumed_request(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            if payload is not None:
                resumed_operation_trials.append(str(payload["trial_key"]))
            return _mock_node_response(url, payload)

        with mock.patch(
            "pathfinder.simulator.container_execution._request_json",
            side_effect=resumed_request,
        ):
            report = execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=2,
                endpoint_override=endpoints,
                request_timeout_seconds=10.0,
            )
        self.assertTrue(report["resume_performed"])
        self.assertEqual(1, report["checkpoint_reused_trial_count"])
        self.assertEqual(1, report["executed_this_invocation"])
        self.assertEqual(2, report["checkpoint_trial_count"])
        self.assertNotIn(first_trial, resumed_operation_trials)
        self.assertEqual(1, len(set(resumed_operation_trials)))
        self.assertEqual("VERIFIED", verify_container_execution(output)["status"])

        with mock.patch(
            "pathfinder.simulator.container_execution._request_json",
            side_effect=AssertionError("completed output contacted a live node"),
        ):
            reused = execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=2,
                endpoint_override=endpoints,
                request_timeout_seconds=10.0,
            )
        self.assertTrue(reused["completed_output_reused"])
        self.assertTrue(reused["resume_performed"])
        self.assertEqual(0, reused["executed_this_invocation"])
        self.assertEqual(2, reused["checkpoint_reused_trial_count"])

    def test_resume_rejects_tampered_checkpoint_before_operation_execution(self) -> None:
        compose = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        endpoints = _mock_endpoints()
        output = self.root / "tampered-checkpoint"
        first_trial: str | None = None

        def interrupt_second(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            nonlocal first_trial
            if payload is None:
                return _mock_node_response(url, payload)
            if first_trial is None:
                first_trial = str(payload["trial_key"])
            if payload["trial_key"] != first_trial:
                raise ContainerExecutionError("injected interruption")
            return _mock_node_response(url, payload)

        with (
            mock.patch(
                "pathfinder.simulator.container_execution._request_json",
                side_effect=interrupt_second,
            ),
            self.assertRaises(ContainerExecutionError),
        ):
            execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=2,
                endpoint_override=endpoints,
            )
        ledger = output / "trial_checkpoint.jsonl"
        row = json.loads(ledger.read_text())
        row["record"]["logical_bytes"] += 1
        ledger.write_text(json.dumps(row, sort_keys=True) + "\n", encoding="utf-8")
        live_operations: list[str] = []

        def reject_operation(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            if payload is not None:
                live_operations.append(str(payload["operation_key"]))
            return _mock_node_response(url, payload)

        with (
            mock.patch(
                "pathfinder.simulator.container_execution._request_json",
                side_effect=reject_operation,
            ),
            self.assertRaisesRegex(ContainerExecutionError, "digest mismatch"),
        ):
            execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=2,
                endpoint_override=endpoints,
            )
        self.assertEqual([], live_operations)

    def test_resume_rejects_changed_node_epoch_before_operation_execution(self) -> None:
        compose = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        endpoints = _mock_endpoints()
        output = self.root / "changed-epoch"
        first_trial: str | None = None

        def interrupt_second(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            nonlocal first_trial
            if payload is None:
                return _mock_node_response(url, payload)
            if first_trial is None:
                first_trial = str(payload["trial_key"])
            if payload["trial_key"] != first_trial:
                raise ContainerExecutionError("injected interruption")
            return _mock_node_response(url, payload)

        with (
            mock.patch(
                "pathfinder.simulator.container_execution._request_json",
                side_effect=interrupt_second,
            ),
            self.assertRaises(ContainerExecutionError),
        ):
            execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=2,
                endpoint_override=endpoints,
            )
        live_operations: list[str] = []

        def restarted_nodes(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            if payload is not None:
                live_operations.append(str(payload["operation_key"]))
            return _mock_node_response(url, payload, epoch_suffix="restarted")

        with (
            mock.patch(
                "pathfinder.simulator.container_execution._request_json",
                side_effect=restarted_nodes,
            ),
            self.assertRaisesRegex(ContainerExecutionError, "runtime epoch changed"),
        ):
            execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=2,
                endpoint_override=endpoints,
            )
        self.assertEqual([], live_operations)


class _FakeSemanticLLMHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        self.server.requests.append({  # type: ignore[attr-defined]
            "payload": payload,
            "authorization": self.headers.get("Authorization"),
        })
        response = json.dumps({
            "choices": [{"message": {"content": getattr(self.server, "answer", "B")}}],
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


class LocalSemanticExecutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.portable = self.root / "portable"
        self.container_plan = self.root / "container-plan"
        build_portable_execution_plan(SCENARIO, output_dir=self.portable)
        plan_container_backend(
            SCENARIO,
            self.portable,
            CONTAINER_SPEC,
            output_dir=self.container_plan,
        )

    def _start(self, server: ThreadingHTTPServer) -> None:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

    def _semantic_inputs(self) -> tuple[Path, Path]:
        representations = self.root / "representations"
        artifact = representations / "objects" / "video-1" / "multimodal_digest.txt"
        artifact.parent.mkdir(parents=True)
        artifact.write_text("Two musicians perform while a woman approaches.", encoding="utf-8")
        manifest = self.root / "semantic-workloads.json"
        manifest.write_text(json.dumps({
            "schema_version": "pathfinder.local-container-semantic-workloads/v1alpha1",
            "semantic_run_id": "semantic-test-v1",
            "success_scoring_rule": "multiple-choice-option-id-exact-match-v1",
            "semantic_executor_node_id": "N6",
            "workloads": [{
                "semantic_trial_key": "semantic-test-v1|video-1|D2|r0000",
                "workload_id": "video-1-question",
                "object_id": "video-1",
                "design_id": "D2",
                "representation_id": "multimodal_digest",
                "representation_path": "objects/video-1/multimodal_digest.txt",
                "question": "Which option is correct?",
                "answer_options": [
                    {"option_id": "A", "text": "A vehicle crosses a river."},
                    {"option_id": "B", "text": "Musicians perform."},
                ],
                "correct_answer_id": "B",
            }],
        }), encoding="utf-8")
        return representations, manifest

    def test_semantic_runner_calls_container_executor_and_scores_answer(self) -> None:
        node = create_container_node_server(
            "N6",
            self.root / "n6-state",
            enable_semantic_llm=True,
        )
        self._start(node)
        node_port = int(node.server_address[1])
        compose = self.root / "semantic-compose"
        build_local_container_compose(
            self.container_plan,
            output_dir=compose,
            host_port_base=node_port - 6,
            semantic_executor_node_id="N6",
        )
        llm = ThreadingHTTPServer(("127.0.0.1", 0), _FakeSemanticLLMHandler)
        llm.requests = []  # type: ignore[attr-defined]
        self._start(llm)
        representations, manifest = self._semantic_inputs()
        output = self.root / "semantic-output"
        with mock.patch.dict(os.environ, {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": f"http://127.0.0.1:{llm.server_address[1]}",
            "PATHFINDER_SEMANTIC_LLM_MODEL": "test-text-model",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "not-a-real-secret",
            "PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS": "10",
        }, clear=False):
            report = execute_local_container_semantic_run(
                compose,
                manifest,
                representations,
                output_dir=output,
                request_timeout_seconds=10.0,
            )
        self.assertEqual("COMPLETE_SEMANTIC_LOCAL", report["status"])
        self.assertEqual(1, report["semantic_workload_count"])
        self.assertEqual(1.0, report["task_accuracy"])
        self.assertTrue(report["llm_called"])
        self.assertFalse(report["data_plane_artifact_delivery_verified"])
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]
        request = llm.requests[0]  # type: ignore[attr-defined]
        self.assertEqual("Bearer not-a-real-secret", request["authorization"])
        self.assertIn("multimodal_digest", request["payload"]["messages"][0]["content"])
        record = json.loads((output / "semantic_records.jsonl").read_text())
        self.assertEqual("B", record["final_answer"])
        self.assertTrue(record["task_success"])
        self.assertNotIn("prompt", record)
        self.assertNotIn("representation_text", record)
        output_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in output.iterdir()
            if path.is_file()
        )
        self.assertNotIn("not-a-real-secret", output_text)
        verified = verify_local_container_semantic_run(output)
        self.assertEqual("VERIFIED_OFFLINE", verified["status"])
        self.assertEqual(1.0, verified["task_accuracy"])

    def test_semantic_runner_refuses_compose_without_explicit_executor(self) -> None:
        compose = self.root / "infrastructure-compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        representations, manifest = self._semantic_inputs()
        with self.assertRaisesRegex(SemanticExecutionError, "no semantic executor"):
            execute_local_container_semantic_run(
                compose,
                manifest,
                representations,
                output_dir=self.root / "unexpected-output",
            )

    def test_score_alignment_recognizes_a_single_bracketed_option_without_replaying(self) -> None:
        node = create_container_node_server(
            "N6",
            self.root / "n6-state",
            enable_semantic_llm=True,
        )
        self._start(node)
        node_port = int(node.server_address[1])
        compose = self.root / "semantic-compose"
        build_local_container_compose(
            self.container_plan,
            output_dir=compose,
            host_port_base=node_port - 6,
            semantic_executor_node_id="N6",
        )
        llm = ThreadingHTTPServer(("127.0.0.1", 0), _FakeSemanticLLMHandler)
        llm.requests = []  # type: ignore[attr-defined]
        llm.answer = "[B]"  # type: ignore[attr-defined]
        self._start(llm)
        representations, legacy_manifest = self._semantic_inputs()
        source_output = self.root / "legacy-semantic-output"
        with mock.patch.dict(os.environ, {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": f"http://127.0.0.1:{llm.server_address[1]}",
            "PATHFINDER_SEMANTIC_LLM_MODEL": "test-text-model",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "not-a-real-secret",
        }, clear=False):
            legacy_report = execute_local_container_semantic_run(
                compose,
                legacy_manifest,
                representations,
                output_dir=source_output,
                request_timeout_seconds=10.0,
            )
        self.assertEqual(0.0, legacy_report["task_accuracy"])
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]

        canonical_manifest = self.root / "canonical-semantic-workloads.json"
        document = json.loads(legacy_manifest.read_text(encoding="utf-8"))
        document["success_scoring_rule"] = (
            "multiple-choice-option-id-canonical-match-v1"
        )
        canonical_manifest.write_text(json.dumps(document), encoding="utf-8")
        alignment_output = self.root / "semantic-score-alignment"
        aligned = align_local_container_semantic_scores(
            source_output,
            canonical_manifest,
            output_dir=alignment_output,
        )
        self.assertEqual(0, aligned["previous_task_success_count"])
        self.assertEqual(1, aligned["aligned_task_success_count"])
        self.assertEqual(1.0, aligned["aligned_task_accuracy"])
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]
        verified = verify_local_container_semantic_score_alignment(
            alignment_output
        )
        self.assertEqual(1.0, verified["aligned_task_accuracy"])

    def test_executor_fetches_representation_from_allowed_source_node(self) -> None:
        representations, _ = self._semantic_inputs()
        source = create_container_node_server(
            "N3",
            self.root / "n3-state",
            semantic_artifact_root=representations,
        )
        self._start(source)
        llm = ThreadingHTTPServer(("127.0.0.1", 0), _FakeSemanticLLMHandler)
        llm.requests = []  # type: ignore[attr-defined]
        self._start(llm)
        representation = (
            representations / "objects" / "video-1" / "multimodal_digest.txt"
        ).read_bytes()
        question = "Which option is correct?\n\nOptions:\n[A] x\n[B] y\n\nReturn exactly one option ID and no other text."
        prompt = ContainerNodeRuntime.build_semantic_prompt(
            "multimodal_digest",
            representation.decode("utf-8"),
            question,
        )
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "n6-state",
            enable_semantic_llm=True,
            transfer_port=int(source.server_address[1]),
            semantic_allowed_source_containers=("127.0.0.1",),
        )
        with mock.patch.dict(os.environ, {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": f"http://127.0.0.1:{llm.server_address[1]}",
            "PATHFINDER_SEMANTIC_LLM_MODEL": "test-text-model",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "not-a-real-secret",
        }, clear=False):
            result = runtime.semantic_complete({
                "schema_version": "pathfinder.container-node-semantic-request/v1alpha1",
                "semantic_request_id": "route-coupled-test",
                "execution_node_id": "N6",
                "source_node_id": "N3",
                "source_container_url": f"http://127.0.0.1:{source.server_address[1]}",
                "representation_path": "objects/video-1/multimodal_digest.txt",
                "representation_id": "multimodal_digest",
                "representation_sha256": sha256(representation).hexdigest(),
                "question": question,
                "prompt_sha256": sha256(prompt.encode("utf-8")).hexdigest(),
            })
        self.assertTrue(result["data_plane_artifact_delivery_verified"])
        self.assertEqual("N3", result["source_node_id"])
        self.assertEqual(len(representation), result["representation_delivery_bytes"])
        self.assertEqual("B", result["final_answer"])
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
