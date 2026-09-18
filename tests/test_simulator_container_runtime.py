from __future__ import annotations

import base64
import contextlib
import errno
import http.client
import io
import json
import os
import re
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
import pathfinder.simulator.container_node as container_node
import pathfinder.simulator.semantic_execution as semantic_execution
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
from pathfinder.simulator.container_node import (
    semantic_fusion_representation_sha256,
    semantic_frame_sequence_sha256,
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
            "operation_result_schema_version": (
                CONTAINER_NODE_RESULT_SCHEMA_VERSION
            ),
            "semantic_quality_enabled": False,
        }
    kind = payload["operation_kind"]
    return {
        "schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "operation_key": payload["operation_key"],
        "execution_node_id": payload["execution_node_id"],
        "runtime_epoch": (
            f"epoch-{payload['execution_node_id']}-{epoch_suffix}"
        ),
        "destination_runtime_epoch": (
            f"epoch-{payload['destination_node_id']}-{epoch_suffix}"
            if kind == "network_transfer"
            else None
        ),
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
        operation["destination_node_id"] = "N7"
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
        self.assertEqual(source.runtime_epoch, result["runtime_epoch"])
        self.assertEqual(
            destination.runtime.runtime_epoch,
            result["destination_runtime_epoch"],
        )
        self.assertIsNotNone(result["network_http_exchange_ms"])
        self.assertIsNotNone(result["application_shaping_sleep_ms"])

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

    def test_cache_scope_isolates_repetitions_but_preserves_one_lane(self) -> None:
        runtime = ContainerNodeRuntime("N7", self.root / "state")

        def cache_operation(kind: str, key: str, scope: str) -> dict:
            operation = _operation(kind, node_id="N7", size=100)
            operation["operation_key"] = key
            operation["object_id"] = "scope-object"
            operation["representation_id"] = "multimodal_digest"
            operation["cache_scope_id"] = scope
            operation["cache_adapter"] = {
                "cache_id": "N7.cache",
                "node_id": "N7",
                "capacity_bytes": 10_000,
                "initial_entries": [],
            }
            return operation

        inserted = runtime.execute(
            cache_operation("cache_insert", "D3-r0|insert", "D3|r0000")
        )
        self.assertEqual("D3|r0000", inserted["cache_scope_id"])
        same_lane = runtime.execute(
            cache_operation("cache_lookup", "D3-r0|lookup", "D3|r0000")
        )
        other_repetition = runtime.execute(
            cache_operation("cache_lookup", "D3-r1|lookup", "D3|r0001")
        )
        self.assertEqual("hit", same_lane["cache_result"])
        self.assertEqual("miss", other_repetition["cache_result"])
        self.assertEqual("D3|r0001", other_repetition["cache_scope_id"])

    def test_scoped_cache_read_requires_a_live_entry_in_the_lookup_namespace(self) -> None:
        runtime = ContainerNodeRuntime("N7", self.root / "state")

        def cache_operation(kind: str, key: str, scope: str) -> dict:
            operation = _operation(kind, node_id="N7", size=100)
            operation["operation_key"] = key
            operation["object_id"] = "cached-object"
            operation["representation_id"] = "multimodal_digest"
            operation["cache_scope_id"] = scope
            operation["cache_adapter"] = {
                "cache_id": "N7.cache",
                "node_id": "N7",
                "capacity_bytes": 10_000,
                "initial_entries": [],
            }
            return operation

        runtime.execute(cache_operation("cache_insert", "D3-r0|insert", "D3|r0000"))
        read = runtime.execute(
            cache_operation("cache_read", "D3-r0|read-local", "D3|r0000")
        )
        self.assertEqual("hit", read["cache_result"])
        self.assertEqual("D3|r0000", read["cache_scope_id"])
        self.assertEqual(100, read["physical_bytes"])

        with self.assertRaisesRegex(
            ContainerNodeError,
            "not backed by a current cache entry",
        ):
            runtime.execute(
                cache_operation("cache_read", "D3-r1|read-local", "D3|r0001")
            )

        unbound = _operation("cache_read", node_id="N7", size=100)
        unbound["operation_key"] = "D3-r0|unbound-cache-read"
        with self.assertRaisesRegex(
            ContainerNodeError,
            "requires a cache adapter",
        ):
            runtime.execute(unbound)

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


class InfraNodeDockerfileContractTest(unittest.TestCase):
    def test_n5_image_installs_frame_and_digest_extras(self) -> None:
        dockerfile = (
            ROOT / "containers" / "pathfinder-infra-node" / "Dockerfile"
        ).read_text(encoding="utf-8")
        logical = dockerfile.replace("\\\n", " ")
        match = re.search(
            r'pip install\b.*?"\.\[([^]]+)\]"',
            logical,
        )
        self.assertIsNotNone(match, "infra-node pip install was not found")
        assert match is not None
        installed = {
            value.strip() for value in match.group(1).split(",")
        }
        required = {"data-prep", "semantic-runtime"}
        self.assertFalse(
            required - installed,
            f"infra-node image is missing extras: {required - installed}",
        )


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
        self.assertEqual(
            "http://pathfinder-sim-n3-origin-cold:9080",
            verified["verified_host_endpoints"]["N3"]["container_url"],
        )

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

    def test_post_run_runtime_epoch_change_refuses_canonical_output(self) -> None:
        compose = self.root / "compose"
        build_local_container_compose(self.container_plan, output_dir=compose)
        health_calls = 0

        def restarted_after_execution(
            url: str,
            *,
            payload: dict | None,
            timeout_seconds: float,
        ) -> dict:
            nonlocal health_calls
            if payload is None:
                health_calls += 1
                suffix = "stable" if health_calls <= 8 else "restarted"
                return _mock_node_response(url, payload, epoch_suffix=suffix)
            return _mock_node_response(url, payload, epoch_suffix="stable")

        output = self.root / "epoch-changed-during-run"
        with (
            mock.patch(
                "pathfinder.simulator.container_execution._request_json",
                side_effect=restarted_after_execution,
            ),
            self.assertRaisesRegex(
                ContainerExecutionError,
                "runtime epoch changed during container execution",
            ),
        ):
            execute_local_container_plan(
                compose,
                self.portable,
                output_dir=output,
                trial_limit=1,
                endpoint_override=_mock_endpoints(),
            )
        self.assertFalse((output / "SHA256SUMS").exists())

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
                return _mock_node_response(url, payload, epoch_suffix="stable")
            if payload["operation_kind"] == "control":
                # Four trials reach a two-slot frozen control resource almost
                # together.  Holding it makes admission queueing observable.
                time.sleep(0.04)
            result = _mock_node_response(url, payload, epoch_suffix="stable")
            result["service_time_ms"] = 40.0
            return result

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
                return _mock_node_response(url, payload, epoch_suffix="stable")
            result = _mock_node_response(url, payload, epoch_suffix="stable")
            if payload["operation_kind"] == "cache_lookup":
                result["cache_result"] = "hit"
            return result

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
        response = getattr(self.server, "raw_response", None)
        if response is None:
            response = json.dumps({
                "model": getattr(
                    self.server,
                    "reported_model",
                    payload["model"],
                ),
                "choices": [{
                    "message": {
                        "content": getattr(self.server, "answer", "B"),
                    },
                }],
            }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


_TEST_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoHBwYIDAoM"
    "DAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/2wBDAQMEBAUEBQkFBQkUDQsN"
    "FBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBT/wAAR"
    "CAACAAIDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAA"
    "AgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkK"
    "FhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWG"
    "h4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl"
    "5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREA"
    "AgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYk"
    "NOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOE"
    "hYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk"
    "5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD7V+C37O3wp1v4OeBNR1H4ZeDr/ULzQbC4"
    "ubu60C0klnle3Rnd3aMlmYkkknJJJNFFFf0xln+40P8ABH8keXiP40/V/mf/2Q=="
)
SEMANTIC_NODE_TOKEN = "test-only-container-node-token"


def _vision_frame(
    frame_index: int,
    timestamp_seconds: float,
    *,
    padding_bytes: int = 0,
) -> dict:
    payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
    comment_segments = []
    distinguishing_comment = f"frame-{frame_index}".encode("ascii")
    comment_segments.append(distinguishing_comment)
    remaining_padding = padding_bytes
    while remaining_padding:
        chunk_size = min(remaining_padding, 65533)
        comment_segments.append(b"p" * chunk_size)
        remaining_padding -= chunk_size
    if comment_segments:
        encoded_comments = b"".join(
            b"\xff\xfe"
            + (len(comment) + 2).to_bytes(2, "big")
            + comment
            for comment in comment_segments
        )
        payload = (
            payload[:2]
            + encoded_comments
            + payload[2:]
        )
    return {
        "frame_index": frame_index,
        "timestamp_seconds": timestamp_seconds,
        "width": 2,
        "height": 2,
        "jpeg_size_bytes": len(payload),
        "jpeg_sha256": sha256(payload).hexdigest(),
        "jpeg_base64": base64.b64encode(payload).decode("ascii"),
    }


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

    def test_legacy_loopback_transport_ignores_ambient_proxy(self) -> None:
        direct_requests: list[str] = []
        proxy_requests: list[str] = []

        class DirectHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _send(self, value: dict[str, object]) -> None:
                body = json.dumps(value).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                direct_requests.append("GET " + self.path)
                self._send({"status": "ok"})

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(length)
                direct_requests.append("POST " + self.path)
                self._send({"status": "completed"})

        class ProxyHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _reject(self) -> None:
                proxy_requests.append(self.command + " " + self.path)
                self.send_response(502)
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_GET = _reject
            do_POST = _reject

        direct = ThreadingHTTPServer(("127.0.0.1", 0), DirectHandler)
        proxy = ThreadingHTTPServer(("127.0.0.1", 0), ProxyHandler)
        self._start(direct)
        self._start(proxy)
        direct_url = f"http://127.0.0.1:{direct.server_port}"
        proxy_url = f"http://127.0.0.1:{proxy.server_port}"
        with mock.patch.dict(os.environ, {
            "HTTP_PROXY": proxy_url,
            "HTTPS_PROXY": proxy_url,
            "ALL_PROXY": proxy_url,
            "NO_PROXY": "",
            "http_proxy": proxy_url,
            "https_proxy": proxy_url,
            "all_proxy": proxy_url,
            "no_proxy": "",
        }, clear=False):
            health = semantic_execution._get_json(
                direct_url + "/healthz",
                2.0,
            )
            result = semantic_execution._request_json(
                direct_url + "/v1/semantic/chat-completions",
                {"sentinel": "private"},
                2.0,
                SEMANTIC_NODE_TOKEN,
            )

        self.assertEqual("ok", health["status"])
        self.assertEqual("completed", result["status"])
        self.assertEqual(
            ["GET /healthz", "POST /v1/semantic/chat-completions"],
            direct_requests,
        )
        self.assertEqual([], proxy_requests)

    def test_legacy_loopback_transport_refuses_redirects(self) -> None:
        source_requests: list[str] = []
        destination_requests: list[str] = []

        class DestinationHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _record(self) -> None:
                destination_requests.append(self.command)
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            do_GET = _record
            do_POST = _record

        destination = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            DestinationHandler,
        )
        self._start(destination)
        destination_url = (
            f"http://127.0.0.1:{destination.server_port}/redirected"
        )

        class SourceHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _redirect(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                if length:
                    self.rfile.read(length)
                source_requests.append(self.command)
                self.send_response(302)
                self.send_header("Location", destination_url)
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_GET = _redirect
            do_POST = _redirect

        source = ThreadingHTTPServer(("127.0.0.1", 0), SourceHandler)
        self._start(source)
        source_url = f"http://127.0.0.1:{source.server_port}"
        with self.assertRaises(SemanticExecutionError):
            semantic_execution._get_json(source_url + "/healthz", 2.0)
        with self.assertRaises(SemanticExecutionError):
            semantic_execution._request_json(
                source_url + "/v1/semantic/chat-completions",
                {"sentinel": "private"},
                2.0,
                SEMANTIC_NODE_TOKEN,
            )

        self.assertEqual(["GET", "POST"], source_requests)
        self.assertEqual([], destination_requests)

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

    @staticmethod
    def _vision_request(
        frames: list[dict] | None = None,
        *,
        question: str = "Which option is correct? Return exactly one option ID.",
    ) -> dict:
        selected = (
            frames
            if frames is not None
            else [
                _vision_frame(0, 0.5),
                _vision_frame(1, 1.5),
            ]
        )
        prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
            "sampled_frame_bundle",
            len(selected),
            question,
        )
        return {
            "schema_version": (
                "pathfinder.container-node-semantic-request/v1alpha2"
            ),
            "semantic_request_id": "vision-test-v2",
            "execution_node_id": "N6",
            "representation_id": "sampled_frame_bundle",
            "representation_sha256": "a" * 64,
            "question": question,
            "prompt_sha256": sha256(prompt.encode("utf-8")).hexdigest(),
            "frame_sequence_sha256": semantic_frame_sequence_sha256(
                selected
            ),
            "frames": selected,
        }

    @staticmethod
    def _fusion_request() -> dict:
        frames = [
            _vision_frame(0, 0.5),
            _vision_frame(1, 1.5),
        ]
        digest_text = "Two musicians perform while a woman approaches."
        digest_sha256 = sha256(digest_text.encode("utf-8")).hexdigest()
        frame_sha256 = semantic_frame_sequence_sha256(frames)
        question = "Which option is correct? Return exactly one option ID."
        prompt = ContainerNodeRuntime.build_semantic_fusion_prompt(
            "multimodal_digest+sampled_frame_bundle",
            digest_text,
            len(frames),
            question,
        )
        return {
            "schema_version": (
                "pathfinder.container-node-semantic-request/v1alpha3"
            ),
            "semantic_request_id": "fusion-test-v3",
            "execution_node_id": "N6",
            "representation_id": (
                "multimodal_digest+sampled_frame_bundle"
            ),
            "representation_sha256": (
                semantic_fusion_representation_sha256(
                    digest_sha256,
                    frame_sha256,
                )
            ),
            "digest_text": digest_text,
            "digest_sha256": digest_sha256,
            "question": question,
            "prompt_sha256": sha256(prompt.encode("utf-8")).hexdigest(),
            "frame_sequence_sha256": frame_sha256,
            "frames": frames,
        }

    def test_semantic_http_endpoint_requires_json_content_type(self) -> None:
        node = create_container_node_server(
            "N6",
            self.root / "content-type-state",
            enable_semantic_llm=True,
            semantic_bearer_token=SEMANTIC_NODE_TOKEN,
        )
        self._start(node)
        body = json.dumps(self._vision_request()).encode("utf-8")
        connection = http.client.HTTPConnection(
            "127.0.0.1",
            node.server_port,
            timeout=2.0,
        )
        with mock.patch.object(
            node.runtime,
            "_call_semantic_llm",
        ) as llm_call:
            connection.request(
                "POST",
                "/v1/semantic/chat-completions",
                body=body,
                headers={
                    "Content-Type": "text/plain",
                    "Authorization": "Bearer " + SEMANTIC_NODE_TOKEN,
                },
            )
            response = connection.getresponse()
            response_body = response.read().decode("utf-8")
        connection.close()

        self.assertEqual(400, response.status)
        self.assertIn("Content-Type must be application/json", response_body)
        llm_call.assert_not_called()
        self.assertEqual({}, node.runtime._semantic_results)

    def test_semantic_runner_calls_container_executor_and_scores_answer(self) -> None:
        node = create_container_node_server(
            "N6",
            self.root / "n6-state",
            enable_semantic_llm=True,
            semantic_bearer_token=SEMANTIC_NODE_TOKEN,
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
            "PATHFINDER_CONTAINER_NODE_TOKEN": SEMANTIC_NODE_TOKEN,
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

    def test_semantic_runner_uses_the_verified_endpoint_snapshot(self) -> None:
        node = create_container_node_server(
            "N6",
            self.root / "snapshot-n6-state",
            enable_semantic_llm=True,
            semantic_bearer_token=SEMANTIC_NODE_TOKEN,
        )
        self._start(node)
        node_port = int(node.server_address[1])
        compose = self.root / "snapshot-semantic-compose"
        build_local_container_compose(
            self.container_plan,
            output_dir=compose,
            host_port_base=node_port - 6,
            semantic_executor_node_id="N6",
        )
        llm = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _FakeSemanticLLMHandler,
        )
        llm.requests = []  # type: ignore[attr-defined]
        self._start(llm)

        unexpected_requests: list[str] = []

        class UnexpectedEndpointHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def do_GET(self) -> None:
                unexpected_requests.append(self.path)
                self.send_response(500)
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_POST = do_GET

        unexpected = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            UnexpectedEndpointHandler,
        )
        self._start(unexpected)
        unexpected_base = f"http://127.0.0.1:{unexpected.server_port}"

        def verify_then_replace_endpoint(package: Path) -> dict[str, object]:
            verified = verify_local_container_compose(package)
            endpoint_path = package / "container_endpoints.json"
            document = json.loads(endpoint_path.read_text(encoding="utf-8"))
            endpoint = document["endpoints"]["N6"]
            endpoint["host_health_url"] = unexpected_base + "/healthz"
            endpoint["host_semantic_url"] = (
                unexpected_base + "/v1/semantic/chat-completions"
            )
            endpoint_path.write_text(
                json.dumps(document),
                encoding="utf-8",
            )
            return verified

        representations, manifest = self._semantic_inputs()
        output = self.root / "snapshot-semantic-output"
        with (
            mock.patch.object(
                semantic_execution,
                "verify_local_container_compose",
                side_effect=verify_then_replace_endpoint,
            ),
            mock.patch.dict(os.environ, {
                "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                    f"http://127.0.0.1:{llm.server_port}"
                ),
                "PATHFINDER_SEMANTIC_LLM_MODEL": "test-text-model",
                "PATHFINDER_SEMANTIC_LLM_API_KEY": "snapshot-test-secret",
                "PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS": "10",
                "PATHFINDER_CONTAINER_NODE_TOKEN": SEMANTIC_NODE_TOKEN,
            }, clear=False),
        ):
            report = execute_local_container_semantic_run(
                compose,
                manifest,
                representations,
                output_dir=output,
                request_timeout_seconds=10.0,
            )

        self.assertEqual("COMPLETE_SEMANTIC_LOCAL", report["status"])
        self.assertEqual([], unexpected_requests)
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]

    def test_v2_vision_request_sends_ordered_openai_image_content_without_leaking_it(
        self,
    ) -> None:
        llm = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _FakeSemanticLLMHandler,
        )
        llm.requests = []  # type: ignore[attr-defined]
        self._start(llm)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "n6-vision-state",
            enable_semantic_llm=True,
        )
        request = self._vision_request()
        frames = request["frames"]
        with mock.patch.dict(os.environ, {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                f"http://127.0.0.1:{llm.server_address[1]}"
            ),
            "PATHFINDER_SEMANTIC_LLM_MODEL": "test-vision-model",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "vision-test-secret",
            "PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS": "10",
        }, clear=False):
            result = runtime.semantic_complete(request)
            replay = runtime.semantic_complete(request)

        self.assertEqual(
            "pathfinder.container-node-semantic-result/v1alpha2",
            result["schema_version"],
        )
        self.assertEqual("ordered-jpeg-frames", result["semantic_input_kind"])
        self.assertEqual(2, result["frame_count"])
        self.assertEqual(
            sum(frame["jpeg_size_bytes"] for frame in frames),
            result["representation_delivery_bytes"],
        )
        self.assertEqual(
            request["frame_sequence_sha256"],
            result["frame_sequence_sha256"],
        )
        self.assertTrue(result["semantic_frame_payload_integrity_verified"])
        self.assertFalse(result["data_plane_artifact_delivery_verified"])
        self.assertFalse(result["idempotent_replay"])
        self.assertTrue(replay["idempotent_replay"])
        health = runtime.health()
        self.assertTrue(
            health["semantic_vision_request_adapter_supported"]
        )
        self.assertNotIn("semantic_vision_supported", health)
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]

        sent = llm.requests[0]  # type: ignore[attr-defined]
        self.assertEqual("Bearer vision-test-secret", sent["authorization"])
        self.assertEqual("test-vision-model", sent["payload"]["model"])
        content = sent["payload"]["messages"][0]["content"]
        self.assertIsInstance(content, list)
        self.assertEqual("text", content[0]["type"])
        self.assertEqual(
            request["prompt_sha256"],
            sha256(content[0]["text"].encode("utf-8")).hexdigest(),
        )
        self.assertEqual(
            [
                "data:image/jpeg;base64," + frame["jpeg_base64"]
                for frame in frames
            ],
            [entry["image_url"]["url"] for entry in content[1:]],
        )
        self.assertEqual(
            ["image_url", "image_url"],
            [entry["type"] for entry in content[1:]],
        )

        result_text = json.dumps(result, sort_keys=True)
        self.assertNotIn(request["question"], result_text)
        self.assertNotIn(content[0]["text"], result_text)
        self.assertNotIn("vision-test-secret", result_text)
        for frame in frames:
            self.assertNotIn(frame["jpeg_base64"], result_text)

    def test_v3_fusion_binds_digest_and_frames_without_persisting_payloads(
        self,
    ) -> None:
        llm = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _FakeSemanticLLMHandler,
        )
        llm.requests = []  # type: ignore[attr-defined]
        self._start(llm)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "n6-fusion-state",
            enable_semantic_llm=True,
        )
        request = self._fusion_request()
        with mock.patch.dict(os.environ, {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                f"http://127.0.0.1:{llm.server_port}"
            ),
            "PATHFINDER_SEMANTIC_LLM_MODEL": "test-fusion-model",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "fusion-test-secret",
            "PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS": "10",
        }, clear=False):
            result = runtime.semantic_complete(request)

        self.assertEqual(
            "pathfinder.container-node-semantic-result/v1alpha3",
            result["schema_version"],
        )
        self.assertEqual(
            "digest-and-ordered-jpeg-frames",
            result["semantic_input_kind"],
        )
        self.assertEqual(request["digest_sha256"], result["digest_sha256"])
        self.assertEqual(
            request["frame_sequence_sha256"],
            result["frame_sequence_sha256"],
        )
        self.assertEqual(
            request["representation_sha256"],
            result["representation_sha256"],
        )
        self.assertTrue(result["semantic_digest_payload_integrity_verified"])
        self.assertTrue(result["semantic_frame_payload_integrity_verified"])
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]
        content = llm.requests[0]["payload"]["messages"][0]["content"]  # type: ignore[attr-defined]
        self.assertEqual("text", content[0]["type"])
        self.assertIn(request["digest_text"], content[0]["text"])
        self.assertEqual(2, len(content[1:]))
        result_text = json.dumps(result, sort_keys=True)
        self.assertNotIn(request["digest_text"], result_text)
        self.assertNotIn(request["question"], result_text)
        self.assertNotIn("fusion-test-secret", result_text)

    def test_v3_fusion_rejects_component_drift_before_llm_call(self) -> None:
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "n6-fusion-drift-state",
            enable_semantic_llm=True,
        )
        cases = []
        wrong_digest = self._fusion_request()
        wrong_digest["semantic_request_id"] = "fusion-drift-digest-v3"
        wrong_digest["digest_text"] += " changed"
        cases.append((wrong_digest, "does not match digest_text"))
        wrong_binding = self._fusion_request()
        wrong_binding["semantic_request_id"] = "fusion-drift-binding-v3"
        wrong_binding["representation_sha256"] = "0" * 64
        cases.append((wrong_binding, "does not bind both fusion components"))
        for request, message in cases:
            with (
                self.subTest(message=message),
                mock.patch.object(runtime, "_call_semantic_llm") as call,
                self.assertRaisesRegex(ContainerNodeError, message),
            ):
                runtime.semantic_complete(request)
            call.assert_not_called()

    def test_semantic_llm_response_model_must_match_requested_model(self) -> None:
        llm = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _FakeSemanticLLMHandler,
        )
        llm.requests = []  # type: ignore[attr-defined]
        llm.reported_model = "different-model"  # type: ignore[attr-defined]
        self._start(llm)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "model-mismatch-state",
            enable_semantic_llm=True,
        )
        with (
            mock.patch.dict(os.environ, {
                "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                    f"http://127.0.0.1:{llm.server_address[1]}"
                ),
                "PATHFINDER_SEMANTIC_LLM_MODEL": "requested-model",
                "PATHFINDER_SEMANTIC_LLM_API_KEY": "model-test-secret",
            }, clear=False),
            self.assertRaisesRegex(
                ContainerNodeError,
                "response model differs from the requested model",
            ),
        ):
            runtime.semantic_complete(self._vision_request())
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]

    def test_semantic_llm_redirect_is_refused_without_forwarding_bearer(
        self,
    ) -> None:
        source_requests: list[dict[str, object]] = []
        destination_requests: list[dict[str, object]] = []

        class DestinationHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def do_GET(self) -> None:
                destination_requests.append({
                    "method": self.command,
                    "authorization": self.headers.get("Authorization"),
                })
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_POST = do_GET

        destination = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            DestinationHandler,
        )
        self._start(destination)
        destination_url = (
            f"http://127.0.0.1:{destination.server_address[1]}/steal"
        )

        class RedirectHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(length)
                source_requests.append({
                    "method": self.command,
                    "authorization": self.headers.get("Authorization"),
                })
                self.send_response(302)
                self.send_header("Location", destination_url)
                self.send_header("Content-Length", "0")
                self.end_headers()

        source = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
        self._start(source)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "redirect-state",
            enable_semantic_llm=True,
        )
        secret = "redirect-test-secret"
        with (
            mock.patch.dict(os.environ, {
                "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                    f"http://127.0.0.1:{source.server_address[1]}"
                ),
                "PATHFINDER_SEMANTIC_LLM_MODEL": "redirect-test-model",
                "PATHFINDER_SEMANTIC_LLM_API_KEY": secret,
            }, clear=False),
            self.assertRaisesRegex(
                ContainerNodeError,
                "failed with HTTP 302",
            ) as context,
        ):
            runtime._call_semantic_llm("Return one option ID.")

        self.assertEqual(1, len(source_requests))
        self.assertEqual(
            "Bearer " + secret,
            source_requests[0]["authorization"],
        )
        self.assertEqual([], destination_requests)
        self.assertNotIn(secret, str(context.exception))
        self.assertNotIn("steal", str(context.exception))

    def test_loopback_semantic_llm_ignores_ambient_proxy(self) -> None:
        proxy_requests: list[str] = []

        class ProxyHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def do_POST(self) -> None:
                proxy_requests.append(self.path)
                self.send_response(502)
                self.send_header("Content-Length", "0")
                self.end_headers()

        proxy = ThreadingHTTPServer(("127.0.0.1", 0), ProxyHandler)
        self._start(proxy)
        llm = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _FakeSemanticLLMHandler,
        )
        llm.requests = []  # type: ignore[attr-defined]
        self._start(llm)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "provider-proxy-state",
            enable_semantic_llm=True,
        )
        proxy_url = f"http://127.0.0.1:{proxy.server_port}"
        with mock.patch.dict(os.environ, {
            "HTTP_PROXY": proxy_url,
            "HTTPS_PROXY": proxy_url,
            "ALL_PROXY": proxy_url,
            "NO_PROXY": "",
            "http_proxy": proxy_url,
            "https_proxy": proxy_url,
            "all_proxy": proxy_url,
            "no_proxy": "",
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                f"http://127.0.0.1:{llm.server_port}"
            ),
            "PATHFINDER_SEMANTIC_LLM_MODEL": "loopback-model",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": "loopback-secret",
        }, clear=False):
            answer, model = runtime._call_semantic_llm(
                "Return one option ID."
            )

        self.assertEqual("B", answer)
        self.assertEqual("loopback-model", model)
        self.assertEqual(1, len(llm.requests))  # type: ignore[attr-defined]
        self.assertEqual([], proxy_requests)

    def test_semantic_llm_retries_transient_http_failures(self) -> None:
        requests: list[dict] = []

        class FlakyHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def do_POST(self) -> None:
                length = int(self.headers["Content-Length"])
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                requests.append(payload)
                if len(requests) < 3:
                    self.send_response(429)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                response = json.dumps({
                    "model": payload["model"],
                    "choices": [{"message": {"content": "C"}}],
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

        llm = ThreadingHTTPServer(("127.0.0.1", 0), FlakyHandler)
        self._start(llm)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "retry-transient-state",
            enable_semantic_llm=True,
        )
        with (
            mock.patch.dict(os.environ, {
                "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                    f"http://127.0.0.1:{llm.server_port}"
                ),
                "PATHFINDER_SEMANTIC_LLM_MODEL": "retry-test-model",
                "PATHFINDER_SEMANTIC_LLM_API_KEY": "retry-test-secret",
            }, clear=False),
            mock.patch.object(container_node.time, "sleep") as sleep,
        ):
            answer, model = runtime._call_semantic_llm("Return an option.")

        self.assertEqual(("C", "retry-test-model"), (answer, model))
        self.assertEqual(3, len(requests))
        self.assertEqual(
            [mock.call(5.0), mock.call(20.0)],
            sleep.call_args_list,
        )

    def test_semantic_llm_does_not_retry_nontransient_http_failure(self) -> None:
        requests = 0

        class RejectedHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def do_POST(self) -> None:
                nonlocal requests
                length = int(self.headers["Content-Length"])
                self.rfile.read(length)
                requests += 1
                self.send_response(400)
                self.send_header("Content-Length", "0")
                self.end_headers()

        llm = ThreadingHTTPServer(("127.0.0.1", 0), RejectedHandler)
        self._start(llm)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "retry-nontransient-state",
            enable_semantic_llm=True,
        )
        with (
            mock.patch.dict(os.environ, {
                "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                    f"http://127.0.0.1:{llm.server_port}"
                ),
                "PATHFINDER_SEMANTIC_LLM_MODEL": "retry-test-model",
                "PATHFINDER_SEMANTIC_LLM_API_KEY": "retry-test-secret",
            }, clear=False),
            mock.patch.object(container_node.time, "sleep") as sleep,
            self.assertRaisesRegex(
                ContainerNodeError,
                "semantic LLM request failed with HTTP 400",
            ),
        ):
            runtime._call_semantic_llm("Return an option.")

        self.assertEqual(1, requests)
        sleep.assert_not_called()

    def test_external_semantic_llm_keeps_default_proxy_discovery(self) -> None:
        sentinel = object()
        with mock.patch.object(
            container_node,
            "build_opener",
            return_value=sentinel,
        ) as build:
            self.assertIs(
                sentinel,
                container_node._semantic_llm_opener(
                    "https://model.example/v1"
                ),
            )
        external_handlers = build.call_args.args
        self.assertEqual(1, len(external_handlers))
        self.assertIsInstance(
            external_handlers[0],
            container_node._RejectRedirects,
        )

        with mock.patch.object(
            container_node,
            "build_opener",
            return_value=sentinel,
        ) as build:
            container_node._semantic_llm_opener(
                "http://127.0.0.1:8000/v1"
            )
        loopback_handlers = build.call_args.args
        self.assertTrue(
            any(
                isinstance(handler, container_node.ProxyHandler)
                for handler in loopback_handlers
            )
        )

    def test_semantic_llm_answer_is_bounded_and_cannot_echo_api_key(self) -> None:
        llm = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _FakeSemanticLLMHandler,
        )
        llm.requests = []  # type: ignore[attr-defined]
        self._start(llm)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "answer-boundary-state",
            enable_semantic_llm=True,
        )
        secret = "answer-echo-secret"
        environment = {
            "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                f"http://127.0.0.1:{llm.server_address[1]}"
            ),
            "PATHFINDER_SEMANTIC_LLM_MODEL": "answer-boundary-model",
            "PATHFINDER_SEMANTIC_LLM_API_KEY": secret,
        }

        def request(request_id: str) -> dict[str, object]:
            prompt = "Return one option ID."
            return {
                "schema_version": (
                    "pathfinder.container-node-semantic-request/v1alpha1"
                ),
                "semantic_request_id": request_id,
                "execution_node_id": "N6",
                "representation_sha256": "a" * 64,
                "prompt": prompt,
                "prompt_sha256": sha256(prompt.encode("utf-8")).hexdigest(),
            }

        echoed_answers = (
            secret,
            "prefix-" + secret,
            secret + "-suffix",
            "prefix-" + secret + "-suffix",
        )
        for index, answer in enumerate(echoed_answers):
            llm.answer = answer  # type: ignore[attr-defined]
            with (
                mock.patch.dict(os.environ, environment, clear=False),
                self.subTest(answer_position=index),
                self.assertRaisesRegex(
                    ContainerNodeError,
                    "contains a configured credential",
                ) as context,
            ):
                runtime.semantic_complete(request(f"credential-echo-{index}"))
            self.assertNotIn(secret, str(context.exception))
            self.assertEqual({}, runtime._semantic_results)

        llm.answer = "A" * (16 * 1024 + 1)  # type: ignore[attr-defined]
        with (
            mock.patch.dict(os.environ, environment, clear=False),
            self.assertRaisesRegex(
                ContainerNodeError,
                "answer exceeds the local safety limit",
            ),
        ):
            runtime.semantic_complete(request("oversized-answer"))
        self.assertEqual({}, runtime._semantic_results)

    def test_semantic_llm_response_rejects_duplicate_json_keys(self) -> None:
        llm = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _FakeSemanticLLMHandler,
        )
        llm.requests = []  # type: ignore[attr-defined]
        llm.raw_response = (  # type: ignore[attr-defined]
            b'{"model":"vision-test-model",'
            b'"model":"vision-test-model",'
            b'"choices":[{"message":{"content":"B"}}]}'
        )
        self._start(llm)
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "duplicate-response-state",
            enable_semantic_llm=True,
        )
        with (
            mock.patch.dict(os.environ, {
                "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                    f"http://127.0.0.1:{llm.server_address[1]}"
                ),
                "PATHFINDER_SEMANTIC_LLM_MODEL": "vision-test-model",
                "PATHFINDER_SEMANTIC_LLM_API_KEY": "duplicate-test-secret",
            }, clear=False),
            self.assertRaisesRegex(ContainerNodeError, "duplicate key"),
        ):
            runtime.semantic_complete(self._vision_request())

    def test_vision_decode_fails_closed_without_pillow(self) -> None:
        frames = [_vision_frame(0, 0.5)]
        with (
            mock.patch.object(
                container_node.importlib.util,
                "find_spec",
                return_value=None,
            ),
        ):
            # The host coordinator can still bind canonical frame bytes; only
            # the N6 semantic ingress makes the stronger full-decode claim.
            self.assertRegex(
                semantic_frame_sequence_sha256(frames),
                r"^[0-9a-f]{64}$",
            )
            with self.assertRaisesRegex(
                ContainerNodeError,
                "Pillow is not installed",
            ):
                container_node._validated_semantic_frames(frames)

    def test_vision_decode_timeout_fails_closed(self) -> None:
        payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
        with (
            mock.patch.object(
                container_node,
                "_semantic_vision_request_adapter_supported",
                return_value=True,
            ),
            mock.patch.object(
                container_node.subprocess,
                "run",
                side_effect=container_node.subprocess.TimeoutExpired(
                    cmd="isolated-decoder",
                    timeout=3.0,
                ),
            ),
            self.assertRaisesRegex(
                ContainerNodeError,
                "decode timeout",
            ),
        ):
            container_node._decoded_jpeg_dimensions(payload, 0)

    def test_vision_decode_enforces_total_bundle_pixel_bound(self) -> None:
        frames = [
            _vision_frame(0, 0.5),
            _vision_frame(1, 1.5),
        ]
        with (
            mock.patch.object(
                container_node,
                "_MAX_SEMANTIC_TOTAL_IMAGE_PIXELS",
                7,
            ),
            self.assertRaisesRegex(
                ContainerNodeError,
                "total decoded pixels exceed",
            ),
        ):
            container_node._validated_semantic_frames(frames)

    def test_vision_decode_treats_decompression_warning_as_error(self) -> None:
        payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
        with (
            mock.patch.object(
                container_node,
                "_MAX_SEMANTIC_IMAGE_PIXELS",
                1,
            ),
            self.assertRaisesRegex(
                ContainerNodeError,
                "could not be safely decoded",
            ),
        ):
            container_node._decoded_jpeg_dimensions(payload, 0)

    def test_v2_vision_request_rejects_invalid_frames_before_calling_llm(
        self,
    ) -> None:
        base_request = self._vision_request()

        def clone() -> dict:
            return json.loads(json.dumps(base_request))

        cases: list[tuple[str, dict, str]] = []

        unordered = clone()
        unordered["frames"].reverse()
        cases.append(("unordered", unordered, "ordered by contiguous"))

        repeated_timestamp = clone()
        repeated_timestamp["frames"][1]["timestamp_seconds"] = 0.5
        cases.append(
            ("timestamp", repeated_timestamp, "strictly increasing timestamps")
        )

        invalid_base64 = clone()
        invalid_base64["frames"][0]["jpeg_base64"] = "not+canonical==="
        cases.append(("base64", invalid_base64, "jpeg_base64 is invalid"))

        noncanonical_base64 = clone()
        noncanonical_base64["frames"][0]["jpeg_base64"] += "\n"
        cases.append(
            (
                "base64-whitespace",
                noncanonical_base64,
                "jpeg_base64 is not canonical",
            )
        )

        wrong_digest = clone()
        wrong_digest["frames"][0]["jpeg_sha256"] = "0" * 64
        cases.append(("jpeg-digest", wrong_digest, "does not match decoded bytes"))

        non_jpeg = clone()
        non_jpeg_bytes = b"\xff\xd8not-a-jpeg\xff\xd9"
        non_jpeg["frames"][0].update({
            "jpeg_size_bytes": len(non_jpeg_bytes),
            "jpeg_sha256": sha256(non_jpeg_bytes).hexdigest(),
            "jpeg_base64": base64.b64encode(non_jpeg_bytes).decode("ascii"),
        })
        cases.append(("jpeg", non_jpeg, "invalid JPEG marker"))

        undecodable = clone()
        valid_jpeg = bytearray(
            base64.b64decode(_TEST_JPEG_BASE64, validate=True)
        )
        scan_marker = valid_jpeg.index(b"\xff\xda")
        valid_jpeg[scan_marker + 5] = 0x7F
        undecodable_bytes = bytes(valid_jpeg)
        undecodable["frames"][0].update({
            "jpeg_size_bytes": len(undecodable_bytes),
            "jpeg_sha256": sha256(undecodable_bytes).hexdigest(),
            "jpeg_base64": base64.b64encode(undecodable_bytes).decode("ascii"),
        })
        cases.append(
            ("undecodable", undecodable, "could not be safely decoded")
        )

        wrong_dimensions = clone()
        wrong_dimensions["frames"][0]["width"] = 3
        cases.append(("dimensions", wrong_dimensions, "dimensions do not match"))

        oversized_frame = clone()
        oversized_frame["frames"][0]["jpeg_size_bytes"] = 512 * 1024 + 1
        cases.append(("frame-limit", oversized_frame, "per-frame byte limit"))

        wrong_sequence = clone()
        wrong_sequence["frame_sequence_sha256"] = "0" * 64
        cases.append(("sequence", wrong_sequence, "does not match the ordered"))

        extra_field = clone()
        extra_field["authorization"] = "must-not-be-accepted"
        cases.append(("extra-field", extra_field, "fields do not match"))

        too_many = clone()
        too_many["frames"] = [
            _vision_frame(index, index + 0.5)
            for index in range(33)
        ]
        too_many["frame_sequence_sha256"] = "0" * 64
        too_many_prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
            too_many["representation_id"],
            len(too_many["frames"]),
            too_many["question"],
        )
        too_many["prompt_sha256"] = sha256(
            too_many_prompt.encode("utf-8")
        ).hexdigest()
        cases.append(("frame-count", too_many, "frame count exceeds"))

        too_many_bytes = clone()
        too_many_bytes["frames"] = [
            _vision_frame(
                index,
                index + 0.5,
                padding_bytes=360 * 1024,
            )
            for index in range(3)
        ]
        too_many_bytes["frame_sequence_sha256"] = "0" * 64
        byte_limit_prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
            too_many_bytes["representation_id"],
            len(too_many_bytes["frames"]),
            too_many_bytes["question"],
        )
        too_many_bytes["prompt_sha256"] = sha256(
            byte_limit_prompt.encode("utf-8")
        ).hexdigest()
        cases.append(
            ("total-frame-bytes", too_many_bytes, "total JPEG bytes exceed")
        )

        for name, request, message in cases:
            request["semantic_request_id"] = f"invalid-{name}"
            runtime = ContainerNodeRuntime(
                "N6",
                self.root / f"invalid-{name}",
                enable_semantic_llm=True,
            )
            with (
                mock.patch.object(
                    runtime,
                    "_call_semantic_llm",
                    side_effect=AssertionError("invalid input reached LLM"),
                ),
                self.subTest(name=name),
                self.assertRaisesRegex(ContainerNodeError, message),
            ):
                runtime.semantic_complete(request)

    def test_v2_vision_request_bounds_prompt_and_pins_sequence_digest(self) -> None:
        frames = [
            _vision_frame(0, 0.5),
            _vision_frame(1, 1.5),
        ]
        digest = semantic_frame_sequence_sha256(frames)
        self.assertEqual(
            "85902fc17ded7544a273973ca883e342c4ec1236a7d4e84c5a5d31d91245536e",
            digest,
        )
        changed = json.loads(json.dumps(frames))
        changed[1]["timestamp_seconds"] = 1.75
        self.assertNotEqual(digest, semantic_frame_sequence_sha256(changed))

        long_question = "q" * (128 * 1024)
        request = self._vision_request(frames, question=long_question)
        request["semantic_request_id"] = "oversized-prompt"
        runtime = ContainerNodeRuntime(
            "N6",
            self.root / "oversized-prompt",
            enable_semantic_llm=True,
        )
        with (
            mock.patch.object(
                runtime,
                "_call_semantic_llm",
                side_effect=AssertionError("oversized prompt reached LLM"),
            ),
            self.assertRaisesRegex(
                ContainerNodeError,
                "vision prompt exceeds",
            ),
        ):
            runtime.semantic_complete(request)

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
            semantic_bearer_token=SEMANTIC_NODE_TOKEN,
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
            "PATHFINDER_CONTAINER_NODE_TOKEN": SEMANTIC_NODE_TOKEN,
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
            semantic_bearer_token=SEMANTIC_NODE_TOKEN,
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
            semantic_bearer_token=SEMANTIC_NODE_TOKEN,
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
