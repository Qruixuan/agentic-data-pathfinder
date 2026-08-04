from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pathfinder.data_agent_client import (
    DATA_AGENT_API_VERSION,
    DataAgentAccessRequest,
    DataAgentClientSettings,
    DataAgentHTTPError,
    HttpDataAgentClient,
)
from pathfinder.data_agent_manifest import (
    DATA_AGENT_MANIFEST_VERSION,
    DATA_OBJECT_CATALOG_VERSION,
    DataAgentManifestLookupError,
    load_data_agent_manifest,
)
from pathfinder.data_agent_server import (
    DataAgentServerSettings,
    create_data_agent_http_server,
)


class DataAgentServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        directory = Path(self.temporary_directory.name)
        self.inline_path = directory / "digest.txt"
        self.inline_path.write_text(
            "The red car enters at 00:17.",
            encoding="utf-8",
        )
        self.artifact_bytes = b"0123456789"
        self.artifact_path = directory / "video.bin"
        self.artifact_path.write_bytes(self.artifact_bytes)
        self.object_catalog_path = directory / "object-catalog.json"
        self.object_catalog_path.write_text(
            json.dumps(
                {
                    "schema_version": DATA_OBJECT_CATALOG_VERSION,
                    "catalog_version": "server-test-catalog-v1",
                    "objects": {
                        "video-001": {
                            "representations": {
                                "multimodal_digest": {
                                    "path": self.inline_path.name,
                                },
                                "compressed_video": {
                                    "path": self.artifact_path.name,
                                },
                            }
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        self.manifest_path = directory / "manifest.json"
        self.manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": DATA_AGENT_MANIFEST_VERSION,
                    "node_id": "test-data-node",
                    "require_plan_binding": True,
                    "object_catalog_path": self.object_catalog_path.name,
                    "representations": {
                        "multimodal_digest": {
                            "kind": "inline_text",
                            "media_type": "text/plain",
                            "path": "digest.txt",
                            "default_binding": {
                                "location": "node-1/nvme",
                                "minimum_latency_ms": 0,
                                "realized_cost": 0.25,
                                "cache_hit": True,
                            },
                            "plan_bindings": {
                                "D_test": {
                                    "location": "node-1/nvme",
                                    "minimum_latency_ms": 0,
                                    "realized_cost": 0.25,
                                    "cache_hit": True,
                                },
                                "D_slow": {
                                    "location": "node-1/nvme",
                                    "minimum_latency_ms": 25,
                                    "realized_cost": 0.25,
                                    "cache_hit": True,
                                }
                            },
                        },
                        "compressed_video": {
                            "kind": "artifact_uri",
                            "media_type": "application/octet-stream",
                            "path": "video.bin",
                            "default_binding": {
                                "location": "node-1/nvme",
                                "minimum_latency_ms": 0,
                                "realized_cost": 0.5,
                                "cache_hit": True,
                            },
                            "plan_bindings": {
                                "D_test": {
                                    "location": "node-1/nvme",
                                    "minimum_latency_ms": 0,
                                    "realized_cost": 0.5,
                                    "cache_hit": True,
                                }
                            },
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        self.operation_db = directory / "operations.sqlite3"
        self.server = create_data_agent_http_server(
            manifest_path=self.manifest_path,
            operation_db=self.operation_db,
            settings=DataAgentServerSettings(
                host="127.0.0.1",
                port=0,
                token="control-token",
                artifact_secret="artifact-secret",
            ),
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.addCleanup(self._stop_server)
        host, port = self.server.server_address[:2]
        self.base_url = f"http://{host}:{port}"
        self.client = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url=self.base_url,
                token="control-token",
                timeout_seconds=2,
                max_retries=0,
            )
        )

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def request(
        self,
        representation_id: str,
        *,
        access_id: str,
        location: str = "node-1/nvme",
        plan_id: str = "D_test",
        latency_multiplier: float = 1.0,
        object_id: str = "video-001",
    ) -> DataAgentAccessRequest:
        return DataAgentAccessRequest(
            access_id=access_id,
            session_id="session-test",
            trial_id="trial-test",
            plan_id=plan_id,
            plan_epoch=0,
            task_class_id="video_qa",
            representation_id=representation_id,
            event_index=0,
            latency_multiplier=latency_multiplier,
            binding={"location": location},
            object_id=object_id,
        )

    def test_inline_access_is_persistent_and_idempotent(self) -> None:
        request = self.request(
            "multimodal_digest",
            access_id="inline-access",
        )
        first = self.client.access(request)
        second = self.client.access(request)
        self.assertEqual(
            "The red car enters at 00:17.",
            first.payload.value,
        )
        self.assertEqual(first.payload.sha256, second.payload.sha256)
        self.assertEqual(0.25, first.realized_cost)
        self.assertEqual("video-001", first.object_id)
        self.assertEqual(
            "server-test-catalog-v1",
            first.object_catalog_version,
        )
        self.assertEqual("node-1/nvme", first.location)
        self.assertTrue(first.cache_hit)
        self.assertEqual(1, self.server.service.store.count())

    def test_same_access_id_with_different_request_is_rejected(self) -> None:
        self.client.access(
            self.request("multimodal_digest", access_id="reused-access")
        )
        with self.assertRaises(DataAgentHTTPError) as context:
            self.client.access(
                self.request("compressed_video", access_id="reused-access")
            )
        self.assertEqual(409, context.exception.status_code)

    def test_manifest_binding_mismatch_is_rejected(self) -> None:
        with self.assertRaises(DataAgentHTTPError) as context:
            self.client.access(
                self.request(
                    "multimodal_digest",
                    access_id="bad-binding",
                    location="different-node/ram",
                )
            )
        self.assertEqual(409, context.exception.status_code)

    def test_unconfigured_plan_is_rejected(self) -> None:
        with self.assertRaises(DataAgentHTTPError) as context:
            self.client.access(
                self.request(
                    "multimodal_digest",
                    access_id="unknown-plan",
                    plan_id="D_unknown",
                )
            )
        self.assertEqual(404, context.exception.status_code)

    def test_unconfigured_object_is_rejected(self) -> None:
        with self.assertRaises(DataAgentHTTPError) as context:
            self.client.access(
                self.request(
                    "multimodal_digest",
                    access_id="unknown-object",
                    object_id="video-missing",
                )
            )
        self.assertEqual(404, context.exception.status_code)

    def test_latency_multiplier_controls_minimum_service_delay(self) -> None:
        slept_seconds: list[float] = []
        self.server.service._sleep = slept_seconds.append
        result = self.client.access(
            self.request(
                "multimodal_digest",
                access_id="controlled-delay",
                plan_id="D_slow",
                latency_multiplier=2.0,
            )
        )
        self.assertEqual(1, len(slept_seconds))
        self.assertGreater(slept_seconds[0], 0.04)
        self.assertGreater(
            result.timings_ms["controlled_delay"],
            40.0,
        )

    def test_artifact_uri_supports_signed_range_download(self) -> None:
        result = self.client.access(
            self.request("compressed_video", access_id="artifact-access")
        )
        self.assertEqual("artifact_uri", result.payload.kind)
        artifact_url = result.payload.value
        self.assertIsInstance(artifact_url, str)
        with urlopen(artifact_url, timeout=2) as response:
            self.assertEqual(self.artifact_bytes, response.read())
        range_request = Request(
            artifact_url,
            headers={"Range": "bytes=2-5"},
        )
        with urlopen(range_request, timeout=2) as response:
            self.assertEqual(206, response.status)
            self.assertEqual(b"2345", response.read())
        telemetry = self.client.get_access_telemetry("artifact-access")
        self.assertEqual(2, telemetry.download_request_count)
        self.assertEqual(2, telemetry.completed_request_count)
        self.assertEqual(1, telemetry.full_download_count)
        self.assertEqual(len(self.artifact_bytes) + 4, telemetry.bytes_sent)
        self.assertEqual(
            "server-test-catalog-v1",
            telemetry.object_catalog_version,
        )
        self.assertGreaterEqual(telemetry.transfer_latency_ms, 0.0)
        self.assertIsNotNone(telemetry.latest_completed_at)

    def test_artifact_uri_rejects_tampered_signature(self) -> None:
        result = self.client.access(
            self.request("compressed_video", access_id="signed-access")
        )
        artifact_url = str(result.payload.value)
        tampered = artifact_url[:-1] + (
            "0" if artifact_url[-1] != "0" else "1"
        )
        with self.assertRaises(HTTPError) as context:
            urlopen(tampered, timeout=2)
        self.assertEqual(401, context.exception.code)

    def test_control_endpoint_requires_bearer_token(self) -> None:
        unauthenticated = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url=self.base_url,
                timeout_seconds=2,
                max_retries=0,
            )
        )
        with self.assertRaises(DataAgentHTTPError) as context:
            unauthenticated.access(
                self.request("multimodal_digest", access_id="unauthorized")
            )
        self.assertEqual(401, context.exception.status_code)

    def test_health_endpoint_reports_manifest(self) -> None:
        with urlopen(f"{self.base_url}/healthz", timeout=2) as response:
            payload = json.load(response)
        self.assertEqual("ok", payload["status"])
        self.assertEqual(DATA_AGENT_API_VERSION, payload["api_version"])
        self.assertEqual("test-data-node", payload["node_id"])
        self.assertEqual("server-test-catalog-v1", payload["object_catalog_version"])
        self.assertEqual(1, payload["object_count"])

    def test_manifest_resolves_relative_paths(self) -> None:
        manifest = load_data_agent_manifest(self.manifest_path)
        resolved = manifest.resolve(
            plan_id="D_test",
            object_id="video-001",
            representation_id="multimodal_digest",
            requested_location="node-1/nvme",
        )
        self.assertEqual(self.inline_path.resolve(), resolved.path)

    def test_manifest_without_catalog_keeps_single_file_compatibility(self) -> None:
        directory = Path(self.temporary_directory.name)
        manifest_payload = json.loads(
            self.manifest_path.read_text(encoding="utf-8")
        )
        manifest_payload.pop("object_catalog_path")
        legacy_path = directory / "legacy-manifest.json"
        legacy_path.write_text(json.dumps(manifest_payload), encoding="utf-8")
        manifest = load_data_agent_manifest(legacy_path)
        resolved = manifest.resolve(
            plan_id="D_test",
            representation_id="multimodal_digest",
            requested_location="node-1/nvme",
        )
        self.assertIsNone(resolved.object_id)
        self.assertEqual(self.inline_path.resolve(), resolved.path)

    def test_manifest_resolves_object_and_plan_specific_paths(self) -> None:
        directory = Path(self.temporary_directory.name)
        object_default = directory / "video-001.bin"
        object_plan = directory / "video-001-nvme.bin"
        object_default.write_bytes(b"origin")
        object_plan.write_bytes(b"nvme")
        catalog_path = directory / "objects.json"
        catalog_path.write_text(
            json.dumps(
                {
                    "schema_version": DATA_OBJECT_CATALOG_VERSION,
                    "catalog_version": "test-catalog-v1",
                    "objects": {
                        "video-001": {
                            "representations": {
                                "compressed_video": {
                                    "path": object_default.name,
                                    "plan_paths": {
                                        "D_test": object_plan.name,
                                    },
                                }
                            }
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        manifest_payload = json.loads(
            self.manifest_path.read_text(encoding="utf-8")
        )
        manifest_payload["object_catalog_path"] = catalog_path.name
        manifest_payload["representations"]["compressed_video"].pop("path")
        object_manifest_path = directory / "object-manifest.json"
        object_manifest_path.write_text(
            json.dumps(manifest_payload),
            encoding="utf-8",
        )
        manifest = load_data_agent_manifest(object_manifest_path)
        resolved = manifest.resolve(
            plan_id="D_test",
            object_id="video-001",
            representation_id="compressed_video",
            requested_location="node-1/nvme",
        )
        self.assertEqual("video-001", resolved.object_id)
        self.assertEqual(object_plan.resolve(), resolved.path)
        with self.assertRaises(DataAgentManifestLookupError):
            manifest.resolve(
                plan_id="D_test",
                object_id="video-missing",
                representation_id="compressed_video",
                requested_location="node-1/nvme",
            )


if __name__ == "__main__":
    unittest.main()
