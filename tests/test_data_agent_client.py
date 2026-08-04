from __future__ import annotations

import json
import tempfile
import threading
import unittest
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar

from pathfinder.config import load_config
from pathfinder.data_agent_client import (
    DATA_AGENT_API_VERSION,
    DataAgentAccessRequest,
    DataAgentAccessResult,
    DataAgentAccessTelemetry,
    DataAgentClientSettings,
    DataAgentPayload,
    DataAgentProtocolError,
    HttpDataAgentClient,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshAgentRunRequest,
)
from pathfinder.integrations.flowmesh.data_agent_backend import (
    RemoteDataAgentBackend,
)
from pathfinder.integrations.flowmesh.gateway import (
    AccessGateway,
    SQLiteSessionStore,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "minimal_system.json"


class RecordingDataAgentHandler(BaseHTTPRequestHandler):
    response_statuses: ClassVar[list[int]] = []
    requests: ClassVar[list[dict[str, Any]]] = []
    request_headers: ClassVar[list[dict[str, str]]] = []

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(content_length))
        type(self).requests.append(payload)
        type(self).request_headers.append(dict(self.headers.items()))
        status = (
            type(self).response_statuses.pop(0)
            if type(self).response_statuses
            else 200
        )
        if status == 200:
            response = {
                "api_version": DATA_AGENT_API_VERSION,
                "status": "succeeded",
                "access_id": payload["access_id"],
                "payload": {
                    "kind": "inline_text",
                    "media_type": "text/plain",
                    "value": "remote digest",
                    "sha256": sha256(b"remote digest").hexdigest(),
                },
                "telemetry": {
                    "service_latency_ms": 18.5,
                    "realized_cost": 0.42,
                    "bytes_read": 13,
                    "location": "data-node-1/nvme",
                    "cache_hit": True,
                    "timings_ms": {
                        "queue": 1.5,
                        "serve": 17.0,
                    },
                },
            }
        else:
            response = {"error": "temporarily unavailable"}
        body = json.dumps(response).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


class FakeDataAgentClient:
    def __init__(self) -> None:
        self.request: DataAgentAccessRequest | None = None

    def access(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentAccessResult:
        self.request = request
        return DataAgentAccessResult(
            access_id=request.access_id,
            payload=DataAgentPayload(
                kind="inline_text",
                media_type="text/plain",
                value="The red car enters at 00:17.",
                sha256=sha256(
                    b"The red car enters at 00:17."
                ).hexdigest(),
            ),
            service_latency_ms=80.0,
            client_round_trip_ms=125.0,
            realized_cost=0.73,
            bytes_read=512,
            location="data-node-2/nvme",
            cache_hit=True,
            timings_ms={"fetch": 5.0, "serve": 75.0},
            object_id=request.object_id,
            object_catalog_version="test-catalog-v1",
        )

    def get_access_telemetry(
        self,
        access_id: str,
    ) -> DataAgentAccessTelemetry:
        return DataAgentAccessTelemetry(
            access_id=access_id,
            object_id="video-001",
            representation_id="multimodal_digest",
            object_catalog_version="test-catalog-v1",
            download_request_count=1,
            completed_request_count=1,
            full_download_count=1,
            bytes_sent=2048,
            transfer_latency_ms=12.5,
            latest_completed_at=123.0,
        )


class DataAgentClientTest(unittest.TestCase):
    def setUp(self) -> None:
        RecordingDataAgentHandler.response_statuses = []
        RecordingDataAgentHandler.requests = []
        RecordingDataAgentHandler.request_headers = []
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            RecordingDataAgentHandler,
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.addCleanup(self._stop_server)

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def request(self, access_id: str = "access-test-001") -> DataAgentAccessRequest:
        return DataAgentAccessRequest(
            access_id=access_id,
            session_id="session-test",
            trial_id="trial-test",
            plan_id="D_structured_digest",
            plan_epoch=0,
            task_class_id="video_qa",
            representation_id="multimodal_digest",
            event_index=0,
            latency_multiplier=1.0,
            binding={"location": "shared_nvme"},
            object_id="video-001",
        )

    def client(
        self,
        *,
        max_retries: int = 0,
    ) -> HttpDataAgentClient:
        host, port = self.server.server_address
        return HttpDataAgentClient(
            DataAgentClientSettings(
                base_url=f"http://{host}:{port}",
                token="test-token",
                timeout_seconds=2.0,
                max_retries=max_retries,
            ),
            sleep=lambda _: None,
        )

    def test_http_client_sends_versioned_idempotent_request(self) -> None:
        result = self.client().access(self.request())
        self.assertEqual("remote digest", result.payload.value)
        self.assertEqual(18.5, result.service_latency_ms)
        self.assertIsNotNone(result.client_round_trip_ms)
        self.assertEqual(0.42, result.realized_cost)
        self.assertEqual(1, len(RecordingDataAgentHandler.requests))

        sent = RecordingDataAgentHandler.requests[0]
        self.assertEqual(DATA_AGENT_API_VERSION, sent["api_version"])
        self.assertEqual("session-test", sent["session_id"])
        self.assertEqual("video-001", sent["object_id"])
        self.assertEqual("shared_nvme", sent["binding"]["location"])
        headers = {
            name.lower(): value
            for name, value in RecordingDataAgentHandler.request_headers[0].items()
        }
        self.assertEqual("access-test-001", headers["idempotency-key"])
        self.assertEqual("Bearer test-token", headers["authorization"])

    def test_http_client_retries_with_the_same_idempotency_key(self) -> None:
        RecordingDataAgentHandler.response_statuses = [503, 200]
        result = self.client(max_retries=1).access(
            self.request("retry-access")
        )
        self.assertEqual("remote digest", result.payload.value)
        self.assertEqual(2, len(RecordingDataAgentHandler.requests))
        keys = [
            {
                name.lower(): value
                for name, value in headers.items()
            }["idempotency-key"]
            for headers in RecordingDataAgentHandler.request_headers
        ]
        self.assertEqual(["retry-access", "retry-access"], keys)

    def test_protocol_rejects_unsupported_version(self) -> None:
        with self.assertRaises(DataAgentProtocolError):
            DataAgentAccessResult.from_dict(
                {
                    "api_version": "unsupported/v9",
                    "status": "succeeded",
                }
            )

    def test_protocol_rejects_mismatched_inline_checksum(self) -> None:
        with self.assertRaises(DataAgentProtocolError):
            DataAgentPayload.from_dict(
                {
                    "kind": "inline_text",
                    "media_type": "text/plain",
                    "value": "actual content",
                    "sha256": sha256(b"different content").hexdigest(),
                }
            )

    def test_settings_reject_non_http_url(self) -> None:
        with self.assertRaises(ValueError):
            DataAgentClientSettings(base_url="file:///tmp/data-agent")


class RemoteDataAgentBackendTest(unittest.TestCase):
    def test_gateway_uses_remote_result_without_exposing_realized_cost(
        self,
    ) -> None:
        config = load_config(CONFIG_PATH)
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteSessionStore(
                Path(directory) / "gateway.sqlite3"
            )
            client = FakeDataAgentClient()
            gateway = AccessGateway(
                config,
                store,
                RemoteDataAgentBackend(client),
            )
            session = gateway.register_session(
                FlowMeshAgentRunRequest(
                    question="When does the red car enter?",
                    design_id="D_structured_digest",
                    task_class_id="video_qa",
                    quote_profile_id="digest_low",
                    seed=42,
                    trial_id="remote-backend",
                    object_id="video-001",
                )
            )
            response = gateway.access_representation(
                session.session_id,
                "multimodal_digest",
            )

            self.assertTrue(response["ok"])
            self.assertEqual(
                "The red car enters at 00:17.",
                response["content"],
            )
            self.assertEqual("inline_text", response["payload"]["kind"])
            self.assertEqual(125.0, response["felt_latency_ms"])
            self.assertNotIn("realized_cost", response)

            self.assertIsNotNone(client.request)
            assert client.request is not None
            self.assertEqual(session.session_id, client.request.session_id)
            self.assertEqual("video-001", client.request.object_id)
            self.assertEqual(session.design_id, client.request.plan_id)
            self.assertEqual(
                "remote_digest_service",
                client.request.binding["location"],
            )

            events = store.list_events(session.session_id)
            self.assertEqual(1, len(events))
            self.assertEqual(0.73, events[0].realized_cost)
            self.assertEqual(
                sha256(b"The red car enters at 00:17.").hexdigest(),
                events[0].content_sha256,
            )
            self.assertEqual("video-001", events[0].object_id)
            self.assertEqual(
                "test-catalog-v1",
                events[0].object_catalog_version,
            )
            self.assertIsNotNone(events[0].data_agent_access_id)

            reconciled = gateway.reconcile_artifact_telemetry(
                session.session_id
            )
            self.assertEqual(2048, reconciled[0].artifact_bytes_sent)
            self.assertEqual(
                12.5,
                reconciled[0].artifact_transfer_latency_ms,
            )
            self.assertEqual(1, reconciled[0].artifact_full_download_count)


if __name__ == "__main__":
    unittest.main()
