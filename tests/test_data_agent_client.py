from __future__ import annotations

import json
import tempfile
import threading
import unittest
from dataclasses import replace
from io import BytesIO
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar
from urllib.error import HTTPError, URLError

from pathfinder.config import load_config
from pathfinder.data_agent_client import (
    DATA_AGENT_API_VERSION,
    DataAgentArtifactIntegrityError,
    DataAgentArtifactRedirectError,
    DataAgentArtifactSecurityError,
    DataAgentArtifactTooLargeError,
    DataAgentArtifactUnsupportedError,
    DataAgentAccessRequest,
    DataAgentAccessResult,
    DataAgentAccessTelemetry,
    DataAgentClientSettings,
    DataAgentPayload,
    DataAgentProtocolError,
    DataAgentTelemetryQuiescenceError,
    DataAgentTelemetryUnsupportedError,
    DataAgentUnavailableError,
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
    TelemetryIncompleteError,
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
    def __init__(
        self,
        *,
        in_flight_request_count: int | None = 0,
        server_reported_complete: bool | None = True,
    ) -> None:
        self.request: DataAgentAccessRequest | None = None
        self.telemetry_quiescence_requests: list[bool] = []
        self.telemetry_timeouts: list[float] = []
        self.in_flight_request_count = in_flight_request_count
        # None on either field models a Data Agent predating the quiescence
        # contract, which must never be accepted as a final observation.
        self.server_reported_complete = server_reported_complete

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
        *,
        wait_for_quiescence: bool = False,
        quiescence_timeout_seconds: float = 5.0,
    ) -> DataAgentAccessTelemetry:
        self.telemetry_quiescence_requests.append(wait_for_quiescence)
        self.telemetry_timeouts.append(quiescence_timeout_seconds)
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
            in_flight_request_count=self.in_flight_request_count,
            server_reported_complete=self.server_reported_complete,
        )


class FakeHTTPResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self, amount: int | None = None) -> bytes:
        return self._body

    def __enter__(self) -> "FakeHTTPResponse":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


def telemetry_frame(
    *,
    in_flight: int,
    bytes_sent: int,
    download_request_count: int,
    completed_request_count: int,
    full_download_count: int,
    transfer_latency_ms: float,
    latest_completed_at: float | None,
    telemetry_complete: bool | None = None,
    omit: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build one artifact_download wire frame.

    ``telemetry_complete`` defaults to the verdict a current Data Agent would
    publish for this counter. ``omit`` drops fields entirely, which is how a
    legacy server that predates the quiescence contract is modelled — dropping
    a key is not the same as sending zero or false.
    """
    frame = {
        "download_request_count": download_request_count,
        "completed_request_count": completed_request_count,
        "full_download_count": full_download_count,
        "bytes_sent": bytes_sent,
        "transfer_latency_ms": transfer_latency_ms,
        "latest_completed_at": latest_completed_at,
        "in_flight_request_count": in_flight,
        "telemetry_complete": (
            in_flight == 0 if telemetry_complete is None
            else telemetry_complete
        ),
    }
    for name in omit:
        del frame[name]
    return frame


PENDING_FRAME = telemetry_frame(
    in_flight=1,
    bytes_sent=0,
    download_request_count=0,
    completed_request_count=0,
    full_download_count=0,
    transfer_latency_ms=0.0,
    latest_completed_at=None,
)

COMMITTED_FRAME = telemetry_frame(
    in_flight=0,
    bytes_sent=2048,
    download_request_count=1,
    completed_request_count=1,
    full_download_count=1,
    transfer_latency_ms=12.5,
    latest_completed_at=123.0,
)

EMPTY_FRAME = telemetry_frame(
    in_flight=0,
    bytes_sent=0,
    download_request_count=0,
    completed_request_count=0,
    full_download_count=0,
    transfer_latency_ms=0.0,
    latest_completed_at=None,
)

# A settled-looking summary from a Data Agent that predates the quiescence
# contract. The figures are identical to COMMITTED_FRAME, which is the point:
# nothing in the payload distinguishes an accurate total from one truncated by
# a transfer still being written, so it must never be accepted as final.
NO_COUNTER_FRAME = telemetry_frame(
    in_flight=0,
    bytes_sent=2048,
    download_request_count=1,
    completed_request_count=1,
    full_download_count=1,
    transfer_latency_ms=12.5,
    latest_completed_at=123.0,
    omit=("in_flight_request_count",),
)

NO_VERDICT_FRAME = telemetry_frame(
    in_flight=0,
    bytes_sent=2048,
    download_request_count=1,
    completed_request_count=1,
    full_download_count=1,
    transfer_latency_ms=12.5,
    latest_completed_at=123.0,
    omit=("telemetry_complete",),
)

FULLY_LEGACY_FRAME = telemetry_frame(
    in_flight=0,
    bytes_sent=2048,
    download_request_count=1,
    completed_request_count=1,
    full_download_count=1,
    transfer_latency_ms=12.5,
    latest_completed_at=123.0,
    omit=("in_flight_request_count", "telemetry_complete"),
)


class ScriptedTelemetryOpener:
    """Replays a fixed sequence of telemetry snapshots.

    The last frame repeats once the script is exhausted, so a "never settles"
    Data Agent is expressed as a single pending frame.
    """

    def __init__(self, frames: list[dict[str, Any]], access_id: str) -> None:
        self.frames = frames
        self.access_id = access_id
        self.call_count = 0

    def __call__(self, request: Any, timeout: float | None = None) -> Any:
        frame = self.frames[min(self.call_count, len(self.frames) - 1)]
        self.call_count += 1
        payload = {
            "api_version": DATA_AGENT_API_VERSION,
            "status": "succeeded",
            "access_id": self.access_id,
            "object_id": "video-001",
            "representation_id": "compressed_video",
            "object_catalog_version": "test-catalog-v1",
            "artifact_download": dict(frame),
        }
        return FakeHTTPResponse(json.dumps(payload).encode("utf-8"))


class UnreachableOpener:
    """Fails every request the way a dead endpoint does."""

    def __init__(self) -> None:
        self.urls: list[str] = []

    def __call__(self, request: Any, timeout: float | None = None) -> Any:
        self.urls.append(request.full_url)
        raise URLError("connection refused")


class ConnectionErrorReportingTest(unittest.TestCase):
    """A connection failure must name the endpoint that actually failed."""

    def build(self) -> tuple[HttpDataAgentClient, UnreachableOpener]:
        opener = UnreachableOpener()
        client = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url="http://127.0.0.1:9",
                timeout_seconds=0.5,
                max_retries=0,
            ),
            opener=opener,
            sleep=lambda _seconds: None,
        )
        return client, opener

    def test_telemetry_failure_names_the_telemetry_url(self) -> None:
        client, opener = self.build()
        with self.assertRaises(DataAgentUnavailableError) as context:
            client.get_access_telemetry("artifact-access")
        message = str(context.exception)
        self.assertIn("/v1/accesses/artifact-access/telemetry", message)
        # The bug this pins: reporting the access endpoint for every failure
        # sends the operator to debug a route that was never contacted.
        self.assertNotIn("/v1/access:", message)
        self.assertEqual([opener.urls[0]], opener.urls)
        self.assertTrue(opener.urls[0].endswith("/telemetry"))

    def test_access_failure_still_names_the_access_url(self) -> None:
        client, _ = self.build()
        with self.assertRaises(DataAgentUnavailableError) as context:
            client.access(
                DataAgentAccessRequest(
                    access_id="a-1",
                    session_id="s-1",
                    trial_id="t-1",
                    plan_id="D_structured_digest",
                    plan_epoch=0,
                    task_class_id="video_qa",
                    representation_id="multimodal_digest",
                    event_index=0,
                    latency_multiplier=1.0,
                    binding={"location": "remote_digest_service"},
                )
            )
        self.assertIn("/v1/access", str(context.exception))
        self.assertNotIn("/telemetry", str(context.exception))


class ArtifactHTTPResponse:
    def __init__(
        self,
        body: bytes,
        media_type: str,
        *,
        declared_length: int | None = None,
    ) -> None:
        self._body = body
        self.status = 200
        self.headers = {
            "Content-Type": media_type,
            "Content-Length": str(
                len(body) if declared_length is None else declared_length
            ),
        }

    def read(self, amount: int | None = None) -> bytes:
        if amount is None:
            return self._body
        return self._body[:amount]

    def __enter__(self) -> "ArtifactHTTPResponse":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


class RecordingArtifactOpener:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.requests: list[Any] = []
        self.timeouts: list[float | None] = []

    def __call__(self, request: Any, timeout: float | None = None) -> Any:
        self.requests.append(request)
        self.timeouts.append(timeout)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class DataAgentArtifactFetchTest(unittest.TestCase):
    ACCESS_ID = "artifact-access"
    BASE_URL = "http://data-agent.test"

    def request(self) -> DataAgentAccessRequest:
        return DataAgentAccessRequest(
            access_id=self.ACCESS_ID,
            session_id="session-1",
            trial_id="trial-1",
            plan_id="D_structured_digest",
            plan_epoch=0,
            task_class_id="video_qa",
            representation_id="sampled_frames",
            event_index=0,
            latency_multiplier=1.0,
            binding={"location": "remote_frame_store"},
            object_id="video-001",
        )

    def build(
        self,
        body: bytes,
        media_type: str,
        *,
        artifact_url: str | None = None,
        artifact_response: Any | None = None,
        max_artifact_bytes: int = 1024,
    ) -> tuple[HttpDataAgentClient, RecordingArtifactOpener]:
        digest = sha256(body).hexdigest()
        signed_url = artifact_url or (
            f"{self.BASE_URL}/v1/artifacts/{self.ACCESS_ID}"
            "?expires=9999999999&signature=do-not-expose"
        )
        access_response = {
            "api_version": DATA_AGENT_API_VERSION,
            "status": "succeeded",
            "access_id": self.ACCESS_ID,
            "object_id": "video-001",
            "object_catalog_version": "test-catalog-v1",
            "payload": {
                "kind": "artifact_uri",
                "media_type": media_type,
                "value": signed_url,
                "sha256": digest,
            },
            "telemetry": {
                "service_latency_ms": 2.0,
                "realized_cost": 0.5,
                "bytes_read": len(body),
                "location": "remote_frame_store",
            },
        }
        artifact_opener = RecordingArtifactOpener(
            artifact_response
            if artifact_response is not None
            else ArtifactHTTPResponse(body, media_type)
        )
        client = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url=self.BASE_URL,
                token="test-token",
                timeout_seconds=2.0,
                max_retries=0,
                max_artifact_bytes=max_artifact_bytes,
            ),
            opener=lambda request, timeout=None: FakeHTTPResponse(
                json.dumps(access_response).encode("utf-8")
            ),
            artifact_opener=artifact_opener,
            sleep=lambda _seconds: None,
        )
        return client, artifact_opener

    def test_fetches_bounded_json_without_returning_the_signed_url(self) -> None:
        body = json.dumps({"frame": 17, "event": "red car"}).encode()
        client, opener = self.build(body, "application/json; charset=utf-8")

        result = client.fetch_artifact(self.request())

        self.assertEqual({"frame": 17, "event": "red car"}, result.content)
        self.assertEqual("application/json", result.media_type)
        self.assertEqual(len(body), result.size_bytes)
        self.assertEqual(sha256(body).hexdigest(), result.sha256)
        self.assertFalse(hasattr(result, "url"))
        self.assertEqual([2.0], opener.timeouts)
        sent_headers = {
            name.lower(): value
            for name, value in opener.requests[0].header_items()
        }
        self.assertEqual("Bearer test-token", sent_headers["authorization"])

    def test_fetches_utf8_text(self) -> None:
        body = "frame 17: red car enters tunnel".encode()
        client, _ = self.build(body, "text/plain")
        result = client.fetch_artifact(self.request())
        self.assertEqual(body.decode(), result.content)

    def test_rejects_an_artifact_url_outside_the_configured_origin(self) -> None:
        client, opener = self.build(
            b"{}",
            "application/json",
            artifact_url=(
                "https://attacker.invalid/v1/artifacts/artifact-access"
                "?signature=secret"
            ),
        )
        with self.assertRaises(DataAgentArtifactSecurityError) as context:
            client.fetch_artifact(self.request())
        self.assertEqual([], opener.requests)
        self.assertNotIn("signature", str(context.exception))

    def test_rejects_redirects_without_exposing_the_signed_url(self) -> None:
        signed_url = (
            f"{self.BASE_URL}/v1/artifacts/{self.ACCESS_ID}"
            "?signature=secret-value"
        )
        redirect = HTTPError(
            signed_url,
            302,
            "Found",
            {"Location": "https://attacker.invalid/steal"},
            BytesIO(b""),
        )
        client, _ = self.build(
            b"{}",
            "application/json",
            artifact_url=signed_url,
            artifact_response=redirect,
        )
        with self.assertRaises(DataAgentArtifactRedirectError) as context:
            client.fetch_artifact(self.request())
        self.assertNotIn("secret-value", str(context.exception))
        self.assertNotIn("attacker.invalid", str(context.exception))
        self.assertIsNone(context.exception.__cause__)

    def test_rejects_declared_and_streamed_oversized_artifacts(self) -> None:
        body = b'{"large":true}'
        cases = {
            "declared": ArtifactHTTPResponse(
                body,
                "application/json",
                declared_length=10_000,
            ),
            "streamed": ArtifactHTTPResponse(
                b"x" * 17,
                "application/json",
                declared_length=16,
            ),
        }
        for label, response in cases.items():
            with self.subTest(label):
                client, _ = self.build(
                    body,
                    "application/json",
                    artifact_response=response,
                    max_artifact_bytes=16,
                )
                with self.assertRaises(DataAgentArtifactTooLargeError):
                    client.fetch_artifact(self.request())

    def test_rejects_unsupported_binary_content(self) -> None:
        client, _ = self.build(b"binary", "application/octet-stream")
        with self.assertRaises(DataAgentArtifactUnsupportedError) as context:
            client.fetch_artifact(self.request())
        self.assertIn("application/octet-stream", str(context.exception))

    def test_rejects_a_checksum_mismatch(self) -> None:
        expected = b'{"expected":true}'
        client, _ = self.build(
            expected,
            "application/json",
            artifact_response=ArtifactHTTPResponse(
                b'{"different":true}',
                "application/json",
            ),
        )
        with self.assertRaises(DataAgentArtifactIntegrityError):
            client.fetch_artifact(self.request())


class TelemetryQuiescenceTest(unittest.TestCase):
    """The fail-closed contract for post-workflow telemetry reconciliation."""

    ACCESS_ID = "artifact-access"

    def build(
        self,
        frames: list[dict[str, Any]],
    ) -> tuple[HttpDataAgentClient, ScriptedTelemetryOpener, list[float]]:
        opener = ScriptedTelemetryOpener(frames, self.ACCESS_ID)
        slept: list[float] = []
        client = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url="http://127.0.0.1:9",
                timeout_seconds=2.0,
                max_retries=0,
            ),
            opener=opener,
            sleep=slept.append,
        )
        return client, opener, slept

    def test_wait_blocks_until_the_transfer_record_is_committed(self) -> None:
        client, opener, slept = self.build(
            [PENDING_FRAME, PENDING_FRAME, COMMITTED_FRAME]
        )
        result = client.get_access_telemetry(
            self.ACCESS_ID,
            wait_for_quiescence=True,
            quiescence_timeout_seconds=5.0,
        )
        self.assertTrue(result.telemetry_complete)
        self.assertEqual(0, result.in_flight_request_count)
        # The committed figures, not the zeros visible while in flight.
        self.assertEqual(2048, result.bytes_sent)
        self.assertEqual(1, result.completed_request_count)
        self.assertEqual(12.5, result.transfer_latency_ms)
        self.assertEqual(3, opener.call_count)
        self.assertEqual(2, len(slept))

    def test_wait_times_out_while_a_transfer_is_in_flight(self) -> None:
        client, opener, _ = self.build([PENDING_FRAME])
        with self.assertRaises(DataAgentTelemetryQuiescenceError) as context:
            client.get_access_telemetry(
                self.ACCESS_ID,
                wait_for_quiescence=True,
                quiescence_timeout_seconds=0.0,
            )
        error = context.exception
        self.assertEqual(self.ACCESS_ID, error.access_id)
        self.assertEqual(1, error.in_flight_request_count)
        self.assertEqual(0.0, error.timeout_seconds)
        # The provisional summary is attached for diagnostics only, and it
        # still reports itself as incomplete.
        self.assertFalse(error.telemetry.telemetry_complete)
        self.assertEqual(0, error.telemetry.bytes_sent)
        self.assertEqual(1, opener.call_count)

    def test_absent_counter_is_not_treated_as_zero(self) -> None:
        client, opener, slept = self.build([NO_COUNTER_FRAME])
        result = client.get_access_telemetry(self.ACCESS_ID)
        # Parsing still succeeds, but absence stays absence.
        self.assertIsNone(result.in_flight_request_count)
        self.assertEqual(2048, result.bytes_sent)
        self.assertFalse(result.telemetry_supported)
        self.assertFalse(result.telemetry_complete)
        self.assertEqual(
            ("in_flight_request_count",),
            result.missing_completeness_fields,
        )
        self.assertEqual([], slept)

    def test_absent_verdict_is_not_inferred_from_the_counter(self) -> None:
        client, _, _ = self.build([NO_VERDICT_FRAME])
        result = client.get_access_telemetry(self.ACCESS_ID)
        # The counter says zero and the figures look settled, but the server
        # never claimed the snapshot was stable, so neither may the client.
        self.assertEqual(0, result.in_flight_request_count)
        self.assertIsNone(result.server_reported_complete)
        self.assertFalse(result.telemetry_supported)
        self.assertFalse(result.telemetry_complete)
        self.assertEqual(
            ("telemetry_complete",),
            result.missing_completeness_fields,
        )

    def test_wait_rejects_a_server_that_omits_the_counter(self) -> None:
        client, opener, slept = self.build([NO_COUNTER_FRAME])
        with self.assertRaises(DataAgentTelemetryUnsupportedError) as context:
            client.get_access_telemetry(
                self.ACCESS_ID,
                wait_for_quiescence=True,
                quiescence_timeout_seconds=5.0,
            )
        error = context.exception
        self.assertEqual(self.ACCESS_ID, error.access_id)
        self.assertEqual(("in_flight_request_count",), error.missing_fields)
        # Polling cannot make a missing field appear, so the client must fail
        # on the first response rather than burn the whole timeout.
        self.assertEqual(1, opener.call_count)
        self.assertEqual([], slept)

    def test_wait_rejects_a_server_that_omits_the_verdict(self) -> None:
        client, opener, _ = self.build([NO_VERDICT_FRAME])
        with self.assertRaises(DataAgentTelemetryUnsupportedError) as context:
            client.get_access_telemetry(
                self.ACCESS_ID,
                wait_for_quiescence=True,
                quiescence_timeout_seconds=5.0,
            )
        self.assertEqual(("telemetry_complete",), context.exception.missing_fields)
        self.assertEqual(1, opener.call_count)

    def test_wait_rejects_a_fully_legacy_data_agent(self) -> None:
        client, opener, _ = self.build([FULLY_LEGACY_FRAME])
        with self.assertRaises(DataAgentTelemetryUnsupportedError) as context:
            client.get_access_telemetry(
                self.ACCESS_ID,
                wait_for_quiescence=True,
                quiescence_timeout_seconds=5.0,
            )
        self.assertEqual(
            ("in_flight_request_count", "telemetry_complete"),
            context.exception.missing_fields,
        )
        # An unsupported server is a protocol-level condition, so it stays
        # catchable as one.
        self.assertIsInstance(context.exception, DataAgentProtocolError)
        self.assertEqual(1, opener.call_count)

    def test_generation_race_error_does_not_claim_nothing_happened(
        self,
    ) -> None:
        """A zero counter with a false verdict must not read as 'all clear'."""
        racing = telemetry_frame(
            in_flight=0,
            bytes_sent=2048,
            download_request_count=1,
            completed_request_count=1,
            full_download_count=1,
            transfer_latency_ms=12.5,
            latest_completed_at=123.0,
            telemetry_complete=False,
        )
        client, _, _ = self.build([racing])
        with self.assertRaises(DataAgentTelemetryQuiescenceError) as context:
            client.get_access_telemetry(
                self.ACCESS_ID,
                wait_for_quiescence=True,
                quiescence_timeout_seconds=0.0,
            )
        message = str(context.exception)
        self.assertIn("transfer activity changed", message)
        self.assertIn("not a stable point in time", message)
        self.assertEqual(0, context.exception.in_flight_request_count)

    def test_stale_summary_never_reports_itself_as_complete(self) -> None:
        client, opener, slept = self.build([PENDING_FRAME])
        result = client.get_access_telemetry(self.ACCESS_ID)
        # A caller that does not ask to wait still gets an answer, but it is
        # explicitly marked provisional so it cannot pass as an observation.
        self.assertFalse(result.telemetry_complete)
        self.assertEqual(1, result.in_flight_request_count)
        self.assertEqual(0, result.bytes_sent)
        self.assertEqual(1, opener.call_count)
        self.assertEqual([], slept)

    def test_zero_downloads_with_no_in_flight_requests_is_complete(
        self,
    ) -> None:
        client, opener, slept = self.build([EMPTY_FRAME])
        result = client.get_access_telemetry(
            self.ACCESS_ID,
            wait_for_quiescence=True,
            quiescence_timeout_seconds=5.0,
        )
        self.assertTrue(result.telemetry_complete)
        self.assertEqual(0, result.download_request_count)
        self.assertEqual(0, result.completed_request_count)
        self.assertEqual(0, result.bytes_sent)
        self.assertEqual(0.0, result.transfer_latency_ms)
        self.assertIsNone(result.latest_completed_at)
        # An inline access that never produced a transfer settles on the
        # first read; it must not burn the whole timeout.
        self.assertEqual(1, opener.call_count)
        self.assertEqual([], slept)


class TelemetryTimingArgumentTest(unittest.TestCase):
    """Timing arguments are rejected at the boundary, not after they hang.

    ``nan`` is the dangerous one: every comparison against it is false, so
    ``time.monotonic() >= deadline`` never fires and the fail-closed wait
    silently becomes an infinite loop. ``inf`` does the same thing openly.
    Neither raises on its own, so nothing catches them but an explicit check.
    """

    ACCESS_ID = "artifact-access"

    #: Values that must never reach a deadline computation. Booleans are in
    #: the list because ``isinstance(True, int)`` is true.
    BAD_VALUES = {
        "nan": float("nan"),
        "positive infinity": float("inf"),
        "negative infinity": float("-inf"),
        "True": True,
        "False": False,
        "negative": -1.0,
    }

    def build(self) -> tuple[HttpDataAgentClient, ScriptedTelemetryOpener]:
        opener = ScriptedTelemetryOpener(
            [COMMITTED_FRAME] * 4,
            self.ACCESS_ID,
        )
        client = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url="http://127.0.0.1:9",
                timeout_seconds=2.0,
                max_retries=0,
            ),
            opener=opener,
            sleep=lambda _seconds: None,
        )
        return client, opener

    def test_timeout_rejects_non_finite_and_negative_values(self) -> None:
        for label, value in self.BAD_VALUES.items():
            with self.subTest(timeout=label):
                client, opener = self.build()
                with self.assertRaises(ValueError) as context:
                    client.get_access_telemetry(
                        self.ACCESS_ID,
                        wait_for_quiescence=True,
                        quiescence_timeout_seconds=value,
                    )
                self.assertIn(
                    "quiescence_timeout_seconds",
                    str(context.exception),
                )
                # Rejected before the wire, so a bad argument cannot leave a
                # half-issued request behind.
                self.assertEqual(0, opener.call_count)

    def test_poll_interval_rejects_non_finite_negative_and_zero(self) -> None:
        # Zero is additionally invalid here: a zero-interval poll would spin
        # against the Data Agent without ever yielding.
        for label, value in {**self.BAD_VALUES, "zero": 0.0}.items():
            with self.subTest(poll=label):
                client, opener = self.build()
                with self.assertRaises(ValueError) as context:
                    client.get_access_telemetry(
                        self.ACCESS_ID,
                        wait_for_quiescence=True,
                        quiescence_poll_seconds=value,
                    )
                self.assertIn(
                    "quiescence_poll_seconds",
                    str(context.exception),
                )
                self.assertEqual(0, opener.call_count)

    def test_arguments_are_checked_even_without_waiting(self) -> None:
        """The caller that passed the bad value is the one that hears it."""
        client, opener = self.build()
        with self.assertRaises(ValueError):
            client.get_access_telemetry(
                self.ACCESS_ID,
                quiescence_timeout_seconds=float("nan"),
            )
        self.assertEqual(0, opener.call_count)

    def test_zero_timeout_is_valid_and_allows_one_attempt(self) -> None:
        """A zero deadline is a legitimate 'check once, do not wait'."""
        client, opener = self.build()
        result = client.get_access_telemetry(
            self.ACCESS_ID,
            wait_for_quiescence=True,
            quiescence_timeout_seconds=0.0,
        )
        self.assertTrue(result.telemetry_complete)
        self.assertEqual(1, opener.call_count)

    def test_backend_rejects_non_finite_and_negative_timeouts(self) -> None:
        for label, value in self.BAD_VALUES.items():
            with self.subTest(timeout=label):
                with self.assertRaises(ValueError) as context:
                    RemoteDataAgentBackend(
                        FakeDataAgentClient(),
                        telemetry_quiescence_timeout_seconds=value,
                    )
                self.assertIn(
                    "telemetry_quiescence_timeout_seconds",
                    str(context.exception),
                )

    def test_backend_accepts_a_zero_timeout(self) -> None:
        backend = RemoteDataAgentBackend(
            FakeDataAgentClient(),
            telemetry_quiescence_timeout_seconds=0,
        )
        self.assertEqual(0.0, backend.telemetry_quiescence_timeout_seconds)


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
            # See test_data_agent_server: the default 0.5s poll interval
            # makes each teardown wait up to half a second.
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        self.thread.start()
        self.addCleanup(self._stop_server)

    def _stop_server(self) -> None:
        try:
            self.server.shutdown()
        finally:
            self.server.server_close()
            self.thread.join(timeout=5)
        self.assertFalse(
            self.thread.is_alive(),
            "recording Data Agent server thread outlived its test",
        )

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
    def test_gateway_rejects_unhandled_payload_kinds_before_tool_output(
        self,
    ) -> None:
        class UnsafePayloadClient(FakeDataAgentClient):
            def access(
                self,
                request: DataAgentAccessRequest,
            ) -> DataAgentAccessResult:
                result = super().access(request)
                return replace(
                    result,
                    payload=DataAgentPayload(
                        kind="frame_uris",
                        media_type="application/json",
                        value={"url": "https://should-not-be-exposed.invalid"},
                    ),
                )

        config = load_config(CONFIG_PATH)
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteSessionStore(Path(directory) / "gateway.sqlite3")
            gateway = AccessGateway(
                config,
                store,
                RemoteDataAgentBackend(UnsafePayloadClient()),
            )
            session = gateway.register_session(
                FlowMeshAgentRunRequest(
                    question="When does the red car enter?",
                    design_id="D_structured_digest",
                    task_class_id="video_qa",
                    quote_profile_id="digest_low",
                    trial_id="unsafe-payload",
                    object_id="video-001",
                )
            )
            with self.assertRaises(DataAgentProtocolError):
                gateway.access_representation(
                    session.session_id,
                    "multimodal_digest",
                )
            self.assertEqual([], store.list_events(session.session_id))

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
            self.assertEqual(125.0, events[0].felt_latency_ms)
            self.assertEqual(80.0, events[0].data_agent_service_latency_ms)
            self.assertEqual(5.0, events[0].data_agent_fetch_latency_ms)
            self.assertIsNone(
                events[0].data_agent_controlled_delay_ms
            )
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

    def test_initial_access_does_not_wait_for_transfer_quiescence(
        self,
    ) -> None:
        """access() must not block: the Agent may not have downloaded yet."""
        config = load_config(CONFIG_PATH)
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteSessionStore(Path(directory) / "gateway.sqlite3")
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
                    trial_id="no-wait-on-access",
                    object_id="video-001",
                )
            )
            gateway.access_representation(
                session.session_id,
                "multimodal_digest",
            )
            self.assertEqual([], client.telemetry_quiescence_requests)

            gateway.reconcile_artifact_telemetry(session.session_id)
            # Reconciliation is the only place that waits.
            self.assertEqual([True], client.telemetry_quiescence_requests)
            self.assertEqual([5.0], client.telemetry_timeouts)

    def test_gateway_refuses_stale_telemetry_as_an_observation(self) -> None:
        """A summary with transfers still in flight is never persisted."""
        config = load_config(CONFIG_PATH)
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteSessionStore(Path(directory) / "gateway.sqlite3")
            client = FakeDataAgentClient(in_flight_request_count=2)
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
                    trial_id="stale-telemetry",
                    object_id="video-001",
                )
            )
            gateway.access_representation(
                session.session_id,
                "multimodal_digest",
            )

            with self.assertRaises(TelemetryIncompleteError) as context:
                gateway.reconcile_artifact_telemetry(session.session_id)
            self.assertEqual(2, context.exception.in_flight_request_count)

            # Nothing was written: the event keeps its unreconciled zeros
            # rather than the stale byte and latency figures.
            events = store.list_events(session.session_id)
            self.assertEqual(0, events[0].artifact_bytes_sent)
            self.assertEqual(0.0, events[0].artifact_transfer_latency_ms)
            self.assertEqual(0, events[0].artifact_download_request_count)
            self.assertEqual(0, events[0].artifact_full_download_count)

    def test_gateway_refuses_telemetry_that_cannot_prove_completeness(
        self,
    ) -> None:
        """A legacy summary is rejected by the gateway on its own account.

        The client already fails closed on these responses, but the gateway is
        what writes the research observation, so it must not depend on some
        other backend having enforced the contract first.
        """
        cases = {
            "no counter": FakeDataAgentClient(
                in_flight_request_count=None,
            ),
            "no verdict": FakeDataAgentClient(
                server_reported_complete=None,
            ),
            "neither": FakeDataAgentClient(
                in_flight_request_count=None,
                server_reported_complete=None,
            ),
        }
        config = load_config(CONFIG_PATH)
        for index, (label, client) in enumerate(cases.items()):
            with self.subTest(label), tempfile.TemporaryDirectory() as directory:
                store = SQLiteSessionStore(
                    Path(directory) / "gateway.sqlite3"
                )
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
                        trial_id=f"legacy-telemetry-{index}",
                        object_id="video-001",
                    )
                )
                gateway.access_representation(
                    session.session_id,
                    "multimodal_digest",
                )

                with self.assertRaises(TelemetryIncompleteError) as context:
                    gateway.reconcile_artifact_telemetry(session.session_id)
                self.assertTrue(context.exception.missing_fields)
                self.assertIn(
                    "cannot report completeness",
                    str(context.exception),
                )

                # The bytes and latency the legacy server offered look
                # perfectly settled, and none of them reached the store.
                events = store.list_events(session.session_id)
                self.assertEqual(0, events[0].artifact_bytes_sent)
                self.assertEqual(
                    0.0,
                    events[0].artifact_transfer_latency_ms,
                )
                self.assertEqual(
                    0,
                    events[0].artifact_download_request_count,
                )
                self.assertEqual(0, events[0].artifact_full_download_count)

    def test_explicit_zero_and_true_remains_a_valid_empty_result(
        self,
    ) -> None:
        """Fail-closed must not break the legitimate zero-download case."""
        config = load_config(CONFIG_PATH)
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteSessionStore(Path(directory) / "gateway.sqlite3")
            client = FakeDataAgentClient(
                in_flight_request_count=0,
                server_reported_complete=True,
            )
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
                    trial_id="explicit-zero",
                    object_id="video-001",
                )
            )
            gateway.access_representation(
                session.session_id,
                "multimodal_digest",
            )

            reconciled = gateway.reconcile_artifact_telemetry(
                session.session_id
            )
            self.assertEqual(2048, reconciled[0].artifact_bytes_sent)
            self.assertEqual(
                12.5,
                reconciled[0].artifact_transfer_latency_ms,
            )


if __name__ == "__main__":
    unittest.main()
