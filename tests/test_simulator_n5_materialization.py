"""Offline tests for portable deterministic N5 materialization."""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import threading
import unittest
import urllib.error
import urllib.request
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from pathfinder.frame_bundle import FRAME_BUNDLE_SCHEMA_VERSION
from pathfinder.frame_bundle_ingest import (
    FRAME_BUNDLE_MEDIA_TYPE,
    validate_frame_bundle_bytes,
)
from pathfinder.video_prep import FRAME_SCHEMA_VERSION, SampledImage
from pathfinder.video_prep import PREP_SCHEMA_VERSION
from pathfinder.simulator.n5_materialization import (
    N4_ATOMIC_PUBLICATION_REQUIREMENTS,
    N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION,
    N5_MATERIALIZATION_HTTP_EXECUTE_SCHEMA_VERSION,
    N5_MATERIALIZATION_PLAN_SCHEMA_VERSION,
    HttpN5MaterializationClient,
    N5MaterializationConflict,
    N5MaterializationError,
    N5MaterializationHttpClientConfig,
    N5MaterializationHttpError,
    N5MaterializationHttpServer,
    N5MaterializationHttpService,
    N5MaterializationRuntime,
    freeze_n5_materialization_plan,
    verify_n5_materialization_plan,
)


OBJECT_ID = "nextqa-val-0000000001"
VIDEO_ID = "0000000001"
VIDEO_NAME = f"{VIDEO_ID}.mp4"
SOURCE = b"\x00\x00\x00\x18ftypmp42deterministic-test-video"
VERSIONS = {
    "Pillow": "12.3.0-test",
    "av": "17.0.1-test",
    "pathfinder-minimal": "0.1-test",
}
TOKEN = "n5-test-token-never-persist"

_TEST_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBAUEBAYFBQUGBgYHCQ4JCQgICRINDQoOFRIW"
    "FhUSFBQXGiEcFxgfGRQUHScdHyIjJSUlFhwpLCgkKyEkJST/2wBDAQYGBgkICREJCREkGBQY"
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/wAAR"
    "CAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAAAAAA"
    "AAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oA"
    "DAMBAAIRAxEAPwCdAAyqX//Z"
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _jpeg(marker: bytes) -> bytes:
    payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
    comment = b"\xff\xfe" + (len(marker) + 2).to_bytes(2, "big") + marker
    return payload[:2] + comment + payload[2:]


class DeterministicSampler:
    def __init__(
        self,
        *,
        marker: bytes = b"n5",
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.marker = marker
        self.entered = entered
        self.release = release
        self.calls = 0
        self.paths: list[Path] = []

    def __call__(
        self,
        path: Path,
        *,
        frame_count: int,
        jpeg_max_dimension: int,
    ) -> tuple[list[SampledImage], float]:
        self.calls += 1
        self.paths.append(path)
        if path.read_bytes() != SOURCE:
            raise RuntimeError("source bytes changed")
        if jpeg_max_dimension != 768:
            raise RuntimeError("dimension changed")
        if self.entered is not None:
            self.entered.set()
        if self.release is not None and not self.release.wait(5):
            raise RuntimeError("test sampler release timed out")
        return ([
            SampledImage(
                frame_index=index,
                timestamp_seconds=0.5 + index * 1.25,
                width=2,
                height=2,
                jpeg_bytes=_jpeg(self.marker + bytes([index])),
            )
            for index in range(frame_count)
        ], 42.5)


def _description_bytes(*, second_width: int = 2) -> bytes:
    value = {
        "schema_version": FRAME_SCHEMA_VERSION,
        "object_id": OBJECT_ID,
        "source_video_id": VIDEO_ID,
        "source_video_sha256": _sha256(SOURCE),
        "source_duration_seconds": 42.5,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": 2,
            "jpeg_max_dimension": 768,
        },
        "generator": {
            "model": "historical-vision-model",
            "temperature": 0,
            "prompt_sha256": "a" * 64,
        },
        "frames": [
            {
                "frame_index": 0,
                "timestamp_seconds": 0.5,
                "width": 2,
                "height": 2,
                "description": "First frozen observation.",
                "visible_text": None,
            },
            {
                "frame_index": 1,
                "timestamp_seconds": 1.75,
                "width": second_width,
                "height": 2,
                "description": "Second frozen observation.",
                "visible_text": None,
            },
        ],
    }
    return (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _generation_manifest_bytes(description: bytes | None = None) -> bytes:
    frame_description = description or _description_bytes()
    value = {
        "schema_version": PREP_SCHEMA_VERSION,
        "frame_count": 2,
        "jpeg_max_dimension": 768,
        "credentials_recorded": False,
        "objects": [
            {
                "object_id": OBJECT_ID,
                "source_video": {
                    "filename": VIDEO_NAME,
                    "size_bytes": len(SOURCE),
                    "sha256": _sha256(SOURCE),
                },
                "representations": {
                    "sampled_frames": {
                        "path": f"{OBJECT_ID}/sampled_frames.json",
                        "size_bytes": len(frame_description),
                        "sha256": _sha256(frame_description),
                    }
                },
            }
        ],
    }
    return (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


class _RunningServer:
    def __init__(self, server: ThreadingHTTPServer) -> None:
        self.server = server
        self.thread = threading.Thread(
            target=server.serve_forever,
            daemon=True,
        )
        self.closed = False
        self.thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


class _RedirectHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    target_hits = 0

    def log_message(self, format: str, *args: Any) -> None:
        del format, args

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self.send_response(302)
            self.send_header("Location", "/redirect-target")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        type(self).target_hits += 1
        payload = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class _CorruptResultHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    expected_handle = ""
    payload = b""

    def log_message(self, format: str, *args: Any) -> None:
        del format, args

    def do_GET(self) -> None:  # noqa: N802
        if self.path != (
            "/v1/materialization-results/" + type(self).expected_handle
        ):
            self.send_error(404)
            return
        payload = type(self).payload + b"corruption"
        self.send_response(200)
        self.send_header("Content-Type", FRAME_BUNDLE_MEDIA_TYPE)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header(
            "X-Pathfinder-Content-SHA256",
            type(self).expected_handle,
        )
        self.end_headers()
        self.wfile.write(payload)


class N5MaterializationTest(unittest.TestCase):
    def freeze(
        self,
        *,
        sampler: Any | None = None,
        idempotency_key: str = "n5-materialize-object-0001-v1",
    ) -> Any:
        return freeze_n5_materialization_plan(
            plan_id="n5-materialization-visible-v1",
            idempotency_key=idempotency_key,
            object_id=OBJECT_ID,
            source_video_id=VIDEO_ID,
            source_video_filename=VIDEO_NAME,
            source_video_bytes=SOURCE,
            source_frame_descriptions_path=(
                f"{OBJECT_ID}/sampled_frames.json"
            ),
            source_frame_descriptions_bytes=_description_bytes(),
            generation_manifest_bytes=_generation_manifest_bytes(),
            frame_count=2,
            jpeg_max_dimension=768,
            sampler=sampler or DeterministicSampler(),
            software_versions=VERSIONS,
        )

    def state_dir(self) -> Path:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name)

    def serve(
        self,
        *,
        sampler: DeterministicSampler | None = None,
        state_dir: Path | None = None,
    ) -> tuple[
        N5MaterializationHttpService,
        HttpN5MaterializationClient,
        _RunningServer,
    ]:
        runtime = N5MaterializationRuntime(
            sampler=sampler or DeterministicSampler(),
            software_versions=VERSIONS,
        )
        service = N5MaterializationHttpService(
            runtime=runtime,
            bearer_token=TOKEN,
            state_dir=state_dir or self.state_dir(),
        )
        running = _RunningServer(
            N5MaterializationHttpServer(("127.0.0.1", 0), service)
        )
        self.addCleanup(running.close)
        client = HttpN5MaterializationClient(
            N5MaterializationHttpClientConfig(
                base_url=running.base_url,
                bearer_token=TOKEN,
                timeout_seconds=5,
            )
        )
        return service, client, running

    def test_freeze_produces_endpoint_free_content_bound_canonical_plan(self) -> None:
        frozen = self.freeze()
        plan = frozen.plan
        self.assertEqual(
            N5_MATERIALIZATION_PLAN_SCHEMA_VERSION,
            plan["schema_version"],
        )
        self.assertEqual("N3", plan["route"]["source_node_id"])
        self.assertEqual("N5", plan["route"]["materializer_node_id"])
        self.assertEqual(_sha256(SOURCE), plan["input"]["sha256"])
        self.assertEqual(len(SOURCE), plan["input"]["size_bytes"])
        self.assertEqual(
            _sha256(frozen.artifact_bytes),
            plan["expected_output"]["artifact_sha256"],
        )
        self.assertTrue(plan["endpoint_free"])
        self.assertFalse(plan["llm_required"])
        raw = json.dumps(plan, sort_keys=True).encode("utf-8")
        self.assertNotIn(b"://", raw)
        self.assertNotIn(b"historical-vision-model", raw)

        bundle = validate_frame_bundle_bytes(
            frozen.artifact_bytes,
            expected_object_id=OBJECT_ID,
            expected_sha256=plan["expected_output"]["artifact_sha256"],
            expected_size_bytes=plan["expected_output"][
                "artifact_size_bytes"
            ],
            artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
        )
        self.assertEqual(FRAME_BUNDLE_SCHEMA_VERSION, bundle.schema_version)
        self.assertEqual(2, bundle.frame_count)

    def test_runtime_reproduces_exact_artifact_and_n5_evidence(self) -> None:
        frozen = self.freeze()
        sampler = DeterministicSampler()
        runtime = N5MaterializationRuntime(
            sampler=sampler,
            software_versions=VERSIONS,
        )
        execution = runtime.execute(frozen.plan, SOURCE)
        self.assertEqual(frozen.artifact_bytes, execution.artifact_bytes)
        self.assertEqual(
            N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION,
            execution.evidence["schema_version"],
        )
        self.assertEqual("COMPLETE", execution.evidence["status"])
        self.assertTrue(execution.evidence["input_content_binding_verified"])
        self.assertTrue(execution.evidence["canonical_frame_bundle_verified"])
        self.assertTrue(
            execution.evidence["logical_n5_execution_binding_verified"]
        )
        self.assertFalse(execution.evidence["physical_host_identity_verified"])
        self.assertFalse(
            execution.evidence["source_delivery_telemetry_verified"]
        )
        self.assertFalse(execution.evidence["llm_called"])
        self.assertFalse(execution.evidence["semantic_digest_generated"])
        self.assertEqual(1, sampler.calls)

    def test_independent_freezes_are_byte_deterministic(self) -> None:
        first = self.freeze()
        second = self.freeze()
        self.assertEqual(first.plan, second.plan)
        self.assertEqual(first.artifact_bytes, second.artifact_bytes)

    def test_identical_execution_replays_without_resampling(self) -> None:
        frozen = self.freeze()
        sampler = DeterministicSampler()
        runtime = N5MaterializationRuntime(
            sampler=sampler,
            software_versions=VERSIONS,
        )
        first = runtime.execute(frozen.plan, SOURCE)
        second = runtime.execute(frozen.plan, SOURCE)
        self.assertFalse(first.evidence["idempotent_replay"])
        self.assertTrue(second.evidence["idempotent_replay"])
        self.assertEqual(first.artifact_bytes, second.artifact_bytes)
        self.assertEqual(1, sampler.calls)

    def test_concurrent_identical_execution_waits_and_runs_once(self) -> None:
        frozen = self.freeze()
        entered = threading.Event()
        release = threading.Event()
        sampler = DeterministicSampler(entered=entered, release=release)
        runtime = N5MaterializationRuntime(
            sampler=sampler,
            software_versions=VERSIONS,
        )
        results: list[Any] = []
        failures: list[BaseException] = []

        def invoke() -> None:
            try:
                results.append(runtime.execute(frozen.plan, SOURCE))
            except BaseException as exc:  # pragma: no cover - assertion aid
                failures.append(exc)

        first = threading.Thread(target=invoke)
        second = threading.Thread(target=invoke)
        first.start()
        self.assertTrue(entered.wait(2))
        second.start()
        self.assertTrue(second.is_alive())
        release.set()
        first.join(5)
        second.join(5)
        self.assertEqual([], failures)
        self.assertEqual(2, len(results))
        self.assertEqual(1, sampler.calls)
        self.assertEqual(
            [False, True],
            sorted(result.evidence["idempotent_replay"] for result in results),
        )

    def test_content_mismatch_is_rejected_before_sampling(self) -> None:
        frozen = self.freeze()
        sampler = DeterministicSampler()
        runtime = N5MaterializationRuntime(
            sampler=sampler,
            software_versions=VERSIONS,
        )
        changed = SOURCE[:-1] + b"x"
        with self.assertRaisesRegex(N5MaterializationError, "input binding"):
            runtime.execute(frozen.plan, changed)
        self.assertEqual(0, sampler.calls)

    def test_output_drift_is_rejected(self) -> None:
        frozen = self.freeze()
        runtime = N5MaterializationRuntime(
            sampler=DeterministicSampler(marker=b"drift"),
            software_versions=VERSIONS,
        )
        with self.assertRaisesRegex(N5MaterializationError, "output differs"):
            runtime.execute(frozen.plan, SOURCE)

    def test_alignment_drift_is_rejected_during_freeze(self) -> None:
        with self.assertRaisesRegex(N5MaterializationError, "alignment"):
            freeze_n5_materialization_plan(
                plan_id="n5-materialization-visible-v1",
                idempotency_key="n5-materialize-object-0001-v1",
                object_id=OBJECT_ID,
                source_video_id=VIDEO_ID,
                source_video_filename=VIDEO_NAME,
                source_video_bytes=SOURCE,
                source_frame_descriptions_path=(
                    f"{OBJECT_ID}/sampled_frames.json"
                ),
                source_frame_descriptions_bytes=_description_bytes(
                    second_width=3
                ),
                generation_manifest_bytes=_generation_manifest_bytes(
                    _description_bytes(second_width=3)
                ),
                frame_count=2,
                jpeg_max_dimension=768,
                sampler=DeterministicSampler(),
                software_versions=VERSIONS,
            )

    def test_generation_manifest_binding_is_verified(self) -> None:
        manifest = json.loads(_generation_manifest_bytes())
        manifest["objects"][0]["source_video"]["sha256"] = "0" * 64
        changed = (
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        with self.assertRaisesRegex(
            N5MaterializationError,
            "source-video binding",
        ):
            freeze_n5_materialization_plan(
                plan_id="n5-materialization-visible-v1",
                idempotency_key="n5-materialize-object-0001-v1",
                object_id=OBJECT_ID,
                source_video_id=VIDEO_ID,
                source_video_filename=VIDEO_NAME,
                source_video_bytes=SOURCE,
                source_frame_descriptions_path=(
                    f"{OBJECT_ID}/sampled_frames.json"
                ),
                source_frame_descriptions_bytes=_description_bytes(),
                generation_manifest_bytes=changed,
                frame_count=2,
                jpeg_max_dimension=768,
                sampler=DeterministicSampler(),
                software_versions=VERSIONS,
            )

    def test_plan_tampering_and_software_drift_fail_closed(self) -> None:
        frozen = self.freeze()
        changed = json.loads(json.dumps(frozen.plan))
        changed["expected_output"]["artifact_size_bytes"] += 1
        with self.assertRaisesRegex(N5MaterializationError, "plan digest"):
            verify_n5_materialization_plan(changed)

        runtime = N5MaterializationRuntime(
            sampler=DeterministicSampler(),
            software_versions={**VERSIONS, "av": "different"},
        )
        with self.assertRaisesRegex(N5MaterializationError, "software"):
            runtime.execute(frozen.plan, SOURCE)

    def test_same_idempotency_key_with_different_plan_conflicts(self) -> None:
        first = self.freeze()
        second = self.freeze()
        changed = json.loads(json.dumps(second.plan))
        changed["plan_id"] = "n5-materialization-visible-v2"
        unsigned = dict(changed)
        unsigned.pop("plan_sha256")
        changed["plan_sha256"] = _sha256(
            json.dumps(
                unsigned,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        runtime = N5MaterializationRuntime(
            sampler=DeterministicSampler(),
            software_versions=VERSIONS,
        )
        runtime.execute(first.plan, SOURCE)
        with self.assertRaises(N5MaterializationConflict):
            runtime.execute(changed, SOURCE)

    def test_real_http_round_trip_uses_binary_handles_and_safe_evidence(
        self,
    ) -> None:
        frozen = self.freeze()
        sampler = DeterministicSampler()
        service, client, _running = self.serve(sampler=sampler)

        execution = client.execute(frozen.plan, SOURCE)

        self.assertEqual(frozen.artifact_bytes, execution.artifact_bytes)
        self.assertEqual(1, sampler.calls)
        self.assertEqual(
            _sha256(SOURCE),
            execution.transport_receipt["source_handle"],
        )
        self.assertEqual(
            _sha256(frozen.artifact_bytes),
            execution.transport_receipt["result_handle"],
        )
        self.assertTrue(
            execution.transport_receipt["stable_runtime_epoch_verified"]
        )
        self.assertFalse(
            execution.transport_receipt["ambient_proxies_used"]
        )
        self.assertFalse(execution.evidence["llm_called"])
        combined = json.dumps(
            {
                "health": service.health(),
                "evidence": execution.evidence,
                "receipt": execution.transport_receipt,
            },
            sort_keys=True,
        ).encode("utf-8")
        self.assertNotIn(TOKEN.encode("utf-8"), combined)
        self.assertNotIn(b"://", combined)

        with closing(sqlite3.connect(service._database_path)) as connection:
            status = connection.execute(
                "SELECT status FROM materialization_requests"
            ).fetchone()[0]
        self.assertEqual("COMPLETE", status)
        for path in service._database_path.parent.iterdir():
            if path.is_file():
                self.assertNotIn(TOKEN.encode("utf-8"), path.read_bytes())

    def test_http_replay_survives_service_restart_without_resampling(
        self,
    ) -> None:
        frozen = self.freeze()
        state_dir = self.state_dir()
        first_sampler = DeterministicSampler()
        _first_service, first_client, first_server = self.serve(
            sampler=first_sampler,
            state_dir=state_dir,
        )
        first = first_client.execute(frozen.plan, SOURCE)
        first_epoch = first.transport_receipt["runtime_epoch"]
        first_server.close()

        second_sampler = DeterministicSampler()
        _second_service, second_client, _second_server = self.serve(
            sampler=second_sampler,
            state_dir=state_dir,
        )
        second = second_client.execute(frozen.plan, SOURCE)

        self.assertEqual(1, first_sampler.calls)
        self.assertEqual(0, second_sampler.calls)
        self.assertEqual(first.artifact_bytes, second.artifact_bytes)
        self.assertTrue(second.evidence["idempotent_replay"])
        self.assertNotEqual(
            first_epoch,
            second.transport_receipt["runtime_epoch"],
        )
        with closing(sqlite3.connect(
            state_dir / "n5-materialization.sqlite3"
        )) as connection:
            counts = (
                connection.execute(
                    "SELECT COUNT(*) FROM materialization_requests"
                ).fetchone()[0],
                connection.execute(
                    "SELECT COUNT(*) FROM materialized_results"
                ).fetchone()[0],
            )
        self.assertEqual((1, 1), counts)

    def test_http_restart_reclaims_incomplete_request_and_finishes(self) -> None:
        frozen = self.freeze()
        state_dir = self.state_dir()
        first_service, first_client, first_server = self.serve(
            state_dir=state_dir,
        )
        handle = first_client.stage_source(frozen.plan, SOURCE)
        with closing(sqlite3.connect(
            first_service._database_path
        )) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(
                """
                INSERT INTO materialization_requests (
                    request_id,
                    plan_sha256,
                    source_handle,
                    status,
                    owner_epoch
                ) VALUES (?, ?, ?, 'RUNNING', ?)
                """,
                (
                    frozen.plan["idempotency_key"],
                    frozen.plan["plan_sha256"],
                    handle,
                    "0" * 32,
                ),
            )
            connection.commit()
        first_server.close()

        sampler = DeterministicSampler()
        _service, client, _server = self.serve(
            sampler=sampler,
            state_dir=state_dir,
        )
        execution = client.execute(frozen.plan, SOURCE)

        self.assertEqual(frozen.artifact_bytes, execution.artifact_bytes)
        self.assertEqual(1, sampler.calls)
        self.assertFalse(execution.evidence["idempotent_replay"])
        with closing(sqlite3.connect(
            state_dir / "n5-materialization.sqlite3"
        )) as connection:
            status = connection.execute(
                "SELECT status FROM materialization_requests"
            ).fetchone()[0]
        self.assertEqual("COMPLETE", status)

    def test_http_same_request_id_with_new_plan_digest_conflicts(self) -> None:
        frozen = self.freeze()
        _service, client, _running = self.serve()
        client.execute(frozen.plan, SOURCE)

        changed = json.loads(json.dumps(frozen.plan))
        changed["plan_id"] = "n5-materialization-visible-v2"
        unsigned = dict(changed)
        unsigned.pop("plan_sha256")
        changed["plan_sha256"] = _sha256(
            json.dumps(
                unsigned,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        with self.assertRaisesRegex(
            N5MaterializationHttpError,
            "HTTP 409",
        ):
            client.submit(changed, _sha256(SOURCE))

    def test_real_http_concurrent_replay_waits_and_materializes_once(
        self,
    ) -> None:
        frozen = self.freeze()
        entered = threading.Event()
        release = threading.Event()
        sampler = DeterministicSampler(entered=entered, release=release)
        _service, first_client, running = self.serve(sampler=sampler)
        second_client = HttpN5MaterializationClient(
            N5MaterializationHttpClientConfig(
                base_url=running.base_url,
                bearer_token=TOKEN,
                timeout_seconds=5,
            )
        )
        results: list[Any] = []
        failures: list[BaseException] = []

        def invoke(client: HttpN5MaterializationClient) -> None:
            try:
                results.append(client.execute(frozen.plan, SOURCE))
            except BaseException as exc:  # pragma: no cover - assertion aid
                failures.append(exc)

        first = threading.Thread(target=invoke, args=(first_client,))
        second = threading.Thread(target=invoke, args=(second_client,))
        first.start()
        self.assertTrue(entered.wait(2))
        second.start()
        second.join(0.1)
        self.assertTrue(second.is_alive())
        release.set()
        first.join(5)
        second.join(5)

        self.assertEqual([], failures)
        self.assertEqual(2, len(results))
        self.assertEqual(1, sampler.calls)
        self.assertEqual(
            [False, True],
            sorted(result.evidence["idempotent_replay"] for result in results),
        )

    def test_http_authentication_and_arbitrary_binary_stage_are_safe(
        self,
    ) -> None:
        service, _client, running = self.serve()
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        wrong = urllib.request.Request(
            running.base_url
            + "/v1/materialization-inputs/"
            + _sha256(SOURCE),
            data=SOURCE,
            headers={
                "Authorization": "Bearer wrong-secret",
                "Content-Type": "video/mp4",
                "X-Pathfinder-Content-SHA256": _sha256(SOURCE),
            },
            method="PUT",
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            opener.open(wrong, timeout=5)
        body = caught.exception.read()
        self.assertEqual(401, caught.exception.code)
        self.assertNotIn(TOKEN.encode("utf-8"), body)
        self.assertNotIn(b"wrong-secret", body)

        binary = b"\xff\xfe\x00not-json\x80"
        handle = _sha256(binary)
        request = urllib.request.Request(
            running.base_url + "/v1/materialization-inputs/" + handle,
            data=binary,
            headers={
                "Authorization": "Bearer " + TOKEN,
                "Content-Type": "video/mp4",
                "X-Pathfinder-Content-SHA256": handle,
            },
            method="PUT",
        )
        with opener.open(request, timeout=5) as response:
            value = json.load(response)
            self.assertEqual(201, response.status)
        self.assertEqual(handle, value["source_handle"])
        self.assertNotIn(binary, json.dumps(value).encode("utf-8"))
        with closing(sqlite3.connect(service._database_path)) as connection:
            persisted = connection.execute(
                """
                SELECT payload FROM staged_sources
                WHERE source_handle = ?
                """,
                (handle,),
            ).fetchone()
        self.assertEqual(binary, bytes(persisted[0]))

    def test_http_rejects_bad_binary_hash_and_raw_bytes_in_execute_json(
        self,
    ) -> None:
        frozen = self.freeze()
        service, client, running = self.serve()
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        bad = urllib.request.Request(
            running.base_url
            + "/v1/materialization-inputs/"
            + _sha256(SOURCE),
            data=SOURCE,
            headers={
                "Authorization": "Bearer " + TOKEN,
                "Content-Type": "video/mp4",
                "X-Pathfinder-Content-SHA256": "0" * 64,
            },
            method="PUT",
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            opener.open(bad, timeout=5)
        self.assertEqual(400, caught.exception.code)
        with closing(sqlite3.connect(service._database_path)) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM staged_sources"
            ).fetchone()[0]
        self.assertEqual(0, count)

        handle = client.stage_source(frozen.plan, SOURCE)
        invalid = {
            "schema_version": N5_MATERIALIZATION_HTTP_EXECUTE_SCHEMA_VERSION,
            "request_id": frozen.plan["idempotency_key"],
            "source_handle": handle,
            "plan": frozen.plan,
            "source_video_bytes": base64.b64encode(SOURCE).decode("ascii"),
        }
        payload = json.dumps(invalid, sort_keys=True).encode("utf-8")
        request = urllib.request.Request(
            running.base_url + "/v1/materializations/execute",
            data=payload,
            headers={
                "Authorization": "Bearer " + TOKEN,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            opener.open(request, timeout=5)
        self.assertEqual(400, caught.exception.code)

    def test_http_client_refuses_redirect_and_untrusted_plain_http(self) -> None:
        _RedirectHandler.target_hits = 0
        running = _RunningServer(
            ThreadingHTTPServer(("127.0.0.1", 0), _RedirectHandler)
        )
        self.addCleanup(running.close)
        client = HttpN5MaterializationClient(
            N5MaterializationHttpClientConfig(
                base_url=running.base_url,
                bearer_token=TOKEN,
                timeout_seconds=5,
            )
        )
        with self.assertRaisesRegex(N5MaterializationHttpError, "HTTP 302"):
            client.health()
        self.assertEqual(0, _RedirectHandler.target_hits)

        with self.assertRaisesRegex(N5MaterializationError, "HTTPS"):
            N5MaterializationHttpClientConfig(
                base_url="http://example.invalid:8080",
                bearer_token=TOKEN,
            )
        with self.assertRaisesRegex(N5MaterializationError, "explicitly bound"):
            N5MaterializationHttpClientConfig(
                base_url="http://pathfinder-sim-n5:8080",
                bearer_token=TOKEN,
            )
        config = N5MaterializationHttpClientConfig(
            base_url="http://pathfinder-sim-n5:8080",
            bearer_token=TOKEN,
            simulator_private_http_hosts=("pathfinder-sim-n5",),
        )
        self.assertNotIn(TOKEN, repr(config))

    def test_http_client_rejects_corrupt_result_content_binding(self) -> None:
        frozen = self.freeze()
        _CorruptResultHandler.expected_handle = frozen.plan[
            "expected_output"
        ]["artifact_sha256"]
        _CorruptResultHandler.payload = frozen.artifact_bytes
        running = _RunningServer(
            ThreadingHTTPServer(("127.0.0.1", 0), _CorruptResultHandler)
        )
        self.addCleanup(running.close)
        client = HttpN5MaterializationClient(
            N5MaterializationHttpClientConfig(
                base_url=running.base_url,
                bearer_token=TOKEN,
                timeout_seconds=5,
            )
        )
        result = {
            "result_handle": _CorruptResultHandler.expected_handle,
            "output": frozen.plan["expected_output"],
        }
        with self.assertRaisesRegex(
            N5MaterializationError,
            "content binding",
        ):
            client.fetch_result(result)

    def test_n4_atomic_publication_contract_is_explicit(self) -> None:
        requirements = "\n".join(N4_ATOMIC_PUBLICATION_REQUIREMENTS)
        self.assertIn("compare-and-swap", requirements)
        self.assertIn("one commit", requirements)
        self.assertIn("idempotent", requirements)
        self.assertIn("no partially visible artifact", requirements)

    def test_portable_path_and_strict_numeric_types_are_enforced(self) -> None:
        with self.assertRaisesRegex(N5MaterializationError, "portable"):
            freeze_n5_materialization_plan(
                plan_id="n5-materialization-visible-v1",
                idempotency_key="n5-materialize-object-0001-v1",
                object_id=OBJECT_ID,
                source_video_id=VIDEO_ID,
                source_video_filename=VIDEO_NAME,
                source_video_bytes=SOURCE,
                source_frame_descriptions_path="../sampled_frames.json",
                source_frame_descriptions_bytes=_description_bytes(),
                generation_manifest_bytes=_generation_manifest_bytes(),
                frame_count=2,
                jpeg_max_dimension=768,
                sampler=DeterministicSampler(),
                software_versions=VERSIONS,
            )
        with self.assertRaisesRegex(N5MaterializationError, "frame_count"):
            freeze_n5_materialization_plan(
                plan_id="n5-materialization-visible-v1",
                idempotency_key="n5-materialize-object-0001-v1",
                object_id=OBJECT_ID,
                source_video_id=VIDEO_ID,
                source_video_filename=VIDEO_NAME,
                source_video_bytes=SOURCE,
                source_frame_descriptions_path="sampled_frames.json",
                source_frame_descriptions_bytes=_description_bytes(),
                generation_manifest_bytes=_generation_manifest_bytes(),
                frame_count=True,
                jpeg_max_dimension=768,
                sampler=DeterministicSampler(),
                software_versions=VERSIONS,
            )


if __name__ == "__main__":
    unittest.main()
