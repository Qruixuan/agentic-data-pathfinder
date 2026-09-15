from __future__ import annotations

import hashlib
import json
import struct
import threading
import urllib.error
import urllib.request
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pathfinder.simulator.n5_digest_http import (
    N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION,
    N5DigestHTTPConflict,
    N5DigestHTTPError,
    N5DigestHTTPService,
    N5DigestHTTPSettings,
    create_n5_digest_http_server,
)
from pathfinder.simulator.n5_digest_materialization import (
    VisionDigestResult,
    freeze_n5_multimodal_digest_plan,
)
from pathfinder.video_prep import SampledImage


MODEL_ID = "qwen3.8-27b"
OBJECT_ID = "video-descriptive"
PLAN_ID = "n5-digest-http-plan-v1"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _mp4() -> bytes:
    brands = b"isom" + struct.pack(">I", 512) + b"isomiso2mp41"
    ftyp = struct.pack(">I", len(brands) + 8) + b"ftyp" + brands
    body = bytes(range(128))
    return ftyp + struct.pack(">I", len(body) + 8) + b"mdat" + body


def _jpeg(seed: int) -> bytes:
    body = bytes((seed + index) % 256 for index in range(32))
    return b"\xff\xd8" + body + b"\xff\xd9"


def _frames() -> list[SampledImage]:
    return [
        SampledImage(
            frame_index=index,
            timestamp_seconds=float(index * 2 + 1),
            width=320,
            height=180,
            jpeg_bytes=_jpeg(index * 7),
        )
        for index in range(3)
    ]


class FakeVisionAdapter:
    def __init__(self) -> None:
        self.calls = 0

    def generate_digest(self, **kwargs) -> VisionDigestResult:
        self.calls += 1
        return VisionDigestResult(
            model_id=kwargs["expected_model_id"],
            digest={
                "events": [{
                    "start_seconds": 1.0,
                    "end_seconds": 3.0,
                    "description": "A person walks through a visible room.",
                }],
                "summary": "A person walks through a room.",
            },
            response_sha256=hashlib.sha256(b"fake-response").hexdigest(),
            protocol_attempts=1,
            llm_called=True,
            adapter_id="fake-runtime-vision-v1",
        )


class N5DigestHTTPFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "source.mp4"
        self.source.write_bytes(_mp4())
        self.frames = _frames()
        self.plan = self.root / "plan"
        freeze_n5_multimodal_digest_plan(
            self.source,
            self.frames,
            source_duration_seconds=8.0,
            object_id=OBJECT_ID,
            model_id=MODEL_ID,
            output_dir=self.plan,
            plan_id=PLAN_ID,
            jpeg_max_dimension=768,
            seed=17,
        )
        self.source_handle = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.adapter = FakeVisionAdapter()
        self.settings = N5DigestHTTPSettings(
            bearer_token="runtime-only-digest-token",
            max_source_bytes=1024 * 1024,
            max_json_bytes=64 * 1024,
            max_result_bytes=1024 * 1024,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def sampler(self, path, *, frame_count, jpeg_max_dimension):
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(self.source_handle, observed)
        self.assertEqual(3, frame_count)
        self.assertEqual(768, jpeg_max_dimension)
        return list(self.frames), 8.0

    def request(self, request_id: str = "digest-request-v1") -> bytes:
        return _canonical({
            "schema_version": N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION,
            "request_id": request_id,
            "plan_id": PLAN_ID,
            "source_handle": self.source_handle,
        })

    def service(self, adapter=None) -> N5DigestHTTPService:
        return N5DigestHTTPService(
            self.root / "state",
            [self.plan],
            vision_adapter=self.adapter if adapter is None else adapter,
            settings=self.settings,
            sampler=self.sampler,
        )


class N5DigestHTTPServiceTest(N5DigestHTTPFixture):
    def test_unregistered_source_is_rejected_without_persistence(self) -> None:
        service = self.service()
        payload = _mp4() + b"unregistered"
        handle = hashlib.sha256(payload).hexdigest()
        with self.assertRaisesRegex(
            N5DigestHTTPError,
            "not registered with this exact size",
        ):
            service.stage_source(handle, payload, handle)
        self.assertEqual([], list((self.root / "state" / "sources").iterdir()))

    def test_stage_execute_replay_restart_and_result(self) -> None:
        service = self.service()
        first_stage = service.stage_source(
            self.source_handle,
            self.source.read_bytes(),
            self.source_handle,
        )
        replay_stage = service.stage_source(
            self.source_handle,
            self.source.read_bytes(),
            self.source_handle,
        )
        self.assertFalse(first_stage["idempotent_replay"])
        self.assertTrue(replay_stage["idempotent_replay"])

        first = service.execute(self.request())
        second = service.execute(self.request())
        self.assertEqual(first["status"], "COMPLETE")
        self.assertTrue(first["llm_called"])
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(1, self.adapter.calls)

        restarted_adapter = FakeVisionAdapter()
        restarted = self.service(restarted_adapter)
        replay = restarted.execute(self.request())
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(0, restarted_adapter.calls)
        payload, digest = restarted.result(replay["result_handle"])
        self.assertEqual(digest, hashlib.sha256(payload).hexdigest())
        self.assertIn(b"QUESTION-INDEPENDENT", payload)
        self.assertNotIn(self.settings.bearer_token, json.dumps(replay))

    def test_conflicting_request_id_fails_closed(self) -> None:
        second_plan = self.root / "plan-two"
        freeze_n5_multimodal_digest_plan(
            self.source,
            self.frames,
            source_duration_seconds=8.0,
            object_id=OBJECT_ID,
            model_id=MODEL_ID,
            output_dir=second_plan,
            plan_id="n5-digest-http-plan-v2",
            jpeg_max_dimension=768,
            seed=17,
        )
        service = N5DigestHTTPService(
            self.root / "state",
            [self.plan, second_plan],
            vision_adapter=self.adapter,
            settings=self.settings,
            sampler=self.sampler,
        )
        service.stage_source(
            self.source_handle,
            self.source.read_bytes(),
            self.source_handle,
        )
        service.execute(self.request())
        changed = json.loads(self.request())
        changed["plan_id"] = "n5-digest-http-plan-v2"
        with self.assertRaisesRegex(
            N5DigestHTTPConflict,
            "another request",
        ):
            service.execute(_canonical(changed))

    def test_noncanonical_request_and_unknown_plan_fail_before_model(self) -> None:
        service = self.service()
        service.stage_source(
            self.source_handle,
            self.source.read_bytes(),
            self.source_handle,
        )
        pretty = json.dumps(json.loads(self.request()), indent=2).encode("utf-8")
        from pathfinder.simulator.n5_digest_http import N5DigestHTTPError

        with self.assertRaisesRegex(N5DigestHTTPError, "canonical"):
            service.execute(pretty)
        unknown = json.loads(self.request())
        unknown["plan_id"] = "unknown-plan"
        with self.assertRaisesRegex(N5DigestHTTPError, "unknown"):
            service.execute(_canonical(unknown))
        self.assertEqual(0, self.adapter.calls)


class N5DigestHTTPServerTest(N5DigestHTTPFixture):
    def setUp(self) -> None:
        super().setUp()
        self.server = create_n5_digest_http_server(
            self.root / "state",
            [self.plan],
            vision_adapter=self.adapter,
            settings=self.settings,
            sampler=self.sampler,
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        host, port = self.server.server_address[:2]
        self.origin = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        super().tearDown()

    def open(
        self,
        path: str,
        *,
        method: str,
        body: bytes | None = None,
        content_type: str | None = None,
        token: bool = True,
        digest: str | None = None,
    ):
        headers = {"Accept-Encoding": "identity"}
        if content_type is not None:
            headers["Content-Type"] = content_type
        if token:
            headers["Authorization"] = "Bearer " + self.settings.bearer_token
        if digest is not None:
            headers["X-Pathfinder-Content-SHA256"] = digest
        request = urllib.request.Request(
            self.origin + path,
            method=method,
            data=body,
            headers=headers,
        )
        try:
            return urllib.request.urlopen(request, timeout=5)
        except urllib.error.HTTPError as exc:
            return exc

    def test_http_source_execute_and_result_with_authentication(self) -> None:
        with self.open("/healthz", method="GET", token=False) as response:
            health = json.load(response)
        self.assertEqual(health["node_id"], "N5")

        with self.open(
            f"/v1/digest-inputs/{self.source_handle}",
            method="PUT",
            body=self.source.read_bytes(),
            content_type="video/mp4",
            token=False,
            digest=self.source_handle,
        ) as response:
            self.assertEqual(response.status, 401)

        with self.open(
            f"/v1/digest-inputs/{self.source_handle}",
            method="PUT",
            body=self.source.read_bytes(),
            content_type="video/mp4",
            digest=self.source_handle,
        ) as response:
            self.assertEqual(response.status, 201)

        with self.open(
            "/v1/digest-materializations/execute",
            method="POST",
            body=self.request(),
            content_type="application/json",
        ) as response:
            result = json.load(response)
        self.assertEqual(result["status"], "COMPLETE")

        with self.open(
            "/v1/digest-results/" + result["result_handle"],
            method="GET",
        ) as response:
            payload = response.read()
            digest = response.headers["X-Pathfinder-Content-SHA256"]
        self.assertEqual(digest, hashlib.sha256(payload).hexdigest())
        self.assertEqual(1, self.adapter.calls)


if __name__ == "__main__":
    unittest.main()
