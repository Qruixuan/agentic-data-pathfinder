"""Offline tests for the unified N4 -> N7 -> N6 full-flow runtime."""

from __future__ import annotations

import base64
import hashlib
import json
import threading
import unittest
from email.message import Message
from typing import Any, Mapping

from pathfinder.data_agent_client import (
    DataAgentAccessTelemetry,
    DataAgentBinaryArtifact,
    HttpDataAgentClient,
)
from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
)
from pathfinder.frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    deterministic_frame_bundle_tar,
)
from pathfinder.frame_bundle_ingest import FRAME_BUNDLE_MEDIA_TYPE
from pathfinder.simulator.container_node import (
    CONTAINER_NODE_API_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION,
)
from pathfinder.simulator.full_flow_runtime import (
    FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
    DataAgentSourceAttestation,
    FullFlowHttpConfig,
    FullFlowIdempotencyConflict,
    FullFlowRouteConfig,
    FullFlowRuntimeError,
    FullFlowTrialRuntime,
    SourceAttestedDataAgentClient,
    build_full_flow_trial_request,
    build_http_full_flow_runtime,
)


OBJECT_ID = "nextqa-val-0000000001"
CATALOG_VERSION = "pathfinder-real-catalog-v1"
MODEL = "qwen3.8-27b"
REPRESENTATION_ID = "sampled_frame_bundle"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


_TEST_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBAUEBAYFBQUGBgYHCQ4JCQgICRINDQoOFRIW"
    "FhUSFBQXGiEcFxgfGRQUHScdHyIjJSUlFhwpLCgkKyEkJST/2wBDAQYGBgkICREJCREkGBQY"
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/wAAR"
    "CAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAAAAAA"
    "AAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oA"
    "DAMBAAIRAxEAPwCdAAyqX//Z"
)


def _jpeg(marker: bytes) -> bytes:
    payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
    comment = b"\xff\xfe" + (len(marker) + 2).to_bytes(2, "big") + marker
    return payload[:2] + comment + payload[2:]


def _bundle_bytes() -> bytes:
    frames = [_jpeg(b"\x11\x21"), _jpeg(b"\x12\x22")]
    frame_rows = [
        {
            "frame_index": index,
            "timestamp_seconds": 0.5 + index * 1.25,
            "width": 2,
            "height": 2,
            "path": f"frames/{index:03d}.jpg",
            "jpeg_size_bytes": len(frame),
            "jpeg_sha256": _sha256(frame),
        }
        for index, frame in enumerate(frames)
    ]
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": REPRESENTATION_ID,
        "object_id": OBJECT_ID,
        "source_video_id": "0000000001",
        "source_video_filename": "0000000001.mp4",
        "source_video_size_bytes": 123456,
        "source_video_sha256": "a" * 64,
        "source_duration_seconds": 42.5,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": len(frames),
            "jpeg_max_dimension": 768,
            "jpeg_quality": 82,
            "jpeg_optimize": True,
        },
        "source_frame_descriptions": {
            "representation_id": "sampled_frames",
            "path": f"{OBJECT_ID}/sampled_frames.json",
            "sha256": "b" * 64,
        },
        "generation_manifest_sha256": "c" * 64,
        "frames": frame_rows,
        "frame_count": len(frames),
        "total_jpeg_bytes": sum(len(frame) for frame in frames),
        "software_versions": {"av": "17.0.1", "Pillow": "12.3.0"},
        "historical_visual_bytes_retained": False,
        "sampling_alignment_statement": (
            "These JPEG frames were regenerated from the same source video "
            "using the same sampling algorithm and are aligned with the "
            "frozen sampling metadata. The historical visual bytes were not "
            "retained, so this artifact does not claim byte identity with "
            "the historical visual input."
        ),
        "claims_byte_identity_with_historical_visual_input": False,
        "credentials_recorded": False,
        "llm_called": False,
        "network_calls_performed": False,
    }
    manifest_bytes = (
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return deterministic_frame_bundle_tar(
        [(OBJECT_MANIFEST_NAME, manifest_bytes)]
        + [
            (f"frames/{index:03d}.jpg", frame)
            for index, frame in enumerate(frames)
        ]
    )


class FakeDataAgentClient:
    def __init__(
        self,
        raw: bytes,
        *,
        catalog_version: str = CATALOG_VERSION,
        location: str = "origin-warm",
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.raw = raw
        self.catalog_version = catalog_version
        self.location = location
        self.entered = entered
        self.release = release
        self.fetch_count = 0
        self.telemetry_count = 0
        self.requests: list[Any] = []

    def get_source_attestation(
        self,
        access_id: str,
    ) -> DataAgentSourceAttestation:
        return DataAgentSourceAttestation(
            access_id=access_id,
            source_node_id="N4",
            object_catalog_version=self.catalog_version,
            representations=(REPRESENTATION_ID,),
            health_sha256_before="d" * 64,
            health_sha256_after="d" * 64,
            stable_health_verified=True,
        )

    def fetch_binary_artifact(
        self,
        request: Any,
        *,
        allowed_media_types: Any,
        on_phase: Any = None,
    ) -> DataAgentBinaryArtifact:
        del allowed_media_types
        self.fetch_count += 1
        self.requests.append(request)
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            if not self.release.wait(5):
                raise RuntimeError("test release timed out")
        if on_phase is not None:
            on_phase("access_completed")
            on_phase("artifact_download_started")
            on_phase("artifact_download_completed")
        return DataAgentBinaryArtifact(
            access_id=request.access_id,
            media_type=FRAME_BUNDLE_MEDIA_TYPE,
            data=self.raw,
            size_bytes=len(self.raw),
            sha256=_sha256(self.raw),
            object_id=OBJECT_ID,
            object_catalog_version=self.catalog_version,
            location=self.location,
            service_latency_ms=3.0,
            client_round_trip_ms=5.0,
            download_elapsed_ms=1.0,
        )

    def get_access_telemetry(
        self,
        access_id: str,
        *,
        wait_for_quiescence: bool = False,
        quiescence_timeout_seconds: float = 5.0,
        quiescence_poll_seconds: float = 0.02,
    ) -> DataAgentAccessTelemetry:
        del quiescence_timeout_seconds, quiescence_poll_seconds
        self.telemetry_count += 1
        self.wait_for_quiescence = wait_for_quiescence
        return DataAgentAccessTelemetry(
            access_id=access_id,
            object_id=OBJECT_ID,
            representation_id=REPRESENTATION_ID,
            object_catalog_version=self.catalog_version,
            download_request_count=1,
            completed_request_count=1,
            full_download_count=1,
            bytes_sent=len(self.raw),
            transfer_latency_ms=2.5,
            latest_completed_at=1.0,
            in_flight_request_count=0,
            server_reported_complete=True,
        )


class FailOnceDataAgentClient(FakeDataAgentClient):
    def __init__(self, raw: bytes) -> None:
        super().__init__(raw)
        self.attempt_count = 0

    def fetch_binary_artifact(
        self,
        request: Any,
        *,
        allowed_media_types: Any,
        on_phase: Any = None,
    ) -> DataAgentBinaryArtifact:
        self.attempt_count += 1
        if self.attempt_count == 1:
            raise RuntimeError("transient test failure before transfer")
        return super().fetch_binary_artifact(
            request,
            allowed_media_types=allowed_media_types,
            on_phase=on_phase,
        )


class CancelOnceDataAgentClient(FakeDataAgentClient):
    def __init__(self, raw: bytes) -> None:
        super().__init__(raw)
        self.cancelled = False

    def fetch_binary_artifact(
        self,
        request: Any,
        *,
        allowed_media_types: Any,
        on_phase: Any = None,
    ) -> DataAgentBinaryArtifact:
        if not self.cancelled:
            self.cancelled = True
            raise KeyboardInterrupt()
        return super().fetch_binary_artifact(
            request,
            allowed_media_types=allowed_media_types,
            on_phase=on_phase,
        )


class FakeSemanticAdapter:
    def __init__(
        self,
        *,
        answer: str = "B",
        model: str = MODEL,
        corrupt: str | None = None,
        corrupt_value: Any = "corrupt",
    ) -> None:
        self.answer = answer
        self.model = model
        self.corrupt = corrupt
        self.corrupt_value = corrupt_value
        self.requests: list[dict[str, Any]] = []
        self.health_verified = True
        self.last_runtime_epoch = "3" * 32

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        copied = json.loads(json.dumps(request))
        self.requests.append(copied)
        delivered = sum(
            frame["jpeg_size_bytes"] for frame in request["frames"]
        )
        result: dict[str, Any] = {
            "schema_version": (
                CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION
            ),
            "api_version": CONTAINER_NODE_API_VERSION,
            "status": "completed",
            "outcome_type": "completed",
            "telemetry_complete": True,
            "semantic_request_id": request["semantic_request_id"],
            "execution_node_id": request["execution_node_id"],
            "started_monotonic_ns": 100,
            "finished_monotonic_ns": 200,
            "service_time_ms": 0.0001,
            "request_sha256": _sha256(_canonical_bytes(request)),
            "prompt_sha256": request["prompt_sha256"],
            "representation_sha256": request["representation_sha256"],
            "frame_sequence_sha256": request["frame_sequence_sha256"],
            "frame_count": len(request["frames"]),
            "representation_delivery_bytes": delivered,
            "semantic_input_kind": "ordered-jpeg-frames",
            "data_plane_artifact_delivery_verified": False,
            "semantic_frame_payload_integrity_verified": True,
            "source_node_id": None,
            "model": self.model,
            "final_answer": self.answer,
            "final_answer_sha256": _sha256(self.answer.encode("utf-8")),
            "llm_called": True,
            "credentials_recorded": False,
            "idempotent_replay": False,
        }
        if self.corrupt is not None:
            result[self.corrupt] = self.corrupt_value
        return result


class FailingSemanticAdapter(FakeSemanticAdapter):
    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        self.requests.append(json.loads(json.dumps(request)))
        raise RuntimeError("simulated N6 failure after artifact delivery")


class FakeHealthResponse:
    def __init__(self, value: Mapping[str, Any]) -> None:
        self.status = 200
        self._raw = _canonical_bytes(value)
        self.headers = Message()
        self.headers["Content-Type"] = "application/json"

    def __enter__(self) -> "FakeHealthResponse":
        return self

    def __exit__(self, *args: Any) -> None:
        del args

    def read(self, limit: int) -> bytes:
        return self._raw[:limit]


class FakeHealthOpener:
    def __init__(self, *, node_id: str = "N4") -> None:
        self.node_id = node_id
        self.calls = 0

    def __call__(self, request: Any, *, timeout: float) -> FakeHealthResponse:
        del request, timeout
        self.calls += 1
        return FakeHealthResponse({
            "status": "ok",
            "api_version": "pathfinder.data-agent/v1alpha1",
            "node_id": self.node_id,
            "representations": [REPRESENTATION_ID],
            "object_catalog_version": CATALOG_VERSION,
            "object_count": 1,
            "credentials_recorded": False,
        })


class BlockingSemanticAdapter(FakeSemanticAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.active = 0
        self.overlap_observed = False
        self._guard = threading.Lock()

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        with self._guard:
            self.active += 1
            if self.active > 1:
                self.overlap_observed = True
        try:
            self.entered.set()
            if not self.release.wait(5):
                raise RuntimeError("test release timed out")
            return super().execute(request)
        finally:
            with self._guard:
                self.active -= 1


class FullFlowRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = _bundle_bytes()
        self.route = FullFlowRouteConfig(
            route_id="real-origin-to-n6-v1",
            requested_location="origin-warm",
            data_agent_plan_id="real-origin-warm-plan-v1",
            data_agent_plan_epoch=4,
        )
        self.client = FakeDataAgentClient(self.raw)
        self.adapter = FakeSemanticAdapter()
        self.runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=self.client,
            semantic_adapter=self.adapter,
        )

    def request(self, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "route_config": self.route,
            "full_flow_request_id": "full-flow-request-0001",
            "run_id": "full-flow-run-v1",
            "trial_id": "trial-0001",
            "trial_key": "scenario|W1|D2|r0000",
            "workload_id": "visible-video-qa-0001",
            "object_id": OBJECT_ID,
            "artifact_sha256": _sha256(self.raw),
            "artifact_size_bytes": len(self.raw),
            "object_catalog_version": CATALOG_VERSION,
            "expected_model": MODEL,
            "question": "Which option describes the main action?",
            "answer_options": [
                {"option_id": "A", "text": "A person cooks food."},
                {"option_id": "B", "text": "Two musicians perform."},
                {"option_id": "C", "text": "A vehicle crosses a river."},
            ],
            "correct_answer_id": "B",
            "success_scoring_rule": MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        }
        values.update(overrides)
        return build_full_flow_trial_request(**values)

    def test_complete_evidence_unifies_real_object_route_and_score(self) -> None:
        evidence = self.runtime.execute(self.request())

        self.assertEqual(
            FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
            evidence["schema_version"],
        )
        self.assertEqual("COMPLETE", evidence["status"])
        self.assertEqual(OBJECT_ID, evidence["object_id"])
        self.assertEqual(REPRESENTATION_ID, evidence["representation_id"])
        self.assertEqual(
            {
                "source_node_id": "N4",
                "executor_node_id": "N7",
                "inference_node_id": "N6",
            },
            {
                key: evidence["route"][key]
                for key in (
                    "source_node_id",
                    "executor_node_id",
                    "inference_node_id",
                )
            },
        )
        self.assertTrue(evidence["route_unified"])
        self.assertTrue(evidence["real_object_identity_verified"])
        self.assertTrue(evidence["data_agent_source_identity_verified"])
        self.assertTrue(evidence["data_agent_artifact_delivery_verified"])
        self.assertTrue(evidence["semantic_health_verified"])
        self.assertTrue(evidence["scoring"]["task_success"])
        self.assertEqual(MODEL, evidence["semantic"]["model"])
        self.assertEqual(_sha256(self.raw), evidence["data_agent"]["artifact_sha256"])
        self.assertEqual("N4", evidence["data_agent"]["source_node_id"])
        self.assertTrue(evidence["data_agent"]["source_identity_verified"])
        self.assertEqual(2, evidence["data_agent"]["frame_count"])
        self.assertEqual(1, self.client.fetch_count)
        self.assertEqual(1, self.client.telemetry_count)
        self.assertEqual(1, len(self.adapter.requests))
        self.assertTrue(self.client.wait_for_quiescence)

        access = self.client.requests[0]
        self.assertEqual(OBJECT_ID, access.object_id)
        self.assertEqual("origin-warm", access.binding["location"])
        self.assertEqual("real-origin-warm-plan-v1", access.plan_id)
        self.assertEqual(4, access.plan_epoch)
        semantic = self.adapter.requests[0]
        self.assertEqual("N6", semantic["execution_node_id"])
        self.assertEqual(2, len(semantic["frames"]))
        self.assertEqual([0, 1], [frame["frame_index"] for frame in semantic["frames"]])

    def test_incorrect_declared_option_is_valid_but_scores_false(self) -> None:
        self.adapter.answer = "A"
        evidence = self.runtime.execute(self.request())
        self.assertFalse(evidence["scoring"]["task_success"])
        self.assertEqual("A", evidence["scoring"]["final_answer"])

    def test_trimmed_answer_has_consistent_evidence_hash(self) -> None:
        self.adapter.answer = "  B\n"
        evidence = self.runtime.execute(self.request())
        self.assertEqual("B", evidence["scoring"]["final_answer"])
        self.assertEqual(
            _sha256(b"B"),
            evidence["scoring"]["final_answer_sha256"],
        )
        self.assertTrue(evidence["scoring"]["task_success"])

    def test_canonical_marker_rule_accepts_bracketed_answer(self) -> None:
        self.adapter.answer = "[B]"
        evidence = self.runtime.execute(self.request(
            success_scoring_rule=(
                MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
            )
        ))
        self.assertTrue(evidence["scoring"]["task_success"])

    def test_prose_answer_is_rejected_before_evidence(self) -> None:
        self.adapter.answer = "The answer is B"
        with self.assertRaisesRegex(FullFlowRuntimeError, "declared option"):
            self.runtime.execute(self.request())

    def test_evidence_has_no_payload_prompt_or_transport_config(self) -> None:
        request = self.request()
        evidence = self.runtime.execute(request)
        raw = _canonical_bytes(evidence)
        self.assertNotIn(request["question"].encode(), raw)
        for option in request["answer_options"]:
            self.assertNotIn(option["text"].encode(), raw)
        for frame in self.adapter.requests[0]["frames"]:
            self.assertNotIn(frame["jpeg_base64"].encode(), raw)
        self.assertNotIn(b"http://", raw)
        self.assertNotIn(b"https://", raw)
        self.assertNotIn(b"super-secret-token", raw)

    def test_identical_request_replays_without_external_calls(self) -> None:
        request = self.request()
        first = self.runtime.execute(request)
        second = self.runtime.execute(request)
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(1, self.client.fetch_count)
        self.assertEqual(1, len(self.adapter.requests))
        comparable = dict(second)
        comparable["idempotent_replay"] = False
        self.assertEqual(first, comparable)

    def test_same_id_with_different_valid_request_conflicts(self) -> None:
        self.runtime.execute(self.request())
        changed = self.request(question="Which option is visible now?")
        with self.assertRaises(FullFlowIdempotencyConflict):
            self.runtime.execute(changed)
        self.assertEqual(1, self.client.fetch_count)

    def test_pretransfer_failure_releases_state_for_retry(self) -> None:
        client = FailOnceDataAgentClient(self.raw)
        adapter = FakeSemanticAdapter()
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=client,
            semantic_adapter=adapter,
        )
        request = self.request(
            full_flow_request_id="full-flow-request-retry"
        )
        with self.assertRaisesRegex(FullFlowRuntimeError, "Data Agent"):
            runtime.execute(request)
        evidence = runtime.execute(request)
        self.assertEqual("COMPLETE", evidence["status"])
        self.assertFalse(evidence["idempotent_replay"])
        self.assertEqual(2, client.attempt_count)
        self.assertEqual(1, client.fetch_count)
        self.assertEqual(1, len(adapter.requests))

    def test_postdownload_failure_refuses_same_id_second_download(self) -> None:
        client = FakeDataAgentClient(self.raw)
        adapter = FailingSemanticAdapter()
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=client,
            semantic_adapter=adapter,
        )
        request = self.request(
            full_flow_request_id="full-flow-request-n6-failure"
        )
        with self.assertRaisesRegex(FullFlowRuntimeError, "N6 semantic"):
            runtime.execute(request)
        with self.assertRaisesRegex(FullFlowRuntimeError, "second download"):
            runtime.execute(request)
        self.assertEqual(1, client.fetch_count)
        self.assertEqual(1, client.telemetry_count)
        self.assertEqual(1, len(adapter.requests))

    def test_base_exception_does_not_wedge_idempotency_key(self) -> None:
        client = CancelOnceDataAgentClient(self.raw)
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=client,
            semantic_adapter=FakeSemanticAdapter(),
        )
        request = self.request(
            full_flow_request_id="full-flow-request-cancelled"
        )
        with self.assertRaises(KeyboardInterrupt):
            runtime.execute(request)
        self.assertEqual("COMPLETE", runtime.execute(request)["status"])

    def test_concurrent_identical_request_waits_and_executes_once(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        client = FakeDataAgentClient(
            self.raw,
            entered=entered,
            release=release,
        )
        adapter = FakeSemanticAdapter()
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=client,
            semantic_adapter=adapter,
        )
        request = self.request()
        results: list[dict[str, Any]] = []
        failures: list[BaseException] = []

        def invoke() -> None:
            try:
                results.append(runtime.execute(request))
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
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(2, len(results))
        self.assertEqual([False, True], sorted(
            (row["idempotent_replay"] for row in results),
        ))
        self.assertEqual(1, client.fetch_count)
        self.assertEqual(1, len(adapter.requests))

    def test_different_ids_serialize_adapter_health_and_epoch_state(self) -> None:
        adapter = BlockingSemanticAdapter()
        client = FakeDataAgentClient(self.raw)
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=client,
            semantic_adapter=adapter,
        )
        requests = [
            self.request(full_flow_request_id=f"full-flow-concurrent-{index}")
            for index in range(2)
        ]
        results: list[dict[str, Any]] = []
        failures: list[BaseException] = []

        def invoke(value: Mapping[str, Any]) -> None:
            try:
                results.append(runtime.execute(value))
            except BaseException as exc:  # pragma: no cover - assertion aid
                failures.append(exc)

        threads = [
            threading.Thread(target=invoke, args=(request,))
            for request in requests
        ]
        threads[0].start()
        self.assertTrue(adapter.entered.wait(2))
        threads[1].start()
        adapter.release.set()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(2, len(results))
        self.assertFalse(adapter.overlap_observed)
        self.assertEqual(2, len(adapter.requests))

    def test_request_rejects_extra_endpoint_configuration(self) -> None:
        request = self.request()
        request["data_agent_url"] = "http://pathfinder-sim-n4:8000"
        with self.assertRaisesRegex(FullFlowRuntimeError, "field set"):
            self.runtime.execute(request)

    def test_tampered_frozen_field_fails_binding_digest(self) -> None:
        request = self.request()
        request["artifact_size_bytes"] += 1
        with self.assertRaisesRegex(FullFlowRuntimeError, "binding digest"):
            self.runtime.execute(request)

    def test_wrong_artifact_catalog_or_model_fails_closed(self) -> None:
        bad_client = FakeDataAgentClient(
            self.raw,
            catalog_version="wrong-catalog",
        )
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=bad_client,
            semantic_adapter=self.adapter,
        )
        with self.assertRaisesRegex(FullFlowRuntimeError, "Data Agent"):
            runtime.execute(self.request())

        wrong_model = FakeSemanticAdapter(model="wrong-model")
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=FakeDataAgentClient(self.raw),
            semantic_adapter=wrong_model,
        )
        with self.assertRaisesRegex(FullFlowRuntimeError, "model"):
            runtime.execute(self.request(
                full_flow_request_id="full-flow-request-model"
            ))

    def test_wrong_data_agent_source_identity_fails_closed(self) -> None:
        client = FakeDataAgentClient(self.raw)

        def wrong_attestation(access_id: str) -> DataAgentSourceAttestation:
            return DataAgentSourceAttestation(
                access_id=access_id,
                source_node_id="N3",
                object_catalog_version=CATALOG_VERSION,
                representations=(REPRESENTATION_ID,),
                health_sha256_before="d" * 64,
                health_sha256_after="d" * 64,
                stable_health_verified=True,
            )

        client.get_source_attestation = wrong_attestation  # type: ignore[method-assign]
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=client,
            semantic_adapter=FakeSemanticAdapter(),
        )
        with self.assertRaisesRegex(FullFlowRuntimeError, "source identity"):
            runtime.execute(self.request(
                full_flow_request_id="full-flow-request-wrong-source"
            ))

    def test_production_source_wrapper_attests_n4_before_and_after(self) -> None:
        client = FakeDataAgentClient(self.raw)
        health = FakeHealthOpener()
        wrapped = SourceAttestedDataAgentClient(
            client,
            health_url="http://pathfinder-sim-n4-origin:8080/healthz",
            opener=health,
            timeout_seconds=1.0,
            max_response_bytes=4096,
        )
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=wrapped,
            semantic_adapter=FakeSemanticAdapter(),
        )
        evidence = runtime.execute(self.request(
            full_flow_request_id="full-flow-request-source-wrapper"
        ))
        self.assertTrue(evidence["data_agent_source_identity_verified"])
        self.assertEqual(2, health.calls)

    def test_production_source_wrapper_rejects_non_n4_before_download(self) -> None:
        client = FakeDataAgentClient(self.raw)
        wrapped = SourceAttestedDataAgentClient(
            client,
            health_url="http://pathfinder-sim-n4-origin:8080/healthz",
            opener=FakeHealthOpener(node_id="N3"),
            timeout_seconds=1.0,
            max_response_bytes=4096,
        )
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=wrapped,
            semantic_adapter=FakeSemanticAdapter(),
        )
        with self.assertRaisesRegex(FullFlowRuntimeError, "Data Agent"):
            runtime.execute(self.request(
                full_flow_request_id="full-flow-request-source-wrapper-bad"
            ))
        self.assertEqual(0, client.fetch_count)

    def test_semantic_result_tampering_is_refused(self) -> None:
        adapter = FakeSemanticAdapter(corrupt="request_sha256")
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=FakeDataAgentClient(self.raw),
            semantic_adapter=adapter,
        )
        with self.assertRaisesRegex(FullFlowRuntimeError, "request digest"):
            runtime.execute(self.request(
                full_flow_request_id="full-flow-request-tampered-result"
            ))

    def test_semantic_count_fields_require_strict_integers(self) -> None:
        for field, value in (
            ("frame_count", 2.0),
            ("representation_delivery_bytes", True),
        ):
            adapter = FakeSemanticAdapter(
                corrupt=field,
                corrupt_value=value,
            )
            runtime = FullFlowTrialRuntime(
                route_config=self.route,
                data_agent_client=FakeDataAgentClient(self.raw),
                semantic_adapter=adapter,
            )
            with self.subTest(field=field):
                with self.assertRaisesRegex(FullFlowRuntimeError, "invalid"):
                    runtime.execute(self.request(
                        full_flow_request_id=f"strict-count-{field}"
                    ))

    def test_route_is_fixed_to_n4_n7_n6(self) -> None:
        for field, value in (
            ("source_node_id", "N3"),
            ("executor_node_id", "N8"),
            ("inference_node_id", "N5"),
        ):
            values = {
                "route_id": "bad-route",
                "requested_location": "origin-warm",
                "data_agent_plan_id": "plan",
                field: value,
            }
            with self.subTest(field=field):
                with self.assertRaises(FullFlowRuntimeError):
                    FullFlowRouteConfig(**values)

    def test_http_configuration_requires_safe_explicit_hosts(self) -> None:
        config = FullFlowHttpConfig(
            data_agent_base_url="http://pathfinder-sim-n4-origin:8080",
            semantic_base_url="http://pathfinder-sim-n6-inference:8080",
            data_agent_token="super-secret-token",
            semantic_bearer_token="semantic-runtime-only-token",
            simulator_private_http_hosts=(
                "pathfinder-sim-n4-origin",
                "pathfinder-sim-n6-inference",
            ),
        )
        self.assertNotIn("super-secret-token", repr(config))
        self.assertNotIn("semantic-runtime-only-token", repr(config))
        with self.assertRaisesRegex(FullFlowRuntimeError, "explicitly bound"):
            FullFlowHttpConfig(
                data_agent_base_url="http://pathfinder-sim-n4-origin:8080",
                semantic_base_url="http://127.0.0.1:19086",
            )
        with self.assertRaisesRegex(FullFlowRuntimeError, "must use HTTPS"):
            FullFlowHttpConfig(
                data_agent_base_url="http://data-agent.example.com:8080",
                semantic_base_url="http://127.0.0.1:19086",
            )

    def test_evidence_exposed_route_rejects_url_or_bearer_material(self) -> None:
        with self.assertRaisesRegex(FullFlowRuntimeError, "endpoint"):
            FullFlowRouteConfig(
                route_id="https://not-a-route.example.test",
                requested_location="origin-warm",
                data_agent_plan_id="plan-v1",
            )
        request = self.request(expected_model="Bearer should-not-persist")
        with self.assertRaisesRegex(FullFlowRuntimeError, "authorization"):
            self.runtime.execute(request)

    def test_recursive_evidence_scan_rejects_secret_and_url_like_values(self) -> None:
        for value in (
            "sk-abcdefghijklmnopqrstuvwxyz012345",
            "service.internal.test:8080",
            "pathfinder-sim-n6-inference:8080",
            "token=credential-value",
        ):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    FullFlowRuntimeError,
                    "endpoint or authorization",
                ):
                    self.runtime._assert_safe_evidence({
                        "ordinary_key": [{"value": value}],
                    })

    def test_ordinary_namespaced_model_id_is_not_a_secret_false_positive(self) -> None:
        model = "qwen/qwen3.8-27b"
        runtime = FullFlowTrialRuntime(
            route_config=self.route,
            data_agent_client=FakeDataAgentClient(self.raw),
            semantic_adapter=FakeSemanticAdapter(model=model),
        )
        evidence = runtime.execute(self.request(
            full_flow_request_id="full-flow-namespaced-model",
            expected_model=model,
        ))
        self.assertEqual(model, evidence["semantic"]["model"])

    def test_http_factory_is_non_networking_and_uses_real_client_type(self) -> None:
        config = FullFlowHttpConfig(
            data_agent_base_url="https://data-agent.example.test",
            semantic_base_url="http://127.0.0.1:19086",
            data_agent_token="super-secret-token",
            semantic_bearer_token="semantic-runtime-only-token",
        )
        runtime = build_http_full_flow_runtime(
            route_config=self.route,
            http_config=config,
        )
        self.assertIsInstance(runtime._data_agent, SourceAttestedDataAgentClient)
        self.assertIsInstance(
            runtime._data_agent.wrapped_client,
            HttpDataAgentClient,
        )
        self.assertEqual(self.route.sha256, runtime.route_config_sha256)


if __name__ == "__main__":
    unittest.main()
