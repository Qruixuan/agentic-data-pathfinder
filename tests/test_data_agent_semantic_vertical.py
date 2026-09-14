"""Focused offline tests for the Data Agent semantic vertical slice."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from unittest import mock

from pathfinder.data_agent_client import (
    DataAgentAccessTelemetry,
    DataAgentBinaryArtifact,
)
from pathfinder.distributed.registry import build_endpoint_registry
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
from pathfinder.simulator.data_agent_semantic_vertical import (
    CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION_V2,
    CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION_V2,
    MANIFEST_NAME,
    RECORD_NAME,
    DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
    DataAgentSemanticVerticalError,
    HttpContainerSemanticVisionAdapter,
    execute_data_agent_frame_bundle_semantic_trial,
    verify_data_agent_frame_bundle_semantic_trial,
)


MATRIX_PLAN_SHA256 = "1" * 64
REGISTRY_SHA256 = "2" * 64
MATRIX_ID = "matrix-test-v1"
TRIAL_KEY = "scenario|smoke-descriptive|D0|r0000"
MATRIX_OBJECT_ID = "video-descriptive-0001"
ARTIFACT_OBJECT_ID = "nextqa-val-0000000001"
CATALOG_VERSION = "catalog-test-v1"
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
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/"
    "wAARCAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAA"
    "AAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA"
    "/9oADAMBAAIRAxEAPwCdAAyqX//Z"
)


def _jpeg(width: int, height: int, filler: bytes) -> bytes:
    if (width, height) != (2, 2):
        raise ValueError("the embedded JPEG fixture is 2x2")
    payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
    comment = b"\xff\xfe" + (len(filler) + 2).to_bytes(2, "big") + filler
    return payload[:2] + comment + payload[2:]


def _bundle_bytes() -> bytes:
    frames = [_jpeg(2, 2, b"\x11\x21"), _jpeg(2, 2, b"\x12\x22")]
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
        "object_id": ARTIFACT_OBJECT_ID,
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
            "path": f"{ARTIFACT_OBJECT_ID}/sampled_frames.json",
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
    raw_manifest = (
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return deterministic_frame_bundle_tar(
        [(OBJECT_MANIFEST_NAME, raw_manifest)]
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
        download_count: int | None = None,
    ) -> None:
        self.raw = raw
        self.download_count = download_count
        self.fetch_count = 0
        self.telemetry_count = 0
        self.requests: list[Any] = []
        self.downloads_by_access_id: dict[str, int] = {}

    def fetch_binary_artifact(
        self,
        request: Any,
        *,
        allowed_media_types: Any,
        on_phase: Any = None,
    ) -> DataAgentBinaryArtifact:
        self.fetch_count += 1
        self.requests.append(request)
        self.downloads_by_access_id[request.access_id] = (
            self.downloads_by_access_id.get(request.access_id, 0) + 1
        )
        self.allowed_media_types = allowed_media_types
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
            object_id=ARTIFACT_OBJECT_ID,
            object_catalog_version=CATALOG_VERSION,
            location="origin-remote",
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
    ) -> DataAgentAccessTelemetry:
        del quiescence_timeout_seconds
        self.telemetry_count += 1
        self.wait_for_quiescence = wait_for_quiescence
        download_count = (
            self.download_count
            if self.download_count is not None
            else self.downloads_by_access_id.get(access_id, 0)
        )
        return DataAgentAccessTelemetry(
            access_id=access_id,
            object_id=ARTIFACT_OBJECT_ID,
            representation_id=REPRESENTATION_ID,
            object_catalog_version=CATALOG_VERSION,
            download_request_count=download_count,
            completed_request_count=download_count,
            full_download_count=download_count,
            bytes_sent=len(self.raw) * download_count,
            transfer_latency_ms=2.5,
            latest_completed_at=1.0,
            in_flight_request_count=0,
            server_reported_complete=True,
        )


class FakeContainerAdapter:
    def __init__(self, *, answer: str = "[B]", corrupt: str | None = None) -> None:
        self.answer = answer
        self.corrupt = corrupt
        self.requests: list[dict[str, Any]] = []
        self.health_verified = True
        self.last_runtime_epoch = "3" * 32

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        copied = json.loads(json.dumps(request))
        self.requests.append(copied)
        delivered = sum(frame["jpeg_size_bytes"] for frame in request["frames"])
        result: dict[str, Any] = {
            "schema_version": CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION_V2,
            "api_version": "pathfinder.container-node/v1alpha1",
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
            "model": "vision-model-test",
            "final_answer": self.answer,
            "final_answer_sha256": _sha256(self.answer.encode("utf-8")),
            "llm_called": True,
            "credentials_recorded": False,
            "idempotent_replay": False,
        }
        if self.corrupt is not None:
            result[self.corrupt] = "corrupt"
        return result


def _registry(*, with_route: bool = True) -> Any:
    root: dict[str, Any] = {
        "schema_version": "pathfinder.data-agent-endpoint-registry/v1alpha1",
        "registry_id": "registry-test-v1",
        "execution_node_id": "N6",
        "endpoints": [
            {
                "endpoint_id": "origin",
                "node_id": "N4",
                "location": "origin-remote",
                "base_url_env": "TEST_DATA_AGENT_URL",
                "token_env": "TEST_DATA_AGENT_TOKEN",
                "timeout_seconds": 5.0,
                "max_retries": 0,
                "telemetry_capabilities": [
                    "access_telemetry",
                    "transfer_bytes",
                    "quiescence_wait",
                ],
                "network_transport": "remote",
            },
            {
                "endpoint_id": "unused",
                "node_id": "N5",
                "location": "materializer",
                "base_url_env": "TEST_UNUSED_DATA_AGENT_URL",
                "timeout_seconds": 5.0,
                "max_retries": 0,
                "telemetry_capabilities": [],
                "network_transport": "remote",
            },
        ],
        "placement": [],
    }
    if with_route:
        root["placement"] = [
            {
                "design_id": "D_origin_remote",
                "representation_id": REPRESENTATION_ID,
                "endpoint_id": "origin",
            }
        ]
    return build_endpoint_registry(root, source_sha256=REGISTRY_SHA256)


def _spec_document(raw: bytes, **overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
        "semantic_run_id": "semantic-run-test-v1",
        "trial_key": TRIAL_KEY,
        "semantic_executor_node_id": "N6",
        "representation_id": REPRESENTATION_ID,
        "workload_id": "smoke-descriptive",
        "task_class_id": "video_qa",
        "artifact_object_id": ARTIFACT_OBJECT_ID,
        "question": "Which option describes the main action?",
        "answer_options": [
            {"option_id": "A", "text": "A vehicle crosses a river."},
            {"option_id": "B", "text": "Two musicians perform."},
            {"option_id": "C", "text": "A person cooks food."},
        ],
        "correct_answer_id": "B",
        "expected_model": "vision-model-test",
        "success_scoring_rule": (
            MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
        ),
        "data_agent_route_design_id": "D_origin_remote",
        "data_agent_plan_id": "D_origin_remote",
        "data_agent_plan_epoch": 3,
        "artifact_sha256": _sha256(raw),
        "artifact_size_bytes": len(raw),
        "object_catalog_version": CATALOG_VERSION,
        "credentials_recorded": False,
    }
    value.update(overrides)
    return value


def _write_spec(path: Path, raw: bytes, **overrides: Any) -> Path:
    path.write_text(
        json.dumps(_spec_document(raw, **overrides), indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    return path


def _restamp_semantic_output(root: Path) -> None:
    record_path = root / RECORD_NAME
    manifest_path = root / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["record_sha256"] = _sha256(record_path.read_bytes())
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{_sha256((root / name).read_bytes())}  {name}\n"
            for name in sorted((RECORD_NAME, MANIFEST_NAME))
        ),
        encoding="utf-8",
    )


def _matrix(root: Path) -> Path:
    root.mkdir()
    (root / "flowmesh-container-matrix-plan.json").write_text(
        json.dumps(
            {
                "schema_version": "test",
                "status": "FROZEN",
                "matrix_id": MATRIX_ID,
                "plan_sha256": MATRIX_PLAN_SHA256,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    trial = {
        "trial_key": TRIAL_KEY,
        "trial_id": "trial-0001",
        "order_index": 0,
        "workload_id": "smoke-descriptive",
        "workload_class": "W1",
        "design_id": "D0",
        "repetition": 0,
        "seed": 7,
        "object_id": MATRIX_OBJECT_ID,
        "executor_node_id": "N7",
        "operation_count": 5,
        "conditional_operation_count": 0,
        "cache_scope_ids": [],
    }
    (root / "flowmesh-container-matrix-trials.jsonl").write_text(
        json.dumps(trial, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return root


class DataAgentSemanticVerticalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.matrix = _matrix(self.root / "matrix")
        self.raw = _bundle_bytes()
        self.client = FakeDataAgentClient(self.raw)
        self.adapter = FakeContainerAdapter()
        self.registry = _registry()
        self.spec = _write_spec(self.root / "semantic-spec.json", self.raw)
        self.output = self.root / "output"
        self.verify_patch = mock.patch(
            "pathfinder.simulator.data_agent_semantic_vertical."
            "verify_flowmesh_container_matrix_plan",
            return_value={
                "status": "VERIFIED",
                "plan_sha256": MATRIX_PLAN_SHA256,
            },
        )
        self.verify_plan = self.verify_patch.start()
        self.addCleanup(self.verify_patch.stop)
        self.addCleanup(self.temporary.cleanup)

    def execute(self, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "matrix_plan_dir": self.matrix,
            "semantic_spec": self.spec,
            "endpoint_registry": self.registry,
            "clients_by_endpoint_id": {"origin": self.client},
            "adapter": self.adapter,
            "output_dir": self.output,
        }
        values.update(overrides)
        return execute_data_agent_frame_bundle_semantic_trial(**values)

    def test_complete_slice_is_bound_redacted_and_offline_verifiable(self) -> None:
        result = self.execute()

        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(1, self.client.fetch_count)
        self.assertEqual(1, self.client.telemetry_count)
        self.assertTrue(self.client.wait_for_quiescence)
        self.assertEqual(1, len(self.adapter.requests))
        request = self.adapter.requests[0]
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION_V2,
            request["schema_version"],
        )
        self.assertEqual(2, len(request["frames"]))
        for index, frame in enumerate(request["frames"]):
            self.assertEqual(index, frame["frame_index"])
            self.assertEqual(
                frame["jpeg_sha256"],
                _sha256(base64.b64decode(frame["jpeg_base64"])),
            )

        verified = verify_data_agent_frame_bundle_semantic_trial(
            output_dir=self.output,
            matrix_plan_dir=self.matrix,
            endpoint_registry=self.registry,
            semantic_spec=self.spec,
        )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(MATRIX_OBJECT_ID, verified["matrix_object_id"])
        self.assertEqual(ARTIFACT_OBJECT_ID, verified["artifact_object_id"])
        self.assertFalse(verified["execution_and_semantic_route_unified"])
        record = json.loads((self.output / RECORD_NAME).read_text())
        self.assertEqual(MATRIX_OBJECT_ID, record["matrix_object_id"])
        self.assertEqual(ARTIFACT_OBJECT_ID, record["artifact_object_id"])
        self.assertFalse(record["container_data_plane_artifact_delivery_verified"])
        self.assertTrue(record["semantic_frame_payload_integrity_verified"])
        self.assertTrue(record["data_agent_artifact_delivery_verified"])
        self.assertTrue(
            record["container_semantic_response_consistency_verified"]
        )
        self.assertFalse(
            record["container_runtime_code_provenance_verified"]
        )
        self.assertEqual("D0", record["matrix_design_id"])
        self.assertEqual(
            "D_origin_remote",
            record["data_agent_route_design_id"],
        )
        self.assertEqual("D_origin_remote", record["data_agent_plan_id"])
        self.assertEqual(3, record["data_agent_plan_epoch"])
        self.assertEqual("D_origin_remote", self.client.requests[0].plan_id)
        self.assertEqual(3, self.client.requests[0].plan_epoch)
        self.assertEqual(0, self.client.requests[0].event_index)
        self.assertEqual(0, record["event_index"])
        self.assertEqual("vision-model-test", record["expected_model"])
        self.assertEqual(record["expected_model"], record["model"])
        self.assertEqual(["A", "B", "C"], record["answer_option_ids"])
        self.assertNotIn("answer_options", record)
        evidence = (self.output / RECORD_NAME).read_text(encoding="utf-8")
        self.assertNotIn("jpeg_base64", evidence)
        self.assertNotIn("Which option describes", evidence)
        self.assertNotIn("TEST_DATA_AGENT_TOKEN", evidence)

    def test_workload_mismatch_fails_before_any_data_agent_access(self) -> None:
        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "workload_id differs",
        ):
            bad_spec = _write_spec(
                self.root / "bad-spec.json",
                self.raw,
                workload_id="other",
            )
            self.execute(semantic_spec=bad_spec)

        self.assertEqual(0, self.client.fetch_count)
        self.assertEqual(0, len(self.adapter.requests))
        self.assertFalse(self.output.exists())

    def test_semantic_executor_is_frozen_to_n6(self) -> None:
        bad_spec = _write_spec(
            self.root / "bad-executor-spec.json",
            self.raw,
            semantic_executor_node_id="N7",
        )
        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "semantic_executor_node_id must be N6",
        ):
            self.execute(semantic_spec=bad_spec)

        self.assertEqual(0, self.client.fetch_count)
        self.assertEqual(0, len(self.adapter.requests))
        self.assertFalse(self.output.exists())

    def test_frozen_spec_rejects_duplicate_keys_and_non_finite_numbers(
        self,
    ) -> None:
        document = _spec_document(self.raw)
        canonical = json.dumps(document, sort_keys=True)
        cases = (
            (
                "duplicate",
                '{"semantic_run_id":"duplicate",' + canonical[1:],
                "duplicate key",
            ),
            (
                "non-finite",
                canonical.replace(
                    '"artifact_size_bytes": ' + str(len(self.raw)),
                    '"artifact_size_bytes": NaN',
                ),
                "non-finite JSON number",
            ),
        )
        for name, content, message in cases:
            with self.subTest(name=name):
                path = self.root / f"invalid-{name}-spec.json"
                path.write_text(content, encoding="utf-8")
                with self.assertRaisesRegex(
                    DataAgentSemanticVerticalError,
                    message,
                ):
                    self.execute(semantic_spec=path)
        self.assertEqual(0, self.client.fetch_count)

    def test_missing_exact_route_fails_without_fallback(self) -> None:
        with self.assertRaisesRegex(Exception, "refusing to guess"):
            self.execute(endpoint_registry=_registry(with_route=False))

        self.assertEqual(0, self.client.fetch_count)
        self.assertFalse(self.output.exists())

    def test_repeated_artifact_download_is_not_a_canonical_observation(self) -> None:
        repeated = FakeDataAgentClient(self.raw, download_count=2)
        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "exactly one complete artifact download",
        ):
            self.execute(clients_by_endpoint_id={"origin": repeated})

        self.assertEqual(1, repeated.fetch_count)
        self.assertEqual(0, len(self.adapter.requests))
        self.assertFalse(self.output.exists())

    def test_container_result_mismatch_is_atomic(self) -> None:
        corrupt = FakeContainerAdapter(corrupt="prompt_sha256")
        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "container changed prompt_sha256",
        ):
            self.execute(adapter=corrupt)

        self.assertEqual(1, len(corrupt.requests))
        self.assertFalse(self.output.exists())

    def test_container_api_version_and_credentials_are_strict(self) -> None:
        cases = (
            ("api_version", "API version changed"),
            ("credentials_recorded", "recorded credentials"),
        )
        for event_index, (field, message) in enumerate(cases):
            with self.subTest(field=field):
                client = FakeDataAgentClient(self.raw)
                corrupt = FakeContainerAdapter(corrupt=field)
                with self.assertRaisesRegex(
                    DataAgentSemanticVerticalError,
                    message,
                ):
                    self.execute(
                        adapter=corrupt,
                        clients_by_endpoint_id={"origin": client},
                        event_index=event_index,
                    )
                self.assertFalse(self.output.exists())

    def test_container_model_must_match_frozen_expected_model(self) -> None:
        spec = _write_spec(
            self.root / "other-model-spec.json",
            self.raw,
            expected_model="different-vision-model",
        )
        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "model differs from the frozen semantic spec",
        ):
            self.execute(semantic_spec=spec)

        self.assertEqual(1, self.client.fetch_count)
        self.assertFalse(self.output.exists())

    def test_invalid_or_oversized_model_answer_is_never_persisted(self) -> None:
        cases = (
            ("prose", "The answer is [B]."),
            ("multiple", "[A] or [B]"),
            ("oversized", "A" * 65),
            (
                "question-echo",
                "Which option describes the main action?",
            ),
        )
        for event_index, (name, answer) in enumerate(cases):
            output = self.root / f"invalid-answer-{name}"
            with (
                self.subTest(name=name),
                self.assertRaises(DataAgentSemanticVerticalError),
            ):
                self.execute(
                    adapter=FakeContainerAdapter(answer=answer),
                    output_dir=output,
                    event_index=event_index,
                )
            self.assertFalse(output.exists())

    def test_declared_wrong_option_is_recorded_as_unsuccessful(self) -> None:
        result = self.execute(adapter=FakeContainerAdapter(answer="[A]"))

        self.assertEqual(0, result["task_success_count"])
        record = json.loads((self.output / RECORD_NAME).read_text())
        self.assertEqual("[A]", record["final_answer"])
        self.assertFalse(record["task_success"])
        verified = verify_data_agent_frame_bundle_semantic_trial(
            output_dir=self.output,
            matrix_plan_dir=self.matrix,
            endpoint_registry=self.registry,
            semantic_spec=self.spec,
        )
        self.assertFalse(verified["task_success"])

    def test_exact_rule_accepts_a_declared_wrong_option_without_prose(self) -> None:
        spec = _write_spec(
            self.root / "exact-spec.json",
            self.raw,
            success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        )
        result = self.execute(
            semantic_spec=spec,
            adapter=FakeContainerAdapter(answer=" A\n"),
        )

        self.assertEqual(0, result["task_success_count"])
        record = json.loads((self.output / RECORD_NAME).read_text())
        self.assertEqual("A", record["final_answer"])
        self.assertFalse(record["task_success"])

    def test_retry_uses_new_access_event_but_stable_semantic_request(self) -> None:
        corrupt = FakeContainerAdapter(corrupt="prompt_sha256")
        with self.assertRaises(DataAgentSemanticVerticalError):
            self.execute(adapter=corrupt, event_index=0)

        result = self.execute(event_index=1)

        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(2, self.client.fetch_count)
        first_access = self.client.requests[0].access_id
        second_access = self.client.requests[1].access_id
        self.assertNotEqual(first_access, second_access)
        self.assertEqual(0, self.client.requests[0].event_index)
        self.assertEqual(1, self.client.requests[1].event_index)
        self.assertEqual(1, self.client.downloads_by_access_id[first_access])
        self.assertEqual(1, self.client.downloads_by_access_id[second_access])
        self.assertEqual(
            corrupt.requests[0]["semantic_request_id"],
            self.adapter.requests[0]["semantic_request_id"],
        )
        record = json.loads((self.output / RECORD_NAME).read_text())
        self.assertEqual(1, record["event_index"])
        self.assertEqual(second_access, record["data_agent_access_id"])

    def test_existing_output_is_refused_before_remote_work(self) -> None:
        self.output.mkdir()
        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "already exists",
        ):
            self.execute()

        self.assertEqual(0, self.client.fetch_count)
        self.assertEqual(0, len(self.adapter.requests))

    def test_checksum_tampering_is_refused_offline(self) -> None:
        self.execute()
        manifest = json.loads((self.output / MANIFEST_NAME).read_text())
        manifest["task_success_count"] = 0
        (self.output / MANIFEST_NAME).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "checksum mismatch",
        ):
            verify_data_agent_frame_bundle_semantic_trial(
                output_dir=self.output,
                matrix_plan_dir=self.matrix,
                endpoint_registry=self.registry,
                semantic_spec=self.spec,
            )

    def test_semantic_request_id_tamper_fails_after_full_restamp(self) -> None:
        self.execute()
        record_path = self.output / RECORD_NAME
        record = json.loads(record_path.read_text(encoding="utf-8"))
        record["semantic_request_id"] = "f" * 64
        record_path.write_text(
            json.dumps(record, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _restamp_semantic_output(self.output)

        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "semantic request ID is not bound",
        ):
            verify_data_agent_frame_bundle_semantic_trial(
                output_dir=self.output,
                matrix_plan_dir=self.matrix,
                endpoint_registry=self.registry,
                semantic_spec=self.spec,
            )


class HttpContainerSemanticVisionAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.raw_post_response: bytes | None = None
        self.health_epochs = ["4" * 32, "4" * 32]
        self.health_credentials_recorded = False
        self.health_count = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                del format, args

            def _send(self, value: Mapping[str, Any]) -> None:
                payload = json.dumps(value).encode("utf-8")
                self._send_raw(payload)

            def _send_raw(self, payload: bytes) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self) -> None:
                if self.path != "/healthz":
                    self.send_error(404)
                    return
                epoch = owner.health_epochs[
                    min(owner.health_count, len(owner.health_epochs) - 1)
                ]
                owner.health_count += 1
                self._send({
                    "status": "ok",
                    "node_id": "N6",
                    "runtime_epoch": epoch,
                    "credentials_recorded": (
                        owner.health_credentials_recorded
                    ),
                    "semantic_quality_enabled": True,
                    "semantic_llm_configured": True,
                    "semantic_vision_request_adapter_supported": True,
                    "semantic_vision_request_schema_version": (
                        CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION_V2
                    ),
                })

            def do_POST(self) -> None:
                if self.path != "/v1/semantic/chat-completions":
                    self.send_error(404)
                    return
                length = int(self.headers["Content-Length"])
                owner.requests.append(json.loads(self.rfile.read(length)))
                if owner.raw_post_response is None:
                    self._send({"sentinel": "fake-container-result"})
                else:
                    self._send_raw(owner.raw_post_response)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.addCleanup(self._close)

    def _close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2.0)

    def adapter(self) -> HttpContainerSemanticVisionAdapter:
        port = self.server.server_port
        return HttpContainerSemanticVisionAdapter(
            semantic_url=(
                f"http://127.0.0.1:{port}/v1/semantic/chat-completions"
            ),
            health_url=f"http://127.0.0.1:{port}/healthz",
            expected_execution_node_id="N6",
            timeout_seconds=2.0,
        )

    def test_local_fake_http_is_health_gated_and_epoch_bound(self) -> None:
        adapter = self.adapter()
        request = {"schema_version": "test", "sentinel": "request"}

        result = adapter.execute(request)

        self.assertEqual("fake-container-result", result["sentinel"])
        self.assertEqual([request], self.requests)
        self.assertTrue(adapter.health_verified)
        self.assertEqual("4" * 32, adapter.last_runtime_epoch)

    def test_loopback_semantic_request_ignores_ambient_proxy(self) -> None:
        proxy_requests: list[str] = []

        class ProxyHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                del format, args

            def _reject(self) -> None:
                proxy_requests.append(self.path)
                self.send_response(502)
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_GET = _reject
            do_POST = _reject

        proxy = ThreadingHTTPServer(("127.0.0.1", 0), ProxyHandler)
        proxy_thread = threading.Thread(
            target=proxy.serve_forever,
            daemon=True,
        )
        proxy_thread.start()
        self.addCleanup(proxy_thread.join, 2.0)
        self.addCleanup(proxy.server_close)
        self.addCleanup(proxy.shutdown)
        proxy_url = f"http://127.0.0.1:{proxy.server_port}"
        request = {"schema_version": "test", "sentinel": "private"}

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
            result = self.adapter().execute(request)

        self.assertEqual("fake-container-result", result["sentinel"])
        self.assertEqual([request], self.requests)
        self.assertEqual(2, self.health_count)
        self.assertEqual([], proxy_requests)

    def test_cross_origin_health_endpoint_is_refused(self) -> None:
        port = self.server.server_port
        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "same origin",
        ):
            HttpContainerSemanticVisionAdapter(
                semantic_url=(
                    f"http://127.0.0.1:{port}/v1/semantic/chat-completions"
                ),
                health_url="http://127.0.0.1:1/healthz",
                expected_execution_node_id="N6",
            )

    def test_public_adapter_refuses_nonliteral_loopback_endpoints(self) -> None:
        port = self.server.server_port
        rejected = (
            f"http://localhost:{port}/v1/semantic/chat-completions",
            f"https://127.0.0.1:{port}/v1/semantic/chat-completions",
            "http://example.test:8080/v1/semantic/chat-completions",
            "http://127.0.0.1/v1/semantic/chat-completions",
        )
        for semantic_url in rejected:
            with (
                self.subTest(semantic_url=semantic_url),
                self.assertRaises(DataAgentSemanticVerticalError),
            ):
                HttpContainerSemanticVisionAdapter(
                    semantic_url=semantic_url,
                    health_url=f"http://127.0.0.1:{port}/healthz",
                    expected_execution_node_id="N6",
                )

    def test_duplicate_container_response_key_is_refused(self) -> None:
        self.raw_post_response = b'{"sentinel":true,"sentinel":false}'
        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "duplicate key",
        ):
            self.adapter().execute({"schema_version": "test"})

    def test_runtime_epoch_change_is_refused(self) -> None:
        self.health_epochs = ["4" * 32, "5" * 32]
        adapter = self.adapter()

        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "runtime epoch changed",
        ):
            adapter.execute({"schema_version": "test"})

        self.assertFalse(adapter.health_verified)
        self.assertIsNone(adapter.last_runtime_epoch)

    def test_health_that_records_credentials_is_refused(self) -> None:
        self.health_credentials_recorded = True
        adapter = self.adapter()

        with self.assertRaisesRegex(
            DataAgentSemanticVerticalError,
            "health reports recorded credentials",
        ):
            adapter.execute({"schema_version": "test"})

        self.assertEqual([], self.requests)
        self.assertFalse(adapter.health_verified)


if __name__ == "__main__":
    unittest.main()
