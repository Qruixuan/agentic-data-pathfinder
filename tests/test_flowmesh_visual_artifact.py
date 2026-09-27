"""No-inference tests for the Pathfinder-owned visual access bridge."""

from __future__ import annotations

import base64
import json
import tempfile
import unittest
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pathfinder.data_agent_client import (
    DataAgentAccessResult,
    DataAgentBinaryArtifact,
    DataAgentPayload,
)
from pathfinder.config import load_config
from pathfinder.distributed.registry import (
    ENDPOINT_REGISTRY_SCHEMA_VERSION,
    EndpointRegistryError,
    build_endpoint_registry,
)
from pathfinder.distributed.routing import (
    CrossEndpointArtifactError,
    RoutedDataAgentBackend,
)
from pathfinder.integrations.flowmesh.gateway import (
    AccessGateway,
    ArtifactFetchUnsupportedError,
    GatewayAccessEvent,
    GatewaySession,
    SQLiteSessionStore,
)
from pathfinder.integrations.flowmesh.data_agent_backend import (
    RemoteDataAgentBackend,
)
from pathfinder.integrations.flowmesh.mcp_server import _build_server
from pathfinder.integrations.flowmesh.visual_artifact import (
    N6VisualInferenceClient,
    VisualArtifactError,
    visual_request,
)
from pathfinder.simulator.container_node import (
    CONTAINER_NODE_SEMANTIC_VIDEO_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
    semantic_frame_sequence_sha256,
)
from pathfinder.models import PathSpec, PhysicalDesign, Representation
from pathfinder.integrations.flowmesh.contracts import FlowMeshAgentRunRequest
from test_frame_bundle_transfer import build_bundle


def artifact(data: bytes, media_type: str) -> DataAgentBinaryArtifact:
    return DataAgentBinaryArtifact(
        access_id="access-1",
        media_type=media_type,
        data=data,
        size_bytes=len(data),
        sha256=sha256(data).hexdigest(),
        object_id="video-1",
    )


class VisualRequestTest(unittest.TestCase):
    def test_dedicated_worker_config_uses_visual_tool(self) -> None:
        root = Path(__file__).resolve().parents[1]
        config = (
            root / "integrations" / "flowmesh" / "agent_configs"
            / "pathfinder_video_visual_cost_aware.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("      - inspect_visual_artifact\n", config)
        self.assertIn("      - fetch_artifact\n", config)
        self.assertIn("Do not claim you saw the video directly", config)

    def test_visual_mcp_tool_is_opt_in(self) -> None:
        class FastMCP:
            def __init__(self, *_args: object, **_kwargs: object):
                self.tools: dict[str, object] = {}

            def tool(self):
                def register(function):
                    self.tools[function.__name__] = function
                    return function
                return register

        root = Path(__file__).resolve().parents[1]
        config_path = root / "configs" / "phase_b_causal_gate_system.json"
        with tempfile.TemporaryDirectory() as directory:
            server = _build_server(
                None, config_path=config_path,
                state_db=Path(directory) / "gateway.sqlite3",
                host="127.0.0.1", port=8765, fast_mcp=FastMCP,
                visual_inference_client=_N6(),
            )
        self.assertIn("inspect_visual_artifact", server.tools)

    def test_raw_video_is_real_base64_media_not_a_text_summary(self) -> None:
        raw = b"\x00\x00\x00\x18ftypmp42-video-bytes"
        request = visual_request(
            session_id="session-1",
            question="Which option?",
            object_id="video-1",
            representation_id="raw_video",
            artifact=artifact(raw, "video/mp4"),
        )
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_VIDEO_REQUEST_SCHEMA_VERSION,
            request["schema_version"],
        )
        self.assertEqual(raw, base64.b64decode(request["video_base64"]))
        self.assertEqual(sha256(raw).hexdigest(), request["video_sha256"])
        self.assertNotIn("digest_text", request)
        self.assertNotIn("signed_url", json.dumps(request))
        self.assertEqual(
            request["semantic_request_id"],
            visual_request(
                session_id="session-1",
                question="Which option?",
                object_id="video-1",
                representation_id="raw_video",
                artifact=artifact(raw, "video/mp4"),
            )["semantic_request_id"],
        )

    def test_frame_bundle_is_verified_before_n6_request(self) -> None:
        raw = build_bundle(object_id="video-1", frame_count=3)
        request = visual_request(
            session_id="session-1",
            question="Which option?",
            object_id="video-1",
            representation_id="sampled_frame_bundle",
            artifact=artifact(raw, "application/x-tar"),
        )
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
            request["schema_version"],
        )
        self.assertEqual(3, len(request["frames"]))
        self.assertEqual(
            semantic_frame_sequence_sha256(request["frames"]),
            request["frame_sequence_sha256"],
        )
        self.assertEqual(
            [0, 1, 2], [item["frame_index"] for item in request["frames"]],
        )

    def test_wrong_media_or_digest_fails_before_inference(self) -> None:
        raw = artifact(b"video", "video/mp4")
        bad = DataAgentBinaryArtifact(
            access_id=raw.access_id,
            media_type=raw.media_type,
            data=raw.data,
            size_bytes=raw.size_bytes,
            sha256="0" * 64,
            object_id=raw.object_id,
        )
        for value, representation in (
            (raw, "sampled_frame_bundle"),
            (bad, "raw_video"),
        ):
            with self.subTest(representation=representation):
                with self.assertRaises(VisualArtifactError):
                    visual_request(
                        session_id="session-1",
                        question="Which option?",
                        object_id="video-1",
                        representation_id=representation,
                        artifact=value,
                    )


class _Response:
    status = 200

    def __init__(self, body: bytes):
        self.body = body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _limit: int) -> bytes:
        return self.body


class _Opener:
    def __init__(self, response: dict[str, object]):
        self.response = response
        self.wire: object | None = None

    def open(self, wire: object, *, timeout: float) -> _Response:
        self.wire = wire
        self.response["request_sha256"] = sha256(wire.data).hexdigest()
        return _Response(json.dumps(self.response).encode("utf-8"))


class N6ResultBindingTest(unittest.TestCase):
    def test_n6_result_is_bound_without_returning_media_or_token(self) -> None:
        raw = artifact(b"\x00\x00\x00\x18ftypmp42-video-bytes", "video/mp4")
        request = visual_request(
            session_id="session-1", question="Which option?",
            object_id="video-1", representation_id="raw_video",
            artifact=raw,
        )
        response = {
            "status": "completed",
            "semantic_request_id": request["semantic_request_id"],
            "representation_sha256": raw.sha256,
            "llm_called": True,
            "final_answer": "B",
            "final_answer_sha256": sha256(b"B").hexdigest(),
            "provider_usage": {"prompt_tokens": 42},
        }
        opener = _Opener(response)
        with patch(
            "pathfinder.integrations.flowmesh.visual_artifact.build_opener",
            return_value=opener,
        ):
            result = N6VisualInferenceClient(
                "http://pathfinder-full-flow-n6-ppd-inference:8780",
                "test-only-token",
                private_http_service_name=(
                    "pathfinder-full-flow-n6-ppd-inference"
                ),
            ).infer(request)
        self.assertEqual("B", result["final_answer"])
        self.assertEqual(42, result["provider_usage"]["prompt_tokens"])
        self.assertNotIn("video_base64", result)
        self.assertNotIn("test-only-token", json.dumps(result))

    def test_n6_result_mismatch_fails_closed(self) -> None:
        raw = artifact(b"\x00\x00\x00\x18ftypmp42-video-bytes", "video/mp4")
        request = visual_request(
            session_id="session-1", question="Which option?",
            object_id="video-1", representation_id="raw_video",
            artifact=raw,
        )
        response = {
            "status": "completed",
            "semantic_request_id": "wrong-request",
            "representation_sha256": raw.sha256,
            "llm_called": True,
            "final_answer": "B",
            "final_answer_sha256": sha256(b"B").hexdigest(),
        }
        with patch(
            "pathfinder.integrations.flowmesh.visual_artifact.build_opener",
            return_value=_Opener(response),
        ):
            with self.assertRaisesRegex(
                VisualArtifactError, "does not bind",
            ):
                N6VisualInferenceClient(
                    "http://pathfinder-full-flow-n6-ppd-inference:8780",
                    "test-only-token",
                    private_http_service_name=(
                        "pathfinder-full-flow-n6-ppd-inference"
                    ),
                ).infer(request)

    def test_numeric_private_http_origin_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            N6VisualInferenceClient(
                "http://10.70.0.16:8780", "test-only-token"
            )


class PrivateAliasContractTest(unittest.TestCase):
    def test_only_declared_service_alias_permits_private_http(self) -> None:
        endpoint = {
            "endpoint_id": "n3",
            "node_id": "N3",
            "location": "remote-origin",
            "base_url_env": "TEST_N3_URL",
            "token_env": "TEST_N3_TOKEN",
            "private_http_service_name": "pathfinder-full-flow-n3-ppd-agent",
            "max_artifact_bytes": 7_000_000,
        }
        root = {
            "schema_version": ENDPOINT_REGISTRY_SCHEMA_VERSION,
            "registry_id": "ppd-test",
            "execution_node_id": "N7",
            "endpoints": [endpoint],
        }
        registry = build_endpoint_registry(root, source_sha256="0" * 64)
        settings = registry.endpoint("n3").client_settings({
            "TEST_N3_URL": "http://pathfinder-full-flow-n3-ppd-agent:8780",
            "TEST_N3_TOKEN": "test-only-token",
        })
        self.assertEqual(7_000_000, settings.max_artifact_bytes)
        self.assertEqual(
            ("pathfinder-full-flow-n3-ppd-agent",),
            settings.simulator_private_http_hosts,
        )
        with self.assertRaises(ValueError):
            registry.endpoint("n3").client_settings({
                "TEST_N3_URL": "http://10.70.0.13:8780",
                "TEST_N3_TOKEN": "test-only-token",
            })
        endpoint["private_http_service_name"] = "10.70.0.13"
        with self.assertRaises(EndpointRegistryError):
            build_endpoint_registry(root, source_sha256="0" * 64)


class _RoutedClient:
    def __init__(self, fetched: DataAgentBinaryArtifact):
        self.fetched = fetched
        self.calls = 0

    def fetch_binary_artifact(
        self, request: object, *, allowed_media_types: frozenset[str],
    ) -> DataAgentBinaryArtifact:
        self.calls += 1
        if allowed_media_types != frozenset({"video/mp4"}):
            raise AssertionError("wrong media allowlist")
        return DataAgentBinaryArtifact(
            **{
                **self.fetched.__dict__,
                "access_id": request.access_id,
            }
        )


class BinaryRoutingTest(unittest.TestCase):
    def test_visual_handle_cannot_move_to_another_endpoint(self) -> None:
        raw = artifact(b"video", "video/mp4")
        client = _RoutedClient(raw)
        registry = SimpleNamespace(
            endpoint_ids=("n3",),
            endpoint=lambda _endpoint_id: SimpleNamespace(
                node_id="N3", location="remote-origin",
            ),
            route=lambda **_kwargs: SimpleNamespace(endpoint_id="n3"),
            secret_environment_names=(),
        )
        backend = RoutedDataAgentBackend(registry, {"n3": client})
        session = GatewaySession(
            session_id="session-1", trial_id="trial-1",
            question="Which option?", design_id="design-1",
            task_class_id="video_qa", quote_profile_id="quoted",
            latency_multiplier=1.0, seed=1, price_universe_version="v1",
            status="RUNNING", object_id="video-1",
        )
        event = GatewayAccessEvent(
            event_id=1, session_id="session-1", event_index=0,
            representation_id="raw_video", quoted_price=1.0,
            accepted=True, rejection_reason=None, felt_latency_ms=5.0,
            realized_cost=0.0, bytes_read=5, location="remote",
            content_sha256=raw.sha256,
            created_at="2026-09-25T00:00:00Z", object_id="video-1",
            data_agent_access_id="placeholder", endpoint_id="n4",
        )
        config = SimpleNamespace(
            designs={"design-1": SimpleNamespace(
                paths={"raw_video": SimpleNamespace(location="remote")},
            )},
            representations={
                "raw_video": SimpleNamespace(size_bytes=raw.size_bytes)
            },
        )
        with self.assertRaises(CrossEndpointArtifactError):
            backend.fetch_binary_artifact(
                config=config, session=session, event=event,
                allowed_media_types=frozenset({"video/mp4"}),
            )
        self.assertEqual(0, client.calls)


class _Store:
    def __init__(self, session: GatewaySession, event: GatewayAccessEvent):
        self.session = session
        self.event = event

    def get_session(self, session_id: str) -> GatewaySession:
        if session_id != self.session.session_id:
            raise ValueError("wrong session")
        return self.session

    def get_artifact_event(
        self, session_id: str, artifact_handle: str,
    ) -> GatewayAccessEvent:
        if session_id != self.session.session_id or artifact_handle != "handle":
            raise ValueError("wrong handle")
        return self.event


class _Backend:
    def __init__(self, fetched: DataAgentBinaryArtifact):
        self.fetched = fetched
        self.calls = 0

    def fetch_binary_artifact(self, **kwargs: object) -> DataAgentBinaryArtifact:
        self.calls += 1
        if kwargs["allowed_media_types"] != frozenset({"video/mp4"}):
            raise AssertionError("wrong media allowlist")
        return self.fetched


class _N6:
    def __init__(self):
        self.request: dict[str, object] | None = None

    def infer(self, request: dict[str, object]) -> dict[str, object]:
        self.request = request
        return {
            "final_answer": "B",
            "semantic_request_id": request["semantic_request_id"],
            "provider_usage": {"prompt_tokens": 42},
        }


class GatewayVisualBoundaryTest(unittest.TestCase):
    def setUp(self) -> None:
        raw = artifact(b"\x00\x00\x00\x18ftypmp42-video-bytes", "video/mp4")
        self.backend = _Backend(raw)
        self.n6 = _N6()
        self.session = GatewaySession(
            session_id="session-1", trial_id="trial-1",
            question="Which option?", design_id="design-1",
            task_class_id="video_qa", quote_profile_id="quoted",
            latency_multiplier=1.0, seed=1, price_universe_version="v1",
            status="RUNNING", object_id="video-1",
        )
        self.event = GatewayAccessEvent(
            event_id=1, session_id="session-1", event_index=0,
            representation_id="raw_video", quoted_price=1.0,
            accepted=True, rejection_reason=None, felt_latency_ms=5.0,
            realized_cost=0.0, bytes_read=raw.size_bytes,
            location="remote", content_sha256=raw.sha256,
            created_at="2026-09-25T00:00:00Z", object_id="video-1",
            data_agent_access_id="access-1", endpoint_id="n3",
        )
        self.gateway = AccessGateway(
            SimpleNamespace(), _Store(self.session, self.event), self.backend,
            visual_inference_client=self.n6,
        )

    def test_gateway_sends_media_only_to_n6(self) -> None:
        output = self.gateway.inspect_visual_artifact("session-1", "handle")
        self.assertEqual("B", output["final_answer"])
        self.assertEqual(1, self.backend.calls)
        self.assertIsNotNone(self.n6.request)
        self.assertNotIn("video_base64", output)
        self.assertNotIn("ftyp", json.dumps(output))
        self.assertNotIn('"artifact_handle": "handle"', json.dumps(output))

    def test_non_visual_access_fails_before_download(self) -> None:
        self.event = GatewayAccessEvent(
            **{**self.event.to_dict(), "representation_id": "multimodal_digest"}
        )
        self.gateway.store.event = self.event
        with self.assertRaises(ArtifactFetchUnsupportedError):
            self.gateway.inspect_visual_artifact("session-1", "handle")
        self.assertEqual(0, self.backend.calls)

    def test_unbound_digest_fails_before_download_or_n6(self) -> None:
        self.event = GatewayAccessEvent(
            **{**self.event.to_dict(), "content_sha256": None}
        )
        self.gateway.store.event = self.event
        with self.assertRaisesRegex(
            RuntimeError, "complete content/object binding",
        ):
            self.gateway.inspect_visual_artifact("session-1", "handle")
        self.assertEqual(0, self.backend.calls)
        self.assertIsNone(self.n6.request)


class _AccessClient:
    def __init__(self, video: bytes):
        self.video = video
        self.accesses = 0
        self.downloads = 0

    def access(self, request):
        self.accesses += 1
        return DataAgentAccessResult(
            access_id=request.access_id,
            payload=DataAgentPayload(
                kind="artifact_uri", media_type="video/mp4",
                value="http://signed-url-never-returned.invalid/artifact",
                sha256=sha256(self.video).hexdigest(),
            ),
            service_latency_ms=5.0, realized_cost=0.1,
            bytes_read=len(self.video), location="origin",
            object_id=request.object_id, object_catalog_version="v1",
        )

    def fetch_binary_artifact(self, request, *, allowed_media_types):
        self.downloads += 1
        if allowed_media_types != frozenset({"video/mp4"}):
            raise AssertionError("wrong binary media allowlist")
        return DataAgentBinaryArtifact(
            access_id=request.access_id, media_type="video/mp4",
            data=self.video, size_bytes=len(self.video),
            sha256=sha256(self.video).hexdigest(),
            object_id=request.object_id,
        )


class AcceptedVisualAccessLifecycleTest(unittest.TestCase):
    def test_offer_access_handle_visual_inference_is_bound(self) -> None:
        root = Path(__file__).resolve().parents[1]
        original = load_config(
            root / "configs" / "phase_b_causal_gate_system.json"
        )
        task = replace(
            original.task_classes["video_qa"],
            candidate_representations=("raw_video",),
        )
        path = PathSpec(
            available=True, location="origin", latency_ms=5.0,
            latency_jitter_ms=0.0, realized_cost=0.1,
            quotes={"video_qa": 1.0},
        )
        config = replace(
            original,
            representations={"raw_video": Representation(
                id="raw_video", description="Actual encoded video",
                size_bytes=24, task_quality={"video_qa": 0.7},
            )},
            task_classes={"video_qa": task},
            designs={"design-1": PhysicalDesign(
                id="design-1", description="test", paths={"raw_video": path},
            )},
        )
        video = b"\x00\x00\x00\x18ftypmp42-video-bytes"
        client = _AccessClient(video)
        n6 = _N6()
        with tempfile.TemporaryDirectory() as directory:
            gateway = AccessGateway(
                config,
                SQLiteSessionStore(Path(directory) / "gateway.sqlite3"),
                RemoteDataAgentBackend(client),
                visual_inference_client=n6,
            )
            session = gateway.register_session(FlowMeshAgentRunRequest(
                question="Which option?", design_id="design-1",
                task_class_id="video_qa", quote_profile_id="as_designed",
                trial_id="trial-1", session_id="session-1",
                object_id="video-1",
            ))
            offers = gateway.list_offers(session.session_id)
            self.assertEqual("raw_video", offers["offers"][0][
                "representation_id"
            ])
            accessed = gateway.access_representation(
                session.session_id, "raw_video"
            )
            self.assertTrue(accessed["ok"])
            self.assertNotIn("signed-url", json.dumps(accessed))
            result = gateway.inspect_visual_artifact(
                session.session_id, accessed["artifact_handle"]
            )
        self.assertEqual("B", result["final_answer"])
        self.assertEqual(1, client.accesses)
        self.assertEqual(1, client.downloads)
        self.assertEqual(
            video, base64.b64decode(n6.request["video_base64"])
        )


if __name__ == "__main__":
    unittest.main()
