from __future__ import annotations

import base64
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.data_agent_client import (
    DataAgentAccessResult,
    DataAgentBinaryArtifact,
    DataAgentBinaryRangeArtifact,
    DataAgentClientSettings,
    DataAgentPayload,
    HttpDataAgentClient,
)
from pathfinder.simulator.container_node import (
    CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
    CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION,
)
from pathfinder.simulator.full_flow_cache import (
    CachedArtifact,
    HttpFullFlowArtifactCacheClient,
)
from pathfinder.simulator.full_flow_n6_adapters import N6SampledFrame
from pathfinder.simulator.full_flow_w4_live_executor import (
    W4LiveComponents,
    W4_ARTIFACT_ACCESS_RESULT_SCHEMA_VERSION,
    W4_SEMANTIC_RANKING_RESULT_SCHEMA_VERSION,
)
from pathfinder.simulator.full_flow_w4_local_factory import (
    DataAgentW4ArtifactAccessAdapter,
    FullFlowW4LocalFactoryError,
    InMemoryW4PayloadRegistry,
    InProcessW4ByteTransportAdapter,
    N6ContainerW4SemanticRankingAdapter,
    RecordingW4CacheAdapter,
    W4LocalRuntimeInputs,
    build_local_w4_live_components,
    local_w4_component_claim_boundary,
)
from pathfinder.simulator.index_service import (
    INDEX_SOURCE_SCHEMA_VERSION,
    N2IndexHTTPClient,
    build_n2_index_package,
)


MODEL = "qwen3.8-27b"
TOKEN = "test-only-token-1234567890"
JPEG = base64.b64decode(
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBAUEBAYFBQUGBgYHCQ4JCQgICRINDQoOFRIW"
    "FhUSFBQXGiEcFxgfGRQUHScdHyIjJSUlFhwpLCgkKyEkJST/2wBDAQYGBgkICREJCREkGBQY"
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/"
    "wAARCAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAA"
    "AAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA"
    "/9oADAMBAAIRAxEAPwCdAAyqX//Z",
    validate=True,
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _frame(index: int) -> N6SampledFrame:
    marker = b"w4-frame-" + str(index).encode("ascii")
    payload = (
        JPEG[:2]
        + b"\xff\xfe"
        + (len(marker) + 2).to_bytes(2, "big")
        + marker
        + JPEG[2:]
    )
    return N6SampledFrame(index, float(index), 2, 2, payload)


class FakeSampler:
    def __init__(self):
        self.calls = []

    def __call__(
        self,
        payload,
        *,
        object_id,
        source_payload_sha256,
        frame_count,
        jpeg_max_dimension,
    ):
        self.calls.append({
            "payload_sha256": _sha(payload),
            "object_id": object_id,
            "source_payload_sha256": source_payload_sha256,
            "frame_count": frame_count,
            "jpeg_max_dimension": jpeg_max_dimension,
        })
        return tuple(_frame(index) for index in range(frame_count))


class FakeSemanticClient:
    def __init__(self, ranking):
        self.ranking = list(ranking)
        self.requests = []
        self.epoch = "1" * 32

    def health(self):
        return {
            "status": "ok",
            "node_id": "N6",
            "runtime_epoch": self.epoch,
            "semantic_quality_enabled": True,
            "semantic_llm_configured": True,
            "semantic_vision_request_adapter_supported": True,
            "credentials_recorded": False,
        }

    def execute(self, request):
        request = dict(request)
        self.requests.append(request)
        answer = json.dumps(self.ranking, separators=(",", ":"))
        visual = (
            request["schema_version"]
            == CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION
        )
        result = {
            "schema_version": (
                CONTAINER_NODE_SEMANTIC_VISION_RESULT_SCHEMA_VERSION
                if visual
                else CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION
            ),
            "status": "completed",
            "outcome_type": "completed",
            "semantic_request_id": request["semantic_request_id"],
            "execution_node_id": "N6",
            "request_sha256": _sha(_canonical(request)),
            "prompt_sha256": request["prompt_sha256"],
            "representation_sha256": request["representation_sha256"],
            "model": MODEL,
            "final_answer": answer,
            "final_answer_sha256": _sha(answer.encode("utf-8")),
            "service_time_ms": 12.5,
            "telemetry_complete": True,
            "llm_called": True,
            "idempotent_replay": False,
            "credentials_recorded": False,
        }
        if visual:
            result.update({
                "frame_sequence_sha256": request["frame_sequence_sha256"],
                "frame_count": len(request["frames"]),
                "semantic_frame_payload_integrity_verified": True,
            })
        return result


class FakeDataAgentClient(HttpDataAgentClient):
    def __init__(
        self,
        *,
        identity,
        payload,
        location,
        inline_media_type="text/plain",
    ):
        self.settings = DataAgentClientSettings(
            base_url="http://127.0.0.1:18083",
            token=TOKEN,
        )
        self.identity = identity
        self.payload_bytes = payload
        self.location = location
        self.inline_media_type = inline_media_type
        self.requests = []

    def access(self, request):
        self.requests.append(("inline", request))
        return DataAgentAccessResult(
            access_id=request.access_id,
            payload=DataAgentPayload(
                kind="inline_text",
                media_type=self.inline_media_type,
                value=self.payload_bytes.decode("utf-8"),
                sha256=_sha(self.payload_bytes),
            ),
            service_latency_ms=1.0,
            realized_cost=0.0,
            bytes_read=len(self.payload_bytes),
            location=self.location,
            object_id=request.object_id,
            object_catalog_version=self.identity["object_catalog_version"],
        )

    def fetch_binary_artifact(self, request, **_kwargs):
        self.requests.append(("full", request))
        return DataAgentBinaryArtifact(
            access_id=request.access_id,
            media_type="video/mp4",
            data=self.payload_bytes,
            size_bytes=len(self.payload_bytes),
            sha256=_sha(self.payload_bytes),
            object_id=request.object_id,
            object_catalog_version=self.identity["object_catalog_version"],
            location=self.location,
            service_latency_ms=1.0,
        )

    def fetch_binary_artifact_range(
        self,
        request,
        *,
        range_start,
        range_end,
        expected_range_sha256,
        **_kwargs,
    ):
        self.requests.append(("range", request))
        data = self.payload_bytes[range_start : range_end + 1]
        self.asserted_range_sha = expected_range_sha256
        return DataAgentBinaryRangeArtifact(
            access_id=request.access_id,
            media_type="video/mp4",
            data=data,
            range_start=range_start,
            range_end=range_end,
            range_size_bytes=len(data),
            range_sha256=_sha(data),
            full_artifact_size_bytes=len(self.payload_bytes),
            full_artifact_sha256=_sha(self.payload_bytes),
            object_id=request.object_id,
            object_catalog_version=self.identity["object_catalog_version"],
            location=self.location,
            service_latency_ms=1.0,
        )


class FakeCacheClient(HttpFullFlowArtifactCacheClient):
    def __init__(self, artifact=None):
        self.artifact = artifact
        self.put_calls = []

    def get(self, **_kwargs):
        return self.artifact

    def put(self, **kwargs):
        self.put_calls.append(kwargs)
        return {
            "status": "STORED",
            "content_sha256": _sha(kwargs["payload"]),
            "size_bytes": len(kwargs["payload"]),
            "credentials_recorded": False,
        }


class FullFlowW4LocalFactoryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = self.root / "index-source.json"
        source.write_text(
            json.dumps({
                "schema_version": INDEX_SOURCE_SCHEMA_VERSION,
                "index_id": "w4-index-v1",
                "logical_node_id": "N2",
                "documents": [
                    {
                        "object_id": "candidate-a",
                        "source_object_group": "group-a",
                        "visible_fields": {
                            "digest": "red vehicle crossing bridge",
                        },
                    },
                    {
                        "object_id": "candidate-b",
                        "source_object_group": "group-b",
                        "visible_fields": {
                            "digest": "musicians performing indoors",
                        },
                    },
                ],
                "credentials_recorded": False,
            }),
            encoding="utf-8",
        )
        self.index_dirs = {}
        canonical = self.root / "index-N2"
        build_n2_index_package(source, output_dir=canonical)
        self.index_dirs["N2"] = canonical
        for node in ("N7", "N8"):
            target = self.root / f"index-{node}"
            shutil.copytree(canonical, target)
            self.index_dirs[node] = target

    def runtime(self, **changes):
        values = {
            "index_base_urls": {
                "N2": "http://127.0.0.1:18082",
                "N7": "http://127.0.0.1:18087",
                "N8": "http://127.0.0.1:18088",
            },
            "index_bearer_tokens": {node: TOKEN for node in ("N2", "N7", "N8")},
            "index_package_dirs": self.index_dirs,
            "data_agent_base_urls": {
                "N3": "http://127.0.0.1:18183",
                "N4": "http://127.0.0.1:18184",
            },
            "data_agent_bearer_tokens": {"N3": TOKEN, "N4": TOKEN},
            "data_agent_locations": {
                "N3": "origin-cold",
                "N4": "origin-warm",
            },
            "cache_base_urls": {
                "N7": "http://127.0.0.1:18287",
                "N8": "http://127.0.0.1:18288",
            },
            "cache_bearer_tokens": {"N7": TOKEN, "N8": TOKEN},
            "cache_ids": {"N7": "w4-cache-n7", "N8": "w4-cache-n8"},
            "n6_base_url": "http://127.0.0.1:18086",
            "n6_bearer_token": TOKEN,
            "semantic_model": MODEL,
            "raw_sampler_scratch_dir": self.root / "scratch",
        }
        values.update(changes)
        return W4LocalRuntimeInputs(**values)

    def test_factory_builds_exact_local_components_without_network(self):
        runtime = self.runtime()
        client = FakeSemanticClient(["candidate-a", "candidate-b"])
        components = build_local_w4_live_components(
            runtime,
            raw_video_sampler=FakeSampler(),
            semantic_client=client,
        )
        self.assertIsInstance(components, W4LiveComponents)
        self.assertEqual({"N2", "N7", "N8"}, set(components.indexes))
        self.assertEqual({"N3", "N4"}, set(components.artifacts))
        self.assertEqual({"N7", "N8"}, set(components.caches))
        self.assertTrue(all(
            isinstance(value.adapter, N2IndexHTTPClient)
            for value in components.indexes.values()
        ))
        self.assertIsInstance(components.transport, InProcessW4ByteTransportAdapter)
        rendered = repr(runtime)
        self.assertNotIn(TOKEN, rendered)
        self.assertNotIn("127.0.0.1", rendered)
        with self.assertRaises(TypeError):
            runtime.index_base_urls["N2"] = "http://127.0.0.1:1"
        with self.assertRaises(TypeError):
            runtime.data_agent_bearer_tokens["N3"] = "changed-token-value"

    def test_runtime_rejects_missing_node_and_mismatched_index(self):
        with self.assertRaisesRegex(
            FullFlowW4LocalFactoryError,
            "index_base_urls node set changed",
        ):
            self.runtime(index_base_urls={"N2": "http://127.0.0.1:1"})

        unsafe_urls = dict(self.runtime().index_base_urls)
        unsafe_urls["N2"] = "https://user:secret@example.test"
        with self.assertRaisesRegex(
            FullFlowW4LocalFactoryError,
            "credential-free HTTP",
        ):
            self.runtime(index_base_urls=unsafe_urls)

        other_source = self.root / "other-source.json"
        other_source.write_text(json.dumps({
            "schema_version": INDEX_SOURCE_SCHEMA_VERSION,
            "index_id": "other-index",
            "logical_node_id": "N2",
            "documents": [{
                "object_id": "candidate-a",
                "source_object_group": "other-group",
                "visible_fields": {"digest": "other visible text"},
            }],
            "credentials_recorded": False,
        }), encoding="utf-8")
        other = self.root / "other-index"
        build_n2_index_package(other_source, output_dir=other)
        packages = dict(self.index_dirs)
        packages["N8"] = other
        with self.assertRaisesRegex(
            FullFlowW4LocalFactoryError,
            "N8 index package differs",
        ):
            build_local_w4_live_components(
                self.runtime(index_package_dirs=packages),
                raw_video_sampler=FakeSampler(),
                semantic_client=FakeSemanticClient(["candidate-a"]),
            )

    def test_data_agent_adapter_translates_full_inline_and_range_access(self):
        registry = InMemoryW4PayloadRegistry(max_artifact_bytes=4096)
        payloads = {
            "multimodal_digest": b"visible digest",
            "raw_video": b"0123456789abcdef",
        }
        for representation, payload in payloads.items():
            node = "N4" if representation == "multimodal_digest" else "N3"
            identity = {
                "representation_id": representation,
                "artifact_sha256": _sha(payload),
                "artifact_size_bytes": len(payload),
                "object_catalog_version": "catalog-v1",
                "data_agent_plan_ids": ["D0", "D2"],
            }
            client = FakeDataAgentClient(
                identity=identity,
                payload=payload,
                location="origin-warm" if node == "N4" else "origin-cold",
            )
            adapter = DataAgentW4ArtifactAccessAdapter(
                node_id=node,
                client=client,
                location=client.location,
                binary_media_types={
                    "raw_video": ("video/mp4",),
                    "sampled_frame_bundle": ("application/x-tar",),
                },
                payload_registry=registry,
            )
            exact_range = None
            plan = "D2"
            expected_payload = payload
            if representation == "raw_video":
                expected_payload = payload[2:8]
                exact_range = {
                    "range_start": 2,
                    "range_end": 7,
                    "range_sha256": _sha(expected_payload),
                }
                plan = "D0"
            request = {
                "execution_token": _sha((node + representation).encode()),
                "operation_key": f"trial|{node}|access",
                "node_id": node,
                "object_id": "candidate-a",
                "artifact_identity": identity,
                "exact_content_range": exact_range,
                "data_agent_plan_id": plan,
                "credentials_recorded": False,
            }
            result = adapter.access(request)
            self.assertEqual(W4_ARTIFACT_ACCESS_RESULT_SCHEMA_VERSION,
                             result["schema_version"])
            self.assertEqual(expected_payload, result["payload"])
            self.assertEqual(_sha(expected_payload), result["content_sha256"])
            self.assertFalse(result["flowmesh_workflow_submitted"])
            descriptor = {
                "object_id": "candidate-a",
                "representation_id": representation,
                "content_sha256": _sha(expected_payload),
                "size_bytes": len(expected_payload),
            }
            self.assertEqual(expected_payload, registry.resolve(descriptor))

        digest = b"visible digest"
        identity = {
            "representation_id": "multimodal_digest",
            "artifact_sha256": _sha(digest),
            "artifact_size_bytes": len(digest),
            "object_catalog_version": "catalog-v1",
            "data_agent_plan_ids": ["D2"],
        }
        unsafe_client = FakeDataAgentClient(
            identity=identity,
            payload=digest,
            location="origin-warm",
            inline_media_type="application/octet-stream",
        )
        unsafe = DataAgentW4ArtifactAccessAdapter(
            node_id="N4",
            client=unsafe_client,
            location="origin-warm",
            binary_media_types={
                "raw_video": ("video/mp4",),
                "sampled_frame_bundle": ("application/x-tar",),
            },
            payload_registry=registry,
        )
        with self.assertRaisesRegex(
            FullFlowW4LocalFactoryError,
            "bound inline-text artifact",
        ):
            unsafe.access({
                "execution_token": "d" * 64,
                "operation_key": "trial|N4|unsafe-digest",
                "node_id": "N4",
                "object_id": "candidate-a",
                "artifact_identity": identity,
                "exact_content_range": None,
                "data_agent_plan_id": "D2",
                "credentials_recorded": False,
            })

    def test_cache_wrapper_registers_hit_and_insert_payloads(self):
        payload = b"cached-visible-digest"
        cached = CachedArtifact(
            cache_id="cache-n7",
            node_id="N7",
            cache_key="a" * 64,
            object_id="candidate-a",
            representation_id="multimodal_digest",
            content_sha256=_sha(payload),
            size_bytes=len(payload),
            payload=payload,
            event_id=1,
        )
        registry = InMemoryW4PayloadRegistry(max_artifact_bytes=4096)
        wrapper = RecordingW4CacheAdapter(FakeCacheClient(cached), registry)
        self.assertIs(cached, wrapper.get(
            object_id="candidate-a",
            representation_id="multimodal_digest",
            expected_sha256=_sha(payload),
        ))
        self.assertEqual(payload, registry.resolve({
            "object_id": "candidate-a",
            "representation_id": "multimodal_digest",
            "content_sha256": _sha(payload),
            "size_bytes": len(payload),
        }))

    def semantic_request(self, descriptors, fallback=None):
        return {
            "execution_token": "a" * 64,
            "operation_key": "trial|rank",
            "action": "rank-complete-candidate-set",
            "query_id": "query-v1",
            "query_text": "Which candidate shows a bridge crossing?",
            "candidate_object_ids": ["candidate-a", "candidate-b"],
            "candidate_inputs": descriptors,
            "fallback_ranking": fallback,
            "credentials_recorded": False,
        }

    def test_n6_digest_ranking_is_complete_and_runtime_epoch_bound(self):
        registry = InMemoryW4PayloadRegistry(max_artifact_bytes=4096)
        descriptors = []
        for object_id, payload in (
            ("candidate-a", b"vehicle bridge"),
            ("candidate-b", b"indoor music"),
        ):
            registry.record(
                object_id=object_id,
                representation_id="multimodal_digest",
                content_sha256=_sha(payload),
                payload=payload,
            )
            descriptors.append({
                "object_id": object_id,
                "representation_id": "multimodal_digest",
                "content_sha256": _sha(payload),
                "size_bytes": len(payload),
            })
        client = FakeSemanticClient(["candidate-b", "candidate-a"])
        ranker = N6ContainerW4SemanticRankingAdapter(
            payload_registry=registry,
            semantic_client=client,
            expected_model=MODEL,
            raw_video_sampler=FakeSampler(),
            max_artifact_bytes=4096,
        )
        request = self.semantic_request(descriptors)
        result = ranker.rank(request)
        self.assertEqual(
            W4_SEMANTIC_RANKING_RESULT_SCHEMA_VERSION,
            result["schema_version"],
        )
        self.assertEqual(["candidate-b", "candidate-a"],
                         result["ranked_object_ids"])
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
            client.requests[0]["schema_version"],
        )
        self.assertLessEqual(
            len(client.requests[0]["prompt"].encode("utf-8")),
            1024 * 1024,
        )
        self.assertFalse(result["flowmesh_workflow_submitted"])

        second = dict(request)
        second["execution_token"] = "b" * 64
        ranker.rank(second)
        self.assertNotEqual(
            client.requests[0]["semantic_request_id"],
            client.requests[1]["semantic_request_id"],
        )

    def test_n6_visual_ranks_prefix_then_preserves_fallback_tail(self):
        registry = InMemoryW4PayloadRegistry(max_artifact_bytes=4096)
        descriptors = []
        sampler = FakeSampler()
        for object_id, payload in (
            ("candidate-a", b"raw-a"),
            ("candidate-b", b"raw-b"),
        ):
            registry.record(
                object_id=object_id,
                representation_id="raw_video",
                content_sha256=_sha(payload),
                payload=payload,
            )
            descriptors.append({
                "object_id": object_id,
                "representation_id": "raw_video",
                "content_sha256": _sha(payload),
                "size_bytes": len(payload),
            })
        client = FakeSemanticClient(["candidate-b", "candidate-a"])
        ranker = N6ContainerW4SemanticRankingAdapter(
            payload_registry=registry,
            semantic_client=client,
            expected_model=MODEL,
            raw_video_sampler=sampler,
            max_frames_per_candidate=2,
            max_total_frames=4,
            max_artifact_bytes=4096,
        )
        result = ranker.rank(self.semantic_request(descriptors))
        self.assertEqual(["candidate-b", "candidate-a"],
                         result["ranked_object_ids"])
        semantic = client.requests[0]
        self.assertEqual(
            CONTAINER_NODE_SEMANTIC_VISION_REQUEST_SCHEMA_VERSION,
            semantic["schema_version"],
        )
        self.assertEqual(4, len(semantic["frames"]))
        self.assertLessEqual(
            max(row["jpeg_size_bytes"] for row in semantic["frames"]),
            512 * 1024,
        )
        self.assertLessEqual(
            sum(row["jpeg_size_bytes"] for row in semantic["frames"]),
            1024 * 1024,
        )
        self.assertLessEqual(len(_canonical(semantic)), 2 * 1024 * 1024)
        self.assertEqual(2, len(sampler.calls))

        one = [descriptors[0]]
        one_client = FakeSemanticClient(["candidate-a"])
        prefix_ranker = N6ContainerW4SemanticRankingAdapter(
            payload_registry=registry,
            semantic_client=one_client,
            expected_model=MODEL,
            raw_video_sampler=sampler,
            max_artifact_bytes=4096,
        )
        composed = prefix_ranker.rank(self.semantic_request(
            one,
            fallback=["candidate-a", "candidate-b"],
        ))
        self.assertEqual(["candidate-a", "candidate-b"],
                         composed["ranked_object_ids"])

    def test_n6_rejects_incomplete_or_restamped_ranking(self):
        payload = b"visible digest"
        registry = InMemoryW4PayloadRegistry(max_artifact_bytes=4096)
        registry.record(
            object_id="candidate-a",
            representation_id="multimodal_digest",
            content_sha256=_sha(payload),
            payload=payload,
        )
        descriptor = {
            "object_id": "candidate-a",
            "representation_id": "multimodal_digest",
            "content_sha256": _sha(payload),
            "size_bytes": len(payload),
        }
        ranker = N6ContainerW4SemanticRankingAdapter(
            payload_registry=registry,
            semantic_client=FakeSemanticClient([]),
            expected_model=MODEL,
            raw_video_sampler=FakeSampler(),
            max_artifact_bytes=4096,
        )
        with self.assertRaisesRegex(
            FullFlowW4LocalFactoryError,
            "complete prepared-candidate permutation",
        ):
            ranker.rank(self.semantic_request(
                [descriptor],
                fallback=["candidate-a", "candidate-b"],
            ))

    def test_n6_rejects_payloads_outside_real_container_limits(self):
        registry = InMemoryW4PayloadRegistry(max_artifact_bytes=2 * 1024 * 1024)
        descriptors = []
        for object_id in ("candidate-a", "candidate-b"):
            payload = (object_id.encode("ascii") + b" ") * 46_000
            registry.record(
                object_id=object_id,
                representation_id="multimodal_digest",
                content_sha256=_sha(payload),
                payload=payload,
            )
            descriptors.append({
                "object_id": object_id,
                "representation_id": "multimodal_digest",
                "content_sha256": _sha(payload),
                "size_bytes": len(payload),
            })
        client = FakeSemanticClient(["candidate-a", "candidate-b"])
        ranker = N6ContainerW4SemanticRankingAdapter(
            payload_registry=registry,
            semantic_client=client,
            expected_model=MODEL,
            raw_video_sampler=FakeSampler(),
            max_digest_bytes_per_candidate=700 * 1024,
            max_artifact_bytes=2 * 1024 * 1024,
        )
        with self.assertRaisesRegex(
            FullFlowW4LocalFactoryError,
            "semantic prompt limit",
        ):
            ranker.rank(self.semantic_request(descriptors))
        self.assertEqual([], client.requests)

        raw = b"raw-video"
        raw_registry = InMemoryW4PayloadRegistry(max_artifact_bytes=4096)
        raw_registry.record(
            object_id="candidate-a",
            representation_id="raw_video",
            content_sha256=_sha(raw),
            payload=raw,
        )
        comment = (
            b"\xff\xfe"
            + (65_533).to_bytes(2, "big")
            + b"x" * 65_531
        )
        oversized_jpeg = JPEG[:2] + comment * 9 + JPEG[2:]

        def oversized_sampler(*_args, **_kwargs):
            return (N6SampledFrame(0, 0.0, 2, 2, oversized_jpeg),)

        visual_ranker = N6ContainerW4SemanticRankingAdapter(
            payload_registry=raw_registry,
            semantic_client=FakeSemanticClient(["candidate-a"]),
            expected_model=MODEL,
            raw_video_sampler=oversized_sampler,
            max_frames_per_candidate=1,
            max_total_frames=1,
            max_artifact_bytes=4096,
        )
        with self.assertRaisesRegex(
            FullFlowW4LocalFactoryError,
            "per-frame byte limit",
        ):
            visual_ranker.rank(self.semantic_request([{
                "object_id": "candidate-a",
                "representation_id": "raw_video",
                "content_sha256": _sha(raw),
                "size_bytes": len(raw),
            }], fallback=["candidate-a", "candidate-b"]))

    def test_claim_boundary_never_upgrades_local_control_to_network_evidence(self):
        claim = local_w4_component_claim_boundary()
        self.assertEqual(
            "in-process-public-control-only",
            claim["n1_admission_and_return"],
        )
        self.assertEqual(
            "in-process-byte-preserving-only",
            claim["inter_stage_transport"],
        )
        self.assertFalse(claim["flowmesh_workflow_submitted_by_factory"])
        self.assertFalse(claim["network_performance_measured"])
        self.assertFalse(claim["eligible_for_scientific_claims"])


if __name__ == "__main__":
    unittest.main()
