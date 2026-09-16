from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pathfinder.data_agent_client import (
    DataAgentAccessResult,
    DataAgentBinaryArtifact,
    DataAgentPayload,
)
from pathfinder.simulator.full_flow_cache import CachedArtifact
from pathfinder.simulator.full_flow_route_adapters import (
    FullFlowRouteAdapterError,
    BoundDataAgentAccessRequestFactory,
    BoundIndexQueryAdapter,
    DataAgentArtifactSourceAdapter,
    FrozenDataAgentPlanIdCatalog,
    FrozenIndexQueryPlan,
    FrozenIndexQueryPlanCatalog,
    FrozenProvisioningReferenceAdapter,
    FullFlowRouteAdapterError,
    HttpArtifactCacheRouteAdapter,
    HttpContainerNodeSemanticClient,
    InProcessByteTransferAdapter,
    InProcessTrialControlAdapter,
    SQLiteCacheLineageStore,
    StaticDataAgentPlanIdResolver,
    VerifiedN1HTTPScoringAdapter,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    ArtifactAccess,
    ArtifactIdentity,
    ExactContentRange,
    ProvisioningReference,
    SemanticInferenceResult,
)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _value_sha(value: object) -> str:
    return _sha(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )


RAW = b"raw-video-bytes"
DIGEST = b"a visible digest\n"
BUNDLE = b"bundle-bytes"
OBJECT = "nextqa-val-4010069381"
CATALOG = "catalog-v1"
TASK_SHA = "a" * 64


def _identity(representation: str, payload: bytes) -> ArtifactIdentity:
    return ArtifactIdentity(
        object_id=OBJECT,
        representation_id=representation,
        artifact_sha256=_sha(payload),
        artifact_size_bytes=len(payload),
        object_catalog_version=CATALOG,
    )


def _trial(
    *,
    route: str = "indexed-raw",
    repetition: int = 0,
    executor: str = "N7",
) -> dict:
    raw = _identity("raw_video", RAW)
    return {
        "trial_key": f"matrix|workload|D1|r{repetition:04d}",
        "order_index": repetition,
        "route_family": route,
        "executor_node_id": executor,
        "public_task_binding": {"task_class_id": "video_qa"},
        "representation_identities": [
            {
                "artifact_object_id": raw.object_id,
                "representation_id": raw.representation_id,
                "representation_binding": {
                    "artifact_sha256": raw.artifact_sha256,
                    "artifact_size_bytes": raw.artifact_size_bytes,
                    "object_catalog_version": raw.object_catalog_version,
                },
            }
        ],
    }


def _task() -> dict:
    return {
        "question": "Which action is visible?",
        "task_binding_sha256": TASK_SHA,
    }


class _RangeCatalog:
    def resolve(self, identity: ArtifactIdentity) -> ExactContentRange:
        return ExactContentRange(
            object_id=identity.object_id,
            representation_id=identity.representation_id,
            object_catalog_version=identity.object_catalog_version,
            full_artifact_size_bytes=identity.artifact_size_bytes,
            full_artifact_sha256=identity.artifact_sha256,
            range_start=0,
            range_end=identity.artifact_size_bytes - 1,
            range_sha256=identity.artifact_sha256,
        )


class _IndexClient:
    def __init__(self, node: str = "N2") -> None:
        self.node = node
        self.requests: list[dict] = []

    def health(self) -> dict:
        return {
            "status": "ok",
            "node_id": self.node,
            "index_id": "visible-index-v1",
            "index_sha256": "b" * 64,
            "credentials_recorded": False,
        }

    def query(self, request: dict) -> dict:
        self.requests.append(request)
        result = {
            "status": "COMPLETED",
            "ranked_candidates": [{"object_id": OBJECT}],
            "lexical_retrieval_executed": True,
            "llm_called": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        result["result_content_sha256"] = _value_sha(result)
        return result


class _LocalIndexService(_IndexClient):
    pass


class _DataAgentClient:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.requests = []

    def access(self, request):
        self.requests.append(request)
        payload = self.payloads[request.representation_id]
        return DataAgentAccessResult(
            access_id=request.access_id,
            payload=DataAgentPayload(
                kind="inline_text",
                media_type="text/plain; charset=utf-8",
                value=payload.decode("utf-8"),
                sha256=_sha(payload),
            ),
            service_latency_ms=1.25,
            realized_cost=0.0,
            bytes_read=len(payload),
            location=request.binding["location"],
            object_id=request.object_id,
            object_catalog_version=CATALOG,
        )

    def fetch_binary_artifact(self, request, *, allowed_media_types):
        del allowed_media_types
        self.requests.append(request)
        payload = self.payloads[request.representation_id]
        return DataAgentBinaryArtifact(
            access_id=request.access_id,
            media_type="video/mp4",
            data=payload,
            size_bytes=len(payload),
            sha256=_sha(payload),
            object_id=request.object_id,
            object_catalog_version=CATALOG,
            location=request.binding["location"],
            service_latency_ms=2.5,
        )


class _CacheClient:
    def __init__(self, node: str, cache_id: str) -> None:
        self.node = node
        self.cache_id = cache_id
        self.values: dict[tuple[str, str], bytes] = {}
        self.event = 0

    def health(self) -> dict:
        return {
            "status": "ok",
            "node_id": self.node,
            "cache_id": self.cache_id,
            "credentials_recorded": False,
        }

    def get(self, *, object_id, representation_id, expected_sha256=None):
        payload = self.values.get((object_id, representation_id))
        if payload is None or (
            expected_sha256 is not None and _sha(payload) != expected_sha256
        ):
            return None
        self.event += 1
        return CachedArtifact(
            cache_id=self.cache_id,
            node_id=self.node,
            cache_key="c" * 64,
            object_id=object_id,
            representation_id=representation_id,
            content_sha256=_sha(payload),
            size_bytes=len(payload),
            payload=payload,
            event_id=self.event,
        )

    def put(
        self,
        *,
        request_id,
        object_id,
        representation_id,
        payload,
        expected_sha256=None,
    ):
        if expected_sha256 is not None:
            assert _sha(payload) == expected_sha256
        self.values[(object_id, representation_id)] = payload
        return {
            "status": "STORED",
            "request_id": request_id,
            "node_id": self.node,
            "cache_id": self.cache_id,
            "content_sha256": _sha(payload),
            "size_bytes": len(payload),
            "credentials_recorded": False,
        }


class _OracleClient:
    def health(self):
        return {
            "status": "ok",
            "node_id": "N1",
            "oracle_id": "oracle-v1",
            "credentials_recorded": False,
        }

    def score(self, request):
        result = {
            "oracle_id": "oracle-v1",
            "score_request_id": request["score_request_id"],
            "request_sha256": _value_sha(request),
            "prediction_sha256": _sha(
                request["predicted_answer"].encode("utf-8")
            ),
            "correct": True,
            "score": 1.0,
            "score_evidence_hmac_sha256": "d" * 64,
        }
        core = dict(result)
        result["result_content_sha256"] = _value_sha(core)
        return result


class _Verifier:
    def verify(self, *, request, result):
        return {
            "status": "VERIFIED",
            "oracle_id": result["oracle_id"],
            "score_request_id": result["score_request_id"],
            "request_sha256": result["request_sha256"],
            "prediction_sha256": result["prediction_sha256"],
            "correct": result["correct"],
            "score": result["score"],
            "hidden_answer_returned": False,
        }


class FullFlowRouteAdaptersTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def query_catalog(self, trial_key: str) -> FrozenIndexQueryPlanCatalog:
        return FrozenIndexQueryPlanCatalog([
            FrozenIndexQueryPlan(
                trial_key=trial_key,
                task_binding_sha256=TASK_SHA,
                index_id="visible-index-v1",
                query_id="visible-query-v1",
                query_text=_task()["question"],
                top_k=1,
                candidate_object_ids=(OBJECT,),
            )
        ])

    def test_n2_query_requires_and_returns_an_exact_content_range(self) -> None:
        trial = _trial()
        client = _IndexClient()
        adapter = BoundIndexQueryAdapter(
            clients={"N2": client},
            query_plans=self.query_catalog(trial["trial_key"]),
            exact_ranges=_RangeCatalog(),
        )
        result = adapter.query(
            run_id="run-v1",
            trial=trial,
            stage={"stage_key": "index", "logical_node_ids": ["N2"]},
            public_task=_task(),
            expected_object_id=OBJECT,
        )
        self.assertEqual(OBJECT, result.selected_object_id)
        self.assertIsNotNone(result.segment)
        self.assertEqual(0, result.segment.range_start)
        self.assertEqual(len(RAW) - 1, result.segment.range_end)
        self.assertEqual(_sha(RAW), result.segment.range_sha256)
        self.assertNotIn("range_fraction", client.requests[0])

    def test_local_index_is_explicitly_in_process_and_content_bound(self) -> None:
        trial = _trial(route="local-cache-derived")
        trial["representation_identities"] = []
        global_client = _IndexClient()
        local = _LocalIndexService("N7")
        adapter = BoundIndexQueryAdapter(
            clients={"N2": global_client},
            query_plans=self.query_catalog(trial["trial_key"]),
            exact_ranges=_RangeCatalog(),
            local_index_services={"N7": local},
        )
        result = adapter.query(
            run_id="run-v1",
            trial=trial,
            stage={"stage_key": "local-index", "logical_node_ids": ["N7"]},
            public_task=_task(),
            expected_object_id=OBJECT,
        )
        self.assertEqual(OBJECT, result.selected_object_id)
        self.assertEqual([], global_client.requests)
        self.assertEqual(1, len(local.requests))

    def test_executor_index_prefers_its_node_bound_http_client(self) -> None:
        trial = _trial(route="local-cache-derived")
        trial["representation_identities"] = []
        n2 = _IndexClient("N2")
        n8 = _IndexClient("N8")
        adapter = BoundIndexQueryAdapter(
            clients={"N2": n2, "N8": n8},
            query_plans=self.query_catalog(trial["trial_key"]),
            exact_ranges=_RangeCatalog(),
        )
        result = adapter.query(
            run_id="run-v1",
            trial=trial,
            stage={"stage_key": "local-index", "logical_node_ids": ["N8"]},
            public_task=_task(),
            expected_object_id=OBJECT,
        )
        self.assertEqual(OBJECT, result.selected_object_id)
        self.assertEqual([], n2.requests)
        self.assertEqual("N8", n8.requests[0]["requested_node_id"])

    def test_index_plan_must_bind_the_public_task(self) -> None:
        trial = _trial()
        adapter = BoundIndexQueryAdapter(
            clients={"N2": _IndexClient()},
            query_plans=self.query_catalog(trial["trial_key"]),
            exact_ranges=_RangeCatalog(),
        )
        changed = {**_task(), "task_binding_sha256": "f" * 64}
        with self.assertRaisesRegex(
            FullFlowRouteAdapterError, "different public task"
        ):
            adapter.query(
                run_id="run-v1",
                trial=trial,
                stage={"stage_key": "index", "logical_node_ids": ["N2"]},
                public_task=changed,
                expected_object_id=OBJECT,
            )

    def request_factory(self) -> BoundDataAgentAccessRequestFactory:
        return BoundDataAgentAccessRequestFactory(
            source_locations={"N3": "origin-cold", "N4": "origin-warm"},
            plan_ids=StaticDataAgentPlanIdResolver({
                ("N3", "raw_video"): "raw-plan-v1",
                ("N4", "multimodal_digest"): "digest-plan-v1",
                ("N4", "sampled_frame_bundle"): "frames-plan-v1",
            }),
        )

    def test_exact_data_agent_plan_catalog_rejects_another_trial(self) -> None:
        trial = _trial(route="raw")
        identity = _identity("raw_video", RAW)
        resolver = FrozenDataAgentPlanIdCatalog({
            (
                trial["trial_key"],
                "N3",
                identity.object_id,
                identity.representation_id,
            ): "D1",
        })
        self.assertEqual(
            "D1",
            resolver.resolve(
                source_node_id="N3",
                trial=trial,
                identity=identity,
            ),
        )
        changed = {**trial, "trial_key": "matrix|workload|D1|r0001"}
        with self.assertRaisesRegex(
            FullFlowRouteAdapterError,
            "exact Data Agent plan binding is missing",
        ):
            resolver.resolve(
                source_node_id="N3",
                trial=changed,
                identity=identity,
            )

    def test_data_agent_adapter_fetches_binary_and_inline_artifacts(self) -> None:
        n3 = _DataAgentClient({"raw_video": RAW})
        n4 = _DataAgentClient({
            "multimodal_digest": DIGEST,
            "sampled_frame_bundle": BUNDLE,
        })
        adapter = DataAgentArtifactSourceAdapter(
            clients={"N3": n3, "N4": n4},
            request_factory=self.request_factory(),
            allowed_media_types={
                "raw_video": ("video/mp4",),
                "sampled_frame_bundle": ("application/x-tar",),
            },
        )
        trial = _trial(route="raw")
        raw = adapter.fetch_full(
            run_id="run-v1",
            trial=trial,
            stage={
                "stage_index": 2,
                "stage_key": "raw",
                "logical_node_ids": ["N3"],
            },
            identity=_identity("raw_video", RAW),
            upstream_values=(),
        )
        digest = adapter.fetch_full(
            run_id="run-v1",
            trial=trial,
            stage={
                "stage_index": 3,
                "stage_key": "digest",
                "logical_node_ids": ["N4"],
            },
            identity=_identity("multimodal_digest", DIGEST),
            upstream_values=(),
        )
        self.assertEqual(RAW, raw.payload)
        self.assertEqual(DIGEST, digest.payload)
        self.assertEqual("origin-cold", n3.requests[0].binding["location"])
        self.assertEqual("origin-warm", n4.requests[0].binding["location"])
        self.assertEqual(_sha(RAW), raw.payload_sha256)
        self.assertEqual(_sha(DIGEST), digest.payload_sha256)

    def test_cache_adapter_preserves_miss_insert_then_paired_hit_lineage(self) -> None:
        clients = {
            "N7": _CacheClient("N7", "n7-cache"),
            "N8": _CacheClient("N8", "n8-cache"),
        }
        epochs = {
            "N7": "1" * 32,
            "N8": "2" * 32,
        }
        adapter = HttpArtifactCacheRouteAdapter(
            clients=clients,
            runtime_epoch_probes={
                node: (
                    lambda node=node: {
                        "status": "ok",
                        "node_id": node,
                        "runtime_epoch": epochs[node],
                        "credentials_recorded": False,
                    }
                )
                for node in ("N7", "N8")
            },
            lineage=SQLiteCacheLineageStore(self.root / "lineage.sqlite3"),
        )
        identity = _identity("sampled_frame_bundle", BUNDLE)
        miss_trial = _trial(route="local-cache-derived", repetition=0)
        miss = adapter.lookup(
            run_id="run-v1",
            trial=miss_trial,
            stage={"logical_node_ids": ["N7"], "stage_key": "lookup"},
            identity=identity,
        )
        self.assertEqual("miss", miss.branch)
        source = ArtifactAccess(source_identity=identity, payload=BUNDLE)
        adapter.insert(
            run_id="run-v1",
            trial=miss_trial,
            stage={"logical_node_ids": ["N7"], "stage_key": "insert"},
            identity=identity,
            lookup=miss,
            artifact=source,
        )
        hit_trial = _trial(route="local-cache-derived", repetition=1)
        hit = adapter.lookup(
            run_id="run-v1",
            trial=hit_trial,
            stage={"logical_node_ids": ["N7"], "stage_key": "lookup"},
            identity=identity,
        )
        self.assertEqual("hit", hit.branch)
        self.assertEqual(miss_trial["trial_key"], hit.source_insert_trial_key)
        read = adapter.read(
            run_id="run-v1",
            trial=hit_trial,
            stage={"logical_node_ids": ["N7"], "stage_key": "read"},
            identity=identity,
            lookup=hit,
        )
        self.assertEqual(BUNDLE, read.payload)

    def test_cache_hit_without_durable_lineage_is_rejected(self) -> None:
        clients = {
            "N7": _CacheClient("N7", "n7-cache"),
            "N8": _CacheClient("N8", "n8-cache"),
        }
        clients["N7"].values[(OBJECT, "sampled_frame_bundle")] = BUNDLE
        adapter = HttpArtifactCacheRouteAdapter(
            clients=clients,
            runtime_epoch_probes={
                "N7": lambda: {
                    "status": "ok",
                    "node_id": "N7",
                    "runtime_epoch": "1" * 32,
                    "credentials_recorded": False,
                },
                "N8": lambda: {
                    "status": "ok",
                    "node_id": "N8",
                    "runtime_epoch": "2" * 32,
                    "credentials_recorded": False,
                },
            },
            lineage=SQLiteCacheLineageStore(self.root / "lineage.sqlite3"),
        )
        with self.assertRaisesRegex(
            FullFlowRouteAdapterError, "lineage disagree"
        ):
            adapter.lookup(
                run_id="run-v1",
                trial=_trial(route="local-cache-derived", repetition=1),
                stage={"logical_node_ids": ["N7"], "stage_key": "lookup"},
                identity=_identity("sampled_frame_bundle", BUNDLE),
            )

    def test_control_and_handoff_do_not_claim_network_or_money(self) -> None:
        trial = _trial(route="raw")
        admitted = InProcessTrialControlAdapter().admit(
            run_id="run-v1",
            trial=trial,
            stage={"stage_key": "admit"},
        )
        artifact = ArtifactAccess(
            source_identity=_identity("raw_video", RAW),
            payload=RAW,
        )
        transferred = InProcessByteTransferAdapter().transfer(
            run_id="run-v1",
            trial=trial,
            stage={"stage_key": "handoff"},
            value=artifact,
        )
        self.assertRegex(admitted.admission_sha256, r"^[0-9a-f]{64}$")
        self.assertIs(artifact, transferred.value)
        self.assertEqual(len(RAW), transferred.telemetry.bytes_sent)

    def test_verified_n1_adapter_refuses_unverified_hmac_evidence(self) -> None:
        adapter = VerifiedN1HTTPScoringAdapter(
            client=_OracleClient(),
            verifier=_Verifier(),
        )
        request = {
            "score_request_id": "score-v1",
            "predicted_answer": "B",
        }
        result = adapter.score_once_and_verify(request)
        self.assertTrue(result.authentication_verified)
        self.assertTrue(result.result["correct"])

        class RejectingVerifier:
            def verify(self, *, request, result):
                return {"status": "FAILED"}

        with self.assertRaisesRegex(
            FullFlowRouteAdapterError, "did not authenticate"
        ):
            VerifiedN1HTTPScoringAdapter(
                client=_OracleClient(),
                verifier=RejectingVerifier(),
            ).score_once_and_verify(request)

    def test_frozen_provisioning_reference_requires_exact_identity(self) -> None:
        identity = _identity("sampled_frame_bundle", BUNDLE)
        reference = ProvisioningReference(
            chain_id="artifact|logical-object|sampled_frame_bundle",
            logical_object_id="logical-object",
            artifact_identity=identity,
            n5_evidence_sha256="e" * 64,
            n4_publication_sha256="f" * 64,
            available=True,
        )
        adapter = FrozenProvisioningReferenceAdapter([reference])
        self.assertEqual(
            reference,
            adapter.resolve(
                run_id="run-v1",
                trial={},
                chain_id=reference.chain_id,
                logical_object_id="logical-object",
                identity=identity,
            ),
        )
        with self.assertRaisesRegex(
            FullFlowRouteAdapterError, "changed the artifact identity"
        ):
            adapter.resolve(
                run_id="run-v1",
                trial={},
                chain_id=reference.chain_id,
                logical_object_id="logical-object",
                identity=_identity("sampled_frame_bundle", b"other"),
            )


class _SemanticHandler(BaseHTTPRequestHandler):
    token = "test-semantic-token"
    requests: list[dict] = []

    def log_message(self, format, *args):
        del format, args

    def _write(self, value):
        body = json.dumps(value).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._write({
            "status": "ok",
            "node_id": "N6",
            "runtime_epoch": "3" * 32,
            "credentials_recorded": False,
            "semantic_quality_enabled": True,
            "semantic_llm_configured": True,
        })

    def do_POST(self):
        if self.headers.get("Authorization") != "Bearer " + self.token:
            self.send_error(401)
            return
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        value = json.loads(raw)
        type(self).requests.append(value)
        self._write({"status": "completed", "echo_sha256": _value_sha(value)})


class HttpContainerNodeSemanticClientTest(unittest.TestCase):
    def setUp(self) -> None:
        _SemanticHandler.requests = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _SemanticHandler)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.addCleanup(self._close)

    def _close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def test_health_and_authenticated_semantic_request(self) -> None:
        client = HttpContainerNodeSemanticClient(
            base_url=f"http://127.0.0.1:{self.server.server_address[1]}",
            bearer_token=_SemanticHandler.token,
        )
        self.assertEqual("N6", client.health()["node_id"])
        result = client.execute({"semantic_request_id": "request-v1"})
        self.assertEqual("completed", result["status"])
        self.assertEqual(
            [{"semantic_request_id": "request-v1"}],
            _SemanticHandler.requests,
        )
        self.assertNotIn(_SemanticHandler.token, repr(client))

    def test_plain_public_http_is_refused(self) -> None:
        with self.assertRaisesRegex(
            FullFlowRouteAdapterError, "plain HTTP"
        ):
            HttpContainerNodeSemanticClient(
                base_url="http://n6.example.test:8086",
                bearer_token=_SemanticHandler.token,
            )


if __name__ == "__main__":
    unittest.main()


class SemanticAnswerTransportTest(unittest.TestCase):
    """The N6 -> N1 return-answer stage transports a SemanticInferenceResult.

    The transport adapter previously bound only artifacts and prepared
    inputs, so the D0 route failed at return-answer with
    "transport cannot bind SemanticInferenceResult" after N6 had already
    produced an answer.
    """

    ANSWER = "B"
    TRIAL = {"trial_key": "scenario|smoke-retrieval|D0|r0000"}
    STAGE = {"stage_key": "scenario|smoke-retrieval|D0|r0000|return-answer"}

    def _result(self, answer: str = ANSWER) -> SemanticInferenceResult:
        return SemanticInferenceResult(
            final_answer=answer,
            model="qwen3.8-27b",
            input_sha256=_sha(b"input"),
            request_sha256=_sha(b"request"),
            result_sha256=_sha(b"result"),
        )

    def test_transfer_accepts_a_semantic_inference_result(self) -> None:
        result = self._result()
        transferred = InProcessByteTransferAdapter().transfer(
            run_id="run-v1", trial=self.TRIAL, stage=self.STAGE, value=result,
        )
        self.assertRegex(transferred.transfer_sha256, r"^[0-9a-f]{64}$")

    def test_transfer_preserves_the_original_result_object(self) -> None:
        result = self._result()
        transferred = InProcessByteTransferAdapter().transfer(
            run_id="run-v1", trial=self.TRIAL, stage=self.STAGE, value=result,
        )
        self.assertIs(result, transferred.value)

    def test_transfer_commitment_uses_result_sha256(self) -> None:
        result = self._result()
        transferred = InProcessByteTransferAdapter().transfer(
            run_id="run-v1", trial=self.TRIAL, stage=self.STAGE, value=result,
        )
        expected = _sha(
            json.dumps(
                {
                    "domain": "pathfinder.in-process-byte-handoff/v1",
                    "run_id": "run-v1",
                    "trial_key": self.TRIAL["trial_key"],
                    "stage_key": self.STAGE["stage_key"],
                    "payload_sha256": result.result_sha256,
                    "payload_size_bytes": len(
                        result.final_answer.encode("utf-8")
                    ),
                    "network_measurement_claimed": False,
                },
                sort_keys=True,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        self.assertEqual(expected, transferred.transfer_sha256)

    def test_bytes_sent_is_the_utf8_answer_length(self) -> None:
        for answer in ("B", "a longer answer", "答案"):
            with self.subTest(answer=answer):
                result = self._result(answer)
                transferred = InProcessByteTransferAdapter().transfer(
                    run_id="run-v1", trial=self.TRIAL, stage=self.STAGE,
                    value=result,
                )
                self.assertEqual(
                    len(answer.encode("utf-8")),
                    transferred.telemetry.bytes_sent,
                )

    def test_unsupported_types_still_fail_closed(self) -> None:
        for value in ({"answer": "B"}, ["B"], "B", b"B", 7, None):
            with self.subTest(value=type(value).__name__):
                with self.assertRaisesRegex(
                    FullFlowRouteAdapterError, "transport cannot bind"
                ):
                    InProcessByteTransferAdapter().transfer(
                        run_id="run-v1", trial=self.TRIAL, stage=self.STAGE,
                        value=value,
                    )
