from __future__ import annotations

import base64
import hashlib
import json
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from pathfinder.simulator.full_flow_semantic_route_runtime import (
    AdapterTelemetry,
    ArtifactAccess,
    ArtifactIdentity,
    AuthenticatedN1Score,
    CacheInsertResult,
    CacheLookupResult,
    ControlAdmission,
    ExactContentRange,
    ExactTemporalFrameSelection,
    GenericSemanticRouteCoordinator,
    InMemoryRouteExecutionStore,
    IndexSelection,
    PreparedSemanticInput,
    ProvisioningReference,
    SemanticInferenceResult,
    SemanticRouteAdapters,
    SemanticRouteRuntimeError,
    TransferResult,
)
from tests import test_simulator_full_flow_semantic_execution_admission as admission_fixture
from pathfinder.simulator.full_flow_semantic_input_profiles import (
    build_semantic_input_profile,
)
from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    verify_semantic_route_evidence,
)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


PAYLOADS = {
    "raw_video": b"0123456789-raw-video-content",
    "multimodal_digest": b"a person opens the door, then walks outside",
    "sampled_frame_bundle": b"ustar-fixture-with-two-ordered-jpeg-frames",
}
INDEXED_BUNDLE = b"8frames"
ORACLE_ID = "generic-route-oracle-v1"
PUBLIC_SET_SHA = _sha(b"generic-route-public-task-set")


def _bound_case(
    trials: Sequence[Mapping[str, Any]],
    stages: Sequence[Mapping[str, Any]],
    *,
    route_family: str,
    workload_class: str | None = None,
    executor_node_id: str | None = None,
    repetition: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    trial = next(
        json.loads(json.dumps(value))
        for value in trials
        if value["route_family"] == route_family
        and (workload_class is None or value["workload_class"] == workload_class)
        and (executor_node_id is None or value["executor_node_id"] == executor_node_id)
        and (repetition is None or value["repetition"] == repetition)
    )
    rows = {
        row["stage_key"]: json.loads(json.dumps(row))
        for row in stages
        if row["trial_key"] == trial["trial_key"]
    }
    selected = [rows[key] for key in trial["semantic_stage_keys"]]

    for identity in trial["representation_identities"]:
        representation = identity["representation_id"]
        payload = PAYLOADS[representation]
        identity["representation_binding"]["artifact_sha256"] = _sha(payload)
        identity["representation_binding"]["artifact_size_bytes"] = len(payload)
    identity_by_representation = {
        row["representation_id"]: row for row in trial["representation_identities"]
    }
    for row in selected:
        stage_identity = row["object_representation_identity"]
        representation = stage_identity["representation_id"]
        if representation is not None:
            source = identity_by_representation[representation]
            stage_identity["representation_binding"] = json.loads(
                json.dumps(source["representation_binding"])
            )
    trial["bound_stage_sha256"] = [_sha(_canonical(row)) for row in selected]
    return trial, selected


class FakeAdapters:
    def __init__(
        self,
        *,
        cache_branch: str = "miss",
        omit_index_range: bool = False,
        unauthenticated_score: bool = False,
        substitute_transfer: bool = False,
        temporal_projection: bool = False,
    ) -> None:
        self.cache_branch = cache_branch
        self.omit_index_range = omit_index_range
        self.unauthenticated_score = unauthenticated_score
        self.substitute_transfer = substitute_transfer
        self.temporal_projection = temporal_projection
        self.calls: Counter[str] = Counter()
        self.events: list[tuple[str, str]] = []
        self.last_range_call: dict[str, Any] | None = None
        self.last_score_request: dict[str, Any] | None = None

    @staticmethod
    def _metric(*, read: int = 0, sent: int = 0) -> AdapterTelemetry:
        return AdapterTelemetry(service_time_ms=1.25, bytes_read=read, bytes_sent=sent)

    def admit(self, *, run_id: str, trial: Mapping[str, Any], stage: Mapping[str, Any]) -> ControlAdmission:
        self.calls["admit"] += 1
        self.events.append(("admit", stage["stage_key"]))
        return ControlAdmission(_sha(f"{run_id}|{trial['trial_key']}".encode()), self._metric())

    def query(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        expected_object_id: str,
    ) -> IndexSelection:
        del run_id, public_task
        self.calls["query"] += 1
        self.events.append(("query", stage["stage_key"]))
        segment = None
        if trial["route_family"] in {"indexed-raw", "indexed-derived"} and not self.omit_index_range:
            identity = next(
                row for row in trial["representation_identities"]
                if row["representation_id"] == "raw_video"
            )["representation_binding"]
            raw = PAYLOADS["raw_video"]
            if self.temporal_projection:
                segment = ExactTemporalFrameSelection(
                    object_id=expected_object_id,
                    representation_id="raw_video",
                    object_catalog_version=identity["object_catalog_version"],
                    full_artifact_size_bytes=len(raw),
                    full_artifact_sha256=_sha(raw),
                    selected_representation_id=(
                        "indexed_temporal_frame_bundle"
                    ),
                    selected_artifact_size_bytes=len(INDEXED_BUNDLE),
                    selected_artifact_sha256=_sha(INDEXED_BUNDLE),
                    frame_count=8,
                    temporal_start_fraction=0.25,
                    temporal_end_fraction=0.75,
                    selection_policy_sha256="c" * 64,
                )
            else:
                start, end = 3, 11
                segment = ExactContentRange(
                    object_id=expected_object_id,
                    representation_id="raw_video",
                    object_catalog_version=identity["object_catalog_version"],
                    full_artifact_size_bytes=len(raw),
                    full_artifact_sha256=_sha(raw),
                    range_start=start,
                    range_end=end,
                    range_sha256=_sha(raw[start : end + 1]),
                )
        commitment = {
            "selected_object_id": expected_object_id,
            "segment": None if segment is None else segment.to_dict(),
        }
        return IndexSelection(
            selected_object_id=expected_object_id,
            index_result_sha256=_sha(_canonical(commitment)),
            segment=segment,
            telemetry=self._metric(sent=len(_canonical(commitment))),
        )

    def fetch_full(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        upstream_values: Sequence[Any],
    ) -> ArtifactAccess:
        del run_id, trial, upstream_values
        self.calls[f"fetch-{stage['logical_node_ids'][0]}"] += 1
        self.events.append(("fetch", stage["stage_key"]))
        payload = PAYLOADS[identity.representation_id]
        return ArtifactAccess(
            source_identity=identity,
            payload=payload,
            telemetry=self._metric(read=len(payload)),
        )

    def fetch_selected(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        source_identity: ArtifactIdentity,
        selection: ExactTemporalFrameSelection,
    ) -> ArtifactAccess:
        del run_id, trial
        self.calls["fetch-selected-N3"] += 1
        self.events.append(("fetch-selected", stage["stage_key"]))
        return ArtifactAccess(
            source_identity=source_identity,
            payload=INDEXED_BUNDLE,
            segment=selection,
            telemetry=self._metric(read=len(INDEXED_BUNDLE)),
        )

    def build_request(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        selection: IndexSelection,
    ) -> Any:
        self.calls["range-request"] += 1
        return {
            "run_id": run_id,
            "trial_key": trial["trial_key"],
            "stage_key": stage["stage_key"],
            "identity": identity,
            "selection": selection,
        }

    def fetch_binary_artifact_range(
        self,
        request: Any,
        *,
        range_start: int,
        range_end: int,
        expected_range_sha256: str,
        allowed_media_types: frozenset[str] | set[str] | tuple[str, ...],
        on_phase: Any = None,
    ) -> Any:
        del on_phase
        self.calls["range-fetch"] += 1
        identity: ArtifactIdentity = request["identity"]
        raw = PAYLOADS["raw_video"]
        selected = raw[range_start : range_end + 1]
        self.last_range_call = {
            "range_start": range_start,
            "range_end": range_end,
            "expected_range_sha256": expected_range_sha256,
            "allowed_media_types": tuple(allowed_media_types),
        }
        return SimpleNamespace(
            data=selected,
            range_start=range_start,
            range_end=range_end,
            range_size_bytes=len(selected),
            range_sha256=_sha(selected),
            full_artifact_size_bytes=len(raw),
            full_artifact_sha256=_sha(raw),
            object_id=identity.object_id,
            object_catalog_version=identity.object_catalog_version,
            download_elapsed_ms=2.5,
        )

    def transfer(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        value: Any,
    ) -> TransferResult:
        del run_id, trial
        self.calls["transfer"] += 1
        self.events.append(("transfer", stage["stage_key"]))
        forwarded = value
        if self.substitute_transfer and isinstance(value, ArtifactAccess):
            forwarded = ArtifactAccess(
                source_identity=value.source_identity,
                payload=b"substitution",
            )
        if isinstance(value, ArtifactAccess):
            size = len(value.payload)
        elif isinstance(value, PreparedSemanticInput):
            size = len(value.payload)
        elif isinstance(value, SemanticInferenceResult):
            size = len(value.final_answer.encode())
        else:
            size = len(_canonical({"kind": type(value).__name__}))
        return TransferResult(
            value=forwarded,
            transfer_sha256=_sha(f"{stage['stage_key']}|{size}".encode()),
            telemetry=self._metric(sent=size),
        )

    def lookup(
        self,
        *,
        run_id: str,
        cache_episode_id: str | None = None,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
    ) -> CacheLookupResult:
        del run_id
        if cache_episode_id is not None:
            self.calls[f"episode-{cache_episode_id}"] += 1
        self.calls["lookup"] += 1
        self.events.append(("lookup", stage["stage_key"]))
        return CacheLookupResult(
            node_id=trial["executor_node_id"],
            cache_id=f"cache-{trial['executor_node_id'].lower()}",
            branch=self.cache_branch,
            runtime_epoch="cache-epoch-v1",
            source_insert_trial_key=(
                trial["trial_key"].rsplit("|", 1)[0]
                + f"|r{trial['repetition'] - 1:04d}"
                if self.cache_branch == "hit"
                else None
            ),
            lookup_sha256=_sha(
                f"{identity.commitment}|{self.cache_branch}".encode()
            ),
            telemetry=self._metric(),
        )

    def read(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        lookup: CacheLookupResult,
    ) -> ArtifactAccess:
        del run_id, trial, lookup
        self.calls["cache-read"] += 1
        self.events.append(("cache-read", stage["stage_key"]))
        payload = PAYLOADS[identity.representation_id]
        return ArtifactAccess(
            identity,
            payload,
            telemetry=self._metric(read=len(payload)),
        )

    def insert(
        self,
        *,
        run_id: str,
        cache_episode_id: str | None = None,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        lookup: CacheLookupResult,
        artifact: ArtifactAccess,
    ) -> CacheInsertResult:
        del run_id, cache_episode_id
        self.calls["cache-insert"] += 1
        self.events.append(("cache-insert", stage["stage_key"]))
        return CacheInsertResult(
            node_id=trial["executor_node_id"],
            cache_id=lookup.cache_id,
            runtime_epoch=lookup.runtime_epoch,
            insert_sha256=_sha(f"{stage['stage_key']}|{identity.commitment}".encode()),
            artifact=artifact,
            telemetry=self._metric(sent=len(artifact.payload)),
        )

    def prepare(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        mode: str,
        artifacts: Sequence[ArtifactAccess],
    ) -> PreparedSemanticInput:
        del run_id, trial, public_task
        self.calls[f"prepare-{mode}"] += 1
        binding_stage_key = str(stage.get("stage_key"))
        content: dict[str, Any] = {
            "mode": mode,
            "artifact_payload_sha256": [value.payload_sha256 for value in artifacts],
        }
        if mode == "direct-video":
            # Direct video must carry the real encoded bytes and commit to
            # their identity, exactly as the production adapter does.
            video = artifacts[0].payload
            content.update({
                "video_base64": base64.b64encode(video).decode("ascii"),
                "video_sha256": _sha(video),
                "video_size_bytes": len(video),
                "representation_sha256": _sha(video),
            })
        if mode == "digest+indexed-frames-fusion":
            digest = next(
                value for value in artifacts
                if value.source_identity.representation_id == "multimodal_digest"
            )
            content.update({
                "semantic_request_id": "a" * 64,
                "digest_sha256": digest.payload_sha256,
                "frame_sequence_sha256": "b" * 64,
                "frames": [
                    {
                        "timestamp_seconds": float(index + 1),
                        "width": 2,
                        "height": 2,
                        "jpeg_base64": base64.b64encode(b"jpeg").decode(),
                    }
                    for index in range(8)
                ],
            })
        payload = _canonical(content)
        identities = tuple(value.source_identity for value in artifacts)
        commitment = _sha(_canonical({
            "mode": mode,
            "payload_sha256": _sha(payload),
            "payload_size_bytes": len(payload),
            "component_identity_sha256": [value.commitment for value in identities],
            "request_binding_stage_key": binding_stage_key,
        }))
        return PreparedSemanticInput(
            mode=mode,
            payload=payload,
            component_identities=identities,
            preparation_sha256=commitment,
            request_binding_stage_key=binding_stage_key,
            telemetry=self._metric(read=sum(len(value.payload) for value in artifacts)),
        )

    def infer(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        model_input: PreparedSemanticInput,
    ) -> SemanticInferenceResult:
        del run_id, trial, stage, public_task
        self.calls["infer"] += 1
        request_sha = _sha(b"request|" + model_input.payload)
        return SemanticInferenceResult(
            final_answer="B",
            model="qwen3.8-27b",
            input_sha256=model_input.payload_sha256,
            request_sha256=request_sha,
            result_sha256=_sha(f"{request_sha}|B".encode()),
            telemetry=self._metric(read=len(model_input.payload), sent=1),
        )

    def score_once_and_verify(self, request: Mapping[str, Any]) -> AuthenticatedN1Score:
        self.calls["score"] += 1
        self.last_score_request = dict(request)
        result = {
            "schema_version": "pathfinder.n1-score-result/v1alpha2",
            "status": "SCORED",
            "score_request_id": request["score_request_id"],
            "evaluation_unit_id": request["evaluation_unit_id"],
            "oracle_id": ORACLE_ID,
            "node_id": "N1",
            "run_id": request["run_id"],
            "trial_id": request["trial_id"],
            "object_id": request["object_id"],
            "task_binding_sha256": request["task_binding_sha256"],
            "request_sha256": _sha(_canonical(request)),
            "prediction_sha256": _sha(request["predicted_answer"].encode()),
            "success_scoring_rule": "multiple-choice-option-id-exact-match-v1",
            "correct": True,
            "score": 1.0,
            "public_task_set_sha256": PUBLIC_SET_SHA,
            "oracle_instance_hmac_sha256": "a" * 64,
            "score_evidence_hmac_sha256": "b" * 64,
            "idempotent_replay": False,
            "hidden_answer_returned": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        result["result_content_sha256"] = _sha(_canonical(result))
        verification = _sha(_canonical({
            "domain": "pathfinder.authenticated-n1-score-verification/v1",
            "request_sha256": result["request_sha256"],
            "result_content_sha256": result["result_content_sha256"],
            "score_evidence_hmac_sha256": result["score_evidence_hmac_sha256"],
        }))
        return AuthenticatedN1Score(
            result=result,
            authentication_verified=not self.unauthenticated_score,
            verification_sha256=verification,
            telemetry=self._metric(sent=len(_canonical(result))),
        )

    def resolve(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        chain_id: str,
        logical_object_id: str,
        identity: ArtifactIdentity,
    ) -> ProvisioningReference:
        del run_id, trial
        self.calls["provision"] += 1
        return ProvisioningReference(
            chain_id=chain_id,
            logical_object_id=logical_object_id,
            artifact_identity=identity,
            n5_evidence_sha256=_sha(f"N5|{identity.commitment}".encode()),
            n4_publication_sha256=_sha(f"N4|{identity.commitment}".encode()),
            available=True,
        )

    def bundle(self) -> SemanticRouteAdapters:
        return SemanticRouteAdapters(
            control=self,
            index=self,
            artifacts=self,
            range_fetcher=self,
            range_request_factory=self,
            transport=self,
            cache=self,
            model_input=self,
            semantic=self,
            scorer=self,
            provisioning=self,
        )


def _coordinator(fake: FakeAdapters, store: InMemoryRouteExecutionStore | None = None) -> GenericSemanticRouteCoordinator:
    return GenericSemanticRouteCoordinator(
        adapters=fake.bundle(),
        store=store or InMemoryRouteExecutionStore(),
        oracle_id=ORACLE_ID,
        oracle_public_task_set_sha256=PUBLIC_SET_SHA,
    )


class FullFlowSemanticRouteRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        admission_fixture.FullFlowSemanticExecutionAdmissionTest.setUpClass()
        owner = admission_fixture.FullFlowSemanticExecutionAdmissionTest(
            "test_freezes_all_trials_as_blocked_not_submittable"
        )
        cls.admission = owner._freeze("generic-route-runtime-source")
        cls.trials = admission_fixture._read_jsonl(
            cls.admission / "semantic-execution-trials.jsonl"
        )
        cls.stages = admission_fixture._read_jsonl(
            cls.admission / "semantic-execution-stages.jsonl"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        admission_fixture.FullFlowSemanticExecutionAdmissionTest.tearDownClass()

    def test_raw_route_runs_n3_n7_n6_n1_with_direct_video(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="raw",
            executor_node_id="N7",
        )
        fake = FakeAdapters()
        evidence = _coordinator(fake).execute(
            run_id="generic-raw-run-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        self.assertEqual(evidence["status"], "COMPLETE")
        self.assertEqual(evidence["route"]["executor_node_id"], "N7")
        # D0/D4 are now genuine direct-video alternatives: the complete
        # encoded object reaches N6 and no frames are prepared.
        self.assertEqual(evidence["model_input"]["mode"], "direct-video")
        self.assertTrue(evidence["model_input"]["direct_video_input"])
        self.assertEqual(
            _sha(PAYLOADS["raw_video"]),
            evidence["model_input"]["direct_video_sha256"],
        )
        self.assertEqual(
            len(PAYLOADS["raw_video"]),
            evidence["model_input"]["direct_video_size_bytes"],
        )
        self.assertEqual(0, evidence["model_input"]["frame_count"])
        self.assertIsNone(evidence["model_input"]["frame_sequence_sha256"])
        self.assertEqual(fake.calls["fetch-N3"], 1)
        self.assertEqual(fake.calls["range-fetch"], 0)
        self.assertEqual(fake.calls["infer"], 1)
        self.assertEqual(fake.calls["score"], 1)
        self.assertTrue(evidence["n1_exactly_once_authenticated_score_verified"])
        self.assertNotIn("final_answer", evidence["semantic"])

    def test_indexed_raw_uses_exact_n2_range_and_compatible_range_fetcher(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="indexed-raw",
            executor_node_id="N7",
        )
        fake = FakeAdapters()
        evidence = _coordinator(fake).execute(
            run_id="generic-indexed-run-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        self.assertEqual(fake.calls["query"], 1)
        self.assertEqual(fake.calls["range-request"], 1)
        self.assertEqual(fake.calls["range-fetch"], 1)
        self.assertEqual(fake.calls["fetch-N3"], 0)
        self.assertEqual(
            fake.last_range_call,
            {
                "range_start": 3,
                "range_end": 11,
                "expected_range_sha256": _sha(PAYLOADS["raw_video"][3:12]),
                "allowed_media_types": ("video/mp4",),
            },
        )
        self.assertEqual(evidence["model_input"]["mode"], "raw-prepared-frames")
        self.assertTrue(evidence["n2_exact_range_required_for_indexed_raw"])

    def test_indexed_raw_fetches_real_n3_projection_without_raw_range(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="indexed-raw",
            executor_node_id="N7",
        )
        fake = FakeAdapters(temporal_projection=True)
        evidence = _coordinator(fake).execute(
            run_id="generic-indexed-projection-run-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        self.assertEqual(1, fake.calls["query"])
        self.assertEqual(1, fake.calls["fetch-selected-N3"])
        self.assertEqual(0, fake.calls["range-request"])
        self.assertEqual(0, fake.calls["range-fetch"])
        self.assertEqual("raw-prepared-frames", evidence["model_input"]["mode"])

    def test_indexed_derived_routes_n3_selection_and_n4_digest_together(
        self,
    ) -> None:
        trial, indexed_stages = _bound_case(
            self.trials, self.stages, route_family="indexed-raw",
            workload_class="W3", executor_node_id="N7",
        )
        derived, derived_stages = _bound_case(
            self.trials, self.stages, route_family="remote-derived",
            workload_class="W3", executor_node_id="N7",
        )
        trial["route_family"] = "indexed-derived"
        trial["representation_identities"].append(next(
            row for row in derived["representation_identities"]
            if row["representation_id"] == "multimodal_digest"
        ))
        trial["required_provisioning_chain_ids"] = [
            "artifact|video-causal|multimodal_digest"
        ]
        trial["semantic_input_profile"] = build_semantic_input_profile(
            route_family="indexed-derived",
            model_input_representation_ids=[
                "raw_video", "multimodal_digest",
            ],
            indexed_selection_kind="query-aware-temporal-index",
            indexed_frame_count=8,
            indexed_temporal_window_fraction=(0.25, 0.75),
        )
        read_digest = next(
            json.loads(json.dumps(stage)) for stage in derived_stages
            if stage["stage_key"].endswith("|read-digest")
        )
        transfer_digest = next(
            json.loads(json.dumps(stage)) for stage in derived_stages
            if stage["stage_key"].endswith("|transfer-digest")
        )
        prefix = trial["trial_key"]
        read_digest.update({
            "trial_key": prefix,
            "stage_key": f"{prefix}|read-digest",
            "dependency_stage_keys": [indexed_stages[0]["stage_key"]],
        })
        transfer_digest.update({
            "trial_key": prefix,
            "stage_key": f"{prefix}|transfer-digest",
            "dependency_stage_keys": [read_digest["stage_key"]],
        })
        stages = [
            *indexed_stages[:5], read_digest, transfer_digest,
            *indexed_stages[5:],
        ]
        prepare = next(
            stage for stage in stages
            if stage["action"] == "prepare-model-input"
        )
        prepare["dependency_stage_keys"].append(transfer_digest["stage_key"])
        for index, stage in enumerate(stages):
            stage["stage_index"] = index
        trial["semantic_stage_keys"] = [stage["stage_key"] for stage in stages]
        trial["bound_stage_sha256"] = [
            _sha(_canonical(stage)) for stage in stages
        ]
        fake = FakeAdapters(temporal_projection=True)
        evidence = _coordinator(fake).execute(
            run_id="generic-indexed-derived-run-v1",
            bound_trial=trial, bound_stages=stages,
        )
        self.assertEqual(1, fake.calls["fetch-selected-N3"])
        self.assertEqual(1, fake.calls["fetch-N4"])
        self.assertEqual(1, fake.calls["prepare-digest+indexed-frames-fusion"])
        self.assertEqual(
            "digest+indexed-frames-fusion", evidence["model_input"]["mode"]
        )
        self.assertEqual(2, len(
            evidence["model_input"]["component_identity_sha256"]
        ))
        self.assertEqual(1, fake.calls["infer"])
        self.assertEqual(1, fake.calls["score"])
        verified = verify_semantic_route_evidence(
            evidence, run_id="generic-indexed-derived-run-v1",
            bound_trial=trial, bound_stages=stages,
        )
        self.assertEqual("COMPLETE", verified["status"])

    def test_indexed_raw_fails_closed_without_content_bound_range(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="indexed-raw",
        )
        fake = FakeAdapters(omit_index_range=True)
        with self.assertRaisesRegex(
            SemanticRouteRuntimeError,
            "exact content-bound N2 range",
        ):
            _coordinator(fake).execute(
                run_id="generic-no-range-v1",
                bound_trial=trial,
                bound_stages=stages,
            )
        self.assertEqual(fake.calls["range-fetch"], 0)
        self.assertEqual(fake.calls["infer"], 0)
        self.assertEqual(fake.calls["score"], 0)

    def test_remote_digest_and_n5_publication_reference_are_preserved(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="remote-derived",
            workload_class="W1",
            executor_node_id="N7",
        )
        fake = FakeAdapters()
        evidence = _coordinator(fake).execute(
            run_id="generic-digest-run-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        self.assertEqual(evidence["model_input"]["mode"], "digest")
        self.assertEqual(fake.calls["fetch-N4"], 1)
        self.assertEqual(fake.calls["provision"], 1)
        self.assertEqual(len(evidence["provisioning_references"]), 1)
        self.assertTrue(evidence["n5_provisioning_references_verified"])

    def test_remote_digest_frames_fusion_runs_on_n8(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="remote-derived",
            workload_class="W3",
            executor_node_id="N8",
        )
        fake = FakeAdapters()
        evidence = _coordinator(fake).execute(
            run_id="generic-fusion-n8-run-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        self.assertEqual(evidence["route"]["executor_node_id"], "N8")
        self.assertEqual(evidence["model_input"]["mode"], "digest+frames-fusion")
        self.assertEqual(fake.calls["fetch-N4"], 2)
        self.assertEqual(fake.calls["provision"], 2)
        self.assertEqual(fake.calls["prepare-digest+frames-fusion"], 1)

    def test_cache_miss_executes_only_remote_insert_branch(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="local-cache-derived",
            workload_class="W3",
            executor_node_id="N7",
        )
        fake = FakeAdapters(cache_branch="miss")
        evidence = _coordinator(fake).execute(
            run_id="generic-cache-miss-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        self.assertEqual(fake.calls["lookup"], 2)
        self.assertEqual(fake.calls["cache-read"], 0)
        self.assertEqual(fake.calls["cache-insert"], 2)
        self.assertEqual(fake.calls["fetch-N4"], 2)
        self.assertEqual(
            {row["branch"] for row in evidence["cache_branches"]},
            {"miss"},
        )
        skipped = {
            row["stage_key"].rsplit("|", 1)[-1]
            for row in evidence["stage_results"]
            if row["state"] == "SKIPPED_INACTIVE_CONDITION"
        }
        self.assertEqual(skipped, {"read-local-digest", "read-local-frames"})

    def test_cache_hit_executes_only_local_branch_on_n8(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="local-cache-derived",
            workload_class="W4",
            executor_node_id="N8",
            repetition=1,
        )
        fake = FakeAdapters(cache_branch="hit")
        evidence = _coordinator(fake).execute(
            run_id="generic-cache-hit-n8-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        self.assertEqual(evidence["route"]["executor_node_id"], "N8")
        self.assertEqual(evidence["model_input"]["mode"], "frame-bundle")
        self.assertEqual(fake.calls["cache-read"], 1)
        self.assertEqual(fake.calls["cache-insert"], 0)
        self.assertEqual(fake.calls["fetch-N4"], 0)
        skipped = {
            row["stage_key"].rsplit("|", 1)[-1]
            for row in evidence["stage_results"]
            if row["state"] == "SKIPPED_INACTIVE_CONDITION"
        }
        self.assertEqual(skipped, {"read-remote", "transfer-remote", "insert"})
        self.assertEqual(evidence["cache_branches"][0]["branch"], "hit")

    def test_cache_hit_without_frozen_paired_predecessor_is_rejected(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="local-cache-derived",
            workload_class="W4",
            executor_node_id="N8",
            repetition=0,
        )
        fake = FakeAdapters(cache_branch="hit")
        with self.assertRaisesRegex(
            SemanticRouteRuntimeError,
            "frozen repetition lifecycle",
        ):
            _coordinator(fake).execute(
                run_id="generic-cache-wrong-branch-v1",
                bound_trial=trial,
                bound_stages=stages,
            )
        self.assertEqual(fake.calls["cache-read"], 0)
        self.assertEqual(fake.calls["score"], 0)

    def test_episode_cache_hit_is_not_tied_to_trial_repetition(self) -> None:
        from pathfinder.simulator.full_flow_semantic_route_evidence import (
            SEMANTIC_ROUTE_EPISODE_EVIDENCE_SCHEMA_VERSION,
            verify_public_semantic_route_evidence,
        )

        trial, stages = _bound_case(
            self.trials, self.stages,
            route_family="local-cache-derived",
            workload_class="W4", executor_node_id="N8", repetition=0,
        )
        fake = FakeAdapters(cache_branch="hit")
        evidence = _coordinator(fake).execute(
            run_id="distinct-question-run-v1",
            bound_trial=trial, bound_stages=stages,
            cache_episode_id="shared-multiq-episode",
        )
        self.assertEqual(
            SEMANTIC_ROUTE_EPISODE_EVIDENCE_SCHEMA_VERSION,
            evidence["schema_version"],
        )
        self.assertEqual("shared-multiq-episode", evidence["cache_episode_id"])
        self.assertEqual(1, fake.calls["episode-shared-multiq-episode"])
        self.assertEqual(
            evidence, verify_public_semantic_route_evidence(evidence)
        )
        from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
            FlowMeshSemanticTrialError,
            verify_semantic_route_evidence,
        )
        self.assertEqual(
            evidence,
            verify_semantic_route_evidence(
                evidence, run_id="distinct-question-run-v1",
                bound_trial=trial, bound_stages=stages,
                cache_episode_id="shared-multiq-episode",
            ),
        )
        forged = dict(evidence, cache_episode_id="other-episode")
        forged.pop("evidence_sha256")
        forged["evidence_sha256"] = _sha(_canonical(forged))
        with self.assertRaisesRegex(
            FlowMeshSemanticTrialError, "cache episode differs"
        ):
            verify_semantic_route_evidence(
                forged, run_id="distinct-question-run-v1",
                bound_trial=trial, bound_stages=stages,
                cache_episode_id="shared-multiq-episode",
            )
        self.assertEqual(fake.calls["cache-read"], 1)
        self.assertEqual(fake.calls["score"], 1)

    def test_replay_returns_stored_evidence_without_duplicate_score(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="remote-derived",
            workload_class="W1",
        )
        fake = FakeAdapters()
        store = InMemoryRouteExecutionStore()
        coordinator = _coordinator(fake, store)
        first = coordinator.execute(
            run_id="generic-idempotent-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        calls = fake.calls.copy()
        second = coordinator.execute(
            run_id="generic-idempotent-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(fake.calls, calls)
        self.assertEqual(fake.calls["score"], 1)
        self.assertEqual(first["evidence_sha256"], second["evidence_sha256"])

        changed = json.loads(json.dumps(trial))
        changed["worker_alias"] = "different-worker-alias"
        with self.assertRaisesRegex(
            SemanticRouteRuntimeError,
            "reused for different frozen input",
        ):
            coordinator.execute(
                run_id="generic-idempotent-v1",
                bound_trial=changed,
                bound_stages=stages,
            )
        self.assertEqual(fake.calls["score"], 1)

    def test_bound_stage_tampering_is_rejected_before_any_adapter(self) -> None:
        trial, stages = _bound_case(self.trials, self.stages, route_family="raw")
        stages[1]["logical_node_ids"] = ["N4"]
        fake = FakeAdapters()
        with self.assertRaisesRegex(SemanticRouteRuntimeError, "content changed"):
            _coordinator(fake).execute(
                run_id="generic-tamper-v1",
                bound_trial=trial,
                bound_stages=stages,
            )
        self.assertFalse(fake.calls)

    def test_missing_and_extra_stage_are_rejected(self) -> None:
        trial, stages = _bound_case(self.trials, self.stages, route_family="raw")
        for changed in (stages[:-1], stages + [dict(stages[-1])]):
            fake = FakeAdapters()
            with self.assertRaisesRegex(
                SemanticRouteRuntimeError,
                "missing or contains unused",
            ):
                _coordinator(fake).execute(
                    run_id="generic-stage-set-v1",
                    bound_trial=trial,
                    bound_stages=changed,
                )
            self.assertFalse(fake.calls)

    def test_unauthenticated_score_is_never_emitted_as_evidence(self) -> None:
        trial, stages = _bound_case(self.trials, self.stages, route_family="raw")
        fake = FakeAdapters(unauthenticated_score=True)
        with self.assertRaisesRegex(SemanticRouteRuntimeError, "not authenticated"):
            _coordinator(fake).execute(
                run_id="generic-unauthenticated-v1",
                bound_trial=trial,
                bound_stages=stages,
            )
        self.assertEqual(fake.calls["score"], 1)

    def test_transport_substitution_fails_closed(self) -> None:
        trial, stages = _bound_case(self.trials, self.stages, route_family="raw")
        fake = FakeAdapters(substitute_transfer=True)
        with self.assertRaisesRegex(SemanticRouteRuntimeError, "substituted"):
            _coordinator(fake).execute(
                run_id="generic-substitution-v1",
                bound_trial=trial,
                bound_stages=stages,
            )
        self.assertEqual(fake.calls["infer"], 0)
        self.assertEqual(fake.calls["score"], 0)

    def test_public_task_tampering_is_rejected_before_execution(self) -> None:
        trial, stages = _bound_case(self.trials, self.stages, route_family="raw")
        trial["public_task_binding"]["question"] += " changed"
        fake = FakeAdapters()
        with self.assertRaisesRegex(SemanticRouteRuntimeError, "not canonical"):
            _coordinator(fake).execute(
                run_id="generic-task-tamper-v1",
                bound_trial=trial,
                bound_stages=stages,
            )
        self.assertFalse(fake.calls)

    def test_evidence_is_neutral_credential_free_and_bridge_ready(self) -> None:
        trial, stages = _bound_case(
            self.trials,
            self.stages,
            route_family="remote-derived",
            workload_class="W1",
        )
        evidence = _coordinator(FakeAdapters()).execute(
            run_id="generic-neutral-v1",
            bound_trial=trial,
            bound_stages=stages,
        )
        encoded = json.dumps(evidence, sort_keys=True)
        self.assertNotIn("correct_answer_id", encoded)
        self.assertNotIn("bearer_token", encoded)
        self.assertNotIn("http://", encoded)
        self.assertFalse(evidence["credentials_recorded"])
        self.assertEqual("B", evidence["n1_score_request"]["predicted_answer"])
        self.assertFalse(
            evidence["n1_score_result"]["hidden_answer_returned"]
        )
        self.assertEqual(
            evidence["semantic"]["final_answer_sha256"],
            evidence["n1_score_result"]["prediction_sha256"],
        )
        self.assertEqual(
            evidence["scoring"]["score_evidence_hmac_sha256"],
            evidence["n1_score_result"]["score_evidence_hmac_sha256"],
        )
        candidate = evidence["neutral_observation_candidate"]
        self.assertTrue(candidate["score_authenticity_verified"])
        self.assertFalse(candidate["monetary_measurement_available"])
        self.assertIn("component_service_time_ms", candidate)
        self.assertIn("byte_measurements", candidate)


if __name__ == "__main__":
    unittest.main()


class D0CrossStageRouteRuntimeBindingTest(unittest.TestCase):
    """D0-shaped route-runtime regression with real N6 adapters.

    Exercises the genuine two-stage shape -- prepare on
    ``prepare-model-input``, infer on ``infer`` -- through the same
    ``_validate_prepared`` commitment check the route runtime applies, using
    the real adapters rather than a same-stage fake.
    """

    def test_real_adapters_bind_across_separate_d0_stages(self) -> None:
        from pathfinder.simulator.full_flow_n6_adapters import (
            BoundN6SemanticInferenceAdapter,
            N6ModelInputAdapter,
            N6PreparationLimits,
        )
        from pathfinder.simulator.full_flow_semantic_route_runtime import (
            _validate_prepared,
        )
        from tests.test_simulator_full_flow_n6_adapters import (
            FakeExecutor,
            MODEL,
            RecordingSampler,
            _access,
            _health,
            _public_task,
        )

        prepare_stage = {
            "stage_key": "flowmesh-infra-4x8-local-smoke-v1|smoke-retrieval"
            "|D0|r0000|prepare-model-input",
            "logical_node_ids": ["N7"],
        }
        infer_stage = {
            "stage_key": "flowmesh-infra-4x8-local-smoke-v1|smoke-retrieval"
            "|D0|r0000|infer",
            "logical_node_ids": ["N6"],
        }
        self.assertNotEqual(
            prepare_stage["stage_key"], infer_stage["stage_key"]
        )
        artifacts = [_access("multimodal_digest", b"digest")]
        prepared = N6ModelInputAdapter(
            raw_sampler=RecordingSampler(),
            limits=N6PreparationLimits(raw_frame_count=2),
            clock_ns=lambda: 1,
        ).prepare(
            run_id="run",
            trial={"trial_key": "trial"},
            stage=prepare_stage,
            public_task=_public_task(),
            mode="digest",
            artifacts=artifacts,
        )
        # The route runtime's own commitment check must accept it.
        _validate_prepared(prepared, artifacts, "digest")
        self.assertEqual(
            prepare_stage["stage_key"], prepared.request_binding_stage_key
        )
        executor = FakeExecutor()
        result = BoundN6SemanticInferenceAdapter(
            executor=executor, health_probe=_health, expected_model=MODEL,
        ).infer(
            run_id="run",
            trial={"trial_key": "trial"},
            stage=infer_stage,
            public_task=_public_task(),
            model_input=prepared,
        )
        self.assertEqual(MODEL, result.model)
        self.assertEqual(1, len(executor.calls))


class D0ReturnAnswerTransportTest(unittest.TestCase):
    """The real transfer adapter must carry the answer past return-answer.

    Complements the D0 cross-stage binding case: that one proves N6 inference
    succeeds across separate stages, this one proves the resulting answer can
    still be handed from N6 to N1 so hidden scoring is reachable.
    """

    def test_real_transfer_adapter_carries_the_answer_to_n1(self) -> None:
        from pathfinder.simulator.full_flow_route_adapters import (
            InProcessByteTransferAdapter,
        )
        from pathfinder.simulator.full_flow_semantic_route_runtime import (
            SemanticInferenceResult,
            _value_commitment,
        )

        trial = {"trial_key": "scenario|smoke-retrieval|D0|r0000"}
        result = SemanticInferenceResult(
            final_answer="B",
            model="qwen3.8-27b",
            input_sha256=_sha(b"input"),
            request_sha256=_sha(b"request"),
            result_sha256=_sha(b"result"),
        )
        before = _value_commitment(result)
        transferred = InProcessByteTransferAdapter().transfer(
            run_id="run-v1",
            trial=trial,
            stage={"stage_key": trial["trial_key"] + "|return-answer"},
            value=result,
        )
        # The route runtime compares the commitment across the handoff and
        # then hands the forwarded value to the hidden-scoring stage.
        self.assertEqual(before, _value_commitment(transferred.value))
        self.assertIs(result, transferred.value)
        self.assertEqual(
            len("B".encode("utf-8")), transferred.telemetry.bytes_sent
        )
        # Hidden scoring consumes the unwrapped answer.
        forwarded = transferred.value
        self.assertIsInstance(forwarded, SemanticInferenceResult)
        self.assertEqual("B", forwarded.final_answer)


class D1IndexedRawTransportTest(unittest.TestCase):
    """The D1 indexed-raw route must advance past the N2 -> N3 transfer.

    D0 has no index stage, so its success never exercised this handoff; the
    indexed-raw route failed the moment the selection crossed it.
    """

    def test_d1_selection_advances_beyond_the_n2_to_n3_transfer(self) -> None:
        from pathfinder.simulator.full_flow_route_adapters import (
            InProcessByteTransferAdapter,
        )
        from pathfinder.simulator.full_flow_semantic_route_runtime import (
            ExactContentRange,
            IndexSelection,
            _value_commitment,
        )

        payload = b"indexed-raw-video-bytes"
        segment = ExactContentRange(
            object_id="nextqa-val-0000000001",
            representation_id="raw_video",
            object_catalog_version="catalog-v1",
            full_artifact_size_bytes=len(payload),
            full_artifact_sha256=_sha(payload),
            range_start=0,
            range_end=len(payload) - 1,
            range_sha256=_sha(payload),
        )
        selection = IndexSelection(
            selected_object_id="nextqa-val-0000000001",
            index_result_sha256=_sha(b"index-result"),
            segment=segment,
        )
        trial = {"trial_key": "scenario|smoke-retrieval|D1|r0000"}
        before = _value_commitment(selection)
        transferred = InProcessByteTransferAdapter().transfer(
            run_id="run-v1",
            trial=trial,
            stage={"stage_key": trial["trial_key"] + "|transfer-scan"},
            value=selection,
        )
        # The route runtime compares the commitment across the handoff and
        # then hands the forwarded selection to the N3 access stage.
        self.assertEqual(before, _value_commitment(transferred.value))
        forwarded = transferred.value
        self.assertIsInstance(forwarded, IndexSelection)
        # N3 must still see the exact range, unchanged.
        self.assertIs(segment, forwarded.segment)
        self.assertEqual(0, forwarded.segment.range_start)
        self.assertEqual(len(payload) - 1, forwarded.segment.range_end)
        self.assertEqual(_sha(payload), forwarded.segment.range_sha256)
        # bytes_sent describes the selection metadata, not the video.
        self.assertNotEqual(len(payload), transferred.telemetry.bytes_sent)
