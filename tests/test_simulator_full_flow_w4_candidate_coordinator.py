from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from unittest import mock

from pathfinder.simulator.full_flow_w4_candidate_coordinator import (
    OBSERVATIONS_NAME,
    OPERATION_EVIDENCE_NAME,
    RUN_NAME,
    W4_CANDIDATE_OPERATION_RESULT_SCHEMA_VERSION,
    DeterministicW4CandidateOperationExecutor,
    FullFlowW4CandidateCoordinatorError,
    run_full_flow_w4_candidate_coordinator,
    verify_full_flow_w4_candidate_coordinator_run,
)
from pathfinder.simulator.full_flow_w4_candidate_routes import (
    freeze_full_flow_w4_candidate_routes,
)
from pathfinder.simulator.full_flow_w4_retrieval_contract import (
    W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
    evaluate_full_flow_w4_retrieval,
)
from pathfinder.simulator.full_flow_w4_retrieval_runtime import (
    load_full_flow_w4_retrieval_runtime_inputs,
)
from tests import test_simulator_full_flow_w4_candidate_routes as route_fixture
from tests import test_simulator_full_flow_w4_retrieval_runtime as runtime_fixture


class DeterministicOperationExecutor:
    def __init__(
        self,
        *,
        corrupt_action: str | None = None,
        corrupt_unactivated_tail: bool = False,
        corrupt_index_binding: bool = False,
        corrupt_artifact_identity: bool = False,
    ) -> None:
        self.corrupt_action = corrupt_action
        self.corrupt_unactivated_tail = corrupt_unactivated_tail
        self.corrupt_index_binding = corrupt_index_binding
        self.corrupt_artifact_identity = corrupt_artifact_identity
        self.calls: list[dict] = []

    def execute(
        self,
        *,
        run_id,
        execution_token,
        trial,
        operation,
        dependency_results,
        public_task,
        context,
    ):
        self.calls.append({
            "run_id": run_id,
            "execution_token": execution_token,
            "trial_key": trial["trial_key"],
            "operation": dict(operation),
            "dependency_results": list(dependency_results),
            "candidate_set_sha256": public_task["candidate_set_sha256"],
            "context": dict(context),
        })
        artifact = context["artifact_identity"]
        content_range = context["exact_content_range"]
        logical_bytes = context["expected_logical_bytes"]
        ranking = context["required_ranking"]
        if context["ranking_candidate_ids"] is not None:
            ranking = sorted(context["ranking_candidate_ids"], reverse=True)
        if (
            self.corrupt_unactivated_tail
            and operation["action"] == "rank-complete-candidate-set"
            and operation["design_id"] == "D2"
        ):
            ranking = [*ranking[1:], ranking[0]]
        result = {
            "schema_version": W4_CANDIDATE_OPERATION_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "execution_token": execution_token,
            "operation_key": operation["operation_key"],
            "action": operation["action"],
            "accepted": operation["action"] == "admit-public-retrieval",
            "artifact_identity": artifact,
            "exact_content_range": content_range,
            "ranked_object_ids": ranking,
            "cache_outcome": context["expected_cache_outcome"],
            "index_binding": context["index_binding"],
            "logical_bytes": logical_bytes,
            "physical_bytes": context["expected_physical_bytes"],
            "service_time_ms": 0.125,
            "telemetry_complete": True,
            "llm_called": operation["action"] == "rank-complete-candidate-set",
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        if self.corrupt_action == operation["action"]:
            result["operation_key"] = "wrong-operation"
        if (
            self.corrupt_index_binding
            and operation["action"] == "query-candidate-index-shard"
        ):
            result["index_binding"] = dict(result["index_binding"])
            result["index_binding"]["index_sha256"] = "f" * 64
        if self.corrupt_artifact_identity and artifact is not None:
            result["artifact_identity"] = dict(artifact)
            result["artifact_identity"]["artifact_sha256"] = "f" * 64
        return result


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class FullFlowW4CandidateCoordinatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = route_fixture.FullFlowW4CandidateRouteTest(
            "test_compiles_candidate_wide_routes_without_hidden_labels"
        )
        self.fixture.setUp()
        self.route_package = self.fixture._freeze("coordinator-routes")
        self.root = self.fixture.root

    def tearDown(self) -> None:
        self.fixture.tearDown()

    def _run(self, name: str = "coordinator-run"):
        executor = DeterministicOperationExecutor()
        output = self.root / name
        result = run_full_flow_w4_candidate_coordinator(
            self.route_package,
            run_id="w4-candidate-coordinator-test-v1",
            executor=executor,
            output_dir=output,
        )
        return executor, output, result

    def test_executes_activated_routes_and_emits_evaluator_observations(self):
        executor, output, result = self._run()
        self.assertEqual("COMPLETE", result["status"])
        verified = verify_full_flow_w4_candidate_coordinator_run(
            output, route_package_dir=self.route_package
        )
        self.assertEqual(
            "VERIFIED_W4_CANDIDATE_COORDINATOR_RUN", verified["status"]
        )
        self.assertEqual(16, verified["trial_count"])
        self.assertEqual(
            verified["activated_operation_count"], len(executor.calls)
        )
        self.assertGreater(verified["inactive_operation_count"], 0)
        observations = json.loads((output / OBSERVATIONS_NAME).read_text())
        self.assertEqual(
            W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
            observations["schema_version"],
        )
        self.assertEqual(16, len(observations["observations"]))
        candidates = set(self.fixture.candidates)
        for row in observations["observations"]:
            self.assertEqual(candidates, set(row["ranked_object_ids"]))

    def test_cache_lifecycle_is_independent_miss_then_hit(self):
        _, output, _ = self._run("cache-run")
        evidence = _jsonl(output / OPERATION_EVIDENCE_NAME)
        lookups = [
            row
            for row in evidence
            if row["action"] == "lookup"
            and row["execution_status"] == "COMPLETED"
        ]
        for design, node in (("D3", "N7"), ("D7", "N8")):
            rows = [row for row in lookups if row["design_id"] == design]
            self.assertTrue(rows)
            by_repetition = {
                repetition: {row["cache_outcome"] for row in rows
                             if row["repetition"] == repetition}
                for repetition in (0, 1)
            }
            self.assertEqual({"miss"}, by_repetition[0])
            self.assertEqual({"hit"}, by_repetition[1])
            report = json.loads((output / RUN_NAME).read_text())
            self.assertGreater(report["cache_lookup_hit_count_by_node"][node], 0)
            self.assertGreater(report["cache_lookup_miss_count_by_node"][node], 0)

    def test_index_prefix_exact_ranges_and_n4_selected_frames_are_executed(self):
        executor, output, _ = self._run("route-semantics")
        evidence = _jsonl(output / OPERATION_EVIDENCE_NAME)
        active = [row for row in evidence if row["execution_status"] == "COMPLETED"]
        self.assertTrue(any(
            row["action"] == "merge-complete-candidate-index" for row in active
        ))
        self.assertTrue(any(
            row["action"] == "access-exact-raw-range"
            and row["exact_content_range_sha256"] is not None
            for row in active
        ))
        selected_frames = [
            call for call in executor.calls
            if call["operation"]["action"] == "access-derived-artifact"
            and call["context"]["artifact_identity"]["representation_id"]
            == "sampled_frame_bundle"
            and call["operation"]["design_id"] in {"D2", "D6"}
        ]
        self.assertEqual(4, len(selected_frames))
        report = json.loads((output / RUN_NAME).read_text())
        self.assertTrue(report["candidate_prefix_activation_verified"])
        self.assertTrue(report["exact_n3_ranges_verified"])
        self.assertTrue(report["n4_digest_and_selected_frame_access_verified"])

    def test_multiple_index_shards_are_executed_before_one_merge(self):
        constant = (
            "pathfinder.simulator.full_flow_w4_candidate_routes."
            "W4_INDEX_QUERY_MAX_CANDIDATES"
        )
        with mock.patch(constant, 1):
            route_package = self.fixture._freeze("sharded-coordinator-routes")
            output = self.root / "sharded-coordinator-run"
            executor = DeterministicOperationExecutor()
            run_full_flow_w4_candidate_coordinator(
                route_package,
                run_id="w4-sharded-coordinator-v1",
                executor=executor,
                output_dir=output,
            )
            verify_full_flow_w4_candidate_coordinator_run(
                output, route_package_dir=route_package
            )
        first_indexed = [
            call
            for call in executor.calls
            if call["operation"]["design_id"] == "D1"
            and call["operation"]["repetition"] == 0
            and call["operation"]["action"]
            in {
                "query-candidate-index-shard",
                "merge-complete-candidate-index",
            }
        ]
        self.assertEqual(
            [
                "query-candidate-index-shard",
                "query-candidate-index-shard",
                "merge-complete-candidate-index",
            ],
            [call["operation"]["action"] for call in first_indexed],
        )
        bindings = [
            call["context"]["index_binding"]
            for call in first_indexed[:2]
        ]
        self.assertEqual([0, 1], [row["shard_index"] for row in bindings])

    def test_executor_identity_mismatch_fails_without_publishing(self):
        output = self.root / "bad-executor"
        with self.assertRaisesRegex(
            FullFlowW4CandidateCoordinatorError,
            "operation result identity",
        ):
            run_full_flow_w4_candidate_coordinator(
                self.route_package,
                run_id="w4-candidate-coordinator-bad-v1",
                executor=DeterministicOperationExecutor(
                    corrupt_action="access-raw"
                ),
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_semantic_executor_cannot_reorder_unactivated_index_tail(self):
        output = self.root / "bad-prefix-ranking"
        with self.assertRaisesRegex(
            FullFlowW4CandidateCoordinatorError,
            "outside the activated prefix",
        ):
            run_full_flow_w4_candidate_coordinator(
                self.route_package,
                run_id="w4-candidate-bad-prefix-v1",
                executor=DeterministicOperationExecutor(
                    corrupt_unactivated_tail=True
                ),
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_executor_cannot_change_frozen_index_or_artifact_identity(self):
        cases = (
            (
                "bad-index-binding",
                DeterministicOperationExecutor(corrupt_index_binding=True),
                "index binding",
            ),
            (
                "bad-artifact-binding",
                DeterministicOperationExecutor(corrupt_artifact_identity=True),
                "artifact identity",
            ),
        )
        for name, executor, message in cases:
            with self.subTest(name=name):
                output = self.root / name
                with self.assertRaisesRegex(
                    FullFlowW4CandidateCoordinatorError, message
                ):
                    run_full_flow_w4_candidate_coordinator(
                        self.route_package,
                        run_id=f"w4-{name}-v1",
                        executor=executor,
                        output_dir=output,
                    )
                self.assertFalse(output.exists())

    def test_tampering_is_detected(self):
        _, output, _ = self._run("tamper-run")
        evidence = output / OPERATION_EVIDENCE_NAME
        evidence.write_text(evidence.read_text() + " ", encoding="utf-8")
        with self.assertRaisesRegex(
            FullFlowW4CandidateCoordinatorError, "checksums failed"
        ):
            verify_full_flow_w4_candidate_coordinator_run(
                output, route_package_dir=self.route_package
            )

    def test_offline_adapter_is_byte_deterministic_for_same_run_id(self):
        first = self.root / "deterministic-a"
        second = self.root / "deterministic-b"
        for output in (first, second):
            run_full_flow_w4_candidate_coordinator(
                self.route_package,
                run_id="w4-deterministic-conformance-v1",
                executor=DeterministicW4CandidateOperationExecutor(),
                output_dir=output,
            )
        first_files = {
            path.name: path.read_bytes() for path in first.iterdir()
        }
        second_files = {
            path.name: path.read_bytes() for path in second.iterdir()
        }
        self.assertEqual(first_files, second_files)


class FullFlowW4CandidateEvaluatorIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        runtime_fixture.FullFlowW4RetrievalRuntimeTest.setUpClass()
        cls.root = runtime_fixture.FullFlowW4RetrievalRuntimeTest.root
        cls.runtime = runtime_fixture.FullFlowW4RetrievalRuntimeTest.overlay
        cls.contract = runtime_fixture.FullFlowW4RetrievalRuntimeTest.contract
        loaded = load_full_flow_w4_retrieval_runtime_inputs(cls.runtime)
        task = loaded.public_task
        candidates = [row["object_id"] for row in task["candidate_objects"]]
        task_by_id = {row["object_id"]: row for row in task["candidate_objects"]}
        cls.n3 = cls.root / "coordinator-eval-n3"
        cls.n4 = cls.root / "coordinator-eval-n4"
        cls.index = cls.root / "coordinator-eval-index"
        cls.ranges = cls.root / "coordinator-eval-ranges"
        for directory in (cls.n3, cls.n4, cls.index, cls.ranges):
            directory.mkdir()
        raw_rows = []
        derived_rows = []
        range_rows = []
        for position, object_id in enumerate(candidates):
            raw_sha = _sha256(f"raw|{object_id}".encode())
            raw_size = 1000 + position
            raw_rows.append({
                "object_id": object_id,
                "representation_id": "raw_video",
                "artifact_sha256": raw_sha,
                "artifact_size_bytes": raw_size,
                "catalog_version": "coordinator-eval-catalog-v1",
                "plan_ids": ["D0", "D1", "D4", "D5"],
            })
            digest = task_by_id[object_id]
            for representation_id, artifact_sha, artifact_size in (
                (
                    "multimodal_digest",
                    digest["artifact_sha256"],
                    digest["artifact_size_bytes"],
                ),
                (
                    "sampled_frame_bundle",
                    _sha256(f"frames|{object_id}".encode()),
                    2000 + position,
                ),
            ):
                derived_rows.append({
                    "object_id": object_id,
                    "representation_id": representation_id,
                    "artifact_sha256": artifact_sha,
                    "artifact_size_bytes": artifact_size,
                    "plan_ids": ["D2", "D3", "D6", "D7"],
                    "provenance": {
                        "schema_version": (
                            "pathfinder.simulator-derived-artifact-"
                            "provenance/v1alpha1"
                        ),
                        "producer_node_id": "N5",
                        "publication_source_id": f"published-{object_id}",
                        "source_representation_id": "raw_video",
                        "source_content_sha256": raw_sha,
                        "derivation_id": f"derive-{representation_id}",
                        "derivation_sha256": _sha256(
                            f"derive|{representation_id}".encode()
                        ),
                    },
                })
            range_rows.append({
                "object_id": object_id,
                "representation_id": "raw_video",
                "object_catalog_version": "coordinator-eval-catalog-v1",
                "full_artifact_size_bytes": raw_size,
                "full_artifact_sha256": raw_sha,
                "range_start": 0,
                "range_end": raw_size - 1,
                "range_size_bytes": raw_size,
                "range_sha256": raw_sha,
                "selection_semantics": "exact-full-object-fallback",
            })
        (cls.n3 / "raw-cold-data-plane.json").write_text(
            json.dumps({
                "catalog_version": "coordinator-eval-catalog-v1",
                "objects": raw_rows,
            }),
            encoding="utf-8",
        )
        (cls.n3 / "SHA256SUMS").write_text(
            "synthetic verifier-owned checksum commitment\n", encoding="utf-8"
        )
        (cls.n4 / "n4-derived-data-package.json").write_text(
            json.dumps({
                "catalog_version": "coordinator-eval-catalog-v1",
                "objects": derived_rows,
            }),
            encoding="utf-8",
        )
        source_manifest_sha = loaded.plan["source_sha256"][
            "representation_manifest"
        ]
        (cls.index / "lexical-index.json").write_text(
            json.dumps({
                "candidate_object_ids": candidates,
                "source_manifest_sha256": source_manifest_sha,
            }),
            encoding="utf-8",
        )
        (cls.ranges / "full-flow-exact-range-catalog.json").write_text(
            json.dumps({"entries": range_rows}), encoding="utf-8"
        )
        cls.routes = cls.root / "coordinator-evaluator-routes"
        prefix = "pathfinder.simulator.full_flow_w4_candidate_routes."
        with (
            mock.patch(
                prefix + "verify_raw_cold_data_plane_package",
                return_value={
                    "catalog_version": "coordinator-eval-catalog-v1"
                },
            ),
            mock.patch(
                prefix + "verify_n4_derived_data_package",
                return_value={
                    "package_sha256": "5" * 64,
                    "catalog_version": "coordinator-eval-catalog-v1",
                },
            ),
            mock.patch(
                prefix + "verify_n2_index_package",
                return_value={
                    "index_id": "coordinator-eval-index-v1",
                    "index_sha256": "6" * 64,
                    "document_count": len(candidates),
                },
            ),
            mock.patch(
                prefix + "verify_full_flow_exact_range_catalog",
                return_value={"catalog_sha256": "7" * 64},
            ),
        ):
            freeze_full_flow_w4_candidate_routes(
                cls.runtime,
                cls.n3,
                cls.n4,
                cls.index,
                cls.ranges,
                physical_plan_id="coordinator-evaluator-routes-v1",
                output_dir=cls.routes,
            )

    @classmethod
    def tearDownClass(cls) -> None:
        runtime_fixture.FullFlowW4RetrievalRuntimeTest.tearDownClass()

    def test_observation_output_is_accepted_by_hidden_n1_evaluator(self) -> None:
        run = self.root / "coordinator-evaluator-run"
        run_full_flow_w4_candidate_coordinator(
            self.routes,
            run_id="coordinator-evaluator-run-v1",
            executor=DeterministicW4CandidateOperationExecutor(),
            output_dir=run,
        )
        evaluation = self.root / "coordinator-hidden-evaluation"
        result = evaluate_full_flow_w4_retrieval(
            self.contract,
            run / OBSERVATIONS_NAME,
            output_dir=evaluation,
        )
        self.assertEqual("COMPLETE", result["status"])
        public_run = "\n".join(
            path.read_text(encoding="utf-8") for path in run.iterdir()
        )
        self.assertNotIn('"relevant_object_ids"', public_run)
        self.assertNotIn('"source_object_group"', public_run)


if __name__ == "__main__":
    unittest.main()
