from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_semantic_matrix import (
    ARTIFACT_BINDING_SET_SCHEMA_VERSION,
    compile_full_flow_semantic_matrix,
)
from pathfinder.simulator.full_flow_w4_retrieval_contract import (
    PRIVATE_DIRECTORY_NAME,
    PRIVATE_RELEVANCE_NAME,
    PUBLIC_BINDINGS_NAME,
    PUBLIC_COMMITMENT_NAME,
    PUBLIC_DIRECTORY_NAME,
    PUBLIC_TASK_NAME,
    W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
    FullFlowW4RetrievalContractError,
    evaluate_full_flow_w4_retrieval,
    freeze_full_flow_w4_retrieval_contract,
    verify_full_flow_w4_retrieval_contract,
    verify_full_flow_w4_retrieval_evaluation,
)
from pathfinder.simulator.hidden_oracle import (
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    build_n1_public_task_binding,
)
from pathfinder.simulator.portable import build_portable_execution_plan
from pathfinder.simulator.retrieval import RETRIEVAL_CONFIG_SCHEMA_VERSION


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"

WORKLOADS = {
    "smoke-descriptive": ("video-descriptive", "video_qa_descriptive"),
    "smoke-temporal": ("video-temporal", "video_qa_temporal"),
    "smoke-causal": ("video-causal", "video_qa_causal"),
    "smoke-retrieval": ("video-retrieval-target", "video_retrieval"),
}


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


class FullFlowW4RetrievalContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        cls.logical = cls.root / "logical"
        cls.semantic = cls.root / "semantic"
        cls.public_tasks = cls.root / "public-tasks.json"
        cls.artifact_bindings = cls.root / "artifact-bindings.json"
        cls.representations = cls.root / "representations"
        cls.representations.mkdir()

        build_portable_execution_plan(SCENARIO, output_dir=cls.portable)
        plan_container_backend(
            SCENARIO,
            cls.portable,
            CONTAINER_SPEC,
            output_dir=cls.container,
        )
        compile_full_flow_logical_routes(
            SCENARIO,
            cls.container,
            output_dir=cls.logical,
        )

        cls.artifact_ids = {
            "video-descriptive": "object-bravo",
            "video-temporal": "object-charlie",
            "video-causal": "object-delta",
            "video-retrieval-target": "object-alpha",
        }
        tasks = []
        for workload_id, (logical_object_id, task_class) in sorted(
            WORKLOADS.items()
        ):
            tasks.append(
                build_n1_public_task_binding(
                    workload_id=workload_id,
                    object_id=cls.artifact_ids[logical_object_id],
                    task_class_id=task_class,
                    question=f"Answer the public task for {workload_id}.",
                    answer_options=[
                        {"option_id": "A", "text": "First option."},
                        {"option_id": "B", "text": "Second option."},
                    ],
                    success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
                )
            )
        cls.public_tasks.write_bytes(
            _json_bytes(
                {
                    "schema_version": "pathfinder.public-task-set/v1alpha1",
                    "task_plane_id": "w4-contract-test-plane-v1",
                    "tasks": tasks,
                    "label_values_included": False,
                    "credentials_recorded": False,
                }
            )
        )

        used_representations = {
            "video-descriptive": {"multimodal_digest", "raw_video"},
            "video-temporal": {"raw_video", "sampled_frame_bundle"},
            "video-causal": {
                "multimodal_digest",
                "raw_video",
                "sampled_frame_bundle",
            },
            "video-retrieval-target": {
                "multimodal_digest",
                "raw_video",
                "sampled_frame_bundle",
            },
        }
        objects = []
        for logical_object_id in sorted(cls.artifact_ids):
            representations = []
            for representation_id, size in (
                ("multimodal_digest", 120),
                ("raw_video", 5000),
                ("sampled_frame_bundle", 1600),
            ):
                if representation_id not in used_representations[logical_object_id]:
                    continue
                representations.append(
                    {
                        "representation_id": representation_id,
                        "artifact_sha256": _sha256(
                            f"{logical_object_id}|{representation_id}".encode()
                        ),
                        "artifact_size_bytes": size,
                        "object_catalog_version": "w4-contract-artifacts-v1",
                    }
                )
            objects.append(
                {
                    "logical_object_id": logical_object_id,
                    "artifact_object_id": cls.artifact_ids[logical_object_id],
                    "representations": representations,
                }
            )
        cls.artifact_bindings.write_bytes(
            _json_bytes(
                {
                    "schema_version": ARTIFACT_BINDING_SET_SCHEMA_VERSION,
                    "binding_set_id": "w4-contract-artifact-bindings-v1",
                    "objects": objects,
                    "credentials_recorded": False,
                }
            )
        )
        compile_full_flow_semantic_matrix(
            cls.logical,
            SCENARIO,
            cls.container,
            cls.public_tasks,
            cls.artifact_bindings,
            output_dir=cls.semantic,
        )

        documents = {
            "object-alpha": "elephant kicks a soccer ball toward a goal",
            "object-bravo": "two musicians play guitar near a woman",
            "object-charlie": "a cyclist follows a white van",
            "object-delta": "a newborn is fed from a bottle",
            "object-echo": "people swim below a waterfall",
            "object-foxtrot": "a girl pets a brown dog on grass",
        }
        representation_objects = []
        for object_id, text in documents.items():
            directory = cls.representations / object_id
            directory.mkdir()
            payload = text.encode("utf-8")
            (directory / "multimodal_digest.txt").write_bytes(payload)
            representation_objects.append(
                {
                    "object_id": object_id,
                    "representations": {
                        "multimodal_digest": {
                            "path": f"{object_id}/multimodal_digest.txt",
                            "size_bytes": len(payload),
                            "sha256": _sha256(payload),
                        }
                    },
                }
            )
        cls.representation_manifest = (
            cls.representations / "generation-manifest.json"
        )
        cls.representation_manifest.write_bytes(
            _json_bytes(
                {
                    "objects": representation_objects,
                    "credentials_recorded": False,
                }
            )
        )
        cls.config = cls.root / "retrieval-config.json"
        cls.base_config = {
            "schema_version": RETRIEVAL_CONFIG_SCHEMA_VERSION,
            "retrieval_id": "w4-prospective-test-v1",
            "candidate_corpus": "all-representation-manifest-objects",
            "independent_unit": "source-object-group",
            "annotation_status": "operator-verified",
            "index": {
                "kind": "bm25-lexical-v1",
                "k1": 1.2,
                "b": 0.75,
                "top_k": [1, 3],
            },
            "queries": [
                {
                    "query_id": "query-train",
                    "query_text": "Find musicians playing guitar",
                    "relevant_object_ids": ["object-bravo"],
                    "source_object_group": "group-bravo",
                    "split": "train",
                },
                {
                    "query_id": "query-validation",
                    "query_text": "Find a newborn being fed",
                    "relevant_object_ids": ["object-delta"],
                    "source_object_group": "group-delta",
                    "split": "validation",
                },
                {
                    "query_id": "query-hidden-test",
                    "query_text": "Find an elephant kicking a soccer ball",
                    "relevant_object_ids": ["object-alpha"],
                    "source_object_group": "group-alpha",
                    "split": "test",
                },
            ],
        }
        cls.config.write_bytes(_json_bytes(cls.base_config))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _freeze(self, name: str, config: Path | None = None) -> Path:
        output = self.root / name
        if output.exists():
            shutil.rmtree(output)
        freeze_full_flow_w4_retrieval_contract(
            self.semantic,
            config or self.config,
            self.representation_manifest,
            selected_query_id="query-hidden-test",
            contract_id="full-flow-w4-prospective-v1",
            output_dir=output,
        )
        return output

    def _observations(self, contract: Path, ranking: list[str]) -> Path:
        bindings = _jsonl(
            contract / PUBLIC_DIRECTORY_NAME / PUBLIC_BINDINGS_NAME
        )
        sequence = len(list(self.root.glob("observations-*")))
        path = self.root / f"observations-{sequence}.json"
        path.write_bytes(
            _json_bytes(
                {
                    "schema_version": W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
                    "contract_id": "full-flow-w4-prospective-v1",
                    "observations": [
                        {
                            "trial_key": row["trial_key"],
                            "retrieval_task_binding_sha256": row[
                                "retrieval_task_binding_sha256"
                            ],
                            "ranked_object_ids": ranking,
                            "outcome_type": "completed",
                            "telemetry_complete": True,
                        }
                        for row in bindings
                    ],
                    "credentials_recorded": False,
                }
            )
        )
        return path

    def test_freezes_public_query_candidate_set_and_private_relevance(self) -> None:
        contract = self._freeze("complete-contract")
        verified = verify_full_flow_w4_retrieval_contract(contract)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(6, verified["candidate_object_count"])
        self.assertEqual(16, verified["w4_trial_count"])
        self.assertFalse(verified["hidden_relevance_values_returned"])

        public_root = contract / PUBLIC_DIRECTORY_NAME
        public_values = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in public_root.iterdir()
            if path.suffix == ".json"
        ]

        def keys(value: object) -> set[str]:
            if isinstance(value, dict):
                return set(value) | set().union(
                    *(keys(child) for child in value.values())
                )
            if isinstance(value, list):
                return (
                    set().union(*(keys(child) for child in value))
                    if value
                    else set()
                )
            return set()

        public_keys = set().union(*(keys(value) for value in public_values))
        self.assertNotIn("relevant_object_ids", public_keys)
        self.assertNotIn("source_object_group", public_keys)
        private = json.loads(
            (
                contract / PRIVATE_DIRECTORY_NAME / PRIVATE_RELEVANCE_NAME
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(["object-alpha"], private["relevant_object_ids"])

    def test_marks_existing_matrix_as_placeholder_and_blocks_execution(self) -> None:
        contract = self._freeze("blocked-contract")
        commitment = json.loads(
            (
                contract / PUBLIC_DIRECTORY_NAME / PUBLIC_COMMITMENT_NAME
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            "multiple-choice-placeholder",
            commitment["current_matrix_native_w4_semantics"],
        )
        self.assertTrue(commitment["overlay_required_at_compile_and_runtime"])
        self.assertFalse(commitment["runtime_executor_binding_present"])
        self.assertFalse(commitment["ready_for_current_64_trial_execution"])
        self.assertEqual(
            "W4_RETRIEVAL_OVERLAY_NOT_YET_CONSUMED_BY_RUNTIME",
            commitment["readiness_blocker"],
        )
        bindings = _jsonl(
            contract / PUBLIC_DIRECTORY_NAME / PUBLIC_BINDINGS_NAME
        )
        self.assertEqual(16, len(bindings))
        self.assertEqual({f"D{index}" for index in range(8)}, {
            row["design_id"] for row in bindings
        })

    def test_contract_is_byte_deterministic_and_tamper_evident(self) -> None:
        first = self._freeze("deterministic-one")
        second = self._freeze("deterministic-two")
        self.assertEqual(_snapshot(first), _snapshot(second))
        task = first / PUBLIC_DIRECTORY_NAME / PUBLIC_TASK_NAME
        task.write_text(task.read_text(encoding="utf-8") + " ", encoding="utf-8")
        with self.assertRaisesRegex(
            FullFlowW4RetrievalContractError, "checksum mismatch"
        ):
            verify_full_flow_w4_retrieval_contract(first)
        (second / "accidental-private-copy.json").write_text(
            "{}", encoding="utf-8"
        )
        with self.assertRaisesRegex(
            FullFlowW4RetrievalContractError, "root structure"
        ):
            verify_full_flow_w4_retrieval_contract(second)

    def test_refuses_draft_labels_and_non_test_selection(self) -> None:
        draft = self.root / "draft-config.json"
        payload = json.loads(json.dumps(self.base_config))
        payload["annotation_status"] = (
            "ai-drafted-requires-operator-verification"
        )
        draft.write_bytes(_json_bytes(payload))
        with self.assertRaisesRegex(
            FullFlowW4RetrievalContractError, "operator-verified"
        ):
            self._freeze("draft-refused", draft)

        with self.assertRaisesRegex(
            FullFlowW4RetrievalContractError, "test split"
        ):
            freeze_full_flow_w4_retrieval_contract(
                self.semantic,
                self.config,
                self.representation_manifest,
                selected_query_id="query-train",
                contract_id="full-flow-w4-train-refused-v1",
                output_dir=self.root / "train-refused",
            )

    def test_scores_rankings_without_emitting_labels_or_raw_rankings(self) -> None:
        contract = self._freeze("evaluation-contract")
        task = json.loads(
            (contract / PUBLIC_DIRECTORY_NAME / PUBLIC_TASK_NAME).read_text(
                encoding="utf-8"
            )
        )
        candidates = [row["object_id"] for row in task["candidate_objects"]]
        ranking = ["object-alpha", *[
            object_id for object_id in candidates if object_id != "object-alpha"
        ]]
        observations = self._observations(contract, ranking)
        output = self.root / "evaluation"
        result = evaluate_full_flow_w4_retrieval(
            contract, observations, output_dir=output
        )
        self.assertEqual("COMPLETE", result["status"])
        integrity = verify_full_flow_w4_retrieval_evaluation(output)
        self.assertEqual("VERIFIED_INTEGRITY", integrity["status"])
        self.assertFalse(integrity["source_bound_replay_performed"])
        verified = verify_full_flow_w4_retrieval_evaluation(
            output,
            contract_dir=contract,
            observations_path=observations,
        )
        self.assertEqual("VERIFIED_SOURCE_BOUND", verified["status"])
        self.assertTrue(verified["source_bound_replay_performed"])
        report = json.loads(
            (output / "w4-retrieval-evaluation.json").read_text(
                encoding="utf-8"
            )
        )
        overall = report["aggregates"][0]
        self.assertEqual(1.0, overall["mrr"])
        self.assertEqual(1.0, overall["mean_recall_at_1"])
        self.assertEqual(1.0, overall["mean_ndcg_at_1"])
        public_output = "\n".join(
            path.read_text(encoding="utf-8") for path in output.iterdir()
        )
        self.assertNotIn("relevant_object_ids", public_output)
        self.assertNotIn("ranked_object_ids", public_output)
        self.assertNotIn("source_object_group", public_output)

    def test_evaluation_requires_all_trials_and_a_complete_permutation(self) -> None:
        contract = self._freeze("invalid-observations-contract")
        task = json.loads(
            (contract / PUBLIC_DIRECTORY_NAME / PUBLIC_TASK_NAME).read_text()
        )
        candidates = [row["object_id"] for row in task["candidate_objects"]]
        observations = self._observations(contract, candidates)
        payload = json.loads(observations.read_text(encoding="utf-8"))
        payload["observations"].pop()
        observations.write_bytes(_json_bytes(payload))
        with self.assertRaisesRegex(
            FullFlowW4RetrievalContractError, "count is incomplete"
        ):
            evaluate_full_flow_w4_retrieval(
                contract,
                observations,
                output_dir=self.root / "incomplete-evaluation",
            )

        observations = self._observations(contract, candidates[:-1])
        with self.assertRaisesRegex(
            FullFlowW4RetrievalContractError, "rank every candidate"
        ):
            evaluate_full_flow_w4_retrieval(
                contract,
                observations,
                output_dir=self.root / "short-evaluation",
            )


if __name__ == "__main__":
    unittest.main()
