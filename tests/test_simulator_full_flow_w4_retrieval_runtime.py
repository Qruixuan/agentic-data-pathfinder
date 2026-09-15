from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from pathfinder.simulator.full_flow_local_semantic_admission import TRIALS_NAME
from pathfinder.simulator.full_flow_w4_retrieval_contract import (
    PRIVATE_RELEVANCE_NAME,
    PUBLIC_DIRECTORY_NAME,
    PUBLIC_TASK_NAME,
    W4_RETRIEVAL_EVALUATION_SCHEMA_VERSION,
    evaluate_full_flow_w4_retrieval,
)
from pathfinder.simulator.full_flow_w4_retrieval_runtime import (
    OBSERVATIONS_NAME,
    RUNTIME_PLAN_NAME,
    RUNTIME_RUN_NAME,
    W4_RETRIEVAL_RANKER_RESULT_SCHEMA_VERSION,
    W4LexicalIndexRankingExecutor,
    FullFlowW4RetrievalRuntimeError,
    freeze_full_flow_w4_retrieval_runtime_overlay,
    load_full_flow_w4_retrieval_runtime_inputs,
    run_full_flow_w4_retrieval_ranker,
    verify_full_flow_w4_retrieval_ranker_run,
    verify_full_flow_w4_retrieval_runtime_overlay,
)
from pathfinder.simulator.index_service import (
    INDEX_SOURCE_SCHEMA_VERSION,
    N2IndexService,
    build_n2_index_package,
    verify_n2_index_package,
)
from tests import test_simulator_full_flow_w4_retrieval_contract as w4_fixture


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _build_index_package(
    root: Path,
    candidate_ids: list[str],
    *,
    index_id: str,
) -> tuple[Path, dict]:
    source = root / f"{index_id}-source.json"
    source.write_text(
        json.dumps(
            {
                "schema_version": INDEX_SOURCE_SCHEMA_VERSION,
                "index_id": index_id,
                "logical_node_id": "N2",
                "documents": [
                    {
                        "object_id": object_id,
                        "source_object_group": f"group-{position:04d}",
                        "visible_fields": {
                            "digest": f"public event candidate {position}",
                            "media_type": "video",
                        },
                    }
                    for position, object_id in enumerate(candidate_ids)
                ],
                "credentials_recorded": False,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    package = root / f"{index_id}-package"
    build_n2_index_package(source, output_dir=package)
    return package, verify_n2_index_package(package)


class FakeRanker:
    def __init__(self, *, incomplete: bool = False) -> None:
        self.incomplete = incomplete
        self.calls: list[str] = []

    def rank(self, *, run_id, trial, public_task):
        self.calls.append(trial["trial_key"])
        candidates = [
            row["object_id"] for row in public_task["candidate_objects"]
        ]
        ranking = [candidates[-1], *candidates[:-1]]
        if self.incomplete:
            ranking.pop()
        return {
            "schema_version": W4_RETRIEVAL_RANKER_RESULT_SCHEMA_VERSION,
            "status": "COMPLETE",
            "trial_key": trial["trial_key"],
            "retrieval_task_binding_sha256": public_task[
                "task_binding_sha256"
            ],
            "ranked_object_ids": ranking,
            "executor_evidence_sha256": _sha256(
                f"{run_id}|{trial['trial_key']}".encode("utf-8")
            ),
            "telemetry_complete": True,
            "llm_called": False,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }


class FakeIndexClient:
    def __init__(self, node_id: str, package_dir: Path) -> None:
        self.node_id = node_id
        self.service = N2IndexService(package_dir, node_id=node_id)
        package = self.service.package
        self.index_id = package["index_id"]
        self.index_sha256 = package["index_sha256"]
        self.requests: list[dict] = []

    def health(self):
        return self.service.health()

    def query_public(self, value):
        request = dict(value)
        self.requests.append(request)
        return self.service.query_public(request)


class TamperingIndexClient(FakeIndexClient):
    def query_public(self, value):
        result = super().query_public(value)
        result["ranked_candidates"][0]["visible_fields_sha256"] = "f" * 64
        result["ranking_sha256"] = _sha256(
            json.dumps(
                result["ranked_candidates"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        core = dict(result)
        core.pop("result_content_sha256")
        result["result_content_sha256"] = _sha256(
            json.dumps(
                core,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        return result


class FullFlowW4RetrievalRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        w4_fixture.FullFlowW4RetrievalContractTest.setUpClass()
        source = w4_fixture.FullFlowW4RetrievalContractTest(
            "test_freezes_public_query_candidate_set_and_private_relevance"
        )
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.contract = source._freeze("runtime-bridge-contract")
        public = cls.contract / PUBLIC_DIRECTORY_NAME
        task = json.loads((public / PUBLIC_TASK_NAME).read_text())
        cls.index_package, cls.index = _build_index_package(
            cls.root,
            [str(row["object_id"]) for row in task["candidate_objects"]],
            index_id="w4-public-index-v1",
        )
        bindings = _jsonl(public / "w4-retrieval-trial-bindings.jsonl")
        route_by_design = {
            "D0": ("raw", "N7"),
            "D1": ("indexed-raw", "N7"),
            "D2": ("remote-derived", "N7"),
            "D3": ("local-cache-derived", "N7"),
            "D4": ("raw", "N8"),
            "D5": ("indexed-raw", "N8"),
            "D6": ("remote-derived", "N8"),
            "D7": ("local-cache-derived", "N8"),
        }
        fake_trials = []
        for binding in bindings:
            route, executor = route_by_design[binding["design_id"]]
            fake_trials.append(
                {
                    "trial_key": binding["trial_key"],
                    "order_index": binding["order_index"],
                    "workload_class": "W4",
                    "design_id": binding["design_id"],
                    "repetition": binding["repetition"],
                    "route_family": route,
                    "executor_node_id": executor,
                    "artifact_object_id": "object-alpha",
                    "public_task_binding_sha256": binding[
                        "replaces_multiple_choice_task_binding_sha256"
                    ],
                    "public_task_binding": {
                        "answer_options": [
                            {"option_id": "A", "text": "placeholder"}
                        ]
                    },
                    "bound_stage_sha256": ["a" * 64],
                }
            )
        cls.local_admission = cls.root / "fake-local-admission"
        cls.local_admission.mkdir()
        (cls.local_admission / TRIALS_NAME).write_bytes(
            b"".join(
                json.dumps(
                    row,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                + b"\n"
                for row in fake_trials
            )
        )
        loaded = SimpleNamespace(bound_trials=tuple(fake_trials))
        cls.loaded = loaded
        cls.overlay = cls.root / "runtime-overlay"
        with (
            mock.patch(
                "pathfinder.simulator.full_flow_w4_retrieval_runtime."
                "verify_full_flow_local_semantic_runtime_package",
                return_value={
                    "promotion_id": "fake-local-promotion-v1",
                    "admission_sha256": "b" * 64,
                },
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_w4_retrieval_runtime."
                "load_full_flow_local_semantic_execution_inputs",
                return_value=loaded,
            ),
        ):
            freeze_full_flow_w4_retrieval_runtime_overlay(
                cls.contract,
                cls.local_admission,
                runtime_overlay_id="w4-public-ranker-runtime-v1",
                output_dir=cls.overlay,
            )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()
        w4_fixture.FullFlowW4RetrievalContractTest.tearDownClass()

    def test_freezes_public_only_runtime_without_relabelling_matrix(self) -> None:
        report = verify_full_flow_w4_retrieval_runtime_overlay(self.overlay)
        self.assertEqual("VERIFIED_PUBLIC_W4_RANKER_RUNTIME", report["status"])
        self.assertEqual(16, report["w4_trial_count"])
        self.assertTrue(report["public_ranker_adapter_contract_complete"])
        self.assertFalse(report["physical_design_retrieval_comparison_ready"])
        self.assertFalse(report["current_64_trial_matrix_modified"])
        self.assertFalse(report["n1_private_relevance_read"])

        plan = json.loads((self.overlay / RUNTIME_PLAN_NAME).read_text())
        boundary = plan["runtime_boundary"]
        self.assertEqual(
            "multiple-choice-placeholder",
            boundary["current_64_trial_matrix_native_w4_semantics"],
        )
        self.assertFalse(
            boundary[
                "current_single_object_stage_dag_expanded_to_candidate_corpus"
            ]
        )
        public_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in self.overlay.iterdir()
        )
        hidden = json.loads(
            (
                self.contract
                / "n1-private"
                / PRIVATE_RELEVANCE_NAME
            ).read_text(encoding="utf-8")
        )
        self.assertNotIn("relevant_object_ids", public_text)
        self.assertNotIn(hidden["source_object_group"], public_text)

    def test_ranker_output_is_directly_consumed_by_n1_evaluator(self) -> None:
        ranker = FakeRanker()
        run_dir = self.root / "ranker-run"
        result = run_full_flow_w4_retrieval_ranker(
            self.overlay,
            run_id="w4-ranker-run-v1",
            executor=ranker,
            output_dir=run_dir,
        )
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(16, len(ranker.calls))
        verified = verify_full_flow_w4_retrieval_ranker_run(
            run_dir,
            runtime_overlay_dir=self.overlay,
        )
        self.assertEqual(
            "VERIFIED_SOURCE_BOUND_PUBLIC_W4_RANKER_RUN",
            verified["status"],
        )
        self.assertTrue(verified["ready_for_n1_hidden_relevance_evaluation"])
        self.assertFalse(verified["physical_design_retrieval_comparison_ready"])

        evaluation = self.root / "n1-evaluation"
        evaluate_full_flow_w4_retrieval(
            self.contract,
            run_dir / OBSERVATIONS_NAME,
            output_dir=evaluation,
        )
        report = json.loads(
            (evaluation / "w4-retrieval-evaluation.json").read_text()
        )
        self.assertEqual(W4_RETRIEVAL_EVALUATION_SCHEMA_VERSION, report[
            "schema_version"
        ])
        self.assertEqual("COMPLETE", report["status"])

    def test_concrete_lexical_adapter_uses_bound_index_nodes(self) -> None:
        clients = {
            node_id: FakeIndexClient(node_id, self.index_package)
            for node_id in ("N2", "N7", "N8")
        }
        adapter = W4LexicalIndexRankingExecutor(
            clients=clients,
            index_package_dir=self.index_package,
            index_id=self.index["index_id"],
            index_sha256=self.index["index_sha256"],
            source_manifest_sha256=self.index["source_manifest_sha256"],
        )
        output = self.root / "lexical-adapter-run"
        result = run_full_flow_w4_retrieval_ranker(
            self.overlay,
            run_id="w4-lexical-adapter-run-v1",
            executor=adapter,
            output_dir=output,
        )
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(12, len(clients["N2"].requests))
        self.assertEqual(2, len(clients["N7"].requests))
        self.assertEqual(2, len(clients["N8"].requests))
        for node_id, client in clients.items():
            self.assertTrue(client.requests)
            self.assertTrue(all(
                request["requested_node_id"] == node_id
                and request["top_k"]
                == len(request["candidate_object_ids"])
                for request in client.requests
            ))
        report = json.loads((output / RUNTIME_RUN_NAME).read_text())
        self.assertEqual("public-ranker-output-only", report["quality_claim_scope"])
        self.assertFalse(report["physical_candidate_route_execution_verified"])

    def test_lexical_adapter_shards_large_candidate_corpus(self) -> None:
        candidate_ids = [f"candidate-{index:04d}" for index in range(1001)]
        fixture_root = Path(tempfile.mkdtemp(dir=self.root))
        package_dir, package = _build_index_package(
            fixture_root,
            candidate_ids,
            index_id="w4-large-public-index-v1",
        )
        clients = {
            node_id: FakeIndexClient(node_id, package_dir)
            for node_id in ("N2", "N7", "N8")
        }
        adapter = W4LexicalIndexRankingExecutor(
            clients=clients,
            index_package_dir=package_dir,
            index_id=package["index_id"],
            index_sha256=package["index_sha256"],
            source_manifest_sha256=package["source_manifest_sha256"],
        )
        loaded = load_full_flow_w4_retrieval_runtime_inputs(self.overlay)
        task = json.loads(json.dumps(loaded.public_task))
        task["candidate_objects"] = [
            {
                "object_id": object_id,
                "representation_id": "multimodal_digest",
                "artifact_sha256": _sha256(f"digest-{index}".encode()),
                "artifact_size_bytes": 10 + index,
            }
            for index, object_id in enumerate(candidate_ids)
        ]
        task["candidate_set_sha256"] = _sha256(
            json.dumps(
                task["candidate_objects"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
        task["required_ranking_length"] = len(task["candidate_objects"])
        core = dict(task)
        core.pop("task_binding_sha256")
        task["task_binding_sha256"] = _sha256(
            json.dumps(
                core,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
        result = adapter.rank(
            run_id="w4-sharded-ranking-v1",
            trial=loaded.trials[0],
            public_task=task,
        )
        self.assertEqual(1001, len(result["ranked_object_ids"]))
        self.assertEqual([1000, 1], [
            row["top_k"] for row in clients["N2"].requests
        ])

    def test_lexical_adapter_rejects_server_asserted_nonreplayable_result(
        self,
    ) -> None:
        clients = {
            node_id: (
                TamperingIndexClient(node_id, self.index_package)
                if node_id == "N2"
                else FakeIndexClient(node_id, self.index_package)
            )
            for node_id in ("N2", "N7", "N8")
        }
        adapter = W4LexicalIndexRankingExecutor(
            clients=clients,
            index_package_dir=self.index_package,
            index_id=self.index["index_id"],
            index_sha256=self.index["index_sha256"],
            source_manifest_sha256=self.index["source_manifest_sha256"],
        )
        loaded = load_full_flow_w4_retrieval_runtime_inputs(self.overlay)
        n2_trial = next(
            row for row in loaded.trials if row["design_id"] == "D0"
        )
        with self.assertRaisesRegex(
            FullFlowW4RetrievalRuntimeError,
            "offline replay",
        ):
            adapter.rank(
                run_id="w4-tampered-index-response-v1",
                trial=n2_trial,
                public_task=loaded.public_task,
            )

    def test_incomplete_ranking_fails_without_publishing_output(self) -> None:
        output = self.root / "incomplete-run"
        with self.assertRaisesRegex(
            FullFlowW4RetrievalRuntimeError,
            "complete candidate permutation",
        ):
            run_full_flow_w4_retrieval_ranker(
                self.overlay,
                run_id="w4-incomplete-run-v1",
                executor=FakeRanker(incomplete=True),
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_public_runtime_loader_never_needs_private_contract_half(self) -> None:
        copied = self.root / "standalone-public-runtime"
        shutil.copytree(self.overlay, copied)
        moved_contract = self.root / "contract-held-at-n1"
        self.contract.rename(moved_contract)
        try:
            loaded = load_full_flow_w4_retrieval_runtime_inputs(copied)
            self.assertEqual(16, len(loaded.trials))
            self.assertEqual("W4", loaded.public_task["workload_class"])
        finally:
            moved_contract.rename(self.contract)

    def test_freeze_reads_only_public_contract_half(self) -> None:
        public_only = self.root / "public-only-contract"
        public_only.mkdir()
        shutil.copytree(
            self.contract / PUBLIC_DIRECTORY_NAME,
            public_only / PUBLIC_DIRECTORY_NAME,
        )
        output = self.root / "public-only-overlay"
        with (
            mock.patch(
                "pathfinder.simulator.full_flow_w4_retrieval_runtime."
                "verify_full_flow_local_semantic_runtime_package",
                return_value={
                    "promotion_id": "fake-local-promotion-v1",
                    "admission_sha256": "b" * 64,
                },
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_w4_retrieval_runtime."
                "load_full_flow_local_semantic_execution_inputs",
                return_value=self.loaded,
            ),
        ):
            freeze_full_flow_w4_retrieval_runtime_overlay(
                public_only,
                self.local_admission,
                runtime_overlay_id="public-only-runtime-v1",
                output_dir=output,
            )
        self.assertTrue(output.is_dir())

    def test_output_cannot_overlap_input(self) -> None:
        with self.assertRaisesRegex(
            FullFlowW4RetrievalRuntimeError,
            "overlaps an input",
        ):
            freeze_full_flow_w4_retrieval_runtime_overlay(
                self.contract,
                self.local_admission,
                runtime_overlay_id="overlap-runtime-v1",
                output_dir=self.contract / "invalid-child",
            )
        self.assertFalse((self.contract / "invalid-child").exists())

    def test_tampering_is_detected(self) -> None:
        copied = self.root / "tampered-runtime"
        if copied.exists():
            shutil.rmtree(copied)
        shutil.copytree(self.overlay, copied)
        task_path = copied / "w4-retrieval-runtime-task.json"
        task_path.write_text(
            task_path.read_text(encoding="utf-8") + " ",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            FullFlowW4RetrievalRuntimeError, "checksum mismatch"
        ):
            verify_full_flow_w4_retrieval_runtime_overlay(copied)

    def test_run_report_preserves_claim_boundary(self) -> None:
        run_dir = self.root / "claim-boundary-run"
        run_full_flow_w4_retrieval_ranker(
            self.overlay,
            run_id="w4-claim-boundary-v1",
            executor=FakeRanker(),
            output_dir=run_dir,
        )
        report = json.loads((run_dir / RUNTIME_RUN_NAME).read_text())
        self.assertEqual("public-ranker-output-only", report["quality_claim_scope"])
        self.assertFalse(report["current_64_trial_matrix_modified"])
        self.assertFalse(report["physical_candidate_route_execution_verified"])
        self.assertFalse(report["physical_design_retrieval_comparison_ready"])
        self.assertFalse(report["eligible_for_scientific_claims"])

    def test_integrity_only_verification_is_not_evaluation_ready(self) -> None:
        run_dir = self.root / "integrity-only-run"
        run_full_flow_w4_retrieval_ranker(
            self.overlay,
            run_id="w4-integrity-only-v1",
            executor=FakeRanker(),
            output_dir=run_dir,
        )
        checked = verify_full_flow_w4_retrieval_ranker_run(run_dir)
        self.assertFalse(checked["source_bound"])
        self.assertFalse(checked["ready_for_n1_hidden_relevance_evaluation"])


if __name__ == "__main__":
    unittest.main()
