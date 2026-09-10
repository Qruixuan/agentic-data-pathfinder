from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.cli import main as cli_main
from pathfinder.simulator import (
    RETRIEVAL_ANSWER_SCHEMA_VERSION,
    RETRIEVAL_CONFIG_SCHEMA_VERSION,
    SimulatorRetrievalError,
    build_simulator_retrieval_cohort,
    verify_simulator_retrieval,
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class SimulatorRetrievalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.representations = self.root / "representations"
        self.representations.mkdir()
        documents = {
            "object-alpha": "two musicians play guitar while a woman watches",
            "object-beta": "a cyclist follows a white van on a street",
            "object-gamma": "a woman feeds a newborn baby with a bottle",
            "object-delta": "a girl pets a large brown dog on grass",
            "object-epsilon": "an elephant kicks a soccer ball toward a goal",
            "object-zeta": "people swim below a tiered waterfall",
        }
        objects = []
        for object_id, text in documents.items():
            directory = self.representations / object_id
            directory.mkdir()
            payload = text.encode("utf-8")
            path = directory / "multimodal_digest.txt"
            path.write_bytes(payload)
            objects.append({
                "object_id": object_id,
                "representations": {
                    "multimodal_digest": {
                        "path": f"{object_id}/multimodal_digest.txt",
                        "size_bytes": len(payload),
                        "sha256": _sha256(payload),
                    }
                },
            })
        self.manifest = self.representations / "generation-manifest.json"
        self.manifest.write_text(
            json.dumps({
                "credentials_recorded": False,
                "objects": objects,
            }),
            encoding="utf-8",
        )
        self.config = self.root / "retrieval.json"
        self.config_payload = {
            "schema_version": RETRIEVAL_CONFIG_SCHEMA_VERSION,
            "retrieval_id": "retrieval-test-v1",
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
                    "query_id": "query-guitar",
                    "query_text": "Find musicians playing guitar near a woman",
                    "relevant_object_ids": ["object-alpha"],
                    "source_object_group": "group-alpha",
                    "split": "train",
                },
                {
                    "query_id": "query-baby",
                    "query_text": "Find a newborn being fed with a bottle",
                    "relevant_object_ids": ["object-gamma"],
                    "source_object_group": "group-gamma",
                    "split": "validation",
                },
                {
                    "query_id": "query-elephant",
                    "query_text": "Find an elephant kicking a soccer ball",
                    "relevant_object_ids": ["object-epsilon"],
                    "source_object_group": "group-epsilon",
                    "split": "test",
                },
            ],
        }
        self._write_config()

    def _write_config(self) -> None:
        self.config.write_text(
            json.dumps(self.config_payload),
            encoding="utf-8",
        )

    def _build(self, output: Path, answers: Path | None = None) -> dict:
        return build_simulator_retrieval_cohort(
            self.config,
            self.manifest,
            output_dir=output,
            answer_observations_path=answers,
        )

    def test_builds_real_deterministic_index_and_metrics(self) -> None:
        first = self.root / "first"
        second = self.root / "second"
        result = self._build(first)
        self._build(second)
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(3, result["query_count"])
        self.assertEqual(6, result["candidate_object_count"])
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        evaluation = json.loads(
            (first / "retrieval_evaluation.json").read_text(encoding="utf-8")
        )
        overall = evaluation["aggregates"][0]
        self.assertEqual(1.0, overall["recall_at_1"])
        self.assertEqual(1.0, overall["mrr"])
        self.assertIsNone(overall["answer_accuracy"])
        self.assertIsNone(overall["joint_success_rate"])
        verified = verify_simulator_retrieval(first)
        self.assertEqual("VERIFIED", verified["status"])

    def test_bound_answer_observations_add_joint_success(self) -> None:
        preliminary = self.root / "preliminary"
        self._build(preliminary)
        rankings = {
            row["query_id"]: row["ranking"]
            for row in (
                json.loads(line)
                for line in (preliminary / "rankings.jsonl").read_text().splitlines()
            )
        }
        observations = []
        for index, query_id in enumerate(sorted(rankings)):
            observations.append({
                "query_id": query_id,
                "top_k": 1,
                "retrieved_object_ids": [rankings[query_id][0]["object_id"]],
                "outcome_type": "completed",
                "telemetry_complete": True,
                "answer_correct": index != 0,
            })
        path = self.root / "answers.json"
        path.write_text(json.dumps({
            "schema_version": RETRIEVAL_ANSWER_SCHEMA_VERSION,
            "retrieval_id": "retrieval-test-v1",
            "credentials_recorded": False,
            "observations": observations,
        }), encoding="utf-8")
        output = self.root / "answered"
        self._build(output, path)
        evaluation = json.loads(
            (output / "retrieval_evaluation.json").read_text(encoding="utf-8")
        )
        overall = evaluation["aggregates"][0]
        self.assertEqual(3, overall["answer_observation_count"])
        self.assertAlmostEqual(2 / 3, overall["answer_accuracy"])
        self.assertAlmostEqual(2 / 3, overall["joint_success_rate"])

    def test_answer_observation_must_bind_exact_ranking(self) -> None:
        path = self.root / "answers.json"
        path.write_text(json.dumps({
            "schema_version": RETRIEVAL_ANSWER_SCHEMA_VERSION,
            "retrieval_id": "retrieval-test-v1",
            "credentials_recorded": False,
            "observations": [{
                "query_id": "query-guitar",
                "top_k": 1,
                "retrieved_object_ids": ["object-zeta"],
                "outcome_type": "completed",
                "telemetry_complete": True,
                "answer_correct": True,
            }],
        }), encoding="utf-8")
        with self.assertRaisesRegex(SimulatorRetrievalError, "ranking mismatch"):
            self._build(self.root / "bad-answer", path)

    def test_refuses_identifier_leakage_and_split_leakage(self) -> None:
        mutations = (
            ("identifier", "leaks a candidate object identifier"),
            ("split", "leaks across splits"),
        )
        for mutation, message in mutations:
            with self.subTest(mutation=mutation):
                payload = json.loads(json.dumps(self.config_payload))
                if mutation == "identifier":
                    payload["queries"][0]["query_text"] += " object-alpha"
                else:
                    payload["queries"][1]["source_object_group"] = "group-alpha"
                self.config_payload = payload
                self._write_config()
                with self.assertRaisesRegex(SimulatorRetrievalError, message):
                    self._build(self.root / f"bad-{mutation}")
                self.config_payload = json.loads(json.dumps({
                    **self.config_payload,
                    "queries": [
                        {
                            "query_id": "query-guitar",
                            "query_text": "Find musicians playing guitar near a woman",
                            "relevant_object_ids": ["object-alpha"],
                            "source_object_group": "group-alpha",
                            "split": "train",
                        },
                        {
                            "query_id": "query-baby",
                            "query_text": "Find a newborn being fed with a bottle",
                            "relevant_object_ids": ["object-gamma"],
                            "source_object_group": "group-gamma",
                            "split": "validation",
                        },
                        {
                            "query_id": "query-elephant",
                            "query_text": "Find an elephant kicking a soccer ball",
                            "relevant_object_ids": ["object-epsilon"],
                            "source_object_group": "group-epsilon",
                            "split": "test",
                        },
                    ],
                }))
                self._write_config()

    def test_refuses_digest_drift_and_existing_output(self) -> None:
        output = self.root / "existing"
        output.mkdir()
        marker = output / "keep.txt"
        marker.write_text("keep", encoding="utf-8")
        with self.assertRaisesRegex(SimulatorRetrievalError, "already exists"):
            self._build(output)
        self.assertEqual("keep", marker.read_text(encoding="utf-8"))
        digest = (
            self.representations
            / "object-alpha"
            / "multimodal_digest.txt"
        )
        digest.write_text("modified", encoding="utf-8")
        with self.assertRaisesRegex(
            SimulatorRetrievalError,
            "digest (size|checksum) mismatch",
        ):
            self._build(self.root / "drift")

    def test_ai_drafted_annotations_are_explicitly_posthoc(self) -> None:
        self.config_payload["annotation_status"] = (
            "ai-drafted-requires-operator-verification"
        )
        self._write_config()
        output = self.root / "draft"
        self._build(output)
        evaluation = json.loads(
            (output / "retrieval_evaluation.json").read_text(encoding="utf-8")
        )
        self.assertTrue(evaluation["posthoc"])
        self.assertFalse(evaluation["eligible_for_scientific_claims"])

    def test_cli_builds_and_verifies(self) -> None:
        output = self.root / "cli"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "build-simulator-retrieval-cohort",
                "--config",
                str(self.config),
                "--representation-manifest",
                str(self.manifest),
                "--output-dir",
                str(output),
                "--compact",
            ])
        self.assertEqual(0, status)
        self.assertEqual("COMPLETE", json.loads(stdout.getvalue())["status"])
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "verify-simulator-retrieval-cohort",
                "--output-dir",
                str(output),
                "--compact",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", json.loads(stdout.getvalue())["status"])


if __name__ == "__main__":
    unittest.main()
