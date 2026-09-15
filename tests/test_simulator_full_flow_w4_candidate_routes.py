from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from pathfinder.simulator.full_flow_w4_candidate_routes import (
    ARTIFACT_CATALOG_NAME,
    OPERATIONS_NAME,
    PLAN_NAME,
    FullFlowW4CandidateRouteError,
    freeze_full_flow_w4_candidate_routes,
    verify_full_flow_w4_candidate_routes,
)
from pathfinder.simulator.full_flow_w4_retrieval_contract import (
    W4_RETRIEVAL_TASK_SCHEMA_VERSION,
)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class FullFlowW4CandidateRouteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.runtime = self.root / "runtime"
        self.n3 = self.root / "n3"
        self.n4 = self.root / "n4"
        self.index = self.root / "index"
        self.ranges = self.root / "ranges"
        for path in (self.runtime, self.n3, self.n4, self.index, self.ranges):
            path.mkdir()
        self.candidates = ["candidate-a-b", "candidate-a|b"]
        self.digest_payload = {
            object_id: f"digest text for {object_id}\n".encode()
            for object_id in self.candidates
        }
        task_candidates = [
            {
                "object_id": object_id,
                "representation_id": "multimodal_digest",
                "artifact_sha256": _sha(self.digest_payload[object_id]),
                "artifact_size_bytes": len(self.digest_payload[object_id]),
            }
            for object_id in self.candidates
        ]
        task_core = {
            "schema_version": W4_RETRIEVAL_TASK_SCHEMA_VERSION,
            "contract_id": "w4-retrieval-contract-test-v1",
            "workload_id": "smoke-retrieval",
            "workload_class": "W4",
            "task_class_id": "video_retrieval",
            "retrieval_id": "visible-retrieval-test-v1",
            "query_id": "query-visible-w4",
            "query_text": "find the requested visible event",
            "candidate_corpus": "all-representation-manifest-objects",
            "candidate_objects": task_candidates,
            "candidate_set_sha256": _sha(_canonical(task_candidates)),
            "required_ranking_length": len(task_candidates),
            "quality_metrics": {
                "per_query": [
                    "reciprocal_rank",
                    "recall_at_1",
                    "recall_at_2",
                    "hit_at_1",
                    "hit_at_2",
                    "ndcg_at_1",
                    "ndcg_at_2",
                ],
                "aggregate": [
                    "mrr",
                    "mean_recall_at_1",
                    "mean_recall_at_2",
                    "mean_hit_at_1",
                    "mean_hit_at_2",
                    "mean_ndcg_at_1",
                    "mean_ndcg_at_2",
                ],
                "top_k": [1, 2],
                "relevance": "binary",
            },
            "relevance_values_included": False,
            "source_object_group_included": False,
            "credentials_recorded": False,
        }
        task = dict(task_core)
        task["task_binding_sha256"] = _sha(_canonical(task_core))
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
        trials = []
        for design_index in range(8):
            design = f"D{design_index}"
            route, executor = route_by_design[design]
            for repetition in (0, 1):
                trials.append({
                    "runtime_overlay_id": "w4-runtime-test-v1",
                    "trial_key": f"scenario|w4|{design}|r{repetition:04d}",
                    "order_index": design_index * 2 + repetition,
                    "design_id": design,
                    "repetition": repetition,
                    "route_family": route,
                    "executor_node_id": executor,
                })
        self.loaded = SimpleNamespace(
            plan={
                "runtime_overlay_id": "w4-runtime-test-v1",
                "plan_sha256": "3" * 64,
                "source_sha256": {"representation_manifest": "4" * 64},
            },
            public_task=task,
            trials=tuple(trials),
        )

        raw_rows = []
        derived_rows = []
        range_rows = []
        for position, object_id in enumerate(self.candidates):
            raw_payload = (object_id.encode() + b"-") * (20 + position)
            raw_digest = _sha(raw_payload)
            raw_size = len(raw_payload)
            raw_rows.append({
                "object_id": object_id,
                "representation_id": "raw_video",
                "artifact_sha256": raw_digest,
                "artifact_size_bytes": raw_size,
                "catalog_version": "candidate-catalog-v1",
                "plan_ids": ["D0", "D1", "D4", "D5"],
            })
            for representation, payload in (
                ("multimodal_digest", self.digest_payload[object_id]),
                ("sampled_frame_bundle", b"bundle-" + object_id.encode()),
            ):
                derived_rows.append({
                    "object_id": object_id,
                    "representation_id": representation,
                    "artifact_sha256": _sha(payload),
                    "artifact_size_bytes": len(payload),
                    "plan_ids": ["D2", "D3", "D6", "D7"],
                    "provenance": {
                        "schema_version": (
                            "pathfinder.simulator-derived-artifact-"
                            "provenance/v1alpha1"
                        ),
                        "producer_node_id": "N5",
                        "publication_source_id": f"publication-{object_id}",
                        "source_representation_id": "raw_video",
                        "source_content_sha256": raw_digest,
                        "derivation_id": f"derive-{representation}",
                        "derivation_sha256": _sha(
                            f"derive|{representation}".encode()
                        ),
                    },
                })
            range_rows.append({
                "object_id": object_id,
                "representation_id": "raw_video",
                "object_catalog_version": "candidate-catalog-v1",
                "full_artifact_size_bytes": raw_size,
                "full_artifact_sha256": raw_digest,
                "range_start": 0,
                "range_end": raw_size - 1,
                "range_size_bytes": raw_size,
                "range_sha256": raw_digest,
                "selection_semantics": "exact-full-object-fallback",
            })
        (self.n3 / "raw-cold-data-plane.json").write_text(
            json.dumps({
                "catalog_version": "candidate-catalog-v1",
                "objects": raw_rows,
            }),
            encoding="utf-8",
        )
        (self.n3 / "SHA256SUMS").write_text(
            "synthetic verifier-owned checksum commitment\n",
            encoding="utf-8",
        )
        (self.n4 / "n4-derived-data-package.json").write_text(
            json.dumps({
                "catalog_version": "candidate-catalog-v1",
                "objects": derived_rows,
            }),
            encoding="utf-8",
        )
        (self.index / "lexical-index.json").write_text(
            json.dumps({
                "candidate_object_ids": self.candidates,
                "source_manifest_sha256": "8" * 64,
            }),
            encoding="utf-8",
        )
        (self.ranges / "full-flow-exact-range-catalog.json").write_text(
            json.dumps({"entries": range_rows}), encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _patches(self):
        prefix = "pathfinder.simulator.full_flow_w4_candidate_routes."
        return (
            mock.patch(
                prefix + "load_full_flow_w4_retrieval_runtime_inputs",
                return_value=self.loaded,
            ),
            mock.patch(
                prefix + "verify_raw_cold_data_plane_package",
                return_value={"catalog_version": "candidate-catalog-v1"},
            ),
            mock.patch(
                prefix + "verify_n4_derived_data_package",
                return_value={
                    "package_sha256": "5" * 64,
                    "catalog_version": "candidate-catalog-v1",
                },
            ),
            mock.patch(
                prefix + "verify_n2_index_package",
                return_value={
                    "index_id": "w4-index-v1",
                    "index_sha256": "6" * 64,
                    "document_count": 2,
                },
            ),
            mock.patch(
                prefix + "verify_full_flow_exact_range_catalog",
                return_value={"catalog_sha256": "7" * 64},
            ),
        )

    def _freeze(self, name: str = "candidate-routes") -> Path:
        output = self.root / name
        patches = self._patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            freeze_full_flow_w4_candidate_routes(
                self.runtime,
                self.n3,
                self.n4,
                self.index,
                self.ranges,
                physical_plan_id="w4-candidate-physical-v1",
                output_dir=output,
            )
        return output

    def test_compiles_candidate_wide_routes_without_hidden_labels(self) -> None:
        output = self._freeze()
        verified = verify_full_flow_w4_candidate_routes(output)
        self.assertEqual(
            "VERIFIED_W4_CANDIDATE_ROUTE_BLUEPRINTS", verified["status"]
        )
        self.assertEqual(16, verified["trial_count"])
        self.assertEqual(2, verified["candidate_object_count"])
        self.assertTrue(
            verified["candidate_wide_physical_route_blueprints_compiled"]
        )
        self.assertFalse(verified["multi_candidate_route_coordinator_implemented"])
        plan = json.loads((output / PLAN_NAME).read_text())
        self.assertFalse(plan["claim_boundary"]["current_64_trial_matrix_modified"])
        self.assertFalse(
            plan["claim_boundary"]["physical_design_retrieval_comparison_ready"]
        )
        text = "\n".join(
            path.read_text(encoding="utf-8") for path in output.iterdir()
        )
        self.assertNotIn('"relevant_object_ids":', text)
        self.assertNotIn('"source_object_group":', text)

    def test_design_routes_use_expected_physical_contracts(self) -> None:
        output = self._freeze("route-shapes")
        operations = [
            json.loads(line)
            for line in (output / OPERATIONS_NAME).read_text().splitlines()
        ]
        by_design = {
            design: [row for row in operations if row["design_id"] == design]
            for design in ("D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7")
        }
        self.assertFalse(any(
            row["action"] == "query-candidate-index-shard"
            for row in by_design["D0"] + by_design["D4"]
        ))
        self.assertTrue(all(
            any(
                row["service_contract_id"] == "N2.global-index"
                for row in by_design[design]
            )
            for design in ("D1", "D2", "D5", "D6")
        ))
        self.assertTrue(any(
            row["service_contract_id"] == "N7.local-index"
            for row in by_design["D3"]
        ))
        self.assertTrue(any(
            row["service_contract_id"] == "N8.local-index"
            for row in by_design["D7"]
        ))
        self.assertTrue(all(
            any(row["exact_content_range"] is not None for row in by_design[design])
            for design in ("D1", "D5")
        ))
        self.assertTrue(all(
            any(
                row["service_contract_id"] == "N4.derived-data-agent"
                for row in by_design[design]
            )
            for design in ("D2", "D3", "D6", "D7")
        ))
        self.assertTrue(all(
            any(
                row["service_contract_id"]
                == f"transport.{executor}-N6-model-input"
                for row in by_design[design]
            )
            for design, executor in {
                "D0": "N7",
                "D1": "N7",
                "D2": "N7",
                "D3": "N7",
                "D4": "N8",
                "D5": "N8",
                "D6": "N8",
                "D7": "N8",
            }.items()
        ))
        self.assertTrue(all(
            any(
                row["action"] == "transfer-ranking-fallback"
                and row["logical_node_ids"][-1] == "N6"
                for row in by_design[design]
            )
            for design in ("D1", "D2", "D3", "D5", "D6", "D7")
        ))
        self.assertTrue(all(
            any(
                row["action"] == "prepare-retrieval-candidate"
                and row["representation_identity"]["representation_id"]
                == "sampled_frame_bundle"
                for row in by_design[design]
            )
            for design in ("D2", "D6")
        ))
        trials = [
            json.loads(line)
            for line in (output / "w4-candidate-route-trials.jsonl")
            .read_text()
            .splitlines()
        ]
        rules = {row["design_id"]: row["complete_ranking_rule"] for row in trials}
        self.assertEqual(
            "semantic-selected-frame-then-append-coarse-digest-tail",
            rules["D2"],
        )
        self.assertEqual(rules["D2"], rules["D6"])
        self.assertEqual(rules["D2"], rules["D3"])
        self.assertEqual(rules["D2"], rules["D7"])

    def test_missing_candidate_representation_fails_before_publication(self) -> None:
        manifest = json.loads(
            (self.n4 / "n4-derived-data-package.json").read_text()
        )
        manifest["objects"].pop()
        (self.n4 / "n4-derived-data-package.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
        output = self.root / "missing-derived"
        patches = self._patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            with self.assertRaisesRegex(
                FullFlowW4CandidateRouteError,
                "N4 candidate representation coverage",
            ):
                freeze_full_flow_w4_candidate_routes(
                    self.runtime,
                    self.n3,
                    self.n4,
                    self.index,
                    self.ranges,
                    physical_plan_id="w4-candidate-physical-v1",
                    output_dir=output,
                )
        self.assertFalse(output.exists())

    def test_public_digest_identity_must_match_n4(self) -> None:
        manifest = json.loads(
            (self.n4 / "n4-derived-data-package.json").read_text()
        )
        manifest["objects"][0]["artifact_sha256"] = "f" * 64
        (self.n4 / "n4-derived-data-package.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
        output = self.root / "digest-drift"
        patches = self._patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            with self.assertRaisesRegex(
                FullFlowW4CandidateRouteError,
                "public W4 digest identity differs",
            ):
                freeze_full_flow_w4_candidate_routes(
                    self.runtime,
                    self.n3,
                    self.n4,
                    self.index,
                    self.ranges,
                    physical_plan_id="w4-candidate-physical-v1",
                    output_dir=output,
                )
        self.assertFalse(output.exists())

    def test_tampering_is_detected(self) -> None:
        output = self._freeze("tamper")
        catalog = output / ARTIFACT_CATALOG_NAME
        catalog.write_text(catalog.read_text() + " ", encoding="utf-8")
        with self.assertRaisesRegex(
            FullFlowW4CandidateRouteError, "checksums failed"
        ):
            verify_full_flow_w4_candidate_routes(output)

    def test_output_cannot_overlap_verified_source(self) -> None:
        output = self.n3 / "invalid-child"
        with self.assertRaisesRegex(
            FullFlowW4CandidateRouteError,
            "overlaps an input",
        ):
            freeze_full_flow_w4_candidate_routes(
                self.runtime,
                self.n3,
                self.n4,
                self.index,
                self.ranges,
                physical_plan_id="w4-candidate-physical-v1",
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_object_ids_that_normalize_alike_have_distinct_operations(self) -> None:
        output = self._freeze("collision-safe")
        operations = [
            json.loads(line)
            for line in (output / OPERATIONS_NAME).read_text().splitlines()
        ]
        keys = [row["operation_key"] for row in operations]
        self.assertEqual(len(keys), len(set(keys)))


if __name__ == "__main__":
    unittest.main()
