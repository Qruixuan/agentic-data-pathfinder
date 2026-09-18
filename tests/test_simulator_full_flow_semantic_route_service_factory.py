from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from contextlib import ExitStack
from dataclasses import fields, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pathfinder.simulator.full_flow_index_query_plan_catalog import (
    build_full_flow_index_query_plan_catalog,
)
from pathfinder.simulator.full_flow_local_semantic_admission import (
    FrozenLocalSemanticExecutionInputs,
)
from pathfinder.simulator.full_flow_semantic_route_service_factory import (
    CHECKSUMS_NAME,
    INDEX_QUERY_PLAN_CATALOG_NAME,
    INDEX_QUERY_PLAN_CATALOG_SCHEMA_VERSION,
    FrozenSemanticRouteServiceSources,
    FullFlowSemanticRouteServiceFactoryError,
    RuntimeSemanticServiceInputs,
    SQLiteRouteExecutionStore,
    _data_agent_plan_catalog,
    assemble_full_flow_semantic_route_service,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    ArtifactIdentity,
)
from pathfinder.simulator.n3_indexed_data_plane import (
    INDEXED_REPRESENTATION_ID,
)


MODULE = "pathfinder.simulator.full_flow_semantic_route_service_factory"
HEX_A = "a" * 64
HEX_B = "b" * 64
HEX_C = "c" * 64


def canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class FakeExactRanges:
    catalog_sha256 = HEX_B

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass


class FakeProvisioningCatalog:
    catalog_sha256 = HEX_C
    references: tuple[object, ...] = ()

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass


class UnsafeLocalVerifier:
    def verify(self, **_kwargs: object) -> dict[str, object]:
        return {"status": "VERIFIED"}


class SemanticRouteServiceFactoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        names = (
            "admission",
            "bindings",
            "n1-commitment",
            "index",
            "n3",
            "n4",
            "ranges",
            "provisioning",
            "query-plans",
        )
        self.paths = {name: self.root / name for name in names}
        for path in self.paths.values():
            path.mkdir()
        self.trial = {
            "schema_version": (
                "pathfinder.full-flow-semantic-bound-trial/v1alpha1"
            ),
            "trial_key": "matrix|W1|D0|r0000",
            "design_id": "D0",
            "executor_node_id": "N7",
            "flowmesh_submission_authorized": True,
            "required_runtime_adapter_ids": [],
            "artifact_object_id": "object-1",
            "public_task_binding": {
                "object_id": "object-1",
                "question": "Which visible object is relevant?",
                "task_binding_sha256": HEX_A,
            },
            "representation_identities": [{
                "artifact_object_id": "object-1",
                "representation_id": "raw_video",
            }],
            "semantic_stage_keys": ["stage-index"],
        }
        self.stage = {
            "schema_version": (
                "pathfinder.full-flow-semantic-bound-stage/v1alpha1"
            ),
            "stage_key": "stage-index",
            "trial_key": self.trial["trial_key"],
            "action": "query-index",
            "logical_node_ids": ["N2"],
        }
        admission = {
            "promotion_id": "promotion-1",
            "admission_sha256": HEX_A,
            "semantics_mode": "legacy-mcq-local-conformance",
            "trial_template_flowmesh_submission_authorized": True,
            "public_oracle_binding": {
                "oracle_id": "oracle-1",
                "public_task_set_sha256": HEX_C,
                "hidden_label_content_included": False,
                "n1_private_package_required_by_n7_n8_runtime": False,
            },
            "source_commitments": {
                "exact_range_catalog_sha256": HEX_B,
                "preprovisioned_catalog_sha256": HEX_C,
            },
        }
        self.admission = admission
        self._write_catalog_rows(admission, [self.trial], [self.stage])
        (self.paths["n1-commitment"] / (
            "n1-oracle-preselection-commitment.json"
        )).write_text(json.dumps({
            "oracle_id": "oracle-1",
            "public_task_set_sha256": HEX_C,
            "label_values_included": False,
        }), encoding="utf-8")
        (self.paths["n3"] / "raw-cold-data-plane.json").write_text(
            json.dumps({"objects": [{
                "object_id": "object-1",
                "representation_id": "raw_video",
                "plan_ids": ["D0"],
            }]}),
            encoding="utf-8",
        )
        (self.paths["n4"] / "n4-derived-data-package.json").write_text(
            json.dumps({"objects": [{
                "object_id": "object-1",
                "representation_id": "multimodal_digest",
                "plan_ids": ["D2"],
            }]}),
            encoding="utf-8",
        )
        (self.paths["index"] / "lexical-index.json").write_text(
            json.dumps({
                "index_id": "index-1",
                "candidate_object_ids": ["object-1"],
            }, sort_keys=True),
            encoding="utf-8",
        )
        self._write_query_plan_catalog()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_catalog_rows(
        self,
        admission: dict[str, object],
        trials: list[dict[str, object]],
        stages: list[dict[str, object]],
    ) -> None:
        (self.paths["admission"] / "semantic-execution-admission.json").write_text(
            json.dumps(admission),
            encoding="utf-8",
        )
        (self.paths["admission"] / "semantic-execution-trials.jsonl").write_text(
            "".join(json.dumps(value) + "\n" for value in trials),
            encoding="utf-8",
        )
        (self.paths["admission"] / "semantic-execution-stages.jsonl").write_text(
            "".join(json.dumps(value) + "\n" for value in stages),
            encoding="utf-8",
        )

    def _write_query_plan_catalog(self) -> None:
        value: dict[str, object] = {
            "schema_version": INDEX_QUERY_PLAN_CATALOG_SCHEMA_VERSION,
            "status": "FROZEN_INDEX_QUERY_PLANS",
            "catalog_id": "local-visible-index-query-plans-v1",
            "semantics_mode": "legacy-mcq-local-conformance",
            "admission_sha256": HEX_A,
            "index_id": "index-1",
            "index_sha256": HEX_B,
            "public_task_set_sha256": HEX_C,
            "query_policy": "single-public-target-local-conformance",
            "entries": [{
                "trial_key": self.trial["trial_key"],
                "task_binding_sha256": HEX_A,
                "index_id": "index-1",
                "query_id": "query-1",
                "query_text": "Which visible object is relevant?",
                "top_k": 1,
                "candidate_object_ids": ["object-1"],
            }],
            "w4_retrieval_quality_evaluated": False,
            "performance_measured": False,
            "cost_measured": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        value["catalog_sha256"] = hashlib.sha256(canonical(value)).hexdigest()
        payload = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
        (self.paths["query-plans"] / INDEX_QUERY_PLAN_CATALOG_NAME).write_bytes(
            payload
        )
        (self.paths["query-plans"] / CHECKSUMS_NAME).write_text(
            hashlib.sha256(payload).hexdigest()
            + f"  {INDEX_QUERY_PLAN_CATALOG_NAME}\n",
            encoding="utf-8",
        )

    def _sources(
        self,
        *,
        query_plans: bool = True,
        query_plan_dir: Path | None = None,
    ) -> FrozenSemanticRouteServiceSources:
        return FrozenSemanticRouteServiceSources(
            local_admission_dir=self.paths["admission"],
            n1_public_commitment_dir=self.paths["n1-commitment"],
            artifact_binding_dir=self.paths["bindings"],
            n2_index_package_dir=self.paths["index"],
            n3_package_dir=self.paths["n3"],
            n4_package_dir=self.paths["n4"],
            exact_range_catalog_dir=self.paths["ranges"],
            provisioning_catalog_dir=self.paths["provisioning"],
            index_query_plan_catalog_dir=(
                query_plan_dir
                if query_plan_dir is not None
                else (self.paths["query-plans"] if query_plans else None)
            ),
        )

    @staticmethod
    def _runtime() -> RuntimeSemanticServiceInputs:
        token = "runtime-secret-123456789"
        return RuntimeSemanticServiceInputs(
            logical_node_id="N7",
            index_base_urls={
                "N2": "http://127.0.0.1:19082",
                "N7": "http://127.0.0.1:19087",
                "N8": "http://127.0.0.1:19088",
            },
            index_bearer_tokens={"N2": token, "N7": token, "N8": token},
            data_agent_base_urls={
                "N3": "http://127.0.0.1:19083",
                "N4": "http://127.0.0.1:19084",
            },
            data_agent_bearer_tokens={"N3": token, "N4": token},
            cache_base_urls={
                "N7": "http://127.0.0.1:19787",
                "N8": "http://127.0.0.1:19788",
            },
            cache_bearer_tokens={"N7": token, "N8": token},
            cache_ids={"N7": "cache-n7", "N8": "cache-n8"},
            node_health_base_urls={
                "N7": "http://127.0.0.1:19087",
                "N8": "http://127.0.0.1:19088",
            },
            n6_base_url="http://127.0.0.1:19086",
            n6_bearer_token=token,
            n1_base_url="http://127.0.0.1:19081",
            n1_bearer_token=token,
            n1_verification_base_url="http://127.0.0.1:19181",
            n1_verification_bearer_token=token,
            semantic_model="qwen3.8-27b",
        )

    def _patch_sources(
        self,
        *,
        patch_index_verifier: bool = True,
        index_sha256: str = HEX_B,
    ) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(patch(
            MODULE + ".verify_full_flow_local_semantic_runtime_package",
            return_value={
                "status": "VERIFIED_PUBLIC_LOCAL_RUNTIME_INPUTS",
                "promotion_id": "promotion-1",
                "admission_sha256": HEX_A,
                "oracle_id": "oracle-1",
                "public_task_set_sha256": HEX_C,
            },
        ))
        stack.enter_context(patch(
            MODULE + ".load_full_flow_local_semantic_execution_inputs",
            return_value=SimpleNamespace(
                admission=self.admission,
                bound_trials=(self.trial,),
                bound_stages=(self.stage,),
            ),
        ))
        stack.enter_context(patch(
            MODULE + ".verify_n1_oracle_preselection_commitment",
            return_value={
                "status": "VERIFIED",
                "commitment_sha256": HEX_B,
                "private_package_binding_verified": False,
            },
        ))
        stack.enter_context(patch(
            MODULE + ".verify_n2_index_package",
            return_value={
                "status": "VERIFIED",
                "index_id": "index-1",
                "index_sha256": index_sha256,
            },
        ))
        if patch_index_verifier:
            stack.enter_context(patch(
                MODULE + ".verify_full_flow_index_query_plan_catalog",
                return_value={
                    "status": "VERIFIED",
                    "catalog_id": "local-visible-index-query-plans-v1",
                    "catalog_sha256": json.loads((
                        self.paths["query-plans"]
                        / INDEX_QUERY_PLAN_CATALOG_NAME
                    ).read_text(encoding="utf-8"))["catalog_sha256"],
                    "indexed_trial_count": 1,
                    "query_policy": "single-public-target-local-conformance",
                    "w4_retrieval_quality_evaluated": False,
                    "source_binding_checked": True,
                    "credentials_recorded": False,
                },
            ))
        stack.enter_context(patch(
            MODULE + ".ExactFullObjectRangeCatalog",
            FakeExactRanges,
        ))
        stack.enter_context(patch(
            MODULE + ".FrozenProvisioningCatalog",
            FakeProvisioningCatalog,
        ))
        return stack

    def test_transfer_shaping_requires_one_complete_positive_profile(self) -> None:
        runtime = self._runtime()
        with self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "fully specified",
        ):
            replace(
                runtime,
                application_transfer_profile_id="core-v1",
            )
        with self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "bandwidth must be positive",
        ):
            replace(
                runtime,
                application_transfer_profile_id="core-v1",
                application_transfer_bandwidth_bytes_per_second=0.0,
                application_transfer_round_trip_time_ms=1.0,
            )
        shaped = replace(
            runtime,
            application_transfer_profile_id="core-v1",
            application_transfer_bandwidth_bytes_per_second=250_000_000.0,
            application_transfer_round_trip_time_ms=1.0,
        )
        self.assertEqual("core-v1", shaped.application_transfer_profile_id)

    def test_assembles_redacted_http_graph_without_network_calls(self) -> None:
        runtime = self._runtime()
        state = self.root / "state"
        with self._patch_sources():
            assembly = assemble_full_flow_semantic_route_service(
                self._sources(),
                runtime,
                state_dir=state,
            )

        self.assertTrue(assembly.ready)
        assembly.require_ready()
        descriptor = assembly.health_descriptor
        self.assertEqual(descriptor["status"], "READY_NOT_PROBED")
        self.assertEqual(descriptor["runtime_gap_count"], 0)
        self.assertEqual(len(descriptor["constructor_graph"]), 9)
        self.assertTrue((state / "route-executions.sqlite3").is_file())
        self.assertTrue((state / "cache-lineage.sqlite3").is_file())
        serialized = json.dumps(descriptor, sort_keys=True)
        self.assertNotIn("http://", serialized)
        self.assertNotIn("runtime-secret", serialized)
        self.assertNotIn(str(state), serialized)
        self.assertNotIn("http://", repr(runtime))
        self.assertNotIn("runtime-secret", repr(runtime))

    def test_indexed_trial_binds_private_n3_projection_plan(self) -> None:
        path = self.paths["n3"] / "raw-cold-data-plane.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document["objects"].append({
            "object_id": "object-1",
            "representation_id": INDEXED_REPRESENTATION_ID,
            "plan_ids": ["D0"],
        })
        path.write_text(json.dumps(document), encoding="utf-8")
        trial = {**self.trial, "route_family": "indexed-raw"}
        catalog = _data_agent_plan_catalog(self._sources(), [trial])
        selected = ArtifactIdentity(
            object_id="object-1",
            representation_id=INDEXED_REPRESENTATION_ID,
            artifact_sha256=HEX_A,
            artifact_size_bytes=123,
            object_catalog_version="catalog-v1",
        )
        self.assertEqual(
            "D0",
            catalog.resolve(
                source_node_id="N3",
                trial=trial,
                identity=selected,
            ),
        )

    def test_source_contract_cannot_accept_private_n1_package(self) -> None:
        names = {value.name for value in fields(FrozenSemanticRouteServiceSources)}
        self.assertIn("n1_public_commitment_dir", names)
        self.assertNotIn("n1_oracle_package_dir", names)
        self.assertNotIn("task_plane_dir", names)
        self.assertNotIn("public_task_set_path", names)

    def test_generated_compose_service_hosts_are_valid_private_http_hosts(
        self,
    ) -> None:
        runtime = replace(
            self._runtime(),
            n1_base_url=(
                "http://pathfinder-full-flow-n1-hidden-score:9081"
            ),
            simulator_private_http_hosts=(
                "pathfinder-full-flow-n1-hidden-score",
                "pathfinder-sim-n2-index",
            ),
        )
        self.assertEqual(
            (
                "pathfinder-full-flow-n1-hidden-score",
                "pathfinder-sim-n2-index",
            ),
            runtime.simulator_private_http_hosts,
        )
        with self._patch_sources():
            assembly = assemble_full_flow_semantic_route_service(
                self._sources(),
                runtime,
                state_dir=self.root / "compose-host-state",
            )
        self.assertTrue(assembly.ready)

    def test_local_or_generic_n1_verifier_is_rejected(self) -> None:
        with self._patch_sources(), self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "accepts only the remote N1",
        ):
            unsafe = UnsafeLocalVerifier()
            assemble_full_flow_semantic_route_service(
                self._sources(),
                self._runtime(),
                state_dir=self.root / "unsafe-verifier-state",
                n1_score_evidence_verifier=unsafe,  # type: ignore[arg-type]
            )

    def test_public_n1_identity_mismatch_fails_before_client_assembly(self) -> None:
        path = (
            self.paths["n1-commitment"]
            / "n1-oracle-preselection-commitment.json"
        )
        value = json.loads(path.read_text(encoding="utf-8"))
        value["public_task_set_sha256"] = HEX_B
        path.write_text(json.dumps(value), encoding="utf-8")
        with self._patch_sources(), self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "public N1 identity differs",
        ):
            assemble_full_flow_semantic_route_service(
                self._sources(),
                self._runtime(),
                state_dir=self.root / "mismatched-n1-state",
            )

    def test_missing_query_catalog_has_machine_readable_gap(self) -> None:
        with self._patch_sources():
            assembly = assemble_full_flow_semantic_route_service(
                self._sources(query_plans=False),
                self._runtime(),
                state_dir=self.root / "blocked-state",
            )

        self.assertFalse(assembly.ready)
        gaps = {
            value["gap_id"] for value in assembly.health_descriptor["runtime_gaps"]
        }
        self.assertEqual(gaps, {"frozen-index-query-plan-catalog-missing"})
        with self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "unresolved runtime gaps",
        ):
            assembly.require_ready()

    def test_query_plan_catalog_is_bound_to_admission(self) -> None:
        path = self.paths["query-plans"] / INDEX_QUERY_PLAN_CATALOG_NAME
        value = json.loads(path.read_text(encoding="utf-8"))
        value["admission_sha256"] = HEX_C
        value.pop("catalog_sha256")
        value["catalog_sha256"] = hashlib.sha256(canonical(value)).hexdigest()
        payload = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
        path.write_bytes(payload)
        (self.paths["query-plans"] / CHECKSUMS_NAME).write_text(
            hashlib.sha256(payload).hexdigest()
            + f"  {INDEX_QUERY_PLAN_CATALOG_NAME}\n",
            encoding="utf-8",
        )
        with self._patch_sources():
            with patch(
                MODULE + ".verify_full_flow_index_query_plan_catalog",
                side_effect=ValueError("public source binding changed"),
            ), self.assertRaisesRegex(
                FullFlowSemanticRouteServiceFactoryError,
                "canonical index-query-plan verification failed",
            ):
                assemble_full_flow_semantic_route_service(
                    self._sources(),
                    self._runtime(),
                    state_dir=self.root / "bad-catalog-state",
                )

    def test_canonical_catalog_producer_output_is_consumed_directly(self) -> None:
        inputs = FrozenLocalSemanticExecutionInputs(
            admission=self.admission,
            bound_trials=(self.trial,),
            bound_stages=(self.stage,),
            representative_smokes=(),
            adapter_inventory={},
        )
        index_sha256 = hashlib.sha256(
            (self.paths["index"] / "lexical-index.json").read_bytes()
        ).hexdigest()
        index_report = {
            "status": "VERIFIED",
            "index_id": "index-1",
            "index_sha256": index_sha256,
        }
        produced = self.root / "canonical-query-plans"
        catalog_module = (
            "pathfinder.simulator.full_flow_index_query_plan_catalog"
        )
        with patch(
            catalog_module + ".load_full_flow_local_semantic_execution_inputs",
            return_value=inputs,
        ), patch(
            catalog_module + ".verify_n2_index_package",
            return_value=index_report,
        ):
            build_full_flow_index_query_plan_catalog(
                self.paths["admission"],
                self.paths["index"],
                output_dir=produced,
            )
            with self._patch_sources(
                patch_index_verifier=False,
                index_sha256=index_sha256,
            ):
                assembly = assemble_full_flow_semantic_route_service(
                    self._sources(query_plan_dir=produced),
                    self._runtime(),
                    state_dir=self.root / "producer-consumer-state",
                )
        self.assertTrue(assembly.ready)
        self.assertEqual(0, assembly.health_descriptor["runtime_gap_count"])

    def test_sqlite_route_store_replays_exact_complete_evidence(self) -> None:
        store = SQLiteRouteExecutionStore(
            self.root / "store.sqlite3",
            logical_node_id="N8",
        )
        self.assertIsNone(store.begin(HEX_A, HEX_B))
        evidence = {"status": "COMPLETE", "value": 1}
        store.complete(HEX_A, HEX_B, evidence)
        self.assertEqual(store.begin(HEX_A, HEX_B), evidence)
        with self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "different frozen input",
        ):
            store.begin(HEX_A, HEX_C)

    def test_sqlite_route_store_never_replays_failed_execution(self) -> None:
        store = SQLiteRouteExecutionStore(
            self.root / "failed.sqlite3",
            logical_node_id="N7",
        )
        self.assertIsNone(store.begin(HEX_A, HEX_B))
        store.fail(HEX_A, HEX_B, "credential-bearing detail stays hashed")
        with self.assertRaisesRegex(
            FullFlowSemanticRouteServiceFactoryError,
            "cannot be replayed ambiguously",
        ):
            store.begin(HEX_A, HEX_B)
        raw = (self.root / "failed.sqlite3").read_bytes()
        self.assertNotIn(b"credential-bearing", raw)


if __name__ == "__main__":
    unittest.main()
