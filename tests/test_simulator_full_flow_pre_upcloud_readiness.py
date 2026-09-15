from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import ExitStack, contextmanager, redirect_stdout
from pathlib import Path
from typing import Any, Iterator, Mapping
from unittest.mock import patch

from pathfinder.cli import _parser, main as cli_main
import pathfinder.simulator as simulator
from pathfinder.simulator import full_flow_pre_upcloud_readiness as readiness


class _Fixture:
    required_names = (
        "provisioning_catalog_dir",
        "artifact_binding_dir",
        "n4_package_dir",
        "logical_route_dir",
        "container_plan_dir",
        "task_plane_dir",
        "n3_package_dir",
        "w4_route_package_dir",
        "w4_index_package_dir",
        "w4_index_crosswalk_dir",
    )

    def __init__(self, root: Path) -> None:
        self.root = root
        self.paths: dict[str, Path] = {}
        for name in self.required_names:
            path = root / "sources" / name
            path.mkdir(parents=True)
            (path / "artifact.json").write_text(
                json.dumps({"source": name}, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self.paths[name] = path
        scenario = root / "sources" / "scenario.json"
        scenario.write_text('{"scenario":"v1"}\n', encoding="utf-8")
        self.paths["scenario_path"] = scenario
        self.bulk = root / "sources" / "bulk"
        (self.bulk / readiness.BULK_FINAL_DIRECTORY_NAME).mkdir(
            parents=True
        )
        self.bulk_bindings = (
            self.bulk
            / readiness.BULK_FINAL_DIRECTORY_NAME
            / readiness.LIVE_RECEIPT_BINDINGS_NAME
        )
        self.bulk_bindings.write_text("[]\n", encoding="utf-8")
        (self.bulk / "journal.jsonl").write_text("{}\n", encoding="utf-8")
        self.bulk_sources = root / "sources" / "bulk-sources.json"
        self.bulk_sources.write_text('{"sources":36}\n', encoding="utf-8")
        self.semantic_outer = self._directory("semantic-outer")
        self.semantic_inner = self.semantic_outer / "matrix-run"
        self.semantic_inner.mkdir()
        (self.semantic_inner / "artifact.json").write_text(
            "{}\n", encoding="utf-8"
        )
        self.neutral_observations = self._directory("neutral-observations")
        self.semantic_admission = self._directory("semantic-admission")
        self.ten_smoke = self._directory("ten-smoke")
        self.n4_serve_gate = self._directory("n4-serve-gate")
        self.compose_overlay = self._directory("compose-overlay")
        self.service_bootstrap = self._directory("service-bootstrap")
        self.deployment_binding = self._directory("deployment-binding")
        self.semantic_matrix = self._directory("semantic-matrix")
        self.public_tasks = root / "sources" / "public-tasks.json"
        self.public_tasks.write_text("{}\n", encoding="utf-8")
        self.semantic_artifact_binding = (
            self.paths["artifact_binding_dir"] / "semantic-artifacts.json"
        )
        self.semantic_artifact_binding.write_text("{}\n", encoding="utf-8")
        self.w4_retrieval_contract = self._directory(
            "w4-retrieval-contract"
        )
        self.w4_retrieval_evaluation = self._directory(
            "w4-retrieval-evaluation"
        )
        self.flowmesh_matrix = self._directory("flowmesh-matrix")
        self.flowmesh_profile = self._directory("flowmesh-profile")
        self.flowmesh_coordinator = self._directory("flowmesh-coordinator")
        self.flowmesh_run = self._directory("flowmesh-run")
        self.flowmesh_w4_plan = self._directory("flowmesh-w4-plan")
        self.flowmesh_w4_run = self._directory("flowmesh-w4-run")
        self.w4_component = (
            self.flowmesh_w4_run / readiness.COMPONENT_RECEIPT_DIR_NAME
        )
        self.w4_component.mkdir()
        (self.w4_component / "receipt.json").write_text(
            "{}\n", encoding="utf-8"
        )
        self.w4_coordinator = (
            self.flowmesh_w4_run / readiness.CANDIDATE_RUN_DIR_NAME
        )
        self.w4_coordinator.mkdir()
        (self.w4_coordinator / "run.json").write_text(
            "{}\n", encoding="utf-8"
        )
        (
            self.w4_coordinator
            / readiness.W4_RETRIEVAL_OBSERVATIONS_NAME
        ).write_text('{"observations":[]}\n', encoding="utf-8")
        self.n1_oracle = self._directory("n1-oracle")

    def _directory(self, name: str) -> Path:
        path = self.root / "sources" / name
        path.mkdir()
        (path / "artifact.json").write_text("{}\n", encoding="utf-8")
        return path

    def required(self, output: str = "audit") -> dict[str, Any]:
        return {
            "audit_id": "pre-upcloud-audit-v1",
            "source_git_revision": "a" * 40,
            "operator_attests_clean_committed_source": True,
            **self.paths,
            "output_dir": self.root / output,
        }

    def full(self, output: str = "full-audit") -> dict[str, Any]:
        return {
            **self.required(output),
            "neutral_observation_dir": self.neutral_observations,
            "neutral_semantic_matrix_run_dir": self.semantic_inner,
            "semantic_execution_admission_dir": self.semantic_admission,
            "smoke_gated_semantic_matrix_run_dir": self.semantic_outer,
            "ten_smoke_dir": self.ten_smoke,
            "n4_serve_gate_dir": self.n4_serve_gate,
            "compose_overlay_dir": self.compose_overlay,
            "service_bootstrap_dir": self.service_bootstrap,
            "deployment_binding_dir": self.deployment_binding,
            "semantic_matrix_dir": self.semantic_matrix,
            "public_task_set_path": self.public_tasks,
            "semantic_artifact_binding_path": (
                self.semantic_artifact_binding
            ),
            "bulk_provisioning_output_dir": self.bulk,
            "bulk_source_manifest": self.bulk_sources,
            "bulk_live_receipt_bindings": self.bulk_bindings,
            "w4_component_receipt_dir": self.w4_component,
            "w4_coordinator_run_dir": self.w4_coordinator,
            "w4_retrieval_contract_dir": self.w4_retrieval_contract,
            "w4_retrieval_evaluation_dir": self.w4_retrieval_evaluation,
            "flowmesh_matrix_plan_dir": self.flowmesh_matrix,
            "flowmesh_formal_profile_dir": self.flowmesh_profile,
            "flowmesh_coordinator_plan_dir": self.flowmesh_coordinator,
            "flowmesh_matrix_run_dir": self.flowmesh_run,
            "flowmesh_w4_plan_dir": self.flowmesh_w4_plan,
            "flowmesh_w4_run_dir": self.flowmesh_w4_run,
            "n1_oracle_package_dir": self.n1_oracle,
            "n1_evidence_secret": b"runtime-token-never-persisted",
        }


class FullFlowPreUpcloudReadinessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.fixture = _Fixture(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @contextmanager
    def verified_sources(
        self,
        *,
        component_evidence: str = "live-local-component-execution",
        catalog_entries: int = 72,
        retrieval_source_bound: bool = True,
        flowmesh_completed_trials: int = 64,
        flowmesh_w4_completed_tasks: int = 16,
        flowmesh_w4_workflows: int = 1,
        flowmesh_w4_source_bound: bool = True,
        flowmesh_w4_component_evidence: str = (
            "live-local-component-execution"
        ),
        neutral_authentic: bool = True,
        neutral_generic_count: int = 64,
        neutral_run_integrity: bool = True,
        outer_completed_trials: int = 64,
    ) -> Iterator[dict[str, Any]]:
        digest = "a" * 64
        w4_digest = "c" * 64
        with ExitStack() as stack:
            results = {
                "verify_n4_derived_data_package": {
                    "status": "VERIFIED",
                    "object_count": 36,
                    "artifact_count": 72,
                },
                "verify_full_flow_artifact_bindings": {
                    "status": "VERIFIED",
                    "artifact_object_count": 36,
                    "artifact_representation_count": 72,
                },
                "verify_full_flow_provisioning_catalog": {
                    "status": "VERIFIED",
                    "entry_count": catalog_entries,
                },
                "verify_full_flow_w4_candidate_routes": {
                    "status": "VERIFIED_W4_CANDIDATE_ROUTE_BLUEPRINTS",
                    "trial_count": 16,
                },
                "verify_full_flow_w4_index_artifact_crosswalk": {
                    "status": "VERIFIED"
                },
                "verify_full_flow_observations": {
                    "status": "VERIFIED",
                    "observation_count": 64,
                    "legacy_full_flow_observation_count": 0,
                    "generic_semantic_route_observation_count": (
                        neutral_generic_count
                    ),
                    "all_score_authenticity_verified": neutral_authentic,
                    "semantic_matrix_run_integrity_verified": (
                        neutral_run_integrity
                    ),
                },
                "verify_smoke_gated_full_flow_local_semantic_matrix_run": {
                    "status": (
                        "VERIFIED_SMOKE_GATED_LOCAL_SEMANTIC_MATRIX"
                    ),
                    "planned_trial_count": 64,
                    "completed_trial_count": outer_completed_trials,
                    "neutral_evidence_count": 64,
                    "full_matrix_runtime_gate_satisfied": True,
                    "source_binding_checked": True,
                },
                "verify_full_flow_bulk_live_provisioning": {
                    "status": "VERIFIED",
                    "object_count": 36,
                    "completed_operation_count": 72,
                    "required_derived_identity_count": 72,
                },
                "verify_full_flow_w4_component_execution_receipt": {
                    "status": "VERIFIED",
                    "evidence_class": component_evidence,
                },
                "verify_full_flow_w4_retrieval_contract": {
                    "status": "VERIFIED",
                    "contract_id": "w4-contract-v1",
                    "w4_trial_count": 16,
                    "candidate_object_count": 8,
                    "hidden_relevance_values_returned": False,
                },
                "verify_full_flow_w4_retrieval_evaluation": {
                    "status": (
                        "VERIFIED_SOURCE_BOUND"
                        if retrieval_source_bound
                        else "VERIFIED_INTEGRITY"
                    ),
                    "contract_id": "w4-contract-v1",
                    "trial_count": 16,
                    "candidate_object_count": 8,
                    "source_bound_replay_performed": retrieval_source_bound,
                    "hidden_relevance_values_returned": False,
                },
                "verify_flowmesh_container_matrix_plan": {
                    "status": "VERIFIED",
                    "matrix_id": "matrix-v1",
                    "matrix_dimensions": {
                        "trial_count": 64,
                        "workload_count": 4,
                        "design_count": 8,
                        "repetitions": [0, 1],
                    },
                    "operation_count": 500,
                    "plan_sha256": digest,
                },
                "verify_flowmesh_container_formal_execution_profile": {
                    "status": "VERIFIED",
                    "matrix_plan_sha256": digest,
                    "profile_sha256": "b" * 64,
                    "primary_trial_wrapper_max_concurrency": 1,
                    "eligible_for_formal_infrastructure_execution": True,
                },
                "verify_flowmesh_container_matrix_coordinator_dry_run": {
                    "status": "VERIFIED",
                    "matrix_plan_sha256": digest,
                    "profile_sha256": "b" * 64,
                    "trial_wrapper_count": 64,
                    "conditional_trial_wrapper_count": 16,
                    "source_binding_checked": True,
                },
                "verify_flowmesh_container_matrix_run": {
                    "status": "VERIFIED",
                    "matrix_id": "matrix-v1",
                    "completed_trial_count": flowmesh_completed_trials,
                    "executed_operation_count": 472,
                    "inactive_operation_count": 28,
                    "workflow_count": 80,
                    "flowmesh_workflow_count": 80,
                    "source_binding_checked": True,
                },
                "verify_flowmesh_w4_candidate_matrix_plan": {
                    "status": "VERIFIED",
                    "run_id": "w4-flowmesh-run-v1",
                    "physical_plan_id": "physical-plan-v1",
                    "trial_count": 16,
                    "flowmesh_api_task_count": 16,
                    "workflow_count": 1,
                    "plan_sha256": w4_digest,
                    "workflow_submitted": False,
                },
                "verify_flowmesh_w4_candidate_matrix_run": {
                    "status": "VERIFIED",
                    "run_id": "w4-flowmesh-run-v1",
                    "physical_plan_id": "physical-plan-v1",
                    "completed_trial_count": flowmesh_w4_completed_tasks,
                    "flowmesh_api_task_count": flowmesh_w4_completed_tasks,
                    "workflow_count": flowmesh_w4_workflows,
                    "plan_sha256": w4_digest,
                    "source_binding_checked": flowmesh_w4_source_bound,
                    "component_evidence_class": (
                        flowmesh_w4_component_evidence
                    ),
                    "ready_for_n1_hidden_relevance_evaluation": True,
                    "real_cloud_performance_measured": False,
                },
            }
            verifiers: dict[str, Any] = {}
            for name, value in results.items():
                verifiers[name] = stack.enter_context(
                    patch.object(readiness, name, return_value=value)
                )
            yield verifiers

    @staticmethod
    def verify_arguments(arguments: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in arguments.items()
            if key != "audit_id"
        }

    def test_minimal_audit_is_honest_about_missing_evidence(self) -> None:
        arguments = self.fixture.required()
        with self.verified_sources() as verifiers:
            frozen = readiness.freeze_full_flow_pre_upcloud_readiness(
                **arguments
            )
            verified = readiness.verify_full_flow_pre_upcloud_readiness(
                **self.verify_arguments(arguments)
            )
        verifiers["verify_full_flow_observations"].assert_not_called()
        verifiers[
            "verify_smoke_gated_full_flow_local_semantic_matrix_run"
        ].assert_not_called()
        self.assertEqual("FROZEN", frozen["status"])
        self.assertEqual(readiness.REQUIRED_CODE_READY, verified["required_code"])
        self.assertEqual(
            readiness.LOCAL_LIVE_EVIDENCE_MISSING,
            verified["local_live_evidence"],
        )
        self.assertEqual(
            readiness.FLOWMESH_EVIDENCE_MISSING,
            verified["flowmesh_evidence"],
        )
        self.assertEqual(readiness.UPCLOUD_ONLY_GAPS_REMAIN, verified["upcloud"])
        self.assertFalse(verified["eligible_for_scientific_claims"])
        output = arguments["output_dir"]
        self.assertEqual(
            {readiness.REPORT_NAME, readiness.MANIFEST_NAME, readiness.CHECKSUMS_NAME},
            {path.name for path in output.iterdir()},
        )

    def test_complete_local_and_flowmesh_evidence_is_source_bound(self) -> None:
        arguments = self.fixture.full()
        with self.verified_sources():
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
        report_path = arguments["output_dir"] / readiness.REPORT_NAME
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(
            readiness.LOCAL_LIVE_EVIDENCE_PRESENT,
            report["readiness"]["local_live_evidence"],
        )
        self.assertEqual(
            readiness.FLOWMESH_EVIDENCE_PRESENT,
            report["readiness"]["flowmesh_evidence"],
        )
        self.assertEqual(37, report["source_count"])
        self.assertEqual(5, len(report["upcloud_only_gaps"]))
        self.assertTrue(report["claim_boundary"]["offline_verification_only"])
        self.assertFalse(
            report["claim_boundary"]["performance_readiness_claimed"]
        )
        serialized = report_path.read_text(encoding="utf-8")
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn("://", serialized)
        self.assertNotIn("runtime-token", serialized)
        self.assertNotIn("relevance", serialized)
        self.assertEqual(
            {
                "infrastructure_matrix_64_trial_execution": "VERIFIED",
                "w4_candidate_16_task_live_execution": "VERIFIED",
            },
            report["flowmesh_evidence_components"],
        )

    def test_fake_component_receipt_does_not_count_as_local_live(self) -> None:
        arguments = self.fixture.full("fake-component-audit")
        with self.verified_sources(
            component_evidence="strict-fake-component-conformance"
        ):
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
        report = json.loads(
            (arguments["output_dir"] / readiness.REPORT_NAME).read_text()
        )
        self.assertEqual(
            readiness.LOCAL_LIVE_EVIDENCE_MISSING,
            report["readiness"]["local_live_evidence"],
        )
        self.assertFalse(
            report["local_live_evidence_components"]["w4_component_is_live"]
        )

    def test_semantic_outer_verifier_receives_every_exact_source(self) -> None:
        arguments = self.fixture.full("outer-binding-audit")
        with self.verified_sources() as verifiers:
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
        outer = verifiers[
            "verify_smoke_gated_full_flow_local_semantic_matrix_run"
        ]
        self.assertEqual(2, outer.call_count)
        for call in outer.call_args_list:
            self.assertEqual(
                (
                    self.fixture.semantic_admission,
                    self.fixture.ten_smoke,
                    self.fixture.n4_serve_gate,
                    self.fixture.compose_overlay,
                    self.fixture.service_bootstrap,
                    self.fixture.paths["provisioning_catalog_dir"],
                    self.fixture.paths["artifact_binding_dir"],
                    self.fixture.paths["n4_package_dir"],
                    self.fixture.semantic_matrix,
                    self.fixture.deployment_binding,
                    self.fixture.paths["logical_route_dir"],
                    self.fixture.paths["scenario_path"],
                    self.fixture.paths["container_plan_dir"],
                    self.fixture.public_tasks,
                    self.fixture.semantic_artifact_binding,
                ),
                call.args,
            )
            self.assertEqual(
                self.fixture.semantic_outer,
                call.kwargs["output_dir"],
            )
            self.assertIsNone(call.kwargs["n4_live_gate_sources"])
        observations = verifiers["verify_full_flow_observations"]
        for call in observations.call_args_list:
            self.assertEqual(
                self.fixture.semantic_inner,
                call.kwargs["semantic_matrix_run_dir"],
            )
            self.assertEqual(
                self.fixture.n1_oracle,
                call.kwargs["n1_oracle_package_dir"],
            )

    def test_semantic_group_and_authenticity_fail_closed(self) -> None:
        partial = self.fixture.required("partial-semantic-audit")
        partial["neutral_observation_dir"] = self.fixture.neutral_observations
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "smoke-gated semantic evidence inputs must be supplied together",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**partial)

        for overrides, suffix in (
            ({"neutral_authentic": False}, "authenticity"),
            ({"neutral_generic_count": 63}, "generic-count"),
            ({"neutral_run_integrity": False}, "run-integrity"),
            ({"outer_completed_trials": 63}, "outer-completeness"),
        ):
            with self.subTest(suffix=suffix):
                arguments = self.fixture.full(f"bad-semantic-{suffix}")
                with self.verified_sources(**overrides):
                    with self.assertRaises(
                        readiness.FullFlowPreUpcloudReadinessError
                    ):
                        readiness.freeze_full_flow_pre_upcloud_readiness(
                            **arguments
                        )

    def test_semantic_and_w4_nested_run_paths_are_exact(self) -> None:
        wrong_inner = self.fixture.full("wrong-inner-audit")
        wrong_inner["neutral_semantic_matrix_run_dir"] = self.fixture._directory(
            "wrong-semantic-inner"
        )
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "exact inner matrix-run",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**wrong_inner)

        wrong_w4 = self.fixture.full("wrong-w4-nested-audit")
        wrong_w4["w4_coordinator_run_dir"] = self.fixture._directory(
            "wrong-w4-candidate"
        )
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "exact nested candidate-run and component-receipt",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**wrong_w4)

    def test_implementation_revision_and_optional_archive_are_bound(self) -> None:
        archive = self.root / "source-code.tar"
        archive.write_bytes(b"frozen source bytes")
        arguments = self.fixture.required("revision-bound-audit")
        arguments["source_archive_path"] = archive
        with self.verified_sources():
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
        report = json.loads(
            (arguments["output_dir"] / readiness.REPORT_NAME).read_text()
        )
        self.assertEqual(
            "a" * 40,
            report["implementation_revision"]["source_git_revision"],
        )
        self.assertTrue(
            report["implementation_revision"][
                "operator_declared_clean_committed_source"
            ]
        )
        self.assertEqual(
            readiness._sha256(archive.read_bytes()),
            report["implementation_revision"][
                "source_archive_file_sha256"
            ],
        )
        invalid = self.fixture.required("invalid-revision-audit")
        invalid["source_git_revision"] = "not-a-commit"
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "lowercase 40-hex",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**invalid)
        unattested = self.fixture.required("unattested-source-audit")
        unattested["operator_attests_clean_committed_source"] = False
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "operator must attest",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**unattested)

    def test_live_n4_descriptor_is_forwarded_and_content_bound(self) -> None:
        publication = self.fixture._directory("live-publication-store")
        receipt = self.fixture._directory("live-frame-receipt")
        live_sources = {
            "live_receipt_bindings": ({
                "kind": "frame_bundle",
                "receipt_dir": receipt,
                "n5_plan": {"plan_id": "frame-plan-v1"},
            },),
            "n4_publication_store_root": publication,
            "rebound_artifact_binding_dir": self.fixture.paths[
                "artifact_binding_dir"
            ],
            "rebound_semantic_matrix_dir": self.fixture.semantic_matrix,
            "rebound_admission_dir": self.fixture.semantic_admission,
        }
        arguments = self.fixture.full("live-n4-audit")
        arguments["n4_live_gate_sources"] = live_sources
        with self.verified_sources() as verifiers:
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
        outer = verifiers[
            "verify_smoke_gated_full_flow_local_semantic_matrix_run"
        ]
        self.assertTrue(all(
            call.kwargs["n4_live_gate_sources"] is live_sources
            for call in outer.call_args_list
        ))
        report = json.loads(
            (arguments["output_dir"] / readiness.REPORT_NAME).read_text()
        )
        source_ids = {row["source_id"] for row in report["sources"]}
        self.assertIn("n4-live-publication-store", source_ids)
        self.assertIn("n4-live-receipt-000", source_ids)

    def test_flowmesh_readiness_requires_both_verified_subcomponents(self) -> None:
        infrastructure_only = self.fixture.full("infrastructure-only-audit")
        for name in (
            "flowmesh_w4_plan_dir",
            "flowmesh_w4_run_dir",
            "w4_component_receipt_dir",
            "w4_coordinator_run_dir",
            "w4_retrieval_contract_dir",
            "w4_retrieval_evaluation_dir",
        ):
            infrastructure_only.pop(name)
        with self.verified_sources():
            readiness.freeze_full_flow_pre_upcloud_readiness(
                **infrastructure_only
            )
        infrastructure_report = json.loads(
            (
                infrastructure_only["output_dir"] / readiness.REPORT_NAME
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            readiness.FLOWMESH_EVIDENCE_MISSING,
            infrastructure_report["readiness"]["flowmesh_evidence"],
        )
        self.assertEqual(
            {
                "infrastructure_matrix_64_trial_execution": "VERIFIED",
                "w4_candidate_16_task_live_execution": "MISSING",
            },
            infrastructure_report["flowmesh_evidence_components"],
        )

        w4_only = self.fixture.full("w4-only-audit")
        for name in (
            "flowmesh_matrix_plan_dir",
            "flowmesh_formal_profile_dir",
            "flowmesh_coordinator_plan_dir",
            "flowmesh_matrix_run_dir",
        ):
            w4_only.pop(name)
        with self.verified_sources():
            readiness.freeze_full_flow_pre_upcloud_readiness(**w4_only)
        w4_report = json.loads(
            (w4_only["output_dir"] / readiness.REPORT_NAME).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            readiness.FLOWMESH_EVIDENCE_MISSING,
            w4_report["readiness"]["flowmesh_evidence"],
        )
        self.assertEqual(
            {
                "infrastructure_matrix_64_trial_execution": "MISSING",
                "w4_candidate_16_task_live_execution": "VERIFIED",
            },
            w4_report["flowmesh_evidence_components"],
        )

    def test_w4_flowmesh_evidence_shape_and_live_class_fail_closed(self) -> None:
        cases = (
            ({"flowmesh_w4_completed_tasks": 15}, "completed"),
            ({"flowmesh_w4_workflows": 2}, "workflow"),
            ({"flowmesh_w4_source_bound": False}, "source"),
            (
                {
                    "flowmesh_w4_component_evidence": (
                        "strict-fake-component-conformance"
                    )
                },
                "evidence-class",
            ),
        )
        for overrides, suffix in cases:
            with self.subTest(suffix=suffix):
                arguments = self.fixture.full(f"invalid-w4-{suffix}-audit")
                with self.verified_sources(**overrides):
                    with self.assertRaisesRegex(
                        readiness.FullFlowPreUpcloudReadinessError,
                        "bound verified 16-task live-component execution",
                    ):
                        readiness.freeze_full_flow_pre_upcloud_readiness(
                            **arguments
                        )

    def test_local_live_requires_source_bound_n1_retrieval_scoring(self) -> None:
        arguments = self.fixture.full("missing-retrieval-audit")
        arguments.pop("w4_retrieval_contract_dir")
        arguments.pop("w4_retrieval_evaluation_dir")
        with self.verified_sources():
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
        report = json.loads(
            (arguments["output_dir"] / readiness.REPORT_NAME).read_text()
        )
        self.assertEqual(
            readiness.LOCAL_LIVE_EVIDENCE_MISSING,
            report["readiness"]["local_live_evidence"],
        )
        self.assertEqual(
            "MISSING",
            report["local_live_evidence_components"][
                "w4_retrieval_evaluation"
            ],
        )

    def test_retrieval_verifier_uses_exact_coordinator_observations(self) -> None:
        arguments = self.fixture.full("retrieval-binding-audit")
        with self.verified_sources() as verifiers:
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
        evaluation = verifiers[
            "verify_full_flow_w4_retrieval_evaluation"
        ]
        expected_evaluation = (
            self.fixture.w4_retrieval_evaluation,
        )
        expected_evaluation_keywords = {
            "contract_dir": self.fixture.w4_retrieval_contract,
            "observations_path": (
                self.fixture.w4_coordinator
                / readiness.W4_RETRIEVAL_OBSERVATIONS_NAME
            ),
        }
        self.assertEqual(2, evaluation.call_count)
        for call in evaluation.call_args_list:
            self.assertEqual(expected_evaluation, call.args)
            self.assertEqual(expected_evaluation_keywords, call.kwargs)
        matrix_run = verifiers["verify_flowmesh_container_matrix_run"]
        self.assertEqual(2, matrix_run.call_count)
        for call in matrix_run.call_args_list:
            self.assertEqual((self.fixture.flowmesh_run,), call.args)
            self.assertEqual(
                {
                    "matrix_plan_dir": self.fixture.flowmesh_matrix,
                    "formal_execution_profile_dir": self.fixture.flowmesh_profile,
                    "coordinator_plan_dir": self.fixture.flowmesh_coordinator,
                },
                call.kwargs,
            )
        w4_plan = verifiers["verify_flowmesh_w4_candidate_matrix_plan"]
        self.assertEqual(2, w4_plan.call_count)
        for call in w4_plan.call_args_list:
            self.assertEqual((self.fixture.flowmesh_w4_plan,), call.args)
            self.assertEqual(
                {"route_package_dir": self.fixture.paths["w4_route_package_dir"]},
                call.kwargs,
            )
        w4_run = verifiers["verify_flowmesh_w4_candidate_matrix_run"]
        self.assertEqual(2, w4_run.call_count)
        for call in w4_run.call_args_list:
            self.assertEqual((self.fixture.flowmesh_w4_run,), call.args)
            self.assertEqual(
                {
                    "plan_dir": self.fixture.flowmesh_w4_plan,
                    "route_package_dir": self.fixture.paths[
                        "w4_route_package_dir"
                    ],
                    "crosswalk_dir": self.fixture.paths[
                        "w4_index_crosswalk_dir"
                    ],
                    "index_package_dir": self.fixture.paths[
                        "w4_index_package_dir"
                    ],
                },
                call.kwargs,
            )

    def test_unbound_scoring_or_incomplete_flowmesh_run_fails_closed(self) -> None:
        unbound = self.fixture.full("unbound-scoring-audit")
        with self.verified_sources(retrieval_source_bound=False):
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "complete source-bound 16-trial N1 scoring replay",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**unbound)

        incomplete = self.fixture.full("incomplete-flowmesh-audit")
        with self.verified_sources(flowmesh_completed_trials=63):
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "bound verified 64-trial execution",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**incomplete)

    def test_missing_exact_coordinator_observations_fail_closed(self) -> None:
        arguments = self.fixture.full("missing-observations-audit")
        (
            self.fixture.w4_coordinator
            / readiness.W4_RETRIEVAL_OBSERVATIONS_NAME
        ).unlink()
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "no safe exact retrieval observations",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)

    def test_source_tamper_and_duplicate_output_keys_are_rejected(self) -> None:
        arguments = self.fixture.required("tamper-audit")
        with self.verified_sources():
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
            source = self.fixture.paths["w4_route_package_dir"] / "artifact.json"
            source.write_text('{"tampered":true}\n', encoding="utf-8")
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "differs from its current verified sources",
            ):
                readiness.verify_full_flow_pre_upcloud_readiness(
                    **self.verify_arguments(arguments)
                )

        duplicate_arguments = self.fixture.required("duplicate-audit")
        with self.verified_sources():
            readiness.freeze_full_flow_pre_upcloud_readiness(
                **duplicate_arguments
            )
            report_path = (
                duplicate_arguments["output_dir"] / readiness.REPORT_NAME
            )
            text = report_path.read_text(encoding="utf-8")
            report_path.write_text(
                text.replace(
                    '  "audit_id": "pre-upcloud-audit-v1",',
                    '  "audit_id": "pre-upcloud-audit-v1",\n'
                    '  "audit_id": "pre-upcloud-audit-v1",',
                    1,
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "repeats JSON key audit_id",
            ):
                readiness.verify_full_flow_pre_upcloud_readiness(
                    **self.verify_arguments(duplicate_arguments)
                )

    def test_partial_optional_groups_and_wrong_core_shape_fail_closed(self) -> None:
        partial = self.fixture.required("partial-audit")
        partial["bulk_provisioning_output_dir"] = self.fixture.bulk
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "bulk live evidence inputs must be supplied together",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**partial)
        partial_retrieval = self.fixture.full("partial-retrieval-audit")
        partial_retrieval.pop("w4_retrieval_evaluation_dir")
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "W4 retrieval evaluation evidence inputs must be supplied "
                "together",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(
                    **partial_retrieval
                )
        partial_flowmesh = self.fixture.full("partial-flowmesh-audit")
        partial_flowmesh.pop("flowmesh_matrix_run_dir")
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "FlowMesh infrastructure evidence inputs must be supplied "
                "together",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(
                    **partial_flowmesh
                )
        partial_flowmesh_w4 = self.fixture.full("partial-flowmesh-w4-audit")
        partial_flowmesh_w4.pop("flowmesh_w4_run_dir")
        with self.verified_sources():
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "FlowMesh W4 evidence inputs must be supplied together",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(
                    **partial_flowmesh_w4
                )
        wrong = self.fixture.required("wrong-shape-audit")
        with self.verified_sources(catalog_entries=70):
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "exact 36-by-2",
            ):
                readiness.freeze_full_flow_pre_upcloud_readiness(**wrong)

    def test_w4_flowmesh_source_tamper_invalidates_frozen_readiness(self) -> None:
        arguments = self.fixture.full("w4-flowmesh-source-tamper-audit")
        with self.verified_sources():
            readiness.freeze_full_flow_pre_upcloud_readiness(**arguments)
            source = self.fixture.flowmesh_w4_run / "artifact.json"
            source.write_text('{"tampered":true}\n', encoding="utf-8")
            with self.assertRaisesRegex(
                readiness.FullFlowPreUpcloudReadinessError,
                "differs from its current verified sources",
            ):
                readiness.verify_full_flow_pre_upcloud_readiness(
                    **self.verify_arguments(arguments)
                )


class FullFlowPreUpcloudReadinessCliTest(unittest.TestCase):
    required_flags = (
        ("--provisioning-catalog-dir", "catalog"),
        ("--artifact-binding-dir", "bindings"),
        ("--n4-package-dir", "n4"),
        ("--logical-route-dir", "routes"),
        ("--scenario", "scenario.json"),
        ("--container-plan-dir", "container"),
        ("--task-plane-dir", "tasks"),
        ("--n3-package-dir", "n3"),
        ("--w4-route-package-dir", "w4-routes"),
        ("--w4-index-package-dir", "index"),
        ("--w4-index-crosswalk-dir", "crosswalk"),
        ("--source-git-revision", "a" * 40),
        ("--attest-clean-committed-source",),
    )

    def arguments(self) -> list[str]:
        return [item for pair in self.required_flags for item in pair]

    def invoke(self, arguments: list[str]) -> tuple[int, dict[str, Any]]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        lines = stdout.getvalue().splitlines()
        self.assertEqual(1, len(lines), stdout.getvalue())
        return status, json.loads(lines[0])

    def test_public_exports_parser_and_both_dispatches(self) -> None:
        choices = next(
            action.choices
            for action in _parser()._actions
            if action.dest == "command"
        )
        commands = {
            "freeze-simulator-full-flow-pre-upcloud-readiness",
            "verify-simulator-full-flow-pre-upcloud-readiness",
        }
        self.assertTrue(commands.issubset(choices))
        freeze_help = choices[
            "freeze-simulator-full-flow-pre-upcloud-readiness"
        ].format_help()
        self.assertIn("--w4-retrieval-contract-dir", freeze_help)
        self.assertIn("--w4-retrieval-evaluation-dir", freeze_help)
        self.assertIn("--flowmesh-matrix-run-dir", freeze_help)
        self.assertIn("--flowmesh-w4-plan-dir", freeze_help)
        self.assertIn("--flowmesh-w4-run-dir", freeze_help)
        self.assertIn("--smoke-gated-semantic-matrix-run-dir", freeze_help)
        self.assertIn("--n4-live-gate-sources", freeze_help)
        self.assertIn("--source-git-revision", freeze_help)
        self.assertIn("source-bound N1 W4 retrieval", freeze_help)
        self.assertIn("completed 64-trial FlowMesh", freeze_help)
        for name in (
            "FullFlowPreUpcloudReadinessError",
            "freeze_full_flow_pre_upcloud_readiness",
            "verify_full_flow_pre_upcloud_readiness",
        ):
            self.assertIn(name, simulator.__all__)
            self.assertTrue(hasattr(simulator, name))

        with patch(
            "pathfinder.simulator.full_flow_pre_upcloud_readiness."
            "freeze_full_flow_pre_upcloud_readiness",
            return_value={"status": "FROZEN"},
        ) as freeze:
            status, payload = self.invoke([
                "freeze-simulator-full-flow-pre-upcloud-readiness",
                *self.arguments(),
                "--audit-id",
                "audit-v1",
                "--output-dir",
                "audit",
                "--w4-retrieval-contract-dir",
                "retrieval-contract",
                "--w4-retrieval-evaluation-dir",
                "retrieval-evaluation",
                "--flowmesh-matrix-run-dir",
                "matrix-run",
                "--flowmesh-w4-plan-dir",
                "w4-flowmesh-plan",
                "--flowmesh-w4-run-dir",
                "w4-flowmesh-run",
                "--neutral-observation-dir",
                "observations",
                "--neutral-semantic-matrix-run-dir",
                "semantic-outer/matrix-run",
                "--semantic-execution-admission-dir",
                "admission",
                "--smoke-gated-semantic-matrix-run-dir",
                "semantic-outer",
                "--ten-smoke-dir",
                "ten-smoke",
                "--n4-serve-gate-dir",
                "n4-gate",
                "--compose-overlay-dir",
                "overlay",
                "--service-bootstrap-dir",
                "bootstrap",
                "--deployment-binding-dir",
                "deployment",
                "--semantic-matrix-dir",
                "semantic-matrix",
                "--public-task-set",
                "tasks.json",
                "--semantic-artifact-binding",
                "bindings/artifacts.json",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN", payload["status"])
        self.assertEqual("audit-v1", freeze.call_args.kwargs["audit_id"])
        self.assertEqual(
            Path("scenario.json"), freeze.call_args.kwargs["scenario_path"]
        )
        self.assertEqual(
            Path("retrieval-contract"),
            freeze.call_args.kwargs["w4_retrieval_contract_dir"],
        )
        self.assertEqual(
            Path("retrieval-evaluation"),
            freeze.call_args.kwargs["w4_retrieval_evaluation_dir"],
        )
        self.assertEqual(
            Path("matrix-run"),
            freeze.call_args.kwargs["flowmesh_matrix_run_dir"],
        )
        self.assertEqual(
            Path("w4-flowmesh-plan"),
            freeze.call_args.kwargs["flowmesh_w4_plan_dir"],
        )
        self.assertEqual(
            Path("w4-flowmesh-run"),
            freeze.call_args.kwargs["flowmesh_w4_run_dir"],
        )
        self.assertEqual(
            Path("semantic-outer"),
            freeze.call_args.kwargs[
                "smoke_gated_semantic_matrix_run_dir"
            ],
        )
        self.assertEqual("a" * 40, freeze.call_args.kwargs["source_git_revision"])
        self.assertTrue(
            freeze.call_args.kwargs[
                "operator_attests_clean_committed_source"
            ]
        )

        with patch(
            "pathfinder.simulator.full_flow_pre_upcloud_readiness."
            "verify_full_flow_pre_upcloud_readiness",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self.invoke([
                "verify-simulator-full-flow-pre-upcloud-readiness",
                *self.arguments(),
                "--output-dir",
                "audit",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        self.assertEqual(Path("audit"), verify.call_args.kwargs["output_dir"])
        self.assertNotIn("audit_id", verify.call_args.kwargs)

    def test_cli_uses_established_n4_live_source_descriptor_loader(self) -> None:
        normalized = {
            "live_receipt_bindings": (),
            "n4_publication_store_root": Path("store"),
            "rebound_artifact_binding_dir": Path("bindings"),
            "rebound_semantic_matrix_dir": Path("semantic"),
            "rebound_admission_dir": Path("admission"),
        }
        with (
            patch(
                "pathfinder.cli._load_n4_live_gate_sources",
                return_value=normalized,
            ) as loader,
            patch(
                "pathfinder.simulator.full_flow_pre_upcloud_readiness."
                "freeze_full_flow_pre_upcloud_readiness",
                return_value={"status": "FROZEN"},
            ) as freeze,
        ):
            status, _ = self.invoke([
                "freeze-simulator-full-flow-pre-upcloud-readiness",
                *self.arguments(),
                "--audit-id",
                "audit-live-v1",
                "--output-dir",
                "audit-live",
                "--n4-live-gate-sources",
                "live-sources.json",
            ])
        self.assertEqual(0, status)
        loader.assert_called_once_with(Path("live-sources.json"))
        self.assertIs(
            normalized,
            freeze.call_args.kwargs["n4_live_gate_sources"],
        )


if __name__ == "__main__":
    unittest.main()
