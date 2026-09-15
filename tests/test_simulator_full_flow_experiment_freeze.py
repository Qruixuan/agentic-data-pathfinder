from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_artifact_bindings import (
    ARTIFACT_BINDINGS_NAME,
    build_full_flow_artifact_bindings,
)
from pathfinder.simulator.full_flow_compose_overlay import (
    render_full_flow_local_compose_overlay,
)
from pathfinder.simulator.full_flow_deployment import (
    DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2,
    build_full_flow_deployment_binding,
    full_flow_w4_runtime_service_binding_requirements,
)
from pathfinder.simulator.full_flow_experiment_freeze import (
    CHECKSUMS_NAME,
    EXPERIMENT_FREEZE_NAME,
    FullFlowExperimentFreezeError,
    freeze_full_flow_offline_experiment,
    verify_full_flow_offline_experiment,
)
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_semantic_execution_admission import (
    freeze_full_flow_semantic_execution_admission,
)
from pathfinder.simulator.full_flow_semantic_matrix import (
    compile_full_flow_semantic_matrix,
)
from pathfinder.simulator.full_flow_service_bootstrap import (
    freeze_full_flow_local_service_bootstrap,
)
from pathfinder.simulator.full_flow_tasks import (
    ORACLE_PACKAGE,
    PUBLIC_TASK_SET,
    build_full_flow_task_plane,
)
from pathfinder.simulator.hidden_oracle_commitment import (
    freeze_n1_oracle_preselection_commitment,
)
from pathfinder.simulator.n4_derived_data_plane import (
    FRAME_BUNDLE_REPRESENTATION_ID,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    build_n4_derived_data_package,
)
from pathfinder.simulator.policy_oed_bridge import (
    freeze_oed_prospective_selection,
    freeze_policy_assignment,
)
from pathfinder.simulator.portable import build_portable_execution_plan
from pathfinder.simulator.raw_cold_data_plane import (
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)

from tests.test_simulator_full_flow_artifact_bindings import (
    WORKLOADS,
    _bundle,
    _mp4,
    _semantic_spec,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"

_PERSISTENT = {
    "immutable-hidden-oracle",
    "durable-trial-identity",
    "immutable-content-addressed-artifacts",
    "frozen-index-snapshot",
    "idempotent-content-addressed-output",
    "persistent-with-explicit-cache-scope",
}


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _deployment_source(catalog: dict) -> dict:
    bindings = []
    for contract in catalog["service_contracts"]:
        contract_id = contract["service_contract_id"]
        network = contract["role"] == "logical-byte-transfer"
        nodes = sorted(contract["logical_node_ids"])
        if network:
            credentials = []
        elif contract_id == "N1.hidden-score":
            credentials = [
                "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET",
                "PATHFINDER_N1_ORACLE_TOKEN",
            ]
        elif contract_id == "N6.semantic-inference":
            credentials = ["PATHFINDER_CONTAINER_NODE_TOKEN"]
        elif contract_id == "N7.execution-compute":
            credentials = ["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"]
        else:
            credentials = ["PATHFINDER_BINDING_TOKEN"]
        bindings.append({
            "service_contract_id": contract_id,
            "adapter_id": "compose-contract-http-v1",
            "logical_node_ids": nodes,
            "actions": sorted(contract["actions"]),
            "representation_ids": [
                "multimodal_digest",
                "raw_video",
                "sampled_frame_bundle",
            ],
            "base_url": (
                None
                if network
                else f"http://127.0.0.1:{19080 + int(nodes[0][1:])}"
            ),
            "credential_env_names": credentials,
            "persistent_state": contract["state_semantics"] in _PERSISTENT,
        })
    runtime_bindings = [
        {
            **requirement,
            "base_url": (
                "http://127.0.0.1:"
                f"{19180 + int(requirement['logical_node_id'][1:])}"
            ),
        }
        for requirement in full_flow_w4_runtime_service_binding_requirements()
    ]
    return {
        "schema_version": DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2,
        "deployment_id": "local-full-flow-experiment-v1",
        "backend": "single-host-compose",
        "service_bindings": bindings,
        "runtime_service_bindings": runtime_bindings,
        "network_binding": {
            "adapter_id": "application-rate-rtt-shaper-v1",
            "mode": "application-shaped-single-host",
            "measurement_class": "configured-shaping-conformance",
            "parameters_fitted": False,
        },
        "trusted_private_http_hosts": [],
        "credentials_recorded": False,
    }


class FullFlowExperimentFreezeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        cls.logical = cls.root / "logical"
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

        cls.specs = []
        for index, (workload_id, (_, object_id)) in enumerate(
            sorted(WORKLOADS.items()), start=1
        ):
            cls.specs.append(_write_json(
                cls.root / f"semantic-spec-{index}.json",
                _semantic_spec(workload_id, object_id, index),
            ))
        cls.task_plane = cls.root / "task-plane"
        build_full_flow_task_plane(
            cls.specs,
            task_plane_id="experiment-task-plane-v1",
            oracle_id="experiment-oracle-v1",
            output_dir=cls.task_plane,
        )

        raw_bindings = []
        for index, (_, (_, object_id)) in enumerate(
            sorted(WORKLOADS.items()), start=1
        ):
            raw = _mp4(index)
            raw_path = cls.root / f"raw-{index}.mp4"
            raw_path.write_bytes(raw)
            raw_bindings.append(RawColdObjectBinding(
                object_id=object_id,
                artifact_path=raw_path,
                catalog_version="n3-experiment-catalog-v1",
                plan_ids=tuple(f"D{number}" for number in range(8)),
                dataset_id="nextqa",
                dataset_revision="test-v1",
                source_object_id=object_id.rsplit("-", 1)[-1],
                artifact_sha256=_sha256(raw),
                artifact_size_bytes=len(raw),
            ))
        cls.n3 = cls.root / "n3"
        build_raw_cold_data_plane_package(
            raw_bindings,
            output_dir=cls.n3,
            package_id="experiment-n3-v1",
        )

        derived = []
        for _, (logical_object_id, object_id) in sorted(WORKLOADS.items()):
            required = {MULTIMODAL_DIGEST_REPRESENTATION_ID}
            if logical_object_id != "video-descriptive":
                required.add(FRAME_BUNDLE_REPRESENTATION_ID)
            for representation_id in sorted(required):
                raw = (
                    _bundle(object_id)
                    if representation_id == FRAME_BUNDLE_REPRESENTATION_ID
                    else f"Verified digest for {object_id}.\n".encode()
                )
                derived.append(N4DerivedArtifactInput(
                    object_id=object_id,
                    representation_id=representation_id,
                    artifact_bytes=raw,
                    plan_ids=("D2", "D3", "D6", "D7"),
                    provenance=N4ArtifactProvenance(
                        producer_node_id="N5",
                        publication_source_id=f"n5-{representation_id}-{object_id}",
                        source_representation_id="raw_video",
                        source_content_sha256="d" * 64,
                        derivation_id=f"derive-{representation_id}-v1",
                        derivation_sha256="e" * 64,
                    ),
                    expected_sha256=_sha256(raw),
                    expected_size_bytes=len(raw),
                ))
        cls.n4 = cls.root / "n4"
        build_n4_derived_data_package(
            derived,
            output_dir=cls.n4,
            package_id="experiment-n4-v1",
            catalog_version="n4-experiment-catalog-v1",
        )

        cls.artifact_package = cls.root / "artifact-package"
        build_full_flow_artifact_bindings(
            cls.logical,
            SCENARIO,
            cls.container,
            cls.task_plane,
            cls.n3,
            cls.n4,
            binding_set_id="experiment-artifact-bindings-v1",
            output_dir=cls.artifact_package,
        )
        cls.public_tasks = cls.task_plane / PUBLIC_TASK_SET
        cls.artifacts = cls.artifact_package / ARTIFACT_BINDINGS_NAME
        cls.semantic = cls.root / "semantic"
        compile_full_flow_semantic_matrix(
            cls.logical,
            SCENARIO,
            cls.container,
            cls.public_tasks,
            cls.artifacts,
            output_dir=cls.semantic,
        )

        catalog = json.loads(
            (cls.logical / "logical-service-contracts.json").read_text()
        )
        source = _write_json(
            cls.root / "deployment-source.json",
            _deployment_source(catalog),
        )
        cls.deployment = cls.root / "deployment"
        build_full_flow_deployment_binding(
            cls.logical,
            SCENARIO,
            cls.container,
            source,
            output_dir=cls.deployment,
        )
        cls.bootstrap = cls.root / "bootstrap"
        freeze_full_flow_local_service_bootstrap(
            cls.logical,
            SCENARIO,
            cls.container,
            bootstrap_id="experiment-bootstrap-v1",
            output_dir=cls.bootstrap,
        )
        cls.overlay = cls.root / "overlay"
        render_full_flow_local_compose_overlay(
            cls.bootstrap,
            cls.deployment,
            logical_plan_dir=cls.logical,
            scenario_path=SCENARIO,
            container_plan_dir=cls.container,
            overlay_id="experiment-overlay-v1",
            output_dir=cls.overlay,
        )
        cls.oracle = cls.task_plane / ORACLE_PACKAGE
        cls.commitment = cls.root / "commitment"
        freeze_n1_oracle_preselection_commitment(
            cls.oracle,
            commitment_id="experiment-oracle-commitment-v1",
            output_dir=cls.commitment,
        )
        cls.admission = cls.root / "admission"
        freeze_full_flow_semantic_execution_admission(
            cls.semantic,
            cls.deployment,
            cls.logical,
            SCENARIO,
            cls.container,
            cls.public_tasks,
            cls.artifacts,
            cls.oracle,
            worker_alias="pathfinder-semantic-worker",
            output_dir=cls.admission,
        )

        cls.policy = cls.root / "policy"
        freeze_policy_assignment(
            logical_route_plan_dir=cls.logical,
            scenario_path=SCENARIO,
            container_plan_dir=cls.container,
            policy_id="experiment-policy-v1",
            awm_policy_sha256="a" * 64,
            assignments={f"W{index}": ["D0", "D2"] for index in range(1, 5)},
            output_dir=cls.policy,
        )
        trial_rows = [
            json.loads(line)
            for line in (
                cls.logical / "logical-route-trials.jsonl"
            ).read_text().splitlines()
        ]
        cls.oed = cls.root / "oed"
        freeze_oed_prospective_selection(
            logical_route_plan_dir=cls.logical,
            scenario_path=SCENARIO,
            container_plan_dir=cls.container,
            oed_request_id="experiment-oed-v1",
            oed_request_sha256="b" * 64,
            requested_trial_keys=[row["trial_key"] for row in trial_rows[:3]],
            output_dir=cls.oed,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def setUp(self) -> None:
        self.case_root = Path(tempfile.mkdtemp(dir=self.root))

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root)

    def _arguments(self, **overrides: object) -> dict:
        arguments = {
            "semantic_matrix_dir": self.semantic,
            "artifact_binding_package_dir": self.artifact_package,
            "deployment_binding_dir": self.deployment,
            "oracle_commitment_dir": self.commitment,
            "n1_oracle_package_dir": self.oracle,
            "service_bootstrap_dir": self.bootstrap,
            "compose_overlay_dir": self.overlay,
            "semantic_execution_admission_dir": self.admission,
            "logical_route_dir": self.logical,
            "scenario_path": SCENARIO,
            "container_plan_dir": self.container,
            "task_plane_dir": self.task_plane,
            "n3_package_dir": self.n3,
            "n4_package_dir": self.n4,
            "public_task_set_path": self.public_tasks,
            "artifact_binding_path": self.artifacts,
            "semantic_spec_paths": self.specs,
            "policy_assignment_dir": self.policy,
            "oed_selection_dir": self.oed,
        }
        arguments.update(overrides)
        return arguments

    def _freeze(self, name: str, **overrides: object) -> Path:
        output = self.case_root / name
        freeze_full_flow_offline_experiment(
            freeze_id="offline-full-flow-experiment-v1",
            output_dir=output,
            **self._arguments(**overrides),
        )
        return output

    def test_freezes_all_sources_but_remains_blocked(self) -> None:
        output = self._freeze("complete")
        report = verify_full_flow_offline_experiment(
            freeze_dir=output,
            **self._arguments(),
        )
        self.assertEqual("VERIFIED_BLOCKED", report["status"])
        self.assertEqual(64, report["trial_count"])
        self.assertEqual(2, report["repetitions"])
        self.assertEqual(20260909, report["scenario_seed"])
        self.assertEqual("vision-model-test", report["semantic_model_id"])
        self.assertFalse(report["flowmesh_submission_authorized"])

        document = json.loads((output / EXPERIMENT_FREEZE_NAME).read_text())
        self.assertEqual(
            "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
            document["status"],
        )
        self.assertEqual(
            list(range(64)),
            [
                index
                for index, _ in enumerate(
                    document["execution_contract"]["trial_order"]
                )
            ],
        )
        self.assertTrue(
            document["source_bindings"]["policy_assignment"]["included"]
        )
        self.assertTrue(
            document["source_bindings"]["oed_prospective_selection"][
                "included"
            ]
        )

    def test_public_freeze_contains_no_oracle_labels_endpoints_or_secrets(self) -> None:
        output = self._freeze("public")
        raw = (output / EXPERIMENT_FREEZE_NAME).read_text(encoding="utf-8")
        self.assertNotIn("correct_answer_id", raw)
        self.assertNotIn("http://", raw)
        self.assertNotIn("https://", raw)
        document = json.loads(raw)
        self.assertFalse(document["private_oracle_content_included"])
        self.assertFalse(document["endpoint_values_included"])
        self.assertTrue(
            document["deployment_endpoints_bound_by_hash_only"]
        )

    def test_optional_policy_and_oed_absence_is_explicit(self) -> None:
        output = self._freeze(
            "no-selections",
            policy_assignment_dir=None,
            oed_selection_dir=None,
        )
        document = json.loads((output / EXPERIMENT_FREEZE_NAME).read_text())
        self.assertEqual(
            "NOT_SUPPLIED",
            document["source_bindings"]["policy_assignment"]["status"],
        )
        self.assertEqual(
            "NOT_SUPPLIED",
            document["source_bindings"]["oed_prospective_selection"][
                "status"
            ],
        )

    def test_two_freezes_are_byte_identical(self) -> None:
        first = self._freeze("deterministic-a")
        second = self._freeze("deterministic-b")
        self.assertEqual(
            {path.name: path.read_bytes() for path in first.iterdir()},
            {path.name: path.read_bytes() for path in second.iterdir()},
        )

    def test_restamped_freeze_tampering_fails_source_recompilation(self) -> None:
        output = self._freeze("restamped")
        document = json.loads((output / EXPERIMENT_FREEZE_NAME).read_text())
        document["execution_contract"]["semantic_model_id"] = "attacker-model"
        unsigned = dict(document)
        unsigned.pop("freeze_sha256")
        document["freeze_sha256"] = _sha256(
            json.dumps(
                unsigned,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
        raw = (
            json.dumps(document, sort_keys=True, indent=2) + "\n"
        ).encode()
        (output / EXPERIMENT_FREEZE_NAME).write_bytes(raw)
        (output / CHECKSUMS_NAME).write_bytes(
            f"{_sha256(raw)}  {EXPERIMENT_FREEZE_NAME}\n".encode("utf-8")
        )
        with self.assertRaisesRegex(
            FullFlowExperimentFreezeError,
            "source recompilation",
        ):
            verify_full_flow_offline_experiment(
                freeze_dir=output,
                **self._arguments(),
            )

    def test_changed_semantic_source_model_fails_task_plane_opening(self) -> None:
        altered_paths = list(self.specs)
        altered = json.loads(altered_paths[0].read_text())
        altered["expected_model"] = "different-model"
        altered_paths[0] = _write_json(
            self.case_root / "altered-semantic-spec.json",
            altered,
        )
        with self.assertRaisesRegex(
            FullFlowExperimentFreezeError,
            "task-plane source hashes",
        ):
            self._freeze("changed-model", semantic_spec_paths=altered_paths)


if __name__ == "__main__":
    unittest.main()
