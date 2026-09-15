from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    build_semantic_route_request,
)
from pathfinder.simulator.full_flow_artifact_bindings import (
    ARTIFACT_BINDINGS_NAME,
)
from pathfinder.simulator.full_flow_artifact_preflight import (
    FullFlowArtifactPreflightError,
    preflight_full_flow_semantic_artifacts,
)
from pathfinder.simulator.full_flow_deployment import (
    build_full_flow_deployment_binding,
)
from pathfinder.simulator.full_flow_exact_range_catalog import (
    CATALOG_NAME as RANGE_CATALOG_NAME,
    build_full_flow_exact_range_catalog,
)
from pathfinder.simulator.full_flow_local_semantic_admission import (
    ADMISSION_NAME,
    CHECKSUMS_NAME,
    INVENTORY_NAME,
    LOCAL_SEMANTICS_MODE,
    SMOKES_NAME,
    STAGES_NAME,
    TRIALS_NAME,
    FullFlowLocalSemanticAdmissionError,
    load_full_flow_local_semantic_execution_inputs,
    promote_full_flow_local_semantic_execution_admission,
    verify_full_flow_local_semantic_execution_admission,
    verify_full_flow_local_semantic_runtime_package,
)
from pathfinder.simulator.full_flow_provisioning_catalog import (
    build_full_flow_provisioning_catalog,
)
from pathfinder.simulator.full_flow_semantic_execution_admission import (
    freeze_full_flow_semantic_execution_admission,
)
from pathfinder.simulator.full_flow_semantic_matrix import (
    compile_full_flow_semantic_matrix,
)
from tests import test_simulator_full_flow_artifact_bindings as artifact_fixture
from tests.test_simulator_full_flow_artifact_preflight import FakeProbe
from tests import (
    test_simulator_full_flow_semantic_execution_admission as admission_fixture,
)


SCENARIO = admission_fixture.SCENARIO


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


class FullFlowLocalSemanticAdmissionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Reuse the real-media fixture used by artifact-binding integration
        # tests.  The package builders and verifiers themselves are not mocked.
        artifact_fixture.FullFlowArtifactBindingsTest.setUpClass()
        cls.source = artifact_fixture.FullFlowArtifactBindingsTest(
            "test_builds_source_verified_binding_set_for_all_four_objects"
        )
        cls.source_root = artifact_fixture.FullFlowArtifactBindingsTest.root
        cls.root_temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.root_temp.name)
        cls.portable = artifact_fixture.FullFlowArtifactBindingsTest.portable
        cls.container = artifact_fixture.FullFlowArtifactBindingsTest.container
        cls.logical = artifact_fixture.FullFlowArtifactBindingsTest.logical
        cls.n3 = artifact_fixture.FullFlowArtifactBindingsTest.n3
        cls.n4 = artifact_fixture.FullFlowArtifactBindingsTest.n4
        cls.task_plane = artifact_fixture.FullFlowArtifactBindingsTest.task_plane
        cls.public_tasks = cls.task_plane / "public/public-tasks.json"
        cls.oracle = cls.task_plane / "n1-private/oracle-package"

        cls.bindings = cls.source.build("local-promotion-bindings")
        cls.artifact_binding = cls.bindings / ARTIFACT_BINDINGS_NAME
        cls.semantic = cls.root / "semantic"
        compile_full_flow_semantic_matrix(
            cls.logical,
            SCENARIO,
            cls.container,
            cls.public_tasks,
            cls.artifact_binding,
            output_dir=cls.semantic,
        )

        logical_catalog = json.loads(
            (cls.logical / "logical-service-contracts.json").read_text(
                encoding="utf-8"
            )
        )
        deployment_source = (
            admission_fixture.FullFlowSemanticExecutionAdmissionTest._deployment_source(
                logical_catalog
            )
        )
        cls.deployment_source = cls.root / "deployment-source.json"
        _json(cls.deployment_source, deployment_source)
        cls.deployment = cls.root / "deployment"
        build_full_flow_deployment_binding(
            cls.logical,
            SCENARIO,
            cls.container,
            cls.deployment_source,
            output_dir=cls.deployment,
        )

        cls.legacy = cls.root / "legacy-admission"
        freeze_full_flow_semantic_execution_admission(
            cls.semantic,
            cls.deployment,
            cls.logical,
            SCENARIO,
            cls.container,
            cls.public_tasks,
            cls.artifact_binding,
            cls.oracle,
            worker_alias="pathfinder-local-semantic-worker",
            admission_id="local-promotion-source-v1",
            output_dir=cls.legacy,
        )
        cls.preflight = cls.root / "artifact-preflight"
        n3_manifest_path = cls.n3 / "raw-cold-data-plane.json"
        n4_manifest_path = cls.n4 / "n4-derived-data-package.json"
        n3_manifest = json.loads(n3_manifest_path.read_text(encoding="utf-8"))
        n4_manifest = json.loads(n4_manifest_path.read_text(encoding="utf-8"))
        plan_ids = {
            "raw_video": n3_manifest["objects"][0]["plan_ids"][0],
        }
        plan_ids.update({
            row["representation_id"]: row["plan_ids"][0]
            for row in n4_manifest["objects"]
        })
        preflight_full_flow_semantic_artifacts(
            cls.legacy,
            preflight_id="local-promotion-preflight-v1",
            probe=FakeProbe(
                plan_ids=plan_ids,
                plan_binding_source_sha256={
                    "N3": _sha256(n3_manifest_path.read_bytes()),
                    "N4": _sha256(n4_manifest_path.read_bytes()),
                },
            ),
            output_dir=cls.preflight,
        )
        cls.ranges = cls.root / "exact-ranges"
        build_full_flow_exact_range_catalog(
            cls.n3,
            catalog_id="local-promotion-full-object-ranges-v1",
            output_dir=cls.ranges,
        )
        cls.provisioning = cls.root / "provisioning"
        build_full_flow_provisioning_catalog(
            cls.bindings,
            cls.n4,
            catalog_id="local-promotion-preprovisioned-v1",
            output_dir=cls.provisioning,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.root_temp.cleanup()
        artifact_fixture.FullFlowArtifactBindingsTest.tearDownClass()

    def _arguments(self, output: Path) -> dict[str, object]:
        return {
            "legacy_admission_dir": self.legacy,
            "semantic_matrix_dir": self.semantic,
            "deployment_binding_dir": self.deployment,
            "logical_route_dir": self.logical,
            "scenario_path": SCENARIO,
            "container_plan_dir": self.container,
            "public_task_set_path": self.public_tasks,
            "artifact_binding_path": self.artifact_binding,
            "n1_oracle_package_dir": self.oracle,
            "artifact_preflight_dir": self.preflight,
            "exact_range_catalog_dir": self.ranges,
            "n3_package_dir": self.n3,
            "provisioning_catalog_dir": self.provisioning,
            "n4_package_dir": self.n4,
            "semantics_mode": LOCAL_SEMANTICS_MODE,
            "promotion_id": "local-semantic-promotion-test-v1",
            "output_dir": output,
        }

    def _promote(self, name: str) -> Path:
        output = self.root / name
        promote_full_flow_local_semantic_execution_admission(
            **self._arguments(output)
        )
        return output

    def _verify(self, output: Path) -> dict:
        arguments = self._arguments(output)
        del arguments["semantics_mode"]
        del arguments["promotion_id"]
        del arguments["output_dir"]
        return verify_full_flow_local_semantic_execution_admission(
            output,
            **arguments,
        )

    def test_promotes_exactly_64_trial_templates_and_preserves_sources(self) -> None:
        before = {
            path.name: path.read_bytes()
            for path in self.legacy.iterdir()
        }
        output = self._promote("complete")
        report = self._verify(output)
        self.assertEqual(
            "VERIFIED_LOCAL_SEMANTIC_CONFORMANCE_INPUTS",
            report["status"],
        )
        self.assertEqual(64, report["trial_count"])
        self.assertTrue(report["trial_templates_authorized"])
        self.assertFalse(report["full_matrix_runtime_gate_satisfied"])
        self.assertEqual(
            before,
            {path.name: path.read_bytes() for path in self.legacy.iterdir()},
        )

        source = _jsonl(self.legacy / TRIALS_NAME)
        promoted = _jsonl(output / TRIALS_NAME)
        self.assertEqual(
            [row["source_semantic_trial_sha256"] for row in source],
            [row["source_semantic_trial_sha256"] for row in promoted],
        )
        self.assertTrue(all(
            row["required_runtime_adapter_ids"] == []
            and row["flowmesh_submission_authorized"] is True
            for row in promoted
        ))
        self.assertEqual(
            (self.legacy / STAGES_NAME).read_bytes(),
            (output / STAGES_NAME).read_bytes(),
        )

    def test_output_is_accepted_by_existing_flowmesh_request_contract(self) -> None:
        output = self._promote("executor-compatible")
        trial = _jsonl(output / TRIALS_NAME)[0]
        stages = {
            row["stage_key"]: row
            for row in _jsonl(output / STAGES_NAME)
        }
        request = build_semantic_route_request(
            run_id="local-conformance-run-v1",
            idempotency_key="a" * 64,
            bound_trial=trial,
            bound_stages=[stages[key] for key in trial["semantic_stage_keys"]],
        )
        self.assertEqual(trial, request["bound_trial"])

    def test_public_runtime_loader_needs_no_oracle_or_original_sources(self) -> None:
        output = self._promote("public-runtime-loader")
        report = verify_full_flow_local_semantic_runtime_package(output)
        self.assertEqual("VERIFIED_PUBLIC_LOCAL_RUNTIME_INPUTS", report["status"])
        self.assertFalse(report["n1_private_package_read"])
        self.assertFalse(report["source_binding_checked_offline"])
        loaded = load_full_flow_local_semantic_execution_inputs(output)
        self.assertEqual(64, len(loaded.bound_trials))
        self.assertGreater(len(loaded.bound_stages), 64)
        self.assertEqual(10, len(loaded.representative_smokes))
        self.assertFalse(
            loaded.admission["public_oracle_binding"][
                "n1_private_package_required_by_n7_n8_runtime"
            ]
        )
        mount = loaded.admission["n7_n8_runtime_mount_contract"]
        self.assertFalse(mount["task_plane_directory_required"])
        self.assertFalse(mount["n1_oracle_package_directory_required"])
        self.assertIn(
            "n1-private-oracle-package",
            mount["private_mount_classes_prohibited"],
        )

    def test_ten_smokes_cover_both_executors_without_claiming_execution(
        self,
    ) -> None:
        output = self._promote("smokes")
        smokes = _jsonl(output / SMOKES_NAME)
        self.assertEqual(
            {
                "n7-raw",
                "n7-indexed-raw",
                "n7-remote-derived",
                "n7-cache-miss",
                "n7-cache-hit",
                "n8-raw",
                "n8-indexed-raw",
                "n8-remote-derived",
                "n8-cache-miss",
                "n8-cache-hit",
            },
            {row["case_id"] for row in smokes},
        )
        self.assertTrue(all(
            row["flowmesh_submission_authorized"] is True
            and row["runtime_gate_state"] == "REQUIRED_NOT_EXECUTED"
            and row["semantic_execution_performed"] is False
            for row in smokes
        ))
        admission = json.loads((output / ADMISSION_NAME).read_text())
        gate = admission["representative_smoke_gate"]
        self.assertFalse(gate["full_matrix_runtime_gate_satisfied"])
        self.assertFalse(gate["full_matrix_submission_authorized"])

    def test_claims_remain_local_mcq_and_do_not_claim_remote_n1(self) -> None:
        output = self._promote("claim-boundary")
        admission = json.loads((output / ADMISSION_NAME).read_text())
        boundary = admission["claim_boundary"]
        self.assertEqual("multiple-choice-placeholder", boundary["w4_task_semantics"])
        for key in (
            "w4_retrieval_quality_evaluated",
            "performance_measured",
            "monetary_cost_measured",
            "scientific_claim_authorized",
            "upcloud_ready",
            "live_materialization_measured",
        ):
            self.assertFalse(boundary[key])
        self.assertTrue(boundary["remote_n1_verifier_implemented"])
        inventory = json.loads((output / INVENTORY_NAME).read_text())
        verifier = inventory["n1_score_verifier"]
        self.assertEqual(
            "remote-n1-authenticated-verification",
            verifier["mode"],
        )
        self.assertTrue(verifier["remote_n1_verifier_implemented"])
        self.assertTrue(
            verifier["hidden_oracle_isolation_suitable_for_multihost"]
        )

    def test_inventory_covers_every_legacy_gap_with_source_commitments(self) -> None:
        output = self._promote("inventory")
        gaps = json.loads(
            (self.legacy / "semantic-execution-runtime-gaps.json").read_text()
        )
        expected = {row["adapter_id"] for row in gaps["required_adapters"]}
        inventory = json.loads((output / INVENTORY_NAME).read_text())
        rows = inventory["adapters"]
        self.assertEqual(expected, {row["adapter_id"] for row in rows})
        self.assertTrue(all(
            row["implemented"] is True
            and len(row["implementation_source_sha256"]) == 64
            and row["requires_upcloud"] is False
            for row in rows
        ))

    def test_only_explicit_legacy_mcq_mode_can_be_promoted(self) -> None:
        output = self.root / "wrong-mode"
        arguments = self._arguments(output)
        arguments["semantics_mode"] = "retrieval-quality"
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError,
            "only legacy-mcq-local-conformance",
        ):
            promote_full_flow_local_semantic_execution_admission(**arguments)
        self.assertFalse(output.exists())

    def test_deterministic_output_and_no_absolute_source_paths(self) -> None:
        first = self._promote("deterministic-a")
        second = self._promote("deterministic-b")
        self.assertEqual(
            {path.name: path.read_bytes() for path in first.iterdir()},
            {path.name: path.read_bytes() for path in second.iterdir()},
        )
        combined = b"".join(path.read_bytes() for path in first.iterdir())
        text = combined.decode("utf-8")
        self.assertNotIn(str(self.root), text)
        self.assertNotIn(str(self.source_root), text)
        self.assertNotIn("correct_answer_id", text)

    def test_authorization_tamper_fails_even_if_checksums_are_rewritten(self) -> None:
        output = self._promote("tampered")
        path = output / TRIALS_NAME
        rows = _jsonl(path)
        rows[0]["flowmesh_submission_authorized"] = False
        path.write_bytes(b"".join(_canonical(row) + b"\n" for row in rows))
        checksum_rows = []
        content_names = (
            item.name
            for item in output.iterdir()
            if item.name != CHECKSUMS_NAME
        )
        for name in sorted(content_names):
            checksum_rows.append(
                f"{_sha256((output / name).read_bytes())}  {name}\n"
            )
        (output / CHECKSUMS_NAME).write_bytes(
            "".join(checksum_rows).encode("utf-8")
        )
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError,
            "authorization changed|output digests changed",
        ):
            self._verify(output)

    def test_missing_or_drifted_evidence_fails_before_publication(self) -> None:
        copied = self.root / "drifted-ranges"
        shutil.copytree(self.ranges, copied)
        path = copied / RANGE_CATALOG_NAME
        document = json.loads(path.read_text())
        document["entries"][0]["range_end"] -= 1
        _json(path, document)
        (copied / CHECKSUMS_NAME).write_text(
            f"{_sha256(path.read_bytes())}  {RANGE_CATALOG_NAME}\n",
            encoding="utf-8",
        )
        output = self.root / "drifted-evidence-output"
        arguments = self._arguments(output)
        arguments["exact_range_catalog_dir"] = copied
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError,
            "runtime evidence failed",
        ):
            promote_full_flow_local_semantic_execution_admission(**arguments)
        self.assertFalse(output.exists())

    def test_data_agent_package_drift_invalidates_preflight_promotion(
        self,
    ) -> None:
        copied = self.root / "drifted-n3-package"
        shutil.copytree(self.n3, copied)
        manifest_path = copied / "raw-cold-data-plane.json"
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
        document["package_id"] = "drifted-n3-package-v1"
        _json(manifest_path, document)
        names = sorted(
            path.relative_to(copied).as_posix()
            for path in copied.rglob("*")
            if path.is_file() and path.name != CHECKSUMS_NAME
        )
        (copied / CHECKSUMS_NAME).write_bytes(b"".join(
            f"{_sha256((copied / name).read_bytes())}  {name}\n".encode()
            for name in names
        ))
        output = self.root / "drifted-n3-promotion"
        arguments = self._arguments(output)
        arguments["n3_package_dir"] = copied
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError,
            "runtime evidence failed",
        ) as caught:
            promote_full_flow_local_semantic_execution_admission(**arguments)
        self.assertIsInstance(
            caught.exception.__cause__,
            FullFlowArtifactPreflightError,
        )
        self.assertIn(
            "package verification failed",
            str(caught.exception.__cause__),
        )
        self.assertFalse(output.exists())

    def test_extra_output_file_is_rejected(self) -> None:
        output = self._promote("extra-file")
        (output / "unexpected.txt").write_text("unexpected", encoding="utf-8")
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError,
            "file set changed",
        ):
            self._verify(output)


if __name__ == "__main__":
    unittest.main()
