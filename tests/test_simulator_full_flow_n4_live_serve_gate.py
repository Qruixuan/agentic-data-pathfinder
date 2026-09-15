from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from pathfinder.simulator.full_flow_artifact_bindings import (
    ARTIFACT_BINDINGS_NAME,
    build_full_flow_artifact_bindings,
)
from pathfinder.simulator.full_flow_deployment import (
    build_full_flow_deployment_binding,
)
from pathfinder.simulator.full_flow_live_provisioning_smoke import (
    DIGEST_RECEIPT_NAME,
    RECEIPT_NAME,
)
from pathfinder.simulator.full_flow_n4_live_serve_gate import (
    CHECKSUMS_NAME,
    GATE_NAME,
    FullFlowN4LiveServeGateError,
    freeze_full_flow_n4_live_serve_gate,
    verify_full_flow_n4_live_serve_gate,
)
from pathfinder.simulator.full_flow_semantic_execution_admission import (
    ADMISSION_NAME,
    freeze_full_flow_semantic_execution_admission,
)
from pathfinder.simulator.full_flow_semantic_matrix import (
    compile_full_flow_semantic_matrix,
)
from pathfinder.simulator.full_flow_tasks import ORACLE_PACKAGE, PUBLIC_TASK_SET
from pathfinder.simulator.n4_derived_data_plane import (
    FRAME_BUNDLE_REPRESENTATION_ID,
    GENERATIONS_DIRECTORY_NAME,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
    N4ArtifactProvenance,
    N4DerivedRepresentationStore,
)
from tests import test_simulator_full_flow_artifact_bindings as artifact_fixture
from tests import (
    test_simulator_full_flow_semantic_execution_admission as admission_fixture,
)


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


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(
        (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                indent=2,
            )
            + "\n"
        ).encode("utf-8")
    )


class FullFlowN4LiveServeGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        artifact_fixture.FullFlowArtifactBindingsTest.setUpClass()
        cls.fixture = artifact_fixture.FullFlowArtifactBindingsTest(
            "test_builds_source_verified_binding_set_for_all_four_objects"
        )
        source_bindings = cls.fixture.build("live-serve-required-source")
        source_binding_document = json.loads(
            (source_bindings / ARTIFACT_BINDINGS_NAME).read_text(
                encoding="utf-8"
            )
        )
        required_derived = {
            (item["artifact_object_id"], representation["representation_id"])
            for item in source_binding_document["objects"]
            for representation in item["representations"]
            if representation["representation_id"]
            in {
                FRAME_BUNDLE_REPRESENTATION_ID,
                MULTIMODAL_DIGEST_REPRESENTATION_ID,
            }
        }
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.store_root = cls.root / "n4-publications"
        cls.store = N4DerivedRepresentationStore(cls.store_root)
        cls.receipt_bindings: list[dict[str, object]] = []
        previous: str | None = None

        for index, source in enumerate(
            (
                item
                for item in artifact_fixture.FullFlowArtifactBindingsTest.
                derived_inputs
                if (item.object_id, item.representation_id)
                in required_derived
            ),
            start=1,
        ):
            source_sha256 = _sha256(
                f"source|{source.object_id}|{source.representation_id}".encode()
            )
            plan_sha256 = _sha256(
                f"plan|{source.object_id}|{source.representation_id}".encode()
            )
            publication_source_id = f"n5-live-source-{index:02d}"
            if source.representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
                derivation_id = "n5-uniform-midpoint-frame-bundle-v1"
                derivation_sha256 = _sha256(
                    f"transform|{source.object_id}".encode()
                )
            else:
                derivation_id = "n5-multimodal-digest-v1"
                derivation_sha256 = plan_sha256
            provenance = N4ArtifactProvenance(
                producer_node_id="N5",
                publication_source_id=publication_source_id,
                source_representation_id="raw_video",
                source_content_sha256=source_sha256,
                derivation_id=derivation_id,
                derivation_sha256=derivation_sha256,
            )
            artifact = replace(source, provenance=provenance)
            catalog_version = f"n4-live-catalog-{index:02d}"
            result = cls.store.publish(
                publication_id=f"n4-live-publication-{index:02d}",
                package_id=f"n4-live-package-{index:02d}",
                catalog_version=catalog_version,
                expected_current_catalog_version=previous,
                artifacts=[artifact],
            )
            previous = catalog_version
            receipt_root = cls.root / f"receipt-{index:02d}"
            receipt_root.mkdir()
            n4_receipt = result.receipt
            common = {
                "smoke_id": f"n5-n4-live-{index:02d}",
                "representation_id": source.representation_id,
                "source_representation_id": "raw_video",
                "object_id": source.object_id,
                "artifact_size_bytes": len(source.artifact_bytes),
                "artifact_sha256": _sha256(source.artifact_bytes),
                "n4_publication_id": n4_receipt["publication_id"],
                "n4_previous_catalog_version": n4_receipt[
                    "previous_catalog_version"
                ],
                "n4_committed_catalog_version": n4_receipt[
                    "committed_catalog_version"
                ],
                "n4_generation_id": n4_receipt["generation_id"],
                "n4_package_sha256": n4_receipt["package_sha256"],
                "n4_publication_receipt": n4_receipt,
                "n4_publication_idempotent_replay": False,
            }
            if source.representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
                plan = {
                    "plan_id": "D2",
                    "plan_sha256": plan_sha256,
                    "idempotency_key": publication_source_id,
                    "input": {
                        "object_id": source.object_id,
                        "sha256": source_sha256,
                    },
                }
                document = {
                    **common,
                    "n5_plan_id": "D2",
                    "n5_plan_sha256": plan_sha256,
                    "n5_transformation_contract_sha256": derivation_sha256,
                    "n5_materialization_idempotent_replay": False,
                }
                receipt_name = RECEIPT_NAME
                binding: dict[str, object] = {
                    "kind": "frame_bundle",
                    "receipt_dir": receipt_root,
                    "n5_plan": plan,
                }
            else:
                plan_root = cls.root / f"digest-plan-{index:02d}"
                plan_root.mkdir()
                source_video = cls.root / f"source-{index:02d}.mp4"
                source_video.write_bytes(f"source-{index:02d}".encode())
                document = {
                    **common,
                    "n5_digest_plan_id": "D2",
                    "n5_digest_plan_sha256": plan_sha256,
                    "n5_digest_result": {
                        "request_id": publication_source_id,
                        "source_handle": source_sha256,
                    },
                    "n5_digest_materialization_idempotent_replay": False,
                }
                receipt_name = DIGEST_RECEIPT_NAME
                binding = {
                    "kind": "multimodal_digest",
                    "receipt_dir": receipt_root,
                    "n5_digest_plan_dir": plan_root,
                    "source_video_path": source_video,
                }
            document["receipt_sha256"] = _sha256(_canonical(document))
            _write_json(receipt_root / receipt_name, document)
            (receipt_root / "SHA256SUMS").write_text(
                f"{_sha256((receipt_root / receipt_name).read_bytes())}  "
                f"{receipt_name}\n",
                encoding="utf-8",
            )
            cls.receipt_bindings.append(binding)

        snapshot = cls.store.current_snapshot()
        assert snapshot is not None
        cls.final_package = snapshot.package_dir
        cls.bindings = cls.root / "rebound-bindings"
        build_full_flow_artifact_bindings(
            artifact_fixture.FullFlowArtifactBindingsTest.logical,
            artifact_fixture.SCENARIO,
            artifact_fixture.FullFlowArtifactBindingsTest.container,
            artifact_fixture.FullFlowArtifactBindingsTest.task_plane,
            artifact_fixture.FullFlowArtifactBindingsTest.n3,
            cls.final_package,
            binding_set_id="n4-live-rebound-bindings-v1",
            output_dir=cls.bindings,
        )
        cls.semantic = cls.root / "rebound-semantic"
        compile_full_flow_semantic_matrix(
            artifact_fixture.FullFlowArtifactBindingsTest.logical,
            artifact_fixture.SCENARIO,
            artifact_fixture.FullFlowArtifactBindingsTest.container,
            artifact_fixture.FullFlowArtifactBindingsTest.task_plane
            / PUBLIC_TASK_SET,
            cls.bindings / ARTIFACT_BINDINGS_NAME,
            compiler_id="n4-live-rebound-semantic-v1",
            output_dir=cls.semantic,
        )
        logical_catalog = json.loads(
            (
                artifact_fixture.FullFlowArtifactBindingsTest.logical
                / "logical-service-contracts.json"
            ).read_text(encoding="utf-8")
        )
        deployment_source = cls.root / "deployment-source.json"
        _write_json(
            deployment_source,
            admission_fixture.FullFlowSemanticExecutionAdmissionTest.
            _deployment_source(logical_catalog),
        )
        cls.deployment = cls.root / "deployment"
        build_full_flow_deployment_binding(
            artifact_fixture.FullFlowArtifactBindingsTest.logical,
            artifact_fixture.SCENARIO,
            artifact_fixture.FullFlowArtifactBindingsTest.container,
            deployment_source,
            output_dir=cls.deployment,
        )
        cls.admission = cls.root / "rebound-admission"
        freeze_full_flow_semantic_execution_admission(
            cls.semantic,
            cls.deployment,
            artifact_fixture.FullFlowArtifactBindingsTest.logical,
            artifact_fixture.SCENARIO,
            artifact_fixture.FullFlowArtifactBindingsTest.container,
            artifact_fixture.FullFlowArtifactBindingsTest.task_plane
            / PUBLIC_TASK_SET,
            cls.bindings / ARTIFACT_BINDINGS_NAME,
            artifact_fixture.FullFlowArtifactBindingsTest.task_plane
            / ORACLE_PACKAGE,
            worker_alias="pathfinder-semantic-worker",
            admission_id="n4-live-rebound-admission-v1",
            output_dir=cls.admission,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()
        artifact_fixture.FullFlowArtifactBindingsTest.tearDownClass()

    def setUp(self) -> None:
        self.case_root = Path(tempfile.mkdtemp(dir=self.root))

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root)

    def _arguments(self) -> dict[str, object]:
        return {
            "live_receipt_bindings": list(self.receipt_bindings),
            "n4_publication_store_root": self.store_root,
            "rebound_artifact_binding_dir": self.bindings,
            "rebound_semantic_matrix_dir": self.semantic,
            "rebound_admission_dir": self.admission,
        }

    def _patch_verifiers(self):
        frame = patch(
            "pathfinder.simulator.full_flow_n4_live_serve_gate."
            "verify_n5_n4_live_frame_bundle_provisioning_smoke",
            return_value={"status": "VERIFIED"},
        )
        digest = patch(
            "pathfinder.simulator.full_flow_n4_live_serve_gate."
            "verify_n5_n4_live_multimodal_digest_provisioning_smoke",
            return_value={"status": "VERIFIED"},
        )
        return frame, digest

    def _freeze(self, name: str, **changes: object) -> Path:
        arguments = self._arguments()
        arguments.update(changes)
        output = self.case_root / name
        frame, digest = self._patch_verifiers()
        with frame, digest:
            freeze_full_flow_n4_live_serve_gate(
                **arguments,
                gate_id="n4-live-rebound-serve-v1",
                output_dir=output,
            )
        return output

    def _verify(self, output: Path, **changes: object) -> dict:
        arguments = self._arguments()
        arguments.update(changes)
        frame, digest = self._patch_verifiers()
        with frame, digest:
            return verify_full_flow_n4_live_serve_gate(output, **arguments)

    def test_freezes_complete_live_chain_and_exact_rebound_inputs(self) -> None:
        before = {
            path.relative_to(self.store_root).as_posix(): path.read_bytes()
            for path in self.store_root.rglob("*")
            if path.is_file()
        }
        output = self._freeze("complete")
        report = self._verify(output)
        after = {
            path.relative_to(self.store_root).as_posix(): path.read_bytes()
            for path in self.store_root.rglob("*")
            if path.is_file()
        }
        gate = json.loads((output / GATE_NAME).read_text(encoding="utf-8"))

        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(6, report["live_receipt_count"])
        self.assertEqual(6, report["required_derived_identity_count"])
        self.assertTrue(report["live_n5_materialization_executed"])
        self.assertEqual("serve-frozen", report["authorized_compose_profile"])
        self.assertTrue(report["publication_companion_excluded"])
        self.assertTrue(report["n4_data_agent_rebind_inputs_verified"])
        self.assertFalse(report["n4_data_agent_runtime_rebind_executed"])
        self.assertEqual(before, after)
        self.assertEqual(3, gate["frame_bundle_receipt_count"])
        self.assertEqual(3, gate["multimodal_digest_receipt_count"])
        self.assertFalse(gate["publication_mutation_during_trials_allowed"])
        self.assertFalse(gate["upcloud_ready"])
        self.assertFalse(gate["performance_measured"])
        self.assertFalse(gate["eligible_for_scientific_claims"])

    def test_missing_receipt_fails_exact_coverage(self) -> None:
        with self.assertRaises(FullFlowN4LiveServeGateError):
            self._freeze(
                "missing-receipt",
                live_receipt_bindings=self.receipt_bindings[:-1],
            )

    def test_noncontiguous_or_reordered_chain_fails_closed(self) -> None:
        reordered = list(self.receipt_bindings)
        reordered[0], reordered[1] = reordered[1], reordered[0]
        with self.assertRaisesRegex(
            FullFlowN4LiveServeGateError,
            "begin from an empty|not contiguous",
        ):
            self._freeze("reordered", live_receipt_bindings=reordered)

    def test_durable_receipt_replay_mismatch_is_rejected(self) -> None:
        copied = self.case_root / "mismatched-store"
        shutil.copytree(self.store_root, copied)
        publication_id = json.loads(
            (
                Path(self.receipt_bindings[0]["receipt_dir"])
                / (
                    RECEIPT_NAME
                    if self.receipt_bindings[0]["kind"] == "frame_bundle"
                    else DIGEST_RECEIPT_NAME
                )
            ).read_text(encoding="utf-8")
        )["n4_publication_id"]
        with closing(
            sqlite3.connect(copied / "n4-publications.sqlite3")
        ) as connection:
            connection.execute(
                "UPDATE n4_publications SET request_sha256 = ? "
                "WHERE publication_id = ?",
                ("f" * 64, publication_id),
            )
            connection.commit()
        with self.assertRaisesRegex(
            FullFlowN4LiveServeGateError,
            "replay does not match",
        ):
            self._freeze(
                "receipt-mismatch",
                n4_publication_store_root=copied,
            )

    def test_stale_artifact_binding_package_is_rejected(self) -> None:
        stale = self.fixture.build("stale-live-serve-bindings")
        with self.assertRaisesRegex(
            FullFlowN4LiveServeGateError,
            "do not bind the final N4 generation",
        ):
            self._freeze(
                "stale-bindings",
                rebound_artifact_binding_dir=stale,
            )

    def test_rebound_admission_drift_fails_even_when_restamped(self) -> None:
        drifted = self.case_root / "drifted-admission"
        shutil.copytree(self.admission, drifted)
        admission_path = drifted / ADMISSION_NAME
        value = json.loads(admission_path.read_text(encoding="utf-8"))
        value["source_bindings"]["semantic_matrix_plan_sha256"] = "f" * 64
        value["source_binding_sha256"] = _sha256(
            _canonical(value["source_bindings"])
        )
        unsigned = dict(value)
        unsigned.pop("admission_sha256")
        value["admission_sha256"] = _sha256(_canonical(unsigned))
        _write_json(admission_path, value)
        content_names = sorted(
            path.name
            for path in drifted.iterdir()
            if path.name != CHECKSUMS_NAME
        )
        (drifted / CHECKSUMS_NAME).write_bytes(b"".join(
            f"{_sha256((drifted / name).read_bytes())}  {name}\n".encode()
            for name in content_names
        ))
        with self.assertRaisesRegex(
            FullFlowN4LiveServeGateError,
            "does not bind the rebound semantic matrix",
        ):
            self._freeze(
                "admission-drift",
                rebound_admission_dir=drifted,
            )

    def test_gate_is_source_bound_after_outer_rehash(self) -> None:
        output = self._freeze("tamper")
        value = json.loads((output / GATE_NAME).read_text(encoding="utf-8"))
        value["source_commitments"]["n4_catalog_version"] = "stale-catalog"
        unsigned = dict(value)
        unsigned.pop("gate_sha256")
        value["gate_sha256"] = _sha256(_canonical(unsigned))
        _write_json(output / GATE_NAME, value)
        (output / CHECKSUMS_NAME).write_text(
            f"{_sha256((output / GATE_NAME).read_bytes())}  {GATE_NAME}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            FullFlowN4LiveServeGateError,
            "does not match its current frozen sources",
        ):
            self._verify(output)


if __name__ == "__main__":
    unittest.main()
