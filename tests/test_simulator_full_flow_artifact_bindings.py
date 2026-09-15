from __future__ import annotations

import base64
import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

from pathfinder.frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    deterministic_frame_bundle_tar,
)
from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.data_agent_semantic_vertical import (
    DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
)
from pathfinder.simulator.full_flow_artifact_bindings import (
    ARTIFACT_BINDINGS_NAME,
    CHECKSUMS_NAME,
    PROVENANCE_NAME,
    FullFlowArtifactBindingError,
    build_full_flow_artifact_bindings,
    verify_full_flow_artifact_bindings,
)
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_provisioning_catalog import (
    CATALOG_NAME as PROVISIONING_CATALOG_NAME,
    FrozenProvisioningCatalog,
    FullFlowProvisioningCatalogError,
    build_full_flow_provisioning_catalog,
    verify_full_flow_provisioning_catalog,
)
from pathfinder.simulator.full_flow_semantic_matrix import (
    compile_full_flow_semantic_matrix,
)
from pathfinder.simulator.full_flow_tasks import build_full_flow_task_plane
from pathfinder.simulator.n4_derived_data_plane import (
    FRAME_BUNDLE_REPRESENTATION_ID,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    build_n4_derived_data_package,
)
from pathfinder.simulator.portable import build_portable_execution_plan
from pathfinder.simulator.raw_cold_data_plane import (
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)
WORKLOADS = {
    "smoke-descriptive": ("video-descriptive", "nextqa-val-1000000001"),
    "smoke-temporal": ("video-temporal", "nextqa-val-1000000002"),
    "smoke-causal": ("video-causal", "nextqa-val-1000000003"),
    "smoke-retrieval": ("video-retrieval-target", "nextqa-val-1000000004"),
}

_JPEG = base64.b64decode(
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBAUEBAYFBQUGBgYHCQ4JCQgICRINDQoOFRIW"
    "FhUSFBQXGiEcFxgfGRQUHScdHyIjJSUlFhwpLCgkKyEkJST/2wBDAQYGBgkICREJCREkGBQY"
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/"
    "wAARCAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAA"
    "AAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA"
    "/9oADAMBAAIRAxEAPwCdAAyqX//Z",
    validate=True,
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _mp4(seed: int) -> bytes:
    compatible = b"isom" + b"iso2" + b"mp41"
    payload = b"isom" + struct.pack(">I", 512) + compatible
    ftyp = struct.pack(">I", len(payload) + 8) + b"ftyp" + payload
    body = bytes((seed + index) % 256 for index in range(128))
    return ftyp + struct.pack(">I", len(body) + 8) + b"mdat" + body


def _bundle(object_id: str) -> bytes:
    frame = _JPEG
    video_id = object_id.rsplit("-", 1)[-1]
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
        "object_id": object_id,
        "source_video_id": video_id,
        "source_video_filename": f"{video_id}.mp4",
        "source_video_size_bytes": 256,
        "source_video_sha256": "a" * 64,
        "source_duration_seconds": 10.0,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": 1,
            "jpeg_max_dimension": 768,
            "jpeg_quality": 82,
            "jpeg_optimize": True,
        },
        "source_frame_descriptions": {
            "representation_id": "sampled_frames",
            "path": f"{object_id}/sampled_frames.json",
            "sha256": "b" * 64,
        },
        "generation_manifest_sha256": "c" * 64,
        "frames": [{
            "frame_index": 0,
            "timestamp_seconds": 0.5,
            "width": 2,
            "height": 2,
            "path": "frames/000.jpg",
            "jpeg_size_bytes": len(frame),
            "jpeg_sha256": _sha256(frame),
        }],
        "frame_count": 1,
        "total_jpeg_bytes": len(frame),
        "software_versions": {"av": "17.0.1", "Pillow": "12.3.0"},
        "historical_visual_bytes_retained": False,
        "sampling_alignment_statement": (
            "These JPEG frames were regenerated from the same source video "
            "using the same sampling algorithm and are aligned with the "
            "frozen sampling metadata. The historical visual bytes were not "
            "retained, so this artifact does not claim byte identity with "
            "the historical visual input."
        ),
        "claims_byte_identity_with_historical_visual_input": False,
        "credentials_recorded": False,
        "llm_called": False,
        "network_calls_performed": False,
    }
    manifest_bytes = (
        json.dumps(manifest, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    return deterministic_frame_bundle_tar([
        (OBJECT_MANIFEST_NAME, manifest_bytes),
        ("frames/000.jpg", frame),
    ])


def _semantic_spec(workload_id: str, object_id: str, index: int) -> dict:
    placeholder = f"artifact-{index}".encode()
    return {
        "schema_version": DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
        "semantic_run_id": "full-flow-binding-source-v1",
        "trial_key": f"scenario|{workload_id}|D2|r0000",
        "semantic_executor_node_id": "N6",
        "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
        "data_agent_route_design_id": "D2",
        "data_agent_plan_id": "D2",
        "data_agent_plan_epoch": 1,
        "workload_id": workload_id,
        "task_class_id": "video_qa",
        "artifact_object_id": object_id,
        "artifact_sha256": _sha256(placeholder),
        "artifact_size_bytes": len(placeholder),
        "object_catalog_version": "source-catalog-v1",
        "question": f"Which option answers {workload_id}?",
        "success_scoring_rule": (
            "multiple-choice-option-id-canonical-match-v1"
        ),
        "answer_options": [
            {"option_id": "A", "text": "First option."},
            {"option_id": "B", "text": "Second option."},
        ],
        "correct_answer_id": "A" if index % 2 else "B",
        "expected_model": "vision-model-test",
        "credentials_recorded": False,
    }


class FullFlowArtifactBindingsTest(unittest.TestCase):
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

        specs = []
        for index, (workload_id, (_, object_id)) in enumerate(
            sorted(WORKLOADS.items()), start=1
        ):
            specs.append(
                _json(
                    cls.root / f"spec-{index}.json",
                    _semantic_spec(workload_id, object_id, index),
                )
            )
        cls.task_plane = cls.root / "task-plane"
        build_full_flow_task_plane(
            specs,
            task_plane_id="full-flow-binding-task-plane-v1",
            oracle_id="full-flow-binding-oracle-v1",
            output_dir=cls.task_plane,
        )

        raw_bindings = []
        for index, (_, (_, object_id)) in enumerate(
            sorted(WORKLOADS.items()), start=1
        ):
            raw = _mp4(index)
            path = cls.root / f"raw-{index}.mp4"
            path.write_bytes(raw)
            raw_bindings.append(RawColdObjectBinding(
                object_id=object_id,
                artifact_path=path,
                catalog_version="n3-real-catalog-v1",
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
            package_id="n3-real-artifacts-v1",
        )

        derived = []
        provenance = lambda representation_id, object_id: N4ArtifactProvenance(
            producer_node_id="N5",
            publication_source_id=f"n5-{representation_id}-{object_id}",
            source_representation_id="raw_video",
            source_content_sha256="d" * 64,
            derivation_id=f"derive-{representation_id}-v1",
            derivation_sha256="e" * 64,
        )
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
                    provenance=provenance(representation_id, object_id),
                    expected_sha256=_sha256(raw),
                    expected_size_bytes=len(raw),
                ))
        cls.n4 = cls.root / "n4"
        cls.derived_inputs = tuple(derived)
        build_n4_derived_data_package(
            derived,
            output_dir=cls.n4,
            package_id="n4-real-artifacts-v1",
            catalog_version="n4-real-catalog-v1",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def build(self, name: str) -> Path:
        output = self.root / name
        build_full_flow_artifact_bindings(
            self.logical,
            SCENARIO,
            self.container,
            self.task_plane,
            self.n3,
            self.n4,
            binding_set_id="verified-real-artifacts-v1",
            output_dir=output,
        )
        return output

    def test_builds_source_verified_binding_set_for_all_four_objects(self) -> None:
        output = self.build("bindings")
        verified = verify_full_flow_artifact_bindings(
            output,
            self.logical,
            SCENARIO,
            self.container,
            self.task_plane,
            self.n3,
            self.n4,
        )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(4, verified["artifact_object_count"])
        self.assertEqual(10, verified["artifact_representation_count"])
        self.assertFalse(verified["artifacts_copied"])

        bindings = json.loads((output / ARTIFACT_BINDINGS_NAME).read_text())
        self.assertEqual(4, len(bindings["objects"]))
        self.assertEqual(
            {
                "multimodal_digest": 3,
                "raw_video": 4,
                "sampled_frame_bundle": 3,
            },
            {
                representation_id: sum(
                    row["representation_id"] == representation_id
                    for item in bindings["objects"]
                    for row in item["representations"]
                )
                for representation_id in (
                    "multimodal_digest",
                    "raw_video",
                    "sampled_frame_bundle",
                )
            },
        )

    def test_binding_output_is_accepted_by_semantic_matrix_compiler(self) -> None:
        bindings = self.build("compiler-bindings")
        semantic = self.root / "semantic"
        result = compile_full_flow_semantic_matrix(
            self.logical,
            SCENARIO,
            self.container,
            self.task_plane / "public/public-tasks.json",
            bindings / ARTIFACT_BINDINGS_NAME,
            output_dir=semantic,
        )
        self.assertEqual(64, result["trial_count"])

    def test_output_has_only_public_hash_metadata_and_no_artifact_bytes(self) -> None:
        output = self.build("safe")
        self.assertEqual(
            {ARTIFACT_BINDINGS_NAME, PROVENANCE_NAME, CHECKSUMS_NAME},
            {path.name for path in output.iterdir()},
        )
        combined = b"".join(path.read_bytes() for path in output.iterdir())
        lowered = combined.decode("utf-8").lower()
        self.assertNotIn("correct_answer_id", lowered)
        self.assertNotIn("hidden_labels_sha256", lowered)
        self.assertNotIn("oracle_package_manifest_sha256", lowered)
        self.assertNotIn("task_plane_checksums_sha256", lowered)
        self.assertNotIn("artifact_package_path", lowered)
        self.assertNotIn("http://", lowered)
        self.assertNotIn("api_key", lowered)

    def test_missing_required_derived_representation_fails_closed(self) -> None:
        missing_n4 = self.root / "n4-missing-required"
        build_n4_derived_data_package(
            [
                row
                for row in self.derived_inputs
                if not (
                    row.object_id == "nextqa-val-1000000004"
                    and row.representation_id
                    == FRAME_BUNDLE_REPRESENTATION_ID
                )
            ],
            output_dir=missing_n4,
            package_id="n4-missing-required-v1",
            catalog_version="n4-real-catalog-v1",
        )
        with self.assertRaisesRegex(
            FullFlowArtifactBindingError,
            "missing required artifact",
        ):
            build_full_flow_artifact_bindings(
                self.logical,
                SCENARIO,
                self.container,
                self.task_plane,
                self.n3,
                missing_n4,
                binding_set_id="missing-derived-v1",
                output_dir=self.root / "missing",
            )

    def test_data_agent_plan_bindings_must_cover_all_matrix_designs(self) -> None:
        narrow_n4 = self.root / "n4-narrow-plans"
        narrowed = [
            N4DerivedArtifactInput(
                object_id=row.object_id,
                representation_id=row.representation_id,
                artifact_bytes=row.artifact_bytes,
                plan_ids=("D2",),
                provenance=row.provenance,
                expected_sha256=row.expected_sha256,
                expected_size_bytes=row.expected_size_bytes,
            )
            for row in self.derived_inputs
        ]
        build_n4_derived_data_package(
            narrowed,
            output_dir=narrow_n4,
            package_id="n4-narrow-plans-v1",
            catalog_version="n4-real-catalog-v1",
        )
        with self.assertRaisesRegex(
            FullFlowArtifactBindingError,
            "does not expose required artifact",
        ):
            build_full_flow_artifact_bindings(
                self.logical,
                SCENARIO,
                self.container,
                self.task_plane,
                self.n3,
                narrow_n4,
                binding_set_id="narrow-plans-v1",
                output_dir=self.root / "narrow-bindings",
            )

    def test_output_is_deterministic_and_rejects_extra_directory(self) -> None:
        first = self.build("deterministic-a")
        second = self.build("deterministic-b")
        for name in (ARTIFACT_BINDINGS_NAME, PROVENANCE_NAME, CHECKSUMS_NAME):
            self.assertEqual(
                (first / name).read_bytes(),
                (second / name).read_bytes(),
            )
        (second / "unexpected").mkdir()
        with self.assertRaisesRegex(
            FullFlowArtifactBindingError,
            "regular files only",
        ):
            verify_full_flow_artifact_bindings(
                second,
                self.logical,
                SCENARIO,
                self.container,
                self.task_plane,
                self.n3,
                self.n4,
            )

    def test_output_cannot_be_nested_in_a_verified_source_package(self) -> None:
        with self.assertRaisesRegex(
            FullFlowArtifactBindingError,
            "must not be inside",
        ):
            build_full_flow_artifact_bindings(
                self.logical,
                SCENARIO,
                self.container,
                self.task_plane,
                self.n3,
                self.n4,
                binding_set_id="nested-output-v1",
                output_dir=self.task_plane / "artifact-bindings",
            )

    def test_preprovisioned_catalog_binds_n5_provenance_to_n4_bytes(self) -> None:
        bindings = self.build("provisioning-source-bindings")
        output = self.root / "preprovisioned-catalog"
        report = build_full_flow_provisioning_catalog(
            bindings,
            self.n4,
            catalog_id="preprovisioned-derived-v1",
            output_dir=output,
        )
        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(6, report["entry_count"])
        self.assertFalse(report["live_materialization_executed"])
        self.assertFalse(report["live_materialization_cost_measured"])

        loaded = FrozenProvisioningCatalog(
            output,
            artifact_binding_dir=bindings,
            n4_package_dir=self.n4,
        )
        self.assertEqual(6, len(loaded.references))
        self.assertTrue(all(row.available for row in loaded.references))
        self.assertEqual(
            {
                "multimodal_digest",
                "sampled_frame_bundle",
            },
            {
                row.artifact_identity.representation_id
                for row in loaded.references
            },
        )

    def test_preprovisioned_catalog_is_deterministic_and_tamper_evident(self) -> None:
        bindings = self.build("provisioning-deterministic-bindings")
        first = self.root / "preprovisioned-deterministic-a"
        second = self.root / "preprovisioned-deterministic-b"
        for output in (first, second):
            build_full_flow_provisioning_catalog(
                bindings,
                self.n4,
                catalog_id="preprovisioned-derived-deterministic-v1",
                output_dir=output,
            )
        self.assertEqual(
            (first / PROVISIONING_CATALOG_NAME).read_bytes(),
            (second / PROVISIONING_CATALOG_NAME).read_bytes(),
        )
        document = json.loads(
            (second / PROVISIONING_CATALOG_NAME).read_text(encoding="utf-8")
        )
        document["entries"][0]["live_materialization_executed"] = True
        _json(second / PROVISIONING_CATALOG_NAME, document)
        with self.assertRaisesRegex(
            FullFlowProvisioningCatalogError,
            "catalog digest|checksums",
        ):
            verify_full_flow_provisioning_catalog(
                second,
                artifact_binding_dir=bindings,
                n4_package_dir=self.n4,
            )


if __name__ == "__main__":
    unittest.main()
