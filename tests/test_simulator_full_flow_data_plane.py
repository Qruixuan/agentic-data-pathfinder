"""Offline tests for the portable full-flow simulator data plane."""

from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.data_agent_manifest import load_data_agent_manifest
from pathfinder.frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    deterministic_frame_bundle_tar,
)
from pathfinder.simulator.data_agent_semantic_vertical import (
    DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
)
from pathfinder.simulator.full_flow_data_plane import (
    CHECKSUMS_NAME,
    DATA_AGENT_MANIFEST_PATH,
    EXECUTOR_NODE_ID,
    INFERENCE_NODE_ID,
    OBJECT_CATALOG_PATH,
    PACKAGE_MANIFEST_NAME,
    SOURCE_NODE_ID,
    FullFlowArtifactBinding,
    FullFlowDataPlaneError,
    artifact_binding_from_semantic_spec,
    build_full_flow_data_plane_package,
    build_full_flow_data_plane_package_from_semantic_specs,
    verify_full_flow_data_plane_package,
)


REPRESENTATION_ID = "sampled_frame_bundle"
CATALOG_VERSION = "full-flow-test-catalog-v1"
OBJECT_A = "nextqa-val-0000000001"
OBJECT_B = "nextqa-val-0000000002"

_TEST_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBAUEBAYFBQUGBgYHCQ4JCQgICRINDQoOFRIW"
    "FhUSFBQXGiEcFxgfGRQUHScdHyIjJSUlFhwpLCgkKyEkJST/2wBDAQYGBgkICREJCREkGBQY"
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/"
    "wAARCAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAA"
    "AAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA"
    "/9oADAMBAAIRAxEAPwCdAAyqX//Z"
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _jpeg(marker: int) -> bytes:
    payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
    filler = bytes((marker, marker + 1))
    comment = b"\xff\xfe" + (len(filler) + 2).to_bytes(2, "big") + filler
    return payload[:2] + comment + payload[2:]


def _bundle_bytes(object_id: str, marker: int) -> bytes:
    frames = [_jpeg(marker), _jpeg(marker + 2)]
    frame_rows = [
        {
            "frame_index": index,
            "timestamp_seconds": 0.5 + index,
            "width": 2,
            "height": 2,
            "path": f"frames/{index:03d}.jpg",
            "jpeg_size_bytes": len(frame),
            "jpeg_sha256": _sha256(frame),
        }
        for index, frame in enumerate(frames)
    ]
    video_id = object_id.rsplit("-", 1)[-1]
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": REPRESENTATION_ID,
        "object_id": object_id,
        "source_video_id": video_id,
        "source_video_filename": f"{video_id}.mp4",
        "source_video_size_bytes": 123456,
        "source_video_sha256": "a" * 64,
        "source_duration_seconds": 42.5,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": len(frames),
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
        "frames": frame_rows,
        "frame_count": len(frames),
        "total_jpeg_bytes": sum(len(frame) for frame in frames),
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
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return deterministic_frame_bundle_tar(
        [(OBJECT_MANIFEST_NAME, manifest_bytes)]
        + [
            (f"frames/{index:03d}.jpg", frame)
            for index, frame in enumerate(frames)
        ]
    )


def _semantic_spec(
    object_id: str,
    artifact: bytes,
    *,
    semantic_run_id: str = "semantic-run-test-v1",
    plan_id: str = "D-origin-warm",
    question: str = "Which option describes the main action?",
) -> dict[str, object]:
    return {
        "schema_version": DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
        "semantic_run_id": semantic_run_id,
        "trial_key": f"scenario|smoke-descriptive|{plan_id}|r0000",
        "semantic_executor_node_id": "N6",
        "representation_id": REPRESENTATION_ID,
        "data_agent_route_design_id": plan_id,
        "data_agent_plan_id": plan_id,
        "data_agent_plan_epoch": 1,
        "workload_id": "smoke-descriptive",
        "task_class_id": "video_qa",
        "artifact_object_id": object_id,
        "artifact_sha256": _sha256(artifact),
        "artifact_size_bytes": len(artifact),
        "object_catalog_version": CATALOG_VERSION,
        "question": question,
        "success_scoring_rule": (
            "multiple-choice-option-id-canonical-match-v1"
        ),
        "answer_options": [
            {"option_id": "A", "text": "A vehicle crosses a river."},
            {"option_id": "B", "text": "Two musicians perform."},
        ],
        "correct_answer_id": "B",
        "expected_model": "vision-model-test",
        "credentials_recorded": False,
    }


def _write_json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _restamp(root: Path) -> None:
    paths = sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name != CHECKSUMS_NAME
    )
    (root / CHECKSUMS_NAME).write_text(
        "".join(
            f"{_sha256((root / path).read_bytes())}  {path}\n"
            for path in paths
        ),
        encoding="utf-8",
    )


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


class FullFlowDataPlaneTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.raw_a = _bundle_bytes(OBJECT_A, 11)
        self.raw_b = _bundle_bytes(OBJECT_B, 21)
        self.artifact_a = self.root / "host-a.tar"
        self.artifact_b = self.root / "host-b.tar"
        self.artifact_a.write_bytes(self.raw_a)
        self.artifact_b.write_bytes(self.raw_b)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _binding(
        self,
        object_id: str = OBJECT_A,
        artifact_path: Path | None = None,
    ) -> FullFlowArtifactBinding:
        artifact = artifact_path or self.artifact_a
        raw = artifact.read_bytes()
        return FullFlowArtifactBinding(
            object_id=object_id,
            artifact_path=artifact,
            catalog_version=CATALOG_VERSION,
            plan_ids=("D-origin-warm",),
            artifact_sha256=_sha256(raw),
            artifact_size_bytes=len(raw),
        )

    def test_builds_portable_n4_n7_n6_package(self) -> None:
        output = self.root / "package"
        result = build_full_flow_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="full-flow-test-v1",
        )

        self.assertEqual("VERIFIED_FULL_FLOW_DATA_PLANE", result["status"])
        self.assertEqual(SOURCE_NODE_ID, result["source_node_id"])
        self.assertEqual(EXECUTOR_NODE_ID, result["executor_node_id"])
        self.assertEqual(INFERENCE_NODE_ID, result["inference_node_id"])
        report = json.loads((output / PACKAGE_MANIFEST_NAME).read_text())
        self.assertEqual("/data", report["route"]["package_mount_path"])
        self.assertEqual("N4", report["route"]["source_node_id"])
        self.assertEqual("N7", report["route"]["executor_node_id"])
        self.assertEqual("N6", report["route"]["inference_node_id"])
        self.assertNotIn(str(self.root), (output / PACKAGE_MANIFEST_NAME).read_text())
        self.assertNotIn("://", (output / PACKAGE_MANIFEST_NAME).read_text())

        manifest = load_data_agent_manifest(output / DATA_AGENT_MANIFEST_PATH)
        resolved = manifest.resolve(
            plan_id="D-origin-warm",
            object_id=OBJECT_A,
            representation_id=REPRESENTATION_ID,
            requested_location="origin-warm",
        )
        self.assertEqual(
            output / "artifacts" / OBJECT_A / "sampled_frame_bundle.tar",
            resolved.path,
        )
        self.assertEqual(self.raw_a, resolved.path.read_bytes())

    def test_semantic_spec_binding_is_copied_canonically(self) -> None:
        spec_path = _write_json(
            self.root / "spec.json",
            _semantic_spec(OBJECT_A, self.raw_a),
        )
        binding = artifact_binding_from_semantic_spec(
            spec_path, self.artifact_a
        )
        output = self.root / "package"
        result = build_full_flow_data_plane_package(
            [binding], output_dir=output, package_id="spec-package-v1"
        )

        self.assertEqual(1, result["semantic_spec_count"])
        report = json.loads((output / PACKAGE_MANIFEST_NAME).read_text())
        packaged = report["objects"][0]["semantic_specs"][0]
        self.assertEqual("D-origin-warm", packaged["data_agent_plan_id"])
        self.assertEqual(
            packaged["sha256"],
            _sha256((output / packaged["package_path"]).read_bytes()),
        )

    def test_multiple_specs_for_one_artifact_are_merged(self) -> None:
        spec_a = _write_json(
            self.root / "spec-a.json",
            _semantic_spec(OBJECT_A, self.raw_a, semantic_run_id="run-a"),
        )
        spec_b = _write_json(
            self.root / "spec-b.json",
            _semantic_spec(
                OBJECT_A,
                self.raw_a,
                semantic_run_id="run-b",
                plan_id="D-derived-warm",
            ),
        )
        output = self.root / "package"
        result = build_full_flow_data_plane_package_from_semantic_specs(
            [(spec_b, self.artifact_a), (spec_a, self.artifact_a)],
            output_dir=output,
            package_id="merged-package-v1",
        )

        self.assertEqual(1, result["object_count"])
        self.assertEqual(2, result["semantic_spec_count"])
        report = json.loads((output / PACKAGE_MANIFEST_NAME).read_text())
        self.assertEqual(
            ["D-derived-warm", "D-origin-warm"],
            report["objects"][0]["plan_ids"],
        )

    def test_two_artifacts_and_checksum_order_are_deterministic(self) -> None:
        bindings = [
            self._binding(),
            self._binding(OBJECT_B, self.artifact_b),
        ]
        first = self.root / "first"
        second = self.root / "second"
        build_full_flow_data_plane_package(
            list(reversed(bindings)),
            output_dir=first,
            package_id="deterministic-package-v1",
        )
        build_full_flow_data_plane_package(
            bindings,
            output_dir=second,
            package_id="deterministic-package-v1",
        )

        self.assertEqual(_tree_bytes(first), _tree_bytes(second))
        checksum_paths = [
            line.split("  ", 1)[1]
            for line in (first / CHECKSUMS_NAME).read_text().splitlines()
        ]
        self.assertEqual(sorted(checksum_paths), checksum_paths)

    def test_wrong_expected_artifact_digest_is_rejected_without_output(self) -> None:
        binding = self._binding()
        binding = FullFlowArtifactBinding(
            **{**binding.__dict__, "artifact_sha256": "f" * 64}
        )
        output = self.root / "package"
        with self.assertRaisesRegex(FullFlowDataPlaneError, "SHA-256 mismatch"):
            build_full_flow_data_plane_package(
                [binding], output_dir=output, package_id="wrong-digest-v1"
            )
        self.assertFalse(output.exists())

    def test_noncanonical_artifact_is_rejected(self) -> None:
        bad = self.root / "bad.tar"
        bad.write_bytes(self.raw_a + b"trailing bytes")
        binding = FullFlowArtifactBinding(
            object_id=OBJECT_A,
            artifact_path=bad,
            catalog_version=CATALOG_VERSION,
            plan_ids=("D-origin-warm",),
        )
        with self.assertRaisesRegex(FullFlowDataPlaneError, "not a canonical"):
            build_full_flow_data_plane_package(
                [binding],
                output_dir=self.root / "package",
                package_id="bad-bundle-v1",
            )

    def test_url_in_semantic_metadata_is_rejected(self) -> None:
        spec = _write_json(
            self.root / "spec.json",
            _semantic_spec(
                OBJECT_A,
                self.raw_a,
                question="Inspect https://host.invalid/image and answer.",
            ),
        )
        with self.assertRaisesRegex(FullFlowDataPlaneError, "contains a URL"):
            artifact_binding_from_semantic_spec(spec, self.artifact_a)

    def test_existing_output_is_never_overwritten(self) -> None:
        output = self.root / "package"
        output.mkdir()
        sentinel = output / "sentinel"
        sentinel.write_text("preserve", encoding="utf-8")
        with self.assertRaisesRegex(FullFlowDataPlaneError, "already exists"):
            build_full_flow_data_plane_package(
                [self._binding()],
                output_dir=output,
                package_id="no-overwrite-v1",
            )
        self.assertEqual("preserve", sentinel.read_text(encoding="utf-8"))

    def test_catalog_path_escape_is_rejected_after_restamp(self) -> None:
        output = self.root / "package"
        build_full_flow_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="tamper-catalog-v1",
        )
        catalog_path = output / OBJECT_CATALOG_PATH
        catalog = json.loads(catalog_path.read_text())
        catalog["objects"][OBJECT_A]["representations"][
            REPRESENTATION_ID
        ]["path"] = "../../../outside.tar"
        _write_json(catalog_path, catalog)
        _restamp(output)

        with self.assertRaisesRegex(
            FullFlowDataPlaneError, "object catalog does not match"
        ):
            verify_full_flow_data_plane_package(output)

    def test_checksum_tampering_is_rejected(self) -> None:
        output = self.root / "package"
        build_full_flow_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="checksum-tamper-v1",
        )
        artifact = output / "artifacts" / OBJECT_A / f"{REPRESENTATION_ID}.tar"
        artifact.write_bytes(artifact.read_bytes()[:-1] + b"x")
        with self.assertRaisesRegex(FullFlowDataPlaneError, "checksum mismatch"):
            verify_full_flow_data_plane_package(output)


if __name__ == "__main__":
    unittest.main()
