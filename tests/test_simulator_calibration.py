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
    RETRIEVAL_CONFIG_SCHEMA_VERSION,
    SimulatorCalibrationError,
    build_simulator_retrieval_cohort,
    calibrate_simulator_scenario,
    load_simulator_scenario,
    verify_simulator_calibration,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CALIBRATION = (
    ROOT
    / "configs"
    / "flowmesh_infra_simulator_4x8_existing_evidence_calibration.json"
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class SimulatorCalibrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.video_root = self.root / "videos"
        self.representation_root = self.root / "representations"
        self.bundle_root = self.root / "bundles"
        self.video_root.mkdir()
        self.representation_root.mkdir()
        self.bundle_root.mkdir()

        specifications = {
            "nextqa-desc": ("descriptive", b"d" * 101, b"digest-d", b"bundle-d" * 9),
            "nextqa-temp": ("temporal", b"t" * 203, b"digest-t-long", b"bundle-t" * 11),
            "nextqa-causal": ("causal", b"c" * 307, b"digest-c-longer", b"bundle-c" * 13),
        }
        generation_objects = []
        workloads = {}
        for object_id, (stratum, video, digest, bundle) in specifications.items():
            video_name = object_id + ".mp4"
            (self.video_root / video_name).write_bytes(video)
            rep_dir = self.representation_root / object_id
            rep_dir.mkdir()
            digest_path = rep_dir / "multimodal_digest.txt"
            digest_path.write_bytes(digest)
            generation_objects.append({
                "object_id": object_id,
                "source_video": {
                    "filename": video_name,
                    "size_bytes": len(video),
                    "sha256": _sha256(video),
                },
                "representations": {
                    "multimodal_digest": {
                        "path": f"{object_id}/multimodal_digest.txt",
                        "size_bytes": len(digest),
                        "sha256": _sha256(digest),
                    }
                },
            })
            workloads[f"{stratum}-q1"] = {
                "stratum_id": stratum,
                "object_id": object_id,
            }
            bundle_dir = self.bundle_root / object_id
            bundle_dir.mkdir()
            (bundle_dir / "sampled_frame_bundle.tar").write_bytes(bundle)

        generation = {
            "credentials_recorded": False,
            "objects": generation_objects,
        }
        self.generation_path = self.representation_root / "generation-manifest.json"
        generation_bytes = (
            json.dumps(generation, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        self.generation_path.write_bytes(generation_bytes)
        generation_sha = _sha256(generation_bytes)

        checksum_lines = []
        for object_id, (_, video, _, bundle) in specifications.items():
            manifest = {
                "object_id": object_id,
                "generation_manifest_sha256": generation_sha,
                "source_video_sha256": _sha256(video),
                "source_video_size_bytes": len(video),
            }
            manifest_path = self.bundle_root / object_id / "frame_bundle_manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            bundle_path = self.bundle_root / object_id / "sampled_frame_bundle.tar"
            checksum_lines.append(
                f"{_sha256(bundle_path.read_bytes())}  "
                f"{object_id}/sampled_frame_bundle.tar\n"
            )
        (self.bundle_root / "SHA256SUMS").write_text(
            "".join(checksum_lines), encoding="utf-8"
        )
        self.workload_path = self.root / "workloads.json"
        self.workload_path.write_text(
            json.dumps(workloads, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _calibrate(self, output: Path) -> dict[str, object]:
        return calibrate_simulator_scenario(
            SCENARIO,
            CALIBRATION,
            workload_manifest_path=self.workload_path,
            representation_manifest_path=self.generation_path,
            frame_bundle_root=self.bundle_root,
            video_root=self.video_root,
            output_dir=output,
        )

    def test_only_identifiable_object_sizes_are_calibrated(self) -> None:
        output = self.root / "calibrated"
        result = self._calibrate(output)
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(9, result["calibrated_object_size_parameter_count"])
        self.assertEqual(0, result["resource_parameter_count_calibrated"])
        self.assertEqual(0, result["link_parameter_count_calibrated"])

        scenario = json.loads(
            (output / "calibrated_scenario.json").read_text(encoding="utf-8")
        )
        objects = {item["object_id"]: item for item in scenario["objects"]}
        self.assertEqual(
            {
                "raw_video": 101,
                "sampled_frame_bundle": len(b"bundle-d" * 9),
                "multimodal_digest": len(b"digest-d"),
            },
            objects["video-descriptive"]["representations"],
        )
        self.assertEqual(
            90000000,
            objects["video-retrieval-target"]["representations"]["raw_video"],
        )
        self.assertEqual(
            len(b"digest-d"),
            scenario["nodes"][6]["caches"][0]["initial_entries"][0][
                "size_bytes"
            ],
        )
        load_simulator_scenario(output / "calibrated_scenario.json")

    def test_report_marks_every_unmeasured_parameter_class(self) -> None:
        output = self.root / "calibrated"
        self._calibrate(output)
        report = json.loads(
            (output / "calibration_report.json").read_text(encoding="utf-8")
        )
        self.assertEqual("partial-mixed-evidence", report["calibration_class"])
        self.assertIn("gpu_service_time", report["retained_provisional_parameter_classes"])
        self.assertIn("iperf3_network_calibration", report["required_next_evidence"])
        self.assertFalse(report["eligible_for_scientific_claims"])

    def test_outputs_are_deterministic_and_verifiable(self) -> None:
        first = self.root / "first"
        second = self.root / "second"
        self._calibrate(first)
        self._calibrate(second)
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        verified = verify_simulator_calibration(first)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(3, verified["checked_files"])

    def test_modified_evidence_is_refused(self) -> None:
        digest = self.representation_root / "nextqa-desc" / "multimodal_digest.txt"
        digest.write_bytes(b"modified")
        with self.assertRaisesRegex(
            SimulatorCalibrationError,
            r"digest (size|checksum) mismatch",
        ):
            self._calibrate(self.root / "modified")

    def test_containment_escape_in_generation_manifest_is_refused(self) -> None:
        manifest = json.loads(self.generation_path.read_text(encoding="utf-8"))
        manifest["objects"][0]["representations"]["multimodal_digest"][
            "path"
        ] = "../outside.txt"
        self.generation_path.write_text(json.dumps(manifest), encoding="utf-8")
        # Frame manifests are deliberately left bound to the previous digest;
        # containment must fail before that later provenance check.
        with self.assertRaisesRegex(SimulatorCalibrationError, "contained relative"):
            self._calibrate(self.root / "escape")

    def test_existing_output_is_not_overwritten(self) -> None:
        output = self.root / "existing"
        output.mkdir()
        marker = output / "operator.txt"
        marker.write_text("keep", encoding="utf-8")
        with self.assertRaisesRegex(SimulatorCalibrationError, "already exists"):
            self._calibrate(output)
        self.assertEqual("keep", marker.read_text(encoding="utf-8"))

    def test_cli_runs_without_external_services(self) -> None:
        output = self.root / "cli"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = cli_main([
                "calibrate-flowmesh-infra-scenario",
                "--scenario",
                str(SCENARIO),
                "--calibration-config",
                str(CALIBRATION),
                "--workload-manifest",
                str(self.workload_path),
                "--representation-manifest",
                str(self.generation_path),
                "--frame-bundle-root",
                str(self.bundle_root),
                "--video-root",
                str(self.video_root),
                "--output-dir",
                str(output),
                "--compact",
            ])
        self.assertEqual(0, status)
        report = json.loads(stdout.getvalue())
        self.assertEqual("COMPLETE", report["status"])
        self.assertFalse(report["external_services_called"])

    def test_retrieval_relevance_calibrates_w4_without_inference(self) -> None:
        retrieval_config = self.root / "retrieval-config.json"
        retrieval_config.write_text(json.dumps({
            "schema_version": RETRIEVAL_CONFIG_SCHEMA_VERSION,
            "retrieval_id": "retrieval-calibration-test",
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
                    "query_id": "retrieve-description",
                    "query_text": "find the descriptive event",
                    "relevant_object_ids": ["nextqa-desc"],
                    "source_object_group": "desc-group",
                    "split": "train",
                },
                {
                    "query_id": "retrieve-temporal",
                    "query_text": "find the temporal event",
                    "relevant_object_ids": ["nextqa-temp"],
                    "source_object_group": "temp-group",
                    "split": "validation",
                },
                {
                    "query_id": "retrieve-causal",
                    "query_text": "find the causal event",
                    "relevant_object_ids": ["nextqa-causal"],
                    "source_object_group": "causal-group",
                    "split": "test",
                },
            ],
        }), encoding="utf-8")
        retrieval_output = self.root / "retrieval-output"
        build_simulator_retrieval_cohort(
            retrieval_config,
            self.generation_path,
            output_dir=retrieval_output,
        )
        calibration_config = self.root / "w4-calibration.json"
        calibration_config.write_text(json.dumps({
            "schema_version": (
                "pathfinder.flowmesh-infra-calibration-config/v1alpha1"
            ),
            "calibration_id": "w4-calibration-test",
            "output_scenario_id": "w4-calibrated-test",
            "object_profiles": [{
                "workload_class": "W4",
                "target_object_id": "video-retrieval-target",
                "mode": "retrieval-relevance-representative",
                "selection_rule": "nearest-object-to-raw-size-median",
            }],
        }), encoding="utf-8")
        output = self.root / "w4-calibrated"
        result = calibrate_simulator_scenario(
            SCENARIO,
            calibration_config,
            workload_manifest_path=self.workload_path,
            representation_manifest_path=self.generation_path,
            frame_bundle_root=self.bundle_root,
            video_root=self.video_root,
            retrieval_output_dir=retrieval_output,
            output_dir=output,
        )
        self.assertEqual(3, result["calibrated_object_size_parameter_count"])
        scenario = json.loads(
            (output / "calibrated_scenario.json").read_text(encoding="utf-8")
        )
        objects = {item["object_id"]: item for item in scenario["objects"]}
        self.assertEqual(
            203,
            objects["video-retrieval-target"]["representations"]["raw_video"],
        )
        report = json.loads(
            (output / "calibration_report.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            "operator-verified",
            report["object_profiles"][0]["retrieval_annotation_status"],
        )

    def test_retrieval_mode_requires_verified_retrieval_output(self) -> None:
        config = self.root / "requires-retrieval.json"
        config.write_text(json.dumps({
            "schema_version": (
                "pathfinder.flowmesh-infra-calibration-config/v1alpha1"
            ),
            "calibration_id": "requires-retrieval",
            "output_scenario_id": "requires-retrieval-output",
            "object_profiles": [{
                "workload_class": "W4",
                "target_object_id": "video-retrieval-target",
                "mode": "retrieval-relevance-representative",
                "selection_rule": "nearest-object-to-raw-size-median",
            }],
        }), encoding="utf-8")
        with self.assertRaisesRegex(
            SimulatorCalibrationError,
            "retrieval_output_dir is required",
        ):
            calibrate_simulator_scenario(
                SCENARIO,
                config,
                workload_manifest_path=self.workload_path,
                representation_manifest_path=self.generation_path,
                frame_bundle_root=self.bundle_root,
                video_root=self.video_root,
                output_dir=self.root / "missing-retrieval",
            )


if __name__ == "__main__":
    unittest.main()
