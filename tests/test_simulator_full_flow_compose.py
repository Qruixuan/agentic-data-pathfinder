"""Offline tests for the native full-flow Compose deployment binding."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pathfinder.frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    deterministic_frame_bundle_tar,
)
from pathfinder.simulator import (
    build_local_container_compose,
    build_portable_execution_plan,
    plan_container_backend,
)
from pathfinder.simulator.full_flow_compose import (
    FullFlowComposeError,
    build_full_flow_compose_binding,
    verify_full_flow_compose_binding,
)
from pathfinder.simulator.full_flow_data_plane import (
    FullFlowArtifactBinding,
    build_full_flow_data_plane_package,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)
OBJECT_ID = "nextqa-val-0000000001"
REPRESENTATION_ID = "sampled_frame_bundle"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _minimal_jpeg() -> bytes:
    """A structural 2x2 JPEG accepted by the non-decoding ingest gate."""

    return bytes.fromhex(
        "ffd8"  # SOI
        "ffc0"  # baseline SOF
        "000b"  # segment length, one component
        "08"  # sample precision
        "0002"  # height
        "0002"  # width
        "01"  # component count
        "01" "11" "00"  # component descriptor
        "ffd9"  # EOI
    )


def _bundle_bytes() -> bytes:
    frame = _minimal_jpeg()
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": REPRESENTATION_ID,
        "object_id": OBJECT_ID,
        "source_video_id": "0000000001",
        "source_video_filename": "0000000001.mp4",
        "source_video_size_bytes": 123456,
        "source_video_sha256": "a" * 64,
        "source_duration_seconds": 2.0,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": 1,
            "jpeg_max_dimension": 768,
            "jpeg_quality": 82,
            "jpeg_optimize": True,
        },
        "source_frame_descriptions": {
            "representation_id": "sampled_frames",
            "path": f"{OBJECT_ID}/sampled_frames.json",
            "sha256": "b" * 64,
        },
        "generation_manifest_sha256": "c" * 64,
        "frames": [{
            "frame_index": 0,
            "timestamp_seconds": 1.0,
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
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return deterministic_frame_bundle_tar([
        (OBJECT_MANIFEST_NAME, manifest_bytes),
        ("frames/000.jpg", frame),
    ])


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def _restamp(root: Path) -> None:
    names = sorted(
        path.name
        for path in root.iterdir()
        if path.is_file() and path.name != "SHA256SUMS"
    )
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{_sha256((root / name).read_bytes())}  {name}\n"
            for name in names
        ),
        encoding="utf-8",
    )


class FullFlowComposeBindingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)

        portable = cls.root / "portable"
        container_plan = cls.root / "container-plan"
        cls.base_compose = cls.root / "base-compose"
        build_portable_execution_plan(SCENARIO, output_dir=portable)
        plan_container_backend(
            SCENARIO,
            portable,
            CONTAINER_SPEC,
            output_dir=container_plan,
        )
        build_local_container_compose(
            container_plan,
            output_dir=cls.base_compose,
            semantic_executor_node_id="N6",
            semantic_artifact_source_node_ids=("N4",),
        )

        cls.raw_bundle = _bundle_bytes()
        artifact = cls.root / "source-frame-bundle.tar"
        artifact.write_bytes(cls.raw_bundle)
        cls.data_plane = cls.root / "data-plane"
        build_full_flow_data_plane_package(
            [FullFlowArtifactBinding(
                object_id=OBJECT_ID,
                artifact_path=artifact,
                catalog_version="full-flow-compose-test-catalog-v1",
                plan_ids=("D-origin-warm",),
                artifact_sha256=_sha256(cls.raw_bundle),
                artifact_size_bytes=len(cls.raw_bundle),
            )],
            output_dir=cls.data_plane,
            package_id="full-flow-compose-test-v1",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def setUp(self) -> None:
        self.case = Path(tempfile.mkdtemp(dir=self.root))

    def tearDown(self) -> None:
        for path in sorted(self.case.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        self.case.rmdir()

    def _build(self, name: str = "binding") -> Path:
        output = self.case / name
        build_full_flow_compose_binding(
            self.base_compose,
            self.data_plane,
            output_dir=output,
        )
        return output

    def test_generated_overlay_is_deterministic_and_nonlaunching(self) -> None:
        first = self._build("first")
        second = self._build("second")

        self.assertEqual(_tree_bytes(first), _tree_bytes(second))
        verified = verify_full_flow_compose_binding(
            first,
            base_compose_package=self.base_compose,
            data_plane_package=self.data_plane,
        )
        self.assertEqual("VERIFIED_NOT_LAUNCHED", verified["status"])
        self.assertFalse(verified["services_started"])
        self.assertFalse(verified["workflow_submitted"])
        self.assertEqual(8, verified["logical_node_count"])
        self.assertEqual(9, verified["container_service_count"])
        checksum_names = [
            line.split("  ", 1)[1]
            for line in (first / "SHA256SUMS").read_text().splitlines()
        ]
        self.assertEqual(sorted(checksum_names), checksum_names)

    def test_overlay_wires_n4_data_agent_n7_executor_and_n6_inference(self) -> None:
        output = self._build()
        overlay = (output / "compose.full-flow.yaml").read_text(
            encoding="utf-8"
        )

        self.assertIn("pathfinder-sim-n4-data-agent:", overlay)
        self.assertIn('      - "serve-data-agent"', overlay)
        self.assertIn('      - "--require-token"', overlay)
        self.assertIn('      - "--require-artifact-secret"', overlay)
        self.assertIn(
            '      - "/data/config/data-agent-manifest.json"', overlay
        )
        self.assertIn(
            "PATHFINDER_FULL_FLOW_DATA_AGENT_BASE_URL="
            "http://pathfinder-sim-n4-data-agent:8780",
            overlay,
        )
        self.assertIn(
            "PATHFINDER_FULL_FLOW_SEMANTIC_BASE_URL="
            "http://pathfinder-sim-n6-inference:9080",
            overlay,
        )
        self.assertIn("PATHFINDER_FULL_FLOW_SOURCE_NODE_ID=N4", overlay)
        self.assertIn("PATHFINDER_FULL_FLOW_EXECUTOR_NODE_ID=N7", overlay)
        self.assertIn("PATHFINDER_FULL_FLOW_INFERENCE_NODE_ID=N6", overlay)
        self.assertEqual(2, overlay.count("PATHFINDER_CONTAINER_NODE_TOKEN"))
        self.assertIn("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET", overlay)
        self.assertIn('pathfinder.full-flow-role: "origin-warm"', overlay)
        self.assertIn('pathfinder.full-flow-role: "trial-executor"', overlay)
        self.assertIn('pathfinder.full-flow-role: "vision-inference"', overlay)
        self.assertEqual(2, overlay.count("pathfinder-sim-n4-data-agent-state:"))
        self.assertIn(
            '      - "pathfinder-sim-n4-data-agent-state:/state"', overlay
        )

    def test_package_records_secret_names_but_never_values_or_host_paths(self) -> None:
        sentinel_token = "sentinel-token-value-must-not-be-written"
        sentinel_secret = "sentinel-artifact-secret-must-not-be-written"
        semantic_token = "sentinel-semantic-token-must-not-be-written"
        ingress_secret = "sentinel-ingress-secret-must-not-be-written"
        with mock.patch.dict(
            os.environ,
            {
                "PATHFINDER_DATA_AGENT_TOKEN": sentinel_token,
                "PATHFINDER_DATA_AGENT_ARTIFACT_SECRET": sentinel_secret,
                "PATHFINDER_CONTAINER_NODE_TOKEN": semantic_token,
                "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": ingress_secret,
                "PATHFINDER_FULL_FLOW_DATA_PLANE_ROOT": str(self.root),
            },
        ):
            output = self._build()

        rendered = b"\n".join(_tree_bytes(output).values()).decode("utf-8")
        self.assertIn("PATHFINDER_DATA_AGENT_TOKEN", rendered)
        self.assertIn("PATHFINDER_DATA_AGENT_ARTIFACT_SECRET", rendered)
        self.assertIn("PATHFINDER_CONTAINER_NODE_TOKEN", rendered)
        self.assertIn("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET", rendered)
        self.assertNotIn(sentinel_token, rendered)
        self.assertNotIn(sentinel_secret, rendered)
        self.assertNotIn(semantic_token, rendered)
        self.assertNotIn(ingress_secret, rendered)
        self.assertNotIn(str(self.root), rendered)
        self.assertNotRegex(rendered, r"(?<![A-Za-z])[A-Za-z]:[\\/]")
        binding = json.loads(
            (output / "full-flow-compose-binding.json").read_text()
        )
        self.assertFalse(binding["credentials_recorded"])
        self.assertTrue(binding["runtime_authentication_required"])

    def test_binding_records_exact_base_and_data_plane_manifest_digests(self) -> None:
        output = self._build()
        binding = json.loads(
            (output / "full-flow-compose-binding.json").read_text()
        )
        self.assertEqual(
            _sha256(
                (self.base_compose / "local_container_manifest.json")
                .read_bytes()
            ),
            binding["base_compose_manifest_sha256"],
        )
        self.assertEqual(
            _sha256(
                (self.data_plane / "full-flow-data-plane.json").read_bytes()
            ),
            binding["data_plane_manifest_sha256"],
        )
        self.assertEqual("N4", binding["source_node_id"])
        self.assertEqual("N7", binding["executor_node_id"])
        self.assertEqual("N6", binding["inference_node_id"])

    def test_overlay_tamper_is_rejected_even_after_checksum_restamp(self) -> None:
        output = self._build()
        overlay_path = output / "compose.full-flow.yaml"
        overlay_path.write_text(
            overlay_path.read_text(encoding="utf-8").replace(
                "PATHFINDER_FULL_FLOW_SOURCE_NODE_ID=N4",
                "PATHFINDER_FULL_FLOW_SOURCE_NODE_ID=N3",
            ),
            encoding="utf-8",
        )
        _restamp(output)

        with self.assertRaisesRegex(FullFlowComposeError, "overlay changed"):
            verify_full_flow_compose_binding(
                output,
                base_compose_package=self.base_compose,
                data_plane_package=self.data_plane,
            )

    def test_binding_tamper_is_rejected_even_after_checksum_restamp(self) -> None:
        output = self._build()
        binding_path = output / "full-flow-compose-binding.json"
        binding = json.loads(binding_path.read_text())
        binding["executor_node_id"] = "N8"
        binding_path.write_text(
            json.dumps(binding, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _restamp(output)

        with self.assertRaisesRegex(
            FullFlowComposeError, "source binding changed"
        ):
            verify_full_flow_compose_binding(
                output,
                base_compose_package=self.base_compose,
                data_plane_package=self.data_plane,
            )

    def test_valid_but_different_data_plane_fails_source_binding(self) -> None:
        alternate = self.case / "alternate-data-plane"
        artifact = self.case / "alternate-source.tar"
        artifact.write_bytes(self.raw_bundle)
        build_full_flow_data_plane_package(
            [FullFlowArtifactBinding(
                object_id=OBJECT_ID,
                artifact_path=artifact,
                catalog_version="full-flow-compose-test-catalog-v1",
                plan_ids=("D-origin-warm",),
            )],
            output_dir=alternate,
            package_id="different-package-id-v1",
        )
        output = self._build()

        with self.assertRaisesRegex(
            FullFlowComposeError, "source binding changed"
        ):
            verify_full_flow_compose_binding(
                output,
                base_compose_package=self.base_compose,
                data_plane_package=alternate,
            )

    def test_existing_output_is_preserved(self) -> None:
        output = self.case / "binding"
        output.mkdir()
        sentinel = output / "operator-owned.txt"
        sentinel.write_text("preserve", encoding="utf-8")

        with self.assertRaisesRegex(FullFlowComposeError, "already exists"):
            build_full_flow_compose_binding(
                self.base_compose,
                self.data_plane,
                output_dir=output,
            )
        self.assertEqual("preserve", sentinel.read_text(encoding="utf-8"))
        self.assertEqual([sentinel], list(output.iterdir()))


if __name__ == "__main__":
    unittest.main()
