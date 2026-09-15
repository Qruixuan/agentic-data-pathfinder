"""Focused offline tests for the one-task full-flow local harness."""

from __future__ import annotations

import base64
import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
)
from pathfinder.frame_bundle_ingest import (
    FRAME_BUNDLE_MEDIA_TYPE,
    validate_frame_bundle_bytes,
)
from pathfinder.simulator.full_flow_local_harness import (
    EVIDENCE_NAME,
    LocalFullFlowHarnessError,
    N6SemanticInput,
    N6SemanticResult,
    run_local_full_flow_harness,
    verify_local_full_flow_harness,
)
from pathfinder.simulator.hidden_oracle import (
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    build_n1_hidden_label_record,
    build_n1_oracle_package,
    build_n1_public_task_binding,
)
from pathfinder.simulator.index_service import (
    INDEX_SOURCE_SCHEMA_VERSION,
    build_n2_index_package,
)
from pathfinder.simulator.n5_materialization import (
    freeze_n5_materialization_plan,
)
from pathfinder.simulator.raw_cold_data_plane import (
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)
from pathfinder.video_prep import (
    FRAME_SCHEMA_VERSION,
    PREP_SCHEMA_VERSION,
    SampledImage,
)


OBJECT_ID = "nextqa-val-0000000001"
DECOY_ID = "nextqa-val-0000000002"
VIDEO_ID = "0000000001"
VIDEO_NAME = f"{VIDEO_ID}.mp4"
SOURCE = b"\x00\x00\x00\x18ftypmp42local-full-flow-test-video"
PLAN_ID = "local-full-flow-materialization-v1"
VERSIONS = {
    "Pillow": "12.3.0-test",
    "av": "17.0.1-test",
    "pathfinder-minimal": "0.1-test",
}
EVIDENCE_SECRET = b"local-harness-test-evidence-key-32-bytes-minimum"

_TEST_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBAUEBAYFBQUGBgYHCQ4JCQgICRINDQoOFRIW"
    "FhUSFBQXGiEcFxgfGRQUHScdHyIjJSUlFhwpLCgkKyEkJST/2wBDAQYGBgkICREJCREkGBQY"
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/wAAR"
    "CAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAAAAAA"
    "AAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oA"
    "DAMBAAIRAxEAPwCdAAyqX//Z"
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _jpeg(marker: bytes) -> bytes:
    payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
    comment = b"\xff\xfe" + (len(marker) + 2).to_bytes(2, "big") + marker
    return payload[:2] + comment + payload[2:]


class AttestedTestSampler:
    sampler_id = "deterministic-offline-jpeg-fixture-v1"
    media_decode_exercised = False

    def __init__(self) -> None:
        self.calls = 0

    def __call__(
        self,
        path: Path,
        *,
        frame_count: int,
        jpeg_max_dimension: int,
    ) -> tuple[list[SampledImage], float]:
        self.calls += 1
        if path.read_bytes() != SOURCE:
            raise RuntimeError("source content changed")
        if frame_count != 2 or jpeg_max_dimension != 768:
            raise RuntimeError("sampling contract changed")
        return ([
            SampledImage(
                frame_index=index,
                timestamp_seconds=0.5 + 1.25 * index,
                width=2,
                height=2,
                jpeg_bytes=_jpeg(b"full-flow-" + bytes([index])),
            )
            for index in range(frame_count)
        ], 42.5)


class OfflineSemanticAdapter:
    def __init__(
        self,
        answer: str,
        *,
        llm_called: bool = False,
        external_services_called: bool = False,
    ) -> None:
        self.answer = answer
        self.llm_called = llm_called
        self.external_services_called = external_services_called
        self.calls: list[N6SemanticInput] = []

    def predict(self, value: N6SemanticInput) -> N6SemanticResult:
        self.calls.append(value)
        self.assert_public(value.public_task_binding)
        checked = validate_frame_bundle_bytes(
            value.artifact_bytes,
            expected_object_id=value.object_id,
            expected_sha256=value.artifact_sha256,
            expected_size_bytes=value.artifact_size_bytes,
            artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
        )
        if checked.frame_count != 2:
            raise RuntimeError("unexpected frame count")
        return N6SemanticResult(
            adapter_id="offline-semantic-fixture-v1",
            predicted_answer=self.answer,
            llm_called=self.llm_called,
            external_services_called=self.external_services_called,
        )

    @staticmethod
    def assert_public(value: Any) -> None:
        if isinstance(value, dict):
            if "correct_answer_id" in value or "predicted_answer" in value:
                raise AssertionError("N6 received a hidden or predicted answer")
            for child in value.values():
                OfflineSemanticAdapter.assert_public(child)
        elif isinstance(value, list):
            for child in value:
                OfflineSemanticAdapter.assert_public(child)


def _description_bytes() -> bytes:
    return _json_bytes({
        "schema_version": FRAME_SCHEMA_VERSION,
        "object_id": OBJECT_ID,
        "source_video_id": VIDEO_ID,
        "source_video_sha256": _sha256(SOURCE),
        "source_duration_seconds": 42.5,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": 2,
            "jpeg_max_dimension": 768,
        },
        "generator": {
            "model": "historical-vision-model",
            "temperature": 0,
            "prompt_sha256": "a" * 64,
        },
        "frames": [
            {
                "frame_index": 0,
                "timestamp_seconds": 0.5,
                "width": 2,
                "height": 2,
                "description": "Two musicians perform on a blue stage.",
                "visible_text": None,
            },
            {
                "frame_index": 1,
                "timestamp_seconds": 1.75,
                "width": 2,
                "height": 2,
                "description": "A person approaches the musicians.",
                "visible_text": None,
            },
        ],
    })


def _generation_manifest_bytes(description: bytes) -> bytes:
    return _json_bytes({
        "schema_version": PREP_SCHEMA_VERSION,
        "frame_count": 2,
        "jpeg_max_dimension": 768,
        "credentials_recorded": False,
        "objects": [{
            "object_id": OBJECT_ID,
            "source_video": {
                "filename": VIDEO_NAME,
                "size_bytes": len(SOURCE),
                "sha256": _sha256(SOURCE),
            },
            "representations": {
                "sampled_frames": {
                    "path": f"{OBJECT_ID}/sampled_frames.json",
                    "size_bytes": len(description),
                    "sha256": _sha256(description),
                }
            },
        }],
    })


class HarnessFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.public_task = build_n1_public_task_binding(
            workload_id="public-video-task-0001",
            object_id=OBJECT_ID,
            task_class_id="video-qa",
            question="Which clip shows two musicians performing on a blue stage?",
            answer_options=[
                {"option_id": "OTHER", "text": "A vehicle crosses a river."},
                {
                    "option_id": "TARGET",
                    "text": "Two musicians perform while a person approaches.",
                },
            ],
            success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        )
        hidden_record = build_n1_hidden_label_record(
            self.public_task,
            correct_answer_id="TARGET",
        )
        hidden_source = root / "operator-hidden-source.json"
        hidden_source.write_bytes(_json_bytes({
            "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
            "oracle_id": "local-full-flow-oracle-v1",
            "logical_node_id": "N1",
            "labels": [hidden_record],
            "credentials_recorded": False,
        }))
        self.n1_package = root / "n1-package"
        build_n1_oracle_package(hidden_source, output_dir=self.n1_package)

        index_source = root / "visible-index-source.json"
        index_source.write_bytes(_json_bytes({
            "schema_version": INDEX_SOURCE_SCHEMA_VERSION,
            "index_id": "local-visible-index-v1",
            "logical_node_id": "N2",
            "documents": [
                {
                    "object_id": OBJECT_ID,
                    "source_object_group": "public-video",
                    "visible_fields": {
                        "title": "Two musicians performing on a blue stage",
                        "tags": ["blue", "musicians", "performing"],
                    },
                },
                {
                    "object_id": DECOY_ID,
                    "source_object_group": "public-video",
                    "visible_fields": {
                        "title": "A red vehicle crossing a river",
                        "tags": ["red", "river", "vehicle"],
                    },
                },
            ],
            "credentials_recorded": False,
        }))
        self.n2_package = root / "n2-package"
        build_n2_index_package(index_source, output_dir=self.n2_package)

        source_path = root / VIDEO_NAME
        source_path.write_bytes(SOURCE)
        self.n3_package = root / "n3-package"
        build_raw_cold_data_plane_package(
            [RawColdObjectBinding(
                object_id=OBJECT_ID,
                artifact_path=source_path,
                catalog_version="local-raw-catalog-v1",
                plan_ids=(PLAN_ID,),
                dataset_id="local-public-dataset",
                dataset_revision="snapshot-v1",
                source_object_id=VIDEO_ID,
            )],
            output_dir=self.n3_package,
            package_id="local-n3-raw-package-v1",
        )

        description = _description_bytes()
        self.sampler = AttestedTestSampler()
        self.n5_plan = freeze_n5_materialization_plan(
            plan_id=PLAN_ID,
            idempotency_key="local-full-flow-n5-materialize-v1",
            object_id=OBJECT_ID,
            source_video_id=VIDEO_ID,
            source_video_filename=VIDEO_NAME,
            source_video_bytes=SOURCE,
            source_frame_descriptions_path=f"{OBJECT_ID}/sampled_frames.json",
            source_frame_descriptions_bytes=description,
            generation_manifest_bytes=_generation_manifest_bytes(description),
            frame_count=2,
            jpeg_max_dimension=768,
            sampler=self.sampler,
            software_versions=VERSIONS,
        ).plan
        self.sampler.calls = 0

    def run(
        self,
        adapter: OfflineSemanticAdapter,
        *,
        name: str = "successful-run",
    ) -> dict[str, Any]:
        return run_local_full_flow_harness(
            harness_id=f"local-full-flow-{name}-v1",
            public_task_binding=self.public_task,
            n1_oracle_package_dir=self.n1_package,
            n1_state_db=self.root / f"{name}-n1" / "oracle.sqlite3",
            n1_evidence_secret=EVIDENCE_SECRET,
            n2_index_package_dir=self.n2_package,
            n3_raw_package_dir=self.n3_package,
            n5_materialization_plan=self.n5_plan,
            n5_sampler=self.sampler,
            n4_store_dir=self.root / f"{name}-n4",
            n7_cache_dir=self.root / f"{name}-n7",
            n8_cache_dir=self.root / f"{name}-n8",
            n6_semantic_adapter=adapter,
            output_dir=self.root / f"{name}-evidence",
            cache_capacity_bytes=2 * 1024 * 1024,
        )


class FullFlowLocalHarnessTest(unittest.TestCase):
    def fixture(self) -> tuple[TemporaryDirectory[str], HarnessFixture]:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return temporary, HarnessFixture(Path(temporary.name))

    def test_one_public_task_exercises_all_nodes_with_hidden_n1_score(self) -> None:
        _temporary, fixture = self.fixture()
        adapter = OfflineSemanticAdapter("TARGET")
        result = fixture.run(adapter)

        self.assertEqual("AUTHENTICATED_LOCAL_COMPONENT_RUN", result["status"])
        self.assertTrue(result["oracle_hmac_verified"])
        self.assertEqual(8, result["logical_node_count"])
        self.assertTrue(result["task_success"])
        self.assertEqual(1.0, result["score"])
        self.assertFalse(result["media_decode_exercised"])
        self.assertFalse(result["four_by_eight_semantic_coverage_verified"])
        self.assertFalse(result["real_performance_measured"])
        self.assertFalse(result["real_cost_measured"])
        self.assertFalse(result["external_network_called"])
        self.assertFalse(result["llm_called"])
        self.assertEqual(1, fixture.sampler.calls)
        self.assertEqual(1, len(adapter.calls))

        output = Path(result["output_dir"])
        structural = verify_local_full_flow_harness(output)
        self.assertEqual("STRUCTURALLY_VERIFIED", structural["status"])
        self.assertIsNone(structural["task_success"])
        self.assertIsNone(structural["score"])
        self.assertTrue(structural["reported_task_success"])
        self.assertFalse(structural["oracle_hmac_verified"])
        evidence = json.loads((output / EVIDENCE_NAME).read_text("utf-8"))
        self.assertEqual(
            ["N2", "N3", "N5", "N4", "N7", "N8", "N6", "N1"],
            evidence["logical_stage_order"],
        )
        self.assertEqual(
            ["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"],
            evidence["logical_nodes_exercised"],
        )
        for node_id in ("N7", "N8"):
            cache = evidence["component_bindings"][node_id]
            self.assertTrue(cache["initial_miss"])
            self.assertTrue(cache["stored"])
            self.assertTrue(cache["persistent_reopen_hit"])
            self.assertTrue(cache["persistent_state_verified"])
        serialized = b"".join(
            path.read_bytes() for path in sorted(output.iterdir())
        )
        self.assertNotIn(b"correct_answer_id", serialized)
        self.assertNotIn(b"predicted_answer", serialized)
        self.assertNotIn(str(fixture.root).encode("utf-8"), serialized)
        self.assertNotIn(EVIDENCE_SECRET, serialized)
        self.assertNotIn(b"://", serialized)
        self.assertIn(b"correct_answer_id", (
            fixture.n1_package / "hidden-labels.json"
        ).read_bytes())

    def test_incorrect_prediction_is_scored_without_breaking_execution(self) -> None:
        _temporary, fixture = self.fixture()
        result = fixture.run(OfflineSemanticAdapter("OTHER"), name="wrong-answer")
        self.assertEqual("AUTHENTICATED_LOCAL_COMPONENT_RUN", result["status"])
        self.assertFalse(result["task_success"])
        self.assertEqual(0.0, result["score"])

    def test_rejects_adapter_that_attests_an_external_or_llm_call(self) -> None:
        _temporary, fixture = self.fixture()
        adapter = OfflineSemanticAdapter("TARGET", llm_called=True)
        with self.assertRaisesRegex(
            LocalFullFlowHarnessError,
            "no LLM or external service call",
        ):
            fixture.run(adapter, name="unsafe-adapter")
        self.assertFalse((fixture.root / "unsafe-adapter-evidence").exists())

    def test_requires_explicit_media_decode_attestation(self) -> None:
        _temporary, fixture = self.fixture()
        delattr(fixture.sampler.__class__, "media_decode_exercised")
        self.addCleanup(
            setattr,
            fixture.sampler.__class__,
            "media_decode_exercised",
            False,
        )
        with self.assertRaisesRegex(
            LocalFullFlowHarnessError,
            "explicitly attest media_decode_exercised",
        ):
            fixture.run(OfflineSemanticAdapter("TARGET"), name="no-attestation")

    def test_tampering_fails_even_with_updated_file_checksum(self) -> None:
        _temporary, fixture = self.fixture()
        result = fixture.run(OfflineSemanticAdapter("TARGET"), name="tamper")
        output = Path(result["output_dir"])
        evidence_path = output / EVIDENCE_NAME
        evidence = json.loads(evidence_path.read_text("utf-8"))
        evidence["task_success"] = False
        evidence_path.write_bytes(_json_bytes(evidence))

        checksum_path = output / "SHA256SUMS"
        rows = {}
        for line in checksum_path.read_text("utf-8").splitlines():
            digest, name = line.split("  ", 1)
            rows[name] = digest
        rows[EVIDENCE_NAME] = _sha256(evidence_path.read_bytes())
        checksum_path.write_text(
            "".join(f"{rows[name]}  {name}\n" for name in sorted(rows)),
            encoding="utf-8",
            newline="",
        )
        with self.assertRaisesRegex(
            LocalFullFlowHarnessError,
            "evidence digest mismatch",
        ):
            verify_local_full_flow_harness(output)


if __name__ == "__main__":
    unittest.main()
