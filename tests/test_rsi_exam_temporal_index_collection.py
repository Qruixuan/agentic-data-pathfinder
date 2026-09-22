from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

from pathfinder.rsi_exam.collection_plan import freeze_collection_plan
from pathfinder.rsi_exam.formal_foundation import (
    build_formal_runtime_foundation,
    verify_formal_runtime_foundation,
)
from pathfinder.rsi_exam.temporal_index_collection import (
    FormalTemporalIndexError,
    _embedding_batches,
    finalize_formal_temporal_index,
    materialize_formal_temporal_captions,
    prepare_formal_temporal_index,
    verify_formal_temporal_caption_package,
    verify_formal_temporal_index_preparation,
)
from pathfinder.simulator.n3_indexed_data_plane import (
    verify_n3_indexed_data_plane_package,
)
from pathfinder.simulator.raw_cold_data_plane import (
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)
from pathfinder.video_prep import SampledImage


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _mp4(marker: int) -> bytes:
    compatible = b"isom" + b"iso2" + b"mp41"
    payload = b"isom" + struct.pack(">I", 512) + compatible
    ftyp = struct.pack(">I", len(payload) + 8) + b"ftyp" + payload
    body = bytes((index + marker) % 251 for index in range(1024 * 1024))
    return ftyp + struct.pack(">I", len(body) + 8) + b"mdat" + body


def _jpeg(index: int, marker: int = 0) -> bytes:
    app0 = b"\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    sof0 = (
        b"\xff\xc0\x00\x0b\x08"
        + (2).to_bytes(2, "big")
        + (2).to_bytes(2, "big")
        + b"\x01\x01\x11\x00"
    )
    sos = b"\xff\xda\x00\x08\x01\x01\x00\x00\x3f\x00"
    return (
        b"\xff\xd8" + app0 + sof0 + sos
        + bytes([index % 250, marker % 250, 0x22]) + b"\xff\xd9"
    )


class _Sampler:
    duration = 20.0

    def __call__(
        self,
        path: Path,
        *,
        frame_count: int,
        jpeg_max_dimension: int,
        temporal_start_fraction: float,
        temporal_end_fraction: float,
    ) -> tuple[list[SampledImage], float]:
        marker = int(path.read_bytes()[-1])
        start = self.duration * temporal_start_fraction
        span = self.duration * (
            temporal_end_fraction - temporal_start_fraction
        )
        frames = []
        for index in range(frame_count):
            timestamp = start + span * (index + 0.5) / frame_count
            frames.append(SampledImage(
                frame_index=index,
                timestamp_seconds=timestamp,
                width=2,
                height=2,
                jpeg_bytes=_jpeg(index, marker),
            ))
        return frames, self.duration


def _task(object_id: str, stratum: str) -> dict:
    question = (
        "what happened after the person moved"
        if stratum == "temporal"
        else f"what is visible in {stratum} video"
    )
    return {
        "answer_options": [
            {"option_id": "A", "text": "first"},
            {"option_id": "B", "text": "second"},
        ],
        "credentials_recorded": False,
        "object_id": object_id,
        "question": question,
        "schema_version": "pathfinder.n1-public-task-binding/v1alpha1",
        "success_scoring_rule": "multiple-choice-option-id-canonical-match-v1",
        "task_binding_sha256": _sha256(object_id.encode()),
        "task_class_id": "video_qa",
        "workload_id": stratum,
    }


class FormalTemporalIndexCollectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tasks = self.root / "public-tasks.json"
        objects = [
            ("video-causal", "causal"),
            ("video-descriptive", "descriptive"),
            ("video-temporal", "temporal"),
        ]
        _write_json(self.tasks, {
            "credentials_recorded": False,
            "label_values_included": False,
            "schema_version": "pathfinder.public-task-set/v1alpha1",
            "task_plane_id": "formal-index-test-v1",
            "tasks": [_task(*item) for item in objects],
        })
        spec = self.root / "spec.json"
        _write_json(spec, {
            "cohort_id": "formal-index-test-v1",
            "collection_repetitions": 1,
            "schema_version": "pathfinder.rsi-exam-cohort-spec/v1alpha1",
            "selection_seed": "formal-index-test-seed",
            "split_stratum_targets": {
                "fixture": {
                    "causal": 1,
                    "descriptive": 1,
                    "temporal": 1,
                },
            },
            "stratum_by_workload": {
                "causal": "causal",
                "descriptive": "descriptive",
                "temporal": "temporal",
            },
        })
        self.plan = self.root / "plan"
        freeze_collection_plan(
            self.tasks,
            spec,
            builder_commit="7" * 40,
            output_dir=self.plan,
        )
        bindings = []
        for marker, (object_id, _) in enumerate(objects):
            payload = _mp4(marker)
            source = self.root / f"{object_id}.mp4"
            source.write_bytes(payload)
            bindings.append(RawColdObjectBinding(
                object_id=object_id,
                artifact_path=source,
                catalog_version="formal-index-test-catalog-v1",
                plan_ids=tuple(f"D{index}" for index in range(8)),
                dataset_id="nextqa",
                dataset_revision="test",
                source_object_id=object_id,
                artifact_sha256=_sha256(payload),
                artifact_size_bytes=len(payload),
            ))
        self.raw = self.root / "raw"
        build_raw_cold_data_plane_package(
            bindings,
            output_dir=self.raw,
            package_id="formal-index-test-raw-v1",
        )
        self.prep = self.root / "prep"
        prepare_formal_temporal_index(
            self.plan,
            self.raw,
            output_dir=self.prep,
            package_id="formal-index-test-prep-v1",
            caption_frame_count=12,
            sampler=_Sampler(),
        )

    @staticmethod
    def _caption_transport(request, timeout: float) -> bytes:
        del timeout
        body = json.loads(request.data.decode("utf-8"))
        assert body["response_format"] == {"type": "json_object"}
        content = body["messages"][0]["content"]
        rendered = json.dumps(content).casefold()
        assert "what happened after the person moved" not in rendered
        assert "what is visible in causal video" not in rendered
        caption = {
            "subjects": ["person"],
            "subject_actions": ["the person moves"],
            "objects_interacted_with": [],
            "camera_relation": "static",
            "initial_visible_state": "person at first position",
            "final_visible_state": "person at later position",
            "observable_transition": "person changes position",
            "uncertainty": "",
        }
        return json.dumps({
            "model": "caption-model",
            "choices": [{
                "finish_reason": "stop",
                "message": {"content": json.dumps(caption)},
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20},
        }).encode("utf-8")

    @staticmethod
    def _embedding_transport(request, timeout: float) -> bytes:
        del timeout
        body = json.loads(request.data.decode("utf-8"))
        rows = []
        for index, text in enumerate(body["input"]):
            seed = sum(text.encode("utf-8")) + index
            vector = [((seed + offset * 17) % 101) / 100.0 for offset in range(8)]
            rows.append({"index": index, "embedding": vector})
        return json.dumps({
            "data": rows,
            "usage": {"prompt_tokens": len(rows)},
        }).encode("utf-8")

    def test_preparation_captions_and_multi_object_n3_are_bound(self) -> None:
        verified = verify_formal_temporal_index_preparation(self.prep)
        self.assertEqual(3, verified["object_count"])
        self.assertGreaterEqual(verified["window_count"], 6)

        captions = self.root / "captions"
        cache = self.root / "cache"
        prior = cache / "raw" / "video-causal" / "00.attempt-01.json"
        prior.parent.mkdir(parents=True)
        prior.write_text("{}\n", encoding="utf-8", newline="\n")
        receipt = materialize_formal_temporal_captions(
            self.prep,
            output_dir=captions,
            cache_dir=cache,
            package_id="formal-index-test-captions-v1",
            model_id="caption-model",
            base_url="https://provider.invalid/v1",
            api_key="not-recorded-test-key",
            parallelism=3,
            transport=self._caption_transport,
        )
        self.assertEqual(receipt["caption_count"], receipt[
            "provider_request_count_this_run"
        ])
        self.assertTrue(
            (cache / "raw" / "video-causal" / "00.attempt-02.json").is_file()
        )
        self.assertTrue(prior.is_file())
        self.assertEqual(
            "VERIFIED_QUESTION_INDEPENDENT_TEMPORAL_CAPTIONS",
            verify_formal_temporal_caption_package(captions, self.prep)[
                "status"
            ],
        )

        result = finalize_formal_temporal_index(
            self.prep,
            captions,
            self.plan,
            self.tasks,
            self.raw,
            output_dir=self.root / "index",
            n3_output_dir=self.root / "n3-indexed",
            runtime_frame_manifest_dir=self.root / "runtime-frames",
            package_id="formal-index-test-index-v1",
            n3_package_id="formal-index-test-n3-v1",
            embedding_model_id="embedding-model",
            base_url="https://provider.invalid/v1",
            api_key="not-recorded-test-key",
            dimension=8,
            batch_size=8,
            runtime_frame_count=2,
            transport=self._embedding_transport,
            n3_sampler=_Sampler(),
        )
        self.assertEqual(3, result["object_count"])
        n3 = verify_n3_indexed_data_plane_package(self.root / "n3-indexed")
        self.assertEqual(
            "pathfinder.simulator-n3-indexed-data-plane/v1alpha2",
            n3["schema_version"],
        )
        self.assertEqual(3, n3["object_count"])
        self.assertEqual(
            3,
            len(list((self.root / "runtime-frames").glob(
                "*/runtime-frame-manifest.json"
            ))),
        )

        pilot = self.root / "pilot.json"
        public_tasks = json.loads(self.tasks.read_text(encoding="utf-8"))["tasks"]
        _write_json(pilot, {
            "schema_version": "fixture/v1",
            "workloads": [
                {
                    "id": row["workload_id"],
                    "object_id": row["object_id"],
                    "question": (
                        f"{row['question']} Answer with the best option text: "
                        "first; second."
                    ),
                    "accepted_answer_substrings": ["first"],
                }
                for row in public_tasks
            ],
        })
        repository = Path(__file__).resolve().parents[1]
        foundation = self.root / "foundation"
        built = build_formal_runtime_foundation(
            self.plan,
            self.tasks,
            pilot,
            self.raw,
            self.root / "n3-indexed",
            self.root / "index",
            self.prep,
            captions,
            repository / "configs" / "flowmesh_infra_simulator_4x8_smoke.json",
            output_dir=foundation,
            package_id="formal-index-test-foundation-v1",
            source_commit="8" * 40,
            expected_model="fixture-model",
        )
        self.assertEqual(3, built["case_count"])
        verified_foundation = verify_formal_runtime_foundation(foundation)
        self.assertEqual(
            "VERIFIED_RSI_EXAM_FORMAL_RUNTIME_FOUNDATION",
            verified_foundation["status"],
        )
        self.assertEqual(6, verified_foundation["n4_artifact_count"])
        self.assertTrue(verified_foundation["labels_confined_to_n1"])
        foundation_manifest = json.loads(
            (foundation / "formal-foundation-manifest.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertNotIn("pilot_config_sha256", foundation_manifest)
        scenario = json.loads(
            (foundation / "scenario.json").read_text(encoding="utf-8")
        )
        by_design = {
            row["design_id"]: row for row in scenario["designs"]
        }
        self.assertEqual(
            "raw-indexed",
            by_design["D1"]["route_templates"]["W1"],
        )
        self.assertEqual(
            "raw-indexed",
            by_design["D5"]["route_templates"]["W1"],
        )

    def test_text_embedding_v4_batch_limit_fails_before_transport(self) -> None:
        def transport(request, timeout):
            del request, timeout
            self.fail("oversized embedding batch reached the provider")

        with self.assertRaisesRegex(
            FormalTemporalIndexError,
            "text-embedding-v4 limit of 10",
        ):
            _embedding_batches(
                texts=[f"text-{index}" for index in range(11)],
                model_id="text-embedding-v4",
                dimension=8,
                base_url="https://provider.invalid/v1",
                api_key="not-recorded-test-key",
                batch_size=11,
                timeout_seconds=1.0,
                transport=transport,
            )


if __name__ == "__main__":
    unittest.main()
