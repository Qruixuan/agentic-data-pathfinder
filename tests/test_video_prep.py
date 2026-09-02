from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pathfinder.video_prep import (
    DIGEST_PROMPT,
    FRAME_SCHEMA_VERSION,
    FORMAL_VIDEO_IDS,
    FRAME_PROMPT,
    PreparationProtocolError,
    SampledImage,
    VideoPreparationError,
    _decode_json_layers,
    _digest_text,
    _extract_json_document,
    _json_bytes,
    _normalize_base_url,
    _normalize_video_ids,
    _sha256_bytes,
    _sha256_file,
    _validated_json_chat,
    _validate_digest,
    _validate_frame_descriptions,
    audit_recovery_checkpoint,
    load_selection_video_ids,
    prepare_representations,
)


ROOT = Path(__file__).resolve().parents[1]


class _ScriptedVisionClient:
    def __init__(
        self,
        responses: list[str],
        *,
        model: str = "test-model",
    ) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []
        self.model = model
        self.base_url = "https://api.example.invalid/v1"

    def chat(
        self,
        *,
        messages: list[dict[str, object]],
        seed: int,
    ) -> str:
        self.calls.append({"messages": messages, "seed": seed})
        return self.responses.pop(0)


class VideoPreparationParsingTest(unittest.TestCase):
    def test_protocol_retry_repairs_invalid_json_without_changing_seed(self) -> None:
        client = _ScriptedVisionClient(
            [
                '{"frames": [}',
                '{"frames": []}',
            ]
        )
        original_messages = [{"role": "user", "content": "original"}]

        result, attempts = _validated_json_chat(
            client=client,  # type: ignore[arg-type]
            messages=original_messages,
            seed=7301,
            response_name="test",
            validator=lambda payload: list(payload["frames"]),
            maximum_attempts=3,
        )

        self.assertEqual([], result)
        self.assertEqual(2, attempts)
        self.assertEqual([7301, 7301], [call["seed"] for call in client.calls])
        self.assertEqual(original_messages, client.calls[0]["messages"])
        repair_messages = client.calls[1]["messages"]
        self.assertEqual("assistant", repair_messages[-2]["role"])
        self.assertEqual("user", repair_messages[-1]["role"])
        self.assertIn("valid JSON", repair_messages[-1]["content"])

    def test_protocol_retry_fails_closed_after_bound(self) -> None:
        client = _ScriptedVisionClient(["bad", "still bad", "also bad"])

        with self.assertRaisesRegex(
            PreparationProtocolError,
            "remained invalid after 3 protocol attempt",
        ):
            _validated_json_chat(
                client=client,  # type: ignore[arg-type]
                messages=[{"role": "user", "content": "original"}],
                seed=42,
                response_name="digest",
                validator=lambda payload: payload,
                maximum_attempts=3,
            )

        self.assertEqual(3, len(client.calls))

    def test_protocol_retry_rejects_an_invalid_bound_before_calling(self) -> None:
        client = _ScriptedVisionClient([])

        for value in (0, -1, True):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    VideoPreparationError,
                    "positive integer",
                ):
                    _validated_json_chat(
                        client=client,  # type: ignore[arg-type]
                        messages=[],
                        seed=1,
                        response_name="test",
                        validator=lambda payload: payload,
                        maximum_attempts=value,
                    )

        self.assertEqual([], client.calls)

    def test_original_prompts_and_default_video_domain_remain_frozen(self) -> None:
        self.assertEqual(
            "f74783538a63279e3643e0960b916f7949e9090f8540e02c5103155c5b4982de",
            _sha256_bytes(FRAME_PROMPT.encode("utf-8")),
        )
        self.assertEqual(
            "475deee78505240ddc2eb28c8ed0d09f0f64167339e83cc12090e5089708748f",
            _sha256_bytes(DIGEST_PROMPT.encode("utf-8")),
        )
        self.assertEqual(8, len(FORMAL_VIDEO_IDS))

    def test_v2_selection_loads_eight_new_unique_videos_in_order(self) -> None:
        video_ids = load_selection_video_ids(
            ROOT
            / "configs"
            / "multi_candidate_formal_v2_workload_selection.json"
        )
        self.assertEqual(
            (
                "6356067859",
                "5296635780",
                "5735711594",
                "8132842161",
                "3462517143",
                "5026660202",
                "4942054721",
                "5840177726",
            ),
            video_ids,
        )

    def test_video_id_validation_rejects_duplicates_and_non_digits(self) -> None:
        with self.assertRaisesRegex(VideoPreparationError, "duplicate"):
            _normalize_video_ids(("123", "123"))
        with self.assertRaisesRegex(VideoPreparationError, "decimal"):
            _normalize_video_ids(("../123",))

    def test_double_encoded_gateway_response_is_decoded(self) -> None:
        payload = {"choices": [{"message": {"content": "ok"}}]}
        encoded = json.dumps(json.dumps(payload)).encode("utf-8")
        self.assertEqual(payload, _decode_json_layers(encoded))

    def test_json_document_accepts_a_fenced_object(self) -> None:
        self.assertEqual(
            {"frames": []},
            _extract_json_document(
                "```json\n{\"frames\": []}\n```",
                "test",
            ),
        )

    def test_base_url_rejects_credentials_and_remote_http(self) -> None:
        with self.assertRaises(VideoPreparationError):
            _normalize_base_url("https://user:secret@example.test/v1")
        with self.assertRaises(VideoPreparationError):
            _normalize_base_url("http://example.test/v1")
        self.assertEqual(
            "https://lum.id/llm/v1",
            _normalize_base_url("https://lum.id/llm/v1/"),
        )

    def test_frame_descriptions_must_align_exactly(self) -> None:
        frames = [
            SampledImage(
                frame_index=0,
                timestamp_seconds=1.25,
                width=640,
                height=480,
                jpeg_bytes=b"jpeg",
            )
        ]
        result = _validate_frame_descriptions(
            {
                "frames": [
                    {
                        "frame_index": 0,
                        "description": "A person stands near a table.",
                        "visible_text": None,
                    }
                ]
            },
            frames,
        )
        self.assertEqual(1.25, result[0]["timestamp_seconds"])

        with self.assertRaisesRegex(
            PreparationProtocolError,
            "indexes",
        ):
            _validate_frame_descriptions(
                {
                    "frames": [
                        {
                            "frame_index": 1,
                            "description": "misaligned",
                            "visible_text": None,
                        }
                    ]
                },
                frames,
            )

    def test_digest_must_be_chronological(self) -> None:
        value = _validate_digest(
            {
                "events": [
                    {
                        "start_seconds": 1,
                        "end_seconds": 2,
                        "description": "First event.",
                    },
                    {
                        "start_seconds": 3,
                        "end_seconds": None,
                        "description": "Second event.",
                    },
                ],
                "summary": "Two events occur.",
            },
            duration_seconds=4,
        )
        self.assertEqual(2, len(value["events"]))

        with self.assertRaisesRegex(
            PreparationProtocolError,
            "chronological",
        ):
            _validate_digest(
                {
                    "events": [
                        {
                            "start_seconds": 3,
                            "end_seconds": None,
                            "description": "Later.",
                        },
                        {
                            "start_seconds": 1,
                            "end_seconds": None,
                            "description": "Earlier.",
                        },
                    ],
                    "summary": "Invalid order.",
                },
                duration_seconds=4,
            )

        with self.assertRaisesRegex(
            PreparationProtocolError,
            "source video ends",
        ):
            _validate_digest(
                {
                    "events": [
                        {
                            "start_seconds": 5,
                            "end_seconds": None,
                            "description": "Out of range.",
                        }
                    ],
                    "summary": "Invalid timestamp.",
                },
                duration_seconds=4,
            )


class AuditedVideoPreparationRecoveryTest(unittest.TestCase):
    model = "qwen-test-versioned"
    video_ids = ("111", "222")

    def _create_checkpoint(
        self,
        root: Path,
        video_directory: Path,
    ) -> Path:
        checkpoint = root / "interrupted-checkpoint"
        object_id = "nextqa-val-111"
        object_directory = checkpoint / object_id
        object_directory.mkdir(parents=True)
        source = video_directory / "111.mp4"
        frame_payload = {
            "schema_version": FRAME_SCHEMA_VERSION,
            "object_id": object_id,
            "source_video_id": "111",
            "source_video_sha256": _sha256_file(source),
            "source_duration_seconds": 1.0,
            "sampling": {
                "method": "uniform-midpoint",
                "frame_count": 1,
                "jpeg_max_dimension": 64,
            },
            "generator": {
                "model": self.model,
                "temperature": 0,
                "prompt_sha256": _sha256_bytes(
                    FRAME_PROMPT.encode("utf-8")
                ),
            },
            "frames": [
                {
                    "frame_index": 0,
                    "timestamp_seconds": 0.5,
                    "width": 64,
                    "height": 48,
                    "description": "A test frame.",
                    "visible_text": None,
                }
            ],
        }
        (object_directory / "sampled_frames.json").write_bytes(
            _json_bytes(frame_payload)
        )
        (object_directory / "multimodal_digest.txt").write_text(
            _digest_text(
                object_id,
                {
                    "events": [
                        {
                            "start_seconds": 0.5,
                            "end_seconds": None,
                            "description": "A test event.",
                        }
                    ],
                    "summary": "A test summary.",
                },
            ),
            encoding="utf-8",
        )
        interruption = {
            "schema_version": (
                "pathfinder.interrupted-representation-prep/v0.1"
            ),
            "status": "INTERRUPTED",
            "model": self.model,
            "expected_object_count": 2,
            "complete_object_count": 1,
            "incomplete_object_directories": [],
            "final_output_created": False,
            "protocol_attempts_for_recovered_objects": (
                "not persisted before interruption"
            ),
        }
        (checkpoint / "INTERRUPTION.json").write_bytes(
            _json_bytes(interruption)
        )
        files = sorted(
            path
            for path in checkpoint.rglob("*")
            if path.is_file()
        )
        checksum_lines = [
            f"{_sha256_file(path)}  {path.relative_to(checkpoint).as_posix()}"
            for path in files
        ]
        (checkpoint / "INTERRUPTED_SHA256SUMS").write_text(
            "\n".join(checksum_lines) + "\n",
            encoding="utf-8",
        )
        return checkpoint

    def _prepare_fixture(self, root: Path) -> tuple[Path, Path]:
        video_directory = root / "videos"
        video_directory.mkdir()
        for video_id in self.video_ids:
            (video_directory / f"{video_id}.mp4").write_bytes(
                f"video-{video_id}".encode("ascii")
            )
        checkpoint = self._create_checkpoint(root, video_directory)
        return video_directory, checkpoint

    def test_reuses_validated_object_and_calls_model_only_for_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            video_directory, checkpoint = self._prepare_fixture(root)
            output = root / "output"
            checkpoint_before = {
                path.relative_to(checkpoint).as_posix(): path.read_bytes()
                for path in checkpoint.rglob("*")
                if path.is_file()
            }
            client = _ScriptedVisionClient(
                [
                    json.dumps(
                        {
                            "frames": [
                                {
                                    "frame_index": 0,
                                    "description": "A new frame.",
                                    "visible_text": None,
                                }
                            ]
                        }
                    ),
                    json.dumps(
                        {
                            "events": [
                                {
                                    "start_seconds": 0.5,
                                    "end_seconds": None,
                                    "description": "A new event.",
                                }
                            ],
                            "summary": "A new summary.",
                        }
                    ),
                ],
                model=self.model,
            )
            audit = audit_recovery_checkpoint(
                root=checkpoint,
                video_directory=video_directory,
                model=self.model,
                frame_count=1,
                jpeg_max_dimension=64,
                video_ids=self.video_ids,
            )
            self.assertEqual("recovery_audit_ok", audit["status"])
            self.assertEqual(1, audit["recovered_object_count"])
            self.assertEqual(1, audit["missing_object_count"])
            self.assertFalse(audit["inference_requests_made"])
            sampled = [
                SampledImage(
                    frame_index=0,
                    timestamp_seconds=0.5,
                    width=64,
                    height=48,
                    jpeg_bytes=b"jpeg",
                )
            ]

            with patch(
                "pathfinder.video_prep.sample_video",
                return_value=(sampled, 1.0),
            ):
                manifest = prepare_representations(
                    video_directory=video_directory,
                    output_directory=output,
                    client=client,  # type: ignore[arg-type]
                    frame_count=1,
                    jpeg_max_dimension=64,
                    video_ids=self.video_ids,
                    resume_from=checkpoint,
                )

            self.assertEqual(2, len(client.calls))
            self.assertEqual(
                ["nextqa-val-111", "nextqa-val-222"],
                [entry["object_id"] for entry in manifest["objects"]],
            )
            recovered = manifest["objects"][0]
            self.assertIsNone(
                recovered["protocol_attempts"]["frame_descriptions"]
            )
            self.assertFalse(
                recovered["recovery"][
                    "historical_protocol_attempts_recorded"
                ]
            )
            self.assertEqual(1, manifest["recovery"]["recovered_object_count"])
            self.assertEqual(
                1,
                manifest["recovery"]["newly_generated_object_count"],
            )
            self.assertTrue((output / "generation-manifest.json").is_file())
            checkpoint_after = {
                path.relative_to(checkpoint).as_posix(): path.read_bytes()
                for path in checkpoint.rglob("*")
                if path.is_file()
            }
            self.assertEqual(checkpoint_before, checkpoint_after)

    def test_checksum_tampering_fails_before_any_model_call(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            video_directory, checkpoint = self._prepare_fixture(root)
            output = root / "output"
            digest = (
                checkpoint
                / "nextqa-val-111"
                / "multimodal_digest.txt"
            )
            digest.write_text(
                digest.read_text(encoding="utf-8") + "tampered\n",
                encoding="utf-8",
            )
            client = _ScriptedVisionClient([], model=self.model)

            with self.assertRaisesRegex(
                VideoPreparationError,
                "checksum mismatch",
            ):
                prepare_representations(
                    video_directory=video_directory,
                    output_directory=output,
                    client=client,  # type: ignore[arg-type]
                    frame_count=1,
                    jpeg_max_dimension=64,
                    video_ids=self.video_ids,
                    resume_from=checkpoint,
                )

            self.assertEqual([], client.calls)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
