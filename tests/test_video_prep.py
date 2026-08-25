from __future__ import annotations

import json
import unittest
from pathlib import Path

from pathfinder.video_prep import (
    DIGEST_PROMPT,
    FORMAL_VIDEO_IDS,
    FRAME_PROMPT,
    PreparationProtocolError,
    SampledImage,
    VideoPreparationError,
    _decode_json_layers,
    _extract_json_document,
    _normalize_base_url,
    _normalize_video_ids,
    _sha256_bytes,
    _validate_digest,
    _validate_frame_descriptions,
    load_selection_video_ids,
)


ROOT = Path(__file__).resolve().parents[1]


class VideoPreparationParsingTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
