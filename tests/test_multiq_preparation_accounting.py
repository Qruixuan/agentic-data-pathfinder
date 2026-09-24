import io
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import Request
import zlib

from experiments.multiq_prepare import (
    BudgetedTransport, freeze_cohort_media, measured, offline, verify_sums,
)


class PreparationAccountingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    def response(self):
        return io.BytesIO(json.dumps({
            "id": "provider-id", "usage": {"prompt_tokens": 3},
            "choices": [{"message": {"content": "caption",
                                      "reasoning_content": "NEVER-PERSIST"}}],
        }).encode())

    def request(self):
        return Request("https://example.invalid/embeddings", data=b"{}",
                       headers={"Authorization": "Bearer NEVER-LOG"})

    def test_usage_before_validation_and_no_secrets(self):
        transport = BudgetedTransport(self.root, "captions", 1)
        with patch("urllib.request.urlopen", return_value=self.response()):
            result = transport(self.request(), 1)
        all_text = b"".join(p.read_bytes() for p in self.root.iterdir())
        self.assertNotIn(b"NEVER", all_text)
        self.assertNotIn(b"provider-id", all_text)
        self.assertNotIn(b"reasoning_content", result)
        events = [json.loads(line) for line in (
            self.root / "captions-attempts.jsonl").read_bytes().splitlines()]
        self.assertEqual([x["event"] for x in events], ["started", "response", "persisted"])
        self.assertEqual(events[1]["usage"]["prompt_tokens"], 3)
        with patch("urllib.request.urlopen") as urlopen:
            with self.assertRaisesRegex(ValueError, "ceiling"):
                transport(self.request(), 1)
            urlopen.assert_not_called()

    def test_embedding_response_reused(self):
        transport = BudgetedTransport(self.root, "query", 2)
        with patch("urllib.request.urlopen", return_value=self.response()) as call:
            first = transport(self.request(), 1)
            second = transport(self.request(), 1)
            self.assertEqual(first, second)
            self.assertEqual(call.call_count, 1)

    def test_failed_attempt_remains_in_budget_and_no_exception_secret(self):
        transport = BudgetedTransport(self.root, "captions", 1)
        with patch("urllib.request.urlopen", side_effect=OSError("NEVER-PRINT")):
            with self.assertRaisesRegex(RuntimeError, "sanitized journal"):
                transport(self.request(), 1)
        text = (self.root / "captions-attempts.jsonl").read_text()
        self.assertNotIn("NEVER", text)
        self.assertIn('"possible_provider_charge": true', text)

    def test_machine_interval_recorded_even_on_failure(self):
        with self.assertRaises(ValueError):
            with measured(self.root, "decode", "N5"):
                raise ValueError("not written")
        rows = [json.loads(line) for line in (
            self.root / "build-events.jsonl").read_bytes().splitlines()]
        self.assertEqual(rows[-1]["status"], "FAILED")
        self.assertGreaterEqual(rows[-1]["wall_seconds"], 0)
        self.assertEqual(rows[-1]["physical_host"], "N5")

    def test_short_source_commit_fails_before_creating_output(self):
        target = self.root / "unused-output"
        with self.assertRaisesRegex(ValueError, "full Git SHA-1"):
            offline(self.root, target, "0c4f7f8", "workstation")
        self.assertFalse(target.exists())

    def test_public_cohort_media_adapter_binds_exact_bytes(self):
        cohort = self.root / "cohort"
        (cohort / "media").mkdir(parents=True)
        video = b"synthetic-video-bytes"
        (cohort / "media" / "123.mp4").write_bytes(video)
        selection = {
            "development_object_ids": ["nextqa-val-123"],
            "test_object_ids": [],
            "video_media": {"123": {
                "bytes": len(video),
                "sha256": hashlib.sha256(video).hexdigest(),
                "crc32": zlib.crc32(video) & 0xFFFFFFFF,
                "archive_entry": "NExTVideo/123.mp4",
            }},
            "label_values_included": False,
            "credentials_recorded": False,
            "quality_outcomes_used_for_test_selection": False,
        }
        raw = json.dumps(selection).encode()
        (cohort / "selection.json").write_bytes(raw)
        with (cohort / "SHA256SUMS").open("wb") as handle:
            for name in ("selection.json", "media/123.mp4"):
                digest = hashlib.sha256((cohort / name).read_bytes()).hexdigest()
                handle.write(f"{digest}  {name}\n".encode())
        output = self.root / "adapted"
        report = freeze_cohort_media(cohort, output)
        self.assertEqual(report["object_count"], 1)
        verify_sums(output)
        self.assertEqual((output / "123.mp4").read_bytes(), video)
        media = json.loads((output / "media.json").read_bytes())
        self.assertFalse(media["credentials_recorded"])
        self.assertEqual(media["objects"][0]["sha256"],
                         hashlib.sha256(video).hexdigest())


if __name__ == "__main__":
    unittest.main()
