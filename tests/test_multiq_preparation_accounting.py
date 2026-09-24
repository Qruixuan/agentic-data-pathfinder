import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import Request

from experiments.multiq_prepare import BudgetedTransport, measured


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


if __name__ == "__main__":
    unittest.main()
