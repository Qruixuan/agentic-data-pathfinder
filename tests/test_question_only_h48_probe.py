"""Offline gates for the bounded H48 question-only development diagnostic."""

import json
from pathlib import Path
import unittest

from experiments.question_only_h48.probe import prompt_for, read_inputs


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = ROOT / "experiments/question_only_h48/PROTOCOL.json"
QUESTIONS = ROOT / "artifacts/h48-runtime-v1/plan/public-questions.jsonl"
COMMITMENT = (
    ROOT / "artifacts/h48-runtime-v1/commitment/"
    "n1-oracle-preselection-commitment.json"
)


class QuestionOnlyH48ProbeTests(unittest.TestCase):
    def test_frozen_public_bindings_and_request_ceiling(self) -> None:
        protocol, rows, commitment = read_inputs(
            PROTOCOL, QUESTIONS, COMMITMENT)
        self.assertEqual(len(rows), protocol["max_n6_requests"])
        self.assertEqual(protocol["max_provider_attempts"], 36)
        self.assertEqual(commitment["label_count"], len(rows))
        self.assertEqual(len({row["object_id"] for row in rows}), 4)
        self.assertEqual(len({row["question_id"] for row in rows}), 12)

    def test_prompt_contains_public_options_without_visual_payload(self) -> None:
        _, rows, _ = read_inputs(PROTOCOL, QUESTIONS, COMMITMENT)
        for row in rows:
            prompt = prompt_for(row)
            self.assertIn(row["question"], prompt)
            self.assertIn("No video, frames, captions, or digest are supplied", prompt)
            self.assertIn("Return exactly one option ID and no other text", prompt)
            for option in row["answer_options"]:
                self.assertIn(
                    f"[{option['option_id']}] {option['text']}", prompt)
            self.assertLess(len(prompt.encode("utf-8")), 65536)

    def test_relational_six_question_protocol_uses_same_probe(self) -> None:
        root = ROOT / "artifacts/relational-development-runtime-20260924-v1"
        protocol, rows, commitment = read_inputs(
            ROOT / "experiments/relational_dev_20260924/question-only-protocol.json",
            root / "plan/public-questions.jsonl",
            root / "commitment/n1-oracle-preselection-commitment.json",
        )
        self.assertEqual(len(rows), 6)
        self.assertEqual(protocol["max_n6_requests"], 6)
        self.assertEqual(protocol["max_provider_attempts"], 18)
        self.assertEqual(commitment["label_count"], 6)
        self.assertEqual(len({row["object_id"] for row in rows}), 2)


if __name__ == "__main__":
    unittest.main()
