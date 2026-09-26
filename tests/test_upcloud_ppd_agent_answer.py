"""Focused checks for public PPD Agent answer closure formatting."""

from __future__ import annotations

import unittest
from pathlib import Path

from experiments.upcloud_ppd_20260925.run_engineering_session import (
    _explicit_final_option_format,
)


class PpdAgentAnswerFormatTests(unittest.TestCase):
    def test_bare_option_remains_the_preferred_format(self) -> None:
        self.assertEqual(_explicit_final_option_format(" C\n"), "bare-option")

    def test_unambiguous_markdown_final_line_is_accepted(self) -> None:
        answer = (
            "The visual evidence is sufficient.\n"
            "The observed count matches one listed response.\n"
            "**C**"
        )
        self.assertEqual(
            _explicit_final_option_format(answer),
            "markdown-bold-final-line",
        )

    def test_ambiguous_or_nonfinal_markers_fail_closed(self) -> None:
        rejected = (
            "The answer is C",
            "C because the image shows it",
            "**C**\nMore commentary",
            "A is possible, but\n**C**",
            "The response could be C or D.\n**C**",
            "**c**",
            "[C]",
            "",
        )
        for answer in rejected:
            with self.subTest(answer=answer):
                self.assertIsNone(_explicit_final_option_format(answer))

    def test_letter_only_prompt_preserves_public_question_and_options(self) -> None:
        root = Path(__file__).resolve().parents[1]
        prompt_dir = root / "experiments" / "upcloud_ppd_20260925"
        original = (prompt_dir / "engineering-q5.txt").read_text(
            encoding="utf-8"
        ).strip()
        updated = (prompt_dir / "engineering-q5-letter-only-v2.txt").read_text(
            encoding="utf-8"
        ).strip()
        self.assertTrue(updated.startswith(original + "\n\n"))
        suffix = updated[len(original) + 2:]
        self.assertIn("After completing the required tool calls", suffix)
        self.assertIn("final message must be exactly", suffix)
        self.assertIn("Do not include Markdown", suffix)


if __name__ == "__main__":
    unittest.main()
