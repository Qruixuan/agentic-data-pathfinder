"""Public-only, outcome-blind selection gates for the relational dev pilot."""

import csv
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from experiments.complexity_dev_selection import public_eligibility, select


class ComplexityDevelopmentSelectionTests(unittest.TestCase):
    def test_relational_public_question_threshold(self) -> None:
        options = ["turn left", "turn right", "move forward",
                   "move backward", "stop moving"]
        self.assertTrue(public_eligibility(
            "What did the child do after the person opened the door?",
            options, "temporal"))
        self.assertFalse(public_eligibility(
            "What color is the door?", options, "temporal"))
        self.assertFalse(public_eligibility(
            "What did the child do after the person opened the door?",
            options[:4] + [options[0]], "temporal"))

    def test_answer_column_cannot_change_selected_public_tasks(self) -> None:
        with TemporaryDirectory() as root:
            directory = Path(root)
            media = directory / "media.json"
            media.write_text(json.dumps({"objects": {
                f"nextqa-val-{video}": {"bytes": 2_000_000}
                for video in ("12345678", "12345679", "12345680")
            }}), encoding="utf-8")
            result_sets = []
            for answer in ("0", "4"):
                path = directory / f"{answer}.csv"
                with path.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=[
                        "video", "qid", "type", "question", "answer",
                        "a0", "a1", "a2", "a3", "a4"])
                    writer.writeheader()
                    for video in ("12345678", "12345679", "12345680"):
                        for qid, kind, question in (
                            ("1", "CW", "Why did the person turn after the door opened?"),
                            ("2", "TC", "What did the person do after the door opened?"),
                            ("3", "DC", "What did the person carry while the door opened?"),
                        ):
                            writer.writerow({
                                "video": video, "qid": qid, "type": kind,
                                "question": question, "answer": answer,
                                **{f"a{i}": f"move object {i}" for i in range(5)},
                            })
                protocol = {
                    "schema_version": "pathfinder.complexity-development-selection/v1",
                    "seed": "frozen-synthetic-seed",
                    "official_csv_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "media_inventory_sha256": hashlib.sha256(media.read_bytes()).hexdigest(),
                    "excluded_object_ids": ["nextqa-val-12345680"],
                    "object_count": 2,
                    "strata": ["causal", "temporal", "descriptive"],
                    "min_video_bytes": 1_500_000,
                    "max_video_bytes": 7_000_000,
                    "selection_rule": (
                        "public-relational-question-threshold-plus-seeded-rank-v1"
                    ),
                    "evaluation_role": "development-only",
                    "credentials_recorded": False,
                    "hidden_label_values_included": False,
                }
                output = select(path, media, protocol)
                result_sets.append((output["selected_object_ids"],
                                    output["tasks"]))
            self.assertEqual(result_sets[0], result_sets[1])


if __name__ == "__main__":
    unittest.main()
