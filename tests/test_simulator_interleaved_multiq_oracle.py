"""N1-only label construction never exports answers in its public report."""

from __future__ import annotations

import csv
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pathfinder.rsi_exam.interleaved_multiq_plan import freeze_interleaved_plan
from pathfinder.rsi_exam.ten_route_multiq_plan import (
    freeze_ten_route_multiq_plan,
)
from pathfinder.simulator.hidden_oracle import (
    build_n1_public_task_binding,
    verify_n1_oracle_package,
)
from pathfinder.simulator.interleaved_multiq_oracle import (
    InterleavedOracleError,
    build_interleaved_n1_oracle,
)


class InterleavedOracleTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        host_patch = patch(
            "pathfinder.simulator.interleaved_multiq_oracle."
            "socket.gethostname", return_value="pathfinder-n1"
        )
        host_patch.start()
        self.addCleanup(host_patch.stop)
        self.questions = []
        self.csv_rows = []
        for video in ("100", "200"):
            for number, stratum in enumerate(
                ("causal", "temporal", "descriptive"), start=1
            ):
                object_id = f"nextqa-val-{video}"
                question_id = f"{object_id}-q{number}"
                question = f"What happens in {question_id}?"
                options = [
                    {"option_id": chr(ord("A") + index),
                     "text": f"option {index} for {question_id}"}
                    for index in range(5)
                ]
                task = build_n1_public_task_binding(
                    workload_id=question_id, object_id=object_id,
                    task_class_id=stratum, question=question,
                    answer_options=options,
                    success_scoring_rule=(
                        "multiple-choice-option-id-canonical-match-v1"
                    ),
                )
                self.questions.append({
                    "question_id": question_id, "object_id": object_id,
                    "stratum": stratum, "question": question,
                    "answer_options": options,
                    "public_task_sha256": task["task_binding_sha256"],
                })
                self.csv_rows.append({
                    "video": video, "qid": str(number), "question": question,
                    "answer": str(number - 1),
                    **{f"a{index}": options[index]["text"]
                       for index in range(5)},
                })
        self.source_sha = hashlib.sha256(b"public-source").hexdigest()
        self.plan = self.root / "plan"
        freeze_interleaved_plan(
            self.questions, seed="n1-private-test-seed",
            experiment_id="n1-private-test-experiment",
            public_source_sha256=self.source_sha, output_dir=self.plan,
        )
        self.csv_path = self.root / "official.csv"
        self._write_csv()

    def _write_csv(self) -> None:
        with self.csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(self.csv_rows[0]))
            writer.writeheader()
            writer.writerows(self.csv_rows)

    def _build(self, output: Path) -> dict:
        return build_interleaved_n1_oracle(
            plan_dir=self.plan, public_questions=self.questions,
            public_source_sha256=self.source_sha,
            official_csv_path=self.csv_path,
            official_csv_sha256=hashlib.sha256(
                self.csv_path.read_bytes()).hexdigest(),
            oracle_id="multiq-n1-test-oracle", output_dir=output,
            private_root=self.root,
        )

    def test_private_package_is_source_bound_and_report_is_label_free(self) -> None:
        output = self.root / "private-n1"
        report = self._build(output)
        self.assertEqual(6, report["label_count"])
        self.assertFalse(report["hidden_label_values_returned"])
        self.assertNotIn("labels", report)
        self.assertEqual(
            6, verify_n1_oracle_package(
                output / "n1-oracle-package"
            )["label_count"],
        )
        with self.assertRaisesRegex(InterleavedOracleError, "already exists"):
            self._build(output)

    def test_public_option_drift_is_rejected_before_writing(self) -> None:
        self.csv_rows[0]["a0"] = "changed option"
        self._write_csv()
        output = self.root / "wrong-private"
        with self.assertRaisesRegex(InterleavedOracleError,
                                    "options differ"):
            self._build(output)
        self.assertFalse(output.exists())

    def test_ten_route_public_plan_reuses_private_oracle_builder(self) -> None:
        selected = [row for row in self.questions if (
            (row["object_id"] == "nextqa-val-100"
             and row["stratum"] in {"causal", "temporal"})
            or (row["object_id"] == "nextqa-val-200"
                and row["stratum"] in {"temporal", "descriptive"})
        )]
        for number, stratum in ((1, "causal"), (3, "descriptive")):
            object_id = "nextqa-val-300"
            question_id = f"{object_id}-q{number}"
            question = f"What happens in {question_id}?"
            options = [{"option_id": chr(65 + index),
                        "text": f"option {index} for {question_id}"}
                       for index in range(5)]
            task = build_n1_public_task_binding(
                workload_id=question_id, object_id=object_id,
                task_class_id=stratum, question=question,
                answer_options=options,
                success_scoring_rule=(
                    "multiple-choice-option-id-canonical-match-v1"
                ),
            )
            selected.append({
                "question_id": question_id, "object_id": object_id,
                "stratum": stratum, "question": question,
                "answer_options": options,
                "public_task_sha256": task["task_binding_sha256"],
            })
            self.csv_rows.append({
                "video": "300", "qid": str(number),
                "question": question, "answer": "0",
                **{f"a{index}": options[index]["text"]
                   for index in range(5)},
            })
        selected_ids = {row["question_id"] for row in selected}
        self.csv_rows = [row for row in self.csv_rows if (
            f"nextqa-val-{row['video']}-q{row['qid']}" in selected_ids
        )]
        self.questions = selected
        self.plan = self.root / "ten-route-plan"
        freeze_ten_route_multiq_plan(
            selected, seed="n1-ten-route-test-seed",
            experiment_id="n1-ten-route-test-experiment",
            public_source_sha256=self.source_sha,
            exposure_inventory_sha256="b" * 64,
            output_dir=self.plan,
        )
        self._write_csv()
        report = self._build(self.root / "private-ten-route")
        self.assertEqual(report["label_count"], 6)
        self.assertFalse(report["hidden_label_values_returned"])


if __name__ == "__main__":
    unittest.main()
