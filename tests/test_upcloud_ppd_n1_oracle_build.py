"""Synthetic-only tests for the protected N1 PPD oracle builder."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from experiments.upcloud_ppd_20260925.build_oracle_n1 import (
    _official_match,
    build_private_ppd_oracle,
)
from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding


class ProtectedOracleBuildTests(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.private = self.root / "private"
        self.private.mkdir()
        self.public = self.root / "public"
        self.public.mkdir()
        self.task = build_n1_public_task_binding(
            workload_id="synthetic-ppd-q5",
            object_id="nextqa-val-11584566583",
            task_class_id="video_qa",
            question="How many people are speaking on the microphone?",
            answer_options=[
                {"option_id": key, "text": text}
                for key, text in zip(
                    "ABCDE", ("one", "five", "nine", "six", "three")
                )
            ],
            success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        )
        self.task_path = self.public / "task.json"
        self.task_path.write_text(json.dumps(self.task), encoding="utf-8")
        self.csv = self.private / "official.csv"
        self.csv.write_text(
            "video,qid,question,answer,a0,a1,a2,a3,a4\n"
            "11584566583,5,How many people are speaking on the microphone?,"
            "2,one,five,nine,six,three\n",
            encoding="utf-8",
        )

    def _build(self):
        with patch(
            "experiments.upcloud_ppd_20260925.build_oracle_n1.socket.gethostname",
            return_value="pathfinder-n1",
        ):
            return build_private_ppd_oracle(
                public_task_path=self.task_path,
                official_csv_path=self.csv,
                official_csv_sha256=hashlib.sha256(self.csv.read_bytes()).hexdigest(),
                private_root=self.private,
                output_dir=self.private / "new-oracle",
                public_commitment_dir=self.public / "commitment",
                oracle_id="synthetic-ppd-q5-oracle",
            )

    def test_builds_one_private_package_and_public_commitment(self) -> None:
        report = self._build()
        self.assertEqual(report["status"], "VERIFIED_PRIVATE_PPD_N1_ORACLE")
        self.assertEqual(report["label_count"], 1)
        self.assertFalse(report["hidden_label_values_returned"])
        self.assertFalse(report["credentials_recorded"])
        self.assertTrue(
            (self.private / "new-oracle/n1-oracle-package/hidden-labels.json").is_file()
        )
        public_bytes = b"".join(
            path.read_bytes() for path in (self.public / "commitment").iterdir()
        )
        self.assertNotIn(b"correct_answer_id", public_bytes)
        self.assertNotIn(b"hidden-labels.json", public_bytes)

    def test_rejects_nonunique_public_match(self) -> None:
        doubled = self.csv.read_bytes() + self.csv.read_bytes().split(b"\n", 1)[1]
        with self.assertRaisesRegex(ValueError, "do not match uniquely"):
            _official_match(doubled, self.task)

    def test_refuses_non_n1_host(self) -> None:
        with patch(
            "experiments.upcloud_ppd_20260925.build_oracle_n1.socket.gethostname",
            return_value="pathfinder-n7",
        ):
            with self.assertRaisesRegex(ValueError, "only be built on N1"):
                build_private_ppd_oracle(
                    public_task_path=self.task_path,
                    official_csv_path=self.csv,
                    official_csv_sha256="0" * 64,
                    private_root=self.private,
                    output_dir=self.private / "new-oracle",
                    public_commitment_dir=self.public / "commitment",
                    oracle_id="synthetic-ppd-q5-oracle",
                )


if __name__ == "__main__":
    unittest.main()
