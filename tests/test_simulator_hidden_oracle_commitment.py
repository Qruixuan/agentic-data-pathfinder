from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.hidden_oracle import (
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    build_n1_hidden_label_record,
    build_n1_oracle_package,
    build_n1_public_task_binding,
)
from pathfinder.simulator.hidden_oracle_commitment import (
    CHECKSUMS_NAME,
    COMMITMENT_NAME,
    HiddenOracleCommitmentError,
    freeze_n1_oracle_preselection_commitment,
    verify_n1_oracle_preselection_commitment,
)


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(
        (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    )


class HiddenOracleCommitmentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        public = build_n1_public_task_binding(
            workload_id="visible-workload",
            object_id="visible-object",
            task_class_id="video_qa",
            question="Which action occurs?",
            answer_options=[
                {"option_id": "A", "text": "First."},
                {"option_id": "B", "text": "Second."},
            ],
            success_scoring_rule=(
                "multiple-choice-option-id-canonical-match-v1"
            ),
        )
        label = build_n1_hidden_label_record(
            public,
            correct_answer_id="B",
        )
        source = self.root / "hidden-source.json"
        _write_json(
            source,
            {
                "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
                "oracle_id": "oracle-v1",
                "logical_node_id": "N1",
                "labels": [label],
                "credentials_recorded": False,
            },
        )
        self.package = self.root / "oracle-package"
        build_n1_oracle_package(source, output_dir=self.package)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _freeze(self, name: str) -> Path:
        output = self.root / name
        freeze_n1_oracle_preselection_commitment(
            self.package,
            commitment_id="oracle-preselection-v1",
            output_dir=output,
        )
        return output

    def test_publishes_only_label_free_commitment(self) -> None:
        output = self._freeze("commitment")
        text = (output / COMMITMENT_NAME).read_text(encoding="utf-8")
        self.assertNotIn("correct_answer_id", text)
        self.assertNotIn('"labels"', text)
        self.assertNotIn('"B"', text)
        structural = verify_n1_oracle_preselection_commitment(output)
        self.assertEqual("VERIFIED", structural["status"])
        self.assertFalse(structural["private_package_binding_verified"])
        self.assertFalse(structural["independent_timestamp_attested"])
        self.assertFalse(structural["label_values_returned"])

        opened = verify_n1_oracle_preselection_commitment(
            output,
            oracle_package_dir=self.package,
        )
        self.assertTrue(opened["private_package_binding_verified"])

    def test_freeze_is_deterministic(self) -> None:
        first = self._freeze("first")
        second = self._freeze("second")
        for name in (COMMITMENT_NAME, CHECKSUMS_NAME):
            self.assertEqual(
                (first / name).read_bytes(),
                (second / name).read_bytes(),
            )

    def test_tampering_is_rejected_even_if_file_checksum_is_restamped(self) -> None:
        output = self._freeze("tampered")
        path = output / COMMITMENT_NAME
        document = json.loads(path.read_text(encoding="utf-8"))
        document["label_count"] = 2
        _write_json(path, document)
        (output / CHECKSUMS_NAME).write_text(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {COMMITMENT_NAME}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            HiddenOracleCommitmentError,
            "content digest",
        ):
            verify_n1_oracle_preselection_commitment(output)

    def test_different_private_package_cannot_open_commitment(self) -> None:
        output = self._freeze("commitment")
        source = json.loads(
            (self.package / "hidden-labels.json").read_text(encoding="utf-8")
        )
        changed = copy.deepcopy(source)
        changed["oracle_id"] = "other-oracle"
        changed_source = self.root / "other-source.json"
        _write_json(changed_source, changed)
        other = self.root / "other-package"
        build_n1_oracle_package(changed_source, output_dir=other)
        with self.assertRaisesRegex(
            HiddenOracleCommitmentError,
            "does not open",
        ):
            verify_n1_oracle_preselection_commitment(
                output,
                oracle_package_dir=other,
            )

    def test_existing_output_is_not_overwritten(self) -> None:
        output = self._freeze("commitment")
        with self.assertRaisesRegex(
            HiddenOracleCommitmentError,
            "exists",
        ):
            freeze_n1_oracle_preselection_commitment(
                self.package,
                commitment_id="oracle-preselection-v1",
                output_dir=output,
            )


if __name__ == "__main__":
    unittest.main()
