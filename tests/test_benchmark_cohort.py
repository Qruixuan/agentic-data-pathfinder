from __future__ import annotations

import contextlib
import csv
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.benchmark_cohort import (
    CohortPreparationError,
    prepare_benchmark_cohort,
)
from pathfinder.cli import main
from pathfinder.video_prep import load_selection_video_ids


HEADERS = (
    "video",
    "frame_count",
    "width",
    "height",
    "question",
    "answer",
    "qid",
    "type",
    "a0",
    "a1",
    "a2",
    "a3",
    "a4",
)


def annotation_rows() -> list[dict[str, str]]:
    rows = []
    for video, kind, answer, qid in (
        ("100", "CW", "0", "1"),
        ("101", "CW", "1", "2"),
        ("101", "CH", "2", "3"),
        ("102", "CH", "2", "4"),
        ("103", "TN", "4", "5"),
        ("104", "TN", "3", "6"),
    ):
        rows.append({
            "video": video,
            "frame_count": "120",
            "width": "640",
            "height": "480",
            "question": f"Question for video {video}?",
            "answer": answer,
            "qid": qid,
            "type": kind,
            "a0": "option zero",
            "a1": "option one",
            "a2": "option two",
            "a3": "option three",
            "a4": "option four",
        })
    return rows


def write_annotations(path: Path, rows: list[dict[str, str]]) -> str:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def selection_config(annotation_sha256: str) -> dict:
    return {
        "schema_version": "pathfinder.benchmark-cohort-selection/v0.1",
        "selection_id": "test-cohort",
        "status": "test",
        "source": {
            "dataset": "NExT-QA",
            "split": "val",
            "repository_commit": "a" * 40,
            "annotation_path": "val.csv",
            "annotation_sha256": annotation_sha256,
        },
        "independent_unit": "source-video",
        "one_primary_question_per_video": True,
        "target_workload_count": 3,
        "quota": [
            {
                "stratum_id": "causal",
                "accepted_upstream_types": ["CW", "CH"],
                "count": 2,
            },
            {
                "stratum_id": "temporal",
                "accepted_upstream_types": ["TN"],
                "count": 1,
            },
        ],
        "selection_rule": {
            "ordering": "official-val-csv-row-order",
            "outcome_blind": True,
        },
        "excluded_video_ids": ["100"],
    }


class BenchmarkCohortTest(unittest.TestCase):
    def prepare_inputs(self, root: Path) -> tuple[Path, Path, dict]:
        annotations = root / "val.csv"
        digest = write_annotations(annotations, annotation_rows())
        payload = selection_config(digest)
        config = root / "selection-config.json"
        config.write_text(json.dumps(payload), encoding="utf-8")
        return config, annotations, payload

    def test_selection_is_deterministic_unique_and_exact_match_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, annotations, _ = self.prepare_inputs(root)
            first = root / "first"
            second = root / "second"
            summary = prepare_benchmark_cohort(
                selection_config=config,
                annotation_csv=annotations,
                output_dir=first,
            )
            prepare_benchmark_cohort(
                selection_config=config,
                annotation_csv=annotations,
                output_dir=second,
            )

            self.assertEqual(3, summary["workload_count"])
            self.assertEqual(3, summary["unique_video_count"])
            self.assertEqual(
                {"causal": 2, "temporal": 1},
                summary["stratum_counts"],
            )
            self.assertEqual(
                ("101", "102", "103"),
                load_selection_video_ids(first / "selection.json"),
            )
            workloads = json.loads(
                (first / "workloads.json").read_text(encoding="utf-8")
            )
            self.assertEqual(3, len(workloads))
            row = workloads["causal-nextqa-val-101-q2"]
            self.assertEqual("B", row["correct_answer_id"])
            self.assertEqual("causal", row["stratum_id"])
            self.assertEqual(
                ["A", "B", "C", "D", "E"],
                [option["option_id"] for option in row["answer_options"]],
            )
            for path in sorted(first.iterdir()):
                self.assertEqual(
                    path.read_bytes(),
                    (second / path.name).read_bytes(),
                    path.name,
                )

    def test_checksums_cover_every_frozen_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, annotations, _ = self.prepare_inputs(root)
            output = root / "cohort"
            prepare_benchmark_cohort(
                selection_config=config,
                annotation_csv=annotations,
                output_dir=output,
            )
            lines = (output / "SHA256SUMS").read_text().splitlines()
            self.assertEqual(4, len(lines))
            for line in lines:
                digest, name = line.split("  ", 1)
                self.assertEqual(
                    digest,
                    hashlib.sha256((output / name).read_bytes()).hexdigest(),
                )

    def test_refuses_hash_drift_and_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, annotations, payload = self.prepare_inputs(root)
            output = root / "cohort"
            prepare_benchmark_cohort(
                selection_config=config,
                annotation_csv=annotations,
                output_dir=output,
            )
            with self.assertRaisesRegex(
                CohortPreparationError, "already exists"
            ):
                prepare_benchmark_cohort(
                    selection_config=config,
                    annotation_csv=annotations,
                    output_dir=output,
                )
            payload["source"]["annotation_sha256"] = "f" * 64
            config.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                CohortPreparationError, "does not match"
            ):
                prepare_benchmark_cohort(
                    selection_config=config,
                    annotation_csv=annotations,
                    output_dir=root / "other",
                )

    def test_refuses_outcome_aware_or_inconsistent_quota_protocol(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, annotations, payload = self.prepare_inputs(root)
            mutations = (
                ("outcome_blind", False, "outcome_blind"),
                ("target_workload_count", 4, "quota counts sum"),
            )
            for name, value, message in mutations:
                with self.subTest(name=name):
                    changed = json.loads(json.dumps(payload))
                    if name == "outcome_blind":
                        changed["selection_rule"][name] = value
                    else:
                        changed[name] = value
                    config.write_text(json.dumps(changed), encoding="utf-8")
                    with self.assertRaisesRegex(
                        CohortPreparationError, message
                    ):
                        prepare_benchmark_cohort(
                            selection_config=config,
                            annotation_csv=annotations,
                            output_dir=root / f"output-{name}",
                        )

    def test_refuses_invalid_answer_and_unfillable_quota(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            rows = annotation_rows()
            rows[1]["answer"] = "B"
            annotations = root / "invalid.csv"
            digest = write_annotations(annotations, rows)
            config = root / "selection-config.json"
            config.write_text(
                json.dumps(selection_config(digest)), encoding="utf-8"
            )
            with self.assertRaisesRegex(
                CohortPreparationError, "integer from 0 to 4"
            ):
                prepare_benchmark_cohort(
                    selection_config=config,
                    annotation_csv=annotations,
                    output_dir=root / "invalid-output",
                )

            digest = write_annotations(annotations, annotation_rows())
            payload = selection_config(digest)
            payload["quota"][1]["count"] = 3
            payload["target_workload_count"] = 5
            config.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                CohortPreparationError, "selected 2 workloads"
            ):
                prepare_benchmark_cohort(
                    selection_config=config,
                    annotation_csv=annotations,
                    output_dir=root / "insufficient-output",
                )

    def test_cli_creates_bound_outputs_and_reports_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, annotations, _ = self.prepare_inputs(root)
            output = root / "cohort"
            args = [
                "prepare-benchmark-cohort",
                "--selection-config",
                str(config),
                "--annotation-csv",
                str(annotations),
                "--output-dir",
                str(output),
                "--compact",
            ]
            with contextlib.redirect_stdout(io.StringIO()) as captured:
                self.assertEqual(0, main(args))
            self.assertEqual("COMPLETE", json.loads(captured.getvalue())["status"])
            with contextlib.redirect_stdout(io.StringIO()) as captured:
                self.assertEqual(2, main(args))
            self.assertIn("already exists", captured.getvalue())


if __name__ == "__main__":
    unittest.main()
