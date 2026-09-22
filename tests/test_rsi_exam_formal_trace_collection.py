from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pathfinder.rsi_exam.formal_trace_collection import (
    CHECKSUMS_NAME,
    PROGRESS_NAME,
    RECEIPT_NAME,
    load_formal_trace_collection_units,
    run_formal_trace_collection,
    verify_formal_trace_collection,
)


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )


class FormalTraceCollectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.plan = self.root / "collection-plan"
        self.plan.mkdir()
        _write_json(
            self.plan / "collection-manifest.json",
            {
                "cohort_id": "formal-cohort-v1",
                "collection_repetitions": 2,
                "operation_count": 40,
            },
        )
        cases = [
            {
                "case_id": "video-a",
                "object_id": "video-a",
                "workload_id": "workload-a",
                "split": "train",
                "stratum": "temporal",
            },
            {
                "case_id": "video-b",
                "object_id": "video-b",
                "workload_id": "workload-b",
                "split": "test",
                "stratum": "causal",
            },
        ]
        (self.plan / "selected-cases.jsonl").write_bytes(
            b"".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
                + b"\n"
                for row in cases
            )
        )
        (self.plan / "SHA256SUMS").write_text(
            "frozen-plan-checksums\n", encoding="utf-8", newline="\n"
        )
        self.one_case_plans = []
        for suffix in ("a", "b"):
            directory = self.root / f"one-case-{suffix}"
            directory.mkdir()
            _write_json(
                directory / "full-flow-one-case-plan.json",
                {
                    "artifact_object_id": f"video-{suffix}",
                    "workload_id": f"workload-{suffix}",
                    "plan_sha256": suffix * 64,
                },
            )
            self.one_case_plans.append(directory)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _plan_report(*args, **kwargs):
        del args, kwargs
        return {"status": "VERIFIED", "cohort_id": "formal-cohort-v1"}

    @staticmethod
    def _one_case_report(root, **kwargs):
        del kwargs
        plan = json.loads(
            (Path(root) / "full-flow-one-case-plan.json").read_text()
        )
        return {
            "status": "VERIFIED",
            "artifact_object_id": plan["artifact_object_id"],
        }

    @staticmethod
    def _unit_report(unit):
        digest_character = format((unit.ordinal % 15) + 1, "x")
        return {
            "status": "VERIFIED",
            "run_id": unit.run_id,
            "smoke_count": 10,
            "one_case_execution_complete": True,
            "one_case_artifact_object_id": unit.object_id,
            "receipt_sha256": digest_character * 64,
        }

    def _patch_inputs(self):
        return (
            patch(
                "pathfinder.rsi_exam.formal_trace_collection."
                "verify_collection_plan",
                side_effect=self._plan_report,
            ),
            patch(
                "pathfinder.rsi_exam.formal_trace_collection."
                "verify_full_flow_one_case_plan",
                side_effect=self._one_case_report,
            ),
        )

    def test_schedule_is_serial_and_uses_unique_cache_namespaces(self) -> None:
        first, second = self._patch_inputs()
        with first, second:
            report, units = load_formal_trace_collection_units(
                self.plan,
                self.one_case_plans,
                local_semantic_admission_dir=self.root / "admission",
                collection_id="formal-run-v1",
            )

        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(4, len(units))
        self.assertEqual([0, 1, 0, 1], [unit.repetition for unit in units])
        self.assertEqual(4, len({unit.run_id for unit in units}))
        self.assertEqual(4, len({unit.cache_namespace for unit in units}))

    def test_collection_resumes_after_last_verified_unit(self) -> None:
        output = self.root / "execution"
        first_attempt = []

        def fail_second(unit, target):
            first_attempt.append(unit.ordinal)
            if unit.ordinal == 1:
                raise RuntimeError("stop after first durable unit")
            target.mkdir()
            return self._unit_report(unit)

        def verify(unit, target):
            self.assertTrue(target.is_dir())
            return self._unit_report(unit)

        first, second = self._patch_inputs()
        with first, second, self.assertRaisesRegex(RuntimeError, "durable"):
            run_formal_trace_collection(
                self.plan,
                self.one_case_plans,
                local_semantic_admission_dir=self.root / "admission",
                collection_id="formal-run-v1",
                output_dir=output,
                execute_unit=fail_second,
                verify_unit=verify,
            )
        self.assertEqual([0, 1], first_attempt)
        progress = json.loads((output / PROGRESS_NAME).read_text())
        self.assertEqual(1, progress["completed_unit_count"])

        resumed = []

        def execute(unit, target):
            resumed.append(unit.ordinal)
            target.mkdir()
            return self._unit_report(unit)

        first, second = self._patch_inputs()
        with first, second:
            report = run_formal_trace_collection(
                self.plan,
                self.one_case_plans,
                local_semantic_admission_dir=self.root / "admission",
                collection_id="formal-run-v1",
                output_dir=output,
                execute_unit=execute,
                verify_unit=verify,
            )

        self.assertEqual([1, 2, 3], resumed)
        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(40, report["route_operation_count"])
        self.assertEqual(4, report["cache_namespace_count"])
        self.assertTrue((output / RECEIPT_NAME).is_file())
        self.assertTrue((output / CHECKSUMS_NAME).is_file())

        first, second = self._patch_inputs()
        with first, second:
            verified = verify_formal_trace_collection(
                output,
                collection_plan_dir=self.plan,
                one_case_plan_dirs=self.one_case_plans,
                local_semantic_admission_dir=self.root / "admission",
                collection_id="formal-run-v1",
                verify_unit=verify,
            )
        self.assertEqual(report, verified)


if __name__ == "__main__":
    unittest.main()
