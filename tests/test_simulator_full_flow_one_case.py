from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pathfinder.simulator.full_flow_local_semantic_admission import (
    ADMISSION_NAME as SOURCE_ADMISSION_NAME,
    CHECKSUMS_NAME as SOURCE_CHECKSUMS_NAME,
    TRIALS_NAME as SOURCE_TRIALS_NAME,
    FrozenLocalSemanticExecutionInputs,
)
from pathfinder.simulator.full_flow_one_case import (
    CHECKSUMS_NAME,
    PLAN_NAME,
    TRIALS_NAME,
    FullFlowOneCaseError,
    freeze_full_flow_one_case_plan,
    verify_full_flow_one_case_plan,
)


SELECTIONS = (
    ("n7-raw", "D0", 0, "N7"),
    ("n7-indexed-raw", "D1", 0, "N7"),
    ("n7-remote-derived", "D2", 0, "N7"),
    ("n7-cache-miss", "D3", 0, "N7"),
    ("n7-cache-hit", "D3", 1, "N7"),
    ("n8-raw", "D4", 0, "N8"),
    ("n8-indexed-raw", "D5", 0, "N8"),
    ("n8-remote-derived", "D6", 0, "N8"),
    ("n8-cache-miss", "D7", 0, "N8"),
    ("n8-cache-hit", "D7", 1, "N8"),
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class FullFlowOneCaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.admission = self.root / "admission"
        self.admission.mkdir()
        for name, payload in (
            (SOURCE_ADMISSION_NAME, b"{}\n"),
            (SOURCE_TRIALS_NAME, b"{}\n"),
            (SOURCE_CHECKSUMS_NAME, b"source checksums\n"),
        ):
            (self.admission / name).write_bytes(payload)
        self.public_task = {
            "schema_version": "pathfinder.n1-public-task-binding/v1alpha1",
            "workload_id": "smoke-causal",
            "object_id": "video-2435100235",
            "question": "What visible action happens?",
            "answer_options": [
                {"option_id": "A", "text": "first action"},
                {"option_id": "B", "text": "second action"},
            ],
            "task_binding_sha256": "b" * 64,
            "credentials_recorded": False,
        }
        trials = []
        for _, design_id, repetition, node_id in SELECTIONS:
            trials.append({
                "trial_key": (
                    "scenario|smoke-causal|"
                    f"{design_id}|r{repetition:04d}"
                ),
                "workload_id": "smoke-causal",
                "design_id": design_id,
                "repetition": repetition,
                "executor_node_id": node_id,
                "route_family": "representative-route",
                "flowmesh_submission_authorized": True,
                "public_task_binding": dict(self.public_task),
                "representation_identities": [{
                    "representation_id": "raw_video",
                }],
                "source_semantic_trial_sha256": (
                    f"{int(design_id[1:]) + repetition + 1:064x}"
                ),
            })
        self.inputs = FrozenLocalSemanticExecutionInputs(
            admission={
                "promotion_id": "promotion-v1",
                "admission_sha256": "a" * 64,
            },
            bound_trials=tuple(trials),
            bound_stages=(),
            representative_smokes=(),
            adapter_inventory={},
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _loader(self, inputs: FrozenLocalSemanticExecutionInputs | None = None):
        return mock.patch(
            "pathfinder.simulator.full_flow_one_case."
            "load_full_flow_local_semantic_execution_inputs",
            return_value=inputs or self.inputs,
        )

    def _freeze(self, output: Path) -> dict:
        with self._loader():
            return freeze_full_flow_one_case_plan(
                self.admission,
                case_id="causal-one-case-v1",
                workload_id="smoke-causal",
                safe_design_id="D0",
                output_dir=output,
            )

    def test_freezes_one_public_task_across_ten_paths(self) -> None:
        output = self.root / "plan"
        report = self._freeze(output)
        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(10, report["trial_count"])
        self.assertEqual(
            [f"D{index}" for index in range(8)],
            report["design_ids"],
        )
        plan = json.loads((output / PLAN_NAME).read_text(encoding="utf-8"))
        rows = [
            json.loads(line)
            for line in (output / TRIALS_NAME)
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertEqual(
            [case_id for case_id, *_ in SELECTIONS],
            [row["case_id"] for row in rows],
        )
        self.assertEqual(
            rows[3]["trial_key"], rows[4]["prerequisite_trial_key"]
        )
        self.assertEqual(
            rows[8]["trial_key"], rows[9]["prerequisite_trial_key"]
        )
        serialized = json.dumps({"plan": plan, "rows": rows})
        self.assertNotIn("correct_answer_id", serialized)
        self.assertFalse(plan["workflow_submitted"])
        self.assertFalse(plan["llm_called"])
        self.assertFalse(plan["eligible_for_scientific_claims"])

    def test_freeze_is_byte_deterministic(self) -> None:
        outputs = [self.root / "first", self.root / "second"]
        for output in outputs:
            self._freeze(output)
        self.assertEqual(
            {path.name: path.read_bytes() for path in outputs[0].iterdir()},
            {path.name: path.read_bytes() for path in outputs[1].iterdir()},
        )

    def test_restamped_tampering_still_fails_source_rederivation(self) -> None:
        output = self.root / "plan"
        self._freeze(output)
        plan = json.loads((output / PLAN_NAME).read_text(encoding="utf-8"))
        plan["safe_design_id"] = "D1"
        unsigned = dict(plan)
        del unsigned["plan_sha256"]
        plan["plan_sha256"] = _sha256(_canonical(unsigned))
        (output / PLAN_NAME).write_text(
            json.dumps(plan, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        checksum_rows = []
        for name in sorted((PLAN_NAME, TRIALS_NAME)):
            checksum_rows.append(
                f"{_sha256((output / name).read_bytes())}  {name}\n"
            )
        (output / CHECKSUMS_NAME).write_text(
            "".join(checksum_rows), encoding="utf-8"
        )
        with self._loader(), self.assertRaises(FullFlowOneCaseError):
            verify_full_flow_one_case_plan(
                output,
                local_semantic_admission_dir=self.admission,
            )

    def test_hidden_label_in_public_binding_is_rejected(self) -> None:
        changed_trials = []
        for trial in self.inputs.bound_trials:
            changed = dict(trial)
            task = dict(changed["public_task_binding"])
            task["correct_answer_id"] = "A"
            changed["public_task_binding"] = task
            changed_trials.append(changed)
        changed_inputs = FrozenLocalSemanticExecutionInputs(
            admission=self.inputs.admission,
            bound_trials=tuple(changed_trials),
            bound_stages=(),
            representative_smokes=(),
            adapter_inventory={},
        )
        with self._loader(changed_inputs), self.assertRaises(
            FullFlowOneCaseError
        ):
            freeze_full_flow_one_case_plan(
                self.admission,
                case_id="causal-one-case-v1",
                workload_id="smoke-causal",
                safe_design_id="D0",
                output_dir=self.root / "rejected",
            )


if __name__ == "__main__":
    unittest.main()
