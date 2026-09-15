from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.data_agent_semantic_vertical import (
    DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
)
from pathfinder.simulator.full_flow_tasks import (
    FullFlowTaskPlaneError,
    build_full_flow_task_plane,
    verify_full_flow_task_plane,
)


def _write_json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _spec(
    *,
    workload_id: str = "visible-workload-1",
    object_id: str = "visible-object-1",
    correct: str = "B",
    question: str = "Which visible action occurs?",
) -> dict[str, object]:
    artifact = b"bundle-placeholder"
    return {
        "schema_version": DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
        "semantic_run_id": "semantic-run-1",
        "trial_key": f"scenario|{workload_id}|D2|r0000",
        "semantic_executor_node_id": "N6",
        "representation_id": "sampled_frame_bundle",
        "data_agent_route_design_id": "D2",
        "data_agent_plan_id": "D2",
        "data_agent_plan_epoch": 1,
        "workload_id": workload_id,
        "task_class_id": "video_qa",
        "artifact_object_id": object_id,
        "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
        "artifact_size_bytes": len(artifact),
        "object_catalog_version": "catalog-v1",
        "question": question,
        "success_scoring_rule": (
            "multiple-choice-option-id-canonical-match-v1"
        ),
        "answer_options": [
            {"option_id": "A", "text": "First action."},
            {"option_id": "B", "text": "Second action."},
        ],
        "correct_answer_id": correct,
        "expected_model": "vision-model-test",
        "credentials_recorded": False,
    }


class FullFlowTaskPlaneTest(unittest.TestCase):
    def test_builds_disjoint_public_and_n1_private_packages(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = _write_json(root / "first.json", _spec())
            second = _write_json(
                root / "second.json",
                _spec(
                    workload_id="visible-workload-2",
                    object_id="visible-object-2",
                    correct="A",
                ),
            )
            output = root / "tasks"
            result = build_full_flow_task_plane(
                [second, first],
                task_plane_id="task-plane-v1",
                oracle_id="hidden-oracle-v1",
                output_dir=output,
            )
            self.assertEqual("FROZEN_PUBLIC_PRIVATE_TASK_PLANE", result["status"])

            verified = verify_full_flow_task_plane(output)
            self.assertEqual("VERIFIED", verified["status"])
            self.assertEqual(2, verified["public_task_count"])
            self.assertEqual(2, verified["hidden_label_count"])
            self.assertTrue(verified["public_private_separation_verified"])

            public_text = (output / "public/public-tasks.json").read_text()
            self.assertNotIn("correct_answer_id", public_text)
            self.assertNotIn('"labels"', public_text)
            hidden_text = (
                output / "n1-private/hidden-label-source.json"
            ).read_text()
            self.assertIn("correct_answer_id", hidden_text)

    def test_duplicate_design_specs_are_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_value = _spec()
            second_value = dict(first_value)
            second_value["trial_key"] = "scenario|visible-workload-1|D6|r0000"
            second_value["data_agent_route_design_id"] = "D6"
            second_value["data_agent_plan_id"] = "D6"
            first = _write_json(root / "first.json", first_value)
            second = _write_json(root / "second.json", second_value)
            output = root / "tasks"
            result = build_full_flow_task_plane(
                [first, second],
                task_plane_id="task-plane-v1",
                oracle_id="hidden-oracle-v1",
                output_dir=output,
            )
            self.assertEqual(1, result["public_task_count"])
            self.assertEqual(1, result["hidden_label_count"])

    def test_disagreeing_hidden_labels_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = _write_json(root / "first.json", _spec(correct="A"))
            second = _write_json(root / "second.json", _spec(correct="B"))
            with self.assertRaisesRegex(
                FullFlowTaskPlaneError,
                "disagree for hidden label",
            ):
                build_full_flow_task_plane(
                    [first, second],
                    task_plane_id="task-plane-v1",
                    oracle_id="hidden-oracle-v1",
                    output_dir=root / "tasks",
                )

    def test_public_tampering_is_detected_even_after_checksum_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = _write_json(root / "spec.json", _spec())
            output = root / "tasks"
            build_full_flow_task_plane(
                [source],
                task_plane_id="task-plane-v1",
                oracle_id="hidden-oracle-v1",
                output_dir=output,
            )
            public_path = output / "public/public-tasks.json"
            public = json.loads(public_path.read_text())
            public["tasks"][0]["question"] = "Tampered question"
            _write_json(public_path, public)
            checksum_lines = []
            for path in sorted(
                item for item in output.rglob("*")
                if item.is_file() and item.name != "SHA256SUMS"
            ):
                name = path.relative_to(output).as_posix()
                checksum_lines.append(
                    f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {name}"
                )
            (output / "SHA256SUMS").write_text(
                "\n".join(checksum_lines) + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(FullFlowTaskPlaneError):
                verify_full_flow_task_plane(output)


if __name__ == "__main__":
    unittest.main()
