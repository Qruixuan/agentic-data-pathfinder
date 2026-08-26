from __future__ import annotations

import contextlib
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.awm import (
    AWMConfigError,
    audit_awm_workload_heterogeneity,
    load_workload_heterogeneity_audit_config,
)
from pathfinder.cli import main as cli_main


ROOT = Path(__file__).resolve().parents[1]
SYSTEM_PATH = ROOT / "configs" / "multi_candidate_formal_v2_system.json"
COMMITTED_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v2_awm_v3alpha5_heterogeneity.json"
)
SAFE = "D_origin_remote"
CANDIDATE = "D_local_digest"
WORKLOAD_GROUPS = {
    "temporal": ("temporal-w1", "temporal-w2"),
    "causal": ("causal-w1", "causal-w2"),
}


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _pilot_config(root: Path) -> Path:
    path = root / "pilot.json"
    workloads = [
        {
            "id": workload_id,
            "object_id": f"object-{workload_id}",
            "question": f"Question for {workload_id}?",
            "accepted_answer_substrings": ["answer"],
        }
        for workload_ids in WORKLOAD_GROUPS.values()
        for workload_id in workload_ids
    ]
    _write_json(path, {
        "schema_version": "pathfinder.flowmesh-pilot/v1alpha1",
        "experiment_id": "heterogeneity-test",
        "system_config": str(SYSTEM_PATH),
        "design_ids": [SAFE, CANDIDATE],
        "task_class_id": "video_qa",
        "quote_profile_ids": ["as_designed"],
        "latency_multipliers": [1],
        "repetitions": 4,
        "base_seed": 10,
        "randomization_seed": 20,
        "workloads": workloads,
    })
    return path


def _oracle_config(root: Path, pilot: Path) -> Path:
    path = root / "oracle.json"
    _write_json(path, {
        "schema_version": "pathfinder.reduced-oracle/v1alpha1",
        "oracle_id": "heterogeneity-test-oracle",
        "workload_pilot_config": str(pilot),
        "safe_design_id": SAFE,
        "design_order": [SAFE, CANDIDATE],
        "quote_profile_id": "as_designed",
        "latency_multiplier": 1,
        "repetitions": 4,
        "base_seed": 10,
        "randomization_seed": 20,
        "horizon_sessions": 100,
        "horizon_hours": 1,
        "minimum_completion_rate": 1,
        "materialization_root": "materialized",
        "cost_model_status": "synthetic-unit-test",
        "transition_cost": {
            "copy_cost_per_gib": 0,
            "elapsed_time_cost_per_second": 0,
            "foreground_loss_per_transition": 0,
            "storage_cost_per_gib_hour": 0,
        },
        "naive_baseline": {
            "candidate_design_id": CANDIDATE,
            "representation_id": "multimodal_digest",
            "decision_margin": 0,
        },
        "designs": [
            {
                "design_id": SAFE,
                "materialization_decision": "reuse",
                "placement_decision": "origin",
                "execution_decision": "flowmesh",
                "materializations": [],
            },
            {
                "design_id": CANDIDATE,
                "materialization_decision": "copy",
                "placement_decision": "local",
                "execution_decision": "flowmesh",
                "materializations": [],
            },
        ],
    })
    return path


def _audit_config(root: Path) -> Path:
    path = root / "audit.json"
    _write_json(path, {
        "schema_version": "pathfinder.awm-heterogeneity/v1alpha1",
        "audit_id": "heterogeneity-unit-test",
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "safe_design_id": SAFE,
        "design_ids": [SAFE, CANDIDATE],
        "training_repetitions": [0, 1],
        "evaluation_repetitions": [2, 3],
        "workload_groups": {
            group: list(workloads)
            for group, workloads in WORKLOAD_GROUPS.items()
        },
        "policy": {
            "minimum_training_workloads_per_group": 1,
            "minimum_candidate_mean_gain": 0,
            "minimum_candidate_positive_fraction": 0.6,
        },
    })
    return path


def _record(
    design_id: str,
    workload_id: str,
    repetition: int,
    *,
    evaluation_drift: bool,
) -> dict[str, object]:
    group = workload_id.split("-", 1)[0]
    if design_id == SAFE:
        success = True
        cost = 1.0
        representation = "sampled_frames"
    else:
        success = group == "temporal"
        if evaluation_drift and repetition >= 2:
            success = not success
        cost = 0.2
        representation = "multimodal_digest"
    return {
        "trial_key": (
            f"{workload_id}|{design_id}|1|r{repetition:04d}"
        ),
        "workload_id": workload_id,
        "design_id": design_id,
        "task_class_id": "video_qa",
        "quote_profile_id": "as_designed",
        "repetition": repetition,
        "seed": 10 + repetition,
        "latency_multiplier": 1,
        "outcome_type": "completed",
        "telemetry_complete": True,
        "task_success": success,
        "selected_representations": [representation],
        "access_events": [{
            "accepted": True,
            "representation_id": representation,
            "realized_cost": cost,
            "artifact_download_request_count": 0,
            "artifact_full_download_count": 0,
            "artifact_bytes_sent": 0,
        }],
    }


def _oracle_output(root: Path, *, evaluation_drift: bool = False) -> Path:
    output = root / "oracle-output"
    for design_id in (SAFE, CANDIDATE):
        records = [
            _record(
                design_id,
                workload_id,
                repetition,
                evaluation_drift=evaluation_drift,
            )
            for workload_ids in WORKLOAD_GROUPS.values()
            for workload_id in workload_ids
            for repetition in range(4)
        ]
        path = output / "designs" / design_id / "runs.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(
                json.dumps(record, sort_keys=True) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )
    table = output / "oracle_table.csv"
    with table.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "design_id",
                "storage_cost",
                "forward_transition_cost",
                "restoration_cost",
            ),
        )
        writer.writeheader()
        for design_id in (SAFE, CANDIDATE):
            writer.writerow({
                "design_id": design_id,
                "storage_cost": 0,
                "forward_transition_cost": 0,
                "restoration_cost": 0,
            })
    return output


class WorkloadHeterogeneityConfigTest(unittest.TestCase):
    def test_committed_v2_gate_is_posthoc_and_partitions_16_workloads(
        self,
    ) -> None:
        config = load_workload_heterogeneity_audit_config(COMMITTED_CONFIG)
        self.assertTrue(config.posthoc)
        self.assertFalse(config.eligible_for_scientific_claims)
        self.assertEqual(16, len(config.workload_to_group))
        self.assertEqual((0, 1), config.training_repetitions)
        self.assertEqual((2, 3), config.evaluation_repetitions)

    def test_config_rejects_overlapping_workload_groups(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _audit_config(root)
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["workload_groups"]["causal"].append("temporal-w1")
            _write_json(path, payload)
            with self.assertRaisesRegex(AWMConfigError, "cannot overlap"):
                load_workload_heterogeneity_audit_config(path)

    def test_config_cannot_relabel_posthoc_output_as_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _audit_config(root)
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["eligible_for_scientific_claims"] = True
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "eligible_for_scientific_claims=false",
            ):
                load_workload_heterogeneity_audit_config(path)


class WorkloadHeterogeneityAuditTest(unittest.TestCase):
    def _run(
        self,
        root: Path,
        *,
        evaluation_drift: bool = False,
    ) -> tuple[dict[str, object], Path]:
        pilot = _pilot_config(root)
        oracle = _oracle_config(root, pilot)
        config = _audit_config(root)
        source = _oracle_output(
            root,
            evaluation_drift=evaluation_drift,
        )
        output = root / "audit-output"
        result = audit_awm_workload_heterogeneity(
            config,
            oracle,
            oracle_output_dir=source,
            output_dir=output,
        )
        return result, output

    def test_group_policy_uses_candidate_and_safe_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result, output = self._run(Path(temporary))
            self.assertEqual("COMPLETE", result["status"])
            evaluation = json.loads(
                (output / "heterogeneity_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertFalse(evaluation["eligible_for_scientific_claims"])
            self.assertFalse(
                evaluation["selection_uses_evaluation_repetitions"]
            )
            with (output / "policy_assignments.csv").open(
                encoding="utf-8",
                newline="",
            ) as handle:
                rows = list(csv.DictReader(handle))
            selected = {
                row["workload_id"]: row[
                    "group_policy_selected_design_id"
                ]
                for row in rows
            }
            self.assertEqual(CANDIDATE, selected["temporal-w1"])
            self.assertEqual(CANDIDATE, selected["temporal-w2"])
            self.assertEqual(SAFE, selected["causal-w1"])
            self.assertEqual(SAFE, selected["causal-w2"])

    def test_evaluation_drift_cannot_change_policy_selection(self) -> None:
        with tempfile.TemporaryDirectory() as first_temporary:
            _, first_output = self._run(Path(first_temporary))
            with (first_output / "policy_assignments.csv").open(
                encoding="utf-8",
                newline="",
            ) as handle:
                first = list(csv.DictReader(handle))
        with tempfile.TemporaryDirectory() as second_temporary:
            _, second_output = self._run(
                Path(second_temporary),
                evaluation_drift=True,
            )
            with (second_output / "policy_assignments.csv").open(
                encoding="utf-8",
                newline="",
            ) as handle:
                second = list(csv.DictReader(handle))
        first_choices = [
            row["group_policy_selected_design_id"] for row in first
        ]
        second_choices = [
            row["group_policy_selected_design_id"] for row in second
        ]
        self.assertEqual(first_choices, second_choices)
        self.assertNotEqual(
            [row["group_policy_evaluation_utility_gain"] for row in first],
            [row["group_policy_evaluation_utility_gain"] for row in second],
        )

    def test_incomplete_record_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pilot = _pilot_config(root)
            oracle = _oracle_config(root, pilot)
            config = _audit_config(root)
            source = _oracle_output(root)
            path = source / "designs" / CANDIDATE / "runs.jsonl"
            records = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            ]
            records[0]["telemetry_complete"] = False
            path.write_text(
                "".join(
                    json.dumps(record, sort_keys=True) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(AWMConfigError, "fails closed"):
                audit_awm_workload_heterogeneity(
                    config,
                    oracle,
                    oracle_output_dir=source,
                    output_dir=root / "audit-output",
                )

    def test_analysis_outputs_are_deterministic(self) -> None:
        payloads: list[tuple[str, str, str, str]] = []
        for _ in range(2):
            with tempfile.TemporaryDirectory() as temporary:
                _, output = self._run(Path(temporary))
                payloads.append(tuple(
                    (output / name).read_text(encoding="utf-8")
                    for name in (
                        "workload_effects.csv",
                        "stratum_summary.csv",
                        "policy_assignments.csv",
                        "heterogeneity_evaluation.json",
                    )
                ))
        self.assertEqual(payloads[0], payloads[1])

    def test_cli_runs_the_read_only_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pilot = _pilot_config(root)
            oracle = _oracle_config(root, pilot)
            config = _audit_config(root)
            source = _oracle_output(root)
            output = root / "cli-output"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = cli_main([
                    "audit-awm-heterogeneity",
                    "--audit-config",
                    str(config),
                    "--oracle-config",
                    str(oracle),
                    "--oracle-output-dir",
                    str(source),
                    "--output-dir",
                    str(output),
                    "--compact",
                ])
            self.assertEqual(0, exit_code)
            payload = json.loads(stdout.getvalue())
            self.assertEqual("COMPLETE", payload["status"])
            self.assertTrue(
                (output / "heterogeneity_manifest.json").is_file()
            )


if __name__ == "__main__":
    unittest.main()
