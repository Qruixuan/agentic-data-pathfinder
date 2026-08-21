from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.awm import (
    AWMConfigError,
    AdaptiveWorkloadModel,
    evaluate_awm,
    load_awm_config,
    load_oracle_dataset,
)
from pathfinder.reduced_oracle import load_reduced_oracle_config


ROOT = Path(__file__).resolve().parents[1]
SYSTEM_PATH = ROOT / "configs" / "phase_b_confirmatory_small_system.json"
PILOT_PATH = ROOT / "configs" / "phase_b_confirmatory_small.json"
COMMITTED_AWM_CONFIG = ROOT / "configs" / "awm_reduced_mvp.json"
DESIGNS = ("D_remote_digest", "D_local_digest")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _oracle_config(root: Path) -> Path:
    path = root / "oracle.json"
    _write_json(
        path,
        {
            "schema_version": "pathfinder.reduced-oracle/v1alpha1",
            "oracle_id": "synthetic-oracle",
            "workload_pilot_config": str(PILOT_PATH),
            "safe_design_id": "D_remote_digest",
            "design_order": list(DESIGNS),
            "quote_profile_id": "as_designed",
            "latency_multiplier": 1.0,
            "repetitions": 3,
            "base_seed": 11,
            "randomization_seed": 21,
            "horizon_sessions": 100,
            "horizon_hours": 1,
            "minimum_completion_rate": 1.0,
            "materialization_root": "materialized",
            "cost_model_status": "synthetic-test",
            "transition_cost": {
                "copy_cost_per_gib": 0,
                "elapsed_time_cost_per_second": 0,
                "foreground_loss_per_transition": 0,
                "storage_cost_per_gib_hour": 0,
            },
            "naive_baseline": {
                "candidate_design_id": "D_local_digest",
                "representation_id": "multimodal_digest",
                "decision_margin": 0,
            },
            "designs": [
                {
                    "design_id": "D_remote_digest",
                    "materialization_decision": "reuse",
                    "placement_decision": "remote",
                    "execution_decision": "flowmesh",
                    "materializations": [],
                },
                {
                    "design_id": "D_local_digest",
                    "materialization_decision": "copy",
                    "placement_decision": "local",
                    "execution_decision": "flowmesh",
                    "materializations": [],
                },
            ],
        },
    )
    return path


def _awm_config(
    root: Path,
    *,
    observed_design_ids: tuple[str, ...] = ("D_remote_digest",),
    confidence_level: float = 0.9,
    quoted_price_sufficiency: bool = True,
) -> Path:
    enabled = quoted_price_sufficiency
    path = root / "awm.json"
    _write_json(
        path,
        {
            "schema_version": "pathfinder.awm/v1alpha1",
            "model_id": "synthetic-awm",
            "confidence_level": confidence_level,
            "holdout_repetitions": 1,
            "observed_design_ids": list(observed_design_ids),
            "minimum_training_sessions": 8,
            "maximum_excluded_training_fraction": 0,
            "maximum_excluded_holdout_fraction": 0,
            "substitution_groups": [
                ["sampled_frames", "multimodal_digest"]
            ],
            "assumptions": {
                "own_price_monotonicity": {
                    "enabled": enabled,
                    "status": "synthetic-pass",
                },
                "substitution_group_monotonicity": {
                    "enabled": enabled,
                    "status": "synthetic-pass",
                },
                "success_monotonicity": {
                    "enabled": enabled,
                    "status": "synthetic-pass",
                },
                "quoted_price_sufficiency": {
                    "enabled": quoted_price_sufficiency,
                    "status": "synthetic-pass",
                },
            },
            "own_price_requires_other_quotes_equal": True,
            "cost_relative_radius": 0.1,
            "transition_relative_radius": 0.1,
            "commit_margin": 0,
        },
    )
    return path


def _record(
    *,
    design_id: str,
    repetition: int,
    index: int,
    selected: str,
    success: bool,
) -> dict[str, object]:
    realized_cost = 0.2 if selected == "multimodal_digest" else 0.35
    return {
        "trial_key": f"{design_id}|r{repetition}|i{index}",
        "design_id": design_id,
        "task_class_id": "video_qa",
        "quote_profile_id": "as_designed",
        "repetition": repetition,
        "outcome_type": "completed",
        "telemetry_complete": True,
        "task_success": success,
        "selected_representations": [selected],
        "access_events": [
            {
                "accepted": True,
                "representation_id": selected,
                "realized_cost": realized_cost,
                "artifact_download_request_count": 0,
                "artifact_full_download_count": 0,
                "artifact_bytes_sent": 0,
            }
        ],
    }


def _synthetic_oracle_output(root: Path, *, drift: bool = False) -> Path:
    output = root / "oracle-output"
    output.mkdir(parents=True, exist_ok=True)
    for design_id in DESIGNS:
        records = []
        for repetition in range(3):
            for index in range(8):
                if drift:
                    training = repetition < 2
                    success = (
                        training
                        if design_id == "D_local_digest"
                        else not training
                    )
                else:
                    success = (
                        True
                        if design_id == "D_local_digest"
                        else index % 2 == 0
                    )
                selected = (
                    "multimodal_digest"
                    if design_id == "D_local_digest"
                    else "sampled_frames"
                )
                records.append(
                    _record(
                        design_id=design_id,
                        repetition=repetition,
                        index=index,
                        selected=selected,
                        success=success,
                    )
                )
        path = output / "designs" / design_id / "runs.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(
                json.dumps(record, sort_keys=True) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )
    with (output / "oracle_table.csv").open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
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
        writer.writerow(
            {
                "design_id": "D_remote_digest",
                "storage_cost": 0,
                "forward_transition_cost": 0,
                "restoration_cost": 0,
            }
        )
        writer.writerow(
            {
                "design_id": "D_local_digest",
                "storage_cost": 1,
                "forward_transition_cost": 2,
                "restoration_cost": 1,
            }
        )
    return output


class AWMConfigTest(unittest.TestCase):
    def test_committed_config_keeps_unvalidated_assumptions_disabled(self) -> None:
        config = load_awm_config(COMMITTED_AWM_CONFIG)
        self.assertEqual(("D_remote_digest",), config.observed_design_ids)
        self.assertTrue(
            all(not assumption.enabled for assumption in config.assumptions.values())
        )

    def test_structural_constraints_require_price_sufficiency(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _awm_config(root, quoted_price_sufficiency=False)
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["assumptions"]["own_price_monotonicity"][
                "enabled"
            ] = True
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "require quoted_price_sufficiency",
            ):
                load_awm_config(path)

    def test_enabled_assumption_requires_passed_status(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _awm_config(root)
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["assumptions"]["own_price_monotonicity"][
                "status"
            ] = "pending-phase-b"
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "must have passed or validated status",
            ):
                load_awm_config(path)


class AWMSyntheticOracleTest(unittest.TestCase):
    def test_oracle_table_requires_complete_reveal_cost_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(root))
            output = _synthetic_oracle_output(root)
            table = output / "oracle_table.csv"
            rows = list(
                csv.DictReader(
                    table.read_text(encoding="utf-8").splitlines()
                )
            )
            with table.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=(
                        "design_id",
                        "storage_cost",
                        "forward_transition_cost",
                    ),
                )
                writer.writeheader()
                writer.writerows(
                    {
                        key: value
                        for key, value in row.items()
                        if key != "restoration_cost"
                    }
                    for row in rows
                )
            with self.assertRaisesRegex(
                AWMConfigError,
                "restoration_cost",
            ):
                load_oracle_dataset(
                    config,
                    oracle,
                    oracle_output_dir=output,
                )

    def test_excluded_holdout_session_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(root))
            output = _synthetic_oracle_output(root)
            path = (
                output
                / "designs"
                / "D_local_digest"
                / "runs.jsonl"
            )
            records = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            ]
            records[-1]["outcome_type"] = "infrastructure_failure"
            records[-1]["telemetry_complete"] = None
            records[-1]["task_success"] = None
            path.write_text(
                "".join(
                    json.dumps(record, sort_keys=True) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "exceeds maximum excluded holdout fraction",
            ):
                load_oracle_dataset(
                    config,
                    oracle,
                    oracle_output_dir=output,
                )

    def test_disabled_coupling_matches_independent_box(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(COMMITTED_AWM_CONFIG)
            dataset = load_oracle_dataset(
                config,
                oracle,
                oracle_output_dir=_synthetic_oracle_output(root),
            )
            independent = AdaptiveWorkloadModel(
                dataset,
                model_kind="independent_box",
            )
            coupled = AdaptiveWorkloadModel(
                dataset,
                model_kind="coupled_awm",
            )
            for design_id in DESIGNS:
                self.assertEqual(
                    independent.bounds[design_id].access,
                    coupled.bounds[design_id].access,
                )
                self.assertEqual(
                    independent.bounds[design_id].group_access,
                    coupled.bounds[design_id].group_access,
                )
                self.assertEqual(
                    independent.bounds[design_id].success,
                    coupled.bounds[design_id].success,
                )
                self.assertEqual(
                    independent.bounds[design_id].phi,
                    coupled.bounds[design_id].phi,
                )

    def test_coupling_narrows_bounds_and_covers_holdout_vector(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(root))
            oracle_output = _synthetic_oracle_output(root)
            dataset = load_oracle_dataset(
                config,
                oracle,
                oracle_output_dir=oracle_output,
            )
            independent = AdaptiveWorkloadModel(
                dataset,
                model_kind="independent_box",
            )
            coupled = AdaptiveWorkloadModel(
                dataset,
                model_kind="coupled_awm",
            )
            candidate = "D_local_digest"

            self.assertGreater(
                coupled.bounds[candidate]
                .group_access["sampled_frames+multimodal_digest"]
                .lower,
                independent.bounds[candidate]
                .group_access["sampled_frames+multimodal_digest"]
                .lower,
            )
            self.assertGreater(
                coupled.bounds[candidate].success.lower,
                independent.bounds[candidate].success.lower,
            )
            self.assertLess(
                coupled.bounds[candidate].phi.width,
                independent.bounds[candidate].phi.width,
            )
            self.assertEqual(
                coupled.lower_bound(candidate),
                coupled.bounds[candidate].phi.lower,
            )

            output = root / "awm-output"
            manifest = evaluate_awm(
                config,
                oracle,
                oracle_output_dir=oracle_output,
                output_dir=output,
            )
            evaluation = json.loads(
                (output / "awm_evaluation.json").read_text(encoding="utf-8")
            )
            self.assertEqual("COMPLETE", manifest["status"])
            self.assertTrue(
                evaluation["models"]["coupled_awm"][
                    "joint_response_vector_covered"
                ]
            )
            self.assertEqual(
                0,
                evaluation["models"]["coupled_awm"][
                    "false_safe_commit_count"
                ],
            )
            self.assertTrue((output / "awm_bounds.csv").is_file())
            self.assertTrue((output / "holdout_truth.json").is_file())
            per_repetition = json.loads(
                (
                    output / "holdout_truth_by_repetition.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual([2], evaluation["holdout_repetition_ids"])
            self.assertEqual(
                {"D_remote_digest", "D_local_digest"},
                set(per_repetition["2"]),
            )
            self.assertEqual(
                str(output / "holdout_truth_by_repetition.json"),
                manifest["holdout_truth_by_repetition_path"],
            )

    def test_evaluator_detects_false_safe_commit_under_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(
                _awm_config(
                    root,
                    observed_design_ids=DESIGNS,
                    confidence_level=0.5,
                )
            )
            oracle_output = _synthetic_oracle_output(root, drift=True)
            output = root / "awm-output"
            evaluate_awm(
                config,
                oracle,
                oracle_output_dir=oracle_output,
                output_dir=output,
            )
            evaluation = json.loads(
                (output / "awm_evaluation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                1,
                evaluation["models"]["independent_box"][
                    "false_safe_commit_count"
                ],
            )
            decision = evaluation["models"]["independent_box"][
                "commit_decisions"
            ][0]
            self.assertTrue(decision["would_commit"])
            self.assertLess(decision["actual_holdout_gain"], 0)


if __name__ == "__main__":
    unittest.main()
