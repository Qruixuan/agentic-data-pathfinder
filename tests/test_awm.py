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
from pathfinder.awm.model import (
    _bernoulli_kl_interval,
    _bounded_kl_mean_interval,
)
from pathfinder.reduced_oracle import load_reduced_oracle_config


ROOT = Path(__file__).resolve().parents[1]
SYSTEM_PATH = ROOT / "configs" / "phase_b_confirmatory_small_system.json"
PILOT_PATH = ROOT / "configs" / "phase_b_confirmatory_small.json"
COMMITTED_AWM_CONFIG = ROOT / "configs" / "awm_reduced_mvp.json"
COMMITTED_AWM_V2_CONFIG = (
    ROOT / "configs" / "multi_candidate_formal_v1_awm_v2_diagnostic.json"
)
COMMITTED_AWM_V2_ALPHA2_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v1_awm_v2alpha2_oed_diagnostic.json"
)
COMMITTED_AWM_V2_ALPHA2_POWER_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v1_awm_v2alpha2_power_diagnostic.json"
)
COMMITTED_AWM_V3_POWER_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v1_awm_v3_power_diagnostic.json"
)
COMMITTED_AWM_V3_ALPHA2_POWER_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v1_awm_v3alpha2_power_diagnostic.json"
)
COMMITTED_AWM_V3_ALPHA3_POWER_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v1_awm_v3alpha3_power_diagnostic.json"
)
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
    schema_version: str = "pathfinder.awm/v1alpha1",
    maximum_looks: int = 8,
) -> Path:
    enabled = quoted_price_sufficiency
    path = root / "awm.json"
    payload = {
        "schema_version": schema_version,
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
    }
    if schema_version in (
        "pathfinder.awm/v2alpha1",
        "pathfinder.awm/v2alpha2",
        "pathfinder.awm/v3alpha1",
        "pathfinder.awm/v3alpha2",
        "pathfinder.awm/v3alpha3",
    ):
        payload["confidence"] = {
            "family_mode": "fixed-full-domain",
            "marginal_alpha_fraction": 0.5,
            "paired_gain_alpha_fraction": 0.5,
            "paired_gain_method": (
                "fixed-looks-empirical-bernstein"
            ),
            "paired_gain_minimum_pairs": 8,
            "maximum_looks": maximum_looks,
            "require_complete_pairs": True,
        }
        if schema_version == "pathfinder.awm/v2alpha2":
            payload["confidence"].update({
                "paired_gain_method": (
                    "fixed-snapshot-empirical-bernstein"
                ),
                "maximum_looks": 1,
                "look_semantics": "fixed-training-snapshot-per-pair",
                "paired_comparisons": [list(DESIGNS)],
            })
        if schema_version == "pathfinder.awm/v3alpha1":
            payload["confidence"].update({
                "paired_gain_method": (
                    "cluster-first-decomposed-kl-empirical-bernstein"
                ),
                "paired_gain_minimum_pairs": 8,
                "maximum_looks": 1,
                "look_semantics": "fixed-training-snapshot-per-pair",
                "sampling_unit": (
                    "workload-cluster-first-paired-observation"
                ),
                "cluster_key_fields": ["workload_id"],
                "cluster_reduction": "lowest-repetition-then-seed",
                "success_alpha_fraction": 0.8,
                "cost_alpha_fraction": 0.2,
                "paired_comparisons": [list(DESIGNS)],
            })
        if schema_version == "pathfinder.awm/v3alpha2":
            payload["confidence"].update({
                "paired_gain_method": (
                    "cluster-mean-decomposed-bounded-kl-"
                    "empirical-bernstein"
                ),
                "paired_gain_minimum_pairs": 8,
                "maximum_looks": 1,
                "look_semantics": "fixed-training-snapshot-per-pair",
                "sampling_unit": (
                    "workload-cluster-mean-paired-observation"
                ),
                "cluster_key_fields": ["workload_id"],
                "cluster_reduction": (
                    "mean-over-complete-repetition-block"
                ),
                "success_alpha_fraction": 0.8,
                "cost_alpha_fraction": 0.2,
                "paired_comparisons": [list(DESIGNS)],
            })
        if schema_version == "pathfinder.awm/v3alpha3":
            payload["confidence"].update({
                "paired_gain_method": (
                    "cluster-mean-direct-bounded-utility-kl"
                ),
                "paired_gain_minimum_pairs": 8,
                "maximum_looks": 1,
                "look_semantics": "fixed-training-snapshot-per-pair",
                "sampling_unit": (
                    "workload-cluster-mean-paired-observation"
                ),
                "cluster_key_fields": ["workload_id"],
                "cluster_reduction": (
                    "mean-over-complete-repetition-block"
                ),
                "success_alpha_fraction": 0.8,
                "cost_alpha_fraction": 0.2,
                "paired_comparisons": [list(DESIGNS)],
            })
    _write_json(path, payload)
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
        "workload_id": f"workload-{index:04d}",
        "design_id": design_id,
        "task_class_id": "video_qa",
        "quote_profile_id": "as_designed",
        "repetition": repetition,
        "seed": 11 + repetition,
        "latency_multiplier": 1.0,
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
        self.assertEqual("dynamic-observed-v1", config.confidence.family_mode)
        self.assertFalse(config.confidence.paired_gain_enabled)

    def test_v2_requires_fixed_family_and_paired_gain_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_awm_config(_awm_config(
                root,
                schema_version="pathfinder.awm/v2alpha1",
            ))
            self.assertEqual("fixed-full-domain", config.confidence.family_mode)
            self.assertTrue(config.confidence.paired_gain_enabled)
            self.assertEqual(8, config.confidence.maximum_looks)

    def test_committed_v2_diagnostic_freezes_four_looks(self) -> None:
        config = load_awm_config(COMMITTED_AWM_V2_CONFIG)
        self.assertEqual(
            "multi-candidate-formal-v1-awm-v2-diagnostic",
            config.model_id,
        )
        self.assertEqual(4, config.confidence.maximum_looks)
        self.assertEqual(16, config.confidence.paired_gain_minimum_pairs)

    def test_v2alpha2_preregisters_three_one_look_comparisons(self) -> None:
        config = load_awm_config(COMMITTED_AWM_V2_ALPHA2_CONFIG)
        self.assertEqual(
            "fixed-training-snapshot-per-pair",
            config.confidence.look_semantics,
        )
        self.assertEqual(1, config.confidence.maximum_looks)
        self.assertEqual(3, len(config.confidence.paired_comparisons))
        self.assertEqual(
            ("D_origin_remote", "D_local_pair"),
            config.confidence.paired_comparisons[-1],
        )
        power = load_awm_config(COMMITTED_AWM_V2_ALPHA2_POWER_CONFIG)
        self.assertEqual(4, len(power.observed_design_ids))
        self.assertEqual(
            config.confidence.paired_comparisons,
            power.confidence.paired_comparisons,
        )

    def test_v2alpha2_rejects_duplicate_unordered_comparisons(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _awm_config(
                root,
                schema_version="pathfinder.awm/v2alpha2",
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["confidence"]["paired_comparisons"].append(
                list(reversed(DESIGNS))
            )
            _write_json(path, payload)
            with self.assertRaisesRegex(AWMConfigError, "duplicate unordered"):
                load_awm_config(path)

    def test_v2alpha2_fixed_snapshot_requires_one_look(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _awm_config(
                root,
                schema_version="pathfinder.awm/v2alpha2",
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["confidence"]["maximum_looks"] = 2
            _write_json(path, payload)
            with self.assertRaisesRegex(AWMConfigError, "maximum_looks=1"):
                load_awm_config(path)

    def test_v3_committed_config_declares_cluster_and_component_contract(
        self,
    ) -> None:
        config = load_awm_config(COMMITTED_AWM_V3_POWER_CONFIG)
        self.assertEqual("pathfinder.awm/v3alpha1", config.schema_version)
        self.assertEqual(
            "workload-cluster-first-paired-observation",
            config.confidence.sampling_unit,
        )
        self.assertEqual(
            ("workload_id",),
            config.confidence.cluster_key_fields,
        )
        self.assertEqual(8, config.confidence.paired_gain_minimum_pairs)
        self.assertAlmostEqual(
            1.0,
            config.confidence.success_alpha_fraction
            + config.confidence.cost_alpha_fraction,
        )

    def test_v3_rejects_invalid_component_alpha_split(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _awm_config(
                root,
                schema_version="pathfinder.awm/v3alpha1",
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["confidence"]["cost_alpha_fraction"] = 0.3
            _write_json(path, payload)
            with self.assertRaisesRegex(AWMConfigError, "must sum to 1"):
                load_awm_config(path)

    def test_v3_rejects_unregistered_cluster_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _awm_config(
                root,
                schema_version="pathfinder.awm/v3alpha1",
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["confidence"]["cluster_key_fields"] = ["video_id"]
            _write_json(path, payload)
            with self.assertRaisesRegex(AWMConfigError, "workload_id"):
                load_awm_config(path)

    def test_v3alpha2_committed_config_declares_cluster_mean_contract(
        self,
    ) -> None:
        config = load_awm_config(COMMITTED_AWM_V3_ALPHA2_POWER_CONFIG)
        self.assertEqual("pathfinder.awm/v3alpha2", config.schema_version)
        self.assertEqual(
            "cluster-mean-decomposed-bounded-kl-empirical-bernstein",
            config.confidence.paired_gain_method,
        )
        self.assertEqual(
            "workload-cluster-mean-paired-observation",
            config.confidence.sampling_unit,
        )
        self.assertEqual(
            "mean-over-complete-repetition-block",
            config.confidence.cluster_reduction,
        )
        self.assertEqual(1, config.confidence.maximum_looks)

    def test_cluster_mean_schemas_require_complete_pairs(self) -> None:
        for schema_version in (
            "pathfinder.awm/v3alpha2",
            "pathfinder.awm/v3alpha3",
        ):
            with self.subTest(schema_version=schema_version):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    path = _awm_config(
                        root,
                        schema_version=schema_version,
                    )
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["confidence"]["require_complete_pairs"] = False
                    _write_json(path, payload)
                    with self.assertRaisesRegex(
                        AWMConfigError,
                        "require_complete_pairs must be true",
                    ):
                        load_awm_config(path)

    def test_v3alpha3_committed_config_declares_direct_utility_contract(
        self,
    ) -> None:
        config = load_awm_config(COMMITTED_AWM_V3_ALPHA3_POWER_CONFIG)
        self.assertEqual("pathfinder.awm/v3alpha3", config.schema_version)
        self.assertEqual(
            "cluster-mean-direct-bounded-utility-kl",
            config.confidence.paired_gain_method,
        )
        self.assertEqual(
            "workload-cluster-mean-paired-observation",
            config.confidence.sampling_unit,
        )
        self.assertEqual(
            "mean-over-complete-repetition-block",
            config.confidence.cluster_reduction,
        )
        self.assertTrue(config.confidence.require_complete_pairs)

    def test_v2_alpha_fractions_must_sum_to_one(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _awm_config(
                root,
                schema_version="pathfinder.awm/v2alpha1",
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["confidence"]["paired_gain_alpha_fraction"] = 0.4
            _write_json(path, payload)
            with self.assertRaisesRegex(AWMConfigError, "must sum to 1"):
                load_awm_config(path)

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
    def test_v3_cluster_certificate_is_decomposed_and_auditable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v3alpha1",
            ))
            oracle_output = _synthetic_oracle_output(root)
            model = AdaptiveWorkloadModel(
                load_oracle_dataset(
                    config,
                    oracle,
                    oracle_output_dir=oracle_output,
                ),
                model_kind="coupled_awm",
            )
            certificate = model.paired_gain_certificate(*DESIGNS)
            self.assertIsNotNone(certificate)
            assert certificate is not None
            self.assertEqual(16, certificate.raw_pair_count)
            self.assertEqual(8, certificate.pair_count)
            self.assertEqual(8, certificate.independent_unit_count)
            self.assertEqual(4, certificate.positive_discordance_count)
            self.assertEqual(0, certificate.negative_discordance_count)
            self.assertEqual(("workload_id",), certificate.cluster_key_fields)
            self.assertEqual(
                "paired-discordance-bernoulli-kl-plus-"
                "service-cost-empirical-bernstein",
                certificate.certificate_construction,
            )
            self.assertIsNotNone(certificate.success_difference)
            self.assertIsNotNone(certificate.service_cost_difference)
            assert certificate.success_difference is not None
            assert certificate.service_cost_difference is not None
            expected_lower = (
                model.dataset.task_class.task_value
                * certificate.success_difference.lower
                - model.dataset.system.resource_cost_weight
                * certificate.service_cost_difference.upper
            )
            self.assertAlmostEqual(
                expected_lower,
                certificate.gain_per_session.lower,
            )
            self.assertEqual(
                "paired-cluster-decomposed-kl-empirical-bernstein",
                model.gain_interval_source(*DESIGNS),
            )
            self.assertIsNone(
                model.paired_gain_certificate(*reversed(DESIGNS))
            )

            planning = model.paired_gain_power_analysis(*DESIGNS)
            self.assertIsNotNone(planning)
            assert planning is not None
            self.assertEqual(8, planning.current_pair_count)
            self.assertEqual(16, planning.raw_pair_count)
            self.assertIsNone(
                planning.estimated_pairs_for_unclipped_interval
            )
            self.assertIn("workload clusters", planning.caveat)

            output = root / "awm-v3-output"
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
                "pathfinder.awm-evaluation/v3alpha1",
                evaluation["schema_version"],
            )

    def test_v3_ignores_later_within_cluster_repetition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v3alpha1",
            ))
            oracle_output = _synthetic_oracle_output(root)

            def certificate():
                model = AdaptiveWorkloadModel(
                    load_oracle_dataset(
                        config,
                        oracle,
                        oracle_output_dir=oracle_output,
                    ),
                    model_kind="coupled_awm",
                )
                result = model.paired_gain_certificate(*DESIGNS)
                assert result is not None
                return result

            before = certificate()
            path = (
                oracle_output
                / "designs"
                / "D_local_digest"
                / "runs.jsonl"
            )
            records = [
                json.loads(line) for line in path.read_text().splitlines()
            ]
            target = next(
                record for record in records
                if record["repetition"] == 1
                and record["workload_id"] == "workload-0000"
            )
            target["task_success"] = not target["task_success"]
            path.write_text(
                "".join(
                    json.dumps(record, sort_keys=True) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )
            after = certificate()
            self.assertEqual(
                before.training_snapshot_sha256,
                after.training_snapshot_sha256,
            )
            self.assertEqual(before.gain_per_session, after.gain_per_session)

            selected = next(
                record for record in records
                if record["repetition"] == 0
                and record["workload_id"] == "workload-0000"
            )
            selected["task_success"] = not selected["task_success"]
            path.write_text(
                "".join(
                    json.dumps(record, sort_keys=True) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )
            changed = certificate()
            self.assertNotEqual(
                before.training_snapshot_sha256,
                changed.training_snapshot_sha256,
            )

    def test_v3alpha2_aggregates_complete_repetition_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            oracle_output = _synthetic_oracle_output(root)
            v3alpha2 = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v3alpha2",
            ))
            model = AdaptiveWorkloadModel(
                load_oracle_dataset(
                    v3alpha2,
                    oracle,
                    oracle_output_dir=oracle_output,
                ),
                model_kind="coupled_awm",
            )
            certificate = model.paired_gain_certificate(*DESIGNS)
            self.assertIsNotNone(certificate)
            assert certificate is not None
            self.assertEqual(16, certificate.raw_pair_count)
            self.assertEqual(8, certificate.pair_count)
            self.assertEqual(8, certificate.independent_unit_count)
            self.assertEqual(2, certificate.within_cluster_repetition_count)
            self.assertEqual(
                (0, 1),
                certificate.within_cluster_repetition_ids,
            )
            self.assertEqual(8, certificate.positive_discordance_count)
            self.assertEqual(0, certificate.negative_discordance_count)
            self.assertAlmostEqual(
                4.0,
                certificate.positive_discordance_mass or 0.0,
            )
            self.assertAlmostEqual(
                0.5,
                certificate.positive_discordance_point_estimate or 0.0,
            )
            self.assertFalse(certificate.repetition_sign_flip)
            self.assertEqual(
                "paired-cluster-mean-discordance-bounded-kl-plus-"
                "cluster-mean-service-cost-empirical-bernstein",
                certificate.certificate_construction,
            )
            self.assertEqual(
                "paired-cluster-mean-decomposed-bounded-kl-"
                "empirical-bernstein",
                model.gain_interval_source(*DESIGNS),
            )

            v2alpha2 = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v2alpha2",
            ))
            pooled = AdaptiveWorkloadModel(
                load_oracle_dataset(
                    v2alpha2,
                    oracle,
                    oracle_output_dir=oracle_output,
                ),
                model_kind="coupled_awm",
            ).paired_gain_certificate(*DESIGNS)
            assert pooled is not None
            self.assertAlmostEqual(
                pooled.point_estimate_per_session,
                certificate.point_estimate_per_session,
            )

            planning = model.paired_gain_power_analysis(*DESIGNS)
            self.assertIsNotNone(planning)
            assert planning is not None
            self.assertEqual(2, planning.within_cluster_repetition_count)
            self.assertEqual((0, 1), planning.within_cluster_repetition_ids)
            self.assertAlmostEqual(
                certificate.gain_per_session.width,
                planning.current_interval_width_per_session or 0.0,
            )
            self.assertIn("repetition block fixed", planning.caveat)

            output = root / "awm-v3alpha2-output"
            evaluate_awm(
                v3alpha2,
                oracle,
                oracle_output_dir=oracle_output,
                output_dir=output,
            )
            evaluation = json.loads(
                (output / "awm_evaluation.json").read_text()
            )
            self.assertEqual(
                "pathfinder.awm-evaluation/v3alpha2",
                evaluation["schema_version"],
            )

    def test_v3alpha2_snapshot_uses_later_repetitions_and_flags_flip(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v3alpha2",
            ))
            oracle_output = _synthetic_oracle_output(root)

            def certificate():
                model = AdaptiveWorkloadModel(
                    load_oracle_dataset(
                        config,
                        oracle,
                        oracle_output_dir=oracle_output,
                    ),
                    model_kind="coupled_awm",
                )
                result = model.paired_gain_certificate(*DESIGNS)
                assert result is not None
                return result

            before = certificate()
            path = (
                oracle_output / "designs" / "D_local_digest" / "runs.jsonl"
            )
            records = [
                json.loads(line) for line in path.read_text().splitlines()
            ]
            for record in records:
                if record["repetition"] == 1:
                    record["task_success"] = False
            path.write_text(
                "".join(
                    json.dumps(record, sort_keys=True) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )
            after = certificate()
            self.assertNotEqual(
                before.training_snapshot_sha256,
                after.training_snapshot_sha256,
            )
            self.assertNotEqual(
                before.point_estimate_per_session,
                after.point_estimate_per_session,
            )
            self.assertTrue(after.repetition_sign_flip)
            self.assertGreater(dict(after.repetition_utility_means)[0], 0)
            self.assertLess(dict(after.repetition_utility_means)[1], 0)

    def test_v3alpha3_uses_direct_bounded_cluster_utility_certificate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v3alpha3",
            ))
            oracle_output = _synthetic_oracle_output(root)
            model = AdaptiveWorkloadModel(
                load_oracle_dataset(
                    config,
                    oracle,
                    oracle_output_dir=oracle_output,
                ),
                model_kind="coupled_awm",
            )
            certificate = model.paired_gain_certificate(*DESIGNS)
            self.assertIsNotNone(certificate)
            assert certificate is not None
            self.assertEqual(16, certificate.raw_pair_count)
            self.assertEqual(8, certificate.independent_unit_count)
            self.assertEqual(2, certificate.within_cluster_repetition_count)
            self.assertEqual((0, 1), certificate.within_cluster_repetition_ids)
            self.assertEqual(
                "paired-cluster-mean-direct-normalized-bounded-utility-kl",
                certificate.certificate_construction,
            )
            self.assertEqual(
                "diagnostic-only-not-part-of-primary-confidence-family",
                certificate.component_interval_role,
            )
            self.assertIsNotNone(certificate.success_difference)
            self.assertIsNotNone(certificate.service_cost_difference)
            self.assertIsNotNone(
                certificate.normalized_utility_point_estimate
            )
            self.assertIsNotNone(certificate.normalized_utility_mean)
            assert certificate.normalized_utility_mean is not None
            expected_point = (
                certificate.point_estimate_per_session
                - certificate.support_per_session.lower
            ) / certificate.support_per_session.width
            self.assertAlmostEqual(
                expected_point,
                certificate.normalized_utility_point_estimate or 0.0,
            )
            expected_normalized = _bounded_kl_mean_interval(
                expected_point,
                certificate.independent_unit_count or 0,
                certificate.alpha_per_pair_look,
            )
            self.assertAlmostEqual(
                expected_normalized.lower,
                certificate.normalized_utility_mean.lower,
            )
            self.assertAlmostEqual(
                expected_normalized.upper,
                certificate.normalized_utility_mean.upper,
            )
            self.assertAlmostEqual(
                certificate.support_per_session.lower
                + certificate.support_per_session.width
                * expected_normalized.lower,
                certificate.gain_per_session.lower,
            )
            self.assertAlmostEqual(
                certificate.support_per_session.lower
                + certificate.support_per_session.width
                * expected_normalized.upper,
                certificate.gain_per_session.upper,
            )
            self.assertEqual(
                "paired-cluster-mean-direct-bounded-utility-kl",
                model.gain_interval_source(*DESIGNS),
            )

            planning = model.paired_gain_power_analysis(*DESIGNS)
            self.assertIsNotNone(planning)
            assert planning is not None
            self.assertEqual(
                "plug-in-cluster-mean-direct-bounded-utility-kl-planning",
                planning.method,
            )
            self.assertEqual(8, planning.current_pair_count)
            self.assertEqual(16, planning.raw_pair_count)
            self.assertAlmostEqual(
                certificate.gain_per_session.width,
                planning.current_interval_width_per_session or 0.0,
            )
            self.assertIn("normalized workload-level utility", planning.caveat)

            output = root / "awm-v3alpha3-output"
            evaluate_awm(
                config,
                oracle,
                oracle_output_dir=oracle_output,
                output_dir=output,
            )
            evaluation = json.loads(
                (output / "awm_evaluation.json").read_text()
            )
            self.assertEqual(
                "pathfinder.awm-evaluation/v3alpha3",
                evaluation["schema_version"],
            )
            self.assertEqual(
                "diagnostic-only-not-part-of-primary-confidence-family",
                evaluation["confidence_contract"][
                    "component_interval_role"
                ],
            )

    def test_v3alpha2_rejects_incomplete_or_duplicate_blocks(self) -> None:
        for mutation in ("incomplete", "duplicate"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                oracle = load_reduced_oracle_config(_oracle_config(root))
                config = load_awm_config(_awm_config(
                    root,
                    observed_design_ids=DESIGNS,
                    schema_version="pathfinder.awm/v3alpha2",
                ))
                oracle_output = _synthetic_oracle_output(root)
                for design_id in DESIGNS:
                    path = oracle_output / "designs" / design_id / "runs.jsonl"
                    records = [
                        json.loads(line)
                        for line in path.read_text().splitlines()
                    ]
                    target = next(
                        record for record in records
                        if record["repetition"] == 1
                        and record["workload_id"] == "workload-0000"
                    )
                    if mutation == "incomplete":
                        records.remove(target)
                    else:
                        duplicate = dict(target)
                        duplicate["seed"] = int(target["seed"]) + 1000
                        duplicate["trial_key"] = str(target["trial_key"]) + "-dup"
                        records.append(duplicate)
                    path.write_text(
                        "".join(
                            json.dumps(record, sort_keys=True) + "\n"
                            for record in records
                        ),
                        encoding="utf-8",
                    )
                with self.assertRaisesRegex(
                    AWMConfigError,
                    "complete common repetition block|exactly one observation",
                ):
                    load_oracle_dataset(
                        config,
                        oracle,
                        oracle_output_dir=oracle_output,
                    )

    def test_bernoulli_kl_interval_contains_empirical_rate(self) -> None:
        for successes in (0, 4, 8):
            with self.subTest(successes=successes):
                interval = _bernoulli_kl_interval(successes, 8, 0.02)
                self.assertTrue(interval.contains(successes / 8))
                if successes == 0:
                    self.assertEqual(0.0, interval.lower)
                if successes == 8:
                    self.assertEqual(1.0, interval.upper)

    def test_v2alpha2_certificate_decomposition_and_power_are_auditable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v2alpha2",
            ))
            oracle_output = _synthetic_oracle_output(root)
            model = AdaptiveWorkloadModel(
                load_oracle_dataset(
                    config,
                    oracle,
                    oracle_output_dir=oracle_output,
                ),
                model_kind="coupled_awm",
            )
            certificate = model.paired_gain_certificate(*DESIGNS)
            self.assertIsNotNone(certificate)
            assert certificate is not None
            self.assertAlmostEqual(
                certificate.total_radius_per_session,
                certificate.variance_radius_per_session
                + certificate.range_radius_per_session,
            )
            self.assertAlmostEqual(
                certificate.point_estimate_per_session,
                model.dataset.task_class.task_value
                * certificate.success_difference_point_estimate
                - model.dataset.system.resource_cost_weight
                * certificate.service_cost_difference_point_estimate,
            )
            self.assertEqual(1, certificate.family_size)
            self.assertEqual(64, len(certificate.training_snapshot_sha256))
            self.assertAlmostEqual(0.05, certificate.alpha_per_pair_look)
            self.assertEqual(
                "paired-fixed-snapshot-empirical-bernstein",
                model.gain_interval_source(*DESIGNS),
            )
            self.assertIsNone(
                model.paired_gain_certificate(*reversed(DESIGNS))
            )
            self.assertEqual(
                "marginal-phi-difference",
                model.gain_interval_source(*reversed(DESIGNS)),
            )

            planning = model.paired_gain_power_analysis(*DESIGNS)
            self.assertIsNotNone(planning)
            assert planning is not None
            self.assertEqual(16, planning.current_pair_count)
            self.assertEqual(
                "posthoc-planning-not-a-confidence-guarantee",
                planning.planning_status,
            )
            self.assertGreaterEqual(
                planning.estimated_pairs_for_50pct_support_width or 0,
                planning.current_pair_count,
            )

            output = root / "awm-v2alpha2-output"
            manifest = evaluate_awm(
                config,
                oracle,
                oracle_output_dir=oracle_output,
                output_dir=output,
            )
            self.assertEqual(
                str(output / "awm_paired_power_analysis.csv"),
                manifest["paired_gain_power_analysis_path"],
            )
            with (output / "awm_paired_power_analysis.csv").open(
                encoding="utf-8",
                newline="",
            ) as handle:
                power_rows = list(csv.DictReader(handle))
            self.assertGreaterEqual(len(power_rows), 1)
            evaluation = json.loads(
                (output / "awm_evaluation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                "pathfinder.awm-evaluation/v2alpha2",
                evaluation["schema_version"],
            )

    def test_v2_fixed_marginal_family_does_not_expand_after_reveal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            oracle_output = _synthetic_oracle_output(root)
            one = load_awm_config(_awm_config(
                root,
                schema_version="pathfinder.awm/v2alpha1",
            ))
            one_model = AdaptiveWorkloadModel(
                load_oracle_dataset(
                    one,
                    oracle,
                    oracle_output_dir=oracle_output,
                ),
                model_kind="coupled_awm",
            )
            both = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v2alpha1",
            ))
            both_model = AdaptiveWorkloadModel(
                load_oracle_dataset(
                    both,
                    oracle,
                    oracle_output_dir=oracle_output,
                ),
                model_kind="coupled_awm",
            )
            self.assertEqual(
                one_model.confidence_contract["marginal_family_size"],
                both_model.confidence_contract["marginal_family_size"],
            )
            self.assertEqual(
                one_model.confidence_contract["marginal_alpha_per_metric"],
                both_model.confidence_contract["marginal_alpha_per_metric"],
            )

    def test_v2_paired_gain_certificate_is_auditable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v2alpha1",
            ))
            oracle_output = _synthetic_oracle_output(root)
            model = AdaptiveWorkloadModel(
                load_oracle_dataset(
                    config,
                    oracle,
                    oracle_output_dir=oracle_output,
                ),
                model_kind="coupled_awm",
            )
            certificate = model.paired_gain_certificate(*DESIGNS)
            self.assertIsNotNone(certificate)
            assert certificate is not None
            self.assertEqual(16, certificate.pair_count)
            self.assertEqual(8, certificate.maximum_looks)
            self.assertEqual(8, certificate.family_size)
            self.assertEqual(
                "paired-fixed-looks-empirical-bernstein",
                model.gain_interval_source(*DESIGNS),
            )
            self.assertEqual(
                certificate.transition_adjusted_gain,
                model.gain_interval(*DESIGNS),
            )

            evaluation_output = root / "awm-v2-output"
            manifest = evaluate_awm(
                config,
                oracle,
                oracle_output_dir=oracle_output,
                output_dir=evaluation_output,
            )
            evaluation = json.loads(
                (evaluation_output / "awm_evaluation.json")
                .read_text(encoding="utf-8")
            )
            self.assertEqual(
                "pathfinder.awm-evaluation/v2alpha1",
                evaluation["schema_version"],
            )
            self.assertEqual(
                1,
                evaluation["models"]["coupled_awm"][
                    "paired_gain_decision_count"
                ],
            )
            self.assertEqual(
                str(evaluation_output / "awm_paired_gain_bounds.csv"),
                manifest["paired_gain_bounds_path"],
            )
            with (
                evaluation_output / "awm_paired_gain_bounds.csv"
            ).open(encoding="utf-8", newline="") as handle:
                paired_rows = list(csv.DictReader(handle))
            self.assertEqual(4, len(paired_rows))

    def test_v2_rejects_misaligned_training_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = load_reduced_oracle_config(_oracle_config(root))
            config = load_awm_config(_awm_config(
                root,
                observed_design_ids=DESIGNS,
                schema_version="pathfinder.awm/v2alpha1",
            ))
            output = _synthetic_oracle_output(root)
            path = output / "designs" / DESIGNS[1] / "runs.jsonl"
            records = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            ]
            records[0]["workload_id"] = "different-workload"
            path.write_text(
                "".join(
                    json.dumps(record, sort_keys=True) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "identical eligible training keys",
            ):
                load_oracle_dataset(
                    config,
                    oracle,
                    oracle_output_dir=output,
                )

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
