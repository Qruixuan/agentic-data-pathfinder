from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.awm import (
    AdaptiveWorkloadModel,
    load_awm_config,
    load_oracle_dataset,
)
from pathfinder.oed import (
    OEDConfigError,
    OEDController,
    OEDState,
    load_oed_config,
    run_oed_replay,
)
from pathfinder.reduced_oracle import load_reduced_oracle_config
from tests.test_awm import (
    DESIGNS,
    _awm_config,
    _oracle_config,
    _synthetic_oracle_output,
    _write_json,
)


ROOT = Path(__file__).resolve().parents[1]
COMMITTED_OED_CONFIG = ROOT / "configs" / "oed_reduced_mvp.json"
COMMITTED_OED_V2_CONFIG = (
    ROOT / "configs" / "multi_candidate_formal_v1_oed_v2_diagnostic.json"
)
COMMITTED_OED_V2_ALPHA2_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v1_oed_v2alpha2_diagnostic.json"
)


def _oed_config(
    root: Path,
    *,
    budget: float = 10.0,
    cap: float = 5.0,
    reveal_candidates: bool = True,
) -> Path:
    path = root / "oed.json"
    _write_json(
        path,
        {
            "schema_version": "pathfinder.oed/v1alpha1",
            "controller_id": "synthetic-oed",
            "commit_margin": 0,
            "reveal_margin": 0,
            "exploration_budget": budget,
            "per_excursion_cap": cap,
            "max_iterations": 8,
            "random_seed": 31,
            "cost_model_status": "synthetic-test",
            "reveal_candidates": (
                [
                    {
                        "design_id": "D_local_digest",
                        "reveal_tier": "simultaneous-canonical",
                        "probe_window_loss": 0.5,
                    }
                ]
                if reveal_candidates
                else []
            ),
            "other_design_ids": (
                [] if reveal_candidates else ["D_local_digest"]
            ),
        },
    )
    return path


class OEDConfigTest(unittest.TestCase):
    def test_committed_config_is_bounded_and_declares_candidate(self) -> None:
        config = load_oed_config(COMMITTED_OED_CONFIG)
        self.assertEqual("oed-reduced-mvp-v1", config.controller_id)
        self.assertLessEqual(
            config.per_excursion_cap,
            config.exploration_budget,
        )
        self.assertEqual(
            {"D_local_digest"},
            set(config.reveal_candidates),
        )

    def test_reveal_and_other_sets_must_be_disjoint(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = _oed_config(root)
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["other_design_ids"] = ["D_local_digest"]
            _write_json(path, payload)
            with self.assertRaisesRegex(OEDConfigError, "overlap"):
                load_oed_config(path)

    def test_excursion_cap_may_exceed_a_smaller_total_purse(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = load_oed_config(
                _oed_config(Path(temporary), budget=2.0, cap=5.0)
            )
            self.assertEqual(2.0, config.exploration_budget)
            self.assertEqual(5.0, config.per_excursion_cap)

    def test_committed_v2_diagnostic_respects_look_budget(self) -> None:
        config = load_oed_config(COMMITTED_OED_V2_CONFIG)
        self.assertEqual(
            "multi-candidate-formal-v1-oed-v2-diagnostic",
            config.controller_id,
        )
        self.assertEqual(4, config.max_iterations)

    def test_committed_v2alpha2_keeps_four_controller_iterations(self) -> None:
        config = load_oed_config(COMMITTED_OED_V2_ALPHA2_CONFIG)
        self.assertEqual(
            "multi-candidate-formal-v1-oed-v2alpha2-diagnostic",
            config.controller_id,
        )
        self.assertEqual(4, config.max_iterations)


class OEDControllerTest(unittest.TestCase):
    def _fixture(self, root: Path, oed_path: Path):
        oracle = load_reduced_oracle_config(_oracle_config(root))
        awm = load_awm_config(
            _awm_config(
                root,
                confidence_level=0.5,
                quoted_price_sufficiency=False,
            )
        )
        output = _synthetic_oracle_output(root)
        dataset = load_oracle_dataset(
            awm,
            oracle,
            oracle_output_dir=output,
        )
        model = AdaptiveWorkloadModel(dataset, model_kind="coupled_awm")
        state = OEDState(
            safe_design_id=oracle.safe_design_id,
            observed_design_ids=awm.observed_design_ids,
            revealed_design_ids=(),
            remaining_exploration_purse=load_oed_config(
                oed_path
            ).exploration_budget,
        )
        return oracle, awm, output, dataset, model, state

    def test_initial_unobserved_lower_price_state_is_revealed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oed_path = _oed_config(root)
            _, _, _, dataset, model, state = self._fixture(root, oed_path)
            decision = OEDController(load_oed_config(oed_path)).decide(
                dataset,
                model,
                state,
                iteration=0,
                policy_kind="full_oed",
            )
            self.assertEqual("REVEAL", decision.selected_action)
            self.assertEqual(
                "D_local_digest",
                decision.selected_design_id,
            )
            self.assertEqual(
                ("D_local_digest",),
                decision.probe_candidates,
            )
            score = decision.candidate_scores[0]
            self.assertEqual(
                ("video_qa|multimodal_digest|2",),
                score.unresolved_price_states,
            )
            self.assertAlmostEqual(3.8, score.reveal_excursion.upper)

    def test_valuable_reveal_that_exceeds_cap_stops_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oed_path = _oed_config(root, budget=3.0, cap=3.0)
            _, _, _, dataset, model, state = self._fixture(root, oed_path)
            decision = OEDController(load_oed_config(oed_path)).decide(
                dataset,
                model,
                state,
                iteration=0,
                policy_kind="full_oed",
            )
            self.assertEqual("STOP", decision.selected_action)
            self.assertEqual("budget_limited_stop", decision.stopping_reason)

    def test_unreachable_candidate_produces_structural_hold(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oed_path = _oed_config(root, reveal_candidates=False)
            _, _, _, dataset, model, state = self._fixture(root, oed_path)
            decision = OEDController(load_oed_config(oed_path)).decide(
                dataset,
                model,
                state,
                iteration=0,
                policy_kind="full_oed",
            )
            self.assertEqual("HOLD", decision.selected_action)
            self.assertEqual("structural_hold", decision.stopping_reason)
            self.assertEqual(
                ("D_local_digest",),
                decision.other_candidates,
            )


class OEDReplayTest(unittest.TestCase):
    def test_v3_replay_exposes_cluster_component_certificate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle_path = _oracle_config(root)
            awm_path = _awm_config(
                root,
                confidence_level=0.5,
                quoted_price_sufficiency=False,
                schema_version="pathfinder.awm/v3alpha1",
            )
            output = root / "oed-v3-output"
            run_oed_replay(
                _oed_config(root),
                awm_path,
                oracle_path,
                oracle_output_dir=_synthetic_oracle_output(root),
                output_dir=output,
            )
            traces = [
                json.loads(line)
                for line in (output / "oed_trace.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            full = [
                row for row in traces
                if row["policy_kind"] == "full_oed"
            ]
            self.assertGreaterEqual(len(full), 2)
            candidate = next(
                score
                for score in full[1]["candidate_scores"]
                if score["design_id"] == "D_local_digest"
            )
            self.assertEqual(
                "paired-cluster-decomposed-kl-empirical-bernstein",
                candidate["gain_interval_source"],
            )
            self.assertEqual(16, candidate["paired_gain_raw_pair_count"])
            self.assertEqual(
                8,
                candidate["paired_gain_independent_unit_count"],
            )
            self.assertIsNotNone(
                candidate["paired_gain_success_difference"]
            )
            evaluation = json.loads(
                (output / "oed_evaluation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                "pathfinder.oed-replay/v3alpha1",
                evaluation["schema_version"],
            )
            self.assertEqual(
                "workload-cluster-first-paired-observation",
                evaluation["awm_confidence_contract"]["sampling_unit"],
            )

    def test_v2alpha2_reuses_one_frozen_snapshot_look(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle_path = _oracle_config(root)
            awm_path = _awm_config(
                root,
                confidence_level=0.5,
                quoted_price_sufficiency=False,
                schema_version="pathfinder.awm/v2alpha2",
            )
            output = root / "oed-v2alpha2-output"
            run_oed_replay(
                _oed_config(root),
                awm_path,
                oracle_path,
                oracle_output_dir=_synthetic_oracle_output(root),
                output_dir=output,
            )
            traces = [
                json.loads(line)
                for line in (output / "oed_trace.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            full = [
                row for row in traces
                if row["policy_kind"] == "full_oed"
            ]
            self.assertGreaterEqual(len(full), 2)
            contract = full[1]["awm_confidence_contract"]
            self.assertEqual(
                "fixed-training-snapshot-per-pair",
                contract["look_semantics"],
            )
            self.assertFalse(
                contract["repeated_fixed_snapshot_reads_count_as_new_looks"]
            )
            candidate = next(
                score
                for score in full[1]["candidate_scores"]
                if score["design_id"] == "D_local_digest"
            )
            self.assertEqual(
                "paired-fixed-snapshot-empirical-bernstein",
                candidate["gain_interval_source"],
            )
            self.assertIsNotNone(
                candidate["paired_gain_range_radius_per_session"]
            )
            self.assertEqual(
                64,
                len(candidate["paired_gain_training_snapshot_sha256"]),
            )
            evaluation = json.loads(
                (output / "oed_evaluation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                "pathfinder.oed-replay/v2alpha2",
                evaluation["schema_version"],
            )

    def test_v2_replay_switches_to_paired_gain_after_reveal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle_path = _oracle_config(root)
            awm_path = _awm_config(
                root,
                confidence_level=0.5,
                quoted_price_sufficiency=False,
                schema_version="pathfinder.awm/v2alpha1",
            )
            output = root / "oed-v2-output"
            run_oed_replay(
                _oed_config(root),
                awm_path,
                oracle_path,
                oracle_output_dir=_synthetic_oracle_output(root),
                output_dir=output,
            )
            traces = [
                json.loads(line)
                for line in (output / "oed_trace.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            full = [
                row for row in traces
                if row["policy_kind"] == "full_oed"
            ]
            self.assertGreaterEqual(len(full), 2)
            self.assertEqual("REVEAL", full[0]["selected_action"])
            candidate = next(
                score
                for score in full[1]["candidate_scores"]
                if score["design_id"] == "D_local_digest"
            )
            self.assertEqual(
                "paired-fixed-looks-empirical-bernstein",
                candidate["gain_interval_source"],
            )
            self.assertEqual(16, candidate["paired_gain_pair_count"])
            self.assertGreater(candidate["commit_gain_width"], 0.0)
            evaluation = json.loads(
                (output / "oed_evaluation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                "pathfinder.oed-replay/v2alpha1",
                evaluation["schema_version"],
            )

    def test_v2_replay_rejects_more_looks_than_preregistered(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            awm_path = _awm_config(
                root,
                schema_version="pathfinder.awm/v2alpha1",
                maximum_looks=2,
            )
            with self.assertRaisesRegex(ValueError, "maximum_looks"):
                run_oed_replay(
                    _oed_config(root),
                    awm_path,
                    _oracle_config(root),
                    oracle_output_dir=_synthetic_oracle_output(root),
                    output_dir=root / "oed-v2-output",
                )

    def test_replay_restores_after_reveal_then_commits_separately(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle_path = _oracle_config(root)
            awm_path = _awm_config(
                root,
                confidence_level=0.5,
                quoted_price_sufficiency=False,
            )
            oracle_output = _synthetic_oracle_output(root)
            output = root / "oed-output"
            manifest = run_oed_replay(
                _oed_config(root),
                awm_path,
                oracle_path,
                oracle_output_dir=oracle_output,
                output_dir=output,
            )
            evaluation = json.loads(
                (output / "oed_evaluation.json").read_text(encoding="utf-8")
            )
            traces = [
                json.loads(line)
                for line in (output / "oed_trace.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            full = [row for row in traces if row["policy_kind"] == "full_oed"]

            self.assertEqual("COMPLETE", manifest["status"])
            self.assertEqual(["REVEAL", "COMMIT"], [
                row["selected_action"] for row in full
            ])
            self.assertEqual(
                "D_remote_digest",
                full[0]["safe_design_id_after"],
            )
            self.assertIn(
                "D_local_digest",
                full[0]["observed_design_ids_after"],
            )
            self.assertEqual(
                "D_local_digest",
                full[1]["safe_design_id_after"],
            )
            policies = evaluation["policies"]
            self.assertTrue(policies["full_oed"]["reached_oracle_design"])
            self.assertFalse(
                evaluation["gate_b4_pre_server_checks"][
                    "full_oed_lower_cost_than_equal_budget_baselines"
                ]
            )
            self.assertEqual(
                0,
                policies["full_oed"]["safe_sequence_regression_count"],
            )
            self.assertEqual(
                "D_remote_digest",
                policies["passive_awm"]["final_safe_design_id"],
            )
            self.assertEqual(set(DESIGNS), set(
                policies["full_oed"]["observed_design_ids"]
            ))
            self.assertTrue((output / "oed_policy_summary.csv").is_file())

    def test_holdout_changes_evaluation_but_not_controller_actions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle_path = _oracle_config(root)
            awm_path = _awm_config(
                root,
                confidence_level=0.5,
                quoted_price_sufficiency=False,
            )
            oracle_output = _synthetic_oracle_output(root)
            oed_path = _oed_config(root)
            first = root / "first"
            run_oed_replay(
                oed_path,
                awm_path,
                oracle_path,
                oracle_output_dir=oracle_output,
                output_dir=first,
            )

            for design_id in DESIGNS:
                path = oracle_output / "designs" / design_id / "runs.jsonl"
                records = [
                    json.loads(line)
                    for line in path.read_text(encoding="utf-8").splitlines()
                ]
                for record in records:
                    if record["repetition"] == 2:
                        record["task_success"] = not record["task_success"]
                path.write_text(
                    "".join(
                        json.dumps(record, sort_keys=True) + "\n"
                        for record in records
                    ),
                    encoding="utf-8",
                )
            second = root / "second"
            run_oed_replay(
                oed_path,
                awm_path,
                oracle_path,
                oracle_output_dir=oracle_output,
                output_dir=second,
            )

            def actions(output: Path):
                return [
                    (
                        row["policy_kind"],
                        row["iteration"],
                        row["selected_action"],
                        row["selected_design_id"],
                    )
                    for row in map(
                        json.loads,
                        (output / "oed_trace.jsonl")
                        .read_text(encoding="utf-8")
                        .splitlines(),
                    )
                ]

            self.assertEqual(actions(first), actions(second))
            first_evaluation = json.loads(
                (first / "oed_evaluation.json").read_text(encoding="utf-8")
            )
            second_evaluation = json.loads(
                (second / "oed_evaluation.json").read_text(encoding="utf-8")
            )
            self.assertNotEqual(
                first_evaluation["policies"]["full_oed"][
                    "final_holdout_phi"
                ],
                second_evaluation["policies"]["full_oed"][
                    "final_holdout_phi"
                ],
            )


if __name__ == "__main__":
    unittest.main()
