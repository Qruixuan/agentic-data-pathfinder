"""Empirical Monte Carlo calibration of the v3alpha5 safety certificate."""

from __future__ import annotations

import contextlib
import copy
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pathfinder.awm import (
    AWMConfigError,
    CONTROL_BOUND_METHOD,
    calibrate_awm_certificate,
    load_calibration_config,
    run_certificate_monte_carlo,
)
from pathfinder.cli import main as cli_main


ROOT = Path(__file__).resolve().parents[1]
COMMITTED_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v2_awm_v3alpha5_calibration.json"
)
# Kept small so the suite stays fast. Every tolerance check consults the
# Monte Carlo interval rather than the point estimate, so a short run widens
# the interval instead of silently weakening the assertion.
TEST_SIMULATIONS = 200
# Output-shape tests do not need the full precision of the rate estimates.
OUTPUT_SIMULATIONS = 100
_REPORT_CACHE: dict[tuple[str, int], dict[str, Any]] = {}


def _cached_report(bound_method: str, simulations: int) -> dict[str, Any]:
    key = (bound_method, simulations)
    if key not in _REPORT_CACHE:
        _REPORT_CACHE[key] = run_certificate_monte_carlo(
            load_calibration_config(COMMITTED_CONFIG),
            bound_method=bound_method,
            simulations=simulations,
        )
    return _REPORT_CACHE[key]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _committed_payload() -> dict[str, Any]:
    return json.loads(COMMITTED_CONFIG.read_text(encoding="utf-8"))


def _config_with(root: Path, **changes: Any) -> Path:
    payload = _committed_payload()
    payload.update(changes)
    path = root / "calibration.json"
    _write_json(path, payload)
    return path


class CalibrationConfigTest(unittest.TestCase):
    def test_committed_plan_covers_every_required_regime(self) -> None:
        config = load_calibration_config(COMMITTED_CONFIG)
        self.assertEqual(90210, config.seed)
        self.assertEqual(2000, config.simulations)
        self.assertEqual(0.05, config.alpha)
        scenarios = {
            scenario.scenario_id for scenario in config.scenarios
        }
        self.assertEqual(
            {
                "success_at_non_inferiority_boundary",
                "success_just_beyond_harmful_boundary",
                "success_clearly_harmful",
                "clearly_safe_effect",
                "cost_at_minimum_saving_boundary",
                "cost_just_below_minimum_saving",
                "cost_clearly_below_minimum_saving",
                "complete_multi_stratum_family",
            },
            scenarios,
        )
        multi = next(
            scenario
            for scenario in config.scenarios
            if scenario.scenario_id == "complete_multi_stratum_family"
        )
        self.assertEqual(3, len(multi.strata))

    def test_boundary_scenarios_sit_exactly_on_their_thresholds(
        self,
    ) -> None:
        config = load_calibration_config(COMMITTED_CONFIG)
        by_id = {
            scenario.scenario_id: scenario.strata[0]
            for scenario in config.scenarios
        }
        self.assertAlmostEqual(
            -config.delta_success_margin,
            by_id["success_at_non_inferiority_boundary"]
            .true_success_difference,
        )
        self.assertAlmostEqual(
            config.minimum_cost_saving,
            by_id["cost_at_minimum_saving_boundary"].true_cost_saving,
        )
        self.assertLess(
            by_id["success_just_beyond_harmful_boundary"]
            .true_success_difference,
            -config.delta_success_margin,
        )
        self.assertLess(
            by_id["cost_just_below_minimum_saving"].true_cost_saving,
            config.minimum_cost_saving,
        )

    def test_a_generator_that_would_clip_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            payload = _committed_payload()
            payload["scenarios"] = [payload["scenarios"][0]]
            payload["scenarios"][0]["strata"][0][
                "true_success_difference"
            ] = -0.9
            path = Path(temporary) / "calibration.json"
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "leaves",
            ):
                load_calibration_config(path)

    def test_a_cost_draw_outside_its_support_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            payload = _committed_payload()
            payload["scenarios"] = [payload["scenarios"][0]]
            payload["scenarios"][0]["strata"][0]["true_cost_saving"] = 0.99
            path = Path(temporary) / "calibration.json"
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "cost saving leaves its declared support",
            ):
                load_calibration_config(path)

    def test_required_fields_are_enforced(self) -> None:
        for pointer in (
            ("seed",),
            ("simulations",),
            ("alpha",),
            ("thresholds", "delta_success_margin"),
            ("thresholds", "minimum_cost_saving"),
            ("tolerances", "monte_carlo_confidence"),
            ("tolerances", "false_safe_rate_tolerance"),
            ("tolerances", "coverage_tolerance"),
            ("tolerances", "control_minimum_false_safe_rate"),
        ):
            with self.subTest(pointer=pointer):
                with tempfile.TemporaryDirectory() as temporary:
                    payload = _committed_payload()
                    target = payload
                    for key in pointer[:-1]:
                        target = target[key]
                    target.pop(pointer[-1])
                    path = Path(temporary) / "calibration.json"
                    _write_json(path, payload)
                    with self.assertRaisesRegex(
                        AWMConfigError,
                        "required configuration field",
                    ):
                        load_calibration_config(path)

    def test_duplicate_scenario_ids_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            payload = _committed_payload()
            payload["scenarios"].append(
                copy.deepcopy(payload["scenarios"][0])
            )
            path = Path(temporary) / "calibration.json"
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "duplicate calibration scenario_id",
            ):
                load_calibration_config(path)


class _CalibrationCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_calibration_config(COMMITTED_CONFIG)
        cls.report = _cached_report(
            "workload-cluster-one-sided-bounded-mean-kl",
            TEST_SIMULATIONS,
        )
        cls.control = _cached_report(
            CONTROL_BOUND_METHOD,
            TEST_SIMULATIONS,
        )
        cls.by_scenario = {
            scenario["scenario_id"]: scenario
            for scenario in cls.report["scenarios"]
        }
        cls.control_by_scenario = {
            scenario["scenario_id"]: scenario
            for scenario in cls.control["scenarios"]
        }


class MonteCarloCalibrationTest(_CalibrationCase):

    def test_family_wise_false_safe_rate_is_within_alpha(self) -> None:
        rate = self.report["overall"]["family_wise_false_safe"]
        limit = self.config.alpha + self.config.false_safe_rate_tolerance
        self.assertLessEqual(
            rate["upper"],
            limit,
            f"family-wise false-safe rate {rate} exceeded {limit}",
        )
        self.assertEqual(TEST_SIMULATIONS * 8, rate["trials"])

    def test_every_scenario_family_is_within_alpha(self) -> None:
        limit = self.config.alpha + self.config.false_safe_rate_tolerance
        for scenario_id, scenario in self.by_scenario.items():
            with self.subTest(scenario=scenario_id):
                self.assertLessEqual(
                    scenario["family_wise_false_safe"]["upper"],
                    limit,
                )

    def test_simultaneous_coverage_meets_the_nominal_level(self) -> None:
        coverage = self.report["overall"]["simultaneous_coverage"]
        floor = 1.0 - self.config.alpha - self.config.coverage_tolerance
        self.assertGreaterEqual(coverage["lower"], floor)
        for scenario_id, scenario in self.by_scenario.items():
            with self.subTest(scenario=scenario_id):
                self.assertGreaterEqual(
                    scenario["simultaneous_coverage"]["lower"],
                    floor,
                )

    def test_the_certificate_is_not_vacuous(self) -> None:
        safe = self.by_scenario["clearly_safe_effect"]["strata"][0]
        self.assertGreater(
            safe["state_frequencies"]["SAFE_TO_COMMIT"],
            0.5,
            "a clearly safe effect must actually be committable",
        )
        multi = self.by_scenario["complete_multi_stratum_family"]
        committed = next(
            stratum
            for stratum in multi["strata"]
            if stratum["stratum_id"] == "safe_like_causal"
        )
        self.assertGreater(
            committed["state_frequencies"]["SAFE_TO_COMMIT"],
            0.5,
        )

    def test_clearly_harmful_effects_are_established_as_unsafe(self) -> None:
        for scenario_id in (
            "success_clearly_harmful",
            "cost_clearly_below_minimum_saving",
        ):
            with self.subTest(scenario=scenario_id):
                stratum = self.by_scenario[scenario_id]["strata"][0]
                self.assertGreater(
                    stratum["state_frequencies"]["UNSAFE"],
                    0.5,
                )
                self.assertEqual(
                    0.0,
                    stratum["state_frequencies"]["SAFE_TO_COMMIT"],
                )

    def test_marginally_harmful_effects_never_commit(self) -> None:
        for scenario_id in (
            "success_just_beyond_harmful_boundary",
            "cost_just_below_minimum_saving",
        ):
            with self.subTest(scenario=scenario_id):
                stratum = self.by_scenario[scenario_id]["strata"][0]
                self.assertTrue(stratum["truth_violates_a_gate"])
                self.assertEqual(
                    0.0,
                    stratum["state_frequencies"]["SAFE_TO_COMMIT"],
                )

    def test_boundary_effects_are_not_declared_harmful(self) -> None:
        for scenario_id in (
            "success_at_non_inferiority_boundary",
            "cost_at_minimum_saving_boundary",
        ):
            with self.subTest(scenario=scenario_id):
                stratum = self.by_scenario[scenario_id]["strata"][0]
                self.assertFalse(stratum["truth_violates_a_gate"])
                self.assertEqual(
                    0.0,
                    stratum["false_unsafe"]["rate"],
                    "an effect exactly on the threshold must not be "
                    "declared harmful",
                )

    def test_every_non_safe_state_falls_back_to_origin(self) -> None:
        for scenario in self.report["scenarios"]:
            for stratum in scenario["strata"]:
                non_safe = (
                    stratum["state_counts"]["UNSAFE"]
                    + stratum["state_counts"]["INSUFFICIENT_EVIDENCE"]
                )
                self.assertEqual(
                    non_safe,
                    stratum["origin_fallback_count"],
                )

    def test_all_three_states_occur_across_the_plan(self) -> None:
        totals = {"SAFE_TO_COMMIT": 0, "UNSAFE": 0, "INSUFFICIENT_EVIDENCE": 0}
        for scenario in self.report["scenarios"]:
            for stratum in scenario["strata"]:
                for state, count in stratum["state_counts"].items():
                    totals[state] += count
        for state, count in totals.items():
            with self.subTest(state=state):
                self.assertGreater(count, 0)

    def test_the_multi_stratum_family_uses_the_full_adjustment(self) -> None:
        multi = self.by_scenario["complete_multi_stratum_family"]
        self.assertEqual(12, multi["family_size"])
        self.assertAlmostEqual(0.05 / 12, multi["adjusted_alpha"])
        self.assertAlmostEqual(0.95, multi["unadjusted_confidence_level"])
        self.assertAlmostEqual(
            1.0 - 0.05 / 12,
            multi["adjusted_confidence_level"],
        )
        self.assertEqual(12, len(multi["family_components"]))
        self.assertEqual(
            {
                (stratum["stratum_id"], gate, side)
                for stratum in multi["strata"]
                for gate in (
                    "success_non_inferiority",
                    "cost_improvement",
                )
                for side in ("lower", "upper")
            },
            {
                (
                    component["stratum_id"],
                    component["gate_id"],
                    component["side"],
                )
                for component in multi["family_components"]
            },
        )

    def test_a_larger_family_is_more_conservative(self) -> None:
        single = self.by_scenario["clearly_safe_effect"]
        multi = self.by_scenario["complete_multi_stratum_family"]
        self.assertGreater(single["adjusted_alpha"], multi["adjusted_alpha"])
        committed = next(
            stratum
            for stratum in multi["strata"]
            if stratum["stratum_id"] == "safe_like_causal"
        )
        self.assertLessEqual(
            committed["state_frequencies"]["SAFE_TO_COMMIT"],
            single["strata"][0]["state_frequencies"]["SAFE_TO_COMMIT"],
        )

    def test_monte_carlo_uncertainty_is_reported_for_every_rate(
        self,
    ) -> None:
        rate = self.report["overall"]["family_wise_false_safe"]
        for key in ("rate", "lower", "upper", "confidence", "events",
                    "trials"):
            self.assertIn(key, rate)
        self.assertEqual(
            self.config.monte_carlo_confidence,
            rate["confidence"],
        )
        self.assertLessEqual(rate["lower"], rate["rate"])
        self.assertLessEqual(rate["rate"], rate["upper"])
        self.assertLess(rate["lower"], rate["upper"])
        self.assertEqual(TEST_SIMULATIONS, self.report[
            "simulations_per_scenario"
        ])
        self.assertEqual(90210, self.report["seed"])

    def test_the_run_is_reproducible_from_its_seed(self) -> None:
        repeated = run_certificate_monte_carlo(
            self.config,
            simulations=TEST_SIMULATIONS,
        )
        self.assertEqual(
            json.dumps(self.report, sort_keys=True),
            json.dumps(repeated, sort_keys=True),
        )

    def test_a_different_seed_changes_the_draws(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            other = load_calibration_config(
                _config_with(Path(temporary), seed=4242)
            )
            report = run_certificate_monte_carlo(
                other,
                simulations=TEST_SIMULATIONS,
            )
        baseline = self.by_scenario["complete_multi_stratum_family"]
        changed = next(
            scenario
            for scenario in report["scenarios"]
            if scenario["scenario_id"] == "complete_multi_stratum_family"
        )
        self.assertNotEqual(
            [s["state_counts"] for s in baseline["strata"]],
            [s["state_counts"] for s in changed["strata"]],
        )


class NegativeControlTest(_CalibrationCase):
    """The control proves a zero false-safe rate is a real result."""

    def test_the_control_is_detected_as_miscalibrated(self) -> None:
        worst = max(
            self.control["scenarios"],
            key=lambda scenario: (
                scenario["family_wise_false_safe"]["lower"]
            ),
        )
        self.assertGreaterEqual(
            worst["family_wise_false_safe"]["lower"],
            self.config.control_minimum_false_safe_rate,
            "the harness must detect an anti-conservative bound",
        )
        self.assertTrue(self.control["is_negative_control"])

    def test_the_control_commits_where_the_certificate_refuses(self) -> None:
        scenario_id = "success_just_beyond_harmful_boundary"
        certificate = self.by_scenario[scenario_id]
        control = self.control_by_scenario[scenario_id]
        self.assertEqual(
            0.0,
            certificate["family_wise_false_safe"]["rate"],
        )
        self.assertGreater(
            control["family_wise_false_safe"]["rate"],
            self.config.alpha,
        )
        self.assertGreater(
            control["family_wise_false_safe"]["rate"],
            certificate["family_wise_false_safe"]["rate"],
        )

    def test_the_control_fails_coverage(self) -> None:
        self.assertLess(
            self.control["overall"]["simultaneous_coverage"]["rate"],
            1.0 - self.config.alpha,
            "a point estimate reported as a bound cannot cover",
        )
        self.assertGreater(
            self.report["overall"]["simultaneous_coverage"]["rate"],
            self.control["overall"]["simultaneous_coverage"]["rate"],
        )


class CalibrationOutputTest(unittest.TestCase):
    def _run(
        self,
        root: Path,
        name: str = "calibration-output",
        *,
        simulations: int = OUTPUT_SIMULATIONS,
    ):
        output = root / name
        manifest = calibrate_awm_certificate(
            COMMITTED_CONFIG,
            output_dir=output,
            simulations=simulations,
        )
        return manifest, output

    def test_every_preregistered_check_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest, output = self._run(
                Path(temporary),
                simulations=TEST_SIMULATIONS,
            )
            self.assertTrue(
                manifest["all_checks_passed"],
                manifest["failed_checks"],
            )
            report = json.loads(
                (output / "calibration_report.json").read_text(
                    encoding="utf-8"
                )
            )
            checks = {
                check["check_id"]: check for check in report["checks"]
            }
            self.assertEqual(
                {
                    "family_wise_false_safe_rate_within_alpha",
                    "simultaneous_coverage_at_least_nominal",
                    "every_scenario_family_within_alpha",
                    "negative_control_is_detected_as_miscalibrated",
                    "certificate_strictly_safer_than_control",
                },
                set(checks),
            )
            for check_id, check in checks.items():
                with self.subTest(check=check_id):
                    self.assertTrue(check["passed"], check)

    def test_outputs_are_machine_readable_and_flagged_synthetic(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest, output = self._run(Path(temporary))
            self.assertTrue(manifest["synthetic"])
            self.assertTrue(manifest["posthoc"])
            self.assertFalse(manifest["eligible_for_scientific_claims"])
            self.assertFalse(manifest["deployment_mutations_performed"])
            self.assertFalse(manifest["secrets_recorded"])
            self.assertEqual(90210, manifest["seed"])
            self.assertEqual(
                OUTPUT_SIMULATIONS,
                manifest["simulations_per_scenario"],
            )
            report = json.loads(
                (output / "calibration_report.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(report["synthetic"])
            self.assertIsNotNone(report["negative_control"])
            with (output / "calibration_summary.csv").open(
                encoding="utf-8",
                newline="",
            ) as handle:
                rows = list(csv.DictReader(handle))
            methods = {row["bound_method"] for row in rows}
            self.assertEqual(2, len(methods))
            for row in rows:
                for column in (
                    "scenario_id",
                    "stratum_id",
                    "simulations",
                    "seed",
                    "workload_count",
                    "repetition_pair_count",
                    "family_size",
                    "adjusted_alpha",
                    "false_safe_rate",
                    "success_coverage_rate",
                    "cost_coverage_rate",
                    "safe_to_commit_frequency",
                    "unsafe_frequency",
                    "insufficient_evidence_frequency",
                ):
                    self.assertIn(column, row)

    def test_reruns_are_byte_identical(self) -> None:
        payloads = []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            hashes = []
            for index in range(2):
                manifest, output = self._run(root, f"output-{index}")
                hashes.append(manifest["calibration_snapshot_sha256"])
                payloads.append(tuple(
                    (output / name).read_text(encoding="utf-8")
                    for name in (
                        "calibration_summary.csv",
                        "calibration_report.json",
                    )
                ))
            self.assertEqual(payloads[0], payloads[1])
            self.assertEqual(hashes[0], hashes[1])

    def test_a_nonempty_output_directory_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            occupied = Path(temporary) / "output"
            occupied.mkdir(parents=True)
            (occupied / "existing.txt").write_text("x", encoding="utf-8")
            with self.assertRaisesRegex(AWMConfigError, "is not empty"):
                calibrate_awm_certificate(
                    COMMITTED_CONFIG,
                    output_dir=occupied,
                    simulations=10,
                )

    def test_cli_runs_the_calibration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "cli-output"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = cli_main([
                    "calibrate-awm-certificate",
                    "--calibration-config",
                    str(COMMITTED_CONFIG),
                    "--output-dir",
                    str(output),
                    "--simulations",
                    str(TEST_SIMULATIONS),
                    "--compact",
                ])
            self.assertEqual(0, exit_code)
            payload = json.loads(stdout.getvalue())
            self.assertEqual("COMPLETE", payload["status"])
            self.assertTrue(payload["all_checks_passed"])
            self.assertTrue(
                (output / "calibration_report.json").is_file()
            )
            self.assertTrue(
                (output / "calibration_manifest.json").is_file()
            )


if __name__ == "__main__":
    unittest.main()
