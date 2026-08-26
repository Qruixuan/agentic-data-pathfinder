from __future__ import annotations

import contextlib
import csv
import io
import json
import math
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping, Sequence

from pathfinder.awm import (
    AWMConfigError,
    certify_awm_restricted_policy,
    load_workload_safety_certificate_config,
    one_sided_bounded_mean_bounds,
)
from pathfinder.cli import main as cli_main


ROOT = Path(__file__).resolve().parents[1]
SYSTEM_PATH = ROOT / "configs" / "multi_candidate_formal_v2_system.json"
COMMITTED_CONFIG = (
    ROOT
    / "configs"
    / "multi_candidate_formal_v2_awm_v3alpha5_certificate.json"
)
SAFE = "D_origin_remote"
FRAMES = "D_local_frames"
DIGEST = "D_local_digest"
PAIR = "D_local_pair"
DESIGN_IDS = (SAFE, FRAMES, DIGEST, PAIR)
SUCCESS_SUPPORT = {"lower": -1.0, "upper": 1.0}
COST_SUPPORT = {"lower": -1.0, "upper": 1.0}


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _workload_ids(stratum: str, count: int) -> tuple[str, ...]:
    return tuple(f"{stratum}-w{index:02d}" for index in range(count))


class _Cell:
    """One declared (design, stratum) response cell of a synthetic Oracle."""

    def __init__(self, successes: int, cost: float) -> None:
        self.successes = successes
        self.cost = cost


class _Fixture:
    """A deterministic synthetic Reduced Oracle for certificate calibration.

    ``plan`` maps ``(design_id, stratum_id)`` to the number of successes in a
    *base* repetition block and the per-run service cost. Repetitions beyond
    ``base_repetitions`` repeat the base block exactly, which is how the
    repetition-duplication calibration is expressed.
    """

    def __init__(
        self,
        root: Path,
        *,
        strata: Mapping[str, Sequence[str]],
        plan: Mapping[tuple[str, str], _Cell],
        base_repetitions: int = 4,
        repetitions: int | None = None,
        design_ids: Sequence[str] = DESIGN_IDS,
    ) -> None:
        self.root = root
        self.strata = {key: tuple(value) for key, value in strata.items()}
        self.plan = plan
        self.base_repetitions = base_repetitions
        self.repetitions = repetitions or base_repetitions
        self.design_ids = tuple(design_ids)
        self.workload_ids = tuple(
            workload_id
            for workloads in self.strata.values()
            for workload_id in workloads
        )
        self.stratum_of = {
            workload_id: stratum
            for stratum, workloads in self.strata.items()
            for workload_id in workloads
        }
        self.pilot_path = self._pilot()
        self.oracle_path = self._oracle()
        self.oracle_output = self._oracle_output()

    def _pilot(self) -> Path:
        path = self.root / "pilot.json"
        _write_json(path, {
            "schema_version": "pathfinder.flowmesh-pilot/v1alpha1",
            "experiment_id": "certificate-test",
            "system_config": str(SYSTEM_PATH),
            "design_ids": list(self.design_ids),
            "task_class_id": "video_qa",
            "quote_profile_ids": ["as_designed"],
            "latency_multipliers": [1],
            "repetitions": self.repetitions,
            "base_seed": 10,
            "randomization_seed": 20,
            "workloads": [
                {
                    "id": workload_id,
                    "object_id": f"object-{workload_id}",
                    "question": f"Question for {workload_id}?",
                    "accepted_answer_substrings": ["answer"],
                }
                for workload_id in self.workload_ids
            ],
        })
        return path

    def _oracle(self) -> Path:
        path = self.root / "oracle.json"
        _write_json(path, {
            "schema_version": "pathfinder.reduced-oracle/v1alpha1",
            "oracle_id": "certificate-test-oracle",
            "workload_pilot_config": str(self.pilot_path),
            "safe_design_id": SAFE,
            "design_order": list(self.design_ids),
            "quote_profile_id": "as_designed",
            "latency_multiplier": 1,
            "repetitions": self.repetitions,
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
                "candidate_design_id": FRAMES,
                "representation_id": "multimodal_digest",
                "decision_margin": 0,
            },
            "designs": [
                {
                    "design_id": design_id,
                    "materialization_decision": (
                        "reuse" if design_id == SAFE else "copy"
                    ),
                    "placement_decision": (
                        "origin" if design_id == SAFE else "local"
                    ),
                    "execution_decision": "flowmesh",
                    "materializations": [],
                }
                for design_id in self.design_ids
            ],
        })
        return path

    def record(
        self,
        design_id: str,
        workload_id: str,
        repetition: int,
    ) -> dict[str, Any]:
        cell = self.plan[(design_id, self.stratum_of[workload_id])]
        base_index = repetition % self.base_repetitions
        return {
            "trial_key": f"{workload_id}|{design_id}|1|r{repetition:04d}",
            "workload_id": workload_id,
            "design_id": design_id,
            "task_class_id": "video_qa",
            "quote_profile_id": "as_designed",
            "repetition": repetition,
            "seed": 10 + repetition,
            "latency_multiplier": 1,
            "outcome_type": "completed",
            "telemetry_complete": True,
            "task_success": base_index < cell.successes,
            "selected_representations": ["sampled_frames"],
            "access_events": [{
                "accepted": True,
                "event_id": 1,
                "representation_id": "sampled_frames",
                "realized_cost": cell.cost,
                "artifact_handle_sha256": "a" * 64,
                "artifact_download_request_count": 1,
                "artifact_full_download_count": 1,
                "artifact_bytes_sent": 128,
            }],
        }

    def _oracle_output(self) -> Path:
        output = self.root / "oracle-output"
        for design_id in self.design_ids:
            self.write_runs(output, design_id, [
                self.record(design_id, workload_id, repetition)
                for workload_id in self.workload_ids
                for repetition in range(self.repetitions)
            ])
        table = output / "oracle_table.csv"
        table.parent.mkdir(parents=True, exist_ok=True)
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
            for design_id in self.design_ids:
                writer.writerow({
                    "design_id": design_id,
                    "storage_cost": 0,
                    "forward_transition_cost": 0,
                    "restoration_cost": 0,
                })
        return output

    @staticmethod
    def write_runs(
        output: Path,
        design_id: str,
        records: Sequence[Mapping[str, Any]],
    ) -> Path:
        path = output / "designs" / design_id / "runs.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(
                json.dumps(record, sort_keys=True) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )
        return path

    @staticmethod
    def read_runs(output: Path, design_id: str) -> list[dict[str, Any]]:
        path = output / "designs" / design_id / "runs.jsonl"
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]

    def certificate_config(
        self,
        *,
        name: str = "certificate.json",
        candidates: Mapping[str, str] | None = None,
        strata: Sequence[str] | None = None,
        delta_success_margin: float = 0.05,
        minimum_cost_saving: float = 0.1,
        minimum_independent_workloads: int = 1,
        alpha: float = 0.05,
        selection_workload_ids: Sequence[str] = (),
        excluded_design_ids: Sequence[str] = (PAIR,),
        cost_support: Mapping[str, float] = COST_SUPPORT,
        overrides: Mapping[str, Any] | None = None,
    ) -> Path:
        chosen = dict(candidates or {key: FRAMES for key in self.strata})
        included = tuple(strata if strata is not None else self.strata)
        certificate_workloads = [
            workload_id
            for stratum in included
            for workload_id in self.strata[stratum]
        ]
        payload: dict[str, Any] = {
            "schema_version": "pathfinder.awm-safety-certificate/v1alpha1",
            "certificate_id": "certificate-unit-test",
            "mode": "posthoc",
            "posthoc": True,
            "eligible_for_scientific_claims": False,
            "safe_design_id": SAFE,
            "design_ids": list(self.design_ids),
            "excluded_design_ids": list(excluded_design_ids),
            "candidate_restriction": {
                "provenance": "posthoc-selected-on-inspected-oracle",
                "note": "synthetic calibration fixture",
            },
            "strata": {
                stratum: {
                    "candidate_design_id": chosen[stratum],
                    "workload_ids": list(self.strata[stratum]),
                }
                for stratum in included
            },
            "workload_split": {
                "selection_workload_ids": list(selection_workload_ids),
                "certificate_workload_ids": certificate_workloads,
            },
            "confidence": {
                "alpha": alpha,
                "family_adjustment": "bonferroni",
                "bound_method": (
                    "workload-cluster-one-sided-bounded-mean-kl"
                ),
                "minimum_independent_workloads": (
                    minimum_independent_workloads
                ),
            },
            "thresholds": {
                "delta_success_margin": delta_success_margin,
                "minimum_cost_saving": minimum_cost_saving,
                "provenance": "posthoc-engineering-placeholder",
            },
            "supports": {
                "success_difference": dict(SUCCESS_SUPPORT),
                "cost_saving": dict(cost_support),
            },
            "estimand": {
                "target": "finite-supplied-workload-set",
                "workload_sampling_mechanism": "none-declared",
                "workload_selection_rule": "none-declared",
            },
            "preregistration": {
                "declared_before_evaluation_outcomes": False,
                "declaration_id": "none-synthetic",
                "declaration_sha256": "0" * 64,
                "inspected_oracle_snapshot_sha256": [],
            },
        }
        for key, value in (overrides or {}).items():
            if isinstance(value, Mapping) and isinstance(
                payload.get(key),
                dict,
            ):
                payload[key] = {**payload[key], **value}
            else:
                payload[key] = value
        path = self.root / name
        _write_json(path, payload)
        return path

    def certify(
        self,
        certificate_config: Path,
        *,
        output_name: str = "certificate-output",
        oracle_output: Path | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, str]], Path]:
        output = self.root / output_name
        manifest = certify_awm_restricted_policy(
            certificate_config,
            self.oracle_path,
            oracle_output_dir=oracle_output or self.oracle_output,
            output_dir=output,
        )
        with (output / "certificate_summary.csv").open(
            encoding="utf-8",
            newline="",
        ) as handle:
            rows = list(csv.DictReader(handle))
        return manifest, rows, output


def _plan(
    strata: Sequence[str],
    *,
    origin: _Cell,
    candidate: _Cell,
) -> dict[tuple[str, str], _Cell]:
    plan: dict[tuple[str, str], _Cell] = {}
    for stratum in strata:
        plan[(SAFE, stratum)] = origin
        for design_id in (FRAMES, DIGEST, PAIR):
            plan[(design_id, stratum)] = candidate
    return plan


def _safe_fixture(root: Path, *, workloads: int = 16, **kwargs: Any):
    return _Fixture(
        root,
        strata={"causal": _workload_ids("causal", workloads)},
        plan=_plan(
            ("causal",),
            origin=_Cell(successes=1, cost=1.0),
            candidate=_Cell(successes=4, cost=0.2),
        ),
        **kwargs,
    )


def _harmful_fixture(root: Path, *, workloads: int = 16, **kwargs: Any):
    return _Fixture(
        root,
        strata={"causal": _workload_ids("causal", workloads)},
        plan=_plan(
            ("causal",),
            origin=_Cell(successes=4, cost=1.0),
            candidate=_Cell(successes=0, cost=0.2),
        ),
        **kwargs,
    )


def _cost_neutral_fixture(root: Path, *, workloads: int = 16, **kwargs: Any):
    return _Fixture(
        root,
        strata={"causal": _workload_ids("causal", workloads)},
        plan=_plan(
            ("causal",),
            origin=_Cell(successes=1, cost=1.0),
            candidate=_Cell(successes=4, cost=1.0),
        ),
        **kwargs,
    )


class OneSidedBoundedMeanTest(unittest.TestCase):
    def test_bounds_invert_the_binary_relative_entropy(self) -> None:
        bounds = one_sided_bounded_mean_bounds(0.75, 16, 0.0125)
        threshold = math.log(1.0 / 0.0125) / 16

        def kl(probability: float, candidate: float) -> float:
            return (
                probability * math.log(probability / candidate)
                + (1.0 - probability)
                * math.log((1.0 - probability) / (1.0 - candidate))
            )

        self.assertLess(bounds.lower, 0.75)
        self.assertGreater(bounds.upper, 0.75)
        self.assertAlmostEqual(threshold, kl(0.75, bounds.lower), places=6)
        self.assertAlmostEqual(threshold, kl(0.75, bounds.upper), places=6)

    def test_more_independent_units_tighten_both_tails(self) -> None:
        few = one_sided_bounded_mean_bounds(0.75, 4, 0.0125)
        many = one_sided_bounded_mean_bounds(0.75, 64, 0.0125)
        self.assertGreater(many.lower, few.lower)
        self.assertLess(many.upper, few.upper)

    def test_one_sided_bound_is_tighter_than_the_two_sided_split(
        self,
    ) -> None:
        one_sided = one_sided_bounded_mean_bounds(0.75, 16, 0.05)
        half = one_sided_bounded_mean_bounds(0.75, 16, 0.025)
        self.assertGreater(one_sided.lower, half.lower)

    def test_degenerate_means_stay_inside_the_unit_interval(self) -> None:
        low = one_sided_bounded_mean_bounds(0.0, 8, 0.01)
        high = one_sided_bounded_mean_bounds(1.0, 8, 0.01)
        self.assertEqual(0.0, low.lower)
        self.assertLess(low.upper, 1.0)
        self.assertEqual(1.0, high.upper)
        self.assertGreater(high.lower, 0.0)


class CertificateConfigTest(unittest.TestCase):
    def test_committed_posthoc_config_declares_the_restricted_policy(
        self,
    ) -> None:
        config = load_workload_safety_certificate_config(COMMITTED_CONFIG)
        self.assertEqual("posthoc", config.mode)
        self.assertTrue(config.posthoc)
        self.assertFalse(config.eligible_for_scientific_claims)
        self.assertEqual((PAIR,), config.excluded_design_ids)
        self.assertEqual(16, len(config.certificate_workload_ids))
        self.assertEqual((), config.selection_workload_ids)
        chosen = {
            stratum.stratum_id: stratum.candidate_design_id
            for stratum in config.strata
        }
        self.assertEqual(
            {
                "causal": FRAMES,
                "descriptive": FRAMES,
                "temporal": DIGEST,
            },
            chosen,
        )
        self.assertEqual(12, config.family_size)
        self.assertAlmostEqual(0.05 / 12, config.adjusted_alpha)
        self.assertEqual(
            "posthoc-selected-on-inspected-oracle",
            config.candidate_restriction_provenance,
        )
        self.assertIn("POST-HOC", config.candidate_restriction_note)

    def test_posthoc_config_cannot_claim_scientific_eligibility(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(
                overrides={"eligible_for_scientific_claims": True},
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "eligible_for_scientific_claims=false",
            ):
                load_workload_safety_certificate_config(path)

    def test_posthoc_config_cannot_declare_preregistered_thresholds(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(overrides={"thresholds": {
                "provenance": "preregistered-before-evaluation-outcomes",
            }})
            with self.assertRaisesRegex(
                AWMConfigError,
                "cannot declare preregistered thresholds",
            ):
                load_workload_safety_certificate_config(path)

    def test_thresholds_are_required_configuration_fields(self) -> None:
        for missing in ("delta_success_margin", "minimum_cost_saving"):
            with self.subTest(missing=missing):
                with tempfile.TemporaryDirectory() as temporary:
                    fixture = _safe_fixture(Path(temporary), workloads=4)
                    path = fixture.certificate_config()
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["thresholds"].pop(missing)
                    _write_json(path, payload)
                    with self.assertRaisesRegex(
                        AWMConfigError,
                        "required configuration field",
                    ):
                        load_workload_safety_certificate_config(path)

    def test_alpha_is_a_required_configuration_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config()
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["confidence"].pop("alpha")
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "required configuration field",
            ):
                load_workload_safety_certificate_config(path)

    def test_excluded_design_cannot_be_a_stratum_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(
                candidates={"causal": PAIR},
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "excluded from the restricted search",
            ):
                load_workload_safety_certificate_config(path)

    def test_confirmatory_mode_requires_a_declared_split_and_thresholds(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(overrides={
                "mode": "confirmatory",
                "posthoc": False,
            })
            with self.assertRaisesRegex(
                AWMConfigError,
                "preregistration_declared_before_evaluation_outcomes",
            ):
                load_workload_safety_certificate_config(path)

    def test_confirmatory_mode_requires_a_nonempty_selection_set(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(overrides={
                "mode": "confirmatory",
                "posthoc": False,
                "candidate_restriction": {
                    "provenance": (
                        "preregistered-before-evaluation-outcomes"
                    ),
                    "note": "declared in advance",
                },
                "thresholds": {
                    "provenance": (
                        "preregistered-before-evaluation-outcomes"
                    ),
                },
                "estimand": {
                    "target": "finite-supplied-workload-set",
                    "workload_sampling_mechanism": "none-declared",
                    "workload_selection_rule": (
                        "preregistered-deterministic-expansion"
                    ),
                },
                "preregistration": {
                    "declared_before_evaluation_outcomes": True,
                },
            })
            with self.assertRaisesRegex(
                AWMConfigError,
                "selection_and_certification_sets_nonempty_and_disjoint",
            ):
                load_workload_safety_certificate_config(path)

    def test_confirmatory_mode_cannot_reuse_an_inspected_snapshot(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _Fixture(
                root,
                strata={
                    "causal": _workload_ids("causal", 4),
                    "holdout": _workload_ids("holdout", 2),
                },
                plan=_plan(
                    ("causal", "holdout"),
                    origin=_Cell(successes=1, cost=1.0),
                    candidate=_Cell(successes=4, cost=0.2),
                ),
            )
            posthoc = fixture.certificate_config(strata=("causal",))
            manifest, _, _ = fixture.certify(posthoc)
            inspected = manifest["oracle_full_snapshot_sha256"]
            confirmatory = fixture.certificate_config(
                name="confirmatory.json",
                strata=("causal",),
                selection_workload_ids=fixture.strata["holdout"],
                overrides={
                    "mode": "confirmatory",
                    "posthoc": False,
                    "candidate_restriction": {
                        "provenance": (
                            "preregistered-before-evaluation-outcomes"
                        ),
                        "note": "declared in advance",
                    },
                    "thresholds": {
                        "provenance": (
                            "preregistered-before-evaluation-outcomes"
                        ),
                    },
                    "estimand": {
                        "target": "finite-supplied-workload-set",
                        "workload_sampling_mechanism": "none-declared",
                        "workload_selection_rule": (
                            "preregistered-deterministic-expansion"
                        ),
                    },
                    "preregistration": {
                        "declared_before_evaluation_outcomes": True,
                        "inspected_oracle_snapshot_sha256": [inspected],
                    },
                },
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "already inspected",
            ):
                fixture.certify(
                    confirmatory,
                    output_name="confirmatory-output",
                )


def _preregistered_overrides(**extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "mode": "confirmatory",
        "posthoc": False,
        "candidate_restriction": {
            "provenance": "preregistered-before-evaluation-outcomes",
            "note": "declared in advance",
        },
        "thresholds": {
            "provenance": "preregistered-before-evaluation-outcomes",
        },
        "estimand": {
            "target": "finite-supplied-workload-set",
            "workload_sampling_mechanism": "none-declared",
            "workload_selection_rule": (
                "preregistered-deterministic-expansion"
            ),
        },
        "preregistration": {
            "declared_before_evaluation_outcomes": True,
        },
    }
    payload.update(extra)
    return payload


class EstimandTest(unittest.TestCase):
    def test_committed_posthoc_config_targets_the_supplied_workloads(
        self,
    ) -> None:
        config = load_workload_safety_certificate_config(COMMITTED_CONFIG)
        self.assertEqual(
            "finite-supplied-workload-set",
            config.estimand_target,
        )
        self.assertEqual(
            "none-declared",
            config.workload_sampling_mechanism,
        )
        self.assertEqual("none-declared", config.workload_selection_rule)
        self.assertFalse(config.generalizes_beyond_the_supplied_workloads)

    def test_estimand_is_a_required_configuration_block(self) -> None:
        for missing in (
            "target",
            "workload_sampling_mechanism",
            "workload_selection_rule",
        ):
            with self.subTest(missing=missing):
                with tempfile.TemporaryDirectory() as temporary:
                    fixture = _safe_fixture(Path(temporary), workloads=4)
                    path = fixture.certificate_config()
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["estimand"].pop(missing)
                    _write_json(path, payload)
                    with self.assertRaisesRegex(
                        AWMConfigError,
                        "required configuration field",
                    ):
                        load_workload_safety_certificate_config(path)

    def test_posthoc_mode_refuses_a_superpopulation_estimand(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(overrides={"estimand": {
                "target": "superpopulation-of-future-workloads",
                "workload_sampling_mechanism": "uniform-random-from-corpus",
                "workload_selection_rule": "preregistered-random-draw",
            }})
            with self.assertRaisesRegex(
                AWMConfigError,
                "cannot claim a superpopulation estimand",
            ):
                load_workload_safety_certificate_config(path)

    def test_superpopulation_requires_a_declared_sampling_mechanism(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(
                overrides=_preregistered_overrides(estimand={
                    "target": "superpopulation-of-future-workloads",
                    "workload_sampling_mechanism": "none-declared",
                    "workload_selection_rule": "preregistered-random-draw",
                }),
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "requires a declared",
            ):
                load_workload_safety_certificate_config(path)

    def test_evaluation_records_the_estimand_and_denies_generalization(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            _, _, output = fixture.certify(fixture.certificate_config())
            evaluation = json.loads(
                (output / "certificate_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            estimand = evaluation["estimand"]
            self.assertEqual(
                "finite-supplied-workload-set",
                estimand["target"],
            )
            self.assertFalse(estimand["generalization_claimed"])
            self.assertFalse(
                estimand["repetitions_increase_independent_units"]
            )
            self.assertIn("NOT a generalization claim", estimand["note"])
            self.assertEqual(
                "workload-object-cluster",
                estimand["independent_unit"],
            )


class ConfirmatoryEligibilityTest(unittest.TestCase):
    def test_clearing_posthoc_alone_does_not_grant_confirmatory_status(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(
                overrides={"posthoc": False},
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "must declare posthoc=true",
            ):
                load_workload_safety_certificate_config(path)

    def test_switching_only_the_mode_lists_every_unmet_requirement(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(overrides={
                "mode": "confirmatory",
                "posthoc": False,
            })
            with self.assertRaises(AWMConfigError) as caught:
                load_workload_safety_certificate_config(path)
            message = str(caught.exception)
            for requirement in (
                "preregistration_declared_before_evaluation_outcomes",
                "thresholds_preregistered",
                "candidate_restriction_preregistered",
                "workload_selection_rule_preregistered",
                "selection_and_certification_sets_nonempty_and_disjoint",
                "no_posthoc_provenance_flags",
            ):
                self.assertIn(requirement, message)

    def test_eligibility_cannot_be_asserted_while_requirements_are_unmet(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config()
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["mode"] = "posthoc"
            payload["posthoc"] = True
            payload["eligible_for_scientific_claims"] = True
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "eligible_for_scientific_claims=false",
            ):
                load_workload_safety_certificate_config(path)

    def test_placeholder_thresholds_block_confirmatory_status(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            overrides = _preregistered_overrides()
            overrides["thresholds"] = {
                "provenance": "posthoc-engineering-placeholder",
            }
            path = fixture.certificate_config(
                selection_workload_ids=("held-out-w0",),
                overrides=overrides,
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "thresholds_preregistered",
            ):
                load_workload_safety_certificate_config(path)

    def test_posthoc_candidate_restriction_blocks_confirmatory_status(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            overrides = _preregistered_overrides()
            overrides["candidate_restriction"] = {
                "provenance": "posthoc-selected-on-inspected-oracle",
                "note": "still post-hoc",
            }
            path = fixture.certificate_config(
                selection_workload_ids=("held-out-w0",),
                overrides=overrides,
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "candidate_restriction_preregistered",
            ):
                load_workload_safety_certificate_config(path)

    def test_an_undeclared_selection_rule_blocks_confirmatory_status(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            overrides = _preregistered_overrides(estimand={
                "target": "finite-supplied-workload-set",
                "workload_sampling_mechanism": "none-declared",
                "workload_selection_rule": "none-declared",
            })
            path = fixture.certificate_config(
                selection_workload_ids=("held-out-w0",),
                overrides=overrides,
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "workload_selection_rule_preregistered",
            ):
                load_workload_safety_certificate_config(path)

    def test_the_committed_posthoc_config_reports_every_unmet_requirement(
        self,
    ) -> None:
        config = load_workload_safety_certificate_config(COMMITTED_CONFIG)
        self.assertFalse(config.confirmatory_static_requirements_met)
        self.assertEqual(
            {
                "mode_is_confirmatory",
                "posthoc_flag_cleared",
                "preregistration_declared_before_evaluation_outcomes",
                "thresholds_preregistered",
                "candidate_restriction_preregistered",
                "workload_selection_rule_preregistered",
                "selection_and_certification_sets_nonempty_and_disjoint",
                "no_posthoc_provenance_flags",
            },
            set(config.unmet_confirmatory_static_requirements),
        )

    def test_evaluation_records_the_eligibility_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            manifest, _, output = fixture.certify(
                fixture.certificate_config()
            )
            evaluation = json.loads(
                (output / "certificate_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            ledger = evaluation["confirmatory_eligibility"]
            self.assertFalse(ledger["eligible_for_scientific_claims"])
            self.assertEqual(
                set(ledger["static_requirements"]),
                set(ledger["unmet_static_requirements"]),
            )
            self.assertIn(
                "Clearing posthoc alone never grants eligibility",
                ledger["note"],
            )
            self.assertEqual(
                "finite-supplied-workload-set",
                manifest["estimand_target"],
            )
            self.assertTrue(
                manifest["unmet_confirmatory_static_requirements"]
            )


class TargetWorkloadLeakageTest(unittest.TestCase):
    def test_selection_and_certificate_workloads_must_be_disjoint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config(
                selection_workload_ids=(fixture.workload_ids[0],),
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "target-workload leakage",
            ):
                load_workload_safety_certificate_config(path)

    def test_a_selection_workload_cannot_appear_in_a_stratum(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config()
            payload = json.loads(path.read_text(encoding="utf-8"))
            leaked = payload["workload_split"]["certificate_workload_ids"][0]
            payload["workload_split"]["certificate_workload_ids"].remove(
                leaked
            )
            payload["workload_split"]["selection_workload_ids"] = [leaked]
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "target-workload leakage",
            ):
                load_workload_safety_certificate_config(path)

    def test_strata_must_partition_the_certificate_workloads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            path = fixture.certificate_config()
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["workload_split"]["certificate_workload_ids"].pop()
            _write_json(path, payload)
            with self.assertRaisesRegex(
                AWMConfigError,
                "must partition certificate_workload_ids",
            ):
                load_workload_safety_certificate_config(path)

    def test_a_selection_workload_is_never_read_into_the_estimate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _Fixture(
                root,
                strata={
                    "causal": _workload_ids("causal", 16),
                    "selection": _workload_ids("selection", 4),
                },
                plan={
                    **_plan(
                        ("causal",),
                        origin=_Cell(successes=1, cost=1.0),
                        candidate=_Cell(successes=4, cost=0.2),
                    ),
                    **_plan(
                        ("selection",),
                        origin=_Cell(successes=4, cost=0.2),
                        candidate=_Cell(successes=0, cost=1.0),
                    ),
                },
            )
            config = fixture.certificate_config(
                strata=("causal",),
                selection_workload_ids=fixture.strata["selection"],
            )
            _, rows, _ = fixture.certify(config)
            self.assertEqual(1, len(rows))
            self.assertEqual(
                16,
                int(rows[0]["independent_workload_count"]),
            )
            self.assertEqual("SAFE_TO_COMMIT", rows[0]["certificate_state"])


class CertificateCalibrationTest(unittest.TestCase):
    def test_clearly_safe_candidate_commits(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary))
            _, rows, _ = fixture.certify(fixture.certificate_config())
            row = rows[0]
            self.assertEqual("PASS", row["success_non_inferiority_gate"])
            self.assertEqual("PASS", row["cost_improvement_gate"])
            self.assertEqual("SAFE_TO_COMMIT", row["certificate_state"])
            self.assertEqual(FRAMES, row["applied_design_id"])
            self.assertEqual(SAFE, row["fallback_design_id"])
            self.assertEqual("False", row["fallback_applied"])
            self.assertAlmostEqual(
                0.75,
                float(row["success_difference_point_estimate"]),
            )
            self.assertAlmostEqual(
                0.8,
                float(row["cost_saving_point_estimate"]),
            )
            self.assertGreaterEqual(
                float(row["success_difference_lower_bound"]),
                -0.05,
            )
            self.assertGreaterEqual(
                float(row["cost_saving_lower_bound"]),
                0.1,
            )

    def test_success_harming_but_cheap_candidate_is_unsafe(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _harmful_fixture(Path(temporary))
            _, rows, _ = fixture.certify(fixture.certificate_config())
            row = rows[0]
            self.assertEqual("VIOLATED", row["success_non_inferiority_gate"])
            self.assertEqual("PASS", row["cost_improvement_gate"])
            self.assertEqual("UNSAFE", row["certificate_state"])
            self.assertEqual(SAFE, row["applied_design_id"])
            self.assertEqual("True", row["fallback_applied"])
            self.assertIn(
                "success_non_inferiority",
                row["fallback_reason"],
            )
            self.assertLess(
                float(row["success_difference_upper_bound"]),
                -0.05,
            )

    def test_cost_neutral_candidate_never_commits(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _cost_neutral_fixture(Path(temporary))
            _, rows, _ = fixture.certify(fixture.certificate_config())
            row = rows[0]
            self.assertEqual("PASS", row["success_non_inferiority_gate"])
            self.assertNotEqual("PASS", row["cost_improvement_gate"])
            self.assertNotEqual("SAFE_TO_COMMIT", row["certificate_state"])
            self.assertEqual(SAFE, row["applied_design_id"])
            self.assertAlmostEqual(
                0.0,
                float(row["cost_saving_point_estimate"]),
            )

    def test_cost_neutral_candidate_is_unsafe_against_a_strict_saving(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _cost_neutral_fixture(Path(temporary))
            _, rows, _ = fixture.certify(fixture.certificate_config(
                minimum_cost_saving=0.9,
            ))
            row = rows[0]
            self.assertEqual("VIOLATED", row["cost_improvement_gate"])
            self.assertEqual("UNSAFE", row["certificate_state"])
            self.assertEqual(SAFE, row["applied_design_id"])

    def test_small_sample_size_yields_insufficient_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=3)
            _, rows, _ = fixture.certify(fixture.certificate_config())
            row = rows[0]
            self.assertEqual(3, int(row["independent_workload_count"]))
            self.assertEqual(
                "INDETERMINATE",
                row["success_non_inferiority_gate"],
            )
            self.assertEqual(
                "INSUFFICIENT_EVIDENCE",
                row["certificate_state"],
            )
            self.assertEqual(SAFE, row["applied_design_id"])
            self.assertIn("gate_not_established", row["fallback_reason"])

    def test_minimum_independent_workloads_blocks_a_thin_stratum(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary))
            _, rows, _ = fixture.certify(fixture.certificate_config(
                minimum_independent_workloads=17,
            ))
            row = rows[0]
            self.assertEqual("PASS", row["success_non_inferiority_gate"])
            self.assertEqual("PASS", row["cost_improvement_gate"])
            self.assertEqual(
                "INSUFFICIENT_EVIDENCE",
                row["certificate_state"],
            )
            self.assertIn(
                "insufficient_independent_workloads",
                row["fallback_reason"],
            )
            self.assertEqual(SAFE, row["applied_design_id"])

    def test_no_false_safe_commit_in_harmful_configurations(self) -> None:
        harms = (
            (4, 0.05),
            (0, 0.0),
            (1, 0.5),
            (2, 0.25),
        )
        for origin_successes, margin in harms:
            for workloads in (4, 16, 32):
                with self.subTest(
                    origin_successes=origin_successes,
                    margin=margin,
                    workloads=workloads,
                ):
                    with tempfile.TemporaryDirectory() as temporary:
                        root = Path(temporary)
                        fixture = _Fixture(
                            root,
                            strata={
                                "causal": _workload_ids(
                                    "causal",
                                    workloads,
                                ),
                            },
                            plan=_plan(
                                ("causal",),
                                origin=_Cell(
                                    successes=origin_successes,
                                    cost=1.0,
                                ),
                                candidate=_Cell(successes=0, cost=0.0),
                            ),
                        )
                        _, rows, _ = fixture.certify(
                            fixture.certificate_config(
                                delta_success_margin=margin,
                            )
                        )
                        row = rows[0]
                        truth = -origin_successes / 4.0
                        if truth < -margin:
                            self.assertNotEqual(
                                "SAFE_TO_COMMIT",
                                row["certificate_state"],
                            )
                            self.assertEqual(
                                SAFE,
                                row["applied_design_id"],
                            )

    def test_every_non_safe_state_falls_back_to_origin(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _Fixture(
                root,
                strata={
                    "safe": _workload_ids("safe", 24),
                    "harmful": _workload_ids("harmful", 24),
                    "thin": _workload_ids("thin", 3),
                },
                plan={
                    **_plan(
                        ("safe",),
                        origin=_Cell(successes=1, cost=1.0),
                        candidate=_Cell(successes=4, cost=0.2),
                    ),
                    **_plan(
                        ("harmful",),
                        origin=_Cell(successes=4, cost=1.0),
                        candidate=_Cell(successes=0, cost=0.2),
                    ),
                    **_plan(
                        ("thin",),
                        origin=_Cell(successes=1, cost=1.0),
                        candidate=_Cell(successes=4, cost=0.2),
                    ),
                },
            )
            _, rows, output = fixture.certify(fixture.certificate_config())
            states = {row["stratum_id"]: row for row in rows}
            self.assertEqual(
                "SAFE_TO_COMMIT",
                states["safe"]["certificate_state"],
            )
            self.assertEqual(
                "UNSAFE",
                states["harmful"]["certificate_state"],
            )
            self.assertEqual(
                "INSUFFICIENT_EVIDENCE",
                states["thin"]["certificate_state"],
            )
            for row in rows:
                self.assertEqual(SAFE, row["fallback_design_id"])
                if row["certificate_state"] == "SAFE_TO_COMMIT":
                    self.assertEqual("False", row["fallback_applied"])
                    self.assertNotEqual(SAFE, row["applied_design_id"])
                else:
                    self.assertEqual("True", row["fallback_applied"])
                    self.assertEqual(SAFE, row["applied_design_id"])
            evaluation = json.loads(
                (output / "certificate_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(
                {
                    "SAFE_TO_COMMIT": 1,
                    "UNSAFE": 1,
                    "INSUFFICIENT_EVIDENCE": 1,
                },
                evaluation["state_counts"],
            )
            self.assertEqual(["safe"], evaluation["safe_to_commit_strata"])


class FamilyAdjustmentTest(unittest.TestCase):
    def _two_stratum_fixture(self, root: Path) -> _Fixture:
        return _Fixture(
            root,
            strata={
                "causal": _workload_ids("causal", 16),
                "temporal": _workload_ids("temporal", 16),
            },
            plan=_plan(
                ("causal", "temporal"),
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            ),
        )

    def test_more_tested_strata_widen_every_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._two_stratum_fixture(root)
            single, narrow_rows, _ = fixture.certify(
                fixture.certificate_config(
                    name="single.json",
                    strata=("causal",),
                ),
                output_name="single-output",
            )
            both, wide_rows, _ = fixture.certify(
                fixture.certificate_config(name="both.json"),
                output_name="both-output",
            )
            self.assertEqual(4, single["family_size"])
            self.assertEqual(8, both["family_size"])
            self.assertAlmostEqual(0.05 / 4, single["adjusted_alpha"])
            self.assertAlmostEqual(0.05 / 8, both["adjusted_alpha"])
            self.assertAlmostEqual(
                0.95,
                single["unadjusted_confidence_level"],
            )
            self.assertAlmostEqual(
                1.0 - 0.05 / 8,
                both["adjusted_confidence_level"],
            )
            narrow = narrow_rows[0]
            wide = next(
                row for row in wide_rows if row["stratum_id"] == "causal"
            )
            self.assertEqual(
                narrow["independent_workload_count"],
                wide["independent_workload_count"],
            )
            self.assertAlmostEqual(
                float(narrow["success_difference_point_estimate"]),
                float(wide["success_difference_point_estimate"]),
            )
            for key in (
                "success_difference_lower_bound",
                "cost_saving_lower_bound",
            ):
                self.assertGreater(float(narrow[key]), float(wide[key]))
            for key in (
                "success_difference_upper_bound",
                "cost_saving_upper_bound",
            ):
                self.assertLess(float(narrow[key]), float(wide[key]))

    def test_family_components_enumerate_every_stratum_gate_and_side(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._two_stratum_fixture(root)
            _, _, output = fixture.certify(fixture.certificate_config())
            evaluation = json.loads(
                (output / "certificate_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            method = evaluation["statistical_method"]
            self.assertEqual("bonferroni", method["family_adjustment"])
            self.assertEqual(8, method["family_size"])
            self.assertEqual(8, len(method["family_components"]))
            self.assertEqual(
                {
                    ("causal", "success_non_inferiority", "lower"),
                    ("causal", "success_non_inferiority", "upper"),
                    ("causal", "cost_improvement", "lower"),
                    ("causal", "cost_improvement", "upper"),
                    ("temporal", "success_non_inferiority", "lower"),
                    ("temporal", "success_non_inferiority", "upper"),
                    ("temporal", "cost_improvement", "lower"),
                    ("temporal", "cost_improvement", "upper"),
                },
                {
                    (
                        component["stratum_id"],
                        component["gate_id"],
                        component["side"],
                    )
                    for component in method["family_components"]
                },
            )
            self.assertFalse(
                evaluation["strata"][0]["utility_gain"][
                    "in_confidence_family"
                ]
            )


class RepetitionBlockTest(unittest.TestCase):
    def test_duplicated_repetitions_do_not_add_independent_units(
        self,
    ) -> None:
        results: list[tuple[dict[str, Any], dict[str, str]]] = []
        for repetitions in (4, 12):
            with tempfile.TemporaryDirectory() as temporary:
                fixture = _safe_fixture(
                    Path(temporary),
                    base_repetitions=4,
                    repetitions=repetitions,
                )
                manifest, rows, _ = fixture.certify(
                    fixture.certificate_config()
                )
                results.append((manifest, rows[0]))
        base, duplicated = results
        self.assertEqual(4, int(base[1]["repetitions_per_workload"]))
        self.assertEqual(12, int(duplicated[1]["repetitions_per_workload"]))
        self.assertEqual(64, int(base[1]["repetition_pair_count"]))
        self.assertEqual(192, int(duplicated[1]["repetition_pair_count"]))
        self.assertEqual(
            base[1]["independent_workload_count"],
            duplicated[1]["independent_workload_count"],
        )
        for key in (
            "success_difference_point_estimate",
            "success_difference_lower_bound",
            "success_difference_upper_bound",
            "cost_saving_point_estimate",
            "cost_saving_lower_bound",
            "cost_saving_upper_bound",
            "utility_gain_point_estimate",
        ):
            self.assertAlmostEqual(
                float(base[1][key]),
                float(duplicated[1][key]),
                places=12,
                msg=key,
            )

    def test_incomplete_repetition_block_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, FRAMES)
            trimmed = [
                record
                for record in records
                if not (
                    record["workload_id"] == fixture.workload_ids[0]
                    and record["repetition"] == 2
                )
            ]
            fixture.write_runs(fixture.oracle_output, FRAMES, trimmed)
            with self.assertRaisesRegex(
                AWMConfigError,
                "incomplete repetition block",
            ):
                fixture.certify(fixture.certificate_config())

    def test_missing_response_cell_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, FRAMES)
            trimmed = [
                record
                for record in records
                if record["workload_id"] != fixture.workload_ids[0]
            ]
            fixture.write_runs(fixture.oracle_output, FRAMES, trimmed)
            with self.assertRaisesRegex(
                AWMConfigError,
                "missing certificate response cell",
            ):
                fixture.certify(fixture.certificate_config())

    def test_duplicate_response_cell_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, FRAMES)
            duplicate = dict(records[0])
            duplicate["trial_key"] = str(duplicate["trial_key"]) + "|retry"
            fixture.write_runs(
                fixture.oracle_output,
                FRAMES,
                records + [duplicate],
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "duplicate certificate response cell",
            ):
                fixture.certify(fixture.certificate_config())

    def test_a_duplicated_trial_key_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, FRAMES)
            fixture.write_runs(
                fixture.oracle_output,
                FRAMES,
                records + [records[0]],
            )
            with self.assertRaisesRegex(
                RuntimeError,
                "duplicate trial_key",
            ):
                fixture.certify(fixture.certificate_config())


class FailClosedTest(unittest.TestCase):
    def test_incomplete_telemetry_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, FRAMES)
            records[0]["telemetry_complete"] = False
            fixture.write_runs(fixture.oracle_output, FRAMES, records)
            with self.assertRaisesRegex(
                AWMConfigError,
                "incomplete telemetry",
            ):
                fixture.certify(fixture.certificate_config())

    def test_missing_task_success_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, SAFE)
            records[0]["task_success"] = None
            fixture.write_runs(fixture.oracle_output, SAFE, records)
            with self.assertRaisesRegex(
                AWMConfigError,
                "incomplete telemetry",
            ):
                fixture.certify(fixture.certificate_config())

    def test_artifact_delivery_failure_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, FRAMES)
            records[0]["access_events"][0][
                "artifact_full_download_count"
            ] = 0
            fixture.write_runs(fixture.oracle_output, FRAMES, records)
            with self.assertRaisesRegex(
                AWMConfigError,
                "artifact-delivery failure",
            ):
                fixture.certify(fixture.certificate_config())

    def test_uncompleted_outcome_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, FRAMES)
            records[0]["outcome_type"] = "timeout"
            records[0]["task_success"] = None
            fixture.write_runs(fixture.oracle_output, FRAMES, records)
            with self.assertRaisesRegex(
                AWMConfigError,
                "incomplete telemetry",
            ):
                fixture.certify(fixture.certificate_config())

    def test_an_excluded_design_is_never_read(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=16)
            records = fixture.read_runs(fixture.oracle_output, PAIR)
            records[0]["telemetry_complete"] = False
            fixture.write_runs(fixture.oracle_output, PAIR, records)
            manifest, rows, _ = fixture.certify(
                fixture.certificate_config()
            )
            self.assertEqual("COMPLETE", manifest["status"])
            self.assertEqual("SAFE_TO_COMMIT", rows[0]["certificate_state"])

    def test_a_value_outside_its_declared_support_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            records = fixture.read_runs(fixture.oracle_output, SAFE)
            for record in records:
                record["access_events"][0]["realized_cost"] = 9.0
            fixture.write_runs(fixture.oracle_output, SAFE, records)
            with self.assertRaisesRegex(
                AWMConfigError,
                "exceeds its declared support",
            ):
                fixture.certify(fixture.certificate_config())

    def test_a_nonempty_output_directory_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _safe_fixture(root, workloads=4)
            config = fixture.certificate_config()
            occupied = root / "certificate-output"
            occupied.mkdir(parents=True)
            (occupied / "existing.txt").write_text("x", encoding="utf-8")
            with self.assertRaisesRegex(AWMConfigError, "is not empty"):
                fixture.certify(config)


class CertificateOutputTest(unittest.TestCase):
    def test_outputs_are_reproducible_and_snapshot_hashed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary))
            config = fixture.certificate_config()
            payloads: list[tuple[str, ...]] = []
            hashes: list[str] = []
            for index in range(2):
                manifest, _, output = fixture.certify(
                    config,
                    output_name=f"certificate-output-{index}",
                )
                hashes.append(manifest["certificate_snapshot_sha256"])
                payloads.append(tuple(
                    (output / name).read_text(encoding="utf-8")
                    for name in (
                        "certificate_summary.csv",
                        "certificate_evaluation.json",
                    )
                ))
            self.assertEqual(payloads[0], payloads[1])
            self.assertEqual(hashes[0], hashes[1])
            self.assertEqual(64, len(hashes[0]))

    def test_snapshot_hash_changes_when_a_response_changes(self) -> None:
        hashes: list[str] = []
        for cost in (0.2, 0.3):
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixture = _Fixture(
                    root,
                    strata={"causal": _workload_ids("causal", 16)},
                    plan=_plan(
                        ("causal",),
                        origin=_Cell(successes=1, cost=1.0),
                        candidate=_Cell(successes=4, cost=cost),
                    ),
                )
                manifest, _, _ = fixture.certify(
                    fixture.certificate_config()
                )
                hashes.append(manifest["certificate_snapshot_sha256"])
        self.assertNotEqual(hashes[0], hashes[1])

    def test_evaluation_reports_the_required_decision_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary))
            manifest, rows, output = fixture.certify(
                fixture.certificate_config()
            )
            evaluation = json.loads(
                (output / "certificate_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(evaluation["posthoc"])
            self.assertFalse(evaluation["eligible_for_scientific_claims"])
            self.assertFalse(
                evaluation["thresholds"]["selected_by_this_tool"]
            )
            self.assertEqual(
                16,
                evaluation["workload_split"]["independent_workload_count"],
            )
            self.assertEqual(
                64,
                evaluation["workload_split"]["repetition_pair_count"],
            )
            stratum = evaluation["strata"][0]
            self.assertEqual(
                {"success_difference", "cost_saving", "utility_gain"},
                set(stratum["point_estimates"]),
            )
            self.assertEqual(
                ["success_non_inferiority", "cost_improvement"],
                [gate["gate_id"] for gate in stratum["gates"]],
            )
            for gate in stratum["gates"]:
                self.assertIn("lower_bound", gate)
                self.assertIn("upper_bound", gate)
                self.assertIn(
                    gate["result"],
                    {"PASS", "VIOLATED", "INDETERMINATE"},
                )
            self.assertEqual(
                16,
                len(stratum["workload_observations"]),
            )
            self.assertTrue(manifest["certificate_config_sha256"])
            self.assertTrue(manifest["oracle_config_sha256"])
            self.assertTrue(manifest["oracle_analysed_snapshot_sha256"])
            self.assertTrue(manifest["oracle_full_snapshot_sha256"])
            self.assertEqual(
                "analysed-design-subset",
                manifest["oracle_analysed_snapshot_scope"],
            )
            self.assertEqual(
                "full-declared-design-set",
                manifest["oracle_full_snapshot_scope"],
            )
            self.assertNotEqual(
                manifest["oracle_analysed_snapshot_sha256"],
                manifest["oracle_full_snapshot_sha256"],
            )
            self.assertFalse(manifest["deployment_mutations_performed"])
            self.assertFalse(manifest["secrets_recorded"])
            self.assertEqual("True", rows[0]["posthoc"])
            self.assertEqual(
                "False",
                rows[0]["eligible_for_scientific_claims"],
            )

    def test_evaluation_labels_the_posthoc_candidate_restriction(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _safe_fixture(Path(temporary), workloads=4)
            _, _, output = fixture.certify(fixture.certificate_config())
            evaluation = json.loads(
                (output / "certificate_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            restriction = evaluation["candidate_restriction"]
            self.assertEqual(
                "posthoc-selected-on-inspected-oracle",
                restriction["provenance"],
            )
            self.assertEqual([PAIR], restriction["excluded_design_ids"])
            self.assertNotIn(PAIR, restriction["analysed_design_ids"])
            self.assertTrue(any(
                "new independent workloads" in limitation
                for limitation in evaluation["limitations"]
            ))

    def test_cli_runs_the_read_only_certificate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _safe_fixture(root)
            config = fixture.certificate_config()
            output = root / "cli-output"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = cli_main([
                    "certify-awm-restricted-policy",
                    "--certificate-config",
                    str(config),
                    "--oracle-config",
                    str(fixture.oracle_path),
                    "--oracle-output-dir",
                    str(fixture.oracle_output),
                    "--output-dir",
                    str(output),
                    "--compact",
                ])
            self.assertEqual(0, exit_code)
            payload = json.loads(stdout.getvalue())
            self.assertEqual("COMPLETE", payload["status"])
            self.assertTrue(payload["posthoc"])
            self.assertFalse(payload["eligible_for_scientific_claims"])
            self.assertEqual(SAFE, payload["fallback_design_id"])
            self.assertTrue(
                (output / "certificate_summary.csv").is_file()
            )
            self.assertTrue(
                (output / "certificate_evaluation.json").is_file()
            )
            self.assertTrue(
                (output / "certificate_manifest.json").is_file()
            )


if __name__ == "__main__":
    unittest.main()
