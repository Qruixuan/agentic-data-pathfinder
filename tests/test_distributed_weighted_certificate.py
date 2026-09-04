"""Weighted stratified certificate for a distributed-policy confirmation."""

from __future__ import annotations

import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from pathfinder.awm.model import Interval
from pathfinder.distributed import (
    EXECUTION_EVIDENCE_SCHEMA_VERSION,
    ConfirmationPlanError,
    StratumEvidence,
    WeightedCertificateError,
    certify_distributed_policy_confirmation,
    distance_to_threshold,
    evaluate_weighted_policy_certificate,
    plan_distributed_policy_oed,
)

from tests.test_distributed_confirmation import (
    EXPECTED_MODEL,
    confirmation_config,
    POLICY,
    SAFE,
    TARGET_WEIGHTS,
    _Fixture,
    _csv_text,
    _write_json,
)


FRAMES = "D_local_frames"
SUCCESS_SUPPORT = Interval(-1.0, 1.0)
COST_SUPPORT = Interval(-2.0, 2.0)


def _evaluate(strata, **overrides: Any) -> dict[str, Any]:
    active = [item for item in strata if item.role == "active"]
    arguments: dict[str, Any] = {
        "minimum_independent_workloads_by_stratum": {
            item.stratum_id: 1 for item in active
        },
        "success_difference_support": SUCCESS_SUPPORT,
        "cost_saving_support": COST_SUPPORT,
        "alpha": 0.05,
        "delta_success_margin": 0.05,
        "minimum_cost_saving": 0.25,
        "safe_design_id": SAFE,
        "policy_id": POLICY,
    }
    arguments.update(overrides)
    return evaluate_weighted_policy_certificate(strata, **arguments)


def _strata(
    *,
    causal_success: float = 0.0,
    causal_cost: float = 0.5,
    causal_n: int = 14,
    descriptive_success: float = 0.0,
    descriptive_cost: float = 0.5,
    descriptive_n: int = 8,
) -> list[StratumEvidence]:
    return [
        StratumEvidence(
            "causal", "active", 14,
            (causal_success,) * causal_n,
            (causal_cost,) * causal_n,
        ),
        StratumEvidence(
            "descriptive", "active", 8,
            (descriptive_success,) * descriptive_n,
            (descriptive_cost,) * descriptive_n,
        ),
        StratumEvidence("temporal", "structural_safe", 14),
    ]


class WeightedAggregationTest(unittest.TestCase):
    def test_hand_calculated_weighted_point_estimates(self) -> None:
        result = _evaluate(_strata(
            causal_success=0.10,
            causal_cost=0.60,
            descriptive_success=-0.20,
            descriptive_cost=0.40,
        ))
        # (14*0.10 + 8*(-0.20) + 14*0) / 36 = (1.4 - 1.6) / 36
        self.assertAlmostEqual(
            (14 * 0.10 + 8 * -0.20 + 14 * 0.0) / 36,
            result["overall_success_difference"]["point_estimate"],
            places=12,
        )
        # (14*0.60 + 8*0.40 + 14*0) / 36 = (8.4 + 3.2) / 36
        self.assertAlmostEqual(
            (14 * 0.60 + 8 * 0.40 + 14 * 0.0) / 36,
            result["overall_cost_saving"]["point_estimate"],
            places=12,
        )

    def test_hand_calculated_weighted_bound_aggregation(self) -> None:
        result = _evaluate(_strata())
        by_id = {item["stratum_id"]: item for item in result["strata"]}
        for quantity, overall in (
            ("success_difference", "overall_success_difference"),
            ("cost_saving", "overall_cost_saving"),
        ):
            for tail in ("lower_bound", "upper_bound"):
                expected = sum(
                    by_id[stratum]["normalized_weight"]
                    * by_id[stratum][quantity][tail]
                    for stratum in ("causal", "descriptive", "temporal")
                )
                self.assertAlmostEqual(
                    expected,
                    result[overall][tail],
                    places=12,
                    msg=f"{overall}.{tail}",
                )

    def test_frozen_weights_are_used_not_sample_proportions(self) -> None:
        # Oversample causal 10x. Sample proportions would be 140:8:0, but
        # the target weights are 14:8:14 and must not move.
        result = _evaluate(_strata(causal_n=140))
        estimand = result["estimand"]
        self.assertEqual(
            {"causal": 14, "descriptive": 8, "temporal": 14},
            estimand["target_stratum_weights_integer"],
        )
        self.assertFalse(
            estimand["weights_are_empirical_sample_proportions"]
        )
        by_id = {item["stratum_id"]: item for item in result["strata"]}
        self.assertAlmostEqual(14 / 36, by_id["causal"]["normalized_weight"])
        self.assertAlmostEqual(
            14 / 36,
            by_id["temporal"]["normalized_weight"],
            msg="temporal keeps its full weight despite zero observations",
        )

    def test_oversampling_does_not_change_the_target(self) -> None:
        baseline = _evaluate(_strata())
        oversampled = _evaluate(_strata(causal_n=140))
        self.assertEqual(
            baseline["estimand"]["target_stratum_weights_normalized"],
            oversampled["estimand"][
                "target_stratum_weights_normalized"
            ],
        )
        # Point estimate unchanged; only the interval tightens.
        self.assertAlmostEqual(
            baseline["overall_cost_saving"]["point_estimate"],
            oversampled["overall_cost_saving"]["point_estimate"],
            places=12,
        )
        self.assertLess(
            oversampled["overall_cost_saving"]["upper_bound"]
            - oversampled["overall_cost_saving"]["lower_bound"],
            baseline["overall_cost_saving"]["upper_bound"]
            - baseline["overall_cost_saving"]["lower_bound"],
        )


class StructuralZeroTest(unittest.TestCase):
    def test_the_structural_effect_is_exactly_zero(self) -> None:
        result = _evaluate(_strata())
        temporal = next(
            item for item in result["strata"]
            if item["stratum_id"] == "temporal"
        )
        for quantity in ("success_difference", "cost_saving"):
            self.assertEqual(0.0, temporal[quantity]["point_estimate"])
            self.assertEqual(0.0, temporal[quantity]["lower_bound"])
            self.assertEqual(0.0, temporal[quantity]["upper_bound"])
        self.assertTrue(temporal["structural_zero_effect"])

    def test_structural_strata_consume_no_alpha(self) -> None:
        result = _evaluate(_strata())
        by_id = {item["stratum_id"]: item for item in result["strata"]}
        self.assertEqual(0.0, by_id["temporal"]["alpha_consumed"])
        self.assertGreater(by_id["causal"]["alpha_consumed"], 0.0)
        confidence = result["confidence"]
        # 2 active strata x 2 gates x 2 tails; temporal contributes none.
        self.assertEqual(8, confidence["family_size"])
        self.assertEqual(0.05 / 8, confidence["adjusted_alpha"])
        self.assertTrue(
            confidence["structural_zero_strata_consume_no_alpha"]
        )
        self.assertEqual(
            ["temporal"],
            confidence["structural_zero_stratum_ids"],
        )
        self.assertNotIn(
            "temporal",
            {item["stratum_id"] for item in confidence["family_components"]},
        )

    def test_structural_strata_need_no_candidate_record(self) -> None:
        result = _evaluate(_strata())
        temporal = next(
            item for item in result["strata"]
            if item["stratum_id"] == "temporal"
        )
        self.assertEqual(0, temporal["independent_workloads"])
        self.assertFalse(temporal["candidate_observations_required"])

    def test_a_candidate_record_for_a_structural_stratum_is_rejected(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            WeightedCertificateError,
            "measured safe-versus-safe pair",
        ):
            StratumEvidence("temporal", "structural_safe", 14, (0.0,), (0.0,))

    def test_structural_strata_do_not_count_as_active_evidence(
        self,
    ) -> None:
        result = _evaluate(_strata(causal_n=14, descriptive_n=8))
        self.assertEqual(22, result["active_independent_workloads"])
        self.assertEqual(
            ["causal", "descriptive"],
            result["active_stratum_ids"],
        )

    def test_an_all_structural_policy_cannot_be_certified(self) -> None:
        with self.assertRaisesRegex(
            WeightedCertificateError,
            "no candidate effect to certify",
        ):
            _evaluate([
                StratumEvidence("causal", "structural_safe", 14),
                StratumEvidence("temporal", "structural_safe", 14),
            ])


class RepetitionSemanticsTest(unittest.TestCase):
    def test_repetitions_do_not_increase_independent_n(self) -> None:
        """Averaged repetitions must land as one observation per workload."""
        # Two workloads, each averaged from repetitions, is n=2 -- not 4.
        two = _evaluate([
            StratumEvidence("causal", "active", 14, (0.1, 0.3), (0.5, 0.7)),
            StratumEvidence("temporal", "structural_safe", 14),
        ])
        four = _evaluate([
            StratumEvidence(
                "causal", "active", 14,
                (0.1, 0.1, 0.3, 0.3), (0.5, 0.5, 0.7, 0.7),
            ),
            StratumEvidence("temporal", "structural_safe", 14),
        ])
        by_two = next(item for item in two["strata"] if item["stratum_id"] == "causal")
        by_four = next(item for item in four["strata"] if item["stratum_id"] == "causal")
        self.assertEqual(2, by_two["independent_workloads"])
        self.assertEqual(4, by_four["independent_workloads"])
        self.assertAlmostEqual(
            by_two["success_difference"]["point_estimate"],
            by_four["success_difference"]["point_estimate"],
        )
        # Duplicating rows would narrow the interval; that is the error the
        # per-workload averaging contract prevents upstream.
        self.assertLess(
            by_four["success_difference"]["upper_bound"]
            - by_four["success_difference"]["lower_bound"],
            by_two["success_difference"]["upper_bound"]
            - by_two["success_difference"]["lower_bound"],
        )

    def test_mismatched_observation_counts_are_refused(self) -> None:
        with self.assertRaisesRegex(
            WeightedCertificateError,
            "one-to-one per independent workload",
        ):
            StratumEvidence("causal", "active", 14, (0.1, 0.2), (0.5,))


class DecisionStateTest(unittest.TestCase):
    def test_both_gates_passing_gives_safe_to_commit(self) -> None:
        result = _evaluate(
            _strata(
                causal_success=0.9, causal_cost=1.9,
                descriptive_success=0.9, descriptive_cost=1.9,
                causal_n=400, descriptive_n=400,
            ),
        )
        self.assertEqual("SAFE_TO_COMMIT", result["certificate_state"])
        self.assertEqual(
            POLICY,
            result["decision"]["applied_design_id"],
        )
        self.assertFalse(result["decision"]["fallback_applied"])

    def test_established_harm_gives_unsafe(self) -> None:
        result = _evaluate(
            _strata(
                causal_success=-0.95, causal_cost=-1.9,
                descriptive_success=-0.95, descriptive_cost=-1.9,
                causal_n=400, descriptive_n=400,
            ),
        )
        self.assertEqual("UNSAFE", result["certificate_state"])
        self.assertEqual(SAFE, result["decision"]["applied_design_id"])

    def test_overlapping_bounds_give_insufficient_evidence(self) -> None:
        result = _evaluate(_strata())
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            result["certificate_state"],
        )
        self.assertEqual(SAFE, result["decision"]["applied_design_id"])

    def test_every_non_safe_state_applies_the_origin(self) -> None:
        cases = {
            "insufficient": _strata(),
            "unsafe": _strata(
                causal_success=-0.95, causal_cost=-1.9,
                descriptive_success=-0.95, descriptive_cost=-1.9,
                causal_n=400, descriptive_n=400,
            ),
        }
        for label, strata in cases.items():
            with self.subTest(case=label):
                result = _evaluate(strata)
                self.assertNotEqual(
                    "SAFE_TO_COMMIT",
                    result["certificate_state"],
                )
                self.assertEqual(
                    SAFE,
                    result["decision"]["applied_design_id"],
                )
                self.assertTrue(result["decision"]["fallback_applied"])

    def test_point_thresholds_are_reported_separately_from_the_state(
        self,
    ) -> None:
        result = _evaluate(_strata(causal_cost=1.0, descriptive_cost=1.0))
        points = result["point_thresholds"]
        self.assertTrue(points["cost_improvement_point_passes"])
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            result["certificate_state"],
            "passing a point threshold is not a certificate",
        )

    def test_distance_to_threshold_is_reported(self) -> None:
        result = _evaluate(_strata())
        distance = distance_to_threshold(result)
        self.assertAlmostEqual(
            result["overall_success_difference"]["lower_bound"] + 0.05,
            distance["success_non_inferiority"],
        )
        self.assertAlmostEqual(
            result["overall_cost_saving"]["lower_bound"] - 0.25,
            distance["cost_improvement"],
        )


# ---------------------------------------------------------------------------
# End-to-end confirmation certificate
# ---------------------------------------------------------------------------
def _evidence_dir(
    root: Path,
    plan_dir: Path,
    *,
    pilot_id: str = "fresh-confirmation-pilot",
    success: float = 0.0,
    cost_delta: float = -0.6,
    telemetry_complete: bool = True,
    artifact_complete: bool = True,
    repetitions: int = 2,
    drop_stratum: str | None = None,
    extra_rows: list[dict[str, Any]] | None = None,
    candidate_override: Mapping[str, str] | None = None,
) -> Path:
    plan = json.loads(
        (plan_dir / "confirmation_plan.json").read_text(encoding="utf-8")
    )
    strata = {item["stratum_id"]: item for item in plan["strata"]}
    rows: list[dict[str, Any]] = []
    for workload_id, entry in sorted(plan["fresh_cohort"].items()):
        stratum_id = entry["stratum_id"]
        item = strata[stratum_id]
        if item["role"] == "structural_safe":
            continue
        if drop_stratum is not None and stratum_id == drop_stratum:
            continue
        design = (candidate_override or {}).get(
            stratum_id,
            item["policy_design_id"],
        )
        rows.append({
            "workload_id": workload_id,
            "object_id": entry["object_id"],
            "stratum_id": stratum_id,
            "safe_design_id": SAFE,
            "candidate_design_id": design,
            "repetitions": repetitions,
            "task_success_delta": success,
            "total_cost_delta": cost_delta,
        })
    rows.extend(extra_rows or [])
    evaluation = {
        "schema_version": "pathfinder.workload-evaluation/v1alpha1",
        "pilot_id": pilot_id,
        "evaluation_id": "fresh-eval-1",
        "telemetry_complete": telemetry_complete,
        "artifact_delivery_complete": artifact_complete,
        "independent_workloads": len(rows),
    }
    documents = {
        "evaluation.json": json.dumps(
            evaluation, indent=2, sort_keys=True
        ) + "\n",
        "evaluation_manifest.json": json.dumps(
            {"pilot_id": pilot_id}, indent=2
        ) + "\n",
        "paired_effects.csv": _csv_text(rows) if rows else "workload_id\n",
        "report.md": "# fresh confirmation evaluation\n",
        "summary_by_design.csv": _csv_text([
            {"design_id": SAFE, "workloads": len(rows)}
        ]),
    }
    documents["SHA256SUMS"] = "".join(
        f"{sha256(content.encode('utf-8')).hexdigest()}  {name}\n"
        for name, content in sorted(documents.items())
    )
    target = root / "evidence"
    target.mkdir(parents=True, exist_ok=True)
    for name, content in documents.items():
        (target / name).write_text(content, encoding="utf-8", newline="\n")
    return target


def _execution_evidence(
    root: Path,
    plan_dir: Path,
    evidence_dir: Path,
    *,
    model: str = EXPECTED_MODEL,
    plan_sha256: str | None = None,
    evaluation_sha256: str | None = None,
    pilot_id: str = "fresh-confirmation-pilot",
    name: str = "execution-evidence.json",
) -> Path:
    plan_bytes = (plan_dir / "confirmation_plan.json").read_bytes()
    evaluation_bytes = (evidence_dir / "evaluation.json").read_bytes()
    path = root / name
    _write_json(path, {
        "schema_version": EXECUTION_EVIDENCE_SCHEMA_VERSION,
        "execution_model_id": model,
        "confirmation_plan_sha256": (
            plan_sha256 or sha256(plan_bytes).hexdigest()
        ),
        "evaluation_sha256": (
            evaluation_sha256 or sha256(evaluation_bytes).hexdigest()
        ),
        "pilot_id": pilot_id,
        "run_id": "fresh-run-1",
        "evaluation_id": "fresh-eval-1",
    })
    return path


class _ConfirmationFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.planner = _Fixture(root)
        self.planner.freeze(output="plan")
        self.plan_dir = root / "plan"
        self.evidence = _evidence_dir(root, self.plan_dir)
        self.execution = _execution_evidence(
            root, self.plan_dir, self.evidence
        )

    def certify(self, *, output: str = "certificate", **overrides: Any):
        arguments: dict[str, Any] = {
            "execution_evidence": self.execution,
            "output_dir": self.root / output,
        }
        arguments.update(overrides)
        evidence = arguments.pop("evidence_dir", self.evidence)
        plan = arguments.pop("plan_dir", self.plan_dir)
        return certify_distributed_policy_confirmation(
            plan, evidence, **arguments
        )


class ConfirmationCertificateTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _ConfirmationFixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_a_valid_fresh_holdout_is_certified(self) -> None:
        result = self.f.certify()
        self.assertEqual("EVALUATED", result["status"])
        self.assertIn(
            result["certificate_state"],
            ("SAFE_TO_COMMIT", "UNSAFE", "INSUFFICIENT_EVIDENCE"),
        )
        self.assertEqual(
            "posthoc_previous_pilot",
            result["policy_selection_origin"],
        )
        self.assertEqual(
            "fresh_preregistered_holdout",
            result["confirmation_evidence_origin"],
        )

    def test_statistical_state_and_eligibility_stay_separate(self) -> None:
        result = self.f.certify()
        self.assertFalse(result["eligible_for_scientific_claims"])
        document = json.loads(
            (
                self.f.root / "certificate"
                / "confirmation_certificate.json"
            ).read_text(encoding="utf-8")
        )
        self.assertIn(
            "separate",
            document["claim_eligibility_note"],
        )
        self.assertIn("not authenticity", document["hash_semantics"])

    def test_a_wrong_model_is_rejected(self) -> None:
        for model in ("qwen/qwen3.6-27b", "qwen3.6-27b", "gpt-4o"):
            with self.subTest(model=model):
                execution = _execution_evidence(
                    self.f.root,
                    self.f.plan_dir,
                    self.f.evidence,
                    model=model,
                    name=f"exec-{abs(hash(model))}.json",
                )
                with self.assertRaisesRegex(
                    ConfirmationPlanError,
                    "frozen plan binds",
                ):
                    self.f.certify(
                        execution_evidence=execution,
                        output=f"m-{abs(hash(model))}",
                    )

    def test_a_wrong_plan_hash_is_rejected(self) -> None:
        execution = _execution_evidence(
            self.f.root,
            self.f.plan_dir,
            self.f.evidence,
            plan_sha256="f" * 64,
            name="exec-plan.json",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "names confirmation plan",
        ):
            self.f.certify(
                execution_evidence=execution,
                output="plan-hash",
            )

    def test_a_wrong_evaluation_hash_is_rejected(self) -> None:
        execution = _execution_evidence(
            self.f.root,
            self.f.plan_dir,
            self.f.evidence,
            evaluation_sha256="e" * 64,
            name="exec-eval.json",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "different evaluation document",
        ):
            self.f.certify(
                execution_evidence=execution,
                output="eval-hash",
            )

    def test_the_selecting_pilot_is_rejected_as_confirmation(self) -> None:
        plan = json.loads(
            (self.f.plan_dir / "confirmation_plan.json").read_text(
                encoding="utf-8"
            )
        )
        selecting = plan["policy_selection"]["selection_evidence_pilot_id"]
        evidence = _evidence_dir(
            self.f.root / "reused",
            self.f.plan_dir,
            pilot_id=selecting,
        )
        execution = _execution_evidence(
            self.f.root,
            self.f.plan_dir,
            evidence,
            pilot_id=selecting,
            name="exec-reused.json",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "can never be its own confirmation",
        ):
            self.f.certify(
                evidence_dir=evidence,
                execution_evidence=execution,
                output="reused",
            )

    def test_incomplete_telemetry_fails_closed(self) -> None:
        for field in ("telemetry_complete", "artifact_complete"):
            with self.subTest(field=field):
                evidence = _evidence_dir(
                    self.f.root / f"bad-{field}",
                    self.f.plan_dir,
                    **{field: False},
                )
                execution = _execution_evidence(
                    self.f.root,
                    self.f.plan_dir,
                    evidence,
                    name=f"exec-{field}.json",
                )
                with self.assertRaisesRegex(
                    ConfirmationPlanError,
                    "literal telemetry and artifact-delivery completeness",
                ):
                    self.f.certify(
                        evidence_dir=evidence,
                        execution_evidence=execution,
                        output=f"tel-{field}",
                    )

    def test_a_missing_active_stratum_fails_closed(self) -> None:
        evidence = _evidence_dir(
            self.f.root / "missing",
            self.f.plan_dir,
            drop_stratum="descriptive",
        )
        execution = _execution_evidence(
            self.f.root, self.f.plan_dir, evidence, name="exec-missing.json"
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "do not match the frozen fresh cohort",
        ):
            self.f.certify(
                evidence_dir=evidence,
                execution_evidence=execution,
                output="missing",
            )

    def test_a_duplicate_workload_fails_closed(self) -> None:
        plan = json.loads(
            (self.f.plan_dir / "confirmation_plan.json").read_text(
                encoding="utf-8"
            )
        )
        first = next(
            (workload_id, entry)
            for workload_id, entry in sorted(plan["fresh_cohort"].items())
            if plan["strata"][0]["stratum_id"] or True
        )
        evidence = _evidence_dir(
            self.f.root / "dupe",
            self.f.plan_dir,
            extra_rows=[{
                "workload_id": first[0],
                "object_id": first[1]["object_id"],
                "stratum_id": first[1]["stratum_id"],
                "safe_design_id": SAFE,
                "candidate_design_id": FRAMES,
                "repetitions": 2,
                "task_success_delta": 0.0,
                "total_cost_delta": -0.6,
            }],
        )
        execution = _execution_evidence(
            self.f.root, self.f.plan_dir, evidence, name="exec-dupe.json"
        )
        with self.assertRaises(ConfirmationPlanError) as caught:
            self.f.certify(
                evidence_dir=evidence,
                execution_evidence=execution,
                output="dupe",
            )
        self.assertIn("one independent cluster", str(caught.exception))

    def test_a_candidate_record_for_a_structural_stratum_is_rejected(
        self,
    ) -> None:
        plan = json.loads(
            (self.f.plan_dir / "confirmation_plan.json").read_text(
                encoding="utf-8"
            )
        )
        temporal = next(
            (workload_id, entry)
            for workload_id, entry in sorted(plan["fresh_cohort"].items())
            if entry["stratum_id"] == "temporal"
        )
        evidence = _evidence_dir(
            self.f.root / "structural",
            self.f.plan_dir,
            extra_rows=[{
                "workload_id": temporal[0],
                "object_id": temporal[1]["object_id"],
                "stratum_id": "temporal",
                "safe_design_id": SAFE,
                "candidate_design_id": FRAMES,
                "repetitions": 2,
                "task_success_delta": 0.3,
                "total_cost_delta": -0.6,
            }],
        )
        execution = _execution_evidence(
            self.f.root,
            self.f.plan_dir,
            evidence,
            name="exec-structural.json",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "comparing the safe design against itself is not an observation",
        ):
            self.f.certify(
                evidence_dir=evidence,
                execution_evidence=execution,
                output="structural",
            )

    def test_an_unplanned_design_is_rejected(self) -> None:
        evidence = _evidence_dir(
            self.f.root / "design",
            self.f.plan_dir,
            candidate_override={"causal": "D_local_pair"},
        )
        execution = _execution_evidence(
            self.f.root, self.f.plan_dir, evidence, name="exec-design.json"
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "candidate design must be",
        ):
            self.f.certify(
                evidence_dir=evidence,
                execution_evidence=execution,
                output="design",
            )

    def test_wrong_repetitions_are_rejected(self) -> None:
        evidence = _evidence_dir(
            self.f.root / "reps",
            self.f.plan_dir,
            repetitions=5,
        )
        execution = _execution_evidence(
            self.f.root, self.f.plan_dir, evidence, name="exec-reps.json"
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "expected 2 repetitions",
        ):
            self.f.certify(
                evidence_dir=evidence,
                execution_evidence=execution,
                output="reps",
            )

    def test_a_tampered_evidence_checksum_fails_closed(self) -> None:
        (self.f.evidence / "report.md").write_text(
            "# tampered\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "checksum mismatch",
        ):
            self.f.certify(output="tampered")

    def test_a_tampered_plan_checksum_fails_closed(self) -> None:
        (self.f.plan_dir / "confirmation_strata.csv").write_text(
            "broken\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "checksum mismatch",
        ):
            self.f.certify(output="tampered-plan")


class CertificateOutputTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _ConfirmationFixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_output_tree_is_complete_and_checksummed(self) -> None:
        self.f.certify()
        target = self.f.root / "certificate"
        names = {path.name for path in target.iterdir()}
        self.assertEqual(
            {
                "confirmation_certificate.json",
                "stratum_certificate.csv",
                "workload_effects.csv",
                "certificate_manifest.json",
                "report.md",
                "SHA256SUMS",
            },
            names,
        )
        for line in (target / "SHA256SUMS").read_text(
            encoding="utf-8"
        ).splitlines():
            digest, name = line.split("  ", 1)
            self.assertEqual(
                digest,
                sha256((target / name).read_bytes()).hexdigest(),
                name,
            )

    def test_no_output_file_contains_an_absolute_path(self) -> None:
        self.f.certify()
        for path in (self.f.root / "certificate").iterdir():
            content = path.read_text(encoding="utf-8")
            self.assertNotIn(str(self.f.root), content, path.name)
            self.assertNotIn("/tmp/", content, path.name)

    def test_the_report_separates_every_required_quantity(self) -> None:
        self.f.certify()
        report = (
            self.f.root / "certificate" / "report.md"
        ).read_text(encoding="utf-8")
        for fragment in (
            "Target stratum weights (frozen, not empirical)",
            "Collected evidence allocation",
            "Active independent workloads",
            "Repetitions per workload",
            "Raw repetition sessions",
            "Structural-zero strata",
            "Point thresholds versus the certificate",
            "Claim eligibility",
            "Point-threshold passage is not a certificate",
        ):
            self.assertIn(fragment, report)

    def test_an_existing_output_directory_is_never_overwritten(self) -> None:
        self.f.certify(output="once")
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "output directory already exists",
        ):
            self.f.certify(output="once")

    def test_a_failure_leaves_no_partial_output(self) -> None:
        target = self.f.root / "partial"
        (self.f.evidence / "report.md").write_text("# broken\n", encoding="utf-8")
        with self.assertRaises(ConfirmationPlanError):
            self.f.certify(output="partial")
        self.assertFalse(target.exists())

    def test_input_files_are_unchanged(self) -> None:
        before = {
            path.name: path.read_bytes()
            for path in sorted(self.f.evidence.iterdir())
        }
        before.update({
            f"plan/{path.name}": path.read_bytes()
            for path in sorted(self.f.plan_dir.iterdir())
        })
        before["execution"] = self.f.execution.read_bytes()
        self.f.certify()
        after = {
            path.name: path.read_bytes()
            for path in sorted(self.f.evidence.iterdir())
        }
        after.update({
            f"plan/{path.name}": path.read_bytes()
            for path in sorted(self.f.plan_dir.iterdir())
        })
        after["execution"] = self.f.execution.read_bytes()
        self.assertEqual(before, after)

    def test_the_output_tree_is_path_independent(self) -> None:
        trees = []
        for name in ("alpha", "beta"):
            with tempfile.TemporaryDirectory(suffix=name) as temporary:
                root = Path(temporary) / name / "nested"
                root.mkdir(parents=True)
                fixture = _ConfirmationFixture(root)
                fixture.certify()
                trees.append({
                    path.name: path.read_bytes()
                    for path in sorted((root / "certificate").iterdir())
                })
        self.assertEqual(sorted(trees[0]), sorted(trees[1]))
        for name in trees[0]:
            self.assertEqual(trees[0][name], trees[1][name], name)

    def test_no_socket_is_opened(self) -> None:
        import socket
        from unittest import mock

        def _refuse(*args: Any, **kwargs: Any):
            raise AssertionError("certification must not open a socket")

        with mock.patch.object(socket, "socket", _refuse), \
                mock.patch.object(socket, "create_connection", _refuse):
            self.f.certify(output="offline")
        document = json.loads(
            (
                self.f.root / "offline" / "confirmation_certificate.json"
            ).read_text(encoding="utf-8")
        )
        self.assertTrue(document["offline"])
        self.assertFalse(document["credentials_recorded"])


class MinimumIndependentWorkloadTest(unittest.TestCase):
    """Frozen per-stratum floors override otherwise-passing bounds."""

    def _passing(self, n: int, minima: Mapping[str, int]):
        return _evaluate(
            [
                StratumEvidence(
                    "causal", "active", 14, (0.9,) * n, (1.9,) * n
                ),
                StratumEvidence(
                    "descriptive", "active", 8, (0.9,) * 400, (1.9,) * 400
                ),
                StratumEvidence("temporal", "structural_safe", 14),
            ],
            minimum_independent_workloads_by_stratum=minima,
        )

    def test_a_deficient_stratum_overrides_passing_gates(self) -> None:
        ample = self._passing(400, {"causal": 3, "descriptive": 3})
        self.assertEqual("SAFE_TO_COMMIT", ample["certificate_state"])
        # Same 400 clusters, but the frozen floor demands 500. The bounds
        # are identical and still pass; only the floor refuses.
        thin = self._passing(400, {"causal": 500, "descriptive": 3})
        for gate in thin["gates"]:
            self.assertEqual("PASS", gate["result"], gate["gate_id"])
        self.assertEqual("INSUFFICIENT_EVIDENCE", thin["certificate_state"])
        self.assertIn(
            "insufficient_independent_workloads:causal=400<500",
            thin["decision"]["decision_reason"],
        )
        self.assertEqual(SAFE, thin["decision"]["applied_design_id"])
        self.assertEqual(
            ample["overall_cost_saving"]["lower_bound"],
            thin["overall_cost_saving"]["lower_bound"],
            "the floor must not alter the computed bounds",
        )

    def test_observed_and_required_counts_are_recorded(self) -> None:
        result = self._passing(5, {"causal": 3, "descriptive": 3})
        minimum = result["minimum_independent_workloads"]
        self.assertEqual(
            {"causal": 3, "descriptive": 3},
            minimum["required_by_active_stratum"],
        )
        self.assertEqual(
            {"causal": 5, "descriptive": 400},
            minimum["observed_by_active_stratum"],
        )
        self.assertTrue(minimum["every_active_stratum_meets_its_minimum"])
        self.assertFalse(minimum["repetitions_count_toward_minimum"])
        self.assertFalse(
            minimum["structural_zero_strata_count_toward_minimum"]
        )
        by_id = {item["stratum_id"]: item for item in result["strata"]}
        self.assertEqual(3, by_id["causal"]["required_independent_workloads"])
        self.assertTrue(
            by_id["causal"]["meets_minimum_independent_workloads"]
        )
        self.assertIsNone(
            by_id["temporal"]["required_independent_workloads"]
        )

    def test_repetitions_do_not_satisfy_the_minimum(self) -> None:
        """Clusters count, not the repetitions averaged inside them."""
        result = self._passing(400, {"causal": 500, "descriptive": 3})
        by_id = {item["stratum_id"]: item for item in result["strata"]}
        self.assertEqual(400, by_id["causal"]["independent_workloads"])
        self.assertEqual(500, by_id["causal"][
            "required_independent_workloads"
        ])
        self.assertFalse(
            by_id["causal"]["meets_minimum_independent_workloads"]
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", result["certificate_state"])
        self.assertFalse(
            result["minimum_independent_workloads"][
                "repetitions_count_toward_minimum"
            ]
        )

    def test_structural_strata_are_excluded_from_the_requirement(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            WeightedCertificateError,
            "name exactly the active strata",
        ):
            self._passing(
                400,
                {"causal": 3, "descriptive": 3, "temporal": 3},
            )

    def test_a_missing_active_stratum_minimum_is_refused(self) -> None:
        with self.assertRaisesRegex(
            WeightedCertificateError,
            "name exactly the active strata",
        ):
            self._passing(400, {"causal": 3})

    def test_invalid_minimum_values_are_refused(self) -> None:
        for value in (0, -1, True, 2.5, "3"):
            with self.subTest(value=value):
                with self.assertRaises(WeightedCertificateError):
                    self._passing(
                        400,
                        {"causal": value, "descriptive": 3},
                    )

    def test_established_harm_is_retained_over_a_deficiency(self) -> None:
        result = _evaluate(
            [
                StratumEvidence(
                    "causal", "active", 14, (-0.95,) * 400, (-1.9,) * 400
                ),
                StratumEvidence(
                    "descriptive", "active", 8,
                    (-0.95,) * 400, (-1.9,) * 400,
                ),
                StratumEvidence("temporal", "structural_safe", 14),
            ],
            minimum_independent_workloads_by_stratum={
                "causal": 500, "descriptive": 3,
            },
        )
        self.assertEqual("UNSAFE", result["certificate_state"])
        self.assertEqual(SAFE, result["decision"]["applied_design_id"])


class PlanMinimumFieldTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_plan_freezes_the_minimum_and_its_provenance(self) -> None:
        self.f.freeze()
        plan = self.f.plan_json()
        self.assertEqual(
            {"causal": 3, "descriptive": 3},
            plan["minimum_independent_workloads_by_active_stratum"],
        )
        self.assertEqual(
            "engineering-placeholder-not-scientifically-justified",
            plan["minimum_independent_workloads_provenance"],
        )
        self.assertNotIn(
            "temporal",
            plan["minimum_independent_workloads_by_active_stratum"],
        )

    def test_changing_the_minimum_changes_the_plan_hash(self) -> None:
        baseline = self.f.freeze(output="base")
        changed = confirmation_config(
            self.f.root / "config-min.json",
            planned=self.f.planned,
            minima={"causal": 5, "descriptive": 3},
        )
        other = self.f.freeze(config_path=changed, output="changed")
        self.assertNotEqual(baseline["plan_sha256"], other["plan_sha256"])

    def test_invalid_plan_minima_are_refused(self) -> None:
        cases = {
            "structural present": {
                "causal": 3, "descriptive": 3, "temporal": 3,
            },
            "missing active": {"causal": 3},
            "zero": {"causal": 0, "descriptive": 3},
            "negative": {"causal": -1, "descriptive": 3},
            "boolean": {"causal": True, "descriptive": 3},
            "fractional": {"causal": 2.5, "descriptive": 3},
        }
        for label, minima in cases.items():
            with self.subTest(case=label):
                path = confirmation_config(
                    self.f.root / f"cfg-{abs(hash(label))}.json",
                    planned=self.f.planned,
                    minima=minima,
                )
                with self.assertRaises(ConfirmationPlanError):
                    self.f.freeze(
                        config_path=path,
                        output=f"min-{abs(hash(label))}",
                    )

    def test_a_missing_minimum_field_is_refused(self) -> None:
        path = confirmation_config(
            self.f.root / "cfg-absent.json",
            planned=self.f.planned,
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.pop("minimum_independent_workloads_by_active_stratum")
        _write_json(path, payload)
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "required field",
        ):
            self.f.freeze(config_path=path, output="absent")


class FeasibilityClassificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plan(
        self,
        *,
        success: float,
        cost: float,
        output: str,
        budget: int = 8,
    ) -> dict[str, Any]:
        from tests.test_distributed_confirmation import build_policy_audit

        audit = build_policy_audit(
            self.root / output / "audit",
            success_delta=success,
            cost_saving=cost,
        )
        plan_distributed_policy_oed(
            audit,
            policy_id=POLICY,
            stratum_weights=dict(TARGET_WEIGHTS),
            output_dir=self.root / output / "plan",
            active_evidence_block_budget=budget,
            minimum_independent_workloads_by_active_stratum={
                "causal": 3, "descriptive": 3,
            },
        )
        return json.loads(
            (self.root / output / "plan" / "oed_plan.json").read_text(
                encoding="utf-8"
            )
        )

    def test_a_point_estimate_below_threshold_is_impossible(self) -> None:
        # Cost point estimate far under the 0.25 minimum saving.
        feasibility = self._plan(
            success=0.0, cost=0.05, output="below",
        )["feasibility"]
        self.assertEqual(
            "POINT_ESTIMATE_BELOW_THRESHOLD",
            feasibility["classification"],
        )
        self.assertIn(
            "cost_improvement",
            feasibility["gates_below_threshold"],
        )
        self.assertIsNone(
            feasibility["first_passing_active_block_budget"]
        )
        self.assertLess(feasibility["cost_point_margin"], 0.0)
        self.assertIn("infinitely narrow", feasibility["detail"])

    def test_a_tiny_positive_margin_is_not_called_impossible(self) -> None:
        # 22/36 * 0.41 = 0.2506; margin above 0.25 but minute.
        feasibility = self._plan(
            success=0.0, cost=0.41, output="tiny",
        )["feasibility"]
        self.assertEqual(
            "PROJECTED_NOT_WITHIN_SEARCH_BUDGET",
            feasibility["classification"],
        )
        self.assertGreater(feasibility["cost_point_margin"], 0.0)
        self.assertIsNone(
            feasibility["first_passing_active_block_budget"]
        )
        self.assertIn("not about mathematical impossibility", feasibility["detail"])

    def test_a_clearly_positive_case_finds_a_passing_budget(self) -> None:
        feasibility = self._plan(
            success=0.5, cost=1.8, output="passes",
        )["feasibility"]
        self.assertEqual(
            "PROJECTED_PASS_WITHIN_SEARCH_BUDGET",
            feasibility["classification"],
        )
        self.assertIsNotNone(
            feasibility["first_passing_active_block_budget"]
        )
        self.assertIn(
            feasibility["first_passing_active_block_budget"],
            feasibility["search_ladder"],
        )

    def test_the_search_is_bounded_and_deterministic(self) -> None:
        first = self._plan(success=0.0, cost=0.41, output="det-a")
        second = self._plan(success=0.0, cost=0.41, output="det-b")
        self.assertEqual(first["feasibility"], second["feasibility"])
        ladder = first["feasibility"]["search_ladder"]
        self.assertTrue(ladder)
        self.assertEqual(sorted(ladder), ladder)
        self.assertEqual(
            max(ladder),
            first["feasibility"]["largest_tested_active_block_budget"],
        )
        self.assertLessEqual(max(ladder), 100_000)

    def test_the_assumption_is_labelled(self) -> None:
        feasibility = self._plan(
            success=0.0, cost=0.41, output="labels",
        )["feasibility"]
        self.assertIn("fixed-effect plug-in", feasibility["assumption"])
        self.assertTrue(feasibility["not_achieved_power"])
        self.assertTrue(feasibility["not_a_confidence_guarantee"])


class AllocationArithmeticTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.f = _Fixture(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plan(self, budget: int, output: str) -> dict[str, Any]:
        plan_distributed_policy_oed(
            self.f.audit,
            policy_id=POLICY,
            stratum_weights=dict(TARGET_WEIGHTS),
            output_dir=self.root / output,
            active_evidence_block_budget=budget,
            minimum_independent_workloads_by_active_stratum={
                "causal": 3, "descriptive": 3,
            },
        )
        return json.loads(
            (self.root / output / "oed_plan.json").read_text(
                encoding="utf-8"
            )
        )

    def test_blocks_sum_to_the_budget(self) -> None:
        for budget in (6, 8, 20, 51):
            with self.subTest(budget=budget):
                plan = self._plan(budget, f"b{budget}")
                blocks = plan["active_evidence_blocks_by_stratum"]
                self.assertEqual(budget, sum(blocks.values()))
                self.assertEqual(
                    budget,
                    plan["budget"]["consumed_active_evidence_blocks"],
                )

    def test_sessions_equal_blocks_times_arms_times_repetitions(
        self,
    ) -> None:
        plan = self._plan(20, "sessions")
        blocks = plan["active_evidence_blocks_by_stratum"]
        reps = plan["repetitions_per_active_block"]
        self.assertEqual(
            sum(count * 2 * reps for count in blocks.values()),
            plan["planned_total_sessions"],
        )
        self.assertEqual(20 * 2 * reps, plan["planned_total_sessions"])

    def test_structural_strata_contribute_no_blocks_or_sessions(
        self,
    ) -> None:
        plan = self._plan(20, "structural")
        self.assertNotIn(
            "temporal",
            plan["active_evidence_blocks_by_stratum"],
        )
        self.assertEqual(
            0,
            plan["planned_candidate_sessions_by_stratum"]["temporal"],
        )
        self.assertEqual(
            0,
            plan["planned_safe_sessions_by_stratum"]["temporal"],
        )
        self.assertEqual(["temporal"], plan["structural_zero_strata"])

    def test_minima_are_seated_before_precision(self) -> None:
        plan = self._plan(6, "minima")
        blocks = plan["active_evidence_blocks_by_stratum"]
        self.assertEqual({"causal": 3, "descriptive": 3}, blocks)
        self.assertTrue(plan["minimum_requirements_satisfied"])
        self.assertEqual([], plan["unmet_minimum_strata"])
        purposes = {
            step.get("purpose") for step in plan["steps"]
        }
        self.assertIn("meet_frozen_minimum_independent_workloads", purposes)

    def test_an_unaffordable_minimum_is_reported_not_crashed(self) -> None:
        plan = self._plan(2, "unaffordable")
        self.assertFalse(plan["minimum_requirements_satisfied"])
        self.assertTrue(plan["unmet_minimum_strata"])
        self.assertFalse(
            plan["projected_weighted_certificate"]["available"]
        )
        self.assertNotEqual("COMMIT", plan["final_action"])



class OedWeightedIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.f = _Fixture(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plan(self, output: str = "oed") -> dict[str, Any]:
        plan_distributed_policy_oed(
            self.f.audit,
            policy_id=POLICY,
            stratum_weights=dict(TARGET_WEIGHTS),
            output_dir=self.root / output,
            active_evidence_block_budget=8,
            minimum_independent_workloads_by_active_stratum={
                "causal": 3, "descriptive": 3,
            },
        )
        return json.loads(
            (self.root / output / "oed_plan.json").read_text(
                encoding="utf-8"
            )
        )

    def test_oed_uses_the_same_weighted_aggregation_core(self) -> None:
        projection = self._plan()["projected_weighted_certificate"]
        self.assertTrue(projection["uses_same_weighted_aggregation_core"])
        for key in (
            "assumed_stratum_point_estimates",
            "projected_independent_workloads",
            "projected_stratum_bounds",
            "projected_overall_success_difference",
            "projected_overall_cost_saving",
            "projected_gate_results",
            "projected_certificate_state",
            "projected_distance_to_threshold",
        ):
            self.assertIn(key, projection, key)

    def test_the_projection_matches_a_direct_core_call(self) -> None:
        projection = self._plan("direct")["projected_weighted_certificate"]
        counts = projection["projected_independent_workloads"]
        assumed = projection["assumed_stratum_point_estimates"]
        direct = _evaluate([
            StratumEvidence(
                "causal", "active", 14,
                (assumed["causal"]["success_difference"],) * counts["causal"],
                (assumed["causal"]["cost_saving"],) * counts["causal"],
            ),
            StratumEvidence(
                "descriptive", "active", 8,
                (assumed["descriptive"]["success_difference"],)
                * counts["descriptive"],
                (assumed["descriptive"]["cost_saving"],)
                * counts["descriptive"],
            ),
            StratumEvidence("temporal", "structural_safe", 14),
        ])
        self.assertEqual(
            direct["overall_cost_saving"]["lower_bound"],
            projection["projected_overall_cost_saving"]["lower_bound"],
        )
        self.assertEqual(
            direct["certificate_state"],
            projection["projected_certificate_state"],
        )

    def test_projections_remain_labelled_as_planning_quantities(
        self,
    ) -> None:
        projection = self._plan("labels")["projected_weighted_certificate"]
        self.assertEqual(
            "posthoc-plugin-planning-projection",
            projection["projection_class"],
        )
        self.assertTrue(projection["not_achieved_power"])
        self.assertTrue(projection["not_a_confidence_guarantee"])
        self.assertTrue(projection["not_a_commit_authorization"])

    def test_the_oed_vocabulary_still_excludes_commit(self) -> None:
        plan = self._plan("nocommit")
        self.assertNotIn("COMMIT", plan["planning_actions_available"])
        self.assertNotEqual("COMMIT", plan["final_action"])
        self.assertNotEqual(
            "COMMIT",
            plan["projected_weighted_certificate"][
                "projected_certificate_state"
            ],
        )
        self.assertFalse(plan["commit_authorised"])
        rendered = json.dumps(plan)
        self.assertNotIn('"COMMIT"', rendered)


if __name__ == "__main__":
    unittest.main()
