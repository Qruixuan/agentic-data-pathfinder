"""Offline planning for a fresh distributed-policy confirmation cohort."""

from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from pathfinder.distributed import (
    CONFIRMATION_CONFIG_SCHEMA_VERSION,
    CONFIRMATION_PLAN_SCHEMA_VERSION,
    ESTIMAND_KIND,
    WEIGHTS_PROVENANCE,
    ConfirmationPlanError,
    freeze_confirmation_plan,
    plan_distributed_policy_oed,
)


#: The benchmark's own outcome-blind composition, exactly.
TARGET_WEIGHTS = {"causal": 14, "descriptive": 8, "temporal": 14}
EXPECTED_MODEL = "qwen3.8-27b"


SAFE = "D_origin_remote"
FRAMES = "D_local_frames"
POLICY = "temporal_origin_fallback"
STRATA = ("causal", "descriptive", "temporal")


def _csv_text(rows: list[dict[str, Any]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=list(rows[0]),
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _workload_id(stratum: str, index: int) -> str:
    return f"{stratum}-pilot-w{index:02d}"


def _object_id(stratum: str, index: int) -> str:
    return f"video-{stratum}-{index:02d}"


def build_policy_audit(
    root: Path,
    *,
    counts: Mapping[str, int] | None = None,
    temporal_role: str = "safe",
    posthoc: bool = True,
    eligible: bool = False,
    success_delta: float = -0.02,
    cost_saving: float = 0.72,
    thresholds: Mapping[str, float] | None = None,
) -> Path:
    """A minimal but structurally faithful policy-audit snapshot."""
    sizes = dict(counts or {"causal": 6, "descriptive": 4, "temporal": 4})
    assignment = {
        "causal": "candidate",
        "descriptive": "candidate",
        "temporal": temporal_role,
    }
    rows: list[dict[str, Any]] = []
    for stratum in STRATA:
        safe_stratum = assignment[stratum] == "safe"
        design = SAFE if safe_stratum else FRAMES
        for index in range(sizes[stratum]):
            rows.append({
                "policy_id": POLICY,
                "policy_provenance": "posthoc-selected",
                "workload_id": _workload_id(stratum, index),
                "stratum_id": stratum,
                "selected_role": "safe" if safe_stratum else "candidate",
                "selected_design_id": design,
                "safe_design_id": SAFE,
                "observed_candidate_design_id": FRAMES,
                "task_success_delta_vs_safe": (
                    0.0 if safe_stratum else success_delta
                ),
                "cost_saving_vs_safe": (
                    0.0 if safe_stratum else cost_saving
                ),
            })
    evaluation = {
        "schema_version": (
            "pathfinder.distributed-policy-awm-evaluation/v1alpha1"
        ),
        "audit_id": "synthetic-policy-audit",
        "pilot_id": "synthetic-pilot",
        "posthoc": posthoc,
        "eligible_for_scientific_claims": eligible,
        "complete_design_oracle": False,
        "independent_workloads": sum(sizes.values()),
        "repetitions_per_workload": 2,
        "safe_baseline": SAFE,
        "thresholds": dict(thresholds or {
            "alpha": 0.05,
            "delta_success_margin": 0.05,
            "minimum_cost_saving": 0.25,
        }),
        "policy_details": [{
            "policy_id": POLICY,
            "assignment_by_stratum": assignment,
            "certificate_state": "INSUFFICIENT_EVIDENCE",
            "cost_saving": {
                "point_estimate": cost_saving,
                "support_lower": -2.0,
                "support_upper": 2.0,
                "lower_bound": -0.33,
                "upper_bound": 1.52,
            },
            "gates": [{
                "gate_id": "success_non_inferiority",
                "support_lower": -1.0,
                "support_upper": 1.0,
                "point_estimate": success_delta,
                "result": "INDETERMINATE",
            }],
        }],
    }
    documents = {
        "policy_evaluation.json": json.dumps(
            evaluation, indent=2, sort_keys=True
        ) + "\n",
        "policy_manifest.json": json.dumps(
            {"audit_id": "synthetic-policy-audit"}, indent=2
        ) + "\n",
        "policy_summary.csv": _csv_text([
            {"policy_id": POLICY, "certificate_state": "INSUFFICIENT_EVIDENCE"}
        ]),
        "policy_workload_effects.csv": _csv_text(rows),
        "report.md": "# synthetic policy audit\n",
    }
    documents["SHA256SUMS"] = "".join(
        f"{sha256(content.encode('utf-8')).hexdigest()}  {name}\n"
        for name, content in sorted(documents.items())
    )
    root.mkdir(parents=True, exist_ok=True)
    for name, content in documents.items():
        (root / name).write_text(content, encoding="utf-8", newline="\n")
    return root


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def inspected_manifest(path: Path, counts: Mapping[str, int]) -> Path:
    _write_json(path, {
        _workload_id(stratum, index): {
            "stratum_id": stratum,
            "object_id": _object_id(stratum, index),
        }
        for stratum, size in counts.items()
        for index in range(size)
    })
    return path


def fresh_cohort(
    path: Path,
    counts: Mapping[str, int],
    *,
    prefix: str = "fresh",
) -> Path:
    _write_json(path, {
        f"{stratum}-{prefix}-w{index:02d}": {
            "stratum_id": stratum,
            "object_id": f"video-{prefix}-{stratum}-{index:02d}",
        }
        for stratum, size in counts.items()
        for index in range(size)
    })
    return path


def confirmation_config(
    path: Path,
    *,
    weights: Mapping[str, float] | None = None,
    planned: Mapping[str, int] | None = None,
    model: str = "qwen3.8-27b",
    repetitions: int = 2,
    minima: Mapping[str, int] | None = None,
    **extra: Any,
) -> Path:
    payload: dict[str, Any] = {
        "schema_version": CONFIRMATION_CONFIG_SCHEMA_VERSION,
        "plan_id": "synthetic-confirmation-plan",
        "selected_policy_id": POLICY,
        "execution_model_id": model,
        "repetitions": repetitions,
        "estimand": {
            "kind": ESTIMAND_KIND,
            "target_stratum_weights_integer": dict(
                weights or TARGET_WEIGHTS
            ),
        },
        "minimum_independent_workloads_by_active_stratum": dict(
            minima if minima is not None
            else {"causal": 3, "descriptive": 3}
        ),
        "planned_workloads_by_stratum": dict(planned or {
            "causal": 12,
            "descriptive": 6,
            "temporal": 12,
        }),
    }
    payload.update(extra)
    _write_json(path, payload)
    return path


class _Fixture:
    """A complete synthetic planning setup."""

    def __init__(self, root: Path, **audit: Any) -> None:
        self.root = root
        self.pilot_counts = {"causal": 6, "descriptive": 4, "temporal": 4}
        self.audit = build_policy_audit(
            root / "audit",
            counts=self.pilot_counts,
            **audit,
        )
        self.inspected = inspected_manifest(
            root / "inspected.json",
            self.pilot_counts,
        )
        self.planned = {"causal": 12, "descriptive": 6, "temporal": 12}
        self.cohort = fresh_cohort(root / "fresh.json", self.planned)
        self.config = confirmation_config(
            root / "config.json",
            planned=self.planned,
        )

    def freeze(self, *, output: str = "plan", **overrides: Any):
        arguments: dict[str, Any] = {
            "inspected_workload_manifests": [self.inspected],
            "fresh_cohort_manifest": self.cohort,
            "output_dir": self.root / output,
        }
        arguments.update(overrides)
        config = arguments.pop("config_path", self.config)
        audit = arguments.pop("policy_audit_dir", self.audit)
        return freeze_confirmation_plan(config, audit, **arguments)

    def plan_json(self, output: str = "plan") -> dict[str, Any]:
        return json.loads(
            (self.root / output / "confirmation_plan.json").read_text(
                encoding="utf-8"
            )
        )

    def oed(self, *, output: str = "oed", **overrides: Any):
        arguments: dict[str, Any] = {
            "policy_id": POLICY,
            "stratum_weights": dict(TARGET_WEIGHTS),
            "output_dir": self.root / output,
            "active_evidence_block_budget": 8,
        }
        arguments.update(overrides)
        return plan_distributed_policy_oed(self.audit, **arguments)


class FreshHoldoutDisjointnessTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_a_valid_disjoint_cohort_freezes(self) -> None:
        result = self.f.freeze()
        self.assertEqual("FROZEN", result["status"])
        self.assertFalse(result["authorises_commit"])
        self.assertFalse(result["eligible_for_scientific_claims"])

    def test_reusing_inspected_workload_ids_is_refused(self) -> None:
        reused = self.f.root / "reused.json"
        _write_json(reused, {
            _workload_id("causal", 0): {
                "stratum_id": "causal",
                "object_id": "video-brand-new-1",
            },
        })
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "reuses already-inspected workload IDs",
        ):
            self.f.freeze(
                fresh_cohort_manifest=reused,
                output="reused-plan",
            )

    def test_reusing_inspected_object_ids_is_refused(self) -> None:
        # Distinct workload ID, same underlying video: still contaminated.
        overlapping = self.f.root / "overlap.json"
        _write_json(overlapping, {
            "causal-brand-new-w00": {
                "stratum_id": "causal",
                "object_id": _object_id("causal", 0),
            },
        })
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "reuses already-inspected object/video IDs",
        ):
            self.f.freeze(
                fresh_cohort_manifest=overlapping,
                output="overlap-plan",
            )

    def test_the_selecting_cohort_cannot_be_the_confirmation_cohort(
        self,
    ) -> None:
        with self.assertRaises(ConfirmationPlanError):
            self.f.freeze(
                fresh_cohort_manifest=self.f.inspected,
                output="self-plan",
            )

    def test_multiple_inspected_manifests_are_all_enforced(self) -> None:
        second = inspected_manifest(
            self.f.root / "inspected2.json",
            {"causal": 2},
        )
        # Rebuild that second manifest with the fresh cohort's own IDs.
        _write_json(second, {
            "causal-fresh-w00": {
                "stratum_id": "causal",
                "object_id": "video-other",
            },
        })
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "already-inspected workload IDs",
        ):
            self.f.freeze(
                inspected_workload_manifests=[self.f.inspected, second],
                output="multi-plan",
            )


class SourceIntegrityTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_plan_freezes_source_audit_hashes(self) -> None:
        self.f.freeze()
        plan = self.f.plan_json()
        hashes = plan["source_policy_audit"]["file_sha256"]
        for name in (
            "policy_evaluation.json",
            "policy_manifest.json",
            "policy_summary.csv",
            "policy_workload_effects.csv",
            "report.md",
        ):
            self.assertEqual(64, len(hashes[name]), name)
        self.assertEqual(
            64,
            len(plan["future_certification_evidence"][
                "fresh_cohort_sha256"
            ]),
        )
        self.assertTrue(plan["inspected_workload_manifest_sha256"])

    def test_a_missing_checksum_file_is_refused(self) -> None:
        (self.f.audit / "SHA256SUMS").unlink()
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "SHA256SUMS is missing",
        ):
            self.f.freeze(output="missing-plan")

    def test_a_tampered_source_file_is_refused(self) -> None:
        (self.f.audit / "report.md").write_text(
            "# tampered\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "checksum mismatch",
        ):
            self.f.freeze(output="tampered-plan")

    def test_an_unbound_required_file_is_refused(self) -> None:
        path = self.f.audit / "SHA256SUMS"
        kept = [
            line for line in path.read_text(encoding="utf-8").splitlines()
            if "report.md" not in line
        ]
        path.write_text("\n".join(kept) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "does not bind report.md",
        ):
            self.f.freeze(output="unbound-plan")

    def test_an_eligible_source_audit_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _Fixture(root, eligible=True, posthoc=False)
            with self.assertRaisesRegex(
                ConfirmationPlanError,
                "must be post-hoc",
            ):
                fixture.freeze()

    def test_source_inputs_remain_byte_identical(self) -> None:
        before = {
            path.name: path.read_bytes()
            for path in sorted(self.f.audit.iterdir())
        }
        before["inspected"] = self.f.inspected.read_bytes()
        before["cohort"] = self.f.cohort.read_bytes()
        before["config"] = self.f.config.read_bytes()
        self.f.freeze()
        after = {
            path.name: path.read_bytes()
            for path in sorted(self.f.audit.iterdir())
        }
        after["inspected"] = self.f.inspected.read_bytes()
        after["cohort"] = self.f.cohort.read_bytes()
        after["config"] = self.f.config.read_bytes()
        self.assertEqual(before, after)


class StructuralZeroTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_temporal_fallback_is_a_structural_zero(self) -> None:
        self.f.freeze()
        plan = self.f.plan_json()
        by_id = {item["stratum_id"]: item for item in plan["strata"]}
        temporal = by_id["temporal"]
        self.assertEqual("structural_safe", temporal["role"])
        self.assertTrue(temporal["structural_zero_effect"])
        self.assertEqual(SAFE, temporal["policy_design_id"])
        self.assertFalse(temporal["candidate_measurement_planned"])
        self.assertEqual(["temporal"], plan["structural_safe_stratum_ids"])

    def test_no_redundant_safe_versus_safe_pair_is_scheduled(self) -> None:
        self.f.freeze()
        plan = self.f.plan_json()
        by_id = {item["stratum_id"]: item for item in plan["strata"]}
        # One arm, not two: a second identical safe run is not a pair.
        self.assertEqual(1, by_id["temporal"]["sessions_per_workload_block"])
        self.assertEqual(2, by_id["causal"]["sessions_per_workload_block"])

    def test_active_strata_are_distinguished_from_structural_ones(
        self,
    ) -> None:
        self.f.freeze()
        plan = self.f.plan_json()
        self.assertEqual(
            ["causal", "descriptive"],
            sorted(plan["active_stratum_ids"]),
        )
        self.assertEqual(
            plan["planned_active_workloads"],
            self.f.planned["causal"] + self.f.planned["descriptive"],
        )

    def test_a_non_zero_structural_effect_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit = build_policy_audit(root / "audit")
            path = audit / "policy_workload_effects.csv"
            text = path.read_text(encoding="utf-8").replace(
                f"temporal,safe,{SAFE},{SAFE},{FRAMES},0.0,0.0",
                f"temporal,safe,{SAFE},{SAFE},{FRAMES},0.5,0.0",
            )
            path.write_text(text, encoding="utf-8", newline="\n")
            sums = audit / "SHA256SUMS"
            sums.write_text("".join(
                f"{sha256(p.read_bytes()).hexdigest()}  {p.name}\n"
                for p in sorted(audit.iterdir()) if p.name != "SHA256SUMS"
            ), encoding="utf-8")
            with self.assertRaisesRegex(
                ConfirmationPlanError,
                "not a structural zero",
            ):
                plan_distributed_policy_oed(
                    audit,
                    policy_id=POLICY,
                    stratum_weights=dict(TARGET_WEIGHTS),
                    output_dir=root / "oed",
                    active_evidence_block_budget=4,
                )

    def test_an_all_safe_policy_has_nothing_to_confirm(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit = build_policy_audit(root / "audit")
            # Force every stratum to the safe design.
            path = audit / "policy_workload_effects.csv"
            rows = list(csv.DictReader(io.StringIO(
                path.read_text(encoding="utf-8")
            )))
            for row in rows:
                row["selected_design_id"] = SAFE
                row["selected_role"] = "safe"
                row["task_success_delta_vs_safe"] = "0.0"
                row["cost_saving_vs_safe"] = "0.0"
            path.write_text(_csv_text(rows), encoding="utf-8", newline="\n")
            (audit / "SHA256SUMS").write_text("".join(
                f"{sha256(p.read_bytes()).hexdigest()}  {p.name}\n"
                for p in sorted(audit.iterdir()) if p.name != "SHA256SUMS"
            ), encoding="utf-8")
            with self.assertRaisesRegex(
                ConfirmationPlanError,
                "nothing to plan",
            ):
                plan_distributed_policy_oed(
                    audit,
                    policy_id=POLICY,
                    stratum_weights=dict(TARGET_WEIGHTS),
                    output_dir=root / "oed",
                    active_evidence_block_budget=4,
                )


class EstimandTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_changing_weights_changes_the_estimand_and_plan_hash(
        self,
    ) -> None:
        first = self.f.freeze(output="weights-a")
        other = confirmation_config(
            self.f.root / "config-b.json",
            weights={"causal": 5, "descriptive": 3, "temporal": 2},
            planned=self.f.planned,
        )
        second = self.f.freeze(config_path=other, output="weights-b")
        self.assertNotEqual(first["plan_sha256"], second["plan_sha256"])
        self.assertNotEqual(
            self.f.plan_json("weights-a")["estimand"][
                "target_stratum_weights_integer"
            ],
            self.f.plan_json("weights-b")["estimand"][
                "target_stratum_weights_integer"
            ],
        )

    def test_rounded_fractions_are_not_silently_accepted(self) -> None:
        """0.4/0.2/0.4 must never stand in for the exact 14:8:14 mixture."""
        bad = confirmation_config(
            self.f.root / "config-rounded.json",
            weights={"causal": 0.4, "descriptive": 0.2, "temporal": 0.4},
            planned=self.f.planned,
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "must be an integer quota, not a rounded fraction",
        ):
            self.f.freeze(config_path=bad, output="rounded-plan")

    def test_a_zero_total_weight_is_refused(self) -> None:
        bad = confirmation_config(
            self.f.root / "config-zero.json",
            weights={"causal": 0, "descriptive": 0, "temporal": 0},
            planned=self.f.planned,
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "positive total",
        ):
            self.f.freeze(config_path=bad, output="zero-weight-plan")

    def test_unknown_strata_are_refused(self) -> None:
        bad = confirmation_config(
            self.f.root / "config-unknown.json",
            weights={"causal": 14, "descriptive": 8, "spatial": 14},
            planned={"causal": 12, "descriptive": 6, "spatial": 12},
            minima={"causal": 3, "descriptive": 3, "spatial": 3},
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "must exactly match the selected policy",
        ):
            self.f.freeze(config_path=bad, output="unknown-plan")

    def test_weights_without_a_positive_total_are_refused(self) -> None:
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "would silently change the estimand",
        ):
            self.f.oed(
                stratum_weights={
                    "causal": 0,
                    "descriptive": 0,
                    "temporal": 0,
                },
                output="zero-total",
            )

    def test_the_oed_planner_refuses_rounded_fractions(self) -> None:
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "integer quota, not a rounded fraction",
        ):
            self.f.oed(
                stratum_weights={
                    "causal": 0.4,
                    "descriptive": 0.2,
                    "temporal": 0.4,
                },
                output="rounded-oed",
            )

    def test_cohort_composition_must_match_the_declared_plan(self) -> None:
        mismatched = fresh_cohort(
            self.f.root / "mismatch.json",
            {"causal": 3, "descriptive": 6, "temporal": 12},
            prefix="mm",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "plans 12 workloads but the fresh cohort supplies 3",
        ):
            self.f.freeze(
                fresh_cohort_manifest=mismatched,
                output="mismatch-plan",
            )

    def test_repetitions_never_increase_independent_units(self) -> None:
        many = confirmation_config(
            self.f.root / "config-reps.json",
            planned=self.f.planned,
            repetitions=8,
        )
        result = self.f.freeze(config_path=many, output="reps-plan")
        plan = self.f.plan_json("reps-plan")
        self.assertFalse(plan["repetitions_increase_independent_units"])
        self.assertEqual(8, plan["repetitions"])
        self.assertEqual(
            sum(self.f.planned.values()),
            plan["planned_independent_workloads"],
            "more repetitions must not add independent workloads",
        )
        self.assertGreater(result["planned_sessions"], 0)

    def test_a_foreign_execution_model_is_refused(self) -> None:
        bad = confirmation_config(
            self.f.root / "config-model.json",
            planned=self.f.planned,
            model="some-other-model",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "different runtime model is a different experiment",
        ):
            self.f.freeze(config_path=bad, output="model-plan")


class OedPlanningTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plan(self, output: str = "oed") -> dict[str, Any]:
        return json.loads(
            (self.f.root / output / "oed_plan.json").read_text(
                encoding="utf-8"
            )
        )

    def test_actions_are_independent_workload_blocks(self) -> None:
        result = self.f.oed()
        plan = self._plan()
        self.assertEqual(
            "one-additional-independent-workload-block",
            plan["action_unit"],
        )
        for step in plan["steps"]:
            self.assertEqual("COLLECT", step["action"])
            self.assertEqual(1, step["independent_workload_blocks"])
            # Cost is the full paired block, not a single session.
            self.assertEqual(
                plan["repetitions_per_workload"] * 2,
                step["sessions"],
            )
        self.assertEqual(
            result["consumed_active_evidence_blocks"],
            len(plan["steps"]),
        )

    def test_no_blocks_are_allocated_to_a_structural_safe_stratum(
        self,
    ) -> None:
        self.f.oed()
        plan = self._plan()
        temporal = plan["allocation_by_stratum"]["temporal"]
        self.assertEqual(0, temporal["additional_independent_workloads"])
        self.assertTrue(temporal["structural_zero_effect"])
        self.assertFalse(temporal["candidate_measurement_planned"])
        self.assertNotIn(
            "temporal",
            {step["stratum_id"] for step in plan["steps"]},
        )

    def test_post_hoc_planning_never_emits_commit(self) -> None:
        for blocks in (0, 1, 8, 64):
            with self.subTest(budget=blocks):
                result = self.f.oed(
                    active_evidence_block_budget=blocks,
                    output=f"oed-{blocks}",
                )
                plan = self._plan(f"oed-{blocks}")
                self.assertFalse(result["commit_authorised"])
                self.assertNotIn("COMMIT", plan["planning_actions_available"])
                self.assertIn(
                    plan["final_action"],
                    ("STOP_INSUFFICIENT_BUDGET", "FREEZE_CONFIRMATION_PLAN"),
                )
                self.assertNotEqual("COMMIT", plan["final_action"])
                for step in plan["steps"]:
                    self.assertNotEqual("COMMIT", step["action"])

    def test_a_zero_budget_stops_safely(self) -> None:
        result = self.f.oed(active_evidence_block_budget=0, output="zero")
        plan = self._plan("zero")
        self.assertEqual("STOP_INSUFFICIENT_BUDGET", result["final_action"])
        self.assertEqual("insufficient_budget", plan["stop_reason"])
        self.assertEqual(0, result["consumed_active_evidence_blocks"])
        self.assertEqual([], plan["steps"])

    def test_a_session_budget_below_one_block_stops_safely(self) -> None:
        result = self.f.oed(total_sessions=3, output="tight")
        self.assertEqual("STOP_INSUFFICIENT_BUDGET", result["final_action"])
        self.assertEqual(0, result["consumed_active_evidence_blocks"])

    def test_a_budget_is_required(self) -> None:
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "budget is required",
        ):
            self.f.oed(
                active_evidence_block_budget=None,
                total_sessions=None,
                output="nobudget",
            )

    def test_allocation_is_deterministic(self) -> None:
        first = self.f.oed(output="det-a")
        second = self.f.oed(output="det-b")
        self.assertEqual(first["plan_sha256"], second["plan_sha256"])
        self.assertEqual(
            first["active_evidence_blocks_by_stratum"],
            second["active_evidence_blocks_by_stratum"],
        )

    def test_more_budget_never_widens_a_projected_gate(self) -> None:
        self.f.oed(active_evidence_block_budget=2, output="small")
        self.f.oed(active_evidence_block_budget=20, output="large")
        small = self._plan("small")["allocation_by_stratum"]
        large = self._plan("large")["allocation_by_stratum"]
        for stratum in ("causal", "descriptive"):
            self.assertLessEqual(
                large[stratum]["projected_worst_gate_width"],
                small[stratum]["projected_worst_gate_width"],
            )

    def test_a_narrow_margin_is_reported(self) -> None:
        self.f.oed(output="narrow")
        plan = self._plan("narrow")
        self.assertTrue(
            plan["margin_warnings"],
            "a point estimate near its threshold must be flagged",
        )
        for warning in plan["margin_warnings"]:
            self.assertIn(warning["gate_id"], (
                "success_non_inferiority",
                "cost_improvement",
            ))
            self.assertIn("normalised_margin", warning)

    def test_projections_are_labelled_as_planning_quantities(self) -> None:
        self.f.oed(output="labels")
        plan = self._plan("labels")
        self.assertIn("NOT achieved power", plan["projection_semantics"])
        self.assertFalse(plan["eligible_for_scientific_claims"])
        self.assertTrue(plan["posthoc_planning_data"])


class OutputSafetyTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_an_existing_output_directory_is_refused(self) -> None:
        self.f.freeze(output="once")
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "output directory already exists",
        ):
            self.f.freeze(output="once")

    def test_a_refused_write_leaves_no_partial_output(self) -> None:
        target = self.f.root / "partial"
        (self.f.audit / "report.md").write_text("# broken\n", encoding="utf-8")
        with self.assertRaises(ConfirmationPlanError):
            self.f.freeze(output="partial")
        self.assertFalse(
            target.exists(),
            "a failed plan must leave no directory behind",
        )

    def test_outputs_carry_deterministic_checksums(self) -> None:
        self.f.freeze()
        sums = (self.f.root / "plan" / "SHA256SUMS").read_text(
            encoding="utf-8"
        )
        names = {line.split("  ", 1)[1] for line in sums.splitlines()}
        self.assertEqual(
            {
                "confirmation_plan.json",
                "confirmation_strata.csv",
                "confirmation_manifest.json",
            },
            names,
        )
        for line in sums.splitlines():
            digest, name = line.split("  ", 1)
            actual = sha256(
                (self.f.root / "plan" / name).read_bytes()
            ).hexdigest()
            self.assertEqual(digest, actual, name)

    def test_the_complete_output_tree_is_path_independent(self) -> None:
        """Every checksummed file, manifest included, must match byte for
        byte when identical inputs live under different absolute trees."""
        trees = []
        for name in ("alpha-root", "beta-root"):
            with tempfile.TemporaryDirectory(suffix=name) as temporary:
                root = Path(temporary) / name / "nested" / "deeper"
                root.mkdir(parents=True)
                fixture = _Fixture(root)
                fixture.freeze()
                fixture.oed()
                captured: dict[str, bytes] = {}
                for sub in ("plan", "oed"):
                    for path in sorted((root / sub).iterdir()):
                        captured[f"{sub}/{path.name}"] = path.read_bytes()
                trees.append(captured)
        self.assertEqual(
            sorted(trees[0]),
            sorted(trees[1]),
            "the two output trees must contain the same files",
        )
        for name in trees[0]:
            self.assertEqual(
                trees[0][name],
                trees[1][name],
                f"{name} differs between absolute directory trees",
            )
        # Including the checksum files themselves.
        self.assertIn("plan/SHA256SUMS", trees[0])
        self.assertIn("oed/SHA256SUMS", trees[0])
        for blob in trees[0].values():
            self.assertNotIn(b"/tmp/", blob)

    def test_the_manifest_records_no_absolute_path(self) -> None:
        self.f.freeze()
        manifest_text = (
            self.f.root / "plan" / "confirmation_manifest.json"
        ).read_text(encoding="utf-8")
        manifest = json.loads(manifest_text)
        self.assertNotIn(str(self.f.root), manifest_text)
        self.assertNotIn("/tmp/", manifest_text)
        self.assertTrue(manifest["portable"])
        self.assertFalse(manifest["absolute_paths_recorded"])
        self.assertFalse(manifest["authorises_commit"])
        self.assertFalse(manifest["credentials_recorded"])
        # Inputs are identified by role and hash instead.
        inputs = manifest["inputs"]
        self.assertEqual(
            "posthoc-policy-selection-evidence",
            inputs["policy_audit"]["role"],
        )
        self.assertEqual(
            64,
            len(inputs["confirmation_config"]["sha256"]),
        )

    def test_operator_paths_are_console_only(self) -> None:
        result = self.f.freeze(output="console")
        self.assertIn("console_only_paths", result)
        self.assertTrue(
            Path(result["console_only_paths"]["output_dir"]).is_absolute()
        )
        for path in (self.f.root / "console").iterdir():
            self.assertNotIn(
                str(self.f.root),
                path.read_text(encoding="utf-8"),
                f"{path.name} must not embed an absolute path",
            )

    def test_planning_performs_no_network_or_deployment_call(self) -> None:
        import socket
        from unittest import mock

        def _refuse(*args: Any, **kwargs: Any):
            raise AssertionError("planning must not open a socket")

        with mock.patch.object(socket, "socket", _refuse), \
                mock.patch.object(socket, "create_connection", _refuse):
            self.f.freeze(output="offline-plan")
            self.f.oed(output="offline-oed")
        plan = self.f.plan_json("offline-plan")
        self.assertTrue(plan["offline"])
        self.assertFalse(plan["credentials_recorded"])


class TargetWeightRepresentationTest(unittest.TestCase):
    """The outcome-blind 14:8:14 mixture must survive exactly."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_exact_benchmark_mixture_normalizes_deterministically(
        self,
    ) -> None:
        self.f.freeze()
        estimand = self.f.plan_json()["estimand"]
        self.assertEqual(
            {"causal": 14, "descriptive": 8, "temporal": 14},
            estimand["target_stratum_weights_integer"],
        )
        self.assertEqual(36, estimand["target_stratum_weight_total"])
        self.assertEqual(
            {
                "causal": 14 / 36,
                "descriptive": 8 / 36,
                "temporal": 14 / 36,
            },
            estimand["target_stratum_weights_normalized"],
        )
        # 7/18 and 4/18 exactly, not a rounded 0.4/0.2/0.4.
        self.assertAlmostEqual(
            7 / 18,
            estimand["target_stratum_weights_normalized"]["causal"],
            places=15,
        )
        self.assertNotEqual(
            0.4,
            estimand["target_stratum_weights_normalized"]["causal"],
        )

    def test_the_weights_record_their_outcome_blind_provenance(
        self,
    ) -> None:
        self.f.freeze()
        estimand = self.f.plan_json()["estimand"]
        self.assertEqual(
            WEIGHTS_PROVENANCE,
            estimand["weights_provenance"],
        )
        self.assertEqual(
            "outcome-blind-benchmark-selection-protocol",
            estimand["weights_provenance"],
        )
        self.assertIn("frozen weights", estimand["aggregation_rule"])
        self.assertIn(
            "NOT the empirical sample proportions",
            estimand["aggregation_rule"],
        )

    def test_changing_the_mixture_changes_the_plan_hash(self) -> None:
        baseline = self.f.freeze(output="base")
        changed = confirmation_config(
            self.f.root / "config-mix.json",
            weights={"causal": 15, "descriptive": 8, "temporal": 13},
            planned=self.f.planned,
        )
        other = self.f.freeze(config_path=changed, output="mix")
        self.assertNotEqual(baseline["plan_sha256"], other["plan_sha256"])

    def test_an_equivalent_ratio_is_still_a_distinct_declaration(
        self,
    ) -> None:
        """28:16:28 normalizes identically but is a different declaration."""
        baseline = self.f.freeze(output="base2")
        doubled = confirmation_config(
            self.f.root / "config-double.json",
            weights={"causal": 28, "descriptive": 16, "temporal": 28},
            planned=self.f.planned,
        )
        other = self.f.freeze(config_path=doubled, output="double")
        self.assertNotEqual(baseline["plan_sha256"], other["plan_sha256"])
        self.assertEqual(
            self.f.plan_json("base2")["estimand"][
                "target_stratum_weights_normalized"
            ],
            self.f.plan_json("double")["estimand"][
                "target_stratum_weights_normalized"
            ],
        )

    def test_the_single_supported_estimand_mode_is_enforced(self) -> None:
        bad = confirmation_config(
            self.f.root / "config-mode.json",
            planned=self.f.planned,
        )
        payload = json.loads(bad.read_text(encoding="utf-8"))
        payload["estimand"]["kind"] = "fixed_cohort_composition"
        _write_json(bad, payload)
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "supports exactly one estimand mode",
        ):
            self.f.freeze(config_path=bad, output="mode-plan")

    def test_allocation_versus_target_is_reported_explicitly(self) -> None:
        self.f.freeze()
        allocation = self.f.plan_json()["collection_allocation"]
        self.assertEqual(
            self.f.planned,
            allocation["planned_workloads_by_stratum"],
        )
        self.assertIn("matches_target_weights", allocation)
        self.assertIsInstance(allocation["matches_target_weights"], bool)
        self.assertIn("precision decision", allocation["note"])


class ExecutionModelBindingTest(unittest.TestCase):
    """The future run's model is part of the plan and its hash."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_expected_model_is_recorded_in_the_portable_plan(
        self,
    ) -> None:
        result = self.f.freeze()
        plan = self.f.plan_json()
        self.assertEqual(EXPECTED_MODEL, plan["execution_model_id"])
        self.assertEqual(EXPECTED_MODEL, result["execution_model_id"])
        self.assertEqual(
            EXPECTED_MODEL,
            plan["future_certificate_requirements"][
                "execution_model_must_match"
            ],
        )

    def test_the_model_is_inside_the_plan_hash(self) -> None:
        self.f.freeze()
        text = (
            self.f.root / "plan" / "confirmation_plan.json"
        ).read_text(encoding="utf-8")
        self.assertIn(EXPECTED_MODEL, text)
        # Mutating only the model changes the document digest.
        mutated = text.replace(EXPECTED_MODEL, "some-other-model")
        self.assertNotEqual(
            sha256(text.encode("utf-8")).hexdigest(),
            sha256(mutated.encode("utf-8")).hexdigest(),
        )

    def test_a_different_model_cannot_be_planned(self) -> None:
        for model in ("qwen/qwen3.6-27b", "qwen3.6-27b", "gpt-4o"):
            with self.subTest(model=model):
                bad = confirmation_config(
                    self.f.root / f"cfg-{abs(hash(model))}.json",
                    planned=self.f.planned,
                    model=model,
                )
                with self.assertRaisesRegex(
                    ConfirmationPlanError,
                    "different runtime model is a different experiment",
                ):
                    self.f.freeze(
                        config_path=bad,
                        output=f"m-{abs(hash(model))}",
                    )

    def test_the_old_model_assumption_is_not_inherited(self) -> None:
        self.f.freeze()
        text = (
            self.f.root / "plan" / "confirmation_plan.json"
        ).read_text(encoding="utf-8")
        self.assertNotIn("qwen3.6", text)
        self.assertNotIn("qwen/qwen3.6-27b", text)

    def test_a_future_run_on_another_model_is_not_acceptable(self) -> None:
        """The frozen contract is what a later certificate must enforce."""
        self.f.freeze()
        requirements = self.f.plan_json()["future_certificate_requirements"]
        required_model = requirements["execution_model_must_match"]
        self.assertEqual(EXPECTED_MODEL, required_model)
        for candidate in ("qwen3.6-27b", "qwen/qwen3.6-27b", "other"):
            self.assertNotEqual(
                required_model,
                candidate,
                "a run on this model must fail the frozen model check",
            )


class BudgetSemanticsTest(unittest.TestCase):
    """A block budget is paired active evidence, not a cohort size."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plan(self, output: str = "oed") -> dict[str, Any]:
        return json.loads(
            (self.f.root / output / "oed_plan.json").read_text(
                encoding="utf-8"
            )
        )

    def test_the_budget_is_named_and_documented_as_active_evidence(
        self,
    ) -> None:
        self.f.oed(active_evidence_block_budget=10, output="named")
        budget = self._plan("named")["budget"]
        self.assertEqual(10, budget["active_evidence_block_budget"])
        self.assertEqual(
            10,
            budget["consumed_active_evidence_blocks"],
        )
        self.assertIn("NOT an", budget["unit_note"])
        self.assertNotIn("total_workload_blocks", budget)

    def test_every_required_accounting_field_is_reported(self) -> None:
        self.f.oed(active_evidence_block_budget=10, output="fields")
        plan = self._plan("fields")
        for key in (
            "target_stratum_weights",
            "active_evidence_blocks_by_stratum",
            "structural_zero_strata",
            "structural_zero_workloads_do_not_require_candidate_execution",
            "planned_safe_sessions_by_stratum",
            "planned_candidate_sessions_by_stratum",
            "planned_total_sessions",
            "repetitions_per_active_block",
            "block_cost_formula",
            "collection_allocation_matches_target_weights",
        ):
            self.assertIn(key, plan, key)
        self.assertEqual(["temporal"], plan["structural_zero_strata"])
        self.assertTrue(plan[
            "structural_zero_workloads_do_not_require_candidate_execution"
        ])

    def test_session_accounting_is_internally_consistent(self) -> None:
        self.f.oed(active_evidence_block_budget=10, output="sessions")
        plan = self._plan("sessions")
        reps = plan["repetitions_per_active_block"]
        blocks = plan["active_evidence_blocks_by_stratum"]
        safe = plan["planned_safe_sessions_by_stratum"]
        candidate = plan["planned_candidate_sessions_by_stratum"]
        for stratum_id, count in blocks.items():
            self.assertEqual(count * reps, safe[stratum_id])
            self.assertEqual(count * reps, candidate[stratum_id])
        # Structural-safe strata schedule no candidate execution at all.
        self.assertEqual(0, candidate["temporal"])
        self.assertEqual(
            sum(safe.values()) + sum(candidate.values()),
            plan["planned_total_sessions"],
        )
        self.assertIn("2 arms", plan["block_cost_formula"])

    def test_the_allocation_intentionally_differs_from_target_weights(
        self,
    ) -> None:
        self.f.oed(active_evidence_block_budget=10, output="differs")
        plan = self._plan("differs")
        self.assertFalse(
            plan["collection_allocation_matches_target_weights"],
            "temporal receives no active blocks, so the allocation cannot "
            "match the target mixture",
        )
        self.assertIn(
            "intentionally differs",
            plan["collection_allocation_note"],
        )
        self.assertIn(
            "frozen weights",
            plan["collection_allocation_note"],
        )

    def test_the_target_weights_are_carried_into_the_oed_plan(self) -> None:
        self.f.oed(active_evidence_block_budget=4, output="carried")
        estimand = self._plan("carried")["estimand"]
        self.assertEqual(
            {"causal": 14, "descriptive": 8, "temporal": 14},
            estimand["target_stratum_weights_integer"],
        )
        self.assertEqual(
            {
                "causal": 14 / 36,
                "descriptive": 8 / 36,
                "temporal": 14 / 36,
            },
            estimand["target_stratum_weights_normalized"],
        )
        self.assertEqual(ESTIMAND_KIND, estimand["kind"])



class FutureCertificateContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.f = _Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_plan_states_every_future_requirement(self) -> None:
        self.f.freeze()
        requirements = self.f.plan_json()["future_certificate_requirements"]
        for key in (
            "policy_identity_must_match",
            "thresholds_and_supports_must_match",
            "workloads_must_be_disjoint_from_selection_evidence",
            "active_strata_require_complete_safe_and_candidate_blocks",
            "telemetry_complete_must_be_literally_true",
            "artifact_delivery_complete_must_be_literally_true",
            "stratum_weights_must_match",
            "policy_may_not_change_after_freezing",
            "every_non_safe_certificate_result_retains_fallback",
        ):
            self.assertTrue(requirements[key], key)
        self.assertEqual("qwen3.8-27b", requirements[
            "execution_model_must_match"
        ])
        self.assertEqual(SAFE, requirements["fallback_design_id"])

    def test_selection_and_certification_evidence_are_separated(
        self,
    ) -> None:
        self.f.freeze()
        plan = self.f.plan_json()
        self.assertTrue(
            plan["policy_selection"]["selected_from_inspected_evidence"]
        )
        self.assertEqual(
            "NOT_YET_COLLECTED",
            plan["future_certification_evidence"]["status"],
        )
        self.assertTrue(plan["posthoc_selection"])
        self.assertFalse(plan["eligible_for_scientific_claims"])
        self.assertFalse(plan["authorises_commit"])

    def test_thresholds_and_supports_are_carried_forward(self) -> None:
        self.f.freeze()
        plan = self.f.plan_json()
        self.assertEqual(
            {
                "delta_success_margin": 0.05,
                "minimum_cost_saving": 0.25,
                "alpha": 0.05,
            },
            plan["thresholds"],
        )
        self.assertEqual([-1.0, 1.0], plan["supports"]["success_difference"])
        self.assertEqual([-2.0, 2.0], plan["supports"]["cost_saving"])

    def test_a_threshold_mismatch_in_the_source_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _Fixture(root, thresholds={
                "alpha": 0.05,
                "delta_success_margin": 0.10,
                "minimum_cost_saving": 0.25,
            })
            with self.assertRaisesRegex(
                ConfirmationPlanError,
                "delta_success_margin is not 0.05",
            ):
                fixture.freeze()

    def test_an_unknown_policy_id_is_refused(self) -> None:
        bad = confirmation_config(
            self.f.root / "config-policy.json",
            planned=self.f.planned,
            selected_policy_id="no_such_policy",
        )
        with self.assertRaisesRegex(
            ConfirmationPlanError,
            "exactly one policy must match",
        ):
            self.f.freeze(config_path=bad, output="policy-plan")

    def test_the_plan_schema_version_is_explicit(self) -> None:
        self.f.freeze()
        self.assertEqual(
            CONFIRMATION_PLAN_SCHEMA_VERSION,
            self.f.plan_json()["schema_version"],
        )


if __name__ == "__main__":
    unittest.main()
