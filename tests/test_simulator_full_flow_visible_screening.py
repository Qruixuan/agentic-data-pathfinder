"""Focused coverage for visible-development demo screening.

These tests pin the properties that keep the screening honest: the candidate
pool and its order come from public metadata only, the selection rule is
frozen before outcomes are seen, an answer-format artifact can never be
reported as a representation-quality difference, and no hidden label or
credential enters any frozen artifact.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pathfinder.simulator.full_flow_visible_screening import (
    SELECTION_KIND,
    VisibleScreeningError,
    build_candidate_pool,
    classify_action_outcome,
    evaluate_selection_rule,
    verify_checksums,
)

EXACT = "multiple-choice-option-id-exact-match-v1"
CANONICAL = "multiple-choice-option-id-canonical-match-v1"

ACTIONS = [
    {"action_id": "direct-video", "design_id": "D0", "repetition": "r0000",
     "executor_node_id": "N7"},
    {"action_id": "remote-derived", "design_id": "D2", "repetition": "r0000",
     "executor_node_id": "N7"},
]


def _trial(workload: str, design: str, *, direct: bool, node: str = "N7") -> dict:
    return {
        "trial_key": f"scenario|{workload}|{design}|r0000",
        "workload_id": workload,
        "executor_node_id": node,
        "route_family": "raw" if direct else "remote-derived",
        "semantic_input_profile": {
            "profile_id": "raw-direct-video-v1" if direct else "derived-sparse-frames-4-v1",
            "direct_video_input": direct,
        },
    }


def _task(workload: str, obj: str, rule: str = EXACT) -> dict:
    return {
        "workload_id": workload,
        "object_id": obj,
        "task_class_id": "video_qa",
        "question": f"question for {workload}",
        "answer_options": [
            {"option_id": "A", "text": "a"},
            {"option_id": "B", "text": "b"},
        ],
        "success_scoring_rule": rule,
        # A hidden label must never be consumed even when present upstream.
        "correct_answer_id": "B",
    }


def _pool(**kwargs: Any):
    trials = []
    for w in ("smoke-causal", "smoke-temporal", "smoke-descriptive"):
        trials.append(_trial(w, "D0", direct=True))
        trials.append(_trial(w, "D2", direct=False))
    tasks = [
        _task("smoke-causal", "obj-causal"),
        _task("smoke-temporal", "obj-temporal"),
        _task("smoke-descriptive", "obj-descriptive"),
    ]
    params = {
        "bound_trials": trials,
        "public_tasks": tasks,
        "screening_actions": ACTIONS,
        "stratum_by_workload": {
            "smoke-causal": "causal",
            "smoke-temporal": "temporal",
            "smoke-descriptive": "descriptive",
        },
        "seed": "fixed-seed-v1",
    }
    params.update(kwargs)
    return build_candidate_pool(**params)


class CandidatePoolTest(unittest.TestCase):
    def test_pool_is_built_from_public_metadata_only(self) -> None:
        pool = _pool()
        self.assertEqual(3, len(pool))
        serialized = json.dumps(pool)
        # The hidden label present on the upstream task must not propagate.
        # Public option IDs legitimately appear; the answer key must not.
        self.assertNotIn("correct_answer_id", serialized)
        for row in pool:
            self.assertNotIn("correct_answer_id", row)
            # Only a digest of the public question is retained, never text.
            self.assertIn("public_question_sha256", row)
            self.assertNotIn("question", row)
            self.assertEqual(["A", "B"], row["public_option_ids"])

    def test_order_is_deterministic_and_seed_dependent(self) -> None:
        first = [row["workload_id"] for row in _pool()]
        again = [row["workload_id"] for row in _pool()]
        self.assertEqual(first, again)
        other = [row["workload_id"] for row in _pool(seed="different-seed")]
        self.assertCountEqual(first, other)
        # Order indexes are always dense and ascending.
        self.assertEqual(
            list(range(3)), [row["order_index"] for row in _pool()]
        )

    def test_order_does_not_depend_on_outcomes_or_task_text(self) -> None:
        baseline = [row["workload_id"] for row in _pool()]
        tasks = [
            _task("smoke-causal", "obj-causal", CANONICAL),
            _task("smoke-temporal", "obj-temporal", CANONICAL),
            _task("smoke-descriptive", "obj-descriptive", CANONICAL),
        ]
        for task in tasks:
            task["question"] = "completely different question text"
        changed = [row["workload_id"] for row in _pool(public_tasks=tasks)]
        self.assertEqual(baseline, changed)

    def test_workload_without_every_planned_action_is_excluded(self) -> None:
        trials = [
            _trial("smoke-causal", "D0", direct=True),
            _trial("smoke-causal", "D2", direct=False),
            # temporal can only run direct video, so it is not eligible
            _trial("smoke-temporal", "D0", direct=True),
        ]
        pool = _pool(bound_trials=trials)
        self.assertEqual(["smoke-causal"], [r["workload_id"] for r in pool])

    def test_excluded_workload_is_not_rescreened(self) -> None:
        pool = _pool(exclude_workload_ids=["smoke-causal"])
        self.assertNotIn("smoke-causal", [r["workload_id"] for r in pool])
        self.assertEqual(2, len(pool))

    def test_action_on_unplanned_node_fails_closed(self) -> None:
        trials = [
            _trial("smoke-causal", "D0", direct=True, node="N8"),
            _trial("smoke-causal", "D2", direct=False),
        ]
        with self.assertRaisesRegex(VisibleScreeningError, "planned node"):
            _pool(bound_trials=trials)


class FormatAmbiguityTest(unittest.TestCase):
    """A scoring-format artifact must never look like a quality difference."""

    def test_bracketed_false_under_exact_match_is_ambiguous(self) -> None:
        outcome = classify_action_outcome(
            success_scoring_rule=EXACT,
            predicted_answer="[C]",
            task_success=False,
        )
        self.assertFalse(outcome["answer_is_bare_option_id"])
        self.assertTrue(outcome["format_ambiguous"])
        self.assertFalse(outcome["usable_for_demo_difference"])

    def test_bracketed_false_under_canonical_rule_is_a_real_result(self) -> None:
        outcome = classify_action_outcome(
            success_scoring_rule=CANONICAL,
            predicted_answer="[C]",
            task_success=False,
        )
        self.assertFalse(outcome["format_ambiguous"])
        self.assertTrue(outcome["usable_for_demo_difference"])

    def test_bare_answers_are_always_usable(self) -> None:
        for rule in (EXACT, CANONICAL):
            for success in (True, False):
                outcome = classify_action_outcome(
                    success_scoring_rule=rule,
                    predicted_answer="C",
                    task_success=success,
                )
                self.assertTrue(outcome["usable_for_demo_difference"])

    def test_ambiguous_action_cannot_select_a_demo_case(self) -> None:
        verdict = evaluate_selection_rule(
            {
                "direct-video": classify_action_outcome(
                    success_scoring_rule=EXACT,
                    predicted_answer="[C]",
                    task_success=False,
                ),
                "remote-derived": classify_action_outcome(
                    success_scoring_rule=EXACT,
                    predicted_answer="C",
                    task_success=True,
                ),
            },
            primary_action_id="direct-video",
            cheap_action_id="remote-derived",
        )
        self.assertFalse(verdict["selected"])
        self.assertEqual("answer-format-ambiguous", verdict["reason"])


class SelectionRuleTest(unittest.TestCase):
    @staticmethod
    def _outcomes(primary: bool, cheap: bool) -> dict[str, Any]:
        return {
            "direct-video": classify_action_outcome(
                success_scoring_rule=CANONICAL,
                predicted_answer="C",
                task_success=primary,
            ),
            "remote-derived": classify_action_outcome(
                success_scoring_rule=CANONICAL,
                predicted_answer="C",
                task_success=cheap,
            ),
        }

    def _verdict(self, primary: bool, cheap: bool) -> dict[str, Any]:
        return evaluate_selection_rule(
            self._outcomes(primary, cheap),
            primary_action_id="direct-video",
            cheap_action_id="remote-derived",
        )

    def test_primary_rule_is_direct_video_only_success(self) -> None:
        verdict = self._verdict(True, False)
        self.assertEqual("primary", verdict["rule"])
        self.assertTrue(verdict["selected"])

    def test_reverse_difference_is_a_fallback_not_a_primary(self) -> None:
        verdict = self._verdict(False, True)
        self.assertEqual("fallback", verdict["rule"])
        self.assertTrue(verdict["selected"])

    def test_both_successful_is_a_cost_fallback(self) -> None:
        verdict = self._verdict(True, True)
        self.assertEqual("fallback-cost", verdict["rule"])

    def test_both_unsuccessful_is_rejected(self) -> None:
        verdict = self._verdict(False, False)
        self.assertFalse(verdict["selected"])
        self.assertEqual("both-unsuccessful", verdict["reason"])

    def test_missing_action_is_rejected(self) -> None:
        verdict = evaluate_selection_rule(
            {"direct-video": self._outcomes(True, False)["direct-video"]},
            primary_action_id="direct-video",
            cheap_action_id="remote-derived",
        )
        self.assertFalse(verdict["selected"])


class ChecksumTest(unittest.TestCase):
    def test_manifest_uses_lf_so_strict_sha256sum_can_verify_it(self) -> None:
        """A manifest frozen on Windows must verify on Linux byte for byte."""

        from pathfinder.simulator.full_flow_visible_screening import (
            CHECKSUMS_NAME,
            _write_package,
        )

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "pkg"
            _write_package(target, {"a.json": b'{"x":1}', "b.json": b'{"y":2}'})
            raw = (target / CHECKSUMS_NAME).read_bytes()
            self.assertNotIn(b"\r", raw)
            self.assertTrue(raw.endswith(b"\n"))
            # Every listed name must be usable verbatim as a path.
            for line in raw.decode("utf-8").splitlines():
                _digest, _, name = line.partition("  ")
                self.assertTrue((target / name).is_file())
            verify_checksums(target)

    def test_tampered_package_fails_verification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.json").write_bytes(b'{"x":1}')
            import hashlib

            digest = hashlib.sha256(b'{"x":1}').hexdigest()
            (root / "SHA256SUMS").write_text(
                f"{digest}  a.json\n", encoding="utf-8"
            )
            verify_checksums(root)
            (root / "a.json").write_bytes(b'{"x":2}')
            with self.assertRaisesRegex(VisibleScreeningError, "checksum"):
                verify_checksums(root)


class ClaimBoundaryTest(unittest.TestCase):
    def test_selection_kind_is_never_a_formal_sample(self) -> None:
        self.assertEqual("visible-development-demo-screening", SELECTION_KIND)


if __name__ == "__main__":
    unittest.main()
