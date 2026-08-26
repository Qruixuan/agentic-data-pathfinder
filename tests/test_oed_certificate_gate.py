"""OED consuming v3alpha5 three-state safety certificates."""

from __future__ import annotations

import contextlib
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pathfinder.awm import AWMConfigError
from pathfinder.cli import main as cli_main
from pathfinder.oed import (
    UNREVEALED_STATE,
    CertificateGateError,
    HiddenOracleView,
    run_oed_certificate_replay,
)
from pathfinder.awm.certificate import (
    load_workload_safety_certificate_config,
)
from pathfinder.reduced_oracle import load_reduced_oracle_config

from tests.test_awm_certificate import (
    DIGEST,
    FRAMES,
    PAIR,
    SAFE,
    _Cell,
    _Fixture,
    _plan,
    _workload_ids,
    _write_json,
)


def _oed_config(
    root: Path,
    *,
    name: str = "oed.json",
    exploration_budget: float = 0.5,
    per_excursion_cap: float = 0.2,
    max_iterations: int = 6,
    commit_margin: float = 0.0,
    probe_window_loss: dict[str, float] | None = None,
    tiers: dict[str, str] | None = None,
) -> Path:
    losses = probe_window_loss or {FRAMES: 0.02, DIGEST: 0.02, PAIR: 0.04}
    order = tiers or {
        DIGEST: "simultaneous-canonical",
        FRAMES: "pair-canonical",
        PAIR: "fallback",
    }
    path = root / name
    _write_json(path, {
        "schema_version": "pathfinder.oed/v1alpha1",
        "controller_id": "certificate-gate-test",
        "commit_margin": commit_margin,
        "reveal_margin": 0,
        "exploration_budget": exploration_budget,
        "per_excursion_cap": per_excursion_cap,
        "max_iterations": max_iterations,
        "random_seed": 7,
        "cost_model_status": "synthetic-unit-test",
        "reveal_candidates": [
            {
                "design_id": design_id,
                "reveal_tier": order[design_id],
                "probe_window_loss": losses[design_id],
            }
            for design_id in (DIGEST, FRAMES, PAIR)
        ],
        "other_design_ids": [],
    })
    return path


def _single_stratum_fixture(
    root: Path,
    *,
    origin: _Cell,
    candidate: _Cell,
    workloads: int = 16,
) -> _Fixture:
    return _Fixture(
        root,
        strata={"causal": _workload_ids("causal", workloads)},
        plan=_plan(
            ("causal",),
            origin=origin,
            candidate=candidate,
        ),
    )


def _two_stratum_fixture(root: Path, **kwargs: Any) -> _Fixture:
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
        **kwargs,
    )


def _replay(
    fixture: _Fixture,
    oed: Path,
    *,
    certificate: Path | None = None,
    output_name: str = "oed-output",
    policies: tuple[str, ...] = (
        "certificate_full_oed",
        "safe_origin_baseline",
    ),
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    output = fixture.root / output_name
    manifest = run_oed_certificate_replay(
        oed,
        certificate or fixture.certificate_config(),
        fixture.oracle_path,
        oracle_output_dir=fixture.oracle_output,
        output_dir=output,
        policies=policies,
    )
    evaluation = json.loads(
        (output / "oed_certificate_evaluation.json").read_text(
            encoding="utf-8"
        )
    )
    with (output / "oed_certificate_trace.csv").open(
        encoding="utf-8",
        newline="",
    ) as handle:
        rows = list(csv.DictReader(handle))
    return manifest, evaluation, rows


def _policy(evaluation: dict[str, Any], kind: str) -> dict[str, Any]:
    return next(
        policy
        for policy in evaluation["policies"]
        if policy["policy_kind"] == kind
    )


class HiddenOracleViewTest(unittest.TestCase):
    def test_reading_an_unrevealed_design_raises(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _single_stratum_fixture(
                Path(temporary),
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
                workloads=4,
            )
            view = HiddenOracleView(
                load_workload_safety_certificate_config(
                    fixture.certificate_config()
                ),
                load_reduced_oracle_config(fixture.oracle_path),
                oracle_output_dir=fixture.oracle_output,
            )
            with self.assertRaisesRegex(
                CertificateGateError,
                "target-outcome leakage",
            ):
                view.inputs((SAFE, FRAMES))
            self.assertEqual((), view.read_history)

    def test_the_safe_design_is_always_visible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _single_stratum_fixture(
                Path(temporary),
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
                workloads=4,
            )
            view = HiddenOracleView(
                load_workload_safety_certificate_config(
                    fixture.certificate_config()
                ),
                load_reduced_oracle_config(fixture.oracle_path),
                oracle_output_dir=fixture.oracle_output,
            )
            inputs = view.inputs((SAFE,))
            self.assertEqual((SAFE,), inputs.loaded_design_ids)
            view.reveal(FRAMES)
            self.assertEqual((FRAMES,), view.revealed_design_ids)
            self.assertEqual(
                (SAFE, FRAMES),
                view.inputs((SAFE, FRAMES)).loaded_design_ids,
            )

    def test_the_safe_design_cannot_be_revealed_or_double_revealed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = _single_stratum_fixture(
                Path(temporary),
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
                workloads=4,
            )
            view = HiddenOracleView(
                load_workload_safety_certificate_config(
                    fixture.certificate_config()
                ),
                load_reduced_oracle_config(fixture.oracle_path),
                oracle_output_dir=fixture.oracle_output,
            )
            with self.assertRaisesRegex(
                CertificateGateError,
                "already deployed",
            ):
                view.reveal(SAFE)
            view.reveal(FRAMES)
            with self.assertRaisesRegex(
                CertificateGateError,
                "already revealed",
            ):
                view.reveal(FRAMES)


class LeakageRegressionTest(unittest.TestCase):
    """A hidden outcome must not influence any pre-Reveal decision."""

    def _perturb(self, fixture: _Fixture, design_id: str) -> None:
        records = fixture.read_runs(fixture.oracle_output, design_id)
        for record in records:
            record["task_success"] = not record["task_success"]
            record["access_events"][0]["realized_cost"] = 0.95
        fixture.write_runs(fixture.oracle_output, design_id, records)

    def _trace(
        self,
        root: Path,
        *,
        perturb: str | None,
    ) -> list[dict[str, str]]:
        fixture = _two_stratum_fixture(root)
        if perturb is not None:
            self._perturb(fixture, perturb)
        _, _, rows = _replay(
            fixture,
            _oed_config(root),
            certificate=fixture.certificate_config(candidates={
                "causal": FRAMES,
                "temporal": DIGEST,
            }),
            policies=("certificate_full_oed",),
        )
        return rows

    def test_a_never_revealed_design_cannot_change_the_trace(self) -> None:
        traces = []
        for perturb in (None, PAIR):
            with tempfile.TemporaryDirectory() as temporary:
                traces.append(self._trace(Path(temporary), perturb=perturb))
        self.assertEqual(traces[0], traces[1])
        self.assertNotIn(
            PAIR,
            {row["selected_design_id"] for row in traces[0]},
        )

    def test_a_later_revealed_outcome_cannot_change_earlier_decisions(
        self,
    ) -> None:
        traces = []
        for perturb in (None, FRAMES):
            with tempfile.TemporaryDirectory() as temporary:
                traces.append(self._trace(Path(temporary), perturb=perturb))
        baseline, perturbed = traces
        reveal_iteration = min(
            int(row["iteration"])
            for row in baseline
            if row["selected_action"] == "REVEAL"
            and row["selected_design_id"] == FRAMES
        )
        self.assertGreater(reveal_iteration, 0)

        def before(rows: list[dict[str, str]]) -> list[dict[str, str]]:
            return [
                row
                for row in rows
                if int(row["iteration"]) <= reveal_iteration
            ]

        self.assertEqual(
            before(baseline),
            before(perturbed),
            "decisions up to and including the REVEAL must not depend on "
            "the outcome being revealed",
        )
        self.assertNotEqual(baseline, perturbed)

    def test_the_read_history_never_precedes_a_reveal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _two_stratum_fixture(root)
            _, evaluation, rows = _replay(
                fixture,
                _oed_config(root),
                certificate=fixture.certificate_config(candidates={
                    "causal": FRAMES,
                    "temporal": DIGEST,
                }),
                policies=("certificate_full_oed",),
            )
            policy = _policy(evaluation, "certificate_full_oed")
            reveal_order = policy["reveal_order"]
            self.assertTrue(reveal_order)
            self.assertTrue(policy["design_read_history"])
            # The k-th read happens at the start of iteration k, by which
            # point at most the first k reveals have been paid for.
            for index, read in enumerate(
                policy["design_read_history"],
                start=1,
            ):
                permitted = {SAFE, *reveal_order[:index]}
                self.assertLessEqual(
                    set(read.split("+")),
                    permitted,
                    f"read {index} ({read}) exceeded the paid-for reveals "
                    f"{sorted(permitted)}",
                )
            self.assertEqual(
                {row["selected_design_id"]
                 for row in rows
                 if row["selected_action"] == "REVEAL"
                 and row["policy_kind"] == "certificate_full_oed"},
                set(reveal_order),
            )

    def test_an_unrevealed_stratum_reports_no_estimates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _two_stratum_fixture(root)
            _, _, rows = _replay(
                fixture,
                _oed_config(root),
                certificate=fixture.certificate_config(candidates={
                    "causal": FRAMES,
                    "temporal": DIGEST,
                }),
                policies=("certificate_full_oed",),
            )
            unrevealed = [
                row
                for row in rows
                if row["certificate_state"] == UNREVEALED_STATE
            ]
            self.assertTrue(unrevealed)
            for row in unrevealed:
                self.assertEqual(
                    "",
                    row["success_difference_point_estimate"],
                )
                self.assertEqual("", row["cost_saving_lower_bound"])
                self.assertEqual("", row["utility_gain_point_estimate"])
                self.assertEqual(SAFE, row["applied_design_id"])
                self.assertEqual(
                    "candidate_not_yet_revealed",
                    row["fallback_reason"],
                )


class CertificateSemanticsTest(unittest.TestCase):
    def test_safe_to_commit_may_commit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            )
            manifest, evaluation, rows = _replay(fixture, _oed_config(root))
            policy = _policy(evaluation, "certificate_full_oed")
            self.assertEqual("commit_completed", policy["terminal_reason"])
            self.assertEqual(1, policy["commit_count"])
            self.assertEqual(1, policy["reveal_count"])
            self.assertEqual([FRAMES], policy["reveal_order"])
            self.assertEqual(FRAMES, policy["final_design_id"])
            self.assertFalse(policy["fallback_applied"])
            self.assertEqual(0, policy["false_safe_commit_count"])
            self.assertEqual(0, manifest["false_safe_commit_count"])
            commits = [
                row for row in rows if row["selected_action"] == "COMMIT"
            ]
            self.assertTrue(commits)
            self.assertEqual(
                "certificate_safe_to_commit_and_clears_value_margin",
                commits[0]["action_reason"],
            )

    def test_unsafe_is_never_committed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=4, cost=1.0),
                candidate=_Cell(successes=0, cost=0.2),
            )
            manifest, evaluation, rows = _replay(fixture, _oed_config(root))
            policy = _policy(evaluation, "certificate_full_oed")
            self.assertEqual(
                {"causal": "UNSAFE"},
                policy["final_state_by_stratum"],
            )
            self.assertEqual(0, policy["commit_count"])
            self.assertEqual(
                "certificate_limited_stop",
                policy["terminal_reason"],
            )
            self.assertEqual(SAFE, policy["final_design_id"])
            self.assertTrue(policy["fallback_applied"])
            self.assertEqual(0, manifest["false_safe_commit_count"])
            self.assertNotIn(
                "COMMIT",
                {row["selected_action"] for row in rows},
            )

    def test_insufficient_evidence_reveals_then_stops(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=2, cost=1.0),
                candidate=_Cell(successes=2, cost=0.9),
                workloads=4,
            )
            _, evaluation, rows = _replay(fixture, _oed_config(root))
            policy = _policy(evaluation, "certificate_full_oed")
            self.assertEqual(
                {"causal": "INSUFFICIENT_EVIDENCE"},
                policy["final_state_by_stratum"],
            )
            self.assertEqual(1, policy["reveal_count"])
            self.assertEqual(0, policy["commit_count"])
            self.assertEqual(
                "certificate_limited_stop",
                policy["terminal_reason"],
            )
            self.assertEqual(SAFE, policy["final_design_id"])
            stop = [
                row
                for row in rows
                if row["selected_action"] == "STOP"
                and row["policy_kind"] == "certificate_full_oed"
            ]
            self.assertIn(
                "no_safe_commit_and_no_further_decision_relevant"
                "_observation",
                stop[-1]["action_reason"],
            )

    def test_a_revealed_candidate_is_not_revealed_twice(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=2, cost=1.0),
                candidate=_Cell(successes=2, cost=0.9),
                workloads=4,
            )
            _, evaluation, _ = _replay(fixture, _oed_config(root))
            policy = _policy(evaluation, "certificate_full_oed")
            self.assertEqual(
                len(policy["reveal_order"]),
                len(set(policy["reveal_order"])),
            )

    def test_an_unaffordable_reveal_stops_on_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            )
            _, evaluation, _ = _replay(
                fixture,
                _oed_config(root, per_excursion_cap=0.001),
            )
            policy = _policy(evaluation, "certificate_full_oed")
            self.assertEqual(0, policy["reveal_count"])
            self.assertEqual(0, policy["commit_count"])
            self.assertEqual(
                "budget_limited_stop",
                policy["terminal_reason"],
            )
            self.assertEqual(SAFE, policy["final_design_id"])

    def test_an_exhausted_purse_stops_on_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _two_stratum_fixture(root)
            _, evaluation, _ = _replay(
                fixture,
                _oed_config(root, exploration_budget=0.03),
                certificate=fixture.certificate_config(candidates={
                    "causal": FRAMES,
                    "temporal": DIGEST,
                }),
            )
            policy = _policy(evaluation, "certificate_full_oed")
            self.assertEqual(1, policy["reveal_count"])
            self.assertEqual(
                "budget_limited_stop",
                policy["terminal_reason"],
            )
            self.assertEqual(SAFE, policy["final_design_id"])

    def test_a_high_commit_margin_blocks_a_safe_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            )
            _, evaluation, _ = _replay(
                fixture,
                _oed_config(root, commit_margin=1000.0),
            )
            policy = _policy(evaluation, "certificate_full_oed")
            self.assertEqual(
                {"causal": "SAFE_TO_COMMIT"},
                policy["final_state_by_stratum"],
            )
            self.assertEqual(0, policy["commit_count"])
            self.assertEqual(SAFE, policy["final_design_id"])
            self.assertEqual(
                "certificate_limited_stop",
                policy["terminal_reason"],
            )

    def test_every_non_commit_path_retains_the_origin(self) -> None:
        cases = (
            ("unsafe", _Cell(successes=4, cost=1.0), _Cell(0, 0.2), 16),
            ("thin", _Cell(successes=2, cost=1.0), _Cell(2, 0.9), 4),
        )
        for label, origin, candidate, workloads in cases:
            with self.subTest(case=label):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    fixture = _single_stratum_fixture(
                        root,
                        origin=origin,
                        candidate=candidate,
                        workloads=workloads,
                    )
                    _, evaluation, rows = _replay(
                        fixture,
                        _oed_config(root),
                    )
                    policy = _policy(evaluation, "certificate_full_oed")
                    self.assertEqual(0, policy["commit_count"])
                    self.assertEqual(SAFE, policy["final_design_id"])
                    self.assertEqual(SAFE, policy["fallback_design_id"])
                    self.assertTrue(policy["fallback_applied"])
                    for row in rows:
                        if row["policy_kind"] != "certificate_full_oed":
                            continue
                        if row["certificate_state"] != "SAFE_TO_COMMIT":
                            self.assertEqual(
                                SAFE,
                                row["applied_design_id"],
                            )

    def test_reveal_order_follows_the_declared_tiers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _two_stratum_fixture(root)
            certificate = fixture.certificate_config(candidates={
                "causal": FRAMES,
                "temporal": DIGEST,
            })
            _, first, _ = _replay(
                fixture,
                _oed_config(root, name="a.json"),
                certificate=certificate,
                output_name="output-a",
                policies=("certificate_full_oed",),
            )
            _, second, _ = _replay(
                fixture,
                _oed_config(
                    root,
                    name="b.json",
                    tiers={
                        DIGEST: "fallback",
                        FRAMES: "simultaneous-canonical",
                        PAIR: "fallback",
                    },
                ),
                certificate=certificate,
                output_name="output-b",
                policies=("certificate_full_oed",),
            )
        self.assertEqual(
            DIGEST,
            _policy(first, "certificate_full_oed")["reveal_order"][0],
        )
        self.assertEqual(
            FRAMES,
            _policy(second, "certificate_full_oed")["reveal_order"][0],
        )


class BaselineComparisonTest(unittest.TestCase):
    def test_the_safe_origin_baseline_never_acts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            )
            _, evaluation, _ = _replay(fixture, _oed_config(root))
            baseline = _policy(evaluation, "safe_origin_baseline")
            self.assertEqual(0, baseline["reveal_count"])
            self.assertEqual(0, baseline["commit_count"])
            self.assertEqual(0.0, baseline["total_reveal_cost"])
            self.assertEqual(SAFE, baseline["final_design_id"])
            self.assertEqual(
                "policy_limited_stop",
                baseline["terminal_reason"],
            )
            self.assertEqual([], baseline["design_read_history"])
            self.assertEqual(
                1,
                baseline["unrevealed_stratum_count"]
                + baseline["safe_to_commit_stratum_count"]
                + baseline["insufficient_evidence_stratum_count"]
                + baseline["unsafe_stratum_count"],
            )

    def test_the_baseline_reads_no_candidate_records(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            )
            _, evaluation, _ = _replay(
                fixture,
                _oed_config(root),
                policies=("safe_origin_baseline",),
            )
            baseline = _policy(evaluation, "safe_origin_baseline")
            self.assertEqual([], baseline["design_read_history"])
            self.assertEqual(
                [UNREVEALED_STATE],
                sorted(set(baseline["final_state_by_stratum"].values())),
            )


class CertificateOEDOutputTest(unittest.TestCase):
    def test_outputs_are_reproducible(self) -> None:
        payloads = []
        hashes = []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            )
            oed = _oed_config(root)
            certificate = fixture.certificate_config()
            for index in range(2):
                manifest, _, _ = _replay(
                    fixture,
                    oed,
                    certificate=certificate,
                    output_name=f"output-{index}",
                )
                hashes.append(manifest["replay_snapshot_sha256"])
                output = root / f"output-{index}"
                payloads.append(tuple(
                    (output / name).read_text(encoding="utf-8")
                    for name in (
                        "oed_certificate_trace.csv",
                        "oed_certificate_summary.csv",
                        "oed_certificate_evaluation.json",
                    )
                ))
        self.assertEqual(payloads[0], payloads[1])
        self.assertEqual(hashes[0], hashes[1])

    def test_the_manifest_records_provenance_and_flags(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
                workloads=4,
            )
            manifest, evaluation, _ = _replay(fixture, _oed_config(root))
            self.assertTrue(manifest["posthoc"])
            self.assertFalse(manifest["eligible_for_scientific_claims"])
            self.assertFalse(manifest["deployment_mutations_performed"])
            self.assertFalse(manifest["secrets_recorded"])
            self.assertEqual(SAFE, manifest["fallback_design_id"])
            for key in (
                "oed_config_sha256",
                "certificate_config_sha256",
                "oracle_config_sha256",
                "oracle_full_snapshot_sha256",
            ):
                self.assertTrue(manifest[key])
            self.assertEqual(
                "full-declared-design-set",
                manifest["oracle_full_snapshot_scope"],
            )
            semantics = evaluation["decision_semantics"]
            self.assertIn("never committed", semantics["UNSAFE"])
            self.assertIn("REVEAL only", semantics["INSUFFICIENT_EVIDENCE"])
            self.assertIn(
                "certificate_limited_stop",
                semantics["stop"],
            )

    def test_a_nonempty_output_directory_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
                workloads=4,
            )
            occupied = root / "oed-output"
            occupied.mkdir(parents=True)
            (occupied / "existing.txt").write_text("x", encoding="utf-8")
            with self.assertRaisesRegex(AWMConfigError, "is not empty"):
                _replay(fixture, _oed_config(root))

    def test_an_unknown_policy_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
                workloads=4,
            )
            with self.assertRaisesRegex(
                CertificateGateError,
                "unknown certificate OED policy",
            ):
                _replay(
                    fixture,
                    _oed_config(root),
                    policies=("nonexistent_policy",),
                )

    def test_cli_runs_the_certificate_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _single_stratum_fixture(
                root,
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            )
            output = root / "cli-output"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = cli_main([
                    "run-oed-certificate-replay",
                    "--oed-config",
                    str(_oed_config(root)),
                    "--certificate-config",
                    str(fixture.certificate_config()),
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
            self.assertEqual(0, payload["false_safe_commit_count"])
            for name in (
                "oed_certificate_trace.csv",
                "oed_certificate_summary.csv",
                "oed_certificate_evaluation.json",
                "oed_certificate_manifest.json",
            ):
                self.assertTrue((output / name).is_file(), name)


if __name__ == "__main__":
    unittest.main()
