from __future__ import annotations

import contextlib
import csv
import io
import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path

from pathfinder.awm import evaluate_awm
from pathfinder.cli import main as cli_main
from pathfinder.oed import run_oed_replay
from pathfinder.reduced_oracle import analyze_reduced_oracle
from pathfinder.synthetic_marker import (
    SyntheticFixtureRefusal,
    assert_not_synthetic_fixture,
    synthetic_fixture_evidence,
)
from pathfinder.synthetic_oracle import (
    MINIMUM_FIXTURE_DESIGNS,
    SyntheticFixtureConfigError,
    generate_synthetic_oracle_fixture,
    load_synthetic_fixture_config,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIGS = ROOT / "configs"
FIXTURE_CONFIG = CONFIGS / "synthetic_oracle_fixture.json"
SYNTHETIC_ORACLE_CONFIG = CONFIGS / "synthetic_multi_candidate_oracle.json"
SYNTHETIC_AWM_CONFIG = CONFIGS / "synthetic_multi_candidate_awm.json"
SYNTHETIC_OED_CONFIG = CONFIGS / "synthetic_multi_candidate_oed.json"

REAL_ORACLE_CONFIG = CONFIGS / "reduced_oracle_mvp.json"
REAL_AWM_CONFIG = CONFIGS / "awm_reduced_mvp.json"
REAL_OED_CONFIG = CONFIGS / "oed_reduced_mvp.json"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _mutated_fixture(root: Path, **changes: object) -> Path:
    payload = json.loads(FIXTURE_CONFIG.read_text(encoding="utf-8"))
    payload["oracle_config"] = str(SYNTHETIC_ORACLE_CONFIG)
    payload.update(changes)
    path = root / "fixture.json"
    _write_json(path, payload)
    return path


class SyntheticFixtureContractTest(unittest.TestCase):
    def test_committed_fixture_declares_a_multi_candidate_domain(
        self,
    ) -> None:
        config = load_synthetic_fixture_config(FIXTURE_CONFIG)
        self.assertTrue(config.synthetic)
        self.assertFalse(config.eligible_for_scientific_claims)
        self.assertGreaterEqual(
            len(config.design_ids),
            MINIMUM_FIXTURE_DESIGNS,
        )
        self.assertEqual(5, len(config.design_ids))

    def test_a_fixture_cannot_declare_itself_real(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(
                SyntheticFixtureConfigError,
                "must declare synthetic=true",
            ):
                load_synthetic_fixture_config(
                    _mutated_fixture(root, synthetic=False)
                )
            with self.assertRaisesRegex(
                SyntheticFixtureConfigError,
                "eligible_for_scientific_claims=false",
            ):
                load_synthetic_fixture_config(
                    _mutated_fixture(
                        root,
                        eligible_for_scientific_claims=True,
                    )
                )

    def test_selection_probabilities_must_sum_to_one(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            payload = json.loads(FIXTURE_CONFIG.read_text(encoding="utf-8"))
            payload["designs"][0]["representation_probabilities"][
                "sampled_frames"
            ] = 0.9
            with self.assertRaisesRegex(
                SyntheticFixtureConfigError,
                "must sum to 1",
            ):
                load_synthetic_fixture_config(
                    _mutated_fixture(
                        Path(temporary),
                        designs=payload["designs"],
                    )
                )

    def test_a_degenerate_domain_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            payload = json.loads(FIXTURE_CONFIG.read_text(encoding="utf-8"))
            designs = payload["designs"][:2]
            with self.assertRaisesRegex(
                SyntheticFixtureConfigError,
                "at least 4 designs",
            ):
                load_synthetic_fixture_config(
                    _mutated_fixture(
                        Path(temporary),
                        designs=designs,
                        design_order=[
                            design["design_id"] for design in designs
                        ],
                    )
                )

    def test_committed_real_configs_are_untouched_by_the_fixture(
        self,
    ) -> None:
        """The fixture must never repoint the real Oracle/AWM/OED contracts."""
        oracle = json.loads(REAL_ORACLE_CONFIG.read_text(encoding="utf-8"))
        awm = json.loads(REAL_AWM_CONFIG.read_text(encoding="utf-8"))
        oed = json.loads(REAL_OED_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual("reduced-oracle-mvp-v1", oracle["oracle_id"])
        self.assertEqual(
            ["D_remote_digest", "D_local_digest"],
            oracle["design_order"],
        )
        self.assertEqual("awm-reduced-mvp-v1", awm["model_id"])
        self.assertEqual(["D_remote_digest"], awm["observed_design_ids"])
        self.assertEqual("oed-reduced-mvp-v1", oed["controller_id"])
        self.assertEqual(1, len(oed["reveal_candidates"]))
        for payload in (oracle, awm, oed):
            self.assertNotIn("synthetic", payload)


class SyntheticFixtureGenerationTest(unittest.TestCase):
    def generate(self, root: Path) -> Path:
        output = root / "oracle"
        generate_synthetic_oracle_fixture(
            FIXTURE_CONFIG,
            output_dir=output,
        )
        return output

    def test_generated_layout_matches_the_oracle_output_contract(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = self.generate(Path(temporary))
            self.assertTrue((output / "oracle_table.csv").is_file())
            self.assertTrue((output / "synthetic_truth.json").is_file())
            self.assertTrue(
                (output / "synthetic_oracle_manifest.json").is_file()
            )
            manifest = json.loads(
                (output / "synthetic_oracle_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(5, manifest["design_count"])
            for design_id in manifest["design_order"]:
                self.assertTrue(
                    (
                        output / "designs" / design_id / "runs.jsonl"
                    ).is_file()
                )

    def test_every_record_and_the_manifest_are_labelled_synthetic(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = self.generate(Path(temporary))
            manifest = json.loads(
                (output / "synthetic_oracle_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            truth = json.loads(
                (output / "synthetic_truth.json").read_text(encoding="utf-8")
            )
            for payload in (manifest, truth):
                self.assertIs(True, payload["synthetic"])
                self.assertIs(
                    False,
                    payload["eligible_for_scientific_claims"],
                )
            self.assertIn("engineering fixture", manifest["statement"])
            self.assertIn(
                "NOT physical Reduced Oracle evidence",
                manifest["statement"],
            )
            self.assertEqual("engineering-fixture", manifest["fixture_kind"])

            record_count = 0
            for design_id in manifest["design_order"]:
                path = output / "designs" / design_id / "runs.jsonl"
                for line in path.read_text(encoding="utf-8").splitlines():
                    record = json.loads(line)
                    record_count += 1
                    self.assertIs(True, record["synthetic"])
                    self.assertIs(
                        False,
                        record["eligible_for_scientific_claims"],
                    )
                    self.assertIs(False, record["measured"])
                    for event in record["access_events"]:
                        self.assertIs(True, event["synthetic"])
                        self.assertIs(
                            False,
                            event["eligible_for_scientific_claims"],
                        )
            self.assertEqual(800, record_count)

            with (output / "oracle_table.csv").open(
                encoding="utf-8",
                newline="",
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(5, len(rows))
            for row in rows:
                self.assertEqual("True", row["synthetic"])
                self.assertEqual(
                    "False",
                    row["eligible_for_scientific_claims"],
                )

    def test_generation_is_byte_reproducible_from_the_fixed_seed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            second = root / "second"
            generate_synthetic_oracle_fixture(
                FIXTURE_CONFIG,
                output_dir=first,
            )
            generate_synthetic_oracle_fixture(
                FIXTURE_CONFIG,
                output_dir=second,
            )
            names = sorted(
                path.relative_to(first)
                for path in first.rglob("*")
                if path.is_file()
            )
            self.assertTrue(names)
            for name in names:
                self.assertEqual(
                    (first / name).read_bytes(),
                    (second / name).read_bytes(),
                    f"{name} is not reproducible",
                )

    def test_a_non_empty_output_directory_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "oracle"
            output.mkdir(parents=True)
            (output / "runs.jsonl").write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(
                SyntheticFixtureConfigError,
                "not empty",
            ):
                generate_synthetic_oracle_fixture(
                    FIXTURE_CONFIG,
                    output_dir=output,
                )
            # The pre-existing file is left exactly as it was.
            self.assertEqual(
                "{}\n",
                (output / "runs.jsonl").read_text(encoding="utf-8"),
            )

    def test_no_credential_or_machine_path_reaches_the_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = self.generate(Path(temporary))
            manifest = (
                output / "synthetic_oracle_manifest.json"
            ).read_text(encoding="utf-8")
            self.assertNotIn(str(ROOT), manifest)
            self.assertNotIn("/home/", manifest)
            self.assertNotIn("http://", manifest)
            self.assertNotIn("https://", manifest)
            payload = json.loads(manifest)
            self.assertEqual(
                "synthetic_multi_candidate_oracle.json",
                payload["oracle_config_name"],
            )
            self.assertFalse(payload["secrets_recorded"])
            self.assertFalse(payload["flowmesh_contacted"])
            self.assertFalse(payload["costs_measured"])


class SyntheticFixtureCliTest(unittest.TestCase):
    def test_cli_generates_the_fixture_and_refuses_to_overwrite_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "oracle"
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = cli_main(
                    [
                        "generate-synthetic-oracle",
                        "--fixture-config",
                        str(FIXTURE_CONFIG),
                        "--output-dir",
                        str(output),
                        "--compact",
                    ]
                )
            self.assertEqual(0, code)
            payload = json.loads(buffer.getvalue())
            self.assertIs(True, payload["synthetic"])
            self.assertIs(False, payload["eligible_for_scientific_claims"])
            self.assertEqual("COMPLETE", payload["status"])

            repeat = io.StringIO()
            with contextlib.redirect_stdout(repeat):
                code = cli_main(
                    [
                        "generate-synthetic-oracle",
                        "--fixture-config",
                        str(FIXTURE_CONFIG),
                        "--output-dir",
                        str(output),
                        "--compact",
                    ]
                )
            self.assertEqual(2, code)
            self.assertEqual(
                "error",
                json.loads(repeat.getvalue())["status"],
            )


class SyntheticFixtureProtectionTest(unittest.TestCase):
    """A real Oracle command must never rewrite a generated fixture."""

    def generate(self, root: Path) -> Path:
        output = root / "oracle"
        generate_synthetic_oracle_fixture(FIXTURE_CONFIG, output_dir=output)
        return output

    def test_analysis_refuses_and_leaves_oracle_table_byte_identical(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = self.generate(Path(temporary))
            table = output / "oracle_table.csv"
            before = sha256(table.read_bytes()).hexdigest()
            records = output / "designs" / "S_local_digest" / "runs.jsonl"
            records_before = sha256(records.read_bytes()).hexdigest()

            with self.assertRaises(SyntheticFixtureRefusal) as context:
                analyze_reduced_oracle(
                    SYNTHETIC_ORACLE_CONFIG,
                    output_dir=output,
                )

            message = str(context.exception)
            self.assertIn("analyze-reduced-oracle", message)
            self.assertIn("synthetic Oracle fixture", message)
            self.assertIn("Nothing was written", message)
            self.assertIn("evaluate-awm", message)
            self.assertIn("run-oed-replay", message)

            self.assertEqual(before, sha256(table.read_bytes()).hexdigest())
            self.assertEqual(
                records_before,
                sha256(records.read_bytes()).hexdigest(),
            )
            # The analysis outputs were never created either.
            self.assertFalse((output / "oracle_summary.json").exists())
            self.assertFalse((output / "lock_in_trace.json").exists())

    def test_cli_analysis_refuses_with_a_clear_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = self.generate(Path(temporary))
            before = sha256((output / "oracle_table.csv").read_bytes())
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = cli_main(
                    [
                        "analyze-reduced-oracle",
                        "--oracle-config",
                        str(SYNTHETIC_ORACLE_CONFIG),
                        "--output-dir",
                        str(output),
                    ]
                )
            self.assertEqual(2, code)
            payload = json.loads(buffer.getvalue())
            self.assertEqual("error", payload["status"])
            self.assertIn("synthetic Oracle fixture", payload["message"])
            self.assertEqual(
                before.hexdigest(),
                sha256((output / "oracle_table.csv").read_bytes()).hexdigest(),
            )

    def test_oracle_runner_refuses_before_creating_the_directory(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = self.generate(Path(temporary))
            names_before = sorted(
                path.name for path in output.iterdir()
            )
            with self.assertRaises(SyntheticFixtureRefusal) as context:
                assert_not_synthetic_fixture(
                    output,
                    command="run-reduced-oracle",
                )
            self.assertIn("run-reduced-oracle", str(context.exception))
            self.assertEqual(
                names_before,
                sorted(path.name for path in output.iterdir()),
            )

    def test_each_marker_alone_is_enough_to_refuse(self) -> None:
        markers = {
            "synthetic_oracle_manifest.json": '{"synthetic": true}\n',
            "synthetic_truth.json": '{"synthetic": true}\n',
        }
        for name, content in markers.items():
            with self.subTest(marker=name), tempfile.TemporaryDirectory() as (
                temporary
            ):
                root = Path(temporary)
                (root / name).write_text(content, encoding="utf-8")
                self.assertTrue(synthetic_fixture_evidence(root))
                with self.assertRaises(SyntheticFixtureRefusal):
                    assert_not_synthetic_fixture(root, command="c")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "oracle_table.csv").write_text(
                "design_id,synthetic\nD,True\n",
                encoding="utf-8",
            )
            with self.assertRaises(SyntheticFixtureRefusal):
                assert_not_synthetic_fixture(root, command="c")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            records = root / "designs" / "D" / "runs.jsonl"
            records.parent.mkdir(parents=True)
            records.write_text(
                json.dumps({"trial_key": "a", "synthetic": True}) + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(SyntheticFixtureRefusal):
                assert_not_synthetic_fixture(root, command="c")

    def test_a_real_oracle_directory_is_not_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "oracle_table.csv").write_text(
                "design_id,storage_cost\nD_remote_digest,0\n",
                encoding="utf-8",
            )
            records = root / "designs" / "D_remote_digest" / "runs.jsonl"
            records.parent.mkdir(parents=True)
            records.write_text(
                json.dumps({"trial_key": "a", "outcome_type": "completed"})
                + "\n",
                encoding="utf-8",
            )
            self.assertEqual([], synthetic_fixture_evidence(root))
            assert_not_synthetic_fixture(root, command="analyze-reduced-oracle")

    def test_an_empty_or_missing_directory_is_not_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual([], synthetic_fixture_evidence(root))
            self.assertEqual(
                [],
                synthetic_fixture_evidence(root / "does-not-exist"),
            )

    def test_a_malformed_marker_file_does_not_crash_the_guard(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            # Still refused: presence of the manifest is decided by filename,
            # so corrupting its contents cannot be used to slip past.
            (root / "synthetic_oracle_manifest.json").write_text(
                "not json at all",
                encoding="utf-8",
            )
            (root / "oracle_table.csv").write_text("\x00\x00", encoding="utf-8")
            reasons = synthetic_fixture_evidence(root)
            self.assertIn(
                "synthetic_oracle_manifest.json is present",
                reasons,
            )

    def test_the_fixture_is_still_consumable_by_awm_and_oed(self) -> None:
        """The guard must not block the two supported consumers."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = self.generate(root)
            awm = evaluate_awm(
                SYNTHETIC_AWM_CONFIG,
                SYNTHETIC_ORACLE_CONFIG,
                oracle_output_dir=output,
                output_dir=root / "awm",
            )
            oed = run_oed_replay(
                SYNTHETIC_OED_CONFIG,
                SYNTHETIC_AWM_CONFIG,
                SYNTHETIC_ORACLE_CONFIG,
                oracle_output_dir=output,
                output_dir=root / "oed",
            )
            self.assertEqual("COMPLETE", awm["status"])
            self.assertEqual("COMPLETE", oed["status"])


class SyntheticFixturePipelineTest(unittest.TestCase):
    """The point of the fixture: AWM and OED run over it unchanged."""

    def test_generate_then_evaluate_awm_then_run_oed_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle_output = root / "oracle"
            generate_synthetic_oracle_fixture(
                FIXTURE_CONFIG,
                output_dir=oracle_output,
            )

            awm_output = root / "awm"
            awm_manifest = evaluate_awm(
                SYNTHETIC_AWM_CONFIG,
                SYNTHETIC_ORACLE_CONFIG,
                oracle_output_dir=oracle_output,
                output_dir=awm_output,
            )
            self.assertEqual("COMPLETE", awm_manifest["status"])
            self.assertEqual(5, len(awm_manifest["design_ids"]))
            self.assertEqual(
                144,
                awm_manifest["training_sessions"]["S_remote_baseline"],
            )
            self.assertTrue((awm_output / "awm_bounds.csv").is_file())
            self.assertTrue((awm_output / "holdout_truth.json").is_file())

            oed_output = root / "oed"
            oed_manifest = run_oed_replay(
                SYNTHETIC_OED_CONFIG,
                SYNTHETIC_AWM_CONFIG,
                SYNTHETIC_ORACLE_CONFIG,
                oracle_output_dir=oracle_output,
                output_dir=oed_output,
            )
            self.assertEqual("COMPLETE", oed_manifest["status"])
            self.assertFalse(oed_manifest["deployment_mutations_performed"])

            evaluation = json.loads(
                (oed_output / "oed_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            trace = [
                json.loads(line)
                for line in (oed_output / "oed_trace.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]

            # The candidate domain is no longer the degenerate single
            # Reveal candidate of the two-design MVP.
            first = next(
                row for row in trace if row["policy_kind"] == "full_oed"
            )
            self.assertEqual(5, len(first["candidate_domain"]))
            self.assertGreaterEqual(len(first["probe_candidates"]), 3)
            scores = {
                score["design_id"]: score
                for score in first["candidate_scores"]
            }
            feasible = [
                design_id
                for design_id, score in scores.items()
                if score["cap_feasible"] and score["purse_feasible"]
            ]
            self.assertGreaterEqual(len(feasible), 3)
            # The excursion cap is doing real work rather than admitting
            # every declared candidate.
            self.assertTrue(
                any(
                    not score["cap_feasible"] for score in scores.values()
                )
            )
            self.assertGreaterEqual(
                len({score["reveal_tier"] for score in scores.values()}),
                2,
            )

            policies = evaluation["policies"]
            self.assertGreaterEqual(
                policies["full_oed"]["reveal_count"],
                1,
            )
            self.assertEqual(
                "S_remote_baseline",
                policies["passive_awm"]["final_safe_design_id"],
            )
            # Non-degenerate: the policies genuinely disagree about where to
            # end up rather than all collapsing to one action.
            self.assertGreaterEqual(
                len(
                    {
                        policy["final_safe_design_id"]
                        for policy in policies.values()
                    }
                ),
                3,
            )
            self.assertEqual(
                0,
                policies["full_oed"]["safe_sequence_regression_count"],
            )
            self.assertTrue((oed_output / "oed_policy_summary.csv").is_file())

    def test_the_fixture_does_not_disturb_the_real_committed_contracts(
        self,
    ) -> None:
        """Generating a fixture writes only inside its output directory."""
        with tempfile.TemporaryDirectory() as temporary:
            before = {
                path: path.read_bytes()
                for path in sorted(CONFIGS.glob("*.json"))
            }
            generate_synthetic_oracle_fixture(
                FIXTURE_CONFIG,
                output_dir=Path(temporary) / "oracle",
            )
            after = {
                path: path.read_bytes()
                for path in sorted(CONFIGS.glob("*.json"))
            }
            self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
