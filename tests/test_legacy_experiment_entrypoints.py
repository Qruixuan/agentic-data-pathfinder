"""Dated CLI compatibility delegates to the reusable implementation."""

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import unittest
from unittest.mock import patch

from experiments.multiq_pilot_20260924 import compat
from experiments.multiq_pilot_20260924 import verify_24route_pilot as verify24
from experiments.multiq_pilot_20260924 import verify_28route_sealed as verify28
from tests.test_reusable_interleaved_batch import ARTIFACTS, _fixture


class LegacyEntrypointTests(unittest.TestCase):
    def test_offline_default_never_contacts_flowmesh(self):
        context = {"report": {"admission_sha256": "a" * 64},
                   "trials": [{}] * 24}
        with (
            patch.object(compat, "historical_inputs",
                         return_value=({}, context)),
            patch.object(compat.batch, "run") as run,
            redirect_stdout(StringIO()),
        ):
            self.assertEqual(compat.runner_main(
                24, ["--artifact-root", "unused"],
            ), 0)
        run.assert_not_called()

    def test_execute_requires_frozen_config_before_any_source_or_network(self):
        for count in (24, 28):
            with (
                self.subTest(count=count),
                patch.object(compat, "historical_inputs") as inputs,
                patch.object(compat.batch, "run") as run,
                redirect_stderr(StringIO()),
                self.assertRaises(SystemExit) as error,
            ):
                compat.runner_main(count, ["--artifact-root", "unused",
                                           "--execute"])
            self.assertEqual(error.exception.code, 2)
            inputs.assert_not_called()
            run.assert_not_called()

    def test_frozen_execute_delegates_and_propagates_failure_status(self):
        config = {"schema_version": compat.batch.SCHEMA,
                  "expected_route_count": 28}
        with (
            patch.object(compat.batch, "load_config",
                         return_value=(config, "digest")),
            patch.object(compat.batch, "load_inputs", return_value={}),
            patch.object(compat.batch, "run", return_value={
                "status": "STOPPED_AT_FIRST_FAILURE",
            }) as run,
            redirect_stdout(StringIO()),
        ):
            code = compat.runner_main(28, [
                "--artifact-root", "inputs", "--config-dir", "frozen",
                "--execute", "--output-dir", "output",
            ])
        self.assertEqual(code, 2)
        run.assert_called_once_with(config, "digest", {}, Path("output"),
                                    execute=True)

    def test_original_verifier_function_signatures_are_preserved(self):
        with patch.object(verify24, "verify_legacy", return_value={}) as verify:
            verify24.verify(Path("inputs"), Path("output"), seal=True)
            verify.assert_called_once_with(24, Path("inputs"), Path("output"),
                                           seal=True)
        with patch.object(verify28, "verify_legacy", return_value={}) as verify:
            verify28.verify(Path("inputs"), Path("baseline"), Path("output"))
            verify.assert_called_once_with(
                28, Path("inputs"), Path("output"),
                baseline_spec_dir=Path("baseline"), seal=False,
            )

    def test_legacy_count_guard_is_not_removed(self):
        with patch.object(compat.batch, "load_inputs", return_value={
            "report": {"stage_count": 275, "data_agent_plan_binding_count": 42},
        }):
            with self.assertRaisesRegex(ValueError, "counts differ"):
                compat.historical_inputs(24, Path("inputs"))

    def test_cost_and_replay_modules_still_import_legacy_verifier(self):
        from experiments.multiq_pilot_20260924 import audit_28route_cost
        from experiments.multiq_pilot_20260924 import replay_28_baselines
        self.assertIs(audit_28route_cost.verify, verify28.verify)
        self.assertIs(replay_28_baselines.verify, verify28.verify)

    def test_historical_outputs_preserve_status_and_result_fields(self):
        for count, draft, relative in (
            (24, "dev-24.draft.json", "multiq-24route-20260923t2250z"),
            (28, "sealed-28.draft.json",
             "multiq-sealed-28route-20260924t080919z-c7df0003/routes"),
        ):
            with self.subTest(count=count):
                config, context, _ = _fixture(draft)
                with patch.object(compat, "historical_inputs",
                                  return_value=(config, context)):
                    result = compat.verify_legacy(
                        count, ARTIFACTS, ARTIFACTS / relative,
                    )
                self.assertEqual(result["status"],
                                 f"VERIFIED_{count}_ROUTE_OUTPUT")
                self.assertEqual(result["route_count"], count)
                self.assertNotIn("config_sha256", result)
                self.assertEqual("baseline_spec_sha256" in result, count == 28)


if __name__ == "__main__":
    unittest.main()
