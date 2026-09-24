"""Offline adapter regressions using the canonical ten-route test fixture."""

from contextlib import redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments import batch as entrypoint
from experiments import interleaved_batch as common
from experiments import ten_route_batch as ten
from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    FlowMeshSemanticTrialError,
)
from tests import test_simulator_full_flow_local_semantic_smoke as fixtures


class TenRouteBatchTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.LocalSemanticSmokeTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        inputs = self.fixture.inputs
        inputs.admission["worker_pin"] = {
            "kind": "worker_alias", "value": "test-worker",
        }
        for trial in inputs.bound_trials:
            trial["workload_id"] = "public-question"
        sources = {
            key: path.relative_to(self.root).as_posix()
            for key, path in self.fixture.n4_source_keywords.items()
        }
        sources.update({
            "local_semantic_admission_dir": "admission",
            "one_case_plan_dir": None,
            "n4_gate_deployment_binding_dir": None,
        })
        self.config = {
            "schema_version": ten.SCHEMA,
            "runtime_environment": "local",
            "source_dirs": sources,
            "worker_alias": "test-worker", "worker_node_alias": "node-n7",
            "task_timeout_seconds": 900,
            "expected_admission_sha256": inputs.admission["admission_sha256"],
            "one_case_selection": None,
            "n4_live_gate_sources_file": None,
            "n4_live_gate_sources_sha256": None,
        }

    def context(self):
        return common.load_inputs(common._validate_config(self.config),
                                  self.root)

    def test_same_entrypoint_freezes_and_checks_ten_route_config(self):
        draft = self.root / "draft.json"
        draft.write_bytes(common._pretty(self.config))
        frozen = self.root / "config"
        with redirect_stdout(StringIO()):
            status = entrypoint.main([
                "freeze-config", "--draft", str(draft),
                "--output-dir", str(frozen),
            ])
        self.assertEqual(status, 0)
        self.assertEqual(common.load_config(frozen)[0], self.config)
        stdout = StringIO()
        with redirect_stdout(stdout):
            status = entrypoint.main([
                "check", "--config-dir", str(frozen),
                "--artifact-root", str(self.root),
            ])
        result = json.loads(stdout.getvalue())
        self.assertEqual(status, 0)
        self.assertEqual(result["route_count"], 10)
        self.assertEqual(result["question_count"], 1)
        with self.assertRaises(FileExistsError):
            common.freeze_config(draft, frozen)

    def test_wrong_admission_or_worker_stops_before_network(self):
        with patch.object(common, "SdkFlowMeshClient") as client:
            self.config["expected_admission_sha256"] = "f" * 64
            with self.assertRaisesRegex(ValueError, "admission digest"):
                self.context()
            self.config["expected_admission_sha256"] = (
                self.fixture.inputs.admission["admission_sha256"]
            )
            self.config["worker_alias"] = "another-worker"
            with self.assertRaisesRegex(ValueError, "worker pin"):
                self.context()
            client.assert_not_called()

    def test_configuration_rejects_extra_keys_and_missing_plan_pair(self):
        self.config["api_key"] = "not-a-real-key"
        with self.assertRaisesRegex(ValueError, "keys"):
            common._validate_config(self.config)
        del self.config["api_key"]
        self.config["source_dirs"]["one_case_plan_dir"] = "plan"
        with self.assertRaisesRegex(ValueError, "together"):
            common._validate_config(self.config)

    def test_live_descriptor_checksum_checked_before_loading(self):
        path = self.root / "live.json"
        path.write_bytes(b"{}\n")
        self.config.update({
            "n4_live_gate_sources_file": "live.json",
            "n4_live_gate_sources_sha256": "0" * 64,
        })
        with patch.object(ten, "_load_n4_live_gate_sources") as loader:
            with self.assertRaisesRegex(ValueError, "checksum"):
                self.context()
            loader.assert_not_called()

    def test_freeze_one_case_delegates_to_existing_freezer(self):
        selection = {"case_id": "case", "workload_id": "public-question",
                     "safe_design_id": "D0"}
        self.config["one_case_selection"] = selection
        self.config["source_dirs"]["one_case_plan_dir"] = "new-plan"
        with patch.object(ten, "freeze_full_flow_one_case_plan",
                          return_value={"status": "VERIFIED"}) as freeze:
            common.freeze_inputs(self.config, self.root)
        freeze.assert_called_once_with(
            self.fixture.source, **selection, output_dir=self.root / "new-plan",
        )

    def test_local_runner_keeps_order_format_and_false_score(self):
        context = self.context()
        output = self.root / "fresh-run"
        executor = fixtures.RecordingExecutor(wrong_answer_case="n7-raw")
        worker = SimpleNamespace(alias="test-worker", node_alias="node-n7",
                                 status="IDLE", worker_id="observed-worker")
        with (
            patch.object(common, "_settings", return_value=object()),
            patch.object(common, "SdkFlowMeshClient") as client,
            patch.object(common, "describe_pinned_worker", return_value=worker),
            patch.object(common, "FlowMeshSemanticTrialExecutor",
                         return_value=executor),
            patch.object(common, "full_flow_hmac_header_provider",
                         return_value=object()),
            patch.dict(os.environ, {
                "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": "test-only",
            }),
        ):
            result = common.run(self.config, "config-digest", context,
                                output, execute=True, run_id="fresh-ten-run")
        self.assertEqual(result["status"], "VERIFIED")
        self.assertEqual(result["smoke_count"], 10)
        self.assertEqual([case for case, _ in executor.calls],
                         list(fixtures.CASES))
        self.assertEqual({p.name for p in output.iterdir()}, {
            fixtures.RECEIPT_NAME, fixtures.RESULTS_NAME, "SHA256SUMS",
        })
        rows = common._rows(output / fixtures.RESULTS_NAME)
        self.assertFalse(rows[0]["result"]["task_success"])
        journal = output.with_name(output.name + ".attempt")
        self.assertEqual(len(list(journal.glob("timing-*.json"))), 10)
        client.return_value.close.assert_called_once()
        verified = common.verify_output(self.config, "config-digest",
                                        context, output)
        self.assertEqual(result["receipt_sha256"], verified["receipt_sha256"])
        # The original verifier must still reject a damaged historical file.
        with (output / fixtures.RESULTS_NAME).open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaises(fixtures.FullFlowLocalSemanticSmokeError):
            common.verify_output(self.config, "config-digest", context, output)

    def test_missing_run_id_and_used_output_fail_before_network(self):
        context = self.context()
        with patch.object(common, "SdkFlowMeshClient") as client:
            with self.assertRaises(ValueError):
                common.run(self.config, "digest", context,
                           self.root / "unused", execute=True)
            with self.assertRaisesRegex(ValueError, "unused output"):
                common.run(self.config, "digest", context,
                           self.root, execute=True, run_id="new-run")
            client.assert_not_called()

    def test_multi_host_check_and_verify_use_canonical_boundaries(self):
        self.config["runtime_environment"] = "multi-host-private-network"
        with patch.object(ten.smoke, "_verify_multi_host_smoke_sources") as gate:
            context = self.context()
        gate.assert_called_once()
        with patch.object(ten.smoke, "verify_full_flow_semantic_smokes",
                          return_value={"status": "VERIFIED"}) as verifier:
            result = common.verify_output(self.config, "digest", context,
                                          self.root / "old-output")
        self.assertEqual(result["status"], "VERIFIED")
        verifier.assert_called_once_with(self.root / "old-output",
                                         **context["sources"])


class HistoricalTenRouteEvidenceTests(unittest.TestCase):
    def test_historical_receipt_keeps_its_semantic_version_boundary(self):
        root = Path(__file__).resolve().parents[1] / (
            "artifacts/minimum-real-retrieval-e1351cf"
        )
        output = root / "upcloud-minimum-real-retrieval-20260918t192435z"
        if not output.is_dir():
            self.skipTest("operator's historical evidence is not in this checkout")
        receipt = common._read(output / fixtures.RECEIPT_NAME)
        admission = root / "local-semantic-admission"
        trials = {row["trial_key"]: row for row in common._rows(
            admission / "semantic-execution-trials.jsonl",
        )}
        stages = {row["stage_key"]: row for row in common._rows(
            admission / "semantic-execution-stages.jsonl",
        )}
        checksums = "".join(
            f"{common._hash(p.read_bytes())}  {p.name}\n"
            for p in sorted(output.iterdir()) if p.name != "SHA256SUMS"
        ).encode("ascii")
        self.assertEqual((output / "SHA256SUMS").read_bytes(), checksums)
        rows = common._rows(output / fixtures.RESULTS_NAME)
        self.assertEqual([row["case_id"] for row in rows], list(fixtures.CASES))
        for row in rows:
            result = row["result"]
            trial = trials[row["trial_key"]]
            arguments = {
                "run_id": receipt["run_id"], "bound_trial": trial,
                "bound_stages": [stages[k]
                                 for k in trial["semantic_stage_keys"]],
            }
            if row["case_id"] in {"n7-raw", "n8-raw"}:
                # This September 18 receipt predates direct-video support.
                # An adapter must never reinterpret its sampled raw path as
                # today's direct-video path to make a historical check pass.
                with self.assertRaisesRegex(
                    FlowMeshSemanticTrialError, "semantic input profile",
                ):
                    common.verify_semantic_route_evidence(
                        result["semantic_route_evidence"], **arguments,
                    )
            else:
                verified = common.verify_semantic_route_evidence(
                    result["semantic_route_evidence"], **arguments,
                )
                self.assertEqual(verified["evidence_sha256"],
                                 result["route_evidence_sha256"])
            self.assertEqual(row["result_sha256"],
                             common._hash(common._canonical(result)))


if __name__ == "__main__":
    unittest.main()
