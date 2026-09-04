from __future__ import annotations

import csv
import json
import socket
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from pathfinder.awm import AWMConfigError, audit_distributed_policy_awm
from pathfinder.cli import main
from pathfinder.distributed import load_distributed_pilot_preregistration
from pathfinder.evaluation import evaluate_distributed_pilot
from pathfinder.evaluation.example import create_evaluation_example


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


class DistributedPolicyAWMAuditTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.example = self.root / "example"
        create_evaluation_example(self.example)
        self.inputs = self.example / "config"
        self.run = self.example / "run"
        self.evaluation = self.root / "evaluation"
        evaluate_distributed_pilot(
            self.run,
            preregistration=self.inputs / "preregistration.json",
            endpoint_registry=self.inputs / "endpoint-registry.json",
            workload_manifest=self.inputs / "workloads.json",
            measurement_manifest=self.inputs / "measurements.json",
            output_dir=self.evaluation,
        )
        prereg = load_distributed_pilot_preregistration(
            self.inputs / "preregistration.json"
        )
        self.strata = tuple(stratum.stratum_id for stratum in prereg.strata)
        self.audit_inputs = self.root / "audit-inputs"
        self.audit_inputs.mkdir()
        self.config = self.audit_inputs / "policy.json"
        fallback_stratum = self.strata[-1]
        self.config_payload = {
            "schema_version": (
                "pathfinder.distributed-policy-awm-audit/v1alpha1"
            ),
            "audit_id": "synthetic-distributed-policy-audit",
            "posthoc": True,
            "eligible_for_scientific_claims": False,
            "executed_policy_id": "executed",
            "policies": [
                {
                    "policy_id": "executed",
                    "provenance": (
                        "executed-preregistered-restricted-policy"
                    ),
                    "note": "The policy that produced candidate observations.",
                    "assignment_by_stratum": {
                        stratum: "candidate" for stratum in self.strata
                    },
                },
                {
                    "policy_id": "fallback",
                    "provenance": (
                        "posthoc-selected-after-inspecting-pilot-outcomes"
                    ),
                    "note": "A deliberately post-hoc fallback diagnostic.",
                    "assignment_by_stratum": {
                        stratum: (
                            "safe" if stratum == fallback_stratum
                            else "candidate"
                        )
                        for stratum in self.strata
                    },
                },
            ],
            "confidence": {
                "family_adjustment": "bonferroni",
                "bound_method": (
                    "workload-cluster-one-sided-bounded-mean-kl"
                ),
                "minimum_independent_workloads": (
                    prereg.independent_workload_count
                ),
            },
            "supports": {
                "success_difference": {"lower": -1.0, "upper": 1.0},
                "cost_saving": {"lower": -100.0, "upper": 100.0},
            },
        }
        self._write_config()
        self.output = self.root / "outputs" / "policy-audit"

    def _write_config(self) -> None:
        self.config.write_text(
            json.dumps(self.config_payload),
            encoding="utf-8",
        )

    def audit(self, output: Path | None = None):
        return audit_distributed_policy_awm(
            self.evaluation,
            preregistration=self.inputs / "preregistration.json",
            audit_config=self.config,
            output_dir=output or self.output,
        )

    def test_executed_policy_reproduces_evaluator_and_fallback_uses_zero_effect(self):
        result = self.audit()
        evaluation = read_json(self.evaluation / "evaluation.json")
        overall = next(
            row for row in evaluation["paired_aggregates"]
            if row["scope"] == "overall"
        )
        summaries = {
            row["policy_id"]: row for row in result["policy_summaries"]
        }
        executed = summaries["executed"]
        self.assertAlmostEqual(
            overall["mean_task_success_delta"],
            executed["mean_task_success_delta"],
        )
        self.assertAlmostEqual(
            -overall["mean_total_cost_delta"],
            executed["mean_cost_saving"],
        )
        fallback = summaries["fallback"]
        self.assertLess(
            fallback["candidate_workloads"],
            executed["candidate_workloads"],
        )
        self.assertEqual(
            result["independent_workloads"] - fallback["candidate_workloads"],
            fallback["safe_fallback_workloads"],
        )
        with (self.output / "policy_workload_effects.csv").open(
            encoding="utf-8", newline=""
        ) as handle:
            rows = list(csv.DictReader(handle))
        safe_rows = [
            row for row in rows
            if row["policy_id"] == "fallback"
            and row["selected_role"] == "safe"
        ]
        self.assertTrue(safe_rows)
        self.assertTrue(all(
            float(row["task_success_delta_vs_safe"]) == 0.0
            and float(row["cost_saving_vs_safe"]) == 0.0
            and row["selected_design_id"] == row["safe_design_id"]
            for row in safe_rows
        ))

    def test_outputs_are_reproducible_hashed_and_keep_claim_boundary(self):
        first = self.audit()
        other = self.root / "other-outputs" / "policy-audit"
        second = self.audit(other)
        self.assertEqual(snapshot(self.output), snapshot(other))
        self.assertIs(first["complete_design_oracle"], False)
        self.assertIs(first["unobserved_design_outcomes_imputed"], False)
        self.assertIs(first["posthoc"], True)
        self.assertIs(first["eligible_for_scientific_claims"], False)
        self.assertEqual(first, second)
        for line in (self.output / "SHA256SUMS").read_text().splitlines():
            digest, name = line.split("  ")
            self.assertEqual(
                digest,
                sha256((self.output / name).read_bytes()).hexdigest(),
            )

    def test_output_is_path_independent(self):
        self.audit()
        second_root = self.root / "second-machine"
        second_evaluation = second_root / "evaluation"
        second_inputs = second_root / "inputs"
        second_inputs.mkdir(parents=True)
        second_evaluation.mkdir()
        for path in self.evaluation.iterdir():
            (second_evaluation / path.name).write_bytes(path.read_bytes())
        second_prereg = second_inputs / "preregistration.json"
        second_config = second_inputs / "policy.json"
        second_prereg.write_bytes(
            (self.inputs / "preregistration.json").read_bytes()
        )
        second_config.write_bytes(self.config.read_bytes())
        second_output = self.root / "second-machine-output" / "audit"
        audit_distributed_policy_awm(
            second_evaluation,
            preregistration=second_prereg,
            audit_config=second_config,
            output_dir=second_output,
        )
        self.assertEqual(snapshot(self.output), snapshot(second_output))

    def test_tampered_evaluation_is_refused_without_output(self):
        path = self.evaluation / "paired_effects.csv"
        path.write_text(path.read_text() + "tampered\n", encoding="utf-8")
        with self.assertRaisesRegex(AWMConfigError, "checksum mismatch"):
            self.audit()
        self.assertFalse(self.output.exists())

    def test_missing_assignment_and_unmarked_derived_policy_are_refused(self):
        del self.config_payload["policies"][1]["assignment_by_stratum"][
            self.strata[0]
        ]
        self._write_config()
        with self.assertRaisesRegex(AWMConfigError, "every observed stratum"):
            self.audit()
        self.assertFalse(self.output.exists())

        self.config_payload["policies"][1]["assignment_by_stratum"][
            self.strata[0]
        ] = "candidate"
        self.config_payload["policies"][1]["provenance"] = (
            "executed-preregistered-restricted-policy"
        )
        self._write_config()
        with self.assertRaisesRegex(AWMConfigError, "derived policy"):
            self.audit()
        self.assertFalse(self.output.exists())

    def test_offline_read_only_and_cli_entrypoint(self):
        before = snapshot(self.root)
        argv = [
            "audit-distributed-policy-awm",
            "--evaluation-dir", str(self.evaluation),
            "--preregistration", str(
                self.inputs / "preregistration.json"
            ),
            "--audit-config", str(self.config),
            "--output-dir", str(self.output),
        ]
        with patch.object(
            socket.socket,
            "connect",
            side_effect=AssertionError("network"),
        ):
            self.assertEqual(main(argv), 0)
        after = snapshot(self.root)
        for name, content in before.items():
            self.assertEqual(content, after[name])
        self.assertTrue(self.output.is_dir())


if __name__ == "__main__":
    unittest.main()
