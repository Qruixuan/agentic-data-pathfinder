from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.cli import main as cli_main
from pathfinder.integrations.flowmesh.container_conditional_dag import (
    derive_initial_cache_outcomes,
    list_conditional_container_trial_candidates,
    resolve_conditional_container_trial,
)
from pathfinder.integrations.flowmesh.container_dag import FlowMeshContainerDagError
from pathfinder.simulator import build_portable_execution_plan, plan_container_backend


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
SCENARIO_ID = "flowmesh-infra-4x8-local-smoke-v1"


def _trial_key(workload: str, design: str = "D3", repetition: int = 0) -> str:
    return f"{SCENARIO_ID}|smoke-{workload}|{design}|r{repetition:04d}"


def _suffixes(operation_keys: list[str]) -> set[str]:
    return {key.rsplit("|", 1)[-1] for key in operation_keys}


class ConditionalContainerDagResolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        portable = root / "portable"
        self.container = root / "container"
        build_portable_execution_plan(SCENARIO, output_dir=portable)
        plan_container_backend(
            SCENARIO,
            portable,
            CONTAINER_SPEC,
            output_dir=self.container,
        )
        self.operations = [
            json.loads(line)
            for line in (self.container / "container_operations.jsonl").read_text(
                encoding="utf-8"
            ).splitlines()
            if line.strip()
        ]

    def test_lists_all_sixteen_cache_conditional_trials_without_hiding_them(self) -> None:
        candidates = list_conditional_container_trial_candidates(self.operations)
        self.assertEqual(16, len(candidates))
        self.assertEqual(
            {
                (design, repetition)
                for design in ("D3", "D7")
                for repetition in ("r0000", "r0001")
            },
            {
                (
                    candidate["trial_key"].split("|")[2],
                    candidate["trial_key"].split("|")[3],
                )
                for candidate in candidates
            },
        )
        self.assertTrue(
            all(candidate["inactive_operation_count"] > 0 for candidate in candidates)
        )

    def test_hit_resolution_excludes_the_remote_miss_branch(self) -> None:
        trial_key = _trial_key("descriptive")
        resolution = resolve_conditional_container_trial(
            self.operations,
            trial_key=trial_key,
        )
        lookup_key = f"{trial_key}|lookup"
        self.assertEqual({lookup_key: "hit"}, resolution["cache_outcomes"])
        self.assertEqual({"schedule", "lookup"}, _suffixes(resolution["phase_a_operation_keys"]))
        self.assertIn("read-local", _suffixes(resolution["phase_b_operation_keys"]))
        self.assertNotIn("read-remote", _suffixes(resolution["active_operation_keys"]))
        self.assertNotIn("transfer-remote", _suffixes(resolution["active_operation_keys"]))
        self.assertNotIn("insert", _suffixes(resolution["active_operation_keys"]))
        self.assertEqual(f"{trial_key}|infer", resolution["terminal_operation_key"])
        self.assertEqual(["D3|r0000"], resolution["cache_scope_ids"])

    def test_miss_resolution_excludes_the_local_hit_branch(self) -> None:
        trial_key = _trial_key("retrieval")
        resolution = resolve_conditional_container_trial(
            self.operations,
            trial_key=trial_key,
        )
        lookup_key = f"{trial_key}|lookup"
        self.assertEqual({lookup_key: "miss"}, resolution["cache_outcomes"])
        active = _suffixes(resolution["active_operation_keys"])
        self.assertNotIn("read-local", active)
        self.assertTrue({"read-remote", "transfer-remote", "insert"} <= active)
        self.assertEqual([f"{trial_key}|read-local"], resolution["inactive_operation_keys"])
        self.assertTrue(resolution["phase_a_satisfied_dependencies"])

    def test_two_lookup_trial_requires_the_exact_complete_outcome_vector(self) -> None:
        trial_key = _trial_key("causal")
        derived = derive_initial_cache_outcomes(self.operations, trial_key=trial_key)
        self.assertEqual(2, len(derived))
        self.assertEqual({"miss"}, set(derived.values()))

        one_lookup = next(iter(derived))
        with self.assertRaisesRegex(
            FlowMeshContainerDagError,
            "must name exactly every cache lookup",
        ):
            resolve_conditional_container_trial(
                self.operations,
                trial_key=trial_key,
                cache_outcomes={one_lookup: "miss"},
            )

        incorrect = dict(derived)
        incorrect[one_lookup] = "hit"
        with self.assertRaisesRegex(
            FlowMeshContainerDagError,
            "disagrees with the frozen initial cache snapshot",
        ):
            resolve_conditional_container_trial(
                self.operations,
                trial_key=trial_key,
                cache_outcomes=incorrect,
            )

    def test_conditional_branch_must_depend_on_its_own_lookup(self) -> None:
        trial_key = _trial_key("retrieval")
        tampered = json.loads(json.dumps(self.operations))
        for row in tampered:
            if row["operation_key"] == f"{trial_key}|read-local":
                row["dependency_operation_keys"] = []
                break
        else:
            self.fail("expected local cache-read branch")
        with self.assertRaisesRegex(
            FlowMeshContainerDagError,
            "does not depend on its cache lookup",
        ):
            resolve_conditional_container_trial(tampered, trial_key=trial_key)

    def test_cli_resolves_only_the_frozen_snapshot_branch_without_submitting(self) -> None:
        trial_key = _trial_key("descriptive")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exit_code = cli_main([
                "resolve-flowmesh-container-conditional-dag",
                "--container-operations",
                str(self.container / "container_operations.jsonl"),
                "--trial-key",
                trial_key,
                "--compact",
            ])
        self.assertEqual(0, exit_code)
        payload = json.loads(output.getvalue())
        self.assertEqual("RESOLVED_FROM_FROZEN_CACHE_SNAPSHOT", payload["status"])
        self.assertFalse(payload["workflow_submitted"])
        self.assertFalse(payload["services_started"])
        self.assertEqual({f"{trial_key}|lookup": "hit"}, payload["cache_outcomes"])


if __name__ == "__main__":
    unittest.main()
