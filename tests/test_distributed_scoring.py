from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.config import load_config
from pathfinder.cli import main
from pathfinder.distributed import (
    ACCEPTED_SUBSTRING_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    FlowMeshDistributedSessionExecutor,
    TrialExecution,
    WorkloadScoringError,
    build_canonical_record,
    build_distributed_trial_plan,
    evaluate_workload_answer,
    load_distributed_pilot_preregistration,
    load_endpoint_registry,
    load_workload_scoring_contract,
    preflight_distributed_pilot,
    render_workload_question,
    validate_workload_manifest,
)
from pathfinder.evaluation import evaluate_distributed_pilot
from pathfinder.evaluation.example import create_evaluation_example


ROOT = Path(__file__).resolve().parents[1]


def exact_workload(**overrides):
    payload = {
        "object_id": "video-new-001",
        "question": "What happens after the door opens?",
        "answer_options": [
            {"option_id": "A", "text": "The person sits down"},
            {"option_id": "B", "text": "The person walks outside"},
            {"option_id": "C", "text": "The door closes again"},
        ],
        "correct_answer_id": "B",
        "task_class_id": "video_qa",
    }
    payload.update(overrides)
    return payload


def exact_preregistration(root: Path):
    payload = json.loads(
        (ROOT / "configs" / "distributed_pilot_example.json").read_text(
            encoding="utf-8"
        )
    )
    payload["success_scoring_rule"] = MULTIPLE_CHOICE_EXACT_SCORING_RULE
    payload["benchmark_bindings"] = {
        "selection_protocol_sha256": "1" * 64,
        "scoring_contract_sha256": "2" * 64,
        "representation_manifest_sha256": "3" * 64,
    }
    path = root / "preregistration.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return load_distributed_pilot_preregistration(path)


class DistributedScoringTest(unittest.TestCase):
    def test_exact_match_has_no_prose_or_case_heuristic(self) -> None:
        contract = load_workload_scoring_contract(
            exact_workload(), MULTIPLE_CHOICE_EXACT_SCORING_RULE
        )
        self.assertIs(True, evaluate_workload_answer(" B\n", contract))
        for answer in ("b", "B.", "The answer is B", "[B]", ""):
            with self.subTest(answer=answer):
                self.assertIs(False, evaluate_workload_answer(answer, contract))

    def test_exact_question_has_frozen_options_and_protocol(self) -> None:
        workload = exact_workload()
        contract = load_workload_scoring_contract(
            workload, MULTIPLE_CHOICE_EXACT_SCORING_RULE
        )
        rendered = render_workload_question(workload, contract)
        self.assertIn("[A] The person sits down", rendered)
        self.assertIn("[B] The person walks outside", rendered)
        self.assertTrue(rendered.endswith(
            "Return exactly one option ID and no other text."
        ))

    def test_malformed_exact_labels_fail_closed(self) -> None:
        cases = (
            {"answer_options": []},
            {"correct_answer_id": "Z"},
            {"answer_options": [
                {"option_id": "A", "text": "one"},
                {"option_id": "A", "text": "two"},
            ]},
            {"answer_options": [
                {"option_id": "a", "text": "one"},
                {"option_id": "B", "text": "two"},
            ], "correct_answer_id": "B"},
            {"accepted_answer_substrings": ["outside"]},
        )
        for mutation in cases:
            with self.subTest(mutation=mutation), self.assertRaises(
                WorkloadScoringError
            ):
                load_workload_scoring_contract(
                    exact_workload(**mutation),
                    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
                )

    def test_legacy_substring_rule_remains_compatible_and_unscored(self) -> None:
        scored = load_workload_scoring_contract(
            {
                "object_id": "old-video",
                "question": "What is shown?",
                "accepted_answer_substrings": ["red car"],
            },
            ACCEPTED_SUBSTRING_SCORING_RULE,
        )
        self.assertIs(True, evaluate_workload_answer("A RED   CAR.", scored))
        unscored = load_workload_scoring_contract(
            {
                "object_id": "old-video",
                "question": "What is shown?",
                "accepted_answer_substrings": [],
            },
            ACCEPTED_SUBSTRING_SCORING_RULE,
        )
        self.assertIsNone(evaluate_workload_answer("anything", unscored))

    def test_manifest_enforces_one_video_per_independent_unit(self) -> None:
        workloads = {
            "w1": exact_workload(),
            "w2": exact_workload(question="A different question"),
        }
        with self.assertRaisesRegex(
            WorkloadScoringError, "independent-unit count"
        ):
            validate_workload_manifest(
                workloads,
                ("w1", "w2"),
                MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            )

    def test_canonical_record_carries_exact_labels_and_score(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prereg = exact_preregistration(Path(temporary))
            trial = build_distributed_trial_plan(prereg)[0]
            workload = exact_workload(object_id="video-for-record")
            record = build_canonical_record(
                prereg,
                trial,
                TrialExecution(final_answer="B", access_events=()),
                workload=workload,
                started_at="2000-01-01T00:00:00+00:00",
                finished_at="2000-01-01T00:00:01+00:00",
                cost_model=prereg.cost_model,
                provider=None,
                source_node_id=None,
            )
        self.assertEqual(
            MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            record["success_scoring_rule"],
        )
        self.assertEqual("B", record["correct_answer_id"])
        self.assertIs(True, record["task_success"])
        self.assertNotIn("accepted_answer_substrings", record)

    def test_flowmesh_request_uses_the_exact_match_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prereg = exact_preregistration(Path(temporary))
            trial = build_distributed_trial_plan(prereg)[0]
            executor = FlowMeshDistributedSessionExecutor(
                object(),
                success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            )
            request = executor.build_request(trial, exact_workload())
        self.assertIn("[C] The door closes again", request.question)
        self.assertTrue(request.question.endswith("no other text."))

    def test_offline_evaluator_replays_the_exact_match_rule(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            snapshot = root / "snapshot"
            output = root / "evaluation"
            create_evaluation_example(
                snapshot,
                success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            )
            result = evaluate_distributed_pilot(
                snapshot / "run",
                preregistration=snapshot / "config" / "preregistration.json",
                endpoint_registry=snapshot / "config" / "endpoint-registry.json",
                workload_manifest=snapshot / "config" / "workloads.json",
                measurement_manifest=snapshot / "config" / "measurements.json",
                output_dir=output,
            )
        self.assertEqual(
            MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            result["success_scoring_rule"],
        )
        self.assertEqual(6, result["by_role"]["safe"]["task_successes"])
        self.assertEqual(
            5,
            result["by_role"]["restricted_candidate"]["task_successes"],
        )
        self.assertTrue(any(
            "without extracting an answer from prose" in limitation
            for limitation in result["limitations"]
        ))

    def test_cli_can_create_an_exact_match_evaluation_example(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "example"
            with contextlib.redirect_stdout(io.StringIO()) as captured:
                code = main([
                    "create-workload-evaluation-example",
                    "--success-scoring-rule",
                    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
                    "--output-dir",
                    str(output),
                ])
            prereg = load_distributed_pilot_preregistration(
                output / "config" / "preregistration.json"
            )
        self.assertEqual(0, code, captured.getvalue())
        self.assertEqual(
            MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            prereg.success_scoring_rule,
        )

    def test_next_pilot_template_has_36_units_and_144_cells(self) -> None:
        prereg = load_distributed_pilot_preregistration(
            ROOT
            / "configs"
            / "pathfinderbench_restricted_pilot_v0_1_preregistration.template.json"
        )
        self.assertEqual(MULTIPLE_CHOICE_EXACT_SCORING_RULE,
                         prereg.success_scoring_rule)
        self.assertEqual(36, prereg.independent_workload_count)
        self.assertEqual(2, prereg.repetitions)
        self.assertEqual(144, prereg.planned_trial_count)
        self.assertEqual(
            "aa6f1c14ee7bce126260008b62d79f9397410b65abfa2dd04bc12f986c13d5b8",
            prereg.selection_protocol_sha256,
        )
        self.assertEqual(
            "2d731a4b50f7421b8fa3fbd6d9e92d0892ec2a11e5b287e2244648f3148d17eb",
            prereg.scoring_contract_sha256,
        )
        self.assertEqual(
            "35ecbfd3de167d15b696d81a7b7440936c249eaee61790301b752cbe5fbcc32d",
            prereg.representation_manifest_sha256,
        )

    def test_next_pilot_system_and_endpoint_routes_match_exactly(self) -> None:
        system = load_config(
            ROOT
            / "configs"
            / "pathfinderbench_restricted_pilot_v0_1_system.json"
        )
        registry = load_endpoint_registry(
            ROOT
            / "configs"
            / "pathfinderbench_restricted_pilot_v0_1_endpoints.json"
        )
        expected = {
            ("D_origin_remote", "sampled_frames"): "origin_remote",
            ("D_origin_remote", "multimodal_digest"): "origin_remote",
            ("D_local_frames", "sampled_frames"): "local_materialized",
            ("D_local_frames", "multimodal_digest"): "origin_remote",
            ("D_local_digest", "sampled_frames"): "origin_remote",
            ("D_local_digest", "multimodal_digest"): "local_materialized",
        }
        for (design_id, representation_id), endpoint_id in expected.items():
            with self.subTest(
                design_id=design_id,
                representation_id=representation_id,
            ):
                route = registry.route(
                    design_id=design_id,
                    representation_id=representation_id,
                )
                self.assertEqual(endpoint_id, route.endpoint_id)
                self.assertEqual(
                    registry.endpoint(endpoint_id).location,
                    system.designs[design_id].paths[
                        representation_id
                    ].location,
                )

    def test_exact_preregistration_requires_all_benchmark_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = json.loads(
                (
                    ROOT / "configs" / "distributed_pilot_example.json"
                ).read_text(encoding="utf-8")
            )
            payload["success_scoring_rule"] = (
                MULTIPLE_CHOICE_EXACT_SCORING_RULE
            )
            payload["benchmark_bindings"] = {
                "selection_protocol_sha256": "1" * 64,
                "scoring_contract_sha256": "2" * 64,
            }
            path = root / "preregistration.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                ValueError, "representation_manifest_sha256 is required"
            ):
                load_distributed_pilot_preregistration(path)

    def test_frozen_bindings_pass_while_deployment_placeholders_block_live(
        self,
    ) -> None:
        prereg = load_distributed_pilot_preregistration(
            ROOT
            / "configs"
            / "pathfinderbench_restricted_pilot_v0_1_preregistration.template.json"
        )
        registry = load_endpoint_registry(
            ROOT
            / "configs"
            / "pathfinderbench_restricted_pilot_v0_1_endpoints.json"
        )
        arguments = {
            "environment": {
                "PATHFINDER_DATA_AGENT_ORIGIN_URL": (
                    "https://origin.example.invalid"
                ),
                "PATHFINDER_DATA_AGENT_LOCAL_URL": (
                    "https://local.example.invalid"
                ),
            },
            "worker_pin": {"kind": "worker_alias", "value": "worker"},
            "measurement_manifest_sha256": "4" * 64,
        }
        offline = preflight_distributed_pilot(
            prereg, registry, mode="offline_validation", **arguments
        )
        live = preflight_distributed_pilot(
            prereg, registry, mode="live_pilot", **arguments
        )
        check = "manifest.benchmark_contracts_bound"
        self.assertNotIn(check, offline["advisory_warnings"])
        self.assertNotIn(check, offline["failed_checks"])
        self.assertNotIn(check, live["failed_checks"])
        self.assertIn(
            "cost_model.rates_are_measured_not_placeholder",
            live["failed_checks"],
        )
        self.assertIn(
            "identity.source_git_revision_is_real",
            live["failed_checks"],
        )


if __name__ == "__main__":
    unittest.main()
