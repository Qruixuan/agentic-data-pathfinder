"""Focused offline regressions for the reusable multi-question batch CLI."""

from __future__ import annotations

import copy
from contextlib import redirect_stdout
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments import interleaved_batch as batch


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"
DRAFTS = ROOT / "experiments/multiq_pilot_20260924/configs"


def _fixture(name: str) -> tuple[dict, dict, Path]:
    config = batch._validate_config(batch._read(DRAFTS / name))
    sources = {key: ARTIFACTS / relative
               for key, relative in config["source_dirs"].items()}
    admission = ARTIFACTS / config["admission_dir"]
    if not (admission / "interleaved-runtime-admission.json").is_file():
        raise unittest.SkipTest("optional operator evidence is not installed")
    manifest = batch._read(admission / "interleaved-runtime-admission.json")
    plan = batch._read(sources["plan_dir"] / "interleaved-plan.json")
    trials = sorted(
        batch._rows(admission / "admitted-trials.jsonl"),
        key=lambda item: item["order_index"],
    )
    routes = batch._rows(sources["binding_dir"] / "route-inputs.jsonl")
    episodes = batch._rows(admission / "cache-episode-bindings.jsonl")
    context = {
        "report": {
            "status": "VERIFIED_INTERLEAVED_RUNTIME_ADMISSION_NOT_DEPLOYED",
            "admission_sha256": manifest["admission_sha256"],
            "trial_count": manifest["trial_count"],
            "stage_count": manifest["stage_count"],
            "index_query_plan_count": manifest["index_query_plan_count"],
            "cache_episode_binding_count": manifest["cache_episode_binding_count"],
        },
        "plan": plan,
        "trials": trials,
        "stages": batch._rows(admission / "admitted-stages.jsonl"),
        "routes": {item["trial_key"]: item for item in routes},
        "episodes": {item["trial_key"]: item for item in episodes},
    }
    context["baseline_sha256"] = batch._baseline(
        ARTIFACTS, config["baseline_spec_dir"],
        manifest["admission_sha256"], plan["plan_sha256"],
    )
    return config, context, admission


class ReusableBatchTests(unittest.TestCase):
    def test_observation_output_keeps_rejection_out_of_answer_accuracy(self):
        config, context, _ = _fixture("sealed-28.draft.json")
        old = ARTIFACTS / "multiq-sealed-28route-20260924t080919z-c7df0003/routes"
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            out = Path(temporary) / "out"
            shutil.copytree(old, out)
            start, summary = batch._read(out / "start.json"), batch._read(out / "summary.json")
            start["config_sha256"] = summary["config_sha256"] = "config"
            summary["status"] = "RECORDED_INTERLEAVED_BATCH_OBSERVATIONS"
            (out / "start.json").write_bytes(batch._pretty(start))
            (out / "summary.json").write_bytes(batch._pretty(summary))
            trial = context["trials"][0]
            route = context["routes"][trial["trial_key"]]
            diagnosis = {
                "run_id": route["run_id"], "trial_key": trial["trial_key"],
                "execution_id": batch._hash(batch._canonical({
                    "domain": "pathfinder.generic-semantic-route-id/v1",
                    "run_id": route["run_id"], "trial_key": trial["trial_key"]})),
                "provider_code": "data_inspection_failed", "http_status": 400,
                "workflow_status": "FAILED", "durable_state": "FAILED",
                "failure_sha256": "a" * 64, "n6_error_sha256": "b" * 64,
                "retry_authorized": False, "credentials_recorded": False,
            }
            terminal = {**batch._read(out / "timing-00.json"),
                        "status": "OBSERVED_PROVIDER_REJECTION", "task_success": None,
                        "diagnosis": diagnosis}
            (out / "route-00.json").unlink()
            (out / "terminal-00.json").write_bytes(batch._pretty(terminal))
            (out / "continuation.json").write_bytes(batch._pretty({
                "schema_version": "pathfinder.batch-continuation/v1",
                "previous_submissions_repeated": False, "credentials_recorded": False}))
            _refresh_checksums(out)
            verified = batch.verify_output(config, "config", context, out)
            self.assertEqual(verified["status"], "VERIFIED_INTERLEAVED_BATCH_OBSERVATIONS")
            self.assertEqual(sum(verified["unavailable_by_arm"].values()), 1)
            self.assertEqual(sum(sum(x.values()) for x in verified["success_by_arm"].values()), 27)
            terminal["task_success"] = False
            (out / "terminal-00.json").write_bytes(batch._pretty(terminal))
            _refresh_checksums(out)
            with self.assertRaisesRegex(ValueError, "terminal failure"):
                batch.verify_output(config, "config", context, out)

    def test_continuation_validates_prefix_and_never_retries_terminal(self):
        config, context, _ = _fixture("sealed-28.draft.json")
        context = copy.deepcopy(context)
        context["trials"] = context["trials"][:3]
        old = ARTIFACTS / "multiq-sealed-28route-20260924t080919z-c7df0003/routes"
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            parent = Path(temporary) / "parent"
            parent.mkdir()
            start = batch._read(old / "start.json")
            start["config_sha256"] = "config"
            (parent / "start.json").write_bytes(batch._pretty(start))
            for name in ("route-00.json", "timing-00.json"):
                shutil.copyfile(old / name, parent / name)
            failure = {**batch._read(old / "timing-01.json"),
                       "status": "STOPPED_AT_FIRST_FAILURE",
                       "failure_class": "infrastructure",
                       "failure_code": "flowmesh-workflow-terminal-failure",
                       "credentials_recorded": False}
            (parent / "failure.json").write_bytes(batch._pretty(failure))
            key = context["trials"][1]["trial_key"]
            run_id = context["routes"][key]["run_id"]
            diagnosis = {
                "run_id": run_id, "trial_key": key,
                "execution_id": batch._hash(batch._canonical({
                    "domain": "pathfinder.generic-semantic-route-id/v1",
                    "run_id": run_id, "trial_key": key})),
                "provider_code": "data_inspection_failed", "http_status": 400,
                "workflow_status": "FAILED", "durable_state": "FAILED",
                "failure_sha256": "a" * 64, "n6_error_sha256": "b" * 64,
                "retry_authorized": False, "credentials_recorded": False,
            }
            receipt = Path(temporary) / "diagnosis.json"
            receipt.write_bytes(batch._pretty(diagnosis))
            before = {p.name: p.read_bytes() for p in parent.iterdir()}
            prefix = batch.continuation_prefix(config, "config", context, parent, receipt)
            self.assertNotIn("route-01.json", prefix)
            self.assertIsNone(json.loads(prefix["terminal-01.json"])["task_success"])
            self.assertEqual(json.loads(prefix["continuation.json"])["next_ordinal"], 2)
            worker = SimpleNamespace(alias=config["worker_alias"],
                                     node_alias=config["worker_node_alias"],
                                     status="IDLE", worker_id="fake-worker")
            with patch.object(batch, "_settings"), \
                    patch.object(batch, "SdkFlowMeshClient"), \
                    patch.object(batch, "describe_pinned_worker", return_value=worker), \
                    patch.object(batch, "full_flow_hmac_header_provider"), \
                    patch.dict(os.environ, {"PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": "test-only"}), \
                    patch.object(batch, "FlowMeshSemanticTrialExecutor") as executor, \
                    redirect_stdout(StringIO()):
                executor.return_value.execute.return_value = batch._read(old / "route-02.json")
                result = batch.run(config, "config", context, Path(temporary) / "out",
                                   execute=True, resume_from=parent, failure_diagnosis=receipt)
                self.assertEqual(result["status"], "ALL_ROUTES_OBSERVED")
                executor.return_value.execute.assert_called_once()
                self.assertEqual(executor.return_value.execute.call_args.kwargs["trial"],
                                 context["trials"][2])
            self.assertEqual(before, {p.name: p.read_bytes() for p in parent.iterdir()})
            for field, bad in (("provider_code", "unknown"), ("retry_authorized", True),
                               ("run_id", "other"), ("execution_id", "c" * 64)):
                invalid = {**diagnosis, field: bad}
                receipt.write_bytes(batch._pretty(invalid))
                with self.subTest(field=field), self.assertRaises(ValueError):
                    batch.continuation_prefix(config, "config", context, parent, receipt)
            receipt.write_bytes(batch._pretty(diagnosis))
            saved = batch._read(parent / "route-00.json")
            saved["idempotency_key"] = "bad"
            (parent / "route-00.json").write_bytes(batch._pretty(saved))
            with self.assertRaisesRegex(ValueError, "saved route evidence"):
                batch.continuation_prefix(config, "config", context, parent, receipt)

    def test_rotated_schedule_changes_order_not_requests(self):
        trials = [{"workload_id": "q", "design_id": arm,
                   "trial_key": arm, "order_index": i}
                  for i, arm in enumerate(("R", "D", "DC", "I"))]
        routes = {arm: {"run_id": "run-" + arm, "object_id": "video"}
                  for arm in ("R", "D", "DC", "I")}
        schedule = [{"ordinal": 0, "question_id": "q", "object_id": "video",
                     "route_slots": [{"arm_id": arm, "run_id": "run-" + arm}
                                     for arm in ("I", "R", "D", "DC")]}]
        ordered = batch.schedule_trials(trials, schedule, routes)
        self.assertEqual([t["design_id"] for t in ordered], ["I", "R", "D", "DC"])
        self.assertIs(ordered[0], trials[3])
        self.assertEqual(ordered[0]["order_index"], 3)
        schedule[0]["route_slots"][0]["run_id"] = "wrong-run"
        with self.assertRaisesRegex(ValueError, "identity differs"):
            batch.schedule_trials(trials, schedule, routes)

    def test_schedule_rejects_duplicate_and_missing_slots(self):
        trials = [{"workload_id": "q", "design_id": "R", "trial_key": "r"}]
        routes = {"r": {"run_id": "run-r", "object_id": "v"}}
        question = {"ordinal": 0, "question_id": "q", "object_id": "v",
                    "route_slots": [{"arm_id": "R", "run_id": "run-r"}]}
        with self.assertRaisesRegex(ValueError, "omit"):
            batch.schedule_trials(trials, [], routes)
        question["route_slots"] *= 2
        with self.assertRaisesRegex(ValueError, "coverage"):
            batch.schedule_trials(trials, [question], routes)

    def test_config_freeze_checksum_and_tamper_rejection(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            frozen = Path(temporary) / "frozen"
            draft = DRAFTS / "dev-24.draft.json"
            digest = batch.freeze_config(draft, frozen)
            config, observed = batch.load_config(frozen)
            self.assertEqual(digest, observed)
            self.assertEqual(config["expected_route_count"], 24)
            (frozen / "batch-config.json").write_bytes(b"{}\n")
            with self.assertRaisesRegex(ValueError, "checksum"):
                batch.load_config(frozen)

    def test_config_rejects_unfrozen_path_and_extra_keys(self) -> None:
        config = batch._read(DRAFTS / "dev-24.draft.json")
        for path in ("../private", "/tmp/private", "C:/private"):
            changed = copy.deepcopy(config)
            changed["admission_dir"] = path
            with self.subTest(path=path), self.assertRaises(ValueError):
                batch._validate_config(changed)
        config["api_key"] = "not-a-real-key"
        with self.assertRaisesRegex(ValueError, "keys differ"):
            batch._validate_config(config)

        config.pop("api_key")
        config["task_timeout_seconds"] = 300
        with self.assertRaisesRegex(ValueError, "900s floor"):
            batch._validate_config(config)

    def test_both_historical_admissions_cover_the_frozen_plan(self) -> None:
        for draft in ("dev-24.draft.json", "sealed-28.draft.json"):
            with self.subTest(draft=draft):
                config, context, _ = _fixture(draft)
                with patch.object(
                    batch, "verify_interleaved_runtime_admission",
                    return_value=context["report"],
                ) as verifier:
                    loaded = batch.load_inputs(config, ARTIFACTS)
                self.assertEqual(
                    loaded["plan"]["question_count"],
                    config["expected_question_count"],
                )
                self.assertEqual(
                    len(loaded["trials"]), config["expected_route_count"],
                )
                verifier.assert_called_once()

    def test_existing_24_and_28_route_outputs_verify_unchanged(self) -> None:
        outputs = {
            "dev-24.draft.json": (
                ARTIFACTS / "multiq-24route-20260923t2250z"
            ),
            "sealed-28.draft.json": (
                ARTIFACTS / "multiq-sealed-28route-20260924t080919z-c7df0003"
                / "routes"
            ),
        }
        for draft, output in outputs.items():
            with self.subTest(draft=draft):
                config, context, _ = _fixture(draft)
                result = batch.verify_output(
                    config, "test-config-digest", context, output,
                )
                self.assertEqual(
                    result["route_count"], config["expected_route_count"],
                )
                self.assertEqual(
                    set(result["success_by_arm"]), {"R", "D", "DC", "I"},
                )

    def test_current_output_format_detects_timing_tamper(self) -> None:
        config, context, _ = _fixture("sealed-28.draft.json")
        old = ARTIFACTS / (
            "multiq-sealed-28route-20260924t080919z-c7df0003/routes"
        )
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            output = Path(temporary) / "output"
            shutil.copytree(old, output)
            start = batch._read(output / "start.json")
            summary = batch._read(output / "summary.json")
            start["config_sha256"] = "test-config-digest"
            summary["config_sha256"] = "test-config-digest"
            summary["status"] = "VERIFIED_INTERLEAVED_BATCH_EXECUTION"
            (output / "start.json").write_bytes(batch._pretty(start))
            (output / "summary.json").write_bytes(batch._pretty(summary))
            _refresh_checksums(output)
            result = batch.verify_output(
                config, "test-config-digest", context, output,
            )
            self.assertEqual(result["route_count"], 28)
            timing = batch._read(output / "timing-01.json")
            timing["route_started_utc"] = "2026-09-24T08:00:00+00:00"
            (output / "timing-01.json").write_bytes(batch._pretty(timing))
            _refresh_checksums(output)
            with self.assertRaisesRegex(ValueError, "not serial"):
                batch.verify_output(
                    config, "test-config-digest", context, output,
                )

    def test_execute_rejects_used_output_before_flowmesh(self) -> None:
        config = batch._read(DRAFTS / "dev-24.draft.json")
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            with patch.object(batch, "SdkFlowMeshClient") as client:
                with self.assertRaisesRegex(ValueError, "unused"):
                    batch.run(
                        config, "test-config-digest", {},
                        Path(temporary), execute=True,
                    )
                client.assert_not_called()

    def test_freeze_inputs_rejects_existing_targets_before_rebuild(self) -> None:
        config = batch._read(DRAFTS / "dev-24.draft.json")
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            (root / config["admission_dir"]).mkdir(parents=True)
            with patch.object(batch, "verify_interleaved_plan") as verifier:
                with self.assertRaisesRegex(ValueError, "already exists"):
                    batch.freeze_inputs(config, root)
                verifier.assert_not_called()

    def test_freeze_inputs_reuses_canonical_three_stage_pipeline(self) -> None:
        config = batch._validate_config(
            batch._read(DRAFTS / "dev-24.draft.json")
        )
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            root = Path(temporary)
            plan_dir = root / config["source_dirs"]["plan_dir"]
            plan_dir.mkdir(parents=True)
            plan = {
                "plan_sha256": config["expected_plan_sha256"],
                "public_source_sha256": "b" * 64,
                "question_count": 6, "route_count": 24,
            }
            (plan_dir / "interleaved-plan.json").write_bytes(batch._pretty(plan))
            (plan_dir / "public-questions.jsonl").write_bytes(b"{}\n")
            bound = {"route_count": 24}
            dags = {"trial_count": 24}
            admitted = {"trial_count": 24, "admission_sha256": "a" * 64}
            with (
                patch.object(batch, "verify_interleaved_plan",
                             return_value=plan),
                patch.object(batch, "freeze_interleaved_route_bindings",
                             return_value=bound) as freeze_binding,
                patch.object(batch, "verify_interleaved_route_bindings",
                             return_value=bound),
                patch.object(batch, "freeze_interleaved_trial_dags",
                             return_value=dags) as freeze_dag,
                patch.object(batch, "verify_interleaved_trial_dags",
                             return_value=dags),
                patch.object(batch, "freeze_interleaved_runtime_admission",
                             return_value=admitted) as freeze_admission,
                patch.object(batch, "verify_interleaved_runtime_admission",
                             return_value=admitted),
            ):
                result = batch.freeze_inputs(config, root)
            self.assertEqual(result["status"],
                             "FROZEN_INTERLEAVED_BATCH_INPUTS")
            freeze_binding.assert_called_once()
            freeze_dag.assert_called_once()
            freeze_admission.assert_called_once()

    def test_runner_uses_frozen_count_with_no_network(self) -> None:
        config = batch._read(DRAFTS / "dev-24.draft.json")
        trials = [{"trial_key": f"test-{i}", "design_id": "R"}
                  for i in range(24)]
        context = {
            "report": {"admission_sha256": "a" * 64},
            "trials": trials, "stages": [], "episodes": {},
            "routes": {trial["trial_key"]: {"run_id": "test-run"}
                       for trial in trials},
            "baseline_sha256": None,
        }

        class FakeExecutor:
            def __init__(self, **_kwargs: object) -> None:
                pass

            def execute(self, *, trial: dict,
                        idempotency_key: str) -> dict:
                return {
                    "status": "COMPLETE", "trial_key": trial["trial_key"],
                    "idempotency_key": idempotency_key,
                }

        worker = SimpleNamespace(
            alias=config["worker_alias"],
            node_alias=config["worker_node_alias"],
            status="IDLE", worker_id="test-worker",
        )
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
            output = Path(temporary) / "new-batch"
            with (
                patch.object(batch, "_settings", return_value=object()),
                patch.object(batch, "SdkFlowMeshClient") as client,
                patch.object(batch, "describe_pinned_worker",
                             return_value=worker),
                patch.object(batch, "FlowMeshSemanticTrialExecutor",
                             FakeExecutor),
                patch.object(batch, "full_flow_hmac_header_provider",
                             return_value=object()),
                patch.object(batch, "_assert_public_evidence"),
                patch.dict(os.environ, {
                    "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": "test-only",
                }),
            ):
                with redirect_stdout(StringIO()):
                    result = batch.run(
                        config, "test-config-digest", context, output,
                        execute=True,
                    )
            self.assertEqual(result["route_count"], 24)
            self.assertEqual(result["status"], "ALL_ROUTES_COMPLETE")
            self.assertEqual(len(list(output.glob("route-*.json"))), 24)
            self.assertEqual(len(list(output.glob("timing-*.json"))), 24)
            self.assertEqual(
                batch._read(output / "summary.json")["config_sha256"],
                "test-config-digest",
            )
            client.return_value.close.assert_called_once()


def _refresh_checksums(root: Path) -> None:
    payload = "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in sorted(root.iterdir()) if path.name != "SHA256SUMS"
    ).encode("ascii")
    (root / "SHA256SUMS").write_bytes(payload)


if __name__ == "__main__":
    unittest.main()
