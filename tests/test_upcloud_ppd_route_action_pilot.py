"""Focused no-network coverage for the choice-to-route PPD runner."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from types import SimpleNamespace
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from experiments.upcloud_ppd_20260925 import run_route_action_pilot as pilot


SESSION = {
    "session_id": "fresh-session",
    "question_id": "public-q1",
    "physical_design_id": "D_joint",
    "object_id": "public-object",
    "public_task_sha256": "a" * 64,
}
TRIAL = {"trial_key": "public-trial"}


@dataclass
class FakeChoice:
    action_id: str = "D1"
    choice_sha256: str = "b" * 64
    offer_set_sha256: str = "c" * 64
    trial_key: str = "public-trial"


class FakeGateway:
    def __init__(self) -> None:
        self.registered = []
        self.bridge = SimpleNamespace(load_choice=lambda _: FakeChoice())

    def register_session(self, **fields):
        self.registered.append(fields)

    def handoff_for_session(self, session_id, trial):
        assert session_id == SESSION["session_id"]
        assert trial == TRIAL
        return SimpleNamespace(
            run_id="fresh-route", cache_episode_id=None,
            trial=trial, idempotency_key="d" * 64,
        )


class FakeClient:
    def __init__(self, *, valid=True) -> None:
        self.valid = valid
        self.submissions = []

    def validate(self, workflow):
        return SimpleNamespace(ok=self.valid)

    def submit(self, workflow):
        self.submissions.append(workflow)
        return SimpleNamespace(workflow_id="choice-workflow",
                               task_ids=("choice-task",))

    def wait(self, workflow_id, poll):
        return SimpleNamespace(status="DONE", workflow_id=workflow_id)

    def describe_task_failure(self, task_id):
        return {"assigned_worker": "choice-worker-id"}


class FakeRouteExecutor:
    settings_seen = None

    def __init__(self, **fields):
        type(self).settings_seen = fields["settings"].worker_alias

    def execute(self, *, trial, idempotency_key):
        assert trial == TRIAL and idempotency_key == "d" * 64
        return {
            "status": "COMPLETE", "credentials_recorded": False,
            "n1_score_authenticity_verified": True,
            "task_success": False,
            "route_evidence_sha256": "e" * 64,
            "n1_score_evidence_sha256": "f" * 64,
            "execution_transport": "flowmesh",
        }


def _settings(alias):
    return SimpleNamespace(worker_alias=alias, poll_interval_seconds=1,
                           task_timeout_seconds=900)


def _run(tmp_path: Path, client: FakeClient, *, route_alias="route-alias"):
    gateway = FakeGateway()
    with (patch.object(pilot, "build_route_choice_workflow",
                       return_value={"graph": "public-choice"}),
          patch.object(pilot, "FlowMeshSemanticTrialExecutor",
                       FakeRouteExecutor)):
        result = pilot.execute_one(
            gateway=gateway, client=client,
            choice_settings=_settings("choice-alias"),
            route_settings=_settings(route_alias),
            session=SESSION, bound_trials=[TRIAL], bound_stages=[],
            runtime_header_provider=lambda _: {},
            output_dir=tmp_path / "one", choice_worker_id="choice-worker-id",
            route_worker_id="route-worker-id",
        )
    return result, gateway


class RouteActionPilotTests(unittest.TestCase):
    def test_worker_turn_ceiling_matches_frozen_provider_budget(self):
        root = Path(__file__).resolve().parents[1]
        protocol = json.loads((root / "experiments/upcloud_ppd_20260925"
                               / "execution-protocol.physical-path-pilot-"
                                 "20260928.json").read_text())
        config = (root / "integrations/flowmesh/agent_configs"
                  / "pathfinder_route_action_qwen_first_offer.yaml").read_text()
        match = re.search(r"^max_turns:\s*(\d+)$", config, re.MULTILINE)
        self.assertIsNotNone(match)
        self.assertEqual(int(match.group(1)), protocol[
            "agent_provider_attempts_per_choice_ceiling"
        ])

    def test_one_committed_action_runs_once_on_separate_route_worker(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            client = FakeClient()
            result, gateway = _run(root, client)
            self.assertEqual(result["status"], "COMPLETE")
            self.assertEqual(result["action_id"], "D1")
            self.assertIs(result["task_success"], False)
            self.assertEqual(FakeRouteExecutor.settings_seen, "route-alias")
            self.assertEqual(len(client.submissions), 1)
            self.assertEqual(gateway.registered, [SESSION])
            self.assertTrue((root / "one/choice-submitted.json").is_file())
            self.assertFalse((root / "one/stopped.json").exists())
            receipt = json.loads((root / "one/complete.json").read_text())
            self.assertNotIn("answer", receipt)
            self.assertIs(receipt["credentials_recorded"], False)

    def test_choice_validation_failure_never_submits(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            client = FakeClient(valid=False)
            with self.assertRaisesRegex(RuntimeError, "validation failed"):
                _run(root, client)
            self.assertEqual(client.submissions, [])
            stopped = json.loads((root / "one/stopped.json").read_text())
            self.assertEqual(stopped["phase"], "choice_validation")
            self.assertEqual(stopped["status"], "STOPPED_NO_RETRY")

    def test_reusing_output_identity_is_rejected_before_registration(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            _run(root, FakeClient())
            with self.assertRaisesRegex(RuntimeError, "already used"):
                _run(root, FakeClient())

    def test_route_worker_cannot_silently_equal_choice_worker(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "must differ"):
                _run(root, FakeClient(), route_alias="choice-alias")
            self.assertFalse((root / "one").exists())

    def test_sealed_gate_requires_exact_checksum_and_ready_status(self):
        with TemporaryDirectory() as directory:
            gate = Path(directory) / "gate"
            gate.mkdir()
            payload = {
                "status": "READY_FOR_FIRST_FORMAL_SESSION",
                "experiment_id": (
                    "pathfinder-ppd-physical-path-pilot-20260928-v1"
                ),
                "workflow_submitted": False,
                "credentials_recorded": False,
                "hidden_labels_included": False,
            }
            raw = (json.dumps(payload) + "\n").encode()
            (gate / "pre-submit-gates.json").write_bytes(raw)
            (gate / "SHA256SUMS").write_text(
                hashlib.sha256(raw).hexdigest()
                + "  pre-submit-gates.json\n",
                encoding="ascii", newline="\n",
            )
            self.assertEqual(pilot._sealed_ready(gate), payload)
            (gate / "pre-submit-gates.json").write_bytes(b"{}\n")
            with self.assertRaisesRegex(RuntimeError, "checksum differs"):
                pilot._sealed_ready(gate)


if __name__ == "__main__":
    unittest.main()
