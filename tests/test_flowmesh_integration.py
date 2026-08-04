from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from pathfinder.config import load_config
from pathfinder.integrations.flowmesh.adapter import (
    FlowMeshAgentAdapter,
    extract_agent_answer,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
    SubmittedWorkflow,
    TerminalWorkflow,
)
from pathfinder.integrations.flowmesh.gateway import (
    AccessGateway,
    EmulatedRepresentationBackend,
    SQLiteSessionStore,
)
from pathfinder.integrations.flowmesh.workflow import build_agent_workflow


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "minimal_system.json"


class FakeFlowMeshClient:
    def __init__(self) -> None:
        self.submitted_workflow: Mapping[str, Any] | None = None

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        self.submitted_workflow = workflow
        return SubmittedWorkflow(
            workflow_id="wfl-test",
            task_ids=("tsk-test",),
        )

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        return TerminalWorkflow(workflow_id=workflow_id, status="DONE")

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        return {
            "task_id": task_id,
            "result": {
                "ok": True,
                "items": [
                    {
                        "index": 0,
                        "output": "The car enters at 00:17.",
                    }
                ],
            },
        }


class FlowMeshIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(CONFIG_PATH)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.store = SQLiteSessionStore(
            Path(self.temporary_directory.name) / "gateway.sqlite3"
        )
        self.gateway = AccessGateway(
            self.config,
            self.store,
            EmulatedRepresentationBackend(sleep=lambda _: None),
        )

    def request(
        self,
        *,
        quote_profile_id: str = "digest_low",
        trial_id: str = "integration-test",
    ) -> FlowMeshAgentRunRequest:
        return FlowMeshAgentRunRequest(
            question="When does the red car enter the tunnel?",
            design_id="D_structured_digest",
            task_class_id="video_qa",
            quote_profile_id=quote_profile_id,
            seed=42,
            trial_id=trial_id,
        )

    def test_gateway_lists_only_agent_visible_offer_fields(self) -> None:
        session = self.gateway.register_session(self.request())
        state = self.gateway.list_offers(session.session_id)
        digest = next(
            offer
            for offer in state["offers"]
            if offer["representation_id"] == "multimodal_digest"
        )
        self.assertEqual(2.0, digest["quoted_price"])
        self.assertTrue(digest["affordable"])
        self.assertNotIn("realized_cost", digest)
        self.assertNotIn("expected_latency_ms", digest)
        self.assertNotIn("task_quality", digest)

    def test_gateway_enforces_budget_and_access_limit(self) -> None:
        session = self.gateway.register_session(self.request())
        first = self.gateway.access_representation(
            session.session_id,
            "multimodal_digest",
        )
        second = self.gateway.access_representation(
            session.session_id,
            "sampled_frames",
        )
        self.assertTrue(first["ok"])
        self.assertEqual(2.0, first["quoted_price_charged"])
        self.assertEqual(4.0, first["remaining_budget"])
        self.assertIn("felt_latency_ms", first)
        self.assertNotIn("realized_cost", first)
        self.assertFalse(second["ok"])
        self.assertEqual("access_limit_reached", second["error"])

        events = self.store.list_events(session.session_id)
        self.assertEqual(2, len(events))
        self.assertTrue(events[0].accepted)
        self.assertGreater(events[0].realized_cost, 0.0)
        self.assertFalse(events[1].accepted)

    def test_emulated_backend_imposes_felt_latency(self) -> None:
        slept_seconds: list[float] = []
        gateway = AccessGateway(
            self.config,
            self.store,
            EmulatedRepresentationBackend(sleep=slept_seconds.append),
        )
        session = gateway.register_session(
            self.request(trial_id="felt-latency")
        )
        result = gateway.access_representation(
            session.session_id,
            "compressed_video",
        )
        self.assertTrue(result["ok"])
        self.assertEqual(1, len(slept_seconds))
        self.assertAlmostEqual(
            result["felt_latency_ms"] / 1_000.0,
            slept_seconds[0],
        )

    def test_high_quote_is_rejected_as_insufficient_budget(self) -> None:
        session = self.gateway.register_session(
            self.request(
                quote_profile_id="digest_high",
                trial_id="high-quote",
            )
        )
        result = self.gateway.access_representation(
            session.session_id,
            "multimodal_digest",
        )
        self.assertFalse(result["ok"])
        self.assertEqual("insufficient_budget", result["error"])

    def test_workflow_uses_agent_config_and_session_tools(self) -> None:
        request = self.request()
        settings = FlowMeshSettings(agent_config_name="pathfinder_video")
        workflow = build_agent_workflow(
            "session-123",
            request,
            settings,
        )
        self.assertEqual("agent", workflow["spec"]["taskType"])
        self.assertEqual(
            "pathfinder_video",
            workflow["spec"]["configName"],
        )
        self.assertIn("session-123", workflow["spec"]["task"])
        self.assertIn("list_offers", workflow["spec"]["task"])

    def test_adapter_submits_waits_and_extracts_result(self) -> None:
        client = FakeFlowMeshClient()
        result = FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(),
        ).run(self.request())
        self.assertEqual("DONE", result.status)
        self.assertEqual("wfl-test", result.workflow_id)
        self.assertEqual("tsk-test", result.task_id)
        self.assertEqual(
            "The car enters at 00:17.",
            result.final_answer,
        )
        session = self.store.get_session(result.session_id)
        self.assertEqual("DONE", session.status)
        self.assertEqual("wfl-test", session.flowmesh_workflow_id)
        self.assertIsNotNone(client.submitted_workflow)

    def test_result_parser_accepts_direct_result_shape(self) -> None:
        answer = extract_agent_answer(
            {"items": [{"response": "Direct response"}]}
        )
        self.assertEqual("Direct response", answer)


if __name__ == "__main__":
    unittest.main()
