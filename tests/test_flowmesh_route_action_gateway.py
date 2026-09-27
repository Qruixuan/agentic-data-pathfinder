"""Offline tool-boundary tests for physical route choice."""

import tempfile
from pathlib import Path
import unittest

from pathfinder.integrations.flowmesh.mcp_server import _build_server
from pathfinder.integrations.flowmesh.route_action_bridge import (
    CacheObservation, RouteActionBridge, RouteActionBridgeError, RouteQuote,
)
from pathfinder.integrations.flowmesh.route_action_gateway import (
    RouteActionGateway, RouteSession,
)
from pathfinder.integrations.flowmesh.route_action_workflow import (
    ROUTE_ACTION_AGENT_CONFIG, build_route_choice_workflow,
)
from pathfinder.integrations.flowmesh.contracts import FlowMeshSettings
from pathfinder.integrations.flowmesh.workflow import (
    workflow_selected_worker,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "artifacts" / "nextqa-atphard-8x5-plan-20260925-v1"
BASIS = "a" * 64  # Synthetic test basis only.


class Quotes:
    def quotes_for(self, _question_id, _design_id):
        return {
            (f"D{number}", state): RouteQuote(
                incremental_usd="0.001", expected_latency_ms=100,
                price_basis_sha256=BASIS, source_trace_count=2,
            )
            for number in range(8)
            for state in (("miss", "hit") if number in (3, 7) else (None,))
        }


class Cache:
    def observe(self, *, question_id, object_id, executor_node_id,
                cache_episode_id):
        del question_id, object_id, executor_node_id
        return CacheObservation(
            cache_episode_id=cache_episode_id, state="miss",
            evidence_sha256="b" * 64,
        )


class FastMCP:
    def __init__(self, *_args, **_kwargs):
        self.tools = {}

    def tool(self):
        def register(function):
            self.tools[function.__name__] = function
            return function
        return register


class RouteActionGatewayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.bridge = RouteActionBridge(
            PLAN, root / "choices.sqlite3", price_basis_sha256=BASIS,
            execution_namespace="test-fresh-gateway",
        )
        self.gateway = RouteActionGateway(
            bridge=self.bridge, session_db=root / "sessions.sqlite3",
            quote_source=Quotes(), cache_reader=Cache(),
        )
        self.question_id = "nextqa-val-4485498145-q2"
        self.object_id, self.task_sha = (
            self.bridge.public_question_identity(self.question_id)
        )

    def register(self, design="D_joint"):
        return self.gateway.register_session(
            session_id="route-session-1", question_id=self.question_id,
            physical_design_id=design, object_id=self.object_id,
            public_task_sha256=self.task_sha,
        )

    def test_session_binds_public_task_and_is_idempotent(self):
        session = self.register()
        self.assertEqual(session, self.register())
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "different task/design"):
            self.register("D_base")
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "differs from the verified"):
            self.gateway.register_session(
                session_id="bad-session", question_id=self.question_id,
                physical_design_id="D_base", object_id="wrong-object",
                public_task_sha256=self.task_sha,
            )

    def test_tool_uses_public_offers_and_commits_one_action(self):
        self.register()
        offer_set = self.gateway.list_route_offers("route-session-1")
        self.assertEqual(len(offer_set["offers"]), 8)
        self.assertEqual(offer_set["question_id"], self.question_id)
        self.assertNotIn("trial_key", str(offer_set))
        self.assertNotIn("run_id", str(offer_set))
        receipt = self.gateway.commit_route_choice(
            "route-session-1", "D1", offer_set["offer_set_sha256"],
        )
        self.assertEqual(receipt["status"], "COMMITTED")
        self.assertEqual(receipt["action_id"], "D1")
        self.assertEqual(self.gateway.commit_route_choice(
            "route-session-1", "D1", offer_set["offer_set_sha256"],
        ), receipt)
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "different choice"):
            self.gateway.commit_route_choice(
                "route-session-1", "D2", offer_set["offer_set_sha256"],
            )
        choice = self.bridge.load_choice("route-session-1")
        trial = {
            "flowmesh_submission_authorized": True,
            "trial_key": choice.trial_key,
            "design_id": "D1",
            "repetition": 0,
            "executor_node_id": "N7",
            "route_family": "indexed-raw",
            "artifact_object_id": self.object_id,
            "public_task_binding_sha256": self.task_sha,
            "route_coordinator_binding": {
                "service_contract_id": "N7.execution-compute",
            },
        }
        handoff = self.gateway.handoff_for_session(
            "route-session-1", trial,
        )
        self.assertEqual(handoff.trial, trial)
        self.assertEqual(handoff.choice_sha256, receipt["choice_sha256"])

    def test_cache_design_refuses_missing_reader(self):
        root = Path(self.temp.name)
        gateway = RouteActionGateway(
            bridge=self.bridge, session_db=root / "no-cache.sqlite3",
            quote_source=Quotes(),
        )
        gateway.register_session(
            session_id="no-cache", question_id=self.question_id,
            physical_design_id="D_cache", object_id=self.object_id,
            public_task_sha256=self.task_sha,
        )
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "verified live cache reader"):
            gateway.list_route_offers("no-cache")

    def test_live_gateway_rejects_unfrozen_sessions_and_runs(self):
        allowed = RouteSession(
            "route-session-1", self.question_id, "D_base",
            self.object_id, self.task_sha,
        )
        gateway = RouteActionGateway(
            bridge=self.bridge, session_db=self.gateway.session_db,
            quote_source=Quotes(), cache_reader=Cache(),
            admitted_sessions={allowed.session_id: allowed},
            admitted_runs={},
        )
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "frozen runtime admission"):
            gateway.register_session(
                session_id="not-precommitted", question_id=self.question_id,
                physical_design_id="D_base", object_id=self.object_id,
                public_task_sha256=self.task_sha,
            )
        gateway.register_session(
            session_id=allowed.session_id, question_id=self.question_id,
            physical_design_id="D_base", object_id=self.object_id,
            public_task_sha256=self.task_sha,
        )
        offers = gateway.list_route_offers(allowed.session_id)
        gateway.commit_route_choice(
            allowed.session_id, "D0", offers["offer_set_sha256"],
        )
        choice = self.bridge.load_choice(allowed.session_id)
        trial = {
            "flowmesh_submission_authorized": True,
            "trial_key": choice.trial_key,
            "design_id": "D0", "repetition": 0,
            "executor_node_id": "N7", "route_family": "raw",
            "artifact_object_id": self.object_id,
            "public_task_binding_sha256": self.task_sha,
            "route_coordinator_binding": {
                "service_contract_id": "N7.execution-compute",
            },
        }
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "absent from runtime admission"):
            gateway.handoff_for_session(allowed.session_id, trial)
        admitted = RouteActionGateway(
            bridge=self.bridge, session_db=self.gateway.session_db,
            quote_source=Quotes(), cache_reader=Cache(),
            admitted_sessions={allowed.session_id: allowed},
            admitted_runs={(choice.run_id, choice.trial_key): None},
        )
        self.assertEqual(choice.run_id, admitted.handoff_for_session(
            allowed.session_id, trial,
        ).run_id)

    def test_mcp_tools_are_opt_in_and_do_not_change_old_tools(self):
        config = ROOT / "configs" / "phase_b_causal_gate_system.json"
        root = Path(self.temp.name)
        plain = _build_server(
            None, config_path=config, state_db=root / "plain.sqlite3",
            host="127.0.0.1", port=8765, fast_mcp=FastMCP,
        )
        enabled = _build_server(
            None, config_path=config, state_db=root / "enabled.sqlite3",
            host="127.0.0.1", port=8766, fast_mcp=FastMCP,
            route_action_gateway=self.gateway,
        )
        self.assertEqual(
            set(enabled.tools) - set(plain.tools),
            {"list_route_offers", "commit_route_choice"},
        )
        self.register()
        listed = enabled.tools["list_route_offers"]("route-session-1")
        committed = enabled.tools["commit_route_choice"](
            "route-session-1", "D0", listed["offer_set_sha256"],
        )
        self.assertEqual(committed["status"], "COMMITTED")

    def test_agent_workflow_only_selects_and_uses_route_tools(self):
        self.register()
        settings = FlowMeshSettings(
            agent_config_name=ROUTE_ACTION_AGENT_CONFIG,
            worker_alias="pathfinder-dedicated-agent",
        )
        workflow = build_route_choice_workflow(
            gateway=self.gateway, session_id="route-session-1",
            settings=settings, selected_worker_id="wkr-observed-only",
        )
        spec = workflow["spec"]["graph"]["nodes"][0]["spec"]
        self.assertEqual(spec["configName"], ROUTE_ACTION_AGENT_CONFIG)
        self.assertEqual(workflow_selected_worker(workflow),
                         "wkr-observed-only")
        self.assertIn("list_route_offers", spec["task"])
        self.assertIn("commit_route_choice", spec["task"])
        self.assertNotIn("access_representation", spec["task"])
        self.assertNotIn("inspect_visual_artifact", spec["task"])
        self.assertEqual(spec["output"]["destination"], {"type": "http"})
        config = (ROOT / "integrations" / "flowmesh" / "agent_configs"
                  / f"{ROUTE_ACTION_AGENT_CONFIG}.yaml").read_text(
                      encoding="utf-8"
                  )
        self.assertIn("tool_choice: list_route_offers", config)
        self.assertIn("      - commit_route_choice", config)
        self.assertNotIn("      - access_representation", config)
        self.assertNotIn("      - inspect_visual_artifact", config)


if __name__ == "__main__":
    unittest.main()
