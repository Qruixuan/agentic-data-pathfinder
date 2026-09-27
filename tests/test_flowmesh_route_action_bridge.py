"""Offline connector checks against the public 400-route schedule."""

from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from pathfinder.integrations.flowmesh.route_action_bridge import (
    CacheObservation, RouteActionBridge, RouteActionBridgeError,
    RouteQuote,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN = (ROOT / "artifacts" / "nextqa-atphard-8x5-plan-20260925-v1")
BASIS = "a" * 64  # Synthetic test basis; not an experimental price.


class RouteActionBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "choices.sqlite3"
        self.bridge = RouteActionBridge(
            PLAN, self.db, price_basis_sha256=BASIS,
            execution_namespace="test-fresh-bridge",
        )
        self.schedule = [json.loads(line) for line in (
            PLAN / "ten-route-multiq-schedule.jsonl"
        ).read_bytes().splitlines()]
        self.row = self.schedule[0]
        self.question_id = self.row["question_id"]
        self.quotes = {
            (f"D{number}", state): RouteQuote(
                incremental_usd="0.001", expected_latency_ms=100,
                price_basis_sha256=BASIS, source_trace_count=2,
            )
            for number in range(8)
            for state in (("miss", "hit") if number in (3, 7) else (None,))
        }

    def cache(self, state: str = "miss", row=None
              ) -> dict[str, CacheObservation]:
        row = self.row if row is None else row
        result = {}
        for node, action in (("N7", "D3"), ("N8", "D7")):
            result[node] = CacheObservation(
                cache_episode_id=self.bridge.cache_episode_ids(
                    row["question_id"]
                )[node], state=state,
                evidence_sha256="b" * 64,
            )
        return result

    def offers(self, design: str, state: str = "miss"):
        return self.bridge.list_route_offers(
            question_id=self.question_id, physical_design_id=design,
            quotes=self.quotes, cache=self.cache(state),
        ).offers

    def commit(self, *, session_id: str, design: str, action_id: str,
               state: str = "miss"):
        offer_set = self.bridge.list_route_offers(
            question_id=self.question_id, physical_design_id=design,
            quotes=self.quotes, cache=self.cache(state),
        )
        return self.bridge.commit_choice(
            session_id=session_id, question_id=self.question_id,
            physical_design_id=design, action_id=action_id,
            offer_set_sha256=offer_set.offer_set_sha256,
            quotes=self.quotes, cache=self.cache(state),
        )

    def test_400_public_observations_reduce_to_eight_actions(self):
        self.assertEqual(len(self.schedule), 40)
        self.assertEqual(sum(len(row["route_slots"])
                             for row in self.schedule), 400)
        for row in self.schedule:
            with self.subTest(question_id=row["question_id"]):
                offer_set = self.bridge.list_route_offers(
                    question_id=row["question_id"],
                    physical_design_id="D_joint", quotes=self.quotes,
                    cache=self.cache(row=row),
                )
                offers = offer_set.offers
                self.assertEqual(
                    [offer.action_id for offer in offers],
                    [f"D{number}" for number in range(8)],
                )
                self.assertEqual(
                    [offer.executor_node_id for offer in offers],
                    ["N7"] * 4 + ["N8"] * 4,
                )
        self.assertEqual(
            [offer.action_id for offer in self.offers("D_base")],
            ["D0", "D2", "D4", "D6"],
        )
        self.assertEqual(len(self.offers("D_index")), 6)
        self.assertEqual(len(self.offers("D_cache")), 6)

    def test_cache_state_selects_slot_not_a_ninth_action(self):
        for state in ("miss", "hit"):
            offers = self.offers("D_joint", state)
            self.assertEqual(len(offers), 8)
            self.assertEqual([offer.cache_state for offer in offers
                              if offer.arm_id == "DC"], [state, state])
            choice = self.commit(
                session_id=f"session-{state}", design="D_joint",
                action_id="D3", state=state,
            )
            self.assertEqual(state, choice.cache_state)
            self.assertFalse(any(
                choice.run_id == slot["run_id"]
                for slot in self.row["route_slots"]
            ))
            self.assertEqual(choice.cache_episode_id,
                             self.cache(state)["N7"].cache_episode_id)

    def test_fresh_namespace_reuses_cache_per_video_not_per_question(self):
        same_video = next(row for row in self.schedule
                          if row["object_id"] == self.row["object_id"]
                          and row["question_id"] != self.question_id)
        other_video = next(row for row in self.schedule
                           if row["object_id"] != self.row["object_id"])
        self.assertEqual(
            self.bridge.cache_episode_ids(self.question_id),
            self.bridge.cache_episode_ids(same_video["question_id"]),
        )
        self.assertNotEqual(
            self.bridge.cache_episode_ids(self.question_id),
            self.bridge.cache_episode_ids(other_video["question_id"]),
        )
        another = RouteActionBridge(
            PLAN, Path(self.temp.name) / "other-choices.sqlite3",
            price_basis_sha256=BASIS,
            execution_namespace="another-fresh-experiment",
        )
        self.assertNotEqual(
            self.bridge.cache_episode_ids(self.question_id),
            another.cache_episode_ids(self.question_id),
        )
        self.assertNotEqual(
            self.bridge.list_route_offers(
                question_id=self.question_id, physical_design_id="D_base",
                quotes=self.quotes,
            ).offer_set_sha256,
            another.list_route_offers(
                question_id=self.question_id, physical_design_id="D_base",
                quotes=self.quotes,
            ).offer_set_sha256,
        )
        choice = self.commit(
            session_id="fresh-identity", design="D_base", action_id="D0",
        )
        self.assertFalse(any(choice.run_id == slot["run_id"]
                             for row in self.schedule
                             for slot in row["route_slots"]))

    def test_commit_is_durable_idempotent_and_single_choice(self):
        args = dict(
            session_id="one-session", question_id=self.question_id,
            physical_design_id="D_joint", action_id="D0",
            offer_set_sha256=self.bridge.list_route_offers(
                question_id=self.question_id,
                physical_design_id="D_joint", quotes=self.quotes,
                cache=self.cache(),
            ).offer_set_sha256,
            quotes=self.quotes, cache=self.cache(),
        )
        choice = self.bridge.commit_choice(**args)
        reopened = RouteActionBridge(
            PLAN, self.db, price_basis_sha256=BASIS,
            execution_namespace="test-fresh-bridge",
        )
        self.assertEqual(choice, reopened.commit_choice(**args))
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "different choice"):
            reopened.commit_choice(**{**args, "action_id": "D2"})
        with self.assertRaisesRegex(RouteActionBridgeError, "stale offer"):
            reopened.commit_choice(**{
                **args, "quotes": {
                    **self.quotes,
                    ("D0", None): replace(
                        self.quotes[("D0", None)], incremental_usd="0.002",
                    ),
                },
            })

    def test_unavailable_or_unpriced_action_fails_closed(self):
        with self.assertRaisesRegex(RouteActionBridgeError, "not offered"):
            self.bridge.commit_choice(
                session_id="bad-choice", question_id=self.question_id,
                physical_design_id="D_base", action_id="D1",
                offer_set_sha256=self.bridge.list_route_offers(
                    question_id=self.question_id,
                    physical_design_id="D_base", quotes=self.quotes,
                ).offer_set_sha256, quotes=self.quotes,
            )
        with self.assertRaisesRegex(RouteActionBridgeError, "no measured"):
            self.bridge.list_route_offers(
                question_id=self.question_id,
                physical_design_id="D_base",
                quotes={key: value for key, value in self.quotes.items()
                        if key != ("D0", None)},
            )
        with self.assertRaisesRegex(RouteActionBridgeError, "stale"):
            bad = replace(self.quotes[("D0", None)],
                          price_basis_sha256="c" * 64)
            self.bridge.list_route_offers(
                question_id=self.question_id,
                physical_design_id="D_base",
                quotes={**self.quotes, ("D0", None): bad},
            )

    def test_cache_observation_must_match_episode(self):
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "trusted state observation"):
            self.bridge.list_route_offers(
                question_id=self.question_id,
                physical_design_id="D_cache", quotes=self.quotes,
            )
        cache = self.cache()
        cache["N7"] = replace(cache["N7"], cache_episode_id="wrong")
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "different episode"):
            self.bridge.list_route_offers(
                question_id=self.question_id,
                physical_design_id="D_cache", quotes=self.quotes,
                cache=cache,
            )

    def test_cache_state_change_invalidates_agent_view(self):
        cold = self.bridge.list_route_offers(
            question_id=self.question_id,
            physical_design_id="D_cache", quotes=self.quotes,
            cache=self.cache("miss"),
        )
        with self.assertRaisesRegex(RouteActionBridgeError, "stale offer"):
            self.bridge.commit_choice(
                session_id="changed-cache", question_id=self.question_id,
                physical_design_id="D_cache", action_id="D3",
                offer_set_sha256=cold.offer_set_sha256,
                quotes=self.quotes, cache=self.cache("hit"),
            )

    def test_handoff_binds_exact_existing_trial_without_submission(self):
        choice = self.commit(
            session_id="handoff-session", design="D_joint", action_id="D1",
        )
        trial = {
            "flowmesh_submission_authorized": True,
            "trial_key": choice.trial_key,
            "design_id": "D1",
            "repetition": 0,
            "executor_node_id": "N7",
            "route_family": "indexed-raw",
            "artifact_object_id": self.row["object_id"],
            "public_task_binding_sha256": self.row["public_task_sha256"],
            "route_coordinator_binding": {
                "service_contract_id": "N7.execution-compute",
            },
        }
        handoff = self.bridge.handoff(choice, trial)
        self.assertEqual(handoff.run_id, choice.run_id)
        self.assertEqual(handoff.trial, trial)
        self.assertEqual(handoff.choice_sha256, choice.choice_sha256)
        self.assertEqual(len(handoff.idempotency_key), 64)
        for change in (
            {"executor_node_id": "N8"},
            {"artifact_object_id": "wrong-object"},
            {"route_family": "raw"},
            {"flowmesh_submission_authorized": False},
        ):
            with self.subTest(change=change):
                with self.assertRaisesRegex(RouteActionBridgeError,
                                            "admitted trial differs"):
                    self.bridge.handoff(choice, {**trial, **change})
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "not durably committed"):
            self.bridge.handoff(replace(choice, action_id="D2"), trial)

    def test_handoff_accepts_an_existing_admitted_trial_offline(self):
        # This second historical fixture tests the real admission shape.
        # Its run identity is already used and is never dispatched here.
        root = ROOT / "artifacts" / "t60-final-input-v1" / "plan"
        admission = (ROOT / "artifacts" / "t60-runtime-v1" / "admission"
                     / "admitted-trials.jsonl")
        bridge = RouteActionBridge(
            root, Path(self.temp.name) / "t60-choices.sqlite3",
            price_basis_sha256=BASIS,
            execution_namespace="test-t60-fresh",
        )
        row = json.loads((root / "ten-route-multiq-schedule.jsonl")
                         .read_bytes().splitlines()[0])
        question_id = row["question_id"]
        offered = bridge.list_route_offers(
            question_id=question_id, physical_design_id="D_index",
            quotes=self.quotes,
        )
        choice = bridge.commit_choice(
            session_id="historical-offline-only", question_id=question_id,
            physical_design_id="D_index", action_id="D1",
            offer_set_sha256=offered.offer_set_sha256,
            quotes=self.quotes,
        )
        trial = next(json.loads(line) for line in admission.read_bytes()
                     .splitlines() if json.loads(line)["trial_key"]
                     == choice.trial_key)
        handoff = bridge.handoff(choice, trial)
        self.assertEqual(handoff.trial["trial_key"], choice.trial_key)
        self.assertEqual(handoff.run_id, choice.run_id)


if __name__ == "__main__":
    unittest.main()
