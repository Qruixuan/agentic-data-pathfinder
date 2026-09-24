"""Offline episode replay refuses unmeasured cache and cost states."""

import unittest

from experiments.ten_route_multiq_replay import (
    TenRouteEpisodeReplay, fixed_policy, no_eviction_admission_policy,
    run_policy,
)


NS = "test-episode"
DIGEST = "multimodal_digest"
FRAMES = "sampled_frame_bundle"


def artifacts():
    return {
        "A": {
            DIGEST: {"sha256": "a" * 64, "size_bytes": 2},
            FRAMES: {"sha256": "b" * 64, "size_bytes": 7},
        },
        "B": {
            DIGEST: {"sha256": "c" * 64, "size_bytes": 2},
            FRAMES: {"sha256": "d" * 64, "size_bytes": 1},
        },
    }


def question(ordinal, qid, oid):
    return {"ordinal": ordinal, "question_id": qid,
            "object_id": oid, "stratum": "temporal"}


def event(rep, hit, evicted=None):
    result = [{"kind": "lookup", "representation_id": rep,
               "hit": hit}]
    if not hit:
        result.append({"kind": "store", "representation_id": rep,
                       "evicted": evicted or []})
    return result


def outcome(qid, oid, action, suffix, events, cost="1"):
    return {
        "question_id": qid, "object_id": oid, "action_id": action,
        "outcome_id": f"{qid}-{action}-{suffix}",
        "cache_events": events,
        "task_success": True,
        "route_cost_usd": cost,
        "elapsed_ms": 100,
    }


def evaluator(schedule, observations, capacity=10):
    return TenRouteEpisodeReplay(
        schedule=schedule, observations=observations,
        artifact_by_object=artifacts(),
        build_cost_by_object={
            oid: {"raw": "0.1", "caption": "0.5", "derived": "0",
                  "index": "0.2"}
            for oid in ("A", "B")
        },
        capacity_by_node={"N7": capacity, "N8": capacity},
        namespace=NS,
    )


class TenRouteMultiqReplayTests(unittest.TestCase):
    def test_cold_then_warm_charges_build_once(self):
        schedule = [question(0, "a1", "A"), question(1, "a2", "A")]
        measured = [
            outcome("a1", "A", "N7/DC", "miss",
                    event(DIGEST, False) + event(FRAMES, False)),
            outcome("a2", "A", "N7/DC", "hit",
                    event(DIGEST, True) + event(FRAMES, True)),
        ]
        episode = evaluator(schedule, measured)
        public = episode.observation()
        self.assertNotIn("remaining_queries", public)
        self.assertNotIn("future_question_ids", public)
        self.assertNotIn("task_success", public)
        result = run_policy(episode, fixed_policy("DC"))
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["complete_list_cost_usd"], "2.600000000")
        self.assertEqual(result["steps"][0]["newly_built_components"],
                         ["raw", "caption", "derived"])
        self.assertEqual(result["steps"][1]["newly_built_components"], [])

    def test_unmeasured_partial_state_does_not_advance(self):
        schedule = [question(0, "a1", "A"), question(1, "b1", "B"),
                    question(2, "a2", "A")]
        measured = [
            outcome("a1", "A", "N7/DC", "miss",
                    event(DIGEST, False) + event(FRAMES, False)),
            outcome("b1", "B", "N7/DC", "miss",
                    event(DIGEST, False, [["A", DIGEST]])
                    + event(FRAMES, False)),
            # An all-hit A control cannot stand in for A's partial state.
            outcome("a2", "A", "N7/DC", "hit",
                    event(DIGEST, True) + event(FRAMES, True)),
        ]
        episode = evaluator(schedule, measured)
        self.assertEqual(episode.step("N7/DC")["status"], "REPLAYED")
        self.assertEqual(episode.step("N7/DC")["status"], "REPLAYED")
        before = episode.observation()
        failed = episode.step("N7/DC")
        self.assertEqual(failed["status"], "UNSUPPORTED_ACTION")
        self.assertFalse(failed["state_changed"])
        self.assertEqual(episode.observation(), before)

    def test_partial_hit_can_be_matched_in_observed_event_order(self):
        schedule = [question(0, "a1", "A"), question(1, "b1", "B"),
                    question(2, "a2", "A")]
        measured = [
            outcome("a1", "A", "N7/DC", "miss",
                    event(DIGEST, False) + event(FRAMES, False)),
            outcome("b1", "B", "N7/DC", "miss",
                    event(DIGEST, False, [["A", DIGEST]])
                    + event(FRAMES, False)),
            outcome("a2", "A", "N7/DC", "partial", [
                {"kind": "lookup", "representation_id": DIGEST,
                 "hit": False},
                {"kind": "lookup", "representation_id": FRAMES,
                 "hit": True},
                {"kind": "store", "representation_id": DIGEST,
                 "evicted": [["B", DIGEST]]},
            ]),
        ]
        result = run_policy(evaluator(schedule, measured), fixed_policy("DC"))
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["correct"], 3)
        self.assertEqual(result["steps"][-1]["outcome_id"],
                         "a2-N7/DC-partial")

    def test_capacity_aware_policy_bypasses_eviction(self):
        schedule = [question(0, "a1", "A"), question(1, "b1", "B")]
        measured = [
            outcome("a1", "A", "N7/DC", "miss",
                    event(DIGEST, False) + event(FRAMES, False)),
            outcome("b1", "B", "N7/D", "bypass", []),
        ]
        result = run_policy(
            evaluator(schedule, measured), no_eviction_admission_policy(),
        )
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual([step["action_id"] for step in result["steps"]],
                         ["N7/DC", "N7/D"])

    def test_missing_cost_is_unknown_not_zero(self):
        schedule = [question(0, "a1", "A")]
        measured = [outcome("a1", "A", "N8/R", "raw", [], cost=None)]
        result = run_policy(evaluator(schedule, measured),
                            fixed_policy("R", node="N8"))
        self.assertEqual(result["status"], "COMPLETE")
        self.assertIsNone(result["complete_list_cost_usd"])
        self.assertEqual(result["known_list_cost_usd"], "0.100000000")
        self.assertEqual(result["missing_cost_fields"],
                         ["route/a1-N8/R-raw"])

    def test_legacy_build_cost_contract_remains_accepted(self):
        result = run_policy(TenRouteEpisodeReplay(
            schedule=[question(0, "a1", "A")],
            observations=[outcome("a1", "A", "N7/I", "index", [])],
            artifact_by_object=artifacts(),
            build_cost_by_object={
                "A": {"raw": "0.1", "index": "0.2", "derived": "0.5"}
            },
            capacity_by_node={"N7": 10, "N8": 10}, namespace=NS,
        ), fixed_policy("I"))
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["steps"][0]["newly_built_components"],
                         ["raw", "index"])

    def test_caption_build_is_shared_when_switching_d_to_i(self):
        episode = evaluator(
            [question(0, "a1", "A"), question(1, "a2", "A")],
            [outcome("a1", "A", "N7/D", "derived", []),
             outcome("a2", "A", "N7/I", "index", [])],
        )
        steps = [episode.step("N7/D"), episode.step("N7/I")]
        result = episode.result(steps)
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["complete_list_cost_usd"], "2.800000000")
        self.assertEqual(steps[1]["newly_built_components"], ["index"])


if __name__ == "__main__":
    unittest.main()
