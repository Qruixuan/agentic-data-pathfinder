"""Read-only cache observation tests; no live services or credentials."""

import unittest

from pathfinder.integrations.flowmesh.route_action_bridge import (
    RouteActionBridgeError,
)
from pathfinder.integrations.flowmesh.route_action_cache_reader import (
    LiveCacheStatusReader,
)


class FakeClient:
    def __init__(self, node, states):
        self.node = node
        self.states = states
        self.calls = []

    def peek(self, **request):
        self.calls.append(request)
        return {
            **request,
            "status": self.states[request["representation_id"]],
            "node_id": self.node,
            "payload_included": False,
            "state_mutated": False,
        }


class LiveCacheStatusReaderTests(unittest.TestCase):
    def setUp(self):
        self.n7 = FakeClient("N7", {"multimodal_digest": "HIT",
                                    "sampled_frame_bundle": "HIT"})
        self.n8 = FakeClient("N8", {"multimodal_digest": "HIT",
                                    "sampled_frame_bundle": "MISS"})
        self.reader = LiveCacheStatusReader(
            clients={"N7": self.n7, "N8": self.n8},
            expected_artifacts={"object-1": {
                "multimodal_digest": "a" * 64,
                "sampled_frame_bundle": "b" * 64,
            }},
            artifact_catalog_sha256="c" * 64,
        )
        self.args = {
            "question_id": "question-1",
            "object_id": "object-1",
            "cache_episode_id": "fresh-episode-1",
        }

    def test_all_components_required_for_hit(self):
        hit = self.reader.observe(**self.args, executor_node_id="N7")
        miss = self.reader.observe(**self.args, executor_node_id="N8")
        self.assertEqual("hit", hit.state)
        self.assertEqual("miss", miss.state)
        self.assertNotEqual(hit.evidence_sha256, miss.evidence_sha256)
        self.assertEqual(2, len(self.n7.calls))
        self.assertTrue(all(request["cache_namespace"].startswith("episode-")
                            for request in self.n7.calls))

    def test_status_change_invalidates_observation(self):
        prior = self.reader.observe(**self.args, executor_node_id="N7")
        self.n7.states["sampled_frame_bundle"] = "MISS"
        current = self.reader.observe(**self.args, executor_node_id="N7")
        self.assertEqual("miss", current.state)
        self.assertNotEqual(prior.evidence_sha256,
                            current.evidence_sha256)

    def test_unknown_object_fails_before_network(self):
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "not in artifact catalog"):
            self.reader.observe(
                **{**self.args, "object_id": "unknown"},
                executor_node_id="N7",
            )
        self.assertEqual([], self.n7.calls)

    def test_invalid_response_fails_closed(self):
        self.n7.node = "N8"
        with self.assertRaisesRegex(RouteActionBridgeError,
                                    "not identity-bound"):
            self.reader.observe(**self.args, executor_node_id="N7")


if __name__ == "__main__":
    unittest.main()
