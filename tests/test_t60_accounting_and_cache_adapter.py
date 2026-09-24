"""Focused fail-closed checks for the sealed t60 accounting adapter."""

from decimal import Decimal
import unittest

from experiments.ten_route_multiq_20260925.account_verified_routes import (
    llm_list_cost,
)
from experiments.ten_route_multiq_20260925.replay_sealed_t60 import (
    _validated_cache_groups,
)


class T60AccountingAndCacheAdapterTests(unittest.TestCase):
    def test_cached_token_price_is_not_charged_as_full_input(self):
        cost = llm_list_cost(
            input_units=100, cached_units=40, output_units=10,
            prices={"input": "0.5", "input_implicit_cache": "0.1",
                    "output": "3"},
        )
        self.assertEqual(cost, Decimal("0.000064"))
        with self.assertRaises(ValueError):
            llm_list_cost(input_units=10, cached_units=11,
                          output_units=0, prices={"input": "0.5",
                                                  "input_implicit_cache": "0.1",
                                                  "output": "3"})

    def test_durable_cache_evictions_are_replayed_exactly(self):
        reps = ("multimodal_digest", "sampled_frame_bundle")
        artifacts = {
            "A": {reps[0]: {"sha256": "a" * 64, "size_bytes": 2},
                  reps[1]: {"sha256": "b" * 64, "size_bytes": 7}},
            "B": {reps[0]: {"sha256": "c" * 64, "size_bytes": 2},
                  reps[1]: {"sha256": "d" * 64, "size_bytes": 7}},
        }
        events = []
        stores = {}
        for object_id, namespace in (("A", "a1"), ("B", "b1")):
            for rep in reps:
                artifact = artifacts[object_id][rep]
                events.append({
                    "event_id": len(events) + 1, "event_kind": "MISS",
                    "cache_namespace": namespace, "object_id": object_id,
                    "representation_id": rep,
                    "content_sha256": artifact["sha256"], "size_bytes": 0,
                })
                event_id = len(events) + 1
                events.append({
                    "event_id": event_id, "event_kind": "STORE",
                    "cache_namespace": namespace, "object_id": object_id,
                    "representation_id": rep,
                    "content_sha256": artifact["sha256"],
                    "size_bytes": artifact["size_bytes"],
                })
                stores[str(event_id)] = []
                if object_id == "B":
                    stores[str(event_id)] = [
                        {"object_id": "A", "representation_id": rep}
                    ]
            for rep in reps:
                artifact = artifacts[object_id][rep]
                events.append({
                    "event_id": len(events) + 1, "event_kind": "HIT",
                    "cache_namespace": namespace, "object_id": object_id,
                    "representation_id": rep,
                    "content_sha256": artifact["sha256"],
                    "size_bytes": artifact["size_bytes"],
                })
        export = {"node_id": "N7", "capacity_bytes": 10,
                  "events": events, "store_evictions_by_event_id": stores}
        schedule = [{"object_id": "A"}, {"object_id": "B"}]
        self.assertEqual(
            len(_validated_cache_groups(export, schedule, artifacts)), 2
        )
        export["store_evictions_by_event_id"]["8"] = []
        with self.assertRaisesRegex(ValueError, "eviction differs"):
            _validated_cache_groups(export, schedule, artifacts)


if __name__ == "__main__":
    unittest.main()
