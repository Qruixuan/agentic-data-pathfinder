"""Metadata replay must agree with the durable cache's eviction behavior."""

import hashlib
from pathlib import Path
import tempfile
import unittest

from pathfinder.rsi_exam.cache_episode_state import (
    ReplayCacheState, tight_cache_capacity,
)
from pathfinder.simulator.full_flow_cache import FullFlowArtifactCache


NS = "rsi-episode"
DIGEST = "multimodal_digest"
FRAMES = "sampled_frame_bundle"


class ReplayCacheStateTests(unittest.TestCase):
    def test_tight_capacity_is_frozen_from_sizes_or_refuses_cohort(self):
        self.assertEqual(tight_cache_capacity({
            "A": 100, "B": 96, "C": 95,
        }), 110)
        with self.assertRaisesRegex(ValueError, "pressure"):
            tight_cache_capacity({"A": 100, "B": 5, "C": 5})
        with self.assertRaisesRegex(ValueError, "three positive"):
            tight_cache_capacity({"A": 100, "B": 96})

    def test_partial_hit_and_eviction_match_durable_cache(self):
        payloads = {
            ("A", DIGEST): b"a" * 2,
            ("A", FRAMES): b"A" * 7,
            ("B", DIGEST): b"b" * 2,
            ("B", FRAMES): b"B" * 7,
            ("C", DIGEST): b"c" * 2,
            ("C", FRAMES): b"C" * 7,
        }
        model = ReplayCacheState(node_id="N7", capacity_bytes=10)
        with tempfile.TemporaryDirectory() as directory:
            durable = FullFlowArtifactCache(
                Path(directory), node_id="N7", cache_id="test-cache",
                capacity_bytes=10,
            )
            counter = 0

            def put(object_id, rep):
                nonlocal counter
                counter += 1
                payload = payloads[object_id, rep]
                digest = hashlib.sha256(payload).hexdigest()
                expected = model.put(
                    namespace=NS, object_id=object_id,
                    representation_id=rep, content_sha256=digest,
                    size_bytes=len(payload),
                )
                actual = durable.put(
                    cache_namespace=NS, request_id=f"put-{counter}",
                    object_id=object_id, representation_id=rep,
                    payload=payload, expected_sha256=digest,
                )
                self.assertEqual(
                    [(row.object_id, row.representation_id) for row in expected],
                    [(row["object_id"], row["representation_id"])
                     for row in actual["evicted"]],
                )

            def lookup(object_id, rep):
                payload = payloads[object_id, rep]
                digest = hashlib.sha256(payload).hexdigest()
                expected = model.lookup(
                    namespace=NS, object_id=object_id,
                    representation_id=rep, expected_sha256=digest,
                )
                actual = durable.lookup(
                    cache_namespace=NS, object_id=object_id,
                    representation_id=rep, expected_sha256=digest,
                )
                self.assertEqual(expected, actual is not None)
                return expected

            put("A", DIGEST)
            put("A", FRAMES)
            self.assertEqual(
                (lookup("A", DIGEST), lookup("A", FRAMES)), (True, True),
            )
            put("B", DIGEST)
            # A's digest and frames are distinct LRU entries: B's insertion
            # can evict only one of them, never an imagined atomic A pair.
            self.assertEqual(
                (lookup("A", DIGEST), lookup("A", FRAMES)), (False, True),
            )
            put("B", FRAMES)
            self.assertEqual(
                (lookup("B", DIGEST), lookup("B", FRAMES)), (False, True),
            )
            self.assertEqual(
                model.snapshot()["used_bytes"], durable.verify()["used_bytes"],
            )

    def test_digest_mismatch_does_not_update_recency(self):
        state = ReplayCacheState(node_id="N8", capacity_bytes=5)
        state.put(namespace=NS, object_id="A", representation_id=DIGEST,
                  content_sha256="a" * 64, size_bytes=2)
        before = state.snapshot()
        self.assertFalse(state.lookup(
            namespace=NS, object_id="A", representation_id=DIGEST,
            expected_sha256="b" * 64,
        ))
        self.assertEqual(state.snapshot(), before)

    def test_capacity_and_identity_fail_closed(self):
        with self.assertRaises(ValueError):
            ReplayCacheState(node_id="N6", capacity_bytes=10)
        state = ReplayCacheState(node_id="N7", capacity_bytes=2)
        with self.assertRaises(ValueError):
            state.put(namespace=NS, object_id="A", representation_id=DIGEST,
                      content_sha256="a" * 64, size_bytes=3)
        self.assertEqual(state.snapshot()["entries"], [])


if __name__ == "__main__":
    unittest.main()
