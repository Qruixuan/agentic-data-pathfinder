from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from pathfinder.simulator.full_flow_cache import (
    FullFlowArtifactCache,
    FullFlowCacheServerSettings,
    FullFlowCacheConflict,
    FullFlowCacheError,
    HttpFullFlowArtifactCacheClient,
    create_full_flow_cache_http_server,
)


@contextmanager
def _serving(server: object) -> Iterator[None]:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class FullFlowArtifactCacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _cache(
        self,
        *,
        node_id: str = "N7",
        capacity_bytes: int = 32,
    ) -> FullFlowArtifactCache:
        return FullFlowArtifactCache(
            self.root / "state",
            node_id=node_id,
            cache_id=f"{node_id}.derived-cache",
            capacity_bytes=capacity_bytes,
        )

    def test_real_bytes_survive_process_equivalent_reopen(self) -> None:
        payload = b"canonical-frame-bundle"
        digest = hashlib.sha256(payload).hexdigest()
        cache = self._cache(capacity_bytes=64)

        stored = cache.put(
            request_id="store-1",
            object_id="video-1",
            representation_id="sampled_frame_bundle",
            payload=payload,
            expected_sha256=digest,
        )
        self.assertEqual("STORED", stored["status"])
        self.assertFalse(stored["idempotent_replay"])

        reopened = self._cache(capacity_bytes=64)
        artifact = reopened.lookup(
            object_id="video-1",
            representation_id="sampled_frame_bundle",
            expected_sha256=digest,
        )
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(payload, artifact.payload)
        self.assertEqual(digest, artifact.content_sha256)
        self.assertEqual("N7", artifact.node_id)
        self.assertNotIn(str(self.root), json.dumps(artifact.metadata()))
        self.assertEqual("VERIFIED", reopened.verify()["status"])

    def test_reopen_removes_known_crash_orphans_and_temp_files(self) -> None:
        cache = self._cache(capacity_bytes=64)
        stored = cache.put(
            request_id="store-1",
            object_id="video-1",
            representation_id="sampled_frame_bundle",
            payload=b"canonical-frame-bundle",
        )
        objects = self.root / "state" / "objects"
        orphan = objects / hashlib.sha256(b"orphan").hexdigest()
        temporary = objects / (f".{stored['content_sha256']}.99.100.tmp")
        orphan.write_bytes(b"orphan")
        temporary.write_bytes(b"partial")

        reopened = self._cache(capacity_bytes=64)

        self.assertFalse(orphan.exists())
        self.assertFalse(temporary.exists())
        self.assertEqual(1, reopened.health()["entry_count"])

    def test_store_idempotency_is_durable_and_conflicts_fail_closed(self) -> None:
        cache = self._cache()
        arguments = {
            "request_id": "same-request",
            "object_id": "video-1",
            "representation_id": "multimodal_digest",
            "payload": b"digest-one",
        }
        first = cache.put(**arguments)
        replay = self._cache().put(**arguments)
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(first["event_id"], replay["event_id"])

        with self.assertRaisesRegex(
            FullFlowCacheConflict,
            "different cache content",
        ):
            cache.put(**{**arguments, "payload": b"digest-two"})
        self.assertEqual(1, cache.health()["entry_count"])

    def test_lru_eviction_uses_real_payload_capacity(self) -> None:
        cache = self._cache(capacity_bytes=8)
        cache.put(
            request_id="store-a",
            object_id="a",
            representation_id="digest",
            payload=b"aaaa",
        )
        cache.put(
            request_id="store-b",
            object_id="b",
            representation_id="digest",
            payload=b"bbbb",
        )
        self.assertIsNotNone(cache.lookup(
            object_id="a",
            representation_id="digest",
        ))
        result = cache.put(
            request_id="store-c",
            object_id="c",
            representation_id="digest",
            payload=b"cccc",
        )

        self.assertEqual(["b"], [row["object_id"] for row in result["evicted"]])
        self.assertIsNone(cache.lookup(
            object_id="b",
            representation_id="digest",
        ))
        self.assertIsNotNone(cache.lookup(
            object_id="a",
            representation_id="digest",
        ))
        self.assertIsNotNone(cache.lookup(
            object_id="c",
            representation_id="digest",
        ))
        self.assertEqual(8, cache.health()["used_bytes"])

    def test_expected_digest_and_capacity_fail_before_metadata_commit(self) -> None:
        cache = self._cache(capacity_bytes=4)
        with self.assertRaisesRegex(FullFlowCacheError, "expected_sha256"):
            cache.put(
                request_id="bad-digest",
                object_id="video",
                representation_id="digest",
                payload=b"data",
                expected_sha256="0" * 64,
            )
        with self.assertRaisesRegex(FullFlowCacheError, "exceeds"):
            cache.put(
                request_id="too-large",
                object_id="video",
                representation_id="digest",
                payload=b"12345",
            )
        self.assertEqual(0, cache.health()["entry_count"])

    def test_wrong_digest_lookup_is_a_miss_without_returning_bytes(self) -> None:
        cache = self._cache()
        cache.put(
            request_id="store",
            object_id="video",
            representation_id="digest",
            payload=b"payload",
        )
        self.assertIsNone(cache.lookup(
            object_id="video",
            representation_id="digest",
            expected_sha256="0" * 64,
        ))
        self.assertEqual(
            ["STORE", "MISS"],
            [event["event_kind"] for event in cache.events()],
        )

    def test_content_corruption_is_detected(self) -> None:
        cache = self._cache()
        stored = cache.put(
            request_id="store",
            object_id="video",
            representation_id="digest",
            payload=b"payload",
        )
        object_path = self.root / "state" / "objects" / stored[
            "content_sha256"
        ]
        object_path.write_bytes(b"corrupt")
        with self.assertRaisesRegex(FullFlowCacheError, "(size|digest) changed"):
            cache.verify()

    def test_persistent_state_cannot_be_rebound_to_another_node(self) -> None:
        self._cache(node_id="N7")
        with self.assertRaisesRegex(FullFlowCacheError, "binding differs"):
            self._cache(node_id="N8")

    def test_only_execution_nodes_are_allowed(self) -> None:
        with self.assertRaisesRegex(FullFlowCacheError, "N7 or N8"):
            self._cache(node_id="N4")

    def test_authenticated_http_contract_moves_real_cache_bytes(self) -> None:
        cache = self._cache(capacity_bytes=64)
        token = "cache-test-token-0123456789"
        server = create_full_flow_cache_http_server(
            cache,
            FullFlowCacheServerSettings(
                host="127.0.0.1",
                port=0,
                token=token,
                max_artifact_bytes=64,
            ),
        )
        base_url = f"http://127.0.0.1:{server.server_port}"
        client = HttpFullFlowArtifactCacheClient(
            base_url=base_url,
            token=token,
            expected_node_id="N7",
            expected_cache_id="N7.derived-cache",
            max_artifact_bytes=64,
        )
        payload = b"real-frame-bundle-bytes"
        digest = hashlib.sha256(payload).hexdigest()

        with _serving(server):
            self.assertEqual("ok", client.health()["status"])
            self.assertIsNone(client.get(
                object_id="video",
                representation_id="sampled_frame_bundle",
                expected_sha256=digest,
            ))
            stored = client.put(
                request_id="cache-store-http-1",
                object_id="video",
                representation_id="sampled_frame_bundle",
                payload=payload,
                expected_sha256=digest,
            )
            artifact = client.get(
                object_id="video",
                representation_id="sampled_frame_bundle",
                expected_sha256=digest,
            )

        self.assertEqual("STORED", stored["status"])
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(payload, artifact.payload)
        self.assertEqual(digest, artifact.content_sha256)
        self.assertEqual("N7", artifact.node_id)
        self.assertEqual(
            ["MISS", "STORE", "HIT"],
            [event["event_kind"] for event in cache.events()],
        )

    def test_http_service_rejects_missing_token(self) -> None:
        cache = self._cache()
        server = create_full_flow_cache_http_server(
            cache,
            FullFlowCacheServerSettings(
                host="127.0.0.1",
                port=0,
                token="cache-test-token-0123456789",
                max_artifact_bytes=32,
            ),
        )
        url = (
            f"http://127.0.0.1:{server.server_port}/v1/cache/artifact?"
            "object_id=video&representation_id=digest"
        )
        with _serving(server), self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(url, timeout=5)
        self.assertEqual(401, caught.exception.code)

    def test_http_client_rejects_unlisted_plain_http_host(self) -> None:
        with self.assertRaisesRegex(FullFlowCacheError, "must use HTTPS"):
            HttpFullFlowArtifactCacheClient(
                base_url="http://cache.example",
                token="cache-test-token-0123456789",
                expected_node_id="N8",
                expected_cache_id="N8.derived-cache",
            )


if __name__ == "__main__":
    unittest.main()
