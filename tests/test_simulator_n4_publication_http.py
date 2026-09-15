from __future__ import annotations

import base64
import hashlib
import json
import threading
import urllib.error
import urllib.request
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pathfinder.simulator.n4_derived_data_plane import N4ArtifactProvenance
from pathfinder.simulator.n4_publication_http import (
    N4_PUBLICATION_HTTP_REQUEST_SCHEMA_VERSION,
    N4PublicationHTTPService,
    N4PublicationHTTPSettings,
    create_n4_publication_http_server,
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _request_payload(
    raw: bytes,
    *,
    publication_id: str = "publish-digest-v1",
    catalog_version: str = "catalog-v1",
    expected: str | None = None,
) -> bytes:
    provenance = N4ArtifactProvenance(
        producer_node_id="N5",
        publication_source_id="n5-digest-output-v1",
        source_representation_id="raw_video",
        source_content_sha256="a" * 64,
        derivation_id="digest-plan-v1",
        derivation_sha256="b" * 64,
    )
    return _canonical({
        "schema_version": N4_PUBLICATION_HTTP_REQUEST_SCHEMA_VERSION,
        "publication_id": publication_id,
        "package_id": "n4-derived-generation-v1",
        "catalog_version": catalog_version,
        "expected_current_catalog_version": expected,
        "artifacts": [{
            "object_id": "nextqa-val-0000000001",
            "representation_id": "multimodal_digest",
            "artifact_base64": base64.b64encode(raw).decode("ascii"),
            "artifact_sha256": hashlib.sha256(raw).hexdigest(),
            "artifact_size_bytes": len(raw),
            "plan_ids": ["D-origin-warm-digest"],
            "provenance": provenance.to_dict(),
        }],
    })


class N4PublicationHTTPServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.settings = N4PublicationHTTPSettings(
            bearer_token="runtime-only-test-token",
            max_request_bytes=1024 * 1024,
            max_artifact_bytes=512 * 1024,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_service_publishes_and_replays_without_paths_or_token(self) -> None:
        from pathfinder.simulator.n4_derived_data_plane import (
            N4DerivedRepresentationStore,
        )

        service = N4PublicationHTTPService(
            N4DerivedRepresentationStore(self.root / "store"),
            self.settings,
        )
        payload = _request_payload(b"A grounded visual digest.\n")
        first = service.publish(payload)
        second = service.publish(payload)

        self.assertEqual(first["status"], "COMMITTED")
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(first["receipt"], second["receipt"])
        self.assertTrue(first["data_agent_reload_required"])
        serialized = json.dumps(first, sort_keys=True)
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn(self.settings.bearer_token, serialized)

    def test_service_rejects_digest_mismatch_and_noncanonical_json(self) -> None:
        from pathfinder.simulator.n4_derived_data_plane import (
            N4DerivedRepresentationStore,
        )
        from pathfinder.simulator.n4_publication_http import (
            N4PublicationHTTPError,
        )

        service = N4PublicationHTTPService(
            N4DerivedRepresentationStore(self.root / "store"),
            self.settings,
        )
        value = json.loads(_request_payload(b"digest\n"))
        value["artifacts"][0]["artifact_sha256"] = "0" * 64
        with self.assertRaisesRegex(N4PublicationHTTPError, "digest"):
            service.publish(_canonical(value))
        pretty = json.dumps(value, indent=2).encode("utf-8")
        with self.assertRaisesRegex(N4PublicationHTTPError, "canonical"):
            service.publish(pretty)


class N4PublicationHTTPServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.token = "runtime-only-http-token"
        self.server = create_n4_publication_http_server(
            self.root / "store",
            settings=N4PublicationHTTPSettings(
                bearer_token=self.token,
                host="127.0.0.1",
                port=0,
                max_request_bytes=1024 * 1024,
                max_artifact_bytes=512 * 1024,
            ),
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        host, port = self.server.server_address[:2]
        self.origin = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temporary.cleanup()

    def _open(
        self,
        path: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
        token: str | None = None,
    ) -> tuple[int, dict]:
        headers = {"Accept-Encoding": "identity"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if token is not None:
            headers["Authorization"] = "Bearer " + token
        request = urllib.request.Request(
            self.origin + path,
            method=method,
            data=body,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_health_is_public_and_does_not_disclose_runtime_values(self) -> None:
        status, health = self._open("/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(health["node_id"], "N4")
        self.assertFalse(health["credentials_recorded"])
        self.assertNotIn(self.token, json.dumps(health))

    def test_http_authentication_publish_and_conflict(self) -> None:
        payload = _request_payload(b"A grounded visual digest.\n")
        status, error = self._open(
            "/v1/publications",
            method="POST",
            body=payload,
        )
        self.assertEqual(status, 401)
        self.assertEqual(error["error"]["code"], "unauthorized")

        status, result = self._open(
            "/v1/publications",
            method="POST",
            body=payload,
            token=self.token,
        )
        self.assertEqual(status, 201)
        self.assertEqual(result["status"], "COMMITTED")

        changed = _request_payload(
            b"Different digest.\n",
            publication_id="publish-digest-v1",
            catalog_version="catalog-v2",
            expected="catalog-v1",
        )
        status, error = self._open(
            "/v1/publications",
            method="POST",
            body=changed,
            token=self.token,
        )
        self.assertEqual(status, 409)
        self.assertEqual(error["error"]["code"], "conflict")


if __name__ == "__main__":
    unittest.main()
