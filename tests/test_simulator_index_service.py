from __future__ import annotations

import copy
import hashlib
import json
import threading
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pathfinder.simulator.index_service import (
    INDEX_SERVICE_API_VERSION,
    INDEX_SOURCE_SCHEMA_VERSION,
    N2IndexError,
    N2IndexHTTPClient,
    N2IndexHTTPError,
    N2IndexService,
    build_n2_index_package,
    build_n2_index_query_request,
    create_n2_index_http_server,
    verify_n2_index_package,
    verify_n2_index_query_result,
    verify_n2_public_index_query_result,
)


class N2IndexServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "visible-index-source.json"
        self.source_value = {
            "schema_version": INDEX_SOURCE_SCHEMA_VERSION,
            "index_id": "nextqa-visible-lexical-v1",
            "logical_node_id": "N2",
            "documents": [
                {
                    "object_id": "object-alpha",
                    "source_object_group": "group-alpha",
                    "visible_fields": {
                        "digest": "two musicians play guitar on a stage",
                        "media_type": "video",
                        "tags": ["guitar", "music"],
                    },
                },
                {
                    "object_id": "object-beta",
                    "source_object_group": "group-beta",
                    "visible_fields": {
                        "digest": "a cyclist follows a white van on a road",
                        "media_type": "video",
                        "tags": ["bicycle", "road"],
                    },
                },
                {
                    "object_id": "object-gamma",
                    "source_object_group": "group-gamma",
                    "visible_fields": {
                        "digest": "an elephant kicks a soccer ball toward a goal",
                        "media_type": "video",
                        "tags": ["animal", "soccer"],
                    },
                },
                {
                    "object_id": "object-zeta",
                    "source_object_group": "group-zeta",
                    "visible_fields": {
                        "digest": "people swim beneath a tiered waterfall",
                        "media_type": "video",
                        "tags": ["swimming", "waterfall"],
                    },
                },
            ],
            "credentials_recorded": False,
        }
        self._write_source()
        self.package = self.root / "index-package"
        build_n2_index_package(self.source, output_dir=self.package)
        self.service = N2IndexService(self.package)

    def _write_source(self) -> None:
        self.source.write_text(
            json.dumps(
                self.source_value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    def request(
        self,
        *,
        query: str = "find musicians playing guitar",
        candidates: list[str] | None = None,
        top_k: int = 2,
    ) -> dict:
        return build_n2_index_query_request(
            request_id="request-guitar-001",
            query_id="query-guitar",
            index_id="nextqa-visible-lexical-v1",
            query_text=query,
            top_k=top_k,
            candidate_object_ids=candidates,
        )

    def test_build_is_byte_deterministic_and_endpoint_free(self) -> None:
        second = self.root / "index-package-second"
        build_n2_index_package(self.source, output_dir=second)
        self.assertEqual(
            {path.name for path in self.package.iterdir()},
            {path.name for path in second.iterdir()},
        )
        for path in self.package.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        verified = verify_n2_index_package(self.package)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual("N2", verified["logical_node_id"])
        combined = b"".join(
            path.read_bytes() for path in sorted(self.package.iterdir())
        ).decode("utf-8")
        for forbidden in (
            "http://",
            "https://",
            "localhost",
            "authorization",
            "api_key",
        ):
            self.assertNotIn(forbidden, combined.casefold())

    def test_executes_real_lexical_ranking_with_exact_hashes(self) -> None:
        request = self.request()
        first = self.service.query(request)
        second = self.service.query(request)
        self.assertEqual(first, second)
        self.assertEqual("COMPLETED", first["status"])
        self.assertEqual("N2", first["node_id"])
        self.assertTrue(first["lexical_retrieval_executed"])
        self.assertEqual("object-alpha", first["ranked_candidates"][0]["object_id"])
        self.assertGreater(
            first["ranked_candidates"][0]["lexical_score_units"],
            first["ranked_candidates"][1]["lexical_score_units"],
        )
        self.assertEqual(
            hashlib.sha256(
                json.dumps(
                    first["ranked_candidates"],
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
            first["ranking_sha256"],
        )
        verified = verify_n2_index_query_result(
            package_dir=self.package,
            request=request,
            result=first,
        )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(
            first["result_content_sha256"],
            verified["result_content_sha256"],
        )

    def test_candidate_filter_is_real_and_canonical(self) -> None:
        request = self.request(
            candidates=["object-beta", "object-zeta"],
            top_k=2,
        )
        result = self.service.query(request)
        self.assertEqual(
            ["object-beta", "object-zeta"],
            [row["object_id"] for row in result["ranked_candidates"]],
        )
        self.assertEqual(0, result["ranked_candidates"][0]["lexical_score_units"])
        self.assertEqual(0, result["ranked_candidates"][1]["lexical_score_units"])

    def test_unicode_nfkc_tokenization_is_deterministic(self) -> None:
        request = self.request(query="ＦＩＮＤ ELEPHANT soccer", top_k=1)
        result = self.service.query(request)
        self.assertEqual("object-gamma", result["ranked_candidates"][0]["object_id"])
        self.assertEqual(3, result["query_token_count"])

    def test_rejects_hidden_or_oracle_fields(self) -> None:
        for field in ("answer", "relevant_object_ids", "endpoint_url"):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.source_value)
                changed["documents"][0]["visible_fields"][field] = "secret"
                self.source_value = changed
                self._write_source()
                with self.assertRaisesRegex(N2IndexError, "hidden field"):
                    build_n2_index_package(
                        self.source,
                        output_dir=self.root / f"bad-{field}",
                    )
                self.source_value = copy.deepcopy(
                    {
                        **self.source_value,
                        "documents": self.setUp_source_documents(),
                    }
                )

    def setUp_source_documents(self) -> list[dict]:
        return [
            {
                "object_id": object_id,
                "source_object_group": f"group-{object_id.split('-')[-1]}",
                "visible_fields": fields,
            }
            for object_id, fields in (
                (
                    "object-alpha",
                    {
                        "digest": "two musicians play guitar on a stage",
                        "media_type": "video",
                        "tags": ["guitar", "music"],
                    },
                ),
                (
                    "object-beta",
                    {
                        "digest": "a cyclist follows a white van on a road",
                        "media_type": "video",
                        "tags": ["bicycle", "road"],
                    },
                ),
                (
                    "object-gamma",
                    {
                        "digest": "an elephant kicks a soccer ball toward a goal",
                        "media_type": "video",
                        "tags": ["animal", "soccer"],
                    },
                ),
                (
                    "object-zeta",
                    {
                        "digest": "people swim beneath a tiered waterfall",
                        "media_type": "video",
                        "tags": ["swimming", "waterfall"],
                    },
                ),
            )
        ]

    def test_source_and_package_tampering_is_rejected(self) -> None:
        changed = copy.deepcopy(self.source_value)
        changed["documents"][0]["object_id"] = "object-zeta"
        self.source_value = changed
        self._write_source()
        with self.assertRaisesRegex(N2IndexError, "sorted|duplicate"):
            build_n2_index_package(self.source, output_dir=self.root / "bad-source")

        artifact = self.package / "lexical-index.json"
        value = json.loads(artifact.read_text(encoding="utf-8"))
        value["term_weight_units"]["guitar"] += 1
        artifact.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(
            N2IndexError,
            "canonically serialized|checksum mismatch|term weights mismatch",
        ):
            verify_n2_index_package(self.package)

    def test_query_rejects_wrong_binding_and_unsafe_candidates(self) -> None:
        cases = []
        wrong_node = self.request()
        wrong_node["requested_node_id"] = "N3"
        cases.append((wrong_node, "target N2"))
        wrong_index = self.request()
        wrong_index["index_id"] = "other-index"
        cases.append((wrong_index, "mounted index"))
        unknown = self.request(candidates=["missing-object"], top_k=1)
        cases.append((unknown, "unknown candidate"))
        too_many = self.request(candidates=["object-alpha"], top_k=2)
        cases.append((too_many, "top_k exceeds candidate count"))
        for request, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(N2IndexError, message):
                    self.service.query(request)

    def test_query_request_requires_sorted_unique_candidates(self) -> None:
        with self.assertRaisesRegex(N2IndexError, "sorted and unique"):
            self.service.query(
                {
                    **self.request(),
                    "candidate_object_ids": ["object-zeta", "object-alpha"],
                }
            )
        with self.assertRaisesRegex(N2IndexError, "sorted and unique"):
            self.service.query(
                {
                    **self.request(),
                    "candidate_object_ids": ["object-alpha", "object-alpha"],
                }
            )

    def test_result_tampering_with_a_rehashed_outer_result_is_rejected(self) -> None:
        request = self.request()
        result = self.service.query(request)
        result["ranked_candidates"][0]["object_id"] = "object-zeta"
        result["ranking_sha256"] = self.value_sha256(result["ranked_candidates"])
        core = dict(result)
        del core["result_content_sha256"]
        result["result_content_sha256"] = self.value_sha256(core)
        with self.assertRaisesRegex(N2IndexError, "deterministic replay"):
            verify_n2_index_query_result(
                package_dir=self.package,
                request=request,
                result=result,
            )

    @staticmethod
    def value_sha256(value: object) -> str:
        return hashlib.sha256(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def _server(
        self,
        token: str | None = "test-index-token",
        *,
        node_id: str = "N2",
    ):
        server = create_n2_index_http_server(
            self.package,
            host="127.0.0.1",
            port=0,
            bearer_token=token,
            node_id=node_id,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def test_n7_http_client_executes_same_contract_and_checks_n2_identity(self) -> None:
        server = self._server()
        port = server.server_address[1]
        client = N2IndexHTTPClient(
            base_url=f"http://127.0.0.1:{port}",
            expected_index_id=self.service.artifact["index_id"],
            expected_index_sha256=self.service.package["index_sha256"],
            bearer_token="test-index-token",
        )
        health = client.health()
        self.assertEqual("N2", health["node_id"])
        request = self.request()
        self.assertEqual(self.service.query(request), client.query(request))

    def test_public_query_projection_excludes_source_group_end_to_end(self) -> None:
        server = self._server()
        client = N2IndexHTTPClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            expected_index_id=self.service.artifact["index_id"],
            expected_index_sha256=self.service.package["index_sha256"],
            bearer_token="test-index-token",
        )
        request = self.request()
        result = client.query_public(request)
        self.assertNotIn("source_object_group", json.dumps(result))
        checked = verify_n2_public_index_query_result(
            package_dir=self.package,
            request=request,
            result=result,
        )
        self.assertEqual("VERIFIED_PUBLIC_PROJECTION", checked["status"])
        self.assertFalse(checked["source_object_group_included"])

    def test_same_frozen_package_serves_authenticated_n7_and_n8_identity(self) -> None:
        package_bytes = {
            path.name: path.read_bytes()
            for path in self.package.iterdir()
        }
        for node_id in ("N7", "N8"):
            with self.subTest(node_id=node_id):
                server = self._server(node_id=node_id)
                client = N2IndexHTTPClient(
                    base_url=(
                        f"http://127.0.0.1:{server.server_address[1]}"
                    ),
                    expected_index_id=self.service.artifact["index_id"],
                    expected_index_sha256=self.service.package["index_sha256"],
                    expected_node_id=node_id,
                    bearer_token="test-index-token",
                )
                request = self.request()
                request = build_n2_index_query_request(
                    request_id=request["request_id"],
                    query_id=request["query_id"],
                    index_id=request["index_id"],
                    query_text=request["query_text"],
                    top_k=request["top_k"],
                    candidate_object_ids=request["candidate_object_ids"],
                    requested_node_id=node_id,
                )
                self.assertEqual(node_id, client.health()["node_id"])
                result = client.query(request)
                self.assertEqual(node_id, result["node_id"])
                verified = verify_n2_index_query_result(
                    package_dir=self.package,
                    request=request,
                    result=result,
                    expected_node_id=node_id,
                )
                self.assertEqual(node_id, verified["node_id"])
                self.assertEqual(
                    package_bytes,
                    {
                        path.name: path.read_bytes()
                        for path in self.package.iterdir()
                    },
                )

    def test_http_contract_rejects_missing_auth_and_protocol_header(self) -> None:
        server = self._server()
        port = server.server_address[1]
        body = json.dumps(self.request()).encode("utf-8")
        with self.assertRaises(HTTPError) as caught:
            urlopen(
                Request(
                    f"http://127.0.0.1:{port}/v1/index/query",
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                ),
                timeout=5,
            )
        self.assertEqual(401, caught.exception.code)

        with self.assertRaises(HTTPError) as caught:
            urlopen(
                Request(
                    f"http://127.0.0.1:{port}/v1/index/query",
                    data=body,
                    headers={
                        "Authorization": "Bearer test-index-token",
                        "Content-Type": "application/json",
                    },
                    method="POST",
                ),
                timeout=5,
            )
        self.assertEqual(400, caught.exception.code)

    def test_client_refuses_public_plain_http_and_wrong_credentials(self) -> None:
        with self.assertRaisesRegex(N2IndexError, "plain HTTP"):
            N2IndexHTTPClient(
                base_url="http://index.example.test:8080",
                expected_index_id=self.service.artifact["index_id"],
                expected_index_sha256=self.service.package["index_sha256"],
            )
        server = self._server()
        client = N2IndexHTTPClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            expected_index_id=self.service.artifact["index_id"],
            expected_index_sha256=self.service.package["index_sha256"],
            bearer_token="wrong",
        )
        with self.assertRaisesRegex(N2IndexHTTPError, "HTTP 401"):
            client.query(self.request())

    def test_explicit_compose_host_is_allowed_but_not_frozen(self) -> None:
        client = N2IndexHTTPClient(
            base_url="http://pathfinder-sim-n2-index:8082",
            expected_index_id=self.service.artifact["index_id"],
            expected_index_sha256=self.service.package["index_sha256"],
            simulator_private_http_hosts=("pathfinder-sim-n2-index",),
        )
        self.assertEqual(
            ("pathfinder-sim-n2-index",),
            client.simulator_private_http_hosts,
        )

    def test_protocol_constant_is_separate_from_portable_package(self) -> None:
        self.assertEqual(
            "pathfinder.n2-index-service/v1alpha1",
            INDEX_SERVICE_API_VERSION,
        )
        artifact = json.loads(
            (self.package / "lexical-index.json").read_text(encoding="utf-8")
        )
        self.assertNotIn("api_url", artifact)
        self.assertFalse(artifact["endpoint_binding_present"])


if __name__ == "__main__":
    unittest.main()
