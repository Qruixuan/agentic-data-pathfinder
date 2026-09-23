"""N6 provider token receipts contain numbers only and fail closed."""

from __future__ import annotations

import json
import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError

from pathfinder.simulator import container_node
from pathfinder.simulator.container_node import ContainerNodeRuntime


class _Response:
    def __init__(
        self, body: bytes, headers: dict[str, str] | None = None,
    ) -> None:
        self.body = body
        self.headers = headers or {}

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def read(self, limit: int) -> bytes:
        return self.body[:limit]


class _Opener:
    def __init__(
        self, body: bytes, headers: dict[str, str] | None = None,
    ) -> None:
        self.body = body
        self.headers = headers

    def open(self, *args: object, **kwargs: object) -> _Response:
        del args, kwargs
        return _Response(self.body, self.headers)


class N6UsageCaptureTests(unittest.TestCase):
    def test_extracts_only_numeric_usage_with_cache(self) -> None:
        payload = {
            "usage": {
                "input_tokens": 120, "output_tokens": 8,
                "total_tokens": 128,
                "prompt_tokens_details": {"cached_tokens": 20},
                "private": "must not be recorded",
            }
        }
        self.assertEqual(container_node._semantic_provider_token_usage(payload), {
            "input_units": 120, "cached_input_units": 20,
            "output_units": 8, "total_units": 128,
        })

    def test_invalid_provider_totals_do_not_claim_usage(self) -> None:
        self.assertIsNone(container_node._semantic_provider_token_usage({
            "usage": {"input_tokens": 120, "output_tokens": 8,
                      "total_tokens": 129},
        }))
        self.assertIsNone(container_node._semantic_provider_token_usage({
            "usage": {"input_tokens": True, "output_tokens": 8},
        }))

    def test_semantic_call_emits_numeric_receipt_only(self) -> None:
        body = json.dumps({
            "model": "test-model",
            "choices": [{"message": {"content": "A"}}],
            "usage": {
                "prompt_tokens": 120, "completion_tokens": 8,
                "total_tokens": 128,
                "prompt_tokens_details": {"cached_tokens": 20},
                "secret": "must not be recorded",
            },
        }).encode("utf-8")
        with tempfile.TemporaryDirectory() as directory:
            runtime = ContainerNodeRuntime(
                "N6", Path(directory), enable_semantic_llm=True,
            )
            captured: list[dict[str, int]] = []
            with (
                mock.patch.dict(os.environ, {
                    "PATHFINDER_SEMANTIC_LLM_BASE_URL": "http://127.0.0.1:1",
                    "PATHFINDER_SEMANTIC_LLM_MODEL": "test-model",
                    "PATHFINDER_SEMANTIC_LLM_API_KEY": "test-key",
                }),
                mock.patch.object(
                    container_node, "_semantic_llm_opener",
                    return_value=_Opener(body),
                ),
            ):
                answer, model = runtime._call_semantic_llm(
                    "Return one letter.", usage_sink=captured.append,
                )
        self.assertEqual((answer, model), ("A", "test-model"))
        self.assertEqual(captured, [{
            "input_units": 120, "cached_input_units": 20,
            "output_units": 8, "total_units": 128,
        }])
        self.assertNotIn("test-key", str(captured))
        self.assertNotIn("must not be recorded", str(captured))

    def test_n6_journal_binds_result_digest_without_answer(self) -> None:
        usage = {
            "input_units": 10, "cached_input_units": 2,
            "output_units": 3, "total_units": 13,
        }
        request = {
            "schema_version": (
                container_node.CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION
            ),
            "semantic_request_id": "usage-test-1",
        }
        with tempfile.TemporaryDirectory() as directory:
            runtime = ContainerNodeRuntime("N6", directory)
            with mock.patch.object(
                runtime, "_semantic_complete_once",
                side_effect=lambda _request, digest: {
                    "request_sha256": digest,
                    "model": "test-model",
                    "final_answer": "private-answer-must-not-be-recorded",
                    "provider_usage": usage,
                },
            ) as execute:
                result = runtime.semantic_complete(request)
                replay = runtime.semantic_complete(request)
            self.assertEqual(execute.call_count, 1)
            self.assertTrue(replay["idempotent_replay"])
            digest = container_node.hashlib.sha256(
                container_node._canonical_json_bytes(result, "result")
            ).hexdigest()
            path = Path(directory) / "n6-provider-usage-v1.sqlite3"
            with closing(sqlite3.connect(
                f"file:{path}?mode=ro", uri=True,
            )) as db:
                rows = db.execute(
                    "SELECT * FROM n6_provider_usage"
                ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], digest)
            self.assertEqual(rows[0][1], result["request_sha256"])
            self.assertEqual(rows[0][2:], (10, 2, 3, 13))
            self.assertNotIn("private-answer", path.read_bytes().decode(
                "latin-1", errors="ignore",
            ))
            self.assertEqual(
                runtime.health()["semantic_usage_journal_error_count"], 0,
            )

    def test_journal_write_failure_does_not_repeat_paid_inference(self) -> None:
        request = {
            "schema_version": (
                container_node.CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION
            ),
            "semantic_request_id": "usage-test-2",
        }
        with tempfile.TemporaryDirectory() as directory:
            runtime = ContainerNodeRuntime("N6", directory)
            with (
                mock.patch.object(
                    runtime, "_semantic_complete_once",
                    side_effect=lambda _request, digest: {
                        "request_sha256": digest,
                        "provider_usage": {
                            "input_units": 10, "cached_input_units": 0,
                            "output_units": 1, "total_units": 11,
                        },
                    },
                ) as execute,
                mock.patch.object(
                    runtime, "_record_semantic_provider_usage",
                    side_effect=OSError("disk unavailable"),
                ),
            ):
                runtime.semantic_complete(request)
                runtime.semantic_complete(request)
            self.assertEqual(execute.call_count, 1)
            self.assertEqual(
                runtime.health()["semantic_usage_journal_error_count"], 1,
            )

    def test_provider_ids_bind_privately_without_changing_public_result(
        self,
    ) -> None:
        completion_id = "chatcmpl-1234567890abcdef"
        request_id = "12345678-1234-1234-1234-123456789abc"
        body = json.dumps({
            "id": completion_id,
            "request_id": request_id,
            "model": "test-model",
            "choices": [{"message": {"content": "A"}}],
            "usage": {
                "prompt_tokens": 120, "completion_tokens": 8,
                "total_tokens": 128,
            },
        }).encode("utf-8")
        prompt = "Return one letter."
        request = {
            "schema_version": (
                container_node.CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION
            ),
            "semantic_request_id": "provider-id-test",
            "execution_node_id": "N6",
            "representation_sha256": "a" * 64,
            "prompt": prompt,
            "prompt_sha256": container_node.hashlib.sha256(
                prompt.encode("utf-8")
            ).hexdigest(),
        }
        with tempfile.TemporaryDirectory() as directory:
            runtime = ContainerNodeRuntime(
                "N6", Path(directory), enable_semantic_llm=True,
            )
            with (
                mock.patch.dict(os.environ, {
                    "PATHFINDER_SEMANTIC_LLM_BASE_URL": "http://127.0.0.1:1",
                    "PATHFINDER_SEMANTIC_LLM_MODEL": "test-model",
                    "PATHFINDER_SEMANTIC_LLM_API_KEY": "test-key",
                }),
                mock.patch.object(
                    container_node, "_semantic_llm_opener",
                    return_value=_Opener(
                        body, {"X-Request-Id": request_id},
                    ),
                ) as opener,
            ):
                result = runtime.semantic_complete(request)
                replay = runtime.semantic_complete(request)
            self.assertEqual(opener.call_count, 1)
            self.assertTrue(replay["idempotent_replay"])
            self.assertNotIn(completion_id, str(result))
            self.assertNotIn(request_id, str(result))
            self.assertEqual(
                result["provider_usage"]["input_units"], 120,
            )
            trace_path = Path(directory) / "n6-provider-trace-v1.sqlite3"
            usage_path = Path(directory) / "n6-provider-usage-v1.sqlite3"
            with closing(sqlite3.connect(
                f"file:{trace_path}?mode=ro", uri=True,
            )) as db:
                trace = db.execute("""
                    SELECT request_sha256, result_sha256, outcome,
                           http_status, body_completion_id_sha256,
                           body_request_id_sha256,
                           header_request_id_sha256
                    FROM n6_provider_attempts
                """).fetchall()
            with closing(sqlite3.connect(
                f"file:{usage_path}?mode=ro", uri=True,
            )) as db:
                usage = db.execute("""
                    SELECT request_sha256, result_sha256
                    FROM n6_provider_usage
                """).fetchone()
            self.assertEqual(len(trace), 1)
            self.assertEqual(trace[0][:2], usage)
            self.assertEqual(trace[0][2:4], ("completed", 200))
            self.assertEqual(
                trace[0][4],
                container_node.hashlib.sha256(
                    completion_id.encode("ascii")
                ).hexdigest(),
            )
            expected_request_digest = container_node.hashlib.sha256(
                request_id.encode("ascii")
            ).hexdigest()
            self.assertEqual(trace[0][5:], (
                expected_request_digest, expected_request_digest,
            ))
            raw_db = trace_path.read_bytes()
            for private in (completion_id, request_id, "test-key", prompt):
                self.assertNotIn(private.encode("ascii"), raw_db)

    def test_retry_attempts_are_recorded_without_error_body(
        self,
    ) -> None:
        first_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        second_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        body = json.dumps({
            "id": "chatcmpl-abcdef0123456789",
            "model": "test-model",
            "choices": [{"message": {"content": "A"}}],
            "usage": {
                "input_tokens": 10, "output_tokens": 2,
                "total_tokens": 12,
            },
        }).encode("utf-8")

        class RetryOpener:
            calls = 0

            def open(self, *args: object, **kwargs: object) -> _Response:
                del args, kwargs
                self.calls += 1
                if self.calls == 1:
                    raise HTTPError(
                        "http://127.0.0.1:1/chat/completions",
                        503, "unavailable",
                        {"X-Request-Id": first_id},
                        io.BytesIO(b'{"error":{"message":"private"}}'),
                    )
                return _Response(
                    body, {"X-Request-Id": second_id},
                )

        with tempfile.TemporaryDirectory() as directory:
            runtime = ContainerNodeRuntime(
                "N6", Path(directory), enable_semantic_llm=True,
            )
            prompt = "Return one letter."
            request = {
                "schema_version": (
                    container_node.CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION
                ),
                "semantic_request_id": "provider-retry-test",
                "execution_node_id": "N6",
                "representation_sha256": "a" * 64,
                "prompt": prompt,
                "prompt_sha256": container_node.hashlib.sha256(
                    prompt.encode("utf-8")
                ).hexdigest(),
            }
            opener = RetryOpener()
            with (
                mock.patch.dict(os.environ, {
                    "PATHFINDER_SEMANTIC_LLM_BASE_URL": "http://127.0.0.1:1",
                    "PATHFINDER_SEMANTIC_LLM_MODEL": "test-model",
                    "PATHFINDER_SEMANTIC_LLM_API_KEY": "test-key",
                }),
                mock.patch.object(
                    container_node, "_semantic_llm_opener",
                    return_value=opener,
                ),
                mock.patch.object(container_node.time, "sleep"),
            ):
                runtime.semantic_complete(request)
            self.assertEqual(opener.calls, 2)
            trace_path = Path(directory) / "n6-provider-trace-v1.sqlite3"
            with closing(sqlite3.connect(
                f"file:{trace_path}?mode=ro", uri=True,
            )) as db:
                rows = db.execute("""
                    SELECT attempt_index, outcome, http_status,
                           header_request_id_sha256
                    FROM n6_provider_attempts ORDER BY attempt_index
                """).fetchall()
            self.assertEqual(
                [(row[0], row[1], row[2]) for row in rows],
                [(0, "http_error", 503), (1, "completed", 200)],
            )
            self.assertEqual(
                [row[3] for row in rows],
                [
                    container_node.hashlib.sha256(
                        value.encode("ascii")
                    ).hexdigest()
                    for value in (first_id, second_id)
                ],
            )
            self.assertNotIn(b"private", trace_path.read_bytes())

    def test_trace_journal_failure_does_not_repeat_paid_inference(
        self,
    ) -> None:
        request = {
            "schema_version": (
                container_node.CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION
            ),
            "semantic_request_id": "trace-write-failure",
        }
        with tempfile.TemporaryDirectory() as directory:
            runtime = ContainerNodeRuntime("N6", directory)

            def execute(_request: object, digest: str) -> dict[str, object]:
                attempts = runtime._semantic_provider_attempts.get()
                assert attempts is not None
                attempts.append({
                    "attempt_index": 0, "outcome": "completed",
                    "http_status": 200,
                    "body_completion_id_sha256": None,
                    "body_request_id_sha256": None,
                    "header_request_id_sha256": None,
                    "header_dashscope_request_id_sha256": None,
                })
                return {
                    "request_sha256": digest,
                    "provider_usage": {
                        "input_units": 10, "cached_input_units": 0,
                        "output_units": 1, "total_units": 11,
                    },
                }

            with (
                mock.patch.object(
                    runtime, "_semantic_complete_once",
                    side_effect=execute,
                ) as inference,
                mock.patch.object(
                    runtime, "_record_semantic_provider_attempts",
                    side_effect=OSError("disk unavailable"),
                ),
            ):
                runtime.semantic_complete(request)
                runtime.semantic_complete(request)
            self.assertEqual(inference.call_count, 1)
            self.assertEqual(
                runtime.health()["semantic_usage_journal_error_count"], 1,
            )


if __name__ == "__main__":
    unittest.main()
