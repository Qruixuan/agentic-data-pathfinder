"""N6 provider token receipts contain numbers only and fail closed."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from pathfinder.simulator import container_node
from pathfinder.simulator.container_node import ContainerNodeRuntime


class _Response:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def read(self, limit: int) -> bytes:
        return self.body[:limit]


class _Opener:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def open(self, *args: object, **kwargs: object) -> _Response:
        del args, kwargs
        return _Response(self.body)


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


if __name__ == "__main__":
    unittest.main()
