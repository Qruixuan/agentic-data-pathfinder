"""Offline tests for exact Agent HTTP-attempt to provider-audit joins."""

import hashlib
import json
import unittest
from unittest.mock import patch

from experiments.upcloud_ppd_20260925 import (
    reconcile_agent_provider_usage as reconciliation,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _fixture():
    sidecar = {
        "schema": "pathfinder.agent-sdk-usage/v1",
        "completion_state": "complete",
        "task_id": "tsk-00000000-0000-4000-8000-000000000001",
        "request_entries_reconciled": True,
        "aggregate_units_coherent": True,
        "sdk_request_count": 2,
        "raw_response_count": 2,
        "provider_transport_attempt_count": 2,
        "provider_transport_attempts": [
            {"attempt_index": index, "http_status": 200,
             "transport_error_class": None,
             "request_id_sha256": _digest(f"request-{index}")}
            for index in (1, 2)
        ],
        "request_units": [
            {"input_tokens": 10, "output_tokens": 2},
            {"input_tokens": 20, "output_tokens": 3},
        ],
        "aggregate": {"input_tokens": 30, "output_tokens": 5},
    }
    provider = [
        {"request_id_sha256": _digest("request-2"),
         "input_units": 20, "cached_input_units": 5,
         "output_units": 3},
        {"request_id_sha256": _digest("request-1"),
         "input_units": 10, "cached_input_units": 0,
         "output_units": 2},
    ]
    card = {
        "qwen_input_usd_per_1m": "0.50",
        "qwen_cached_input_usd_per_1m": "0.10",
        "qwen_output_usd_per_1m": "3.00",
    }
    return sidecar, provider, card


class AgentProviderReconciliationTests(unittest.TestCase):
    def _reconcile(self, sidecar, provider, card):
        with patch.object(reconciliation, "validate_rate_card"):
            return reconciliation.reconcile_agent_provider_rows(
                sidecar, provider, card,
                expected_task_id="tsk-00000000-0000-4000-8000-000000000001",
            )

    def test_exact_ids_and_units_price_provider_verified_cache(self):
        sidecar, provider, card = _fixture()
        report = self._reconcile(sidecar, provider, card)
        self.assertEqual("VERIFIED_AGENT_PROVIDER_REQUEST_JOIN",
                         report["status"])
        self.assertEqual(2, report["provider_request_id_match_count"])
        self.assertEqual(5, report["cached_input_units"])
        self.assertEqual("0.000028000", report["agent_list_price_usd"])
        self.assertNotIn("request-1", json.dumps(report))

    def test_missing_provider_id_fails_closed(self):
        sidecar, provider, card = _fixture()
        provider.pop()
        with self.assertRaisesRegex(ValueError, "absent"):
            self._reconcile(sidecar, provider, card)

    def test_failed_or_retried_attempt_is_not_zero_priced(self):
        sidecar, provider, card = _fixture()
        sidecar["provider_transport_attempts"][0]["http_status"] = 429
        with self.assertRaisesRegex(ValueError, "did not complete once"):
            self._reconcile(sidecar, provider, card)

    def test_token_disagreement_fails_closed(self):
        sidecar, provider, card = _fixture()
        provider[0]["input_units"] += 1
        with self.assertRaisesRegex(ValueError, "tokens differ"):
            self._reconcile(sidecar, provider, card)

    def test_missing_http_capture_fails_closed(self):
        sidecar, provider, card = _fixture()
        sidecar["provider_transport_attempt_count"] = 0
        with self.assertRaisesRegex(ValueError, "count differs"):
            self._reconcile(sidecar, provider, card)

    def test_wrong_task_sidecar_fails_closed(self):
        sidecar, provider, card = _fixture()
        sidecar["task_id"] = "tsk-00000000-0000-4000-8000-000000000002"
        with self.assertRaisesRegex(ValueError, "another task"):
            self._reconcile(sidecar, provider, card)


if __name__ == "__main__":
    unittest.main()
