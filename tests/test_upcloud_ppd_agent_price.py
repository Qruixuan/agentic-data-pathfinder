"""Focused tests for conditional official list-price Agent accounting."""

import json
import unittest
from pathlib import Path

from experiments.upcloud_ppd_20260925.price_agent_usage import (
    price_observed_agent_usage,
)


TASK = "tsk-1228b33c-1842-4c1b-9d30-ce4bf4727daa"
RATES = {
    "currency": "CNY", "unit": "per_1m_tokens",
    "input_standard": "3.646", "output": "21.875",
}


def _usage():
    rows = [
        (1112, 53, 1165), (1485, 76, 1561), (6522, 54, 6576),
    ]
    return {
        "schema": "pathfinder.agent-sdk-usage/v1",
        "task_id": TASK,
        "completion_state": "complete",
        "aggregate_units_coherent": True,
        "request_entries_reconciled": True,
        "provider_attempts_reconciled": False,
        "cached_input_provider_verified": False,
        "sdk_request_count": 3,
        "raw_response_count": 3,
        "aggregate": {
            "input_tokens": 9119, "output_tokens": 183,
            "total_tokens": 9302, "cached_input_tokens_sdk": 0,
        },
        "request_units": [
            {"input_tokens": a, "output_tokens": b, "total_tokens": c,
             "cached_input_tokens_sdk": 0}
            for a, b, c in rows
        ],
    }


class AgentPriceTests(unittest.TestCase):
    def test_frozen_receipt_recomputes_from_numeric_units(self):
        root = (Path(__file__).resolve().parents[1] / "artifacts" /
                "ppd-agent-cost-20260927t161731z")
        frozen = json.loads((root / "agent-cost-receipt.json").read_text())
        rates = json.loads((root / "price-snapshot.json").read_text())
        usage = _usage()
        usage["task_id"] = frozen["task_id"]
        usage["aggregate"] = {
            key: frozen[key] for key in (
                "input_tokens", "output_tokens", "total_tokens",
                "cached_input_tokens_sdk",
            )
        }
        usage["request_units"] = frozen["request_units"]
        usage["sdk_request_count"] = frozen["sdk_request_count"]
        usage["raw_response_count"] = frozen["raw_response_count"]
        result = price_observed_agent_usage(
            usage=usage, rates=rates, expected_task_id=frozen["task_id"],
        )
        for name in (
            "input_standard_list_price_cny", "output_list_price_cny",
            "observed_sdk_units_list_price_cny",
        ):
            self.assertEqual(frozen[name], result[name])

    def test_exact_decimal_quote_keeps_incomplete_status(self):
        result = price_observed_agent_usage(
            usage=_usage(), rates=RATES, expected_task_id=TASK,
        )
        self.assertEqual("0.033247874", result["input_standard_list_price_cny"])
        self.assertEqual("0.004003125", result["output_list_price_cny"])
        self.assertEqual("0.037250999", result["observed_sdk_units_list_price_cny"])
        self.assertFalse(result["complete_episode_cost"])
        self.assertFalse(result["provider_attempts_reconciled"])

    def test_wrong_task_and_missing_request_rejected(self):
        with self.assertRaisesRegex(ValueError, "task binding"):
            price_observed_agent_usage(
                usage=_usage(), rates=RATES, expected_task_id="tsk-other",
            )
        value = _usage()
        value["request_units"].pop()
        with self.assertRaisesRegex(ValueError, "request count"):
            price_observed_agent_usage(
                usage=value, rates=RATES, expected_task_id=TASK,
            )

    def test_mismatched_units_and_bad_rate_rejected(self):
        value = _usage()
        value["aggregate"]["input_tokens"] += 1
        with self.assertRaisesRegex(ValueError, "aggregate differs"):
            price_observed_agent_usage(
                usage=value, rates=RATES, expected_task_id=TASK,
            )
        with self.assertRaisesRegex(ValueError, "rate-card unit"):
            price_observed_agent_usage(
                usage=_usage(), rates={**RATES, "unit": "per_token"},
                expected_task_id=TASK,
            )
        with self.assertRaisesRegex(ValueError, "input_standard is invalid"):
            price_observed_agent_usage(
                usage=_usage(), rates={**RATES, "input_standard": "not-a-rate"},
                expected_task_id=TASK,
            )


if __name__ == "__main__":
    unittest.main()
