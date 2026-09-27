"""No-network checks for the bounded FlowMesh Agent usage diagnostic."""

import unittest

from experiments.upcloud_ppd_20260925.agent_usage_numeric import (
    summarize_agent_usage,
)


class AgentUsageNumericTests(unittest.TestCase):
    def test_numeric_only_and_no_completeness_claim(self):
        report = summarize_agent_usage({
            "usage": {
                "num_requests": 2,
                "input_tokens": 120,
                "output_tokens": 15,
                "prompt_tokens_details": {"cached_tokens": 20},
                "prompt": "private text",
                "api_key": "private value",
            },
            "output": "private answer",
        })
        self.assertEqual({
            "num_requests": 2, "input_tokens": 120,
            "output_tokens": 15, "cached_input_tokens": 20,
        }, report["numeric_usage"])
        self.assertTrue(report["cached_input_units_known"])
        self.assertFalse(report["provider_attempts_reconciled"])
        self.assertFalse(report["complete_list_price_claimed"])
        self.assertNotIn("private", str(report))

    def test_missing_cache_detail_stays_unknown(self):
        report = summarize_agent_usage({
            "usage": {"num_requests": 1, "prompt_tokens": 10,
                      "completion_tokens": 3}
        })
        self.assertFalse(report["cached_input_units_known"])
        self.assertNotIn("cached_input_tokens", report["numeric_usage"])

    def test_no_usage_stays_unknown(self):
        report = summarize_agent_usage({"output": "not relevant"})
        self.assertFalse(report["usage_present"])
        self.assertFalse(report["complete_list_price_claimed"])

    def test_invalid_cache_accounting_rejected(self):
        with self.assertRaisesRegex(ValueError, "cached tokens exceed"):
            summarize_agent_usage({"usage": {
                "input_tokens": 4,
                "prompt_tokens_details": {"cached_tokens": 5},
            }})


if __name__ == "__main__":
    unittest.main()
