"""Quote recomputation is offline and cannot use hidden outcomes."""

from hashlib import sha256
from pathlib import Path
import tempfile
import unittest

from pathfinder.integrations.flowmesh.route_action_bridge import (
    RouteActionBridgeError,
)
from pathfinder.integrations.flowmesh.route_action_quote_freezer import (
    QUOTE_SCHEMA, QUOTE_SCOPE, RATE_SCHEMA, TRACE_SCHEMA, _canonical,
    calculate_quotes,
)
from pathfinder.integrations.flowmesh.route_action_quotes import (
    FrozenRouteQuoteSource,
)


class FrozenRouteQuoteSourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.card = {
            "schema_version": RATE_SCHEMA,
            "observed_utc": "2026-09-27T08:00:00Z",
            "currency": "USD",
            "region": "sg-sin1",
            "qwen_model_id": "qwen3.8-27b",
            "qwen_input_usd_per_1m": "0.50",
            "qwen_cached_input_usd_per_1m": "0.10",
            "qwen_output_usd_per_1m": "3.00",
            "embedding_model_id": "text-embedding-v4",
            "embedding_input_usd_per_1m": "0.07",
            "provider_source_url": "https://example.org/provider",
            "vm_source_url": "https://example.org/vm",
            "network_source_url": "https://example.org/network",
            "network_incremental_usd_per_byte": "0",
            "storage_occupancy_scope": "separate-design-ledger",
            "agent_call_scope": "separate-session-ledger",
            "build_scope": "separate-design-ledger",
            "node_plans": {node: "small" for node in
                           ("ROOT", *(f"N{number}" for number in range(1, 9)))},
            "plan_hourly_usd": {"small": "0.01"},
            "credentials_recorded": False,
        }
        rows = []
        for number in range(8):
            for state in (("miss", "hit") if number in (3, 7) else (None,)):
                for repeat in range(2):
                    rows.append({
                        "action_id": f"D{number}",
                        "cache_state": state,
                        "executor_node_id": "N7" if number < 4 else "N8",
                        "elapsed_ms": 100 + repeat,
                        "n6_input_units": 1000,
                        "n6_cached_input_units": 100,
                        "n6_output_units": 10,
                        "query_embedding_input_units": (
                            10 if number in (1, 5) else 0
                        ),
                        "result_sha256": sha256(
                            f"{number}-{state}-{repeat}".encode()
                        ).hexdigest(),
                        "provider_attempt_count": 1,
                    })
        self.traces = {
            "schema_version": TRACE_SCHEMA,
            "source_split": "development-only",
            "source_accounting_sha256": "f" * 64,
            "experiment_elapsed_seconds": "2.010",
            "all_provider_attempts_joined": True,
            "outcomes_accessed": False,
            "credentials_recorded": False,
            "rows": rows,
        }
        self.manifest = {
            "schema_version": QUOTE_SCHEMA,
            "release_status": "DEVELOPMENT_QUOTE_NOT_SUBMISSION_ADMISSION",
            "plan_sha256": "a" * 64,
            "source_trace_package_sha256": sha256(
                _canonical(self.traces)
            ).hexdigest(),
            "rate_card_sha256": sha256(_canonical(self.card)).hexdigest(),
            "source_split": "development-only",
            "outcomes_accessed": False,
            "incremental_quote_components_complete": True,
            "full_episode_cost_complete": False,
            "cost_scope": QUOTE_SCOPE,
            "price_character": "prediction-from-development-list-price-not-invoice",
            "credentials_recorded": False,
            "quotes": calculate_quotes(self.traces, self.card),
        }

    def freeze(self):
        files = {
            "route-quotes.json": _canonical(self.manifest),
            "source-traces.json": _canonical(self.traces),
            "rate-card.json": _canonical(self.card),
        }
        for name, payload in files.items():
            (self.root / name).write_bytes(payload)
        (self.root / "SHA256SUMS").write_bytes(b"".join(
            f"{sha256(files[name]).hexdigest()}  {name}\n".encode("ascii")
            for name in sorted(files)
        ))

    def load(self):
        return FrozenRouteQuoteSource(
            self.root,
            plan_sha256=self.manifest["plan_sha256"],
            source_trace_package_sha256=self.manifest[
                "source_trace_package_sha256"],
            rate_card_sha256=self.manifest["rate_card_sha256"],
        )

    def test_quotes_recomputed_and_filtered_by_design(self):
        self.freeze()
        source = self.load()
        self.assertEqual(4, len(source.quotes_for("public-q", "D_base")))
        self.assertEqual(10, len(source.quotes_for("public-q", "D_joint")))
        self.assertTrue(all(quote.source_trace_count == 2 for quote in
                            source.quotes_for("public-q", "D_joint").values()))

    def test_tampered_quote_cannot_self_declare_complete(self):
        self.manifest["quotes"][0]["incremental_usd"] = "0.000000000"
        self.freeze()
        with self.assertRaisesRegex(RouteActionBridgeError, "recompute"):
            self.load()

    def test_outcome_field_is_rejected_even_when_quote_is_recomputed(self):
        self.traces["rows"][0]["task_success"] = True
        self.manifest["source_trace_package_sha256"] = sha256(
            _canonical(self.traces)
        ).hexdigest()
        self.freeze()
        with self.assertRaisesRegex(RouteActionBridgeError, "evidence"):
            self.load()

    def test_unpriced_retry_is_rejected(self):
        self.traces["rows"][0]["provider_attempt_count"] = 2
        self.manifest["source_trace_package_sha256"] = sha256(
            _canonical(self.traces)
        ).hexdigest()
        self.freeze()
        with self.assertRaisesRegex(RouteActionBridgeError, "evidence"):
            self.load()

    def test_holdout_role_and_unrecognized_rate_field_are_rejected(self):
        self.traces["source_role"] = "held-out-test"
        self.manifest["source_trace_package_sha256"] = sha256(
            _canonical(self.traces)
        ).hexdigest()
        self.freeze()
        with self.assertRaisesRegex(RouteActionBridgeError, "evidence"):
            self.load()
        self.traces.pop("source_role")
        self.manifest["source_trace_package_sha256"] = sha256(
            _canonical(self.traces)
        ).hexdigest()
        self.card["secret_value"] = "not-allowed"
        self.manifest["rate_card_sha256"] = sha256(
            _canonical(self.card)
        ).hexdigest()
        self.freeze()
        with self.assertRaisesRegex(RouteActionBridgeError, "evidence"):
            self.load()

    def test_batch_overhead_is_allocated_to_vm_time(self):
        base = calculate_quotes(self.traces, self.card)
        self.traces["experiment_elapsed_seconds"] = "4.020"
        doubled = calculate_quotes(self.traces, self.card)
        self.assertEqual(
            base[0]["component_means_usd"]["n6_provider"],
            doubled[0]["component_means_usd"]["n6_provider"],
        )
        self.assertAlmostEqual(
            2 * float(base[0]["component_means_usd"][
                "shared_vm_time_allocation"]),
            float(doubled[0]["component_means_usd"][
                "shared_vm_time_allocation"]),
            delta=0.000000001,
        )

    def test_incomplete_or_outcome_sourced_package_is_rejected(self):
        for change in (
            {"incremental_quote_components_complete": False},
            {"full_episode_cost_complete": True},
            {"outcomes_accessed": True},
            {"source_split": "test-and-development"},
        ):
            with self.subTest(change=change):
                self.manifest.update(change)
                self.freeze()
                with self.assertRaisesRegex(
                    RouteActionBridgeError, "not a bounded pre-outcome"
                ):
                    self.load()
                self.manifest.update({
                    "incremental_quote_components_complete": True,
                    "full_episode_cost_complete": False,
                    "outcomes_accessed": False,
                    "source_split": "development-only",
                })

    def test_checksum_and_action_coverage_are_required(self):
        self.freeze()
        (self.root / "route-quotes.json").write_bytes(b"{}")
        with self.assertRaisesRegex(RouteActionBridgeError, "checksum"):
            self.load()
        self.manifest["quotes"].pop()
        self.freeze()
        with self.assertRaisesRegex(RouteActionBridgeError, "does not cover"):
            self.load()


if __name__ == "__main__":
    unittest.main()
