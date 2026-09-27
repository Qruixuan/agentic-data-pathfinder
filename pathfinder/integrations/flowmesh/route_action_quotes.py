"""Fail-closed loader for cost-only development route estimates.

The quote covers prebuilt route execution, including allocated VM time. It
does not claim the full episode/build/transition/Agent cost or an invoice.
"""

from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

from .route_action_bridge import (
    RouteActionBridgeError, RouteQuote,
)
from .route_action_quote_freezer import (
    EXPECTED, QUOTE_SCHEMA, QUOTE_SCOPE, RouteQuoteFreezeError,
    _read_sealed, calculate_quotes,
)


_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_QUOTE_FILE = "route-quotes.json"
_DESIGNS = {
    "D_base": {"R", "D"},
    "D_index": {"R", "D", "I"},
    "D_cache": {"R", "D", "DC"},
    "D_joint": {"R", "D", "I", "DC"},
}
_ARMS = ("R", "I", "D", "DC", "R", "I", "D", "DC")


def _require(condition: object, message: str) -> None:
    if not condition:
        raise RouteActionBridgeError(message)


class FrozenRouteQuoteSource:
    """Expose only complete, development-sourced, checksum-bound quotes."""

    def __init__(
        self, package_dir: str | Path, *, plan_sha256: str,
        source_trace_package_sha256: str, rate_card_sha256: str,
    ) -> None:
        expected_bindings = {
            "plan_sha256": plan_sha256,
            "source_trace_package_sha256": source_trace_package_sha256,
            "rate_card_sha256": rate_card_sha256,
        }
        _require(all(isinstance(value, str) and _DIGEST.fullmatch(value)
                     for value in expected_bindings.values()),
                 "quote source bindings are invalid")
        root = Path(package_dir)
        try:
            files = _read_sealed(root, {
                _QUOTE_FILE, "source-traces.json", "rate-card.json",
            })
        except RouteQuoteFreezeError as exc:
            raise RouteActionBridgeError(str(exc)) from exc
        raw = files[_QUOTE_FILE]
        _require(sha256(files["source-traces.json"]).hexdigest()
                 == source_trace_package_sha256
                 and sha256(files["rate-card.json"]).hexdigest()
                 == rate_card_sha256,
                 "quote source or rate-card bytes differ from binding")
        try:
            manifest = json.loads(raw)
            traces = json.loads(files["source-traces.json"])
            rate_card = json.loads(files["rate-card.json"])
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise RouteActionBridgeError("quote package is invalid JSON") from exc
        _require(isinstance(manifest, dict)
                 and manifest.get("schema_version") == QUOTE_SCHEMA
                 and manifest.get("release_status")
                 == "DEVELOPMENT_QUOTE_NOT_SUBMISSION_ADMISSION"
                 and all(manifest.get(key) == value
                         for key, value in expected_bindings.items())
                 and manifest.get("source_split") == "development-only"
                 and manifest.get("outcomes_accessed") is False
                 and manifest.get("incremental_quote_components_complete") is True
                 and manifest.get("full_episode_cost_complete") is False
                 and manifest.get("cost_scope") == QUOTE_SCOPE
                 and manifest.get("price_character")
                 == "prediction-from-development-list-price-not-invoice"
                 and manifest.get("credentials_recorded") is False,
                 "quote package is not a bounded pre-outcome estimate")
        try:
            calculated = calculate_quotes(traces, rate_card)
        except (RouteQuoteFreezeError, AttributeError, KeyError, TypeError) as exc:
            raise RouteActionBridgeError(
                "quote evidence is incomplete or invalid"
            ) from exc
        rows = manifest.get("quotes")
        _require(isinstance(rows, list) and len(rows) == len(EXPECTED),
                 "quote package does not cover ten route observations")
        _require(rows == calculated,
                 "quote values do not recompute from development traces")
        self.price_basis_sha256 = sha256(raw).hexdigest()
        self.plan_sha256 = plan_sha256
        quotes: dict[tuple[str, str | None], RouteQuote] = {}
        for row in rows:
            _require(isinstance(row, Mapping), "quote row is invalid")
            key = (row.get("action_id"), row.get("cache_state"))
            _require(key in EXPECTED and key not in quotes,
                     "quote action/state is absent or repeated")
            quote = RouteQuote(
                incremental_usd=row.get("incremental_usd"),
                expected_latency_ms=row.get("expected_latency_ms"),
                price_basis_sha256=self.price_basis_sha256,
                source_trace_count=row.get("source_trace_count"),
            )
            quote.validate()
            quotes[key] = quote
        _require(set(quotes) == EXPECTED,
                 "quote package lacks an action/cache state")
        self._quotes = quotes

    def quotes_for(
        self, question_id: str, physical_design_id: str,
    ) -> Mapping[tuple[str, str | None], RouteQuote]:
        _require(isinstance(question_id, str) and bool(question_id),
                 "quote question identity is invalid")
        _require(physical_design_id in _DESIGNS,
                 "quote physical design is unsupported")
        arms = _DESIGNS[physical_design_id]
        return {
            key: quote for key, quote in self._quotes.items()
            if _ARMS[int(key[0][1])] in arms
        }
