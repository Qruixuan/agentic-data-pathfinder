"""Freeze prebuilt-route quotes from cost-only development observations.

The historical t60 adapter is deliberately narrow.  It will not turn a
partially priced run, an unpriced retry, or a test/holdout run into offers.
One-time construction, storage occupancy, design transitions and the Agent
selection call belong to the separate episode ledger, not these offers.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any, Mapping


RATE_SCHEMA = "pathfinder.route-action-rate-card/v1"
TRACE_SCHEMA = "pathfinder.route-action-cost-only-dev-traces/v1"
QUOTE_SCHEMA = "pathfinder.route-action-frozen-quotes/v2"
QUOTE_SCOPE = "prebuilt-route-execution-list-price-allocation"
EXPECTED = {
    (f"D{number}", state)
    for number in range(8)
    for state in (("miss", "hit") if number in (3, 7) else (None,))
}
NODES = {"ROOT", *(f"N{number}" for number in range(1, 9))}
RATE_KEYS = {
    "schema_version", "observed_utc", "currency", "region",
    "qwen_model_id", "qwen_input_usd_per_1m",
    "qwen_cached_input_usd_per_1m", "qwen_output_usd_per_1m",
    "embedding_model_id", "embedding_input_usd_per_1m",
    "provider_source_url", "vm_source_url", "network_source_url",
    "network_incremental_usd_per_byte", "storage_occupancy_scope",
    "agent_call_scope", "build_scope", "node_plans", "plan_hourly_usd",
    "credentials_recorded",
}
UP_CLOUD_RATE_KEYS = {
    "upcloud_api_price_divisor", "upcloud_api_raw_plan_prices",
}
ROW_KEYS = {
    "action_id", "cache_state", "executor_node_id", "elapsed_ms",
    "n6_input_units", "n6_cached_input_units", "n6_output_units",
    "query_embedding_input_units", "result_sha256",
    "provider_attempt_count",
}
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_NANO = Decimal("0.000000001")
_MILLION = Decimal(1_000_000)


class RouteQuoteFreezeError(ValueError):
    """A price input is incomplete or its source binding does not verify."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise RouteQuoteFreezeError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(",", ":")).encode("utf-8") + b"\n"


def _decimal(value: Any, name: str) -> Decimal:
    _require(isinstance(value, str), f"{name} must be a decimal string")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise RouteQuoteFreezeError(f"{name} is invalid") from exc
    _require(number.is_finite() and number >= 0, f"{name} is invalid")
    return number


def _usd(value: Decimal) -> str:
    return format(value.quantize(_NANO, rounding=ROUND_HALF_UP), "f")


def _read_sealed(directory: Path, files: set[str]) -> dict[str, bytes]:
    _require(directory.is_dir(), "source package directory is missing")
    _require({path.name for path in directory.iterdir()} == files | {"SHA256SUMS"},
             "source package file set is not exact")
    sums = (directory / "SHA256SUMS").read_bytes().replace(b"\r\n", b"\n")
    payloads = {name: (directory / name).read_bytes() for name in files}
    expected = b"".join(
        sha256(payloads[name]).hexdigest().encode("ascii")
        + b"  " + name.encode("ascii") + b"\n"
        for name in sorted(files)
    )
    _require(sums == expected, "source package checksum is invalid")
    return payloads


def validate_rate_card(card: Mapping[str, Any]) -> Decimal:
    _require(set(card) in (RATE_KEYS, RATE_KEYS | UP_CLOUD_RATE_KEYS),
             "route rate card contains an unsupported field")
    _require(card.get("schema_version") == RATE_SCHEMA
             and card.get("currency") == "USD"
             and card.get("region") == "sg-sin1"
             and card.get("qwen_model_id") == "qwen3.8-27b"
             and card.get("embedding_model_id") == "text-embedding-v4"
             and card.get("credentials_recorded") is False,
             "route rate card identity is invalid")
    _require(isinstance(card.get("observed_utc"), str)
             and card["observed_utc"].endswith("Z")
             and all(isinstance(card.get(key), str) and card[key].startswith("https://")
                     for key in ("provider_source_url", "vm_source_url",
                                 "network_source_url")),
             "rate card lacks a dated official source")
    _require(card.get("network_incremental_usd_per_byte") == "0"
             and card.get("storage_occupancy_scope") == "separate-design-ledger"
             and card.get("agent_call_scope") == "separate-session-ledger"
             and card.get("build_scope") == "separate-design-ledger",
             "route quote scope is incomplete")
    plans = card.get("node_plans")
    prices = card.get("plan_hourly_usd")
    _require(isinstance(plans, dict) and set(plans) == NODES
             and isinstance(prices, dict)
             and set(prices) == set(plans.values()),
             "rate card does not cover the nine deployed VMs")
    if "upcloud_api_raw_plan_prices" in card:
        raw_prices = card["upcloud_api_raw_plan_prices"]
        _require(card.get("upcloud_api_price_divisor") == "100"
                 and isinstance(raw_prices, dict)
                 and set(raw_prices) == set(prices),
                 "UpCloud plan-price source is incomplete")
        _require(all(
            _decimal(raw_prices[plan], "UpCloud API plan price")
            == _decimal(prices[plan], "VM hourly rate") * 100
            for plan in prices
        ), "VM hourly rates differ from the captured UpCloud API")
    hourly = sum((_decimal(prices[plans[node]], "VM hourly rate")
                  for node in sorted(NODES)), Decimal(0))
    _require(hourly > 0, "VM hourly rate is zero")
    for key in ("qwen_input_usd_per_1m", "qwen_cached_input_usd_per_1m",
                "qwen_output_usd_per_1m", "embedding_input_usd_per_1m"):
        _require(_decimal(card.get(key), key) > 0, f"{key} is zero")
    return hourly


def _sample_cost(row: Mapping[str, Any], card: Mapping[str, Any],
                 hourly: Decimal,
                 vm_overhead_factor: Decimal = Decimal(1)
                 ) -> tuple[Decimal, Decimal, Decimal]:
    _require(set(row) == ROW_KEYS, "cost trace contains an unsupported field")
    key = (row["action_id"], row["cache_state"])
    _require(key in EXPECTED, "cost trace action/state is invalid")
    _require(row["executor_node_id"] == ("N7" if int(key[0][1]) < 4 else "N8"),
             "cost trace executor differs from action")
    _require(type(row["provider_attempt_count"]) is int
             and row["provider_attempt_count"] == 1,
             "cost trace has an unpriced or repeated provider attempt")
    _require(isinstance(row["result_sha256"], str)
             and _DIGEST.fullmatch(row["result_sha256"]),
             "cost trace result binding is invalid")
    _require(type(row["elapsed_ms"]) is int and row["elapsed_ms"] > 0,
             "cost trace elapsed time is invalid")
    units = (row["n6_input_units"], row["n6_cached_input_units"],
             row["n6_output_units"], row["query_embedding_input_units"])
    _require(all(type(value) is int and value >= 0 for value in units)
             and units[1] <= units[0], "cost trace token units are invalid")
    _require((key[0] in {"D1", "D5"}) or units[3] == 0,
             "non-index route charges query embedding")
    n6 = (
        Decimal(units[0] - units[1])
        * _decimal(card["qwen_input_usd_per_1m"], "input price")
        + Decimal(units[1])
        * _decimal(card["qwen_cached_input_usd_per_1m"], "cache price")
        + Decimal(units[2])
        * _decimal(card["qwen_output_usd_per_1m"], "output price")
    ) / _MILLION
    embedding = (Decimal(units[3])
                 * _decimal(card["embedding_input_usd_per_1m"], "embedding price")
                 / _MILLION)
    vm = (hourly * Decimal(row["elapsed_ms"]) * vm_overhead_factor
          / Decimal(3_600_000))
    return n6, embedding, vm


def calculate_quotes(traces: Mapping[str, Any],
                     card: Mapping[str, Any]) -> list[dict[str, Any]]:
    hourly = validate_rate_card(card)
    _require(set(traces) in ({
        "schema_version", "source_split", "source_accounting_sha256",
        "all_provider_attempts_joined", "outcomes_accessed",
        "credentials_recorded", "experiment_elapsed_seconds", "rows",
    }, {
        "schema_version", "source_split", "source_role",
        "source_accounting_sha256", "all_provider_attempts_joined",
        "outcomes_accessed", "credentials_recorded",
        "experiment_elapsed_seconds", "rows",
    }), "cost-only trace contains an unsupported field")
    _require(traces.get("schema_version") == TRACE_SCHEMA
             and traces.get("source_split") == "development-only"
             and traces.get("source_role", "historical-engineering-pilot-not-held-out")
             == "historical-engineering-pilot-not-held-out"
             and traces.get("outcomes_accessed") is False
             and traces.get("all_provider_attempts_joined") is True
             and traces.get("credentials_recorded") is False
             and isinstance(traces.get("source_accounting_sha256"), str)
             and _DIGEST.fullmatch(traces["source_accounting_sha256"]),
             "cost-only development trace provenance is invalid")
    rows = traces.get("rows")
    _require(isinstance(rows, list) and rows,
             "cost-only development traces are empty")
    route_elapsed_ms = sum(
        row.get("elapsed_ms", 0) for row in rows if isinstance(row, dict)
    )
    _require(type(route_elapsed_ms) is int and route_elapsed_ms > 0,
             "route elapsed-time total is invalid")
    experiment_ms = _decimal(traces.get("experiment_elapsed_seconds"),
                             "experiment elapsed seconds") * 1000
    _require(experiment_ms >= route_elapsed_ms,
             "batch elapsed time cannot cover its serial route times")
    vm_overhead_factor = experiment_ms / Decimal(route_elapsed_ms)
    groups: dict[tuple[str, str | None], list] = defaultdict(list)
    seen = set()
    for row in rows:
        _require(isinstance(row, dict), "cost trace row is invalid")
        costs = _sample_cost(row, card, hourly, vm_overhead_factor)
        _require(row["result_sha256"] not in seen,
                 "cost trace result identity repeats")
        seen.add(row["result_sha256"])
        groups[(row["action_id"], row["cache_state"])].append((row, costs))
    _require(set(groups) == EXPECTED
             and all(len(group) >= 2 for group in groups.values()),
             "development traces do not cover all ten action/states")
    quotes = []
    for action, state in sorted(EXPECTED, key=lambda key: (key[0], key[1] or "")):
        group = groups[(action, state)]
        count = Decimal(len(group))
        component_means = [sum((costs[index] for _, costs in group), Decimal(0))
                           / count for index in range(3)]
        latency = sum(row["elapsed_ms"] for row, _ in group) / count
        quotes.append({
            "action_id": action,
            "cache_state": state,
            "incremental_usd": _usd(sum(component_means, Decimal(0))),
            "expected_latency_ms": int(latency.to_integral_value(
                rounding=ROUND_HALF_UP)),
            "source_trace_count": len(group),
            "component_means_usd": {
                "n6_provider": _usd(component_means[0]),
                "query_embedding": _usd(component_means[1]),
                "shared_vm_time_allocation": _usd(component_means[2]),
            },
        })
    return quotes


def project_t60_development(accounting_dir: Path,
                            card: Mapping[str, Any]) -> dict[str, Any]:
    """Project cost-only rows; never copy scores, answers or task text."""
    payload = _read_sealed(accounting_dir, {"t60-list-cost-accounting.json"})[
        "t60-list-cost-accounting.json"]
    source = json.loads(payload)
    _require(source.get("schema_version")
             == "pathfinder.t60-list-cost-accounting/v1"
             and source.get("status") == "VERIFIED_NUMERIC_USAGE_MATCHED"
             and source.get("route_count") == 60
             and source.get("n6_usage_match_count") == 60
             and source.get("n6_completed_attempt_match_count") == 60
             and source.get("n6_retried_or_failed_attempt_count") == 0
             and len(source.get("rows", [])) == 60,
             "t60 accounting cannot support complete development quotes")
    # The frozen t60 query price is invertible at its historical $0.07 rate.
    _require(card["embedding_input_usd_per_1m"] == "0.07",
             "t60 query units require their original embedding rate")
    rows = []
    for old in source["rows"]:
        action = old["design_id"]
        repetition = old["repetition"]
        branches = old["cache_branches"]
        state = None
        if action in {"D3", "D7"}:
            _require(branches == (["miss", "miss"] if repetition == "r0000"
                                  else ["hit", "hit"]),
                     "t60 cache state is not bound")
            state = "miss" if repetition == "r0000" else "hit"
        else:
            _require(branches == [] and repetition == "r0000",
                     "t60 non-cache state is invalid")
        query_cost = _decimal(old["query_embedding_list_cost_usd"],
                              "t60 query embedding cost")
        query_units = query_cost * _MILLION / Decimal("0.07")
        _require(query_units == query_units.to_integral_value(),
                 "t60 query embedding units cannot be recovered exactly")
        row = {
            "action_id": action,
            "cache_state": state,
            "executor_node_id": old["executor_node_id"],
            "elapsed_ms": old["elapsed_ms"],
            "n6_input_units": old["n6_input_units"],
            "n6_cached_input_units": old["n6_cached_input_units"],
            "n6_output_units": old["n6_output_units"],
            "query_embedding_input_units": (
                int(query_units) if action in {"D1", "D5"} else 0
            ),
            "result_sha256": old["result_sha256"],
            "provider_attempt_count": 1,
        }
        n6, _, _ = _sample_cost(row, card, validate_rate_card(card))
        _require(_usd(n6) == old["n6_list_cost_usd"],
                 "t60 token usage does not reproduce source list cost")
        rows.append(row)
    result = {
        "schema_version": TRACE_SCHEMA,
        "source_split": "development-only",
        "source_role": "historical-engineering-pilot-not-held-out",
        "source_accounting_sha256": sha256(payload).hexdigest(),
        "experiment_elapsed_seconds": source["experiment_elapsed_seconds"],
        "all_provider_attempts_joined": True,
        "outcomes_accessed": False,
        "credentials_recorded": False,
        "rows": rows,
    }
    calculate_quotes(result, card)
    return result


def freeze_t60_quotes(accounting_dir: Path, rate_card_file: Path,
                      plan_sha256: str, output_dir: Path) -> dict[str, Any]:
    _require(isinstance(plan_sha256, str) and _DIGEST.fullmatch(plan_sha256),
             "target plan digest is invalid")
    _require(not output_dir.exists(), "quote package already exists")
    card = json.loads(rate_card_file.read_bytes())
    traces = project_t60_development(accounting_dir, card)
    trace_raw = _canonical(traces)
    card_raw = _canonical(card)
    manifest = {
        "schema_version": QUOTE_SCHEMA,
        "release_status": "DEVELOPMENT_QUOTE_NOT_SUBMISSION_ADMISSION",
        "plan_sha256": plan_sha256,
        "source_trace_package_sha256": sha256(trace_raw).hexdigest(),
        "rate_card_sha256": sha256(card_raw).hexdigest(),
        "source_split": "development-only",
        "outcomes_accessed": False,
        "incremental_quote_components_complete": True,
        "full_episode_cost_complete": False,
        "cost_scope": QUOTE_SCOPE,
        "price_character": "prediction-from-development-list-price-not-invoice",
        "credentials_recorded": False,
        "quotes": calculate_quotes(traces, card),
    }
    files = {
        "route-quotes.json": _canonical(manifest),
        "source-traces.json": trace_raw,
        "rate-card.json": card_raw,
    }
    output_dir.mkdir(parents=True)
    for name, raw in files.items():
        (output_dir / name).write_bytes(raw)
    (output_dir / "SHA256SUMS").write_bytes(b"".join(
        sha256(files[name]).hexdigest().encode("ascii")
        + b"  " + name.encode("ascii") + b"\n"
        for name in sorted(files)
    ))
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--t60-accounting-dir", type=Path, required=True)
    parser.add_argument("--rate-card", type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = freeze_t60_quotes(
        args.t60_accounting_dir, args.rate_card,
        args.plan_sha256, args.output_dir,
    )
    print(json.dumps({"status": "DEVELOPMENT_QUOTE_NOT_SUBMISSION_ADMISSION",
                      "quote_count": len(manifest["quotes"]),
                      "full_episode_cost_complete": False}))


if __name__ == "__main__":
    main()
