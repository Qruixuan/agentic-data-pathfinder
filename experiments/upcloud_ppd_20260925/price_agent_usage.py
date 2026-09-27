"""Price observed PPD Agent token units against a dated official rate card.

The output is a conditional list-price subtotal, never an invoice or a
complete episode cost. Unknown provider attempts and cache treatment remain
explicitly outside the calculation.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


def _count(value: Any, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _rate(value: Any, name: str) -> Decimal:
    try:
        result = Decimal(value)
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f"{name} is invalid") from exc
    if not result.is_finite() or result < 0:
        raise ValueError(f"{name} is invalid")
    return result


def price_observed_agent_usage(
    *, usage: Mapping[str, Any], rates: Mapping[str, Any],
    expected_task_id: str,
) -> dict[str, Any]:
    """Validate task-bound SDK units, then price them at uncached rates.

    The uncached calculation is conditional when provider cache accounting
    is unverified. It also excludes any provider attempt absent from the SDK
    aggregate, so ``complete_episode_cost`` is always false here.
    """
    if usage.get("schema") != "pathfinder.agent-sdk-usage/v1":
        raise ValueError("usage schema differs")
    if usage.get("task_id") != expected_task_id:
        raise ValueError("usage task binding differs")
    if usage.get("completion_state") != "complete":
        raise ValueError("Agent task did not complete")
    if usage.get("aggregate_units_coherent") is not True:
        raise ValueError("SDK aggregate is not coherent")
    if usage.get("request_entries_reconciled") is not True:
        raise ValueError("SDK request entries do not reconcile")
    entries = usage.get("request_units")
    if not isinstance(entries, list) or not entries:
        raise ValueError("SDK request entries are missing")
    if _count(usage.get("sdk_request_count"), "sdk_request_count") != len(entries):
        raise ValueError("SDK request count differs")
    if _count(usage.get("raw_response_count"), "raw_response_count") != len(entries):
        raise ValueError("raw response count differs")
    aggregate = usage.get("aggregate")
    if not isinstance(aggregate, Mapping):
        raise ValueError("SDK aggregate is missing")
    summed = {name: 0 for name in (
        "input_tokens", "output_tokens", "total_tokens",
        "cached_input_tokens_sdk",
    )}
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise ValueError("SDK request entry is invalid")
        for name in summed:
            summed[name] += _count(entry.get(name), name)
        if entry["input_tokens"] + entry["output_tokens"] != (
            entry["total_tokens"]
        ):
            raise ValueError("SDK request units do not balance")
        if entry["cached_input_tokens_sdk"] > entry["input_tokens"]:
            raise ValueError("SDK cached input exceeds input")
    for name, value in summed.items():
        if _count(aggregate.get(name), name) != value:
            raise ValueError("SDK aggregate differs from request entries")
    if rates.get("currency") != "CNY" or rates.get("unit") != "per_1m_tokens":
        raise ValueError("rate-card unit differs")
    input_rate = _rate(rates.get("input_standard"), "input_standard")
    output_rate = _rate(rates.get("output"), "output")
    divisor = Decimal(1_000_000)
    input_cost = Decimal(summed["input_tokens"]) * input_rate / divisor
    output_cost = Decimal(summed["output_tokens"]) * output_rate / divisor
    return {
        "task_id": expected_task_id,
        "sdk_request_count": len(entries),
        "input_tokens": summed["input_tokens"],
        "output_tokens": summed["output_tokens"],
        "cached_input_tokens_sdk": summed["cached_input_tokens_sdk"],
        "currency": "CNY",
        "input_standard_list_price_cny": format(input_cost, "f"),
        "output_list_price_cny": format(output_cost, "f"),
        "observed_sdk_units_list_price_cny": format(input_cost + output_cost, "f"),
        "pricing_assumption": "all-input-at-standard-rate",
        "cached_input_provider_verified": (
            usage.get("cached_input_provider_verified") is True
        ),
        "provider_attempts_reconciled": (
            usage.get("provider_attempts_reconciled") is True
        ),
        "complete_episode_cost": False,
        "invoice_amount": None,
    }
