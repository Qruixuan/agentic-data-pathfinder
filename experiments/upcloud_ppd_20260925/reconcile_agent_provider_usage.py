"""Join a task's private Agent sidecar to Model Studio audit Request IDs.

Only hashed identifiers and numeric usage leave this function. A missing
HTTP attempt, provider row, or cache detail is an accounting failure rather
than a zero-dollar request. This does not submit a FlowMesh task.
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from pathfinder.integrations.flowmesh.route_action_quote_freezer import (
    validate_rate_card,
)
from pathfinder.rsi_exam.offline_replay_costing import (
    _workbook_request_id_rows, _workbook_selected_cells,
)


_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_TASK_ID = re.compile(
    r"tsk-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
_MILLION = Decimal(1_000_000)


def _require(condition: object, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _units(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    _require(type(value) is int and value >= 0, f"{key} is missing")
    return value


def reconcile_agent_provider_rows(
    sidecar: Mapping[str, Any],
    provider_rows: Sequence[Mapping[str, Any]],
    rate_card: Mapping[str, Any],
    *, expected_task_id: str,
) -> dict[str, Any]:
    """Require exact request-ID coverage and token agreement for one task."""
    validate_rate_card(rate_card)
    _require(isinstance(expected_task_id, str)
             and _TASK_ID.fullmatch(expected_task_id)
             and sidecar.get("task_id") == expected_task_id,
             "Agent usage sidecar belongs to another task")
    _require(sidecar.get("schema") == "pathfinder.agent-sdk-usage/v1"
             and sidecar.get("completion_state") == "complete"
             and sidecar.get("request_entries_reconciled") is True
             and sidecar.get("aggregate_units_coherent") is True,
             "Agent SDK usage is not complete")
    attempts = sidecar.get("provider_transport_attempts")
    entries = sidecar.get("request_units")
    count = sidecar.get("sdk_request_count")
    _require(isinstance(attempts, list) and isinstance(entries, list)
             and type(count) is int and count > 0
             and len(attempts) == len(entries) == count
             and sidecar.get("provider_transport_attempt_count") == count
             and sidecar.get("raw_response_count") == count,
             "Agent provider attempt count differs from SDK responses")
    ids = []
    for index, attempt in enumerate(attempts, 1):
        _require(isinstance(attempt, Mapping)
                 and attempt.get("attempt_index") == index
                 and attempt.get("http_status") == 200
                 and attempt.get("transport_error_class") is None,
                 "Agent provider attempt did not complete once")
        digest = attempt.get("request_id_sha256")
        _require(isinstance(digest, str) and _DIGEST.fullmatch(digest),
                 "Agent provider attempt lacks a Request ID digest")
        ids.append(digest)
    _require(len(ids) == len(set(ids)), "Agent provider Request ID repeats")
    by_id = {}
    for row in provider_rows:
        digest = row.get("request_id_sha256")
        _require(isinstance(digest, str) and _DIGEST.fullmatch(digest)
                 and digest not in by_id,
                 "provider audit Request ID is invalid or duplicated")
        by_id[digest] = row
    _require(all(digest in by_id for digest in ids),
             "Agent Request ID is absent from provider audit")
    matched = [by_id[digest] for digest in ids]
    sdk_pairs = Counter(
        (_units(row, "input_tokens"), _units(row, "output_tokens"))
        for row in entries
    )
    provider_pairs = Counter(
        (_units(row, "input_units"), _units(row, "output_units"))
        for row in matched
    )
    _require(sdk_pairs == provider_pairs,
             "Agent SDK tokens differ from provider audit")
    standard_rate = Decimal(rate_card["qwen_input_usd_per_1m"])
    cached_rate = Decimal(rate_card["qwen_cached_input_usd_per_1m"])
    output_rate = Decimal(rate_card["qwen_output_usd_per_1m"])
    input_total = cached_total = output_total = 0
    price = Decimal(0)
    for row in matched:
        incoming = _units(row, "input_units")
        cached = _units(row, "cached_input_units")
        outgoing = _units(row, "output_units")
        _require(cached <= incoming, "provider cached units exceed input")
        input_total += incoming
        cached_total += cached
        output_total += outgoing
        price += ((incoming - cached) * standard_rate
                  + cached * cached_rate + outgoing * output_rate) / _MILLION
    aggregate = sidecar.get("aggregate")
    _require(isinstance(aggregate, Mapping)
             and _units(aggregate, "input_tokens") == input_total
             and _units(aggregate, "output_tokens") == output_total,
             "Agent aggregate differs from provider audit")
    return {
        "schema_version": "pathfinder.ppd-agent-provider-request-join/v1",
        "status": "VERIFIED_AGENT_PROVIDER_REQUEST_JOIN",
        "task_id": sidecar.get("task_id"),
        "provider_request_id_match_count": len(matched),
        "provider_audit_unmatched_row_count": len(by_id) - len(matched),
        "input_units": input_total,
        "cached_input_units": cached_total,
        "output_units": output_total,
        "agent_list_price_usd": format(price.quantize(
            Decimal("0.000000001"), rounding=ROUND_HALF_UP,
        ), "f"),
        "raw_request_ids_recorded": False,
        "credentials_recorded": False,
    }


def reconcile_agent_provider_workbook(
    sidecar_file: str | Path, workbook_file: str | Path,
    rate_card_file: str | Path,
    *, expected_task_id: str,
) -> dict[str, Any]:
    """Read existing private evidence; emit only a credential-free report."""
    sidecar = json.loads(Path(sidecar_file).read_bytes())
    card = json.loads(Path(rate_card_file).read_bytes())
    rows = _workbook_request_id_rows(workbook_file)
    result = reconcile_agent_provider_rows(
        sidecar, rows, card, expected_task_id=expected_task_id,
    )
    result["provider_log_sha256"] = sha256(
        Path(workbook_file).read_bytes()
    ).hexdigest()
    result["numeric_usage_sidecar_sha256"] = sha256(
        Path(sidecar_file).read_bytes()
    ).hexdigest()
    return result


def corroborate_legacy_agent_provider_rows(
    receipt: Mapping[str, Any], provider_rows: Sequence[Mapping[str, Any]],
    rate_card: Mapping[str, Any],
) -> dict[str, Any]:
    """Corroborate old SDK units without claiming a Request-ID task join.

    Legacy sidecars did not capture HTTP Request IDs. Unique token pairs in
    one export are useful evidence, but cannot establish attempt coverage.
    """
    _require(receipt.get("schema_version")
             == "pathfinder.ppd-agent-observed-list-price-subtotal/v1"
             and receipt.get("status")
             == "CONDITIONAL_AGENT_SDK_LIST_PRICE_SUBTOTAL"
             and receipt.get("sdk_request_entries_reconciled") is True
             and receipt.get("provider_attempts_reconciled") is False,
             "legacy SDK cost receipt is not eligible for corroboration")
    _require(rate_card.get("schema_version")
             == "pathfinder.ppd-agent-official-list-price-snapshot/v1"
             and rate_card.get("model_id") == receipt.get("model_id")
             and rate_card.get("currency") == "CNY",
             "legacy model price snapshot differs")
    entries = receipt.get("request_units")
    count = receipt.get("sdk_request_count")
    _require(isinstance(entries, list) and type(count) is int
             and count > 0 and len(entries) == count,
             "legacy SDK request count differs")
    by_pair: dict[tuple[int, int], list[Mapping[str, Any]]] = {}
    seen_ids: set[str] = set()
    for row in provider_rows:
        request_id = row.get("request_id_sha256")
        _require(isinstance(request_id, str)
                 and _DIGEST.fullmatch(request_id)
                 and request_id not in seen_ids,
                 "provider Request ID is invalid or repeated")
        seen_ids.add(request_id)
        pair = (_units(row, "input_units"), _units(row, "output_units"))
        by_pair.setdefault(pair, []).append(row)
    matched = []
    for entry in entries:
        pair = (_units(entry, "input_tokens"),
                _units(entry, "output_tokens"))
        _require(len(by_pair.get(pair, [])) == 1,
                 "legacy SDK token pair is absent or ambiguous")
        matched.append(by_pair[pair][0])
    _require(len({row["request_id_sha256"] for row in matched}) == count,
             "legacy SDK entries do not map to distinct provider rows")
    input_total = sum(_units(row, "input_units") for row in matched)
    output_total = sum(_units(row, "output_units") for row in matched)
    cached_total = sum(_units(row, "cached_input_units") for row in matched)
    _require(all(row.get("cached_detail_explicit") is True for row in matched)
             and cached_total <= input_total,
             "provider cache detail is missing or invalid")
    _require(input_total == _units(receipt, "input_tokens")
             and output_total == _units(receipt, "output_tokens"),
             "provider numeric totals differ from legacy SDK receipt")
    sidecar_sha = receipt.get("numeric_usage_sidecar_sha256")
    _require(isinstance(sidecar_sha, str) and _DIGEST.fullmatch(sidecar_sha),
             "legacy numeric sidecar binding is absent")
    standard = Decimal(rate_card["input_standard"])
    cached = Decimal(rate_card["input_implicit_cache"])
    output = Decimal(rate_card["output"])
    _require(all(value.is_finite() and value >= 0 for value in
                 (standard, cached, output)), "price rate is invalid")
    price = ((input_total - cached_total) * standard
             + cached_total * cached + output_total * output) / _MILLION
    return {
        "schema_version": "pathfinder.ppd-legacy-agent-corroboration/v1",
        "status": "NUMERIC_CORROBORATION_NOT_REQUEST_ID_JOIN",
        "run_id": receipt["run_id"],
        "task_id": receipt["task_id"],
        "numeric_usage_sidecar_sha256": sidecar_sha,
        "model_id": receipt["model_id"],
        "matching_provider_row_count": count,
        "provider_export_other_row_count": len(provider_rows) - count,
        "input_units": input_total,
        "cached_input_units": cached_total,
        "output_units": output_total,
        "correlated_rows_list_price_cny": format(price.quantize(
            Decimal("0.000000001"), rounding=ROUND_HALF_UP,
        ), "f"),
        "exact_task_request_id_join": False,
        "provider_attempt_coverage_verified": False,
        "complete_episode_cost": False,
        "raw_request_ids_recorded": False,
        "credentials_recorded": False,
    }


def corroborate_legacy_agent_provider_workbook(
    receipt_file: str | Path, workbook_file: str | Path,
    rate_card_file: str | Path,
) -> dict[str, Any]:
    """Use an old immutable receipt and one provider export read-only."""
    receipt = json.loads(Path(receipt_file).read_bytes())
    card = json.loads(Path(rate_card_file).read_bytes())
    rows = _workbook_request_id_rows(workbook_file)
    selected = [cells for cells in _workbook_selected_cells(
        workbook_file, include_request_id=True,
    ) if cells.get("C") == "qwen3.8-27b" and cells.get("G") == "200"]
    _require(len(rows) == len(selected), "provider row count differs")
    for row, cells in zip(rows, selected, strict=True):
        usage = json.loads(cells["D"])
        details = usage.get("prompt_tokens_details", {})
        row["cached_detail_explicit"] = (
            isinstance(details, dict)
            and type(details.get("cached_tokens")) is int
        )
    result = corroborate_legacy_agent_provider_rows(receipt, rows, card)
    result["provider_log_sha256"] = sha256(
        Path(workbook_file).read_bytes()
    ).hexdigest()
    result["legacy_cost_receipt_sha256"] = sha256(
        Path(receipt_file).read_bytes()
    ).hexdigest()
    result["price_snapshot_sha256"] = sha256(
        Path(rate_card_file).read_bytes()
    ).hexdigest()
    return result
