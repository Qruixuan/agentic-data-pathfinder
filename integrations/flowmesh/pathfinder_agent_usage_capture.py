"""Pathfinder-owned numeric-only capture for an isolated FlowMesh Agent worker.

The installed worker omits SDK token units from its AgentResult. This module
records the SDK aggregate and per-request numeric units in the exact task's
private result directory before that lossy conversion. It never serializes a
prompt, answer, response ID, credential, or model response body.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from pathlib import Path
from typing import Any

from .pathfinder_agent_provider_attempts import take_httpx_attempts


SCHEMA = "pathfinder.agent-sdk-usage/v1"
_TASK_ID = re.compile(r"tsk-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                      r"[0-9a-f]{4}-[0-9a-f]{12}\Z")


def _number(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _units(value: Any) -> dict[str, int | None]:
    details = getattr(value, "input_tokens_details", None)
    return {
        "input_tokens": _number(getattr(value, "input_tokens", None)),
        "output_tokens": _number(getattr(value, "output_tokens", None)),
        "total_tokens": _number(getattr(value, "total_tokens", None)),
        "cached_input_tokens_sdk": _number(
            getattr(details, "cached_tokens", None)
        ),
    }


def _coherent(units: dict[str, int | None]) -> bool:
    incoming = units["input_tokens"]
    outgoing = units["output_tokens"]
    total = units["total_tokens"]
    cached = units["cached_input_tokens_sdk"]
    return (
        incoming is not None and outgoing is not None and total is not None
        and total == incoming + outgoing
        and (cached is None or cached <= incoming)
    )


def summarize_sdk_usage(result: Any, state: str) -> dict[str, Any]:
    if state not in {"complete", "failed", "timeout"}:
        raise ValueError("invalid Agent completion state")
    wrapper = getattr(result, "context_wrapper", None)
    usage = getattr(wrapper, "usage", None)
    responses = getattr(result, "raw_responses", None)
    response_count = len(responses) if isinstance(responses, list) else None
    if usage is None:
        return {
            "schema": SCHEMA, "completion_state": state,
            "usage_present": False, "sdk_request_count": None,
            "raw_response_count": response_count, "aggregate": None,
            "request_units": [], "aggregate_units_coherent": False,
            "request_entries_reconciled": False,
            "provider_attempts_reconciled": False,
            "cached_input_provider_verified": False,
        }
    aggregate = _units(usage)
    request_count = _number(getattr(usage, "requests", None))
    source_entries = getattr(usage, "request_usage_entries", None)
    entries = (
        [_units(entry) for entry in source_entries]
        if isinstance(source_entries, list) else []
    )
    entries_coherent = all(_coherent(entry) for entry in entries)
    aggregate_coherent = _coherent(aggregate)
    entries_reconciled = (
        aggregate_coherent and entries_coherent and request_count is not None
        and request_count > 0 and len(entries) == request_count
        and all(
            sum(entry[key] or 0 for entry in entries) == aggregate[key]
            for key in ("input_tokens", "output_tokens", "total_tokens")
        )
    )
    return {
        "schema": SCHEMA, "completion_state": state,
        "usage_present": True, "sdk_request_count": request_count,
        "raw_response_count": response_count, "aggregate": aggregate,
        "request_units": entries,
        "aggregate_units_coherent": aggregate_coherent,
        "request_entries_reconciled": entries_reconciled,
        # SDK response coverage does not prove upstream transport retries.
        "provider_attempts_reconciled": False,
        # The SDK normalizes missing cache details to zero. Do not treat
        # that number alone as provider proof of an uncached request.
        "cached_input_provider_verified": False,
    }


def capture_agent_usage(result: Any, out_dir: Path, state: str) -> None:
    """Persist one bounded attempt receipt without affecting Agent execution."""
    try:
        task_dir = Path(out_dir)
        if _TASK_ID.fullmatch(task_dir.name) is None:
            return
        report = summarize_sdk_usage(result, state)
        attempts = take_httpx_attempts()
        report["provider_transport_attempt_count"] = len(attempts)
        report["provider_transport_attempts"] = attempts
        report["provider_transport_capture_present"] = bool(attempts)
        # Transport metadata alone cannot prove provider-side billing or
        # that an exported audit log includes every attempt.
        report["provider_attempts_reconciled"] = False
        report["task_id"] = task_dir.name
        report["attempt_nonce"] = uuid.uuid4().hex
        directory = task_dir / "artifacts"
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = directory / (
            "pathfinder-agent-usage-" + report["attempt_nonce"] + ".json"
        )
        payload = (json.dumps(report, sort_keys=True, separators=(",", ":"))
                   + "\n").encode("utf-8")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
    except Exception:
        # Instrumentation must not retry or invalidate a paid Agent result.
        # Missing evidence is reported as unknown by the external auditor.
        return
