"""Inspect only numeric Agent usage from an existing FlowMesh task result.

This read-only diagnostic never submits a workflow and never emits task output,
tool arguments, prompts, answers, provider credentials, or environment data.
Aggregate SDK usage is not proof that every provider attempt was captured.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Mapping


_NUMERIC = (
    "num_requests", "input_tokens", "output_tokens", "total_tokens",
    "prompt_tokens", "completion_tokens", "cached_input_tokens",
)


def summarize_agent_usage(result: Mapping[str, Any]) -> dict[str, Any]:
    usage = result.get("usage")
    if not isinstance(usage, Mapping):
        return {
            "usage_present": False,
            "numeric_usage": {},
            "cached_input_units_known": False,
            "provider_attempts_reconciled": False,
            "complete_list_price_claimed": False,
        }
    numeric = {
        key: usage[key] for key in _NUMERIC
        if type(usage.get(key)) is int and usage[key] >= 0
    }
    details = usage.get("prompt_tokens_details")
    cached = (
        details.get("cached_tokens")
        if isinstance(details, Mapping) else None
    )
    if type(cached) is int and cached >= 0:
        numeric["cached_input_tokens"] = cached
    else:
        cached = numeric.get("cached_input_tokens")
    input_units = numeric.get("input_tokens", numeric.get("prompt_tokens"))
    if input_units is not None and cached is not None and cached > input_units:
        raise ValueError("Agent cached tokens exceed input tokens")
    return {
        "usage_present": True,
        "numeric_usage": numeric,
        "cached_input_units_known": cached is not None,
        "provider_attempts_reconciled": False,
        "complete_list_price_claimed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flowmesh-base-url", required=True)
    parser.add_argument("--task-id", required=True)
    args = parser.parse_args()
    from pathfinder.integrations.flowmesh.client import SdkFlowMeshClient
    from pathfinder.integrations.flowmesh.contracts import FlowMeshSettings

    client = SdkFlowMeshClient(FlowMeshSettings(base_url=args.flowmesh_base_url))
    try:
        result = client.retrieve_result(args.task_id)
    except Exception as exc:
        print(json.dumps({"status": "READ_FAILED", "error_class":
                          type(exc).__name__}, sort_keys=True))
        return 2
    finally:
        client.close()
    report = summarize_agent_usage(result)
    print(json.dumps({"status": "READ_ONLY_USAGE", **report},
                     sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
