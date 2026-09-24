"""Join the sealed route evidence to N6 usage and frozen build receipts."""

from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from pathfinder.rsi_exam.offline_replay_costing import (
    PRICE_SNAPSHOT, _workbook_usage_rows, reconcile_caption_cache,
)

from experiments.multiq_pilot_20260924.audit_24route_cost import (
    _embedding, _lines, _qwen, _read, _usd, _verify_sums,
)
from experiments.multiq_pilot_20260924.verify_28route_sealed import verify


def audit(args: argparse.Namespace) -> dict:
    result = verify(args.artifact_root, args.baseline_spec_dir, args.routes)
    for root in (args.materialization, args.video_index, args.query_batch):
        _verify_sums(root)
    journal = _read(args.n6_export)
    if journal.get("schema_version") != "pathfinder.n6-numeric-usage-export/v1":
        raise ValueError("N6 numeric export schema differs")
    rows = journal.get("rows")
    if not isinstance(rows, list) or journal.get("record_count") != len(rows):
        raise ValueError("N6 numeric export count differs")
    by_result = {}
    for row in rows:
        digest = row["result_sha256"]
        if digest in by_result or len(digest) != 64:
            raise ValueError("N6 numeric result digest duplicates")
        by_result[digest] = row
    groups: dict[str, dict] = defaultdict(lambda: {
        "routes": 0, "correct": 0, "input_units": 0,
        "cached_input_units": 0, "output_units": 0,
        "n6_list_price_usd": Decimal(0),
    })
    per_route = []
    questions = set()
    objects = set()
    for ordinal in range(28):
        route = _read(args.routes / f"route-{ordinal:02d}.json")
        timing = _read(args.routes / f"timing-{ordinal:02d}.json")
        semantic = route["semantic_route_evidence"]["semantic"]
        observation = route["semantic_route_evidence"][
            "neutral_observation_candidate"
        ]
        usage = by_result.get(semantic["result_sha256"])
        if usage is None:
            raise ValueError(f"route {ordinal} lacks exact N6 usage")
        if (
            usage["request_sha256"] != semantic["request_sha256"]
            or semantic["model"] != "qwen3.8-27b"
            or usage["total_units"]
            != usage["input_units"] + usage["output_units"]
        ):
            raise ValueError(f"route {ordinal} N6 usage binding differs")
        design = observation["design_id"]
        if design not in {"R", "D", "DC", "I"}:
            raise ValueError("unknown design")
        price = _qwen(
            usage["input_units"], usage["cached_input_units"],
            usage["output_units"],
        )
        group = groups[design]
        group["routes"] += 1
        group["correct"] += int(route["task_success"])
        for key in ("input_units", "cached_input_units", "output_units"):
            group[key] += usage[key]
        group["n6_list_price_usd"] += price
        trial_key = route["trial_key"]
        questions.add(trial_key.split("|")[1])
        objects.add(observation["object_id"])
        per_route.append({
            "ordinal": ordinal,
            "trial_key": trial_key,
            "object_id": observation["object_id"],
            "design_id": design,
            "task_success": route["task_success"],
            "elapsed_ms": timing["elapsed_ms"],
            "n6_input_units": usage["input_units"],
            "n6_cached_input_units": usage["cached_input_units"],
            "n6_output_units": usage["output_units"],
            "n6_list_price_usd": _usd(price),
        })
    if (
        set(groups) != {"R", "D", "DC", "I"}
        or any(group["routes"] != 7 for group in groups.values())
        or len(questions) != 7 or len(objects) != 2
    ):
        raise ValueError("sealed cohort shape differs")
    material = _read(args.materialization / "cost-evidence.json")
    if material["object_count"] != 12:
        raise ValueError("historical materialization receipt differs")
    object_rows = {row["object_id"]: row for row in _lines(
        args.materialization / "objects.jsonl",
    )}
    cached_by_object = reconcile_caption_cache(
        _lines(args.materialization / "caption-requests.jsonl"),
        _workbook_usage_rows(args.caption_log),
    )
    index = _read(args.video_index / "video-temporal-index.json")
    index_rows = {row["object_id"]: row for row in index[
        "video_build_embedding_receipts"
    ]}
    if set(objects) - set(object_rows) or set(objects) - set(index_rows):
        raise ValueError("sealed object build receipt is missing")
    builds = []
    caption_total = Decimal(0)
    index_total = Decimal(0)
    for object_id in sorted(objects):
        row = object_rows[object_id]
        cached = cached_by_object[object_id]
        caption = _qwen(
            row["caption_input_units"], cached,
            row["caption_output_units"],
        )
        index_units = index_rows[object_id]["usage"]["prompt_tokens"]
        index_price = _embedding(index_units)
        caption_total += caption
        index_total += index_price
        builds.append({
            "object_id": object_id,
            "caption_request_count": row["caption_request_count"],
            "caption_input_units": row["caption_input_units"],
            "caption_cached_input_units": cached,
            "caption_output_units": row["caption_output_units"],
            "caption_list_price_usd": _usd(caption),
            "video_index_input_units": index_units,
            "video_index_list_price_usd": _usd(index_price),
            "historical_build_compute_cost_usd": None,
        })
    query = _read(args.query_batch / "temporal-query-batch.json")
    query_rows = query["query_embedding_receipts"]
    if {row["question_id"] for row in query_rows} != questions:
        raise ValueError("query embedding receipts differ")
    query_units = sum(row["usage"]["prompt_tokens"] for row in query_rows)
    query_total = _embedding(query_units)
    n6_total = sum((group["n6_list_price_usd"] for group in groups.values()),
                   Decimal(0))
    for group in groups.values():
        group["n6_list_price_usd"] = _usd(group["n6_list_price_usd"])
    return {
        "schema_version": "pathfinder.multiq28-cost-audit/v1",
        "status": "VERIFIED_PARTIAL_FULL_PATH_COST",
        "route_count": result["route_count"],
        "question_count": result["question_count"],
        "object_count": len(objects),
        "n6_join": "28/28 result-and-request-sha256",
        "cost_basis": "provider-list-price-not-actual-paid-invoice",
        "price_snapshot": PRICE_SNAPSHOT,
        "n6_by_design": dict(sorted(groups.items())),
        "per_route": per_route,
        "one_time_builds": builds,
        "query_embedding_input_units": query_units,
        "n6_inference_list_price_usd": _usd(n6_total),
        "caption_build_list_price_usd": _usd(caption_total),
        "video_index_build_list_price_usd": _usd(index_total),
        "query_embedding_list_price_usd": _usd(query_total),
        "known_provider_list_price_usd": _usd(
            n6_total + caption_total + index_total + query_total,
        ),
        "experiment_started_utc": _read(args.routes / "start.json")[
            "started_utc"
        ],
        "experiment_ended_utc": _read(args.routes / "summary.json")[
            "ended_utc"
        ],
        "historical_build_compute_cost_usd": None,
        "query_frame_projection_compute_cost_usd": None,
        "n4_publication_compute_cost_usd": None,
        "experiment_vm_cost_usd": None,
        "complete_full_path_cost_usd": None,
        "unmeasured": [
            "historical frame decode and N4 publication compute intervals",
            "historical query-frame projection compute interval",
            "VM rate allocation requires a verified cloud rate card",
            "failed provider attempts may incur unobserved charges",
        ],
        "source_sha256": {
            "routes_sums": hashlib.sha256(
                (args.routes / "SHA256SUMS").read_bytes(),
            ).hexdigest(),
            "n6_export": hashlib.sha256(args.n6_export.read_bytes()).hexdigest(),
            "materialization_sums": hashlib.sha256(
                (args.materialization / "SHA256SUMS").read_bytes(),
            ).hexdigest(),
            "video_index_sums": hashlib.sha256(
                (args.video_index / "SHA256SUMS").read_bytes(),
            ).hexdigest(),
            "query_batch_sums": hashlib.sha256(
                (args.query_batch / "SHA256SUMS").read_bytes(),
            ).hexdigest(),
        },
        "credentials_recorded": False,
        "hidden_labels_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in (
        "artifact-root", "baseline-spec-dir", "routes", "n6-export",
        "materialization", "caption-log", "video-index", "query-batch",
        "output-dir",
    ):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    payload = json.dumps(result, indent=2, sort_keys=True,
                         ensure_ascii=False, allow_nan=False).encode() + b"\n"
    path = args.output_dir / "cost-audit.json"
    with path.open("xb") as handle:
        handle.write(payload)
    with (args.output_dir / "SHA256SUMS").open(
        "x", encoding="ascii", newline="\n",
    ) as handle:
        handle.write(f"{hashlib.sha256(payload).hexdigest()}  cost-audit.json\n")
    print(json.dumps({
        "status": result["status"],
        "known_provider_list_price_usd": result["known_provider_list_price_usd"],
        "complete_full_path_cost_usd": None,
    }))


if __name__ == "__main__":
    main()
