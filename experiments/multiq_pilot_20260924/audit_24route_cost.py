"""Audit numeric provider-list-price cost of the verified 24-route pilot.

This reads only frozen public receipts and the N6 numeric usage export. It
never estimates token counts from bytes and never treats missing VM time as 0.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from pathfinder.rsi_exam.offline_replay_costing import (
    PRICE_SNAPSHOT,
    _workbook_usage_rows,
    reconcile_caption_cache,
)


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(
        encoding="utf-8",
    ).splitlines()]


def _verify_sums(root: Path) -> None:
    lines = (root / "SHA256SUMS").read_text(encoding="ascii").splitlines()
    for line in lines:
        digest, name = line.split("  ", 1)
        path = root / name
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError(f"checksum differs: {name}")


def _usd(amount: Decimal) -> str:
    return format(amount.quantize(Decimal("0.000000001")), "f")


def _qwen(input_units: int, cached_units: int,
          output_units: int) -> Decimal:
    if not (0 <= cached_units <= input_units and output_units >= 0):
        raise ValueError("invalid Qwen usage")
    rate = PRICE_SNAPSHOT["qwen3.8-27b"]
    return (
        Decimal(input_units - cached_units) * Decimal(rate["input"])
        + Decimal(cached_units) * Decimal(rate["implicit_cached_input"])
        + Decimal(output_units) * Decimal(rate["output"])
    ) / Decimal(1_000_000)


def _embedding(units: int) -> Decimal:
    if units < 0:
        raise ValueError("negative embedding usage")
    return (
        Decimal(units)
        * Decimal(PRICE_SNAPSHOT["text-embedding-v4"]["input"])
        / Decimal(1_000_000)
    )


def audit(args: argparse.Namespace) -> dict:
    for root in (args.routes, args.materialization, args.video_index,
                 args.query_batch):
        _verify_sums(root)
    summary = _read(args.routes / "summary.json")
    if summary["status"] != "VERIFIED_24_ROUTE_EXECUTION":
        raise ValueError("route batch was not verified")
    exported = _read(args.n6_export)
    rows = exported["rows"]
    if exported["record_count"] != 24 or len(rows) != 24:
        raise ValueError("N6 export must cover exactly 24 routes")
    by_design: dict[str, dict] = defaultdict(lambda: {
        "routes": 0, "correct": 0, "input_units": 0,
        "cached_input_units": 0, "output_units": 0,
        "n6_list_price_usd": Decimal(0),
    })
    route_prices = []
    seen_results = set()
    questions = set()
    objects = set()
    for ordinal, usage in enumerate(rows):
        result = _read(args.routes / f"route-{ordinal:02d}.json")
        semantic = result["semantic_route_evidence"]["semantic"]
        observation = result["semantic_route_evidence"][
            "neutral_observation_candidate"
        ]
        if (
            usage["ordinal"] != ordinal
            or usage["route_file"] != f"route-{ordinal:02d}.json"
            or result["trial_key"] != usage["trial_key"]
            or semantic["result_sha256"] != usage["result_sha256"]
            or semantic["request_sha256"] != usage["request_sha256"]
            or observation["object_id"] != usage["object_id"]
            or observation["design_id"] != usage["design_id"]
            or result["task_success"] != usage["task_success"]
            or result["status"] != "COMPLETE"
            or semantic["model"] != "qwen3.8-27b"
            or usage["total_units"] != (
                usage["input_units"] + usage["output_units"]
            )
        ):
            raise ValueError(f"N6 export differs from route {ordinal}")
        if usage["result_sha256"] in seen_results:
            raise ValueError("N6 result was reused")
        seen_results.add(usage["result_sha256"])
        design = usage["design_id"]
        if design not in {"R", "D", "DC", "I"}:
            raise ValueError("unknown route design")
        price = _qwen(usage["input_units"],
                      usage["cached_input_units"], usage["output_units"])
        group = by_design[design]
        group["routes"] += 1
        group["correct"] += int(usage["task_success"])
        for key in ("input_units", "cached_input_units", "output_units"):
            group[key] += usage[key]
        group["n6_list_price_usd"] += price
        route_prices.append({
            "ordinal": ordinal,
            "trial_key": usage["trial_key"],
            "object_id": usage["object_id"],
            "design_id": design,
            "n6_input_units": usage["input_units"],
            "n6_cached_input_units": usage["cached_input_units"],
            "n6_output_units": usage["output_units"],
            "n6_list_price_usd": _usd(price),
            "task_success": usage["task_success"],
        })
        objects.add(usage["object_id"])
        questions.add(usage["trial_key"].split("|")[1])
    if set(by_design) != {"R", "D", "DC", "I"} or any(
        row["routes"] != 6 for row in by_design.values()
    ) or len(objects) != 2 or len(questions) != 6:
        raise ValueError("24-route cohort shape differs")

    material = _read(args.materialization / "cost-evidence.json")
    if material["object_count"] != 12:
        raise ValueError("materialization receipt differs")
    object_rows = {row["object_id"]: row for row in _lines(
        args.materialization / "objects.jsonl",
    )}
    caption_rows = _lines(args.materialization / "caption-requests.jsonl")
    cached_by_object = reconcile_caption_cache(
        caption_rows, _workbook_usage_rows(args.caption_log),
    )
    index = _read(args.video_index / "video-temporal-index.json")
    index_rows = {row["object_id"]: row for row in index[
        "video_build_embedding_receipts"
    ]}
    query = _read(args.query_batch / "temporal-query-batch.json")
    query_rows = query["query_embedding_receipts"]
    if set(objects) - set(object_rows) or set(objects) - set(index_rows):
        raise ValueError("build evidence is missing a pilot object")
    if {row["question_id"] for row in query_rows} != questions:
        raise ValueError("query embedding receipts differ from pilot questions")
    builds = []
    caption_total = Decimal(0)
    index_total = Decimal(0)
    for object_id in sorted(objects):
        row = object_rows[object_id]
        cached = cached_by_object[object_id]
        caption = _qwen(row["caption_input_units"], cached,
                        row["caption_output_units"])
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
    query_units = sum(row["usage"]["prompt_tokens"] for row in query_rows)
    query_total = _embedding(query_units)
    n6_total = sum((row["n6_list_price_usd"] for row in by_design.values()),
                   Decimal(0))
    for row in by_design.values():
        row["n6_list_price_usd"] = _usd(row["n6_list_price_usd"])
    return {
        "schema_version": "pathfinder.multiq24-cost-audit/v1",
        "status": "VERIFIED_PARTIAL_FULL_PATH_COST",
        "route_count": 24,
        "question_count": 6,
        "object_count": 2,
        "price_snapshot": PRICE_SNAPSHOT,
        "cost_basis": "provider-list-price-not-actual-paid-invoice",
        "n6_join": "24/24 result-and-request-sha256 plus trial and ordinal",
        "caption_cache_join": "111/111 unique input-output-token pairs",
        "n6_by_design": dict(sorted(by_design.items())),
        "per_route": route_prices,
        "one_time_builds": builds,
        "query_embedding_input_units": query_units,
        "n6_inference_list_price_usd": _usd(n6_total),
        "caption_build_list_price_usd": _usd(caption_total),
        "video_index_build_list_price_usd": _usd(index_total),
        "query_embedding_list_price_usd": _usd(query_total),
        "known_provider_list_price_usd": _usd(
            n6_total + caption_total + index_total + query_total,
        ),
        "experiment_vm_cost_usd": None,
        "historical_build_compute_cost_usd": None,
        "query_frame_projection_compute_cost_usd": None,
        "n4_publication_compute_cost_usd": None,
        "complete_full_path_cost_usd": None,
        "unmeasured": [
            "historical frame decode and N4 publication compute intervals",
            "historical query-frame projection compute interval",
            "24-route FlowMesh batch time and per-task VM allocation",
        ],
        "source_sha256": {
            "routes_sums": hashlib.sha256((args.routes / "SHA256SUMS").read_bytes()).hexdigest(),
            "n6_export": hashlib.sha256(args.n6_export.read_bytes()).hexdigest(),
            "materialization_sums": hashlib.sha256((args.materialization / "SHA256SUMS").read_bytes()).hexdigest(),
            "caption_provider_log": hashlib.sha256(args.caption_log.read_bytes()).hexdigest(),
            "video_index_sums": hashlib.sha256((args.video_index / "SHA256SUMS").read_bytes()).hexdigest(),
            "query_batch_sums": hashlib.sha256((args.query_batch / "SHA256SUMS").read_bytes()).hexdigest(),
        },
        "credentials_recorded": False,
        "hidden_labels_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("routes", "n6-export", "materialization", "caption-log",
                 "video-index", "query-batch", "output-dir"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / "cost-audit.json"
    payload = json.dumps(result, indent=2, sort_keys=True,
                         ensure_ascii=False, allow_nan=False).encode() + b"\n"
    with output.open("xb") as handle:
        handle.write(payload)
    checksums = args.output_dir / "SHA256SUMS"
    with checksums.open("x", encoding="ascii", newline="\n") as handle:
        for name in ("cost-audit.json", "n6-usage-joined.json"):
            digest = hashlib.sha256((args.output_dir / name).read_bytes()).hexdigest()
            handle.write(f"{digest}  {name}\n")
    print(json.dumps({
        "status": result["status"],
        "known_provider_list_price_usd": result["known_provider_list_price_usd"],
        "n6_inference_list_price_usd": result["n6_inference_list_price_usd"],
        "complete_full_path_cost_usd": None,
    }))


if __name__ == "__main__":
    main()
