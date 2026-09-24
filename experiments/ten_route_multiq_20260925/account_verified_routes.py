"""Join sealed route evidence to numeric N6 usage and frozen list prices.

This is accounting, not a billing claim. It never reads model prompts,
answers, provider IDs, credentials, or N1 labels. Historical N6 journal rows
are excluded by exact semantic result SHA-256 matching.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from statistics import median


MILLION = Decimal(1_000_000)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_sums(directory: Path) -> None:
    manifest = directory / "SHA256SUMS"
    if not manifest.is_file():
        raise ValueError(f"SHA256SUMS missing: {directory}")
    for line in manifest.read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        if not separator or Path(name).is_absolute() or ".." in Path(name).parts:
            raise ValueError("SHA256SUMS entry is invalid")
        target = directory / name
        if not target.is_file() or _sha(target) != digest:
            raise ValueError(f"SHA256SUMS mismatch: {name}")


def llm_list_cost(*, input_units: int, cached_units: int,
                  output_units: int, prices: dict) -> Decimal:
    if not (0 <= cached_units <= input_units and output_units >= 0):
        raise ValueError("N6 token usage is invalid")
    return ((Decimal(input_units - cached_units) * Decimal(prices["input"]))
            + (Decimal(cached_units) * Decimal(prices["input_implicit_cache"]))
            + (Decimal(output_units) * Decimal(prices["output"]))) / MILLION


def _money(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.000000001")), "f")


def _usage_index(paths: list[Path]) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    by_result = {}
    attempts_by_result = defaultdict(list)
    for path in paths:
        export = _load(path)
        if export.get("schema_version") != "pathfinder.n6-numeric-usage-export/v2":
            raise ValueError("unexpected N6 export schema")
        if (export.get("credentials_recorded") is not False
                or export.get("prompts_or_answers_included") is not False
                or export.get("provider_ids_included") is not False):
            raise ValueError("N6 export is not credential-safe")
        for row in export["rows"]:
            key = row["result_sha256"]
            if key in by_result:
                raise ValueError("N6 result digest is duplicated")
            by_result[key] = row
        for attempt in export["attempts"]:
            if attempt.get("result_sha256"):
                attempts_by_result[attempt["result_sha256"]].append(attempt)
    return by_result, attempts_by_result


def _preparation_costs(*, captions: dict, video_index: dict,
                       query: dict, prices: dict) -> tuple[dict, dict]:
    llm = prices["qwen3.8-27b"]
    embedding = Decimal(prices["text-embedding-v4"]["input"]) / MILLION
    video = defaultdict(lambda: {"caption": Decimal(0),
                                 "index_embedding": Decimal(0)})
    for row in captions["per_request_usage"]:
        object_id, marker, _ = row["window_id"].partition("#")
        if marker != "#":
            raise ValueError("caption window ID is invalid")
        usage = row["provider_usage"]
        video[object_id]["caption"] += llm_list_cost(
            input_units=usage["prompt_tokens"],
            cached_units=usage["prompt_tokens_details"]["cached_tokens"],
            output_units=usage["completion_tokens"], prices=llm,
        )
    for row in video_index["video_build_embedding_receipts"]:
        video[row["object_id"]]["index_embedding"] += (
            Decimal(row["usage"]["prompt_tokens"]) * embedding
        )
    question = {}
    for row in query["query_embedding_receipts"]:
        key = row["question_id"]
        if key in question:
            raise ValueError("query embedding receipt duplicated")
        question[key] = Decimal(row["usage"]["prompt_tokens"]) * embedding
    if (len(captions["per_request_usage"]) != 27
            or len(video_index["video_build_embedding_receipts"]) != 3
            or len(question) != 6 or len(video) != 3):
        raise ValueError("preparation usage is incomplete")
    return video, question


def account(*, route_dir: Path, usage_paths: list[Path],
            captions_path: Path, video_index_path: Path, query_path: Path,
            price_path: Path) -> dict:
    verify_sums(route_dir)
    summary = _load(route_dir / "summary.json")
    if (summary.get("status") != "VERIFIED_INTERLEAVED_BATCH_EXECUTION"
            or summary.get("route_count") != 60):
        raise ValueError("route output is not the sealed 60-route result")
    prices = _load(price_path)
    if (prices.get("schema_version")
            != "pathfinder.provider-list-price-snapshot/v1"):
        raise ValueError("price snapshot schema is invalid")
    by_result, attempts_by_result = _usage_index(usage_paths)
    started = datetime.fromisoformat(summary["started_utc"])
    ended = datetime.fromisoformat(summary["ended_utc"])
    elapsed_seconds = Decimal(str((ended - started).total_seconds()))
    if elapsed_seconds <= 0:
        raise ValueError("experiment time interval is invalid")
    video_cost, question_cost = _preparation_costs(
        captions=_load(captions_path), video_index=_load(video_index_path),
        query=_load(query_path), prices=prices,
    )
    rows = []
    matched = set()
    for ordinal in range(60):
        route = _load(route_dir / f"route-{ordinal:02d}.json")
        timing = _load(route_dir / f"timing-{ordinal:02d}.json")
        trial_key = route["trial_key"]
        if timing["trial_key"] != trial_key or route["status"] != "COMPLETE":
            raise ValueError("route and timing are not complete and bound")
        parts = trial_key.split("|")
        if len(parts) != 4:
            raise ValueError("trial key shape is invalid")
        _, question_id, design, repetition = parts
        object_id = question_id.rsplit("-q", 1)[0]
        semantic = route["semantic_route_evidence"]["semantic"]
        result_sha = semantic["result_sha256"]
        if result_sha in matched or result_sha not in by_result:
            raise ValueError("N6 usage is absent or reused")
        attempts = attempts_by_result[result_sha]
        if (len(attempts) != 1 or attempts[0]["outcome"] != "completed"
                or attempts[0]["attempt_index"] != 0
                or attempts[0]["http_status"] != 200):
            raise ValueError("N6 attempt accounting is incomplete or retried")
        matched.add(result_sha)
        usage = by_result[result_sha]
        cost = llm_list_cost(
            input_units=usage["input_units"],
            cached_units=usage["cached_input_units"],
            output_units=usage["output_units"],
            prices=prices["qwen3.8-27b"],
        )
        rows.append({
            "ordinal": ordinal, "trial_key": trial_key,
            "question_id": question_id, "object_id": object_id,
            "design_id": design, "repetition": repetition,
            "executor_node_id": route["semantic_route_evidence"]["route"]
                ["executor_node_id"],
            "result_sha256": result_sha, "task_success": route["task_success"],
            "elapsed_ms": timing["elapsed_ms"],
            "n6_input_units": usage["input_units"],
            "n6_cached_input_units": usage["cached_input_units"],
            "n6_output_units": usage["output_units"],
            "n6_list_cost_usd": _money(cost),
            "query_embedding_list_cost_usd": _money(question_cost[question_id]),
            "cache_branches": [branch["branch"] for branch in
                               route["semantic_route_evidence"]["cache_branches"]],
        })
    if len(matched) != 60:
        raise ValueError("not all route N6 usages matched")
    by_design = defaultdict(list)
    for row in rows:
        by_design[row["design_id"]].append(row)
    aggregates = {}
    for design, group in sorted(by_design.items()):
        aggregates[design] = {
            "count": len(group),
            "correct": sum(row["task_success"] is True for row in group),
            "median_elapsed_ms": median(row["elapsed_ms"] for row in group),
            "n6_list_cost_usd": _money(sum(
                (Decimal(row["n6_list_cost_usd"]) for row in group),
                Decimal(0),
            )),
        }
    video_output = {key: {component: _money(value)
                          for component, value in components.items()}
                    for key, components in sorted(video_cost.items())}
    return {
        "schema_version": "pathfinder.t60-list-cost-accounting/v1",
        "status": "VERIFIED_NUMERIC_USAGE_MATCHED",
        "route_output_summary_sha256": _sha(route_dir / "summary.json"),
        "rate_card_sha256": _sha(price_path),
        "usage_export_sha256": [_sha(path) for path in usage_paths],
        "route_count": 60, "n6_usage_match_count": len(matched),
        "n6_completed_attempt_match_count": len(matched),
        "n6_retried_or_failed_attempt_count": 0,
        "experiment_elapsed_seconds": format(elapsed_seconds, "f"),
        "shared_vm_hour_fraction": format(elapsed_seconds / Decimal(3600),
                                           ".12f"),
        "shared_vm_time_allocation_rule": (
            "multiply the observed experiment hour fraction by the sum "
            "of concurrently active Root and N1-N8 hourly rates; no "
            "historical build time is inferred"
        ),
        "correct_count": sum(row["task_success"] is True for row in rows),
        "n6_total_list_cost_usd": _money(sum(
            (Decimal(row["n6_list_cost_usd"]) for row in rows), Decimal(0),
        )),
        "caption_build_list_cost_usd": _money(sum(
            (parts["caption"] for parts in video_cost.values()), Decimal(0),
        )),
        "video_index_build_list_cost_usd": _money(sum(
            (parts["index_embedding"] for parts in video_cost.values()),
            Decimal(0),
        )),
        "query_embedding_list_cost_usd": _money(sum(
            question_cost.values(), Decimal(0),
        )),
        "video_preparation_list_cost_usd": video_output,
        "question_embedding_list_cost_usd": {
            key: _money(value) for key, value in sorted(question_cost.items())
        },
        "by_design": aggregates, "rows": rows,
        "vm_billed_cost_included": False,
        "historical_build_vm_cost_included": False,
        "provider_cash_bill_claimed": False,
        "cross_node_latency_comparison_valid": False,
        "cross_node_latency_confounded_by_n6_replica": True,
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("route-dir", "captions", "video-index", "query", "prices",
                 "output-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--usage", required=True, type=Path,
                        action="append", dest="usages")
    args = parser.parse_args()
    result = account(
        route_dir=args.route_dir, usage_paths=args.usages,
        captions_path=args.captions, video_index_path=args.video_index,
        query_path=args.query, price_path=args.prices,
    )
    if args.output_dir.exists():
        raise ValueError("output directory already exists")
    args.output_dir.mkdir(parents=True)
    path = args.output_dir / "t60-list-cost-accounting.json"
    path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n",
                    encoding="utf-8", newline="\n")
    digest = _sha(path)
    (args.output_dir / "SHA256SUMS").write_text(
        f"{digest}  {path.name}\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps({key: result[key] for key in (
        "status", "route_count", "n6_usage_match_count", "correct_count",
        "n6_total_list_cost_usd", "caption_build_list_cost_usd",
        "video_index_build_list_cost_usd", "query_embedding_list_cost_usd",
    )}, sort_keys=True))


if __name__ == "__main__":
    main()
