"""Replay pre-frozen baselines over seven verified interleaved questions.

This reports known provider list-price cost without inventing cloud/build
charges. It never reads the N1 private oracle or raw model answers.
"""

from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from experiments.multiq_pilot_20260924.audit_24route_cost import (
    _embedding, _read, _usd,
)
from experiments.multiq_pilot_20260924.verify_28route_sealed import verify


POLICIES = (
    "always-R", "always-D", "always-DC", "always-I", "first-R-then-I",
)
ARMS = ("R", "D", "DC", "I")


def _choice(policy: str, prior_video_questions: int) -> str:
    if policy == "first-R-then-I":
        return "R" if prior_video_questions == 0 else "I"
    if policy.startswith("always-") and policy[7:] in ARMS:
        return policy[7:]
    raise ValueError("unknown frozen policy")


def replay(
    artifact_root: Path, baseline_spec_dir: Path,
    routes_dir: Path, cost_audit_dir: Path,
) -> dict:
    verified = verify(artifact_root, baseline_spec_dir, routes_dir)
    cost_bytes = (cost_audit_dir / "cost-audit.json").read_bytes()
    cost = json.loads(cost_bytes)
    if (
        cost.get("status") != "VERIFIED_PARTIAL_FULL_PATH_COST"
        or cost.get("route_count") != 28
        or cost.get("n6_join") != "28/28 result-and-request-sha256"
        or cost.get("complete_full_path_cost_usd") is not None
    ):
        raise ValueError("28-route cost evidence differs")
    checksums = (cost_audit_dir / "SHA256SUMS").read_text("ascii")
    if checksums != (
        f"{hashlib.sha256(cost_bytes).hexdigest()}  cost-audit.json\n"
    ):
        raise ValueError("cost audit checksum differs")
    source_rows = cost["per_route"]
    if len(source_rows) != 28:
        raise ValueError("route cost count differs")
    build_by_object = {
        row["object_id"]: row for row in cost["one_time_builds"]
    }
    if len(build_by_object) != 2:
        raise ValueError("video build count differs")
    query = _read(
        artifact_root / "multiq-sealed-query-20260924-v2"
        / "temporal-query-batch.json"
    )
    query_by_id = {
        row["question_id"]: row for row in query["query_embedding_receipts"]
    }
    if len(query_by_id) != 7:
        raise ValueError("query embedding count differs")
    spec = _read(baseline_spec_dir / "baseline-spec.json")
    if (
        tuple(spec["policies"]) != POLICIES
        or spec["status"] != "FROZEN_BEFORE_SEALED_TEST_OUTCOMES"
        or spec["admission_sha256"] != verified["admission_sha256"]
    ):
        raise ValueError("baseline policy specification differs")
    for model, fields in (
        ("qwen3.8-27b", ("input", "implicit_cached_input", "output")),
        ("text-embedding-v4", ("input",)),
    ):
        for field in fields:
            if Decimal(spec["price_snapshot"][model][field]) != Decimal(
                cost["price_snapshot"][model][field]
            ):
                raise ValueError("price snapshot differs")
    grouped: dict[str, dict[str, dict]] = {}
    schedule: list[str] = []
    for ordinal, row in enumerate(source_rows):
        if row["ordinal"] != ordinal or row["design_id"] != ARMS[ordinal % 4]:
            raise ValueError("frozen route order differs")
        route = _read(routes_dir / f"route-{ordinal:02d}.json")
        if (
            row["trial_key"] != route["trial_key"]
            or row["task_success"] is not route["task_success"]
        ):
            raise ValueError("route outcome differs from cost evidence")
        question = row["trial_key"].split("|")[1]
        if question not in grouped:
            grouped[question] = {}
            schedule.append(question)
        grouped[question][row["design_id"]] = row
    if len(schedule) != 7 or any(set(arms) != set(ARMS)
                                 for arms in grouped.values()):
        raise ValueError("question/action coverage differs")
    results = []
    for policy in POLICIES:
        seen_video: dict[str, int] = {}
        index_built: set[str] = set()
        derived_cached: set[str] = set()
        known = Decimal(0)
        n6_total = Decimal(0)
        index_build_total = Decimal(0)
        query_total = Decimal(0)
        successes = 0
        steps = []
        missing = {"experiment_vm_time_allocation"}
        for question in schedule:
            # All four observed rows bind one public video for this question.
            objects = {row["object_id"] for row in grouped[question].values()}
            if len(objects) != 1:
                raise ValueError("question rows bind different videos")
            object_id = next(iter(objects))
            arm = _choice(policy, seen_video.get(object_id, 0))
            row = grouped[question][arm]
            success = row["task_success"]
            if type(success) is not bool:
                raise ValueError("task outcome is not boolean")
            successes += int(success)
            n6 = Decimal(row["n6_list_price_usd"])
            n6_total += n6
            step_cost = n6
            build = Decimal(0)
            embedding = Decimal(0)
            cache_state = None
            if arm == "I":
                if object_id not in index_built:
                    source = build_by_object[object_id]
                    build = Decimal(source["caption_list_price_usd"])
                    build += Decimal(source["video_index_list_price_usd"])
                    index_built.add(object_id)
                    missing.add("historical_index_build_compute_and_storage")
                embedding = _embedding(
                    query_by_id[question]["usage"]["prompt_tokens"]
                )
                missing.add("question_frame_projection_compute")
            if arm in {"D", "DC"}:
                missing.add("historical_n4_derived_build_and_publication")
            if arm == "DC":
                route = _read(routes_dir / f"route-{row['ordinal']:02d}.json")
                branches = route["semantic_route_evidence"]["cache_branches"]
                expected = "hit" if object_id in derived_cached else "miss"
                if (
                    len(branches) != 2
                    or {item["representation_id"] for item in branches}
                    != {"multimodal_digest", "sampled_frame_bundle"}
                    or any(item["branch"] != expected for item in branches)
                ):
                    raise ValueError("DC counterfactual cache state differs")
                cache_state = expected
                derived_cached.add(object_id)
            step_cost += build + embedding
            index_build_total += build
            query_total += embedding
            known += step_cost
            steps.append({
                "question_id": question,
                "object_id": object_id,
                "arm": arm,
                "task_success": success,
                "dc_cache_state": cache_state,
                "known_provider_list_price_usd": _usd(step_cost),
            })
            seen_video[object_id] = seen_video.get(object_id, 0) + 1
        results.append({
            "policy_name": policy,
            "question_count": len(steps),
            "task_successes": successes,
            "n6_list_price_usd": _usd(n6_total),
            "index_build_list_price_usd": _usd(index_build_total),
            "query_embedding_list_price_usd": _usd(query_total),
            "known_provider_list_price_usd": _usd(known),
            "complete_full_path_cost_usd": None,
            "missing_cost_components": sorted(missing),
            "steps": steps,
        })
    return {
        "schema_version": "pathfinder.multiq-sealed-baseline-replay/v1",
        "status": "VERIFIED_BASELINES_PARTIAL_COST_NO_WINNER",
        "route_output_sha256": hashlib.sha256(
            (routes_dir / "SHA256SUMS").read_bytes()
        ).hexdigest(),
        "cost_audit_sha256": hashlib.sha256(cost_bytes).hexdigest(),
        "baseline_spec_sha256": verified["baseline_spec_sha256"],
        "question_count": 7,
        "policy_count": len(results),
        "policies": results,
        "winner": None,
        "winner_reason": (
            "All observed held-out arms are correct, but required full-path "
            "cost components are not measured."
        ),
        "credentials_recorded": False,
        "hidden_labels_included": False,
        "eligible_for_scientific_claims": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("artifact-root", "baseline-spec-dir", "routes",
                 "cost-audit-dir", "output-dir"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    result = replay(
        args.artifact_root, args.baseline_spec_dir, args.routes,
        args.cost_audit_dir,
    )
    if args.output_dir.exists():
        raise ValueError("baseline replay output directory already exists")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    payload = json.dumps(result, indent=2, sort_keys=True,
                         ensure_ascii=False, allow_nan=False).encode() + b"\n"
    with (args.output_dir / "baseline-replay.json").open("xb") as handle:
        handle.write(payload)
    with (args.output_dir / "SHA256SUMS").open(
        "x", encoding="ascii", newline="\n",
    ) as handle:
        handle.write(
            f"{hashlib.sha256(payload).hexdigest()}  baseline-replay.json\n"
        )
    print(json.dumps({
        "status": result["status"],
        "question_count": result["question_count"],
        "policy_count": result["policy_count"],
        "winner": None,
    }))


if __name__ == "__main__":
    main()
