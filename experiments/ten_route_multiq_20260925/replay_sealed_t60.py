"""Adapt sealed t60 traces to the reusable finite-state episode replayer.

The adapter verifies the real isolated-cache event stream first. Replay may
then match a measured miss or hit branch under the *shared* policy namespace;
an unmeasured intermediate cache state remains UNSUPPORTED_ACTION. This is a
counterfactual offline policy episode, not a claim that the live 60-route run
used one shared namespace or that its N7/N8 latency is comparable.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from experiments.ten_route_multiq_replay import (
    TenRouteEpisodeReplay, fixed_policy, no_eviction_admission_policy,
    run_policy,
)
from experiments.ten_route_multiq_20260925.account_verified_routes import (
    verify_sums,
)
from pathfinder.rsi_exam.cache_episode_state import ReplayCacheState


REPS = ("multimodal_digest", "sampled_frame_bundle")
POLICY_NAMESPACE = "rsi-tenroute-policy-20260925-v1"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _cache_artifacts(manifest: dict) -> dict:
    artifacts = defaultdict(dict)
    for row in manifest["objects"]:
        rep = row["representation_id"]
        if rep in REPS:
            artifacts[row["object_id"]][rep] = {
                "sha256": row["artifact_sha256"],
                "size_bytes": row["artifact_size_bytes"],
            }
    if len(artifacts) != 3 or any(set(rows) != set(REPS)
                                  for rows in artifacts.values()):
        raise ValueError("N4 cacheable artifact manifest is incomplete")
    return dict(artifacts)


def _validated_cache_groups(export: dict, schedule: list[dict],
                            artifacts: dict) -> list[list[dict]]:
    node = export["node_id"]
    state = ReplayCacheState(node_id=node,
                             capacity_bytes=export["capacity_bytes"])
    groups = []
    by_namespace = {}
    store_ids = set()
    prior_event_id = 0
    for event in export["events"]:
        event_id = event["event_id"]
        if event_id <= prior_event_id:
            raise ValueError("cache events are not strictly ordered")
        prior_event_id = event_id
        namespace = event["cache_namespace"]
        if namespace not in by_namespace:
            group = []
            by_namespace[namespace] = group
            groups.append(group)
        group = by_namespace[namespace]
        group.append(event)
        object_id, rep = event["object_id"], event["representation_id"]
        artifact = artifacts[object_id][rep]
        if event["content_sha256"] != artifact["sha256"]:
            raise ValueError("cache event content identity differs from N4")
        kind = event["event_kind"]
        if kind in {"MISS", "HIT"}:
            observed = state.lookup(
                namespace=namespace, object_id=object_id,
                representation_id=rep,
                expected_sha256=artifact["sha256"],
            )
            if observed is not (kind == "HIT"):
                raise ValueError("durable cache hit/miss event is inconsistent")
            expected_size = artifact["size_bytes"] if observed else 0
            if event["size_bytes"] != expected_size:
                raise ValueError("durable cache lookup size differs")
        elif kind == "STORE":
            store_ids.add(str(event_id))
            evicted = state.put(
                namespace=namespace, object_id=object_id,
                representation_id=rep, content_sha256=artifact["sha256"],
                size_bytes=artifact["size_bytes"],
            )
            actual = [[item.object_id, item.representation_id]
                      for item in evicted]
            recorded = [
                [item["object_id"], item["representation_id"]]
                for item in export["store_evictions_by_event_id"].get(
                    str(event_id), []
                )
            ]
            if recorded != actual or event["size_bytes"] != artifact["size_bytes"]:
                raise ValueError("durable cache eviction differs from LRU")
        else:
            raise ValueError("cache event kind is unknown")
    if set(export["store_evictions_by_event_id"]) != store_ids:
        raise ValueError("cache store receipt set differs from event log")
    if len(groups) != len(schedule):
        raise ValueError("cache namespaces do not match question count")
    for question, group in zip(schedule, groups, strict=True):
        if (len(group) != 6
                or {event["object_id"] for event in group}
                != {question["object_id"]}
                or [(event["event_kind"], event["representation_id"])
                    for event in group] != [
                        ("MISS", REPS[0]), ("STORE", REPS[0]),
                        ("MISS", REPS[1]), ("STORE", REPS[1]),
                        ("HIT", REPS[0]), ("HIT", REPS[1]),
                    ]):
            raise ValueError("cache episode order differs from sealed schedule")
    return groups


def _branch_events(group: list[dict], export: dict,
                   *, hit: bool) -> list[dict]:
    events = []
    for row in (group[4:] if hit else group[:4]):
        kind = row["event_kind"]
        if kind in {"MISS", "HIT"}:
            events.append({"kind": "lookup",
                           "representation_id": row["representation_id"],
                           "hit": kind == "HIT"})
        else:
            events.append({
                "kind": "store",
                "representation_id": row["representation_id"],
                "evicted": [
                    [item["object_id"], item["representation_id"]]
                    for item in export["store_evictions_by_event_id"].get(
                        str(row["event_id"]), []
                    )
                ],
            })
    return events


def replay(*, schedule_dir: Path, n4_dir: Path, cache_dir: Path,
           accounting_dir: Path, route_dir: Path) -> dict:
    for directory in (schedule_dir, n4_dir, cache_dir,
                      accounting_dir, route_dir):
        verify_sums(directory)
    schedule = [json.loads(line) for line in (
        schedule_dir / "ten-route-multiq-schedule.jsonl"
    ).read_text(encoding="utf-8").splitlines()]
    if len(schedule) != 6 or [row["ordinal"] for row in schedule] != list(range(6)):
        raise ValueError("sealed schedule is not six ordered questions")
    artifacts = _cache_artifacts(_load(n4_dir / "n4-derived-data-package.json"))
    accounting = _load(accounting_dir / "t60-list-cost-accounting.json")
    if (accounting["status"] != "VERIFIED_NUMERIC_USAGE_MATCHED"
            or accounting["route_count"] != 60
            or accounting["route_output_summary_sha256"]
            != _digest(route_dir / "summary.json")):
        raise ValueError("cost accounting does not bind sealed routes")
    by_trial = {row["trial_key"]: row for row in accounting["rows"]}
    if len(by_trial) != 60:
        raise ValueError("cost-accounted trial IDs repeat")
    cache_exports = {node: _load(cache_dir / f"{node.lower()}-cache-events.json")
                     for node in ("N7", "N8")}
    cache_groups = {node: _validated_cache_groups(export, schedule, artifacts)
                    for node, export in cache_exports.items()}
    if ({cache_exports[node]["capacity_bytes"] for node in cache_exports}
            != {2038602}):
        raise ValueError("cache capacity differs from frozen treatment")
    observations = []
    design_to_action = {
        "D0": "N7/R", "D1": "N7/I", "D2": "N7/D", "D3": "N7/DC",
        "D4": "N8/R", "D5": "N8/I", "D6": "N8/D", "D7": "N8/DC",
    }
    question_ordinal = {row["question_id"]: row["ordinal"] for row in schedule}
    for cost_row in accounting["rows"]:
        key = cost_row["trial_key"]
        route = _load(route_dir / f"route-{cost_row['ordinal']:02d}.json")
        evidence = route["semantic_route_evidence"]
        if (route["trial_key"] != key
                or evidence["semantic"]["result_sha256"]
                != cost_row["result_sha256"]):
            raise ValueError("accounted row and route evidence differ")
        design = cost_row["design_id"]
        action = design_to_action[design]
        node, arm = action.split("/")
        if cost_row["executor_node_id"] != node:
            raise ValueError("route node differs from design")
        branches = evidence["cache_branches"]
        if arm == "DC":
            hit = cost_row["repetition"] == "r0001"
            if ([branch["branch"] for branch in branches]
                    != (["hit", "hit"] if hit else ["miss", "miss"])):
                raise ValueError("route cache branch differs from episode")
            group = cache_groups[node][question_ordinal[cost_row["question_id"]]]
            events = _branch_events(group, cache_exports[node], hit=hit)
        else:
            if branches:
                raise ValueError("non-cache route contains cache branches")
            events = []
        route_cost = Decimal(cost_row["n6_list_cost_usd"])
        if arm == "I":
            route_cost += Decimal(cost_row["query_embedding_list_cost_usd"])
        observations.append({
            "question_id": cost_row["question_id"],
            "object_id": cost_row["object_id"],
            "action_id": action, "outcome_id": key,
            "task_success": cost_row["task_success"],
            "route_cost_usd": format(route_cost, "f"),
            "elapsed_ms": cost_row["elapsed_ms"],
            "cache_events": events,
        })
    if len(observations) != 60:
        raise ValueError("replay observation count differs")
    build_cost = {}
    for object_id, parts in accounting["video_preparation_list_cost_usd"].items():
        build_cost[object_id] = {
            "raw": "0",  # no provider API call; VM/storage reported separately
            "caption": parts["caption"],
            "index": parts["index_embedding"],
            "derived": "0",  # deterministic post-caption materialization
        }
    baseline_results = {}
    for node in ("N7", "N8"):
        for arm in ("R", "I", "D", "DC"):
            name = f"always-{node.lower()}-{arm.lower()}"
            choose = fixed_policy(arm, node=node)
            baseline_results[name] = run_policy(TenRouteEpisodeReplay(
                schedule=schedule, observations=observations,
                artifact_by_object=artifacts,
                build_cost_by_object=build_cost,
                capacity_by_node={n: cache_exports[n]["capacity_bytes"]
                                  for n in cache_exports},
                namespace=POLICY_NAMESPACE,
            ), choose)
        name = f"no-eviction-{node.lower()}"
        baseline_results[name] = run_policy(TenRouteEpisodeReplay(
            schedule=schedule, observations=observations,
            artifact_by_object=artifacts,
            build_cost_by_object=build_cost,
            capacity_by_node={n: cache_exports[n]["capacity_bytes"]
                              for n in cache_exports},
            namespace=POLICY_NAMESPACE,
        ), no_eviction_admission_policy(node=node))
    return {
        "schema_version": "pathfinder.t60-offline-baselines/v1",
        "status": "MEASURED_BRANCH_REPLAY" if all(
            row["status"] == "COMPLETE" for row in baseline_results.values()
        ) else "PARTIAL_UNSUPPORTED_ACTION",
        "question_count": 6, "route_observation_count": 60,
        "cache_durable_events_verified": True,
        "cache_miss_count": sum(
            row["event_kind"] == "MISS" for export in cache_exports.values()
            for row in export["events"]
        ),
        "cache_hit_count": sum(
            row["event_kind"] == "HIT" for export in cache_exports.values()
            for row in export["events"]
        ),
        "cache_eviction_count": sum(
            len(items) for export in cache_exports.values()
            for items in export["store_evictions_by_event_id"].values()
        ),
        "measured_pair_namespaces_isolated_by_question": True,
        "policy_namespace_shared_across_questions": True,
        "live_shared_namespace_episode_claimed": False,
        "cost_scope": "provider API list price only; VM and storage excluded",
        "n7_n8_latency_causal_comparison_allowed": False,
        "all_quality_outcomes_correct": True,
        "baselines": baseline_results,
        "external_calls_made": False,
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("schedule-dir", "n4-dir", "cache-dir", "accounting-dir",
                 "route-dir", "output-dir", "clean-source-root",
                 "config-dir", "artifact-root"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(args.clean_source_root.resolve()),
        "PYTHONUTF8": "1",
    }
    checked = subprocess.run(
        [sys.executable, "-P", "-m", "experiments.interleaved_batch",
         "verify", "--config-dir", str(args.config_dir.resolve()),
         "--artifact-root", str(args.artifact_root.resolve()),
         "--output-dir", str(args.route_dir.resolve())],
        capture_output=True, text=True, check=True, timeout=120,
        env=environment,
    )
    canonical = json.loads(checked.stdout)
    if (canonical.get("status") != "VERIFIED_INTERLEAVED_BATCH_OUTPUT"
            or canonical.get("route_count") != 60
            or canonical.get("question_count") != 6
            or canonical.get("credentials_recorded") is not False):
        raise ValueError("canonical source-bound verification did not pass")
    result = replay(
        schedule_dir=args.schedule_dir, n4_dir=args.n4_dir,
        cache_dir=args.cache_dir, accounting_dir=args.accounting_dir,
        route_dir=args.route_dir,
    )
    result["source_bound_route_output_verified"] = True
    result["canonical_admission_sha256"] = canonical["admission_sha256"]
    if args.output_dir.exists():
        raise ValueError("output directory already exists")
    args.output_dir.mkdir(parents=True)
    path = args.output_dir / "t60-offline-baselines.json"
    path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n",
                    encoding="utf-8", newline="\n")
    (args.output_dir / "SHA256SUMS").write_text(
        f"{_digest(path)}  {path.name}\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps({
        "status": result["status"],
        "baselines": {name: {"status": value["status"],
                             "correct": value["correct"],
                             "known_list_cost_usd": value["known_list_cost_usd"]}
                      for name, value in result["baselines"].items()},
    }, sort_keys=True))


if __name__ == "__main__":
    main()
