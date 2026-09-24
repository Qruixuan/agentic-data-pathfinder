"""Checkpointed seven-question FlowMesh driver; no automatic retries.

Without --preflight or --execute, only frozen public inputs are verified.
The operator must satisfy EXPERIMENT_OPERATIONS_RUNBOOK.md before --execute.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from time import monotonic_ns

from pathfinder.integrations.flowmesh import (
    FlowMeshSemanticTrialExecutor,
    FlowMeshSettings,
    SdkFlowMeshClient,
    full_flow_hmac_header_provider,
)
from pathfinder.integrations.flowmesh.preflight import describe_pinned_worker
from pathfinder.simulator.full_flow_matrix_runner import _assert_public_evidence
from pathfinder.simulator.interleaved_multiq_runtime_admission import (
    verify_interleaved_runtime_admission,
)

from experiments.multiq_pilot_20260924.run_24route_pilot import (
    _atomic_json, _canonical, _rows,
)


ADMISSION = "multiq-sealed-runtime-admission-20260924-v2"
ALIAS = "pathfinder_costaware_20260815a"
ORIGIN = "http://10.70.0.17:18780"
SOURCES = {
    "trial_dag_dir": "multiq-sealed-trial-dags-20260924-v2",
    "binding_dir": "multiq-sealed-route-bindings-20260924-v2",
    "plan_dir": "multiq-sealed-test-plan-20260924-v2",
    "n1_public_commitment_dir": (
        "multiq-sealed-test-oracle-commitment-20260924-v2"
    ),
    "n2_index_package_dir": (
        "rsi-exam-formal-runtime-foundation-321f33a-v2-public/n2-package"
    ),
    "n3_package_dir": "multiq-sealed-n3-20260924-v2",
    "raw_package_dir": "rsi-exam-formal-n3-raw-12video-ab60687-v2",
    "n4_package_dir": (
        "rsi-exam-formal-runtime-foundation-321f33a-v2-public/n4-package"
    ),
    "query_dir": "multiq-sealed-query-20260924-v2",
    "video_index_dir": "interleaved-multiq-index-2561a1f-v1/video-index-v1",
    "preparation_dir": "rsi-exam-formal-temporal-preparation-ab60687-v2",
    "caption_dir": "rsi-exam-formal-temporal-captions-7a5a8dd-v2",
}
COUNTS = {
    "trial_count": 28,
    "stage_count": 322,
    "index_query_plan_count": 7,
    "data_agent_plan_binding_count": 49,
    "cache_episode_binding_count": 7,
}


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _inputs(root: Path) -> tuple[dict, list[dict], list[dict], dict, dict]:
    admission = root / ADMISSION
    sources = {name: root / relative for name, relative in SOURCES.items()}
    report = verify_interleaved_runtime_admission(
        admission, **sources, coordinator_base_url=ORIGIN,
    )
    if report["status"] != (
        "VERIFIED_INTERLEAVED_RUNTIME_ADMISSION_NOT_DEPLOYED"
    ) or any(report.get(key) != value for key, value in COUNTS.items()):
        raise ValueError("seven-question admission pre-submit gate differs")
    trials = sorted(
        _rows(admission / "admitted-trials.jsonl"),
        key=lambda row: row["order_index"],
    )
    stages = _rows(admission / "admitted-stages.jsonl")
    routes = _rows(sources["binding_dir"] / "route-inputs.jsonl")
    episodes = _rows(admission / "cache-episode-bindings.jsonl")
    route_by_trial = {row["trial_key"]: row for row in routes}
    episode_by_trial = {row["trial_key"]: row for row in episodes}
    if (
        len(trials) != 28 or len(stages) != 322
        or len(route_by_trial) != 28 or len(episode_by_trial) != 7
        or sorted(row["order_index"] for row in trials) != list(range(28))
    ):
        raise ValueError("seven-question trial order or bindings differ")
    for trial in trials:
        key = trial["trial_key"]
        route = route_by_trial[key]
        if (
            trial["worker_alias"] != ALIAS
            or route["trial_key"] != key
            or trial["route_coordinator_binding"]["base_url"] != ORIGIN
        ):
            raise ValueError("route, coordinator, or worker binding differs")
        episode = episode_by_trial.get(key)
        if trial["design_id"] == "DC":
            if episode is None or episode["run_id"] != route["run_id"]:
                raise ValueError("DC cache episode differs")
        elif episode is not None:
            raise ValueError("non-DC route has a cache episode")
    return report, trials, stages, route_by_trial, episode_by_trial


def _baseline(root: Path, report: dict) -> str:
    checksum = (root / "SHA256SUMS").read_text(encoding="ascii").strip()
    digest, name = checksum.split("  ", 1)
    if name != "baseline-spec.json":
        raise ValueError("baseline checksum file differs")
    payload = (root / name).read_bytes()
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError("baseline checksum differs")
    spec = json.loads(payload)
    if (
        spec["status"] != "FROZEN_BEFORE_SEALED_TEST_OUTCOMES"
        or spec["admission_sha256"] != report["admission_sha256"]
        or spec["plan_sha256"] != (
            "47d04af19e3f3ff83adf0d936d1c35ebf4c0aa225a7437e1aa7a71f3145a6edb"
        )
    ):
        raise ValueError("baseline does not bind seven-question inputs")
    return digest


def _settings() -> FlowMeshSettings:
    if not os.getenv("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"):
        raise ValueError("route ingress credential is missing")
    base_url = os.getenv("FLOWMESH_BASE_URL")
    if not base_url:
        raise ValueError("FlowMesh Root URL is missing")
    return FlowMeshSettings.from_environment(
        base_url=base_url, worker_alias=ALIAS,
        task_timeout_seconds=900, poll_interval_seconds=2.0,
        validate_before_submit=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--baseline-spec-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--preflight", action="store_true")
    action.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.execute and args.output_dir is None:
        parser.error("--execute requires --output-dir")
    report, trials, stages, routes, episodes = _inputs(args.artifact_root)
    baseline_digest = _baseline(args.baseline_spec_dir, report)
    print("SOURCE_BOUND_INPUTS_VERIFIED", report["admission_sha256"],
          baseline_digest, len(trials), flush=True)
    if not (args.preflight or args.execute):
        return 0
    settings = _settings()
    client = SdkFlowMeshClient(settings)
    try:
        worker = describe_pinned_worker(client, settings)
        if (
            worker.alias != ALIAS or worker.status not in {"IDLE", "BUSY"}
            or worker.node_alias != "pathfinder-n7"
        ):
            raise ValueError("exact current N7 worker alias is not ready")
        print("FLOWMESH_WORKER_READY", worker.worker_id,
              worker.node_alias, flush=True)
        if not args.execute:
            return 0
        output = args.output_dir.resolve()
        output.mkdir(parents=True, exist_ok=False)
        started = _utc()
        _atomic_json(output / "start.json", {
            "status": "STARTED",
            "started_utc": started,
            "admission_sha256": report["admission_sha256"],
            "baseline_spec_sha256": baseline_digest,
            "worker_alias": ALIAS,
            "observed_worker_id": worker.worker_id,
            "price_basis": "frozen-official-list-price-per-recorded-token",
            "credentials_recorded": False,
        })
        signer = full_flow_hmac_header_provider(
            os.environ["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"]
        )
        for ordinal, trial in enumerate(trials):
            route = routes[trial["trial_key"]]
            run_id = route["run_id"]
            episode = episodes.get(trial["trial_key"])
            idempotency_key = hashlib.sha256(_canonical({
                "domain": "interleaved-admission-idempotency/v1",
                "run_id": run_id, "trial_key": trial["trial_key"],
            })).hexdigest()
            executor = FlowMeshSemanticTrialExecutor(
                client=client, settings=settings, run_id=run_id,
                bound_trials=trials, bound_stages=stages,
                runtime_header_provider=signer,
                api_task_timeout_seconds=900,
                cache_episode_id=(
                    episode["cache_episode_id"] if episode else None
                ),
            )
            start_ns = monotonic_ns()
            route_started = _utc()
            print("ROUTE_START", ordinal, trial["design_id"], flush=True)
            try:
                result = dict(executor.execute(
                    trial=trial, idempotency_key=idempotency_key,
                ))
                _assert_public_evidence(result)
                if result.get("status") != "COMPLETE":
                    raise ValueError("route result is not COMPLETE")
                _atomic_json(output / f"route-{ordinal:02d}.json", result)
            except Exception as exc:
                _atomic_json(output / "failure.json", {
                    "status": "STOPPED_AT_FIRST_FAILURE",
                    "ordinal": ordinal,
                    "trial_key": trial["trial_key"],
                    "failure_class": getattr(exc, "failure_class", "internal"),
                    "failure_code": getattr(exc, "failure_code", type(exc).__name__),
                    "route_started_utc": route_started,
                    "route_ended_utc": _utc(),
                    "elapsed_ms": (monotonic_ns() - start_ns) // 1_000_000,
                    "credentials_recorded": False,
                })
                print("ROUTE_FAILED", ordinal, type(exc).__name__, flush=True)
                return 2
            _atomic_json(output / f"timing-{ordinal:02d}.json", {
                "ordinal": ordinal,
                "trial_key": trial["trial_key"],
                "route_started_utc": route_started,
                "route_ended_utc": _utc(),
                "elapsed_ms": (monotonic_ns() - start_ns) // 1_000_000,
            })
            print("ROUTE_COMPLETE", ordinal, trial["design_id"], flush=True)
        _atomic_json(output / "summary.json", {
            "status": "VERIFIED_28_ROUTE_EXECUTION",
            "admission_sha256": report["admission_sha256"],
            "baseline_spec_sha256": baseline_digest,
            "route_count": len(trials),
            "started_utc": started,
            "ended_utc": _utc(),
            "worker_alias": ALIAS,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        })
        print("ALL_28_ROUTES_COMPLETE", flush=True)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("SEALED_PREFLIGHT_FAILED", type(exc).__name__, file=sys.stderr)
        raise SystemExit(2) from None
