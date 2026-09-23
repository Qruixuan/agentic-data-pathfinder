"""Source-bound, checkpointed FlowMesh driver for the frozen 24-route pilot.

The default action is offline verification.  --preflight contacts only the
FlowMesh worker registry.  --execute additionally submits the 24 frozen
requests, once each, in their frozen order.  There is no automatic retry.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

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


ADMISSION = "interleaved-multiq-runtime-admission-d328726-isolated-v1"
ALIAS = "pathfinder_costaware_20260815a"
ORIGIN = "http://10.70.0.17:18780"
SOURCES = {
    "trial_dag_dir": "interleaved-multiq-trial-dags-754760e-v1",
    "binding_dir": "interleaved-multiq-route-bindings-74114d7-v1",
    "plan_dir": "interleaved-multiq-24route-2561a1f-v1",
    "n1_public_commitment_dir": "interleaved-multiq-n1-public-1c9ad84-v1",
    "n2_index_package_dir": "rsi-exam-formal-runtime-foundation-321f33a-v2-public/n2-package",
    "n3_package_dir": "interleaved-multiq-n3-1c9ad84-v1",
    "raw_package_dir": "rsi-exam-formal-n3-raw-12video-ab60687-v2",
    "n4_package_dir": "rsi-exam-formal-runtime-foundation-321f33a-v2-public/n4-package",
    "query_dir": "interleaved-multiq-index-2561a1f-v1/query-batch-v1",
    "video_index_dir": "interleaved-multiq-index-2561a1f-v1/video-index-v1",
    "preparation_dir": "rsi-exam-formal-temporal-preparation-ab60687-v2",
    "caption_dir": "rsi-exam-formal-temporal-captions-7a5a8dd-v2",
}


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")


def _rows(path: Path) -> list[dict]:
    data = path.read_bytes()
    if not data.endswith(b"\n"):
        raise ValueError(f"torn frozen JSONL file: {path.name}")
    return [json.loads(row) for row in data.splitlines()]


def _atomic_json(path: Path, value: dict) -> None:
    payload = json.dumps(
        value, indent=2, sort_keys=True, ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    temporary = path.with_suffix(path.suffix + ".partial")
    with temporary.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _inputs(root: Path) -> tuple[dict, list[dict], list[dict], dict, dict]:
    admission = root / ADMISSION
    sources = {name: root / relative for name, relative in SOURCES.items()}
    report = verify_interleaved_runtime_admission(
        admission, **sources, coordinator_base_url=ORIGIN,
    )
    if (
        report["status"] != "VERIFIED_INTERLEAVED_RUNTIME_ADMISSION_NOT_DEPLOYED"
        or report["trial_count"] != 24
        or report["stage_count"] != 276
        or report["index_query_plan_count"] != 6
        or report["data_agent_plan_binding_count"] != 42
        or report["cache_episode_binding_count"] != 6
    ):
        raise ValueError("frozen admission pre-submit counts differ")
    trials = _rows(admission / "admitted-trials.jsonl")
    stages = _rows(admission / "admitted-stages.jsonl")
    routes = _rows(sources["binding_dir"] / "route-inputs.jsonl")
    episodes = _rows(admission / "cache-episode-bindings.jsonl")
    trials.sort(key=lambda row: row["order_index"])
    route_by_trial = {row["trial_key"]: row for row in routes}
    episode_by_trial = {row["trial_key"]: row for row in episodes}
    if (
        len(trials) != 24
        or len(route_by_trial) != 24
        or len(stages) != 276
        or len(episode_by_trial) != 6
        or sorted(row["order_index"] for row in trials) != list(range(24))
    ):
        raise ValueError("frozen trial sequence is incomplete")
    for trial in trials:
        key = trial["trial_key"]
        route = route_by_trial[key]
        if trial["worker_alias"] != ALIAS or route["trial_key"] != key:
            raise ValueError("trial, route, or worker binding differs")
        if trial["route_coordinator_binding"]["base_url"] != ORIGIN:
            raise ValueError("coordinator origin differs")
        episode = episode_by_trial.get(key)
        if trial["design_id"] == "DC":
            if episode is None or episode["run_id"] != route["run_id"]:
                raise ValueError("DC cache episode binding differs")
        elif episode is not None:
            raise ValueError("non-DC trial has cache episode")
    return report, trials, stages, route_by_trial, episode_by_trial


def _settings() -> FlowMeshSettings:
    if not os.getenv("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"):
        raise ValueError("route ingress HMAC credential is missing")
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
    parser.add_argument("--output-dir", type=Path)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--preflight", action="store_true")
    action.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.execute and args.output_dir is None:
        parser.error("--execute requires --output-dir")
    report, trials, stages, routes, episodes = _inputs(args.artifact_root)
    print("SOURCE_BOUND_INPUTS_VERIFIED", report["admission_sha256"], len(trials))
    if not (args.preflight or args.execute):
        return 0
    print("RUNTIME_ENV_KEYS_PRESENT", {
        name: bool(os.getenv(name)) for name in (
            "FLOWMESH_API_KEY", "FLOWMESH_BASE_URL",
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
            "PATHFINDER_FLOWMESH_WORKER_ID",
            "PATHFINDER_FLOWMESH_WORKER_ALIAS",
        )
    }, flush=True)
    settings = _settings()
    print("FLOWMESH_SETTINGS_READY", flush=True)
    client = SdkFlowMeshClient(settings)
    print("FLOWMESH_CLIENT_READY", flush=True)
    try:
        worker = describe_pinned_worker(client, settings)
        print("FLOWMESH_PIN_RESOLVED", worker.worker_id, worker.node_alias,
              worker.status, flush=True)
        if worker.alias != ALIAS or worker.status not in {"IDLE", "BUSY"}:
            raise ValueError("exact current worker alias is not ready")
        if worker.node_alias != "pathfinder-n7":
            raise ValueError("worker alias belongs to a different node")
        print("FLOWMESH_WORKER_READY", worker.worker_id, worker.node_alias)
        if not args.execute:
            return 0
        output = args.output_dir.resolve()
        output.mkdir(parents=True, exist_ok=False)
        (output / "admission-sha256.txt").write_text(
            report["admission_sha256"] + "\n", encoding="ascii", newline="\n",
        )
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
            print("ROUTE_START", ordinal, trial["design_id"], run_id, flush=True)
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
                    "credentials_recorded": False,
                })
                print("ROUTE_FAILED", ordinal, type(exc).__name__, flush=True)
                return 2
            print("ROUTE_COMPLETE", ordinal, trial["design_id"], flush=True)
        summary = {
            "status": "VERIFIED_24_ROUTE_EXECUTION",
            "admission_sha256": report["admission_sha256"],
            "route_count": len(trials),
            "worker_alias": ALIAS,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        _atomic_json(output / "summary.json", summary)
        print("ALL_24_ROUTES_COMPLETE", flush=True)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("PILOT_PREFLIGHT_FAILED", type(exc).__name__, file=sys.stderr)
        raise SystemExit(2) from None
