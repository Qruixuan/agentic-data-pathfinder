"""Reusable source-bound multi-question FlowMesh runner and verifier.

The default ``check`` action is offline. Only ``execute`` submits workflows.
Draft configuration is frozen to an immutable, checksummed directory before
it can be used. No credential or hidden label belongs in that configuration.
"""

from __future__ import annotations

import argparse
from collections import Counter
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
from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    verify_semantic_route_evidence,
)
from pathfinder.simulator.full_flow_matrix_runner import _assert_public_evidence
from pathfinder.rsi_exam.interleaved_multiq_plan import (
    verify_interleaved_plan,
)
from pathfinder.simulator.interleaved_multiq_route_bindings import (
    freeze_interleaved_route_bindings,
    verify_interleaved_route_bindings,
)
from pathfinder.simulator.interleaved_multiq_trial_dag import (
    freeze_interleaved_trial_dags,
    verify_interleaved_trial_dags,
)
from pathfinder.simulator.interleaved_multiq_runtime_admission import (
    freeze_interleaved_runtime_admission,
    verify_interleaved_runtime_admission,
)


SCHEMA = "pathfinder.interleaved-batch-config/v1"
SOURCE_KEYS = frozenset({
    "trial_dag_dir", "binding_dir", "plan_dir", "n1_public_commitment_dir",
    "n2_index_package_dir", "n3_package_dir", "raw_package_dir",
    "n4_package_dir", "query_dir", "video_index_dir", "preparation_dir",
    "caption_dir",
})
CONFIG_KEYS = frozenset({
    "schema_version", "admission_dir", "source_dirs",
    "coordinator_base_url", "worker_alias", "worker_node_alias",
    "task_timeout_seconds", "expected_plan_sha256",
    "expected_question_count", "expected_route_count", "baseline_spec_dir",
})


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _pretty(value: object) -> bytes:
    return json.dumps(
        value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False,
    ).encode("utf-8") + b"\n"


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read(path: Path) -> dict:
    return json.loads(path.read_bytes())


def _rows(path: Path) -> list[dict]:
    data = path.read_bytes()
    if not data.endswith(b"\n"):
        raise ValueError(f"torn frozen JSONL file: {path.name}")
    return [json.loads(row) for row in data.splitlines()]


def _relative(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("artifact directory must be a relative POSIX path")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts) or ":" in parts[0]:
        raise ValueError("artifact directory escapes the artifact root")
    return value


def _validate_config(config: dict) -> dict:
    if isinstance(config, dict) and config.get("schema_version") == (
        "pathfinder.ten-route-batch-config/v1"
    ):
        from experiments.ten_route_batch import validate_config
        return validate_config(config)
    if not isinstance(config, dict) or set(config) != CONFIG_KEYS:
        raise ValueError("batch configuration keys differ")
    if config["schema_version"] != SCHEMA:
        raise ValueError("batch configuration schema differs")
    for key in ("admission_dir", "baseline_spec_dir"):
        if config[key] is not None:
            _relative(config[key])
    if config["admission_dir"] is None:
        raise ValueError("admission directory is required")
    sources = config["source_dirs"]
    if not isinstance(sources, dict) or set(sources) != SOURCE_KEYS:
        raise ValueError("source directory keys differ")
    for value in sources.values():
        _relative(value)
    for key in ("worker_alias", "worker_node_alias"):
        if not isinstance(config[key], str) or not config[key].strip():
            raise ValueError(f"{key} is missing")
    origin = config["coordinator_base_url"]
    if not isinstance(origin, str) or not origin.startswith("http://"):
        raise ValueError("coordinator origin must be an HTTP service origin")
    if type(config["task_timeout_seconds"]) is not int or (
        config["task_timeout_seconds"] < 900
    ):
        raise ValueError("task timeout is below the established 900s floor")
    if not isinstance(config["expected_plan_sha256"], str) or (
        len(config["expected_plan_sha256"]) != 64
        or any(c not in "0123456789abcdef"
               for c in config["expected_plan_sha256"])
    ):
        raise ValueError("expected plan digest is invalid")
    for key in ("expected_question_count", "expected_route_count"):
        if type(config[key]) is not int or config[key] < 1:
            raise ValueError(f"{key} is invalid")
    if config["expected_route_count"] < config["expected_question_count"]:
        raise ValueError("route count is smaller than question count")
    return config


def freeze_config(draft: Path, target: Path) -> str:
    config = _validate_config(_read(draft))
    payload = _pretty(config)
    digest = _hash(payload)
    target.mkdir(parents=True, exist_ok=False)
    (target / "batch-config.json").write_bytes(payload)
    (target / "SHA256SUMS").write_bytes(
        f"{digest}  batch-config.json\n".encode("ascii")
    )
    return digest


def load_config(root: Path) -> tuple[dict, str]:
    if not root.is_dir() or {p.name for p in root.iterdir()} != {
        "batch-config.json", "SHA256SUMS",
    }:
        raise ValueError("frozen batch configuration file set differs")
    payload = (root / "batch-config.json").read_bytes()
    digest = _hash(payload)
    if (root / "SHA256SUMS").read_bytes() != (
        f"{digest}  batch-config.json\n".encode("ascii")
    ):
        raise ValueError("frozen batch configuration checksum differs")
    return _validate_config(json.loads(payload)), digest


def _path(root: Path, relative: str) -> Path:
    target = (root / relative).resolve()
    if not target.is_relative_to(root.resolve()):
        raise ValueError("source directory escapes the artifact root")
    return target


def _baseline(root: Path, relative: str | None,
              admission_sha: str, plan_sha: str) -> str | None:
    if relative is None:
        return None
    directory = _path(root, relative)
    if {p.name for p in directory.iterdir()} != {
        "baseline-spec.json", "SHA256SUMS",
    }:
        raise ValueError("baseline file set differs")
    payload = (directory / "baseline-spec.json").read_bytes()
    digest = _hash(payload)
    if (directory / "SHA256SUMS").read_bytes() != (
        f"{digest}  baseline-spec.json\n".encode("ascii")
    ):
        raise ValueError("baseline checksum differs")
    spec = json.loads(payload)
    if (
        spec.get("status") != "FROZEN_BEFORE_SEALED_TEST_OUTCOMES"
        or spec.get("admission_sha256") != admission_sha
        or spec.get("plan_sha256") != plan_sha
    ):
        raise ValueError("baseline does not bind the plan and admission")
    return digest


def freeze_inputs(config: dict, artifact_root: Path) -> dict:
    """Freeze route bindings, DAGs, and admission from prepared inputs."""

    if config["schema_version"] != SCHEMA:
        from experiments.ten_route_batch import freeze_inputs as freeze_ten
        return freeze_ten(config, artifact_root)
    root = artifact_root.resolve()
    sources = {
        key: _path(root, value)
        for key, value in config["source_dirs"].items()
    }
    targets = {
        key: sources[key] for key in ("binding_dir", "trial_dag_dir")
    }
    targets["admission_dir"] = _path(root, config["admission_dir"])
    if any(path.exists() for path in targets.values()):
        raise ValueError("downstream freeze target already exists")
    plan_dir = sources["plan_dir"]
    plan = _read(plan_dir / "interleaved-plan.json")
    questions = _rows(plan_dir / "public-questions.jsonl")
    verified_plan = verify_interleaved_plan(
        plan_dir, questions,
        public_source_sha256=plan["public_source_sha256"],
    )
    if (
        verified_plan["plan_sha256"] != config["expected_plan_sha256"]
        or verified_plan["question_count"]
        != config["expected_question_count"]
        or verified_plan["route_count"] != config["expected_route_count"]
    ):
        raise ValueError("prepared plan does not match frozen configuration")
    binding_sources = {
        key: value for key, value in sources.items()
        if key not in {"binding_dir", "trial_dag_dir",
                       "n2_index_package_dir"}
    }
    bound = freeze_interleaved_route_bindings(
        output_dir=targets["binding_dir"], **binding_sources,
    )
    if bound != verify_interleaved_route_bindings(
        targets["binding_dir"], **binding_sources,
    ) or bound["route_count"] != plan["route_count"]:
        raise ValueError("frozen route binding verification differs")
    dag_sources = {
        **binding_sources, "binding_dir": targets["binding_dir"],
    }
    dags = freeze_interleaved_trial_dags(
        output_dir=targets["trial_dag_dir"], **dag_sources,
    )
    if dags != verify_interleaved_trial_dags(
        targets["trial_dag_dir"], **dag_sources,
    ) or dags["trial_count"] != plan["route_count"]:
        raise ValueError("frozen trial DAG verification differs")
    admission_sources = {
        **dag_sources,
        "trial_dag_dir": targets["trial_dag_dir"],
        "n2_index_package_dir": sources["n2_index_package_dir"],
        "coordinator_base_url": config["coordinator_base_url"],
    }
    admitted = freeze_interleaved_runtime_admission(
        output_dir=targets["admission_dir"], **admission_sources,
    )
    if admitted != verify_interleaved_runtime_admission(
        targets["admission_dir"], **admission_sources,
    ) or admitted["trial_count"] != plan["route_count"]:
        raise ValueError("frozen admission verification differs")
    return {
        "status": "FROZEN_INTERLEAVED_BATCH_INPUTS",
        "question_count": plan["question_count"],
        "route_count": admitted["trial_count"],
        "admission_sha256": admitted["admission_sha256"],
    }


def load_inputs(config: dict, artifact_root: Path) -> dict:
    if config["schema_version"] != SCHEMA:
        from experiments.ten_route_batch import load_inputs as load_ten
        return load_ten(config, artifact_root)
    root = artifact_root.resolve()
    sources = {
        key: _path(root, value)
        for key, value in config["source_dirs"].items()
    }
    admission = _path(root, config["admission_dir"])
    report = verify_interleaved_runtime_admission(
        admission, **sources,
        coordinator_base_url=config["coordinator_base_url"],
    )
    if report["status"] != (
        "VERIFIED_INTERLEAVED_RUNTIME_ADMISSION_NOT_DEPLOYED"
    ):
        raise ValueError("admission source-bound verification failed")
    plan = _read(sources["plan_dir"] / "interleaved-plan.json")
    if (
        plan.get("plan_sha256") != config["expected_plan_sha256"]
        or plan.get("question_count") != config["expected_question_count"]
        or plan.get("route_count") != config["expected_route_count"]
        or report["trial_count"] != plan["route_count"]
        or report["index_query_plan_count"] != plan["question_count"]
    ):
        raise ValueError("frozen plan or admission cardinality differs")
    questions = _rows(sources["plan_dir"] / "public-questions.jsonl")
    trials = sorted(
        _rows(admission / "admitted-trials.jsonl"),
        key=lambda row: row["order_index"],
    )
    stages = _rows(admission / "admitted-stages.jsonl")
    routes = _rows(sources["binding_dir"] / "route-inputs.jsonl")
    episodes = _rows(admission / "cache-episode-bindings.jsonl")
    route_by_key = {row["trial_key"]: row for row in routes}
    episode_by_key = {row["trial_key"]: row for row in episodes}
    arms = tuple(plan["arm_ids"])
    question_ids = {row["question_id"] for row in questions}
    pairs = Counter((row["workload_id"], row["design_id"]) for row in trials)
    if (
        len(questions) != plan["question_count"]
        or len(question_ids) != len(questions)
        or len(trials) != plan["route_count"]
        or len(stages) != report["stage_count"]
        or len(route_by_key) != len(trials)
        or len(episode_by_key) != report["cache_episode_binding_count"]
        or len(set(arms)) != len(arms)
        or len(trials) != len(questions) * len(arms)
        or pairs != Counter({(q, arm): 1 for q in question_ids
                            for arm in arms})
        or [row["order_index"] for row in trials] != list(range(len(trials)))
    ):
        raise ValueError("frozen trials do not cover each question and arm once")
    for trial in trials:
        key = trial["trial_key"]
        route = route_by_key[key]
        episode = episode_by_key.get(key)
        if (
            trial["worker_alias"] != config["worker_alias"]
            or trial["route_coordinator_binding"]["base_url"]
            != config["coordinator_base_url"]
            or route["trial_key"] != key
        ):
            raise ValueError("trial, route, worker, or origin binding differs")
        if trial["design_id"] == "DC":
            if episode is None or episode["run_id"] != route["run_id"]:
                raise ValueError("DC cache episode binding differs")
        elif episode is not None:
            raise ValueError("non-DC route has a cache episode")
    baseline_sha = _baseline(
        root, config["baseline_spec_dir"],
        report["admission_sha256"], plan["plan_sha256"],
    )
    return {
        "report": report, "plan": plan, "trials": trials,
        "stages": stages, "routes": route_by_key,
        "episodes": episode_by_key, "baseline_sha256": baseline_sha,
    }


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _atomic_json(path: Path, value: dict) -> None:
    payload = _pretty(value)
    temporary = path.with_suffix(path.suffix + ".partial")
    with temporary.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _settings(config: dict) -> FlowMeshSettings:
    if not os.getenv("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"):
        raise ValueError("route ingress credential is missing")
    base_url = os.getenv("FLOWMESH_BASE_URL")
    if not base_url:
        raise ValueError("FlowMesh Root URL is missing")
    return FlowMeshSettings.from_environment(
        base_url=base_url, worker_alias=config["worker_alias"],
        task_timeout_seconds=config["task_timeout_seconds"],
        poll_interval_seconds=2.0, validate_before_submit=True,
    )


def run(config: dict, config_sha: str, context: dict,
        output_dir: Path | None, *, execute: bool,
        run_id: str | None = None) -> dict:
    if config["schema_version"] != SCHEMA:
        from experiments.ten_route_batch import run as run_ten
        return run_ten(config, config_sha, context, output_dir,
                       execute=execute, run_id=run_id)
    if run_id is not None:
        raise ValueError("interleaved run identities are frozen in the plan")
    if execute and (output_dir is None or output_dir.exists()):
        raise ValueError("execution requires an unused output directory")
    settings = _settings(config)
    client = SdkFlowMeshClient(settings)
    try:
        worker = describe_pinned_worker(client, settings)
        if (
            worker.alias != config["worker_alias"]
            or worker.node_alias != config["worker_node_alias"]
            or worker.status not in {"IDLE", "BUSY"}
        ):
            raise ValueError("exact current worker alias/node is not ready")
        if not execute:
            return {"status": "WORKER_READY", "worker_id": worker.worker_id}
        output = output_dir.resolve()
        output.mkdir(parents=True, exist_ok=False)
        report = context["report"]
        started = _utc()
        _atomic_json(output / "start.json", {
            "status": "STARTED", "started_utc": started,
            "config_sha256": config_sha,
            "admission_sha256": report["admission_sha256"],
            "baseline_spec_sha256": context["baseline_sha256"],
            "worker_alias": config["worker_alias"],
            "observed_worker_id": worker.worker_id,
            "credentials_recorded": False,
        })
        signer = full_flow_hmac_header_provider(
            os.environ["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"]
        )
        for ordinal, trial in enumerate(context["trials"]):
            route = context["routes"][trial["trial_key"]]
            episode = context["episodes"].get(trial["trial_key"])
            key = _hash(_canonical({
                "domain": "interleaved-admission-idempotency/v1",
                "run_id": route["run_id"], "trial_key": trial["trial_key"],
            }))
            executor = FlowMeshSemanticTrialExecutor(
                client=client, settings=settings, run_id=route["run_id"],
                bound_trials=context["trials"],
                bound_stages=context["stages"],
                runtime_header_provider=signer,
                api_task_timeout_seconds=config["task_timeout_seconds"],
                cache_episode_id=(
                    episode["cache_episode_id"] if episode else None
                ),
            )
            start_ns = monotonic_ns()
            route_started = _utc()
            print("ROUTE_START", ordinal, trial["design_id"], flush=True)
            try:
                result = dict(executor.execute(
                    trial=trial, idempotency_key=key,
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
                    "failure_code": getattr(
                        exc, "failure_code", type(exc).__name__,
                    ),
                    "route_started_utc": route_started,
                    "route_ended_utc": _utc(),
                    "elapsed_ms": (monotonic_ns() - start_ns) // 1_000_000,
                    "credentials_recorded": False,
                })
                return {"status": "STOPPED_AT_FIRST_FAILURE",
                        "ordinal": ordinal}
            _atomic_json(output / f"timing-{ordinal:02d}.json", {
                "ordinal": ordinal, "trial_key": trial["trial_key"],
                "route_started_utc": route_started,
                "route_ended_utc": _utc(),
                "elapsed_ms": (monotonic_ns() - start_ns) // 1_000_000,
            })
            print("ROUTE_COMPLETE", ordinal, trial["design_id"], flush=True)
        _atomic_json(output / "summary.json", {
            "status": "VERIFIED_INTERLEAVED_BATCH_EXECUTION",
            "config_sha256": config_sha,
            "admission_sha256": report["admission_sha256"],
            "baseline_spec_sha256": context["baseline_sha256"],
            "route_count": len(context["trials"]),
            "started_utc": started, "ended_utc": _utc(),
            "worker_alias": config["worker_alias"],
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        })
        return {"status": "ALL_ROUTES_COMPLETE",
                "route_count": len(context["trials"])}
    finally:
        client.close()


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("batch timestamp is absent")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("batch timestamp has no UTC offset")
    return parsed


def verify_output(config: dict, config_sha: str, context: dict,
                  output_dir: Path, *, seal: bool = False) -> dict:
    if config["schema_version"] != SCHEMA:
        from experiments.ten_route_batch import verify_output as verify_ten
        return verify_ten(config, context, output_dir, seal=seal)
    root = output_dir.resolve()
    if not root.is_dir() or (root / "failure.json").exists():
        raise ValueError("route output is missing or failed")
    trials = context["trials"]
    count = len(trials)
    summary = _read(root / "summary.json")
    status = summary.get("status")
    legacy24 = status == "VERIFIED_24_ROUTE_EXECUTION" and count == 24
    legacy28 = status == "VERIFIED_28_ROUTE_EXECUTION" and count == 28
    current = status == "VERIFIED_INTERLEAVED_BATCH_EXECUTION"
    if not (legacy24 or legacy28 or current):
        raise ValueError("route output contract or count differs")
    expected = {
        "summary.json", "SHA256SUMS",
        *(f"route-{i:02d}.json" for i in range(count)),
    }
    if legacy24:
        expected.add("admission-sha256.txt")
    else:
        expected.add("start.json")
        expected.update(f"timing-{i:02d}.json" for i in range(count))
    if {p.name for p in root.iterdir()} != (
        expected - {"SHA256SUMS"} if seal else expected
    ):
        raise ValueError("route output file set differs")
    if (
        summary.get("route_count") != count
        or summary.get("admission_sha256")
        != context["report"]["admission_sha256"]
        or summary.get("worker_alias") != config["worker_alias"]
        or summary.get("credentials_recorded") is not False
        or summary.get("eligible_for_scientific_claims") is not False
    ):
        raise ValueError("route summary binding differs")
    if legacy24:
        if (root / "admission-sha256.txt").read_bytes() != (
            context["report"]["admission_sha256"] + "\n"
        ).encode("ascii"):
            raise ValueError("24-route admission marker differs")
    else:
        start = _read(root / "start.json")
        for record, required in ((start, "STARTED"), (summary, status)):
            if (
                record.get("status") != required
                or record.get("admission_sha256")
                != context["report"]["admission_sha256"]
                or record.get("baseline_spec_sha256")
                != context["baseline_sha256"]
                or record.get("worker_alias") != config["worker_alias"]
                or record.get("credentials_recorded") is not False
            ):
                raise ValueError("batch start/summary binding differs")
        if current and any(
            record.get("config_sha256") != config_sha
            for record in (start, summary)
        ):
            raise ValueError("batch configuration digest differs")
        if summary.get("started_utc") != start.get("started_utc"):
            raise ValueError("batch start timestamp differs")
        batch_start = _timestamp(start["started_utc"])
        batch_end = _timestamp(summary["ended_utc"])
        if batch_end < batch_start:
            raise ValueError("batch end precedes start")
        prior_end = batch_start
    stages = {row["stage_key"]: row for row in context["stages"]}
    success = {arm: {True: 0, False: 0}
               for arm in context["plan"]["arm_ids"]}
    execution_ids: set[str] = set()
    flowmesh_ids: set[str] = set()
    for ordinal, trial in enumerate(trials):
        result = _read(root / f"route-{ordinal:02d}.json")
        _assert_public_evidence(result)
        route = context["routes"][trial["trial_key"]]
        episode = context["episodes"].get(trial["trial_key"])
        evidence = verify_semantic_route_evidence(
            result["semantic_route_evidence"],
            run_id=route["run_id"], bound_trial=trial,
            bound_stages=[stages[key]
                          for key in trial["semantic_stage_keys"]],
            cache_episode_id=(
                episode["cache_episode_id"] if episode else None
            ),
        )
        key = _hash(_canonical({
            "domain": "interleaved-admission-idempotency/v1",
            "run_id": route["run_id"], "trial_key": trial["trial_key"],
        }))
        if (
            result.get("status") != "COMPLETE"
            or result.get("trial_key") != trial["trial_key"]
            or result.get("idempotency_key") != key
            or result.get("route_evidence_sha256")
            != evidence["evidence_sha256"]
            or result.get("execution_transport") != "flowmesh"
            or result.get("n1_score_authenticity_verified") is not True
            or result.get("telemetry_complete") is not True
            or result.get("llm_called") is not True
            or result.get("credentials_recorded") is not False
            or result.get("eligible_for_scientific_claims") is not False
            or type(result.get("task_success")) is not bool
        ):
            raise ValueError(f"route {ordinal} public evidence differs")
        if not legacy24:
            timing = _read(root / f"timing-{ordinal:02d}.json")
            if (
                timing.get("ordinal") != ordinal
                or timing.get("trial_key") != trial["trial_key"]
                or type(timing.get("elapsed_ms")) is not int
                or timing["elapsed_ms"] < 0
            ):
                raise ValueError(f"route {ordinal} timing differs")
            route_start = _timestamp(timing["route_started_utc"])
            route_end = _timestamp(timing["route_ended_utc"])
            if not (prior_end <= route_start <= route_end <= batch_end):
                raise ValueError(f"route {ordinal} is not serial in batch")
            prior_end = route_end
            for identity, seen in (
                (result["semantic_route_evidence"].get("execution_id"),
                 execution_ids),
                (result.get("flowmesh_workflow_evidence_sha256"),
                 flowmesh_ids),
            ):
                if not isinstance(identity, str) or not identity or (
                    identity in seen
                ):
                    raise ValueError(f"route {ordinal} identity is reused")
                seen.add(identity)
        success[trial["design_id"]][result["task_success"]] += 1
    if any(sum(values.values()) != context["plan"]["question_count"]
           for values in success.values()):
        raise ValueError("per-arm question coverage differs")
    checksums = "".join(
        f"{_hash(path.read_bytes())}  {path.name}\n"
        for path in sorted(root.iterdir()) if path.name != "SHA256SUMS"
    ).encode("ascii")
    if seal:
        with (root / "SHA256SUMS").open("xb") as handle:
            handle.write(checksums)
            handle.flush()
            os.fsync(handle.fileno())
    if (root / "SHA256SUMS").read_bytes() != checksums:
        raise ValueError("route checksum manifest differs")
    return {
        "status": "VERIFIED_INTERLEAVED_BATCH_OUTPUT",
        "question_count": context["plan"]["question_count"],
        "route_count": count,
        "config_sha256": config_sha if current else None,
        "admission_sha256": context["report"]["admission_sha256"],
        "baseline_spec_sha256": context["baseline_sha256"],
        "success_by_arm": {
            arm: {"correct": values[True], "incorrect": values[False]}
            for arm, values in success.items()
        },
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    actions = parser.add_subparsers(dest="action", required=True)
    freeze = actions.add_parser("freeze-config")
    freeze.add_argument("--draft", type=Path, required=True)
    freeze.add_argument("--output-dir", type=Path, required=True)
    for name in ("freeze-inputs", "check", "preflight", "execute", "verify"):
        action = actions.add_parser(name)
        action.add_argument("--config-dir", type=Path, required=True)
        action.add_argument("--artifact-root", type=Path, required=True)
        if name in {"execute", "verify"}:
            action.add_argument("--output-dir", type=Path, required=True)
        if name == "execute":
            action.add_argument("--run-id", help="fresh ten-route run ID")
        if name == "verify":
            action.add_argument("--seal", action="store_true")
    args = parser.parse_args(argv)
    if args.action == "freeze-config":
        print(json.dumps({"config_sha256": freeze_config(
            args.draft, args.output_dir,
        )}, sort_keys=True))
        return 0
    config, config_sha = load_config(args.config_dir)
    if args.action == "freeze-inputs":
        print(json.dumps(
            freeze_inputs(config, args.artifact_root), sort_keys=True,
        ))
        return 0
    context = load_inputs(config, args.artifact_root)
    if args.action == "check":
        result = {"status": "SOURCE_BOUND_INPUTS_VERIFIED",
                  "config_sha256": config_sha,
                  "admission_sha256": context["report"]["admission_sha256"],
                  "question_count": context["plan"]["question_count"],
                  "route_count": len(context["trials"])}
    elif args.action == "verify":
        result = verify_output(
            config, config_sha, context, args.output_dir, seal=args.seal,
        )
    else:
        result = run(
            config, config_sha, context,
            getattr(args, "output_dir", None),
            execute=args.action == "execute",
            run_id=getattr(args, "run_id", None),
        )
    print(json.dumps(result, sort_keys=True))
    return 2 if result["status"] == "STOPPED_AT_FIRST_FAILURE" else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("INTERLEAVED_BATCH_FAILED", type(exc).__name__, file=sys.stderr)
        raise SystemExit(2) from None
