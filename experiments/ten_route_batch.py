"""Configuration adapter for the existing ten-route runner and verifier.

Keep the canonical D0--D7 selection, cache ordering, idempotency domain and
three-file receipt format. No route or scoring semantics are reimplemented.
"""

from __future__ import annotations

import os
from pathlib import Path
from time import monotonic_ns

from experiments import interleaved_batch as common
from pathfinder.cli import _load_n4_live_gate_sources
from pathfinder.simulator import full_flow_local_semantic_smoke as smoke
from pathfinder.simulator.full_flow_one_case import (
    freeze_full_flow_one_case_plan,
    load_full_flow_one_case_plan,
)


SCHEMA = "pathfinder.ten-route-batch-config/v1"
REQUIRED_SOURCES = frozenset({
    "local_semantic_admission_dir", "n4_serve_gate_dir",
    "deployment_binding_dir", "logical_route_dir", "scenario_path",
    "container_plan_dir", "artifact_binding_dir",
})
OPTIONAL_SOURCES = frozenset({
    "compose_overlay_dir", "service_bootstrap_dir",
    "n4_gate_deployment_binding_dir", "provisioning_catalog_dir",
    "n4_package_dir", "one_case_plan_dir",
})
CONFIG_KEYS = frozenset({
    "schema_version", "runtime_environment", "source_dirs",
    "worker_alias", "worker_node_alias", "task_timeout_seconds",
    "expected_admission_sha256", "one_case_selection",
    "n4_live_gate_sources_file", "n4_live_gate_sources_sha256",
})


def validate_config(config: dict) -> dict:
    if set(config) != CONFIG_KEYS or config["schema_version"] != SCHEMA:
        raise ValueError("ten-route configuration keys or schema differ")
    if config["runtime_environment"] not in {
        "local", "multi-host-private-network",
    }:
        raise ValueError("unknown ten-route runtime environment")
    sources = config["source_dirs"]
    if not isinstance(sources, dict) or set(sources) != (
        REQUIRED_SOURCES | OPTIONAL_SOURCES
    ):
        raise ValueError("ten-route source keys differ")
    for key, value in sources.items():
        if value is None and key in OPTIONAL_SOURCES:
            continue
        common._relative(value)
    for key in ("worker_alias", "worker_node_alias"):
        smoke._identifier(config[key], key)
    timeout = config["task_timeout_seconds"]
    if type(timeout) is not int or timeout < 900:
        raise ValueError("task timeout is below the established 900s floor")
    smoke._digest(config["expected_admission_sha256"], "admission digest")
    live = config["n4_live_gate_sources_file"]
    live_sha = config["n4_live_gate_sources_sha256"]
    if live is not None:
        common._relative(live)
        smoke._digest(live_sha, "live gate descriptor digest")
    elif live_sha is not None:
        raise ValueError("live gate descriptor digest without a descriptor")
    if live is None or config["runtime_environment"] == "local":
        for key in ("compose_overlay_dir", "service_bootstrap_dir",
                    "provisioning_catalog_dir", "n4_package_dir"):
            if sources[key] is None:
                raise ValueError("preprovisioned/local source is missing")
    selection = config["one_case_selection"]
    if (selection is None) != (sources["one_case_plan_dir"] is None):
        raise ValueError("one-case plan and selection must be supplied together")
    if selection is not None:
        if not isinstance(selection, dict) or set(selection) != {
            "case_id", "workload_id", "safe_design_id",
        }:
            raise ValueError("one-case selection keys differ")
        for key, value in selection.items():
            smoke._identifier(value, key)
        if selection["safe_design_id"] not in {f"D{i}" for i in range(8)}:
            raise ValueError("one-case safe design is outside D0--D7")
    return config


def _sources(config: dict, root: Path) -> dict:
    sources = {
        key: common._path(root, value)
        for key, value in config["source_dirs"].items() if value is not None
    }
    live_file = config["n4_live_gate_sources_file"]
    if live_file is not None:
        path = common._path(root, live_file)
        if common._hash(path.read_bytes()) != (
            config["n4_live_gate_sources_sha256"]
        ):
            raise ValueError("N4 live gate source descriptor checksum differs")
        live = _load_n4_live_gate_sources(path)
        # The existing loader resolves descriptor-relative paths. Enforce the
        # same artifact-root boundary used by the reusable batch configuration.
        def check_paths(value: object) -> None:
            if isinstance(value, Path):
                if not value.is_relative_to(root.resolve()):
                    raise ValueError("live gate path escapes artifact root")
            elif isinstance(value, dict):
                for child in value.values():
                    check_paths(child)
            elif isinstance(value, (list, tuple)):
                for child in value:
                    check_paths(child)
        check_paths(live)
        sources["n4_live_gate_sources"] = live
    return sources


def _admission(config: dict, sources: dict):
    inputs = smoke.load_full_flow_local_semantic_execution_inputs(
        sources["local_semantic_admission_dir"],
    )
    if (
        inputs.admission["admission_sha256"]
        != config["expected_admission_sha256"]
        or inputs.admission.get("worker_pin")
        != {"kind": "worker_alias", "value": config["worker_alias"]}
    ):
        raise ValueError("ten-route admission digest or worker pin differs")
    return inputs


def freeze_inputs(config: dict, artifact_root: Path) -> dict:
    selection = config["one_case_selection"]
    if selection is None:
        raise ValueError("representative smokes already belong to admission")
    sources = _sources(config, artifact_root)
    _admission(config, sources)
    return freeze_full_flow_one_case_plan(
        sources["local_semantic_admission_dir"], **selection,
        output_dir=sources["one_case_plan_dir"],
    )


def load_inputs(config: dict, artifact_root: Path) -> dict:
    sources = _sources(config, artifact_root)
    inputs = _admission(config, sources)
    plan_root = sources.get("one_case_plan_dir")
    one_case = None
    if plan_root is not None:
        one_case = load_full_flow_one_case_plan(
            plan_root,
            local_semantic_admission_dir=sources["local_semantic_admission_dir"],
        )
        if any(one_case.plan.get(k) != v
               for k, v in config["one_case_selection"].items()):
            raise ValueError("one-case selection differs from configuration")
    selections = smoke._smoke_rows(inputs, one_case)
    if config["runtime_environment"] == "multi-host-private-network":
        smoke._verify_multi_host_smoke_sources(**{
            key: sources[key] for key in (
                "local_semantic_admission_dir", "deployment_binding_dir",
                "logical_route_dir", "scenario_path", "container_plan_dir",
            )
        })
    gate_sources = {
        "logical_plan_dir": sources["logical_route_dir"],
        **{key: sources[key] for key in (
            "compose_overlay_dir", "service_bootstrap_dir",
            "deployment_binding_dir", "scenario_path", "container_plan_dir",
            "provisioning_catalog_dir", "artifact_binding_dir", "n4_package_dir",
        ) if key in sources},
    }
    if "n4_gate_deployment_binding_dir" in sources:
        gate_sources["deployment_binding_dir"] = sources[
            "n4_gate_deployment_binding_dir"
        ]
    smoke._verify_n4_serve_gate(
        sources["n4_serve_gate_dir"],
        preprovisioned_sources=gate_sources,
        live_sources=sources.get("n4_live_gate_sources"),
        admission_root=sources["local_semantic_admission_dir"],
        artifact_binding_root=sources["artifact_binding_dir"],
    )
    if config["runtime_environment"] == "local":
        if "n4_gate_deployment_binding_dir" in sources:
            sources["deployment_binding_dir"] = sources.pop(
                "n4_gate_deployment_binding_dir"
            )
    return {
        "sources": sources, "inputs": inputs,
        "trials": [trial for _, trial in selections],
        "plan": {"question_count": len({
            trial["workload_id"] for _, trial in selections
        })},
        "report": {"admission_sha256": inputs.admission["admission_sha256"]},
    }


def verify_output(config: dict, context: dict, output_dir: Path,
                  *, seal: bool = False) -> dict:
    if seal:
        raise ValueError("ten-route canonical runner already seals its output")
    verifier = (
        smoke.verify_full_flow_semantic_smokes
        if config["runtime_environment"] == "multi-host-private-network"
        else smoke.verify_full_flow_local_semantic_smokes
    )
    return verifier(output_dir, **context["sources"])


def run(config: dict, config_sha: str, context: dict,
        output_dir: Path | None, *, execute: bool,
        run_id: str | None = None) -> dict:
    if execute:
        smoke._identifier(run_id, "fresh run_id")
        if output_dir is None or output_dir.exists():
            raise ValueError("execution requires an unused output directory")
        journal = output_dir.with_name(output_dir.name + ".attempt")
        if journal.exists():
            raise ValueError("execution attempt already exists")
    settings = common._settings(config)
    client = common.SdkFlowMeshClient(settings)
    try:
        worker = common.describe_pinned_worker(client, settings)
        if (worker.alias != config["worker_alias"]
                or worker.node_alias != config["worker_node_alias"]
                or worker.status not in {"IDLE", "BUSY"}):
            raise ValueError("exact current worker alias/node is not ready")
        if not execute:
            return {"status": "WORKER_READY", "worker_id": worker.worker_id}
        journal.mkdir(parents=True, exist_ok=False)
        common._atomic_json(journal / "start.json", {
            "config_sha256": config_sha, "run_id": run_id,
            "admission_sha256": context["report"]["admission_sha256"],
            "observed_worker_id": worker.worker_id,
            "started_utc": common._utc(), "credentials_recorded": False,
        })
        executor = common.FlowMeshSemanticTrialExecutor(
            client=client, settings=settings, run_id=run_id,
            bound_trials=context["inputs"].bound_trials,
            bound_stages=context["inputs"].bound_stages,
            runtime_header_provider=common.full_flow_hmac_header_provider(
                os.environ["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"],
            ),
            api_task_timeout_seconds=config["task_timeout_seconds"],
        )

        class JournalExecutor:
            ordinal = 0

            def execute(self, *, trial, idempotency_key):
                started = common._utc()
                clock = monotonic_ns()
                result = dict(executor.execute(
                    trial=trial, idempotency_key=idempotency_key,
                ))
                common._assert_public_evidence(result)
                common._atomic_json(
                    journal / f"route-{self.ordinal:02d}.json", result,
                )
                common._atomic_json(
                    journal / f"timing-{self.ordinal:02d}.json", {
                        "trial_key": trial["trial_key"],
                        "route_started_utc": started,
                        "route_ended_utc": common._utc(),
                        "elapsed_ms": (monotonic_ns() - clock) // 1_000_000,
                    },
                )
                self.ordinal += 1
                return result

        runner = (
            smoke.run_full_flow_semantic_smokes
            if config["runtime_environment"] == "multi-host-private-network"
            else smoke.run_full_flow_local_semantic_smokes
        )
        try:
            return runner(
                **context["sources"], run_id=run_id,
                executor=JournalExecutor(), output_dir=output_dir,
            )
        except Exception as exc:
            common._atomic_json(journal / "failure.json", {
                "status": "STOPPED_AT_FIRST_FAILURE",
                "failure_class": getattr(exc, "failure_class", "internal"),
                "failure_code": getattr(exc, "failure_code",
                                        type(exc).__name__),
                "ended_utc": common._utc(), "credentials_recorded": False,
            })
            raise
    finally:
        client.close()
