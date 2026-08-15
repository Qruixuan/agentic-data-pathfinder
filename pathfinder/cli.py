from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Sequence

from .config import ConfigError, load_config
from .experiment import run_pilot, run_session
from .telemetry import JsonlTelemetryStore


DEFAULT_CONFIG = Path("configs/minimal_system.json")
DEFAULT_FLOWMESH_STATE_DB = Path("outputs/flowmesh/gateway.sqlite3")
DEFAULT_FLOWMESH_PILOT_CONFIG = Path(
    "configs/phase_a_quote_pilot_dry_run.json"
)
DEFAULT_DATA_AGENT_MANIFEST = Path("configs/data_agent_manifest.json")
DEFAULT_DATA_AGENT_OPERATION_DB = Path(
    "outputs/data_agent/operations.sqlite3"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pathfinder",
        description="Minimal Pathfinder causal access-response harness",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    validate = subcommands.add_parser(
        "validate-config",
        help="validate the experiment contract",
    )
    validate.add_argument("--config", type=Path, default=DEFAULT_CONFIG)

    session = subcommands.add_parser(
        "run-session",
        help="run one reproducible agent session",
    )
    session.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    session.add_argument("--design", required=True)
    session.add_argument("--task-class", required=True)
    session.add_argument("--quote-profile", default="as_designed")
    session.add_argument("--latency-multiplier", type=float, default=1.0)
    session.add_argument("--seed", type=int, default=1)
    session.add_argument("--trial-id", default="manual")
    session.add_argument("--output", type=Path)
    session.add_argument("--compact", action="store_true")

    pilot = subcommands.add_parser(
        "run-pilot",
        help="run the quote and latency intervention grid",
    )
    pilot.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    pilot.add_argument("--output-dir", type=Path, default=Path("outputs/pilot"))
    pilot.add_argument("--design", action="append", dest="designs")
    pilot.add_argument("--task-class", action="append", dest="task_classes")
    pilot.add_argument("--quote-profile", action="append", dest="quote_profiles")
    pilot.add_argument(
        "--latency-multiplier",
        action="append",
        type=float,
        dest="latency_multipliers",
    )
    pilot.add_argument("--trials-per-cell", type=int)

    flowmesh_session = subcommands.add_parser(
        "run-flowmesh-session",
        help="run one real Agent session through FlowMesh",
    )
    flowmesh_session.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    flowmesh_session.add_argument("--design", required=True)
    flowmesh_session.add_argument("--task-class", required=True)
    flowmesh_session.add_argument("--quote-profile", default="as_designed")
    flowmesh_session.add_argument(
        "--latency-multiplier",
        type=float,
        default=1.0,
    )
    flowmesh_session.add_argument("--seed", type=int, default=1)
    flowmesh_session.add_argument("--trial-id", default="flowmesh-manual")
    flowmesh_session.add_argument("--session-id")
    flowmesh_session.add_argument(
        "--object-id",
        help="logical dataset object served by the Data Agent",
    )
    question = flowmesh_session.add_mutually_exclusive_group(required=True)
    question.add_argument("--question")
    question.add_argument("--question-file", type=Path)
    flowmesh_session.add_argument(
        "--state-db",
        type=Path,
        default=DEFAULT_FLOWMESH_STATE_DB,
    )
    flowmesh_session.add_argument("--flowmesh-base-url")
    flowmesh_session.add_argument("--agent-config")
    flowmesh_session.add_argument("--task-timeout", type=int, default=600)
    flowmesh_session.add_argument("--poll-interval", type=float, default=2.0)
    pin = flowmesh_session.add_mutually_exclusive_group()
    pin.add_argument(
        "--worker-id",
        help=(
            "pin this session to an exact FlowMesh worker ID, e.g. wkr-16; "
            "falls back to PATHFINDER_FLOWMESH_WORKER_ID"
        ),
    )
    pin.add_argument(
        "--worker-alias",
        help=(
            "pin this session to the worker currently holding this stable "
            "alias; falls back to PATHFINDER_FLOWMESH_WORKER_ALIAS"
        ),
    )
    flowmesh_session.add_argument(
        "--validate-workflow",
        action="store_true",
        help="validate the workflow through FlowMesh before submitting it",
    )
    flowmesh_session.add_argument(
        "--data-agent-url",
        help=(
            "remote Data Agent base URL used to reconcile artifact "
            "downloads; falls back to PATHFINDER_DATA_AGENT_URL"
        ),
    )
    flowmesh_session.add_argument(
        "--data-agent-timeout",
        type=float,
        default=30.0,
    )
    flowmesh_session.add_argument(
        "--data-agent-max-retries",
        type=int,
        default=1,
    )
    flowmesh_session.add_argument(
        "--telemetry-quiescence-timeout",
        type=float,
        default=15.0,
        help=(
            "seconds to wait for final artifact telemetry; shared-host "
            "experiments default to 15s and still fail closed on timeout"
        ),
    )
    flowmesh_session.add_argument("--compact", action="store_true")

    flowmesh_pilot = subcommands.add_parser(
        "run-flowmesh-pilot",
        help="run or resume a randomized pilot through real FlowMesh",
    )
    flowmesh_pilot.add_argument(
        "--pilot-config",
        type=Path,
        default=DEFAULT_FLOWMESH_PILOT_CONFIG,
    )
    flowmesh_pilot.add_argument(
        "--config",
        type=Path,
        help="override the system_config named by the pilot plan",
    )
    flowmesh_pilot.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "batch output directory; defaults to "
            "outputs/flowmesh-pilot/<experiment-id>"
        ),
    )
    flowmesh_pilot.add_argument(
        "--state-db",
        type=Path,
        help=(
            "SQLite state shared with the already-running MCP gateway; "
            "defaults to <output-dir>/gateway.sqlite3"
        ),
    )
    flowmesh_pilot.add_argument(
        "--repetitions",
        type=int,
        help="override repetitions per workload/intervention cell",
    )
    flowmesh_pilot.add_argument(
        "--randomization-seed",
        type=int,
        help="override the frozen trial-order seed",
    )
    flowmesh_pilot.add_argument("--flowmesh-base-url")
    flowmesh_pilot.add_argument("--agent-config")
    flowmesh_pilot.add_argument("--task-timeout", type=int, default=600)
    flowmesh_pilot.add_argument("--poll-interval", type=float, default=2.0)
    pilot_pin = flowmesh_pilot.add_mutually_exclusive_group()
    pilot_pin.add_argument("--worker-id")
    pilot_pin.add_argument("--worker-alias")
    flowmesh_pilot.add_argument(
        "--validate-workflow",
        action="store_true",
    )
    flowmesh_pilot.add_argument(
        "--data-agent-url",
        help=(
            "required remote Data Agent URL; falls back to "
            "PATHFINDER_DATA_AGENT_URL"
        ),
    )
    flowmesh_pilot.add_argument(
        "--data-agent-timeout",
        type=float,
        default=30.0,
    )
    flowmesh_pilot.add_argument(
        "--data-agent-max-retries",
        type=int,
        default=1,
    )
    flowmesh_pilot.add_argument(
        "--telemetry-quiescence-timeout",
        type=float,
        default=15.0,
        help=(
            "seconds to wait for final artifact telemetry; timeout records "
            "a telemetry failure rather than accepting stale counters"
        ),
    )
    flowmesh_pilot.add_argument("--compact", action="store_true")

    gateway = subcommands.add_parser(
        "serve-flowmesh-tools",
        help="serve Pathfinder access tools over Streamable HTTP MCP",
    )
    gateway.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    gateway.add_argument(
        "--state-db",
        type=Path,
        default=DEFAULT_FLOWMESH_STATE_DB,
    )
    gateway.add_argument("--host", default="0.0.0.0")
    gateway.add_argument("--port", type=int, default=8765)
    gateway.add_argument(
        "--data-agent-url",
        help=(
            "remote Data Agent base URL; falls back to "
            "PATHFINDER_DATA_AGENT_URL"
        ),
    )
    gateway.add_argument(
        "--data-agent-timeout",
        type=float,
        default=30.0,
    )
    gateway.add_argument(
        "--data-agent-max-retries",
        type=int,
        default=1,
    )
    gateway.add_argument(
        "--telemetry-quiescence-timeout",
        type=float,
        default=15.0,
    )

    data_agent = subcommands.add_parser(
        "serve-data-agent",
        help="serve manifest-backed Pathfinder Data Agent access",
    )
    data_agent.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_DATA_AGENT_MANIFEST,
    )
    data_agent.add_argument(
        "--operation-db",
        type=Path,
        default=DEFAULT_DATA_AGENT_OPERATION_DB,
    )
    data_agent.add_argument("--host", default="0.0.0.0")
    data_agent.add_argument("--port", type=int, default=8780)
    data_agent.add_argument("--public-base-url")
    data_agent.add_argument(
        "--artifact-url-ttl",
        type=int,
        default=300,
    )
    data_agent.add_argument(
        "--max-request-bytes",
        type=int,
        default=1024 * 1024,
    )
    data_agent.add_argument(
        "--max-inline-bytes",
        type=int,
        default=1024 * 1024,
    )
    return parser


def _print_payload(payload: object, *, compact: bool) -> int:
    print(
        json.dumps(
            payload,
            indent=None if compact else 2,
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "serve-data-agent":
            from .data_agent_server import (
                DataAgentServerSettings,
                run_data_agent_server,
            )

            run_data_agent_server(
                manifest_path=args.manifest,
                operation_db=args.operation_db,
                settings=DataAgentServerSettings.from_environment(
                    host=args.host,
                    port=args.port,
                    public_base_url=args.public_base_url,
                    artifact_url_ttl_seconds=args.artifact_url_ttl,
                    max_request_bytes=args.max_request_bytes,
                    max_inline_bytes=args.max_inline_bytes,
                ),
            )
            return 0
        if args.command == "run-flowmesh-pilot":
            from .data_agent_client import (
                DataAgentClientSettings,
                HttpDataAgentClient,
            )
            from .integrations.flowmesh import (
                AccessGateway,
                FlowMeshAgentAdapter,
                FlowMeshSettings,
                RemoteDataAgentBackend,
                SdkFlowMeshClient,
                SQLiteSessionStore,
            )
            from .integrations.flowmesh.pilot import (
                load_flowmesh_pilot_config,
                run_flowmesh_pilot,
            )

            pilot_config = load_flowmesh_pilot_config(args.pilot_config)
            config = load_config(args.config or pilot_config.system_config_path)
            output_dir = args.output_dir or (
                Path("outputs")
                / "flowmesh-pilot"
                / pilot_config.experiment_id
            )
            state_db = args.state_db or output_dir / "gateway.sqlite3"
            resolved_data_agent_url = (
                args.data_agent_url
                or os.getenv("PATHFINDER_DATA_AGENT_URL")
            )
            if not resolved_data_agent_url:
                raise ConfigError(
                    "run-flowmesh-pilot requires --data-agent-url or "
                    "PATHFINDER_DATA_AGENT_URL; the emulated backend is not "
                    "valid for a real pilot"
                )
            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                agent_config_name=args.agent_config,
                task_timeout_seconds=args.task_timeout,
                poll_interval_seconds=args.poll_interval,
                worker_id=args.worker_id,
                worker_alias=args.worker_alias,
                validate_before_submit=(
                    True if args.validate_workflow else None
                ),
            )
            backend = RemoteDataAgentBackend(
                HttpDataAgentClient(
                    DataAgentClientSettings.from_environment(
                        base_url=resolved_data_agent_url,
                        timeout_seconds=args.data_agent_timeout,
                        max_retries=args.data_agent_max_retries,
                    )
                ),
                telemetry_quiescence_timeout_seconds=(
                    args.telemetry_quiescence_timeout
                ),
            )
            gateway = AccessGateway(
                config,
                SQLiteSessionStore(state_db),
                backend,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = run_flowmesh_pilot(
                    pilot=pilot_config,
                    system=config,
                    adapter=FlowMeshAgentAdapter(
                        client,
                        gateway,
                        settings,
                    ),
                    output_dir=output_dir,
                    repetitions=args.repetitions,
                    randomization_seed=args.randomization_seed,
                    progress_callback=lambda event: print(
                        json.dumps(
                            {"status": "trial_recorded", **event},
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        file=sys.stderr,
                        flush=True,
                    ),
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        config = load_config(args.config)
        if args.command == "validate-config":
            payload = {
                "status": "valid",
                "config": str(config.source_path),
                "representations": len(config.representations),
                "task_classes": len(config.task_classes),
                "physical_designs": len(config.designs),
                "quote_profiles": len(config.quote_profiles),
            }
        elif args.command == "run-session":
            observation = run_session(
                config=config,
                design_id=args.design,
                task_class_id=args.task_class,
                quote_profile_id=args.quote_profile,
                latency_multiplier=args.latency_multiplier,
                seed=args.seed,
                trial_id=args.trial_id,
            )
            if args.output:
                JsonlTelemetryStore(args.output).append(observation)
            payload = observation.to_dict()
        elif args.command == "run-pilot":
            payload = run_pilot(
                config=config,
                output_dir=args.output_dir,
                design_ids=args.designs,
                task_class_ids=args.task_classes,
                quote_profile_ids=args.quote_profiles,
                latency_multipliers=args.latency_multipliers,
                trials_per_cell=args.trials_per_cell,
            )
        elif args.command == "run-flowmesh-session":
            from .integrations.flowmesh import (
                AccessGateway,
                FlowMeshAgentAdapter,
                FlowMeshAgentRunRequest,
                FlowMeshSettings,
                SdkFlowMeshClient,
                SQLiteSessionStore,
            )

            question_text = (
                args.question
                if args.question is not None
                else args.question_file.read_text(encoding="utf-8")
            )
            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                agent_config_name=args.agent_config,
                task_timeout_seconds=args.task_timeout,
                poll_interval_seconds=args.poll_interval,
                worker_id=args.worker_id,
                worker_alias=args.worker_alias,
                validate_before_submit=(
                    True if args.validate_workflow else None
                ),
            )
            resolved_data_agent_url = (
                args.data_agent_url
                or os.getenv("PATHFINDER_DATA_AGENT_URL")
            )
            backend = None
            if resolved_data_agent_url:
                from .data_agent_client import (
                    DataAgentClientSettings,
                    HttpDataAgentClient,
                )
                from .integrations.flowmesh.data_agent_backend import (
                    RemoteDataAgentBackend,
                )

                backend = RemoteDataAgentBackend(
                    HttpDataAgentClient(
                        DataAgentClientSettings.from_environment(
                            base_url=resolved_data_agent_url,
                            timeout_seconds=args.data_agent_timeout,
                            max_retries=args.data_agent_max_retries,
                        )
                    ),
                    telemetry_quiescence_timeout_seconds=(
                        args.telemetry_quiescence_timeout
                    ),
                )
            gateway = AccessGateway(
                config,
                SQLiteSessionStore(args.state_db),
                backend,
            )
            client = SdkFlowMeshClient(settings)
            try:
                result = FlowMeshAgentAdapter(
                    client,
                    gateway,
                    settings,
                ).run(
                    FlowMeshAgentRunRequest(
                        question=question_text,
                        design_id=args.design,
                        task_class_id=args.task_class,
                        quote_profile_id=args.quote_profile,
                        latency_multiplier=args.latency_multiplier,
                        seed=args.seed,
                        trial_id=args.trial_id,
                        session_id=args.session_id,
                        object_id=args.object_id,
                    )
                )
            finally:
                client.close()
            payload = result.to_dict()
        else:
            from .integrations.flowmesh.mcp_server import run_mcp_server

            run_mcp_server(
                config_path=args.config,
                state_db=args.state_db,
                host=args.host,
                port=args.port,
                data_agent_url=args.data_agent_url,
                data_agent_timeout_seconds=args.data_agent_timeout,
                data_agent_max_retries=args.data_agent_max_retries,
                telemetry_quiescence_timeout_seconds=(
                    args.telemetry_quiescence_timeout
                ),
            )
            return 0
    except (ConfigError, OSError, RuntimeError, ValueError) as exc:
        print(json.dumps({"status": "error", "message": str(exc)}))
        return 2

    return _print_payload(
        payload,
        compact=getattr(args, "compact", False),
    )
