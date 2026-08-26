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
DEFAULT_REDUCED_ORACLE_CONFIG = Path(
    "configs/reduced_oracle_mvp.json"
)
DEFAULT_AWM_CONFIG = Path("configs/awm_reduced_mvp.json")
DEFAULT_OED_CONFIG = Path("configs/oed_reduced_mvp.json")
DEFAULT_SYNTHETIC_FIXTURE_CONFIG = Path(
    "configs/synthetic_oracle_fixture.json"
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

    flowmesh_analysis = subcommands.add_parser(
        "analyze-flowmesh-pilot",
        help=(
            "audit a completed pilot without modifying its original "
            "runs.jsonl"
        ),
    )
    flowmesh_analysis.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="pilot directory containing runs.jsonl and trial_plan.json",
    )
    flowmesh_analysis.add_argument(
        "--output-dir",
        type=Path,
        help="derived-analysis directory; defaults to <input>-analysis",
    )
    flowmesh_analysis.add_argument("--compact", action="store_true")

    reduced_oracle = subcommands.add_parser(
        "run-reduced-oracle",
        help=(
            "exhaustively run a reduced design set without managing worker "
            "or service lifecycle"
        ),
    )
    reduced_oracle.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    reduced_oracle.add_argument("--output-dir", type=Path, required=True)
    reduced_oracle.add_argument("--state-db", type=Path, required=True)
    reduced_oracle.add_argument("--flowmesh-base-url")
    reduced_oracle.add_argument("--agent-config")
    reduced_oracle.add_argument("--task-timeout", type=int, default=600)
    reduced_oracle.add_argument("--poll-interval", type=float, default=2.0)
    oracle_pin = reduced_oracle.add_mutually_exclusive_group()
    oracle_pin.add_argument("--worker-id")
    oracle_pin.add_argument("--worker-alias")
    reduced_oracle.add_argument("--validate-workflow", action="store_true")
    reduced_oracle.add_argument("--data-agent-url")
    reduced_oracle.add_argument(
        "--data-agent-timeout",
        type=float,
        default=30.0,
    )
    reduced_oracle.add_argument(
        "--data-agent-max-retries",
        type=int,
        default=1,
    )
    reduced_oracle.add_argument(
        "--telemetry-quiescence-timeout",
        type=float,
        default=15.0,
    )
    reduced_oracle.add_argument("--compact", action="store_true")

    oracle_recovery_plan = subcommands.add_parser(
        "plan-reduced-oracle-recovery",
        help=(
            "audit an interrupted Oracle as immutable evidence and freeze "
            "the exact missing/infrastructure-failure retry set"
        ),
    )
    oracle_recovery_plan.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    oracle_recovery_plan.add_argument(
        "--incident-dir",
        type=Path,
        required=True,
    )
    oracle_recovery_plan.add_argument(
        "--recovery-dir",
        type=Path,
        required=True,
    )
    oracle_recovery_plan.add_argument("--compact", action="store_true")

    oracle_recovery = subcommands.add_parser(
        "run-reduced-oracle-recovery",
        help=(
            "retry only an audited Oracle's missing and infrastructure-failed "
            "trials using new session identities"
        ),
    )
    oracle_recovery.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    oracle_recovery.add_argument(
        "--incident-dir",
        type=Path,
        required=True,
    )
    oracle_recovery.add_argument(
        "--recovery-dir",
        type=Path,
        required=True,
    )
    oracle_recovery.add_argument("--state-db", type=Path, required=True)
    oracle_recovery.add_argument("--flowmesh-base-url")
    oracle_recovery.add_argument("--agent-config")
    oracle_recovery.add_argument("--task-timeout", type=int, default=600)
    oracle_recovery.add_argument("--poll-interval", type=float, default=2.0)
    recovery_pin = oracle_recovery.add_mutually_exclusive_group()
    recovery_pin.add_argument("--worker-id")
    recovery_pin.add_argument("--worker-alias")
    oracle_recovery.add_argument("--validate-workflow", action="store_true")
    oracle_recovery.add_argument("--data-agent-url")
    oracle_recovery.add_argument(
        "--data-agent-timeout",
        type=float,
        default=30.0,
    )
    oracle_recovery.add_argument(
        "--data-agent-max-retries",
        type=int,
        default=1,
    )
    oracle_recovery.add_argument(
        "--telemetry-quiescence-timeout",
        type=float,
        default=15.0,
    )
    oracle_recovery.add_argument(
        "--max-consecutive-infrastructure-failures",
        type=int,
        default=3,
    )
    oracle_recovery.add_argument(
        "--max-attempts-per-trial",
        type=int,
        default=3,
    )
    oracle_recovery.add_argument("--compact", action="store_true")

    oracle_analysis = subcommands.add_parser(
        "analyze-reduced-oracle",
        help="recompute a reduced-oracle table and lock-in trace",
    )
    oracle_analysis.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    oracle_analysis.add_argument("--output-dir", type=Path, required=True)
    oracle_analysis.add_argument("--compact", action="store_true")

    awm_evaluation = subcommands.add_parser(
        "evaluate-awm",
        help=(
            "fit assumption-free, independent, and coupled envelopes from "
            "a frozen Reduced Oracle run"
        ),
    )
    awm_evaluation.add_argument(
        "--awm-config",
        type=Path,
        default=DEFAULT_AWM_CONFIG,
    )
    awm_evaluation.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    awm_evaluation.add_argument(
        "--oracle-output-dir",
        type=Path,
        required=True,
    )
    awm_evaluation.add_argument("--output-dir", type=Path, required=True)
    awm_evaluation.add_argument("--compact", action="store_true")

    awm_heterogeneity = subcommands.add_parser(
        "audit-awm-heterogeneity",
        help=(
            "run a read-only post-hoc workload heterogeneity and "
            "safe-fallback policy diagnostic over a frozen Reduced Oracle"
        ),
    )
    awm_heterogeneity.add_argument(
        "--audit-config",
        type=Path,
        required=True,
    )
    awm_heterogeneity.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    awm_heterogeneity.add_argument(
        "--oracle-output-dir",
        type=Path,
        required=True,
    )
    awm_heterogeneity.add_argument(
        "--output-dir",
        type=Path,
        required=True,
    )
    awm_heterogeneity.add_argument("--compact", action="store_true")

    awm_certificate = subcommands.add_parser(
        "certify-awm-restricted-policy",
        help=(
            "run a read-only risk-constrained, workload-aware safety "
            "certificate for a restricted candidate policy over a frozen "
            "Reduced Oracle"
        ),
    )
    awm_certificate.add_argument(
        "--certificate-config",
        type=Path,
        required=True,
    )
    awm_certificate.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    awm_certificate.add_argument(
        "--oracle-output-dir",
        type=Path,
        required=True,
    )
    awm_certificate.add_argument(
        "--output-dir",
        type=Path,
        required=True,
    )
    awm_certificate.add_argument("--compact", action="store_true")

    awm_calibration = subcommands.add_parser(
        "calibrate-awm-certificate",
        help=(
            "run the fixed-seed Monte Carlo calibration of the v3alpha5 "
            "safety certificate over synthetic datasets with known truths"
        ),
    )
    awm_calibration.add_argument(
        "--calibration-config",
        type=Path,
        required=True,
    )
    awm_calibration.add_argument("--output-dir", type=Path, required=True)
    awm_calibration.add_argument(
        "--simulations",
        type=int,
        help="override the configured simulation count",
    )
    awm_calibration.add_argument(
        "--no-negative-control",
        action="store_true",
        help=(
            "skip the deliberately anti-conservative control arm; the "
            "control is what makes a zero false-safe rate informative"
        ),
    )
    awm_calibration.add_argument("--compact", action="store_true")

    pilot_preflight = subcommands.add_parser(
        "preflight-distributed-pilot",
        help=(
            "read-only verification of a distributed pilot deployment; "
            "submits nothing and starts nothing"
        ),
    )
    pilot_preflight.add_argument(
        "--preregistration",
        type=Path,
        required=True,
    )
    pilot_preflight.add_argument(
        "--endpoint-registry",
        type=Path,
        required=True,
    )
    pilot_preflight.add_argument(
        "--worker-alias",
        help="worker alias to check syntactically (no Root query)",
    )
    pilot_preflight.add_argument("--worker-id")
    pilot_preflight.add_argument(
        "--mode",
        choices=("offline_validation", "live_pilot"),
        default="offline_validation",
        help=(
            "live_pilot fails rather than warns on any remaining "
            "placeholder identity, provenance, or conversion rate"
        ),
    )
    pilot_preflight.add_argument(
        "--measurement-manifest",
        type=Path,
        help="bind an operator measurement manifest to this preflight",
    )
    pilot_preflight.add_argument("--compact", action="store_true")

    pilot_plan = subcommands.add_parser(
        "plan-distributed-pilot",
        help=(
            "build and print the deterministic distributed-pilot trial "
            "plan without executing any trial"
        ),
    )
    pilot_plan.add_argument("--preregistration", type=Path, required=True)
    pilot_plan.add_argument(
        "--output-dir",
        type=Path,
        help="write the frozen plan document here",
    )
    pilot_plan.add_argument("--compact", action="store_true")

    run_pilot = subcommands.add_parser(
        "run-distributed-pilot",
        help=(
            "execute the frozen distributed plan through the real FlowMesh "
            "adapter; starts no worker, Data Agent, or MCP service"
        ),
    )
    run_pilot.add_argument("--preregistration", type=Path, required=True)
    run_pilot.add_argument("--endpoint-registry", type=Path, required=True)
    run_pilot.add_argument("--measurement-manifest", type=Path, required=True)
    run_pilot.add_argument("--workload-manifest", type=Path, required=True)
    run_pilot.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    run_pilot.add_argument("--state-db", type=Path, required=True)
    run_pilot.add_argument("--output-dir", type=Path, required=True)
    run_pilot.add_argument("--worker-id")
    run_pilot.add_argument("--worker-alias")
    run_pilot.add_argument("--flowmesh-base-url")
    run_pilot.add_argument("--agent-config")
    run_pilot.add_argument("--task-timeout", type=float)
    run_pilot.add_argument("--poll-interval", type=float)
    run_pilot.add_argument(
        "--validate-workflow",
        action="store_true",
        default=None,
    )
    run_pilot.add_argument(
        "--telemetry-quiescence-timeout",
        type=float,
        default=15.0,
    )
    run_pilot.add_argument("--max-attempts", type=int, default=3)
    run_pilot.add_argument(
        "--mode",
        choices=("offline_validation", "live_pilot"),
        default="live_pilot",
        help="preflight strictness required before execution",
    )
    run_pilot.add_argument("--compact", action="store_true")

    oed_certificate = subcommands.add_parser(
        "run-oed-certificate-replay",
        help=(
            "replay Commit/Reveal/Stop under v3alpha5 three-state safety "
            "certificates against a frozen Reduced Oracle"
        ),
    )
    oed_certificate.add_argument("--oed-config", type=Path, required=True)
    oed_certificate.add_argument(
        "--certificate-config",
        type=Path,
        required=True,
    )
    oed_certificate.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    oed_certificate.add_argument(
        "--oracle-output-dir",
        type=Path,
        required=True,
    )
    oed_certificate.add_argument("--output-dir", type=Path, required=True)
    oed_certificate.add_argument("--compact", action="store_true")

    oed_replay = subcommands.add_parser(
        "run-oed-replay",
        help=(
            "replay Commit/Reveal/Hold/Stop and equal-budget baselines "
            "against a frozen Reduced Oracle"
        ),
    )
    oed_replay.add_argument(
        "--oed-config",
        type=Path,
        default=DEFAULT_OED_CONFIG,
    )
    oed_replay.add_argument(
        "--awm-config",
        type=Path,
        default=DEFAULT_AWM_CONFIG,
    )
    oed_replay.add_argument(
        "--oracle-config",
        type=Path,
        default=DEFAULT_REDUCED_ORACLE_CONFIG,
    )
    oed_replay.add_argument(
        "--oracle-output-dir",
        type=Path,
        required=True,
    )
    oed_replay.add_argument("--output-dir", type=Path, required=True)
    oed_replay.add_argument("--compact", action="store_true")

    synthetic_oracle = subcommands.add_parser(
        "generate-synthetic-oracle",
        help=(
            "generate a deterministic multi-candidate engineering fixture "
            "for the offline AWM and OED consumers; never physical evidence"
        ),
    )
    synthetic_oracle.add_argument(
        "--fixture-config",
        type=Path,
        default=DEFAULT_SYNTHETIC_FIXTURE_CONFIG,
    )
    synthetic_oracle.add_argument("--output-dir", type=Path, required=True)
    synthetic_oracle.add_argument("--compact", action="store_true")

    preflight = subcommands.add_parser(
        "preflight-flowmesh",
        help=(
            "read-only check that the configured FlowMesh Root sees exactly "
            "one current worker for a requested pin"
        ),
    )
    preflight.add_argument("--flowmesh-base-url")
    preflight_pin = preflight.add_mutually_exclusive_group()
    preflight_pin.add_argument(
        "--worker-id",
        help=(
            "verify this exact worker ID is current and visible through the "
            "configured Root; falls back to PATHFINDER_FLOWMESH_WORKER_ID"
        ),
    )
    preflight_pin.add_argument(
        "--worker-alias",
        help=(
            "verify this stable alias resolves to exactly one current "
            "worker; falls back to PATHFINDER_FLOWMESH_WORKER_ALIAS"
        ),
    )
    preflight.add_argument("--compact", action="store_true")

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
    gateway.add_argument(
        "--endpoint-registry",
        type=Path,
        help=(
            "route Data Agent access across the endpoints declared in this "
            "registry instead of a single --data-agent-url"
        ),
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
        if args.command == "analyze-flowmesh-pilot":
            from .integrations.flowmesh.analysis import (
                analyze_flowmesh_pilot,
            )

            payload = analyze_flowmesh_pilot(
                args.input_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "analyze-reduced-oracle":
            from .reduced_oracle import (
                analyze_reduced_oracle,
                load_reduced_oracle_config,
            )

            payload = analyze_reduced_oracle(
                load_reduced_oracle_config(args.oracle_config),
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "evaluate-awm":
            from .awm import evaluate_awm

            payload = evaluate_awm(
                args.awm_config,
                args.oracle_config,
                oracle_output_dir=args.oracle_output_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "generate-synthetic-oracle":
            from .synthetic_oracle import generate_synthetic_oracle_fixture

            payload = generate_synthetic_oracle_fixture(
                args.fixture_config,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "preflight-flowmesh":
            from .integrations.flowmesh import (
                FlowMeshSettings,
                SdkFlowMeshClient,
                preflight_flowmesh_worker,
            )

            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                worker_id=args.worker_id,
                worker_alias=args.worker_alias,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = preflight_flowmesh_worker(client, settings)
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-oed-replay":
            from .oed import run_oed_replay

            payload = run_oed_replay(
                args.oed_config,
                args.awm_config,
                args.oracle_config,
                oracle_output_dir=args.oracle_output_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-distributed-pilot":
            from .distributed import (
                FlowMeshDistributedSessionExecutor,
                HttpDataAgentHealthProbe,
                load_distributed_pilot_preregistration,
                load_endpoint_registry,
                load_measurement_manifest,
                preflight_distributed_pilot,
                run_distributed_pilot,
            )
            from .distributed.routing import (
                build_routed_gateway_backend,
                close_routed_backend,
            )
            from .integrations.flowmesh import (
                AccessGateway,
                FlowMeshAgentAdapter,
                FlowMeshSettings,
                SdkFlowMeshClient,
                SQLiteSessionStore,
            )

            preregistration = load_distributed_pilot_preregistration(
                args.preregistration
            )
            registry = load_endpoint_registry(args.endpoint_registry)
            provider = load_measurement_manifest(args.measurement_manifest)
            workloads = json.loads(
                Path(args.workload_manifest).read_text(encoding="utf-8")
            )
            if not isinstance(workloads, dict):
                raise ConfigError(
                    "the workload manifest must map workload_id -> workload"
                )
            config = load_config(args.config)

            backend, _ = build_routed_gateway_backend(
                args.endpoint_registry,
                telemetry_quiescence_timeout_seconds=(
                    args.telemetry_quiescence_timeout
                ),
            )
            probe = HttpDataAgentHealthProbe(registry)
            try:
                report = preflight_distributed_pilot(
                    preregistration,
                    registry,
                    probe=probe,
                    worker_pin=(
                        {"kind": "worker_id", "value": args.worker_id}
                        if args.worker_id
                        else (
                            {
                                "kind": "worker_alias",
                                "value": args.worker_alias,
                            }
                            if args.worker_alias
                            else None
                        )
                    ),
                    mode=args.mode,
                    measurement_manifest_sha256=provider.manifest_sha256,
                )
                if report["status"] != "ok":
                    return _print_payload(
                        {
                            "status": "error",
                            "message": (
                                "preflight failed; refusing to execute"
                            ),
                            "failed_checks": report["failed_checks"],
                        },
                        compact=args.compact,
                    ) or 1
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
                gateway = AccessGateway(
                    config,
                    SQLiteSessionStore(args.state_db),
                    backend,
                )
                client = SdkFlowMeshClient(settings)
                try:
                    executor = FlowMeshDistributedSessionExecutor(
                        FlowMeshAgentAdapter(client, gateway, settings)
                    )
                    payload = run_distributed_pilot(
                        preregistration,
                        registry,
                        executor,
                        output_dir=args.output_dir,
                        workloads=workloads,
                        provider=provider,
                        preflight=report,
                        max_attempts=args.max_attempts,
                    )
                finally:
                    client.close()
            finally:
                close_routed_backend(backend)
            return _print_payload(payload, compact=args.compact)
        if args.command == "preflight-distributed-pilot":
            from .distributed import (
                load_distributed_pilot_preregistration,
                load_endpoint_registry,
                preflight_distributed_pilot,
            )

            pin = None
            if args.worker_id:
                pin = {"kind": "worker_id", "value": args.worker_id}
            elif args.worker_alias:
                pin = {"kind": "worker_alias", "value": args.worker_alias}
            manifest_sha256 = None
            if args.measurement_manifest is not None:
                from .distributed import load_measurement_manifest

                manifest_sha256 = load_measurement_manifest(
                    args.measurement_manifest
                ).manifest_sha256
            payload = preflight_distributed_pilot(
                load_distributed_pilot_preregistration(args.preregistration),
                load_endpoint_registry(args.endpoint_registry),
                worker_pin=pin,
                mode=args.mode,
                measurement_manifest_sha256=manifest_sha256,
            )
            _print_payload(payload, compact=args.compact)
            return 0 if payload["status"] == "ok" else 1
        if args.command == "plan-distributed-pilot":
            from .distributed import (
                build_distributed_trial_plan,
                ensure_frozen_plan,
                load_distributed_pilot_preregistration,
                trial_plan_payload,
            )

            preregistration = load_distributed_pilot_preregistration(
                args.preregistration
            )
            trials = build_distributed_trial_plan(preregistration)
            plan = trial_plan_payload(preregistration, trials)
            if args.output_dir is not None:
                ensure_frozen_plan(
                    Path(args.output_dir) / "distributed_pilot_plan.json",
                    plan,
                )
            summary = {
                key: value
                for key, value in plan.items()
                if key != "trials"
            }
            summary["preregistration"] = preregistration.to_public_dict()
            return _print_payload(summary, compact=args.compact)
        if args.command == "run-oed-certificate-replay":
            from .oed import run_oed_certificate_replay

            payload = run_oed_certificate_replay(
                args.oed_config,
                args.certificate_config,
                args.oracle_config,
                oracle_output_dir=args.oracle_output_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "calibrate-awm-certificate":
            from .awm import calibrate_awm_certificate

            payload = calibrate_awm_certificate(
                args.calibration_config,
                output_dir=args.output_dir,
                include_negative_control=not args.no_negative_control,
                simulations=args.simulations,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "certify-awm-restricted-policy":
            from .awm import certify_awm_restricted_policy

            payload = certify_awm_restricted_policy(
                args.certificate_config,
                args.oracle_config,
                oracle_output_dir=args.oracle_output_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "audit-awm-heterogeneity":
            from .awm import audit_awm_workload_heterogeneity

            payload = audit_awm_workload_heterogeneity(
                args.audit_config,
                args.oracle_config,
                oracle_output_dir=args.oracle_output_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "plan-reduced-oracle-recovery":
            from .integrations.flowmesh.pilot import (
                load_flowmesh_pilot_config,
            )
            from .reduced_oracle import (
                load_reduced_oracle_config,
                plan_reduced_oracle_recovery,
            )

            oracle_config = load_reduced_oracle_config(args.oracle_config)
            workload_pilot = load_flowmesh_pilot_config(
                oracle_config.workload_pilot_config_path
            )
            config = load_config(workload_pilot.system_config_path)
            payload = plan_reduced_oracle_recovery(
                config=oracle_config,
                system=config,
                incident_dir=args.incident_dir,
                recovery_dir=args.recovery_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-reduced-oracle-recovery":
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
            )
            from .reduced_oracle import (
                load_reduced_oracle_config,
                run_reduced_oracle_recovery,
            )

            oracle_config = load_reduced_oracle_config(args.oracle_config)
            workload_pilot = load_flowmesh_pilot_config(
                oracle_config.workload_pilot_config_path
            )
            config = load_config(workload_pilot.system_config_path)
            resolved_data_agent_url = (
                args.data_agent_url
                or os.getenv("PATHFINDER_DATA_AGENT_URL")
            )
            if not resolved_data_agent_url:
                raise ConfigError(
                    "run-reduced-oracle-recovery requires "
                    "--data-agent-url or PATHFINDER_DATA_AGENT_URL"
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
                SQLiteSessionStore(args.state_db),
                backend,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = run_reduced_oracle_recovery(
                    config=oracle_config,
                    system=config,
                    adapter=FlowMeshAgentAdapter(
                        client,
                        gateway,
                        settings,
                    ),
                    incident_dir=args.incident_dir,
                    recovery_dir=args.recovery_dir,
                    max_consecutive_infrastructure_failures=(
                        args.max_consecutive_infrastructure_failures
                    ),
                    max_attempts_per_trial=args.max_attempts_per_trial,
                    progress_callback=lambda event: print(
                        json.dumps(
                            {"status": "recovery_attempt_recorded", **event},
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
        if args.command == "run-reduced-oracle":
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
            )
            from .reduced_oracle import (
                load_reduced_oracle_config,
                run_reduced_oracle,
            )

            oracle_config = load_reduced_oracle_config(args.oracle_config)
            workload_pilot = load_flowmesh_pilot_config(
                oracle_config.workload_pilot_config_path
            )
            config = load_config(workload_pilot.system_config_path)
            resolved_data_agent_url = (
                args.data_agent_url
                or os.getenv("PATHFINDER_DATA_AGENT_URL")
            )
            if not resolved_data_agent_url:
                raise ConfigError(
                    "run-reduced-oracle requires --data-agent-url or "
                    "PATHFINDER_DATA_AGENT_URL"
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
                SQLiteSessionStore(args.state_db),
                backend,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = run_reduced_oracle(
                    config=oracle_config,
                    system=config,
                    adapter=FlowMeshAgentAdapter(
                        client,
                        gateway,
                        settings,
                    ),
                    output_dir=args.output_dir,
                    progress_callback=lambda event: print(
                        json.dumps(
                            {"status": "oracle_trial_recorded", **event},
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
                endpoint_registry=args.endpoint_registry,
            )
            return 0
    except (ConfigError, OSError, RuntimeError, ValueError) as exc:
        print(json.dumps({"status": "error", "message": str(exc)}))
        return 2

    return _print_payload(
        payload,
        compact=getattr(args, "compact", False),
    )
