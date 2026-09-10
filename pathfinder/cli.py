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


def _node_api_url_mapping(values: Sequence[str]) -> dict[str, str]:
    """Parse repeatable ``NODE_ID=URL`` command-line bindings.

    A mapping is used instead of a positional URL list so a local reverse
    tunnel cannot accidentally be associated with a different simulated node.
    This function validates only the unambiguous command-line shape; the
    container-DAG planner performs the stricter URL and operation checks.
    """

    bindings: dict[str, str] = {}
    for raw in values:
        node_id, separator, url = raw.partition("=")
        if not separator or not node_id.strip() or not url.strip():
            raise ConfigError(
                "--node-api-url must have the form NODE_ID=http://host:port"
            )
        if node_id.strip() in bindings:
            raise ConfigError(
                f"duplicate --node-api-url binding for {node_id.strip()}"
            )
        bindings[node_id.strip()] = url.strip()
    if not bindings:
        raise ConfigError("at least one --node-api-url binding is required")
    return bindings


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pathfinder",
        description="Minimal Pathfinder causal access-response harness",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    evaluation = subcommands.add_parser(
        "evaluate-distributed-pilot",
        help="audit and summarize a complete frozen workload pilot offline",
    )
    for flag in ("run-dir", "preregistration", "endpoint-registry",
                 "workload-manifest", "measurement-manifest", "output-dir"):
        evaluation.add_argument("--" + flag, type=Path, required=True)
    policy_awm = subcommands.add_parser(
        "audit-distributed-policy-awm",
        help=(
            "audit observed safe/candidate distributed policies offline "
            "without inventing a complete design Oracle"
        ),
    )
    for flag in (
        "evaluation-dir",
        "preregistration",
        "audit-config",
        "output-dir",
    ):
        policy_awm.add_argument("--" + flag, type=Path, required=True)
    example = subcommands.add_parser(
        "create-workload-evaluation-example",
        help="create a deterministic synthetic format example, never a live run",
    )
    example.add_argument("--output-dir", type=Path, required=True)
    example.add_argument(
        "--success-scoring-rule",
        choices=(
            "accepted-answer-substring-match",
            "multiple-choice-option-id-exact-match-v1",
        ),
        default="accepted-answer-substring-match",
    )

    cohort = subcommands.add_parser(
        "prepare-benchmark-cohort",
        help=(
            "construct an outcome-blind exact-match workload cohort from "
            "a pinned NExT-QA annotation CSV"
        ),
    )
    cohort.add_argument("--selection-config", type=Path, required=True)
    cohort.add_argument("--annotation-csv", type=Path, required=True)
    cohort.add_argument("--output-dir", type=Path, required=True)
    cohort.add_argument("--compact", action="store_true")

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
        "--config",
        type=Path,
        help=(
            "system configuration whose representation IDs define the "
            "complete routing matrix; required when candidate designs use "
            "exact per-representation routes instead of a wildcard, and "
            "bound by an execution amendment when one is supplied"
        ),
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
    pilot_preflight.add_argument(
        "--execution-amendment",
        type=Path,
        help=(
            "compatibility evidence permitting execution at a revision "
            "other than the preregistered protocol revision"
        ),
    )
    pilot_preflight.add_argument(
        "--workload-manifest",
        type=Path,
        help="workload definitions, required to check an amendment",
    )
    pilot_preflight.add_argument(
        "--frozen-plan",
        type=Path,
        help="frozen plan document, required to check an amendment",
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
        help=(
            "write the frozen plan document here; requires "
            "--workload-manifest so the plan is bound to real workload "
            "content and stays resumable"
        ),
    )
    pilot_plan.add_argument(
        "--workload-manifest",
        type=Path,
        help=(
            "workload definitions to bind into the written plan; required "
            "with --output-dir"
        ),
    )
    pilot_plan.add_argument(
        "--endpoint-registry",
        type=Path,
        help=(
            "endpoint registry to bind into the written plan; required "
            "with --output-dir so the document matches the one execution "
            "recomputes"
        ),
    )
    pilot_plan.add_argument("--compact", action="store_true")

    run_pilot = subcommands.add_parser(
        "run-distributed-pilot",
        help=(
            "execute the frozen distributed plan through the real FlowMesh "
            "adapter; starts no worker, Data Agent, or MCP service"
        ),
    )
    confirm_plan = subcommands.add_parser(
        "freeze-distributed-policy-confirmation",
        help=(
            "freeze a prospective confirmation plan for one post-hoc "
            "selected distributed policy (offline; authorises no commit)"
        ),
    )
    confirm_plan.add_argument("--config", type=Path, required=True)
    confirm_plan.add_argument("--policy-audit-dir", type=Path, required=True)
    confirm_plan.add_argument(
        "--inspected-workload-manifest",
        type=Path,
        action="append",
        required=True,
        help="repeatable; every already-inspected workload manifest",
    )
    confirm_plan.add_argument(
        "--fresh-cohort-manifest",
        type=Path,
        required=True,
    )
    confirm_plan.add_argument("--output-dir", type=Path, required=True)
    confirm_plan.add_argument("--compact", action="store_true")

    frame_bundles = subcommands.add_parser(
        "build-frame-bundles",
        help=(
            "deterministically regenerate JPEG frame bundles aligned with "
            "the frozen sampling metadata (offline; no LLM, no network)"
        ),
    )
    frame_bundles.add_argument("--video-dir", type=Path, required=True)
    frame_bundles.add_argument(
        "--representation-dir", type=Path, required=True
    )
    frame_bundles.add_argument("--generation-manifest", type=Path)
    frame_bundles.add_argument("--output-dir", type=Path, required=True)
    frame_bundles.add_argument(
        "--object-id",
        action="append",
        help="repeatable; build only these objects",
    )
    frame_bundles.add_argument("--compact", action="store_true")

    bundle_smoke = subcommands.add_parser(
        "run-frame-bundle-transfer-smoke",
        help=(
            "download one sampled_frame_bundle from a Data Agent, validate "
            "the tar and manifest, reconcile transfer telemetry, and write "
            "a conformance report (no LLM; transfer evidence only)"
        ),
    )
    bundle_smoke.add_argument("--data-agent-url", required=True)
    bundle_smoke.add_argument("--object-id", required=True)
    bundle_smoke.add_argument("--plan-id", required=True)
    bundle_smoke.add_argument(
        "--location",
        required=True,
        help="requested binding.location for the access",
    )
    bundle_smoke.add_argument("--output-dir", type=Path, required=True)
    bundle_smoke.add_argument("--representation-id")
    bundle_smoke.add_argument("--task-class", default="video_qa")
    bundle_smoke.add_argument("--expected-sha256")
    bundle_smoke.add_argument("--expected-size-bytes", type=int)
    bundle_smoke.add_argument("--expected-catalog-version")
    bundle_smoke.add_argument("--access-id")
    bundle_smoke.add_argument("--session-id")
    bundle_smoke.add_argument("--trial-id")
    bundle_smoke.add_argument("--latency-multiplier", type=float, default=1.0)
    bundle_smoke.add_argument("--timeout", type=float, default=30.0)
    bundle_smoke.add_argument("--max-retries", type=int, default=0)
    bundle_smoke.add_argument(
        "--telemetry-quiescence-timeout", type=float, default=5.0
    )
    bundle_smoke.add_argument("--max-artifact-bytes", type=int)
    bundle_smoke.add_argument("--max-member-count", type=int)
    bundle_smoke.add_argument("--max-frame-count", type=int)
    bundle_smoke.add_argument("--max-frame-bytes", type=int)
    bundle_smoke.add_argument("--max-total-contained-bytes", type=int)
    bundle_smoke.add_argument("--max-manifest-bytes", type=int)
    bundle_smoke.add_argument("--max-frame-dimension", type=int)
    bundle_smoke.add_argument(
        "--retain-artifact",
        action="store_true",
        help="also write the verified tar into the output directory",
    )
    bundle_smoke.add_argument("--compact", action="store_true")

    cost_audit = subcommands.add_parser(
        "audit-distributed-cost-reality",
        help=(
            "read-only post-hoc audit separating measured resources from "
            "configured tariffs in a frozen distributed pilot"
        ),
    )
    cost_audit.add_argument("--snapshot-dir", type=Path, required=True)
    cost_audit.add_argument("--output-dir", type=Path, required=True)
    cost_audit.add_argument("--compact", action="store_true")

    certify_confirm = subcommands.add_parser(
        "certify-distributed-policy-confirmation",
        help=(
            "evaluate a completed confirmation run against its frozen plan "
            "using the weighted stratified certificate (offline)"
        ),
    )
    certify_confirm.add_argument("--plan-dir", type=Path, required=True)
    certify_confirm.add_argument("--evidence-dir", type=Path, required=True)
    certify_confirm.add_argument(
        "--execution-evidence",
        type=Path,
        required=True,
        help="manifest binding the run to its plan and runtime model",
    )
    certify_confirm.add_argument("--output-dir", type=Path, required=True)
    certify_confirm.add_argument("--compact", action="store_true")

    oed_plan = subcommands.add_parser(
        "plan-distributed-policy-oed",
        help=(
            "offline OED planning: allocate future independent workload "
            "blocks for a confirmation cohort (never emits COMMIT)"
        ),
    )
    oed_plan.add_argument("--policy-audit-dir", type=Path, required=True)
    oed_plan.add_argument("--policy-id", required=True)
    oed_plan.add_argument(
        "--target-stratum-weight",
        action="append",
        required=True,
        metavar="STRATUM=INTEGER",
        help=(
            "repeatable; integer target quotas defining the fixed "
            "externally weighted stratified estimand, e.g. causal=14"
        ),
    )
    oed_plan.add_argument("--repetitions", type=int, default=2)
    oed_plan.add_argument(
        "--minimum-independent-workloads",
        action="append",
        metavar="STRATUM=INTEGER",
        help=(
            "repeatable; frozen per-active-stratum floor treated as a "
            "feasibility constraint before optimising precision"
        ),
    )
    oed_plan.add_argument(
        "--active-evidence-block-budget",
        type=int,
        help=(
            "number of paired safe/candidate workload blocks in ACTIVE "
            "strata; not a benchmark cohort size"
        ),
    )
    oed_plan.add_argument("--total-sessions", type=int)
    oed_plan.add_argument("--target-gate-width", type=float)
    oed_plan.add_argument("--plan-id", default="distributed-policy-oed-plan")
    oed_plan.add_argument("--output-dir", type=Path, required=True)
    oed_plan.add_argument("--compact", action="store_true")

    amendment = subcommands.add_parser(
        "create-distributed-execution-amendment",
        help=(
            "record auditable evidence that the current implementation "
            "revision may execute a pilot frozen at an earlier revision"
        ),
    )
    amendment.add_argument("--preregistration", type=Path, required=True)
    amendment.add_argument("--endpoint-registry", type=Path, required=True)
    amendment.add_argument("--measurement-manifest", type=Path, required=True)
    amendment.add_argument("--workload-manifest", type=Path, required=True)
    amendment.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    amendment.add_argument("--frozen-plan", type=Path, required=True)
    amendment.add_argument(
        "--input-freeze-dir",
        type=Path,
        required=True,
        help=(
            "the immutable input freeze; the amendment must be written "
            "outside it"
        ),
    )
    amendment.add_argument("--amendment-id", required=True)
    amendment.add_argument(
        "--reason",
        required=True,
        help="why this revision differs and why it is orchestration-only",
    )
    amendment.add_argument(
        "--change-classification",
        default="orchestration-only",
        help=(
            "only orchestration-only is accepted; anything else requires a "
            "new freeze"
        ),
    )
    amendment.add_argument(
        "--output",
        type=Path,
        required=True,
        help="write the amendment here; must be outside the input freeze",
    )
    amendment.add_argument("--compact", action="store_true")

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
    run_pilot.add_argument(
        "--execution-amendment",
        type=Path,
        help=(
            "compatibility evidence permitting execution at a revision "
            "other than the preregistered protocol revision"
        ),
    )
    run_pilot.add_argument(
        "--frozen-plan",
        type=Path,
        help=(
            "frozen plan document an execution amendment is validated "
            "against; defaults to the plan in --output-dir"
        ),
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
    simulator = subcommands.add_parser(
        "simulate-flowmesh-infra",
        help=(
            "run a deterministic offline FlowMesh physical-layout and "
            "infrastructure scenario"
        ),
    )
    simulator.add_argument("--scenario", type=Path, required=True)
    simulator.add_argument("--output-dir", type=Path, required=True)
    simulator.add_argument("--compact", action="store_true")

    verify_simulator = subcommands.add_parser(
        "verify-flowmesh-infra-simulation",
        help="verify the checksums and completion state of a simulator run",
    )
    verify_simulator.add_argument("--output-dir", type=Path, required=True)
    verify_simulator.add_argument("--compact", action="store_true")

    trace_import = subcommands.add_parser(
        "import-flowmesh-infra-trace",
        help=(
            "normalize frozen FlowMesh records into privacy-minimized "
            "simulator calibration observations"
        ),
    )
    trace_import.add_argument("--records", type=Path, required=True)
    trace_import.add_argument("--output-dir", type=Path, required=True)
    trace_import.add_argument("--compact", action="store_true")

    verify_trace_import = subcommands.add_parser(
        "verify-flowmesh-infra-trace-import",
        help="verify a published FlowMesh simulator trace import",
    )
    verify_trace_import.add_argument("--output-dir", type=Path, required=True)
    verify_trace_import.add_argument("--compact", action="store_true")

    calibrate_simulator = subcommands.add_parser(
        "calibrate-flowmesh-infra-scenario",
        help=(
            "produce an evidence-bound partially calibrated simulator "
            "scenario without inferring unmeasured parameters"
        ),
    )
    for flag in (
        "scenario",
        "calibration-config",
        "workload-manifest",
        "representation-manifest",
        "frame-bundle-root",
        "video-root",
        "output-dir",
    ):
        calibrate_simulator.add_argument("--" + flag, type=Path, required=True)
    calibrate_simulator.add_argument("--retrieval-output-dir", type=Path)
    calibrate_simulator.add_argument("--compact", action="store_true")

    verify_calibration = subcommands.add_parser(
        "verify-flowmesh-infra-calibration",
        help="verify a published simulator calibration",
    )
    verify_calibration.add_argument("--output-dir", type=Path, required=True)
    verify_calibration.add_argument("--compact", action="store_true")

    retrieval = subcommands.add_parser(
        "build-simulator-retrieval-cohort",
        help=(
            "build a content-bound W4 cohort, deterministic lexical index, "
            "and retrieval-quality evaluation"
        ),
    )
    retrieval.add_argument("--config", type=Path, required=True)
    retrieval.add_argument(
        "--representation-manifest",
        type=Path,
        required=True,
    )
    retrieval.add_argument("--answer-observations", type=Path)
    retrieval.add_argument("--output-dir", type=Path, required=True)
    retrieval.add_argument("--compact", action="store_true")

    verify_retrieval = subcommands.add_parser(
        "verify-simulator-retrieval-cohort",
        help="verify an immutable W4 retrieval/index evaluation",
    )
    verify_retrieval.add_argument("--output-dir", type=Path, required=True)
    verify_retrieval.add_argument("--compact", action="store_true")

    evidence = subcommands.add_parser(
        "build-flowmesh-infra-evidence",
        help=(
            "normalize fio, iperf3, model timing, and FlowMesh trace "
            "measurements into one immutable evidence bundle"
        ),
    )
    evidence.add_argument("--spec", type=Path, required=True)
    evidence.add_argument("--output-dir", type=Path, required=True)
    evidence.add_argument("--compact", action="store_true")

    verify_evidence = subcommands.add_parser(
        "verify-flowmesh-infra-evidence",
        help="verify a unified infrastructure evidence bundle",
    )
    verify_evidence.add_argument("--output-dir", type=Path, required=True)
    verify_evidence.add_argument("--compact", action="store_true")

    fitter = subcommands.add_parser(
        "fit-flowmesh-infra-scenario",
        help=(
            "fit directly identified storage, network, and compute "
            "parameters into a new simulator scenario"
        ),
    )
    fitter.add_argument("--scenario", type=Path, required=True)
    fitter.add_argument("--evidence-dir", type=Path, required=True)
    fitter.add_argument("--output-scenario-id", required=True)
    fitter.add_argument("--output-dir", type=Path, required=True)
    fitter.add_argument("--compact", action="store_true")

    verify_fit = subcommands.add_parser(
        "verify-flowmesh-infra-fit",
        help="verify an evidence-fitted simulator scenario",
    )
    verify_fit.add_argument("--output-dir", type=Path, required=True)
    verify_fit.add_argument("--compact", action="store_true")

    portable = subcommands.add_parser(
        "build-portable-execution-plan",
        help=(
            "compile a simulator scenario into a backend-neutral trial and "
            "operation contract without executing it"
        ),
    )
    portable.add_argument("--scenario", type=Path, required=True)
    portable.add_argument("--output-dir", type=Path, required=True)
    portable.add_argument("--compact", action="store_true")

    verify_portable = subcommands.add_parser(
        "verify-portable-execution-plan",
        help="verify an immutable backend-neutral execution plan",
    )
    verify_portable.add_argument("--output-dir", type=Path, required=True)
    verify_portable.add_argument("--compact", action="store_true")

    container_plan = subcommands.add_parser(
        "plan-container-simulation",
        help=(
            "bind a portable plan to exact container nodes, resources, links, "
            "caches, and operation adapters without launching Docker"
        ),
    )
    container_plan.add_argument("--scenario", type=Path, required=True)
    container_plan.add_argument(
        "--portable-plan-dir",
        type=Path,
        required=True,
    )
    container_plan.add_argument(
        "--container-spec",
        type=Path,
        required=True,
    )
    container_plan.add_argument("--output-dir", type=Path, required=True)
    container_plan.add_argument("--compact", action="store_true")

    verify_container = subcommands.add_parser(
        "verify-container-simulation-plan",
        help="verify a non-launching container-emulation execution contract",
    )
    verify_container.add_argument("--output-dir", type=Path, required=True)
    verify_container.add_argument("--compact", action="store_true")

    local_compose = subcommands.add_parser(
        "build-local-container-compose",
        help=(
            "generate an eight-node Docker Compose project from a validated "
            "container plan without invoking Docker"
        ),
    )
    local_compose.add_argument(
        "--container-plan-dir",
        type=Path,
        required=True,
    )
    local_compose.add_argument("--output-dir", type=Path, required=True)
    local_compose.add_argument("--host-port-base", type=int, default=19080)
    local_compose.add_argument(
        "--semantic-executor-node",
        help=(
            "enable a credential-free-recording OpenAI-compatible semantic "
            "executor only on this container node; credentials remain runtime "
            "environment variables"
        ),
    )
    local_compose.add_argument(
        "--semantic-artifact-source-node",
        action="append",
        help=(
            "repeat a node ID whose container may read frozen text "
            "representations through the route-coupled semantic path"
        ),
    )
    local_compose.add_argument("--compact", action="store_true")

    verify_local_compose = subcommands.add_parser(
        "verify-local-container-compose",
        help="verify a generated local Compose project without calling Docker",
    )
    verify_local_compose.add_argument("--output-dir", type=Path, required=True)
    verify_local_compose.add_argument("--compact", action="store_true")

    local_preflight = subcommands.add_parser(
        "preflight-local-container-host",
        help="read-only check for a usable Docker engine and Compose v2",
    )
    local_preflight.add_argument("--compact", action="store_true")

    container_node = subcommands.add_parser(
        "serve-container-node",
        help="serve one bounded container node; semantic LLM support is opt-in",
    )
    container_node.add_argument("--node-id", required=True)
    container_node.add_argument(
        "--state-dir",
        type=Path,
        default=Path("/tmp/pathfinder-node"),
    )
    container_node.add_argument("--host", default="0.0.0.0")
    container_node.add_argument("--port", type=int, default=9080)
    container_node.add_argument(
        "--max-operation-bytes",
        type=int,
        default=1024 * 1024 * 1024,
    )
    container_node.add_argument(
        "--enable-semantic-llm",
        action="store_true",
        help=(
            "expose the bounded semantic completion endpoint; it reads model "
            "configuration and credentials only from runtime environment"
        ),
    )
    container_node.add_argument(
        "--semantic-artifact-root",
        type=Path,
        help=(
            "read-only in-container root for bounded UTF-8 semantic "
            "representations"
        ),
    )
    container_node.add_argument(
        "--semantic-allowed-source-container",
        action="append",
        help=(
            "repeat one Docker service name this semantic executor may fetch "
            "a representation from"
        ),
    )

    container_run = subcommands.add_parser(
        "run-local-container-simulation",
        help=(
            "execute serial or capacity-gated concurrent infrastructure trials "
            "against already-running local container nodes"
        ),
    )
    container_run.add_argument("--compose-package-dir", type=Path, required=True)
    container_run.add_argument("--portable-plan-dir", type=Path, required=True)
    container_run.add_argument("--output-dir", type=Path, required=True)
    container_run.add_argument("--trial-limit", type=int)
    container_run.add_argument(
        "--trial-key",
        action="append",
        help=(
            "select an exact frozen trial; repeat to run an auditable subset "
            "concurrently"
        ),
    )
    container_run.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help=(
            "maximum concurrent trials; defaults to the portable plan's "
            "frozen trial-admission slots, cannot exceed them, and must equal "
            "them for a complete run"
        ),
    )
    container_run.add_argument(
        "--request-timeout",
        type=float,
        default=900.0,
    )
    container_run.add_argument("--compact", action="store_true")

    semantic_container_run = subcommands.add_parser(
        "run-local-container-semantic-execution",
        help=(
            "run bounded real text representations through an enabled local "
            "container LLM executor and score frozen single-option answers"
        ),
    )
    semantic_container_run.add_argument(
        "--compose-package-dir", type=Path, required=True
    )
    semantic_container_run.add_argument(
        "--semantic-workload-manifest", type=Path, required=True
    )
    semantic_container_run.add_argument(
        "--representation-root", type=Path, required=True
    )
    semantic_container_run.add_argument("--output-dir", type=Path, required=True)
    semantic_container_run.add_argument("--request-timeout", type=float, default=240.0)
    semantic_container_run.add_argument(
        "--max-representation-bytes", type=int, default=1024 * 1024
    )
    semantic_container_run.add_argument("--compact", action="store_true")

    verify_semantic_container_run = subcommands.add_parser(
        "verify-local-container-semantic-execution",
        help="verify a local semantic execution ledger without API access",
    )
    verify_semantic_container_run.add_argument(
        "--output-dir", type=Path, required=True
    )
    verify_semantic_container_run.add_argument("--compact", action="store_true")

    align_semantic_scores = subcommands.add_parser(
        "align-local-container-semantic-scores",
        help=(
            "derive a separately checksummed canonical-option score from a "
            "legacy exact-match semantic smoke without calling an LLM"
        ),
    )
    align_semantic_scores.add_argument(
        "--semantic-output-dir", type=Path, required=True
    )
    align_semantic_scores.add_argument(
        "--canonical-workload-manifest", type=Path, required=True
    )
    align_semantic_scores.add_argument("--output-dir", type=Path, required=True)
    align_semantic_scores.add_argument("--compact", action="store_true")

    verify_semantic_score_alignment = subcommands.add_parser(
        "verify-local-container-semantic-score-alignment",
        help="verify a local semantic score-alignment artifact without API access",
    )
    verify_semantic_score_alignment.add_argument(
        "--output-dir", type=Path, required=True
    )
    verify_semantic_score_alignment.add_argument("--compact", action="store_true")

    verify_container_run = subcommands.add_parser(
        "verify-local-container-simulation",
        help="verify an infrastructure-only container execution ledger offline",
    )
    verify_container_run.add_argument("--output-dir", type=Path, required=True)
    verify_container_run.add_argument("--compact", action="store_true")

    flowmesh_container_dag_candidates = subcommands.add_parser(
        "list-flowmesh-container-dag-candidates",
        help=(
            "list exact frozen read-transfer-compute chains that are safe "
            "to pin as a small FlowMesh container DAG"
        ),
    )
    flowmesh_container_dag_candidates.add_argument(
        "--container-operations", type=Path, required=True
    )
    flowmesh_container_dag_candidates.add_argument(
        "--compact", action="store_true"
    )

    flowmesh_container_dag_plan = subcommands.add_parser(
        "plan-flowmesh-container-dag",
        help=(
            "freeze one existing storage-read, network-transfer, compute "
            "container chain as a non-submitting FlowMesh DAG input package"
        ),
    )
    flowmesh_container_dag_plan.add_argument(
        "--container-operations", type=Path, required=True
    )
    flowmesh_container_dag_plan.add_argument(
        "--node-api-url",
        action="append",
        required=True,
        help=(
            "repeat NODE_ID=http://host:port for every selected execution "
            "node; use a separate reverse tunnel for each remote local node"
        ),
    )
    flowmesh_container_dag_plan.add_argument("--worker-alias", required=True)
    flowmesh_container_dag_plan.add_argument("--smoke-id", required=True)
    flowmesh_container_dag_plan.add_argument("--trial-key")
    flowmesh_container_dag_plan.add_argument("--owner", default="pathfinder")
    flowmesh_container_dag_plan.add_argument(
        "--output-dir", type=Path, required=True
    )
    flowmesh_container_dag_plan.add_argument("--compact", action="store_true")

    verify_flowmesh_container_dag_plan = subcommands.add_parser(
        "verify-flowmesh-container-dag-plan",
        help="verify a frozen non-submitting FlowMesh container-DAG package",
    )
    verify_flowmesh_container_dag_plan.add_argument(
        "--plan-dir", type=Path, required=True
    )
    verify_flowmesh_container_dag_plan.add_argument(
        "--compact", action="store_true"
    )

    flowmesh_container_dag_run = subcommands.add_parser(
        "run-flowmesh-container-dag",
        help=(
            "validate, submit, and verify one three-node FlowMesh "
            "container-operation DAG against already-running services"
        ),
    )
    flowmesh_container_dag_run.add_argument("--plan-dir", type=Path, required=True)
    flowmesh_container_dag_run.add_argument("--output-dir", type=Path, required=True)
    flowmesh_container_dag_run.add_argument("--worker-alias", required=True)
    flowmesh_container_dag_run.add_argument("--flowmesh-base-url")
    flowmesh_container_dag_run.add_argument(
        "--task-timeout", type=int, default=600
    )
    flowmesh_container_dag_run.add_argument(
        "--poll-interval", type=float, default=2.0
    )
    flowmesh_container_dag_run.add_argument("--compact", action="store_true")

    parity = subcommands.add_parser(
        "evaluate-backend-parity",
        help=(
            "descriptively compare two complete backend ledgers under one "
            "portable plan without making an unregistered parity claim"
        ),
    )
    parity.add_argument("--portable-plan-dir", type=Path, required=True)
    parity.add_argument("--reference-records", type=Path, required=True)
    parity.add_argument("--candidate-records", type=Path, required=True)
    parity.add_argument("--reference-label", required=True)
    parity.add_argument("--candidate-label", required=True)
    parity.add_argument(
        "--comparison-scope",
        choices=("full", "infrastructure-only"),
        default="full",
        help=(
            "full requires literal task_success on both backends; "
            "infrastructure-only excludes semantic quality and compares only "
            "latency, bytes, and resource service/queue metrics"
        ),
    )
    parity.add_argument("--output-dir", type=Path, required=True)
    parity.add_argument("--compact", action="store_true")

    verify_parity = subcommands.add_parser(
        "verify-backend-parity",
        help="verify an immutable descriptive backend-parity evaluation",
    )
    verify_parity.add_argument("--output-dir", type=Path, required=True)
    verify_parity.add_argument("--compact", action="store_true")

    container_calibration = subcommands.add_parser(
        "calibrate-container-backend",
        help=(
            "post-hoc fit only directly identifiable fixture-storage "
            "parameters from a bound container execution"
        ),
    )
    container_calibration.add_argument("--scenario", type=Path, required=True)
    container_calibration.add_argument(
        "--portable-plan-dir", type=Path, required=True
    )
    container_calibration.add_argument(
        "--reference-run-dir", type=Path, required=True
    )
    container_calibration.add_argument(
        "--container-run-dir", type=Path, required=True
    )
    container_calibration.add_argument("--output-scenario-id", required=True)
    container_calibration.add_argument("--output-dir", type=Path, required=True)
    container_calibration.add_argument("--compact", action="store_true")

    verify_container_calibration = subcommands.add_parser(
        "verify-container-backend-calibration",
        help="verify an immutable post-hoc container calibration",
    )
    verify_container_calibration.add_argument(
        "--output-dir", type=Path, required=True
    )
    verify_container_calibration.add_argument("--compact", action="store_true")
    return parser


def _stratum_integers(
    items: list[str] | None,
    option: str,
) -> dict[str, int] | None:
    """Parse repeatable STRATUM=INTEGER options."""
    if not items:
        return None
    parsed: dict[str, int] = {}
    for item in items:
        if "=" not in item:
            raise ConfigError(f"{option} must be STRATUM=INTEGER")
        key, _, value = item.partition("=")
        try:
            parsed[key.strip()] = int(value)
        except ValueError as exc:
            raise ConfigError(f"invalid {option} value: {item}") from exc
    return parsed


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
        if args.command == "simulate-flowmesh-infra":
            from .simulator import run_simulator_scenario

            payload = run_simulator_scenario(
                args.scenario,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-infra-simulation":
            from .simulator import verify_simulator_run

            payload = verify_simulator_run(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "import-flowmesh-infra-trace":
            from .simulator import import_flowmesh_trace

            payload = import_flowmesh_trace(
                args.records,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-infra-trace-import":
            from .simulator import verify_flowmesh_trace_import

            payload = verify_flowmesh_trace_import(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "calibrate-flowmesh-infra-scenario":
            from .simulator import calibrate_simulator_scenario

            payload = calibrate_simulator_scenario(
                args.scenario,
                args.calibration_config,
                workload_manifest_path=args.workload_manifest,
                representation_manifest_path=args.representation_manifest,
                frame_bundle_root=args.frame_bundle_root,
                video_root=args.video_root,
                output_dir=args.output_dir,
                retrieval_output_dir=args.retrieval_output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-infra-calibration":
            from .simulator import verify_simulator_calibration

            payload = verify_simulator_calibration(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-retrieval-cohort":
            from .simulator import build_simulator_retrieval_cohort

            payload = build_simulator_retrieval_cohort(
                args.config,
                args.representation_manifest,
                answer_observations_path=args.answer_observations,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-retrieval-cohort":
            from .simulator import verify_simulator_retrieval

            payload = verify_simulator_retrieval(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-flowmesh-infra-evidence":
            from .simulator import build_simulator_evidence_bundle

            payload = build_simulator_evidence_bundle(
                args.spec,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-infra-evidence":
            from .simulator import verify_simulator_evidence_bundle

            payload = verify_simulator_evidence_bundle(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "fit-flowmesh-infra-scenario":
            from .simulator import fit_simulator_scenario

            payload = fit_simulator_scenario(
                args.scenario,
                args.evidence_dir,
                output_scenario_id=args.output_scenario_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-infra-fit":
            from .simulator import verify_simulator_fit

            payload = verify_simulator_fit(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-portable-execution-plan":
            from .simulator import build_portable_execution_plan

            payload = build_portable_execution_plan(
                args.scenario,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-portable-execution-plan":
            from .simulator import verify_portable_execution_plan

            payload = verify_portable_execution_plan(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "plan-container-simulation":
            from .simulator import plan_container_backend

            payload = plan_container_backend(
                args.scenario,
                args.portable_plan_dir,
                args.container_spec,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-container-simulation-plan":
            from .simulator import verify_container_backend_plan

            payload = verify_container_backend_plan(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-local-container-compose":
            from .simulator import build_local_container_compose

            payload = build_local_container_compose(
                args.container_plan_dir,
                output_dir=args.output_dir,
                host_port_base=args.host_port_base,
                semantic_executor_node_id=args.semantic_executor_node,
                semantic_artifact_source_node_ids=(
                    args.semantic_artifact_source_node or ()
                ),
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-local-container-compose":
            from .simulator import verify_local_container_compose

            payload = verify_local_container_compose(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "preflight-local-container-host":
            from .simulator import preflight_local_container_host

            payload = preflight_local_container_host()
            return _print_payload(payload, compact=args.compact)
        if args.command == "serve-container-node":
            from .simulator import serve_container_node

            serve_container_node(
                args.node_id,
                args.state_dir,
                host=args.host,
                port=args.port,
                max_operation_bytes=args.max_operation_bytes,
                enable_semantic_llm=args.enable_semantic_llm,
                semantic_artifact_root=args.semantic_artifact_root,
                semantic_allowed_source_containers=tuple(
                    args.semantic_allowed_source_container or ()
                ),
            )
            return 0
        if args.command == "run-local-container-simulation":
            from .simulator import execute_local_container_plan

            payload = execute_local_container_plan(
                args.compose_package_dir,
                args.portable_plan_dir,
                output_dir=args.output_dir,
                trial_limit=args.trial_limit,
                trial_key=args.trial_key,
                max_concurrency=args.max_concurrency,
                request_timeout_seconds=args.request_timeout,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-local-container-semantic-execution":
            from .simulator import execute_local_container_semantic_run

            payload = execute_local_container_semantic_run(
                args.compose_package_dir,
                args.semantic_workload_manifest,
                args.representation_root,
                output_dir=args.output_dir,
                request_timeout_seconds=args.request_timeout,
                max_representation_bytes=args.max_representation_bytes,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-local-container-semantic-execution":
            from .simulator import verify_local_container_semantic_run

            payload = verify_local_container_semantic_run(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "align-local-container-semantic-scores":
            from .simulator import align_local_container_semantic_scores

            payload = align_local_container_semantic_scores(
                args.semantic_output_dir,
                args.canonical_workload_manifest,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-local-container-semantic-score-alignment":
            from .simulator import verify_local_container_semantic_score_alignment

            payload = verify_local_container_semantic_score_alignment(
                args.output_dir
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-local-container-simulation":
            from .simulator import verify_container_execution

            payload = verify_container_execution(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "list-flowmesh-container-dag-candidates":
            from .integrations.flowmesh.container_dag import (
                list_linear_container_operation_dag_candidates,
                load_container_operations,
            )

            candidates = list_linear_container_operation_dag_candidates(
                load_container_operations(args.container_operations)
            )
            payload = {
                "status": "COMPLETE",
                "candidate_count": len(candidates),
                "candidates": candidates,
                "workflow_submitted": False,
                "services_started": False,
                "eligible_for_scientific_claims": False,
            }
            return _print_payload(payload, compact=args.compact)
        if args.command == "plan-flowmesh-container-dag":
            from .integrations.flowmesh.container_dag import (
                plan_flowmesh_container_operation_dag,
            )

            payload = plan_flowmesh_container_operation_dag(
                container_operations_path=args.container_operations,
                node_api_urls=_node_api_url_mapping(args.node_api_url),
                worker_alias=args.worker_alias,
                smoke_id=args.smoke_id,
                trial_key=args.trial_key,
                owner=args.owner,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-dag-plan":
            from .integrations.flowmesh.container_dag import (
                verify_flowmesh_container_operation_dag_plan,
            )

            payload = verify_flowmesh_container_operation_dag_plan(
                args.plan_dir
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-flowmesh-container-dag":
            from .integrations.flowmesh import (
                FlowMeshSettings,
                SdkFlowMeshClient,
            )
            from .integrations.flowmesh.container_dag import (
                run_flowmesh_container_operation_dag,
            )

            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                task_timeout_seconds=args.task_timeout,
                poll_interval_seconds=args.poll_interval,
                worker_alias=args.worker_alias,
                validate_before_submit=True,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = run_flowmesh_container_operation_dag(
                    plan_dir=args.plan_dir,
                    output_dir=args.output_dir,
                    client=client,
                    settings=settings,
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if args.command == "evaluate-backend-parity":
            from .simulator import evaluate_backend_parity

            payload = evaluate_backend_parity(
                args.portable_plan_dir,
                args.reference_records,
                args.candidate_records,
                reference_label=args.reference_label,
                candidate_label=args.candidate_label,
                output_dir=args.output_dir,
                comparison_scope=args.comparison_scope,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-backend-parity":
            from .simulator import verify_backend_parity

            payload = verify_backend_parity(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "calibrate-container-backend":
            from .simulator import calibrate_container_backend

            payload = calibrate_container_backend(
                args.scenario,
                args.portable_plan_dir,
                args.reference_run_dir,
                args.container_run_dir,
                output_scenario_id=args.output_scenario_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-container-backend-calibration":
            from .simulator import verify_container_backend_calibration

            payload = verify_container_backend_calibration(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "evaluate-distributed-pilot":
            from .evaluation import evaluate_distributed_pilot

            result = evaluate_distributed_pilot(
                args.run_dir, preregistration=args.preregistration,
                endpoint_registry=args.endpoint_registry,
                workload_manifest=args.workload_manifest,
                measurement_manifest=args.measurement_manifest,
                output_dir=args.output_dir,
            )
            print(json.dumps({key: result[key] for key in (
                "status", "pilot_id", "input_origin", "planned_trials",
                "canonical_records", "independent_workloads", "attempt_records",
                "attempt_classes", "failure_classes", "paired_aggregates",
                "eligible_for_scientific_claims",
            )}, indent=2))
            return 0
        if args.command == "freeze-distributed-policy-confirmation":
            from .distributed import freeze_confirmation_plan

            payload = freeze_confirmation_plan(
                args.config,
                args.policy_audit_dir,
                inspected_workload_manifests=(
                    args.inspected_workload_manifest
                ),
                fresh_cohort_manifest=args.fresh_cohort_manifest,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-frame-bundles":
            from .frame_bundle import build_frame_bundles

            payload = build_frame_bundles(
                video_dir=args.video_dir,
                representation_dir=args.representation_dir,
                generation_manifest=args.generation_manifest,
                output_dir=args.output_dir,
                object_ids=args.object_id,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-frame-bundle-transfer-smoke":
            from dataclasses import replace as _replace

            from .frame_bundle import REPRESENTATION_ID
            from .frame_bundle_ingest import DEFAULT_FRAME_BUNDLE_LIMITS
            from .frame_bundle_transfer import (
                SMOKE_REPORT_NAME,
                run_frame_bundle_transfer_smoke,
            )

            overrides = {
                name: getattr(args, name)
                for name in (
                    "max_artifact_bytes",
                    "max_member_count",
                    "max_frame_count",
                    "max_frame_bytes",
                    "max_total_contained_bytes",
                    "max_manifest_bytes",
                    "max_frame_dimension",
                )
                if getattr(args, name) is not None
            }
            limits = (
                _replace(DEFAULT_FRAME_BUNDLE_LIMITS, **overrides)
                if overrides
                else DEFAULT_FRAME_BUNDLE_LIMITS
            )
            # The bearer token is read from PATHFINDER_DATA_AGENT_TOKEN
            # inside the client settings. It is deliberately not an option:
            # a token on the command line lands in shell history and in the
            # process table.
            report = run_frame_bundle_transfer_smoke(
                base_url=args.data_agent_url,
                object_id=args.object_id,
                plan_id=args.plan_id,
                requested_location=args.location,
                output_dir=args.output_dir,
                representation_id=(
                    args.representation_id or REPRESENTATION_ID
                ),
                task_class_id=args.task_class,
                access_id=args.access_id,
                session_id=args.session_id,
                trial_id=args.trial_id,
                latency_multiplier=args.latency_multiplier,
                expected_artifact_sha256=args.expected_sha256,
                expected_artifact_size_bytes=args.expected_size_bytes,
                expected_object_catalog_version=(
                    args.expected_catalog_version
                ),
                limits=limits,
                quiescence_timeout_seconds=(
                    args.telemetry_quiescence_timeout
                ),
                timeout_seconds=args.timeout,
                max_retries=args.max_retries,
                retain_artifact=args.retain_artifact,
            )
            # stdout carries a summary; the full canonical report, including
            # per-frame metadata, is the file on disk.
            payload = {
                "status": report["status"],
                "evidence_class": report["evidence_class"],
                "eligible_for_scientific_claims": False,
                "object_id": report["bundle"]["object_id"],
                "representation_id": report["bundle"]["representation_id"],
                "artifact": report["artifact"],
                "frame_count": report["bundle"]["frame_count"],
                "total_jpeg_bytes": report["bundle"]["total_jpeg_bytes"],
                "tar_member_count": report["bundle"]["tar_member_count"],
                "latency_ms": report["latency_ms"],
                "delivery": report["delivery"],
                "report_path": str(args.output_dir / SMOKE_REPORT_NAME),
                "retained_artifact": report["outputs"]["retained_artifact"],
            }
            return _print_payload(payload, compact=args.compact)
        if args.command == "audit-distributed-cost-reality":
            import subprocess

            from .distributed import audit_distributed_cost_reality

            try:
                revision = subprocess.run(
                    ("git", "rev-parse", "HEAD"),
                    capture_output=True, text=True, check=True,
                ).stdout.strip()
            except Exception:
                revision = None
            payload = audit_distributed_cost_reality(
                args.snapshot_dir,
                output_dir=args.output_dir,
                audit_git_revision=revision,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "certify-distributed-policy-confirmation":
            from .distributed import (
                certify_distributed_policy_confirmation,
            )

            payload = certify_distributed_policy_confirmation(
                args.plan_dir,
                args.evidence_dir,
                execution_evidence=args.execution_evidence,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "plan-distributed-policy-oed":
            from .distributed import plan_distributed_policy_oed

            weights: dict[str, int] = {}
            for item in args.target_stratum_weight:
                if "=" not in item:
                    raise ConfigError(
                        "--target-stratum-weight must be STRATUM=INTEGER"
                    )
                key, _, value = item.partition("=")
                try:
                    weights[key.strip()] = int(value)
                except ValueError as exc:
                    raise ConfigError(
                        "target stratum weights must be integer quotas, "
                        f"not rounded fractions: {item}"
                    ) from exc
            payload = plan_distributed_policy_oed(
                args.policy_audit_dir,
                policy_id=args.policy_id,
                stratum_weights=weights,
                output_dir=args.output_dir,
                repetitions=args.repetitions,
                active_evidence_block_budget=(
                    args.active_evidence_block_budget
                ),
                minimum_independent_workloads_by_active_stratum=(
                    _stratum_integers(
                        args.minimum_independent_workloads,
                        "--minimum-independent-workloads",
                    )
                ),
                total_sessions=args.total_sessions,
                target_gate_width=args.target_gate_width,
                plan_id=args.plan_id,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "audit-distributed-policy-awm":
            from .awm import audit_distributed_policy_awm

            result = audit_distributed_policy_awm(
                args.evaluation_dir,
                preregistration=args.preregistration,
                audit_config=args.audit_config,
                output_dir=args.output_dir,
            )
            print(json.dumps({key: result[key] for key in (
                "status",
                "audit_id",
                "pilot_id",
                "independent_workloads",
                "complete_design_oracle",
                "policy_summaries",
                "posthoc",
                "eligible_for_scientific_claims",
                "recommended_next_step",
            )}, indent=2))
            return 0
        if args.command == "create-workload-evaluation-example":
            from .evaluation.example import create_evaluation_example

            print(json.dumps(create_evaluation_example(
                args.output_dir,
                success_scoring_rule=args.success_scoring_rule,
            ), indent=2))
            return 0
        if args.command == "prepare-benchmark-cohort":
            from .benchmark_cohort import prepare_benchmark_cohort

            payload = prepare_benchmark_cohort(
                selection_config=args.selection_config,
                annotation_csv=args.annotation_csv,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
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
        if args.command == "create-distributed-execution-amendment":
            from .distributed import (
                build_execution_amendment,
                build_frozen_plan_document,
                default_revision_resolver,
                load_distributed_pilot_preregistration,
                load_endpoint_registry,
                load_measurement_manifest,
                require_outside_input_freeze,
            )

            target = Path(args.output)
            # Checked before anything is created or written.
            require_outside_input_freeze(target, args.input_freeze_dir)
            if target.exists():
                raise ConfigError(
                    f"refusing to overwrite an existing amendment: {target}"
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
            payload = build_execution_amendment(
                preregistration,
                registry,
                amendment_id=args.amendment_id,
                reason=args.reason,
                measurement_manifest_sha256=provider.manifest_sha256,
                workloads=workloads,
                system_config_path=args.config,
                frozen_plan_path=args.frozen_plan,
                recomputed_plan=build_frozen_plan_document(
                    preregistration,
                    registry,
                    workloads=workloads,
                ),
                resolver=default_revision_resolver(),
                change_classification=args.change_classification,
                input_freeze_dir=args.input_freeze_dir,
                output_path=target,
                provider=provider,
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            from hashlib import sha256 as _sha256

            return _print_payload(
                {
                    **payload,
                    "amendment_path": str(target.resolve()),
                    "amendment_sha256": _sha256(
                        target.read_bytes()
                    ).hexdigest(),
                },
                compact=args.compact,
            )
        if args.command == "run-distributed-pilot":
            from .distributed import (
                FlowMeshDistributedSessionExecutor,
                HttpDataAgentHealthProbe,
                build_frozen_plan_document,
                default_revision_resolver,
                load_distributed_pilot_preregistration,
                load_endpoint_registry,
                load_measurement_manifest,
                preflight_distributed_pilot,
                require_execution_compatibility,
                require_matching_measurement_manifest,
                run_distributed_pilot,
                validate_workload_manifest,
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
            validate_workload_manifest(
                workloads,
                preregistration.workload_ids,
                preregistration.success_scoring_rule,
            )
            config = load_config(args.config)

            # Bound before a backend, client, or run directory exists: a
            # foreign manifest must not be detected only after the run has
            # already written state.
            require_matching_measurement_manifest(
                provider,
                preregistration,
                registry,
            )

            # Resolved before any client, gateway, or session exists: a
            # revision mismatch must stop the run before FlowMesh is touched.
            frozen_plan_path = args.frozen_plan or (
                Path(args.output_dir) / "distributed_pilot_plan.json"
            )
            execution_provenance = require_execution_compatibility(
                preregistration,
                registry,
                resolver=default_revision_resolver(),
                measurement_manifest_sha256=provider.manifest_sha256,
                workloads=workloads,
                system_config_path=args.config,
                frozen_plan_path=(
                    frozen_plan_path
                    if Path(frozen_plan_path).is_file()
                    else None
                ),
                recomputed_plan=build_frozen_plan_document(
                    preregistration,
                    registry,
                    workloads=workloads,
                ),
                amendment_path=args.execution_amendment,
            )

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
                    representation_ids=tuple(config.representations),
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
                        FlowMeshAgentAdapter(client, gateway, settings),
                        success_scoring_rule=(
                            preregistration.success_scoring_rule
                        ),
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
                        execution_provenance=execution_provenance,
                        execution_amendment_path=args.execution_amendment,
                    )
                finally:
                    client.close()
            finally:
                close_routed_backend(backend)
            _print_payload(payload, compact=args.compact)
            # A PARTIAL run, an exhausted retry budget, or any incomplete
            # Oracle is a failure: exiting 0 would let a scheduler treat a
            # half-finished dataset as a finished one.
            complete = (
                payload.get("status") == "COMPLETE"
                and bool(payload.get("oracle_complete"))
            )
            return 0 if complete else 1
        if args.command == "preflight-distributed-pilot":
            from .distributed import (
                ExecutionAmendmentError,
                HttpDataAgentHealthProbe,
                build_frozen_plan_document,
                default_revision_resolver,
                load_distributed_pilot_preregistration,
                load_endpoint_registry,
                preflight_distributed_pilot,
                require_execution_compatibility,
                require_matching_measurement_manifest,
            )

            pin = None
            if args.worker_id:
                pin = {"kind": "worker_id", "value": args.worker_id}
            elif args.worker_alias:
                pin = {"kind": "worker_alias", "value": args.worker_alias}
            manifest_sha256 = None
            measurement_provider = None
            if args.measurement_manifest is not None:
                from .distributed import load_measurement_manifest

                measurement_provider = load_measurement_manifest(
                    args.measurement_manifest
                )
                manifest_sha256 = measurement_provider.manifest_sha256
            # Loaded once and shared: the probe must resolve endpoints from
            # the same registry the preflight is checking, and without a
            # probe every endpoint health check fails by construction.
            registry = load_endpoint_registry(args.endpoint_registry)
            representation_ids = (
                tuple(load_config(args.config).representations)
                if args.config is not None
                else ()
            )
            preregistration = load_distributed_pilot_preregistration(
                args.preregistration
            )
            if measurement_provider is not None:
                # Before any Data Agent is probed.
                require_matching_measurement_manifest(
                    measurement_provider,
                    preregistration,
                    registry,
                )
            amendment_workloads = None
            if args.workload_manifest is not None:
                amendment_workloads = json.loads(
                    Path(args.workload_manifest).read_text(encoding="utf-8")
                )
            # Preflight is read-only and must stay usable for inspection at
            # any revision, so a bare revision mismatch is reported rather
            # than raised. A *supplied* amendment is fully validated, and an
            # invalid one fails here -- before the operator reaches the run.
            try:
                amendment_provenance = require_execution_compatibility(
                    preregistration,
                    registry,
                    resolver=default_revision_resolver(),
                    measurement_manifest_sha256=manifest_sha256 or "",
                    workloads=amendment_workloads or {},
                    system_config_path=args.config,
                    frozen_plan_path=args.frozen_plan,
                    recomputed_plan=(
                        build_frozen_plan_document(
                            preregistration,
                            registry,
                            workloads=amendment_workloads,
                        )
                        if amendment_workloads is not None
                        else None
                    ),
                    amendment_path=args.execution_amendment,
                )
                amendment_problem = None
            except ExecutionAmendmentError as exc:
                # A supplied amendment that does not validate is always a
                # failure: the operator asserted compatibility and the
                # assertion is wrong. A bare revision mismatch with no
                # amendment is advisory offline and blocking for a live
                # pilot, which is the run this preflight gates.
                if args.execution_amendment is not None:
                    raise
                amendment_problem = str(exc)
                amendment_provenance = {
                    "protocol_git_revision": (
                        preregistration.source_git_revision
                    ),
                    "execution_amendment_required": True,
                    "execution_amendment_supplied": False,
                    "detail": amendment_problem,
                }
            payload = preflight_distributed_pilot(
                preregistration,
                registry,
                probe=HttpDataAgentHealthProbe(registry),
                representation_ids=representation_ids,
                worker_pin=pin,
                mode=args.mode,
                measurement_manifest_sha256=manifest_sha256,
            )
            payload = {
                **payload,
                "execution_provenance": amendment_provenance,
            }
            if amendment_problem is not None:
                live = args.mode == "live_pilot"
                check = {
                    "check_id": "execution.revision_matches_or_amended",
                    "passed": False,
                    "advisory": not live,
                    "detail": amendment_problem,
                }
                payload["checks"] = list(payload["checks"]) + [check]
                if live:
                    payload["failed_checks"] = list(
                        payload["failed_checks"]
                    ) + [check["check_id"]]
                    payload["status"] = "failed"
                else:
                    payload["advisory_warnings"] = list(
                        payload["advisory_warnings"]
                    ) + [check["check_id"]]
            _print_payload(payload, compact=args.compact)
            return 0 if payload["status"] == "ok" else 1
        if args.command == "plan-distributed-pilot":
            from .distributed import (
                build_distributed_trial_plan,
                build_frozen_plan_document,
                ensure_frozen_plan,
                load_distributed_pilot_preregistration,
                load_endpoint_registry,
                trial_plan_payload,
            )

            preregistration = load_distributed_pilot_preregistration(
                args.preregistration
            )
            if args.output_dir is not None:
                # A plan written with a null content hash, or without the
                # registry digest, looks resumable but is not: what
                # execution recomputes would differ and the run would refuse
                # to start on the very plan this command prepared.
                missing = [
                    name
                    for name, value in (
                        ("--workload-manifest", args.workload_manifest),
                        ("--endpoint-registry", args.endpoint_registry),
                    )
                    if value is None
                ]
                if missing:
                    raise ConfigError(
                        "plan-distributed-pilot --output-dir requires "
                        + " and ".join(missing)
                        + "; a plan missing either would block the pilot it "
                        "was meant to prepare. Omit --output-dir for an "
                        "inspection preview."
                    )
            workloads = None
            if args.workload_manifest is not None:
                workloads = json.loads(
                    Path(args.workload_manifest).read_text(encoding="utf-8")
                )
                if not isinstance(workloads, dict):
                    raise ConfigError(
                        "the workload manifest must map workload_id -> "
                        "workload"
                    )
            trials = build_distributed_trial_plan(preregistration)
            if args.output_dir is not None:
                plan = build_frozen_plan_document(
                    preregistration,
                    load_endpoint_registry(args.endpoint_registry),
                    workloads=workloads,
                    trials=trials,
                )
                ensure_frozen_plan(
                    Path(args.output_dir) / "distributed_pilot_plan.json",
                    plan,
                )
            else:
                plan = trial_plan_payload(
                    preregistration,
                    trials,
                    workloads=workloads,
                )
            summary = {
                key: value
                for key, value in plan.items()
                if key != "trials"
            }
            summary["preview_only"] = args.output_dir is None
            summary["workload_content_bound"] = (
                plan.get("workload_content_sha256") is not None
            )
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
