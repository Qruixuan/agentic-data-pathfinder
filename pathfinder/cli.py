from __future__ import annotations

import argparse
import json
import math
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

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


def _positive_finite_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return parsed


def _finite_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(parsed):
        raise argparse.ArgumentTypeError("must be a finite number")
    return parsed


def _unit_interval_float(value: str) -> float:
    parsed = _finite_float(value)
    if not 0.0 < parsed < 1.0:
        raise argparse.ArgumentTypeError("must be strictly between 0 and 1")
    return parsed


def _closed_unit_interval_float(value: str) -> float:
    parsed = _finite_float(value)
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return parsed


def _neutral_oed_selection_size(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if not 1 <= parsed <= 4:
        raise argparse.ArgumentTypeError("must be between 1 and 4")
    return parsed


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


def _cache_outcome_mapping(values: Sequence[str]) -> dict[str, str]:
    """Parse repeatable frozen cache lookup outcomes.

    These values are never a way to choose a branch freely: the conditional
    planner compares them against the cache snapshot recorded in the frozen
    container operation.  Parsing them here simply makes an operator-supplied
    expectation explicit and rejects ambiguous command lines early.
    """

    outcomes: dict[str, str] = {}
    for raw in values:
        operation_key, separator, outcome = raw.partition("=")
        if (
            not separator
            or not operation_key.strip()
            or outcome.strip() not in {"hit", "miss"}
        ):
            raise ConfigError(
                "--cache-outcome must have the form "
                "LOOKUP_OPERATION_KEY=hit|miss"
            )
        key = operation_key.strip()
        if key in outcomes:
            raise ConfigError(f"duplicate --cache-outcome binding for {key}")
        outcomes[key] = outcome.strip()
    return outcomes


def _load_strict_json_file(
    path: Path,
    *,
    label: str,
    expected_type: type,
) -> object:
    """Read operator JSON without silently collapsing ambiguous input."""

    source = path.resolve()
    if not source.is_file() or source.is_symlink():
        raise ConfigError(f"{label} is missing or unsafe")

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, child in pairs:
            if key in value:
                raise ConfigError(f"{label} repeats JSON key {key}")
            value[key] = child
        return value

    try:
        value = json.loads(
            source.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ConfigError(f"{label} contains invalid number {token}")
            ),
        )
    except ConfigError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError(f"{label} must be readable JSON") from exc
    if not isinstance(value, expected_type):
        expected = "object" if expected_type is dict else "array"
        raise ConfigError(f"{label} must be a JSON {expected}")
    return value


_N4_LIVE_GATE_SOURCE_FIELDS = {
    "live_receipt_bindings",
    "n4_publication_store_root",
    "rebound_artifact_binding_dir",
    "rebound_semantic_matrix_dir",
    "rebound_admission_dir",
}
_N4_LIVE_FRAME_BINDING_FIELDS = {"kind", "receipt_dir", "n5_plan"}
_N4_LIVE_DIGEST_BINDING_FIELDS = {
    "kind",
    "receipt_dir",
    "n5_digest_plan_dir",
    "source_video_path",
}


def _load_n4_live_gate_sources(path: Path | None) -> dict | None:
    """Load one strict operator-local live-gate source descriptor."""

    if path is None:
        return None
    source = path.resolve()
    if not source.is_file() or source.is_symlink():
        raise ConfigError("N4 live gate sources file is missing or unsafe")

    def unique(pairs: list[tuple[str, object]]) -> dict:
        value: dict = {}
        for key, child in pairs:
            if key in value:
                raise ConfigError(
                    f"N4 live gate sources repeat JSON key {key}"
                )
            value[key] = child
        return value

    try:
        value = json.loads(
            source.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ConfigError(
                    f"N4 live gate sources contain invalid number {token}"
                )
            ),
        )
    except ConfigError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError(
            "N4 live gate sources must be a readable JSON object"
        ) from exc
    if not isinstance(value, dict) or set(value) != _N4_LIVE_GATE_SOURCE_FIELDS:
        raise ConfigError(
            "N4 live gate sources must contain exactly "
            + ", ".join(sorted(_N4_LIVE_GATE_SOURCE_FIELDS))
        )

    def local_path(raw: object, name: str) -> Path:
        if not isinstance(raw, str) or not raw.strip():
            raise ConfigError(f"{name} must be a non-empty path string")
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = source.parent / candidate
        return candidate.resolve()

    raw_bindings = value["live_receipt_bindings"]
    if not isinstance(raw_bindings, list) or not raw_bindings:
        raise ConfigError(
            "live_receipt_bindings must be a non-empty JSON array"
        )
    bindings: list[dict] = []
    for index, raw in enumerate(raw_bindings):
        if not isinstance(raw, dict):
            raise ConfigError(
                f"live_receipt_bindings[{index}] must be an object"
            )
        kind = raw.get("kind")
        if kind == "frame_bundle":
            if set(raw) != _N4_LIVE_FRAME_BINDING_FIELDS:
                raise ConfigError(
                    f"live_receipt_bindings[{index}] frame fields changed"
                )
            if not isinstance(raw.get("n5_plan"), dict):
                raise ConfigError(
                    f"live_receipt_bindings[{index}].n5_plan must be an object"
                )
            bindings.append({
                "kind": kind,
                "receipt_dir": local_path(
                    raw["receipt_dir"],
                    f"live_receipt_bindings[{index}].receipt_dir",
                ),
                "n5_plan": raw["n5_plan"],
            })
        elif kind == "multimodal_digest":
            if set(raw) != _N4_LIVE_DIGEST_BINDING_FIELDS:
                raise ConfigError(
                    f"live_receipt_bindings[{index}] digest fields changed"
                )
            bindings.append({
                "kind": kind,
                "receipt_dir": local_path(
                    raw["receipt_dir"],
                    f"live_receipt_bindings[{index}].receipt_dir",
                ),
                "n5_digest_plan_dir": local_path(
                    raw["n5_digest_plan_dir"],
                    f"live_receipt_bindings[{index}].n5_digest_plan_dir",
                ),
                "source_video_path": local_path(
                    raw["source_video_path"],
                    f"live_receipt_bindings[{index}].source_video_path",
                ),
            })
        else:
            raise ConfigError(
                f"live_receipt_bindings[{index}].kind is unsupported"
            )
    return {
        "live_receipt_bindings": bindings,
        "n4_publication_store_root": local_path(
            value["n4_publication_store_root"],
            "n4_publication_store_root",
        ),
        "rebound_artifact_binding_dir": local_path(
            value["rebound_artifact_binding_dir"],
            "rebound_artifact_binding_dir",
        ),
        "rebound_semantic_matrix_dir": local_path(
            value["rebound_semantic_matrix_dir"],
            "rebound_semantic_matrix_dir",
        ),
        "rebound_admission_dir": local_path(
            value["rebound_admission_dir"],
            "rebound_admission_dir",
        ),
    }


@contextmanager
def _local_semantic_flowmesh_executor(
    local_semantic_admission_dir: Path,
    *,
    run_id: str,
    flowmesh_base_url: str | None,
    task_timeout_seconds: int,
    poll_interval_seconds: float,
) -> Iterator[object]:
    """Build the local semantic effect boundary without persisting secrets."""

    from .integrations.flowmesh import (
        FlowMeshSemanticTrialExecutor,
        FlowMeshSettings,
        SdkFlowMeshClient,
        full_flow_hmac_header_provider,
    )
    from .simulator.full_flow_local_semantic_admission import (
        load_full_flow_local_semantic_execution_inputs,
    )

    inputs = load_full_flow_local_semantic_execution_inputs(
        local_semantic_admission_dir
    )
    worker_pin = inputs.admission.get("worker_pin")
    if (
        not isinstance(worker_pin, dict)
        or worker_pin.get("kind") != "worker_alias"
        or not isinstance(worker_pin.get("value"), str)
        or not worker_pin["value"].strip()
    ):
        raise ConfigError(
            "local semantic admission requires one frozen worker-alias pin"
        )
    ingress_secret = os.getenv(
        "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"
    )
    if ingress_secret is None or not ingress_secret.strip():
        raise ConfigError(
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET is required at runtime"
        )
    settings = FlowMeshSettings.from_environment(
        base_url=flowmesh_base_url,
        task_timeout_seconds=task_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        worker_alias=worker_pin["value"],
        validate_before_submit=True,
    )
    client = SdkFlowMeshClient(settings)
    try:
        yield FlowMeshSemanticTrialExecutor(
            client=client,
            settings=settings,
            run_id=run_id,
            bound_trials=inputs.bound_trials,
            bound_stages=inputs.bound_stages,
            runtime_header_provider=full_flow_hmac_header_provider(
                ingress_secret
            ),
            api_task_timeout_seconds=task_timeout_seconds,
        )
    finally:
        client.close()


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
    data_agent.add_argument(
        "--require-token",
        action="store_true",
        help="refuse startup unless PATHFINDER_DATA_AGENT_TOKEN is set",
    )
    data_agent.add_argument(
        "--require-artifact-secret",
        action="store_true",
        help=(
            "refuse startup unless PATHFINDER_DATA_AGENT_ARTIFACT_SECRET "
            "is set"
        ),
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

    full_flow_data_plane = subcommands.add_parser(
        "build-full-flow-data-plane",
        help=(
            "freeze real frame bundles and semantic trial specs into a "
            "portable N4 Data Agent package"
        ),
    )
    full_flow_data_plane.add_argument(
        "--semantic-spec-artifact",
        action="append",
        nargs=2,
        metavar=("SEMANTIC_SPEC", "FRAME_BUNDLE"),
        required=True,
        help="repeat for each semantic spec and canonical frame bundle pair",
    )
    full_flow_data_plane.add_argument("--package-id", required=True)
    full_flow_data_plane.add_argument(
        "--output-dir", type=Path, required=True
    )
    full_flow_data_plane.add_argument("--compact", action="store_true")

    verify_full_flow_data_plane = subcommands.add_parser(
        "verify-full-flow-data-plane",
        help="verify a portable full-flow N4 Data Agent package offline",
    )
    verify_full_flow_data_plane.add_argument(
        "--output-dir", type=Path, required=True
    )
    verify_full_flow_data_plane.add_argument("--compact", action="store_true")

    full_flow_compose = subcommands.add_parser(
        "build-full-flow-compose-binding",
        help=(
            "bind a portable full-flow data plane to the eight-node "
            "Compose simulator without launching it"
        ),
    )
    full_flow_compose.add_argument(
        "--base-compose-package", type=Path, required=True
    )
    full_flow_compose.add_argument(
        "--data-plane-package", type=Path, required=True
    )
    full_flow_compose.add_argument(
        "--route-id", default="full-flow-n4-n7-n6-v1"
    )
    full_flow_compose.add_argument("--data-agent-plan-id")
    full_flow_compose.add_argument(
        "--data-agent-plan-epoch", type=int, default=0
    )
    full_flow_compose.add_argument("--output-dir", type=Path, required=True)
    full_flow_compose.add_argument("--compact", action="store_true")

    verify_full_flow_compose = subcommands.add_parser(
        "verify-full-flow-compose-binding",
        help="verify an eight-node full-flow Compose binding offline",
    )
    verify_full_flow_compose.add_argument(
        "--base-compose-package", type=Path, required=True
    )
    verify_full_flow_compose.add_argument(
        "--data-plane-package", type=Path, required=True
    )
    verify_full_flow_compose.add_argument(
        "--output-dir", type=Path, required=True
    )
    verify_full_flow_compose.add_argument("--compact", action="store_true")

    full_flow_deployment = subcommands.add_parser(
        "build-full-flow-deployment-binding",
        help=(
            "freeze the environment-specific N7 endpoint and FlowMesh "
            "worker binding separately from a logical full-flow trial"
        ),
    )
    full_flow_deployment.add_argument("--deployment-binding-id", required=True)
    full_flow_deployment.add_argument(
        "--coordinator-api-url", required=True
    )
    full_flow_deployment.add_argument("--worker-alias", required=True)
    full_flow_deployment.add_argument(
        "--api-task-timeout-seconds", type=int, required=True
    )
    full_flow_deployment.add_argument(
        "--output", type=Path, required=True
    )
    full_flow_deployment.add_argument("--compact", action="store_true")

    full_flow_plan = subcommands.add_parser(
        "plan-flowmesh-full-flow-trial",
        help=(
            "freeze one real-object N4 to N7 to N6 Pathfinder trial and a "
            "non-submittable FlowMesh template"
        ),
    )
    full_flow_plan.add_argument("--semantic-spec", type=Path, required=True)
    full_flow_plan.add_argument(
        "--data-plane-package", type=Path, required=True
    )
    full_flow_plan.add_argument(
        "--deployment-binding", type=Path, required=True
    )
    full_flow_plan.add_argument("--owner", default="pathfinder")
    full_flow_plan.add_argument("--output-dir", type=Path, required=True)
    full_flow_plan.add_argument("--compact", action="store_true")

    verify_full_flow_plan = subcommands.add_parser(
        "verify-flowmesh-full-flow-trial-plan",
        help="verify a frozen full-flow logical plan and deployment binding",
    )
    verify_full_flow_plan.add_argument("--plan-dir", type=Path, required=True)
    verify_full_flow_plan.add_argument("--compact", action="store_true")

    full_flow_run = subcommands.add_parser(
        "run-flowmesh-full-flow-trial",
        help=(
            "validate, submit, and collect one worker-pinned full-flow N7 "
            "trial against already-running services"
        ),
    )
    full_flow_run.add_argument("--plan-dir", type=Path, required=True)
    full_flow_run.add_argument("--output-dir", type=Path, required=True)
    full_flow_run.add_argument("--flowmesh-base-url")
    full_flow_run.add_argument("--task-timeout", type=int, default=1200)
    full_flow_run.add_argument(
        "--poll-interval", type=_positive_finite_float, default=2.0
    )
    full_flow_run.add_argument("--compact", action="store_true")

    verify_full_flow_run = subcommands.add_parser(
        "verify-flowmesh-full-flow-trial-run",
        help="verify a completed full-flow trial and its frozen plan offline",
    )
    verify_full_flow_run.add_argument("--run-dir", type=Path, required=True)
    verify_full_flow_run.add_argument("--plan-dir", type=Path, required=True)
    verify_full_flow_run.add_argument("--compact", action="store_true")

    full_flow_v2_request = subcommands.add_parser(
        "build-flowmesh-full-flow-public-request-v2",
        help=(
            "build a label-free N4 to N7 to N6 to N1 public trial "
            "request"
        ),
    )
    full_flow_v2_request.add_argument(
        "--public-task-binding", type=Path, required=True
    )
    full_flow_v2_request.add_argument("--oracle-id", required=True)
    full_flow_v2_request.add_argument("--full-flow-request-id", required=True)
    full_flow_v2_request.add_argument("--run-id", required=True)
    full_flow_v2_request.add_argument("--trial-id", required=True)
    full_flow_v2_request.add_argument("--trial-key", required=True)
    full_flow_v2_request.add_argument("--route-id", required=True)
    full_flow_v2_request.add_argument("--requested-location", required=True)
    full_flow_v2_request.add_argument("--data-agent-plan-id", required=True)
    full_flow_v2_request.add_argument(
        "--data-agent-plan-epoch", type=int, default=0
    )
    full_flow_v2_request.add_argument(
        "--quiescence-timeout-seconds",
        type=_positive_finite_float,
        default=5.0,
    )
    full_flow_v2_request.add_argument("--artifact-sha256", required=True)
    full_flow_v2_request.add_argument(
        "--artifact-size-bytes", type=int, required=True
    )
    full_flow_v2_request.add_argument(
        "--object-catalog-version", required=True
    )
    full_flow_v2_request.add_argument("--expected-model", required=True)
    full_flow_v2_request.add_argument("--output", type=Path, required=True)
    full_flow_v2_request.add_argument("--compact", action="store_true")

    full_flow_v2_plan = subcommands.add_parser(
        "plan-flowmesh-full-flow-trial-v2",
        help="freeze a label-free N4 to N7 to N6 to N1 trial",
    )
    full_flow_v2_plan.add_argument(
        "--public-request", type=Path, required=True
    )
    full_flow_v2_plan.add_argument(
        "--data-plane-package", type=Path, required=True
    )
    full_flow_v2_plan.add_argument(
        "--deployment-binding", type=Path, required=True
    )
    full_flow_v2_plan.add_argument("--route-id", required=True)
    full_flow_v2_plan.add_argument("--requested-location", required=True)
    full_flow_v2_plan.add_argument("--data-agent-plan-id", required=True)
    full_flow_v2_plan.add_argument(
        "--data-agent-plan-epoch", type=int, default=0
    )
    full_flow_v2_plan.add_argument(
        "--quiescence-timeout-seconds",
        type=_positive_finite_float,
        default=5.0,
    )
    full_flow_v2_plan.add_argument("--owner", default="pathfinder")
    full_flow_v2_plan.add_argument("--output-dir", type=Path, required=True)
    full_flow_v2_plan.add_argument("--compact", action="store_true")

    full_flow_v2_plan_verify = subcommands.add_parser(
        "verify-flowmesh-full-flow-trial-v2-plan",
        help="verify a frozen label-free full-flow v2 plan offline",
    )
    full_flow_v2_plan_verify.add_argument(
        "--plan-dir", type=Path, required=True
    )
    full_flow_v2_plan_verify.add_argument("--compact", action="store_true")

    full_flow_v2_run = subcommands.add_parser(
        "run-flowmesh-full-flow-trial-v2",
        help=(
            "submit one worker-pinned label-free full-flow trial against "
            "already-running services"
        ),
    )
    full_flow_v2_run.add_argument("--plan-dir", type=Path, required=True)
    full_flow_v2_run.add_argument("--output-dir", type=Path, required=True)
    full_flow_v2_run.add_argument("--flowmesh-base-url")
    full_flow_v2_run.add_argument("--task-timeout", type=int, default=1200)
    full_flow_v2_run.add_argument(
        "--poll-interval", type=_positive_finite_float, default=2.0
    )
    full_flow_v2_run.add_argument("--compact", action="store_true")

    full_flow_v2_run_verify = subcommands.add_parser(
        "verify-flowmesh-full-flow-trial-v2-run",
        help="verify a completed label-free full-flow v2 run offline",
    )
    full_flow_v2_run_verify.add_argument(
        "--run-dir", type=Path, required=True
    )
    full_flow_v2_run_verify.add_argument(
        "--plan-dir", type=Path, required=True
    )
    full_flow_v2_run_verify.add_argument(
        "--n1-oracle-package",
        type=Path,
        help=(
            "authenticate the N1 score using this package and runtime-only "
            "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET"
        ),
    )
    full_flow_v2_run_verify.add_argument("--compact", action="store_true")

    n2_index_build = subcommands.add_parser(
        "build-simulator-n2-index",
        help="freeze a portable deterministic N2 lexical index package",
    )
    n2_index_build.add_argument(
        "--source-manifest", type=Path, required=True
    )
    n2_index_build.add_argument("--output-dir", type=Path, required=True)
    n2_index_build.add_argument("--compact", action="store_true")

    n1_oracle_build = subcommands.add_parser(
        "build-simulator-n1-hidden-oracle",
        help="freeze an endpoint-free N1 hidden-label scoring package",
    )
    n1_oracle_build.add_argument(
        "--label-source", type=Path, required=True
    )
    n1_oracle_build.add_argument("--output-dir", type=Path, required=True)
    n1_oracle_build.add_argument("--compact", action="store_true")

    n1_oracle_verify = subcommands.add_parser(
        "verify-simulator-n1-hidden-oracle",
        help="verify a frozen N1 hidden-label scoring package offline",
    )
    n1_oracle_verify.add_argument("--output-dir", type=Path, required=True)
    n1_oracle_verify.add_argument("--compact", action="store_true")

    n1_commitment_freeze = subcommands.add_parser(
        "freeze-simulator-n1-oracle-preselection-commitment",
        help=(
            "publish a label-free hash commitment to a frozen N1 oracle "
            "before policy selection"
        ),
    )
    n1_commitment_freeze.add_argument(
        "--oracle-package-dir", type=Path, required=True
    )
    n1_commitment_freeze.add_argument("--commitment-id", required=True)
    n1_commitment_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    n1_commitment_freeze.add_argument("--compact", action="store_true")

    n1_commitment_verify = subcommands.add_parser(
        "verify-simulator-n1-oracle-preselection-commitment",
        help=(
            "verify a public N1 commitment and optionally open it with the "
            "private package"
        ),
    )
    n1_commitment_verify.add_argument(
        "--commitment-dir", type=Path, required=True
    )
    n1_commitment_verify.add_argument(
        "--oracle-package-dir", type=Path
    )
    n1_commitment_verify.add_argument("--compact", action="store_true")

    n1_oracle_serve = subcommands.add_parser(
        "serve-simulator-n1-hidden-oracle",
        help="serve N1 hidden scoring with runtime-only credentials",
    )
    n1_oracle_serve.add_argument("--package-dir", type=Path, required=True)
    n1_oracle_serve.add_argument("--state-db", type=Path, required=True)
    n1_oracle_serve.add_argument("--host", default="0.0.0.0")
    n1_oracle_serve.add_argument("--port", type=int, default=9081)

    n1_verifier_serve = subcommands.add_parser(
        "serve-simulator-n1-remote-verifier",
        help=(
            "serve authenticated N1 score-evidence verification without "
            "exporting the hidden package or evidence secret"
        ),
    )
    n1_verifier_serve.add_argument(
        "--package-dir", type=Path, required=True
    )
    n1_verifier_serve.add_argument("--state-db", type=Path, required=True)
    n1_verifier_serve.add_argument("--host", default="0.0.0.0")
    n1_verifier_serve.add_argument("--port", type=int, default=9181)

    task_plane_build = subcommands.add_parser(
        "build-simulator-full-flow-task-plane",
        help="split frozen semantic tasks into public and N1-private inputs",
    )
    task_plane_build.add_argument(
        "--semantic-spec", type=Path, action="append", required=True
    )
    task_plane_build.add_argument("--task-plane-id", required=True)
    task_plane_build.add_argument("--oracle-id", required=True)
    task_plane_build.add_argument("--output-dir", type=Path, required=True)
    task_plane_build.add_argument("--compact", action="store_true")

    task_plane_verify = subcommands.add_parser(
        "verify-simulator-full-flow-task-plane",
        help="verify public/private task separation and N1 package bindings",
    )
    task_plane_verify.add_argument("--output-dir", type=Path, required=True)
    task_plane_verify.add_argument("--compact", action="store_true")

    logical_routes_compile = subcommands.add_parser(
        "compile-simulator-full-flow-logical-routes",
        help="compile the 4x8 operation ledger into endpoint-free services",
    )
    logical_routes_compile.add_argument("--scenario", type=Path, required=True)
    logical_routes_compile.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    logical_routes_compile.add_argument(
        "--compiler-id",
        default="full-flow-logical-route-compiler-v1",
    )
    logical_routes_compile.add_argument("--output-dir", type=Path, required=True)
    logical_routes_compile.add_argument("--compact", action="store_true")

    logical_routes_verify = subcommands.add_parser(
        "verify-simulator-full-flow-logical-routes",
        help="verify logical routes against their scenario and container plan",
    )
    logical_routes_verify.add_argument("--plan-dir", type=Path, required=True)
    logical_routes_verify.add_argument("--scenario", type=Path, required=True)
    logical_routes_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    logical_routes_verify.add_argument("--compact", action="store_true")

    artifact_bindings_build = subcommands.add_parser(
        "build-simulator-full-flow-artifact-bindings",
        help=(
            "bind public tasks to verified N3 raw and N4 derived artifact "
            "identities"
        ),
    )
    artifact_bindings_build.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    artifact_bindings_build.add_argument(
        "--scenario", type=Path, required=True
    )
    artifact_bindings_build.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    artifact_bindings_build.add_argument(
        "--task-plane-dir", type=Path, required=True
    )
    artifact_bindings_build.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    artifact_bindings_build.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    artifact_bindings_build.add_argument("--binding-set-id", required=True)
    artifact_bindings_build.add_argument(
        "--output-dir", type=Path, required=True
    )
    artifact_bindings_build.add_argument("--compact", action="store_true")

    artifact_bindings_verify = subcommands.add_parser(
        "verify-simulator-full-flow-artifact-bindings",
        help="verify artifact identities against all source packages",
    )
    artifact_bindings_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    artifact_bindings_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    artifact_bindings_verify.add_argument(
        "--scenario", type=Path, required=True
    )
    artifact_bindings_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    artifact_bindings_verify.add_argument(
        "--task-plane-dir", type=Path, required=True
    )
    artifact_bindings_verify.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    artifact_bindings_verify.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    artifact_bindings_verify.add_argument("--compact", action="store_true")

    provisioning_build = subcommands.add_parser(
        "build-simulator-full-flow-provisioning-catalog",
        help=(
            "bind already materialized N4 artifacts to verified N5-derived "
            "provenance without re-running materialization"
        ),
    )
    provisioning_build.add_argument(
        "--artifact-binding-dir", type=Path, required=True
    )
    provisioning_build.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    provisioning_build.add_argument("--catalog-id", required=True)
    provisioning_build.add_argument(
        "--output-dir", type=Path, required=True
    )
    provisioning_build.add_argument("--compact", action="store_true")

    provisioning_verify = subcommands.add_parser(
        "verify-simulator-full-flow-provisioning-catalog",
        help="verify preprovisioned N5/N4 references against frozen sources",
    )
    provisioning_verify.add_argument(
        "--catalog-dir", type=Path, required=True
    )
    provisioning_verify.add_argument(
        "--artifact-binding-dir", type=Path, required=True
    )
    provisioning_verify.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    provisioning_verify.add_argument("--compact", action="store_true")

    exact_ranges_build = subcommands.add_parser(
        "build-simulator-full-flow-exact-range-catalog",
        help=(
            "freeze content-bound full-object fallback ranges for the "
            "indexed-raw semantic route"
        ),
    )
    exact_ranges_build.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    exact_ranges_build.add_argument("--catalog-id", required=True)
    exact_ranges_build.add_argument(
        "--output-dir", type=Path, required=True
    )
    exact_ranges_build.add_argument("--compact", action="store_true")

    exact_ranges_verify = subcommands.add_parser(
        "verify-simulator-full-flow-exact-range-catalog",
        help="verify exact fallback ranges against the frozen N3 package",
    )
    exact_ranges_verify.add_argument(
        "--catalog-dir", type=Path, required=True
    )
    exact_ranges_verify.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    exact_ranges_verify.add_argument("--compact", action="store_true")

    semantic_matrix_compile = subcommands.add_parser(
        "compile-simulator-full-flow-semantic-matrix",
        help=(
            "bind real public tasks and exact artifact identities to every "
            "endpoint-free 4x8 route"
        ),
    )
    semantic_matrix_compile.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    semantic_matrix_compile.add_argument(
        "--scenario", type=Path, required=True
    )
    semantic_matrix_compile.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    semantic_matrix_compile.add_argument(
        "--public-task-set", type=Path, required=True
    )
    semantic_matrix_compile.add_argument(
        "--artifact-bindings", type=Path, required=True
    )
    semantic_matrix_compile.add_argument(
        "--compiler-id",
        default="full-flow-semantic-matrix-compiler-v1",
    )
    semantic_matrix_compile.add_argument(
        "--output-dir", type=Path, required=True
    )
    semantic_matrix_compile.add_argument("--compact", action="store_true")

    semantic_matrix_verify = subcommands.add_parser(
        "verify-simulator-full-flow-semantic-matrix",
        help="recompile and verify an endpoint-free 4x8 semantic matrix",
    )
    semantic_matrix_verify.add_argument(
        "--plan-dir", type=Path, required=True
    )
    semantic_matrix_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    semantic_matrix_verify.add_argument(
        "--scenario", type=Path, required=True
    )
    semantic_matrix_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    semantic_matrix_verify.add_argument(
        "--public-task-set", type=Path, required=True
    )
    semantic_matrix_verify.add_argument(
        "--artifact-bindings", type=Path, required=True
    )
    semantic_matrix_verify.add_argument("--compact", action="store_true")

    w4_contract_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-retrieval-contract",
        help=(
            "freeze the public W4 ranking task and a separate N1-private "
            "relevance oracle"
        ),
    )
    w4_contract_freeze.add_argument(
        "--semantic-matrix-dir", type=Path, required=True
    )
    w4_contract_freeze.add_argument(
        "--retrieval-config", type=Path, required=True
    )
    w4_contract_freeze.add_argument(
        "--representation-manifest", type=Path, required=True
    )
    w4_contract_freeze.add_argument("--selected-query-id", required=True)
    w4_contract_freeze.add_argument("--contract-id", required=True)
    w4_contract_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_contract_freeze.add_argument("--compact", action="store_true")

    w4_contract_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-retrieval-contract",
        help="verify the split public/private W4 retrieval contract",
    )
    w4_contract_verify.add_argument(
        "--contract-dir", type=Path, required=True
    )
    w4_contract_verify.add_argument("--compact", action="store_true")

    w4_evaluate = subcommands.add_parser(
        "evaluate-simulator-full-flow-w4-retrieval",
        help="score complete W4 rankings without returning hidden relevance IDs",
    )
    w4_evaluate.add_argument("--contract-dir", type=Path, required=True)
    w4_evaluate.add_argument("--observations", type=Path, required=True)
    w4_evaluate.add_argument("--output-dir", type=Path, required=True)
    w4_evaluate.add_argument("--compact", action="store_true")

    w4_evaluation_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-retrieval-evaluation",
        help="verify W4 ranking metrics, optionally by source-bound replay",
    )
    w4_evaluation_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_evaluation_verify.add_argument("--contract-dir", type=Path)
    w4_evaluation_verify.add_argument("--observations", type=Path)
    w4_evaluation_verify.add_argument("--compact", action="store_true")

    w4_runtime_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-retrieval-runtime",
        help=(
            "bind the public W4 retrieval contract to all sixteen local "
            "design/repetition coordinates"
        ),
    )
    w4_runtime_freeze.add_argument(
        "--contract-dir", type=Path, required=True
    )
    w4_runtime_freeze.add_argument(
        "--local-semantic-admission-dir", type=Path, required=True
    )
    w4_runtime_freeze.add_argument("--runtime-overlay-id", required=True)
    w4_runtime_freeze.add_argument("--output-dir", type=Path, required=True)
    w4_runtime_freeze.add_argument("--compact", action="store_true")

    w4_runtime_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-retrieval-runtime",
        help="verify the public-only W4 ranker runtime package",
    )
    w4_runtime_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_runtime_verify.add_argument("--compact", action="store_true")

    w4_ranker_run = subcommands.add_parser(
        "run-simulator-full-flow-w4-lexical-ranker",
        help=(
            "run the public W4 lexical ranker against bound N2/N7/N8 "
            "index services without reading N1 relevance labels"
        ),
    )
    w4_ranker_run.add_argument(
        "--runtime-overlay-dir", type=Path, required=True
    )
    w4_ranker_run.add_argument("--n2-index-base-url", required=True)
    w4_ranker_run.add_argument("--n7-index-base-url", required=True)
    w4_ranker_run.add_argument("--n8-index-base-url", required=True)
    w4_ranker_run.add_argument(
        "--index-package-dir",
        type=Path,
        required=True,
        help=(
            "verified endpoint-free N2 index package used to pin the "
            "index ID, index digest, and source-manifest digest"
        ),
    )
    w4_ranker_run.add_argument("--run-id", required=True)
    w4_ranker_run.add_argument(
        "--allow-http-simulator-host", action="append", default=[]
    )
    w4_ranker_run.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=10.0
    )
    w4_ranker_run.add_argument("--output-dir", type=Path, required=True)
    w4_ranker_run.add_argument("--compact", action="store_true")

    w4_ranker_run_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-ranker-run",
        help=(
            "verify public W4 ranking observations, optionally against "
            "their frozen runtime package"
        ),
    )
    w4_ranker_run_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_ranker_run_verify.add_argument(
        "--runtime-overlay-dir", type=Path
    )
    w4_ranker_run_verify.add_argument("--compact", action="store_true")

    w4_candidate_routes_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-candidate-routes",
        help="freeze candidate-wide D0-D7 W4 physical-route blueprints",
    )
    w4_candidate_routes_freeze.add_argument(
        "--runtime-overlay-dir", type=Path, required=True
    )
    w4_candidate_routes_freeze.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    w4_candidate_routes_freeze.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    w4_candidate_routes_freeze.add_argument(
        "--index-package-dir", type=Path, required=True
    )
    w4_candidate_routes_freeze.add_argument(
        "--exact-range-catalog-dir", type=Path, required=True
    )
    w4_candidate_routes_freeze.add_argument(
        "--physical-plan-id", required=True
    )
    w4_candidate_routes_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_candidate_routes_freeze.add_argument("--compact", action="store_true")

    w4_candidate_routes_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-candidate-routes",
        help="verify frozen candidate-wide W4 physical-route blueprints",
    )
    w4_candidate_routes_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_candidate_routes_verify.add_argument("--compact", action="store_true")

    w4_crosswalk_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-index-artifact-crosswalk",
        help=(
            "freeze the public N2 index-to-representation identity "
            "crosswalk"
        ),
    )
    w4_crosswalk_freeze.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_crosswalk_freeze.add_argument(
        "--index-package-dir", type=Path, required=True
    )
    w4_crosswalk_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_crosswalk_freeze.add_argument("--compact", action="store_true")

    w4_crosswalk_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-index-artifact-crosswalk",
        help="verify the crosswalk by replaying its route and N2 sources",
    )
    w4_crosswalk_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_crosswalk_verify.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_crosswalk_verify.add_argument(
        "--index-package-dir", type=Path, required=True
    )
    w4_crosswalk_verify.add_argument("--compact", action="store_true")

    w4_candidate_run = subcommands.add_parser(
        "run-simulator-full-flow-w4-candidate-conformance",
        help=(
            "execute all sixteen W4 candidate-wide route plans with the "
            "deterministic public conformance adapter; no LLM or FlowMesh"
        ),
    )
    w4_candidate_run.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_candidate_run.add_argument("--run-id", required=True)
    w4_candidate_run.add_argument("--output-dir", type=Path, required=True)
    w4_candidate_run.add_argument("--compact", action="store_true")

    w4_candidate_run_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-candidate-conformance",
        help="verify the source-bound W4 candidate coordinator output",
    )
    w4_candidate_run_verify.add_argument(
        "--run-dir", type=Path, required=True
    )
    w4_candidate_run_verify.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_candidate_run_verify.add_argument("--compact", action="store_true")

    w4_component_receipt_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-component-execution-receipt",
        help=(
            "freeze strict persisted component events against a completed "
            "W4 coordinator run"
        ),
    )
    w4_component_receipt_freeze.add_argument(
        "--coordinator-run-dir", type=Path, required=True
    )
    w4_component_receipt_freeze.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_component_receipt_freeze.add_argument(
        "--crosswalk-dir", type=Path, required=True
    )
    w4_component_receipt_freeze.add_argument(
        "--index-package-dir", type=Path, required=True
    )
    w4_component_receipt_freeze.add_argument(
        "--component-events", type=Path, required=True
    )
    w4_component_receipt_freeze.add_argument(
        "--evidence-class",
        choices=(
            "strict-fake-component-conformance",
            "live-local-component-execution",
        ),
        required=True,
    )
    w4_component_receipt_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_component_receipt_freeze.add_argument(
        "--compact", action="store_true"
    )

    w4_component_receipt_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-component-execution-receipt",
        help="verify a component receipt against all of its frozen sources",
    )
    w4_component_receipt_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_component_receipt_verify.add_argument(
        "--coordinator-run-dir", type=Path, required=True
    )
    w4_component_receipt_verify.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_component_receipt_verify.add_argument(
        "--crosswalk-dir", type=Path, required=True
    )
    w4_component_receipt_verify.add_argument(
        "--index-package-dir", type=Path, required=True
    )
    w4_component_receipt_verify.add_argument(
        "--compact", action="store_true"
    )

    w4_local_components_run = subcommands.add_parser(
        "run-simulator-full-flow-w4-local-component-execution",
        help=(
            "run all sixteen W4 trials through local index, Data Agent, "
            "cache, and N6 semantic components, then freeze their receipt"
        ),
    )
    w4_local_components_run.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_local_components_run.add_argument(
        "--crosswalk-dir", type=Path, required=True
    )
    for node in ("n2", "n7", "n8"):
        w4_local_components_run.add_argument(
            f"--{node}-index-package-dir", type=Path, required=True
        )
        w4_local_components_run.add_argument(
            f"--{node}-index-base-url", required=True
        )
    w4_local_components_run.add_argument(
        "--n3-data-agent-base-url", required=True
    )
    w4_local_components_run.add_argument(
        "--n4-data-agent-base-url", required=True
    )
    w4_local_components_run.add_argument(
        "--n3-data-agent-location", default="origin-cold"
    )
    w4_local_components_run.add_argument(
        "--n4-data-agent-location", default="origin-warm"
    )
    for node in ("n7", "n8"):
        w4_local_components_run.add_argument(
            f"--{node}-cache-base-url", required=True
        )
        w4_local_components_run.add_argument(
            f"--{node}-cache-id", required=True
        )
    w4_local_components_run.add_argument(
        "--n6-base-url", required=True
    )
    w4_local_components_run.add_argument(
        "--semantic-model", required=True
    )
    w4_local_components_run.add_argument(
        "--raw-sampler-scratch-dir", type=Path, required=True
    )
    w4_local_components_run.add_argument("--run-id", required=True)
    w4_local_components_run.add_argument(
        "--output-dir", type=Path, required=True
    )
    w4_local_components_run.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=300.0
    )
    w4_local_components_run.add_argument(
        "--simulator-private-http-hosts",
        default="",
        help="comma-separated simulator-private service names",
    )
    w4_local_components_run.add_argument("--compact", action="store_true")

    w4_flowmesh_plan = subcommands.add_parser(
        "freeze-simulator-full-flow-w4-flowmesh-plan",
        help="freeze sixteen serial worker-pinned W4 coordinator API tasks",
    )
    w4_flowmesh_plan.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_flowmesh_plan.add_argument("--run-id", required=True)
    w4_flowmesh_plan.add_argument("--worker-alias", required=True)
    w4_flowmesh_plan.add_argument("--owner", default="pathfinder")
    w4_flowmesh_plan.add_argument(
        "--api-task-timeout-seconds", type=int, default=900
    )
    w4_flowmesh_plan.add_argument("--output-dir", type=Path, required=True)
    w4_flowmesh_plan.add_argument("--compact", action="store_true")

    w4_flowmesh_plan_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-flowmesh-plan",
        help="recompile and verify a frozen W4 FlowMesh plan",
    )
    w4_flowmesh_plan_verify.add_argument(
        "--plan-dir", type=Path, required=True
    )
    w4_flowmesh_plan_verify.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_flowmesh_plan_verify.add_argument("--compact", action="store_true")

    w4_flowmesh_serve = subcommands.add_parser(
        "serve-simulator-full-flow-w4-flowmesh-coordinator",
        help="serve one source-bound N7 or N8 W4 trial coordinator",
    )
    w4_flowmesh_serve.add_argument(
        "--coordinator-node-id", choices=("N7", "N8"), required=True
    )
    w4_flowmesh_serve.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_flowmesh_serve.add_argument(
        "--crosswalk-dir", type=Path, required=True
    )
    for node in ("n2", "n7", "n8"):
        w4_flowmesh_serve.add_argument(
            f"--{node}-index-package-dir", type=Path, required=True
        )
        w4_flowmesh_serve.add_argument(
            f"--{node}-index-base-url", required=True
        )
    w4_flowmesh_serve.add_argument(
        "--n3-data-agent-base-url", required=True
    )
    w4_flowmesh_serve.add_argument(
        "--n4-data-agent-base-url", required=True
    )
    w4_flowmesh_serve.add_argument(
        "--n3-data-agent-location", default="origin-cold"
    )
    w4_flowmesh_serve.add_argument(
        "--n4-data-agent-location", default="origin-warm"
    )
    for node in ("n7", "n8"):
        w4_flowmesh_serve.add_argument(
            f"--{node}-cache-base-url", required=True
        )
        w4_flowmesh_serve.add_argument(f"--{node}-cache-id", required=True)
    w4_flowmesh_serve.add_argument("--n6-base-url", required=True)
    w4_flowmesh_serve.add_argument("--semantic-model", required=True)
    w4_flowmesh_serve.add_argument(
        "--raw-sampler-scratch-dir", type=Path, required=True
    )
    w4_flowmesh_serve.add_argument("--state-db", type=Path, required=True)
    w4_flowmesh_serve.add_argument("--host", default="127.0.0.1")
    w4_flowmesh_serve.add_argument("--port", type=int, required=True)
    w4_flowmesh_serve.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=300.0
    )
    w4_flowmesh_serve.add_argument(
        "--max-artifact-bytes", type=int, default=2 * 1024 * 1024 * 1024
    )
    w4_flowmesh_serve.add_argument(
        "--simulator-private-http-hosts", default=""
    )
    w4_flowmesh_serve.add_argument("--compact", action="store_true")

    w4_flowmesh_run = subcommands.add_parser(
        "run-simulator-full-flow-w4-flowmesh-matrix",
        help="submit and freeze the sixteen-task W4 FlowMesh wrapper",
    )
    w4_flowmesh_run.add_argument("--plan-dir", type=Path, required=True)
    w4_flowmesh_run.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_flowmesh_run.add_argument("--crosswalk-dir", type=Path, required=True)
    w4_flowmesh_run.add_argument(
        "--index-package-dir", type=Path, required=True
    )
    w4_flowmesh_run.add_argument(
        "--n7-coordinator-base-url", required=True
    )
    w4_flowmesh_run.add_argument(
        "--n8-coordinator-base-url", required=True
    )
    w4_flowmesh_run.add_argument("--worker-alias", required=True)
    w4_flowmesh_run.add_argument("--flowmesh-base-url")
    w4_flowmesh_run.add_argument(
        "--poll-interval", type=_positive_finite_float, default=2.0
    )
    w4_flowmesh_run.add_argument(
        "--simulator-private-http-hosts", default=""
    )
    w4_flowmesh_run.add_argument("--output-dir", type=Path, required=True)
    w4_flowmesh_run.add_argument("--compact", action="store_true")

    w4_flowmesh_run_verify = subcommands.add_parser(
        "verify-simulator-full-flow-w4-flowmesh-matrix",
        help="verify W4 FlowMesh evidence against every frozen source",
    )
    w4_flowmesh_run_verify.add_argument(
        "--run-dir", type=Path, required=True
    )
    w4_flowmesh_run_verify.add_argument(
        "--plan-dir", type=Path, required=True
    )
    w4_flowmesh_run_verify.add_argument(
        "--route-package-dir", type=Path, required=True
    )
    w4_flowmesh_run_verify.add_argument(
        "--crosswalk-dir", type=Path, required=True
    )
    w4_flowmesh_run_verify.add_argument(
        "--index-package-dir", type=Path, required=True
    )
    w4_flowmesh_run_verify.add_argument("--compact", action="store_true")

    semantic_admission_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-semantic-execution-admission",
        help=(
            "bind semantic routes to a deployment and report exact runtime "
            "admission gaps"
        ),
    )
    semantic_admission_freeze.add_argument(
        "--semantic-matrix-dir", type=Path, required=True
    )
    semantic_admission_freeze.add_argument(
        "--deployment-binding-dir", type=Path, required=True
    )
    semantic_admission_freeze.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    semantic_admission_freeze.add_argument(
        "--scenario", type=Path, required=True
    )
    semantic_admission_freeze.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    semantic_admission_freeze.add_argument(
        "--public-task-set", type=Path, required=True
    )
    semantic_admission_freeze.add_argument(
        "--artifact-bindings", type=Path, required=True
    )
    semantic_admission_freeze.add_argument(
        "--n1-oracle-package-dir", type=Path, required=True
    )
    semantic_admission_freeze.add_argument("--worker-alias", required=True)
    semantic_admission_freeze.add_argument(
        "--admission-id",
        default="full-flow-semantic-execution-admission-v1",
    )
    semantic_admission_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    semantic_admission_freeze.add_argument("--compact", action="store_true")

    semantic_admission_verify = subcommands.add_parser(
        "verify-simulator-full-flow-semantic-execution-admission",
        help="verify the source-bound semantic execution admission package",
    )
    semantic_admission_verify.add_argument(
        "--admission-dir", type=Path, required=True
    )
    semantic_admission_verify.add_argument(
        "--semantic-matrix-dir", type=Path, required=True
    )
    semantic_admission_verify.add_argument(
        "--deployment-binding-dir", type=Path, required=True
    )
    semantic_admission_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    semantic_admission_verify.add_argument(
        "--scenario", type=Path, required=True
    )
    semantic_admission_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    semantic_admission_verify.add_argument(
        "--public-task-set", type=Path, required=True
    )
    semantic_admission_verify.add_argument(
        "--artifact-bindings", type=Path, required=True
    )
    semantic_admission_verify.add_argument(
        "--n1-oracle-package-dir", type=Path, required=True
    )
    semantic_admission_verify.add_argument("--compact", action="store_true")

    artifact_preflight = subcommands.add_parser(
        "preflight-simulator-full-flow-semantic-artifacts",
        help=(
            "authenticate to N3/N4 and fully fetch every frozen semantic "
            "artifact exactly once"
        ),
    )
    artifact_preflight.add_argument(
        "--semantic-execution-admission-dir", type=Path, required=True
    )
    artifact_preflight.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    artifact_preflight.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    artifact_preflight.add_argument("--n3-data-agent-url", required=True)
    artifact_preflight.add_argument("--n4-data-agent-url", required=True)
    artifact_preflight.add_argument("--preflight-id", required=True)
    artifact_preflight.add_argument(
        "--timeout-seconds", type=float, default=30.0
    )
    artifact_preflight.add_argument("--max-retries", type=int, default=1)
    artifact_preflight.add_argument(
        "--max-artifact-bytes",
        type=int,
        default=64 * 1024 * 1024 * 1024,
    )
    artifact_preflight.add_argument(
        "--telemetry-quiescence-timeout-seconds",
        type=float,
        default=5.0,
    )
    artifact_preflight.add_argument(
        "--simulator-private-http-host",
        action="append",
        default=[],
    )
    artifact_preflight.add_argument(
        "--output-dir", type=Path, required=True
    )
    artifact_preflight.add_argument("--compact", action="store_true")

    artifact_preflight_verify = subcommands.add_parser(
        "verify-simulator-full-flow-semantic-artifact-preflight",
        help=(
            "verify semantic artifact evidence against admission and N3/N4 "
            "packages"
        ),
    )
    artifact_preflight_verify.add_argument(
        "--preflight-dir", type=Path, required=True
    )
    artifact_preflight_verify.add_argument(
        "--semantic-execution-admission-dir", type=Path, required=True
    )
    artifact_preflight_verify.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    artifact_preflight_verify.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    artifact_preflight_verify.add_argument("--compact", action="store_true")

    local_semantic_promote = subcommands.add_parser(
        "promote-simulator-full-flow-local-semantic-execution-admission",
        help=(
            "promote the immutable blocked admission into public-only local "
            "semantic execution inputs"
        ),
    )
    local_semantic_promote.add_argument(
        "--legacy-admission-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--semantic-matrix-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--deployment-binding-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--scenario", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--public-task-set", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--artifact-bindings", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--n1-oracle-package-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--artifact-preflight-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--exact-range-catalog-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--provisioning-catalog-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument("--semantics-mode", required=True)
    local_semantic_promote.add_argument("--promotion-id", required=True)
    local_semantic_promote.add_argument(
        "--output-dir", type=Path, required=True
    )
    local_semantic_promote.add_argument("--compact", action="store_true")

    local_semantic_verify = subcommands.add_parser(
        "verify-simulator-full-flow-local-semantic-execution-admission",
        help="verify promoted local semantic inputs against every source",
    )
    local_semantic_verify.add_argument(
        "--admission-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--legacy-admission-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--semantic-matrix-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--deployment-binding-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument("--scenario", type=Path, required=True)
    local_semantic_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--public-task-set", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--artifact-bindings", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--n1-oracle-package-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--artifact-preflight-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--exact-range-catalog-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--n3-package-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--provisioning-catalog-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument(
        "--n4-package-dir", type=Path, required=True
    )
    local_semantic_verify.add_argument("--compact", action="store_true")

    local_semantic_runtime_verify = subcommands.add_parser(
        "verify-simulator-full-flow-local-semantic-runtime-package",
        help="verify the self-contained public local runtime package",
    )
    local_semantic_runtime_verify.add_argument(
        "--admission-dir", type=Path, required=True
    )
    local_semantic_runtime_verify.add_argument(
        "--compact", action="store_true"
    )

    index_query_plan_build = subcommands.add_parser(
        "build-simulator-full-flow-index-query-plan-catalog",
        help="freeze visible N2 query plans for indexed semantic trials",
    )
    index_query_plan_build.add_argument(
        "--local-semantic-admission-dir", type=Path, required=True
    )
    index_query_plan_build.add_argument(
        "--n2-index-package-dir", type=Path, required=True
    )
    index_query_plan_build.add_argument(
        "--output-dir", type=Path, required=True
    )
    index_query_plan_build.add_argument("--compact", action="store_true")

    index_query_plan_verify = subcommands.add_parser(
        "verify-simulator-full-flow-index-query-plan-catalog",
        help="verify visible N2 query plans against their frozen sources",
    )
    index_query_plan_verify.add_argument(
        "--catalog-dir", type=Path, required=True
    )
    index_query_plan_verify.add_argument(
        "--local-semantic-admission-dir", type=Path, required=True
    )
    index_query_plan_verify.add_argument(
        "--n2-index-package-dir", type=Path, required=True
    )
    index_query_plan_verify.add_argument("--compact", action="store_true")

    n4_serve_gate_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-n4-preprovisioned-serve-gate",
        help=(
            "freeze the source-bound authorization for serving an immutable "
            "preprovisioned N4 snapshot"
        ),
    )
    for flag in (
        "compose-overlay-dir",
        "service-bootstrap-dir",
        "deployment-binding-dir",
        "logical-plan-dir",
        "scenario",
        "container-plan-dir",
        "provisioning-catalog-dir",
        "artifact-binding-dir",
        "n4-package-dir",
    ):
        n4_serve_gate_freeze.add_argument(
            "--" + flag, type=Path, required=True
        )
    n4_serve_gate_freeze.add_argument("--gate-id", required=True)
    n4_serve_gate_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    n4_serve_gate_freeze.add_argument("--compact", action="store_true")

    n4_serve_gate_verify = subcommands.add_parser(
        "verify-simulator-full-flow-n4-preprovisioned-serve-gate",
        help="verify the N4 serve authorization against every frozen source",
    )
    n4_serve_gate_verify.add_argument(
        "--gate-dir", type=Path, required=True
    )
    for flag in (
        "compose-overlay-dir",
        "service-bootstrap-dir",
        "deployment-binding-dir",
        "logical-plan-dir",
        "scenario",
        "container-plan-dir",
        "provisioning-catalog-dir",
        "artifact-binding-dir",
        "n4-package-dir",
    ):
        n4_serve_gate_verify.add_argument(
            "--" + flag, type=Path, required=True
        )
    n4_serve_gate_verify.add_argument("--compact", action="store_true")

    def add_n4_live_gate_sources(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--live-receipt-bindings",
            type=Path,
            required=True,
            help=(
                "operator-local JSON array of frame/digest receipt source "
                "bindings; paths are used for verification and not frozen"
            ),
        )
        command.add_argument(
            "--n4-publication-store-root", type=Path, required=True
        )
        command.add_argument(
            "--rebound-artifact-binding-dir", type=Path, required=True
        )
        command.add_argument(
            "--rebound-semantic-matrix-dir", type=Path, required=True
        )
        command.add_argument(
            "--rebound-admission-dir", type=Path, required=True
        )

    n4_live_gate_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-n4-live-serve-gate",
        help=(
            "freeze serve-frozen authorization after live N5-to-N4 "
            "publication and downstream input rebinding"
        ),
    )
    add_n4_live_gate_sources(n4_live_gate_freeze)
    n4_live_gate_freeze.add_argument("--gate-id", required=True)
    n4_live_gate_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    n4_live_gate_freeze.add_argument("--compact", action="store_true")

    n4_live_gate_verify = subcommands.add_parser(
        "verify-simulator-full-flow-n4-live-serve-gate",
        help="verify live publication, immutable N4 state, and rebound inputs",
    )
    n4_live_gate_verify.add_argument("--gate-dir", type=Path, required=True)
    add_n4_live_gate_sources(n4_live_gate_verify)
    n4_live_gate_verify.add_argument("--compact", action="store_true")

    def add_n4_serve_gate_sources(
        command: argparse.ArgumentParser,
        *,
        include_shared_sources: bool,
    ) -> None:
        command.add_argument(
            "--n4-serve-gate-dir", type=Path, required=True
        )
        command.add_argument(
            "--compose-overlay-dir", type=Path, required=True
        )
        command.add_argument(
            "--service-bootstrap-dir", type=Path, required=True
        )
        command.add_argument(
            "--provisioning-catalog-dir", type=Path, required=True
        )
        command.add_argument(
            "--artifact-binding-dir", type=Path, required=True
        )
        command.add_argument("--n4-package-dir", type=Path, required=True)
        command.add_argument(
            "--n4-live-gate-sources",
            type=Path,
            help=(
                "operator-local JSON object selecting the live N5-to-N4 "
                "serve gate; relative paths resolve from this file"
            ),
        )
        if include_shared_sources:
            command.add_argument(
                "--deployment-binding-dir", type=Path, required=True
            )
            command.add_argument(
                "--logical-plan-dir", type=Path, required=True
            )
            command.add_argument("--scenario", type=Path, required=True)
            command.add_argument(
                "--container-plan-dir", type=Path, required=True
            )

    local_semantic_smoke_run = subcommands.add_parser(
        "run-simulator-full-flow-local-semantic-smokes",
        help=(
            "run the ten semantic interoperability smokes after verifying "
            "the source-bound N4 serve-frozen authorization"
        ),
    )
    local_semantic_smoke_run.add_argument(
        "--local-semantic-admission-dir", type=Path, required=True
    )
    add_n4_serve_gate_sources(
        local_semantic_smoke_run,
        include_shared_sources=True,
    )
    local_semantic_smoke_run.add_argument("--run-id", required=True)
    local_semantic_smoke_run.add_argument(
        "--output-dir", type=Path, required=True
    )
    local_semantic_smoke_run.add_argument("--flowmesh-base-url")
    local_semantic_smoke_run.add_argument(
        "--task-timeout", type=int, default=900
    )
    local_semantic_smoke_run.add_argument(
        "--poll-interval", type=_positive_finite_float, default=2.0
    )
    local_semantic_smoke_run.add_argument("--compact", action="store_true")

    local_semantic_smoke_verify = subcommands.add_parser(
        "verify-simulator-full-flow-local-semantic-smokes",
        help="verify the ten-smoke receipt, N4 gate, and frozen sources",
    )
    local_semantic_smoke_verify.add_argument(
        "--smoke-dir", type=Path, required=True
    )
    local_semantic_smoke_verify.add_argument(
        "--local-semantic-admission-dir", type=Path, required=True
    )
    add_n4_serve_gate_sources(
        local_semantic_smoke_verify,
        include_shared_sources=True,
    )
    local_semantic_smoke_verify.add_argument("--compact", action="store_true")

    def add_local_semantic_matrix_gate_sources(
        command: argparse.ArgumentParser,
    ) -> None:
        command.add_argument(
            "--local-semantic-admission-dir", type=Path, required=True
        )
        command.add_argument("--smoke-dir", type=Path, required=True)
        add_n4_serve_gate_sources(
            command,
            include_shared_sources=False,
        )
        command.add_argument(
            "--semantic-matrix-dir", type=Path, required=True
        )
        command.add_argument(
            "--deployment-binding-dir", type=Path, required=True
        )
        command.add_argument(
            "--logical-plan-dir", type=Path, required=True
        )
        command.add_argument("--scenario", type=Path, required=True)
        command.add_argument(
            "--container-plan-dir", type=Path, required=True
        )
        command.add_argument("--public-task-set", type=Path, required=True)
        command.add_argument(
            "--artifact-bindings", type=Path, required=True
        )

    local_semantic_matrix_run = subcommands.add_parser(
        "run-simulator-full-flow-local-semantic-matrix",
        help=(
            "run or resume the semantic 64-trial matrix only after a "
            "verified source-bound ten-smoke receipt"
        ),
    )
    add_local_semantic_matrix_gate_sources(local_semantic_matrix_run)
    local_semantic_matrix_run.add_argument("--run-id", required=True)
    local_semantic_matrix_run.add_argument(
        "--output-dir", type=Path, required=True
    )
    local_semantic_matrix_run.add_argument("--flowmesh-base-url")
    local_semantic_matrix_run.add_argument(
        "--task-timeout", type=int, default=900
    )
    local_semantic_matrix_run.add_argument(
        "--poll-interval", type=_positive_finite_float, default=2.0
    )
    local_semantic_matrix_run.add_argument(
        "--acknowledge-failed-entry-sha256"
    )
    local_semantic_matrix_run.add_argument("--compact", action="store_true")

    local_semantic_matrix_verify = subcommands.add_parser(
        "verify-simulator-full-flow-local-semantic-matrix",
        help="verify the source-bound smoke gate and completed semantic run",
    )
    add_local_semantic_matrix_gate_sources(local_semantic_matrix_verify)
    local_semantic_matrix_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    local_semantic_matrix_verify.add_argument("--compact", action="store_true")

    semantic_route_serve = subcommands.add_parser(
        "serve-simulator-full-flow-semantic-route",
        help=(
            "serve one N7/N8 catalog-bound semantic route coordinator using "
            "public frozen inputs and runtime-only credentials"
        ),
    )
    semantic_route_serve.add_argument(
        "--node-id", choices=("N7", "N8"), required=True
    )
    for flag in (
        "local-semantic-admission-dir",
        "n1-public-commitment-dir",
        "artifact-binding-dir",
        "n2-index-package-dir",
        "n3-package-dir",
        "n4-package-dir",
        "exact-range-catalog-dir",
        "provisioning-catalog-dir",
        "index-query-plan-catalog-dir",
        "state-dir",
    ):
        semantic_route_serve.add_argument(
            "--" + flag, type=Path, required=True
        )
    for flag in (
        "n2-index-base-url",
        "n7-index-base-url",
        "n8-index-base-url",
        "n3-data-agent-base-url",
        "n4-data-agent-base-url",
        "n7-cache-base-url",
        "n8-cache-base-url",
        "n7-cache-id",
        "n8-cache-id",
        "n7-node-health-base-url",
        "n8-node-health-base-url",
        "n6-base-url",
        "n1-base-url",
        "n1-verification-base-url",
        "semantic-model",
    ):
        semantic_route_serve.add_argument("--" + flag, required=True)
    semantic_route_serve.add_argument(
        "--simulator-private-http-hosts",
        default="",
        help=(
            "comma-separated pathfinder-sim-* or pathfinder-full-flow-* "
            "container hostnames permitted for private HTTP"
        ),
    )
    semantic_route_serve.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=300.0
    )
    semantic_route_serve.add_argument(
        "--max-artifact-bytes", type=int, default=2 * 1024 * 1024 * 1024
    )
    semantic_route_serve.add_argument("--host", default="0.0.0.0")
    semantic_route_serve.add_argument("--port", type=int, required=True)

    service_bootstrap_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-service-bootstrap",
        help="freeze endpoint-free N1-N8 process startup contracts",
    )
    service_bootstrap_freeze.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    service_bootstrap_freeze.add_argument(
        "--scenario", type=Path, required=True
    )
    service_bootstrap_freeze.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    service_bootstrap_freeze.add_argument("--bootstrap-id", required=True)
    service_bootstrap_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    service_bootstrap_freeze.add_argument("--compact", action="store_true")

    service_bootstrap_verify = subcommands.add_parser(
        "verify-simulator-full-flow-service-bootstrap",
        help="re-derive and verify N1-N8 process startup contracts",
    )
    service_bootstrap_verify.add_argument(
        "--bootstrap-dir", type=Path, required=True
    )
    service_bootstrap_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    service_bootstrap_verify.add_argument(
        "--scenario", type=Path, required=True
    )
    service_bootstrap_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    service_bootstrap_verify.add_argument("--compact", action="store_true")

    deployment_build = subcommands.add_parser(
        "build-simulator-full-flow-deployment",
        help="bind every logical full-flow service to a concrete deployment",
    )
    deployment_build.add_argument("--logical-plan-dir", type=Path, required=True)
    deployment_build.add_argument("--scenario", type=Path, required=True)
    deployment_build.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    deployment_build.add_argument(
        "--deployment-source", type=Path, required=True
    )
    deployment_build.add_argument("--output-dir", type=Path, required=True)
    deployment_build.add_argument("--compact", action="store_true")

    deployment_verify = subcommands.add_parser(
        "verify-simulator-full-flow-deployment",
        help="verify all service, representation, and state bindings offline",
    )
    deployment_verify.add_argument("--binding-dir", type=Path, required=True)
    deployment_verify.add_argument("--logical-plan-dir", type=Path, required=True)
    deployment_verify.add_argument("--scenario", type=Path, required=True)
    deployment_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    deployment_verify.add_argument("--compact", action="store_true")

    deployment_preflight = subcommands.add_parser(
        "preflight-simulator-full-flow-deployment",
        help="read-only health probe every distinct full-flow service origin",
    )
    deployment_preflight.add_argument("--binding-dir", type=Path, required=True)
    deployment_preflight.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    deployment_preflight.add_argument("--scenario", type=Path, required=True)
    deployment_preflight.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    deployment_preflight.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=5.0
    )
    deployment_preflight.add_argument("--compact", action="store_true")

    compose_overlay_render = subcommands.add_parser(
        "render-simulator-full-flow-compose-overlay",
        help="freeze a unified N1-N8 Compose overlay without launching it",
    )
    compose_overlay_render.add_argument(
        "--service-bootstrap-dir", type=Path, required=True
    )
    compose_overlay_render.add_argument(
        "--deployment-binding-dir", type=Path, required=True
    )
    compose_overlay_render.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    compose_overlay_render.add_argument(
        "--scenario", type=Path, required=True
    )
    compose_overlay_render.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    compose_overlay_render.add_argument("--overlay-id", required=True)
    compose_overlay_render.add_argument(
        "--output-dir", type=Path, required=True
    )
    compose_overlay_render.add_argument("--compact", action="store_true")

    compose_overlay_verify = subcommands.add_parser(
        "verify-simulator-full-flow-compose-overlay",
        help="verify a unified N1-N8 Compose overlay from source contracts",
    )
    compose_overlay_verify.add_argument(
        "--overlay-dir", type=Path, required=True
    )
    compose_overlay_verify.add_argument(
        "--service-bootstrap-dir", type=Path, required=True
    )
    compose_overlay_verify.add_argument(
        "--deployment-binding-dir", type=Path, required=True
    )
    compose_overlay_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    compose_overlay_verify.add_argument(
        "--scenario", type=Path, required=True
    )
    compose_overlay_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    compose_overlay_verify.add_argument("--compact", action="store_true")

    deployment_template_generate = subcommands.add_parser(
        "generate-simulator-full-flow-deployment-template",
        help=(
            "generate an endpoint-free deployment source template for all "
            "logical services"
        ),
    )
    deployment_template_generate.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    deployment_template_generate.add_argument(
        "--scenario", type=Path, required=True
    )
    deployment_template_generate.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    deployment_template_generate.add_argument("--template-id", required=True)
    deployment_template_generate.add_argument(
        "--output-dir", type=Path, required=True
    )
    deployment_template_generate.add_argument("--compact", action="store_true")

    deployment_template_verify = subcommands.add_parser(
        "verify-simulator-full-flow-deployment-template",
        help="verify a full-flow deployment source template offline",
    )
    deployment_template_verify.add_argument(
        "--template-dir", type=Path, required=True
    )
    deployment_template_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    deployment_template_verify.add_argument(
        "--scenario", type=Path, required=True
    )
    deployment_template_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    deployment_template_verify.add_argument("--compact", action="store_true")

    deployment_source_validate = subcommands.add_parser(
        "validate-simulator-full-flow-deployment-source",
        help=(
            "validate an operator-completed deployment source without "
            "publishing a binding"
        ),
    )
    deployment_source_validate.add_argument(
        "--deployment-source", type=Path, required=True
    )
    deployment_source_validate.add_argument(
        "--template-dir", type=Path, required=True
    )
    deployment_source_validate.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    deployment_source_validate.add_argument(
        "--scenario", type=Path, required=True
    )
    deployment_source_validate.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    deployment_source_validate.add_argument("--compact", action="store_true")

    n3_raw_build = subcommands.add_parser(
        "build-simulator-n3-raw-data-plane",
        help="freeze authoritative raw MP4 bytes into a portable N3 package",
    )
    n3_raw_build.add_argument(
        "--binding-manifest", type=Path, required=True
    )
    n3_raw_build.add_argument("--output-dir", type=Path, required=True)
    n3_raw_build.add_argument("--compact", action="store_true")

    n3_raw_verify = subcommands.add_parser(
        "verify-simulator-n3-raw-data-plane",
        help="verify a portable N3 raw/cold Data Agent package offline",
    )
    n3_raw_verify.add_argument("--output-dir", type=Path, required=True)
    n3_raw_verify.add_argument("--compact", action="store_true")

    n4_derived_build = subcommands.add_parser(
        "build-simulator-n4-derived-data-plane",
        help=(
            "freeze frame bundles and multimodal digests into a portable "
            "N4 Data Agent package"
        ),
    )
    n4_derived_build.add_argument(
        "--binding-manifest", type=Path, required=True
    )
    n4_derived_build.add_argument("--output-dir", type=Path, required=True)
    n4_derived_build.add_argument("--compact", action="store_true")

    n4_derived_verify = subcommands.add_parser(
        "verify-simulator-n4-derived-data-plane",
        help="verify a portable N4 derived-representation package offline",
    )
    n4_derived_verify.add_argument("--output-dir", type=Path, required=True)
    n4_derived_verify.add_argument("--compact", action="store_true")

    n2_index_verify = subcommands.add_parser(
        "verify-simulator-n2-index",
        help="verify a portable N2 index package offline",
    )
    n2_index_verify.add_argument("--output-dir", type=Path, required=True)
    n2_index_verify.add_argument("--compact", action="store_true")

    n2_index_serve = subcommands.add_parser(
        "serve-simulator-n2-index",
        help="serve a frozen N2 index package without embedding its endpoint",
    )
    n2_index_serve.add_argument("--package-dir", type=Path, required=True)
    n2_index_serve.add_argument(
        "--node-id",
        choices=("N2", "N7", "N8"),
        default="N2",
        help="logical index-service identity; frozen index bytes are unchanged",
    )
    n2_index_serve.add_argument("--host", default="0.0.0.0")
    n2_index_serve.add_argument("--port", type=int, default=9082)
    n2_index_serve.add_argument(
        "--require-token",
        action="store_true",
        help="require PATHFINDER_N2_INDEX_TOKEN at startup",
    )

    cache_serve = subcommands.add_parser(
        "serve-simulator-full-flow-cache",
        help="serve a durable real-byte cache on logical N7 or N8",
    )
    cache_serve.add_argument("--node-id", choices=("N7", "N8"), required=True)
    cache_serve.add_argument("--cache-id", required=True)
    cache_serve.add_argument("--state-dir", type=Path, required=True)
    cache_serve.add_argument("--capacity-bytes", type=int, required=True)
    cache_serve.add_argument("--host", default="0.0.0.0")
    cache_serve.add_argument("--port", type=int, default=9081)
    cache_serve.add_argument(
        "--token-env-name",
        choices=(
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            "PATHFINDER_N7_W4_CACHE_TOKEN",
            "PATHFINDER_N8_W4_CACHE_TOKEN",
        ),
        default="PATHFINDER_FULL_FLOW_CACHE_TOKEN",
        help="runtime-only bearer-token environment name",
    )
    cache_serve.add_argument(
        "--fallback-token-env-name",
        choices=(
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            "PATHFINDER_N7_W4_CACHE_TOKEN",
            "PATHFINDER_N8_W4_CACHE_TOKEN",
        ),
        help="optional runtime-only fallback bearer-token environment name",
    )
    cache_serve.add_argument(
        "--max-artifact-bytes", type=int, default=64 * 1024 * 1024
    )

    n5_serve = subcommands.add_parser(
        "serve-simulator-n5-materializer",
        help="serve durable handle-based N5 frame-bundle materialization",
    )
    n5_serve.add_argument("--state-dir", type=Path, required=True)
    n5_serve.add_argument("--host", default="0.0.0.0")
    n5_serve.add_argument("--port", type=int, default=9085)

    live_provisioning_run = subcommands.add_parser(
        "run-simulator-n5-n4-live-frame-bundle-smoke",
        help=(
            "exercise authenticated local N5 materialization and atomic "
            "N4 publication for one frozen frame-bundle plan"
        ),
    )
    live_provisioning_run.add_argument(
        "--n5-plan", type=Path, required=True
    )
    live_provisioning_run.add_argument(
        "--source-video", type=Path, required=True
    )
    live_provisioning_run.add_argument("--n5-base-url", required=True)
    live_provisioning_run.add_argument("--n4-base-url", required=True)
    live_provisioning_run.add_argument(
        "--allow-http-simulator-host", action="append", default=[]
    )
    live_provisioning_run.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=300.0
    )
    live_provisioning_run.add_argument("--smoke-id", required=True)
    live_provisioning_run.add_argument("--publication-id", required=True)
    live_provisioning_run.add_argument("--package-id", required=True)
    live_provisioning_run.add_argument("--catalog-version", required=True)
    live_provisioning_run.add_argument("--expected-current-catalog-version")
    live_provisioning_run.add_argument(
        "--output-dir", type=Path, required=True
    )
    live_provisioning_run.add_argument("--compact", action="store_true")

    live_provisioning_verify = subcommands.add_parser(
        "verify-simulator-n5-n4-live-frame-bundle-smoke",
        help="verify the local N5-to-N4 receipt against its frozen N5 plan",
    )
    live_provisioning_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    live_provisioning_verify.add_argument(
        "--n5-plan", type=Path, required=True
    )
    live_provisioning_verify.add_argument("--compact", action="store_true")

    live_digest_run = subcommands.add_parser(
        "run-simulator-n5-n4-live-digest-smoke",
        help=(
            "exercise authenticated local N5 vision-digest generation and "
            "atomic N4 publication for one frozen digest plan"
        ),
    )
    live_digest_run.add_argument(
        "--n5-digest-plan-dir", type=Path, required=True
    )
    live_digest_run.add_argument(
        "--source-video", type=Path, required=True
    )
    live_digest_run.add_argument("--n5-digest-base-url", required=True)
    live_digest_run.add_argument("--n4-base-url", required=True)
    live_digest_run.add_argument(
        "--allow-http-simulator-host", action="append", default=[]
    )
    live_digest_run.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=300.0
    )
    live_digest_run.add_argument("--smoke-id", required=True)
    live_digest_run.add_argument("--request-id", required=True)
    live_digest_run.add_argument("--publication-id", required=True)
    live_digest_run.add_argument("--package-id", required=True)
    live_digest_run.add_argument("--catalog-version", required=True)
    live_digest_run.add_argument("--expected-current-catalog-version")
    live_digest_run.add_argument("--output-dir", type=Path, required=True)
    live_digest_run.add_argument("--compact", action="store_true")

    live_digest_verify = subcommands.add_parser(
        "verify-simulator-n5-n4-live-digest-smoke",
        help="verify a local live digest receipt against its plan and video",
    )
    live_digest_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    live_digest_verify.add_argument(
        "--n5-digest-plan-dir", type=Path, required=True
    )
    live_digest_verify.add_argument(
        "--source-video", type=Path, required=True
    )
    live_digest_verify.add_argument("--compact", action="store_true")

    def add_bulk_live_sources(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--provisioning-catalog-dir", type=Path, required=True
        )
        command.add_argument(
            "--artifact-binding-dir", type=Path, required=True
        )
        command.add_argument("--n4-package-dir", type=Path, required=True)

    bulk_source_manifest_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-bulk-provisioning-source-manifest",
        help=(
            "freeze an operator-local, content-bound source manifest for "
            "every catalog object"
        ),
    )
    add_bulk_live_sources(bulk_source_manifest_freeze)
    bulk_source_manifest_freeze.add_argument(
        "--object-mapping", type=Path, required=True
    )
    bulk_source_manifest_freeze.add_argument(
        "--output-path", type=Path, required=True
    )
    bulk_source_manifest_freeze.add_argument(
        "--compact", action="store_true"
    )

    bulk_live_run = subcommands.add_parser(
        "run-simulator-full-flow-bulk-live-provisioning",
        help=(
            "durably materialize and publish every frozen frame bundle and "
            "digest through the existing N5-to-N4 live adapters"
        ),
    )
    add_bulk_live_sources(bulk_live_run)
    bulk_live_run.add_argument(
        "--operator-source-manifest", type=Path, required=True
    )
    bulk_live_run.add_argument(
        "--n5-frame-base-url",
        "--n5-base-url",
        dest="n5_frame_base_url",
        required=True,
    )
    bulk_live_run.add_argument("--n5-digest-base-url", required=True)
    bulk_live_run.add_argument("--n4-base-url", required=True)
    bulk_live_run.add_argument(
        "--allow-http-simulator-host", action="append", default=[]
    )
    bulk_live_run.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=300.0
    )
    bulk_live_run.add_argument("--run-id", required=True)
    bulk_live_run.add_argument("--output-dir", type=Path, required=True)
    bulk_live_run.add_argument("--resume", action="store_true")
    bulk_live_run.add_argument("--compact", action="store_true")

    bulk_live_verify = subcommands.add_parser(
        "verify-simulator-full-flow-bulk-live-provisioning",
        help=(
            "verify a complete bulk N5-to-N4 run against every frozen and "
            "operator-local source"
        ),
    )
    add_bulk_live_sources(bulk_live_verify)
    bulk_live_verify.add_argument(
        "--operator-source-manifest", type=Path, required=True
    )
    bulk_live_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    bulk_live_verify.add_argument("--compact", action="store_true")

    def add_pre_upcloud_readiness_sources(
        command: argparse.ArgumentParser,
    ) -> None:
        for flag in (
            "provisioning-catalog-dir",
            "artifact-binding-dir",
            "n4-package-dir",
            "logical-route-dir",
            "scenario",
            "container-plan-dir",
            "task-plane-dir",
            "n3-package-dir",
            "w4-route-package-dir",
            "w4-index-package-dir",
            "w4-index-crosswalk-dir",
        ):
            command.add_argument("--" + flag, type=Path, required=True)
        for flag, help_text in (
            (
                "source-archive",
                "optional source archive/file bound by content digest",
            ),
            ("neutral-observation-dir", None),
            ("neutral-semantic-matrix-run-dir", None),
            ("semantic-execution-admission-dir", None),
            ("smoke-gated-semantic-matrix-run-dir", None),
            ("ten-smoke-dir", None),
            ("n4-serve-gate-dir", None),
            ("compose-overlay-dir", None),
            ("service-bootstrap-dir", None),
            ("deployment-binding-dir", None),
            ("semantic-matrix-dir", None),
            ("public-task-set", None),
            ("semantic-artifact-binding", None),
            (
                "n4-live-gate-sources",
                "strict local N4 live-gate source descriptor JSON",
            ),
            ("n1-oracle-package-dir", None),
            ("bulk-provisioning-output-dir", None),
            ("bulk-source-manifest", None),
            ("bulk-live-receipt-bindings", None),
            ("w4-component-receipt-dir", None),
            ("w4-coordinator-run-dir", None),
            (
                "w4-retrieval-contract-dir",
                "private/public W4 retrieval contract used for N1 scoring",
            ),
            (
                "w4-retrieval-evaluation-dir",
                "source-bound N1 W4 retrieval evaluation output",
            ),
            ("flowmesh-matrix-plan-dir", None),
            ("flowmesh-formal-profile-dir", None),
            ("flowmesh-coordinator-plan-dir", None),
            (
                "flowmesh-matrix-run-dir",
                "completed 64-trial FlowMesh matrix execution to verify",
            ),
            (
                "flowmesh-w4-plan-dir",
                "frozen 16-task W4 FlowMesh candidate plan to verify",
            ),
            (
                "flowmesh-w4-run-dir",
                "completed 16-task W4 FlowMesh candidate execution to verify",
            ),
        ):
            command.add_argument("--" + flag, type=Path, help=help_text)
        command.add_argument("--source-git-revision", required=True)
        command.add_argument(
            "--attest-clean-committed-source",
            action="store_true",
            required=True,
            help=(
                "operator declaration that the supplied revision is committed "
                "and the source working tree used for evidence was clean"
            ),
        )

    pre_upcloud_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-pre-upcloud-readiness",
        help=(
            "freeze a non-mutating offline audit of required, local-live, "
            "FlowMesh-execution, and UpCloud-only readiness"
        ),
    )
    add_pre_upcloud_readiness_sources(pre_upcloud_freeze)
    pre_upcloud_freeze.add_argument("--audit-id", required=True)
    pre_upcloud_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    pre_upcloud_freeze.add_argument("--compact", action="store_true")

    pre_upcloud_verify = subcommands.add_parser(
        "verify-simulator-full-flow-pre-upcloud-readiness",
        help="reproduce the offline pre-UpCloud audit from exact sources",
    )
    add_pre_upcloud_readiness_sources(pre_upcloud_verify)
    pre_upcloud_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    pre_upcloud_verify.add_argument("--compact", action="store_true")

    n5_digest_freeze = subcommands.add_parser(
        "freeze-simulator-n5-digest-plan",
        help=(
            "sample one real video and freeze an endpoint-free N5 vision "
            "digest materialization plan"
        ),
    )
    n5_digest_freeze.add_argument(
        "--source-video", type=Path, required=True
    )
    n5_digest_freeze.add_argument("--object-id", required=True)
    n5_digest_freeze.add_argument("--model-id", required=True)
    n5_digest_freeze.add_argument("--plan-id", required=True)
    n5_digest_freeze.add_argument("--frame-count", type=int, default=16)
    n5_digest_freeze.add_argument(
        "--jpeg-max-dimension", type=int, default=768
    )
    n5_digest_freeze.add_argument("--seed", type=int, default=0)
    n5_digest_freeze.add_argument(
        "--maximum-digest-bytes", type=int, default=256 * 1024
    )
    n5_digest_freeze.add_argument("--output-dir", type=Path, required=True)
    n5_digest_freeze.add_argument("--compact", action="store_true")

    n5_digest_plan_verify = subcommands.add_parser(
        "verify-simulator-n5-digest-plan",
        help="verify an N5 digest plan and its exact source video offline",
    )
    n5_digest_plan_verify.add_argument(
        "--plan-dir", type=Path, required=True
    )
    n5_digest_plan_verify.add_argument(
        "--source-video", type=Path, required=True
    )
    n5_digest_plan_verify.add_argument("--compact", action="store_true")

    n5_digest_run = subcommands.add_parser(
        "run-simulator-n5-digest-materialization",
        help=(
            "materialize one N5 digest through the runtime-configured "
            "OpenAI-compatible vision service"
        ),
    )
    n5_digest_run.add_argument("--plan-dir", type=Path, required=True)
    n5_digest_run.add_argument("--source-video", type=Path, required=True)
    n5_digest_run.add_argument("--output-dir", type=Path, required=True)
    n5_digest_run.add_argument(
        "--allow-http-simulator-host", action="append", default=[]
    )
    n5_digest_run.add_argument(
        "--timeout-seconds", type=_positive_finite_float, default=180.0
    )
    n5_digest_run.add_argument("--max-attempts", type=int, default=3)
    n5_digest_run.add_argument("--compact", action="store_true")

    n5_digest_output_verify = subcommands.add_parser(
        "verify-simulator-n5-digest-materialization",
        help="verify N5 digest bytes and semantic provenance offline",
    )
    n5_digest_output_verify.add_argument(
        "--output-dir", type=Path, required=True
    )
    n5_digest_output_verify.add_argument(
        "--plan-dir", type=Path, required=True
    )
    n5_digest_output_verify.add_argument(
        "--source-video", type=Path, required=True
    )
    n5_digest_output_verify.add_argument("--compact", action="store_true")

    policy_routes_freeze = subcommands.add_parser(
        "freeze-simulator-policy-routes",
        help="bind a prospective W1-W4 AWM assignment to verified routes",
    )
    policy_routes_freeze.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    policy_routes_freeze.add_argument("--scenario", type=Path, required=True)
    policy_routes_freeze.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    policy_routes_freeze.add_argument("--policy-id", required=True)
    policy_routes_freeze.add_argument("--awm-policy-sha256", required=True)
    policy_routes_freeze.add_argument(
        "--assignment",
        action="append",
        required=True,
        help="repeat W1=D0,D1 through W4=...",
    )
    policy_routes_freeze.add_argument("--output-dir", type=Path, required=True)
    policy_routes_freeze.add_argument("--compact", action="store_true")

    policy_routes_verify = subcommands.add_parser(
        "verify-simulator-policy-routes",
        help="verify a frozen W1-W4 policy route selection",
    )
    policy_routes_verify.add_argument(
        "--assignment-dir", type=Path, required=True
    )
    policy_routes_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    policy_routes_verify.add_argument("--scenario", type=Path, required=True)
    policy_routes_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    policy_routes_verify.add_argument("--compact", action="store_true")

    oed_routes_freeze = subcommands.add_parser(
        "freeze-simulator-oed-routes",
        help="freeze an exact prospective OED trial subset and order",
    )
    oed_routes_freeze.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    oed_routes_freeze.add_argument("--scenario", type=Path, required=True)
    oed_routes_freeze.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    oed_routes_freeze.add_argument("--oed-request-id", required=True)
    oed_routes_freeze.add_argument("--oed-request-sha256", required=True)
    oed_routes_freeze.add_argument(
        "--trial-key", action="append", required=True
    )
    oed_routes_freeze.add_argument("--output-dir", type=Path, required=True)
    oed_routes_freeze.add_argument("--compact", action="store_true")

    oed_routes_verify = subcommands.add_parser(
        "verify-simulator-oed-routes",
        help="verify a frozen prospective OED route selection",
    )
    oed_routes_verify.add_argument(
        "--selection-dir", type=Path, required=True
    )
    oed_routes_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    oed_routes_verify.add_argument("--scenario", type=Path, required=True)
    oed_routes_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    oed_routes_verify.add_argument("--compact", action="store_true")

    observations_freeze = subcommands.add_parser(
        "freeze-simulator-full-flow-observations",
        help=(
            "convert verified full-flow evidence into neutral AWM/OED "
            "observations without inventing monetary cost"
        ),
    )
    observations_freeze.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    observations_freeze.add_argument("--scenario", type=Path, required=True)
    observations_freeze.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    observations_freeze.add_argument("--observation-set-id", required=True)
    observations_freeze_source = (
        observations_freeze.add_mutually_exclusive_group(required=True)
    )
    observations_freeze_source.add_argument(
        "--evidence-json",
        type=Path,
        action="append",
        help="explicit full-flow evidence JSON; repeat for each trial",
    )
    observations_freeze_source.add_argument(
        "--semantic-matrix-run-dir",
        type=Path,
        help=(
            "verified completed semantic matrix run whose bound public "
            "route-evidence sidecar supplies all observations"
        ),
    )
    observations_freeze.add_argument(
        "--external-real-cost-manifest", type=Path
    )
    observations_freeze.add_argument(
        "--n1-oracle-package",
        type=Path,
        help=(
            "privileged offline authentication of hidden-oracle v2 and "
            "generic semantic-route scores using this package and the "
            "runtime-only PATHFINDER_N1_ORACLE_EVIDENCE_SECRET"
        ),
    )
    observations_freeze.add_argument(
        "--semantic-execution-admission-dir",
        type=Path,
        help=(
            "required when evidence uses the promoted generic semantic "
            "route schema"
        ),
    )
    observations_freeze.add_argument("--output-dir", type=Path, required=True)
    observations_freeze.add_argument("--compact", action="store_true")

    observations_verify = subcommands.add_parser(
        "verify-simulator-full-flow-observations",
        help="verify neutral observations against their source evidence",
    )
    observations_verify.add_argument(
        "--observation-dir", type=Path, required=True
    )
    observations_verify.add_argument(
        "--logical-plan-dir", type=Path, required=True
    )
    observations_verify.add_argument("--scenario", type=Path, required=True)
    observations_verify.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    observations_verify_source = (
        observations_verify.add_mutually_exclusive_group(required=True)
    )
    observations_verify_source.add_argument(
        "--evidence-json",
        type=Path,
        action="append",
        help="explicit full-flow evidence JSON; repeat for each trial",
    )
    observations_verify_source.add_argument(
        "--semantic-matrix-run-dir",
        type=Path,
        help=(
            "verified completed semantic matrix run whose bound public "
            "route-evidence sidecar supplies all observations"
        ),
    )
    observations_verify.add_argument(
        "--n1-oracle-package",
        type=Path,
        help=(
            "privileged offline authentication of hidden-oracle v2 and "
            "generic semantic-route scores using this package and the "
            "runtime-only PATHFINDER_N1_ORACLE_EVIDENCE_SECRET"
        ),
    )
    observations_verify.add_argument(
        "--semantic-execution-admission-dir",
        type=Path,
        help=(
            "required when evidence uses the promoted generic semantic "
            "route schema"
        ),
    )
    observations_verify.add_argument("--compact", action="store_true")

    neutral_analysis_freeze = subcommands.add_parser(
        "freeze-simulator-neutral-awm-oed-analysis",
        help=(
            "freeze an offline quality/infrastructure AWM/OED analysis of "
            "a verified complete neutral 4x8x2 observation package"
        ),
    )
    neutral_analysis_freeze.add_argument(
        "--observation-dir", type=Path, required=True
    )
    neutral_analysis_freeze.add_argument("--analysis-id", required=True)
    neutral_analysis_freeze.add_argument(
        "--baseline-design-id",
        choices=tuple(f"D{index}" for index in range(8)),
        default="D0",
    )
    neutral_analysis_freeze.add_argument(
        "--oed-selection-size",
        type=_neutral_oed_selection_size,
        default=4,
        help="number of W1-W4 fresh-workload pairs to prioritize (1-4)",
    )
    neutral_analysis_freeze.add_argument(
        "--require-real-cost",
        action="store_true",
        help="fail closed unless the observation package binds external costs",
    )
    neutral_analysis_freeze.add_argument(
        "--alpha", type=_unit_interval_float, default=0.05
    )
    neutral_analysis_freeze.add_argument(
        "--delta-success-margin",
        type=_closed_unit_interval_float,
        default=0.0,
    )
    neutral_analysis_freeze.add_argument(
        "--minimum-cost-saving", type=_finite_float, default=0.0
    )
    neutral_analysis_freeze.add_argument(
        "--cost-saving-support",
        type=_finite_float,
        nargs=2,
        metavar=("LOWER", "UPPER"),
        help=(
            "predeclared finite lower and upper support for real cost "
            "savings; required when external costs are present"
        ),
    )
    neutral_analysis_freeze.add_argument(
        "--output-dir", type=Path, required=True
    )
    neutral_analysis_freeze.add_argument("--compact", action="store_true")

    neutral_analysis_verify = subcommands.add_parser(
        "verify-simulator-neutral-awm-oed-analysis",
        help=(
            "recompute a frozen neutral AWM/OED analysis from its bound "
            "observation package"
        ),
    )
    neutral_analysis_verify.add_argument(
        "--observation-dir", type=Path, required=True
    )
    neutral_analysis_verify.add_argument(
        "--analysis-dir", type=Path, required=True
    )
    neutral_analysis_verify.add_argument("--compact", action="store_true")

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

    data_agent_semantic_trial = subcommands.add_parser(
        "run-data-agent-frame-bundle-semantic-trial",
        help=(
            "run one matrix-bound frame-bundle trial through a routed Data "
            "Agent and an enabled container vision executor"
        ),
    )
    data_agent_semantic_trial.add_argument(
        "--matrix-plan-dir", type=Path, required=True
    )
    data_agent_semantic_trial.add_argument(
        "--semantic-spec", type=Path, required=True
    )
    data_agent_semantic_trial.add_argument(
        "--endpoint-registry", type=Path, required=True
    )
    data_agent_semantic_trial.add_argument(
        "--compose-package-dir", type=Path, required=True
    )
    data_agent_semantic_trial.add_argument(
        "--output-dir", type=Path, required=True
    )
    data_agent_semantic_trial.add_argument(
        "--request-timeout", type=float, default=240.0
    )
    data_agent_semantic_trial.add_argument(
        "--telemetry-quiescence-timeout", type=float, default=5.0
    )
    data_agent_semantic_trial.add_argument(
        "--max-artifact-bytes", type=int, default=8 * 1024 * 1024
    )
    data_agent_semantic_trial.add_argument(
        "--event-index",
        type=int,
        default=0,
        help=(
            "non-negative Data Agent access attempt index; increment it "
            "when retrying after a post-download semantic failure"
        ),
    )
    data_agent_semantic_trial.add_argument("--compact", action="store_true")

    verify_data_agent_semantic_trial = subcommands.add_parser(
        "verify-data-agent-frame-bundle-semantic-trial",
        help=(
            "verify a matrix-bound Data Agent and container-vision semantic "
            "trial without network, container, or LLM access"
        ),
    )
    verify_data_agent_semantic_trial.add_argument(
        "--output-dir", type=Path, required=True
    )
    verify_data_agent_semantic_trial.add_argument(
        "--matrix-plan-dir", type=Path, required=True
    )
    verify_data_agent_semantic_trial.add_argument(
        "--semantic-spec", type=Path, required=True
    )
    verify_data_agent_semantic_trial.add_argument(
        "--endpoint-registry", type=Path, required=True
    )
    verify_data_agent_semantic_trial.add_argument(
        "--compact", action="store_true"
    )

    build_pathfinder_evidence = subcommands.add_parser(
        "build-flowmesh-pathfinder-evidence",
        help=(
            "bind verified matrix infrastructure results to separately "
            "verified Data Agent semantic trials (offline association only)"
        ),
    )
    build_pathfinder_evidence.add_argument(
        "--binding-spec", type=Path, required=True
    )
    build_pathfinder_evidence.add_argument(
        "--matrix-plan-dir", type=Path, required=True
    )
    build_pathfinder_evidence.add_argument(
        "--matrix-run-dir", type=Path, required=True
    )
    build_pathfinder_evidence.add_argument(
        "--endpoint-registry", type=Path, required=True
    )
    build_pathfinder_evidence.add_argument(
        "--data-agent-semantic-dir",
        type=Path,
        action="append",
        required=True,
    )
    build_pathfinder_evidence.add_argument(
        "--data-agent-semantic-spec",
        type=Path,
        action="append",
        required=True,
    )
    build_pathfinder_evidence.add_argument(
        "--output-dir", type=Path, required=True
    )
    build_pathfinder_evidence.add_argument("--compact", action="store_true")

    verify_pathfinder_evidence = subcommands.add_parser(
        "verify-flowmesh-pathfinder-evidence",
        help=(
            "rebuild and verify a simulator-specific Pathfinder evidence "
            "association from its immutable sources"
        ),
    )
    verify_pathfinder_evidence.add_argument(
        "--evidence-dir", type=Path, required=True
    )
    verify_pathfinder_evidence.add_argument(
        "--binding-spec", type=Path, required=True
    )
    verify_pathfinder_evidence.add_argument(
        "--matrix-plan-dir", type=Path, required=True
    )
    verify_pathfinder_evidence.add_argument(
        "--matrix-run-dir", type=Path, required=True
    )
    verify_pathfinder_evidence.add_argument(
        "--endpoint-registry", type=Path, required=True
    )
    verify_pathfinder_evidence.add_argument(
        "--data-agent-semantic-dir",
        type=Path,
        action="append",
        required=True,
    )
    verify_pathfinder_evidence.add_argument(
        "--data-agent-semantic-spec",
        type=Path,
        action="append",
        required=True,
    )
    verify_pathfinder_evidence.add_argument("--compact", action="store_true")

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
        "--api-task-timeout-seconds",
        type=int,
        default=None,
        help=(
            "per-task FlowMesh API executor timeout in seconds (default 120), "
            "frozen into the plan and used verbatim at submission; planning is "
            "refused if it is below a selected operation's derived lower "
            "bound. This is distinct from run-time workflow polling"
        ),
    )
    flowmesh_container_dag_plan.add_argument(
        "--output-dir", type=Path, required=True
    )
    flowmesh_container_dag_plan.add_argument("--compact", action="store_true")

    verify_flowmesh_container_dag_run = subcommands.add_parser(
        "verify-flowmesh-container-dag-run",
        help=(
            "verify a completed FlowMesh container-DAG run artifact offline: "
            "checksums, plan binding, task-result coverage, worker identity, "
            "and preserved container telemetry"
        ),
    )
    verify_flowmesh_container_dag_run.add_argument(
        "--run-dir", type=Path, required=True
    )
    verify_flowmesh_container_dag_run.add_argument(
        "--plan-dir",
        type=Path,
        help="optional frozen plan directory to bind the run against",
    )
    verify_flowmesh_container_dag_run.add_argument(
        "--compact", action="store_true"
    )

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

    flowmesh_container_matrix_plan = subcommands.add_parser(
        "plan-flowmesh-container-matrix",
        help=(
            "freeze the complete 64-trial / 500-operation 4x8 container "
            "matrix for a later plan-bound FlowMesh trial-wrapper run; "
            "does not start services or submit work"
        ),
    )
    flowmesh_container_matrix_plan.add_argument(
        "--portable-plan-dir", type=Path, required=True
    )
    flowmesh_container_matrix_plan.add_argument(
        "--container-plan-dir", type=Path, required=True
    )
    flowmesh_container_matrix_plan.add_argument(
        "--node-api-url",
        action="append",
        required=True,
        help=(
            "repeat NODE_ID=http://host:port for every one of the eight "
            "frozen topology nodes"
        ),
    )
    flowmesh_container_matrix_plan.add_argument("--worker-alias", required=True)
    flowmesh_container_matrix_plan.add_argument("--matrix-id", required=True)
    flowmesh_container_matrix_plan.add_argument(
        "--source-git-revision",
        required=True,
        help="40-character lowercase commit ID that produced the frozen inputs",
    )
    flowmesh_container_matrix_plan.add_argument(
        "--execution-profile-id",
        required=True,
        help="explicit frozen admission/cache profile identifier",
    )
    flowmesh_container_matrix_plan.add_argument(
        "--api-task-timeout-seconds",
        type=int,
        default=None,
        help=(
            "per-operation FlowMesh API timeout in seconds; frozen into the "
            "matrix and refused below any link-rate lower bound"
        ),
    )
    flowmesh_container_matrix_plan.add_argument(
        "--output-dir", type=Path, required=True
    )
    flowmesh_container_matrix_plan.add_argument("--compact", action="store_true")

    verify_flowmesh_container_matrix_plan = subcommands.add_parser(
        "verify-flowmesh-container-matrix-plan",
        help=(
            "verify a frozen complete 4x8 container-matrix plan offline; "
            "no service or FlowMesh request is made"
        ),
    )
    verify_flowmesh_container_matrix_plan.add_argument(
        "--plan-dir", type=Path, required=True
    )
    verify_flowmesh_container_matrix_plan.add_argument(
        "--compact", action="store_true"
    )

    conditional_dag_candidates = subcommands.add_parser(
        "list-flowmesh-container-conditional-dag-candidates",
        help=(
            "list every cache-conditional 4x8 trial with the hit/miss "
            "outcomes derivable from its frozen initial cache snapshot; "
            "does not start services or submit work"
        ),
    )
    conditional_dag_candidates.add_argument(
        "--container-operations", type=Path, required=True
    )
    conditional_dag_candidates.add_argument("--compact", action="store_true")

    conditional_dag_resolve = subcommands.add_parser(
        "resolve-flowmesh-container-conditional-dag",
        help=(
            "resolve one cache-conditional container trial into a safe "
            "two-phase DAG from frozen cache state; does not start services "
            "or submit work"
        ),
    )
    conditional_dag_resolve.add_argument(
        "--container-operations", type=Path, required=True
    )
    conditional_dag_resolve.add_argument("--trial-key", required=True)
    conditional_dag_resolve.add_argument(
        "--cache-outcome",
        action="append",
        default=[],
        help=(
            "optional exact expectation LOOKUP_OPERATION_KEY=hit|miss; it "
            "must agree with the frozen cache snapshot"
        ),
    )
    conditional_dag_resolve.add_argument("--compact", action="store_true")

    conditional_trial_plan = subcommands.add_parser(
        "plan-flowmesh-container-conditional-trial",
        help=(
            "freeze one D3/D7 cache-conditional trial into a two-phase "
            "FlowMesh plan; does not start services or submit work"
        ),
    )
    conditional_trial_plan.add_argument(
        "--matrix-plan-dir", type=Path, required=True
    )
    conditional_trial_plan.add_argument("--trial-key", required=True)
    conditional_trial_plan.add_argument("--smoke-id", required=True)
    conditional_trial_plan.add_argument("--owner", default="pathfinder")
    conditional_trial_plan.add_argument("--output-dir", type=Path, required=True)
    conditional_trial_plan.add_argument("--compact", action="store_true")

    verify_conditional_trial_plan = subcommands.add_parser(
        "verify-flowmesh-container-conditional-trial-plan",
        help="verify a frozen two-phase cache-conditional trial plan offline",
    )
    verify_conditional_trial_plan.add_argument("--plan-dir", type=Path, required=True)
    verify_conditional_trial_plan.add_argument("--compact", action="store_true")

    conditional_trial_run = subcommands.add_parser(
        "run-flowmesh-container-conditional-trial",
        help=(
            "submit the phase-A cache checks of one frozen D3/D7 trial and "
            "submit phase B only when their literal outcomes match"
        ),
    )
    conditional_trial_run.add_argument("--plan-dir", type=Path, required=True)
    conditional_trial_run.add_argument("--output-dir", type=Path, required=True)
    conditional_trial_run.add_argument("--worker-alias", required=True)
    conditional_trial_run.add_argument("--flowmesh-base-url")
    conditional_trial_run.add_argument("--task-timeout", type=int, default=600)
    conditional_trial_run.add_argument("--poll-interval", type=float, default=2.0)
    conditional_trial_run.add_argument("--compact", action="store_true")

    verify_conditional_trial_run = subcommands.add_parser(
        "verify-flowmesh-container-conditional-trial-run",
        help=(
            "verify a completed or phase-B-refused conditional trial "
            "artifact offline"
        ),
    )
    verify_conditional_trial_run.add_argument("--run-dir", type=Path, required=True)
    verify_conditional_trial_run.add_argument("--plan-dir", type=Path)
    verify_conditional_trial_run.add_argument("--compact", action="store_true")

    formal_profile = subcommands.add_parser(
        "freeze-flowmesh-container-formal-execution-profile",
        help=(
            "bind a v2 matrix and the read-only fast/slow audit into the "
            "conservative serial infrastructure-conformance profile"
        ),
    )
    formal_profile.add_argument("--matrix-plan-dir", type=Path, required=True)
    formal_profile.add_argument(
        "--calibration-audit-dir", type=Path, required=True
    )
    formal_profile.add_argument("--execution-profile-id", required=True)
    formal_profile.add_argument(
        "--primary-trial-wrapper-max-concurrency", type=int, default=1
    )
    formal_profile.add_argument("--output-dir", type=Path, required=True)
    formal_profile.add_argument("--compact", action="store_true")

    verify_formal_profile = subcommands.add_parser(
        "verify-flowmesh-container-formal-execution-profile",
        help="verify a frozen formal container-infrastructure profile offline",
    )
    verify_formal_profile.add_argument("--profile-dir", type=Path, required=True)
    verify_formal_profile.add_argument("--compact", action="store_true")

    matrix_coordinator = subcommands.add_parser(
        "plan-flowmesh-container-matrix-coordinator-dry-run",
        help=(
            "freeze a non-submitting, globally serial 64-trial coordinator "
            "admission package bound to a v2 matrix and formal profile"
        ),
    )
    matrix_coordinator.add_argument("--matrix-plan-dir", type=Path, required=True)
    matrix_coordinator.add_argument(
        "--formal-execution-profile-dir", type=Path, required=True
    )
    matrix_coordinator.add_argument("--coordinator-id", required=True)
    matrix_coordinator.add_argument("--output-dir", type=Path, required=True)
    matrix_coordinator.add_argument("--compact", action="store_true")

    verify_matrix_coordinator = subcommands.add_parser(
        "verify-flowmesh-container-matrix-coordinator-dry-run",
        help=(
            "verify a frozen 64-trial coordinator admission package; "
            "source matrix/profile bindings are optional"
        ),
    )
    verify_matrix_coordinator.add_argument("--plan-dir", type=Path, required=True)
    verify_matrix_coordinator.add_argument("--matrix-plan-dir", type=Path)
    verify_matrix_coordinator.add_argument(
        "--formal-execution-profile-dir", type=Path
    )
    verify_matrix_coordinator.add_argument("--compact", action="store_true")

    matrix_run = subcommands.add_parser(
        "run-flowmesh-container-matrix",
        help=(
            "execute or safely resume the frozen, globally serial 64-trial "
            "FlowMesh container matrix against already-running services"
        ),
    )
    matrix_run.add_argument("--matrix-plan-dir", type=Path, required=True)
    matrix_run.add_argument(
        "--formal-execution-profile-dir", type=Path, required=True
    )
    matrix_run.add_argument(
        "--coordinator-plan-dir", type=Path, required=True
    )
    matrix_run.add_argument("--output-dir", type=Path, required=True)
    matrix_run.add_argument(
        "--run-id",
        required=True,
        help=(
            "stable identifier for this execution; reuse it with the same "
            "output directory to resume"
        ),
    )
    matrix_run.add_argument("--worker-alias", required=True)
    matrix_run.add_argument("--flowmesh-base-url")
    matrix_run.add_argument(
        "--poll-interval", type=_positive_finite_float, default=2.0
    )
    matrix_run.add_argument(
        "--recovery-id",
        help=(
            "explicit identifier for one audited infrastructure recovery; "
            "requires the recovery reason and failed-entry digest"
        ),
    )
    matrix_run.add_argument(
        "--recovery-reason",
        help=(
            "operator rationale for retrying an allowlisted FlowMesh "
            "identity-provider failure or exact Root result-upload read "
            "timeout at a structurally proven safe schedule root; "
            "result-upload timeout recovery is unconditional-phase only"
        ),
    )
    matrix_run.add_argument(
        "--recover-failed-entry-sha256",
        help=(
            "SHA-256 of the terminal RUN_FAILED journal entry being "
            "authorized; this is not a general force/retry option"
        ),
    )
    matrix_run.add_argument("--compact", action="store_true")

    matrix_replay_adoption = subcommands.add_parser(
        "adopt-flowmesh-container-matrix-replay-results",
        help=(
            "adopt one safe schedule replay from an already-bound DONE "
            "recovery workflow without validating or submitting a workflow"
        ),
    )
    matrix_replay_adoption.add_argument(
        "--matrix-plan-dir", type=Path, required=True
    )
    matrix_replay_adoption.add_argument(
        "--formal-execution-profile-dir", type=Path, required=True
    )
    matrix_replay_adoption.add_argument(
        "--coordinator-plan-dir", type=Path, required=True
    )
    matrix_replay_adoption.add_argument("--run-dir", type=Path, required=True)
    matrix_replay_adoption.add_argument("--run-id", required=True)
    matrix_replay_adoption.add_argument("--worker-alias", required=True)
    matrix_replay_adoption.add_argument("--flowmesh-base-url")
    matrix_replay_adoption.add_argument(
        "--poll-interval", type=_positive_finite_float, default=2.0
    )
    matrix_replay_adoption.add_argument("--adoption-id", required=True)
    matrix_replay_adoption.add_argument("--adoption-reason", required=True)
    matrix_replay_adoption.add_argument(
        "--adopt-failed-entry-sha256",
        required=True,
        help=(
            "exact digest of the latest replay-only RUN_FAILED journal entry"
        ),
    )
    matrix_replay_adoption.add_argument("--compact", action="store_true")

    verify_matrix_run = subcommands.add_parser(
        "verify-flowmesh-container-matrix-run",
        help=(
            "verify a completed formal matrix run offline; optionally "
            "re-bind it to all three frozen source packages"
        ),
    )
    verify_matrix_run.add_argument("--run-dir", type=Path, required=True)
    verify_matrix_run.add_argument("--matrix-plan-dir", type=Path)
    verify_matrix_run.add_argument(
        "--formal-execution-profile-dir", type=Path
    )
    verify_matrix_run.add_argument("--coordinator-plan-dir", type=Path)
    verify_matrix_run.add_argument("--compact", action="store_true")

    matrix_statistics = subcommands.add_parser(
        "summarize-flowmesh-container-matrix-run",
        help=(
            "publish read-only workload/design/route statistics from one "
            "verified 64-trial matrix run without deriving cost, throughput, "
            "end-to-end latency, quality, or a design ranking"
        ),
    )
    matrix_statistics.add_argument("--run-dir", type=Path, required=True)
    matrix_statistics.add_argument(
        "--matrix-plan-dir", type=Path, required=True
    )
    matrix_statistics.add_argument(
        "--formal-execution-profile-dir", type=Path, required=True
    )
    matrix_statistics.add_argument(
        "--coordinator-plan-dir", type=Path, required=True
    )
    matrix_statistics.add_argument("--output-dir", type=Path, required=True)
    matrix_statistics.add_argument("--compact", action="store_true")

    verify_matrix_statistics = subcommands.add_parser(
        "verify-flowmesh-container-matrix-statistics",
        help=(
            "verify a source-independent, descriptive-only container-matrix "
            "statistics package"
        ),
    )
    verify_matrix_statistics.add_argument(
        "--output-dir", type=Path, required=True
    )
    verify_matrix_statistics.add_argument("--compact", action="store_true")

    full_chain_candidates = subcommands.add_parser(
        "list-flowmesh-container-full-chain-candidates",
        help=(
            "list complete, terminal, unconditional physical operation paths "
            "that can be pinned as FlowMesh workflows"
        ),
    )
    full_chain_candidates.add_argument(
        "--container-operations", type=Path, required=True
    )
    full_chain_candidates.add_argument("--compact", action="store_true")

    full_chain_plan = subcommands.add_parser(
        "plan-flowmesh-container-full-chain",
        help=(
            "freeze one complete existing physical operation path as a "
            "non-submitting FlowMesh workflow package"
        ),
    )
    full_chain_plan.add_argument(
        "--container-operations", type=Path, required=True
    )
    full_chain_plan.add_argument(
        "--node-api-url",
        action="append",
        required=True,
        help=(
            "repeat NODE_ID=http://host:port for every selected execution "
            "node; bindings are frozen into the plan"
        ),
    )
    full_chain_plan.add_argument("--worker-alias", required=True)
    full_chain_plan.add_argument("--smoke-id", required=True)
    full_chain_plan.add_argument("--trial-key")
    full_chain_plan.add_argument("--owner", default="pathfinder")
    full_chain_plan.add_argument(
        "--api-task-timeout-seconds",
        type=int,
        default=None,
        help=(
            "per-task FlowMesh API executor timeout in seconds (default 120), "
            "frozen into the plan and refused below any derived operation floor"
        ),
    )
    full_chain_plan.add_argument("--output-dir", type=Path, required=True)
    full_chain_plan.add_argument("--compact", action="store_true")

    verify_full_chain_plan = subcommands.add_parser(
        "verify-flowmesh-container-full-chain-plan",
        help="verify a frozen complete physical-chain workflow package offline",
    )
    verify_full_chain_plan.add_argument("--plan-dir", type=Path, required=True)
    verify_full_chain_plan.add_argument("--compact", action="store_true")

    full_chain_run = subcommands.add_parser(
        "run-flowmesh-container-full-chain",
        help=(
            "validate, submit, and verify every task in one frozen complete "
            "physical path against already-running services"
        ),
    )
    full_chain_run.add_argument("--plan-dir", type=Path, required=True)
    full_chain_run.add_argument("--output-dir", type=Path, required=True)
    full_chain_run.add_argument("--worker-alias", required=True)
    full_chain_run.add_argument("--flowmesh-base-url")
    full_chain_run.add_argument("--task-timeout", type=int, default=600)
    full_chain_run.add_argument("--poll-interval", type=float, default=2.0)
    full_chain_run.add_argument("--compact", action="store_true")

    verify_full_chain_run = subcommands.add_parser(
        "verify-flowmesh-container-full-chain-run",
        help=(
            "verify a completed full physical-chain FlowMesh artifact offline: "
            "checksums, plan binding, pinning, coverage, and telemetry"
        ),
    )
    verify_full_chain_run.add_argument("--run-dir", type=Path, required=True)
    verify_full_chain_run.add_argument("--plan-dir", type=Path)
    verify_full_chain_run.add_argument("--compact", action="store_true")

    full_chain_calibration = subcommands.add_parser(
        "audit-flowmesh-container-full-chain-calibration",
        help=(
            "read only two existing full-chain artifacts and audit configured "
            "fast/slow application-shaping conformance; fits no parameters "
            "and derives no network throughput"
        ),
    )
    full_chain_calibration.add_argument(
        "--fast-plan-dir", type=Path, required=True
    )
    full_chain_calibration.add_argument(
        "--fast-run-dir", type=Path, required=True
    )
    full_chain_calibration.add_argument(
        "--slow-plan-dir", type=Path, required=True
    )
    full_chain_calibration.add_argument(
        "--slow-run-dir", type=Path, required=True
    )
    full_chain_calibration.add_argument("--output-dir", type=Path, required=True)
    full_chain_calibration.add_argument("--compact", action="store_true")

    verify_full_chain_calibration = subcommands.add_parser(
        "verify-flowmesh-container-full-chain-calibration",
        help="verify an immutable read-only fast/slow full-chain audit",
    )
    verify_full_chain_calibration.add_argument(
        "--output-dir", type=Path, required=True
    )
    verify_full_chain_calibration.add_argument("--compact", action="store_true")

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
        if args.command == "build-full-flow-data-plane":
            from .simulator.full_flow_data_plane import (
                build_full_flow_data_plane_package_from_semantic_specs,
            )

            payload = build_full_flow_data_plane_package_from_semantic_specs(
                [
                    (Path(spec), Path(artifact))
                    for spec, artifact in args.semantic_spec_artifact
                ],
                output_dir=args.output_dir,
                package_id=args.package_id,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-full-flow-data-plane":
            from .simulator.full_flow_data_plane import (
                verify_full_flow_data_plane_package,
            )

            payload = verify_full_flow_data_plane_package(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-full-flow-compose-binding":
            from .simulator.full_flow_compose import (
                build_full_flow_compose_binding,
            )

            payload = build_full_flow_compose_binding(
                args.base_compose_package,
                args.data_plane_package,
                output_dir=args.output_dir,
                route_id=args.route_id,
                data_agent_plan_id=args.data_agent_plan_id,
                data_agent_plan_epoch=args.data_agent_plan_epoch,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-full-flow-compose-binding":
            from .simulator.full_flow_compose import (
                verify_full_flow_compose_binding,
            )

            payload = verify_full_flow_compose_binding(
                args.output_dir,
                base_compose_package=args.base_compose_package,
                data_plane_package=args.data_plane_package,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-full-flow-deployment-binding":
            from .integrations.flowmesh.full_flow_trial import (
                build_full_flow_deployment_binding,
            )

            payload = build_full_flow_deployment_binding(
                deployment_binding_id=args.deployment_binding_id,
                coordinator_api_url=args.coordinator_api_url,
                worker_alias=args.worker_alias,
                api_task_timeout_seconds=args.api_task_timeout_seconds,
                output_path=args.output,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "plan-flowmesh-full-flow-trial":
            from .integrations.flowmesh.full_flow_trial import (
                plan_flowmesh_full_flow_trial,
            )

            payload = plan_flowmesh_full_flow_trial(
                semantic_spec=args.semantic_spec,
                data_plane_package=args.data_plane_package,
                deployment_binding=args.deployment_binding,
                output_dir=args.output_dir,
                owner=args.owner,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-full-flow-trial-plan":
            from .integrations.flowmesh.full_flow_trial import (
                verify_flowmesh_full_flow_trial_plan,
            )

            payload = verify_flowmesh_full_flow_trial_plan(args.plan_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-flowmesh-full-flow-trial":
            from .integrations.flowmesh import (
                FlowMeshSettings,
                SdkFlowMeshClient,
            )
            from .integrations.flowmesh.full_flow_trial import (
                run_flowmesh_full_flow_trial,
                verify_flowmesh_full_flow_trial_plan,
            )

            verified = verify_flowmesh_full_flow_trial_plan(args.plan_dir)
            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                task_timeout_seconds=args.task_timeout,
                poll_interval_seconds=args.poll_interval,
                worker_alias=verified["worker_alias"],
                validate_before_submit=True,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = run_flowmesh_full_flow_trial(
                    plan_dir=args.plan_dir,
                    output_dir=args.output_dir,
                    client=client,
                    settings=settings,
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-full-flow-trial-run":
            from .integrations.flowmesh.full_flow_trial import (
                verify_flowmesh_full_flow_trial_run,
            )

            payload = verify_flowmesh_full_flow_trial_run(
                args.run_dir,
                plan_dir=args.plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-flowmesh-full-flow-public-request-v2":
            from .simulator.full_flow_runtime import (
                FullFlowRouteConfig,
                build_full_flow_trial_request_v2,
            )

            public_task = json.loads(
                args.public_task_binding.read_text(encoding="utf-8")
            )
            if not isinstance(public_task, dict):
                raise ConfigError("public task binding must be a JSON object")
            route = FullFlowRouteConfig(
                route_id=args.route_id,
                requested_location=args.requested_location,
                data_agent_plan_id=args.data_agent_plan_id,
                data_agent_plan_epoch=args.data_agent_plan_epoch,
                quiescence_timeout_seconds=(
                    args.quiescence_timeout_seconds
                ),
            )
            try:
                payload = build_full_flow_trial_request_v2(
                    route_config=route,
                    full_flow_request_id=args.full_flow_request_id,
                    run_id=args.run_id,
                    trial_id=args.trial_id,
                    trial_key=args.trial_key,
                    workload_id=public_task["workload_id"],
                    task_class_id=public_task["task_class_id"],
                    object_id=public_task["object_id"],
                    artifact_sha256=args.artifact_sha256,
                    artifact_size_bytes=args.artifact_size_bytes,
                    object_catalog_version=args.object_catalog_version,
                    expected_model=args.expected_model,
                    question=public_task["question"],
                    answer_options=public_task["answer_options"],
                    success_scoring_rule=public_task[
                        "success_scoring_rule"
                    ],
                    oracle_id=args.oracle_id,
                    task_binding_sha256=public_task[
                        "task_binding_sha256"
                    ],
                )
            except KeyError as exc:
                raise ConfigError(
                    "public task binding is incomplete"
                ) from exc
            target = args.output.resolve()
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                with target.open("x", encoding="utf-8", newline="\n") as handle:
                    json.dump(
                        payload,
                        handle,
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                    handle.write("\n")
            except FileExistsError as exc:
                raise ConfigError(
                    "public request output already exists"
                ) from exc
            return _print_payload(
                {
                    "status": "FROZEN_PUBLIC_FULL_FLOW_REQUEST_V2",
                    "output": str(target),
                    "full_flow_request_id": payload[
                        "full_flow_request_id"
                    ],
                    "task_binding_sha256": payload[
                        "task_binding_sha256"
                    ],
                    "frozen_binding_sha256": payload[
                        "frozen_binding_sha256"
                    ],
                    "credentials_recorded": False,
                },
                compact=args.compact,
            )
        if args.command == "plan-flowmesh-full-flow-trial-v2":
            from .integrations.flowmesh.full_flow_trial import (
                plan_flowmesh_full_flow_trial_v2,
            )
            from .simulator.full_flow_runtime import FullFlowRouteConfig

            public_request = json.loads(
                args.public_request.read_text(encoding="utf-8")
            )
            if not isinstance(public_request, dict):
                raise ConfigError("public request must be a JSON object")
            route = FullFlowRouteConfig(
                route_id=args.route_id,
                requested_location=args.requested_location,
                data_agent_plan_id=args.data_agent_plan_id,
                data_agent_plan_epoch=args.data_agent_plan_epoch,
                quiescence_timeout_seconds=(
                    args.quiescence_timeout_seconds
                ),
            )
            payload = plan_flowmesh_full_flow_trial_v2(
                public_request=public_request,
                route_config=route,
                data_plane_package=args.data_plane_package,
                deployment_binding=args.deployment_binding,
                output_dir=args.output_dir,
                owner=args.owner,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-full-flow-trial-v2-plan":
            from .integrations.flowmesh.full_flow_trial import (
                verify_flowmesh_full_flow_trial_v2_plan,
            )

            payload = verify_flowmesh_full_flow_trial_v2_plan(args.plan_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-flowmesh-full-flow-trial-v2":
            from .integrations.flowmesh import (
                FlowMeshSettings,
                SdkFlowMeshClient,
            )
            from .integrations.flowmesh.full_flow_trial import (
                run_flowmesh_full_flow_trial_v2,
                verify_flowmesh_full_flow_trial_v2_plan,
            )

            verified = verify_flowmesh_full_flow_trial_v2_plan(args.plan_dir)
            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                task_timeout_seconds=args.task_timeout,
                poll_interval_seconds=args.poll_interval,
                worker_alias=verified["worker_alias"],
                validate_before_submit=True,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = run_flowmesh_full_flow_trial_v2(
                    plan_dir=args.plan_dir,
                    output_dir=args.output_dir,
                    client=client,
                    settings=settings,
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-full-flow-trial-v2-run":
            from .integrations.flowmesh.full_flow_trial import (
                verify_flowmesh_full_flow_trial_v2_run,
            )

            n1_secret = None
            if args.n1_oracle_package is not None:
                n1_secret_text = os.environ.get(
                    "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET"
                )
                if not n1_secret_text:
                    raise ConfigError(
                        "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET is required "
                        "when --n1-oracle-package is used"
                    )
                n1_secret = n1_secret_text.encode("utf-8")
            payload = verify_flowmesh_full_flow_trial_v2_run(
                args.run_dir,
                plan_dir=args.plan_dir,
                n1_oracle_package_dir=args.n1_oracle_package,
                n1_evidence_secret=n1_secret,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-n2-index":
            from .simulator.index_service import build_n2_index_package

            payload = build_n2_index_package(
                args.source_manifest,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-n1-hidden-oracle":
            from .simulator.hidden_oracle import build_n1_oracle_package

            payload = build_n1_oracle_package(
                args.label_source,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-n1-hidden-oracle":
            from .simulator.hidden_oracle import verify_n1_oracle_package

            payload = verify_n1_oracle_package(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-n1-oracle-preselection-commitment"
        ):
            from .simulator.hidden_oracle_commitment import (
                freeze_n1_oracle_preselection_commitment,
            )

            payload = freeze_n1_oracle_preselection_commitment(
                args.oracle_package_dir,
                commitment_id=args.commitment_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-n1-oracle-preselection-commitment"
        ):
            from .simulator.hidden_oracle_commitment import (
                verify_n1_oracle_preselection_commitment,
            )

            payload = verify_n1_oracle_preselection_commitment(
                args.commitment_dir,
                oracle_package_dir=args.oracle_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "serve-simulator-n1-hidden-oracle":
            from .simulator.hidden_oracle import create_n1_oracle_http_server

            token = os.environ.get("PATHFINDER_N1_ORACLE_TOKEN")
            secret = os.environ.get("PATHFINDER_N1_ORACLE_EVIDENCE_SECRET")
            if not token or not secret:
                raise ConfigError(
                    "PATHFINDER_N1_ORACLE_TOKEN and "
                    "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET are required"
                )
            server = create_n1_oracle_http_server(
                args.package_dir,
                state_db=args.state_db,
                bearer_token=token,
                evidence_secret=secret.encode("utf-8"),
                host=args.host,
                port=args.port,
            )
            try:
                server.serve_forever(poll_interval=0.2)
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0
        if args.command == "serve-simulator-n1-remote-verifier":
            from .simulator.full_flow_n1_remote_verification import (
                create_n1_remote_verification_http_server,
            )

            token = os.environ.get("PATHFINDER_N1_VERIFICATION_TOKEN")
            secret = os.environ.get("PATHFINDER_N1_ORACLE_EVIDENCE_SECRET")
            if not token or not secret:
                raise ConfigError(
                    "PATHFINDER_N1_VERIFICATION_TOKEN and "
                    "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET are required"
                )
            server = create_n1_remote_verification_http_server(
                args.package_dir,
                state_db=args.state_db,
                bearer_token=token,
                evidence_secret=secret.encode("utf-8"),
                host=args.host,
                port=args.port,
            )
            try:
                server.serve_forever(poll_interval=0.2)
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0
        if args.command == "build-simulator-full-flow-task-plane":
            from .simulator.full_flow_tasks import build_full_flow_task_plane

            payload = build_full_flow_task_plane(
                args.semantic_spec,
                task_plane_id=args.task_plane_id,
                oracle_id=args.oracle_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-task-plane":
            from .simulator.full_flow_tasks import verify_full_flow_task_plane

            payload = verify_full_flow_task_plane(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "compile-simulator-full-flow-logical-routes":
            from .simulator.full_flow_logical_routes import (
                compile_full_flow_logical_routes,
            )

            payload = compile_full_flow_logical_routes(
                args.scenario,
                args.container_plan_dir,
                output_dir=args.output_dir,
                compiler_id=args.compiler_id,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-logical-routes":
            from .simulator.full_flow_logical_routes import (
                verify_full_flow_logical_routes,
            )

            payload = verify_full_flow_logical_routes(
                args.plan_dir,
                args.scenario,
                args.container_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-full-flow-artifact-bindings":
            from .simulator.full_flow_artifact_bindings import (
                build_full_flow_artifact_bindings,
            )

            payload = build_full_flow_artifact_bindings(
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.task_plane_dir,
                args.n3_package_dir,
                args.n4_package_dir,
                binding_set_id=args.binding_set_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-artifact-bindings":
            from .simulator.full_flow_artifact_bindings import (
                verify_full_flow_artifact_bindings,
            )

            payload = verify_full_flow_artifact_bindings(
                args.output_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.task_plane_dir,
                args.n3_package_dir,
                args.n4_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-full-flow-provisioning-catalog":
            from .simulator.full_flow_provisioning_catalog import (
                build_full_flow_provisioning_catalog,
            )

            payload = build_full_flow_provisioning_catalog(
                args.artifact_binding_dir,
                args.n4_package_dir,
                catalog_id=args.catalog_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-provisioning-catalog":
            from .simulator.full_flow_provisioning_catalog import (
                verify_full_flow_provisioning_catalog,
            )

            payload = verify_full_flow_provisioning_catalog(
                args.catalog_dir,
                artifact_binding_dir=args.artifact_binding_dir,
                n4_package_dir=args.n4_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-full-flow-exact-range-catalog":
            from .simulator.full_flow_exact_range_catalog import (
                build_full_flow_exact_range_catalog,
            )

            payload = build_full_flow_exact_range_catalog(
                args.n3_package_dir,
                catalog_id=args.catalog_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-exact-range-catalog":
            from .simulator.full_flow_exact_range_catalog import (
                verify_full_flow_exact_range_catalog,
            )

            payload = verify_full_flow_exact_range_catalog(
                args.catalog_dir,
                args.n3_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "compile-simulator-full-flow-semantic-matrix":
            from .simulator.full_flow_semantic_matrix import (
                compile_full_flow_semantic_matrix,
            )

            payload = compile_full_flow_semantic_matrix(
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.public_task_set,
                args.artifact_bindings,
                output_dir=args.output_dir,
                compiler_id=args.compiler_id,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-semantic-matrix":
            from .simulator.full_flow_semantic_matrix import (
                verify_full_flow_semantic_matrix,
            )

            payload = verify_full_flow_semantic_matrix(
                args.plan_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.public_task_set,
                args.artifact_bindings,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-w4-retrieval-contract"
        ):
            from .simulator.full_flow_w4_retrieval_contract import (
                freeze_full_flow_w4_retrieval_contract,
            )

            payload = freeze_full_flow_w4_retrieval_contract(
                args.semantic_matrix_dir,
                args.retrieval_config,
                args.representation_manifest,
                selected_query_id=args.selected_query_id,
                contract_id=args.contract_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-retrieval-contract"
        ):
            from .simulator.full_flow_w4_retrieval_contract import (
                verify_full_flow_w4_retrieval_contract,
            )

            payload = verify_full_flow_w4_retrieval_contract(
                args.contract_dir
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "evaluate-simulator-full-flow-w4-retrieval":
            from .simulator.full_flow_w4_retrieval_contract import (
                evaluate_full_flow_w4_retrieval,
            )

            payload = evaluate_full_flow_w4_retrieval(
                args.contract_dir,
                args.observations,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-retrieval-evaluation"
        ):
            from .simulator.full_flow_w4_retrieval_contract import (
                verify_full_flow_w4_retrieval_evaluation,
            )

            payload = verify_full_flow_w4_retrieval_evaluation(
                args.output_dir,
                contract_dir=args.contract_dir,
                observations_path=args.observations,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-w4-retrieval-runtime"
        ):
            from .simulator.full_flow_w4_retrieval_runtime import (
                freeze_full_flow_w4_retrieval_runtime_overlay,
            )

            payload = freeze_full_flow_w4_retrieval_runtime_overlay(
                args.contract_dir,
                args.local_semantic_admission_dir,
                runtime_overlay_id=args.runtime_overlay_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-retrieval-runtime"
        ):
            from .simulator.full_flow_w4_retrieval_runtime import (
                verify_full_flow_w4_retrieval_runtime_overlay,
            )

            payload = verify_full_flow_w4_retrieval_runtime_overlay(
                args.output_dir
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-simulator-full-flow-w4-lexical-ranker":
            from .simulator.full_flow_w4_retrieval_runtime import (
                W4LexicalIndexRankingExecutor,
                run_full_flow_w4_retrieval_ranker,
            )
            from .simulator.index_service import (
                N2IndexHTTPClient,
                verify_n2_index_package,
            )

            token = os.environ.get("PATHFINDER_N2_INDEX_TOKEN")
            index = verify_n2_index_package(args.index_package_dir)
            private_hosts = tuple(args.allow_http_simulator_host)
            urls = {
                "N2": args.n2_index_base_url,
                "N7": args.n7_index_base_url,
                "N8": args.n8_index_base_url,
            }
            clients = {
                node_id: N2IndexHTTPClient(
                    base_url=base_url,
                    expected_index_id=index["index_id"],
                    expected_index_sha256=index["index_sha256"],
                    bearer_token=token,
                    timeout_seconds=args.timeout_seconds,
                    simulator_private_http_hosts=private_hosts,
                    expected_node_id=node_id,
                )
                for node_id, base_url in urls.items()
            }
            executor = W4LexicalIndexRankingExecutor(
                clients=clients,
                index_package_dir=args.index_package_dir,
                index_id=index["index_id"],
                index_sha256=index["index_sha256"],
                source_manifest_sha256=index["source_manifest_sha256"],
            )
            payload = run_full_flow_w4_retrieval_ranker(
                args.runtime_overlay_dir,
                run_id=args.run_id,
                executor=executor,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-w4-ranker-run":
            from .simulator.full_flow_w4_retrieval_runtime import (
                verify_full_flow_w4_retrieval_ranker_run,
            )

            payload = verify_full_flow_w4_retrieval_ranker_run(
                args.output_dir,
                runtime_overlay_dir=args.runtime_overlay_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-w4-candidate-routes"
        ):
            from .simulator.full_flow_w4_candidate_routes import (
                freeze_full_flow_w4_candidate_routes,
            )

            payload = freeze_full_flow_w4_candidate_routes(
                args.runtime_overlay_dir,
                args.n3_package_dir,
                args.n4_package_dir,
                args.index_package_dir,
                args.exact_range_catalog_dir,
                physical_plan_id=args.physical_plan_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-candidate-routes"
        ):
            from .simulator.full_flow_w4_candidate_routes import (
                verify_full_flow_w4_candidate_routes,
            )

            payload = verify_full_flow_w4_candidate_routes(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-w4-index-artifact-crosswalk"
        ):
            from .simulator.full_flow_w4_live_executor import (
                freeze_full_flow_w4_index_artifact_crosswalk,
            )

            payload = freeze_full_flow_w4_index_artifact_crosswalk(
                args.route_package_dir,
                args.index_package_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-index-artifact-crosswalk"
        ):
            from .simulator.full_flow_w4_live_executor import (
                verify_full_flow_w4_index_artifact_crosswalk,
            )

            payload = verify_full_flow_w4_index_artifact_crosswalk(
                args.output_dir,
                route_package_dir=args.route_package_dir,
                index_package_dir=args.index_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "run-simulator-full-flow-w4-candidate-conformance"
        ):
            from .simulator.full_flow_w4_candidate_coordinator import (
                DeterministicW4CandidateOperationExecutor,
                run_full_flow_w4_candidate_coordinator,
            )

            payload = run_full_flow_w4_candidate_coordinator(
                args.route_package_dir,
                run_id=args.run_id,
                executor=DeterministicW4CandidateOperationExecutor(),
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-candidate-conformance"
        ):
            from .simulator.full_flow_w4_candidate_coordinator import (
                verify_full_flow_w4_candidate_coordinator_run,
            )

            payload = verify_full_flow_w4_candidate_coordinator_run(
                args.run_dir,
                route_package_dir=args.route_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-w4-component-execution-receipt"
        ):
            from .simulator.full_flow_w4_live_executor import (
                freeze_full_flow_w4_component_execution_receipt,
            )

            payload = freeze_full_flow_w4_component_execution_receipt(
                args.coordinator_run_dir,
                route_package_dir=args.route_package_dir,
                crosswalk_dir=args.crosswalk_dir,
                index_package_dir=args.index_package_dir,
                component_events_path=args.component_events,
                evidence_class=args.evidence_class,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-component-execution-receipt"
        ):
            from .simulator.full_flow_w4_live_executor import (
                verify_full_flow_w4_component_execution_receipt,
            )

            payload = verify_full_flow_w4_component_execution_receipt(
                args.output_dir,
                coordinator_run_dir=args.coordinator_run_dir,
                route_package_dir=args.route_package_dir,
                crosswalk_dir=args.crosswalk_dir,
                index_package_dir=args.index_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "run-simulator-full-flow-w4-local-component-execution"
        ):
            from .simulator.full_flow_w4_local_factory import (
                W4LocalRuntimeInputs,
            )
            from .simulator.full_flow_w4_local_run import (
                run_full_flow_w4_local_component_execution,
            )

            credential_environment = {
                "N2 index": ("PATHFINDER_N2_INDEX_TOKEN",),
                "N7 index": (
                    "PATHFINDER_N2_INDEX_TOKEN",
                    "PATHFINDER_N7_INDEX_TOKEN",
                ),
                "N8 index": (
                    "PATHFINDER_N2_INDEX_TOKEN",
                    "PATHFINDER_N8_INDEX_TOKEN",
                ),
                "N3 Data Agent": (
                    "PATHFINDER_DATA_AGENT_TOKEN",
                    "PATHFINDER_N3_DATA_AGENT_TOKEN",
                ),
                "N4 Data Agent": (
                    "PATHFINDER_DATA_AGENT_TOKEN",
                    "PATHFINDER_N4_DATA_AGENT_TOKEN",
                ),
                "N7 cache": (
                    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
                ),
                "N8 cache": (
                    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
                ),
                "N6 semantic": ("PATHFINDER_CONTAINER_NODE_TOKEN",),
            }
            credentials = {
                name: next(
                    (
                        os.environ[environment_name]
                        for environment_name in environment_names
                        if os.environ.get(environment_name)
                    ),
                    None,
                )
                for name, environment_names in credential_environment.items()
            }
            missing = [
                "/".join(credential_environment[name])
                for name, value in credentials.items()
                if not value
            ]
            if missing:
                raise ConfigError(
                    "local W4 component execution requires runtime "
                    "credential environment variables: "
                    + ", ".join(sorted(missing))
                )
            private_hosts = tuple(
                value.strip()
                for value in args.simulator_private_http_hosts.split(",")
                if value.strip()
            )
            runtime = W4LocalRuntimeInputs(
                index_base_urls={
                    "N2": args.n2_index_base_url,
                    "N7": args.n7_index_base_url,
                    "N8": args.n8_index_base_url,
                },
                index_bearer_tokens={
                    "N2": credentials["N2 index"],
                    "N7": credentials["N7 index"],
                    "N8": credentials["N8 index"],
                },
                index_package_dirs={
                    "N2": args.n2_index_package_dir,
                    "N7": args.n7_index_package_dir,
                    "N8": args.n8_index_package_dir,
                },
                data_agent_base_urls={
                    "N3": args.n3_data_agent_base_url,
                    "N4": args.n4_data_agent_base_url,
                },
                data_agent_bearer_tokens={
                    "N3": credentials["N3 Data Agent"],
                    "N4": credentials["N4 Data Agent"],
                },
                data_agent_locations={
                    "N3": args.n3_data_agent_location,
                    "N4": args.n4_data_agent_location,
                },
                cache_base_urls={
                    "N7": args.n7_cache_base_url,
                    "N8": args.n8_cache_base_url,
                },
                cache_bearer_tokens={
                    "N7": credentials["N7 cache"],
                    "N8": credentials["N8 cache"],
                },
                cache_ids={
                    "N7": args.n7_cache_id,
                    "N8": args.n8_cache_id,
                },
                n6_base_url=args.n6_base_url,
                n6_bearer_token=credentials["N6 semantic"],
                semantic_model=args.semantic_model,
                raw_sampler_scratch_dir=args.raw_sampler_scratch_dir,
                timeout_seconds=args.timeout_seconds,
                simulator_private_http_hosts=private_hosts,
            )
            payload = run_full_flow_w4_local_component_execution(
                route_package_dir=args.route_package_dir,
                crosswalk_dir=args.crosswalk_dir,
                runtime=runtime,
                run_id=args.run_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-w4-flowmesh-plan"
        ):
            from .integrations.flowmesh.w4_candidate_matrix import (
                plan_flowmesh_w4_candidate_matrix,
            )

            payload = plan_flowmesh_w4_candidate_matrix(
                route_package_dir=args.route_package_dir,
                run_id=args.run_id,
                worker_alias=args.worker_alias,
                owner=args.owner,
                api_task_timeout_seconds=args.api_task_timeout_seconds,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-flowmesh-plan"
        ):
            from .integrations.flowmesh.w4_candidate_matrix import (
                verify_flowmesh_w4_candidate_matrix_plan,
            )

            payload = verify_flowmesh_w4_candidate_matrix_plan(
                args.plan_dir,
                route_package_dir=args.route_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "serve-simulator-full-flow-w4-flowmesh-coordinator"
        ):
            from .simulator.full_flow_w4_flowmesh_service import (
                build_local_full_flow_w4_flowmesh_coordinator,
                create_full_flow_w4_flowmesh_http_server,
            )
            from .simulator.full_flow_w4_local_factory import (
                W4LocalRuntimeInputs,
            )

            credential_environment = {
                "N2 index": ("PATHFINDER_N2_INDEX_TOKEN",),
                "N7 index": (
                    "PATHFINDER_N2_INDEX_TOKEN",
                    "PATHFINDER_N7_INDEX_TOKEN",
                ),
                "N8 index": (
                    "PATHFINDER_N2_INDEX_TOKEN",
                    "PATHFINDER_N8_INDEX_TOKEN",
                ),
                "N3 Data Agent": (
                    "PATHFINDER_DATA_AGENT_TOKEN",
                    "PATHFINDER_N3_DATA_AGENT_TOKEN",
                ),
                "N4 Data Agent": (
                    "PATHFINDER_DATA_AGENT_TOKEN",
                    "PATHFINDER_N4_DATA_AGENT_TOKEN",
                ),
                "N7 cache": (
                    "PATHFINDER_N7_W4_CACHE_TOKEN",
                    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
                ),
                "N8 cache": (
                    "PATHFINDER_N8_W4_CACHE_TOKEN",
                    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
                ),
                "N6 semantic": ("PATHFINDER_CONTAINER_NODE_TOKEN",),
                "W4 ingress": (
                    "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
                ),
            }
            credentials = {
                name: next(
                    (
                        os.environ[environment_name]
                        for environment_name in environment_names
                        if os.environ.get(environment_name)
                    ),
                    None,
                )
                for name, environment_names in credential_environment.items()
            }
            missing = [
                "/".join(credential_environment[name])
                for name, value in credentials.items()
                if not value
            ]
            if missing:
                raise ConfigError(
                    "W4 FlowMesh coordinator requires runtime credential "
                    "environment variables: " + ", ".join(sorted(missing))
                )
            private_hosts = tuple(
                value.strip()
                for value in args.simulator_private_http_hosts.split(",")
                if value.strip()
            )
            runtime = W4LocalRuntimeInputs(
                index_base_urls={
                    "N2": args.n2_index_base_url,
                    "N7": args.n7_index_base_url,
                    "N8": args.n8_index_base_url,
                },
                index_bearer_tokens={
                    "N2": credentials["N2 index"],
                    "N7": credentials["N7 index"],
                    "N8": credentials["N8 index"],
                },
                index_package_dirs={
                    "N2": args.n2_index_package_dir,
                    "N7": args.n7_index_package_dir,
                    "N8": args.n8_index_package_dir,
                },
                data_agent_base_urls={
                    "N3": args.n3_data_agent_base_url,
                    "N4": args.n4_data_agent_base_url,
                },
                data_agent_bearer_tokens={
                    "N3": credentials["N3 Data Agent"],
                    "N4": credentials["N4 Data Agent"],
                },
                data_agent_locations={
                    "N3": args.n3_data_agent_location,
                    "N4": args.n4_data_agent_location,
                },
                cache_base_urls={
                    "N7": args.n7_cache_base_url,
                    "N8": args.n8_cache_base_url,
                },
                cache_bearer_tokens={
                    "N7": credentials["N7 cache"],
                    "N8": credentials["N8 cache"],
                },
                cache_ids={
                    "N7": args.n7_cache_id,
                    "N8": args.n8_cache_id,
                },
                n6_base_url=args.n6_base_url,
                n6_bearer_token=credentials["N6 semantic"],
                semantic_model=args.semantic_model,
                raw_sampler_scratch_dir=args.raw_sampler_scratch_dir,
                timeout_seconds=args.timeout_seconds,
                max_artifact_bytes=args.max_artifact_bytes,
                simulator_private_http_hosts=private_hosts,
            )
            coordinator = build_local_full_flow_w4_flowmesh_coordinator(
                coordinator_node_id=args.coordinator_node_id,
                route_package_dir=args.route_package_dir,
                crosswalk_dir=args.crosswalk_dir,
                runtime=runtime,
                state_db=args.state_db,
            )
            server = create_full_flow_w4_flowmesh_http_server(
                coordinator,
                host=args.host,
                port=args.port,
                hmac_secret=credentials["W4 ingress"],
            )
            _print_payload(coordinator.health(), compact=args.compact)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0
        if args.command == "run-simulator-full-flow-w4-flowmesh-matrix":
            from .integrations.flowmesh import (
                FlowMeshSettings,
                SdkFlowMeshClient,
            )
            from .integrations.flowmesh.w4_candidate_matrix import (
                full_flow_w4_hmac_header_provider,
                run_flowmesh_w4_candidate_matrix,
            )

            ingress_secret = os.environ.get(
                "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"
            )
            if not ingress_secret:
                raise ConfigError(
                    "W4 FlowMesh run requires "
                    "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"
                )
            private_hosts = tuple(
                value.strip()
                for value in args.simulator_private_http_hosts.split(",")
                if value.strip()
            )
            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                poll_interval_seconds=args.poll_interval,
                worker_alias=args.worker_alias,
                validate_before_submit=True,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = run_flowmesh_w4_candidate_matrix(
                    plan_dir=args.plan_dir,
                    route_package_dir=args.route_package_dir,
                    crosswalk_dir=args.crosswalk_dir,
                    index_package_dir=args.index_package_dir,
                    output_dir=args.output_dir,
                    coordinator_base_urls={
                        "N7": args.n7_coordinator_base_url,
                        "N8": args.n8_coordinator_base_url,
                    },
                    runtime_header_provider=(
                        full_flow_w4_hmac_header_provider(ingress_secret)
                    ),
                    client=client,
                    settings=settings,
                    simulator_private_http_hosts=private_hosts,
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-w4-flowmesh-matrix"
        ):
            from .integrations.flowmesh.w4_candidate_matrix import (
                verify_flowmesh_w4_candidate_matrix_run,
            )

            payload = verify_flowmesh_w4_candidate_matrix_run(
                args.run_dir,
                plan_dir=args.plan_dir,
                route_package_dir=args.route_package_dir,
                crosswalk_dir=args.crosswalk_dir,
                index_package_dir=args.index_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-semantic-execution-admission"
        ):
            from .simulator.full_flow_semantic_execution_admission import (
                freeze_full_flow_semantic_execution_admission,
            )

            payload = freeze_full_flow_semantic_execution_admission(
                args.semantic_matrix_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.public_task_set,
                args.artifact_bindings,
                args.n1_oracle_package_dir,
                worker_alias=args.worker_alias,
                admission_id=args.admission_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-semantic-execution-admission"
        ):
            from .simulator.full_flow_semantic_execution_admission import (
                verify_full_flow_semantic_execution_admission,
            )

            payload = verify_full_flow_semantic_execution_admission(
                args.admission_dir,
                args.semantic_matrix_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.public_task_set,
                args.artifact_bindings,
                args.n1_oracle_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "preflight-simulator-full-flow-semantic-artifacts"
        ):
            from .simulator.full_flow_artifact_preflight import (
                preflight_full_flow_semantic_artifacts_over_http,
            )

            shared_token = os.environ.get("PATHFINDER_DATA_AGENT_TOKEN")
            n3_token = (
                os.environ.get("PATHFINDER_N3_DATA_AGENT_TOKEN")
                or shared_token
            )
            n4_token = (
                os.environ.get("PATHFINDER_N4_DATA_AGENT_TOKEN")
                or shared_token
            )
            if not n3_token or not n4_token:
                raise ConfigError(
                    "node-specific PATHFINDER_N3_DATA_AGENT_TOKEN and "
                    "PATHFINDER_N4_DATA_AGENT_TOKEN, or the shared "
                    "PATHFINDER_DATA_AGENT_TOKEN, are required"
                )
            payload = preflight_full_flow_semantic_artifacts_over_http(
                args.semantic_execution_admission_dir,
                args.n3_package_dir,
                args.n4_package_dir,
                n3_base_url=args.n3_data_agent_url,
                n4_base_url=args.n4_data_agent_url,
                n3_token=n3_token,
                n4_token=n4_token,
                preflight_id=args.preflight_id,
                output_dir=args.output_dir,
                timeout_seconds=args.timeout_seconds,
                max_retries=args.max_retries,
                max_artifact_bytes=args.max_artifact_bytes,
                telemetry_quiescence_timeout_seconds=(
                    args.telemetry_quiescence_timeout_seconds
                ),
                simulator_private_http_hosts=tuple(
                    args.simulator_private_http_host
                ),
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-semantic-artifact-preflight"
        ):
            from .simulator.full_flow_artifact_preflight import (
                verify_full_flow_semantic_artifact_preflight,
            )

            payload = verify_full_flow_semantic_artifact_preflight(
                args.preflight_dir,
                semantic_execution_admission_dir=(
                    args.semantic_execution_admission_dir
                ),
                n3_package_dir=args.n3_package_dir,
                n4_package_dir=args.n4_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "promote-simulator-full-flow-local-semantic-execution-admission"
        ):
            from .simulator.full_flow_local_semantic_admission import (
                promote_full_flow_local_semantic_execution_admission,
            )

            payload = promote_full_flow_local_semantic_execution_admission(
                args.legacy_admission_dir,
                args.semantic_matrix_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.public_task_set,
                args.artifact_bindings,
                args.n1_oracle_package_dir,
                args.artifact_preflight_dir,
                args.exact_range_catalog_dir,
                args.n3_package_dir,
                args.provisioning_catalog_dir,
                args.n4_package_dir,
                semantics_mode=args.semantics_mode,
                promotion_id=args.promotion_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-local-semantic-execution-admission"
        ):
            from .simulator.full_flow_local_semantic_admission import (
                verify_full_flow_local_semantic_execution_admission,
            )

            payload = verify_full_flow_local_semantic_execution_admission(
                args.admission_dir,
                args.legacy_admission_dir,
                args.semantic_matrix_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.public_task_set,
                args.artifact_bindings,
                args.n1_oracle_package_dir,
                args.artifact_preflight_dir,
                args.exact_range_catalog_dir,
                args.n3_package_dir,
                args.provisioning_catalog_dir,
                args.n4_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-local-semantic-runtime-package"
        ):
            from .simulator.full_flow_local_semantic_admission import (
                verify_full_flow_local_semantic_runtime_package,
            )

            payload = verify_full_flow_local_semantic_runtime_package(
                args.admission_dir
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "build-simulator-full-flow-index-query-plan-catalog"
        ):
            from .simulator.full_flow_index_query_plan_catalog import (
                build_full_flow_index_query_plan_catalog,
            )

            payload = build_full_flow_index_query_plan_catalog(
                args.local_semantic_admission_dir,
                args.n2_index_package_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-index-query-plan-catalog"
        ):
            from .simulator.full_flow_index_query_plan_catalog import (
                verify_full_flow_index_query_plan_catalog,
            )

            payload = verify_full_flow_index_query_plan_catalog(
                args.catalog_dir,
                local_semantic_admission_dir=(
                    args.local_semantic_admission_dir
                ),
                n2_index_package_dir=args.n2_index_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-n4-preprovisioned-serve-gate"
        ):
            from .simulator.full_flow_n4_serve_gate import (
                freeze_full_flow_n4_preprovisioned_serve_gate,
            )

            payload = freeze_full_flow_n4_preprovisioned_serve_gate(
                args.compose_overlay_dir,
                args.service_bootstrap_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.provisioning_catalog_dir,
                args.artifact_binding_dir,
                args.n4_package_dir,
                gate_id=args.gate_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-n4-preprovisioned-serve-gate"
        ):
            from .simulator.full_flow_n4_serve_gate import (
                verify_full_flow_n4_preprovisioned_serve_gate,
            )

            payload = verify_full_flow_n4_preprovisioned_serve_gate(
                args.gate_dir,
                args.compose_overlay_dir,
                args.service_bootstrap_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.provisioning_catalog_dir,
                args.artifact_binding_dir,
                args.n4_package_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command in {
            "freeze-simulator-full-flow-n4-live-serve-gate",
            "verify-simulator-full-flow-n4-live-serve-gate",
        }:
            live_receipt_bindings = _load_strict_json_file(
                args.live_receipt_bindings,
                label="live receipt bindings",
                expected_type=list,
            )
            if args.command.startswith("freeze-"):
                from .simulator.full_flow_n4_live_serve_gate import (
                    freeze_full_flow_n4_live_serve_gate,
                )

                payload = freeze_full_flow_n4_live_serve_gate(
                    live_receipt_bindings,
                    args.n4_publication_store_root,
                    args.rebound_artifact_binding_dir,
                    args.rebound_semantic_matrix_dir,
                    args.rebound_admission_dir,
                    gate_id=args.gate_id,
                    output_dir=args.output_dir,
                )
            else:
                from .simulator.full_flow_n4_live_serve_gate import (
                    verify_full_flow_n4_live_serve_gate,
                )

                payload = verify_full_flow_n4_live_serve_gate(
                    args.gate_dir,
                    live_receipt_bindings,
                    args.n4_publication_store_root,
                    args.rebound_artifact_binding_dir,
                    args.rebound_semantic_matrix_dir,
                    args.rebound_admission_dir,
                )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-simulator-full-flow-local-semantic-smokes":
            from .simulator.full_flow_local_semantic_smoke import (
                run_full_flow_local_semantic_smokes,
            )

            n4_live_gate_sources = _load_n4_live_gate_sources(
                args.n4_live_gate_sources
            )
            with _local_semantic_flowmesh_executor(
                args.local_semantic_admission_dir,
                run_id=args.run_id,
                flowmesh_base_url=args.flowmesh_base_url,
                task_timeout_seconds=args.task_timeout,
                poll_interval_seconds=args.poll_interval,
            ) as executor:
                payload = run_full_flow_local_semantic_smokes(
                    args.local_semantic_admission_dir,
                    args.n4_serve_gate_dir,
                    args.compose_overlay_dir,
                    args.service_bootstrap_dir,
                    args.deployment_binding_dir,
                    args.logical_plan_dir,
                    args.scenario,
                    args.container_plan_dir,
                    args.provisioning_catalog_dir,
                    args.artifact_binding_dir,
                    args.n4_package_dir,
                    run_id=args.run_id,
                    executor=executor,
                    output_dir=args.output_dir,
                    n4_live_gate_sources=n4_live_gate_sources,
                )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-local-semantic-smokes":
            from .simulator.full_flow_local_semantic_smoke import (
                verify_full_flow_local_semantic_smokes,
            )

            n4_live_gate_sources = _load_n4_live_gate_sources(
                args.n4_live_gate_sources
            )
            payload = verify_full_flow_local_semantic_smokes(
                args.smoke_dir,
                local_semantic_admission_dir=(
                    args.local_semantic_admission_dir
                ),
                n4_serve_gate_dir=args.n4_serve_gate_dir,
                compose_overlay_dir=args.compose_overlay_dir,
                service_bootstrap_dir=args.service_bootstrap_dir,
                deployment_binding_dir=args.deployment_binding_dir,
                logical_route_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
                provisioning_catalog_dir=args.provisioning_catalog_dir,
                artifact_binding_dir=args.artifact_binding_dir,
                n4_package_dir=args.n4_package_dir,
                n4_live_gate_sources=n4_live_gate_sources,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-simulator-full-flow-local-semantic-matrix":
            from .simulator.full_flow_local_semantic_matrix_gate import (
                run_smoke_gated_full_flow_local_semantic_matrix,
            )

            n4_live_gate_sources = _load_n4_live_gate_sources(
                args.n4_live_gate_sources
            )
            with _local_semantic_flowmesh_executor(
                args.local_semantic_admission_dir,
                run_id=args.run_id,
                flowmesh_base_url=args.flowmesh_base_url,
                task_timeout_seconds=args.task_timeout,
                poll_interval_seconds=args.poll_interval,
            ) as executor:
                payload = run_smoke_gated_full_flow_local_semantic_matrix(
                    args.local_semantic_admission_dir,
                    args.smoke_dir,
                    args.n4_serve_gate_dir,
                    args.compose_overlay_dir,
                    args.service_bootstrap_dir,
                    args.provisioning_catalog_dir,
                    args.artifact_binding_dir,
                    args.n4_package_dir,
                    args.semantic_matrix_dir,
                    args.deployment_binding_dir,
                    args.logical_plan_dir,
                    args.scenario,
                    args.container_plan_dir,
                    args.public_task_set,
                    args.artifact_bindings,
                    run_id=args.run_id,
                    output_dir=args.output_dir,
                    executor=executor,
                    acknowledge_failed_entry_sha256=(
                        args.acknowledge_failed_entry_sha256
                    ),
                    n4_live_gate_sources=n4_live_gate_sources,
                )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-local-semantic-matrix":
            from .simulator.full_flow_local_semantic_matrix_gate import (
                verify_smoke_gated_full_flow_local_semantic_matrix_run,
            )

            n4_live_gate_sources = _load_n4_live_gate_sources(
                args.n4_live_gate_sources
            )
            payload = verify_smoke_gated_full_flow_local_semantic_matrix_run(
                args.local_semantic_admission_dir,
                args.smoke_dir,
                args.n4_serve_gate_dir,
                args.compose_overlay_dir,
                args.service_bootstrap_dir,
                args.provisioning_catalog_dir,
                args.artifact_binding_dir,
                args.n4_package_dir,
                args.semantic_matrix_dir,
                args.deployment_binding_dir,
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.public_task_set,
                args.artifact_bindings,
                output_dir=args.output_dir,
                n4_live_gate_sources=n4_live_gate_sources,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "serve-simulator-full-flow-semantic-route":
            from .simulator.container_node import serve_container_node
            from .simulator.full_flow_semantic_route_service_factory import (
                FrozenSemanticRouteServiceSources,
                RuntimeSemanticServiceInputs,
                assemble_full_flow_semantic_route_service,
            )

            credential_environment = {
                "N2 index": ("PATHFINDER_N2_INDEX_TOKEN",),
                "N7 index": (
                    "PATHFINDER_N2_INDEX_TOKEN",
                    "PATHFINDER_N7_INDEX_TOKEN",
                ),
                "N8 index": (
                    "PATHFINDER_N2_INDEX_TOKEN",
                    "PATHFINDER_N8_INDEX_TOKEN",
                ),
                "N3 Data Agent": (
                    "PATHFINDER_DATA_AGENT_TOKEN",
                    "PATHFINDER_N3_DATA_AGENT_TOKEN",
                ),
                "N4 Data Agent": (
                    "PATHFINDER_DATA_AGENT_TOKEN",
                    "PATHFINDER_N4_DATA_AGENT_TOKEN",
                ),
                "N7 cache": (
                    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
                ),
                "N8 cache": (
                    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
                ),
                "N6 semantic": ("PATHFINDER_CONTAINER_NODE_TOKEN",),
                "N1 score": ("PATHFINDER_N1_ORACLE_TOKEN",),
                "N1 verifier": ("PATHFINDER_N1_VERIFICATION_TOKEN",),
                "route ingress": (
                    "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
                ),
            }
            credentials = {
                name: next(
                    (
                        value
                        for environment_name in environment_names
                        if (value := os.environ.get(environment_name))
                    ),
                    None,
                )
                for name, environment_names in credential_environment.items()
            }
            missing = sorted({
                " or ".join(credential_environment[name])
                for name, value in credentials.items()
                if not value
            })
            if missing:
                raise ConfigError(
                    "semantic route service requires runtime-only "
                    "credential environment variables: "
                    + ", ".join(missing)
                )
            private_hosts = tuple(
                value.strip()
                for value in args.simulator_private_http_hosts.split(",")
                if value.strip()
            )
            sources = FrozenSemanticRouteServiceSources(
                local_admission_dir=args.local_semantic_admission_dir,
                n1_public_commitment_dir=args.n1_public_commitment_dir,
                artifact_binding_dir=args.artifact_binding_dir,
                n2_index_package_dir=args.n2_index_package_dir,
                n3_package_dir=args.n3_package_dir,
                n4_package_dir=args.n4_package_dir,
                exact_range_catalog_dir=args.exact_range_catalog_dir,
                provisioning_catalog_dir=args.provisioning_catalog_dir,
                index_query_plan_catalog_dir=(
                    args.index_query_plan_catalog_dir
                ),
            )
            runtime = RuntimeSemanticServiceInputs(
                logical_node_id=args.node_id,
                index_base_urls={
                    "N2": args.n2_index_base_url,
                    "N7": args.n7_index_base_url,
                    "N8": args.n8_index_base_url,
                },
                index_bearer_tokens={
                    "N2": credentials["N2 index"],
                    "N7": credentials["N7 index"],
                    "N8": credentials["N8 index"],
                },
                data_agent_base_urls={
                    "N3": args.n3_data_agent_base_url,
                    "N4": args.n4_data_agent_base_url,
                },
                data_agent_bearer_tokens={
                    "N3": credentials["N3 Data Agent"],
                    "N4": credentials["N4 Data Agent"],
                },
                cache_base_urls={
                    "N7": args.n7_cache_base_url,
                    "N8": args.n8_cache_base_url,
                },
                cache_bearer_tokens={
                    "N7": credentials["N7 cache"],
                    "N8": credentials["N8 cache"],
                },
                cache_ids={
                    "N7": args.n7_cache_id,
                    "N8": args.n8_cache_id,
                },
                node_health_base_urls={
                    "N7": args.n7_node_health_base_url,
                    "N8": args.n8_node_health_base_url,
                },
                n6_base_url=args.n6_base_url,
                n6_bearer_token=credentials["N6 semantic"],
                n1_base_url=args.n1_base_url,
                n1_bearer_token=credentials["N1 score"],
                n1_verification_base_url=args.n1_verification_base_url,
                n1_verification_bearer_token=credentials["N1 verifier"],
                semantic_model=args.semantic_model,
                timeout_seconds=args.timeout_seconds,
                max_artifact_bytes=args.max_artifact_bytes,
                simulator_private_http_hosts=private_hosts,
            )
            assembly = assemble_full_flow_semantic_route_service(
                sources,
                runtime,
                state_dir=args.state_dir,
            )
            assembly.require_ready()
            serve_container_node(
                args.node_id,
                args.state_dir,
                host=args.host,
                port=args.port,
                semantic_route_handler=assembly.handler,
            )
            return 0
        if args.command == "freeze-simulator-full-flow-service-bootstrap":
            from .simulator.full_flow_service_bootstrap import (
                freeze_full_flow_local_service_bootstrap,
            )

            payload = freeze_full_flow_local_service_bootstrap(
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                bootstrap_id=args.bootstrap_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-service-bootstrap":
            from .simulator.full_flow_service_bootstrap import (
                verify_full_flow_local_service_bootstrap,
            )

            payload = verify_full_flow_local_service_bootstrap(
                args.bootstrap_dir,
                logical_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-full-flow-deployment":
            from .simulator.full_flow_deployment import (
                build_full_flow_deployment_binding,
            )

            payload = build_full_flow_deployment_binding(
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                args.deployment_source,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-deployment":
            from .simulator.full_flow_deployment import (
                verify_full_flow_deployment_binding,
            )

            payload = verify_full_flow_deployment_binding(
                args.binding_dir,
                logical_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "preflight-simulator-full-flow-deployment":
            from .simulator.full_flow_deployment import (
                preflight_full_flow_deployment,
            )

            payload = preflight_full_flow_deployment(
                args.binding_dir,
                logical_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
                timeout_seconds=args.timeout_seconds,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "render-simulator-full-flow-compose-overlay":
            from .simulator.full_flow_compose_overlay import (
                render_full_flow_local_compose_overlay,
            )

            payload = render_full_flow_local_compose_overlay(
                args.service_bootstrap_dir,
                args.deployment_binding_dir,
                logical_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
                overlay_id=args.overlay_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-compose-overlay":
            from .simulator.full_flow_compose_overlay import (
                verify_full_flow_local_compose_overlay,
            )

            payload = verify_full_flow_local_compose_overlay(
                args.overlay_dir,
                service_bootstrap_dir=args.service_bootstrap_dir,
                deployment_binding_dir=args.deployment_binding_dir,
                logical_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "generate-simulator-full-flow-deployment-template"
        ):
            from .simulator.full_flow_deployment_template import (
                generate_full_flow_deployment_source_template,
            )

            payload = generate_full_flow_deployment_source_template(
                args.logical_plan_dir,
                args.scenario,
                args.container_plan_dir,
                template_id=args.template_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-deployment-template"
        ):
            from .simulator.full_flow_deployment_template import (
                verify_full_flow_deployment_source_template,
            )

            payload = verify_full_flow_deployment_source_template(
                args.template_dir,
                logical_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "validate-simulator-full-flow-deployment-source"
        ):
            from .simulator.full_flow_deployment_template import (
                validate_completed_full_flow_deployment_source,
            )

            payload = validate_completed_full_flow_deployment_source(
                args.deployment_source,
                template_dir=args.template_dir,
                logical_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-n3-raw-data-plane":
            from .simulator.raw_cold_data_plane import (
                build_raw_cold_data_plane_package_from_manifest,
            )

            payload = build_raw_cold_data_plane_package_from_manifest(
                args.binding_manifest,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-n3-raw-data-plane":
            from .simulator.raw_cold_data_plane import (
                verify_raw_cold_data_plane_package,
            )

            payload = verify_raw_cold_data_plane_package(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-simulator-n4-derived-data-plane":
            from .simulator.n4_derived_data_plane import (
                build_n4_derived_data_package_from_manifest,
            )

            payload = build_n4_derived_data_package_from_manifest(
                args.binding_manifest,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-n4-derived-data-plane":
            from .simulator.n4_derived_data_plane import (
                verify_n4_derived_data_package,
            )

            payload = verify_n4_derived_data_package(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-n2-index":
            from .simulator.index_service import verify_n2_index_package

            payload = verify_n2_index_package(args.output_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "serve-simulator-n2-index":
            from .simulator.index_service import create_n2_index_http_server

            token = os.environ.get("PATHFINDER_N2_INDEX_TOKEN")
            if args.require_token and not token:
                raise ConfigError(
                    "PATHFINDER_N2_INDEX_TOKEN is required for this service"
                )
            server = create_n2_index_http_server(
                args.package_dir,
                host=args.host,
                port=args.port,
                bearer_token=token,
                node_id=args.node_id,
            )
            try:
                server.serve_forever(poll_interval=0.2)
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0
        if args.command == "serve-simulator-full-flow-cache":
            from .simulator.full_flow_cache import serve_full_flow_cache

            token_names = [args.token_env_name]
            if args.fallback_token_env_name:
                token_names.append(args.fallback_token_env_name)
            token = next(
                (
                    os.environ[name]
                    for name in token_names
                    if os.environ.get(name)
                ),
                None,
            )
            if not token:
                raise ConfigError(
                    "/".join(token_names)
                    + " is required for this service"
                )
            serve_full_flow_cache(
                args.state_dir,
                node_id=args.node_id,
                cache_id=args.cache_id,
                capacity_bytes=args.capacity_bytes,
                token=token,
                host=args.host,
                port=args.port,
                max_artifact_bytes=args.max_artifact_bytes,
            )
            return 0
        if args.command == "serve-simulator-n5-materializer":
            from .simulator.n5_materialization import (
                N5MaterializationHttpServer,
                N5MaterializationHttpService,
                N5MaterializationRuntime,
            )

            token = os.environ.get("PATHFINDER_N5_MATERIALIZATION_TOKEN")
            if not token:
                raise ConfigError(
                    "PATHFINDER_N5_MATERIALIZATION_TOKEN is required for "
                    "this service"
                )
            service = N5MaterializationHttpService(
                runtime=N5MaterializationRuntime(),
                bearer_token=token,
                state_dir=args.state_dir,
            )
            server = N5MaterializationHttpServer(
                (args.host, args.port),
                service,
            )
            try:
                server.serve_forever(poll_interval=0.2)
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0
        if args.command == "run-simulator-n5-n4-live-frame-bundle-smoke":
            from .simulator.full_flow_live_provisioning_smoke import (
                N4PublicationHttpClientConfig,
                run_n5_n4_live_frame_bundle_provisioning_smoke,
            )
            from .simulator.n5_materialization import (
                N5MaterializationHttpClientConfig,
            )

            n5_token = os.environ.get(
                "PATHFINDER_N5_MATERIALIZATION_TOKEN"
            )
            n4_token = os.environ.get("PATHFINDER_N4_PUBLICATION_TOKEN")
            if not n5_token or not n4_token:
                raise ConfigError(
                    "PATHFINDER_N5_MATERIALIZATION_TOKEN and "
                    "PATHFINDER_N4_PUBLICATION_TOKEN are required"
                )
            n5_plan = _load_strict_json_file(
                args.n5_plan,
                label="N5 frame-bundle plan",
                expected_type=dict,
            )
            private_hosts = tuple(args.allow_http_simulator_host)
            payload = run_n5_n4_live_frame_bundle_provisioning_smoke(
                n5_plan,
                args.source_video.read_bytes(),
                n5_config=N5MaterializationHttpClientConfig(
                    base_url=args.n5_base_url,
                    bearer_token=n5_token,
                    simulator_private_http_hosts=private_hosts,
                    timeout_seconds=args.timeout_seconds,
                ),
                n4_config=N4PublicationHttpClientConfig(
                    base_url=args.n4_base_url,
                    bearer_token=n4_token,
                    simulator_private_http_hosts=private_hosts,
                    timeout_seconds=args.timeout_seconds,
                ),
                smoke_id=args.smoke_id,
                publication_id=args.publication_id,
                package_id=args.package_id,
                catalog_version=args.catalog_version,
                expected_current_catalog_version=(
                    args.expected_current_catalog_version
                ),
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-n5-n4-live-frame-bundle-smoke":
            from .simulator.full_flow_live_provisioning_smoke import (
                verify_n5_n4_live_frame_bundle_provisioning_smoke,
            )

            n5_plan = _load_strict_json_file(
                args.n5_plan,
                label="N5 frame-bundle plan",
                expected_type=dict,
            )
            payload = verify_n5_n4_live_frame_bundle_provisioning_smoke(
                args.output_dir,
                n5_plan=n5_plan,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-simulator-n5-n4-live-digest-smoke":
            from .simulator.full_flow_live_provisioning_smoke import (
                HttpN5DigestMaterializationExecutor,
                N4PublicationHttpClientConfig,
                N5DigestHttpClientConfig,
                run_n5_n4_live_multimodal_digest_provisioning_smoke,
            )

            n5_token = os.environ.get("PATHFINDER_N5_DIGEST_TOKEN")
            n4_token = os.environ.get("PATHFINDER_N4_PUBLICATION_TOKEN")
            if not n5_token or not n4_token:
                raise ConfigError(
                    "PATHFINDER_N5_DIGEST_TOKEN and "
                    "PATHFINDER_N4_PUBLICATION_TOKEN are required"
                )
            private_hosts = tuple(args.allow_http_simulator_host)
            executor = HttpN5DigestMaterializationExecutor(
                N5DigestHttpClientConfig(
                    base_url=args.n5_digest_base_url,
                    bearer_token=n5_token,
                    simulator_private_http_hosts=private_hosts,
                    timeout_seconds=args.timeout_seconds,
                )
            )
            payload = run_n5_n4_live_multimodal_digest_provisioning_smoke(
                args.n5_digest_plan_dir,
                args.source_video,
                n5_executor=executor,
                n4_config=N4PublicationHttpClientConfig(
                    base_url=args.n4_base_url,
                    bearer_token=n4_token,
                    simulator_private_http_hosts=private_hosts,
                    timeout_seconds=args.timeout_seconds,
                ),
                smoke_id=args.smoke_id,
                request_id=args.request_id,
                publication_id=args.publication_id,
                package_id=args.package_id,
                catalog_version=args.catalog_version,
                expected_current_catalog_version=(
                    args.expected_current_catalog_version
                ),
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-n5-n4-live-digest-smoke":
            from .simulator.full_flow_live_provisioning_smoke import (
                verify_n5_n4_live_multimodal_digest_provisioning_smoke,
            )

            payload = verify_n5_n4_live_multimodal_digest_provisioning_smoke(
                args.output_dir,
                n5_digest_plan_dir=args.n5_digest_plan_dir,
                source_video_path=args.source_video,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "freeze-simulator-full-flow-bulk-provisioning-source-manifest"
        ):
            from .simulator.full_flow_bulk_live_provisioning import (
                freeze_full_flow_bulk_live_provisioning_source_manifest,
            )

            payload = freeze_full_flow_bulk_live_provisioning_source_manifest(
                args.provisioning_catalog_dir,
                args.artifact_binding_dir,
                args.n4_package_dir,
                args.object_mapping,
                output_path=args.output_path,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "run-simulator-full-flow-bulk-live-provisioning"
        ):
            from .simulator.full_flow_bulk_live_provisioning import (
                ExistingDigestBulkExecutor,
                ExistingFrameBundleBulkExecutor,
                run_full_flow_bulk_live_provisioning,
            )
            from .simulator.full_flow_live_provisioning_smoke import (
                HttpN5DigestMaterializationExecutor,
                N4PublicationHttpClientConfig,
                N5DigestHttpClientConfig,
            )
            from .simulator.n5_materialization import (
                N5MaterializationHttpClientConfig,
            )

            n5_frame_token = os.environ.get(
                "PATHFINDER_N5_MATERIALIZATION_TOKEN"
            )
            n5_digest_token = os.environ.get("PATHFINDER_N5_DIGEST_TOKEN")
            n4_token = os.environ.get("PATHFINDER_N4_PUBLICATION_TOKEN")
            if not n5_frame_token or not n5_digest_token or not n4_token:
                raise ConfigError(
                    "PATHFINDER_N5_MATERIALIZATION_TOKEN, "
                    "PATHFINDER_N5_DIGEST_TOKEN, and "
                    "PATHFINDER_N4_PUBLICATION_TOKEN are required"
                )
            private_hosts = tuple(args.allow_http_simulator_host)
            n4_config = N4PublicationHttpClientConfig(
                base_url=args.n4_base_url,
                bearer_token=n4_token,
                simulator_private_http_hosts=private_hosts,
                timeout_seconds=args.timeout_seconds,
            )
            frame_executor = ExistingFrameBundleBulkExecutor(
                n5_config=N5MaterializationHttpClientConfig(
                    base_url=args.n5_frame_base_url,
                    bearer_token=n5_frame_token,
                    simulator_private_http_hosts=private_hosts,
                    timeout_seconds=args.timeout_seconds,
                ),
                n4_config=n4_config,
            )
            digest_executor = ExistingDigestBulkExecutor(
                n5_executor=HttpN5DigestMaterializationExecutor(
                    N5DigestHttpClientConfig(
                        base_url=args.n5_digest_base_url,
                        bearer_token=n5_digest_token,
                        simulator_private_http_hosts=private_hosts,
                        timeout_seconds=args.timeout_seconds,
                    )
                ),
                n4_config=n4_config,
            )
            payload = run_full_flow_bulk_live_provisioning(
                args.provisioning_catalog_dir,
                args.artifact_binding_dir,
                args.n4_package_dir,
                args.operator_source_manifest,
                frame_executor=frame_executor,
                digest_executor=digest_executor,
                run_id=args.run_id,
                output_dir=args.output_dir,
                resume=args.resume,
            )
            return _print_payload(payload, compact=args.compact)
        if (
            args.command
            == "verify-simulator-full-flow-bulk-live-provisioning"
        ):
            from .simulator.full_flow_bulk_live_provisioning import (
                verify_full_flow_bulk_live_provisioning,
            )

            payload = verify_full_flow_bulk_live_provisioning(
                args.output_dir,
                provisioning_catalog_dir=args.provisioning_catalog_dir,
                artifact_binding_dir=args.artifact_binding_dir,
                n4_package_dir=args.n4_package_dir,
                operator_source_manifest=args.operator_source_manifest,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command in {
            "freeze-simulator-full-flow-pre-upcloud-readiness",
            "verify-simulator-full-flow-pre-upcloud-readiness",
        }:
            from .simulator.full_flow_pre_upcloud_readiness import (
                freeze_full_flow_pre_upcloud_readiness,
                verify_full_flow_pre_upcloud_readiness,
            )

            n1_evidence_secret = None
            if args.n1_oracle_package_dir is not None:
                secret_text = os.environ.get(
                    "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET"
                )
                if not secret_text:
                    raise ConfigError(
                        "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET is required "
                        "when --n1-oracle-package-dir is used"
                    )
                n1_evidence_secret = secret_text.encode("utf-8")
            n4_live_gate_sources = _load_n4_live_gate_sources(
                args.n4_live_gate_sources
            )
            readiness_sources = {
                "source_git_revision": args.source_git_revision,
                "operator_attests_clean_committed_source": (
                    args.attest_clean_committed_source
                ),
                "source_archive_path": args.source_archive,
                "provisioning_catalog_dir": args.provisioning_catalog_dir,
                "artifact_binding_dir": args.artifact_binding_dir,
                "n4_package_dir": args.n4_package_dir,
                "logical_route_dir": args.logical_route_dir,
                "scenario_path": args.scenario,
                "container_plan_dir": args.container_plan_dir,
                "task_plane_dir": args.task_plane_dir,
                "n3_package_dir": args.n3_package_dir,
                "w4_route_package_dir": args.w4_route_package_dir,
                "w4_index_package_dir": args.w4_index_package_dir,
                "w4_index_crosswalk_dir": args.w4_index_crosswalk_dir,
                "neutral_observation_dir": args.neutral_observation_dir,
                "neutral_semantic_matrix_run_dir": (
                    args.neutral_semantic_matrix_run_dir
                ),
                "semantic_execution_admission_dir": (
                    args.semantic_execution_admission_dir
                ),
                "smoke_gated_semantic_matrix_run_dir": (
                    args.smoke_gated_semantic_matrix_run_dir
                ),
                "ten_smoke_dir": args.ten_smoke_dir,
                "n4_serve_gate_dir": args.n4_serve_gate_dir,
                "compose_overlay_dir": args.compose_overlay_dir,
                "service_bootstrap_dir": args.service_bootstrap_dir,
                "deployment_binding_dir": args.deployment_binding_dir,
                "semantic_matrix_dir": args.semantic_matrix_dir,
                "public_task_set_path": args.public_task_set,
                "semantic_artifact_binding_path": (
                    args.semantic_artifact_binding
                ),
                "n4_live_gate_sources": n4_live_gate_sources,
                "n1_oracle_package_dir": args.n1_oracle_package_dir,
                "n1_evidence_secret": n1_evidence_secret,
                "bulk_provisioning_output_dir": (
                    args.bulk_provisioning_output_dir
                ),
                "bulk_source_manifest": args.bulk_source_manifest,
                "bulk_live_receipt_bindings": (
                    args.bulk_live_receipt_bindings
                ),
                "w4_component_receipt_dir": (
                    args.w4_component_receipt_dir
                ),
                "w4_coordinator_run_dir": args.w4_coordinator_run_dir,
                "w4_retrieval_contract_dir": (
                    args.w4_retrieval_contract_dir
                ),
                "w4_retrieval_evaluation_dir": (
                    args.w4_retrieval_evaluation_dir
                ),
                "flowmesh_matrix_plan_dir": args.flowmesh_matrix_plan_dir,
                "flowmesh_formal_profile_dir": (
                    args.flowmesh_formal_profile_dir
                ),
                "flowmesh_coordinator_plan_dir": (
                    args.flowmesh_coordinator_plan_dir
                ),
                "flowmesh_matrix_run_dir": args.flowmesh_matrix_run_dir,
                "flowmesh_w4_plan_dir": args.flowmesh_w4_plan_dir,
                "flowmesh_w4_run_dir": args.flowmesh_w4_run_dir,
                "output_dir": args.output_dir,
            }
            if (
                args.command
                == "freeze-simulator-full-flow-pre-upcloud-readiness"
            ):
                payload = freeze_full_flow_pre_upcloud_readiness(
                    audit_id=args.audit_id,
                    **readiness_sources,
                )
            else:
                payload = verify_full_flow_pre_upcloud_readiness(
                    **readiness_sources
                )
            return _print_payload(payload, compact=args.compact)
        if args.command == "freeze-simulator-n5-digest-plan":
            from .simulator.n5_digest_materialization import (
                freeze_n5_multimodal_digest_plan,
            )
            from .video_prep import sample_video

            frames, duration = sample_video(
                args.source_video,
                frame_count=args.frame_count,
                jpeg_max_dimension=args.jpeg_max_dimension,
            )
            payload = freeze_n5_multimodal_digest_plan(
                args.source_video,
                frames,
                source_duration_seconds=duration,
                object_id=args.object_id,
                model_id=args.model_id,
                output_dir=args.output_dir,
                plan_id=args.plan_id,
                jpeg_max_dimension=args.jpeg_max_dimension,
                seed=args.seed,
                maximum_digest_bytes=args.maximum_digest_bytes,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-n5-digest-plan":
            from .simulator.n5_digest_materialization import (
                verify_n5_multimodal_digest_plan,
            )

            payload = verify_n5_multimodal_digest_plan(
                args.plan_dir,
                args.source_video,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-simulator-n5-digest-materialization":
            from .simulator.n5_digest_materialization import (
                OpenAICompatibleVisionDigestAdapter,
                materialize_n5_multimodal_digest,
                verify_n5_multimodal_digest_plan,
            )

            base_url = (
                os.environ.get("PATHFINDER_N5_DIGEST_LLM_BASE_URL")
                or os.environ.get("UTU_LLM_BASE_URL")
            )
            api_key = (
                os.environ.get("PATHFINDER_N5_DIGEST_LLM_API_KEY")
                or os.environ.get("UTU_LLM_API_KEY")
            )
            configured_model = (
                os.environ.get("PATHFINDER_N5_DIGEST_LLM_MODEL")
                or os.environ.get("UTU_LLM_MODEL")
            )
            if not base_url or not api_key or not configured_model:
                raise ConfigError(
                    "PATHFINDER_N5_DIGEST_LLM_BASE_URL, "
                    "PATHFINDER_N5_DIGEST_LLM_MODEL, and "
                    "PATHFINDER_N5_DIGEST_LLM_API_KEY are required "
                    "(UTU_LLM_* aliases are accepted)"
                )
            verified = verify_n5_multimodal_digest_plan(
                args.plan_dir,
                args.source_video,
            )
            if configured_model != verified["model_id"]:
                raise ConfigError(
                    "runtime vision model differs from the frozen N5 plan"
                )
            adapter = OpenAICompatibleVisionDigestAdapter(
                base_url=base_url,
                api_key=api_key,
                model_id=configured_model,
                allowed_http_simulator_hosts=(
                    args.allow_http_simulator_host
                ),
                timeout_seconds=args.timeout_seconds,
                max_attempts=args.max_attempts,
            )
            payload = materialize_n5_multimodal_digest(
                args.plan_dir,
                args.source_video,
                output_dir=args.output_dir,
                vision_adapter=adapter,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-n5-digest-materialization":
            from .simulator.n5_digest_materialization import (
                verify_n5_multimodal_digest_materialization,
            )

            payload = verify_n5_multimodal_digest_materialization(
                args.output_dir,
                args.plan_dir,
                args.source_video,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "freeze-simulator-policy-routes":
            from .simulator.policy_oed_bridge import freeze_policy_assignment

            assignments: dict[str, list[str]] = {}
            for item in args.assignment:
                workload_class, separator, designs = item.partition("=")
                if not separator or workload_class in assignments:
                    raise ConfigError(
                        "each --assignment must uniquely use Wn=Dm[,Dm]"
                    )
                assignments[workload_class] = designs.split(",")
            payload = freeze_policy_assignment(
                logical_route_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
                policy_id=args.policy_id,
                awm_policy_sha256=args.awm_policy_sha256,
                assignments=assignments,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-policy-routes":
            from .simulator.policy_oed_bridge import verify_policy_assignment

            payload = verify_policy_assignment(
                assignment_dir=args.assignment_dir,
                logical_route_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "freeze-simulator-oed-routes":
            from .simulator.policy_oed_bridge import (
                freeze_oed_prospective_selection,
            )

            payload = freeze_oed_prospective_selection(
                logical_route_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
                oed_request_id=args.oed_request_id,
                oed_request_sha256=args.oed_request_sha256,
                requested_trial_keys=args.trial_key,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-oed-routes":
            from .simulator.policy_oed_bridge import (
                verify_oed_prospective_selection,
            )

            payload = verify_oed_prospective_selection(
                selection_dir=args.selection_dir,
                logical_route_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "freeze-simulator-full-flow-observations":
            from .simulator.policy_oed_bridge import (
                freeze_full_flow_observations,
            )

            evidence = None
            if args.evidence_json is not None:
                evidence = [
                    _load_strict_json_file(
                        path,
                        label="semantic route evidence",
                        expected_type=dict,
                    )
                    for path in args.evidence_json
                ]
            external_cost = None
            if args.external_real_cost_manifest is not None:
                external_cost = _load_strict_json_file(
                    args.external_real_cost_manifest,
                    label="external real-cost manifest",
                    expected_type=dict,
                )
            n1_secret = None
            if args.n1_oracle_package is not None:
                secret_text = os.environ.get(
                    "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET"
                )
                if not secret_text:
                    raise ConfigError(
                        "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET is required "
                        "when --n1-oracle-package is used"
                    )
                n1_secret = secret_text.encode("utf-8")
            payload = freeze_full_flow_observations(
                logical_route_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
                observation_set_id=args.observation_set_id,
                evidence_records=evidence,
                output_dir=args.output_dir,
                external_real_cost_manifest=external_cost,
                n1_oracle_package_dir=args.n1_oracle_package,
                n1_evidence_secret=n1_secret,
                semantic_execution_admission_dir=(
                    args.semantic_execution_admission_dir
                ),
                semantic_matrix_run_dir=args.semantic_matrix_run_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-full-flow-observations":
            from .simulator.policy_oed_bridge import (
                verify_full_flow_observations,
            )

            evidence = None
            if args.evidence_json is not None:
                evidence = [
                    _load_strict_json_file(
                        path,
                        label="semantic route evidence",
                        expected_type=dict,
                    )
                    for path in args.evidence_json
                ]
            n1_secret = None
            if args.n1_oracle_package is not None:
                secret_text = os.environ.get(
                    "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET"
                )
                if not secret_text:
                    raise ConfigError(
                        "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET is required "
                        "when --n1-oracle-package is used"
                    )
                n1_secret = secret_text.encode("utf-8")
            payload = verify_full_flow_observations(
                observation_dir=args.observation_dir,
                logical_route_plan_dir=args.logical_plan_dir,
                scenario_path=args.scenario,
                container_plan_dir=args.container_plan_dir,
                evidence_records=evidence,
                n1_oracle_package_dir=args.n1_oracle_package,
                n1_evidence_secret=n1_secret,
                semantic_execution_admission_dir=(
                    args.semantic_execution_admission_dir
                ),
                semantic_matrix_run_dir=args.semantic_matrix_run_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "freeze-simulator-neutral-awm-oed-analysis":
            from .simulator.neutral_awm_oed_consumer import (
                freeze_neutral_awm_oed_analysis,
            )

            cost_support = (
                tuple(args.cost_saving_support)
                if args.cost_saving_support is not None
                else None
            )
            if cost_support is not None and not (
                cost_support[0] < cost_support[1]
            ):
                raise ConfigError(
                    "--cost-saving-support LOWER must be less than UPPER"
                )
            payload = freeze_neutral_awm_oed_analysis(
                observation_dir=args.observation_dir,
                analysis_id=args.analysis_id,
                output_dir=args.output_dir,
                baseline_design_id=args.baseline_design_id,
                oed_selection_size=args.oed_selection_size,
                require_real_cost=args.require_real_cost,
                alpha=args.alpha,
                delta_success_margin=args.delta_success_margin,
                minimum_cost_saving=args.minimum_cost_saving,
                cost_saving_support=cost_support,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-simulator-neutral-awm-oed-analysis":
            from .simulator.neutral_awm_oed_consumer import (
                verify_neutral_awm_oed_analysis,
            )

            payload = verify_neutral_awm_oed_analysis(
                observation_dir=args.observation_dir,
                analysis_dir=args.analysis_dir,
            )
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
        if args.command == "run-data-agent-frame-bundle-semantic-trial":
            from dataclasses import replace as _replace
            from urllib.parse import urlsplit as _urlsplit

            from .data_agent_client import HttpDataAgentClient
            from .distributed.registry import load_endpoint_registry
            from .frame_bundle_ingest import DEFAULT_FRAME_BUNDLE_LIMITS
            from .simulator.data_agent_semantic_vertical import (
                HttpContainerSemanticVisionAdapter,
                execute_data_agent_frame_bundle_semantic_trial,
                load_data_agent_frame_bundle_semantic_spec,
            )
            from .simulator.local_container import (
                verify_local_container_compose,
            )

            registry = load_endpoint_registry(args.endpoint_registry)
            spec = load_data_agent_frame_bundle_semantic_spec(
                args.semantic_spec
            )
            route = registry.route(
                design_id=spec.document["data_agent_route_design_id"],
                representation_id=spec.document["representation_id"],
            )
            endpoint = registry.endpoint(route.endpoint_id)
            client_settings = _replace(
                endpoint.client_settings(),
                max_artifact_bytes=args.max_artifact_bytes,
            )
            compose = verify_local_container_compose(
                args.compose_package_dir
            )
            expected_executor = spec.document["semantic_executor_node_id"]
            if compose.get("semantic_quality_enabled") is not True:
                raise ConfigError(
                    "the Compose package does not enable semantic quality"
                )
            if compose.get("semantic_executor_node_id") != expected_executor:
                raise ConfigError(
                    "the Compose semantic executor differs from the frozen "
                    "semantic spec"
                )
            semantic_endpoint = compose.get("verified_semantic_endpoint")
            if not isinstance(semantic_endpoint, dict):
                raise ConfigError(
                    "the verified Compose package has no semantic endpoint"
                )
            semantic_url = semantic_endpoint.get("host_semantic_url")
            health_url = semantic_endpoint.get("host_health_url")
            if not isinstance(semantic_url, str) or not isinstance(
                health_url, str
            ):
                raise ConfigError(
                    "the verified Compose semantic endpoint is invalid"
                )
            try:
                semantic_port = _urlsplit(semantic_url).port
            except ValueError as exc:
                raise ConfigError(
                    "the verified Compose semantic endpoint is invalid"
                ) from exc
            if (
                semantic_port is None
                or not 1024 <= semantic_port <= 65535
                or semantic_url
                != (
                    f"http://127.0.0.1:{semantic_port}"
                    "/v1/semantic/chat-completions"
                )
                or health_url
                != f"http://127.0.0.1:{semantic_port}/healthz"
            ):
                raise ConfigError(
                    "the verified Compose semantic endpoint must use the "
                    "expected literal loopback URL"
                )
            adapter = HttpContainerSemanticVisionAdapter(
                semantic_url=semantic_url,
                health_url=health_url,
                expected_execution_node_id=expected_executor,
                bearer_token=os.environ.get(
                    "PATHFINDER_CONTAINER_NODE_TOKEN"
                ),
                timeout_seconds=args.request_timeout,
            )
            payload = execute_data_agent_frame_bundle_semantic_trial(
                matrix_plan_dir=args.matrix_plan_dir,
                semantic_spec=args.semantic_spec,
                endpoint_registry=registry,
                clients_by_endpoint_id={
                    route.endpoint_id: HttpDataAgentClient(client_settings)
                },
                adapter=adapter,
                output_dir=args.output_dir,
                event_index=args.event_index,
                limits=_replace(
                    DEFAULT_FRAME_BUNDLE_LIMITS,
                    max_artifact_bytes=args.max_artifact_bytes,
                ),
                quiescence_timeout_seconds=(
                    args.telemetry_quiescence_timeout
                ),
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-data-agent-frame-bundle-semantic-trial":
            from .distributed.registry import load_endpoint_registry
            from .simulator.data_agent_semantic_vertical import (
                verify_data_agent_frame_bundle_semantic_trial,
            )

            payload = verify_data_agent_frame_bundle_semantic_trial(
                output_dir=args.output_dir,
                matrix_plan_dir=args.matrix_plan_dir,
                endpoint_registry=load_endpoint_registry(
                    args.endpoint_registry
                ),
                semantic_spec=args.semantic_spec,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "build-flowmesh-pathfinder-evidence":
            from .distributed.registry import load_endpoint_registry
            from .integrations.flowmesh.pathfinder_evidence_bridge import (
                build_flowmesh_pathfinder_evidence,
            )

            payload = build_flowmesh_pathfinder_evidence(
                binding_spec=args.binding_spec,
                matrix_plan_dir=args.matrix_plan_dir,
                matrix_run_dir=args.matrix_run_dir,
                endpoint_registry=load_endpoint_registry(
                    args.endpoint_registry
                ),
                data_agent_semantic_dirs=(
                    args.data_agent_semantic_dir
                ),
                data_agent_semantic_specs=(
                    args.data_agent_semantic_spec
                ),
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-pathfinder-evidence":
            from .distributed.registry import load_endpoint_registry
            from .integrations.flowmesh.pathfinder_evidence_bridge import (
                verify_flowmesh_pathfinder_evidence,
            )

            payload = verify_flowmesh_pathfinder_evidence(
                evidence_dir=args.evidence_dir,
                binding_spec=args.binding_spec,
                matrix_plan_dir=args.matrix_plan_dir,
                matrix_run_dir=args.matrix_run_dir,
                endpoint_registry=load_endpoint_registry(
                    args.endpoint_registry
                ),
                data_agent_semantic_dirs=(
                    args.data_agent_semantic_dir
                ),
                data_agent_semantic_specs=(
                    args.data_agent_semantic_spec
                ),
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
                DEFAULT_API_TASK_TIMEOUT_SECONDS,
                plan_flowmesh_container_operation_dag,
            )

            # Resolved here rather than in the parser so the default lives in
            # exactly one place and the flowmesh module stays lazily imported.
            api_task_timeout = args.api_task_timeout_seconds
            if api_task_timeout is None:
                api_task_timeout = DEFAULT_API_TASK_TIMEOUT_SECONDS
            payload = plan_flowmesh_container_operation_dag(
                container_operations_path=args.container_operations,
                node_api_urls=_node_api_url_mapping(args.node_api_url),
                worker_alias=args.worker_alias,
                smoke_id=args.smoke_id,
                trial_key=args.trial_key,
                owner=args.owner,
                api_task_timeout_seconds=api_task_timeout,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-dag-run":
            from .integrations.flowmesh.container_dag import (
                verify_flowmesh_container_operation_dag_run,
            )

            payload = verify_flowmesh_container_operation_dag_run(
                args.run_dir,
                plan_dir=args.plan_dir,
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
        if args.command == "plan-flowmesh-container-matrix":
            from .integrations.flowmesh.container_dag import (
                DEFAULT_API_TASK_TIMEOUT_SECONDS,
            )
            from .integrations.flowmesh.container_matrix import (
                plan_flowmesh_container_matrix,
            )

            api_task_timeout = args.api_task_timeout_seconds
            if api_task_timeout is None:
                api_task_timeout = DEFAULT_API_TASK_TIMEOUT_SECONDS
            payload = plan_flowmesh_container_matrix(
                portable_plan_dir=args.portable_plan_dir,
                container_plan_dir=args.container_plan_dir,
                node_api_urls=_node_api_url_mapping(args.node_api_url),
                worker_alias=args.worker_alias,
                matrix_id=args.matrix_id,
                source_git_revision=args.source_git_revision,
                execution_profile_id=args.execution_profile_id,
                api_task_timeout_seconds=api_task_timeout,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-matrix-plan":
            from .integrations.flowmesh.container_matrix import (
                verify_flowmesh_container_matrix_plan,
            )

            payload = verify_flowmesh_container_matrix_plan(args.plan_dir)
            return _print_payload(payload, compact=args.compact)
        if args.command == "list-flowmesh-container-conditional-dag-candidates":
            from .integrations.flowmesh.container_conditional_dag import (
                list_conditional_container_trial_candidates,
            )
            from .integrations.flowmesh.container_dag import (
                load_container_operations,
            )

            candidates = list_conditional_container_trial_candidates(
                load_container_operations(args.container_operations)
            )
            payload = {
                "status": "COMPLETE",
                "candidate_count": len(candidates),
                "candidates": candidates,
                "workflow_submitted": False,
                "services_started": False,
                "credentials_recorded": False,
                "eligible_for_scientific_claims": False,
            }
            return _print_payload(payload, compact=args.compact)
        if args.command == "resolve-flowmesh-container-conditional-dag":
            from .integrations.flowmesh.container_conditional_dag import (
                resolve_conditional_container_trial,
            )
            from .integrations.flowmesh.container_dag import (
                load_container_operations,
            )

            expected_outcomes = _cache_outcome_mapping(args.cache_outcome)
            resolution = resolve_conditional_container_trial(
                load_container_operations(args.container_operations),
                trial_key=args.trial_key,
                cache_outcomes=expected_outcomes or None,
            )
            payload = {
                "status": "RESOLVED_FROM_FROZEN_CACHE_SNAPSHOT",
                **resolution,
            }
            return _print_payload(payload, compact=args.compact)
        if args.command == "plan-flowmesh-container-conditional-trial":
            from .integrations.flowmesh.container_conditional_runner import (
                plan_flowmesh_container_conditional_trial,
            )

            payload = plan_flowmesh_container_conditional_trial(
                matrix_plan_dir=args.matrix_plan_dir,
                trial_key=args.trial_key,
                smoke_id=args.smoke_id,
                owner=args.owner,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-conditional-trial-plan":
            from .integrations.flowmesh.container_conditional_runner import (
                verify_flowmesh_container_conditional_trial_plan,
            )

            payload = verify_flowmesh_container_conditional_trial_plan(
                args.plan_dir
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-flowmesh-container-conditional-trial":
            from .integrations.flowmesh import FlowMeshSettings, SdkFlowMeshClient
            from .integrations.flowmesh.container_conditional_runner import (
                run_flowmesh_container_conditional_trial,
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
                payload = run_flowmesh_container_conditional_trial(
                    plan_dir=args.plan_dir,
                    output_dir=args.output_dir,
                    client=client,
                    settings=settings,
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-conditional-trial-run":
            from .integrations.flowmesh.container_conditional_runner import (
                verify_flowmesh_container_conditional_trial_run,
            )

            payload = verify_flowmesh_container_conditional_trial_run(
                args.run_dir,
                plan_dir=args.plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "freeze-flowmesh-container-formal-execution-profile":
            from .integrations.flowmesh.container_formal_profile import (
                freeze_flowmesh_container_formal_execution_profile,
            )

            payload = freeze_flowmesh_container_formal_execution_profile(
                matrix_plan_dir=args.matrix_plan_dir,
                calibration_audit_dir=args.calibration_audit_dir,
                execution_profile_id=args.execution_profile_id,
                primary_trial_wrapper_max_concurrency=(
                    args.primary_trial_wrapper_max_concurrency
                ),
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-formal-execution-profile":
            from .integrations.flowmesh.container_formal_profile import (
                verify_flowmesh_container_formal_execution_profile,
            )

            payload = verify_flowmesh_container_formal_execution_profile(
                args.profile_dir
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "plan-flowmesh-container-matrix-coordinator-dry-run":
            from .integrations.flowmesh.container_matrix_coordinator import (
                plan_flowmesh_container_matrix_coordinator_dry_run,
            )

            payload = plan_flowmesh_container_matrix_coordinator_dry_run(
                matrix_plan_dir=args.matrix_plan_dir,
                formal_execution_profile_dir=args.formal_execution_profile_dir,
                coordinator_id=args.coordinator_id,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-matrix-coordinator-dry-run":
            from .integrations.flowmesh.container_matrix_coordinator import (
                verify_flowmesh_container_matrix_coordinator_dry_run,
            )

            payload = verify_flowmesh_container_matrix_coordinator_dry_run(
                args.plan_dir,
                matrix_plan_dir=args.matrix_plan_dir,
                formal_execution_profile_dir=args.formal_execution_profile_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-flowmesh-container-matrix":
            from .integrations.flowmesh import FlowMeshSettings, SdkFlowMeshClient
            from .integrations.flowmesh.container_matrix_runner import (
                run_flowmesh_container_matrix,
            )

            if not args.worker_alias.strip():
                raise ValueError("worker alias must be a non-empty string")
            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                poll_interval_seconds=args.poll_interval,
                worker_alias=args.worker_alias,
                validate_before_submit=True,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = run_flowmesh_container_matrix(
                    matrix_plan_dir=args.matrix_plan_dir,
                    formal_execution_profile_dir=(
                        args.formal_execution_profile_dir
                    ),
                    coordinator_plan_dir=args.coordinator_plan_dir,
                    output_dir=args.output_dir,
                    run_id=args.run_id,
                    client=client,
                    settings=settings,
                    recovery_id=args.recovery_id,
                    recovery_reason=args.recovery_reason,
                    recover_failed_entry_sha256=(
                        args.recover_failed_entry_sha256
                    ),
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if args.command == "adopt-flowmesh-container-matrix-replay-results":
            from .integrations.flowmesh import FlowMeshSettings, SdkFlowMeshClient
            from .integrations.flowmesh.container_matrix_runner import (
                adopt_flowmesh_container_matrix_replay_results,
            )

            if not args.worker_alias.strip():
                raise ValueError("worker alias must be a non-empty string")
            settings = FlowMeshSettings.from_environment(
                base_url=args.flowmesh_base_url,
                poll_interval_seconds=args.poll_interval,
                worker_alias=args.worker_alias,
                validate_before_submit=False,
            )
            client = SdkFlowMeshClient(settings)
            try:
                payload = adopt_flowmesh_container_matrix_replay_results(
                    matrix_plan_dir=args.matrix_plan_dir,
                    formal_execution_profile_dir=(
                        args.formal_execution_profile_dir
                    ),
                    coordinator_plan_dir=args.coordinator_plan_dir,
                    run_dir=args.run_dir,
                    run_id=args.run_id,
                    client=client,
                    settings=settings,
                    adoption_id=args.adoption_id,
                    adoption_reason=args.adoption_reason,
                    adopt_failed_entry_sha256=(
                        args.adopt_failed_entry_sha256
                    ),
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-matrix-run":
            from .integrations.flowmesh.container_matrix_runner import (
                verify_flowmesh_container_matrix_run,
            )

            payload = verify_flowmesh_container_matrix_run(
                args.run_dir,
                matrix_plan_dir=args.matrix_plan_dir,
                formal_execution_profile_dir=(
                    args.formal_execution_profile_dir
                ),
                coordinator_plan_dir=args.coordinator_plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "summarize-flowmesh-container-matrix-run":
            from .integrations.flowmesh.container_matrix_statistics import (
                summarize_flowmesh_container_matrix_run,
            )

            payload = summarize_flowmesh_container_matrix_run(
                run_dir=args.run_dir,
                matrix_plan_dir=args.matrix_plan_dir,
                formal_execution_profile_dir=(
                    args.formal_execution_profile_dir
                ),
                coordinator_plan_dir=args.coordinator_plan_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-matrix-statistics":
            from .integrations.flowmesh.container_matrix_statistics import (
                verify_flowmesh_container_matrix_statistics,
            )

            payload = verify_flowmesh_container_matrix_statistics(
                args.output_dir
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "list-flowmesh-container-full-chain-candidates":
            from .integrations.flowmesh.container_full_chain import (
                list_full_physical_container_operation_chain_candidates,
            )
            from .integrations.flowmesh.container_dag import (
                load_container_operations,
            )

            candidates = list_full_physical_container_operation_chain_candidates(
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
        if args.command == "plan-flowmesh-container-full-chain":
            from .integrations.flowmesh.container_dag import (
                DEFAULT_API_TASK_TIMEOUT_SECONDS,
            )
            from .integrations.flowmesh.container_full_chain import (
                plan_flowmesh_container_full_physical_chain,
            )

            api_task_timeout = args.api_task_timeout_seconds
            if api_task_timeout is None:
                api_task_timeout = DEFAULT_API_TASK_TIMEOUT_SECONDS
            payload = plan_flowmesh_container_full_physical_chain(
                container_operations_path=args.container_operations,
                node_api_urls=_node_api_url_mapping(args.node_api_url),
                worker_alias=args.worker_alias,
                smoke_id=args.smoke_id,
                trial_key=args.trial_key,
                owner=args.owner,
                api_task_timeout_seconds=api_task_timeout,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-full-chain-plan":
            from .integrations.flowmesh.container_full_chain import (
                verify_flowmesh_container_full_physical_chain_plan,
            )

            payload = verify_flowmesh_container_full_physical_chain_plan(
                args.plan_dir
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "run-flowmesh-container-full-chain":
            from .integrations.flowmesh import FlowMeshSettings, SdkFlowMeshClient
            from .integrations.flowmesh.container_full_chain import (
                run_flowmesh_container_full_physical_chain,
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
                payload = run_flowmesh_container_full_physical_chain(
                    plan_dir=args.plan_dir,
                    output_dir=args.output_dir,
                    client=client,
                    settings=settings,
                )
            finally:
                client.close()
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-full-chain-run":
            from .integrations.flowmesh.container_full_chain import (
                verify_flowmesh_container_full_physical_chain_run,
            )

            payload = verify_flowmesh_container_full_physical_chain_run(
                args.run_dir,
                plan_dir=args.plan_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "audit-flowmesh-container-full-chain-calibration":
            from .integrations.flowmesh.container_full_chain_calibration import (
                audit_flowmesh_container_full_chain_calibration,
            )

            payload = audit_flowmesh_container_full_chain_calibration(
                fast_plan_dir=args.fast_plan_dir,
                fast_run_dir=args.fast_run_dir,
                slow_plan_dir=args.slow_plan_dir,
                slow_run_dir=args.slow_run_dir,
                output_dir=args.output_dir,
            )
            return _print_payload(payload, compact=args.compact)
        if args.command == "verify-flowmesh-container-full-chain-calibration":
            from .integrations.flowmesh.container_full_chain_calibration import (
                verify_flowmesh_container_full_chain_calibration,
            )

            payload = verify_flowmesh_container_full_chain_calibration(
                args.output_dir
            )
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

            settings = DataAgentServerSettings.from_environment(
                host=args.host,
                port=args.port,
                public_base_url=args.public_base_url,
                artifact_url_ttl_seconds=args.artifact_url_ttl,
                max_request_bytes=args.max_request_bytes,
                max_inline_bytes=args.max_inline_bytes,
            )
            if args.require_token and settings.token is None:
                raise ConfigError(
                    "PATHFINDER_DATA_AGENT_TOKEN is required for this service"
                )
            if (
                args.require_artifact_secret
                and settings.artifact_secret is None
            ):
                raise ConfigError(
                    "PATHFINDER_DATA_AGENT_ARTIFACT_SECRET is required for "
                    "this service"
                )
            run_data_agent_server(
                manifest_path=args.manifest,
                operation_db=args.operation_db,
                settings=settings,
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
        from .integrations.flowmesh.redaction import redact_secrets

        print(
            json.dumps(
                {"status": "error", "message": redact_secrets(str(exc))}
            )
        )
        return 2

    return _print_payload(
        payload,
        compact=getattr(args, "compact", False),
    )
