from __future__ import annotations

import csv
import json
import math
import os
import platform
import random
import re
import subprocess
import time
import uuid
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from importlib import metadata
from itertools import combinations
from pathlib import Path
from statistics import mean
from typing import Any, Callable, Iterable, Iterator, Protocol

from ...config import ConfigError
from ...data_agent_client import (
    DataAgentTelemetryQuiescenceError,
    DataAgentTelemetryUnsupportedError,
)
from ...models import SystemConfig
from .contracts import (
    FlowMeshAgentRun,
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
)
from .gateway import TelemetryIncompleteError


PILOT_SCHEMA_VERSION = "pathfinder.flowmesh-pilot/v1alpha1"
PILOT_RECORD_SCHEMA_VERSION = "pathfinder.flowmesh-pilot-record/v1alpha1"


class FlowMeshPilotConfigError(ConfigError):
    """Raised when a real-FlowMesh pilot plan is invalid."""


@dataclass(frozen=True)
class PilotWorkload:
    id: str
    object_id: str
    question: str
    accepted_answer_substrings: tuple[str, ...] = ()


@dataclass(frozen=True)
class FlowMeshPilotConfig:
    schema_version: str
    experiment_id: str
    source_path: Path
    system_config_path: Path
    design_ids: tuple[str, ...]
    task_class_id: str
    quote_profile_ids: tuple[str, ...]
    latency_multipliers: tuple[float, ...]
    repetitions: int
    base_seed: int
    randomization_seed: int
    workloads: tuple[PilotWorkload, ...]
    source_sha256: str

    @property
    def design_id(self) -> str:
        """Return the legacy singleton design without hiding a factorial."""
        if len(self.design_ids) != 1:
            raise FlowMeshPilotConfigError(
                "this pilot declares multiple design_ids; consume design_ids "
                "or the per-trial design_id instead"
            )
        return self.design_ids[0]


@dataclass(frozen=True)
class PilotTrial:
    trial_key: str
    trial_id: str
    session_id: str
    order_index: int
    workload_id: str
    object_id: str
    question: str
    accepted_answer_substrings: tuple[str, ...]
    design_id: str
    task_class_id: str
    quote_profile_id: str
    latency_multiplier: float
    repetition: int
    seed: int

    def to_public_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        # Keep the in-memory representation identical to its JSON round trip,
        # otherwise a resumed run would compare tuple != list and reject its
        # own frozen plan.
        payload["accepted_answer_substrings"] = list(
            self.accepted_answer_substrings
        )
        return payload


class AgentAdapterProtocol(Protocol):
    settings: FlowMeshSettings

    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        """Execute one pre-registered FlowMesh experiment session."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FlowMeshPilotConfigError(f"{name} must be a JSON object")
    return value


def _require_list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise FlowMeshPilotConfigError(f"{name} must be a JSON array")
    return value


def _nonempty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FlowMeshPilotConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _unique_strings(values: Iterable[Any], name: str) -> tuple[str, ...]:
    result = tuple(_nonempty_string(value, name) for value in values)
    if not result:
        raise FlowMeshPilotConfigError(f"{name} cannot be empty")
    if len(result) != len(set(result)):
        raise FlowMeshPilotConfigError(f"{name} contains duplicates")
    return result


def load_flowmesh_pilot_config(
    path: str | Path,
) -> FlowMeshPilotConfig:
    source_path = Path(path).resolve()
    try:
        raw_bytes = source_path.read_bytes()
        raw = json.loads(raw_bytes.decode("utf-8"))
    except FileNotFoundError as exc:
        raise FlowMeshPilotConfigError(
            f"pilot configuration does not exist: {source_path}"
        ) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FlowMeshPilotConfigError(
            f"invalid JSON in pilot configuration {source_path}: {exc}"
        ) from exc

    root = _require_mapping(raw, "pilot configuration")
    schema_version = _nonempty_string(
        root.get("schema_version"),
        "schema_version",
    )
    if schema_version != PILOT_SCHEMA_VERSION:
        raise FlowMeshPilotConfigError(
            f"unsupported pilot schema_version: {schema_version}"
        )

    system_config_value = _nonempty_string(
        root.get("system_config"),
        "system_config",
    )
    system_config_path = Path(system_config_value)
    if not system_config_path.is_absolute():
        system_config_path = source_path.parent / system_config_path
    system_config_path = system_config_path.resolve()

    raw_workloads = _require_list(root.get("workloads"), "workloads")
    workloads: list[PilotWorkload] = []
    workload_ids: set[str] = set()
    for index, item_raw in enumerate(raw_workloads):
        item = _require_mapping(item_raw, f"workloads[{index}]")
        workload_id = _nonempty_string(
            item.get("id"),
            f"workloads[{index}].id",
        )
        if workload_id in workload_ids:
            raise FlowMeshPilotConfigError(
                f"duplicate workload id: {workload_id}"
            )
        workload_ids.add(workload_id)
        accepted = tuple(
            _nonempty_string(
                value,
                f"workloads[{index}].accepted_answer_substrings",
            )
            for value in _require_list(
                item.get("accepted_answer_substrings", []),
                f"workloads[{index}].accepted_answer_substrings",
            )
        )
        workloads.append(
            PilotWorkload(
                id=workload_id,
                object_id=_nonempty_string(
                    item.get("object_id"),
                    f"workloads[{index}].object_id",
                ),
                question=_nonempty_string(
                    item.get("question"),
                    f"workloads[{index}].question",
                ),
                accepted_answer_substrings=accepted,
            )
        )
    if not workloads:
        raise FlowMeshPilotConfigError("workloads cannot be empty")

    quote_profiles = _unique_strings(
        _require_list(root.get("quote_profile_ids"), "quote_profile_ids"),
        "quote_profile_ids",
    )
    raw_multipliers = _require_list(
        root.get("latency_multipliers"),
        "latency_multipliers",
    )
    if any(isinstance(value, bool) for value in raw_multipliers):
        raise FlowMeshPilotConfigError(
            "latency_multipliers must contain numbers, not booleans"
        )
    try:
        latency_multipliers = tuple(float(value) for value in raw_multipliers)
    except (TypeError, ValueError) as exc:
        raise FlowMeshPilotConfigError(
            "latency_multipliers must contain numbers"
        ) from exc
    if not latency_multipliers or any(
        not math.isfinite(value) or value <= 0
        for value in latency_multipliers
    ):
        raise FlowMeshPilotConfigError(
            "latency_multipliers must contain positive values"
        )
    if len(latency_multipliers) != len(set(latency_multipliers)):
        raise FlowMeshPilotConfigError(
            "latency_multipliers contains duplicates"
        )

    repetitions = root.get("repetitions")
    if isinstance(repetitions, bool) or not isinstance(repetitions, int):
        raise FlowMeshPilotConfigError("repetitions must be an integer")
    if repetitions <= 0:
        raise FlowMeshPilotConfigError("repetitions must be positive")

    experiment_id = _nonempty_string(
        root.get("experiment_id"),
        "experiment_id",
    )
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", experiment_id)
        is None
    ):
        raise FlowMeshPilotConfigError(
            "experiment_id may contain only letters, digits, '.', '_', and '-'"
        )
    for name in ("base_seed", "randomization_seed"):
        value = root.get(name, 1)
        if isinstance(value, bool) or not isinstance(value, int):
            raise FlowMeshPilotConfigError(f"{name} must be an integer")

    raw_design_ids = root.get("design_ids")
    raw_design_id = root.get("design_id")
    if raw_design_ids is not None and raw_design_id is not None:
        raise FlowMeshPilotConfigError(
            "pilot configuration must use either design_id or design_ids, "
            "not both"
        )
    if raw_design_ids is not None:
        design_ids = _unique_strings(
            _require_list(raw_design_ids, "design_ids"),
            "design_ids",
        )
    else:
        design_ids = (
            _nonempty_string(raw_design_id, "design_id"),
        )

    return FlowMeshPilotConfig(
        schema_version=schema_version,
        experiment_id=experiment_id,
        source_path=source_path,
        system_config_path=system_config_path,
        design_ids=design_ids,
        task_class_id=_nonempty_string(
            root.get("task_class_id"),
            "task_class_id",
        ),
        quote_profile_ids=quote_profiles,
        latency_multipliers=latency_multipliers,
        repetitions=repetitions,
        base_seed=root.get("base_seed", 1),
        randomization_seed=root.get("randomization_seed", 1),
        workloads=tuple(workloads),
        source_sha256=sha256(raw_bytes).hexdigest(),
    )


def validate_flowmesh_pilot_config(
    pilot: FlowMeshPilotConfig,
    system: SystemConfig,
) -> None:
    for design_id in pilot.design_ids:
        if design_id not in system.designs:
            raise FlowMeshPilotConfigError(
                f"pilot references unknown design: {design_id}"
            )
    if pilot.task_class_id not in system.task_classes:
        raise FlowMeshPilotConfigError(
            f"pilot references unknown task class: {pilot.task_class_id}"
        )
    for profile_id in pilot.quote_profile_ids:
        if profile_id not in system.quote_profiles:
            raise FlowMeshPilotConfigError(
                f"pilot references unknown quote profile: {profile_id}"
            )


def build_trial_plan(
    pilot: FlowMeshPilotConfig,
    *,
    repetitions: int | None = None,
    randomization_seed: int | None = None,
) -> list[PilotTrial]:
    resolved_repetitions = (
        pilot.repetitions if repetitions is None else repetitions
    )
    if (
        isinstance(resolved_repetitions, bool)
        or not isinstance(resolved_repetitions, int)
        or resolved_repetitions <= 0
    ):
        raise FlowMeshPilotConfigError("repetitions must be positive")
    resolved_randomization_seed = (
        pilot.randomization_seed
        if randomization_seed is None
        else randomization_seed
    )
    if isinstance(resolved_randomization_seed, bool) or not isinstance(
        resolved_randomization_seed,
        int,
    ):
        raise FlowMeshPilotConfigError("randomization_seed must be an integer")

    blocks: list[list[dict[str, Any]]] = []
    for workload in pilot.workloads:
        for repetition in range(resolved_repetitions):
            # The same Pathfinder seed is registered across intervention
            # cells. It is a pairing key for Gateway/backend behavior; the
            # current FlowMesh/UTU workflow does not set an LLM sampling seed.
            seed = pilot.base_seed + repetition
            block: list[dict[str, Any]] = []
            for design_id in pilot.design_ids:
                for latency_multiplier in pilot.latency_multipliers:
                    for profile_id in pilot.quote_profile_ids:
                        multiplier_key = format(latency_multiplier, ".12g")
                        # Preserve the v1 single-design identity so an existing
                        # pilot can still be resumed after this schema extension.
                        # Multi-design plans need the design in the key because
                        # every block contains a matched physical intervention.
                        if len(pilot.design_ids) == 1:
                            trial_key = (
                                f"{workload.id}|{profile_id}|{multiplier_key}|"
                                f"r{repetition:04d}"
                            )
                        else:
                            trial_key = (
                                f"{workload.id}|{design_id}|{profile_id}|"
                                f"{multiplier_key}|r{repetition:04d}"
                            )
                        identity = (
                            f"{pilot.source_sha256}:{pilot.experiment_id}:"
                            f"{trial_key}"
                        )
                        block.append(
                            {
                                "trial_key": trial_key,
                                "trial_id": (
                                    f"{pilot.experiment_id}-"
                                    f"{sha256(trial_key.encode()).hexdigest()[:12]}"
                                ),
                                "session_id": str(
                                    uuid.uuid5(uuid.NAMESPACE_URL, identity)
                                ),
                                "workload": workload,
                                "design_id": design_id,
                                "profile_id": profile_id,
                                "latency_multiplier": latency_multiplier,
                                "repetition": repetition,
                                "seed": seed,
                            }
                        )
            blocks.append(block)

    # Randomize blocks and the intervention order inside each block. Every
    # workload/repetition block still contains exactly one copy of every cell,
    # so transient host load cannot leave a long prefix with one condition.
    rng = random.Random(resolved_randomization_seed)
    rng.shuffle(blocks)
    raw_trials: list[dict[str, Any]] = []
    for block in blocks:
        rng.shuffle(block)
        raw_trials.extend(block)
    return [
        PilotTrial(
            trial_key=item["trial_key"],
            trial_id=item["trial_id"],
            session_id=item["session_id"],
            order_index=order_index,
            workload_id=item["workload"].id,
            object_id=item["workload"].object_id,
            question=item["workload"].question,
            accepted_answer_substrings=(
                item["workload"].accepted_answer_substrings
            ),
            design_id=item["design_id"],
            task_class_id=pilot.task_class_id,
            quote_profile_id=item["profile_id"],
            latency_multiplier=item["latency_multiplier"],
            repetition=item["repetition"],
            seed=item["seed"],
        )
        for order_index, item in enumerate(raw_trials)
    ]


def _normalized_answer(value: str) -> str:
    return " ".join(value.casefold().split())


def _evaluate_answer(
    answer: str,
    accepted_substrings: tuple[str, ...],
) -> bool | None:
    if not accepted_substrings:
        return None
    normalized = _normalized_answer(answer)
    return any(
        _normalized_answer(candidate) in normalized
        for candidate in accepted_substrings
    )


def _classify_exception(exc: Exception) -> str:
    if isinstance(
        exc,
        (
            DataAgentTelemetryQuiescenceError,
            DataAgentTelemetryUnsupportedError,
            TelemetryIncompleteError,
        ),
    ):
        return "telemetry_failure"
    return "infrastructure_failure"


def _safe_error_message(exc: Exception) -> str:
    message = str(exc)
    # Third-party exceptions must not turn the research log into a credential
    # or signed-URL sink.
    for name in (
        "FLOWMESH_API_KEY",
        "PATHFINDER_DATA_AGENT_TOKEN",
        "UTU_LLM_API_KEY",
    ):
        value = os.getenv(name)
        if value:
            message = message.replace(value, "<redacted>")
    message = re.sub(
        r"(?i)(authorization\s*[:=]\s*bearer\s+)\S+",
        r"\1<redacted>",
        message,
    )
    message = re.sub(
        r"(?i)(bearer\s+)\S+",
        r"\1<redacted>",
        message,
    )
    message = re.sub(r"(https?://[^\s?]+)\?\S+", r"\1?<redacted>", message)
    return message[:2000]


def _offered_quotes(
    system: SystemConfig,
    trial: PilotTrial,
) -> dict[str, float]:
    task = system.task_classes[trial.task_class_id]
    design = system.designs[trial.design_id]
    profile = system.quote_profiles[trial.quote_profile_id]
    result: dict[str, float] = {}
    for representation_id in task.candidate_representations:
        path = design.paths.get(representation_id)
        if path is None or not path.available:
            continue
        default = path.quotes.get(task.id)
        if default is None:
            continue
        result[representation_id] = profile.quote_for(
            task.id,
            representation_id,
            default,
        )
    return result


def _sanitized_access_event(event: dict[str, Any]) -> dict[str, Any]:
    """Remove reusable capabilities while retaining a correlation key."""
    sanitized = dict(event)
    raw_handle = sanitized.pop("artifact_handle", None)
    if raw_handle is not None:
        if not isinstance(raw_handle, str):
            raise RuntimeError("artifact_handle in an access event is not text")
        sanitized["artifact_handle_sha256"] = sha256(
            raw_handle.encode("utf-8")
        ).hexdigest()
    return sanitized


def artifact_delivery_failures(
    events: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Describe accepted artifact accesses without a completed full fetch."""
    failures: list[dict[str, Any]] = []
    for event in events:
        if not event.get("accepted"):
            continue
        fingerprint = event.get("artifact_handle_sha256")
        if not isinstance(fingerprint, str) or not fingerprint:
            continue
        request_count = int(event.get("artifact_download_request_count") or 0)
        full_download_count = int(
            event.get("artifact_full_download_count") or 0
        )
        reasons: list[str] = []
        if request_count == 0:
            reasons.append("no_download_request")
        if full_download_count == 0:
            reasons.append("no_completed_full_download")
        if reasons:
            failures.append(
                {
                    "event_id": event.get("event_id"),
                    "representation_id": event.get("representation_id"),
                    "artifact_handle_sha256": fingerprint,
                    "artifact_download_request_count": request_count,
                    "artifact_full_download_count": full_download_count,
                    "artifact_bytes_sent": int(
                        event.get("artifact_bytes_sent") or 0
                    ),
                    "reasons": reasons,
                }
            )
    return failures


def _record_for_success(
    pilot: FlowMeshPilotConfig,
    trial: PilotTrial,
    result: FlowMeshAgentRun,
    *,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    offered_quotes: dict[str, float],
    recovered_from_gateway_state: bool,
) -> dict[str, Any]:
    events = [
        _sanitized_access_event(dict(event))
        for event in result.access_events
    ]
    accepted_events = [event for event in events if event.get("accepted")]
    selected = [
        str(event["representation_id"])
        for event in accepted_events
        if event.get("representation_id") is not None
    ]
    delivery_failures = artifact_delivery_failures(events)
    artifact_required = any(
        event.get("accepted") and event.get("artifact_handle_sha256")
        for event in events
    )
    outcome_type = (
        "artifact_delivery_failure" if delivery_failures else "completed"
    )
    return {
        "schema_version": PILOT_RECORD_SCHEMA_VERSION,
        "experiment_id": pilot.experiment_id,
        **trial.to_public_dict(),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 6),
        "outcome_type": outcome_type,
        "recovered_from_gateway_state": recovered_from_gateway_state,
        "offered_quotes": offered_quotes,
        "telemetry_complete": True,
        "workflow_id": result.workflow_id,
        "task_id": result.task_id,
        "flowmesh_status": result.status,
        "final_answer": result.final_answer,
        "task_success": (
            None
            if delivery_failures
            else _evaluate_answer(
                result.final_answer,
                trial.accepted_answer_substrings,
            )
        ),
        "access_event_count": len(events),
        "accepted_access_count": len(accepted_events),
        "selected_representations": selected,
        "access_events": events,
        "artifact_delivery_required": bool(artifact_required),
        "artifact_delivery_complete": not delivery_failures,
        "artifact_delivery_failure_count": len(delivery_failures),
        "artifact_delivery_failures": delivery_failures,
        "error_type": (
            "ArtifactDeliveryFailure" if delivery_failures else None
        ),
        "error_message": (
            f"{len(delivery_failures)} accepted artifact access(es) did not "
            "complete a full artifact download"
            if delivery_failures
            else None
        ),
    }


def _record_for_failure(
    pilot: FlowMeshPilotConfig,
    trial: PilotTrial,
    exc: Exception,
    *,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    offered_quotes: dict[str, float],
) -> dict[str, Any]:
    outcome_type = _classify_exception(exc)
    return {
        "schema_version": PILOT_RECORD_SCHEMA_VERSION,
        "experiment_id": pilot.experiment_id,
        **trial.to_public_dict(),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 6),
        "outcome_type": outcome_type,
        "recovered_from_gateway_state": False,
        "offered_quotes": offered_quotes,
        "telemetry_complete": (
            False if outcome_type == "telemetry_failure" else None
        ),
        "workflow_id": None,
        "task_id": None,
        "flowmesh_status": "FAILED",
        "final_answer": None,
        "task_success": None,
        "access_event_count": None,
        "accepted_access_count": None,
        "selected_representations": [],
        "access_events": [],
        "artifact_delivery_required": None,
        "artifact_delivery_complete": None,
        "artifact_delivery_failure_count": 0,
        "artifact_delivery_failures": [],
        "error_type": type(exc).__name__,
        "error_message": _safe_error_message(exc),
    }


def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def load_pilot_records(
    path: str | Path,
    *,
    repair_truncated_tail: bool = True,
) -> list[dict[str, Any]]:
    """Load durable pilot records.

    The batch runner keeps the historical tail-repair behaviour enabled.
    Read-only analysis disables it so auditing can never alter its source.
    """
    source = Path(path)
    if not source.exists():
        return []
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    mode = "r+" if repair_truncated_tail else "r"
    with source.open(mode, encoding="utf-8", newline="") as handle:
        lines = handle.readlines()
        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                if (
                    repair_truncated_tail
                    and line_number == len(lines)
                    and not line.endswith("\n")
                ):
                    # A process can die inside the final append. That fragment
                    # never became a record, so discard only this provably
                    # truncated tail and let Gateway recovery decide whether
                    # the corresponding session needs reconstruction.
                    handle.seek(0)
                    handle.writelines(lines[: line_number - 1])
                    handle.truncate()
                    handle.flush()
                    os.fsync(handle.fileno())
                    break
                raise RuntimeError(
                    f"invalid pilot JSONL at {source}:{line_number}: {exc}"
                ) from exc
            if not isinstance(record, dict):
                raise RuntimeError(
                    f"pilot JSONL entry at {source}:{line_number} is not an object"
                )
            if (
                repair_truncated_tail
                and line_number == len(lines)
                and not line.endswith("\n")
            ):
                handle.seek(0, os.SEEK_END)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            trial_key = record.get("trial_key")
            if not isinstance(trial_key, str) or not trial_key:
                raise RuntimeError(
                    f"pilot JSONL entry at {source}:{line_number} has no trial_key"
                )
            if trial_key in seen:
                raise RuntimeError(
                    f"pilot JSONL contains duplicate trial_key: {trial_key}"
                )
            seen.add(trial_key)
            records.append(record)
    return records


def _mean_or_blank(values: Iterable[float]) -> float | str:
    collected = list(values)
    return round(mean(collected), 6) if collected else ""


def _temporary_sibling(path: Path) -> Path:
    return path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")


def summarize_pilot_records(
    records: Iterable[dict[str, Any]],
    trials: Iterable[PilotTrial],
) -> list[dict[str, Any]]:
    planned: Counter[tuple[str, str, float]] = Counter(
        (trial.design_id, trial.quote_profile_id, trial.latency_multiplier)
        for trial in trials
    )
    groups: dict[
        tuple[str, str, float], list[dict[str, Any]]
    ] = defaultdict(list)
    for record in records:
        groups[
            (
                record["design_id"],
                record["quote_profile_id"],
                record["latency_multiplier"],
            )
        ].append(record)

    rows: list[dict[str, Any]] = []
    for design_id, profile_id, latency_multiplier in sorted(planned):
        group_key = (design_id, profile_id, latency_multiplier)
        sessions = groups.get(group_key, [])
        completed = [
            record for record in sessions if record["outcome_type"] == "completed"
        ]
        evaluable = [
            record for record in completed if record.get("task_success") is not None
        ]
        accessed = [
            record
            for record in completed
            if (record.get("accepted_access_count") or 0) > 0
        ]
        selected_counts = Counter(
            representation
            for record in completed
            for representation in record.get("selected_representations", [])
        )
        accepted_events = [
            event
            for record in completed
            for event in record.get("access_events", [])
            if event.get("accepted")
        ]
        rows.append(
            {
                "design_id": design_id,
                "quote_profile_id": profile_id,
                "latency_multiplier": latency_multiplier,
                "planned_trials": planned[group_key],
                "attempted_trials": len(sessions),
                "completed_trials": len(completed),
                "infrastructure_failures": sum(
                    record["outcome_type"] == "infrastructure_failure"
                    for record in sessions
                ),
                "telemetry_failures": sum(
                    record["outcome_type"] == "telemetry_failure"
                    for record in sessions
                ),
                "artifact_delivery_failures": sum(
                    record["outcome_type"] == "artifact_delivery_failure"
                    for record in sessions
                ),
                "completion_rate": (
                    round(len(completed) / len(sessions), 6) if sessions else ""
                ),
                "access_rate_completed": (
                    round(len(accessed) / len(completed), 6) if completed else ""
                ),
                "no_access_count": len(completed) - len(accessed),
                "evaluable_task_count": len(evaluable),
                "task_success_rate": (
                    round(
                        sum(bool(record["task_success"]) for record in evaluable)
                        / len(evaluable),
                        6,
                    )
                    if evaluable
                    else ""
                ),
                "multimodal_digest_access_rate": (
                    round(
                        selected_counts["multimodal_digest"] / len(completed),
                        6,
                    )
                    if completed
                    else ""
                ),
                "selection_counts_json": json.dumps(
                    dict(sorted(selected_counts.items())),
                    sort_keys=True,
                ),
                "mean_realized_cost_per_access": _mean_or_blank(
                    float(event.get("realized_cost", 0.0))
                    for event in accepted_events
                ),
                "mean_felt_latency_ms_per_access": _mean_or_blank(
                    float(event["felt_latency_ms"])
                    for event in accepted_events
                    if event.get("felt_latency_ms") is not None
                ),
                "mean_data_agent_service_latency_ms_per_access": (
                    _mean_or_blank(
                        float(event["data_agent_service_latency_ms"])
                        for event in accepted_events
                        if event.get("data_agent_service_latency_ms") is not None
                    )
                ),
                "mean_data_agent_fetch_latency_ms_per_access": (
                    _mean_or_blank(
                        float(event["data_agent_fetch_latency_ms"])
                        for event in accepted_events
                        if event.get("data_agent_fetch_latency_ms") is not None
                    )
                ),
                "mean_data_agent_controlled_delay_ms_per_access": (
                    _mean_or_blank(
                        float(event["data_agent_controlled_delay_ms"])
                        for event in accepted_events
                        if event.get("data_agent_controlled_delay_ms") is not None
                    )
                ),
                "mean_artifact_bytes_sent_per_access": _mean_or_blank(
                    float(event.get("artifact_bytes_sent", 0))
                    for event in accepted_events
                ),
                "mean_artifact_transfer_latency_ms_per_access": _mean_or_blank(
                    float(event.get("artifact_transfer_latency_ms", 0.0))
                    for event in accepted_events
                ),
            }
        )
    return rows


def summarize_pilot_records_by_workload(
    records: Iterable[dict[str, Any]],
    trials: Iterable[PilotTrial],
) -> list[dict[str, Any]]:
    """Produce the same cell metrics without hiding task heterogeneity.

    The first real pilot showed that the quote response was concentrated in a
    single question type. Aggregating only by quote can therefore manufacture
    a misleading global elasticity estimate. Phase B treats workload as a
    pre-registered stratum and emits one row per physical-design, workload,
    quote, and latency cell.
    """
    trials_by_workload: dict[str, list[PilotTrial]] = defaultdict(list)
    records_by_workload: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trial in trials:
        trials_by_workload[trial.workload_id].append(trial)
    for record in records:
        records_by_workload[str(record["workload_id"])].append(record)

    rows: list[dict[str, Any]] = []
    for workload_id in sorted(trials_by_workload):
        for row in summarize_pilot_records(
            records_by_workload.get(workload_id, []),
            trials_by_workload[workload_id],
        ):
            rows.append({"workload_id": workload_id, **row})
    return rows


def _exact_two_sided_sign_pvalue(left_only: int, right_only: int) -> float:
    discordant = left_only + right_only
    if discordant == 0:
        return 1.0
    tail = sum(
        math.comb(discordant, index)
        for index in range(min(left_only, right_only) + 1)
    ) / (2**discordant)
    return round(min(1.0, 2.0 * tail), 8)


def summarize_paired_contrasts(
    records: Iterable[dict[str, Any]],
    trials: Iterable[PilotTrial],
    *,
    primary_representation_id: str = "multimodal_digest",
) -> list[dict[str, Any]]:
    """Summarize matched one-factor contrasts from the frozen block design.

    Only pairs that differ in exactly one of physical design, quote profile, or
    latency multiplier are compared. Results are emitted both per workload and
    pooled. The exact sign-test p-value is descriptive engineering evidence;
    the experiment protocol must still pre-register the primary contrast and
    account for repeated workloads before making a paper-level claim.
    """
    trial_blocks: dict[tuple[str, int], list[PilotTrial]] = defaultdict(list)
    for trial in trials:
        trial_blocks[(trial.workload_id, trial.repetition)].append(trial)
    records_by_key = {
        str(record["trial_key"]): record for record in records
    }
    aggregated: dict[tuple[Any, ...], list[tuple[Any, Any]]] = defaultdict(list)

    for (workload_id, _), block in trial_blocks.items():
        for left, right in combinations(
            sorted(
                block,
                key=lambda item: (
                    item.design_id,
                    item.quote_profile_id,
                    item.latency_multiplier,
                ),
            ),
            2,
        ):
            changed = [
                name
                for name, left_value, right_value in (
                    ("physical", left.design_id, right.design_id),
                    ("quote", left.quote_profile_id, right.quote_profile_id),
                    (
                        "latency",
                        left.latency_multiplier,
                        right.latency_multiplier,
                    ),
                )
                if left_value != right_value
            ]
            if len(changed) != 1:
                continue
            contrast_key = (
                changed[0],
                left.design_id,
                right.design_id,
                left.quote_profile_id,
                right.quote_profile_id,
                left.latency_multiplier,
                right.latency_multiplier,
            )
            pair = (
                records_by_key.get(left.trial_key),
                records_by_key.get(right.trial_key),
            )
            aggregated[(workload_id, *contrast_key)].append(pair)
            aggregated[("__pooled__", *contrast_key)].append(pair)

    rows: list[dict[str, Any]] = []
    for key in sorted(aggregated, key=lambda value: tuple(map(str, value))):
        (
            workload_id,
            contrast_type,
            left_design,
            right_design,
            left_profile,
            right_profile,
            left_latency,
            right_latency,
        ) = key
        pairs = aggregated[key]
        attempted = [pair for pair in pairs if all(pair)]
        complete = [
            pair
            for pair in attempted
            if all(record["outcome_type"] == "completed" for record in pair)
        ]

        def selected(record: dict[str, Any]) -> bool:
            return primary_representation_id in record.get(
                "selected_representations", []
            )

        left_only = sum(
            selected(left) and not selected(right)
            for left, right in complete
        )
        right_only = sum(
            not selected(left) and selected(right)
            for left, right in complete
        )
        left_rate = _mean_or_blank(float(selected(left)) for left, _ in complete)
        right_rate = _mean_or_blank(float(selected(right)) for _, right in complete)

        def completed_value(
            record: dict[str, Any],
            field: str,
        ) -> float | None:
            value = record.get(field)
            if value is None:
                return None
            return float(value)

        def access_total(
            record: dict[str, Any], field: str
        ) -> float | None:
            values = [
                float(event[field])
                for event in record.get("access_events", [])
                if event.get("accepted") and event.get(field) is not None
            ]
            return sum(values) if values else None

        def mean_pair_delta(
            extractor: Callable[[dict[str, Any]], float | None],
        ) -> float | str:
            deltas = []
            for left, right in complete:
                left_value = extractor(left)
                right_value = extractor(right)
                if left_value is not None and right_value is not None:
                    deltas.append(right_value - left_value)
            return _mean_or_blank(deltas)

        rows.append(
            {
                "workload_id": workload_id,
                "contrast_type": contrast_type,
                "left_design_id": left_design,
                "right_design_id": right_design,
                "left_quote_profile_id": left_profile,
                "right_quote_profile_id": right_profile,
                "left_latency_multiplier": left_latency,
                "right_latency_multiplier": right_latency,
                "primary_representation_id": primary_representation_id,
                "planned_pairs": len(pairs),
                "attempted_pairs": len(attempted),
                "complete_pairs": len(complete),
                "left_primary_access_rate": left_rate,
                "right_primary_access_rate": right_rate,
                "primary_access_rate_delta_right_minus_left": (
                    round(float(right_rate) - float(left_rate), 6)
                    if left_rate != "" and right_rate != ""
                    else ""
                ),
                "discordant_left_only": left_only,
                "discordant_right_only": right_only,
                "exact_two_sided_sign_pvalue": (
                    _exact_two_sided_sign_pvalue(left_only, right_only)
                    if complete
                    else ""
                ),
                "mean_task_success_delta_right_minus_left": mean_pair_delta(
                    lambda record: completed_value(record, "task_success")
                ),
                "mean_realized_cost_delta_right_minus_left": mean_pair_delta(
                    lambda record: access_total(record, "realized_cost")
                ),
                "mean_felt_latency_ms_delta_right_minus_left": mean_pair_delta(
                    lambda record: access_total(record, "felt_latency_ms")
                ),
                "mean_service_latency_ms_delta_right_minus_left": mean_pair_delta(
                    lambda record: access_total(
                        record,
                        "data_agent_service_latency_ms",
                    )
                ),
            }
        )
    return rows


def _write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(path)
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            if rows:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_json(data: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(path)
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(
                json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _package_version(name: str) -> str | None:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _git_revision(repository: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def _git_dirty(repository: Path) -> bool | None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return bool(result.stdout.strip())


def _trial_plan_payload(
    pilot: FlowMeshPilotConfig,
    system: SystemConfig,
    trials: list[PilotTrial],
    *,
    repetitions: int,
    randomization_seed: int,
) -> dict[str, Any]:
    return {
        "schema_version": PILOT_SCHEMA_VERSION,
        "experiment_id": pilot.experiment_id,
        "pilot_config_sha256": pilot.source_sha256,
        "system_config_sha256": sha256(system.source_path.read_bytes()).hexdigest(),
        "repetitions": repetitions,
        "randomization_seed": randomization_seed,
        "randomization_strategy": "blocked_by_workload_and_repetition",
        "pathfinder_seeds_paired_across_cells": True,
        "llm_sampling_seed_controlled": False,
        "trial_count": len(trials),
        "trials": [trial.to_public_dict() for trial in trials],
    }


def _ensure_trial_plan(path: Path, payload: dict[str, Any]) -> None:
    if not path.exists():
        _write_json(payload, path)
        return
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"existing trial plan is invalid JSON: {path}") from exc
    if existing != payload:
        raise RuntimeError(
            "existing trial plan does not match this invocation; use a new "
            f"output directory instead of mixing experiments: {path}"
        )


@contextmanager
def _exclusive_pilot_lock(path: Path) -> Iterator[None]:
    """Prevent two processes from submitting the same frozen trial."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    if handle.tell() == 0:
        handle.write(b"0")
        handle.flush()
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise RuntimeError(
            "another Pathfinder pilot process already holds the output lock: "
            f"{path}"
        ) from exc
    try:
        yield
    finally:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _run_flowmesh_pilot_unlocked(
    *,
    pilot: FlowMeshPilotConfig,
    system: SystemConfig,
    adapter: AgentAdapterProtocol,
    output_dir: str | Path,
    repetitions: int | None = None,
    randomization_seed: int | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    validate_flowmesh_pilot_config(pilot, system)
    if not adapter.settings.pinning_requested:
        raise FlowMeshPilotConfigError(
            "real pilot batches require --worker-id or --worker-alias; "
            "uncontrolled scheduling would contaminate the experiment"
        )

    resolved_repetitions = (
        pilot.repetitions if repetitions is None else repetitions
    )
    resolved_randomization_seed = (
        pilot.randomization_seed
        if randomization_seed is None
        else randomization_seed
    )
    trials = build_trial_plan(
        pilot,
        repetitions=resolved_repetitions,
        randomization_seed=resolved_randomization_seed,
    )
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    records_path = output_path / "runs.jsonl"
    summary_path = output_path / "summary.csv"
    workload_summary_path = output_path / "summary_by_workload.csv"
    paired_contrasts_path = output_path / "paired_contrasts.csv"
    trial_plan_path = output_path / "trial_plan.json"
    manifest_path = output_path / "manifest.json"

    plan_payload = _trial_plan_payload(
        pilot,
        system,
        trials,
        repetitions=resolved_repetitions,
        randomization_seed=resolved_randomization_seed,
    )
    _ensure_trial_plan(trial_plan_path, plan_payload)
    records = load_pilot_records(records_path)
    expected_keys = {trial.trial_key for trial in trials}
    recorded_keys = {str(record["trial_key"]) for record in records}
    unexpected = recorded_keys - expected_keys
    if unexpected:
        raise RuntimeError(
            "runs.jsonl contains trials outside the frozen plan: "
            + ", ".join(sorted(unexpected))
        )

    previous_manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            previous_manifest = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"existing pilot manifest is invalid JSON: {manifest_path}"
            ) from exc
    started_at = previous_manifest.get("started_at") or _utc_now()
    repository = Path(__file__).resolve().parents[3]
    git_revision = _git_revision(repository)
    git_dirty = _git_dirty(repository)
    flowmesh_sdk_version = _package_version("flowmesh-sdk")
    mcp_version = _package_version("mcp")
    adapter_gateway = getattr(adapter, "gateway", None)
    adapter_backend = getattr(adapter_gateway, "backend", None)
    telemetry_quiescence_timeout_seconds = getattr(
        adapter_backend,
        "telemetry_quiescence_timeout_seconds",
        None,
    )

    def write_progress(status: str) -> dict[str, Any]:
        rows = summarize_pilot_records(records, trials)
        _write_csv(rows, summary_path)
        workload_rows = summarize_pilot_records_by_workload(records, trials)
        _write_csv(workload_rows, workload_summary_path)
        contrast_rows = summarize_paired_contrasts(records, trials)
        _write_csv(contrast_rows, paired_contrasts_path)
        outcome_counts = Counter(
            str(record["outcome_type"]) for record in records
        )
        manifest = {
            "schema_version": PILOT_SCHEMA_VERSION,
            "experiment_id": pilot.experiment_id,
            "status": status,
            "started_at": started_at,
            "updated_at": _utc_now(),
            "planned_trials": len(trials),
            "recorded_trials": len(records),
            "remaining_trials": len(trials) - len(records),
            "outcome_counts": dict(sorted(outcome_counts.items())),
            "pilot_config_path": str(pilot.source_path),
            "pilot_config_sha256": pilot.source_sha256,
            "system_config_path": str(system.source_path),
            "system_config_sha256": plan_payload["system_config_sha256"],
            "trial_plan_path": str(trial_plan_path),
            "records_path": str(records_path),
            "summary_path": str(summary_path),
            "workload_summary_path": str(workload_summary_path),
            "paired_contrasts_path": str(paired_contrasts_path),
            "gateway_state_is_external_to_records": True,
            "git_revision": git_revision,
            "git_dirty": git_dirty,
            "python_version": platform.python_version(),
            "flowmesh_sdk_version": flowmesh_sdk_version,
            "mcp_version": mcp_version,
            "agent_config_name": adapter.settings.agent_config_name,
            "worker_id": adapter.settings.worker_id,
            "worker_alias": adapter.settings.worker_alias,
            "workflow_validation_enabled": (
                adapter.settings.validate_before_submit
            ),
            "task_timeout_seconds": adapter.settings.task_timeout_seconds,
            "poll_interval_seconds": adapter.settings.poll_interval_seconds,
            "telemetry_quiescence_timeout_seconds": (
                telemetry_quiescence_timeout_seconds
            ),
            "secrets_recorded": False,
        }
        _write_json(manifest, manifest_path)
        return manifest

    write_progress("RUNNING")
    for trial in trials:
        if trial.trial_key in recorded_keys:
            continue
        started_at_trial = _utc_now()
        monotonic_start = time.monotonic()
        offered_quotes = _offered_quotes(system, trial)
        recovered_from_gateway_state = False
        try:
            request = FlowMeshAgentRunRequest(
                question=trial.question,
                design_id=trial.design_id,
                task_class_id=trial.task_class_id,
                quote_profile_id=trial.quote_profile_id,
                latency_multiplier=trial.latency_multiplier,
                seed=trial.seed,
                trial_id=trial.trial_id,
                session_id=trial.session_id,
                object_id=trial.object_id,
            )
            recover = getattr(adapter, "recover", None)
            result = recover(trial.session_id) if callable(recover) else None
            recovered_from_gateway_state = result is not None
            if result is None:
                result = adapter.run(request)
        except Exception as exc:
            record = _record_for_failure(
                pilot,
                trial,
                exc,
                started_at=started_at_trial,
                finished_at=_utc_now(),
                duration_seconds=time.monotonic() - monotonic_start,
                offered_quotes=offered_quotes,
            )
        else:
            record = _record_for_success(
                pilot,
                trial,
                result,
                started_at=started_at_trial,
                finished_at=_utc_now(),
                duration_seconds=time.monotonic() - monotonic_start,
                offered_quotes=offered_quotes,
                recovered_from_gateway_state=recovered_from_gateway_state,
            )
        _append_jsonl(records_path, record)
        records.append(record)
        recorded_keys.add(trial.trial_key)
        write_progress("RUNNING")
        if progress_callback is not None:
            progress_callback(
                {
                    "trial_key": trial.trial_key,
                    "order_index": trial.order_index,
                    "outcome_type": record["outcome_type"],
                    "recorded_trials": len(records),
                    "planned_trials": len(trials),
                }
            )

    return write_progress("COMPLETE")


def run_flowmesh_pilot(
    *,
    pilot: FlowMeshPilotConfig,
    system: SystemConfig,
    adapter: AgentAdapterProtocol,
    output_dir: str | Path,
    repetitions: int | None = None,
    randomization_seed: int | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run or resume a randomized real-FlowMesh pilot.

    This function only submits sessions through the supplied adapter. It does
    not create, start, stop, or remove workers and does not manage Data Agent
    or MCP server processes. One output directory admits only one active
    runner process.
    """
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    with _exclusive_pilot_lock(output_path / ".pilot.lock"):
        return _run_flowmesh_pilot_unlocked(
            pilot=pilot,
            system=system,
            adapter=adapter,
            output_dir=output_path,
            repetitions=repetitions,
            randomization_seed=randomization_seed,
            progress_callback=progress_callback,
        )
