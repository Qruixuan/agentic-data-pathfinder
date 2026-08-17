from __future__ import annotations

import json
import os
import uuid
from collections import Counter
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

from .pilot import (
    PILOT_SCHEMA_VERSION,
    PilotTrial,
    _sanitized_access_event,
    _write_csv,
    _write_json,
    artifact_delivery_failures,
    load_pilot_records,
    summarize_paired_contrasts,
    summarize_pilot_records,
    summarize_pilot_records_by_workload,
)


ANALYSIS_SCHEMA_VERSION = "pathfinder.flowmesh-pilot-analysis/v1alpha1"


def _load_frozen_trials(path: Path) -> list[PilotTrial]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(f"frozen trial plan does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"frozen trial plan is invalid JSON: {path}") from exc
    if payload.get("schema_version") != PILOT_SCHEMA_VERSION:
        raise RuntimeError(
            "frozen trial plan has an unsupported schema_version: "
            f"{payload.get('schema_version')!r}"
        )
    rows = payload.get("trials")
    if not isinstance(rows, list):
        raise RuntimeError("frozen trial plan has no trials array")
    trials: list[PilotTrial] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise RuntimeError(f"frozen trial {index} is not an object")
        values = dict(row)
        accepted = values.get("accepted_answer_substrings", [])
        if not isinstance(accepted, list) or not all(
            isinstance(value, str) for value in accepted
        ):
            raise RuntimeError(
                f"frozen trial {index} has invalid accepted answers"
            )
        values["accepted_answer_substrings"] = tuple(accepted)
        try:
            trials.append(PilotTrial(**values))
        except TypeError as exc:
            raise RuntimeError(
                f"frozen trial {index} does not match the pilot schema: {exc}"
            ) from exc
    return trials


def audit_pilot_records(
    records: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Create a capability-safe analysis view with delivery failures closed."""
    audited: list[dict[str, Any]] = []
    for source in records:
        record = dict(source)
        events = [
            _sanitized_access_event(dict(event))
            for event in record.get("access_events", [])
        ]
        failures = artifact_delivery_failures(events)
        artifact_required = any(
            event.get("accepted") and event.get("artifact_handle_sha256")
            for event in events
        )
        record["access_events"] = events
        outcome_type = record.get("outcome_type")
        if outcome_type in {"completed", "artifact_delivery_failure"}:
            record["artifact_delivery_required"] = bool(artifact_required)
            record["artifact_delivery_complete"] = not failures
            record["artifact_delivery_failure_count"] = len(failures)
            record["artifact_delivery_failures"] = failures
        else:
            record.setdefault("artifact_delivery_required", None)
            record.setdefault("artifact_delivery_complete", None)
            record.setdefault("artifact_delivery_failure_count", 0)
            record.setdefault("artifact_delivery_failures", [])
        if failures and outcome_type == "completed":
            record["original_outcome_type"] = "completed"
            record["outcome_type"] = "artifact_delivery_failure"
            record["task_success"] = None
            record["error_type"] = "ArtifactDeliveryFailure"
            record["error_message"] = (
                f"{len(failures)} accepted artifact access(es) did not "
                "complete a full artifact download"
            )
        audited.append(record)
    return audited


def _task_metrics(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    evaluable = [
        record
        for record in records
        if record.get("outcome_type") == "completed"
        and record.get("task_success") is not None
    ]
    successes = sum(bool(record["task_success"]) for record in evaluable)
    return {
        "evaluable_task_count": len(evaluable),
        "task_success_count": successes,
        "task_success_rate": (
            round(successes / len(evaluable), 6) if evaluable else None
        ),
    }


def _write_jsonl(records: Iterable[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def analyze_flowmesh_pilot(
    input_dir: str | Path,
    *,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Audit one frozen pilot without modifying its original artifacts."""
    source_dir = Path(input_dir).resolve()
    destination = (
        Path(output_dir).resolve()
        if output_dir is not None
        else source_dir.with_name(f"{source_dir.name}-analysis")
    )
    if destination == source_dir:
        raise ValueError("analysis output directory must differ from input")

    records_path = source_dir / "runs.jsonl"
    trial_plan_path = source_dir / "trial_plan.json"
    try:
        source_bytes = records_path.read_bytes()
    except FileNotFoundError as exc:
        raise RuntimeError(f"pilot records do not exist: {records_path}") from exc
    source_digest = sha256(source_bytes).hexdigest()
    records = load_pilot_records(
        records_path,
        repair_truncated_tail=False,
    )
    trials = _load_frozen_trials(trial_plan_path)
    expected_keys = {trial.trial_key for trial in trials}
    unexpected = {
        str(record.get("trial_key")) for record in records
    } - expected_keys
    if unexpected:
        raise RuntimeError(
            "runs.jsonl contains trials outside the frozen plan: "
            + ", ".join(sorted(unexpected))
        )

    audited = audit_pilot_records(records)
    destination.mkdir(parents=True, exist_ok=True)
    audited_records_path = destination / "audited_runs.jsonl"
    summary_path = destination / "summary.csv"
    workload_summary_path = destination / "summary_by_workload.csv"
    contrasts_path = destination / "paired_contrasts.csv"
    report_path = destination / "audit_report.json"
    _write_jsonl(audited, audited_records_path)
    _write_csv(summarize_pilot_records(audited, trials), summary_path)
    _write_csv(
        summarize_pilot_records_by_workload(audited, trials),
        workload_summary_path,
    )
    _write_csv(
        summarize_paired_contrasts(audited, trials),
        contrasts_path,
    )

    raw_outcomes = Counter(
        str(record.get("outcome_type")) for record in records
    )
    audited_outcomes = Counter(
        str(record.get("outcome_type")) for record in audited
    )
    reclassified = sum(
        record.get("original_outcome_type") == "completed"
        and record.get("outcome_type") == "artifact_delivery_failure"
        for record in audited
    )
    final_source_digest = sha256(records_path.read_bytes()).hexdigest()
    report = {
        "schema_version": ANALYSIS_SCHEMA_VERSION,
        "input_dir": str(source_dir),
        "output_dir": str(destination),
        "source_records_path": str(records_path),
        "source_records_sha256": source_digest,
        "source_records_unchanged": final_source_digest == source_digest,
        "record_count": len(records),
        "planned_trial_count": len(trials),
        "raw_outcome_counts": dict(sorted(raw_outcomes.items())),
        "audited_outcome_counts": dict(sorted(audited_outcomes.items())),
        "newly_reclassified_artifact_delivery_failures": reclassified,
        "artifact_delivery_failure_count": audited_outcomes.get(
            "artifact_delivery_failure", 0
        ),
        "raw_task_metrics": _task_metrics(records),
        "audited_task_metrics": _task_metrics(audited),
        "audited_records_path": str(audited_records_path),
        "summary_path": str(summary_path),
        "workload_summary_path": str(workload_summary_path),
        "paired_contrasts_path": str(contrasts_path),
        "raw_artifact_handles_recorded_in_analysis": False,
        "audit_report_path": str(report_path),
    }
    _write_json(report, report_path)
    return report
