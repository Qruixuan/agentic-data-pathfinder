"""Read-only evaluation of a complete, frozen distributed workload pilot.

Reuse the production plan, record, route, scoring and cost contracts. Never
open a runner to validate a run: opening it may repair its journal or plan.
Successful attempts are normal ledger entries, not recovery failures.
"""

from __future__ import annotations

import csv
import io
import json
import math
import platform
from collections import Counter, defaultdict
from hashlib import sha256
from pathlib import Path
from statistics import mean
from tempfile import TemporaryDirectory
from typing import Any

from ..distributed.cost import COST_COMPONENT_IDS, record_total_cost
from ..distributed.execution import (
    ATTEMPT_LEDGER, CANONICAL_LEDGER, CELL_JOURNAL, CELL_STATES,
    PLAN_DOCUMENT, FlowMeshDistributedSessionExecutor,
    build_frozen_plan_document, load_durable_canonical_records,
)
from ..distributed.measurements import (
    build_measured_cost_ledger, load_measurement_manifest,
    transferred_bytes_for_event,
)
from ..distributed.preregistration import load_distributed_pilot_preregistration
from ..distributed.registry import load_endpoint_registry
from ..distributed.runner import PILOT_RUN_STATE_SCHEMA_VERSION, build_distributed_trial_plan
from ..integrations.flowmesh.pilot import (
    _evaluate_answer, _sanitized_access_event, artifact_delivery_failures,
)


EVALUATION_SCHEMA = "pathfinder.workload-evaluation/v1alpha1"
LATENCY_FIELDS = (
    "felt_latency_ms", "data_agent_service_latency_ms",
    "data_agent_fetch_latency_ms", "data_agent_controlled_delay_ms",
    "artifact_transfer_latency_ms",
)
RUN_SUMMARY = "run_summary.jsonl"


class EvaluationError(ValueError):
    """The supplied snapshot cannot support a complete paired evaluation."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EvaluationError(message)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def _read(path: Path) -> Any:
    def invalid_number(value: str) -> None:
        raise EvaluationError(f"{path.name}: non-finite JSON number")

    def unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{path.name}: duplicate JSON key {key}")
            result[key] = value
        return result

    try:
        content = path.read_text(encoding="utf-8")
        kwargs = dict(parse_constant=invalid_number, object_pairs_hook=unique_keys)
        if path.suffix == ".jsonl":
            rows = [json.loads(line, **kwargs) for line in content.splitlines()
                    if line.strip()]
            _require(all(isinstance(row, dict) for row in rows),
                     f"{path.name}: every row must be an object")
            return rows
        return json.loads(content, **kwargs)
    except (OSError, json.JSONDecodeError, UnicodeError) as exc:
        raise EvaluationError(f"cannot read valid {path.name}") from exc


def _number(value: Any, name: str, *, integer: bool = False) -> float:
    _require(type(value) in (int, float), f"{name}: expected a numeric value")
    try:
        converted = float(value)
    except OverflowError as exc:
        raise EvaluationError(f"{name}: invalid quantity") from exc
    _require(math.isfinite(converted) and converted >= 0, f"{name}: invalid quantity")
    if integer:
        _require(type(value) is int, f"{name}: expected an integer")
    return converted


def _stats(values: list[float | None]) -> dict[str, Any]:
    observed = sorted(value for value in values if value is not None)

    def quantile(q: float) -> float | None:
        if not observed:
            return None
        index = (len(observed) - 1) * q
        lo, hi = math.floor(index), math.ceil(index)
        return observed[lo] + (observed[hi] - observed[lo]) * (index - lo)

    return {
        "count": len(observed), "missing_count": len(values) - len(observed),
        "mean": mean(observed) if observed else None,
        "median": quantile(0.5), "p95": quantile(0.95),
    }


def _audit_attempts(rows: list[dict], trials: dict, records: dict) -> dict:
    by_cell: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = row.get("trial_key")
        _require(isinstance(key, str) and key in trials,
                 "attempt ledger: unknown trial_key")
        _number(row.get("attempt"), "attempt number", integer=True)
        _require(row["attempt"] == len(by_cell[key]) + 1,
                 f"attempt ledger: duplicate or non-contiguous attempt for {key}")
        for field in ("succeeded", "telemetry_complete", "artifact_selected",
                      "artifact_delivery_complete"):
            _require(type(row.get(field)) is bool,
                     f"attempt ledger: {field} must be a literal boolean")
        kind = row.get("observation_class")
        _require(kind in ("canonical", "infrastructure", "recovery_attempt"),
                 "attempt ledger: unknown observation_class")
        _require(not any(item["observation_class"] == "canonical"
                         for item in by_cell[key]),
                 f"attempt ledger: attempt after canonical completion for {key}")
        if kind == "canonical":
            _require(all(row[field] is True for field in (
                "succeeded", "telemetry_complete", "artifact_delivery_complete"
            )), "attempt ledger: incomplete canonical attempt")
            _require(row.get("failure_class") is None,
                     "attempt ledger: canonical attempt has a failure")
            _require(type(records[key].get("artifact_delivery_required")) is bool
                     and row["artifact_selected"] is
                     records[key].get("artifact_delivery_required"),
                     "attempt ledger: artifact selection disagrees with record")
        else:
            _require(row["succeeded"] is False,
                     "attempt ledger: noncanonical attempt claims success")
            _require(isinstance(row.get("failure_class"), str)
                     and bool(row["failure_class"]),
                     "attempt ledger: failed attempt has no failure_class")
        by_cell[key].append(row)
    _require(set(by_cell) == set(trials), "attempt ledger: missing planned cells")
    _require(all(items[-1]["observation_class"] == "canonical"
                 for items in by_cell.values()),
             "attempt ledger: not every cell has a final canonical attempt")
    return by_cell


def _audit_journal(rows: list[dict], trials: dict) -> Counter:
    latest: dict[str, str] = {}
    for row in rows:
        key, state = row.get("trial_key"), row.get("state")
        _require(isinstance(key, str) and key in trials,
                 "cell journal: unknown trial_key")
        _require(state in CELL_STATES, "cell journal: unknown state")
        _require(latest.get(key) != "COMPLETED" or state == "COMPLETED",
                 "cell journal: state regressed after completion")
        latest[key] = state
    _require(set(latest) == set(trials)
             and all(state == "COMPLETED" for state in latest.values()),
             "cell journal: snapshot is not fully COMPLETED; resume separately")
    return Counter(latest.values())


def _audit_summary(rows: list[dict], prereg: Any, plan: dict, provider: Any,
                   attempts: list[dict]) -> None:
    """Bind the measurements to the runtime, not just to its configuration.

    The production plan freezes workloads and endpoints but does not contain a
    measurement hash. The final runtime summary does. Without this check, an
    edited measurement kind/provenance could be substituted without necessarily
    changing the numeric cost ledger (for example when a conversion rate is 0).
    """
    _require(bool(rows), "run summary: final summary is missing")
    row = rows[-1]
    expected = {
        "schema_version": PILOT_RUN_STATE_SCHEMA_VERSION,
        "pilot_id": prereg.pilot_id, "status": "COMPLETE",
        "complete": True, "oracle_complete": True,
        "posthoc": prereg.posthoc, "confirmatory": False,
        "eligible_for_scientific_claims": False,
        "measurement_manifest_sha256": provider.manifest_sha256,
        "workload_content_sha256": plan["workload_content_sha256"],
        "workload_id_manifest_sha256": plan["workload_id_manifest_sha256"],
        "cost_basis": prereg.cost_basis,
        "planned_trial_count": prereg.planned_trial_count,
        "completed_canonical_count": prereg.planned_trial_count,
        "remaining_trial_count": 0,
        "independent_workload_count": prereg.independent_workload_count,
        "complete_workload_count": prereg.independent_workload_count,
        "infrastructure_failure_count": sum(a["observation_class"] == "infrastructure" for a in attempts),
        "recovery_attempt_count": sum(a["observation_class"] == "recovery_attempt" for a in attempts),
    }
    for field, value in expected.items():
        _require(type(row.get(field)) is type(value) and row[field] == value,
                 f"run summary: {field} disagrees with audited inputs")
    blocks = row.get("complete_workload_blocks")
    _require(isinstance(blocks, list) and all(isinstance(b, str) for b in blocks)
             and sorted(blocks) == sorted(prereg.workload_ids),
             "run summary: complete workload blocks disagree with plan")


def _audit_record(record: dict, trial: Any, workload: dict, registry: Any,
                  provider: Any, model: Any, attempt: int) -> dict:
    key = trial.trial_key
    for field, expected in trial.to_public_dict().items():
        _require(type(record[field]) is type(expected),
                 f"{key}: identity field {field} has the wrong type")
    for field in ("object_id", "question"):
        _require(record.get(field) == workload.get(field),
                 f"{key}: {field} differs from frozen workload")
    answers = workload.get("accepted_answer_substrings", [])
    _require(isinstance(answers, list)
             and all(isinstance(value, str) and value.strip() for value in answers),
             f"{key}: invalid accepted_answer_substrings")
    _require(record.get("accepted_answer_substrings") == answers,
             f"{key}: scoring labels differ from frozen workload")
    _require(record.get("final_answer") is None
             or isinstance(record["final_answer"], str), f"{key}: invalid answer")
    expected_score = _evaluate_answer(record.get("final_answer") or "", tuple(answers))
    _require("task_success" in record and record["task_success"] is expected_score,
             f"{key}: task_success disagrees with frozen substring scorer")
    for field, default in (("task_class_id", "video_qa"),
                           ("quote_profile_id", "as_designed"),
                           ("latency_multiplier", 1)):
        _require(record.get(field) == workload.get(field, default),
                 f"{key}: {field} differs from frozen workload")
    events = record.get("access_events")
    _require(isinstance(events, list), f"{key}: access_events must be an array")
    accepted: list[dict] = []
    routes = []
    payload_bytes = network_bytes = artifact_bytes = 0.0
    missing_destination = 0
    expected_session = FlowMeshDistributedSessionExecutor.session_id_for_attempt(
        trial, attempt
    )
    for raw in events:
        _require(isinstance(raw, dict) and type(raw.get("accepted")) is bool,
                 f"{key}: access accepted flag must be a literal boolean")
        if not raw["accepted"]:
            continue
        event = _sanitized_access_event(raw)
        _require(event.get("object_id") == record["object_id"],
                 f"{key}: event object does not match the trial")
        if "session_id" in event:
            _require(event["session_id"] == expected_session,
                     f"{key}: event belongs to a different attempt session")
        representation = event.get("representation_id")
        _require(isinstance(representation, str) and bool(representation),
                 f"{key}: event representation is missing")
        route = registry.route(design_id=trial.design_id,
                               representation_id=representation)
        for field in ("endpoint_id", "source_node_id"):
            _require(event.get(field) == getattr(route, field),
                     f"{key}: {field} disagrees with frozen placement")
        destination = event.get("destination_execution_node_id")
        if destination is None:
            missing_destination += 1
            basis = "inferred-from-frozen-registry"
        else:
            _require(destination == route.destination_execution_node_id,
                     f"{key}: destination disagrees with frozen placement")
            basis = "recorded"
        routes.append((route.endpoint_id, route.source_node_id,
                       route.destination_execution_node_id, basis))
        _number(event.get("realized_cost"), f"{key}: realized_cost")
        _number(event.get("bytes_read"), f"{key}: bytes_read", integer=True)
        for field in LATENCY_FIELDS:
            if event.get(field) is not None:
                _number(event[field], f"{key}: {field}")
        for field in ("artifact_bytes_sent", "artifact_download_request_count",
                      "artifact_full_download_count"):
            if event.get(field) is not None:
                _number(event[field], f"{key}: {field}", integer=True)
        if event.get("artifact_handle_sha256"):
            fingerprint = event["artifact_handle_sha256"]
            _require(isinstance(fingerprint, str) and len(fingerprint) == 64
                     and all(c in "0123456789abcdef" for c in fingerprint),
                     f"{key}: invalid artifact handle fingerprint")
            for field in ("artifact_download_request_count",
                          "artifact_full_download_count", "artifact_bytes_sent"):
                _require(event.get(field, 0) is not None
                         and event.get(field, 0) > 0,
                         f"{key}: artifact delivery is incomplete ({field})")
        else:
            _require(not any(event.get(field, 0) for field in (
                "artifact_bytes_sent", "artifact_download_request_count",
                "artifact_full_download_count"
            )), f"{key}: artifact counters lack a handle fingerprint")
        payload = transferred_bytes_for_event(event, crosses_network=True)
        _require(payload is not None, f"{key}: payload bytes unavailable")
        payload_bytes += payload
        if registry.endpoint(route.endpoint_id).crosses_network:
            network_bytes += payload
        artifact_bytes += event.get("artifact_bytes_sent", 0) or 0
        accepted.append(event)
    _require(not artifact_delivery_failures(accepted),
             f"{key}: artifact_delivery_failure")
    required = any(event.get("artifact_handle_sha256") for event in accepted)
    _require(record.get("artifact_delivery_required") is required,
             f"{key}: artifact_delivery_required disagrees with events")
    for field, expected in (("access_event_count", len(events)),
                            ("accepted_access_count", len(accepted)),
                            ("artifact_delivery_failure_count", 0)):
        _require(type(record.get(field)) is int and record[field] == expected,
                 f"{key}: {field} disagrees with events")
    _require(record.get("selected_representations") ==
             [event["representation_id"] for event in accepted],
             f"{key}: selected_representations disagrees with events")
    _require(record.get("artifact_delivery_failures") == [],
             f"{key}: canonical record contains artifact delivery failures")
    ledger = record.get("cost_ledger")
    _require(isinstance(ledger, dict)
             and ledger.get("total_cost_available") is True,
             f"{key}: total-cost ledger is unavailable")
    components = ledger.get("components")
    _require(isinstance(components, dict) and set(components) == set(COST_COMPONENT_IDS),
             f"{key}: ledger must contain exactly five cost components")
    for name, component in components.items():
        _require(isinstance(component, dict) and component.get("available") is True,
                 f"{key}: cost component {name} unavailable")
        _number(component.get("value"), f"{key}: component {name}")
    total = record_total_cost(record)
    recomputed = build_measured_cost_ledger(
        record, model=model, provider=provider, design_id=trial.design_id,
        object_id=record["object_id"],
        # Match the production builder's measurement lookup, including the
        # no-accepted-access case with a routed rejection event.
        node_id=next((str(e["source_node_id"]) for e in events
                      if e.get("source_node_id")), ""),
        network_transport_for={key: endpoint.network_transport
                               for key, endpoint in registry.endpoints.items()},
    ).to_public_dict()
    _require(_json(ledger) == _json(recomputed),
             f"{key}: cost ledger disagrees with events, model or measurements")
    _require(math.isclose(total, sum(c["value"] for c in components.values()),
                         rel_tol=1e-12, abs_tol=1e-12),
             f"{key}: total_cost does not sum to its components")
    return {
        "record": record, "accepted": accepted, "routes": routes,
        "payload_bytes": payload_bytes, "network_bytes": network_bytes,
        "artifact_bytes": artifact_bytes, "missing_destination": missing_destination,
        "costs": {name: components[name]["value"] for name in COST_COMPONENT_IDS},
        "total_cost": total,
    }


def _group_summary(items: list[dict]) -> dict:
    scores = [item["record"]["task_success"] for item in items]
    evaluable = [score for score in scores if score is not None]
    events = [event for item in items for event in item["accepted"]]
    route_counts = Counter(route for item in items for route in item["routes"])
    return {
        "sessions": len(items), "evaluable_tasks": len(evaluable),
        "task_successes": sum(evaluable),
        "task_success_rate": mean(evaluable) if evaluable else None,
        "no_access_sessions": sum(not item["accepted"] for item in items),
        "accepted_accesses": len(events),
        "selection_counts": dict(sorted(Counter(
            event["representation_id"] for event in events
        ).items())),
        "routes": [dict(endpoint_id=route[0], source_node_id=route[1],
                        destination_execution_node_id=route[2],
                        destination_basis=route[3], access_count=count)
                   for route, count in sorted(route_counts.items())],
        "mean_total_cost_per_session": mean(item["total_cost"] for item in items),
        "mean_component_cost_per_session": {
            name: mean(item["costs"][name] for item in items)
            for name in COST_COMPONENT_IDS
        },
        "mean_payload_bytes_per_session": mean(item["payload_bytes"] for item in items),
        "mean_cross_node_payload_bytes_per_session": mean(item["network_bytes"] for item in items),
        "mean_artifact_bytes_per_session": mean(item["artifact_bytes"] for item in items),
        "latency_ms_per_access": {
            field: _stats([event.get(field) for event in events])
            for field in LATENCY_FIELDS
        },
    }


def _paired_effects(items: list[dict], prereg: Any) -> tuple[list[dict], list[dict]]:
    by_workload: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        by_workload[item["record"]["workload_id"]].append(item)
    pairs = []
    for workload_id, group in sorted(by_workload.items()):
        safe = [item for item in group if item["record"]["is_safe_design"]]
        candidate = [item for item in group if not item["record"]["is_safe_design"]]
        safe_scores = [item["record"]["task_success"] for item in safe]
        candidate_scores = [item["record"]["task_success"] for item in candidate]
        task_delta = None if None in safe_scores + candidate_scores else (
            mean(candidate_scores) - mean(safe_scores)
        )
        pair = {
            "workload_id": workload_id, "object_id": group[0]["record"]["object_id"],
            "stratum_id": group[0]["record"]["stratum_id"],
            "safe_design_id": prereg.safe_design_id,
            "candidate_design_id": candidate[0]["record"]["design_id"],
            "repetitions": len(safe), "task_success_delta": task_delta,
            "total_cost_delta": mean(i["total_cost"] for i in candidate)
                                - mean(i["total_cost"] for i in safe),
            "safe_mean_total_cost": mean(i["total_cost"] for i in safe),
            "candidate_mean_total_cost": mean(i["total_cost"] for i in candidate),
        }
        for name in COST_COMPONENT_IDS:
            pair[f"{name}_cost_delta"] = mean(i["costs"][name] for i in candidate) - mean(
                i["costs"][name] for i in safe
            )
        pairs.append(pair)
    aggregates = []
    for stratum in (None, *sorted({pair["stratum_id"] for pair in pairs})):
        group = [pair for pair in pairs if stratum is None or pair["stratum_id"] == stratum]
        scores = [pair["task_success_delta"] for pair in group
                  if pair["task_success_delta"] is not None]
        aggregates.append({
            "scope": "overall" if stratum is None else "stratum",
            "stratum_id": stratum, "independent_workloads": len(group),
            "evaluable_pairs": len(scores),
            "mean_task_success_delta": mean(scores) if scores else None,
            "mean_total_cost_delta": mean(p["total_cost_delta"] for p in group),
            "mean_component_cost_delta": {
                name: mean(p[f"{name}_cost_delta"] for p in group)
                for name in COST_COMPONENT_IDS
            },
        })
    return pairs, aggregates


def _csv(rows: list[dict]) -> str:
    if not rows:
        return ""
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _json(value) if isinstance(value, (dict, list)) else value
                         for key, value in row.items()})
    return output.getvalue()


def _markdown(report: dict) -> str:
    lines = ["# Pathfinder workload evaluation", "", f"Pilot: `{report['pilot_id']}`", "",
             "Offline descriptive reproduction; not a confidence certificate or live PPD proof.",
             f"Input origin: `{report['input_origin']}`.",
             f"Accounting unit: `{report['accounting_unit']}`.",
             f"Cost-rate provenance: {report['cost_rate_provenance']}", "",
             f"Canonical sessions: {report['canonical_records']}; "
             f"independent workloads: {report['independent_workloads']}; "
             f"attempts (including failures): {report['attempt_records']}.",
             "", "| Stratum | Design | Sessions | Task successes / evaluable | Mean total cost |",
             "|---|---|---:|---:|---:|"]
    for group in report["by_stratum_design"]:
        lines.append(f"| {group['stratum_id']} | {group['design_id']} | {group['sessions']} | "
                     f"{group['task_successes']} / {group['evaluable_tasks']} | "
                     f"{group['mean_total_cost_per_session']:.9g} |")
    aggregate = report["paired_aggregates"][0]
    lines.extend(["", "## Paired effects", "",
                  "Candidate minus safe; repetitions are averaged inside each workload first.",
                  f"Independent workloads: {aggregate['independent_workloads']}.",
                  f"Mean success difference: {aggregate['mean_task_success_delta']}.",
                  f"Mean total-cost difference: {aggregate['mean_total_cost_delta']:.9g}.",
                  "", "## Limits", ""])
    lines.extend(f"- {warning}" for warning in report["limitations"])
    return "\n".join(lines) + "\n"


def evaluate_distributed_pilot(
    run_dir: str | Path, *, preregistration: str | Path,
    endpoint_registry: str | Path, workload_manifest: str | Path,
    measurement_manifest: str | Path, output_dir: str | Path,
) -> dict[str, Any]:
    """Evaluate a complete snapshot, writing only a new report directory.

    Hashes establish reproducibility/consistency, not authenticity. Missing
    canonical cells, broken delivery and a journal crash window are refused,
    never silently repaired or dropped from a paired comparison.
    """
    source, target = Path(run_dir).resolve(), Path(output_dir).resolve()
    files = {"preregistration.json": Path(preregistration).resolve(),
             "endpoint-registry.json": Path(endpoint_registry).resolve(),
             "workloads.json": Path(workload_manifest).resolve(),
             "measurements.json": Path(measurement_manifest).resolve()}
    files.update({name: source / name for name in (
        PLAN_DOCUMENT, CANONICAL_LEDGER, ATTEMPT_LEDGER, CELL_JOURNAL, RUN_SUMMARY
    )})
    _require(not target.exists(), "evaluation output directory already exists")
    for protected in (source, *(path.parent for path in files.values())):
        _require(not target.is_relative_to(protected)
                 and not protected.is_relative_to(target),
                 "evaluation output must be separate from all input directories")
    # Validate strict JSON before handing paths to the existing loaders.
    snapshots = {name: path.read_bytes() for name, path in files.items()}
    payloads = {name: _read(path) for name, path in files.items()}
    prereg = load_distributed_pilot_preregistration(files["preregistration.json"])
    registry = load_endpoint_registry(files["endpoint-registry.json"])
    provider = load_measurement_manifest(files["measurements.json"])
    workloads = payloads["workloads.json"]
    _require(isinstance(workloads, dict), "workloads must map IDs to definitions")
    objects = []
    for key in prereg.workload_ids:
        workload = workloads.get(key)
        _require(isinstance(workload, dict), "a planned workload is missing")
        obj = workload.get("object_id")
        _require(isinstance(obj, str) and bool(obj), "workload object_id is missing")
        _require(isinstance(workload.get("question"), str), "workload question is missing")
        objects.append(obj)
    _require(len(set(objects)) == len(objects),
             "multiple workloads share an object; independent-unit count would be inflated")
    trials = build_distributed_trial_plan(prereg)
    expected_plan = build_frozen_plan_document(prereg, registry, workloads=workloads, trials=trials)
    _require(_json(payloads[PLAN_DOCUMENT]) == _json(expected_plan),
             "frozen plan disagrees with configuration, workload content or registry")
    provider.require_matching_run(
        pilot_id=prereg.pilot_id, preregistration_sha256=prereg.source_sha256,
        endpoint_registry_sha256=registry.source_sha256,
        execution_node_id=registry.execution_node_id,
    )
    trial_map = {trial.trial_key: trial for trial in trials}
    records = load_durable_canonical_records(files[CANONICAL_LEDGER], trial_map,
                                             pilot_id=prereg.pilot_id)
    _require(set(records) == set(trial_map),
             "canonical dataset is incomplete; no partial-block evaluation is produced")
    attempts = payloads[ATTEMPT_LEDGER]
    by_cell = _audit_attempts(attempts, trial_map, records)
    journal_counts = _audit_journal(payloads[CELL_JOURNAL], trial_map)
    _audit_summary(payloads[RUN_SUMMARY], prereg, expected_plan, provider, attempts)
    items = [_audit_record(records[trial.trial_key], trial, workloads[trial.workload_id],
                           registry, provider, prereg.cost_model,
                           by_cell[trial.trial_key][-1]["attempt"]) for trial in trials]
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for item in items:
        row = item["record"]
        groups[(row["stratum_id"], row["design_id"])].append(item)
    summaries = [dict(stratum_id=key[0], design_id=key[1], **_group_summary(group))
                 for key, group in sorted(groups.items())]
    pairs, aggregates = _paired_effects(items, prereg)
    classes = Counter(row["observation_class"] for row in attempts)
    missing_destinations = sum(item["missing_destination"] for item in items)
    limitations = [
        "Development/pilot evidence only; this evaluator does not establish generalization, "
        "statistical significance, or SAFE_TO_COMMIT.",
        "Task scores reproduce the frozen accepted-substring rule, not a new exact-match grader.",
        "Service cost is Data-Agent-reported accounting, not independently verified physical cost. "
        "Ledger provenance and rates are preserved; configured values are not made measured by evaluation.",
        "Felt latency is per access, not full Agent runtime. Controlled delays remain included; "
        "a missing timing is unavailable, never zero.",
        "Payload bytes exclude HTTP/TLS overhead; local payload is not cross-node traffic. "
        "Inline bytes and artifact bytes are both counted, without adding artifact bytes_read twice.",
        "Paired aggregates weight workload objects equally, not repetitions; the overall candidate "
        "is the frozen stratum-restricted policy, not a single global design.",
        "Input hashes detect changes, not forged records. Offline checks cannot verify a live physical path.",
        "Reports do not copy answer, handle or raw runtime-trace fields. Operator-chosen IDs "
        "and provenance text still need privacy and licensing review before publication; "
        "this is not a general secret-redaction tool.",
    ]
    if missing_destinations:
        limitations.append(f"{missing_destinations} access destinations were absent and inferred "
                           "from the frozen registry; they are labeled as inferred in route summaries.")
    report = {
        "schema_version": EVALUATION_SCHEMA, "status": "COMPLETE",
        "pilot_id": prereg.pilot_id, "confirmatory": False,
        "collection_source_git_revision": prereg.source_git_revision,
        "eligible_for_scientific_claims": False, "offline": True,
        "input_origin": "synthetic-format-example" if
        payloads["preregistration.json"].get("synthetic") is True else
        "operator-supplied-snapshot-not-independently-verified",
        "planned_trials": len(trials), "canonical_records": len(records),
        "independent_workloads": len(objects), "repetitions": prereg.repetitions,
        "attempt_records": len(attempts), "attempt_classes": dict(sorted(classes.items())),
        "failure_classes": dict(sorted(Counter(row["failure_class"] for row in attempts
                                                if row.get("failure_class")).items())),
        "latest_journal_states": dict(sorted(journal_counts.items())),
        "accounting_unit": prereg.cost_model.accounting_unit,
        "declared_cost_basis": prereg.cost_basis,
        "success_scoring_rule": prereg.success_scoring_rule,
        "cost_rate_provenance": prereg.cost_model.rate_provenance,
        "cost_model_sha256": prereg.cost_model.source_sha256,
        "measurement_manifest_sha256": provider.manifest_sha256,
        "plan_sha256": expected_plan["plan_sha256"],
        "cost_component_value_kinds": {
            name: dict(sorted(Counter(i["record"]["cost_ledger"]["components"][name]["value_kind"]
                                      for i in items).items())) for name in COST_COMPONENT_IDS
        },
        "declared_measurement_kinds": {
            name: dict(sorted(Counter(entry.by_component()[name].kind
                                      for entry in provider.entries.values()).items()))
            for name in ("storage", "amortized_materialization", "transition")
        },
        "by_stratum_design": summaries,
        "by_role": {role: _group_summary([i for i in items
                                         if i["record"]["is_safe_design"] is safe])
                    for role, safe in (("safe", True), ("restricted_candidate", False))},
        "paired_workload_effects": pairs, "paired_aggregates": aggregates,
        "limitations": limitations,
    }
    code_root = Path(__file__).resolve().parents[1]
    code_hashes = {path.relative_to(code_root).as_posix(): sha256(path.read_bytes()).hexdigest()
                   for path in sorted(code_root.rglob("*.py"))}
    manifest = {
        "schema_version": EVALUATION_SCHEMA, "input_files_sha256": {
            name: sha256(value).hexdigest() for name, value in sorted(snapshots.items())
        },
        "evaluator_source_sha256": sha256(_json(code_hashes).encode()).hexdigest(),
        "python_version": platform.python_version(),
        "offline": True, "inputs_unchanged": True, "outputs_contain_raw_records": False,
    }
    documents = {
        "evaluation.json": json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        "evaluation_manifest.json": json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        "summary_by_design.csv": _csv(summaries),
        "paired_effects.csv": _csv(pairs), "report.md": _markdown(report),
    }
    documents["SHA256SUMS"] = "".join(
        f"{sha256(value.encode('utf-8')).hexdigest()}  {name}\n"
        for name, value in sorted(documents.items())
    )
    _require(all(path.read_bytes() == snapshots[name] for name, path in files.items()),
             "an input changed during evaluation; no report was written")
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".pathfinder-eval-", dir=target.parent) as temporary:
        staging = Path(temporary) / "report"
        staging.mkdir()
        for name, value in documents.items():
            (staging / name).write_text(value, encoding="utf-8", newline="\n")
        _require(not target.exists(), "evaluation output directory already exists")
        staging.rename(target)
    return report
