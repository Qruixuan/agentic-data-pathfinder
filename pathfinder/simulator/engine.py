"""Deterministic discrete-event engine for physical-layout experiments."""

from __future__ import annotations

import heapq
import json
import uuid
from collections import Counter, OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, Iterable

from .config import (
    Cache,
    FlowMeshWorkload,
    Link,
    Operation,
    PhysicalDesign,
    Resource,
    SimulatorConfigError,
    SimulatorScenario,
)
from .admission import trial_admission_contract


SIMULATOR_EVENT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-simulator-event/v1alpha1"
)
SIMULATOR_RECORD_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-simulator-record/v1alpha2"
)
SIMULATOR_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-simulator-plan/v1alpha2"
)


@dataclass(frozen=True)
class SimulatorTrial:
    trial_key: str
    trial_id: str
    workflow_id: str
    task_id: str
    session_id: str
    order_index: int
    workload_id: str
    workload_class: str
    object_id: str
    task_type: str
    design_id: str
    executor_node_id: str
    repetition: int
    seed: int
    arrival_time_ms: float

    def to_public_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SimulationEvent:
    schema_version: str
    event_id: str
    event_index: int
    trial_key: str
    workflow_id: str
    task_id: str
    operation_id: str
    operation_kind: str
    executed: bool
    skip_reason: str | None
    resource_id: str | None
    resource_kind: str | None
    source_node_id: str | None
    destination_node_id: str | None
    ready_time_ms: float
    start_time_ms: float
    end_time_ms: float
    queue_time_ms: float
    service_time_ms: float
    logical_bytes: int
    physical_bytes: int
    cache_result: str | None
    cache_evictions: tuple[str, ...]
    cost_components: dict[str, float]

    def to_public_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["cache_evictions"] = list(self.cache_evictions)
        return payload


@dataclass(frozen=True)
class SimulationResult:
    plan: dict[str, Any]
    events: tuple[SimulationEvent, ...]
    canonical_records: tuple[dict[str, Any], ...]
    summary: dict[str, Any]


class _ResourceQueue:
    def __init__(self, slots: int) -> None:
        self.available = [0.0] * slots

    def reserve(self, ready_time_ms: float, duration_ms: float) -> tuple[float, float]:
        slot = min(range(len(self.available)), key=lambda i: (self.available[i], i))
        start = max(ready_time_ms, self.available[slot])
        end = start + duration_ms
        self.available[slot] = end
        return start, end


class _CacheState:
    def __init__(self, cache: Cache) -> None:
        self.capacity_bytes = cache.capacity_bytes
        self.entries: OrderedDict[tuple[str, str], int] = OrderedDict(
            (entry.key, entry.size_bytes) for entry in cache.initial_entries
        )
        self.used_bytes = sum(self.entries.values())

    def lookup(self, key: tuple[str, str]) -> bool:
        size = self.entries.get(key)
        if size is None:
            return False
        self.entries.move_to_end(key)
        return True

    def insert(self, key: tuple[str, str], size_bytes: int) -> tuple[str, ...]:
        if size_bytes > self.capacity_bytes:
            return ()
        previous = self.entries.pop(key, None)
        if previous is not None:
            self.used_bytes -= previous
        evicted: list[str] = []
        while self.entries and self.used_bytes + size_bytes > self.capacity_bytes:
            evicted_key, evicted_size = self.entries.popitem(last=False)
            self.used_bytes -= evicted_size
            evicted.append(f"{evicted_key[0]}:{evicted_key[1]}")
        self.entries[key] = size_bytes
        self.used_bytes += size_bytes
        return tuple(evicted)


@dataclass
class _TrialRuntime:
    trial: SimulatorTrial
    workload: FlowMeshWorkload
    design: PhysicalDesign
    operations: dict[str, Operation]
    operation_order: dict[str, int]
    dependents: dict[str, tuple[str, ...]]
    completed_at: dict[str, float]
    admission_time_ms: float | None
    cache_decisions: dict[str, str]
    events: list[SimulationEvent]


def _stable_uuid(namespace: str, value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{namespace}:{value}"))


def build_simulator_trials(
    scenario: SimulatorScenario,
) -> tuple[SimulatorTrial, ...]:
    """Build a deterministic complete workload x design x repetition plan."""

    trials: list[SimulatorTrial] = []
    order = 0
    for workload in scenario.workloads:
        for design in scenario.designs:
            for repetition in range(scenario.repetitions):
                key = (
                    f"{scenario.scenario_id}|{workload.workload_id}|"
                    f"{design.design_id}|r{repetition:04d}"
                )
                trials.append(SimulatorTrial(
                    trial_key=key,
                    trial_id=_stable_uuid("sim-trial", key),
                    workflow_id=_stable_uuid("sim-workflow", key),
                    task_id=_stable_uuid("sim-task", key),
                    session_id=_stable_uuid("sim-session", key),
                    order_index=order,
                    workload_id=workload.workload_id,
                    workload_class=workload.workload_class,
                    object_id=workload.object_id,
                    task_type=workload.task_type,
                    design_id=design.design_id,
                    executor_node_id=design.executor_node_id,
                    repetition=repetition,
                    seed=scenario.seed + order,
                    arrival_time_ms=order * scenario.arrival_interval_ms,
                ))
                order += 1
    expected = scenario.planned_trial_count
    if len(trials) != expected:
        raise RuntimeError(
            f"simulator planned {len(trials)} trials, expected {expected}"
        )
    if len({trial.trial_key for trial in trials}) != len(trials):
        raise RuntimeError("simulator trial plan contains duplicate keys")
    return tuple(trials)


def _plan_document(
    scenario: SimulatorScenario,
    trials: Iterable[SimulatorTrial],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SIMULATOR_PLAN_SCHEMA_VERSION,
        "scenario_id": scenario.scenario_id,
        "scenario_sha256": scenario.source_sha256,
        "rate_card_id": scenario.rate_card.rate_card_id,
        "rate_card_provenance": scenario.rate_card.provenance,
        "calibration_provenance": scenario.calibration_provenance,
        "seed": scenario.seed,
        "repetitions": scenario.repetitions,
        "arrival_interval_ms": scenario.arrival_interval_ms,
        "trial_admission": trial_admission_contract(
            scenario.trial_admission_slots
        ),
        "node_count": len(scenario.nodes),
        "link_count": len(scenario.links),
        "workload_count": len(scenario.workloads),
        "design_count": len(scenario.designs),
        "planned_trial_count": scenario.planned_trial_count,
        "flowmesh_deployed": False,
        "trials": [trial.to_public_dict() for trial in trials],
    }
    payload["plan_sha256"] = sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()
    return payload


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _jitter_multiplier(
    fraction: float,
    *,
    scenario_seed: int,
    trial_key: str,
    operation_id: str,
) -> float:
    if fraction == 0.0:
        return 1.0
    digest = sha256(
        f"{scenario_seed}|{trial_key}|{operation_id}".encode("utf-8")
    ).digest()
    unit_interval = int.from_bytes(
        digest[:8], "big", signed=False
    ) / ((1 << 64) - 1)
    return 1.0 - fraction + (2.0 * fraction * unit_interval)


def _operation_bytes(
    operation: Operation,
    runtime: _TrialRuntime,
    scenario: SimulatorScenario,
) -> int:
    if operation.kind == "cache_lookup":
        return 0
    if operation.size_bytes is not None:
        base = operation.size_bytes
    elif operation.representation_id is not None:
        obj = scenario.objects[runtime.workload.object_id]
        try:
            base = obj.representations[operation.representation_id].size_bytes
        except KeyError as exc:
            raise SimulatorConfigError(
                f"trial {runtime.trial.trial_key} requires unavailable "
                f"representation {operation.representation_id}"
            ) from exc
    else:
        return 0
    return int(round(base * operation.byte_multiplier))


def _operation_resource(
    operation: Operation,
    scenario: SimulatorScenario,
) -> Resource | Link | None:
    if operation.resource_id is not None:
        return scenario.resources[operation.resource_id]
    if operation.link_id is not None:
        return scenario.links[operation.link_id]
    return None


def _duration_ms(
    operation: Operation,
    resource: Resource | Link | None,
    size_bytes: int,
    *,
    scenario: SimulatorScenario,
    trial_key: str,
) -> float:
    if isinstance(resource, Link):
        duration = (
            resource.round_trip_time_ms
            + size_bytes / resource.bandwidth_bytes_per_second * 1000.0
            + operation.service_ms
        )
        jitter = resource.jitter_fraction
    elif isinstance(resource, Resource):
        duration = resource.base_latency_ms + operation.service_ms
        if size_bytes and resource.throughput_bytes_per_second is not None:
            duration += (
                size_bytes / resource.throughput_bytes_per_second * 1000.0
            )
        jitter = resource.jitter_fraction
    else:
        duration = operation.service_ms
        jitter = 0.0
    return duration * _jitter_multiplier(
        jitter,
        scenario_seed=scenario.seed,
        trial_key=trial_key,
        operation_id=operation.op_id,
    )


def _resource_identity(
    resource: Resource | Link | None,
) -> tuple[str | None, str | None, str | None, str | None]:
    if isinstance(resource, Link):
        return (
            resource.link_id,
            "network",
            resource.source_node_id,
            resource.destination_node_id,
        )
    if isinstance(resource, Resource):
        return (resource.resource_id, resource.kind, resource.node_id, resource.node_id)
    return (None, None, None, None)


def _cost_components(
    resource: Resource | Link | None,
    *,
    service_time_ms: float,
    physical_bytes: int,
    scenario: SimulatorScenario,
) -> dict[str, float]:
    if resource is None:
        return {}
    values: dict[str, float] = {}
    if resource.time_rate_id is not None:
        values[resource.time_rate_id] = (
            service_time_ms * scenario.rate_card.rates[resource.time_rate_id]
        )
    if resource.byte_rate_id is not None:
        values[resource.byte_rate_id] = (
            physical_bytes * scenario.rate_card.rates[resource.byte_rate_id]
        )
    return values


def _condition_met(operation: Operation, runtime: _TrialRuntime) -> bool:
    if operation.condition is None:
        return True
    actual = runtime.cache_decisions.get(operation.condition.cache_op_id)
    if actual is None:
        raise RuntimeError(
            f"cache condition for {operation.op_id} was evaluated before "
            f"{operation.condition.cache_op_id}"
        )
    return actual == operation.condition.equals


def _cache_scope(trial: SimulatorTrial, cache_id: str) -> tuple[str, int, str]:
    return (trial.design_id, trial.repetition, cache_id)


def _event_id(trial_key: str, operation_id: str) -> str:
    return _stable_uuid("sim-event", f"{trial_key}|{operation_id}")


def _new_runtime(
    scenario: SimulatorScenario,
    trial: SimulatorTrial,
    workload: FlowMeshWorkload,
    design: PhysicalDesign,
) -> _TrialRuntime:
    ordered = scenario.operations_for(design, workload.workload_class)
    operations = {operation.op_id: operation for operation in ordered}
    dependents: dict[str, list[str]] = defaultdict(list)
    for operation in ordered:
        for dependency in operation.depends_on:
            dependents[dependency].append(operation.op_id)
    return _TrialRuntime(
        trial=trial,
        workload=workload,
        design=design,
        operations=operations,
        operation_order={op.op_id: i for i, op in enumerate(ordered)},
        dependents={key: tuple(value) for key, value in dependents.items()},
        completed_at={},
        admission_time_ms=None,
        cache_decisions={},
        events=[],
    )


def run_discrete_event_simulation(
    scenario: SimulatorScenario,
) -> SimulationResult:
    """Execute a complete scenario in deterministic virtual time."""

    trials = build_simulator_trials(scenario)
    workloads = {value.workload_id: value for value in scenario.workloads}
    designs = {value.design_id: value for value in scenario.designs}
    runtimes = {
        trial.trial_key: _new_runtime(
            scenario,
            trial,
            workloads[trial.workload_id],
            designs[trial.design_id],
        )
        for trial in trials
    }
    queues: dict[str, _ResourceQueue] = {
        resource.resource_id: _ResourceQueue(resource.slots)
        for resource in scenario.resources.values()
    }
    queues.update({
        link.link_id: _ResourceQueue(link.slots)
        for link in scenario.links.values()
    })
    cache_states: dict[tuple[str, int, str], _CacheState] = {}
    for trial in trials:
        for cache in scenario.caches.values():
            key = _cache_scope(trial, cache.cache_id)
            cache_states.setdefault(key, _CacheState(cache))

    # action tuple: time, priority (complete, arrival, then ready), trial order,
    # operation order, serial, action, trial key, operation id, payload.
    actions: list[tuple[float, int, int, int, int, str, str, str, Any]] = []
    serial = 0

    def push_ready(runtime: _TrialRuntime, op_id: str, ready: float) -> None:
        nonlocal serial
        heapq.heappush(actions, (
            ready,
            2,
            runtime.trial.order_index,
            runtime.operation_order[op_id],
            serial,
            "ready",
            runtime.trial.trial_key,
            op_id,
            None,
        ))
        serial += 1

    waiting_trials: deque[str] = deque()
    active_trials = 0

    def admit_trial(runtime: _TrialRuntime, admitted_at_ms: float) -> None:
        nonlocal active_trials
        if runtime.admission_time_ms is not None:
            raise RuntimeError(
                f"trial admitted more than once: {runtime.trial.trial_key}"
            )
        runtime.admission_time_ms = admitted_at_ms
        active_trials += 1
        for op_id, operation in runtime.operations.items():
            if not operation.depends_on:
                push_ready(runtime, op_id, admitted_at_ms)

    for runtime in runtimes.values():
        heapq.heappush(actions, (
            runtime.trial.arrival_time_ms,
            1,
            runtime.trial.order_index,
            -1,
            serial,
            "trial_arrival",
            runtime.trial.trial_key,
            "",
            None,
        ))
        serial += 1

    completed_trials = 0
    for runtime in runtimes.values():
        if not any(
            not operation.depends_on
            for operation in runtime.operations.values()
        ):
            raise RuntimeError(
                f"trial has no root operation: {runtime.trial.trial_key}"
            )

    completed_operations = 0
    while actions:
        (
            time_ms,
            _,
            _,
            _,
            _,
            action,
            trial_key,
            op_id,
            payload,
        ) = heapq.heappop(actions)
        runtime = runtimes[trial_key]

        if action == "trial_arrival":
            if active_trials < scenario.trial_admission_slots:
                admit_trial(runtime, time_ms)
            else:
                waiting_trials.append(trial_key)
            continue

        operation = runtime.operations[op_id]

        if action == "complete":
            event: SimulationEvent = payload
            if operation.kind == "cache_insert" and event.executed:
                cache = scenario.caches[operation.cache_id or ""]
                state = cache_states[
                    _cache_scope(runtime.trial, cache.cache_id)
                ]
                representation = operation.representation_id or "literal"
                evictions = state.insert(
                    (runtime.workload.object_id, representation),
                    event.physical_bytes,
                )
                if evictions:
                    event = SimulationEvent(
                        **{
                            **event.to_public_dict(),
                            "cache_evictions": evictions,
                        }
                    )
            runtime.events.append(event)
            runtime.completed_at[op_id] = event.end_time_ms
            completed_operations += 1
            for dependent_id in runtime.dependents.get(op_id, ()):
                dependent = runtime.operations[dependent_id]
                if all(dep in runtime.completed_at for dep in dependent.depends_on):
                    ready = max(
                        runtime.completed_at[dep]
                        for dep in dependent.depends_on
                    )
                    push_ready(runtime, dependent_id, ready)
            if len(runtime.completed_at) == len(runtime.operations):
                active_trials -= 1
                completed_trials += 1
                if waiting_trials:
                    waiting_key = waiting_trials.popleft()
                    admit_trial(runtimes[waiting_key], time_ms)
            continue

        if not _condition_met(operation, runtime):
            condition = operation.condition
            assert condition is not None
            event = SimulationEvent(
                schema_version=SIMULATOR_EVENT_SCHEMA_VERSION,
                event_id=_event_id(trial_key, op_id),
                event_index=-1,
                trial_key=trial_key,
                workflow_id=runtime.trial.workflow_id,
                task_id=runtime.trial.task_id,
                operation_id=op_id,
                operation_kind=operation.kind,
                executed=False,
                skip_reason=(
                    f"condition {condition.cache_op_id}="
                    f"{condition.equals} did not match"
                ),
                resource_id=None,
                resource_kind=None,
                source_node_id=None,
                destination_node_id=None,
                ready_time_ms=time_ms,
                start_time_ms=time_ms,
                end_time_ms=time_ms,
                queue_time_ms=0.0,
                service_time_ms=0.0,
                logical_bytes=0,
                physical_bytes=0,
                cache_result=None,
                cache_evictions=(),
                cost_components={},
            )
            heapq.heappush(actions, (
                time_ms,
                0,
                runtime.trial.order_index,
                runtime.operation_order[op_id],
                serial,
                "complete",
                trial_key,
                op_id,
                event,
            ))
            serial += 1
            continue

        size_bytes = _operation_bytes(operation, runtime, scenario)
        resource = _operation_resource(operation, scenario)
        duration = _duration_ms(
            operation,
            resource,
            size_bytes,
            scenario=scenario,
            trial_key=trial_key,
        )
        if resource is None:
            start, end = time_ms, time_ms + duration
        else:
            resource_id = (
                resource.link_id if isinstance(resource, Link) else resource.resource_id
            )
            start, end = queues[resource_id].reserve(time_ms, duration)

        cache_result = None
        if operation.kind == "cache_lookup":
            cache = scenario.caches[operation.cache_id or ""]
            state = cache_states[_cache_scope(runtime.trial, cache.cache_id)]
            key = (
                runtime.workload.object_id,
                operation.representation_id or "literal",
            )
            cache_result = "hit" if state.lookup(key) else "miss"
            runtime.cache_decisions[op_id] = cache_result

        resource_id, resource_kind, source_node, destination_node = (
            _resource_identity(resource)
        )
        event = SimulationEvent(
            schema_version=SIMULATOR_EVENT_SCHEMA_VERSION,
            event_id=_event_id(trial_key, op_id),
            event_index=-1,
            trial_key=trial_key,
            workflow_id=runtime.trial.workflow_id,
            task_id=runtime.trial.task_id,
            operation_id=op_id,
            operation_kind=operation.kind,
            executed=True,
            skip_reason=None,
            resource_id=resource_id,
            resource_kind=resource_kind,
            source_node_id=source_node,
            destination_node_id=destination_node,
            ready_time_ms=time_ms,
            start_time_ms=start,
            end_time_ms=end,
            queue_time_ms=start - time_ms,
            service_time_ms=end - start,
            logical_bytes=size_bytes,
            physical_bytes=size_bytes,
            cache_result=cache_result,
            cache_evictions=(),
            cost_components=_cost_components(
                resource,
                service_time_ms=end - start,
                physical_bytes=size_bytes,
                scenario=scenario,
            ),
        )
        heapq.heappush(actions, (
            end,
            0,
            runtime.trial.order_index,
            runtime.operation_order[op_id],
            serial,
            "complete",
            trial_key,
            op_id,
            event,
        ))
        serial += 1

    expected_operations = sum(len(runtime.operations) for runtime in runtimes.values())
    if completed_operations != expected_operations:
        raise RuntimeError(
            f"simulator completed {completed_operations} operations, "
            f"expected {expected_operations}"
        )
    if completed_trials != len(trials) or active_trials != 0 or waiting_trials:
        raise RuntimeError("simulator trial admission did not drain cleanly")

    ordered_events = sorted(
        (event for runtime in runtimes.values() for event in runtime.events),
        key=lambda event: (
            event.start_time_ms,
            runtimes[event.trial_key].trial.order_index,
            runtimes[event.trial_key].operation_order[event.operation_id],
        ),
    )
    indexed_events = tuple(
        SimulationEvent(**{
            **event.to_public_dict(),
            "event_index": index,
            "cache_evictions": tuple(event.cache_evictions),
        })
        for index, event in enumerate(ordered_events)
    )
    events_by_trial: dict[str, list[SimulationEvent]] = defaultdict(list)
    for event in indexed_events:
        events_by_trial[event.trial_key].append(event)

    canonical_records = tuple(
        _canonical_record(
            scenario,
            runtimes[trial.trial_key],
            events_by_trial[trial.trial_key],
        )
        for trial in trials
    )
    plan = _plan_document(scenario, trials)
    summary = _summary(scenario, canonical_records, indexed_events, plan)
    return SimulationResult(
        plan=plan,
        events=indexed_events,
        canonical_records=canonical_records,
        summary=summary,
    )


def _canonical_record(
    scenario: SimulatorScenario,
    runtime: _TrialRuntime,
    events: list[SimulationEvent],
) -> dict[str, Any]:
    executed = [event for event in events if event.executed]
    if not executed:
        raise RuntimeError(f"trial {runtime.trial.trial_key} executed no events")
    components: dict[str, float] = defaultdict(float)
    for event in executed:
        for key, value in event.cost_components.items():
            components[key] += value
    resource_service_ms: dict[str, float] = defaultdict(float)
    resource_queue_ms: dict[str, float] = defaultdict(float)
    for event in executed:
        kind = event.resource_kind or "unmetered"
        resource_service_ms[kind] += event.service_time_ms
        resource_queue_ms[kind] += event.queue_time_ms
    network_bytes = sum(
        event.physical_bytes
        for event in executed
        if event.resource_kind == "network"
    )
    total_cost = sum(components.values())
    end = max(event.end_time_ms for event in executed)
    trial = runtime.trial
    admission_time_ms = runtime.admission_time_ms
    if admission_time_ms is None:
        raise RuntimeError(f"trial was never admitted: {trial.trial_key}")
    trial_admission_queue_ms = admission_time_ms - trial.arrival_time_ms
    active_execution_latency_ms = end - admission_time_ms
    return {
        "schema_version": SIMULATOR_RECORD_SCHEMA_VERSION,
        "evidence_class": "deterministic-discrete-event-simulation",
        "scenario_id": scenario.scenario_id,
        "scenario_sha256": scenario.source_sha256,
        **trial.to_public_dict(),
        "assigned_worker_node_id": trial.executor_node_id,
        "flowmesh_task_type": trial.task_type,
        "outcome_type": "completed",
        "telemetry_complete": True,
        "artifact_delivery_complete": True,
        "task_success": runtime.workload.task_success_by_design[trial.design_id],
        "quality_provenance": runtime.workload.quality_provenance,
        "latency_ms": trial_admission_queue_ms + active_execution_latency_ms,
        "trial_admission_queue_ms": trial_admission_queue_ms,
        "active_execution_latency_ms": active_execution_latency_ms,
        "latency_origin": "planned-trial-arrival",
        "trial_admission_algorithm": (
            "fifo-by-arrival-time-then-order-index"
        ),
        "trial_admission_slots": scenario.trial_admission_slots,
        "event_count": len(events),
        "executed_event_count": len(executed),
        "logical_bytes": sum(event.logical_bytes for event in executed),
        "physical_bytes": sum(event.physical_bytes for event in executed),
        "network_bytes": network_bytes,
        "cache_hits": sum(event.cache_result == "hit" for event in executed),
        "cache_misses": sum(event.cache_result == "miss" for event in executed),
        "resource_service_ms": dict(sorted(resource_service_ms.items())),
        "resource_queue_ms": dict(sorted(resource_queue_ms.items())),
        "cost_components": dict(sorted(components.items())),
        "total_cost": total_cost,
        "cost_unit": "scenario-rate-card-unit",
        "rate_card_id": scenario.rate_card.rate_card_id,
        "rate_card_provenance": scenario.rate_card.provenance,
        "calibration_provenance": scenario.calibration_provenance,
        "simulated": True,
        "flowmesh_deployed": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _summary(
    scenario: SimulatorScenario,
    records: tuple[dict[str, Any], ...],
    events: tuple[SimulationEvent, ...],
    plan: dict[str, Any],
) -> dict[str, Any]:
    by_design: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_design[record["design_id"]].append(record)
    design_summaries = []
    for design in scenario.designs:
        rows = by_design[design.design_id]
        design_summaries.append({
            "design_id": design.design_id,
            "trials": len(rows),
            "mean_latency_ms": sum(row["latency_ms"] for row in rows) / len(rows),
            "mean_trial_admission_queue_ms": sum(
                row["trial_admission_queue_ms"] for row in rows
            ) / len(rows),
            "mean_active_execution_latency_ms": sum(
                row["active_execution_latency_ms"] for row in rows
            ) / len(rows),
            "mean_total_cost": sum(row["total_cost"] for row in rows) / len(rows),
            "task_success_rate": sum(row["task_success"] for row in rows) / len(rows),
            "network_bytes": sum(row["network_bytes"] for row in rows),
            "cache_hits": sum(row["cache_hits"] for row in rows),
            "cache_misses": sum(row["cache_misses"] for row in rows),
        })
    return {
        "schema_version": "pathfinder.flowmesh-infra-simulator-summary/v1alpha2",
        "status": "COMPLETE",
        "scenario_id": scenario.scenario_id,
        "scenario_sha256": scenario.source_sha256,
        "calibration_provenance": scenario.calibration_provenance,
        "plan_sha256": plan["plan_sha256"],
        "planned_trials": scenario.planned_trial_count,
        "canonical_records": len(records),
        "events": len(events),
        "executed_events": sum(event.executed for event in events),
        "skipped_branch_events": sum(not event.executed for event in events),
        "node_count": len(scenario.nodes),
        "link_count": len(scenario.links),
        "workload_count": len(scenario.workloads),
        "design_count": len(scenario.designs),
        "repetitions": scenario.repetitions,
        "trial_admission": trial_admission_contract(
            scenario.trial_admission_slots
        ),
        "total_trial_admission_queue_ms": sum(
            row["trial_admission_queue_ms"] for row in records
        ),
        "rate_card_id": scenario.rate_card.rate_card_id,
        "rate_card_provenance": scenario.rate_card.provenance,
        "operation_counts": dict(sorted(Counter(
            event.operation_kind for event in events if event.executed
        ).items())),
        "design_summaries": design_summaries,
        "flowmesh_deployed": False,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
