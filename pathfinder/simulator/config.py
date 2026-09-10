"""Strict configuration contract for the FlowMesh infrastructure simulator.

The simulator core is deliberately independent of Pathfinder's policy and of
the live FlowMesh SDK.  A scenario describes logical infrastructure, frozen
workloads, physical designs, and generic operation DAGs.  The same engine can
therefore replay a FlowMesh-shaped task without starting a Root Server,
worker, Data Agent, or model service.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping


SIMULATOR_SCENARIO_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-simulator-scenario/v1alpha1"
)
OPERATION_KINDS = (
    "barrier",
    "cache_insert",
    "cache_lookup",
    "cache_read",
    "compute",
    "control",
    "index_query",
    "network_transfer",
    "storage_read",
)
RESOURCE_KINDS = (
    "cache",
    "control",
    "cpu",
    "gpu",
    "index",
    "storage",
)


class SimulatorConfigError(ValueError):
    """Raised when a simulator scenario is incomplete or ambiguous."""


def _fail(condition: bool, message: str) -> None:
    if not condition:
        raise SimulatorConfigError(message)


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _fail(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _list(value: Any, name: str) -> list[Any]:
    _fail(isinstance(value, list), f"{name} must be an array")
    return value


def _string(value: Any, name: str) -> str:
    _fail(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _number(value: Any, name: str, *, positive: bool = False) -> float:
    _fail(
        not isinstance(value, bool) and isinstance(value, (int, float)),
        f"{name} must be a number",
    )
    result = float(value)
    _fail(math.isfinite(result), f"{name} must be finite")
    if positive:
        _fail(result > 0.0, f"{name} must be positive")
    else:
        _fail(result >= 0.0, f"{name} must be non-negative")
    return result


def _fraction(value: Any, name: str) -> float:
    result = _number(value, name)
    _fail(result < 1.0, f"{name} must be less than 1")
    return result


def _integer(value: Any, name: str, *, positive: bool = False) -> int:
    _fail(
        not isinstance(value, bool) and isinstance(value, int),
        f"{name} must be an integer",
    )
    if positive:
        _fail(value > 0, f"{name} must be positive")
    else:
        _fail(value >= 0, f"{name} must be non-negative")
    return value


def _required(root: Mapping[str, Any], key: str, name: str) -> Any:
    _fail(key in root, f"{name}.{key} is required")
    return root[key]


def _unique(items: list[Any], attr: str, name: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in items:
        key = getattr(item, attr)
        _fail(key not in result, f"duplicate {name}: {key}")
        result[key] = item
    return result


def _no_duplicate_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SimulatorConfigError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


@dataclass(frozen=True)
class RateCard:
    rate_card_id: str
    provenance: str
    rates: dict[str, float]


@dataclass(frozen=True)
class Resource:
    resource_id: str
    node_id: str
    kind: str
    slots: int
    base_latency_ms: float
    throughput_bytes_per_second: float | None
    jitter_fraction: float
    time_rate_id: str | None
    byte_rate_id: str | None


@dataclass(frozen=True)
class CacheEntry:
    object_id: str
    representation_id: str
    size_bytes: int

    @property
    def key(self) -> tuple[str, str]:
        return (self.object_id, self.representation_id)


@dataclass(frozen=True)
class Cache:
    cache_id: str
    node_id: str
    capacity_bytes: int
    initial_entries: tuple[CacheEntry, ...]


@dataclass(frozen=True)
class Node:
    node_id: str
    roles: tuple[str, ...]
    resources: tuple[Resource, ...]
    caches: tuple[Cache, ...]


@dataclass(frozen=True)
class Link:
    link_id: str
    source_node_id: str
    destination_node_id: str
    slots: int
    bandwidth_bytes_per_second: float
    round_trip_time_ms: float
    jitter_fraction: float
    time_rate_id: str | None
    byte_rate_id: str | None


@dataclass(frozen=True)
class Representation:
    representation_id: str
    size_bytes: int


@dataclass(frozen=True)
class DataObject:
    object_id: str
    representations: dict[str, Representation]


@dataclass(frozen=True)
class FlowMeshWorkload:
    workload_id: str
    workload_class: str
    object_id: str
    task_type: str
    quality_provenance: str
    task_success_by_design: dict[str, bool]


@dataclass(frozen=True)
class Condition:
    cache_op_id: str
    equals: str


@dataclass(frozen=True)
class Operation:
    op_id: str
    kind: str
    depends_on: tuple[str, ...]
    resource_id: str | None
    link_id: str | None
    cache_id: str | None
    representation_id: str | None
    size_bytes: int | None
    byte_multiplier: float
    service_ms: float
    condition: Condition | None


@dataclass(frozen=True)
class OperationTemplate:
    template_id: str
    raw_operations: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class PhysicalDesign:
    design_id: str
    executor_node_id: str
    route_templates: dict[str, str]
    bindings: dict[str, str]


@dataclass(frozen=True)
class SimulatorScenario:
    source_path: Path
    source_sha256: str
    scenario_id: str
    seed: int
    repetitions: int
    arrival_interval_ms: float
    trial_admission_slots: int
    calibration_provenance: str
    rate_card: RateCard
    nodes: dict[str, Node]
    resources: dict[str, Resource]
    caches: dict[str, Cache]
    links: dict[str, Link]
    objects: dict[str, DataObject]
    workloads: tuple[FlowMeshWorkload, ...]
    templates: dict[str, OperationTemplate]
    designs: tuple[PhysicalDesign, ...]

    @property
    def planned_trial_count(self) -> int:
        return len(self.workloads) * len(self.designs) * self.repetitions

    def operations_for(
        self,
        design: PhysicalDesign,
        workload_class: str,
    ) -> tuple[Operation, ...]:
        template_id = design.route_templates[workload_class]
        template = self.templates[template_id]
        resolved = tuple(
            _resolve_operation(raw, design.bindings, template_id)
            for raw in template.raw_operations
        )
        _validate_operation_dag(
            resolved,
            scenario=self,
            label=f"designs.{design.design_id}.{workload_class}",
        )
        return resolved


def _optional_rate_id(
    value: Any,
    name: str,
    rates: Mapping[str, float],
) -> str | None:
    if value is None:
        return None
    rate_id = _string(value, name)
    _fail(rate_id in rates, f"{name} references unknown rate {rate_id}")
    return rate_id


def _load_rate_card(payload: Any) -> RateCard:
    root = _mapping(payload, "rate_card")
    values = _mapping(_required(root, "rates", "rate_card"), "rate_card.rates")
    rates = {
        _string(key, "rate_card rate id"): _number(
            value,
            f"rate_card.rates.{key}",
        )
        for key, value in values.items()
    }
    _fail(bool(rates), "rate_card.rates must not be empty")
    return RateCard(
        rate_card_id=_string(
            _required(root, "rate_card_id", "rate_card"),
            "rate_card.rate_card_id",
        ),
        provenance=_string(
            _required(root, "provenance", "rate_card"),
            "rate_card.provenance",
        ),
        rates=rates,
    )


def _load_resource(
    payload: Any,
    *,
    node_id: str,
    rates: Mapping[str, float],
    index: int,
) -> Resource:
    name = f"nodes.{node_id}.resources[{index}]"
    root = _mapping(payload, name)
    kind = _string(_required(root, "kind", name), f"{name}.kind")
    _fail(kind in RESOURCE_KINDS, f"{name}.kind is unsupported: {kind}")
    throughput = root.get("throughput_bytes_per_second")
    return Resource(
        resource_id=_string(
            _required(root, "resource_id", name), f"{name}.resource_id"
        ),
        node_id=node_id,
        kind=kind,
        slots=_integer(root.get("slots", 1), f"{name}.slots", positive=True),
        base_latency_ms=_number(
            root.get("base_latency_ms", 0.0), f"{name}.base_latency_ms"
        ),
        throughput_bytes_per_second=(
            None
            if throughput is None
            else _number(
                throughput,
                f"{name}.throughput_bytes_per_second",
                positive=True,
            )
        ),
        jitter_fraction=_fraction(
            root.get("jitter_fraction", 0.0), f"{name}.jitter_fraction"
        ),
        time_rate_id=_optional_rate_id(
            root.get("time_rate_id"), f"{name}.time_rate_id", rates
        ),
        byte_rate_id=_optional_rate_id(
            root.get("byte_rate_id"), f"{name}.byte_rate_id", rates
        ),
    )


def _load_cache(payload: Any, *, node_id: str, index: int) -> Cache:
    name = f"nodes.{node_id}.caches[{index}]"
    root = _mapping(payload, name)
    entries: list[CacheEntry] = []
    for entry_index, value in enumerate(
        _list(root.get("initial_entries", []), f"{name}.initial_entries")
    ):
        entry_name = f"{name}.initial_entries[{entry_index}]"
        entry = _mapping(value, entry_name)
        entries.append(CacheEntry(
            object_id=_string(
                _required(entry, "object_id", entry_name),
                f"{entry_name}.object_id",
            ),
            representation_id=_string(
                _required(entry, "representation_id", entry_name),
                f"{entry_name}.representation_id",
            ),
            size_bytes=_integer(
                _required(entry, "size_bytes", entry_name),
                f"{entry_name}.size_bytes",
                positive=True,
            ),
        ))
    cache = Cache(
        cache_id=_string(
            _required(root, "cache_id", name), f"{name}.cache_id"
        ),
        node_id=node_id,
        capacity_bytes=_integer(
            _required(root, "capacity_bytes", name),
            f"{name}.capacity_bytes",
            positive=True,
        ),
        initial_entries=tuple(entries),
    )
    _fail(
        sum(entry.size_bytes for entry in entries) <= cache.capacity_bytes,
        f"{name} initial entries exceed capacity",
    )
    _fail(
        len({entry.key for entry in entries}) == len(entries),
        f"{name} contains duplicate initial entries",
    )
    return cache


def _load_node(payload: Any, rates: Mapping[str, float], index: int) -> Node:
    name = f"nodes[{index}]"
    root = _mapping(payload, name)
    node_id = _string(_required(root, "node_id", name), f"{name}.node_id")
    roles = tuple(
        _string(value, f"{name}.roles")
        for value in _list(_required(root, "roles", name), f"{name}.roles")
    )
    _fail(bool(roles), f"{name}.roles must not be empty")
    resources = tuple(
        _load_resource(value, node_id=node_id, rates=rates, index=i)
        for i, value in enumerate(
            _list(_required(root, "resources", name), f"{name}.resources")
        )
    )
    caches = tuple(
        _load_cache(value, node_id=node_id, index=i)
        for i, value in enumerate(_list(root.get("caches", []), f"{name}.caches"))
    )
    _unique(list(resources), "resource_id", f"resource on node {node_id}")
    _unique(list(caches), "cache_id", f"cache on node {node_id}")
    return Node(node_id=node_id, roles=roles, resources=resources, caches=caches)


def _load_link(payload: Any, rates: Mapping[str, float], index: int) -> Link:
    name = f"links[{index}]"
    root = _mapping(payload, name)
    return Link(
        link_id=_string(_required(root, "link_id", name), f"{name}.link_id"),
        source_node_id=_string(
            _required(root, "source_node_id", name),
            f"{name}.source_node_id",
        ),
        destination_node_id=_string(
            _required(root, "destination_node_id", name),
            f"{name}.destination_node_id",
        ),
        slots=_integer(root.get("slots", 1), f"{name}.slots", positive=True),
        bandwidth_bytes_per_second=_number(
            _required(root, "bandwidth_bytes_per_second", name),
            f"{name}.bandwidth_bytes_per_second",
            positive=True,
        ),
        round_trip_time_ms=_number(
            _required(root, "round_trip_time_ms", name),
            f"{name}.round_trip_time_ms",
        ),
        jitter_fraction=_fraction(
            root.get("jitter_fraction", 0.0), f"{name}.jitter_fraction"
        ),
        time_rate_id=_optional_rate_id(
            root.get("time_rate_id"), f"{name}.time_rate_id", rates
        ),
        byte_rate_id=_optional_rate_id(
            root.get("byte_rate_id"), f"{name}.byte_rate_id", rates
        ),
    )


def _load_object(payload: Any, index: int) -> DataObject:
    name = f"objects[{index}]"
    root = _mapping(payload, name)
    object_id = _string(
        _required(root, "object_id", name), f"{name}.object_id"
    )
    raw_representations = _mapping(
        _required(root, "representations", name), f"{name}.representations"
    )
    representations = {
        _string(key, f"{name}.representation id"): Representation(
            representation_id=_string(key, f"{name}.representation id"),
            size_bytes=_integer(
                value,
                f"{name}.representations.{key}",
                positive=True,
            ),
        )
        for key, value in raw_representations.items()
    }
    _fail(bool(representations), f"{name}.representations must not be empty")
    return DataObject(object_id=object_id, representations=representations)


def _load_workload(payload: Any, index: int) -> FlowMeshWorkload:
    name = f"workloads[{index}]"
    root = _mapping(payload, name)
    outcomes = _mapping(
        _required(root, "task_success_by_design", name),
        f"{name}.task_success_by_design",
    )
    parsed_outcomes: dict[str, bool] = {}
    for key, value in outcomes.items():
        _fail(
            isinstance(value, bool),
            f"{name}.task_success_by_design.{key} must be true or false",
        )
        parsed_outcomes[_string(key, f"{name}.design id")] = value
    return FlowMeshWorkload(
        workload_id=_string(
            _required(root, "workload_id", name), f"{name}.workload_id"
        ),
        workload_class=_string(
            _required(root, "workload_class", name),
            f"{name}.workload_class",
        ),
        object_id=_string(
            _required(root, "object_id", name), f"{name}.object_id"
        ),
        task_type=_string(
            _required(root, "task_type", name), f"{name}.task_type"
        ),
        quality_provenance=_string(
            _required(root, "quality_provenance", name),
            f"{name}.quality_provenance",
        ),
        task_success_by_design=parsed_outcomes,
    )


def _load_template(payload: Any, index: int) -> OperationTemplate:
    name = f"operation_templates[{index}]"
    root = _mapping(payload, name)
    operations = tuple(
        _mapping(value, f"{name}.operations[{i}]")
        for i, value in enumerate(
            _list(_required(root, "operations", name), f"{name}.operations")
        )
    )
    _fail(bool(operations), f"{name}.operations must not be empty")
    return OperationTemplate(
        template_id=_string(
            _required(root, "template_id", name), f"{name}.template_id"
        ),
        raw_operations=operations,
    )


def _load_design(payload: Any, index: int) -> PhysicalDesign:
    name = f"designs[{index}]"
    root = _mapping(payload, name)
    routes = _mapping(
        _required(root, "route_templates", name), f"{name}.route_templates"
    )
    bindings = _mapping(
        _required(root, "bindings", name), f"{name}.bindings"
    )
    return PhysicalDesign(
        design_id=_string(
            _required(root, "design_id", name), f"{name}.design_id"
        ),
        executor_node_id=_string(
            _required(root, "executor_node_id", name),
            f"{name}.executor_node_id",
        ),
        route_templates={
            _string(key, f"{name}.workload class"): _string(
                value, f"{name}.route_templates.{key}"
            )
            for key, value in routes.items()
        },
        bindings={
            _string(key, f"{name}.binding id"): _string(
                value, f"{name}.bindings.{key}"
            )
            for key, value in bindings.items()
        },
    )


def _resolve_value(value: Any, bindings: Mapping[str, str], name: str) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        binding_id = value[1:]
        _fail(
            binding_id in bindings,
            f"{name} references missing binding {binding_id}",
        )
        return bindings[binding_id]
    return value


def _resolve_operation(
    payload: Mapping[str, Any],
    bindings: Mapping[str, str],
    template_id: str,
) -> Operation:
    name = f"operation_templates.{template_id}"
    root = {
        key: _resolve_value(value, bindings, f"{name}.{key}")
        for key, value in payload.items()
    }
    condition_payload = root.get("condition")
    condition = None
    if condition_payload is not None:
        raw_condition = _mapping(condition_payload, f"{name}.condition")
        condition = Condition(
            cache_op_id=_string(
                _required(raw_condition, "cache_op_id", f"{name}.condition"),
                f"{name}.condition.cache_op_id",
            ),
            equals=_string(
                _required(raw_condition, "equals", f"{name}.condition"),
                f"{name}.condition.equals",
            ),
        )
        _fail(
            condition.equals in ("hit", "miss"),
            f"{name}.condition.equals must be hit or miss",
        )
    size = root.get("size_bytes")
    return Operation(
        op_id=_string(_required(root, "op_id", name), f"{name}.op_id"),
        kind=_string(_required(root, "kind", name), f"{name}.kind"),
        depends_on=tuple(
            _string(value, f"{name}.depends_on")
            for value in _list(root.get("depends_on", []), f"{name}.depends_on")
        ),
        resource_id=(
            None
            if root.get("resource_id") is None
            else _string(root["resource_id"], f"{name}.resource_id")
        ),
        link_id=(
            None
            if root.get("link_id") is None
            else _string(root["link_id"], f"{name}.link_id")
        ),
        cache_id=(
            None
            if root.get("cache_id") is None
            else _string(root["cache_id"], f"{name}.cache_id")
        ),
        representation_id=(
            None
            if root.get("representation_id") is None
            else _string(
                root["representation_id"], f"{name}.representation_id"
            )
        ),
        size_bytes=(
            None
            if size is None
            else _integer(size, f"{name}.size_bytes", positive=True)
        ),
        byte_multiplier=_number(
            root.get("byte_multiplier", 1.0),
            f"{name}.byte_multiplier",
            positive=True,
        ),
        service_ms=_number(
            root.get("service_ms", 0.0), f"{name}.service_ms"
        ),
        condition=condition,
    )


def _validate_operation_dag(
    operations: tuple[Operation, ...],
    *,
    scenario: SimulatorScenario,
    label: str,
) -> None:
    by_id = _unique(list(operations), "op_id", f"operation in {label}")
    for operation in operations:
        _fail(
            operation.kind in OPERATION_KINDS,
            f"{label}.{operation.op_id} has unsupported kind {operation.kind}",
        )
        for dependency in operation.depends_on:
            _fail(
                dependency in by_id,
                f"{label}.{operation.op_id} depends on unknown {dependency}",
            )
            _fail(
                dependency != operation.op_id,
                f"{label}.{operation.op_id} depends on itself",
            )
        if operation.resource_id is not None:
            _fail(
                operation.resource_id in scenario.resources,
                f"{label}.{operation.op_id} references unknown resource "
                f"{operation.resource_id}",
            )
        if operation.link_id is not None:
            _fail(
                operation.link_id in scenario.links,
                f"{label}.{operation.op_id} references unknown link "
                f"{operation.link_id}",
            )
        if operation.kind in (
            "cache_read",
            "compute",
            "control",
            "index_query",
            "storage_read",
        ):
            _fail(
                operation.resource_id in scenario.resources,
                f"{label}.{operation.op_id} references unknown resource "
                f"{operation.resource_id}",
            )
        if operation.kind == "network_transfer":
            _fail(
                operation.link_id in scenario.links,
                f"{label}.{operation.op_id} references unknown link "
                f"{operation.link_id}",
            )
        if operation.kind in ("cache_insert", "cache_lookup"):
            _fail(
                operation.cache_id in scenario.caches,
                f"{label}.{operation.op_id} references unknown cache "
                f"{operation.cache_id}",
            )
        if operation.kind in (
            "cache_insert",
            "cache_lookup",
            "cache_read",
            "network_transfer",
            "storage_read",
        ):
            _fail(
                operation.representation_id is not None
                or operation.size_bytes is not None,
                f"{label}.{operation.op_id} requires representation_id or "
                "size_bytes",
            )
        if operation.condition is not None:
            cache_op = by_id.get(operation.condition.cache_op_id)
            _fail(
                cache_op is not None and cache_op.kind == "cache_lookup",
                f"{label}.{operation.op_id} condition must reference a "
                "cache_lookup",
            )
            _fail(
                operation.condition.cache_op_id in operation.depends_on,
                f"{label}.{operation.op_id} must depend on its cache lookup",
            )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(op_id: str) -> None:
        if op_id in visiting:
            raise SimulatorConfigError(f"{label} operation DAG contains a cycle")
        if op_id in visited:
            return
        visiting.add(op_id)
        for dependency in by_id[op_id].depends_on:
            visit(dependency)
        visiting.remove(op_id)
        visited.add(op_id)

    for op_id in by_id:
        visit(op_id)


def load_simulator_scenario(path: str | Path) -> SimulatorScenario:
    """Load and fully validate one immutable simulator scenario."""

    source = Path(path).resolve()
    _fail(source.is_file(), f"simulator scenario does not exist: {source}")
    encoded = source.read_bytes()
    try:
        root = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=_no_duplicate_object_pairs,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SimulatorConfigError(
            f"simulator scenario is not valid UTF-8 JSON: {source}"
        ) from exc
    root = _mapping(root, "scenario")
    schema = _string(
        _required(root, "schema_version", "scenario"),
        "scenario.schema_version",
    )
    _fail(
        schema == SIMULATOR_SCENARIO_SCHEMA_VERSION,
        "unsupported simulator scenario schema_version",
    )
    rate_card = _load_rate_card(_required(root, "rate_card", "scenario"))
    nodes_tuple = tuple(
        _load_node(value, rate_card.rates, i)
        for i, value in enumerate(
            _list(_required(root, "nodes", "scenario"), "scenario.nodes")
        )
    )
    nodes = _unique(list(nodes_tuple), "node_id", "node")
    resources = _unique(
        [resource for node in nodes_tuple for resource in node.resources],
        "resource_id",
        "resource",
    )
    caches = _unique(
        [cache for node in nodes_tuple for cache in node.caches],
        "cache_id",
        "cache",
    )
    links_tuple = tuple(
        _load_link(value, rate_card.rates, i)
        for i, value in enumerate(
            _list(_required(root, "links", "scenario"), "scenario.links")
        )
    )
    links = _unique(list(links_tuple), "link_id", "link")
    for link in links.values():
        _fail(
            link.source_node_id in nodes,
            f"link {link.link_id} has unknown source node",
        )
        _fail(
            link.destination_node_id in nodes,
            f"link {link.link_id} has unknown destination node",
        )
        _fail(
            link.source_node_id != link.destination_node_id,
            f"link {link.link_id} must connect two distinct nodes",
        )

    objects_tuple = tuple(
        _load_object(value, i)
        for i, value in enumerate(
            _list(_required(root, "objects", "scenario"), "scenario.objects")
        )
    )
    objects = _unique(list(objects_tuple), "object_id", "object")
    workloads = tuple(
        _load_workload(value, i)
        for i, value in enumerate(
            _list(_required(root, "workloads", "scenario"), "scenario.workloads")
        )
    )
    _unique(list(workloads), "workload_id", "workload")
    templates_tuple = tuple(
        _load_template(value, i)
        for i, value in enumerate(
            _list(
                _required(root, "operation_templates", "scenario"),
                "scenario.operation_templates",
            )
        )
    )
    templates = _unique(list(templates_tuple), "template_id", "template")
    designs = tuple(
        _load_design(value, i)
        for i, value in enumerate(
            _list(_required(root, "designs", "scenario"), "scenario.designs")
        )
    )
    _unique(list(designs), "design_id", "design")
    _fail(bool(workloads), "scenario.workloads must not be empty")
    _fail(bool(designs), "scenario.designs must not be empty")
    repetitions = _integer(
        _required(root, "repetitions", "scenario"),
        "scenario.repetitions",
        positive=True,
    )

    scenario = SimulatorScenario(
        source_path=source,
        source_sha256=_canonical_sha256(root),
        scenario_id=_string(
            _required(root, "scenario_id", "scenario"),
            "scenario.scenario_id",
        ),
        seed=_integer(_required(root, "seed", "scenario"), "scenario.seed"),
        repetitions=repetitions,
        arrival_interval_ms=_number(
            root.get("arrival_interval_ms", 0.0),
            "scenario.arrival_interval_ms",
        ),
        trial_admission_slots=_integer(
            root.get(
                "trial_admission_slots",
                len(workloads) * len(designs) * repetitions,
            ),
            "scenario.trial_admission_slots",
            positive=True,
        ),
        calibration_provenance=_string(
            _required(root, "calibration_provenance", "scenario"),
            "scenario.calibration_provenance",
        ),
        rate_card=rate_card,
        nodes=nodes,
        resources=resources,
        caches=caches,
        links=links,
        objects=objects,
        workloads=workloads,
        templates=templates,
        designs=designs,
    )
    _fail(
        scenario.trial_admission_slots <= scenario.planned_trial_count,
        "scenario.trial_admission_slots cannot exceed planned trial count",
    )

    design_ids = {design.design_id for design in designs}
    workload_classes = {workload.workload_class for workload in workloads}
    for cache in caches.values():
        for entry in cache.initial_entries:
            obj = objects.get(entry.object_id)
            _fail(obj is not None, f"cache {cache.cache_id} has unknown object")
            representation = obj.representations.get(entry.representation_id)
            _fail(
                representation is not None,
                f"cache {cache.cache_id} has unknown representation",
            )
            _fail(
                representation.size_bytes == entry.size_bytes,
                f"cache {cache.cache_id} entry size disagrees with object",
            )
    for workload in workloads:
        _fail(
            workload.object_id in objects,
            f"workload {workload.workload_id} references unknown object",
        )
        _fail(
            set(workload.task_success_by_design) == design_ids,
            f"workload {workload.workload_id} must declare a frozen success "
            "outcome for every design",
        )
    for design in designs:
        _fail(
            design.executor_node_id in nodes,
            f"design {design.design_id} has unknown executor node",
        )
        _fail(
            set(design.route_templates) == workload_classes,
            f"design {design.design_id} must route every workload class",
        )
        for workload_class, template_id in design.route_templates.items():
            _fail(
                template_id in templates,
                f"design {design.design_id} references unknown template "
                f"{template_id}",
            )
            operations = scenario.operations_for(design, workload_class)
            for operation in operations:
                if operation.representation_id is None:
                    continue
                for workload in workloads:
                    if workload.workload_class != workload_class:
                        continue
                    _fail(
                        operation.representation_id
                        in objects[workload.object_id].representations,
                        f"design {design.design_id} template {template_id} "
                        f"requires missing representation "
                        f"{operation.representation_id} for "
                        f"{workload.object_id}",
                    )
    return scenario
