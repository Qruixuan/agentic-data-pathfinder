"""Validated container-emulation bindings for backend-neutral plans.

This is a planning and readiness layer, not a Docker launcher.  It proves that
every logical node, resource, cache, link, operation kind, and task type has
one explicit container-side implementation.  Host privileges, image digests,
and service health remain launch-time evidence and are never assumed here.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from collections import Counter
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from .config import SimulatorScenario, load_simulator_scenario
from .portable import verify_portable_execution_plan


CONTAINER_SPEC_SCHEMA_VERSION = (
    "pathfinder.container-emulation-backend-spec/v1alpha1"
)
CONTAINER_TOPOLOGY_SCHEMA_VERSION = (
    "pathfinder.container-emulation-topology/v1alpha1"
)
CONTAINER_OPERATION_SCHEMA_VERSION = (
    "pathfinder.container-emulation-operation/v1alpha1"
)
CONTAINER_READINESS_SCHEMA_VERSION = (
    "pathfinder.container-emulation-readiness/v1alpha1"
)
CONTAINER_PLAN_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.container-emulation-plan-run/v1alpha1"
)

_OUTPUT_FILES = {
    "container_operations.jsonl",
    "container_plan_manifest.json",
    "container_readiness.json",
    "container_topology.json",
}

_CONTAINER_NAME = re.compile(r"[a-z0-9][a-z0-9_.-]*")
_SENSITIVE_PARTS = (
    "api_key",
    "authorization",
    "bearer",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)


class ContainerContractError(ValueError):
    """Raised when a container backend cannot cover the portable plan."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContainerContractError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise ContainerContractError(f"non-finite JSON number: {value}")


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ContainerContractError(f"cannot read valid {name}: {path}") from exc
    return raw, value


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ContainerContractError(f"cannot read {name}: {path}") from exc
    result: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(
                line,
                object_pairs_hook=_unique_keys,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, ContainerContractError) as exc:
            raise ContainerContractError(
                f"invalid {name} at line {line_number}: {exc}"
            ) from exc
        _require(isinstance(value, dict), f"{name} row must be an object")
        result.append(value)
    return result


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _array(value: Any, name: str) -> list[Any]:
    _require(isinstance(value, list), f"{name} must be an array")
    return value


def _text(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical_bytes(value) + b"\n" for value in values)


def _scan_sensitive(value: Any, path: str = "spec") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).casefold()
            if lowered == "credentials_recorded":
                _require(
                    child is False,
                    f"{path}.{key} must be the literal false",
                )
            else:
                _require(
                    not any(part in lowered for part in _SENSITIVE_PARTS),
                    f"{path}.{key} is credential-like and must not be recorded",
                )
            _scan_sensitive(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _scan_sensitive(child, f"{path}[{index}]")


def _unique_by(
    values: list[Any],
    field: str,
    label: str,
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for index, value in enumerate(values):
        item = _mapping(value, f"{label}[{index}]")
        key = _text(item.get(field), f"{label}[{index}].{field}")
        _require(key not in result, f"duplicate {label} {field}: {key}")
        result[key] = item
    return result


def _exact_coverage(actual: set[str], expected: set[str], label: str) -> None:
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    _require(
        not missing and not extra,
        f"{label} coverage mismatch; missing={missing}, extra={extra}",
    )


def _load_spec(
    path: Path,
    scenario: SimulatorScenario,
) -> tuple[bytes, dict[str, Any]]:
    raw, value = _read_json(path, "container backend spec")
    root = dict(_mapping(value, "container backend spec"))
    _scan_sensitive(root)
    _require(
        root.get("schema_version") == CONTAINER_SPEC_SCHEMA_VERSION,
        "unsupported container backend spec schema_version",
    )
    _text(root.get("backend_id"), "backend_id")
    _require(root.get("scenario_id") == scenario.scenario_id, "scenario_id mismatch")
    _require(
        root.get("deployment_mode") == "single-host-eight-container",
        "deployment_mode must be single-host-eight-container",
    )
    _require(
        root.get("orchestrator") == "docker-compose-v2",
        "orchestrator must be docker-compose-v2",
    )
    _require(
        root.get("measurement_clock") == "monotonic-nanoseconds",
        "measurement_clock must be monotonic-nanoseconds",
    )
    nodes = _unique_by(_array(root.get("nodes"), "nodes"), "node_id", "nodes")
    _exact_coverage(set(nodes), set(scenario.nodes), "node")
    container_names: set[str] = set()
    network_namespaces: set[str] = set()
    for node_id, node in nodes.items():
        container_name = _text(node.get("container_name"), f"nodes.{node_id}.name")
        _require(
            _CONTAINER_NAME.fullmatch(container_name) is not None,
            f"invalid container_name for {node_id}",
        )
        _require(container_name not in container_names, "duplicate container_name")
        container_names.add(container_name)
        namespace = _text(
            node.get("network_namespace"),
            f"nodes.{node_id}.network_namespace",
        )
        _require(namespace not in network_namespaces, "duplicate network_namespace")
        network_namespaces.add(namespace)
        _text(node.get("image_ref"), f"nodes.{node_id}.image_ref")
        digest = node.get("image_digest")
        _require(
            digest is None
            or (
                isinstance(digest, str)
                and len(digest) == 71
                and digest.startswith("sha256:")
                and all(char in "0123456789abcdef" for char in digest[7:])
            ),
            f"nodes.{node_id}.image_digest is invalid",
        )
        _require(
            node.get("read_only_root_filesystem") is True,
            f"nodes.{node_id} must use a read-only root filesystem",
        )
    resources = _unique_by(
        _array(root.get("resource_adapters"), "resource_adapters"),
        "resource_id",
        "resource_adapters",
    )
    _exact_coverage(set(resources), set(scenario.resources), "resource adapter")
    for resource_id, item in resources.items():
        expected = scenario.resources[resource_id]
        _require(item.get("node_id") == expected.node_id, "resource node mismatch")
        _require(item.get("resource_kind") == expected.kind, "resource kind mismatch")
        _text(item.get("adapter"), f"resource_adapters.{resource_id}.adapter")
        _text(
            item.get("measurement_source"),
            f"resource_adapters.{resource_id}.measurement_source",
        )
    caches = _unique_by(
        _array(root.get("cache_adapters"), "cache_adapters"),
        "cache_id",
        "cache_adapters",
    )
    _exact_coverage(set(caches), set(scenario.caches), "cache adapter")
    for cache_id, item in caches.items():
        expected = scenario.caches[cache_id]
        _require(item.get("node_id") == expected.node_id, "cache node mismatch")
        _require(
            item.get("capacity_bytes") == expected.capacity_bytes,
            "cache capacity mismatch",
        )
        _text(item.get("adapter"), f"cache_adapters.{cache_id}.adapter")
        expected_entries = [
            {
                "object_id": entry.object_id,
                "representation_id": entry.representation_id,
                "size_bytes": entry.size_bytes,
            }
            for entry in expected.initial_entries
        ]
        _require(
            item.get("initial_entries") == expected_entries,
            f"cache adapter {cache_id} initial entries mismatch",
        )
    links = _unique_by(
        _array(root.get("link_adapters"), "link_adapters"),
        "link_id",
        "link_adapters",
    )
    _exact_coverage(set(links), set(scenario.links), "link adapter")
    for link_id, item in links.items():
        expected = scenario.links[link_id]
        _require(
            item.get("source_node_id") == expected.source_node_id,
            "link source node mismatch",
        )
        _require(
            item.get("destination_node_id") == expected.destination_node_id,
            "link destination node mismatch",
        )
        _require(
            item.get("bandwidth_bytes_per_second")
            == expected.bandwidth_bytes_per_second,
            "link bandwidth mismatch",
        )
        _require(
            item.get("round_trip_time_ms") == expected.round_trip_time_ms,
            "link RTT mismatch",
        )
        adapter = _text(
            item.get("adapter"),
            f"link_adapters.{link_id}.adapter",
        )
        capabilities = _array(
            item.get("required_capabilities"),
            f"link_adapters.{link_id}.required_capabilities",
        )
        if adapter == "linux-tc-netem-tbf-v1":
            _require(
                capabilities == ["NET_ADMIN"],
                f"link adapter {link_id} must declare NET_ADMIN",
            )
        elif adapter == "application-rate-rtt-shaper-v1":
            _require(
                capabilities == [],
                f"application link adapter {link_id} needs no host capability",
            )
        else:
            raise ContainerContractError(
                f"unsupported link adapter for {link_id}: {adapter}"
            )
    operation_adapters = _mapping(
        root.get("operation_adapters"),
        "operation_adapters",
    )
    operation_kinds = {
        operation.kind
        for design in scenario.designs
        for workload in scenario.workloads
        for operation in scenario.operations_for(design, workload.workload_class)
    }
    _exact_coverage(set(operation_adapters), operation_kinds, "operation adapter")
    for kind, adapter in operation_adapters.items():
        _text(adapter, f"operation_adapters.{kind}")
    task_executors = _unique_by(
        _array(root.get("task_executors"), "task_executors"),
        "task_type",
        "task_executors",
    )
    _exact_coverage(
        set(task_executors),
        {workload.task_type for workload in scenario.workloads},
        "task executor",
    )
    for task_type, executor in task_executors.items():
        _text(executor.get("adapter"), f"task_executors.{task_type}.adapter")
        _require(
            type(executor.get("semantic_quality_enabled")) is bool,
            f"task_executors.{task_type}.semantic_quality_enabled must be boolean",
        )
    payload = _mapping(root.get("payload_policy"), "payload_policy")
    _require(
        payload.get("mode")
        in ("content-addressed-artifacts", "deterministic-size-preserving-fixture"),
        "unsupported payload_policy.mode",
    )
    _require(
        type(payload.get("semantic_content_required")) is bool,
        "payload_policy.semantic_content_required must be boolean",
    )
    return raw, root


def _documents(
    scenario: SimulatorScenario,
    spec_raw: bytes,
    spec: Mapping[str, Any],
    portable_root: Path,
    portable: Mapping[str, Any],
) -> dict[str, bytes]:
    nodes = _unique_by(list(spec["nodes"]), "node_id", "nodes")
    resources = _unique_by(
        list(spec["resource_adapters"]),
        "resource_id",
        "resource_adapters",
    )
    caches = _unique_by(
        list(spec["cache_adapters"]),
        "cache_id",
        "cache_adapters",
    )
    links = _unique_by(
        list(spec["link_adapters"]),
        "link_id",
        "link_adapters",
    )
    task_executors = _unique_by(
        list(spec["task_executors"]),
        "task_type",
        "task_executors",
    )
    operation_adapters = _mapping(
        spec["operation_adapters"],
        "operation_adapters",
    )
    trials = _read_jsonl(portable_root / "trials.jsonl", "portable trials")
    trial_by_key = {row["trial_key"]: row for row in trials}
    portable_operations = _read_jsonl(
        portable_root / "operations.jsonl",
        "portable operations",
    )
    rows: list[dict[str, Any]] = []
    for operation in portable_operations:
        trial = trial_by_key[operation["trial_key"]]
        resource_binding = operation.get("resource_binding")
        link_binding = operation.get("link_binding")
        cache_binding = operation.get("cache_binding")
        resource_adapter = None
        if resource_binding is not None:
            resource_adapter = dict(resources[resource_binding["resource_id"]])
        link_adapter = None
        if link_binding is not None:
            link_adapter = dict(links[link_binding["link_id"]])
        cache_adapter = None
        if cache_binding is not None:
            cache_adapter = dict(caches[cache_binding["cache_id"]])
        if resource_binding is not None:
            execution_node_id = resource_binding["node_id"]
            destination_node_id = execution_node_id
        elif link_binding is not None:
            execution_node_id = link_binding["source_node_id"]
            destination_node_id = link_binding["destination_node_id"]
        elif cache_binding is not None:
            execution_node_id = cache_binding["node_id"]
            destination_node_id = execution_node_id
        else:
            execution_node_id = trial["executor_node_id"]
            destination_node_id = execution_node_id
        rows.append({
            "schema_version": CONTAINER_OPERATION_SCHEMA_VERSION,
            "backend_id": spec["backend_id"],
            "portable_plan_sha256": portable["plan_sha256"],
            "operation_key": operation["operation_key"],
            "trial_key": operation["trial_key"],
            "operation_id": operation["operation_id"],
            "operation_kind": operation["operation_kind"],
            "dependency_operation_keys": operation[
                "dependency_operation_keys"
            ],
            "condition": operation["condition"],
            "object_id": operation["object_id"],
            "representation_id": operation["representation_id"],
            "logical_bytes": operation["logical_bytes"],
            "operation_adapter": operation_adapters[
                operation["operation_kind"]
            ],
            "resource_adapter": resource_adapter,
            "link_adapter": link_adapter,
            "cache_adapter": cache_adapter,
            "task_executor": dict(task_executors[trial["task_type"]]),
            "execution_node_id": execution_node_id,
            "execution_container": nodes[execution_node_id]["container_name"],
            "destination_node_id": destination_node_id,
            "destination_container": nodes[destination_node_id][
                "container_name"
            ],
            "measure_actual_duration": True,
            "simulation_hint_used_as_measured_duration": False,
        })
    unpinned_nodes = sorted(
        node_id for node_id, node in nodes.items()
        if node.get("image_digest") is None
    )
    semantic_disabled = sorted(
        task_type for task_type, executor in task_executors.items()
        if executor["semantic_quality_enabled"] is False
    )
    blockers: list[dict[str, Any]] = []
    if unpinned_nodes:
        blockers.append({
            "check_id": "immutable_container_images",
            "detail": "container image digests must be recorded before a frozen run",
            "affected_node_ids": unpinned_nodes,
        })
    network_shaping_requires_net_admin = any(
        "NET_ADMIN" in item.get("required_capabilities", [])
        for item in links.values()
    )
    capability_detail = "Docker Compose v2 requires live host preflight"
    if network_shaping_requires_net_admin:
        capability_detail += "; NET_ADMIN support must also be verified"
    blockers.append({
        "check_id": "host_runtime_capabilities",
        "detail": capability_detail,
        "affected_node_ids": sorted(nodes),
    })
    advisories: list[dict[str, Any]] = []
    if semantic_disabled:
        advisories.append({
            "check_id": "semantic_quality_disabled",
            "detail": (
                "infrastructure conformance may run, but task-quality claims are disabled"
            ),
            "task_types": semantic_disabled,
        })
    if spec["payload_policy"]["mode"] == "deterministic-size-preserving-fixture":
        advisories.append({
            "check_id": "synthetic_payload_content",
            "detail": (
                "payload sizes and transfers are executable, but bytes are not modality evidence"
            ),
        })
    readiness = {
        "schema_version": CONTAINER_READINESS_SCHEMA_VERSION,
        "status": "CONTRACT_READY_LAUNCH_UNVERIFIED",
        "backend_id": spec["backend_id"],
        "scenario_id": scenario.scenario_id,
        "portable_plan_sha256": portable["plan_sha256"],
        "contract_complete": True,
        "launch_authorized": False,
        "launch_blockers": blockers,
        "advisory_warnings": advisories,
        "covered_nodes": len(nodes),
        "covered_resources": len(resources),
        "covered_caches": len(caches),
        "covered_links": len(links),
        "covered_operations": len(rows),
        "network_shaping_requires_net_admin": (
            network_shaping_requires_net_admin
        ),
        "docker_or_host_probed": False,
        "container_started": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    topology = {
        "schema_version": CONTAINER_TOPOLOGY_SCHEMA_VERSION,
        "backend_id": spec["backend_id"],
        "scenario_id": scenario.scenario_id,
        "deployment_mode": spec["deployment_mode"],
        "orchestrator": spec["orchestrator"],
        "measurement_clock": spec["measurement_clock"],
        "nodes": [dict(nodes[key]) for key in sorted(nodes)],
        "resource_adapters": [
            dict(resources[key]) for key in sorted(resources)
        ],
        "cache_adapters": [dict(caches[key]) for key in sorted(caches)],
        "link_adapters": [dict(links[key]) for key in sorted(links)],
        "operation_adapters": dict(sorted(operation_adapters.items())),
        "task_executors": [
            dict(task_executors[key]) for key in sorted(task_executors)
        ],
        "payload_policy": dict(spec["payload_policy"]),
        "container_started": False,
        "credentials_recorded": False,
    }
    topology_bytes = _json_bytes(topology)
    operation_bytes = _jsonl_bytes(rows)
    readiness_bytes = _json_bytes(readiness)
    documents = {
        "container_operations.jsonl": operation_bytes,
        "container_readiness.json": readiness_bytes,
        "container_topology.json": topology_bytes,
    }
    manifest = {
        "schema_version": CONTAINER_PLAN_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "backend_id": spec["backend_id"],
        "scenario_id": scenario.scenario_id,
        "scenario_sha256": scenario.source_sha256,
        "portable_plan_sha256": portable["plan_sha256"],
        "portable_manifest_sha256": _sha256_bytes(
            (portable_root / "portable_plan_manifest.json").read_bytes()
        ),
        "container_spec_sha256": _sha256_bytes(spec_raw),
        "planned_trial_count": portable["planned_trial_count"],
        "planned_operation_count": len(rows),
        "contract_complete": True,
        "launch_authorized": False,
        "container_started": False,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["container_plan_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )
    return documents


def _verify_output(root: Path) -> dict[str, Any]:
    expected = _OUTPUT_FILES | {"SHA256SUMS"}
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected, "container plan output file set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _OUTPUT_FILES,
            "container plan SHA256SUMS is malformed",
        )
        _require(name not in checksums, f"duplicate checksum entry: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"container plan checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(set(checksums) == _OUTPUT_FILES, "container checksums are incomplete")
    _, manifest_value = _read_json(
        root / "container_plan_manifest.json",
        "container plan manifest",
    )
    manifest = _mapping(manifest_value, "container plan manifest")
    _require(
        manifest.get("schema_version") == CONTAINER_PLAN_MANIFEST_SCHEMA_VERSION,
        "unsupported container plan manifest schema_version",
    )
    _require(manifest.get("status") == "COMPLETE", "container plan is incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "container_plan_manifest.json"
        },
        "container manifest output digests disagree",
    )
    _, readiness_value = _read_json(
        root / "container_readiness.json",
        "container readiness",
    )
    readiness = _mapping(readiness_value, "container readiness")
    _require(readiness.get("contract_complete") is True, "contract is incomplete")
    _require(
        readiness.get("launch_authorized") is False,
        "planning output must not authorize a launch",
    )
    operations = _read_jsonl(
        root / "container_operations.jsonl",
        "container operations",
    )
    _require(
        len(operations) == manifest.get("planned_operation_count"),
        "container operation count mismatch",
    )
    keys = [row.get("operation_key") for row in operations]
    _require(len(keys) == len(set(keys)), "duplicate container operation key")
    for row in operations:
        _require(
            row.get("schema_version") == CONTAINER_OPERATION_SCHEMA_VERSION,
            "unsupported container operation schema_version",
        )
        _text(row.get("execution_node_id"), "container execution_node_id")
        _text(row.get("execution_container"), "container execution_container")
        _text(row.get("destination_node_id"), "container destination_node_id")
        _text(
            row.get("destination_container"),
            "container destination_container",
        )
        _require(
            row.get("simulation_hint_used_as_measured_duration") is False,
            "container operation attempts to promote a simulation hint",
        )
    return dict(manifest)


def plan_container_backend(
    scenario_path: str | Path,
    portable_plan_dir: str | Path,
    container_spec_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Bind a portable plan to a complete, non-launching container contract."""

    scenario = load_simulator_scenario(scenario_path)
    portable_root = Path(portable_plan_dir).resolve()
    portable_verified = verify_portable_execution_plan(portable_root)
    _require(
        portable_verified["scenario_id"] == scenario.scenario_id,
        "portable plan and scenario IDs differ",
    )
    _, portable_value = _read_json(
        portable_root / "portable_plan.json",
        "portable plan",
    )
    portable = _mapping(portable_value, "portable plan")
    _require(
        portable.get("scenario_sha256") == scenario.source_sha256,
        "portable plan is bound to different scenario content",
    )
    spec_path = Path(container_spec_path).resolve()
    spec_raw, spec = _load_spec(spec_path, scenario)
    documents = _documents(
        scenario,
        spec_raw,
        spec,
        portable_root,
        portable,
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"container plan output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".container-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_output(staging)
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    manifest = _verify_output(target)
    readiness = json.loads(
        (target / "container_readiness.json").read_text(encoding="utf-8")
    )
    return {
        **manifest,
        "readiness_status": readiness["status"],
        "launch_blocker_count": len(readiness["launch_blockers"]),
        "output_dir": str(target),
        "readiness_path": str(target / "container_readiness.json"),
    }


def verify_container_backend_plan(output_dir: str | Path) -> dict[str, Any]:
    """Read-only verification of one published container contract."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"container plan output does not exist: {root}")
    manifest = _verify_output(root)
    readiness = json.loads(
        (root / "container_readiness.json").read_text(encoding="utf-8")
    )
    return {
        "status": "VERIFIED",
        "backend_id": manifest["backend_id"],
        "scenario_id": manifest["scenario_id"],
        "portable_plan_sha256": manifest["portable_plan_sha256"],
        "planned_trial_count": manifest["planned_trial_count"],
        "planned_operation_count": manifest["planned_operation_count"],
        "readiness_status": readiness["status"],
        "launch_blocker_count": len(readiness["launch_blockers"]),
        "checked_files": len(_OUTPUT_FILES),
        "eligible_for_scientific_claims": False,
    }
