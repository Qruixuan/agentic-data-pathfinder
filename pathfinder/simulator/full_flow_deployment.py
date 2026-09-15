"""Bind endpoint-free full-flow routes to a concrete deployment.

The logical route package is portable.  This module creates the separate
environment-specific artifact that says which implementation serves each
logical service contract.  It records endpoint addresses and *names* of
credential environment variables, never credential values.  A deployment is
accepted only when every action and representation required by the frozen
route plan is implemented.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import os
import re
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable, Mapping

from .full_flow_logical_routes import verify_full_flow_logical_routes


# Keep the original exported names as compatibility aliases.  Older callers
# imported them when constructing v1alpha1 fixtures.  New operator templates
# deliberately use the explicit v1alpha2 constants below.
DEPLOYMENT_SOURCE_SCHEMA_VERSION = (
    "pathfinder.full-flow-deployment-source/v1alpha1"
)
DEPLOYMENT_BINDING_SCHEMA_VERSION = (
    "pathfinder.full-flow-deployment-binding/v1alpha1"
)
DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2 = (
    "pathfinder.full-flow-deployment-source/v1alpha2"
)
DEPLOYMENT_BINDING_SCHEMA_VERSION_V1ALPHA2 = (
    "pathfinder.full-flow-deployment-binding/v1alpha2"
)
W4_COORDINATOR_HEALTH_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-coordinator-health/v1alpha2"
)
DEPLOYMENT_BINDING_NAME = "full-flow-deployment-binding.json"
CHECKSUMS_NAME = "SHA256SUMS"

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_ENVIRONMENT_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SIMULATOR_HOST = re.compile(r"pathfinder-sim-[a-z0-9-]+\Z")
_BACKENDS = {"single-host-compose", "multi-host-private-network"}
_NETWORK_MODES = {
    "application-shaped-single-host",
    "physical-private-network",
    "kernel-shaped-private-network",
}
_MAX_HEALTH_BYTES = 1024 * 1024

_SOURCE_FIELDS_V1ALPHA1 = {
    "schema_version",
    "deployment_id",
    "backend",
    "service_bindings",
    "network_binding",
    "trusted_private_http_hosts",
    "credentials_recorded",
}
_SOURCE_FIELDS_V1ALPHA2 = _SOURCE_FIELDS_V1ALPHA1 | {
    "runtime_service_bindings",
}
_SERVICE_FIELDS = {
    "service_contract_id",
    "adapter_id",
    "logical_node_ids",
    "actions",
    "representation_ids",
    "base_url",
    "credential_env_names",
    "persistent_state",
}
_NETWORK_FIELDS = {
    "adapter_id",
    "mode",
    "measurement_class",
    "parameters_fitted",
}
_RUNTIME_SERVICE_FIELDS = {
    "runtime_service_contract_id",
    "parent_service_contract_id",
    "logical_node_id",
    "adapter_id",
    "base_url",
    "credential_env_names",
    "persistent_state",
    "health_route",
    "health_schema_version",
}
_W4_COORDINATOR_ADAPTER_ID = "pathfinder-w4-coordinator-http-v1"
_W4_COORDINATOR_CREDENTIAL_ENV_NAMES = (
    "PATHFINDER_CONTAINER_NODE_TOKEN",
    "PATHFINDER_DATA_AGENT_TOKEN",
    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
    "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
    "PATHFINDER_N2_INDEX_TOKEN",
    "PATHFINDER_N3_DATA_AGENT_TOKEN",
    "PATHFINDER_N4_DATA_AGENT_TOKEN",
    "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
    "PATHFINDER_N7_INDEX_TOKEN",
    "PATHFINDER_N7_W4_CACHE_TOKEN",
    "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
    "PATHFINDER_N8_INDEX_TOKEN",
    "PATHFINDER_N8_W4_CACHE_TOKEN",
)
_W4_RUNTIME_CONTRACTS = {
    "N7.w4-candidate-coordinator": {
        "parent_service_contract_id": "N7.execution-compute",
        "logical_node_id": "N7",
    },
    "N8.w4-candidate-coordinator": {
        "parent_service_contract_id": "N8.execution-compute",
        "logical_node_id": "N8",
    },
}


class FullFlowDeploymentError(ValueError):
    """Raised when a runtime binding is missing or unsafe."""


def full_flow_w4_runtime_service_binding_requirements() -> list[dict[str, Any]]:
    """Return the immutable deployment contract for both W4 coordinators."""

    return [
        {
            "runtime_service_contract_id": runtime_id,
            "parent_service_contract_id": str(
                expectation["parent_service_contract_id"]
            ),
            "logical_node_id": str(expectation["logical_node_id"]),
            "adapter_id": _W4_COORDINATOR_ADAPTER_ID,
            "credential_env_names": list(
                _W4_COORDINATOR_CREDENTIAL_ENV_NAMES
            ),
            "persistent_state": True,
            "health_route": "/healthz",
            "health_schema_version": W4_COORDINATOR_HEALTH_SCHEMA_VERSION,
        }
        for runtime_id, expectation in sorted(_W4_RUNTIME_CONTRACTS.items())
    ]


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowDeploymentError(message)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _strict_json(path: Path, name: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{name} repeats key {key}")
            result[key] = value
        return result

    def invalid(value: str) -> None:
        raise FullFlowDeploymentError(f"{name} contains {value}")

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=invalid,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowDeploymentError(f"cannot read {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _array(value: Any, name: str) -> list[Any]:
    _require(isinstance(value, list), f"{name} must be an array")
    return value


def _string_array(
    value: Any,
    name: str,
    *,
    environment_names: bool = False,
) -> list[str]:
    values = _array(value, name)
    normalized: list[str] = []
    for position, item in enumerate(values):
        if environment_names:
            _require(
                isinstance(item, str)
                and _ENVIRONMENT_NAME.fullmatch(item) is not None,
                f"{name}[{position}] is not an environment variable name",
            )
            normalized.append(str(item))
        else:
            normalized.append(_identifier(item, f"{name}[{position}]"))
    _require(
        normalized == sorted(set(normalized)),
        f"{name} must be sorted and unique",
    )
    return normalized


def _loopback(hostname: str | None) -> bool:
    if hostname is None:
        return False
    if hostname.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _origin(
    value: Any,
    *,
    backend: str,
    trusted_private_hosts: set[str],
) -> str:
    _require(isinstance(value, str) and value == value.strip(), "base_url is invalid")
    parsed = urllib.parse.urlsplit(value)
    _require(
        parsed.scheme in {"http", "https"}
        and parsed.hostname is not None
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment,
        "base_url must be an HTTP(S) origin without credentials",
    )
    try:
        port = parsed.port
    except ValueError as exc:
        raise FullFlowDeploymentError("base_url port is invalid") from exc
    host = str(parsed.hostname).casefold()
    _require(
        _loopback(host) or host in trusted_private_hosts,
        "base_url host must be loopback or explicitly trusted",
    )
    if parsed.scheme == "http":
        _require(port is not None, "plain HTTP base_url requires an explicit port")
    if backend == "multi-host-private-network":
        _require(not _loopback(host), "multi-host bindings may not use loopback endpoints")
    return value.rstrip("/")


def _load_logical_inputs(
    logical_plan_dir: Path,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    plan = _strict_json(logical_plan_dir / "logical-route-plan.json", "logical plan")
    catalog = _strict_json(
        logical_plan_dir / "logical-service-contracts.json",
        "logical service catalog",
    )
    contracts_value = catalog.get("service_contracts")
    _require(isinstance(contracts_value, list), "logical service contracts are invalid")
    contracts: dict[str, dict[str, Any]] = {}
    for item in contracts_value:
        _require(isinstance(item, dict), "logical service contract must be an object")
        contract_id = _identifier(
            item.get("service_contract_id"),
            "service_contract_id",
        )
        _require(contract_id not in contracts, "logical service contract repeats")
        contracts[contract_id] = dict(item)
    stages: list[dict[str, Any]] = []
    try:
        lines = (logical_plan_dir / "logical-route-stages.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    except OSError as exc:
        raise FullFlowDeploymentError("cannot read logical stages") from exc
    for line in lines:
        _require(bool(line), "logical stage ledger contains a blank line")
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FullFlowDeploymentError("logical stage is invalid") from exc
        _require(isinstance(item, dict), "logical stage must be an object")
        stages.append(item)
    return plan, contracts, stages


def _required_representations(
    contracts: Mapping[str, Mapping[str, Any]],
    stages: Iterable[Mapping[str, Any]],
) -> dict[str, set[str]]:
    required = {contract_id: set() for contract_id in contracts}
    trial_representations: set[str] = set()
    for stage in stages:
        contract_id = stage.get("service_contract_id")
        representation = stage.get("representation_id")
        if isinstance(contract_id, str) and isinstance(representation, str):
            if contract_id in required:
                required[contract_id].add(representation)
        if (
            stage.get("phase") == "execution"
            and isinstance(representation, str)
            and representation in {
                "raw_video",
                "sampled_frame_bundle",
                "multimodal_digest",
            }
        ):
            trial_representations.add(representation)
    if "N6.semantic-inference" in required:
        required["N6.semantic-inference"].update(trial_representations)
    return required


def _validate_runtime_service_bindings(
    value: Any,
    *,
    backend: str,
    trusted_private_hosts: set[str],
    logical_bindings: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Validate the two non-logical W4 coordinator service endpoints.

    These services are runtime companions of the N7/N8 execution contracts;
    they are intentionally not new logical-plan contracts.  Their identities
    and credential-name contract are fixed so a deployment cannot silently
    point a W4 FlowMesh task at the generic semantic-route endpoint.
    """

    rows = _array(value, "runtime_service_bindings")
    _require(
        len(rows) == len(_W4_RUNTIME_CONTRACTS),
        "runtime_service_bindings must contain exactly two W4 coordinators",
    )
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    logical_origins = {
        str(row["base_url"])
        for row in logical_bindings.values()
        if row.get("base_url") is not None
    }
    runtime_origins: set[str] = set()
    for position, raw in enumerate(rows):
        _require(
            isinstance(raw, Mapping),
            f"runtime_service_bindings[{position}] is invalid",
        )
        _require(
            set(raw) == _RUNTIME_SERVICE_FIELDS,
            "runtime service binding fields changed",
        )
        runtime_id = _identifier(
            raw.get("runtime_service_contract_id"),
            "runtime_service_contract_id",
        )
        _require(
            runtime_id in _W4_RUNTIME_CONTRACTS,
            f"unknown runtime service contract: {runtime_id}",
        )
        _require(
            runtime_id not in seen,
            f"duplicate runtime service binding: {runtime_id}",
        )
        seen.add(runtime_id)
        expected = _W4_RUNTIME_CONTRACTS[runtime_id]
        parent_id = _identifier(
            raw.get("parent_service_contract_id"),
            f"{runtime_id}.parent_service_contract_id",
        )
        node_id = _identifier(
            raw.get("logical_node_id"),
            f"{runtime_id}.logical_node_id",
        )
        _require(
            parent_id == expected["parent_service_contract_id"],
            f"{runtime_id} parent service contract changed",
        )
        _require(node_id == expected["logical_node_id"], f"{runtime_id} node changed")
        parent = logical_bindings.get(parent_id)
        _require(parent is not None, f"{runtime_id} parent binding is missing")
        _require(
            parent.get("logical_node_ids") == [node_id],
            f"{runtime_id} parent binding names the wrong node",
        )
        _require(
            raw.get("adapter_id") == _W4_COORDINATOR_ADAPTER_ID,
            f"{runtime_id} adapter_id changed",
        )
        origin = _origin(
            raw.get("base_url"),
            backend=backend,
            trusted_private_hosts=trusted_private_hosts,
        )
        _require(
            origin not in logical_origins,
            f"{runtime_id} must use a dedicated service origin",
        )
        _require(
            origin not in runtime_origins,
            "W4 coordinator runtime service origins must be distinct",
        )
        runtime_origins.add(origin)
        credential_names = _string_array(
            raw.get("credential_env_names"),
            f"{runtime_id}.credential_env_names",
            environment_names=True,
        )
        _require(
            credential_names == list(_W4_COORDINATOR_CREDENTIAL_ENV_NAMES),
            f"{runtime_id} credential environment names changed",
        )
        _require(
            raw.get("persistent_state") is True,
            f"{runtime_id} requires persistent state",
        )
        _require(
            raw.get("health_route") == "/healthz",
            f"{runtime_id} health route changed",
        )
        _require(
            raw.get("health_schema_version")
            == W4_COORDINATOR_HEALTH_SCHEMA_VERSION,
            f"{runtime_id} health schema changed",
        )
        normalized.append({
            "runtime_service_contract_id": runtime_id,
            "parent_service_contract_id": parent_id,
            "logical_node_id": node_id,
            "adapter_id": _W4_COORDINATOR_ADAPTER_ID,
            "base_url": origin,
            "credential_env_names": credential_names,
            "persistent_state": True,
            "health_route": "/healthz",
            "health_schema_version": W4_COORDINATOR_HEALTH_SCHEMA_VERSION,
        })
    _require(
        seen == set(_W4_RUNTIME_CONTRACTS),
        "deployment does not bind both W4 coordinator runtime services",
    )
    _require(
        normalized
        == sorted(
            normalized,
            key=lambda item: item["runtime_service_contract_id"],
        ),
        "runtime service bindings must be sorted by runtime_service_contract_id",
    )
    return normalized


def _validate_source(
    value: Mapping[str, Any],
    contracts: Mapping[str, Mapping[str, Any]],
    stages: list[Mapping[str, Any]],
) -> dict[str, Any]:
    source_schema = value.get("schema_version")
    _require(
        source_schema
        in {
            DEPLOYMENT_SOURCE_SCHEMA_VERSION,
            DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2,
        },
        "unsupported deployment source schema_version",
    )
    source_fields = (
        _SOURCE_FIELDS_V1ALPHA2
        if source_schema == DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2
        else _SOURCE_FIELDS_V1ALPHA1
    )
    _require(set(value) == source_fields, "deployment source fields changed")
    deployment_id = _identifier(value.get("deployment_id"), "deployment_id")
    backend = value.get("backend")
    _require(backend in _BACKENDS, "unsupported deployment backend")
    _require(value.get("credentials_recorded") is False, "source records credentials")
    trusted = _string_array(
        value.get("trusted_private_http_hosts"),
        "trusted_private_http_hosts",
    )
    trusted_hosts = {host.casefold() for host in trusted}
    _require(len(trusted_hosts) == len(trusted), "trusted hosts differ only by case")
    for host in trusted_hosts:
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            _require(
                _SIMULATOR_HOST.fullmatch(host) is not None
                or "." in host,
                "trusted private HTTP host is not a valid deployment host",
            )
        else:
            _require(
                address.is_private
                and not address.is_loopback
                and not address.is_link_local
                and not address.is_multicast
                and not address.is_unspecified,
                "trusted private HTTP address is not a private host",
            )

    network = value.get("network_binding")
    _require(isinstance(network, Mapping), "network_binding must be an object")
    _require(set(network) == _NETWORK_FIELDS, "network_binding fields changed")
    network_normalized = {
        "adapter_id": _identifier(network.get("adapter_id"), "network adapter_id"),
        "mode": network.get("mode"),
        "measurement_class": _identifier(
            network.get("measurement_class"),
            "network measurement_class",
        ),
        "parameters_fitted": network.get("parameters_fitted"),
    }
    _require(network_normalized["mode"] in _NETWORK_MODES, "network mode is invalid")
    _require(
        network_normalized["parameters_fitted"] is False,
        "deployment binding may not fit parameters from evaluation outcomes",
    )
    if backend == "multi-host-private-network":
        _require(
            network_normalized["mode"] != "application-shaped-single-host",
            "multi-host deployment may not claim the single-host shaper",
        )

    required_representations = _required_representations(contracts, stages)
    service_rows = _array(value.get("service_bindings"), "service_bindings")
    normalized_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for position, raw in enumerate(service_rows):
        _require(isinstance(raw, Mapping), f"service_bindings[{position}] is invalid")
        _require(set(raw) == _SERVICE_FIELDS, "service binding fields changed")
        contract_id = _identifier(raw.get("service_contract_id"), "service_contract_id")
        _require(contract_id in contracts, f"unknown service contract: {contract_id}")
        _require(contract_id not in seen, f"duplicate service binding: {contract_id}")
        seen.add(contract_id)
        contract = contracts[contract_id]
        nodes = _string_array(raw.get("logical_node_ids"), "logical_node_ids")
        expected_nodes = sorted(contract.get("logical_node_ids", []))
        _require(nodes == expected_nodes, f"logical nodes changed for {contract_id}")
        actions = _string_array(raw.get("actions"), "actions")
        expected_actions = sorted(contract.get("actions", []))
        _require(actions == expected_actions, f"actions changed for {contract_id}")
        representations = _string_array(
            raw.get("representation_ids"),
            "representation_ids",
        )
        missing_representations = (
            required_representations.get(contract_id, set()) - set(representations)
        )
        _require(
            not missing_representations,
            f"{contract_id} lacks representations {sorted(missing_representations)}",
        )
        token_names = _string_array(
            raw.get("credential_env_names"),
            "credential_env_names",
            environment_names=True,
        )
        base_url = raw.get("base_url")
        role = str(contract.get("role", ""))
        if role == "logical-byte-transfer":
            _require(base_url is None, "network contracts may not contain a service URL")
            _require(not token_names, "network contracts may not name credentials")
            normalized_url = None
        else:
            normalized_url = _origin(
                base_url,
                backend=str(backend),
                trusted_private_hosts=trusted_hosts,
            )
        persistent = raw.get("persistent_state")
        _require(isinstance(persistent, bool), "persistent_state must be boolean")
        if contract.get("state_semantics") in {
            "immutable-hidden-oracle",
            "durable-trial-identity",
            "immutable-content-addressed-artifacts",
            "frozen-index-snapshot",
            "idempotent-content-addressed-output",
            "persistent-with-explicit-cache-scope",
        }:
            _require(persistent, f"{contract_id} requires persistent state")
        normalized_rows.append(
            {
                "service_contract_id": contract_id,
                "adapter_id": _identifier(raw.get("adapter_id"), "adapter_id"),
                "logical_node_ids": nodes,
                "actions": actions,
                "representation_ids": representations,
                "base_url": normalized_url,
                "credential_env_names": token_names,
                "persistent_state": persistent,
            }
        )
    _require(seen == set(contracts), "deployment does not bind every service contract")
    _require(
        normalized_rows == sorted(normalized_rows, key=lambda item: item["service_contract_id"]),
        "service bindings must be sorted by service_contract_id",
    )
    nodes_by_origin: dict[str, set[str]] = {}
    for row in normalized_rows:
        origin = row["base_url"]
        if origin is not None:
            nodes_by_origin.setdefault(origin, set()).update(
                row["logical_node_ids"]
            )
    ambiguous = {
        origin: sorted(nodes)
        for origin, nodes in nodes_by_origin.items()
        if len(nodes) != 1
    }
    _require(
        not ambiguous,
        "each service origin must identify exactly one logical node",
    )
    logical_bindings = {
        row["service_contract_id"]: row for row in normalized_rows
    }
    runtime_bindings = (
        _validate_runtime_service_bindings(
            value.get("runtime_service_bindings"),
            backend=str(backend),
            trusted_private_hosts=trusted_hosts,
            logical_bindings=logical_bindings,
        )
        if source_schema == DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2
        else []
    )
    return {
        "schema_version": source_schema,
        "deployment_id": deployment_id,
        "backend": backend,
        "trusted_private_http_hosts": trusted,
        "network_binding": network_normalized,
        "service_bindings": normalized_rows,
        "runtime_service_bindings": runtime_bindings,
    }


def build_full_flow_deployment_binding(
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    deployment_source: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze one concrete binding after proving complete capabilities."""

    logical_root = Path(logical_plan_dir).resolve()
    logical_report = verify_full_flow_logical_routes(
        logical_root,
        scenario_path,
        container_plan_dir,
    )
    logical_plan, contracts, stages = _load_logical_inputs(logical_root)
    source_path = Path(deployment_source).resolve()
    source = _strict_json(source_path, "deployment source")
    normalized = _validate_source(source, contracts, stages)
    source_bytes = _json_bytes(source)
    v2 = (
        normalized["schema_version"]
        == DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2
    )
    binding: dict[str, Any] = {
        "schema_version": (
            DEPLOYMENT_BINDING_SCHEMA_VERSION_V1ALPHA2
            if v2
            else DEPLOYMENT_BINDING_SCHEMA_VERSION
        ),
        "status": "FROZEN_FULL_FLOW_DEPLOYMENT_BINDING",
        "deployment_id": normalized["deployment_id"],
        "backend": normalized["backend"],
        "logical_plan_sha256": logical_report["plan_sha256"],
        "logical_source_binding_sha256": logical_report["source_binding_sha256"],
        "deployment_source_sha256": _sha256(source_bytes),
        "service_contract_count": len(contracts),
        "bound_service_contract_count": len(normalized["service_bindings"]),
        "service_bindings": normalized["service_bindings"],
        "network_binding": normalized["network_binding"],
        "trusted_private_http_hosts": normalized["trusted_private_http_hosts"],
        "credential_values_included": False,
        "credentials_recorded": False,
        "services_started": False,
        "preflight_performed": False,
        "eligible_for_scientific_claims": False,
    }
    if v2:
        binding.update({
            "runtime_service_binding_count": len(
                normalized["runtime_service_bindings"]
            ),
            "runtime_service_bindings": normalized[
                "runtime_service_bindings"
            ],
            "w4_runtime_bindings_complete": True,
        })
    binding["binding_sha256"] = _sha256(_canonical_bytes(binding))
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"deployment binding already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".full-flow-deployment-", dir=target.parent))
    stage = parent / "binding"
    try:
        stage.mkdir()
        binding_bytes = _json_bytes(binding)
        (stage / DEPLOYMENT_BINDING_NAME).write_bytes(binding_bytes)
        (stage / CHECKSUMS_NAME).write_text(
            f"{_sha256(binding_bytes)}  {DEPLOYMENT_BINDING_NAME}\n",
            encoding="utf-8",
        )
        verify_full_flow_deployment_binding(
            stage,
            logical_plan_dir=logical_root,
            scenario_path=scenario_path,
            container_plan_dir=container_plan_dir,
        )
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return {
        "status": "FROZEN_FULL_FLOW_DEPLOYMENT_BINDING",
        "deployment_id": binding["deployment_id"],
        "backend": binding["backend"],
        "logical_plan_sha256": binding["logical_plan_sha256"],
        "binding_sha256": binding["binding_sha256"],
        "service_contract_count": len(contracts),
        "runtime_service_binding_count": len(
            normalized["runtime_service_bindings"]
        ),
        "w4_runtime_bindings_complete": v2,
        "pre_upcloud_deployment_ready": False,
        "output_dir": str(target),
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_full_flow_deployment_binding(
    binding_dir: str | Path,
    *,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    """Verify the binding and re-derive every required capability."""

    root = Path(binding_dir).resolve()
    _require(root.is_dir(), "deployment binding directory does not exist")
    _require(
        {path.name for path in root.iterdir()} == {
            DEPLOYMENT_BINDING_NAME,
            CHECKSUMS_NAME,
        },
        "deployment binding file set changed",
    )
    binding_bytes = (root / DEPLOYMENT_BINDING_NAME).read_bytes()
    expected_line = f"{_sha256(binding_bytes)}  {DEPLOYMENT_BINDING_NAME}\n"
    _require(
        (root / CHECKSUMS_NAME).read_text(encoding="utf-8") == expected_line,
        "deployment binding checksum failed",
    )
    binding = _strict_json(root / DEPLOYMENT_BINDING_NAME, "deployment binding")
    common_fields = {
        "schema_version",
        "status",
        "deployment_id",
        "backend",
        "logical_plan_sha256",
        "logical_source_binding_sha256",
        "deployment_source_sha256",
        "service_contract_count",
        "bound_service_contract_count",
        "service_bindings",
        "network_binding",
        "trusted_private_http_hosts",
        "credential_values_included",
        "credentials_recorded",
        "services_started",
        "preflight_performed",
        "eligible_for_scientific_claims",
        "binding_sha256",
    }
    binding_schema = binding.get("schema_version")
    v2 = binding_schema == DEPLOYMENT_BINDING_SCHEMA_VERSION_V1ALPHA2
    required_fields = set(common_fields)
    if v2:
        required_fields.update({
            "runtime_service_binding_count",
            "runtime_service_bindings",
            "w4_runtime_bindings_complete",
        })
    _require(set(binding) == required_fields, "deployment binding fields changed")
    _require(
        binding_schema
        in {
            DEPLOYMENT_BINDING_SCHEMA_VERSION,
            DEPLOYMENT_BINDING_SCHEMA_VERSION_V1ALPHA2,
        }
        and binding.get("status") == "FROZEN_FULL_FLOW_DEPLOYMENT_BINDING",
        "deployment binding schema or status changed",
    )
    recorded_sha = _digest(binding.pop("binding_sha256", None), "binding_sha256")
    _require(recorded_sha == _sha256(_canonical_bytes(binding)), "binding_sha256 failed")
    binding["binding_sha256"] = recorded_sha
    _require(
        binding.get("credential_values_included") is False
        and binding.get("credentials_recorded") is False
        and binding.get("services_started") is False
        and binding.get("preflight_performed") is False
        and binding.get("eligible_for_scientific_claims") is False,
        "deployment safety classification changed",
    )
    logical_root = Path(logical_plan_dir).resolve()
    logical_report = verify_full_flow_logical_routes(
        logical_root,
        scenario_path,
        container_plan_dir,
    )
    _require(
        binding.get("logical_plan_sha256") == logical_report["plan_sha256"]
        and binding.get("logical_source_binding_sha256")
        == logical_report["source_binding_sha256"],
        "deployment binding names a different logical plan",
    )
    _logical_plan, contracts, stages = _load_logical_inputs(logical_root)
    source_shape = {
        "schema_version": (
            DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2
            if v2
            else DEPLOYMENT_SOURCE_SCHEMA_VERSION
        ),
        "deployment_id": binding.get("deployment_id"),
        "backend": binding.get("backend"),
        "service_bindings": binding.get("service_bindings"),
        "network_binding": binding.get("network_binding"),
        "trusted_private_http_hosts": binding.get("trusted_private_http_hosts"),
        "credentials_recorded": False,
    }
    if v2:
        source_shape["runtime_service_bindings"] = binding.get(
            "runtime_service_bindings"
        )
    normalized = _validate_source(source_shape, contracts, stages)
    _require(
        binding.get("service_contract_count") == len(contracts)
        and binding.get("bound_service_contract_count")
        == len(normalized["service_bindings"]),
        "service contract count changed",
    )
    _digest(binding.get("deployment_source_sha256"), "deployment_source_sha256")
    if v2:
        _require(
            binding.get("runtime_service_binding_count")
            == len(normalized["runtime_service_bindings"])
            == len(_W4_RUNTIME_CONTRACTS),
            "runtime service binding count changed",
        )
        _require(
            binding.get("w4_runtime_bindings_complete") is True,
            "W4 runtime binding completeness changed",
        )
    return {
        "status": "VERIFIED",
        "legacy_schema": not v2,
        "deployment_schema_version": binding_schema,
        "deployment_id": binding["deployment_id"],
        "backend": binding["backend"],
        "logical_plan_sha256": binding["logical_plan_sha256"],
        "binding_sha256": binding["binding_sha256"],
        "service_contract_count": len(contracts),
        "runtime_service_binding_count": len(
            normalized["runtime_service_bindings"]
        ),
        "runtime_service_bindings": normalized["runtime_service_bindings"],
        "w4_runtime_bindings_complete": v2,
        "pre_upcloud_deployment_schema_ready": v2,
        "capability_coverage_complete": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def preflight_full_flow_deployment(
    binding_dir: str | Path,
    *,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    """Read-only health probe of each distinct bound service origin."""

    _require(
        isinstance(timeout_seconds, (int, float))
        and not isinstance(timeout_seconds, bool)
        and math.isfinite(float(timeout_seconds))
        and float(timeout_seconds) > 0,
        "timeout_seconds must be positive",
    )
    verified = verify_full_flow_deployment_binding(
        binding_dir,
        logical_plan_dir=logical_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    root = Path(binding_dir).resolve()
    binding = _strict_json(root / DEPLOYMENT_BINDING_NAME, "deployment binding")
    origins: dict[str, dict[str, Any]] = {}
    for row in binding["service_bindings"]:
        origin = row["base_url"]
        if origin is not None:
            node_ids = set(row["logical_node_ids"])
            observed = origins.setdefault(origin, {
                "kind": "logical-service",
                "logical_node_ids": set(),
                "health_route": "/healthz",
                "runtime_service_contract_id": None,
                "health_schema_version": None,
            })
            _require(
                observed["kind"] == "logical-service",
                "runtime and logical service origins collide",
            )
            observed["logical_node_ids"].update(node_ids)
    for row in binding.get("runtime_service_bindings", []):
        origin = row["base_url"]
        _require(origin not in origins, "runtime service origins collide")
        origins[origin] = {
            "kind": "w4-runtime-service",
            "logical_node_ids": {row["logical_node_id"]},
            "health_route": row["health_route"],
            "runtime_service_contract_id": row[
                "runtime_service_contract_id"
            ],
            "health_schema_version": row["health_schema_version"],
        }
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _RejectRedirects(),
    )
    observations: list[dict[str, Any]] = []
    for origin in sorted(origins):
        expectation = origins[origin]
        request = urllib.request.Request(
            origin + str(expectation["health_route"]),
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
            method="GET",
        )
        try:
            with opener.open(request, timeout=float(timeout_seconds)) as response:
                _require(response.status == 200, "health endpoint returned non-200")
                _require(
                    response.headers.get_content_type() == "application/json",
                    "health endpoint did not return JSON",
                )
                raw = response.read(_MAX_HEALTH_BYTES + 1)
                _require(len(raw) <= _MAX_HEALTH_BYTES, "health response is too large")
        except (urllib.error.URLError, TimeoutError) as exc:
            raise FullFlowDeploymentError("deployment service health probe failed") from exc
        try:
            health = json.loads(raw)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise FullFlowDeploymentError("health response is invalid JSON") from exc
        _require(isinstance(health, dict), "health response must be an object")
        expected_nodes = expectation["logical_node_ids"]
        expected_node = next(iter(expected_nodes))
        if expectation["kind"] == "w4-runtime-service":
            _require(health.get("status") == "ok", "W4 coordinator is not healthy")
            _require(
                health.get("schema_version")
                == expectation["health_schema_version"],
                "W4 coordinator health schema changed",
            )
            _require(
                health.get("node_id") == expected_node
                and health.get("coordinator_node_id") == expected_node,
                "W4 coordinator health endpoint reports the wrong node",
            )
            _require(
                health.get("runtime_service_contract_id")
                == expectation["runtime_service_contract_id"],
                "W4 coordinator health endpoint reports the wrong runtime service",
            )
        else:
            _require(
                health.get("status") in {"ok", "healthy"},
                "service is not healthy",
            )
        _require(
            health.get("credentials_recorded") is False,
            "health response does not attest credential safety",
        )
        claimed_node = health.get("node_id", health.get("logical_node_id"))
        _require(
            len(expected_nodes) == 1
            and claimed_node == expected_node,
            "health endpoint must report its exact logical node",
        )
        observations.append(
            {
                "origin_sha256": _sha256(origin.encode("utf-8")),
                "logical_node_ids": sorted(expected_nodes),
                "service_kind": expectation["kind"],
                "runtime_service_contract_id": expectation[
                    "runtime_service_contract_id"
                ],
                "health_sha256": _sha256(_canonical_bytes(health)),
                "status": "healthy",
            }
        )
    runtime_preflight_complete = (
        verified["w4_runtime_bindings_complete"]
        and sum(
            row["service_kind"] == "w4-runtime-service"
            for row in observations
        )
        == 2
    )
    return {
        "status": "READY",
        "deployment_id": verified["deployment_id"],
        "binding_sha256": verified["binding_sha256"],
        "distinct_service_origin_count": len(observations),
        "runtime_service_origin_count": sum(
            row["service_kind"] == "w4-runtime-service"
            for row in observations
        ),
        "w4_runtime_preflight_complete": runtime_preflight_complete,
        "pre_upcloud_deployment_ready": runtime_preflight_complete,
        "service_origins": observations,
        "read_only_probe": True,
        "services_started": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "DEPLOYMENT_BINDING_SCHEMA_VERSION",
    "DEPLOYMENT_BINDING_SCHEMA_VERSION_V1ALPHA2",
    "DEPLOYMENT_SOURCE_SCHEMA_VERSION",
    "DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2",
    "FullFlowDeploymentError",
    "W4_COORDINATOR_HEALTH_SCHEMA_VERSION",
    "build_full_flow_deployment_binding",
    "full_flow_w4_runtime_service_binding_requirements",
    "preflight_full_flow_deployment",
    "verify_full_flow_deployment_binding",
]
