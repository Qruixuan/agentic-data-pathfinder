"""Endpoint-free startup contracts for the N1--N8 full-flow topology.

The logical route compiler says *what* must happen and the deployment binding
says *where* a service lives.  This module records the missing middle layer:
which Pathfinder process (or orchestration component) implements each logical
service contract, which runtime-only inputs it needs, and whether the contract
is independently deployable today.

The resulting package is deliberately not a Docker Compose file.  It contains
no image, host, port, URL, filesystem path, or credential value.  The same
startup contracts can therefore be rendered into a single-host Compose
deployment or a private multi-host deployment without changing the logical
plan.  Contracts that currently exist only as in-process library adapters are
reported as gaps instead of being represented by a fictional service.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from .full_flow_logical_routes import (
    SERVICE_CATALOG_NAME,
    verify_full_flow_logical_routes,
)


SERVICE_BOOTSTRAP_SCHEMA_VERSION = (
    "pathfinder.full-flow-service-bootstrap/v1alpha2"
)
SERVICE_LAUNCHER_SCHEMA_VERSION = (
    "pathfinder.full-flow-service-launcher/v1alpha2"
)
BOOTSTRAP_NAME = "full-flow-local-service-bootstrap.json"
LAUNCHERS_NAME = "full-flow-local-service-launchers.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")
_ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")


class FullFlowServiceBootstrapError(ValueError):
    """Raised when a startup contract is unsafe or no longer reproducible."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowServiceBootstrapError(message)


def _json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowServiceBootstrapError(
            "service bootstrap contains invalid JSON"
        ) from exc


def _jsonl_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    try:
        return b"".join(
            (
                json.dumps(
                    row,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
            for row in rows
        )
    except (TypeError, ValueError) as exc:
        raise FullFlowServiceBootstrapError(
            "service launcher contains invalid JSON"
        ) from exc


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowServiceBootstrapError(
            "service bootstrap is not canonical JSON"
        ) from exc


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _parse_json(payload: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(
                FullFlowServiceBootstrapError(
                    f"{label} contains non-finite number {item}"
                )
            ),
        )
    except FullFlowServiceBootstrapError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowServiceBootstrapError(
            f"cannot parse {label}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_json(path: Path, label: str) -> dict[str, Any]:
    _require(
        path.is_file() and not path.is_symlink(),
        f"{label} is not a regular file",
    )
    try:
        return _parse_json(path.read_bytes(), label)
    except OSError as exc:
        raise FullFlowServiceBootstrapError(
            f"cannot read {label}"
        ) from exc


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    _require(
        path.is_file() and not path.is_symlink(),
        f"{label} is not a regular file",
    )
    try:
        lines = path.read_bytes().splitlines()
    except OSError as exc:
        raise FullFlowServiceBootstrapError(
            f"cannot read {label}"
        ) from exc
    _require(bool(lines), f"{label} cannot be empty")
    return [
        _parse_json(line, f"{label} line {index}")
        for index, line in enumerate(lines, start=1)
    ]


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return value


def _strings(value: Any, label: str) -> list[str]:
    _require(
        isinstance(value, list)
        and all(isinstance(item, str) and item for item in value),
        f"{label} must be an array of strings",
    )
    _require(len(value) == len(set(value)), f"{label} must be unique")
    return list(value)


def _assert_runtime_values_absent(value: Any) -> None:
    """Reject concrete endpoints, host paths, and credential-value fields."""

    forbidden_keys = {
        "api_key",
        "authorization",
        "bearer_token",
        "credential_value",
        "endpoint",
        "host",
        "host_path",
        "password",
        "port",
        "secret",
        "token",
        "url",
    }

    def visit(item: Any) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                _require(
                    str(key).casefold() not in forbidden_keys,
                    "service bootstrap contains a runtime binding field",
                )
                visit(child)
            return
        if isinstance(item, list):
            for child in item:
                visit(child)
            return
        if isinstance(item, str):
            lowered = item.casefold()
            _require(
                "://" not in item
                and not item.startswith(("~", "\\\\"))
                and _WINDOWS_ABSOLUTE.match(item) is None
                and "bearer " not in lowered,
                "service bootstrap contains a concrete endpoint, path, or "
                "credential",
            )
            return
        if isinstance(item, float):
            _require(
                math.isfinite(item),
                "service bootstrap contains a non-finite number",
            )

    visit(value)


def _argv(*values: str) -> list[str]:
    return list(values)


def _w4_flowmesh_coordinator_companion(node: str) -> dict[str, Any]:
    """Return the endpoint-free N7/N8 W4 coordinator process contract."""

    _require(node in {"N7", "N8"}, "W4 coordinator node is invalid")
    route_package = "PATHFINDER_FULL_FLOW_W4_ROUTE_PACKAGE_DIR"
    crosswalk = "PATHFINDER_FULL_FLOW_W4_CROSSWALK_DIR"
    scratch = f"PATHFINDER_{node}_W4_RAW_SAMPLER_SCRATCH_DIR"
    state_db = f"PATHFINDER_{node}_W4_COORDINATOR_STATE_DB"
    listen_port = f"PATHFINDER_{node}_W4_COORDINATOR_LISTEN_PORT"
    artifact_names = [
        route_package,
        crosswalk,
        "PATHFINDER_N2_PACKAGE_DIR",
        "PATHFINDER_N7_PACKAGE_DIR",
        "PATHFINDER_N8_PACKAGE_DIR",
    ]
    credential_names = [
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
    ]
    configuration_names = [
        "PATHFINDER_FULL_FLOW_SERVICE_LISTEN_HOST",
        "PATHFINDER_N2_INDEX_BASE_URL",
        "PATHFINDER_N3_DATA_AGENT_BASE_URL",
        "PATHFINDER_N3_DATA_AGENT_LOCATION",
        "PATHFINDER_N4_DATA_AGENT_BASE_URL",
        "PATHFINDER_N4_DATA_AGENT_LOCATION",
        "PATHFINDER_N6_SEMANTIC_BASE_URL",
        "PATHFINDER_N7_W4_CACHE_BASE_URL",
        "PATHFINDER_N7_W4_CACHE_ID",
        "PATHFINDER_N7_INDEX_BASE_URL",
        "PATHFINDER_N8_W4_CACHE_BASE_URL",
        "PATHFINDER_N8_W4_CACHE_ID",
        "PATHFINDER_N8_INDEX_BASE_URL",
        "PATHFINDER_SEMANTIC_ROUTE_MODEL",
        "PATHFINDER_SEMANTIC_ROUTE_PRIVATE_HTTP_HOSTS",
        "PATHFINDER_SEMANTIC_ROUTE_TIMEOUT_SECONDS",
        "PATHFINDER_SEMANTIC_ROUTE_MAX_ARTIFACT_BYTES",
        listen_port,
        scratch,
        state_db,
    ]
    return {
        "implementation_id": (
            "pathfinder.simulator.full_flow_w4_flowmesh_service"
        ),
        "runtime_service_contract_id": (
            f"{node}.w4-candidate-coordinator"
        ),
        "entrypoint_argv_template": _argv(
            "python",
            "-m",
            "pathfinder",
            "serve-simulator-full-flow-w4-flowmesh-coordinator",
            "--coordinator-node-id",
            node,
            "--route-package-dir",
            f"${{{route_package}}}",
            "--crosswalk-dir",
            f"${{{crosswalk}}}",
            "--n2-index-package-dir",
            "${PATHFINDER_N2_PACKAGE_DIR}",
            "--n7-index-package-dir",
            "${PATHFINDER_N7_PACKAGE_DIR}",
            "--n8-index-package-dir",
            "${PATHFINDER_N8_PACKAGE_DIR}",
            "--n2-index-base-url",
            "${PATHFINDER_N2_INDEX_BASE_URL}",
            "--n7-index-base-url",
            "${PATHFINDER_N7_INDEX_BASE_URL}",
            "--n8-index-base-url",
            "${PATHFINDER_N8_INDEX_BASE_URL}",
            "--n3-data-agent-base-url",
            "${PATHFINDER_N3_DATA_AGENT_BASE_URL}",
            "--n4-data-agent-base-url",
            "${PATHFINDER_N4_DATA_AGENT_BASE_URL}",
            "--n3-data-agent-location",
            "${PATHFINDER_N3_DATA_AGENT_LOCATION}",
            "--n4-data-agent-location",
            "${PATHFINDER_N4_DATA_AGENT_LOCATION}",
            "--n7-cache-base-url",
            "${PATHFINDER_N7_W4_CACHE_BASE_URL}",
            "--n7-cache-id",
            "${PATHFINDER_N7_W4_CACHE_ID}",
            "--n8-cache-base-url",
            "${PATHFINDER_N8_W4_CACHE_BASE_URL}",
            "--n8-cache-id",
            "${PATHFINDER_N8_W4_CACHE_ID}",
            "--n6-base-url",
            "${PATHFINDER_N6_SEMANTIC_BASE_URL}",
            "--semantic-model",
            "${PATHFINDER_SEMANTIC_ROUTE_MODEL}",
            "--raw-sampler-scratch-dir",
            f"${{{scratch}}}",
            "--state-db",
            f"${{{state_db}}}",
            "--host",
            "${PATHFINDER_FULL_FLOW_SERVICE_LISTEN_HOST}",
            "--port",
            f"${{{listen_port}}}",
            "--timeout-seconds",
            "${PATHFINDER_SEMANTIC_ROUTE_TIMEOUT_SECONDS}",
            "--max-artifact-bytes",
            "${PATHFINDER_SEMANTIC_ROUTE_MAX_ARTIFACT_BYTES}",
            "--simulator-private-http-hosts",
            "${PATHFINDER_SEMANTIC_ROUTE_PRIVATE_HTTP_HOSTS}",
        ),
        "artifact_binding_env_names": sorted(artifact_names),
        "credential_env_names": sorted(credential_names),
        "configuration_env_names": sorted(configuration_names),
        "health_route": "/healthz",
        "persistent_state_required": True,
        "state_semantics": "durable-trial-identity-with-ephemeral-scratch",
        "ephemeral_state_env_names": [scratch],
        "depends_on_runtime_service_contract_ids": sorted([
            "N2.global-index",
            "N3.raw-data-agent",
            "N4.derived-data-agent",
            "N6.semantic-inference",
            "N7.local-index",
            "N8.local-index",
            f"{node}.w4-candidate-cache",
        ]),
    }


def _w4_cache_companion(node: str) -> dict[str, Any]:
    """Return the cache namespace dedicated to one W4 coordinator."""

    _require(node in {"N7", "N8"}, "W4 cache node is invalid")
    token = f"PATHFINDER_{node}_W4_CACHE_TOKEN"
    cache_id = f"PATHFINDER_{node}_W4_CACHE_ID"
    state_dir = f"PATHFINDER_{node}_W4_CACHE_STATE_DIR"
    capacity = f"PATHFINDER_{node}_W4_CACHE_CAPACITY_BYTES"
    max_artifact = f"PATHFINDER_{node}_W4_CACHE_MAX_ARTIFACT_BYTES"
    listen_port = f"PATHFINDER_{node}_W4_CACHE_LISTEN_PORT"
    return {
        "implementation_id": "pathfinder.simulator.full_flow_cache",
        "runtime_service_contract_id": f"{node}.w4-candidate-cache",
        "entrypoint_argv_template": _argv(
            "python",
            "-m",
            "pathfinder",
            "serve-simulator-full-flow-cache",
            "--node-id",
            node,
            "--cache-id",
            f"${{{cache_id}}}",
            "--state-dir",
            f"${{{state_dir}}}",
            "--capacity-bytes",
            f"${{{capacity}}}",
            "--max-artifact-bytes",
            f"${{{max_artifact}}}",
            "--token-env-name",
            token,
            "--fallback-token-env-name",
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            "--port",
            f"${{{listen_port}}}",
        ),
        "artifact_binding_env_names": [],
        "credential_env_names": [
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            token,
        ],
        "configuration_env_names": sorted(
            [cache_id, state_dir, capacity, max_artifact, listen_port]
        ),
        "health_route": "/healthz",
        "persistent_state_required": True,
        "state_semantics": "exclusive-w4-cache-namespace",
    }


def _launcher(
    contract: Mapping[str, Any],
    *,
    implementation_kind: str,
    implementation_id: str,
    argv_template: Sequence[str] = (),
    artifact_binding_names: Sequence[str] = (),
    credential_env_names: Sequence[str] = (),
    configuration_env_names: Sequence[str] = (),
    health_route: str | None = None,
    independently_startable: bool,
    supported_actions: Sequence[str] | None = None,
    missing_actions: Sequence[str] = (),
    migration_unit: str,
    limitations: Sequence[str] = (),
    companion_processes: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    contract_id = _identifier(
        contract.get("service_contract_id"),
        "service_contract_id",
    )
    actions = _strings(contract.get("actions"), f"{contract_id}.actions")
    supported = sorted(actions if supported_actions is None else supported_actions)
    missing = sorted(missing_actions)
    _require(
        set(supported).isdisjoint(missing)
        and set(supported) | set(missing) == set(actions),
        f"{contract_id} action support does not cover the contract exactly",
    )
    credentials = sorted(credential_env_names)
    configuration = sorted(configuration_env_names)
    artifacts = sorted(artifact_binding_names)
    _require(
        all(_ENV_NAME.fullmatch(name) for name in credentials),
        f"{contract_id} has an invalid credential environment name",
    )
    _require(
        all(_ENV_NAME.fullmatch(name) for name in configuration + artifacts),
        f"{contract_id} has an invalid runtime binding name",
    )
    return {
        "schema_version": SERVICE_LAUNCHER_SCHEMA_VERSION,
        "service_contract_id": contract_id,
        "logical_node_ids": _strings(
            contract.get("logical_node_ids"),
            f"{contract_id}.logical_node_ids",
        ),
        "contract_role": _identifier(
            contract.get("role"),
            f"{contract_id}.role",
        ),
        "implementation_kind": implementation_kind,
        "implementation_id": implementation_id,
        "entrypoint_argv_template": list(argv_template),
        "companion_processes": [dict(row) for row in companion_processes],
        "artifact_binding_env_names": artifacts,
        "credential_env_names": credentials,
        "configuration_env_names": configuration,
        "health_route": health_route,
        "independently_startable": independently_startable,
        "supported_actions": supported,
        "missing_actions": missing,
        "contract_complete": not missing,
        "migration_unit": migration_unit,
        "host_rebindable": True,
        "limitations": list(limitations),
        "credential_values_included": False,
        "concrete_endpoint_included": False,
    }


def _direct_launcher(contract: Mapping[str, Any]) -> dict[str, Any] | None:
    contract_id = str(contract.get("service_contract_id"))
    node = str(contract_id).split(".", 1)[0]
    package_name = f"PATHFINDER_{node}_PACKAGE_DIR"
    state_name = f"PATHFINDER_{node}_STATE_DIR"
    listen_name = f"PATHFINDER_{node}_LISTEN_PORT"

    if contract_id == "N1.hidden-score":
        return _launcher(
            contract,
            implementation_kind="multi-process-http-service-group",
            implementation_id="pathfinder.simulator.hidden_oracle",
            argv_template=_argv(
                "python",
                "-m",
                "pathfinder",
                "serve-simulator-n1-hidden-oracle",
                "--package-dir",
                "${PATHFINDER_N1_PACKAGE_DIR}",
                "--state-db",
                "${PATHFINDER_N1_STATE_DB}",
                "--port",
                "${PATHFINDER_N1_LISTEN_PORT}",
            ),
            artifact_binding_names=("PATHFINDER_N1_PACKAGE_DIR",),
            credential_env_names=(
                "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET",
                "PATHFINDER_N1_ORACLE_TOKEN",
                "PATHFINDER_N1_VERIFICATION_TOKEN",
            ),
            configuration_env_names=(
                "PATHFINDER_N1_LISTEN_PORT",
                "PATHFINDER_N1_STATE_DB",
                "PATHFINDER_N1_VERIFICATION_LISTEN_PORT",
                "PATHFINDER_N1_VERIFICATION_STATE_DB",
            ),
            health_route="/healthz",
            independently_startable=True,
            migration_unit="N1-hidden-oracle-service",
            limitations=(
                "Hidden scoring and remote evidence verification are "
                "separate authenticated N1 processes sharing only the "
                "immutable oracle package and N1-owned evidence secret.",
            ),
            companion_processes=({
                "implementation_id": (
                    "pathfinder.simulator.full_flow_n1_remote_verification"
                ),
                "entrypoint_argv_template": _argv(
                    "python",
                    "-m",
                    "pathfinder",
                    "serve-simulator-n1-remote-verifier",
                    "--package-dir",
                    "${PATHFINDER_N1_PACKAGE_DIR}",
                    "--state-db",
                    "${PATHFINDER_N1_VERIFICATION_STATE_DB}",
                    "--port",
                    "${PATHFINDER_N1_VERIFICATION_LISTEN_PORT}",
                ),
                "health_route": "/healthz",
            },),
        )
    if contract_id in {
        "N2.global-index",
        "N7.local-index",
        "N8.local-index",
    }:
        return _launcher(
            contract,
            implementation_kind="standalone-http-service",
            implementation_id="pathfinder.simulator.index_service",
            argv_template=_argv(
                "python",
                "-m",
                "pathfinder",
                "serve-simulator-n2-index",
                "--package-dir",
                f"${{{package_name}}}",
                "--node-id",
                node,
                "--port",
                f"${{{listen_name}}}",
                "--require-token",
            ),
            artifact_binding_names=(package_name,),
            credential_env_names=("PATHFINDER_N2_INDEX_TOKEN",),
            configuration_env_names=(listen_name,),
            health_route="/healthz",
            independently_startable=True,
            migration_unit=f"{node}-index-service",
        )
    if contract_id in {"N3.raw-data-agent", "N4.derived-data-agent"}:
        actions = _strings(contract.get("actions"), f"{contract_id}.actions")
        manifest_name = f"PATHFINDER_{node}_DATA_AGENT_MANIFEST"
        operation_db_name = f"PATHFINDER_{node}_OPERATION_DB"
        companion_processes: tuple[Mapping[str, Any], ...] = ()
        if contract_id == "N4.derived-data-agent":
            companion_processes = ({
                "implementation_id": (
                    "pathfinder.simulator.n4_publication_http"
                ),
                "entrypoint_argv_template": _argv(
                    "python",
                    "-m",
                    "pathfinder.simulator.n4_publication_http",
                    "--store-root",
                    "${PATHFINDER_N4_PUBLICATION_STORE}",
                    "--port",
                    "${PATHFINDER_N4_PUBLICATION_LISTEN_PORT}",
                ),
                "health_route": "/healthz",
            },)
        return _launcher(
            contract,
            implementation_kind=(
                "multi-process-http-service-group"
                if companion_processes
                else "standalone-http-service"
            ),
            implementation_id="pathfinder.data_agent_server",
            argv_template=_argv(
                "python",
                "-m",
                "pathfinder",
                "serve-data-agent",
                "--manifest",
                f"${{{manifest_name}}}",
                "--operation-db",
                f"${{{operation_db_name}}}",
                "--port",
                f"${{{listen_name}}}",
                "--public-base-url",
                f"${{PATHFINDER_{node}_PUBLIC_BASE_URL}}",
                "--require-token",
                "--require-artifact-secret",
            ),
            artifact_binding_names=(manifest_name,),
            credential_env_names=tuple(
                sorted({
                    "PATHFINDER_DATA_AGENT_ARTIFACT_SECRET",
                    "PATHFINDER_DATA_AGENT_TOKEN",
                    *(
                        {"PATHFINDER_N4_PUBLICATION_TOKEN"}
                        if companion_processes
                        else set()
                    ),
                })
            ),
            configuration_env_names=tuple(
                sorted({
                    listen_name,
                    f"PATHFINDER_{node}_PUBLIC_BASE_URL",
                    operation_db_name,
                    *(
                        {
                            "PATHFINDER_N4_PUBLICATION_LISTEN_PORT",
                            "PATHFINDER_N4_PUBLICATION_STORE",
                        }
                        if companion_processes
                        else set()
                    ),
                })
            ),
            health_route="/healthz",
            independently_startable=True,
            migration_unit=f"{node}-data-agent-service",
            limitations=(
                (
                    "N4 publication is a separate authenticated companion; "
                    "the standard Data Agent is rebound to the committed "
                    "immutable generation between provisioning and trials."
                ),
            )
            if companion_processes
            else (),
            companion_processes=companion_processes,
        )
    if contract_id == "N5.materializer":
        return _launcher(
            contract,
            implementation_kind="multi-process-http-service-group",
            implementation_id="pathfinder.simulator.n5_materialization",
            argv_template=_argv(
                "python",
                "-m",
                "pathfinder",
                "serve-simulator-n5-materializer",
                "--state-dir",
                "${PATHFINDER_N5_STATE_DIR}",
                "--port",
                "${PATHFINDER_N5_LISTEN_PORT}",
            ),
            credential_env_names=(
                "PATHFINDER_N5_MATERIALIZATION_TOKEN",
                "PATHFINDER_N5_DIGEST_LLM_API_KEY",
                "PATHFINDER_N5_DIGEST_TOKEN",
            ),
            artifact_binding_names=("PATHFINDER_N5_DIGEST_PLAN_DIR",),
            configuration_env_names=(
                "PATHFINDER_N5_DIGEST_LISTEN_PORT",
                "PATHFINDER_N5_DIGEST_LLM_BASE_URL",
                "PATHFINDER_N5_DIGEST_LLM_MODEL",
                "PATHFINDER_N5_DIGEST_PLAN_DIR",
                "PATHFINDER_N5_LISTEN_PORT",
                "PATHFINDER_N5_STATE_DIR",
            ),
            health_route="/healthz",
            independently_startable=True,
            migration_unit="N5-materialization-service",
            limitations=(
                "Frame bundles and semantic digests use separate authenticated "
                "HTTP processes under the same N5 logical role.",
            ),
            companion_processes=({
                "implementation_id": (
                    "pathfinder.simulator.n5_digest_http"
                ),
                "entrypoint_argv_template": _argv(
                    "python",
                    "-m",
                    "pathfinder.simulator.n5_digest_http",
                    "--state-dir",
                    "${PATHFINDER_N5_STATE_DIR}",
                    "--plan-dir",
                    "${PATHFINDER_N5_DIGEST_PLAN_DIR}",
                    "--port",
                    "${PATHFINDER_N5_DIGEST_LISTEN_PORT}",
                ),
                "health_route": "/healthz",
            },),
        )
    if contract_id == "N6.semantic-inference":
        return _launcher(
            contract,
            implementation_kind="standalone-http-service",
            implementation_id="pathfinder.simulator.container_node",
            argv_template=_argv(
                "python",
                "-m",
                "pathfinder",
                "serve-container-node",
                "--node-id",
                "N6",
                "--state-dir",
                "${PATHFINDER_N6_STATE_DIR}",
                "--port",
                "${PATHFINDER_N6_LISTEN_PORT}",
                "--enable-semantic-llm",
            ),
            credential_env_names=(
                "PATHFINDER_CONTAINER_NODE_TOKEN",
                "PATHFINDER_SEMANTIC_LLM_API_KEY",
            ),
            configuration_env_names=(
                "PATHFINDER_N6_LISTEN_PORT",
                "PATHFINDER_N6_STATE_DIR",
                "PATHFINDER_SEMANTIC_LLM_BASE_URL",
                "PATHFINDER_SEMANTIC_LLM_MODEL",
            ),
            health_route="/healthz",
            independently_startable=True,
            migration_unit="N6-semantic-inference-service",
        )
    if contract_id in {"N7.execution-compute", "N8.execution-compute"}:
        artifact_names = (
            "PATHFINDER_FULL_FLOW_ARTIFACT_BINDING_DIR",
            "PATHFINDER_FULL_FLOW_EXACT_RANGE_CATALOG_DIR",
            "PATHFINDER_FULL_FLOW_INDEX_QUERY_PLAN_CATALOG_DIR",
            "PATHFINDER_FULL_FLOW_PROVISIONING_CATALOG_DIR",
            "PATHFINDER_FULL_FLOW_W4_CROSSWALK_DIR",
            "PATHFINDER_FULL_FLOW_W4_ROUTE_PACKAGE_DIR",
            "PATHFINDER_LOCAL_SEMANTIC_ADMISSION_DIR",
            "PATHFINDER_N1_PUBLIC_COMMITMENT_DIR",
            "PATHFINDER_N2_PACKAGE_DIR",
            "PATHFINDER_N3_PACKAGE_DIR",
            "PATHFINDER_N4_PACKAGE_DIR",
            "PATHFINDER_N7_PACKAGE_DIR",
            "PATHFINDER_N8_PACKAGE_DIR",
        )
        endpoint_names = (
            "PATHFINDER_N1_ORACLE_BASE_URL",
            "PATHFINDER_N1_VERIFICATION_BASE_URL",
            "PATHFINDER_N2_INDEX_BASE_URL",
            "PATHFINDER_N3_DATA_AGENT_BASE_URL",
            "PATHFINDER_N4_DATA_AGENT_BASE_URL",
            "PATHFINDER_N6_SEMANTIC_BASE_URL",
            "PATHFINDER_N7_CACHE_BASE_URL",
            "PATHFINDER_N7_INDEX_BASE_URL",
            "PATHFINDER_N7_NODE_HEALTH_BASE_URL",
            "PATHFINDER_N8_CACHE_BASE_URL",
            "PATHFINDER_N8_INDEX_BASE_URL",
            "PATHFINDER_N8_NODE_HEALTH_BASE_URL",
        )
        option_bindings = (
            ("--local-semantic-admission-dir", artifact_names[6]),
            ("--n1-public-commitment-dir", artifact_names[7]),
            ("--artifact-binding-dir", artifact_names[0]),
            ("--n2-index-package-dir", artifact_names[8]),
            ("--n3-package-dir", artifact_names[9]),
            ("--n4-package-dir", artifact_names[10]),
            ("--exact-range-catalog-dir", artifact_names[1]),
            ("--provisioning-catalog-dir", artifact_names[3]),
            ("--index-query-plan-catalog-dir", artifact_names[2]),
            ("--state-dir", f"PATHFINDER_{node}_ROUTE_STATE_DIR"),
            ("--n2-index-base-url", "PATHFINDER_N2_INDEX_BASE_URL"),
            ("--n7-index-base-url", "PATHFINDER_N7_INDEX_BASE_URL"),
            ("--n8-index-base-url", "PATHFINDER_N8_INDEX_BASE_URL"),
            (
                "--n3-data-agent-base-url",
                "PATHFINDER_N3_DATA_AGENT_BASE_URL",
            ),
            (
                "--n4-data-agent-base-url",
                "PATHFINDER_N4_DATA_AGENT_BASE_URL",
            ),
            ("--n7-cache-base-url", "PATHFINDER_N7_CACHE_BASE_URL"),
            ("--n8-cache-base-url", "PATHFINDER_N8_CACHE_BASE_URL"),
            ("--n7-cache-id", "PATHFINDER_N7_CACHE_ID"),
            ("--n8-cache-id", "PATHFINDER_N8_CACHE_ID"),
            (
                "--n7-node-health-base-url",
                "PATHFINDER_N7_NODE_HEALTH_BASE_URL",
            ),
            (
                "--n8-node-health-base-url",
                "PATHFINDER_N8_NODE_HEALTH_BASE_URL",
            ),
            ("--n6-base-url", "PATHFINDER_N6_SEMANTIC_BASE_URL"),
            ("--n1-base-url", "PATHFINDER_N1_ORACLE_BASE_URL"),
            (
                "--n1-verification-base-url",
                "PATHFINDER_N1_VERIFICATION_BASE_URL",
            ),
            ("--semantic-model", "PATHFINDER_SEMANTIC_ROUTE_MODEL"),
            (
                "--simulator-private-http-hosts",
                "PATHFINDER_SEMANTIC_ROUTE_PRIVATE_HTTP_HOSTS",
            ),
            (
                "--timeout-seconds",
                "PATHFINDER_SEMANTIC_ROUTE_TIMEOUT_SECONDS",
            ),
            (
                "--max-artifact-bytes",
                "PATHFINDER_SEMANTIC_ROUTE_MAX_ARTIFACT_BYTES",
            ),
            ("--port", f"PATHFINDER_{node}_ROUTE_LISTEN_PORT"),
        )
        argv: list[str] = [
            "python",
            "-m",
            "pathfinder",
            "serve-simulator-full-flow-semantic-route",
            "--node-id",
            node,
        ]
        for option, environment_name in option_bindings:
            argv.extend((option, f"${{{environment_name}}}"))
        return _launcher(
            contract,
            implementation_kind="multi-process-http-service-group",
            implementation_id=(
                "pathfinder.simulator."
                "full_flow_semantic_route_service_factory"
            ),
            argv_template=_argv(*argv),
            artifact_binding_names=artifact_names,
            credential_env_names=(
                "PATHFINDER_CONTAINER_NODE_TOKEN",
                "PATHFINDER_DATA_AGENT_TOKEN",
                "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
                "PATHFINDER_N1_ORACLE_TOKEN",
                "PATHFINDER_N1_VERIFICATION_TOKEN",
                "PATHFINDER_N2_INDEX_TOKEN",
                "PATHFINDER_N3_DATA_AGENT_TOKEN",
                "PATHFINDER_N4_DATA_AGENT_TOKEN",
                "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
                "PATHFINDER_N7_INDEX_TOKEN",
                "PATHFINDER_N7_W4_CACHE_TOKEN",
                "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
                "PATHFINDER_N8_INDEX_TOKEN",
                "PATHFINDER_N8_W4_CACHE_TOKEN",
            ),
            configuration_env_names=(
                *endpoint_names,
                "PATHFINDER_FULL_FLOW_SERVICE_LISTEN_HOST",
                "PATHFINDER_N3_DATA_AGENT_LOCATION",
                "PATHFINDER_N4_DATA_AGENT_LOCATION",
                f"PATHFINDER_{node}_ROUTE_LISTEN_PORT",
                f"PATHFINDER_{node}_ROUTE_STATE_DIR",
                f"PATHFINDER_{node}_W4_COORDINATOR_LISTEN_PORT",
                f"PATHFINDER_{node}_W4_COORDINATOR_STATE_DB",
                f"PATHFINDER_{node}_W4_RAW_SAMPLER_SCRATCH_DIR",
                "PATHFINDER_N7_CACHE_ID",
                "PATHFINDER_N7_W4_CACHE_BASE_URL",
                "PATHFINDER_N7_W4_CACHE_CAPACITY_BYTES",
                "PATHFINDER_N7_W4_CACHE_ID",
                "PATHFINDER_N7_W4_CACHE_LISTEN_PORT",
                "PATHFINDER_N7_W4_CACHE_MAX_ARTIFACT_BYTES",
                "PATHFINDER_N7_W4_CACHE_STATE_DIR",
                "PATHFINDER_N8_CACHE_ID",
                "PATHFINDER_N8_W4_CACHE_BASE_URL",
                "PATHFINDER_N8_W4_CACHE_CAPACITY_BYTES",
                "PATHFINDER_N8_W4_CACHE_ID",
                "PATHFINDER_N8_W4_CACHE_LISTEN_PORT",
                "PATHFINDER_N8_W4_CACHE_MAX_ARTIFACT_BYTES",
                "PATHFINDER_N8_W4_CACHE_STATE_DIR",
                "PATHFINDER_SEMANTIC_ROUTE_MAX_ARTIFACT_BYTES",
                "PATHFINDER_SEMANTIC_ROUTE_MODEL",
                "PATHFINDER_SEMANTIC_ROUTE_PRIVATE_HTTP_HOSTS",
                "PATHFINDER_SEMANTIC_ROUTE_TIMEOUT_SECONDS",
            ),
            health_route="/healthz",
            independently_startable=True,
            migration_unit=f"{node}-semantic-route-coordinator",
            limitations=(
                "Only public promoted inputs and the label-free N1 "
                "commitment are mounted; hidden scoring remains behind "
                "authenticated N1 HTTP boundaries.",
                "The W4 FlowMesh coordinator is a separate authenticated "
                "companion with durable trial identity and scratch state.",
            ),
            companion_processes=(
                _w4_cache_companion(node),
                _w4_flowmesh_coordinator_companion(node),
            ),
        )
    if contract_id in {"N7.persistent-cache", "N8.persistent-cache"}:
        return _launcher(
            contract,
            implementation_kind="standalone-http-service",
            implementation_id="pathfinder.simulator.full_flow_cache",
            argv_template=_argv(
                "python",
                "-m",
                "pathfinder",
                "serve-simulator-full-flow-cache",
                "--node-id",
                node,
                "--cache-id",
                f"${{PATHFINDER_{node}_CACHE_ID}}",
                "--state-dir",
                f"${{{state_name}}}",
                "--capacity-bytes",
                f"${{PATHFINDER_{node}_CACHE_CAPACITY_BYTES}}",
                "--port",
                f"${{{listen_name}}}",
            ),
            credential_env_names=("PATHFINDER_FULL_FLOW_CACHE_TOKEN",),
            configuration_env_names=(
                f"PATHFINDER_{node}_CACHE_CAPACITY_BYTES",
                f"PATHFINDER_{node}_CACHE_ID",
                listen_name,
                state_name,
            ),
            health_route="/healthz",
            independently_startable=True,
            migration_unit=f"{node}-persistent-cache-service",
        )
    return None


def _embedded_launcher(contract: Mapping[str, Any]) -> dict[str, Any]:
    contract_id = str(contract.get("service_contract_id"))
    if contract_id == "N1.trial-control":
        return _launcher(
            contract,
            implementation_kind="flowmesh-runner-embedded",
            implementation_id=(
                "pathfinder.integrations.flowmesh.full_flow_trial"
            ),
            independently_startable=False,
            migration_unit="FlowMesh-trial-coordinator",
            limitations=(
                "Trial admission is a durable coordinator contract, not a "
                "separate HTTP daemon.",
            ),
        )
    if contract_id.endswith(".branch-join"):
        return _launcher(
            contract,
            implementation_kind="flowmesh-runner-embedded",
            implementation_id=(
                "pathfinder.integrations.flowmesh.full_flow_trial"
            ),
            independently_startable=False,
            migration_unit="FlowMesh-trial-coordinator",
            limitations=(
                "The branch-join action remains a FlowMesh workflow stage, "
                "not an independent service.",
            ),
        )
    if contract_id.startswith("transport."):
        return _launcher(
            contract,
            implementation_kind="container-runtime-embedded",
            implementation_id="pathfinder.simulator.container_node",
            independently_startable=False,
            migration_unit="deployment-network-link",
            limitations=(
                "Single-host transfer shaping is replaced by measured private "
                "network links in a multi-host deployment.",
            ),
        )
    raise FullFlowServiceBootstrapError(
        f"no startup implementation is registered for {contract_id}"
    )


def _build_launchers(contracts: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for contract in contracts:
        direct = _direct_launcher(contract)
        rows.append(direct if direct is not None else _embedded_launcher(contract))
    rows.sort(key=lambda row: row["service_contract_id"])
    return rows


def _documents(
    *,
    bootstrap_id: str,
    logical_report: Mapping[str, Any],
    contracts: Sequence[Mapping[str, Any]],
) -> dict[str, bytes]:
    bootstrap_id = _identifier(bootstrap_id, "bootstrap_id")
    launchers = _build_launchers(contracts)
    incomplete = [
        row for row in launchers if row["contract_complete"] is False
    ]
    direct = [row for row in launchers if row["independently_startable"]]
    embedded = [row for row in launchers if not row["independently_startable"]]
    w4_coordinator_nodes = sorted(
        node
        for row in direct
        for node in row["logical_node_ids"]
        for process in row["companion_processes"]
        if process.get("implementation_id")
        == "pathfinder.simulator.full_flow_w4_flowmesh_service"
    )
    w4_coordinators_complete = w4_coordinator_nodes == ["N7", "N8"]
    w4_cache_nodes = sorted(
        node
        for row in direct
        for node in row["logical_node_ids"]
        for process in row["companion_processes"]
        if process.get("runtime_service_contract_id")
        == f"{node}.w4-candidate-cache"
    )
    w4_caches_complete = w4_cache_nodes == ["N7", "N8"]
    logical_nodes = sorted({
        node for row in launchers for node in row["logical_node_ids"]
    })
    _require(
        logical_nodes == [f"N{index}" for index in range(1, 9)],
        "startup contracts do not cover N1--N8 exactly",
    )
    blocker_rows = [
        {
            "service_contract_id": row["service_contract_id"],
            "missing_actions": row["missing_actions"],
            "reason": row["limitations"][0],
            "requires_upcloud": False,
        }
        for row in incomplete
    ]
    report: dict[str, Any] = {
        "schema_version": SERVICE_BOOTSTRAP_SCHEMA_VERSION,
        "status": (
            "FROZEN_LOCAL_SERVICE_BOOTSTRAP_WITH_CODE_GAPS"
            if blocker_rows
            else "FROZEN_LOCAL_SERVICE_BOOTSTRAP_READY"
        ),
        "bootstrap_id": bootstrap_id,
        "logical_plan_sha256": logical_report["plan_sha256"],
        "logical_source_binding_sha256": logical_report[
            "source_binding_sha256"
        ],
        "scenario_id": logical_report["scenario_id"],
        "logical_node_ids": logical_nodes,
        "service_contract_count": len(launchers),
        "direct_process_contract_count": len(direct),
        "embedded_contract_count": len(embedded),
        "complete_contract_count": len(launchers) - len(incomplete),
        "incomplete_contract_count": len(incomplete),
        "local_code_blockers": blocker_rows,
        "upcloud_required_blocker_count": 0,
        "base_services_startable_without_upcloud": True,
        "full_api_isomorphic_topology_ready": (
            not blocker_rows
            and w4_coordinators_complete
            and w4_caches_complete
        ),
        "w4_flowmesh_coordinator_count": len(w4_coordinator_nodes),
        "w4_flowmesh_coordinator_nodes": w4_coordinator_nodes,
        "w4_flowmesh_coordinators_startable_without_upcloud": (
            w4_coordinators_complete
        ),
        "w4_dedicated_cache_count": len(w4_cache_nodes),
        "w4_dedicated_cache_nodes": w4_cache_nodes,
        "w4_cache_namespaces_exclusive": w4_caches_complete,
        "runtime_secret_resolution": (
            "service-aligned-canonical-name-with-documented-fallbacks"
        ),
        "single_host_compose_rendering_supported": True,
        "multi_host_private_network_rendering_supported": True,
        "same_logical_contracts_across_deployments": True,
        "compose_file_included": False,
        "automatic_service_lifecycle_included": False,
        "n4_publication_before_data_agent_rebind_required": True,
        "runtime_bindings_deferred": [
            "container-or-process-image",
            "credential-environment-values",
            "host-or-service-addresses",
            "listen-ports",
            "package-and-state-paths",
        ],
        "upcloud_only_evidence_deferred": [
            "measured-cross-host-network-throughput-and-rtt",
            "measured-provider-storage-behaviour",
            "measured-resource-contention-and-queueing",
            "provider-billed-monetary-cost",
        ],
        "launcher_file": LAUNCHERS_NAME,
        "launcher_file_sha256": _sha256(_jsonl_bytes(launchers)),
        "concrete_endpoints_included": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "services_started": False,
        "workflow_submitted": False,
        "cloud_calls_made": False,
        "eligible_for_scientific_claims": False,
    }
    report["bootstrap_sha256"] = _sha256(_canonical_bytes(report))
    _assert_runtime_values_absent([report, launchers])
    return {
        BOOTSTRAP_NAME: _json_bytes(report),
        LAUNCHERS_NAME: _jsonl_bytes(launchers),
    }


def _load_contracts(logical_root: Path) -> list[dict[str, Any]]:
    catalog = _read_json(
        logical_root / SERVICE_CATALOG_NAME,
        "logical service catalog",
    )
    rows = catalog.get("service_contracts")
    _require(isinstance(rows, list) and bool(rows), "service catalog is empty")
    contracts: list[dict[str, Any]] = []
    for row in rows:
        _require(isinstance(row, dict), "service contract must be an object")
        contracts.append(row)
    ids = [row.get("service_contract_id") for row in contracts]
    _require(
        all(isinstance(item, str) for item in ids)
        and ids == sorted(ids)
        and len(ids) == len(set(ids)),
        "service catalog order or identity is invalid",
    )
    return contracts


def _verify_file_set(root: Path) -> None:
    _require(root.is_dir(), "service bootstrap directory does not exist")
    expected = {BOOTSTRAP_NAME, LAUNCHERS_NAME, CHECKSUMS_NAME}
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "service bootstrap must contain regular files only",
    )
    _require(
        {path.name for path in entries} == expected,
        "service bootstrap file set changed",
    )
    documents = {
        name: (root / name).read_bytes()
        for name in expected - {CHECKSUMS_NAME}
    }
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(documents),
        "service bootstrap checksum failed",
    )


def freeze_full_flow_local_service_bootstrap(
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    *,
    bootstrap_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze endpoint-free process contracts without starting services."""

    logical_root = Path(logical_plan_dir).resolve()
    logical_report = verify_full_flow_logical_routes(
        logical_root,
        scenario_path,
        container_plan_dir,
    )
    contracts = _load_contracts(logical_root)
    documents = _documents(
        bootstrap_id=bootstrap_id,
        logical_report=logical_report,
        contracts=contracts,
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), "service bootstrap output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(
        tempfile.mkdtemp(prefix=".full-flow-bootstrap-", dir=target.parent)
    )
    staging = parent / "bootstrap"
    try:
        staging.mkdir()
        complete = dict(documents)
        complete[CHECKSUMS_NAME] = _checksums(documents)
        for name, payload in complete.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        verify_full_flow_local_service_bootstrap(
            staging,
            logical_plan_dir=logical_root,
            scenario_path=scenario_path,
            container_plan_dir=container_plan_dir,
        )
        os.replace(staging, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    verified = verify_full_flow_local_service_bootstrap(
        target,
        logical_plan_dir=logical_root,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    return {**verified, "output_dir": str(target)}


def verify_full_flow_local_service_bootstrap(
    bootstrap_dir: str | Path,
    *,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    """Re-derive every launcher and verify its exact logical-plan binding."""

    root = Path(bootstrap_dir).resolve()
    _verify_file_set(root)
    report = _read_json(root / BOOTSTRAP_NAME, "service bootstrap")
    unsigned = dict(report)
    recorded = unsigned.pop("bootstrap_sha256", None)
    _require(
        isinstance(recorded, str)
        and _SHA256.fullmatch(recorded) is not None
        and recorded == _sha256(_canonical_bytes(unsigned)),
        "service bootstrap digest failed",
    )
    logical_root = Path(logical_plan_dir).resolve()
    logical_report = verify_full_flow_logical_routes(
        logical_root,
        scenario_path,
        container_plan_dir,
    )
    contracts = _load_contracts(logical_root)
    expected = _documents(
        bootstrap_id=report.get("bootstrap_id"),
        logical_report=logical_report,
        contracts=contracts,
    )
    for name, payload in expected.items():
        _require(
            (root / name).read_bytes() == payload,
            f"{name} does not match its verified logical plan",
        )
    launchers = _read_jsonl(root / LAUNCHERS_NAME, "service launchers")
    _require(
        len(launchers) == report.get("service_contract_count"),
        "service launcher count changed",
    )
    _assert_runtime_values_absent([report, launchers])
    return {
        "status": "VERIFIED_LOCAL_SERVICE_BOOTSTRAP",
        "bootstrap_id": report["bootstrap_id"],
        "logical_plan_sha256": logical_report["plan_sha256"],
        "logical_node_count": len(report["logical_node_ids"]),
        "service_contract_count": report["service_contract_count"],
        "direct_process_contract_count": report[
            "direct_process_contract_count"
        ],
        "embedded_contract_count": report["embedded_contract_count"],
        "incomplete_contract_count": report["incomplete_contract_count"],
        "local_code_blockers": report["local_code_blockers"],
        "upcloud_required_blocker_count": 0,
        "base_services_startable_without_upcloud": True,
        "full_api_isomorphic_topology_ready": report[
            "full_api_isomorphic_topology_ready"
        ],
        "w4_flowmesh_coordinator_count": report[
            "w4_flowmesh_coordinator_count"
        ],
        "w4_flowmesh_coordinator_nodes": report[
            "w4_flowmesh_coordinator_nodes"
        ],
        "w4_flowmesh_coordinators_startable_without_upcloud": report[
            "w4_flowmesh_coordinators_startable_without_upcloud"
        ],
        "w4_dedicated_cache_count": report["w4_dedicated_cache_count"],
        "w4_dedicated_cache_nodes": report["w4_dedicated_cache_nodes"],
        "w4_cache_namespaces_exclusive": report[
            "w4_cache_namespaces_exclusive"
        ],
        "runtime_secret_resolution": report["runtime_secret_resolution"],
        "compose_file_included": False,
        "automatic_service_lifecycle_included": False,
        "n4_publication_before_data_agent_rebind_required": True,
        "concrete_endpoints_included": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "services_started": False,
        "workflow_submitted": False,
        "cloud_calls_made": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "BOOTSTRAP_NAME",
    "CHECKSUMS_NAME",
    "LAUNCHERS_NAME",
    "SERVICE_BOOTSTRAP_SCHEMA_VERSION",
    "SERVICE_LAUNCHER_SCHEMA_VERSION",
    "FullFlowServiceBootstrapError",
    "freeze_full_flow_local_service_bootstrap",
    "verify_full_flow_local_service_bootstrap",
]
