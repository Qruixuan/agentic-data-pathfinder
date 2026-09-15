"""Deterministic, non-launching Compose rendering for the full-flow simulator.

The endpoint-free service bootstrap is the process contract and the deployment
binding is the placement contract.  This module joins those two verified
artifacts into one local Docker Compose overlay without resolving any runtime
value.  Image references, ports, paths, network names, user IDs, and secrets
remain environment-variable placeholders supplied by the operator.

FlowMesh-owned trial control, branch joins, and logical transport are
intentionally not rendered as services.  N7/N8 semantic route execution is a
real independently startable service, while FlowMesh still owns the workflow
envelope and branch ordering.  The overlay records embedded runtime
requirements separately so it cannot silently replace the control plane it is
meant to exercise.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ._full_flow_primitives import (
    canonical_json_bytes,
    checked_identifier,
    checksum_manifest_bytes,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .container_node import (
    CONTAINER_NODE_BEARER_TOKEN_ENV,
    FULL_FLOW_INGRESS_HMAC_SECRET_ENV,
)
from .full_flow_deployment import (
    DEPLOYMENT_BINDING_NAME,
    verify_full_flow_deployment_binding,
)
from .full_flow_service_bootstrap import (
    BOOTSTRAP_NAME,
    LAUNCHERS_NAME,
    verify_full_flow_local_service_bootstrap,
)


COMPOSE_OVERLAY_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-compose-overlay/v1alpha2"
)
COMPOSE_GATE_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-compose-stage-gate/v1alpha1"
)
COMPOSE_NAME = "compose.full-flow-services.yaml"
MANIFEST_NAME = "full-flow-compose-overlay-manifest.json"
GATE_NAME = "full-flow-compose-stage-gate.json"
CHECKSUMS_NAME = "SHA256SUMS"

_OUTPUT_NAMES = {COMPOSE_NAME, MANIFEST_NAME, GATE_NAME}
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")
_ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_PLACEHOLDER = re.compile(r"\$\{([A-Z][A-Z0-9_]{0,127})\}")
_EXPECTED_NODES = [f"N{index}" for index in range(1, 9)]

_IMAGE_ENV = "PATHFINDER_FULL_FLOW_SERVICE_IMAGE"
_NETWORK_ENV = "PATHFINDER_FULL_FLOW_NETWORK_NAME"
_BIND_ADDRESS_ENV = "PATHFINDER_FULL_FLOW_BIND_ADDRESS"
_RUNTIME_USER_ENV = "PATHFINDER_FULL_FLOW_RUNTIME_UID_GID"


class FullFlowComposeOverlayError(ValueError):
    """Raised when a Compose overlay is unsafe or no longer reproducible."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowComposeOverlayError(message)


def _identifier(value: Any, name: str) -> str:
    return str(
        checked_identifier(
            value,
            name,
            error_type=FullFlowComposeOverlayError,
            pattern=_SAFE_ID,
        )
    )


def _env_name(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _ENV_NAME.fullmatch(value) is not None,
        f"{name} is not an environment variable name",
    )
    return str(value)


def _sha256(payload: bytes) -> str:
    return sha256_hex(payload)


def _json_bytes(value: Any) -> bytes:
    return pretty_json_bytes(
        value,
        error_type=FullFlowComposeOverlayError,
        error_message="Compose overlay document is not valid JSON",
    )


def _canonical_bytes(value: Any) -> bytes:
    return canonical_json_bytes(
        value,
        error_type=FullFlowComposeOverlayError,
        error_message="Compose overlay document is not canonical JSON",
    )


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return checksum_manifest_bytes(documents)


def _strict_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = strict_json_loads(
            path.read_text(encoding="utf-8"),
            error_type=FullFlowComposeOverlayError,
            duplicate_key_message=lambda key: f"{label} repeats key {key}",
            nonfinite_number_message=lambda token: f"{label} contains {token}",
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowComposeOverlayError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _strict_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowComposeOverlayError(f"cannot read {label}") from exc
    _require(lines and all(lines), f"{label} is empty or contains a blank row")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FullFlowComposeOverlayError(
                f"{label}[{index}] is invalid JSON"
            ) from exc
        _require(isinstance(row, dict), f"{label}[{index}] must be an object")
        rows.append(row)
    return rows


def _strings(value: Any, label: str, *, environment: bool = False) -> list[str]:
    _require(isinstance(value, list), f"{label} must be an array")
    result: list[str] = []
    for index, item in enumerate(value):
        result.append(
            _env_name(item, f"{label}[{index}]")
            if environment
            else _identifier(item, f"{label}[{index}]")
        )
    _require(result == sorted(set(result)), f"{label} must be sorted and unique")
    return result


def _ordered_strings(value: Any, label: str) -> list[str]:
    _require(isinstance(value, list), f"{label} must be an array")
    result = [
        _identifier(item, f"{label}[{index}]")
        for index, item in enumerate(value)
    ]
    _require(len(result) == len(set(result)), f"{label} must be unique")
    return result


def _yaml_scalar(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _compose_required(value: str) -> str:
    _env_name(value, "Compose runtime variable")
    return "${" + value + ":?set " + value + "}"


def _required_command_argument(value: str) -> str:
    return _PLACEHOLDER.sub(
        lambda match: _compose_required(match.group(1)),
        value,
    )


def _service_name(contract_id: str, implementation_id: str, primary: bool) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", contract_id.casefold()).strip("-")
    if primary:
        return f"pathfinder-full-flow-{base}"
    suffix = re.sub(
        r"[^a-z0-9]+",
        "-",
        implementation_id.rsplit(".", 1)[-1].casefold(),
    ).strip("-")
    return f"pathfinder-full-flow-{base}-{suffix}"


def _host_port_env(service_name: str) -> str:
    suffix = re.sub(r"[^A-Z0-9]+", "_", service_name.upper()).strip("_")
    return _env_name(f"{suffix}_HOST_PORT", "host port variable")


def _state_volume_key(contract_id: str) -> str:
    suffix = re.sub(r"[^a-z0-9]+", "-", contract_id.casefold()).strip("-")
    return f"full-flow-{suffix}-state"


def _state_volume_env(contract_id: str) -> str:
    suffix = re.sub(r"[^A-Z0-9]+", "_", contract_id.upper()).strip("_")
    return _env_name(f"PATHFINDER_COMPOSE_{suffix}_STATE_VOLUME", "state volume")


def _port_env(argv: Sequence[Any], label: str) -> str:
    values = [str(item) for item in argv]
    positions = [index for index, item in enumerate(values) if item == "--port"]
    _require(len(positions) == 1, f"{label} must declare exactly one --port")
    position = positions[0]
    _require(position + 1 < len(values), f"{label} --port has no value")
    match = _PLACEHOLDER.fullmatch(values[position + 1])
    _require(match is not None, f"{label} port must remain an environment placeholder")
    return _env_name(match.group(1), f"{label} port variable")


def _argv(value: Any, label: str) -> list[str]:
    _require(isinstance(value, list) and value, f"{label} command is empty")
    result: list[str] = []
    for index, item in enumerate(value):
        _require(isinstance(item, str) and item, f"{label}[{index}] is invalid")
        result.append(str(item))
    return result


def _placeholder_names(argv: Sequence[str]) -> set[str]:
    names: set[str] = set()
    for value in argv:
        names.update(_PLACEHOLDER.findall(value))
    return names


def _profiles(contract_id: str, *, primary: bool) -> list[str]:
    if contract_id == "N4.derived-data-agent" and not primary:
        return ["provision-derived"]
    if contract_id == "N4.derived-data-agent":
        return ["serve-frozen"]
    if contract_id == "N5.materializer":
        return ["provision-derived", "serve-frozen"]
    return ["serve-frozen"]


def _component_runtime_names(
    contract_id: str,
    *,
    primary: bool,
    command_names: set[str],
    artifact_names: Sequence[str],
    launcher_credentials: Sequence[str],
    launcher_configuration: Sequence[str],
    binding_credentials: Sequence[str],
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Return least-privilege names for one process in a service group."""

    # A companion is a separate process boundary.  Deployment credentials on
    # the parent logical binding belong to the primary process unless the
    # companion explicitly declares the same environment name.
    credentials = set(binding_credentials) if primary else set()
    configuration = set(command_names)
    artifacts = set(artifact_names) & command_names
    if contract_id == "N1.hidden-score":
        companion_prefix = "PATHFINDER_N1_VERIFICATION_"
        if primary:
            credentials.update(
                name
                for name in launcher_credentials
                if not name.startswith(companion_prefix)
                and name != "PATHFINDER_N1_VERIFICATION_TOKEN"
            )
            configuration.update(
                name
                for name in launcher_configuration
                if not name.startswith(companion_prefix)
            )
        else:
            credentials.update({
                name
                for name in launcher_credentials
                if name.startswith(companion_prefix)
                or name == "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET"
            })
            configuration.update(
                name
                for name in launcher_configuration
                if name.startswith(companion_prefix)
            )
    elif contract_id == "N4.derived-data-agent":
        companion_prefix = "PATHFINDER_N4_PUBLICATION_"
        if primary:
            credentials.update(
                name
                for name in launcher_credentials
                if not name.startswith(companion_prefix)
            )
            configuration.update(
                name
                for name in launcher_configuration
                if not name.startswith(companion_prefix)
            )
        else:
            credentials.update(
                name
                for name in launcher_credentials
                if name.startswith(companion_prefix)
            )
            configuration.update(
                name
                for name in launcher_configuration
                if name.startswith(companion_prefix)
            )
    elif contract_id == "N5.materializer":
        companion_prefix = "PATHFINDER_N5_DIGEST_"
        if primary:
            credentials.update(
                name
                for name in launcher_credentials
                if not name.startswith(companion_prefix)
            )
            configuration.update(
                name
                for name in launcher_configuration
                if not name.startswith(companion_prefix)
            )
        else:
            credentials.update(
                name
                for name in launcher_credentials
                if name.startswith(companion_prefix)
            )
            configuration.update(
                name
                for name in launcher_configuration
                if name.startswith(companion_prefix)
                or name == "PATHFINDER_N5_STATE_DIR"
            )
    else:
        credentials.update(launcher_credentials)
        configuration.update(launcher_configuration)
    if contract_id == "N6.semantic-inference":
        credentials.add(CONTAINER_NODE_BEARER_TOKEN_ENV)
    environment = sorted(artifacts | credentials | configuration | command_names)
    return (
        sorted(artifacts),
        sorted(credentials),
        sorted(configuration),
        environment,
    )


def _state_path_names(configuration_names: Sequence[str]) -> list[str]:
    suffixes = (
        "_OPERATION_DB",
        "_PUBLICATION_STORE",
        "_SCRATCH_DIR",
        "_STATE_DB",
        "_STATE_DIR",
    )
    return sorted(
        name for name in configuration_names if name.endswith(suffixes)
    )


def _component(
    launcher: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    process: Mapping[str, Any] | None,
    runtime_binding: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    contract_id = _identifier(
        launcher.get("service_contract_id"),
        "launcher service_contract_id",
    )
    nodes = _strings(launcher.get("logical_node_ids"), f"{contract_id}.nodes")
    _require(len(nodes) == 1, f"{contract_id} must map to one logical node")
    primary = process is None
    implementation_id = _identifier(
        (
            launcher.get("implementation_id")
            if primary
            else process.get("implementation_id")
        ),
        f"{contract_id}.implementation_id",
    )
    runtime_service_contract_id = _identifier(
        (
            contract_id
            if process is None
            else process.get("runtime_service_contract_id", contract_id)
        ),
        f"{contract_id}.runtime_service_contract_id",
    )
    if runtime_binding is not None:
        _require(
            process is not None
            and runtime_binding.get("runtime_service_contract_id")
            == runtime_service_contract_id
            and runtime_binding.get("parent_service_contract_id") == contract_id
            and runtime_binding.get("logical_node_id") == nodes[0],
            f"{contract_id} runtime deployment binding identity changed",
        )
    command = _argv(
        (
            launcher.get("entrypoint_argv_template")
            if primary
            else process.get("entrypoint_argv_template")
        ),
        f"{contract_id}.command",
    )
    health_route = (
        launcher.get("health_route")
        if primary
        else process.get("health_route")
    )
    _require(health_route == "/healthz", f"{contract_id} health route changed")
    launcher_artifacts = _strings(
        launcher.get("artifact_binding_env_names"),
        f"{contract_id}.artifact_binding_env_names",
        environment=True,
    )
    launcher_credentials = _strings(
        launcher.get("credential_env_names"),
        f"{contract_id}.credential_env_names",
        environment=True,
    )
    binding_credentials = _strings(
        (
            runtime_binding.get("credential_env_names")
            if runtime_binding is not None
            else binding.get("credential_env_names")
        ),
        f"{contract_id}.deployment_credentials",
        environment=True,
    )
    launcher_configuration = _strings(
        launcher.get("configuration_env_names"),
        f"{contract_id}.configuration_env_names",
        environment=True,
    )
    if (
        process is None
        and contract_id in {"N7.execution-compute", "N8.execution-compute"}
    ):
        launcher_credentials = [
            name for name in launcher_credentials if "_W4_" not in name
        ]
        launcher_configuration = [
            name for name in launcher_configuration if "_W4_" not in name
        ]
    if process is not None:
        for field, launcher_values in (
            ("artifact_binding_env_names", launcher_artifacts),
            ("credential_env_names", launcher_credentials),
            ("configuration_env_names", launcher_configuration),
        ):
            if field not in process:
                continue
            process_values = _strings(
                process.get(field),
                f"{contract_id}.companion.{field}",
                environment=True,
            )
            _require(
                set(process_values).issubset(launcher_values),
                f"{contract_id} companion {field} exceeds its launcher",
            )
            if field == "artifact_binding_env_names":
                launcher_artifacts = process_values
            elif field == "credential_env_names":
                launcher_credentials = process_values
            else:
                launcher_configuration = process_values
    if runtime_binding is not None:
        _require(
            set(launcher_credentials) == set(binding_credentials),
            f"{contract_id} runtime credential contract differs from launcher",
        )
    (
        artifact_names,
        credential_names,
        configuration_names,
        environment_names,
    ) = _component_runtime_names(
        contract_id,
        primary=primary,
        command_names=_placeholder_names(command),
        artifact_names=launcher_artifacts,
        launcher_credentials=launcher_credentials,
        launcher_configuration=launcher_configuration,
        binding_credentials=binding_credentials,
    )
    for name in environment_names:
        _env_name(name, f"{contract_id} environment")
    service_name = _service_name(contract_id, implementation_id, primary)
    host_port_name = _host_port_env(service_name)
    ephemeral_state_names = (
        []
        if process is None
        else _strings(
            process.get("ephemeral_state_env_names", []),
            f"{contract_id}.companion.ephemeral_state_env_names",
            environment=True,
        )
    )
    _require(
        set(ephemeral_state_names).issubset(configuration_names),
        f"{contract_id} companion ephemeral state exceeds its configuration",
    )
    state_path_names = [
        name
        for name in _state_path_names(configuration_names)
        if name not in ephemeral_state_names
    ]
    process_persistent_state = (
        process.get("persistent_state_required", False)
        if process is not None
        else False
    )
    _require(
        isinstance(process_persistent_state, bool),
        f"{contract_id} companion persistent-state flag is invalid",
    )
    persistent_state = (
        (
            runtime_binding.get("persistent_state") is True
            if runtime_binding is not None
            else binding.get("persistent_state") is True
        )
        or process_persistent_state
    )
    state_volume_required = persistent_state and bool(state_path_names)
    _require(
        not persistent_state or state_volume_required or bool(artifact_names),
        f"{contract_id} persistent state has neither state nor artifact binding",
    )
    state_owner_id = (
        runtime_service_contract_id
        if process_persistent_state
        else contract_id
    )
    return {
        "service_name": service_name,
        "service_contract_id": contract_id,
        "runtime_service_contract_id": runtime_service_contract_id,
        "logical_node_id": nodes[0],
        "component_kind": "primary" if primary else "companion",
        "implementation_id": implementation_id,
        "command": command,
        "health_route": str(health_route),
        "listen_port_env_name": _port_env(command, service_name),
        "host_port_env_name": host_port_name,
        "artifact_binding_env_names": artifact_names,
        "credential_env_names": credential_names,
        "configuration_env_names": configuration_names,
        "environment_names": environment_names,
        "persistent_state": persistent_state,
        "state_path_env_names": state_path_names,
        "ephemeral_state_path_env_names": ephemeral_state_names,
        "state_volume_required": state_volume_required,
        "state_volume_key": (
            _state_volume_key(state_owner_id)
            if state_volume_required
            else None
        ),
        "state_volume_env_name": (
            _state_volume_env(state_owner_id)
            if state_volume_required
            else None
        ),
        "profiles": _profiles(contract_id, primary=primary),
        "deployment_origin_sha256": (
            _sha256(str(runtime_binding["base_url"]).encode("utf-8"))
            if runtime_binding is not None
            and runtime_binding.get("base_url") is not None
            else (
                _sha256(str(binding["base_url"]).encode("utf-8"))
                if primary and binding.get("base_url") is not None
                else None
            )
        ),
        "depends_on_runtime_service_contract_ids": (
            []
            if process is None
            else _strings(
                process.get("depends_on_runtime_service_contract_ids", []),
                f"{contract_id}.companion.dependencies",
            )
        ),
    }


def _build_components(
    launchers: Sequence[Mapping[str, Any]],
    bindings: Mapping[str, Mapping[str, Any]],
    runtime_bindings: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    components: list[dict[str, Any]] = []
    embedded: list[dict[str, Any]] = []
    for launcher in launchers:
        contract_id = _identifier(
            launcher.get("service_contract_id"),
            "launcher service_contract_id",
        )
        _require(contract_id in bindings, f"deployment lacks {contract_id}")
        binding = bindings[contract_id]
        if launcher.get("independently_startable") is not True:
            credentials = _strings(
                binding.get("credential_env_names"),
                f"{contract_id}.deployment_credentials",
                environment=True,
            )
            embedded.append({
                "service_contract_id": contract_id,
                "logical_node_ids": _ordered_strings(
                    launcher.get("logical_node_ids"),
                    f"{contract_id}.nodes",
                ),
                "implementation_kind": launcher.get("implementation_kind"),
                "credential_env_names": credentials,
            })
            continue
        _require(
            launcher.get("contract_complete") is True
            and launcher.get("missing_actions") == [],
            f"{contract_id} is not locally complete",
        )
        components.append(_component(launcher, binding, process=None))
        companions = launcher.get("companion_processes")
        _require(isinstance(companions, list), f"{contract_id} companions changed")
        for process in companions:
            _require(isinstance(process, Mapping), f"{contract_id} companion invalid")
            runtime_id = _identifier(
                process.get("runtime_service_contract_id", contract_id),
                f"{contract_id}.companion.runtime_service_contract_id",
            )
            components.append(
                _component(
                    launcher,
                    binding,
                    process=process,
                    runtime_binding=runtime_bindings.get(runtime_id),
                )
            )
    components.sort(key=lambda row: row["service_name"])
    embedded.sort(key=lambda row: row["service_contract_id"])
    for row in components:
        dependency_services: list[str] = []
        for runtime_id in row["depends_on_runtime_service_contract_ids"]:
            matching_rows = [
                candidate
                for candidate in components
                if candidate["runtime_service_contract_id"] == runtime_id
            ]
            primary_matches = [
                candidate
                for candidate in matching_rows
                if candidate["component_kind"] == "primary"
            ]
            if len(primary_matches) == 1:
                matching_rows = primary_matches
            matches = [
                candidate["service_name"] for candidate in matching_rows
            ]
            _require(
                len(matches) == 1 and matches[0] != row["service_name"],
                f"{row['service_name']} dependency {runtime_id} is ambiguous",
            )
            dependency_services.append(matches[0])
        row["depends_on_service_names"] = sorted(dependency_services)
    service_names = [row["service_name"] for row in components]
    _require(
        service_names == sorted(set(service_names)),
        "Compose service names collide",
    )
    covered = sorted({row["logical_node_id"] for row in components})
    _require(covered == _EXPECTED_NODES, "Compose services do not cover N1--N8")
    companions = {
        (row["service_contract_id"], row["implementation_id"])
        for row in components
        if row["component_kind"] == "companion"
    }
    _require(
        companions
        == {
            (
                "N1.hidden-score",
                (
                    "pathfinder.simulator."
                    "full_flow_n1_remote_verification"
                ),
            ),
            (
                "N4.derived-data-agent",
                "pathfinder.simulator.n4_publication_http",
            ),
            (
                "N5.materializer",
                "pathfinder.simulator.n5_digest_http",
            ),
            (
                "N7.execution-compute",
                "pathfinder.simulator.full_flow_cache",
            ),
            (
                "N7.execution-compute",
                "pathfinder.simulator.full_flow_w4_flowmesh_service",
            ),
            (
                "N8.execution-compute",
                "pathfinder.simulator.full_flow_cache",
            ),
            (
                "N8.execution-compute",
                "pathfinder.simulator.full_flow_w4_flowmesh_service",
            ),
        },
        "required N1, N4, N5, N7, or N8 companion is missing",
    )
    return components, embedded


def _health_script(port_env_name: str, route: str) -> str:
    return (
        "import http.client, json, os; "
        "c=http.client.HTTPConnection('127.0.0.1', "
        f"int(os.environ[{port_env_name!r}]), "
        "timeout=2); "
        f"c.request('GET', {route!r}); "
        "r=c.getresponse(); b=r.read(65537); "
        "p=json.loads(b) if len(b) <= 65536 else {}; "
        "raise SystemExit(0 if r.status == 200 "
        "and p.get('status') == 'ok' "
        "and p.get('credentials_recorded') is False else 1)"
    )


def _compose_bytes(components: Sequence[Mapping[str, Any]]) -> bytes:
    lines = ["services:"]
    for row in components:
        service = str(row["service_name"])
        lines.extend([
            f"  {service}:",
            f"    image: {_yaml_scalar(_compose_required(_IMAGE_ENV))}",
            '    pull_policy: "never"',
            f"    user: {_yaml_scalar(_compose_required(_RUNTIME_USER_ENV))}",
            "    command:",
        ])
        for argument in row["command"]:
            lines.append(
                "      - "
                + _yaml_scalar(_required_command_argument(str(argument)))
            )
        lines.append("    environment:")
        for name in row["environment_names"]:
            if name in row["ephemeral_state_path_env_names"]:
                lines.append(f'      - "{name}=/scratch"')
            else:
                lines.append(f"      - {name}")
        lines.extend([
            "    read_only: true",
            "    init: true",
            "    tmpfs:",
            '      - "/tmp:rw,noexec,nosuid,nodev,size=64m"',
        ])
        if row["ephemeral_state_path_env_names"]:
            lines.append(
                '      - "/scratch:rw,noexec,nosuid,nodev,size=2147483648"'
            )
        lines.extend([
            "    cap_drop:",
            '      - "ALL"',
            "    security_opt:",
            '      - "no-new-privileges:true"',
            "    pids_limit: 128",
            '    restart: "no"',
            "    stop_grace_period: 15s",
            "    profiles:",
        ])
        for profile in row["profiles"]:
            lines.append(f"      - {_yaml_scalar(str(profile))}")
        if row["depends_on_service_names"]:
            lines.append("    depends_on:")
            for dependency in row["depends_on_service_names"]:
                lines.extend([
                    f"      {dependency}:",
                    '        condition: "service_healthy"',
                ])
        lines.extend([
            "    ports:",
            "      - "
            + _yaml_scalar(
                _compose_required(_BIND_ADDRESS_ENV)
                + ":"
                + _compose_required(str(row["host_port_env_name"]))
                + ":"
                + _compose_required(str(row["listen_port_env_name"]))
            ),
            "    healthcheck:",
            "      test:",
            '        - "CMD"',
            '        - "python"',
            '        - "-c"',
            "        - "
            + _yaml_scalar(
                _health_script(
                    str(row["listen_port_env_name"]),
                    str(row["health_route"]),
                )
            ),
            "      interval: 5s",
            "      timeout: 3s",
            "      retries: 12",
            "      start_period: 5s",
            "    networks:",
            "      - pathfinder-full-flow",
            "    labels:",
            "      pathfinder.logical-node: "
            + _yaml_scalar(str(row["logical_node_id"])),
            "      pathfinder.service-contract: "
            + _yaml_scalar(str(row["service_contract_id"])),
            "      pathfinder.runtime-service-contract: "
            + _yaml_scalar(str(row["runtime_service_contract_id"])),
            "      pathfinder.component-kind: "
            + _yaml_scalar(str(row["component_kind"])),
        ])
        if row["service_contract_id"] == "N4.derived-data-agent":
            gate = (
                "publish-first"
                if row["component_kind"] == "companion"
                else "serve-only-after-rebind"
            )
            lines.append(
                "      pathfinder.n4-publication-gate: " + _yaml_scalar(gate)
            )
        volume_rows: list[tuple[str, str, bool]] = []
        if row["state_volume_required"]:
            volume_rows.append((str(row["state_volume_key"]), "/state", False))
        for env_name in row["artifact_binding_env_names"]:
            placeholder = _compose_required(str(env_name))
            volume_rows.append((placeholder, placeholder, True))
        if volume_rows:
            lines.append("    volumes:")
            for source, target, read_only in volume_rows:
                lines.extend([
                    "      - type: " + ("bind" if read_only else "volume"),
                    f"        source: {_yaml_scalar(source)}",
                    f"        target: {_yaml_scalar(target)}",
                ])
                if read_only:
                    lines.append("        read_only: true")
    lines.extend([
        "networks:",
        "  pathfinder-full-flow:",
        "    external: true",
        f"    name: {_yaml_scalar(_compose_required(_NETWORK_ENV))}",
        "volumes:",
    ])
    volumes = {
        (str(row["state_volume_key"]), str(row["state_volume_env_name"]))
        for row in components
        if row["state_volume_required"]
    }
    for key, env_name in sorted(volumes):
        lines.extend([
            f"  {key}:",
            f"    name: {_yaml_scalar(_compose_required(env_name))}",
            "    labels:",
            '      pathfinder.persistence: "required"',
        ])
    lines.append("")
    return "\n".join(lines).encode("utf-8")


def _stage_gate(
    *,
    overlay_id: str,
    components: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    publication = next(
        row
        for row in components
        if row["service_contract_id"] == "N4.derived-data-agent"
        and row["component_kind"] == "companion"
    )
    data_agent = next(
        row
        for row in components
        if row["service_contract_id"] == "N4.derived-data-agent"
        and row["component_kind"] == "primary"
    )
    provisioning = sorted(
        row["service_name"]
        for row in components
        if "provision-derived" in row["profiles"]
    )
    serving = sorted(
        row["service_name"]
        for row in components
        if "serve-frozen" in row["profiles"]
    )
    return {
        "schema_version": COMPOSE_GATE_SCHEMA_VERSION,
        "status": "OPERATOR_GATE_REQUIRED_NOT_SATISFIED",
        "overlay_id": overlay_id,
        "gate_id": "N4-publish-before-immutable-data-agent-rebind",
        "provision_profile": "provision-derived",
        "provision_services": provisioning,
        "publication_service": publication["service_name"],
        "publication_store_env_name": "PATHFINDER_N4_PUBLICATION_STORE",
        "required_evidence_before_rebind": [
            "all-required-derived-artifacts-published",
            "published-artifact-content-digests-verified",
            "immutable-N4-generation-manifest-frozen",
            "N4-publication-service-stopped",
        ],
        "rebind_env_name": "PATHFINDER_N4_DATA_AGENT_MANIFEST",
        "serve_profile": "serve-frozen",
        "serve_services": serving,
        "data_agent_service": data_agent["service_name"],
        "enforcement": "operator-attested-staged-profile-gate",
        "compose_automatically_enforces_gate": False,
        "serve_profile_must_not_be_selected_before_gate": True,
        "data_agent_start_before_gate_allowed": False,
        "publication_mutation_during_trials_allowed": False,
        "gate_evidence_included": False,
        "gate_satisfied": False,
        "services_started": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
    }


def _persistent_volume_bindings(
    components: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in components:
        if not row["state_volume_required"]:
            continue
        key = str(row["state_volume_key"])
        current = grouped.setdefault(key, {
            "service_contract_id": row["service_contract_id"],
            "runtime_service_contract_id": row[
                "runtime_service_contract_id"
            ],
            "logical_node_id": row["logical_node_id"],
            "state_volume_key": key,
            "state_volume_env_name": row["state_volume_env_name"],
            "container_mount": "/state",
            "state_path_env_names": set(),
        })
        _require(
            current["service_contract_id"] == row["service_contract_id"]
            and current["runtime_service_contract_id"]
            == row["runtime_service_contract_id"]
            and current["logical_node_id"] == row["logical_node_id"]
            and current["state_volume_env_name"] == row["state_volume_env_name"],
            "persistent volume identity changed within a service group",
        )
        current["state_path_env_names"].update(row["state_path_env_names"])
    result: list[dict[str, Any]] = []
    for key in sorted(grouped):
        row = dict(grouped[key])
        row["state_path_env_names"] = sorted(row["state_path_env_names"])
        result.append(row)
    return result


def _documents(
    *,
    overlay_id: str,
    bootstrap_root: Path,
    bootstrap_report: Mapping[str, Any],
    deployment_report: Mapping[str, Any],
    launchers: Sequence[Mapping[str, Any]],
    deployment: Mapping[str, Any],
) -> dict[str, bytes]:
    overlay_id = _identifier(overlay_id, "overlay_id")
    binding_rows = deployment.get("service_bindings")
    _require(isinstance(binding_rows, list), "deployment service bindings are invalid")
    bindings: dict[str, Mapping[str, Any]] = {}
    for row in binding_rows:
        _require(isinstance(row, Mapping), "deployment service binding is invalid")
        contract_id = _identifier(row.get("service_contract_id"), "contract ID")
        _require(contract_id not in bindings, "deployment contract repeats")
        bindings[contract_id] = row
    runtime_rows = deployment.get("runtime_service_bindings")
    _require(
        isinstance(runtime_rows, list),
        "deployment lacks W4 runtime service bindings",
    )
    runtime_bindings: dict[str, Mapping[str, Any]] = {}
    for row in runtime_rows:
        _require(
            isinstance(row, Mapping),
            "deployment runtime service binding is invalid",
        )
        runtime_id = _identifier(
            row.get("runtime_service_contract_id"),
            "runtime service contract ID",
        )
        _require(
            runtime_id not in runtime_bindings,
            "deployment runtime service contract repeats",
        )
        runtime_bindings[runtime_id] = row
    components, embedded = _build_components(
        launchers,
        bindings,
        runtime_bindings,
    )
    compose = _compose_bytes(components)
    gate = _stage_gate(overlay_id=overlay_id, components=components)
    gate_bytes = _json_bytes(gate)
    persistent_volumes = _persistent_volume_bindings(components)
    node_groups = {
        node: sorted(
            row["service_name"]
            for row in components
            if row["logical_node_id"] == node
        )
        for node in _EXPECTED_NODES
    }
    runtime_env_names = sorted({
        _IMAGE_ENV,
        _NETWORK_ENV,
        _BIND_ADDRESS_ENV,
        _RUNTIME_USER_ENV,
        *(
            str(name)
            for row in components
            for name in (
                list(row["environment_names"])
                + [row["host_port_env_name"]]
                + (
                    [row["state_volume_env_name"]]
                    if row["state_volume_env_name"] is not None
                    else []
                )
            )
        ),
    })
    credential_names = sorted({
        *(str(name) for row in components for name in row["credential_env_names"]),
        *(
            str(name)
            for row in embedded
            for name in row["credential_env_names"]
        ),
    })
    service_inventory = [
        {
            "service_name": row["service_name"],
            "service_contract_id": row["service_contract_id"],
            "runtime_service_contract_id": row[
                "runtime_service_contract_id"
            ],
            "logical_node_id": row["logical_node_id"],
            "component_kind": row["component_kind"],
            "implementation_id": row["implementation_id"],
            "profiles": row["profiles"],
            "depends_on_service_names": row[
                "depends_on_service_names"
            ],
            "listen_port_env_name": row["listen_port_env_name"],
            "host_port_env_name": row["host_port_env_name"],
            "artifact_binding_env_names": row[
                "artifact_binding_env_names"
            ],
            "credential_env_names": row["credential_env_names"],
            "state_volume_key": row["state_volume_key"],
            "state_volume_env_name": row["state_volume_env_name"],
            "ephemeral_state_path_env_names": row[
                "ephemeral_state_path_env_names"
            ],
            "deployment_origin_sha256": row["deployment_origin_sha256"],
        }
        for row in components
    ]
    w4_coordinator_services = sorted(
        row["service_name"]
        for row in service_inventory
        if row["implementation_id"]
        == "pathfinder.simulator.full_flow_w4_flowmesh_service"
    )
    w4_coordinator_nodes = sorted(
        row["logical_node_id"]
        for row in service_inventory
        if row["implementation_id"]
        == "pathfinder.simulator.full_flow_w4_flowmesh_service"
    )
    _require(
        w4_coordinator_nodes == ["N7", "N8"],
        "Compose overlay must contain N7 and N8 W4 coordinators",
    )
    _require(
        set(runtime_bindings)
        == {"N7.w4-candidate-coordinator", "N8.w4-candidate-coordinator"},
        "Compose overlay requires exact N7/N8 W4 runtime bindings",
    )
    w4_bound_origin_count = sum(
        row["runtime_service_contract_id"] in runtime_bindings
        and row["deployment_origin_sha256"] is not None
        for row in service_inventory
    )
    _require(
        w4_bound_origin_count == 2,
        "W4 coordinator deployment origins are not bound exactly",
    )
    w4_cache_services = sorted(
        row["service_name"]
        for row in service_inventory
        if row["runtime_service_contract_id"]
        in {"N7.w4-candidate-cache", "N8.w4-candidate-cache"}
    )
    _require(
        len(w4_cache_services) == 2,
        "Compose overlay must contain two dedicated W4 caches",
    )
    manifest: dict[str, Any] = {
        "schema_version": COMPOSE_OVERLAY_SCHEMA_VERSION,
        "status": "FROZEN_LOCAL_COMPOSE_OVERLAY_NOT_LAUNCHED",
        "overlay_id": overlay_id,
        "bootstrap_id": bootstrap_report["bootstrap_id"],
        "deployment_id": deployment_report["deployment_id"],
        "deployment_backend": deployment_report["backend"],
        "logical_plan_sha256": deployment_report["logical_plan_sha256"],
        "service_bootstrap_sha256": _sha256(
            (bootstrap_root / BOOTSTRAP_NAME).read_bytes()
        ),
        "service_launchers_sha256": _sha256(
            (bootstrap_root / LAUNCHERS_NAME).read_bytes()
        ),
        "deployment_binding_sha256": deployment_report["binding_sha256"],
        "compose_file": COMPOSE_NAME,
        "compose_sha256": _sha256(compose),
        "stage_gate_file": GATE_NAME,
        "stage_gate_sha256": _sha256(gate_bytes),
        "logical_node_ids": list(_EXPECTED_NODES),
        "logical_node_group_count": len(node_groups),
        "logical_node_service_groups": node_groups,
        "compose_service_count": len(components),
        "service_inventory": service_inventory,
        "primary_service_count": sum(
            row["component_kind"] == "primary" for row in components
        ),
        "companion_service_count": sum(
            row["component_kind"] == "companion" for row in components
        ),
        "w4_flowmesh_coordinator_service_count": len(
            w4_coordinator_services
        ),
        "w4_flowmesh_coordinator_services": w4_coordinator_services,
        "w4_flowmesh_coordinator_nodes": w4_coordinator_nodes,
        "w4_flowmesh_coordinator_health_routes_configured": True,
        "w4_runtime_service_binding_count": len(runtime_bindings),
        "w4_runtime_bindings_complete": True,
        "w4_coordinator_deployment_origin_count": w4_bound_origin_count,
        "w4_dedicated_cache_service_count": len(w4_cache_services),
        "w4_dedicated_cache_services": w4_cache_services,
        "w4_cache_namespaces_exclusive": True,
        "w4_coordinator_cache_startup_order_enforced": True,
        "runtime_secret_resolution": (
            "service-aligned-canonical-name-with-documented-fallbacks"
        ),
        "persistent_volume_count": len(persistent_volumes),
        "persistent_volume_bindings": persistent_volumes,
        "persistent_artifact_binding_contracts": sorted({
            str(row["service_contract_id"])
            for row in components
            if row["persistent_state"] and row["artifact_binding_env_names"]
        }),
        "state_path_values_must_resolve_beneath_container_mount": True,
        "embedded_flowmesh_contract_count": len(embedded),
        "embedded_flowmesh_contracts": list(embedded),
        "flowmesh_owns_transport_and_control": True,
        "n4_publication_before_data_agent_rebind_required": True,
        "n4_gate_satisfied": False,
        "n4_gate_automatically_enforced_by_compose": False,
        "n6_semantic_bearer_env_name": CONTAINER_NODE_BEARER_TOKEN_ENV,
        "n7_ingress_hmac_env_name": FULL_FLOW_INGRESS_HMAC_SECRET_ENV,
        "semantic_route_ingress_hmac_env_name": (
            FULL_FLOW_INGRESS_HMAC_SECRET_ENV
        ),
        "runtime_environment_names": runtime_env_names,
        "credential_environment_names": credential_names,
        "runtime_environment_values_included": False,
        "credential_values_included": False,
        "concrete_deployment_endpoints_included": False,
        "fixed_internal_health_loopback_only": True,
        "runtime_endpoint_match_requires_preflight": True,
        "compose_rendered": True,
        "docker_invoked": False,
        "services_started": False,
        "workflow_submitted": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["manifest_sha256"] = _sha256(_canonical_bytes(manifest))
    documents = {
        COMPOSE_NAME: compose,
        GATE_NAME: gate_bytes,
        MANIFEST_NAME: _json_bytes(manifest),
    }
    serialized = b"".join(documents.values())
    for binding in [*binding_rows, *runtime_rows]:
        origin = binding.get("base_url")
        if isinstance(origin, str):
            _require(
                origin.encode("utf-8") not in serialized,
                "Compose overlay records a concrete deployment endpoint",
            )
    return documents


def _verified_inputs(
    service_bootstrap_dir: str | Path,
    deployment_binding_dir: str | Path,
    *,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> tuple[
    Path,
    Path,
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, Any],
]:
    bootstrap_root = Path(service_bootstrap_dir).resolve()
    deployment_root = Path(deployment_binding_dir).resolve()
    bootstrap_report = verify_full_flow_local_service_bootstrap(
        bootstrap_root,
        logical_plan_dir=logical_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    deployment_report = verify_full_flow_deployment_binding(
        deployment_root,
        logical_plan_dir=logical_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    _require(
        bootstrap_report.get("full_api_isomorphic_topology_ready") is True
        and bootstrap_report.get("incomplete_contract_count") == 0,
        "service bootstrap is not locally complete",
    )
    _require(
        deployment_report.get("backend") == "single-host-compose",
        "local Compose overlay requires a single-host-compose binding",
    )
    _require(
        deployment_report.get("w4_runtime_bindings_complete") is True
        and deployment_report.get("runtime_service_binding_count") == 2
        and deployment_report.get("pre_upcloud_deployment_schema_ready") is True,
        "local Compose overlay requires v1alpha2 W4 runtime bindings",
    )
    _require(
        bootstrap_report.get("logical_plan_sha256")
        == deployment_report.get("logical_plan_sha256"),
        "bootstrap and deployment bind different logical plans",
    )
    launchers = _strict_jsonl(
        bootstrap_root / LAUNCHERS_NAME,
        "service launchers",
    )
    deployment = _strict_json(
        deployment_root / DEPLOYMENT_BINDING_NAME,
        "deployment binding",
    )
    return (
        bootstrap_root,
        deployment_root,
        bootstrap_report,
        deployment_report,
        launchers,
        deployment,
    )


def render_full_flow_local_compose_overlay(
    service_bootstrap_dir: str | Path,
    deployment_binding_dir: str | Path,
    *,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    overlay_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze one checksum-bound Compose overlay without invoking Docker."""

    inputs = _verified_inputs(
        service_bootstrap_dir,
        deployment_binding_dir,
        logical_plan_dir=logical_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    documents = _documents(
        overlay_id=overlay_id,
        bootstrap_root=inputs[0],
        bootstrap_report=inputs[2],
        deployment_report=inputs[3],
        launchers=inputs[4],
        deployment=inputs[5],
    )
    documents[CHECKSUMS_NAME] = _checksums(documents)
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"Compose overlay already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".full-flow-overlay-", dir=target.parent))
    stage = parent / "overlay"
    try:
        stage.mkdir()
        for name, payload in documents.items():
            path = stage / name
            path.write_bytes(payload)
        verified = verify_full_flow_local_compose_overlay(
            stage,
            service_bootstrap_dir=inputs[0],
            deployment_binding_dir=inputs[1],
            logical_plan_dir=logical_plan_dir,
            scenario_path=scenario_path,
            container_plan_dir=container_plan_dir,
        )
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return {**verified, "output_dir": str(target)}


def verify_full_flow_local_compose_overlay(
    overlay_dir: str | Path,
    *,
    service_bootstrap_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    """Verify checksums and re-derive every byte from the source contracts."""

    root = Path(overlay_dir).resolve()
    _require(root.is_dir(), "Compose overlay directory does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "Compose overlay must contain regular files only",
    )
    _require(
        {path.name for path in entries} == _OUTPUT_NAMES | {CHECKSUMS_NAME},
        "Compose overlay file set changed",
    )
    documents = {name: (root / name).read_bytes() for name in _OUTPUT_NAMES}
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(documents),
        "Compose overlay checksum failed",
    )
    manifest = _strict_json(root / MANIFEST_NAME, "Compose overlay manifest")
    recorded_digest = manifest.get("manifest_sha256")
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    _require(
        isinstance(recorded_digest, str)
        and recorded_digest == _sha256(_canonical_bytes(unsigned)),
        "Compose overlay manifest digest failed",
    )
    inputs = _verified_inputs(
        service_bootstrap_dir,
        deployment_binding_dir,
        logical_plan_dir=logical_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    expected = _documents(
        overlay_id=manifest.get("overlay_id"),
        bootstrap_root=inputs[0],
        bootstrap_report=inputs[2],
        deployment_report=inputs[3],
        launchers=inputs[4],
        deployment=inputs[5],
    )
    for name, payload in expected.items():
        _require(
            documents[name] == payload,
            f"{name} does not match the verified source contracts",
        )
    _require(
        manifest.get("logical_node_ids") == _EXPECTED_NODES
        and manifest.get("logical_node_group_count") == 8,
        "Compose overlay does not map N1--N8 exactly",
    )
    return {
        "status": "VERIFIED_LOCAL_COMPOSE_OVERLAY_NOT_LAUNCHED",
        "overlay_id": manifest["overlay_id"],
        "deployment_id": manifest["deployment_id"],
        "logical_plan_sha256": manifest["logical_plan_sha256"],
        "logical_node_count": 8,
        "compose_service_count": manifest["compose_service_count"],
        "primary_service_count": manifest["primary_service_count"],
        "companion_service_count": manifest["companion_service_count"],
        "w4_flowmesh_coordinator_service_count": manifest[
            "w4_flowmesh_coordinator_service_count"
        ],
        "w4_flowmesh_coordinator_nodes": manifest[
            "w4_flowmesh_coordinator_nodes"
        ],
        "w4_runtime_service_binding_count": manifest[
            "w4_runtime_service_binding_count"
        ],
        "w4_runtime_bindings_complete": manifest[
            "w4_runtime_bindings_complete"
        ],
        "w4_coordinator_deployment_origin_count": manifest[
            "w4_coordinator_deployment_origin_count"
        ],
        "w4_dedicated_cache_service_count": manifest[
            "w4_dedicated_cache_service_count"
        ],
        "w4_cache_namespaces_exclusive": manifest[
            "w4_cache_namespaces_exclusive"
        ],
        "persistent_volume_count": manifest["persistent_volume_count"],
        "n4_operator_gate_required": True,
        "n4_gate_satisfied": False,
        "n6_semantic_bearer_env_name": CONTAINER_NODE_BEARER_TOKEN_ENV,
        "n7_ingress_hmac_env_name": FULL_FLOW_INGRESS_HMAC_SECRET_ENV,
        "runtime_environment_values_included": False,
        "credential_values_included": False,
        "services_started": False,
        "docker_invoked": False,
        "workflow_submitted": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "COMPOSE_GATE_SCHEMA_VERSION",
    "COMPOSE_NAME",
    "COMPOSE_OVERLAY_SCHEMA_VERSION",
    "GATE_NAME",
    "MANIFEST_NAME",
    "FullFlowComposeOverlayError",
    "render_full_flow_local_compose_overlay",
    "verify_full_flow_local_compose_overlay",
]
