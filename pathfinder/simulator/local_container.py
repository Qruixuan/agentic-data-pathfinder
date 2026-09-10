"""Local Docker Compose packaging and read-only host preflight.

This module deliberately separates generation from execution.  Generating a
Compose project never probes Docker, starts a service, or changes host state.
The preflight is read-only and reports missing prerequisites without trying to
install or repair them.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from .container_contract import verify_container_backend_plan


LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.local-container-compose-package/v1alpha3"
)
PREVIOUS_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.local-container-compose-package/v1alpha2"
)
LEGACY_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.local-container-compose-package/v1alpha1"
)
LOCAL_CONTAINER_PREFLIGHT_SCHEMA_VERSION = (
    "pathfinder.local-container-host-preflight/v1alpha1"
)

_COPIED_INPUTS = {
    "container_operations.jsonl",
    "container_topology.json",
}
_OUTPUT_FILES = _COPIED_INPUTS | {
    "compose.yaml",
    "container_endpoints.json",
    "local_container_manifest.json",
}


class LocalContainerError(ValueError):
    """Raised when a local container package is malformed or unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise LocalContainerError(message)


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


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _yaml_scalar(value: str) -> str:
    # JSON strings are valid YAML scalars and avoid ambiguous YAML values.
    return json.dumps(value, ensure_ascii=False)


def _compose_bytes(
    nodes: list[Mapping[str, Any]],
    *,
    host_port_base: int,
    semantic_executor_node_id: str | None,
    semantic_artifact_source_node_ids: tuple[str, ...],
    semantic_runtime_build: bool,
) -> tuple[bytes, dict[str, Any]]:
    source_node_ids = set(semantic_artifact_source_node_ids)
    source_container_names = {
        str(node["container_name"])
        for node in nodes
        if str(node["node_id"]) in source_node_ids
    }
    endpoints: dict[str, Any] = {}
    lines = [
        'name: "pathfinder-infra-local"',
        "services:",
    ]
    for offset, node in enumerate(nodes, start=1):
        node_id = str(node["node_id"])
        service = str(node["container_name"])
        image_ref = str(node["image_ref"])
        image_digest = node.get("image_digest")
        if semantic_runtime_build:
            # The frozen simulator image intentionally contains the
            # infrastructure-only runtime.  A semantic smoke must rebuild a
            # local development image from the checked-out source; pretending
            # that its digest is the frozen image's digest would be false
            # provenance.
            image = "pathfinder-simulator-node:semantic-local"
            needs_build = True
        else:
            image = (
                f"{image_ref}@{image_digest}"
                if isinstance(image_digest, str)
                else image_ref
            )
            needs_build = image_digest is None
        host_port = host_port_base + offset
        endpoints[node_id] = {
            "container_name": service,
            "container_url": f"http://{service}:9080",
            "host_health_url": f"http://127.0.0.1:{host_port}/healthz",
            "host_operation_url": (
                f"http://127.0.0.1:{host_port}/v1/operations/execute"
            ),
            "host_semantic_url": (
                f"http://127.0.0.1:{host_port}/v1/semantic/chat-completions"
            ),
        }
        lines.append(f"  {service}:")
        if needs_build:
            lines.extend([
                "    build:",
                '      context: "${PATHFINDER_REPO_ROOT:?set PATHFINDER_REPO_ROOT}"',
                '      dockerfile: "containers/pathfinder-infra-node/Dockerfile"',
            ])
        command_lines = [
            f"    image: {_yaml_scalar(image)}",
            f"    container_name: {_yaml_scalar(service)}",
            "    command:",
            '      - "serve-container-node"',
            '      - "--node-id"',
            f"      - {_yaml_scalar(node_id)}",
            '      - "--state-dir"',
            '      - "/state"',
            '      - "--host"',
            '      - "0.0.0.0"',
            '      - "--port"',
            '      - "9080"',
            '      - "--max-operation-bytes"',
            '      - "1073741824"',
        ]
        if node_id == semantic_executor_node_id:
            command_lines.extend([
                '      - "--enable-semantic-llm"',
            ])
            for container_name in sorted(source_container_names):
                command_lines.extend([
                    '      - "--semantic-allowed-source-container"',
                    f"      - {_yaml_scalar(container_name)}",
                ])
        if node_id in source_node_ids:
            command_lines.extend([
                '      - "--semantic-artifact-root"',
                '      - "/artifacts"',
            ])
        lines.extend(command_lines)
        if node_id == semantic_executor_node_id:
            # Bare environment names deliberately inherit values at `docker
            # compose up` time.  The generated package contains no secret,
            # endpoint, or model value.
            lines.extend([
                "    environment:",
                "      - PATHFINDER_SEMANTIC_LLM_BASE_URL",
                "      - PATHFINDER_SEMANTIC_LLM_MODEL",
                "      - PATHFINDER_SEMANTIC_LLM_API_KEY",
                "      - PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS",
            ])
        lines.extend([
            "    read_only: true",
            "    tmpfs:",
            '      - "/tmp:rw,noexec,nosuid,size=64m"',
            "    volumes:",
            f"      - {_yaml_scalar(service + '-state:/state')}",
        ])
        if node_id in source_node_ids:
            lines.extend([
                '      - "${PATHFINDER_SEMANTIC_ARTIFACT_ROOT:?set PATHFINDER_SEMANTIC_ARTIFACT_ROOT}:/artifacts:ro"',
            ])
        lines.extend([
            "    security_opt:",
            '      - "no-new-privileges:true"',
            "    pids_limit: 128",
            "    restart: \"no\"",
            "    ports:",
            f'      - "127.0.0.1:{host_port}:9080"',
            "    healthcheck:",
            "      test:",
            '        - "CMD"',
            '        - "python"',
            '        - "-c"',
            "        - \"import urllib.request; "
            "urllib.request.urlopen('http://127.0.0.1:9080/healthz', "
            "timeout=2).read()\"",
            "      interval: 5s",
            "      timeout: 3s",
            "      retries: 12",
            "      start_period: 5s",
            "    networks:",
            "      - pathfinder-infra",
        ])
    lines.extend([
        "networks:",
        "  pathfinder-infra:",
        '    name: "pathfinder-infra-local"',
        "    driver: bridge",
        "volumes:",
    ])
    for node in nodes:
        service = str(node["container_name"])
        lines.append(f"  {service}-state:")
    lines.extend([
        "",
    ])
    return "\n".join(lines).encode("utf-8"), endpoints


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )


def _verify_checksums(root: Path) -> dict[str, str]:
    expected = _OUTPUT_FILES
    actual = {
        path.name
        for path in root.iterdir()
        if path.is_file() and path.name != "SHA256SUMS"
    }
    _require(actual == expected, "local Compose package file set changed")
    checksums: dict[str, str] = {}
    try:
        lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise LocalContainerError("local Compose checksums are missing") from exc
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed checksum row")
        _require(name not in checksums, f"duplicate checksum entry: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"local Compose checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(set(checksums) == expected, "local Compose checksums are incomplete")
    return checksums


def build_local_container_compose(
    container_plan_dir: str | Path,
    *,
    output_dir: str | Path,
    host_port_base: int = 19080,
    semantic_executor_node_id: str | None = None,
    semantic_artifact_source_node_ids: Iterable[str] = (),
) -> dict[str, Any]:
    """Create an eight-service Compose project without invoking Docker."""

    _require(
        type(host_port_base) is int
        and 1024 <= host_port_base
        and host_port_base + 8 <= 65535,
        "host_port_base must reserve eight non-privileged ports",
    )
    source = Path(container_plan_dir).resolve()
    verified = verify_container_backend_plan(source)
    topology = json.loads(
        (source / "container_topology.json").read_text(encoding="utf-8")
    )
    nodes = topology.get("nodes")
    _require(isinstance(nodes, list) and len(nodes) == 8, "exactly eight nodes required")
    node_ids = {str(node.get("node_id")) for node in nodes}
    requested_source_node_ids = tuple(semantic_artifact_source_node_ids)
    source_node_ids = tuple(sorted(set(requested_source_node_ids)))
    _require(
        len(source_node_ids) == len(requested_source_node_ids),
        "semantic artifact source nodes contain duplicates",
    )
    _require(
        semantic_executor_node_id is None
        or semantic_executor_node_id in node_ids,
        "semantic_executor_node_id must name a container node",
    )
    _require(
        set(source_node_ids) <= node_ids,
        "semantic artifact source node must name a container node",
    )
    _require(
        not source_node_ids or semantic_executor_node_id is not None,
        "semantic artifact sources require a semantic executor node",
    )
    semantic_runtime_build = semantic_executor_node_id is not None
    compose, endpoint_rows = _compose_bytes(
        nodes,
        host_port_base=host_port_base,
        semantic_executor_node_id=semantic_executor_node_id,
        semantic_artifact_source_node_ids=source_node_ids,
        semantic_runtime_build=semantic_runtime_build,
    )
    endpoints = {
        "schema_version": "pathfinder.local-container-endpoints/v1alpha1",
        "backend_id": verified["backend_id"],
        "scenario_id": verified["scenario_id"],
        "endpoints": endpoint_rows,
        "credentials_recorded": False,
    }
    documents: dict[str, bytes] = {
        "compose.yaml": compose,
        "container_endpoints.json": _json_bytes(endpoints),
        "container_operations.jsonl": (
            source / "container_operations.jsonl"
        ).read_bytes(),
        "container_topology.json": (
            source / "container_topology.json"
        ).read_bytes(),
    }
    manifest = {
        "schema_version": LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
        "status": "GENERATED_NOT_LAUNCHED",
        "backend_id": verified["backend_id"],
        "scenario_id": verified["scenario_id"],
        "portable_plan_sha256": verified["portable_plan_sha256"],
        "planned_trial_count": verified["planned_trial_count"],
        "planned_operation_count": verified["planned_operation_count"],
        "service_count": len(nodes),
        "pinned_image_count": (
            0
            if semantic_runtime_build
            else sum(node.get("image_digest") is not None for node in nodes)
        ),
        "image_pinning_enforced": (
            False
            if semantic_runtime_build
            else all(node.get("image_digest") is not None for node in nodes)
        ),
        "build_context_included": (
            semantic_runtime_build
            or any(node.get("image_digest") is None for node in nodes)
        ),
        "host_port_base": host_port_base,
        "docker_probed": False,
        "docker_called": False,
        "container_started": False,
        "semantic_quality_enabled": semantic_executor_node_id is not None,
        "semantic_runtime_build": semantic_runtime_build,
        "semantic_runtime_image": (
            "pathfinder-simulator-node:semantic-local"
            if semantic_runtime_build
            else None
        ),
        "semantic_executor_node_id": semantic_executor_node_id,
        "semantic_artifact_source_node_ids": list(source_node_ids),
        "semantic_llm_credentials_bound": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["local_container_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = _checksum_bytes(documents)

    target = Path(output_dir).resolve()
    _require(not target.exists(), f"local Compose output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".compose-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        verify_local_container_compose(staging)
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    return {
        **manifest,
        "output_dir": str(target),
        "compose_path": str(target / "compose.yaml"),
    }


def verify_local_container_compose(output_dir: str | Path) -> dict[str, Any]:
    """Verify a generated local Compose project without calling Docker."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"local Compose output does not exist: {root}")
    checksums = _verify_checksums(root)
    manifest = json.loads(
        (root / "local_container_manifest.json").read_text(encoding="utf-8")
    )
    _require(
        manifest.get("schema_version") in (
            LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
            PREVIOUS_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
            LEGACY_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
        ),
        "unsupported local Compose manifest schema_version",
    )
    _require(manifest.get("status") == "GENERATED_NOT_LAUNCHED", "bad status")
    _require(manifest.get("docker_called") is False, "generator called Docker")
    _require(manifest.get("container_started") is False, "generator launched a container")
    semantic_quality_enabled = manifest.get("semantic_quality_enabled", False)
    _require(
        type(semantic_quality_enabled) is bool,
        "semantic quality flag is invalid",
    )
    semantic_executor_node_id = manifest.get("semantic_executor_node_id")
    _require(
        semantic_executor_node_id is None or isinstance(semantic_executor_node_id, str),
        "semantic executor node is invalid",
    )
    raw_source_node_ids = manifest.get("semantic_artifact_source_node_ids", [])
    _require(
        isinstance(raw_source_node_ids, list)
        and all(isinstance(value, str) and bool(value) for value in raw_source_node_ids),
        "semantic artifact source nodes are invalid",
    )
    _require(
        len(raw_source_node_ids) == len(set(raw_source_node_ids)),
        "semantic artifact source nodes contain duplicates",
    )
    semantic_artifact_source_node_ids = tuple(sorted(raw_source_node_ids))
    _require(
        manifest.get("semantic_llm_credentials_bound", False) is False,
        "Compose package must not bind semantic LLM credentials",
    )
    semantic_runtime_build = manifest.get("semantic_runtime_build", False)
    _require(
        type(semantic_runtime_build) is bool,
        "semantic runtime build flag is invalid",
    )
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "local_container_manifest.json"
        },
        "local Compose manifest digests disagree",
    )
    endpoints = json.loads(
        (root / "container_endpoints.json").read_text(encoding="utf-8")
    )
    rows = endpoints.get("endpoints")
    _require(isinstance(rows, dict) and len(rows) == 8, "endpoint count changed")
    compose = (root / "compose.yaml").read_text(encoding="utf-8")
    topology = json.loads(
        (root / "container_topology.json").read_text(encoding="utf-8")
    )
    nodes = topology.get("nodes")
    _require(isinstance(nodes, list) and len(nodes) == 8, "topology node count changed")
    node_ids = {str(node.get("node_id")) for node in nodes}
    _require(
        (semantic_executor_node_id is not None) is semantic_quality_enabled,
        "semantic quality configuration is inconsistent",
    )
    _require(
        semantic_executor_node_id is None or semantic_executor_node_id in node_ids,
        "semantic executor node is not in the topology",
    )
    _require(
        set(semantic_artifact_source_node_ids) <= node_ids,
        "semantic artifact source node is not in the topology",
    )
    _require(
        not semantic_artifact_source_node_ids or semantic_executor_node_id is not None,
        "semantic artifact sources lack an executor",
    )
    if manifest["schema_version"] == LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION:
        pinned_count = sum(node.get("image_digest") is not None for node in nodes)
        expected_pinned_count = 0 if semantic_runtime_build else pinned_count
        _require(
            manifest.get("pinned_image_count") == expected_pinned_count,
            "pinned image count changed",
        )
        _require(
            manifest.get("image_pinning_enforced") is (
                not semantic_runtime_build and pinned_count == len(nodes)
            ),
            "image pinning enforcement flag changed",
        )
        _require(
            manifest.get("build_context_included") is (
                semantic_runtime_build or pinned_count < len(nodes)
            ),
            "build context flag changed",
        )
        _require(
            compose.count("    build:") == (
                len(nodes) if semantic_runtime_build else len(nodes) - pinned_count
            ),
            "Compose build blocks do not match unpinned nodes",
        )
        compose_lines = compose.splitlines()
        service_starts = [
            compose_lines.index(f"  {node['container_name']}:")
            for node in nodes
        ]
        network_start = compose_lines.index("networks:")
        for index, node in enumerate(nodes):
            image_ref = str(node["image_ref"])
            image_digest = node.get("image_digest")
            expected_image = (
                "pathfinder-simulator-node:semantic-local"
                if semantic_runtime_build
                else (
                    f"{image_ref}@{image_digest}"
                    if isinstance(image_digest, str)
                    else image_ref
                )
            )
            block_end = (
                service_starts[index + 1]
                if index + 1 < len(service_starts)
                else network_start
            )
            service_block = compose_lines[service_starts[index]:block_end]
            _require(
                f"    image: {_yaml_scalar(expected_image)}" in service_block,
                f"Compose image identity changed for {node['node_id']}",
            )
            _require(
                ("    build:" in service_block)
                is (semantic_runtime_build or image_digest is None),
                f"Compose build policy changed for {node['node_id']}",
            )
            semantic_enabled = node["node_id"] == semantic_executor_node_id
            _require(
                ('      - "--enable-semantic-llm"' in service_block) is semantic_enabled,
                f"Compose semantic executor policy changed for {node['node_id']}",
            )
            if semantic_enabled:
                for environment_name in (
                    "PATHFINDER_SEMANTIC_LLM_BASE_URL",
                    "PATHFINDER_SEMANTIC_LLM_MODEL",
                    "PATHFINDER_SEMANTIC_LLM_API_KEY",
                    "PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS",
                ):
                    _require(
                        f"      - {environment_name}" in service_block,
                        f"missing semantic runtime environment passthrough: {environment_name}",
                    )
                expected_sources = [
                    str(source["container_name"])
                    for source in nodes
                    if str(source["node_id"]) in semantic_artifact_source_node_ids
                ]
                for source_name in sorted(expected_sources):
                    _require(
                        f"      - {_yaml_scalar(source_name)}" in service_block,
                        f"missing semantic allowed source container: {source_name}",
                    )
            is_artifact_source = node["node_id"] in semantic_artifact_source_node_ids
            _require(
                ('      - "--semantic-artifact-root"' in service_block)
                == is_artifact_source,
                f"Compose semantic artifact source policy changed for {node['node_id']}",
            )
            if is_artifact_source:
                _require(
                    any(
                        "PATHFINDER_SEMANTIC_ARTIFACT_ROOT" in line
                        and "/artifacts:ro" in line
                        for line in service_block
                    ),
                    f"missing semantic artifact mount for {node['node_id']}",
                )
    for node_id, endpoint in rows.items():
        container_name = endpoint["container_name"]
        _require(f"  {container_name}:" in compose, f"missing service for {node_id}")
        _require(
            endpoint["container_url"] == f"http://{container_name}:9080",
            f"bad container endpoint for {node_id}",
        )
        if "host_semantic_url" in endpoint:
            _require(
                endpoint.get("host_semantic_url")
                == endpoint["host_operation_url"].replace(
                    "/v1/operations/execute",
                    "/v1/semantic/chat-completions",
                ),
                f"bad semantic endpoint for {node_id}",
            )
        else:
            _require(
                semantic_executor_node_id is None,
                f"missing semantic endpoint for {node_id}",
            )
    return {
        "status": "VERIFIED_NOT_LAUNCHED",
        "backend_id": manifest["backend_id"],
        "scenario_id": manifest["scenario_id"],
        "service_count": manifest["service_count"],
        "pinned_image_count": manifest.get("pinned_image_count"),
        "image_pinning_enforced": manifest.get("image_pinning_enforced"),
        "build_context_included": manifest.get("build_context_included"),
        "planned_trial_count": manifest["planned_trial_count"],
        "planned_operation_count": manifest["planned_operation_count"],
        "semantic_quality_enabled": semantic_quality_enabled,
        "semantic_runtime_build": semantic_runtime_build,
        "semantic_executor_node_id": semantic_executor_node_id,
        "semantic_artifact_source_node_ids": list(semantic_artifact_source_node_ids),
        "checked_files": len(_OUTPUT_FILES),
        "docker_called": False,
        "container_started": False,
        "eligible_for_scientific_claims": False,
    }


def _probe_command(command: list[str]) -> dict[str, Any]:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "available": False,
            "exit_code": None,
            "detail": type(exc).__name__,
        }
    output = (result.stdout or result.stderr).strip()
    return {
        "available": result.returncode == 0,
        "exit_code": result.returncode,
        "detail": output[:500],
    }


def preflight_local_container_host() -> dict[str, Any]:
    """Read-only Docker/Compose capability check for the current host."""

    docker = shutil.which("docker")
    docker_probe = {
        "available": False,
        "exit_code": None,
        "detail": "docker executable not found",
    }
    compose_probe = {
        "available": False,
        "exit_code": None,
        "detail": "docker executable not found",
    }
    if docker is not None:
        docker_probe = _probe_command([
            docker,
            "version",
            "--format",
            "{{json .Server.Version}}",
        ])
        compose_probe = _probe_command([
            docker,
            "compose",
            "version",
            "--short",
        ])
    blockers: list[str] = []
    if docker is None:
        blockers.append("docker_cli_missing")
    elif not docker_probe["available"]:
        blockers.append("docker_engine_unavailable")
    if not compose_probe["available"]:
        blockers.append("docker_compose_v2_unavailable")
    ready = not blockers
    return {
        "schema_version": LOCAL_CONTAINER_PREFLIGHT_SCHEMA_VERSION,
        "status": "READY" if ready else "BLOCKED",
        "platform": platform.system(),
        "machine": platform.machine(),
        "docker_executable_found": docker is not None,
        "docker_engine": docker_probe,
        "docker_compose_v2": compose_probe,
        "blockers": blockers,
        "read_only_probe": True,
        "package_installed": False,
        "service_started": False,
        "credentials_recorded": False,
    }
