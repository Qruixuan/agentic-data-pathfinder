"""Compose deployment binding for the first native full-flow simulator path.

The base eight-node package remains the logical infrastructure topology.  This
module adds an N4 Data Agent sidecar and enables the N7 trial coordinator to
reach that sidecar and the N6 semantic service.  It is intentionally an
environment-specific *binding*: the logical route lives in the data-plane
package and can be rendered again for a real multi-host backend.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import re
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from .full_flow_data_plane import verify_full_flow_data_plane_package
from .local_container import verify_local_container_compose


FULL_FLOW_COMPOSE_BINDING_SCHEMA_VERSION = (
    "pathfinder.full-flow-compose-binding/v1alpha1"
)

_FILES = {
    "compose.full-flow.yaml",
    "full-flow-compose-binding.json",
}
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class FullFlowComposeError(ValueError):
    """Raised when a full-flow Compose binding is unsafe or inconsistent."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FullFlowComposeError(message)


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


def _digest(payload: bytes) -> str:
    return sha256(payload).hexdigest()


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_digest(payload)}  {name}\n".encode("utf-8")
        for name, payload in sorted(documents.items())
    )


def _quoted(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _node_names(base_root: Path) -> dict[str, str]:
    try:
        topology = json.loads(
            (base_root / "container_topology.json").read_text(
                encoding="utf-8"
            )
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise FullFlowComposeError(
            "cannot read the base container topology"
        ) from exc
    rows = topology.get("nodes")
    _require(isinstance(rows, list), "base topology nodes are invalid")
    names: dict[str, str] = {}
    for row in rows:
        _require(isinstance(row, Mapping), "base topology node is invalid")
        node_id = row.get("node_id")
        name = row.get("container_name")
        _require(
            isinstance(node_id, str)
            and node_id
            and isinstance(name, str)
            and name,
            "base topology node identity is invalid",
        )
        names[node_id] = name
    _require(len(names) == 8, "full-flow binding requires eight logical nodes")
    for node_id in ("N4", "N6", "N7"):
        _require(node_id in names, f"base topology is missing {node_id}")
    return names


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _route_values(
    data_root: Path,
    *,
    route_id: str,
    data_agent_plan_id: str | None,
    data_agent_plan_epoch: int,
) -> dict[str, Any]:
    route_id = _identifier(route_id, "route_id")
    _require(
        type(data_agent_plan_epoch) is int and data_agent_plan_epoch >= 0,
        "data_agent_plan_epoch must be a non-negative integer",
    )
    data_plane = json.loads(
        (data_root / "full-flow-data-plane.json").read_text(encoding="utf-8")
    )
    plan_ids = sorted({
        plan_id
        for row in data_plane["objects"]
        for plan_id in row["plan_ids"]
    })
    if data_agent_plan_id is None:
        _require(
            len(plan_ids) == 1,
            "data_agent_plan_id is required when the package binds multiple plans",
        )
        selected_plan_id = plan_ids[0]
    else:
        selected_plan_id = _identifier(
            data_agent_plan_id,
            "data_agent_plan_id",
        )
        _require(
            selected_plan_id in plan_ids,
            "data_agent_plan_id is not present in the data-plane package",
        )
    return {
        "route_id": route_id,
        "requested_location": data_plane["route"]["source_location"],
        "data_agent_plan_id": selected_plan_id,
        "data_agent_plan_epoch": data_agent_plan_epoch,
    }


def _overlay_bytes(
    names: Mapping[str, str],
    route: Mapping[str, Any],
) -> bytes:
    source = names["N4"]
    inference = names["N6"]
    executor = names["N7"]
    sidecar = "pathfinder-sim-n4-data-agent"
    lines = [
        "services:",
        f"  {sidecar}:",
        '    image: "pathfinder-simulator-node:semantic-local"',
        f"    container_name: {_quoted(sidecar)}",
        "    command:",
        '      - "serve-data-agent"',
        '      - "--manifest"',
        '      - "/data/config/data-agent-manifest.json"',
        '      - "--operation-db"',
        '      - "/state/data-agent.sqlite3"',
        '      - "--host"',
        '      - "0.0.0.0"',
        '      - "--port"',
        '      - "8780"',
        '      - "--public-base-url"',
        f'      - "http://{sidecar}:8780"',
        '      - "--require-token"',
        '      - "--require-artifact-secret"',
        "    environment:",
        "      - PATHFINDER_DATA_AGENT_TOKEN",
        "      - PATHFINDER_DATA_AGENT_ARTIFACT_SECRET",
        "    read_only: true",
        "    tmpfs:",
        '      - "/tmp:rw,noexec,nosuid,size=64m"',
        "    volumes:",
        (
            '      - "${PATHFINDER_FULL_FLOW_DATA_PLANE_ROOT:'
            '?set PATHFINDER_FULL_FLOW_DATA_PLANE_ROOT}:/data:ro"'
        ),
        f'      - "{sidecar}-state:/state"',
        "    security_opt:",
        '      - "no-new-privileges:true"',
        "    pids_limit: 128",
        '    restart: "no"',
        "    healthcheck:",
        "      test:",
        '        - "CMD"',
        '        - "python"',
        '        - "-c"',
        (
            '        - "import urllib.request; urllib.request.urlopen('
            "'http://127.0.0.1:8780/healthz', timeout=2).read()\""
        ),
        "      interval: 5s",
        "      timeout: 3s",
        "      retries: 12",
        "      start_period: 5s",
        "    networks:",
        "      - pathfinder-infra",
        f"  {executor}:",
        "    depends_on:",
        f"      {sidecar}:",
        "        condition: service_healthy",
        f"      {inference}:",
        "        condition: service_healthy",
        "    environment:",
        "      - PATHFINDER_DATA_AGENT_TOKEN",
        "      - PATHFINDER_CONTAINER_NODE_TOKEN",
        "      - PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
        "      - PATHFINDER_FULL_FLOW_ENABLED=1",
        "      - PATHFINDER_FULL_FLOW_SOURCE_NODE_ID=N4",
        "      - PATHFINDER_FULL_FLOW_EXECUTOR_NODE_ID=N7",
        "      - PATHFINDER_FULL_FLOW_INFERENCE_NODE_ID=N6",
        (
            "      - PATHFINDER_FULL_FLOW_ROUTE_ID="
            f"{route['route_id']}"
        ),
        (
            "      - PATHFINDER_FULL_FLOW_REQUESTED_LOCATION="
            f"{route['requested_location']}"
        ),
        (
            "      - PATHFINDER_FULL_FLOW_DATA_AGENT_PLAN_ID="
            f"{route['data_agent_plan_id']}"
        ),
        (
            "      - PATHFINDER_FULL_FLOW_DATA_AGENT_PLAN_EPOCH="
            f"{route['data_agent_plan_epoch']}"
        ),
        (
            "      - PATHFINDER_FULL_FLOW_DATA_AGENT_BASE_URL="
            f"http://{sidecar}:8780"
        ),
        (
            "      - PATHFINDER_FULL_FLOW_SEMANTIC_BASE_URL="
            f"http://{inference}:9080"
        ),
        (
            "      - PATHFINDER_FULL_FLOW_SIMULATOR_PRIVATE_HOSTS="
            f"{sidecar},{inference}"
        ),
        "    labels:",
        '      pathfinder.logical-node: "N7"',
        '      pathfinder.full-flow-role: "trial-executor"',
        f"  {source}:",
        "    labels:",
        '      pathfinder.logical-node: "N4"',
        '      pathfinder.full-flow-role: "origin-warm"',
        f"  {inference}:",
        "    environment:",
        "      - PATHFINDER_CONTAINER_NODE_TOKEN",
        "    labels:",
        '      pathfinder.logical-node: "N6"',
        '      pathfinder.full-flow-role: "vision-inference"',
        "volumes:",
        f"  {sidecar}-state:",
        "",
    ]
    return "\n".join(lines).encode("utf-8")


def _source_binding(
    base_root: Path,
    data_root: Path,
    route: Mapping[str, Any],
) -> tuple[dict[str, Any], bytes]:
    base_manifest = (base_root / "local_container_manifest.json").read_bytes()
    data_manifest = (data_root / "full-flow-data-plane.json").read_bytes()
    binding = {
        "schema_version": FULL_FLOW_COMPOSE_BINDING_SCHEMA_VERSION,
        "status": "GENERATED_NOT_LAUNCHED",
        "backend": "single-host-docker-compose",
        "base_compose_manifest_sha256": _digest(base_manifest),
        "data_plane_manifest_sha256": _digest(data_manifest),
        "logical_node_count": 8,
        "container_service_count": 9,
        "source_node_id": "N4",
        "executor_node_id": "N7",
        "inference_node_id": "N6",
        "route": dict(route),
        "data_agent_sidecar_count": 1,
        "runtime_data_plane_root_required": True,
        "runtime_authentication_required": True,
        "runtime_secret_names": [
            "PATHFINDER_CONTAINER_NODE_TOKEN",
            "PATHFINDER_DATA_AGENT_ARTIFACT_SECRET",
            "PATHFINDER_DATA_AGENT_TOKEN",
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
        ],
        "credentials_recorded": False,
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
    }
    return binding, _json_bytes(binding)


def build_full_flow_compose_binding(
    base_compose_package: str | Path,
    data_plane_package: str | Path,
    *,
    output_dir: str | Path,
    route_id: str = "full-flow-n4-n7-n6-v1",
    data_agent_plan_id: str | None = None,
    data_agent_plan_epoch: int = 0,
) -> dict[str, Any]:
    """Freeze a non-launching Compose overlay for the native full-flow path."""

    base_root = Path(base_compose_package).resolve()
    data_root = Path(data_plane_package).resolve()
    base_report = verify_local_container_compose(base_root)
    _require(
        base_report.get("service_count") == 8
        and base_report.get("semantic_quality_enabled") is True
        and base_report.get("semantic_executor_node_id") == "N6",
        "base Compose package must enable N6 as the semantic executor",
    )
    data_report = verify_full_flow_data_plane_package(data_root)
    _require(
        data_report.get("source_node_id") == "N4",
        "the first full-flow slice requires an N4 data plane",
    )
    names = _node_names(base_root)
    route = _route_values(
        data_root,
        route_id=route_id,
        data_agent_plan_id=data_agent_plan_id,
        data_agent_plan_epoch=data_agent_plan_epoch,
    )
    binding, binding_bytes = _source_binding(base_root, data_root, route)
    documents = {
        "compose.full-flow.yaml": _overlay_bytes(names, route),
        "full-flow-compose-binding.json": binding_bytes,
    }
    documents["SHA256SUMS"] = _checksums(documents)

    target = Path(output_dir).resolve()
    _require(not target.exists(), f"full-flow binding already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".full-flow-compose-", dir=target.parent))
    staging = parent / "output"
    try:
        staging.mkdir()
        for name, payload in documents.items():
            path = staging / name
            with path.open("wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
        verify_full_flow_compose_binding(
            staging,
            base_compose_package=base_root,
            data_plane_package=data_root,
        )
        os.replace(staging, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return {**binding, "output_dir": str(target)}


def verify_full_flow_compose_binding(
    output_dir: str | Path,
    *,
    base_compose_package: str | Path,
    data_plane_package: str | Path,
) -> dict[str, Any]:
    """Verify an overlay and its exact base/data-plane bindings offline."""

    root = Path(output_dir).resolve()
    base_root = Path(base_compose_package).resolve()
    data_root = Path(data_plane_package).resolve()
    _require(root.is_dir(), "full-flow Compose binding directory is missing")
    base_report = verify_local_container_compose(base_root)
    _require(
        base_report.get("service_count") == 8
        and base_report.get("semantic_quality_enabled") is True
        and base_report.get("semantic_executor_node_id") == "N6",
        "base Compose package must enable N6 as the semantic executor",
    )
    data_report = verify_full_flow_data_plane_package(data_root)
    actual = {
        path.name
        for path in root.iterdir()
        if path.is_file() and path.name != "SHA256SUMS"
    }
    _require(actual == _FILES, "full-flow Compose binding file set changed")
    lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    expected_checksums = {
        name: _digest((root / name).read_bytes()) for name in _FILES
    }
    parsed: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _FILES and name not in parsed,
            "full-flow Compose checksum row is invalid",
        )
        parsed[name] = digest
    _require(parsed == expected_checksums, "full-flow Compose checksums changed")

    raw_binding = json.loads(
        (root / "full-flow-compose-binding.json").read_text(encoding="utf-8")
    )
    _require(
        isinstance(raw_binding.get("route"), Mapping),
        "full-flow Compose route binding is missing",
    )
    route = _route_values(
        data_root,
        route_id=raw_binding["route"].get("route_id"),
        data_agent_plan_id=raw_binding["route"].get("data_agent_plan_id"),
        data_agent_plan_epoch=raw_binding["route"].get(
            "data_agent_plan_epoch"
        ),
    )
    _require(
        raw_binding["route"].get("requested_location")
        == route["requested_location"],
        "full-flow requested location changed",
    )
    binding, binding_bytes = _source_binding(base_root, data_root, route)
    _require(
        (root / "full-flow-compose-binding.json").read_bytes()
        == binding_bytes,
        "full-flow Compose source binding changed",
    )
    _require(
        (root / "compose.full-flow.yaml").read_bytes()
        == _overlay_bytes(_node_names(base_root), route),
        "full-flow Compose overlay changed",
    )
    _require(
        data_report.get("source_node_id") == "N4",
        "full-flow data plane is not assigned to N4",
    )
    return {
        **binding,
        "status": "VERIFIED_NOT_LAUNCHED",
        "checked_files": len(_FILES),
    }
