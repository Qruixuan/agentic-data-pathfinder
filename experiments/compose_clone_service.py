"""Start an isolated Compose service from a healthy predecessor.

The JSON spec contains only public deployment parameters.  Credential values
are inherited in process memory from the predecessor, never serialized.
This helper deliberately leaves the predecessor running and preserves every
container and volume on failure.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess


def _run(argv: list[str], *, env: dict[str, str] | None = None) -> bytes:
    return subprocess.check_output(argv, env=env, stderr=subprocess.STDOUT)


def _inspect(kind: str, name: str) -> dict:
    return json.loads(_run(["docker", kind, "inspect", name]))[0]


def _exists(kind: str, name: str) -> bool:
    return subprocess.run(
        ["docker", kind, "inspect", name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
    ).returncode == 0


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def deploy(spec: dict, *, apply: bool) -> dict:
    service = spec["service"]
    project = spec["project"]
    predecessor = _inspect("container", spec["predecessor"])
    _require(predecessor["State"]["Health"]["Status"] == "healthy",
             "predecessor is not healthy")
    _require(predecessor["Config"]["Labels"]["com.docker.compose.service"]
             == service, "predecessor service identity differs")
    _require(predecessor["Config"]["User"] == "10001:10001"
             and predecessor["HostConfig"]["ReadonlyRootfs"] is True,
             "predecessor security profile differs")
    image = spec.get("image") or predecessor["Image"]
    _require(image.startswith("sha256:"), "image must be digest-pinned")
    _require(_exists("image", image), "pinned image is absent")
    inherited = dict(entry.split("=", 1)
                     for entry in predecessor["Config"]["Env"])
    networks = predecessor["NetworkSettings"]["Networks"]
    _require(len(networks) == 1,
             "predecessor network identity is ambiguous")
    overrides = spec["overrides"]
    _require(all(isinstance(key, str) and isinstance(value, str)
                 for key, value in overrides.items()),
             "deployment overrides must be strings")
    _require(not any("TOKEN" in key or "SECRET" in key or "KEY" in key
                     for key in overrides),
             "credential override is prohibited")
    environment = {
        **os.environ, **inherited, **overrides,
        "PATHFINDER_FULL_FLOW_SERVICE_IMAGE": image,
        "PATHFINDER_FULL_FLOW_RUNTIME_UID_GID": "10001:10001",
        "PATHFINDER_FULL_FLOW_BIND_ADDRESS": spec["bind_address"],
        "PATHFINDER_FULL_FLOW_NETWORK_NAME": next(iter(networks)),
    }
    fragments = [str(Path(path).resolve()) for path in spec["fragments"]]
    _require(all(Path(path).is_file() for path in fragments),
             "a Compose fragment is missing")
    command = ["docker", "compose", "--project-name", project]
    for env_file in spec.get("env_files", []):
        _require(Path(env_file).is_file(), "a deployment env file is missing")
        command.extend(("--env-file", env_file))
    for fragment in fragments:
        command.extend(("-f", fragment))
    rendered = json.loads(_run(command + ["--profile", "serve-frozen",
                                         "config", "--format", "json"],
                               env=environment))
    unit = rendered["services"][service]
    _require(unit["image"] == image and unit["user"] == "10001:10001"
             and unit.get("read_only") is True
             and unit.get("init") is True
             and "ALL" in unit.get("cap_drop", [])
             and "no-new-privileges:true" in unit.get("security_opt", []),
             "rendered service security or image differs")
    _require(all(mount.get("read_only") is True
                 for mount in unit.get("volumes", [])
                 if mount["type"] == "bind"),
             "rendered package bind is writable")
    current = dict(entry.split("=", 1)
                   for entry in predecessor["Config"]["Env"])
    _require(all(unit["environment"].get(key) == value
                 for key, value in current.items()
                 if "TOKEN" in key or "SECRET" in key or "API_KEY" in key),
             "rendered credential differs from predecessor")
    _require(len(unit.get("ports", [])) == 1
             and unit["ports"][0]["published"] == str(spec["host_port"])
             and unit["ports"][0]["host_ip"] == spec["bind_address"],
             "rendered published origin differs")
    for mount in unit.get("volumes", []):
        if mount["type"] == "bind":
            _require(Path(mount["source"]).is_dir(),
                     "a source-bound package directory is absent")
    used = {mount["source"] for mount in unit.get("volumes", [])
            if mount["type"] == "volume"}
    for key in used:
        volume = rendered["volumes"][key]["name"]
        if _exists("volume", volume):
            details = _inspect("volume", volume)
            _require(
                volume in spec.get("reuse_state_volumes", [])
                and details.get("Labels", {}).get("com.docker.compose.project")
                == project,
                "an unapproved state volume already exists",
            )
    new_name = f"{project}-{service}-1"
    _require(not _exists("container", new_name),
             "a supposedly fresh container already exists")
    receipt = {
        "status": "RENDER_VERIFIED", "service": service,
        "project": project, "image": image,
        "predecessor_id": predecessor["Id"],
        "credentials_unchanged": True, "credentials_recorded": False,
        "predecessor_preserved": True,
    }
    if not apply:
        return receipt
    _run(command + ["--profile", "serve-frozen", "up", "-d",
                    "--no-deps", "--wait", service], env=environment)
    created = _inspect("container", new_name)
    _require(created["Image"] == _inspect("image", image)["Id"]
             and created["State"]["Health"]["Status"] == "healthy"
             and created["RestartCount"] == 0,
             "new container image or health differs")
    receipt.update(status="HEALTHY", container_id=created["Id"])
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        spec = json.loads(args.spec.read_text(encoding="utf-8"))
        print(json.dumps(deploy(spec, apply=args.apply), sort_keys=True))
        return 0
    except Exception as exc:
        # Docker/Compose exception strings can contain environment values.
        print(json.dumps({"status": "DEPLOYMENT_STOPPED",
                          "error_class": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
