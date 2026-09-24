"""Rollback-safe, credential-preserving cutover to the relational cohort.

Run on one named UpCloud node as root. Frozen old containers and all volumes
remain available. Secrets are inherited in process memory and never emitted.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess


PUBLIC = Path("/home/pathfinder/relational-dev-public-20260924-v1")
CONFIG = Path("/home/pathfinder/relational-dev-config-20260924-v1")
ORACLE = (
    "/opt/pathfinder/formal/private/relational-dev-oracle-20260924-v1/"
    "oracle/n1-oracle-package"
)
PREFIX = "pathfinder-full-flow-"
SERVICES = {
    "N1": ["n1-hidden-score", "n1-hidden-score-n1-remote-verification"],
    "N2": ["n2-global-index"],
    "N3": ["n3-raw-data-agent"],
    "N4": ["n4-derived-data-agent"],
    "N7": ["n7-execution-compute"],
}


def _command(args, **kwargs):
    return subprocess.check_output(args, **kwargs)


def _inspect(name):
    return json.loads(_command(["docker", "inspect", name]))[0]


def _exists(kind, name):
    return subprocess.run(
        ["docker", kind, "inspect", name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def _service_names(node):
    return [PREFIX + suffix for suffix in SERVICES[node]]


def rollback(node):
    names = _service_names(node)
    old_names = ["h48" + node.lower() + "-" + name + "-1" for name in names]
    new_names = ["r24" + node.lower() + "-" + name + "-1" for name in names]
    if not all(_exists("container", name) for name in old_names):
        raise ValueError("preserved old container is absent")
    for name in new_names:
        if _exists("container", name):
            subprocess.run(["docker", "stop", name], check=True,
                           stdout=subprocess.DEVNULL)
    for name in old_names:
        _command(["docker", "start", name])
        if _inspect(name)["State"]["Status"] != "running":
            raise ValueError("old container did not resume")
    print(json.dumps({"status": "OLD_H48_CONTAINERS_RESUMED",
                      "node": node, "old_containers": old_names,
                      "new_containers_preserved": True,
                      "credentials_recorded": False}), flush=True)


def run(node, apply):
    names = _service_names(node)
    old_project = "h48" + node.lower()
    old = [_inspect(old_project + "-" + name + "-1") for name in names]
    if not all(item["State"]["Health"]["Status"] == "healthy"
               and item["Config"]["Labels"]["com.docker.compose.project"]
               == old_project
               and item["Config"]["User"] == "10001:10001"
               and item["HostConfig"]["ReadonlyRootfs"] is True
               for item in old):
        raise ValueError("old service identity or health differs")
    if len({item["Image"] for item in old}) != 1:
        raise ValueError("old service images disagree")
    image = old[0]["Image"]
    inherited = {}
    for item in old:
        values = dict(entry.split("=", 1) for entry in item["Config"]["Env"])
        for key, value in values.items():
            if key in inherited and inherited[key] != value:
                raise ValueError("old environment values disagree: " + key)
        inherited.update(values)

    nonsecret = {
        "PATHFINDER_FULL_FLOW_SERVICE_IMAGE": image,
        "PATHFINDER_FULL_FLOW_RUNTIME_UID_GID": "10001:10001",
        "PATHFINDER_FULL_FLOW_BIND_ADDRESS": "10.70.0.1" + node[1],
        "PATHFINDER_FULL_FLOW_NETWORK_NAME": next(
            iter(old[0]["NetworkSettings"]["Networks"])
        ),
    }
    for name, item in zip(names, old):
        ports = item["HostConfig"]["PortBindings"]
        if len(ports) != 1:
            raise ValueError("old service port binding count differs")
        binding = next(iter(ports.values()))
        if len(binding) != 1:
            raise ValueError("old service host port count differs")
        nonsecret[name.upper().replace("-", "_") + "_HOST_PORT"] = (
            binding[0]["HostPort"]
        )
    if node == "N1":
        nonsecret.update(
            PATHFINDER_N1_PACKAGE_DIR=ORACLE,
            PATHFINDER_COMPOSE_N1_HIDDEN_SCORE_STATE_VOLUME=(
                "pathfinder-r24-n1-state-v1"
            ),
        )
    elif node in {"N2", "N3", "N4"}:
        nonsecret["PATHFINDER_" + node + "_PACKAGE_DIR"] = str(
            PUBLIC / "final" / node.lower()
        )
        if node in {"N3", "N4"}:
            kind = "RAW" if node == "N3" else "DERIVED"
            nonsecret[
                f"PATHFINDER_COMPOSE_{node}_{kind}_DATA_AGENT_STATE_VOLUME"
            ] = "pathfinder-r24-" + node.lower() + "-state-v1"
    else:
        paths = {
            "PATHFINDER_LOCAL_SEMANTIC_ADMISSION_DIR": "runtime/admission",
            "PATHFINDER_N1_PUBLIC_COMMITMENT_DIR": "commitment",
            "PATHFINDER_FULL_FLOW_ARTIFACT_BINDING_DIR": "runtime/bindings",
            "PATHFINDER_N2_PACKAGE_DIR": "final/n2",
            "PATHFINDER_N3_PACKAGE_DIR": "final/n3",
            "PATHFINDER_N4_PACKAGE_DIR": "final/n4",
            "PATHFINDER_INTERLEAVED_TRIAL_DAG_DIR": "runtime/dags",
            "PATHFINDER_INTERLEAVED_PLAN_DIR": "plan",
            "PATHFINDER_INTERLEAVED_RAW_PACKAGE_DIR": "build/raw",
            "PATHFINDER_INTERLEAVED_QUERY_DIR": "paid/query",
            "PATHFINDER_INTERLEAVED_VIDEO_INDEX_DIR": "paid/video-index",
            "PATHFINDER_INTERLEAVED_PREPARATION_DIR": "build/preparation",
            "PATHFINDER_INTERLEAVED_CAPTION_DIR": "paid/captions",
        }
        nonsecret.update({key: str(PUBLIC / value)
                          for key, value in paths.items()})
    for key, value in nonsecret.items():
        if key.endswith("_DIR") and not Path(value).is_dir():
            raise ValueError("prepared directory is missing: " + key)

    CONFIG.mkdir(mode=0o700, exist_ok=True)
    env_path = CONFIG / (node.lower() + "-public.env")
    payload = "".join(f"{key}={value}\n"
                      for key, value in sorted(nonsecret.items())).encode()
    if env_path.exists():
        if env_path.read_bytes() != payload:
            raise ValueError("existing nonsecret deployment config differs")
    else:
        env_path.write_bytes(payload)
        env_path.chmod(0o600)

    base_name = (
        "node-formal-bd2da19-" + node.lower()
        + ("-route" if node == "N7" else "") + ".env"
    )
    pilot_name = "pilot-v2.env" if node in {"N3", "N7"} else "pilot.env"
    base = [
        "docker", "compose", "--project-name", "r24" + node.lower(),
        "--env-file", "/opt/pathfinder/env/" + base_name,
        "--env-file", "/home/pathfinder/multiq-config-d328726/" + pilot_name,
        "--env-file", str(env_path),
    ]
    units = [
        base + ["-f", "/opt/pathfinder/deploy/node-bundles/" + node
                + "/compose.service." + name + ".yaml"]
        for name in names
    ]
    combined = units[0]
    if node == "N7":
        combined += [
            "-f", "/home/pathfinder/multiq-config-d328726/n7-route-v2.yaml"
        ]
        overlay = CONFIG / "n7-state.json"
        document = {"volumes": {"multiq-n7-route-state": {
            "name": "pathfinder-r24-n7-route-state-v1"}}}
        encoded = json.dumps(document, sort_keys=True).encode() + b"\n"
        if overlay.exists():
            if overlay.read_bytes() != encoded:
                raise ValueError("existing N7 state override differs")
        else:
            overlay.write_bytes(encoded)
        combined += ["-f", str(overlay)]
    commands = units if node == "N1" else [combined]
    environment = {**os.environ, **inherited, **nonsecret}

    rendered = {"services": {}, "volumes": {}}
    for unit in commands:
        document = json.loads(_command(
            unit + ["--profile", "serve-frozen", "config", "--format", "json"],
            env=environment, stderr=subprocess.STDOUT,
        ))
        rendered["services"].update(document["services"])
        rendered["volumes"].update(document.get("volumes", {}))
    for name, item in zip(names, old):
        service = rendered["services"][name]
        if service["image"] != image or service.get("read_only") is not True:
            raise ValueError("rendered image or read-only root differs")
        current = dict(entry.split("=", 1)
                       for entry in item["Config"]["Env"])
        for key, value in current.items():
            if (("TOKEN" in key or "SECRET" in key)
                    and service["environment"].get(key) != value):
                raise ValueError("credential would change: " + key)
        if any(volume.get("read_only") is not True
               for volume in service.get("volumes", [])
               if volume["type"] == "bind"):
            raise ValueError("package bind is not read-only")
    print(json.dumps({"status": "DEPLOYMENT_RENDER_VERIFIED",
                      "node": node, "image": image,
                      "credentials_unchanged": True}), flush=True)
    if not apply:
        return

    project = "r24" + node.lower()
    new_names = [project + "-" + name + "-1" for name in names]
    if any(_exists("container", name) for name in new_names):
        raise ValueError("fresh container name already exists")
    used_volumes = {
        volume["source"]
        for name in names
        for volume in rendered["services"][name].get("volumes", [])
        if volume["type"] == "volume" and volume.get("source")
    }
    for key in used_volumes:
        named = rendered["volumes"][key]["name"]
        if _exists("volume", named):
            raise ValueError("fresh state volume already exists")

    stopped = []
    try:
        for item in old:
            _command(["docker", "stop", item["Id"]])
            stopped.append(item["Id"])
        for unit, name in zip(commands, names):
            _command(
                unit + ["--profile", "serve-frozen", "up", "-d",
                        "--no-deps", "--wait", name],
                env=environment, stderr=subprocess.STDOUT,
            )
        for name in new_names:
            item = _inspect(name)
            if (item["State"]["Health"]["Status"] != "healthy"
                    or item["Image"] != image or item["RestartCount"] != 0):
                raise ValueError("new service identity or health differs")
        receipt = {
            "status": "HEALTHY", "node": node, "project": project,
            "image": image, "old_container_ids": stopped,
            "new_container_ids": [_inspect(name)["Id"] for name in new_names],
            "old_containers_preserved": True,
            "credentials_unchanged": True, "credentials_recorded": False,
        }
        (CONFIG / (node.lower() + "-receipt.json")).write_bytes(
            json.dumps(receipt, sort_keys=True).encode() + b"\n"
        )
        print(json.dumps(receipt), flush=True)
    except Exception:
        for name in new_names:
            if _exists("container", name):
                subprocess.run(["docker", "stop", name],
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, check=False)
        for identity in stopped:
            _command(["docker", "start", identity])
        print("ROLLED_BACK_TO_PRESERVED_SERVICES", flush=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node", choices=tuple(SERVICES))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    args = parser.parse_args()
    try:
        if args.rollback:
            rollback(args.node)
        else:
            run(args.node, args.apply)
    except Exception as exc:
        # Compose failures can contain credentials. Never print them here.
        print(json.dumps({"status": "DEPLOYMENT_STOPPED",
                          "error_class": type(exc).__name__}), flush=True)
        raise SystemExit(2)
