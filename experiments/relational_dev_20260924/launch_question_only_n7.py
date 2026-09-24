"""Bounded text-only control using existing N7-to-N6/N1 credentials in memory."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlsplit


ROUTE = "r24n7-pathfinder-full-flow-n7-execution-compute-1"
PUBLIC = "/home/pathfinder/relational-dev-public-20260924-v1"
STATE_ROOT = Path("/home/pathfinder/relational-dev-question-only-20260924-v1")
PROTOCOL = Path("/tmp/relational-dev-question-only-protocol.json")


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"network-probe", "preflight", "run"}:
        raise ValueError("question-only action is unsupported")
    action = sys.argv[1]
    if hashlib.sha256(PROTOCOL.read_bytes()).hexdigest() != (
        "56b92f0d93e1e81422d126fdaa8e8078ced380816f9c37cdca336977dbaeb411"
    ):
        raise ValueError("frozen question-only protocol differs")
    route = json.loads(subprocess.check_output(["docker", "inspect", ROUTE]))[0]
    if route["State"]["Health"]["Status"] != "healthy":
        raise ValueError("N7 route is not healthy")
    env = dict(value.split("=", 1) for value in route["Config"]["Env"])
    names = (
        "PATHFINDER_N6_SEMANTIC_BASE_URL", "PATHFINDER_N1_ORACLE_BASE_URL",
        "PATHFINDER_CONTAINER_NODE_TOKEN", "PATHFINDER_N1_ORACLE_TOKEN",
    )
    if any(not env.get(name) for name in names):
        raise ValueError("existing N1/N6 credentials or endpoints missing")
    environment = {**os.environ, **{name: env[name] for name in names}}
    image = route["Image"]
    network = next(iter(route["NetworkSettings"]["Networks"]))
    extra_hosts = route["HostConfig"].get("ExtraHosts") or []
    mapped = set()
    for item in extra_hosts:
        name, address = item.rsplit(":", 1)
        if (re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", name) is None
                or ipaddress.ip_address(address)
                not in ipaddress.ip_network("10.70.0.0/24")
                or name in mapped):
            raise ValueError("inherited cross-host mapping is invalid")
        mapped.add(name)
    required_hosts = {
        urlsplit(env[name]).hostname for name in names[:2]
    }
    if not required_hosts <= mapped:
        raise ValueError("N1/N6 origins lack inherited cross-host mappings")
    if action == "run":
        if STATE_ROOT.exists():
            raise ValueError("question-only recovery directory already exists")
        STATE_ROOT.mkdir(mode=0o700)
        os.chown(STATE_ROOT, 10001, 10001)
    command = [
        "docker", "run", "--rm", "--network", network, "--read-only",
        "--user", "10001:10001", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m,mode=1777",
        "--mount", f"type=bind,src={PUBLIC},dst=/public,readonly",
        "--mount", "type=bind,src=/tmp/relational-dev-question-only-protocol.json,dst=/protocol.json,readonly",
        "--mount", "type=bind,src=/tmp/relational-dev-question-only-probe.py,dst=/tool.py,readonly",
        "--mount", "type=bind,src=/tmp/relational-dev-question-only-network.py,dst=/network.py,readonly",
    ]
    for item in extra_hosts:
        command += ["--add-host", item]
    if action == "run":
        command += ["--mount", f"type=bind,src={STATE_ROOT},dst=/state"]
    for name in names:
        command += ["-e", name]
    command += [
        "--entrypoint", "python", image,
        "/network.py" if action == "network-probe" else "/tool.py",
    ]
    if action != "network-probe":
        command += [action,
        "--protocol", "/protocol.json",
        "--questions", "/public/plan/public-questions.jsonl",
        "--commitment", "/public/commitment/n1-oracle-preselection-commitment.json",
        ]
    if action == "run":
        command += ["--output-dir", "/state/observations"]
    return subprocess.run(command, env=environment, check=False).returncode


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "QUESTION_ONLY_LAUNCH_STOPPED",
                          "error_class": type(exc).__name__}))
        sys.exit(2)
