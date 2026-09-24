"""Pass existing FlowMesh and ingress credentials to the shared runner in memory."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


STAGE = Path("/home/pathfinder/relational-dev-runner-20260924-v1")
PUBLIC = Path("/home/pathfinder/relational-dev-public-20260924-v1")
ROUTE = "r24n7-pathfinder-full-flow-n7-execution-compute-1"
RUNNER_IMAGE = (
    "sha256:9cc1202c88d14665ffdce135421092449d2172f01f4087ed5455bb88eab713b1"
)


def _inspect(name):
    return json.loads(subprocess.check_output(["docker", "inspect", name]))[0]


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {
        "check", "preflight", "execute", "verify"
    }:
        raise ValueError("runner action is not supported")
    action = sys.argv[1]
    route = _inspect(ROUTE)
    worker = _inspect("pathfinder_costaware_20260815a")
    if (route["State"]["Health"]["Status"] != "healthy"
            or worker["State"]["Health"]["Status"] != "healthy"):
        raise ValueError("route or worker is not healthy")
    route_env = dict(x.split("=", 1) for x in route["Config"]["Env"])
    worker_env = dict(x.split("=", 1) for x in worker["Config"]["Env"])
    selected = {
        "FLOWMESH_BASE_URL": worker_env["FLOWMESH_BASE_URL"],
        "FLOWMESH_API_KEY": worker_env["FLOWMESH_API_KEY"],
        "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET":
            route_env["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"],
    }
    if (not selected["FLOWMESH_BASE_URL"]
            or not selected["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"]):
        raise ValueError("runtime ingress or FlowMesh configuration missing")
    environment = {**os.environ, **selected}
    command = [
        "docker", "run", "--rm", "--network", "host", "--read-only",
        "--user", "10001:10001", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        "--tmpfs", "/tmp:rw,nosuid,nodev,size=256m,mode=1777",
        "--mount", f"type=bind,src={STAGE}/source,dst=/work,readonly",
        "--mount", f"type=bind,src={PUBLIC},dst={PUBLIC},readonly",
        "--mount", f"type=bind,src={STAGE}/output,dst=/output",
        "--workdir", "/work", "-e", "PYTHONPATH=/work",
    ]
    for name in selected:
        command.extend(["-e", name])
    command.extend([
        "--entrypoint", "python", RUNNER_IMAGE,
        "-m", "experiments.batch", action,
        "--config-dir", str(PUBLIC / "runtime/config"),
        "--artifact-root", str(PUBLIC),
    ])
    if action in {"execute", "verify"}:
        command.extend(["--output-dir", "/output/routes"])
    if action == "verify":
        command.append("--seal")
    return subprocess.run(command, env=environment, check=False).returncode


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "BATCH_LAUNCH_FAILED",
                          "error_class": type(exc).__name__}), flush=True)
        sys.exit(2)
