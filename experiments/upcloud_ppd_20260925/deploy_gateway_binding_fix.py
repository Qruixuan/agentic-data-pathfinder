"""Rebind only the isolated PPD Gateway to the verified v4 package.

No FlowMesh workflow, Data Agent access, or model inference is performed.
The previous Gateway container and state volume are preserved for rollback.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import time
from pathlib import Path


NAME = "pathfinder-ppd-gateway-v1"
BACKUP = "pathfinder-ppd-gateway-v1-before-binding-fix-20260926"
FAILED = "pathfinder-ppd-gateway-v1-failed-binding-fix-20260926"
IMAGE = "sha256:7e86efd423bc332b6908f6041f39aa5a834fc34f192a24b6abd88ed787f1ae52"
OLD_PACKAGE = Path("/home/pathfinder/upcloud-ppd-engineering-20260926-v3")
NEW_PACKAGE = Path("/home/pathfinder/upcloud-ppd-engineering-20260926-v4")
ENV_FILE = Path("/home/pathfinder/ppd-deploy-20260926-v1/secrets/gateway.env")
VOLUME = "pathfinder-ppd-gateway-state-v1"
ALIASES = {
    "pathfinder-full-flow-ppd-n3-raw:10.70.0.13",
    "pathfinder-full-flow-ppd-n4-remote:10.70.0.14",
    "pathfinder-full-flow-ppd-n4-n7-replica:10.70.0.17",
    "pathfinder-full-flow-ppd-n6-semantic:10.70.0.16",
}
COMMAND = [
    "serve-flowmesh-tools",
    "--config", "/opt/pathfinder/ppd/system.json",
    "--endpoint-registry", "/opt/pathfinder/ppd/endpoint-registry.json",
    "--state-db", "/state/gateway.sqlite3",
    "--host", "0.0.0.0", "--port", "8765",
]


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["sudo", "-n", "docker", *args], check=check,
        capture_output=True, text=True, timeout=45,
    )


def field(name: str, expression: str) -> object:
    result = docker(
        "inspect", "--format", "{{json " + expression + "}}", name,
    )
    return json.loads(result.stdout)


def absent(name: str) -> bool:
    return docker("inspect", name, check=False).returncode != 0


def health(name: str) -> tuple[str, str]:
    return str(field(name, ".State.Status")), str(field(name, ".State.Health.Status"))


def verify_sums(package: Path) -> None:
    for subdir in (Path("."), Path("n3"), Path("n4")):
        root = package / subdir
        lines = (root / "SHA256SUMS").read_text(encoding="ascii").splitlines()
        if not lines:
            raise RuntimeError("empty PPD checksum manifest")
        for line in lines:
            digest, relative = line.split("  ", 1)
            target = Path(relative.lstrip("*"))
            if target.is_absolute() or ".." in target.parts:
                raise RuntimeError("PPD checksum path escapes package")
            actual = hashlib.sha256((root / target).read_bytes()).hexdigest()
            if actual != digest:
                raise RuntimeError("PPD package checksum differs")


def active_sessions() -> int:
    code = (
        "import sqlite3; "
        "c=sqlite3.connect('file:/state/gateway.sqlite3?mode=ro', uri=True); "
        "print(c.execute(\"SELECT count(*) FROM gateway_sessions "
        "WHERE status IN ('CREATED','RUNNING')\").fetchone()[0])"
    )
    return int(docker("exec", NAME, "python", "-c", code).stdout.strip())


def verify_binding_in_image() -> None:
    code = (
        "from pathlib import Path; "
        "from pathfinder.config import load_config; "
        "from pathfinder.distributed.registry import load_endpoint_registry; "
        "from pathfinder.data_agent_manifest import load_data_agent_manifest; "
        "r=Path('/opt/pathfinder/ppd'); "
        "c=load_config(r/'system.json'); "
        "g=load_endpoint_registry(r/'endpoint-registry.json'); "
        "m={e:load_data_agent_manifest(r/('n3' if e=='n3_raw' else 'n4')/"
        "'config'/'data-agent-manifest.json') for e in g.endpoint_ids}; "
        "pairs=[(d,p,g.route(design_id=d,representation_id=p).endpoint_id) "
        "for d,v in c.designs.items() for p in v.paths]; "
        "[m[e].resolve(plan_id=d,object_id='nextqa-val-11584566583',"
        "representation_id=p,requested_location=c.designs[d].paths[p].location) "
        "for d,p,e in pairs]; "
        "assert len(pairs)==6; print('BINDINGS_OK_6')"
    )
    result = docker(
        "run", "--rm", "--network", "none", "--read-only",
        "--user", "10001:10001", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges:true",
        "--mount", f"type=bind,src={NEW_PACKAGE},dst=/opt/pathfinder/ppd,readonly",
        "--entrypoint", "python", IMAGE, "-P", "-c", code,
    )
    if result.stdout.strip() != "BINDINGS_OK_6":
        raise RuntimeError("in-image PPD cross-binding verification failed")


def verify_mounts(name: str, package: Path) -> None:
    mounts = field(name, ".Mounts")
    package_mount = next(
        (m for m in mounts if m["Destination"] == "/opt/pathfinder/ppd"), None
    )
    state_mount = next((m for m in mounts if m["Destination"] == "/state"), None)
    if len(mounts) != 2 or not package_mount or not state_mount:
        raise RuntimeError("Gateway mount set differs")
    if (package_mount["Type"], package_mount["Source"], package_mount["RW"]) != (
        "bind", str(package), False,
    ):
        raise RuntimeError("Gateway package mount differs")
    if (state_mount["Type"], state_mount["Name"], state_mount["RW"]) != (
        "volume", VOLUME, True,
    ):
        raise RuntimeError("Gateway state volume differs")


def preflight() -> dict[str, object]:
    if os.uname().nodename != "pathfinder-n7":
        raise RuntimeError("operator is not on N7")
    if not absent(BACKUP) or not absent(FAILED):
        raise RuntimeError("rollback or failed Gateway name already exists")
    if health(NAME) != ("running", "healthy"):
        raise RuntimeError("current Gateway is not healthy")
    if field(NAME, ".Image") != IMAGE:
        raise RuntimeError("current Gateway image differs")
    if field(NAME, ".Config.Cmd") != COMMAND:
        raise RuntimeError("current Gateway command differs")
    if field(NAME, ".Config.User") != "10001:10001":
        raise RuntimeError("Gateway user differs")
    if field(NAME, ".HostConfig.ReadonlyRootfs") is not True:
        raise RuntimeError("Gateway root filesystem differs")
    if field(NAME, ".HostConfig.NetworkMode") != "bridge":
        raise RuntimeError("Gateway network mode differs")
    if field(NAME, ".HostConfig.CapDrop") != ["ALL"]:
        raise RuntimeError("Gateway capabilities differ")
    if field(NAME, ".HostConfig.SecurityOpt") != ["no-new-privileges:true"]:
        raise RuntimeError("Gateway security policy differs")
    if set(field(NAME, ".HostConfig.ExtraHosts")) != ALIASES:
        raise RuntimeError("Gateway private host aliases differ")
    if field(NAME, ".HostConfig.PortBindings") != {
        "8765/tcp": [{"HostIp": "127.0.0.1", "HostPort": "18765"}],
    }:
        raise RuntimeError("Gateway host port differs")
    verify_mounts(NAME, OLD_PACKAGE)
    if not ENV_FILE.is_file() or stat.S_IMODE(ENV_FILE.stat().st_mode) != 0o600:
        raise RuntimeError("Gateway credential file mode differs")
    verify_sums(NEW_PACKAGE)
    verify_binding_in_image()
    count = active_sessions()
    if count:
        raise RuntimeError("active Gateway sessions prohibit cutover")
    return {
        "status": "PREFLIGHT_OK", "active_sessions": count,
        "image": IMAGE, "new_package": str(NEW_PACKAGE),
        "credential_values_read": False, "workflow_submitted": False,
        "llm_called": False,
    }


def start_new() -> str:
    command = [
        "run", "--detach", "--name", NAME, "--network", "bridge",
        "--restart", "no", "--init", "--read-only", "--user", "10001:10001",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
        "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m",
        "--publish", "127.0.0.1:18765:8765",
        "--mount", f"type=bind,src={NEW_PACKAGE},dst=/opt/pathfinder/ppd,readonly",
        "--mount", f"type=volume,src={VOLUME},dst=/state",
        "--env-file", str(ENV_FILE),
        "-e", "PATHFINDER_PPD_TOOL_TRACE=1",
        "--health-interval", "10s", "--health-timeout", "3s",
        "--health-retries", "12",
        "--health-cmd", (
            "python -c 'import socket; "
            "s=socket.create_connection((\"127.0.0.1\",8765),2); "
            "s.close()'"
        ),
    ]
    for alias in sorted(ALIASES):
        command.extend(("--add-host", alias))
    command.extend((IMAGE, *COMMAND))
    return docker(*command).stdout.strip()


def wait_healthy(name: str) -> bool:
    for _ in range(45):
        if health(name) == ("running", "healthy"):
            return True
        time.sleep(2)
    return False


def apply() -> dict[str, object]:
    result = preflight()
    old_id = str(field(NAME, ".Id"))
    docker("rename", NAME, BACKUP)
    try:
        docker("stop", BACKUP)
        new_id = start_new()
        if not wait_healthy(NAME):
            raise RuntimeError("replacement Gateway failed health")
        if field(NAME, ".Image") != IMAGE:
            raise RuntimeError("replacement Gateway image differs")
        verify_mounts(NAME, NEW_PACKAGE)
        result.update({
            "status": "BINDING_FIXED_GATEWAY_HEALTHY",
            "old_container_id": old_id,
            "new_container_id": new_id,
            "old_container_preserved": True,
            "state_volume_preserved": True,
        })
        return result
    except Exception:
        if not absent(NAME):
            if field(NAME, ".State.Running"):
                docker("stop", NAME)
            docker("rename", NAME, FAILED)
        docker("rename", BACKUP, NAME)
        docker("start", NAME)
        if not wait_healthy(NAME):
            raise RuntimeError("replacement failed and rollback unhealthy")
        raise RuntimeError("replacement failed; previous Gateway restored")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(apply() if args.apply else preflight(), sort_keys=True))
    except Exception as exc:
        print(json.dumps({
            "status": "BLOCKED", "error_class": type(exc).__name__,
            "credential_values_read": False, "workflow_submitted": False,
            "llm_called": False,
        }, sort_keys=True))
        raise SystemExit(1) from None
