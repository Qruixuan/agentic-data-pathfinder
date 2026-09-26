"""Preflight, then optionally run one isolated public Qwen PPD sample on N7.

Only sanitized runner JSON is emitted. Execute is a separate explicit mode;
preflight never submits a FlowMesh workflow or calls an LLM.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import subprocess
from pathlib import Path


PACKAGE = Path("/home/pathfinder/upcloud-ppd-engineering-20260926-v4")
OPERATOR = Path("/home/pathfinder/ppd-deploy-20260926-v1/operator")
RUNNER = OPERATOR / "run_engineering_session_v5.py"
QUESTION = OPERATOR / "engineering-q5.txt"
OUT_ROOT = Path("/home/pathfinder/ppd-runs")
ENV_FILE = Path("/home/pathfinder/ppd-deploy-20260926-v1/secrets/gateway.env")
SDK_SITE = Path("/opt/flowmesh/venv/lib/python3.12/site-packages")
GATEWAY = "pathfinder-ppd-gateway-v1"
WORKER = "pathfinder_ppd_visual_20260926e"
RUNNER_SHA256 = "78e9a97fc4ccee804cffba02af1da12084b43faeb56150dbf36c3f9fb4dfd5c4"
QUESTION_SHA256 = "570b67ec01c1a78c65952657de2f95db58b27dd07595211529e4b035513d7841"
DEFAULT_GATEWAY_IMAGE = "sha256:7e86efd423bc332b6908f6041f39aa5a834fc34f192a24b6abd88ed787f1ae52"
WORKER_IMAGE = "sha256:f8f977fe69cea83a950c60f50b40c1787f332e3084d81f142c181336c9e299eb"
EXPECTED_HOSTS = {
    "pathfinder-full-flow-ppd-n3-raw:10.70.0.13",
    "pathfinder-full-flow-ppd-n4-remote:10.70.0.14",
    "pathfinder-full-flow-ppd-n4-n7-replica:10.70.0.17",
    "pathfinder-full-flow-ppd-n6-semantic:10.70.0.16",
}


def _run(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=False)


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    return _run("sudo", "-n", "docker", *args)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _static_preflight(run_id: str, gateway_image: str) -> list[str]:
    if socket.gethostname() != "pathfinder-n7":
        raise RuntimeError("operator host differs")
    if not re.fullmatch(r"ppd-qwen-first-[a-z0-9-]{20,80}", run_id):
        raise RuntimeError("fresh run identity format differs")
    if (OUT_ROOT / run_id).exists():
        raise RuntimeError("output directory already exists")
    if _sha(RUNNER) != RUNNER_SHA256 or _sha(QUESTION) != QUESTION_SHA256:
        raise RuntimeError("runner or public question bytes differ")
    if not ENV_FILE.is_file() or not SDK_SITE.is_dir():
        raise RuntimeError("operator environment or SDK site is missing")
    if not PACKAGE.is_dir():
        raise RuntimeError("frozen PPD package is missing")
    for relative in (Path("."), Path("n3"), Path("n4")):
        directory = PACKAGE / relative
        if not (directory / "SHA256SUMS").is_file():
            raise RuntimeError("frozen package checksum list is missing")
        checked = _run("sha256sum", "-c", "SHA256SUMS", cwd=directory)
        if checked.returncode:
            raise RuntimeError("frozen package checksum verification failed")
    for name, image in (
        (GATEWAY, gateway_image),
        (WORKER, WORKER_IMAGE),
    ):
        state = _docker("inspect", "--format",
                        "{{.State.Status}}:{{.State.Health.Status}}:"
                        "{{.RestartCount}}:{{.Image}}", name)
        if state.returncode or state.stdout.strip() != f"running:healthy:0:{image}":
            raise RuntimeError("participating service state differs")
    mounts = _docker("inspect", "--format", "{{json .Mounts}}", GATEWAY)
    if mounts.returncode:
        raise RuntimeError("Gateway mounts unavailable")
    package_mount = next(
        (row for row in json.loads(mounts.stdout)
         if row.get("Destination") == "/opt/pathfinder/ppd"),
        None,
    )
    if package_mount is None or (
        package_mount.get("Type"), package_mount.get("Source"),
        package_mount.get("RW"),
    ) != ("bind", str(PACKAGE), False):
        raise RuntimeError("Gateway does not mount the verified v4 package")
    generic = _docker("inspect", "--format", "{{.State.Health.Status}}",
                      "pathfinder_costaware_20260815a")
    if generic.returncode or generic.stdout.strip() != "healthy":
        raise RuntimeError("existing generic worker is not healthy")
    hosts = _docker("inspect", "--format", "{{json .HostConfig.ExtraHosts}}",
                    GATEWAY)
    if hosts.returncode:
        raise RuntimeError("Gateway host mappings unavailable")
    aliases = json.loads(hosts.stdout)
    if not isinstance(aliases, list) or set(aliases) != EXPECTED_HOSTS:
        raise RuntimeError("Gateway private host mappings differ")
    with socket.create_connection(("127.0.0.1", 18765), timeout=3):
        pass
    return aliases


def _container_command(
    mode: str, run_id: str, aliases: list[str], gateway_image: str,
) -> list[str]:
    command = [
        "sudo", "-n", "docker", "run", "--rm", "--init", "--network", "host",
        "--read-only", "--user", "10001:10001", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges:true",
        "--tmpfs", "/tmp:rw,nosuid,size=64m",
        "--env-file", str(ENV_FILE),
        "-e", "PYTHONPATH=/app:/tmp/fm",
        "--volumes-from", GATEWAY,
        "--mount", f"type=bind,src={RUNNER},dst=/opt/runner.py,readonly",
        "--mount", f"type=bind,src={QUESTION},dst=/opt/q5.txt,readonly",
        "--mount", f"type=bind,src={SDK_SITE},dst=/tmp/fm,readonly",
    ]
    for item in aliases:
        command.extend(("--add-host", item))
    if mode == "execute":
        output = OUT_ROOT / run_id
        created = _run("sudo", "-n", "install", "-d", "-m", "700", "-o",
                       "10001", "-g", "10001", str(output))
        if created.returncode:
            raise RuntimeError("fresh output directory creation failed")
        command.extend(("--mount", f"type=bind,src={output},dst=/out"))
    command.extend((
        "--entrypoint", "python", gateway_image, "-P", "/opt/runner.py", mode,
        "--config", "/opt/pathfinder/ppd/system.json",
        "--endpoint-registry", "/opt/pathfinder/ppd/endpoint-registry.json",
        "--state-db", "/state/gateway.sqlite3",
        "--question-file", "/opt/q5.txt",
        "--design", "PPD_REMOTE_DIGEST",
        "--object-id", "nextqa-val-11584566583",
        "--trial-id", run_id, "--session-id", run_id,
        "--worker-alias", WORKER,
        "--agent-config-name", "pathfinder_video_visual_qwen_first_offer",
    ))
    if mode == "execute":
        command.extend(("--receipt", "/out/receipt.json"))
    return command


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("preflight", "execute"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--gateway-image", default=DEFAULT_GATEWAY_IMAGE)
    args = parser.parse_args()
    try:
        aliases = _static_preflight(args.run_id, args.gateway_image)
        outcome = _run(*_container_command(
            args.mode, args.run_id, aliases, args.gateway_image,
        ))
        objects = []
        for line in outcome.stdout.splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict) and "status" in value:
                objects.append(value)
        if not objects:
            raise RuntimeError("runner returned no sanitized status")
        selected = objects[-1]
        print(json.dumps({**selected, "native_exit_status": outcome.returncode},
                         sort_keys=True))
        return outcome.returncode
    except Exception as exc:
        print(json.dumps({"status": "BLOCKED_BEFORE_OR_DURING_RUN",
                          "error_class": type(exc).__name__,
                          "run_id": args.run_id}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
