"""Run the shared interleaved batch CLI with existing runtime credentials.

No credential is written to disk or passed as a command-line value.  The
launcher reads only the named healthy route and FlowMesh worker containers,
then exports the three required values to a short-lived runner process.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess


def _inspect(name: str) -> dict:
    return json.loads(subprocess.check_output(
        ["docker", "inspect", name], stderr=subprocess.DEVNULL,
    ))[0]


def _env(item: dict) -> dict[str, str]:
    return dict(entry.split("=", 1) for entry in item["Config"]["Env"])


def launch(args: argparse.Namespace) -> int:
    route = _inspect(args.route_container)
    worker = _inspect(args.worker_container)
    if (route["State"]["Health"]["Status"] != "healthy"
            or worker["State"]["Health"]["Status"] != "healthy"):
        raise ValueError("route or worker is not healthy")
    if (route["Image"] != args.route_image
            or not args.image.startswith("sha256:")
            or not args.route_image.startswith("sha256:")):
        raise ValueError("route or runner image is not digest-pinned")
    route_env = _env(route)
    worker_env = _env(worker)
    names = (
        "FLOWMESH_BASE_URL", "FLOWMESH_API_KEY",
        "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
    )
    values = {
        "FLOWMESH_BASE_URL": worker_env["FLOWMESH_BASE_URL"],
        "FLOWMESH_API_KEY": worker_env.get("FLOWMESH_API_KEY", ""),
        "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": route_env[
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"],
    }
    if not values["FLOWMESH_BASE_URL"] or not values[
        "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"
    ]:
        raise ValueError("required runtime credential name is empty")
    source = args.source.resolve()
    root = args.artifact_root.resolve()
    config = args.config_dir.resolve()
    if (not (source / "experiments" / "interleaved_batch.py").is_file()
            or not (config / "batch-config.json").is_file()
            or not config.is_relative_to(root)):
        raise ValueError("source or frozen configuration is absent")
    command = [
        "docker", "run", "--rm", "--network", "host", "--read-only",
        "--user", "10001:10001", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        "--tmpfs", "/tmp:rw,nosuid,nodev,size=256m,mode=1777",
        "--mount", f"type=bind,src={source},dst=/work,readonly",
        "--mount", f"type=bind,src={root},dst={root},readonly",
        "--workdir", "/work", "-e", "PYTHONPATH=/work",
    ]
    for name in names:
        command.extend(("-e", name))
    output = args.output_dir.resolve() if args.output_dir else None
    if args.action in {"execute", "verify"}:
        if output is None or not output.parent.is_dir():
            raise ValueError("output parent directory is absent")
        if args.action == "execute" and output.exists():
            raise ValueError("execution output directory already exists")
        command.extend(("--mount", "type=bind,src=" + str(output.parent)
                        + ",dst=" + str(output.parent)))
    command.extend((
        "--entrypoint", "python", args.image, "-m",
        "experiments.interleaved_batch", args.action,
        "--config-dir", str(config), "--artifact-root", str(root),
    ))
    if output is not None and args.action in {"execute", "verify"}:
        command.extend(("--output-dir", str(output)))
    if args.action == "verify" and args.seal:
        command.append("--seal")
    environment = {**os.environ, **values}
    print(json.dumps({"action": args.action,
                      "route_health": "healthy", "worker_health": "healthy",
                      "runner_image": args.image,
                      "credentials_recorded": False}), flush=True)
    return subprocess.run(command, env=environment, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("preflight", "execute", "verify"))
    parser.add_argument("--route-container", required=True)
    parser.add_argument("--worker-container", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--route-image", required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--config-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--seal", action="store_true")
    args = parser.parse_args()
    try:
        return launch(args)
    except Exception as exc:
        # Never serialize exception text: SDK/Docker failures may embed values.
        print(json.dumps({"status": "BATCH_LAUNCH_STOPPED",
                          "class": type(exc).__name__,
                          "credentials_recorded": False}), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
