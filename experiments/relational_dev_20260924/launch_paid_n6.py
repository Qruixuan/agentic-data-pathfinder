"""N6-only bounded public preparation; provider credentials remain in memory."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


STAGE = Path("/home/pathfinder/relational-dev-paid-20260924-v1")
CONTAINER = (
    "pathfinder-multiq-d328726-n6-"
    "pathfinder-full-flow-n6-semantic-inference-1"
)
PROTOCOL_SHA256 = (
    "f944899d127e385de4672805115beb248651d9899c56e193fbc2acfd30c5fca8"
)


def _run(image: str, environment: dict[str, str],
         command: list[str], network: bool) -> int:
    args = [
        "docker", "run", "--rm", "--read-only", "--user", "10001:10001",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--network", "bridge" if network else "none",
        "--tmpfs", "/tmp:rw,nosuid,nodev,size=128m,mode=1777",
        "--mount", f"type=bind,src={STAGE}/source,dst=/work,readonly",
        "--mount", f"type=bind,src={STAGE}/input,dst=/input,readonly",
        "--mount", f"type=bind,src={STAGE}/protocol,dst=/protocol,readonly",
        "--mount", f"type=bind,src={STAGE}/output,dst=/output",
        "--workdir", "/work", "-e", "PYTHONPATH=/work",
    ]
    if network:
        for name in (
            "PATHFINDER_SEMANTIC_LLM_BASE_URL",
            "PATHFINDER_SEMANTIC_LLM_API_KEY",
            "PATHFINDER_SEMANTIC_LLM_MODEL",
        ):
            args.extend(["-e", name])
    args.extend(["--entrypoint", "python", image, *command])
    return subprocess.run(args, env=environment, check=False).returncode


def main() -> int:
    raw = (STAGE / "protocol/execution-protocol.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != PROTOCOL_SHA256:
        raise ValueError("frozen execution protocol differs")
    protocol = json.loads(raw)
    info = json.loads(subprocess.check_output(["docker", "inspect", CONTAINER]))[0]
    if info["State"]["Health"]["Status"] != "healthy":
        raise ValueError("existing N6 service is not healthy")
    values = dict(item.split("=", 1) for item in info["Config"]["Env"])
    names = (
        "PATHFINDER_SEMANTIC_LLM_BASE_URL",
        "PATHFINDER_SEMANTIC_LLM_API_KEY",
        "PATHFINDER_SEMANTIC_LLM_MODEL",
    )
    if (any(not values.get(name) for name in names)
            or values[names[2]] != protocol["caption_model"]
            or not values[names[0]].startswith("https://")):
        raise ValueError("N6 provider configuration differs from protocol")
    environment = {**os.environ, **{name: values[name] for name in names}}
    image = info["Image"]
    del info, values

    verify = (
        "from pathlib import Path;"
        "from experiments.multiq_prepare import verify_sums;"
        "from pathfinder.rsi_exam.temporal_index_collection import "
        "verify_formal_temporal_index_preparation;"
        "verify_sums(Path('/input/build/raw'));"
        "verify_formal_temporal_index_preparation("
        "Path('/input/build/preparation'));"
        "print('NO_CALL_SOURCE_PREFLIGHT_VERIFIED')"
    )
    if _run(image, environment, ["-c", verify], network=False):
        raise ValueError("no-call source preflight failed")
    print(json.dumps({"status": "PAID_PREPARATION_GATES_VERIFIED",
                      "credentials_recorded": False}), flush=True)

    for phase in ("captions", "video-index", "query"):
        command = [
            "-m", "experiments.multiq_prepare", phase,
            "--input-dir", "/input", "--output-dir", "/output",
            "--source-commit", "b02ac75daf5ed206a2bb7e15d47f5d1b8fa189d3",
            "--physical-host", "pathfinder-n6",
            "--protocol", "/protocol/execution-protocol.json",
        ]
        code = _run(image, environment, command, network=True)
        if code:
            print(json.dumps({"status": "PREPARATION_STOPPED",
                              "phase": phase, "exit_code": code}), flush=True)
            return code
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "LAUNCHER_FAILED",
                          "error_class": type(exc).__name__}), flush=True)
        sys.exit(2)
