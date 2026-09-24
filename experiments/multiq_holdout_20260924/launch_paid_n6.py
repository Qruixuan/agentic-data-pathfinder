"""N6-only launcher: reuse its credential in memory, never in argv/files."""
import json
import os
from pathlib import Path
import subprocess
import sys

STAGE = Path("/home/pathfinder/h48-paid-20260924-v1")
CONTAINER = "pathfinder-multiq-d328726-n6-pathfinder-full-flow-n6-semantic-inference-1"
SOURCE_COMMIT = "a00bb13"


def main():
    # Captured output stays in this process; no environment dump is emitted.
    info = json.loads(subprocess.check_output(["docker", "inspect", CONTAINER]))[0]
    if info["State"]["Health"]["Status"] != "healthy":
        raise RuntimeError("existing N6 service is not healthy")
    values = dict(item.split("=", 1) for item in info["Config"]["Env"])
    names = ("PATHFINDER_SEMANTIC_LLM_BASE_URL", "PATHFINDER_SEMANTIC_LLM_API_KEY",
             "PATHFINDER_SEMANTIC_LLM_MODEL")
    if any(not values.get(name) for name in names):
        raise RuntimeError("existing N6 provider configuration is incomplete")
    if values[names[2]] != "qwen3.8-27b" or not values[names[0]].startswith("https://"):
        raise RuntimeError("provider model or TLS differs from protocol")
    environment = {**os.environ, **{name: values[name] for name in names}}
    image = info["Image"]
    del info, values
    print(json.dumps({"status": "PAID_PREPARATION_CONFIG_VERIFIED",
                      "image": image, "credentials_recorded": False}), flush=True)
    for phase in ("captions", "video-index", "query"):
        command = [
            "docker", "run", "--rm", "--read-only", "--user", "10001:10001",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m,mode=1777",
            "--mount", f"type=bind,src={STAGE}/source,dst=/work,readonly",
            "--mount", f"type=bind,src={STAGE}/input,dst=/input,readonly",
            "--mount", f"type=bind,src={STAGE}/output,dst=/output",
            "--workdir", "/work", "-e", "PYTHONPATH=/work",
        ]
        for name in names:
            command.extend(["-e", name])
        command.extend([
            "--entrypoint", "python", image, "-m", "experiments.multiq_prepare",
            phase, "--input-dir", "/input", "--output-dir", "/output",
            "--source-commit", SOURCE_COMMIT, "--physical-host", "pathfinder-n6",
            "--protocol", "/work/experiments/multiq_holdout_20260924/execution-protocol.json",
        ])
        result = subprocess.run(command, env=environment, check=False)
        if result.returncode:
            print(json.dumps({"status": "PREPARATION_STOPPED", "phase": phase,
                              "exit_code": result.returncode}), flush=True)
            return result.returncode
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "LAUNCHER_FAILED", "class": type(exc).__name__}))
        sys.exit(2)
