"""Run an existing multi-question preparer image with its N6 credentials.

The provider values are borrowed in memory from one healthy N6 container.
They never enter argv, an env file, a receipt, or printed Docker inspection.
The default action performs an offline, network-disabled source check only.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


NAMES = (
    "PATHFINDER_SEMANTIC_LLM_BASE_URL",
    "PATHFINDER_SEMANTIC_LLM_API_KEY",
    "PATHFINDER_SEMANTIC_LLM_MODEL",
)
PHASES = ("captions", "video-index", "query")


def _container(name: str) -> dict:
    info = json.loads(subprocess.check_output(
        ["docker", "inspect", name], stderr=subprocess.DEVNULL,
    ))
    if len(info) != 1 or info[0]["State"]["Health"]["Status"] != "healthy":
        raise ValueError("the pinned N6 service is not healthy")
    return info[0]


def _command(stage: Path, image: str, *, paid: bool) -> list[str]:
    command = [
        "docker", "run", "--rm", "--read-only", "--user", "10001:10001",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m,mode=1777",
        "--mount", f"type=bind,src={stage}/source,dst=/work,readonly",
        "--mount", f"type=bind,src={stage}/input,dst=/input,readonly",
        "--workdir", "/work", "-e", "PYTHONPATH=/work",
    ]
    if paid:
        command.extend((
            "--mount", f"type=bind,src={stage}/output,dst=/output",
        ))
        for name in NAMES:
            command.extend(("-e", name))
    else:
        command.extend(("--network", "none"))
    command.extend(("--entrypoint", "python", image))
    return command


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", type=Path, required=True)
    parser.add_argument("--n6-container", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--phase", choices=PHASES)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    stage = args.stage.resolve()
    if (not stage.is_relative_to(Path("/home/pathfinder").resolve())
            or not re.fullmatch(r"[0-9a-f]{40}", args.source_commit)
            or (args.execute != (args.phase is not None))):
        raise ValueError("paid preparation path, source, or action is invalid")
    for name in ("source/experiments/multiq_prepare.py", "input/plan",
                 "input/build/preparation", "input/protocol.json"):
        if not (stage / name).exists():
            raise ValueError("the staged public preparation is incomplete")
    info = _container(args.n6_container)
    image = info["Image"]
    if not args.execute:
        check = (
            "import json;from pathlib import Path;"
            "from pathfinder.rsi_exam.temporal_index_collection import "
            "verify_formal_temporal_index_preparation as v;"
            "from pathfinder.rsi_exam.ten_route_multiq_plan import "
            "load_verified_multiq_plan as p;"
            "root=Path('/input');"
            "prep=v(root/'build/preparation');"
            "plan,questions,_=p(root/'plan');"
            "protocol=json.loads((root/'protocol.json').read_bytes());"
            "assert prep['window_count']==protocol['caption_windows'];"
            "assert plan['plan_sha256']==protocol['plan_sha256'];"
            "assert len(questions)==protocol['question_count'];"
            "print('PUBLIC_PREPARATION_VERIFIED')"
        )
        result = subprocess.run(
            _command(stage, image, paid=False) + ["-c", check],
            check=False,
        )
        return result.returncode
    values = dict(item.split("=", 1) for item in info["Config"]["Env"])
    protocol = json.loads((stage / "input/protocol.json").read_bytes())
    if (any(not values.get(name) for name in NAMES)
            or values[NAMES[2]] != protocol["caption_model"]
            or not values[NAMES[0]].startswith("https://")):
        raise ValueError("the existing N6 provider configuration differs")
    environment = {**os.environ, **{name: values[name] for name in NAMES}}
    del values, info
    command = _command(stage, image, paid=True) + [
        "-m", "experiments.multiq_prepare", args.phase,
        "--input-dir", "/input", "--output-dir", "/output",
        "--source-commit", args.source_commit,
        "--physical-host", "pathfinder-n6",
        "--protocol", "/input/protocol.json",
    ]
    return subprocess.run(command, env=environment, check=False).returncode


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"status": "PREPARATION_LAUNCHER_FAILED",
                          "error_class": type(exc).__name__,
                          "credentials_recorded": False}))
        sys.exit(2)
