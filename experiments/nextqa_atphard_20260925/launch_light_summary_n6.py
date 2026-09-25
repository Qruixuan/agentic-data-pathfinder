"""Run the one-summary-per-video freezer with N6 credentials in memory only.

This helper lives on N6. It never displays, hashes, serializes or places a
credential value on a command line. The running N6 service is not modified.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess


N6 = (
    "pathfinder-multiq-d328726-n6-"
    "pathfinder-full-flow-n6-semantic-inference-1"
)
N6_IMAGE = (
    "sha256:002415af005a0cd0159a71b367f2eca3d5044fa238d73cd1b8a5ed8972bd3f05"
)
PROVIDER_KEYS = (
    "PATHFINDER_SEMANTIC_LLM_MODEL",
    "PATHFINDER_SEMANTIC_LLM_BASE_URL",
    "PATHFINDER_SEMANTIC_LLM_API_KEY",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage-root", required=True, type=Path)
    root = parser.parse_args().stage_root.resolve()
    try:
        details = json.loads(subprocess.check_output(
            ["sudo", "-n", "docker", "inspect", N6],
            stderr=subprocess.DEVNULL,
        ))[0]
        if (details["State"]["Health"]["Status"] != "healthy"
                or details["RestartCount"] != 0
                or details["Image"] != N6_IMAGE):
            raise ValueError("N6 source service is not the verified runtime")
        inherited = dict(item.split("=", 1) for item in details["Config"]["Env"])
        selected = {key: inherited[key] for key in PROVIDER_KEYS}
        if (not all(selected.values()) or selected[PROVIDER_KEYS[0]]
                != "qwen3.8-27b"):
            raise ValueError("N6 provider contract is incomplete")
        for path in (root / "source", root / "frames/n4", root / "output",
                     root / "cache"):
            if not path.is_dir():
                raise ValueError("summary staging directory is absent")
        target = root / "output/summaries-v1"
        if target.exists():
            raise ValueError("immutable summary output already exists")
        command = [
            "sudo", "-n", "docker", "run", "--rm", "--network", "host",
            "--read-only", "--user", "1000:1000", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges",
            "--tmpfs", "/tmp:rw,nosuid,nodev,size=128m,mode=1777",
            "--workdir", "/src", "--entrypoint", "python",
            "-v", f"{root / 'source'}:/src:ro",
            "-v", f"{root / 'frames/n4'}:/frames:ro",
            "-v", f"{root / 'output'}:/out:rw",
            "-v", f"{root / 'cache'}:/cache:rw",
            "-e", "PYTHONPATH=/src",
        ]
        for key in PROVIDER_KEYS:
            command.extend(("-e", key))
        command.extend((
            N6_IMAGE, "-m", "pathfinder.rsi_exam.lightweight_fusion",
            "summarize", "--frame-n4-dir", "/frames",
            "--cache-dir", "/cache", "--output-dir", "/out/summaries-v1",
        ))
        result = subprocess.run(
            command, env={**os.environ, **selected},
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, check=False,
        )
        if result.returncode != 0:
            print(json.dumps({"status": "SUMMARY_STOPPED",
                              "exit_code": result.returncode,
                              "credentials_recorded": False}))
            return 2
        report = json.loads(result.stdout)
        if (report["status"] != "VERIFIED_VIDEO_SUMMARIES"
                or report["object_count"] != 8
                or report["credentials_recorded"] is not False):
            raise ValueError("summary freezer did not verify all videos")
        print(json.dumps(report, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "SUMMARY_STOPPED",
                          "error_class": type(exc).__name__,
                          "credentials_recorded": False}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
