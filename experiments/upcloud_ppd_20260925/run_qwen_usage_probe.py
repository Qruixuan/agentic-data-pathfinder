"""Run the proven public PPD supervisor with an isolated usage worker.

Only the worker alias/image change; the frozen public task and Gateway stay
unchanged. The image digest is supplied explicitly after its verified build.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import re
import sys
from pathlib import Path
from urllib.request import ProxyHandler, Request, build_opener


BASE = Path("/home/pathfinder/ppd-qwen-first-20260926-v1/run_qwen_letter_v6.py")
BASE_SHA256 = (
    "73ad7def7b0a51105c1ee28015aa2eb82c8493b0496978d6af09e46a28f2ee7f"
)
RUNNER_V7 = Path(
    "/home/pathfinder/ppd-deploy-20260926-v1/operator/"
    "run_engineering_session_v7.py"
)
RUNNER_V7_SHA256 = (
    "ff40f884c2b16e90121676d20694f6f8a7b0123044ddc672c559b8d201acfb20"
)


def _require_node_registry_healthy() -> None:
    """Health alone does not prove the N7 worker dispatch API is responsive."""
    request = Request("http://127.0.0.1:8000/api/v1/stack/workers")
    try:
        with build_opener(ProxyHandler({})).open(request, timeout=5) as response:
            if response.status != 200:
                raise RuntimeError("N7 worker registry did not return HTTP 200")
            response.read(1)
    except Exception as exc:
        raise RuntimeError("N7 worker registry is not dispatch-ready") from exc


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--worker-alias", required=True)
    args, remaining = parser.parse_known_args()
    if re.fullmatch(r"sha256:[0-9a-f]{64}", args.worker_image) is None:
        raise RuntimeError("usage worker image identity is invalid")
    if re.fullmatch(r"pathfinder_ppd_visual_[0-9]{8}[a-z]",
                    args.worker_alias) is None:
        raise RuntimeError("usage worker alias is invalid")
    _require_node_registry_healthy()
    if not BASE.is_file() or hashlib.sha256(BASE.read_bytes()).hexdigest() != (
        BASE_SHA256
    ):
        raise RuntimeError("proven PPD supervisor bytes differ")
    if not RUNNER_V7.is_file() or hashlib.sha256(
        RUNNER_V7.read_bytes()
    ).hexdigest() != RUNNER_V7_SHA256:
        raise RuntimeError("proven final-option runner bytes differ")
    spec = importlib.util.spec_from_file_location("ppd_usage_supervisor", BASE)
    if spec is None or spec.loader is None:
        raise RuntimeError("proven PPD supervisor is unavailable")
    supervisor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(supervisor)
    supervisor.WORKER = args.worker_alias
    supervisor.WORKER_IMAGE = args.worker_image
    supervisor.RUNNER = RUNNER_V7
    supervisor.RUNNER_SHA256 = RUNNER_V7_SHA256
    sys.argv = [sys.argv[0], *remaining]
    return supervisor.main()


if __name__ == "__main__":
    raise SystemExit(main())
