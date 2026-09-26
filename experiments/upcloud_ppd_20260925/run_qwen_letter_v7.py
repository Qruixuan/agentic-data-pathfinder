"""Use the proven v6 N7 gates with the versioned final-option parser.

The v6 supervisor remains unchanged for rollback. This wrapper substitutes
only the read-only runner mount and its clean-Git-archive SHA-256.
"""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path


_BASE = Path("/home/pathfinder/ppd-qwen-first-20260926-v1/run_qwen_letter_v6.py")
_BASE_SHA256 = (
    "73ad7def7b0a51105c1ee28015aa2eb82c8493b0496978d6af09e46a28f2ee7f"
)
_RUNNER = Path(
    "/home/pathfinder/ppd-deploy-20260926-v1/operator/"
    "run_engineering_session_v7.py"
)
_RUNNER_SHA256 = (
    "ff40f884c2b16e90121676d20694f6f8a7b0123044ddc672c559b8d201acfb20"
)


def main() -> int:
    if not _BASE.is_file() or hashlib.sha256(_BASE.read_bytes()).hexdigest() != (
        _BASE_SHA256
    ):
        raise RuntimeError("versioned PPD supervisor bytes differ")
    spec = importlib.util.spec_from_file_location("ppd_qwen_v6_supervisor", _BASE)
    if spec is None or spec.loader is None:
        raise RuntimeError("versioned PPD supervisor is unavailable")
    supervisor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(supervisor)
    supervisor.RUNNER = _RUNNER
    supervisor.RUNNER_SHA256 = _RUNNER_SHA256
    return supervisor.main()


if __name__ == "__main__":
    raise SystemExit(main())
