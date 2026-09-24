"""N1-only, network-isolated seven-label package and public commitment.

This helper is mounted read-only into a one-shot container. It emits only
counts and public commitments, never a hidden answer value.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from pathfinder.simulator.hidden_oracle_commitment import (
    freeze_n1_oracle_preselection_commitment,
    verify_n1_oracle_preselection_commitment,
)
from pathfinder.simulator.interleaved_multiq_oracle import (
    build_interleaved_n1_oracle,
)


PRIVATE = Path("/private")
PUBLIC_PLAN = Path("/input")
OUTPUT_PARENT = PRIVATE / "multiq-sealed-test-staging-20260924-v2"
OUTPUT = OUTPUT_PARENT / "oracle"
CSV = PRIVATE / "multiq-24route-1c9ad84-v1/official-val.csv"
CSV_SHA = "43198bdef8436b8d64a9b75d846b0987c10cbf94ebf4be325c4a4e54634d66b8"
PLANNER_SHA = "294c1ce229aba0958b44ad691d6c4a095bef737e993b7e4f3fa44d3fdb08073b"


def main() -> None:
    module = Path("/app/pathfinder/rsi_exam/interleaved_multiq_plan.py")
    if hashlib.sha256(module.read_bytes()).hexdigest() != PLANNER_SHA:
        raise ValueError("mounted planner source digest differs")
    if OUTPUT.exists():
        raise ValueError("private output already exists")
    questions = [json.loads(line) for line in (
        PUBLIC_PLAN / "public-questions.jsonl"
    ).read_text(encoding="utf-8").splitlines()]
    plan = json.loads((PUBLIC_PLAN / "interleaved-plan.json").read_text(
        encoding="utf-8"
    ))
    if len(questions) != 7 or plan["route_count"] != 28:
        raise ValueError("seven-question plan coverage differs")
    report = build_interleaved_n1_oracle(
        plan_dir=PUBLIC_PLAN, public_questions=questions,
        public_source_sha256=plan["public_source_sha256"],
        official_csv_path=CSV, official_csv_sha256=CSV_SHA,
        oracle_id="n1-multiq-sealed-test-20260924-v2",
        output_dir=OUTPUT, private_root=PRIVATE,
    )
    commitment = freeze_n1_oracle_preselection_commitment(
        OUTPUT / "n1-oracle-package",
        commitment_id="n1-multiq-sealed-test-commitment-20260924-v2",
        output_dir=OUTPUT / "oracle-commitment",
    )
    verified = verify_n1_oracle_preselection_commitment(
        OUTPUT / "oracle-commitment",
        oracle_package_dir=OUTPUT / "n1-oracle-package",
    )
    if not (report["label_count"] == verified["label_count"] == 7):
        raise ValueError("N1 label count differs")
    for root, directories, files in os.walk(OUTPUT):
        os.chown(root, 10001, 10001)
        os.chmod(root, 0o700)
        for name in directories:
            path = Path(root) / name
            os.chown(path, 10001, 10001)
            os.chmod(path, 0o700)
        for name in files:
            path = Path(root) / name
            os.chown(path, 10001, 10001)
            os.chmod(path, 0o600)
    print(json.dumps({
        "status": "SEVEN_LABEL_N1_PACKAGE_VERIFIED",
        "label_count": report["label_count"],
        "oracle_id": report["oracle_id"],
        "public_task_set_sha256": report["public_task_set_sha256"],
        "commitment_sha256": commitment["commitment_sha256"],
        "hidden_label_values_returned": False,
        "credentials_recorded": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
