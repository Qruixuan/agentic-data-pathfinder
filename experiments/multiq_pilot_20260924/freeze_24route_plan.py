"""Freeze the six-question, four-arm integration schedule from public data.

This script reads only ``public-plan.json``.  It does not read the private
diagnostic label file or submit a FlowMesh workflow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.rsi_exam.interleaved_multiq_plan import freeze_interleaved_plan
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding


def public_questions(path: Path) -> tuple[list[dict], str]:
    raw = path.read_bytes()
    source = json.loads(raw)
    if (
        source.get("schema_version")
        != "pathfinder.rsi-multiq-n6-diagnostic/v1"
        or source.get("selection_uses_answers") is not False
        or source.get("label_values_included") is not False
        or source.get("credentials_recorded") is not False
    ):
        raise ValueError("diagnostic public source has an unsafe contract")
    tasks = source.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 6:
        raise ValueError("expected exactly six public diagnostic questions")
    result = []
    for row in tasks:
        task = build_n1_public_task_binding(
            workload_id=row["task_id"], object_id=row["object_id"],
            task_class_id=row["stratum"], question=row["question"],
            answer_options=row["answer_options"],
            success_scoring_rule=(
                MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
            ),
        )
        result.append({
            "question_id": row["task_id"],
            "object_id": row["object_id"],
            "stratum": row["stratum"],
            "question": row["question"],
            "answer_options": row["answer_options"],
            "public_task_sha256": task["task_binding_sha256"],
        })
    return result, hashlib.sha256(raw).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-plan", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--seed", required=True)
    args = parser.parse_args()
    rows, source_sha = public_questions(args.public_plan)
    report = freeze_interleaved_plan(
        rows, seed=args.seed, experiment_id=args.experiment_id,
        public_source_sha256=source_sha, output_dir=args.output_dir,
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
