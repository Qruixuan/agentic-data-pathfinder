"""Freeze the outcome-blind two-video test schedule from public fields only."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.rsi_exam.interleaved_multiq_plan import (
    freeze_interleaved_plan,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--extra-selection", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    raw = args.selection.read_bytes()
    source = json.loads(raw)
    if (
        source.get("schema_version")
        != "pathfinder.multiq-sealed-public-selection/v1"
        or source.get("selection_uses_answers") is not False
        or source.get("label_values_included") is not False
        or source.get("credentials_recorded") is not False
        or len(source.get("selected_test_video_ids", [])) != 2
        or len(source.get("tasks", [])) != 6
    ):
        raise ValueError("public test selection contract differs")
    source_sha256 = hashlib.sha256(raw).hexdigest()
    experiment_id = "multiq-sealed-test-20260924-v1"
    if args.extra_selection is not None:
        extra_root = args.extra_selection.resolve()
        if {path.name for path in extra_root.iterdir()} != {
            "extra-selection.json", "SHA256SUMS"
        }:
            raise ValueError("extra selection file set differs")
        extra_raw = (extra_root / "extra-selection.json").read_bytes()
        expected = hashlib.sha256(extra_raw).hexdigest()
        if (extra_root / "SHA256SUMS").read_text(encoding="ascii") != (
            f"{expected}  extra-selection.json\n"
        ):
            raise ValueError("extra selection checksum differs")
        extra = json.loads(extra_raw)
        canonical_parent = json.dumps(
            source, sort_keys=True, ensure_ascii=False, allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if (
            extra.get("schema_version")
            != "pathfinder.multiq-sealed-public-extra/v1"
            or extra.get("parent_six_selection_sha256")
            != hashlib.sha256(canonical_parent).hexdigest()
            or extra.get("selection_seed") != source["selection_seed"]
            or extra.get("official_csv_sha256")
            != source["official_csv_sha256"]
            or extra.get("selected_test_video_ids")
            != source["selected_test_video_ids"]
            or extra.get("extra_selection_policy")
            != "seeded-rank-over-other-public-questions-v1"
            or extra.get("extra_question_id")
            != extra.get("extra_task", {}).get("question_id")
            or extra.get("selection_uses_answers") is not False
            or extra.get("label_values_included") is not False
            or extra.get("credentials_recorded") is not False
            or extra["extra_question_id"] in {
                row["question_id"] for row in source["tasks"]
            }
        ):
            raise ValueError("extra public selection contract differs")
        source["tasks"] = source["tasks"] + [extra["extra_task"]]
        source_sha256 = hashlib.sha256(json.dumps({
            "parent_selection_sha256": hashlib.sha256(raw).hexdigest(),
            "extra_selection_sha256": expected,
        }, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()
        experiment_id = "multiq-sealed-test-20260924-v2"
    selected = {
        f"nextqa-val-{video}" for video in source["selected_test_video_ids"]
    }
    rows = []
    for task in source["tasks"]:
        if set(task) != {"question_id", "object_id", "stratum",
                         "question", "answer_options"}:
            raise ValueError("test selection contains an extra field")
        if task["object_id"] not in selected:
            raise ValueError("task video differs from selected test videos")
        binding = build_n1_public_task_binding(
            workload_id=task["question_id"],
            object_id=task["object_id"],
            task_class_id=task["stratum"],
            question=task["question"],
            answer_options=task["answer_options"],
            success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        )
        rows.append({
            **task,
            "public_task_sha256": binding["task_binding_sha256"],
        })
    report = freeze_interleaved_plan(
        rows,
        seed=source["selection_seed"],
        experiment_id=experiment_id,
        public_source_sha256=source_sha256,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
