"""Add one outcome-blind public question to the sealed two-video cohort.

The original six tasks and selected videos stay fixed. The extra task is the
lowest seeded rank among the remaining public questions on those videos.
Neither the CSV answer column nor any scored result enters the ranking.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

from select_sealed_public import (
    OFFICIAL_CSV_SHA256,
    TYPE_STRATUM,
    _rank,
    select,
)


def select_seven(csv_path: Path) -> dict:
    base = select(csv_path)
    selected_ids = {row["question_id"] for row in base["tasks"]}
    chosen = set(base["selected_test_video_ids"])
    candidates = []
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            video = row["video"]
            stratum = TYPE_STRATUM.get(row["type"])
            question_id = f"nextqa-val-{video}-q{row['qid']}"
            if video not in chosen or stratum is None or (
                question_id in selected_ids
            ):
                continue
            candidates.append({
                "question_id": question_id,
                "object_id": f"nextqa-val-{video}",
                "stratum": stratum,
                "question": row["question"],
                "answer_options": [
                    {"option_id": chr(ord("A") + index), "text": row[f"a{index}"]}
                    for index in range(5)
                ],
            })
    if not candidates:
        raise ValueError("selected videos have no additional public question")
    extra = min(candidates, key=lambda item: (
        _rank("extra-question-v1", item["object_id"], item["stratum"],
              item["question_id"]), item["question_id"],
    ))
    previous = json.dumps(base, sort_keys=True, ensure_ascii=False,
                          allow_nan=False, separators=(",", ":")).encode("utf-8")
    return {
        "schema_version": "pathfinder.multiq-sealed-public-selection/v2",
        "selection_seed": base["selection_seed"],
        "extra_selection_policy": "seeded-rank-over-other-public-questions-v1",
        "parent_six_selection_sha256": hashlib.sha256(previous).hexdigest(),
        "official_csv_sha256": OFFICIAL_CSV_SHA256,
        "selected_test_video_ids": base["selected_test_video_ids"],
        "development_video_ids": base["development_video_ids"],
        "tasks": sorted(base["tasks"] + [extra],
                        key=lambda item: item["question_id"]),
        "extra_question_id": extra["question_id"],
        "selection_uses_answers": False,
        "label_values_included": False,
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--official-csv", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(select_seven(args.official_csv),
                     sort_keys=True, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
