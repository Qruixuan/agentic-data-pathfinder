"""Select two public multi-question test videos on N1 without using labels.

Run with the already-staged official CSV on N1. Standard output contains only
public question/option text and selection metadata, never the answer column.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path


COHORT_VIDEO_IDS = frozenset({
    "2834146886", "2976913210", "3462517143", "4130504920",
    "4260763967", "4942054721", "5296635780", "5735711594",
    "5840177726", "8132842161", "8547321641", "9088819598",
})
DEVELOPMENT_VIDEO_IDS = frozenset({"4130504920", "4260763967"})
COHORT_SHA256 = "85ad8f46479bc8a8a0818ec9d631ca771684b5ee014ae23a4dc4773f65ee5717"
OFFICIAL_CSV_SHA256 = "43198bdef8436b8d64a9b75d846b0987c10cbf94ebf4be325c4a4e54634d66b8"
SEED = "pathfinder-rsi-multiq-sealed-20260924-v1"
STRATA = ("causal", "temporal", "descriptive")
TYPE_STRATUM = {
    "CW": "causal", "CH": "causal",
    "TC": "temporal", "TN": "temporal", "TP": "temporal",
    "DC": "descriptive", "DL": "descriptive", "DO": "descriptive",
}


def _rank(*parts: str) -> str:
    canonical = json.dumps({
        "domain": "interleaved-multiq-order-v1",
        "seed": SEED,
        "parts": list(parts),
    }, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def select(csv_path: Path) -> dict:
    raw = csv_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != OFFICIAL_CSV_SHA256:
        raise ValueError("official CSV digest differs")
    by_video: dict[str, dict[str, list[dict[str, str]]]] = {}
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            video = row["video"]
            if video not in COHORT_VIDEO_IDS:
                continue
            stratum = TYPE_STRATUM.get(row["type"])
            if stratum is None:
                continue
            # Deliberately select only public columns. The answer column is
            # not inspected, ranked, transmitted, or written.
            public = {
                "video": video,
                "qid": row["qid"],
                "stratum": stratum,
                "question": row["question"],
                "answer_options": [
                    {"option_id": chr(ord("A") + i), "text": row[f"a{i}"]}
                    for i in range(5)
                ],
            }
            by_video.setdefault(video, {}).setdefault(stratum, []).append(
                public,
            )
    eligible = sorted(video for video, strata in by_video.items()
                      if set(strata) == set(STRATA)
                      and video not in DEVELOPMENT_VIDEO_IDS)
    if len(eligible) < 2:
        raise ValueError("fewer than two outcome-blind test videos")
    chosen = sorted(eligible, key=lambda video: (
        _rank("cohort-video", f"nextqa-val-{video}"), video,
    ))[:2]
    tasks = []
    for video in chosen:
        for stratum in STRATA:
            candidates = by_video[video][stratum]
            question = min(candidates, key=lambda row: (
                _rank("cohort-question", f"nextqa-val-{video}",
                      stratum, row["qid"]), row["qid"],
            ))
            tasks.append({
                "question_id": f"nextqa-val-{video}-q{question['qid']}",
                "object_id": f"nextqa-val-{video}",
                "stratum": stratum,
                "question": question["question"],
                "answer_options": question["answer_options"],
            })
    return {
        "schema_version": "pathfinder.multiq-sealed-public-selection/v1",
        "selection_seed": SEED,
        "source_cohort_sha256": COHORT_SHA256,
        "official_csv_sha256": OFFICIAL_CSV_SHA256,
        "development_video_ids": sorted(DEVELOPMENT_VIDEO_IDS),
        "eligible_test_video_ids": eligible,
        "selected_test_video_ids": chosen,
        "tasks": tasks,
        "selection_uses_answers": False,
        "label_values_included": False,
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--official-csv", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(select(args.official_csv), sort_keys=True,
                     ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
