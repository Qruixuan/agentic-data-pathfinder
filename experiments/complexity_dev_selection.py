"""Outcome-blind public selection for a small, event-focused VideoQA pilot.

The official CSV is processed only on N1. This module never selects, compares,
or exports answer columns; every selection feature is a public field or MP4
size from the pre-existing pinned archive inventory.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
from pathlib import Path
import re


TYPE_STRATUM = {
    "CW": "causal", "CH": "causal", "TC": "temporal",
    "TN": "temporal", "TP": "temporal", "DC": "descriptive",
    "DL": "descriptive", "DO": "descriptive",
}
STRATA = ("causal", "temporal", "descriptive")
TEMPORAL = {"before", "after", "while", "during", "when", "first", "last"}
CAUSAL = {"why", "how"}
WORD = re.compile(r"[a-z]+")


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(",", ":")).encode()


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def words(text: str) -> list[str]:
    return WORD.findall(text.casefold())


def public_eligibility(question: str, options: list[str],
                       stratum: str) -> bool:
    terms = words(question)
    if (len(terms) < 8 or len(options) != 5
            or len({o.casefold().strip() for o in options}) != 5
            or any(not words(o) for o in options)):
        return False
    if stratum == "causal":
        return bool(CAUSAL & set(terms))
    if stratum == "temporal":
        return bool(TEMPORAL & set(terms))
    if stratum == "descriptive":
        return bool(TEMPORAL & set(terms)) and terms[0] in {
            "what", "which", "who", "where"
        }
    return False


def rank(seed: str, domain: str, value: object) -> str:
    return sha(canonical({"seed": seed, "domain": domain, "value": value}))


def select(csv_path: Path, inventory_path: Path, protocol: dict) -> dict:
    required = {
        "schema_version", "seed", "official_csv_sha256",
        "media_inventory_sha256", "excluded_object_ids", "object_count",
        "strata", "min_video_bytes", "max_video_bytes",
        "selection_rule", "evaluation_role", "credentials_recorded",
        "hidden_label_values_included",
    }
    if (set(protocol) != required
            or protocol["schema_version"]
            != "pathfinder.complexity-development-selection/v1"
            or protocol["selection_rule"]
            != "public-relational-question-threshold-plus-seeded-rank-v1"
            or protocol["strata"] != list(STRATA)
            or protocol["object_count"] != 2
            or protocol["evaluation_role"] != "development-only"
            or protocol["credentials_recorded"] is not False
            or protocol["hidden_label_values_included"] is not False):
        raise ValueError("public development protocol differs")
    if (sha(csv_path.read_bytes()) != protocol["official_csv_sha256"]
            or sha(inventory_path.read_bytes())
            != protocol["media_inventory_sha256"]):
        raise ValueError("official public source digest differs")
    inventory = json.loads(inventory_path.read_bytes())["objects"]
    excluded = set(protocol["excluded_object_ids"])
    by_object: dict[str, dict[str, list[dict]]] = {}
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            object_id = "nextqa-val-" + row["video"]
            stratum = TYPE_STRATUM.get(row["type"])
            media = inventory.get(object_id)
            if (object_id in excluded or stratum is None or media is None
                    or not protocol["min_video_bytes"] <= media["bytes"]
                    <= protocol["max_video_bytes"]):
                continue
            options = [row[f"a{i}"] for i in range(5)]
            if not public_eligibility(row["question"], options, stratum):
                continue
            public = {
                "question_id": object_id + "-q" + row["qid"],
                "object_id": object_id, "stratum": stratum,
                "question": row["question"],
                "answer_options": [
                    {"option_id": chr(65 + i), "text": option}
                    for i, option in enumerate(options)
                ],
            }
            by_object.setdefault(object_id, {}).setdefault(
                stratum, []).append(public)
    eligible = [oid for oid, strata in by_object.items()
                if set(strata) == set(STRATA)]
    if len(eligible) < 2:
        raise ValueError("not enough public videos satisfy predeclared criteria")
    chosen = sorted(eligible, key=lambda oid: (
        rank(protocol["seed"], "video", oid), oid))[:2]
    tasks = [
        min(by_object[oid][stratum], key=lambda row: (
            rank(protocol["seed"], "question", row), row["question_id"]
        ))
        for oid in chosen for stratum in STRATA
    ]
    return {
        "schema_version": "pathfinder.complexity-development-public-selection/v1",
        "status": "SELECTED_PUBLIC_DEVELOPMENT_TASKS",
        "protocol_sha256": sha(canonical(protocol)),
        "official_csv_sha256": protocol["official_csv_sha256"],
        "eligible_video_count": len(eligible),
        "selected_object_ids": chosen,
        "tasks": tasks,
        "selection_uses_answers": False,
        "label_values_included": False,
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--official-csv", type=Path, required=True)
    parser.add_argument("--media-inventory", type=Path, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--protocol-base64")
    group.add_argument("--protocol-file", type=Path)
    args = parser.parse_args()
    protocol = json.loads(
        args.protocol_file.read_bytes() if args.protocol_file is not None
        else base64.b64decode(args.protocol_base64, validate=True)
    )
    print(canonical(select(args.official_csv, args.media_inventory,
                           protocol)).decode())


if __name__ == "__main__":
    main()
