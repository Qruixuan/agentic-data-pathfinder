"""Outcome-blind public selection from the official CSV, run on N1.

This standalone selector deliberately exports only an allowlist of public
columns. No answer column is ranked, inspected or returned. The selection
protocol (including exclusions) must be frozen before invoking this tool.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
from pathlib import Path


TYPE_STRATUM = {
    "CW": "causal", "CH": "causal", "TC": "temporal",
    "TN": "temporal", "TP": "temporal", "DC": "descriptive",
    "DL": "descriptive", "DO": "descriptive",
}
STRATA = ("causal", "temporal", "descriptive")


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(",", ":")).encode("utf-8")


def select(csv_path: Path, protocol: dict, media_raw: bytes | None = None) -> dict:
    required = {"schema_version", "seed", "object_count", "strata",
                "official_csv_sha256", "excluded_object_ids",
                "selection_rule"}
    media_sizes = None
    if protocol.get("schema_version") == "pathfinder.fresh-multiq-selection/v2":
        required |= {"media_inventory_sha256", "max_direct_video_bytes"}
        if (media_raw is None or hashlib.sha256(media_raw).hexdigest()
                != protocol.get("media_inventory_sha256")):
            raise ValueError("source media inventory digest differs")
        media_sizes = json.loads(media_raw)["objects"]
        if type(protocol["max_direct_video_bytes"]) is not int or protocol["max_direct_video_bytes"] <= 0:
            raise ValueError("direct video limit is invalid")
    if (set(protocol) != required
            or protocol["schema_version"] not in {
                "pathfinder.fresh-multiq-selection/v1",
                "pathfinder.fresh-multiq-selection/v2"}
            or protocol["strata"] != list(STRATA)
            or protocol["selection_rule"] != "sha256-seeded-public-fields-v1"
            or type(protocol["object_count"]) is not int
            or protocol["object_count"] < 2
            or not isinstance(protocol["seed"], str)
            or not protocol["seed"]):
        raise ValueError("selection protocol is invalid")
    excluded = protocol["excluded_object_ids"]
    if (not isinstance(excluded, list) or not excluded
            or any(not isinstance(item, str) for item in excluded)
            or len(set(excluded)) != len(excluded)):
        raise ValueError("explicit unique exposure exclusions are required")
    raw = csv_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != protocol["official_csv_sha256"]:
        raise ValueError("official CSV digest differs")
    by_object: dict[str, dict[str, list[dict]]] = {}
    seen = set()
    excluded = set(excluded)
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            object_id = "nextqa-val-" + row["video"]
            stratum = TYPE_STRATUM.get(row["type"])
            if object_id in excluded or stratum is None:
                continue
            if media_sizes is not None and (
                object_id not in media_sizes
                or not 0 < media_sizes[object_id]["bytes"] <= protocol["max_direct_video_bytes"]
            ):
                continue
            question_id = object_id + "-q" + row["qid"]
            if question_id in seen:
                raise ValueError("official public question identity repeats")
            seen.add(question_id)
            public = {
                "question_id": question_id, "object_id": object_id,
                "stratum": stratum, "question": row["question"],
                "answer_options": [
                    {"option_id": chr(65 + i), "text": row[f"a{i}"]}
                    for i in range(5)
                ],
            }
            by_object.setdefault(object_id, {}).setdefault(
                stratum, [],
            ).append(public)

    def rank(domain: str, value: object) -> str:
        return hashlib.sha256(canonical({
            "seed": protocol["seed"], "domain": domain, "value": value,
        })).hexdigest()

    eligible = sorted(oid for oid, strata in by_object.items()
                      if set(strata) == set(STRATA))
    if len(eligible) < protocol["object_count"]:
        raise ValueError("not enough fresh videos with all three strata")
    selected = sorted(eligible, key=lambda oid: (rank("video", oid), oid))[
        :protocol["object_count"]
    ]
    questions = [
        min(by_object[oid][stratum], key=lambda item: (
            rank("question", item), item["question_id"],
        ))
        for oid in selected for stratum in STRATA
    ]
    return {
        "schema_version": "pathfinder.fresh-multiq-public-selection/v1",
        "protocol_sha256": hashlib.sha256(canonical(protocol)).hexdigest(),
        "official_csv_sha256": protocol["official_csv_sha256"],
        "eligible_object_count": len(eligible),
        "eligible_object_ids_sha256": hashlib.sha256(canonical(eligible)).hexdigest(),
        "selected_object_ids": selected, "tasks": questions,
        "selection_uses_answers": False, "label_values_included": False,
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--official-csv", type=Path, required=True)
    parser.add_argument("--protocol-base64", required=True)
    parser.add_argument("--media-inventory", type=Path)
    args = parser.parse_args()
    protocol = json.loads(base64.b64decode(args.protocol_base64, validate=True))
    media = None if args.media_inventory is None else args.media_inventory.read_bytes()
    print(canonical(select(args.official_csv, protocol, media)).decode("utf-8"))


if __name__ == "__main__":
    main()
