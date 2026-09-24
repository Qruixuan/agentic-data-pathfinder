"""Freeze the public, video-disjoint relational development selection."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from experiments.complexity_dev_selection import canonical


def checked(directory: Path, filename: str) -> bytes:
    manifest = {}
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        manifest[name] = digest
    raw = (directory / filename).read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest[filename]:
        raise ValueError("public source checksum differs")
    return raw


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exposure", type=Path, required=True)
    parser.add_argument("--prior-cohort", type=Path, required=True)
    parser.add_argument("--media-inventory", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    exposure = json.loads(checked(args.exposure, "exposure-inventory.json"))
    cohort = json.loads(checked(args.prior_cohort, "public-selection.json"))
    media_raw = checked(args.media_inventory, "inventory.json")
    excluded = sorted(set(exposure["object_ids"])
                      | set(cohort["selected_object_ids"]))
    if len(excluded) < 56 or len(cohort["selected_object_ids"]) != 4:
        raise ValueError("historical video exposure inventory is incomplete")
    protocol = {
        "schema_version": "pathfinder.complexity-development-selection/v1",
        "seed": "pathfinder-relational-dev-20260924-v1",
        "official_csv_sha256": (
            "43198bdef8436b8d64a9b75d846b0987c10cbf94ebf4be325c4a4e54634d66b8"
        ),
        "media_inventory_sha256": hashlib.sha256(media_raw).hexdigest(),
        "excluded_object_ids": excluded,
        "object_count": 2,
        "strata": ["causal", "temporal", "descriptive"],
        "min_video_bytes": 1_500_000,
        "max_video_bytes": 7_000_000,
        "selection_rule": (
            "public-relational-question-threshold-plus-seeded-rank-v1"
        ),
        "evaluation_role": "development-only",
        "credentials_recorded": False,
        "hidden_label_values_included": False,
    }
    if args.output_dir.exists():
        raise ValueError("immutable selection protocol already exists")
    args.output_dir.mkdir(parents=True)
    payload = canonical(protocol) + b"\n"
    (args.output_dir / "protocol.json").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    (args.output_dir / "SHA256SUMS").write_bytes(
        f"{digest}  protocol.json\n".encode())
    print(json.dumps({"status": "PUBLIC_RELATIONAL_PROTOCOL_FROZEN",
                      "excluded_video_count": len(excluded),
                      "protocol_sha256": hashlib.sha256(
                          canonical(protocol)).hexdigest()}))


if __name__ == "__main__":
    main()
