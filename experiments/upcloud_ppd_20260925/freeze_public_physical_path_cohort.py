"""Freeze an outcome-blind public PPD pilot cohort; do not inspect labels.

This is a selection protocol, not a media package or experiment admission.
The development and held-out test videos are both excluded from prior
exposure, and all source files are pinned before any route outcome is read.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from experiments.nextqa_atphard_cohort import (
    canonical,
    checked_sources,
    public_hard_rows,
    rank,
    rows,
    select,
    sha256,
)
from experiments.ten_route_multiq_20260925.account_verified_routes import (
    verify_sums,
)


SEED = "pathfinder-ppd-atphard-physical-path-20260928-v1"
EXPECTED_SOURCE_SHA256 = {
    "official_val": "43198bdef8436b8d64a9b75d846b0987c10cbf94ebf4be325c4a4e54634d66b8",
    "atp_hard": "82aa5a5dc453fce28151002dc5d1a9179dee3cbfee14aa860949071fc49e4db2",
    "grounding": "4feb23e53102cb2d4779e857ff8f9fd9b552fc38435621406b2ed9b0bf76204b",
    "inventory": "9987435684959abdb4131a3a3e1026cf295f69490aac53fd871706fa97fd0d3c",
    "prior_exposure": "237793bda98637a02b4179947757c8d3feb5485de83c5c46014e957fabfa2838",
    "prior_selection": "b0026ac141f3647646848f86387b4132328014fe519ea58013a72fee96ddab38",
}


def freeze(args: argparse.Namespace) -> dict[str, object]:
    if args.output_dir.exists():
        raise ValueError("immutable cohort output directory already exists")
    sources = {
        "official_val": args.official_val,
        "atp_hard": args.atp_hard,
        "grounding": args.grounding,
        "inventory": args.inventory,
        "prior_exposure": args.prior_exposure,
        "prior_selection": args.prior_selection,
    }
    source_sha = checked_sources(sources, EXPECTED_SOURCE_SHA256)
    verify_sums(args.previous_plan_dir)
    verify_sums(args.previous_media_dir)
    official = rows(args.official_val)
    hard = rows(args.atp_hard)
    grounding = json.loads(args.grounding.read_bytes())
    inventory = json.loads(args.inventory.read_bytes())
    prior_exposure = json.loads(args.prior_exposure.read_bytes())
    prior_selection = json.loads(args.prior_selection.read_bytes())
    previous_video_ids = {
        f"nextqa-val-{path.stem}"
        for path in args.previous_media_dir.glob("*.mp4")
    }
    if len(previous_video_ids) != 8:
        raise ValueError("previous 8x5 video exposure set is incomplete")
    exposed = (
        set(prior_exposure["object_ids"])
        | set(prior_selection["selected_object_ids"])
        | previous_video_ids
    )
    tasks = public_hard_rows(official, hard)
    eligible: dict[str, list[dict]] = {}
    for identity, task in tasks.items():
        video, qid = identity.split("|", 1)
        media = inventory["objects"].get(task["object_id"])
        spans = grounding.get(video, {}).get("location", {}).get(qid)
        if (
            task["object_id"] in exposed
            or media is None
            or not spans
            or not 1_500_000 <= media["bytes"] <= 7_000_000
        ):
            continue
        eligible.setdefault(video, []).append(task)
    videos = [
        video for video, pool in eligible.items()
        if len(pool) >= 5
        and sum(row["stratum"] == "causal" for row in pool) >= 1
        and sum(row["stratum"] == "temporal" for row in pool) >= 2
    ]
    if len(videos) < 2:
        raise ValueError("insufficient unexposed grounded ATP-Hard videos")
    development_video = min(
        videos, key=lambda video: rank(SEED, "development-video", video)
    )
    exposure = {
        "schema_version": "pathfinder.ppd-public-exposure-union/v1",
        "object_ids": sorted(exposed),
        "prior_exposure_sha256": source_sha["prior_exposure"],
        "prior_selection_sha256": source_sha["prior_selection"],
        "previous_plan_sha256": sha256(
            (args.previous_plan_dir / "ten-route-multiq-plan.json").read_bytes()
        ),
        "previous_media_object_count": 8,
        "quality_outcomes_read": False,
        "label_values_included": False,
        "credentials_recorded": False,
    }
    exposure_bytes = canonical(exposure) + b"\n"
    config = {
        "schema_version": "pathfinder.nextqa-atphard-multiq-selection-config/v1",
        "seed": SEED,
        "min_video_bytes": 1_500_000,
        "max_video_bytes": 7_000_000,
        "development": [{
            "video": development_video,
            "required_qids": [],
            "question_count": 5,
        }],
        "test_video_count": 1,
        "questions_per_video": 5,
        "source_sha256": {
            "official_val": source_sha["official_val"],
            "atp_hard": source_sha["atp_hard"],
            "grounding": source_sha["grounding"],
            "inventory": source_sha["inventory"],
            "exposure": sha256(exposure_bytes),
            "prior_selection": source_sha["prior_selection"],
        },
    }
    cohort = select(official, hard, grounding, inventory, exposed, config)
    if (
        len(cohort["development"]) != 5
        or len(cohort["test"]) != 5
        or set(cohort["development_object_ids"])
        & set(cohort["test_object_ids"])
        or set(cohort["development_object_ids"] + cohort["test_object_ids"])
        & exposed
    ):
        raise ValueError("cohort violates count or video-disjointness")
    cohort.update({
        "selection_protocol": "outcome-blind-hash-ranked-public-fields-v1",
        "source_sha256": source_sha,
        "exposure_union_sha256": sha256(exposure_bytes),
        "selection_config_sha256": sha256(canonical(config) + b"\n"),
        "route_outcomes_read": False,
        "media_packaged": False,
        "experiment_admission": False,
    })
    files = {
        "exposure-inventory.json": exposure_bytes,
        "selection-config.json": canonical(config) + b"\n",
        "public-selection.json": canonical(cohort) + b"\n",
    }
    sums = "".join(
        f"{hashlib.sha256(value).hexdigest()}  {name}\n"
        for name, value in sorted(files.items())
    ).encode("ascii")
    args.output_dir.mkdir(parents=True)
    for name, value in files.items():
        (args.output_dir / name).write_bytes(value)
    (args.output_dir / "SHA256SUMS").write_bytes(sums)
    verify_sums(args.output_dir)
    return {
        "status": "PUBLIC_PPD_COHORT_SELECTION_FROZEN",
        "development_object_ids": cohort["development_object_ids"],
        "test_object_ids": cohort["test_object_ids"],
        "development_questions": 5,
        "test_questions": 5,
        "eligible_video_count": len(videos),
        "quality_outcomes_read": False,
        "media_packaged": False,
        "experiment_admission": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "official-val", "atp-hard", "grounding", "inventory",
        "prior-exposure", "prior-selection", "previous-plan-dir",
        "previous-media-dir", "output-dir",
    ):
        parser.add_argument("--" + name, required=True, type=Path)
    print(json.dumps(freeze(parser.parse_args()), sort_keys=True))


if __name__ == "__main__":
    main()
