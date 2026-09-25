"""Freeze a fresh D/DC-only plan over an existing verified public cohort."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pathfinder.rsi_exam.ten_route_multiq_plan import (
    SCHEMA, freeze_ten_route_multiq_plan, load_verified_multiq_plan,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-plan-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument(
        "--derived-profile", choices=("frame-only", "single-summary-fusion"),
        default="frame-only",
    )
    args = parser.parse_args()
    original, questions, _ = load_verified_multiq_plan(args.source_plan_dir)
    if original["schema_version"] != SCHEMA:
        raise ValueError("source plan is not the original ten-route cohort")
    report = freeze_ten_route_multiq_plan(
        questions,
        seed=original["seed"],
        experiment_id=args.experiment_id,
        public_source_sha256=original["public_source_sha256"],
        exposure_inventory_sha256=original["exposure_inventory_sha256"],
        output_dir=args.output_dir,
        derived_profile=args.derived_profile,
    )
    if report["question_count"] != original["question_count"] or (
        report["route_observation_count"] != 6 * original["question_count"]
    ):
        raise ValueError("lightweight supplement does not cover the cohort")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
