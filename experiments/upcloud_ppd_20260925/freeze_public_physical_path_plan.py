"""Freeze a ten-route public plan for the outcome-blind PPD pilot cohort.

The plan is not a source-bound runtime admission or submission permission.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from experiments.ten_route_multiq_20260925.account_verified_routes import (
    verify_sums,
)
from pathfinder.rsi_exam.ten_route_multiq_plan import (
    freeze_ten_route_multiq_plan,
)
from pathfinder.simulator.hidden_oracle import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    build_n1_public_task_binding,
)


def freeze(args: argparse.Namespace) -> dict[str, object]:
    verify_sums(args.protocol_dir)
    verify_sums(args.cohort_dir)
    protocol = json.loads(
        (args.protocol_dir / "public-selection.json").read_bytes()
    )
    cohort = json.loads((args.cohort_dir / "selection.json").read_bytes())
    expected = protocol["development"] + protocol["test"]
    observed = cohort["development"] + cohort["test"]
    if (
        expected != observed
        or protocol["development_object_ids"]
        != cohort["development_object_ids"]
        or protocol["test_object_ids"] != cohort["test_object_ids"]
        or protocol["exposure_union_sha256"]
        != cohort["source_sha256"]["exposure"]
        or cohort.get("label_values_included") is not False
        or cohort.get("quality_outcomes_used_for_test_selection") is not False
        or protocol.get("route_outcomes_read") is not False
        or set(cohort["video_media"])
        != {item.removeprefix("nextqa-val-") for item in
            cohort["development_object_ids"] + cohort["test_object_ids"]}
    ):
        raise ValueError("cohort media or public selection differs from protocol")
    questions = []
    for row in expected:
        if set(row) != {
            "question_id", "object_id", "stratum", "question",
            "answer_options", "question_type",
        }:
            raise ValueError("cohort task has a non-public field")
        public = {
            key: row[key] for key in (
                "question_id", "object_id", "stratum", "question",
                "answer_options",
            )
        }
        task = build_n1_public_task_binding(
            workload_id=public["question_id"],
            object_id=public["object_id"],
            task_class_id=public["stratum"],
            question=public["question"],
            answer_options=public["answer_options"],
            success_scoring_rule=(
                MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
            ),
        )
        questions.append({
            **public, "public_task_sha256": task["task_binding_sha256"],
        })
    config = json.loads(
        (args.protocol_dir / "selection-config.json").read_bytes()
    )
    report = freeze_ten_route_multiq_plan(
        questions,
        seed=config["seed"],
        experiment_id=args.experiment_id,
        public_source_sha256=cohort["source_sha256"]["official_val"],
        exposure_inventory_sha256=protocol["exposure_union_sha256"],
        output_dir=args.output_dir,
    )
    if (report["question_count"] != 10
            or report["object_count"] != 2
            or report["route_observation_count"] != 100):
        raise ValueError("public route plan coverage differs")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol-dir", type=Path, required=True)
    parser.add_argument("--cohort-dir", type=Path, required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    print(json.dumps(freeze(parser.parse_args()), sort_keys=True))


if __name__ == "__main__":
    main()
