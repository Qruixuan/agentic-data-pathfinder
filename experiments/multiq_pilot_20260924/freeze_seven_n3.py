"""Build seven question-bound N3 frame bundles from frozen public inputs."""

from __future__ import annotations

import json
from pathlib import Path

from pathfinder.simulator.n3_multiq_indexed_data_plane import (
    build_n3_multiq_indexed_package,
    derive_n3_multiq_question_policies,
    verify_n3_multiq_indexed_package,
)


ROOT = Path(__file__).resolve().parents[2] / "artifacts"
PLAN = ROOT / "multiq-sealed-test-plan-20260924-v2"
QUERY = ROOT / "multiq-sealed-query-20260924-v2"
VIDEO_INDEX = ROOT / "interleaved-multiq-index-2561a1f-v1/video-index-v1"
PREPARATION = ROOT / "rsi-exam-formal-temporal-preparation-ab60687-v2"
CAPTIONS = ROOT / "rsi-exam-formal-temporal-captions-7a5a8dd-v2"
RAW = ROOT / "rsi-exam-formal-n3-raw-12video-ab60687-v2"
OUTPUT = ROOT / "multiq-sealed-n3-20260924-v2"


def main() -> None:
    questions = [json.loads(row) for row in (
        PLAN / "public-questions.jsonl"
    ).read_text(encoding="utf-8").splitlines()]
    document = json.loads((PLAN / "interleaved-plan.json").read_text(
        encoding="utf-8"
    ))
    if len(questions) != 7 or document["route_count"] != 28:
        raise ValueError("seven-question plan coverage differs")
    policies = derive_n3_multiq_question_policies(
        plan_dir=PLAN, public_questions=questions,
        public_source_sha256=document["public_source_sha256"],
        query_dir=QUERY, video_index_dir=VIDEO_INDEX,
        preparation_dir=PREPARATION, caption_dir=CAPTIONS,
        raw_package_dir=RAW,
    )
    if len(policies) != 7:
        raise ValueError("seven query-aware N3 policies are required")
    report = build_n3_multiq_indexed_package(
        RAW, output_dir=OUTPUT,
        package_id="n3-multiq-sealed-test-20260924-v2",
        question_policies=policies,
    )
    verified = verify_n3_multiq_indexed_package(
        OUTPUT, raw_package_dir=RAW, question_policies=policies,
    )
    if report != verified:
        raise ValueError("N3 package verification differs")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
