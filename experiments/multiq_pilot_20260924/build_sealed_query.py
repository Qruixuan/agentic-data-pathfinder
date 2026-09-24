"""Freeze the sealed public query batch with durable per-call usage receipts.

The default mode is offline preflight. --execute alone may contact the
embedding provider, and does so only through N6's existing credential.
"""

from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from pathfinder.rsi_exam.offline_replay_costing import PRICE_SNAPSHOT
from pathfinder.rsi_exam.temporal_index_layers import (
    build_temporal_query_batch,
    verify_temporal_query_batch,
    verify_video_temporal_index,
)

from experiments.multiq_pilot_20260924.remote_embedding_transport import (
    DurableN6EmbeddingTransport,
)


ROOT = Path(__file__).resolve().parents[2]
PLAN = ROOT / "artifacts/multiq-sealed-test-plan-20260924-v1"
PLAN_V2 = ROOT / "artifacts/multiq-sealed-test-plan-20260924-v2"
STAGE = ROOT / "artifacts/multiq-sealed-query-input-stage-20260924-v1"
VIDEO = STAGE / "video-index"
PREP = STAGE / "preparation"
CAPTIONS = STAGE / "captions"
OUTPUT = ROOT / "artifacts/multiq-sealed-query-20260924-v1"
OUTPUT_V2 = ROOT / "artifacts/multiq-sealed-query-20260924-v2"
CACHE = ROOT / "artifacts/multiq-sealed-query-response-cache-20260924-v1"
EXPECTED_VIDEO_SHA256 = (
    "8e20e00027164de0cd452049788a912ccf4408977fac0c01e47c291acdc0b06d"
)
EXPECTED_QUESTION_IDS = {
    "nextqa-val-2834146886-q1", "nextqa-val-2834146886-q6",
    "nextqa-val-2834146886-q9", "nextqa-val-8547321641-q3",
    "nextqa-val-8547321641-q5", "nextqa-val-8547321641-q8",
}
EXTRA_QUESTION_ID = "nextqa-val-2834146886-q8"


def _verify_checksums(root: Path) -> None:
    for line in (root / "SHA256SUMS").read_text(encoding="ascii").splitlines():
        digest, name = line.split("  ", 1)
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise ValueError("source checksum differs: " + name)


def _questions(plan: Path, expected_ids: set[str]) -> list[dict[str, str]]:
    _verify_checksums(plan)
    rows = [json.loads(line) for line in (
        plan / "public-questions.jsonl"
    ).read_text(encoding="utf-8").splitlines()]
    if {row["question_id"] for row in rows} != expected_ids:
        raise ValueError("sealed public question IDs differ")
    return [{key: row[key] for key in ("question_id", "object_id", "question")}
            for row in rows]


def preflight(seven: bool) -> tuple[list[dict[str, str]], dict, Path]:
    plan = PLAN_V2 if seven else PLAN
    output = OUTPUT_V2 if seven else OUTPUT
    expected_ids = EXPECTED_QUESTION_IDS | ({EXTRA_QUESTION_ID} if seven
                                            else set())
    questions = _questions(plan, expected_ids)
    report = verify_video_temporal_index(VIDEO, PREP, CAPTIONS)
    if report["package_sha256"] != EXPECTED_VIDEO_SHA256:
        raise ValueError("video index source binding differs")
    if output.exists():
        raise ValueError("immutable query output already exists")
    return questions, report, output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--seven", action="store_true")
    args = parser.parse_args()
    questions, video, output = preflight(args.seven)
    print("SEALED_QUERY_PREFLIGHT_VERIFIED", video["package_sha256"],
          len(questions), flush=True)
    if not args.execute:
        return 0
    transport = DurableN6EmbeddingTransport(CACHE)
    result = build_temporal_query_batch(
        VIDEO, PREP, CAPTIONS, questions, output_dir=output,
        package_id=("multiq-sealed-query-20260924-v2" if args.seven
                    else "multiq-sealed-query-20260924-v1"),
        base_url="https://provider-proxy.invalid/v1",
        api_key="remote-n6-credential-only",
        batch_size=1, transport=transport,
    )
    verified = verify_temporal_query_batch(
        output, VIDEO, PREP, CAPTIONS, questions
    )
    if verified != result:
        raise ValueError("query package verification differs")
    document = json.loads((output / "temporal-query-batch.json").read_text(
        encoding="utf-8"
    ))
    units = sum(
        int(row["usage"]["total_tokens"])
        for row in document["query_embedding_receipts"]
    )
    rate = Decimal(PRICE_SNAPSHOT["text-embedding-v4"]["input"])
    usd = Decimal(units) * rate / Decimal(1_000_000)
    print("SEALED_QUERY_VERIFIED", result["package_sha256"],
          "requests", len(document["query_embedding_receipts"]),
          "input_tokens", units, "list_price_usd", format(usd, "f"),
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
