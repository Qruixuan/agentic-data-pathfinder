"""Verify public H48 question-only observations and seal a numeric summary."""

from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from experiments.question_only_h48.probe import canonical, read_inputs


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def verify(source: Path, protocol_path: Path, questions_path: Path,
           commitment_path: Path) -> dict:
    protocol, questions, commitment = read_inputs(
        protocol_path, questions_path, commitment_path)
    expected = {"start.json"} | {
        f"observation-{i:02d}.json" for i in range(len(questions))
    }
    actual = {p.name for p in source.iterdir() if p.is_file()}
    if actual != expected:
        raise ValueError("public observation set is incomplete or has extras")
    start = json.loads((source / "start.json").read_bytes())
    if (start["run_id"] != protocol["run_id"]
            or start["protocol_sha256"] != sha(canonical(protocol))
            or start["question_file_sha256"]
            != protocol["question_file_sha256"]
            or start["public_task_set_sha256"]
            != commitment["public_task_set_sha256"]
            or start["n6_request_ceiling"] != len(questions)
            or start["provider_attempt_ceiling"]
            != protocol["max_provider_attempts"]):
        raise ValueError("start receipt differs from source and budget")
    by_stratum: dict[str, Counter] = {}
    total = Counter()
    price = Decimal(0)
    n6_requests = set()
    n1_requests = set()
    failed_questions = []
    observations = []
    for i, question in enumerate(questions):
        record = json.loads((source / f"observation-{i:02d}.json").read_bytes())
        score = record["n1_score_result"]
        if (record["ordinal"] != i
                or record["question_id"] != question["question_id"]
                or record["object_id"] != question["object_id"]
                or record["stratum"] != question["stratum"]
                or record["public_task_sha256"]
                != question["public_task_sha256"]
                or record["correct"] is not score["correct"]
                or score["status"] != "SCORED"
                or score["node_id"] != "N1"
                or score["oracle_id"] != commitment["oracle_id"]
                or score["run_id"] != protocol["run_id"]
                or score["trial_id"] != question["question_id"]
                or score["task_binding_sha256"]
                != question["public_task_sha256"]
                or score["public_task_set_sha256"]
                != commitment["public_task_set_sha256"]
                or score["prediction_sha256"]
                != record["prediction_sha256"]
                or score["request_sha256"]
                != record["n1_score_request_sha256"]
                or score["hidden_answer_returned"] is not False
                or record["credentials_recorded"] is not False):
            raise ValueError("question or authenticated scoring binding differs")
        score_without_digest = dict(score)
        result_digest = score_without_digest.pop("result_content_sha256")
        if sha(canonical(score_without_digest)) != result_digest:
            raise ValueError("N1 result content digest differs")
        usage = record["provider_usage"]
        if (not isinstance(usage, dict)
                or any(type(usage.get(k)) is not int for k in (
                    "input_units", "cached_input_units", "output_units",
                    "total_units"))
                or usage["input_units"] + usage["output_units"]
                != usage["total_units"]
                or not 0 <= usage["cached_input_units"]
                <= usage["input_units"]):
            raise ValueError("provider token usage is invalid")
        expected_price = (
            Decimal(usage["input_units"] - usage["cached_input_units"])
            * Decimal("0.50")
            + Decimal(usage["cached_input_units"]) * Decimal("0.10")
            + Decimal(usage["output_units"]) * Decimal("3.00")
        ) / Decimal(1_000_000)
        if abs(Decimal(str(record["known_model_list_price_usd"]))
               - expected_price) > Decimal("0.000000001"):
            raise ValueError("frozen list-price calculation differs")
        price += expected_price
        n6_requests.add(record["n6_request_sha256"])
        n1_requests.add(score["score_request_id"])
        group = by_stratum.setdefault(question["stratum"], Counter())
        group["questions"] += 1
        group["correct"] += int(record["correct"])
        total["correct"] += int(record["correct"])
        if not record["correct"]:
            failed_questions.append(question["question_id"])
        observations.append({
            "question_id": question["question_id"],
            "stratum": question["stratum"], "correct": record["correct"],
            "n6_request_sha256": record["n6_request_sha256"],
            "n6_result_sha256": record["n6_result_sha256"],
            "input_units": usage["input_units"],
            "cached_input_units": usage["cached_input_units"],
            "output_units": usage["output_units"],
        })
    if len(n6_requests) != 12 or len(n1_requests) != 12:
        raise ValueError("diagnostic request identities are not unique")
    return {
        "schema_version": "pathfinder.question-only-development-summary/v1",
        "status": "VERIFIED_QUESTION_ONLY_DEVELOPMENT_DIAGNOSTIC",
        "run_id": protocol["run_id"], "question_count": len(questions),
        "correct": total["correct"],
        "incorrect": len(questions) - total["correct"],
        "by_stratum": {k: dict(v) for k, v in sorted(by_stratum.items())},
        "known_model_list_price_usd": str(price),
        "price_basis": protocol["price_basis"],
        "observations": observations,
        "incorrect_question_ids": failed_questions,
        "evaluation_role": protocol["evaluation_role"],
        "eligible_for_scientific_claims": False,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--questions", type=Path, required=True)
    parser.add_argument("--commitment", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.source, args.protocol, args.questions,
                    args.commitment)
    summary = args.source / "summary.json"
    with summary.open("xb") as handle:
        handle.write(canonical(result) + b"\n")
    names = sorted(p for p in args.source.iterdir() if p.is_file()
                   and p.name != "SHA256SUMS")
    with (args.source / "SHA256SUMS").open("xb") as handle:
        for path in names:
            handle.write(f"{sha(path.read_bytes())}  {path.name}\n".encode())
    print(json.dumps({"status": result["status"],
                      "question_count": result["question_count"],
                      "correct": result["correct"],
                      "known_model_list_price_usd":
                      result["known_model_list_price_usd"]}))


if __name__ == "__main__":
    main()
