"""Build a parameterized private oracle on N1; export only its commitment."""
import argparse
import json
from pathlib import Path

from pathfinder.simulator.interleaved_multiq_oracle import build_interleaved_n1_oracle
from pathfinder.simulator.hidden_oracle_commitment import (
    freeze_n1_oracle_preselection_commitment, verify_n1_oracle_preselection_commitment,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan-dir", type=Path, required=True)
    parser.add_argument("--official-csv", type=Path, required=True)
    parser.add_argument("--official-csv-sha256", required=True)
    parser.add_argument("--private-root", type=Path, required=True)
    parser.add_argument("--private-output", type=Path, required=True)
    parser.add_argument("--public-output", type=Path, required=True)
    args = parser.parse_args()
    plan = json.loads((args.plan_dir / "interleaved-plan.json").read_bytes())
    questions = [json.loads(line) for line in (
        args.plan_dir / "public-questions.jsonl").read_bytes().splitlines()]
    report = build_interleaved_n1_oracle(
        plan_dir=args.plan_dir, public_questions=questions,
        public_source_sha256=plan["public_source_sha256"],
        official_csv_path=args.official_csv, official_csv_sha256=args.official_csv_sha256,
        oracle_id=plan["experiment_id"] + "-oracle",
        output_dir=args.private_output, private_root=args.private_root,
    )
    oracle = args.private_output / "n1-oracle-package"
    freeze_n1_oracle_preselection_commitment(
        oracle, commitment_id=plan["experiment_id"] + "-commitment",
        output_dir=args.public_output,
    )
    verified = verify_n1_oracle_preselection_commitment(
        args.public_output, oracle_package_dir=oracle,
    )
    if report["label_count"] != verified["label_count"]:
        raise ValueError("private/public commitment counts differ")
    print(json.dumps({"status": "VERIFIED_N1_PRIVATE_AND_PUBLIC_COMMITMENT",
                      "label_count": report["label_count"],
                      "hidden_label_values_returned": False,
                      "credentials_recorded": False}))


if __name__ == "__main__":
    main()
