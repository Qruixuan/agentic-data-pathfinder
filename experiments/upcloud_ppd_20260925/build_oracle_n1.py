"""Freeze one PPD oracle from the official annotation, only on N1.

The official answer is accessed inside the N1 private boundary. Only the
one-label public commitment and a credential-free status leave that boundary.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import socket
import tempfile
import unicodedata
from pathlib import Path

from pathfinder.simulator.hidden_oracle import (
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    build_n1_hidden_label_record,
    build_n1_oracle_package,
    build_n1_public_task_binding,
    verify_n1_oracle_package,
)
from pathfinder.simulator.hidden_oracle_commitment import (
    freeze_n1_oracle_preselection_commitment,
    verify_n1_oracle_preselection_commitment,
)


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False,
                   allow_nan=False) + "\n"
    ).encode("utf-8")


def _load_public_task(path: Path) -> dict:
    supplied = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(supplied, dict):
        raise ValueError("public task must be an object")
    names = (
        "workload_id", "object_id", "task_class_id", "question",
        "answer_options", "success_scoring_rule",
    )
    if set(supplied) != {
        "schema_version", *names, "credentials_recorded", "task_binding_sha256"
    }:
        raise ValueError("public task field set changed")
    rebuilt = build_n1_public_task_binding(
        **{name: supplied[name] for name in names}
    )
    if supplied != rebuilt:
        raise ValueError("public task binding is not canonical")
    return rebuilt


def _official_match(
    csv_bytes: bytes, public_task: dict,
) -> dict[str, str]:
    def public_question_words(value: str) -> tuple[str, ...]:
        # The public prompt and official annotation may differ in punctuation.
        # Preserve every letter and digit; never use the answer to choose a row.
        without_punctuation = "".join(
            " " if unicodedata.category(character).startswith("P") else character
            for character in value.casefold()
        )
        return tuple(without_punctuation.split())

    video = public_task["object_id"].removeprefix("nextqa-val-")
    if public_task["object_id"] != f"nextqa-val-{video}" or not video.isdigit():
        raise ValueError("public task object is not a NextQA video")
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig")))
    if reader.fieldnames is None or not {
        "video", "qid", "question", "answer", "a0", "a1", "a2", "a3", "a4"
    } <= set(reader.fieldnames):
        raise ValueError("official annotation schema changed")
    matches = []
    for row in reader:
        if row["video"] != video or public_question_words(
            row["question"]
        ) != public_question_words(public_task["question"]):
            continue
        options = [
            {"option_id": chr(65 + index), "text": row[f"a{index}"]}
            for index in range(5)
        ]
        if options == public_task["answer_options"]:
            matches.append(row)
    if len(matches) != 1:
        raise ValueError("official public question/options do not match uniquely")
    return matches[0]


def build_private_ppd_oracle(
    *, public_task_path: Path, official_csv_path: Path,
    official_csv_sha256: str, private_root: Path, output_dir: Path,
    public_commitment_dir: Path, oracle_id: str,
) -> dict[str, object]:
    if socket.gethostname() != "pathfinder-n1":
        raise ValueError("private oracle may only be built on N1")
    if private_root.is_symlink() or official_csv_path.is_symlink():
        raise ValueError("N1 private roots must not be symbolic links")
    private = private_root.resolve(strict=True)
    source = official_csv_path.resolve(strict=True)
    target = output_dir.resolve()
    public_target = public_commitment_dir.resolve()
    if not private.is_dir() or not source.is_relative_to(private):
        raise ValueError("official annotation is outside the N1 private root")
    if not target.is_relative_to(private) or public_target.is_relative_to(private):
        raise ValueError("private/public oracle output roots overlap")
    if target.exists() or public_target.exists():
        raise ValueError("fresh oracle output already exists")
    if len(official_csv_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in official_csv_sha256
    ):
        raise ValueError("official annotation digest is invalid")
    task = _load_public_task(public_task_path)
    csv_bytes = source.read_bytes()
    if hashlib.sha256(csv_bytes).hexdigest() != official_csv_sha256:
        raise ValueError("official annotation digest differs")
    row = _official_match(csv_bytes, task)
    answer = row["answer"]
    if answer not in {"0", "1", "2", "3", "4"}:
        raise ValueError("official answer index is invalid")
    label = build_n1_hidden_label_record(
        task, correct_answer_id=chr(65 + int(answer)),
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".ppd-n1-oracle-", dir=target.parent))
    try:
        source_path = stage / "hidden-label-source.json"
        source_doc = {
            "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
            "oracle_id": oracle_id,
            "logical_node_id": "N1",
            "labels": [label],
            "credentials_recorded": False,
        }
        with os.fdopen(
            os.open(source_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
            "wb",
        ) as handle:
            handle.write(_canonical(source_doc))
        package = stage / "n1-oracle-package"
        build_n1_oracle_package(source_path, output_dir=package)
        report = verify_n1_oracle_package(package)
        if report["label_count"] != 1:
            raise ValueError("N1 package does not contain exactly one label")
        os.replace(stage, target)
    finally:
        if stage.exists():
            # The stage is private and newly created by this process only.
            import shutil
            shutil.rmtree(stage)
    commitment = freeze_n1_oracle_preselection_commitment(
        target / "n1-oracle-package",
        commitment_id=oracle_id + "-public-commitment",
        output_dir=public_target,
    )
    verified = verify_n1_oracle_preselection_commitment(
        public_target,
        oracle_package_dir=target / "n1-oracle-package",
    )
    if verified["oracle_id"] != oracle_id or verified["label_count"] != 1:
        raise ValueError("public commitment does not bind the new oracle")
    return {
        "status": "VERIFIED_PRIVATE_PPD_N1_ORACLE",
        "oracle_id": oracle_id,
        "label_count": 1,
        "public_task_set_sha256": report["public_task_set_sha256"],
        "commitment_sha256": commitment["commitment_sha256"],
        "hidden_label_values_returned": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-task", type=Path, required=True)
    parser.add_argument("--official-csv", type=Path, required=True)
    parser.add_argument("--official-csv-sha256", required=True)
    parser.add_argument("--private-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--public-commitment-dir", type=Path, required=True)
    parser.add_argument("--oracle-id", required=True)
    args = parser.parse_args()
    try:
        result = build_private_ppd_oracle(
            public_task_path=args.public_task,
            official_csv_path=args.official_csv,
            official_csv_sha256=args.official_csv_sha256,
            private_root=args.private_root,
            output_dir=args.output_dir,
            public_commitment_dir=args.public_commitment_dir,
            oracle_id=args.oracle_id,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as exc:
        # Never display private paths, answer text, or the source CSV.
        print(json.dumps({
            "status": "BLOCKED_PRIVATE_ORACLE_BUILD",
            "error_class": type(exc).__name__,
            "hidden_label_values_returned": False,
        }, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
