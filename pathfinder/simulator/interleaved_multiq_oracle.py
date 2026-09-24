"""Freeze six NextQA labels on N1 without exporting their values.

This is a private N1-only construction boundary.  Public task and CSV
identities are checked before any label source is written.  The returned
report contains counts and commitments, never an answer option.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import shutil
import socket
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..distributed.scoring import MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
from ..rsi_exam.ten_route_multiq_plan import load_verified_multiq_plan
from .hidden_oracle import (
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    build_n1_hidden_label_record,
    build_n1_oracle_package,
    build_n1_public_task_binding,
    verify_n1_oracle_package,
)

_QUESTION_ID = re.compile(r"nextqa-val-([0-9]+)-q([0-9]+)\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class InterleavedOracleError(ValueError):
    """Private source does not exactly bind the frozen public questions."""


def _require(value: object, message: str) -> None:
    if not value:
        raise InterleavedOracleError(message)


def _json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def build_interleaved_n1_oracle(
    *,
    plan_dir: str | Path,
    public_questions: Sequence[Mapping[str, Any]],
    public_source_sha256: str,
    official_csv_path: str | Path,
    official_csv_sha256: str,
    oracle_id: str,
    output_dir: str | Path,
    private_root: str | Path,
) -> dict[str, Any]:
    """Make a new private package; call only on N1 with a private output root."""

    _require(socket.gethostname() == "pathfinder-n1",
             "private oracle may only be built on N1")
    private = Path(private_root).resolve()
    _require(private.is_dir() and not private.is_symlink(),
             "N1 private root is missing or symbolic")
    csv_path = Path(official_csv_path).resolve()
    target = Path(output_dir).resolve()
    _require(csv_path.is_relative_to(private)
             and target.is_relative_to(private),
             "oracle inputs and output must remain below N1 private root")
    plan_doc, _, plan = load_verified_multiq_plan(
        plan_dir, public_questions,
    )
    _require(plan_doc["public_source_sha256"] == public_source_sha256,
             "oracle public source differs from the verified plan")
    _require(isinstance(official_csv_sha256, str)
             and _SHA256.fullmatch(official_csv_sha256),
             "official CSV digest is invalid")
    csv_bytes = csv_path.read_bytes()
    _require(hashlib.sha256(csv_bytes).hexdigest() == official_csv_sha256,
             "official CSV bytes differ from their frozen digest")
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig")))
    _require(reader.fieldnames is not None, "official CSV has no header")
    required = {"video", "qid", "question", "answer", "a0", "a1", "a2",
                "a3", "a4"}
    _require(required <= set(reader.fieldnames),
             "official CSV lacks required public or private columns")
    wanted: dict[tuple[str, str], Mapping[str, Any]] = {}
    for question in public_questions:
        match = _QUESTION_ID.fullmatch(str(question.get("question_id")))
        _require(match is not None
                 and question["object_id"] == f"nextqa-val-{match.group(1)}",
                 "frozen question ID does not bind its NextQA video")
        key = (match.group(1), match.group(2))
        _require(key not in wanted, "frozen NextQA question identity repeats")
        wanted[key] = question
    by_key: dict[tuple[str, str], dict[str, str]] = {}
    for row in reader:
        key = (row.get("video", ""), row.get("qid", ""))
        if key in wanted:
            _require(key not in by_key, "official CSV question repeats")
            by_key[key] = row
    _require(set(by_key) == set(wanted),
             "official CSV is missing a frozen public question")
    labels = []
    for key, public in wanted.items():
        source = by_key[key]
        _require(source["question"] == public["question"],
                 "official CSV question text differs from public task")
        options = [
            {"option_id": chr(ord("A") + index), "text": source[f"a{index}"]}
            for index in range(5)
        ]
        _require(options == public["answer_options"],
                 "official CSV options differ from public task")
        task = build_n1_public_task_binding(
            workload_id=public["question_id"],
            object_id=public["object_id"],
            task_class_id=public["stratum"],
            question=public["question"],
            answer_options=options,
            success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        )
        _require(task["task_binding_sha256"] == public["public_task_sha256"],
                 "N1 task digest differs from frozen public question")
        try:
            answer_index = int(source["answer"])
        except (TypeError, ValueError) as exc:
            raise InterleavedOracleError("official answer index is invalid") from exc
        _require(str(answer_index) == source["answer"]
                 and 0 <= answer_index < 5,
                 "official answer index is invalid")
        labels.append(build_n1_hidden_label_record(
            task, correct_answer_id=chr(ord("A") + answer_index),
        ))
    labels.sort(key=lambda row: (row["object_id"],
                                 row["task_binding_sha256"]))
    _require(not target.exists(), "private oracle output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".multiq-n1-", dir=target.parent))
    try:
        source_path = stage / "hidden-label-source.json"
        source_document = {
            "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
            "logical_node_id": "N1",
            "oracle_id": oracle_id,
            "labels": labels,
            "credentials_recorded": False,
        }
        with os.fdopen(os.open(source_path,
                               os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                       "wb") as handle:
            handle.write(_json(source_document))
        package = stage / "n1-oracle-package"
        build_n1_oracle_package(source_path, output_dir=package)
        verified = verify_n1_oracle_package(package)
        _require(verified["label_count"] == plan["question_count"],
                 "N1 private label count differs from frozen plan")
        os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return {
        "status": "FROZEN_INTERLEAVED_N1_ORACLE",
        "label_count": len(labels),
        "oracle_id": oracle_id,
        "public_task_set_sha256": verified["public_task_set_sha256"],
        "hidden_label_values_returned": False,
        "credentials_recorded": False,
    }


__all__ = ["InterleavedOracleError", "build_interleaved_n1_oracle"]
