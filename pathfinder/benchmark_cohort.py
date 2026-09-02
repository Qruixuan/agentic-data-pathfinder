"""Outcome-blind construction of a prospective NExT-QA pilot cohort."""

from __future__ import annotations

import csv
import json
from collections import Counter
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Mapping

from .distributed.scoring import (
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    validate_workload_manifest,
)


SELECTION_SCHEMA = "pathfinder.benchmark-cohort-selection/v0.1"
OUTPUT_SELECTION_SCHEMA = "pathfinder.workload-expansion-selection/v1alpha1"
COHORT_MANIFEST_SCHEMA = "pathfinder.benchmark-cohort/v0.1"
REQUIRED_COLUMNS = (
    "video",
    "frame_count",
    "question",
    "answer",
    "qid",
    "type",
    "a0",
    "a1",
    "a2",
    "a3",
    "a4",
)
OPTION_IDS = ("A", "B", "C", "D", "E")


class CohortPreparationError(ValueError):
    """Raised when a prospective cohort cannot be constructed exactly."""


def _json_bytes(payload: Any) -> bytes:
    return (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _write(path: Path, content: bytes) -> None:
    path.write_bytes(content)


def _load_config(path: Path) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise CohortPreparationError(
            f"cannot read cohort selection config: {path}"
        ) from exc
    if not isinstance(payload, dict):
        raise CohortPreparationError("cohort selection config must be an object")
    if payload.get("schema_version") != SELECTION_SCHEMA:
        raise CohortPreparationError("unsupported cohort selection schema_version")
    return payload, sha256(raw).hexdigest()


def _rows(path: Path, expected_sha256: str) -> list[dict[str, str]]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise CohortPreparationError(f"cannot read annotation CSV: {path}") from exc
    actual = sha256(raw).hexdigest()
    if actual != expected_sha256:
        raise CohortPreparationError(
            "annotation SHA-256 does not match the pinned selection config: "
            f"expected {expected_sha256}, observed {actual}"
        )
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CohortPreparationError("annotation CSV is not UTF-8") from exc
    reader = csv.DictReader(text.splitlines())
    missing = sorted(set(REQUIRED_COLUMNS) - set(reader.fieldnames or ()))
    if missing:
        raise CohortPreparationError(
            "annotation CSV is missing column(s): " + ", ".join(missing)
        )
    return [dict(row) for row in reader]


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise CohortPreparationError(f"{name} must be a positive integer")
    try:
        result = int(str(value))
    except (TypeError, ValueError) as exc:
        raise CohortPreparationError(f"{name} must be a positive integer") from exc
    if result <= 0:
        raise CohortPreparationError(f"{name} must be a positive integer")
    return result


def _answer_index(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise CohortPreparationError(f"{name} must be an integer from 0 to 4")
    try:
        result = int(str(value))
    except (TypeError, ValueError) as exc:
        raise CohortPreparationError(
            f"{name} must be an integer from 0 to 4"
        ) from exc
    if result not in range(len(OPTION_IDS)):
        raise CohortPreparationError(f"{name} must be an integer from 0 to 4")
    return result


def _sha256(value: Any, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise CohortPreparationError(
            f"{name} must be a lowercase hexadecimal SHA-256 digest"
        )
    return value


def _select(
    config: Mapping[str, Any],
    rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    if config.get("independent_unit") != "source-video":
        raise CohortPreparationError(
            "independent_unit must be 'source-video'"
        )
    if config.get("one_primary_question_per_video") is not True:
        raise CohortPreparationError(
            "one_primary_question_per_video must be the literal true"
        )
    selection_rule = config.get("selection_rule")
    if not isinstance(selection_rule, Mapping):
        raise CohortPreparationError("selection_rule must be an object")
    if selection_rule.get("outcome_blind") is not True:
        raise CohortPreparationError(
            "selection_rule.outcome_blind must be the literal true"
        )
    raw_quota = config.get("quota")
    if not isinstance(raw_quota, list) or not raw_quota:
        raise CohortPreparationError("quota must be a non-empty array")
    excluded_raw = config.get("excluded_video_ids")
    if not isinstance(excluded_raw, list) or not all(
        isinstance(value, str) and value.isdecimal()
        for value in excluded_raw
    ):
        raise CohortPreparationError(
            "excluded_video_ids must be an array of decimal strings"
        )
    excluded = set(excluded_raw)
    if len(excluded) != len(excluded_raw):
        raise CohortPreparationError("excluded_video_ids contains duplicates")
    selection_id = str(config.get("selection_id") or "").strip()
    if not selection_id:
        raise CohortPreparationError("selection_id must be a non-empty string")

    selected: list[dict[str, Any]] = []
    used_videos: set[str] = set()
    seen_strata: set[str] = set()
    quota_total = 0
    for quota_index, raw in enumerate(raw_quota):
        if not isinstance(raw, Mapping):
            raise CohortPreparationError(f"quota[{quota_index}] must be an object")
        stratum = str(raw.get("stratum_id") or "").strip()
        accepted_types = raw.get("accepted_upstream_types")
        count = _positive_int(raw.get("count"), f"quota[{quota_index}].count")
        if not stratum:
            raise CohortPreparationError(
                f"quota[{quota_index}].stratum_id must be a non-empty string"
            )
        if stratum in seen_strata:
            raise CohortPreparationError(
                f"quota contains duplicate stratum_id {stratum!r}"
            )
        seen_strata.add(stratum)
        if not isinstance(accepted_types, list) or not accepted_types or not all(
            isinstance(value, str) and value.strip() for value in accepted_types
        ):
            raise CohortPreparationError(
                f"quota[{quota_index}].accepted_upstream_types is invalid"
            )
        accepted_types = [value.strip() for value in accepted_types]
        if len(set(accepted_types)) != len(accepted_types):
            raise CohortPreparationError(
                f"quota[{quota_index}].accepted_upstream_types has duplicates"
            )
        quota_total += count
        matched = 0
        for row_index, row in enumerate(rows, start=2):
            video_id = str(row["video"]).strip()
            if (
                row["type"].strip() not in accepted_types
                or video_id in excluded
                or video_id in used_videos
            ):
                continue
            if not video_id.isdecimal():
                raise CohortPreparationError(
                    f"annotation row {row_index} has an invalid video ID"
                )
            answer_index = _answer_index(
                row["answer"],
                f"annotation row {row_index} answer",
            )
            question = row["question"].strip()
            options = [row[f"a{index}"].strip() for index in range(5)]
            if not question or any(not option for option in options):
                raise CohortPreparationError(
                    f"annotation row {row_index} has blank question/options"
                )
            qid = str(row["qid"]).strip()
            if not qid.isdecimal():
                raise CohortPreparationError(
                    f"annotation row {row_index} has an invalid qid"
                )
            workload_id = f"{stratum}-nextqa-val-{video_id}-q{qid}"
            selected.append({
                "group": stratum,
                "stratum_id": stratum,
                "type": row["type"].strip(),
                "video_id": video_id,
                "frame_count": _positive_int(
                    row["frame_count"],
                    f"annotation row {row_index} frame_count",
                ),
                "qid": int(qid),
                "question": question,
                "options": options,
                "answer_index": answer_index,
                "answer_text": options[answer_index],
                "workload_id": workload_id,
                "object_id": f"nextqa-val-{video_id}",
                "source_row_id": f"nextqa-val-row-{row_index}",
                "selection_rule_id": selection_id,
            })
            used_videos.add(video_id)
            matched += 1
            if matched == count:
                break
        if matched != count:
            raise CohortPreparationError(
                f"stratum {stratum!r} selected {matched} workloads; "
                f"the frozen quota requires {count}"
            )
    expected = _positive_int(
        config.get("target_workload_count"), "target_workload_count"
    )
    if quota_total != expected:
        raise CohortPreparationError(
            f"quota counts sum to {quota_total}; target_workload_count is "
            f"{expected}"
        )
    if len(selected) != expected:
        raise CohortPreparationError(
            f"selection produced {len(selected)} workloads; expected {expected}"
        )
    if len(used_videos) != len(selected):
        raise CohortPreparationError("selection reused a source video")
    return selected


def prepare_benchmark_cohort(
    *,
    selection_config: str | Path,
    annotation_csv: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Select and freeze an outcome-blind exact-match workload cohort."""
    config_path = Path(selection_config).resolve()
    annotations_path = Path(annotation_csv).resolve()
    target = Path(output_dir).resolve()
    if target.exists():
        raise CohortPreparationError("cohort output directory already exists")
    config, config_sha = _load_config(config_path)
    source = config.get("source")
    if not isinstance(source, Mapping):
        raise CohortPreparationError("source must be an object")
    expected_sha = _sha256(
        source.get("annotation_sha256"), "source.annotation_sha256"
    )
    rows = _rows(annotations_path, expected_sha)
    selected = _select(config, rows)

    workloads = {
        row["workload_id"]: {
            "workload_id": row["workload_id"],
            "object_id": row["object_id"],
            "question": row["question"],
            "answer_options": [
                {"option_id": option_id, "text": text}
                for option_id, text in zip(OPTION_IDS, row["options"])
            ],
            "correct_answer_id": OPTION_IDS[row["answer_index"]],
            "task_class_id": "video_qa",
            "stratum_id": row["stratum_id"],
            "quote_profile_id": "as_designed",
            "latency_multiplier": 1,
            "source_video_id": row["video_id"],
            "source_row_id": row["source_row_id"],
            "selection_rule_id": row["selection_rule_id"],
            "split": "restricted_pilot",
        }
        for row in selected
    }
    validate_workload_manifest(
        workloads,
        tuple(workloads),
        MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    )
    selection = {
        "schema_version": OUTPUT_SELECTION_SCHEMA,
        "selection_id": config["selection_id"],
        "source": dict(source),
        "selection_config_sha256": config_sha,
        "selection_rule": dict(config["selection_rule"]),
        "excluded_video_ids": list(config["excluded_video_ids"]),
        "added_workloads": selected,
    }
    files = {
        "workloads.json": _json_bytes(workloads),
        "selection.json": _json_bytes(selection),
        "video_ids.txt": (
            "\n".join(row["video_id"] for row in selected) + "\n"
        ).encode("utf-8"),
    }
    counts = Counter(row["group"] for row in selected)
    manifest = {
        "schema_version": COHORT_MANIFEST_SCHEMA,
        "status": "COMPLETE",
        "selection_id": config["selection_id"],
        "selection_config_sha256": config_sha,
        "annotation_sha256": expected_sha,
        "success_scoring_rule": MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        "workload_count": len(selected),
        "unique_video_count": len({row["video_id"] for row in selected}),
        "stratum_counts": dict(sorted(counts.items())),
        "excluded_video_overlap": [],
        "outcomes_inspected": False,
        "files_sha256": {
            name: sha256(content).hexdigest()
            for name, content in sorted(files.items())
        },
    }
    files["cohort_manifest.json"] = _json_bytes(manifest)
    sums = "".join(
        f"{sha256(content).hexdigest()}  {name}\n"
        for name, content in sorted(files.items())
    ).encode("utf-8")
    files["SHA256SUMS"] = sums

    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".pathfinder-cohort-", dir=target.parent) as temp:
        staging = Path(temp) / "cohort"
        staging.mkdir()
        for name, content in files.items():
            _write(staging / name, content)
        if target.exists():
            raise CohortPreparationError("cohort output directory already exists")
        staging.rename(target)
    return {
        "status": "COMPLETE",
        "selection_id": config["selection_id"],
        "workload_count": len(selected),
        "unique_video_count": len(selected),
        "stratum_counts": dict(sorted(counts.items())),
        "success_scoring_rule": MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        "output_dir": str(target),
        "outcomes_inspected": False,
    }
