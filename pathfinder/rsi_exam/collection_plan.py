"""Outcome-blind cohort selection and trace-collection planning for RSI-Exam.

The planner consumes only a public task set and an operator-authored cohort
specification.  It never accepts outcomes, predictions, labels, or runtime
evidence as inputs.  Selected objects are assigned to exactly one split and
expanded into the frozen Pathfinder D0--D7 action/state matrix.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .offline_replay import (
    CHECKSUMS_NAME,
    OfflineReplayError,
    _atomic_write,
    _checksum_bytes,
    _compact_json_bytes,
    _json_bytes,
    _jsonl_bytes,
    _load_json_bytes,
    _require,
    _sha256,
    _verify_checksum_directory,
)


COLLECTION_SPEC_SCHEMA_VERSION = "pathfinder.rsi-exam-cohort-spec/v1alpha1"
COLLECTION_PLAN_SCHEMA_VERSION = (
    "pathfinder.rsi-exam-trace-collection-plan/v1alpha1"
)
PUBLIC_TASK_SET_SCHEMA_VERSION = "pathfinder.public-task-set/v1alpha1"
MANIFEST_NAME = "collection-manifest.json"
CASES_NAME = "selected-cases.jsonl"
OPERATIONS_NAME = "collection-operations.jsonl"
SPLITS_NAME = "split-manifest.json"
README_NAME = "README.md"
PLAN_FILES = frozenset({
    MANIFEST_NAME,
    CASES_NAME,
    OPERATIONS_NAME,
    SPLITS_NAME,
    README_NAME,
    CHECKSUMS_NAME,
})
SPLIT_ORDER = ("train", "development", "test", "fixture")
ACTION_STATE_MATRIX = (
    ("D0", "default"),
    ("D1", "default"),
    ("D2", "default"),
    ("D3", "cache-miss"),
    ("D3", "cache-hit"),
    ("D4", "default"),
    ("D5", "default"),
    ("D6", "default"),
    ("D7", "cache-miss"),
    ("D7", "cache-hit"),
)


def _strict_file(path: str | Path, label: str) -> tuple[Path, bytes, Any]:
    source = Path(path).resolve()
    _require(source.is_file() and not source.is_symlink(), f"{label} is missing")
    raw = source.read_bytes()
    _require(b"\r" not in raw, f"{label} contains CR bytes")
    value = _load_json_bytes(raw, label)
    _require(isinstance(value, dict), f"{label} must be a JSON object")
    return source, raw, value


def _allowed_keys(
    value: Mapping[str, Any],
    allowed: set[str] | frozenset[str],
    label: str,
) -> None:
    unknown = set(value) - set(allowed)
    _require(not unknown, f"{label} contains unsupported fields: {sorted(unknown)}")


def _nonempty_string(value: Any, label: str) -> str:
    _require(isinstance(value, str) and bool(value), f"{label} must be a string")
    return value


def _positive_integer(value: Any, label: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value > 0,
        f"{label} must be a positive integer",
    )
    return value


def _validate_public_task_set(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    _allowed_keys(
        value,
        {
            "credentials_recorded",
            "label_values_included",
            "schema_version",
            "task_plane_id",
            "tasks",
        },
        "public task set",
    )
    _require(
        value.get("schema_version") == PUBLIC_TASK_SET_SCHEMA_VERSION,
        "unsupported public task-set schema",
    )
    _require(value.get("credentials_recorded") is False, "task set records credentials")
    _require(value.get("label_values_included") is False, "task set includes labels")
    _nonempty_string(value.get("task_plane_id"), "task_plane_id")
    tasks = value.get("tasks")
    _require(isinstance(tasks, list) and bool(tasks), "public tasks are missing")
    result: list[dict[str, Any]] = []
    seen_bindings: set[str] = set()
    for index, raw in enumerate(tasks):
        label = f"public task {index}"
        _require(isinstance(raw, dict), f"{label} is not an object")
        _allowed_keys(
            raw,
            {
                "answer_options",
                "credentials_recorded",
                "object_id",
                "question",
                "schema_version",
                "success_scoring_rule",
                "task_binding_sha256",
                "task_class_id",
                "workload_id",
            },
            label,
        )
        _require(
            raw.get("credentials_recorded") is False,
            f"{label} records credentials",
        )
        object_id = _nonempty_string(raw.get("object_id"), f"{label} object_id")
        workload_id = _nonempty_string(raw.get("workload_id"), f"{label} workload_id")
        binding = _nonempty_string(
            raw.get("task_binding_sha256"),
            f"{label} task_binding_sha256",
        )
        _require(
            re.fullmatch(r"[0-9a-f]{64}", binding) is not None,
            f"{label} binding is invalid",
        )
        _require(binding not in seen_bindings, "public task binding is duplicated")
        seen_bindings.add(binding)
        question = _nonempty_string(raw.get("question"), f"{label} question")
        _nonempty_string(raw.get("task_class_id"), f"{label} task_class_id")
        _nonempty_string(
            raw.get("success_scoring_rule"),
            f"{label} success_scoring_rule",
        )
        options = raw.get("answer_options")
        _require(
            isinstance(options, list) and len(options) >= 2,
            f"{label} options are invalid",
        )
        option_ids: set[str] = set()
        for option_index, option in enumerate(options):
            _require(isinstance(option, dict), f"{label} option is not an object")
            _allowed_keys(option, {"option_id", "text"}, f"{label} option")
            option_id = _nonempty_string(
                option.get("option_id"),
                f"{label} option {option_index} id",
            )
            _nonempty_string(option.get("text"), f"{label} option {option_index} text")
            _require(option_id not in option_ids, f"{label} repeats option ID")
            option_ids.add(option_id)
        result.append({
            "object_id": object_id,
            "workload_id": workload_id,
            "task_binding_sha256": binding,
            "task_class_id": raw["task_class_id"],
            "question_sha256": _sha256(question.encode("utf-8")),
            "answer_option_count": len(options),
            "public_task_sha256": _sha256(_compact_json_bytes(raw)),
        })
    return result


def _validate_spec(value: Mapping[str, Any]) -> dict[str, Any]:
    _allowed_keys(
        value,
        {
            "cohort_id",
            "collection_repetitions",
            "schema_version",
            "selection_seed",
            "split_stratum_targets",
            "stratum_by_workload",
        },
        "cohort spec",
    )
    _require(
        value.get("schema_version") == COLLECTION_SPEC_SCHEMA_VERSION,
        "unsupported cohort-spec schema",
    )
    cohort_id = _nonempty_string(value.get("cohort_id"), "cohort_id")
    _require("/" not in cohort_id and "\\" not in cohort_id, "cohort_id is invalid")
    seed = _nonempty_string(value.get("selection_seed"), "selection_seed")
    repetitions = _positive_integer(
        value.get("collection_repetitions"),
        "collection_repetitions",
    )
    mapping = value.get("stratum_by_workload")
    _require(isinstance(mapping, dict) and bool(mapping), "stratum mapping is missing")
    normalized_mapping: dict[str, str] = {}
    for workload, stratum in mapping.items():
        normalized_mapping[
            _nonempty_string(workload, "workload mapping key")
        ] = _nonempty_string(stratum, "workload stratum")
    targets = value.get("split_stratum_targets")
    _require(isinstance(targets, dict) and bool(targets), "split targets are missing")
    normalized_targets: dict[str, dict[str, int]] = {}
    for split, raw_targets in targets.items():
        _require(split in SPLIT_ORDER, f"unsupported split {split!r}")
        _require(
            isinstance(raw_targets, dict) and bool(raw_targets),
            f"{split} targets are missing",
        )
        normalized_targets[split] = {}
        for stratum, count in raw_targets.items():
            normalized_targets[split][
                _nonempty_string(stratum, f"{split} stratum")
            ] = _positive_integer(count, f"{split}/{stratum} target")
    target_strata = {
        stratum for values in normalized_targets.values() for stratum in values
    }
    _require(
        target_strata <= set(normalized_mapping.values()),
        "split targets name unmapped strata",
    )
    return {
        "cohort_id": cohort_id,
        "selection_seed": seed,
        "collection_repetitions": repetitions,
        "stratum_by_workload": normalized_mapping,
        "split_stratum_targets": normalized_targets,
    }


def _stable_rank(seed: str, *parts: str) -> str:
    payload = "\0".join((seed, *parts)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _eligible_tasks(
    tasks: Sequence[Mapping[str, Any]],
    spec: Mapping[str, Any],
) -> list[dict[str, Any]]:
    mapping = spec["stratum_by_workload"]
    result: list[dict[str, Any]] = []
    for task in tasks:
        stratum = mapping.get(task["workload_id"])
        if stratum is None:
            continue
        result.append({**task, "stratum": stratum})
    return result


def _candidate_summary(
    tasks: Sequence[Mapping[str, Any]],
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    eligible = _eligible_tasks(tasks, spec)
    candidates_by_stratum: dict[str, set[str]] = defaultdict(set)
    for task in eligible:
        candidates_by_stratum[task["stratum"]].add(task["object_id"])
    required = Counter()
    for values in spec["split_stratum_targets"].values():
        required.update(values)
    strata = sorted(set(required) | set(candidates_by_stratum))
    rows = []
    ready = True
    for stratum in strata:
        available = len(candidates_by_stratum[stratum])
        target = required[stratum]
        shortfall = max(0, target - available)
        ready = ready and shortfall == 0
        rows.append({
            "stratum": stratum,
            "available_distinct_objects": available,
            "required_distinct_objects": target,
            "minimum_shortfall": shortfall,
        })
    return {
        "eligible_task_count": len(eligible),
        "eligible_distinct_object_count": len({row["object_id"] for row in eligible}),
        "required_case_count": sum(required.values()),
        "strata": rows,
        "minimum_count_gate_satisfied": ready,
    }


def audit_collection_candidates(
    public_task_set: str | Path,
    cohort_spec: str | Path,
) -> dict[str, Any]:
    """Return an outcome-blind readiness audit without freezing a plan."""

    _, task_raw, task_value = _strict_file(public_task_set, "public task set")
    _, spec_raw, spec_value = _strict_file(cohort_spec, "cohort spec")
    tasks = _validate_public_task_set(task_value)
    spec = _validate_spec(spec_value)
    summary = _candidate_summary(tasks, spec)
    assignment_satisfied = False
    if summary["minimum_count_gate_satisfied"]:
        try:
            _select_cases(tasks, spec)
        except OfflineReplayError:
            assignment_satisfied = False
        else:
            assignment_satisfied = True
    return {
        "status": (
            "READY_FOR_OUTCOME_BLIND_SELECTION"
            if assignment_satisfied
            else (
                "BLOCKED_VIDEO_DISJOINT_ASSIGNMENT"
                if summary["minimum_count_gate_satisfied"]
                else "BLOCKED_INSUFFICIENT_PUBLIC_CANDIDATES"
            )
        ),
        "cohort_id": spec["cohort_id"],
        "public_task_set_sha256": _sha256(task_raw),
        "cohort_spec_sha256": _sha256(spec_raw),
        **summary,
        "video_disjoint_assignment_satisfied": assignment_satisfied,
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }


def _slots(spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    slots: list[dict[str, Any]] = []
    for split in SPLIT_ORDER:
        targets = spec["split_stratum_targets"].get(split, {})
        for stratum in sorted(targets):
            for ordinal in range(targets[stratum]):
                slots.append({
                    "split": split,
                    "stratum": stratum,
                    "ordinal": ordinal,
                })
    return slots


def _select_cases(
    tasks: Sequence[Mapping[str, Any]],
    spec: Mapping[str, Any],
) -> list[dict[str, Any]]:
    eligible = _eligible_tasks(tasks, spec)
    by_stratum_object: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for task in eligible:
        by_stratum_object[(task["stratum"], task["object_id"])].append(dict(task))
    objects_by_stratum: dict[str, list[str]] = defaultdict(list)
    for stratum, object_id in by_stratum_object:
        objects_by_stratum[stratum].append(object_id)
    seed = spec["selection_seed"]
    slots = _slots(spec)
    candidates_by_slot: list[list[str]] = []
    for slot in slots:
        candidates = sorted(
            set(objects_by_stratum[slot["stratum"]]),
            key=lambda object_id: (
                _stable_rank(
                    seed,
                    slot["split"],
                    slot["stratum"],
                    str(slot["ordinal"]),
                    object_id,
                ),
                object_id,
            ),
        )
        candidates_by_slot.append(candidates)

    order = sorted(
        range(len(slots)),
        key=lambda index: (
            len(candidates_by_slot[index]),
            slots[index]["stratum"],
            slots[index]["split"],
            slots[index]["ordinal"],
        ),
    )
    object_to_slot: dict[str, int] = {}
    slot_to_object: dict[int, str] = {}

    def assign(slot_index: int, visited: set[str]) -> bool:
        for object_id in candidates_by_slot[slot_index]:
            if object_id in visited:
                continue
            visited.add(object_id)
            displaced = object_to_slot.get(object_id)
            if displaced is None or assign(displaced, visited):
                object_to_slot[object_id] = slot_index
                slot_to_object[slot_index] = object_id
                return True
        return False

    for slot_index in order:
        _require(
            assign(slot_index, set()),
            "public candidate pool cannot satisfy video-disjoint split targets",
        )

    selected: list[dict[str, Any]] = []
    for slot_index, slot in enumerate(slots):
        object_id = slot_to_object[slot_index]
        task_candidates = by_stratum_object[(slot["stratum"], object_id)]
        task = min(
            task_candidates,
            key=lambda row: (
                _stable_rank(
                    seed,
                    "question",
                    object_id,
                    row["task_binding_sha256"],
                ),
                row["task_binding_sha256"],
            ),
        )
        selected.append({
            "case_id": object_id,
            "object_id": object_id,
            "split": slot["split"],
            "stratum": slot["stratum"],
            "workload_id": task["workload_id"],
            "task_class_id": task["task_class_id"],
            "task_binding_sha256": task["task_binding_sha256"],
            "public_task_sha256": task["public_task_sha256"],
            "question_sha256": task["question_sha256"],
            "answer_option_count": task["answer_option_count"],
        })
    return sorted(selected, key=lambda row: row["case_id"])


def _collection_operations(
    cases: Sequence[Mapping[str, Any]],
    repetitions: int,
) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    for case in cases:
        for repetition in range(repetitions):
            prior_miss: dict[tuple[str, str], str] = {}
            for sequence, (action_id, state_variant) in enumerate(ACTION_STATE_MATRIX):
                identity = {
                    "case_id": case["case_id"],
                    "action_id": action_id,
                    "state_variant": state_variant,
                    "repetition": repetition,
                }
                operation_id = "op-" + _sha256(_compact_json_bytes(identity))[:24]
                node_id = "N7" if action_id in {"D0", "D1", "D2", "D3"} else "N8"
                dependency = None
                if state_variant == "cache-miss":
                    prior_miss[(node_id, action_id)] = operation_id
                elif state_variant == "cache-hit":
                    dependency = prior_miss[(node_id, action_id)]
                operations.append({
                    "operation_id": operation_id,
                    "case_id": case["case_id"],
                    "object_id": case["object_id"],
                    "split": case["split"],
                    "stratum": case["stratum"],
                    "action_id": action_id,
                    "executor_node_id": node_id,
                    "state_variant": state_variant,
                    "repetition": repetition,
                    "sequence_within_repetition": sequence,
                    "requires_prior_operation_id": dependency,
                    "requires_fresh_cache_state": state_variant == "cache-miss",
                    "requires_frozen_index": action_id in {"D1", "D5"},
                })
    return operations


def _readme(cohort_id: str) -> bytes:
    return (
        f"# RSI-Exam trace collection plan: `{cohort_id}`\n\n"
        "This immutable package selects cases using only public task metadata. "
        "It binds a video-disjoint split and expands every selected case into "
        "the same Pathfinder D0--D7 action/state matrix.\n\n"
        "It is a collection plan, not experiment evidence. It contains no "
        "task outcome, prediction, correct answer, hidden label, credential, "
        "or private runtime endpoint. Collection requires the separately "
        "verified UpCloud/FlowMesh environment and all runbook gates.\n\n"
        "Cache-hit operations depend on the matching measured miss. Indexed "
        "actions require a frozen per-object index and separate one-time build "
        "accounting. Missing cells must remain missing; do not interpolate.\n"
    ).encode("utf-8")


def _derive_documents(
    task_raw: bytes,
    task_value: Mapping[str, Any],
    spec_raw: bytes,
    spec_value: Mapping[str, Any],
    *,
    builder_commit: str,
) -> dict[str, bytes]:
    _require(
        re.fullmatch(r"[0-9a-f]{40}", builder_commit) is not None,
        "builder_commit must be a full Git SHA-1",
    )
    tasks = _validate_public_task_set(task_value)
    spec = _validate_spec(spec_value)
    summary = _candidate_summary(tasks, spec)
    _require(
        summary["minimum_count_gate_satisfied"],
        "public candidate pool does not meet minimum stratum counts",
    )
    cases = _select_cases(tasks, spec)
    operations = _collection_operations(cases, spec["collection_repetitions"])
    split_manifest = {
        case["object_id"]: case["split"]
        for case in sorted(cases, key=lambda row: row["object_id"])
    }
    target_counts = {
        split: dict(sorted(values.items()))
        for split, values in spec["split_stratum_targets"].items()
    }
    actual_counts: dict[str, dict[str, int]] = {}
    for split in SPLIT_ORDER:
        counts = Counter(
            case["stratum"] for case in cases if case["split"] == split
        )
        if counts:
            actual_counts[split] = dict(sorted(counts.items()))
    manifest = {
        "schema_version": COLLECTION_PLAN_SCHEMA_VERSION,
        "cohort_id": spec["cohort_id"],
        "builder_source_commit": builder_commit,
        "selection_algorithm": "seeded-video-disjoint-bipartite-matching-v1",
        "selection_seed": spec["selection_seed"],
        "public_task_set_sha256": _sha256(task_raw),
        "cohort_spec_sha256": _sha256(spec_raw),
        "case_count": len(cases),
        "distinct_object_count": len({case["object_id"] for case in cases}),
        "collection_repetitions": spec["collection_repetitions"],
        "action_state_cell_count_per_repetition": len(ACTION_STATE_MATRIX),
        "operation_count": len(operations),
        "target_split_stratum_counts": target_counts,
        "actual_split_stratum_counts": actual_counts,
        "one_time_index_build_evidence_required_per_object": True,
        "fresh_cache_state_required_per_case_repetition": True,
        "outcome_blind_selection": True,
        "task_outcomes_read": False,
        "predictions_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
        "execution_authorized": False,
        "eligible_for_scientific_claims": False,
    }
    documents = {
        MANIFEST_NAME: _json_bytes(manifest),
        CASES_NAME: _jsonl_bytes(cases),
        OPERATIONS_NAME: _jsonl_bytes(operations),
        SPLITS_NAME: _json_bytes(split_manifest),
        README_NAME: _readme(spec["cohort_id"]),
    }
    documents[CHECKSUMS_NAME] = _checksum_bytes(documents)
    return documents


def freeze_collection_plan(
    public_task_set: str | Path,
    cohort_spec: str | Path,
    *,
    builder_commit: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze an outcome-blind, video-disjoint trace collection plan."""

    _, task_raw, task_value = _strict_file(public_task_set, "public task set")
    _, spec_raw, spec_value = _strict_file(cohort_spec, "cohort spec")
    documents = _derive_documents(
        task_raw,
        task_value,
        spec_raw,
        spec_value,
        builder_commit=builder_commit,
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), "collection plan output directory already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in documents.items():
            _atomic_write(staging / name, payload)
        verify_collection_plan(
            staging,
            public_task_set=public_task_set,
            cohort_spec=cohort_spec,
            builder_commit=builder_commit,
        )
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    manifest = _load_json_bytes(documents[MANIFEST_NAME], MANIFEST_NAME)
    return {
        "status": "FROZEN_OUTCOME_BLIND_COLLECTION_PLAN",
        "cohort_id": manifest["cohort_id"],
        "plan_dir": str(target),
        "plan_sha256": _sha256(documents[CHECKSUMS_NAME]),
        "case_count": manifest["case_count"],
        "operation_count": manifest["operation_count"],
        "execution_authorized": False,
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }


def _load_jsonl(payload: bytes, label: str) -> list[dict[str, Any]]:
    _require(b"\r" not in payload, f"{label} contains CR bytes")
    _require(payload.endswith(b"\n"), f"{label} is not LF terminated")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(payload.splitlines(), start=1):
        value = _load_json_bytes(line, f"{label} line {index}")
        _require(isinstance(value, dict), f"{label} line {index} is not an object")
        rows.append(value)
    return rows


def _verify_internal_documents(
    root: Path,
    entries: Mapping[str, str],
) -> dict[str, Any]:
    _require(
        set(entries) == PLAN_FILES - {CHECKSUMS_NAME},
        "collection plan file set differs",
    )
    actual_files = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual_files == PLAN_FILES, "collection plan directory file set differs")
    manifest_raw = (root / MANIFEST_NAME).read_bytes()
    cases_raw = (root / CASES_NAME).read_bytes()
    operations_raw = (root / OPERATIONS_NAME).read_bytes()
    splits_raw = (root / SPLITS_NAME).read_bytes()
    manifest = _load_json_bytes(manifest_raw, MANIFEST_NAME)
    splits = _load_json_bytes(splits_raw, SPLITS_NAME)
    cases = _load_jsonl(cases_raw, CASES_NAME)
    operations = _load_jsonl(operations_raw, OPERATIONS_NAME)
    _require(isinstance(manifest, dict), "collection manifest is not an object")
    _require(isinstance(splits, dict), "split manifest is not an object")
    _require(
        manifest_raw == _json_bytes(manifest),
        "collection manifest is not canonical",
    )
    _require(splits_raw == _json_bytes(splits), "split manifest is not canonical")
    _require(cases_raw == _jsonl_bytes(cases), "selected cases are not canonical")
    _require(operations_raw == _jsonl_bytes(operations), "operations are not canonical")
    _require(
        manifest.get("schema_version") == COLLECTION_PLAN_SCHEMA_VERSION,
        "unsupported plan schema",
    )
    _require(
        re.fullmatch(r"[0-9a-f]{40}", str(manifest.get("builder_source_commit")))
        is not None,
        "builder source commit is invalid",
    )
    _require(manifest.get("case_count") == len(cases), "case_count differs")
    _require(
        manifest.get("operation_count") == len(operations),
        "operation_count differs",
    )
    _require(
        manifest.get("distinct_object_count")
        == len({row.get("object_id") for row in cases}),
        "object count differs",
    )
    _require(
        manifest.get("outcome_blind_selection") is True,
        "selection is not outcome blind",
    )
    for key in (
        "task_outcomes_read",
        "predictions_read",
        "hidden_label_values_read",
        "credentials_recorded",
        "execution_authorized",
        "eligible_for_scientific_claims",
    ):
        _require(manifest.get(key) is False, f"manifest {key} must be false")
    case_ids = [row.get("case_id") for row in cases]
    object_ids = [row.get("object_id") for row in cases]
    _require(len(case_ids) == len(set(case_ids)), "case IDs are duplicated")
    _require(len(object_ids) == len(set(object_ids)), "objects are not video disjoint")
    _require(set(splits) == set(object_ids), "split manifest objects differ")
    case_fields = {
        "answer_option_count",
        "case_id",
        "object_id",
        "public_task_sha256",
        "question_sha256",
        "split",
        "stratum",
        "task_binding_sha256",
        "task_class_id",
        "workload_id",
    }
    for case in cases:
        _require(set(case) == case_fields, "selected case fields differ")
        _require(
            case.get("case_id") == case.get("object_id"),
            "case/object binding differs",
        )
        _require(case.get("split") in SPLIT_ORDER, "case split is invalid")
        _require(
            splits.get(case["object_id"]) == case["split"],
            "case split binding differs",
        )
        for field in (
            "public_task_sha256",
            "question_sha256",
            "task_binding_sha256",
        ):
            _require(
                re.fullmatch(r"[0-9a-f]{64}", str(case.get(field))) is not None,
                f"case {field} is invalid",
            )
    operation_ids = [row.get("operation_id") for row in operations]
    _require(
        len(operation_ids) == len(set(operation_ids)),
        "operation IDs are duplicated",
    )
    known_cases = set(case_ids)
    known_operations = set(operation_ids)
    by_rep: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for operation in operations:
        _require(
            operation.get("case_id") in known_cases,
            "operation names unknown case",
        )
        repetition = operation.get("repetition")
        _require(
            isinstance(repetition, int) and repetition >= 0,
            "operation repetition is invalid",
        )
        by_rep[(operation["case_id"], repetition)].append(operation)
        dependency = operation.get("requires_prior_operation_id")
        _require(
            dependency is None or dependency in known_operations,
            "operation dependency is missing",
        )
    repetitions = manifest.get("collection_repetitions")
    _positive_integer(repetitions, "manifest repetitions")
    _require(
        len(operations) == len(cases) * repetitions * len(ACTION_STATE_MATRIX),
        "operation matrix size differs",
    )
    actual_counts: dict[str, dict[str, int]] = {}
    for split in SPLIT_ORDER:
        counts = Counter(
            case["stratum"] for case in cases if case["split"] == split
        )
        if counts:
            actual_counts[split] = dict(sorted(counts.items()))
    _require(
        manifest.get("actual_split_stratum_counts") == actual_counts,
        "actual split/stratum counts differ",
    )
    _require(
        manifest.get("target_split_stratum_counts") == actual_counts,
        "target split/stratum counts are not satisfied",
    )
    for case_id in case_ids:
        for repetition in range(repetitions):
            rows = sorted(
                by_rep[(case_id, repetition)],
                key=lambda row: row["sequence_within_repetition"],
            )
            observed = [(row["action_id"], row["state_variant"]) for row in rows]
            _require(
                observed == list(ACTION_STATE_MATRIX),
                "action/state matrix differs",
            )
            positions = {row["operation_id"]: index for index, row in enumerate(rows)}
            for index, row in enumerate(rows):
                expected_node = (
                    "N7"
                    if row["action_id"] in {"D0", "D1", "D2", "D3"}
                    else "N8"
                )
                _require(
                    row.get("executor_node_id") == expected_node,
                    "operation node differs",
                )
                _require(
                    row.get("requires_frozen_index")
                    is (row["action_id"] in {"D1", "D5"}),
                    "operation index requirement differs",
                )
                _require(
                    row.get("requires_fresh_cache_state")
                    is (row["state_variant"] == "cache-miss"),
                    "operation cache requirement differs",
                )
                dependency = row["requires_prior_operation_id"]
                if dependency is not None:
                    _require(
                        positions[dependency] < index,
                        "cache hit does not follow its miss",
                    )
    return {
        "manifest": manifest,
        "cases": cases,
        "operations": operations,
    }


def verify_collection_plan(
    plan_dir: str | Path,
    *,
    public_task_set: str | Path | None = None,
    cohort_spec: str | Path | None = None,
    builder_commit: str | None = None,
) -> dict[str, Any]:
    """Verify a collection plan and optionally rebuild it from public sources."""

    root = Path(plan_dir).resolve()
    _require(root.is_dir(), "collection plan directory is missing")
    entries = _verify_checksum_directory(root)
    package = _verify_internal_documents(root, entries)
    source_bound = public_task_set is not None or cohort_spec is not None
    _require(
        (public_task_set is None) == (cohort_spec is None),
        "both public_task_set and cohort_spec are required for source verification",
    )
    _require(
        not source_bound or builder_commit is not None,
        "builder_commit is required for source verification",
    )
    if builder_commit is not None:
        _require(
            package["manifest"].get("builder_source_commit") == builder_commit,
            "builder source commit differs",
        )
    if source_bound:
        _, task_raw, task_value = _strict_file(public_task_set, "public task set")
        _, spec_raw, spec_value = _strict_file(cohort_spec, "cohort spec")
        expected = _derive_documents(
            task_raw,
            task_value,
            spec_raw,
            spec_value,
            builder_commit=builder_commit,
        )
        for name in PLAN_FILES:
            _require(
                (root / name).read_bytes() == expected[name],
                f"source-bound {name} differs",
            )
    manifest = package["manifest"]
    return {
        "status": "VERIFIED_OUTCOME_BLIND_COLLECTION_PLAN",
        "cohort_id": manifest["cohort_id"],
        "plan_sha256": _sha256((root / CHECKSUMS_NAME).read_bytes()),
        "case_count": manifest["case_count"],
        "operation_count": manifest["operation_count"],
        "source_binding_checked": source_bound,
        "video_disjoint": True,
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }
