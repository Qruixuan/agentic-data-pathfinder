"""Outcome-blind, source-bound question schedule for an interleaved pilot.

This is an offline planner, not a full-route runner. It deliberately accepts
only public question fields and keeps unique question and video identities.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..distributed.scoring import MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
from ..simulator.hidden_oracle import build_n1_public_task_binding

SCHEMA = "pathfinder.rsi-exam-interleaved-multiq-plan/v1alpha4"
MANIFEST = "interleaved-plan.json"
QUESTIONS = "public-questions.jsonl"
SCHEDULE = "interleaved-schedule.jsonl"
CHECKSUMS = "SHA256SUMS"
ARMS = ("R", "D", "DC", "I")
STRATA = ("causal", "temporal", "descriptive")
ARM_CONTRACTS = {
    "R": {
        "route_family": "raw",
        "input_kind": "complete-mp4",
        "cache_policy": "disabled",
        "frame_selection": None,
    },
    "D": {
        "route_family": "remote-derived",
        "input_kind": "digest-plus-four-frames",
        "cache_policy": "disabled",
        "frame_selection": "question-independent-uniform",
    },
    "DC": {
        "route_family": "local-cache-derived",
        "input_kind": "digest-plus-four-frames",
        "cache_policy": "shared-episode-video-artifact",
        "frame_selection": "question-independent-uniform",
    },
    "I": {
        "route_family": "indexed-derived",
        "input_kind": "digest-plus-four-frames",
        "cache_policy": "disabled",
        "frame_selection": "query-aware-temporal-index",
    },
}
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


class InterleavedPlanError(ValueError):
    """Raised when a public plan is incomplete or a source binding changes."""


def _require(value: object, message: str) -> None:
    if not value:
        raise InterleavedPlanError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      allow_nan=False, separators=(",", ":")).encode("utf-8")


def _json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=2,
                      sort_keys=True, allow_nan=False).encode("utf-8") + b"\n"


def _jsonl(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _checksums(root: Path) -> bytes:
    return b"".join(
        f"{_sha((root / name).read_bytes())}  {name}\n".encode("ascii")
        for name in sorted((MANIFEST, QUESTIONS, SCHEDULE))
    )


def _public_questions(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    _require(bool(rows), "public question set is empty")
    allowed = {"question_id", "object_id", "stratum", "question",
               "answer_options", "public_task_sha256"}
    result = []
    for row in rows:
        _require(isinstance(row, Mapping) and set(row) == allowed,
                 "public question fields differ; labels/outcomes are refused")
        question_id, object_id, stratum = (row["question_id"],
                                           row["object_id"], row["stratum"])
        question = row["question"]
        options = row["answer_options"]
        digest = row["public_task_sha256"]
        _require(
            isinstance(question_id, str) and _ID.fullmatch(question_id)
            and isinstance(object_id, str) and _ID.fullmatch(object_id)
            and stratum in STRATA
            and isinstance(question, str) and bool(question.strip())
            and len(question) <= 4096
            and isinstance(options, list) and len(options) == 5
            and all(isinstance(option, Mapping)
                    and set(option) == {"option_id", "text"}
                    and option["option_id"] == chr(ord("A") + index)
                    and isinstance(option["text"], str)
                    and bool(option["text"].strip())
                    and len(option["text"]) <= 4096
                    for index, option in enumerate(options))
            and isinstance(digest, str)
            and re.fullmatch(r"[0-9a-f]{64}", digest),
            "public question identity or binding is invalid",
        )
        try:
            task = build_n1_public_task_binding(
                workload_id=question_id,
                object_id=object_id,
                task_class_id=stratum,
                question=question,
                answer_options=options,
                success_scoring_rule=(
                    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                ),
            )
        except ValueError as exc:
            raise InterleavedPlanError(
                "public question cannot form a canonical N1 task"
            ) from exc
        _require(
            digest == task["task_binding_sha256"],
            "public question digest differs from its canonical N1 task",
        )
        result.append({key: row[key] for key in sorted(allowed)})
    result.sort(key=lambda row: row["question_id"])
    _require(len({row["question_id"] for row in result}) == len(result),
             "public question IDs repeat")
    return result


def _rank(seed: str, *parts: str) -> str:
    return _sha(_canonical({"domain": "interleaved-multiq-order-v1",
                            "seed": seed, "parts": list(parts)}))


def interleaved_trial_key(
    experiment_id: str, question_id: str, arm_id: str,
) -> str:
    _require(isinstance(experiment_id, str) and _ID.fullmatch(experiment_id),
             "experiment ID is invalid")
    _require(isinstance(question_id, str) and _ID.fullmatch(question_id),
             "question ID is invalid")
    _require(arm_id in ARMS, "arm ID is invalid")
    return f"{experiment_id}|{question_id}|{arm_id}"


def select_outcome_blind_cohort(
    public_candidates: Sequence[Mapping[str, Any]],
    *, seed: str, excluded_object_ids: Sequence[str], object_count: int = 4,
) -> list[dict[str, Any]]:
    """Select complete video-disjoint question triples from public fields.

    An exclusion set must include every video used in earlier exploratory
    work before the result may be described as a fresh cohort.
    """

    _require(isinstance(seed, str) and _ID.fullmatch(seed), "seed is invalid")
    _require(type(object_count) is int and object_count >= 2,
             "object count is invalid")
    _require(isinstance(excluded_object_ids, Sequence)
             and not isinstance(excluded_object_ids, (str, bytes))
             and all(isinstance(item, str) and _ID.fullmatch(item)
                     for item in excluded_object_ids)
             and len(set(excluded_object_ids)) == len(excluded_object_ids),
             "excluded object IDs are invalid")
    excluded = set(excluded_object_ids)
    rows = _public_questions(public_candidates)
    by_object: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        if row["object_id"] not in excluded:
            by_object[row["object_id"]][row["stratum"]].append(row)
    eligible = [object_id for object_id, strata in by_object.items()
                if set(strata) == set(STRATA)]
    _require(len(eligible) >= object_count,
             "public candidate pool cannot fill the fresh multiq cohort")
    chosen = sorted(
        eligible, key=lambda object_id: (
            _rank(seed, "cohort-video", object_id), object_id
        )
    )[:object_count]
    selected = []
    for object_id in chosen:
        for stratum in STRATA:
            selected.append(min(
                by_object[object_id][stratum],
                key=lambda row: (
                    _rank(seed, "cohort-question", object_id, stratum,
                          row["public_task_sha256"]),
                    row["question_id"],
                ),
            ))
    return sorted(selected, key=lambda row: row["question_id"])


def _schedule(
    questions: Sequence[Mapping[str, Any]], seed: str, experiment_id: str,
) -> list[dict[str, Any]]:
    by_object: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in questions:
        by_object[row["object_id"]].append(row)
    _require(len(by_object) >= 2, "interleaving needs at least two videos")
    sizes = sorted(len(rows) for rows in by_object.values())
    _require(sizes == [3] * len(by_object)
             or (len(by_object) == 2 and sizes == [3, 4]),
             "interleaving requires one question per stratum, or a "
             "two-video seven-question extension")
    _require(all({r["stratum"] for r in rows} == set(STRATA)
                 for rows in by_object.values()),
             "each video needs all three question strata")
    _require(all(len({r["question_id"] for r in rows}) == len(rows)
                 for rows in by_object.values()),
             "interleaved question IDs repeat")
    for object_id, rows in by_object.items():
        rows.sort(key=lambda row: (_rank(seed, "question", object_id,
                                            row["question_id"]), row["question_id"]))
    objects = sorted(by_object)
    rounds = []
    last_object: str | None = None
    for round_index in range(3):
        order = sorted(objects, key=lambda object_id: (
            _rank(seed, "round", str(round_index), object_id), object_id
        ))
        if order[0] == last_object:
            order = order[1:] + order[:1]
        rounds.append(order)
        last_object = order[-1]
    sequence: list[tuple[Mapping[str, Any], int]] = []
    for round_index, order in enumerate(rounds):
        for object_id in order:
            sequence.append((by_object[object_id][round_index], round_index))
    if len(by_object) == 2 and sizes == [3, 4]:
        extra_object = next(object_id for object_id, rows in by_object.items()
                            if len(rows) == 4)
        extra = (by_object[extra_object][3], 3)
        if sequence[-1][0]["object_id"] != extra_object:
            sequence.append(extra)
        else:
            sequence.insert(0, extra)
    result = []
    for ordinal, (row, round_index) in enumerate(sequence):
        object_id = row["object_id"]
        rotation = int(_rank(seed, "arm", row["question_id"])[:8], 16) % 4
        arms = ARMS[rotation:] + ARMS[:rotation]
        result.append({
                "ordinal": ordinal,
                "round": round_index,
                "question_id": row["question_id"],
                "object_id": object_id,
                "stratum": row["stratum"],
                "question_sha256": _sha(row["question"].encode("utf-8")),
                "public_task_sha256": row["public_task_sha256"],
                "arm_order": list(arms),
                "route_slots": [
                    {
                        "arm_id": arm,
                        "run_id": f"{experiment_id}-{ordinal:04d}-{arm.lower()}",
                        "cache_episode_id": (
                            f"{experiment_id}-dc" if arm == "DC" else None
                        ),
                    }
                    for arm in arms
                ],
        })
    _require(all(a["object_id"] != b["object_id"]
                 for a, b in zip(result, result[1:])),
             "adjacent questions reference the same video")
    return result


def freeze_interleaved_plan(
    public_questions: Sequence[Mapping[str, Any]],
    *, seed: str, experiment_id: str, public_source_sha256: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze one fresh, label-free schedule without reading task outcomes."""

    target = Path(output_dir).resolve()
    _require(not target.exists(), "plan output already exists")
    _require(isinstance(seed, str) and _ID.fullmatch(seed), "seed is invalid")
    _require(isinstance(experiment_id, str) and _ID.fullmatch(experiment_id)
             and len(experiment_id) <= 96, "experiment ID is invalid")
    _require(isinstance(public_source_sha256, str)
             and re.fullmatch(r"[0-9a-f]{64}", public_source_sha256),
             "public source digest is invalid")
    rows = _public_questions(public_questions)
    schedule = _schedule(rows, seed, experiment_id)
    question_bytes = _jsonl(rows)
    schedule_bytes = _jsonl(schedule)
    manifest = {
        "schema_version": SCHEMA,
        "seed": seed,
        "experiment_id": experiment_id,
        "public_source_sha256": public_source_sha256,
        "object_count": len({row["object_id"] for row in rows}),
        "question_count": len(rows),
        "route_count": len(rows) * len(ARMS),
        "arm_ids": list(ARMS),
        "arm_contracts": ARM_CONTRACTS,
        "public_questions_sha256": _sha(question_bytes),
        "schedule_sha256": _sha(schedule_bytes),
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
        "workflow_submitted": False,
    }
    manifest["plan_sha256"] = _sha(_canonical(manifest))
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        (stage / QUESTIONS).write_bytes(question_bytes)
        (stage / SCHEDULE).write_bytes(schedule_bytes)
        (stage / MANIFEST).write_bytes(_json(manifest))
        (stage / CHECKSUMS).write_bytes(_checksums(stage))
        verify_interleaved_plan(
            stage, rows, public_source_sha256=public_source_sha256
        )
        os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return verify_interleaved_plan(
        target, rows, public_source_sha256=public_source_sha256
    )


def verify_interleaved_plan(
    plan_dir: str | Path,
    public_questions: Sequence[Mapping[str, Any]],
    *, public_source_sha256: str,
) -> dict[str, Any]:
    root = Path(plan_dir).resolve()
    _require(root.is_dir(), "interleaved plan directory is missing")
    _require({path.name for path in root.iterdir()} ==
             {MANIFEST, QUESTIONS, SCHEDULE, CHECKSUMS},
             "interleaved plan file set changed")
    _require((root / CHECKSUMS).read_bytes() == _checksums(root),
             "interleaved plan checksums differ")
    manifest_bytes = (root / MANIFEST).read_bytes()
    manifest = json.loads(manifest_bytes)
    _require(manifest_bytes == _json(manifest),
             "interleaved manifest is not canonical")
    _require(isinstance(manifest.get("seed"), str)
             and _ID.fullmatch(manifest["seed"]),
             "interleaved seed is invalid")
    _require(isinstance(manifest.get("experiment_id"), str)
             and _ID.fullmatch(manifest["experiment_id"])
             and len(manifest["experiment_id"]) <= 96,
             "interleaved experiment ID is invalid")
    digest = manifest.pop("plan_sha256", None)
    _require(digest == _sha(_canonical(manifest)),
             "interleaved plan digest differs")
    manifest["plan_sha256"] = digest
    rows = _public_questions(public_questions)
    question_bytes = _jsonl(rows)
    schedule = _schedule(rows, manifest["seed"], manifest["experiment_id"])
    schedule_bytes = _jsonl(schedule)
    _require(
        manifest.get("schema_version") == SCHEMA
        and manifest.get("public_source_sha256") == public_source_sha256
        and manifest.get("public_questions_sha256") == _sha(question_bytes)
        and manifest.get("schedule_sha256") == _sha(schedule_bytes)
        and (root / QUESTIONS).read_bytes() == question_bytes
        and (root / SCHEDULE).read_bytes() == schedule_bytes
        and manifest.get("object_count") == len({row["object_id"] for row in rows})
        and manifest.get("question_count") == len(rows)
        and manifest.get("route_count") == len(rows) * len(ARMS)
        and manifest.get("arm_ids") == list(ARMS)
        and manifest.get("arm_contracts") == ARM_CONTRACTS
        and manifest.get("task_outcomes_read") is False
        and manifest.get("hidden_label_values_read") is False
        and manifest.get("credentials_recorded") is False
        and manifest.get("workflow_submitted") is False,
        "interleaved plan differs from public tasks or schedule",
    )
    return {
        "status": "VERIFIED_INTERLEAVED_MULTI_QUESTION_PLAN",
        "plan_sha256": digest,
        "object_count": manifest["object_count"],
        "question_count": manifest["question_count"],
        "route_count": manifest["route_count"],
        "experiment_id": manifest["experiment_id"],
        "public_source_sha256": manifest["public_source_sha256"],
        "workflow_submitted": False,
        "credentials_recorded": False,
    }


def interleaved_cache_episode_bindings(
    plan_dir: str | Path,
    public_questions: Sequence[Mapping[str, Any]],
    *,
    public_source_sha256: str,
) -> dict[tuple[str, str], str]:
    """Bind only DC requests to the one frozen cross-question cache episode.

    The map is keyed by both run and trial identity, as required by the
    catalog-bound route handler.  Non-cache arms can never acquire an episode
    by supplying its ID in a request.
    """

    report = verify_interleaved_plan(
        plan_dir, public_questions,
        public_source_sha256=public_source_sha256,
    )
    root = Path(plan_dir).resolve()
    schedule = [json.loads(line) for line in
                (root / SCHEDULE).read_text(encoding="utf-8").splitlines()]
    bindings: dict[tuple[str, str], str] = {}
    for row in schedule:
        for slot in row["route_slots"]:
            if slot["arm_id"] != "DC":
                _require(slot["cache_episode_id"] is None,
                         "non-cache arm carries a cache episode")
                continue
            episode_id = slot["cache_episode_id"]
            _require(isinstance(episode_id, str) and _ID.fullmatch(episode_id),
                     "DC cache episode ID is invalid")
            key = (
                slot["run_id"],
                interleaved_trial_key(
                    report["experiment_id"], row["question_id"], "DC",
                ),
            )
            _require(key not in bindings, "DC run and trial identity repeats")
            bindings[key] = episode_id
    _require(len(bindings) == report["question_count"],
             "DC cache episode binding count differs")
    _require(len(set(bindings.values())) == 1,
             "DC arms do not share one cache episode")
    return bindings


__all__ = ["ARMS", "InterleavedPlanError", "freeze_interleaved_plan",
           "interleaved_cache_episode_bindings", "interleaved_trial_key",
           "select_outcome_blind_cohort", "verify_interleaved_plan"]
