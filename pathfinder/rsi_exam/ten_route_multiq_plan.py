"""Outcome-blind public schedule for multi-question ten-route cohorts.

This is an additive plan format. The existing four-arm and single-question
ten-route frozen formats retain their original verifiers and meanings.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from itertools import permutations
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any

from .interleaved_multiq_plan import (
    _canonical, _json, _jsonl, _public_questions, _rank, _sha,
    STRATA, verify_interleaved_plan,
)


SCHEMA = "pathfinder.rsi-exam-ten-route-multiq-plan/v1alpha1"
LIGHT_D_SCHEMA = "pathfinder.rsi-exam-light-derived-supplement-plan/v1alpha1"
MANIFEST = "ten-route-multiq-plan.json"
QUESTIONS = "public-questions.jsonl"
SCHEDULE = "ten-route-multiq-schedule.jsonl"
CHECKSUMS = "SHA256SUMS"
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_OBSERVATIONS = (
    ("D0", "N7", "R", 0, None),
    ("D1", "N7", "I", 0, None),
    ("D2", "N7", "D", 0, None),
    ("D3", "N7", "DC", 0, "miss"),
    ("D3", "N7", "DC", 1, "hit"),
    ("D4", "N8", "R", 0, None),
    ("D5", "N8", "I", 0, None),
    ("D6", "N8", "D", 0, None),
    ("D7", "N8", "DC", 0, "miss"),
    ("D7", "N8", "DC", 1, "hit"),
)
_LIGHT_D_OBSERVATIONS = tuple(
    row for row in _OBSERVATIONS if row[2] in {"D", "DC"}
)
_LIGHT_PROFILES = {
    "frame-only": ("light-derived-supplement", ["sampled_frame_bundle"]),
    "single-summary-fusion": (
        "light-derived-fusion-supplement",
        ["multimodal_digest", "sampled_frame_bundle"],
    ),
}


def is_ten_route_family(schema_version: str) -> bool:
    return schema_version in {SCHEMA, LIGHT_D_SCHEMA}


def observations_for_schema(schema_version: str) -> tuple:
    if schema_version == SCHEMA:
        return _OBSERVATIONS
    if schema_version == LIGHT_D_SCHEMA:
        return _LIGHT_D_OBSERVATIONS
    raise TenRouteMultiQuestionPlanError("unsupported ten-route-family schema")


class TenRouteMultiQuestionPlanError(ValueError):
    """The proposed public cohort or schedule is not source-bound."""


def load_verified_multiq_plan(
    plan_dir: str | Path,
    public_questions: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Dispatch only between the two canonical, immutable plan contracts."""

    root = Path(plan_dir)
    old = root / "interleaved-plan.json"
    new = root / MANIFEST
    _require(old.is_file() != new.is_file(),
             "exactly one supported multi-question plan is required")
    manifest = json.loads((old if old.is_file() else new).read_bytes())
    rows = (list(public_questions) if public_questions is not None else [
        json.loads(line) for line in (root / QUESTIONS).read_bytes().splitlines()
    ])
    if old.is_file():
        report = verify_interleaved_plan(
            root, rows,
            public_source_sha256=manifest["public_source_sha256"],
        )
    else:
        report = verify_ten_route_multiq_plan(
            root, rows,
            public_source_sha256=manifest["public_source_sha256"],
            exposure_inventory_sha256=manifest["exposure_inventory_sha256"],
        )
    return manifest, _public_questions(rows), report


def _require(condition: object, message: str) -> None:
    if not condition:
        raise TenRouteMultiQuestionPlanError(message)


def ten_route_trial_key(experiment_id: str, question_id: str,
                        design_id: str, repetition: int) -> str:
    """Unique across questions, both executors and cache repetitions."""

    _require(isinstance(experiment_id, str) and _ID.fullmatch(experiment_id)
             and isinstance(question_id, str) and _ID.fullmatch(question_id)
             and design_id in {f"D{i}" for i in range(8)}
             and type(repetition) is int and repetition in (0, 1)
             and (repetition == 0 or design_id in {"D3", "D7"}),
             "ten-route trial identity is invalid")
    return f"{experiment_id}|{question_id}|{design_id}|r{repetition:04d}"


def _schedule(rows: Sequence[Mapping[str, Any]], *, seed: str,
              experiment_id: str,
              observations: Sequence[tuple] = _OBSERVATIONS,
              ) -> list[dict[str, Any]]:
    questions = _public_questions(rows)
    by_object: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in questions:
        by_object[row["object_id"]].append(row)
    legacy = (len(by_object) == 3
              and all(len(group) == 2 for group in by_object.values())
              and Counter(row["stratum"] for row in questions)
              == {stratum: 2 for stratum in STRATA})
    if not legacy:
        counts = {len(group) for group in by_object.values()}
        _require(len(by_object) >= 2 and len(counts) == 1
                 and next(iter(counts)) >= 3,
                 "ten-route cohort needs equal multi-question videos")
        _require(all({"causal", "temporal"}
                     <= {row["stratum"] for row in group}
                     for group in by_object.values()),
                 "ten-route cohort needs both causal and temporal questions")
    for object_id, video_rows in by_object.items():
        video_rows.sort(key=lambda row: (
            _rank(seed, "ten-route-question-order", object_id,
                  row["question_id"]), row["question_id"],
        ))
    videos = tuple(sorted(by_object))
    if legacy:
        first = min(permutations(videos), key=lambda order: (
            _rank(seed, "ten-route-first-round", *order), order,
        ))
        possible = [order for order in permutations(videos)
                    if order[0] != first[-1]
                    and len({3 + order.index(oid) - first.index(oid)
                             for oid in videos}) > 1]
        _require(bool(possible), "interleaved revisit distances cannot vary")
        second = min(possible, key=lambda order: (
            _rank(seed, "ten-route-second-round", *order), order,
        ))
        visits = [(round_index, oid)
                  for round_index, order in enumerate((first, second))
                  for oid in order]
    else:
        visits = []
        previous = None
        for round_index in range(next(iter(counts))):
            order = sorted(videos, key=lambda oid: (
                _rank(seed, "ten-route-video-order", str(round_index), oid),
                oid,
            ))
            if order[0] == previous:
                order = order[1:] + order[:1]
            visits.extend((round_index, oid) for oid in order)
            previous = order[-1]
    schedule = []
    for ordinal, (round_index, object_id) in enumerate(visits):
        question = by_object[object_id][round_index]
        blocks = ("N7", "N8")
        if int(_rank(seed, "ten-route-node-order",
                     question["question_id"])[:8], 16) % 2:
            blocks = tuple(reversed(blocks))
        slots = []
        for node in blocks:
            noncache = [row for row in observations
                        if row[1] == node and row[2] != "DC"]
            rotation = int(_rank(seed, "ten-route-arm-order", node,
                                 question["question_id"])[:8], 16) % len(noncache)
            noncache = noncache[rotation:] + noncache[:rotation]
            cache = [row for row in observations
                     if row[1] == node and row[2] == "DC"]
            for design, node_id, arm, repetition, expectation in (
                *noncache, *cache,
            ):
                run_id = (f"{experiment_id}-{ordinal:04d}-{design.lower()}"
                          f"-r{repetition:04d}")
                slots.append({
                    "design_id": design,
                    "executor_node_id": node_id,
                    "arm_id": arm,
                    "repetition": repetition,
                    "cache_expectation": expectation,
                    "run_id": run_id,
                    "cache_episode_id": (
                        f"{experiment_id}-{ordinal:04d}-{node_id.lower()}-dc"
                        if arm == "DC" else None
                    ),
                })
        schedule.append({
            "ordinal": ordinal,
            "round": round_index,
            "question_id": question["question_id"],
            "object_id": object_id,
            "stratum": question["stratum"],
            "public_task_sha256": question["public_task_sha256"],
            "route_slots": slots,
        })
    _require(all(a["object_id"] != b["object_id"]
                 for a, b in zip(schedule, schedule[1:])),
             "adjacent questions use the same video")
    return schedule


def _checksums(root: Path) -> bytes:
    return b"".join(
        f"{_sha((root / name).read_bytes())}  {name}\n".encode("ascii")
        for name in sorted((MANIFEST, QUESTIONS, SCHEDULE))
    )


def freeze_ten_route_multiq_plan(
    public_questions: Sequence[Mapping[str, Any]], *, seed: str,
    experiment_id: str, public_source_sha256: str,
    exposure_inventory_sha256: str, output_dir: str | Path,
    derived_profile: str = "caption-fusion",
) -> dict[str, Any]:
    _require(isinstance(seed, str) and _ID.fullmatch(seed), "seed is invalid")
    _require(isinstance(experiment_id, str) and _ID.fullmatch(experiment_id)
             and len(experiment_id) <= 75, "experiment ID is invalid")
    for name, digest in (("public source", public_source_sha256),
                         ("exposure inventory", exposure_inventory_sha256)):
        _require(isinstance(digest, str) and _DIGEST.fullmatch(digest),
                 f"{name} digest is invalid")
    target = Path(output_dir).resolve()
    _require(not target.exists(), "plan output already exists")
    rows = _public_questions(public_questions)
    _require(derived_profile in {"caption-fusion", *_LIGHT_PROFILES},
             "derived profile is unsupported")
    schema = SCHEMA if derived_profile == "caption-fusion" else LIGHT_D_SCHEMA
    observations = observations_for_schema(schema)
    schedule = _schedule(rows, seed=seed, experiment_id=experiment_id,
                         observations=observations)
    question_bytes = _jsonl(rows)
    schedule_bytes = _jsonl(schedule)
    manifest = {
        "schema_version": schema,
        "seed": seed,
        "experiment_id": experiment_id,
        "public_source_sha256": public_source_sha256,
        "exposure_inventory_sha256": exposure_inventory_sha256,
        "question_count": len(rows),
        "object_count": len({row["object_id"] for row in rows}),
        "route_observation_count": len(rows) * len(observations),
        "public_questions_sha256": _sha(question_bytes),
        "schedule_sha256": _sha(schedule_bytes),
        "hidden_label_values_read": False,
        "task_outcomes_read": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
    }
    if schema == LIGHT_D_SCHEMA:
        profile_name, representations = _LIGHT_PROFILES[derived_profile]
        manifest["derived_representation_ids"] = representations
        manifest["profile"] = profile_name
    manifest["plan_sha256"] = _sha(_canonical(manifest))
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{target.name}.",
                                      dir=target.parent))
    try:
        (stage / QUESTIONS).write_bytes(question_bytes)
        (stage / SCHEDULE).write_bytes(schedule_bytes)
        (stage / MANIFEST).write_bytes(_json(manifest))
        (stage / CHECKSUMS).write_bytes(_checksums(stage))
        verify_ten_route_multiq_plan(
            stage, rows, public_source_sha256=public_source_sha256,
            exposure_inventory_sha256=exposure_inventory_sha256,
        )
        os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return verify_ten_route_multiq_plan(
        target, rows, public_source_sha256=public_source_sha256,
        exposure_inventory_sha256=exposure_inventory_sha256,
    )


def verify_ten_route_multiq_plan(
    plan_dir: str | Path, public_questions: Sequence[Mapping[str, Any]], *,
    public_source_sha256: str, exposure_inventory_sha256: str,
) -> dict[str, Any]:
    root = Path(plan_dir).resolve()
    _require(root.is_dir(), "ten-route multi-question plan is absent")
    _require({path.name for path in root.iterdir()}
             == {MANIFEST, QUESTIONS, SCHEDULE, CHECKSUMS},
             "ten-route multi-question plan file set changed")
    _require((root / CHECKSUMS).read_bytes() == _checksums(root),
             "ten-route multi-question plan checksums differ")
    data = (root / MANIFEST).read_bytes()
    manifest = json.loads(data)
    _require(data == _json(manifest), "plan manifest is not canonical")
    digest = manifest.pop("plan_sha256", None)
    _require(digest == _sha(_canonical(manifest)), "plan digest differs")
    manifest["plan_sha256"] = digest
    rows = _public_questions(public_questions)
    schema = manifest.get("schema_version")
    observations = observations_for_schema(schema)
    light_profile = next((value for value in _LIGHT_PROFILES.values()
                          if manifest.get("profile") == value[0]), None)
    _require(schema != LIGHT_D_SCHEMA or light_profile is not None,
             "light-derived profile is unsupported")
    schedule = _schedule(rows, seed=manifest["seed"],
                         experiment_id=manifest["experiment_id"],
                         observations=observations)
    _require(
        manifest == {
            "schema_version": schema,
            "seed": manifest["seed"],
            "experiment_id": manifest["experiment_id"],
            "public_source_sha256": public_source_sha256,
            "exposure_inventory_sha256": exposure_inventory_sha256,
            "question_count": len(rows),
            "object_count": len({row["object_id"] for row in rows}),
            "route_observation_count": len(observations) * len(rows),
            "public_questions_sha256": _sha(_jsonl(rows)),
            "schedule_sha256": _sha(_jsonl(schedule)),
            "hidden_label_values_read": False,
            "task_outcomes_read": False,
            "workflow_submitted": False,
            "credentials_recorded": False,
            "plan_sha256": digest,
            **({"derived_representation_ids": light_profile[1],
                "profile": light_profile[0]}
               if schema == LIGHT_D_SCHEMA else {}),
        }
        and (root / QUESTIONS).read_bytes() == _jsonl(rows)
        and (root / SCHEDULE).read_bytes() == _jsonl(schedule),
        "ten-route plan differs from the frozen public schedule",
    )
    return {
        "status": "VERIFIED_TEN_ROUTE_MULTIQ_PLAN_NOT_ADMITTED",
        "plan_sha256": digest,
        "question_count": len(rows),
        "object_count": len({row["object_id"] for row in rows}),
        "route_observation_count": len(schedule) * len(observations),
        "workflow_submitted": False,
        "credentials_recorded": False,
    }
