"""Freeze the public, exact-input bindings for a 24-route multi-question run.

This is a plumbing gate, not a runtime admission.  In particular, verifying
this package does not authorize a FlowMesh submission or imply that N7/N8 have
mounted these inputs.  The N1 private package never leaves N1.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from ..distributed.scoring import MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
from ..rsi_exam.interleaved_multiq_plan import (
    QUESTIONS,
    SCHEDULE,
    interleaved_trial_key,
    verify_interleaved_plan,
)
from .full_flow_multiq_exact_selection import interleaved_data_agent_plan_bindings
from .hidden_oracle_commitment import verify_n1_oracle_preselection_commitment
from .n3_indexed_data_plane import INDEXED_REPRESENTATION_ID
from .n3_multiq_indexed_data_plane import derive_n3_multiq_question_policies
from .raw_cold_data_plane import PACKAGE_MANIFEST_NAME as N3_MANIFEST

SCHEMA = "pathfinder.interleaved-multiq-route-input-bindings/v1alpha1"
MANIFEST = "route-input-bindings.json"
ROUTES = "route-inputs.jsonl"
CHECKSUMS = "SHA256SUMS"


class InterleavedRouteBindingsError(ValueError):
    """An exact route input differs from its immutable source."""


def _require(value: object, message: str) -> None:
    if not value:
        raise InterleavedRouteBindingsError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _pretty(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2,
                       ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _rows(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8")
            .splitlines() if line]


def _source_sha(root: Path, name: str) -> str:
    return _sha((root / name).read_bytes())


def _expected(
    *, plan_dir: Path, n1_public_commitment_dir: Path,
    n3_package_dir: Path, raw_package_dir: Path, n4_package_dir: Path,
    query_dir: Path, video_index_dir: Path, preparation_dir: Path,
    caption_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    questions = _rows(plan_dir / QUESTIONS)
    plan_document = json.loads((plan_dir / "interleaved-plan.json").read_bytes())
    plan = verify_interleaved_plan(
        plan_dir, questions,
        public_source_sha256=plan_document["public_source_sha256"],
    )
    commitment = verify_n1_oracle_preselection_commitment(
        n1_public_commitment_dir,
    )
    commitment_doc = json.loads((n1_public_commitment_dir /
                                 "n1-oracle-preselection-commitment.json").read_bytes())
    public_tasks = sorted(({
        "object_id": row["object_id"],
        "task_binding_sha256": row["public_task_sha256"],
        "success_scoring_rule": MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        "answer_option_ids": [option["option_id"]
                              for option in row["answer_options"]],
    } for row in questions),
        key=lambda row: (row["object_id"], row["task_binding_sha256"]))
    _require(
        commitment["label_count"] == len(questions)
        and commitment_doc["public_task_set_sha256"]
        == _sha(_canonical(public_tasks))
        and commitment["label_values_returned"] is False,
        "N1 public commitment does not bind the public tasks",
    )
    policies = derive_n3_multiq_question_policies(
        plan_dir=plan_dir, public_questions=questions,
        public_source_sha256=plan["public_source_sha256"],
        query_dir=query_dir, video_index_dir=video_index_dir,
        preparation_dir=preparation_dir, caption_dir=caption_dir,
        raw_package_dir=raw_package_dir,
    )
    access = interleaved_data_agent_plan_bindings(
        plan_dir=plan_dir, public_questions=questions,
        public_source_sha256=plan["public_source_sha256"],
        n3_package_dir=n3_package_dir, raw_package_dir=raw_package_dir,
        question_policies=policies, n4_package_dir=n4_package_dir,
    )
    n3 = json.loads((n3_package_dir / N3_MANIFEST).read_bytes())
    n4 = json.loads((n4_package_dir / "n4-derived-data-package.json").read_bytes())
    raw = {row["object_id"]: row for row in n3["raw_objects"]}
    selected = {(row["object_id"], row["task_binding_sha256"]): row
                for row in n3["question_selections"]}
    derived = {(row["object_id"], row["representation_id"]): row
               for row in n4["objects"]}
    question_by_id = {row["question_id"]: row for row in questions}
    _require(len(question_by_id) == len(questions), "question IDs repeat")

    routes: list[dict[str, Any]] = []
    for round_row in _rows(plan_dir / SCHEDULE):
        question = question_by_id[round_row["question_id"]]
        object_id = question["object_id"]
        task_sha = question["public_task_sha256"]
        _require(round_row["object_id"] == object_id
                 and round_row["public_task_sha256"] == task_sha,
                 "schedule question binding changed")
        for slot in round_row["route_slots"]:
            arm = slot["arm_id"]
            trial_key = interleaved_trial_key(
                plan["experiment_id"], question["question_id"], arm,
            )
            if arm == "R":
                source = raw[object_id]
                required = (("N3", "raw_video", source),)
            elif arm == "I":
                source = selected[(object_id, task_sha)]
                required = (
                    ("N3", INDEXED_REPRESENTATION_ID, source),
                    ("N4", "multimodal_digest",
                     derived[(object_id, "multimodal_digest")]),
                )
            else:
                required = tuple(
                    ("N4", representation, derived[(object_id, representation)])
                    for representation in ("multimodal_digest",
                                           "sampled_frame_bundle")
                )
            inputs = []
            for node, representation, artifact in required:
                key = (trial_key, node, object_id, representation)
                _require(key in access, "route access plan binding is absent")
                inputs.append({
                    "node_id": node,
                    "representation_id": representation,
                    "artifact_sha256": artifact["artifact_sha256"],
                    "artifact_size_bytes": artifact["artifact_size_bytes"],
                    "plan_id": access[key],
                })
            routes.append({
                "ordinal": round_row["ordinal"],
                "question_id": question["question_id"],
                "object_id": object_id,
                "stratum": question["stratum"],
                "public_task_sha256": task_sha,
                "arm_id": arm,
                "trial_key": trial_key,
                "run_id": slot["run_id"],
                "cache_episode_id": slot["cache_episode_id"],
                "inputs": inputs,
            })
    _require(len(routes) == plan["route_count"],
             "route binding count differs from the plan")
    _require(Counter(row["arm_id"] for row in routes)
             == {arm: len(questions) for arm in ("R", "D", "DC", "I")},
             "four-arm route coverage changed")
    _require(len({row["trial_key"] for row in routes}) == len(routes)
             and len({row["run_id"] for row in routes}) == len(routes),
             "route or run identity repeats")
    _require(sum(len(row["inputs"]) for row in routes)
             == len(access) == 7 * len(questions),
             "exact Data Agent binding count changed")
    manifest = {
        "schema_version": SCHEMA,
        "status": "FROZEN_INTERLEAVED_ROUTE_INPUTS_NOT_ADMITTED",
        "plan_sha256": plan["plan_sha256"],
        "n1_public_commitment_sha256": commitment["commitment_sha256"],
        "n3_package_manifest_sha256": _source_sha(n3_package_dir, N3_MANIFEST),
        "n4_package_manifest_sha256": _source_sha(
            n4_package_dir, "n4-derived-data-package.json"),
        "route_count": len(routes),
        "question_count": len(questions),
        "data_agent_binding_count": len(access),
        "runtime_admission_created": False,
        "workflow_submitted": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }
    manifest["routes_sha256"] = _sha(b"".join(_canonical(row) + b"\n"
                                           for row in routes))
    manifest["manifest_sha256"] = _sha(_canonical(manifest))
    return manifest, routes


def _checksums(root: Path) -> bytes:
    return b"".join(f"{_source_sha(root, name)}  {name}\n".encode("ascii")
                    for name in (MANIFEST, ROUTES))


def freeze_interleaved_route_bindings(
    *, output_dir: str | Path, **sources: str | Path,
) -> dict[str, Any]:
    """Freeze all planned real inputs, without authorizing execution."""

    paths = {key: Path(value).resolve() for key, value in sources.items()}
    manifest, routes = _expected(**paths)
    target = Path(output_dir).resolve()
    _require(not target.exists(), "route binding output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".multiq-route-bindings-",
                                  dir=target.parent))
    try:
        (stage / MANIFEST).write_bytes(_pretty(manifest))
        (stage / ROUTES).write_bytes(b"".join(
            _canonical(row) + b"\n" for row in routes))
        (stage / CHECKSUMS).write_bytes(_checksums(stage))
        verify_interleaved_route_bindings(stage, **sources)
        os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return verify_interleaved_route_bindings(target, **sources)


def verify_interleaved_route_bindings(
    binding_dir: str | Path, **sources: str | Path,
) -> dict[str, Any]:
    root = Path(binding_dir).resolve()
    _require(root.is_dir() and {path.name for path in root.iterdir()}
             == {MANIFEST, ROUTES, CHECKSUMS},
             "route binding file set changed")
    _require((root / CHECKSUMS).read_bytes() == _checksums(root),
             "route binding checksums differ")
    paths = {key: Path(value).resolve() for key, value in sources.items()}
    manifest, routes = _expected(**paths)
    _require((root / MANIFEST).read_bytes() == _pretty(manifest)
             and (root / ROUTES).read_bytes() == b"".join(
                 _canonical(row) + b"\n" for row in routes),
             "route input bindings differ from verified sources")
    return {
        "status": "VERIFIED_INTERLEAVED_ROUTE_INPUTS_NOT_ADMITTED",
        "route_count": len(routes),
        "question_count": manifest["question_count"],
        "data_agent_binding_count": manifest["data_agent_binding_count"],
        "manifest_sha256": manifest["manifest_sha256"],
        "runtime_admission_created": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
    }


__all__ = ["InterleavedRouteBindingsError",
           "freeze_interleaved_route_bindings",
           "verify_interleaved_route_bindings"]
