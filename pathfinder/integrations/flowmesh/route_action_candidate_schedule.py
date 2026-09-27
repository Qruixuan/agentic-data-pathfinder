"""Freeze every possible PPD route choice before an Agent sees outcomes.

This is an outcome-blind candidate schedule, not runtime admission. The
coordinator must separately bind these fresh run/cache identities to verified
trials before any FlowMesh submission.
"""

from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, Sequence

from .route_action_bridge import (
    RouteActionBridge, _DESIGNS as DESIGN_ARMS, ten_route_trial_key,
)
from .route_action_quotes import FrozenRouteQuoteSource


SCHEMA = "pathfinder.route-action-candidate-schedule/v1"
MANIFEST = "route-action-candidate-schedule.json"
ROWS = "route-action-candidates.jsonl"
CHECKSUMS = "SHA256SUMS"
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_DESIGNS = ("D_base", "D_index", "D_cache", "D_joint")


class RouteActionCandidateScheduleError(ValueError):
    """A candidate differs from its public plan or development quote."""


def _require(value: object, message: str) -> None:
    if not value:
        raise RouteActionCandidateScheduleError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(",", ":")).encode("utf-8")


def _digest(value: Any) -> str:
    return sha256(_canonical(value)).hexdigest()


def _jsonl(rows: Sequence[dict[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _inputs(plan_dir: Path, quote_dir: Path,
            execution_namespace: str) -> tuple[
    RouteActionBridge, FrozenRouteQuoteSource,
]:
    try:
        quote_doc = json.loads((quote_dir / "route-quotes.json").read_bytes())
        quote = FrozenRouteQuoteSource(
            quote_dir,
            plan_sha256=quote_doc["plan_sha256"],
            source_trace_package_sha256=quote_doc[
                "source_trace_package_sha256"
            ],
            rate_card_sha256=quote_doc["rate_card_sha256"],
        )
        bridge = RouteActionBridge(
            plan_dir, plan_dir / "never-created-route-choices.sqlite3",
            price_basis_sha256=quote.price_basis_sha256,
            execution_namespace=execution_namespace,
        )
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise RouteActionCandidateScheduleError(
            "candidate source plan or quote does not verify"
        ) from exc
    _require(bridge.plan_sha256 == quote.plan_sha256,
             "candidate plan differs from development quote")
    return bridge, quote


def _expected(
    *, plan_dir: Path, quote_dir: Path, question_ids: Sequence[str],
    execution_namespace: str, order_seed: str,
) -> tuple[dict[str, Any], bytes]:
    _require(isinstance(execution_namespace, str)
             and _ID.fullmatch(execution_namespace),
             "fresh execution namespace is invalid")
    _require(isinstance(order_seed, str) and _ID.fullmatch(order_seed),
             "design-order seed is invalid")
    _require(isinstance(question_ids, (list, tuple)) and bool(question_ids)
             and all(isinstance(item, str) and item for item in question_ids)
             and len(question_ids) == len(set(question_ids)),
             "selected public questions are invalid")
    bridge, quote = _inputs(plan_dir, quote_dir, execution_namespace)
    _require(tuple(bridge.supported_physical_designs()) == _DESIGNS,
             "physical design vocabulary differs from route bridge")
    schedule = [json.loads(line) for line in (
        plan_dir / "ten-route-multiq-schedule.jsonl"
    ).read_bytes().splitlines()]
    selected = set(question_ids)
    _require(selected <= {row["question_id"] for row in schedule},
             "selected question is absent from verified public plan")
    ordered_questions = [row for row in schedule
                         if row["question_id"] in selected]
    rows: list[dict[str, Any]] = []
    session_count = 0
    for question_index, scheduled in enumerate(ordered_questions):
        question_id = scheduled["question_id"]
        object_id, public_sha = bridge.public_question_identity(question_id)
        _require(object_id == scheduled["object_id"]
                 and public_sha == scheduled["public_task_sha256"],
                 "candidate question differs from verified schedule")
        slots = bridge._slots(question_id)
        episode_by_node = bridge.cache_episode_ids(question_id)
        offset = (int(_digest({
            "domain": "pathfinder.ppd-design-order-offset/v1",
            "seed": order_seed,
        })[:8], 16) + question_index) % len(_DESIGNS)
        designs = _DESIGNS[offset:] + _DESIGNS[:offset]
        for design_order, design in enumerate(designs):
            quotes = quote.quotes_for(question_id, design)
            session_id = "pf-session-" + execution_namespace + "-" + _digest({
                "domain": "pathfinder.route-action-session/v1",
                "plan_sha256": bridge.plan_sha256,
                "question_id": question_id,
                "physical_design_id": design,
                "execution_namespace": execution_namespace,
            })[:24]
            session_count += 1
            for action_number in range(8):
                action_id = f"D{action_number}"
                canonical = slots[(action_id, "miss" if action_number in
                                   (3, 7) else None)]
                if canonical["arm_id"] not in DESIGN_ARMS[design]:
                    continue
                run_id = bridge._run_id(session_id, action_id)
                _require(not any(
                    old["run_id"] == run_id
                    for old in scheduled["route_slots"]
                ), "candidate run ID reuses a historical identity")
                states = ("miss", "hit") if action_number in (3, 7) else (None,)
                for state in states:
                    slot = slots[(action_id, state)]
                    _require((action_id, state) in quotes,
                             "candidate action lacks a measured quote")
                    rows.append({
                        "session_id": session_id,
                        "question_id": question_id,
                        "object_id": object_id,
                        "public_task_sha256": public_sha,
                        "question_order": question_index,
                        "physical_design_id": design,
                        "design_order": design_order,
                        "action_id": action_id,
                        "arm_id": slot["arm_id"],
                        "executor_node_id": slot["executor_node_id"],
                        "cache_state": state,
                        "trial_key": ten_route_trial_key(
                            bridge.experiment_id, question_id, action_id,
                            slot["repetition"],
                        ),
                        "run_id": run_id,
                        "cache_episode_id": (
                            episode_by_node[slot["executor_node_id"]]
                            if slot["arm_id"] == "DC" else None
                        ),
                        "price_basis_sha256": quote.price_basis_sha256,
                    })
    _require(len({row["session_id"] for row in rows}) == session_count,
             "candidate sessions repeat")
    _require(len({(row["run_id"], row["trial_key"])
                  for row in rows}) == len(rows),
             "candidate run/trial identity repeats")
    candidate_bytes = _jsonl(rows)
    manifest = {
        "schema_version": SCHEMA,
        "status": "FROZEN_CANDIDATES_NOT_RUNTIME_ADMISSION",
        "compiler_source_sha256": sha256(
            Path(__file__).read_bytes().replace(b"\r\n", b"\n")
        ).hexdigest(),
        "plan_sha256": bridge.plan_sha256,
        "price_basis_sha256": quote.price_basis_sha256,
        "execution_namespace": execution_namespace,
        "order_seed": order_seed,
        "question_ids": sorted(selected),
        "question_count": len(selected),
        "session_count": session_count,
        "candidate_count": len(rows),
        "candidate_rows_sha256": sha256(candidate_bytes).hexdigest(),
        "outcomes_accessed": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
    }
    manifest["package_sha256"] = _digest(manifest)
    return manifest, candidate_bytes


def _checksums(manifest: bytes, rows: bytes) -> bytes:
    return b"".join(
        sha256(value).hexdigest().encode("ascii") + b"  " + name + b"\n"
        for name, value in sorted(((MANIFEST.encode(), manifest),
                                   (ROWS.encode(), rows)))
    )


def freeze_route_action_candidates(
    *, output_dir: str | Path, plan_dir: str | Path, quote_dir: str | Path,
    question_ids: Sequence[str], execution_namespace: str, order_seed: str,
) -> dict[str, Any]:
    root = Path(output_dir).resolve()
    _require(not root.exists(), "candidate output already exists")
    for frozen in (Path(plan_dir).resolve(), Path(quote_dir).resolve()):
        _require(root != frozen and frozen not in root.parents,
                 "candidate output must not write into a frozen source")
    manifest, rows = _expected(
        plan_dir=Path(plan_dir), quote_dir=Path(quote_dir),
        question_ids=question_ids, execution_namespace=execution_namespace,
        order_seed=order_seed,
    )
    root.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".route-candidates-", dir=root.parent))
    try:
        manifest_bytes = _canonical(manifest) + b"\n"
        (stage / MANIFEST).write_bytes(manifest_bytes)
        (stage / ROWS).write_bytes(rows)
        (stage / CHECKSUMS).write_bytes(_checksums(manifest_bytes, rows))
        verify_route_action_candidates(
            stage, plan_dir=plan_dir, quote_dir=quote_dir,
        )
        os.replace(stage, root)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return verify_route_action_candidates(
        root, plan_dir=plan_dir, quote_dir=quote_dir,
    )


def verify_route_action_candidates(
    candidate_dir: str | Path, *, plan_dir: str | Path,
    quote_dir: str | Path,
) -> dict[str, Any]:
    root = Path(candidate_dir)
    _require(root.is_dir() and {path.name for path in root.iterdir()}
             == {MANIFEST, ROWS, CHECKSUMS},
             "candidate package file set differs")
    raw_manifest = (root / MANIFEST).read_bytes()
    try:
        manifest = json.loads(raw_manifest)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RouteActionCandidateScheduleError(
            "candidate manifest is not JSON"
        ) from exc
    _require(isinstance(manifest, dict)
             and manifest.get("schema_version") == SCHEMA
             and manifest.get("status")
             == "FROZEN_CANDIDATES_NOT_RUNTIME_ADMISSION"
             and isinstance(manifest.get("package_sha256"), str)
             and _DIGEST.fullmatch(manifest["package_sha256"]),
             "candidate manifest identity differs")
    expected, rows = _expected(
        plan_dir=Path(plan_dir), quote_dir=Path(quote_dir),
        question_ids=manifest.get("question_ids"),
        execution_namespace=manifest.get("execution_namespace"),
        order_seed=manifest.get("order_seed"),
    )
    expected_bytes = _canonical(expected) + b"\n"
    _require(raw_manifest == expected_bytes
             and (root / ROWS).read_bytes() == rows
             and (root / CHECKSUMS).read_bytes()
             == _checksums(expected_bytes, rows),
             "candidate package differs from verified public sources")
    return {
        "status": "VERIFIED_CANDIDATES_NOT_RUNTIME_ADMISSION",
        "package_sha256": expected["package_sha256"],
        "question_count": expected["question_count"],
        "session_count": expected["session_count"],
        "candidate_count": expected["candidate_count"],
        "workflow_submitted": False,
        "credentials_recorded": False,
    }
