"""Bind outcome-blind route candidates to a verified runtime admission.

The candidate schedule alone is not permission to run.  The caller must
first verify the source-bound trial admission and pass its exact trial rows
and semantic admission digest to both the freezer and verifier.
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

from .route_action_candidate_schedule import (
    candidate_run_bindings,
    verify_route_action_candidates,
)


SCHEMA = "pathfinder.route-action-runtime-admission/v1"
MANIFEST = "route-action-runtime-admission.json"
ROWS = "route-action-runtime-runs.jsonl"
CHECKSUMS = "SHA256SUMS"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class RouteActionRuntimeAdmissionError(ValueError):
    """Candidate runs do not match the independently admitted trials."""


def _require(value: object, message: str) -> None:
    if not value:
        raise RouteActionRuntimeAdmissionError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(",", ":")).encode("utf-8")


def _expected(
    *, candidate_dir: Path, plan_dir: Path, quote_dir: Path,
    admitted_trials: Sequence[dict[str, Any]], admission_sha256: str,
) -> tuple[bytes, bytes]:
    _require(isinstance(admission_sha256, str)
             and _SHA256.fullmatch(admission_sha256),
             "verified semantic admission digest is invalid")
    candidate = verify_route_action_candidates(
        candidate_dir, plan_dir=plan_dir, quote_dir=quote_dir,
    )
    bindings = candidate_run_bindings(
        candidate_dir, plan_dir=plan_dir, quote_dir=quote_dir,
        admitted_trials=admitted_trials,
    )
    rows = [
        {"executor_node_id": node, "run_id": run_id,
         "trial_key": trial_key, "cache_episode_id": episode_id}
        for node, values in bindings.items()
        for (run_id, trial_key), episode_id in values.items()
    ]
    rows.sort(key=lambda row: (
        row["executor_node_id"], row["run_id"], row["trial_key"]
    ))
    _require(len(rows) == candidate["candidate_count"],
             "runtime candidate binding coverage differs")
    rows_bytes = b"".join(_canonical(row) + b"\n" for row in rows)
    manifest = {
        "schema_version": SCHEMA,
        "status": "FROZEN_ROUTE_ACTION_RUNTIME_ADMISSION",
        "semantic_admission_sha256": admission_sha256,
        "candidate_package_sha256": candidate["package_sha256"],
        "run_count": len(rows),
        "n7_run_count": len(bindings["N7"]),
        "n8_run_count": len(bindings["N8"]),
        "rows_sha256": sha256(rows_bytes).hexdigest(),
        "outcomes_accessed": False,
        "credentials_recorded": False,
    }
    manifest["package_sha256"] = sha256(_canonical(manifest)).hexdigest()
    return _canonical(manifest) + b"\n", rows_bytes


def _checksums(manifest: bytes, rows: bytes) -> bytes:
    return b"".join(
        sha256(value).hexdigest().encode("ascii") + b"  " + name + b"\n"
        for name, value in sorted(((MANIFEST.encode(), manifest),
                                   (ROWS.encode(), rows)))
    )


def freeze_route_action_runtime_admission(
    *, output_dir: str | Path, candidate_dir: str | Path,
    plan_dir: str | Path, quote_dir: str | Path,
    admitted_trials: Sequence[dict[str, Any]], admission_sha256: str,
) -> dict[str, Any]:
    root = Path(output_dir).resolve()
    _require(not root.exists(), "runtime admission output already exists")
    for source in (candidate_dir, plan_dir, quote_dir):
        frozen = Path(source).resolve()
        _require(root != frozen and frozen not in root.parents,
                 "runtime admission output must not write into a source")
    manifest, rows = _expected(
        candidate_dir=Path(candidate_dir), plan_dir=Path(plan_dir),
        quote_dir=Path(quote_dir), admitted_trials=admitted_trials,
        admission_sha256=admission_sha256,
    )
    root.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".route-runtime-", dir=root.parent))
    try:
        (stage / MANIFEST).write_bytes(manifest)
        (stage / ROWS).write_bytes(rows)
        (stage / CHECKSUMS).write_bytes(_checksums(manifest, rows))
        verify_route_action_runtime_admission(
            stage, candidate_dir=candidate_dir, plan_dir=plan_dir,
            quote_dir=quote_dir, admitted_trials=admitted_trials,
            admission_sha256=admission_sha256,
        )
        os.replace(stage, root)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return verify_route_action_runtime_admission(
        root, candidate_dir=candidate_dir, plan_dir=plan_dir,
        quote_dir=quote_dir, admitted_trials=admitted_trials,
        admission_sha256=admission_sha256,
    )


def verify_route_action_runtime_admission(
    root: str | Path, *, candidate_dir: str | Path,
    plan_dir: str | Path, quote_dir: str | Path,
    admitted_trials: Sequence[dict[str, Any]], admission_sha256: str,
) -> dict[str, Any]:
    package = Path(root)
    _require(package.is_dir() and {p.name for p in package.iterdir()}
             == {MANIFEST, ROWS, CHECKSUMS},
             "runtime admission file set differs")
    manifest, rows = _expected(
        candidate_dir=Path(candidate_dir), plan_dir=Path(plan_dir),
        quote_dir=Path(quote_dir), admitted_trials=admitted_trials,
        admission_sha256=admission_sha256,
    )
    _require((package / MANIFEST).read_bytes() == manifest
             and (package / ROWS).read_bytes() == rows
             and (package / CHECKSUMS).read_bytes()
             == _checksums(manifest, rows),
             "runtime admission differs from verified candidates and trials")
    document = json.loads(manifest)
    bindings: dict[str, dict[tuple[str, str], str | None]] = {
        "N7": {}, "N8": {},
    }
    for line in rows.splitlines():
        row = json.loads(line)
        bindings[row["executor_node_id"]][
            (row["run_id"], row["trial_key"])
        ] = row["cache_episode_id"]
    return {
        "status": "VERIFIED_ROUTE_ACTION_RUNTIME_ADMISSION",
        "package_sha256": document["package_sha256"],
        "run_count": document["run_count"],
        "bindings": bindings,
        "credentials_recorded": False,
    }
