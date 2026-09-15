"""Freeze visible, source-bound query plans for local semantic execution.

The semantic matrix already freezes the public question and the exact object
assigned to each trial.  Indexed routes still need an explicit request for
the N2/N7/N8 lexical service.  This module turns only those public values into
one deterministic catalog.  It deliberately restricts each legacy MCQ trial
to its already-public object identity; this proves route interoperability but
is not a retrieval-quality experiment and is not used by the separate W4
retrieval contract.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .full_flow_local_semantic_admission import (
    LOCAL_SEMANTICS_MODE,
    FrozenLocalSemanticExecutionInputs,
    load_full_flow_local_semantic_execution_inputs,
)
from .full_flow_route_adapters import FrozenIndexQueryPlan
from .index_service import verify_n2_index_package


INDEX_QUERY_PLAN_CATALOG_SCHEMA_VERSION = (
    "pathfinder.full-flow-index-query-plan-catalog/v1alpha1"
)
INDEX_QUERY_PLAN_CATALOG_NAME = "full-flow-index-query-plan-catalog.json"
CHECKSUMS_NAME = "SHA256SUMS"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_FILES = {INDEX_QUERY_PLAN_CATALOG_NAME, CHECKSUMS_NAME}


class FullFlowIndexQueryPlanCatalogError(ValueError):
    """Raised when visible query inputs are incomplete or have drifted."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowIndexQueryPlanCatalogError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowIndexQueryPlanCatalogError(
            "index-query-plan value is not canonical JSON"
        ) from exc


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _strict_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, child in pairs:
            _require(key not in value, f"{name} repeats key {key}")
            value[key] = child
        return value

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowIndexQueryPlanCatalogError(
                    f"{name} contains invalid number {token}"
                )
            ),
        )
    except FullFlowIndexQueryPlanCatalogError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowIndexQueryPlanCatalogError(
            f"cannot read {name}"
        ) from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _indexed_trial_keys(
    inputs: FrozenLocalSemanticExecutionInputs,
) -> set[str]:
    result = {
        str(stage.get("trial_key"))
        for stage in inputs.bound_stages
        if stage.get("action") == "query-index"
    }
    _require(bool(result), "local semantic matrix has no indexed trials")
    return result


def _query_id(trial_key: str, task_binding_sha256: str) -> str:
    return "query-" + _sha256(
        _canonical({
            "domain": "pathfinder.visible-index-query-plan/v1",
            "trial_key": trial_key,
            "task_binding_sha256": task_binding_sha256,
        })
    )[:32]


def _entries(
    inputs: FrozenLocalSemanticExecutionInputs,
    *,
    index_id: str,
    candidate_object_ids: set[str],
) -> list[dict[str, Any]]:
    indexed = _indexed_trial_keys(inputs)
    rows: list[dict[str, Any]] = []
    for trial in inputs.bound_trials:
        trial_key = str(trial.get("trial_key"))
        if trial_key not in indexed:
            continue
        public = trial.get("public_task_binding")
        _require(isinstance(public, Mapping), "public task binding is missing")
        task_binding = _digest(
            public.get("task_binding_sha256"), "task_binding_sha256"
        )
        question = public.get("question")
        _require(
            isinstance(question, str) and bool(question.strip()),
            "public query text is empty",
        )
        object_id = _identifier(
            public.get("object_id"), "public task object_id"
        )
        _require(
            object_id == trial.get("artifact_object_id"),
            "public query object differs from the frozen semantic trial",
        )
        _require(
            object_id in candidate_object_ids,
            "public query object is absent from the frozen index",
        )
        plan = FrozenIndexQueryPlan(
            trial_key=trial_key,
            task_binding_sha256=task_binding,
            index_id=index_id,
            query_id=_query_id(trial_key, task_binding),
            query_text=question,
            top_k=1,
            candidate_object_ids=(object_id,),
        )
        rows.append({
            "trial_key": plan.trial_key,
            "task_binding_sha256": plan.task_binding_sha256,
            "index_id": plan.index_id,
            "query_id": plan.query_id,
            "query_text": plan.query_text,
            "top_k": plan.top_k,
            "candidate_object_ids": list(plan.candidate_object_ids or ()),
        })
    rows.sort(key=lambda row: row["trial_key"])
    _require(
        len(rows) == len(indexed)
        and [row["trial_key"] for row in rows] == sorted(indexed),
        "index-query-plan coverage differs from indexed trials",
    )
    return rows


def _expected_document(
    admission_dir: Path,
    n2_index_package_dir: Path,
) -> dict[str, Any]:
    inputs = load_full_flow_local_semantic_execution_inputs(admission_dir)
    index_report = verify_n2_index_package(n2_index_package_dir)
    index_artifact = _strict_json(
        n2_index_package_dir / "lexical-index.json",
        "N2 lexical index",
    )
    _require(
        index_artifact.get("index_id") == index_report.get("index_id")
        and _sha256(
            (n2_index_package_dir / "lexical-index.json").read_bytes()
        )
        == index_report.get("index_sha256"),
        "N2 verifier returned a different index identity",
    )
    candidates = index_artifact.get("candidate_object_ids")
    _require(
        isinstance(candidates, list)
        and candidates == sorted(set(candidates))
        and bool(candidates),
        "N2 candidate set is invalid",
    )
    admission = inputs.admission
    _require(
        admission.get("semantics_mode") == LOCAL_SEMANTICS_MODE,
        "query plans require the reviewed local semantics mode",
    )
    document: dict[str, Any] = {
        "schema_version": INDEX_QUERY_PLAN_CATALOG_SCHEMA_VERSION,
        "status": "FROZEN_INDEX_QUERY_PLANS",
        "catalog_id": "local-visible-index-query-plans-v1",
        "semantics_mode": LOCAL_SEMANTICS_MODE,
        "admission_sha256": _digest(
            admission.get("admission_sha256"), "admission_sha256"
        ),
        "index_id": _identifier(index_report.get("index_id"), "index_id"),
        "index_sha256": _digest(
            index_report.get("index_sha256"), "index_sha256"
        ),
        "public_task_set_sha256": _digest(
            admission.get("public_oracle_binding", {}).get(
                "public_task_set_sha256"
            ),
            "public_task_set_sha256",
        ),
        "query_policy": "single-public-target-local-conformance",
        "entries": _entries(
            inputs,
            index_id=str(index_report["index_id"]),
            candidate_object_ids=set(candidates),
        ),
        "w4_retrieval_quality_evaluated": False,
        "performance_measured": False,
        "cost_measured": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    document["catalog_sha256"] = _sha256(_canonical(document))
    return document


def build_full_flow_index_query_plan_catalog(
    local_semantic_admission_dir: str | Path,
    n2_index_package_dir: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze one visible query plan for every indexed semantic trial."""

    admission_root = Path(local_semantic_admission_dir).resolve()
    index_root = Path(n2_index_package_dir).resolve()
    document = _expected_document(admission_root, index_root)
    payload = _json_bytes(document)
    checksum = (
        f"{_sha256(payload)}  {INDEX_QUERY_PLAN_CATALOG_NAME}\n"
    ).encode("utf-8")
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".index-query-plans-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        (stage / INDEX_QUERY_PLAN_CATALOG_NAME).write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_bytes(checksum)
        verify_full_flow_index_query_plan_catalog(
            stage,
            local_semantic_admission_dir=admission_root,
            n2_index_package_dir=index_root,
        )
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return verify_full_flow_index_query_plan_catalog(
        target,
        local_semantic_admission_dir=admission_root,
        n2_index_package_dir=index_root,
    ) | {"output_dir": str(target)}


def verify_full_flow_index_query_plan_catalog(
    output_dir: str | Path,
    *,
    local_semantic_admission_dir: str | Path,
    n2_index_package_dir: str | Path,
) -> dict[str, Any]:
    """Verify the catalog and reproduce it from public frozen sources."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), "index-query-plan catalog directory is missing")
    _require(
        {path.name for path in root.iterdir()} == _FILES
        and all(path.is_file() and not path.is_symlink() for path in root.iterdir()),
        "index-query-plan catalog file set changed",
    )
    payload = (root / INDEX_QUERY_PLAN_CATALOG_NAME).read_bytes()
    _require(
        (root / CHECKSUMS_NAME).read_bytes()
        == f"{_sha256(payload)}  {INDEX_QUERY_PLAN_CATALOG_NAME}\n".encode(
            "utf-8"
        ),
        "index-query-plan checksums failed",
    )
    document = _strict_json(
        root / INDEX_QUERY_PLAN_CATALOG_NAME,
        "index-query-plan catalog",
    )
    supplied = _digest(document.get("catalog_sha256"), "catalog_sha256")
    core = dict(document)
    del core["catalog_sha256"]
    _require(
        supplied == _sha256(_canonical(core)),
        "index-query-plan catalog digest failed",
    )
    expected = _expected_document(
        Path(local_semantic_admission_dir).resolve(),
        Path(n2_index_package_dir).resolve(),
    )
    _require(
        payload == _json_bytes(expected),
        "index-query-plan catalog does not match its frozen public sources",
    )
    entries = document.get("entries")
    _require(isinstance(entries, list), "index-query-plan entries are missing")
    return {
        "status": "VERIFIED",
        "catalog_id": document["catalog_id"],
        "catalog_sha256": supplied,
        "indexed_trial_count": len(entries),
        "query_policy": document["query_policy"],
        "w4_retrieval_quality_evaluated": False,
        "source_binding_checked": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "FullFlowIndexQueryPlanCatalogError",
    "INDEX_QUERY_PLAN_CATALOG_NAME",
    "INDEX_QUERY_PLAN_CATALOG_SCHEMA_VERSION",
    "build_full_flow_index_query_plan_catalog",
    "verify_full_flow_index_query_plan_catalog",
]
