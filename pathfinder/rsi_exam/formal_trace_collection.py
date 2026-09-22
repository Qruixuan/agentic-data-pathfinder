"""Resumable serial execution of a frozen RSI-Exam trace collection.

Each collection unit is one public case and one repetition.  Its ten physical
paths execute in their frozen dependency order.  Different units use distinct
run IDs, which the runtime maps to distinct persistent-cache namespaces.
Completed unit directories are immutable and re-verified before a resume.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .collection_plan import (
    CASES_NAME,
    CHECKSUMS_NAME as PLAN_CHECKSUMS_NAME,
    MANIFEST_NAME as PLAN_MANIFEST_NAME,
    verify_collection_plan,
)
from .offline_replay import (
    _checksum_bytes,
    _compact_json_bytes,
    _json_bytes,
    _load_json_bytes,
    _require,
    _sha256,
    _verify_checksum_directory,
)
from ..simulator.full_flow_one_case import (
    PLAN_NAME as ONE_CASE_PLAN_NAME,
    verify_full_flow_one_case_plan,
)
from ..simulator.full_flow_route_adapters import semantic_cache_namespace


COLLECTION_EXECUTION_SCHEMA_VERSION = (
    "pathfinder.rsi-exam-formal-trace-collection/v1alpha1"
)
COLLECTION_PROGRESS_SCHEMA_VERSION = (
    "pathfinder.rsi-exam-formal-trace-collection-progress/v1alpha1"
)
RECEIPT_NAME = "formal-trace-collection-receipt.json"
PROGRESS_NAME = "formal-trace-collection-progress.json"
CHECKSUMS_NAME = "SHA256SUMS"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class FormalTraceCollectionUnit:
    """One serially executed case/repetition unit."""

    ordinal: int
    case_id: str
    object_id: str
    workload_id: str
    split: str
    stratum: str
    repetition: int
    run_id: str
    cache_namespace: str
    one_case_plan_dir: Path
    one_case_plan_sha256: str

    @property
    def directory_name(self) -> str:
        return f"unit-{self.ordinal:04d}-{self.case_id}-r{self.repetition:04d}"


UnitExecutor = Callable[[FormalTraceCollectionUnit, Path], Mapping[str, Any]]
UnitVerifier = Callable[[FormalTraceCollectionUnit, Path], Mapping[str, Any]]


def _strict_json(path: Path, label: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    value = _load_json_bytes(path.read_bytes(), label)
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _strict_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    raw = path.read_bytes()
    _require(b"\r" not in raw and raw.endswith(b"\n"), f"{label} is not canonical")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(raw.splitlines(), start=1):
        value = _load_json_bytes(line, f"{label} line {index}")
        _require(isinstance(value, dict), f"{label} row is not an object")
        rows.append(value)
    return rows


def _collection_plan_sha256(root: Path) -> str:
    return _sha256((root / PLAN_CHECKSUMS_NAME).read_bytes())


def _unit_run_id(
    collection_id: str,
    *,
    case_id: str,
    repetition: int,
) -> str:
    suffix = hashlib.sha256(
        _compact_json_bytes({
            "domain": "pathfinder.rsi-exam-formal-unit/v1",
            "collection_id": collection_id,
            "case_id": case_id,
            "repetition": repetition,
        })
    ).hexdigest()[:16]
    value = f"{collection_id}-{case_id}-r{repetition:04d}-{suffix}"
    _require(_IDENTIFIER.fullmatch(value) is not None, "formal unit run_id is invalid")
    return value


def load_formal_trace_collection_units(
    collection_plan_dir: str | Path,
    one_case_plan_dirs: Sequence[str | Path],
    *,
    local_semantic_admission_dir: str | Path,
    collection_id: str,
) -> tuple[dict[str, Any], tuple[FormalTraceCollectionUnit, ...]]:
    """Verify inputs and derive the exact serial unit schedule."""

    _require(
        isinstance(collection_id, str)
        and _IDENTIFIER.fullmatch(collection_id) is not None,
        "collection_id is invalid",
    )
    plan_root = Path(collection_plan_dir).resolve()
    plan_report = verify_collection_plan(plan_root)
    manifest = _strict_json(plan_root / PLAN_MANIFEST_NAME, "collection manifest")
    cases = _strict_jsonl(plan_root / CASES_NAME, "selected cases")
    admission_root = Path(local_semantic_admission_dir).resolve()

    plans_by_case: dict[str, tuple[Path, dict[str, Any]]] = {}
    for value in one_case_plan_dirs:
        root = Path(value).resolve()
        report = verify_full_flow_one_case_plan(
            root,
            local_semantic_admission_dir=admission_root,
        )
        plan = _strict_json(root / ONE_CASE_PLAN_NAME, "one-case plan")
        case_id = str(report["artifact_object_id"])
        _require(case_id not in plans_by_case, "one-case plan repeats an object")
        plans_by_case[case_id] = (root, plan)

    expected_objects = {str(case["object_id"]) for case in cases}
    _require(
        set(plans_by_case) == expected_objects,
        "one-case plans do not exactly cover the frozen collection",
    )
    repetitions = int(manifest["collection_repetitions"])
    units: list[FormalTraceCollectionUnit] = []
    for case in cases:
        object_id = str(case["object_id"])
        plan_root_for_case, one_case = plans_by_case[object_id]
        _require(
            one_case.get("workload_id") == case.get("workload_id")
            and one_case.get("artifact_object_id") == object_id,
            "one-case plan differs from its frozen collection case",
        )
        for repetition in range(repetitions):
            run_id = _unit_run_id(
                collection_id,
                case_id=str(case["case_id"]),
                repetition=repetition,
            )
            units.append(FormalTraceCollectionUnit(
                ordinal=len(units),
                case_id=str(case["case_id"]),
                object_id=object_id,
                workload_id=str(case["workload_id"]),
                split=str(case["split"]),
                stratum=str(case["stratum"]),
                repetition=repetition,
                run_id=run_id,
                cache_namespace=semantic_cache_namespace(run_id),
                one_case_plan_dir=plan_root_for_case,
                one_case_plan_sha256=str(one_case["plan_sha256"]),
            ))
    _require(
        len(units) * 10 == int(manifest["operation_count"]),
        "formal serial schedule does not cover every frozen operation",
    )
    return plan_report, tuple(units)


def _unit_record(
    unit: FormalTraceCollectionUnit,
    report: Mapping[str, Any],
) -> dict[str, Any]:
    receipt_sha256 = report.get("receipt_sha256")
    _require(
        report.get("status") == "VERIFIED"
        and report.get("run_id") == unit.run_id
        and report.get("smoke_count") == 10
        and report.get("one_case_execution_complete") is True
        and report.get("one_case_artifact_object_id") == unit.object_id,
        "formal collection unit verification report differs",
    )
    _require(
        isinstance(receipt_sha256, str)
        and _SHA256.fullmatch(receipt_sha256) is not None,
        "formal collection unit receipt digest is invalid",
    )
    return {
        "ordinal": unit.ordinal,
        "case_id": unit.case_id,
        "object_id": unit.object_id,
        "workload_id": unit.workload_id,
        "split": unit.split,
        "stratum": unit.stratum,
        "repetition": unit.repetition,
        "run_id": unit.run_id,
        "cache_namespace": unit.cache_namespace,
        "one_case_plan_sha256": unit.one_case_plan_sha256,
        "evidence_directory": unit.directory_name,
        "evidence_receipt_sha256": receipt_sha256,
        "route_count": 10,
        "status": "VERIFIED",
    }


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _progress_document(
    *,
    collection_id: str,
    plan_sha256: str,
    units: Sequence[FormalTraceCollectionUnit],
    completed: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": COLLECTION_PROGRESS_SCHEMA_VERSION,
        "collection_id": collection_id,
        "collection_plan_sha256": plan_sha256,
        "unit_count": len(units),
        "route_operation_count": len(units) * 10,
        "completed_unit_count": len(completed),
        "completed_route_operation_count": len(completed) * 10,
        "next_unit_ordinal": len(completed),
        "execution_order": "serial-case-then-repetition",
        "cache_isolation": "run-scoped-persistent-cache-namespace",
        "workflow_submitted": bool(completed),
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "eligible_for_scientific_claims": False,
    }


def run_formal_trace_collection(
    collection_plan_dir: str | Path,
    one_case_plan_dirs: Sequence[str | Path],
    *,
    local_semantic_admission_dir: str | Path,
    collection_id: str,
    output_dir: str | Path,
    execute_unit: UnitExecutor,
    verify_unit: UnitVerifier,
) -> dict[str, Any]:
    """Run or resume all case/repetition units strictly serially."""

    plan_report, units = load_formal_trace_collection_units(
        collection_plan_dir,
        one_case_plan_dirs,
        local_semantic_admission_dir=local_semantic_admission_dir,
        collection_id=collection_id,
    )
    target = Path(output_dir).resolve()
    target.mkdir(parents=True, exist_ok=True)
    _require(not (target / RECEIPT_NAME).exists(), "collection is already complete")
    plan_sha256 = _collection_plan_sha256(Path(collection_plan_dir).resolve())
    completed: list[dict[str, Any]] = []
    for unit in units:
        evidence = target / unit.directory_name
        if evidence.exists():
            report = verify_unit(unit, evidence)
        else:
            report = execute_unit(unit, evidence)
            report = verify_unit(unit, evidence)
        completed.append(_unit_record(unit, report))
        _write_atomic(
            target / PROGRESS_NAME,
            _json_bytes(_progress_document(
                collection_id=collection_id,
                plan_sha256=plan_sha256,
                units=units,
                completed=completed,
            )),
        )

    receipt = {
        "schema_version": COLLECTION_EXECUTION_SCHEMA_VERSION,
        "status": "VERIFIED",
        "collection_id": collection_id,
        "cohort_id": plan_report["cohort_id"],
        "collection_plan_sha256": plan_sha256,
        "unit_count": len(units),
        "route_operation_count": len(units) * 10,
        "execution_order": "serial-case-then-repetition",
        "cache_isolation": "run-scoped-persistent-cache-namespace",
        "all_units_verified": True,
        "units": completed,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "eligible_for_scientific_claims": False,
    }
    receipt["receipt_sha256"] = _sha256(_compact_json_bytes(receipt))
    _write_atomic(target / RECEIPT_NAME, _json_bytes(receipt))
    files = {
        RECEIPT_NAME: (target / RECEIPT_NAME).read_bytes(),
        PROGRESS_NAME: (target / PROGRESS_NAME).read_bytes(),
    }
    _write_atomic(target / CHECKSUMS_NAME, _checksum_bytes(files))
    return verify_formal_trace_collection(
        target,
        collection_plan_dir=collection_plan_dir,
        one_case_plan_dirs=one_case_plan_dirs,
        local_semantic_admission_dir=local_semantic_admission_dir,
        collection_id=collection_id,
        verify_unit=verify_unit,
    )


def verify_formal_trace_collection(
    collection_dir: str | Path,
    *,
    collection_plan_dir: str | Path,
    one_case_plan_dirs: Sequence[str | Path],
    local_semantic_admission_dir: str | Path,
    collection_id: str,
    verify_unit: UnitVerifier,
) -> dict[str, Any]:
    """Verify the root receipt and every immutable ten-path unit."""

    root = Path(collection_dir).resolve()
    entries = _verify_checksum_directory(root)
    _require(
        set(entries) == {RECEIPT_NAME, PROGRESS_NAME},
        "formal collection root checksum file set differs",
    )
    plan_report, units = load_formal_trace_collection_units(
        collection_plan_dir,
        one_case_plan_dirs,
        local_semantic_admission_dir=local_semantic_admission_dir,
        collection_id=collection_id,
    )
    receipt = _strict_json(root / RECEIPT_NAME, "collection receipt")
    progress = _strict_json(root / PROGRESS_NAME, "collection progress")
    records = []
    for unit in units:
        report = verify_unit(unit, root / unit.directory_name)
        records.append(_unit_record(unit, report))
    expected = {
        "schema_version": COLLECTION_EXECUTION_SCHEMA_VERSION,
        "status": "VERIFIED",
        "collection_id": collection_id,
        "cohort_id": plan_report["cohort_id"],
        "collection_plan_sha256": _collection_plan_sha256(
            Path(collection_plan_dir).resolve()
        ),
        "unit_count": len(units),
        "route_operation_count": len(units) * 10,
        "execution_order": "serial-case-then-repetition",
        "cache_isolation": "run-scoped-persistent-cache-namespace",
        "all_units_verified": True,
        "units": records,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "eligible_for_scientific_claims": False,
    }
    expected["receipt_sha256"] = _sha256(_compact_json_bytes(expected))
    _require(receipt == expected, "formal collection receipt is not reproducible")
    _require(
        progress
        == _progress_document(
            collection_id=collection_id,
            plan_sha256=expected["collection_plan_sha256"],
            units=units,
            completed=records,
        ),
        "formal collection progress is not complete or reproducible",
    )
    return {
        "status": "VERIFIED",
        "collection_id": collection_id,
        "cohort_id": receipt["cohort_id"],
        "unit_count": len(units),
        "route_operation_count": len(units) * 10,
        "receipt_sha256": receipt["receipt_sha256"],
        "cache_namespace_count": len({unit.cache_namespace for unit in units}),
        "all_units_verified": True,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "FormalTraceCollectionUnit",
    "load_formal_trace_collection_units",
    "run_formal_trace_collection",
    "verify_formal_trace_collection",
]
