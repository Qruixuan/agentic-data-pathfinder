"""Freeze source-verified artifact identities for the semantic 4x8 matrix.

This module is deliberately an offline bridge.  It joins the public half of
the N1 task plane to immutable artifacts already packaged for the N3 and N4
Data Agents.  It copies no artifact bytes, endpoint, credential, or hidden
label.  The resulting binding set is accepted directly by
``compile_full_flow_semantic_matrix``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

from .full_flow_logical_routes import (
    PLAN_NAME as LOGICAL_PLAN_NAME,
    STAGES_NAME as LOGICAL_STAGES_NAME,
    TRIALS_NAME as LOGICAL_TRIALS_NAME,
    verify_full_flow_logical_routes,
)
from .full_flow_semantic_matrix import ARTIFACT_BINDING_SET_SCHEMA_VERSION
from .full_flow_tasks import (
    PUBLIC_TASK_SET,
    verify_full_flow_task_plane,
)
from .n4_derived_data_plane import (
    CHECKSUMS_NAME as N4_CHECKSUMS_NAME,
    PACKAGE_MANIFEST_NAME as N4_MANIFEST_NAME,
    verify_n4_derived_data_package,
)
from .raw_cold_data_plane import (
    CHECKSUMS_NAME as N3_CHECKSUMS_NAME,
    PACKAGE_MANIFEST_NAME as N3_MANIFEST_NAME,
    verify_raw_cold_data_plane_package,
)


ARTIFACT_BINDINGS_NAME = "artifact-bindings.json"
PROVENANCE_NAME = "artifact-binding-provenance.json"
CHECKSUMS_NAME = "SHA256SUMS"
PROVENANCE_SCHEMA_VERSION = (
    "pathfinder.full-flow-artifact-binding-provenance/v1alpha1"
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class FullFlowArtifactBindingError(ValueError):
    """Raised when real artifacts cannot be bound without ambiguity."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowArtifactBindingError(message)


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return str(value)


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _strict_json_bytes(raw: bytes, label: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{label} repeats key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowArtifactBindingError(
                    f"{label} contains non-finite value {token}"
                )
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowArtifactBindingError(f"{label} is not valid JSON") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_json(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise FullFlowArtifactBindingError(f"cannot read {label}") from exc
    return raw, _strict_json_bytes(raw, label)


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_bytes().splitlines()
    except OSError as exc:
        raise FullFlowArtifactBindingError(f"cannot read {label}") from exc
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(lines, start=1):
        if not line:
            continue
        rows.append(_strict_json_bytes(line, f"{label} line {index}"))
    _require(bool(rows), f"{label} is empty")
    return rows


def _source_digest(path: Path, label: str) -> str:
    try:
        return _sha256(path.read_bytes())
    except OSError as exc:
        raise FullFlowArtifactBindingError(f"cannot hash {label}") from exc


def _artifact_rows(
    n3_report: Mapping[str, Any],
    n4_report: Mapping[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    sources = (
        (n3_report, n3_report.get("catalog_version"), "N3"),
        (n4_report, n4_report.get("catalog_version"), "N4"),
    )
    for report, catalog_value, source_node in sources:
        catalog = _identifier(catalog_value, f"{source_node} catalog_version")
        values = report.get("objects")
        _require(isinstance(values, list), f"{source_node} objects are invalid")
        for position, value in enumerate(values):
            _require(
                isinstance(value, Mapping),
                f"{source_node} object {position} is invalid",
            )
            object_id = _identifier(value.get("object_id"), "artifact object_id")
            representation_id = _identifier(
                value.get("representation_id"),
                "representation_id",
            )
            digest = value.get("artifact_sha256")
            size = value.get("artifact_size_bytes")
            _require(
                isinstance(digest, str) and _SHA256.fullmatch(digest) is not None,
                f"artifact digest is invalid: {object_id}/{representation_id}",
            )
            _require(
                type(size) is int and size > 0,
                f"artifact size is invalid: {object_id}/{representation_id}",
            )
            key = (object_id, representation_id)
            _require(key not in rows, f"artifact identity is ambiguous: {key}")
            plan_ids = value.get("plan_ids")
            _require(
                isinstance(plan_ids, list)
                and bool(plan_ids)
                and plan_ids == sorted(set(plan_ids))
                and all(isinstance(plan_id, str) for plan_id in plan_ids),
                f"artifact plan bindings are invalid: "
                f"{object_id}/{representation_id}",
            )
            rows[key] = {
                "representation_id": representation_id,
                "artifact_sha256": digest,
                "artifact_size_bytes": size,
                "object_catalog_version": catalog,
                # Plan IDs are verified source metadata used only to prove
                # route coverage.  They are intentionally not emitted into
                # the semantic compiler's public identity-only schema.
                "plan_ids": list(plan_ids),
            }
    return rows


def _documents(
    logical_route_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
    task_plane_dir: Path,
    n3_package_dir: Path,
    n4_package_dir: Path,
    binding_set_id: str,
) -> dict[str, bytes]:
    binding_set_id = _identifier(binding_set_id, "binding_set_id")
    try:
        logical_verified = verify_full_flow_logical_routes(
            logical_route_dir,
            scenario_path,
            container_plan_dir,
        )
        task_verified = verify_full_flow_task_plane(task_plane_dir)
        n3_verified = verify_raw_cold_data_plane_package(n3_package_dir)
        n4_verified = verify_n4_derived_data_package(n4_package_dir)
    except (OSError, ValueError, RuntimeError) as exc:
        raise FullFlowArtifactBindingError(
            "an artifact-binding source package failed verification"
        ) from exc

    logical_trials = _read_jsonl(
        logical_route_dir / LOGICAL_TRIALS_NAME,
        "logical route trials",
    )
    logical_stages = _read_jsonl(
        logical_route_dir / LOGICAL_STAGES_NAME,
        "logical route stages",
    )
    _, public = _read_json(
        task_plane_dir / PUBLIC_TASK_SET,
        "public task set",
    )
    _, n3_report = _read_json(
        n3_package_dir / N3_MANIFEST_NAME,
        "N3 package manifest",
    )
    _, n4_report = _read_json(
        n4_package_dir / N4_MANIFEST_NAME,
        "N4 package manifest",
    )

    expected: dict[str, dict[str, Any]] = {}
    required_representations: dict[str, set[str]] = defaultdict(set)
    required_plan_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    for trial in logical_trials:
        workload_id = _identifier(trial.get("workload_id"), "workload_id")
        design_id = _identifier(trial.get("design_id"), "design_id")
        logical_object_id = _identifier(
            trial.get("object_id"),
            "logical object_id",
        )
        current = expected.setdefault(
            workload_id,
            {"logical_object_id": logical_object_id},
        )
        _require(
            current["logical_object_id"] == logical_object_id,
            f"workload maps to multiple logical objects: {workload_id}",
        )
        representations = trial.get("representation_ids")
        _require(isinstance(representations, list), "representation_ids is invalid")
        for representation_id in representations:
            checked_representation = _identifier(
                representation_id,
                "representation_id",
            )
            required_representations[logical_object_id].add(
                checked_representation
            )
            required_plan_ids[
                (logical_object_id, checked_representation)
            ].add(design_id)
        # Every route is rooted in the authoritative N3 raw object.  Derived
        # routes need it for their N3 -> N5 -> N4 provisioning chain even
        # though raw_video is not a trial-facing representation.
        required_plan_ids[(logical_object_id, "raw_video")].add(design_id)
    for stage in logical_stages:
        logical_object_id = _identifier(
            stage.get("object_id"),
            "logical stage object_id",
        )
        representation_id = stage.get("representation_id")
        if representation_id is not None:
            required_representations[logical_object_id].add(
                _identifier(representation_id, "representation_id")
            )
    _require(len(expected) == 4, "logical route package must contain four workloads")
    _require(
        len({item["logical_object_id"] for item in expected.values()}) == 4,
        "logical workloads must map one-to-one to four logical objects",
    )

    tasks = public.get("tasks")
    _require(isinstance(tasks, list), "public tasks are invalid")
    public_by_workload: dict[str, dict[str, Any]] = {}
    for task in tasks:
        _require(isinstance(task, dict), "public task must be an object")
        workload_id = _identifier(task.get("workload_id"), "public workload_id")
        object_id = _identifier(task.get("object_id"), "artifact object_id")
        _require(
            workload_id in expected,
            f"public task has no logical workload: {workload_id}",
        )
        _require(
            workload_id not in public_by_workload,
            f"public task workload is duplicated: {workload_id}",
        )
        public_by_workload[workload_id] = task
        expected[workload_id]["artifact_object_id"] = object_id
    _require(
        set(public_by_workload) == set(expected),
        "public task plane does not exactly cover the four logical workloads",
    )

    artifacts = _artifact_rows(n3_report, n4_report)
    objects: list[dict[str, Any]] = []
    artifact_object_ids: set[str] = set()
    for workload_id in sorted(expected):
        item = expected[workload_id]
        logical_object_id = item["logical_object_id"]
        artifact_object_id = item["artifact_object_id"]
        _require(
            artifact_object_id not in artifact_object_ids,
            f"public workloads share artifact object: {artifact_object_id}",
        )
        representations: list[dict[str, Any]] = []
        for representation_id in sorted(
            required_representations[logical_object_id]
        ):
            key = (artifact_object_id, representation_id)
            _require(
                key in artifacts,
                "verified Data Agent packages are missing required artifact "
                f"{artifact_object_id}/{representation_id}",
            )
            artifact = artifacts[key]
            required_plans = required_plan_ids[
                (logical_object_id, representation_id)
            ]
            _require(
                required_plans <= set(artifact["plan_ids"]),
                "verified Data Agent package does not expose required artifact "
                f"{artifact_object_id}/{representation_id} for designs "
                f"{sorted(required_plans - set(artifact['plan_ids']))}",
            )
            representations.append({
                "representation_id": artifact["representation_id"],
                "artifact_sha256": artifact["artifact_sha256"],
                "artifact_size_bytes": artifact["artifact_size_bytes"],
                "object_catalog_version": artifact[
                    "object_catalog_version"
                ],
            })
        _require(
            bool(representations),
            f"logical object has no data: {logical_object_id}",
        )
        objects.append({
            "logical_object_id": logical_object_id,
            "artifact_object_id": artifact_object_id,
            "representations": representations,
        })
        artifact_object_ids.add(artifact_object_id)
    objects.sort(key=lambda item: item["logical_object_id"])
    bindings = {
        "schema_version": ARTIFACT_BINDING_SET_SCHEMA_VERSION,
        "binding_set_id": binding_set_id,
        "objects": objects,
        "credentials_recorded": False,
    }
    bindings_bytes = _json_bytes(bindings)

    logical_plan_raw, logical_plan = _read_json(
        logical_route_dir / LOGICAL_PLAN_NAME,
        "logical route plan",
    )
    provenance = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "status": "FROZEN_VERIFIED_ARTIFACT_BINDINGS",
        "binding_set_id": binding_set_id,
        "artifact_binding_set_sha256": _sha256(bindings_bytes),
        "logical_plan_sha256": logical_plan["plan_sha256"],
        "logical_plan_file_sha256": _sha256(logical_plan_raw),
        # Bind only the public half of the task plane.  Hashes of the private
        # manifest/checksum tree are intentionally excluded because hidden
        # multiple-choice labels have low entropy and a public deterministic
        # digest would unnecessarily widen their disclosure surface.
        "task_plane_id": task_verified["task_plane_id"],
        "public_task_set_sha256": _source_digest(
            task_plane_dir / PUBLIC_TASK_SET,
            "public task set",
        ),
        "n3_package_manifest_sha256": _source_digest(
            n3_package_dir / N3_MANIFEST_NAME,
            "N3 package manifest",
        ),
        "n3_package_checksums_sha256": _source_digest(
            n3_package_dir / N3_CHECKSUMS_NAME,
            "N3 package checksums",
        ),
        "n4_package_manifest_sha256": _source_digest(
            n4_package_dir / N4_MANIFEST_NAME,
            "N4 package manifest",
        ),
        "n4_package_checksums_sha256": _source_digest(
            n4_package_dir / N4_CHECKSUMS_NAME,
            "N4 package checksums",
        ),
        "logical_trial_count": logical_verified["trial_count"],
        "public_task_count": task_verified["public_task_count"],
        "artifact_object_count": len(objects),
        "artifact_representation_count": sum(
            len(item["representations"]) for item in objects
        ),
        "n3_source_node_id": n3_verified["source_node_id"],
        "n4_source_node_id": n4_verified["logical_node_id"],
        "data_agent_plan_binding_coverage_verified": True,
        "artifacts_copied": False,
        "endpoint_binding_included": False,
        "private_task_plane_binding_included": False,
        "hidden_label_values_included": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    return {
        ARTIFACT_BINDINGS_NAME: bindings_bytes,
        PROVENANCE_NAME: _json_bytes(provenance),
    }


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def build_full_flow_artifact_bindings(
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    task_plane_dir: str | Path,
    n3_package_dir: str | Path,
    n4_package_dir: str | Path,
    *,
    binding_set_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Build a public, endpoint-free binding package from verified sources."""

    sources = tuple(
        Path(path).resolve()
        for path in (
            logical_route_dir,
            scenario_path,
            container_plan_dir,
            task_plane_dir,
            n3_package_dir,
            n4_package_dir,
        )
    )
    target = Path(output_dir).resolve()
    for source in sources:
        if source.is_dir():
            _require(
                target != source and not target.is_relative_to(source),
                "output directory must not be inside a verified source "
                f"package: {source}",
            )
    documents = _documents(*sources, binding_set_id)
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, raw in documents.items():
            (stage / name).write_bytes(raw)
        (stage / CHECKSUMS_NAME).write_bytes(_checksums(documents))
        verify_full_flow_artifact_bindings(
            stage,
            *sources,
        )
        os.replace(stage, target)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    report = _strict_json_bytes(documents[PROVENANCE_NAME], PROVENANCE_NAME)
    return {**report, "output_dir": str(target)}


def verify_full_flow_artifact_bindings(
    output_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    task_plane_dir: str | Path,
    n3_package_dir: str | Path,
    n4_package_dir: str | Path,
) -> dict[str, Any]:
    """Verify checksums and deterministic recompilation from all six sources."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"artifact-binding package does not exist: {root}")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "artifact-binding package must contain regular files only",
    )
    actual_names = sorted(path.name for path in entries)
    _require(
        actual_names
        == sorted((ARTIFACT_BINDINGS_NAME, PROVENANCE_NAME, CHECKSUMS_NAME)),
        "artifact-binding package file set changed",
    )
    _, bindings = _read_json(root / ARTIFACT_BINDINGS_NAME, ARTIFACT_BINDINGS_NAME)
    binding_set_id = _identifier(bindings.get("binding_set_id"), "binding_set_id")
    expected = _documents(
        Path(logical_route_dir).resolve(),
        Path(scenario_path).resolve(),
        Path(container_plan_dir).resolve(),
        Path(task_plane_dir).resolve(),
        Path(n3_package_dir).resolve(),
        Path(n4_package_dir).resolve(),
        binding_set_id,
    )
    for name, raw in expected.items():
        _require((root / name).read_bytes() == raw, f"source binding changed: {name}")
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(expected),
        "artifact-binding checksums changed",
    )
    provenance = _strict_json_bytes(expected[PROVENANCE_NAME], PROVENANCE_NAME)
    return {
        "status": "VERIFIED",
        "binding_set_id": binding_set_id,
        "artifact_object_count": provenance["artifact_object_count"],
        "artifact_representation_count": provenance[
            "artifact_representation_count"
        ],
        "source_binding_checked": True,
        "artifacts_copied": False,
        "hidden_label_values_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "ARTIFACT_BINDINGS_NAME",
    "CHECKSUMS_NAME",
    "PROVENANCE_NAME",
    "PROVENANCE_SCHEMA_VERSION",
    "FullFlowArtifactBindingError",
    "build_full_flow_artifact_bindings",
    "verify_full_flow_artifact_bindings",
]
