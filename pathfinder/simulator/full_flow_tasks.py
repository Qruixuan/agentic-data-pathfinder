"""Split public task inputs from N1-only hidden labels.

This is an offline packaging boundary.  It converts existing frozen semantic
specifications into a public task set safe for FlowMesh and a separate N1
oracle package.  The output deliberately contains no deployment address or
credential; operators mount only ``public/`` into the coordinator and only
``n1-private/`` into N1.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .data_agent_semantic_vertical import (
    load_data_agent_frame_bundle_semantic_spec,
)
from .hidden_oracle import (
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    assert_hidden_oracle_fields_absent,
    build_n1_hidden_label_record,
    build_n1_oracle_package,
    build_n1_public_task_binding,
    verify_n1_oracle_package,
)


FULL_FLOW_TASK_PLANE_SCHEMA_VERSION = (
    "pathfinder.full-flow-task-plane/v1alpha1"
)
PUBLIC_TASK_SET_SCHEMA_VERSION = "pathfinder.public-task-set/v1alpha1"
TASK_PLANE_MANIFEST = "task-plane-manifest.json"
PUBLIC_TASK_SET = "public/public-tasks.json"
HIDDEN_LABEL_SOURCE = "n1-private/hidden-label-source.json"
ORACLE_PACKAGE = "n1-private/oracle-package"
CHECKSUMS = "SHA256SUMS"

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class FullFlowTaskPlaneError(ValueError):
    """Raised when public/private task separation cannot be proven."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowTaskPlaneError(message)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
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


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _strict_json(path: Path, name: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{name} contains duplicate key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda item: (_ for _ in ()).throw(
                FullFlowTaskPlaneError(f"{name} contains {item}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowTaskPlaneError(f"cannot read {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _relative_files(root: Path) -> list[str]:
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name != CHECKSUMS
    )


def _checksums(root: Path) -> bytes:
    return b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in _relative_files(root)
    )


def _read_checksums(root: Path) -> dict[str, str]:
    try:
        lines = (root / CHECKSUMS).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise FullFlowTaskPlaneError("task-plane SHA256SUMS is missing") from exc
    entries: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(separator == "  ", "task-plane checksum line is malformed")
        _require(_SHA256.fullmatch(digest) is not None, "checksum is invalid")
        _require(name not in entries, "task-plane checksum repeats a path")
        _require(
            name and not name.startswith(("/", "\\")) and ".." not in Path(name).parts,
            "task-plane checksum path is unsafe",
        )
        entries[name] = digest
    _require(set(entries) == set(_relative_files(root)), "checksum file set changed")
    for name, digest in entries.items():
        _require(_sha256((root / name).read_bytes()) == digest, f"checksum failed: {name}")
    return entries


def build_full_flow_task_plane(
    semantic_spec_paths: Sequence[str | Path],
    *,
    task_plane_id: str,
    oracle_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze public tasks and N1-private labels into disjoint subtrees."""

    task_plane_id = _identifier(task_plane_id, "task_plane_id")
    oracle_id = _identifier(oracle_id, "oracle_id")
    _require(bool(semantic_spec_paths), "at least one semantic spec is required")
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"task-plane output already exists: {target}")

    tasks: dict[tuple[str, str], dict[str, Any]] = {}
    labels: dict[tuple[str, str], dict[str, Any]] = {}
    source_hashes: set[str] = set()
    for source in semantic_spec_paths:
        spec = load_data_agent_frame_bundle_semantic_spec(source)
        document = spec.document
        public = build_n1_public_task_binding(
            workload_id=document["workload_id"],
            object_id=document["artifact_object_id"],
            task_class_id=document["task_class_id"],
            question=document["question"],
            answer_options=document["answer_options"],
            success_scoring_rule=document["success_scoring_rule"],
        )
        key = (public["workload_id"], public["object_id"])
        previous = tasks.get(key)
        _require(
            previous is None or previous == public,
            f"semantic specs disagree for public task {key}",
        )
        label = build_n1_hidden_label_record(
            public,
            correct_answer_id=document["correct_answer_id"],
        )
        label_key = (label["object_id"], label["task_binding_sha256"])
        previous_label = labels.get(label_key)
        _require(
            previous_label is None or previous_label == label,
            f"semantic specs disagree for hidden label {label_key}",
        )
        tasks[key] = public
        labels[label_key] = label
        source_hashes.add(spec.source_sha256)

    public_rows = [tasks[key] for key in sorted(tasks)]
    label_rows = [labels[key] for key in sorted(labels)]
    public_document = {
        "schema_version": PUBLIC_TASK_SET_SCHEMA_VERSION,
        "task_plane_id": task_plane_id,
        "tasks": public_rows,
        "label_values_included": False,
        "credentials_recorded": False,
    }
    assert_hidden_oracle_fields_absent(public_document)
    public_bytes = _json_bytes(public_document)
    hidden_document = {
        "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
        "oracle_id": oracle_id,
        "logical_node_id": "N1",
        "labels": label_rows,
        "credentials_recorded": False,
    }
    hidden_bytes = _json_bytes(hidden_document)

    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".full-flow-tasks-", dir=target.parent))
    stage = parent / "package"
    try:
        (stage / "public").mkdir(parents=True)
        (stage / "n1-private").mkdir(parents=True)
        (stage / PUBLIC_TASK_SET).write_bytes(public_bytes)
        (stage / HIDDEN_LABEL_SOURCE).write_bytes(hidden_bytes)
        build_n1_oracle_package(
            stage / HIDDEN_LABEL_SOURCE,
            output_dir=stage / ORACLE_PACKAGE,
        )
        oracle_manifest_bytes = (
            stage / ORACLE_PACKAGE / "n1-oracle-package.json"
        ).read_bytes()
        manifest = {
            "schema_version": FULL_FLOW_TASK_PLANE_SCHEMA_VERSION,
            "status": "FROZEN_PUBLIC_PRIVATE_TASK_PLANE",
            "task_plane_id": task_plane_id,
            "oracle_id": oracle_id,
            "public_task_count": len(public_rows),
            "hidden_label_count": len(label_rows),
            "semantic_spec_source_sha256": sorted(source_hashes),
            "public_task_set_sha256": _sha256(public_bytes),
            "hidden_label_source_sha256": _sha256(hidden_bytes),
            "oracle_package_manifest_sha256": _sha256(oracle_manifest_bytes),
            "public_mount_subtree": "public",
            "n1_private_mount_subtree": "n1-private",
            "subtrees_must_not_be_co_mounted": True,
            "endpoint_binding_present": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        (stage / TASK_PLANE_MANIFEST).write_bytes(_json_bytes(manifest))
        (stage / CHECKSUMS).write_bytes(_checksums(stage))
        verify_full_flow_task_plane(stage)
        _require(not target.exists(), f"task-plane output already exists: {target}")
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return {
        "status": "FROZEN_PUBLIC_PRIVATE_TASK_PLANE",
        "task_plane_id": task_plane_id,
        "public_task_count": len(public_rows),
        "hidden_label_count": len(label_rows),
        "public_task_set_sha256": _sha256(public_bytes),
        "output_dir": str(target),
        "label_values_returned": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_full_flow_task_plane(output_dir: str | Path) -> dict[str, Any]:
    """Verify checksums, task hashes, separation, and the nested N1 package."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"task-plane package does not exist: {root}")
    entries = _read_checksums(root)
    manifest = _strict_json(root / TASK_PLANE_MANIFEST, "task-plane manifest")
    expected_manifest_fields = {
        "schema_version",
        "status",
        "task_plane_id",
        "oracle_id",
        "public_task_count",
        "hidden_label_count",
        "semantic_spec_source_sha256",
        "public_task_set_sha256",
        "hidden_label_source_sha256",
        "oracle_package_manifest_sha256",
        "public_mount_subtree",
        "n1_private_mount_subtree",
        "subtrees_must_not_be_co_mounted",
        "endpoint_binding_present",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    }
    _require(set(manifest) == expected_manifest_fields, "task-plane manifest fields changed")
    _require(
        manifest.get("schema_version") == FULL_FLOW_TASK_PLANE_SCHEMA_VERSION
        and manifest.get("status") == "FROZEN_PUBLIC_PRIVATE_TASK_PLANE",
        "task-plane schema or status changed",
    )
    _identifier(manifest.get("task_plane_id"), "task_plane_id")
    _identifier(manifest.get("oracle_id"), "oracle_id")
    _require(
        manifest.get("public_mount_subtree") == "public"
        and manifest.get("n1_private_mount_subtree") == "n1-private"
        and manifest.get("subtrees_must_not_be_co_mounted") is True,
        "public/private mount separation changed",
    )
    _require(
        manifest.get("endpoint_binding_present") is False
        and manifest.get("credentials_recorded") is False
        and manifest.get("eligible_for_scientific_claims") is False,
        "task-plane safety classification changed",
    )
    source_hashes = manifest.get("semantic_spec_source_sha256")
    _require(
        isinstance(source_hashes, list)
        and source_hashes == sorted(set(source_hashes))
        and all(isinstance(item, str) and _SHA256.fullmatch(item) for item in source_hashes),
        "semantic source hashes are invalid",
    )

    public_path = root / PUBLIC_TASK_SET
    hidden_path = root / HIDDEN_LABEL_SOURCE
    public_raw = public_path.read_bytes()
    hidden_raw = hidden_path.read_bytes()
    _require(_sha256(public_raw) == manifest["public_task_set_sha256"], "public task hash changed")
    _require(_sha256(hidden_raw) == manifest["hidden_label_source_sha256"], "hidden label hash changed")
    public = _strict_json(public_path, "public task set")
    _require(
        set(public) == {
            "schema_version",
            "task_plane_id",
            "tasks",
            "label_values_included",
            "credentials_recorded",
        }
        and public.get("schema_version") == PUBLIC_TASK_SET_SCHEMA_VERSION
        and public.get("task_plane_id") == manifest["task_plane_id"]
        and public.get("label_values_included") is False
        and public.get("credentials_recorded") is False,
        "public task set binding changed",
    )
    assert_hidden_oracle_fields_absent(public)
    tasks = public.get("tasks")
    _require(isinstance(tasks, list) and bool(tasks), "public task set is empty")
    normalized: list[dict[str, Any]] = []
    for item in tasks:
        _require(isinstance(item, Mapping), "public task must be an object")
        rebuilt = build_n1_public_task_binding(
            workload_id=item.get("workload_id"),
            object_id=item.get("object_id"),
            task_class_id=item.get("task_class_id"),
            question=item.get("question"),
            answer_options=item.get("answer_options"),
            success_scoring_rule=item.get("success_scoring_rule"),
        )
        _require(dict(item) == rebuilt, "public task binding hash changed")
        normalized.append(rebuilt)
    _require(
        normalized == sorted(normalized, key=lambda item: (item["workload_id"], item["object_id"])),
        "public tasks are not canonical",
    )
    _require(len(normalized) == manifest["public_task_count"], "public task count changed")

    hidden = _strict_json(hidden_path, "hidden label source")
    labels = hidden.get("labels")
    _require(
        hidden.get("oracle_id") == manifest["oracle_id"]
        and isinstance(labels, list)
        and len(labels) == manifest["hidden_label_count"],
        "hidden label binding changed",
    )
    oracle = verify_n1_oracle_package(root / ORACLE_PACKAGE)
    oracle_manifest_raw = (root / ORACLE_PACKAGE / "n1-oracle-package.json").read_bytes()
    _require(
        _sha256(oracle_manifest_raw) == manifest["oracle_package_manifest_sha256"],
        "nested oracle package binding changed",
    )
    _require(
        oracle["oracle_id"] == manifest["oracle_id"]
        and oracle["label_count"] == manifest["hidden_label_count"],
        "nested oracle package identity changed",
    )
    return {
        "status": "VERIFIED",
        "task_plane_id": manifest["task_plane_id"],
        "oracle_id": manifest["oracle_id"],
        "public_task_count": len(normalized),
        "hidden_label_count": len(labels),
        "checked_file_count": len(entries),
        "public_private_separation_verified": True,
        "label_values_returned": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "FULL_FLOW_TASK_PLANE_SCHEMA_VERSION",
    "PUBLIC_TASK_SET_SCHEMA_VERSION",
    "FullFlowTaskPlaneError",
    "build_full_flow_task_plane",
    "verify_full_flow_task_plane",
]
