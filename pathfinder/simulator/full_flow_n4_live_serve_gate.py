"""Freeze a read-only N4 serve gate after live N5 publication.

This gate is deliberately separate from the historical preprovisioned gate.
It proves that a complete sequence of locally executed N5 -> N4 publication
receipts terminates at one immutable N4 generation and that newly compiled
downstream inputs bind that exact generation.  It never starts a service or
mutates the publication store.  The only authorized next mode is
``serve-frozen`` with the mutable publication companion excluded.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .full_flow_artifact_bindings import (
    ARTIFACT_BINDINGS_NAME,
    CHECKSUMS_NAME as ARTIFACT_CHECKSUMS_NAME,
    PROVENANCE_NAME as ARTIFACT_PROVENANCE_NAME,
    PROVENANCE_SCHEMA_VERSION,
)
from .full_flow_live_provisioning_smoke import (
    DIGEST_RECEIPT_NAME,
    RECEIPT_NAME,
    verify_n5_n4_live_frame_bundle_provisioning_smoke,
    verify_n5_n4_live_multimodal_digest_provisioning_smoke,
)
from .full_flow_local_semantic_admission import (
    ADMISSION_NAME as LOCAL_ADMISSION_NAME,
    CHECKSUMS_NAME as LOCAL_ADMISSION_CHECKSUMS_NAME,
    verify_full_flow_local_semantic_runtime_package,
)
from .full_flow_semantic_execution_admission import (
    ADMISSION_NAME as LEGACY_ADMISSION_NAME,
    CHECKSUMS_NAME as LEGACY_ADMISSION_CHECKSUMS_NAME,
    _verify_files as _verify_legacy_admission_files,
)
from .full_flow_semantic_matrix import (
    ARTIFACT_BINDINGS_NAME as SEMANTIC_ARTIFACT_BINDINGS_NAME,
    ARTIFACT_BINDING_SET_SCHEMA_VERSION,
    CHECKSUMS_NAME as SEMANTIC_CHECKSUMS_NAME,
    PLAN_NAME as SEMANTIC_PLAN_NAME,
    _verify_published as _verify_semantic_matrix_package,
)
from .n4_derived_data_plane import (
    CHECKSUMS_NAME as N4_CHECKSUMS_NAME,
    GENERATIONS_DIRECTORY_NAME,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
    OBJECT_CATALOG_PATH,
    PACKAGE_MANIFEST_NAME,
    STORE_DATABASE_NAME,
    FRAME_BUNDLE_REPRESENTATION_ID,
    verify_n4_derived_data_package,
    verify_n4_publication_receipt,
)


N4_LIVE_SERVE_GATE_SCHEMA_VERSION = (
    "pathfinder.full-flow-n4-live-serve-gate/v1alpha1"
)
GATE_NAME = "n4-live-serve-gate.json"
CHECKSUMS_NAME = "SHA256SUMS"

_FILES = {GATE_NAME, CHECKSUMS_NAME}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,255}\Z")
_DERIVED_REPRESENTATIONS = {
    FRAME_BUNDLE_REPRESENTATION_ID,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
}
_FRAME_BINDING_FIELDS = {"kind", "receipt_dir", "n5_plan"}
_DIGEST_BINDING_FIELDS = {
    "kind",
    "receipt_dir",
    "n5_digest_plan_dir",
    "source_video_path",
}


class FullFlowN4LiveServeGateError(ValueError):
    """Raised when a live N4 generation cannot be served immutably."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowN4LiveServeGateError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowN4LiveServeGateError(
            "live N4 serve gate is not canonical JSON"
        ) from exc


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


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _strict_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{name} repeats key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowN4LiveServeGateError(
                    f"{name} contains invalid number {token}"
                )
            ),
        )
    except FullFlowN4LiveServeGateError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowN4LiveServeGateError(f"cannot read {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _source_inventory(root: Path) -> tuple[str, list[dict[str, Any]]]:
    """Hash a source tree without recording host-specific absolute paths."""

    _require(root.is_dir() and not root.is_symlink(), "source directory is missing")
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        _require(not path.is_symlink(), "source tree contains a symbolic link")
        if path.is_dir():
            continue
        _require(path.is_file(), "source tree contains a non-regular entry")
        raw = path.read_bytes()
        rows.append({
            "path": path.relative_to(root).as_posix(),
            "size_bytes": len(raw),
            "sha256": _sha256(raw),
        })
    _require(bool(rows), "source directory is empty")
    return _sha256(_canonical(rows)), rows


def _read_store_database(
    store_root: Path,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    database = store_root / STORE_DATABASE_NAME
    _require(database.is_file(), "N4 publication database is missing")
    # immutable=1 prevents journal/shm creation.  This gate is intentionally a
    # quiescent, read-only transition between publication and serving.
    uri = database.as_uri() + "?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            current_row = connection.execute(
                """
                SELECT generation_id, package_sha256, catalog_version
                FROM n4_current_generation WHERE singleton = 1
                """
            ).fetchone()
            rows = connection.execute(
                """
                SELECT publication_id, request_sha256, generation_id,
                       receipt_json
                FROM n4_publications
                """
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise FullFlowN4LiveServeGateError(
            "cannot read the N4 publication database in immutable mode"
        ) from exc
    _require(current_row is not None, "N4 has no current immutable generation")
    current = {
        "generation_id": _identifier(current_row["generation_id"], "generation_id"),
        "package_sha256": _digest(current_row["package_sha256"], "package_sha256"),
        "catalog_version": _identifier(
            current_row["catalog_version"], "catalog_version"
        ),
    }
    publications: dict[str, dict[str, Any]] = {}
    for row in rows:
        publication_id = _identifier(row["publication_id"], "publication_id")
        _require(
            publication_id not in publications,
            "N4 publication database repeats a publication ID",
        )
        try:
            receipt = json.loads(row["receipt_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise FullFlowN4LiveServeGateError(
                "N4 publication database contains an invalid receipt"
            ) from exc
        publications[publication_id] = {
            "request_sha256": _digest(row["request_sha256"], "request_sha256"),
            "generation_id": _identifier(row["generation_id"], "generation_id"),
            "receipt": verify_n4_publication_receipt(receipt),
        }
    _require(bool(publications), "N4 publication history is empty")
    return current, publications


def _receipt_summary(binding: Mapping[str, Any]) -> dict[str, Any]:
    _require(isinstance(binding, Mapping), "live receipt binding is not an object")
    kind = binding.get("kind")
    if kind == "frame_bundle":
        _require(
            set(binding) == _FRAME_BINDING_FIELDS,
            "frame-bundle receipt binding field set changed",
        )
        receipt_root = Path(binding["receipt_dir"]).resolve()
        plan = binding.get("n5_plan")
        _require(isinstance(plan, Mapping), "frame-bundle N5 plan is missing")
        try:
            verify_n5_n4_live_frame_bundle_provisioning_smoke(
                receipt_root,
                n5_plan=plan,
            )
        except Exception as exc:
            raise FullFlowN4LiveServeGateError(
                "frame-bundle live receipt failed exact N5-plan verification"
            ) from exc
        receipt_name = RECEIPT_NAME
        document = _strict_json(receipt_root / receipt_name, "frame receipt")
        plan_id = _identifier(document.get("n5_plan_id"), "n5_plan_id")
        plan_sha256 = _digest(document.get("n5_plan_sha256"), "n5_plan_sha256")
        source_sha256 = _digest(
            plan.get("input", {}).get("sha256")
            if isinstance(plan.get("input"), Mapping)
            else None,
            "N5 source content SHA-256",
        )
        derivation_id = "n5-uniform-midpoint-frame-bundle-v1"
        derivation_sha256 = _digest(
            document.get("n5_transformation_contract_sha256"),
            "n5_transformation_contract_sha256",
        )
        publication_source_id = _identifier(
            plan.get("idempotency_key"), "N5 idempotency_key"
        )
        n5_replay = document.get("n5_materialization_idempotent_replay")
    elif kind == "multimodal_digest":
        _require(
            set(binding) == _DIGEST_BINDING_FIELDS,
            "digest receipt binding field set changed",
        )
        receipt_root = Path(binding["receipt_dir"]).resolve()
        try:
            verify_n5_n4_live_multimodal_digest_provisioning_smoke(
                receipt_root,
                n5_digest_plan_dir=Path(binding["n5_digest_plan_dir"]).resolve(),
                source_video_path=Path(binding["source_video_path"]).resolve(),
            )
        except Exception as exc:
            raise FullFlowN4LiveServeGateError(
                "digest live receipt failed exact N5-plan/source verification"
            ) from exc
        receipt_name = DIGEST_RECEIPT_NAME
        document = _strict_json(receipt_root / receipt_name, "digest receipt")
        plan_id = _identifier(
            document.get("n5_digest_plan_id"), "n5_digest_plan_id"
        )
        plan_sha256 = _digest(
            document.get("n5_digest_plan_sha256"), "n5_digest_plan_sha256"
        )
        result = document.get("n5_digest_result")
        _require(isinstance(result, Mapping), "N5 digest result is missing")
        source_sha256 = _digest(
            result.get("source_handle"), "N5 digest source content SHA-256"
        )
        derivation_id = "n5-multimodal-digest-v1"
        derivation_sha256 = plan_sha256
        publication_source_id = _identifier(
            result.get("request_id"), "N5 digest request_id"
        )
        n5_replay = document.get(
            "n5_digest_materialization_idempotent_replay"
        )
    else:
        raise FullFlowN4LiveServeGateError("live receipt kind is unsupported")

    _require(
        receipt_root.is_dir() and not receipt_root.is_symlink(),
        "live receipt directory is missing",
    )
    receipt_path = receipt_root / receipt_name
    raw = receipt_path.read_bytes()
    outer_sha256 = _digest(document.get("receipt_sha256"), "receipt_sha256")
    n4_receipt = verify_n4_publication_receipt(
        document.get("n4_publication_receipt")
    )
    representation_id = _identifier(
        document.get("representation_id"), "representation_id"
    )
    _require(
        representation_id in _DERIVED_REPRESENTATIONS,
        "live receipt is not for an N4 derived representation",
    )
    _require(
        representation_id
        == (
            FRAME_BUNDLE_REPRESENTATION_ID
            if kind == "frame_bundle"
            else MULTIMODAL_DIGEST_REPRESENTATION_ID
        ),
        "receipt kind and representation disagree",
    )
    _require(type(n5_replay) is bool, "N5 replay state is invalid")
    n4_replay = document.get("n4_publication_idempotent_replay")
    _require(type(n4_replay) is bool, "N4 replay state is invalid")
    _require(
        document.get("object_id") == n4_receipt["published_artifacts"][0]["object_id"]
        and document.get("artifact_sha256")
        == n4_receipt["published_artifacts"][0]["artifact_sha256"]
        and document.get("artifact_size_bytes")
        == n4_receipt["published_artifacts"][0]["artifact_size_bytes"]
        and document.get("n4_publication_id") == n4_receipt["publication_id"]
        and document.get("n4_previous_catalog_version")
        == n4_receipt["previous_catalog_version"]
        and document.get("n4_committed_catalog_version")
        == n4_receipt["committed_catalog_version"]
        and document.get("n4_generation_id") == n4_receipt["generation_id"]
        and document.get("n4_package_sha256") == n4_receipt["package_sha256"],
        "outer live receipt and N4 publication receipt disagree",
    )
    return {
        "receipt_kind": str(kind),
        "receipt_sha256": outer_sha256,
        "receipt_file_sha256": _sha256(raw),
        "smoke_id": _identifier(document.get("smoke_id"), "smoke_id"),
        "object_id": _identifier(document.get("object_id"), "object_id"),
        "representation_id": representation_id,
        "artifact_size_bytes": document["artifact_size_bytes"],
        "artifact_sha256": _digest(
            document.get("artifact_sha256"), "artifact_sha256"
        ),
        "n5_plan_id": plan_id,
        "n5_plan_sha256": plan_sha256,
        "n5_source_representation_id": _identifier(
            document.get("source_representation_id"),
            "source_representation_id",
        ),
        "n5_source_content_sha256": source_sha256,
        "n5_publication_source_id": publication_source_id,
        "n5_derivation_id": derivation_id,
        "n5_derivation_sha256": derivation_sha256,
        "n5_idempotent_replay": n5_replay,
        "n4_publication_id": n4_receipt["publication_id"],
        "n4_publication_receipt_sha256": n4_receipt["receipt_sha256"],
        "n4_request_sha256": n4_receipt["request_sha256"],
        "n4_previous_catalog_version": n4_receipt["previous_catalog_version"],
        "n4_committed_catalog_version": n4_receipt["committed_catalog_version"],
        "n4_generation_id": n4_receipt["generation_id"],
        "n4_package_sha256": n4_receipt["package_sha256"],
        "n4_idempotent_replay": n4_replay,
    }


def _verify_receipt_chain(
    bindings: Sequence[Mapping[str, Any]],
    store_root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    _require(
        isinstance(bindings, Sequence)
        and not isinstance(bindings, (str, bytes))
        and bool(bindings),
        "live receipt bindings are empty",
    )
    summaries = [_receipt_summary(binding) for binding in bindings]
    receipt_sha256 = [row["receipt_sha256"] for row in summaries]
    publication_ids = [row["n4_publication_id"] for row in summaries]
    identities = [
        (row["object_id"], row["representation_id"]) for row in summaries
    ]
    _require(
        len(receipt_sha256) == len(set(receipt_sha256))
        and len(publication_ids) == len(set(publication_ids)),
        "live receipt replay binding is duplicated or ambiguous",
    )
    _require(
        len(identities) == len(set(identities)),
        "live receipt chain publishes a representation more than once",
    )
    _require(
        summaries[0]["n4_previous_catalog_version"] is None,
        "live receipt chain does not begin from an empty N4 catalog",
    )
    for previous, current_row in zip(summaries, summaries[1:]):
        _require(
            current_row["n4_previous_catalog_version"]
            == previous["n4_committed_catalog_version"],
            "live N4 catalog chain is not contiguous",
        )

    current, stored = _read_store_database(store_root)
    _require(
        set(stored) == set(publication_ids),
        "N4 publication history is not the exact supplied live receipt chain",
    )
    generation_ids: set[str] = set()
    generation_reports: dict[str, dict[str, Any]] = {}
    for summary in summaries:
        publication = stored.get(summary["n4_publication_id"])
        _require(publication is not None, "live receipt is absent from N4 history")
        nested = verify_n4_publication_receipt(publication["receipt"])
        _require(
            publication["request_sha256"] == summary["n4_request_sha256"]
            and publication["generation_id"] == summary["n4_generation_id"]
            and nested["receipt_sha256"]
            == summary["n4_publication_receipt_sha256"],
            "live receipt replay does not match durable N4 history",
        )
        generation_id = summary["n4_generation_id"]
        generation = store_root / GENERATIONS_DIRECTORY_NAME / generation_id
        try:
            report = verify_n4_derived_data_package(generation)
        except Exception as exc:
            raise FullFlowN4LiveServeGateError(
                "an N4 receipt generation failed immutable-package verification"
            ) from exc
        _require(
            generation_id == "generation-" + report["package_sha256"]
            and report["package_sha256"] == summary["n4_package_sha256"]
            and report["catalog_version"]
            == summary["n4_committed_catalog_version"]
            and report["artifact_count"] == nested["artifact_count"]
            and report["object_count"] == nested["object_count"],
            "N4 receipt does not bind its immutable generation",
        )
        generation_ids.add(generation_id)
        generation_reports[generation_id] = report
    actual_generations = {
        path.name
        for path in (store_root / GENERATIONS_DIRECTORY_NAME).iterdir()
        if path.is_dir() and not path.is_symlink()
    }
    _require(
        actual_generations == generation_ids,
        "N4 publication store contains stale or mixed generations",
    )
    final = summaries[-1]
    _require(
        current
        == {
            "generation_id": final["n4_generation_id"],
            "package_sha256": final["n4_package_sha256"],
            "catalog_version": final["n4_committed_catalog_version"],
        },
        "live receipt chain does not terminate at the current N4 snapshot",
    )
    final_report = generation_reports[current["generation_id"]]
    return summaries, current, final_report


def _artifact_binding_source(
    root: Path,
    final_package: Path,
) -> tuple[dict[str, Any], dict[str, Any], set[tuple[str, str]]]:
    expected_names = {
        ARTIFACT_BINDINGS_NAME,
        ARTIFACT_PROVENANCE_NAME,
        ARTIFACT_CHECKSUMS_NAME,
    }
    _require(root.is_dir(), "rebound artifact-binding package is missing")
    entries = list(root.iterdir())
    _require(
        {path.name for path in entries} == expected_names
        and all(path.is_file() and not path.is_symlink() for path in entries),
        "rebound artifact-binding package file set changed",
    )
    bindings_raw = (root / ARTIFACT_BINDINGS_NAME).read_bytes()
    provenance_raw = (root / ARTIFACT_PROVENANCE_NAME).read_bytes()
    expected_checksums = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted({ARTIFACT_BINDINGS_NAME, ARTIFACT_PROVENANCE_NAME})
    )
    _require(
        (root / ARTIFACT_CHECKSUMS_NAME).read_bytes() == expected_checksums,
        "rebound artifact-binding package checksums failed",
    )
    bindings = _strict_json(root / ARTIFACT_BINDINGS_NAME, "artifact bindings")
    provenance = _strict_json(
        root / ARTIFACT_PROVENANCE_NAME,
        "artifact-binding provenance",
    )
    _require(
        bindings_raw == _json_bytes(bindings)
        and provenance_raw == _json_bytes(provenance),
        "rebound artifact-binding package is not canonical",
    )
    _require(
        bindings.get("schema_version") == ARTIFACT_BINDING_SET_SCHEMA_VERSION
        and bindings.get("credentials_recorded") is False
        and provenance.get("schema_version") == PROVENANCE_SCHEMA_VERSION
        and provenance.get("status") == "FROZEN_VERIFIED_ARTIFACT_BINDINGS"
        and provenance.get("binding_set_id") == bindings.get("binding_set_id")
        and provenance.get("artifact_binding_set_sha256")
        == _sha256(bindings_raw)
        and provenance.get("data_agent_plan_binding_coverage_verified") is True
        and provenance.get("credentials_recorded") is False,
        "rebound artifact-binding identity or safety binding changed",
    )
    _identifier(bindings.get("binding_set_id"), "binding_set_id")
    final_manifest = final_package / PACKAGE_MANIFEST_NAME
    final_checksums = final_package / N4_CHECKSUMS_NAME
    _require(
        provenance.get("n4_package_manifest_sha256")
        == _sha256(final_manifest.read_bytes())
        and provenance.get("n4_package_checksums_sha256")
        == _sha256(final_checksums.read_bytes()),
        "rebound artifact bindings do not bind the final N4 generation",
    )
    required: set[tuple[str, str]] = set()
    objects = bindings.get("objects")
    _require(isinstance(objects, list) and bool(objects), "artifact bindings are empty")
    for item in objects:
        _require(isinstance(item, Mapping), "artifact binding object is invalid")
        object_id = _identifier(item.get("artifact_object_id"), "artifact_object_id")
        representations = item.get("representations")
        _require(isinstance(representations, list), "representations are invalid")
        for representation in representations:
            _require(
                isinstance(representation, Mapping),
                "artifact representation is invalid",
            )
            representation_id = representation.get("representation_id")
            if representation_id in _DERIVED_REPRESENTATIONS:
                required.add((object_id, str(representation_id)))
    _require(bool(required), "rebound bindings require no N4 derived artifacts")
    return bindings, provenance, required


def _semantic_source(
    root: Path,
    artifact_binding_root: Path,
) -> dict[str, Any]:
    try:
        plan = _verify_semantic_matrix_package(root)
    except Exception as exc:
        raise FullFlowN4LiveServeGateError(
            "rebound semantic matrix failed self-contained verification"
        ) from exc
    _require(
        (root / SEMANTIC_ARTIFACT_BINDINGS_NAME).read_bytes()
        == (artifact_binding_root / ARTIFACT_BINDINGS_NAME).read_bytes()
        and plan.get("source_bindings", {}).get("artifact_binding_set_sha256")
        == _sha256((artifact_binding_root / ARTIFACT_BINDINGS_NAME).read_bytes()),
        "rebound semantic matrix does not bind the rebound artifacts",
    )
    return plan


def _admission_source(
    root: Path,
    semantic_root: Path,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    if (root / LEGACY_ADMISSION_NAME).is_file():
        try:
            admission = _verify_legacy_admission_files(root)
        except Exception as exc:
            raise FullFlowN4LiveServeGateError(
                "rebound semantic admission failed self-contained verification"
            ) from exc
        source = admission.get("source_bindings")
        kind = "semantic-execution-admission"
        admission_name = LEGACY_ADMISSION_NAME
        checksum_name = LEGACY_ADMISSION_CHECKSUMS_NAME
        admission_id = admission.get("admission_id")
        admission_sha256 = admission.get("admission_sha256")
    elif (root / LOCAL_ADMISSION_NAME).is_file():
        try:
            verify_full_flow_local_semantic_runtime_package(root)
        except Exception as exc:
            raise FullFlowN4LiveServeGateError(
                "rebound local semantic admission failed public verification"
            ) from exc
        admission = _strict_json(root / LOCAL_ADMISSION_NAME, "local admission")
        source = admission.get("source_commitments", {}).get(
            "legacy_original_source_bindings"
        )
        kind = "local-semantic-execution-admission"
        admission_name = LOCAL_ADMISSION_NAME
        checksum_name = LOCAL_ADMISSION_CHECKSUMS_NAME
        admission_id = admission.get("promotion_id")
        admission_sha256 = admission.get("admission_sha256")
    else:
        raise FullFlowN4LiveServeGateError(
            "rebound semantic admission package type is unsupported"
        )
    _require(isinstance(source, Mapping), "admission source bindings are missing")
    _require(
        source.get("semantic_matrix_plan_sha256") == plan.get("plan_sha256")
        and source.get("semantic_matrix_source_binding_sha256")
        == plan.get("source_binding_sha256")
        and source.get("semantic_matrix_checksums_sha256")
        == _sha256((semantic_root / SEMANTIC_CHECKSUMS_NAME).read_bytes()),
        "rebound admission does not bind the rebound semantic matrix",
    )
    return {
        "kind": kind,
        "admission_id": _identifier(admission_id, "admission_id"),
        "admission_sha256": _digest(admission_sha256, "admission_sha256"),
        "admission_file_sha256": _sha256((root / admission_name).read_bytes()),
        "admission_checksums_sha256": _sha256((root / checksum_name).read_bytes()),
    }


def _expected_document(
    *,
    live_receipt_bindings: Sequence[Mapping[str, Any]],
    n4_publication_store_root: Path,
    rebound_artifact_binding_dir: Path,
    rebound_semantic_matrix_dir: Path,
    rebound_admission_dir: Path,
    gate_id: str,
) -> dict[str, Any]:
    gate_id = _identifier(gate_id, "gate_id")
    before_sha256, before_inventory = _source_inventory(
        n4_publication_store_root
    )
    summaries, current, final_report = _verify_receipt_chain(
        live_receipt_bindings,
        n4_publication_store_root,
    )
    final_package = (
        n4_publication_store_root
        / GENERATIONS_DIRECTORY_NAME
        / current["generation_id"]
    )
    bindings, provenance, required = _artifact_binding_source(
        rebound_artifact_binding_dir,
        final_package,
    )
    semantic = _semantic_source(
        rebound_semantic_matrix_dir,
        rebound_artifact_binding_dir,
    )
    admission = _admission_source(
        rebound_admission_dir,
        rebound_semantic_matrix_dir,
        semantic,
    )

    final_manifest = _strict_json(
        final_package / PACKAGE_MANIFEST_NAME,
        "final N4 package manifest",
    )
    final_rows = {
        (row["object_id"], row["representation_id"]): row
        for row in final_manifest["objects"]
    }
    receipt_rows = {
        (row["object_id"], row["representation_id"]): row for row in summaries
    }
    _require(
        set(final_rows) == required == set(receipt_rows),
        "live receipts, final N4 snapshot, and rebound bindings do not have "
        "exact derived-representation coverage",
    )
    for identity in sorted(required):
        final = final_rows[identity]
        receipt = receipt_rows[identity]
        provenance_row = final.get("provenance")
        _require(isinstance(provenance_row, Mapping), "N4 lineage is missing")
        _require(
            final.get("artifact_sha256") == receipt["artifact_sha256"]
            and final.get("artifact_size_bytes") == receipt["artifact_size_bytes"]
            and receipt["n5_plan_id"] in final.get("plan_ids", [])
            and provenance_row.get("producer_node_id") == "N5"
            and provenance_row.get("publication_source_id")
            == receipt["n5_publication_source_id"]
            and provenance_row.get("source_representation_id")
            == receipt["n5_source_representation_id"]
            and provenance_row.get("source_content_sha256")
            == receipt["n5_source_content_sha256"]
            and provenance_row.get("derivation_id")
            == receipt["n5_derivation_id"]
            and provenance_row.get("derivation_sha256")
            == receipt["n5_derivation_sha256"],
            f"final N4 content or N5 lineage changed for {identity}",
        )
        binding_item = next(
            item
            for item in bindings["objects"]
            if item["artifact_object_id"] == identity[0]
        )
        binding_representation = next(
            row
            for row in binding_item["representations"]
            if row["representation_id"] == identity[1]
        )
        _require(
            binding_representation["artifact_sha256"]
            == final["artifact_sha256"]
            and binding_representation["artifact_size_bytes"]
            == final["artifact_size_bytes"]
            and binding_representation["object_catalog_version"]
            == current["catalog_version"],
            f"rebound artifact identity changed for {identity}",
        )

    after_sha256, after_inventory = _source_inventory(n4_publication_store_root)
    _require(
        before_sha256 == after_sha256 and before_inventory == after_inventory,
        "N4 publication store changed while freezing the live serve gate",
    )
    receipt_set = sorted(row["receipt_sha256"] for row in summaries)
    catalog_raw = (final_package / OBJECT_CATALOG_PATH).read_bytes()
    manifest_raw = (final_package / PACKAGE_MANIFEST_NAME).read_bytes()
    n4_checksums_raw = (final_package / N4_CHECKSUMS_NAME).read_bytes()
    source_commitments = {
        "receipt_sha256_set": receipt_set,
        "receipt_sha256_set_sha256": _sha256(_canonical(receipt_set)),
        "n4_generation_id": current["generation_id"],
        "n4_package_sha256": current["package_sha256"],
        "n4_catalog_version": current["catalog_version"],
        "n4_catalog_file_sha256": _sha256(catalog_raw),
        "n4_package_manifest_file_sha256": _sha256(manifest_raw),
        "n4_package_checksums_sha256": _sha256(n4_checksums_raw),
        "artifact_binding_set_sha256": provenance[
            "artifact_binding_set_sha256"
        ],
        "artifact_binding_provenance_file_sha256": _sha256(
            (rebound_artifact_binding_dir / ARTIFACT_PROVENANCE_NAME).read_bytes()
        ),
        "artifact_binding_checksums_sha256": _sha256(
            (rebound_artifact_binding_dir / ARTIFACT_CHECKSUMS_NAME).read_bytes()
        ),
        "semantic_matrix_plan_sha256": semantic["plan_sha256"],
        "semantic_matrix_plan_file_sha256": _sha256(
            (rebound_semantic_matrix_dir / SEMANTIC_PLAN_NAME).read_bytes()
        ),
        "semantic_matrix_checksums_sha256": _sha256(
            (rebound_semantic_matrix_dir / SEMANTIC_CHECKSUMS_NAME).read_bytes()
        ),
        **admission,
        "n4_publication_store_snapshot_sha256": before_sha256,
    }
    source_commitments["source_commitments_sha256"] = _sha256(
        _canonical(source_commitments)
    )
    document: dict[str, Any] = {
        "schema_version": N4_LIVE_SERVE_GATE_SCHEMA_VERSION,
        "status": "FROZEN_LIVE_N4_SERVE_AUTHORIZATION",
        "gate_id": gate_id,
        "live_receipt_count": len(summaries),
        "frame_bundle_receipt_count": sum(
            row["receipt_kind"] == "frame_bundle" for row in summaries
        ),
        "multimodal_digest_receipt_count": sum(
            row["receipt_kind"] == "multimodal_digest" for row in summaries
        ),
        "required_derived_identity_count": len(required),
        "final_n4_object_count": final_report["object_count"],
        "final_n4_artifact_count": final_report["artifact_count"],
        "live_receipt_bindings": summaries,
        "source_commitments": source_commitments,
        "live_receipt_chain_contiguous": True,
        "live_receipts_exactly_cover_rebound_n4_artifacts": True,
        "final_n4_generation_verified": True,
        "rebound_artifact_bindings_verified": True,
        "rebound_semantic_matrix_verified": True,
        "rebound_semantic_admission_verified": True,
        "live_n5_materialization_executed": True,
        "authorized_compose_profile": "serve-frozen",
        "serve_profile_authorized": True,
        "publication_companion_excluded_by_authorized_profile": True,
        "publication_mutation_during_trials_allowed": False,
        "n4_data_agent_rebind_inputs_verified": True,
        "n4_data_agent_runtime_rebind_executed": False,
        "source_artifacts_modified": False,
        "services_started": False,
        "workflow_submitted": False,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "cloud_network_measured": False,
        "upcloud_used": False,
        "upcloud_ready": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    document["gate_sha256"] = _sha256(_canonical(document))
    return document


def _normal_paths(
    n4_publication_store_root: str | Path,
    rebound_artifact_binding_dir: str | Path,
    rebound_semantic_matrix_dir: str | Path,
    rebound_admission_dir: str | Path,
) -> dict[str, Path]:
    return {
        "n4_publication_store_root": Path(n4_publication_store_root).resolve(),
        "rebound_artifact_binding_dir": Path(
            rebound_artifact_binding_dir
        ).resolve(),
        "rebound_semantic_matrix_dir": Path(
            rebound_semantic_matrix_dir
        ).resolve(),
        "rebound_admission_dir": Path(rebound_admission_dir).resolve(),
    }


def _verify_gate_files(root: Path) -> dict[str, Any]:
    _require(root.is_dir(), "live N4 serve gate directory is missing")
    entries = list(root.iterdir())
    _require(
        {path.name for path in entries} == _FILES
        and all(path.is_file() and not path.is_symlink() for path in entries),
        "live N4 serve gate file set changed",
    )
    payload = (root / GATE_NAME).read_bytes()
    _require(
        (root / CHECKSUMS_NAME).read_text(encoding="utf-8")
        == f"{_sha256(payload)}  {GATE_NAME}\n",
        "live N4 serve gate checksums failed",
    )
    document = _strict_json(root / GATE_NAME, "live N4 serve gate")
    _require(payload == _json_bytes(document), "live N4 serve gate is not canonical")
    recorded = _digest(document.get("gate_sha256"), "gate_sha256")
    unsigned = dict(document)
    del unsigned["gate_sha256"]
    _require(
        recorded == _sha256(_canonical(unsigned)),
        "live N4 serve gate digest failed",
    )
    _require(
        document.get("schema_version") == N4_LIVE_SERVE_GATE_SCHEMA_VERSION
        and document.get("status") == "FROZEN_LIVE_N4_SERVE_AUTHORIZATION"
        and document.get("live_n5_materialization_executed") is True
        and document.get("authorized_compose_profile") == "serve-frozen"
        and document.get("serve_profile_authorized") is True
        and document.get("publication_companion_excluded_by_authorized_profile")
        is True
        and document.get("publication_mutation_during_trials_allowed") is False
        and document.get("n4_data_agent_rebind_inputs_verified") is True
        and document.get("n4_data_agent_runtime_rebind_executed") is False
        and document.get("source_artifacts_modified") is False
        and document.get("services_started") is False
        and document.get("workflow_submitted") is False
        and document.get("performance_measured") is False
        and document.get("monetary_cost_measured") is False
        and document.get("upcloud_used") is False
        and document.get("upcloud_ready") is False
        and document.get("credentials_recorded") is False
        and document.get("eligible_for_scientific_claims") is False,
        "live N4 serve gate claim boundary changed",
    )
    return document


def freeze_full_flow_n4_live_serve_gate(
    live_receipt_bindings: Sequence[Mapping[str, Any]],
    n4_publication_store_root: str | Path,
    rebound_artifact_binding_dir: str | Path,
    rebound_semantic_matrix_dir: str | Path,
    rebound_admission_dir: str | Path,
    *,
    gate_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze a serve-only authorization for the current live N4 snapshot."""

    paths = _normal_paths(
        n4_publication_store_root,
        rebound_artifact_binding_dir,
        rebound_semantic_matrix_dir,
        rebound_admission_dir,
    )
    target = Path(output_dir).resolve()
    for source in paths.values():
        _require(
            target != source
            and not target.is_relative_to(source)
            and not source.is_relative_to(target),
            "gate output and verified sources must be disjoint",
        )
    document = _expected_document(
        live_receipt_bindings=live_receipt_bindings,
        gate_id=gate_id,
        **paths,
    )
    payload = _json_bytes(document)
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".n4-live-serve-gate-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        (stage / GATE_NAME).write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {GATE_NAME}\n",
            encoding="utf-8",
        )
        _verify_gate_files(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return verify_full_flow_n4_live_serve_gate(
        target,
        live_receipt_bindings,
        **paths,
    ) | {"output_dir": str(target)}


def verify_full_flow_n4_live_serve_gate(
    gate_dir: str | Path,
    live_receipt_bindings: Sequence[Mapping[str, Any]],
    n4_publication_store_root: str | Path,
    rebound_artifact_binding_dir: str | Path,
    rebound_semantic_matrix_dir: str | Path,
    rebound_admission_dir: str | Path,
) -> dict[str, Any]:
    """Reproduce the gate from receipts, CAS state, and rebound inputs."""

    root = Path(gate_dir).resolve()
    document = _verify_gate_files(root)
    paths = _normal_paths(
        n4_publication_store_root,
        rebound_artifact_binding_dir,
        rebound_semantic_matrix_dir,
        rebound_admission_dir,
    )
    for source in paths.values():
        _require(
            root != source
            and not root.is_relative_to(source)
            and not source.is_relative_to(root),
            "gate output and verified sources must be disjoint",
        )
    expected = _expected_document(
        live_receipt_bindings=live_receipt_bindings,
        gate_id=document["gate_id"],
        **paths,
    )
    _require(
        (root / GATE_NAME).read_bytes() == _json_bytes(expected),
        "live N4 serve gate does not match its current frozen sources",
    )
    commitments = document["source_commitments"]
    return {
        "status": "VERIFIED",
        "gate_id": document["gate_id"],
        "gate_sha256": document["gate_sha256"],
        "live_receipt_count": document["live_receipt_count"],
        "required_derived_identity_count": document[
            "required_derived_identity_count"
        ],
        "n4_generation_id": commitments["n4_generation_id"],
        "n4_package_sha256": commitments["n4_package_sha256"],
        "n4_catalog_version": commitments["n4_catalog_version"],
        "live_n5_materialization_executed": True,
        "authorized_compose_profile": "serve-frozen",
        "publication_companion_excluded": True,
        "n4_data_agent_rebind_inputs_verified": True,
        "n4_data_agent_runtime_rebind_executed": False,
        "source_binding_checked": True,
        "source_artifacts_modified": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "FullFlowN4LiveServeGateError",
    "GATE_NAME",
    "N4_LIVE_SERVE_GATE_SCHEMA_VERSION",
    "freeze_full_flow_n4_live_serve_gate",
    "verify_full_flow_n4_live_serve_gate",
]
