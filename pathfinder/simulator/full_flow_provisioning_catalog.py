"""Freeze N5-to-N4 provenance for already materialized representations.

The semantic matrix is allowed to consume the immutable N4 snapshot without
re-running an expensive model.  That shortcut is safe only when every routed
derived artifact is tied to its frozen N5 derivation metadata and exact N4
package record.  The resulting catalog is *availability provenance*, not a
measurement of a live materialization operation.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from ._full_flow_primitives import (
    canonical_json_bytes,
    checked_identifier,
    checked_lower_sha256,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .full_flow_artifact_bindings import ARTIFACT_BINDINGS_NAME
from .full_flow_semantic_route_runtime import (
    ArtifactIdentity,
    ProvisioningReference,
)
from .n4_derived_data_plane import (
    PACKAGE_MANIFEST_NAME as N4_MANIFEST_NAME,
    verify_n4_derived_data_package,
)


PROVISIONING_CATALOG_SCHEMA_VERSION = (
    "pathfinder.full-flow-preprovisioned-derived-catalog/v1alpha1"
)
PROVISIONING_ENTRY_SCHEMA_VERSION = (
    "pathfinder.full-flow-preprovisioned-derived-entry/v1alpha1"
)
CATALOG_NAME = "full-flow-preprovisioned-derived-catalog.json"
CHECKSUMS_NAME = "SHA256SUMS"

_FILES = {CATALOG_NAME, CHECKSUMS_NAME}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_DERIVED = {"multimodal_digest", "sampled_frame_bundle"}


class FullFlowProvisioningCatalogError(ValueError):
    """Raised when frozen derived provenance is incomplete or ambiguous."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowProvisioningCatalogError(message)


def _sha256(value: bytes) -> str:
    return sha256_hex(value)


def _canonical(value: Any) -> bytes:
    return canonical_json_bytes(
        value,
        error_type=FullFlowProvisioningCatalogError,
        error_message="provisioning catalog is not canonical JSON",
    )


def _json_bytes(value: Any) -> bytes:
    return pretty_json_bytes(value)


def _strict_json(path: Path, label: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")

    try:
        value = strict_json_loads(
            path.read_text(encoding="utf-8"),
            error_type=FullFlowProvisioningCatalogError,
            duplicate_key_message=lambda key: f"{label} repeats key {key}",
            nonfinite_number_message=(
                lambda token: f"{label} contains invalid constant {token}"
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowProvisioningCatalogError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _digest(value: Any, label: str) -> str:
    return str(
        checked_lower_sha256(
            value,
            label,
            error_type=FullFlowProvisioningCatalogError,
            pattern=_SHA256,
        )
    )


def _identifier(value: Any, label: str) -> str:
    return str(
        checked_identifier(
            value,
            label,
            error_type=FullFlowProvisioningCatalogError,
            pattern=_IDENTIFIER,
        )
    )


def _binding_files(root: Path) -> tuple[dict[str, Any], str]:
    binding_path = root / ARTIFACT_BINDINGS_NAME
    checksum_path = root / CHECKSUMS_NAME
    _require(root.is_dir(), "artifact-binding directory is missing")
    _require(checksum_path.is_file(), "artifact-binding checksums are missing")
    try:
        rows = checksum_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowProvisioningCatalogError(
            "cannot read artifact-binding checksums"
        ) from exc
    matches = [row for row in rows if row.endswith(f"  {ARTIFACT_BINDINGS_NAME}")]
    _require(len(matches) == 1, "artifact binding has no unique checksum entry")
    expected, separator, name = matches[0].partition("  ")
    _require(separator and name == ARTIFACT_BINDINGS_NAME, "checksum row is invalid")
    _digest(expected, "artifact-binding checksum")
    _require(
        _sha256(binding_path.read_bytes()) == expected,
        "artifact-binding checksum failed",
    )
    return _strict_json(binding_path, "artifact bindings"), expected


def _document(
    binding_root: Path,
    n4_root: Path,
    catalog_id: str,
) -> dict[str, Any]:
    catalog_id = _identifier(catalog_id, "catalog_id")
    bindings, binding_sha = _binding_files(binding_root)
    try:
        n4_report = verify_n4_derived_data_package(n4_root)
    except Exception as exc:
        raise FullFlowProvisioningCatalogError(
            "N4 derived package verification failed"
        ) from exc
    n4_manifest_path = n4_root / N4_MANIFEST_NAME
    n4 = _strict_json(n4_manifest_path, "N4 package manifest")
    rows = n4.get("objects")
    _require(isinstance(rows, list), "N4 package objects are invalid")
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        _require(isinstance(raw, Mapping), "N4 object row is invalid")
        key = (
            _identifier(raw.get("object_id"), "N4 object_id"),
            _identifier(raw.get("representation_id"), "N4 representation_id"),
        )
        _require(key not in by_identity, "N4 artifact identity repeats")
        by_identity[key] = dict(raw)

    entries: list[dict[str, Any]] = []
    objects = bindings.get("objects")
    _require(isinstance(objects, list), "artifact-binding objects are invalid")
    for raw_object in objects:
        _require(isinstance(raw_object, Mapping), "artifact-binding object is invalid")
        logical = _identifier(
            raw_object.get("logical_object_id"), "logical_object_id"
        )
        artifact_object = _identifier(
            raw_object.get("artifact_object_id"), "artifact_object_id"
        )
        representations = raw_object.get("representations")
        _require(isinstance(representations, list), "representations are invalid")
        for raw_binding in representations:
            _require(isinstance(raw_binding, Mapping), "representation is invalid")
            representation = _identifier(
                raw_binding.get("representation_id"), "representation_id"
            )
            if representation not in _DERIVED:
                continue
            n4_row = by_identity.get((artifact_object, representation))
            _require(
                n4_row is not None,
                f"N4 lacks {artifact_object}/{representation}",
            )
            identity = ArtifactIdentity(
                object_id=artifact_object,
                representation_id=representation,
                artifact_sha256=_digest(
                    raw_binding.get("artifact_sha256"), "artifact SHA-256"
                ),
                artifact_size_bytes=raw_binding.get("artifact_size_bytes"),
                object_catalog_version=_identifier(
                    raw_binding.get("object_catalog_version"),
                    "object_catalog_version",
                ),
            )
            _require(
                n4_row.get("artifact_sha256") == identity.artifact_sha256
                and n4_row.get("artifact_size_bytes")
                == identity.artifact_size_bytes,
                "N4 and semantic artifact identities differ",
            )
            provenance = n4_row.get("provenance")
            _require(isinstance(provenance, Mapping), "N4 provenance is missing")
            _require(
                provenance.get("producer_node_id") == "N5",
                "derived artifact was not produced by N5",
            )
            _digest(provenance.get("derivation_sha256"), "N5 derivation SHA-256")
            n5_commitment = _sha256(_canonical({
                "domain": "pathfinder.preprovisioned-n5-provenance/v1",
                "artifact_identity_sha256": identity.commitment,
                "provenance": provenance,
            }))
            n4_commitment = _sha256(_canonical({
                "domain": "pathfinder.frozen-n4-publication/v1",
                "package_id": n4.get("package_id"),
                "catalog_version": n4.get("catalog_version"),
                "artifact_record": n4_row,
            }))
            entry = {
                "schema_version": PROVISIONING_ENTRY_SCHEMA_VERSION,
                "chain_id": f"artifact|{logical}|{representation}",
                "logical_object_id": logical,
                "artifact_identity": identity.to_dict(),
                "artifact_identity_sha256": identity.commitment,
                "n5_evidence_sha256": n5_commitment,
                "n4_publication_sha256": n4_commitment,
                "evidence_class": "verified-preprovisioned-snapshot-provenance",
                "available": True,
                "live_materialization_executed": False,
                "live_materialization_cost_measured": False,
            }
            entry["entry_sha256"] = _sha256(_canonical(entry))
            entries.append(entry)
    entries.sort(key=lambda row: row["chain_id"])
    _require(entries, "no derived provisioning entries were produced")
    _require(
        len(entries) == len({row["chain_id"] for row in entries}),
        "provisioning chain ID repeats",
    )
    value: dict[str, Any] = {
        "schema_version": PROVISIONING_CATALOG_SCHEMA_VERSION,
        "status": "FROZEN_PREPROVISIONED_DERIVED_ARTIFACTS",
        "catalog_id": catalog_id,
        "artifact_binding_set_sha256": binding_sha,
        "n4_package_manifest_sha256": _sha256(n4_manifest_path.read_bytes()),
        "n4_package_sha256": n4_report["package_sha256"],
        "entry_count": len(entries),
        "entries": entries,
        "all_artifacts_available": True,
        "live_materialization_executed": False,
        "live_materialization_cost_measured": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    value["catalog_sha256"] = _sha256(_canonical(value))
    return value


def _verify_files(root: Path) -> dict[str, Any]:
    _require(root.is_dir(), "provisioning catalog directory is missing")
    actual = {path.name for path in root.iterdir()}
    _require(actual == _FILES, "provisioning catalog file set changed")
    value = _strict_json(root / CATALOG_NAME, "provisioning catalog")
    supplied = _digest(value.pop("catalog_sha256", None), "catalog_sha256")
    _require(supplied == _sha256(_canonical(value)), "catalog digest failed")
    value["catalog_sha256"] = supplied
    expected = f"{_sha256((root / CATALOG_NAME).read_bytes())}  {CATALOG_NAME}\n"
    _require(
        (root / CHECKSUMS_NAME).read_text(encoding="utf-8") == expected,
        "provisioning catalog checksums failed",
    )
    return value


def build_full_flow_provisioning_catalog(
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    *,
    catalog_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze preprovisioned N5/N4 references without executing N5."""

    binding_root = Path(artifact_binding_dir).resolve()
    n4_root = Path(n4_package_dir).resolve()
    document = _document(binding_root, n4_root, catalog_id)
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".provisioning-catalog-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        payload = _json_bytes(document)
        (stage / CATALOG_NAME).write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_bytes(
            f"{_sha256(payload)}  {CATALOG_NAME}\n".encode("utf-8")
        )
        _verify_files(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return verify_full_flow_provisioning_catalog(
        target,
        artifact_binding_dir=binding_root,
        n4_package_dir=n4_root,
    ) | {"output_dir": str(target)}


def verify_full_flow_provisioning_catalog(
    catalog_dir: str | Path,
    *,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
) -> dict[str, Any]:
    """Re-derive every provisioning reference from the two frozen inputs."""

    root = Path(catalog_dir).resolve()
    supplied = _verify_files(root)
    expected = _document(
        Path(artifact_binding_dir).resolve(),
        Path(n4_package_dir).resolve(),
        str(supplied["catalog_id"]),
    )
    _require(
        (root / CATALOG_NAME).read_bytes() == _json_bytes(expected),
        "provisioning catalog does not match its frozen sources",
    )
    return {
        "status": "VERIFIED",
        "catalog_id": supplied["catalog_id"],
        "catalog_sha256": supplied["catalog_sha256"],
        "entry_count": supplied["entry_count"],
        "all_artifacts_available": True,
        "live_materialization_executed": False,
        "live_materialization_cost_measured": False,
        "source_binding_checked": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


class FrozenProvisioningCatalog:
    """Load verified catalog rows as route-runtime references."""

    def __init__(
        self,
        catalog_dir: str | Path,
        *,
        artifact_binding_dir: str | Path,
        n4_package_dir: str | Path,
    ) -> None:
        verify_full_flow_provisioning_catalog(
            catalog_dir,
            artifact_binding_dir=artifact_binding_dir,
            n4_package_dir=n4_package_dir,
        )
        document = _verify_files(Path(catalog_dir).resolve())
        self.catalog_id = str(document["catalog_id"])
        self.catalog_sha256 = str(document["catalog_sha256"])
        self.references = tuple(self._reference(row) for row in document["entries"])

    @staticmethod
    def _reference(row: Mapping[str, Any]) -> ProvisioningReference:
        identity = row["artifact_identity"]
        return ProvisioningReference(
            chain_id=str(row["chain_id"]),
            logical_object_id=str(row["logical_object_id"]),
            artifact_identity=ArtifactIdentity(
                object_id=str(identity["object_id"]),
                representation_id=str(identity["representation_id"]),
                artifact_sha256=str(identity["artifact_sha256"]),
                artifact_size_bytes=int(identity["artifact_size_bytes"]),
                object_catalog_version=str(identity["object_catalog_version"]),
            ),
            n5_evidence_sha256=str(row["n5_evidence_sha256"]),
            n4_publication_sha256=str(row["n4_publication_sha256"]),
            available=True,
        )


__all__ = [
    "CATALOG_NAME",
    "CHECKSUMS_NAME",
    "FrozenProvisioningCatalog",
    "FullFlowProvisioningCatalogError",
    "PROVISIONING_CATALOG_SCHEMA_VERSION",
    "PROVISIONING_ENTRY_SCHEMA_VERSION",
    "build_full_flow_provisioning_catalog",
    "verify_full_flow_provisioning_catalog",
]
