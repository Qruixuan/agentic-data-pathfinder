"""Public pre-selection commitment for an N1 hidden-label package.

The commitment intentionally contains only hashes and public cardinalities.
It can be published before policy selection without exposing an answer.  A
later verifier may optionally mount the private N1 package to prove that it
opens the commitment.  No timestamp authority or external approval is
invented by this local mechanism.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from pathfinder.simulator.hidden_oracle import verify_n1_oracle_package


COMMITMENT_SCHEMA_VERSION = (
    "pathfinder.n1-oracle-preselection-commitment/v1alpha1"
)
COMMITMENT_NAME = "n1-oracle-preselection-commitment.json"
CHECKSUMS_NAME = "SHA256SUMS"

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+|-]{0,255}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "commitment_id",
        "logical_node_id",
        "oracle_id",
        "label_count",
        "hidden_labels_sha256",
        "public_task_set_sha256",
        "oracle_package_manifest_sha256",
        "commitment_scope",
        "label_values_included",
        "independent_timestamp_attested",
        "external_approval_attested",
        "credentials_recorded",
        "eligible_for_scientific_claims",
        "commitment_sha256",
    }
)


class HiddenOracleCommitmentError(ValueError):
    """Raised when a public oracle commitment cannot be trusted."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise HiddenOracleCommitmentError(message)


def _json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise HiddenOracleCommitmentError(
            "commitment is not canonical JSON"
        ) from exc


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise HiddenOracleCommitmentError(
            "commitment is not canonical JSON"
        ) from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_json(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(
                HiddenOracleCommitmentError(
                    f"non-finite JSON number: {value}"
                )
            ),
        )
    except HiddenOracleCommitmentError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HiddenOracleCommitmentError(
            f"cannot read canonical {label}: {path}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    _require(raw == _json_bytes(value), f"{label} is not canonical")
    return raw, value


def _identifier(value: Any, label: str) -> str:
    _require(isinstance(value, str), f"{label} must be a string")
    _require(
        _IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return value


def _digest(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{label} is not SHA-256",
    )
    return value


def _oracle_manifest(package: Path) -> tuple[bytes, dict[str, Any]]:
    verified = verify_n1_oracle_package(package)
    raw, manifest = _read_json(
        package / "n1-oracle-package.json",
        "N1 oracle package manifest",
    )
    _require(
        manifest.get("oracle_id") == verified["oracle_id"]
        and manifest.get("label_count") == verified["label_count"]
        and manifest.get("public_task_set_sha256")
        == verified["public_task_set_sha256"],
        "N1 oracle package verification disagrees with its manifest",
    )
    return raw, manifest


def freeze_n1_oracle_preselection_commitment(
    oracle_package_dir: str | Path,
    *,
    commitment_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Publish a label-free hash commitment to an immutable N1 package."""

    commitment_id = _identifier(commitment_id, "commitment_id")
    package = Path(oracle_package_dir).resolve()
    output = Path(output_dir).resolve()
    _require(not output.exists(), f"commitment directory exists: {output}")
    manifest_raw, manifest = _oracle_manifest(package)
    document: dict[str, Any] = {
        "schema_version": COMMITMENT_SCHEMA_VERSION,
        "status": "FROZEN_PRESELECTION_ORACLE_COMMITMENT",
        "commitment_id": commitment_id,
        "logical_node_id": "N1",
        "oracle_id": manifest["oracle_id"],
        "label_count": manifest["label_count"],
        "hidden_labels_sha256": manifest["hidden_labels_sha256"],
        "public_task_set_sha256": manifest["public_task_set_sha256"],
        "oracle_package_manifest_sha256": _sha256(manifest_raw),
        "commitment_scope": "hidden-labels-before-policy-selection",
        "label_values_included": False,
        "independent_timestamp_attested": False,
        "external_approval_attested": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    document["commitment_sha256"] = _sha256(_canonical_bytes(document))
    content = _json_bytes(document)
    checksums = f"{_sha256(content)}  {COMMITMENT_NAME}\n".encode("utf-8")

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=".n1-oracle-commitment-", dir=output.parent)
    )
    stage = temporary / "commitment"
    try:
        stage.mkdir()
        (stage / COMMITMENT_NAME).write_bytes(content)
        (stage / CHECKSUMS_NAME).write_bytes(checksums)
        verify_n1_oracle_preselection_commitment(
            stage,
            oracle_package_dir=package,
        )
        _require(not output.exists(), f"commitment directory exists: {output}")
        os.replace(stage, output)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)
    return {
        "status": "FROZEN_PRESELECTION_ORACLE_COMMITMENT",
        "commitment_id": commitment_id,
        "oracle_id": manifest["oracle_id"],
        "label_count": manifest["label_count"],
        "commitment_sha256": document["commitment_sha256"],
        "label_values_returned": False,
        "independent_timestamp_attested": False,
        "output_dir": str(output),
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_n1_oracle_preselection_commitment(
    commitment_dir: str | Path,
    *,
    oracle_package_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify public structure and optionally open it with the N1 package."""

    root = Path(commitment_dir).resolve()
    _require(root.is_dir(), f"commitment directory does not exist: {root}")
    _require(
        {path.name for path in root.iterdir()}
        == {COMMITMENT_NAME, CHECKSUMS_NAME},
        "commitment file set changed",
    )
    try:
        checksum_lines = (root / CHECKSUMS_NAME).read_text(
            encoding="utf-8"
        ).splitlines()
    except OSError as exc:
        raise HiddenOracleCommitmentError("cannot read SHA256SUMS") from exc
    _require(len(checksum_lines) == 1, "commitment checksum count changed")
    digest, separator, name = checksum_lines[0].partition("  ")
    _digest(digest, "commitment file checksum")
    _require(
        separator == "  " and name == COMMITMENT_NAME,
        "commitment checksum entry changed",
    )
    raw, document = _read_json(root / COMMITMENT_NAME, "commitment")
    _require(_sha256(raw) == digest, "commitment checksum mismatch")
    _require(set(document) == _FIELDS, "commitment fields changed")
    _require(
        document.get("schema_version") == COMMITMENT_SCHEMA_VERSION
        and document.get("status")
        == "FROZEN_PRESELECTION_ORACLE_COMMITMENT",
        "commitment schema or status changed",
    )
    _identifier(document.get("commitment_id"), "commitment_id")
    _identifier(document.get("oracle_id"), "oracle_id")
    _require(document.get("logical_node_id") == "N1", "logical node changed")
    _require(
        type(document.get("label_count")) is int
        and document["label_count"] > 0,
        "label_count must be positive",
    )
    for field in (
        "hidden_labels_sha256",
        "public_task_set_sha256",
        "oracle_package_manifest_sha256",
        "commitment_sha256",
    ):
        _digest(document.get(field), field)
    _require(
        document.get("commitment_scope")
        == "hidden-labels-before-policy-selection",
        "commitment scope changed",
    )
    for field in (
        "label_values_included",
        "independent_timestamp_attested",
        "external_approval_attested",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(document.get(field) is False, f"{field} must be false")
    unsigned = dict(document)
    recorded = unsigned.pop("commitment_sha256")
    _require(
        _sha256(_canonical_bytes(unsigned)) == recorded,
        "commitment content digest mismatch",
    )

    private_binding = False
    if oracle_package_dir is not None:
        manifest_raw, manifest = _oracle_manifest(
            Path(oracle_package_dir).resolve()
        )
        expected = {
            "logical_node_id": manifest["logical_node_id"],
            "oracle_id": manifest["oracle_id"],
            "label_count": manifest["label_count"],
            "hidden_labels_sha256": manifest["hidden_labels_sha256"],
            "public_task_set_sha256": manifest["public_task_set_sha256"],
            "oracle_package_manifest_sha256": _sha256(manifest_raw),
        }
        _require(
            all(document[field] == value for field, value in expected.items()),
            "private N1 package does not open this commitment",
        )
        private_binding = True
    return {
        "status": "VERIFIED",
        "commitment_id": document["commitment_id"],
        "oracle_id": document["oracle_id"],
        "label_count": document["label_count"],
        "commitment_sha256": document["commitment_sha256"],
        "private_package_binding_verified": private_binding,
        "label_values_returned": False,
        "independent_timestamp_attested": False,
        "checked_files": 2,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "COMMITMENT_NAME",
    "COMMITMENT_SCHEMA_VERSION",
    "HiddenOracleCommitmentError",
    "freeze_n1_oracle_preselection_commitment",
    "verify_n1_oracle_preselection_commitment",
]
