"""Freeze the N4 serve gate for an already-provisioned derived snapshot.

The local semantic experiment intentionally reuses a frozen N4 package rather
than paying for N5 materialization inside every matrix run.  This gate binds
that immutable package, its verified N5 provenance catalog, and the exact
Compose ``serve-frozen`` profile.  It does not claim that live materialization
was executed or measured; it authorizes only the preprovisioned path, where
the mutable N4 publication companion is excluded from the selected profile.
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

from .full_flow_compose_overlay import (
    CHECKSUMS_NAME as OVERLAY_CHECKSUMS_NAME,
    GATE_NAME as OVERLAY_GATE_NAME,
    MANIFEST_NAME as OVERLAY_MANIFEST_NAME,
    verify_full_flow_local_compose_overlay,
)
from .full_flow_provisioning_catalog import (
    CATALOG_NAME as PROVISIONING_CATALOG_NAME,
    verify_full_flow_provisioning_catalog,
)
from .n4_derived_data_plane import (
    N4_DERIVED_PACKAGE_VERIFIED_STATUS,
    PACKAGE_MANIFEST_NAME as N4_PACKAGE_MANIFEST_NAME,
    verify_n4_derived_data_package,
)


N4_SERVE_GATE_SCHEMA_VERSION = (
    "pathfinder.full-flow-n4-preprovisioned-serve-gate/v1alpha1"
)
GATE_NAME = "n4-preprovisioned-serve-gate.json"
CHECKSUMS_NAME = "SHA256SUMS"

_FILES = {GATE_NAME, CHECKSUMS_NAME}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")


class FullFlowN4ServeGateError(ValueError):
    """Raised when immutable N4 serving is not safely authorized."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowN4ServeGateError(message)


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
        raise FullFlowN4ServeGateError(
            "N4 serve gate is not canonical JSON"
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
                FullFlowN4ServeGateError(
                    f"{name} contains invalid number {token}"
                )
            ),
        )
    except FullFlowN4ServeGateError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowN4ServeGateError(f"cannot read {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _verified_sources(
    *,
    compose_overlay_dir: Path,
    service_bootstrap_dir: Path,
    deployment_binding_dir: Path,
    logical_plan_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
    provisioning_catalog_dir: Path,
    artifact_binding_dir: Path,
    n4_package_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        overlay = verify_full_flow_local_compose_overlay(
            compose_overlay_dir,
            service_bootstrap_dir=service_bootstrap_dir,
            deployment_binding_dir=deployment_binding_dir,
            logical_plan_dir=logical_plan_dir,
            scenario_path=scenario_path,
            container_plan_dir=container_plan_dir,
        )
        n4 = verify_n4_derived_data_package(n4_package_dir)
        provisioning = verify_full_flow_provisioning_catalog(
            provisioning_catalog_dir,
            artifact_binding_dir=artifact_binding_dir,
            n4_package_dir=n4_package_dir,
        )
    except Exception as exc:
        raise FullFlowN4ServeGateError(
            "N4 serve-gate source verification failed"
        ) from exc
    _require(
        overlay.get("status")
        == "VERIFIED_LOCAL_COMPOSE_OVERLAY_NOT_LAUNCHED"
        and overlay.get("n4_operator_gate_required") is True
        and overlay.get("n4_gate_satisfied") is False,
        "Compose source does not expose the expected staged N4 gate",
    )
    _require(
        n4.get("status") == N4_DERIVED_PACKAGE_VERIFIED_STATUS
        and n4.get("logical_node_id") == "N4",
        "N4 immutable package did not verify",
    )
    _require(
        provisioning.get("status") == "VERIFIED"
        and provisioning.get("all_artifacts_available") is True
        and provisioning.get("live_materialization_executed") is False,
        "N5/N4 preprovisioned provenance did not verify",
    )
    overlay_gate = _strict_json(
        compose_overlay_dir / OVERLAY_GATE_NAME,
        "Compose N4 stage gate",
    )
    _require(
        overlay_gate.get("gate_id")
        == "N4-publish-before-immutable-data-agent-rebind"
        and overlay_gate.get("serve_profile") == "serve-frozen"
        and overlay_gate.get("serve_profile_must_not_be_selected_before_gate")
        is True
        and overlay_gate.get("publication_mutation_during_trials_allowed")
        is False,
        "Compose N4 stage-gate semantics changed",
    )
    return overlay, n4, provisioning


def _document(
    *,
    gate_id: str,
    compose_overlay_dir: Path,
    overlay: Mapping[str, Any],
    n4_package_dir: Path,
    n4: Mapping[str, Any],
    provisioning_catalog_dir: Path,
    provisioning: Mapping[str, Any],
) -> dict[str, Any]:
    gate_id = _identifier(gate_id, "gate_id")
    n4_manifest = n4_package_dir / N4_PACKAGE_MANIFEST_NAME
    provisioning_manifest = (
        provisioning_catalog_dir / PROVISIONING_CATALOG_NAME
    )
    document: dict[str, Any] = {
        "schema_version": N4_SERVE_GATE_SCHEMA_VERSION,
        "status": "FROZEN_PREPROVISIONED_N4_SERVE_AUTHORIZATION",
        "gate_id": gate_id,
        "overlay_id": overlay["overlay_id"],
        "overlay_gate_file_sha256": _sha256(
            (compose_overlay_dir / OVERLAY_GATE_NAME).read_bytes()
        ),
        "overlay_manifest_file_sha256": _sha256(
            (compose_overlay_dir / OVERLAY_MANIFEST_NAME).read_bytes()
        ),
        "overlay_checksums_sha256": _sha256(
            (compose_overlay_dir / OVERLAY_CHECKSUMS_NAME).read_bytes()
        ),
        "n4_package_id": n4["package_id"],
        "n4_catalog_version": n4["catalog_version"],
        "n4_package_sha256": n4["package_sha256"],
        "n4_package_manifest_sha256": _sha256(n4_manifest.read_bytes()),
        "n4_artifact_count": n4["artifact_count"],
        "provisioning_catalog_id": provisioning["catalog_id"],
        "provisioning_catalog_sha256": provisioning["catalog_sha256"],
        "provisioning_catalog_file_sha256": _sha256(
            provisioning_manifest.read_bytes()
        ),
        "provisioning_entry_count": provisioning["entry_count"],
        "all_required_derived_artifacts_available": True,
        "all_content_and_provenance_bindings_verified": True,
        "authorized_compose_profile": "serve-frozen",
        "prohibited_concurrent_profile": "provision-derived",
        "publication_companion_excluded_by_authorized_profile": True,
        "publication_mutation_during_trials_allowed": False,
        "serve_profile_authorized": True,
        "preprovisioned_snapshot_used": True,
        "live_n5_materialization_executed": False,
        "live_n5_materialization_time_measured": False,
        "live_n5_materialization_cost_measured": False,
        "services_started": False,
        "workflow_submitted": False,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "upcloud_ready": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    document["gate_sha256"] = _sha256(_canonical(document))
    return document


def _expected_document(
    *,
    gate_id: str,
    compose_overlay_dir: Path,
    service_bootstrap_dir: Path,
    deployment_binding_dir: Path,
    logical_plan_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
    provisioning_catalog_dir: Path,
    artifact_binding_dir: Path,
    n4_package_dir: Path,
) -> dict[str, Any]:
    overlay, n4, provisioning = _verified_sources(
        compose_overlay_dir=compose_overlay_dir,
        service_bootstrap_dir=service_bootstrap_dir,
        deployment_binding_dir=deployment_binding_dir,
        logical_plan_dir=logical_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        provisioning_catalog_dir=provisioning_catalog_dir,
        artifact_binding_dir=artifact_binding_dir,
        n4_package_dir=n4_package_dir,
    )
    return _document(
        gate_id=gate_id,
        compose_overlay_dir=compose_overlay_dir,
        overlay=overlay,
        n4_package_dir=n4_package_dir,
        n4=n4,
        provisioning_catalog_dir=provisioning_catalog_dir,
        provisioning=provisioning,
    )


def freeze_full_flow_n4_preprovisioned_serve_gate(
    compose_overlay_dir: str | Path,
    service_bootstrap_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    *,
    gate_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Authorize only immutable preprovisioned N4 serving."""

    paths = {
        "compose_overlay_dir": Path(compose_overlay_dir).resolve(),
        "service_bootstrap_dir": Path(service_bootstrap_dir).resolve(),
        "deployment_binding_dir": Path(deployment_binding_dir).resolve(),
        "logical_plan_dir": Path(logical_plan_dir).resolve(),
        "scenario_path": Path(scenario_path).resolve(),
        "container_plan_dir": Path(container_plan_dir).resolve(),
        "provisioning_catalog_dir": Path(
            provisioning_catalog_dir
        ).resolve(),
        "artifact_binding_dir": Path(artifact_binding_dir).resolve(),
        "n4_package_dir": Path(n4_package_dir).resolve(),
    }
    document = _expected_document(gate_id=gate_id, **paths)
    payload = _json_bytes(document)
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".n4-serve-gate-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        (stage / GATE_NAME).write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {GATE_NAME}\n", encoding="utf-8"
        )
        _verify_gate_files(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return verify_full_flow_n4_preprovisioned_serve_gate(
        target,
        **paths,
    ) | {"output_dir": str(target)}


def _verify_gate_files(root: Path) -> dict[str, Any]:
    _require(root.is_dir(), "N4 serve gate directory is missing")
    files = list(root.iterdir())
    _require(
        {path.name for path in files} == _FILES
        and all(path.is_file() and not path.is_symlink() for path in files),
        "N4 serve gate file set changed",
    )
    payload = (root / GATE_NAME).read_bytes()
    _require(
        (root / CHECKSUMS_NAME).read_text(encoding="utf-8")
        == f"{_sha256(payload)}  {GATE_NAME}\n",
        "N4 serve gate checksums failed",
    )
    document = _strict_json(root / GATE_NAME, "N4 serve gate")
    supplied = _digest(document.get("gate_sha256"), "gate_sha256")
    unsigned = dict(document)
    del unsigned["gate_sha256"]
    _require(
        supplied == _sha256(_canonical(unsigned)),
        "N4 serve gate digest failed",
    )
    _require(
        document.get("schema_version") == N4_SERVE_GATE_SCHEMA_VERSION
        and document.get("status")
        == "FROZEN_PREPROVISIONED_N4_SERVE_AUTHORIZATION"
        and document.get("authorized_compose_profile") == "serve-frozen"
        and document.get("prohibited_concurrent_profile")
        == "provision-derived"
        and document.get("publication_companion_excluded_by_authorized_profile")
        is True
        and document.get("serve_profile_authorized") is True
        and document.get("live_n5_materialization_executed") is False
        and document.get("performance_measured") is False
        and document.get("monetary_cost_measured") is False
        and document.get("upcloud_ready") is False
        and document.get("credentials_recorded") is False,
        "N4 serve gate scope changed",
    )
    return document


def verify_full_flow_n4_preprovisioned_serve_gate(
    gate_dir: str | Path,
    compose_overlay_dir: str | Path,
    service_bootstrap_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
) -> dict[str, Any]:
    """Reproduce the N4 serve decision from every frozen source."""

    root = Path(gate_dir).resolve()
    document = _verify_gate_files(root)
    paths = {
        "compose_overlay_dir": Path(compose_overlay_dir).resolve(),
        "service_bootstrap_dir": Path(service_bootstrap_dir).resolve(),
        "deployment_binding_dir": Path(deployment_binding_dir).resolve(),
        "logical_plan_dir": Path(logical_plan_dir).resolve(),
        "scenario_path": Path(scenario_path).resolve(),
        "container_plan_dir": Path(container_plan_dir).resolve(),
        "provisioning_catalog_dir": Path(
            provisioning_catalog_dir
        ).resolve(),
        "artifact_binding_dir": Path(artifact_binding_dir).resolve(),
        "n4_package_dir": Path(n4_package_dir).resolve(),
    }
    expected = _expected_document(
        gate_id=str(document["gate_id"]),
        **paths,
    )
    _require(
        (root / GATE_NAME).read_bytes() == _json_bytes(expected),
        "N4 serve gate does not match its frozen sources",
    )
    return {
        "status": "VERIFIED",
        "gate_id": document["gate_id"],
        "gate_sha256": document["gate_sha256"],
        "serve_profile_authorized": True,
        "authorized_compose_profile": "serve-frozen",
        "prohibited_concurrent_profile": "provision-derived",
        "preprovisioned_snapshot_used": True,
        "live_n5_materialization_executed": False,
        "source_binding_checked": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "FullFlowN4ServeGateError",
    "GATE_NAME",
    "N4_SERVE_GATE_SCHEMA_VERSION",
    "freeze_full_flow_n4_preprovisioned_serve_gate",
    "verify_full_flow_n4_preprovisioned_serve_gate",
]
