"""Offline aggregate readiness evidence for the pre-UpCloud simulator.

This module does not contact services.  It invokes the existing offline
verifiers, binds the exact source bytes, and records which evidence classes
are present.  Passing this audit is deliberately not a performance,
multi-host, monetary-cost, or scientific-readiness result.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
from pathlib import Path
from typing import Any, Mapping

from pathfinder.integrations.flowmesh.container_formal_profile import (
    verify_flowmesh_container_formal_execution_profile,
)
from pathfinder.integrations.flowmesh.container_matrix import (
    verify_flowmesh_container_matrix_plan,
)
from pathfinder.integrations.flowmesh.container_matrix_coordinator import (
    verify_flowmesh_container_matrix_coordinator_dry_run,
)
from pathfinder.integrations.flowmesh.container_matrix_runner import (
    verify_flowmesh_container_matrix_run,
)
from pathfinder.integrations.flowmesh.w4_candidate_matrix import (
    CANDIDATE_RUN_DIR_NAME,
    COMPONENT_RECEIPT_DIR_NAME,
    verify_flowmesh_w4_candidate_matrix_plan,
    verify_flowmesh_w4_candidate_matrix_run,
)

from .full_flow_artifact_bindings import verify_full_flow_artifact_bindings
from .full_flow_bulk_live_provisioning import (
    FINAL_DIRECTORY_NAME as BULK_FINAL_DIRECTORY_NAME,
    LIVE_RECEIPT_BINDINGS_NAME,
    verify_full_flow_bulk_live_provisioning,
)
from .full_flow_local_semantic_matrix_gate import (
    MATRIX_RUN_DIR_NAME as SEMANTIC_MATRIX_RUN_DIR_NAME,
    verify_smoke_gated_full_flow_local_semantic_matrix_run,
)
from .full_flow_local_semantic_smoke import N4LiveServeGateSources
from .full_flow_provisioning_catalog import (
    verify_full_flow_provisioning_catalog,
)
from .full_flow_w4_candidate_routes import (
    verify_full_flow_w4_candidate_routes,
)
from .full_flow_w4_candidate_coordinator import (
    OBSERVATIONS_NAME as W4_RETRIEVAL_OBSERVATIONS_NAME,
)
from .full_flow_w4_live_executor import (
    verify_full_flow_w4_component_execution_receipt,
    verify_full_flow_w4_index_artifact_crosswalk,
)
from .full_flow_w4_retrieval_contract import (
    verify_full_flow_w4_retrieval_contract,
    verify_full_flow_w4_retrieval_evaluation,
)
from .n4_derived_data_plane import verify_n4_derived_data_package
from .policy_oed_bridge import verify_full_flow_observations


READINESS_REPORT_SCHEMA_VERSION = (
    "pathfinder.full-flow-pre-upcloud-readiness-report/v1alpha3"
)
READINESS_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.full-flow-pre-upcloud-readiness-manifest/v1alpha3"
)
REPORT_NAME = "pre-upcloud-readiness-report.json"
MANIFEST_NAME = "pre-upcloud-readiness-manifest.json"
CHECKSUMS_NAME = "SHA256SUMS"

REQUIRED_CODE_READY = "REQUIRED_CODE_READY"
LOCAL_LIVE_EVIDENCE_PRESENT = "LOCAL_LIVE_EVIDENCE_PRESENT"
LOCAL_LIVE_EVIDENCE_MISSING = "LOCAL_LIVE_EVIDENCE_MISSING"
FLOWMESH_EVIDENCE_PRESENT = "FLOWMESH_EVIDENCE_PRESENT"
FLOWMESH_EVIDENCE_MISSING = "FLOWMESH_EVIDENCE_MISSING"
UPCLOUD_ONLY_GAPS_REMAIN = "UPCLOUD_ONLY_GAPS_REMAIN"

_OUTPUT_FILES = {REPORT_NAME, MANIFEST_NAME, CHECKSUMS_NAME}
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_GIT_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_FORBIDDEN_KEYS = re.compile(
    r"(?:^|_)(?:api_?key|bearer|password|secret|token|base_?url|endpoint)"
    r"(?:$|_)",
    re.IGNORECASE,
)


class FullFlowPreUpcloudReadinessError(RuntimeError):
    """Raised when aggregate readiness evidence cannot be trusted."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowPreUpcloudReadinessError(message)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str)
        and value == value.strip()
        and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _git_revision(value: Any) -> str:
    _require(
        isinstance(value, str)
        and _GIT_REVISION.fullmatch(value) is not None,
        "source_git_revision must be a lowercase 40-hex commit identity",
    )
    return value


def _live_gate_source_paths(
    sources: N4LiveServeGateSources | None,
) -> list[tuple[str, Path]]:
    if sources is None:
        return []
    _require(
        isinstance(sources, Mapping)
        and set(sources) == {
            "live_receipt_bindings",
            "n4_publication_store_root",
            "rebound_artifact_binding_dir",
            "rebound_semantic_matrix_dir",
            "rebound_admission_dir",
        },
        "N4 live-gate source descriptor fields changed",
    )
    bindings = sources["live_receipt_bindings"]
    _require(
        isinstance(bindings, (list, tuple)) and bool(bindings),
        "N4 live-gate receipt bindings must be a non-empty sequence",
    )
    rows: list[tuple[str, Path]] = [
        (
            "n4-live-publication-store",
            Path(sources["n4_publication_store_root"]).resolve(),
        ),
        (
            "n4-live-rebound-artifact-bindings",
            Path(sources["rebound_artifact_binding_dir"]).resolve(),
        ),
        (
            "n4-live-rebound-semantic-matrix",
            Path(sources["rebound_semantic_matrix_dir"]).resolve(),
        ),
        (
            "n4-live-rebound-admission",
            Path(sources["rebound_admission_dir"]).resolve(),
        ),
    ]
    for index, binding in enumerate(bindings):
        _require(
            isinstance(binding, Mapping)
            and binding.get("kind") in {
                "frame_bundle",
                "multimodal_digest",
            }
            and "receipt_dir" in binding,
            "N4 live-gate receipt binding is invalid",
        )
        expected_fields = (
            {"kind", "receipt_dir", "n5_plan"}
            if binding.get("kind") == "frame_bundle"
            else {
                "kind",
                "receipt_dir",
                "n5_digest_plan_dir",
                "source_video_path",
            }
        )
        _require(
            set(binding) == expected_fields,
            "N4 live-gate receipt binding fields changed",
        )
        rows.append((
            f"n4-live-receipt-{index:03d}",
            Path(binding["receipt_dir"]).resolve(),
        ))
        if binding.get("kind") == "multimodal_digest":
            rows.extend((
                (
                    f"n4-live-digest-plan-{index:03d}",
                    Path(binding["n5_digest_plan_dir"]).resolve(),
                ),
                (
                    f"n4-live-source-video-{index:03d}",
                    Path(binding["source_video_path"]).resolve(),
                ),
            ))
    return rows


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
        raise FullFlowPreUpcloudReadinessError(
            "readiness value is not canonical JSON"
        ) from exc


def _json_bytes(value: Any) -> bytes:
    try:
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
    except (TypeError, ValueError) as exc:
        raise FullFlowPreUpcloudReadinessError(
            "readiness value is not serializable"
        ) from exc


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _document_sha256(document: Mapping[str, Any], field: str) -> str:
    value = dict(document)
    value.pop(field, None)
    return _sha256(_canonical(value))


def _strict_json(payload: bytes, name: str) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise FullFlowPreUpcloudReadinessError(
                    f"{name} repeats JSON key {key}"
                )
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise FullFlowPreUpcloudReadinessError(
            f"{name} contains non-finite JSON number {value}"
        )

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=reject_constant,
        )
    except FullFlowPreUpcloudReadinessError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowPreUpcloudReadinessError(
            f"cannot parse {name}"
        ) from exc
    return value


def _assert_safe_output(value: Any, name: str = "output") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            _require(
                key == "credentials_recorded"
                or _FORBIDDEN_KEYS.search(str(key)) is None,
                f"{name} contains a forbidden runtime configuration key",
            )
            _assert_safe_output(child, f"{name}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_safe_output(child, f"{name}[{index}]")
        return
    if isinstance(value, float):
        _require(math.isfinite(value), f"{name} is non-finite")
        return
    if isinstance(value, str):
        _require(
            "://" not in value
            and not value.startswith(("/", "\\\\"))
            and re.match(r"^[A-Za-z]:[\\/]", value) is None,
            f"{name} contains an endpoint or absolute path",
        )


def _source_binding(source_id: str, path: Path) -> dict[str, Any]:
    root = path.resolve()
    _require(root.exists() and not root.is_symlink(), f"{source_id} is missing")
    inventory: list[dict[str, Any]] = []
    source_kind: str
    if root.is_file():
        source_kind = "file"
        payload = root.read_bytes()
        inventory.append({
            "relative_path": root.name,
            "size_bytes": len(payload),
            "sha256": _sha256(payload),
        })
    else:
        _require(root.is_dir(), f"{source_id} is not a file or directory")
        source_kind = "directory"
        for child in sorted(root.rglob("*")):
            _require(not child.is_symlink(), f"{source_id} contains a symlink")
            _require(
                child.is_dir() or child.is_file(),
                f"{source_id} contains a special filesystem entry",
            )
            if child.is_file():
                payload = child.read_bytes()
                inventory.append({
                    "relative_path": child.relative_to(root).as_posix(),
                    "size_bytes": len(payload),
                    "sha256": _sha256(payload),
                })
    _require(bool(inventory), f"{source_id} contains no files")
    return {
        "source_id": source_id,
        "source_kind": source_kind,
        "source_sha256": _sha256(_canonical(inventory)),
    }


def _verified_status(result: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(result, Mapping), f"{name} verifier returned no mapping")
    status = result.get("status")
    _require(
        isinstance(status, str)
        and (status == "VERIFIED" or status.startswith("VERIFIED_")),
        f"{name} did not verify",
    )
    return result


def _all_or_none(values: Mapping[str, Path | None], name: str) -> bool:
    present = {key: value is not None for key, value in values.items()}
    _require(
        len(set(present.values())) == 1,
        f"{name} inputs must be supplied together",
    )
    return all(present.values())


def _gap_rows() -> list[dict[str, Any]]:
    return [
        {
            "gap_id": "real-multi-host-storage-hardware",
            "classification": "UPCLOUD_ONLY",
            "satisfied": False,
        },
        {
            "gap_id": "real-inter-node-network-paths",
            "classification": "UPCLOUD_ONLY",
            "satisfied": False,
        },
        {
            "gap_id": "real-cross-host-contention-and-queueing",
            "classification": "UPCLOUD_ONLY",
            "satisfied": False,
        },
        {
            "gap_id": "real-cloud-failure-behaviour",
            "classification": "UPCLOUD_ONLY",
            "satisfied": False,
        },
        {
            "gap_id": "real-cloud-monetary-cost-and-performance",
            "classification": "UPCLOUD_ONLY",
            "satisfied": False,
        },
    ]


def _readiness_report(
    *,
    audit_id: str,
    source_git_revision: str,
    operator_attests_clean_committed_source: bool,
    source_archive_path: Path | None,
    provisioning_catalog_dir: Path,
    artifact_binding_dir: Path,
    n4_package_dir: Path,
    logical_route_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
    task_plane_dir: Path,
    n3_package_dir: Path,
    w4_route_package_dir: Path,
    w4_index_package_dir: Path,
    w4_index_crosswalk_dir: Path,
    neutral_observation_dir: Path | None,
    neutral_semantic_matrix_run_dir: Path | None,
    semantic_execution_admission_dir: Path | None,
    smoke_gated_semantic_matrix_run_dir: Path | None,
    ten_smoke_dir: Path | None,
    n4_serve_gate_dir: Path | None,
    compose_overlay_dir: Path | None,
    service_bootstrap_dir: Path | None,
    deployment_binding_dir: Path | None,
    semantic_matrix_dir: Path | None,
    public_task_set_path: Path | None,
    semantic_artifact_binding_path: Path | None,
    n4_live_gate_sources: N4LiveServeGateSources | None,
    n1_oracle_package_dir: Path | None,
    n1_evidence_secret: bytes | None,
    bulk_provisioning_output_dir: Path | None,
    bulk_source_manifest: Path | None,
    bulk_live_receipt_bindings: Path | None,
    w4_component_receipt_dir: Path | None,
    w4_coordinator_run_dir: Path | None,
    w4_retrieval_contract_dir: Path | None,
    w4_retrieval_evaluation_dir: Path | None,
    flowmesh_matrix_plan_dir: Path | None,
    flowmesh_formal_profile_dir: Path | None,
    flowmesh_coordinator_plan_dir: Path | None,
    flowmesh_matrix_run_dir: Path | None,
    flowmesh_w4_plan_dir: Path | None,
    flowmesh_w4_run_dir: Path | None,
) -> dict[str, Any]:
    audit = _identifier(audit_id, "audit_id")
    revision = _git_revision(source_git_revision)
    _require(
        operator_attests_clean_committed_source is True,
        "the operator must attest that source_git_revision is committed "
        "and the source working tree is clean",
    )
    if source_archive_path is not None:
        _require(
            source_archive_path.is_file()
            and not source_archive_path.is_symlink(),
            "source archive must be one safe regular file",
        )
    _require(
        (n1_oracle_package_dir is None) == (n1_evidence_secret is None),
        "N1 oracle package and runtime evidence secret must be supplied together",
    )
    if n1_evidence_secret is not None:
        _require(
            isinstance(n1_evidence_secret, bytes)
            and 1 <= len(n1_evidence_secret) <= 8192,
            "N1 runtime evidence secret is invalid",
        )
    n4_result = _verified_status(
        verify_n4_derived_data_package(n4_package_dir), "N4 package"
    )
    binding_result = _verified_status(
        verify_full_flow_artifact_bindings(
            artifact_binding_dir,
            logical_route_dir,
            scenario_path,
            container_plan_dir,
            task_plane_dir,
            n3_package_dir,
            n4_package_dir,
        ),
        "artifact bindings",
    )
    catalog_result = _verified_status(
        verify_full_flow_provisioning_catalog(
            provisioning_catalog_dir,
            artifact_binding_dir=artifact_binding_dir,
            n4_package_dir=n4_package_dir,
        ),
        "provisioning catalog",
    )
    route_result = _verified_status(
        verify_full_flow_w4_candidate_routes(w4_route_package_dir),
        "W4 route package",
    )
    crosswalk_result = _verified_status(
        verify_full_flow_w4_index_artifact_crosswalk(
            w4_index_crosswalk_dir,
            route_package_dir=w4_route_package_dir,
            index_package_dir=w4_index_package_dir,
        ),
        "W4 index-artifact crosswalk",
    )
    _require(
        catalog_result.get("entry_count") == 72
        and binding_result.get("artifact_object_count") == 36
        and binding_result.get("artifact_representation_count") == 72
        and n4_result.get("object_count") == 36
        and n4_result.get("artifact_count") == 72,
        "required code artifacts are not the exact 36-by-2 derived set",
    )
    _require(
        route_result.get("trial_count") == 16,
        "W4 route coverage changed",
    )

    sources: list[dict[str, Any]] = []

    def bind(source_id: str, path: Path, group: str, status: str) -> None:
        row = _source_binding(source_id, path)
        row.update({"evidence_group": group, "verification_status": status})
        sources.append(row)

    for source_id, path, status in (
        ("logical-routes", logical_route_dir, "SOURCE_BOUND"),
        ("scenario", scenario_path, "SOURCE_BOUND"),
        ("container-plan", container_plan_dir, "SOURCE_BOUND"),
        ("task-plane", task_plane_dir, "SOURCE_BOUND"),
        ("n3-package", n3_package_dir, "SOURCE_BOUND"),
        ("n4-package", n4_package_dir, str(n4_result["status"])),
        (
            "artifact-bindings",
            artifact_binding_dir,
            str(binding_result["status"]),
        ),
        (
            "provisioning-catalog",
            provisioning_catalog_dir,
            str(catalog_result["status"]),
        ),
        ("w4-route-package", w4_route_package_dir, str(route_result["status"])),
        ("w4-index-package", w4_index_package_dir, "SOURCE_BOUND"),
        (
            "w4-index-artifact-crosswalk",
            w4_index_crosswalk_dir,
            str(crosswalk_result["status"]),
        ),
    ):
        bind(source_id, path, "required-code", status)
    if source_archive_path is not None:
        bind(
            "source-archive",
            source_archive_path,
            "implementation-revision",
            "SOURCE_BOUND",
        )

    semantic_inputs = {
        "neutral observation directory": neutral_observation_dir,
        "neutral semantic matrix run": neutral_semantic_matrix_run_dir,
        "semantic execution admission": semantic_execution_admission_dir,
        "smoke-gated semantic matrix run": (
            smoke_gated_semantic_matrix_run_dir
        ),
        "ten-smoke receipt directory": ten_smoke_dir,
        "N4 serve gate": n4_serve_gate_dir,
        "Compose overlay": compose_overlay_dir,
        "service bootstrap": service_bootstrap_dir,
        "deployment binding": deployment_binding_dir,
        "semantic matrix sources": semantic_matrix_dir,
        "public task set": public_task_set_path,
        "semantic artifact binding": semantic_artifact_binding_path,
        "N1 oracle package": n1_oracle_package_dir,
    }
    semantic_present = _all_or_none(
        semantic_inputs,
        "smoke-gated semantic evidence",
    )
    _require(
        n4_live_gate_sources is None or semantic_present,
        "N4 live-gate sources require the complete smoke-gated semantic group",
    )
    semantic_outer_result: Mapping[str, Any] | None = None
    observation_result: Mapping[str, Any] | None = None
    if semantic_present:
        _require(
            neutral_observation_dir is not None
            and neutral_semantic_matrix_run_dir is not None
            and semantic_execution_admission_dir is not None
            and smoke_gated_semantic_matrix_run_dir is not None
            and ten_smoke_dir is not None
            and n4_serve_gate_dir is not None
            and compose_overlay_dir is not None
            and service_bootstrap_dir is not None
            and deployment_binding_dir is not None
            and semantic_matrix_dir is not None
            and public_task_set_path is not None
            and semantic_artifact_binding_path is not None
            and n1_oracle_package_dir is not None
            and n1_evidence_secret is not None,
            "smoke-gated semantic evidence is incomplete",
        )
        expected_inner = (
            smoke_gated_semantic_matrix_run_dir
            / SEMANTIC_MATRIX_RUN_DIR_NAME
        ).resolve()
        _require(
            neutral_semantic_matrix_run_dir.resolve() == expected_inner
            and expected_inner.is_dir()
            and not expected_inner.is_symlink(),
            "neutral observations must bind the exact inner matrix-run of "
            "the smoke-gated semantic execution",
        )
        semantic_outer_result = _verified_status(
            verify_smoke_gated_full_flow_local_semantic_matrix_run(
                semantic_execution_admission_dir,
                ten_smoke_dir,
                n4_serve_gate_dir,
                compose_overlay_dir,
                service_bootstrap_dir,
                provisioning_catalog_dir,
                artifact_binding_dir,
                n4_package_dir,
                semantic_matrix_dir,
                deployment_binding_dir,
                logical_route_dir,
                scenario_path,
                container_plan_dir,
                public_task_set_path,
                semantic_artifact_binding_path,
                output_dir=smoke_gated_semantic_matrix_run_dir,
                n4_live_gate_sources=n4_live_gate_sources,
            ),
            "smoke-gated semantic matrix run",
        )
        _require(
            semantic_outer_result.get("planned_trial_count") == 64
            and semantic_outer_result.get("completed_trial_count") == 64
            and semantic_outer_result.get("neutral_evidence_count") == 64
            and semantic_outer_result.get(
                "full_matrix_runtime_gate_satisfied"
            )
            is True
            and semantic_outer_result.get("source_binding_checked") is True,
            "outer smoke-gated semantic matrix evidence is incomplete",
        )
        observation_result = _verified_status(
            verify_full_flow_observations(
                observation_dir=neutral_observation_dir,
                logical_route_plan_dir=logical_route_dir,
                scenario_path=scenario_path,
                container_plan_dir=container_plan_dir,
                semantic_execution_admission_dir=(
                    semantic_execution_admission_dir
                ),
                semantic_matrix_run_dir=neutral_semantic_matrix_run_dir,
                n1_oracle_package_dir=n1_oracle_package_dir,
                n1_evidence_secret=n1_evidence_secret,
            ),
            "neutral N1 observations",
        )
        _require(
            observation_result.get("observation_count") == 64
            and observation_result.get("legacy_full_flow_observation_count")
            == 0
            and observation_result.get(
                "generic_semantic_route_observation_count"
            )
            == 64
            and observation_result.get("all_score_authenticity_verified")
            is True
            and observation_result.get(
                "semantic_matrix_run_integrity_verified"
            )
            is True,
            "neutral observations are not 64 authentic generic observations "
            "from one verified semantic matrix run",
        )
        for source_id, path, status in (
            (
                "smoke-gated-semantic-matrix-run",
                smoke_gated_semantic_matrix_run_dir,
                str(semantic_outer_result["status"]),
            ),
            ("ten-smoke-receipt", ten_smoke_dir, "OUTER_VERIFIED"),
            ("n4-serve-gate", n4_serve_gate_dir, "OUTER_VERIFIED"),
            ("compose-overlay", compose_overlay_dir, "OUTER_VERIFIED"),
            ("service-bootstrap", service_bootstrap_dir, "OUTER_VERIFIED"),
            (
                "deployment-binding",
                deployment_binding_dir,
                "OUTER_VERIFIED",
            ),
            ("semantic-matrix", semantic_matrix_dir, "OUTER_VERIFIED"),
            ("public-task-set", public_task_set_path, "OUTER_VERIFIED"),
            (
                "semantic-artifact-binding",
                semantic_artifact_binding_path,
                "OUTER_VERIFIED",
            ),
            (
                "neutral-semantic-matrix-run",
                neutral_semantic_matrix_run_dir,
                "EXACT_OUTER_INNER_RUN",
            ),
            (
                "neutral-n1-observations",
                neutral_observation_dir,
                str(observation_result["status"]),
            ),
            (
                "semantic-execution-admission",
                semantic_execution_admission_dir,
                "OUTER_VERIFIED",
            ),
            (
                "n1-oracle-package",
                n1_oracle_package_dir,
                "SOURCE_BOUND_AND_AUTHENTICATED",
            ),
        ):
            bind(source_id, path, "local-semantic-live", status)
        for source_id, path in _live_gate_source_paths(
            n4_live_gate_sources
        ):
            bind(
                source_id,
                path,
                "local-semantic-live",
                "OUTER_LIVE_GATE_VERIFIED",
            )

    bulk_inputs = {
        "bulk output": bulk_provisioning_output_dir,
        "bulk source manifest": bulk_source_manifest,
        "bulk live receipt bindings": bulk_live_receipt_bindings,
    }
    bulk_present = _all_or_none(bulk_inputs, "bulk live evidence")
    if bulk_present:
        _require(
            bulk_provisioning_output_dir is not None
            and bulk_source_manifest is not None
            and bulk_live_receipt_bindings is not None,
            "bulk inputs are incomplete",
        )
        expected_bindings = (
            bulk_provisioning_output_dir
            / BULK_FINAL_DIRECTORY_NAME
            / LIVE_RECEIPT_BINDINGS_NAME
        ).resolve()
        _require(
            bulk_live_receipt_bindings.resolve() == expected_bindings
            and expected_bindings.is_file()
            and not expected_bindings.is_symlink(),
            "bulk live-receipt bindings are not the verified final descriptor",
        )
        bulk_result = _verified_status(
            verify_full_flow_bulk_live_provisioning(
                bulk_provisioning_output_dir,
                provisioning_catalog_dir=provisioning_catalog_dir,
                artifact_binding_dir=artifact_binding_dir,
                n4_package_dir=n4_package_dir,
                operator_source_manifest=bulk_source_manifest,
            ),
            "bulk live provisioning",
        )
        _require(
            bulk_result.get("object_count") == 36
            and bulk_result.get("completed_operation_count") == 72
            and bulk_result.get("required_derived_identity_count") == 72,
            "bulk live evidence is not a complete 36-by-2 run",
        )
        bind(
            "bulk-live-provisioning-output",
            bulk_provisioning_output_dir,
            "local-live",
            str(bulk_result["status"]),
        )
        bind(
            "bulk-operator-source-manifest",
            bulk_source_manifest,
            "local-live",
            "SOURCE_BOUND",
        )
        bind(
            "bulk-live-receipt-bindings",
            bulk_live_receipt_bindings,
            "local-live",
            "VERIFIED_BY_BULK_RECEIPT",
        )

    flowmesh_w4_inputs = {
        "FlowMesh W4 plan": flowmesh_w4_plan_dir,
        "FlowMesh W4 run": flowmesh_w4_run_dir,
    }
    flowmesh_w4_present = _all_or_none(
        flowmesh_w4_inputs,
        "FlowMesh W4 evidence",
    )

    component_inputs = {
        "W4 component receipt": w4_component_receipt_dir,
        "W4 coordinator run": w4_coordinator_run_dir,
    }
    component_present = _all_or_none(component_inputs, "W4 component evidence")
    component_live = False
    if component_present:
        _require(
            w4_component_receipt_dir is not None
            and w4_coordinator_run_dir is not None,
            "W4 component inputs are incomplete",
        )
        _require(
            flowmesh_w4_run_dir is not None,
            "W4 component evidence must come from a supplied FlowMesh W4 run",
        )
        expected_candidate_run = (
            flowmesh_w4_run_dir / CANDIDATE_RUN_DIR_NAME
        ).resolve()
        expected_component_receipt = (
            flowmesh_w4_run_dir / COMPONENT_RECEIPT_DIR_NAME
        ).resolve()
        _require(
            w4_coordinator_run_dir.resolve() == expected_candidate_run
            and w4_component_receipt_dir.resolve()
            == expected_component_receipt,
            "W4 local evidence must use the exact nested candidate-run and "
            "component-receipt of the FlowMesh W4 run",
        )
        component_result = _verified_status(
            verify_full_flow_w4_component_execution_receipt(
                w4_component_receipt_dir,
                coordinator_run_dir=w4_coordinator_run_dir,
                route_package_dir=w4_route_package_dir,
                crosswalk_dir=w4_index_crosswalk_dir,
                index_package_dir=w4_index_package_dir,
            ),
            "W4 component receipt",
        )
        component_live = (
            component_result.get("evidence_class")
            == "live-local-component-execution"
        )
        bind(
            "w4-component-receipt",
            w4_component_receipt_dir,
            "local-live",
            str(component_result["status"]),
        )
        bind(
            "w4-coordinator-run",
            w4_coordinator_run_dir,
            "local-live",
            "SOURCE_BOUND",
        )

    retrieval_inputs = {
        "W4 retrieval contract": w4_retrieval_contract_dir,
        "W4 retrieval evaluation": w4_retrieval_evaluation_dir,
    }
    retrieval_present = _all_or_none(
        retrieval_inputs, "W4 retrieval evaluation evidence"
    )
    retrieval_source_bound = False
    if retrieval_present:
        _require(
            w4_retrieval_contract_dir is not None
            and w4_retrieval_evaluation_dir is not None
            and w4_coordinator_run_dir is not None,
            "W4 retrieval evaluation requires its coordinator run",
        )
        _require(
            flowmesh_w4_run_dir is not None
            and w4_coordinator_run_dir.resolve()
            == (flowmesh_w4_run_dir / CANDIDATE_RUN_DIR_NAME).resolve(),
            "W4 N1 evaluation must use the exact nested candidate-run of "
            "the FlowMesh W4 run",
        )
        observations_path = (
            w4_coordinator_run_dir / W4_RETRIEVAL_OBSERVATIONS_NAME
        )
        _require(
            observations_path.is_file()
            and not observations_path.is_symlink(),
            "W4 coordinator run has no safe exact retrieval observations",
        )
        contract_result = _verified_status(
            verify_full_flow_w4_retrieval_contract(
                w4_retrieval_contract_dir
            ),
            "W4 retrieval contract",
        )
        evaluation_result = _verified_status(
            verify_full_flow_w4_retrieval_evaluation(
                w4_retrieval_evaluation_dir,
                contract_dir=w4_retrieval_contract_dir,
                observations_path=observations_path,
            ),
            "W4 retrieval evaluation",
        )
        retrieval_source_bound = (
            contract_result.get("w4_trial_count") == 16
            and contract_result.get("contract_id")
            == evaluation_result.get("contract_id")
            and isinstance(
                contract_result.get("candidate_object_count"), int
            )
            and contract_result.get("candidate_object_count") >= 2
            and contract_result.get("candidate_object_count")
            == evaluation_result.get("candidate_object_count")
            and contract_result.get("hidden_relevance_values_returned")
            is False
            and evaluation_result.get("status")
            == "VERIFIED_SOURCE_BOUND"
            and evaluation_result.get("trial_count") == 16
            and evaluation_result.get("source_bound_replay_performed")
            is True
            and evaluation_result.get("hidden_relevance_values_returned")
            is False
        )
        _require(
            retrieval_source_bound,
            "W4 retrieval evaluation is not a complete source-bound "
            "16-trial N1 scoring replay",
        )
        bind(
            "w4-retrieval-contract",
            w4_retrieval_contract_dir,
            "local-live",
            str(contract_result["status"]),
        )
        bind(
            "w4-retrieval-evaluation",
            w4_retrieval_evaluation_dir,
            "local-live",
            str(evaluation_result["status"]),
        )

    flowmesh_infrastructure_inputs = {
        "FlowMesh matrix plan": flowmesh_matrix_plan_dir,
        "FlowMesh formal profile": flowmesh_formal_profile_dir,
        "FlowMesh coordinator plan": flowmesh_coordinator_plan_dir,
        "FlowMesh matrix run": flowmesh_matrix_run_dir,
    }
    flowmesh_infrastructure_present = _all_or_none(
        flowmesh_infrastructure_inputs,
        "FlowMesh infrastructure evidence",
    )
    if flowmesh_infrastructure_present:
        _require(
            flowmesh_matrix_plan_dir is not None
            and flowmesh_formal_profile_dir is not None
            and flowmesh_coordinator_plan_dir is not None
            and flowmesh_matrix_run_dir is not None,
            "FlowMesh inputs are incomplete",
        )
        matrix_result = _verified_status(
            verify_flowmesh_container_matrix_plan(flowmesh_matrix_plan_dir),
            "FlowMesh matrix plan",
        )
        profile_result = _verified_status(
            verify_flowmesh_container_formal_execution_profile(
                flowmesh_formal_profile_dir
            ),
            "FlowMesh formal profile",
        )
        coordinator_result = _verified_status(
            verify_flowmesh_container_matrix_coordinator_dry_run(
                flowmesh_coordinator_plan_dir,
                matrix_plan_dir=flowmesh_matrix_plan_dir,
                formal_execution_profile_dir=flowmesh_formal_profile_dir,
            ),
            "FlowMesh coordinator plan",
        )
        matrix_run_result = _verified_status(
            verify_flowmesh_container_matrix_run(
                flowmesh_matrix_run_dir,
                matrix_plan_dir=flowmesh_matrix_plan_dir,
                formal_execution_profile_dir=flowmesh_formal_profile_dir,
                coordinator_plan_dir=flowmesh_coordinator_plan_dir,
            ),
            "FlowMesh matrix run",
        )
        dimensions = matrix_result.get("matrix_dimensions")
        _require(
            isinstance(dimensions, Mapping)
            and dimensions.get("trial_count") == 64
            and dimensions.get("workload_count") == 4
            and dimensions.get("design_count") == 8
            and dimensions.get("repetitions") == [0, 1]
            and matrix_result.get("operation_count") == 500
            and coordinator_result.get("trial_wrapper_count") == 64
            and coordinator_result.get("conditional_trial_wrapper_count")
            == 16
            and coordinator_result.get("source_binding_checked") is True
            and profile_result.get(
                "eligible_for_formal_infrastructure_execution"
            )
            is True
            and profile_result.get(
                "primary_trial_wrapper_max_concurrency"
            )
            == 1
            and profile_result.get("matrix_plan_sha256")
            == matrix_result.get("plan_sha256")
            == coordinator_result.get("matrix_plan_sha256")
            and coordinator_result.get("profile_sha256")
            == profile_result.get("profile_sha256")
            and matrix_run_result.get("completed_trial_count") == 64
            and matrix_run_result.get("source_binding_checked") is True
            and matrix_run_result.get("matrix_id")
            == matrix_result.get("matrix_id")
            and matrix_run_result.get("executed_operation_count", 0)
            + matrix_run_result.get("inactive_operation_count", 0)
            == 500
            and matrix_run_result.get("workflow_count") == 80
            and matrix_run_result.get("flowmesh_workflow_count") >= 80,
            "FlowMesh evidence is not the bound verified 64-trial execution",
        )
        bind(
            "flowmesh-matrix-plan",
            flowmesh_matrix_plan_dir,
            "flowmesh-execution",
            str(matrix_result["status"]),
        )
        bind(
            "flowmesh-formal-profile",
            flowmesh_formal_profile_dir,
            "flowmesh-execution",
            str(profile_result["status"]),
        )
        bind(
            "flowmesh-coordinator-plan",
            flowmesh_coordinator_plan_dir,
            "flowmesh-execution",
            str(coordinator_result["status"]),
        )
        bind(
            "flowmesh-matrix-run",
            flowmesh_matrix_run_dir,
            "flowmesh-execution",
            str(matrix_run_result["status"]),
        )

    if flowmesh_w4_present:
        _require(
            flowmesh_w4_plan_dir is not None
            and flowmesh_w4_run_dir is not None,
            "FlowMesh W4 inputs are incomplete",
        )
        w4_flowmesh_plan_result = _verified_status(
            verify_flowmesh_w4_candidate_matrix_plan(
                flowmesh_w4_plan_dir,
                route_package_dir=w4_route_package_dir,
            ),
            "FlowMesh W4 plan",
        )
        w4_flowmesh_run_result = _verified_status(
            verify_flowmesh_w4_candidate_matrix_run(
                flowmesh_w4_run_dir,
                plan_dir=flowmesh_w4_plan_dir,
                route_package_dir=w4_route_package_dir,
                crosswalk_dir=w4_index_crosswalk_dir,
                index_package_dir=w4_index_package_dir,
            ),
            "FlowMesh W4 run",
        )
        _require(
            w4_flowmesh_plan_result.get("trial_count") == 16
            and w4_flowmesh_plan_result.get("flowmesh_api_task_count") == 16
            and w4_flowmesh_plan_result.get("workflow_count") == 1
            and w4_flowmesh_plan_result.get("workflow_submitted") is False
            and w4_flowmesh_run_result.get("completed_trial_count") == 16
            and w4_flowmesh_run_result.get("flowmesh_api_task_count") == 16
            and w4_flowmesh_run_result.get("workflow_count") == 1
            and w4_flowmesh_run_result.get("source_binding_checked") is True
            and w4_flowmesh_run_result.get("component_evidence_class")
            == "live-local-component-execution"
            and w4_flowmesh_run_result.get(
                "ready_for_n1_hidden_relevance_evaluation"
            )
            is True
            and w4_flowmesh_run_result.get("real_cloud_performance_measured")
            is False
            and w4_flowmesh_run_result.get("run_id")
            == w4_flowmesh_plan_result.get("run_id")
            and w4_flowmesh_run_result.get("physical_plan_id")
            == w4_flowmesh_plan_result.get("physical_plan_id")
            and w4_flowmesh_run_result.get("plan_sha256")
            == w4_flowmesh_plan_result.get("plan_sha256"),
            "FlowMesh W4 evidence is not the bound verified 16-task "
            "live-component execution",
        )
        bind(
            "flowmesh-w4-plan",
            flowmesh_w4_plan_dir,
            "flowmesh-w4-execution",
            str(w4_flowmesh_plan_result["status"]),
        )
        bind(
            "flowmesh-w4-run",
            flowmesh_w4_run_dir,
            "flowmesh-w4-execution",
            str(w4_flowmesh_run_result["status"]),
        )

    source_ids = [row["source_id"] for row in sources]
    _require(
        len(source_ids) == len(set(source_ids)),
        "readiness source identity repeats",
    )
    local_complete = (
        semantic_present
        and bulk_present
        and component_present
        and component_live
        and retrieval_present
        and retrieval_source_bound
    )
    flowmesh_complete = (
        flowmesh_infrastructure_present and flowmesh_w4_present
    )
    report: dict[str, Any] = {
        "schema_version": READINESS_REPORT_SCHEMA_VERSION,
        "status": "FROZEN_PRE_UPCLOUD_READINESS",
        "audit_id": audit,
        "implementation_revision": {
            "source_git_revision": revision,
            "operator_declared_clean_committed_source": True,
            "source_archive_bound": source_archive_path is not None,
            "source_archive_file_sha256": (
                _sha256(source_archive_path.read_bytes())
                if source_archive_path is not None
                else None
            ),
        },
        "readiness": {
            "required_code": REQUIRED_CODE_READY,
            "local_live_evidence": (
                LOCAL_LIVE_EVIDENCE_PRESENT
                if local_complete
                else LOCAL_LIVE_EVIDENCE_MISSING
            ),
            "flowmesh_evidence": (
                FLOWMESH_EVIDENCE_PRESENT
                if flowmesh_complete
                else FLOWMESH_EVIDENCE_MISSING
            ),
            "upcloud": UPCLOUD_ONLY_GAPS_REMAIN,
        },
        "required_code_summary": {
            "derived_object_count": 36,
            "derived_identity_count": 72,
            "w4_route_trial_count": 16,
        },
        "local_live_evidence_components": {
            "smoke_gated_semantic_matrix_64_trial_execution": (
                "PRESENT" if semantic_present else "MISSING"
            ),
            "authentic_generic_neutral_observations": (
                "PRESENT" if observation_result is not None else "MISSING"
            ),
            "bulk_36_by_2": "PRESENT" if bulk_present else "MISSING",
            "w4_component_receipt": (
                "PRESENT" if component_present else "MISSING"
            ),
            "w4_component_is_live": component_live,
            "w4_retrieval_evaluation": (
                "PRESENT" if retrieval_present else "MISSING"
            ),
            "w4_retrieval_source_bound_n1_scoring": (
                retrieval_source_bound
            ),
        },
        "flowmesh_evidence_scope": (
            "verified-local-64-trial-infrastructure-and-16-task-w4-execution"
            if flowmesh_complete
            else (
                "verified-local-64-trial-infrastructure-only"
                if flowmesh_infrastructure_present
                else (
                    "verified-local-16-task-w4-execution-only"
                    if flowmesh_w4_present
                    else "not-supplied"
                )
            )
        ),
        "flowmesh_evidence_components": {
            "infrastructure_matrix_64_trial_execution": (
                "VERIFIED"
                if flowmesh_infrastructure_present
                else "MISSING"
            ),
            "w4_candidate_16_task_live_execution": (
                "VERIFIED" if flowmesh_w4_present else "MISSING"
            ),
        },
        "source_count": len(sources),
        "sources": sources,
        "source_bindings_sha256": _sha256(_canonical(sources)),
        "upcloud_only_gaps": _gap_rows(),
        "claim_boundary": {
            "offline_verification_only": True,
            "services_contacted": False,
            "flowmesh_workflow_submitted": False,
            "real_multi_host_execution_performed": False,
            "real_cloud_performance_measured": False,
            "real_cloud_monetary_cost_measured": False,
            "performance_readiness_claimed": False,
            "scientific_readiness_claimed": False,
        },
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    report["report_sha256"] = _document_sha256(report, "report_sha256")
    _assert_safe_output(report)
    return report


def _manifest(report: Mapping[str, Any]) -> dict[str, Any]:
    report_bytes = _json_bytes(report)
    document: dict[str, Any] = {
        "schema_version": READINESS_MANIFEST_SCHEMA_VERSION,
        "status": "FROZEN_PRE_UPCLOUD_READINESS_MANIFEST",
        "audit_id": report["audit_id"],
        "source_git_revision": report["implementation_revision"][
            "source_git_revision"
        ],
        "operator_declared_clean_committed_source": True,
        "source_archive_bound": report["implementation_revision"][
            "source_archive_bound"
        ],
        "source_archive_file_sha256": report["implementation_revision"][
            "source_archive_file_sha256"
        ],
        "report_file_sha256": _sha256(report_bytes),
        "source_bindings_sha256": report["source_bindings_sha256"],
        "source_count": report["source_count"],
        "readiness": report["readiness"],
        "services_contacted": False,
        "performance_readiness_claimed": False,
        "scientific_readiness_claimed": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    document["manifest_sha256"] = _document_sha256(
        document, "manifest_sha256"
    )
    _assert_safe_output(document)
    return document


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return "".join(
        f"{_sha256(documents[name])}  {name}\n" for name in sorted(documents)
    ).encode("ascii")


def _paths(**values: str | Path | None) -> dict[str, Path | None]:
    return {
        key: None if value is None else Path(value).resolve()
        for key, value in values.items()
    }


def _build_report(
    audit_id: str,
    paths: Mapping[str, Path | None],
    *,
    source_git_revision: str,
    operator_attests_clean_committed_source: bool,
    n1_evidence_secret: bytes | None,
    n4_live_gate_sources: N4LiveServeGateSources | None,
) -> dict[str, Any]:
    required = [
        "provisioning_catalog_dir",
        "artifact_binding_dir",
        "n4_package_dir",
        "logical_route_dir",
        "scenario_path",
        "container_plan_dir",
        "task_plane_dir",
        "n3_package_dir",
        "w4_route_package_dir",
        "w4_index_package_dir",
        "w4_index_crosswalk_dir",
    ]
    _require(
        all(paths[name] is not None for name in required),
        "required readiness source is missing",
    )
    return _readiness_report(
        audit_id=audit_id,
        source_git_revision=source_git_revision,
        operator_attests_clean_committed_source=(
            operator_attests_clean_committed_source
        ),
        n1_evidence_secret=n1_evidence_secret,
        n4_live_gate_sources=n4_live_gate_sources,
        **{key: value for key, value in paths.items()},
    )


def freeze_full_flow_pre_upcloud_readiness(
    *,
    audit_id: str,
    source_git_revision: str,
    operator_attests_clean_committed_source: bool,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    task_plane_dir: str | Path,
    n3_package_dir: str | Path,
    w4_route_package_dir: str | Path,
    w4_index_package_dir: str | Path,
    w4_index_crosswalk_dir: str | Path,
    output_dir: str | Path,
    source_archive_path: str | Path | None = None,
    neutral_observation_dir: str | Path | None = None,
    neutral_semantic_matrix_run_dir: str | Path | None = None,
    semantic_execution_admission_dir: str | Path | None = None,
    smoke_gated_semantic_matrix_run_dir: str | Path | None = None,
    ten_smoke_dir: str | Path | None = None,
    n4_serve_gate_dir: str | Path | None = None,
    compose_overlay_dir: str | Path | None = None,
    service_bootstrap_dir: str | Path | None = None,
    deployment_binding_dir: str | Path | None = None,
    semantic_matrix_dir: str | Path | None = None,
    public_task_set_path: str | Path | None = None,
    semantic_artifact_binding_path: str | Path | None = None,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
    n1_oracle_package_dir: str | Path | None = None,
    n1_evidence_secret: bytes | None = None,
    bulk_provisioning_output_dir: str | Path | None = None,
    bulk_source_manifest: str | Path | None = None,
    bulk_live_receipt_bindings: str | Path | None = None,
    w4_component_receipt_dir: str | Path | None = None,
    w4_coordinator_run_dir: str | Path | None = None,
    w4_retrieval_contract_dir: str | Path | None = None,
    w4_retrieval_evaluation_dir: str | Path | None = None,
    flowmesh_matrix_plan_dir: str | Path | None = None,
    flowmesh_formal_profile_dir: str | Path | None = None,
    flowmesh_coordinator_plan_dir: str | Path | None = None,
    flowmesh_matrix_run_dir: str | Path | None = None,
    flowmesh_w4_plan_dir: str | Path | None = None,
    flowmesh_w4_run_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Freeze one deterministic, non-mutating pre-UpCloud audit."""

    paths = _paths(
        source_archive_path=source_archive_path,
        provisioning_catalog_dir=provisioning_catalog_dir,
        artifact_binding_dir=artifact_binding_dir,
        n4_package_dir=n4_package_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        task_plane_dir=task_plane_dir,
        n3_package_dir=n3_package_dir,
        w4_route_package_dir=w4_route_package_dir,
        w4_index_package_dir=w4_index_package_dir,
        w4_index_crosswalk_dir=w4_index_crosswalk_dir,
        neutral_observation_dir=neutral_observation_dir,
        neutral_semantic_matrix_run_dir=neutral_semantic_matrix_run_dir,
        semantic_execution_admission_dir=semantic_execution_admission_dir,
        smoke_gated_semantic_matrix_run_dir=(
            smoke_gated_semantic_matrix_run_dir
        ),
        ten_smoke_dir=ten_smoke_dir,
        n4_serve_gate_dir=n4_serve_gate_dir,
        compose_overlay_dir=compose_overlay_dir,
        service_bootstrap_dir=service_bootstrap_dir,
        deployment_binding_dir=deployment_binding_dir,
        semantic_matrix_dir=semantic_matrix_dir,
        public_task_set_path=public_task_set_path,
        semantic_artifact_binding_path=semantic_artifact_binding_path,
        n1_oracle_package_dir=n1_oracle_package_dir,
        bulk_provisioning_output_dir=bulk_provisioning_output_dir,
        bulk_source_manifest=bulk_source_manifest,
        bulk_live_receipt_bindings=bulk_live_receipt_bindings,
        w4_component_receipt_dir=w4_component_receipt_dir,
        w4_coordinator_run_dir=w4_coordinator_run_dir,
        w4_retrieval_contract_dir=w4_retrieval_contract_dir,
        w4_retrieval_evaluation_dir=w4_retrieval_evaluation_dir,
        flowmesh_matrix_plan_dir=flowmesh_matrix_plan_dir,
        flowmesh_formal_profile_dir=flowmesh_formal_profile_dir,
        flowmesh_coordinator_plan_dir=flowmesh_coordinator_plan_dir,
        flowmesh_matrix_run_dir=flowmesh_matrix_run_dir,
        flowmesh_w4_plan_dir=flowmesh_w4_plan_dir,
        flowmesh_w4_run_dir=flowmesh_w4_run_dir,
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), "readiness output directory already exists")
    all_source_paths = [
        path for path in paths.values() if path is not None
    ] + [path for _, path in _live_gate_source_paths(n4_live_gate_sources)]
    for path in all_source_paths:
        _require(
            target != path
            and not target.is_relative_to(path)
            and not path.is_relative_to(target),
            "readiness output overlaps a source",
        )
    report = _build_report(
        audit_id,
        paths,
        source_git_revision=source_git_revision,
        operator_attests_clean_committed_source=(
            operator_attests_clean_committed_source
        ),
        n1_evidence_secret=n1_evidence_secret,
        n4_live_gate_sources=n4_live_gate_sources,
    )
    manifest = _manifest(report)
    documents = {
        REPORT_NAME: _json_bytes(report),
        MANIFEST_NAME: _json_bytes(manifest),
    }
    documents[CHECKSUMS_NAME] = _checksums(documents)
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = target.parent / f".{target.name}.{os.getpid()}.tmp"
    _require(not stage.exists(), "stale readiness staging directory exists")
    try:
        stage.mkdir()
        for name, payload in documents.items():
            (stage / name).write_bytes(payload)
        os.replace(stage, target)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    verified = verify_full_flow_pre_upcloud_readiness(
        output_dir=target,
        source_git_revision=source_git_revision,
        operator_attests_clean_committed_source=(
            operator_attests_clean_committed_source
        ),
        n1_evidence_secret=n1_evidence_secret,
        n4_live_gate_sources=n4_live_gate_sources,
        **{
            key: value
            for key, value in paths.items()
            if value is not None
        },
    )
    return {**verified, "status": "FROZEN", "output_dir": str(target)}


def verify_full_flow_pre_upcloud_readiness(
    *,
    output_dir: str | Path,
    source_git_revision: str,
    operator_attests_clean_committed_source: bool,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    task_plane_dir: str | Path,
    n3_package_dir: str | Path,
    w4_route_package_dir: str | Path,
    w4_index_package_dir: str | Path,
    w4_index_crosswalk_dir: str | Path,
    source_archive_path: str | Path | None = None,
    neutral_observation_dir: str | Path | None = None,
    neutral_semantic_matrix_run_dir: str | Path | None = None,
    semantic_execution_admission_dir: str | Path | None = None,
    smoke_gated_semantic_matrix_run_dir: str | Path | None = None,
    ten_smoke_dir: str | Path | None = None,
    n4_serve_gate_dir: str | Path | None = None,
    compose_overlay_dir: str | Path | None = None,
    service_bootstrap_dir: str | Path | None = None,
    deployment_binding_dir: str | Path | None = None,
    semantic_matrix_dir: str | Path | None = None,
    public_task_set_path: str | Path | None = None,
    semantic_artifact_binding_path: str | Path | None = None,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
    n1_oracle_package_dir: str | Path | None = None,
    n1_evidence_secret: bytes | None = None,
    bulk_provisioning_output_dir: str | Path | None = None,
    bulk_source_manifest: str | Path | None = None,
    bulk_live_receipt_bindings: str | Path | None = None,
    w4_component_receipt_dir: str | Path | None = None,
    w4_coordinator_run_dir: str | Path | None = None,
    w4_retrieval_contract_dir: str | Path | None = None,
    w4_retrieval_evaluation_dir: str | Path | None = None,
    flowmesh_matrix_plan_dir: str | Path | None = None,
    flowmesh_formal_profile_dir: str | Path | None = None,
    flowmesh_coordinator_plan_dir: str | Path | None = None,
    flowmesh_matrix_run_dir: str | Path | None = None,
    flowmesh_w4_plan_dir: str | Path | None = None,
    flowmesh_w4_run_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Re-run every offline verifier and reproduce the frozen audit."""

    root = Path(output_dir).resolve()
    _require(root.is_dir() and not root.is_symlink(), "readiness output missing")
    entries = list(root.iterdir())
    _require(
        {path.name for path in entries} == _OUTPUT_FILES
        and all(path.is_file() and not path.is_symlink() for path in entries),
        "readiness output file set changed",
    )
    report_payload = (root / REPORT_NAME).read_bytes()
    manifest_payload = (root / MANIFEST_NAME).read_bytes()
    report = _strict_json(report_payload, "readiness report")
    manifest = _strict_json(manifest_payload, "readiness manifest")
    _require(
        isinstance(report, dict)
        and report_payload == _json_bytes(report)
        and isinstance(manifest, dict)
        and manifest_payload == _json_bytes(manifest),
        "readiness documents are not canonical JSON",
    )
    documents = {REPORT_NAME: report_payload, MANIFEST_NAME: manifest_payload}
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(documents),
        "readiness checksums changed",
    )
    _require(
        report.get("schema_version") == READINESS_REPORT_SCHEMA_VERSION
        and report.get("status") == "FROZEN_PRE_UPCLOUD_READINESS"
        and _DIGEST.fullmatch(str(report.get("report_sha256"))) is not None
        and report["report_sha256"]
        == _document_sha256(report, "report_sha256"),
        "readiness report schema, status, or digest changed",
    )
    _require(
        manifest.get("schema_version") == READINESS_MANIFEST_SCHEMA_VERSION
        and manifest.get("status")
        == "FROZEN_PRE_UPCLOUD_READINESS_MANIFEST"
        and _DIGEST.fullmatch(str(manifest.get("manifest_sha256"))) is not None
        and manifest["manifest_sha256"]
        == _document_sha256(manifest, "manifest_sha256")
        and manifest.get("report_file_sha256") == _sha256(report_payload),
        "readiness manifest schema, binding, or digest changed",
    )
    _assert_safe_output(report)
    _assert_safe_output(manifest)
    paths = _paths(
        source_archive_path=source_archive_path,
        provisioning_catalog_dir=provisioning_catalog_dir,
        artifact_binding_dir=artifact_binding_dir,
        n4_package_dir=n4_package_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        task_plane_dir=task_plane_dir,
        n3_package_dir=n3_package_dir,
        w4_route_package_dir=w4_route_package_dir,
        w4_index_package_dir=w4_index_package_dir,
        w4_index_crosswalk_dir=w4_index_crosswalk_dir,
        neutral_observation_dir=neutral_observation_dir,
        neutral_semantic_matrix_run_dir=neutral_semantic_matrix_run_dir,
        semantic_execution_admission_dir=semantic_execution_admission_dir,
        smoke_gated_semantic_matrix_run_dir=(
            smoke_gated_semantic_matrix_run_dir
        ),
        ten_smoke_dir=ten_smoke_dir,
        n4_serve_gate_dir=n4_serve_gate_dir,
        compose_overlay_dir=compose_overlay_dir,
        service_bootstrap_dir=service_bootstrap_dir,
        deployment_binding_dir=deployment_binding_dir,
        semantic_matrix_dir=semantic_matrix_dir,
        public_task_set_path=public_task_set_path,
        semantic_artifact_binding_path=semantic_artifact_binding_path,
        n1_oracle_package_dir=n1_oracle_package_dir,
        bulk_provisioning_output_dir=bulk_provisioning_output_dir,
        bulk_source_manifest=bulk_source_manifest,
        bulk_live_receipt_bindings=bulk_live_receipt_bindings,
        w4_component_receipt_dir=w4_component_receipt_dir,
        w4_coordinator_run_dir=w4_coordinator_run_dir,
        w4_retrieval_contract_dir=w4_retrieval_contract_dir,
        w4_retrieval_evaluation_dir=w4_retrieval_evaluation_dir,
        flowmesh_matrix_plan_dir=flowmesh_matrix_plan_dir,
        flowmesh_formal_profile_dir=flowmesh_formal_profile_dir,
        flowmesh_coordinator_plan_dir=flowmesh_coordinator_plan_dir,
        flowmesh_matrix_run_dir=flowmesh_matrix_run_dir,
        flowmesh_w4_plan_dir=flowmesh_w4_plan_dir,
        flowmesh_w4_run_dir=flowmesh_w4_run_dir,
    )
    expected_report = _build_report(
        str(report.get("audit_id")),
        paths,
        source_git_revision=source_git_revision,
        operator_attests_clean_committed_source=(
            operator_attests_clean_committed_source
        ),
        n1_evidence_secret=n1_evidence_secret,
        n4_live_gate_sources=n4_live_gate_sources,
    )
    expected_manifest = _manifest(expected_report)
    _require(
        report_payload == _json_bytes(expected_report)
        and manifest_payload == _json_bytes(expected_manifest),
        "readiness output differs from its current verified sources",
    )
    readiness = expected_report["readiness"]
    return {
        "status": "VERIFIED",
        "audit_id": expected_report["audit_id"],
        "required_code": readiness["required_code"],
        "local_live_evidence": readiness["local_live_evidence"],
        "flowmesh_evidence": readiness["flowmesh_evidence"],
        "upcloud": readiness["upcloud"],
        "source_count": expected_report["source_count"],
        "services_contacted": False,
        "performance_readiness_claimed": False,
        "scientific_readiness_claimed": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "FLOWMESH_EVIDENCE_MISSING",
    "FLOWMESH_EVIDENCE_PRESENT",
    "FullFlowPreUpcloudReadinessError",
    "LOCAL_LIVE_EVIDENCE_MISSING",
    "LOCAL_LIVE_EVIDENCE_PRESENT",
    "MANIFEST_NAME",
    "READINESS_MANIFEST_SCHEMA_VERSION",
    "READINESS_REPORT_SCHEMA_VERSION",
    "REPORT_NAME",
    "REQUIRED_CODE_READY",
    "UPCLOUD_ONLY_GAPS_REMAIN",
    "freeze_full_flow_pre_upcloud_readiness",
    "verify_full_flow_pre_upcloud_readiness",
]
