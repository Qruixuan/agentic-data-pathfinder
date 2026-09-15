"""Portable operator template for full-flow deployment bindings.

The logical route plan defines what each N1--N8 service contract must do.
This module turns those verified requirements into an endpoint-free template.
Operators may then bind the same template either to a single-host Compose
deployment or to private multi-host infrastructure without changing the
logical actions, representations, or state requirements.

Template placeholders are deliberately invalid deployment-source values.  A
template therefore cannot accidentally pass the final binding validator.
Completed sources are checked here and then passed through the production
``full_flow_deployment`` validator in a temporary directory.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .full_flow_deployment import (
    DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2,
    FullFlowDeploymentError,
    build_full_flow_deployment_binding,
    full_flow_w4_runtime_service_binding_requirements,
)
from .full_flow_logical_routes import (
    SERVICE_CATALOG_NAME,
    STAGES_NAME,
    verify_full_flow_logical_routes,
)


DEPLOYMENT_SOURCE_TEMPLATE_SCHEMA_VERSION = (
    "pathfinder.full-flow-deployment-source-template/v1alpha2"
)
DEPLOYMENT_REQUIREMENTS_SCHEMA_VERSION = (
    "pathfinder.full-flow-deployment-requirements/v1alpha2"
)
SOURCE_TEMPLATE_NAME = "full-flow-deployment-source.template.json"
REQUIREMENTS_NAME = "full-flow-deployment-requirements.json"
CHECKSUMS_NAME = "SHA256SUMS"

_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")
_PLACEHOLDER = re.compile(r"\$\{[A-Z][A-Z0-9_]*\}")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
_PERSISTENT_STATE = {
    "immutable-hidden-oracle",
    "durable-trial-identity",
    "immutable-content-addressed-artifacts",
    "frozen-index-snapshot",
    "idempotent-content-addressed-output",
    "persistent-with-explicit-cache-scope",
}
_SERVICE_STATIC_FIELDS = (
    "service_contract_id",
    "logical_node_ids",
    "actions",
    "representation_ids",
    "persistent_state",
)
_RUNTIME_SERVICE_STATIC_FIELDS = (
    "runtime_service_contract_id",
    "parent_service_contract_id",
    "logical_node_id",
    "adapter_id",
    "credential_env_names",
    "persistent_state",
    "health_route",
    "health_schema_version",
)


class FullFlowDeploymentTemplateError(ValueError):
    """Raised when a template or completed source is not safely bound."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowDeploymentTemplateError(message)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowDeploymentTemplateError(
            "deployment template contains non-canonical JSON"
        ) from exc


def _json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowDeploymentTemplateError(
            "deployment template contains invalid JSON"
        ) from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _parse_json(payload: bytes, name: str) -> dict[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(
                FullFlowDeploymentTemplateError(f"{name} contains {item}")
            ),
        )
    except FullFlowDeploymentTemplateError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowDeploymentTemplateError(f"cannot parse {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _read_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is not a regular file")
    try:
        return _parse_json(path.read_bytes(), name)
    except OSError as exc:
        raise FullFlowDeploymentTemplateError(f"cannot read {name}") from exc


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is not a regular file")
    try:
        lines = path.read_bytes().splitlines()
    except OSError as exc:
        raise FullFlowDeploymentTemplateError(f"cannot read {name}") from exc
    _require(bool(lines), f"{name} cannot be empty")
    return [
        _parse_json(line, f"{name} line {index}")
        for index, line in enumerate(lines, start=1)
    ]


def _sorted_strings(value: Any, name: str) -> list[str]:
    _require(isinstance(value, list), f"{name} must be an array")
    _require(
        all(isinstance(item, str) and item for item in value),
        f"{name} contains a non-string value",
    )
    normalized = sorted(value)
    _require(
        len(normalized) == len(set(normalized)),
        f"{name} must be unique",
    )
    return normalized


def _assert_endpoint_and_credential_free(value: Any) -> None:
    def visit(item: Any) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                lowered = str(key).casefold()
                _require(
                    lowered not in {
                        "api_key",
                        "authorization",
                        "bearer_token",
                        "password",
                        "secret",
                    },
                    "template contains a credential-value field",
                )
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)
        elif isinstance(item, str):
            lowered = item.casefold()
            _require(
                "://" not in item
                and (
                    item == "/healthz"
                    or not item.startswith(("/", "~", "\\\\"))
                )
                and _WINDOWS_ABSOLUTE.match(item) is None
                and "bearer " not in lowered,
                "template contains a concrete endpoint, path, or credential",
            )
        elif isinstance(item, float):
            _require(math.isfinite(item), "template contains a non-finite number")

    visit(value)


def _placeholder(label: str) -> str:
    normalized = re.sub(r"[^A-Z0-9]+", "_", label.upper()).strip("_")
    return "${" + normalized + "}"


def _contains_placeholder(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(_contains_placeholder(child) for child in value.values())
    if isinstance(value, list):
        return any(_contains_placeholder(child) for child in value)
    return isinstance(value, str) and _PLACEHOLDER.search(value) is not None


def _placeholder_count(value: Any) -> int:
    if isinstance(value, Mapping):
        return sum(_placeholder_count(child) for child in value.values())
    if isinstance(value, list):
        return sum(_placeholder_count(child) for child in value)
    return len(_PLACEHOLDER.findall(value)) if isinstance(value, str) else 0


def _logical_inputs(
    logical_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    catalog = _read_json(
        logical_root / SERVICE_CATALOG_NAME,
        "logical service catalog",
    )
    raw_contracts = catalog.get("service_contracts")
    _require(isinstance(raw_contracts, list), "service contract catalog is invalid")
    contracts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_contracts:
        _require(isinstance(raw, dict), "service contract must be an object")
        contract_id = _identifier(
            raw.get("service_contract_id"),
            "service_contract_id",
        )
        _require(contract_id not in seen, "service contract ID repeats")
        seen.add(contract_id)
        contracts.append(raw)
    _require(
        [row["service_contract_id"] for row in contracts] == sorted(seen),
        "service contract catalog is not canonical",
    )
    stages = _read_jsonl(
        logical_root / STAGES_NAME,
        "logical route stages",
    )
    return catalog, stages


def _required_representations(
    contracts: Sequence[Mapping[str, Any]],
    stages: Sequence[Mapping[str, Any]],
) -> dict[str, list[str]]:
    required = {str(row["service_contract_id"]): set() for row in contracts}
    inference_inputs: set[str] = set()
    for stage in stages:
        contract_id = stage.get("service_contract_id")
        representation = stage.get("representation_id")
        if (
            isinstance(contract_id, str)
            and contract_id in required
            and isinstance(representation, str)
        ):
            required[contract_id].add(representation)
        if (
            stage.get("phase") == "execution"
            and representation in {
                "raw_video",
                "sampled_frame_bundle",
                "multimodal_digest",
            }
        ):
            inference_inputs.add(str(representation))
    if "N6.semantic-inference" in required:
        required["N6.semantic-inference"].update(inference_inputs)
    return {
        contract_id: sorted(values)
        for contract_id, values in required.items()
    }


def _template_documents(
    *,
    template_id: str,
    logical_report: Mapping[str, Any],
    catalog: Mapping[str, Any],
    stages: Sequence[Mapping[str, Any]],
) -> dict[str, bytes]:
    template_id = _identifier(template_id, "template_id")
    raw_contracts = catalog["service_contracts"]
    _require(isinstance(raw_contracts, list), "service contract catalog is invalid")
    contracts = [dict(row) for row in raw_contracts]
    representations = _required_representations(contracts, stages)
    requirements: list[dict[str, Any]] = []
    bindings: list[dict[str, Any]] = []
    for contract in contracts:
        contract_id = contract["service_contract_id"]
        role = contract.get("role")
        nodes = _sorted_strings(
            contract.get("logical_node_ids"),
            f"{contract_id}.logical_node_ids",
        )
        actions = _sorted_strings(
            contract.get("actions"),
            f"{contract_id}.actions",
        )
        protocols = _sorted_strings(
            contract.get("protocol_contracts"),
            f"{contract_id}.protocol_contracts",
        )
        state_semantics = _identifier(
            contract.get("state_semantics"),
            f"{contract_id}.state_semantics",
        )
        is_network = role == "logical-byte-transfer"
        persistent = state_semantics in _PERSISTENT_STATE
        required_representations = representations[contract_id]
        requirements.append({
            "service_contract_id": contract_id,
            "role": _identifier(role, f"{contract_id}.role"),
            "logical_node_ids": nodes,
            "required_actions": actions,
            "required_representation_ids": required_representations,
            "protocol_contracts": protocols,
            "state_semantics": state_semantics,
            "persistent_state_required": persistent,
            "invocation_scope": _identifier(
                contract.get("invocation_scope"),
                f"{contract_id}.invocation_scope",
            ),
            "deployment_binding_kind": _identifier(
                contract.get("deployment_binding_kind"),
                f"{contract_id}.deployment_binding_kind",
            ),
            "service_endpoint_required": not is_network,
            "credential_values_allowed": False,
        })
        bindings.append({
            "service_contract_id": contract_id,
            "adapter_id": _placeholder(f"adapter_id_for_{contract_id}"),
            "logical_node_ids": nodes,
            "actions": actions,
            "representation_ids": required_representations,
            "base_url": (
                None
                if is_network
                else _placeholder(f"base_url_for_{contract_id}")
            ),
            "credential_env_names": (
                []
                if is_network
                else [_placeholder(
                    f"credential_env_name_for_{contract_id}_or_remove"
                )]
            ),
            "persistent_state": persistent,
        })
    runtime_requirements = (
        full_flow_w4_runtime_service_binding_requirements()
    )
    runtime_bindings = [
        {
            **row,
            "base_url": _placeholder(
                "base_url_for_" + row["runtime_service_contract_id"]
            ),
        }
        for row in runtime_requirements
    ]
    source = {
        "schema_version": DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2,
        "deployment_id": _placeholder("deployment_id"),
        "backend": _placeholder("deployment_backend"),
        "service_bindings": bindings,
        "runtime_service_bindings": runtime_bindings,
        "network_binding": {
            "adapter_id": _placeholder("network_adapter_id"),
            "mode": _placeholder("network_mode"),
            "measurement_class": _placeholder(
                "network_measurement_class"
            ),
            "parameters_fitted": False,
        },
        "trusted_private_http_hosts": [
            _placeholder("trusted_private_http_host_or_remove")
        ],
        "credentials_recorded": False,
    }
    source_bytes = _json_bytes(source)
    requirements_document: dict[str, Any] = {
        "schema_version": DEPLOYMENT_REQUIREMENTS_SCHEMA_VERSION,
        "source_template_schema_version": (
            DEPLOYMENT_SOURCE_TEMPLATE_SCHEMA_VERSION
        ),
        "status": "FROZEN_OPERATOR_EDITABLE_DEPLOYMENT_TEMPLATE",
        "template_id": template_id,
        "logical_plan_sha256": logical_report["plan_sha256"],
        "logical_source_binding_sha256": logical_report[
            "source_binding_sha256"
        ],
        "scenario_id": logical_report["scenario_id"],
        "service_contract_count": len(requirements),
        "service_contracts": requirements,
        "runtime_service_binding_count": len(runtime_requirements),
        "runtime_service_bindings": [
            {
                **row,
                "service_endpoint_required": True,
                "credential_values_allowed": False,
                "exact_health_identity_required": True,
            }
            for row in runtime_requirements
        ],
        "source_template_file": SOURCE_TEMPLATE_NAME,
        "source_template_file_sha256": _sha256(source_bytes),
        "supported_deployment_backends": [
            "multi-host-private-network",
            "single-host-compose",
        ],
        "supported_network_modes": [
            "application-shaped-single-host",
            "kernel-shaped-private-network",
            "physical-private-network",
        ],
        "operator_steps": [
            "Copy the source template to a new deployment source file.",
            "Replace every ${...} marker; remove optional credential or "
            "trusted-host entries when they are not used.",
            "Credential entries must name environment variables only; never "
            "put credential values in the source.",
            "Keep contract IDs, nodes, actions, representations, and "
            "persistent-state flags unchanged.",
            "Bind the N7 and N8 W4 coordinator rows to dedicated service "
            "origins; their parent, node, adapter, health, persistence, and "
            "credential-environment-name contracts are immutable.",
            "Validate the completed source before freezing a final binding.",
        ],
        "operator_may_change_only_binding_values": True,
        "logical_contracts_shared_across_backends": True,
        "contains_unresolved_placeholders": True,
        "final_binding_ready": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "services_started": False,
        "cloud_calls_made": False,
        "eligible_for_scientific_claims": False,
    }
    requirements_document["requirements_sha256"] = _sha256(
        _canonical_bytes(requirements_document)
    )
    _assert_endpoint_and_credential_free([source, requirements_document])
    return {
        SOURCE_TEMPLATE_NAME: source_bytes,
        REQUIREMENTS_NAME: _json_bytes(requirements_document),
    }


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _verify_files(root: Path) -> None:
    _require(root.is_dir(), "deployment template directory does not exist")
    entries = list(root.iterdir())
    expected = {SOURCE_TEMPLATE_NAME, REQUIREMENTS_NAME, CHECKSUMS_NAME}
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "deployment template must contain regular files only",
    )
    _require(
        {path.name for path in entries} == expected,
        "deployment template file set changed",
    )
    try:
        checksum_bytes = (root / CHECKSUMS_NAME).read_bytes()
    except OSError as exc:
        raise FullFlowDeploymentTemplateError(
            "cannot read deployment template checksums"
        ) from exc
    expected_documents = {
        name: (root / name).read_bytes()
        for name in expected - {CHECKSUMS_NAME}
    }
    _require(
        checksum_bytes == _checksums(expected_documents),
        "deployment template checksum failed",
    )


def generate_full_flow_deployment_source_template(
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    *,
    template_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze one endpoint-free template for either supported backend."""

    logical_root = Path(logical_plan_dir).resolve()
    report = verify_full_flow_logical_routes(
        logical_root,
        scenario_path,
        container_plan_dir,
    )
    catalog, stages = _logical_inputs(logical_root)
    documents = _template_documents(
        template_id=template_id,
        logical_report=report,
        catalog=catalog,
        stages=stages,
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), "deployment template output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(prefix=".deployment-template-", dir=target.parent)
    )
    staging = staging_root / "template"
    try:
        staging.mkdir()
        complete = dict(documents)
        complete[CHECKSUMS_NAME] = _checksums(documents)
        for name, payload in complete.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        verify_full_flow_deployment_source_template(
            staging,
            logical_plan_dir=logical_root,
            scenario_path=scenario_path,
            container_plan_dir=container_plan_dir,
        )
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
    verified = verify_full_flow_deployment_source_template(
        target,
        logical_plan_dir=logical_root,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    return {**verified, "output_dir": str(target)}


def verify_full_flow_deployment_source_template(
    template_dir: str | Path,
    *,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    """Verify checksums and deterministic derivation from the logical plan."""

    root = Path(template_dir).resolve()
    _verify_files(root)
    requirements = _read_json(
        root / REQUIREMENTS_NAME,
        "deployment requirements",
    )
    _require(
        requirements.get("schema_version")
        == DEPLOYMENT_REQUIREMENTS_SCHEMA_VERSION
        and requirements.get("source_template_schema_version")
        == DEPLOYMENT_SOURCE_TEMPLATE_SCHEMA_VERSION
        and requirements.get("status")
        == "FROZEN_OPERATOR_EDITABLE_DEPLOYMENT_TEMPLATE",
        "deployment requirements schema or status changed",
    )
    unsigned = dict(requirements)
    recorded_digest = unsigned.pop("requirements_sha256", None)
    _require(
        isinstance(recorded_digest, str)
        and _SHA256.fullmatch(recorded_digest) is not None
        and recorded_digest == _sha256(_canonical_bytes(unsigned)),
        "deployment requirements digest failed",
    )
    report = verify_full_flow_logical_routes(
        logical_plan_dir,
        scenario_path,
        container_plan_dir,
    )
    catalog, stages = _logical_inputs(Path(logical_plan_dir).resolve())
    expected = _template_documents(
        template_id=requirements.get("template_id"),
        logical_report=report,
        catalog=catalog,
        stages=stages,
    )
    _require(
        all(
            (root / name).read_bytes() == payload
            for name, payload in expected.items()
        ),
        "deployment template does not match its verified logical plan",
    )
    source = _read_json(root / SOURCE_TEMPLATE_NAME, "deployment source template")
    placeholder_count = _placeholder_count(source)
    _require(placeholder_count > 0, "deployment source template has no placeholders")
    return {
        "status": "VERIFIED_OPERATOR_EDITABLE_TEMPLATE",
        "template_id": requirements["template_id"],
        "logical_plan_sha256": report["plan_sha256"],
        "service_contract_count": requirements["service_contract_count"],
        "runtime_service_binding_count": requirements[
            "runtime_service_binding_count"
        ],
        "w4_runtime_bindings_required": True,
        "unresolved_placeholder_count": placeholder_count,
        "final_binding_ready": False,
        "single_host_compose_supported": True,
        "multi_host_private_network_supported": True,
        "endpoint_values_included": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "services_started": False,
        "cloud_calls_made": False,
        "eligible_for_scientific_claims": False,
    }


def _candidate_static_contracts(source: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = source.get("service_bindings")
    _require(isinstance(rows, list), "completed source service_bindings is invalid")
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        _require(isinstance(row, dict), "completed service binding is invalid")
        contract_id = row.get("service_contract_id")
        _require(
            isinstance(contract_id, str) and contract_id not in result,
            "completed source has an invalid or duplicate service contract",
        )
        result[contract_id] = row
    return result


def _candidate_runtime_contracts(
    source: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    rows = source.get("runtime_service_bindings")
    _require(
        isinstance(rows, list),
        "completed source runtime_service_bindings is invalid",
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        _require(isinstance(row, dict), "completed runtime binding is invalid")
        runtime_id = row.get("runtime_service_contract_id")
        _require(
            isinstance(runtime_id, str) and runtime_id not in result,
            "completed source has an invalid or duplicate runtime service",
        )
        result[runtime_id] = row
    return result


def validate_completed_full_flow_deployment_source(
    deployment_source: str | Path,
    *,
    template_dir: str | Path,
    logical_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    """Validate complete coverage and production binding compatibility.

    No service is contacted and no final binding is retained.  The production
    binding builder runs only in a temporary directory so this check exercises
    exactly the same capability and endpoint rules as real binding creation.
    """

    template_report = verify_full_flow_deployment_source_template(
        template_dir,
        logical_plan_dir=logical_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    template_root = Path(template_dir).resolve()
    template = _read_json(
        template_root / SOURCE_TEMPLATE_NAME,
        "deployment source template",
    )
    source_path = Path(deployment_source).resolve()
    source = _read_json(source_path, "completed deployment source")
    _require(
        not _contains_placeholder(source),
        "completed deployment source contains unresolved placeholders",
    )
    template_rows = _candidate_static_contracts(template)
    source_rows = _candidate_static_contracts(source)
    _require(
        set(source_rows) == set(template_rows),
        "completed source does not cover every logical service contract",
    )
    for contract_id, expected in template_rows.items():
        observed = source_rows[contract_id]
        for field in _SERVICE_STATIC_FIELDS:
            _require(
                observed.get(field) == expected.get(field),
                f"completed source changed {contract_id}.{field}",
            )
    template_runtime_rows = _candidate_runtime_contracts(template)
    source_runtime_rows = _candidate_runtime_contracts(source)
    _require(
        set(source_runtime_rows) == set(template_runtime_rows),
        "completed source does not bind both W4 coordinator runtime services",
    )
    for runtime_id, expected in template_runtime_rows.items():
        observed = source_runtime_rows[runtime_id]
        for field in _RUNTIME_SERVICE_STATIC_FIELDS:
            _require(
                observed.get(field) == expected.get(field),
                f"completed source changed {runtime_id}.{field}",
            )
    try:
        with tempfile.TemporaryDirectory(prefix="deployment-source-check-") as raw:
            scratch = Path(raw) / "binding"
            production = build_full_flow_deployment_binding(
                logical_plan_dir,
                scenario_path,
                container_plan_dir,
                source_path,
                output_dir=scratch,
            )
    except FullFlowDeploymentError as exc:
        raise FullFlowDeploymentTemplateError(
            f"completed source failed final binding validation: {exc}"
        ) from exc
    return {
        "status": "VALID_COMPLETED_DEPLOYMENT_SOURCE",
        "template_id": template_report["template_id"],
        "deployment_id": production["deployment_id"],
        "backend": production["backend"],
        "logical_plan_sha256": production["logical_plan_sha256"],
        "binding_sha256": production["binding_sha256"],
        "service_contract_count": production["service_contract_count"],
        "runtime_service_binding_count": production[
            "runtime_service_binding_count"
        ],
        "w4_runtime_bindings_complete": production[
            "w4_runtime_bindings_complete"
        ],
        "capability_coverage_complete": True,
        "final_binding_ready": True,
        "binding_retained": False,
        "services_started": False,
        "cloud_calls_made": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "DEPLOYMENT_REQUIREMENTS_SCHEMA_VERSION",
    "DEPLOYMENT_SOURCE_TEMPLATE_SCHEMA_VERSION",
    "FullFlowDeploymentTemplateError",
    "REQUIREMENTS_NAME",
    "SOURCE_TEMPLATE_NAME",
    "generate_full_flow_deployment_source_template",
    "validate_completed_full_flow_deployment_source",
    "verify_full_flow_deployment_source_template",
]
