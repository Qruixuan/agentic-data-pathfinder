"""Read-only preflight for a distributed pilot.

Verifies everything that can be verified without submitting a workflow, and
is explicit about the things it cannot verify. No worker, Data Agent, or MCP
lifecycle is touched; no mutating API is called; no credential value is
emitted or returned.

The honest boundary matters as much as the checks: a green preflight proves
the deployment is *declared* consistently and that each Data Agent answers a
health read. It does not prove a task will be dispatched, executed, and have
its result delivered. That is only learned by running one.
"""

from __future__ import annotations

from os import environ as _os_environ
from typing import Any, Mapping, Protocol


def os_environ() -> Mapping[str, str]:
    return _os_environ

from ..integrations.flowmesh.redaction import redact_secrets
from .cost import CostModel
from .preregistration import DistributedPilotPreregistration
from .scoring import MULTIPLE_CHOICE_EXACT_SCORING_RULE
from .registry import (
    DataAgentEndpoint,
    EndpointRegistry,
    EndpointUnreachableError,
)


DISTRIBUTED_PREFLIGHT_SCHEMA_VERSION = (
    "pathfinder.distributed-pilot-preflight/v1alpha1"
)
#: ``offline_validation`` checks shape and consistency and tolerates
#: placeholders. ``live_pilot`` refuses to pass while any placeholder that
#: would corrupt a real measurement remains.
PREFLIGHT_MODES = ("offline_validation", "live_pilot")

#: Values that mark a field as not yet filled in for a real deployment, or
#: that admit on their face that they are not a real measurement. A rate
#: whose provenance calls itself a fixture is exactly what live_pilot mode
#: exists to catch, so those words are markers too.
_PLACEHOLDER_MARKERS = (
    "PLACEHOLDER",
    "TBD",
    "CHANGEME",
    "CHANGE_ME",
    "REPLACE_ME",
    "REPLACEME",
    "EXAMPLE",
    "FIXTURE",
    "SYNTHETIC",
    "NON-SCIENTIFIC",
    "NOT MEASURED",
    "NOT-MEASURED",
    "DUMMY",
    "SAMPLE",
    "FIXME",
    "XXX",
)
_PLACEHOLDER_GIT_REVISIONS = ("0" * 40, "0" * 7, "unknown", "none")

REQUIRED_TELEMETRY_CAPABILITIES = (
    "access_telemetry",
    "transfer_bytes",
)

UNVERIFIED_PROPERTIES = (
    "FlowMesh task dispatch: that a submitted workflow reaches the pinned "
    "worker at all",
    "worker-to-endpoint network reachability: this process probing a Data "
    "Agent says nothing about whether the worker can reach it",
    "task result delivery: that a completed run's result is uploaded back "
    "to this control plane",
    "future telemetry completeness: that transfers during the pilot will "
    "reconcile, which is only known per session after the fact",
    "measured cost conversion rates reflect the real deployment",
    "the object bytes behind a matching catalog version are identical",
)

OPERATOR_NEXT_STEPS = (
    "Start the Data Agent and FlowMesh worker processes manually; this "
    "tooling never manages their lifecycle.",
    "Run one throwaway workload end to end and confirm the worker logs a "
    "received task before treating dispatch as healthy.",
    "Replace every placeholder cost conversion rate with a measured value "
    "before interpreting any cost comparison.",
)


class HealthProbe(Protocol):
    """Read-only health surface a Data Agent client must expose."""

    def health(self, endpoint_id: str) -> Mapping[str, Any]:
        """Return a health document, or raise EndpointUnreachableError."""


def _is_placeholder(value: object) -> bool:
    """True when a declared value is still a fill-me-in marker."""
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    upper = text.upper()
    return any(marker in upper for marker in _PLACEHOLDER_MARKERS)


def _is_placeholder_git_revision(value: object) -> bool:
    text = str(value or "").strip().lower()
    if not text or text in _PLACEHOLDER_GIT_REVISIONS:
        return True
    if set(text) == {"0"}:
        return True
    return _is_placeholder(value)


def _check(
    check_id: str,
    passed: bool,
    detail: str,
    *,
    secret_environment_names: tuple[str, ...] = (),
    **extra: Any,
) -> dict[str, Any]:
    """Build one check result, redacting the detail unconditionally.

    Detail text can originate in a third-party client or a probe that did not
    redact anything, so every string is scrubbed here against the registry's
    declared token variables before it can reach a report.
    """
    return {
        "check_id": check_id,
        "passed": bool(passed),
        "detail": redact_secrets(
            detail,
            extra_environment_names=secret_environment_names,
        ),
        **extra,
    }


def _endpoint_checks(
    endpoint: DataAgentEndpoint,
    probe: HealthProbe | None,
    *,
    expected_catalog_versions: Mapping[str, str],
    environment: Mapping[str, str] | None,
    secret_environment_names: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    prefix = f"endpoint[{endpoint.endpoint_id}]"
    try:
        endpoint.resolve_base_url(environment)
        configured = True
        detail = (
            f"{endpoint.base_url_env} is set; the URL itself is not recorded"
        )
    except Exception as exc:
        configured = False
        detail = redact_secrets(str(exc))
    checks.append(_check(
        f"{prefix}.base_url_configured",
        configured,
        detail,
        secret_environment_names=secret_environment_names,
    ))
    missing_capabilities = [
        capability
        for capability in REQUIRED_TELEMETRY_CAPABILITIES
        if capability not in endpoint.telemetry_capabilities
    ]
    checks.append(_check(
        f"{prefix}.telemetry_capabilities_advertised",
        not missing_capabilities,
        (
            "all required telemetry capabilities are advertised"
            if not missing_capabilities
            else "missing: " + ", ".join(missing_capabilities)
        ),
        required=list(REQUIRED_TELEMETRY_CAPABILITIES),
        advertised=list(endpoint.telemetry_capabilities),
    ))
    if probe is None:
        checks.append(_check(
            f"{prefix}.health",
            False,
            "no health probe was supplied, so reachability is unverified",
            probe_supplied=False,
        ))
        return checks
    try:
        document = probe.health(endpoint.endpoint_id)
    except EndpointUnreachableError as exc:
        checks.append(_check(
            f"{prefix}.health",
            False,
            exc.detail,
            secret_environment_names=secret_environment_names,
            failure_class="infrastructure",
        ))
        return checks
    except Exception as exc:
        checks.append(_check(
            f"{prefix}.health",
            False,
            str(exc),
            secret_environment_names=secret_environment_names,
            failure_class="unknown",
        ))
        return checks
    healthy = bool(document.get("healthy"))
    checks.append(_check(
        f"{prefix}.health",
        healthy,
        (
            "Data Agent reports healthy"
            if healthy
            else "Data Agent is not healthy"
        ),
        node_id=document.get("node_id"),
    ))
    reported_node = document.get("node_id")
    checks.append(_check(
        f"{prefix}.node_identity_matches_registry",
        reported_node == endpoint.node_id,
        (
            f"reported node '{reported_node}' matches the registry"
            if reported_node == endpoint.node_id
            else f"registry declares '{endpoint.node_id}' but the endpoint "
                 f"reports '{reported_node}'"
        ),
    ))
    reported_versions = document.get("object_catalog_versions") or {}
    mismatches = sorted(
        object_id
        for object_id, expected in expected_catalog_versions.items()
        if reported_versions.get(object_id) not in (None, expected)
    )
    unknown = sorted(
        object_id
        for object_id in expected_catalog_versions
        if object_id not in reported_versions
    )
    checks.append(_check(
        f"{prefix}.object_catalog_versions_match",
        not mismatches and not unknown,
        (
            "every preregistered object catalog version matches"
            if not mismatches and not unknown
            else "mismatched: " + ", ".join(mismatches)
            + "; not reported: " + ", ".join(unknown)
        ),
        mismatched=mismatches,
        not_reported=unknown,
    ))
    return checks


def _cost_model_checks(
    model: CostModel,
    *,
    mode: str,
) -> list[dict[str, Any]]:
    """Validate the declared cost contract.

    A zero rate is legitimate only when its provenance is a real, measured
    justification. A ``PLACEHOLDER`` provenance always fails a live pilot,
    because a placeholder zero and a measured zero are indistinguishable in
    the ledger but not in what they mean.
    """
    placeholder = _is_placeholder(model.rate_provenance)
    live = mode == "live_pilot"
    zero_rates = sorted(
        name
        for name, value in (
            ("network_cost_per_gib", model.network_cost_per_gib),
            ("storage_cost_per_gib_hour", model.storage_cost_per_gib_hour),
            (
                "materialization_cost_per_gib",
                model.materialization_cost_per_gib,
            ),
            ("transition_cost_per_gib", model.transition_cost_per_gib),
            (
                "elapsed_time_cost_per_second",
                model.elapsed_time_cost_per_second,
            ),
        )
        if value == 0.0
    )
    return [
        _check(
            "cost_model.conversion_rates_complete",
            True,
            "every conversion rate is declared; the loader rejects a "
            "missing or non-finite rate",
            accounting_unit=model.accounting_unit,
        ),
        _check(
            "cost_model.amortization_horizon_declared",
            model.materialization_amortization_horizon_sessions > 0,
            "materialization amortization horizon is "
            f"{model.materialization_amortization_horizon_sessions} sessions",
        ),
        _check(
            "cost_model.artifact_transfer_accounted_once",
            model.artifact_transfer_accounted_in in (
                "service_cost",
                "network_cost",
            ),
            "artifact transfer is accounted in "
            f"{model.artifact_transfer_accounted_in}; the "
            f"{model.transfer_excluded_component} component excludes it",
        ),
        _check(
            "cost_model.rates_are_measured_not_placeholder",
            not placeholder,
            (
                "rate_provenance is a placeholder; cost comparisons from "
                "this run are not interpretable"
                if placeholder
                else "rate provenance claims measured deployment rates"
            ),
            # Advisory offline, blocking for a live pilot.
            advisory=not live,
            zero_rates=zero_rates,
        ),
        _check(
            "cost_model.zero_rates_are_justified",
            not (zero_rates and placeholder),
            (
                "zero conversion rate(s) "
                + ", ".join(zero_rates)
                + " carry a placeholder provenance; a zero rate is "
                "legitimate only when its provenance explicitly justifies "
                "it as measured"
                if zero_rates and placeholder
                else "no unjustified zero conversion rate"
            ),
            advisory=not live,
            zero_rates=zero_rates,
        ),
    ]


def preflight_distributed_pilot(
    preregistration: DistributedPilotPreregistration,
    registry: EndpointRegistry,
    *,
    probe: HealthProbe | None = None,
    representation_ids: tuple[str, ...] = (),
    expected_catalog_versions: Mapping[str, str] | None = None,
    worker_pin: Mapping[str, Any] | None = None,
    environment: Mapping[str, str] | None = None,
    mode: str = "offline_validation",
    measurement_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Verify a distributed pilot deployment without mutating anything.

    ``offline_validation`` checks structural consistency and tolerates
    placeholders. ``live_pilot`` additionally refuses every placeholder that
    would silently corrupt a real measurement.
    """
    if mode not in PREFLIGHT_MODES:
        raise ValueError(
            "preflight mode must be one of " + ", ".join(PREFLIGHT_MODES)
        )
    live = mode == "live_pilot"
    catalog_versions = dict(expected_catalog_versions or {})
    secret_names = registry.secret_environment_names
    checks: list[dict[str, Any]] = []

    required_designs = (
        preregistration.safe_design_id,
        *preregistration.candidate_design_ids,
    )
    routes: list[dict[str, Any]] = []
    unroutable: list[str] = []
    for design_id in required_designs:
        for representation_id in (representation_ids or ("*",)):
            try:
                route = registry.route(
                    design_id=design_id,
                    representation_id=representation_id,
                )
            except Exception as exc:
                unroutable.append(f"{design_id}/{representation_id}")
                checks.append(_check(
                    f"routing[{design_id}/{representation_id}]",
                    False,
                    str(exc),
                    secret_environment_names=secret_names,
                ))
                continue
            routes.append(route.to_public_dict())
    checks.append(_check(
        "every_required_endpoint_is_declared",
        not unroutable,
        (
            "every required design resolves to exactly one declared endpoint"
            if not unroutable
            else "unroutable: " + ", ".join(unroutable)
        ),
    ))
    checks.append(_check(
        "execution_node_identity_configured",
        bool(registry.execution_node_id.strip()),
        f"execution node is '{registry.execution_node_id}'",
    ))

    used_endpoints = sorted({route["endpoint_id"] for route in routes})
    for endpoint_id in used_endpoints:
        checks.extend(_endpoint_checks(
            registry.endpoint(endpoint_id),
            probe,
            expected_catalog_versions=catalog_versions,
            environment=environment,
            secret_environment_names=secret_names,
        ))

    checks.extend(_cost_model_checks(preregistration.cost_model, mode=mode))

    # Live-pilot identity and provenance gates. Each is advisory offline and
    # blocking for a live run, so an operator can validate a draft plan long
    # before the deployment identifiers exist.
    checks.append(_check(
        "identity.source_git_revision_is_real",
        not _is_placeholder_git_revision(
            preregistration.source_git_revision
        ),
        (
            "source_git_revision is still a placeholder"
            if _is_placeholder_git_revision(
                preregistration.source_git_revision
            )
            else "source_git_revision names a real commit"
        ),
        advisory=not live,
    ))
    checks.append(_check(
        "identity.execution_node_is_real",
        not _is_placeholder(registry.execution_node_id),
        (
            f"execution_node_id '{registry.execution_node_id}' is a "
            "placeholder"
            if _is_placeholder(registry.execution_node_id)
            else f"execution node is '{registry.execution_node_id}'"
        ),
        advisory=not live,
    ))
    placeholder_nodes = sorted(
        registry.endpoint(endpoint_id).node_id
        for endpoint_id in used_endpoints
        if _is_placeholder(registry.endpoint(endpoint_id).node_id)
    )
    checks.append(_check(
        "identity.endpoint_node_ids_are_real",
        not placeholder_nodes,
        (
            "placeholder node identities: " + ", ".join(placeholder_nodes)
            if placeholder_nodes
            else "every routed endpoint declares a real node identity"
        ),
        advisory=not live,
    ))
    unset_urls = sorted(
        endpoint_id
        for endpoint_id in used_endpoints
        if not str(
            (environment if environment is not None else os_environ()).get(
                registry.endpoint(endpoint_id).base_url_env
            )
            or ""
        ).strip()
    )
    checks.append(_check(
        "identity.endpoint_urls_are_configured",
        not unset_urls,
        (
            "unset endpoint URL variable(s) for: " + ", ".join(unset_urls)
            if unset_urls
            else "every routed endpoint has its URL variable set"
        ),
        advisory=not live,
    ))
    checks.append(_check(
        "manifest.workload_hashes_declared",
        bool(preregistration.workload_manifest_sha256)
        and bool(preregistration.excluded_workload_manifest_sha256),
        "workload and exclusion manifest digests are computed and bound "
        "to the plan",
    ))
    if (
        preregistration.success_scoring_rule
        == MULTIPLE_CHOICE_EXACT_SCORING_RULE
    ):
        bindings = {
            "selection_protocol_sha256": (
                preregistration.selection_protocol_sha256
            ),
            "scoring_contract_sha256": (
                preregistration.scoring_contract_sha256
            ),
            "representation_manifest_sha256": (
                preregistration.representation_manifest_sha256
            ),
        }
        placeholder_bindings = sorted(
            field
            for field, digest in bindings.items()
            if digest is None or set(digest) == {"0"}
        )
        checks.append(_check(
            "manifest.benchmark_contracts_bound",
            not placeholder_bindings,
            (
                "selection, scoring, and representation contracts are "
                "bound by SHA-256"
                if not placeholder_bindings
                else "placeholder benchmark binding(s): "
                     + ", ".join(placeholder_bindings)
            ),
            advisory=not live,
        ))
    checks.append(_check(
        "manifest.measurement_manifest_bound",
        measurement_manifest_sha256 is not None,
        (
            f"measurement manifest {measurement_manifest_sha256[:16]}... "
            "is bound to this run"
            if measurement_manifest_sha256
            else "no measurement manifest was supplied; storage, "
                 "materialization, and transition costs cannot be measured"
        ),
        advisory=not live,
    ))

    if worker_pin is None:
        checks.append(_check(
            "worker_pin_configuration_valid",
            False,
            "no worker pin was supplied",
        ))
    else:
        kind = worker_pin.get("kind")
        value = worker_pin.get("value")
        valid = (
            kind in ("worker_id", "worker_alias")
            and isinstance(value, str)
            and bool(value.strip())
        )
        checks.append(_check(
            "worker_pin_configuration_valid",
            valid,
            (
                f"worker pin is syntactically valid ({kind})"
                if valid
                else "worker pin must declare kind=worker_id|worker_alias "
                     "and a non-empty value"
            ),
            syntactic_only=True,
        ))

    blocking = [
        check["check_id"]
        for check in checks
        if not check["passed"] and not check.get("advisory")
    ]
    advisory = [
        check["check_id"]
        for check in checks
        if not check["passed"] and check.get("advisory")
    ]
    return {
        "schema_version": DISTRIBUTED_PREFLIGHT_SCHEMA_VERSION,
        "status": "ok" if not blocking else "failed",
        "mode": mode,
        "pilot_id": preregistration.pilot_id,
        "registry_id": registry.registry_id,
        "execution_node_id": registry.execution_node_id,
        "posthoc": preregistration.posthoc,
        "confirmatory": preregistration.confirmatory,
        "eligible_for_scientific_claims": (
            preregistration.eligible_for_scientific_claims
        ),
        "checks": checks,
        "failed_checks": blocking,
        "advisory_warnings": advisory,
        "routes": routes,
        "endpoints": [
            registry.endpoint(endpoint_id).to_public_dict(environment)
            for endpoint_id in used_endpoints
        ],
        "read_only": {
            "workflow_submitted": False,
            "workflow_validated": False,
            "worker_lifecycle_touched": False,
            "data_agent_lifecycle_touched": False,
            "mcp_started": False,
            "mutating_api_called": False,
            "deployment_state_changed": False,
        },
        "credentials_recorded": False,
        "not_verified": list(UNVERIFIED_PROPERTIES),
        "interpretation": (
            "Preflight proves the deployment is declared consistently and "
            "that each required Data Agent answers a read-only health "
            "query from THIS process. It cannot prove FlowMesh task "
            "dispatch, worker-to-endpoint reachability, task result "
            "delivery, or that future telemetry will reconcile; only an "
            "end-to-end run establishes those."
        ),
        "operator_next_steps": list(OPERATOR_NEXT_STEPS),
    }
