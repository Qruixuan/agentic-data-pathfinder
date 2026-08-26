"""Measurement contract for cost components the Data Agent cannot emit.

The Data Agent reports service cost and transfer bytes. Storage occupancy,
materialization work, and transition work are properties of the deployment
that nothing in the access path observes, so they arrive through a separate,
content-hashed manifest that the operator produces.

The manifest is bound to the run it was measured for. It carries the
preregistration and endpoint-registry digests, and a mismatch is refused
rather than tolerated: a stale manifest silently reused across two pilots
would attribute one deployment's storage cost to another's designs.

Missing data never becomes zero. A quantity the operator could not measure is
``unavailable`` and suppresses ``total_cost``; a quantity a design provably
does not incur is ``not_applicable`` and contributes a justified zero.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping, Protocol

from .cost import (
    CostLedger,
    CostModel,
    CostModelError,
    CostTelemetryIncompleteError,
    build_cost_ledger,
    not_applicable_component,
)


MEASUREMENT_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.pilot-measurements/v1alpha1"
)
MEASUREMENT_KINDS = ("measured", "configured", "derived", "unavailable",
                     "not_applicable")
WILDCARD_OBJECT = "*"


class MeasurementError(ValueError):
    """Raised when a measurement manifest is invalid or does not apply."""


class StaleMeasurementError(MeasurementError):
    """Raised when a manifest was measured for a different frozen run."""


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MeasurementError(f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MeasurementError(f"{name} must be a non-empty string")
    return value.strip()


def _require(payload: Mapping[str, Any], key: str, name: str) -> Any:
    if key not in payload:
        raise MeasurementError(f"{name} is a required field")
    return payload[key]


def _quantity(value: Any, name: str) -> float:
    if value is None:
        raise MeasurementError(f"{name} is required but missing")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MeasurementError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise MeasurementError(f"{name} must be finite")
    if result < 0.0:
        raise MeasurementError(f"{name} must be non-negative")
    return result


@dataclass(frozen=True)
class ComponentMeasurement:
    """One measured quantity with its provenance kind."""

    component_id: str
    kind: str
    quantities: dict[str, float]
    justification: str | None
    provenance: str

    @property
    def usable(self) -> bool:
        return self.kind in ("measured", "configured", "derived")


@dataclass(frozen=True)
class ComponentMeasurements:
    """Every non-access-path component for one (design, object, node)."""

    design_id: str
    object_id: str
    node_id: str
    storage: ComponentMeasurement
    materialization: ComponentMeasurement
    transition: ComponentMeasurement

    def by_component(self) -> dict[str, ComponentMeasurement]:
        return {
            "storage": self.storage,
            "amortized_materialization": self.materialization,
            "transition": self.transition,
        }


class MeasurementProvider(Protocol):
    """Supplies cost quantities the access path cannot observe."""

    manifest_sha256: str

    def measurements_for(
        self,
        *,
        design_id: str,
        object_id: str,
        node_id: str,
    ) -> ComponentMeasurements:
        """Return the declared quantities, or raise if none apply."""


def _component(
    payload: Any,
    *,
    component_id: str,
    required: tuple[str, ...],
    name: str,
) -> ComponentMeasurement:
    root = _mapping(payload, name)
    kind = _string(_require(root, "kind", f"{name}.kind"), f"{name}.kind")
    if kind not in MEASUREMENT_KINDS:
        raise MeasurementError(
            f"{name}.kind must be one of " + ", ".join(MEASUREMENT_KINDS)
        )
    provenance = _string(
        root.get("provenance", "operator-measurement-manifest"),
        f"{name}.provenance",
    )
    if kind == "not_applicable":
        return ComponentMeasurement(
            component_id=component_id,
            kind=kind,
            quantities={},
            justification=_string(
                _require(
                    root,
                    "justification",
                    f"{name}.justification",
                ),
                f"{name}.justification",
            ),
            provenance=provenance,
        )
    if kind == "unavailable":
        return ComponentMeasurement(
            component_id=component_id,
            kind=kind,
            quantities={},
            justification=_string(
                _require(root, "reason", f"{name}.reason"),
                f"{name}.reason",
            ),
            provenance=provenance,
        )
    return ComponentMeasurement(
        component_id=component_id,
        kind=kind,
        quantities={
            field: _quantity(
                _require(root, field, f"{name}.{field}"),
                f"{name}.{field}",
            )
            for field in required
        },
        justification=None,
        provenance=provenance,
    )


@dataclass(frozen=True)
class ManifestMeasurementProvider:
    """Measurements loaded from a content-hashed operator manifest."""

    schema_version: str
    measurement_id: str
    pilot_id: str
    preregistration_sha256: str
    endpoint_registry_sha256: str
    execution_node_id: str
    manifest_sha256: str
    entries: dict[tuple[str, str, str], ComponentMeasurements]

    def measurements_for(
        self,
        *,
        design_id: str,
        object_id: str,
        node_id: str,
    ) -> ComponentMeasurements:
        for key in (
            (design_id, object_id, node_id),
            (design_id, WILDCARD_OBJECT, node_id),
        ):
            found = self.entries.get(key)
            if found is not None:
                return found
        raise MeasurementError(
            "the measurement manifest declares nothing for design "
            f"'{design_id}', object '{object_id}', node '{node_id}'; "
            "refusing to assume zero"
        )

    def require_matching_run(
        self,
        *,
        pilot_id: str,
        preregistration_sha256: str,
        endpoint_registry_sha256: str,
        execution_node_id: str,
    ) -> None:
        """Refuse a manifest measured for a different frozen run."""
        mismatches = []
        if self.pilot_id != pilot_id:
            mismatches.append(
                f"pilot_id {self.pilot_id!r} != {pilot_id!r}"
            )
        if self.preregistration_sha256 != preregistration_sha256:
            mismatches.append("preregistration_sha256")
        if self.endpoint_registry_sha256 != endpoint_registry_sha256:
            mismatches.append("endpoint_registry_sha256")
        if self.execution_node_id != execution_node_id:
            mismatches.append(
                f"execution_node_id {self.execution_node_id!r} != "
                f"{execution_node_id!r}"
            )
        if mismatches:
            raise StaleMeasurementError(
                "measurement manifest was produced for a different frozen "
                "run: " + ", ".join(mismatches)
            )

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "measurement_id": self.measurement_id,
            "pilot_id": self.pilot_id,
            "manifest_sha256": self.manifest_sha256,
            "preregistration_sha256": self.preregistration_sha256,
            "endpoint_registry_sha256": self.endpoint_registry_sha256,
            "execution_node_id": self.execution_node_id,
            "entry_count": len(self.entries),
        }


def load_measurement_manifest(
    path: str | Path,
) -> ManifestMeasurementProvider:
    """Load and validate an operator-produced measurement manifest."""
    source = Path(path).resolve()
    if not source.is_file():
        raise MeasurementError(
            f"measurement manifest does not exist: {source}"
        )
    raw_bytes = source.read_bytes()
    try:
        root = _mapping(json.loads(raw_bytes), "measurement manifest")
    except json.JSONDecodeError as exc:
        raise MeasurementError(
            f"invalid measurement manifest JSON: {source}"
        ) from exc
    schema_version = _string(
        _require(root, "schema_version", "schema_version"),
        "schema_version",
    )
    if schema_version != MEASUREMENT_MANIFEST_SCHEMA_VERSION:
        raise MeasurementError(
            "unsupported measurement manifest schema_version: "
            + schema_version
        )
    raw_entries = _require(root, "measurements", "measurements")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise MeasurementError("measurements must be a non-empty array")

    entries: dict[tuple[str, str, str], ComponentMeasurements] = {}
    for index, raw in enumerate(raw_entries):
        name = f"measurements[{index}]"
        payload = _mapping(raw, name)
        design_id = _string(
            _require(payload, "design_id", f"{name}.design_id"),
            f"{name}.design_id",
        )
        object_id = _string(
            payload.get("object_id", WILDCARD_OBJECT),
            f"{name}.object_id",
        )
        node_id = _string(
            _require(payload, "node_id", f"{name}.node_id"),
            f"{name}.node_id",
        )
        key = (design_id, object_id, node_id)
        if key in entries:
            raise MeasurementError(
                f"duplicate measurement entry for {key}"
            )
        entries[key] = ComponentMeasurements(
            design_id=design_id,
            object_id=object_id,
            node_id=node_id,
            storage=_component(
                _require(payload, "storage", f"{name}.storage"),
                component_id="storage",
                required=("bytes", "hours"),
                name=f"{name}.storage",
            ),
            materialization=_component(
                _require(
                    payload,
                    "materialization",
                    f"{name}.materialization",
                ),
                component_id="amortized_materialization",
                required=("bytes",),
                name=f"{name}.materialization",
            ),
            transition=_component(
                _require(payload, "transition", f"{name}.transition"),
                component_id="transition",
                required=("bytes", "seconds"),
                name=f"{name}.transition",
            ),
        )

    return ManifestMeasurementProvider(
        schema_version=schema_version,
        measurement_id=_string(
            _require(root, "measurement_id", "measurement_id"),
            "measurement_id",
        ),
        pilot_id=_string(
            _require(root, "pilot_id", "pilot_id"),
            "pilot_id",
        ),
        preregistration_sha256=_string(
            _require(
                root,
                "preregistration_sha256",
                "preregistration_sha256",
            ),
            "preregistration_sha256",
        ),
        endpoint_registry_sha256=_string(
            _require(
                root,
                "endpoint_registry_sha256",
                "endpoint_registry_sha256",
            ),
            "endpoint_registry_sha256",
        ),
        execution_node_id=_string(
            _require(root, "execution_node_id", "execution_node_id"),
            "execution_node_id",
        ),
        manifest_sha256=sha256(raw_bytes).hexdigest(),
        entries=entries,
    )


@dataclass(frozen=True)
class AccessCostInputs:
    """The access-path half of the ledger, read from reconciled events."""

    service_cost: float
    transferred_bytes: float | None
    endpoint_id: str | None
    source_node_id: str | None
    source_location: str | None
    destination_execution_node_id: str | None
    telemetry_complete: bool


def transferred_bytes_for_event(
    event: Mapping[str, Any],
    *,
    crosses_network: bool,
) -> float | None:
    """Bytes this one accepted access moved across the network.

    An artifact access is charged its completed ``artifact_bytes_sent`` and
    *not* its ``bytes_read``: the latter measures the small handle-metadata
    response, and adding both would count one payload twice. An inline access
    is charged its reconciled ``bytes_read``, which is the payload itself --
    counting only artifact downloads would make a design that returns data
    inline look as if it used no network at all.

    Returns ``None`` when the telemetry needed to answer is absent, which
    propagates as an unavailable network component rather than a zero.
    """
    if not crosses_network:
        return 0.0
    if event.get("artifact_handle_sha256"):
        requested = event.get("artifact_download_request_count")
        completed = event.get("artifact_full_download_count")
        sent = event.get("artifact_bytes_sent")
        if sent is None or requested is None or completed is None:
            return None
        if int(completed) < 1:
            # The transfer did not complete, so its byte count is not a
            # measurement of what crossed the network.
            return None
        return float(sent)
    value = event.get("bytes_read")
    if value is None:
        return None
    return float(value)


def access_cost_inputs(
    record: Mapping[str, Any],
    *,
    network_transport_for: Mapping[str, str] | None = None,
) -> AccessCostInputs:
    """Extract service cost and transferred bytes from one canonical record.

    Only accepted events contribute. ``network_transport_for`` maps an
    endpoint id to ``remote`` or ``local``; a local endpoint contributes a
    justified zero because its bytes never leave the execution node.
    """
    events = [
        event
        for event in record.get("access_events", [])
        if isinstance(event, Mapping) and event.get("accepted")
    ]
    service_cost = sum(
        float(event.get("realized_cost", 0.0)) for event in events
    )
    telemetry_complete = bool(record.get("telemetry_complete"))
    transports = dict(network_transport_for or {})
    transferred: float | None
    if not telemetry_complete:
        transferred = None
    else:
        transferred = 0.0
        for event in events:
            endpoint_id = event.get("endpoint_id")
            crosses = transports.get(str(endpoint_id), "remote") == "remote"
            measured = transferred_bytes_for_event(
                event,
                crosses_network=crosses,
            )
            if measured is None:
                transferred = None
                break
            transferred += measured
    endpoint_ids = {
        event.get("endpoint_id")
        for event in events
        if event.get("endpoint_id")
    }
    return AccessCostInputs(
        service_cost=service_cost,
        transferred_bytes=transferred,
        endpoint_id=(
            next(iter(endpoint_ids)) if len(endpoint_ids) == 1 else None
        ),
        source_node_id=next(
            (
                str(event.get("source_node_id"))
                for event in events
                if event.get("source_node_id")
            ),
            None,
        ),
        source_location=next(
            (
                str(event.get("source_location"))
                for event in events
                if event.get("source_location")
            ),
            None,
        ),
        destination_execution_node_id=next(
            (
                str(event.get("destination_execution_node_id"))
                for event in events
                if event.get("destination_execution_node_id")
            ),
            None,
        ),
        telemetry_complete=telemetry_complete,
    )


def build_measured_cost_ledger(
    record: Mapping[str, Any],
    *,
    model: CostModel,
    provider: MeasurementProvider,
    design_id: str,
    object_id: str,
    node_id: str,
    network_transport_for: Mapping[str, str] | None = None,
) -> CostLedger:
    """Assemble one ledger from live access records plus operator measurements.

    Service cost and transfer bytes come from the reconciled access events;
    storage, materialization, and transition come from the provider. Artifact
    transfer is charged to whichever single component the cost model declares,
    so it can never be counted twice.
    """
    inputs = access_cost_inputs(
        record,
        network_transport_for=network_transport_for,
    )
    measurements = provider.measurements_for(
        design_id=design_id,
        object_id=object_id,
        node_id=node_id,
    )
    unavailable: dict[str, str] = {}
    if inputs.transferred_bytes is None:
        unavailable["network"] = (
            "transfer telemetry did not reconcile for at least one accepted "
            "access, so transferred bytes are unknown"
        )

    by_component = measurements.by_component()
    overrides: dict[str, Any] = {}
    for component_id, measurement in by_component.items():
        if measurement.kind == "unavailable":
            unavailable[component_id] = (
                measurement.justification or "declared unavailable"
            )
        elif measurement.kind == "not_applicable":
            overrides[component_id] = not_applicable_component(
                component_id,
                raw_unit={
                    "storage": "gib_hours",
                    "amortized_materialization": "bytes",
                    "transition": "bytes+seconds",
                }[component_id],
                justification=measurement.justification or "",
                provenance=measurement.provenance,
            )

    storage = by_component["storage"]
    materialization = by_component["amortized_materialization"]
    transition = by_component["transition"]
    ledger = build_cost_ledger(
        model,
        service_cost=inputs.service_cost,
        transferred_bytes=inputs.transferred_bytes,
        stored_bytes=(
            storage.quantities.get("bytes") if storage.usable else 0.0
        ),
        stored_hours=(
            storage.quantities.get("hours") if storage.usable else 0.0
        ),
        materialized_bytes=(
            materialization.quantities.get("bytes")
            if materialization.usable
            else 0.0
        ),
        transition_bytes=(
            transition.quantities.get("bytes") if transition.usable else 0.0
        ),
        transition_seconds=(
            transition.quantities.get("seconds")
            if transition.usable
            else 0.0
        ),
        unavailable=unavailable,
    )
    if not overrides:
        return ledger
    return CostLedger(
        schema_version=ledger.schema_version,
        accounting_unit=ledger.accounting_unit,
        components=tuple(
            overrides.get(component.component_id, component)
            for component in ledger.components
        ),
        cost_model_id=ledger.cost_model_id,
        cost_model_sha256=ledger.cost_model_sha256,
        artifact_transfer_accounted_in=(
            ledger.artifact_transfer_accounted_in
        ),
        amortization_horizon_sessions=(
            ledger.amortization_horizon_sessions
        ),
    )
