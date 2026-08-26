"""Versioned total-cost ledger for the distributed pilot.

    total_cost = service
               + network
               + storage
               + amortized_materialization
               + transition

Every raw component is preserved separately alongside its unit, the rule used
to convert it into accounting units, and whether the number was measured,
configured, or derived. Nothing is silently defaulted to zero: a component
the pilot could not measure is recorded as *unavailable*, which makes the
whole ledger unavailable rather than quietly cheaper.

This module deliberately declares no conversion rates. Network, storage,
materialization, and transition rates are properties of a real deployment,
so they are required external pilot configuration and every committed example
uses transparently non-scientific placeholder values.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping


COST_MODEL_SCHEMA_VERSION = "pathfinder.total-cost-model/v1alpha1"
COST_LEDGER_SCHEMA_VERSION = "pathfinder.total-cost-ledger/v1alpha1"

COST_COMPONENT_IDS = (
    "service",
    "network",
    "storage",
    "amortized_materialization",
    "transition",
)
#: How a value entered the ledger.
#:
#: ``unavailable`` means the pilot could not measure the quantity: the total
#: is suppressed. ``not_applicable`` means the quantity provably does not
#: exist for this design -- an origin design materializes nothing -- and
#: contributes a justified zero. Conflating the two is exactly how a design
#: comes to look cheaper than it is.
VALUE_KINDS = (
    "measured",
    "configured",
    "derived",
    "unavailable",
    "not_applicable",
)
#: Which single component absorbs artifact transfer. Declaring this is what
#: makes double counting detectable instead of plausible.
TRANSFER_ACCOUNTS = ("service_cost", "network_cost")

_BYTES_PER_GIB = float(1024 ** 3)


class CostModelError(ValueError):
    """Raised when a cost model or a cost ledger is invalid."""


class CostTelemetryIncompleteError(CostModelError):
    """Raised when telemetry required by the declared cost rule is missing."""


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CostModelError(f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CostModelError(f"{name} must be a non-empty string")
    return value.strip()


def _require(payload: Mapping[str, Any], key: str, name: str) -> Any:
    if key not in payload:
        raise CostModelError(f"{name} is a required configuration field")
    return payload[key]


def _rate(value: Any, name: str) -> float:
    """Validate a conversion rate: finite, non-negative, not a bool."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CostModelError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise CostModelError(f"{name} must be finite")
    if result < 0.0:
        raise CostModelError(f"{name} must be non-negative")
    return result


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CostModelError(f"{name} must be a positive integer")
    return value


def _quantity(value: Any, name: str) -> float:
    """Validate an observed raw quantity (bytes, seconds, GiB-hours)."""
    if value is None:
        raise CostTelemetryIncompleteError(f"{name} is required but missing")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CostModelError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise CostModelError(f"{name} must be finite")
    if result < 0.0:
        raise CostModelError(f"{name} must be non-negative")
    return result


@dataclass(frozen=True)
class CostModel:
    """Declared conversion rules for one pilot. No rate has a default."""

    schema_version: str
    cost_model_id: str
    accounting_unit: str
    rate_provenance: str
    network_cost_per_gib: float
    storage_cost_per_gib_hour: float
    materialization_cost_per_gib: float
    transition_cost_per_gib: float
    elapsed_time_cost_per_second: float
    materialization_amortization_horizon_sessions: int
    artifact_transfer_accounted_in: str
    source_sha256: str

    @property
    def transfer_excluded_component(self) -> str:
        return (
            "network"
            if self.artifact_transfer_accounted_in == "service_cost"
            else "service"
        )

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "cost_model_id": self.cost_model_id,
            "accounting_unit": self.accounting_unit,
            "rate_provenance": self.rate_provenance,
            "network_cost_per_gib": self.network_cost_per_gib,
            "storage_cost_per_gib_hour": self.storage_cost_per_gib_hour,
            "materialization_cost_per_gib": (
                self.materialization_cost_per_gib
            ),
            "transition_cost_per_gib": self.transition_cost_per_gib,
            "elapsed_time_cost_per_second": (
                self.elapsed_time_cost_per_second
            ),
            "materialization_amortization_horizon_sessions": (
                self.materialization_amortization_horizon_sessions
            ),
            "artifact_transfer_accounted_in": (
                self.artifact_transfer_accounted_in
            ),
            "cost_model_sha256": self.source_sha256,
        }


def load_cost_model(payload: Any, *, name: str = "cost_model") -> CostModel:
    """Validate a declared cost model. Every rate is required."""
    root = _mapping(payload, name)
    schema_version = _string(
        _require(root, "schema_version", f"{name}.schema_version"),
        f"{name}.schema_version",
    )
    if schema_version != COST_MODEL_SCHEMA_VERSION:
        raise CostModelError(
            f"unsupported cost model schema_version: {schema_version}"
        )
    transfer = _string(
        _require(
            root,
            "artifact_transfer_accounted_in",
            f"{name}.artifact_transfer_accounted_in",
        ),
        f"{name}.artifact_transfer_accounted_in",
    )
    if transfer not in TRANSFER_ACCOUNTS:
        raise CostModelError(
            f"{name}.artifact_transfer_accounted_in must be one of "
            + ", ".join(TRANSFER_ACCOUNTS)
        )
    rates = _mapping(
        _require(root, "conversion_rates", f"{name}.conversion_rates"),
        f"{name}.conversion_rates",
    )
    canonical = json.dumps(root, sort_keys=True, separators=(",", ":"))
    return CostModel(
        schema_version=schema_version,
        cost_model_id=_string(
            _require(root, "cost_model_id", f"{name}.cost_model_id"),
            f"{name}.cost_model_id",
        ),
        accounting_unit=_string(
            _require(root, "accounting_unit", f"{name}.accounting_unit"),
            f"{name}.accounting_unit",
        ),
        rate_provenance=_string(
            _require(root, "rate_provenance", f"{name}.rate_provenance"),
            f"{name}.rate_provenance",
        ),
        network_cost_per_gib=_rate(
            _require(
                rates,
                "network_cost_per_gib",
                f"{name}.conversion_rates.network_cost_per_gib",
            ),
            f"{name}.conversion_rates.network_cost_per_gib",
        ),
        storage_cost_per_gib_hour=_rate(
            _require(
                rates,
                "storage_cost_per_gib_hour",
                f"{name}.conversion_rates.storage_cost_per_gib_hour",
            ),
            f"{name}.conversion_rates.storage_cost_per_gib_hour",
        ),
        materialization_cost_per_gib=_rate(
            _require(
                rates,
                "materialization_cost_per_gib",
                f"{name}.conversion_rates.materialization_cost_per_gib",
            ),
            f"{name}.conversion_rates.materialization_cost_per_gib",
        ),
        transition_cost_per_gib=_rate(
            _require(
                rates,
                "transition_cost_per_gib",
                f"{name}.conversion_rates.transition_cost_per_gib",
            ),
            f"{name}.conversion_rates.transition_cost_per_gib",
        ),
        elapsed_time_cost_per_second=_rate(
            _require(
                rates,
                "elapsed_time_cost_per_second",
                f"{name}.conversion_rates.elapsed_time_cost_per_second",
            ),
            f"{name}.conversion_rates.elapsed_time_cost_per_second",
        ),
        materialization_amortization_horizon_sessions=_positive_integer(
            _require(
                root,
                "materialization_amortization_horizon_sessions",
                f"{name}.materialization_amortization_horizon_sessions",
            ),
            f"{name}.materialization_amortization_horizon_sessions",
        ),
        artifact_transfer_accounted_in=transfer,
        source_sha256=sha256(canonical.encode("utf-8")).hexdigest(),
    )


@dataclass(frozen=True)
class CostComponent:
    """One additive term, with its raw quantity and conversion preserved."""

    component_id: str
    value: float | None
    value_kind: str
    raw_quantity: float | None
    raw_unit: str
    conversion_rule: str
    conversion_rate: float | None
    provenance: str
    unavailable_reason: str | None = None

    @property
    def available(self) -> bool:
        return self.value_kind != "unavailable"

    @property
    def not_applicable(self) -> bool:
        return self.value_kind == "not_applicable"

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "value": self.value,
            "value_kind": self.value_kind,
            "available": self.available,
            "raw_quantity": self.raw_quantity,
            "raw_unit": self.raw_unit,
            "conversion_rule": self.conversion_rule,
            "conversion_rate": self.conversion_rate,
            "provenance": self.provenance,
            "unavailable_reason": self.unavailable_reason,
        }


def not_applicable_component(
    component_id: str,
    *,
    raw_unit: str,
    justification: str,
    provenance: str,
) -> CostComponent:
    """A justified zero for a quantity this design provably does not incur."""
    if component_id not in COST_COMPONENT_IDS:
        raise CostModelError(f"unknown cost component: {component_id}")
    if not justification.strip():
        raise CostModelError(
            f"{component_id} declared not_applicable without a justification"
        )
    return CostComponent(
        component_id=component_id,
        value=0.0,
        value_kind="not_applicable",
        raw_quantity=0.0,
        raw_unit=raw_unit,
        conversion_rule=f"not applicable: {justification}",
        conversion_rate=None,
        provenance=provenance,
    )


def _unavailable(
    component_id: str,
    *,
    raw_unit: str,
    conversion_rule: str,
    reason: str,
    provenance: str,
) -> CostComponent:
    return CostComponent(
        component_id=component_id,
        value=None,
        value_kind="unavailable",
        raw_quantity=None,
        raw_unit=raw_unit,
        conversion_rule=conversion_rule,
        conversion_rate=None,
        provenance=provenance,
        unavailable_reason=reason,
    )


@dataclass(frozen=True)
class CostLedger:
    """A complete additive decomposition for one observation."""

    schema_version: str
    accounting_unit: str
    components: tuple[CostComponent, ...]
    cost_model_id: str
    cost_model_sha256: str
    artifact_transfer_accounted_in: str
    amortization_horizon_sessions: int

    @property
    def not_applicable_component_ids(self) -> tuple[str, ...]:
        return tuple(
            component.component_id
            for component in self.components
            if component.not_applicable
        )

    @property
    def unavailable_component_ids(self) -> tuple[str, ...]:
        return tuple(
            component.component_id
            for component in self.components
            if not component.available
        )

    @property
    def available(self) -> bool:
        return not self.unavailable_component_ids

    @property
    def total_cost(self) -> float | None:
        """Sum the components, or None when any one is unavailable.

        Returning None rather than a partial sum is the point: a ledger
        missing its network term must not read as a cheaper design.
        """
        if not self.available:
            return None
        return sum(
            float(component.value or 0.0) for component in self.components
        )

    def component(self, component_id: str) -> CostComponent:
        for component in self.components:
            if component.component_id == component_id:
                return component
        raise CostModelError(f"unknown cost component: {component_id}")

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "accounting_unit": self.accounting_unit,
            "cost_equation": (
                "total_cost = service + network + storage + "
                "amortized_materialization + transition"
            ),
            "total_cost": self.total_cost,
            "total_cost_available": self.available,
            "unavailable_components": list(self.unavailable_component_ids),
            "not_applicable_components": list(
                self.not_applicable_component_ids
            ),
            "artifact_transfer_accounted_in": (
                self.artifact_transfer_accounted_in
            ),
            "materialization_amortization_horizon_sessions": (
                self.amortization_horizon_sessions
            ),
            "cost_model_id": self.cost_model_id,
            "cost_model_sha256": self.cost_model_sha256,
            "components": {
                component.component_id: component.to_public_dict()
                for component in self.components
            },
        }


def build_cost_ledger(
    model: CostModel,
    *,
    service_cost: float | None,
    transferred_bytes: float | None,
    stored_bytes: float | None,
    stored_hours: float | None,
    materialized_bytes: float | None,
    transition_bytes: float | None,
    transition_seconds: float | None,
    service_cost_provenance: str = "data_agent_realized_cost",
    unavailable: Mapping[str, str] | None = None,
) -> CostLedger:
    """Convert measured quantities into one additive ledger.

    ``unavailable`` maps a component id to the reason it could not be
    measured; such a component is recorded as unavailable and suppresses the
    total rather than contributing zero.
    """
    missing = dict(unavailable or {})
    unknown = set(missing) - set(COST_COMPONENT_IDS)
    if unknown:
        raise CostModelError(
            "unknown unavailable cost component: " + ", ".join(sorted(unknown))
        )
    transfer_in_service = (
        model.artifact_transfer_accounted_in == "service_cost"
    )

    def build(
        component_id: str,
        *,
        quantity: float | None,
        raw_unit: str,
        rule: str,
        rate: float | None,
        provenance: str,
        kind: str,
    ) -> CostComponent:
        if component_id in missing:
            return _unavailable(
                component_id,
                raw_unit=raw_unit,
                conversion_rule=rule,
                reason=missing[component_id],
                provenance=provenance,
            )
        observed = _quantity(quantity, f"{component_id} raw quantity")
        value = observed if rate is None else observed * rate
        return CostComponent(
            component_id=component_id,
            value=value,
            value_kind=kind,
            raw_quantity=observed,
            raw_unit=raw_unit,
            conversion_rule=rule,
            conversion_rate=rate,
            provenance=provenance,
        )

    service = build(
        "service",
        quantity=service_cost,
        raw_unit=model.accounting_unit,
        rule=(
            "service cost is reported directly in accounting units; it "
            + (
                "INCLUDES artifact transfer"
                if transfer_in_service
                else "EXCLUDES artifact transfer"
            )
        ),
        rate=None,
        provenance=service_cost_provenance,
        kind="measured",
    )

    if transfer_in_service:
        # Artifact bytes are already priced inside the service cost. Charging
        # them again through the network term would double-count the single
        # largest quantity in the whole ledger.
        network = CostComponent(
            component_id="network",
            value=0.0,
            value_kind="derived",
            raw_quantity=(
                None
                if transferred_bytes is None
                else _quantity(transferred_bytes, "transferred_bytes")
            ),
            raw_unit="bytes",
            conversion_rule=(
                "excluded to avoid double counting: artifact transfer is "
                "accounted in service_cost by declaration"
            ),
            conversion_rate=0.0,
            provenance="declared-in-cost-model",
        )
    else:
        network = build(
            "network",
            quantity=transferred_bytes,
            raw_unit="bytes",
            rule="network_cost_per_gib * transferred_bytes / 2**30",
            rate=model.network_cost_per_gib / _BYTES_PER_GIB,
            provenance="data_agent_transfer_telemetry",
            kind="measured",
        )

    if "storage" in missing:
        storage = _unavailable(
            "storage",
            raw_unit="gib_hours",
            conversion_rule="storage_cost_per_gib_hour * gib_hours",
            reason=missing["storage"],
            provenance="pilot_storage_accounting",
        )
    else:
        gib_hours = (
            _quantity(stored_bytes, "stored_bytes")
            / _BYTES_PER_GIB
            * _quantity(stored_hours, "stored_hours")
        )
        storage = CostComponent(
            component_id="storage",
            value=gib_hours * model.storage_cost_per_gib_hour,
            value_kind="derived",
            raw_quantity=gib_hours,
            raw_unit="gib_hours",
            conversion_rule=(
                "storage_cost_per_gib_hour * (stored_bytes / 2**30) * "
                "stored_hours"
            ),
            conversion_rate=model.storage_cost_per_gib_hour,
            provenance="pilot_storage_accounting",
        )

    if "amortized_materialization" in missing:
        materialization = _unavailable(
            "amortized_materialization",
            raw_unit="bytes",
            conversion_rule=(
                "materialization_cost_per_gib * bytes / 2**30 / horizon"
            ),
            reason=missing["amortized_materialization"],
            provenance="pilot_materialization_accounting",
        )
    else:
        observed = _quantity(materialized_bytes, "materialized_bytes")
        horizon = model.materialization_amortization_horizon_sessions
        materialization = CostComponent(
            component_id="amortized_materialization",
            value=(
                observed
                / _BYTES_PER_GIB
                * model.materialization_cost_per_gib
                / horizon
            ),
            value_kind="derived",
            raw_quantity=observed,
            raw_unit="bytes",
            conversion_rule=(
                "materialization_cost_per_gib * materialized_bytes / 2**30 "
                f"/ amortization_horizon_sessions({horizon})"
            ),
            conversion_rate=model.materialization_cost_per_gib,
            provenance="pilot_materialization_accounting",
        )

    if "transition" in missing:
        transition = _unavailable(
            "transition",
            raw_unit="bytes+seconds",
            conversion_rule=(
                "transition_cost_per_gib * bytes / 2**30 + "
                "elapsed_time_cost_per_second * seconds"
            ),
            reason=missing["transition"],
            provenance="pilot_transition_accounting",
        )
    else:
        moved = _quantity(transition_bytes, "transition_bytes")
        seconds = _quantity(transition_seconds, "transition_seconds")
        transition = CostComponent(
            component_id="transition",
            value=(
                moved / _BYTES_PER_GIB * model.transition_cost_per_gib
                + seconds * model.elapsed_time_cost_per_second
            ),
            value_kind="derived",
            raw_quantity=moved,
            raw_unit="bytes+seconds",
            conversion_rule=(
                "transition_cost_per_gib * transition_bytes / 2**30 + "
                "elapsed_time_cost_per_second * transition_seconds"
            ),
            conversion_rate=model.transition_cost_per_gib,
            provenance="pilot_transition_accounting",
        )

    return CostLedger(
        schema_version=COST_LEDGER_SCHEMA_VERSION,
        accounting_unit=model.accounting_unit,
        components=(service, network, storage, materialization, transition),
        cost_model_id=model.cost_model_id,
        cost_model_sha256=model.source_sha256,
        artifact_transfer_accounted_in=model.artifact_transfer_accounted_in,
        amortization_horizon_sessions=(
            model.materialization_amortization_horizon_sessions
        ),
    )


def record_total_cost(record: Mapping[str, Any]) -> float:
    """Read a stored ledger's total, failing closed on any gap.

    Used by the AWM loader when a run selects ``total_cost``. A record with
    no ledger, or one whose ledger is incomplete, raises rather than falling
    back to the service-cost-only value.
    """
    ledger = record.get("cost_ledger")
    if not isinstance(ledger, Mapping):
        raise CostTelemetryIncompleteError(
            "total_cost was requested but this record carries no cost "
            "ledger; service-cost-only data cannot be read as total_cost"
        )
    version = ledger.get("schema_version")
    if version != COST_LEDGER_SCHEMA_VERSION:
        raise CostModelError(
            f"unsupported cost ledger schema_version: {version}"
        )
    if not ledger.get("total_cost_available"):
        raise CostTelemetryIncompleteError(
            "cost ledger is incomplete; unavailable components: "
            + ", ".join(ledger.get("unavailable_components") or ["unknown"])
        )
    total = ledger.get("total_cost")
    if isinstance(total, bool) or not isinstance(total, (int, float)):
        raise CostModelError("cost ledger total_cost must be a number")
    result = float(total)
    if not math.isfinite(result):
        raise CostModelError("cost ledger total_cost must be finite")
    if result < 0.0:
        raise CostModelError("cost ledger total_cost must be non-negative")
    return result


def load_cost_model_file(path: str | Path) -> CostModel:
    """Load a standalone cost-model JSON document."""
    source = Path(path).resolve()
    if not source.is_file():
        raise CostModelError(f"cost model does not exist: {source}")
    try:
        payload = json.loads(source.read_bytes())
    except json.JSONDecodeError as exc:
        raise CostModelError(f"invalid cost model JSON: {source}") from exc
    return load_cost_model(payload, name="cost_model")
