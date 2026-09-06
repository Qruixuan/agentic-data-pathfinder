"""Read-only physical cost reality audit for a frozen distributed pilot.

The pilot reports a large mean cost saving. This module asks a narrower
question than the certificate does: *what is that number made of?*

It separates five things the accounting deliberately keeps in one column:

* the **quote** shown to the Agent, which is a price signal, not a cost;
* the **configured service tariff**, a constant authored in the system
  configuration and returned by the Data Agent;
* the **measured raw resource use** -- bytes, seconds, requests;
* the **normalized cost** derived from raw use and frozen coefficients;
* the **actual monetary expenditure**, which this pilot never observed.

Conflating any two of them turns a configuration choice into an empirical
finding. A latency that is mostly an injected delay is not network latency; a
tariff read back from a service that was told what to charge is not a
measurement of anything physical.

Nothing here re-runs, re-prices, or re-interprets a frozen record. It reads,
classifies, decomposes, and reports. The output is post-hoc engineering
diagnosis and is never confirmatory evidence.
"""

from __future__ import annotations

import csv
import io
import json
import statistics
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable, Mapping, Sequence


COST_REALITY_AUDIT_SCHEMA_VERSION = (
    "pathfinder.distributed-cost-reality-audit/v1alpha1"
)
#: How a reported quantity came to exist. ``measured`` means an instrument
#: observed it; ``configured`` means a human wrote it down.
PROVENANCE_CLASSES = (
    "measured",
    "derived-from-measured",
    "configured",
    "controlled-intervention",
    "injected-or-simulated",
    "missing",
)
#: Concepts that must never be described in each other's language.
COST_CONCEPTS = (
    "quote-shown-to-agent",
    "configured-service-tariff",
    "measured-raw-resource",
    "normalized-derived-cost",
    "actual-monetary-expenditure",
)
BREAK_EVEN_STATUSES = (
    "IDENTIFIED",
    "NOT_IDENTIFIED_MISSING_INPUT",
    "NO_BREAK_EVEN_NONPOSITIVE_SAVING",
    "NOT_COMPARABLE_UNITS",
)
SAFE_DESIGN_ID = "D_origin_remote"
COST_COMPONENTS = (
    "service",
    "network",
    "storage",
    "amortized_materialization",
    "transition",
)


class CostAuditError(ValueError):
    """Raised when the audit cannot proceed safely."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise CostAuditError(message)


def _csv_text(rows: Sequence[Mapping[str, Any]]) -> str:
    """Write rows whose key sets may legitimately differ.

    A summary of an entirely missing quantity has no ``mean`` or ``total``
    key at all -- that absence is the point. Taking field names from the
    first row alone would either crash or silently drop those columns, so
    the union is used, in first-seen order.
    """
    if not rows:
        return ""
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=fieldnames,
        lineterminator="\n",
        restval="",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _json(value: Any) -> str:
    return json.dumps(
        value, indent=2, sort_keys=True, ensure_ascii=False
    ) + "\n"


def verify_snapshot(snapshot_dir: str | Path) -> dict[str, str]:
    """Verify every checksum file the snapshot ships, refusing on mismatch."""
    root = Path(snapshot_dir).resolve()
    _require(root.is_dir(), f"snapshot directory does not exist: {root}")
    verified: dict[str, str] = {}
    checksum_files = sorted(root.rglob("*SHA256SUMS"))
    _require(
        checksum_files,
        "the snapshot ships no SHA256SUMS file; integrity cannot be "
        "established and the audit refuses to read it",
    )
    for checksum_path in checksum_files:
        base = checksum_path.parent
        for line in checksum_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            parts = line.split("  ", 1)
            _require(
                len(parts) == 2,
                f"malformed checksum line in {checksum_path.name}",
            )
            digest, name = parts
            target = (base / name).resolve()
            _require(
                target.is_file(),
                f"{checksum_path.name} references a missing file: {name}",
            )
            actual = sha256(target.read_bytes()).hexdigest()
            _require(
                actual == digest,
                f"snapshot checksum mismatch in {checksum_path.name}: "
                f"{name}",
            )
            verified[str(target.relative_to(root))] = actual
        verified[str(checksum_path.relative_to(root))] = sha256(
            checksum_path.read_bytes()
        ).hexdigest()
    return dict(sorted(verified.items()))


def snapshot_fingerprint(snapshot_dir: str | Path) -> str:
    """A single content fingerprint over every regular file."""
    root = Path(snapshot_dir).resolve()
    digest = sha256()
    for path in sorted(
        (p for p in root.rglob("*") if p.is_file()),
        key=lambda p: p.relative_to(root).as_posix(),
    ):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(sha256(path.read_bytes()).digest())
    return digest.hexdigest()


@dataclass(frozen=True)
class SnapshotInputs:
    """Everything the audit reads, loaded once from the frozen snapshot."""

    root: Path
    canonical_records: list[dict[str, Any]]
    attempt_records: list[dict[str, Any]]
    system_config: dict[str, Any]
    measurements: dict[str, Any]
    endpoint_registry: dict[str, Any]
    plan: dict[str, Any]
    evaluation: dict[str, Any]
    transfer_receipt: dict[str, Any] | None
    local_catalog: dict[str, Any] | None

    @property
    def pilot_id(self) -> str:
        return str(self.plan.get("pilot_id") or "")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    _require(path.is_file(), f"snapshot file is missing: {path.name}")
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _read_json(path: Path, *, required: bool = True) -> dict[str, Any] | None:
    if not path.is_file():
        _require(not required, f"snapshot file is missing: {path.name}")
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_snapshot(snapshot_dir: str | Path) -> SnapshotInputs:
    """Load the frozen artefacts, never falling back to repository config."""
    root = Path(snapshot_dir).resolve()
    config = root / "input-freeze" / "config"
    _require(
        config.is_dir(),
        "the snapshot has no input-freeze/config; the audit will not fall "
        "back to mutable repository configuration",
    )
    return SnapshotInputs(
        root=root,
        canonical_records=_read_jsonl(
            root / "run" / "canonical_records.jsonl"
        ),
        attempt_records=_read_jsonl(root / "run" / "attempt_ledger.jsonl"),
        system_config=_read_json(config / "system.json"),
        measurements=_read_json(config / "measurements.json"),
        endpoint_registry=_read_json(config / "endpoint-registry.json"),
        plan=_read_json(root / "run" / "distributed_pilot_plan.json"),
        evaluation=_read_json(root / "evaluation" / "evaluation.json"),
        transfer_receipt=_read_json(
            root / "input-freeze" / "provenance"
            / "network-transfer-receipt.json",
            required=False,
        ),
        local_catalog=_read_json(
            root / "input-freeze" / "provenance" / "local-v2"
            / "local-object-catalog.json",
            required=False,
        ),
    )


# ---------------------------------------------------------------------------
# 1. Cost provenance classification
# ---------------------------------------------------------------------------
def _configured_path_values(
    system_config: Mapping[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Hand-authored per-design, per-representation constants."""
    table: dict[tuple[str, str], dict[str, Any]] = {}
    for design in system_config.get("physical_designs", []):
        for representation_id, path in (design.get("paths") or {}).items():
            table[(str(design["id"]), str(representation_id))] = dict(path)
    return table


def classify_cost_provenance(
    inputs: SnapshotInputs,
) -> list[dict[str, Any]]:
    """Classify every field that feeds a cost or resource interpretation."""
    configured = _configured_path_values(inputs.system_config)
    accepted = [
        event
        for record in inputs.canonical_records
        for event in record["access_events"]
        if event.get("accepted")
    ]
    _require(accepted, "no accepted access events in the snapshot")

    # Does the observed realized_cost equal the configured constant?
    tariff_matches = all(
        event["realized_cost"]
        == configured[
            (record["design_id"], event["representation_id"])
        ]["realized_cost"]
        for record in inputs.canonical_records
        for event in record["access_events"]
        if event.get("accepted")
    )
    quote_matches = all(
        event["quoted_price"]
        == configured[
            (record["design_id"], event["representation_id"])
        ]["quotes"][record["task_class_id"]]
        for record in inputs.canonical_records
        for event in record["access_events"]
        if event.get("accepted")
    )
    delay_share = [
        event["data_agent_controlled_delay_ms"]
        / event["data_agent_service_latency_ms"]
        for event in accepted
        if event.get("data_agent_service_latency_ms")
    ]
    median_delay_share = (
        statistics.median(delay_share) if delay_share else None
    )

    def row(
        field: str,
        source: str,
        provenance: str,
        concept: str,
        evidence: str,
        *,
        observed: int = 0,
        missing: int = 0,
        note: str = "",
    ) -> dict[str, Any]:
        _require(
            provenance in PROVENANCE_CLASSES,
            f"unknown provenance class: {provenance}",
        )
        _require(concept in COST_CONCEPTS, f"unknown concept: {concept}")
        return {
            "field": field,
            "source": source,
            "provenance": provenance,
            "cost_concept": concept,
            "observed_count": observed,
            "missing_count": missing,
            "evidence": evidence,
            "note": note,
        }

    total = len(accepted)
    rows = [
        row(
            "quoted_price",
            "canonical_records.access_events",
            "controlled-intervention",
            "quote-shown-to-agent",
            "equals system.json physical_designs[].paths[].quotes"
            f"[task_class]: {quote_matches}",
            observed=total,
            note=(
                "A quote is the price signal the Agent optimises against, "
                "and it is varied deliberately across designs: it is the "
                "experimental intervention, not an observation. It is not "
                "a measured cost and never enters the cost ledger."
            ),
        ),
        row(
            "realized_cost",
            "canonical_records.access_events",
            "configured",
            "configured-service-tariff",
            "equals system.json physical_designs[].paths[]."
            f"realized_cost for every accepted access: {tariff_matches}",
            observed=total,
            note=(
                "The cost ledger labels this component value_kind="
                "'measured' with provenance 'data_agent_realized_cost'. "
                "That is measured *as reported by the service*, but the "
                "service returns a constant authored in system.json. It "
                "measures no physical resource."
            ),
        ),
        row(
            "cost_ledger.service.value",
            "canonical_records.cost_ledger",
            "configured",
            "configured-service-tariff",
            "carries realized_cost through unchanged (conversion_rate=null)",
            observed=total,
        ),
        row(
            "bytes_read",
            "canonical_records.access_events",
            "measured",
            "measured-raw-resource",
            "per-access byte count of the served representation payload",
            observed=sum(
                1 for e in accepted if e.get("bytes_read") is not None
            ),
            missing=sum(1 for e in accepted if e.get("bytes_read") is None),
        ),
        row(
            "artifact_bytes_sent",
            "canonical_records.access_events",
            "measured",
            "measured-raw-resource",
            "Data Agent transfer telemetry for completed artifact downloads",
            observed=sum(
                1 for e in accepted
                if e.get("artifact_bytes_sent") is not None
            ),
            missing=sum(
                1 for e in accepted if e.get("artifact_bytes_sent") is None
            ),
        ),
        row(
            "cost_ledger.network.raw_quantity",
            "canonical_records.cost_ledger",
            "measured",
            "measured-raw-resource",
            "transferred bytes; zero by declaration for the local endpoint",
            observed=total,
            note=(
                "The local endpoint declares network_transport='local', so "
                "its bytes are charged zero network cost by contract, not "
                "because no bytes moved."
            ),
        ),
        row(
            "cost_ledger.network.value",
            "canonical_records.cost_ledger",
            "derived-from-measured",
            "normalized-derived-cost",
            "network_cost_per_gib * transferred_bytes / 2**30",
            observed=total,
        ),
        row(
            "data_agent_fetch_latency_ms",
            "canonical_records.access_events",
            "measured",
            "measured-raw-resource",
            "service-side fetch time excluding the controlled delay",
            observed=sum(
                1 for e in accepted
                if e.get("data_agent_fetch_latency_ms") is not None
            ),
        ),
        row(
            "data_agent_controlled_delay_ms",
            "canonical_records.access_events",
            "controlled-intervention",
            "measured-raw-resource",
            "deliberately injected delay from system.json latency_ms and "
            "latency_jitter_ms",
            observed=total,
            note=(
                "This is the experimental intervention itself. It must "
                "never be described as natural network or service latency."
            ),
        ),
        row(
            "data_agent_service_latency_ms",
            "canonical_records.access_events",
            "injected-or-simulated",
            "measured-raw-resource",
            "wall-clock service latency; median controlled-delay share = "
            + (
                f"{median_delay_share:.4f}"
                if median_delay_share is not None
                else "unknown"
            ),
            observed=total,
            note=(
                "Wall clock is genuinely measured, but it is dominated by "
                "the injected delay, so the difference between designs is "
                "an artefact of configuration rather than of placement."
            ),
        ),
        row(
            "felt_latency_ms",
            "canonical_records.access_events",
            "injected-or-simulated",
            "measured-raw-resource",
            "end-to-end latency observed by the gateway, inclusive of the "
            "injected controlled delay",
            observed=total,
        ),
        row(
            "artifact_transfer_latency_ms",
            "canonical_records.access_events",
            "measured",
            "measured-raw-resource",
            "artifact transfer duration reported by the Data Agent",
            observed=sum(
                1 for e in accepted
                if e.get("artifact_transfer_latency_ms") is not None
            ),
        ),
    ]

    # Components sourced from the operator measurement manifest.
    kinds = _measurement_kinds(inputs.measurements)
    for component, fields in (
        ("storage", ("bytes", "hours")),
        ("materialization", ("bytes",)),
        ("transition", ("bytes", "seconds")),
    ):
        for field in fields:
            observed_kinds = kinds.get(component, {})
            # 'not_applicable' says a design incurs none of this quantity,
            # not that the quantity is configured. Letting it into the vote
            # would let a single baseline entry relabel every real
            # measurement, so it is excluded and reported separately.
            voting = {
                kind: count
                for kind, count in observed_kinds.items()
                if kind != "not_applicable"
            }
            dominant = (
                max(sorted(voting), key=voting.get)
                if voting
                else ("configured" if observed_kinds else "missing")
            )
            provenance = {
                "measured": "measured",
                "derived": "derived-from-measured",
                "configured": "configured",
                "not_applicable": "configured",
            }.get(dominant, "missing")
            if component == "storage" and field == "hours":
                # The retention window is predeclared, not observed. Its
                # parent block is 'derived', but the duration itself is a
                # human choice and must not inherit that label.
                window = inputs.measurements.get(
                    "storage_accounting_window_hours"
                )
                provenance = (
                    "configured" if window is not None else provenance
                )
            rows.append(row(
                f"measurements.{component}.{field}",
                "input-freeze/config/measurements.json",
                provenance,
                "measured-raw-resource",
                f"declared kinds across entries: {dict(observed_kinds)}",
                observed=sum(observed_kinds.values()),
                note=(
                    "predeclared accounting window, not an observed "
                    "retention period"
                    if component == "storage" and field == "hours"
                    else (
                        "transition quantities are a proportional "
                        "allocation of one audited bundle transfer, not a "
                        "per-workload measurement"
                        if component == "transition"
                        else ""
                    )
                ),
            ))
    rows.append(row(
        "cost_ledger.amortized_materialization.value",
        "canonical_records.cost_ledger",
        "derived-from-measured",
        "normalized-derived-cost",
        "materialized bytes divided by the declared amortization horizon",
        observed=total,
    ))

    # Quantities the pilot never captured.
    for field, why in (
        ("llm_request_count", "no LLM request counter in any frozen record"),
        ("llm_input_tokens", "no token accounting in any frozen record"),
        ("llm_output_tokens", "no token accounting in any frozen record"),
        ("llm_total_tokens", "no token accounting in any frozen record"),
        ("cpu_seconds", "no CPU accounting in any frozen record"),
        ("gpu_seconds", "no GPU accounting in any frozen record"),
        ("actual_monetary_expenditure",
         "the pilot priced nothing in currency"),
    ):
        rows.append(row(
            field,
            "not present",
            "missing",
            (
                "actual-monetary-expenditure"
                if field == "actual_monetary_expenditure"
                else "measured-raw-resource"
            ),
            why,
            observed=0,
            missing=total,
            note="absent, not zero",
        ))

    failures = [
        r for r in inputs.attempt_records
        if r.get("observation_class") != "canonical"
    ]
    rows.append(row(
        "retry_and_failed_attempt_overhead",
        "run/attempt_ledger.jsonl",
        "measured",
        "measured-raw-resource",
        f"{len(failures)} non-canonical attempts recorded",
        observed=len(failures),
        note=(
            "Recorded but deliberately excluded from canonical cost, so "
            "the reported cost understates total experimental resource use."
        ),
    ))
    return rows


def _measurement_kinds(
    measurements: Mapping[str, Any],
) -> dict[str, dict[str, int]]:
    kinds: dict[str, dict[str, int]] = {}
    for entry in measurements.get("measurements", []):
        for component in ("storage", "materialization", "transition"):
            block = entry.get(component) or {}
            kind = str(block.get("kind", "missing"))
            kinds.setdefault(component, {})
            kinds[component][kind] = kinds[component].get(kind, 0) + 1
    return kinds


# ---------------------------------------------------------------------------
# 2. Paired decomposition
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PairedTrial:
    """One baseline/candidate pair at identical workload and repetition."""

    workload_id: str
    object_id: str
    stratum_id: str
    repetition: int
    candidate_design_id: str
    baseline: dict[str, Any]
    candidate: dict[str, Any]


def _component_values(record: Mapping[str, Any]) -> dict[str, float | None]:
    ledger = record.get("cost_ledger") or {}
    components = ledger.get("components") or {}
    values: dict[str, float | None] = {}
    for name in COST_COMPONENTS:
        block = components.get(name)
        if block is None or not block.get("available", False):
            values[name] = None
        else:
            values[name] = float(block["value"])
    return values


def _raw_resources(record: Mapping[str, Any]) -> dict[str, float | None]:
    accepted = [
        e for e in record.get("access_events", []) if e.get("accepted")
    ]
    if not accepted:
        return {key: None for key in (
            "bytes_read", "artifact_bytes_sent", "network_raw_bytes",
            "fetch_latency_ms", "controlled_delay_ms",
            "service_latency_ms", "felt_latency_ms",
        )}

    def total(field: str) -> float | None:
        values = [e.get(field) for e in accepted]
        if any(value is None for value in values):
            return None
        return float(sum(values))

    ledger = record.get("cost_ledger") or {}
    network = (ledger.get("components") or {}).get("network") or {}
    return {
        "bytes_read": total("bytes_read"),
        "artifact_bytes_sent": total("artifact_bytes_sent"),
        "network_raw_bytes": (
            float(network["raw_quantity"])
            if network.get("raw_quantity") is not None
            else None
        ),
        "fetch_latency_ms": total("data_agent_fetch_latency_ms"),
        "controlled_delay_ms": total("data_agent_controlled_delay_ms"),
        "service_latency_ms": total("data_agent_service_latency_ms"),
        "felt_latency_ms": total("felt_latency_ms"),
    }


def build_pairs(inputs: SnapshotInputs) -> list[PairedTrial]:
    """Pair on exact frozen identity, refusing anything ambiguous."""
    index: dict[tuple[str, int, str], dict[str, Any]] = {}
    for record in inputs.canonical_records:
        _require(
            record.get("experiment_id") == inputs.pilot_id,
            "a canonical record belongs to a different pilot: "
            f"{record.get('experiment_id')}",
        )
        for field in ("workload_id", "repetition", "design_id"):
            _require(
                record.get(field) is not None,
                f"canonical record is missing {field}",
            )
        key = (
            str(record["workload_id"]),
            int(record["repetition"]),
            str(record["design_id"]),
        )
        _require(
            key not in index,
            f"duplicate canonical record for {key}",
        )
        index[key] = record

    pairs: list[PairedTrial] = []
    for (workload_id, repetition, design_id), record in sorted(
        index.items()
    ):
        if design_id == SAFE_DESIGN_ID:
            continue
        baseline = index.get((workload_id, repetition, SAFE_DESIGN_ID))
        _require(
            baseline is not None,
            f"candidate {design_id} at {workload_id} rep {repetition} has "
            "no baseline record; refusing an unpaired comparison",
        )
        _require(
            baseline["object_id"] == record["object_id"]
            and baseline["stratum_id"] == record["stratum_id"],
            f"baseline and candidate disagree on identity at {workload_id}",
        )
        pairs.append(PairedTrial(
            workload_id=workload_id,
            object_id=str(record["object_id"]),
            stratum_id=str(record["stratum_id"]),
            repetition=repetition,
            candidate_design_id=design_id,
            baseline=baseline,
            candidate=record,
        ))
    _require(pairs, "no baseline/candidate pairs could be formed")
    return pairs


def _subtract(
    baseline: float | None,
    candidate: float | None,
) -> float | None:
    """Missing stays missing; it never becomes zero."""
    if baseline is None or candidate is None:
        return None
    return baseline - candidate


def paired_decomposition(pairs: Sequence[PairedTrial]) -> list[dict[str, Any]]:
    """One row per pair with every sign convention applied explicitly."""
    rows: list[dict[str, Any]] = []
    for pair in pairs:
        base_components = _component_values(pair.baseline)
        cand_components = _component_values(pair.candidate)
        base_raw = _raw_resources(pair.baseline)
        cand_raw = _raw_resources(pair.candidate)
        base_total = pair.baseline["cost_ledger"].get("total_cost")
        cand_total = pair.candidate["cost_ledger"].get("total_cost")
        service_saving = _subtract(
            base_components["service"], cand_components["service"]
        )
        total_saving = _subtract(base_total, cand_total)
        row = {
            "workload_id": pair.workload_id,
            "object_id": pair.object_id,
            "stratum_id": pair.stratum_id,
            "repetition": pair.repetition,
            "candidate_design_id": pair.candidate_design_id,
            "baseline_design_id": SAFE_DESIGN_ID,
            # success_delta = candidate - baseline
            "success_delta": (
                float(bool(pair.candidate["task_success"]))
                - float(bool(pair.baseline["task_success"]))
            ),
            # cost_saving = baseline - candidate
            "total_cost_saving": total_saving,
            "configured_service_cost_saving": service_saving,
            "non_service_cost_saving": (
                None
                if total_saving is None or service_saving is None
                else total_saving - service_saving
            ),
        }
        for component in COST_COMPONENTS:
            row[f"{component}_cost_saving"] = _subtract(
                base_components[component], cand_components[component]
            )
        for field in base_raw:
            # raw_resource_delta = candidate - baseline
            row[f"{field}_delta"] = _subtract(
                cand_raw[field], base_raw[field]
            )
            row[f"baseline_{field}"] = base_raw[field]
            row[f"candidate_{field}"] = cand_raw[field]
        rows.append(row)
    return rows


def _summarise(values: Sequence[float | None]) -> dict[str, Any]:
    present = [v for v in values if v is not None]
    summary: dict[str, Any] = {
        "count": len(values),
        "observed": len(present),
        "missing": len(values) - len(present),
    }
    if present:
        summary.update({
            "total": sum(present),
            "mean": statistics.fmean(present),
            "median": statistics.median(present),
            "minimum": min(present),
            "maximum": max(present),
        })
        if len(present) >= 4:
            quantiles = statistics.quantiles(present, n=4)
            summary["quartiles"] = {
                "p25": quantiles[0],
                "p50": quantiles[1],
                "p75": quantiles[2],
            }
    return summary


def _group(
    rows: Sequence[Mapping[str, Any]],
    key: str,
    fields: Sequence[str],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row[key]), []).append(row)
    return {
        name: {
            "pairs": len(members),
            **{
                field: _summarise([m.get(field) for m in members])
                for field in fields
            },
        }
        for name, members in sorted(grouped.items())
    }


# ---------------------------------------------------------------------------
# 3-4. Three layers and the counterfactual decomposition
# ---------------------------------------------------------------------------
SUMMARY_FIELDS = (
    "total_cost_saving",
    "configured_service_cost_saving",
    "non_service_cost_saving",
    "success_delta",
    *(f"{component}_cost_saving" for component in COST_COMPONENTS),
    "bytes_read_delta",
    "artifact_bytes_sent_delta",
    "network_raw_bytes_delta",
    "fetch_latency_ms_delta",
    "controlled_delay_ms_delta",
    "service_latency_ms_delta",
    "felt_latency_ms_delta",
)


def _dominance_ratio(
    total: float | None,
    service: float | None,
) -> dict[str, Any]:
    """Share of the reported saving attributable to configured tariffs.

    Only meaningful when the denominator is non-zero and the two quantities
    do not straddle zero; a ratio across a sign change reads as a percentage
    while meaning nothing.
    """
    if total is None or service is None:
        return {
            "status": "UNDEFINED_MISSING_INPUT",
            "ratio": None,
            "explanation": "a required aggregate was missing",
        }
    if total == 0.0:
        return {
            "status": "UNDEFINED_ZERO_DENOMINATOR",
            "ratio": None,
            "explanation": (
                "the reported total saving is exactly zero, so no share of "
                "it can be attributed"
            ),
        }
    ratio = service / total
    if (service < 0) != (total < 0):
        return {
            "status": "UNDEFINED_SIGN_CANCELLATION",
            "ratio": None,
            "explanation": (
                "the configured service saving and the total saving have "
                "opposite signs, so a share is not interpretable"
            ),
        }
    residual = total - service
    explanation = (
        "share of the reported normalized saving contributed by the "
        "configured service tariff difference"
    )
    if ratio > 1.0:
        explanation = (
            "the configured service tariff difference EXCEEDS the total "
            "reported saving. This is not an ordinary percentage: it "
            "arises because the remaining non-service contribution has "
            f"the opposite sign ({residual:.6g}), so the local designs are "
            "marginally more expensive once the configured tariff is "
            "removed. Read it as 'configured tariff explains the whole "
            "saving and then some', never as 'more than 100% efficient'."
        )
    return {
        "status": "DEFINED",
        "ratio": ratio,
        "explanation": explanation,
        "exceeds_total": ratio > 1.0,
        "non_service_residual": residual,
        "non_service_residual_sign": (
            "negative" if residual < 0 else
            "positive" if residual > 0 else "zero"
        ),
    }


def configured_versus_measured(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Separate the three result layers and never merge their language."""
    overall = {
        field: _summarise([row.get(field) for row in rows])
        for field in SUMMARY_FIELDS
    }
    total_mean = overall["total_cost_saving"].get("mean")
    service_mean = overall["configured_service_cost_saving"].get("mean")
    non_service_mean = overall["non_service_cost_saving"].get("mean")

    by_design_rows = _group(rows, "candidate_design_id", SUMMARY_FIELDS)
    by_design = {}
    for design, block in by_design_rows.items():
        by_design[design] = {
            **block,
            "service_dominance": _dominance_ratio(
                block["total_cost_saving"].get("mean"),
                block["configured_service_cost_saving"].get("mean"),
            ),
            "counterfactual_without_configured_service": {
                "definition": (
                    "reported total cost difference minus the configured "
                    "service-cost difference"
                ),
                "value": block["non_service_cost_saving"].get("mean"),
                "candidate_still_cheaper": (
                    None
                    if block["non_service_cost_saving"].get("mean") is None
                    else block["non_service_cost_saving"]["mean"] > 0.0
                ),
                "is_post_hoc_decomposition_not_a_rerun": True,
                "is_causal_estimate": False,
            },
        }
    return {
        "layer_1_raw_measured_resources": {
            "description": (
                "directly observed bytes, seconds and requests; no "
                "coefficient applied"
            ),
            "unit_note": "bytes and milliseconds",
            "fields": {
                field: overall[field]
                for field in SUMMARY_FIELDS
                if field.endswith("_delta")
            },
        },
        "layer_2_frozen_normalized_accounting": {
            "description": (
                "the frozen five-component pilot-cost-unit accounting; "
                "normalized accounting, not money"
            ),
            "unit": "pilot-cost-unit",
            "total_normalized_cost_saving": total_mean,
            "components": {
                f"{component}_cost_saving": overall[
                    f"{component}_cost_saving"
                ]
                for component in COST_COMPONENTS
            },
        },
        "layer_3_configured_service_cost_scenario": {
            "description": (
                "the portion caused by manually configured representation "
                "service tariffs"
            ),
            "configured_service_cost_saving": service_mean,
            "non_service_normalized_cost_saving": non_service_mean,
            "non_service_is_still_normalized_accounting_not_money": True,
        },
        "headline": {
            "total_normalized_cost_saving": total_mean,
            "configured_service_cost_saving": service_mean,
            "non_service_normalized_cost_saving": non_service_mean,
            "service_dominance": _dominance_ratio(total_mean, service_mean),
        },
        "overall": overall,
        "by_design": by_design,
        "by_stratum": _group(rows, "stratum_id", SUMMARY_FIELDS),
        "by_repetition_diagnostic": _group(
            rows, "repetition", SUMMARY_FIELDS
        ),
        "actual_monetary_expenditure": {
            "observed": False,
            "note": (
                "No currency amount was recorded anywhere in the pilot. "
                "Every figure above is normalized accounting."
            ),
        },
    }


# ---------------------------------------------------------------------------
# 5. Break-even analysis
# ---------------------------------------------------------------------------
def _break_even(
    *,
    design: str,
    scope: str,
    numerator: float | None,
    numerator_provenance: str,
    denominator: float | None,
    denominator_provenance: str,
    unit: str,
    assumptions: Sequence[str],
    comparable: bool = True,
    denominator_is_injected: bool = False,
    reuse_unit: str = "not-applicable",
    allocation_method: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not comparable:
        status, sessions = "NOT_COMPARABLE_UNITS", None
    elif numerator is None or denominator is None:
        status, sessions = "NOT_IDENTIFIED_MISSING_INPUT", None
    elif denominator_is_injected:
        status, sessions = "NOT_IDENTIFIED_MISSING_INPUT", None
    elif denominator <= 0.0:
        status, sessions = "NO_BREAK_EVEN_NONPOSITIVE_SAVING", None
    else:
        status = "IDENTIFIED"
        sessions = numerator / denominator
    _require(status in BREAK_EVEN_STATUSES, f"bad status {status}")
    return {
        "design_id": design,
        "scope": scope,
        "numerator": numerator,
        "numerator_provenance": numerator_provenance,
        "denominator_per_session": denominator,
        "denominator_provenance": denominator_provenance,
        "unit": unit,
        "estimated_reuse_sessions_to_break_even": sessions,
        "reuse_unit": reuse_unit,
        "one_time_allocation_method": allocation_method,
        "assumptions": list(assumptions),
        "is_monetary_break_even": False,
        "is_latency_or_time_break_even": False,
        "proves_lower_total_resource_cost": False,
        "interpretation_limits": [
            "This is a BYTE-VOLUME break-even only where the unit is "
            "bytes: how many sessions of avoided cross-boundary transfer "
            "repay the one-time bundle bytes attributed to the design.",
            "It is NOT a monetary break-even; the pilot priced nothing "
            "in currency.",
            "It is NOT a latency or wall-clock break-even; the observed "
            "latency difference is dominated by an injected delay.",
            "It does NOT prove lower total resource cost: storage, "
            "materialization and normalized non-service accounting all "
            "move against the candidate.",
        ],
        "status": status,
        **dict(extra or {}),
        "detail": {
            "NOT_COMPARABLE_UNITS": (
                "numerator and denominator are not in the same unit"
            ),
            "NOT_IDENTIFIED_MISSING_INPUT": (
                "a required quantity is missing, or the recurring saving "
                "is an injected delay rather than a physical resource"
            ),
            "NO_BREAK_EVEN_NONPOSITIVE_SAVING": (
                "the recurring saving is zero or negative, so no reuse "
                "count recovers the one-time cost"
            ),
            "IDENTIFIED": "break-even reuse count computed",
        }[status],
    }


def break_even_analysis(
    inputs: SnapshotInputs,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Break-even only where units match and the saving is positive."""
    by_design = _group(rows, "candidate_design_id", SUMMARY_FIELDS)
    one_time = _one_time_costs(inputs)
    results: list[dict[str, Any]] = []
    for design, block in by_design.items():
        transferred = one_time.get(design, {})
        # Recurring remote bytes avoided per session, from measurement.
        bytes_avoided = block["network_raw_bytes_delta"].get("mean")
        recurring_bytes = (
            None if bytes_avoided is None else -bytes_avoided
        )
        allocation = _allocation_method(inputs)
        objects = transferred.get("object_count") or 0
        per_object = (
            transferred["transition_bytes"] / objects
            if transferred.get("transition_bytes") is not None and objects
            else None
        )
        results.append(_break_even(
            design=design,
            scope=(
                "bytes: design-wide one-time bundle share vs "
                "cross-boundary bytes avoided per session"
            ),
            numerator=transferred.get("transition_bytes"),
            numerator_provenance=(
                "measurements.json transition.bytes summed over every "
                f"entry for this design ({transferred.get('entry_count')} "
                "entries). The per-design sums recover the audited bundle "
                "size exactly, so this is that design's share of the "
                "one-time transfer."
            ),
            denominator=recurring_bytes,
            denominator_provenance=(
                "mean per-session reduction in cost-ledger network "
                "raw_quantity, i.e. bytes that no longer cross the node "
                "boundary because the representation is served locally"
            ),
            unit="bytes",
            reuse_unit="aggregate-pilot-sessions-for-this-design",
            allocation_method=allocation,
            assumptions=[
                "one representation fetch per session",
                "the numerator is design-wide across all its materialized "
                "objects while the denominator is a single session, so the "
                "result counts aggregate sessions, NOT reuse per object",
                "the local endpoint declares network_transport=local, so "
                "its bytes are charged zero network cost by contract; the "
                "bytes still move, they simply stop crossing hosts",
            ],
            extra={
                "materialized_object_count": objects or None,
                "one_time_bytes_per_materialized_object": per_object,
                "observed_sessions_for_this_design": (
                    transferred.get("observed_sessions")
                ),
            },
        ))
        results.append(_break_even(
            design=design,
            scope=(
                "bytes: per-materialized-object one-time share vs "
                "cross-boundary bytes avoided per access"
            ),
            numerator=per_object,
            numerator_provenance=(
                "design-wide bundle share divided by the number of "
                "distinct materialized objects for this design"
            ),
            denominator=recurring_bytes,
            denominator_provenance=(
                "mean per-session reduction in cross-boundary bytes"
            ),
            unit="bytes",
            reuse_unit="reuse-accesses-per-materialized-object",
            allocation_method=allocation,
            assumptions=[
                "each materialized object is re-accessed independently",
                "every access avoids the same cross-boundary volume",
            ],
            extra={"materialized_object_count": objects or None},
        ))
        results.append(_break_even(
            design=design,
            scope="time: one-time transition seconds vs latency saved",
            numerator=transferred.get("transition_seconds"),
            numerator_provenance=(
                "measurements.json transition.seconds, which the manifest "
                "states includes SSH authentication and operator overhead"
            ),
            denominator=(
                None
                if block["felt_latency_ms_delta"].get("mean") is None
                else -block["felt_latency_ms_delta"]["mean"] / 1000.0
            ),
            denominator_provenance=(
                "mean felt-latency reduction per session, which is "
                "dominated by the injected controlled delay"
            ),
            unit="seconds",
            assumptions=[
                "latency saving would have to be physical to be bankable",
            ],
            denominator_is_injected=True,
        ))
        results.append(_break_even(
            design=design,
            scope=(
                "normalized: one-time non-service cost vs recurring "
                "non-service saving"
            ),
            numerator=transferred.get("normalized_one_time"),
            numerator_provenance=(
                "summed transition and amortized-materialization ledger "
                "components carried by the candidate"
            ),
            denominator=block["non_service_cost_saving"].get("mean"),
            denominator_provenance=(
                "mean normalized saving after removing the configured "
                "service tariff difference"
            ),
            unit="pilot-cost-unit",
            assumptions=[
                "frozen conversion coefficients are held fixed",
                "still normalized accounting, not money",
            ],
        ))
        results.append(_break_even(
            design=design,
            scope="mixed-unit control: bytes numerator vs seconds saving",
            numerator=transferred.get("transition_bytes"),
            numerator_provenance="measurements.json transition.bytes",
            denominator=(
                None
                if block["felt_latency_ms_delta"].get("mean") is None
                else -block["felt_latency_ms_delta"]["mean"] / 1000.0
            ),
            denominator_provenance="latency seconds",
            unit="bytes-per-second (incoherent)",
            assumptions=["deliberately refused: unlike units"],
            comparable=False,
        ))
    return {
        "results": results,
        "statuses": list(BREAK_EVEN_STATUSES),
        "note": (
            "A break-even point is reported only when the recurring saving "
            "is a positive, physically measured quantity in the same unit "
            "as the one-time cost."
        ),
        "scope_warning": (
            "Every IDENTIFIED result here is a BYTE-VOLUME break-even and "
            "nothing else. It answers 'how many accesses of avoided "
            "cross-boundary transfer repay the one-time bundle bytes', not "
            "'when does this become cheaper'. It is not monetary, not "
            "latency-based, and does not establish lower total resource "
            "cost -- storage, materialization and the normalized "
            "non-service accounting all move against the candidate."
        ),
        "reuse_units": {
            "aggregate-pilot-sessions-for-this-design": (
                "numerator is the design-wide bundle share across all its "
                "materialized objects; denominator is one session"
            ),
            "reuse-accesses-per-materialized-object": (
                "numerator is one object's share of the bundle; "
                "denominator is one access of that object"
            ),
        },
    }


def _allocation_method(inputs: SnapshotInputs) -> str:
    """The manifest's own words for how the bundle was split."""
    block = inputs.measurements.get("transition_allocation") or {}
    method = block.get("method")
    scope = block.get("elapsed_time_scope")
    bundle = block.get("bundle_bytes")
    return (
        f"method={method!r}; bundle_bytes={bundle}; "
        f"elapsed_time_scope={scope!r}"
    )


def _one_time_costs(inputs: SnapshotInputs) -> dict[str, dict[str, float]]:
    """Per-design one-time materialization and transition quantities."""
    totals: dict[str, dict[str, float]] = {}
    objects: dict[str, set[str]] = {}
    entries: dict[str, int] = {}
    for entry in inputs.measurements.get("measurements", []):
        design = str(entry.get("design_id"))
        block = totals.setdefault(design, {
            "transition_bytes": 0.0,
            "transition_seconds": 0.0,
            "materialization_bytes": 0.0,
        })
        transition = entry.get("transition") or {}
        if transition.get("kind") in ("measured", "derived"):
            entries[design] = entries.get(design, 0) + 1
            object_id = str(entry.get("object_id") or "*")
            if object_id != "*":
                objects.setdefault(design, set()).add(object_id)
            block["transition_bytes"] += float(transition.get("bytes") or 0.0)
            block["transition_seconds"] += float(
                transition.get("seconds") or 0.0
            )
        materialization = entry.get("materialization") or {}
        if materialization.get("kind") in ("measured", "derived"):
            block["materialization_bytes"] += float(
                materialization.get("bytes") or 0.0
            )
    # Normalized one-time cost carried in the candidate ledgers.
    for design in list(totals):
        normalized = [
            (record["cost_ledger"]["components"]["transition"]["value"]
             + record["cost_ledger"]["components"][
                 "amortized_materialization"]["value"])
            for record in inputs.canonical_records
            if record["design_id"] == design
        ]
        totals[design]["normalized_one_time"] = (
            statistics.fmean(normalized) if normalized else 0.0
        )
        totals[design]["object_count"] = len(objects.get(design, ()))
        totals[design]["entry_count"] = entries.get(design, 0)
        totals[design]["observed_sessions"] = sum(
            1 for record in inputs.canonical_records
            if record["design_id"] == design
        )
    return totals


# ---------------------------------------------------------------------------
# 6. Physical data-path inventory
# ---------------------------------------------------------------------------
def physical_data_path(inputs: SnapshotInputs) -> dict[str, Any]:
    """Describe which bytes actually crossed the node boundary, and when."""
    endpoints = {
        str(e["endpoint_id"]): e
        for e in inputs.endpoint_registry.get("endpoints", [])
    }
    remote_ids = {
        endpoint_id
        for endpoint_id, entry in endpoints.items()
        if entry.get("network_transport") == "remote"
    }
    per_route: dict[tuple[str, str, str], dict[str, Any]] = {}
    for record in inputs.canonical_records:
        for event in record["access_events"]:
            if not event.get("accepted"):
                continue
            key = (
                str(record["design_id"]),
                str(event["representation_id"]),
                str(event["endpoint_id"]),
            )
            block = per_route.setdefault(key, {
                "accesses": 0,
                "bytes_read": [],
                "artifact_bytes_sent": [],
            })
            block["accesses"] += 1
            if event.get("bytes_read") is not None:
                block["bytes_read"].append(float(event["bytes_read"]))
            if event.get("artifact_bytes_sent") is not None:
                block["artifact_bytes_sent"].append(
                    float(event["artifact_bytes_sent"])
                )

    routes = []
    execution_bytes_remote = 0.0
    for (design, representation, endpoint_id), block in sorted(
        per_route.items()
    ):
        entry = endpoints.get(endpoint_id, {})
        crosses = endpoint_id in remote_ids
        payload = sum(block["bytes_read"])
        if crosses:
            execution_bytes_remote += payload
        routes.append({
            "design_id": design,
            "representation_id": representation,
            "endpoint_id": endpoint_id,
            "source_node_id": entry.get("node_id"),
            "source_location": entry.get("location"),
            "destination_execution_node_id": (
                inputs.endpoint_registry.get("execution_node_id")
            ),
            "network_transport": entry.get("network_transport"),
            "crosses_node_boundary_during_execution": crosses,
            "accesses": block["accesses"],
            "payload_bytes_total": payload,
            "payload_bytes_summary": _summarise(block["bytes_read"]),
            "artifact_bytes_sent_total": sum(block["artifact_bytes_sent"]),
        })

    receipt = inputs.transfer_receipt or {}
    artifact_types = _artifact_types(inputs)
    return {
        "execution_node_id": inputs.endpoint_registry.get(
            "execution_node_id"
        ),
        "endpoints": [
            {
                "endpoint_id": endpoint_id,
                "node_id": entry.get("node_id"),
                "location": entry.get("location"),
                "network_transport": entry.get("network_transport"),
                "network_zero_justification": entry.get(
                    "network_zero_justification"
                ),
            }
            for endpoint_id, entry in sorted(endpoints.items())
        ],
        "placement": inputs.endpoint_registry.get("placement", []),
        "routes": routes,
        "artifact_types": artifact_types,
        "bytes_crossing_nodes_during_execution": {
            "total_bytes": execution_bytes_remote,
            "mebibytes": execution_bytes_remote / (1024 ** 2),
            "definition": (
                "summed payload bytes served by endpoints declared "
                "network_transport=remote, across every accepted access in "
                "every canonical trial"
            ),
        },
        "bytes_crossing_nodes_once_before_execution": {
            "archive_name": receipt.get("archive_name"),
            "archive_size_bytes": receipt.get("archive_size_bytes"),
            "mebibytes": (
                receipt["archive_size_bytes"] / (1024 ** 2)
                if receipt.get("archive_size_bytes") is not None
                else None
            ),
            "elapsed_seconds": receipt.get("elapsed_seconds"),
            "source_node_id": receipt.get("source_node_id"),
            "destination_node_id": receipt.get("destination_node_id"),
            "measurement_scope": receipt.get("measurement_scope"),
            "occurred": "offline-operator-materialization-transfer",
        },
        "operations_offline": [
            "representation generation",
            "one-time materialization bundle transfer luyao2 -> luyao3",
            "storage-window and transition allocation bookkeeping",
        ],
        "operations_during_pilot_execution": [
            "Data Agent representation access",
            "artifact download for sampled_frames",
            "FlowMesh Agent task execution",
        ],
        "costs_excluded_from_the_live_experiment": [
            "actual monetary expenditure (never priced)",
            "LLM request and token accounting (never captured)",
            "CPU/GPU execution time (never captured)",
            "retry and failed-attempt overhead (recorded but excluded "
            "from canonical cost)",
        ],
    }


def _artifact_types(inputs: SnapshotInputs) -> dict[str, Any]:
    """What the Agent actually received, from the frozen catalog."""
    catalog = inputs.local_catalog or {}
    suffixes: dict[str, int] = {}
    for entry in (catalog.get("objects") or {}).values():
        for representation, block in (
            entry.get("representations") or {}
        ).items():
            suffix = Path(str(block.get("path", ""))).suffix or "(none)"
            suffixes[f"{representation}{suffix}"] = (
                suffixes.get(f"{representation}{suffix}", 0) + 1
            )
    declared = {
        str(r["id"]): r.get("size_bytes")
        for r in inputs.system_config.get("representations", [])
    }
    observed: dict[str, list[float]] = {}
    for record in inputs.canonical_records:
        for event in record["access_events"]:
            if event.get("accepted") and event.get("bytes_read") is not None:
                observed.setdefault(
                    str(event["representation_id"]), []
                ).append(float(event["bytes_read"]))
    return {
        "materialized_file_suffixes": dict(sorted(suffixes.items())),
        "declared_size_bytes_in_system_config": declared,
        "observed_payload_bytes": {
            name: _summarise(values)
            for name, values in sorted(observed.items())
        },
        "interpretation": (
            "The Agent received JSON text: a structured description of "
            "sampled frames and a structured temporal digest. It did not "
            "receive raw video, encoded images, or frame pixel bundles."
        ),
        "declared_versus_observed_note": (
            "system.json declares nominal representation sizes in the "
            "megabyte range; the payloads actually served are kilobytes. "
            "The declared sizes are a modelling parameter, not a "
            "description of the bytes that moved."
        ),
    }


# ---------------------------------------------------------------------------
# 7. Orchestration, report and manifest
# ---------------------------------------------------------------------------
def _report(
    inputs: SnapshotInputs,
    provenance: Sequence[Mapping[str, Any]],
    layers: Mapping[str, Any],
    breakeven: Mapping[str, Any],
    path: Mapping[str, Any],
    pairs: int,
) -> str:
    headline = layers["headline"]
    dominance = headline["service_dominance"]
    by_class: dict[str, list[str]] = {}
    for row in provenance:
        by_class.setdefault(row["provenance"], []).append(row["field"])
    crossing = path["bytes_crossing_nodes_during_execution"]
    one_time = path["bytes_crossing_nodes_once_before_execution"]

    def money(value: float | None) -> str:
        return "unavailable" if value is None else f"{value:.6g}"

    lines = [
        "# Physical cost reality audit",
        "",
        f"Pilot: `{inputs.pilot_id}`  ",
        f"Paired baseline/candidate trials: {pairs}  ",
        "Status: **post-hoc engineering diagnosis, not scientific "
        "evidence**",
        "",
        "## 1. What was actually measured?",
        "",
        "Directly observed: "
        + ", ".join(f"`{f}`" for f in by_class.get("measured", []))
        + ".",
        "",
        "Derived from measured quantities using frozen coefficients: "
        + ", ".join(
            f"`{f}`" for f in by_class.get("derived-from-measured", [])
        )
        + ".",
        "",
        "## 2. What was manually configured?",
        "",
        "Hand-authored constants in `system.json`: "
        + ", ".join(f"`{f}`" for f in by_class.get("configured", []))
        + ".",
        "",
        "The service tariff is the important one. The cost ledger records "
        "it as `value_kind: measured` with provenance "
        "`data_agent_realized_cost`, and that is accurate in a narrow "
        "sense -- the value was read back from the Data Agent. But the "
        "Data Agent returns a constant that a human wrote into the "
        "configuration. It measures no physical resource.",
        "",
        "## 3. What was injected or simulated?",
        "",
        "`data_agent_controlled_delay_ms` is the experimental "
        "intervention itself, drawn from the configured `latency_ms` and "
        "`latency_jitter_ms`. Because it dominates service and felt "
        "latency, the latency difference between designs is a property of "
        "the configuration, not of the physical placement.",
        "",
        "## 4. What information is missing?",
        "",
        "Absent -- not zero: "
        + ", ".join(f"`{f}`" for f in by_class.get("missing", []))
        + ".",
        "",
        "## 5. How much of the reported cost difference came from "
        "configured service cost?",
        "",
        f"- total normalized cost saving (mean per pair): "
        f"{money(headline['total_normalized_cost_saving'])}",
        f"- configured service-cost saving: "
        f"{money(headline['configured_service_cost_saving'])}",
        f"- service dominance: {dominance['status']}"
        + (
            f", ratio {dominance['ratio']:.6g}"
            if dominance.get("ratio") is not None
            else ""
        ),
        "",
        "## 6. What remains after removing that configured component?",
        "",
        f"Non-service normalized saving: "
        f"{money(headline['non_service_normalized_cost_saving'])} "
        "pilot-cost-units per paired session. This is still normalized "
        "accounting, not money.",
        "",
        "## 7. Are raw physical savings distinguishable from noise?",
        "",
    ]
    raw = layers["layer_1_raw_measured_resources"]["fields"]
    for field in (
        "network_raw_bytes_delta",
        "bytes_read_delta",
        "artifact_bytes_sent_delta",
        "fetch_latency_ms_delta",
    ):
        block = raw.get(field, {})
        lines.append(
            f"- `{field}`: mean {money(block.get('mean'))}, "
            f"min {money(block.get('minimum'))}, "
            f"max {money(block.get('maximum'))} "
            f"({block.get('observed', 0)} observed, "
            f"{block.get('missing', 0)} missing)"
        )
    lines += [
        "",
        "## 8. Is a defensible break-even point identifiable?",
        "",
        "Every identified result below is a **byte-volume** break-even "
        "only. It is not monetary, not latency-based, and does not "
        "establish lower total resource cost.",
        "",
        "| design | scope | status | value | reuse unit |",
        "|---|---|---|---|---|",
    ]
    for result in breakeven["results"]:
        sessions = result["estimated_reuse_sessions_to_break_even"]
        lines.append(
            f"| {result['design_id']} | {result['scope'].split(':')[0]} "
            f"| {result['status']} "
            f"| {'n/a' if sessions is None else f'{sessions:.6g}'} "
            f"| {result['reuse_unit']} |"
        )
    lines += [
        "",
        breakeven["scope_warning"],
    ]
    lines += [
        "",
        "## 9. Does the substrate transfer meaningful data volumes?",
        "",
        f"- during execution, across all {pairs * 2} canonical trials, "
        f"bytes served by remote endpoints: "
        f"{crossing['total_bytes']:.0f} bytes "
        f"({crossing['mebibytes']:.4f} MiB)",
        f"- one-time operator materialization transfer: "
        f"{one_time.get('archive_size_bytes')} bytes "
        + (
            f"({one_time['mebibytes']:.4f} MiB)"
            if one_time.get("mebibytes") is not None
            else ""
        ),
        "",
        path["artifact_types"]["interpretation"],
        "",
        path["artifact_types"]["declared_versus_observed_note"],
        "",
        "## 10. Recommendation (diagnostic)",
        "",
        "This is a diagnostic recommendation for experimental design. It "
        "is not a scientific conclusion and it certifies nothing.",
        "",
        "The reported saving is, on this evidence, an artefact of the "
        "configured representation service tariff rather than a "
        "measurement of physical placement benefit. The bytes that cross "
        "the node boundary during execution are kilobytes per access, the "
        "latency difference is injected, and the non-service normalized "
        "difference is small and of the opposite sign to the headline.",
        "",
        "A next pilot that wanted to measure placement would need to make "
        "the physical path carry the effect: match the quote and tariff "
        "across designs so they cannot drive the difference, remove or "
        "separately account for the injected delay, and move "
        "representations whose size makes transfer time observable. "
        "Retaining the current lightweight framing is defensible only if "
        "the object of study is explicitly the Agent's response to price "
        "signals rather than the physics of data placement.",
        "",
    ]
    return "\n".join(lines)


def audit_distributed_cost_reality(
    snapshot_dir: str | Path,
    *,
    output_dir: str | Path,
    audit_git_revision: str | None = None,
) -> dict[str, Any]:
    """Run the read-only audit and publish it atomically."""
    snapshot = Path(snapshot_dir).resolve()
    target = Path(output_dir).resolve()
    _require(
        not target.exists(),
        f"audit output directory already exists: {target}",
    )
    _require(
        snapshot not in target.parents and target != snapshot,
        "the audit output must live outside the frozen snapshot",
    )

    verified_before = verify_snapshot(snapshot)
    fingerprint_before = snapshot_fingerprint(snapshot)

    inputs = load_snapshot(snapshot)
    provenance = classify_cost_provenance(inputs)
    pairs = build_pairs(inputs)
    rows = paired_decomposition(pairs)
    layers = configured_versus_measured(rows)
    breakeven = break_even_analysis(inputs, rows)
    path = physical_data_path(inputs)

    resource_rows = [
        {
            "quantity": field,
            **{
                key: value
                for key, value in _summarise(
                    [row.get(field) for row in rows]
                ).items()
                if key != "quartiles"
            },
        }
        for field in SUMMARY_FIELDS
    ]

    documents: dict[str, str] = {
        "cost_provenance.csv": _csv_text(provenance),
        "measured_resource_summary.csv": _csv_text(resource_rows),
        "paired_cost_decomposition.csv": _csv_text(rows),
        "configured_vs_measured_cost.json": _json(layers),
        "break_even_analysis.json": _json(breakeven),
        "physical_data_path.json": _json(path),
        "audit_summary.md": _report(
            inputs, provenance, layers, breakeven, path, len(rows)
        ),
    }

    completeness = {
        row["field"]: {
            "observed": row["observed_count"],
            "missing": row["missing_count"],
            "provenance": row["provenance"],
        }
        for row in provenance
    }
    manifest = {
        "schema_version": COST_REALITY_AUDIT_SCHEMA_VERSION,
        "audit_status": "COMPLETE",
        "pilot_id": inputs.pilot_id,
        "audit_code_git_revision": audit_git_revision,
        "snapshot_content_fingerprint": fingerprint_before,
        "snapshot_verified_file_sha256": verified_before,
        "record_counts": {
            "canonical_records": len(inputs.canonical_records),
            "attempt_records": len(inputs.attempt_records),
            "paired_trials": len(rows),
            "measurement_entries": len(
                inputs.measurements.get("measurements", [])
            ),
        },
        "completeness_summary": completeness,
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "deployment_mutations_performed": False,
        "credentials_recorded": False,
        "read_only": True,
        "snapshot_modified": False,
        "output_file_sha256": {
            name: sha256(content.encode("utf-8")).hexdigest()
            for name, content in sorted(documents.items())
        },
    }
    documents["audit_manifest.json"] = _json(manifest)
    documents["SHA256SUMS"] = "".join(
        f"{sha256(content.encode('utf-8')).hexdigest()}  {name}\n"
        for name, content in sorted(documents.items())
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(
        prefix=".pathfinder-cost-audit-",
        dir=target.parent,
    ) as temporary:
        staging = Path(temporary) / "audit"
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_text(
                content, encoding="utf-8", newline="\n"
            )
        _require(
            not target.exists(),
            f"audit output directory already exists: {target}",
        )
        staging.rename(target)

    # Verify the published checksums, then re-verify the snapshot.
    for line in (target / "SHA256SUMS").read_text(
        encoding="utf-8"
    ).splitlines():
        digest, name = line.split("  ", 1)
        _require(
            sha256((target / name).read_bytes()).hexdigest() == digest,
            f"published audit checksum mismatch: {name}",
        )
    fingerprint_after = snapshot_fingerprint(snapshot)
    _require(
        fingerprint_after == fingerprint_before,
        "the frozen snapshot changed during the audit",
    )

    headline = layers["headline"]
    return {
        "audit_status": "COMPLETE",
        "pilot_id": inputs.pilot_id,
        "paired_trials": len(rows),
        "total_normalized_cost_saving": headline[
            "total_normalized_cost_saving"
        ],
        "configured_service_cost_saving": headline[
            "configured_service_cost_saving"
        ],
        "non_service_normalized_cost_saving": headline[
            "non_service_normalized_cost_saving"
        ],
        "service_dominance": headline["service_dominance"],
        "bytes_crossing_nodes_during_execution": path[
            "bytes_crossing_nodes_during_execution"
        ]["total_bytes"],
        "break_even_statuses": sorted({
            result["status"] for result in breakeven["results"]
        }),
        "snapshot_fingerprint_before": fingerprint_before,
        "snapshot_fingerprint_after": fingerprint_after,
        "snapshot_modified": False,
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "deployment_mutations_performed": False,
        "credentials_recorded": False,
        "console_only_paths": {"output_dir": str(target)},
    }
