from __future__ import annotations

import json
import os
import random
import uuid
from collections import Counter
from hashlib import sha256
from pathlib import Path
from statistics import mean
from typing import Any

from ..config import load_config
from ..integrations.flowmesh.pilot import (
    _write_csv,
    _write_json,
    load_flowmesh_pilot_config,
)
from ..models import SystemConfig, TaskClass
from ..reduced_oracle.contracts import (
    ReducedOracleConfig,
    load_reduced_oracle_config,
)
from ..synthetic_marker import (
    ORACLE_TABLE_FILENAME,
    SCIENTIFIC_CLAIM_FLAG,
    SYNTHETIC_FLAG,
    SYNTHETIC_MANIFEST_FILENAME,
    SYNTHETIC_TRUTH_FILENAME,
)
from .contracts import (
    SyntheticFixtureConfig,
    SyntheticFixtureConfigError,
    load_synthetic_fixture_config,
)


SYNTHETIC_ORACLE_RECORD_SCHEMA_VERSION = (
    "pathfinder.synthetic-oracle-record/v1alpha1"
)
SYNTHETIC_ORACLE_TRUTH_SCHEMA_VERSION = (
    "pathfinder.synthetic-oracle-truth/v1alpha1"
)
SYNTHETIC_ORACLE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.synthetic-oracle-fixture-manifest/v1alpha1"
)

SYNTHETIC_FIXTURE_STATEMENT = (
    "This directory is a deterministic engineering fixture generated from a "
    "declared synthetic scenario. It is NOT physical Reduced Oracle "
    "evidence: no FlowMesh worker, Data Agent, MCP Gateway, LLM, or "
    "filesystem materialization was involved, and no quantity here was "
    "measured. It exists to exercise the Reduced Oracle output contract, the "
    "AWM envelopes, and the OED controller offline. It must not be used for "
    "any scientific claim. It may be consumed only by `evaluate-awm` and "
    "`run-oed-replay`; `analyze-reduced-oracle` and `run-reduced-oracle` "
    "refuse to run against this directory, because the former would recompute "
    "and overwrite oracle_table.csv from transition records this fixture "
    "deliberately does not have, and the latter would mix measured sessions "
    "into generated ones."
)

SYNTHETIC_CATALOG_VERSION = "synthetic-fixture-catalog"
_SYNTHETIC_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL,
    "https://pathfinder.invalid/synthetic-oracle-fixture",
)


def _fail_closed_output_dir(output_dir: Path) -> None:
    """Refuse to write into a directory that already holds anything.

    Merging a synthetic fixture into a directory that may contain real Oracle
    output is the one mistake that would silently turn a fixture into
    apparent evidence.
    """
    if output_dir.exists():
        if not output_dir.is_dir():
            raise SyntheticFixtureConfigError(
                f"synthetic fixture output path is not a directory: "
                f"{output_dir}"
            )
        if any(output_dir.iterdir()):
            raise SyntheticFixtureConfigError(
                "synthetic fixture output directory is not empty; refusing "
                f"to mix generated fixture data into it: {output_dir}"
            )
    output_dir.mkdir(parents=True, exist_ok=True)


def _write_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(
                    json.dumps(row, ensure_ascii=False, sort_keys=True)
                )
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_domain(
    fixture: SyntheticFixtureConfig,
    oracle: ReducedOracleConfig,
    system: SystemConfig,
    task_class: TaskClass,
) -> None:
    if set(fixture.design_ids) != set(oracle.design_ids):
        raise SyntheticFixtureConfigError(
            "the synthetic fixture and its oracle config declare different "
            "design sets"
        )
    representation_ids = set(task_class.candidate_representations)
    for design_id in fixture.design_ids:
        if design_id not in system.designs:
            raise SyntheticFixtureConfigError(
                f"synthetic design is absent from the system config: "
                f"{design_id}"
            )
        spec = fixture.designs[design_id]
        unknown = set(spec.representation_probabilities) - representation_ids
        if unknown:
            raise SyntheticFixtureConfigError(
                f"{design_id} declares representations outside the task "
                "class: " + ", ".join(sorted(unknown))
            )
        missing = representation_ids - set(spec.representation_probabilities)
        if missing:
            raise SyntheticFixtureConfigError(
                f"{design_id} has no selection probability for: "
                + ", ".join(sorted(missing))
            )
        for representation_id in representation_ids:
            path = system.designs[design_id].paths.get(representation_id)
            if path is None or not path.available:
                raise SyntheticFixtureConfigError(
                    f"{design_id} does not offer an available "
                    f"{representation_id} path"
                )
    safe = fixture.designs[oracle.safe_design_id]
    if safe.forward_transition_cost or safe.restoration_cost:
        raise SyntheticFixtureConfigError(
            "the safe incumbent design cannot declare a forward or "
            "restoration transition cost"
        )


def _session_generator(
    fixture: SyntheticFixtureConfig,
    design_id: str,
    repetition: int,
    workload_id: str,
) -> random.Random:
    """Seed each session independently of iteration order.

    Deriving the stream from the identity of the session rather than from a
    single walked generator keeps the fixture byte-identical even if the
    generation loops are ever reordered or parallelised.
    """
    digest = sha256(
        "|".join(
            (
                fixture.fixture_id,
                str(fixture.seed),
                design_id,
                str(repetition),
                workload_id,
            )
        ).encode("utf-8")
    ).hexdigest()
    return random.Random(int(digest[:16], 16))


def _select(
    generator: random.Random,
    representation_ids: tuple[str, ...],
    probabilities: dict[str, float],
) -> str:
    draw = generator.random()
    cumulative = 0.0
    for representation_id in representation_ids:
        cumulative += probabilities[representation_id]
        if draw < cumulative:
            return representation_id
    return representation_ids[-1]


def _records_for_design(
    fixture: SyntheticFixtureConfig,
    oracle: ReducedOracleConfig,
    system: SystemConfig,
    task_class: TaskClass,
    workloads: tuple[Any, ...],
    design_id: str,
) -> list[dict[str, Any]]:
    spec = fixture.designs[design_id]
    design = system.designs[design_id]
    profile = system.quote_profiles[oracle.quote_profile_id]
    representation_ids = task_class.candidate_representations
    rows: list[dict[str, Any]] = []
    order_index = 0
    offered_quotes = {
        representation_id: profile.quote_for(
            task_class.id,
            representation_id,
            design.paths[representation_id].quotes[task_class.id],
        )
        for representation_id in representation_ids
    }
    for repetition in range(oracle.repetitions):
        for workload in workloads:
            generator = _session_generator(
                fixture,
                design_id,
                repetition,
                workload.id,
            )
            selected = _select(
                generator,
                representation_ids,
                spec.representation_probabilities,
            )
            success = (
                generator.random() < spec.success_probabilities[selected]
            )
            path = design.paths[selected]
            trial_key = (
                f"{workload.id}|{design_id}|{oracle.quote_profile_id}|"
                f"r{repetition:04d}"
            )
            identity = f"{fixture.source_sha256}:{fixture.fixture_id}:{trial_key}"
            session_id = str(uuid.uuid5(_SYNTHETIC_NAMESPACE, identity))
            event_id = str(
                uuid.uuid5(_SYNTHETIC_NAMESPACE, f"{identity}:event:0")
            )
            handle_fingerprint = sha256(
                f"{identity}:artifact".encode("utf-8")
            ).hexdigest()
            rows.append(
                {
                    "schema_version": SYNTHETIC_ORACLE_RECORD_SCHEMA_VERSION,
                    SYNTHETIC_FLAG: True,
                    SCIENTIFIC_CLAIM_FLAG: False,
                    "fixture_id": fixture.fixture_id,
                    "generator": "pathfinder.generate-synthetic-oracle",
                    "measured": False,
                    "task_success_source": (
                        "declared-synthetic-bernoulli-draw"
                    ),
                    "experiment_id": f"{oracle.oracle_id}-{design_id}",
                    "trial_key": trial_key,
                    "trial_id": (
                        f"{fixture.fixture_id}-"
                        f"{sha256(trial_key.encode()).hexdigest()[:12]}"
                    ),
                    "session_id": session_id,
                    "order_index": order_index,
                    "workload_id": workload.id,
                    "object_id": workload.object_id,
                    "question": workload.question,
                    "accepted_answer_substrings": list(
                        workload.accepted_answer_substrings
                    ),
                    "design_id": design_id,
                    "task_class_id": task_class.id,
                    "quote_profile_id": oracle.quote_profile_id,
                    "latency_multiplier": oracle.latency_multiplier,
                    "repetition": repetition,
                    "seed": oracle.base_seed + repetition,
                    "started_at": None,
                    "finished_at": None,
                    "duration_seconds": None,
                    "outcome_type": "completed",
                    "recovered_from_gateway_state": False,
                    "offered_quotes": dict(offered_quotes),
                    "telemetry_complete": True,
                    "workflow_id": None,
                    "task_id": None,
                    "flowmesh_status": None,
                    "final_answer": None,
                    "task_success": success,
                    "access_event_count": 1,
                    "accepted_access_count": 1,
                    "selected_representations": [selected],
                    "access_events": [
                        {
                            "event_id": event_id,
                            "event_index": 0,
                            "accepted": True,
                            "representation_id": selected,
                            "quoted_price": offered_quotes[selected],
                            "realized_cost": path.realized_cost,
                            "location": path.location,
                            "object_id": workload.object_id,
                            "object_catalog_version": (
                                SYNTHETIC_CATALOG_VERSION
                            ),
                            "felt_latency_ms": round(
                                path.latency_ms * oracle.latency_multiplier,
                                6,
                            ),
                            "data_agent_service_latency_ms": path.latency_ms,
                            "artifact_handle_sha256": handle_fingerprint,
                            "artifact_download_request_count": 1,
                            "artifact_full_download_count": 1,
                            "artifact_bytes_sent": system.representations[
                                selected
                            ].size_bytes,
                            "artifact_transfer_latency_ms": path.latency_ms,
                            SYNTHETIC_FLAG: True,
                            SCIENTIFIC_CLAIM_FLAG: False,
                        }
                    ],
                    "artifact_delivery_required": True,
                    "artifact_delivery_complete": True,
                    "artifact_delivery_failure_count": 0,
                    "artifact_delivery_failures": [],
                    "error_type": None,
                    "error_message": None,
                }
            )
            order_index += 1
    return rows


def _design_row(
    fixture: SyntheticFixtureConfig,
    oracle: ReducedOracleConfig,
    system: SystemConfig,
    task_class: TaskClass,
    design_id: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Mirror the real Oracle table row so AWM/OED read it unchanged."""
    spec = fixture.designs[design_id]
    selections = Counter(
        str(record["selected_representations"][0]) for record in records
    )
    success_rate = mean(
        float(bool(record["task_success"])) for record in records
    )
    service_cost = mean(
        float(record["access_events"][0]["realized_cost"])
        for record in records
    )
    task_value = task_class.task_value
    storage_cost = spec.storage_cost
    is_safe = design_id == oracle.safe_design_id
    phi = (
        oracle.horizon_sessions
        * (
            task_value * success_rate
            - system.resource_cost_weight * service_cost
        )
        - storage_cost
    )
    return {
        "design_id": design_id,
        "materialization_decision": oracle.designs[
            design_id
        ].materialization_decision,
        "placement_decision": oracle.designs[design_id].placement_decision,
        "execution_decision": oracle.designs[design_id].execution_decision,
        "recorded_trials": len(records),
        "completed_trials": len(records),
        "evaluable_task_count": len(records),
        "completion_rate": 1.0,
        "telemetry_complete": True,
        "objective_evaluable": True,
        "outcome_counts_json": json.dumps(
            {"completed": len(records)},
            sort_keys=True,
        ),
        "task_success_rate": round(success_rate, 6),
        "mean_realized_service_cost_per_session": round(service_cost, 9),
        "selection_rates_json": json.dumps(
            {
                representation_id: round(count / len(records), 6)
                for representation_id, count in sorted(selections.items())
            },
            sort_keys=True,
        ),
        "materialized_bytes": 0,
        "storage_cost": round(storage_cost, 9),
        "forward_transition_count": 0 if is_safe else 1,
        "forward_transition_cost": round(spec.forward_transition_cost, 9),
        "forward_transition_seconds": 0.0,
        "forward_copied_bytes": 0,
        "restoration_count": 0 if is_safe else 1,
        "restoration_cost": round(spec.restoration_cost, 9),
        "restoration_seconds": 0.0,
        "phi": round(phi, 9),
        "transition_adjusted_phi": round(
            phi - spec.forward_transition_cost,
            9,
        ),
        "probe_adjusted_phi": round(
            phi - spec.forward_transition_cost - spec.restoration_cost,
            9,
        ),
        SYNTHETIC_FLAG: True,
        SCIENTIFIC_CLAIM_FLAG: False,
        "costs_measured": False,
    }


def _truth_entry(
    fixture: SyntheticFixtureConfig,
    oracle: ReducedOracleConfig,
    system: SystemConfig,
    task_class: TaskClass,
    design_id: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    spec = fixture.designs[design_id]
    declared_success = spec.declared_success_rate()
    declared_cost = sum(
        probability
        * system.designs[design_id].paths[representation_id].realized_cost
        for representation_id, probability in (
            spec.representation_probabilities.items()
        )
    )
    selections = Counter(
        str(record["selected_representations"][0]) for record in records
    )
    realized_success = mean(
        float(bool(record["task_success"])) for record in records
    )
    realized_cost = mean(
        float(record["access_events"][0]["realized_cost"])
        for record in records
    )

    def phi(success: float, cost: float) -> float:
        return (
            oracle.horizon_sessions
            * (
                task_class.task_value * success
                - system.resource_cost_weight * cost
            )
            - spec.storage_cost
        )

    return {
        SYNTHETIC_FLAG: True,
        SCIENTIFIC_CLAIM_FLAG: False,
        "design_id": design_id,
        "sessions": len(records),
        "declared_representation_probabilities": dict(
            sorted(spec.representation_probabilities.items())
        ),
        "declared_success_probabilities": dict(
            sorted(spec.success_probabilities.items())
        ),
        "declared_success_rate": round(declared_success, 9),
        "declared_service_cost_per_session": round(declared_cost, 9),
        "declared_phi": round(phi(declared_success, declared_cost), 9),
        "realized_selection_rates": {
            representation_id: round(count / len(records), 9)
            for representation_id, count in sorted(selections.items())
        },
        "realized_success_rate": round(realized_success, 9),
        "realized_service_cost_per_session": round(realized_cost, 9),
        "realized_phi": round(phi(realized_success, realized_cost), 9),
        "storage_cost": spec.storage_cost,
        "forward_transition_cost": spec.forward_transition_cost,
        "restoration_cost": spec.restoration_cost,
    }


def generate_synthetic_oracle_fixture(
    fixture_config: SyntheticFixtureConfig | str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Generate a deterministic multi-candidate Oracle-shaped fixture.

    Writes only inside ``output_dir``. Nothing is submitted, no service is
    contacted, and no file outside the output directory is touched.
    """
    fixture = (
        load_synthetic_fixture_config(fixture_config)
        if isinstance(fixture_config, (str, Path))
        else fixture_config
    )
    oracle = load_reduced_oracle_config(fixture.oracle_config_path)
    pilot = load_flowmesh_pilot_config(oracle.workload_pilot_config_path)
    system = load_config(pilot.system_config_path)
    task_class = system.task_classes[pilot.task_class_id]
    _validate_domain(fixture, oracle, system, task_class)

    output = Path(output_dir).resolve()
    _fail_closed_output_dir(output)

    records_by_design: dict[str, list[dict[str, Any]]] = {}
    record_paths: dict[str, str] = {}
    for design_id in oracle.design_ids:
        records = _records_for_design(
            fixture,
            oracle,
            system,
            task_class,
            pilot.workloads,
            design_id,
        )
        records_by_design[design_id] = records
        path = output / "designs" / design_id / "runs.jsonl"
        _write_jsonl(records, path)
        record_paths[design_id] = path.name

    rows = [
        _design_row(
            fixture,
            oracle,
            system,
            task_class,
            design_id,
            records_by_design[design_id],
        )
        for design_id in oracle.design_ids
    ]
    table_path = output / ORACLE_TABLE_FILENAME
    _write_csv(rows, table_path)

    truth = {
        "schema_version": SYNTHETIC_ORACLE_TRUTH_SCHEMA_VERSION,
        SYNTHETIC_FLAG: True,
        SCIENTIFIC_CLAIM_FLAG: False,
        "fixture_id": fixture.fixture_id,
        "statement": SYNTHETIC_FIXTURE_STATEMENT,
        "scenario_note": fixture.scenario_note,
        "horizon_sessions": oracle.horizon_sessions,
        "task_value": task_class.task_value,
        "resource_cost_weight": system.resource_cost_weight,
        "safe_design_id": oracle.safe_design_id,
        "designs": {
            design_id: _truth_entry(
                fixture,
                oracle,
                system,
                task_class,
                design_id,
                records_by_design[design_id],
            )
            for design_id in oracle.design_ids
        },
    }
    truth["declared_best_design_id_after_forward_transition"] = max(
        oracle.design_ids,
        key=lambda design_id: (
            truth["designs"][design_id]["declared_phi"]
            - fixture.designs[design_id].forward_transition_cost
        ),
    )
    truth["realized_best_design_id_after_forward_transition"] = max(
        oracle.design_ids,
        key=lambda design_id: (
            truth["designs"][design_id]["realized_phi"]
            - fixture.designs[design_id].forward_transition_cost
        ),
    )
    truth_path = output / SYNTHETIC_TRUTH_FILENAME
    _write_json(truth, truth_path)

    manifest = {
        "schema_version": SYNTHETIC_ORACLE_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        SYNTHETIC_FLAG: True,
        SCIENTIFIC_CLAIM_FLAG: False,
        "fixture_kind": fixture.fixture_kind,
        "statement": SYNTHETIC_FIXTURE_STATEMENT,
        "scenario_note": fixture.scenario_note,
        "fixture_id": fixture.fixture_id,
        # Names plus digests, never resolved paths: a fixture manifest must
        # not carry a machine layout or any deployment-specific value.
        "fixture_config_name": fixture.source_path.name,
        "fixture_config_sha256": fixture.source_sha256,
        "oracle_config_name": oracle.source_path.name,
        "oracle_config_sha256": oracle.source_sha256,
        "workload_pilot_config_name": pilot.source_path.name,
        "workload_pilot_config_sha256": pilot.source_sha256,
        "system_config_name": system.source_path.name,
        "system_config_sha256": sha256(
            system.source_path.read_bytes()
        ).hexdigest(),
        "seed": fixture.seed,
        "deterministic": True,
        "wall_clock_recorded": False,
        "design_order": list(oracle.design_ids),
        "safe_design_id": oracle.safe_design_id,
        "design_count": len(oracle.design_ids),
        "representation_ids": list(task_class.candidate_representations),
        "task_class_id": task_class.id,
        "quote_profile_id": oracle.quote_profile_id,
        "repetitions": oracle.repetitions,
        "sessions_per_design_repetition": len(pilot.workloads),
        "records_per_design": {
            design_id: len(records)
            for design_id, records in sorted(records_by_design.items())
        },
        "record_schema_version": SYNTHETIC_ORACLE_RECORD_SCHEMA_VERSION,
        "truth_schema_version": SYNTHETIC_ORACLE_TRUTH_SCHEMA_VERSION,
        "design_records_filename": "runs.jsonl",
        "oracle_table_filename": table_path.name,
        "synthetic_truth_filename": truth_path.name,
        "flowmesh_contacted": False,
        "data_agent_contacted": False,
        "llm_contacted": False,
        "filesystem_materialization_performed": False,
        "costs_measured": False,
        "secrets_recorded": False,
        "do_not_run_against_this_directory": ["analyze-reduced-oracle"],
    }
    manifest_path = output / SYNTHETIC_MANIFEST_FILENAME
    _write_json(manifest, manifest_path)
    return {**manifest, "output_dir": str(output)}
