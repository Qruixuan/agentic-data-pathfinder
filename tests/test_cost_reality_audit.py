"""Physical cost reality audit over a frozen distributed pilot snapshot."""

from __future__ import annotations

import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from pathfinder.distributed import (
    BREAK_EVEN_STATUSES,
    PROVENANCE_CLASSES,
    CostAuditError,
    audit_distributed_cost_reality,
    classify_cost_provenance,
    load_snapshot,
    snapshot_fingerprint,
    verify_snapshot,
)


SAFE = "D_origin_remote"
CANDIDATE = "D_local_frames"
PILOT = "synthetic-cost-pilot"


def _sums(directory: Path) -> None:
    lines = "".join(
        f"{sha256(p.read_bytes()).hexdigest()}  {p.name}\n"
        for p in sorted(directory.iterdir())
        if p.is_file() and p.name != "SHA256SUMS"
    )
    (directory / "SHA256SUMS").write_text(lines, encoding="utf-8")


def _write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=1, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _event(
    *,
    representation: str,
    endpoint: str,
    quoted: float,
    realized: float,
    bytes_read: int,
    artifact_bytes: int,
    delay_ms: float,
    fetch_ms: float,
) -> dict[str, Any]:
    return {
        "accepted": True,
        "event_index": 0,
        "representation_id": representation,
        "endpoint_id": endpoint,
        "source_node_id": (
            "node-origin" if endpoint == "origin_remote" else "node-exec"
        ),
        "source_location": (
            "origin-remote" if endpoint == "origin_remote"
            else "local-materialized"
        ),
        "destination_execution_node_id": "node-exec",
        "quoted_price": quoted,
        "realized_cost": realized,
        "bytes_read": bytes_read,
        "artifact_bytes_sent": artifact_bytes,
        "artifact_download_request_count": 1 if artifact_bytes else 0,
        "artifact_full_download_count": 1 if artifact_bytes else 0,
        "artifact_transfer_latency_ms": 0.3,
        "data_agent_fetch_latency_ms": fetch_ms,
        "data_agent_controlled_delay_ms": delay_ms,
        "data_agent_service_latency_ms": delay_ms + fetch_ms,
        "felt_latency_ms": delay_ms + fetch_ms + 2.0,
        "object_id": "obj-1",
        "data_agent_access_id": "acc-1",
    }


def _ledger(
    *,
    service: float,
    network_bytes: float,
    network_value: float,
    storage: float | None = 0.0,
    materialization: float | None = 0.0,
    transition: float | None = 0.0,
    not_applicable: bool = True,
) -> dict[str, Any]:
    def block(
        name: str,
        value: float | None,
        raw: float,
        unit: str,
        kind: str,
    ) -> dict[str, Any]:
        available = value is not None
        return {
            "component_id": name,
            "available": available,
            "value": 0.0 if value is None else value,
            "raw_quantity": raw,
            "raw_unit": unit,
            "value_kind": kind,
            "conversion_rate": None,
            "conversion_rule": "test fixture",
            "provenance": "test",
            "unavailable_reason": None if available else "missing",
        }

    kind = "not_applicable" if not_applicable else "derived"
    total = None
    values = [service, network_value, storage, materialization, transition]
    if all(v is not None for v in values):
        total = sum(values)
    return {
        "schema_version": "pathfinder.total-cost-ledger/v1alpha1",
        "accounting_unit": "pilot-cost-unit",
        "cost_equation": (
            "total_cost = service + network + storage + "
            "amortized_materialization + transition"
        ),
        "artifact_transfer_accounted_in": "network_cost",
        "materialization_amortization_horizon_sessions": 4,
        "total_cost": total,
        "components": {
            "service": block(
                "service", service, service, "pilot-cost-unit", "measured"
            ),
            "network": block(
                "network", network_value, network_bytes, "bytes", "measured"
            ),
            "storage": block("storage", storage, 0.0, "gib_hours", kind),
            "amortized_materialization": block(
                "amortized_materialization", materialization, 0.0,
                "bytes", kind,
            ),
            "transition": block(
                "transition", transition, 0.0, "bytes+seconds", kind,
            ),
        },
    }


def _record(
    *,
    design: str,
    workload: str,
    repetition: int,
    success: bool,
    event: Mapping[str, Any],
    ledger: Mapping[str, Any],
    stratum: str = "causal",
) -> dict[str, Any]:
    return {
        "schema_version": "pathfinder.distributed-pilot-record/v1alpha1",
        "experiment_id": PILOT,
        "design_id": design,
        "is_safe_design": design == SAFE,
        "workload_id": workload,
        "object_id": event["object_id"],
        "stratum_id": stratum,
        "repetition": repetition,
        "task_class_id": "video_qa",
        "task_success": success,
        "telemetry_complete": True,
        "artifact_delivery_complete": True,
        "outcome_type": "completed",
        "trial_key": f"{PILOT}|{workload}|{design}|r{repetition}",
        "access_events": [dict(event)],
        "cost_ledger": dict(ledger),
    }


def build_snapshot(
    root: Path,
    *,
    workloads: int = 3,
    repetitions: int = 2,
    safe_service: float = 0.9,
    candidate_service: float = 0.15,
    candidate_network_bytes: float = 0.0,
    candidate_storage: float | None = 6.4e-05,
    candidate_transition: float | None = 9.2e-04,
    candidate_materialization: float | None = 3.7e-08,
    drop_baseline: bool = False,
    duplicate: bool = False,
    foreign_pilot: bool = False,
    transition_bytes: float = 822.9,
    transition_seconds: float = 0.0142,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    for index in range(workloads):
        workload = f"causal-w{index:02d}"
        for repetition in range(repetitions):
            safe_event = _event(
                representation="sampled_frames",
                endpoint="origin_remote",
                quoted=4.0, realized=safe_service,
                bytes_read=5738, artifact_bytes=5738,
                delay_ms=159.8, fetch_ms=0.17,
            )
            safe_event["object_id"] = f"obj-{index}"
            records.append(_record(
                design=SAFE, workload=workload, repetition=repetition,
                success=True, event=safe_event,
                ledger=_ledger(
                    service=safe_service,
                    network_bytes=5738.0,
                    network_value=5.34e-06,
                ),
            ))
            cand_event = _event(
                representation="sampled_frames",
                endpoint="local_materialized",
                quoted=1.0, realized=candidate_service,
                bytes_read=5738, artifact_bytes=5738,
                delay_ms=19.7, fetch_ms=0.34,
            )
            cand_event["object_id"] = f"obj-{index}"
            records.append(_record(
                design=CANDIDATE, workload=workload,
                repetition=repetition, success=True, event=cand_event,
                ledger=_ledger(
                    service=candidate_service,
                    network_bytes=candidate_network_bytes,
                    network_value=0.0,
                    storage=candidate_storage,
                    materialization=candidate_materialization,
                    transition=candidate_transition,
                    not_applicable=False,
                ),
            ))
    if drop_baseline:
        records = [
            r for r in records
            if not (r["design_id"] == SAFE
                    and r["workload_id"] == "causal-w00"
                    and r["repetition"] == 0)
        ]
    if duplicate:
        records.append(dict(records[0]))
    if foreign_pilot:
        records[0] = {**records[0], "experiment_id": "another-pilot"}

    run = root / "run"
    run.mkdir(parents=True, exist_ok=True)
    (run / "canonical_records.jsonl").write_text(
        "".join(json.dumps(r, sort_keys=True) + "\n" for r in records),
        encoding="utf-8",
    )
    (run / "attempt_ledger.jsonl").write_text(
        "".join(
            json.dumps({
                "trial_key": r["trial_key"],
                "observation_class": "canonical",
                "failure_class": None,
                "succeeded": True,
            }, sort_keys=True) + "\n"
            for r in records
        )
        + json.dumps({
            "trial_key": "infra",
            "observation_class": "infrastructure",
            "failure_class": "workflow_failure",
            "succeeded": False,
        }, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write(run / "distributed_pilot_plan.json", {"pilot_id": PILOT})
    _sums(run)

    config = root / "input-freeze" / "config"
    _write(config / "system.json", {
        "schema_version": "pathfinder.system/v1",
        "representations": [
            {"id": "sampled_frames", "size_bytes": 8000000},
            {"id": "multimodal_digest", "size_bytes": 1000000},
        ],
        "physical_designs": [
            {
                "id": SAFE,
                "paths": {
                    "sampled_frames": {
                        "location": "origin-remote", "latency_ms": 160,
                        "latency_jitter_ms": 14,
                        "realized_cost": safe_service,
                        "quotes": {"video_qa": 4},
                    },
                },
            },
            {
                "id": CANDIDATE,
                "paths": {
                    "sampled_frames": {
                        "location": "local-materialized", "latency_ms": 20,
                        "latency_jitter_ms": 4,
                        "realized_cost": candidate_service,
                        "quotes": {"video_qa": 1},
                    },
                },
            },
        ],
    })
    _write(config / "measurements.json", {
        "schema_version": "pathfinder.pilot-measurements/v1alpha1",
        "pilot_id": PILOT,
        "execution_node_id": "node-exec",
        "storage_accounting_window_hours": 24.0,
        "transition_allocation": {
            "bundle_bytes": 245760,
            "bundle_elapsed_seconds": 4.25,
            "method": "representation-byte-proportional",
            "elapsed_time_scope": "operator-observed bundle transfer",
        },
        "measurements": [
            {
                "design_id": SAFE, "node_id": "node-origin",
                "object_id": "*",
                "storage": {"kind": "not_applicable", "justification": "x"},
                "materialization": {
                    "kind": "not_applicable", "justification": "x",
                },
                "transition": {
                    "kind": "not_applicable", "justification": "x",
                },
            },
            {
                "design_id": CANDIDATE, "node_id": "node-exec",
                "object_id": "obj-0",
                "storage": {
                    "kind": "derived", "bytes": 5738, "hours": 12.0,
                    "provenance": "predeclared window",
                },
                "materialization": {
                    "kind": "measured", "bytes": 5738,
                    "provenance": "verified byte size",
                },
                "transition": {
                    "kind": "derived", "bytes": transition_bytes,
                    "seconds": transition_seconds,
                    "provenance": "proportional allocation",
                },
            },
        ],
    })
    _write(config / "endpoint-registry.json", {
        "execution_node_id": "node-exec",
        "endpoints": [
            {
                "endpoint_id": "origin_remote", "node_id": "node-origin",
                "location": "origin-remote", "network_transport": "remote",
                "network_zero_justification": None,
            },
            {
                "endpoint_id": "local_materialized", "node_id": "node-exec",
                "location": "local-materialized",
                "network_transport": "local",
                "network_zero_justification": "same host",
            },
        ],
        "placement": [
            {
                "design_id": SAFE, "representation_id": "*",
                "endpoint_id": "origin_remote",
            },
            {
                "design_id": CANDIDATE,
                "representation_id": "sampled_frames",
                "endpoint_id": "local_materialized",
            },
        ],
    })
    _sums(config)

    provenance = root / "input-freeze" / "provenance"
    _write(provenance / "network-transfer-receipt.json", {
        "archive_name": "bundle.tar", "archive_size_bytes": 245760,
        "elapsed_seconds": 4.25, "source_node_id": "node-origin",
        "destination_node_id": "node-exec",
        "measurement_scope": "single-bundle-transfer",
        "credentials_recorded": False,
    })
    _write(provenance / "local-v2" / "local-object-catalog.json", {
        "catalog_version": "v1",
        "objects": {
            f"obj-{i}": {
                "representations": {
                    "sampled_frames": {
                        "path": f"/data/obj-{i}/sampled_frames.json",
                    },
                },
            }
            for i in range(workloads)
        },
    })
    _sums(provenance)
    _sums(provenance / "local-v2")

    evaluation = root / "evaluation"
    _write(evaluation / "evaluation.json", {"pilot_id": PILOT})
    _sums(evaluation)
    return root


class SnapshotIntegrityTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.snapshot = build_snapshot(self.root / "snap")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_a_clean_snapshot_verifies(self) -> None:
        verified = verify_snapshot(self.snapshot)
        self.assertTrue(verified)
        self.assertEqual(64, len(snapshot_fingerprint(self.snapshot)))

    def test_a_tampered_snapshot_is_refused(self) -> None:
        (self.snapshot / "evaluation" / "evaluation.json").write_text(
            "{}", encoding="utf-8"
        )
        with self.assertRaisesRegex(CostAuditError, "checksum mismatch"):
            verify_snapshot(self.snapshot)

    def test_the_audit_does_not_modify_the_snapshot(self) -> None:
        before = snapshot_fingerprint(self.snapshot)
        result = audit_distributed_cost_reality(
            self.snapshot, output_dir=self.root / "audit"
        )
        after = snapshot_fingerprint(self.snapshot)
        self.assertEqual(before, after)
        self.assertEqual(before, result["snapshot_fingerprint_before"])
        self.assertEqual(after, result["snapshot_fingerprint_after"])
        self.assertFalse(result["snapshot_modified"])

    def test_output_inside_the_snapshot_is_refused(self) -> None:
        with self.assertRaisesRegex(CostAuditError, "outside the frozen"):
            audit_distributed_cost_reality(
                self.snapshot,
                output_dir=self.snapshot / "audit",
            )

    def test_an_existing_output_directory_is_refused(self) -> None:
        audit_distributed_cost_reality(
            self.snapshot, output_dir=self.root / "once"
        )
        with self.assertRaisesRegex(CostAuditError, "already exists"):
            audit_distributed_cost_reality(
                self.snapshot, output_dir=self.root / "once"
            )


class ProvenanceClassificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.snapshot = build_snapshot(self.root / "snap")
        self.rows = {
            row["field"]: row
            for row in classify_cost_provenance(load_snapshot(self.snapshot))
        }

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_every_class_is_a_declared_class(self) -> None:
        for row in self.rows.values():
            self.assertIn(row["provenance"], PROVENANCE_CLASSES)

    def test_the_service_tariff_is_configured_not_measured(self) -> None:
        row = self.rows["realized_cost"]
        self.assertEqual("configured", row["provenance"])
        self.assertEqual("configured-service-tariff", row["cost_concept"])
        self.assertIn("system.json", row["evidence"])
        self.assertIn("measures no physical resource", row["note"])
        self.assertEqual(
            "configured",
            self.rows["cost_ledger.service.value"]["provenance"],
        )

    def test_the_quote_is_a_controlled_intervention(self) -> None:
        row = self.rows["quoted_price"]
        self.assertEqual("controlled-intervention", row["provenance"])
        self.assertIn("experimental intervention", row["note"])
        self.assertEqual("quote-shown-to-agent", row["cost_concept"])
        self.assertNotEqual(
            row["cost_concept"],
            self.rows["realized_cost"]["cost_concept"],
        )

    def test_bytes_are_measured_raw_resources(self) -> None:
        for field in ("bytes_read", "artifact_bytes_sent"):
            self.assertEqual("measured", self.rows[field]["provenance"])
            self.assertEqual(
                "measured-raw-resource",
                self.rows[field]["cost_concept"],
            )

    def test_controlled_delay_is_not_natural_latency(self) -> None:
        delay = self.rows["data_agent_controlled_delay_ms"]
        self.assertEqual("controlled-intervention", delay["provenance"])
        self.assertIn("never be described as natural", delay["note"])
        service = self.rows["data_agent_service_latency_ms"]
        self.assertEqual("injected-or-simulated", service["provenance"])
        self.assertEqual(
            "injected-or-simulated",
            self.rows["felt_latency_ms"]["provenance"],
        )

    def test_normalized_cost_is_derived_not_measured(self) -> None:
        self.assertEqual(
            "derived-from-measured",
            self.rows["cost_ledger.network.value"]["provenance"],
        )
        self.assertEqual(
            "normalized-derived-cost",
            self.rows["cost_ledger.network.value"]["cost_concept"],
        )

    def test_absent_quantities_are_missing_not_zero(self) -> None:
        for field in (
            "llm_request_count", "llm_total_tokens", "cpu_seconds",
            "gpu_seconds", "actual_monetary_expenditure",
        ):
            row = self.rows[field]
            self.assertEqual("missing", row["provenance"])
            self.assertEqual(0, row["observed_count"])
            self.assertIn("absent, not zero", row["note"])

    def test_monetary_expenditure_is_its_own_concept(self) -> None:
        self.assertEqual(
            "actual-monetary-expenditure",
            self.rows["actual_monetary_expenditure"]["cost_concept"],
        )

    def test_storage_duration_is_configured_not_derived(self) -> None:
        row = self.rows["measurements.storage.hours"]
        self.assertEqual("configured", row["provenance"])
        self.assertIn("predeclared accounting window", row["note"])
        self.assertEqual(
            "derived-from-measured",
            self.rows["measurements.storage.bytes"]["provenance"],
        )

    def test_retry_overhead_is_recorded_but_excluded(self) -> None:
        row = self.rows["retry_and_failed_attempt_overhead"]
        self.assertEqual(1, row["observed_count"])
        self.assertIn("excluded from canonical cost", row["note"])


class PairingTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _audit(self, **kwargs: Any):
        snapshot = build_snapshot(self.root / "snap", **kwargs)
        return audit_distributed_cost_reality(
            snapshot, output_dir=self.root / "audit"
        )

    def test_sign_conventions(self) -> None:
        self._audit()
        rows = (
            self.root / "audit" / "paired_cost_decomposition.csv"
        ).read_text(encoding="utf-8").splitlines()
        header = rows[0].split(",")
        values = dict(zip(header, rows[1].split(",")))
        # cost_saving = baseline - candidate = 0.9 - 0.15
        self.assertAlmostEqual(
            0.75, float(values["configured_service_cost_saving"]), places=9
        )
        # raw_resource_delta = candidate - baseline
        self.assertAlmostEqual(
            -5738.0, float(values["network_raw_bytes_delta"]), places=6
        )
        self.assertAlmostEqual(
            0.0, float(values["success_delta"]), places=9
        )

    def test_a_missing_baseline_is_refused(self) -> None:
        with self.assertRaisesRegex(CostAuditError, "no baseline record"):
            self._audit(drop_baseline=True)

    def test_a_duplicate_record_is_refused(self) -> None:
        with self.assertRaisesRegex(CostAuditError, "duplicate canonical"):
            self._audit(duplicate=True)

    def test_a_foreign_pilot_record_is_refused(self) -> None:
        with self.assertRaisesRegex(CostAuditError, "different pilot"):
            self._audit(foreign_pilot=True)


class ConfiguredDominanceTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _layers(self, name: str, **kwargs: Any) -> dict[str, Any]:
        snapshot = build_snapshot(self.root / f"snap-{name}", **kwargs)
        audit_distributed_cost_reality(
            snapshot, output_dir=self.root / f"audit-{name}"
        )
        return json.loads(
            (
                self.root / f"audit-{name}"
                / "configured_vs_measured_cost.json"
            ).read_text(encoding="utf-8")
        )

    def test_configured_service_dominates(self) -> None:
        layers = self._layers("dominant")
        headline = layers["headline"]
        self.assertAlmostEqual(
            0.75, headline["configured_service_cost_saving"], places=9
        )
        self.assertEqual("DEFINED", headline["service_dominance"]["status"])
        # Service saving exceeds the total: non-service is negative.
        dominance = headline["service_dominance"]
        self.assertGreater(dominance["ratio"], 1.0)
        self.assertTrue(dominance["exceeds_total"])
        self.assertIn("opposite sign", dominance["explanation"])
        self.assertIn("never as", dominance["explanation"])
        self.assertEqual("negative", dominance["non_service_residual_sign"])
        self.assertLess(
            headline["non_service_normalized_cost_saving"], 0.0
        )

    def test_the_three_layers_stay_separate(self) -> None:
        layers = self._layers("layers")
        self.assertIn("layer_1_raw_measured_resources", layers)
        self.assertIn("layer_2_frozen_normalized_accounting", layers)
        self.assertIn("layer_3_configured_service_cost_scenario", layers)
        self.assertEqual(
            "pilot-cost-unit",
            layers["layer_2_frozen_normalized_accounting"]["unit"],
        )
        self.assertTrue(
            layers["layer_3_configured_service_cost_scenario"][
                "non_service_is_still_normalized_accounting_not_money"
            ]
        )
        self.assertFalse(
            layers["actual_monetary_expenditure"]["observed"]
        )

    def test_the_counterfactual_is_labelled_post_hoc(self) -> None:
        layers = self._layers("counterfactual")
        block = layers["by_design"][CANDIDATE][
            "counterfactual_without_configured_service"
        ]
        self.assertTrue(block["is_post_hoc_decomposition_not_a_rerun"])
        self.assertFalse(block["is_causal_estimate"])
        self.assertFalse(block["candidate_still_cheaper"])

    def test_sign_cancellation_yields_a_status_not_a_percentage(
        self,
    ) -> None:
        # Candidate costs more in service but the total still favours it.
        layers = self._layers(
            "cancel",
            safe_service=0.10,
            candidate_service=0.20,
            candidate_storage=0.0,
            candidate_transition=0.0,
            candidate_materialization=0.0,
        )
        dominance = layers["headline"]["service_dominance"]
        self.assertIn(
            dominance["status"],
            ("UNDEFINED_SIGN_CANCELLATION", "DEFINED"),
        )
        if dominance["status"] != "DEFINED":
            self.assertIsNone(dominance["ratio"])
            self.assertIn("opposite signs", dominance["explanation"])

    def test_missing_components_stay_missing(self) -> None:
        layers = self._layers(
            "missing",
            candidate_storage=None,
            candidate_transition=None,
        )
        overall = layers["overall"]
        self.assertGreater(overall["storage_cost_saving"]["missing"], 0)
        self.assertGreater(overall["total_cost_saving"]["missing"], 0)
        self.assertNotIn("total", overall["storage_cost_saving"])


class BreakEvenTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _results(self, name: str, **kwargs: Any) -> list[dict[str, Any]]:
        snapshot = build_snapshot(self.root / f"s-{name}", **kwargs)
        audit_distributed_cost_reality(
            snapshot, output_dir=self.root / f"a-{name}"
        )
        return json.loads(
            (
                self.root / f"a-{name}" / "break_even_analysis.json"
            ).read_text(encoding="utf-8")
        )["results"]

    def test_every_status_is_declared(self) -> None:
        for result in self._results("statuses"):
            self.assertIn(result["status"], BREAK_EVEN_STATUSES)
            for key in (
                "design_id", "scope", "numerator", "numerator_provenance",
                "denominator_per_session", "denominator_provenance",
                "unit", "estimated_reuse_sessions_to_break_even",
                "assumptions", "status",
            ):
                self.assertIn(key, result)

    def test_a_positive_byte_saving_identifies_a_break_even(self) -> None:
        results = self._results("bytes")
        byte_result = next(
            r for r in results
            if r["reuse_unit"] == "aggregate-pilot-sessions-for-this-design"
        )
        self.assertEqual("IDENTIFIED", byte_result["status"])
        # 822.9 one-time bytes / 5738 bytes avoided per session.
        self.assertAlmostEqual(
            822.9 / 5738.0,
            byte_result["estimated_reuse_sessions_to_break_even"],
            places=6,
        )

    def test_both_reuse_scopes_are_reported(self) -> None:
        results = self._results("scopes")
        units = {r["reuse_unit"] for r in results}
        self.assertIn("aggregate-pilot-sessions-for-this-design", units)
        self.assertIn("reuse-accesses-per-materialized-object", units)

    def test_byte_break_even_records_its_limits(self) -> None:
        for result in self._results("limits"):
            self.assertFalse(result["is_monetary_break_even"])
            self.assertFalse(result["is_latency_or_time_break_even"])
            self.assertFalse(result["proves_lower_total_resource_cost"])
            self.assertTrue(result["interpretation_limits"])
            self.assertIn("BYTE-VOLUME", result["interpretation_limits"][0])

    def test_the_allocation_method_is_recorded(self) -> None:
        byte_result = next(
            r for r in self._results("alloc")
            if r["reuse_unit"] == "aggregate-pilot-sessions-for-this-design"
        )
        self.assertIsNotNone(byte_result["one_time_allocation_method"])
        self.assertIn("method=", byte_result["one_time_allocation_method"])
        self.assertIn(
            "entries",
            byte_result["numerator_provenance"],
        )
        self.assertIn("NOT reuse per object", " ".join(
            byte_result["assumptions"]
        ))

    def test_a_latency_saving_from_injected_delay_is_refused(self) -> None:
        results = self._results("latency")
        time_result = next(
            r for r in results if r["scope"].startswith("time")
        )
        self.assertEqual(
            "NOT_IDENTIFIED_MISSING_INPUT", time_result["status"]
        )
        self.assertIsNone(
            time_result["estimated_reuse_sessions_to_break_even"]
        )
        self.assertIn("injected", time_result["denominator_provenance"])

    def test_a_nonpositive_saving_yields_no_break_even(self) -> None:
        results = self._results("nonpositive")
        normalized = next(
            r for r in results if r["scope"].startswith("normalized")
        )
        self.assertEqual(
            "NO_BREAK_EVEN_NONPOSITIVE_SAVING", normalized["status"]
        )
        self.assertIsNone(
            normalized["estimated_reuse_sessions_to_break_even"]
        )

    def test_incompatible_units_are_refused(self) -> None:
        results = self._results("units")
        mixed = next(
            r for r in results if r["scope"].startswith("mixed-unit")
        )
        self.assertEqual("NOT_COMPARABLE_UNITS", mixed["status"])
        self.assertIsNone(
            mixed["estimated_reuse_sessions_to_break_even"]
        )


class PhysicalPathAndOutputTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.snapshot = build_snapshot(self.root / "snap")
        self.result = audit_distributed_cost_reality(
            self.snapshot, output_dir=self.root / "audit"
        )
        self.audit = self.root / "audit"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_execution_and_one_time_bytes_are_separated(self) -> None:
        path = json.loads(
            (self.audit / "physical_data_path.json").read_text(
                encoding="utf-8"
            )
        )
        during = path["bytes_crossing_nodes_during_execution"]
        once = path["bytes_crossing_nodes_once_before_execution"]
        # Only the remote endpoint's bytes cross during execution.
        self.assertAlmostEqual(6 * 5738.0, during["total_bytes"], places=3)
        self.assertEqual(245760, once["archive_size_bytes"])
        self.assertEqual(
            "offline-operator-materialization-transfer", once["occurred"]
        )

    def test_the_artifact_type_is_identified(self) -> None:
        path = json.loads(
            (self.audit / "physical_data_path.json").read_text(
                encoding="utf-8"
            )
        )
        artifacts = path["artifact_types"]
        self.assertIn(
            "sampled_frames.json",
            " ".join(artifacts["materialized_file_suffixes"]),
        )
        self.assertIn("JSON text", artifacts["interpretation"])
        self.assertEqual(
            8000000,
            artifacts["declared_size_bytes_in_system_config"][
                "sampled_frames"
            ],
        )
        self.assertLess(
            artifacts["observed_payload_bytes"]["sampled_frames"]["mean"],
            10000,
        )

    def test_all_required_files_exist_and_verify(self) -> None:
        expected = {
            "cost_provenance.csv", "measured_resource_summary.csv",
            "paired_cost_decomposition.csv",
            "configured_vs_measured_cost.json", "break_even_analysis.json",
            "physical_data_path.json", "audit_summary.md",
            "audit_manifest.json", "SHA256SUMS",
        }
        self.assertEqual(
            expected, {p.name for p in self.audit.iterdir()}
        )
        for line in (self.audit / "SHA256SUMS").read_text(
            encoding="utf-8"
        ).splitlines():
            digest, name = line.split("  ", 1)
            self.assertEqual(
                digest,
                sha256((self.audit / name).read_bytes()).hexdigest(),
                name,
            )

    def test_safety_flags_are_recorded(self) -> None:
        manifest = json.loads(
            (self.audit / "audit_manifest.json").read_text(encoding="utf-8")
        )
        self.assertTrue(manifest["posthoc"])
        self.assertFalse(manifest["eligible_for_scientific_claims"])
        self.assertFalse(manifest["deployment_mutations_performed"])
        self.assertFalse(manifest["credentials_recorded"])
        self.assertTrue(manifest["read_only"])
        self.assertFalse(manifest["snapshot_modified"])
        self.assertEqual(64, len(manifest["snapshot_content_fingerprint"]))
        self.assertIn("completeness_summary", manifest)

    def test_output_is_deterministic(self) -> None:
        other = build_snapshot(self.root / "snap2")
        audit_distributed_cost_reality(
            other, output_dir=self.root / "audit2"
        )
        for name in (
            "cost_provenance.csv", "configured_vs_measured_cost.json",
            "break_even_analysis.json", "physical_data_path.json",
            "audit_summary.md", "SHA256SUMS",
        ):
            self.assertEqual(
                (self.audit / name).read_bytes(),
                (self.root / "audit2" / name).read_bytes(),
                name,
            )

    def test_no_credential_or_token_appears_in_output(self) -> None:
        for path in self.audit.iterdir():
            content = path.read_text(encoding="utf-8")
            for marker in (
                "Bearer ", "authorization", "_TOKEN", "password",
                "secret", "PRIVATE KEY",
            ):
                self.assertNotIn(marker, content, f"{path.name}:{marker}")

    def test_the_report_answers_every_required_question(self) -> None:
        report = (self.audit / "audit_summary.md").read_text(
            encoding="utf-8"
        )
        for fragment in (
            "What was actually measured?",
            "What was manually configured?",
            "What was injected or simulated?",
            "What information is missing?",
            "How much of the reported cost difference",
            "What remains after removing",
            "distinguishable from noise?",
            "break-even point identifiable?",
            "meaningful data volumes?",
            "Recommendation (diagnostic)",
            "not a scientific conclusion",
        ):
            self.assertIn(fragment, report)


if __name__ == "__main__":
    unittest.main()
