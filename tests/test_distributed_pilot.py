"""Minimum distributed-pilot infrastructure.

Every test here is offline: no worker, Data Agent, or MCP process is started,
no workflow is submitted, and the only transports are in-process fakes.
"""

from __future__ import annotations

import contextlib
import copy
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping
from unittest import mock

from pathfinder.awm import AWMConfigError, certify_awm_restricted_policy
from pathfinder.cli import main as cli_main
from pathfinder.distributed import (
    COST_COMPONENT_IDS,
    CostModelError,
    CostTelemetryIncompleteError,
    CrossEndpointHandleError,
    EndpointRegistryError,
    EndpointScopedHandle,
    EndpointUnreachableError,
    IncompleteOracleError,
    ObservationOutcome,
    PilotPreregistrationError,
    PilotResumeError,
    PilotRunnerError,
    build_cost_ledger,
    build_distributed_trial_plan,
    build_endpoint_registry,
    ensure_frozen_plan,
    load_cost_model,
    load_distributed_pilot_preregistration,
    load_endpoint_registry,
    manifest_sha256,
    new_run_state,
    preflight_distributed_pilot,
    record_total_cost,
    trial_plan_payload,
)
from pathfinder.distributed.health import (
    HttpDataAgentHealthProbe,
    MAXIMUM_HEALTH_RESPONSE_BYTES,
)
from pathfinder.integrations.flowmesh.gateway import (
    artifact_handle_fingerprint,
)

from tests.test_awm_certificate import (
    DIGEST,
    FRAMES,
    PAIR,
    SAFE,
    _Cell,
    _Fixture,
    _plan,
    _workload_ids,
    _write_json,
)


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_PREREGISTRATION = ROOT / "configs" / "distributed_pilot_example.json"
EXAMPLE_REGISTRY = ROOT / "configs" / "distributed_endpoints_example.json"
GIB = float(1024 ** 3)

ORIGIN_URL_ENV = "PATHFINDER_TEST_ORIGIN_URL"
LOCAL_URL_ENV = "PATHFINDER_TEST_LOCAL_URL"
ORIGIN_TOKEN_ENV = "PATHFINDER_TEST_ORIGIN_TOKEN"

TEST_ENVIRONMENT = {
    ORIGIN_URL_ENV: "https://origin.invalid:8443/data-agent",
    LOCAL_URL_ENV: "https://local.invalid:9443/data-agent",
    ORIGIN_TOKEN_ENV: "super-secret-token-value",
}


def _cost_model_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "pathfinder.total-cost-model/v1alpha1",
        "cost_model_id": "unit-test-fixture",
        "accounting_unit": "pilot-cost-unit",
        "rate_provenance": "NON-SCIENTIFIC UNIT TEST FIXTURE",
        "materialization_amortization_horizon_sessions": 4,
        "artifact_transfer_accounted_in": "network_cost",
        "conversion_rates": {
            "network_cost_per_gib": 1024.0,
            "storage_cost_per_gib_hour": 2048.0,
            "materialization_cost_per_gib": 4096.0,
            "transition_cost_per_gib": 512.0,
            "elapsed_time_cost_per_second": 0.5,
        },
    }
    payload.update(overrides)
    return payload


def _preregistration_payload(**overrides: Any) -> dict[str, Any]:
    payload = json.loads(
        EXAMPLE_PREREGISTRATION.read_text(encoding="utf-8")
    )
    payload.update(overrides)
    return payload


def _write_preregistration(root: Path, payload: Mapping[str, Any]) -> Path:
    path = root / "preregistration.json"
    _write_json(path, payload)
    return path


def _registry_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": (
            "pathfinder.data-agent-endpoint-registry/v1alpha1"
        ),
        "registry_id": "unit-test-registry",
        "execution_node_id": "exec-node-1",
        "endpoints": [
            {
                "endpoint_id": "origin_remote",
                "node_id": "node-origin",
                "location": "origin-remote",
                "base_url_env": ORIGIN_URL_ENV,
                "token_env": ORIGIN_TOKEN_ENV,
                "telemetry_capabilities": [
                    "access_telemetry",
                    "transfer_bytes",
                ],
            },
            {
                "endpoint_id": "local_materialized",
                "node_id": "node-local",
                "location": "local-materialized",
                "base_url_env": LOCAL_URL_ENV,
                "token_env": None,
                "telemetry_capabilities": [
                    "access_telemetry",
                    "transfer_bytes",
                ],
            },
        ],
        "placement": [
            {
                "design_id": SAFE,
                "representation_id": "*",
                "endpoint_id": "origin_remote",
            },
            {
                "design_id": FRAMES,
                "representation_id": "*",
                "endpoint_id": "local_materialized",
            },
            {
                "design_id": DIGEST,
                "representation_id": "*",
                "endpoint_id": "local_materialized",
            },
        ],
    }
    payload.update(overrides)
    return payload


def _registry(**overrides: Any):
    return build_endpoint_registry(
        _registry_payload(**overrides),
        source_sha256="0" * 64,
    )


class _FakeProbe:
    """In-process health surface. Never opens a socket."""

    def __init__(
        self,
        *,
        healthy: Mapping[str, bool] | None = None,
        node_ids: Mapping[str, str] | None = None,
        catalog: Mapping[str, Mapping[str, str]] | None = None,
        unreachable: tuple[str, ...] = (),
    ) -> None:
        self.healthy = dict(healthy or {})
        self.node_ids = dict(node_ids or {})
        self.catalog = {k: dict(v) for k, v in (catalog or {}).items()}
        self.unreachable = set(unreachable)
        self.calls: list[str] = []

    def health(self, endpoint_id: str) -> dict[str, Any]:
        self.calls.append(endpoint_id)
        if endpoint_id in self.unreachable:
            # Deliberately unredacted at the raise site: the report must
            # scrub it regardless of what a third-party client produces.
            raise EndpointUnreachableError(
                endpoint_id,
                "connection refused for token "
                + TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
            )
        return {
            "healthy": self.healthy.get(endpoint_id, True),
            "node_id": self.node_ids.get(
                endpoint_id,
                {
                    "origin_remote": "node-origin",
                    "local_materialized": "node-local",
                }[endpoint_id],
            ),
            "object_catalog_versions": self.catalog.get(endpoint_id, {}),
        }


# --------------------------------------------------------------------------
# 1. Preregistration contract
# --------------------------------------------------------------------------
class PreregistrationTest(unittest.TestCase):
    def test_committed_example_loads_and_declares_its_boundary(self) -> None:
        prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        self.assertFalse(prereg.posthoc)
        self.assertFalse(prereg.confirmatory)
        self.assertFalse(prereg.eligible_for_scientific_claims)
        self.assertEqual(0.05, prereg.delta_success_margin)
        self.assertEqual(0.25, prereg.minimum_cost_saving)
        self.assertEqual(0.05, prereg.alpha)
        self.assertEqual(SAFE, prereg.safe_design_id)
        self.assertEqual(SAFE, prereg.fallback_design_id)
        self.assertIn(PAIR, prereg.excluded_design_ids)
        self.assertNotIn(PAIR, prereg.candidate_design_ids)
        public = prereg.to_public_dict()
        self.assertFalse(public["thresholds"]["scientifically_justified"])
        self.assertIn("not confirmatory", public["claim_boundary"])
        self.assertFalse(
            public["repetitions_increase_independent_units"]
        )

    def test_restricted_candidate_set_matches_the_declared_policy(
        self,
    ) -> None:
        prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        mapping = {
            stratum.stratum_id: stratum.candidate_design_id
            for stratum in prereg.strata
        }
        self.assertEqual(
            {
                "causal": FRAMES,
                "descriptive": FRAMES,
                "temporal": DIGEST,
            },
            mapping,
        )

    def test_workload_manifest_hash_is_order_independent(self) -> None:
        forward = manifest_sha256(["b", "a", "c"])
        backward = manifest_sha256(["c", "b", "a"])
        self.assertEqual(forward, backward)
        self.assertNotEqual(forward, manifest_sha256(["a", "b"]))

    def test_a_declared_manifest_hash_must_match(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(
                Path(temporary),
                _preregistration_payload(
                    workload_manifest_sha256="0" * 64,
                ),
            )
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "workload_manifest_sha256 does not match",
            ):
                load_distributed_pilot_preregistration(path)

    def test_duplicate_workload_across_strata_is_refused(self) -> None:
        payload = _preregistration_payload()
        shared = payload["strata"]["causal"]["workload_ids"][0]
        payload["strata"]["temporal"]["workload_ids"].append(shared)
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "cannot be counted twice",
            ):
                load_distributed_pilot_preregistration(path)

    def test_duplicate_workload_within_a_stratum_is_refused(self) -> None:
        payload = _preregistration_payload()
        ids = payload["strata"]["causal"]["workload_ids"]
        ids.append(ids[0])
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "contains duplicates",
            ):
                load_distributed_pilot_preregistration(path)

    def test_overlap_with_the_frozen_workload_manifest_is_refused(
        self,
    ) -> None:
        payload = _preregistration_payload()
        frozen = payload["excluded_workload_ids"][0]
        payload["strata"]["causal"]["workload_ids"].append(frozen)
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "overlap the frozen/excluded workload manifest",
            ):
                load_distributed_pilot_preregistration(path)

    def test_a_missing_stratum_field_is_refused(self) -> None:
        payload = _preregistration_payload()
        del payload["strata"]["temporal"]["candidate_design_id"]
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "required configuration field",
            ):
                load_distributed_pilot_preregistration(path)

    def test_empty_strata_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(
                Path(temporary),
                _preregistration_payload(strata={}),
            )
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "strata cannot be empty",
            ):
                load_distributed_pilot_preregistration(path)

    def test_an_unknown_candidate_design_is_refused(self) -> None:
        payload = _preregistration_payload()
        payload["strata"]["causal"]["candidate_design_id"] = "D_not_declared"
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "not a declared design",
            ):
                load_distributed_pilot_preregistration(path)

    def test_an_excluded_design_cannot_be_a_candidate(self) -> None:
        payload = _preregistration_payload()
        payload["strata"]["causal"]["candidate_design_id"] = PAIR
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "names an excluded design",
            ):
                load_distributed_pilot_preregistration(path)

    def test_missing_thresholds_are_refused(self) -> None:
        for field in (
            "delta_success_margin",
            "minimum_cost_saving",
            "alpha",
            "provenance",
        ):
            with self.subTest(field=field):
                payload = _preregistration_payload()
                payload["thresholds"].pop(field)
                with tempfile.TemporaryDirectory() as temporary:
                    path = _write_preregistration(Path(temporary), payload)
                    with self.assertRaisesRegex(
                        PilotPreregistrationError,
                        "required configuration field",
                    ):
                        load_distributed_pilot_preregistration(path)

    def test_a_wrong_threshold_provenance_is_refused(self) -> None:
        payload = _preregistration_payload()
        payload["thresholds"]["provenance"] = (
            "preregistered-before-evaluation-outcomes"
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "thresholds.provenance must be",
            ):
                load_distributed_pilot_preregistration(path)

    def test_claiming_confirmatory_eligibility_is_refused(self) -> None:
        for field in ("confirmatory", "eligible_for_scientific_claims"):
            with self.subTest(field=field):
                with tempfile.TemporaryDirectory() as temporary:
                    path = _write_preregistration(
                        Path(temporary),
                        _preregistration_payload(**{field: True}),
                    )
                    with self.assertRaisesRegex(
                        PilotPreregistrationError,
                        "cannot claim confirmatory status",
                    ):
                        load_distributed_pilot_preregistration(path)

    def test_declaring_posthoc_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(
                Path(temporary),
                _preregistration_payload(posthoc=True),
            )
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "must declare posthoc=false",
            ):
                load_distributed_pilot_preregistration(path)

    def test_inconsistent_repetitions_are_refused(self) -> None:
        for value in (0, -1, 2.5, True):
            with self.subTest(value=value):
                with tempfile.TemporaryDirectory() as temporary:
                    path = _write_preregistration(
                        Path(temporary),
                        _preregistration_payload(repetitions=value),
                    )
                    with self.assertRaisesRegex(
                        PilotPreregistrationError,
                        "must be a positive integer",
                    ):
                        load_distributed_pilot_preregistration(path)

    def test_a_non_origin_fallback_is_refused(self) -> None:
        payload = _preregistration_payload()
        payload["fallback_rule"]["design_id"] = FRAMES
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "must be D_origin_remote",
            ):
                load_distributed_pilot_preregistration(path)

    def test_a_mutable_run_declaration_is_refused(self) -> None:
        payload = _preregistration_payload()
        payload["run_declaration"][
            "immutable_after_first_observation"
        ] = False
        with tempfile.TemporaryDirectory() as temporary:
            path = _write_preregistration(Path(temporary), payload)
            with self.assertRaisesRegex(
                PilotPreregistrationError,
                "must be true",
            ):
                load_distributed_pilot_preregistration(path)

    def test_the_config_hash_changes_with_any_edit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = load_distributed_pilot_preregistration(
                _write_preregistration(root, _preregistration_payload())
            )
            second = load_distributed_pilot_preregistration(
                _write_preregistration(
                    root,
                    _preregistration_payload(pilot_id="changed"),
                )
            )
        self.assertNotEqual(first.source_sha256, second.source_sha256)

    def test_the_committed_example_has_no_server_specific_values(
        self,
    ) -> None:
        text = EXAMPLE_PREREGISTRATION.read_text(encoding="utf-8")
        for forbidden in ("/home/", "/Users/", "http://", "https://"):
            self.assertNotIn(forbidden, text)


# --------------------------------------------------------------------------
# 2. Total-cost ledger
# --------------------------------------------------------------------------
class CostLedgerTest(unittest.TestCase):
    def _model(self, **overrides: Any):
        return load_cost_model(_cost_model_payload(**overrides))

    def _ledger(self, model=None, **overrides: Any):
        arguments: dict[str, Any] = {
            "service_cost": 1.0,
            "transferred_bytes": GIB / 1024,
            "stored_bytes": GIB / 2,
            "stored_hours": 2.0,
            "materialized_bytes": GIB / 4,
            "transition_bytes": GIB / 8,
            "transition_seconds": 4.0,
        }
        arguments.update(overrides)
        return build_cost_ledger(model or self._model(), **arguments)

    def test_total_is_the_sum_of_the_five_components(self) -> None:
        ledger = self._ledger()
        self.assertEqual(
            set(COST_COMPONENT_IDS),
            {component.component_id for component in ledger.components},
        )
        self.assertAlmostEqual(1.0, ledger.component("service").value)
        self.assertAlmostEqual(1.0, ledger.component("network").value)
        self.assertAlmostEqual(2048.0, ledger.component("storage").value)
        self.assertAlmostEqual(
            256.0,
            ledger.component("amortized_materialization").value,
        )
        self.assertAlmostEqual(66.0, ledger.component("transition").value)
        self.assertAlmostEqual(2372.0, ledger.total_cost)
        self.assertTrue(ledger.available)

    def test_every_raw_component_and_its_conversion_are_preserved(
        self,
    ) -> None:
        ledger = self._ledger()
        network = ledger.component("network")
        self.assertAlmostEqual(GIB / 1024, network.raw_quantity)
        self.assertEqual("bytes", network.raw_unit)
        self.assertIn("network_cost_per_gib", network.conversion_rule)
        self.assertEqual("measured", network.value_kind)
        storage = ledger.component("storage")
        self.assertEqual("gib_hours", storage.raw_unit)
        self.assertEqual("derived", storage.value_kind)
        self.assertEqual(2048.0, storage.conversion_rate)
        self.assertEqual("measured", ledger.component("service").value_kind)
        public = ledger.to_public_dict()
        self.assertEqual(
            "total_cost = service + network + storage + "
            "amortized_materialization + transition",
            public["cost_equation"],
        )
        self.assertEqual("pilot-cost-unit", public["accounting_unit"])

    def test_amortization_divides_by_the_declared_horizon(self) -> None:
        one = self._ledger(
            self._model(
                materialization_amortization_horizon_sessions=1,
            ),
        )
        eight = self._ledger(
            self._model(
                materialization_amortization_horizon_sessions=8,
            ),
        )
        self.assertAlmostEqual(
            one.component("amortized_materialization").value / 8.0,
            eight.component("amortized_materialization").value,
        )
        self.assertEqual(
            8,
            eight.to_public_dict()[
                "materialization_amortization_horizon_sessions"
            ],
        )

    def test_the_amortization_horizon_is_required(self) -> None:
        payload = _cost_model_payload()
        payload.pop("materialization_amortization_horizon_sessions")
        with self.assertRaisesRegex(
            CostModelError,
            "required configuration field",
        ):
            load_cost_model(payload)

    def test_transfer_is_never_counted_in_both_components(self) -> None:
        in_network = self._ledger(
            self._model(artifact_transfer_accounted_in="network_cost"),
            transferred_bytes=GIB,
        )
        in_service = self._ledger(
            self._model(artifact_transfer_accounted_in="service_cost"),
            transferred_bytes=GIB,
        )
        self.assertGreater(in_network.component("network").value, 0.0)
        self.assertEqual(0.0, in_service.component("network").value)
        self.assertIn(
            "double counting",
            in_service.component("network").conversion_rule,
        )
        # The raw byte count is still preserved for audit on both paths.
        self.assertEqual(
            GIB,
            in_service.component("network").raw_quantity,
        )
        self.assertIn(
            "INCLUDES artifact transfer",
            in_service.component("service").conversion_rule,
        )
        self.assertIn(
            "EXCLUDES artifact transfer",
            in_network.component("service").conversion_rule,
        )
        difference = in_network.total_cost - in_service.total_cost
        self.assertAlmostEqual(1024.0, difference)

    def test_an_unavailable_component_suppresses_the_total(self) -> None:
        ledger = self._ledger(
            stored_bytes=None,
            stored_hours=None,
            unavailable={"storage": "no storage accounting on this node"},
        )
        self.assertFalse(ledger.available)
        self.assertEqual(("storage",), ledger.unavailable_component_ids)
        self.assertIsNone(ledger.total_cost)
        component = ledger.component("storage")
        self.assertEqual("unavailable", component.value_kind)
        self.assertIsNone(component.value)
        self.assertEqual(
            "no storage accounting on this node",
            component.unavailable_reason,
        )

    def test_missing_required_telemetry_fails_closed(self) -> None:
        for field in (
            "service_cost",
            "transferred_bytes",
            "stored_bytes",
            "materialized_bytes",
            "transition_seconds",
        ):
            with self.subTest(field=field):
                with self.assertRaises(CostTelemetryIncompleteError):
                    self._ledger(**{field: None})

    def test_invalid_quantities_are_rejected(self) -> None:
        for value in (float("nan"), float("inf"), -1.0, True, "1.0"):
            with self.subTest(value=value):
                with self.assertRaises(CostModelError):
                    self._ledger(transferred_bytes=value)

    def test_invalid_rates_are_rejected(self) -> None:
        for value in (-1.0, float("nan"), True, None, "x"):
            with self.subTest(value=value):
                payload = _cost_model_payload()
                payload["conversion_rates"]["network_cost_per_gib"] = value
                with self.assertRaises(CostModelError):
                    load_cost_model(payload)

    def test_every_conversion_rate_is_required(self) -> None:
        for rate in (
            "network_cost_per_gib",
            "storage_cost_per_gib_hour",
            "materialization_cost_per_gib",
            "transition_cost_per_gib",
            "elapsed_time_cost_per_second",
        ):
            with self.subTest(rate=rate):
                payload = _cost_model_payload()
                payload["conversion_rates"].pop(rate)
                with self.assertRaisesRegex(
                    CostModelError,
                    "required configuration field",
                ):
                    load_cost_model(payload)

    def test_reading_a_record_total_fails_closed_without_a_ledger(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            CostTelemetryIncompleteError,
            "carries no cost ledger",
        ):
            record_total_cost({"trial_key": "t"})

    def test_reading_an_incomplete_ledger_fails_closed(self) -> None:
        ledger = self._ledger(
            stored_bytes=None,
            stored_hours=None,
            unavailable={"storage": "unmeasured"},
        )
        with self.assertRaisesRegex(
            CostTelemetryIncompleteError,
            "cost ledger is incomplete",
        ):
            record_total_cost({"cost_ledger": ledger.to_public_dict()})

    def test_reading_a_complete_ledger_returns_the_total(self) -> None:
        ledger = self._ledger()
        self.assertAlmostEqual(
            2372.0,
            record_total_cost({"cost_ledger": ledger.to_public_dict()}),
        )

    def test_the_committed_example_marks_its_rates_as_placeholders(
        self,
    ) -> None:
        prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        self.assertTrue(
            prereg.cost_model.rate_provenance.upper().startswith(
                "PLACEHOLDER"
            )
        )


# --------------------------------------------------------------------------
# 3. Endpoint registry and routing
# --------------------------------------------------------------------------
class EndpointRegistryTest(unittest.TestCase):
    def test_each_design_routes_to_exactly_one_endpoint(self) -> None:
        registry = _registry()
        self.assertEqual(
            ("local_materialized", "origin_remote"),
            registry.endpoint_ids,
        )
        origin = registry.route(
            design_id=SAFE,
            representation_id="sampled_frames",
        )
        local = registry.route(
            design_id=FRAMES,
            representation_id="sampled_frames",
        )
        self.assertEqual("origin_remote", origin.endpoint_id)
        self.assertEqual("node-origin", origin.source_node_id)
        self.assertEqual("origin-remote", origin.source_location)
        self.assertEqual(
            "exec-node-1",
            origin.destination_execution_node_id,
        )
        self.assertEqual("local_materialized", local.endpoint_id)
        self.assertEqual("node-local", local.source_node_id)
        self.assertNotEqual(origin.endpoint_id, local.endpoint_id)

    def test_a_route_records_both_node_identities(self) -> None:
        route = _registry().route(
            design_id=DIGEST,
            representation_id="multimodal_digest",
        )
        payload = route.to_public_dict()
        for key in (
            "endpoint_id",
            "source_node_id",
            "source_location",
            "destination_execution_node_id",
            "routing_rule",
        ):
            self.assertIn(key, payload)
        self.assertFalse(payload["credentials_recorded"])

    def test_an_undeclared_route_never_falls_back(self) -> None:
        registry = _registry()
        with self.assertRaisesRegex(
            EndpointRegistryError,
            "refusing to guess a Data Agent",
        ):
            registry.route(
                design_id="D_undeclared",
                representation_id="sampled_frames",
            )

    def test_a_representation_rule_beats_the_design_default(self) -> None:
        payload = _registry_payload()
        payload["placement"].append({
            "design_id": SAFE,
            "representation_id": "multimodal_digest",
            "endpoint_id": "local_materialized",
        })
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        specific = registry.route(
            design_id=SAFE,
            representation_id="multimodal_digest",
        )
        default = registry.route(
            design_id=SAFE,
            representation_id="sampled_frames",
        )
        self.assertEqual("local_materialized", specific.endpoint_id)
        self.assertEqual(
            "explicit-design-representation",
            specific.rule,
        )
        self.assertEqual("origin_remote", default.endpoint_id)
        self.assertEqual("explicit-design-default", default.rule)

    def test_a_single_endpoint_registry_preserves_existing_behaviour(
        self,
    ) -> None:
        payload = _registry_payload()
        payload["endpoints"] = [payload["endpoints"][0]]
        payload.pop("placement")
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        self.assertTrue(registry.single_endpoint)
        for design_id in (SAFE, FRAMES, DIGEST, PAIR):
            route = registry.route(
                design_id=design_id,
                representation_id="sampled_frames",
            )
            self.assertEqual("origin_remote", route.endpoint_id)
            self.assertEqual(
                "single-endpoint-compatibility",
                route.rule,
            )
        self.assertTrue(
            registry.to_public_dict(TEST_ENVIRONMENT)[
                "single_endpoint_compatibility_mode"
            ]
        )

    def test_multiple_endpoints_require_explicit_placement(self) -> None:
        payload = _registry_payload()
        payload.pop("placement")
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        self.assertIsNone(registry.default_endpoint_id)
        with self.assertRaisesRegex(
            EndpointRegistryError,
            "refusing to guess",
        ):
            registry.route(design_id=SAFE, representation_id="x")

    def test_duplicate_endpoint_ids_are_refused(self) -> None:
        payload = _registry_payload()
        payload["endpoints"].append(copy.deepcopy(payload["endpoints"][0]))
        with self.assertRaisesRegex(
            EndpointRegistryError,
            "duplicate endpoint_id",
        ):
            build_endpoint_registry(payload, source_sha256="0" * 64)

    def test_duplicate_placement_rules_are_refused(self) -> None:
        payload = _registry_payload()
        payload["placement"].append(copy.deepcopy(payload["placement"][0]))
        with self.assertRaisesRegex(
            EndpointRegistryError,
            "duplicate placement rule",
        ):
            build_endpoint_registry(payload, source_sha256="0" * 64)

    def test_a_placement_rule_must_name_a_declared_endpoint(self) -> None:
        payload = _registry_payload()
        payload["placement"][0]["endpoint_id"] = "nowhere"
        with self.assertRaisesRegex(
            EndpointRegistryError,
            "undeclared endpoint",
        ):
            build_endpoint_registry(payload, source_sha256="0" * 64)


class EndpointCredentialTest(unittest.TestCase):
    def test_inline_urls_and_credentials_are_refused(self) -> None:
        for field, value in (
            ("base_url", "https://agent.invalid"),
            ("token", "secret"),
            ("api_key", "secret"),
            ("password", "secret"),
        ):
            with self.subTest(field=field):
                payload = _registry_payload()
                payload["endpoints"][0][field] = value
                with self.assertRaisesRegex(
                    EndpointRegistryError,
                    "is not allowed",
                ):
                    build_endpoint_registry(
                        payload,
                        source_sha256="0" * 64,
                    )

    def test_connection_settings_come_from_the_environment(self) -> None:
        endpoint = _registry().endpoint("origin_remote")
        settings = endpoint.client_settings(TEST_ENVIRONMENT)
        self.assertEqual(
            TEST_ENVIRONMENT[ORIGIN_URL_ENV],
            settings.base_url,
        )
        self.assertEqual(
            TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
            settings.token,
        )

    def test_an_unset_url_variable_fails_closed(self) -> None:
        endpoint = _registry().endpoint("origin_remote")
        with self.assertRaisesRegex(
            EndpointRegistryError,
            "unset or empty",
        ):
            endpoint.client_settings({})

    @mock.patch.dict(os.environ, TEST_ENVIRONMENT)
    def test_public_output_never_contains_a_url_or_a_token(self) -> None:
        payload = _registry().to_public_dict(TEST_ENVIRONMENT)
        text = json.dumps(payload)
        self.assertNotIn(TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV], text)
        self.assertNotIn("/data-agent", text)
        self.assertNotIn("super-secret", text)
        origin = next(
            item
            for item in payload["endpoints"]
            if item["endpoint_id"] == "origin_remote"
        )
        # Only a sanitized scheme://host:port and a fingerprint survive.
        self.assertEqual("https://origin.invalid:8443", origin["endpoint"])
        self.assertEqual(64, len(origin["endpoint_sha256"]))
        self.assertTrue(origin["token_configured"])
        self.assertFalse(origin["credentials_recorded"])
        self.assertFalse(payload["credentials_recorded"])

    @mock.patch.dict(os.environ, TEST_ENVIRONMENT)
    def test_an_unreachable_error_redacts_declared_token_variables(
        self,
    ) -> None:
        registry = _registry()
        self.assertEqual(
            (ORIGIN_TOKEN_ENV,),
            registry.secret_environment_names,
        )
        error = EndpointUnreachableError(
            "origin_remote",
            "connection refused for token "
            + TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
            secret_environment_names=registry.secret_environment_names,
        )
        self.assertNotIn(
            TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
            str(error),
        )
        self.assertNotIn(TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV], error.detail)
        self.assertIn("<redacted>", error.detail)


class EndpointFailureClassificationTest(unittest.TestCase):
    def test_unreachable_is_infrastructure_not_policy(self) -> None:
        error = EndpointUnreachableError("origin_remote", "connection reset")
        self.assertEqual("infrastructure", error.failure_class)
        self.assertEqual("origin_remote", error.endpoint_id)

    def test_cross_endpoint_handle_is_policy_not_infrastructure(
        self,
    ) -> None:
        self.assertEqual("policy", CrossEndpointHandleError.failure_class)

    def test_the_two_classes_are_distinct_types(self) -> None:
        self.assertFalse(
            issubclass(EndpointUnreachableError, CrossEndpointHandleError)
        )
        self.assertFalse(
            issubclass(CrossEndpointHandleError, EndpointUnreachableError)
        )


class EndpointScopedHandleTest(unittest.TestCase):
    def test_a_handle_redeems_only_at_its_issuing_endpoint(self) -> None:
        handle = EndpointScopedHandle("origin_remote", "opaque-value")
        self.assertEqual("opaque-value", handle.redeem_at("origin_remote"))
        with self.assertRaisesRegex(
            CrossEndpointHandleError,
            "cannot be redeemed at",
        ):
            handle.redeem_at("local_materialized")

    def test_the_same_value_at_two_endpoints_fingerprints_apart(
        self,
    ) -> None:
        first = EndpointScopedHandle("origin_remote", "same")
        second = EndpointScopedHandle("local_materialized", "same")
        self.assertNotEqual(first.fingerprint, second.fingerprint)

    def test_public_output_carries_only_a_fingerprint(self) -> None:
        handle = EndpointScopedHandle("origin_remote", "opaque-value")
        payload = handle.to_public_dict()
        self.assertNotIn("opaque-value", json.dumps(payload))
        self.assertEqual(handle.fingerprint, payload["artifact_handle_sha256"])
        self.assertFalse(payload["handle_recorded"])

    def test_the_gateway_persists_only_handle_fingerprints(self) -> None:
        # Regression guard on the existing gateway contract.
        fingerprint = artifact_handle_fingerprint("opaque-value")
        self.assertEqual(64, len(fingerprint))
        self.assertNotIn("opaque-value", fingerprint)
        self.assertNotEqual(
            fingerprint,
            artifact_handle_fingerprint("other-value"),
        )


# --------------------------------------------------------------------------
# 4. Trial planning, resume, recovery
# --------------------------------------------------------------------------
class TrialPlanTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        self.trials = build_distributed_trial_plan(self.prereg)

    def test_plan_size_is_workloads_times_two_designs_times_repetitions(
        self,
    ) -> None:
        self.assertEqual(10, self.prereg.independent_workload_count)
        self.assertEqual(80, len(self.trials))
        self.assertEqual(
            self.prereg.planned_trial_count,
            len(self.trials),
        )

    def test_each_stratum_uses_its_own_restricted_candidate(self) -> None:
        by_stratum: dict[str, set[str]] = {}
        for trial in self.trials:
            by_stratum.setdefault(trial.stratum_id, set()).add(
                trial.design_id
            )
        self.assertEqual({SAFE, FRAMES}, by_stratum["causal"])
        self.assertEqual({SAFE, FRAMES}, by_stratum["descriptive"])
        self.assertEqual({SAFE, DIGEST}, by_stratum["temporal"])

    def test_the_excluded_design_never_appears(self) -> None:
        self.assertNotIn(
            PAIR,
            {trial.design_id for trial in self.trials},
        )

    def test_every_workload_gets_a_complete_block_for_both_designs(
        self,
    ) -> None:
        counts: dict[tuple[str, str], set[int]] = {}
        for trial in self.trials:
            counts.setdefault(
                (trial.workload_id, trial.design_id),
                set(),
            ).add(trial.repetition)
        for key, repetitions in counts.items():
            with self.subTest(cell=key):
                self.assertEqual(
                    set(range(self.prereg.repetitions)),
                    repetitions,
                )

    def test_the_plan_is_deterministic(self) -> None:
        again = build_distributed_trial_plan(self.prereg)
        self.assertEqual(
            [trial.to_public_dict() for trial in self.trials],
            [trial.to_public_dict() for trial in again],
        )
        self.assertEqual(
            trial_plan_payload(self.prereg, self.trials)["plan_sha256"],
            trial_plan_payload(self.prereg, again)["plan_sha256"],
        )

    def test_no_duplicate_canonical_cell(self) -> None:
        keys = [trial.trial_key for trial in self.trials]
        self.assertEqual(len(keys), len(set(keys)))
        ids = [trial.trial_id for trial in self.trials]
        self.assertEqual(len(ids), len(set(ids)))

    def test_the_plan_records_workload_level_independence(self) -> None:
        payload = trial_plan_payload(self.prereg, self.trials)
        self.assertEqual(
            "workload-object-cluster",
            payload["independent_unit"],
        )
        self.assertEqual(10, payload["independent_workload_count"])
        self.assertEqual(80, payload["planned_trial_count"])


class ResumeTest(unittest.TestCase):
    def _payload(self, root: Path, **overrides: Any):
        path = _write_preregistration(
            root,
            _preregistration_payload(**overrides),
        )
        prereg = load_distributed_pilot_preregistration(path)
        return prereg, trial_plan_payload(
            prereg,
            build_distributed_trial_plan(prereg),
        )

    def test_an_unchanged_plan_resumes_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, payload = self._payload(root)
            target = root / "plan.json"
            ensure_frozen_plan(target, payload)
            first = target.read_text(encoding="utf-8")
            for _ in range(3):
                ensure_frozen_plan(target, payload)
            self.assertEqual(first, target.read_text(encoding="utf-8"))

    def test_a_changed_configuration_is_refused_on_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, original = self._payload(root)
            target = root / "plan.json"
            ensure_frozen_plan(target, original)
            _, edited = self._payload(root, pilot_id="edited-mid-run")
            with self.assertRaisesRegex(
                PilotResumeError,
                "preregistration configuration",
            ):
                ensure_frozen_plan(target, edited)

    def test_a_changed_workload_manifest_is_refused_on_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, original = self._payload(root)
            target = root / "plan.json"
            ensure_frozen_plan(target, original)
            payload = _preregistration_payload()
            payload["strata"]["causal"]["workload_ids"].append(
                "causal-pilot-w99"
            )
            _, edited = self._payload(root, **payload)
            with self.assertRaises(PilotResumeError):
                ensure_frozen_plan(target, edited)

    def test_a_changed_repetition_count_is_refused_on_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, original = self._payload(root)
            target = root / "plan.json"
            ensure_frozen_plan(target, original)
            _, edited = self._payload(root, repetitions=2)
            with self.assertRaises(PilotResumeError):
                ensure_frozen_plan(target, edited)

    def test_an_unreadable_frozen_plan_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, payload = self._payload(root)
            target = root / "plan.json"
            target.write_text("{not json", encoding="utf-8")
            with self.assertRaisesRegex(PilotResumeError, "unreadable"):
                ensure_frozen_plan(target, payload)


class RunStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        self.trials = build_distributed_trial_plan(self.prereg)
        self.state = new_run_state(self.prereg, self.trials, max_attempts=3)

    def _canonical(self, trial_key: str, **overrides: Any):
        payload: dict[str, Any] = {
            "trial_key": trial_key,
            "observation_class": "canonical",
            "attempt": 1,
            "succeeded": True,
            "telemetry_complete": True,
            "artifact_selected": True,
            "artifact_delivery_complete": True,
        }
        payload.update(overrides)
        return ObservationOutcome(**payload)

    def test_a_duplicate_canonical_observation_is_refused(self) -> None:
        key = self.trials[0].trial_key
        self.state.record(self._canonical(key))
        with self.assertRaisesRegex(
            PilotRunnerError,
            "duplicate canonical observation",
        ):
            self.state.record(self._canonical(key))

    def test_an_unplanned_cell_is_refused(self) -> None:
        with self.assertRaisesRegex(
            PilotRunnerError,
            "unplanned cell",
        ):
            self.state.record(self._canonical("not-in-the-plan"))

    def test_incomplete_telemetry_cannot_be_canonical(self) -> None:
        with self.assertRaisesRegex(
            PilotRunnerError,
            "incomplete attempt cannot be filed",
        ):
            self.state.record(self._canonical(
                self.trials[0].trial_key,
                telemetry_complete=False,
            ))

    def test_incomplete_artifact_delivery_cannot_be_canonical(self) -> None:
        with self.assertRaisesRegex(
            PilotRunnerError,
            "incomplete attempt cannot be filed",
        ):
            self.state.record(self._canonical(
                self.trials[0].trial_key,
                artifact_delivery_complete=False,
            ))

    def test_an_unselected_artifact_does_not_require_delivery(self) -> None:
        self.state.record(self._canonical(
            self.trials[0].trial_key,
            artifact_selected=False,
            artifact_delivery_complete=False,
        ))
        self.assertEqual(1, len(self.state.completed))

    def test_recovery_attempts_stay_out_of_canonical_data(self) -> None:
        key = self.trials[0].trial_key
        self.state.record(ObservationOutcome(
            trial_key=key,
            observation_class="recovery_attempt",
            attempt=1,
            succeeded=False,
            telemetry_complete=False,
            artifact_selected=True,
            artifact_delivery_complete=False,
            failure_class="policy",
        ))
        self.assertEqual(0, len(self.state.completed))
        self.assertEqual(1, len(self.state.recovery_attempts))
        self.state.record(self._canonical(key))
        self.assertEqual(1, len(self.state.completed))
        self.assertEqual(1, len(self.state.recovery_attempts))
        public = self.state.to_public_dict()
        self.assertEqual(1, public["recovery_attempt_count"])
        self.assertFalse(
            public["recovery_attempts_included_in_canonical_data"]
        )

    def test_infrastructure_failures_are_recorded_separately(self) -> None:
        key = self.trials[0].trial_key
        self.state.record(ObservationOutcome(
            trial_key=key,
            observation_class="infrastructure",
            attempt=1,
            succeeded=False,
            telemetry_complete=False,
            artifact_selected=False,
            artifact_delivery_complete=False,
            failure_class="infrastructure",
            failure_detail="endpoint unreachable",
        ))
        self.assertEqual(1, len(self.state.infrastructure_failures))
        self.assertEqual(0, len(self.state.recovery_attempts))
        self.assertEqual(0, len(self.state.completed))

    def test_retries_are_bounded(self) -> None:
        key = self.trials[0].trial_key
        for attempt in range(3):
            self.assertTrue(self.state.may_retry(key))
            self.state.record(ObservationOutcome(
                trial_key=key,
                observation_class="infrastructure",
                attempt=attempt + 1,
                succeeded=False,
                telemetry_complete=False,
                artifact_selected=False,
                artifact_delivery_complete=False,
                failure_class="infrastructure",
            ))
        self.assertFalse(self.state.may_retry(key))
        with self.assertRaisesRegex(
            PilotRunnerError,
            "bounded retry budget",
        ):
            self.state.record(ObservationOutcome(
                trial_key=key,
                observation_class="infrastructure",
                attempt=4,
                succeeded=False,
                telemetry_complete=False,
                artifact_selected=False,
                artifact_delivery_complete=False,
            ))

    def test_a_completed_cell_is_not_retried(self) -> None:
        key = self.trials[0].trial_key
        self.state.record(self._canonical(key))
        self.assertFalse(self.state.may_retry(key))

    def test_a_partial_run_is_not_an_oracle(self) -> None:
        self.state.record(self._canonical(self.trials[0].trial_key))
        with self.assertRaisesRegex(
            IncompleteOracleError,
            "is not a Reduced Oracle",
        ):
            self.state.require_complete_oracle(self.prereg)
        self.assertFalse(self.state.complete)

    def test_a_complete_run_is_accepted(self) -> None:
        for trial in self.trials:
            self.state.record(self._canonical(trial.trial_key))
        self.assertTrue(self.state.complete)
        self.state.require_complete_oracle(self.prereg)
        self.assertEqual((), self.state.remaining_trial_keys)

    def test_a_workload_is_ready_only_with_both_complete_blocks(
        self,
    ) -> None:
        workload = self.prereg.strata[0].workload_ids[0]
        safe_only = [
            trial for trial in self.trials
            if trial.workload_id == workload
            and trial.design_id == SAFE
        ]
        for trial in safe_only:
            self.state.record(self._canonical(trial.trial_key))
        self.assertNotIn(
            workload,
            self.state.complete_repetition_blocks(self.prereg),
        )
        candidate_only = [
            trial for trial in self.trials
            if trial.workload_id == workload
            and trial.design_id != SAFE
        ]
        for trial in candidate_only[:-1]:
            self.state.record(self._canonical(trial.trial_key))
        self.assertNotIn(
            workload,
            self.state.complete_repetition_blocks(self.prereg),
        )
        self.state.record(self._canonical(candidate_only[-1].trial_key))
        self.assertIn(
            workload,
            self.state.complete_repetition_blocks(self.prereg),
        )

    def test_the_runner_never_manages_worker_lifecycle(self) -> None:
        self.assertFalse(
            self.state.to_public_dict()["worker_lifecycle_managed"]
        )


# --------------------------------------------------------------------------
# 5. Read-only preflight
# --------------------------------------------------------------------------
class PreflightTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        self.registry = _registry()
        self.pin = {"kind": "worker_alias", "value": "pilot-worker"}

    def _run(self, **overrides: Any) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "probe": _FakeProbe(),
            "worker_pin": self.pin,
            "environment": TEST_ENVIRONMENT,
        }
        arguments.update(overrides)
        return preflight_distributed_pilot(
            self.prereg,
            self.registry,
            **arguments,
        )

    def test_a_healthy_deployment_passes(self) -> None:
        report = self._run()
        self.assertEqual("ok", report["status"], report["failed_checks"])
        self.assertEqual([], report["failed_checks"])

    def test_preflight_mutates_nothing(self) -> None:
        report = self._run()
        for key, value in report["read_only"].items():
            with self.subTest(key=key):
                self.assertFalse(value)

    def test_preflight_states_what_it_cannot_prove(self) -> None:
        report = self._run()
        self.assertTrue(report["not_verified"])
        joined = " ".join(report["not_verified"])
        self.assertIn("dispatch", joined)
        self.assertIn("delivery", joined)
        self.assertIn("cannot prove", report["interpretation"])
        self.assertTrue(report["operator_next_steps"])

    @mock.patch.dict(os.environ, TEST_ENVIRONMENT)
    def test_an_unreachable_endpoint_fails_and_is_classified(self) -> None:
        report = self._run(probe=_FakeProbe(unreachable=("origin_remote",)))
        self.assertEqual("failed", report["status"])
        failing = next(
            check for check in report["checks"]
            if check["check_id"] == "endpoint[origin_remote].health"
        )
        self.assertEqual("infrastructure", failing["failure_class"])
        self.assertNotIn(
            TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
            json.dumps(report),
        )

    def test_an_unhealthy_endpoint_fails(self) -> None:
        report = self._run(
            probe=_FakeProbe(healthy={"origin_remote": False}),
        )
        self.assertEqual("failed", report["status"])
        self.assertIn(
            "endpoint[origin_remote].health",
            report["failed_checks"],
        )

    def test_a_node_identity_mismatch_fails(self) -> None:
        report = self._run(
            probe=_FakeProbe(node_ids={"origin_remote": "some-other-node"}),
        )
        self.assertEqual("failed", report["status"])
        self.assertIn(
            "endpoint[origin_remote].node_identity_matches_registry",
            report["failed_checks"],
        )

    def test_a_catalog_version_mismatch_fails(self) -> None:
        report = self._run(
            probe=_FakeProbe(catalog={
                "origin_remote": {"object-a": "v2"},
                "local_materialized": {"object-a": "v1"},
            }),
            expected_catalog_versions={"object-a": "v1"},
        )
        self.assertEqual("failed", report["status"])
        self.assertIn(
            "endpoint[origin_remote].object_catalog_versions_match",
            report["failed_checks"],
        )

    def test_matching_catalog_versions_pass(self) -> None:
        report = self._run(
            probe=_FakeProbe(catalog={
                "origin_remote": {"object-a": "v1"},
                "local_materialized": {"object-a": "v1"},
            }),
            expected_catalog_versions={"object-a": "v1"},
        )
        self.assertEqual("ok", report["status"], report["failed_checks"])

    def test_missing_telemetry_capabilities_fail(self) -> None:
        payload = _registry_payload()
        payload["endpoints"][0]["telemetry_capabilities"] = [
            "access_telemetry"
        ]
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        report = preflight_distributed_pilot(
            self.prereg,
            registry,
            probe=_FakeProbe(),
            worker_pin=self.pin,
            environment=TEST_ENVIRONMENT,
        )
        self.assertEqual("failed", report["status"])
        self.assertIn(
            "endpoint[origin_remote].telemetry_capabilities_advertised",
            report["failed_checks"],
        )

    def test_an_unroutable_design_fails(self) -> None:
        payload = _registry_payload()
        payload["placement"] = [payload["placement"][0]]
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        report = preflight_distributed_pilot(
            self.prereg,
            registry,
            probe=_FakeProbe(),
            worker_pin=self.pin,
            environment=TEST_ENVIRONMENT,
        )
        self.assertEqual("failed", report["status"])
        self.assertIn(
            "every_required_endpoint_is_declared",
            report["failed_checks"],
        )

    def test_an_unset_endpoint_url_fails(self) -> None:
        report = self._run(environment={})
        self.assertEqual("failed", report["status"])
        self.assertIn(
            "endpoint[origin_remote].base_url_configured",
            report["failed_checks"],
        )

    def test_an_invalid_worker_pin_fails(self) -> None:
        for pin in (None, {}, {"kind": "nonsense", "value": "x"},
                    {"kind": "worker_id", "value": "  "}):
            with self.subTest(pin=pin):
                report = self._run(worker_pin=pin)
                self.assertIn(
                    "worker_pin_configuration_valid",
                    report["failed_checks"],
                )

    def test_the_worker_pin_check_is_only_syntactic(self) -> None:
        report = self._run()
        check = next(
            item for item in report["checks"]
            if item["check_id"] == "worker_pin_configuration_valid"
        )
        self.assertTrue(check["syntactic_only"])

    def test_cost_completeness_checks_are_reported(self) -> None:
        report = self._run()
        ids = {check["check_id"] for check in report["checks"]}
        for expected in (
            "cost_model.conversion_rates_complete",
            "cost_model.amortization_horizon_declared",
            "cost_model.artifact_transfer_accounted_once",
        ):
            self.assertIn(expected, ids)

    def test_placeholder_rates_warn_without_blocking(self) -> None:
        report = self._run()
        self.assertIn(
            "cost_model.rates_are_measured_not_placeholder",
            report["advisory_warnings"],
        )
        self.assertNotIn(
            "cost_model.rates_are_measured_not_placeholder",
            report["failed_checks"],
        )
        self.assertEqual("ok", report["status"])

    @mock.patch.dict(os.environ, TEST_ENVIRONMENT)
    def test_preflight_emits_no_credential_value(self) -> None:
        text = json.dumps(self._run())
        self.assertNotIn(TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV], text)
        self.assertNotIn("/data-agent", text)
        self.assertFalse(self._run()["credentials_recorded"])

    def test_preflight_reports_the_pilot_claim_flags(self) -> None:
        report = self._run()
        self.assertFalse(report["posthoc"])
        self.assertFalse(report["confirmatory"])
        self.assertFalse(report["eligible_for_scientific_claims"])



# --------------------------------------------------------------------------
# HTTP health adapter
# --------------------------------------------------------------------------
class _FakeResponse:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self._body = body
        self.status = status

    def read(self, amount: int | None = None) -> bytes:
        return self._body if amount is None else self._body[:amount]

    def __enter__(self):
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


def _opener_returning(payload: Any, status: int = 200):
    captured: dict[str, Any] = {}

    def opener(request, timeout=None):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["timeout"] = timeout
        captured["headers"] = dict(request.header_items())
        body = (
            payload
            if isinstance(payload, bytes)
            else json.dumps(payload).encode("utf-8")
        )
        return _FakeResponse(body, status)

    opener.captured = captured
    return opener


def _health_document(**overrides: Any) -> dict[str, Any]:
    payload = {
        "status": "ok",
        "api_version": "pathfinder.data-agent/v1alpha1",
        "node_id": "node-origin",
        "representations": ["sampled_frames", "multimodal_digest"],
        "object_catalog_version": "catalog-v3",
    }
    payload.update(overrides)
    return payload


class HttpHealthProbeTest(unittest.TestCase):
    def _probe(self, opener, **overrides: Any) -> HttpDataAgentHealthProbe:
        return HttpDataAgentHealthProbe(
            _registry(),
            environment=TEST_ENVIRONMENT,
            opener=opener,
            **overrides,
        )

    def test_a_healthy_endpoint_is_verified_against_the_registry(
        self,
    ) -> None:
        opener = _opener_returning(_health_document())
        health = self._probe(opener).describe("origin_remote")
        self.assertTrue(health.healthy)
        self.assertEqual(200, health.status_code)
        self.assertEqual(
            "pathfinder.data-agent/v1alpha1",
            health.api_version,
        )
        self.assertEqual("node-origin", health.node_id)
        self.assertIn("sampled_frames", health.representations)
        self.assertEqual("catalog-v3", health.object_catalog_version)

    def test_the_probe_is_a_bounded_read_only_get(self) -> None:
        opener = _opener_returning(_health_document())
        self._probe(opener).describe("origin_remote")
        captured = opener.captured
        self.assertEqual("GET", captured["method"])
        self.assertTrue(captured["url"].endswith("/healthz"))
        self.assertEqual(30.0, captured["timeout"])

    def test_an_explicit_timeout_overrides_the_endpoint_default(
        self,
    ) -> None:
        opener = _opener_returning(_health_document())
        self._probe(opener, timeout_seconds=1.5).describe("origin_remote")
        self.assertEqual(1.5, opener.captured["timeout"])

    def test_no_token_is_sent_to_an_unauthenticated_health_route(
        self,
    ) -> None:
        opener = _opener_returning(_health_document())
        health = self._probe(opener).describe("origin_remote")
        headers = {
            k.lower(): v for k, v in opener.captured["headers"].items()
        }
        self.assertNotIn(
            "authorization",
            headers,
            "a liveness probe must not spend a bearer token by default",
        )
        self.assertFalse(health.authenticated)
        self.assertNotIn(
            TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
            json.dumps(opener.captured),
        )

    def test_a_token_is_sent_only_when_health_requires_auth(self) -> None:
        payload = _registry_payload()
        payload["endpoints"][0]["health_requires_auth"] = True
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        opener = _opener_returning(_health_document())
        probe = HttpDataAgentHealthProbe(
            registry,
            environment=TEST_ENVIRONMENT,
            opener=opener,
        )
        health = probe.describe("origin_remote")
        headers = {
            k.lower(): v for k, v in opener.captured["headers"].items()
        }
        self.assertEqual(
            f"Bearer {TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV]}",
            headers["authorization"],
        )
        self.assertTrue(health.authenticated)

    def test_the_token_never_reaches_a_report_either_way(self) -> None:
        for requires_auth in (False, True):
            with self.subTest(health_requires_auth=requires_auth):
                payload = _registry_payload()
                payload["endpoints"][0][
                    "health_requires_auth"
                ] = requires_auth
                registry = build_endpoint_registry(
                    payload,
                    source_sha256="0" * 64,
                )
                opener = _opener_returning(_health_document())
                health = HttpDataAgentHealthProbe(
                    registry,
                    environment=TEST_ENVIRONMENT,
                    opener=opener,
                ).describe("origin_remote")
                document = json.dumps(health.to_public_dict())
                self.assertNotIn(
                    TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
                    document,
                )
                self.assertFalse(
                    health.to_public_dict()["credentials_recorded"]
                )

    def test_a_failure_redacts_the_token_when_auth_is_enabled(self) -> None:
        payload = _registry_payload()
        payload["endpoints"][0]["health_requires_auth"] = True
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)

        def opener(request, timeout=None):
            raise OSError(
                "TLS failure carrying "
                + TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV]
            )

        with mock.patch.dict(os.environ, TEST_ENVIRONMENT):
            probe = HttpDataAgentHealthProbe(
                registry,
                environment=TEST_ENVIRONMENT,
                opener=opener,
            )
            with self.assertRaises(EndpointUnreachableError) as caught:
                probe.describe("origin_remote")
        self.assertNotIn(
            TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
            str(caught.exception),
        )
        self.assertIn("<redacted>", caught.exception.detail)

    def test_a_wrong_api_version_is_unhealthy(self) -> None:
        opener = _opener_returning(
            _health_document(api_version="pathfinder.data-agent/v0")
        )
        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)
        self.assertIn("api_version", health.detail)

    def test_a_wrong_node_id_is_unhealthy(self) -> None:
        opener = _opener_returning(_health_document(node_id="some-other"))
        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)
        self.assertIn("registry declares", health.detail)

    def test_absent_representation_capabilities_are_unhealthy(self) -> None:
        opener = _opener_returning(_health_document(representations=[]))
        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)
        self.assertIn("no representation capabilities", health.detail)

    def test_a_non_ok_status_field_is_unhealthy(self) -> None:
        opener = _opener_returning(_health_document(status="degraded"))
        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)

    def test_a_non_2xx_response_is_unhealthy_not_unreachable(self) -> None:
        opener = _opener_returning(_health_document(), status=503)
        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)
        self.assertEqual(503, health.status_code)

    def test_an_oversized_response_is_refused(self) -> None:
        opener = _opener_returning(b"x" * (MAXIMUM_HEALTH_RESPONSE_BYTES + 10))
        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)
        self.assertIn("size bound", health.detail)

    def test_unreadable_json_is_unhealthy(self) -> None:
        opener = _opener_returning(b"{not json")
        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)
        self.assertIn("unreadable health document", health.detail)

    def test_a_connection_error_is_endpoint_unreachable(self) -> None:
        def opener(request, timeout=None):
            raise OSError(
                "connection refused with "
                + TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV]
            )

        with mock.patch.dict(os.environ, TEST_ENVIRONMENT):
            with self.assertRaises(EndpointUnreachableError) as caught:
                self._probe(opener).describe("origin_remote")
        self.assertEqual("infrastructure", caught.exception.failure_class)
        self.assertNotIn(
            TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV],
            str(caught.exception),
        )

    def test_an_unset_url_is_endpoint_unreachable(self) -> None:
        opener = _opener_returning(_health_document())
        probe = HttpDataAgentHealthProbe(
            _registry(),
            environment={},
            opener=opener,
        )
        with self.assertRaises(EndpointUnreachableError):
            probe.describe("origin_remote")


    def test_a_redirect_is_refused_not_followed(self) -> None:
        from urllib.error import HTTPError

        for code in (301, 302, 303, 307, 308):
            with self.subTest(status=code):
                followed: list[str] = []

                def opener(request, timeout=None, _code=code):
                    followed.append(request.full_url)
                    raise HTTPError(
                        request.full_url,
                        _code,
                        "redirect",
                        {"Location": "https://elsewhere.invalid/healthz"},
                        None,
                    )

                health = self._probe(opener).describe("origin_remote")
                self.assertFalse(health.healthy)
                self.assertEqual(code, health.status_code)
                self.assertIn("refused", health.detail)
                self.assertEqual(
                    1,
                    len(followed),
                    "the probe must not request the redirect target",
                )

    def test_a_cross_origin_redirect_is_refused(self) -> None:
        from urllib.error import HTTPError

        requested: list[str] = []

        def opener(request, timeout=None):
            requested.append(request.full_url)
            raise HTTPError(
                request.full_url,
                302,
                "moved",
                {"Location": "https://attacker.invalid/healthz"},
                None,
            )

        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)
        self.assertEqual(1, len(requested))
        self.assertNotIn("attacker.invalid", json.dumps(
            health.to_public_dict()
        ))

    def test_a_redirect_status_returned_as_a_response_is_refused(
        self,
    ) -> None:
        opener = _opener_returning(_health_document(), status=302)
        health = self._probe(opener).describe("origin_remote")
        self.assertFalse(health.healthy)
        self.assertEqual(302, health.status_code)
        self.assertIn("refused", health.detail)

    def test_an_authenticated_redirect_never_forwards_the_token(
        self,
    ) -> None:
        from urllib.error import HTTPError

        payload = _registry_payload()
        payload["endpoints"][0]["health_requires_auth"] = True
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        seen: list[dict[str, str]] = []

        def opener(request, timeout=None):
            seen.append(
                {k.lower(): v for k, v in request.header_items()}
            )
            raise HTTPError(
                request.full_url,
                307,
                "temporary redirect",
                {"Location": "https://attacker.invalid/healthz"},
                None,
            )

        with mock.patch.dict(os.environ, TEST_ENVIRONMENT):
            health = HttpDataAgentHealthProbe(
                registry,
                environment=TEST_ENVIRONMENT,
                opener=opener,
            ).describe("origin_remote")
        self.assertFalse(health.healthy)
        # Exactly one request was made -- to the declared endpoint only.
        self.assertEqual(1, len(seen))
        self.assertIn("authorization", seen[0])
        report = json.dumps(health.to_public_dict())
        self.assertNotIn(TEST_ENVIRONMENT[ORIGIN_TOKEN_ENV], report)
        self.assertNotIn("attacker.invalid", report)

    def test_the_production_opener_refuses_redirects(self) -> None:
        from pathfinder.distributed.health import (
            _RefuseRedirects,
            _no_redirect_opener,
        )

        opener = _no_redirect_opener()
        self.assertTrue(
            any(
                isinstance(handler, _RefuseRedirects)
                for handler in opener.handlers
            ),
            "the default opener must refuse redirects, not follow them",
        )
        self.assertIsNone(
            _RefuseRedirects().redirect_request(
                None, None, 302, "", {}, "https://elsewhere.invalid"
            )
        )

    def test_the_health_mapping_matches_the_preflight_contract(
        self,
    ) -> None:
        opener = _opener_returning(_health_document())
        document = self._probe(opener).health("origin_remote")
        for key in ("healthy", "node_id"):
            self.assertIn(key, document)
        self.assertTrue(document["healthy"])
        self.assertEqual("node-origin", document["node_id"])


class PreflightModeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        self.registry = _registry()

    def _run(self, mode: str, **overrides: Any) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "probe": _FakeProbe(),
            "worker_pin": {"kind": "worker_alias", "value": "w"},
            "environment": TEST_ENVIRONMENT,
            "mode": mode,
        }
        arguments.update(overrides)
        return preflight_distributed_pilot(
            self.prereg,
            self.registry,
            **arguments,
        )

    def test_an_unknown_mode_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "preflight mode must be"):
            self._run("whatever")

    def test_offline_validation_tolerates_placeholders(self) -> None:
        report = self._run("offline_validation")
        self.assertEqual("ok", report["status"], report["failed_checks"])
        self.assertTrue(report["advisory_warnings"])

    def test_live_pilot_fails_on_placeholder_rate_provenance(self) -> None:
        report = self._run("live_pilot")
        self.assertEqual("failed", report["status"])
        self.assertIn(
            "cost_model.rates_are_measured_not_placeholder",
            report["failed_checks"],
        )
        self.assertEqual([], report["advisory_warnings"])

    def test_live_pilot_fails_on_a_placeholder_git_revision(self) -> None:
        report = self._run("live_pilot")
        self.assertIn(
            "identity.source_git_revision_is_real",
            report["failed_checks"],
        )

    def test_live_pilot_fails_on_placeholder_node_identities(self) -> None:
        payload = _registry_payload(execution_node_id="EXEC_PLACEHOLDER")
        payload["endpoints"][0]["node_id"] = "NODE_ORIGIN_PLACEHOLDER"
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        report = preflight_distributed_pilot(
            self.prereg,
            registry,
            probe=_FakeProbe(node_ids={
                "origin_remote": "NODE_ORIGIN_PLACEHOLDER",
            }),
            worker_pin={"kind": "worker_alias", "value": "w"},
            environment=TEST_ENVIRONMENT,
            mode="live_pilot",
        )
        self.assertIn(
            "identity.execution_node_is_real",
            report["failed_checks"],
        )
        self.assertIn(
            "identity.endpoint_node_ids_are_real",
            report["failed_checks"],
        )

    def test_live_pilot_fails_on_an_unset_endpoint_url(self) -> None:
        report = self._run("live_pilot", environment={})
        self.assertIn(
            "identity.endpoint_urls_are_configured",
            report["failed_checks"],
        )

    def test_live_pilot_fails_without_a_measurement_manifest(self) -> None:
        report = self._run("live_pilot")
        self.assertIn(
            "manifest.measurement_manifest_bound",
            report["failed_checks"],
        )
        bound = self._run(
            "live_pilot",
            measurement_manifest_sha256="c" * 64,
        )
        self.assertNotIn(
            "manifest.measurement_manifest_bound",
            bound["failed_checks"],
        )

    def test_a_zero_rate_passes_when_its_provenance_justifies_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            payload = _preregistration_payload()
            model = payload["total_cost_contract"]["cost_model"]
            model["rate_provenance"] = (
                "measured on the pilot deployment: this cluster bills no "
                "egress between the two nodes, so the network rate is a "
                "measured zero"
            )
            payload["source_git_revision"] = "b" * 40
            path = _write_preregistration(Path(temporary), payload)
            prereg = load_distributed_pilot_preregistration(path)
        report = preflight_distributed_pilot(
            prereg,
            self.registry,
            probe=_FakeProbe(),
            worker_pin={"kind": "worker_alias", "value": "w"},
            environment=TEST_ENVIRONMENT,
            mode="live_pilot",
            measurement_manifest_sha256="d" * 64,
        )
        self.assertNotIn(
            "cost_model.rates_are_measured_not_placeholder",
            report["failed_checks"],
        )
        self.assertNotIn(
            "cost_model.zero_rates_are_justified",
            report["failed_checks"],
        )
        self.assertEqual("ok", report["status"], report["failed_checks"])

    def test_workload_manifest_digests_are_always_checked(self) -> None:
        for mode in ("offline_validation", "live_pilot"):
            with self.subTest(mode=mode):
                report = self._run(mode)
                self.assertNotIn(
                    "manifest.workload_hashes_declared",
                    report["failed_checks"],
                )

    def test_preflight_separates_what_it_cannot_prove(self) -> None:
        joined = " ".join(self._run("offline_validation")["not_verified"])
        for expected in (
            "FlowMesh task dispatch",
            "worker-to-endpoint network reachability",
            "task result delivery",
            "future telemetry completeness",
        ):
            self.assertIn(expected, joined)


# --------------------------------------------------------------------------
# 6. AWM v3alpha5 compatibility
# --------------------------------------------------------------------------
def _ledger_payload(total: float) -> dict[str, Any]:
    model = load_cost_model(_cost_model_payload(
        conversion_rates={
            "network_cost_per_gib": 0.0,
            "storage_cost_per_gib_hour": 0.0,
            "materialization_cost_per_gib": 0.0,
            "transition_cost_per_gib": 0.0,
            "elapsed_time_cost_per_second": 0.0,
        },
    ))
    return build_cost_ledger(
        model,
        service_cost=total,
        transferred_bytes=0.0,
        stored_bytes=0.0,
        stored_hours=0.0,
        materialized_bytes=0.0,
        transition_bytes=0.0,
        transition_seconds=0.0,
    ).to_public_dict()


class AWMCompatibilityTest(unittest.TestCase):
    def _fixture(self, root: Path) -> _Fixture:
        return _Fixture(
            root,
            strata={"causal": _workload_ids("causal", 6)},
            plan=_plan(
                ("causal",),
                origin=_Cell(successes=1, cost=1.0),
                candidate=_Cell(successes=4, cost=0.2),
            ),
        )

    def _attach_ledgers(self, fixture: _Fixture) -> None:
        for design_id in fixture.design_ids:
            records = fixture.read_runs(fixture.oracle_output, design_id)
            for record in records:
                service = sum(
                    float(event["realized_cost"])
                    for event in record["access_events"]
                    if event["accepted"]
                )
                record["cost_ledger"] = _ledger_payload(service)
            fixture.write_runs(fixture.oracle_output, design_id, records)

    def test_service_cost_basis_is_the_backward_compatible_default(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._fixture(root)
            manifest, _, _ = fixture.certify(fixture.certificate_config())
            self.assertEqual("service_cost", manifest["cost_basis"])

    def test_total_cost_basis_reads_a_ledger_bearing_oracle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._fixture(root)
            self._attach_ledgers(fixture)
            config = fixture.certificate_config(
                overrides={"cost_basis": "total_cost"},
            )
            manifest, _, output = fixture.certify(config)
            self.assertEqual("total_cost", manifest["cost_basis"])
            evaluation = json.loads(
                (output / "certificate_evaluation.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(
                "total_cost",
                evaluation["statistical_method"]["cost_basis"],
            )
            self.assertIn(
                "does not change the statistical decision rule",
                evaluation["statistical_method"]["cost_basis_note"],
            )

    def test_the_two_bases_agree_when_the_ledger_holds_only_service_cost(
        self,
    ) -> None:
        # Same statistical rule, same numbers: only the selected scalar
        # differs, and here the two scalars are constructed to be equal.
        results = {}
        for basis in ("service_cost", "total_cost"):
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixture = self._fixture(root)
                self._attach_ledgers(fixture)
                config = fixture.certificate_config(
                    overrides={"cost_basis": basis},
                )
                _, _, output = fixture.certify(config)
                evaluation = json.loads(
                    (output / "certificate_evaluation.json").read_text(
                        encoding="utf-8"
                    )
                )
                results[basis] = [
                    (
                        stratum["stratum_id"],
                        stratum["certificate_state"],
                        stratum["point_estimates"]["cost_saving"],
                        stratum["gates"][1]["lower_bound"],
                    )
                    for stratum in evaluation["strata"]
                ]
        self.assertEqual(results["service_cost"], results["total_cost"])

    def test_total_cost_fails_closed_on_service_cost_only_data(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._fixture(root)
            config = fixture.certificate_config(
                overrides={"cost_basis": "total_cost"},
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "incomplete total-cost ledger",
            ):
                certify_awm_restricted_policy(
                    config,
                    fixture.oracle_path,
                    oracle_output_dir=fixture.oracle_output,
                    output_dir=root / "certificate-output",
                )

    def test_total_cost_fails_closed_on_an_incomplete_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._fixture(root)
            self._attach_ledgers(fixture)
            records = fixture.read_runs(fixture.oracle_output, SAFE)
            records[0]["cost_ledger"]["total_cost_available"] = False
            records[0]["cost_ledger"]["unavailable_components"] = ["network"]
            fixture.write_runs(fixture.oracle_output, SAFE, records)
            config = fixture.certificate_config(
                overrides={"cost_basis": "total_cost"},
            )
            with self.assertRaisesRegex(
                AWMConfigError,
                "incomplete total-cost ledger",
            ):
                certify_awm_restricted_policy(
                    config,
                    fixture.oracle_path,
                    oracle_output_dir=fixture.oracle_output,
                    output_dir=root / "certificate-output",
                )

    def test_an_unknown_cost_basis_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = self._fixture(Path(temporary))
            with self.assertRaises(AWMConfigError):
                fixture.certificate_config(
                    overrides={"cost_basis": "made_up_basis"},
                ) and certify_awm_restricted_policy(
                    fixture.certificate_config(
                        overrides={"cost_basis": "made_up_basis"},
                    ),
                    fixture.oracle_path,
                    oracle_output_dir=fixture.oracle_output,
                    output_dir=Path(temporary) / "out",
                )

    def test_the_pilot_contract_and_certificate_agree_on_the_policy(
        self,
    ) -> None:
        prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        certificate = json.loads(
            (
                ROOT / "configs"
                / "multi_candidate_formal_v2_awm_v3alpha5_certificate.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            certificate["safe_design_id"],
            prereg.safe_design_id,
        )
        self.assertEqual(
            sorted(certificate["excluded_design_ids"]),
            sorted(prereg.excluded_design_ids),
        )
        self.assertEqual(
            {
                stratum_id: payload["candidate_design_id"]
                for stratum_id, payload in certificate["strata"].items()
            },
            {
                stratum.stratum_id: stratum.candidate_design_id
                for stratum in prereg.strata
            },
        )


# --------------------------------------------------------------------------
# 7. CLI
# --------------------------------------------------------------------------
class DistributedCLITest(unittest.TestCase):
    def _cli(self, argv: list[str]) -> tuple[int, dict[str, Any]]:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = cli_main(argv)
        return code, json.loads(stdout.getvalue())

    def test_plan_command_prints_a_deterministic_plan(self) -> None:
        code, payload = self._cli([
            "plan-distributed-pilot",
            "--preregistration",
            str(EXAMPLE_PREREGISTRATION),
            "--compact",
        ])
        self.assertEqual(0, code)
        self.assertEqual(80, payload["planned_trial_count"])
        self.assertEqual(10, payload["independent_workload_count"])
        self.assertFalse(
            payload["preregistration"]["eligible_for_scientific_claims"]
        )

    def test_plan_command_freezes_and_resumes(self) -> None:
        prereg = load_distributed_pilot_preregistration(
            EXAMPLE_PREREGISTRATION
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "workloads.json"
            _write_json(manifest, {
                workload_id: {
                    "object_id": f"object-{workload_id}",
                    "question": f"Question for {workload_id}?",
                    "accepted_answer_substrings": ["answer"],
                }
                for workload_id in prereg.workload_ids
            })
            argv = [
                "plan-distributed-pilot",
                "--preregistration",
                str(EXAMPLE_PREREGISTRATION),
                "--workload-manifest",
                str(manifest),
                "--endpoint-registry",
                str(EXAMPLE_REGISTRY),
                "--output-dir",
                str(root / "plan"),
                "--compact",
            ]
            first, payload = self._cli(argv)
            second, _ = self._cli(argv)
            self.assertEqual(0, first)
            self.assertEqual(0, second)
            self.assertTrue(payload["workload_content_bound"])
            self.assertTrue(
                (
                    root / "plan" / "distributed_pilot_plan.json"
                ).is_file()
            )

    def test_plan_command_refuses_to_write_an_unbound_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "unbound"
            code, payload = self._cli([
                "plan-distributed-pilot",
                "--preregistration",
                str(EXAMPLE_PREREGISTRATION),
                "--output-dir",
                str(target),
                "--compact",
            ])
            self.assertEqual(2, code)
            self.assertIn("--output-dir requires", payload["message"])
            self.assertFalse(
                (target / "distributed_pilot_plan.json").exists()
            )

    def test_preflight_command_fails_closed_without_endpoints(self) -> None:
        code, payload = self._cli([
            "preflight-distributed-pilot",
            "--preregistration",
            str(EXAMPLE_PREREGISTRATION),
            "--endpoint-registry",
            str(EXAMPLE_REGISTRY),
            "--worker-alias",
            "pilot-worker",
            "--compact",
        ])
        self.assertEqual(1, code)
        self.assertEqual("failed", payload["status"])
        self.assertTrue(payload["failed_checks"])
        for value in payload["read_only"].values():
            self.assertFalse(value)
        self.assertFalse(payload["credentials_recorded"])

    def test_the_committed_registry_example_has_no_urls(self) -> None:
        text = EXAMPLE_REGISTRY.read_text(encoding="utf-8")
        for forbidden in ("/home/", "http://", "https://", "token\":"):
            self.assertNotIn(forbidden, text)
        registry = load_endpoint_registry(EXAMPLE_REGISTRY)
        self.assertEqual(
            ("local_materialized", "origin_remote"),
            registry.endpoint_ids,
        )


if __name__ == "__main__":
    unittest.main()
