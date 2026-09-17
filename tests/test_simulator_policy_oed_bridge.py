from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_logical_routes import (
    TRIALS_NAME as LOGICAL_TRIALS_NAME,
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_runtime import (
    FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
    FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
    FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
    full_flow_n1_score_request_id,
)
from pathfinder.simulator.full_flow_local_semantic_admission import (
    FrozenLocalSemanticExecutionInputs,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    NEUTRAL_OBSERVATION_CANDIDATE_SCHEMA_VERSION,
    SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
)
from pathfinder.simulator.hidden_oracle import (
    N1_SCORE_RESULT_SCHEMA_VERSION,
    build_n1_public_task_binding,
    build_n1_score_request,
)
from pathfinder.simulator.policy_oed_bridge import (
    CHECKSUMS_NAME,
    EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION,
    NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION,
    OBSERVATION_MANIFEST_NAME,
    OBSERVATIONS_NAME,
    OED_MANIFEST_NAME,
    OED_ROUTES_NAME,
    POLICY_MANIFEST_NAME,
    POLICY_ROUTES_NAME,
    PolicyOedBridgeError,
    freeze_full_flow_observations,
    freeze_oed_prospective_selection,
    freeze_policy_assignment,
    verify_full_flow_observations,
    verify_oed_prospective_selection,
    verify_policy_assignment,
)
from pathfinder.simulator.portable import build_portable_execution_plan


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _full_flow_evidence(
    route: dict,
    *,
    success: bool = True,
    schema_version: str = FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
) -> dict:
    return {
        "schema_version": schema_version,
        "status": "COMPLETE",
        "trial_key": route["trial_key"],
        "workload_id": route["workload_id"],
        "object_id": route["object_id"],
        "route": {
            "source_node_id": "N4",
            "executor_node_id": "N7",
            "inference_node_id": "N6",
        },
        "data_agent": {
            "artifact_size_bytes": 1000,
            "delivery": {
                "telemetry_complete": True,
                "exactly_one_full_download": True,
                "bytes_sent_equals_artifact_size": True,
                "bytes_sent": 1000,
            },
            "latency_ms": {
                "data_agent_service": 1.0,
                "client_access_round_trip": 2.0,
                "artifact_download_elapsed": 3.0,
                "server_reported_transfer": None,
            },
        },
        "semantic": {
            "service_time_ms": 4.0,
            "representation_delivery_bytes": 800,
        },
        "scoring": {"task_success": success},
        "route_unified": True,
        "real_object_identity_verified": True,
        "data_agent_source_identity_verified": True,
        "data_agent_artifact_delivery_verified": True,
        "semantic_frame_payload_integrity_verified": True,
        "semantic_health_verified": True,
        "scoring_verified": True,
        "telemetry_complete": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _generic_runtime_inputs(
    routes: list[dict],
) -> tuple[
    FrozenLocalSemanticExecutionInputs,
    dict[str, dict],
    dict[str, list[dict]],
]:
    bound_trials: list[dict] = []
    all_stages: list[dict] = []
    stages_by_trial: dict[str, list[dict]] = {}
    for route in routes:
        stage_keys = (
            route["execution_stage_keys"] + route["evaluation_stage_keys"]
        )
        stages: list[dict] = []
        for index, stage_key in enumerate(stage_keys):
            condition = None
            action = f"stage-{index:02d}"
            stage_suffix = stage_key.rsplit("|", 1)[-1]
            if route["route_family"] == "local-cache-derived":
                if index == 0:
                    action = "lookup"
                elif index == 1:
                    condition = {
                        "cache_operation_id": stage_keys[0].rsplit("|", 1)[-1],
                        "cache_operation_key": stage_keys[0],
                        "equals": "miss" if route["repetition"] == 0 else "hit",
                    }
            if stage_suffix == "infer":
                action = "infer"
            representation_identity = None
            representation_id = None
            if stage_suffix == "send-model-input":
                representation_id = route["representation_ids"][-1]
            elif stage_suffix == "send-digest":
                representation_id = "multimodal_digest"
            elif stage_suffix == "send-frames":
                representation_id = "sampled_frame_bundle"
            if representation_id is not None:
                representation_index = route["representation_ids"].index(
                    representation_id
                )
                representation_identity = {
                    "logical_object_id": route["object_id"],
                    "artifact_object_id": f"artifact-{route['object_id']}",
                    "representation_id": representation_id,
                    "representation_binding": {
                        "artifact_sha256": f"{representation_index + 1:064x}",
                        "artifact_size_bytes": 100 + representation_index,
                        "object_catalog_version": "test-catalog-v1",
                    },
                }
            dependencies = [] if index == 0 else [stage_keys[index - 1]]
            if stage_suffix == "infer":
                dependencies = [
                    key
                    for key in stage_keys[:index]
                    if key.rsplit("|", 1)[-1]
                    in {"send-model-input", "send-digest", "send-frames"}
                ]
            stage = {
                "stage_key": stage_key,
                "stage_index": index,
                "action": action,
                "condition": condition,
                "dependency_stage_keys": dependencies,
                "object_representation_identity": representation_identity,
            }
            stages.append(stage)
            all_stages.append(stage)
        stages_by_trial[route["trial_key"]] = stages
        artifact_object_id = f"artifact-{route['object_id']}"
        public_task = build_n1_public_task_binding(
            workload_id=route["workload_id"],
            object_id=artifact_object_id,
            task_class_id="video-qa",
            question="Which option best describes the video?",
            answer_options=[
                {"option_id": "A", "text": "First option"},
                {"option_id": "B", "text": "Second option"},
            ],
            success_scoring_rule="multiple-choice-option-id-exact-match-v1",
        )
        identities = []
        for index, representation_id in enumerate(route["representation_ids"]):
            identities.append({
                "logical_object_id": route["object_id"],
                "artifact_object_id": artifact_object_id,
                "representation_id": representation_id,
                "representation_binding": {
                    "artifact_sha256": f"{index + 1:064x}",
                    "artifact_size_bytes": 100 + index,
                    "object_catalog_version": "test-catalog-v1",
                },
            })
        bound = {
            "trial_key": route["trial_key"],
            "order_index": route["order_index"],
            "workload_id": route["workload_id"],
            "workload_class": route["workload_class"],
            "design_id": route["design_id"],
            "repetition": route["repetition"],
            "route_family": route["route_family"],
            "executor_node_id": route["executor_node_id"],
            "public_task_binding_sha256": public_task["task_binding_sha256"],
            "public_task_binding": public_task,
            "artifact_object_id": artifact_object_id,
            "representation_identities": identities,
            "semantic_stage_keys": stage_keys,
            "bound_stage_sha256": [
                _sha256(_canonical(stage)) for stage in stages
            ],
            "required_provisioning_chain_ids": [],
            "source_semantic_trial_sha256": _sha256(
                ("semantic-" + route["trial_key"]).encode()
            ),
            "flowmesh_submission_authorized": True,
            "credentials_recorded": False,
        }
        bound_trials.append(bound)
    inputs = FrozenLocalSemanticExecutionInputs(
        admission={
            "scenario_id": "flowmesh-infra-4x8-local-smoke-v1",
            "admission_sha256": "d" * 64,
        },
        bound_trials=tuple(bound_trials),
        bound_stages=tuple(all_stages),
        representative_smokes=(),
        adapter_inventory={},
    )
    return inputs, {
        row["trial_key"]: row for row in bound_trials
    }, stages_by_trial


def _generic_route_evidence(
    bound: dict,
    stages: list[dict],
    *,
    cache_branch: str | None = None,
) -> dict:
    stage_results = []
    component_ms: dict[str, float] = {}
    bytes_read = 0
    bytes_sent = 0
    for index, stage in enumerate(stages):
        inactive = (
            stage["condition"] is not None
            and stage["condition"]["equals"] != cache_branch
        )
        if inactive:
            stage_results.append({
                "stage_key": stage["stage_key"],
                "stage_index": stage["stage_index"],
                "action": stage["action"],
                "condition": stage["condition"],
                "state": "SKIPPED_INACTIVE_CONDITION",
                "outcome_kind": None,
                "outcome_sha256": None,
                "service_time_ms": 0.0,
                "bytes_read": 0,
                "bytes_sent": 0,
            })
            continue
        service_time_ms = float(index + 1)
        read = index * 10
        sent = index * 5
        stage_results.append({
            "stage_key": stage["stage_key"],
            "stage_index": stage["stage_index"],
            "action": stage["action"],
            "condition": stage["condition"],
            "state": "EXECUTED",
            "outcome_kind": "test",
            "outcome_sha256": _sha256(f"outcome-{index}".encode()),
            "service_time_ms": service_time_ms,
            "bytes_read": read,
            "bytes_sent": sent,
        })
        component_ms[stage["action"]] = (
            component_ms.get(stage["action"], 0.0) + service_time_ms
        )
        bytes_read += read
        bytes_sent += sent
    artifacts = []
    for item in bound["representation_identities"]:
        binding = item["representation_binding"]
        core = {
            "object_id": item["artifact_object_id"],
            "representation_id": item["representation_id"],
            "artifact_sha256": binding["artifact_sha256"],
            "artifact_size_bytes": binding["artifact_size_bytes"],
            "object_catalog_version": binding["object_catalog_version"],
        }
        identity = _sha256(_canonical(core))
        artifacts.append({
            "logical_object_id": item["logical_object_id"],
            **core,
            "identity_sha256": identity,
        })
    infer_stage = next(stage for stage in stages if stage["action"] == "infer")
    stages_by_key = {stage["stage_key"]: stage for stage in stages}
    input_representation_ids = {
        stages_by_key[key]["object_representation_identity"][
            "representation_id"
        ]
        for key in infer_stage["dependency_stage_keys"]
    }
    model_input_identity_digests = [
        artifact["identity_sha256"]
        for artifact in artifacts
        if artifact["representation_id"] in input_representation_ids
    ]
    model_input_bytes = 321
    score = 1.0
    predicted_answer = "A"
    oracle_id = "oracle-v1"
    run_id = "generic-route-run-v1"
    score_request = build_n1_score_request(
        score_request_id=_sha256(
            ("score-request-" + bound["trial_key"]).encode()
        ),
        oracle_id=oracle_id,
        run_id=run_id,
        trial_id=bound["trial_key"],
        object_id=bound["artifact_object_id"],
        task_binding_sha256=bound["public_task_binding_sha256"],
        predicted_answer=predicted_answer,
    )
    score_result = {
        "schema_version": N1_SCORE_RESULT_SCHEMA_VERSION,
        "status": "SCORED",
        "score_request_id": score_request["score_request_id"],
        "evaluation_unit_id": score_request["evaluation_unit_id"],
        "oracle_id": oracle_id,
        "node_id": "N1",
        "run_id": run_id,
        "trial_id": bound["trial_key"],
        "object_id": bound["artifact_object_id"],
        "task_binding_sha256": bound["public_task_binding_sha256"],
        "request_sha256": _sha256(_canonical(score_request)),
        "prediction_sha256": _sha256(predicted_answer.encode()),
        "success_scoring_rule": bound["public_task_binding"][
            "success_scoring_rule"
        ],
        "correct": True,
        "score": score,
        "public_task_set_sha256": _sha256(b"public-task-set"),
        "oracle_instance_hmac_sha256": _sha256(b"oracle-instance-hmac"),
        "score_evidence_hmac_sha256": _sha256(b"score-evidence-hmac"),
        "idempotent_replay": False,
        "hidden_answer_returned": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    score_result["result_content_sha256"] = _sha256(
        _canonical(score_result)
    )
    authentication_verification_sha256 = _sha256(_canonical({
        "domain": "pathfinder.authenticated-n1-score-verification/v1",
        "request_sha256": score_result["request_sha256"],
        "result_content_sha256": score_result["result_content_sha256"],
        "score_evidence_hmac_sha256": score_result[
            "score_evidence_hmac_sha256"
        ],
    }))
    candidate = {
        "schema_version": NEUTRAL_OBSERVATION_CANDIDATE_SCHEMA_VERSION,
        "trial_key": bound["trial_key"],
        "order_index": bound["order_index"],
        "workload_id": bound["workload_id"],
        "workload_class": bound["workload_class"],
        "design_id": bound["design_id"],
        "repetition": bound["repetition"],
        "object_id": bound["artifact_object_id"],
        "route_family": bound["route_family"],
        "executor_node_id": bound["executor_node_id"],
        "task_success": True,
        "score": score,
        "score_authenticity_verified": True,
        "score_authentication": "n1-hmac-verified",
        "component_service_time_ms": dict(sorted(component_ms.items())),
        "byte_measurements": {
            "adapter_bytes_read": bytes_read,
            "adapter_bytes_sent": bytes_sent,
            "semantic_input_bytes": model_input_bytes,
        },
        "monetary_measurement_available": False,
        "monetary_values_included": False,
        "synthetic_monetary_inputs_consumed": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    source_insert_trial_key = None
    if cache_branch == "hit":
        prefix, _, _ = bound["trial_key"].rpartition("|")
        source_insert_trial_key = f"{prefix}|r{bound['repetition'] - 1:04d}"
    evidence = {
        "schema_version": SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
        "status": "COMPLETE",
        "execution_id": _sha256(("execution-" + bound["trial_key"]).encode()),
        "request_sha256": _sha256(("request-" + bound["trial_key"]).encode()),
        "run_id": run_id,
        "trial_id": bound["trial_key"],
        "trial_key": bound["trial_key"],
        "trial_sha256": _sha256(_canonical(bound)),
        "stage_dag_sha256": _sha256(_canonical(stages)),
        "workload_id": bound["workload_id"],
        "workload_class": bound["workload_class"],
        "design_id": bound["design_id"],
        "repetition": bound["repetition"],
        "artifact_object_id": bound["artifact_object_id"],
        "public_task_binding_sha256": bound["public_task_binding_sha256"],
        "route": {
            "route_family": bound["route_family"],
            "executor_node_id": bound["executor_node_id"],
            "inference_node_id": "N6",
            "score_node_id": "N1",
        },
        "artifact_identities": artifacts,
        "provisioning_references": [],
        "stage_results": stage_results,
        "cache_branches": (
            [
                {
                    "lookup_stage_key": stages[0]["stage_key"],
                    "representation_id": artifact["representation_id"],
                    "branch": cache_branch,
                    "cache_node_id": bound["executor_node_id"],
                    "cache_id": "test-cache-v1",
                    "runtime_epoch": "a" * 32,
                    "source_insert_trial_key": source_insert_trial_key,
                    "lookup_sha256": _sha256(
                        ("lookup-" + artifact["representation_id"]).encode()
                    ),
                }
                for artifact in artifacts
            ]
            if cache_branch is not None
            else []
        ),
        "model_input": {
            "mode": "digest",
            "payload_sha256": _sha256(b"model-input"),
            "payload_size_bytes": model_input_bytes,
            "component_identity_sha256": model_input_identity_digests,
            "preparation_sha256": _sha256(b"preparation"),
        },
        "semantic": {
            "model": "vision-model",
            "input_sha256": _sha256(b"model-input"),
            "request_sha256": _sha256(b"semantic-request"),
            "result_sha256": _sha256(b"semantic-result"),
            "final_answer_sha256": _sha256(predicted_answer.encode()),
            "service_time_ms": 2.0,
        },
        "scoring": {
            "oracle_id": oracle_id,
            "score_request_id": score_request["score_request_id"],
            "evaluation_unit_id": score_request["evaluation_unit_id"],
            "task_binding_sha256": bound["public_task_binding_sha256"],
            "task_success": True,
            "score": score,
            "score_evidence_hmac_sha256": score_result[
                "score_evidence_hmac_sha256"
            ],
            "result_content_sha256": score_result[
                "result_content_sha256"
            ],
            "authentication_verification_sha256": (
                authentication_verification_sha256
            ),
            "authenticated_n1_v1alpha2": True,
        },
        "n1_score_request": score_request,
        "n1_score_result": score_result,
        "neutral_observation_candidate": candidate,
        "all_frozen_stages_accounted_for": True,
        "exclusive_cache_branches_verified": True,
        "n2_exact_range_required_for_indexed_raw": True,
        "n3_n4_artifact_identity_verified": True,
        "n5_provisioning_references_verified": True,
        "n6_input_mode_verified": True,
        "n1_exactly_once_authenticated_score_verified": True,
        "idempotent_replay": False,
        "endpoint_values_included": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    evidence["evidence_sha256"] = _sha256(_canonical(evidence))
    return evidence


def _matching_n1_verification(
    *,
    package_dir: str | Path,
    request: dict,
    result: dict,
    evidence_secret: bytes,
) -> dict:
    del package_dir, request, evidence_secret
    return {
        "correct": result["correct"],
        "score": result["score"],
    }


def _real_cost_manifest(plan_sha256: str, trial_keys: list[str]) -> dict:
    manifest = {
        "schema_version": EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION,
        "status": "FROZEN_EXTERNALLY_CALIBRATED_REAL_COSTS",
        "calibration_id": "external-metering-v1",
        "calibration_evidence_sha256": "c" * 64,
        "logical_route_plan_sha256": plan_sha256,
        "currency": "USD",
        "entries": [
            {
                "trial_key": key,
                "amount": float(index + 1) / 100,
                "measurement_sha256": f"{index + 1:064x}",
            }
            for index, key in enumerate(sorted(trial_keys))
        ],
        "external_calibration": True,
        "synthetic_simulator_inputs_used": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["manifest_sha256"] = _sha256(_canonical(manifest))
    return manifest


class PolicyOedBridgeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        cls.logical = cls.root / "logical"
        build_portable_execution_plan(SCENARIO, output_dir=cls.portable)
        plan_container_backend(
            SCENARIO,
            cls.portable,
            CONTAINER_SPEC,
            output_dir=cls.container,
        )
        compile_full_flow_logical_routes(
            SCENARIO,
            cls.container,
            output_dir=cls.logical,
        )
        cls.routes = _jsonl(cls.logical / LOGICAL_TRIALS_NAME)
        cls.route_map = {row["trial_key"]: row for row in cls.routes}

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def setUp(self) -> None:
        self.case_root = Path(tempfile.mkdtemp(dir=self.root))

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root)

    def _route(self, workload_class: str, design_id: str, repetition: int) -> dict:
        return next(
            row
            for row in self.routes
            if row["workload_class"] == workload_class
            and row["design_id"] == design_id
            and row["repetition"] == repetition
        )

    def _policy(self, output: Path) -> dict:
        return freeze_policy_assignment(
            logical_route_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
            policy_id="awm-policy-v1",
            awm_policy_sha256="a" * 64,
            assignments={
                "W1": ["D0"],
                "W2": ["D2"],
                "W3": ["D4"],
                "W4": ["D6"],
            },
            output_dir=output,
        )

    def test_policy_freeze_is_deterministic_and_compiles_routes(self) -> None:
        first = self.case_root / "policy-a"
        second = self.case_root / "policy-b"
        report = self._policy(first)
        self._policy(second)

        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(8, report["selected_trial_count"])
        self.assertEqual(
            sorted(path.name for path in first.iterdir()),
            [CHECKSUMS_NAME, POLICY_MANIFEST_NAME, POLICY_ROUTES_NAME],
        )
        for name in (CHECKSUMS_NAME, POLICY_MANIFEST_NAME, POLICY_ROUTES_NAME):
            self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())
        rows = _jsonl(first / POLICY_ROUTES_NAME)
        self.assertEqual(list(range(8)), [row["selection_index"] for row in rows])
        self.assertEqual(
            {("W1", "D0"), ("W2", "D2"), ("W3", "D4"), ("W4", "D6")},
            {(row["workload_class"], row["design_id"]) for row in rows},
        )
        self.assertTrue(all(not row["endpoint_binding_included"] for row in rows))

    def test_policy_rejects_incomplete_or_invalid_assignments(self) -> None:
        base = {"W1": ["D0"], "W2": ["D1"], "W3": ["D2"]}
        with self.assertRaisesRegex(PolicyOedBridgeError, "W1 through W4"):
            freeze_policy_assignment(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                policy_id="invalid-policy",
                awm_policy_sha256="a" * 64,
                assignments=base,
                output_dir=self.case_root / "invalid-a",
            )
        base["W4"] = ["D8"]
        with self.assertRaisesRegex(PolicyOedBridgeError, "allowed designs"):
            freeze_policy_assignment(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                policy_id="invalid-policy",
                awm_policy_sha256="a" * 64,
                assignments=base,
                output_dir=self.case_root / "invalid-b",
            )

    def test_policy_verifier_detects_tampering(self) -> None:
        output = self.case_root / "policy"
        self._policy(output)
        with (output / POLICY_ROUTES_NAME).open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaisesRegex(PolicyOedBridgeError, "checksum mismatch"):
            verify_policy_assignment(
                assignment_dir=output,
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
            )

    def test_oed_freezes_requested_order_without_outcomes(self) -> None:
        keys = [
            self._route("W4", "D7", 1)["trial_key"],
            self._route("W1", "D0", 0)["trial_key"],
            self._route("W3", "D5", 1)["trial_key"],
        ]
        first = self.case_root / "oed-a"
        second = self.case_root / "oed-b"
        for output in (first, second):
            report = freeze_oed_prospective_selection(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                oed_request_id="oed-request-v1",
                oed_request_sha256="b" * 64,
                requested_trial_keys=keys,
                output_dir=output,
            )
            self.assertFalse(report["bridge_consumed_outcomes"])
            self.assertTrue(report["order_frozen_before_execution"])
        self.assertEqual(
            keys,
            [row["trial_key"] for row in _jsonl(first / OED_ROUTES_NAME)],
        )
        for name in (CHECKSUMS_NAME, OED_MANIFEST_NAME, OED_ROUTES_NAME):
            self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())

    def test_oed_rejects_duplicate_and_unknown_trial_keys(self) -> None:
        key = self.routes[0]["trial_key"]
        for suffix, keys in (("duplicate", [key, key]), ("unknown", ["missing-trial"])):
            with self.subTest(suffix=suffix):
                with self.assertRaises(PolicyOedBridgeError):
                    freeze_oed_prospective_selection(
                        logical_route_plan_dir=self.logical,
                        scenario_path=SCENARIO,
                        container_plan_dir=self.container,
                        oed_request_id=f"oed-{suffix}",
                        oed_request_sha256="b" * 64,
                        requested_trial_keys=keys,
                        output_dir=self.case_root / suffix,
                    )

    def test_observations_are_neutral_sorted_and_have_no_monetary_cost(self) -> None:
        selected = [self.routes[9], self.routes[2]]
        evidence = [
            _full_flow_evidence(selected[0], success=False),
            _full_flow_evidence(selected[1], success=True),
        ]
        output = self.case_root / "observations"
        report = freeze_full_flow_observations(
            logical_route_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
            observation_set_id="neutral-observations-v1",
            evidence_records=evidence,
            output_dir=output,
        )

        self.assertEqual("VERIFIED", report["status"])
        self.assertFalse(report["monetary_cost_available"])
        manifest = _json(output / OBSERVATION_MANIFEST_NAME)
        rows = _jsonl(output / OBSERVATIONS_NAME)
        self.assertEqual(
            NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION,
            manifest["schema_version"],
        )
        self.assertFalse(manifest["end_to_end_latency_available"])
        self.assertFalse(manifest["eligible_for_scientific_claims"])
        self.assertEqual(
            sorted(row["source_order_index"] for row in rows),
            [row["source_order_index"] for row in rows],
        )
        self.assertTrue(all(row["monetary_cost"] is None for row in rows))
        self.assertEqual(
            1800,
            rows[0]["byte_measurements"]["two_leg_transfer_bytes_sum"],
        )
        self.assertEqual(4.0, rows[0]["latency_measurements_ms"]["semantic_service"])

    def test_generic_route_evidence_covers_all_route_families_and_executors(
        self,
    ) -> None:
        inputs, bound_by_trial, stages_by_trial = _generic_runtime_inputs(
            self.routes
        )
        selected = [
            self._route("W1", "D0", 0),
            self._route("W2", "D5", 0),
            self._route("W1", "D2", 0),
            self._route("W1", "D3", 0),
            self._route("W1", "D7", 1),
        ]
        evidence = []
        for route in selected:
            branch = None
            if route["route_family"] == "local-cache-derived":
                branch = "miss" if route["repetition"] == 0 else "hit"
            evidence.append(_generic_route_evidence(
                bound_by_trial[route["trial_key"]],
                stages_by_trial[route["trial_key"]],
                cache_branch=branch,
            ))
        output = self.case_root / "generic-observations"
        with patch(
            "pathfinder.simulator.policy_oed_bridge."
            "load_full_flow_local_semantic_execution_inputs",
            return_value=inputs,
        ), patch(
            "pathfinder.simulator.policy_oed_bridge.verify_n1_score_result",
            side_effect=_matching_n1_verification,
        ) as verify_score:
            report = freeze_full_flow_observations(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                observation_set_id="generic-route-observations-v1",
                evidence_records=evidence,
                semantic_execution_admission_dir=self.case_root / "admission",
                n1_oracle_package_dir=self.case_root / "oracle",
                n1_evidence_secret=b"s" * 32,
                output_dir=output,
            )

        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(0, report["legacy_full_flow_observation_count"])
        self.assertEqual(5, report["generic_semantic_route_observation_count"])
        self.assertGreaterEqual(verify_score.call_count, 10)
        self.assertEqual("d" * 64, report["semantic_execution_admission_sha256"])
        rows = _jsonl(output / OBSERVATIONS_NAME)
        self.assertEqual(
            {"raw", "indexed-raw", "remote-derived", "local-cache-derived"},
            {row["route_family"] for row in rows},
        )
        self.assertEqual({"N7", "N8"}, {row["executor_node_id"] for row in rows})
        self.assertEqual(
            {"hit", "miss"},
            {row["cache_branch"] for row in rows if row["cache_branch"]},
        )
        self.assertTrue(all(row["task_success"] for row in rows))
        self.assertTrue(all(row["score_authenticity_verified"] for row in rows))
        self.assertTrue(all(not row["end_to_end_latency_available"] for row in rows))
        self.assertTrue(all(not row["performance_evidence_claimed"] for row in rows))
        self.assertTrue(all(row["monetary_cost"] is None for row in rows))
        self.assertNotIn("correct_answer", json.dumps(rows))

    def test_verified_matrix_run_is_consumed_without_raw_result_recapture(
        self,
    ) -> None:
        inputs, bound_by_trial, stages_by_trial = _generic_runtime_inputs(
            self.routes
        )
        evidence = []
        for route in self.routes:
            branch = None
            if route["route_family"] == "local-cache-derived":
                branch = "miss" if route["repetition"] == 0 else "hit"
            evidence.append(_generic_route_evidence(
                bound_by_trial[route["trial_key"]],
                stages_by_trial[route["trial_key"]],
                cache_branch=branch,
            ))
        matrix_source = {
            "status": "VERIFIED_PUBLIC_ROUTE_EVIDENCE",
            "run_id": "matrix-run-v1",
            "report_sha256": "1" * 64,
            "report_file_sha256": "2" * 64,
            "route_evidence_file_sha256": "3" * 64,
            "route_evidence_count": 64,
            "evidence_records": evidence,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        output = self.case_root / "matrix-run-observations"
        with patch(
            "pathfinder.simulator.policy_oed_bridge."
            "load_full_flow_local_semantic_execution_inputs",
            return_value=inputs,
        ), patch(
            "pathfinder.simulator.policy_oed_bridge."
            "load_full_flow_semantic_matrix_route_evidence",
            return_value=matrix_source,
        ) as load_matrix, patch(
            "pathfinder.simulator.policy_oed_bridge.verify_n1_score_result",
            side_effect=_matching_n1_verification,
        ) as verify_score:
            report = freeze_full_flow_observations(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                observation_set_id="verified-matrix-run-v1",
                semantic_matrix_run_dir=self.case_root / "matrix-run",
                semantic_execution_admission_dir=self.case_root / "admission",
                n1_oracle_package_dir=self.case_root / "oracle",
                n1_evidence_secret=b"s" * 32,
                output_dir=output,
            )

        self.assertGreaterEqual(load_matrix.call_count, 2)
        self.assertGreaterEqual(verify_score.call_count, 128)
        self.assertEqual("verified-semantic-matrix-run", report[
            "evidence_source_kind"
        ])
        self.assertTrue(report["semantic_matrix_run_integrity_verified"])
        self.assertEqual("matrix-run-v1", report["semantic_matrix_run_id"])
        self.assertEqual("1" * 64, report[
            "semantic_matrix_run_report_sha256"
        ])
        self.assertEqual("3" * 64, report[
            "semantic_matrix_route_evidence_file_sha256"
        ])
        manifest = _json(output / OBSERVATION_MANIFEST_NAME)
        self.assertEqual(64, manifest[
            "generic_semantic_route_observation_count"
        ])
        self.assertEqual("2" * 64, manifest[
            "semantic_matrix_run_report_file_sha256"
        ])

    def test_observation_source_is_strictly_mutually_exclusive(self) -> None:
        route = self.routes[0]
        evidence = [_full_flow_evidence(route)]
        with self.assertRaisesRegex(
            PolicyOedBridgeError,
            "exactly one",
        ):
            freeze_full_flow_observations(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                observation_set_id="ambiguous-source-v1",
                evidence_records=evidence,
                semantic_matrix_run_dir=self.case_root / "matrix-run",
                output_dir=self.case_root / "ambiguous-source",
            )

    def test_generic_route_evidence_requires_its_promoted_admission(self) -> None:
        inputs, bound_by_trial, stages_by_trial = _generic_runtime_inputs(
            self.routes
        )
        route = self._route("W2", "D1", 0)
        evidence = _generic_route_evidence(
            bound_by_trial[route["trial_key"]],
            stages_by_trial[route["trial_key"]],
        )
        with self.assertRaisesRegex(
            PolicyOedBridgeError,
            "requires a local semantic admission",
        ):
            freeze_full_flow_observations(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                observation_set_id="generic-without-admission",
                evidence_records=[evidence],
                output_dir=self.case_root / "missing-admission",
            )

        changed_inputs = FrozenLocalSemanticExecutionInputs(
            admission=inputs.admission,
            bound_trials=tuple(
                {
                    **row,
                    "route_family": "raw",
                }
                if row["trial_key"] == route["trial_key"]
                else row
                for row in inputs.bound_trials
            ),
            bound_stages=inputs.bound_stages,
            representative_smokes=(),
            adapter_inventory={},
        )
        with patch(
            "pathfinder.simulator.policy_oed_bridge."
            "load_full_flow_local_semantic_execution_inputs",
            return_value=changed_inputs,
        ):
            with self.assertRaisesRegex(
                PolicyOedBridgeError,
                "differs from logical route",
            ):
                freeze_full_flow_observations(
                    logical_route_plan_dir=self.logical,
                    scenario_path=SCENARIO,
                    container_plan_dir=self.container,
                    observation_set_id="generic-wrong-admission",
                    evidence_records=[evidence],
                    semantic_execution_admission_dir=(
                        self.case_root / "wrong-admission"
                    ),
                    n1_oracle_package_dir=self.case_root / "oracle",
                    n1_evidence_secret=b"s" * 32,
                    output_dir=self.case_root / "wrong-source",
                )

    def test_generic_route_requires_privileged_n1_and_rejects_score_tampering(
        self,
    ) -> None:
        inputs, bound_by_trial, stages_by_trial = _generic_runtime_inputs(
            self.routes
        )
        route = self._route("W1", "D2", 0)
        evidence = _generic_route_evidence(
            bound_by_trial[route["trial_key"]],
            stages_by_trial[route["trial_key"]],
        )
        with patch(
            "pathfinder.simulator.policy_oed_bridge."
            "load_full_flow_local_semantic_execution_inputs",
            return_value=inputs,
        ):
            with self.assertRaisesRegex(
                PolicyOedBridgeError,
                "requires privileged N1 verification",
            ):
                freeze_full_flow_observations(
                    logical_route_plan_dir=self.logical,
                    scenario_path=SCENARIO,
                    container_plan_dir=self.container,
                    observation_set_id="generic-without-privileged-n1",
                    evidence_records=[evidence],
                    semantic_execution_admission_dir=(
                        self.case_root / "admission"
                    ),
                    output_dir=self.case_root / "without-privileged-n1",
                )

        tampered = json.loads(json.dumps(evidence))
        tampered["scoring"]["task_success"] = False
        tampered["scoring"]["score"] = 0.0
        tampered["n1_score_result"]["correct"] = False
        tampered["n1_score_result"]["score"] = 0.0
        tampered["n1_score_result"].pop("result_content_sha256")
        tampered["n1_score_result"]["result_content_sha256"] = _sha256(
            _canonical(tampered["n1_score_result"])
        )
        tampered["scoring"]["result_content_sha256"] = tampered[
            "n1_score_result"
        ]["result_content_sha256"]
        tampered["scoring"]["authentication_verification_sha256"] = _sha256(
            _canonical({
                "domain": (
                    "pathfinder.authenticated-n1-score-verification/v1"
                ),
                "request_sha256": tampered["n1_score_result"][
                    "request_sha256"
                ],
                "result_content_sha256": tampered["n1_score_result"][
                    "result_content_sha256"
                ],
                "score_evidence_hmac_sha256": tampered["n1_score_result"][
                    "score_evidence_hmac_sha256"
                ],
            })
        )
        tampered["neutral_observation_candidate"]["task_success"] = False
        tampered["neutral_observation_candidate"]["score"] = 0.0
        tampered.pop("evidence_sha256")
        tampered["evidence_sha256"] = _sha256(_canonical(tampered))
        tampered_output = self.case_root / "tampered-score"
        with patch(
            "pathfinder.simulator.policy_oed_bridge."
            "load_full_flow_local_semantic_execution_inputs",
            return_value=inputs,
        ), patch(
            "pathfinder.simulator.policy_oed_bridge.verify_n1_score_result",
            return_value={"correct": True, "score": 1.0},
        ) as verify_score:
            with self.assertRaisesRegex(
                PolicyOedBridgeError,
                "not bound to N6, N1, and the frozen trial",
            ):
                freeze_full_flow_observations(
                    logical_route_plan_dir=self.logical,
                    scenario_path=SCENARIO,
                    container_plan_dir=self.container,
                    observation_set_id="generic-tampered-score",
                    evidence_records=[tampered],
                    semantic_execution_admission_dir=(
                        self.case_root / "admission"
                    ),
                    n1_oracle_package_dir=self.case_root / "oracle",
                    n1_evidence_secret=b"s" * 32,
                    output_dir=tampered_output,
                )
        self.assertEqual(1, verify_score.call_count)
        self.assertFalse((tampered_output / OBSERVATIONS_NAME).exists())

    def test_generic_route_rejects_stage_or_hidden_label_tampering(self) -> None:
        inputs, bound_by_trial, stages_by_trial = _generic_runtime_inputs(
            self.routes
        )
        route = self._route("W1", "D0", 0)
        valid = _generic_route_evidence(
            bound_by_trial[route["trial_key"]],
            stages_by_trial[route["trial_key"]],
        )
        cases = []
        changed_stage = json.loads(json.dumps(valid))
        changed_stage["stage_results"][0]["action"] = "different-action"
        changed_stage.pop("evidence_sha256")
        changed_stage["evidence_sha256"] = _sha256(_canonical(changed_stage))
        cases.append(("stage", changed_stage))
        hidden = json.loads(json.dumps(valid))
        hidden["scoring"]["correct_answer_id"] = "A"
        hidden.pop("evidence_sha256")
        hidden["evidence_sha256"] = _sha256(_canonical(hidden))
        cases.append(("hidden", hidden))
        for suffix, evidence in cases:
            with self.subTest(suffix=suffix), patch(
                "pathfinder.simulator.policy_oed_bridge."
                "load_full_flow_local_semantic_execution_inputs",
                return_value=inputs,
            ), patch(
                "pathfinder.simulator.policy_oed_bridge."
                "verify_n1_score_result",
                side_effect=_matching_n1_verification,
            ):
                with self.assertRaises(PolicyOedBridgeError):
                    freeze_full_flow_observations(
                        logical_route_plan_dir=self.logical,
                        scenario_path=SCENARIO,
                        container_plan_dir=self.container,
                        observation_set_id=f"generic-tamper-{suffix}",
                        evidence_records=[evidence],
                        semantic_execution_admission_dir=(
                            self.case_root / "admission"
                        ),
                        n1_oracle_package_dir=self.case_root / "oracle",
                        n1_evidence_secret=b"s" * 32,
                        output_dir=self.case_root / f"tamper-{suffix}",
                    )

    def test_hidden_oracle_v2_evidence_becomes_neutral_observation(self) -> None:
        route = next(
            row
            for row in self.routes
            if row["design_id"] == "D2"
        )
        evidence = _full_flow_evidence(
            route,
            schema_version=FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
        )
        evidence["full_flow_request_id"] = "hidden-v2-request"
        evidence["run_id"] = "hidden-v2-run"
        evidence["trial_id"] = "hidden-v2-trial"
        evidence["frozen_binding_sha256"] = "f" * 64
        task_binding_sha256 = "b" * 64
        final_answer = "A"
        score_request_id = full_flow_n1_score_request_id(
            {
                "schema_version": FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
                "oracle_id": "hidden-oracle-v1",
                "run_id": evidence["run_id"],
                "trial_id": evidence["trial_id"],
                "full_flow_request_id": evidence["full_flow_request_id"],
                "frozen_binding_sha256": evidence[
                    "frozen_binding_sha256"
                ],
                "task_binding_sha256": task_binding_sha256,
            },
            final_answer=final_answer,
        )
        evidence["scoring"] = {
            "task_success": True,
            "score": 1.0,
            "final_answer": final_answer,
            "oracle_result": {
                "score_request_id": score_request_id,
                "oracle_id": "hidden-oracle-v1",
                "task_binding_sha256": task_binding_sha256,
                "hidden_answer_returned": False,
            },
        }
        output = self.case_root / "v2-observations"
        with self.assertRaisesRegex(
            PolicyOedBridgeError,
            "requires privileged N1 verification",
        ):
            freeze_full_flow_observations(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                observation_set_id="hidden-v2-without-auth",
                evidence_records=[evidence],
                output_dir=self.case_root / "v2-without-auth",
            )
        with patch(
            "pathfinder.simulator.policy_oed_bridge.verify_n1_score_result",
            return_value={"correct": True},
        ) as verify_score:
            report = freeze_full_flow_observations(
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                observation_set_id="hidden-v2-observations",
                evidence_records=[evidence],
                output_dir=output,
                n1_oracle_package_dir=self.case_root / "oracle",
                n1_evidence_secret=b"s" * 32,
            )
        self.assertEqual("VERIFIED", report["status"])
        self.assertTrue(report["all_score_authenticity_verified"])
        self.assertGreaterEqual(verify_score.call_count, 2)
        row = _jsonl(output / OBSERVATIONS_NAME)[0]
        self.assertEqual(
            FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
            row["source_full_flow_evidence_schema_version"],
        )
        self.assertTrue(row["task_success"])
        self.assertTrue(row["score_authenticity_verified"])
        self.assertEqual("n1-hmac-verified", row["score_authentication"])
        self.assertNotIn("correct_answer_id", json.dumps(row))

    def test_observations_reject_synthetic_cost_and_scientific_claims(self) -> None:
        route = self.routes[0]
        cases = []
        cost = _full_flow_evidence(route)
        cost["realized_cost"] = 0.2
        cases.append(cost)
        hint = _full_flow_evidence(route)
        hint["synthetic_service_hint"] = "cheap"
        cases.append(hint)
        scientific = _full_flow_evidence(route)
        scientific["eligible_for_scientific_claims"] = True
        cases.append(scientific)
        for index, evidence in enumerate(cases):
            with self.subTest(index=index):
                with self.assertRaises(PolicyOedBridgeError):
                    freeze_full_flow_observations(
                        logical_route_plan_dir=self.logical,
                        scenario_path=SCENARIO,
                        container_plan_dir=self.container,
                        observation_set_id=f"rejected-{index}",
                        evidence_records=[evidence],
                        output_dir=self.case_root / f"rejected-{index}",
                    )

    def test_external_cost_does_not_grant_claim_eligibility(self) -> None:
        selected = [self.routes[0], self.routes[1]]
        evidence = [_full_flow_evidence(route) for route in selected]
        logical_plan = _json(self.logical / "logical-route-plan.json")
        costs = _real_cost_manifest(
            logical_plan["plan_sha256"],
            [row["trial_key"] for row in selected],
        )
        output = self.case_root / "costed"
        report = freeze_full_flow_observations(
            logical_route_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
            observation_set_id="externally-costed-v1",
            evidence_records=evidence,
            external_real_cost_manifest=costs,
            output_dir=output,
        )

        self.assertTrue(report["monetary_cost_available"])
        self.assertFalse(report["eligible_for_scientific_claims"])
        rows = _jsonl(output / OBSERVATIONS_NAME)
        self.assertTrue(all(row["monetary_cost_available"] for row in rows))
        self.assertEqual("USD", rows[0]["monetary_cost"]["currency"])
        self.assertFalse(_json(output / OBSERVATION_MANIFEST_NAME)[
            "external_cost_claim_independently_verified"
        ])

    def test_external_real_cost_rejects_synthetic_or_incomplete_manifests(self) -> None:
        route = self.routes[0]
        evidence = [_full_flow_evidence(route)]
        logical_plan = _json(self.logical / "logical-route-plan.json")
        valid = _real_cost_manifest(
            logical_plan["plan_sha256"],
            [route["trial_key"]],
        )
        synthetic = dict(valid)
        synthetic["synthetic_simulator_inputs_used"] = True
        synthetic["manifest_sha256"] = _sha256(_canonical({
            key: value
            for key, value in synthetic.items()
            if key != "manifest_sha256"
        }))
        missing = dict(valid)
        missing["entries"] = []
        missing["manifest_sha256"] = _sha256(_canonical({
            key: value
            for key, value in missing.items()
            if key != "manifest_sha256"
        }))
        for index, manifest in enumerate((synthetic, missing)):
            with self.subTest(index=index):
                with self.assertRaises(PolicyOedBridgeError):
                    freeze_full_flow_observations(
                        logical_route_plan_dir=self.logical,
                        scenario_path=SCENARIO,
                        container_plan_dir=self.container,
                        observation_set_id=f"bad-cost-{index}",
                        evidence_records=evidence,
                        external_real_cost_manifest=manifest,
                        output_dir=self.case_root / f"bad-cost-{index}",
                    )

    def test_observation_verifier_detects_tampering(self) -> None:
        route = self.routes[0]
        evidence = [_full_flow_evidence(route)]
        output = self.case_root / "observations"
        freeze_full_flow_observations(
            logical_route_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
            observation_set_id="tamper-test-v1",
            evidence_records=evidence,
            output_dir=output,
        )
        with (output / OBSERVATIONS_NAME).open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaisesRegex(PolicyOedBridgeError, "checksum mismatch"):
            verify_full_flow_observations(
                observation_dir=output,
                logical_route_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
                evidence_records=evidence,
            )

    def test_output_directories_are_immutable(self) -> None:
        output = self.case_root / "policy"
        self._policy(output)
        with self.assertRaisesRegex(PolicyOedBridgeError, "already exists"):
            self._policy(output)


if __name__ == "__main__":
    unittest.main()
