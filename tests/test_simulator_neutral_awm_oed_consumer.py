"""Focused tests for the offline neutral-observation AWM/OED consumer."""

from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

import pathfinder.simulator as simulator_api
from pathfinder.cli import _parser, main as cli_main
from pathfinder.simulator.neutral_awm_oed_consumer import (
    CHECKSUMS_NAME,
    DATASET_NAME,
    EVALUATION_NAME,
    MANIFEST_NAME,
    OED_SELECTION_NAME,
    NeutralAwmOedConsumerError,
    freeze_neutral_awm_oed_analysis,
    verify_neutral_awm_oed_analysis,
)
from pathfinder.simulator.policy_oed_bridge import (
    EXTERNAL_COST_NAME,
    EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION,
    NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION,
    NEUTRAL_OBSERVATION_SCHEMA_VERSION,
    OBSERVATION_MANIFEST_NAME,
    OBSERVATIONS_NAME,
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _json_document(value: object) -> bytes:
    return _canonical(value) + b"\n"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _observation(
    workload_index: int,
    design_index: int,
    repetition: int,
    *,
    monetary: bool,
) -> dict:
    workload_class = f"W{workload_index}"
    design_id = f"D{design_index}"
    trial_key = f"scenario|{workload_class}|{design_id}|r{repetition:04d}"
    digest_seed = workload_index * 100 + design_index * 10 + repetition + 1
    digest = f"{digest_seed:064x}"
    cost_amount = None
    if monetary:
        cost_amount = 10.0 if design_index == 0 else 4.0 + design_index
    return {
        "schema_version": NEUTRAL_OBSERVATION_SCHEMA_VERSION,
        "trial_key": trial_key,
        "source_full_flow_evidence_sha256": digest,
        "source_full_flow_evidence_schema_version": (
            "pathfinder.semantic-route-evidence/v1alpha2"
        ),
        "source_route_row_sha256": digest,
        "source_semantic_admission_sha256": "a" * 64,
        "source_bound_trial_sha256": "b" * 64,
        "source_stage_dag_sha256": "c" * 64,
        "source_route_evidence_commitment_sha256": "d" * 64,
        "source_order_index": (
            (workload_index - 1) * 16 + design_index * 2 + repetition
        ),
        "workload_id": f"workload-{workload_class}",
        "workload_class": workload_class,
        "design_id": design_id,
        "repetition": repetition,
        "object_id": f"object-{workload_class}",
        "route_family": (
            "local-cache-derived" if design_index in {6, 7} else "remote-derived"
        ),
        "executor_node_id": "N7" if design_index % 2 == 0 else "N8",
        "cache_branch": (
            ("miss" if repetition == 0 else "hit")
            if design_index in {6, 7}
            else None
        ),
        "task_success": design_index != 0,
        "score_authenticity_verified": True,
        "score_authentication": "n1-privileged-offline-hmac-verification",
        "latency_measurements_ms": {
            "semantic_inference": 100.0 + design_index,
            "storage_read": 10.0 + repetition,
        },
        "latency_measurement_semantics": (
            "executed-stage-service-time-sums-not-end-to-end"
        ),
        "end_to_end_latency_available": False,
        "byte_measurements": {
            "adapter_bytes_read": 1000 + design_index,
            "adapter_bytes_sent": 500 + repetition,
            "semantic_input_bytes": 100,
        },
        "byte_measurement_semantics": (
            "adapter-read-write-and-semantic-input-not-network-throughput"
        ),
        "monetary_cost_available": monetary,
        "monetary_cost": (
            {
                "amount": cost_amount,
                "currency": "USD",
                "measurement_sha256": f"{digest_seed + 1000:064x}",
            }
            if monetary
            else None
        ),
        "synthetic_simulator_cost_hints_consumed": False,
        "performance_evidence_claimed": False,
        "scientific_evidence_claimed": False,
        "hidden_label_values_included": False,
        "hidden_label_values_consumed_by_bridge": False,
        "credentials_recorded": False,
    }


def _write_source(
    root: Path,
    *,
    monetary: bool = False,
    omit_cell: tuple[str, str, int] | None = None,
    mutation=None,
) -> Path:
    root.mkdir()
    rows = []
    for workload_index in range(1, 5):
        for design_index in range(8):
            for repetition in range(2):
                coordinate = (
                    f"W{workload_index}",
                    f"D{design_index}",
                    repetition,
                )
                if coordinate == omit_cell:
                    continue
                rows.append(_observation(
                    workload_index,
                    design_index,
                    repetition,
                    monetary=monetary,
                ))
    if mutation is not None:
        mutation(rows)
    rows.sort(key=lambda row: row["source_order_index"])
    observations_bytes = b"".join(_json_document(row) for row in rows)
    (root / OBSERVATIONS_NAME).write_bytes(observations_bytes)

    logical_sha = "e" * 64
    external = None
    if monetary:
        entries = [
            {
                "trial_key": row["trial_key"],
                "amount": row["monetary_cost"]["amount"],
                "measurement_sha256": row["monetary_cost"][
                    "measurement_sha256"
                ],
            }
            for row in sorted(rows, key=lambda item: item["trial_key"])
        ]
        external = {
            "schema_version": EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION,
            "status": "FROZEN_EXTERNALLY_CALIBRATED_REAL_COSTS",
            "calibration_id": "metered-cost-v1",
            "calibration_evidence_sha256": "f" * 64,
            "logical_route_plan_sha256": logical_sha,
            "currency": "USD",
            "entries": entries,
            "external_calibration": True,
            "synthetic_simulator_inputs_used": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        external["manifest_sha256"] = _sha256(_canonical(external))
        (root / EXTERNAL_COST_NAME).write_bytes(_json_document(external))

    manifest = {
        "schema_version": NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION,
        "status": "FROZEN_NEUTRAL_FULL_FLOW_OBSERVATIONS",
        "observation_set_id": "neutral-source-v1",
        "logical_route_plan_sha256": logical_sha,
        "scenario_id": "scenario-v1",
        "observation_count": len(rows),
        "legacy_full_flow_observation_count": 0,
        "generic_semantic_route_observation_count": len(rows),
        "evidence_source_kind": "verified-semantic-matrix-run",
        "semantic_matrix_run_id": "matrix-run-v1",
        "semantic_matrix_run_report_sha256": "1" * 64,
        "semantic_matrix_run_report_file_sha256": "2" * 64,
        "semantic_matrix_route_evidence_file_sha256": "3" * 64,
        "semantic_matrix_run_integrity_verified": True,
        "semantic_execution_admission_sha256": "4" * 64,
        "trial_keys_sha256": _sha256(_canonical([
            row["trial_key"] for row in rows
        ])),
        "observations_file_sha256": _sha256(observations_bytes),
        "task_success_available": True,
        "all_score_authenticity_verified": True,
        "hidden_v2_score_authentication_required": True,
        "component_latency_available": True,
        "end_to_end_latency_available": False,
        "measured_bytes_available": True,
        "monetary_cost_available": monetary,
        "external_real_cost_manifest_sha256": (
            external["manifest_sha256"] if external is not None else None
        ),
        "external_real_cost_file_sha256": (
            _sha256((root / EXTERNAL_COST_NAME).read_bytes())
            if external is not None
            else None
        ),
        "external_cost_claim_independently_verified": False,
        "synthetic_simulator_cost_hints_consumed": False,
        "hidden_label_values_included": False,
        "hidden_label_values_consumed_by_bridge": False,
        "component_measurements_are_performance_claims": False,
        "performance_analysis_performed": False,
        "generic_route_source_trial_stage_binding_verified": True,
        "statistical_analysis_performed": False,
        "awm_oed_mathematics_modified": False,
        "endpoint_free": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["observation_manifest_sha256"] = _sha256(_canonical(manifest))
    (root / OBSERVATION_MANIFEST_NAME).write_bytes(_json_document(manifest))
    names = [OBSERVATION_MANIFEST_NAME, OBSERVATIONS_NAME]
    if monetary:
        names.append(EXTERNAL_COST_NAME)
    (root / CHECKSUMS_NAME).write_text(
        "".join(
            f"{_sha256((root / name).read_bytes())}  {name}\n"
            for name in sorted(names)
        ),
        encoding="utf-8",
    )
    return root


class NeutralAwmOedConsumerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_full_matrix_produces_quality_only_policy_and_oed_plan(self) -> None:
        source = _write_source(self.root / "source")
        output = self.root / "analysis"
        report = freeze_neutral_awm_oed_analysis(
            observation_dir=source,
            analysis_id="analysis-v1",
            output_dir=output,
        )

        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(64, report["trial_count"])
        self.assertFalse(report["monetary_cost_available"])
        self.assertFalse(report["weighted_certificate_core_used"])
        self.assertEqual(
            "NOT_EVALUATED_MISSING_EXTERNAL_REAL_COST",
            report["mathematical_certificate_state"],
        )
        self.assertFalse(report["commit_authorized"])
        self.assertEqual(64, len(_read_jsonl(output / DATASET_NAME)))
        evaluation = _read_json(output / EVALUATION_NAME)
        self.assertEqual(
            [{"workload_class": f"W{index}", "design_id": "D1"}
             for index in range(1, 5)],
            evaluation["policy_assignments"],
        )
        self.assertEqual(4, len(_read_jsonl(output / OED_SELECTION_NAME)))
        self.assertEqual(
            "POSTHOC_SIMULATOR_ANALYSIS_ONLY",
            evaluation["decision_state"],
        )
        self.assertFalse(evaluation["eligible_for_scientific_claims"])
        self.assertEqual(
            "VERIFIED",
            verify_neutral_awm_oed_analysis(
                observation_dir=source,
                analysis_dir=output,
            )["status"],
        )

    def test_missing_cost_fails_closed_when_certificate_is_required(self) -> None:
        source = _write_source(self.root / "source")
        with self.assertRaisesRegex(
            NeutralAwmOedConsumerError,
            "external real cost is required but absent",
        ):
            freeze_neutral_awm_oed_analysis(
                observation_dir=source,
                analysis_id="requires-cost-v1",
                output_dir=self.root / "analysis",
                require_real_cost=True,
                cost_saving_support=(-10.0, 10.0),
            )

    def test_cost_support_is_validated_even_without_cost_observations(self) -> None:
        source = _write_source(self.root / "source")
        with self.assertRaisesRegex(
            NeutralAwmOedConsumerError,
            "lower must be less than upper",
        ):
            freeze_neutral_awm_oed_analysis(
                observation_dir=source,
                analysis_id="invalid-support-v1",
                output_dir=self.root / "analysis",
                cost_saving_support=(10.0, -10.0),
            )

    def test_external_cost_uses_existing_weighted_certificate_core(self) -> None:
        source = _write_source(self.root / "source", monetary=True)
        output = self.root / "analysis"
        report = freeze_neutral_awm_oed_analysis(
            observation_dir=source,
            analysis_id="costed-analysis-v1",
            output_dir=output,
            require_real_cost=True,
            cost_saving_support=(-10.0, 10.0),
        )

        self.assertTrue(report["monetary_cost_available"])
        self.assertTrue(report["weighted_certificate_core_used"])
        evaluation = _read_json(output / EVALUATION_NAME)
        self.assertIsNotNone(evaluation["weighted_certificate"])
        self.assertEqual(
            report["mathematical_certificate_state"],
            evaluation["weighted_certificate"]["certificate_state"],
        )
        self.assertEqual(
            "POSTHOC_SIMULATOR_ANALYSIS_ONLY",
            report["decision_state"],
        )
        self.assertFalse(report["commit_authorized"])
        self.assertFalse(report["eligible_for_scientific_claims"])

    def test_external_cost_row_must_match_its_bound_manifest(self) -> None:
        source = _write_source(self.root / "source", monetary=True)
        rows = _read_jsonl(source / OBSERVATIONS_NAME)
        rows[0]["monetary_cost"]["amount"] += 1.0
        observation_bytes = b"".join(_json_document(row) for row in rows)
        (source / OBSERVATIONS_NAME).write_bytes(observation_bytes)
        manifest = _read_json(source / OBSERVATION_MANIFEST_NAME)
        manifest["observations_file_sha256"] = _sha256(observation_bytes)
        manifest.pop("observation_manifest_sha256")
        manifest["observation_manifest_sha256"] = _sha256(_canonical(manifest))
        (source / OBSERVATION_MANIFEST_NAME).write_bytes(_json_document(manifest))
        names = [
            EXTERNAL_COST_NAME,
            OBSERVATION_MANIFEST_NAME,
            OBSERVATIONS_NAME,
        ]
        (source / CHECKSUMS_NAME).write_text(
            "".join(
                f"{_sha256((source / name).read_bytes())}  {name}\n"
                for name in sorted(names)
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            NeutralAwmOedConsumerError,
            "cost differs from external cost manifest",
        ):
            freeze_neutral_awm_oed_analysis(
                observation_dir=source,
                analysis_id="cost-mismatch-v1",
                output_dir=self.root / "analysis",
                cost_saving_support=(-10.0, 10.0),
            )

    def test_missing_cell_is_rejected_even_when_package_is_restamped(self) -> None:
        source = _write_source(
            self.root / "source",
            omit_cell=("W4", "D7", 1),
        )
        with self.assertRaisesRegex(
            NeutralAwmOedConsumerError,
            "complete 4x8x2 matrix",
        ):
            freeze_neutral_awm_oed_analysis(
                observation_dir=source,
                analysis_id="missing-cell-v1",
                output_dir=self.root / "analysis",
            )

    def test_source_and_output_tampering_are_rejected(self) -> None:
        source = _write_source(self.root / "source")
        with (source / OBSERVATIONS_NAME).open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaisesRegex(
            NeutralAwmOedConsumerError,
            "source checksum mismatch",
        ):
            freeze_neutral_awm_oed_analysis(
                observation_dir=source,
                analysis_id="source-tamper-v1",
                output_dir=self.root / "analysis-a",
            )

        clean = _write_source(self.root / "clean")
        output = self.root / "analysis-b"
        freeze_neutral_awm_oed_analysis(
            observation_dir=clean,
            analysis_id="output-tamper-v1",
            output_dir=output,
        )
        evaluation = _read_json(output / EVALUATION_NAME)
        evaluation["commit_authorized"] = True
        (output / EVALUATION_NAME).write_bytes(_json_document(evaluation))
        with self.assertRaisesRegex(
            NeutralAwmOedConsumerError,
            "output checksum mismatch",
        ):
            verify_neutral_awm_oed_analysis(
                observation_dir=clean,
                analysis_dir=output,
            )

    def test_hidden_or_credential_fields_are_rejected_after_restamping(self) -> None:
        cases = (
            ("api_key", "credential-like-value"),
            ("hidden_labels", ["A"]),
            ("relevance_values", [1.0]),
        )
        for index, (key, value) in enumerate(cases):
            with self.subTest(key=key):
                source = _write_source(
                    self.root / f"source-{index}",
                    mutation=lambda rows, k=key, v=value: rows[0][
                        "latency_measurements_ms"
                    ].update({k: v}),
                )
                with self.assertRaisesRegex(
                    NeutralAwmOedConsumerError,
                    "hidden/credential boundary|must be a finite number",
                ):
                    freeze_neutral_awm_oed_analysis(
                        observation_dir=source,
                        analysis_id=f"private-{index}",
                        output_dir=self.root / f"analysis-{index}",
                    )

    def test_n1_authentication_flag_is_required(self) -> None:
        source = _write_source(
            self.root / "source",
            mutation=lambda rows: rows[0].update({
                "score_authenticity_verified": False,
            }),
        )
        with self.assertRaisesRegex(
            NeutralAwmOedConsumerError,
            "authenticated N1 scoring",
        ):
            freeze_neutral_awm_oed_analysis(
                observation_dir=source,
                analysis_id="unauthenticated-v1",
                output_dir=self.root / "analysis",
            )

    def test_source_package_is_read_only(self) -> None:
        source = _write_source(self.root / "source")
        before = {
            path.name: path.read_bytes()
            for path in source.iterdir()
        }
        freeze_neutral_awm_oed_analysis(
            observation_dir=source,
            analysis_id="read-only-v1",
            output_dir=self.root / "analysis",
        )
        after = {
            path.name: path.read_bytes()
            for path in source.iterdir()
        }
        self.assertEqual(before, after)


class NeutralAwmOedConsumerCliTest(unittest.TestCase):
    def _invoke(self, arguments: list[str]) -> tuple[int, dict]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        lines = stdout.getvalue().splitlines()
        self.assertEqual(1, len(lines), stdout.getvalue())
        return status, json.loads(lines[0])

    def test_public_simulator_exports_are_available(self) -> None:
        self.assertIs(
            freeze_neutral_awm_oed_analysis,
            simulator_api.freeze_neutral_awm_oed_analysis,
        )
        self.assertIs(
            verify_neutral_awm_oed_analysis,
            simulator_api.verify_neutral_awm_oed_analysis,
        )
        self.assertIs(
            NeutralAwmOedConsumerError,
            simulator_api.NeutralAwmOedConsumerError,
        )

    def test_freeze_command_wires_all_explicit_parameters(self) -> None:
        with mock.patch(
            "pathfinder.simulator.neutral_awm_oed_consumer."
            "freeze_neutral_awm_oed_analysis",
            return_value={"status": "VERIFIED"},
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-neutral-awm-oed-analysis",
                "--observation-dir",
                "observations",
                "--analysis-id",
                "analysis-v1",
                "--baseline-design-id",
                "D3",
                "--oed-selection-size",
                "2",
                "--require-real-cost",
                "--alpha",
                "0.1",
                "--delta-success-margin",
                "0.2",
                "--minimum-cost-saving",
                "-1.5",
                "--cost-saving-support",
                "-5",
                "10",
                "--output-dir",
                "analysis",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        freeze.assert_called_once_with(
            observation_dir=Path("observations"),
            analysis_id="analysis-v1",
            output_dir=Path("analysis"),
            baseline_design_id="D3",
            oed_selection_size=2,
            require_real_cost=True,
            alpha=0.1,
            delta_success_margin=0.2,
            minimum_cost_saving=-1.5,
            cost_saving_support=(-5.0, 10.0),
        )

    def test_freeze_command_has_fail_closed_defaults(self) -> None:
        with mock.patch(
            "pathfinder.simulator.neutral_awm_oed_consumer."
            "freeze_neutral_awm_oed_analysis",
            return_value={"status": "VERIFIED"},
        ) as freeze:
            status, _ = self._invoke([
                "freeze-simulator-neutral-awm-oed-analysis",
                "--observation-dir",
                "observations",
                "--analysis-id",
                "analysis-v1",
                "--output-dir",
                "analysis",
            ])
        self.assertEqual(0, status)
        freeze.assert_called_once_with(
            observation_dir=Path("observations"),
            analysis_id="analysis-v1",
            output_dir=Path("analysis"),
            baseline_design_id="D0",
            oed_selection_size=4,
            require_real_cost=False,
            alpha=0.05,
            delta_success_margin=0.0,
            minimum_cost_saving=0.0,
            cost_saving_support=None,
        )

    def test_verify_command_wires_bound_directories_only(self) -> None:
        with mock.patch(
            "pathfinder.simulator.neutral_awm_oed_consumer."
            "verify_neutral_awm_oed_analysis",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-neutral-awm-oed-analysis",
                "--observation-dir",
                "observations",
                "--analysis-dir",
                "analysis",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            observation_dir=Path("observations"),
            analysis_dir=Path("analysis"),
        )

    def test_parser_rejects_unsafe_numeric_arguments(self) -> None:
        base = [
            "freeze-simulator-neutral-awm-oed-analysis",
            "--observation-dir",
            "observations",
            "--analysis-id",
            "analysis-v1",
            "--output-dir",
            "analysis",
        ]
        cases = (
            ["--oed-selection-size", "0"],
            ["--oed-selection-size", "5"],
            ["--alpha", "0"],
            ["--alpha", "1"],
            ["--delta-success-margin", "-0.1"],
            ["--delta-success-margin", "1.1"],
            ["--minimum-cost-saving", "nan"],
            ["--cost-saving-support", "-inf", "10"],
        )
        for extra in cases:
            with self.subTest(extra=extra):
                with (
                    redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit),
                ):
                    _parser().parse_args([*base, *extra])

    def test_cli_rejects_reversed_cost_support_before_consumer_call(self) -> None:
        with mock.patch(
            "pathfinder.simulator.neutral_awm_oed_consumer."
            "freeze_neutral_awm_oed_analysis",
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-neutral-awm-oed-analysis",
                "--observation-dir",
                "observations",
                "--analysis-id",
                "analysis-v1",
                "--cost-saving-support",
                "10",
                "-10",
                "--output-dir",
                "analysis",
            ])
        self.assertEqual(2, status)
        self.assertEqual("error", payload["status"])
        self.assertIn("LOWER must be less than UPPER", payload["message"])
        freeze.assert_not_called()


if __name__ == "__main__":
    unittest.main()
