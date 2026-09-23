"""Focused, offline tests for list-priced replay accounting."""

from __future__ import annotations

import unittest
import hashlib
import io
import json
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zipfile import ZipFile

from pathfinder.rsi_exam.offline_replay_costing import (
    allocate_embedding_units,
    build_object_price_table,
    main,
    price_replay_result,
    price_verified_smoke_n6_usage,
    reconcile_caption_cache,
    verify_smoke_provider_request_log,
    _workbook_usage_rows,
    _package_self_digest,
)


def _objects() -> list[dict[str, object]]:
    return [
        {
            "object_id": "nextqa-val-1", "source_video_sha256": "a" * 64,
            "caption_window_count": 2, "caption_request_count": 2,
            "caption_input_units": 1000, "caption_output_units": 100,
        },
        {
            "object_id": "nextqa-val-2", "source_video_sha256": "b" * 64,
            "caption_window_count": 2, "caption_request_count": 2,
            "caption_input_units": 2000, "caption_output_units": 200,
        },
    ]


def _batches() -> list[dict[str, int]]:
    return [
        {"ordinal": 0, "input_count": 3, "input_units": 7},
        {"ordinal": 1, "input_count": 3, "input_units": 7},
    ]


class OfflineReplayCostingTests(unittest.TestCase):
    def test_provider_request_id_joins_exact_smoke_and_usage(self) -> None:
        smoke = []
        usage = []
        attempts = []
        provider_ids = []
        for index in range(10):
            result_digest = f"{index + 1:064x}"
            request_digest = f"{index + 21:064x}"
            provider_id = f"00000000-0000-0000-0000-{index + 1:012x}"
            provider_ids.append(provider_id)
            smoke.append({
                "case_id": f"case-{index}",
                "trial_key": f"trial-{index}",
                "result": {
                    "status": "COMPLETE",
                    "route_evidence_sha256": f"{index + 41:064x}",
                    "semantic_route_evidence": {
                        "evidence_sha256": f"{index + 41:064x}",
                        "design_id": "D0",
                        "semantic": {
                            "model": "qwen3.8-27b",
                            "result_sha256": result_digest,
                            "request_sha256": request_digest,
                        },
                    },
                },
            })
            usage.append({
                "result_sha256": result_digest,
                "request_sha256": request_digest,
                "input_units": 100,
                "cached_input_units": 20,
                "output_units": 10,
                "total_units": 110,
            })
            attempts.append({
                "result_sha256": result_digest,
                "request_sha256": request_digest,
                "attempt_index": 0,
                "outcome": "completed",
                "http_status": 200,
                "header_request_id_sha256": hashlib.sha256(
                    provider_id.encode("ascii")
                ).hexdigest(),
            })

        def workbook_xml(*, duplicate: bool = False,
                         wrong_usage: bool = False) -> str:
            rows = []
            for index, provider_id in enumerate(provider_ids):
                if duplicate and index == 1:
                    provider_id = provider_ids[0]
                input_units = 101 if wrong_usage and index == 0 else 100
                provider_usage = json.dumps({
                    "input_tokens": input_units,
                    "output_tokens": 10,
                    "prompt_tokens_details": {"cached_tokens": 20},
                })
                row_number = index + 2
                rows.append(
                    f'<row r="{row_number}">'
                    f'<c r="A{row_number}" t="inlineStr"><is><t>'
                    f'{provider_id}</t></is></c>'
                    f'<c r="C{row_number}" t="inlineStr"><is><t>'
                    'qwen3.8-27b</t></is></c>'
                    f'<c r="D{row_number}" t="inlineStr"><is><t>'
                    f'{provider_usage}</t></is></c>'
                    f'<c r="G{row_number}"><v>200</v></c>'
                    '</row>'
                )
            return (
                '<worksheet xmlns="http://schemas.openxmlformats.org/'
                'spreadsheetml/2006/main"><sheetData>'
                + ''.join(rows) + '</sheetData></worksheet>'
            )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "provider.xlsx"
            with ZipFile(path, "w") as archive:
                archive.writestr(
                    "xl/worksheets/sheet1.xml", workbook_xml()
                )
            matched = verify_smoke_provider_request_log(
                smoke, usage, attempts, path,
            )
            self.assertEqual(matched["provider_request_id_match_count"], 10)
            self.assertEqual(matched["n6_list_price_usd"], "0.000720000")
            self.assertNotIn(provider_ids[0], json.dumps(matched))
            self.assertFalse(matched["raw_request_ids_recorded"])

            smoke_path = Path(directory) / "smoke.jsonl"
            smoke_path.write_text(
                "".join(json.dumps(row) + "\n" for row in smoke),
                encoding="utf-8",
            )
            snapshot_path = Path(directory) / "snapshot.json"
            snapshot_path.write_text(
                json.dumps({"usage_rows": usage, "attempt_rows": attempts}),
                encoding="utf-8",
            )
            output = io.StringIO()
            with patch("sys.argv", [
                "offline_replay_costing",
                "--smoke-results-jsonl", str(smoke_path),
                "--n6-trace-snapshot", str(snapshot_path),
                "--provider-log-xlsx", str(path),
            ]), redirect_stdout(output):
                main()
            self.assertEqual(
                json.loads(output.getvalue())[
                    "provider_request_id_match_count"
                ], 10,
            )

            with ZipFile(path, "w") as archive:
                archive.writestr(
                    "xl/worksheets/sheet1.xml",
                    workbook_xml(wrong_usage=True),
                )
            with self.assertRaisesRegex(ValueError, "usage differs"):
                verify_smoke_provider_request_log(
                    smoke, usage, attempts, path,
                )

            with ZipFile(path, "w") as archive:
                archive.writestr(
                    "xl/worksheets/sheet1.xml",
                    workbook_xml(duplicate=True),
                )
            with self.assertRaisesRegex(ValueError, "duplicate"):
                verify_smoke_provider_request_log(
                    smoke, usage, attempts, path,
                )

            attempts[0]["header_request_id_sha256"] = "f" * 64
            with ZipFile(path, "w") as archive:
                archive.writestr(
                    "xl/worksheets/sheet1.xml", workbook_xml()
                )
            with self.assertRaisesRegex(ValueError, "not in log"):
                verify_smoke_provider_request_log(
                    smoke, usage, attempts, path,
                )

    def test_smoke_usage_requires_exact_result_and_request_bindings(self) -> None:
        smoke = []
        journal = []
        for index in range(10):
            digest = f"{index + 1:064x}"
            request = f"{index + 20:064x}"
            smoke.append({
                "case_id": f"case-{index}",
                "trial_key": f"trial-{index}",
                "result": {
                    "status": "COMPLETE",
                    "route_evidence_sha256": f"{index + 40:064x}",
                    "semantic_route_evidence": {
                        "evidence_sha256": f"{index + 40:064x}",
                        "design_id": f"D{index}",
                        "semantic": {
                            "model": "qwen3.8-27b",
                            "result_sha256": digest,
                            "request_sha256": request,
                        },
                    },
                },
            })
            journal.append({
                "result_sha256": digest,
                "request_sha256": request,
                "input_units": 100, "cached_input_units": 20,
                "output_units": 10, "total_units": 110,
                "private_answer": "must-not-be-copied",
            })
        priced = price_verified_smoke_n6_usage(smoke, journal)
        self.assertEqual(priced["smoke_count"], 10)
        self.assertEqual(priced["input_units"], 1000)
        self.assertEqual(priced["n6_list_price_usd"], "0.000720000")
        self.assertNotIn("must-not-be-copied", json.dumps(priced))
        journal[0]["request_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "different semantic request"):
            price_verified_smoke_n6_usage(smoke, journal)
        journal[0]["request_sha256"] = f"{20:064x}"
        journal.pop()
        with self.assertRaisesRegex(ValueError, "missing for a route result"):
            price_verified_smoke_n6_usage(smoke, journal)

    def test_embedding_allocation_conserves_measured_batch_units(self) -> None:
        self.assertEqual(allocate_embedding_units(_objects(), _batches()), {
            "nextqa-val-1": 7, "nextqa-val-2": 7,
        })

    def test_rejects_unbound_embedding_input_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "do not match"):
            allocate_embedding_units(_objects(), _batches()[:1])

    def test_build_price_table_distinguishes_measurement_from_allocation(self) -> None:
        table = build_object_price_table(_objects(), _batches())
        first = table["objects"][0]
        self.assertEqual(first["caption_input_units_measured"], 1000)
        self.assertEqual(first["embedding_input_units_allocated"], 7)
        self.assertFalse(table["embedding_allocation_is_measured_per_object"])
        self.assertEqual(first["caption_list_price_usd"], "0.000800000")
        self.assertEqual(table["cohort"]["embedding_input_units"], 14)
        self.assertIsNone(table["cohort"]["complete_path_cost_usd"])

    def test_cached_input_uses_its_own_rate(self) -> None:
        table = build_object_price_table(
            _objects(), _batches(),
            cached_caption_units={"nextqa-val-1": 100},
        )
        self.assertEqual(
            table["objects"][0]["caption_list_price_usd"], "0.000760000"
        )
        self.assertEqual(table["caption_cache_basis"],
                         "caller-supplied-object-cache-unverified")

    def test_provider_log_reads_only_numeric_usage_fields(self) -> None:
        xml = (
            '<worksheet xmlns="http://schemas.openxmlformats.org/'
            'spreadsheetml/2006/main"><sheetData><row r="2">'
            '<c r="A2" t="inlineStr"><is><t>private-id</t></is></c>'
            '<c r="C2" t="inlineStr"><is><t>qwen3.8-27b</t></is></c>'
            '<c r="D2" t="inlineStr"><is><t>'
            '{"input_tokens":100,"output_tokens":5,'
            '"prompt_tokens_details":{"cached_tokens":20}}'
            '</t></is></c><c r="G2"><v>200</v></c>'
            '</row></sheetData></worksheet>'
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "usage.xlsx"
            with ZipFile(path, "w") as archive:
                archive.writestr("xl/worksheets/sheet1.xml", xml)
            rows = _workbook_usage_rows(path)
        self.assertEqual(rows, [{
            "input_units": 100, "output_units": 5,
            "cached_input_units": 20,
        }])
        self.assertNotIn("private-id", str(rows))

    def test_caption_matching_refuses_ambiguous_token_pairs(self) -> None:
        captions = [{
            "object_id": "nextqa-val-1", "input_units": 100,
            "output_units": 5,
        }]
        provider = [{
            "input_units": 100, "output_units": 5,
            "cached_input_units": 20,
        }]
        self.assertEqual(reconcile_caption_cache(captions, provider),
                         {"nextqa-val-1": 20})
        with self.assertRaisesRegex(ValueError, "not unique"):
            reconcile_caption_cache(captions, provider + provider)

    def test_price_source_manifest_must_recompute_its_digest(self) -> None:
        contents = {"model_id": "qwen3.8-27b"}
        digest = hashlib.sha256(json.dumps(
            contents, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        self.assertEqual(_package_self_digest({
            **contents, "package_sha256": digest,
        }), digest)
        with self.assertRaisesRegex(ValueError, "self digest differs"):
            _package_self_digest({
                "model_id": "different", "package_sha256": digest,
            })

    def test_missing_n6_usage_never_becomes_zero_or_complete(self) -> None:
        table = build_object_price_table(_objects(), _batches())
        replay = {
            "mode": "shared-dataset-sequence", "query_count": 2,
            "steps": [
                {"status": "replayed", "case_id": "case-1",
                 "action_id": "indexed", "outcome_id": "outcome-1",
                 "newly_built_components": ["captions", "index_embedding"]},
                {"status": "replayed", "case_id": "case-1",
                 "action_id": "indexed", "outcome_id": "outcome-2",
                 "newly_built_components": []},
            ],
        }
        priced = price_replay_result(
            replay, table, object_id_by_case={"case-1": "nextqa-val-1"}
        )
        self.assertFalse(priced["provider_cost_complete"])
        self.assertIsNone(priced["provider_list_price_usd"])
        self.assertIsNone(priced["steps"][0]["n6_inference_provider_usd"])
        self.assertEqual(priced["steps"][1]["known_cold_build_provider_usd"],
                         "0.000000000")

    def test_bound_n6_usage_prices_cold_and_reuse_separately(self) -> None:
        table = build_object_price_table(_objects(), _batches())
        replay = {
            "mode": "shared-dataset-sequence", "query_count": 2,
            "steps": [
                {"status": "replayed", "case_id": "case-1",
                 "action_id": "indexed", "outcome_id": "outcome-1",
                 "newly_built_components": ["captions", "index_embedding"]},
                {"status": "replayed", "case_id": "case-1",
                 "action_id": "indexed", "outcome_id": "outcome-2",
                 "newly_built_components": []},
            ],
        }
        usage = {
            "outcome-1": {"input_units": 100, "cached_input_units": 0,
                          "output_units": 10},
            "outcome-2": {"input_units": 100, "cached_input_units": 20,
                          "output_units": 10},
        }
        priced = price_replay_result(
            replay, table, object_id_by_case={"case-1": "nextqa-val-1"},
            n6_usage_by_outcome=usage,
        )
        self.assertTrue(priced["provider_cost_complete"])
        self.assertEqual(priced["steps"][0]["n6_inference_provider_usd"],
                         "0.000080000")
        self.assertEqual(priced["steps"][1]["n6_inference_provider_usd"],
                         "0.000072000")
        self.assertEqual(priced["steps"][1]["complete_provider_usd"],
                         "0.000072000")
        self.assertGreater(
            Decimal(priced["steps"][0]["complete_provider_usd"]),
            Decimal(priced["steps"][1]["complete_provider_usd"]),
        )

    def test_independent_queries_charge_each_cold_build(self) -> None:
        table = build_object_price_table(_objects(), _batches())
        replay = {
            "mode": "independent-query", "query_count": 2,
            "steps": [
                {"status": "replayed", "case_id": "case-1",
                 "action_id": "derived", "outcome_id": f"outcome-{index}",
                 "newly_built_components": ["captions"]}
                for index in (1, 2)
            ],
        }
        priced = price_replay_result(
            replay, table, object_id_by_case={"case-1": "nextqa-val-1"}
        )
        self.assertEqual(
            [row["known_cold_build_provider_usd"] for row in priced["steps"]],
            ["0.000800000", "0.000800000"],
        )

    def test_replay_embedded_n6_usage_is_preferred_and_conflicts_fail(self) -> None:
        table = build_object_price_table(_objects(), _batches())
        usage = {
            "input_units": 100, "cached_input_units": 20,
            "output_units": 10, "total_units": 110,
        }
        replay = {
            "mode": "shared-dataset-sequence", "query_count": 1,
            "steps": [{
                "status": "replayed", "case_id": "case-1",
                "action_id": "raw", "outcome_id": "outcome-1",
                "newly_built_components": [],
                "metrics": {"n6_provider_usage": usage},
            }],
        }
        priced = price_replay_result(
            replay, table, object_id_by_case={"case-1": "nextqa-val-1"}
        )
        self.assertEqual(priced["n6_usage_binding"], ["replay-outcome"])
        self.assertEqual(priced["provider_list_price_usd"], "0.000072000")
        with self.assertRaisesRegex(ValueError, "differs"):
            price_replay_result(
                replay, table,
                object_id_by_case={"case-1": "nextqa-val-1"},
                n6_usage_by_outcome={"outcome-1": {
                    **usage, "input_units": 101,
                }},
            )


if __name__ == "__main__":
    unittest.main()
