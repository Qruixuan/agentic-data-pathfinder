"""Focused, offline tests for list-priced replay accounting."""

from __future__ import annotations

import unittest
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from pathfinder.rsi_exam.offline_replay_costing import (
    allocate_embedding_units,
    build_object_price_table,
    price_replay_result,
    price_verified_smoke_n6_usage,
    reconcile_caption_cache,
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
