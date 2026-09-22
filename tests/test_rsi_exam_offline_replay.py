from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pathfinder.rsi_exam.offline_replay import (
    ACTIONS_NAME,
    CASES_NAME,
    CHECKSUMS_NAME,
    MANIFEST_NAME,
    OUTCOMES_NAME,
    OfflineReplayError,
    ReplayEvaluator,
    build_offline_replay_package,
    compare_offline_replay_baselines,
    load_offline_replay_package,
    run_offline_replay_policy,
    verify_offline_replay_package,
)


SOURCE_COMMIT = "5" * 40
OBJECT_ID = "nextqa-val-3429509208"


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _row(
    case_id: str,
    design_id: str,
    node: str,
    route_family: str,
    *,
    origin_bytes: int,
    cache_bytes: int,
    model_input_bytes: int,
    task_success: bool,
    cache_branch: str | None = None,
) -> dict:
    profiles = {
        "raw": "raw-direct-video-v1",
        "indexed-raw": "indexed-query-aware-temporal-selection-v1",
        "remote-derived": "derived-sparse-frames-4-v1",
        "local-cache-derived": "derived-sparse-frames-4-v1",
    }
    return {
        "cache_branch": cache_branch,
        "case_id": case_id,
        "design_id": design_id,
        "executor_node_id": node,
        "index_query_bytes_read": 1499 if route_family == "indexed-raw" else 0,
        "index_query_bytes_sent": 431 if route_family == "indexed-raw" else 0,
        "index_query_ms": 50.0 if route_family == "indexed-raw" else 0.0,
        "model_input_bytes_sent_to_n6": model_input_bytes,
        "n1_exactly_once_authenticated": True,
        "n1_score_ms": 10.0,
        "n6_infer_ms": 100.0,
        "origin_bytes_read": origin_bytes,
        "cache_bytes_read": cache_bytes,
        "prepare_model_input_ms": 5.0,
        "route_family": route_family,
        "route_wall_ms_excluding_inference": 20.0,
        "semantic_input_profile_id": profiles[route_family],
        "source_read_ms": 2.0,
        "status": "COMPLETE",
        "task_success": task_success,
        "trial_key": (
            "flowmesh-infra-4x8-local-smoke-v1|smoke-temporal|"
            f"{design_id}|r0000-{case_id}"
        ),
    }


def _accounting() -> dict:
    rows = [
        _row(
            "n7-raw", "D0", "N7", "raw",
            origin_bytes=1_626_982,
            cache_bytes=0,
            model_input_bytes=2_170_148,
            task_success=True,
        ),
        _row(
            "n7-indexed-raw", "D1", "N7", "indexed-raw",
            origin_bytes=389_120,
            cache_bytes=0,
            model_input_bytes=493_260,
            task_success=True,
        ),
        _row(
            "n7-remote-derived", "D2", "N7", "remote-derived",
            origin_bytes=727_040,
            cache_bytes=0,
            model_input_bytes=251_587,
            task_success=False,
        ),
        _row(
            "n7-cache-miss", "D3", "N7", "local-cache-derived",
            origin_bytes=727_040,
            cache_bytes=0,
            model_input_bytes=251_587,
            task_success=True,
            cache_branch="miss",
        ),
        _row(
            "n7-cache-hit", "D3", "N7", "local-cache-derived",
            origin_bytes=0,
            cache_bytes=727_040,
            model_input_bytes=251_587,
            task_success=True,
            cache_branch="hit",
        ),
        _row(
            "n8-raw", "D4", "N8", "raw",
            origin_bytes=1_626_982,
            cache_bytes=0,
            model_input_bytes=2_170_148,
            task_success=True,
        ),
        _row(
            "n8-indexed-raw", "D5", "N8", "indexed-raw",
            origin_bytes=389_120,
            cache_bytes=0,
            model_input_bytes=493_260,
            task_success=True,
        ),
        _row(
            "n8-remote-derived", "D6", "N8", "remote-derived",
            origin_bytes=727_040,
            cache_bytes=0,
            model_input_bytes=251_587,
            task_success=True,
        ),
        _row(
            "n8-cache-miss", "D7", "N8", "local-cache-derived",
            origin_bytes=727_040,
            cache_bytes=0,
            model_input_bytes=251_587,
            task_success=True,
            cache_branch="miss",
        ),
        _row(
            "n8-cache-hit", "D7", "N8", "local-cache-derived",
            origin_bytes=0,
            cache_bytes=727_040,
            model_input_bytes=251_587,
            task_success=True,
            cache_branch="hit",
        ),
    ]
    return {
        "schema_version": "pathfinder.temporal-index-v2-accounting/v1",
        "run_id": "offline-replay-source-run-v1",
        "object_id": OBJECT_ID,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "one_time_build": {
            "this_object_source_bytes_read": 1_626_982,
            "this_object_projection_bytes": 389_120,
            "read_mode": "complete-object-read-then-source-side-decode",
            "partial_mp4_byte_range_claimed": False,
            "reduced_source_storage_io_claimed": False,
        },
        "rows": rows,
    }


def _write_accounting(
    root: Path,
    value: dict | None = None,
    *,
    name: str = "accounting",
) -> Path:
    source = root / name
    source.mkdir()
    payload = (
        json.dumps(value or _accounting(), sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    (source / "accounting.json").write_bytes(payload)
    # The source artifact historically used one separator space. The replay
    # importer accepts and verifies it but writes canonical two-space rows.
    (source / CHECKSUMS_NAME).write_bytes(
        f"{_sha(payload)} accounting.json\n".encode("utf-8")
    )
    return source


def _build(root: Path) -> tuple[Path, Path]:
    source = _write_accounting(root)
    package = root / "replay"
    build_offline_replay_package(
        [source],
        output_dir=package,
        source_commit=SOURCE_COMMIT,
        builder_commit=SOURCE_COMMIT,
        package_id="offline-replay-test-v1",
    )
    return source, package


class OfflineReplayPackageTest(unittest.TestCase):
    def test_build_verify_and_load_are_canonical_and_leak_free(self) -> None:
        with TemporaryDirectory() as name:
            source, package = _build(Path(name))
            receipt = verify_offline_replay_package(
                package,
                source_accounting_dirs=[source],
            )
            self.assertEqual("VERIFIED_OFFLINE_REPLAY", receipt["status"])
            self.assertEqual(1, receipt["case_count"])
            self.assertEqual(8, receipt["action_count"])
            self.assertEqual(10, receipt["outcome_count"])
            self.assertTrue(receipt["source_binding_checked"])
            for path in package.iterdir():
                self.assertNotIn(b"\r", path.read_bytes(), path.name)
            loaded = load_offline_replay_package(package)
            observation = ReplayEvaluator(
                loaded,
                mode="shared-dataset-sequence",
            ).observation(OBJECT_ID, remaining_queries=1).to_dict()
            rendered = json.dumps(observation, sort_keys=True)
            self.assertNotIn("task_success", rendered)
            self.assertNotIn("answer", rendered)
            self.assertNotIn("outcome", rendered)

    def test_package_is_immutable_and_tampering_fails(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            source, package = _build(root)
            with self.assertRaisesRegex(
                OfflineReplayError,
                "already exists",
            ):
                build_offline_replay_package(
                    [source],
                    output_dir=package,
                    source_commit=SOURCE_COMMIT,
                    builder_commit=SOURCE_COMMIT,
                    package_id="offline-replay-test-v1",
                )
            (package / CASES_NAME).write_bytes(
                (package / CASES_NAME).read_bytes() + b" \n"
            )
            with self.assertRaisesRegex(OfflineReplayError, "mismatch"):
                verify_offline_replay_package(package)

    def test_source_with_secret_bearing_field_is_rejected(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            value = _accounting()
            value["api_key"] = "must-not-enter-a-replay"
            source = _write_accounting(root, value)
            with self.assertRaisesRegex(
                OfflineReplayError,
                "forbidden field",
            ):
                build_offline_replay_package(
                    [source],
                    output_dir=root / "replay",
                    source_commit=SOURCE_COMMIT,
                    builder_commit=SOURCE_COMMIT,
                    package_id="offline-replay-test-v1",
                )

    def test_unmeasured_index_build_latency_remains_null(self) -> None:
        with TemporaryDirectory() as name:
            _, package = _build(Path(name))
            loaded = load_offline_replay_package(package)
            case = loaded["cases"][0]
            self.assertIsNone(case["index_build"]["latency_ms"])
            result = run_offline_replay_policy(
                package,
                policy_name="always-indexed",
                mode="shared-dataset-sequence",
                query_count=1,
            )
            self.assertIsNone(
                result["steps"][0]["metrics"]["index_build_latency_ms"]
            )

    def test_repeated_measurements_form_one_seeded_empirical_cell(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            first = _accounting()
            second = _accounting()
            second["run_id"] = "offline-replay-source-run-v2"
            # A repeated model observation may differ without changing the
            # public case or action schema.
            second["rows"][2]["task_success"] = True
            sources = [
                _write_accounting(root, first, name="accounting-a"),
                _write_accounting(root, second, name="accounting-b"),
            ]
            package = root / "replay"
            build_offline_replay_package(
                sources,
                output_dir=package,
                source_commit=SOURCE_COMMIT,
                builder_commit=SOURCE_COMMIT,
                package_id="offline-replay-repeated-v1",
            )
            verified = verify_offline_replay_package(
                package,
                source_accounting_dirs=sources,
            )
            self.assertEqual(1, verified["case_count"])
            self.assertEqual(20, verified["outcome_count"])
            evaluator = ReplayEvaluator(
                load_offline_replay_package(package),
                mode="independent-query",
                seed=19,
            )
            first_draw = evaluator.step(
                OBJECT_ID,
                "D2",
                remaining_queries=1,
            )
            evaluator.reset_case(OBJECT_ID)
            second_draw = evaluator.step(
                OBJECT_ID,
                "D2",
                remaining_queries=1,
            )
            self.assertIn(first_draw["task_success"], {True, False})
            self.assertIn(second_draw["task_success"], {True, False})

    def test_multiple_objects_bind_video_disjoint_splits(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            first = _accounting()
            second = _accounting()
            second_object = "nextqa-val-second-video"
            second["object_id"] = second_object
            second["run_id"] = "offline-replay-second-object-run-v1"
            sources = [
                _write_accounting(root, first, name="accounting-a"),
                _write_accounting(root, second, name="accounting-b"),
            ]
            split_manifest = root / "splits.json"
            split_manifest.write_bytes(
                (
                    json.dumps(
                        {OBJECT_ID: "train", second_object: "test"},
                        sort_keys=True,
                        indent=2,
                    )
                    + "\n"
                ).encode("utf-8")
            )
            package = root / "replay"
            build_offline_replay_package(
                sources,
                output_dir=package,
                source_commit=SOURCE_COMMIT,
                builder_commit=SOURCE_COMMIT,
                package_id="offline-replay-multicase-v1",
                split_manifest=split_manifest,
            )
            verified = verify_offline_replay_package(
                package,
                source_accounting_dirs=sources,
            )
            self.assertEqual(2, verified["case_count"])
            loaded = load_offline_replay_package(package)
            splits = {
                row["object_id"]: row["split"] for row in loaded["cases"]
            }
            self.assertEqual(
                {OBJECT_ID: "train", second_object: "test"},
                splits,
            )


class OfflineReplayStateTest(unittest.TestCase):
    def test_cache_miss_transitions_to_exact_cache_hit(self) -> None:
        with TemporaryDirectory() as name:
            _, package = _build(Path(name))
            evaluator = ReplayEvaluator(
                load_offline_replay_package(package),
                mode="shared-dataset-sequence",
            )
            miss = evaluator.step(OBJECT_ID, "D3", remaining_queries=2)
            hit = evaluator.step(OBJECT_ID, "D3", remaining_queries=1)
            self.assertEqual("cache-miss", miss["state_variant"])
            self.assertEqual(727_040, miss["metrics"]["query_origin_bytes"])
            self.assertEqual("cache-hit", hit["state_variant"])
            self.assertEqual(0, hit["metrics"]["query_origin_bytes"])
            self.assertEqual(727_040, hit["metrics"]["cache_bytes"])

    def test_missing_action_fails_closed_without_state_change(self) -> None:
        with TemporaryDirectory() as name:
            _, package = _build(Path(name))
            evaluator = ReplayEvaluator(
                load_offline_replay_package(package),
                mode="shared-dataset-sequence",
            )
            result = evaluator.step(
                OBJECT_ID,
                "D-does-not-exist",
                remaining_queries=1,
            )
            self.assertEqual("unsupported_action", result["status"])
            self.assertFalse(result["state_changed"])
            observation = evaluator.observation(
                OBJECT_ID,
                remaining_queries=1,
            )
            self.assertFalse(observation.index_available)
            self.assertFalse(any(observation.cache_warm_by_node.values()))

    def test_shared_index_amortizes_and_independent_mode_does_not(self) -> None:
        with TemporaryDirectory() as name:
            _, package = _build(Path(name))
            expected_shared = {
                1: 2_016_102,
                10: 5_518_182,
                100: 40_538_982,
            }
            for query_count, expected in expected_shared.items():
                result = run_offline_replay_policy(
                    package,
                    policy_name="always-indexed",
                    mode="shared-dataset-sequence",
                    query_count=query_count,
                )
                self.assertEqual(
                    expected,
                    result["metrics"]["total_source_bytes"],
                )
                self.assertEqual(1, result["metrics"]["index_builds"])
            independent = run_offline_replay_policy(
                package,
                policy_name="always-indexed",
                mode="independent-query",
                query_count=10,
            )
            self.assertEqual(
                10 * 2_016_102,
                independent["metrics"]["total_source_bytes"],
            )
            self.assertEqual(10, independent["metrics"]["index_builds"])

    def test_seeded_replay_is_deterministic(self) -> None:
        with TemporaryDirectory() as name:
            _, package = _build(Path(name))
            first = run_offline_replay_policy(
                package,
                policy_name="random-seeded",
                mode="shared-dataset-sequence",
                query_count=10,
                seed=71,
            )
            second = run_offline_replay_policy(
                package,
                policy_name="random-seeded",
                mode="shared-dataset-sequence",
                query_count=10,
                seed=71,
            )
            self.assertEqual(first, second)

    def test_baselines_are_ranked_by_quality_then_bytes(self) -> None:
        with TemporaryDirectory() as name:
            _, package = _build(Path(name))
            comparison = compare_offline_replay_baselines(
                package,
                mode="shared-dataset-sequence",
                query_count=10,
                seed=3,
            )
            self.assertEqual("COMPLETE", comparison["status"])
            policies = {
                row["policy_name"]: row for row in comparison["policies"]
            }
            self.assertFalse(
                policies["always-derived"]["objective"]["feasible"]
            )
            self.assertTrue(
                policies["always-indexed"]["objective"]["feasible"]
            )
            self.assertLess(
                comparison["ranking"].index("always-indexed"),
                comparison["ranking"].index("always-derived"),
            )


if __name__ == "__main__":
    unittest.main()
