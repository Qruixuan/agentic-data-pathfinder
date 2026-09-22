from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pathfinder.rsi_exam.offline_replay import (
    ACTIONS_NAME,
    CASES_NAME,
    CHECKSUMS_NAME,
    MANIFEST_NAME,
    OUTCOMES_NAME,
    README_NAME,
    OfflineReplayError,
    _checksum_bytes,
    _json_bytes,
    _jsonl_bytes,
    load_offline_replay_package,
)
from pathfinder.rsi_exam.offline_replay_v2 import (
    PROFILE_COMPONENTS,
    PROFILE_REPRESENTATION,
    V2_SCHEMA_VERSION,
    MaterializationReplayEvaluator,
    _component,
    build_offline_replay_v2,
    run_offline_replay_v2,
    verify_offline_replay_v2,
)


V1_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "data" / "fixtures" / "rsi_exam_offline_replay"
    / "pathfinder-one-case-v1"
)


def _costs(source_bytes: int, projection_bytes: int) -> dict:
    return {
        "sampling": _component(
            source_bytes, 180_000,
            source_basis="modeled-complete-object-size-proxy",
        ),
        "frame_bundle": _component(
            0, 250_000, source_basis="shared-sampling-output",
        ),
        "captions": _component(
            0, None, source_basis="shared-sampling-output",
            usage_unknown=True,
        ),
        "digest": _component(
            0, 1_000, source_basis="shared-caption-output",
        ),
        "index_embedding": _component(
            0, None, source_basis="shared-caption-output",
            usage_unknown=True,
        ),
        "index_projection": _component(
            source_bytes, projection_bytes,
            source_basis="frozen-index-build-accounting",
        ),
    }


def _fixture_package(root: Path, *, digest_action: bool = False) -> Path:
    v1 = load_offline_replay_package(V1_FIXTURE)
    cases = json.loads(json.dumps(v1["cases"]))
    actions = json.loads(json.dumps(v1["actions"]))
    manifest = json.loads(json.dumps(v1["manifest"]))
    case = cases[0]
    case["initial_state"]["built_components"] = []
    case["materialization_source_video_sha256"] = "a" * 64
    case["materialization_components"] = _costs(
        case["index_build"]["source_bytes"],
        case["index_build"]["output_bytes"],
    )
    for action in actions:
        if digest_action and action["action_id"] == "D2":
            action["semantic_input_profile_id"] = "derived-digest-only-v1"
        if digest_action and action["action_id"] == "D6":
            action["semantic_input_profile_id"] = "derived-sparse-fusion-4-v1"
        profile = action["semantic_input_profile_id"]
        if profile in PROFILE_REPRESENTATION:
            action["representation_id"] = PROFILE_REPRESENTATION[profile]
        action["required_build_components"] = list(PROFILE_COMPONENTS[
            action["semantic_input_profile_id"]
        ])
    manifest.update({
        "schema_version": V2_SCHEMA_VERSION,
        "package_id": "materialization-test-v2",
        "source_v1_package_id": v1["manifest"]["package_id"],
        "source_v1_package_sha256": v1["package_sha256"],
        "materialization_cost_model": {
            "monetary_cost_measured": False,
        },
        "objective": {
            "kind": "quality-then-logical-byte-proxy-v2",
            "monetary_cost_ranked": False,
        },
    })
    documents = {
        MANIFEST_NAME: _json_bytes(manifest),
        CASES_NAME: _jsonl_bytes(cases),
        ACTIONS_NAME: _jsonl_bytes(actions),
        OUTCOMES_NAME: _jsonl_bytes(v1["outcomes"]),
        README_NAME: b"Materialization-aware fixture.\n",
    }
    documents[CHECKSUMS_NAME] = _checksum_bytes(documents)
    package = root / "v2"
    package.mkdir()
    for name, payload in documents.items():
        (package / name).write_bytes(payload)
    return package


class MaterializationReplayTest(unittest.TestCase):
    def test_first_derived_query_pays_once_then_reuses(self) -> None:
        with TemporaryDirectory() as name:
            package = _fixture_package(Path(name))
            verified = verify_offline_replay_v2(
                package, source_v1_dir=V1_FIXTURE,
            )
            self.assertEqual(
                "VERIFIED_MATERIALIZATION_AWARE_REPLAY",
                verified["status"],
            )
            result = run_offline_replay_v2(
                package, policy_name="always-derived",
                mode="shared-dataset-sequence", query_count=2,
            )
            self.assertEqual("COMPLETE", result["status"])
            self.assertEqual(
                ["sampling", "frame_bundle"],
                result["steps"][0]["newly_built_components"],
            )
            self.assertEqual([], result["steps"][1]["newly_built_components"])
            self.assertEqual(
                1_626_982,
                result["metrics"]["total_build_source_bytes_proxy"],
            )
            self.assertIsNone(result["metrics"]["monetary_cost_usd"])
            self.assertFalse(result["objective"]["complete_monetary_ranking"])

    def test_independent_queries_repay_materialization(self) -> None:
        with TemporaryDirectory() as name:
            package = _fixture_package(Path(name))
            result = run_offline_replay_v2(
                package, policy_name="always-derived",
                mode="independent-query", query_count=2,
            )
            self.assertEqual(
                ["sampling", "frame_bundle"],
                result["steps"][1]["newly_built_components"],
            )
            self.assertEqual(
                2 * 1_626_982,
                result["metrics"]["total_build_source_bytes_proxy"],
            )

    def test_digest_then_fusion_only_adds_frame_bundle(self) -> None:
        with TemporaryDirectory() as name:
            package = _fixture_package(Path(name), digest_action=True)
            replay = load_offline_replay_package(V1_FIXTURE)
            v2 = {
                **replay,
                "cases": [json.loads(line) for line in
                          (package / CASES_NAME).read_text().splitlines()],
                "actions": [json.loads(line) for line in
                            (package / ACTIONS_NAME).read_text().splitlines()],
            }
            evaluator = MaterializationReplayEvaluator(
                v2, mode="shared-dataset-sequence",
            )
            case_id = v2["cases"][0]["case_id"]
            first = evaluator.step(
                case_id, "D2", remaining_queries=2,
            )
            second = evaluator.step(
                case_id, "D6", remaining_queries=1,
            )
            self.assertEqual(
                ["sampling", "captions", "digest"],
                first["newly_built_components"],
            )
            self.assertEqual(
                ["frame_bundle"], second["newly_built_components"],
            )
            self.assertIsNone(first["metrics"]["provider_input_units"])
            self.assertFalse(first["metrics"]["provider_usage_complete"])
            self.assertEqual(0, second["metrics"]["build_source_bytes_proxy"])

    def test_index_and_derived_share_sampling_and_captions(self) -> None:
        with TemporaryDirectory() as name:
            package = _fixture_package(Path(name), digest_action=True)
            v2 = {
                "cases": [json.loads(line) for line in
                          (package / CASES_NAME).read_text().splitlines()],
                "actions": [json.loads(line) for line in
                            (package / ACTIONS_NAME).read_text().splitlines()],
                "outcomes": [json.loads(line) for line in
                             (package / OUTCOMES_NAME).read_text().splitlines()],
            }
            evaluator = MaterializationReplayEvaluator(
                v2, mode="shared-dataset-sequence",
            )
            case_id = v2["cases"][0]["case_id"]
            index = evaluator.step(
                case_id, "D1", remaining_queries=2,
            )
            digest = evaluator.step(
                case_id, "D2", remaining_queries=1,
            )
            self.assertEqual(
                ["sampling", "captions", "index_embedding",
                 "index_projection"],
                index["newly_built_components"],
            )
            self.assertEqual(["digest"], digest["newly_built_components"])
            self.assertEqual(0, digest["metrics"]["build_source_bytes_proxy"])

    def test_unsupported_action_does_not_build_anything(self) -> None:
        with TemporaryDirectory() as name:
            package = _fixture_package(Path(name))
            rows = [json.loads(line) for line in
                    (package / CASES_NAME).read_text().splitlines()]
            actions = [json.loads(line) for line in
                       (package / ACTIONS_NAME).read_text().splitlines()]
            outcomes = [json.loads(line) for line in
                        (package / OUTCOMES_NAME).read_text().splitlines()]
            evaluator = MaterializationReplayEvaluator(
                {"cases": rows, "actions": actions, "outcomes": outcomes},
                mode="shared-dataset-sequence",
            )
            case_id = rows[0]["case_id"]
            failed = evaluator.step(
                case_id, "unfrozen-action", remaining_queries=1,
            )
            self.assertEqual("unsupported_action", failed["status"])
            self.assertEqual(
                [], evaluator.observation(
                    case_id, remaining_queries=1,
                ).to_dict()["state"]["built_components"],
            )

    def test_builder_binds_verified_n4_and_preserves_v1_outcomes(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            n4_root = root / "n4"
            n4_root.mkdir()
            case = load_offline_replay_package(V1_FIXTURE)["cases"][0]
            object_id = case["object_id"]
            rows = []
            for representation, derivation in (
                ("sampled_frame_bundle",
                 "rsi-exam-question-independent-frame-bundle-v1"),
                ("multimodal_digest",
                 "rsi-exam-question-independent-temporal-digest-v1"),
            ):
                rows.append({
                    "object_id": object_id,
                    "representation_id": representation,
                    "artifact_size_bytes": 250_000,
                    "provenance": {
                        "source_content_sha256": "a" * 64,
                        "derivation_id": derivation,
                    },
                })
            (n4_root / "n4-derived-data-package.json").write_bytes(
                _json_bytes({"objects": rows})
            )
            output = root / "new-v2"
            with patch(
                "pathfinder.rsi_exam.offline_replay_v2."
                "verify_n4_derived_data_package",
                return_value={"package_sha256": "b" * 64},
            ):
                built = build_offline_replay_v2(
                    V1_FIXTURE, n4_package_dir=n4_root,
                    output_dir=output, package_id="new-v2",
                    builder_commit="5" * 40,
                )
            self.assertEqual(1, built["case_count"])
            self.assertTrue(
                verify_offline_replay_v2(
                    output, source_v1_dir=V1_FIXTURE,
                )["source_v1_checked"]
            )

    def test_modified_dependency_is_rejected_even_with_new_checksums(self) -> None:
        with TemporaryDirectory() as name:
            package = _fixture_package(Path(name))
            actions = [json.loads(line) for line in
                       (package / ACTIONS_NAME).read_text().splitlines()]
            next(row for row in actions if row["action_id"] == "D2")[
                "required_build_components"
            ] = []
            (package / ACTIONS_NAME).write_bytes(_jsonl_bytes(actions))
            documents = {
                path.name: path.read_bytes()
                for path in package.iterdir() if path.name != CHECKSUMS_NAME
            }
            (package / CHECKSUMS_NAME).write_bytes(_checksum_bytes(documents))
            with self.assertRaisesRegex(
                OfflineReplayError, "build dependencies differ",
            ):
                verify_offline_replay_v2(package)


if __name__ == "__main__":
    unittest.main()
