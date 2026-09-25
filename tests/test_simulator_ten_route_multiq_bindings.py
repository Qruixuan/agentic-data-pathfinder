"""The ten-observation input binder reuses canonical multiq identities."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.rsi_exam.ten_route_multiq_plan import (
    freeze_ten_route_multiq_plan, ten_route_trial_key,
)
from pathfinder.simulator.interleaved_multiq_route_bindings import _expected
from tests.test_rsi_exam_ten_route_multiq_plan import public_questions


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode()


class TenRouteMultiqBindingsTests(unittest.TestCase):
    def test_light_derived_binds_one_n4_artifact_per_supplemental_route(self):
        omitted = {"video-a": "causal", "video-b": "temporal",
                   "video-c": "descriptive"}
        questions = [row for row in public_questions()
                     if row["object_id"] in omitted
                     and row["stratum"] != omitted[row["object_id"]]]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = root / "plan"
            freeze_ten_route_multiq_plan(
                questions, seed="light-bind-seed", experiment_id="light-bind",
                public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
                output_dir=plan, derived_profile="frame-only",
            )
            for name in ("n1", "n3", "n4"):
                (root / name).mkdir()
            public_tasks = sorted(({
                "object_id": row["object_id"],
                "task_binding_sha256": row["public_task_sha256"],
                "success_scoring_rule": (
                    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE),
                "answer_option_ids": [option["option_id"]
                                      for option in row["answer_options"]],
            } for row in questions), key=lambda row: (
                row["object_id"], row["task_binding_sha256"],
            ))
            (root / "n1/n1-oracle-preselection-commitment.json").write_bytes(
                canonical({"public_task_set_sha256": hashlib.sha256(
                    canonical(public_tasks)).hexdigest()}))
            (root / "n3/raw-cold-data-plane.json").write_bytes(canonical({
                "raw_objects": [], "question_selections": [],
            }))
            (root / "n4/n4-derived-data-package.json").write_bytes(canonical({
                "objects": [{
                    "object_id": object_id,
                    "representation_id": "sampled_frame_bundle",
                    "artifact_sha256": "3" * 64,
                    "artifact_size_bytes": 31,
                } for object_id in omitted],
            }))
            schedule = [json.loads(line) for line in (
                plan / "ten-route-multiq-schedule.jsonl"
            ).read_bytes().splitlines()]
            access = {
                (ten_route_trial_key(
                    "light-bind", item["question_id"], slot["design_id"],
                    slot["repetition"],
                ), "N4", item["object_id"], "sampled_frame_bundle"):
                    "plan-bound"
                for item in schedule for slot in item["route_slots"]
            }
            with (patch(
                    "pathfinder.simulator.interleaved_multiq_route_bindings."
                    "verify_n1_oracle_preselection_commitment",
                    return_value={"label_count": 6,
                                  "label_values_returned": False,
                                  "commitment_sha256": "4" * 64},
                  ), patch(
                    "pathfinder.simulator.interleaved_multiq_route_bindings."
                    "derive_n3_multiq_question_policies", return_value=[],
                  ), patch(
                    "pathfinder.simulator.interleaved_multiq_route_bindings."
                    "interleaved_data_agent_plan_bindings",
                    return_value=access,
                  )):
                manifest, routes = _expected(
                    plan_dir=plan, n1_public_commitment_dir=root / "n1",
                    n3_package_dir=root / "n3", raw_package_dir=root,
                    n4_package_dir=root / "n4", query_dir=root,
                    video_index_dir=root, preparation_dir=root,
                    caption_dir=root,
                )
            self.assertEqual(len(routes), 36)
            self.assertEqual(manifest["data_agent_binding_count"], 36)
            self.assertEqual({row["arm_id"] for row in routes}, {"D", "DC"})
            self.assertTrue(all(len(row["inputs"]) == 1
                                and row["inputs"][0]["representation_id"]
                                == "sampled_frame_bundle" for row in routes))

    def test_sixty_routes_have_ninety_six_exact_agent_plans(self):
        omitted = {"video-a": "causal", "video-b": "temporal",
                   "video-c": "descriptive"}
        questions = [row for row in public_questions()
                     if row["object_id"] in omitted
                     and row["stratum"] != omitted[row["object_id"]]]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = root / "plan"
            freeze_ten_route_multiq_plan(
                questions, seed="binding-test-seed", experiment_id="bind-test",
                public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
                output_dir=plan,
            )
            n1 = root / "n1"
            n3 = root / "n3"
            n4 = root / "n4"
            for path in (n1, n3, n4):
                path.mkdir()
            public_tasks = sorted(({
                "object_id": row["object_id"],
                "task_binding_sha256": row["public_task_sha256"],
                "success_scoring_rule": (
                    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                ),
                "answer_option_ids": [option["option_id"]
                                      for option in row["answer_options"]],
            } for row in questions), key=lambda row: (
                row["object_id"], row["task_binding_sha256"],
            ))
            (n1 / "n1-oracle-preselection-commitment.json").write_bytes(
                canonical({"public_task_set_sha256": hashlib.sha256(
                    canonical(public_tasks)
                ).hexdigest()}))
            raw = [{"object_id": oid, "artifact_sha256": "1" * 64,
                    "artifact_size_bytes": 101} for oid in omitted]
            selected = [{"object_id": row["object_id"],
                         "task_binding_sha256": row["public_task_sha256"],
                         "artifact_sha256": "2" * 64,
                         "artifact_size_bytes": 21} for row in questions]
            (n3 / "raw-cold-data-plane.json").write_bytes(canonical({
                "raw_objects": raw, "question_selections": selected,
            }))
            (n4 / "n4-derived-data-package.json").write_bytes(canonical({
                "objects": [{"object_id": oid, "representation_id": rep,
                             "artifact_sha256": "3" * 64,
                             "artifact_size_bytes": 31}
                            for oid in omitted for rep in
                            ("multimodal_digest", "sampled_frame_bundle")],
            }))
            schedule = [json.loads(line) for line in
                        (plan / "ten-route-multiq-schedule.jsonl")
                        .read_bytes().splitlines()]
            access = {}
            for row in schedule:
                for slot in row["route_slots"]:
                    key = ten_route_trial_key(
                        "bind-test", row["question_id"],
                        slot["design_id"], slot["repetition"],
                    )
                    reps = (("N3", "raw_video") if slot["arm_id"] == "R"
                            else ("N3", "indexed_temporal_frame_bundle")
                            if slot["arm_id"] == "I" else None)
                    pairs = ([reps] if reps is not None else
                             [("N4", "multimodal_digest"),
                              ("N4", "sampled_frame_bundle")])
                    for node, rep in pairs:
                        access[key, node, row["object_id"], rep] = "plan-bound"
            self.assertEqual(len(access), 96)
            with (patch(
                    "pathfinder.simulator.interleaved_multiq_route_bindings."
                    "verify_n1_oracle_preselection_commitment",
                    return_value={"label_count": 6,
                                  "label_values_returned": False,
                                  "commitment_sha256": "4" * 64},
                  ), patch(
                    "pathfinder.simulator.interleaved_multiq_route_bindings."
                    "derive_n3_multiq_question_policies", return_value=[],
                  ), patch(
                    "pathfinder.simulator.interleaved_multiq_route_bindings."
                    "interleaved_data_agent_plan_bindings",
                    return_value=access,
                  )):
                manifest, routes = _expected(
                    plan_dir=plan, n1_public_commitment_dir=n1,
                    n3_package_dir=n3, raw_package_dir=root,
                    n4_package_dir=n4, query_dir=root,
                    video_index_dir=root, preparation_dir=root,
                    caption_dir=root,
                )
            self.assertEqual(len(routes), 60)
            self.assertEqual(manifest["data_agent_binding_count"], 96)
            self.assertEqual({route["executor_node_id"] for route in routes},
                             {"N7", "N8"})
            indexed = [route for route in routes if route["arm_id"] == "I"]
            self.assertEqual(len(indexed), 12)
            self.assertTrue(all(len(route["inputs"]) == 1
                                and route["inputs"][0]["node_id"] == "N3"
                                for route in indexed))


if __name__ == "__main__":
    unittest.main()
