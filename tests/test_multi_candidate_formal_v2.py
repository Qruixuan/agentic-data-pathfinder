from __future__ import annotations

import json
import unittest
from collections import Counter
from pathlib import Path

from pathfinder.awm import load_awm_config
from pathfinder.config import load_config
from pathfinder.data_agent_manifest import load_data_agent_manifest
from pathfinder.integrations.flowmesh.pilot import (
    build_trial_plan,
    load_flowmesh_pilot_config,
    validate_flowmesh_pilot_config,
)
from pathfinder.oed import load_oed_config
from pathfinder.reduced_oracle import load_reduced_oracle_config


ROOT = Path(__file__).resolve().parents[1]
CONFIGS = ROOT / "configs"
PREFIX = "multi_candidate_formal_v2"
DESIGN_IDS = (
    "D_origin_remote",
    "D_local_frames",
    "D_local_digest",
    "D_local_pair",
)
V1_OBJECT_IDS = {
    "nextqa-val-2834146886",
    "nextqa-val-3441428429",
    "nextqa-val-2976913210",
    "nextqa-val-4882821564",
    "nextqa-val-4260763967",
    "nextqa-val-9088819598",
    "nextqa-val-8547321641",
    "nextqa-val-4130504920",
}
NEW_OBJECT_IDS = {
    "nextqa-val-6356067859",
    "nextqa-val-5296635780",
    "nextqa-val-5735711594",
    "nextqa-val-8132842161",
    "nextqa-val-3462517143",
    "nextqa-val-5026660202",
    "nextqa-val-4942054721",
    "nextqa-val-5840177726",
}


class MultiCandidateFormalV2ConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.system = load_config(CONFIGS / f"{PREFIX}_system.json")
        cls.pilot = load_flowmesh_pilot_config(
            CONFIGS / f"{PREFIX}_pilot.json"
        )
        cls.manifest = load_data_agent_manifest(
            CONFIGS / f"{PREFIX}_data_agent_manifest.json"
        )
        cls.oracle = load_reduced_oracle_config(
            CONFIGS / f"{PREFIX}_oracle.json"
        )
        cls.awm_power = load_awm_config(
            CONFIGS / f"{PREFIX}_awm_v3alpha4_power_diagnostic.json"
        )
        cls.awm_oed = load_awm_config(
            CONFIGS / f"{PREFIX}_awm_v3alpha4_oed.json"
        )
        cls.oed = load_oed_config(
            CONFIGS / f"{PREFIX}_oed_v3alpha4.json"
        )
        cls.selection = json.loads(
            (CONFIGS / f"{PREFIX}_workload_selection.json").read_text()
        )

    def test_expansion_has_sixteen_independent_objects(self) -> None:
        object_ids = [workload.object_id for workload in self.pilot.workloads]
        self.assertEqual(16, len(object_ids))
        self.assertEqual(16, len(set(object_ids)))
        self.assertEqual(V1_OBJECT_IDS | NEW_OBJECT_IDS, set(object_ids))
        self.assertFalse(V1_OBJECT_IDS & NEW_OBJECT_IDS)
        self.assertEqual(4, self.pilot.repetitions)
        self.assertEqual(256, len(build_trial_plan(self.pilot)))
        self.assertEqual(64, self.system.pilot.trials_per_cell)
        validate_flowmesh_pilot_config(self.pilot, self.system)

    def test_selection_is_pinned_and_mirrors_v1_subtypes(self) -> None:
        source = self.selection["source"]
        self.assertEqual(
            "2432e9724f88ed9f40010e2989f104570a91de4e",
            source["repository_commit"],
        )
        self.assertEqual(64, len(source["annotation_sha256"]))
        selected = self.selection["added_workloads"]
        self.assertEqual(8, len(selected))
        self.assertEqual(
            Counter({"TN": 2, "TC": 1, "CW": 3, "DC": 1, "DO": 1}),
            Counter(row["type"] for row in selected),
        )
        self.assertEqual(
            NEW_OBJECT_IDS,
            {row["object_id"] for row in selected},
        )
        for row in selected:
            self.assertEqual(
                row["answer_text"],
                row["options"][row["answer_index"]],
            )

    def test_catalog_and_materializations_cover_every_object(self) -> None:
        self.assertIsNotNone(self.manifest.object_catalog)
        assert self.manifest.object_catalog is not None
        self.assertEqual(
            {workload.object_id for workload in self.pilot.workloads},
            set(self.manifest.object_catalog.objects),
        )
        config_dir = self.oracle.source_path.parent
        for object_id, representations in (
            self.manifest.object_catalog.objects.items()
        ):
            for representation_id, representation in representations.items():
                self.assertIn("multi_candidate_formal_v2", str(representation.path))
                with self.assertRaises(ValueError):
                    representation.path.relative_to(
                        self.oracle.materialization_root
                    )
                for design_id, configured in representation.plan_paths.items():
                    materialization = next(
                        item
                        for item in self.oracle.designs[design_id].materializations
                        if item.representation_id == representation_id
                    )
                    raw_target = Path(
                        materialization.target_template.format(
                            object_id=object_id
                        )
                    )
                    expected = (
                        raw_target
                        if raw_target.is_absolute()
                        else config_dir / raw_target
                    ).resolve()
                    self.assertEqual(expected, configured)

    def test_v3alpha4_is_frozen_for_sixteen_workloads(self) -> None:
        self.assertEqual(DESIGN_IDS, self.oracle.design_ids)
        self.assertEqual("D_origin_remote", self.oracle.safe_design_id)
        self.assertEqual(32, self.awm_power.minimum_training_sessions)
        self.assertEqual(32, self.awm_oed.minimum_training_sessions)
        self.assertEqual(16, self.awm_power.confidence.paired_gain_minimum_pairs)
        self.assertEqual(16, self.awm_oed.confidence.paired_gain_minimum_pairs)
        self.assertEqual(5, self.awm_power.confidence.joint_state_bin_count)
        self.assertEqual(tuple(DESIGN_IDS), self.awm_power.observed_design_ids)
        self.assertEqual(("D_origin_remote",), self.awm_oed.observed_design_ids)
        self.assertEqual(
            set(DESIGN_IDS) - {"D_origin_remote"},
            set(self.oed.reveal_candidates),
        )
        self.assertIn("not-distributed-cost", self.oracle.cost_model_status)
        self.assertIn("not-distributed-cost", self.oed.cost_model_status)


if __name__ == "__main__":
    unittest.main()
