from __future__ import annotations

import unittest
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
SYSTEM_PATH = CONFIGS / "multi_candidate_formal_v1_system.json"
PILOT_PATH = CONFIGS / "multi_candidate_formal_v1_pilot.json"
MANIFEST_PATH = (
    CONFIGS / "multi_candidate_formal_v1_data_agent_manifest.json"
)
ORACLE_PATH = CONFIGS / "multi_candidate_formal_v1_oracle.json"
AWM_PATH = CONFIGS / "multi_candidate_formal_v1_awm.json"
OED_PATH = CONFIGS / "multi_candidate_formal_v1_oed.json"


DESIGN_IDS = (
    "D_origin_remote",
    "D_local_frames",
    "D_local_digest",
    "D_local_pair",
)


class MultiCandidateFormalConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.system = load_config(SYSTEM_PATH)
        cls.pilot = load_flowmesh_pilot_config(PILOT_PATH)
        cls.manifest = load_data_agent_manifest(MANIFEST_PATH)
        cls.oracle = load_reduced_oracle_config(ORACLE_PATH)
        cls.awm = load_awm_config(AWM_PATH)
        cls.oed = load_oed_config(OED_PATH)

    def test_domain_and_trial_count_are_frozen(self) -> None:
        self.assertEqual(DESIGN_IDS, self.oracle.design_ids)
        self.assertEqual(DESIGN_IDS, self.pilot.design_ids)
        self.assertEqual(set(DESIGN_IDS), set(self.system.designs))
        self.assertEqual("D_origin_remote", self.oracle.safe_design_id)
        self.assertEqual(8, len(self.pilot.workloads))
        self.assertEqual(4, self.oracle.repetitions)
        self.assertEqual(128, len(build_trial_plan(self.pilot)))
        self.assertEqual(1.0, self.oracle.minimum_completion_rate)
        validate_flowmesh_pilot_config(self.pilot, self.system)

    def test_manifest_matches_every_design_path(self) -> None:
        self.assertIsNotNone(self.manifest.object_catalog)
        assert self.manifest.object_catalog is not None
        self.assertEqual(
            {workload.object_id for workload in self.pilot.workloads},
            set(self.manifest.object_catalog.objects),
        )
        for design_id, design in self.system.designs.items():
            for representation_id, path in design.paths.items():
                binding = self.manifest.representations[
                    representation_id
                ].plan_bindings[design_id]
                self.assertEqual(path.location, binding.location)
                self.assertEqual(path.latency_ms, binding.minimum_latency_ms)
                self.assertEqual(path.realized_cost, binding.realized_cost)

    def test_materialized_paths_are_served_by_the_matching_plan(self) -> None:
        assert self.manifest.object_catalog is not None
        config_dir = self.oracle.source_path.parent
        expected_counts = {
            "D_origin_remote": 0,
            "D_local_frames": 1,
            "D_local_digest": 1,
            "D_local_pair": 2,
        }
        for design_id, design in self.oracle.designs.items():
            self.assertEqual(
                expected_counts[design_id],
                len(design.materializations),
            )
            for workload in self.pilot.workloads:
                for materialization in design.materializations:
                    raw_target = Path(
                        materialization.target_template.format(
                            object_id=workload.object_id
                        )
                    )
                    target = (
                        raw_target
                        if raw_target.is_absolute()
                        else config_dir / raw_target
                    ).resolve()
                    target.relative_to(self.oracle.materialization_root)
                    configured = self.manifest.object_catalog.objects[
                        workload.object_id
                    ][materialization.representation_id].plan_paths[
                        design_id
                    ]
                    self.assertEqual(target, configured)

    def test_origin_paths_remain_outside_materialization_root(self) -> None:
        assert self.manifest.object_catalog is not None
        for representations in self.manifest.object_catalog.objects.values():
            for representation in representations.values():
                with self.assertRaises(ValueError):
                    representation.path.relative_to(
                        self.oracle.materialization_root
                    )

    def test_awm_split_and_oed_candidate_domain_are_preregistered(self) -> None:
        self.assertEqual(("D_origin_remote",), self.awm.observed_design_ids)
        self.assertEqual(2, self.awm.holdout_repetitions)
        self.assertEqual(16, self.awm.minimum_training_sessions)
        self.assertFalse(
            any(
                assumption.enabled
                for assumption in self.awm.assumptions.values()
            )
        )
        self.assertEqual(
            set(DESIGN_IDS) - {"D_origin_remote"},
            set(self.oed.reveal_candidates),
        )
        self.assertEqual((), self.oed.other_design_ids)
        self.assertEqual(0.5, self.oed.exploration_budget)
        self.assertEqual(0.2, self.oed.per_excursion_cap)

    def test_formal_inputs_are_not_marked_as_synthetic(self) -> None:
        identifiers = (
            self.oracle.oracle_id,
            self.awm.model_id,
            self.oed.controller_id,
            self.oracle.cost_model_status,
            self.oed.cost_model_status,
        )
        self.assertTrue(all("synthetic" not in value for value in identifiers))
        self.assertIn("not-distributed-cost", self.oracle.cost_model_status)


if __name__ == "__main__":
    unittest.main()
