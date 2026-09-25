"""The light-D blue-green spec changes only the intended public bindings."""

from pathlib import Path
import unittest

from experiments.nextqa_atphard_20260925.make_light_fusion_specs import (
    build,
)


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "artifacts/nextqa-atphard-8x5-deployment-specs-v1"
IMAGE = "sha256:" + "a" * 64


class LightFusionSpecTests(unittest.TestCase):
    def test_parallel_specs_preserve_original_services(self):
        if not BASE.is_dir():
            self.skipTest("frozen 8x5 deployment specs are absent")
        specs, overlays = build(BASE, IMAGE)
        self.assertEqual(set(specs), {
            "n4", "n7-cache", "n8-cache", "n7-route", "n8-route",
        })
        self.assertEqual(specs["n4"]["host_port"], 19124)
        self.assertEqual(specs["n4"]["overrides"]["PATHFINDER_N4_PUBLIC_BASE_URL"],
                         "http://pathfinder-full-flow-n4-derived-data-agent:19124")
        self.assertEqual(specs["n7-route"]["image"], IMAGE)
        self.assertEqual(specs["n8-route"]["image"], IMAGE)
        for name, spec in specs.items():
            self.assertEqual(spec["host_port"], int(next(
                value for key, value in spec["overrides"].items()
                if key.endswith("_HOST_PORT"))))
            self.assertTrue(spec["predecessor"].startswith("a8x5"))
            self.assertTrue(spec["project"].startswith("a8x5light"))
            self.assertFalse(any("TOKEN" in key or "SECRET" in key
                                 or "API_KEY" in key
                                 for key in spec["overrides"]))
        self.assertEqual(set(overlays), {"n7", "n8"})
        self.assertNotIn("pathfinder-a8x5-n7-route-state-v1", overlays["n7"])
        self.assertNotIn("pathfinder-a8x5-n8-route-state-v1", overlays["n8"])
        retry, retry_overlays = build(BASE, IMAGE, route_iteration=2)
        self.assertEqual(retry["n7-cache"], specs["n7-cache"])
        self.assertEqual(retry["n8-cache"], specs["n8-cache"])
        self.assertEqual(retry["n7-route"]["project"], "a8x5light2n7route")
        self.assertIn("/deploy-v2/route-overlay-n7.yaml",
                      retry["n7-route"]["fragments"][1])
        self.assertIn("pathfinder-a8x5-light-n7-route-state-v2",
                      retry_overlays["n7"])


if __name__ == "__main__":
    unittest.main()
