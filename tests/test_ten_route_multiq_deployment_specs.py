import unittest

from experiments.ten_route_multiq_20260925.make_deployment_specs import (
    ORIGINS,
    _route_overlay,
    build,
)


class TenRouteDeploymentSpecTests(unittest.TestCase):
    def test_isolated_services_and_origins(self):
        reference = (
            "services:\n"
            "  pathfinder-full-flow-n7-execution-compute:\n"
            "    command:\n"
            "      - N7\n"
            "      - ${PATHFINDER_N7_ROUTE_STATE_DIR:?}\n"
            "      - ${PATHFINDER_N7_ROUTE_LISTEN_PORT:?}\n"
            "    volumes:\n"
            "      - multiq-n7-route-state\n"
            "volumes:\n"
            "  multiq-n7-route-state:\n"
            "    name: pathfinder-multiq-n7-route-state-d328726\n"
        )
        specs, overlays = build(reference, 2_038_602,
                                "sha256:" + "a" * 64)
        self.assertEqual(len(specs), 9)
        self.assertEqual(len({row["project"] for row in specs.values()}), 8)
        self.assertEqual(specs["n7-cache"]["overrides"][
            "PATHFINDER_N7_CACHE_CAPACITY_BYTES"], "2038602")
        self.assertEqual(specs["n8-route"]["overrides"][
            "PATHFINDER_N3_DATA_AGENT_BASE_URL"],
            ORIGINS["PATHFINDER_N3_DATA_AGENT_BASE_URL"])
        self.assertEqual(specs["n7-route"]["image"],
                         "sha256:" + "a" * 64)
        self.assertIn("pathfinder-t60-n8-route-state-v1", overlays["N8"])
        self.assertIn("      - N8\n", overlays["N8"])
        self.assertNotIn("PATHFINDER_N7_ROUTE_STATE_DIR", overlays["N8"])
        for row in specs.values():
            self.assertFalse(any("TOKEN" in key or "SECRET" in key
                                 for key in row["overrides"]))

    def test_overlay_rejects_shape_drift(self):
        with self.assertRaises(ValueError):
            _route_overlay("services: {}\n", "N8")


if __name__ == "__main__":
    unittest.main()
