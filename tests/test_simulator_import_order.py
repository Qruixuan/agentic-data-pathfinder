import subprocess
import sys
import unittest
from pathlib import Path


class SimulatorImportOrderTests(unittest.TestCase):
    def test_flowmesh_can_be_imported_before_lazy_simulator_exports(self):
        repo_root = Path(__file__).resolve().parents[1]
        code = "\n".join(
            (
                "import pathfinder.integrations.flowmesh.w4_candidate_matrix",
                "import pathfinder.simulator as simulator",
                "assert simulator.REQUIRED_CODE_READY == 'REQUIRED_CODE_READY'",
                "assert simulator.LiveW4CandidateOperationExecutor is not None",
            )
        )

        completed = subprocess.run(
            [sys.executable, "-c", code],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(
            completed.returncode,
            0,
            msg=completed.stdout + completed.stderr,
        )

    def test_heavy_simulator_module_can_be_imported_directly(self):
        repo_root = Path(__file__).resolve().parents[1]
        code = "\n".join(
            (
                "import pathfinder.simulator.full_flow_pre_upcloud_readiness",
                "import pathfinder.integrations.flowmesh.container_matrix",
                "import pathfinder.simulator as simulator",
                "assert simulator.freeze_full_flow_pre_upcloud_readiness",
            )
        )

        completed = subprocess.run(
            [sys.executable, "-c", code],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(
            completed.returncode,
            0,
            msg=completed.stdout + completed.stderr,
        )


if __name__ == "__main__":
    unittest.main()
