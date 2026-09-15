from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_logical_routes import (
    CHECKSUMS_NAME,
    COVERAGE_NAME,
    PLAN_NAME,
    SERVICE_CATALOG_NAME,
    STAGES_NAME,
    TRIALS_NAME,
    FullFlowLogicalRouteError,
    compile_full_flow_logical_routes,
    verify_full_flow_logical_routes,
)
from pathfinder.simulator.portable import build_portable_execution_plan


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(
        (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    )


def _restamp_logical(root: Path) -> None:
    plan_path = root / PLAN_NAME
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["output_sha256"] = {
        name: _sha256((root / name).read_bytes())
        for name in (
            COVERAGE_NAME,
            STAGES_NAME,
            TRIALS_NAME,
            SERVICE_CATALOG_NAME,
        )
    }
    plan.pop("plan_sha256", None)
    canonical = json.dumps(
        plan,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    plan["plan_sha256"] = _sha256(canonical)
    _write_json(plan_path, plan)
    content_names = sorted(
        name for name in (
            COVERAGE_NAME,
            PLAN_NAME,
            SERVICE_CATALOG_NAME,
            STAGES_NAME,
            TRIALS_NAME,
        )
    )
    (root / CHECKSUMS_NAME).write_text(
        "".join(
            f"{_sha256((root / name).read_bytes())}  {name}\n"
            for name in content_names
        ),
        encoding="utf-8",
    )


def _restamp_container(root: Path) -> None:
    manifest_path = root / "container_plan_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for name in (
        "container_operations.jsonl",
        "container_readiness.json",
        "container_topology.json",
    ):
        manifest["output_sha256"][name] = _sha256((root / name).read_bytes())
    _write_json(manifest_path, manifest)
    names = sorted(
        (
            "container_operations.jsonl",
            "container_plan_manifest.json",
            "container_readiness.json",
            "container_topology.json",
        )
    )
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{_sha256((root / name).read_bytes())}  {name}\n"
            for name in names
        ),
        encoding="utf-8",
    )


class FullFlowLogicalRoutesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        build_portable_execution_plan(SCENARIO, output_dir=cls.portable)
        plan_container_backend(
            SCENARIO,
            cls.portable,
            CONTAINER_SPEC,
            output_dir=cls.container,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _compile(self, name: str) -> Path:
        output = self.root / name
        if output.exists():
            shutil.rmtree(output)
        compile_full_flow_logical_routes(
            SCENARIO,
            self.container,
            output_dir=output,
        )
        return output

    def test_compiles_complete_endpoint_free_4x8_route_matrix(self) -> None:
        output = self._compile("complete")
        report = json.loads((output / PLAN_NAME).read_text(encoding="utf-8"))
        summary = report["coverage_summary"]
        self.assertEqual(64, summary["trial_count"])
        self.assertEqual(32, summary["matrix_cell_count"])
        self.assertEqual(500, summary["source_matrix_operation_stage_count"])
        self.assertEqual(658, summary["logical_stage_count"])
        self.assertEqual(6, summary["artifact_provisioning_chain_count"])
        self.assertEqual(
            {
                "indexed-raw": 12,
                "local-cache-derived": 16,
                "raw": 20,
                "remote-derived": 16,
            },
            summary["route_family_trial_counts"],
        )
        self.assertEqual(
            {
                "N1": 64,
                "N2": 16,
                "N3": 64,
                "N4": 32,
                "N5": 32,
                "N6": 64,
                "N7": 32,
                "N8": 32,
            },
            summary["service_node_trial_coverage"],
        )
        self.assertEqual(
            sorted(
                (
                    CHECKSUMS_NAME,
                    COVERAGE_NAME,
                    PLAN_NAME,
                    SERVICE_CATALOG_NAME,
                    STAGES_NAME,
                    TRIALS_NAME,
                )
            ),
            sorted(path.name for path in output.iterdir()),
        )

    def test_route_rows_expose_required_nodes_and_contracts(self) -> None:
        output = self._compile("routes")
        rows = _jsonl(output / TRIALS_NAME)

        def route(workload: str, design: str) -> dict:
            return next(
                row for row in rows
                if row["workload_id"] == workload
                and row["design_id"] == design
                and row["repetition"] == 0
            )

        raw_w1_d1 = route("smoke-descriptive", "D1")
        self.assertEqual("raw", raw_w1_d1["route_family"])
        self.assertEqual("none", raw_w1_d1["index_mode"])
        self.assertEqual(["raw_video"], raw_w1_d1["representation_ids"])

        indexed = route("smoke-temporal", "D1")
        self.assertEqual("indexed-raw", indexed["route_family"])
        self.assertEqual("global", indexed["index_mode"])
        self.assertIn("N2.global-index", indexed["required_service_contract_ids"])

        remote = route("smoke-retrieval", "D2")
        self.assertEqual("remote-derived", remote["route_family"])
        self.assertEqual(
            ["multimodal_digest", "sampled_frame_bundle"],
            remote["representation_ids"],
        )
        self.assertEqual(
            ["N1", "N2", "N3", "N4", "N5", "N6", "N7"],
            remote["required_service_nodes"],
        )

        local = route("smoke-causal", "D7")
        self.assertEqual("local-cache-derived", local["route_family"])
        self.assertEqual("none", local["index_mode"])
        self.assertTrue(local["conditional_cache_branch"])
        self.assertEqual("N8", local["cache_node_id"])
        self.assertIn("N8.persistent-cache", local[
            "required_service_contract_ids"
        ])

        local_retrieval = route("smoke-retrieval", "D7")
        self.assertEqual("local", local_retrieval["index_mode"])
        self.assertIn("N8.local-index", local_retrieval[
            "required_service_contract_ids"
        ])

    def test_provisioning_and_hidden_score_are_explicit_extensions(self) -> None:
        output = self._compile("extensions")
        stages = _jsonl(output / STAGES_NAME)
        provisioning = [row for row in stages if row["phase"] == "provisioning"]
        self.assertEqual(30, len(provisioning))
        self.assertTrue(all(
            not row["counted_in_source_matrix"] for row in provisioning
        ))
        self.assertEqual(
            6,
            len({row["scope_id"] for row in provisioning}),
        )
        actions = {row["action"] for row in provisioning}
        self.assertIn("access-raw-artifact", actions)
        self.assertIn("materialize-representation", actions)
        self.assertIn("publish-derived-artifact", actions)

        trial_key = next(
            row["trial_key"]
            for row in _jsonl(output / TRIALS_NAME)
            if row["workload_class"] == "W1"
            and row["design_id"] == "D0"
            and row["repetition"] == 0
        )
        scoped = [row for row in stages if row["trial_key"] == trial_key]
        infer = next(row for row in scoped if row["action"] == "infer")
        answer = next(
            row for row in scoped
            if row["stage_key"].endswith("|return-answer")
        )
        score = next(
            row for row in scoped
            if row["stage_key"].endswith("|hidden-score")
        )
        self.assertEqual([infer["stage_key"]], answer["dependency_stage_keys"])
        self.assertEqual([answer["stage_key"]], score["dependency_stage_keys"])
        self.assertEqual("N1.hidden-score", score["service_contract_id"])
        self.assertIsNone(score["planned_logical_bytes"])

    def test_conditional_cache_branches_are_preserved(self) -> None:
        output = self._compile("conditions")
        stages = _jsonl(output / STAGES_NAME)
        trial_prefix = (
            "flowmesh-infra-4x8-local-smoke-v1|"
            "smoke-causal|D3|r0000"
        )
        scoped = [row for row in stages if row["trial_key"] == trial_prefix]
        conditions = [row["condition"] for row in scoped if row["condition"]]
        self.assertEqual(8, len(conditions))
        self.assertEqual({"hit", "miss"}, {row["equals"] for row in conditions})
        for condition in conditions:
            lookup = next(
                row for row in scoped
                if row["stage_key"] == condition["cache_operation_key"]
            )
            self.assertEqual("lookup", lookup["action"])

    def test_service_catalog_contains_no_deployment_binding(self) -> None:
        output = self._compile("safe")
        combined = b"".join(
            path.read_bytes()
            for path in output.iterdir()
            if path.name != CHECKSUMS_NAME
        ).decode("utf-8")
        lowered = combined.lower()
        self.assertNotIn("http://", lowered)
        self.assertNotIn("https://", lowered)
        self.assertNotIn("127.0.0.1", lowered)
        self.assertNotIn("pathfinder-sim-n", lowered)
        self.assertNotIn(str(self.root).lower(), lowered)
        catalog = json.loads(
            (output / SERVICE_CATALOG_NAME).read_text(encoding="utf-8")
        )
        self.assertTrue(all(
            row["deployment_binding_included"] is False
            and row["secret_material_included"] is False
            for row in catalog["service_contracts"]
        ))

    def test_output_is_byte_deterministic_and_strictly_verifiable(self) -> None:
        first = self._compile("deterministic-a")
        second = self._compile("deterministic-b")
        self.assertEqual(
            {path.name: path.read_bytes() for path in first.iterdir()},
            {path.name: path.read_bytes() for path in second.iterdir()},
        )
        report = verify_full_flow_logical_routes(
            first,
            SCENARIO,
            self.container,
        )
        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(64, report["trial_count"])
        self.assertFalse(report["deployment_binding_included"])

    def test_checksum_tampering_is_rejected(self) -> None:
        output = self._compile("checksum-tamper")
        with (output / TRIALS_NAME).open("ab") as handle:
            handle.write(b" ")
        with self.assertRaisesRegex(
            FullFlowLogicalRouteError,
            "checksum mismatch",
        ):
            verify_full_flow_logical_routes(output, SCENARIO, self.container)

    def test_restamped_semantic_tampering_fails_recompilation(self) -> None:
        output = self._compile("semantic-tamper")
        rows = _jsonl(output / STAGES_NAME)
        infer = next(row for row in rows if row["action"] == "infer")
        infer["action"] = "unregistered-inference"
        (output / STAGES_NAME).write_text(
            "".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                for row in rows
            ),
            encoding="utf-8",
        )
        _restamp_logical(output)
        with self.assertRaisesRegex(
            FullFlowLogicalRouteError,
            "deterministic source recompilation",
        ):
            verify_full_flow_logical_routes(output, SCENARIO, self.container)

    def test_source_schema_extension_is_rejected_even_when_restamped(self) -> None:
        source = self.root / "mutated-container"
        if source.exists():
            shutil.rmtree(source)
        shutil.copytree(self.container, source)
        operations = _jsonl(source / "container_operations.jsonl")
        operations[0]["unexpected_runtime_field"] = "not-allowed"
        (source / "container_operations.jsonl").write_text(
            "".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                for row in operations
            ),
            encoding="utf-8",
        )
        _restamp_container(source)
        with self.assertRaisesRegex(
            FullFlowLogicalRouteError,
            "fields changed",
        ):
            compile_full_flow_logical_routes(
                SCENARIO,
                source,
                output_dir=self.root / "source-schema-output",
            )

    def test_nonportable_compiler_id_and_extra_files_are_rejected(self) -> None:
        with self.assertRaisesRegex(FullFlowLogicalRouteError, "not portable"):
            compile_full_flow_logical_routes(
                SCENARIO,
                self.container,
                output_dir=self.root / "bad-compiler",
                compiler_id="bad/compiler",
            )
        output = self._compile("extra-file")
        (output / "unexpected.txt").write_text("x", encoding="utf-8")
        with self.assertRaisesRegex(
            FullFlowLogicalRouteError,
            "file set changed",
        ):
            verify_full_flow_logical_routes(output, SCENARIO, self.container)


if __name__ == "__main__":
    unittest.main()
