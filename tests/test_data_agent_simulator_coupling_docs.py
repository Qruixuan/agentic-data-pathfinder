"""Keep the documented semantic-coupling contracts synchronized with code."""

from __future__ import annotations

import copy
import json
import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pathfinder.integrations.flowmesh import pathfinder_evidence_bridge
from pathfinder.simulator import data_agent_semantic_vertical


ROOT = Path(__file__).resolve().parents[1]
COUPLING_DOC = ROOT / "DATA_AGENT_SIMULATOR_COUPLING.md"


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise AssertionError(f"documented JSON repeats key {key!r}")
        value[key] = item
    return value


def _documented_json_examples() -> dict[str, dict[str, Any]]:
    text = COUPLING_DOC.read_text(encoding="utf-8")
    blocks = re.findall(r"```json\s+(.*?)\s+```", text, flags=re.DOTALL)
    examples: dict[str, dict[str, Any]] = {}
    for block in blocks:
        value = json.loads(block, object_pairs_hook=_unique_object)
        schema = value["schema_version"]
        if schema in examples:
            raise AssertionError(f"duplicate documented schema {schema!r}")
        examples[schema] = value
    return examples


class DataAgentSimulatorCouplingDocsTest(unittest.TestCase):
    def test_semantic_spec_example_matches_and_passes_strict_loader(self) -> None:
        examples = _documented_json_examples()
        schema = data_agent_semantic_vertical.DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION
        example = examples[schema]
        self.assertEqual(
            set(example),
            data_agent_semantic_vertical.DATA_AGENT_SEMANTIC_SPEC_REQUIRED_KEYS,
        )
        self.assertEqual(
            example["semantic_executor_node_id"],
            data_agent_semantic_vertical.DATA_AGENT_SEMANTIC_EXECUTOR_NODE_ID,
        )

        runnable = copy.deepcopy(example)
        runnable["artifact_sha256"] = "a" * 64
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "semantic-spec.json"
            path.write_text(json.dumps(runnable), encoding="utf-8")
            loaded = (
                data_agent_semantic_vertical
                .load_data_agent_frame_bundle_semantic_spec(path)
            )
        self.assertEqual(loaded.document, runnable)

    def test_binding_example_matches_and_passes_strict_loader(self) -> None:
        examples = _documented_json_examples()
        schema = pathfinder_evidence_bridge.PATHFINDER_EVIDENCE_SPEC_SCHEMA_VERSION
        example = examples[schema]
        self.assertEqual(
            set(example),
            pathfinder_evidence_bridge.PATHFINDER_EVIDENCE_SPEC_REQUIRED_KEYS,
        )
        self.assertEqual(
            set(example["bindings"][0]),
            pathfinder_evidence_bridge.PATHFINDER_EVIDENCE_BINDING_REQUIRED_KEYS,
        )

        runnable = copy.deepcopy(example)
        runnable["matrix_plan_sha256"] = "b" * 64
        runnable["bindings"][0]["semantic_spec_sha256"] = "c" * 64
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "binding-spec.json"
            path.write_text(json.dumps(runnable), encoding="utf-8")
            loaded, _digest = pathfinder_evidence_bridge._load_binding_spec(path)
        self.assertEqual(loaded, runnable)

    def test_related_docs_preserve_the_cross_layer_claim_boundary(self) -> None:
        coupling = " ".join(COUPLING_DOC.read_text(encoding="utf-8").split())
        for statement in (
            "calls N6 directly",
            "does not submit a FlowMesh workflow",
            "posthoc cross-layer bundle",
            "flowmesh_semantic_execution_verified = false",
            "cost_basis = unavailable",
            "eligible_for_awm_oed = false",
            "eligible_for_scientific_claims = false",
            "base64 exists only in the bounded in-memory request",
            (
                "rendered question, prompt, endpoint URL, bearer token, nor "
                "API key is recorded"
            ),
        ):
            self.assertIn(statement, coupling)

        for name in (
            "README.md",
            "FLOWMESH_INFRA_SIMULATOR.md",
            "LOCAL_CONTAINER_SEMANTIC_EXECUTION.md",
        ):
            text = " ".join((ROOT / name).read_text(encoding="utf-8").split())
            with self.subTest(document=name):
                self.assertIn("N6 directly", text)
                self.assertIn("FlowMesh semantic scheduling", text)
                self.assertIn("posthoc", text)
                self.assertIn("no cost basis", text)
                self.assertIn("AWM/OED", text)
                self.assertIn("scientific eligibility", text)
                self.assertIn("credentials", text)
                self.assertIn("questions", text)
                self.assertIn("base64 frame payloads", text)


if __name__ == "__main__":
    unittest.main()
