from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pathfinder.simulator.full_flow_index_query_plan_catalog import (
    CHECKSUMS_NAME,
    INDEX_QUERY_PLAN_CATALOG_NAME,
    FullFlowIndexQueryPlanCatalogError,
    build_full_flow_index_query_plan_catalog,
    verify_full_flow_index_query_plan_catalog,
)
from pathfinder.simulator.full_flow_local_semantic_admission import (
    LOCAL_SEMANTICS_MODE,
    FrozenLocalSemanticExecutionInputs,
)


class FullFlowIndexQueryPlanCatalogTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.admission = self.root / "admission"
        self.admission.mkdir()
        self.index = self.root / "index"
        self.index.mkdir()
        (self.index / "lexical-index.json").write_text(
            json.dumps(
                {
                    "index_id": "visible-index-v1",
                    "candidate_object_ids": ["artifact-a", "artifact-b"],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        self.inputs = FrozenLocalSemanticExecutionInputs(
            admission={
                "semantics_mode": LOCAL_SEMANTICS_MODE,
                "admission_sha256": "a" * 64,
                "public_oracle_binding": {
                    "oracle_id": "oracle-v1",
                    "public_task_set_sha256": "b" * 64,
                },
            },
            bound_trials=(
                {
                    "trial_key": "scenario|W1|D1|r0000",
                    "artifact_object_id": "artifact-a",
                    "public_task_binding": {
                        "object_id": "artifact-a",
                        "question": "Which visible action occurs?",
                        "task_binding_sha256": "c" * 64,
                    },
                },
                {
                    "trial_key": "scenario|W1|D0|r0000",
                    "artifact_object_id": "artifact-b",
                    "public_task_binding": {
                        "object_id": "artifact-b",
                        "question": "What happens?",
                        "task_binding_sha256": "d" * 64,
                    },
                },
            ),
            bound_stages=(
                {
                    "trial_key": "scenario|W1|D1|r0000",
                    "action": "query-index",
                },
                {
                    "trial_key": "scenario|W1|D0|r0000",
                    "action": "access-raw-artifact",
                },
            ),
            representative_smokes=(),
            adapter_inventory={},
        )
        self.index_report = {
            "status": "VERIFIED",
            "index_id": "visible-index-v1",
            "index_sha256": self._index_sha256(),
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _index_sha256(self) -> str:
        import hashlib

        return hashlib.sha256(
            (self.index / "lexical-index.json").read_bytes()
        ).hexdigest()

    def _patches(self):
        return (
            mock.patch(
                "pathfinder.simulator.full_flow_index_query_plan_catalog."
                "load_full_flow_local_semantic_execution_inputs",
                return_value=self.inputs,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_index_query_plan_catalog."
                "verify_n2_index_package",
                return_value=self.index_report,
            ),
        )

    def test_freezes_only_indexed_trials_from_visible_inputs(self) -> None:
        output = self.root / "catalog"
        first, second = self._patches()
        with first, second:
            report = build_full_flow_index_query_plan_catalog(
                self.admission,
                self.index,
                output_dir=output,
            )
        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(1, report["indexed_trial_count"])
        document = json.loads(
            (output / INDEX_QUERY_PLAN_CATALOG_NAME).read_text()
        )
        self.assertEqual(
            ["artifact-a"],
            document["entries"][0]["candidate_object_ids"],
        )
        self.assertEqual(1, document["entries"][0]["top_k"])
        self.assertFalse(document["w4_retrieval_quality_evaluated"])
        self.assertNotIn("correct", json.dumps(document).casefold())

    def test_freeze_is_byte_deterministic(self) -> None:
        outputs = [self.root / "first", self.root / "second"]
        for output in outputs:
            first, second = self._patches()
            with first, second:
                build_full_flow_index_query_plan_catalog(
                    self.admission,
                    self.index,
                    output_dir=output,
                )
        self.assertEqual(
            {path.name: path.read_bytes() for path in outputs[0].iterdir()},
            {path.name: path.read_bytes() for path in outputs[1].iterdir()},
        )

    def test_tampering_fails_closed(self) -> None:
        output = self.root / "catalog"
        first, second = self._patches()
        with first, second:
            build_full_flow_index_query_plan_catalog(
                self.admission,
                self.index,
                output_dir=output,
            )
        path = output / INDEX_QUERY_PLAN_CATALOG_NAME
        path.write_bytes(path.read_bytes().replace(b"artifact-a", b"artifact-x"))
        first, second = self._patches()
        with first, second, self.assertRaisesRegex(
            FullFlowIndexQueryPlanCatalogError,
            "checksums",
        ):
            verify_full_flow_index_query_plan_catalog(
                output,
                local_semantic_admission_dir=self.admission,
                n2_index_package_dir=self.index,
            )

    def test_index_must_contain_public_trial_object(self) -> None:
        (self.index / "lexical-index.json").write_text(
            json.dumps(
                {
                    "index_id": "visible-index-v1",
                    "candidate_object_ids": ["artifact-b"],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        self.index_report["index_sha256"] = self._index_sha256()
        first, second = self._patches()
        with first, second, self.assertRaisesRegex(
            FullFlowIndexQueryPlanCatalogError,
            "absent from the frozen index",
        ):
            build_full_flow_index_query_plan_catalog(
                self.admission,
                self.index,
                output_dir=self.root / "catalog",
            )

    def test_output_file_set_is_exact(self) -> None:
        output = self.root / "catalog"
        first, second = self._patches()
        with first, second:
            build_full_flow_index_query_plan_catalog(
                self.admission,
                self.index,
                output_dir=output,
            )
        self.assertEqual(
            {INDEX_QUERY_PLAN_CATALOG_NAME, CHECKSUMS_NAME},
            {path.name for path in output.iterdir()},
        )


if __name__ == "__main__":
    unittest.main()
