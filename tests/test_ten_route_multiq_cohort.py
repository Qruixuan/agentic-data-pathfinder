"""Only public, video-disjoint cohort inputs can be frozen."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from experiments.fresh_multiq_cohort import canonical
from experiments.ten_route_multiq_cohort import (
    freeze_protocol, freeze_selection,
)


def package(root: Path, name: str, document: dict) -> None:
    root.mkdir()
    payload = canonical(document) + b"\n"
    (root / name).write_bytes(payload)
    (root / "SHA256SUMS").write_bytes(
        f"{hashlib.sha256(payload).hexdigest()}  {name}\n".encode()
    )


class TenRouteMultiqCohortTests(unittest.TestCase):
    def test_selection_rejects_exposed_video_and_answer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            protocol_root = root / "protocol"
            protocol = {
                "schema_version": "pathfinder.fresh-multiq-selection/v3",
                "official_csv_sha256": "a" * 64,
                "excluded_object_ids": ["old"],
            }
            package(protocol_root, "selection-protocol.json", protocol)
            rows = [{"question_id": f"{video}-{stratum}",
                     "object_id": video, "stratum": stratum,
                     "question": "public", "answer_options": []}
                    for video, strata in (("A", ("causal", "temporal")),
                                         ("B", ("causal", "descriptive")),
                                         ("C", ("temporal", "descriptive")))
                    for stratum in strata]
            report = {
                "protocol_sha256": hashlib.sha256(
                    canonical(protocol)
                ).hexdigest(),
                "official_csv_sha256": "a" * 64,
                "selection_uses_answers": False,
                "label_values_included": False,
                "credentials_recorded": False,
                "selected_object_ids": ["A", "B", "C"],
                "tasks": rows,
            }
            self.assertEqual(freeze_selection(
                protocol_dir=protocol_root,
                selection_bytes=canonical(report),
                output_dir=root / "accepted",
            )["question_count"], 6)
            report["tasks"][0]["answer"] = "A"
            with self.assertRaisesRegex(ValueError, "non-public"):
                freeze_selection(
                    protocol_dir=protocol_root,
                    selection_bytes=canonical(report),
                    output_dir=root / "rejected-answer",
                )
            del report["tasks"][0]["answer"]
            report["selected_object_ids"][0] = "old"
            with self.assertRaisesRegex(ValueError, "differs"):
                freeze_selection(
                    protocol_dir=protocol_root,
                    selection_bytes=canonical(report),
                    output_dir=root / "rejected-exposure",
                )

    def test_freezer_unions_previous_public_exposures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prior = root / "prior"
            prior.mkdir()
            documents = {
                "selection-protocol.json": {
                    "schema_version": "pathfinder.fresh-multiq-selection/v2",
                    "excluded_object_ids": ["old"],
                    "max_direct_video_bytes": 7_000_000,
                    "official_csv_sha256": "a" * 64,
                },
                "exposure-inventory.json": {"object_ids": ["old"]},
            }
            for name, doc in documents.items():
                (prior / name).write_bytes(canonical(doc) + b"\n")
            (prior / "SHA256SUMS").write_bytes(b"".join(
                f"{hashlib.sha256((prior / name).read_bytes()).hexdigest()}  "
                f"{name}\n".encode() for name in sorted(documents)
            ))
            for name, object_id in (("s1", "seen-a"), ("s2", "seen-b")):
                package(root / name, "public-selection.json", {
                    "selected_object_ids": [object_id],
                    "selection_uses_answers": False,
                    "label_values_included": False,
                })
            result = freeze_protocol(
                prior_protocol_dir=prior,
                public_selection_dirs=[root / "s1", root / "s2"],
                output_dir=root / "frozen",
            )
            self.assertEqual(result["excluded_video_count"], 3)
            protocol = json.loads((root / "frozen" /
                                   "selection-protocol.json").read_bytes())
            self.assertEqual(protocol["excluded_object_ids"],
                             ["old", "seen-a", "seen-b"])
            self.assertEqual(protocol["questions_per_video"], 2)


if __name__ == "__main__":
    unittest.main()
