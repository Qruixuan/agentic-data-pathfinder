import csv
import hashlib
import tempfile
import unittest
from pathlib import Path

from experiments.fresh_multiq_cohort import canonical, select


class FreshCohortTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.csv = Path(self.temp.name) / "official.csv"
        self.rows = [
            {"video": str(1000000000 + v), "qid": str(i), "type": kind,
             "question": f"public question {v} {i}", "answer": "SECRET",
             **{f"a{n}": f"choice {n}" for n in range(5)}}
            for v in range(6) for i, kind in enumerate(("CW", "TN", "DC"))
        ]
        self.write()
        self.protocol = {
            "schema_version": "pathfinder.fresh-multiq-selection/v1",
            "seed": "frozen-seed", "object_count": 4,
            "strata": ["causal", "temporal", "descriptive"],
            "official_csv_sha256": hashlib.sha256(self.csv.read_bytes()).hexdigest(),
            "excluded_object_ids": ["nextqa-val-1000000000"],
            "selection_rule": "sha256-seeded-public-fields-v1",
        }

    def write(self):
        with self.csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(self.rows[0]))
            writer.writeheader()
            writer.writerows(self.rows)

    def test_disjoint_complete_deterministic(self):
        a = select(self.csv, self.protocol)
        self.assertEqual(a, select(self.csv, self.protocol))
        self.assertEqual(len(a["tasks"]), 12)
        self.assertNotIn("nextqa-val-1000000000", a["selected_object_ids"])
        self.assertNotIn("SECRET", str(a))
        for oid in a["selected_object_ids"]:
            self.assertEqual({q["stratum"] for q in a["tasks"]
                              if q["object_id"] == oid},
                             {"causal", "temporal", "descriptive"})

    def test_label_changes_do_not_change_selection(self):
        old = select(self.csv, self.protocol)
        for row in self.rows:
            row["answer"] = "OTHER-HIDDEN"
        self.write()
        self.protocol["official_csv_sha256"] = hashlib.sha256(
            self.csv.read_bytes()).hexdigest()
        new = select(self.csv, self.protocol)
        self.assertEqual(old["tasks"], new["tasks"])
        self.assertEqual(old["selected_object_ids"], new["selected_object_ids"])

    def test_source_drift_rejected(self):
        self.protocol["official_csv_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "digest differs"):
            select(self.csv, self.protocol)

    def test_no_silent_small_cohort(self):
        self.protocol["object_count"] = 6
        with self.assertRaisesRegex(ValueError, "not enough"):
            select(self.csv, self.protocol)

    def test_runtime_size_filter_is_outcome_blind(self):
        media = canonical({"objects": {
            "nextqa-val-" + str(1000000000 + i): {
                "bytes": 8_000_000 if i == 5 else 1_000_000,
            } for i in range(6)
        }})
        self.protocol.update({
            "schema_version": "pathfinder.fresh-multiq-selection/v2",
            "media_inventory_sha256": hashlib.sha256(media).hexdigest(),
            "max_direct_video_bytes": 7_000_000,
        })
        result = select(self.csv, self.protocol, media)
        self.assertNotIn("nextqa-val-1000000005", result["selected_object_ids"])
        self.assertEqual(result["eligible_object_count"], 4)
        with self.assertRaisesRegex(ValueError, "inventory digest"):
            select(self.csv, self.protocol, b"{}")

    def test_three_video_two_question_protocol_balances_strata(self):
        media = canonical({"objects": {
            "nextqa-val-" + str(1000000000 + i): {"bytes": 1_000_000}
            for i in range(6)
        }})
        self.protocol.update({
            "schema_version": "pathfinder.fresh-multiq-selection/v3",
            "object_count": 3, "questions_per_video": 2,
            "media_inventory_sha256": hashlib.sha256(media).hexdigest(),
            "max_direct_video_bytes": 7_000_000,
        })
        selected = select(self.csv, self.protocol, media)
        self.assertEqual(len(selected["tasks"]), 6)
        self.assertEqual(len(selected["selected_object_ids"]), 3)
        self.assertEqual(sorted(q["stratum"] for q in selected["tasks"]),
                         sorted(["causal", "temporal", "descriptive"] * 2))
        for oid in selected["selected_object_ids"]:
            self.assertEqual(sum(q["object_id"] == oid
                                 for q in selected["tasks"]), 2)
        self.assertNotIn("SECRET", str(selected))
        for row in self.rows:
            row["answer"] = "DIFFERENT-HIDDEN"
        self.write()
        self.protocol["official_csv_sha256"] = hashlib.sha256(
            self.csv.read_bytes()
        ).hexdigest()
        after = select(self.csv, self.protocol, media)
        self.assertEqual(selected["tasks"], after["tasks"])


if __name__ == "__main__":
    unittest.main()
