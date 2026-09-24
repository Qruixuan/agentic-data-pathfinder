"""Focused tests for the public ATP-Hard cohort selector."""

import json
import unittest

from experiments.nextqa_atphard_cohort import public_hard_rows, select


def row(video: str, qid: str, kind: str) -> dict[str, str]:
    result = {
        "video": video, "qid": qid, "type": kind,
        "question": f"What happened after event {qid}?",
        "answer": "2",
    }
    result.update({f"a{i}": f"option {i}" for i in range(5)})
    return result


class PublicHardRowsTests(unittest.TestCase):
    def test_public_fields_are_bound_without_exporting_answer(self) -> None:
        original = row("123", "1", "TN")
        result = public_hard_rows([original], [dict(original)])
        self.assertEqual(result["123|1"]["question_id"], "nextqa-val-123-q1")
        self.assertNotIn('"answer"', json.dumps(result))

    def test_public_question_drift_is_rejected(self) -> None:
        original = row("123", "1", "TN")
        changed = dict(original, question="A different question")
        with self.assertRaisesRegex(ValueError, "public fields"):
            public_hard_rows([original], [changed])


class SelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rows = [
            row("111", "1", "CW"), row("111", "2", "TN"),
            row("111", "3", "TC"), row("222", "1", "CH"),
            row("222", "2", "TN"), row("222", "3", "TC"),
        ]
        self.grounding = {
            video: {"location": {qid: [[0.0, 1.0]]
                                 for qid in ("1", "2", "3")}}
            for video in ("111", "222")
        }
        self.inventory = {"objects": {
            f"nextqa-val-{video}": {"bytes": 2_000_000}
            for video in ("111", "222")
        }}
        self.config = {
            "seed": "test-seed", "min_video_bytes": 1_500_000,
            "max_video_bytes": 7_000_000,
            "development": [{"video": "111", "required_qids": ["1", "2"],
                             "question_count": 3}],
            "test_video_count": 1,
        }

    def test_video_disjoint_hard_and_grounded_selection(self) -> None:
        result = select(self.rows, self.rows, self.grounding,
                        self.inventory, set(), self.config)
        self.assertEqual(result["development_object_ids"], ["nextqa-val-111"])
        self.assertEqual(result["test_object_ids"], ["nextqa-val-222"])
        self.assertEqual(len(result["development"]), 3)
        self.assertEqual(len(result["test"]), 3)
        self.assertEqual({r["stratum"] for r in result["test"]},
                         {"causal", "temporal"})
        self.assertNotIn('"answer"', json.dumps(result))

    def test_exposed_video_is_not_tested(self) -> None:
        with self.assertRaisesRegex(ValueError, "video-disjoint"):
            select(self.rows, self.rows, self.grounding, self.inventory,
                   {"nextqa-val-222"}, self.config)

    def test_five_questions_per_video_preserve_required_strata(self) -> None:
        for video in ("111", "222"):
            self.rows.extend((row(video, "4", "CW"),
                              row(video, "5", "TN")))
            self.grounding[video]["location"].update({
                "4": [[0.0, 1.0]], "5": [[0.0, 1.0]],
            })
        config = dict(self.config, questions_per_video=5)
        config["development"] = [dict(self.config["development"][0],
                                      question_count=5)]
        result = select(self.rows, self.rows, self.grounding,
                        self.inventory, set(), config)
        self.assertEqual(len(result["development"]), 5)
        self.assertEqual(len(result["test"]), 5)
        self.assertGreaterEqual(sum(r["stratum"] == "causal"
                                    for r in result["test"]), 1)
        self.assertGreaterEqual(sum(r["stratum"] == "temporal"
                                    for r in result["test"]), 2)
        self.assertNotIn('"answer"', json.dumps(result))


if __name__ == "__main__":
    unittest.main()
