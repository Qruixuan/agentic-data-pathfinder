from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
)
from pathfinder.simulator.hidden_oracle import (
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    N1HiddenOracleService,
    N1OracleError,
    N1OracleHTTPClient,
    N1OracleHTTPError,
    N1OracleIdempotencyConflict,
    assert_hidden_oracle_fields_absent,
    build_n1_hidden_label_record,
    build_n1_oracle_package,
    build_n1_public_task_binding,
    build_n1_score_request,
    create_n1_oracle_http_server,
    verify_n1_oracle_package,
    verify_n1_score_result,
)


SECRET = b"test-only-oracle-evidence-secret-32-bytes-minimum"
TOKEN = "test-only-n1-bearer"


class N1HiddenOracleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tasks = [
            build_n1_public_task_binding(
                workload_id="workload-alpha",
                object_id="object-alpha",
                task_class_id="video_qa",
                question="Which action is shown?",
                answer_options=[
                    {"option_id": "A", "text": "A cyclist rides"},
                    {"option_id": "B", "text": "Musicians perform"},
                    {"option_id": "C", "text": "An animal runs"},
                ],
                success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            ),
            build_n1_public_task_binding(
                workload_id="workload-beta",
                object_id="object-beta",
                task_class_id="video_qa",
                question="Which option matches the scene?",
                answer_options=[
                    {"option_id": "A", "text": "Cooking"},
                    {"option_id": "B", "text": "Driving"},
                    {"option_id": "C", "text": "Swimming"},
                ],
                success_scoring_rule=(
                    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                ),
            ),
        ]
        self.label_source = self.root / "hidden-label-source.json"
        self.label_value = {
            "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
            "oracle_id": "nextqa-hidden-oracle-v1",
            "logical_node_id": "N1",
            "labels": [
                {
                    "object_id": "object-alpha",
                    "task_binding_sha256": self.tasks[0][
                        "task_binding_sha256"
                    ],
                    "success_scoring_rule": (
                        MULTIPLE_CHOICE_EXACT_SCORING_RULE
                    ),
                    "answer_option_ids": ["A", "B", "C"],
                    "correct_answer_id": "B",
                },
                {
                    "object_id": "object-beta",
                    "task_binding_sha256": self.tasks[1][
                        "task_binding_sha256"
                    ],
                    "success_scoring_rule": (
                        MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                    ),
                    "answer_option_ids": ["A", "B", "C"],
                    "correct_answer_id": "C",
                },
            ],
            "credentials_recorded": False,
        }
        self._write_source()
        self.package = self.root / "oracle-package"
        build_n1_oracle_package(self.label_source, output_dir=self.package)
        self.database = self.root / "oracle-state.sqlite3"
        self.service = N1HiddenOracleService(
            self.package,
            state_db=self.database,
            evidence_secret=SECRET,
        )

    def _write_source(self) -> None:
        self.label_source.write_text(
            json.dumps(
                self.label_value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    def request(
        self,
        *,
        request_id: str = "score-alpha-001",
        run_id: str = "oracle-test-run",
        trial_id: str | None = None,
        task_index: int = 0,
        prediction: str = "B",
    ) -> dict:
        task = self.tasks[task_index]
        return build_n1_score_request(
            score_request_id=request_id,
            oracle_id="nextqa-hidden-oracle-v1",
            run_id=run_id,
            trial_id=request_id if trial_id is None else trial_id,
            object_id=task["object_id"],
            task_binding_sha256=task["task_binding_sha256"],
            predicted_answer=prediction,
        )

    def test_public_task_and_score_request_never_contain_the_label(self) -> None:
        public_serialized = json.dumps(self.tasks[0], sort_keys=True)
        request_serialized = json.dumps(self.request(), sort_keys=True)
        self.assertNotIn("correct_answer", public_serialized)
        self.assertNotIn("correct_answer", request_serialized)
        self.assertNotIn("hidden", public_serialized)
        assert_hidden_oracle_fields_absent(self.tasks[0])
        assert_hidden_oracle_fields_absent(self.request())

    def test_private_label_is_derived_from_the_frozen_public_task(self) -> None:
        label = build_n1_hidden_label_record(
            self.tasks[0],
            correct_answer_id="B",
        )
        self.assertEqual(self.label_value["labels"][0], label)
        changed = copy.deepcopy(self.tasks[0])
        changed["question"] = "Changed after freezing"
        with self.assertRaisesRegex(N1OracleError, "binding mismatch"):
            build_n1_hidden_label_record(changed, correct_answer_id="B")

    def test_hidden_field_guard_rejects_nested_flowmesh_leak(self) -> None:
        with self.assertRaisesRegex(N1OracleError, "entered public payload"):
            assert_hidden_oracle_fields_absent(
                {"spec": {"tasks": [{"correct_answer_id": "B"}]}}
            )

    def test_package_is_deterministic_endpoint_free_and_safe_to_summarize(self) -> None:
        second = self.root / "oracle-package-second"
        build_n1_oracle_package(self.label_source, output_dir=second)
        for path in self.package.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        verified = verify_n1_oracle_package(self.package)
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual("N1", verified["logical_node_id"])
        self.assertFalse(verified["label_values_returned"])
        self.assertNotIn("correct_answer_id", verified)
        combined = b"".join(
            path.read_bytes() for path in sorted(self.package.iterdir())
        ).decode("utf-8")
        self.assertNotIn("http://", combined)
        self.assertNotIn("https://", combined)
        self.assertNotIn(TOKEN, combined)
        self.assertNotIn(SECRET.decode("utf-8"), combined)

    def test_correct_and_incorrect_predictions_return_scores_and_hashes(self) -> None:
        correct = self.service.score(self.request())
        wrong = self.service.score(
            self.request(request_id="score-alpha-002", prediction="A")
        )
        self.assertTrue(correct["correct"])
        self.assertEqual(1.0, correct["score"])
        self.assertFalse(wrong["correct"])
        self.assertEqual(0.0, wrong["score"])
        serialized = json.dumps([correct, wrong], sort_keys=True)
        self.assertNotIn("correct_answer_id", serialized)
        self.assertNotIn("predicted_answer", serialized)
        self.assertNotIn('"B"', serialized)
        self.assertFalse(correct["hidden_answer_returned"])

    def test_exact_and_canonical_rules_remain_distinct(self) -> None:
        exact_marker = self.service.score(
            self.request(request_id="score-exact-marker", prediction="[B]")
        )
        canonical_marker = self.service.score(
            self.request(
                request_id="score-canonical-marker",
                task_index=1,
                prediction="[C]",
            )
        )
        prose = self.service.score(
            self.request(
                request_id="score-canonical-prose",
                task_index=1,
                prediction="The answer is C",
            )
        )
        self.assertFalse(exact_marker["correct"])
        self.assertTrue(canonical_marker["correct"])
        self.assertFalse(prose["correct"])

    def test_privileged_verifier_checks_hmac_without_returning_answer(self) -> None:
        request = self.request()
        result = self.service.score(request)
        verified = verify_n1_score_result(
            package_dir=self.package,
            request=request,
            result=result,
            evidence_secret=SECRET,
        )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertTrue(verified["correct"])
        self.assertFalse(verified["hidden_answer_returned"])
        self.assertNotIn("correct_answer_id", verified)

    def test_hmac_or_result_tampering_is_rejected(self) -> None:
        request = self.request()
        result = self.service.score(request)
        result["score_evidence_hmac_sha256"] = "0" * 64
        core = dict(result)
        del core["result_content_sha256"]
        result["result_content_sha256"] = self.value_sha256(core)
        with self.assertRaisesRegex(N1OracleError, "HMAC mismatch"):
            verify_n1_score_result(
                package_dir=self.package,
                request=request,
                result=result,
                evidence_secret=SECRET,
            )

    def test_run_and_trial_binding_is_covered_by_the_evidence_hmac(self) -> None:
        request = self.request(trial_id="frozen-trial-signed")
        result = self.service.score(request)
        tampered_request = self.request(
            trial_id="different-frozen-trial",
        )
        tampered = dict(result)
        for name in ("run_id", "trial_id", "evaluation_unit_id"):
            tampered[name] = tampered_request[name]
        tampered["request_sha256"] = self.value_sha256(tampered_request)
        core = dict(tampered)
        del core["result_content_sha256"]
        tampered["result_content_sha256"] = self.value_sha256(core)
        with self.assertRaisesRegex(N1OracleError, "HMAC mismatch"):
            verify_n1_score_result(
                package_dir=self.package,
                request=tampered_request,
                result=tampered,
                evidence_secret=SECRET,
            )

    def test_durable_idempotent_replay_survives_service_restart(self) -> None:
        request = self.request()
        first = self.service.score(request)
        restarted = N1HiddenOracleService(
            self.package,
            state_db=self.database,
            evidence_secret=SECRET,
        )
        replay = restarted.score(request)
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(
            first["score_evidence_hmac_sha256"],
            replay["score_evidence_hmac_sha256"],
        )
        self.assertEqual(1, restarted.score_count())

    def test_conflicting_request_id_is_rejected_after_restart(self) -> None:
        self.service.score(self.request())
        restarted = N1HiddenOracleService(
            self.package,
            state_db=self.database,
            evidence_secret=SECRET,
        )
        with self.assertRaisesRegex(
            N1OracleIdempotencyConflict,
            "different request",
        ):
            restarted.score(self.request(prediction="A"))
        self.assertEqual(1, restarted.score_count())

    def test_different_request_id_cannot_rescore_one_evaluation_unit(self) -> None:
        first = self.request(
            request_id="score-unit-first",
            trial_id="frozen-trial-one",
            prediction="B",
        )
        second = self.request(
            request_id="score-unit-second",
            trial_id="frozen-trial-one",
            prediction="A",
        )
        self.service.score(first)
        restarted = N1HiddenOracleService(
            self.package,
            state_db=self.database,
            evidence_secret=SECRET,
        )
        with self.assertRaisesRegex(
            N1OracleIdempotencyConflict,
            "evaluation unit already consumed",
        ):
            restarted.score(second)
        self.assertEqual(1, restarted.score_count())

    def test_concurrent_predictions_consume_an_evaluation_unit_once(self) -> None:
        requests = [
            self.request(
                request_id="score-unit-concurrent-a",
                trial_id="frozen-trial-concurrent",
                prediction="A",
            ),
            self.request(
                request_id="score-unit-concurrent-b",
                trial_id="frozen-trial-concurrent",
                prediction="B",
            ),
        ]

        def attempt(request: dict) -> str:
            try:
                self.service.score(request)
            except N1OracleIdempotencyConflict:
                return "conflict"
            return "scored"

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(attempt, requests))
        self.assertEqual(["conflict", "scored"], sorted(outcomes))
        self.assertEqual(1, self.service.score_count())

    def test_conflict_precedes_lookup_for_an_unknown_replacement_task(self) -> None:
        self.service.score(self.request())
        changed = {
            **self.request(),
            "object_id": "unknown-object",
            "task_binding_sha256": "f" * 64,
        }
        with self.assertRaisesRegex(
            N1OracleIdempotencyConflict,
            "different request",
        ):
            self.service.score(changed)

    def test_concurrent_identical_requests_commit_one_durable_row(self) -> None:
        request = self.request(request_id="score-concurrent")
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(lambda _: self.service.score(request), range(8))
            )
        self.assertEqual(1, sum(not row["idempotent_replay"] for row in results))
        self.assertEqual(7, sum(row["idempotent_replay"] for row in results))
        self.assertEqual(1, self.service.score_count())

    def test_database_refuses_a_different_hidden_label_package(self) -> None:
        changed = copy.deepcopy(self.label_value)
        changed["labels"][0]["correct_answer_id"] = "A"
        other_source = self.root / "other-hidden-labels.json"
        other_source.write_text(
            json.dumps(changed, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        other_package = self.root / "other-package"
        build_n1_oracle_package(other_source, output_dir=other_package)
        with self.assertRaisesRegex(N1OracleError, "database binding differs"):
            N1HiddenOracleService(
                other_package,
                state_db=self.database,
                evidence_secret=SECRET,
            )

    def test_database_refuses_evidence_secret_rotation_without_new_state(self) -> None:
        with self.assertRaisesRegex(N1OracleError, "database binding differs"):
            N1HiddenOracleService(
                self.package,
                state_db=self.database,
                evidence_secret=b"another-test-evidence-secret-of-sufficient-length",
            )

    def test_legacy_request_and_state_database_are_explicitly_rejected(self) -> None:
        legacy_request = {
            "schema_version": "pathfinder.n1-score-request/v1alpha1",
            "score_request_id": "legacy-score",
            "oracle_id": "nextqa-hidden-oracle-v1",
            "object_id": self.tasks[0]["object_id"],
            "task_binding_sha256": self.tasks[0]["task_binding_sha256"],
            "predicted_answer": "B",
            "credentials_recorded": False,
        }
        with self.assertRaisesRegex(N1OracleError, "legacy score request"):
            self.service.score(legacy_request)

        legacy_db = self.root / "legacy-oracle.sqlite3"
        connection = sqlite3.connect(legacy_db)
        try:
            connection.executescript(
                """
                CREATE TABLE oracle_state (
                    singleton INTEGER PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    oracle_id TEXT NOT NULL,
                    hidden_labels_sha256 TEXT NOT NULL,
                    evidence_key_id TEXT NOT NULL
                );
                CREATE TABLE oracle_scores (
                    score_request_id TEXT PRIMARY KEY,
                    request_sha256 TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                INSERT INTO oracle_state VALUES (
                    1,
                    'pathfinder.n1-score-result/v1alpha1',
                    'nextqa-hidden-oracle-v1',
                    '0000000000000000000000000000000000000000000000000000000000000000',
                    '0000000000000000000000000000000000000000000000000000000000000000'
                );
                """
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(N1OracleError, "legacy oracle score database"):
            N1HiddenOracleService(
                self.package,
                state_db=legacy_db,
                evidence_secret=SECRET,
            )

    def test_package_tampering_and_invalid_label_are_rejected(self) -> None:
        hidden = self.package / "hidden-labels.json"
        value = json.loads(hidden.read_text(encoding="utf-8"))
        value["labels"][0]["correct_answer_id"] = "A"
        hidden.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(
            N1OracleError,
            "canonical|checksum mismatch",
        ):
            verify_n1_oracle_package(self.package)

        changed = copy.deepcopy(self.label_value)
        changed["labels"][0]["correct_answer_id"] = "Z"
        self.label_value = changed
        self._write_source()
        with self.assertRaisesRegex(N1OracleError, "not a declared option"):
            build_n1_oracle_package(
                self.label_source,
                output_dir=self.root / "invalid-package",
            )

    def test_database_contains_no_secret_or_hidden_answer(self) -> None:
        result = self.service.score(self.request())
        del result
        connection = sqlite3.connect(self.database)
        try:
            response = connection.execute(
                "SELECT response_json FROM oracle_scores"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertNotIn(TOKEN, response)
        self.assertNotIn(SECRET.decode("utf-8"), response)
        self.assertNotIn("correct_answer_id", response)
        self.assertNotIn("predicted_answer", response)

    @staticmethod
    def value_sha256(value: object) -> str:
        return hashlib.sha256(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def _server(self):
        server = create_n1_oracle_http_server(
            self.package,
            state_db=self.root / "http-oracle.sqlite3",
            bearer_token=TOKEN,
            evidence_secret=SECRET,
            host="127.0.0.1",
            port=0,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def test_real_loopback_client_checks_n1_and_scores_without_label(self) -> None:
        server = self._server()
        manifest = json.loads(
            (self.package / "n1-oracle-package.json").read_text(
                encoding="utf-8"
            )
        )
        client = N1OracleHTTPClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            expected_oracle_id=manifest["oracle_id"],
            expected_public_task_set_sha256=manifest[
                "public_task_set_sha256"
            ],
            bearer_token=TOKEN,
        )
        health = client.health()
        self.assertEqual("N1", health["node_id"])
        result = client.score(self.request(request_id="score-http"))
        self.assertTrue(result["correct"])
        self.assertNotIn("correct_answer_id", result)

    def test_http_rejects_missing_auth_and_protocol_header(self) -> None:
        server = self._server()
        url = f"http://127.0.0.1:{server.server_address[1]}/v1/oracle/score"
        body = json.dumps(self.request()).encode("utf-8")
        with self.assertRaises(HTTPError) as caught:
            urlopen(
                Request(
                    url,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                ),
                timeout=5,
            )
        self.assertEqual(401, caught.exception.code)
        with self.assertRaises(HTTPError) as caught:
            urlopen(
                Request(
                    url,
                    data=body,
                    headers={
                        "Authorization": f"Bearer {TOKEN}",
                        "Content-Type": "application/json",
                    },
                    method="POST",
                ),
                timeout=5,
            )
        self.assertEqual(400, caught.exception.code)

    def test_http_conflict_is_409_and_does_not_echo_predictions(self) -> None:
        server = self._server()
        manifest = json.loads(
            (self.package / "n1-oracle-package.json").read_text()
        )
        client = N1OracleHTTPClient(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            expected_oracle_id=manifest["oracle_id"],
            expected_public_task_set_sha256=manifest[
                "public_task_set_sha256"
            ],
            bearer_token=TOKEN,
        )
        client.score(
            self.request(
                request_id="score-http-conflict-first",
                trial_id="score-http-one-shot-trial",
            )
        )
        with self.assertRaisesRegex(N1OracleHTTPError, "HTTP 409"):
            client.score(
                self.request(
                    request_id="score-http-conflict-second",
                    trial_id="score-http-one-shot-trial",
                    prediction="A",
                )
            )

    def test_client_token_is_not_in_repr_and_plain_public_http_is_refused(self) -> None:
        manifest = json.loads(
            (self.package / "n1-oracle-package.json").read_text()
        )
        with self.assertRaisesRegex(N1OracleError, "plain HTTP"):
            N1OracleHTTPClient(
                base_url="http://oracle.example.test:8081",
                expected_oracle_id=manifest["oracle_id"],
                expected_public_task_set_sha256=manifest[
                    "public_task_set_sha256"
                ],
                bearer_token=TOKEN,
            )
        client = N1OracleHTTPClient(
            base_url="http://pathfinder-sim-n1-oracle:8081",
            expected_oracle_id=manifest["oracle_id"],
            expected_public_task_set_sha256=manifest[
                "public_task_set_sha256"
            ],
            bearer_token=TOKEN,
            simulator_private_http_hosts=("pathfinder-sim-n1-oracle",),
        )
        self.assertNotIn(TOKEN, repr(client))


if __name__ == "__main__":
    unittest.main()
