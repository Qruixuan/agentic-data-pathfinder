from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
)
from pathfinder.simulator.full_flow_n1_remote_verification import (
    N1_REMOTE_VERIFICATION_API_VERSION,
    N1_REMOTE_VERIFICATION_REQUEST_SCHEMA_VERSION,
    N1RemoteScoreEvidenceVerifier,
    N1RemoteScoreVerificationService,
    N1RemoteVerificationError,
    N1RemoteVerificationHTTPError,
    N1RemoteVerificationReplayError,
    create_n1_remote_verification_http_server,
)
from pathfinder.simulator.full_flow_route_adapters import (
    VerifiedN1HTTPScoringAdapter,
)
from pathfinder.simulator.hidden_oracle import (
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    N1HiddenOracleService,
    N1OracleHTTPClient,
    build_n1_oracle_package,
    build_n1_public_task_binding,
    build_n1_score_request,
    create_n1_oracle_http_server,
)


EVIDENCE_SECRET = b"test-only-n1-evidence-secret-with-32-bytes"
SCORING_TOKEN = "test-only-n1-scoring-token"
VERIFICATION_TOKEN = "test-only-n1-verification-token"
CORRECT_ANSWER = "Q"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


class N1RemoteScoreVerificationTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.task = build_n1_public_task_binding(
            workload_id="workload-remote-verification",
            object_id="object-remote-verification",
            task_class_id="video_qa",
            question="Which declared option is visible?",
            answer_options=[
                {"option_id": "P", "text": "First public option"},
                {"option_id": "Q", "text": "Second public option"},
                {"option_id": "R", "text": "Third public option"},
            ],
            success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        )
        source = self.root / "hidden-label-source.json"
        source.write_text(
            json.dumps(
                {
                    "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
                    "oracle_id": "remote-verification-oracle-v1",
                    "logical_node_id": "N1",
                    "labels": [
                        {
                            "object_id": self.task["object_id"],
                            "task_binding_sha256": self.task[
                                "task_binding_sha256"
                            ],
                            "success_scoring_rule": (
                                MULTIPLE_CHOICE_EXACT_SCORING_RULE
                            ),
                            "answer_option_ids": ["P", "Q", "R"],
                            "correct_answer_id": CORRECT_ANSWER,
                        }
                    ],
                    "credentials_recorded": False,
                },
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        self.package = self.root / "oracle-package"
        package = build_n1_oracle_package(source, output_dir=self.package)
        self.oracle_id = package["oracle_id"]
        self.public_task_set_sha256 = package["public_task_set_sha256"]
        self.scoring = N1HiddenOracleService(
            self.package,
            state_db=self.root / "score.sqlite3",
            evidence_secret=EVIDENCE_SECRET,
        )
        self.verification_db = self.root / "verification.sqlite3"

    def score_request(
        self,
        *,
        score_request_id: str = "remote-score-request-v1",
        prediction: str = CORRECT_ANSWER,
    ) -> dict:
        return build_n1_score_request(
            score_request_id=score_request_id,
            oracle_id=self.oracle_id,
            run_id="remote-verification-run-v1",
            trial_id=score_request_id,
            object_id=self.task["object_id"],
            task_binding_sha256=self.task["task_binding_sha256"],
            predicted_answer=prediction,
        )

    def envelope(
        self,
        request: dict,
        result: dict,
        *,
        verification_request_id: str,
    ) -> dict:
        return {
            "schema_version": N1_REMOTE_VERIFICATION_REQUEST_SCHEMA_VERSION,
            "verification_request_id": verification_request_id,
            "oracle_id": self.oracle_id,
            "public_task_set_sha256": self.public_task_set_sha256,
            "score_request_sha256": _sha(request),
            "score_result_sha256": _sha(result),
            "score_request": request,
            "score_result": result,
            "credentials_recorded": False,
        }

    def _server(self):
        server = create_n1_remote_verification_http_server(
            self.package,
            state_db=self.verification_db,
            bearer_token=VERIFICATION_TOKEN,
            evidence_secret=EVIDENCE_SECRET,
            host="127.0.0.1",
            port=0,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def _client(self, server, *, nonce: str = "a" * 64):
        return N1RemoteScoreEvidenceVerifier(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            expected_oracle_id=self.oracle_id,
            expected_public_task_set_sha256=self.public_task_set_sha256,
            bearer_token=VERIFICATION_TOKEN,
            verification_request_id_factory=lambda: nonce,
        )

    def test_remote_verifier_returns_content_bound_non_secret_attestation(
        self,
    ) -> None:
        request = self.score_request()
        result = self.scoring.score(request)
        server = self._server()
        verifier = self._client(server)

        attestation = dict(verifier.verify(request=request, result=result))

        self.assertEqual("VERIFIED", attestation["status"])
        self.assertEqual("N1", attestation["node_id"])
        self.assertEqual(_sha(request), attestation["score_request_sha256"])
        self.assertEqual(_sha(result), attestation["score_result_sha256"])
        self.assertEqual(
            result["result_content_sha256"],
            attestation["result_content_sha256"],
        )
        self.assertFalse(attestation["hidden_answer_returned"])
        serialized = _canonical(attestation).decode("utf-8")
        self.assertNotIn("correct_answer_id", serialized)
        self.assertNotIn(CORRECT_ANSWER, serialized)
        self.assertNotIn(EVIDENCE_SECRET.decode("utf-8"), serialized)
        self.assertNotIn(VERIFICATION_TOKEN, serialized)
        self.assertNotIn(VERIFICATION_TOKEN, repr(verifier))
        self.assertFalse(hasattr(verifier, "package_dir"))
        self.assertFalse(hasattr(verifier, "evidence_secret"))

        connection = sqlite3.connect(self.verification_db)
        try:
            durable_rows = json.dumps(
                connection.execute(
                    "SELECT * FROM consumed_challenges"
                ).fetchall()
            )
        finally:
            connection.close()
        self.assertNotIn(CORRECT_ANSWER, durable_rows)
        self.assertNotIn(EVIDENCE_SECRET.decode("utf-8"), durable_rows)
        self.assertNotIn(VERIFICATION_TOKEN, durable_rows)

    def test_hmac_and_result_tampering_is_refused_without_detail_leakage(
        self,
    ) -> None:
        request = self.score_request()
        result = self.scoring.score(request)
        server = self._server()
        tampered_results: list[dict] = []
        changed_score = dict(result)
        changed_score["correct"] = False
        changed_score["score"] = 0.0
        tampered_results.append(changed_score)
        changed_hmac = dict(result)
        changed_hmac["score_evidence_hmac_sha256"] = "0" * 64
        tampered_results.append(changed_hmac)
        for index, tampered in enumerate(tampered_results):
            core = dict(tampered)
            del core["result_content_sha256"]
            tampered["result_content_sha256"] = _sha(core)
            with self.subTest(index=index):
                verifier = self._client(
                    server,
                    nonce=f"{index + 1:x}" * 64,
                )
                with self.assertRaisesRegex(
                    N1RemoteVerificationHTTPError,
                    "HTTP 400",
                ):
                    verifier.verify(request=request, result=tampered)

        challenge = "b" * 64
        body = _canonical(
            self.envelope(
                request,
                tampered_results[0],
                verification_request_id=challenge,
            )
        )
        url = (
            f"http://127.0.0.1:{server.server_address[1]}"
            "/v1/oracle/verify-score"
        )
        with self.assertRaises(HTTPError) as caught:
            urlopen(
                Request(
                    url,
                    data=body,
                    headers={
                        "Authorization": f"Bearer {VERIFICATION_TOKEN}",
                        "Content-Type": "application/json",
                        "Idempotency-Key": challenge,
                        "X-Pathfinder-Oracle-Verification-Version": (
                            N1_REMOTE_VERIFICATION_API_VERSION
                        ),
                    },
                    method="POST",
                ),
                timeout=5,
            )
        error_body = caught.exception.read().decode("utf-8")
        self.assertEqual(400, caught.exception.code)
        self.assertIn("score evidence verification failed", error_body)
        self.assertNotIn("correctness differs", error_body)
        self.assertNotIn(CORRECT_ANSWER, error_body)
        self.assertNotIn(EVIDENCE_SECRET.decode("utf-8"), error_body)

    def test_missing_auth_is_rejected_and_request_is_not_logged(self) -> None:
        request = self.score_request()
        result = self.scoring.score(request)
        server = self._server()
        challenge = "c" * 64
        url = (
            f"http://127.0.0.1:{server.server_address[1]}"
            "/v1/oracle/verify-score"
        )
        captured = io.StringIO()
        with contextlib.redirect_stderr(captured):
            with self.assertRaises(HTTPError) as caught:
                urlopen(
                    Request(
                        url,
                        data=_canonical(
                            self.envelope(
                                request,
                                result,
                                verification_request_id=challenge,
                            )
                        ),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    ),
                    timeout=5,
                )
        error_body = caught.exception.read().decode("utf-8")
        self.assertEqual(401, caught.exception.code)
        self.assertEqual("", captured.getvalue())
        self.assertNotIn(CORRECT_ANSWER, error_body)
        self.assertNotIn(EVIDENCE_SECRET.decode("utf-8"), error_body)
        self.assertNotIn(VERIFICATION_TOKEN, error_body)

    def test_verification_challenge_replay_is_durably_rejected(self) -> None:
        request = self.score_request()
        result = self.scoring.score(request)
        challenge = "d" * 64
        envelope = self.envelope(
            request,
            result,
            verification_request_id=challenge,
        )
        first = N1RemoteScoreVerificationService(
            self.package,
            state_db=self.verification_db,
            evidence_secret=EVIDENCE_SECRET,
        )
        self.assertEqual("VERIFIED", first.verify(envelope)["status"])

        restarted = N1RemoteScoreVerificationService(
            self.package,
            state_db=self.verification_db,
            evidence_secret=EVIDENCE_SECRET,
        )
        with self.assertRaisesRegex(
            N1RemoteVerificationReplayError,
            "already consumed",
        ):
            restarted.verify(envelope)

    def test_client_rejects_a_replayed_attestation_for_a_new_challenge(
        self,
    ) -> None:
        request = self.score_request()
        result = self.scoring.score(request)
        server = self._server()
        first_client = self._client(server, nonce="e" * 64)
        stale = dict(first_client.verify(request=request, result=result))

        class ReplayedResponseVerifier(N1RemoteScoreEvidenceVerifier):
            def health(inner_self):
                return first_client.health()

            def _request(inner_self, *args, **kwargs):
                del args, kwargs
                return dict(stale)

        replayed = ReplayedResponseVerifier(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            expected_oracle_id=self.oracle_id,
            expected_public_task_set_sha256=self.public_task_set_sha256,
            bearer_token=VERIFICATION_TOKEN,
            verification_request_id_factory=lambda: "f" * 64,
        )
        with self.assertRaisesRegex(
            N1RemoteVerificationError,
            "verification_request_id mismatch",
        ):
            replayed.verify(request=request, result=result)

    def test_identity_mismatch_and_plain_remote_http_fail_closed(self) -> None:
        server = self._server()
        wrong_identity = N1RemoteScoreEvidenceVerifier(
            base_url=f"http://127.0.0.1:{server.server_address[1]}",
            expected_oracle_id="different-oracle-v1",
            expected_public_task_set_sha256=self.public_task_set_sha256,
            bearer_token=VERIFICATION_TOKEN,
        )
        with self.assertRaisesRegex(
            N1RemoteVerificationError,
            "health oracle_id mismatch",
        ):
            wrong_identity.health()
        with self.assertRaisesRegex(
            N1RemoteVerificationError,
            "plain HTTP",
        ):
            N1RemoteScoreEvidenceVerifier(
                base_url="http://n1.example.test:8081",
                expected_oracle_id=self.oracle_id,
                expected_public_task_set_sha256=self.public_task_set_sha256,
                bearer_token=VERIFICATION_TOKEN,
            )

    def test_existing_verified_http_scorer_accepts_remote_verifier(self) -> None:
        scoring_server = create_n1_oracle_http_server(
            self.package,
            state_db=self.root / "http-score.sqlite3",
            bearer_token=SCORING_TOKEN,
            evidence_secret=EVIDENCE_SECRET,
            host="127.0.0.1",
            port=0,
        )
        scoring_thread = threading.Thread(
            target=scoring_server.serve_forever,
            daemon=True,
        )
        scoring_thread.start()
        self.addCleanup(scoring_thread.join, 5)
        self.addCleanup(scoring_server.server_close)
        self.addCleanup(scoring_server.shutdown)
        verification_server = self._server()
        score_client = N1OracleHTTPClient(
            base_url=(
                f"http://127.0.0.1:{scoring_server.server_address[1]}"
            ),
            expected_oracle_id=self.oracle_id,
            expected_public_task_set_sha256=self.public_task_set_sha256,
            bearer_token=SCORING_TOKEN,
        )
        remote_verifier = self._client(
            verification_server,
            nonce="1" * 64,
        )
        scorer = VerifiedN1HTTPScoringAdapter(
            client=score_client,
            verifier=remote_verifier,
        )

        authenticated = scorer.score_once_and_verify(
            self.score_request(score_request_id="integrated-remote-score-v1")
        )

        self.assertTrue(authenticated.authentication_verified)
        self.assertTrue(authenticated.result["correct"])
        self.assertRegex(authenticated.verification_sha256, r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()
