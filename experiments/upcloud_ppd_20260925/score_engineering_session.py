"""Score a completed public PPD session without re-running its Agent.

The public task must already be bound to the mounted N1 oracle package. This
tool never opens that private package: N1 scores, then its separate verifier
authenticates the public result. No answer or credential is written to output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pathfinder.simulator.full_flow_n1_remote_verification import (
    N1RemoteScoreEvidenceVerifier,
)
from pathfinder.simulator.full_flow_route_adapters import (
    VerifiedN1HTTPScoringAdapter,
)
from pathfinder.simulator.hidden_oracle import (
    N1OracleHTTPClient,
    build_n1_public_task_binding,
    build_n1_score_request,
)

from experiments.upcloud_ppd_20260925.run_engineering_session import (
    _answer_fields,
)


@dataclass(frozen=True)
class PreparedScore:
    request: dict[str, object]
    receipt_sha256: str
    workflow_id: str
    task_id: str
    public_task_set_sha256: str


def _load_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("score input must be a JSON object")
    return value


def _public_task(path: Path) -> dict[str, object]:
    supplied = _load_json(path)
    fields = (
        "workload_id", "object_id", "task_class_id", "question",
        "answer_options", "success_scoring_rule",
    )
    if set(supplied) != {
        "schema_version", *fields, "credentials_recorded", "task_binding_sha256"
    }:
        raise ValueError("public task field set changed")
    rebuilt = build_n1_public_task_binding(
        **{name: supplied[name] for name in fields}
    )
    if supplied != rebuilt:
        raise ValueError("public task binding is not canonical")
    return rebuilt


def _prompt_matches_task(prompt: str, task: dict[str, object]) -> bool:
    lines = [line.strip() for line in prompt.splitlines() if line.strip()]
    question = task["question"]
    if lines.count(question) != 1:
        return False
    options = task["answer_options"]
    observed = []
    for line in lines:
        match = re.fullmatch(r"([A-E])\. (.+)", line)
        if match:
            observed.append({"option_id": match.group(1), "text": match.group(2)})
    return observed == options


def prepare_score(
    *, receipt_path: Path, state_db: Path, public_task_path: Path,
    oracle_id: str, public_task_set_sha256: str,
) -> PreparedScore:
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    if not isinstance(receipt, dict):
        raise ValueError("source receipt is not a JSON object")
    task = _public_task(public_task_path)
    session_id = receipt.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("source receipt has no session identity")
    if (
        receipt.get("status") != "DONE"
        or receipt.get("closure_verified") is not True
        or receipt.get("task_success_evaluated") is not False
        or receipt.get("answer_has_explicit_final_option") is not True
        or receipt.get("eligible_for_scientific_claims") is not False
    ):
        raise ValueError("source receipt is not an unscored completed closure")
    if not state_db.is_file():
        raise ValueError("Gateway state database is missing")
    connection = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        cursor = connection.execute(
            "SELECT trial_id, question, design_id, task_class_id, status, "
            "flowmesh_workflow_id, flowmesh_task_id, final_answer, object_id "
            "FROM gateway_sessions WHERE session_id = ?", (session_id,),
        )
        try:
            row = cursor.fetchone()
        finally:
            cursor.close()
        cursor = connection.execute(
            "SELECT COUNT(*) FROM gateway_access_events "
            "WHERE session_id = ? AND accepted = 1", (session_id,),
        )
        try:
            access_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    finally:
        connection.close()
    if row is None or row["status"] != "DONE" or access_count < 1:
        raise ValueError("Gateway session is not an accessed completed run")
    if (
        row["flowmesh_workflow_id"] != receipt.get("workflow_id")
        or row["flowmesh_task_id"] != receipt.get("task_id")
        or row["design_id"] != receipt.get("design_id")
        or row["object_id"] != task["object_id"]
        or row["task_class_id"] != task["task_class_id"]
        or not _prompt_matches_task(row["question"], task)
    ):
        raise ValueError("public task differs from the completed Gateway session")
    parsed = _answer_fields(row["final_answer"] or "")
    prediction = parsed["extracted_option_id"]
    if (
        prediction is None
        or parsed["answer_format"] != receipt.get("answer_format")
        or prediction != receipt.get("extracted_option_id")
    ):
        raise ValueError("source receipt option differs from Gateway answer")
    request_id = "ppd-n1-" + hashlib.sha256(
        (oracle_id + "|" + session_id + "|" + row["trial_id"] + "|"
         + task["task_binding_sha256"]).encode("utf-8")
    ).hexdigest()
    request = build_n1_score_request(
        score_request_id=request_id,
        oracle_id=oracle_id,
        run_id=session_id,
        trial_id=row["trial_id"],
        object_id=task["object_id"],
        task_binding_sha256=task["task_binding_sha256"],
        predicted_answer=prediction,
    )
    return PreparedScore(
        request=request,
        receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        workflow_id=row["flowmesh_workflow_id"],
        task_id=row["flowmesh_task_id"],
        public_task_set_sha256=public_task_set_sha256,
    )


def score_once(
    prepared: PreparedScore, scorer: VerifiedN1HTTPScoringAdapter,
) -> dict[str, object]:
    authenticated = scorer.score_once_and_verify(prepared.request)
    result = authenticated.result
    if (
        not authenticated.authentication_verified
        or result["hidden_answer_returned"] is not False
    ):
        raise ValueError("N1 score authenticity or confidentiality failed")
    if result["public_task_set_sha256"] != prepared.public_task_set_sha256:
        raise ValueError("N1 public task set changed")
    return {
        "status": "VERIFIED_N1_SCORED_PPD_SESSION",
        "source_receipt_sha256": prepared.receipt_sha256,
        "workflow_id": prepared.workflow_id,
        "task_id": prepared.task_id,
        "run_id": prepared.request["run_id"],
        "trial_id": prepared.request["trial_id"],
        "oracle_id": prepared.request["oracle_id"],
        "public_task_set_sha256": prepared.public_task_set_sha256,
        "task_binding_sha256": prepared.request["task_binding_sha256"],
        "score_request_id": prepared.request["score_request_id"],
        "result_content_sha256": result["result_content_sha256"],
        "score_evidence_hmac_sha256": result["score_evidence_hmac_sha256"],
        "verification_sha256": authenticated.verification_sha256,
        "scoring_rule": result["success_scoring_rule"],
        "task_success_evaluated": True,
        "task_success": result["correct"],
        "score": result["score"],
        "hidden_answer_returned": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("preflight", "score"))
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--gateway-state-db", type=Path, required=True)
    parser.add_argument("--public-task", type=Path, required=True)
    parser.add_argument("--oracle-id", required=True)
    parser.add_argument("--public-task-set-sha256", required=True)
    parser.add_argument("--n1-oracle-url", required=True)
    parser.add_argument("--n1-verifier-url", required=True)
    parser.add_argument("--private-http-host", action="append", default=[])
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    try:
        if args.mode == "score" and args.output_dir is None:
            raise ValueError("score requires a new output directory")
        if args.output_dir is not None and args.output_dir.exists():
            raise ValueError("score output directory already exists")
        prepared = prepare_score(
            receipt_path=args.receipt,
            state_db=args.gateway_state_db,
            public_task_path=args.public_task,
            oracle_id=args.oracle_id,
            public_task_set_sha256=args.public_task_set_sha256,
        )
        token = os.environ.get("PATHFINDER_N1_ORACLE_TOKEN")
        verifier_token = os.environ.get("PATHFINDER_N1_VERIFICATION_TOKEN")
        if not token or not verifier_token:
            raise ValueError("N1 credential configuration is incomplete")
        hosts = tuple(args.private_http_host)
        client = N1OracleHTTPClient(
            base_url=args.n1_oracle_url,
            expected_oracle_id=args.oracle_id,
            expected_public_task_set_sha256=args.public_task_set_sha256,
            bearer_token=token,
            simulator_private_http_hosts=hosts,
        )
        verifier = N1RemoteScoreEvidenceVerifier(
            base_url=args.n1_verifier_url,
            expected_oracle_id=args.oracle_id,
            expected_public_task_set_sha256=args.public_task_set_sha256,
            bearer_token=verifier_token,
            simulator_private_http_hosts=hosts,
        )
        client.health()
        verifier.health()
        if args.mode == "preflight":
            print(json.dumps({
                "status": "N1_ENDPOINT_READY_TASK_MEMBERSHIP_UNPROVEN",
                "score_submitted": False,
                "task_binding_sha256": prepared.request["task_binding_sha256"],
                "hidden_answer_returned": False,
                "credentials_recorded": False,
            }, sort_keys=True))
            return 0
        scorer = VerifiedN1HTTPScoringAdapter(
            client=client, verifier=verifier,
        )
        result = score_once(prepared, scorer)
        target = args.output_dir
        assert target is not None
        target.parent.mkdir(parents=True, exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix=".ppd-n1-score-", dir=target.parent))
        try:
            body = (
                json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode("utf-8")
            (stage / "n1-score-receipt.json").write_bytes(body)
            (stage / "SHA256SUMS").write_bytes(
                (
                    hashlib.sha256(body).hexdigest()
                    + "  n1-score-receipt.json\n"
                ).encode("ascii")
            )
            if target.exists():
                raise ValueError("score output directory already exists")
            stage.rename(target)
        finally:
            if stage.exists():
                for child in stage.iterdir():
                    child.unlink()
                stage.rmdir()
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({
            "status": "BLOCKED_OR_FAILED",
            "error_class": type(exc).__name__,
        }, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
