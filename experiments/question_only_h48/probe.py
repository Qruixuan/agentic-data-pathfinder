"""Bounded question-only development diagnostic on the existing H48 cohort.

Run inside the existing N7 container. This uses N6's authenticated text
endpoint and N1's authenticated score endpoint, without FlowMesh dispatch.
Only a private recovery record contains the model answer. Public receipts
contain hashes, usage and N1's authenticated correctness bit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from pathfinder.simulator.container_node import (
    CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
    _SEMANTIC_LLM_RETRY_BACKOFF_SECONDS,
)
from pathfinder.simulator.full_flow_n6_adapters import _render_public_question
from pathfinder.simulator.hidden_oracle import (
    N1OracleHTTPClient,
    build_n1_public_task_binding,
    build_n1_score_request,
)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_new(path: Path, value: object, *, private: bool = False) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600 if private else 0o644)
    with os.fdopen(fd, "wb") as handle:
        handle.write(canonical(value) + b"\n")


def read_inputs(protocol_path: Path, questions_path: Path,
                commitment_path: Path) -> tuple[dict, list[dict], dict]:
    protocol = json.loads(protocol_path.read_bytes())
    count = protocol.get("question_count")
    cohort = protocol.get("cohort")
    if (type(count) is not int or not 1 <= count <= 24
            or not isinstance(cohort, str)
            or re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", cohort) is None
            or not isinstance(protocol.get("run_id"), str)
            or re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", protocol["run_id"])
            is None):
        raise ValueError("diagnostic size or identity is invalid")
    required = {
        "schema_version": "pathfinder.question-only-development-diagnostic/v1",
        "max_n6_requests": count, "max_n1_score_requests": count,
        "model": "qwen3.8-27b",
        "temperature": 0, "input_kind": "public-question-and-options-only",
        "prompt_profile": "question-only-choice-v1",
        "max_provider_attempts": 3 * count,
        "provider_retry_policy": "deployed-N6-semantic-endpoint",
        "allow_question_replacement": False,
        "credentials_recorded": False, "hidden_labels_read": False,
    }
    if any(protocol.get(key) != value for key, value in required.items()):
        raise ValueError("diagnostic protocol differs from frozen contract")
    if protocol.get("evaluation_role") not in {
        "development-diagnostic-on-already-exposed-questions",
        "development-diagnostic-on-public-relational-cohort",
    } or protocol.get("price_basis") != (
        "frozen-2026-09-23-singapore-usd-list-price"
    ):
        raise ValueError("diagnostic role or price basis differs")
    if len(_SEMANTIC_LLM_RETRY_BACKOFF_SECONDS) + 1 > 3:
        raise ValueError("deployed N6 retry ceiling exceeds frozen budget")
    if digest(questions_path.read_bytes()) != protocol["question_file_sha256"]:
        raise ValueError("question source digest differs")
    rows = [json.loads(line) for line in questions_path.read_text(
        encoding="utf-8").splitlines() if line]
    if len(rows) != count or len({row["question_id"] for row in rows}) != count:
        raise ValueError("public questions do not match frozen task count")
    commitment = json.loads(commitment_path.read_bytes())
    if (commitment.get("oracle_id") != cohort + "-oracle"
            or commitment.get("label_count") != count
            or commitment.get("label_values_included") is not False):
        raise ValueError("N1 public commitment differs")
    for row in rows:
        task = build_n1_public_task_binding(
            workload_id=row["question_id"], object_id=row["object_id"],
            task_class_id=row["stratum"], question=row["question"],
            answer_options=row["answer_options"],
            success_scoring_rule=(
                "multiple-choice-option-id-canonical-match-v1"
            ),
        )
        if task["task_binding_sha256"] != row["public_task_sha256"]:
            raise ValueError("public task binding differs")
    return protocol, rows, commitment


def prompt_for(row: dict) -> str:
    task = build_n1_public_task_binding(
        workload_id=row["question_id"], object_id=row["object_id"],
        task_class_id=row["stratum"], question=row["question"],
        answer_options=row["answer_options"],
        success_scoring_rule="multiple-choice-option-id-canonical-match-v1",
    )
    rendered = _render_public_question(task, 65536)
    return (
        "You are answering a controlled multiple-choice VideoQA question.\n"
        "No video, frames, captions, or digest are supplied. Base your answer "
        "only on the question and choices. If the question requires seeing "
        "the video, make your best guess.\n\n"
        + rendered
    )


def opener():
    return build_opener(ProxyHandler({}))


def get_health(origin: str) -> dict:
    with opener().open(origin.rstrip("/") + "/healthz", timeout=10) as response:
        return json.load(response)


def bad_body_probe(origin: str, path: str, token: str) -> int:
    request = Request(origin.rstrip("/") + path, data=b"{}", method="POST",
                      headers={"Authorization": "Bearer " + token,
                               "Content-Type": "application/json"})
    try:
        with opener().open(request, timeout=10) as response:
            response.read(4096)
            return response.status
    except HTTPError as exc:
        exc.read(4096)
        return exc.code


def preflight(protocol: dict, commitment: dict) -> tuple[str, str, str, str]:
    names = ("PATHFINDER_N6_SEMANTIC_BASE_URL",
             "PATHFINDER_N1_ORACLE_BASE_URL",
             "PATHFINDER_CONTAINER_NODE_TOKEN", "PATHFINDER_N1_ORACLE_TOKEN")
    values = tuple(os.environ.get(name, "") for name in names)
    if not all(values):
        raise ValueError("required existing runtime key or endpoint is absent")
    n6_origin, n1_origin, n6_token, n1_token = values
    if not (n6_origin.startswith("http://pathfinder-full-flow-")
            and n1_origin.startswith("http://pathfinder-full-flow-")):
        raise ValueError("runtime origins are not the approved service aliases")
    n6 = get_health(n6_origin)
    n1 = get_health(n1_origin)
    if (n6.get("node_id") != "N6" or n6.get("status") != "ok"
            or n6.get("semantic_quality_enabled") is not True
            or n6.get("semantic_llm_configured") is not True
            or type(n6.get("semantic_usage_journal_error_count")) is not int
            or n1.get("node_id") != "N1" or n1.get("status") != "ok"
            or n1.get("oracle_id") != commitment["oracle_id"]
            or n1.get("public_task_set_sha256")
            != commitment["public_task_set_sha256"]):
        raise ValueError("N1/N6 health or source binding differs")
    for origin, path, token in (
        (n6_origin, "/v1/semantic/chat-completions", n6_token),
        (n1_origin, "/v1/oracle/score", n1_token),
    ):
        if (bad_body_probe(origin, path, token) != 400
                or bad_body_probe(origin, path, "invalid-diagnostic-token") != 401):
            raise ValueError("N1/N6 authentication boundary differs")
    return n6_origin, n1_origin, n6_token, n1_token


def n6_complete(origin: str, token: str, request: dict) -> dict:
    body = canonical(request)
    call = Request(origin.rstrip("/") + "/v1/semantic/chat-completions",
                   data=body, method="POST", headers={
                       "Authorization": "Bearer " + token,
                       "Content-Type": "application/json"})
    with opener().open(call, timeout=210) as response:
        result = json.load(response)
    if (result.get("status") != "completed"
            or result.get("semantic_request_id")
            != request["semantic_request_id"]
            or result.get("request_sha256") != digest(body)
            or result.get("llm_called") is not True
            or result.get("idempotent_replay") is not False
            or not isinstance(result.get("final_answer"), str)):
        raise ValueError("N6 result binding or first-call status differs")
    return result


def run(protocol: dict, rows: list[dict], commitment: dict, out: Path) -> None:
    n6_origin, n1_origin, n6_token, n1_token = preflight(
        protocol, commitment)
    if out.exists():
        raise ValueError("diagnostic output already exists; no request repeated")
    out.mkdir(mode=0o700, parents=True)
    private = out / "private-recovery"
    private.mkdir(mode=0o700)
    write_new(out / "start.json", {
        "run_id": protocol["run_id"], "protocol_sha256": digest(canonical(protocol)),
        "question_file_sha256": protocol["question_file_sha256"],
        "oracle_id": commitment["oracle_id"],
        "public_task_set_sha256": commitment["public_task_set_sha256"],
        "n6_request_ceiling": len(rows), "n1_score_ceiling": len(rows),
        "provider_attempt_ceiling": protocol["max_provider_attempts"],
        "credentials_recorded": False, "hidden_labels_read": False,
    })
    client = N1OracleHTTPClient(
        base_url=n1_origin,
        expected_oracle_id=commitment["oracle_id"],
        expected_public_task_set_sha256=commitment["public_task_set_sha256"],
        bearer_token=n1_token,
        simulator_private_http_hosts=(
            n1_origin.split("/")[2].split(":")[0],
        ),
    )
    client.health()
    for ordinal, row in enumerate(rows):
        stage = "n6"
        try:
            prompt = prompt_for(row)
            request = {
                "schema_version": CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
                "execution_node_id": "N6",
                "semantic_request_id": digest(canonical({
                    "domain": "pathfinder.question-only-development/v1",
                    "run_id": protocol["run_id"],
                    "question_id": row["question_id"],
                })),
                "prompt": prompt, "prompt_sha256": digest(prompt.encode()),
                "representation_sha256": digest(b""),
            }
            result = n6_complete(n6_origin, n6_token, request)
            answer = result["final_answer"]
            write_new(private / f"prediction-{ordinal:02d}.json", {
                "question_id": row["question_id"],
                "answer": answer,
                "n6_request_sha256": result["request_sha256"],
                "n6_result_sha256": digest(canonical(result)),
            }, private=True)
            stage = "n1"
            score_request = build_n1_score_request(
                score_request_id=digest(canonical({
                    "domain": "pathfinder.question-only-score/v1",
                    "run_id": protocol["run_id"],
                    "question_id": row["question_id"],
                    "prediction_sha256": digest(answer.encode()),
                })),
                oracle_id=commitment["oracle_id"],
                run_id=protocol["run_id"], trial_id=row["question_id"],
                object_id=row["object_id"],
                task_binding_sha256=row["public_task_sha256"],
                predicted_answer=answer,
            )
            score = client.score(score_request)
            if (score.get("correct") not in (True, False)
                    or score.get("prediction_sha256") != digest(answer.encode())):
                raise ValueError("N1 score binding differs")
            usage = result.get("provider_usage")
            priced = None
            if usage is not None:
                priced = round(((usage["input_units"]
                                 - usage["cached_input_units"]) * 0.50
                                + usage["cached_input_units"] * 0.10
                                + usage["output_units"] * 3.00) / 1_000_000, 9)
            write_new(out / f"observation-{ordinal:02d}.json", {
                "ordinal": ordinal, "question_id": row["question_id"],
                "object_id": row["object_id"], "stratum": row["stratum"],
                "public_task_sha256": row["public_task_sha256"],
                "prompt_sha256": request["prompt_sha256"],
                "n6_request_sha256": result["request_sha256"],
                "n6_result_sha256": digest(canonical(result)),
                "prediction_sha256": digest(answer.encode()),
                "provider_usage": usage, "known_model_list_price_usd": priced,
                "n1_score_request_sha256": digest(canonical(score_request)),
                "n1_score_result": score, "correct": score["correct"],
                "credentials_recorded": False, "hidden_labels_read": False,
            })
            print(json.dumps({"ordinal": ordinal, "status": "SCORED"}),
                  flush=True)
        except Exception as exc:
            write_new(out / f"failure-{ordinal:02d}.json", {
                "ordinal": ordinal, "stage": stage,
                "error_class": type(exc).__name__,
                "n6_request_may_have_completed": (
                    private / f"prediction-{ordinal:02d}.json").exists(),
            })
            raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("preflight", "run"))
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--questions", type=Path, required=True)
    parser.add_argument("--commitment", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    stage = args.phase
    try:
        protocol, rows, commitment = read_inputs(
            args.protocol, args.questions, args.commitment)
        if args.phase == "preflight":
            preflight(protocol, commitment)
            print(json.dumps({"status": "QUESTION_ONLY_PREFLIGHT_OK",
                              "question_count": len(rows),
                              "llm_called": False,
                              "credentials_recorded": False}))
        else:
            if args.output_dir is None:
                raise ValueError("run requires a new output directory")
            run(protocol, rows, commitment, args.output_dir)
            print(json.dumps({"status": "QUESTION_ONLY_DEV_COMPLETE",
                              "question_count": len(rows),
                              "credentials_recorded": False}))
    except Exception as exc:
        print(json.dumps({"status": "QUESTION_ONLY_STOPPED", "stage": stage,
                          "error_class": type(exc).__name__,
                          "credentials_recorded": False}), flush=True)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
