"""Durable, credential-free transport for frozen public query embeddings.

The provider key remains in the N6 container. Each raw response is persisted
before the package builder receives it, so a later verifier failure cannot
turn an already-paid embedding call into an unrecorded duplicate.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request


N6_HOST = "pathfinder@10.70.0.16"
ROOT_HOST = "pathfinder@94.237.65.184"
N6_CONTAINER = (
    "pathfinder-multiq-d328726-n6-"
    "pathfinder-full-flow-n6-semantic-inference-1"
)
REMOTE_PROGRAM = """
import os
import sys
import urllib.request

body = sys.stdin.buffer.read()
base = os.environ["PATHFINDER_SEMANTIC_LLM_BASE_URL"].rstrip("/")
key = os.environ["PATHFINDER_SEMANTIC_LLM_API_KEY"]
request = urllib.request.Request(
    base + "/embeddings", data=body, method="POST",
    headers={"Authorization": "Bearer " + key,
             "Content-Type": "application/json",
             "Accept": "application/json"},
)
with urllib.request.urlopen(request, timeout=180) as response:
    sys.stdout.buffer.write(response.read())
"""
REMOTE_PREFLIGHT = """
import os
for name in ("PATHFINDER_SEMANTIC_LLM_BASE_URL",
             "PATHFINDER_SEMANTIC_LLM_API_KEY"):
    assert bool(os.environ.get(name)), name + " is absent"
print("N6_EMBEDDING_CONFIG_PRESENT")
"""


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")


def _append_attempt(path: Path, value: dict) -> None:
    entry = {"at_utc": datetime.now(timezone.utc).isoformat(), **value}
    with path.open("ab") as handle:
        handle.write(_json_bytes(entry))
        handle.flush()
        os.fsync(handle.fileno())


class DurableN6EmbeddingTransport:
    def __init__(self, cache_dir: str | Path) -> None:
        self.cache_dir = Path(cache_dir).resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def __call__(self, request: Request, timeout: float) -> bytes:
        body = request.data
        if not isinstance(body, bytes) or not request.full_url.endswith(
            "/embeddings"
        ):
            raise ValueError("only a bounded embedding request is allowed")
        payload = json.loads(body)
        if (
            set(payload) != {"model", "input", "dimensions",
                             "encoding_format"}
            or payload["model"] != "text-embedding-v4"
            or payload["dimensions"] != 1024
            or payload["encoding_format"] != "float"
            or not isinstance(payload["input"], list)
            or len(payload["input"]) != 1
            or not isinstance(payload["input"][0], str)
            or not payload["input"][0].strip()
        ):
            raise ValueError("request differs from frozen query model contract")
        digest = hashlib.sha256(body).hexdigest()
        response_path = self.cache_dir / f"{digest}.response.json"
        receipt_path = self.cache_dir / f"{digest}.receipt.json"
        if response_path.exists():
            raw = response_path.read_bytes()
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            if (
                receipt["request_sha256"] != digest
                or receipt["response_sha256"]
                != hashlib.sha256(raw).hexdigest()
            ):
                raise ValueError("cached provider response binding differs")
            return raw
        if receipt_path.exists():
            raise ValueError("torn provider cache: receipt without response")
        attempt_path = self.cache_dir / f"{digest}.attempts.jsonl"
        _append_attempt(attempt_path, {
            "event": "ssh_start", "request_sha256": digest,
            "possible_provider_charge": True,
        })
        command = (
            "sudo -n docker exec -i " + shlex.quote(N6_CONTAINER)
            + " python -c " + shlex.quote(REMOTE_PROGRAM)
        )
        completed = subprocess.run(
            ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=30",
             "-o", "ConnectionAttempts=1", "-J", ROOT_HOST, N6_HOST,
             command],
            input=body, capture_output=True,
            timeout=max(1, int(timeout) + 60), check=False,
        )
        _append_attempt(attempt_path, {
            "event": "ssh_exit", "request_sha256": digest,
            "ssh_exit_code": completed.returncode,
            "response_bytes_received": len(completed.stdout),
            "possible_provider_charge": completed.returncode != 0,
        })
        if completed.returncode:
            raise RuntimeError(
                "remote embedding call failed: exit="
                + str(completed.returncode)
            )
        raw = completed.stdout
        parsed = json.loads(raw)
        if not isinstance(parsed.get("data"), list) or not isinstance(
            parsed.get("usage"), dict
        ):
            raise ValueError("provider response lacks vectors or usage")
        receipt = {
            "schema_version": "pathfinder.durable-query-embedding/v1",
            "request_sha256": digest,
            "response_sha256": hashlib.sha256(raw).hexdigest(),
            "model_id": "text-embedding-v4",
            "usage": parsed["usage"],
            "credentials_recorded": False,
        }
        with response_path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        with receipt_path.open("xb") as handle:
            handle.write(_json_bytes(receipt))
            handle.flush()
            os.fsync(handle.fileno())
        return raw


def remote_configuration_preflight() -> str:
    command = (
        "sudo -n docker exec " + shlex.quote(N6_CONTAINER)
        + " python -c " + shlex.quote(REMOTE_PREFLIGHT)
    )
    completed = subprocess.run(
        ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=30",
         "-o", "ConnectionAttempts=1", "-J", ROOT_HOST, N6_HOST,
         command],
        capture_output=True, timeout=45, check=False,
    )
    if completed.returncode or completed.stdout.strip() != (
        b"N6_EMBEDDING_CONFIG_PRESENT"
    ):
        raise RuntimeError("N6 embedding configuration preflight failed")
    return "N6_EMBEDDING_CONFIG_PRESENT"
