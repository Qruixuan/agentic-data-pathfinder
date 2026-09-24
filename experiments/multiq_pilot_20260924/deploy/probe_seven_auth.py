"""Probe auth boundary only; never request an artifact, score, or model call."""

from __future__ import annotations

import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def status(url: str, token: str) -> int:
    request = Request(
        url,
        data=b"{}",
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            return response.status
    except HTTPError as error:
        error.read(4096)
        return error.code


for name, url, key in (
    ("N1", "http://10.70.0.11:18891/v1/oracle/score",
     "PATHFINDER_N1_ORACLE_TOKEN"),
    ("N1-verifier", "http://10.70.0.11:18991/v1/oracle/verify-score",
     "PATHFINDER_N1_VERIFICATION_TOKEN"),
    ("N3", "http://10.70.0.13:18893/v1/access",
     "PATHFINDER_N3_DATA_AGENT_TOKEN"),
):
    token = os.environ.get(key)
    if not token:
        raise RuntimeError(f"{name} credential is absent")
    valid, invalid = status(url, token), status(url, "invalid-probe-token")
    if (valid, invalid) != (400, 401):
        raise RuntimeError(f"{name} auth boundary: {valid}/{invalid}")
    print(f"{name}: valid-token/bad-body=400, invalid-token=401")
