"""Credential-safe validation probes for the new isolated route dependencies.

Run with ``python -`` inside an existing route container.  This sends only an
empty invalid request body to each authenticated endpoint.  It never reads a
task, artifact, hidden label, or LLM response and prints status codes only.
"""

from __future__ import annotations

import json
import os
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener


BOUNDARIES = (
    ("n1-score", "http://10.70.0.11:19091/v1/oracle/score",
     "PATHFINDER_N1_ORACLE_TOKEN"),
    ("n1-verify", "http://10.70.0.11:19191/v1/oracle/verify-score",
     "PATHFINDER_N1_VERIFICATION_TOKEN"),
    ("n2", "http://10.70.0.12:19092/v1/index/query",
     "PATHFINDER_N2_INDEX_TOKEN"),
    ("n3", "http://10.70.0.13:19093/v1/access",
     "PATHFINDER_N3_DATA_AGENT_TOKEN"),
    ("n4", "http://10.70.0.14:19094/v1/access",
     "PATHFINDER_N4_DATA_AGENT_TOKEN"),
    ("n7-cache", "http://10.70.0.17:19287/v1/cache/artifact",
     "PATHFINDER_FULL_FLOW_CACHE_TOKEN"),
    ("n8-cache", "http://10.70.0.18:19288/v1/cache/artifact",
     "PATHFINDER_FULL_FLOW_CACHE_TOKEN"),
)


def _status(url: str, token: str, *, cache: bool) -> int:
    request = Request(
        url, data=None if cache else b"{}",
        method="GET" if cache else "POST",
        headers={"Authorization": "Bearer " + token,
                 "Content-Type": "application/json"},
    )
    try:
        with build_opener(ProxyHandler({})).open(request, timeout=5) as response:
            return response.status
    except HTTPError as exc:
        return exc.code


def main() -> None:
    rows = []
    for name, url, key in BOUNDARIES:
        token = os.environ.get(key)
        if not token:
            raise RuntimeError("credential name absent: " + key)
        cache = name.endswith("-cache")
        invalid = _status(url, "definitely-invalid-probe", cache=cache)
        valid = _status(url, token, cache=cache)
        if invalid != 401 or valid != 400:
            raise RuntimeError(
                "auth-before-validation gate failed: " + name
                + f" (invalid={invalid}, valid={valid})"
            )
        rows.append({"service": name, "invalid_status": invalid,
                     "valid_bad_body_status": valid})
    print(json.dumps({"status": "AUTH_BOUNDARIES_VERIFIED",
                      "credentials_recorded": False, "rows": rows},
                     sort_keys=True))


if __name__ == "__main__":
    main()
