"""Test only the N7 ingress HMAC boundary with an invalid empty request."""

from __future__ import annotations

import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pathfinder.simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    full_flow_request_hmac_sha256,
)


def status(signature: str) -> int:
    request = Request(
        "http://127.0.0.1:18025/v1/full-flow/execute",
        data=b"{}",
        headers={
            "Content-Type": "application/json",
            FULL_FLOW_INGRESS_SIGNATURE_HEADER: signature,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=8) as response:
            return response.status
    except HTTPError as error:
        error.read(4096)
        return error.code


secret = os.environ["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"]
valid = status(full_flow_request_hmac_sha256({}, secret))
invalid = status("0" * 64)
if (valid, invalid) != (400, 401):
    raise RuntimeError(f"ingress boundary differs: {valid}/{invalid}")
print("N7_INGRESS_AUTH_VERIFIED valid-signature/invalid-body=400 invalid=401")
