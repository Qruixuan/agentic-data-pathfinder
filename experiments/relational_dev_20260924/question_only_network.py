"""No-call N1/N6 DNS and health probe without printing endpoint values."""

import json
import os
import socket
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener


opener = build_opener(ProxyHandler({}))
for label, key in (
    ("N1", "PATHFINDER_N1_ORACLE_BASE_URL"),
    ("N6", "PATHFINDER_N6_SEMANTIC_BASE_URL"),
):
    origin = os.environ[key]
    parsed = urlsplit(origin)
    result = {"node": label, "dns": "unchecked", "health": "unchecked"}
    try:
        socket.getaddrinfo(parsed.hostname, parsed.port or 80)
        result["dns"] = "resolved"
    except Exception as exc:
        result["dns"] = type(exc).__name__
    try:
        with opener.open(origin.rstrip("/") + "/healthz", timeout=8) as response:
            payload = json.load(response)
        result["health"] = (
            "ok" if response.status == 200
            and payload.get("node_id") == label else "unexpected"
        )
    except Exception as exc:
        result["health"] = type(exc).__name__
    print(json.dumps(result), flush=True)
