"""Read-only health checks through the deployed route's configured origins."""

from __future__ import annotations

import json
import os
from urllib.request import urlopen


dependencies = (
    ("PATHFINDER_N1_ORACLE_BASE_URL", "N1"),
    ("PATHFINDER_N1_VERIFICATION_BASE_URL", "N1"),
    ("PATHFINDER_N2_INDEX_BASE_URL", "N2"),
    ("PATHFINDER_N3_DATA_AGENT_BASE_URL", "N3"),
    ("PATHFINDER_N4_DATA_AGENT_BASE_URL", "N4"),
    ("PATHFINDER_N6_SEMANTIC_BASE_URL", "N6"),
    ("PATHFINDER_N7_INDEX_BASE_URL", "N7"),
    ("PATHFINDER_N7_CACHE_BASE_URL", "N7"),
    ("PATHFINDER_N8_INDEX_BASE_URL", "N8"),
    ("PATHFINDER_N8_CACHE_BASE_URL", "N8"),
    ("PATHFINDER_N7_NODE_HEALTH_BASE_URL", "N7"),
    ("PATHFINDER_N8_NODE_HEALTH_BASE_URL", "N8"),
)
for name, expected in dependencies:
    origin = os.environ[name].rstrip("/")
    with urlopen(origin + "/healthz", timeout=5) as response:
        payload = json.load(response)
        if response.status != 200 or payload.get("node_id") != expected:
            raise RuntimeError(f"{name} has wrong health identity")
    print(f"{name}: 200 {expected}")
print("12/12_DEPENDENCY_HEALTH_VERIFIED")
