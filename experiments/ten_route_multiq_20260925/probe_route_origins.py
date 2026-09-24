"""Report only allowlisted non-secret origins from a route container."""

import json
import os


NAMES = (
    "PATHFINDER_N1_ORACLE_BASE_URL",
    "PATHFINDER_N1_VERIFICATION_BASE_URL",
    "PATHFINDER_N2_INDEX_BASE_URL",
    "PATHFINDER_N3_DATA_AGENT_BASE_URL",
    "PATHFINDER_N4_DATA_AGENT_BASE_URL",
    "PATHFINDER_N6_SEMANTIC_BASE_URL",
    "PATHFINDER_N7_CACHE_BASE_URL",
    "PATHFINDER_N8_CACHE_BASE_URL",
)
print(json.dumps({name: os.environ.get(name) for name in NAMES},
                 sort_keys=True))
