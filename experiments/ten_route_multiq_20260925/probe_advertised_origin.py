"""Print only the non-secret advertised Data Agent origin for one node."""

import os
import sys


node = sys.argv[1]
if node not in {"N3", "N4"}:
    raise SystemExit("invalid node")
value = os.environ.get(f"PATHFINDER_{node}_PUBLIC_BASE_URL")
if not value or not value.startswith("http://pathfinder-full-flow-"):
    raise SystemExit("advertised origin absent or unexpected")
print(value)
