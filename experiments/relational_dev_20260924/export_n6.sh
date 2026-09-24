#!/usr/bin/env bash
set -euo pipefail

target=/tmp/relational-dev-n6-accounting.json
test ! -e "$target"
sudo -n docker exec -i \
  pathfinder-multiq-d328726-n6-pathfinder-full-flow-n6-semantic-inference-1 \
  python - < /tmp/relational-dev-export-n6-accounting.py > "$target"
python3 -I -m json.tool "$target" > /dev/null
sha256sum "$target"
