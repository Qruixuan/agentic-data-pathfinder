#!/usr/bin/env bash
set -euo pipefail

public=/home/pathfinder/relational-dev-public-20260924-v1
image=$(sudo -n docker inspect h48n7-pathfinder-full-flow-n7-execution-compute-1 --format '{{.Image}}')
sudo -n docker run --rm --network none --read-only --user 10001:10001 \
  --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m,mode=1777 \
  --mount type=bind,src="$public",dst=/public,readonly \
  --mount type=bind,src=/tmp/relational-dev-verify-public-in-image.py,dst=/verify.py,readonly \
  --entrypoint python "$image" /verify.py
