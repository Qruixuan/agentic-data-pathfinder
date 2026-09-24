#!/usr/bin/env bash
set -euo pipefail

stage=/home/pathfinder/relational-dev-runner-20260924-v1
test ! -e "$stage"
test "$(sha256sum /tmp/relational-dev-source-b02ac75.tar | cut -d' ' -f1)" = dc8d37d67539b70289f1200b844d6bfc511c56f7112b0a2ff89d8554ebe8bd75
sudo -n install -d -m 0700 -o 10001 -g 10001 "$stage/source" "$stage/output"
sudo -n tar -xf /tmp/relational-dev-source-b02ac75.tar -C "$stage/source"
sudo -n chown -R 10001:10001 "$stage/source"
sudo -n docker exec -i r24n7-pathfinder-full-flow-n7-execution-compute-1 \
  python - < /tmp/relational-dev-preflight.py
sudo -n python3 -I /tmp/relational-dev-launch-batch-n7.py check
sudo -n python3 -I /tmp/relational-dev-launch-batch-n7.py preflight
