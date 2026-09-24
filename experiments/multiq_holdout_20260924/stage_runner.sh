#!/usr/bin/env bash
set -euo pipefail
stage=/home/pathfinder/h48-runner-20260924-v1
test ! -e "$stage"
test "$(sha256sum /tmp/a22e5c7-h48.tar | cut -d' ' -f1)" = 7868f531ff485d2fadc58243d61c84d12b8b52f4990c3c00bb732fd3e51ed204
sudo -n install -d -m 0700 -o 10001 -g 10001 "$stage/source" "$stage/output"
sudo -n tar -xf /tmp/a22e5c7-h48.tar -C "$stage/source"
sudo -n chown -R 10001:10001 "$stage/source"
sudo -n docker exec -i h48n7-pathfinder-full-flow-n7-execution-compute-1 python - < /tmp/preflight_h48.py
sudo -n python3 -I /tmp/launch_batch_n7.py preflight
