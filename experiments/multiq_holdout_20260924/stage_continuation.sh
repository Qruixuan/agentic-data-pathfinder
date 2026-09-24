#!/usr/bin/env bash
set -euo pipefail
stage=/home/pathfinder/h48-runner-ed3d5fd
test ! -e "$stage"
test "$(sha256sum /tmp/ed3d5fd-h48.tar | cut -d' ' -f1)" = abfe288dfe1b01b75d36287691b7538ecccd674e335505985a1b0e440169e045
sudo -n install -d -m 0700 -o 10001 -g 10001 "$stage/source"
sudo -n tar -xf /tmp/ed3d5fd-h48.tar -C "$stage/source"
sudo -n chown -R 10001:10001 "$stage/source"
sudo -n docker exec -i h48n7-pathfinder-full-flow-n7-execution-compute-1 python - < /tmp/preflight_h48.py
sudo -n python3 -I /tmp/launch_batch_n7.py preflight
