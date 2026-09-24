#!/usr/bin/env bash
set -euo pipefail
stage=/home/pathfinder/h48-paid-20260924-v1
test ! -e "$stage"
test "$(sha256sum /tmp/a00bb13-h48.tar | cut -d' ' -f1)" = 799c31940f5ba29a19676d1a765d75e0067230979f21c2d163f748c7a9155e61
test "$(sha256sum /tmp/h48-paid-input.tar | cut -d' ' -f1)" = 79d5994138a6f0cc9f880dd1b509cee2d5598e197d78535dfd167573e18b1c99
mkdir -p "$stage/source" "$stage/input"
sudo -n tar -xf /tmp/a00bb13-h48.tar -C "$stage/source"
sudo -n tar -xf /tmp/h48-paid-input.tar -C "$stage/input"
sudo -n chown -R 10001:10001 "$stage/source" "$stage/input"
sudo -n install -d -m 0700 -o 10001 -g 10001 "$stage/output"
sudo -n python3 /tmp/launch_paid_n6.py
test ! -e /tmp/h48-n6-paid-output.tar
sudo -n tar -cf /tmp/h48-n6-paid-output.tar -C "$stage" output
sudo -n chmod 0644 /tmp/h48-n6-paid-output.tar
sha256sum /tmp/h48-n6-paid-output.tar
