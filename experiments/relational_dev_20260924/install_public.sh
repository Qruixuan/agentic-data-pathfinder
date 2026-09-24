#!/usr/bin/env bash
set -euo pipefail

archive=/tmp/relational-dev-public-runtime.tar
target=/home/pathfinder/relational-dev-public-20260924-v1
test ! -e "$target"
test "$(sha256sum "$archive" | cut -d' ' -f1)" = 41e3bc5a9754dbc04beb12259e1399ba513382e5f36b75942e9c512f74836792
sudo -n install -d -m 0755 -o 10001 -g 10001 "$target"
sudo -n tar -xf "$archive" -C "$target"
sudo -n chown -R 10001:10001 "$target"
sudo -n test -f "$target/runtime/admission/interleaved-runtime-admission.json"
echo PUBLIC_RUNTIME_INSTALLED_FROM_FROZEN_ARCHIVE
