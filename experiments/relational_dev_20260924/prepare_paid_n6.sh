#!/usr/bin/env bash
set -euo pipefail

stage=/home/pathfinder/relational-dev-paid-20260924-v1
source_archive=/tmp/relational-dev-source-b02ac75.tar
input_archive=/tmp/relational-dev-paid-input-20260924.tar
test ! -e "$stage"
test "$(sha256sum "$source_archive" | cut -d' ' -f1)" = dc8d37d67539b70289f1200b844d6bfc511c56f7112b0a2ff89d8554ebe8bd75
test "$(sha256sum "$input_archive" | cut -d' ' -f1)" = 7a2aab51793b5254488d86c5fcc13103b3a556b6d5f44ca10323b2e444e64dec
test "$(sha256sum /tmp/relational-dev-execution-protocol.json | cut -d' ' -f1)" = f944899d127e385de4672805115beb248651d9899c56e193fbc2acfd30c5fca8

mkdir -p "$stage/source" "$stage/input" "$stage/protocol"
tar -xf "$source_archive" -C "$stage/source"
tar -xf "$input_archive" -C "$stage/input"
cp /tmp/relational-dev-execution-protocol.json "$stage/protocol/execution-protocol.json"
sudo -n chown -R 10001:10001 "$stage/source" "$stage/input" "$stage/protocol"
sudo -n install -d -m 0700 -o 10001 -g 10001 "$stage/output"
sudo -n python3 -I /tmp/relational-dev-launch-paid-n6.py

test ! -e /tmp/relational-dev-n6-paid-output.tar
sudo -n tar -cf /tmp/relational-dev-n6-paid-output.tar -C "$stage" output
sudo -n chmod 0644 /tmp/relational-dev-n6-paid-output.tar
sha256sum /tmp/relational-dev-n6-paid-output.tar
