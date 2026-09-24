#!/usr/bin/env bash
set -euo pipefail

source=/home/pathfinder/relational-dev-question-only-20260924-v1/observations
target=/tmp/relational-dev-question-only-public.tar
test ! -e "$target"
sudo -n test ! -e "$source/failure-00.json"
for ordinal in 00 01 02 03 04 05; do
  sudo -n test -f "$source/observation-$ordinal.json"
  sudo -n test ! -e "$source/failure-$ordinal.json"
done
sudo -n tar -cf "$target" -C "$source" \
  start.json \
  observation-00.json observation-01.json observation-02.json \
  observation-03.json observation-04.json observation-05.json
sudo -n chmod 0644 "$target"
sha256sum "$target"
