#!/usr/bin/env bash
set -euo pipefail

stage=/home/pathfinder/relational-dev-paid-20260924-v1
public=/home/pathfinder/relational-dev-public-20260924-v1
test ! -e "$public"
test "$(sha256sum /tmp/relational-dev-n1-public-commitment.tar | cut -d' ' -f1)" = 267b2f0e89ae29f43f2a6b2363cbec4003dc950e08bfca3f37d867077bdf6584
sudo -n install -d -m 0755 -o 10001 -g 10001 "$public" "$public/paid"
sudo -n cp -a "$stage/input/plan" "$stage/input/build" "$public/"
for package in captions video-index query; do
  sudo -n cp -a "$stage/output/$package" "$public/paid/"
done
sudo -n tar -xf /tmp/relational-dev-n1-public-commitment.tar -C "$public"
sudo -n chown -R 10001:10001 "$public"
image=$(sudo -n docker inspect pathfinder-multiq-d328726-n6-pathfinder-full-flow-n6-semantic-inference-1 --format '{{.Image}}')

sudo -n docker run --rm --network none --read-only --user 10001:10001 \
  --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=256m,mode=1777 \
  --mount type=bind,src="$stage/source",dst=/work,readonly \
  --mount type=bind,src="$public",dst="$public" \
  --workdir /work -e PYTHONPATH=/work --entrypoint python "$image" \
  -m experiments.finalize_multiq_inputs \
  --input-dir "$public" --paid-dir "$public/paid" \
  --output-dir "$public/final" --physical-host pathfinder-n6

sudo -n docker run --rm --network none --read-only --user 10001:10001 \
  --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=256m,mode=1777 \
  --mount type=bind,src="$stage/source",dst=/work,readonly \
  --mount type=bind,src="$stage/protocol",dst=/protocol,readonly \
  --mount type=bind,src="$public",dst="$public" \
  --workdir /work -e PYTHONPATH=/work --entrypoint python "$image" \
  /work/experiments/multiq_holdout_20260924/freeze_runtime.py \
  "$public" /protocol/execution-protocol.json

test ! -e /tmp/relational-dev-public-runtime.tar
sudo -n tar -cf /tmp/relational-dev-public-runtime.tar -C "$public" .
sudo -n chmod 0644 /tmp/relational-dev-public-runtime.tar
sha256sum /tmp/relational-dev-public-runtime.tar
