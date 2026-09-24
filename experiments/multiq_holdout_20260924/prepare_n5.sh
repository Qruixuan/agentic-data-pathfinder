#!/usr/bin/env bash
set -euo pipefail
stage=/home/pathfinder/h48-20260924-v1
test ! -e "$stage"
test "$(sha256sum /tmp/28b5ede-h48.tar | cut -d' ' -f1)" = a145c8f830422caac16e405d9758a55d9f0f4f0eea71a843d4d243c3fa8dec06
test "$(sha256sum /tmp/h48-inputs.tar | cut -d' ' -f1)" = a61242a42e2fdea2eb34063b50eb7b2412d5880ffbacffe49c16f4dca896497b
test "$(sha256sum /tmp/h48-tools.tar | cut -d' ' -f1)" = 32748a38a45f409e13eeca2a7c7feec0321d6eb7ab968f7ef7ed0ce0c3a6531c
mkdir -p "$stage/source" "$stage/input" "$stage/tools"
tar -xf /tmp/28b5ede-h48.tar -C "$stage/source"
tar -xf /tmp/h48-inputs.tar -C "$stage/input"
tar -xf /tmp/h48-tools.tar -C "$stage/tools"
sudo -n install -d -m 0755 -o 10001 -g 10001 "$stage/output"
image=$(sudo -n docker image inspect f83f59a52abe --format '{{.Id}}')
printf 'OFFLINE_PREPARATION_IMAGE %s\n' "$image"
sudo -n docker run --rm --network none --read-only --user 10001:10001 \
  --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=128m,mode=1777 \
  --mount type=bind,src="$stage/source",dst=/work,readonly \
  --mount type=bind,src="$stage/input",dst=/input,readonly \
  --mount type=bind,src="$stage/tools",dst=/tools,readonly \
  --mount type=bind,src="$stage/output",dst=/output \
  --workdir /work -e PYTHONPATH=/work --entrypoint python "$image" \
  /tools/experiments/multiq_prepare.py offline \
  --input-dir /input --output-dir /output/build \
  --source-commit 28b5ede8e6aa66e0512d5173c4ee8dfd6359db90 \
  --physical-host pathfinder-n5
test ! -e /tmp/h48-n5-offline-output.tar
sudo -n tar -cf /tmp/h48-n5-offline-output.tar -C "$stage/output" build
sudo -n chmod 0644 /tmp/h48-n5-offline-output.tar
sha256sum /tmp/h48-n5-offline-output.tar
