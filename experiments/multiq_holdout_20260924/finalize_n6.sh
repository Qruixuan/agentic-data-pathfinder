#!/usr/bin/env bash
set -euo pipefail
stage=/home/pathfinder/h48-paid-20260924-v1
public=/home/pathfinder/h48-public-20260924-v1
image=sha256:002415af005a0cd0159a71b367f2eca3d5044fa238d73cd1b8a5ed8972bd3f05
test ! -e "$public"
test "$(sha256sum /tmp/h48-n1-public-commitment.tar | cut -d' ' -f1)" = a4e39dd73aa2e116f82806bccd2a21b02a948ab4dab19c66db612d9baf371945
sudo -n install -d -m 0700 -o 10001 -g 10001 "$public" "$public/paid"
sudo -n cp -a "$stage/input/plan" "$stage/input/build" "$public/"
for package in captions video-index query; do
  sudo -n cp -a "$stage/output/$package" "$public/paid/"
done
sudo -n tar -xf /tmp/h48-n1-public-commitment.tar -C "$public"
sudo -n chown -R 10001:10001 "$public"
sudo -n docker run --rm --network none --read-only --user 10001:10001 \
  --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=256m,mode=1777 \
  --mount type=bind,src="$stage/source",dst=/work,readonly \
  --mount type=bind,src="$public",dst="$public" \
  --mount type=bind,src=/tmp/finalize_multiq_inputs.py,dst=/tools/finalize.py,readonly \
  --workdir /work -e PYTHONPATH=/work --entrypoint python "$image" \
  /tools/finalize.py --input-dir "$public" --paid-dir "$public/paid" \
  --output-dir "$public/final" --physical-host pathfinder-n6
sudo -n docker run --rm --network none --read-only --user 10001:10001 \
  --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=256m,mode=1777 \
  --mount type=bind,src="$stage/source",dst=/work,readonly \
  --mount type=bind,src="$public",dst="$public" \
  --mount type=bind,src=/tmp/freeze_runtime.py,dst=/tools/freeze.py,readonly \
  --workdir /work -e PYTHONPATH=/work --entrypoint python "$image" \
  /tools/freeze.py "$public" /work/experiments/multiq_holdout_20260924/execution-protocol.json
test ! -e /tmp/h48-public-runtime.tar
sudo -n tar -cf /tmp/h48-public-runtime.tar -C "$public" .
sudo -n chmod 0644 /tmp/h48-public-runtime.tar
sha256sum /tmp/h48-public-runtime.tar
