#!/usr/bin/env bash
set -euo pipefail

stage=/home/pathfinder/relational-dev-oracle-build-20260924-v1
private=/opt/pathfinder/formal/private/relational-dev-oracle-20260924-v1
test ! -e "$stage"
sudo -n test ! -e "$private"
test "$(sha256sum /tmp/relational-dev-source-b02ac75.tar | cut -d' ' -f1)" = dc8d37d67539b70289f1200b844d6bfc511c56f7112b0a2ff89d8554ebe8bd75
test "$(sha256sum /tmp/relational-dev-plan-20260924.tar | cut -d' ' -f1)" = fabcb86605f586c3fd0ca0ee5db56791a715bd0a4146b115be60ecc122ab7332

mkdir -p "$stage/source" "$stage/input" "$stage/public"
tar -xf /tmp/relational-dev-source-b02ac75.tar -C "$stage/source"
tar -xf /tmp/relational-dev-plan-20260924.tar -C "$stage/input"
sudo -n install -d -m 0700 "$private"
image=$(sudo -n docker inspect h48n1-pathfinder-full-flow-n1-hidden-score-1 --format '{{.Image}}')

sudo -n docker run --rm --hostname pathfinder-n1 --network none \
  --read-only --user 0:0 --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m,mode=1777 \
  --mount type=bind,src="$stage/source",dst=/work,readonly \
  --mount type=bind,src="$stage/input/plan",dst=/plan,readonly \
  --mount type=bind,src=/opt/pathfinder/formal/private/multiq-24route-1c9ad84-v1,dst=/private/source,readonly \
  --mount type=bind,src="$private",dst=/private/output \
  --mount type=bind,src="$stage/public",dst=/public \
  --workdir /work -e PYTHONPATH=/work --entrypoint python "$image" \
  -m experiments.build_multiq_oracle_n1 \
  --plan-dir /plan --official-csv /private/source/official-val.csv \
  --official-csv-sha256 43198bdef8436b8d64a9b75d846b0987c10cbf94ebf4be325c4a4e54634d66b8 \
  --private-root /private --private-output /private/output/oracle \
  --public-output /public/commitment

sudo -n chown -R 10001:10001 "$private"
sudo -n find "$private" -type d -exec chmod 0500 {} +
sudo -n find "$private" -type f -exec chmod 0400 {} +
test ! -e /tmp/relational-dev-n1-public-commitment.tar
sudo -n tar -cf /tmp/relational-dev-n1-public-commitment.tar -C "$stage/public" commitment
sudo -n chmod 0644 /tmp/relational-dev-n1-public-commitment.tar
sha256sum /tmp/relational-dev-n1-public-commitment.tar
