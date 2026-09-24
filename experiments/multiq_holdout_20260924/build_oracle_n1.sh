#!/usr/bin/env bash
set -euo pipefail
stage=/home/pathfinder/h48-oracle-build-20260924-v1
private=/opt/pathfinder/formal/private/h48-oracle-20260924-v1
sudo -n test ! -e "$private/oracle"
test ! -e "$stage/public/commitment"
test "$(sha256sum /tmp/a00bb13-h48.tar | cut -d' ' -f1)" = 799c31940f5ba29a19676d1a765d75e0067230979f21c2d163f748c7a9155e61
test "$(sha256sum /tmp/h48-paid-input.tar | cut -d' ' -f1)" = 79d5994138a6f0cc9f880dd1b509cee2d5598e197d78535dfd167573e18b1c99
mkdir -p "$stage/source" "$stage/input" "$stage/public"
test -d "$stage/source/pathfinder"
tar -xf /tmp/h48-paid-input.tar -C "$stage/input" plan
sudo -n chown root:root "$stage/public"
sudo -n install -d -m 0700 "$private"
image=$(sudo -n docker inspect mq7n1-pathfinder-full-flow-n1-hidden-score-1 --format '{{.Image}}')
sudo -n docker run --rm --hostname pathfinder-n1 --network none \
  --read-only --user 0:0 --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m,mode=1777 \
  --mount type=bind,src="$stage/source",dst=/work,readonly \
  --mount type=bind,src="$stage/input/plan",dst=/plan,readonly \
  --mount type=bind,src=/tmp/build_multiq_oracle_n1.py,dst=/tool.py,readonly \
  --mount type=bind,src=/opt/pathfinder/formal/private/multiq-24route-1c9ad84-v1,dst=/private/source,readonly \
  --mount type=bind,src="$private",dst=/private/output \
  --mount type=bind,src="$stage/public",dst=/public \
  --workdir /work -e PYTHONPATH=/work --entrypoint python "$image" /tool.py \
  --plan-dir /plan --official-csv /private/source/official-val.csv \
  --official-csv-sha256 43198bdef8436b8d64a9b75d846b0987c10cbf94ebf4be325c4a4e54634d66b8 \
  --private-root /private --private-output /private/output/oracle \
  --public-output /public/commitment
sudo -n chown -R 10001:10001 "$private"
sudo -n find "$private" -type d -exec chmod 0500 {} +
sudo -n find "$private" -type f -exec chmod 0400 {} +
sudo -n tar -cf /tmp/h48-n1-public-commitment.tar -C "$stage/public" commitment
sudo -n chmod 0644 /tmp/h48-n1-public-commitment.tar
sha256sum /tmp/h48-n1-public-commitment.tar
