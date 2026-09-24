#!/usr/bin/env bash
set -euo pipefail

stage=/home/pathfinder/relational-dev-oracle-build-20260924-v1
private=/opt/pathfinder/formal/private/relational-dev-oracle-20260924-v1
sudo -n test -d "$private/oracle/n1-oracle-package"
test ! -e "$stage/public/commitment"
sudo -n chown root:root "$stage/public"
image=$(sudo -n docker inspect h48n1-pathfinder-full-flow-n1-hidden-score-1 --format '{{.Image}}')

sudo -n docker run --rm --hostname pathfinder-n1 --network none \
  --read-only --user 0:0 --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m,mode=1777 \
  --mount type=bind,src="$stage/source",dst=/work,readonly \
  --mount type=bind,src="$private",dst=/private/output,readonly \
  --mount type=bind,src="$stage/public",dst=/public \
  --mount type=bind,src=/tmp/relational-dev-freeze-public-n1.py,dst=/tool.py,readonly \
  --workdir /work -e PYTHONPATH=/work --entrypoint python "$image" /tool.py

sudo -n chown -R 10001:10001 "$private"
sudo -n find "$private" -type d -exec chmod 0500 {} +
sudo -n find "$private" -type f -exec chmod 0400 {} +
test ! -e /tmp/relational-dev-n1-public-commitment.tar
sudo -n tar -cf /tmp/relational-dev-n1-public-commitment.tar -C "$stage/public" commitment
sudo -n chmod 0644 /tmp/relational-dev-n1-public-commitment.tar
sha256sum /tmp/relational-dev-n1-public-commitment.tar
