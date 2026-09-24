#!/usr/bin/env bash
set -euo pipefail

old=pathfinder-multiq-d328726-n7-pathfinder-full-flow-n7-execution-compute-1
new=mq7n7v3-pathfinder-full-flow-n7-execution-compute-1
image=sha256:f2a7aa94b28ccd20c902b5f4f50d2e0dda2904e878564a70aedf8261981dde89
volume=pathfinder-multiq-n7-route-state-sealed-20260924-v3
root=/home/pathfinder/multiq-public-d328726-root/artifacts

compose=(
  sudo -n docker compose
  --project-name mq7n7v3
  --env-file /opt/pathfinder/env/node-formal-bd2da19-n7-route.env
  --env-file /home/pathfinder/multiq-config-d328726/pilot-v2.env
  --env-file /home/pathfinder/multiq-config-d328726/sealed-n7-nonsecret-v3.env
  -f /opt/pathfinder/deploy/node-bundles/N7/compose.service.pathfinder-full-flow-n7-execution-compute.yaml
  -f /home/pathfinder/multiq-config-d328726/n7-route-v2.yaml
  -f /home/pathfinder/multiq-config-d328726/sealed-n7-state-v3.yaml
)

test "$(sudo -n docker inspect "$old" --format '{{index .Config.Labels "com.docker.compose.project"}}')" = pathfinder-multiq-d328726-n7
test "$(sudo -n docker inspect "$old" --format '{{.State.Health.Status}}')" = healthy
old_id=$(sudo -n docker inspect "$old" --format '{{.Id}}')
if sudo -n docker container inspect "$new" >/dev/null 2>&1; then
  echo NEW_CONTAINER_NAME_ALREADY_USED >&2
  exit 2
fi
if sudo -n docker volume inspect "$volume" >/dev/null 2>&1; then
  echo NEW_STATE_VOLUME_NAME_ALREADY_USED >&2
  exit 2
fi
sudo -n docker image inspect "$image" >/dev/null
for package in \
  multiq-sealed-test-plan-20260924-v2 \
  multiq-sealed-query-20260924-v2 \
  multiq-sealed-n3-20260924-v2 \
  multiq-sealed-test-oracle-commitment-20260924-v2 \
  multiq-sealed-route-bindings-20260924-v2 \
  multiq-sealed-trial-dags-20260924-v2 \
  multiq-sealed-runtime-admission-20260924-v2; do
  (cd "$root/$package" && sha256sum -c SHA256SUMS >/dev/null)
done
"${compose[@]}" --profile serve-frozen config --quiet

old_stopped=false
completed=false
rollback() {
  if [[ $old_stopped == true && $completed == false ]]; then
    sudo -n docker stop "$new" >/dev/null 2>&1 || true
    sudo -n docker start "$old" >/dev/null || true
    echo ROLLED_BACK_TO_OLD_N7_ROUTE >&2
  fi
}
trap rollback EXIT

sudo -n docker stop "$old" >/dev/null
old_stopped=true
"${compose[@]}" --profile serve-frozen up -d --no-deps --wait \
  pathfinder-full-flow-n7-execution-compute

test "$(sudo -n docker inspect "$new" --format '{{.State.Health.Status}}')" = healthy
test "$(sudo -n docker inspect "$new" --format '{{.Image}}')" = "$image"
test "$(sudo -n docker inspect "$new" --format '{{.RestartCount}}')" = 0
sudo -n docker inspect "$new" --format '{{json .Mounts}}' | python3 -c '
import json, sys
mounts = json.load(sys.stdin)
expected = {
    "multiq-sealed-test-plan-20260924-v2",
    "multiq-sealed-query-20260924-v2",
    "multiq-sealed-n3-20260924-v2",
    "multiq-sealed-test-oracle-commitment-20260924-v2",
    "multiq-sealed-route-bindings-20260924-v2",
    "multiq-sealed-trial-dags-20260924-v2",
    "multiq-sealed-runtime-admission-20260924-v2",
}
found = {m["Source"].rstrip("/").split("/")[-1]
         for m in mounts if m["Type"] == "bind"}
assert expected <= found, sorted(expected - found)
assert all(not m["RW"] for m in mounts if m["Type"] == "bind")
assert any(m["Type"] == "volume" and
           m["Name"] == "pathfinder-multiq-n7-route-state-sealed-20260924-v3"
           and m["Destination"] == "/state" for m in mounts)
print("N7_SEVEN_MOUNTS_VERIFIED")
'
curl --fail --silent --show-error --max-time 5 \
  http://10.70.0.17:18780/healthz >/dev/null
test "$(sudo -n docker inspect "$old" --format '{{.Id}}')" = "$old_id"
completed=true
echo N7_SEVEN_ROUTE_CUTOVER_HEALTHY
