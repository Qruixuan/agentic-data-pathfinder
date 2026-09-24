#!/usr/bin/env bash
set -euo pipefail
root=/home/pathfinder/h48-public-20260924-v1
test ! -e "$root"
test "$(sha256sum /tmp/h48-public-runtime.tar | cut -d' ' -f1)" = b97edacfa50bed0d9effba097186521020e1d55e6998d335086e5f77a3ea2814
sudo -n install -d -m 0700 -o 10001 -g 10001 "$root"
sudo -n tar -xf /tmp/h48-public-runtime.tar -C "$root"
sudo -n chown -R 10001:10001 "$root"
sudo -n python3 - "$root" <<'PY'
import hashlib
from pathlib import Path
import sys
root = Path(sys.argv[1])
count = 0
for sums in root.rglob('SHA256SUMS'):
    for line in sums.read_text().splitlines():
        expected, name = line.split('  ', 1)
        target = (sums.parent / name).resolve()
        assert target.is_relative_to(sums.parent.resolve())
        assert hashlib.sha256(target.read_bytes()).hexdigest() == expected
        count += 1
print('PUBLIC_INPUT_CHECKSUMS_VERIFIED', count)
PY
