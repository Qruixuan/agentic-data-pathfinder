"""Check each SHA256SUMS from its own directory without dumping payloads."""

import argparse
from pathlib import Path
import subprocess


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    if not root.is_dir() or root.is_symlink():
        raise ValueError("frozen root is not a directory")
    manifests = sorted(root.rglob("SHA256SUMS"))
    if not manifests:
        raise ValueError("frozen root has no checksum manifests")
    failures = []
    for manifest in manifests:
        result = subprocess.run(
            ["sha256sum", "--quiet", "-c", "SHA256SUMS"],
            cwd=manifest.parent, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=False,
        )
        if result.returncode:
            failures.append(str(manifest.relative_to(root)))
    if failures:
        print("CHECKSUM_FAILED", ",".join(failures))
        return 2
    print("CHECKSUMS_VERIFIED", len(manifests))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
