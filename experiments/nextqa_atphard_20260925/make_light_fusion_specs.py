"""Derive five public blue-green Compose specs from the proven 8x5 specs.

The generic compose_clone_service helper handles credentials in memory. This
transform only changes public paths, ports, project names and state identities.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


OLD_PUBLIC = "/home/pathfinder/a8x5-public-20260925-v1"
NEW_PUBLIC = "/home/pathfinder/a8x5-light-fusion-c458262-v1"
OLD_DEPLOY = "/home/pathfinder/a8x5-deployment-specs-v1"
PORTS = {
    "n4": 19124,
    "n7-cache": 19397,
    "n8-cache": 19398,
    "n7-route": 19782,
    "n8-route": 19882,
}
PATHS = {
    OLD_PUBLIC + "/final/n4": NEW_PUBLIC + "/n4",
    OLD_PUBLIC + "/runtime/bindings": NEW_PUBLIC + "/runtime/bindings",
    OLD_PUBLIC + "/runtime/dags": NEW_PUBLIC + "/runtime/dags",
    OLD_PUBLIC + "/runtime/admission": NEW_PUBLIC + "/runtime/admission",
    OLD_PUBLIC + "/plan": NEW_PUBLIC + "/plan",
}
URL_PORTS = {
    "pathfinder-full-flow-n4-derived-data-agent": 19124,
    "pathfinder-full-flow-n7-persistent-cache": 19397,
    "pathfinder-full-flow-n8-persistent-cache": 19398,
    "pathfinder-full-flow-n7-execution-compute": 19782,
    "pathfinder-full-flow-n8-execution-compute": 19882,
}


def _map_value(value: str) -> str:
    if value in PATHS:
        return PATHS[value]
    if value.startswith("http://"):
        from urllib.parse import urlsplit, urlunsplit

        parsed = urlsplit(value)
        if parsed.hostname in URL_PORTS:
            return urlunsplit((parsed.scheme,
                               f"{parsed.hostname}:{URL_PORTS[parsed.hostname]}",
                               parsed.path, parsed.query, parsed.fragment))
    return value


def build(
    base: Path, image: str, *, route_iteration: int = 1,
) -> tuple[dict[str, dict], dict[str, str]]:
    if not image.startswith("sha256:") or len(image) != 71:
        raise ValueError("route image must use a full immutable digest")
    if route_iteration not in (1, 2):
        raise ValueError("route iteration must be a frozen first or second")
    deploy_dir = NEW_PUBLIC + (
        "/deploy" if route_iteration == 1 else "/deploy-v2"
    )
    specs: dict[str, dict] = {}
    overlays: dict[str, str] = {}
    for name, port in PORTS.items():
        old = json.loads((base / f"{name}.json").read_bytes())
        if (old["project"] != "a8x5" + name.replace("-", "")
                or old["host_port"] == port):
            raise ValueError("source service identity or port differs")
        spec = dict(old)
        spec["predecessor"] = (
            f"{old['project']}-{old['service']}-1"
        )
        prefix = ("a8x5light2" if name.endswith("route")
                  and route_iteration == 2 else "a8x5light")
        spec["project"] = prefix + name.replace("-", "")
        spec["host_port"] = port
        spec["fragments"] = [
            path.replace(OLD_DEPLOY + "/", deploy_dir + "/")
            for path in old["fragments"]
        ]
        spec["overrides"] = {
            key: _map_value(value)
            for key, value in old["overrides"].items()
        }
        host_keys = [key for key in spec["overrides"]
                     if key.endswith("_HOST_PORT")]
        if len(host_keys) != 1:
            raise ValueError("expected exactly one host-port override")
        spec["overrides"][host_keys[0]] = str(port)
        if name == "n4":
            spec["overrides"]["PATHFINDER_COMPOSE_N4_DERIVED_DATA_AGENT_STATE_VOLUME"] = (
                "pathfinder-a8x5-light-n4-state-v1"
            )
        if name.endswith("cache"):
            node = name[:2].upper()
            spec["overrides"][f"PATHFINDER_COMPOSE_{node}_PERSISTENT_CACHE_STATE_VOLUME"] = (
                f"pathfinder-a8x5-light-{name}-state-v1"
            )
            spec["overrides"][f"PATHFINDER_{node}_CACHE_ID"] = (
                f"pathfinder-a8x5-light-{name}-v1"
            )
        if name.endswith("route"):
            spec["image"] = image
            spec["overrides"]["PATHFINDER_N7_CACHE_ID"] = (
                "pathfinder-a8x5-light-n7-cache-v1"
            )
            spec["overrides"]["PATHFINDER_N8_CACHE_ID"] = (
                "pathfinder-a8x5-light-n8-cache-v1"
            )
        specs[name] = spec
    for node in ("n7", "n8"):
        source = (base / f"route-overlay-{node}.yaml").read_text("utf-8")
        old_volume = f"pathfinder-a8x5-{node}-route-state-v1"
        new_volume = (
            f"pathfinder-a8x5-light-{node}-route-state-v{route_iteration}"
        )
        if source.count(old_volume) != 1:
            raise ValueError("source overlay state volume differs")
        overlays[node] = source.replace(old_volume, new_volume)
    return specs, overlays


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-spec-dir", type=Path, required=True)
    parser.add_argument("--route-image", required=True)
    parser.add_argument("--route-iteration", type=int, default=1)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise ValueError("new deployment spec directory already exists")
    specs, overlays = build(
        args.base_spec_dir, args.route_image,
        route_iteration=args.route_iteration,
    )
    args.output_dir.mkdir(parents=True)
    for name, spec in specs.items():
        (args.output_dir / f"{name}.json").write_text(
            json.dumps(spec, indent=2, sort_keys=True) + "\n",
            encoding="utf-8", newline="\n",
        )
    for name, overlay in overlays.items():
        (args.output_dir / f"route-overlay-{name}.yaml").write_text(
            overlay, encoding="utf-8", newline="\n",
        )


if __name__ == "__main__":
    main()
