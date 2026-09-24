"""Read-only deployment inspection with explicit non-secret output fields."""
import json
import subprocess

ids = subprocess.check_output(["docker", "ps", "-q"]).decode().split()
all_info = json.loads(subprocess.check_output(["docker", "inspect", *ids]))
rows = []
for info in all_info:
    name = info["Name"].lstrip("/")
    if "pathfinder" not in name:
        continue
    env = dict(item.split("=", 1) for item in info["Config"]["Env"])
    rows.append({
        "name": name, "image": info["Image"],
        "project": info["Config"]["Labels"].get("com.docker.compose.project"),
        "compose_files": info["Config"]["Labels"].get("com.docker.compose.project.config_files"),
        "mounts": [{k: m.get(k) for k in ("Type", "Source", "Destination", "RW", "Name")}
                   for m in info["Mounts"]],
        "ports": info["HostConfig"]["PortBindings"],
        "origins": {k: v for k, v in env.items() if k.endswith("_BASE_URL")
                    and k.startswith("PATHFINDER_") and "LLM" not in k},
        "environment_key_names": sorted(env),
        "status": info["State"].get("Health", {}).get("Status"),
    })
print(json.dumps(rows, sort_keys=True))
