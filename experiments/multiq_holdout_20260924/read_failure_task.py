"""Inspect the bounded SDK task-list interface, never print task bodies."""
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from flowmesh import FlowMesh

client = FlowMesh(base_url=os.environ['FLOWMESH_BASE_URL'],
                  api_key=os.environ.get('FLOWMESH_API_KEY') or None)
key = sys.argv[1] if len(sys.argv) > 1 else 'fresh-multiq-holdout-20260924-v2|nextqa-val-2400715506-q3|I'
root = Path('/home/pathfinder/h48-public-20260924-v1')
rows = [json.loads(x) for x in (root / 'runtime/bindings/route-inputs.jsonl').read_bytes().splitlines()]
run_id = next(x['run_id'] for x in rows if x['trial_key'] == key)
matches = []
for task in client.tasks.list(task_type='api', assigned_worker='wkr-2'):
    payload = task.model_dump(mode='json')
    serialized = json.dumps(payload)
    if run_id not in serialized or key not in serialized:
        continue
    detail = str(payload.get('error') or payload.get('last_error') or '')
    wrapper = re.search(r'semantic route adapter failed: ([A-Za-z][A-Za-z0-9_]*)', detail)
    workflow = client.workflows.retrieve(payload['workflow_id'])
    workflow_status = getattr(workflow.status, 'value', workflow.status)
    matches.append({
        'task_id': payload.get('id', payload.get('task_id')),
        'workflow_id': payload.get('workflow_id'),
        'workflow_status': str(workflow_status),
        'status': payload.get('status'),
        'assigned_worker': payload.get('assigned_worker'),
        'adapter_error_class': wrapper.group(1) if wrapper else None,
        'detail_sha256': hashlib.sha256(detail.encode()).hexdigest(),
        'metadata_keys': sorted(payload),
    })
print(json.dumps({'matches': matches, 'count': len(matches)}))
client.close()
