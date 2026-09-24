"""Read-only exact-run lineage; never export task bodies or predictions."""
import json
import os
from pathlib import Path
import re
from flowmesh import FlowMesh

root = Path('/home/pathfinder/h48-public-20260924-v1')
bindings = [json.loads(x) for x in (root / 'runtime/bindings/route-inputs.jsonl').read_bytes().splitlines()]
client = FlowMesh(base_url=os.environ['FLOWMESH_BASE_URL'],
                  api_key=os.environ.get('FLOWMESH_API_KEY') or None)

def bound(text, value):
    # D must not match DC, nor a run ID that merely extends this one.
    return re.search(r'(?<![A-Za-z0-9_|.-])' + re.escape(value)
                     + r'(?![A-Za-z0-9_|.-])', text) is not None

try:
    tasks = [x.model_dump(mode='json') for x in
             client.tasks.list(task_type='api', assigned_worker='wkr-2')]
    serialized = [(x, json.dumps(x)) for x in tasks]
    coverage = [{'run_id': r['run_id'], 'matches': sum(bound(text, r['run_id']) and bound(text, r['trial_key'])
                                                     for _, text in serialized)} for r in bindings]
    print(json.dumps({'returned_task_count': len(tasks),
                      'nonunit_coverage': [x for x in coverage if x['matches'] != 1]}), flush=True)
    rows = []
    for route in bindings:
        matches = [x for x, text in serialized
                   if bound(text, route['run_id']) and bound(text, route['trial_key'])]
        assert len(matches) == 1
        task = matches[0]
        workflow = client.workflows.retrieve(task['workflow_id'])
        workflow_status = str(getattr(workflow.status, 'value', workflow.status))
        assert task['assigned_worker'] == 'wkr-2'
        assert task['status'] in {'DONE', 'FAILED'}
        assert workflow_status in {'DONE', 'FAILED'}
        rows.append({
            'run_id': route['run_id'], 'trial_key': route['trial_key'],
            'workflow_id': task['workflow_id'], 'task_id': task['task_id'],
            'task_status': task['status'], 'workflow_status': workflow_status,
            'assigned_worker_id': task['assigned_worker'],
        })
    assert len(rows) == 48 and len({r['workflow_id'] for r in rows}) == 48
    print(json.dumps({'status': 'VERIFIED_EXACT_RUN_LINEAGE', 'rows': rows,
                      'workflow_count': 48, 'duplicate_submissions': 0,
                      'credentials_recorded': False, 'task_bodies_included': False}))
finally:
    client.close()
