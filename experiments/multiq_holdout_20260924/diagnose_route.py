"""Read one failed route row and resolve only exact static error hashes."""
import ast
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

root = Path('/home/pathfinder/h48-public-20260924-v1')
key = sys.argv[1] if len(sys.argv) > 1 else 'fresh-multiq-holdout-20260924-v2|nextqa-val-2400715506-q3|I'
bindings = [json.loads(x) for x in (root / 'runtime/bindings/route-inputs.jsonl').read_bytes().splitlines()]
route = next(x for x in bindings if x['trial_key'] == key)
identity = {'domain': 'pathfinder.generic-semantic-route-id/v1',
            'run_id': route['run_id'], 'trial_key': key}
execution = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
db = sqlite3.connect('file:/state/route-executions.sqlite3?mode=ro', uri=True)
db.execute('PRAGMA query_only=ON')
row = db.execute('SELECT state, failure_sha256, length(evidence_json) FROM route_executions WHERE execution_id=?', (execution,)).fetchone()
db.close()
matches = []
if row and row[1]:
    for path in Path('/app/pathfinder').rglob('*.py'):
        try:
            tree = ast.parse(path.read_bytes())
        except (ValueError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if hashlib.sha256(node.value.encode()).hexdigest() == row[1]:
                    matches.append({'file': str(path.relative_to('/app')), 'line': node.lineno, 'message': node.value})
print(json.dumps({'execution_id': execution, 'row': row, 'matches': matches,
                  'workflow_submitted': False, 'llm_called': False}))
