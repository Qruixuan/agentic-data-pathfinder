"""Operator-side, no-retry continuation of exactly diagnosed refusals.

All actual execution remains in experiments.batch. This helper reads only
sanitized diagnostics; it cannot change inputs, images, credentials or IDs.
"""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
PUBLIC = ROOT / 'artifacts/h48-runtime-v1'
AUDIT = ROOT / 'artifacts/h48-provider-diagnostics-v1'
REMOTE = '/home/pathfinder/h48-runner-20260924-v1/output'
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
       '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3',
       '-J', 'pathfinder@94.237.65.184']
ROUTE = 'h48n7-pathfinder-full-flow-n7-execution-compute-1'
N6 = 'pathfinder-multiq-d328726-n6-pathfinder-full-flow-n6-semantic-inference-1'


def call(host, command):
    response = subprocess.run(SSH + [f'pathfinder@10.70.0.{host}', command],
                              capture_output=True, text=True, timeout=90)
    if response.returncode:
        raise RuntimeError(f'read-only remote check failed on node {host}')
    return response.stdout, response.stderr


def json_lines(text):
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def main():
    batch_number = int(sys.argv[1])
    assert 1 <= batch_number <= 48
    AUDIT.mkdir(parents=True, exist_ok=True)
    # Public-only copy staged for the elevated Windows network identity;
    # never copy private oracle or credential files between identities.
    binding_bytes = (ROOT / '.codex_build/h48-route-inputs.jsonl').read_bytes()
    assert hashlib.sha256(binding_bytes).hexdigest() == 'bdfa0bc98cc488f656fa605c9bf9ca26edad3050aeb30929e3ad79924a3ecdbd'
    bindings = [json.loads(x) for x in binding_bytes.splitlines()]
    by_key = {r['trial_key']: r for r in bindings}
    while batch_number <= 48:
        parent = f'routes-cont{batch_number}'
        failed = json.loads(call(17, f'sudo -n cat {REMOTE}/{parent}/failure.json')[0])
        ordinal, key = failed['ordinal'], failed['trial_key']
        assert 0 <= ordinal < 48
        assert failed['failure_code'] in {
            'flowmesh-workflow-terminal-failure', 'flowmesh-result-retrieval-failed'}
        route = by_key[key]
        durable = json.loads(call(17, f'sudo -n docker exec -i {ROUTE} python - '
                                  f'{shlex.quote(key)} < /tmp/diagnose_route.py')[0])
        assert durable['row'] == ['FAILED',
            'bcb93bc665615217dc474bcec92edc7b3702d8343927b906cc7e8b2bc9d254ff', None]
        metadata = json_lines(call(17, 'sudo -n python3 -I /tmp/launch_batch_n7.py diagnose '
                                  + shlex.quote(key))[0])[-1]
        assert metadata['count'] == 1
        task = metadata['matches'][0]
        assert task['status'] == task['workflow_status'] == 'FAILED'
        assert task['adapter_error_class'] == 'N6AdapterError'
        begin, end = failed['route_started_utc'], failed['route_ended_utc']
        stdout, stderr = call(16, f'sudo -n docker logs --since {shlex.quote(begin)} '
                                   f'--until {shlex.quote(end)} {N6}')
        logs = json_lines(stdout + stderr)
        assert len(logs) == 1
        event = logs[0]
        assert event['event'] == 'semantic-request-error'
        assert event['error_type'] == 'SemanticLLMRequestError'
        assert event['provider_code'] == event['provider_type'] == 'data_inspection_failed'
        assert event['http_status'] == 400 and event['credentials_recorded'] is False
        diagnosis = {
            'schema_version': 'pathfinder.operator-terminal-diagnosis/v1',
            'run_id': route['run_id'], 'trial_key': key,
            'execution_id': durable['execution_id'], 'durable_state': 'FAILED',
            'failure_sha256': durable['row'][1],
            'workflow_id': task['workflow_id'], 'task_id': task['task_id'],
            'workflow_status': task['workflow_status'], 'task_status': task['status'],
            'assigned_worker': task['assigned_worker'],
            'initial_runner_failure_code': failed['failure_code'],
            'n6_error_sha256': event['error_sha256'],
            'provider_code': event['provider_code'], 'http_status': 400,
            'n6_log_interval_utc': [begin, end],
            'retry_authorized': False, 'input_modification_authorized': False,
            'sample_replacement_authorized': False,
            'provider_failed_attempt_charge_known': False,
            'diagnostic_is_operator_observation_not_n1_score': True,
            'credentials_recorded': False,
        }
        path = AUDIT / f'provider-rejection-{ordinal}.json'
        payload = json.dumps(diagnosis, indent=2, sort_keys=True).encode() + b'\n'
        if path.exists():
            assert path.read_bytes() == payload
        else:
            with path.open('xb') as stream:
                stream.write(payload)
        digest = hashlib.sha256(payload).hexdigest()
        scp = ['scp', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
               '-J', 'pathfinder@94.237.65.184', str(path),
               f'pathfinder@10.70.0.17:/tmp/{path.name}']
        subprocess.run(scp, check=True, timeout=90, capture_output=True)
        actual = call(17, f'sha256sum /tmp/{path.name}')[0].split()[0]
        assert actual == digest
        call(17, f'if test ! -e {REMOTE}/{path.name}; then sudo -n install -m 0444 '
                 f'/tmp/{path.name} {REMOTE}/{path.name}; fi')
        assert call(17, f'sudo -n sha256sum {REMOTE}/{path.name}')[0].split()[0] == digest
        print(json.dumps({'ordinal': ordinal, 'status': 'PROVIDER_REJECTION_PRESERVED',
                          'retry': False, 'diagnosis_sha256': digest}), flush=True)
        # The ordinary zero-inference auth/health gate remains mandatory.
        gate = json.loads(call(17, f'sudo -n docker exec -i {ROUTE} python - '
                                    '< /tmp/preflight_h48.py')[0])
        assert gate['status'] == 'AUTH_AND_USAGE_GATES_VERIFIED'
        batch_number += 1
        assert batch_number <= 49
        target = f'routes-cont{batch_number}'
        command = ('sudo -n python3 -I /tmp/launch_batch_n7.py continue '
                   f'/output/{target} /output/{parent} /output/{path.name}')
        # Stream public route progress. The runner validates the prefix and
        # submits only its never-executed suffix, bounded by the original plan.
        status = subprocess.run(SSH + ['pathfinder@10.70.0.17', command]).returncode
        if status == 0:
            print(json.dumps({'status': 'ALL_ORIGINAL_SLOTS_OBSERVED',
                              'remote_output': f'{REMOTE}/{target}'}), flush=True)
            return
        if status not in {1, 2}:
            raise RuntimeError('continuation runner stopped outside terminal route handling')
    raise RuntimeError('continuation budget exhausted')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('CONTINUATION_STOPPED', type(error).__name__,
              getattr(error, 'filename', None), flush=True)
        raise SystemExit(2) from None
