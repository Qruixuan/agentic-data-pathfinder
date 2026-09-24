"""Pass existing runtime credentials in memory to the shared batch CLI."""
import json
import os
from pathlib import Path
import subprocess
import sys

stage = Path('/home/pathfinder/h48-runner-20260924-v1')
public = Path('/home/pathfinder/h48-public-20260924-v1')
route_name = 'h48n7-pathfinder-full-flow-n7-execution-compute-1'
runner_image = 'sha256:9cc1202c88d14665ffdce135421092449d2172f01f4087ed5455bb88eab713b1'


def container(name):
    return json.loads(subprocess.check_output(['docker', 'inspect', name]))[0]


try:
    route = container(route_name)
    worker = container('pathfinder_costaware_20260815a')
    print(json.dumps({'route_health': route['State']['Health']['Status'],
                      'worker_health': worker['State']['Health']['Status']}), flush=True)
    assert route['State']['Health']['Status'] == 'healthy'
    assert worker['State']['Health']['Status'] == 'healthy'
    runtime_env = dict(x.split('=', 1) for x in route['Config']['Env'])
    worker_env = dict(x.split('=', 1) for x in worker['Config']['Env'])
    selected = {k: worker_env[k] for k in ('FLOWMESH_BASE_URL', 'FLOWMESH_API_KEY')}
    selected['PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET'] = runtime_env['PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET']
    print(json.dumps({'missing_client_key_names': [k for k, v in selected.items() if not v]}), flush=True)
    assert selected['FLOWMESH_BASE_URL']
    assert selected['PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET']
    # The existing private Root uses its default no-API-key configuration.
    # Match FlowMeshSettings.from_environment (empty key -> None); preflight
    # still has to authenticate successfully against the actual Root.
    environment = {**os.environ, **selected}
    action = sys.argv[1]
    continuation = action in {'continue', 'verify-continuation'}
    source = (Path('/home/pathfinder/h48-runner-2f2ef1a/source')
              if continuation else stage / 'source')
    command = [
        'docker', 'run', '--rm', '--network', 'host', '--read-only',
        '--user', '10001:10001', '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges',
        '--tmpfs', '/tmp:rw,nosuid,nodev,size=256m,mode=1777',
        '--mount', f'type=bind,src={source},dst=/work,readonly',
        '--mount', f'type=bind,src={public},dst={public},readonly',
        '--mount', f'type=bind,src={stage}/output,dst=/output',
        '--workdir', '/work', '-e', 'PYTHONPATH=/work',
    ]
    for name in selected:
        command += ['-e', name]
    assert action in {'preflight', 'execute', 'verify', 'diagnose', 'audit', 'continue', 'verify-continuation'}
    if action in {'diagnose', 'audit'}:
        script = 'read_failure_task.py' if action == 'diagnose' else 'audit_flowmesh_lineage.py'
        command += ['--mount', f'type=bind,src=/tmp/{script},dst=/tools/diagnose.py,readonly',
                    '--entrypoint', 'python', runner_image, '/tools/diagnose.py']
        command += sys.argv[2:]
        raise SystemExit(subprocess.run(command, env=environment).returncode)
    verb = {'continue': 'execute', 'verify-continuation': 'verify'}.get(action, action)
    command += ['--entrypoint', 'python', runner_image, '-m', 'experiments.batch', verb,
                '--config-dir', str(public / 'runtime/config'), '--artifact-root', str(public)]
    if verb in {'execute', 'verify'}:
        command += ['--output-dir', sys.argv[2] if continuation and len(sys.argv) > 2
                    else '/output/routes-cont1' if continuation else '/output/routes']
    if action == 'continue':
        command += ['--resume-from', sys.argv[3] if len(sys.argv) > 3 else '/output/routes',
                    '--failure-diagnosis', sys.argv[4] if len(sys.argv) > 4 else
                    '/work/experiments/multiq_holdout_20260924/provider-rejection-13.json']
    if verb == 'verify':
        command += ['--seal']
    raise SystemExit(subprocess.run(command, env=environment).returncode)
except Exception as exc:
    print(json.dumps({'status': 'BATCH_LAUNCH_FAILED', 'class': type(exc).__name__}))
    raise SystemExit(2)
