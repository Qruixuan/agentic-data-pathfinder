"""Scoped, rollback-safe cutover of the existing isolated pilot services.

Run as root on the named node. Secrets are reused in process memory from
the current service and authoritative env files, never serialized by this
helper. Old container objects, volumes, env files and images are retained.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess

PUBLIC = Path('/home/pathfinder/h48-public-20260924-v1')
CONFIG = Path('/home/pathfinder/h48-config-20260924-v1')
ORACLE = '/opt/pathfinder/formal/private/h48-oracle-20260924-v1/oracle/n1-oracle-package'
PREFIX = 'pathfinder-full-flow-'
SERVICES = {
    'N1': ['n1-hidden-score', 'n1-hidden-score-n1-remote-verification'],
    'N2': ['n2-global-index'], 'N3': ['n3-raw-data-agent'],
    'N4': ['n4-derived-data-agent'], 'N7': ['n7-execution-compute'],
}
OLD_PROJECT = {'N1': 'mq7n1', 'N3': 'mq7n3', 'N7': 'mq7n7v3',
               'N2': 'pathfinder-multiq-d328726-n2',
               'N4': 'pathfinder-multiq-d328726-n4'}


def command(args, **kwargs):
    return subprocess.check_output(args, **kwargs)


def inspect(name):
    return json.loads(command(['docker', 'inspect', name]))[0]


def run(node, apply):
    names = [PREFIX + name for name in SERVICES[node]]
    old = [inspect(OLD_PROJECT[node] + '-' + name + '-1') for name in names]
    assert all(x['State']['Health']['Status'] == 'healthy' for x in old)
    assert len({x['Image'] for x in old}) == 1
    for item in old:
        assert item['Config']['Labels']['com.docker.compose.project'] == OLD_PROJECT[node]
        assert item['Config']['User'] == '10001:10001'
        assert item['HostConfig']['ReadonlyRootfs'] is True
    project = 'h48' + node.lower()
    image = old[0]['Image']
    inherited = {}
    for item in old:
        values = dict(value.split('=', 1) for value in item['Config']['Env'])
        for key, value in values.items():
            if key in inherited and inherited[key] != value:
                raise ValueError('existing service environment disagrees: ' + key)
        inherited.update(values)
    nonsecret = {
        'PATHFINDER_FULL_FLOW_SERVICE_IMAGE': image,
        'PATHFINDER_FULL_FLOW_RUNTIME_UID_GID': '10001:10001',
        'PATHFINDER_FULL_FLOW_BIND_ADDRESS': '10.70.0.1' + node[1],
        'PATHFINDER_FULL_FLOW_NETWORK_NAME': next(iter(old[0]['NetworkSettings']['Networks'])),
    }
    for name, item in zip(names, old):
        ports = item['HostConfig']['PortBindings']
        assert len(ports) == 1
        binding = next(iter(ports.values()))
        assert len(binding) == 1
        nonsecret[name.upper().replace('-', '_') + '_HOST_PORT'] = binding[0]['HostPort']
    if node == 'N1':
        nonsecret.update(PATHFINDER_N1_PACKAGE_DIR=ORACLE,
                         PATHFINDER_COMPOSE_N1_HIDDEN_SCORE_STATE_VOLUME='pathfinder-h48-n1-state-v1')
    elif node in {'N2', 'N3', 'N4'}:
        nonsecret['PATHFINDER_' + node + '_PACKAGE_DIR'] = str(PUBLIC / 'final' / node.lower())
        if node != 'N2':
            kind = 'RAW' if node == 'N3' else 'DERIVED'
            nonsecret[f'PATHFINDER_COMPOSE_{node}_{kind}_DATA_AGENT_STATE_VOLUME'] = (
                'pathfinder-h48-' + node.lower() + '-state-v1')
    else:
        paths = {
            'PATHFINDER_LOCAL_SEMANTIC_ADMISSION_DIR': 'runtime/admission',
            'PATHFINDER_N1_PUBLIC_COMMITMENT_DIR': 'commitment',
            'PATHFINDER_FULL_FLOW_ARTIFACT_BINDING_DIR': 'runtime/bindings',
            'PATHFINDER_N2_PACKAGE_DIR': 'final/n2',
            'PATHFINDER_N3_PACKAGE_DIR': 'final/n3',
            'PATHFINDER_N4_PACKAGE_DIR': 'final/n4',
            'PATHFINDER_INTERLEAVED_TRIAL_DAG_DIR': 'runtime/dags',
            'PATHFINDER_INTERLEAVED_PLAN_DIR': 'plan',
            'PATHFINDER_INTERLEAVED_RAW_PACKAGE_DIR': 'build/raw',
            'PATHFINDER_INTERLEAVED_QUERY_DIR': 'paid/query',
            'PATHFINDER_INTERLEAVED_VIDEO_INDEX_DIR': 'paid/video-index',
            'PATHFINDER_INTERLEAVED_PREPARATION_DIR': 'build/preparation',
            'PATHFINDER_INTERLEAVED_CAPTION_DIR': 'paid/captions',
        }
        nonsecret.update({key: str(PUBLIC / value) for key, value in paths.items()})
    # Frozen private inputs never leave N1; all other bind targets must be present.
    for key, value in nonsecret.items():
        if key.endswith('_DIR') and not Path(value).is_dir():
            raise ValueError('missing prepared package: ' + key)
    CONFIG.mkdir(mode=0o700, exist_ok=True)
    env_path = CONFIG / (node.lower() + '-public.env')
    payload = ''.join(f'{k}={v}\n' for k, v in sorted(nonsecret.items())).encode()
    if env_path.exists():
        assert env_path.read_bytes() == payload
    else:
        env_path.write_bytes(payload)
        env_path.chmod(0o600)
    base_name = 'node-formal-bd2da19-' + node.lower() + ('-route' if node == 'N7' else '') + '.env'
    pilot_name = 'pilot-v2.env' if node in {'N3', 'N7'} else 'pilot.env'
    args = ['docker', 'compose', '--project-name', project,
            '--env-file', '/opt/pathfinder/env/' + base_name,
            '--env-file', '/home/pathfinder/multiq-config-d328726/' + pilot_name,
            '--env-file', str(env_path)]
    prefix_args = list(args)
    individual = [prefix_args + ['-f', '/opt/pathfinder/deploy/node-bundles/' + node + '/compose.service.' + name + '.yaml']
                  for name in names]
    args = individual[0]
    if node == 'N7':
        args += ['-f', '/home/pathfinder/multiq-config-d328726/n7-route-v2.yaml']
        overlay = CONFIG / 'n7-state.json'
        document = {'volumes': {'multiq-n7-route-state': {'name': 'pathfinder-h48-n7-route-state-v1'}}}
        encoded = json.dumps(document, sort_keys=True).encode() + b'\n'
        if overlay.exists():
            assert overlay.read_bytes() == encoded
        else:
            overlay.write_bytes(encoded)
        args += ['-f', str(overlay)]
    # Process environment takes precedence over env files. Only declared public
    # changes override current runtime values; credentials stay identical.
    environment = {**os.environ, **inherited, **nonsecret}
    commands = individual if node == 'N1' else [args]
    rendered = {'services': {}, 'volumes': {}}
    for unit in commands:
        document = json.loads(command(unit + ['--profile', 'serve-frozen', 'config', '--format', 'json'], env=environment))
        rendered['services'].update(document['services'])
        rendered['volumes'].update(document.get('volumes', {}))
    for name, item in zip(names, old):
        service = rendered['services'][name]
        if service['image'] != image or service.get('read_only') is not True:
            raise ValueError('rendered image/security differs')
        current = dict(value.split('=', 1) for value in item['Config']['Env'])
        for key, value in current.items():
            if ('TOKEN' in key or 'SECRET' in key) and service['environment'].get(key) != value:
                raise ValueError('credential would change: ' + key)
        assert all(v.get('read_only') is True for v in service.get('volumes', []) if v['type'] == 'bind')
    print(json.dumps({'status': 'DEPLOYMENT_RENDER_VERIFIED', 'node': node,
                      'image': image, 'credentials_unchanged': True}), flush=True)
    if not apply:
        return
    new_names = [project + '-' + name + '-1' for name in names]
    for name in new_names:
        if subprocess.run(['docker', 'inspect', name], stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL).returncode == 0:
            raise ValueError('fresh container name already exists')
    # Ensure state is new; checking the render is safer than guessing names.
    used_volumes = {v['source'] for name in names
                    for v in rendered['services'][name].get('volumes', [])
                    if v['type'] == 'volume'}
    for key in used_volumes:
        volume = rendered['volumes'][key]
        name = volume['name']
        if subprocess.run(['docker', 'volume', 'inspect', name], stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL).returncode == 0:
            raise ValueError('new state volume already exists')
    stopped = []
    try:
        for item in old:
            command(['docker', 'stop', item['Id']])
            stopped.append(item['Id'])
        for unit, name in zip(commands, names):
            command(unit + ['--profile', 'serve-frozen', 'up', '-d', '--no-deps', '--wait', name], env=environment,
                    stderr=subprocess.STDOUT)
        for name in new_names:
            info = inspect(name)
            assert info['State']['Health']['Status'] == 'healthy'
            assert info['Image'] == image and info['RestartCount'] == 0
        receipt = {'status': 'HEALTHY', 'node': node, 'project': project,
                   'image': image, 'old_container_ids': stopped,
                   'new_container_ids': [inspect(name)['Id'] for name in new_names],
                   'old_containers_preserved': True, 'credentials_unchanged': True,
                   'credentials_recorded': False}
        (CONFIG / (node.lower() + '-receipt.json')).write_bytes(json.dumps(receipt, sort_keys=True).encode() + b'\n')
        print(json.dumps(receipt), flush=True)
    except Exception:
        for name in new_names:
            subprocess.run(['docker', 'stop', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for identity in stopped:
            command(['docker', 'start', identity])
        print('ROLLED_BACK_TO_PRESERVED_SERVICES', flush=True)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('node', choices=tuple(SERVICES))
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    try:
        run(args.node, args.apply)
    except Exception as exc:
        # CalledProcessError can contain rendered credentials; never print it.
        print(json.dumps({'status': 'DEPLOYMENT_STOPPED', 'class': type(exc).__name__}))
        raise SystemExit(2)
