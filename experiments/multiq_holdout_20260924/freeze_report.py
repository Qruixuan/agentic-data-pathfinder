"""Freeze a public, self-contained fixed-baseline replay/report bundle."""
import hashlib
import json
from pathlib import Path
import subprocess

from experiments.interleaved_cost_replay import replay

ROOT = Path(__file__).resolve().parents[2]
DEST = ROOT / 'artifacts/h48-proposal-baseline-report-v1'


def read(path):
    return json.loads(path.read_bytes())


def pretty(value):
    return json.dumps(value, indent=2, sort_keys=True).encode() + b'\n'


def main():
    assert not DEST.exists()
    costroot = ROOT / 'artifacts/h48-cost-replay-v2'
    for line in (costroot / 'SHA256SUMS').read_text().splitlines():
        digest, name = line.split('  ', 1)
        assert hashlib.sha256((costroot / name).read_bytes()).hexdigest() == digest
    cost = read(costroot / 'cost-audit.json')
    results = read(costroot / 'baseline-replay.json')
    public = ROOT / 'artifacts/h48-runtime-v1'
    schedule_bytes = (public / 'plan/interleaved-schedule.jsonl').read_bytes()
    schedule = [json.loads(line) for line in schedule_bytes.splitlines()]
    spec = read(public / 'runtime/baselines/baseline-spec.json')
    assert replay(cost, schedule, spec['policies']) == results
    lineage_path = ROOT / '.codex_build/h48-flowmesh-lineage-final-v2.jsonl'
    assert hashlib.sha256(lineage_path.read_bytes()).hexdigest() == 'ca0995482aaace001050eeb88c5edcb164b027796f0c9e8ad11c6fe2cb73614a'
    lineage = json.loads(lineage_path.read_bytes().splitlines()[-1])
    assert lineage['workflow_count'] == 48 and lineage['duplicate_submissions'] == 0
    by_key = {r['trial_key']: r for r in lineage['rows']}
    for row in cost['per_route']:
        assert by_key[row['trial_key']]['workflow_status'] == (
            'FAILED' if row['task_success'] is None else 'DONE')
    export = ROOT / '.codex_build/h48-n6-accounting-final.json'
    assert hashlib.sha256(export.read_bytes()).hexdigest() == cost['n6_numeric_export_sha256']
    payloads = {name: (costroot / name).read_bytes() for name in
                ('REPORT.md', 'cost-audit.json', 'baseline-replay.json')}
    payloads.update({
        'interleaved-schedule.jsonl': schedule_bytes,
        'baseline-spec.json': pretty(spec), 'flowmesh-lineage.json': pretty(lineage),
        'n6-numeric-usage.json': export.read_bytes(),
        'execution-protocol.json': (ROOT / 'experiments/multiq_holdout_20260924/execution-protocol.json').read_bytes(),
        'selection-protocol.json': (ROOT / 'artifacts/multiq-fresh-holdout-20260924-v3/selection-protocol.json').read_bytes(),
    })
    verification = {
        'status': 'VERIFIED_FROZEN_BASELINE_REPLAY_PILOT',
        'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
        'route_slots': 48, 'completed': 44, 'provider_rejections': 4,
        'baseline_count': len(results['policies']), 'offline_replay_reproduced': True,
        'unique_workflows': 48, 'workflow_retries': 0,
        'credentials_recorded': False, 'hidden_label_values_included': False,
        'eligible_for_scientific_claims': False,
        'full_cost_known': False, 'missing_components': cost['missing_components'],
    }
    payloads['verification.json'] = pretty(verification)
    for data in payloads.values():
        lower = data.lower()
        assert b'bearer ' not in lower and b'api_key' not in lower
        assert b'authorization' not in lower and b'hidden-labels.json' not in lower
    DEST.mkdir()
    for name, data in payloads.items():
        (DEST / name).write_bytes(data)
    checksums = b''.join(f'{hashlib.sha256(data).hexdigest()}  {name}\n'.encode()
                         for name, data in sorted(payloads.items()))
    (DEST / 'SHA256SUMS').write_bytes(checksums)
    for name, data in payloads.items():
        assert (DEST / name).read_bytes() == data
    print(json.dumps(verification, sort_keys=True))


if __name__ == '__main__':
    main()
