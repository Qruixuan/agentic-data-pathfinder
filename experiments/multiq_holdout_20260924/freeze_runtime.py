"""Freeze the shared runner configuration and predeclared baselines offline."""
import hashlib
import json
from pathlib import Path
import sys

from experiments.interleaved_batch import freeze_config, freeze_inputs, load_inputs
from experiments.multiq_prepare import read, write, measured
from pathfinder.rsi_exam.offline_replay_costing import PRICE_SNAPSHOT

root = Path(sys.argv[1])
protocol = read(Path(sys.argv[2]))
plan = read(root / 'plan/interleaved-plan.json')
assert plan['plan_sha256'] == protocol['plan_sha256']
config = {
    'schema_version': 'pathfinder.interleaved-batch-config/v1',
    'admission_dir': 'runtime/admission',
    'source_dirs': {
        'trial_dag_dir': 'runtime/dags', 'binding_dir': 'runtime/bindings',
        'plan_dir': 'plan', 'n1_public_commitment_dir': 'commitment',
        'n2_index_package_dir': 'final/n2', 'n3_package_dir': 'final/n3',
        'raw_package_dir': 'build/raw', 'n4_package_dir': 'final/n4',
        'query_dir': 'paid/query', 'video_index_dir': 'paid/video-index',
        'preparation_dir': 'build/preparation', 'caption_dir': 'paid/captions',
    },
    'coordinator_base_url': 'http://10.70.0.17:18780',
    'worker_alias': 'pathfinder_costaware_20260815a',
    'worker_node_alias': 'pathfinder-n7',
    'task_timeout_seconds': protocol['task_timeout_seconds'],
    'expected_plan_sha256': plan['plan_sha256'],
    'expected_question_count': protocol['question_count'],
    'expected_route_count': protocol['route_count'],
    'baseline_spec_dir': 'runtime/baselines',
}
runtime = root / 'runtime'
runtime.mkdir(exist_ok=False)
write(runtime / 'draft-config.json', config)
freeze_config(runtime / 'draft-config.json', runtime / 'config')
with measured(runtime, 'canonical-runtime-freeze-and-verification', 'pathfinder-n6'):
    report = freeze_inputs(config, root)
    baseline = {
        'schema_version': 'pathfinder.frozen-interleaved-baselines/v1',
        'status': 'FROZEN_BEFORE_SEALED_TEST_OUTCOMES',
        'admission_sha256': report['admission_sha256'],
        'plan_sha256': plan['plan_sha256'],
        'policies': protocol['baseline_policies'],
        'first_R_then_I': protocol['first_R_then_I'],
        'replay_matching': protocol['replay_matching'],
        'price_snapshot': PRICE_SNAPSHOT,
        'vm_allocation': protocol['vm_allocation'],
        'unknown_cost_policy': protocol['unknown_cost_policy'],
        'protocol_sha256': hashlib.sha256(Path(sys.argv[2]).read_bytes()).hexdigest(),
        'outcomes_used': False,
        'credentials_recorded': False,
    }
    target = runtime / 'baselines'
    target.mkdir()
    write(target / 'baseline-spec.json', baseline)
    digest = hashlib.sha256((target / 'baseline-spec.json').read_bytes()).hexdigest()
    (target / 'SHA256SUMS').write_bytes(f'{digest}  baseline-spec.json\n'.encode())
    loaded = load_inputs(config, root)
print(json.dumps(report, sort_keys=True))
