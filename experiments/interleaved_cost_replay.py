"""Evidence-bound list-price accounting and fixed baselines for R/D/DC/I.

No agent training, provider call, or oracle read. Costs are measured-token
list prices plus the declared time allocation, not invoice payments. A replay
may use only cache states actually observed for that question and arm.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from experiments.interleaved_batch import load_config, load_inputs, verify_output
from experiments.multiq_prepare import read, verify_sums, write
from pathfinder.rsi_exam.offline_replay_costing import (
    PRICE_SNAPSHOT, _qwen_cost, _embedding_cost, _usd,
)


def lines(path):
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def dec(value):
    value = Decimal(str(value))
    if not value.is_finite() or value < 0:
        raise ValueError('negative or nonfinite measurement')
    return value


def policy_arm(policy, seen):
    if policy == 'first-R-then-I':
        return 'R' if seen == 0 else 'I'
    if policy in {'always-R', 'always-D', 'always-DC', 'always-I'}:
        return policy[7:]
    raise ValueError('unsupported baseline')


def cached_units(usage):
    details = usage.get('prompt_tokens_details')
    if not isinstance(details, dict) or type(details.get('cached_tokens')) is not int:
        raise ValueError('cached-token measurement is missing, not zero')
    return details['cached_tokens']


def audit(root, routes, paid, export, cloud_dir):
    config, config_sha = load_config(root / 'runtime/config')
    context = load_inputs(config, root)
    verified = verify_output(config, config_sha, context, routes)
    verify_sums(cloud_dir)
    cloud = read(cloud_dir / 'allocation.json')
    rates = {x['host']: dec(x['usd_per_hour']) for x in cloud['hosts']}
    if len(rates) != 9 or any(s['part_of_plan'] != 'yes'
                            for h in cloud['hosts'] for s in h['storage']):
        raise ValueError('cloud inclusion/rate scope differs')
    fleet_rate = sum(rates.values(), Decimal(0))
    spec = read(root / 'runtime/baselines/baseline-spec.json')
    if spec['price_snapshot'] != PRICE_SNAPSHOT:
        raise ValueError('frozen provider prices differ')
    questions = lines(root / 'plan/public-questions.jsonl')
    objects = sorted({x['object_id'] for x in questions})
    builds = {oid: defaultdict(Decimal) for oid in objects}

    def machine(seconds, host):
        return dec(seconds) * rates[host] / Decimal(3600)

    offline_events = [x for x in lines(root / 'build/build-events.jsonl')
                      if x['status'] == 'COMPLETE']
    paid_events = {x['step']: x for x in lines(paid / 'build-events.jsonl')
                   if x['status'] == 'COMPLETE'}
    final_events = [x for x in lines(root / 'final/build-events.jsonl')
                    if x['status'] == 'COMPLETE']
    caption_responses = {x['persisted_response_sha256']: x for x in
                         lines(paid / 'provider-journal/captions-attempts.jsonl')
                         if x['event'] == 'persisted'}
    caption_times = {x['request_sha256']: x for x in
                     lines(paid / 'provider-journal/captions-attempts.jsonl')
                     if x['event'] == 'response'}
    weights = defaultdict(Decimal)
    caption_count = 0
    for file in sorted((paid / 'caption-cache/raw').rglob('*.json')):
        row = read(file)
        oid = row['window_id'].split('#')[0]
        if oid not in builds:
            raise ValueError('caption belongs to another video')
        persisted = caption_responses.pop(row['response_sha256'], None)
        if persisted is None:
            raise ValueError('caption does not bind a unique paid response')
        event = caption_times[persisted['request_sha256']]
        usage = row['usage']
        if usage != event['usage'] or usage['total_tokens'] != (
                usage['prompt_tokens'] + usage['completion_tokens']):
            raise ValueError('caption usage differs from persisted provider response')
        builds[oid]['caption_api_usd'] += _qwen_cost(
            usage['prompt_tokens'], cached_units(usage), usage['completion_tokens'])
        weights[oid] += dec(event['wall_seconds'])
        caption_count += 1
    if caption_responses or caption_count != sum(1 for _ in caption_times):
        raise ValueError('unpriced caption attempt remains')
    caption_machine = machine(paid_events['captions']['wall_seconds'], 'pathfinder-n6')
    for oid in objects:
        builds[oid]['caption_compute_usd'] = caption_machine * weights[oid] / sum(weights.values())
    for event in offline_events:
        if event['step'] in {'raw-import', 'caption-frame-decoding'}:
            category = 'raw_import_compute_usd' if event['step'] == 'raw-import' else 'frame_preparation_compute_usd'
            for oid in objects:
                builds[oid][category] += machine(event['wall_seconds'], event['physical_host']) / len(objects)
    for event in final_events:
        if event['step'].startswith('derived-assembly:'):
            builds[event['step'].split(':')[1]]['derived_assembly_compute_usd'] += machine(event['wall_seconds'], event['physical_host'])
        elif event['step'] == 'n4-package-and-verification':
            for oid in objects:
                builds[oid]['derived_assembly_compute_usd'] += machine(event['wall_seconds'], event['physical_host']) / len(objects)
    index = read(root / 'paid/video-index/video-temporal-index.json')
    index_receipts = index['video_build_embedding_receipts']
    if {r['object_id'] for r in index_receipts} != set(objects):
        raise ValueError('video embedding receipt coverage differs')
    for row in index_receipts:
        builds[row['object_id']]['index_api_usd'] = _embedding_cost(row['usage']['prompt_tokens'])
        builds[row['object_id']]['index_compute_usd'] = machine(paid_events['video-index']['wall_seconds'], 'pathfinder-n6') / len(objects)
    query = read(root / 'paid/query/temporal-query-batch.json')
    per_question = {}
    for row in query['query_embedding_receipts']:
        per_question[row['question_id']] = {
            'embedding_api_usd': _embedding_cost(row['usage']['prompt_tokens']),
            'embedding_compute_usd': machine(paid_events['query']['wall_seconds'], 'pathfinder-n6') / len(questions),
        }
    projection_count = 0
    for event in final_events:
        if event['step'].startswith('projection:build:'):
            qid = event['step'].split(':', 2)[2]
            if 'projection_compute_usd' in per_question[qid]:
                raise ValueError('duplicate query projection construction')
            per_question[qid]['projection_compute_usd'] = machine(event['wall_seconds'], event['physical_host'])
            projection_count += 1
    if projection_count != len(questions):
        raise ValueError('query projection timing coverage differs')

    journal = read(export)
    if journal['schema_version'] != 'pathfinder.n6-numeric-usage-export/v2':
        raise ValueError('usage and attempt trace export required')
    usage_by_result = {x['result_sha256']: x for x in journal['rows']}
    if len(usage_by_result) != len(journal['rows']):
        raise ValueError('duplicate usage result')
    attempts = defaultdict(list)
    for row in journal['attempts']:
        attempts[row['request_sha256']].append(row)
    timings = [read(routes / f'timing-{i:02d}.json') for i in range(verified['route_count'])]
    summary = read(routes / 'summary.json')
    observed_wall = dec((datetime.fromisoformat(summary['ended_utc']) -
                   datetime.fromisoformat(summary['started_utc'])).total_seconds())
    pause_seconds = Decimal(0)
    if (routes / 'continuation.json').is_file():
        continuation = read(routes / 'continuation.json')
        points = continuation.get('previous_continuation_points', []) + [continuation['next_ordinal']]
        if points != sorted(set(points)):
            raise ValueError('continuation points repeat or move backward')
        for resumed in points:
            if resumed < len(timings):
                pause_seconds += dec((datetime.fromisoformat(timings[resumed]['route_started_utc'])
                                      - datetime.fromisoformat(timings[resumed - 1]['route_ended_utc'])).total_seconds())
    elapsed = observed_wall - pause_seconds
    if elapsed < 0:
        raise ValueError('operator pause exceeds observed batch window')
    total_route_ms = sum(dec(t['elapsed_ms']) for t in timings)
    cloud_batch = elapsed * fleet_rate / 3600
    result_rows = []
    missing = []
    for i, timing in enumerate(timings):
        terminal_path = routes / f'terminal-{i:02d}.json'
        if terminal_path.is_file():
            terminal = read(terminal_path)
            route = context['routes'][terminal['trial_key']]
            missing_key = f'provider_rejected_attempt_usage:{i}'
            missing.append(missing_key)
            result_rows.append({
                'ordinal': i, 'trial_key': terminal['trial_key'],
                'question_id': route['question_id'], 'object_id': route['object_id'],
                'arm': context['trials'][i]['design_id'], 'task_success': None,
                'route_status': 'OBSERVED_PROVIDER_REJECTION',
                'elapsed_ms': timing['elapsed_ms'], 'n6_list_price_usd': None,
                'allocated_cloud_usd': _usd(cloud_batch * dec(timing['elapsed_ms']) / total_route_ms),
                'observed_cache_state': None, 'missing_components': [missing_key],
                'input_tokens': None, 'cached_input_tokens': None, 'output_tokens': None,
                'provider_code': terminal['diagnosis']['provider_code'],
            })
            continue
        result = read(routes / f'route-{i:02d}.json')
        evidence = result['semantic_route_evidence']
        semantic = evidence['semantic']
        observation = evidence['neutral_observation_candidate']
        usage = usage_by_result[semantic['result_sha256']]
        if usage['request_sha256'] != semantic['request_sha256']:
            raise ValueError('N6 request binding differs')
        traces = attempts[semantic['request_sha256']]
        row_missing = []
        if (len(traces) != 1 or traces[0]['result_sha256'] != semantic['result_sha256']
                or traces[0]['outcome'] != 'completed' or traces[0]['http_status'] != 200):
            missing.append(f'provider_attempt_usage:{i}')
            row_missing.append(f'provider_attempt_usage:{i}')
        provider = _qwen_cost(usage['input_units'], usage['cached_input_units'], usage['output_units'])
        vm = cloud_batch * dec(timing['elapsed_ms']) / total_route_ms
        branches = evidence.get('cache_branches', [])
        state = None
        if observation['design_id'] == 'DC':
            states = {r['branch'] for r in branches}
            if len(branches) != 2 or len(states) != 1:
                raise ValueError('DC cache state is not a single observed hit/miss state')
            state = next(iter(states))
        result_rows.append({
            'ordinal': i, 'trial_key': result['trial_key'],
            'question_id': context['routes'][result['trial_key']]['question_id'],
            'object_id': observation['object_id'], 'arm': observation['design_id'],
            'task_success': result['task_success'], 'elapsed_ms': timing['elapsed_ms'],
            'route_status': 'COMPLETE', 'missing_components': row_missing,
            'n6_list_price_usd': _usd(provider), 'allocated_cloud_usd': _usd(vm),
            'observed_cache_state': state, 'n6_provider_attempt_count': len(traces),
            'input_tokens': usage['input_units'], 'cached_input_tokens': usage['cached_input_units'],
            'output_tokens': usage['output_units'],
        })
    return {
        'schema_version': 'pathfinder.interleaved-cost-audit/v1',
        'status': 'VERIFIED_KNOWN_COSTS' if not missing else 'VERIFIED_PARTIAL_COSTS',
        'route_count': len(result_rows), 'question_count': len(questions),
        'object_count': len(objects), 'price_snapshot': PRICE_SNAPSHOT,
        'cloud_allocation_sha256': hashlib.sha256((cloud_dir / 'allocation.json').read_bytes()).hexdigest(),
        'vm_batch_wall_seconds': str(elapsed), 'vm_fleet_usd_per_hour': str(fleet_rate),
        'vm_batch_allocated_usd': _usd(cloud_batch),
        'observed_full_window_seconds': str(observed_wall),
        'observed_full_window_fleet_list_price_usd': _usd(observed_wall * fleet_rate / 3600),
        'excluded_operator_pause_seconds': str(pause_seconds),
        'excluded_operator_pause_fleet_list_price_usd': _usd(pause_seconds * fleet_rate / 3600),
        'per_route': result_rows,
        'one_time_builds': {oid: {k: _usd(v) for k, v in row.items()} for oid, row in builds.items()},
        'per_question_builds': {qid: {k: _usd(v) for k, v in row.items()} for qid, row in per_question.items()},
        'missing_components': missing,
        'accounting_boundary': 'Recorded API attempts; active preparation-host wall time; nine-VM route-window allocation including plan disks/network. Retention after the run and operator idle/setup gaps excluded from policy cost.',
        'allocation_disclosures': [
            'Caption machine time apportioned by provider response wall time; includes waiting.',
            'Shared N5 decode and N4 packaging time allocated equally per video, not claimed as separate measurements.',
            'Embedding host overhead allocated equally per video/question.',
            'Index projection verification re-decodes excluded from per-query construction; retained in build journal.',
            'N4 digest/frame bundle materialization is one shared build action; charged once on first D/DC/I use.',
            'Plan-included storage/network are not added again to the VM allocation.',
            'Operator diagnosis pause excluded from policy costs; full-window fleet list price reported separately.',
        ],
        'invoice_payment_claimed': False, 'credentials_recorded': False,
    }


def replay(cost, schedule, policies):
    by_pair = {(r['question_id'], r['arm']): r for r in cost['per_route']}
    if len(by_pair) != len(cost['per_route']):
        raise ValueError('duplicate observed action')
    results = []
    for policy in policies:
        seen, materialized, indexed, cached = defaultdict(int), set(), set(), set()
        raw_imported = set()
        totals = defaultdict(Decimal)
        steps = []
        scoped_missing = {x for r in cost['per_route'] for x in r.get('missing_components', [])}
        policy_missing = set(cost['missing_components']) - scoped_missing
        for question in schedule:
            qid, oid = question['question_id'], question['object_id']
            arm = policy_arm(policy, seen[oid])
            row = by_pair[(qid, arm)]
            if row['object_id'] != oid:
                raise ValueError('replay video identity differs')
            build = cost['one_time_builds'][oid]
            step_build = Decimal(0)
            if oid not in raw_imported:
                step_build += dec(build['raw_import_compute_usd'])
                raw_imported.add(oid)
            if arm != 'R' and oid not in materialized:
                step_build += sum((dec(build[k]) for k in (
                    'caption_api_usd', 'caption_compute_usd',
                    'frame_preparation_compute_usd', 'derived_assembly_compute_usd')), Decimal(0))
                materialized.add(oid)
            if arm == 'I':
                if oid not in indexed:
                    step_build += dec(build['index_api_usd']) + dec(build['index_compute_usd'])
                    indexed.add(oid)
                step_build += sum((dec(v) for v in cost['per_question_builds'][qid].values()), Decimal(0))
            if arm == 'DC':
                expected = 'hit' if oid in cached else 'miss'
                if row['observed_cache_state'] != expected:
                    raise ValueError('unsupported counterfactual cache state')
                cached.add(oid)
            policy_missing.update(row.get('missing_components', []))
            # Zero here is only the known subtotal, never an asserted charge.
            n6 = Decimal(0) if row['n6_list_price_usd'] is None else dec(row['n6_list_price_usd'])
            vm = dec(row['allocated_cloud_usd'])
            totals['n6'] += n6
            totals['cloud'] += vm
            totals['build'] += step_build
            steps.append({'question_id': qid, 'object_id': oid, 'arm': arm,
                          'task_success': row['task_success'],
                          'known_cost_usd': _usd(n6 + vm + step_build),
                          'elapsed_ms': row['elapsed_ms']})
            seen[oid] += 1
        total = sum(totals.values(), Decimal(0))
        results.append({
            'policy': policy, 'question_count': len(steps),
            'correct': sum(x['task_success'] is True for x in steps),
            'incorrect': sum(x['task_success'] is False for x in steps),
            'unavailable': sum(x['task_success'] is None for x in steps),
            'n6_list_price_usd': _usd(totals['n6']), 'allocated_cloud_usd': _usd(totals['cloud']),
            'one_time_and_query_preparation_usd': _usd(totals['build']),
            'known_total_usd': _usd(total),
            'total_within_declared_boundary_usd': None if policy_missing else _usd(total),
            'missing_components': sorted(policy_missing),
            'sum_observed_route_seconds': str(sum(dec(x['elapsed_ms']) for x in steps) / 1000),
            'steps': steps,
        })
    return {'status': 'FIXED_BASELINES_REPLAYED', 'policies': results,
            'latency_counterfactual_claimed': False, 'agent_trained': False,
            'test_outcomes_used_for_action_selection': False,
            'eligible_for_scientific_claims': False, 'credentials_recorded': False}


def main():
    parser = argparse.ArgumentParser()
    for name in ('artifact-root', 'routes', 'paid-evidence', 'n6-export', 'cloud-allocation', 'output-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise ValueError('fresh accounting output required')
    cost = audit(args.artifact_root, args.routes, args.paid_evidence, args.n6_export, args.cloud_allocation)
    spec = read(args.artifact_root / 'runtime/baselines/baseline-spec.json')
    result = replay(cost, lines(args.artifact_root / 'plan/interleaved-schedule.jsonl'), spec['policies'])
    args.output_dir.mkdir(parents=True)
    write(args.output_dir / 'cost-audit.json', cost)
    write(args.output_dir / 'baseline-replay.json', result)
    payload = b''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n'.encode()
                       for p in sorted(args.output_dir.iterdir()))
    (args.output_dir / 'SHA256SUMS').write_bytes(payload)
    print(json.dumps({'status': result['status'], 'routes': cost['route_count'],
                      'missing_components': cost['missing_components']}))


if __name__ == '__main__':
    main()
