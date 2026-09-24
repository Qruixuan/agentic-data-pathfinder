import copy
from decimal import Decimal
import unittest

from experiments.interleaved_cost_replay import (
    replay, cached_units, policy_arm, render_report,
)


class CostReplayTests(unittest.TestCase):
    def fixture(self):
        rows = [{
            'question_id': q, 'object_id': 'video', 'arm': arm,
            'task_success': arm != 'R', 'n6_list_price_usd': '1',
            'allocated_cloud_usd': '0.2', 'elapsed_ms': 1000,
            'observed_cache_state': ('miss' if q == 'q1' else 'hit') if arm == 'DC' else None,
        } for q in ('q1', 'q2') for arm in ('I', 'R', 'D', 'DC')]
        cost = {
            'per_route': rows, 'missing_components': [],
            'one_time_builds': {'video': {
                'raw_import_compute_usd': '0.1', 'caption_api_usd': '2',
                'caption_compute_usd': '0.2', 'frame_preparation_compute_usd': '0.3',
                'derived_assembly_compute_usd': '0.4', 'index_api_usd': '0.5',
                'index_compute_usd': '0.1',
            }},
            'per_question_builds': {q: {'embedding_api_usd': '0.1',
                                        'embedding_compute_usd': '0.2',
                                        'projection_compute_usd': '0.3'}
                                    for q in ('q1', 'q2')},
        }
        schedule = [{'question_id': q, 'object_id': 'video'} for q in ('q1', 'q2')]
        return cost, schedule

    def test_shared_materialization_once_and_question_cost_only_for_index(self):
        cost, schedule = self.fixture()
        result = replay(cost, schedule, ['always-R', 'always-D', 'always-DC', 'always-I', 'first-R-then-I'])
        expected = ['2.5', '5.4', '5.4', '7.2', '6.6']
        for row, total in zip(result['policies'], expected):
            self.assertEqual(Decimal(row['known_total_usd']), Decimal(total))
        self.assertEqual([x['arm'] for x in result['policies'][-1]['steps']], ['R', 'I'])

    def test_replay_joins_identity_not_ordinal_modulo(self):
        cost, schedule = self.fixture()
        expected = replay(cost, schedule, ['always-I'])
        cost['per_route'].reverse()
        self.assertEqual(replay(cost, schedule, ['always-I']), expected)

    def test_mismatched_cache_state_fails_closed(self):
        cost, schedule = self.fixture()
        next(r for r in cost['per_route'] if r['arm'] == 'DC')['observed_cache_state'] = 'hit'
        with self.assertRaisesRegex(ValueError, 'counterfactual'):
            replay(cost, schedule, ['always-DC'])

    def test_missing_attempt_cost_keeps_total_unknown(self):
        cost, schedule = self.fixture()
        cost['missing_components'] = ['provider-attempt']
        row = replay(cost, schedule, ['always-R'])['policies'][0]
        self.assertIsNone(row['total_within_declared_boundary_usd'])
        self.assertGreater(Decimal(row['known_total_usd']), 0)

    def test_unknown_cache_token_count_is_not_zero(self):
        with self.assertRaisesRegex(ValueError, 'missing'):
            cached_units({'prompt_tokens': 4})
        self.assertEqual(cached_units({'prompt_tokens_details': {'cached_tokens': 0}}), 0)

    def test_provider_rejection_is_unavailable_not_incorrect_or_free(self):
        cost, schedule = self.fixture()
        row = next(r for r in cost['per_route'] if r['arm'] == 'I')
        row.update(task_success=None, n6_list_price_usd=None,
                   missing_components=['provider-rejection:0'])
        cost['missing_components'] = ['provider-rejection:0']
        result = replay(cost, schedule, ['always-I', 'always-R'])['policies']
        self.assertEqual(result[0]['unavailable'], 1)
        self.assertEqual(result[0]['incorrect'], 0)
        self.assertIsNone(result[0]['total_within_declared_boundary_usd'])
        self.assertIsNotNone(result[1]['total_within_declared_boundary_usd'])

    def test_report_uses_actual_cohort_size_and_attempt_boundary(self):
        cost, schedule = self.fixture()
        for row in cost['per_route']:
            row.update(model_input_bytes=1024,
                       semantic_content_sha256='same-input')
        cost.update(
            object_count=1, question_count=2,
            vm_batch_allocated_usd='0.01',
            excluded_operator_pause_fleet_list_price_usd='0',
            observed_full_window_fleet_list_price_usd='0.02',
            observed_preparation_to_finish={
                'started_utc': '2026-09-24T00:00:00Z',
                'ended_utc': '2026-09-24T00:01:00Z',
                'known_all_api_list_price_usd': '1',
                'fleet_list_price_usd': '0.02',
                'known_fleet_plus_api_usd': '1.02',
            },
        )
        report = render_report(cost, replay(cost, schedule, ['always-R']))
        self.assertIn('1 video means 1 independent video unit', report)
        self.assertIn('not 2 independent video units', report)
        self.assertIn('Missing provider-attempt usage is unknown', report)
        self.assertNotIn('Four videos', report)


if __name__ == '__main__':
    unittest.main()
