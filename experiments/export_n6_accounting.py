"""Read-only numeric usage and hashed request/result attempt metadata export."""
import argparse
import json
from pathlib import Path
import sqlite3


def query(path, sql):
    if not path.is_file():
        raise ValueError('N6 accounting database is missing')
    connection = sqlite3.connect(f'file:{path.as_posix()}?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute('PRAGMA query_only=ON')
        return [dict(r) for r in connection.execute(sql)]
    finally:
        connection.close()


def export(root):
    rows = query(root / 'n6-provider-usage-v1.sqlite3', '''
        SELECT result_sha256, request_sha256, input_units, cached_input_units,
               output_units, total_units FROM n6_provider_usage ORDER BY result_sha256
    ''')
    attempts = query(root / 'n6-provider-trace-v1.sqlite3', '''
        SELECT request_sha256, result_sha256, attempt_index, outcome, http_status
        FROM n6_provider_attempts ORDER BY request_sha256, trace_id, attempt_index
    ''')
    for row in rows:
        assert row['total_units'] == row['input_units'] + row['output_units']
        assert 0 <= row['cached_input_units'] <= row['input_units']
    return {'schema_version': 'pathfinder.n6-numeric-usage-export/v2',
            'record_count': len(rows), 'rows': rows, 'attempts': attempts,
            'credentials_recorded': False, 'provider_ids_included': False,
            'prompts_or_answers_included': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--state-dir', type=Path, default=Path('/state'))
    args = parser.parse_args()
    print(json.dumps(export(args.state_dir), sort_keys=True))
