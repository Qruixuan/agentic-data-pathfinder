"""Health and invalid-body auth probes only; zero artifact or model access."""
import json
import os
from urllib.request import Request, urlopen
from urllib.error import HTTPError

specs = [
    ('N1', 'PATHFINDER_N1_ORACLE_BASE_URL', '/v1/oracle/score', 'PATHFINDER_N1_ORACLE_TOKEN', 'POST'),
    ('N1-verifier', 'PATHFINDER_N1_VERIFICATION_BASE_URL', '/v1/oracle/verify-score', 'PATHFINDER_N1_VERIFICATION_TOKEN', 'POST'),
    ('N2', 'PATHFINDER_N2_INDEX_BASE_URL', '/v1/index/query', 'PATHFINDER_N2_INDEX_TOKEN', 'POST'),
    ('N3', 'PATHFINDER_N3_DATA_AGENT_BASE_URL', '/v1/access', 'PATHFINDER_N3_DATA_AGENT_TOKEN', 'POST'),
    ('N4', 'PATHFINDER_N4_DATA_AGENT_BASE_URL', '/v1/access', 'PATHFINDER_N4_DATA_AGENT_TOKEN', 'POST'),
    ('N6', 'PATHFINDER_N6_SEMANTIC_BASE_URL', '/v1/semantic/chat-completions', 'PATHFINDER_CONTAINER_NODE_TOKEN', 'POST'),
    ('N7-cache', 'PATHFINDER_N7_CACHE_BASE_URL', '/v1/cache/artifact', 'PATHFINDER_FULL_FLOW_CACHE_TOKEN', 'PUT'),
]
results = []
for name, origin_key, endpoint, token_key, method in specs:
    origin = os.environ[origin_key].rstrip('/')
    with urlopen(origin + '/healthz', timeout=8) as response:
        health = json.load(response)
    assert health['status'] == 'ok'
    if name == 'N6':
        assert health['semantic_usage_journal_error_count'] == 0
    codes = []
    for token in (os.environ[token_key], 'invalid-preflight-token'):
        request = Request(origin + endpoint, data=b'{}', method=method,
                          headers={'Authorization': 'Bearer ' + token,
                                   'Content-Type': 'application/json'})
        try:
            with urlopen(request, timeout=8) as response:
                codes.append(response.status)
        except HTTPError as error:
            codes.append(error.code)
            error.read(4096)
    assert codes == [400, 401], (name, codes)
    results.append({'service': name, 'valid_token_bad_body': 400, 'invalid_token': 401})
print(json.dumps({'status': 'AUTH_AND_USAGE_GATES_VERIFIED', 'checks': results,
                  'llm_called': False, 'credentials_recorded': False}))
