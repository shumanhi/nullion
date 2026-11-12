import json

import pytest
from jsonschema import validate, ValidationError

from tests.test_live_tracking import fixture, create
from nullion.live_tracking import MAX_INTERVAL, observation_preparation, Observation, utcnow
from datetime import datetime, timedelta
from nullion.live_tracking_tools import register_live_tracker_tools
from nullion.tools import ToolInvocation


@pytest.mark.parametrize('interval', [5, 60, 300, 900, 1800, 3600, MAX_INTERVAL])
def test_interval_schema_handler_and_store_agree(fixture, interval):
    f = fixture
    f.registry.live_tracker_store_path = f.store.path
    register_live_tracker_tools(f.registry)
    step = {'tool_name': 'sensor_read', 'arguments': {}}
    signature = json.dumps(step, ensure_ascii=False, sort_keys=True, default=str)
    args = {'name': 'Selected observation', 'interval_seconds': interval, 'duration_seconds': (7*86400),
        'probe': [step], 'matcher': {'kind': 'structured', 'conditions': [
            {'step': 0, 'pointer': '/reading', 'op': 'greater_than', 'value': 20}], 'summary': 'Condition met.'}}
    validate(args, f.registry.get_spec('create_live_tracker').input_schema)
    result = f.registry.invoke(ToolInvocation('create', 'create_live_tracker', 'web:alpha', args,
        flow_context={'conversation_id': 'web:alpha', 'prepared_observations': {signature: {'reading': 23}},
            'observation_preparations': {signature: observation_preparation(step, {'reading': 23})}}))
    assert result.status == 'completed', result
    assert result.output['interval_seconds'] == interval
    job = f.store.get(result.output['id'])
    assert job['interval_seconds'] == interval and job['state'] == 'active'


@pytest.mark.parametrize('interval', [4, MAX_INTERVAL+1])
def test_interval_boundaries_remain_enforced(fixture, interval):
    f = fixture
    f.registry.live_tracker_store_path = f.store.path
    register_live_tracker_tools(f.registry)
    args = {'name': 'Selected observation', 'interval_seconds': interval, 'duration_seconds': (7*86400),
        'probe': [{'tool_name': 'sensor_read', 'arguments': {}}],
        'matcher': {'kind': 'structured', 'conditions': [{'step': 0, 'pointer': '/reading', 'op': 'exists'}], 'summary': 'Condition met.'}}
    with pytest.raises(ValidationError):
        validate(args, f.registry.get_spec('create_live_tracker').input_schema)
    result = f.registry.invoke(ToolInvocation('create', 'create_live_tracker', 'web:alpha', args))
    assert result.status == 'failed' and f.store.list('workspace_admin') == []


@pytest.mark.parametrize('interval', [60, 300, 900, 3600])
def test_failed_checks_never_poll_faster_than_the_configured_interval(fixture, interval):
    now = utcnow()
    job = create(fixture, now=now, interval_seconds=interval, expires_at=now+timedelta(seconds=(7*86400)))
    claimed, token = fixture.store.claim(4, now=now)
    assert claimed['id'] == job['id']
    assert fixture.store.finish(job['id'], token, Observation('source_error'), now=now)
    saved = fixture.store.get(job['id'])
    assert (datetime.fromisoformat(saved['next_check_at'])-now).total_seconds() >= interval
