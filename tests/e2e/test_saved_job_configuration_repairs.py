"""Saved-job repairs use real APIs/persistence and preserve unrelated settings."""
from datetime import datetime
import os
from zoneinfo import ZoneInfo

import pytest

from tests.e2e.test_tracker_status_delivery import setup

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('label', ['保存済み通知', 'Aviso guardado'])
def test_payment_context_time_zone_and_expired_alert_repairs_are_scoped(tmp_path, monkeypatch, label):
    from fastapi.testclient import TestClient
    from nullion.crons import _compute_job_next_run, get_cron
    app, runtime, db = setup(tmp_path, monkeypatch)
    with TestClient(app) as client:
        def create(suffix, **fields):
            payload = {'name': label + suffix, 'task': '確認した記録から通知。', 'schedule': '0 0 1 1 *',
                'schedule_timezone': 'UTC', 'enabled': True, 'delivery_channel': 'web',
                'delivery_target': 'web:fixture', **fields}
            response = client.post('/api/crons', json=payload)
            assert response.status_code == 200, response.text
            return response.json()['job']
        payment = create('1', task='Read current account records. Historical example due 2026-06-07.')
        market = create('2', schedule='0 17 * * 1-5')
        train = create('3', schedule='* * 23 10 *', schedule_timezone='America/New_York')
        other = create('4')
        updated_payment = 'Read current account records. Resolve upcoming and overdue obligations from current evidence at this run time.'
        edits = [(payment, {'task': updated_payment}),
                 (market, {'schedule': '0 13 * * 1-5', 'schedule_timezone': 'America/New_York'}),
                 (train, {'enabled': False})]
        for before, patch in edits:
            response = client.patch('/api/crons/' + before['id'], json=patch)
            assert response.status_code == 200, response.text
            after = response.json()['job']
            for key in ['name', 'task', 'schedule', 'schedule_timezone', 'enabled', 'workspace_id',
                        'delivery_channel', 'delivery_target', 'execution_window', 'artifact_delivery_options']:
                assert after.get(key) == patch.get(key, before.get(key)), key
        assert get_cron(payment['id']).task == updated_payment
        assert get_cron(train['id']).enabled is False and get_cron(train['id']).next_run is None
        assert get_cron(other['id']).to_dict() == other
        job = get_cron(market['id'])
        for month, utc_hour in [(1, 18), (7, 17)]:
            after = datetime(2027, month, 15, 9, tzinfo=ZoneInfo('America/New_York'))
            fire = datetime.fromisoformat(_compute_job_next_run(job, after=after))
            assert fire.astimezone(ZoneInfo('America/New_York')).hour == 13
            assert fire.astimezone(ZoneInfo('UTC')).hour == utc_hour
