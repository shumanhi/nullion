"""Opt-in replay through the real stage Web UI with backing SQLite evidence.

Set NULLION_LIVE_STAGE_E2E=1 and NULLION_STAGE_CRON_ID to an authorized,
paused task. For an action case also set NULLION_STAGE_CRON_EXPECTED_WRITE;
for a monitor set NULLION_STAGE_CRON_EXPECTED_MODE=verified_matches_only.
The replay never approves account actions or enables a schedule.
"""
import json
import os
from pathlib import Path
import sqlite3
import time
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get('NULLION_LIVE_STAGE_E2E') != '1' or not os.environ.get('NULLION_STAGE_CRON_ID'),
    reason='Requires an explicitly selected paused stage task and real provider quota',
)


def test_live_stage_saved_cron_has_valid_scope_and_visible_terminal_outcome():
    from playwright.sync_api import sync_playwright, expect
    cron_id = os.environ['NULLION_STAGE_CRON_ID']
    expected_write = os.environ.get('NULLION_STAGE_CRON_EXPECTED_WRITE')
    expected_mode = os.environ.get('NULLION_STAGE_CRON_EXPECTED_MODE')
    db = Path(os.environ.get('NULLION_LIVE_RUNTIME_DB', str(Path.home()/'.nullion-stage/runtime.db')))
    base = os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753')
    assert db.is_file()
    def records(table, since):
        with sqlite3.connect(f'{db.as_uri()}?mode=ro',uri=True) as conn:
            return [json.loads(row[0]) for row in conn.execute(
                f'SELECT payload FROM {table} WHERE updated_at>=? ORDER BY updated_at', (since,))]
    started = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(f'{db.as_uri()}?mode=ro',uri=True) as conn:
        job = json.loads(conn.execute("SELECT payload FROM reminders_crons WHERE collection='cron_jobs' AND item_key=?",
                                     (cron_id,)).fetchone()[0])
    assert job['enabled'] is False, 'Select a paused task; this replay does not alter schedules'
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(base)
        box = page.locator('#user-input')
        expect(box).to_be_enabled(timeout=30000)
        box.fill(f'Run task id {cron_id} once. Keep it paused.')
        page.get_by_role('button',name='Send',exact=True).click()
        deadline = time.monotonic()+240
        terminal = None
        while time.monotonic()<deadline:
            events = [event for event in records('runtime_events',started)
                      if event.get('payload',{}).get('cron_id')==cron_id]
            terminal = next((event for event in reversed(events) if event.get('event_type') in {
                'cron.delivery.paused','cron.delivery.saved','cron.delivery.sent','cron.delivery.failed'}),None)
            if terminal: break
            page.wait_for_timeout(500)
        assert terminal, 'The UI-triggered task never reached a terminal/approval boundary'
        assert terminal['event_type']!='cron.delivery.failed',terminal
        scope = next(event['payload'] for event in events
                     if event.get('payload',{}).get('stage')=='connector_scope')
        if expected_write:
            assert expected_write in scope['selected_write_tools']
            assert terminal['event_type']=='cron.delivery.paused'
            approvals = [entry for entry in records('approvals',started)
                         if entry.get('resource')==expected_write]
            assert approvals, 'The approval must survive a real checkpoint/reload'
            page.reload()
            expect(page.get_by_role('button',name='Allow once',exact=True).first).to_be_visible(timeout=15000)
        if expected_mode:
            assert scope['alert_delivery_mode']==expected_mode
            decisions = [event['payload'] for event in events if event.get('payload',{}).get('stage')=='alert_delivery_policy_decided']
            assert decisions and decisions[-1]['reason'] in {'verified_match','no_verified_match'},decisions
        browser.close()
    with sqlite3.connect(f'{db.as_uri()}?mode=ro',uri=True) as conn:
        after = json.loads(conn.execute("SELECT payload FROM reminders_crons WHERE collection='cron_jobs' AND item_key=?",
                                       (cron_id,)).fetchone()[0])
    assert after['enabled'] is False
