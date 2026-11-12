from datetime import UTC, datetime
import json
import os

import pytest

from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_settings_tab

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker browser E2E only')


def app_and_store(tmp_path, monkeypatch, zone='America/New_York'):
    from nullion import preferences, chat_store, crons, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setattr(preferences, '_PREFS_PATH', tmp_path / 'preferences.json')
    monkeypatch.setattr(preferences, '_PROFILE_PATH', tmp_path / 'profile.json')
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', tmp_path / 'runtime.db')
    monkeypatch.setattr(chat_store, '_store', None)
    preferences.save_preferences(preferences.Preferences(timezone=zone, time_format='12h'))
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    registry = ToolRegistry()
    return web_app.create_app(runtime, orchestrator=None, registry=registry), chat_store.get_chat_store()


def save_at(store, cid, text, instant):
    ident = store.save_message(cid, 'bot', text, metadata={'created_at': instant})
    with store._connect() as db:
        db.execute('UPDATE messages SET created_at=? WHERE id=?', (instant, ident))


@pytest.mark.parametrize('zone,label', [('America/New_York', '4:05 PM'), ('Asia/Kolkata', '2:35 AM')])
def test_real_web_history_uses_saved_zone_and_am_pm_with_foreign_browser_locale(tmp_path, monkeypatch, zone, label):
    from playwright.sync_api import sync_playwright, expect
    app, store = app_and_store(tmp_path, monkeypatch, zone)
    cid = 'web:時間表示'
    iso = '2026-11-04T21:05:00+00:00'
    save_at(store, cid, '確認した予定', iso)
    with _UvicornThread(app, _free_port()) as server, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(locale='en-GB', timezone_id='Asia/Tokyo')
            context.add_init_script('localStorage.setItem("nullion_conv_id", ' + json.dumps(cid) + ');')
            page = context.new_page()
            page.goto(server.base_url)
            bubble = page.locator('.msg.bot .bubble').filter(has_text='確認した予定')
            expect(bubble).to_be_visible()
            expect(bubble.locator('.message-time')).to_have_text(label)
            assert page.evaluate('userTimePreferences().timezone') == zone
            assert store.load_messages(cid)[0]['metadata']['created_at'] == iso
            assert page.evaluate("formatUserTime('2026-10-30T20:05:00Z')") == ('4:05 PM' if zone == 'America/New_York' else '1:35 AM')
            assert page.evaluate("formatUserDate('2026-11-05')") == 'Nov 5, 2026'
            assert page.evaluate("""() => {
                const saved = window.__NULLION_TIME_PRESENTATION__;
                try {
                    window.__NULLION_TIME_PRESENTATION__ = {...saved, timezone: 'UTC', time_format: '24h'};
                    return formatUserTime('2026-11-04T00:05:00Z');
                } finally { window.__NULLION_TIME_PRESENTATION__ = saved; }
            }""") == '00:05'
            if zone == 'America/New_York':
                assert page.evaluate("_localDateKey(new Date('2026-11-04T02:00:00Z'))") == '2026-11-03'
                assert page.evaluate("formatUserTime('2026-11-04T21:05:00')") == '4:05 PM'
            page.reload()
            expect(page.locator('.msg.bot .bubble').filter(has_text='確認した予定').locator('.message-time')).to_have_text(label)
        finally:
            browser.close()


def test_settings_save_changes_visible_clock_without_changing_stored_message_instant(tmp_path, monkeypatch):
    from playwright.sync_api import sync_playwright, expect
    from nullion import preferences
    app, store = app_and_store(tmp_path, monkeypatch)
    cid = 'web:time-settings'
    iso = '2026-11-04T21:05:00+00:00'
    save_at(store, cid, 'Horario confirmado', iso)
    with _UvicornThread(app, _free_port()) as server, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(locale='en-GB', timezone_id='Asia/Tokyo')
            context.add_init_script('localStorage.setItem("nullion_conv_id", ' + json.dumps(cid) + ');')
            page = context.new_page(); page.goto(server.base_url)
            bubble = page.locator('.msg.bot .bubble').filter(has_text='Horario confirmado')
            expect(bubble.locator('.message-time')).to_have_text('4:05 PM')
            _open_settings_tab(page, 'Preferences')
            page.locator('#pref-timezone').fill('UTC')
            page.locator('#pref-timeformat').select_option('12h')
            page.locator('.modal-footer button:has-text("Save changes")').click()
            expect(page.locator('#save-feedback')).to_have_text('✓ Saved', timeout=10000)
            expect(bubble.locator('.message-time')).to_have_text('9:05 PM')
            assert preferences.load_preferences().timezone == 'UTC'
            assert store.load_messages(cid)[0]['metadata']['created_at'] == iso
        finally:
            browser.close()


@pytest.mark.parametrize('bad', [{'timezone': 'No/SuchZone'}, {'time_format': 'military'},
    {'date_format': []}, {'timezone': ''}, []])
def test_invalid_time_preferences_leave_saved_settings_unchanged(tmp_path, monkeypatch, bad):
    from fastapi.testclient import TestClient
    from nullion import preferences
    app, _ = app_and_store(tmp_path, monkeypatch)
    before = preferences._PREFS_PATH.read_bytes()
    with TestClient(app) as client:
        reply = client.post('/api/preferences', json=bad)
        assert reply.status_code == 400
        assert preferences._PREFS_PATH.read_bytes() == before


def test_history_date_buckets_respect_explicit_utc_on_non_utc_host(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import preferences
    app, store = app_and_store(tmp_path, monkeypatch, 'UTC')
    monkeypatch.setattr(preferences, 'detect_system_timezone', lambda default='UTC': 'America/New_York')
    store.save_message('web:local-calendar', 'bot', '確認', metadata={'created_at': '2026-11-04T02:05:00+00:00'})
    # Stored event instants remain UTC; the real history route owns day grouping.
    with store._connect() as db:
        db.execute("UPDATE messages SET created_at=?", ('2026-11-04T02:05:00+00:00',))
    with TestClient(app) as client:
        days = client.get('/api/history/calendar/web?month=2026-11').json()['days']
        assert days == {'2026-11-04': 1}


def test_cron_api_reports_winter_and_half_hour_local_schedules_without_rescheduling(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import crons, preferences
    app, _ = app_and_store(tmp_path, monkeypatch)
    job = crons.add_cron('予定', '0 20 * * 1-5', '確認', enabled=False, schedule_timezone='UTC')
    job.next_run = '2026-11-04T20:00:00+00:00'; crons.save_crons([job])
    with TestClient(app) as client:
        row = next(r for r in client.get('/api/crons').json() if r['id'] == job.id)
        assert '3:00 PM' in row['schedule_description']
        assert row['schedule_timezone'] == 'UTC' and row['schedule'] == '0 20 * * 1-5'
        assert row['next_run'] == job.next_run
        prefs = preferences.load_preferences(); prefs.timezone = 'Asia/Kolkata'; preferences.save_preferences(prefs)
        updated = client.patch('/api/crons/' + job.id, json={'schedule': '45 23 * * 1-5', 'schedule_timezone': 'UTC'})
        assert updated.status_code == 200
        row = next(r for r in client.get('/api/crons').json() if r['id'] == job.id)
        assert '5:15 AM' in row['schedule_description']
        assert 'Saturday' in row['schedule_description']
