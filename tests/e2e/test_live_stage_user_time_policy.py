"""Focused live Web time display acceptance, with no provider-dependent task."""
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Explicit live stage UI acceptance')


def test_live_stage_web_uses_saved_time_policy_in_foreign_browser():
    from playwright.sync_api import sync_playwright, expect
    from tests.e2e.test_live_stage_web_matrix import catalogue
    base = os.environ['NULLION_LIVE_BASE_URL']
    assert urlsplit(base).port == 8753, 'Stage only'
    cid = 'web:live-ui-audit:time-' + uuid4().hex
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ident = None
        try:
            context = browser.new_context(locale='en-GB', timezone_id='Asia/Tokyo')
            context.add_init_script('localStorage.setItem("nullion_conv_id", ' + json.dumps(cid) + ');')
            page = context.new_page(); page.goto(base)
            expect(page.locator('#user-input')).to_be_visible()
            prefs = page.request.get(base + '/api/preferences').json()
            policy = page.evaluate('userTimePreferences()')
            assert policy['timezone'] == prefs['timezone']
            assert policy['time_format'] == prefs['time_format']
            page.locator('#user-input').fill('/help')
            page.locator('#user-input').press('Enter')
            bubble = page.locator('.msg.user .bubble').filter(has_text='/help').last
            expect(bubble).to_be_visible()
            clock = bubble.locator('.message-time')
            expect(clock).to_be_visible()
            instant = clock.get_attribute('datetime')
            local = datetime.fromisoformat(instant.replace('Z', '+00:00')).astimezone(ZoneInfo(prefs['timezone']))
            expected = (local.strftime('%I:%M %p').lstrip('0') if prefs['time_format'] == '12h'
                        else local.strftime('%H:%M'))
            expect(clock).to_have_text(expected)
            expect(page.locator('.msg.bot').last).to_be_visible()
            events = catalogue._conversation_event_payloads(Path(os.environ['NULLION_LIVE_RUNTIME_DB']),
                cid, event_type='conversation.model_timing')
            assert events == [], 'A local command must not spend model tokens'
            # An owned disabled fixture exercises the actual Crons UI without
            # running a scheduled task or touching existing schedules.
            token = page.request.get(base + '/api/session').json()['csrf_token']
            created = page.request.post(base + '/api/crons', headers={'x-nullion-csrf': token}, data={
                'name': 'Local time display ' + uuid4().hex[:8], 'schedule': '0 20 * * 1-5',
                'task': '時間表示の確認', 'enabled': False, 'schedule_timezone': 'UTC',
                'delivery_channel': 'web', 'delivery_target': cid})
            assert created.ok, created.text()
            ident = created.json()['job']['id']
            from tests.e2e.test_web_surface_contracts import _open_settings_tab
            _open_settings_tab(page, 'Crons')
            row = page.locator('#crow-' + ident)
            expect(row).to_be_visible()
            anchor = datetime.now(UTC).replace(hour=20, minute=0, second=0, microsecond=0)
            zone = ZoneInfo(prefs['timezone'])
            offsets = {(anchor + timedelta(days=i)).astimezone(zone).utcoffset() for i in range(7)}
            if len(offsets) == 1:
                local = anchor.astimezone(zone)
                scheduled_clock = (local.strftime('%I:%M %p').lstrip('0') if prefs['time_format'] == '12h'
                                   else local.strftime('%H:%M'))
                expect(row).to_contain_text(scheduled_clock)
            else:
                expect(row).to_contain_text('Custom schedule')
            output = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
            page.screenshot(path=str(output / 'user-time-policy.png'), full_page=True)
            (output / 'user-time-policy.json').write_text(json.dumps({'conversation_id': cid,
                'policy': policy, 'browser_timezone': 'Asia/Tokyo', 'browser_locale': 'en-GB',
                'stored_instant': instant, 'visible_time': clock.inner_text(), 'expected_time': expected,
                'model_calls': len(events), 'schedule_card': row.inner_text()}, indent=2))
        finally:
            if ident:
                response = page.request.delete(base + '/api/crons/' + ident, headers={'x-nullion-csrf': token})
                assert response.ok, response.text()
            browser.close()
