from datetime import UTC, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from nullion import preferences
from nullion.preferences import Preferences, format_user_datetime, format_clock_time


@pytest.fixture
def saved(monkeypatch):
    prefs = Preferences(timezone='America/New_York', time_format='12h')
    monkeypatch.setattr(preferences, 'load_preferences', lambda: prefs)
    monkeypatch.setattr(preferences, 'detect_system_timezone', lambda default='UTC': 'Asia/Tokyo')
    return prefs


@pytest.mark.parametrize('hour,expected', [(0, '12:05 AM'), (12, '12:05 PM'), (16, '4:05 PM'), (23, '11:05 PM')])
def test_shared_clock_uses_am_pm_without_leading_hour_zero(hour, expected):
    assert format_clock_time(hour, 5) == expected


@pytest.mark.parametrize('instant,zone,expected', [
    ('2026-10-30T20:05:00Z', 'America/New_York', 'Oct 30, 2026, 4:05 PM EDT'),
    ('2026-11-04T21:05:00Z', 'America/New_York', 'Nov 4, 2026, 4:05 PM EST'),
    ('2026-11-04T21:05:00Z', 'Asia/Kolkata', 'Nov 5, 2026, 2:35 AM IST'),
    ('2026-11-04T11:05:00Z', 'Pacific/Kiritimati', 'Nov 5, 2026, 1:05 AM +14'),
    ('2026-11-04T00:05:00', 'UTC', 'Nov 4, 2026, 12:05 AM UTC'),
])
def test_time_policy_converts_dates_and_offsets_without_using_host(instant, zone, expected):
    assert format_user_datetime(instant, prefs=Preferences(timezone=zone)) == expected


def test_explicit_utc_is_not_replaced_by_host_timezone(saved):
    from nullion.crons import _cron_timezone
    from nullion.reminders import reminder_timezone, normalize_reminder_due_at
    from nullion.chat_store import _local_history_timezone
    saved.timezone = 'UTC'
    assert _cron_timezone() == UTC
    assert reminder_timezone() == UTC
    assert _local_history_timezone() == ZoneInfo('UTC')
    assert normalize_reminder_due_at(datetime(2026, 11, 4, 16, 5)) == datetime(2026, 11, 4, 16, 5, tzinfo=UTC)


def test_request_timezone_fallback_does_not_assume_a_particular_city(saved, monkeypatch):
    from nullion.chat_operator import _current_user_timezone_name
    saved.timezone = 'Europe/Paris'
    assert _current_user_timezone_name() == 'Europe/Paris'
    monkeypatch.setattr(preferences, 'time_presentation_context', lambda: (_ for _ in ()).throw(OSError('unavailable')))
    assert _current_user_timezone_name() == 'Asia/Tokyo'


def test_related_notices_share_user_clock_even_when_job_uses_utc(saved):
    from nullion.live_tracking import local_time
    from nullion.tracker_status import _time
    from nullion.operator_commands import _operator_timestamp_label
    from nullion.reminders import format_reminder_due_at
    from nullion.approval_display import _format_calendar_datetime
    instant = '2026-11-04T21:05:00+00:00'
    assert local_time(instant) == _time(instant, SimpleNamespace(schedule_timezone='UTC'))
    assert '4:05 PM EST' in _operator_timestamp_label(instant).replace(':00 PM', ' PM')
    assert format_reminder_due_at(datetime.fromisoformat(instant)) == '4 Nov at 4:05 PM'
    assert _format_calendar_datetime(instant) == 'Nov 4, 2026, 4:05 PM EST'
    assert _format_calendar_datetime('2026-11-05') == 'Nov 5, 2026'
    assert _format_calendar_datetime('2026-11-04T13:05:00', source_timezone='America/Los_Angeles') == 'Nov 4, 2026, 4:05 PM EST'


def test_report_clock_and_manual_window_notice_follow_saved_timezone(saved):
    from nullion.cron_delivery import _cron_run_time_context, manual_cron_silent_delivery_text
    context = _cron_run_time_context()
    assert context['timezone'] == 'America/New_York'
    assert datetime.fromisoformat(context['current_datetime']) == datetime.fromisoformat(context['local_datetime'])
    text = manual_cron_silent_delivery_text(None, 'Manual', {'reason': 'workflow_window_not_started',
        'workflow_window': dict(mode='daily', timezone='UTC', current_datetime='2026-11-04T20:00:00+00:00',
                                start_time='21:00', end_time='23:00')})
    assert '4:00 PM EST' in text and '6:00 PM EST' in text
    assert '21:00' not in text


def test_clock_policy_honors_an_explicit_24_hour_override_only(saved):
    from nullion.crons import cron_display_fields, CronJob
    assert '4:05 PM' in format_user_datetime('2026-11-04T21:05:00Z')
    saved.time_format = '24h'
    assert '16:05' in format_user_datetime('2026-11-04T21:05:00Z')
    fields = cron_display_fields(CronJob(id='a', name='予定', task='確認', schedule='5 21 * * *',
        schedule_timezone='UTC', next_run='2026-11-04T21:05:00Z'))
    assert '16:05' in fields['next_run_description']


@pytest.mark.parametrize('instant,expected', [('2026-05-15T20:00:00Z', '4:00 PM'),
                                             ('2026-11-04T20:00:00Z', '3:00 PM')])
def test_schedule_summary_uses_upcoming_run_season_instead_of_a_fixed_reference(saved, instant, expected):
    from nullion.crons import CronJob, cron_display_fields
    job = CronJob(id='a', name='予定', task='確認', schedule='0 20 * * 1-5',
                  schedule_timezone='UTC', next_run=instant)
    assert expected in cron_display_fields(job)['schedule_description']


def test_schedule_summary_preserves_half_hour_minute_and_weekday_rollover(saved):
    from nullion.crons import CronJob, cron_display_fields
    saved.timezone = 'Asia/Kolkata'
    job = CronJob(id='a', name='予定', task='確認', schedule='45 23 * * 1-5',
                  schedule_timezone='UTC', next_run='2026-11-04T23:45:00Z')
    text = cron_display_fields(job)['schedule_description']
    assert '5:15 AM' in text and 'Saturday' in text and 'Monday' not in text


@pytest.mark.parametrize('schedule,instant', [('0 20 * * 1-5', '2026-10-30T20:00:00Z'),
                                           ('0 20 1 * *', '2026-11-01T20:00:00Z')])
def test_nonrepresentable_local_schedule_shows_exact_next_instant_instead_of_invented_pattern(saved, schedule, instant):
    from nullion.crons import CronJob, cron_display_fields
    fields = cron_display_fields(CronJob(id='a', name='予定', task='確認', schedule=schedule,
        schedule_timezone='UTC', next_run=instant))
    assert fields['schedule_description'].startswith('Custom schedule')
    assert 'PM' in fields['next_run_description']
