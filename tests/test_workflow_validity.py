from datetime import datetime, timezone
import pytest
from nullion.workflow_validity import execution_window_context, validate_execution_window


def test_rolling_dates_advance_locally_across_dst_without_rewriting_fixed_dates():
    policy = {'mode': 'rolling', 'timezone': 'America/New_York', 'lookahead_days': 3}
    before = execution_window_context(policy, datetime(2026, 11, 1, 3, tzinfo=timezone.utc))
    after = execution_window_context(policy, datetime(2026, 11, 1, 7, tzinfo=timezone.utc))
    assert before['start_date'] == '2026-10-31' and after['start_date'] == '2026-11-01'
    assert before['end_date'] == '2026-11-03' and after['end_date'] == '2026-11-04'
    assert policy == {'mode': 'rolling', 'timezone': 'America/New_York', 'lookahead_days': 3}
    fixed = {'mode': 'fixed', 'timezone': 'America/New_York', 'ends_at': '2026-10-16T23:59:00-04:00'}
    assert execution_window_context(fixed, datetime(2026, 10, 25, tzinfo=timezone.utc))['status'] == 'expired'
    assert fixed['ends_at'].startswith('2026-10-16')
    assert execution_window_context({}) is None


@pytest.mark.parametrize('policy', [
    {'mode': 'rolling', 'timezone': 'UTC'},
    {'mode': 'rolling', 'timezone': 'UTC', 'lookahead_days': 0},
    {'mode': 'rolling', 'timezone': 'UTC', 'lookahead_days': 3, 'ends_at': '2026-10-16'},
    {'mode': 'fixed', 'timezone': 'UTC', 'ends_at': '2026-10-16'},
    {'mode': 'fixed', 'timezone': 'not-a-zone', 'ends_at': '2026-10-16T00:00:00Z'},
    {'mode': 'fixed', 'timezone': 'UTC', 'starts_at': '2026-10-17T00:00:00Z', 'ends_at': '2026-10-16T00:00:00Z'},
])
def test_ambiguous_or_invalid_time_policies_are_rejected(policy):
    with pytest.raises(ValueError): validate_execution_window(policy)
