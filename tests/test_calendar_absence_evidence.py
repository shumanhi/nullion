from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
from nullion.tools import ToolInvocation, _build_calendar_list_handler

import pytest


@pytest.mark.parametrize('records', [[], [{'id': 'existing', 'summary': 'Appointment'}]])
def test_native_calendar_result_declares_returned_count_without_claiming_unbounded_coverage(records):
    calls = []
    def provider(start, end, limit):
        calls.append((start, end, limit))
        return records
    result = _build_calendar_list_handler(provider)(ToolInvocation(
        'calendar-read', 'calendar_list', 'web:absence',
        {'start': '2028-11-10T00:00:00Z', 'end': '2028-11-11T00:00:00Z', 'max': 20}))
    assert calls == [('2028-11-10T00:00:00Z', '2028-11-11T00:00:00Z', 20)]
    assert result.output['results'] == records
    assert result.output['result_count'] == len(records)
    assert 'total_count' not in result.output and 'complete' not in result.output
    assert _completion_review_evidence_result_is_positive(result)


@pytest.mark.parametrize('malformed', [None, {}, [None], ['not an event']])
def test_malformed_calendar_provider_result_cannot_become_verified_absence(malformed):
    result = _build_calendar_list_handler(lambda *args: malformed)(ToolInvocation(
        'calendar-read', 'calendar_list', 'web:absence',
        {'start': '2028-11-10T00:00:00Z', 'end': '2028-11-11T00:00:00Z', 'max': 20}))
    assert result.status == 'failed'
    assert result.output.get('result_count') is None
    assert not _completion_review_evidence_result_is_positive(result)
