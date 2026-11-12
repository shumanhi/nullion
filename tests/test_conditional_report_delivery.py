from datetime import datetime

import pytest

from nullion.report_completion import report_delivery_gate_contract, validated_report_delivery_gate
from nullion.workflow_validity import execution_window_context, validate_execution_window


DAILY = dict(mode='daily', timezone='America/New_York', start_time='16:00',
             end_time='23:59:59', weekdays=[0, 1, 2, 3, 4])


@pytest.mark.parametrize('instant,status', [
    ('2026-10-30T19:59:59+00:00', 'waiting'),
    ('2026-10-30T20:00:00+00:00', 'active'),
    ('2026-11-04T20:00:00+00:00', 'waiting'),
    ('2026-11-04T21:00:00+00:00', 'active'),
    ('2026-11-07T21:05:00+00:00', 'waiting'),
    ('2027-03-15T20:05:00+00:00', 'active'),
])
def test_daily_execution_interval_follows_local_clock_and_weekday(instant, status):
    assert execution_window_context(DAILY, datetime.fromisoformat(instant))['status'] == status


@pytest.mark.parametrize('updates', [dict(start_time='16:00Z'), dict(end_time='12:00'),
    dict(weekdays=[7]), dict(weekdays=[]), dict(ends_at='2090-01-01T00:00:00Z'), dict(start_time='bad')])
def test_daily_interval_rejects_ambiguous_or_invalid_scope(updates):
    with pytest.raises(ValueError):
        validate_execution_window({**DAILY, **updates})


@pytest.mark.parametrize('condition', ['閉館日は何も送らない。', 'No enviar si no hay cambios.', 'لا ترسل قبل الموعد.'])
def test_no_delivery_requires_current_request_and_known_evidence(condition):
    contract = report_delivery_gate_contract({'instructions': condition, 'run_context': {'now': '2026-11-04'}},
        ['run_context', 'tool_evidence:0'])
    decision = dict(contract_id=contract['contract_id'], delivery_decision='withhold',
                    instruction_condition=condition, evidence_refs=['run_context'])
    assert validated_report_delivery_gate(decision, contract, instructions=condition) == decision
    for bad in [dict(contract_id='stale'), dict(instruction_condition='invented rule'),
                dict(evidence_refs=[]), dict(evidence_refs=['tool_evidence:1']), dict(extra=True)]:
        assert validated_report_delivery_gate({**decision, **bad}, contract, instructions=condition) is None
