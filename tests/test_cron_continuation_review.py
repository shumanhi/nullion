"""Scheduler projects the common owner's verdict without restarting execution."""
from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import TurnResult, _CompletionReviewHandoff, _CompletionReviewDecision
from nullion.completion_pipeline import publish_completion
from nullion.cron_delivery import run_single_agent_cron_turn
from nullion.crons import CronJob
from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
from tests.e2e.test_scheduled_report_completion import setup_runtime


@pytest.mark.parametrize('verified', [False, True])
@pytest.mark.parametrize('task', ['読んだ記録を照合して報告してください。', 'Contrasta los registros y presenta los hallazgos.'])
def test_scheduler_consumes_owned_review_without_starting_another_controller(tmp_path, monkeypatch, verified, task):
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry()
    for name in ('record_read', 'calendar_list'):
        registry.register(ToolSpec(name, 'Read selected structured records', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2), lambda invocation: pytest.fail('Scheduler repeated a completed read'))
    source = ToolResult('source', 'record_read', 'completed', {'value': 7})
    calendar = ToolResult('calendar', 'calendar_list', 'completed', {'value': 1})
    final = 'Selected record is 7; calendar coverage is shown in the report.'
    class Model:
        def create(self, **kw):
            pytest.fail('Scheduler started another review or composition controller')
    class Producer:
        model_client = Model()
        runs = 0
        def run_turn(self, **kw):
            self.runs += 1
            result = TurnResult('terminal', final, [source, calendar], response_fulfilled=True,
                response_presentation={'scheduled_source_verification_required': True,
                    'scheduled_source_verification_satisfied': True, 'outcome_review_role': 'helper'},
                completion_review_handoff=_CompletionReviewHandoff({}, _CompletionReviewDecision('complete')))
            if verified:
                publish_completion(result, task, conversation_id=kw['conversation_id'], principal_id=kw['principal_id'])
            return result
        def resume_turn(self, **kw):
            pytest.fail('Scheduler restarted completed execution')
    producer = Producer()
    job = CronJob(id='review-obligation', name='確認', task=task, schedule='* * * * *')
    result = run_single_agent_cron_turn(job, 'web:review-obligation', label='Scheduled task',
        orchestrator=producer, runtime=runtime, tool_registry=registry,
        settings=SimpleNamespace(enabled_skill_packs=()))
    assert producer.runs == 1
    assert result['response_fulfilled'] is True
    # A stale success flag or unbound handoff cannot certify source coverage.
    assert result['cron_source_verification_satisfied'] is verified
    assert result['text'] == final and result['text'].count('Selected record is 7') == 1
    assert result['cron_shared_completion_owner'] is True
