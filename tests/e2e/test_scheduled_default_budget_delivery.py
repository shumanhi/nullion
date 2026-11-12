"""A legitimate larger scheduled run finishes beyond the old 64-call cutoff."""
import os

import pytest

from tests.e2e.test_scheduled_report_completion import (
    run_scheduled, setup_runtime, telegram_boundary, text_response, tool_response,
)
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker scheduled delivery boundary')


@pytest.mark.parametrize('selected_request', [
    '選択した70件の記録を確認して、合計を報告してください。',
    'Revisa los 70 registros seleccionados y entrega su suma.',
])
def test_default_finishes_useful_work_past_old_limit_without_budget_prompt(tmp_path, monkeypatch, selected_request):
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.runtime_persistence import load_runtime_store
    monkeypatch.delenv('NULLION_CRON_AGENT_MAX_ITERATIONS', raising=False)
    monkeypatch.delenv('NULLION_AGENT_TURN_MAX_ITERATIONS', raising=False)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'missing.env'))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    runtime = setup_runtime(tmp_path, monkeypatch)
    reads = []
    registry = ToolRegistry()
    def read(invocation):
        value = invocation.arguments['record']
        reads.append(value)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'text': f'Selected record {value}: value 7.'})
    registry.register(ToolSpec('browser_extract_text', 'Read a selected record', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, input_schema={'type': 'object',
            'properties': {'record': {'type': 'integer'}}, 'required': ['record']}), read)
    class Provider:
        producer_calls = 0
        total_calls = 0
        def create(self, **kwargs):
            self.total_calls += 1
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            self.producer_calls += 1
            if len(reads) < 70:
                return tool_response('browser_extract_text', {'record': len(reads)})
            return text_response('Checked all 70 selected records. Total: 490. All requested checks are complete.')
    provider = Provider()
    deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='large-default', name='記録の確認', task=selected_request, schedule='0 9 * * *',
        delivery_channel='telegram', delivery_target='123')
    result = run_scheduled(job, provider, runtime, registry, send)
    assert reads == list(range(70))
    assert provider.producer_calls == 71
    assert 64 < provider.total_calls <= 128
    assert not result.get('suspended_for_approval'), result.get('reason')
    assert result['response_fulfilled'] is True
    assert len(deliveries) == 1 and '490' in deliveries[0]['text']
    restored = load_runtime_store(runtime.checkpoint_path)
    timing = restored.list_recent_conversation_events('telegram:123',
        event_type='conversation.model_timing', limit=128)
    assert 64 < len(timing) <= 128
    assert not [approval for approval in restored.approval_requests.values()
        if approval.request_kind == 'agent_turn_limit_extension']
