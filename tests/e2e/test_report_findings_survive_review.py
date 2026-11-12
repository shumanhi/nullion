"""Real graph, scheduler, SQLite, Telegram bytes; scripted external model/source."""
import json
import os
from types import SimpleNamespace

import pytest

from tests.e2e.test_scheduled_report_completion import telegram_boundary
from tests.e2e.test_universal_outcome_review_boundary import runtime_and_registry, text

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


class ReportProvider:
    def __init__(self, source, *, correct=False, invalid=False):
        self.source, self.correct, self.invalid = source, correct, invalid
        self.agent_calls = self.review_calls = self.repair_calls = 0

    def create(self, **kw):
        system = kw.get('system', '')
        if 'material-requirements planner' in system:
            return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                'material_requirements': [{'requirement': 'Report each selected record with date and action result',
                    'requirement_kind': 'fact', 'evidence_subject_id': 'report',
                    'eligible_evidence_tool_names': [self.source]}]})
        if 'semantic completion reviewer' in system:
            self.review_calls += 1
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            ref = next(row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name'] == self.source)
            complete = self.correct and self.repair_calls > 0
            return text({'disposition': 'complete' if complete else 'retry',
                'all_material_requirements_enumerated': True,
                'unresolved_requirements': [] if complete else ['Include the available received date for record B.'],
                'retry_tool_names': [], 'user_facing_failure': None if complete else 'Record B’s received date is not included.',
                'supported_findings': [] if complete else [{
                    'text': '**Record A — Monthly notice**\nReceived Apr 2. Due May 7: 35 units. Matching entry already exists.',
                    'evidence_invocation_ids': ['not-owned'] if self.invalid else [ref]}, {
                    'text': '**Record B — Completed transfer**\n250 units completed Apr 1; no new action needed.',
                    'evidence_invocation_ids': ['not-owned'] if self.invalid else [ref]}],
                'material_requirements': [{'requirement_id': row['requirement_id'],
                    'satisfied': complete, 'draft_covered': complete, 'evidence_invocation_ids': [ref]}
                    for row in packet['material_requirement_contract']]})
        if 'You repair scheduled-task final answers' in system:
            self.repair_calls += 1
            prompt = kw['messages'][0]['content'][0]['text']
            evidence = json.loads(prompt.split('Verified compact tool evidence from this run:\n', 1)[1]
                .split('\n\nRuntime-verified', 1)[0])
            ref = next(row['evidence_id'] for row in evidence if row.get('tool_name') == self.source)
            # The external composer still misses one available date. The review
            # independently certifies useful findings, not this rejected draft.
            return text({'requirements_satisfied': True, 'source_state': 'available', 'evidence_refs': [ref],
                'delivery_decision': 'send', 'delivery_reason': 'report', 'remaining_work': [],
                'report': 'Record A: 35 units due May 7. Record B: transfer completed.'})
        self.agent_calls += 1
        if self.agent_calls == 1:
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'read', 'name': self.source, 'input': {}}]}
        return text('Rejected draft: every record was resolved; no details supplied.')


def run_report(job, provider, runtime, registry, send):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import (run_single_agent_cron_turn, run_cron_delivery_workflow,
        CronRunDeliveryCallbacks, cron_delivery_block_reason, record_cron_delivery_chat_turn,
        checkpoint_cron_delivery_records)
    from nullion.events import make_event
    def record(event, job, channel, target, conversation_id, **extra):
        runtime.store.add_event(make_event(event, 'cron_scheduler', {'cron_id': job.id, **extra}))
    def record_chat(job, conversation, channel, target, delivered):
        event = record_cron_delivery_chat_turn(runtime.store, job, conversation_id=conversation,
            delivery_channel=channel, delivery_target=target, delivered_text=delivered)
        checkpoint_cron_delivery_records(runtime, conversation_events=[event])
    return run_cron_delivery_workflow(job, label='Scheduled task', callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda job: job.delivery_channel, delivery_target=lambda job, channel: job.delivery_target,
        run_agent_turn=lambda job, conversation: run_single_agent_cron_turn(job, conversation, label='Scheduled task',
            orchestrator=AgentOrchestrator(model_client=provider), runtime=runtime, tool_registry=registry,
            settings=SimpleNamespace(enabled_skill_packs=()), record_event=record),
        record_event=record, block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda *args: None, send_platform_delivery=send, record_chat_turn=record_chat))


@pytest.mark.parametrize('source,prompt', [('account_report_read', '項目ごとの日付、金額と処理結果を報告。'),
    ('inventory_read', 'Informa de cada registro, sus fechas, importes y acciones.')])
@pytest.mark.parametrize('correct,invalid', [(False, False), (True, False), (False, True)])
def test_scheduled_report_preserves_certified_items_after_bounded_repair(tmp_path, monkeypatch, record_property, source, prompt, correct, invalid):
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, _, _ = runtime_and_registry(tmp_path, monkeypatch, source)
    from nullion import crons, chat_store
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', runtime.checkpoint_path)
    monkeypatch.setattr(chat_store, '_store', None)
    registry = ToolRegistry()
    reads = []
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id, source, 'completed', {'records': [
            {'subject': 'Monthly notice', 'received': 'Apr 2', 'due': 'May 7', 'amount': 35,
                'calendar_result': 'Matching entry already exists'},
            {'subject': 'Completed transfer', 'received': 'Apr 1', 'amount': 250,
                'action_result': 'Completed; no new action needed'}]})
    registry.register(ToolSpec(source, 'Read selected records and existing action receipts', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    provider = ReportProvider(source, correct=correct, invalid=invalid)
    deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='selected-report', name='Selected records', schedule='0 * * * *', task=prompt,
        delivery_channel='telegram', delivery_target='123')
    result = run_report(job, provider, runtime, registry, send)
    record_property('runtime_evidence',json.dumps(result,default=str))
    assert len(reads) == 1
    assert len(deliveries) == 1
    rendered = deliveries[0]['text']
    assert 'Rejected draft' not in rendered
    if invalid:
        assert '250 units completed' not in rendered
        assert result['response_fulfilled'] is True
        assert not result.get('cron_report_failed')
        assert result['response_presentation']['outcome_review_status'] == 'unavailable'
        assert result['execution_outcome']['completion_verified'] is False
    elif correct:
        assert '35 units due May 7' in rendered, (provider.repair_calls, result)
        assert result['response_fulfilled'] is True
    else:
        for detail in ['Monthly notice', 'Received Apr 2', 'Due May 7: 35 units',
                       'Matching entry already exists', 'Completed transfer', '250 units completed Apr 1']:
            assert detail in rendered
        assert 'Record B’s received date is not included.' not in rendered
        assert 'Some requested information remains unconfirmed.' not in rendered
        assert result['response_fulfilled'] is True and not result.get('cron_report_failed')
        assert result['response_presentation']['outcome_review_role'] == 'helper'
        assert provider.repair_calls == 2, 'Initial composition and one retained-evidence revision stay bounded'
    from nullion.runtime_persistence import load_runtime_store
    rows = load_runtime_store(runtime.checkpoint_path).list_recent_conversation_events(
        'telegram:123', event_type='conversation.chat_turn', limit=10)
    assert any(row.get('cron_id') == job.id and ('Monthly notice' in row['assistant_reply']) == (not correct and not invalid)
        for row in rows), rows
