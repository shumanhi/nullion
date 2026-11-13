"""Presentation instructions reach producer and recovery, then real delivery.

The external model is scripted: this proves propagation, field preservation and
single-message delivery, not a real model's subjective writing quality.
"""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import (
    setup_runtime, telegram_boundary, run_scheduled, text_response, tool_response,
)

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('repair', [False, True])
@pytest.mark.parametrize('task', [
    '送信者、件名、受信日、期限、金額、予定表の結果を簡潔に報告。',
    'Resume remitente, asunto, fecha, vencimiento, importe y resultado del calendario.',
])
def test_compact_report_preserves_metadata_through_scheduled_telegram_delivery(tmp_path, monkeypatch, repair, task):
    from nullion.cron_delivery import _scheduled_chat_presentation_instructions
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    deliveries, send = telegram_boundary(monkeypatch)
    registry = ToolRegistry()
    reads = []
    record = {'value': 7, 'sender': 'Fixture Bank', 'subject': 'November statement', 'received_date': '2026-11-02',
        'due_date': '2026-11-07', 'minimum': '35 USD', 'calendar': 'existing',
        'later_authorization': '735.93 USD', 'settlement_verified': False}
    def read(invocation):
        reads.append(invocation)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', record)
    registry.register(ToolSpec('record_read', 'Read selected records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    report = ('Fixture Bank — 7 Nov: minimum 35 USD. Later authorization: 735.93 USD; settlement unknown.\n'
        'Source: November statement, received 2 Nov 2026.\n\n'
        'Calendar: existing reminder verified; 0 new events.')
    class Model:
        producer_calls = 0
        repair_calls = 0
        saw_guidance = False
        diagnostics = []
        def create(self, **kw):
            system = kw.get('system') or ''
            if system.startswith('You repair scheduled-task final answers'):
                self.repair_calls += 1
                assert _scheduled_chat_presentation_instructions() in system
                raw = kw['messages'][0]['content'][0]['text']
                evidence = json.JSONDecoder().raw_decode(raw.split('Verified compact tool evidence from this run:\n', 1)[1])[0]
                return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [r['evidence_id'] for r in evidence if r['status'] == 'completed'], 'report': report}))
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [{
                        'requirement': 'Report source obligation and calendar outcome with requested metadata',
                        'requirement_kind': 'fact', 'evidence_subject_id': 'selected-record',
                        'eligible_evidence_tool_names': ['record_read']}]}))
            if 'semantic completion reviewer' in system:
                assert 'Execution constraints are not additional reporting requirements.' in system
                assert 'Keep every explicitly requested reporting detail mandatory.' in system
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                self.diagnostics.append(packet.get('runtime_coverage_validation_errors'))
                refs = [r['invocation_id'] for r in packet['tool_evidence']
                    if r['tool_name'] == 'record_read' and r['status'] == 'completed']
                return text_response(json.dumps({'disposition': 'complete',
                    'all_material_requirements_enumerated': True, 'unresolved_requirements': [],
                    'retry_tool_names': [], 'material_requirements': [{
                        'requirement_id': r['requirement_id'], 'satisfied': True,
                        'draft_covered': True, 'evidence_invocation_ids': refs[-1:]}
                        for r in packet['material_requirement_contract']]}))
            self.producer_calls += 1
            messages = '\n'.join(block.get('text', '') for row in kw.get('messages', ())
                for block in (row.get('content') if isinstance(row.get('content'), list) else
                    [{'text': row.get('content', '')}]) if isinstance(block, dict))
            assert _scheduled_chat_presentation_instructions() in messages
            self.saw_guidance = True
            if not reads:
                return tool_response('record_read', {})
            if repair:
                raise TimeoutError('Synthetic provider interruption after successful source read')
            return text_response(report)
    model = Model()
    result = run_scheduled(CronJob(id='compact-report', name='記録', task=task, schedule='* * * * *',
        delivery_channel='telegram', delivery_target='123'), model, runtime, registry, send)
    assert model.saw_guidance and len(reads) == 1
    assert model.repair_calls == int(repair), json.dumps(model.diagnostics)
    assert result['response_fulfilled'] is True, result
    assert len(deliveries) == 1 and report in deliveries[0]['text']
    assert '\n\n\n' not in deliveries[0]['text']
    reviews = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.completion_review')
    assert reviews == []
