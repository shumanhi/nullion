"""Prepared file plus rejected chat summary through real runner, SQLite and upload."""
import json
from html import unescape
import os
from pathlib import Path

import pytest

from tests.e2e.test_scheduled_report_completion import (
    setup_runtime, run_scheduled, telegram_boundary, tool_response, text_response,
)
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def encoded(value):
    return text_response(json.dumps(value, ensure_ascii=False))


@pytest.mark.parametrize('task', ['確認結果を .html で作成し、要約を添えてください。',
                                  'Crea un informe .html y resume los hallazgos.'])
@pytest.mark.parametrize('recover_source', [False, True])
@pytest.mark.parametrize('repair_ok', [False, True, 'changed'])
def test_file_and_chat_summary_have_separate_recovery(tmp_path, monkeypatch, task, recover_source, repair_ok, cancel_stage=None, reviewer_correction=False):
    from nullion.crons import CronJob
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    reads, writes = [], []
    cancelled = {"value": False}
    artifact = Path(artifact_root_for_principal('telegram:123'))/'selected.html'
    artifact.parent.mkdir(parents=True, exist_ok=True)
    history = artifact.parent/'history.json'
    complete_document = '<!doctype html><html><body><h1>Selected report</h1><p>A: 7; B: 8.</p></body></html>'
    partial_document = complete_document.replace('B: 8.', 'B: not checked.')
    summary = 'Selected records: A = 7 and B = 8. The HTML report is attached.'
    editorial = 'Replace the nonresponsive draft and identify the two prepared downloadable files.'
    def read(inv):
        reads.append(inv.arguments['record'])
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7 if reads[-1] == 'A' else 8})
    def write(inv):
        path = Path(inv.arguments['path']); path.write_text(inv.arguments['content']); writes.append(path)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'path': str(path), 'bytes_written': path.stat().st_size})
    registry.register(ToolSpec('sensor_read', 'Read chosen value', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2,
        input_schema={'type': 'object', 'properties': {'record': {'type': 'string'}}, 'required': ['record']}), read)
    registry.register(ToolSpec('file_write', 'Write chosen report', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2,
        input_schema={'type': 'object', 'properties': {'path': {'type': 'string'}, 'content': {'type': 'string'}},
                      'required': ['path', 'content']}), write)
    class Provider:
        reviews = 0
        repairs = 0
        producer_calls = 0
        after_cancel_calls = 0
        def create(self, **kw):
            if cancelled['value']: self.after_cancel_calls += 1
            system = kw.get('system') or ''
            if 'material-requirements planner' in system:
                return encoded({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': ['.html'],
                    'material_requirements': [
                        {'requirement': 'Verify the selected records', 'requirement_kind': 'fact',
                         'evidence_subject_id': 'records', 'eligible_evidence_tool_names': ['sensor_read']},
                        {'requirement': 'Create one HTML report with the selected records', 'requirement_kind': 'deliverable',
                         'evidence_subject_id': '.html', 'eligible_evidence_tool_names': ['file_write']}]})
            if 'semantic completion reviewer' in system:
                self.reviews += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                ready = artifact.read_text() == complete_document
                correct = packet['draft_final_response'] == summary or reviewer_correction and ready
                if cancel_stage == 'review': cancelled['value'] = True
                rows = []
                for row in packet['material_requirement_contract']:
                    candidates = [r for r in packet['tool_evidence'] if r['status'] == 'completed'
                        and r['tool_name'] in row['eligible_evidence_tool_names']]
                    if row['requirement_kind'] == 'deliverable':
                        bound = [r for r in packet['tool_evidence'] if r['status'] == 'completed'
                            and (r.get('output') or {}).get('artifact_record_binding')]
                        if bound: candidates = bound
                    rows.append({'requirement_id': row['requirement_id'], 'satisfied': ready,
                        'draft_covered': ready and correct,
                        'evidence_invocation_ids': [r['invocation_id'] for r in candidates[-1:]]})
                replacement = {}
                if ready and reviewer_correction:
                    from nullion.chat_text import REPORT_MESSAGE_FORMAT
                    replacement = {'reply_target': 'corrected_findings', 'source_presentation_valid': True,
                        'supported_findings': [{'text': {'format': REPORT_MESSAGE_FORMAT, 'message': {
                            'summary': summary, 'detail_level': 'compact', 'sections': [], 'sources': [], 'shared_details': []}},
                            'presentation': 'paragraph', 'requirement_ids': [row['requirement_id'] for row in rows],
                            'evidence_invocation_ids': list(dict.fromkeys(ref for row in rows for ref in row['evidence_invocation_ids']))}]}
                return encoded({**replacement, 'disposition': 'complete' if ready and correct else 'retry',
                    'all_material_requirements_enumerated': True, 'material_requirements': rows,
                    'unresolved_requirements': [] if ready and correct else [editorial if ready else 'Verify B and update the report.'],
                    'retry_tool_names': [] if ready else ['sensor_read', 'file_write']})
            if system.startswith('You repair scheduled-task final answers'):
                self.repairs += 1
                if cancel_stage == 'repair': cancelled['value'] = True
                assert not kw['tools']
                prompt = kw['messages'][0]['content'][0]['text']
                selected = json.JSONDecoder().raw_decode(prompt.split('Selected user deliverables', 1)[1].split('\n', 1)[1])[0]
                assert selected == [{'filename': 'selected.html', 'extension': '.html'}]
                records = json.JSONDecoder().raw_decode(prompt.split('Verified compact tool evidence from this run:\n', 1)[1])[0]
                if repair_ok is False:
                    raise TimeoutError('Synthetic composer outage')
                if repair_ok == 'changed':
                    artifact.write_text(complete_document.replace('</body>', '<p>Changed concurrently.</p></body>'))
                return encoded({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [next(r['evidence_id'] for r in records if r['status'] == 'completed')], 'report': summary})
            protocol = workflow_provider_reply(kw)
            if protocol is not None: return protocol
            self.producer_calls += 1
            if not reads: return tool_response('sensor_read', {'record': 'A'})
            if not recover_source and 'B' not in reads: return tool_response('sensor_read', {'record': 'B'})
            if 'B' in reads and not history.exists():
                return tool_response('file_write', {'path': str(history), 'content': '{"A": 7, "B": 8}'})
            if not artifact.exists(): return tool_response('file_write', {'path': str(artifact),
                'content': partial_document if recover_source else complete_document})
            if self.reviews and recover_source and 'B' not in reads:
                return tool_response('sensor_read', {'record': 'B'})
            if self.reviews and recover_source and artifact.read_text() != complete_document:
                return tool_response('file_write', {'path': str(artifact), 'content': complete_document})
            return text_response('The draft is not responsive.')
    provider = Provider(); deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='artifact-summary', name='選択', task=task, schedule='* * * * *',
        delivery_channel='telegram', delivery_target='123', artifact_delivery_options={
            'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html']})
    if cancel_stage:
        from types import SimpleNamespace
        from nullion.agent_orchestrator import AgentOrchestrator
        from nullion.cron_delivery import (run_single_agent_cron_turn, run_cron_delivery_workflow,
            CronRunDeliveryCallbacks, cron_delivery_block_reason)
        result = run_cron_delivery_workflow(job, label='Scheduled task', callbacks=CronRunDeliveryCallbacks(
            effective_channel=lambda job: job.delivery_channel, delivery_target=lambda job, channel: job.delivery_target,
            run_agent_turn=lambda job, conversation: run_single_agent_cron_turn(job, conversation, label='Scheduled task',
                orchestrator=AgentOrchestrator(model_client=provider), runtime=runtime, tool_registry=registry,
                settings=SimpleNamespace(enabled_skill_packs=()), cancellation_checker=lambda: cancelled['value']),
            record_event=lambda *args, **kwargs: None, block_reason=cron_delivery_block_reason,
            save_web_delivery=lambda *args: None, send_platform_delivery=send))
        assert result['cron_run_cancelled'] is True and result['response_fulfilled'] is False
        assert provider.after_cancel_calls == 0 and provider.repairs == int(cancel_stage == 'repair')
        assert not deliveries
        return
    result = run_scheduled(job, provider, runtime, registry, send)
    uploads = [row for row in deliveries if 'bytes' in row]
    assert len(uploads) == 1, json.dumps({'result':{k:v for k,v in result.items() if k not in {'tool_results','messages_snapshot','completion_review_handoff'}},
        'tools':[(r.tool_name,r.status,r.output.get('reason')) for r in result.get('tool_results',[])],
        'producer_calls':provider.producer_calls,'repairs':provider.repairs,'reviews':provider.reviews},default=str)
    assert 'A: 7; B: 8.' in uploads[0]['bytes'].decode()
    assert reads == ['A', 'B'] and len([p for p in writes if p.suffix == '.html']) == (2 if recover_source else 1)
    assert result.get('cron_shared_completion_owner') is True, result
    assert provider.repairs == (0 if reviewer_correction else 2 if repair_ok is False else 1), result
    visible = unescape('\n'.join(str(row.get('text') or row.get('caption') or '') for row in deliveries))
    repaired = repair_ok is True or reviewer_correction
    assert editorial not in visible and 'two prepared' not in visible
    assert (summary in visible) is repaired, (result, deliveries)
    assert result['response_fulfilled'] is repaired, result
    assert result['execution_outcome']['report_content_status'] == ('unknown' if repair_ok == 'changed' else 'complete'), result
    assert result['execution_outcome']['completion_verified'] is repaired
    if repair_ok is False:
        assert "couldn't complete the chat summary" in visible
    events = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.completion_review')
    assert any(row.get('execution_satisfied') for row in events)
    import sqlite3
    with sqlite3.connect(tmp_path/'runtime.db') as database:
        runs = [json.loads(row[0]) for row in database.execute(
            "SELECT payload FROM conversation_events WHERE json_extract(payload, '$.event_type') = ? ORDER BY updated_at",
            ('conversation.cron_run',))]
    assert runs[-1]['report_content_status'] == ('unknown' if repair_ok == 'changed' else 'complete')
    assert runs[-1]['report_presentation_status'] == ('complete' if repaired else 'partial')
    if reviewer_correction:
        assert provider.reviews == 1
        assert any(row.get('presentation_repaired') for row in events)


@pytest.mark.parametrize('cancel_stage', ['review', 'repair'])
def test_cancelled_summary_recovery_stops_calls_and_delivery(tmp_path, monkeypatch, cancel_stage):
    test_file_and_chat_summary_have_separate_recovery(tmp_path, monkeypatch,
        '結果を .html で作成し、要約を添付。', False, True, cancel_stage=cancel_stage)


def test_delivery_owned_artifact_review_publishes_its_corrected_summary_without_restarting_producer(tmp_path, monkeypatch):
    test_file_and_chat_summary_have_separate_recovery(tmp_path, monkeypatch,
        'Crea el informe .html y resume los resultados.', False, True, reviewer_correction=True)
