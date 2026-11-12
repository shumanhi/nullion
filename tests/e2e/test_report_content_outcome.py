"""Real Web scheduler, SQLite outcome and delivered generated report bytes."""
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import time

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def text_response(text):
    return {'stop_reason':'end_turn', 'content':[{'type':'text', 'text':text}]}


def tool_response(name, arguments):
    return {'stop_reason':'tool_use', 'content':[{'type':'tool_use', 'id':name+'-call', 'name':name, 'input':arguments}]}


def setup_runtime(tmp_path, monkeypatch):
    from nullion import crons, chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    checkpoint = tmp_path/'runtime.db'
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', checkpoint)
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path/'data'))
    monkeypatch.setattr(chat_store, '_store', None)
    return bootstrap_persistent_runtime(checkpoint)


@pytest.mark.parametrize('task', ['全対象の現在値と関連情報を調査し、報告書を作成してください。',
    'Investiga los valores actuales y las novedades de todos los elementos y prepara el informe.'])
@pytest.mark.parametrize('content_status,mode', [(status, 'report_each_run') for status in (
    'complete', 'partial', 'unknown', 'structured_partial')]+[
    ('partial', 'verified_matches_only'), ('unknown', 'verified_matches_only')])
def test_generated_file_receipt_does_not_invent_research_completion(tmp_path, monkeypatch, task, content_status, mode):
    from fastapi.testclient import TestClient
    from nullion import crons, config, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_store import get_chat_store
    from nullion.config import NullionSettings
    from nullion.report_completion import MARKER
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    source_calls = []
    def source(invocation):
        source_calls.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {
            'items': [{'id': 'item-a', 'current_value': 12},
                {'id': 'item-b', 'current_value' if content_status == 'complete' else 'prior_value': 8}],
            'coverage': {'current_count': 2 if content_status == 'complete' else 1, 'requested_count': 2}})
    registry.register(ToolSpec('sensor_read', 'Read selected observations', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, input_schema={'type':'object', 'properties':{}}), source)
    missing = '現在値と関連情報が不足しています。' if task.startswith('全') else 'Faltan valores actuales y novedades.'
    class Model:
        turns = 0
        generation_calls = 0
        def create(self, **kwargs):
            system = kwargs.get('system') or ''
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': ['.html'], 'material_requirements': [
                        {'requirement': 'Observe the current value for every selected record.',
                            'requirement_kind': 'fact', 'evidence_subject_id': 'selected-records',
                            'eligible_evidence_tool_names': ['sensor_read']},
                        {'requirement': 'Prepare the selected HTML report.', 'requirement_kind': 'deliverable',
                            'evidence_subject_id': '.html', 'eligible_evidence_tool_names': ['file_write']}]}))
            if 'semantic completion reviewer' in system:
                reply = workflow_provider_reply(kwargs)
                decision = json.loads(reply['content'][0]['text'])
                if content_status != 'complete':
                    decision.update(disposition='blocked', retry_tool_names=[], unresolved_requirements=[missing])
                    for row in decision['material_requirements']:
                        if row['requirement_kind'] == 'fact':
                            row.update(satisfied=False, draft_covered=False)
                return text_response(json.dumps(decision))
            if not kwargs.get('tools'):
                try: packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                except (ValueError, KeyError, TypeError): packet = {}
                if 'verification_scope' in packet and 'existing_html' in packet:
                    self.generation_calls += 1
                    contract = packet.get('report_outcome_contract') or {'contract_id':'legacy-request'}
                    metadata = {'contract_id': contract['contract_id'], 'status': content_status,
                        'all_material_requirements_enumerated': True,
                        'remaining_work': [missing] if content_status == 'partial' else []}
                    comment = '<!--'+MARKER+json.dumps(metadata, ensure_ascii=False)+'-->' if content_status != 'unknown' else ''
                    second = 'item-b: 8' if content_status == 'complete' else 'item-b: prior observation 8; current observation unavailable.'
                    document = '<!doctype html><html><body><h1>Report</h1><p>item-a: 12</p><p>'+second+'</p>'+comment+'</body></html>'
                    if content_status == 'structured_partial':
                        return text_response(json.dumps({'requirements_satisfied':False, 'html':document,
                            'remaining_work':[missing]}))
                    return text_response(document)
            reply = workflow_provider_reply(kwargs)
            if reply is not None: return reply
            if not kwargs.get('tools'): raise AssertionError('Unexpected extra composition or grading call')
            self.turns += 1
            return tool_response('sensor_read', {}) if self.turns == 1 else text_response('The report file is still missing.')
    model = Model()
    settings = NullionSettings(doctor_enabled=False)
    monkeypatch.setattr(config, 'load_settings', lambda *a, **kw: settings)
    monkeypatch.setattr(web_app, 'load_settings', lambda *a, **kw: settings)
    schedulers = []
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: schedulers.append(self))
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    with TestClient(app) as client:
        response = client.post('/api/crons', json={'name':'Selected report', 'schedule':'* * * * *', 'task':task,
            'enabled':True, 'delivery_channel':'web', 'delivery_target':'web:operator',
            'artifact_delivery_options':{'requires_attachment_delivery':True,
                'required_artifact_extensions':['.html'], 'alert_delivery_mode':mode}})
        assert response.status_code == 200, response.text
        ident = response.json()['job']['id']
        job = crons.get_cron(ident)
        job.next_run = (datetime.now(UTC)-timedelta(minutes=1)).isoformat()
        crons.save_crons([job])
        fired = []
        original_fire = schedulers[-1]._fire
        def observe_fire(selected):
            result = original_fire(selected)
            fired.append(result)
            return result
        schedulers[-1]._fire = observe_fire
        started = time.monotonic()
        schedulers[-1]._tick()
        from cron_scheduler_helpers import finish_scheduler
        finish_scheduler(schedulers[-1])
        deadline = started+15
        while not crons.get_cron(ident).last_result and time.monotonic() < deadline:
            time.sleep(.03)
        saved = crons.get_cron(ident)
        if mode == 'verified_matches_only':
            assert saved.last_result != 'ok', saved
            messages = get_chat_store().load_messages('web:operator')
            assert not any('MEDIA:' in m.get('text', '') for m in messages), messages
            assert len(source_calls) == 1 and model.generation_calls == 1
            assert fired[-1]['response_fulfilled'] is False
            assert not fired[-1]['execution_outcome']['artifacts_delivered']
            return
        if saved.last_result != ('ok' if content_status == 'complete' else 'partial_success'):
            print('RESULT', json.dumps({k:v for k,v in fired[-1].items() if k in (
                'execution_outcome', 'cron_report_content_receipt', 'cron_task', 'cron_report_request',
                'cron_report_partial', 'cron_report_failed', 'cron_run_failed', 'artifacts',
                'response_fulfilled', 'artifact_delivery_satisfied', 'cron_delivery_status', 'reason')}, default=str))
        assert saved.last_result == ('ok' if content_status == 'complete' else 'partial_success'), (
            saved, model.generation_calls, model.turns, fired,
            [(e.event_type, e.payload) for e in runtime.store.list_events() if e.event_type.startswith('cron.delivery')])
        messages = get_chat_store().load_messages('web:operator')
        delivered = [m for m in messages if (m.get('metadata') or {}).get('artifacts')]
        assert len(delivered) == 1, messages
        text = delivered[0]['text']
        assert ('Partial report attached.' in text) is (content_status != 'complete'), text
        if content_status in {'partial', 'structured_partial'}:
            assert missing.rstrip('。.') not in text
            assert 'Some required checks remain incomplete.' in text
        paths = list((tmp_path/'workspaces').rglob('*.html'))
        if not paths:
            from nullion.artifacts import artifact_root_for_principal
            paths = list(Path(artifact_root_for_principal('web:operator')).glob('*.html'))
        assert len(paths) == 1 and b'item-a: 12' in paths[0].read_bytes(), paths
        assert len(source_calls) == 1 and model.generation_calls == 1
        assert time.monotonic()-started < 8, 'Completion metadata introduced extra slow work'
        events = [e for e in runtime.store.list_events() if e.event_type.startswith('cron.delivery')]
        assert any(e.event_type == 'cron.delivery.saved' for e in events)
        refreshed = client.get('/api/crons').json()
        assert saved.last_result in str(refreshed), refreshed
        traces = [e.payload for e in events if e.event_type == 'cron.delivery.agent_preflight'
            and e.payload.get('stage') == 'structured_artifact_recovered']
        assert traces and traces[-1]['report_content_status'] == (
            'partial' if content_status == 'structured_partial' else content_status), [(e.event_type, e.payload) for e in events]
