"""A pending scheduler confirmation preserves the actual request and numeric reply."""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker Web/SQLite boundary')


@pytest.mark.parametrize('prompt', [
    '「確認済みの予定」という停止状態の定期タスクを、毎日UTC午前0時に設定してください。処理は指定した情報源の確認です。作成前に確認してください。',
    'Crea una tarea recurrente pausada llamada 確認済みの予定 para revisar la fuente indicada cada día a medianoche UTC. Confirma antes de crearla.'])
@pytest.mark.parametrize('choice', ['1', '2'])
def test_declared_scheduler_action_waits_for_its_actual_missing_confirmation(tmp_path, monkeypatch, prompt, choice):
    from fastapi.testclient import TestClient
    from nullion import crons, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import create_default_tool_registry, register_cron_tools
    runtime = setup_runtime(tmp_path, monkeypatch)
    for key, value in {'NULLION_ENV_FILE': str(tmp_path/'none.env'), 'NULLION_MEMORY_ENABLED': '0',
        'NULLION_TASK_PLANNER_FEED_MODE': 'off', 'NULLION_CHAT_STARTUP_WARMUP': '0',
        'NULLION_ACTIVITY_TRACE_ENABLED': '0'}.items():
        monkeypatch.setenv(key, value)
    cid = 'web:pending-scheduler-confirmation'
    question = '指定済みの設定で、停止状態の予定を作成しますか？'
    options = ['作成する', '作成しない']
    name = '確認済みの予定'
    registry = create_default_tool_registry(workspace_root=tmp_path)
    register_cron_tools(registry)
    runtime.active_tool_registry = registry

    class Provider:
        confirmed = False
        declined = False
        reviews = 0
        call_tools = []
        scope_choices = []

        def create(self, **kw):
            system = kw.get('system') or ''; names = {row['name'] for row in kw.get('tools', [])}
            if names and len(self.call_tools) < 8:
                self.call_tools.append(sorted(names))
            try:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                scope_packet = isinstance(packet, dict) and packet.get('surface') == 'ordinary_chat' and 'registered_special_tool_names' in packet
            except (KeyError, IndexError, TypeError, ValueError):
                scope_packet = False
            if not names and scope_packet:
                self.scope_choices.append('none' if self.declined else 'mutate')
                return text_response(json.dumps({'scheduler_action': 'none' if self.declined else 'mutate',
                    'tracking_mode': 'unspecified' if self.declined else 'recurring',
                    'requested_tool_names': [] if self.declined else ['create_cron'],
                    'required_tool_names': [] if self.declined else ['create_cron'], 'confidence': 1.0}))
            if self.declined:
                protocol = workflow_provider_reply(kw, completion=True, reasoned=True)
                if protocol is not None:
                    return protocol
                return text_response('予定は作成せず、停止したままにします。')
            if 'material-requirements planner' in system and not self.confirmed:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [{
                        'requirement': '作成前にユーザーの確認を待つ', 'requirement_kind': 'user_input',
                        'evidence_subject_id': 'creation-confirmation', 'eligible_evidence_tool_names': []},
                        {'requirement': '指定した停止状態の予定を作成する', 'requirement_kind': 'action',
                         'evidence_subject_id': 'requested-scheduler-setup',
                         'eligible_evidence_tool_names': ['create_cron']}]}))
            if 'semantic completion reviewer' in system and not self.confirmed:
                self.reviews += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                return text_response(json.dumps({'disposition': 'needs_user_input',
                    'unresolved_requirements': ['作成前の確認'], 'retry_tool_names': [],
                    'all_material_requirements_enumerated': True, 'supported_findings': [],
                    'user_facing_failure': 'Pending user confirmation; no mutation performed.',
                    'material_requirements': [{'requirement_id': row['requirement_id'],
                        'satisfied': False, 'draft_covered': False, 'evidence_invocation_ids': []}
                        for row in packet['material_requirement_contract']],
                    'clarification': {'question': question, 'options': options, 'input_kind': 'other'}}))
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None:
                return protocol
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            if 'create_cron' not in names:
                assert 'request_tool_scope' in names, 'Fixture did not answer the structured scope protocol'
                return tool_response('request_tool_scope', {'capabilities': ['scheduler_mutate'],
                    'scheduler_action': 'mutate', 'tracking_mode': 'recurring', 'tool_names': ['create_cron'],
                    'required_tool_names': ['create_cron']})
            if not self.confirmed:
                return text_response(question+'\n\n1. '+options[0]+'\n2. '+options[1])
            if not crons.list_crons():
                return tool_response('create_cron', {'name': name, 'schedule': '0 0 1 1 *',
                    'task': 'Check the specified source.', 'enabled': False,
                    'delivery_channel': 'web', 'delivery_target': cid})
            return text_response(name+'を停止状態で作成しました。')

    provider = Provider(); runtime.model_client = provider
    with TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=provider), registry)) as api:
        first = api.post('/api/chat', json={'text': '/chat '+prompt, 'conversation_id': cid})
        assert first.status_code == 200, first.text
        body = first.json()['text']
        assert body.count(question) == 1 and '1.' in body and '2.' in body, body
        assert 'missing schedule' not in body and not crons.list_crons()
        turns = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')
        assert turns[-1].get('response_fulfilled') is not True
        assert not any(row['tool_name'] == 'create_cron' for row in turns[-1]['tool_results'])
        assert provider.reviews == 1
        provider.confirmed = choice == '1'; provider.declined = choice == '2'
        second = api.post('/api/chat', json={'text': choice, 'conversation_id': cid})
        assert second.status_code == 200, second.text
        if choice == '2':
            assert not crons.list_crons() and not second.json().get('approval_id'), second.json()
            turns = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')
            assert not any(row['tool_name'] == 'create_cron' for turn in turns for row in turn['tool_results'])
            assert '作成せず' in second.json()['text'], json.dumps({
                'turns': [{key: turn.get(key) for key in ('turn_id', 'parent_turn_id', 'user_message',
                    'pending_request', 'response_fulfilled')} for turn in turns],
                'received': runtime.store.list_recent_conversation_events(cid, event_type='conversation.message_received'),
                'model_tools': provider.call_tools, 'scope_choices': provider.scope_choices,
                'scopes': runtime.store.list_recent_conversation_events(cid, event_type='conversation.pending_choice_scope'),
                'phases': [[row.get('phase') for row in timing.get('phase_timings', [])] for timing in
                    runtime.store.list_recent_conversation_events(cid, event_type='conversation.latency_timing')]}, ensure_ascii=False)
            return
        if second.json().get('approval_id'):
            pending = runtime.store.get_approval_request(second.json()['approval_id'])
            last = runtime.store.list_recent_conversation_events(cid,
                event_type='conversation.chat_turn')[-1]
            assert pending.request_kind != 'agent_turn_limit_extension', json.dumps({'model_tools': provider.call_tools, 'scope_results': [
                {key: row.get('output', {}).get(key) for key in ('available_tools', 'unavailable_tools',
                    'scheduler_action', 'required_tool_names', 'reason', 'message')}
                for row in last.get('tool_results', [])[-2:]]}, ensure_ascii=False)
            approved = api.post('/api/approve/'+second.json()['approval_id'], json={'mode': 'once'})
            assert approved.status_code == 200, approved.text
        jobs = crons.list_crons()
        assert len(jobs) == 1 and jobs[0].name == name and jobs[0].enabled is False
        turns = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')
        assert turns[-1].get('response_fulfilled') is True, turns[-1]
        assert sum(row['tool_name'] == 'create_cron' and row['status'] == 'completed'
            for turn in turns for row in turn['tool_results']) == 1
        history = api.get('/api/chat/history/'+cid+'?sync_runtime=1').json()['messages']
        assert any(row['role'] == 'bot' and question in row['text'] for row in history)
