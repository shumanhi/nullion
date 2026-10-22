"""Personal tasks exercise the real Telegram handler, Web route and durable SQLite."""
from datetime import UTC, datetime
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


def response(text):
    return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': text}]}


def tool(name, args):
    return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': name, 'name': name, 'input': args}]}


class Model:
    def __init__(self, operation):
        self.operation = operation
        self.steps = 0
        self.main_calls = 0

    def create(self, **kwargs):
        system = str(kwargs.get('system') or '')
        names = {row['name'] for row in kwargs.get('tools', [])}
        if 'select_attachment_format' in names:
            return tool('select_attachment_format', {'extension': None, 'confidence': 1, 'embedded_media_extensions': []})
        if 'relationship' in system and 'target_index' in system:
            return response(json.dumps({'relationship': 'independent', 'effect': 'none', 'confidence': 1, 'target_index': None}))
        if 'web_action' in system and 'scheduler_action' in system:
            return response(json.dumps({'web_action': 'none', 'scheduler_action': 'none', 'skill_pack_action': 'none',
                'requested_tool_names': ['personal_task_save' if self.operation == 'add' else 'personal_tasks_list'],
                'required_tool_names': ['personal_task_save' if self.operation == 'add' else 'personal_tasks_list'], 'confidence': 1}))
        if not names and system:
            return response('{}')
        self.main_calls += 1
        needed = 'personal_task_save' if self.operation == 'add' else 'personal_tasks_list'
        if needed not in names:
            assert self.main_calls < 3, json.dumps(kwargs['messages'][-3:], ensure_ascii=False)
            assert 'request_tool_scope' in names
            return tool('request_tool_scope', {'capabilities': ['personal_tasks'], 'tool_names': [needed], 'required_tool_names': [needed]})
        self.steps += 1
        if self.steps == 1:
            return tool(needed, {'title': 'Return parcel', 'priority': 'normal'} if self.operation == 'add' else {})
        history = json.dumps(kwargs['messages'], ensure_ascii=False)
        assert 'persisted' in history and 'Return parcel' in history
        return response('Saved: Return parcel.' if self.operation == 'add' else 'Pending: Return parcel.')


@pytest.mark.parametrize('add_prompt,list_prompt', [
    ('Add parcel return to my todo list', 'What is on my todo list?'),
    ('Añade devolver el paquete a mi lista', '¿Qué está pendiente en mi lista?'),
    ('荷物の返品を覚えておいて', '未完了の用事を見せて'),
])
def test_telegram_add_and_web_recall_survive_restart_and_reset(tmp_path, monkeypatch, add_prompt, list_prompt):
    from fastapi.testclient import TestClient
    from nullion import chat_operator, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')  # Tasks must not depend on fact-memory retention.
    path = tmp_path / 'runtime.db'
    runtime = bootstrap_persistent_runtime(path)
    runtime.active_tool_registry = ToolRegistry()
    settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuv', operator_chat_id='2222', chat_enabled=True))
    model = Model('add')
    reply = chat_operator.handle_chat_operator_message(runtime, add_prompt, chat_id='2222', settings=settings,
        request_id='add-task', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
        append_activity_trace=False, allow_mini_agents=False)
    assert 'Saved: Return parcel' in reply, (reply,json.dumps([r for e in runtime.store.list_conversation_events('telegram:2222') for r in e.get('tool_results',[])],ensure_ascii=False))
    events = runtime.store.list_conversation_events('telegram:2222')
    assert any(row.get('tool_name') == 'personal_task_save' and row.get('output', {}).get('persisted')
               for event in events for row in event.get('tool_results', []))
    runtime.store.add_conversation_event({'conversation_id': 'telegram:2222', 'event_type': 'conversation.session_reset', 'created_at': datetime.now(UTC).isoformat()})
    runtime.checkpoint(force=True)
    restarted = bootstrap_persistent_runtime(path)
    read_model = Model('list')
    client = TestClient(web_app.create_app(restarted, AgentOrchestrator(model_client=read_model), ToolRegistry()))
    result = client.post('/api/chat', json={'text': list_prompt, 'conversation_id': 'web:task-recall'})
    assert result.status_code == 200
    payload = result.json()
    assert 'Pending: Return parcel' in json.dumps(payload), payload
    assert any(event.get('event_type') == 'workspace.personal_task' and event['task']['title'] == 'Return parcel'
               for event in restarted.store.list_conversation_events())


def test_history_recovery_restores_real_old_item_without_importing_assistant_examples(tmp_path, monkeypatch):
    from nullion import chat_operator
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    runtime.active_tool_registry = ToolRegistry()
    for ident, user, reply in [('old-add', 'Add amazon return to my todo list', 'Added Amazon return. Pending.'),
                                ('unrelated', 'Open a todo catalog https://example.test/catalog', 'The catalog is open.')]:
        runtime.store.add_conversation_event({'conversation_id': 'telegram:2222', 'workspace_id': 'workspace_admin',
            'event_type': 'conversation.chat_turn', 'turn_id': ident, 'created_at': datetime.now(UTC).isoformat(),
            'user_message': user, 'assistant_reply': reply})
    runtime.checkpoint(force=True)
    class Recovery(Model):
        def create(self, **kwargs):
            system = str(kwargs.get('system') or '')
            names = {row['name'] for row in kwargs.get('tools', [])}
            if 'select_attachment_format' in names or ('relationship' in system and 'target_index' in system) or not names:
                return super().create(**kwargs)
            if 'personal_tasks_list' not in names or 'chat_history_search' not in names or 'personal_task_save' not in names:
                assert 'request_tool_scope' in names
                return tool('request_tool_scope', {'capabilities': ['personal_tasks', 'conversation_history'],
                    'tool_names': ['personal_tasks_list', 'personal_task_save', 'chat_history_search'], 'required_tool_names': ['personal_tasks_list']})
            self.steps += 1
            if self.steps == 1:
                return tool('personal_tasks_list', {})
            if self.steps == 2:
                return tool('chat_history_search', {'query': 'todo list', 'limit': 5})
            if self.steps == 3:
                assert 'old-add' in json.dumps(kwargs['messages'])
                return tool('personal_task_save', {'title': 'Amazon return', 'source_turn_id': 'old-add'})
            return response('Recovered your pending item: Amazon return.')
    model = Recovery('list')
    settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuv', operator_chat_id='2222', chat_enabled=True))
    reply = chat_operator.handle_chat_operator_message(runtime, 'Whats in my todo list?', chat_id='2222', settings=settings,
        request_id='recover-task', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model), append_activity_trace=False, allow_mini_agents=False)
    assert 'Recovered your pending item: Amazon return' in reply, reply
    restarted = bootstrap_persistent_runtime(runtime.checkpoint_path)
    records = [event['task'] for event in restarted.store.list_conversation_events() if event.get('event_type') == 'workspace.personal_task']
    assert len(records) == 1 and records[0]['title'] == 'Amazon return' and records[0]['source_turn_id'] == 'old-add'


def test_personal_task_tools_keep_plain_chat_fast_without_storage_reads(tmp_path, monkeypatch, record_property):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.conversation_history_tools import with_conversation_history_tool
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.turn_context_policy import turn_tool_registry_for_evidence, TurnToolEvidence
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    monkeypatch.setattr('nullion.personal_tasks._records', lambda *args: (_ for _ in ()).throw(AssertionError('Plain chat scanned task records')))
    registry = turn_tool_registry_for_evidence(with_conversation_history_tool(ToolRegistry(), runtime=runtime, conversation_id='telegram:2222'), evidence=TurnToolEvidence(), model_client=None, user_message='', skip_tool_scope_decision=True)
    elapsed = []
    for prompt in ['Hello', 'Hola', 'こんにちは', 'Thanks', 'Gracias', 'ありがとう']:
        class Plain:
            calls = 0
            def create(self, **kwargs):
                self.calls += 1
                if self.calls > 1 or not kwargs.get('tools'):
                    time.sleep(2)
                    raise AssertionError('Unexpected scope/classifier call')
                assert {row['name'] for row in kwargs['tools']} == {'request_tool_scope','chat_history_search','memory_search'}
                return response('Hello.')
        model = Plain()
        start = time.perf_counter()
        result = AgentOrchestrator(model_client=model).run_turn(conversation_id='telegram:2222', principal_id='operator', user_message=prompt,
            conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
        elapsed.append(time.perf_counter()-start)
        assert result.final_text == 'Hello.' and model.calls == 1 and not result.tool_results
    assert sum(elapsed)/len(elapsed) < .5 and sorted(elapsed)[-1] < 1.5
    record_property('elapsed_seconds', json.dumps(elapsed))


def test_claimed_task_save_without_receipt_cannot_complete(tmp_path, monkeypatch):
    from nullion import chat_operator
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    runtime.active_tool_registry = ToolRegistry()
    class NoSave(Model):
        def create(self, **kwargs):
            names = {row['name'] for row in kwargs.get('tools', [])}
            if 'personal_task_save' in names:
                return response('Saved: Return parcel.')
            return super().create(**kwargs)
    model = NoSave('add')
    settings = NullionSettings(telegram=TelegramSettings(operator_chat_id='2222', chat_enabled=True))
    reply = chat_operator.handle_chat_operator_message(runtime, 'Añade devolver el paquete a mi lista', chat_id='2222', settings=settings,
        request_id='unsaved-task', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
        append_activity_trace=False, allow_mini_agents=False)
    assert 'Saved: Return parcel.' not in reply, reply
    assert not any(event.get('event_type') == 'workspace.personal_task' for event in runtime.store.list_conversation_events())
    turns = [event for event in runtime.store.list_conversation_events('telegram:2222') if event.get('event_type') == 'conversation.chat_turn']
    assert turns and any(row.get('tool_name') == 'request_tool_scope' and 'personal_task_save' in row.get('output', {}).get('required_tool_names', [])
                         for event in turns for row in event.get('tool_results', []))
