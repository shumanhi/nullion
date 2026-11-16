"""A status report cannot silently create an already-completed personal task."""
import json
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('prompt', ['Parcel is done.', '荷物の件は完了しました。', 'El paquete está resuelto.'])
@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('choice', ['1', '3'])
def test_missing_task_target_asks_numbered_question_and_followup_controls_creation(tmp_path, monkeypatch, prompt, surface, choice):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
    from nullion.personal_tasks import PersonalTaskToolRegistry
    from nullion.tools import ToolRegistry, ToolInvocation
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.workflow_provider_protocol import workflow_provider_reply, is_tool_scope_request
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', '0')
    runtime.active_tool_registry = ToolRegistry()
    class Model:
        phase = 0
        followup = False
        def create(self, **kwargs):
            if self.followup and choice == '3' and is_tool_scope_request(kwargs):
                return text(json.dumps({'web_action':'none', 'scheduler_action':'none', 'skill_pack_action':'none',
                    'requested_tool_names':['personal_tasks_list', 'personal_task_save'],
                    'required_tool_names':['personal_task_save'], 'confidence':1}))
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if self.followup and choice == '1':
                return text('Noted as your status update. Your personal task list is unchanged.')
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase in {1, 4}:
                return call('request_tool_scope', {'capabilities':['personal_tasks'],
                    'tool_names':['personal_tasks_list', 'personal_task_save'],
                    'required_tool_names':['personal_task_save']})
            if self.phase == 2: return call('personal_tasks_list', {'status':'all'})
            if self.phase == 3:
                return call('personal_task_save', {'title':'Parcel reference', 'status':'completed'})
            if self.phase == 5:
                return call('personal_task_save', {'operation':'create', 'title':'Parcel reference', 'status':'completed'})
            return text('Created a completed personal task named Parcel reference.')
    model = Model(); settings = NullionSettings(telegram=TelegramSettings(
        bot_token='123456:synthetic', operator_chat_id='42', chat_enabled=True))
    cid = 'web:task-target' if surface == 'web' else 'telegram:42'
    def current_tasks():
        registry = PersonalTaskToolRegistry(ToolRegistry(), runtime=runtime, workspace_id='workspace_admin')
        result = registry.invoke(ToolInvocation('read', 'personal_tasks_list', cid, {'status':'all'}))
        return result.output['items']
    def submit(client, value, turn_id):
        if surface == 'web':
            response = client.post('/api/chat', json={'text':value, 'conversation_id':cid, 'turn_id':turn_id})
            assert response.status_code == 200, response.text
            return response.json()['text']
        result = run_platform_chat_request(runtime, PlatformChatRequest(platform='telegram', text=value,
            chat_id='42', conversation_id=cid, turn_id=turn_id, settings=settings, model_client=model,
            agent_orchestrator=AgentOrchestrator(model_client=model), append_activity_trace=False))
        return result.text
    with TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=model), runtime.active_tool_registry)) as client:
        answer = submit(client, prompt, 'status-report')
        assert current_tasks() == [], answer
        assert all(f'{number}.' in answer for number in [1,2,3]), answer
        assert 'No personal task was changed' in answer, answer
        assert 'required step' not in answer and 'marked as completed' not in answer, answer
        assert model.phase == 3, 'Do not retry the rejected mutation or create a guessed task'
        model.followup = True
        answer = submit(client, choice, 'selected-choice')
        records = current_tasks()
        if choice == '1':
            assert not records and 'unchanged' in answer
        else:
            assert len(records) == 1 and records[0]['title'] == 'Parcel reference' and records[0]['status'] == 'completed'
            turns = [row for row in runtime.store.list_conversation_events(cid) if row.get('event_type')=='conversation.chat_turn']
            saves = [row for row in turns[-1]['tool_results'] if row['tool_name']=='personal_task_save']
            assert saves[-1]['output']['operation'] == 'created'
            assert saves[-1]['output']['action_receipt']['object_name'] == 'Parcel reference'


@pytest.mark.parametrize('prompt,target', [('Observatory calibration is done.', 'Observatory calibration'),
                                         ('観測所の調整は完了しました。', '観測所の調整')])
@pytest.mark.parametrize('surface', ['web', 'telegram'])
def test_unrelated_single_pending_task_cannot_be_substituted(tmp_path, monkeypatch, prompt, target, surface):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
    from nullion.personal_tasks import PersonalTaskToolRegistry
    from nullion.tools import ToolRegistry, ToolInvocation
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', '0')
    runtime.active_tool_registry = ToolRegistry()
    registry = PersonalTaskToolRegistry(ToolRegistry(), runtime=runtime, workspace_id='workspace_admin')
    record = registry.invoke(ToolInvocation('fixture', 'personal_task_save', 'fixture', {'title':'Return parcel'})).output['task']
    class Model:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            self.phase += 1
            if self.phase == 1: return call('request_tool_scope', {'capabilities':['personal_tasks'],
                'tool_names':['personal_tasks_list','personal_task_save'], 'required_tool_names':['personal_task_save']})
            if self.phase == 2: return call('personal_tasks_list', {'status':'pending'})
            assert self.phase == 3, 'No retry or invented task after typed target mismatch'
            return call('personal_task_save', {'task_id':record['task_id'], 'expected_revision':1,
                'target_title':target, 'status':'completed'})
    model = Model()
    with TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=model), runtime.active_tool_registry)) as client:
        if surface == 'web':
            response = client.post('/api/chat', json={'text':prompt, 'conversation_id':'web:target-mismatch'})
            assert response.status_code == 200, response.text
            reply = response.json()['text']
        else:
            settings = NullionSettings(telegram=TelegramSettings(bot_token='123:synthetic', operator_chat_id='42',chat_enabled=True))
            reply = run_platform_chat_request(runtime, PlatformChatRequest(platform='telegram', text=prompt,
                chat_id='42', conversation_id='telegram:42', turn_id='target-mismatch', settings=settings,
                model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model), append_activity_trace=False)).text
    assert 'No personal task was changed' in reply and '1.' in reply and '2.' in reply, {
        'reply':reply, 'turns':[{k:v for k,v in row.items() if k in {'assistant_reply','response_presentation','tool_results'}}
        for row in runtime.store.list_conversation_events('web:target-mismatch' if surface=='web' else 'telegram:42') if row.get('event_type')=='conversation.chat_turn']}
    saved = registry.invoke(ToolInvocation('read', 'personal_tasks_list','fixture',{'status':'all'})).output['items']
    assert len(saved)==1 and saved[0]['status']=='pending' and saved[0]['revision']==1


@pytest.mark.parametrize('prompt_text',['Complete observatory calibration.','観測所の校正を完了してください。','Completa la calibración del observatorio.'])
def test_unknown_named_target_cannot_complete_the_only_unrelated_task(tmp_path,monkeypatch,prompt_text):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.personal_tasks import PersonalTaskToolRegistry
    from nullion.tools import ToolRegistry,ToolInvocation
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);base=ToolRegistry()
    registry=PersonalTaskToolRegistry(base,runtime=runtime,workspace_id='workspace_admin')
    task=registry.invoke(ToolInvocation('seed','personal_task_save','operator',{'title':'Unrelated native task'})).output['task']
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['personal_tasks'],
                'tool_names':['personal_tasks_list','personal_task_save'],'required_tool_names':['personal_task_save']})
            if self.phase==2:return call('personal_tasks_list',{'status':'all'})
            if self.phase==3:return call('personal_task_save',{'task_id':task['task_id'],'target_title':task['title'],
                'expected_revision':task['revision'],'status':'completed'})
            assert self.phase==4
            return text('1. Show existing tasks.\n2. Leave tasks unchanged.')
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=Model()),base)) as client:
        response=client.post('/api/chat',json={'text':prompt_text,'conversation_id':'web:unknown-task'})
        assert response.status_code==200,response.text
        rows=registry.invoke(ToolInvocation('check','personal_tasks_list','operator',{'status':'all'})).output['items']
        assert len(rows)==1 and rows[0]['revision']==task['revision'] and rows[0]['status']=='pending'
        turn=runtime.store.list_recent_conversation_events('web:unknown-task',event_type='conversation.chat_turn')[-1]
        attempted=[r for r in turn['tool_results'] if r['tool_name']=='personal_task_save']
        assert attempted and all(r['status']=='failed' and r['output']['persisted'] is False for r in attempted),turn
        assert '1.' in response.json()['text'] and '2.' in response.json()['text'],response.text
