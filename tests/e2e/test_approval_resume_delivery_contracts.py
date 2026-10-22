"""Approval callbacks and real terminal artifact handling at product boundaries."""
from __future__ import annotations
import asyncio
from datetime import UTC,datetime
import json
import os
from pathlib import Path
from types import SimpleNamespace
import threading
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='container E2E only')


def setup_terminal(tmp_path,monkeypatch,backend):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_core_tool_registry
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    monkeypatch.setenv('NULLION_TERMINAL_ENABLED','1')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'storage'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=create_core_tool_registry(workspace_root=tmp_path,terminal_executor_backend=backend)
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)))
    runtime.active_tool_registry=registry;runtime.checkpoint(force=True)
    return runtime,registry


@pytest.mark.parametrize('prompt',['Inspect the local helper','ローカルプロセスを確認して','Revisa el proceso local'])
def test_diagnostics_through_chat_api_do_not_attach_internal_capture(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import TerminalBackendDescriptor,TerminalExecutionResult
    class Backend:
        calls=0
        def describe(self):return TerminalBackendDescriptor(mode='subprocess',attested_capabilities=(),metadata={})
        def run(self,*args,**kwargs):
            self.calls+=1
            return TerminalExecutionResult(exit_code=0,stdout='helper is running\n'*400,stderr='')
    backend=Backend();runtime,registry=setup_terminal(tmp_path,monkeypatch,backend)
    class Model:
        def create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            if not names:
                return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps({'requested_tool_names':['terminal_exec'],'required_tool_names':['terminal_exec'],'requested_artifact_extensions':[],'confidence':1})}]}
            if 'terminal_exec' not in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope','input':{'capabilities':['local_shell'],'tool_names':['terminal_exec'],'required_tool_names':['terminal_exec'],'artifact_extensions':[]}}]}
            if backend.calls==0:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'inspect','name':'terminal_exec','input':{'command':'printf status'}}]}
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'The local helper is running.'}]}
    model=Model();runtime.model_client=model
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)
    with TestClient(app) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:local-helper'})
        payload=response.json()
        history=client.get('/api/chat/history/web:local-helper').json()
    assert response.status_code==200,response.text
    assert payload.get('artifacts',[])==[],payload
    assert 'helper' in payload['text'] and 'MEDIA:' not in payload['text'],(payload, runtime.store.list_conversation_events('web:local-helper')[-3:])
    assert 'requested text' not in payload['text'],payload
    assert backend.calls==1
    turns=[t for t in runtime.store.list_conversation_events('web:local-helper') if t.get('event_type')=='conversation.chat_turn']
    terminal=next(t for t in turns[-1]['tool_results'] if t['tool_name']=='terminal_exec')
    assert terminal['output']['artifact_descriptors'][-1]['role']=='source'
    assert Path(terminal['output']['terminal_output_path']).is_file()
    assert all(not item.get('artifacts') for item in history['messages'])


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["button", "numeric_reply"])
async def test_telegram_double_callback_and_stale_callback_execute_once(tmp_path,monkeypatch,entry):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion import telegram_app
    from nullion.tools import TerminalBackendDescriptor,TerminalExecutionResult
    started=threading.Event();release=threading.Event()
    class Backend:
        calls=0
        def describe(self):return TerminalBackendDescriptor(mode='subprocess',attested_capabilities=(),metadata={})
        def run(self,*args,**kw):
            self.calls+=1;started.set();assert release.wait(5)
            return TerminalExecutionResult(exit_code=0,stdout='Synthetic operation completed.\n'*200,stderr='')
    backend=Backend();runtime,registry=setup_terminal(tmp_path,monkeypatch,backend)
    class Model:
        def create(self,**kw):return {'stop_reason':'end_turn','content':[{'type':'text','text':'Synthetic operation completed.'}]}
    runner=AgentOrchestrator(model_client=Model())
    snapshot=[{'role':'user','content':[{'type':'text','text':'合成操作を実行して'}]},
        {'role':'assistant','content':[{'type':'tool_use','id':'operation','name':'terminal_exec',
             'input':{'command':'printf fixture > /tmp/nullion-synthetic-approval.log'}}]}]
    denied=runner.resume_turn(conversation_id='telegram:77',principal_id='telegram_chat',user_message='合成操作を実行して',
        messages_snapshot=snapshot,tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    assert denied.suspended_for_approval
    approval_id=denied.approval_id;runtime.checkpoint()
    settings=load_settings(env={'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true','NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz'})
    service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token='fixture',operator_chat_id='77',agent_orchestrator=runner)
    runtime.active_tool_registry=registry
    class Message:
        chat_id=77;message_id=3;text='Approval';caption=None
        from_user=SimpleNamespace(id=77)
        def __init__(self):self.chat=SimpleNamespace(id=77,send_action=self.send_action);self.replies=[];self.edits=[]
        async def send_action(self,*args):pass
        async def reply_text(self,text,**kw):self.replies.append(text);return SimpleNamespace(message_id=len(self.replies)+10)
        async def edit_text(self,text,**kw):self.edits.append(text)
        async def reply_document(self,*args,**kw):pytest.fail('Internal terminal capture must not be uploaded')
    message=Message()
    class Callback:
        from_user=SimpleNamespace(id=77)
        def __init__(self):self.message=message;self.data=telegram_app._build_callback_data(kind='approval',action='allow_once',record_id=approval_id);self.answers=[]
        async def answer(self,text,**kw):self.answers.append(text)
    first=Callback()
    if entry == "numeric_reply":
        message.from_user = SimpleNamespace(id=888, is_bot=True)
        message.reply_markup = telegram_app._build_approval_markup(approval=runtime.store.get_approval_request(approval_id))
        numeric = SimpleNamespace(text="1", chat=message.chat, chat_id=77, message_id=4,
            from_user=SimpleNamespace(id=77), reply_to_message=message, reply_text=message.reply_text)
        task=asyncio.create_task(service.on_message(SimpleNamespace(message=numeric,update_id=42),SimpleNamespace(bot=SimpleNamespace(id=888))))
    else:
        task=asyncio.create_task(service.on_callback_query(SimpleNamespace(callback_query=first),SimpleNamespace()))
    if not await asyncio.to_thread(started.wait,3):
        await task
        pytest.fail(str((first.answers,message.edits,message.replies,settings.telegram.operator_chat_id,runtime.store.list_conversation_events("telegram:77"))))
    duplicate=Callback();await service.on_callback_query(SimpleNamespace(callback_query=duplicate),SimpleNamespace())
    release.set();await task
    await service.on_callback_query(SimpleNamespace(callback_query=Callback()),SimpleNamespace())
    assert backend.calls==1
    assert len(runtime.store.list_approval_requests())==1
    assert len(message.edits)==1,message.edits
    assert len(message.replies)==1,message.replies
    assert 'completed' in message.replies[0] and 'MEDIA:' not in message.replies[0]
    assert runtime.store.get_suspended_turn(approval_id) is None
