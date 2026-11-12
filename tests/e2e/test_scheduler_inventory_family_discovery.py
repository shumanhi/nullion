"""Real API, scheduler stores and read scopes recover from the wrong inventory."""
import json,os,time
from datetime import datetime,UTC,timedelta
import pytest
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E')


@pytest.mark.parametrize('kind',['cron','reminder'])
@pytest.mark.parametrize('prompt,surface',[('対象「観測甲」の状態と間隔を確認してください。','browser'),('Comprueba el estado y el intervalo de «観測甲».','api'),('تحقق من حالة «観測甲» وفترة التحقق.','api')])
def test_unknown_named_task_is_not_declared_absent_from_an_empty_live_inventory(tmp_path,monkeypatch,kind,prompt,surface):
    from fastapi.testclient import TestClient
    from nullion import crons,chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_default_tool_registry,register_reminder_tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.reminders import ReminderRecord
    from test_memory_and_document_recall_contract import response,tool,ScriptedProvider
    for key,value in {'NULLION_HOME':tmp_path/'home','NULLION_ENV_FILE':tmp_path/'none.env',
        'NULLION_WORKSPACE_STORAGE_ROOT':tmp_path/'workspaces','NULLION_MEMORY_ENABLED':'0',
        'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_CHAT_STARTUP_WARMUP':'0'}.items():monkeypatch.setenv(key,str(value))
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'runtime.db');monkeypatch.setattr(chat_store,'_store',None)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    target=(crons.add_cron('観測甲','*/30 * * * *','Observe',enabled=False) if kind=='cron' else
        ReminderRecord('owned-reminder','web:inventory','観測甲',datetime.now(UTC)+timedelta(days=1)))
    if kind=='reminder':runtime.store.add_reminder(target)
    registry=create_default_tool_registry(workspace_root=tmp_path)
    register_reminder_tools(registry,runtime)
    expected='観測甲: disabled; every 30 minutes.' if kind=='cron' else '観測甲: pending one-off reminder.'
    class Model(ScriptedProvider):
        phase=0;plain=False;calls=0;total_calls=0;errors=[]
        def create(self,**kwargs):
            self.total_calls+=1
            try:return self._create(**kwargs)
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs)
            if protocol is not None:return protocol
            auxiliary=self.auxiliary(kwargs)
            if auxiliary is not None:return auxiliary
            self.calls+=1
            if self.plain:return response('Hello.')
            names={row['name'] for row in kwargs.get('tools',[])};self.phase+=1
            if not names:
                self.phase-=1
                return response('{}')
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['live_tracking'],'tool_names':['list_live_trackers']})
            assert {'list_crons','list_reminders','list_live_trackers'}<=names,names
            assert not names.intersection({'toggle_cron','run_cron','create_live_tracker','stop_live_tracker'}),names
            if self.phase==2:return tool('list_live_trackers',{'query':'観測甲'})
            if self.phase==3:
                assert 'single_kind_only' in json.dumps(kwargs['messages'])
                return tool('list_crons',{'query':'観測甲'}) if kind=='cron' else tool('list_reminders',{})
            assert self.phase==4
            return response(expected)
    model=Model();runtime.model_client=model
    app=create_app(runtime,AgentOrchestrator(model_client=model),registry)
    if surface=='browser':
        from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
        from playwright.sync_api import expect
        with _UvicornThread(app,_free_port()) as server:
            playwright,browser,page,errors=_open_page(server.base_url)
            try:
                page.locator('#user-input').fill(prompt)
                page.get_by_role('button',name='Send',exact=True).click()
                expect(page.locator('#messages')).to_contain_text(expected,timeout=20000)
                assert not errors and not model.errors,(errors,model.errors)
            finally:browser.close();playwright.stop()
    else:
        with TestClient(app) as client:
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:inventory'})
            assert result.status_code==200 and result.json()['text']==expected,(result.text,model.errors)
    with TestClient(app) as client:
        turn=next(e for e in reversed(runtime.store.list_conversation_events()) if e.get('event_type')=='conversation.chat_turn' and e.get('user_message')==prompt)
        assert turn.get('response_fulfilled') is True,turn
        assert [r['tool_name'] for r in turn['tool_results']]==['request_tool_scope','list_live_trackers','list_crons' if kind=='cron' else 'list_reminders']
        assert all(r['status']=='completed' for r in turn['tool_results'])
        if kind=='cron':assert crons.get_cron(target.id).enabled is False
        else:assert runtime.store.get_reminder(target.task_id).delivered_at is None
        model.plain=True;elapsed=[]
        for index,text in enumerate(['Hola','こんにちは','مرحبا']):
            before=model.total_calls;start=time.perf_counter()
            result=client.post('/api/chat',json={'text':text,'conversation_id':f'web:plain-{index}'})
            elapsed.append(time.perf_counter()-start)
            assert result.json()['text']=='Hello.' and model.total_calls==before+1
        assert sum(elapsed)/len(elapsed)<.5 and max(elapsed)<.9,elapsed


@pytest.mark.parametrize('action',['set_reminder','update_reminder','delete_reminder'])
def test_real_tool_boundary_never_turns_a_tool_grant_into_foreign_reminder_access(tmp_path,monkeypatch,action):
    from nullion.runtime import bootstrap_persistent_runtime,invoke_tool_with_boundary_policy
    from nullion.tools import ToolRegistry,ToolInvocation,register_reminder_tools
    from nullion.reminders import ReminderRecord
    from nullion.approvals import create_permission_grant
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=ToolRegistry()
    register_reminder_tools(registry,runtime)
    other=ReminderRecord('foreign','workspace:beta','Private',datetime.now(UTC)+timedelta(days=1))
    own=ReminderRecord('own','workspace:alpha','Visible',datetime.now(UTC)+timedelta(days=1))
    runtime.store.add_reminder(other);runtime.store.add_reminder(own)
    runtime.store.add_permission_grant(create_permission_grant(approval_id='tool-consent',principal_id='workspace:alpha',
        permission='tool:'+action,granted_by='fixture',granted_at=datetime.now(UTC)))
    read=invoke_tool_with_boundary_policy(runtime.store,ToolInvocation('read','list_reminders','workspace:alpha',{}),registry=registry)
    assert read.status=='completed' and [r['task_id'] for r in read.output['reminders']]==['own']
    args={'text':'Changed','chat_id':'workspace:beta','due_in_seconds':3600} if action=='set_reminder' else {'task_id':'foreign','text':'Changed','due_in_seconds':3600}
    result=invoke_tool_with_boundary_policy(runtime.store,ToolInvocation('write',action,'workspace:alpha',args),registry=registry)
    assert result.status=='denied' and result.output['reason']=='workspace_access_denied',result
    assert runtime.store.get_reminder('foreign')==other and runtime.store.get_reminder('own')==own
    assert len(runtime.store.list_reminders())==2
