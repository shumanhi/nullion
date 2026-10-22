"""Recall through real routes, SQLite, browser tools and platform delivery."""
import asyncio
from html import unescape
import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')

PROMPTS = [
    ('Where is my Cedar parcel now?', 'Cedar', 'Cedar was delivered today; the carrier confirms receipt.'),
    ('¿Dónde está ahora mi paquete Cedro?', 'Cedro', 'El transportista confirma que Cedro se entregó hoy.'),
    ('青葉の荷物は今どこにありますか？', '青葉', '配送会社の最新情報で、青葉の荷物が本日配達済みと確認できました。'),
]


def setup(tmp_path, monkeypatch):
    from nullion import chat_store, turn_context_policy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, register_email_plugin
    for key, value in {'NULLION_HOME':tmp_path/'home', 'NULLION_ENV_FILE':tmp_path/'home'/'.env',
        'NULLION_WORKSPACE_STORAGE_ROOT':tmp_path/'storage','NULLION_TASK_PLANNER_FEED_MODE':'off',
        'NULLION_ACTIVITY_TRACE_ENABLED':'0','NULLION_MEMORY_ENABLED':'0','NULLION_CHAT_STARTUP_WARMUP':'0'}.items():
        monkeypatch.setenv(key,str(value))
    monkeypatch.setattr(chat_store,'_store',None)
    monkeypatch.setattr(turn_context_policy,'_active_connector_provider_context',lambda:[{
        'provider_id':'fixture','display_name':'Example account','active_app_ids':['google-mail'],
        'permission_mode':'read','structured_tools':['email_search','email_read']}])
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    reads=[]
    registry=register_email_plugin(ToolRegistry(),email_searcher=lambda query,limit: reads.append(query) or [],
        email_reader=lambda identity: pytest.fail('no unselected mailbox reads'))
    from datetime import UTC, datetime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL, BoundaryPolicyRule, BoundaryKind, PolicyMode
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-prior-permission',
        principal_id=OPERATOR_PERMISSION_PRINCIPAL, permission='tool:email_search',
        granted_by='fixture', granted_at=datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-account', OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.ACCOUNT_ACCESS, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    runtime.active_tool_registry=registry
    return runtime,registry,reads


def deliver_telegram(reply):
    from nullion.telegram_app import _deliver_reply
    wire=[]
    class Message:
        chat=SimpleNamespace(id=90210)
        message_id=1
        async def reply_text(self,text,**kwargs):
            wire.append(text)
            return SimpleNamespace(message_id=2)
    asyncio.run(_deliver_reply(Message(),reply,principal_id='telegram:90210'))
    assert wire
    return unescape('\n'.join(wire))


@pytest.mark.parametrize('source_detour',[False,True])
@pytest.mark.parametrize('surface',['api','telegram','browser'])
@pytest.mark.parametrize('prompt,topic,answer',PROMPTS)
def test_independent_followup_recovers_old_identifier_then_checks_live(tmp_path,monkeypatch,surface,prompt,topic,answer,source_detour):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends.auto_backend import AutoBackend
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    runtime,registry,reads=setup(tmp_path,monkeypatch)
    url='https://carrier.example.test/track/REF-317'
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'telegram:90211',
        'workspace_id':'workspace_admin','created_at':'2026-10-01T10:00:00Z','turn_id':'prior-identifier',
        'user_message':topic,'assistant_reply':f'{topic}: REF-317. Earlier status: shipped. {url}'})
    for index in range(35):
        runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'web:unrelated',
            'workspace_id':'workspace_admin','created_at':f'2026-10-10T10:{index:02}:00Z','turn_id':f'noise-{index}',
            'user_message':'Dimensions','assistant_reply':'Recorded unrelated dimensions.'})
    runtime.checkpoint(force=True)
    async def navigate(self,session_id,target):
        assert target==url
        return 'Navigated (status 200)'
    async def extract(self,session_id,selector=None):
        return 'REF-317: Delivered today. Carrier receipt confirmed.'
    async def close(self,*args,**kwargs): pass
    monkeypatch.setattr(AutoBackend,'navigate',navigate)
    monkeypatch.setattr(AutoBackend,'extract_text',extract)
    monkeypatch.setattr(AutoBackend,'close_session',close)
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','auto')
    register_browser_tools(registry)
    class Model(ScriptedProvider):
        steps=0
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.steps+=1
            if source_detour:
                if self.steps==1:return tool('request_tool_scope',{'capabilities':['connector'],
                    'tool_names':['email_search'],'required_tool_names':['email_search'],
                    'source_user_requested':False})
                if self.steps==2:return response('Please select a source.')
            step=self.steps-(2 if source_detour else 0)
            names={row['name'] for row in kw.get('tools',[])}
            if step==1:
                assert {'chat_history_search','memory_search'}<=names
                assert 'email_search' not in names
                return tool('chat_history_search',{'query':topic,'limit':3})
            context=json.dumps(kw['messages'],ensure_ascii=False)
            assert 'REF-317' in context and url in context
            if step==2:return tool('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_navigate','browser_extract_text']})
            if step==3:return tool('browser_navigate',{'url':url})
            if step==4:return tool('browser_extract_text',{})
            assert step==5,'completed current-status answer must not enter a recovery loop'
            assert 'Carrier receipt confirmed' in context
            return response(answer)
    model=Model();runtime.model_client=model
    orchestrator=AgentOrchestrator(model_client=model)
    conv='web:recall-status' if surface=='api' else 'telegram:90210'
    if surface=='api':
        with TestClient(create_app(runtime,orchestrator,registry)) as client:
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':conv})
            assert result.status_code==200,result.text
            reply=result.json()['text']
    elif surface=='browser':
        from test_web_surface_contracts import _UvicornThread, _free_port, _open_page
        from playwright.sync_api import expect
        with _UvicornThread(create_app(runtime,orchestrator,registry),_free_port()) as server:
            playwright,browser,page,errors=_open_page(server.base_url)
            try:
                page.locator('#user-input').fill(prompt)
                page.get_by_role('button',name='Send',exact=True).click()
                expect(page.locator('#messages')).to_contain_text(answer,timeout=15000)
                reply=page.locator('#messages').inner_text()
                conv=next(e['conversation_id'] for e in reversed(runtime.store.list_conversation_events())
                          if e.get('event_type')=='conversation.chat_turn' and e.get('assistant_reply')==answer)
                assert not errors,errors
            finally:browser.close();playwright.stop()
    else:
        reply=handle_chat_operator_message(runtime,'/chat '+prompt,chat_id='90210',
            settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuvwxyz',operator_chat_id='90210',chat_enabled=True)),agent_orchestrator=orchestrator,allow_mini_agents=False)
        reply=deliver_telegram(reply)
    assert answer in reply and not reads
    event=next(row for row in reversed(runtime.store.list_conversation_events(conv)) if row.get('event_type')=='conversation.chat_turn')
    names=[row['tool_name'] for row in event['tool_results']]
    assert names==(['request_tool_scope'] if source_detour else [])+['chat_history_search','request_tool_scope','browser_navigate','browser_extract_text'],names
    assert event['parent_turn_id'] is None


@pytest.mark.parametrize('prompt,topic,answer',PROMPTS)
def test_source_question_survives_and_numeric_reply_can_continue(tmp_path,monkeypatch,prompt,topic,answer):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    runtime,registry,reads=setup(tmp_path,monkeypatch)
    class Model(ScriptedProvider):
        calls=0
        followup=False
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.calls+=1
            if not self.followup:
                if self.calls==1:return tool('request_tool_scope',{'capabilities':['connector'],
                    'tool_names':['email_search'],'required_tool_names':['email_search'],'source_user_requested':False})
                return response('Choose a source before continuing.')
            names={row['name'] for row in kw.get('tools',[])}
            if 'email_search' not in names:
                return tool('request_tool_scope',{'capabilities':['connector'],'tool_names':['email_search'],
                    'source_user_requested':True,'source_evidence':'Email records'})
            if not reads:return tool('email_search',{'query':topic,'limit':3})
            return response('The selected mailbox has no matching confirmation.')
    model=Model();runtime.model_client=model
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        first=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:source'})
        assert first.status_code==200,first.text
        question=first.json()['text']
        assert '1.' in question and '2.' in question and 'required step' not in question
        assert not reads
        assert '1.' in deliver_telegram(question)
        model.followup=True
        second=client.post('/api/chat',json={'text':'1','conversation_id':'web:source'})
        assert second.status_code==200,second.text
        assert 'no matching confirmation' in second.json()['text'],second.json()
        assert reads==[topic]


def test_plain_turns_with_recall_available_do_not_search_or_classify(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from test_memory_and_document_recall_contract import ScriptedProvider,response
    runtime,registry,reads=setup(tmp_path,monkeypatch)
    class Model(ScriptedProvider):
        calls=0
        def create(self,**kw):
            if not kw.get('tools') and 'assistant_no_tool_draft' in str(kw):
                time.sleep(.8)
                pytest.fail('unrelated plain turn entered a scope classifier')
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.calls+=1
            return response('A complete self-contained answer.')
    model=Model();runtime.model_client=model
    elapsed=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,prompt in enumerate(['Explain a triangle.','三角形を説明してください。','Explica un triángulo.']):
            start=time.perf_counter()
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:plain-{index}'})
            elapsed.append(time.perf_counter()-start)
            assert result.status_code==200 and result.json()['text']=='A complete self-contained answer.'
            event=next(e for e in reversed(runtime.store.list_conversation_events(f'web:plain-{index}')) if e.get('event_type')=='conversation.chat_turn')
            assert event['tool_results']==[]
    record_property('plain_recall_average_seconds',sum(elapsed)/len(elapsed))
    record_property('plain_recall_max_seconds',max(elapsed))
    assert sum(elapsed)/len(elapsed)<.8 and max(elapsed)<1.2
    assert model.calls==3 and not reads


@pytest.mark.parametrize('case',['latest','ambiguous','reset','other_workspace','unicode','missing'])
def test_recall_answer_and_boundaries_complete_through_real_api(tmp_path,monkeypatch,case):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.chat_store import ChatStore
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    runtime,registry,reads=setup(tmp_path,monkeypatch)
    def seed(identity,text,reply,date='2026-10-01T10:00:00Z',workspace='workspace_admin'):
        event={'event_type':'conversation.chat_turn','conversation_id':'telegram:90211',
            'workspace_id':workspace,'created_at':date,'turn_id':identity,
            'user_message':text,'assistant_reply':reply}
        runtime.store.add_conversation_event(event)
        return event
    query='Cedar reference'
    if case=='latest':
        seed('old',query,'Cedar reference: first observation.')
        seed('new',query,'Cedar reference: latest observation.','2026-10-02T10:00:00Z')
        answer='The latest saved observation is from October 2.'
    elif case=='ambiguous':
        seed('a',query,'Cedar reference A: https://records.example.test/a')
        seed('b',query,'Cedar reference B: https://records.example.test/b','2026-10-02T10:00:00Z')
        answer='Which reference should I use?\n1. Cedar reference A\n2. Cedar reference B'
    elif case=='reset':
        old=seed('old',query,'REMOVED-PRIVATE-DETAIL')
        encrypted=ChatStore(db_path=tmp_path/'chat_history.db',key_path=tmp_path/'chat_history.key')
        encrypted.import_runtime_chat_turns([old])
        runtime.store.add_conversation_event({'event_type':'conversation.session_reset','conversation_id':'telegram:90211',
            'workspace_id':'workspace_admin','created_at':'2026-10-02T10:00:00Z'})
        answer='No saved reference remains after that conversation was reset.'
    elif case=='other_workspace':
        seed('private',query,'OTHER-WORKSPACE-PRIVATE-DETAIL',workspace='workspace_other')
        answer='I did not find that reference in this workspace.'
    elif case=='unicode':
        query='青葉の現在地'
        seed('unicode','青葉の発送記録','保存した受付番号は３１７です。')
        for index in range(10):seed(f'unrelated-{index}',f'別件{index}','記録済み')
        answer='保存した青葉の受付番号は３１７です。現在の配送状況はまだ確認していません。'
    else:
        seed('unrelated','Completely separate dimensions','NOT-A-MATCH')
        answer='I could not find a saved reference; please provide its identifier.'
    runtime.checkpoint(force=True)
    class Model(ScriptedProvider):
        steps=0
        followup=False
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.steps+=1
            if self.steps==1:return tool('chat_history_search',{'query':query,'limit':4})
            context=json.dumps(kw['messages'],ensure_ascii=False)
            assert 'OTHER-WORKSPACE-PRIVATE-DETAIL' not in context and 'REMOVED-PRIVATE-DETAIL' not in context
            if not self.followup:
                assert self.steps==2,'a complete recalled answer or clarification must not be replaced by another lookup'
                if case=='latest':
                    assert context.index('latest observation')<context.index('first observation')
                if case=='unicode':assert '３１７' in context
                return response(answer)
            if self.steps==3:return tool('chat_history_search',{'query':'Cedar reference A','limit':1})
            assert self.steps==4
            return response('Selected Cedar reference A: https://records.example.test/a')
    model=Model();runtime.model_client=model
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        result=client.post('/api/chat',json={'text':'Recover the saved reference: '+query,'conversation_id':'web:boundary'})
        assert result.status_code==200,result.text
        assert result.json()['text']==answer,result.json()
        if case=='ambiguous':
            model.followup=True
            selected=client.post('/api/chat',json={'text':'1','conversation_id':'web:boundary'})
            assert selected.status_code==200,selected.text
            assert selected.json()['text']=='Selected Cedar reference A: https://records.example.test/a',selected.json()
        assert not reads


@pytest.mark.parametrize('user_text',['Cedar status?',''])
def test_history_route_preserves_distinct_repeated_turns_on_repeat_restore(tmp_path,monkeypatch,user_text):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    runtime,registry,_=setup(tmp_path,monkeypatch)
    conv='telegram:90210'
    for index in (1,2):
        runtime.store.add_conversation_event({'event_type':'conversation.chat_turn',
            'conversation_id':conv,'workspace_id':'workspace_admin','turn_id':f'run-{index}',
            'created_at':f'2026-10-0{index}T10:00:00Z','user_message':user_text,
            'assistant_reply':'Still in transit.'})
    runtime.checkpoint(force=True)
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=None),registry)) as client:
        for _ in range(2):
            result=client.get(f'/api/chat/history/{conv}',params={'sync_runtime':'true'})
            assert result.status_code==200,result.text
            rows=result.json()['messages']
            assert [r['text'] for r in rows]==([user_text,'Still in transit.']*2 if user_text else ['Still in transit.']*2)
            assert all('runtime_turn_id' not in r and 'runtime_turn_id' not in r.get('metadata',{}) for r in rows)


@pytest.mark.parametrize('action',['archive','clear','hard','date'])
def test_history_actions_preserve_archive_and_never_backfill_deleted_turns(tmp_path,monkeypatch,action):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion import web_app
    from nullion.web_app import create_app, _remember_web_chat_turn
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    runtime,registry,reads=setup(tmp_path,monkeypatch)
    old='web:history-action-old'
    _remember_web_chat_turn(runtime,conversation_id=old,user_message='CedarParcel source reference',
        assistant_reply='CedarParcel reference REF-618; earlier observation only.')
    runtime.checkpoint(force=True)
    class Model(ScriptedProvider):
        calls=0
        def create(self,**kw):
            if not kw.get("tools"):return response("{}")
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.calls+=1
            if self.calls==1:return tool('chat_history_search',{'query':'CedarParcel','limit':5})
            evidence=[m for m in kw['messages'] if m.get('role')=='user' and isinstance(m.get('content'),list)
                for b in m['content'] if b.get('type')=='tool_result']
            encoded=json.dumps(evidence)
            if action=='archive':assert 'REF-618' in encoded,encoded
            else:assert 'REF-618' not in encoded,encoded
            return response('Saved reference REF-618.' if action=='archive' else 'No saved reference remains.')
    model=Model();runtime.model_client=model
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        if action=='archive':changed=client.post(f'/api/chat/history/{old}/archive')
        elif action=='date':
            from nullion import chat_store
            from datetime import datetime, UTC
            monkeypatch.setattr(chat_store,'_local_history_timezone',lambda:UTC)
            changed=client.delete('/api/history/conversations/web/'+datetime.now(UTC).date().isoformat())
        else:changed=client.delete(f'/api/chat/history/{old}'+('/hard' if action=='hard' else ''))
        assert changed.status_code==200,changed.text
        from nullion.runtime import bootstrap_persistent_runtime
        persisted=bootstrap_persistent_runtime(runtime.checkpoint_path)
        boundaries=[e for e in persisted.store.list_conversation_events(old)
            if e.get('event_type')=='conversation.session_reset']
        assert boundaries and (boundaries[-1].get('history_retained') is True)==(action=='archive')
        web_app._RUNTIME_HISTORY_SYNC_MTIMES.clear();web_app._RUNTIME_HISTORY_SYNC_CHECKED_AT.clear()
        restored=client.get(f'/api/chat/history/{old}',params={'sync_runtime':'true'})
        assert restored.status_code==200,restored.text
        assert bool(restored.json()['messages']) is (action=='archive'),restored.json()
        result=client.post('/api/chat',json={'text':'Recover our CedarParcel reference from saved history.',
            'conversation_id':'web:history-action-new'})
        assert result.status_code==200,result.text
        assert result.json()['text']==('Saved reference REF-618.' if action=='archive' else 'No saved reference remains.')
        assert model.calls==2 and not reads


def test_new_session_clears_automatic_context_but_explicit_history_still_recalls(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import handle_chat_operator_message,_recent_tool_scopes_for_context
    from nullion.config import NullionSettings,TelegramSettings
    from nullion.web_app import create_app
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    runtime,registry,reads=setup(tmp_path,monkeypatch)
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'telegram:90210',
        'workspace_id':'workspace_admin','turn_id':'original-before-new','created_at':'2026-10-01T10:00:00Z',
        'user_message':'Cedar parcel reference','assistant_reply':'Recorded reference REF-740, previously shipped.',
        'tool_results':[{'tool_name':'email_search','status':'completed','output':{'results':[]}}]})
    runtime.checkpoint(force=True)
    settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuvwxyz',operator_chat_id='90210',chat_enabled=True))
    assert handle_chat_operator_message(runtime,'/new',chat_id='90210',settings=settings)=='Starting fresh.'
    assert runtime.store.list_recent_conversation_events_after_reset('telegram:90210',event_type='conversation.chat_turn')==[]
    assert _recent_tool_scopes_for_context(runtime,'telegram:90210')==()
    class Model(ScriptedProvider):
        steps=0
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.steps+=1
            if self.steps==1:return tool('chat_history_search',{'query':'Cedar parcel reference','limit':3})
            assert 'REF-740' in json.dumps(kw['messages'])
            return response('Earlier recorded reference: REF-740; current status is unverified.')
    model=Model();runtime.model_client=model
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        result=client.post('/api/chat',json={'text':'Recover the previously recorded Cedar parcel reference.',
            'conversation_id':'web:after-messaging-new'})
        assert result.status_code==200,result.text
        assert 'REF-740' in result.json()['text'] and not reads,result.json()
