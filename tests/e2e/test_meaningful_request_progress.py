"""Real shared runtime, Web UI and platform ingress; fake external endpoints only."""
import asyncio
from datetime import UTC, datetime
import json
import os
import time
import threading
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('surface', ['web', 'web_http', 'telegram', 'slack', 'discord'])
@pytest.mark.parametrize('tool_family', ['web_search', 'initial_ack', 'chat_history_search', 'calendar_list', 'calendar_then_search', 'code_mode'])
def test_progress_is_visible_without_verbose_and_final_is_delivered_once(tmp_path, monkeypatch, surface, tool_family):
    from nullion import telegram_app, slack_app, discord_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.tools import ToolRegistry, register_search_plugin, register_calendar_plugin
    from nullion.preferences import save_preferences
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, GLOBAL_PERMISSION_PRINCIPAL, PolicyMode
    from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'outbound_request_mode':'allow_all', 'emoji_level':'none', 'markdown_style':'plain'})
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED','false')
    monkeypatch.setenv('NULLION_CHAT_STREAMING_ENABLED','false')
    monkeypatch.setenv('NULLION_CODE_MODE','on' if tool_family in {'code_mode','calendar_then_search'} else 'off')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('sources',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    if tool_family in {'calendar_list','calendar_then_search'}:
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('calendar',GLOBAL_PERMISSION_PRINCIPAL,
            BoundaryKind.ACCOUNT_ACCESS,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
        from nullion import connections
        monkeypatch.setattr(connections,'_CONNECTIONS_PATH',tmp_path/'connections.json')
        connections.save_connection_registry({'connections':[{'connection_id':'fixture-calendar',
            'workspace_id':'workspace_admin','provider_id':'fixture_connector_provider',
            'display_name':'Fixture account','active':True,'permission_mode':'read'}]})
    prompt='カレンダーを確認してください。' if tool_family in {'calendar_list','calendar_then_search'} else '現在の選択肢を調べてください。'
    def search(query, limit):
        time.sleep(2 if tool_family=='calendar_then_search' else .08)
        return [{'title':'選択肢','url':'https://example.test/option','snippet':'Displayed price $17'}]
    monkeypatch.setenv('NULLION_PROVIDER_BINDINGS','search_plugin=brave_search_provider')
    monkeypatch.setenv('NULLION_BRAVE_SEARCH_API_KEY','local-fixture-not-a-real-key')
    monkeypatch.setattr('nullion.providers.resolve_plugin_provider_kwargs',lambda **kwargs:{'web_searcher':search})
    registry=ToolRegistry();register_search_plugin(registry,web_searcher=search)
    if tool_family in {'calendar_list','calendar_then_search'}:
        register_calendar_plugin(registry,calendar_lister=lambda *_:[{
            'summary':'Alpha $17','description':'SECRET /private/calendar',
            'start':{'dateTime':'2026-11-15T09:00:00Z'}}])
    if tool_family == 'chat_history_search':
        from nullion.conversation_history_tools import with_conversation_history_tool
        runtime.store.add_conversation_event({'conversation_id':'web:prior','workspace_id':'workspace_admin',
            'event_type':'conversation.chat_turn','turn_id':'saved-source','created_at':datetime.now(UTC).isoformat(),
            'user_message':'Cedar reference','assistant_reply':'Alpha, $17. https://example.test/option'})
        # It is intentionally absent from runtime.active_tool_registry: the
        # executing registry owns dynamically installed history metadata.
        history_registry=with_conversation_history_tool(registry,runtime=runtime,conversation_id='web:prior')
    else:
        history_registry=registry
    runtime.active_tool_registry=registry
    from nullion.chat_text import WORKING_ACK_SUMMARY
    expected_progress = (('Planning the next step' if surface in {'web','web_http'}
        else WORKING_ACK_SUMMARY.removesuffix('.')) if tool_family=='initial_ack' else
        'Calendar checked' if tool_family in {'calendar_list','calendar_then_search'} else
        '1 step completed' if tool_family=='chat_history_search' else '2 searches')
    class Model:
        calls=0
        requests=[]
        def program(self,names,code):
            result=call('code_mode',{'tool_names':names,'code':code})
            result['content'][0]['id']=f'program-{self.calls}'
            return result
        def create(self, **kwargs):
            protocol=workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:return protocol
            self.requests.append({'system':str(kwargs.get('system',''))[:900],
                'tools':[t['name'] for t in kwargs.get('tools',[])],
                'last_message':str(kwargs.get('messages',[])[-1:])[:900]})
            self.calls+=1
            if tool_family=='initial_ack' and self.calls==1:time.sleep(3.2)
            if tool_family in {'code_mode','calendar_list','calendar_then_search'} and self.calls==1:
                name='web_search' if tool_family=='code_mode' else 'calendar_list'
                return call('request_tool_scope',{'capabilities':(['connector','web'] if tool_family=='calendar_then_search'
                    else ['web' if tool_family=='code_mode' else 'connector']),
                    'tool_names':([name,'web_search'] if tool_family=='calendar_then_search' else [name]),
                    'required_tool_names':[name],
                    **({'connector_app_ids':['google-calendar']} if tool_family in {'calendar_list','calendar_then_search'} else {}),
                    'source_user_requested':True,'source_evidence':prompt})
            if self.calls==(2 if tool_family in {'code_mode','calendar_list','calendar_then_search'} else 1):
                if tool_family == 'chat_history_search':
                    return call('chat_history_search', {'query':'Cedar reference','limit':1})
                if tool_family in {'calendar_list','calendar_then_search'}:
                    if tool_family=='calendar_then_search':
                        return self.program(['calendar_list'],
                            "call_tool('calendar_list', {'start':'2026-11-15T00:00:00Z','end':'2026-11-16T00:00:00Z'})")
                    return call('calendar_list',{'start':'2026-11-15T00:00:00Z','end':'2026-11-16T00:00:00Z'})
                if tool_family=='code_mode':
                    return call('code_mode',{'tool_names':['web_search'],
                        'code':"call_tool('web_search', {'queries':['現在の選択肢','otras opciones'], 'independent_queries':True})"})
                return call('web_search',{'queries':['現在の選択肢','otras opciones'],'independent_queries':True})
            if tool_family=='calendar_then_search' and self.calls==3:
                time.sleep(3.2)
                return self.program(['web_search'],
                    "call_tool('web_search', {'queries':['現在の選択肢','otras opciones'], 'independent_queries':True})")
            # A slow external inference call must leave verified progress visible.
            time.sleep(3.2)
            return text('確認結果: Alpha, $17. https://example.test/option')
    model=Model();orchestrator=AgentOrchestrator(model_client=model)
    sent=[];edited=[];removed=[];typing=[]
    delivered=threading.Event();checked=[]
    checkpoint=type(runtime).checkpoint
    def checkpoint_after_delivery(self,*args,**kwargs):
        if self is runtime and delivered.is_set():
            before=len(typing)
            time.sleep(.12)
            assert len(typing)==before, 'Typing must stop at delivery, before runtime cleanup'
            checked.append(True)
        return checkpoint(self,*args,**kwargs)
    if surface=='telegram':
        monkeypatch.setattr(telegram_app,'_TYPING_KEEPALIVE_INTERVAL_SECONDS',.05)
        monkeypatch.setattr(type(runtime),'checkpoint',checkpoint_after_delivery)
    started=time.monotonic()
    if surface in {'web','web_http'}:
        from playwright.sync_api import expect
        from nullion.web_app import create_app
        from tests.e2e.test_web_surface_contracts import _UvicornThread,_free_port,_open_page
        with _UvicornThread(create_app(runtime,orchestrator,history_registry),_free_port()) as server:
            def configure(page):
                if surface=='web_http':
                    page.add_init_script("window.WebSocket = class extends WebSocket { constructor() { throw new Error('Fixture: upgrade unavailable'); } };")
            pw,browser,page,errors=_open_page(server.base_url,configure_page=configure)
            completed_http=[]
            page.on('response',lambda response: completed_http.append(response)
                if response.url==server.base_url+'/api/chat' and response.request.method=='POST' else None)
            try:
                page.locator('#user-input').fill(prompt)
                page.locator('#send-btn').click()
                expect(page.locator('#messages .status-bubble').filter(has_text=expected_progress)).to_have_count(1,timeout=4000)
                progress=page.locator('#messages .status-bubble').filter(has_text=expected_progress)
                assert progress.evaluate('(node) => getComputedStyle(node).fontSize')=='12px'
                if tool_family=='calendar_then_search':
                    latest=page.locator('#messages .status-bubble')
                    try:
                        expect(latest).to_contain_text('2 searches',timeout=7000)
                    except AssertionError as error:
                        turns=runtime.store.list_conversation_events(event_types=('conversation.chat_turn',))
                        raise AssertionError({'model_calls':model.calls,'requests':model.requests,'turns':turns}) from error
                    expect(latest).not_to_contain_text('Calendar checked')
                    progress=latest
                assert '\n\n' not in progress.inner_text()
                assert len(progress.inner_text())<120
                from pathlib import Path
                evidence=Path('.e2e-artifacts/compact-progress');evidence.mkdir(parents=True,exist_ok=True)
                page.screenshot(path=str(evidence/f'{surface}-{tool_family}.png'))
                page.set_viewport_size({'width':390,'height':780})
                assert progress.bounding_box()['width']<=390
                page.screenshot(path=str(evidence/f'{surface}-{tool_family}-mobile.png'))
                if tool_family=='initial_ack':
                    expect(page.locator('#messages .status-bubble')).to_contain_text('2 searches',timeout=5000)
                if surface=='web_http':
                    from urllib.parse import urlencode
                    ident=page.locator('#messages .status-bubble').get_attribute('data-turn-id')
                    conv=page.evaluate('conversationId')
                    path=server.base_url+'/api/chat/progress?'
                    assert page.request.get(path+urlencode({'turn_id':ident,'conversation_id':conv})).json()['active'] is True
                    assert page.request.get(path+urlencode({'turn_id':ident,'conversation_id':'web:other'})).json()['active'] is False
                expect(page.locator('#messages .msg.bot .bubble').filter(has_text='確認結果: Alpha')).to_have_count(1,timeout=5500)
                expect(page.locator('#messages .status-bubble')).to_have_count(0)
                if surface=='web_http':
                    # Producer text can render before the HTTP request has
                    # finalized. Check terminal cleanup after that boundary.
                    for _ in range(60):
                        if completed_http:break
                        page.wait_for_timeout(50)
                    assert completed_http and completed_http[-1].status==200
                    assert page.request.get(path+urlencode({'turn_id':ident,'conversation_id':conv})).json()['active'] is False
                assert not errors
            finally:browser.close();pw.stop()
    else:
        from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN,TelegramWire,install_wire
        wire=TelegramWire();original=wire.handle
        async def handle(request):
            method=request.url.path.rsplit('/',1)[-1]
            fields=parse_qs((await request.aread()).decode())
            if method=='sendChatAction':
                typing.append(time.monotonic())
                if len(typing)==1:return httpx.Response(500,json={'ok':False,'error_code':500,'description':'Temporary failure'})
                return httpx.Response(200,json={'ok':True,'result':True})
            if method=='editMessageText' and tool_family=='calendar_then_search' and not edited:
                edited.append(fields.get('text',[''])[0])
                return httpx.Response(500,json={'ok':False,'error_code':500,'description':'Temporary edit failure'})
            if method in {'editMessageText','deleteMessage'}:
                (edited if method=='editMessageText' else removed).append(fields.get('text',[''])[0])
                return httpx.Response(200,json={'ok':True,'result':True} if method=='deleteMessage' else
                    {'ok':True,'result':{'message_id':201,'date':1,'chat':{'id':77,'type':'private'},'text':fields['text'][0]}})
            if method=='sendMessage':
                sent.append(fields.get('text',[''])[0])
                if '確認結果: Alpha' in sent[-1]:delivered.set()
            return await original(request)
        wire.handle=handle;install_wire(monkeypatch,wire)
        settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77',
            'NULLION_TELEGRAM_CHAT_ENABLED':'true','NULLION_SLACK_OPERATOR_USER_ID':'U1',
            'NULLION_DISCORD_OPERATOR_USER_ID':'123','NULLION_SLACK_ENABLED':'true','NULLION_DISCORD_ENABLED':'true'})
        if tool_family == 'chat_history_search':
            runtime.active_tool_registry=history_registry
        service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token=TOKEN,
            operator_chat_id='77',model_client=model,agent_orchestrator=orchestrator)
        class Sent:
            async def edit(self, **kwargs):
                first=not edited
                edited.append(kwargs['content'])
                if tool_family=='calendar_then_search' and first:raise OSError('Temporary edit failure')
            async def delete(self):removed.append('receipt')
        class Channel:
            id='C1'
            async def send(self,value):sent.append(value);return Sent()
        channel=Channel()
        async def slack_send(**kwargs):sent.append(kwargs['text']);return {'ok':True,'ts':'receipt'}
        async def slack_edit(**kwargs):
            first=not edited
            edited.append(kwargs['text'])
            if tool_family=='calendar_then_search' and first:raise OSError('Temporary edit failure')
            return {'ok':True}
        async def slack_delete(**kwargs):removed.append(kwargs['ts']);return {'ok':True}
        async def say(value):sent.append(value);return {'ok':True,'ts':'final'}
        async def replay():
            if surface=='telegram':
                from telegram import Bot,Chat,Message,Update,User
                from nullion.telegram_transport import build_telegram_bot
                async with build_telegram_bot(Bot,TOKEN) as bot:
                    message=Message(message_id=1,date=datetime.now(UTC),chat=Chat(77,'private'),from_user=User(77,'Fixture',False),text=prompt)
                    message.set_bot(bot)
                    message.chat.set_bot(bot)
                    await service.on_message(Update(update_id=1,message=message),SimpleNamespace(bot=bot))
            elif surface=='slack':
                await slack_app.handle_slack_message(service,settings,event={'user':'U1','text':prompt,'channel':'C1','event_ts':'1'},
                    say=say,client=SimpleNamespace(chat_postMessage=slack_send,chat_update=slack_edit,chat_delete=slack_delete))
            else:
                await discord_app.handle_discord_message(service,settings,SimpleNamespace(
                    author=SimpleNamespace(id=123,bot=False),content=prompt,id='1',channel=channel,attachments=[]))
        monkeypatch.setattr(slack_app,'GLOBAL_TURN_DISPATCH_TRACKER',AsyncTurnDispatchTracker())
        monkeypatch.setattr(discord_app,'GLOBAL_TURN_DISPATCH_TRACKER',AsyncTurnDispatchTracker())
        asyncio.run(replay())
        if surface == 'telegram':
            from nullion.runtime_checkpoints import wait_runtime_checkpoints
            assert wait_runtime_checkpoints(runtime, timeout=5)
        assert len(sent)==2 and expected_progress in sent[0] and '確認結果: Alpha' in sent[1],(sent,edited)
        assert removed and not any('Working on your request' in s for s in sent), (sent,removed)
        if tool_family=='calendar_then_search':
            assert edited and '2 searches' in edited[-1], (sent,edited)
            assert 'Calendar checked' not in edited[-1], edited
        if surface=='telegram':assert len(typing)>=2 and checked,(typing,checked)
    turns=[e for e in runtime.store.list_conversation_events() if e.get('event_type')=='conversation.chat_turn']
    turns=[t for t in turns if t.get('turn_id')!='saved-source']
    assert len(turns)==1 and '確認結果: Alpha' in turns[0]['assistant_reply']
    result=next(r for r in turns[0]['tool_results'] if r['tool_name']==('calendar_list' if tool_family=='calendar_then_search'
        else 'web_search' if tool_family=='initial_ack' else tool_family))
    if tool_family in {'web_search','initial_ack','code_mode'}:
        native=next(r for r in turns[0]['tool_results'] if r['tool_name']=='web_search')
        assert native['output']['completed_queries']==2
        if tool_family=='code_mode':assert result['status']=='completed'
    elif tool_family=='chat_history_search':
        assert result['output']['suppress_activity'] is True and result['output']['match_count']==1
        assert not any('Cedar reference' in value for value in sent+edited)
    if tool_family in {'calendar_list','calendar_then_search'}:
        assert not any('SECRET' in value for value in sent+edited)
    assert model.calls==(4 if tool_family=='calendar_then_search' else 3 if tool_family in {'code_mode','calendar_list','calendar_then_search'} else 2), model.requests
    if surface in {'telegram','slack','discord'}:
        timings=runtime.store.list_conversation_events(event_types=('conversation.latency_timing',))
        marks=[mark for row in timings if row.get('surface')==surface for mark in row.get('marks',())]
        assert any(str(mark.get('name','')).startswith('progress_send_') and isinstance(mark.get('ms'),(int,float)) for mark in marks),timings
        if tool_family=='initial_ack':
            assert any(mark.get('name')=='progress_send_prepare' for mark in marks),marks
    assert time.monotonic()-started<(13 if tool_family=='calendar_then_search' else 11 if tool_family=='initial_ack' else 7)
