"""One native action; Telegram approval; durable default Web restoration."""
import asyncio
from datetime import UTC, datetime
import os
import sqlite3
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1',reason='Docker workflow E2E')


@pytest.mark.parametrize('snapshot,locked,anchor',[(True,True,'scope'),(False,False,'event'),(True,False,'legacy')])
def test_telegram_callback_keeps_native_web_final_through_projection_retry_and_parallel_turn(tmp_path,monkeypatch,snapshot,locked,anchor):
    from playwright.sync_api import expect
    from nullion import chat_store,telegram_app,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _remember_chat_turn
    from nullion.config import NullionSettings,TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_store import conversation_event_observer
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolExecutor,ToolInvocation
    from tests.e2e.test_terminal_redirection_approval_contract import terminal_runtime,allow_selected_file
    from tests.e2e.test_quoted_reply_review_and_cleanup import text,call
    from tests.e2e.test_web_surface_contracts import _UvicornThread,_free_port,_open_page
    from tests.workflow_provider_protocol import workflow_provider_reply
    monkeypatch.setenv('NULLION_CODE_MODE','off')
    runtime,registry,workspace = terminal_runtime(tmp_path,monkeypatch)
    mirror = chat_store.ChatStore(tmp_path/'chat.db',key_path=tmp_path/'chat.key')
    monkeypatch.setattr(chat_store,'get_chat_store',lambda:mirror)
    conversation,source_turn = 'web:review','original-web-turn'
    prompt = '選択した一時ファイルを削除してください。' if snapshot else 'Remove the selected disposable file.'
    selected = workspace/'選択した一時ファイル.txt';selected.write_text('owned bytes')
    allow_selected_file(runtime,selected)
    command = "rm -f -- '"+str(selected)+"' 2>/dev/null"
    flow={'conversation_id':conversation}
    if anchor!='legacy':flow['browser_session_scope']=source_turn if anchor=='scope' else 'foreign-turn'
    inv = ToolInvocation('owned-native-delete','terminal_exec',conversation,{'command':command},flow_context=flow)
    blocked = ToolExecutor(store=runtime.store,registry=registry).invoke(inv)
    aid = blocked.output['approval_id']
    expected_turn = source_turn if anchor!='legacy' else 'approval-resume:'+aid
    runtime.process_conversation_message(conversation_id=conversation,chat_id='review',user_message=prompt,
        turn_id=source_turn,checkpoint=False)
    provisional = _remember_chat_turn(runtime,chat_id='review',conversation_id=conversation,user_message=prompt,
        assistant_reply='Tool approval requested: '+aid,conversation_turn_id=source_turn,tool_results=[] if anchor=='legacy' else [blocked])
    mirror.import_runtime_chat_turns([provisional])
    runtime.process_conversation_message(conversation_id='web:foreign',chat_id='foreign',user_message='Other source',
        turn_id='foreign-turn',checkpoint=False)
    runtime.process_conversation_message(conversation_id=conversation,chat_id='review',user_message='Later independent request',
        turn_id='later-turn',checkpoint=False)
    runtime.store.add_suspended_turn(SuspendedTurn(approval_id=aid,conversation_id=conversation,chat_id='review',
        message='/chat '+prompt,request_id='owned-request',message_id='owned-message',created_at=datetime.now(UTC),
        messages_snapshot=[{'role':'user','content':[{'type':'text','text':prompt}]}] if snapshot else [],
        pending_tool_calls=[{'invocation_id':inv.invocation_id,'tool_name':inv.tool_name,'status':'denied','output':blocked.output}],
        resume_token=None))
    runtime.checkpoint(force=True)
    # Another adapter process can hold an older runtime view until history read.
    web_runtime = bootstrap_persistent_runtime(runtime.checkpoint_path)
    executions=[]
    native_handler=registry._handlers['terminal_exec']
    def observed_handler(invocation):
        executions.append(invocation)
        return native_handler(invocation)
    registry._handlers['terminal_exec']=observed_handler
    class Model:
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            names={tool['name'] for tool in kwargs.get('tools',[])}
            if not names:return text('{}')
            if 'terminal_exec' in names and not executions:return call('terminal_exec',{'command':command})
            if 'terminal_exec' not in names and not executions:return call('request_tool_scope',{'capabilities':['local_shell'],
                'tool_names':['terminal_exec'],'required_tool_names':['terminal_exec']})
            return text('The selected file was removed.')
    model=Model();runner=AgentOrchestrator(model_client=model)
    settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',operator_chat_id='123',chat_enabled=True))
    edits,replies=[],[]
    class Message:
        chat=SimpleNamespace(id=123,send_action=lambda **kwargs:asyncio.sleep(0))
        message_id=7
        async def edit_text(self,text,**kwargs):edits.append(text)
        async def reply_text(self,text,**kwargs):replies.append(text)
    class Query:
        message=Message();from_user=SimpleNamespace(id=123)
        data=telegram_app._build_callback_data(kind='approval',action='allow_once',record_id=aid)
        async def answer(self,*args,**kwargs):pass
    service=telegram_app.ChatOperatorService(runtime,'123456:synthetic','123',settings,model_client=model,agent_orchestrator=runner)
    parallel=[]
    def concurrent_completion(event):
        if (not parallel and event.get('event_type')=='conversation.chat_turn'
                and event.get('turn_id')==expected_turn and event.get('history_state')=='terminal'):
            parallel.append(True)
            _remember_chat_turn(runtime,chat_id=conversation,conversation_id=conversation,user_message='Otra solicitud',
                assistant_reply='Resultado paralelo.',conversation_turn_id='parallel-turn',history_state='terminal')
    connection=sqlite3.connect(mirror._path)
    if locked:connection.execute('BEGIN IMMEDIATE')
    try:
        with conversation_event_observer(concurrent_completion):
            asyncio.run(service.on_callback_query(SimpleNamespace(callback_query=Query()),None))
        assert not selected.exists() and len(executions)==1
        assert executions[0].principal_id == conversation
        assert not replies,'Web-origin final must not be sent to the approving Telegram chat'
        if locked:
            assert mirror.load_messages(conversation)[-1]['text']=='Tool approval requested: '+aid
        persisted=bootstrap_persistent_runtime(runtime.checkpoint_path)
        own=[e for e in persisted.store.list_conversation_events(conversation) if e.get('history_state')=='terminal' and e.get('turn_id')==expected_turn]
        assert len(own)==1,own
        assert own[0]['approval_resume']['original_turn_resolved'] is (anchor!='legacy')
        assert own[0]['approval_resume']['original_turn_id'] == (source_turn if anchor!='legacy' else None)
        receipts=[r['output']['action_receipt'] for r in own[0]['tool_results'] if isinstance(r.get('output'),dict) and r['output'].get('action_receipt')]
        assert len(receipts)==1 and receipts[0]['changes'][0]['status']=='deleted'
        assert receipts[0]['changes'][0]['present_before'] is True and receipts[0]['changes'][0]['present_after'] is False
        assert persisted.store.list_conversation_chat_turns('telegram:review')==[]
    finally:
        connection.rollback();connection.close()
    assert parallel
    asyncio.run(service.on_callback_query(SimpleNamespace(callback_query=Query()),None))
    assert len(executions)==1
    def configure(page):
        page.add_init_script("localStorage.setItem('nullion_conv_id','web:review');")
    with _UvicornThread(web_app.create_app(web_runtime,orchestrator=None,registry=registry),_free_port()) as server:
        pw,browser,page,errors=_open_page(server.base_url,configure_page=configure)
        try:
            payload=page.request.get(server.base_url+'/api/chat/history/web:review').json()
            own_reply=own[0]['assistant_reply']
            assert [m['text'] for m in payload['messages'] if m['role']=='bot'].count(own_reply)==1
            # Simulate the original client's delayed save after native projection.
            response=page.request.post(server.base_url+'/api/chat/history/web:review/message',data={
                'role':'bot','text':'Tool approval requested: '+aid,
                'metadata':{'runtime_turn_id':source_turn,'history_message_kind':'terminal_reply','artifacts':[{'path':'/private/stale.txt'}]}})
            assert response.status==200
            page.reload(wait_until='domcontentloaded')
            expected=page.evaluate(r"text => {const el=document.createElement('div');el.innerHTML=renderMessageText(text);return el.textContent.replace(/\s+/g,' ').trim();}",own_reply)
            page.wait_for_function(r"text => Array.from(document.querySelectorAll('#messages .msg.bot .bubble')).filter(el=>el.textContent.replace(/\s+/g,' ').trim().includes(text)).length===1",arg=expected,timeout=10000)
            expect(page.locator('#messages')).to_contain_text('Resultado paralelo.')
            assert len(executions)==1 and not errors
            final=page.request.get(server.base_url+'/api/chat/history/web:review').json()
            if anchor!='legacy':
                assert 'Tool approval requested: '+aid not in [m['text'] for m in final['messages']]
        finally:
            browser.close();pw.stop()
