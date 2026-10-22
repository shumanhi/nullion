"""Real API, SQLite, cron workflow and platform message boundaries."""
from __future__ import annotations
import asyncio
import json
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1', reason='container E2E only')


def source_presentation_response(kwargs):
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    message=kwargs['messages'][0]['content'][0]['text']
    raw=message.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
    context=json.loads(raw)
    draft={'intro':'', 'points':[{'evidence_ids':[row['id']],
        'detail':'\n'.join(row['value'].splitlines()[1:]),'relevance':'Answers the requested comparison'} for row in context['evidence'][:7]],
        'limitation_ids':[row['id'] for row in context['limitations'][:3]]}
    return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'present','name':'present_source_answer','input':draft}]}

@pytest.mark.parametrize('prompt,requested,alternative,partial,recorded', [
    ('Find current opening times. No screenshots or other attachments are requested.', False, False, False, False),
    ('今の営業時間を教えて', False, True, True, False),
    ('Check availability and send a screenshot', True, False, True, False),
    ('近くの場所を比較してください', False, False, True, True),
])
def test_browser_source_delivery_and_alternative_lookup_through_api(tmp_path, monkeypatch, prompt, requested, alternative, partial, recorded, source_summary_model_reply):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools, browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.messaging_turn_graph import _latest_stored_turn_delivery_evidence
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available', lambda: True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python', lambda: '/worker/python')
    captures=[]
    def external_browser(task, **kw):
        from nullion.artifacts import artifact_path_for_generated_workspace_file
        path=artifact_path_for_generated_workspace_file(principal_id=kw['principal_id'],suffix='.png',stem='capture');Image.new('RGB',(30,30)).save(path);captures.append(path)
        if recorded:
            report = json.loads((Path(__file__).resolve().parents[1] / 'fixtures/nearby_source_report.json').read_text())
            return {**report, 'artifact_paths': [str(path)]}
        first=alternative and len(captures)==1
        return {'result_kind':'source_observations', 'report_status':'blocked' if first else 'partial' if partial else 'completed',
            'observations':[{'label':'answer','value':'unavailable' if first else '19:30',
                'quote':'unavailable' if first else '19:30','source_url':task.url}],
            'unknowns':['Availability'] if first else ['Unshown constraint'] if partial else [],'page_url':task.url,'artifact_paths':[str(path)],
            'continuation': {'kind':'alternative_source','tool_names':['browser_run_task'],'source_url':task.url} if first else None}
    monkeypatch.setattr(browser_use_worker,'run_isolated_browser_task',external_browser)
    class Model:
        calls=0
        def create(self, **kw):
            names={t['name'] for t in kw.get('tools',[])}
            reviewed=source_summary_model_reply(kw)
            if reviewed is not None: return reviewed
            if not names:
                return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps({'web_action':'browser_interaction','requested_tool_names':['browser_run_task'],'required_tool_names':['browser_run_task'],'requested_artifact_extensions':['.png'], 'source_artifact_delivery_requested':requested, 'source_artifact_request_evidence':prompt if requested else '', 'confidence':1})}]}
            if 'select_attachment_format' in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':'.png','confidence':1}}]}
            self.calls+=1
            if 'present_source_answer' in names:return source_presentation_response(kw)
            if self.calls==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope','input':{'capabilities':['web'],'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task'],'artifact_extensions':['.png'], 'source_artifact_delivery_requested':requested, 'source_artifact_request_evidence':prompt if requested else ''}}]}
            if self.calls==2 or alternative and self.calls==4:
                target='https://second.example/' if self.calls==4 else 'https://first.example/'
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.calls),'name':'browser_run_task','input':{'task':prompt,'url':target}}]}
            # Simulate the reported early stop. Runtime must continue instead.
            value='This source is blocked.' if alternative and self.calls==3 else 'Could not confirm anything. Try https://guessed.example/'
            return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');model=Model();runtime.model_client=model
    registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)
    with TestClient(app) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:new-user'})
        events=[e for e in runtime.store.list_conversation_events('web:new-user') if e.get('event_type')=='conversation.chat_turn']
        restored=client.get('/api/chat/history/web:new-user', params={'turn_ids':json.dumps([events[-1]['turn_id']])})
        assert restored.status_code == 200, restored.text
        assert bool(restored.json()['terminal_replies'][0]['artifacts']) is requested, restored.json()
    assert response.status_code==200,response.text
    payload=response.json()
    if recorded:
        from nullion.telegram_formatting import format_telegram_text
        text = payload['text']
        assert text.count('**Riverfront Park**') == 1, payload
        assert '\n\n- **The Peekskill Museum**' in text
        assert '(914) 736-0473' in text
        rendered, options = format_telegram_text(text)
        assert '<b>Riverfront Park</b>' in rendered
        assert '<a href="https://example.com/places">Source 1</a>' in rendered
        assert options == {'parse_mode': 'HTML'}
        assert restored.json()['terminal_replies'][0]['text'] == text
    else:
        assert '19:30' in payload['text'],payload
    assert 'guessed.example' not in payload['text'], payload
    assert 'unavailable' not in payload['text'], payload
    assert bool(payload['artifacts']) is requested,payload
    if partial:
        assert '**Not verified from this check:** ' in payload['text']
        from nullion.telegram_app import _deliver_reply
        sent = []
        class Message:
            chat = SimpleNamespace(id=100)
            message_id = 1
            async def reply_text(self, text, **kwargs):
                sent.append((text, kwargs))
                return SimpleNamespace(message_id=2)
        asyncio.run(_deliver_reply(Message(), payload['text'], principal_id='telegram:100'))
        assert len(sent) == 1
        assert '<b>Not verified from this check:</b>' in sent[0][0]
        assert sent[0][1]['parse_mode'] == 'HTML'
    events=[e for e in runtime.store.list_conversation_events('web:new-user') if e.get('event_type')=='conversation.chat_turn']
    results=events[-1]['tool_results'];browses=[t for t in results if t['tool_name']=='browser_run_task']
    assert len(browses)==(2 if alternative else 1)
    assert all(d['role']==('deliverable' if requested else 'source') for t in browses for d in t['output']['artifact_descriptors'])
    # Real Telegram/Slack/Discord preparation must not resurrect source files,
    # even when a draft model attempts to include their MEDIA directives.
    draft=payload['text']+'\n'+'\n'.join('MEDIA:'+str(p) for p in captures)
    event={**events[-1], "assistant_reply": draft}
    runtime.store.add_conversation_event(event)
    _, paths, _, _ = _latest_stored_turn_delivery_evidence(runtime, 'web:new-user', request_text=prompt)
    assert bool(paths) is requested, paths


def test_telegram_delivery_preserves_code_literals_through_real_adapter(tmp_path,monkeypatch):
    from nullion.telegram_app import _deliver_reply
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path))
    wire=[]
    class Message:
        chat=SimpleNamespace(id=100);message_id=1
        async def reply_text(self,text,**kwargs):
            wire.append((text,kwargs));return SimpleNamespace(message_id=2)
    code='<b>literal</b>\n| A | B | | --- | --- | | 1 | 2 |'
    asyncio.run(_deliver_reply(Message(),'**結果**\n```html\n'+code+'\n```',principal_id='telegram:100'))
    assert len(wire)==1
    assert '&lt;b&gt;literal&lt;/b&gt;' in wire[0][0]
    assert '| A | B | | --- | --- | | 1 | 2 |' in wire[0][0]
    assert wire[0][1]['parse_mode']=='HTML'
    receipt=json.loads((tmp_path/'delivery_receipts.jsonl').read_text().splitlines()[-1])
    assert receipt['status']=='succeeded'


def test_long_telegram_code_preserves_literal_content_in_every_delivered_chunk(tmp_path,monkeypatch):
    import html
    from nullion.telegram_app import _deliver_reply
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path))
    wire=[]
    class Message:
        chat=SimpleNamespace(id=100);message_id=1
        async def reply_text(self,text,**kwargs):
            assert len(text)<=4000
            wire.append((text,kwargs));return SimpleNamespace(message_id=len(wire)+1)
    code='\n'.join(f'    <b>literal-{i:04d}</b>' for i in range(350))
    asyncio.run(_deliver_reply(Message(),'```html\n'+code+'\n```',principal_id='telegram:100'))
    assert len(wire)>1
    for text,kwargs in wire:
        assert kwargs.get('parse_mode')=='HTML'
        assert '<pre><code' in text and '</code></pre>' in text
    delivered='\n'.join(html.unescape(text) for text,_ in wire)
    for i in range(350): assert f'    <b>literal-{i:04d}</b>' in delivered


@pytest.mark.parametrize('editable', [True, False])
def test_streamed_telegram_final_reply_has_a_real_delivery_receipt(tmp_path, monkeypatch, editable):
    from nullion.telegram_app import _deliver_reply, ChatStreamingMode
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path))
    wire=[]
    class Message:
        chat=SimpleNamespace(id=100); message_id=1
        async def reply_text(self, text, **kwargs):
            wire.append(('send', text))
            sent=SimpleNamespace(message_id=len(wire)+100)
            if editable:
                sent.edit_text=self.edit_text
            return sent
        async def edit_text(self, text, **kwargs):
            wire.append(('edit', text))
    reply='This is a complete readable report. ' * 12
    asyncio.run(_deliver_reply(Message(), reply, principal_id='telegram:100',
                              streaming_mode=ChatStreamingMode.MESSAGE_EDITS, request_id='streamed-final'))
    assert len(wire)>1
    assert wire[-1][1]==reply.strip() if editable else ''.join(text for _,text in wire)==reply.strip()
    receipts=[json.loads(line) for line in (tmp_path/'delivery_receipts.jsonl').read_text().splitlines()]
    assert len(receipts)==1 and receipts[0]['status']=='succeeded'
    assert receipts[0]['message_id']==str(101 if editable else len(wire)+100)
    assert receipts[0]['request_id']=='streamed-final'


def test_manual_cron_foreground_and_background_share_one_telegram_card(tmp_path, monkeypatch):
    from nullion.cron_delivery import CronRunDeliveryCallbacks, start_manual_cron_background_delivery
    from nullion.crons import CronJob
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import _send_or_edit_telegram_task_status_message
    runtime=bootstrap_persistent_runtime(tmp_path/'cron.db')
    messages,texts,locks={},{},{};network=[];sending=threading.Event();release=threading.Event();finished=threading.Event()
    class Bot:
        async def send_message(self,chat,text,**kw):
            network.append(('send',text));sending.set();await asyncio.sleep(.1)
            return SimpleNamespace(message_id=12)
        async def edit_message_text(self,**kw):
            network.append(('edit',kw['message_id'],kw['text']))
    async def deliver(group,text):
        return await _send_or_edit_telegram_task_status_message(Bot(),messages,chat_id='another-user',group_id=group,text=text,
            runtime=runtime,bot_token='unused',status_texts=texts,status_locks=locks)
    def status(group,text,terminal):
        result=asyncio.run(deliver(group,text))
        if terminal:finished.set()
        return result
    callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:'telegram',delivery_target=lambda j,c:'another-user',
        run_agent_turn=lambda j,c,**kw: (release.wait(2) and {'text':'Result delivered','artifacts':[]}),
        record_event=lambda kind,job,channel,target,conv,**kw: runtime.store.add_conversation_event({'event_type':kind,'conversation_id':conv,'cron_id':job.id,**kw}),
        block_reason=lambda *args:None,save_web_delivery=lambda *args:False,
        send_platform_delivery=lambda *args:True)
    receipt=start_manual_cron_background_delivery(CronJob(id='one-job',name='別の確認',schedule='0 * * * *',task='調べて',delivery_channel='telegram',delivery_target='another-user'),
        label='Manual scheduled task run',callbacks=callbacks,origin_conversation_id='telegram:another-user',
        status_update_callback=status, initial_status_timeout_seconds=.01, status_update_interval_seconds=0,background_start_grace_seconds=0)
    assert sending.wait(1)
    asyncio.run(deliver(receipt['task_group_id'],receipt['progress_status_text']))
    release.set();assert finished.wait(3)
    assert sum(row[0]=='send' for row in network)==1,network
    assert network[-1][0]=='edit' and 'Completed' in network[-1][2],network
    events=runtime.store.list_conversation_events('telegram:another-user')
    assert sum(e.get('event_type')=='cron.delivery.started' for e in events)==1,events


def test_plain_chat_with_browser_installed_stays_on_fast_path(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools, browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available', lambda: True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python', lambda: '/worker/python')
    browser_calls = []

    def slow_external_browser(*args, **kwargs):
        browser_calls.append(args)
        time.sleep(1)
        return {'result_kind': 'source_observations', 'report_status': 'blocked'}

    monkeypatch.setattr(browser_use_worker, 'run_isolated_browser_task', slow_external_browser)

    class Model:
        calls = 0

        def create(self, **kwargs):
            self.calls += 1
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'Hello.'}]}

    runtime = bootstrap_persistent_runtime(tmp_path / 'plain.db')
    model = Model()
    runtime.model_client = model
    registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    elapsed = []
    with TestClient(app) as client:
        for index, prompt in enumerate(('Hello', 'こんにちは', 'Hola', 'Bonjour', 'Hallo')):
            before = time.perf_counter()
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': f'web:plain-{index}'})
            elapsed.append(time.perf_counter() - before)
            assert response.status_code == 200, response.text
            assert response.json()['text'] == 'Hello.'
            assert response.json()['artifacts'] == []
            events = runtime.store.list_conversation_events(f'web:plain-{index}')
            turn = next(e for e in reversed(events) if e.get('event_type') == 'conversation.chat_turn')
            assert turn['tool_results'] == []
    assert browser_calls == []
    assert model.calls == len(elapsed)
    assert sum(elapsed) / len(elapsed) < .5, elapsed
    assert max(elapsed) < 1, elapsed


def test_validated_scope_clears_stale_capture_requirement_in_real_turn(tmp_path, monkeypatch):
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools, browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolScopeDecision, build_turn_tool_evidence
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available', lambda: True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python', lambda: '/worker/python')
    capture = tmp_path / 'internal.png'
    Image.new('RGB', (30, 30)).save(capture)
    monkeypatch.setattr(browser_use_worker, 'run_isolated_browser_task', lambda task, **kwargs: {
        'result_kind': 'source_observations', 'report_status': 'completed', 'goal_status': 'answered',
        'observations': [{'label': '予約時間', 'value': '19:30', 'quote': '19:30', 'source_url': task.url}],
        'unknowns': [], 'page_url': task.url, 'artifact_paths': [str(capture)]})
    class Model:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            if any(t['name']=='present_source_answer' for t in kwargs.get('tools',[])):
                return source_presentation_response(kwargs)
            if self.calls == 1:
                name, args = 'request_tool_scope', {'capabilities': ['web'], 'tool_names': ['browser_run_task'],
                    'required_tool_names': ['browser_run_task'], 'artifact_extensions': [], 'source_artifact_delivery_requested': False}
            elif self.calls == 2:
                name, args = 'browser_run_task', {'task': '予約時間', 'url': 'https://example.com/'}
            else:
                return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': '19:30'}]}
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': str(self.calls), 'name': name, 'input': args}]}
    model = Model()
    runtime = bootstrap_persistent_runtime(tmp_path / 'stale-context.db')
    base = ToolRegistry()
    register_browser_tools(base, policy=BrowserPolicy(), model_client_getter=lambda: model)
    scope = ScopedTurnToolRegistry(base, evidence=build_turn_tool_evidence(user_message='予約時間を教えて', conversation_result=None),
        tool_scope_decision=TurnToolScopeDecision(valid=True, requested_tool_names=('browser_run_task',)))
    result = AgentOrchestrator(model_client=model).run_turn(conversation_id='web:stale-capture', principal_id='operator',
        user_message='予約時間を教えて', conversation_history=[], tool_registry=scope,
        policy_store=runtime.store, approval_store=runtime.store,
        tool_flow_context={'artifact_extensions': ['.png'], 'requires_artifact_delivery': True})
    assert '19:30' in (result.final_text or ''), result
    assert result.artifacts == [], result
    output = next(row.output for row in result.tool_results if row.tool_name == 'browser_run_task')
    assert output['artifact_descriptors'][0]['role'] == 'source', output
    assert model.calls <= 3
