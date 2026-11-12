"""Verified multi-result changes survive evaluator and actual delivery adapters."""
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
import json
import os
import sys
import threading
from types import SimpleNamespace
from urllib.request import urlopen

import pytest
from tests.e2e.test_tracker_status_delivery import setup

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('channel', ['web', 'telegram'])
@pytest.mark.parametrize('structured', [False, True])
@pytest.mark.parametrize('emoji,markdown', [('none','plain'),('expressive','light')])
def test_verified_long_change_alert_reaches_history_and_platform_once(tmp_path, monkeypatch, channel, structured, emoji, markdown):
    from fastapi.testclient import TestClient
    from tests.e2e.test_shared_presentation_delivery import isolate_preferences
    isolate_preferences(tmp_path,monkeypatch)
    from nullion import config, web_app
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.live_tracking import LiveTrackerEngine, utcnow
    from nullion.tools import ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    deliveries = []
    class Bot:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
        async def send_message(self, chat_id, text, **kw):
            deliveries.append((str(chat_id), text, kw))
            return SimpleNamespace(message_id=len(deliveries))
        async def send_document(self, *a, **kw): raise AssertionError('Result alerts are text')
    monkeypatch.setitem(sys.modules, 'telegram', SimpleNamespace(Bot=Bot))
    settings = NullionSettings(doctor_enabled=False, telegram=TelegramSettings(bot_token='12345:synthetic'))
    monkeypatch.setattr(config, 'load_settings', lambda *a, **kw: settings)
    monkeypatch.setattr(web_app, 'load_settings', lambda *a, **kw: settings)
    lines = [f'結果 {i}: the confirmed result changed from {i} to {i+10}; this source now reports the updated total for district {i}.'
             for i in range(44)]
    summary = '\n'.join(lines)
    assert len(summary) > 4096
    state = {'text': 'Earlier counts'}
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps(state).encode(); self.send_response(200)
            self.send_header('Content-Type', 'application/json'); self.end_headers(); self.wfile.write(body)
        def log_message(self, *a): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    calls = []
    class Model:
        def create(self, **kw):
            assert kw['system'].count('Shared presentation policy')==1
            assert ('emojis liberally' in kw['system'])==(emoji=='expressive')
            packet = json.loads(kw['messages'][0]['content'][0]['text']); calls.append(packet)
            decision = {'status':'no_match'}
            if packet['snapshot_history']['baseline_established']:
                decision = {'status':'matched', 'summary':summary, 'evidence':[
                    {'step':0, 'pointer':'/observations/0/text', 'kind':'quote', 'value':line} for line in lines]}
                if structured:
                    decision.update(summary='複数の結果が更新されました。', sections=[
                        {'heading':'最初の結果', 'items':lines[:22]}, {'heading':'追加の結果', 'items':lines[22:]}])
            return {'content':[{'type':'text','text':json.dumps(decision)}]}
    app, runtime, db = setup(tmp_path, monkeypatch, provider=Model())
    registry = app.state.live_tracker_engine.registry
    def read(inv):
        with urlopen(f'http://127.0.0.1:{server.server_port}/', timeout=2) as response: output=json.load(response)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', output)
    registry.register(ToolSpec('results_read', 'Read external results', ToolRiskLevel.LOW,
                              ToolSideEffectClass.READ, False, 2), read)
    try:
        with TestClient(app) as client:
            csrf=client.get('/api/session').json()['csrf_token']
            assert client.post('/api/preferences',headers={'x-nullion-csrf':csrf},json={
                'emoji_level':emoji,'markdown_style':markdown}).json()['ok']
            app.state.live_tracker_engine.stop()
            store = app.state.live_tracker_store
            conversation = 'web:changes' if channel == 'web' else 'telegram:4242'
            job = store.create(name='Verified changes', principal_id=conversation, conversation_id=conversation,
                workspace_id='workspace_admin', request_key='owned-change-delivery',
                probe=[{'tool_name':'results_read','arguments':{}}],
                matcher={'kind':'semantic','history_mode':'snapshot','instruction':'変更をすべて通知'},
                interval_seconds=5, expires_at=utcnow()+timedelta(hours=1))
            for text in ['Earlier counts', summary]:
                state['text'] = text
                engine = LiveTrackerEngine(store, registry=registry, runtime_store=runtime.store,
                    model_client=Model(), deliver=app.state.live_tracker_engine.deliver)
                current, token = store.claim(4, now=utcnow()+timedelta(seconds=10))
                engine.check_once(current, token)
            current = store.get(job['id'])
            assert current['state'] == 'matched' and current['last_problem'] is None
            notice = store.claim_notice(); assert notice and all(line in notice['text'] for line in lines)
            if structured:
                assert ('\n\n最初の結果\n• ' if markdown=='plain' else '\n\n**最初の結果**\n- ') in notice['text']
                assert ('\n\n追加の結果\n• ' if markdown=='plain' else '\n\n**追加の結果**\n- ') in notice['text']
            assert ('🔔' in notice['text'])==(emoji=='expressive')
            engine._send(notice)
            assert store.get(job['id'])['notification_status'] == 'sent'
            assert store.claim_notice() is None
            history = client.get('/api/chat/history/'+conversation).json()['messages']
            matches = [m for m in history if (m.get('metadata') or {}).get('live_notice_id') == notice['id']]
            assert len(matches) == 1 and all(line in matches[0]['text'] for line in lines)
            assert len(calls) == 2, 'Formatting must not invoke another model'
            if channel == 'telegram':
                assert len(deliveries) >= 2
                combined = '\n'.join(text for _,text,_ in deliveries)
                class VisibleText(HTMLParser):
                    def __init__(self): super().__init__(); self.parts=[]
                    def handle_data(self, data): self.parts.append(data)
                rendered = VisibleText(); rendered.feed(combined)
                visible = ' '.join(''.join(rendered.parts).split())
                assert all(' '.join(line.split()) in visible for line in lines)
                assert all(target == '4242' and kw.get('parse_mode') == 'HTML' for target,_,kw in deliveries)
                receipts = [json.loads(line) for line in (tmp_path/'data/delivery_receipts.jsonl').read_text().splitlines()]
                assert receipts[-1]['status'] == 'succeeded' and receipts[-1]['channel'] == 'telegram'
    finally:
        server.shutdown();server.server_close()


@pytest.mark.parametrize('prompt',['新しい記事のみ。','Solo novedades.'])
def test_generic_semantic_match_delivers_verified_record_and_link_once(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion.live_tracking import LiveTrackerEngine,utcnow
    from nullion.tools import ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from tests.e2e.test_shared_presentation_delivery import isolate_preferences
    isolate_preferences(tmp_path,monkeypatch)
    state={'rows':[{'title':'Unrelated record','canonical_url':'https://example.test/unrelated'}]}
    class Model:
        calls=0
        def create(self,**kw):
            self.calls+=1
            packet=json.loads(kw['messages'][0]['content'][0]['text'])
            if not packet['snapshot_history']['baseline_established']:
                return {'content':[{'type':'text','text':json.dumps({'status':'no_match','checked_scopes':[{'step':0,'section_id':None}]})}]}
            return {'content':[{'type':'text','text':json.dumps({'status':'matched','summary':'Se ha confirmado un cambio.',
                'evidence':[{'step':0,'pointer':'/items/1/title','value':'Accessible garden opens'}]})}]}
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    registry=app.state.live_tracker_engine.registry
    def read(inv):return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'result_kind':'browser_item_collection',
        'items':list(state['rows']),'coverage':{'scope_found':True,'truncated':False}})
    registry.register(ToolSpec('news_read','Read external article collection',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    with TestClient(app) as client:
        app.state.live_tracker_engine.stop();store=app.state.live_tracker_store
        made=client.post('/api/live-trackers',json={'name':'Owned grounded alert','conversation_id':'web:grounded-context',
            'duration_seconds':3600,'interval_seconds':10,'stop_after_match':False,
            'probe':[{'tool_name':'news_read','arguments':{}}],'matcher':{'kind':'semantic','instruction':prompt,'history_mode':'snapshot'}})
        assert made.status_code==200,made.text
        engine=LiveTrackerEngine(store,registry=registry,runtime_store=runtime.store,model_client=model,deliver=app.state.live_tracker_engine.deliver)
        current,token=store.claim(1);engine.check_once(current,token)
        assert model.calls==1  # one evaluation establishes the baseline
        state['rows'].append({'title':'Accessible garden opens','canonical_url':'https://example.test/garden'})
        current,token=store.claim(1,now=utcnow()+timedelta(seconds=11));engine.check_once(current,token)
        notice=store.claim_notice();assert notice
        assert 'Accessible garden opens' in notice['text'] and 'https://example.test/garden' in notice['text']
        assert '/unrelated' not in notice['text'] and model.calls==2
        engine._send(notice);assert store.claim_notice() is None
        saved=client.get('/api/chat/history/web:grounded-context').json()['messages']
        delivered=[r for r in saved if (r.get('metadata') or {}).get('source')=='live_tracker']
        assert len(delivered)==1 and 'https://example.test/garden' in delivered[0]['text']
        assert store.get(made.json()['id'])['state']=='active'
        store.stop(made.json()['id'],'workspace_admin')
