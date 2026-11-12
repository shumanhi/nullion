from __future__ import annotations

import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


def test_web_chat_recovers_persisted_model_alert_with_valid_provider_request(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import model_clients, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolRegistry

    for key in ('NULLION_TASK_DECOMPOSITION_ENABLED', 'NULLION_MULTI_AGENT_ENABLED', 'NULLION_SKILL_LEARNING_ENABLED'):
        monkeypatch.setenv(key, '0')
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'missing.env'))
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *args, **kwargs: None)
    requests = []
    reply = '受信しました。続けてください。'

    class Response:
        def __init__(self, body):
            self.body = body
            self.status_code = 400 if body.get('max_output_tokens', 16) < 16 else 200
            self.reason_phrase = 'Invalid request' if self.status_code == 400 else 'OK'
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def read(self):
            return b'{"error":{"message":"Expected max_output_tokens >= 16","code":"integer_below_min_value"}}'
        def raise_for_status(self): assert self.status_code == 200
        def iter_lines(self):
            yield 'data: '+json.dumps({'type': 'response.output_text.delta', 'delta': reply})
            yield 'data: '+json.dumps({'type': 'response.completed', 'response': {'status': 'completed'}})

    class ProviderHTTP:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def stream(self, method, url, *, json, headers, timeout):
            requests.append(json)
            return Response(json)

    monkeypatch.setattr(model_clients, '_httpx', SimpleNamespace(Client=ProviderHTTP))
    model = model_clients.CodexResponsesModelClient(api_key='synthetic', account_id='synthetic', model='test')
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_doctor_action({'action_id': 'stale-provider-alert', 'action_type': 'inspect',
        'recommendation_code': 'model_api_unreachable', 'status': 'pending',
        'summary': 'Model API is unreachable', 'severity': 'high', 'owner': 'runtime'})
    runtime.checkpoint(force=True)
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=ToolRegistry())
    elapsed = []
    with TestClient(app) as client:
        for index, prompt in enumerate(('こんにちは', 'مرحبا', '続けてください')):
            started = time.monotonic()
            result = client.post('/api/chat', json={'text': prompt, 'conversation_id': f'web:health-recovery-{index}'})
            elapsed.append(time.monotonic()-started)
            assert result.status_code == 200, result.text
            assert result.json()['text'] == reply, result.json()
            assert result.json().get('artifacts', []) == []
    saved = load_runtime_store(tmp_path/'runtime.db')
    action = next(row for row in saved.list_doctor_actions() if row['action_id'] == 'stale-provider-alert')
    assert action['status'] == 'cancelled'
    probes = [body for body in requests if body.get('max_output_tokens') == 16]
    assert len(probes) == 1
    assert all(body.get('max_output_tokens', 16) >= 16 for body in requests)
    assert sum(elapsed)/len(elapsed) < 3 and max(elapsed) < 4, elapsed


def test_web_chat_identifies_recorded_provider_quota_without_retrying_every_turn(tmp_path, monkeypatch):
    from datetime import UTC, datetime
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    for name in ('_schedule_web_builder_reflection', '_schedule_web_no_tool_memory_capture'):
        monkeypatch.setattr(web_app, name, lambda *a, **kw: None)
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', '0')
    diagnostic='Codex Responses request failed with HTTP 429: ' + json.dumps({'error': {
        'type':'usage_limit_reached', 'message':'利用上限に達しました', 'resets_in_seconds':600}})
    class Model:
        calls=0
        def create(self, *, messages, tools, max_tokens=8192, **kwargs):
            self.calls+=1
            assert max_tokens==16, 'Only the existing recovery check may reach the exhausted provider'
            raise RuntimeError(diagnostic)
    model=Model()
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    now=datetime.now(UTC).isoformat()
    runtime.store.add_doctor_action({'action_id':'quota', 'status':'pending', 'owner':'doctor',
        'action_type':'inspect', 'severity':'high', 'summary':'Review provider error', 'recommendation_code':'review_doctor_route',
        'reason':'source=telegram_chat;detail=Agent orchestrator error: '+diagnostic+';message_text=こんにちは',
        'created_at':now, 'updated_at':now})
    runtime.checkpoint(force=True)
    app=web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=ToolRegistry())
    with TestClient(app) as client:
        for index,prompt in enumerate(('続けて', 'Continúa', 'تابع')):
            turn_id=f'quota-{index}'
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:quota-{index}', 'turn_id':turn_id})
            assert result.status_code==200, result.text
            assert 'quota' in result.json()['text'] and 'reset' in result.json()['text'],result.json()
            assert 'connectivity' not in result.json()['text'] and not result.json().get('artifacts')
            assert result.json()['turn_id']==turn_id and result.json()['conversation_id']==f'web:quota-{index}'
            turns=[row for row in runtime.store.list_conversation_events(f'web:quota-{index}') if row.get('event_type')=='conversation.chat_turn']
            assert len(turns)==1 and turns[0]['turn_id']==turn_id
    assert model.calls==1
    observations=[event for event in runtime.store.list_events() if event.event_type=='model.provider_health']
    assert len(observations)==1 and observations[0].payload['provider_error']['category']=='quota'


@pytest.mark.parametrize('transport', ['http', 'websocket'])
def test_web_quota_warning_has_one_visible_and_durable_turn(tmp_path, monkeypatch, transport):
    from datetime import UTC, datetime
    from playwright.sync_api import expect, sync_playwright
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from test_web_surface_contracts import _UvicornThread, _free_port
    for key,value in {'NULLION_HOME':str(tmp_path/'home'),'NULLION_ENV_FILE':str(tmp_path/'absent.env'),
                      'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_MEMORY_ENABLED':'0'}.items():
        monkeypatch.setenv(key,value)
    for name in ('_schedule_web_builder_reflection', '_schedule_web_no_tool_memory_capture'):
        monkeypatch.setattr(web_app,name,lambda *a,**kw:None)
    error='Codex Responses request failed with HTTP 429: {"error":{"type":"usage_limit_reached"}}'
    class Provider:
        calls=0
        def create(self, *, max_tokens=8192, **kwargs):
            self.calls+=1
            assert max_tokens==16
            raise RuntimeError(error)
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    now=datetime.now(UTC).isoformat();cid='web:quota-ui-'+transport
    runtime.store.add_doctor_action({'action_id':'quota-ui','status':'pending','owner':'doctor','severity':'high',
        'action_type':'inspect','recommendation_code':'review_doctor_route','summary':'Review provider error',
        'error':error,'created_at':now,'updated_at':now})
    runtime.checkpoint(force=True)
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=ToolRegistry())
    with _UvicornThread(app,_free_port()) as server, sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        try:
            context=browser.new_context()
            context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
            if transport=='http':
                context.add_init_script("window.WebSocket=class { static OPEN=1;static CONNECTING=0; constructor(){throw new Error('WebSocket transport unavailable in this fixture');} };")
            page=context.new_page();posts=[]
            page.on('request',lambda r:posts.append(r.post_data_json) if r.method=='POST' and r.url.endswith('/api/chat') else None)
            page.goto(server.base_url,wait_until='domcontentloaded')
            if transport=='websocket':expect(page.get_by_text('Connected',exact=True)).to_be_visible(timeout=10000)
            page.locator('#user-input').fill('続けてください')
            page.locator('#send-btn').click()
            bubbles=page.locator('.msg.bot .bubble[data-turn-id]')
            expect(bubbles).to_have_count(1,timeout=15000)
            expect(bubbles.first).to_contain_text('quota',timeout=15000)
            events=[r for r in runtime.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn']
            assert len(events)==1
            assert bubbles.first.get_attribute('data-turn-id')==events[0]['turn_id']
            if transport=='http':assert len(posts)==1 and posts[0]['turn_id']==events[0]['turn_id']
            assert model.calls==1
            assert not page.locator('.msg.bot a[download]').count()
        finally:
            browser.close()
