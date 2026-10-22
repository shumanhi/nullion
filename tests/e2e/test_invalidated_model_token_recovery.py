import base64
import json
import os
import time
import threading
from types import SimpleNamespace
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('matching_account', [True, False])
def test_invalidated_token_reuses_same_account_credential_at_real_chat_boundary(tmp_path, monkeypatch, surface, matching_account):
    from fastapi.testclient import TestClient
    from nullion import model_clients
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import ChatOperatorService
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    for key in ('NULLION_TASK_DECOMPOSITION_ENABLED', 'NULLION_MULTI_AGENT_ENABLED', 'NULLION_SKILL_LEARNING_ENABLED', 'NULLION_WEB_BACKGROUND_MEMORY_ENABLED'):
        monkeypatch.setenv(key, 'false')
    home=tmp_path/'home';home.mkdir()
    for key in ('HOME', 'NULLION_HOME', 'CODEX_HOME'):
        monkeypatch.setenv(key, str(home))
    monkeypatch.setenv('NULLION_ENV_FILE', str(home/'missing.env'))
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path/'data'))
    def token(account, identity):
        payload={'https://api.openai.com/auth': {'chatgpt_account_id': account}, 'exp': 9999999999, 'jti': identity}
        return 'header.'+base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')+'.signature'
    old=token('account', 'old');fresh=token('account' if matching_account else 'different-account', 'fresh')
    monkeypatch.setenv('NULLION_MODEL_PROVIDER', 'codex')
    monkeypatch.setenv('NULLION_MODEL', 'test')
    monkeypatch.setenv('NULLION_OPENAI_API_KEY', old)
    auth=home/'auth.json';auth.write_text(json.dumps({'tokens': {'access_token': fresh}}))
    monkeypatch.setenv('NULLION_CODEX_AUTH_JSON', str(auth))
    requests=[]
    reply='受信しました。続けてください。'
    class Response:
        def __init__(self, headers):
            self.headers=headers;self.status_code=200 if headers['Authorization']=='Bearer '+fresh and matching_account else 401
            self.reason_phrase='OK' if self.status_code==200 else 'Unauthorized'
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def read(self):return json.dumps({'error': {'code':'token_invalidated','message':'認証エラー'}}).encode()
        def raise_for_status(self):assert self.status_code==200
        def iter_lines(self):
            yield 'data: '+json.dumps({'type':'response.output_text.delta','delta':reply})
            yield 'data: '+json.dumps({'type':'response.completed','response':{'status':'completed'}})
    class HTTP:
        def __init__(self,**kwargs):pass
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def stream(self,method,url,*,json,headers):
            requests.append({'body':json,'headers':headers,'thread':threading.current_thread().name,'thread_id':threading.get_ident()})
            if headers['Authorization']=='Bearer '+old:time.sleep(.15)
            return Response(headers)
    monkeypatch.setattr(model_clients,'_httpx',SimpleNamespace(**{**vars(model_clients._httpx), 'Client': HTTP}))
    provider=model_clients.CodexResponsesModelClient(api_key=old,account_id='account',model='test')
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    orchestrator=AgentOrchestrator(model_client=provider)
    prompts=['比較する候補を教えてください','اعرض الخيارات المتاحة','Explain the available choices']
    elapsed=[];visible=[]
    if surface=='web':
        app=create_app(runtime,orchestrator=orchestrator,registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))
        with TestClient(app) as client:
            for prompt in prompts:
                start=time.monotonic();result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:token-recovery'})
                elapsed.append(time.monotonic()-start);assert result.status_code==200;visible.append(result.json()['text'])
    else:
        settings=NullionSettings(telegram=TelegramSettings(chat_enabled=True,operator_chat_id='123'))
        service=ChatOperatorService(runtime=runtime,bot_token='synthetic-token',operator_chat_id='123',settings=settings,model_client=provider,agent_orchestrator=orchestrator)
        for prompt in prompts:
            start=time.monotonic();visible.append(service.handle_text_message(text=prompt,chat_id='123',append_activity_trace=False));elapsed.append(time.monotonic()-start)
    runtime.checkpoint(force=True)
    if matching_account:
        assert visible==[reply]*3
        # Observe in-flight background retries before inspecting their receipt.
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            rejected=[i for i,r in enumerate(requests) if r['headers']['Authorization']=='Bearer '+old]
            if all(any(attempt['thread_id'] == requests[index]['thread_id']
                       for attempt in requests[index+1:]) for index in rejected):
                break
            time.sleep(.01)
        assert rejected
        assert len(requests)>=4
        for index in rejected:
            # Background memory extraction can interleave its own model call
            # with a foreground retry. Compare the next attempt by this actor,
            # rather than the next request globally.
            original = requests[index]
            retry = next((request for request in requests[index+1:]
                          if request['thread_id'] == original['thread_id']), None)
            assert retry is not None, original['thread']
            assert retry['headers']['Authorization']=='Bearer '+fresh
            assert retry['body']==original['body']
        assert all(r['headers']['Authorization'] in ('Bearer '+old,'Bearer '+fresh) and r['headers']['ChatGPT-Account-Id']=='account' for r in requests)
        conversation='web:token-recovery' if surface=='web' else 'telegram:123'
        turns=runtime.store.list_recent_conversation_events(conversation,event_type='conversation.chat_turn',limit=10)
        assert len(turns)>=3 and all(t.get('assistant_reply')==reply for t in turns[:3])
    else:
        assert all(v!=reply for v in visible)
        assert all(r['headers']['Authorization']=='Bearer '+old for r in requests), [
            {'model':r['body'].get('model'), 'thread':r['thread'], 'expected_token':r['headers']['Authorization']=='Bearer '+old,
             'replacement_token':r['headers']['Authorization']=='Bearer '+fresh, 'account':r['headers'].get('ChatGPT-Account-Id')}
            for r in requests
        ]
        assert len(requests)<=len(prompts)*3
    assert sum(elapsed)/len(elapsed)<2.5 and max(elapsed)<3.5,elapsed
