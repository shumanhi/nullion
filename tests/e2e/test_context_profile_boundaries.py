"""Real Web/SQLite and SSE boundaries for context/profile invariants."""
import json
import os
import time

import pytest

from tests.e2e.test_generated_artifact_protocol_recovery import model_boundary, text

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('truncated',[False,True])
def test_real_web_preserves_fast_chat_and_recovers_truncated_first_response(tmp_path,monkeypatch,truncated):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.model_clients import CodexResponsesModelClient
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('HOME',str(tmp_path))
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    for key in ('NULLION_TASK_DECOMPOSITION_ENABLED','NULLION_MULTI_AGENT_ENABLED','NULLION_SKILL_LEARNING_ENABLED','NULLION_MEMORY_ENABLED'):
        monkeypatch.setenv(key,'0')
    monkeypatch.setenv('NULLION_WEB_INTERACTIVE_FAST_MAX_TOKENS','768')
    monkeypatch.setattr(web_app,'_schedule_web_builder_reflection',lambda *a,**k:None)
    monkeypatch.setattr(web_app,'_schedule_web_no_tool_memory_capture',lambda *a,**k:None)
    chat_store._store=None
    calls=[]
    def provider(body):
        cap=body.get('max_output_tokens')
        calls.append(cap)
        if truncated and cap==768:
            return [{'type':'response.incomplete','response':{'status':'incomplete',
                'incomplete_details':{'reason':'max_output_tokens'},'output':[{'type':'message',
                    'content':[{'type':'output_text','text':'partial'}]}]}}]
        if cap!=768:
            # A slow workflow dependency must never enter ordinary chat.
            time.sleep(1.6)
        return text('受信しました。')
    with model_boundary(provider) as (endpoint,bodies):
        model=CodexResponsesModelClient(api_key='fixture',account_id='fixture',model='fixture',_ENDPOINT=endpoint)
        normal=AgentOrchestrator(model_client=model)
        registry=ToolRegistry()
        fast=web_app._orchestrator_with_interactive_fast_profile(normal,enabled=True,tool_registry=registry)
        assert fast.model_client.max_tokens==768
        runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
        elapsed=[]
        prompts=['こんにちは','Hola','مرحبا','Bonjour','नमस्ते','안녕하세요'] if not truncated else ['続けて']
        with TestClient(web_app.create_app(runtime,fast,registry)) as client:
            for i,prompt in enumerate(prompts):
                started=time.monotonic()
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:budget-{i}'})
                elapsed.append(time.monotonic()-started)
                assert response.status_code==200,response.text
                assert response.json()['text']=='受信しました。'
        assert calls==([768,None] if truncated else [768]*6)
        events=runtime.store.list_conversation_events()
        turns=[e for e in events if e.get('event_type')=='conversation.chat_turn']
        assert len(turns)==len(prompts)
        assert all(e['assistant_reply']=='受信しました。' and not e.get('tool_results') for e in turns)
        if not truncated:
            assert sum(elapsed)/len(elapsed)<1.0 and sorted(elapsed)[-1]<1.5,elapsed
        else:
            timings=[e for e in events if e.get('event_type')=='conversation.model_timing']
            assert any(e.get('fast_profile_truncation_recovered') is True for e in timings)
        model.close()
