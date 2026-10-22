from __future__ import annotations
import asyncio
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E only')


def wire_client(monkeypatch, decision):
    from nullion import model_clients
    requests=[]
    class Response:
        status_code=200
        reason_phrase='OK'
        def __enter__(self): return self
        def __exit__(self,*a): return False
        def raise_for_status(self): pass
        def iter_lines(self):
            response=decision(len(requests),requests[-1])
            yield 'data:'+json.dumps(response)
    class HTTP:
        def __init__(self,**kwargs): pass
        def __enter__(self): return self
        def __exit__(self,*a): return False
        def stream(self,method,url,**kwargs):
            requests.append(kwargs['json']);return Response()
    monkeypatch.setattr(model_clients,'_httpx',SimpleNamespace(Client=HTTP,TimeoutException=TimeoutError))
    return model_clients.CodexResponsesModelClient(api_key='synthetic',account_id='synthetic',model='test'),requests


def complete(text):
    return {'type':'response.completed','response':{'status':'completed','output':[
        {'type':'message','content':[{'type':'output_text','text':text}]}]}}


def test_real_web_chat_recovers_failed_stream_and_keeps_plain_turns_fast(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    for name in ('NULLION_TASK_DECOMPOSITION_ENABLED','NULLION_MULTI_AGENT_ENABLED','NULLION_SKILL_LEARNING_ENABLED','NULLION_MEMORY_ENABLED'):
        monkeypatch.setenv(name,'0')
    monkeypatch.setattr(web_app,'_schedule_web_builder_reflection',lambda *a,**k:None)
    monkeypatch.setattr(web_app,'_schedule_web_no_tool_memory_capture',lambda *a,**k:None)
    def boundary(count,body):
        if count==1:
            return {'type':'response.failed','response':{'status':'failed','error':{'code':'server_error'}}}
        return complete('受信しました。')
    model,requests=wire_client(monkeypatch,boundary)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=ToolRegistry())
    elapsed=[]
    with TestClient(app) as client:
        for i,prompt in enumerate(('こんにちは','Hola','مرحبا','Bonjour','नमस्ते','안녕하세요')):
            started=time.monotonic()
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:terminal-{i}'})
            elapsed.append(time.monotonic()-started)
            assert response.status_code==200,response.text
            assert response.json()['text']=='受信しました。'
    assert len(requests)==7
    runtime.checkpoint(force=True)
    saved=load_runtime_store(tmp_path/'runtime.db')
    turns=[r for r in saved.list_conversation_events() if r.get('event_type')=='conversation.chat_turn']
    assert len(turns)==6
    assert all(r['assistant_reply']=='受信しました。' and r.get('tool_results',[])==[] for r in turns)
    assert sum(elapsed)/len(elapsed)<1.5 and sorted(elapsed)[-1]<3,elapsed


@pytest.mark.parametrize('failed_after_production',[False,True])
@pytest.mark.parametrize('instructions',['AAAとBBB。上昇は緑、下落は赤。','AAA y BBB. Verde para alzas, rojo para caídas.'])
def test_real_cron_skips_review_and_telegram_uploads_produced_report_bytes(tmp_path,monkeypatch,instructions,failed_after_production):
    from nullion import crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import (run_single_agent_cron_turn,run_cron_delivery_workflow,CronRunDeliveryCallbacks,
        cron_structured_result_block_reason,record_cron_delivery_chat_turn)
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult,_build_file_write_handler
    from nullion.telegram_app import _deliver_reply
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    db=tmp_path/'runtime.db';monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',db)
    runtime=bootstrap_persistent_runtime(db)
    artifact=tmp_path/'report.html'
    html='<!doctype html><html><body><article style="background:green">AAA 20 +2%</article><article style="background:red">BBB 10 -2%</article></body></html>'
    job=crons.add_cron('Report','0 8 * * 1-5',instructions,artifact_delivery_options={
        'requires_attachment_delivery':True,'required_artifact_extensions':['.html'],
        'required_artifact_content_tokens':['AAA','BBB'],'deliver_required_extensions_only':True},workspace_id='workspace_admin')
    def boundary(count,body):
        content=body['input'][0]['content']
        payload=json.loads(content if isinstance(content,str) else content[0]['text'])
        assert payload['existing_html']==html
        assert payload['instructions']==instructions
        assert len(payload['verified_source_records'])==2
        if count==1:
            return {'type':'response.failed','response':{'status':'failed','error':{'code':'server_error'}}}
        return complete(json.dumps({'requirements_satisfied':True,'html':'','issues':[],'edits':[]}))
    adapter,requests=wire_client(monkeypatch,boundary)
    class AgentProvider:
        calls=0
        def create(self,**kwargs):
            if not kwargs.get('tools'): return adapter.create(**kwargs)
            self.calls+=1
            if self.calls==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'q','name':'market_quote','input':{'symbols':['AAA','BBB']}},
                    {'type':'tool_use','id':'optional','name':'file_search','input':{'query':'例','path':str(tmp_path)}}]}
            if self.calls==2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'w','name':'file_write','input':{'path':str(artifact),'content':html}}]}
            if failed_after_production:
                from nullion.model_clients import ModelClientResponseError
                raise ModelClientResponseError('server_error')
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'Report attached.'}]}
    model=AgentProvider();registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));lookups=[]
    def quotes(inv):
        lookups.append(inv.invocation_id)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'quotes':[
            {'symbol':'AAA','price':20,'change_percent':2},{'symbol':'BBB','price':10,'change_percent':-2}]})
    registry.register(ToolSpec(name='market_quote',description='Quote source',risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=2),quotes)
    registry.register(ToolSpec(name='file_search',description='Discover earlier report files',risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=2),
        lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'failed',{},error='Source unavailable'))
    registry.register(ToolSpec(name='file_write',description='Create report',risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.WRITE,requires_approval=False,timeout_seconds=2),
        _build_file_write_handler(workspace_root=tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False))
    uploads=[]
    class Message:
        chat=SimpleNamespace(id=123);message_id=1
        async def reply_text(self,*a,**k): pass
        async def reply_document(self,document,**kwargs):
            uploads.append(document.read())
    def run(job,conv,**kwargs):
        return run_single_agent_cron_turn(job,conv,label='Scheduled task',orchestrator=AgentOrchestrator(model_client=model),
            runtime=runtime,tool_registry=registry,settings=SimpleNamespace(enabled_skill_packs=()),model_client=model)
    def send(job,channel,target,text):
        asyncio.run(_deliver_reply(Message(),text,principal_id='telegram:123'));return True
    callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:'telegram',delivery_target=lambda j,c:'123',
        run_agent_turn=run,record_event=lambda *a,**k:None,
        block_reason=lambda result,text,artifacts:cron_structured_result_block_reason(result,text=text,artifacts=artifacts),
        save_web_delivery=lambda *a:False,send_platform_delivery=send,
        record_chat_turn=lambda job,conv,channel,target,text:record_cron_delivery_chat_turn(runtime.store,job,
            conversation_id=conv,delivery_channel=channel,delivery_target=target,delivered_text=text))
    started=time.monotonic()
    result=run_cron_delivery_workflow(job,label='Scheduled task',callbacks=callbacks)
    assert time.monotonic()-started<3
    assert model.calls<=4
    if result.get('cron_report_failed'):
        (tmp_path/'failed-run.json').write_text(json.dumps(result,default=str,indent=2))
    assert not result.get('cron_report_failed'),result.get('reason')
    assert result['artifact_delivery_satisfied'] is True
    assert len(lookups)==1 and len(requests)==0
    assert len(uploads)==1
    from playwright.sync_api import sync_playwright
    with sync_playwright() as api:
        browser=api.chromium.launch()
        page=browser.new_page(viewport={'width':390,'height':844})
        page.set_content(uploads[0].decode())
        assert page.locator('article').all_text_contents()==['AAA 20 +2%','BBB 10 -2%']
        assert page.locator('article').evaluate_all('cards => cards.map(c => getComputedStyle(c).backgroundColor)')==['rgb(0, 128, 0)','rgb(255, 0, 0)']
        assert page.locator('script').count()==0
        browser.close()
    runtime.checkpoint(force=True)
    turns=[r for r in load_runtime_store(db).list_conversation_events() if r.get('cron_id')==job.id]
    assert len(turns)==1 and turns[0]['delivery_channel']=='telegram'
    assert 'MEDIA:'+str(artifact) in turns[0]['assistant_reply']
