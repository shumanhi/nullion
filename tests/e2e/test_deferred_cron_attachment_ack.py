from __future__ import annotations
import json
import os
import time
import threading
from pathlib import Path
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E only')

@pytest.mark.parametrize('prompt',['既存のタスクを実行して、.htmlをここに届けてください。','Ejecuta la tarea existente y entrega el informe .html aquí.'])
def test_real_http_cron_dispatch_ack_is_not_a_missing_attachment_and_final_file_arrives(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import crons,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult,_build_file_write_handler
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    for name in ('NULLION_MEMORY_ENABLED','NULLION_TASK_DECOMPOSITION_ENABLED','NULLION_MULTI_AGENT_ENABLED','NULLION_SKILL_LEARNING_ENABLED'):
        monkeypatch.setenv(name,'0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setattr(web_app,'_schedule_web_builder_reflection',lambda *a,**k:None)
    monkeypatch.setattr(web_app,'_schedule_web_no_tool_memory_capture',lambda *a,**k:None)
    db=tmp_path/'runtime.db';monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',db)
    runtime=bootstrap_persistent_runtime(db)
    from datetime import UTC,datetime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-consent',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:run_cron',granted_by='fixture-user',granted_at=datetime.now(UTC)))
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture-user',datetime.now(UTC)))
    html='<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><h1>確認</h1><p>AAA 7</p><p>BBB 9</p></body></html>'
    from nullion.artifacts import artifact_root_for_principal
    report=Path(artifact_root_for_principal('web:deferred-attachment'))/'report.html'
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    release_source=threading.Event()
    def quote_source(inv):
        assert release_source.wait(8) # external service; the initial acknowledgement must not wait for the file
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'quotes':[
            {'symbol':'AAA','price':7},{'symbol':'BBB','price':9}]})
    registry.register(ToolSpec('market_quote','Quote service',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,15),quote_source)
    registry.register(ToolSpec('file_write','Create report',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),
        _build_file_write_handler(workspace_root=tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False))
    job=crons.add_cron('確認','0 0 1 1 *','AAAとBBBの価格をHTMLで報告。',artifact_delivery_options={
        'requires_attachment_delivery':True,'required_artifact_extensions':['.html'],
        'required_artifact_content_tokens':['AAA','BBB'],'deliver_required_extensions_only':True})
    class Provider:
        scope=False;foreground=False;background=0;calls=[]
        def create(self,**kwargs):
            names={t['name'] for t in kwargs.get('tools',[])}
            self.calls.append({'tools':sorted(names),'system':str(kwargs.get('system',''))[:100],'tokens':kwargs.get('max_tokens')})
            if names=={'select_attachment_format'}:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':'.html','confidence':1,'embedded_media_extensions':[]}}]}
            if not names and kwargs.get('max_tokens')==96:
                return {'content':[{'type':'text','text':json.dumps({'requested_outcome':'unspecified','scheduler_action':'run',
                    'scheduler_selection_policy':'user_selected','requested_tool_names':['run_cron'],'required_tool_names':['run_cron'],
                    'requested_artifact_extensions':['.html'],'confidence':1})}]}
            if not names:
                payload=json.loads(kwargs['messages'][0]['content'][0]['text'])
                assert payload['existing_html'] is not None
                return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':True,'html':'','issues':[],'edits':[]})}]}
            if not self.scope and 'run_cron' not in names:
                self.scope=True
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                    'input':{'capabilities':['scheduler_run'],'scheduler_action':'run','tool_names':['run_cron'],'required_tool_names':['run_cron'],'scheduler_selection_policy':'user_selected','artifact_extensions':['.html']}}]}
            if 'run_cron' in names:
                assert not self.foreground,'The dispatch should return its real receipt immediately'
                self.foreground=True
                self.scope=True
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'dispatch','name':'run_cron','input':{'id':job.id}}]}
            self.background+=1
            if self.background==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'quotes','name':'market_quote','input':{'symbols':['AAA','BBB']}}]}
            if self.background==2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'write','name':'file_write','input':{'path':str(report),'content':html}}]}
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'Report attached.'}]}
    provider=Provider()
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=registry)
    conversation='web:deferred-attachment'
    request_text=prompt+' '+job.id
    with TestClient(app) as client:
        started=time.monotonic()
        response=client.post('/api/chat',json={'text':request_text,'conversation_id':conversation})
        assert time.monotonic()-started<3
        assert response.status_code==200,response.text
        result=response.json()
        if not result.get('mini_agent_dispatch'):
            print('PROVIDER_CALLS',provider.calls)
        assert result['mini_agent_dispatch'] is True,result
        assert result['task_group_id']
        initial=[r for r in runtime.store.list_conversation_events(conversation)
                 if r.get('event_type')=='conversation.chat_turn' and r.get('user_message')==request_text][-1]
        assert result['text']==initial['assistant_reply'],result
        assert result['artifacts']==[]
        assert any(r.get('tool_name')=='run_cron' and r['output']['delivery_status']=='deferred' for r in initial['tool_results'])
        release_source.set()
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            history=client.get('/api/chat/history/'+conversation).json()
            rows=runtime.store.list_conversation_events(conversation)
            final=[r for r in rows if r.get('event_type')=='conversation.chat_turn' and r.get('source')=='cron']
            if final and report.name in json.dumps(history):break
            time.sleep(0.05)
        assert final and 'MEDIA:'+str(report) in final[-1]['assistant_reply'],json.dumps(history)
        from nullion.artifacts import artifact_descriptor_for_path
        descriptor=artifact_descriptor_for_path(report,artifact_root=report.parent)
        downloaded=client.get('/api/artifacts/'+descriptor.artifact_id)
        assert downloaded.status_code==200,downloaded.text
        assert downloaded.content==report.read_bytes()
        assert b'AAA 7' in downloaded.content and b'BBB 9' in downloaded.content
    assert provider.background==3  # source, production, explicit final response
