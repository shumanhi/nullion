"""Real FastAPI/SQLite fault paths; no model or external account required."""
import gc
import os
from pathlib import Path
import time

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


def isolated(tmp_path,monkeypatch):
    from nullion import chat_store,crons
    from nullion.runtime import bootstrap_persistent_runtime
    home=tmp_path/'home';home.mkdir()
    for key,value in {'NULLION_HOME':home,'NULLION_ENV_FILE':home/'absent.env','NULLION_KEY_STORAGE':'local',
        'NULLION_MEMORY_ENABLED':'0','NULLION_DOCTOR_ENABLED':'0','NULLION_CHAT_STARTUP_WARMUP':'0'}.items():
        monkeypatch.setenv(key,str(value))
    monkeypatch.setattr(chat_store,'_store',None)
    path=home/'runtime.db';monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',path)
    runtime=bootstrap_persistent_runtime(path);runtime.checkpoint(force=True)
    return runtime,home


@pytest.mark.skipif(not Path('/proc/self/fd').is_dir(),reason='Linux container descriptor audit')
def test_scheduler_status_does_not_accumulate_sqlite_descriptors(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    runtime,home=isolated(tmp_path,monkeypatch)
    from nullion.tools import ToolRegistry
    client=TestClient(create_app(runtime,None,ToolRegistry()))
    def handles():
        count=0
        for path in Path('/proc/self/fd').iterdir():
            try:count+=str(runtime.checkpoint_path) in os.readlink(path)
            except FileNotFoundError:pass
        return count
    gc.collect();before=handles();was_enabled=gc.isenabled();gc.disable();samples=[]
    try:
        for _ in range(20):
            started=time.perf_counter();response=client.get('/api/cron-scheduler');samples.append(time.perf_counter()-started)
            assert response.status_code==200 and response.json()['max_parallel_jobs']>=1
        after=handles()
        assert after<=before+1,{'before':before,'after':after}
        assert sum(samples)/len(samples)<.2 and sorted(samples)[-1]<.75
        record_property('db_handle_counts',f'{before}->{after}')
    finally:
        client.close()
        if was_enabled:gc.enable()
        gc.collect()


def test_analysis_storage_failure_returns_controlled_http_error(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app
    runtime,home=isolated(tmp_path,monkeypatch)
    # This directory forces a real sqlite3 open error at the storage boundary.
    (home/'chat_history.db').mkdir()
    from nullion.tools import ToolRegistry
    with TestClient(web_app.create_app(runtime,None,ToolRegistry())) as client:
        response=client.post('/api/chat/analyze/web:fixture')
        assert response.status_code==500
        assert response.json()=={'ok':False,'error':'Conversation analysis could not be completed.'}


@pytest.mark.parametrize('read_fails',[False,True])
def test_connection_test_http_error_releases_response_before_delivery(tmp_path,monkeypatch,read_fails):
    from io import BytesIO
    import urllib.error
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.tools import ToolRegistry
    runtime,home=isolated(tmp_path,monkeypatch)
    class Body(BytesIO):
        def read(self,*args):
            if read_fails:raise OSError('Synthetic read failure')
            return super().read(*args)
    body=Body(b'Upstream unavailable')
    failure=urllib.error.HTTPError('https://example.org',503,'Unavailable',{},body)
    attempts=[]
    def request(*args,**kwargs):
        attempts.append(args[0].full_url)
        raise failure
    monkeypatch.setattr(web_app.urllib.request,'urlopen',request)
    try:
        with TestClient(web_app.create_app(runtime,None,ToolRegistry())) as client:
            result=client.post('/api/connections/test-provider',json={
                'provider_id':'custom_api_provider','api_base_url':'https://example.org','token_value':'fixture'})
            assert result.status_code==502 and result.json()['ok'] is False
            assert ('Synthetic read failure' if read_fails else 'HTTP 503') in result.json()['error']
        assert len(attempts)==1 and body.closed
    finally:failure.close()


@pytest.mark.parametrize('http_status',[401,429,503])
def test_http_source_error_keeps_its_observed_status_through_chat_delivery(tmp_path,monkeypatch,http_status):
    import json
    import urllib.error
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,register_web_fetch_tool
    from tests.e2e.test_personal_task_persistence_contract import response,tool
    runtime,home=isolated(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED','1')
    from datetime import UTC,datetime
    from nullion.approvals import create_permission_grant
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,PolicyMode,OPERATOR_PERMISSION_PRINCIPAL
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-source',OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-fetch',
        principal_id=OPERATOR_PERMISSION_PRINCIPAL,permission='tool:web_fetch',granted_by='fixture',granted_at=datetime.now(UTC)))
    attempts=[]
    def source(url,timeout):
        attempts.append(url)
        raise urllib.error.HTTPError(url,http_status,'private upstream diagnostic',{},None)
    from nullion import tools as source_tools
    monkeypatch.setattr(source_tools,'_default_web_fetcher',source)
    registry=ToolRegistry();register_web_fetch_tool(registry,web_fetcher=source)
    runtime.active_tool_registry=registry
    class Provider:
        calls=0
        def create(self,**kw):
            from tests.workflow_provider_protocol import workflow_provider_reply
            auxiliary=workflow_provider_reply(kw,completion=False)
            if auxiliary is not None:return auxiliary
            system=str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                return response(json.dumps({'all_material_requirements_enumerated':True,
                    'material_requirements':[{'requirement':'Report the selected source result','requirement_kind':'fact',
                        'evidence_subject_id':'selected','eligible_evidence_tool_names':['web_fetch']}]}))
            if 'semantic completion reviewer' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                row=next(r for r in packet['tool_evidence'] if r['tool_name']=='web_fetch' and r['status']=='failed')
                assert row['output']['reason']=='http_error' and row['output']['status_code']==http_status
                return response(json.dumps({'disposition':'blocked','all_material_requirements_enumerated':True,
                    'unresolved_requirements':['Internal repair instruction'],'user_facing_failure':'Internal repair instruction',
                    'retry_tool_names':[],'supported_findings':[{'kind':'limitation','text':f'The selected source returned HTTP {http_status}.',
                        'evidence_invocation_ids':[row['invocation_id']]}],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':False,'draft_covered':False,
                        'evidence_invocation_ids':[]} for r in packet['material_requirement_contract']]}))
            names={r['name'] for r in kw.get('tools',[])}
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return response('{}')
            self.calls+=1
            if self.calls==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['web_fetch'],'required_tool_names':['web_fetch']})
            if self.calls==2:return tool('web_fetch',{'url':'https://example.org/selected'})
            return response('Rejected source claim.')
    provider=Provider()
    with TestClient(web_app.create_app(runtime,AgentOrchestrator(model_client=provider),registry)) as client:
        result=client.post('/api/chat',json={'conversation_id':'web:source-fault','text':'選択した情報源を確認して結果を報告してください。 https://example.org/selected'})
        assert result.status_code==200,result.text
        text=result.json()['text']
        assert f'HTTP {http_status}' in text and 'network error' not in text
        assert 'private upstream' not in text and 'Internal repair instruction' not in text and 'Rejected source claim' not in text
    assert len(attempts)==1
    events=runtime.store.list_conversation_events('web:source-fault')
    turn=next(e for e in reversed(events) if e.get('event_type')=='conversation.chat_turn')
    assert f'HTTP {http_status}' in turn['assistant_reply']
    assert any(r['tool_name']=='web_fetch' and r['status']=='failed' and r['output']['status_code']==http_status for r in turn['tool_results'])
