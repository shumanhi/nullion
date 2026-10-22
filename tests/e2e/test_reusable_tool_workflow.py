"""Real chat route, SQLite restart, saved code execution and visible replies."""
from datetime import UTC, datetime
import json
import os
import time

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E only')


def setup(tmp_path,monkeypatch,grant=True):
    from nullion import chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_core_tool_registry
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    for key,value in {'NULLION_HOME':tmp_path/'home','NULLION_ENV_FILE':tmp_path/'home'/'.env',
        'NULLION_WORKSPACE_STORAGE_ROOT':tmp_path/'storage','NULLION_TASK_PLANNER_FEED_MODE':'off',
        'NULLION_ACTIVITY_TRACE_ENABLED':'0','NULLION_MEMORY_ENABLED':'0','NULLION_CHAT_STARTUP_WARMUP':'0',
        'NULLION_TERMINAL_ENABLED':'1'}.items():monkeypatch.setenv(key,str(value))
    monkeypatch.setattr(chat_store,'_store',None)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=create_core_tool_registry(workspace_root=tmp_path)
    runtime.active_tool_registry=registry
    if grant:
        runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)))
    return runtime,registry


def last_output(kwargs,name):
    for message in reversed(kwargs['messages']):
        for item in reversed(message.get('content',[]) if isinstance(message.get('content'),list) else []):
            if item.get('type')=='tool_result' and item.get('tool_use_id')==name:
                content=item['content']
                if isinstance(content,list):content='\n'.join(b.get('text','') for b in content if b.get('type')=='text')
                payload=json.loads(content)
                return payload.get('output',payload)
    raise AssertionError(f'Missing {name} output')


@pytest.mark.parametrize('prompt,surface',[
    ('Create and reuse a helper with JSON schemas for these numbers. No attachment.','api'),('JSON スキーマで数値処理を再利用できるようにして','api'),
    ('Crea una herramienta reutilizable con esquemas JSON para estos números','api'),('操作 37','browser')])
def test_chat_create_run_restart_and_reuse_with_new_arguments(tmp_path,monkeypatch,prompt,surface):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_core_tool_registry,ToolInvocation
    from test_reusable_tools import definition
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    runtime,registry=setup(tmp_path,monkeypatch)
    class Create(ScriptedProvider):
        steps=0
        def create(self,**kw):
            if 'select_attachment_format' in {t['name'] for t in kw.get('tools',[])}:
                return tool('select_attachment_format',{'extension':'.json','output_kind':'internal',
                    'confidence':1,'embedded_media_extensions':[]})
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.steps+=1
            if self.steps==1:return tool('request_tool_scope',{'capabilities':['local_tools'],
                'tool_names':['local_tool_search','local_tool_save','local_tool_run'],'required_tool_names':['local_tool_run']})
            assert {'local_tool_search','local_tool_save','local_tool_run'}<={d['name'] for d in kw['tools']}, (self.steps, sorted(d['name'] for d in kw['tools']),str(kw['messages'][-2:]))
            if self.steps==2:return tool('local_tool_search',{'query':'sum_values'})
            if self.steps==3:
                assert last_output(kw,'local_tool_search')['tools']==[]
                return tool('local_tool_save',definition())
            if self.steps==4:
                saved=last_output(kw,'local_tool_save');self.revision=saved['revision']
                assert saved['persisted'] and not saved['verified']
                return tool('local_tool_run',{'name':'sum_values','revision':self.revision,'arguments':{'values':[2,3]}})
            assert self.steps==5
            result=last_output(kw,'local_tool_run')
            assert result['result']=={'total':5} and result['verified'],result
            return response('Total: 5. Saved and verified the reusable helper.')
    model=Create();runtime.model_client=model
    def send(app,text,expected,conversation_id):
        if surface=='api':
            with TestClient(app) as client:
                result=client.post('/api/chat',json={'text':text,'conversation_id':conversation_id})
                assert result.status_code==200,result.text
                assert expected in result.json()['text'],result.json()
                assert result.json().get('artifacts',[])==[]
        else:
            from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
            from playwright.sync_api import expect
            with _UvicornThread(app,_free_port()) as server:
                playwright,browser,page,errors=_open_page(server.base_url)
                try:
                    page.locator('#user-input').fill(text)
                    page.get_by_role('button',name='Send',exact=True).click()
                    expect(page.locator('#messages')).to_contain_text(expected,timeout=20000)
                    assert 'def run(arguments)' not in page.locator('#messages').inner_text()
                    assert not errors,errors
                finally:browser.close();playwright.stop()
    send(create_app(runtime,AgentOrchestrator(model_client=model),registry),prompt,'Total: 5.','web:tool-create')
    runtime.checkpoint(force=True)
    # New runtime and registry, independent conversation/surface, no prior model messages.
    fresh=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    new_registry=create_core_tool_registry(workspace_root=tmp_path);fresh.active_tool_registry=new_registry
    class Reuse(ScriptedProvider):
        steps=0
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.steps+=1
            if self.steps==1:return tool('request_tool_scope',{'capabilities':['local_tools']})
            if self.steps==2:return tool('local_tool_search',{'query':'sum_values'})
            if self.steps==3:
                found=last_output(kw,'local_tool_search')['tools'][0]
                assert found['revision']==model.revision and found['verified']
                return tool('local_tool_read',{'name':found['name'],'revision':found['revision']})
            if self.steps==4:
                exact=last_output(kw,'local_tool_read')
                return tool('local_tool_run',{'name':exact['name'],'revision':exact['revision'],'arguments':{'values':[11,29]}})
            assert self.steps==5
            assert last_output(kw,'local_tool_run')['result']=={'total':40}
            return response('Total: 40. Reused the saved helper.')
    reuse=Reuse();fresh.model_client=reuse
    send(create_app(fresh,AgentOrchestrator(model_client=reuse),new_registry),'同じ処理を11と29で','Total: 40.','web:tool-reuse')
    saved=new_registry.invoke(ToolInvocation('inspect','local_tool_read','telegram:501',{'name':'sum_values'}))
    assert saved.output['successful_runs']==2
    events=fresh.store.list_conversation_events('web:tool-reuse') if surface=='api' else fresh.store.list_conversation_events()
    actual=next(e for e in reversed(events) if e.get('event_type')=='conversation.chat_turn')
    names=[r['tool_name'] for r in actual['tool_results']]
    assert names==['request_tool_scope','local_tool_search','local_tool_read','local_tool_run'],names
    assert any(e.event_type=='tool.invocation.completed' and e.payload.get('tool_name')=='terminal_exec'
               for e in fresh.store.list_events())
    if prompt.startswith('Create'):
        # A genuinely fresh interpreter also discovers and runs the persisted tool.
        import subprocess,sys
        program='''import os,sys,json
sys.path.insert(0,os.environ['NULLION_APP_REPO']+'/src')
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import create_core_tool_registry,ToolExecutor,ToolInvocation
r=bootstrap_persistent_runtime(sys.argv[1]);g=create_core_tool_registry(workspace_root=sys.argv[2])
found=g.invoke(ToolInvocation('find','local_tool_search','telegram:501',{'query':'sum_values'}))
t=found.output['tools'][0]
out=ToolExecutor(store=r.store,registry=g).invoke(ToolInvocation('reuse','local_tool_run','telegram:501',{'name':t['name'],'revision':t['revision'],'arguments':{'values':[3,4]}}))
print(json.dumps({'status':out.status,'output':out.output}))
'''
        fresh.checkpoint(force=True)
        process=subprocess.run([sys.executable,'-c',program,str(tmp_path/'runtime.db'),str(tmp_path)],capture_output=True,text=True,timeout=20)
        assert process.returncode==0,process.stderr
        output=json.loads(process.stdout.splitlines()[-1])
        assert output['status']=='completed' and output['output']['result']=={'total':7},output


def test_real_terminal_schema_failure_exception_and_approval_resume(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.tools import ToolExecutor,ToolInvocation
    from nullion.web_app import create_app
    from test_reusable_tools import definition
    runtime,registry=setup(tmp_path,monkeypatch,grant=False)
    saved=registry.invoke(ToolInvocation('save','local_tool_save','web:review',definition()))
    args={'name':'sum_values','revision':saved.output['revision'],'arguments':{'values':[4,8]}}
    blocked=ToolExecutor(store=runtime.store,registry=registry).invoke(ToolInvocation('run','local_tool_run','web:review',args))
    assert blocked.status=='denied' and blocked.output['requires_approval'],blocked
    aid=blocked.output['approval_id'];approval=runtime.store.get_approval_request(aid)
    assert approval.resource=='terminal_exec'
    assert approval.context['tool_arguments']['saved_tool']['revision']==saved.output['revision']
    # Approving the exact nested command runs actual Python, never a mocked tool body.
    runtime.checkpoint(force=True)
    with TestClient(create_app(runtime,None,registry)) as client:
        accepted=client.post('/api/approve/'+aid,json={'mode':'once'})
        assert accepted.status_code==200,accepted.text
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)))
    for source,reason in [('def run(arguments):\n    return {"total":"wrong"}', 'output_schema_mismatch'),
                          ('def run(arguments):\n    raise RuntimeError("synthetic tool error")','execution_failed')]:
        changed=definition();changed.update(source=source,expected_revision=args['revision'])
        save=registry.invoke(ToolInvocation('update','local_tool_save','web:review',changed))
        args['revision']=save.output['revision']
        result=ToolExecutor(store=runtime.store,registry=registry).invoke(ToolInvocation('run','local_tool_run','web:review',args))
        assert result.status=='failed' and result.output['reason']==reason,result
        exact=registry.invoke(ToolInvocation('read','local_tool_read','web:review',{'name':'sum_values'}))
        assert exact.output['successful_runs']==0 and exact.output['failed_runs']==1


def test_simple_chat_never_scans_catalog_or_adds_a_model_round(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import reusable_tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from test_memory_and_document_recall_contract import ScriptedProvider,response
    runtime,registry=setup(tmp_path,monkeypatch)
    def slow_catalog(*a,**kw):
        time.sleep(2)
        pytest.fail('Reusable-tool catalog re-entered the simple chat path')
    monkeypatch.setattr(reusable_tools,'_connect',slow_catalog)
    class Plain(ScriptedProvider):
        calls=0
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.calls+=1
            assert 'local_tool_save' not in {d['name'] for d in kw.get('tools',[])}
            return response('Acknowledged.')
    model=Plain();runtime.model_client=model
    elapsed=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for i,prompt in enumerate(['Hello','こんにちは','Hola','Thanks','了解']):
            start=time.perf_counter()
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:plain-{i}'})
            elapsed.append(time.perf_counter()-start)
            assert result.status_code==200 and result.json()['text']=='Acknowledged.',result.text
    assert model.calls==5
    assert sum(elapsed)/len(elapsed)<1.0 and max(elapsed)<1.8,elapsed


@pytest.mark.parametrize('mode',['once','always'])
def test_chat_approval_resumes_saved_revision_without_granting_blanket_shell(tmp_path,monkeypatch,mode):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.tools import ToolInvocation,ToolExecutor
    from test_reusable_tools import definition
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    runtime,registry=setup(tmp_path,monkeypatch,grant=False)
    saved=registry.invoke(ToolInvocation('save','local_tool_save','web:review',definition()))
    args={'name':'sum_values','revision':saved.output['revision'],'arguments':{'values':[6,7]}}
    class Model(ScriptedProvider):
        steps=0
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.steps+=1
            if self.steps==1:return tool('request_tool_scope',{'capabilities':['local_tools']})
            if self.steps==2:return tool('local_tool_run',args)
            assert last_output(kw,'local_tool_run')['result']=={'total':13}
            return response('Total: 13. The saved tool completed.')
    model=Model();runtime.model_client=model
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        result=client.post('/api/chat',json={'text':'操作 51','conversation_id':'web:review'})
        payload=result.json()
        assert payload.get('type')=='approval_required',payload
        aid=payload['approval_id']
        approval=runtime.store.get_approval_request(aid)
        assert approval.context['tool_name']=='terminal_exec' and 'command' in approval.context['tool_arguments']
        assert approval.context['delegated_resume']['tool_name']=='local_tool_run'
        result=client.post('/api/approve/'+aid,json={'mode':mode})
        assert result.status_code==200,result.text
        history=client.get('/api/chat/history/web:review').json()
        assert 'Total: 13.' in str(history),(result.text,history)
    # Even an Always choice is for this reviewed command/revision, not any shell command.
    executor=ToolExecutor(store=runtime.store,registry=registry)
    other=executor.invoke(ToolInvocation('other','terminal_exec','web:review',{'command':'printf unrelated'}))
    assert other.status=='denied' and other.output.get('requires_approval'),other
    repeated=executor.invoke(ToolInvocation('repeat','local_tool_run','web:review',args))
    assert repeated.status==('completed' if mode=='always' else 'denied'),repeated
    changed=definition();changed.update(source='def run(arguments):\n    return {"total": 99}',expected_revision=args['revision'])
    update=registry.invoke(ToolInvocation('update','local_tool_save','web:review',changed))
    newer=executor.invoke(ToolInvocation('new-code','local_tool_run','web:review',{**args,'revision':update.output['revision']}))
    assert newer.status=='denied' and newer.output.get('requires_approval'),newer


@pytest.mark.skipif(os.name!='nt',reason='Requires a native Windows PowerShell runner')
def test_windows_saved_tool_preserves_native_argument_bytes(tmp_path,monkeypatch):
    from nullion.tools import ToolInvocation,ToolExecutor
    runtime,registry=setup(tmp_path,monkeypatch)
    source='def run(arguments):\n    return {"value": arguments["value"]}\n'
    schema={'type':'object','properties':{'value':{'type':'string'}},'required':['value']}
    saved=registry.invoke(ToolInvocation('save','local_tool_save','web:review',{
        'name':'quoted_echo','description':'Return string data unchanged','source':source,
        'input_schema':schema,'output_schema':schema}))
    value='青葉 \' " $() ` & ; $(Write-Output injected)'
    result=ToolExecutor(store=runtime.store,registry=registry).invoke(ToolInvocation('run','local_tool_run','web:review',{
        'name':'quoted_echo','revision':saved.output['revision'],'arguments':{'value':value}}))
    assert result.status=='completed' and result.output['result']=={'value':value},result
