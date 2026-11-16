"""Host modules compose through the real chat API, native files and SQLite."""
import json
import os
import time
from html import escape
from urllib.parse import quote_plus

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('session_mode',['single','restored'])
def test_host_helpers_create_verified_html_without_model_recovery(tmp_path,monkeypatch,record_property,session_mode):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from nullion.web_app import create_app
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    registry.register(ToolSpec('file_write','Save a local file',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5,
        input_schema={'type':'object','properties':{'path':{'type':'string'},'content':{'type':'string'}},'required':['path','content']}),
        _build_file_write_handler(tmp_path,allowed_roots=(tmp_path,)))
    root=workspace_storage_roots_for_principal('workspace_admin').artifacts
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if any(t['name']=='select_attachment_format' for t in kwargs.get('tools',[])):
                return call('select_attachment_format',{'extension':'.html','confidence':1})
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1;time.sleep(.05)
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_write'],'required_tool_names':['file_write'],'artifact_extensions':['.html']})
            prefix="from html import escape\nfrom textwrap import dedent\n"+f"label={self.source!r}\n"
            def program(code,names):
                selected=call('code_mode',{'tool_names':names,'code':code})
                selected['content'][0]['id']=f'program-{self.phase}'
                return selected
            if self.phase==2 and session_mode=='restored':
                return program(prefix+'encoded=quote_plus(label)\nencoded',[])
            if self.phase==(2 if session_mode=='single' else 3):
                code=((prefix if session_mode=='single' else '')+
                    "link='?q='+quote_plus(label)\n"
                    "markup=dedent('<html><body><h1>'+escape(label)+'</h1><a href=\"'+link+'\">Source</a></body></html>')\n"
                    f"call_tool('file_write',{{'path':{str(self.path)!r},'content':markup}})['output']")
                return program(code,['file_write'])
            assert self.phase==(3 if session_mode=='single' else 4),'Host helpers caused an extra model round trip'
            return text('Created the verified HTML page.')
    model=Model();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,(prompt,source) in enumerate([
            ('ページを .html で保存','東京 & café <37>'),
            ('Guarda una página .html','¡Hola! & <37>'),
            ('احفظ الصفحة .html','مرحبا & <37>')]):
            model.phase=0;model.source=source;model.path=root/f'host-modules-{index}.html'
            conversation=f'web:host-modules-{index}';started=time.monotonic()
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation})
            samples.append(time.monotonic()-started)
            if not response.json()['artifacts']:
                print('native failures',json.dumps([r for e in runtime.store.list_conversation_events(conversation)
                    if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])
                    if r['status']!='completed' and r['tool_name']!='code_mode'],ensure_ascii=False))
            assert response.status_code==200 and response.json()['artifacts'],(response.text,
                [(r['tool_name'],r['status'],r.get('output')) for e in runtime.store.list_conversation_events(conversation)
                    if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])])
            artifacts=response.json()['artifacts'];assert len(artifacts)==1
            data=client.get(artifacts[0]['url']).content
            assert data==model.path.read_bytes()
            assert escape(source).encode() in data and quote_plus(source).encode() in data
            turn=[e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type')=='conversation.chat_turn'][-1]
            assert turn['response_fulfilled'] is True
            programs=[r for r in turn['tool_results'] if r['tool_name']=='code_mode']
            assert len(programs)==(1 if session_mode=='single' else 2),programs
            assert all(row['status']=='completed' for row in programs),programs
            assert programs[-1]['output']['host_calls']=={'urllib.parse.quote_plus':1,'html.escape':1,'textwrap.dedent':1}
            assert len([r for r in turn['tool_results'] if r['tool_name']=='file_write'])==1
    assert sum(samples)/len(samples)<2 and max(samples)<4,samples
    record_property('mean_host_workflow_seconds',sum(samples)/len(samples))


@pytest.mark.parametrize('prompt',['保存してください','Guarda el resultado','احفظ النتيجة'])
@pytest.mark.parametrize('failure',['import','python'])
def test_repeated_program_errors_end_the_real_run_without_replaying_write(tmp_path,monkeypatch,prompt,failure):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    registry=ToolRegistry();writes=[]
    def save(inv):
        writes.append(inv.invocation_id)
        (tmp_path/'prepared.txt').write_text('verified-37')
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'saved':True})
    registry.register(ToolSpec('prepare_record','Prepare a record',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5,
        input_schema={'type':'object'}),save)
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            self.phase+=1
            limit=2 if failure=='import' else 3
            assert self.phase<=limit,'Program failures consumed further model iterations'
            tail=f'import missing_package_{self.phase}' if failure=='import' else f'missing_variable_{self.phase}'
            code=("saved=call_tool('prepare_record', {})\n" if self.phase==1 else '')+tail
            selected=call('code_mode',{'tool_names':['prepare_record'],'code':code})
            selected['content'][0]['id']=f'program-{self.phase}';return selected
    model=Model()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='telegram:host-budget',principal_id='telegram_chat',
        user_message=prompt,conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    assert model.phase==(2 if failure=='import' else 3) and len(writes)==1
    assert not result.suspended_for_approval and not result.reached_iteration_limit
    assert ('unsupported Python imports' if failure=='import' else 'Python errors') in result.final_text
    assert not result.response_fulfilled
    assert (tmp_path/'prepared.txt').read_text()=='verified-37'
    programs=[r for r in result.tool_results if r.tool_name=='code_mode']
    assert programs[-1].output['reason']==('code_mode_import_recovery_exhausted' if failure=='import' else 'code_mode_python_recovery_exhausted')


@pytest.mark.parametrize('fault',['memory','watchdog'])
def test_unusable_worker_stops_without_replaying_confirmed_native_write(tmp_path,monkeypatch,fault):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    if fault=='watchdog':
        # Change only the external worker deadline; use the real subprocess,
        # executor, durable journal and model/receipt boundary throughout.
        import pydantic_monty
        pool=pydantic_monty.Monty
        def short_deadline(*args,**kwargs):
            kwargs['request_timeout']=.5
            return pool(*args,**kwargs)
        monkeypatch.setattr(pydantic_monty,'Monty',short_deadline)
    writes=[];registry=ToolRegistry()
    def save(invocation):
        writes.append(invocation.invocation_id)
        with (tmp_path/'confirmed.txt').open('a') as stream:stream.write('37')
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'saved':True})
    registry.register(ToolSpec('prepare_record','Prepare a record',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5,
        input_schema={'type':'object'}),save)
    class Model:
        calls=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            self.calls+=1
            assert self.calls==1,'An unusable worker triggered another model/program attempt'
            tail="huge='x'*100_000_000" if fault=='memory' else 'while True: pass'
            return call('code_mode',{'tool_names':['prepare_record'],'code':"saved=call_tool('prepare_record', {})\n"+tail})
    model=Model()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='web:worker-failure',principal_id='workspace_admin',
        user_message='記録を準備',conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    assert len(writes)==1 and (tmp_path/'confirmed.txt').read_text()=='37'
    assert not result.response_fulfilled and not result.suspended_for_approval
    summary=[r for r in result.tool_results if r.tool_name=='code_mode'][-1]
    assert summary.output['reason']==('code_mode_resource_limit' if fault=='memory' else 'code_mode_worker_lost'),summary.output
    assert summary.output['execution_error']['session_usable'] is False
    if fault=='watchdog':assert summary.output['execution_error']['timed_out'] is True


@pytest.mark.parametrize('prompt',['処理してください','Procesa los datos','عالج البيانات'])
def test_repeated_invalid_program_selections_stop_with_actionable_constraints(tmp_path,monkeypatch,prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    registry=ToolRegistry();effects=[]
    registry.register(ToolSpec('read_record','Read data',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5),
        lambda inv:effects.append(inv) or ToolResult(inv.invocation_id,inv.tool_name,'completed',{}))
    class Model:
        calls=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            self.calls+=1
            assert self.calls<=3,'Invalid selections consumed the full iteration budget'
            if self.calls>1:assert 'outside_current_scope' in json.dumps(kwargs['messages'])
            response=call('code_mode',{'tool_names':['unscoped_tool'],'code':'1'})
            response['content'][0]['id']=f'invalid-{self.calls}';return response
    model=Model()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='web:invalid-selection',principal_id='workspace_admin',
        user_message=prompt,conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    assert model.calls==3 and not effects and not result.reached_iteration_limit
    summary=[r for r in result.tool_results if r.tool_name=='code_mode'][-1]
    assert summary.output['reason']=='code_mode_validation_exhausted'
    assert set(summary.output['validation_errors'][0]['allowed_tool_names'])=={'inspect_retained_evidence','read_record'}
    assert summary.output['executed'] is False
