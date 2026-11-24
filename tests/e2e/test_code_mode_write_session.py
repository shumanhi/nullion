"""Request sessions cross real native writes, SQLite and approval continuation."""
import json
import os
from pathlib import Path
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('prompt',['確認して保存','Guarda el resultado','Enregistre le résultat'])
@pytest.mark.parametrize('with_inspection',[False,True])
def test_write_approval_resume_preserves_program_and_does_not_repeat_writes(tmp_path,monkeypatch,prompt,with_inspection):
    from nullion import code_mode
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from nullion.approvals import approve
    from nullion.runtime import bootstrap_persistent_runtime
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    from nullion.preferences import save_preferences
    save_preferences({'sentinel_mode':'risk_based'})
    first=tmp_path/'first.txt';last=tmp_path/'last.txt';published=tmp_path/'published.txt'
    calls=[]
    reg=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    reg.register(ToolSpec('file_write','Save local text',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5,
        input_schema={'type':'object','required':['path','content'],'properties':{'path':{'type':'string'},'content':{'type':'string'}}}),
        _build_file_write_handler(tmp_path,allowed_roots=(tmp_path,)))
    def publish(invocation):
        calls.append(invocation)
        with published.open('a') as stream:stream.write(invocation.arguments['content'])
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'published':True})
    reg.register(ToolSpec('publish_record','Publish the prepared record',ToolRiskLevel.HIGH,ToolSideEffectClass.ACCOUNT_WRITE,True,5,
        input_schema={'type':'object','required':['content'],'properties':{'content':{'type':'string'}}}),publish)
    program=("from urllib.parse import quote\nremembered = quote('verified-37')\n"
        f"saved = call_tool('file_write', {{'path':{str(first)!r},'content':remembered}})\n"
        "published = call_tool('publish_record', {'content':remembered})\n"
        f"final = call_tool('file_write', {{'path':{str(last)!r},'content':remembered+quote('-finished')}})\n"
        "{'published':published['output']['published'],'path':final['output']['path']}")
    if with_inspection:
        reg.register(ToolSpec('read_record','Read external retained data',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5),
            lambda i:ToolResult(i.invocation_id,i.tool_name,'completed',{'large':'x'*40000}))
        inspect="call_tool('inspect_retained_evidence', {'requests':[{'invocation_id':retained['invocation_id'],'json_pointer':'/large','offset':OFFSET,'length':12000}]})"
        program=inspect.replace('OFFSET','0')+'\n'+program+'\n'+inspect.replace('OFFSET','12000')+'\n'+inspect.replace('OFFSET','24000')
    selection={'type':'tool_use','id':'request-program','name':code_mode.TOOL_NAME,
        'input':{'code':program,'tool_names':['file_write','publish_record']+(['inspect_retained_evidence'] if with_inspection else [])}}
    state={'tool_registry':reg}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    class Model:
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if {t['name'] for t in kwargs.get('tools',[])}=={'select_artifact_delivery'}:
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                raw=kwargs['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                from tests.e2e.test_quoted_reply_review_and_cleanup import call
                return call('select_artifact_delivery',{'status':'selected','selected_ids':[r['candidate_id'] for r in json.loads(raw)],'reason':'Both saved records'})
            assert last.read_text()=='verified-37-finished'
            return text('The prepared record was published and saved.')
    runner=AgentOrchestrator(model_client=Model())
    # Start through the real graph with a model-selected program, not a helper
    # executor. The model view establishes the exact scoped selection.
    class StartModel(Model):
        reads=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if with_inspection and not self.reads:
                self.reads+=1
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read-prelude','name':'code_mode',
                    'input':{'tool_names':['read_record'],'code':"retained=call_tool('read_record', {})\nretained"}}]}
            return {'stop_reason':'tool_use','content':[selection]}
    result=AgentOrchestrator(model_client=StartModel()).run_turn(conversation_id='telegram:77',principal_id='telegram_chat',
        user_message=prompt,conversation_history=[],tool_registry=reg,policy_store=runtime.store,approval_store=runtime.store)
    assert result.suspended_for_approval, [(r.tool_name,r.status,r.output) for r in result.tool_results]
    assert first.read_text()=='verified-37' and not last.exists() and not calls
    suspended=runtime.store.get_suspended_turn(result.approval_id)
    assert suspended and suspended.pending_tool_calls[0]['_code_mode_session_id']==result.code_mode_session_id
    # A recovery wrapper may finish without carrying the orchestrator's
    # suspended result. Cleanup still has the original durable approval.
    from nullion.code_mode_sessions import SessionJournal
    cleanup_state = {'runtime_store': runtime.store, 'principal_id': 'telegram_chat',
        'conversation_id': 'telegram:77', 'user_message': prompt,
        'code_mode_session_id': result.code_mode_session_id}
    code_mode.finish_request(cleanup_state)
    checkpoint = SessionJournal(cleanup_state).read(result.code_mode_session_id)
    assert checkpoint['phase'] == 'approval' and checkpoint['approval_id'] == result.approval_id
    runtime.checkpoint(force=True)
    # Restore both native approvals and the VM from durable state, not the old
    # Python objects. This also exercises process-independent session claims.
    restarted=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    suspended=restarted.store.get_suspended_turn(result.approval_id)
    from nullion.chat_operator import approved_read_needs_continuation
    assert approved_read_needs_continuation(restarted,suspended,tool_registry=reg,orchestrator=runner)
    restarted.approve_approval_request(result.approval_id, principal_id='telegram_chat',permissions=['tool:publish_record'])
    resumed=runner.resume_turn(conversation_id='telegram:77',principal_id='telegram_chat',user_message=prompt,
        messages_snapshot=suspended.messages_snapshot,suspended_tool_calls=suspended.pending_tool_calls,
        tool_registry=reg,policy_store=restarted.store,approval_store=restarted.store)
    assert not resumed.suspended_for_approval, [(r.tool_name,r.status,r.output) for r in resumed.tool_results]
    assert last.exists(), json.dumps({'results':[(r.tool_name,r.status,r.output) for r in resumed.tool_results], 'snapshot':suspended.messages_snapshot, 'session':resumed.code_mode_session_id},ensure_ascii=False)
    assert last.read_text()=='verified-37-finished'
    assert published.read_text()=='verified-37' and len(calls)==1
    writes=[r for r in resumed.tool_results if r.tool_name=='file_write' and r.status=='completed']
    assert len(writes)==2 and len({r.invocation_id for r in writes})==2
    summary=[r for r in resumed.tool_results if r.tool_name==code_mode.TOOL_NAME][-1]
    assert summary.status=='completed'
    if with_inspection:
        assert summary.output['value']['output']['windows'][0]['inspection_status']=='budget_exhausted'
    else:
        assert summary.output['value']['published'] is True
    assert summary.output['session_id']==result.code_mode_session_id
    assert summary.output['host_calls']=={'urllib.parse.quote':2}
    assert 'published' in resumed.final_text
    closed = SessionJournal({**cleanup_state, 'runtime_store': restarted.store}).read(result.code_mode_session_id)
    assert closed['phase'] == 'closed' and closed['snapshot'] is None and closed['selected'] is None
    # Even a duplicate caller bypassing the transport's resume claim cannot
    # restart an already consumed program or repeat the protected effect.
    runner.resume_turn(conversation_id='telegram:77',principal_id='telegram_chat',user_message=prompt,
        messages_snapshot=suspended.messages_snapshot,suspended_tool_calls=suspended.pending_tool_calls,
        tool_registry=reg,policy_store=restarted.store,approval_store=restarted.store,max_iterations=1)
    assert len(calls)==1 and published.read_text()=='verified-37'


@pytest.mark.asyncio
@pytest.mark.parametrize('exact_approval', [False, True])
async def test_async_harness_resumes_same_write_program_after_restart(tmp_path,monkeypatch,exact_approval):
    from nullion import code_mode
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=ToolRegistry();calls=[]
    def save(invocation):
        calls.append((invocation.tool_name, invocation.invocation_id))
        with (tmp_path/(invocation.tool_name+'.txt')).open('a') as stream:stream.write(str(invocation.arguments['value']))
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'value':invocation.arguments['value']})
    for name,risk,approval in [('prepare_record',ToolRiskLevel.LOW,False),('publish_record',ToolRiskLevel.HIGH,True)]:
        reg.register(ToolSpec(name,name,risk,ToolSideEffectClass.WRITE,approval,5,
            **({'permission_scope': 'exact_invocation'} if approval and exact_approval else {}),
            input_schema={'type':'object',
            'properties':{'value':{'type':'integer'}},'required':['value']}),save)
    flow={'conversation_id':'telegram:77','original_user_message':'確認して保存'}
    observed=[]
    tool=nullion_tools_as_langchain_tools(reg,allowed_tools=['prepare_record','publish_record'],principal_id='telegram_chat',
        cleanup_scope='task-37',policy_store=runtime.store,tool_flow_context=flow,tool_result_callback=observed.append)[0]
    response=json.loads(await tool.ainvoke({'tool_names':['prepare_record','publish_record'],
        'code':"remembered = 37\ncall_tool('prepare_record', {'value':remembered})\ncall_tool('publish_record', {'value':remembered})['output']"}))
    assert response['output']['reason']=='code_mode_awaiting_approval',response
    assert [name for name, _ in calls] == ['prepare_record']
    identity=response['output']['session_id']
    approval=runtime.store.list_approval_requests()[-1]
    runtime.checkpoint(force=True)
    restarted=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    restarted.approve_approval_request(approval.approval_id,principal_id='telegram_chat',permissions=['tool:publish_record'])
    resumed=[]
    tool=nullion_tools_as_langchain_tools(reg,allowed_tools=['prepare_record','publish_record'],principal_id='telegram_chat',
        cleanup_scope='task-37',policy_store=restarted.store,tool_flow_context={**flow,'code_mode_session_id':identity},
        tool_result_callback=resumed.append)[0]
    await tool.coroutine._nullion_code_mode_resume()
    assert [name for name, _ in calls] == ['prepare_record', 'publish_record']
    if exact_approval:
        assert calls[-1][1] == approval.context['approval_invocation_id']
        assert len(restarted.store.list_approval_requests()) == 1
    with pytest.raises(RuntimeError, match='not resumable'):
        await tool.coroutine._nullion_code_mode_resume()
    assert len(calls) == 2
    assert (tmp_path/'prepare_record.txt').read_text()=='37'
    assert (tmp_path/'publish_record.txt').read_text()=='37'
    assert resumed[-1].tool_name==code_mode.TOOL_NAME and resumed[-1].status=='completed',resumed[-1]
    from nullion.deep_agent_runner import _pending_approval_from_tool_results
    assert _pending_approval_from_tool_results(resumed) is None
    tool.coroutine._nullion_code_mode_finish()


def test_failed_program_recovers_with_same_evidence_and_one_final_delivery(tmp_path,monkeypatch,record_property):
    import time
    from nullion import code_mode
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    calls=[];samples=[]
    for index,prompt in enumerate(['保存を完了してください','Termina el archivo','Termine le fichier']):
        prep=tmp_path/f'prep-{index}.txt';final=tmp_path/f'final-{index}.txt'
        registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
        def prepare(invocation):
            calls.append(('prepare',index))
            with prep.open('a') as stream:stream.write('37')
            return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'prepared':True})
        def read(invocation):
            calls.append(('read',index))
            return ToolResult(invocation.invocation_id,invocation.tool_name,
                'failed' if invocation.arguments['fail'] else 'completed',{'executed':False} if invocation.arguments['fail'] else {'value':42})
        for name,effect,handler in [('prepare_record',ToolSideEffectClass.WRITE,prepare),('read_record',ToolSideEffectClass.READ,read),
            ('file_write',ToolSideEffectClass.WRITE,_build_file_write_handler(tmp_path,allowed_roots=(tmp_path,)))]:
            registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,5,input_schema={'type':'object'}),handler)
        class Model:
            rounds=0
            def create(self,**kwargs):
                protocol=workflow_provider_reply(kwargs,completion=True)
                if protocol is not None:return protocol
                self.rounds+=1;time.sleep(.05)
                if self.rounds==1:
                    return call(code_mode.TOOL_NAME,{'tool_names':['prepare_record','read_record','file_write'],
                        'code':"kept = 37\ncall_tool('prepare_record', {})\ncall_tool('read_record', {'fail':True})\n"
                            + f"call_tool('file_write', {{'path':{str(final)!r},'content':'wrong'}})"})
                if self.rounds==2:
                    assert prep.read_text()=='37' and not final.exists()
                    result=call(code_mode.TOOL_NAME,{'tool_names':['read_record','file_write'],
                        'code':"fresh = call_tool('read_record', {'fail':False})\n"
                            + f"call_tool('file_write', {{'path':{str(final)!r},'content':str(kept + fresh['output']['value'])}})"})
                    result['content'][0]['id']='corrected-program'
                    return result
                assert self.rounds==3
                return text('Saved the verified result 79.')
        model=Model();started=time.monotonic()
        result=AgentOrchestrator(model_client=model).run_turn(conversation_id=f'web:recovery-{index}',principal_id='workspace_admin',
            user_message=prompt,conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
        samples.append(time.monotonic()-started)
        assert model.rounds==3 and '79' in result.final_text
        assert prep.read_text()=='37' and final.read_text()=='79'
        assert result.artifacts==[str(final)]
        programs=[r for r in result.tool_results if r.tool_name==code_mode.TOOL_NAME]
        assert [r.status for r in programs]==['failed','completed']
        assert programs[0].output['session_id']==programs[1].output['session_id']
        assert calls.count(('prepare',index))==1 and calls.count(('read',index))==2
    assert sum(samples)/len(samples)<2 and max(samples)<4,samples
    record_property('mean_recovery_seconds',sum(samples)/len(samples))


def test_unusable_session_stops_instead_of_repeating_scope_or_extending_budget(tmp_path,monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    registry=ToolRegistry();executed=[]
    for name,effect in [('read_record',ToolSideEffectClass.READ),('write_record',ToolSideEffectClass.WRITE)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,5,input_schema={'type':'object'}),
            lambda i:executed.append(i) or ToolResult(i.invocation_id,i.tool_name,'completed',{}))
    from nullion.code_mode_sessions import SessionJournal
    from nullion.agent_orchestrator import TurnContinuationEvidence
    journal=SessionJournal({'principal_id':'telegram_chat','conversation_id':'telegram:blocked-session',
        'user_message':'確認','runtime_store':runtime.store})
    crashed=journal.create();journal.save(crashed,phase='running')
    registry.turn_continuation_evidence=TurnContinuationEvidence('telegram:blocked-session','telegram_chat',('owned-prior',),(),
        same_request_retry=True,user_message='確認',code_mode_session_id=crashed['id'])
    class Model:
        rounds=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            self.rounds+=1
            result=call('code_mode',{'tool_names':['read_record'],
                'code':"call_tool('write_record', {})" if self.rounds==1 else "call_tool('read_record', {})"})
            result['content'][0]['id']=f'program-{self.rounds}'
            return result
    model=Model()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='telegram:blocked-session',principal_id='telegram_chat',
        user_message='確認',conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    assert model.rounds==1 and not executed
    assert result.response_fulfilled is False and 'cannot continue safely' in result.final_text
    assert not result.reached_iteration_limit and not result.suspended_for_approval
    assert not runtime.store.list_approval_requests()


@pytest.mark.parametrize('prompt',['画像を生成してください','Genera la imagen'])
def test_provider_access_failure_is_not_retried_with_different_image_prompts(tmp_path,monkeypatch,prompt):
    from urllib.error import HTTPError
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,register_media_plugin
    from nullion.preferences import save_preferences
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    save_preferences({'outbound_request_mode':'allow_all'})
    called=[]
    def rejected(*args):
        called.append(args)
        raise HTTPError('https://provider.invalid/generate',402,'Provider rejected request',{},None)
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));register_media_plugin(registry,image_generator=rejected)
    class Model:
        rounds=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            self.rounds+=1
            programs=[t for t in kwargs.get('tools',[]) if t['name']=='code_mode']
            offered=programs[0]['input_schema']['properties']['tool_names']['items']['enum'] if programs else []
            if 'image_generate' in offered:
                response=call('code_mode',{'tool_names':['image_generate'],
                    'code':f"call_tool('image_generate', {{'prompt':{('variant '+str(self.rounds))!r},'output_path':{str(tmp_path/'output.png')!r}}})"})
                response['content'][0]['id']=f'program-{self.rounds}';return response
            return text('The image provider rejected the request. Image generation needs provider attention.')
    model=Model()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='web:provider-rejection',principal_id='workspace_admin',
        user_message=prompt,conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,max_iterations=6)
    assert len(called)==1 and model.rounds<=3
    assert not result.artifacts and not result.reached_iteration_limit
    assert not result.suspended_for_approval
    assert 'provider' in result.final_text


@pytest.mark.parametrize('prompt',['Create a clear infographic image as .png.','説明図を .png 画像で作成してください。'])
def test_local_renderer_finishes_image_after_provider_rejection(tmp_path,monkeypatch,prompt):
    import shlex,sys
    from datetime import UTC,datetime
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,register_media_plugin,_build_terminal_exec_handler
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.preferences import save_preferences
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on');monkeypatch.setenv('NULLION_TERMINAL_ENABLED','1')
    save_preferences({'outbound_request_mode':'allow_all'})
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    image_path=workspace_storage_roots_for_principal('workspace_admin').artifacts/'infographic.png';provider_calls=[]
    def rejected(*args):
        from urllib.error import HTTPError
        provider_calls.append(args);raise HTTPError('https://provider.invalid/generate',402,'Provider rejected request',{},None)
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));register_media_plugin(registry,image_generator=rejected)
    registry.register(ToolSpec('terminal_exec','Render the requested local image',ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC,False,10,input_schema={'type':'object','properties':{'command':{'type':'string'}},'required':['command']}),
        _build_terminal_exec_handler(tmp_path,allowed_roots=(tmp_path,)))
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-render',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)))
    script="from PIL import Image,ImageDraw; im=Image.new('RGB',(640,480),(17,34,51)); ImageDraw.Draw(im).text((30,30),'Verified infographic 37',fill='white'); im.save("+repr(str(image_path))+")"
    command=shlex.quote(sys.executable)+' -c '+shlex.quote(script)
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            names={t['name'] for t in kwargs.get('tools',[])}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.png','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['image_generation'],'tool_names':['image_generate'],
                'required_tool_names':['image_generate'],'artifact_extensions':['.png']})
            if self.phase==2:return call('code_mode',{'tool_names':['image_generate'],
                'code':f"call_tool('image_generate', {{'prompt':'A clear infographic','output_path':{str(image_path)!r}}})"})
            if self.phase==3:return call('request_tool_scope',{'capabilities':['local_shell'],'tool_names':['terminal_exec'],
                'required_tool_names':['terminal_exec'],'artifact_extensions':['.png']})
            if self.phase==4:
                result=call('code_mode',{'tool_names':['terminal_exec'],'code':'import unavailable_example_library'})
                result['content'][0]['id']='unsupported-import';return result
            if self.phase==5:
                assert 'ModuleNotFoundError' in json.dumps(kwargs['messages'])
                assert 'unavailable_example_library' in json.dumps(kwargs['messages'])
                result=call('code_mode',{'tool_names':['terminal_exec'],'code':f"call_tool('terminal_exec', {{'command':{command!r}}})['output']"})
                result['content'][0]['id']='local-renderer';return result
            assert self.phase==6
            return text('Created the infographic image using the local renderer.')
    model=Model()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:local-renderer'})
        assert response.status_code==200 and response.json().get('artifacts'),response.text
        assert len(provider_calls)==1 and model.phase==6
        artifacts=response.json()['artifacts'];assert len(artifacts)==1
        downloaded=client.get(artifacts[0]['url']);assert downloaded.content==image_path.read_bytes()
        with Image.open(image_path) as picture:assert picture.size==(640,480) and picture.getpixel((100,100))==(17,34,51)
        events=[e for e in runtime.store.list_conversation_events('web:local-renderer') if e.get('event_type')=='conversation.chat_turn']
        scopes=[r['output'] for r in events[-1]['tool_results'] if r['tool_name']=='request_tool_scope']
        assert scopes[-1]['required_tool_names']==['terminal_exec'],scopes
        assert events[-1]['response_fulfilled'] is True
        assert 'I need one more step before I can complete this' not in response.json()['text']
