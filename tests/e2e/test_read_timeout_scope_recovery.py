"""A timed-out read does not disable healthy requests through the same tool."""
import json,os,threading,time
import pytest
from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_scheduled_report_completion import setup_runtime,text_response,tool_response
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker API/store E2E')


@pytest.mark.parametrize('name',['web_record_read','browser_source_read'])
@pytest.mark.parametrize('prompt',['別の公開情報源も確認して。','Comprueba también la otra fuente pública.'])
def test_read_timeout_keeps_same_tool_for_a_different_source(tmp_path,monkeypatch,name,prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from tests.source_answer_fixture import source_presentation_response
    runtime=setup_runtime(tmp_path,monkeypatch);monkeypatch.setattr(chat_store,'_store',None)
    for k,v in {'NULLION_ENV_FILE':str(tmp_path/'none.env'),'NULLION_MEMORY_ENABLED':'0',
        'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_CHAT_STARTUP_WARMUP':'0'}.items():monkeypatch.setenv(k,v)
    release=threading.Event();reads=[];registry=ToolRegistry()
    urls=['https://slow.example/record','https://healthy.example/record']
    def external_read(inv):
        reads.append(inv.arguments['url'])
        if inv.arguments['url']==urls[0]:release.wait(2)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'source_url':inv.arguments['url'],
            'text':'Value: 37','record':{'url':inv.arguments['url'],'text':'Value: 37'},
            'source_record_collections':[{'path':['record'],'identity_fields':['url'],'cardinality':'one'}]})
    registry.register(ToolSpec(name,'Read one requested public source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,
        False,.04,capability_tags=('web',),input_schema={'type':'object','properties':{'url':{'type':'string'}},'required':['url']}),external_read)
    # A second registered reader makes the real recovery graph widen scope;
    # the timed-out reader must remain callable for a different source.
    registry.register(ToolSpec('web_fetch','Alternative public reader',ToolRiskLevel.LOW,ToolSideEffectClass.READ,
        False,.04,capability_tags=('web',),input_schema={'type':'object','properties':{'url':{'type':'string'}},'required':['url']}),external_read)
    class Model:
        calls=0
        packets=[]
        def create(self,**kw):
            self.packets.append({'system':str(kw.get('system',''))[:150],'tools':[r['name'] for r in kw.get('tools',[])]})
            response=workflow_provider_reply(kw,completion=True)
            if response is not None:return response
            names={r['name'] for r in kw.get('tools',[])}
            if 'present_source_answer' in names:return source_presentation_response(kw)
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return text_response(json.dumps({'web_action':'live_research','requested_tool_names':[name],
                'confidence':1}))
            self.calls+=1
            if self.calls==1:return tool_response('request_tool_scope',{'capabilities':['web'],'tool_names':[name]})
            assert name in names,{'available':sorted(names),'calls':self.calls}
            if self.calls in (2,3):return tool_response(name,{'url':urls[self.calls-2]})
            return text_response('37 — '+urls[1])
    model=Model();runtime.model_client=model
    started=time.monotonic()
    try:
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:read-recovery'})
            assert result.status_code==200 and '37' in result.json()['text'],json.dumps({'response':result.json(),'calls':model.calls,'packets':model.packets,'reads':reads,'events':runtime.store.list_recent_conversation_events('web:read-recovery',limit=30)},ensure_ascii=False)
            turns=[r for r in runtime.store.list_conversation_events('web:read-recovery') if r.get('event_type')=='conversation.chat_turn']
            outcomes=[r for r in turns[-1]['tool_results'] if r['tool_name']==name]
            assert [r['status'] for r in outcomes]==['failed','completed'],outcomes
            assert outcomes[0]['output']['reason']=='handler_timeout'
            assert outcomes[0]['output']['failure_scope']=='invocation'
            # The API may already have enough scope to try the next source.
            # Automatic widening/retirement has a separate real-registry check.
            assert outcomes[-1]['output']['source_url']==urls[1]
            assert reads==urls and time.monotonic()-started<3
            history=client.get('/api/chat/history/web:read-recovery?sync_runtime=1').json()['messages']
            assert any(r['role']=='bot' and '37' in r['text'] for r in history)
    finally:release.set()
