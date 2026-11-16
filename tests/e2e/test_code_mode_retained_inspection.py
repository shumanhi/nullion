"""Real API/SQLite/Monty batches retained-data inspection without new research."""
import json
import os
import time
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

def test_retained_inspection_stays_in_one_program_with_shared_budget(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    registry=ToolRegistry();reads=[]
    def external(i):
        reads.append(i.invocation_id)
        return ToolResult(i.invocation_id,i.tool_name,'completed',{'a':'37','b':'73','large':'x'*70000})
    registry.register(ToolSpec('read_fixture','Read external data',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,capability_tags=('web',)),external)
    class Model:
        phase=0;errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            try:return self.respond(kw)
            except Exception as e:self.errors.append(repr(e));raise
        def respond(self,kw):
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':['read_fixture'],'required_tool_names':['read_fixture']})
            if self.phase==2:return call('code_mode',{'tool_names':['read_fixture'],'code':"saved=call_tool('read_fixture', {})\nsaved"})
            if self.phase==3:
                definition=next(t for t in kw['tools'] if t['name']=='code_mode')
                assert 'inspect_retained_evidence' in definition['input_schema']['properties']['tool_names']['items']['enum']
                assert not any(t['name']=='inspect_retained_evidence' for t in kw['tools'])
                program=call('code_mode',{'tool_names':['inspect_retained_evidence'],'code':
                    "def inspect(pointer, offset, length):\n"
                    "    return call_tool('inspect_retained_evidence', {'requests':[{'invocation_id':saved['invocation_id'],'json_pointer':pointer,'offset':offset,'length':length}]})['output']['windows'][0]\n"
                    "a=inspect('/a',0,500)['content']\nb=inspect('/b',0,500)['content']\n"
                    "windows=[inspect('/large',offset,12000) for offset in [0,12000,24000]]\n"
                    "{'a':a,'b':b,'exhausted':windows[-1].get('inspection_status')=='budget_exhausted','spent':len(a)+len(b)+sum(w.get('shown_chars',0) for w in windows)}"})
                program['content'][0]['id']='inspect-program';return program
            assert self.phase==4,'Do not inspect each field in another model round or repeat research'
            wire=json.dumps(kw['messages'])
            assert 'budget_exhausted' in wire and '73' in wire
            return text('Verified values: 37 and 73.')
    model=Model();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,prompt in enumerate(['確認してください','Comprueba los datos','Vérifiez les données']):
            model.phase=0;start=time.monotonic();cid='web:retained-program-'+str(index)
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});samples.append(time.monotonic()-start)
            assert not model.errors,model.errors
            assert response.status_code==200 and '37 and 73' in response.json()['text'],response.text
            turns=[e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn'];results=turns[-1]['tool_results']
            programs=[r for r in results if r['tool_name']=='code_mode']
            assert len(programs)==2 and all(r['status']=='completed' for r in programs),[(r['status'],r['output']) for r in programs]
            assert programs[0]['output']['value_limit']['omitted']
            assert programs[1]['output']['value']=={'a':'37','b':'73','exhausted':True,'spent':24000}
            inspections=[r for r in results if r['tool_name']=='inspect_retained_evidence']
            assert len(inspections)==5
            assert all(r['status']=='completed' for r in inspections)
    assert len(reads)==3 and sum(samples)/len(samples)<4 and max(samples)<6,samples
    record_property('retained_inspection_mean_seconds',sum(samples)/len(samples))


@pytest.mark.parametrize('implicit_window',[False,True])
def test_invalid_projection_pointer_is_repaired_locally_without_research_restart(tmp_path,monkeypatch,implicit_window):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    registry=ToolRegistry();reads=[]
    def external(invocation):
        reads.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',
            {'items':[{'title':'Verified option 37','url':'https://source.test/37'}],'body':'x'*70000})
    registry.register(ToolSpec('read_fixture','Read external source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,capability_tags=('web',),
        input_schema={'type':'object','additionalProperties':False,'properties':{}}),external)
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':['read_fixture'],'required_tool_names':['read_fixture']})
            if self.phase==2:
                code="saved=call_tool('read_fixture', {})\nsaved"
                names=['read_fixture']
            elif self.phase==3:
                code="call_tool('inspect_retained_evidence', {'requests':[{'invocation_id':saved['invocation_id'],'json_pointer':'/collection_views/0/rows','offset':0,'length':1000}]})"
                names=['inspect_retained_evidence']
            elif self.phase==4:
                wire=json.dumps(kwargs['messages'])
                assert 'available_keys' in wire and 'correct_arguments' in wire
                code="call_tool('inspect_retained_evidence', {'requests':[{'invocation_id':saved['invocation_id'],'json_pointer':'/items/0','offset':0,'length':1000}]})['output']"
                if implicit_window:code=code.replace(",'offset':0,'length':1000",'')
                names=['inspect_retained_evidence']
            else:
                assert self.phase==5,'Local inspection correction must not restart source research'
                assert 'Verified option 37' in json.dumps(kwargs['messages'])
                return text('Verified option 37. https://source.test/37')
            program=call('code_mode',{'tool_names':names,'code':code})
            program['content'][0]['id']=f'research-program-{self.phase}'
            return program
    model=Model()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        response=client.post('/api/chat',json={'text':'候補を確認してください。','conversation_id':'web:local-inspection-repair'})
        if 'Verified option 37' not in response.json()['text']:
            turns=runtime.store.list_recent_conversation_events('web:local-inspection-repair',event_type='conversation.chat_turn')
            print(json.dumps({'phase':model.phase,'errors':getattr(model,'errors',[]),'results':[
                {'name':r['tool_name'],'status':r['status'],'error':r.get('error'),
                 'output':{k:v for k,v in r['output'].items() if k in {'reason','execution_error','failures','request_errors','validation_errors','recovery'}}}
                for t in turns for r in t.get('tool_results',[])]},ensure_ascii=False,default=str))
        assert response.status_code==200 and 'Verified option 37' in response.json()['text'],(response.text,model.phase,getattr(model,'errors',[]),
            [(e.get('event_type'),e.get('error_type'),e.get('error')) for e in runtime.store.list_conversation_events('web:local-inspection-repair')])
    assert len(reads)==1 and model.phase==5
    turn=runtime.store.list_recent_conversation_events('web:local-inspection-repair',event_type='conversation.chat_turn')[-1]
    inspections=[r for r in turn['tool_results'] if r['tool_name']=='inspect_retained_evidence']
    assert [r['status'] for r in inspections]==['failed','completed']
    assert inspections[0]['output']['request_errors'][0]['available_keys']==['items','body']
