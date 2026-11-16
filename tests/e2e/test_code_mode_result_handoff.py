"""Real API/SQLite/Monty handoff; external source records are deterministic."""
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def test_large_program_returns_native_findings_to_model_and_visible_reply(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    tool_name,capability='browser_run_task','browser'
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_MODEL_TOOL_RESULT_MAX_CHARS','10000')
    native_calls=[]
    source={'result_kind':'source_observations','report_status':'partial','goal_status':'answered',
        'page_url':'https://example.test/records',
        'observations':[{'label':'選択値','value':'37','quote':'37','source_url':'https://example.test/records',
            'control_state':{'tag':'SELECT','value':'37',
                'options':[{'label':'option-'+str(n),'value':str(n),'selected':n==37} for n in range(200)]}},
            {'label':'Resultado','value':'verified-73','quote':'Exact verified-73, 6:00 PM, party 2',
                'source_url':'https://example.test/records',
                'control_state':{'tag':'A','disabled':False,'label':'Exact verified-73, 6:00 PM, party 2'}}],
        'captured_documents':[{'source_id':0,'source_url':'https://example.test/records',
            'main_document_text':'optional context '*1000}]}
    def external_source(invocation):
        native_calls.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',source)
    registry=ToolRegistry()
    registry.register(ToolSpec(tool_name,'Read external records',ToolRiskLevel.LOW,ToolSideEffectClass.READ,
        False,5,capability_tags=(capability,),code_mode_allowed=True),external_source)
    class Model:
        phase=0
        errors=[]
        def create(self,**kwargs):
            try:return self.respond(**kwargs)
            except Exception as error:self.errors.append(repr(error));raise
        def respond(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:
                return call('request_tool_scope',{'capabilities':[capability],'tool_names':[tool_name],
                    'required_tool_names':[tool_name],'source_user_requested':True,'source_evidence':self.prompt})
            if self.phase==2:
                return call('code_mode',{'tool_names':[tool_name], 'code':
                    f"result = call_tool({tool_name!r}, {{}})\n"
                    + "{'untrusted_program_claim':'not evidence','huge':'x'*30000,'source':result['output']}"})
            assert self.phase==3, 'Handoff must not introduce another research/recovery round'
            # Inspect the actual protocol block delivered to inference, not the
            # unbounded persisted source or a direct call to its compactor.
            blocks=[b for m in kwargs['messages'] for b in m.get('content',[])
                if isinstance(b,dict) and b.get('type')=='tool_result' and b.get('tool_use_id')=='code_mode-id']
            assert blocks, kwargs['messages'][-1:]
            payload=json.loads(blocks[-1]['content'][0]['text'])
            assert len(blocks[-1]['content'][0]['text']) <= 10000
            output=payload['output']
            assert isinstance(output,dict), payload
            evidence=output['native_observations'][0]
            assert evidence['invocation_id']==native_calls[-1]
            findings=evidence['output']['observations']
            assert findings[-1]['value']=='verified-73'
            assert findings[-1]['control_state']['disabled'] is False
            assert findings[0]['control_state']['selected_options'][0]['value']=='37'
            return text('Verified: verified-73. 6:00 PM is enabled for 2 people. No action was taken.')
    model=Model();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,prompt in enumerate(['確認してください','Consulta los registros','Consulte les résultats']):
            model.phase=0;model.prompt=prompt
            cid='web:native-handoff-'+str(index)
            started=time.monotonic()
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
            samples.append(time.monotonic()-started)
            assert not model.errors, '\n'.join(model.errors)
            assert response.status_code==200 and 'verified-73' in response.json()['text'], (response.text,model.errors)
            events=[e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn']
            results=events[-1]['tool_results']
            assert any(r['tool_name']==tool_name and r['status']=='completed' for r in results)
            summary=next(r for r in results if r['tool_name']=='code_mode')
            assert summary['output']['native_observations'][0]['invocation_id']==native_calls[-1]
    assert not model.errors and len(native_calls)==3
    assert sum(samples)/len(samples)<4 and max(samples)<6, samples
    record_property('mean_handoff_seconds',sum(samples)/len(samples))
