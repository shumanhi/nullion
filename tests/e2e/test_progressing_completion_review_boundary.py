"""Real Web/store/review recovery; only the external provider and read source are fakes."""
import json,os
import pytest
from tests.e2e.test_scheduled_report_completion import setup_runtime,text_response,tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('prompt',['四つの現在の記録を集めてください。','Recoge los cuatro registros actuales.'])
@pytest.mark.parametrize('stalled',[False,True,'uncited','navigation'])
def test_review_continues_new_facts_and_stops_identical_facts_through_real_web(tmp_path,monkeypatch,prompt,stalled,cron_surface=False):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    runtime=setup_runtime(tmp_path,monkeypatch);registry=ToolRegistry();reads=[]
    def read(inv):
        ident=inv.arguments['id'];reads.append(ident)
        output={'id':1 if stalled is True else ident,'value':10 if stalled is True else ident*10}
        if stalled=='navigation' and ident>1:
            output={'result_kind':'browser_navigation','page_url':f'https://source.example/{ident}'}
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',output)
    registry.register(ToolSpec('opaque_read','Read one current record',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,
        input_schema={'type':'object','properties':{'id':{'type':'integer'}},'required':['id']},capability_tags=('web',)),read)
    class Provider:
        reviews=0;read_sent=False
        def create(self,**kw):
            system=kw.get('system') or '';names={r['name'] for r in kw.get('tools') or ()}
            def text(v):return text_response(v if isinstance(v,str) else json.dumps(v))
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':f'Verify record {i}','requirement_kind':'fact',
                        'evidence_subject_id':f's{i}','eligible_evidence_tool_names':['opaque_read']} for i in range(1,5)]})
            if 'semantic completion reviewer' in system:
                self.reviews+=1;self.read_sent=False;packet=json.loads(kw['messages'][0]['content'][0]['text'])
                records=[r for r in packet['tool_evidence'] if r['tool_name']=='opaque_read' and r['status']=='completed']
                complete=len(reads)>=4 and not stalled
                return text({'disposition':'complete' if complete else 'retry','retry_tool_names':[] if complete else ['opaque_read'],
                    'unresolved_requirements':[] if complete else ['Remaining records need current evidence'],
                    'all_material_requirements_enumerated':True,
                    'supported_findings':[] if complete else [{'text':'Confirmed record 1: 10.','evidence_invocation_ids':[records[0]['invocation_id']]}],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':i<len(records) and not(stalled and i>0),
                        'draft_covered':not (stalled is True or stalled=='navigation'),'evidence_invocation_ids':[records[i]['invocation_id']] if i<len(records) and not(stalled and i>0) else []}
                        for i,r in enumerate(packet['material_requirement_contract'])]})
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':None,'confidence':1,'embedded_media_extensions':[]})
            if 'relationship' in system and 'target_index' in system:return text({'relationship':'independent','effect':'none','confidence':1,'target_index':None})
            if 'Return JSON only for a single already-selected read-only tool' in system:return text({'use_direct':False,'arguments':{}})
            if 'request_tool_scope' in names and 'opaque_read' not in names:return tool_response('request_tool_scope',{'capabilities':['web'],'tool_names':['opaque_read']})
            if 'opaque_read' in names and not self.read_sent:
                self.read_sent=True;return tool_response('opaque_read',{'id':len(reads)+1})
            return text('Verified records: '+', '.join(str(i*10) for i in range(1,len(reads)+1))+'.')
    for key,val in {'NULLION_ENV_FILE':str(tmp_path/'none.env'),'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_CHAT_STARTUP_WARMUP':'0','NULLION_ACTIVITY_TRACE_ENABLED':'0'}.items():monkeypatch.setenv(key,val)
    monkeypatch.setattr(chat_store,'_store',None);runtime.model_client=Provider()
    cid='telegram:123' if cron_surface else 'web:review-progress'
    if cron_surface:
        from nullion.crons import CronJob
        from tests.e2e.test_scheduled_report_completion import run_scheduled,telegram_boundary
        deliveries,send=telegram_boundary(monkeypatch)
        output=run_scheduled(CronJob(id='progress',name='確認',task=prompt,schedule='0 0 1 1 *',
            delivery_channel='telegram',delivery_target='123'),runtime.model_client,runtime,registry,send)
        assert output['response_fulfilled'] is True,output
        assert len(deliveries)==1,deliveries
        visible=deliveries[0]['text']
    else:
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=runtime.model_client),registry=registry)) as client:
            result=client.post('/api/chat',json={'text':'/chat '+prompt,'conversation_id':cid});assert result.status_code==200,result.text
            visible=result.json()['text']
    reviews=runtime.store.list_recent_conversation_events(cid,event_type='conversation.completion_review')
    if stalled:
        assert len(reads)==3 and reviews[-1]['disposition']!='complete'
        assert ('10' in visible if stalled=='uncited' else 'Confirmed record 1: 10' in visible) and '40' not in visible
    else:
        assert reads==[1,2,3,4] and len(reviews)==4 and reviews[-1]['disposition']=='complete'
        assert all(str(i*10) in visible for i in range(1,5)),visible


def test_scheduled_delivery_keeps_productive_review_recovery_and_delivers_once(tmp_path,monkeypatch):
    test_review_continues_new_facts_and_stops_identical_facts_through_real_web(
        tmp_path,monkeypatch,'四つの現在の記録を集めてください。',False,cron_surface=True)
