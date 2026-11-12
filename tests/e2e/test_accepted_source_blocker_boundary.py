"""One lookup can offer verified records or an explicitly accepted source blocker."""
import json
import os
import pytest
from tests.e2e.test_scheduled_report_completion import setup_runtime,text_response,tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E')


@pytest.mark.parametrize('cron_surface',[False,True])
@pytest.mark.parametrize('prompt',[
    '比較できる二つの候補を調べるか、情報源に具体的な問題があれば説明してください。',
    'Busca dos opciones comparables o explica el bloqueo concreto de la fuente.',
])
def test_explicit_blocker_alternative_remains_one_verified_lookup_outcome(tmp_path,monkeypatch,prompt,cron_surface):
    from fastapi.testclient import TestClient
    from nullion import chat_store,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    runtime=setup_runtime(tmp_path,monkeypatch);registry=ToolRegistry();reads=[]
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{
            'source_url':'https://source.example/inventory',
            'text':'The reservation form cannot load its rate inventory. No rate records were displayed.',
            'interaction_status':'blocked'})
    registry.register(ToolSpec('opaque_read','Read selected inventory source',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2,capability_tags=('web',)),read)
    class Provider:
        def create(self,**kw):
            system=kw.get('system') or '';names={r['name'] for r in kw.get('tools') or ()}
            def text(d):return text_response(d if isinstance(d,str) else json.dumps(d,ensure_ascii=False))
            if 'material-requirements planner' in system:
                assert 'plan that alternative as part of the same lookup-outcome subject' in system
                assert 'never additional fact/comparison_side entities' in system
                packet=json.loads(kw['messages'][0]['content'][0]['text']);candidate=packet['discovered_sources']['candidates'][0]['candidate_id']
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'search_coverage':{'scope':'restricted','constraints':[],'restricted_source_ids':[candidate],
                        'source_blocker_alternative':{'request_quote':prompt}},
                    'material_requirements':[{'requirement':'Verify two requested records or the explicitly accepted concrete source blocker',
                        'requirement_kind':'fact','evidence_subject_id':'selected_lookup_outcome','eligible_evidence_tool_names':['opaque_read']},
                        {'requirement':'Keep the source basis concise and directly relevant','requirement_kind':'constraint',
                         'evidence_subject_id':'source_basis','eligible_evidence_tool_names':['opaque_read']}]})
            if 'semantic completion reviewer' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text']);record=next(r for r in packet['tool_evidence'] if r['tool_name']=='opaque_read')
                return text({'disposition':'complete','unresolved_requirements':[],'retry_tool_names':[],'supported_findings':[],
                    'all_material_requirements_enumerated':True,'material_requirements':[
                        {'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[record['invocation_id']]}
                        for r in packet['material_requirement_contract']],
                    'search_coverage':{'outcome_kind':'accepted_blocker','conclusion':'limited','coverage_sufficient':False,'scope_matches_request':True,
                        'match_evidence_invocation_ids':[record['invocation_id']],'discovery_evidence_invocation_ids':[],
                        'candidates':[],'constraints':[]}})
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':None,'confidence':1,'embedded_media_extensions':[]})
            if 'relationship' in system and 'target_index' in system:return text({'relationship':'independent','effect':'none','confidence':1,'target_index':None})
            if 'Return JSON only for a single already-selected read-only tool' in system:return text({'use_direct':False,'arguments':{}})
            if 'request_tool_scope' in names and 'opaque_read' not in names:return tool_response('request_tool_scope',{'capabilities':['web'],'tool_names':['opaque_read']})
            if not reads and 'opaque_read' in names:return tool_response('opaque_read',{})
            return text('The reservation form could not load rate inventory. No rate records were displayed; two options and prices could not be established.')
    for key,val in {'NULLION_ENV_FILE':str(tmp_path/'none.env'),'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_CHAT_STARTUP_WARMUP':'0','NULLION_ACTIVITY_TRACE_ENABLED':'0'}.items():monkeypatch.setenv(key,val)
    monkeypatch.setattr(chat_store,'_store',None);runtime.model_client=Provider()
    cid='telegram:123' if cron_surface else 'web:accepted-blocker'
    if cron_surface:
        from nullion.crons import CronJob
        from tests.e2e.test_scheduled_report_completion import run_scheduled,telegram_boundary
        deliveries,send=telegram_boundary(monkeypatch)
        output=run_scheduled(CronJob(id='accepted-blocker',name='確認',task=prompt,schedule='0 0 1 1 *',
            delivery_channel='telegram',delivery_target='123'),runtime.model_client,runtime,registry,send)
        assert output['response_fulfilled'] is True and not output.get('report_failed',False),output
        assert len(deliveries)==1,deliveries
        text=deliveries[0]['text']
    else:
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=runtime.model_client),registry=registry)) as client:
            response=client.post('/api/chat',json={'text':'/chat '+prompt,'conversation_id':cid})
            assert response.status_code==200,response.text
            text=response.json()['text']
    events=runtime.store.list_recent_conversation_events(cid,event_type='conversation.completion_review')
    assert 'could not load rate inventory' in text and 'No rate records' in text,events
    assert len(reads)==1 and not events
