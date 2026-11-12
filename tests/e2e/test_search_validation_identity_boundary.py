"""Search identity through the real Web/cron, graph, persistence and delivery.

Only external model/source/platform boundaries are scripted. Discovery URLs and
actual observed query URLs deliberately differ, matching the reported failure.
"""
import asyncio
import json
import os

import httpx
import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response, run_scheduled
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')
BASE='https://catalog.example/対象'
ALT='https://other.example/対象'
FILTERED=BASE+'?day=2030-01-01&format=large'
BOOK='https://booking.example/slots?day=2030-01-01&show=7'


@pytest.mark.parametrize('surface', ['web','cron'])
@pytest.mark.parametrize('mode', ['booking','collection','multi_page','protocol_error','wrong_filter','negative'])
@pytest.mark.parametrize('prompt,description', [
    ('条件に合う対象を見つけてください。','指定された日の対象形式。'),
    ('Busca una opción que cumpla las condiciones.','La fecha y el formato solicitados.'),
])
def test_search_identity_preserves_supported_outcome_without_unnecessary_research(
        tmp_path,monkeypatch,surface,mode,prompt,description):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion import chat_store, web_app
    runtime=setup_runtime(tmp_path,monkeypatch)
    for key,value in {'NULLION_ENV_FILE':str(tmp_path/'absent.env'),'NULLION_MEMORY_ENABLED':'false',
        'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_ACTIVITY_TRACE_ENABLED':'false',
        'NULLION_CHAT_STARTUP_WARMUP':'0'}.items():monkeypatch.setenv(key,value)
    monkeypatch.setattr(chat_store,'_store',None)
    reads=[];discoveries=[];wire=[]
    def source(request):
        url=str(request.url)
        if request.url.host=='discovery.example':
            return httpx.Response(200,json={'source_candidates':[{'url':BASE,'title':'対象'},
                {'url':ALT,'title':'別形式'}]})
        if mode=='negative':
            return httpx.Response(200,json={'source_url':url,'canonical_source_url':BASE if request.url.host=='catalog.example' else ALT,
                'text':'この条件に一致する結果は0件です。','count':0})
        if mode=='collection':
            return httpx.Response(200,json={'result_kind':'browser_item_collection','page':{'url':url},
                'items':[{'canonical_url':BOOK,'title':'Verified option — 82 seats available','available':82}]})
        if request.url.host=='catalog.example':
            return httpx.Response(200,json={'source_url':url,'text':'Matching option: show 7. Open its booking page for current seat availability.'})
        if mode=='multi_page':
            return httpx.Response(200,json={'result_kind':'source_observations','page_url':'https://footer.example/info',
                'report_status':'completed','goal_status':'answered','next_action':'report','unknowns':[],
                'observations':[{'source_url':url,'label':'Current seats','value':'82 seats available for the requested day and format.',
                    'quote':'82 seats available for the requested day and format.','semantic_role':'entity','captured_at':1}]})
        return httpx.Response(200,json={'source_url':url,'text':'82 seats available for the requested day and format.',
            'count':82,'read_number':len(reads)})
    client=httpx.Client(transport=httpx.MockTransport(source))
    registry=ToolRegistry()
    def discover(inv):
        discoveries.append(inv.invocation_id)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',client.get('https://discovery.example/').json())
    def read(inv):
        url=inv.arguments['url'];reads.append(url)
        output=client.get(url).json()
        if mode=='multi_page':
            from nullion.source_observation_contract import compact_source_observations
            output=compact_source_observations(output) or output
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',output)
    registry.register(ToolSpec('opaque_discover','Discover source URLs',ToolRiskLevel.LOW,ToolSideEffectClass.READ,
        False,2,capability_tags=('web',)),discover)
    registry.register(ToolSpec('opaque_read','Read exact selected source URL',ToolRiskLevel.LOW,ToolSideEffectClass.READ,
        False,2,input_schema={'type':'object','properties':{'url':{'type':'string'}},'required':['url']},
        capability_tags=('web',)),read)
    class Model:
        reviews=0
        plans=0
        def create(self,**kw):
            system=kw.get('system') or '';names={t['name'] for t in kw.get('tools') or ()}
            if 'present_source_answer' in names:
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                message=kw['messages'][0]['content'][0]['text']
                raw=message.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                context=json.loads(raw); row=context['evidence'][-1]
                return tool_response('present_source_answer',{'intro':'','focus_roles':['entity'],
                    'summary_point_indices':[],'points':[{'semantic_role':'entity','evidence_ids':[row['id']],
                        'title_ids':[row['excerpts'][0]['id']],'detail_ids':[], 'relevance':'Matches the requested option.'}],
                    'limitation_ids':[]})
            if 'material-requirements planner' in system:
                self.plans+=1
                return text_response(json.dumps({'all_material_requirements_enumerated':True,
                    'requested_artifact_extensions':[], 'search_coverage':{'scope':'evidence_summary' if mode=='inconsistent_summary' else 'find_any',
                        'constraints':['Requested day and format'],'restricted_source_ids':[]},
                    'material_requirements':[{'requirement':'Find a qualifying option or establish scoped absence.',
                        'requirement_kind':'fact','evidence_subject_id':'selected-option','eligible_evidence_tool_names':['opaque_read']}]}))
            if 'semantic completion reviewer' in system:
                self.reviews+=1
                packet=json.loads(kw['messages'][0]['content'][0]['text']);ctx=packet['search_coverage_context']
                proof=[r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']=='opaque_read' and r['status']=='completed']
                selected=proof[-1];definition=ctx['constraint_definitions'][0]
                constraint={'constraint_id':definition['constraint_id'],'constraint':description,
                    'verified':mode!='wrong_filter' or self.reviews>1,'evidence_invocation_ids':[selected]}
                if mode=='protocol_error':constraint.pop('constraint_id')
                candidates=[{'candidate_id':r['candidate_id'],'disposition':'checked' if r['observation_invocation_ids'] else 'pending',
                    'reason':'Current source receipt' if r['observation_invocation_ids'] else 'Alternative source',
                    'evidence_invocation_ids':r['observation_invocation_ids'] or r['discovery_invocation_ids']}
                    for r in ctx['candidates']]
                if mode!='negative':
                    # Deliberately reproduce the optional base-URL association error.
                    base=next(r for r in candidates if next(c for c in ctx['candidates'] if c['candidate_id']==r['candidate_id'])['url']==BASE)
                    base.update(disposition='checked',evidence_invocation_ids=[selected])
                return text_response(json.dumps({'disposition':'complete','unresolved_requirements':[],
                    'retry_tool_names':[],'all_material_requirements_enumerated':True,
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,
                        'evidence_invocation_ids':[selected]} for r in packet['material_requirement_contract']],
                    'search_coverage':{'conclusion':'negative' if mode=='negative' else 'limited' if mode=='inconsistent_summary' else 'positive',
                        'coverage_sufficient':mode!='inconsistent_summary','scope_matches_request':True,
                        'discovery_evidence_invocation_ids':ctx['discovery_invocation_ids'],
                        'match_evidence_invocation_ids':[] if mode=='negative' else [selected],
                        'constraints':[constraint],'candidates':candidates}}))
            if system.startswith('You repair scheduled-task final answers'):
                packet=kw['messages'][0]['content'][0]['text']
                proof=json.JSONDecoder().raw_decode(packet.split('Verified compact tool evidence from this run:\n',1)[1])[0]
                return text_response(json.dumps({'requirements_satisfied':True,'source_state':'available',
                    'evidence_refs':[r['evidence_id'] for r in proof if r['status']=='completed'][-1:],
                    'report':'82 seats available. Booking: '+BOOK}))
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format',{'extension':None,'confidence':1,'embedded_media_extensions':[]})
            if 'request_tool_scope' in names and 'opaque_discover' not in names:
                return tool_response('request_tool_scope',{'capabilities':['web'],'tool_names':['opaque_discover','opaque_read']})
            if not names:return text_response('{}')
            if not discoveries:return tool_response('opaque_discover',{})
            if not reads:return tool_response('opaque_read',{'url':FILTERED})
            if mode not in {'collection','negative'} and BOOK not in reads:return tool_response('opaque_read',{'url':BOOK})
            if self.reviews and mode=='negative' and ALT not in reads:return tool_response('opaque_read',{'url':ALT})
            if self.reviews and mode=='wrong_filter' and reads.count(BOOK)<2:return tool_response('opaque_read',{'url':BOOK})
            return text_response('No qualifying options in the checked sources.' if mode=='negative' else '82 seats available. Booking: '+BOOK)
    model=Model()
    if surface=='web':
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as api:
            response=api.post('/api/chat',json={'text':'/chat '+prompt,'conversation_id':'web:identity'})
            assert response.status_code==200,response.text
            data=response.json();reply=data['text']
        conversation='web:identity'
    else:
        from nullion.crons import CronJob
        from nullion.telegram_entrypoint import _send_operator_telegram_delivery
        from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
        wire=TelegramWire(); install_wire(monkeypatch,wire)
        def send(job,channel,target,text):
            assert channel=='telegram'
            return asyncio.run(_send_operator_telegram_delivery(TOKEN,target,text,message_only=True))
        data=run_scheduled(CronJob(id='identity',name='確認',task=prompt,schedule='* * * * *',
            delivery_channel='telegram',delivery_target='123'),model,runtime,registry,send)
        assert len(wire.texts)==1,wire.texts
        reply=wire.texts[0];conversation='telegram:123'

    client.close()
    events=runtime.store.list_recent_conversation_events(conversation,event_type='conversation.completion_review')
    assert len(discoveries)==1
    if mode in {'protocol_error','inconsistent_summary'}:
        assert '82' in reply and 'Some requested work remains incomplete' not in reply, ({k:v for k,v in data.items() if isinstance(v,(str,bool,int,float,type(None)))}, events[-1])
        assert events[-1]['review_error']==('invalid_search_coverage_assessment' if mode=='protocol_error' else 'inconsistent_search_coverage_assessment')
        assert data.get('response_fulfilled') is not True
        assert not events[-1]['runtime_failure_code']
        assert len(reads)==2 and model.reviews==1
    else:
        assert events[-1]['disposition']=='complete' and not events[-1]['search_coverage_errors'],events
        assert not events[-1]['runtime_failure_code']
        assert 'Some requested work remains incomplete' not in reply
        if mode=='negative':
            assert reads==[FILTERED,ALT] and model.reviews==2
        elif mode=='wrong_filter':
            assert reads==[FILTERED,BOOK,BOOK] and model.reviews==2
        else:
            assert '82' in reply and 'Some requested work remains incomplete' not in reply, ({k:v for k,v in data.items() if isinstance(v,(str,bool,int,float,type(None)))}, events[-1])
            # The typed collection already supports the final answer. Keeping
            # that answer through publication avoids a second cron repair/review.
            assert len(reads)==(1 if mode=='collection' else 2) and model.reviews==1
            assert events[-1]['search_coverage_diagnostics']==['unobserved_candidate']
        if surface=='cron': assert data.get('cron_delivery_status')=='sent',data


@pytest.mark.parametrize('surface',['web','cron'])
def test_inconsistent_bounded_summary_review_does_not_start_new_research(tmp_path,monkeypatch,surface):
    test_search_identity_preserves_supported_outcome_without_unnecessary_research(
        tmp_path,monkeypatch,surface,'inconsistent_summary','取得済みの情報を要約してください。','指定された資料。')
