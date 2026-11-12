"""Native tracker setup after source discovery is not a future-results search."""
import json,os,time
import pytest
from tests.e2e.test_live_tracker_workflow import live_fixture,text
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E')


@pytest.mark.parametrize('prompt,surface,mode',[
    ('観測を準備して有限の監視を開始してください。','browser','setup'),
    ('Prepara la fuente y activa el seguimiento temporal.','api','setup'),
    ('Start the temporary monitor and report the current availability.','api','mixed'),
    ('起動と現在の観測値をお願いします。','api','mixed_missing_coverage')])
def test_discovered_source_does_not_block_accepted_setup_but_mixed_lookup_keeps_coverage(live_fixture,prompt,surface,mode):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    mixed=mode!='setup'
    f=live_fixture;f.source.extra={'result_kind':'browser_item_collection','page_url':f.url,
        'items':[{'title':'Observed baseline','canonical_url':f.url+'/record'}]}
    class Model(type(f.model)):
        plans=0;reviews=0;errors=[];total=0
        def create(self,**kw):
            self.total+=1
            try:return self.run(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def run(self,**kw):
            system=str(kw.get('system') or '')
            if self.mode=='plain':return text('Hello.')
            if 'material-requirements planner' in system:
                self.plans+=1;packet=json.loads(kw['messages'][0]['content'][0]['text'])
                assert packet['accepted_background_execution'] and packet['discovered_sources']
                rows=[{'requirement':'Accept the finite monitor configuration','requirement_kind':'action',
                    'evidence_subject_id':'setup','eligible_evidence_tool_names':['create_live_tracker','observation_read']},
                    {'requirement':'Prepare the observation source','requirement_kind':'action',
                    'evidence_subject_id':'prep','eligible_evidence_tool_names':['observation_read']}]
                if mixed:rows.append({'requirement':'Report current availability','requirement_kind':'fact',
                    'evidence_subject_id':'current','eligible_evidence_tool_names':['observation_read']})
                return text(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':rows,'search_coverage':{'scope':'evidence_summary','constraints':[],
                    'restricted_source_ids':[]} if mode=='mixed' else None}))
            if 'semantic completion reviewer' in system:
                self.reviews+=1;packet=json.loads(kw['messages'][0]['content'][0]['text'])
                assert bool(packet.get('search_coverage_context')) is mixed
                reply=workflow_provider_reply(kw)
                if mixed:
                    value=json.loads(reply['content'][0]['text'])
                    value['search_coverage']['conclusion']='limited'
                    value['search_coverage']['match_evidence_invocation_ids']=[r['invocation_id']
                        for r in packet['citable_evidence'] if r['tool_name']=='observation_read']
                    return text(json.dumps(value))
                return reply
            if self.created and kw.get('tools') and 'Workflow action preflight.' not in system:
                return text('Live tracker is active. Current availability is false.' if mixed else 'Live tracker is active.')
            return super().create(**kw)
    model=Model();f.runtime.model_client=model;app=create_app(f.runtime,AgentOrchestrator(model_client=model),f.registry)
    cid='web:setup-coverage'
    if surface=='browser':
        from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
        from playwright.sync_api import expect
        with _UvicornThread(app,_free_port()) as server:
            playwright,browser,page,errors=_open_page(server.base_url)
            try:
                page.locator('#user-input').fill(prompt);page.get_by_role('button',name='Send',exact=True).click()
                expect(page.locator('#messages')).to_contain_text('Live tracker',timeout=20000)
                assert not errors and not model.errors,(errors,model.errors)
            finally:browser.close();playwright.stop()
    else:
        with TestClient(app) as client:
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
            assert result.status_code==200,(result.text,model.errors)
    turn=next(r for r in reversed(f.runtime.store.list_conversation_events()) if r.get('event_type')=='conversation.chat_turn')
    if mode=='mixed_missing_coverage':
        assert turn.get('response_fulfilled') is not True,(turn,model.errors)
        assert model.plans==2 and model.reviews==0
        plans=[e for e in f.runtime.store.list_conversation_events() if e.get('event_type')=='conversation.completion_requirement_contract']
        assert len(plans)==2 and all('invalid_search_coverage_plan' in e['validation_errors'] for e in plans),plans
    else:
        assert turn.get('response_fulfilled') is True,(turn['assistant_reply'],model.errors,
            [{k:e.get(k) for k in ['review_error','unresolved_requirements','search_coverage_errors','response_diagnostics']}
             for e in f.runtime.store.list_conversation_events() if e.get('event_type')=='conversation.completion_review'])
        assert model.plans==model.reviews==1
    assert [r['tool_name'] for r in turn['tool_results']]==['request_tool_scope','observation_read','create_live_tracker']
    # A separate ordinary turn retains the no-classifier/no-review fast path.
    model.mode='plain'
    with TestClient(app) as client:
        before=model.total;started=time.perf_counter()
        result=client.post('/api/chat',json={'text':'Hola','conversation_id':'web:plain-setup-coverage'})
        assert result.json()['text']=='Hello.' and model.total==before+1
        assert time.perf_counter()-started<.9
