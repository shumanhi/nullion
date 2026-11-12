"""A batched source call proves separate subjects through exact row bindings."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product-boundary E2E')

@pytest.mark.parametrize('complete',[True,False])
@pytest.mark.parametrize('prompt',['Compare these three symbols in one CSV.','Compara estos tres símbolos en un CSV.','3銘柄を比較してCSVを一つ作成。'])
def test_batch_quote_rows_prove_distinct_subjects_without_repeating_lookup(tmp_path,monkeypatch,complete,prompt,invalid_first=False,invalid_plan=False):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,register_market_quote_tool,_build_file_write_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);fetches=[]
    symbols=['AAA','BBB','CCC']
    def quote(symbol,timeout):
        fetches.append(symbol)
        if not complete and symbol=='CCC':raise ConnectionError('CCC quote service did not return a price')
        return {'symbol':symbol,'price':10+symbols.index(symbol),'previous_close':9+symbols.index(symbol),
            'source_url':'https://example.test/quote/'+symbol,'regular_market_time':'2026-10-25T01:00:00Z'}
    register_market_quote_tool(registry,quote_fetcher=quote)
    registry.register(ToolSpec('file_write','Write CSV',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    root=artifact_root_for_principal('web:batch-proof');root.mkdir(parents=True,exist_ok=True);path=root/'quotes.csv'
    content='Symbol,Price\n'+''.join(symbol+','+str(10+i)+'\n' for i,symbol in enumerate(symbols) if complete or symbol!='CCC')
    class Provider:
        phase=0;reviews=0;plans=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name+str(self.phase),'name':name,'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                self.plans+=1
                planning_input=json.loads(kw['messages'][0]['content'][0]['text'])
                if invalid_plan and self.plans==2:
                    assert planning_input['runtime_requirement_validation_errors']==['invalid_requirement_fields']
                    assert planning_input['previous_requirement_contract_response']
                rows=[{'requirement':'Verify '+symbol+' price and source','requirement_kind':'comparison_side','evidence_subject_id':symbol,'eligible_evidence_tool_names':['market_quote']} for symbol in symbols]
                rows.append({'requirement':'Deliver the comparison CSV','requirement_kind':'deliverable','evidence_subject_id':'csv','eligible_evidence_tool_names':['file_write']})
                if invalid_plan and self.plans==1:
                    rows[1]['evidence_subject_id']=rows[0]['evidence_subject_id']
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':rows}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);self.proof_errors=data.get('runtime_coverage_validation_errors');records=[r for r in data['tool_evidence'] if r.get('output',{}).get('result_kind')=='structured_source_record']
                assert len(records)==(3 if complete else 2),data['tool_evidence']
                assert len({r['output']['source_record_binding']['parent_invocation_id'] for r in records})==1
                coverage=[]
                parent=next(row['invocation_id'] for row in data['tool_evidence']
                    if row['tool_name']=='market_quote' and row.get('output',{}).get('result_kind')!='structured_source_record')
                for requirement in data['material_requirement_contract']:
                    symbol=requirement['evidence_subject_id']
                    matches=([r for r in records if r['output']['record']['symbol']==symbol] if symbol!='csv' else [r for r in data['tool_evidence'] if r.get('output',{}).get('artifact_record_binding')])
                    assert len(matches)<=1
                    ids=[r['invocation_id'] for r in matches]
                    if invalid_first and self.reviews==1 and symbol!='csv':
                        ids=[matches[0]['output']['source_record_binding']['parent_invocation_id']]
                    coverage.append({'requirement_id':requirement['requirement_id'],'satisfied':bool(matches),'draft_covered':bool(matches),'evidence_invocation_ids':ids})
                if invalid_first and self.reviews==2:
                    assert data['runtime_coverage_validation_errors'],data
                return text(json.dumps({'disposition':'complete' if complete else 'blocked','all_material_requirements_enumerated':True,'material_requirements':coverage,
                    'unresolved_requirements':[] if complete else ['CCC quote unavailable'],
                    'source_presentation_valid':None if complete else False,
                    'supported_findings':[] if complete else [{'kind':'limitation',
                        'text':'CCC quote service did not return a price.',
                        'evidence_invocation_ids':[parent]}],
                    'user_facing_failure':None if complete else 'CCC quote service did not return a price. No complete comparison was delivered.'}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.csv','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['market_data','local_files'],'tool_names':['market_quote','file_write'],'required_tool_names':['market_quote','file_write'],'artifact_extensions':['.csv']})
            if self.phase==2:return tool('market_quote',{'symbols':symbols})
            if self.phase==3:return tool('file_write',{'path':str(path),'content':content})
            assert self.phase==4,'Completed batch research was repeated'
            return text('The comparison is attached.\nMEDIA:'+str(path))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:batch-proof'});assert response.status_code==200,response.text;data=response.json()
        assert not model.errors,model.errors
        assert sorted(fetches)==symbols and model.reviews==(2 if invalid_first else 1), str(getattr(model,'proof_errors',None))
        assert model.plans==(2 if invalid_plan else 1)
        if complete:
            assert len(data['artifacts'])==1,data
            assert client.get(data['artifacts'][0]['url']).content==content.encode()
        else:
            assert len(data['artifacts'])==1 and 'CCC quote service' in data['text'],repr({'response':data,'events':[{k:e.get(k) for k in ('event_type','assistant_reply','text','response_fulfilled','response_presentation','disposition','review_error','supported_finding_count')} for e in runtime.store.list_conversation_events('web:batch-proof')]})
            assert client.get(data['artifacts'][0]['url']).content==content.encode()
            assert data.get('response_fulfilled') is not True
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    turn=next(r for r in restored.store.list_conversation_events('web:batch-proof') if r.get('event_type')=='conversation.chat_turn')
    assert len([r for r in turn['tool_results'] if r['tool_name']=='market_quote'])==1
    reviews=[r for r in restored.store.list_conversation_events('web:batch-proof') if r.get('event_type')=='conversation.completion_review']
    assert reviews and reviews[-1]['disposition']==('complete' if complete else 'blocked'),reviews
    assert reviews[-1].get('review_error') is None,reviews[-1]


@pytest.mark.parametrize('prompt',['Compare the saved quote rows in one CSV.','Compara las cotizaciones en un CSV.','相場を比較してCSVを一つ作成してください。'])
def test_invalid_review_binding_repairs_only_the_review_without_repeating_generation(tmp_path,monkeypatch,prompt):
    test_batch_quote_rows_prove_distinct_subjects_without_repeating_lookup(tmp_path,monkeypatch,True,prompt,invalid_first=True)


@pytest.mark.parametrize('prompt',['Compare the saved quote rows in one CSV.','Compara las cotizaciones en un CSV.','相場を比較してCSVを一つ作成してください。'])
def test_invalid_requirements_plan_repairs_without_repeating_sources_or_generation(tmp_path,monkeypatch,prompt):
    test_batch_quote_rows_prove_distinct_subjects_without_repeating_lookup(tmp_path,monkeypatch,True,prompt,invalid_plan=True)
