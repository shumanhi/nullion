"""A real API request reads missing facts and delivers its same already-created file."""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
import json,os
from datetime import UTC,datetime
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker native routing/store/artifact boundary')

@pytest.mark.parametrize('narrow_before_review',[False,True])
@pytest.mark.parametrize('language',['en','es','ja'])
def test_blocked_missing_fact_retries_read_without_recreating_file(tmp_path,monkeypatch,language,narrow_before_review):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,tools
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.preferences import save_preferences
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);cid='web:missing-read';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);path=root/'report.txt';writes=[];searches=[]
    writer=tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):r=writer(inv);writes.append(r);return r
    registry.register(tools.ToolSpec('file_write','Write final file',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,10),write)
    def search(query,limit):
        searches.append(query);return [{'url':'https://example.org/reference','title':'Current reference','snippet':'The current value is 37.' if len(searches)>1 else 'Reference metadata is available, but the value was not captured.'}]
    monkeypatch.setenv('NULLION_PROVIDER_BINDINGS','search_plugin=brave_search_provider');monkeypatch.setenv('NULLION_BRAVE_SEARCH_API_KEY','fixture-not-real');monkeypatch.setattr('nullion.providers.resolve_plugin_provider_kwargs',lambda **kw:{'web_searcher':search})
    save_preferences({'outbound_request_mode':'allow_all'});runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-search',principal_id='global:operator',kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='https://www.bing.com/*',created_by='fixture',created_at=datetime.now(UTC)))
    class Provider:
        phase=0;reviews=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except Exception as e:self.errors.append(repr(e));raise
        def run(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'),'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Verify the current source value','requirement_kind':'fact','evidence_subject_id':'current','eligible_evidence_tool_names':['web_search']},{'requirement':'Deliver the report of the current value','requirement_kind':'deliverable','evidence_subject_id':'report','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);coverage=[]
                for row in data['material_requirement_contract']:
                    fact=row['requirement_kind']=='fact'
                    ids=([item['invocation_id'] for item in data['tool_evidence'] if item['tool_name']=='web_search' and '37' in json.dumps(item.get('output',{})) and 'https://example.org/reference' in json.dumps(item.get('output',{}))] if fact else [item['invocation_id'] for item in data['tool_evidence'] if item.get('output',{}).get('artifact_record_binding',{}).get('filename')=='report.txt'])
                    if fact and self.reviews==2:assert ids,'The fresh source record was not bound'
                    coverage.append({'requirement_id':row['requirement_id'],'satisfied':not fact or self.reviews>1,'draft_covered':True,'evidence_invocation_ids':ids if not fact or self.reviews>1 else []})
                return text(json.dumps({**fixture_source_assessment(data,conclusion='limited'),'disposition':'retry' if self.reviews==1 else 'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':['Read the current source value using a more specific query.'] if self.reviews==1 else [],'retry_tool_names':(['request_tool_scope'] if narrow_before_review else ['web_search']) if self.reviews==1 else []}))
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.txt','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web','local_files'],'tool_names':['web_search','file_write'],'required_tool_names':['web_search','file_write'],'artifact_extensions':['.txt']})
            if self.phase in [2,7 if narrow_before_review else 5]:return call('web_search',{'query':'current reference value' if self.phase==2 else 'current reference value with full detail'})
            if self.phase==3:return call('file_write',{'path':str(path),'content':'Current value: 37.\n'})
            if self.phase==4 and narrow_before_review:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],'required_tool_names':['file_write'],'artifact_extensions':['.txt']})
            if self.phase==6 and narrow_before_review:
                assert 'request_tool_scope' in names,'Missing source recovery hid scope selection'
                return call('request_tool_scope',{'capabilities':['web','local_files'],'tool_names':['web_search','file_write'],'required_tool_names':['web_search','file_write'],'artifact_extensions':['.txt']})
            assert self.phase in ([5,8] if narrow_before_review else [4,6]),'Read recovery recreated or restarted unrelated work'
            return text('Current value: 37. [Source](https://example.org/reference)\nMEDIA:'+str(path))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        prompt={'en':'Read the current source value and attach a text report.','es':'Lee el valor actual de la fuente y adjunta un informe de texto.','ja':'情報源の現在の値を読み取り、テキストの報告書を添付してください。'}[language]
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});data=response.json();assert not model.errors,model.errors
        assert response.status_code==200 and len(data['artifacts'])==1 and data['text'].strip(),(data,model.phase,model.reviews,[e for e in runtime.store.list_conversation_events(cid) if e.get('event_type') in {'conversation.tool_completed','conversation.completion_review','conversation.tool_scope_selected'}])
        assert client.get(data['artifacts'][0]['url']).content==b'Current value: 37.\n'
        if len(searches)!=2:
            from pathlib import Path
            target=Path('.e2e-artifacts/scheduled-budget-20261108/missing-source-review-events.json');target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(json.dumps({'phase':model.phase,'reviews':model.reviews,'events':runtime.store.list_conversation_events(cid)},default=str))
        assert len(searches)==2 and len(writes)==1 and model.reviews==2 and model.phase==(8 if narrow_before_review else 6)
    events=runtime.store.list_conversation_events(cid);reviews=[row for row in events if row.get('event_type')=='conversation.completion_review']
    assert [row['disposition'] for row in reviews]==['retry','complete']
    assert len(set(searches))==2, 'Recovery must change the lookup, not repeat the same source query'
