"""A linked URL retains native source search; ordinary turns stay cheap."""
from tests.workflow_provider_protocol import workflow_provider_reply
import base64,json,os,statistics,threading,time
from datetime import UTC,datetime
from concurrent.futures import ThreadPoolExecutor
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real chat route/store')

@pytest.fixture(scope='module')
def linked_research_latency_samples():
    samples=[]
    yield samples
    if samples:
        assert statistics.mean(samples)<1 and max(samples)<1.5,samples

@pytest.mark.parametrize('followup',['Use this source link.','Usa este enlace.','このリンクで確認してください。'])
def test_linked_image_research_can_use_declared_search_without_extra_plain_turn_calls(tmp_path,monkeypatch,followup,linked_research_latency_samples,record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult,_build_web_search_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    image=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jL1sAAAAASUVORK5CYII=')
    source=tmp_path/'reference.png';source.write_bytes(image)
    entered=threading.Event();release=threading.Event();reads=[];searches=[];browser_runs=[]
    registry=ToolRegistry()
    def read(invocation):
        reads.append(invocation.arguments)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'url':'https://venue.example/event','http_status':403,'next_tools':['browser_navigate']})
    registry.register(ToolSpec(name='browser_navigate',description='Read a source URL',risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=5,
        continuation_tools=('web_search','browser_run_task'),input_schema={'type':'object','properties':{'url':{'type':'string'}},'required':['url']}),read)
    def unnecessary_browser(invocation):
        browser_runs.append(1);time.sleep(2)
        raise AssertionError('Completed source research restarted a heavy browser task')
    registry.register(ToolSpec(name='browser_run_task',description='Optional multi-step browser source observation',
        risk_level=ToolRiskLevel.MEDIUM,side_effect_class=ToolSideEffectClass.WRITE,requires_approval=False,
        timeout_seconds=5),unnecessary_browser)
    def search(query,limit):
        searches.append(query);return [{'title':'Cedar musician','url':'https://artist.example/cedar','snippet':'Cedar performs house music.'}]
    # The real settings refresh owns search registration. Fake only its external
    # provider binding, with test-only credentials isolated under this HOME.
    monkeypatch.setenv('NULLION_PROVIDER_BINDINGS','search_plugin=brave_search_provider')
    monkeypatch.setenv('NULLION_BRAVE_SEARCH_API_KEY','local-fixture-not-a-real-key')
    monkeypatch.setattr('nullion.providers.resolve_plugin_provider_kwargs',lambda **kwargs:{'web_searcher':search})
    def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
    def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Provider:
        main_calls=0;plain=False;plain_calls=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            if self.plain:self.plain_calls+=1;return text('4')
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if 'relationship' in system:
                release.set();return text(json.dumps({'relationship':'follow_up','effect':'revise','confidence':1,'target_index':0,'target_is_explicit':True}))
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Research the source subject','requirement_kind':'fact','evidence_subject_id':'cedar',
                     'eligible_evidence_tool_names':['web_search']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                p=json.loads(kw['messages'][0]['content'][0]['text']);ids=[r['invocation_id'] for r in p['tool_evidence'] if r['tool_name']=='web_search'];assert ids
                return text(json.dumps({'disposition':'complete','source_presentation_valid':True,'all_material_requirements_enumerated':True,
                    'unresolved_requirements':[],'material_requirements':[{**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids[:1]} for r in p['material_requirement_contract']]}))
            if not names:
                if 'web_action' in system:
                    payload=json.loads(kw['messages'][0]['content'][0]['text'])
                    if 'https://venue.example/event' in payload['user_turn']:
                        assert payload['linked_request_user_messages']==['Research this source subject.'],payload
                    return text(json.dumps({'web_action':'live_research','requested_tool_names':['browser_navigate'],
                        'required_tool_names':['browser_navigate'],'confidence':1}))
                return text('{}')
            self.main_calls+=1
            media=[b for m in kw['messages'] for b in m.get('content',[]) if isinstance(b,dict) and b.get('type')=='image']
            assert media and any(base64.b64decode(b['source']['data'])==image for b in media)
            if self.main_calls==1:
                entered.set();assert release.wait(10);return text('Superseded draft')
            assert 'web_search' in names,names
            if self.main_calls==2:return tool('browser_navigate',{'url':'https://venue.example/event'})
            if self.main_calls==3:return tool('web_search',{'query':'Cedar musician','limit':3})
            assert self.main_calls==4
            return text('Cedar performs house music. [Artist source](https://artist.example/cedar)')
    from nullion.preferences import save_preferences
    save_preferences({'outbound_request_mode':'allow_all'})
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-search-consent',principal_id='global:operator',
        kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='https://www.bing.com/*',created_by='fixture',created_at=datetime.now(UTC)))
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        with ThreadPoolExecutor(max_workers=2) as pool:
            original=pool.submit(client.post,'/api/chat',json={'text':'Research this source subject.','conversation_id':'web:scope','turn_id':'first',
                'attachments':[{'path':str(source),'name':source.name,'media_type':'image/png'}]})
            assert entered.wait(10)
            started=time.perf_counter()
            try: amended=client.post('/api/chat',json={'text':followup+' https://venue.example/event','conversation_id':'web:scope','turn_id':'second'})
            finally:release.set()
            elapsed=time.perf_counter()-started;linked_research_latency_samples.append(elapsed)
            record_property('linked_source_research_seconds',elapsed)
            initial=original.result(timeout=10)
        assert initial.json().get('superseded') is True,initial.text
        assert amended.status_code==200 and 'https://artist.example/cedar' in amended.json()['text'],amended.text
        assert len(reads)==1 and searches==['Cedar musician'] and not browser_runs
        rows=runtime.store.list_recent_conversation_events('web:scope',event_type='conversation.chat_turn',limit=2)
        final=next(row for row in rows if row['turn_id']=='second')
        assert [(r['tool_name'],r['status']) for r in final['tool_results']]==[('browser_navigate','completed'),('web_search','completed')]
        timings=runtime.store.list_recent_conversation_events('web:scope',event_type='conversation.model_timing',limit=10)
        assert any('web_search' in row.get('tool_names',[]) for row in timings)
        model.plain=True;samples=[]
        for index,prompt in enumerate(['2+2','Calcula 2+2','2と2の和']):
            started=time.perf_counter();reply=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:plain:'+str(index)})
            samples.append(time.perf_counter()-started)
            assert reply.status_code==200 and reply.json()['text']=='4',reply.text
        assert model.plain_calls==3 and statistics.mean(samples)<1 and max(samples)<1.5,(model.plain_calls,samples)
