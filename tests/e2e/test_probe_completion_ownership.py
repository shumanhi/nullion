"""A prepared read is not terminal proof of the request owning that probe."""
from tests.workflow_provider_protocol import workflow_provider_reply
import os,json,time,statistics
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container product boundary')

@pytest.mark.parametrize('direct',[False,True])
@pytest.mark.parametrize('language',['en','es','ja'])
def test_probe_continues_to_native_tracker_but_explicit_answer_stays_fast(tmp_path,monkeypatch,direct,language):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.live_tracking import LiveTrackerStore
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools.ToolRegistry();registry.live_tracker_store_path=tmp_path/'runtime.db';reads=[]
    args={'location_text':'London','forecast_days':1}
    def weather(inv):
        reads.append(inv);return tools.ToolResult(inv.invocation_id,inv.tool_name,'completed',{'location':{'name':'London'},'current':{'temperature_2m':12,'apparent_temperature':10,'weather_code':0},'current_units':{'temperature_2m':'°C'}})
    registry.register(tools.ToolSpec('weather_forecast','Read weather',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.READ,False,10,capability_tags=('weather',),input_schema={'type':'object','properties':{'location_text':{'type':'string'},'forecast_days':{'type':'integer'}},'required':['location_text']}),weather)
    register_live_tracker_tools(registry)
    class Provider:
        phase=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def run(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if not names:return {'stop_reason':'end_turn','content':[{'type':'text','text':'{}'}]}
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['weather'],'tool_names':['weather_forecast'],'required_tool_names':['weather_forecast'],'direct_read_complete_request':direct})
            if self.phase==2:return call('weather_forecast',args)
            assert not direct,'An explicit answer-only scope must not add a completion call'
            if self.phase==3:return call('request_tool_scope',{'capabilities':['live_tracking','weather'],'tool_names':['create_live_tracker','weather_forecast'],'required_tool_names':['create_live_tracker'],'tracking_mode':'live'})
            if self.phase==4:return call('weather_forecast',args)
            if self.phase==5:return call('create_live_tracker',{'name':'Owned observation','duration_seconds':90,'interval_seconds':5,'probe':[{'tool_name':'weather_forecast','arguments':args}],'matcher':{'kind':'structured','conditions':[{'step':0,'pointer':'/current/temperature_2m','op':'greater_than','value':1000}],'summary':'Temperature above threshold'}})
            assert self.phase==6
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'The live tracker is active.'}]}
    normal={'en':'What is the current London temperature?','es':'¿Cuál es la temperatura actual en Londres?','ja':'ロンドンの現在の気温は？'}
    monitor={'en':'Watch the London temperature for 90 seconds, checking every 5 seconds. Alert only above 1000°C.','es':'Observa la temperatura de Londres durante 90 segundos, cada 5 segundos. Avisa solo por encima de 1000°C.','ja':'ロンドンの気温を90秒間、5秒ごとに監視してください。1000°Cを超えたら通知してください。'}
    model=Provider();runtime.model_client=model;samples=[]
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        started=time.monotonic();response=client.post('/api/chat',json={'text':(normal if direct else monitor)[language],'conversation_id':'web:probe-owner'});samples.append(time.monotonic()-started)
        assert response.status_code==200,response.text
        data=response.json();assert not model.errors,(model.errors,[(r.get('tool_name'),r.get('status'),r.get('error'),r.get('output')) for t in runtime.store.list_conversation_events('web:probe-owner') if t.get('event_type')=='conversation.chat_turn' for r in t.get('tool_results',[])])
        jobs=LiveTrackerStore(tmp_path/'runtime.db').list('workspace_admin',include_finished=True)
        if direct:assert not jobs and '12' in data['text'] and model.phase==2,data
        else:
            assert len(jobs)==1 and jobs[0]['state']=='active',data
            assert jobs[0]['probe']==[{'tool_name':'weather_forecast','arguments':args}]
            assert jobs[0]['conversation_id']=='web:probe-owner' and jobs[0]['interval_seconds']==5
            assert 'Live tracker' in data['text'] or 'live tracker' in data['text'],data
            client.post('/api/live-trackers/'+jobs[0]['id']+'/stop',json={})
        ordinary=[inv for inv in reads if not str(inv.capsule_id or '').startswith('live-tracker:')]
        assert len(ordinary)==(1 if direct else 2),reads
        assert len(reads)-len(ordinary)<=1,reads
    assert statistics.mean(samples)<3 and max(samples)<4,samples
