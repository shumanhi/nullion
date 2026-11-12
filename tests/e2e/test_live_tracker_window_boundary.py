"""Real Web routes, tracker tools and SQLite; a controlled read source only."""
from datetime import datetime, timedelta, timezone
import os

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('name',['長期の観測','Seguimiento temporal','مراقبة مؤقتة'])
def test_web_tracker_accepts_long_window_and_amends_same_identity(tmp_path,monkeypatch,name):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'absent.env'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=ToolRegistry();reads=[]
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':0})
    registry.register(ToolSpec('owned_read','Read controlled source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    registry.live_tracker_store_path=tmp_path/'runtime.db';register_live_tracker_tools(registry)
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=None),registry=registry)
    cid='web:window-contract'
    source={'probe':[{'tool_name':'owned_read','arguments':{}}],
        'matcher':{'kind':'structured','conditions':[{'step':0,'pointer':'/value','op':'equals','value':100}], 'summary':'Condition met'}}
    with TestClient(app) as client:
        # Exercise timing changes independently of the poll loop, using its real
        # lifecycle stop before creating any fixture. Polling has separate E2Es.
        app.state.live_tracker_engine.stop()
        missing=client.post('/api/live-trackers',json={'name':name,'conversation_id':cid,**source})
        assert missing.status_code==400 and not app.state.live_tracker_store.list('workspace_admin')
        first=client.post('/api/live-trackers',json={'name':name,'conversation_id':cid,'duration_seconds':14*86400,
            'interval_seconds':900,**source},headers={'Idempotency-Key':'first'})
        assert first.status_code==200,first.text
        created=first.json();ident=created['id'];before=app.state.live_tracker_store.get(ident)
        config=created['deferred_execution']['configuration']
        assert datetime.fromisoformat(config['expires_at'])-datetime.fromisoformat(config['accepted_at'])==timedelta(days=14)
        deadline=(datetime.now(timezone.utc)+timedelta(days=60)).isoformat()
        changed=client.patch('/api/live-trackers/'+ident,json={'conversation_id':cid,'expires_at':deadline,'interval_seconds':300},
                             headers={'Idempotency-Key':'amend'})
        assert changed.status_code==200,changed.text
        assert changed.json()['id']==ident and changed.json()['expires_at']==deadline
        assert changed.json()['deferred_execution']['configuration']['deadline_kind']=='absolute'
        repeated=client.patch('/api/live-trackers/'+ident,json={'conversation_id':cid,'expires_at':deadline,'interval_seconds':300},
                              headers={'Idempotency-Key':'amend'})
        assert repeated.status_code==200 and repeated.json()['expires_at']==deadline
        relative=client.patch('/api/live-trackers/'+ident,json={'conversation_id':cid,'duration_seconds':10800},
                             headers={'Idempotency-Key':'three-hours'})
        assert relative.status_code==200,relative.text
        config=relative.json()['deferred_execution']['configuration']
        assert datetime.fromisoformat(config['expires_at'])-datetime.fromisoformat(config['accepted_at'])==timedelta(hours=3)
        after=app.state.live_tracker_store.get(ident)
        assert after['require_start_receipt'] is False, 'An API receipt must not wait for a nonexistent chat acknowledgement'
        for key in ('id','probe','matcher','baseline','checks','seen_observation_keys'):
            assert after.get(key)==before.get(key)
        assert len(app.state.live_tracker_store.list('workspace_admin'))==1 and not reads
        policy=client.patch('/api/live-trackers/'+ident,json={'conversation_id':cid,'stop_after_match':True},
                            headers={'Idempotency-Key':'one-shot-policy'})
        assert policy.status_code==200 and policy.json()['stop_after_match'] is True
        continuous=client.patch('/api/live-trackers/'+ident,json={'conversation_id':cid,'stop_after_match':False},
                                headers={'Idempotency-Key':'continuous-policy'})
        assert continuous.status_code==200 and continuous.json()['stop_after_match'] is False
        assert continuous.json()['expires_at']==after['expires_at'] and not reads
        denied=client.patch('/api/live-trackers/'+ident,json={'conversation_id':'web:foreign','duration_seconds':3600})
        assert denied.status_code==403 and app.state.live_tracker_store.get(ident)['expires_at']==after['expires_at']
        metadata=client.get('/api/live-trackers').json()
        assert metadata['max_duration_seconds'] is None and metadata['deadline_required'] is True
        assert client.post('/api/live-trackers/'+ident+'/stop').status_code==200
