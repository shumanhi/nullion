"""Default Web admission, repeated real reads, durable outbox and expiry renewal."""
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
from urllib.request import urlopen

import pytest
from tests.e2e.test_tracker_status_delivery import setup

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def test_continuous_snapshot_sends_each_transition_once_and_renews_only_at_expiry(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.live_tracking import LiveTrackerEngine, utcnow
    from nullion.tools import ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    state={'value':10}; packets=[]
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.end_headers();self.wfile.write(json.dumps(state).encode())
        def log_message(self,*a):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    class Model:
        def create(self,**kw):
            p=json.loads(kw['messages'][0]['content'][0]['text']);packets.append(p)
            h=p['snapshot_history'];value=p['observations'][0]['value']
            decision={'status':'no_match'}
            if h['baseline_established'] and value != h['previous_observations'][0]['value']:
                decision={'status':'matched','summary':'更新', 'sections':[{'heading':'結果','items':[str(value)]}],
                    'evidence':[{'step':0,'pointer':'/observations/0/value','kind':'value','value':value}]}
            return {'content':[{'type':'text','text':json.dumps(decision)}]}
    app,runtime,db=setup(tmp_path,monkeypatch,provider=Model());registry=app.state.live_tracker_engine.registry
    def read(inv):
        with urlopen(f'http://127.0.0.1:{server.server_port}/',timeout=2) as response:value=json.load(response)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',value)
    registry.register(ToolSpec('state_read','Public counter',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    try:
        with TestClient(app) as api:
            app.state.live_tracker_engine.stop();store=app.state.live_tracker_store
            response=api.post('/api/live-trackers',json={'name':'変化の追跡','conversation_id':'web:continuous',
                'duration_seconds':3600,'interval_seconds':5,'probe':[{'tool_name':'state_read','arguments':{}}],
                'matcher':{'kind':'semantic','history_mode':'snapshot','instruction':'前の観測から変わったら通知'}})
            assert response.status_code==200,response.text
            job=response.json();assert job['stop_after_match'] is False
            assert job['deferred_execution']['configuration']['stop_after_match'] is False
            ident=job['id'];original_end=job['expires_at']
            for value in [10,11,11,12,10,11]:
                state['value']=value
                engine=LiveTrackerEngine(store,registry=registry,runtime_store=runtime.store,
                    model_client=Model(),deliver=app.state.live_tracker_engine.deliver)
                current,token=store.claim(4,now=utcnow()+timedelta(seconds=10));engine.check_once(current,token)
                saved=store.get(ident)
                assert saved['state']=='active' and saved['expires_at']==original_end
                assert saved['comparison_snapshot']['outputs'][0]['value']==value
            assert [p['snapshot_history']['previous_observations'][0]['value'] for p in packets[1:]]==[10,11,11,12,10]
            assert all('previous_matches' not in p for p in packets)
            notices=[]
            while notice:=store.claim_notice():
                assert 'Continue tracking' not in notice['text'] and 'Tracking continues until' in notice['text']
                engine._send(notice);notices.append(notice)
            assert len(notices)==4 and len({n['id'] for n in notices})==4
            history=api.get('/api/chat/history/web:continuous').json()['messages']
            assert len([m for m in history if (m.get('metadata') or {}).get('source')=='live_tracker'])==4
            with store.connection() as connection:
                assert connection.execute("select count(*) from live_tracker_choices where tracker_id=? and kind='matched'",(ident,)).fetchone()[0]==0
                assert connection.execute("select count(*) from live_tracker_notifications where tracker_id=? and state='sent'",(ident,)).fetchone()[0]==4
            end=__import__('datetime').datetime.fromisoformat(original_end)
            store.expire(now=end+timedelta(seconds=1));expired=store.get(ident)
            assert expired['state']=='expired'
            offer=store.extension_offer(expired['extension_offer_id'])
            assert offer['kind']=='expired' and 'Matching updates were found' in offer['text']
            # Deliver the real expiry menu and use its numeric chat reply,
            # including the surface's renewal copy and durable consent path.
            monkeypatch.setattr('nullion.live_tracking.utcnow',lambda:end+timedelta(seconds=2))
            expiry_notice=store.claim_notice();assert expiry_notice['kind']=='expired'
            engine._send(expiry_notice)
            reply=api.post('/api/chat',json={'text':'1','conversation_id':'web:continuous'})
            assert reply.status_code==200,reply.text
            assert 'keep checking' in reply.json()['text'] and 'first match' not in reply.json()['text']
            renewed=store.get(ident)
            assert renewed['state']=='active' and not renewed['stop_after_match']
            assert renewed['window_match_count']==0 and renewed['comparison_snapshot']['outputs'][0]['value']==11
            store.stop(ident,'workspace_admin')
            assert store.claim(4,now=end+timedelta(seconds=3)) is None
    finally:server.shutdown();server.server_close()
