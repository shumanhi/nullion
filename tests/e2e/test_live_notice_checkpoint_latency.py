"""Visible live alerts commit their receipt without a full-runtime snapshot."""
import json,os,time,threading,statistics
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real Web/SQLite boundary')


def test_live_alert_ui_and_durable_receipt_skip_slow_full_snapshot(tmp_path,monkeypatch,unused_tcp_port):
    from nullion import web_app,chat_store,tools
    from nullion.runtime import PersistentRuntime,bootstrap_persistent_runtime
    from tests.e2e.test_web_surface_contracts import _UvicornThread
    from playwright.sync_api import sync_playwright,expect
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools.ToolRegistry();registry.live_tracker_store_path=tmp_path/'runtime.db';reads=[]
    def sensor(inv):reads.append(inv);return tools.ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':37})
    registry.register(tools.ToolSpec('sensor_read','Fixture external observation',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.READ,False,5,input_schema={'type':'object','properties':{}}),sensor)
    app=web_app.create_app(runtime,orchestrator=None,registry=registry)
    full_calls=[];samples=[];identities=[];original=PersistentRuntime.checkpoint
    def slow_full(self,**kwargs):
        if self is runtime and threading.current_thread().name=='live-notice':
            full_calls.append(True);time.sleep(1.5)
        return original(self,**kwargs)
    monkeypatch.setattr(PersistentRuntime,'checkpoint',slow_full)
    with _UvicornThread(app,unused_tcp_port) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        try:
            for i,label in enumerate(['Current reading','Lectura actual','現在の観測値']):
                cid='web:notice-'+str(i);context=browser.new_context();context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
                page=context.new_page();page.goto(server.base_url,wait_until='domcontentloaded');page.locator('#user-input').wait_for()
                started=time.monotonic();response=page.request.post(server.base_url+'/api/live-trackers',data={'name':label,'conversation_id':cid,'stop_after_match':True,'duration_seconds':90,'interval_seconds':5,'probe':[{'tool_name':'sensor_read','arguments':{}}],'matcher':{'kind':'structured','conditions':[{'step':0,'pointer':'/value','op':'greater_than','value':0,'label':label}],'summary':label+' 37'}})
                assert response.ok,response.text();job=response.json();identities.append((cid,job['id']))
                expect(page.locator('#messages')).to_contain_text(label+' 37',timeout=5000)
                deadline=started+4
                while True:
                    current=app.state.live_tracker_store.get(job['id'])
                    if current['notification_status']=='sent':break
                    assert time.monotonic()<deadline,current
                    page.wait_for_timeout(20)
                samples.append(time.monotonic()-started)
                assert current['state']=='matched' and current['checks']==1
                history=page.request.get(server.base_url+'/api/chat/history/'+cid).json()
                notices=[row for row in history['messages'] if row.get('metadata',{}).get('live_notice_id')==job['id']+':matched']
                assert len(notices)==1 and '37' in notices[0]['text'],history
                page.reload(wait_until='domcontentloaded');expect(page.locator('#messages')).to_contain_text(label+' 37')
                assert page.locator('#messages').inner_text().count(label+' 37')==1
                context.close()
        finally:browser.close()
    assert not full_calls,'Delivery waited for a full runtime snapshot'
    assert statistics.mean(samples)<2 and max(samples)<3.5,samples
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    events=[event for event in restored.store.events if event.event_type=='live_tracker.notification.sent']
    assert {event.payload['tracker_id'] for event in events}=={ident for cid,ident in identities}
    assert len(reads)==3
