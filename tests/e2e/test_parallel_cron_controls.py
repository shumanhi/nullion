"""Real scheduler, agent runner, SQLite, delivery and UI; only provider is fake."""
import os,time,threading,json
from datetime import datetime,timedelta,timezone
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='container E2E only')

def test_parallel_runs_deliver_and_plain_chat_stays_fast(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion import crons
    from nullion.chat_store import get_chat_store
    from test_tracker_status_delivery import setup
    class Provider:
        def __init__(self):self.entered=[];self.gate=threading.Event();self.lock=threading.Lock();self.local=threading.local()
        def create(self,**kw):
            if threading.current_thread().name.startswith('nullion-cron-') and not getattr(self.local,'seen',False):
                self.local.seen=True
                with self.lock:self.entered.append(time.monotonic())
                assert self.gate.wait(5),'Second job never reached the provider concurrently'
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'42'}]}
    provider=Provider();app,runtime,db=setup(tmp_path,monkeypatch,provider)
    app.state.cron_scheduler.set_max_parallel_jobs(2)
    samples=[]
    with TestClient(app) as client:
        for repeat in range(3):
            provider.entered=[];provider.gate.clear();jobs=[]
            for index in range(2):
                response=client.post('/api/crons',json={'name':f'Calculation {repeat}-{index}',
                    'task':'Calculate six multiplied by seven. Return the answer.', 'schedule':'*/5 * * * *',
                    'delivery_channel':'web','delivery_target':'web:parallel-proof',
                    'artifact_delivery_options':{'requires_attachment_delivery':False,'required_artifact_extensions':[],'deliver_required_extensions_only':False}})
                assert response.status_code==200,response.text
                job=crons.get_cron(response.json()['job']['id'])
                job.next_run=(datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat();jobs.append(job)
            identities={j.id for j in jobs}
            jobs=[j for j in crons.load_crons(refresh_next_runs=False) if j.id in identities]
            for job in jobs:job.next_run=(datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()
            crons.save_crons(jobs)
            started=time.monotonic()
            try:
                app.state.cron_scheduler._tick()
                deadline=time.monotonic()+2
                while len(provider.entered)<2 and time.monotonic()<deadline:time.sleep(.01)
                assert len(provider.entered)==2,'Scheduler still serializes provider calls'
                samples.append(max(provider.entered)-started)
                before=time.monotonic()
                plain=client.post('/api/chat',json={'text':'Calculate six multiplied by seven.','conversation_id':f'web:plain-parallel-{repeat}'})
                assert plain.status_code==200 and '42' in plain.json()['text'],plain.text
                assert time.monotonic()-before<1.5
                status=client.get('/api/cron-scheduler').json()
                assert status['running_jobs']==2 and status['max_parallel_jobs']==2
            finally:provider.gate.set()
            from cron_scheduler_helpers import finish_scheduler
            finish_scheduler(app.state.cron_scheduler)
            for job in jobs:
                assert crons.get_cron(job.id).last_result=='ok'
                assert client.patch('/api/crons/'+job.id,json={'enabled':False}).json()['ok']
        history=get_chat_store().load_messages('web:parallel-proof',limit=30)
        assert len(history)==6 and all('42' in message['text'] for message in history),history
    record_property('parallel_provider_start_seconds',json.dumps(samples))
    assert sum(samples)/len(samples)<.6 and max(samples)<1,samples


def test_parallel_setting_survives_browser_reload_and_runtime_checkpoint(tmp_path,monkeypatch):
    from playwright.sync_api import sync_playwright,expect
    from test_tracker_status_delivery import setup
    from test_web_surface_contracts import _UvicornThread,_free_port
    from nullion.cron_scheduler_state import read_limit
    app,runtime,db=setup(tmp_path,monkeypatch)
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True);page=browser.new_page()
        page.goto(server.base_url);page.locator('#settings-btn').click()
        page.locator('.settings-nav button',has_text='Crons').click()
        field=page.get_by_label('Maximum parallel scheduled jobs')
        expect(field).to_have_value('4');field.fill('3')
        with page.expect_response(lambda r:r.request.method=='PUT' and r.url.endswith('/api/cron-scheduler')) as saved:
            page.get_by_role('button',name='Save parallel limit').click()
        assert saved.value.ok and saved.value.json()['max_parallel_jobs']==3
        expect(page.locator('#cron-parallel-message')).to_have_text('Saved')
        runtime.checkpoint(force=True);assert read_limit(db)==3
        page.reload();page.locator('#settings-btn').click();page.locator('.settings-nav button',has_text='Crons').click()
        expect(page.get_by_label('Maximum parallel scheduled jobs')).to_have_value('3')
        assert page.request.get(server.base_url+'/api/cron-scheduler').json()['max_parallel_jobs']==3
        page.screenshot(path=str(tmp_path/'parallel-setting.png'));browser.close()

@pytest.mark.parametrize('value',[0,33,True,'4',1.5,None])
def test_capacity_validation_at_real_api_boundary(tmp_path,monkeypatch,value):
    from fastapi.testclient import TestClient
    from test_tracker_status_delivery import setup
    app,runtime,db=setup(tmp_path,monkeypatch)
    with TestClient(app) as client:
        response=client.put('/api/cron-scheduler',json={'max_parallel_jobs':value})
        assert response.status_code==400,response.text
        assert client.get('/api/cron-scheduler').json()['max_parallel_jobs']==4
