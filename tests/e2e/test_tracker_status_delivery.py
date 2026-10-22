"""Real API, SQLite, notification worker, platform adapter and rendered Web UI."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


def setup(tmp_path,monkeypatch,provider=None):
    from nullion import crons, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'data'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','false')
    db=tmp_path/'runtime.db';monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',db)
    runtime=bootstrap_persistent_runtime(db)
    monkeypatch.setattr(chat_store, "_store", chat_store.ChatStore(tmp_path/"chat-history.db"))
    app=create_app(runtime=runtime,registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,)),
                   orchestrator=AgentOrchestrator(model_client=provider) if provider else None)
    return app,runtime,db


def add(client,name='Aviso',channel='web',target='web:fixture'):
    response=client.post('/api/crons',json={'name':name,'task':'Comprueba la condición y avisa solo si se verifica.',
        'schedule':'0 9 * * *','delivery_channel':channel,'delivery_target':target,
        'artifact_delivery_options':{'alert_delivery_mode':'verified_matches_only','tracker_health_notifications':True}})
    assert response.status_code==200,response.text
    from nullion.crons import get_cron
    return get_cron(response.json()['job']['id'])


def record(db,j,kind,when):
    from nullion.tracker_status import start_check,finish_check
    run_id=j.id+':'+when.isoformat()
    start_check(db,j,run_id,now=when)
    result=({'cron_report_failed':True,'response_fulfilled':False} if kind=='incomplete' else
        {'execution_outcome':{'execution_succeeded':True},'response_fulfilled':True,
         'cron_alert_delivery_decision':'silent','cron_alert_delivery_reason':'no_verified_match'})
    finish_check(db,j,run_id,result,now=when+timedelta(seconds=1))


def test_real_worker_delivers_one_doctor_notice_and_silently_resolves(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.crons import load_crons
    from nullion.chat_store import get_chat_store
    from nullion.runtime_persistence import load_runtime_store
    app,runtime,db=setup(tmp_path,monkeypatch)
    now=datetime.now(timezone.utc)
    with TestClient(app) as client:
        j=add(client,name='確認')
        for index in range(11):record(db,j,'incomplete',now-timedelta(seconds=120-index*5))
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==0
        record(db,j,'incomplete',now-timedelta(seconds=1))
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==1
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now+timedelta(days=1))==0
        actions=runtime.store.list_doctor_actions()
        assert len(actions)==1 and actions[0]['status']=='pending'
        record(db,j,'no_match',now+timedelta(days=1,seconds=1))
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now+timedelta(days=1,seconds=3))==0
        assert runtime.store.list_doctor_actions()[0]['status']=='cancelled'
        history=get_chat_store().load_messages('web:fixture',limit=50)
        assert len(history)==1 and 'last 12 checks' in history[0]['text'] and '確認' in history[0]['text']
        assert 'No repair has been confirmed' in history[0]['text']
        events=[e for e in load_runtime_store(db).events if e.event_type=='cron.tracker_status.sent']
        assert [e.payload['notification_kind'] for e in events]==['failure']


def test_tracker_notification_uses_actual_telegram_formatter_and_receipt(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import config, web_app
    from nullion.config import NullionSettings,TelegramSettings
    from nullion.crons import load_crons
    deliveries=[]
    class Bot:
        def __init__(self,*a,**kw):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def send_message(self,chat_id,text,**kw):
            deliveries.append((str(chat_id),text,kw));return SimpleNamespace(message_id=len(deliveries))
        async def send_document(self,*a,**kw):raise AssertionError('Tracker status must not attach files')
    monkeypatch.setitem(sys.modules,'telegram',SimpleNamespace(Bot=Bot))
    settings=NullionSettings(doctor_enabled=False,telegram=TelegramSettings(bot_token='12345:synthetic'))
    monkeypatch.setattr(config,'load_settings',lambda *a,**kw:settings)
    monkeypatch.setattr(web_app,'load_settings',lambda *a,**kw:settings)
    app,runtime,db=setup(tmp_path,monkeypatch);now=datetime.now(timezone.utc)
    with TestClient(app) as client:
        j=add(client,name='Condition <neutral>',channel='telegram',target='4242')
        for i in range(12):record(db,j,'incomplete',now-timedelta(seconds=120-i*5))
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==1
        assert len(deliveries)==1 and deliveries[0][0]=='4242'
        assert 'last 12 checks' in deliveries[0][1]
        assert deliveries[0][2]['parse_mode']=='HTML'
        assert '&lt;neutral&gt;' in deliveries[0][1]
        receipts=[json.loads(line) for line in (tmp_path/'data/delivery_receipts.jsonl').read_text().splitlines()]
        assert receipts[-1]['status']=='succeeded' and receipts[-1]['channel']=='telegram'
        assert not receipts[-1].get('attachment_paths')


def test_notification_names_are_readable_and_never_attach_files(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.crons import load_crons
    app,runtime,db=setup(tmp_path,monkeypatch);now=datetime.now(timezone.utc)
    with TestClient(app) as client:
        j=add(client,name='[Condition] **neutral**')
        for i in range(12):record(db,j,'incomplete',now-timedelta(seconds=120-i*5))
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==1
        history=client.get('/api/chat/history/web%3Afixture?sync_runtime=1').json()['messages']
        assert len(history)==1
        assert '\\[' not in history[0]['text'] and '\\*' not in history[0]['text']
        assert '[Condition]' in history[0]['text']
        assert history[0]['metadata']['source']=='tracker_status'
        assert not history[0]['metadata'].get('artifacts')


def test_rendered_scheduler_health_and_optout_preserve_match_policy(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright,expect
    from test_web_surface_contracts import _UvicornThread,_free_port
    from nullion.crons import get_cron
    app,runtime,db=setup(tmp_path,monkeypatch);now=datetime.now(timezone.utc)
    with TestClient(app) as client:
        j=add(client,name='確認');record(db,j,'no_match',now-timedelta(minutes=1))
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True);page=browser.new_page()
        page.goto(server.base_url)
        page.locator("#settings-btn").click()
        page.locator(".settings-nav button",has_text="Crons").click()
        row=page.locator('#crow-'+j.id)
        expect(row.locator('.cron-health')).to_have_text('Checked — no match')
        expect(row).to_contain_text('Last successful check:')
        expect(row).to_contain_text('1 successful · 0 incomplete')
        row.locator('summary',has_text='Doctor monitoring').click()
        checkbox=row.locator('#cron-doctor-'+j.id)
        expect(checkbox).to_be_checked()
        with page.expect_response(lambda response: response.request.method=='PATCH' and response.url.endswith('/api/crons/'+j.id)) as saved:
            checkbox.uncheck()
            row.locator('#cron-limit-'+j.id).fill('5')
            row.get_by_role('button',name='Save monitoring').click()
        assert saved.value.ok and saved.value.json()['ok']
        expect(page.locator('#cron-doctor-'+j.id)).not_to_be_checked()
        updated=get_cron(j.id)
        assert updated.artifact_delivery_options['alert_delivery_mode']=='verified_matches_only'
        assert updated.artifact_delivery_options['doctor_monitoring_enabled'] is False
        assert updated.artifact_delivery_options['doctor_streak_limit']==5
        assert updated.enabled
        for i in range(5):record(db,updated,'incomplete',now+timedelta(minutes=i+1))
        from nullion.crons import load_crons
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now+timedelta(minutes=6))==0
        row=page.locator('#crow-'+j.id)
        with page.expect_response(lambda response: response.request.method=='PATCH' and response.url.endswith('/api/crons/'+j.id)):
            row.locator('#cron-doctor-'+j.id).check()
            row.get_by_role('button',name='Save monitoring').click()
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now+timedelta(minutes=7))==1
        page.reload()
        page.locator('#settings-btn').click()
        page.locator('.settings-nav button',has_text='Crons').click()
        row=page.locator('#crow-'+j.id)
        row.locator('summary',has_text='Doctor monitoring').click()
        expect(row.locator('#cron-doctor-'+j.id)).to_be_checked()
        expect(row.locator('#cron-limit-'+j.id)).to_have_value('5')
        assert page.request.get(server.base_url+'/api/crons').json()[0]['tracker_status']['no_match_checks_24h']==1
        page.screenshot(path=str(tmp_path/'tracker-health-ui.png'))
        browser.close()


def test_plain_chat_does_not_load_tracker_history_or_add_model_calls(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion import tracker_status
    class Provider:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            return {'content':[{'type':'text','text':'24'}]}
    p=Provider();app,runtime,db=setup(tmp_path,monkeypatch,p)
    def slow_history(*a,**kw):
        time.sleep(2);raise AssertionError('Tracker history entered plain-chat path')
    samples=[]
    with TestClient(app) as client:
        add(client)
        monkeypatch.setattr(tracker_status,'tracker_statuses',slow_history)
        csrf=client.get('/api/session').json()['csrf_token']
        for index,prompt in enumerate(['4 × 6 だけ。','Solo 4 × 6.','فقط 4 × 6.']):
            started=time.monotonic()
            response=client.post('/api/chat',headers={'x-nullion-csrf':csrf},json={'text':prompt,'conversation_id':f'web:plain-{index}'})
            assert response.status_code==200 and response.json()['text']=='24'
            samples.append(time.monotonic()-started)
    assert p.calls==3 and sum(samples)/len(samples)<1.5 and max(samples)<2.5
    record_property('tracker_plain_average_seconds',sum(samples)/len(samples))


@pytest.mark.parametrize('channel,name',[('web','通知条件'),('telegram','تنبيه')])
def test_delayed_and_slow_checks_do_not_deliver_false_health_alerts(tmp_path,monkeypatch,channel,name):
    """Replay the incident through HTTP, SQLite, worker and delivery adapter."""
    import hashlib
    import sqlite3
    from fastapi.testclient import TestClient
    from nullion import config,web_app
    from nullion.config import NullionSettings,TelegramSettings
    from nullion.crons import load_crons
    from nullion.tracker_status import _scope,start_check,finish_check
    deliveries=[]
    class Bot:
        def __init__(self,*a,**kw):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def send_message(self,chat_id,text,**kw):
            deliveries.append((str(chat_id),text,kw));return SimpleNamespace(message_id=len(deliveries))
        async def send_document(self,*a,**kw):raise AssertionError('Tracker status must not attach files')
    monkeypatch.setitem(sys.modules,'telegram',SimpleNamespace(Bot=Bot))
    settings=NullionSettings(doctor_enabled=False,telegram=TelegramSettings(bot_token='12345:synthetic'))
    monkeypatch.setattr(config,'load_settings',lambda *a,**kw:settings)
    monkeypatch.setattr(web_app,'load_settings',lambda *a,**kw:settings)
    app,runtime,db=setup(tmp_path,monkeypatch);now=datetime.now(timezone.utc)
    target='web:fixture' if channel=='web' else '4242'
    with TestClient(app) as client:
        j=add(client,name=name,channel=channel,target=target)
        record(db,j,'no_match',now-timedelta(minutes=30))
        # Stored overdue occurrence, as seen by both status API and notifier.
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE reminders_crons SET payload=json_set(payload,'$.next_run',?) WHERE item_key=?",
                         ((now-timedelta(minutes=11)).isoformat(),j.id))
            route=(str(j.workspace_id),channel,target)
            notice_scope=_scope(j)+':route:'+hashlib.sha256(json.dumps(route).encode()).hexdigest()
            conn.execute('INSERT INTO cron_tracker_state VALUES (?,?)',
                         (notice_scope,json.dumps({'warning_sent_at':(now-timedelta(minutes=25)).isoformat()})))
        statuses=client.get('/api/crons').json()
        status=next(x['tracker_status'] for x in statuses if x['id']==j.id)
        assert status['state']=='delayed' and status['last_success_at']
        assert status['consecutive_failures']==0
        for _ in range(3):
            assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==0
        with sqlite3.connect(db) as conn:
            assert json.loads(conn.execute('SELECT payload FROM cron_tracker_state WHERE scope=?',(notice_scope,)).fetchone()[0])['warning_sent_at'] is not None
        # A slow check remains active while its durable execution lease is live.
        from nullion.cron_scheduler_state import connection,scheduled_token
        token='fixture:slow'
        with connection(db) as conn:
            conn.execute('INSERT OR REPLACE INTO cron_execution_leases VALUES (?,?,?)',
                         (j.id,token,(now+timedelta(hours=3)).isoformat()))
        with scheduled_token(token):
            start_check(db,j,'slow',now=now-timedelta(minutes=12))
        status=next(x['tracker_status'] for x in client.get('/api/crons').json() if x['id']==j.id)
        assert status['state']=='running'
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==0
        finish_check(db,j,'slow',{'execution_outcome':{'execution_succeeded':True},'response_fulfilled':True,
            'cron_alert_delivery_decision':'silent','cron_alert_delivery_reason':'no_verified_match'},now=now)
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==0
        with sqlite3.connect(db) as conn:
            assert conn.execute('SELECT COUNT(*) FROM cron_tracker_notices').fetchone()[0]==0
        assert not deliveries
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now+timedelta(days=1,seconds=1))==0
        from nullion.chat_store import get_chat_store
        history=get_chat_store().load_messages('web:fixture' if channel=='web' else 'telegram:4242',limit=20)
        assert not history and not deliveries
        client.patch('/api/crons/'+j.id,json={'enabled':False})


def test_rendered_delayed_check_has_no_failure_label(tmp_path,monkeypatch):
    import sqlite3
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright,expect
    from test_web_surface_contracts import _UvicornThread,_free_port
    app,runtime,db=setup(tmp_path,monkeypatch);now=datetime.now(timezone.utc)
    with TestClient(app) as client:
        j=add(client,name='確認');record(db,j,'no_match',now-timedelta(minutes=30))
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE reminders_crons SET payload=json_set(payload,'$.next_run',?) WHERE item_key=?",
                         ((now-timedelta(minutes=11)).isoformat(),j.id))
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True);page=browser.new_page()
        page.goto(server.base_url)
        page.locator('#settings-btn').click()
        page.locator('.settings-nav button',has_text='Crons').click()
        row=page.locator('#crow-'+j.id)
        expect(row.locator('.cron-health')).to_have_text('Check delayed')
        expect(row).to_contain_text('1 successful · 0 incomplete')
        assert 'Needs attention' not in row.inner_text()
        assert page.request.patch(server.base_url+'/api/crons/'+j.id,data={'enabled':False}).ok
        browser.close()


def test_successful_no_match_streak_delivers_one_informational_notice(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.crons import load_crons
    from nullion.chat_store import get_chat_store
    app,runtime,db=setup(tmp_path,monkeypatch);now=datetime.now(timezone.utc)
    with TestClient(app) as client:
        j=add(client,name='Flight watch')
        for i in range(12):record(db,j,'no_match',now-timedelta(minutes=20-i))
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==1
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now+timedelta(days=2))==0
        history=get_chat_store().load_messages('web:fixture',limit=50)
        assert len(history)==1 and '12 consecutive times successfully' in history[0]['text']
        assert 'saved conditions' in history[0]['text'] and 'Flight watch' in history[0]['text']
        assert runtime.store.list_doctor_actions()==[]

@pytest.mark.parametrize('options',[{'doctor_monitoring_enabled':'false'},{'doctor_streak_limit':0},{'doctor_streak_limit':1.5}])
def test_invalid_doctor_controls_rejected_at_api_boundary(tmp_path,monkeypatch,options):
    from fastapi.testclient import TestClient
    app,runtime,db=setup(tmp_path,monkeypatch)
    with TestClient(app) as client:
        j=add(client)
        response=client.patch('/api/crons/'+j.id,json={'artifact_delivery_options':options})
        assert response.status_code==400,response.text
        current=next(row for row in client.get('/api/crons').json() if row['id']==j.id)
        assert current['artifact_delivery_options']['alert_delivery_mode']=='verified_matches_only'
