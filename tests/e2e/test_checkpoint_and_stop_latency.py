"""A full checkpoint cannot freeze stop, health, or unrelated real Web chat."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os,sqlite3,threading,time,statistics
from datetime import UTC,datetime,timedelta
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker Uvicorn/Chromium/SQLite boundary')


def frame(cid,identity='frame',updated=None):
    from nullion.task_frames import TaskFrame,TaskFrameStatus,TaskFrameOperation,TaskFrameExecutionContract,TaskFrameOutputContract,TaskFrameFinishCriteria
    now=updated or datetime.now(UTC)
    return TaskFrame(identity,cid,'branch-'+identity,'turn-'+identity,None,TaskFrameStatus.ACTIVE,TaskFrameOperation.ANSWER_WITH_CONTEXT,None,TaskFrameExecutionContract(),TaskFrameOutputContract(),TaskFrameFinishCriteria(),'Current task',now,now)


def seed(runtime,active):
    from nullion.conversation_runtime import ConversationTurn,ConversationTurnDisposition
    runtime.store.add_task_frame(active);runtime.store.set_active_task_frame_id(active.conversation_id,active.frame_id)
    runtime.store.add_conversation_turn(ConversationTurn(active.source_turn_id,active.conversation_id,active.branch_id,None,ConversationTurnDisposition.INDEPENDENT,'Fixture request','accepted',active.created_at))
    runtime.store.set_conversation_head(active.conversation_id,active_branch_id=active.branch_id,active_turn_id=active.source_turn_id)


def test_full_checkpoint_keeps_stop_health_and_plain_chat_responsive(tmp_path,monkeypatch,unused_tcp_port):
    from nullion import web_app,chat_store,tools,runtime_persistence as persistence
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.events import make_event
    from nullion.task_frames import TaskFrameStatus
    from tests.e2e.test_web_surface_contracts import _UvicornThread
    from playwright.sync_api import sync_playwright,expect
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path);cid='web:stop-latency';active=frame(cid);seed(runtime,active);runtime.checkpoint(force=True)
    class Provider:
        calls=0
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            self.calls += 1
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': '4'}]}
    model=Provider();runtime.model_client=model;app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=tools.ToolRegistry())
    entered=threading.Event();release=threading.Event();failures=[];original=persistence._encode_sqlite_runtime_payload
    def delayed(collection,*args,**kwargs):
        if threading.current_thread().name=='slow-full-writer' and collection=='conversation_events':
            entered.set();assert release.wait(12),'Test did not release checkpoint encoding'
        return original(collection,*args,**kwargs)
    monkeypatch.setattr(persistence,'_encode_sqlite_runtime_payload',delayed)
    def save():
        try:runtime.checkpoint(force=True)
        except Exception as exc:failures.append(repr(exc))
    with _UvicornThread(app,unused_tcp_port) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True);context=browser.new_context();context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");");page=context.new_page();page.goto(server.base_url,wait_until='domcontentloaded');page.locator('#user-input').wait_for()
        # More than SQLite's ordinary dirty-page cache: the old writer spills
        # before pausing its later history encoding and locks native readers.
        for index in range(180):runtime.store.add_event(make_event('fixture.bulk','fixture',{'index':index,'payload':'x'*18000}))
        runtime.store.add_conversation_event({'conversation_id':cid,'event_type':'fixture.history','created_at':datetime.now(UTC).isoformat()})
        writer=threading.Thread(target=save,name='slow-full-writer');writer.start();assert entered.wait(4)
        try:
            read_samples=[]
            for _ in range(3):
                started=time.monotonic()
                with sqlite3.connect(path,timeout=.1) as db:assert db.execute('SELECT COUNT(*) FROM runtime_meta').fetchone()[0]
                assert page.request.get(server.base_url+'/api/health',timeout=1000).json()['status']=='ok'
                read_samples.append(time.monotonic()-started)
            started=time.monotonic();page.locator('#user-input').fill('/stop');page.locator('#user-input').press('Enter')
            expect(page.locator('#messages')).to_contain_text('Stopped',timeout=2000)
            assert time.monotonic()-started<2
            assert runtime.store.get_task_frame(active.frame_id).status==TaskFrameStatus.CANCELLED and runtime.store.get_active_task_frame_id(cid) is None
            chat_samples=[]
            for prompt in ['Return only 4.','Devuelve solo 4.','4だけ返してください。']:
                before=model.calls;started=time.monotonic();response=page.request.post(server.base_url+'/api/chat',data={'text':prompt,'conversation_id':cid+'-plain-'+str(before)},timeout=2500)
                chat_samples.append(time.monotonic()-started);assert response.ok and response.json()['text']=='4' and model.calls==before+1,response.text()
            assert statistics.mean(read_samples)<.2 and max(read_samples)<.5,read_samples
            assert statistics.mean(chat_samples)<1 and max(chat_samples)<2,chat_samples
        finally:release.set();writer.join(10);browser.close()
    assert not writer.is_alive() and not failures,failures
    deadline=time.monotonic()+4
    while True:
        reopened=bootstrap_persistent_runtime(path)
        if reopened.store.get_task_frame(active.frame_id).status==TaskFrameStatus.CANCELLED:break
        assert time.monotonic()<deadline,'Deferred cancellation did not persist after the writer finished'
        time.sleep(.05)
    assert reopened.store.get_active_task_frame_id(cid) is None


def test_late_stop_checkpoint_preserves_a_newer_active_frame(tmp_path):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.session_stop import cancel_active_task_frame
    from nullion.task_frames import TaskFrameStatus
    path=tmp_path/'runtime.db';first=bootstrap_persistent_runtime(path);cid='web:late-stop';old=frame(cid);seed(first,old);first.checkpoint(force=True)
    stale=bootstrap_persistent_runtime(path);assert cancel_active_task_frame(stale,cid)
    fresh=bootstrap_persistent_runtime(path);new=frame(cid,'newer',datetime.now(UTC)+timedelta(seconds=1));seed(fresh,new);fresh.checkpoint_conversation_events(conversation_id=cid)
    stale.checkpoint_conversation_events(conversation_id=cid)
    restored=bootstrap_persistent_runtime(path);assert restored.store.get_active_task_frame_id(cid)==new.frame_id
    assert restored.store.get_task_frame(new.frame_id).status==TaskFrameStatus.ACTIVE
