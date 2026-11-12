"""Real archive UI, scoped persistence, history retention and reset ordering."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os,time,statistics
from datetime import datetime,UTC,timedelta
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


def _frame(runtime,cid,name,when=None):
    from nullion.runtime import process_conversation_message
    from nullion.task_frames import TaskFrame,TaskFrameStatus,TaskFrameOperation,TaskFrameExecutionContract,TaskFrameOutputContract,TaskFrameFinishCriteria
    turn=process_conversation_message(runtime.store,user_message='Reference 37',conversation_id=cid,received_at=when)
    moment=when or datetime.now(UTC)
    frame=TaskFrame(name,cid,turn.turn.branch_id,turn.turn.turn_id,None,TaskFrameStatus.RUNNING,TaskFrameOperation.ANSWER_WITH_CONTEXT,None,TaskFrameExecutionContract(),TaskFrameOutputContract(),TaskFrameFinishCriteria(),'Reference lookup',moment,moment)
    runtime.store.add_task_frame(frame);runtime.store.set_active_task_frame_id(cid,name)
    return frame


def _setup(tmp_path,monkeypatch):
    from nullion import chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    return bootstrap_persistent_runtime(tmp_path/'runtime.db')


def test_bounded_checkpoint_persists_frame_in_its_real_collection(tmp_path,monkeypatch):
    from nullion.runtime import bootstrap_persistent_runtime
    runtime=_setup(tmp_path,monkeypatch);_frame(runtime,'web:owned','frame-owned')
    runtime.checkpoint_conversation_events(conversation_id='web:owned')
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert restored.store.get_active_task_frame_id('web:owned')=='frame-owned'
    assert restored.store.get_task_frame('frame-owned').conversation_id=='web:owned'


def test_late_reset_cannot_remove_a_newer_persisted_frame(tmp_path,monkeypatch):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.session_stop import reset_conversation_session
    runtime=_setup(tmp_path,monkeypatch);_frame(runtime,'web:owned','frame-old');runtime.checkpoint()
    late=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    reset_conversation_session(late,'web:owned',checkpoint=False,history_retained=True)
    _frame(runtime,'web:owned','frame-new',datetime.now(UTC)+timedelta(seconds=1))
    runtime.checkpoint_conversation_events(conversation_id='web:owned')
    late.checkpoint_conversation_events(conversation_id='web:owned')
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert restored.store.get_active_task_frame_id('web:owned')=='frame-new'


def test_archive_ui_starts_fresh_without_full_snapshot_and_retains_history(tmp_path,monkeypatch,unused_tcp_port):
    from nullion import web_app
    from nullion.runtime import PersistentRuntime,bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from tests.e2e.test_web_surface_contracts import _UvicornThread
    from playwright.sync_api import sync_playwright,expect
    runtime=_setup(tmp_path,monkeypatch);_frame(runtime,'web:unrelated','frame-unrelated');runtime.checkpoint()
    from nullion.agent_orchestrator import AgentOrchestrator
    class Model:
        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'Acknowledged.'}]}
    model=Model();runtime.model_client=model
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=ToolRegistry())
    full_calls=[];samples=[];original=PersistentRuntime.checkpoint
    with _UvicornThread(app,unused_tcp_port) as server, sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        try:
            for i,label in enumerate(['Recorded reference','Referencia registrada','記録された参照']):
                cid='web:archive-ui-'+str(i);_frame(runtime,cid,'frame-'+str(i));runtime.checkpoint()
                context=browser.new_context();context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
                page=context.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
                token=label+' REF-37-'+str(i)
                response=context.request.post(server.base_url+'/api/chat/history/'+cid+'/message',data={'role':'user','text':token});assert response.ok,response.text()
                page.goto(server.base_url,wait_until='domcontentloaded');expect(page.locator('#messages')).to_contain_text(token)
                def slow_full(self,**kwargs):
                    if self is runtime:full_calls.append(True);time.sleep(1.5)
                    return original(self,**kwargs)
                monkeypatch.setattr(PersistentRuntime,'checkpoint',slow_full)
                started=time.monotonic();page.locator('#composer-archive-btn').click();page.get_by_role('button',name='Archive',exact=True).click()
                expect(page.locator('#messages')).not_to_contain_text(token,timeout=3000)
                samples.append(time.monotonic()-started)
                fresh=page.evaluate("localStorage.getItem('nullion_conv_id')");assert fresh and fresh!=cid
                restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
                assert restored.store.get_active_task_frame_id(cid) is None
                assert restored.store.get_active_task_frame_id('web:unrelated')=='frame-unrelated'
                events=restored.store.list_conversation_events(cid)
                assert any(r.get('event_type')=='conversation.session_reset' and r.get('history_retained') is True for r in events)
                history=context.request.get(server.base_url+'/api/chat/history/'+cid).json()
                assert any(r.get('text')==token for r in history['messages']),history
                assert not errors,errors
                context.close();monkeypatch.setattr(PersistentRuntime,'checkpoint',original)
        finally:browser.close()
    assert not full_calls,'A whole-runtime snapshot blocked the archive UI'
    assert statistics.mean(samples)<1.5 and max(samples)<3,samples


def test_late_bounded_writer_cannot_reactivate_frame_before_reset(tmp_path,monkeypatch):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.session_stop import reset_conversation_session
    runtime=_setup(tmp_path,monkeypatch);_frame(runtime,'web:owned','frame-old');runtime.checkpoint()
    late=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    reset_conversation_session(runtime,'web:owned',checkpoint=False,history_retained=True)
    runtime.checkpoint_conversation_events(conversation_id='web:owned')
    late.checkpoint_conversation_events(conversation_id='web:owned')
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert restored.store.get_active_task_frame_id('web:owned') is None


def test_frame_identity_preserves_two_frames_on_one_branch(tmp_path,monkeypatch):
    from dataclasses import replace
    from nullion.runtime import bootstrap_persistent_runtime
    runtime=_setup(tmp_path,monkeypatch);first=_frame(runtime,'web:owned','frame-a')
    second=replace(first,frame_id='frame-b',updated_at=datetime.now(UTC))
    runtime.store.add_task_frame(second);runtime.store.set_active_task_frame_id('web:owned','frame-b');runtime.checkpoint()
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert restored.store.get_task_frame('frame-a') is not None
    assert restored.store.get_task_frame('frame-b') is not None
    assert restored.store.get_active_task_frame_id('web:owned')=='frame-b'
