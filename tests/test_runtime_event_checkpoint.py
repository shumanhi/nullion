import sqlite3,threading,time
import pytest
from nullion.runtime import bootstrap_persistent_runtime
from nullion.events import make_event
from nullion.runtime_persistence import _runtime_store_file_lock


def test_exact_event_checkpoint_preserves_other_process_events(tmp_path):
    path=tmp_path/'runtime.db';first=bootstrap_persistent_runtime(path);second=bootstrap_persistent_runtime(path)
    a=make_event('fixture.first','fixture',{'value':37});b=make_event('fixture.second','fixture',{'value':41})
    first.store.add_event(a);first.checkpoint_runtime_events((a,));second.store.add_event(b);second.checkpoint_runtime_events((b,));second.checkpoint_runtime_events((b,))
    restored=bootstrap_persistent_runtime(path)
    assert {event.event_id for event in restored.store.events}=={a.event_id,b.event_id}
    second.checkpoint()
    assert {event.event_id for event in bootstrap_persistent_runtime(path).store.events}=={a.event_id,b.event_id}


def test_event_checkpoint_has_bounded_lock_wait(tmp_path):
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path);event=make_event('fixture','fixture',{})
    locked=threading.Event();release=threading.Event()
    def holder():
        with _runtime_store_file_lock(path):locked.set();release.wait(3)
    thread=threading.Thread(target=holder);thread.start();assert locked.wait(1)
    try:
        started=time.monotonic()
        with pytest.raises(sqlite3.OperationalError,match='locked'):runtime.checkpoint_runtime_events((event,),timeout_seconds=.05)
        assert time.monotonic()-started<.3
    finally:release.set();thread.join(2)
    runtime.checkpoint_runtime_events((event,));assert bootstrap_persistent_runtime(path).store.events[-1].event_id==event.event_id


def test_busy_cron_receipts_survive_store_refresh_without_full_snapshot(tmp_path,monkeypatch):
    from nullion.audit import make_audit_record
    from nullion.cron_delivery import checkpoint_cron_delivery_records,record_cron_delivery_chat_turn
    from nullion.runtime import PersistentRuntime
    from types import SimpleNamespace
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path);source=runtime.store
    prior=make_event('prior','fixture',{});source.add_event(prior);runtime.checkpoint_runtime_events((prior,))
    event=make_event('cron.delivery.saved','fixture',{'cron_id':'owned'});audit=make_audit_record('cron.delivery.saved','fixture',{'cron_id':'owned'});source.add_event(event);source.add_audit_record(audit)
    turn=record_cron_delivery_chat_turn(source,SimpleNamespace(id='owned',name='Report'),conversation_id='web:owned',delivery_channel='web',delivered_text='Current value 37')
    monkeypatch.setattr(PersistentRuntime,'checkpoint',lambda *args,**kw:pytest.fail('Whole runtime checkpoint entered delivery'))
    locked=threading.Event();release=threading.Event()
    def holder():
        with _runtime_store_file_lock(path):locked.set();release.wait(3)
    thread=threading.Thread(target=holder);thread.start();assert locked.wait(1)
    try:
        started=time.monotonic();checkpoint_cron_delivery_records(runtime,store=source,events=(event,),audit_records=(audit,),conversation_events=(turn,));assert time.monotonic()-started<.4
        runtime.store=bootstrap_persistent_runtime(tmp_path/'replacement.db').store
    finally:release.set();thread.join(2)
    deadline=time.monotonic()+3
    while True:
        restored=bootstrap_persistent_runtime(path)
        if len(restored.store.events)==2 and restored.store.list_conversation_events('web:owned'):break
        assert time.monotonic()<deadline
        time.sleep(.03)
    assert {e.event_id for e in restored.store.events}=={prior.event_id,event.event_id}
    assert [a.record_id for a in restored.store.list_audit_records()]==[audit.record_id]
    assert restored.store.list_conversation_events('web:owned')[0]['assistant_reply']=='Current value 37'
