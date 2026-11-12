"""Bounded thread checkpoints retain durable state under independent writers."""
import json
import sqlite3
import pytest
from dataclasses import replace
from datetime import UTC,datetime,timedelta

from nullion.conversation_runtime import ConversationBranch,ConversationBranchStatus,ConversationTurn,ConversationTurnDisposition
from nullion.runtime import bootstrap_persistent_runtime


def add_turn(runtime,identity,stamp,parent=None):
    turn=ConversationTurn(turn_id=identity,conversation_id='web:thread',branch_id='branch',parent_turn_id=parent,
        disposition=ConversationTurnDisposition.CHATTER,user_message='記録',status='completed',created_at=stamp)
    runtime.store.add_conversation_turn(turn)
    runtime.store.add_conversation_branch(ConversationBranch(branch_id='branch',conversation_id='web:thread',
        status=ConversationBranchStatus.ACTIVE,created_from_turn_id='first'))
    runtime.store.set_conversation_head('web:thread',active_branch_id='branch',active_turn_id=identity)
    runtime.store.add_conversation_event({'conversation_id':'web:thread','event_type':'conversation.chat_turn',
        'created_at':stamp.isoformat(),'turn_id':identity,'assistant_reply':'記録済み'})
    return turn


@pytest.mark.parametrize('clock_offset',[-1,0,1])
def test_scoped_checkpoint_keeps_newer_head_terminal_status_and_committed_ids(tmp_path,clock_offset):
    path=tmp_path/'runtime.db';first=bootstrap_persistent_runtime(path);stamp=datetime.now(UTC)
    add_turn(first,'first',stamp);first.store.add_committed_idempotency_key('web:thread','first-key')
    first.store.add_conversation_ingress_id('web:thread','first-ingress')
    first.checkpoint_conversation_events(conversation_id='web:thread')
    stale=bootstrap_persistent_runtime(path);newer=bootstrap_persistent_runtime(path)
    add_turn(newer,'second',stamp+timedelta(seconds=clock_offset),'first')
    newer.store.add_committed_idempotency_key('web:thread','second-key');newer.store.add_conversation_ingress_id('web:thread','second-ingress')
    newer.checkpoint_conversation_events(conversation_id='web:thread')
    stale.store.add_conversation_turn(replace(stale.store.get_conversation_turn('first'),status='accepted'))
    stale.store.add_committed_idempotency_key('web:thread','late-key');stale.store.add_conversation_ingress_id('web:thread','late-ingress')
    stale.store.add_conversation_event({'conversation_id':'web:thread','event_type':'conversation.late_receipt',
        'created_at':(stamp+timedelta(seconds=2)).isoformat(),'turn_id':'first'})
    stale.checkpoint_conversation_events(conversation_id='web:thread')
    loaded=bootstrap_persistent_runtime(path)
    assert loaded.store.get_conversation_head('web:thread')['active_turn_id']=='second'
    assert loaded.store.get_conversation_turn('second').parent_turn_id=='first'
    assert loaded.store.get_conversation_turn('first').status=='completed'
    assert loaded.store.list_committed_idempotency_keys('web:thread')=={'first-key','second-key','late-key'}
    assert loaded.store.list_conversation_ingress_ids('web:thread')=={'first-ingress','second-ingress','late-ingress'}
    assert any(e['event_type']=='conversation.late_receipt' for e in loaded.store.list_conversation_events('web:thread'))


def test_scoped_checkpoint_does_not_scan_or_rewrite_other_conversations(tmp_path,monkeypatch):
    import nullion.runtime_persistence as persistence
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path);add_turn(runtime,'first',datetime.now(UTC))
    runtime.checkpoint_conversation_events(conversation_id='web:thread')
    with sqlite3.connect(path) as db:
        payload=json.dumps({'conversation_id':'web:other','event_type':'unrelated','created_at':datetime.now(UTC).isoformat()})
        db.execute('INSERT INTO conversation_events (collection,item_key,payload,updated_at) VALUES (?,?,?,?)',
            ('conversation_events','unrelated',payload,'unchanged'))
    def forbidden(*args,**kw):raise AssertionError('A full history or full runtime snapshot entered the scoped write')
    monkeypatch.setattr(runtime.store.__class__,'list_conversation_events',forbidden)
    monkeypatch.setattr(persistence,'build_runtime_store_payload',forbidden)
    runtime.checkpoint_conversation_events(conversation_id='web:thread')
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT payload,updated_at FROM conversation_events WHERE item_key=?',('unrelated',)).fetchone()==(payload,'unchanged')


def test_scoped_checkpoint_timeout_includes_shared_file_lock(tmp_path):
    import threading,time
    from nullion.runtime_persistence import _runtime_store_file_lock
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path)
    add_turn(runtime,'first',datetime.now(UTC));runtime.checkpoint_conversation_events(conversation_id='web:thread')
    ready=threading.Event();release=threading.Event()
    def hold():
        with _runtime_store_file_lock(path):
            ready.set();release.wait(3)
    thread=threading.Thread(target=hold);thread.start();assert ready.wait(1)
    try:
        started=time.perf_counter()
        with pytest.raises(sqlite3.OperationalError,match='database is locked'):
            runtime.checkpoint_conversation_events(conversation_id='web:thread',timeout_seconds=.05)
        assert time.perf_counter()-started<.4,'The file-lock wait escaped the secondary-history deadline'
    finally:release.set();thread.join(2)
    runtime.checkpoint_conversation_events(conversation_id='web:thread',timeout_seconds=.5)
    assert bootstrap_persistent_runtime(path).store.get_conversation_head('web:thread')['active_turn_id']=='first'
