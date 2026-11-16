import sqlite3
import threading
import time

import pytest

from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_persistence import _runtime_store_file_lock
from nullion.runtime_receipts import checkpoint_runtime_receipts


@pytest.mark.parametrize('lock_kind', ['file', 'sqlite'])
def test_busy_history_is_retained_across_refresh_and_saved_once(tmp_path, lock_kind):
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    runtime.checkpoint()
    original = runtime.store
    ready, release = threading.Event(), threading.Event()
    def hold():
        if lock_kind == 'file':
            with _runtime_store_file_lock(runtime.checkpoint_path):
                ready.set(); release.wait(5)
        else:
            with sqlite3.connect(runtime.checkpoint_path) as db:
                db.execute('BEGIN EXCLUSIVE')
                ready.set(); release.wait(5)
    thread = threading.Thread(target=hold)
    thread.start(); assert ready.wait(2)
    cid = 'telegram:fixture'
    try:
        started = time.monotonic()
        for index in range(3):
            original.add_conversation_event({'event_type': 'conversation.chat_turn',
                'conversation_id': cid, 'turn_id': str(index), 'assistant_reply': '記録済み'})
            assert checkpoint_runtime_receipts(runtime, conversation_id=cid) is False
        assert time.monotonic() - started < .8
        runtime.store = bootstrap_persistent_runtime(tmp_path / 'replacement.db').store
    finally:
        release.set(); thread.join(2)
    deadline = time.monotonic() + 4
    while True:
        with sqlite3.connect(runtime.checkpoint_path) as db:
            rows = db.execute("SELECT payload FROM conversation_events WHERE collection='conversation_events'").fetchall()
        if len(rows) == 3: break
        assert time.monotonic() < deadline, rows
        time.sleep(.03)
    restored = bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert len(restored.store.list_conversation_events(cid)) == 3


def test_nonbusy_database_failure_is_not_silently_deferred(tmp_path):
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    runtime.checkpoint_path.write_bytes(b'invalid database')
    with pytest.raises(sqlite3.DatabaseError):
        checkpoint_runtime_receipts(runtime, conversation_id='web:fixture')
