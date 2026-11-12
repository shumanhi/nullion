"""Short-lived DB work releases handles independently of Python GC."""
from contextlib import closing
from types import SimpleNamespace
import sqlite3
import threading

import pytest


@pytest.mark.parametrize('operation',['approval_read','approval_claim','scheduler_read','scheduler_write','cache_read','cache_write','memory_read','workflow_read','workflow_write'])
def test_database_operations_close_real_connections(tmp_path,monkeypatch,operation):
    from nullion import approval_resume,cron_scheduler_state,runtime_cache,runtime_persistence
    from nullion.runtime import bootstrap_persistent_runtime
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_KEY_STORAGE','local')
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path);runtime.checkpoint(force=True)
    approval_resume.claim_approval_resume(runtime,'seed')
    cron_scheduler_state.write_limit(path,4)
    runtime_cache.set_json('fixture','seed',{'value':3},db_path=path)
    from nullion.workflow_recovery import WorkflowCheckpointStore,WorkflowCheckpoint
    workflows=WorkflowCheckpointStore(path)
    original=sqlite3.connect;opened=[]
    owner_thread=threading.get_ident()
    def tracked(*args,**kwargs):
        connection=original(*args,**kwargs)
        # Other tests may still be draining their own background checkpoint.
        # Only this operation's thread owns the handles inspected below.
        if threading.get_ident()==owner_thread:
            opened.append(connection)
        return connection
    monkeypatch.setattr(sqlite3,'connect',tracked)
    for index in range(5):
        if operation=='approval_read':assert not approval_resume.approval_resume_claimed(runtime,'missing')
        elif operation=='approval_claim':assert approval_resume.claim_approval_resume(runtime,f'new-{index}')
        elif operation=='scheduler_read':assert cron_scheduler_state.read_limit(path)==4
        elif operation=='scheduler_write':assert cron_scheduler_state.write_limit(path,4)==4
        elif operation=='cache_read':
            runtime_cache._HOT_CACHE.clear()
            assert runtime_cache.get_json('fixture','seed',db_path=path).value=={'value':3}
        elif operation=='cache_write':runtime_cache.set_json('fixture',index,{'value':index},db_path=path)
        elif operation=='workflow_read':assert workflows.load('missing','web:test','goal') is None
        elif operation=='workflow_write':workflows.save(WorkflowCheckpoint(f'workflow-{index}','web:test','goal',revision=1))
        else:assert runtime_persistence.read_persisted_memory_entries(path)==[]
    assert opened
    try:
        for connection in opened:
            with pytest.raises(sqlite3.ProgrammingError,match='closed'):
                connection.execute('SELECT 1')
    finally:
        for connection in opened:connection.close()


@pytest.mark.parametrize('folder',['plain','state # one','state ? two','状態 #? %'])
def test_approval_identity_lookup_uses_literal_database_path(tmp_path,folder):
    from nullion.approval_resume import claim_approval_resume,approval_resume_claimed
    path=tmp_path/folder/'runtime.db';path.parent.mkdir()
    with closing(sqlite3.connect(path)) as connection:connection.execute('CREATE TABLE seed (value TEXT)')
    original=SimpleNamespace(checkpoint_path=path,store=SimpleNamespace())
    assert claim_approval_resume(original,'selected')
    restarted=SimpleNamespace(checkpoint_path=path,store=SimpleNamespace())
    before={str(p.relative_to(tmp_path)) for p in tmp_path.rglob('*')}
    assert approval_resume_claimed(restarted,'selected')
    assert {str(p.relative_to(tmp_path)) for p in tmp_path.rglob('*')}==before
    assert not claim_approval_resume(restarted,'selected')


def test_owned_sqlite_context_preserves_commit_rollback_and_early_return(tmp_path):
    from nullion.sqlite_connections import sqlite_connection
    path=tmp_path/'transactions.db'
    with sqlite_connection(path) as connection:
        connection.execute('CREATE TABLE records (value TEXT UNIQUE)')
        connection.execute('INSERT INTO records VALUES (?)',('committed',))
    with pytest.raises(sqlite3.ProgrammingError):connection.execute('SELECT 1')
    with pytest.raises(sqlite3.IntegrityError):
        with sqlite_connection(path) as failed:
            failed.execute('INSERT INTO records VALUES (?)',('rolled back',))
            failed.execute('INSERT INTO records VALUES (?)',('committed',))
    with pytest.raises(sqlite3.ProgrammingError):failed.execute('SELECT 1')
    def return_early():
        with sqlite_connection(path) as connection:
            connection.execute('INSERT INTO records VALUES (?)',('early return',))
            return connection
    closed=return_early()
    with pytest.raises(sqlite3.ProgrammingError):closed.execute('SELECT 1')
    with sqlite_connection(path) as check:
        assert [r[0] for r in check.execute('SELECT value FROM records ORDER BY rowid')]==['committed','early return']


@pytest.mark.parametrize('owner',['chat','outbox','updates'])
def test_connection_closes_even_when_initial_schema_setup_fails(tmp_path,monkeypatch,owner):
    from nullion.chat_store import ChatStore
    from nullion.delivery_outbox import DeliveryOutbox
    from nullion.update_operations import UpdateOperations
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_KEY_STORAGE','local')
    target=(ChatStore(tmp_path/'chat.db',key_path=tmp_path/'key') if owner=='chat' else
        DeliveryOutbox(tmp_path/'outbox.db') if owner=='outbox' else UpdateOperations(tmp_path/'updates.db'))
    original=sqlite3.connect;opened=[]
    class BlockedConnection(sqlite3.Connection):
        def execute(self,sql,*args,**kwargs):
            raise sqlite3.OperationalError('Database setup unavailable')
    def blocked(*args,**kwargs):
        result=original(*args,**kwargs,factory=BlockedConnection);opened.append(result);return result
    monkeypatch.setattr(sqlite3,'connect',blocked)
    try:
        with pytest.raises(sqlite3.OperationalError):
            with target._connect():pytest.fail('A failed setup cannot yield its connection')
        assert len(opened)==1
        # Call SQLite's base method to bypass only the injected setup fault.
        with pytest.raises(sqlite3.ProgrammingError,match='closed'):
            sqlite3.Connection.execute(opened[0],'SELECT 1')
    finally:
        for connection in opened:connection.close()


def test_commit_failure_also_releases_the_handle_and_rolls_back(tmp_path):
    from nullion.sqlite_connections import sqlite_connection
    path=tmp_path/'deferred.db'
    with sqlite_connection(path) as setup:
        setup.execute('CREATE TABLE parents(id INTEGER PRIMARY KEY)')
        setup.execute('CREATE TABLE children(parent_id INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)')
    with pytest.raises(sqlite3.IntegrityError):
        with sqlite_connection(path) as failed:
            failed.execute('PRAGMA foreign_keys=ON')
            failed.execute('INSERT INTO children VALUES (42)')
    with pytest.raises(sqlite3.ProgrammingError):failed.execute('SELECT 1')
    with sqlite_connection(path) as check:assert check.execute('SELECT COUNT(*) FROM children').fetchone()[0]==0
