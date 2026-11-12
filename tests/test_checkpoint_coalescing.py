"""Checkpoint no-ops and coalesced writes preserve persistence intent."""
from types import SimpleNamespace
import threading

import pytest

from nullion.audit import make_audit_record
from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_persistence import build_runtime_store_payload, runtime_store_payload_fingerprint


@pytest.mark.parametrize('filename', ['runtime.db', 'runtime.json'])
def test_reopened_runtime_uses_the_checkpoint_hash_and_does_not_rewrite(tmp_path, monkeypatch, filename):
    import nullion.runtime as module
    path = tmp_path/filename
    first = bootstrap_persistent_runtime(path)
    first.store.add_audit_record(make_audit_record('fixture.initial', 'fixture'))
    first.checkpoint()
    reopened = bootstrap_persistent_runtime(path)
    assert reopened.last_checkpoint_fingerprint == runtime_store_payload_fingerprint(build_runtime_store_payload(reopened.store))
    def unexpected(*a, **k): raise AssertionError('Unchanged reopened runtime wrote a full checkpoint')
    monkeypatch.setattr(module, 'checkpoint_runtime_store', unexpected)
    assert reopened.checkpoint() == path


@pytest.mark.parametrize('cache_surface', ['none', 'telegram'])
def test_no_local_change_does_not_merge_external_history_but_later_mutation_does(tmp_path, monkeypatch, cache_surface):
    import nullion.runtime_persistence as persistence
    path = tmp_path/'runtime.db'
    current = bootstrap_persistent_runtime(path)
    current.store.add_audit_record(make_audit_record('fixture.initial', 'fixture'))
    current.checkpoint()
    baseline_signature = current.last_checkpoint_file_signature
    other = bootstrap_persistent_runtime(path)
    external = make_audit_record('fixture.external', 'fixture'); other.store.add_audit_record(external)
    other.checkpoint(force=True)
    calls=[]; original=persistence._load_runtime_store_sqlite_collections
    def record(*a, **k): calls.append(True); return original(*a, **k)
    monkeypatch.setattr(persistence, '_load_runtime_store_sqlite_collections', record)
    current.checkpoint()
    assert not calls
    if cache_surface == 'telegram':
        from nullion import telegram_app
        telegram_app._CHECKPOINT_FILE_SIGNATURES[str(path)] = baseline_signature
        telegram_app._cache_runtime_checkpoint_signature(current)
        assert telegram_app._CHECKPOINT_FILE_SIGNATURES[str(path)] == baseline_signature
    assert current.last_checkpoint_file_signature == baseline_signature, 'A no-op must not acknowledge an unseen external snapshot'
    local=make_audit_record('fixture.local', 'fixture');current.store.add_audit_record(local)
    current.checkpoint()
    assert len(calls)==1
    reopened=bootstrap_persistent_runtime(path)
    assert {external.record_id, local.record_id}.issubset({r.record_id for r in reopened.store.audit_records})


def test_force_and_deleted_checkpoint_still_write_without_local_mutation(tmp_path, monkeypatch):
    import nullion.runtime as module
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path);runtime.checkpoint()
    writes=[];original=module.checkpoint_runtime_store
    def record(*a,**k):writes.append(True);return original(*a,**k)
    monkeypatch.setattr(module,'checkpoint_runtime_store',record)
    runtime.checkpoint(force=True)
    assert len(writes)==1
    path.unlink();runtime.checkpoint()
    assert len(writes)==2 and path.exists()


@pytest.mark.parametrize('initial,later,expected', [(False, True, [False,True]), (True,False,[True,False]), (False,False,[False,False])])
def test_background_checkpoint_preserves_force_only_for_the_requested_pass(monkeypatch,initial,later,expected):
    from nullion import web_app
    queued=[];calls=[]
    class Thread:
        def __init__(self,*,target,**kw):self.target=target
        def start(self):queued.append(self.target)
    monkeypatch.setattr(web_app.threading,'Thread',Thread)
    def checkpoint(*,force=False):
        calls.append(force)
        if len(calls)==1:web_app._schedule_web_checkpoint(runtime,force=later)
    runtime=SimpleNamespace(checkpoint=checkpoint)
    web_app._schedule_web_checkpoint(runtime,force=initial)
    queued.pop(0)()
    assert calls==expected
    assert id(runtime) not in web_app._WEB_CHECKPOINT_INFLIGHT


def test_retiring_writer_cannot_clear_a_new_writers_pending_state(monkeypatch):
    from nullion import web_app
    queued=[];calls=[];restarted=False;lock=threading.RLock()
    class Thread:
        def __init__(self,*,target,**kw):self.target=target
        def start(self):queued.append(self.target)
    class Gate:
        def __enter__(self):lock.acquire()
        def __exit__(self,*exc):
            nonlocal restarted
            lock.release()
            if calls and not restarted and id(runtime) not in web_app._WEB_CHECKPOINT_INFLIGHT:
                restarted=True;web_app._schedule_web_checkpoint(runtime,force=True)
    monkeypatch.setattr(web_app.threading,'Thread',Thread)
    monkeypatch.setattr(web_app,'_WEB_CHECKPOINT_LOCK',Gate())
    runtime=SimpleNamespace(checkpoint=lambda *,force=False:calls.append(force))
    web_app._schedule_web_checkpoint(runtime)
    queued.pop(0)()
    assert queued and id(runtime) in web_app._WEB_CHECKPOINT_INFLIGHT
    queued.pop(0)()
    assert calls==[False,True] and id(runtime) not in web_app._WEB_CHECKPOINT_INFLIGHT


def test_requests_queued_before_worker_starts_share_one_forced_pass(monkeypatch):
    from nullion import web_app
    queued = []
    calls = []

    class Thread:
        def __init__(self, *, target, **kwargs):
            self.target = target

        def start(self):
            queued.append(self.target)

    monkeypatch.setattr(web_app.threading, 'Thread', Thread)
    runtime = SimpleNamespace(checkpoint=lambda *, force=False: calls.append(force))
    web_app._schedule_web_checkpoint(runtime)
    web_app._schedule_web_checkpoint(runtime, force=True)
    web_app._schedule_web_checkpoint(runtime)
    assert len(queued) == 1
    queued.pop()()
    assert calls == [True]
    assert id(runtime) not in web_app._WEB_CHECKPOINT_INFLIGHT


def test_failed_worker_start_does_not_poison_later_checkpoint_requests(monkeypatch):
    from nullion import web_app
    class FailedThread:
        def __init__(self,**kw):pass
        def start(self):raise RuntimeError('No worker available')
    monkeypatch.setattr(web_app.threading,'Thread',FailedThread)
    runtime=SimpleNamespace(checkpoint=lambda **kw:None)
    with pytest.raises(RuntimeError):web_app._schedule_web_checkpoint(runtime,force=True)
    assert id(runtime) not in web_app._WEB_CHECKPOINT_INFLIGHT
    assert id(runtime) not in web_app._WEB_CHECKPOINT_FORCE
    assert id(runtime) not in web_app._WEB_CHECKPOINT_DIRTY
