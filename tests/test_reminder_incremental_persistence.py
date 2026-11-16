"""Primary reminder writes are atomic, bounded and safe beside stale snapshots."""
import asyncio
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import sqlite3
import threading

import pytest

from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_persistence import load_runtime_store


def runtime_for(tmp_path, monkeypatch):
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.checkpoint(force=True)
    return runtime


def test_reminder_tool_lifecycle_is_durable_without_a_full_snapshot(tmp_path, monkeypatch):
    from nullion.tools import ToolRegistry, ToolInvocation, register_reminder_tools
    runtime = runtime_for(tmp_path, monkeypatch)
    monkeypatch.setattr(type(runtime), 'checkpoint', lambda *a, **k: pytest.fail('A reminder must not snapshot history'))
    registry = ToolRegistry(); register_reminder_tools(registry, runtime, default_chat_id='web:fixture')
    def invoke(name, arguments):
        result = registry.invoke(ToolInvocation(name, name, 'web:fixture', arguments))
        assert result.status == 'completed', result
        return result
    created = invoke('set_reminder', {'text':'確認する', 'due_at':'2030-06-10T09:00:00+00:00'})
    task_id = created.output['task_id']
    restored = load_runtime_store(runtime.checkpoint_path)
    assert restored.get_reminder(task_id).text == '確認する'
    assert restored.get_scheduled_task(task_id).enabled
    invoke('update_reminder', {'task_id':task_id, 'text':'Revisar', 'due_at':'2030-06-11T10:00:00+00:00'})
    assert load_runtime_store(runtime.checkpoint_path).get_reminder(task_id).text == 'Revisar'
    invoke('delete_reminder', {'task_id':task_id})
    restored = load_runtime_store(runtime.checkpoint_path)
    assert restored.get_reminder(task_id) is None and restored.get_scheduled_task(task_id) is None


@pytest.mark.parametrize('operation', ['create', 'update', 'delete', 'claim'])
def test_stale_full_snapshot_cannot_overwrite_a_native_reminder_change(tmp_path, monkeypatch, operation):
    from nullion.reminder_delivery import _claim_due_reminder_persistently
    runtime = runtime_for(tmp_path, monkeypatch)
    now = datetime.now(UTC)
    if operation != 'create':
        task = runtime.schedule_reminder(chat_id='web:fixture', text='Original', due_at=now-timedelta(minutes=1))
    stale = bootstrap_persistent_runtime(runtime.checkpoint_path)
    if operation == 'create':
        task = runtime.schedule_reminder(chat_id='web:fixture', text='Original', due_at=now+timedelta(days=1))
    elif operation == 'update':
        old = runtime.store.get_reminder(task.task_id)
        runtime.commit_reminder_change(task.task_id, task=task, reminder=replace(old, text='Updated'), previous_reminder=old)
    elif operation == 'delete':
        runtime.commit_reminder_change(task.task_id, task=None, reminder=None,
            previous_reminder=runtime.store.get_reminder(task.task_id))
    else:
        assert _claim_due_reminder_persistently(runtime, task, now)[0] is not None
    stale.checkpoint(force=True)
    restored = load_runtime_store(runtime.checkpoint_path)
    reminder = restored.get_reminder(task.task_id)
    if operation == 'delete':
        assert reminder is None and restored.get_scheduled_task(task.task_id) is None
    elif operation == 'claim':
        assert reminder.delivered_at == now and not restored.get_scheduled_task(task.task_id).enabled
    else:
        assert reminder.text == ('Updated' if operation == 'update' else 'Original')


def test_an_aborted_pair_write_never_publishes_a_partial_reminder(tmp_path, monkeypatch):
    runtime = runtime_for(tmp_path, monkeypatch)
    with closing(sqlite3.connect(runtime.checkpoint_path)) as connection:
        connection.execute("CREATE TRIGGER reject_reminder BEFORE INSERT ON reminders_crons "
            "WHEN NEW.collection='reminders' BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
        connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match='fixture failure'):
        runtime.schedule_reminder(chat_id='web:fixture', text='Never acknowledged', due_at=datetime.now(UTC))
    assert not runtime.store.list_reminders() and not runtime.store.list_scheduled_tasks()
    restored = load_runtime_store(runtime.checkpoint_path)
    assert not restored.list_reminders() and not restored.list_scheduled_tasks()


def test_update_rejects_a_stale_revision_without_mutating_memory(tmp_path, monkeypatch):
    first = runtime_for(tmp_path, monkeypatch)
    task = first.schedule_reminder(chat_id='web:fixture', text='Old', due_at=datetime.now(UTC))
    second = bootstrap_persistent_runtime(first.checkpoint_path)
    old = first.store.get_reminder(task.task_id)
    second.commit_reminder_change(task.task_id, task=task, reminder=replace(old, text='New'), previous_reminder=old)
    with pytest.raises(ValueError, match='changed'):
        first.commit_reminder_change(task.task_id, task=None, reminder=None, previous_reminder=old)
    assert first.store.get_reminder(task.task_id) == old
    first.refresh_reminders()
    assert first.store.get_reminder(task.task_id).text == 'New'


@pytest.mark.parametrize('change', ['update', 'delete'])
def test_delivery_failure_cannot_undo_a_concurrent_change(tmp_path, monkeypatch, change):
    from nullion.reminder_delivery import _claim_due_reminder_persistently, _rollback_due_reminder_claim
    first = runtime_for(tmp_path, monkeypatch)
    now = datetime.now(UTC)
    task = first.schedule_reminder(chat_id='web:fixture', text='Original', due_at=now-timedelta(minutes=1))
    reminder, original_task = _claim_due_reminder_persistently(first, task, now)
    second = bootstrap_persistent_runtime(first.checkpoint_path)
    claimed = second.store.get_reminder(task.task_id)
    second.commit_reminder_change(task.task_id, task=task if change == 'update' else None,
        reminder=replace(claimed, text='New', delivered_at=None) if change == 'update' else None,
        previous_reminder=claimed)
    _rollback_due_reminder_claim(first, original_task, reminder)
    restored = load_runtime_store(first.checkpoint_path)
    if change == 'update': assert restored.get_reminder(task.task_id).text == 'New'
    else: assert restored.get_reminder(task.task_id) is None


def test_due_delivery_reads_new_reminders_and_sends_once_across_runtimes(tmp_path, monkeypatch):
    from nullion import reminder_delivery
    first = runtime_for(tmp_path, monkeypatch)
    second = bootstrap_persistent_runtime(first.checkpoint_path)
    now = datetime.now(UTC)
    task = first.schedule_reminder(chat_id='web:fixture', text='Deliver once', due_at=now-timedelta(minutes=1))
    sent = []
    async def send(*args, **kwargs): sent.append(args); return True
    monkeypatch.setattr(reminder_delivery, '_deliver_reminder', send)
    async def run():
        return await asyncio.gather(reminder_delivery.deliver_due_reminders_once(first, send=send, now=now),
            reminder_delivery.deliver_due_reminders_once(second, send=send, now=now))
    assert sum(asyncio.run(run())) == 1 and len(sent) == 1
    assert load_runtime_store(first.checkpoint_path).get_reminder(task.task_id).delivered_at == now


def test_rescheduled_reminder_cannot_fire_from_a_stale_due_list(tmp_path, monkeypatch):
    from nullion.reminder_delivery import _claim_due_reminder_persistently
    first = runtime_for(tmp_path, monkeypatch)
    now = datetime.now(UTC)
    task = first.schedule_reminder(chat_id='web:fixture', text='Later', due_at=now-timedelta(minutes=1))
    second = bootstrap_persistent_runtime(first.checkpoint_path)
    old = first.store.get_reminder(task.task_id)
    first.commit_reminder_change(task.task_id, task=task, reminder=replace(old, due_at=now+timedelta(days=1)), previous_reminder=old)
    assert _claim_due_reminder_persistently(second, task, now) == (None, None)


def test_native_write_does_not_wait_for_a_snapshot_encoding_lock(tmp_path, monkeypatch):
    from nullion.runtime_persistence import _runtime_store_file_lock
    runtime = runtime_for(tmp_path, monkeypatch)
    started, release = threading.Event(), threading.Event()
    def snapshot():
        with runtime._checkpoint_lock, _runtime_store_file_lock(runtime.checkpoint_path):
            started.set(); assert release.wait(5)
    worker = threading.Thread(target=snapshot); worker.start(); assert started.wait(2)
    try:
        task = runtime.schedule_reminder(chat_id='web:fixture', text='No history wait', due_at=datetime.now(UTC))
        assert not release.is_set() and load_runtime_store(runtime.checkpoint_path).get_reminder(task.task_id)
    finally:
        release.set(); worker.join(2)


@pytest.mark.parametrize('surface', ['operator', 'telegram_button'])
def test_direct_cancellation_uses_the_same_durable_mutation(tmp_path, monkeypatch, surface):
    from types import SimpleNamespace
    runtime = runtime_for(tmp_path, monkeypatch)
    task = runtime.schedule_reminder(chat_id='web:fixture', text='Cancel this', due_at=datetime.now(UTC))
    monkeypatch.setattr(type(runtime), 'checkpoint', lambda *a, **k: pytest.fail('Cancellation must not snapshot history'))
    if surface == 'operator':
        from nullion.operator_commands import _reminder_cmd
        assert 'Cancelled' in _reminder_cmd(runtime, ['/reminder', 'cancel', task.task_id])
    else:
        from nullion.telegram_app import _execute_decision_action
        label, reply = _execute_decision_action(SimpleNamespace(runtime=runtime),
            kind='reminder', action='cancel', record_id=task.task_id)
        assert label == 'Cancelled'
    assert load_runtime_store(runtime.checkpoint_path).get_reminder(task.task_id) is None


@pytest.mark.parametrize('setting', ['activity', 'thinking', 'streaming'])
def test_chat_display_setting_commits_only_its_conversation(tmp_path, monkeypatch, setting):
    from nullion import chat_operator
    runtime = runtime_for(tmp_path, monkeypatch)
    monkeypatch.setattr(type(runtime), 'checkpoint', lambda *a, **k: pytest.fail('Display settings must not snapshot history'))
    setter = {'activity':chat_operator.set_activity_trace_enabled_for_chat,
        'thinking':chat_operator.set_thinking_display_enabled_for_chat,
        'streaming':chat_operator.set_chat_streaming_enabled_for_chat}[setting]
    setter(runtime, chat_id='telegram:77', enabled=True)
    restored = load_runtime_store(runtime.checkpoint_path)
    key = {'activity':'activity_trace_enabled', 'thinking':'thinking_display_enabled', 'streaming':'chat_streaming_enabled'}[setting]
    assert restored.get_conversation_session_setting('telegram:77', key) is True
