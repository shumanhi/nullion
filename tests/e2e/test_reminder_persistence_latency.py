"""Chat reminder CRUD stays responsive beside a slow unrelated runtime snapshot."""
import json
import os
from pathlib import Path
import threading
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('prompts', [
    ['Remind me tomorrow at 9 AM.', 'Change it to 10 AM.', 'Delete that reminder.'],
    ['明日の午前9時に通知してください。', '午前10時に変更してください。', 'その通知を削除してください。'],
])
def test_real_chat_reminder_lifecycle_does_not_serialize_or_wait_for_snapshot_history(tmp_path, monkeypatch, prompts, record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import PersistentRuntime
    from nullion.runtime_persistence import load_runtime_store, _runtime_store_file_lock
    from nullion.tools import ToolRegistry, register_reminder_tools
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', '0')
    registry = ToolRegistry(); register_reminder_tools(registry, runtime, default_chat_id='web:reminder-delta')
    runtime.active_tool_registry = registry
    # A real snapshot can spend minutes encoding history while holding these
    # two locks. Simulate that dependency without actually copying private data.
    started, release = threading.Event(), threading.Event()
    real_checkpoint = PersistentRuntime.checkpoint
    checkpoint_calls = []
    def slow_snapshot(self, *args, **kwargs):
        if self is runtime:
            checkpoint_calls.append(True)
            with self._checkpoint_lock, _runtime_store_file_lock(self.checkpoint_path):
                started.set()
                assert release.wait(8)
        return real_checkpoint(self, *args, **kwargs)
    monkeypatch.setattr(PersistentRuntime, 'checkpoint', slow_snapshot)
    class Model:
        operation = 'set_reminder'
        scoped = False
        invoked = False
        target_id = None
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if not kwargs.get('tools'): return text('{}')
            if not self.scoped:
                self.scoped = True
                return call('request_tool_scope', {'capabilities':['scheduler_mutate'], 'scheduler_action':'mutate',
                    'tool_names':[self.operation], 'required_tool_names':[self.operation]})
            if not self.invoked:
                self.invoked = True
                args = {'text':'確認する', 'due_at':'2030-06-10T09:00:00+00:00'} if self.operation == 'set_reminder' else {
                    'task_id':self.target_id, **({'due_at':'2030-06-10T10:00:00+00:00'} if self.operation == 'update_reminder' else {})}
                return call(self.operation, args)
            return text('Reminder updated.')
    model = Model(); samples = []
    with TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        from nullion.runtime_checkpoints import schedule_runtime_checkpoint, wait_runtime_checkpoints
        schedule_runtime_checkpoint(runtime)
        assert started.wait(2)
        try:
            for prompt, operation in zip(prompts, ['set_reminder', 'update_reminder', 'delete_reminder']):
                model.operation, model.scoped, model.invoked = operation, False, False
                before = time.monotonic()
                response = client.post('/api/chat', json={'text':prompt, 'conversation_id':'web:reminder-delta'})
                samples.append(time.monotonic()-before)
                assert response.status_code == 200, response.text
                payload = response.json()
                assert payload['text'].strip() and not payload.get('artifacts'), payload
                assert not release.is_set(), 'The result waited for the unrelated full snapshot'
                stored = load_runtime_store(runtime.checkpoint_path)
                if operation == 'set_reminder':
                    reminders = stored.list_reminders(); assert len(reminders) == 1
                    model.target_id = reminders[0].task_id
                    assert stored.get_scheduled_task(model.target_id).enabled
                elif operation == 'update_reminder':
                    assert stored.get_reminder(model.target_id).due_at.hour == 10
                else:
                    assert stored.get_reminder(model.target_id) is None
                    assert stored.get_scheduled_task(model.target_id) is None
                turns = [row for row in runtime.store.list_conversation_events('web:reminder-delta')
                    if row.get('event_type') == 'conversation.chat_turn']
                assert turns[-1]['response_fulfilled'] is True, turns[-1]
                assert any(row['tool_name'] == operation and row['status'] == 'completed'
                    for row in turns[-1]['tool_results']), turns[-1]
            # The only checkpoint owner is the deliberately blocked background
            # snapshot. No reminder handler may enter the full checkpoint path.
            assert checkpoint_calls == [True]
        finally:
            release.set(); assert wait_runtime_checkpoints(runtime, timeout=5)
    assert sum(samples)/len(samples) < .8 and max(samples) < 1.5, samples
    record_property('reminder_latency_seconds', json.dumps(samples))
    restored = load_runtime_store(runtime.checkpoint_path)
    assert restored.get_reminder(model.target_id) is None, 'The older snapshot resurrected the deletion'
