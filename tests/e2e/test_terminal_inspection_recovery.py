"""Real chat/SQLite/shell boundary: partial evidence and fast durable approvals."""
import json
import os
import shlex
import statistics
import sys
import threading
import time

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_approval_resume_delivery_contracts import setup_terminal

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


def provider_for(command, timeout=20):
    class Provider:
        phase = 0
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None:
                return protocol
            def text(value):
                return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': value}]}
            def tool(name, args):
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'step-'+str(self.phase), 'name': name, 'input': args}]}
            names = {t['name'] for t in kw.get('tools', [])}
            if not names:
                return text('{}')
            self.phase += 1
            if self.phase == 1:
                return tool('request_tool_scope', {'capabilities': ['local_shell'], 'tool_names': ['terminal_exec'], 'required_tool_names': ['terminal_exec']})
            if self.phase == 2:
                return tool('terminal_exec', {'command': command, 'timeout_seconds': timeout, 'shell': 'bash'})
            return text('Captured row: Cedar 120 bytes. This scan is incomplete; it does not establish the largest file on the computer.')
    return Provider()


@pytest.mark.parametrize('mode', ['nonzero', 'timeout'])
def test_partial_shell_evidence_survives_real_chat_boundary_without_rescan(tmp_path, monkeypatch, mode):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime_checkpoints import wait_runtime_checkpoints
    runtime, registry = setup_terminal(tmp_path, monkeypatch, None)
    code = "print('Cedar 120 bytes', flush=True); " + ("raise SystemExit(141)" if mode == 'nonzero' else
        "import subprocess,sys,time; child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
        "print('child_pid='+str(child.pid), flush=True); time.sleep(30)")
    model = provider_for(shlex.quote(sys.executable)+' -c '+shlex.quote(code), 1)
    runtime.model_client = model
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
        started = time.perf_counter()
        response = client.post('/api/chat', json={'text': 'Inspect local file metadata, preserving partial evidence.', 'conversation_id': 'web:partial'})
        assert time.perf_counter()-started < 3, 'A descendant kept the timed-out command alive'

        assert response.status_code == 200, response.text
        data = response.json()
        # Required execution failed, so the final result must remain a failure.
        # Its partial rows must still survive in the actual persisted evidence.
        assert 'Cedar 120 bytes' in data['text'] and 'incomplete' in data['text'], data
        assert model.phase == 3, 'The primary partial answer triggered redundant recovery calls'
        turns = [r for r in runtime.store.list_conversation_events('web:partial') if r.get('event_type') == 'conversation.chat_turn']
        assert turns[-1]['response_fulfilled'] is False, turns[-1]
        commands = [r for r in turns[-1]['tool_results'] if r['tool_name'] == 'terminal_exec']
        assert len(commands) == 1, commands
        result = commands[0]
        assert result['status'] == 'failed' and 'Cedar' in result['output']['stdout'], result
        assert result['output']['output_complete'] is False
        assert result['output']['execution_outcome'] == ('nonzero_exit' if mode == 'nonzero' else 'timeout')
        if mode == 'timeout':
            from pathlib import Path
            identity = next(row.split('=', 1)[1] for row in result['output']['stdout'].splitlines() if row.startswith('child_pid='))
            # Docker PID1 may briefly retain a killed child as a zombie.
            status = Path('/proc')/identity/'stat'
            try:
                assert status.read_text().split()[2] == 'Z'
            except (FileNotFoundError, ProcessLookupError):
                pass
        assert not data.get('artifacts') and not runtime.store.list_approval_requests()
    assert wait_runtime_checkpoints(runtime)


def test_approval_delivery_does_not_wait_for_full_snapshot_and_keeps_exact_resume(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import PersistentRuntime, bootstrap_persistent_runtime
    from nullion.runtime_checkpoints import wait_runtime_checkpoints
    runtime, registry = setup_terminal(tmp_path, monkeypatch, None)
    original = PersistentRuntime.checkpoint
    release = threading.Event()
    full_calls = []
    def slow_full(self, **kw):
        if self is runtime:
            full_calls.append(threading.current_thread().name)
            assert release.wait(10), 'Full checkpoint blocked delivery'
        return original(self, **kw)
    monkeypatch.setattr(PersistentRuntime, 'checkpoint', slow_full)
    samples = []
    approvals = []
    model = provider_for('rm -f never-created-for-this-test')
    runtime.model_client = model
    try:
        with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
            for index, prompt in enumerate(['Inspect the local files.', 'Inspecciona los archivos locales.', 'ローカルファイルを調べて。']):
                model.phase = 0
                started = time.perf_counter()
                response = client.post('/api/chat', json={'text': prompt, 'conversation_id': 'web:approval-'+str(index)})
                samples.append(time.perf_counter()-started)
                assert response.status_code == 200, response.text
                assert response.json()['text'], response.json()
                approval = runtime.store.list_approval_requests()[-1]
                approvals.append(approval.approval_id)
                # Read from a new runtime while the full writer is still paused.
                reopened = bootstrap_persistent_runtime(runtime.checkpoint_path)
                persisted = reopened.store.get_approval_request(approval.approval_id)
                suspended = reopened.store.get_suspended_turn(approval.approval_id)
                assert persisted is not None and persisted.status.value == 'pending'
                assert suspended is not None and suspended.messages_snapshot
                assert suspended.conversation_id == 'web:approval-'+str(index)
                assert 'rm -f never-created-for-this-test' in json.dumps(suspended.messages_snapshot)
                assert not (tmp_path/'never-created-for-this-test').exists()
        assert len(set(approvals)) == 3
        assert full_calls and set(full_calls) == {'runtime-checkpoint'}, full_calls
        record_property('approval_mean_seconds', statistics.mean(samples))
        record_property('approval_p95_seconds', max(samples))
        assert statistics.mean(samples) < 1 and max(samples) < 1.5, samples
    finally:
        release.set()
        assert wait_runtime_checkpoints(runtime, timeout=10)
