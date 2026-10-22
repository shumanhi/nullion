from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


def test_real_chat_bounds_provider_chunks_without_complete_lines(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import model_clients, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolRegistry

    for key in ('NULLION_TASK_DECOMPOSITION_ENABLED', 'NULLION_MULTI_AGENT_ENABLED',
                'NULLION_SKILL_LEARNING_ENABLED', 'NULLION_DOCTOR_ENABLED'):
        monkeypatch.setenv(key, '0')
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'missing.env'))
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *args, **kwargs: None)
    # Accelerate only the adapter's budget clock; networking and product timings
    # remain real. The provider continuously sends an unfinished SSE line.
    monotonic = time.monotonic
    monkeypatch.setattr(model_clients, '_time', SimpleNamespace(monotonic=lambda: monotonic()*100))
    closed = []

    class Provider(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            self.rfile.read(int(self.headers.get('Content-Length', '0')))
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            try:
                self.wfile.write(b'data: {"type":"response.output_text.delta","delta":"')
                self.wfile.flush()
                until = monotonic()+2
                while monotonic() < until:
                    self.wfile.write(b'x')
                    self.wfile.flush()
                    time.sleep(.02)
                self.wfile.write(b'"}\n\ndata: [DONE]\n\n')
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                closed.append(True)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Provider)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    model = model_clients.CodexResponsesModelClient(api_key='synthetic', account_id='synthetic', model='test',
                        _ENDPOINT=f'http://127.0.0.1:{server.server_port}/responses')
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=ToolRegistry())
    elapsed = []
    conversation_ids = []
    try:
        with TestClient(app) as client:
            for index, prompt in enumerate(('こんにちは', 'مرحبا', 'Hola')):
                cid = f'web:bounded-stream-{index}'
                conversation_ids.append(cid)
                started = monotonic()
                response = client.post('/api/chat', json={'text':prompt, 'conversation_id':cid})
                elapsed.append(monotonic()-started)
                assert response.status_code == 200, response.text
                result = response.json()
                assert result.get('text'), result
                assert 'task is still open' not in result['text'].lower()
                assert not result.get('artifacts')
                assert not result.get('suspended_for_approval')
        runtime.checkpoint(force=True)
        saved = load_runtime_store(tmp_path/'runtime.db')
        events = saved.list_conversation_events()
        for cid in conversation_ids:
            turns = [row for row in events if row.get('conversation_id') == cid
                     and row.get('event_type') == 'conversation.chat_turn']
            assert turns and turns[-1].get('assistant_reply'), (cid, events)
            assert not turns[-1].get('artifact_paths')
        assert sum(elapsed)/len(elapsed) < 1.8 and max(elapsed) < 2.2, elapsed
        assert closed, 'Timed-out provider streams were not closed'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
