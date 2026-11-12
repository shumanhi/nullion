"""Real sockets, product routes and SQLite; no stage or paid provider calls."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import json
import os
import socket
import threading
import time

import pytest
import uvicorn
from uvicorn.protocols.http.h11_impl import H11Protocol

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@contextmanager
def serve(app, *, port=0):
    from nullion.http_transport import WEB_KEEPALIVE_TIMEOUT_SECONDS

    opened, active = [], set()

    class CountingProtocol(H11Protocol):
        def connection_made(self, transport):
            opened.append(transport.get_extra_info('peername'))
            active.add(self)
            super().connection_made(transport)

        def connection_lost(self, exc):
            active.discard(self)
            super().connection_lost(exc)

    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', port))
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, http=CountingProtocol, loop='asyncio', log_level='error',
                            timeout_keep_alive=WEB_KEEPALIVE_TIMEOUT_SECONDS,
                            timeout_graceful_shutdown=2)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=lambda: server.run(sockets=[sock]), daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 15
        while not server.started and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(.01)
        assert server.started, 'Product server did not start'
        yield port, opened, active
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()
        assert not thread.is_alive(), 'Product server did not shut down'


def product(tmp_path, monkeypatch):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_DOCTOR_ENABLED', '0')
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    return create_app(runtime, orchestrator=None, registry=ToolRegistry()), runtime


def test_tray_reuses_one_tcp_connection_across_polls_and_idle_gap(tmp_path, monkeypatch, record_property):
    from nullion.tray_app import NullionTray

    app, runtime = product(tmp_path, monkeypatch)
    with serve(app) as (port, opened, active):
        tray = NullionTray(host='127.0.0.1', port=port, poll_interval=10)
        elapsed = []
        try:
            for group in range(2):
                for _ in range(15):
                    start = time.monotonic()
                    tray.refresh_status()
                    elapsed.append(time.monotonic()-start)
                    assert tray.online and tray.status_label.startswith('Running')
                if group == 0:
                    # Exceeds the former server AND default HTTPX idle timeouts.
                    time.sleep(6)
            assert len(opened) == 1, opened
            assert len(active) == 1
            assert sum(elapsed)/len(elapsed) < .5
            assert sorted(elapsed)[int(len(elapsed)*.95)] < 1.5
            record_property('poll_count', len(elapsed))
            record_property('accepted_tcp_connections', len(opened))
            record_property('average_poll_seconds', sum(elapsed)/len(elapsed))
        finally:
            tray.close()
        deadline = time.monotonic()+2
        while active and time.monotonic() < deadline:
            time.sleep(.01)
        assert not active, 'Tray exit leaked its pooled socket'
    assert runtime.store is not None


def test_trays_isolate_lanes_and_recover_after_server_retires_connection(tmp_path, monkeypatch):
    from nullion.tray_app import NullionTray

    app, _ = product(tmp_path, monkeypatch)
    retire = [False]
    @app.middleware('http')
    async def retire_socket(request, call_next):
        response = await call_next(request)
        if retire[0]:
            response.headers['Connection'] = 'close'
        return response

    with serve(app) as (port1, opened1, _), serve(app) as (port2, opened2, _):
        trays = [NullionTray(host='127.0.0.1', port=port, poll_interval=10) for port in (port1, port2)]
        try:
            with ThreadPoolExecutor(max_workers=8) as workers:
                list(workers.map(lambda i: trays[i % 2].refresh_status(), range(30)))
            assert all(tray.online for tray in trays)
            assert len(opened1) == len(opened2) == 1
            retire[0] = True
            trays[0].refresh_status(force=True)
            retire[0] = False
            trays[0].refresh_status(force=True)
            assert trays[0].online
            assert len(opened1) == 3
            for _ in range(10):
                trays[0].refresh_status()
            assert len(opened1) == 3
            assert len(opened2) == 1
        finally:
            for tray in trays:
                tray.close()


def test_restart_post_is_not_replayed_after_server_drops_response(tmp_path, monkeypatch):
    from nullion.tray_app import NullionTray
    calls = []

    # Use a raw peer for the ambiguous read: an ASGI exception would produce
    # a definite HTTP 500 instead of the lost acknowledgement in this case.
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    class Drop(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *args): pass
        def do_POST(self):
            calls.append((self.command, self.path))
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()

    server = ThreadingHTTPServer(('127.0.0.1', 0), Drop)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    tray = NullionTray(host='127.0.0.1', port=server.server_port, poll_interval=10)
    notices = []
    monkeypatch.setattr(tray, 'notify', notices.append)
    monkeypatch.setattr(tray, '_delayed_refresh', lambda: None)
    monkeypatch.setattr(tray, '_run_nullion_shortcut', lambda *args: pytest.fail('Restart repeated via CLI'))
    try:
        tray.restart_nullion()
        assert calls == [('POST', '/api/restart')]
        assert notices == ['Restart result unknown. Check status before retrying.']
    finally:
        tray.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_tray_recovers_when_web_service_restarts_on_same_port(tmp_path, monkeypatch):
    from nullion.tray_app import NullionTray

    app, _ = product(tmp_path, monkeypatch)
    with serve(app) as (port, _, _):
        tray = NullionTray(host='127.0.0.1', port=port, poll_interval=10)
        tray.refresh_status()
        assert tray.online
    try:
        tray.refresh_status(force=True)
        assert not tray.online
        assert tray._failed_checks == 1
        with serve(app, port=port) as (_, opened, _):
            tray.refresh_status(force=True)
            assert tray.online
            assert tray._failed_checks == 0
            for _ in range(10):
                tray.refresh_status()
            assert len(opened) == 1
    finally:
        tray.close()


@pytest.mark.parametrize('terminal', [False, True])
def test_model_pool_reuses_fully_consumed_streams_and_closes_terminal_streams(terminal):
    from fastapi import FastAPI, Request
    from fastapi.responses import Response
    from nullion.model_clients import CodexResponsesModelClient, clone_model_client_with_model

    app = FastAPI()
    requests = []
    @app.post('/responses')
    async def response(request: Request):
        requests.append(await request.json())
        event = {'type': 'response.output_text.delta', 'delta': '受信しました'}
        body = 'data: '+json.dumps(event)+'\n\n'
        if terminal:
            body += 'data: '+json.dumps({'type': 'response.completed', 'response': {'status': 'completed'}})+'\n\n'
        return Response(body, media_type='text/event-stream')

    with serve(app) as (port, opened, active):
        model = CodexResponsesModelClient(api_key='synthetic', account_id='synthetic', model='a',
                                         _ENDPOINT=f'http://127.0.0.1:{port}/responses')
        variant = clone_model_client_with_model(model, 'b')
        try:
            for client in [model, variant]*5:
                result = client.create(messages=[{'role':'user', 'content':'続行'}], tools=[], timeout=2)
                assert result['content'] == [{'type':'text', 'text':'受信しました'}]
            if not terminal:
                assert len(opened) == 1
            assert [request['model'] for request in requests] == ['a', 'b']*5
        finally:
            model.close()
        deadline = time.monotonic()+2
        while active and time.monotonic() < deadline:
            time.sleep(.01)
        assert not active
