from concurrent.futures import ThreadPoolExecutor
import gc
from types import SimpleNamespace

import httpx
import pytest

from nullion.http_transport import SharedHttpClient
from nullion import tray_app


def test_pool_initialization_is_thread_safe_and_finalized_once():
    opened, closed = [], []

    class Client:
        def __enter__(self):
            opened.append(self)
            return self

        def __exit__(self, *args):
            closed.append(self)

    owner = SharedHttpClient()
    with ThreadPoolExecutor(max_workers=8) as executor:
        clients = list(executor.map(lambda _: owner.get(Client), range(40)))
    assert all(client is opened[0] for client in clients)
    assert len(opened) == 1
    owner.close()
    owner.close()
    with pytest.raises(RuntimeError, match='closed'):
        owner.get(Client)
    assert closed == opened

    other = SharedHttpClient()
    other.get(Client)
    del other
    gc.collect()
    assert closed == opened


def test_tray_backs_off_address_exhaustion_and_recovers(monkeypatch):
    now = [0.0]
    calls = []
    healthy = [False]
    monkeypatch.setattr(tray_app, 'time', SimpleNamespace(monotonic=lambda: now[0]))

    def request(*args, **kwargs):
        calls.append(now[0])
        if not healthy[0]:
            raise httpx.ConnectError('[Errno 49] Cannot assign requested address')
        return {'status': 'ok'}

    monkeypatch.setattr(tray_app, '_request_json', request)
    tray = tray_app.NullionTray(host='127.0.0.1', port=8742, poll_interval=10)
    try:
        for second in range(131):
            now[0] = float(second)
            tray.refresh_status()
        assert calls == [0, 10, 30, 70, 130]
        assert not tray.online
        healthy[0] = True
        tray.refresh_status(force=True)
        assert tray.online
        assert tray._failed_checks == 0
        now[0] += 10
        tray.refresh_status()
        assert len(calls) == 9
    finally:
        tray.close()


def test_overlapping_tray_checks_and_checks_after_shutdown_are_skipped(monkeypatch):
    tray = tray_app.NullionTray(host='127.0.0.1', port=8742, poll_interval=10)
    monkeypatch.setattr(tray_app, '_request_json', lambda *a, **kw: pytest.fail('Unexpected network check'))
    tray._refresh_lock.acquire()
    try:
        tray.refresh_status()
    finally:
        tray._refresh_lock.release()
    tray.close()
    tray.refresh_status(force=True)


@pytest.mark.parametrize('failure', [httpx.ReadTimeout, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError])
def test_uncertain_restart_does_not_run_cli_or_repeat_post(monkeypatch, failure):
    calls, notices = [], []
    tray = tray_app.NullionTray(host='127.0.0.1', port=8742, poll_interval=10)

    def request(url, **kwargs):
        calls.append((url, kwargs['method']))
        raise failure('connection lost after write')

    monkeypatch.setattr(tray_app, '_request_json', request)
    monkeypatch.setattr(tray, '_run_nullion_shortcut', lambda *a: pytest.fail('Restart replayed'))
    monkeypatch.setattr(tray, 'notify', notices.append)
    monkeypatch.setattr(tray, '_delayed_refresh', lambda: None)
    tray.restart_nullion()
    assert len(calls) == 1
    assert 'unknown' in notices[0]
    tray.close()


def test_model_variants_share_pool_without_caching_auth_or_request_deadlines(monkeypatch):
    from nullion import model_clients

    opened, calls, closed = [], [], []

    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def raise_for_status(self): pass
        def iter_lines(self):
            yield 'data: {"type":"response.output_text.delta","delta":"受信"}'
            yield 'data: [DONE]'

    class Client:
        def __init__(self, **kwargs): opened.append(kwargs)
        def __enter__(self): return self
        def __exit__(self, *args): closed.append(True)
        def stream(self, *args, **kwargs):
            calls.append(kwargs)
            return Response()

    monkeypatch.setattr(model_clients, '_httpx', SimpleNamespace(Client=Client))
    client = model_clients.CodexResponsesModelClient(api_key='first', account_id='account', model='a')
    variant = model_clients.clone_model_client_with_model(client, 'b')
    for model, timeout in [(client, 2), (variant, 7)]:
        model.create(messages=[{'role': 'user', 'content': '続行'}], tools=[], timeout=timeout)
    variant.api_key = 'refreshed'
    variant.create(messages=[], tools=[], timeout=3)
    assert len(opened) == 1
    assert [call['json']['model'] for call in calls] == ['a', 'b', 'b']
    assert [call['headers']['Authorization'] for call in calls] == ['Bearer first', 'Bearer first', 'Bearer refreshed']
    assert 0 < calls[0]['timeout'] <= 2
    assert 6 < calls[1]['timeout'] <= 7
    assert 2 < calls[2]['timeout'] <= 3
    client.close()
    assert closed == [True]
    variant.close()
    assert closed == [True]
