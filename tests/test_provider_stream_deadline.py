from types import SimpleNamespace
import json

import httpx
import pytest

from nullion import model_clients


def stream_client(monkeypatch, response_factory, calls):
    class Client:
        def __init__(self, timeout):
            calls.append(timeout)
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def stream(self, *args, **kwargs): return response_factory()
    monkeypatch.setattr(model_clients, '_httpx', SimpleNamespace(
        Client=Client, TimeoutException=httpx.TimeoutException, ReadTimeout=httpx.ReadTimeout,
        TransportError=httpx.TransportError))


def adapter():
    return model_clients.CodexResponsesModelClient(api_key='synthetic', account_id='synthetic', model='test')


class Response:
    status_code = 200
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def raise_for_status(self): pass
    def iter_lines(self):
        pending = ''.join(self.iter_text())
        yield from pending.splitlines()


@pytest.mark.parametrize('timeout', [None, 1.0])
def test_default_and_explicit_budgets_expire_before_unterminated_line(monkeypatch, timeout):
    clock = [0.0]
    monkeypatch.setattr(model_clients, '_time', SimpleNamespace(monotonic=lambda: clock[0]))
    calls = []
    class Slow(Response):
        def iter_text(self):
            clock[0] = 121.0 if timeout is None else 1.1
            yield 'data: {"type":"response.output_text.delta","delta":"'
            raise AssertionError('Provider stream must be closed before reading more chunks')
    stream_client(monkeypatch, Slow, calls)
    with pytest.raises(model_clients.ModelClientTimeoutError, match='timeout budget'):
        adapter().create(messages=[{'role':'user','content':'続けて'}], tools=[], timeout=timeout)
    assert len(calls) == 1


def test_expired_transport_attempt_does_not_restart_budget(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(model_clients, '_time', SimpleNamespace(monotonic=lambda: clock[0]))
    calls = []
    class Failed(Response):
        def iter_text(self):
            clock[0] = 1.1
            raise httpx.ReadTimeout('synthetic stalled provider')
            yield ''
    stream_client(monkeypatch, Failed, calls)
    with pytest.raises(model_clients.ModelClientTimeoutError, match='timeout budget'):
        adapter().create(messages=[{'role':'user','content':'مرحبا'}], tools=[], timeout=1)
    assert len(calls) == 1


@pytest.mark.parametrize('reply', ['受信しました', 'تم الاستلام', 'Recibido'])
def test_chunked_unicode_and_crlf_preserve_provider_reply(monkeypatch, reply):
    calls = []
    class Completed(Response):
        def iter_text(self):
            text = 'data: '+json.dumps({'type':'response.output_text.delta','delta':reply}, ensure_ascii=False)+'\r\n\r\n'
            text += 'data: '+json.dumps({'type':'response.completed','response':{'status':'completed'}})+'\r\n'
            for character in text:
                yield character
    stream_client(monkeypatch, Completed, calls)
    result = adapter().create(messages=[{'role':'user','content':reply}], tools=[], timeout=1)
    assert result['content'] == [{'type':'text','text':reply}]
    assert result['completion']['status'] == 'completed'
