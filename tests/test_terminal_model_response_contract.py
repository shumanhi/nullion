from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from nullion import model_clients
from nullion.cron_delivery import _instruction_aware_cron_html
from nullion.tools import ToolResult


def provider(monkeypatch, streams):
    calls = []
    class Response:
        status_code = 200
        reason_phrase = 'OK'
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def raise_for_status(self): pass
        def iter_lines(self):
            yield from streams[len(calls)-1]
    class HTTP:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def stream(self, method, url, **kwargs):
            calls.append(kwargs['json'])
            return Response()
    monkeypatch.setattr(model_clients, '_httpx', SimpleNamespace(Client=HTTP))
    return model_clients.CodexResponsesModelClient(api_key='synthetic', account_id='synthetic', model='test'), calls


def event(kind, response):
    return 'data:'+json.dumps({'type': kind, 'response': response})


def snapshot(text):
    return event('response.completed', {'status': 'completed', 'output': [
        {'type': 'message', 'content': [{'type': 'output_text', 'text': text}]}]})


@pytest.mark.parametrize('prompt', ['こんにちは', 'Hola', 'مرحبا'])
def test_terminal_snapshot_is_an_answer_without_deltas(monkeypatch, prompt):
    client, calls = provider(monkeypatch, [[snapshot('結果')]])
    result = client.create(messages=[{'role':'user','content':prompt}], tools=[])
    assert result['content'] == [{'type':'text','text':'結果'}]
    assert len(calls) == 1


def test_terminal_snapshot_does_not_duplicate_streamed_content(monkeypatch):
    client, _ = provider(monkeypatch, [[
        'data: '+json.dumps({'type':'response.output_text.delta','delta':'結果'}), snapshot('結果')]])
    deltas = []
    result = client.create(messages=[], tools=[], text_delta_callback=deltas.append)
    assert result['content'] == [{'type':'text','text':'結果'}]
    assert deltas == ['結果']


def test_terminal_snapshot_recovers_complete_function_call(monkeypatch):
    client, _ = provider(monkeypatch, [[event('response.completed', {'status':'completed','output':[
        {'type':'function_call','call_id':'c1','name':'lookup','arguments':'{"id":7}'}]})]])
    result = client.create(messages=[], tools=[])
    assert result['stop_reason'] == 'tool_use'
    assert result['content'] == [{'type':'tool_use','id':'c1','name':'lookup','input':{'id':7}}]


@pytest.mark.parametrize('kind', ['response.failed', 'error'])
def test_transient_failed_stream_retries_without_publishing_empty_success(monkeypatch, kind):
    failure = event(kind, {'status':'failed','error':{'code':'server_error'}}) if kind!='error' else 'data:'+json.dumps({'type':'error','code':'server_error'})
    client, calls = provider(monkeypatch, [[failure], [snapshot('Recovered')]])
    result = client.create(messages=[], tools=[], timeout=2)
    assert result['content'][0]['text'] == 'Recovered'
    assert len(calls) == 2
    assert calls[0] == calls[1]


@pytest.mark.parametrize('code,count', [('server_error',2),('invalid_request_error',1)])
def test_failed_stream_exhaustion_raises_without_provider_payload(monkeypatch, code, count):
    failure=event('response.failed', {'status':'failed','error':{'code':code,'message':'private request payload'}})
    client,calls=provider(monkeypatch, [[failure],[failure]])
    with pytest.raises(model_clients.ModelClientResponseError) as error:
        client.create(messages=[],tools=[])
    assert error.value.code == code
    assert 'private' not in str(error.value)
    assert len(calls) == count


def test_failed_stream_never_flushes_partial_tool_calls(monkeypatch):
    added='data:'+json.dumps({'type':'response.output_item.added','output_index':0,
                             'item':{'type':'function_call','call_id':'c','name':'send','arguments':'{}'}})
    client,calls=provider(monkeypatch, [[added,event('response.failed',{'status':'failed','error':{'code':'server_error'}})]])
    with pytest.raises(model_clients.ModelClientResponseError): client.create(messages=[],tools=[])
    assert len(calls)==1


def test_empty_completed_response_is_bounded_failure(monkeypatch):
    empty=event('response.completed',{'status':'completed','output':[]})
    client,calls=provider(monkeypatch, [[empty],[empty]])
    with pytest.raises(model_clients.ModelClientResponseError,match='empty_response'):
        client.create(messages=[],tools=[])
    assert len(calls)==2
