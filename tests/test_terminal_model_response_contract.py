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


@pytest.mark.parametrize('retry_succeeds',[True,False])
def test_report_review_retries_failed_completion_using_same_document_and_sources(retry_succeeds):
    html='<!doctype html><html><body>AAA 20</body></html>'
    job=SimpleNamespace(name='Report',task='AAAを報告。',artifact_delivery_options={})
    result={'tool_results':[ToolResult('quotes','market_quote','completed',{'symbol':'AAA','price':20})]}
    class Client:
        calls=[]
        def create(self,**kwargs):
            self.calls.append(kwargs)
            if len(self.calls)==1 or not retry_succeeds:
                return {'content':[],'completion':{'status':'failed','error_code':'server_error'}}
            return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':True,'html':'','issues':[],'edits':[]})}],
                    'completion':{'status':'completed'}}
    client=Client()
    reviewed=_instruction_aware_cron_html(job,result,model_client=client,existing_html=html)
    assert reviewed==(html if retry_succeeds else None)
    assert len(client.calls)==2
    assert client.calls[0]['messages']==client.calls[1]['messages']
    assert client.calls[0]['tools']==client.calls[1]['tools']==[]
    assert [trace['completion_status'] for trace in result['cron_artifact_review_attempts']]==['failed','completed' if retry_succeeds else 'failed']

@pytest.mark.parametrize('context,delegated',[
    ({'scheduled_task_run':True,'requires_artifact_delivery':True,'completion_review_owner':'cron_delivery','required_artifact_extensions':['.html']},True),
    ({'scheduled_task_run':True,'requires_artifact_delivery':True,'completion_review_owner':'cron_delivery','required_artifact_extensions':['.html','.pdf']},False),
    ({'scheduled_task_run':False,'requires_artifact_delivery':True,'completion_review_owner':'cron_delivery','required_artifact_extensions':['.html']},False),
    ({'scheduled_task_run':True,'requires_artifact_delivery':False,'completion_review_owner':'cron_delivery','required_artifact_extensions':['.html']},False),
    ({'scheduled_task_run':True,'requires_artifact_delivery':True,'required_artifact_extensions':['.html']},False),
])
def test_review_delegation_requires_typed_scheduled_html_contract(context,delegated):
    from nullion.agent_orchestrator import _review_risky_agent_completion
    from nullion.tools import ToolRegistry
    class Client:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            raise AssertionError('Generic reviewer dependency must stay outside the scheduled HTML phase')
    client=Client()
    result=_review_risky_agent_completion({'orchestrator':SimpleNamespace(model_client=client),
        'tool_registry':ToolRegistry(),'tool_flow_context':context,'user_message':'確認。'},
        final_text='確認。',tool_results=[],artifacts=[],force=True)
    assert (result is None)==delegated
    assert client.calls==(0 if delegated else 1)
