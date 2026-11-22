from email.message import Message
from io import BytesIO
import json
import threading
from types import SimpleNamespace

import pytest

from nullion import tools
from nullion.email_read_batch import read_independent_messages
from nullion.retained_work_context import inspect_retained


def test_independent_email_read_retains_partial_and_stops_dispatch_at_deadline(monkeypatch):
    calls = []
    release = threading.Event()
    monkeypatch.setattr(tools, 'tool_execution_remaining_seconds', lambda: .55)
    invocation = tools.ToolInvocation('batch', 'email_read', 'web:fixture', {
        'ids': [f'inbox-record-{i}' for i in range(10)],
        'provider_id': 'fixture', 'independent_messages': True,
    })

    def read(child):
        calls.append(child.arguments['id'])
        identifier = child.arguments['id']
        if identifier == 'inbox-record-0':
            return tools.ToolResult(child.invocation_id, child.tool_name, 'completed', {
                'id': identifier, 'message': {'id': identifier, 'body': 'Due 2027-07-14'},
                'evidence_scope': {'kind': 'email_message'},
            })
        release.wait(2)
        return tools.ToolResult(child.invocation_id, child.tool_name, 'failed', {})

    try:
        result = read_independent_messages(invocation, read)
    finally:
        release.set()

    assert result.status == 'failed' and result.output['complete'] is False
    rows = result.output['messages']
    assert [row['id'] for row in rows] == invocation.arguments['ids']
    assert result.output['completed_messages'] == 1
    assert set(calls) == set(invocation.arguments['ids'][:4])
    assert all(row['output']['reason'] == 'source_timeout' for row in rows[1:])
    assert {row['output']['executed'] for row in rows[1:4]} == {True}
    assert {row['output']['executed'] for row in rows[4:]} == {False}
    window = inspect_retained({'requests': [rows[0]['inspection_request']]}, [result])['windows'][0]
    assert window['content'] == 'Due 2027-07-14' and window['complete']


def test_oversized_mail_batch_enters_vm_in_order_with_exact_retained_pointer():
    from nullion.code_mode import _bounded_native_payload

    body = 'Known beginning. ' + 'z' * 450_000 + 'Exact late deadline: 2027-07-14.'
    rows = [{'id': f'mail-{i}', 'invocation_id': f'batch:message:{i}', 'status': 'completed',
        'inspection_request': {'invocation_id': 'batch', 'json_pointer': f'/messages/{i}/output/message/body'},
        'output': {'id': f'mail-{i}', 'message': {'id': f'mail-{i}', 'body': body},
                   'evidence_scope': {'kind': 'email_message'}}} for i in range(10)]
    result = tools.ToolResult('batch', 'email_read', 'completed', {'result_kind': 'email_message_batch',
        'requested_messages': 10, 'completed_messages': 10, 'complete': True, 'messages': rows})

    payload, raw_bytes = _bounded_native_payload(result, remaining=4_000_000, per_call=1_000_000)

    assert raw_bytes > 4_000_000 and payload['output_projection']['partial']
    assert len(json.dumps(payload, ensure_ascii=False).encode()) < 64000
    view = payload['output']['messages']
    assert [row['id'] for row in view] == [row['id'] for row in rows]
    assert all(row['output']['message']['body_truncated'] for row in view)
    exact = {**view[-1]['inspection_request'], 'offset': body.index('Exact late deadline')}
    assert inspect_retained({'requests': [exact]}, [result])['windows'][0]['content'] == 'Exact late deadline: 2027-07-14.'
    assert result.output['messages'][-1]['output']['message']['body'] == body


@pytest.mark.parametrize('change', ['connection', 'bearer'])
def test_reconnected_mail_batch_never_reads_a_different_account(change, monkeypatch):
    connection_id = 'account-a'
    wire_headers = []
    lock = threading.Lock()
    monkeypatch.setenv('NULLION_FIXTURE_MAIL_TOKEN', 'fixture-generation-a')
    monkeypatch.setattr(tools, '_connector_connection_for_invocation', lambda *_: SimpleNamespace(
        connection_id=connection_id, workspace_id='fixture-workspace', provider_id='batch-fixture',
        provider_profile='https://connector.invalid', credential_ref='env:NULLION_FIXTURE_MAIL_TOKEN',
        permission_mode='read', active=True))
    monkeypatch.setattr(tools, '_resolve_web_fetch_resolution', lambda _url: None)

    class Opener:
        def open(self, request, timeout):
            nonlocal connection_id
            with lock:
                wire_headers.append(request.get_header('Authorization'))
                if len(wire_headers) == 1:
                    if change == 'connection':
                        connection_id = 'account-b'
                    else:
                        monkeypatch.setenv('NULLION_FIXTURE_MAIL_TOKEN', 'fixture-generation-b')
            message_id = request.full_url.split('?', 1)[0].rsplit('/', 1)[-1]
            response = BytesIO(json.dumps({'id': message_id, 'snippet': 'Account A message'}).encode())
            response.headers = Message()
            response.headers['Content-Type'] = 'application/json'
            return response

    monkeypatch.setattr(tools, '_build_web_fetch_opener', Opener)
    invocation = tools.ToolInvocation('batch', 'email_read', 'web:fixture', {
        'ids': [f'inbox-record-{i}' for i in range(10)], 'independent_messages': True,
        'provider_id': 'batch-fixture',
    })
    result = tools._build_connector_email_read_handler()(invocation)

    assert result.status == 'failed' and result.output['complete'] is False
    assert 1 <= result.output['completed_messages'] <= 3
    assert wire_headers and set(wire_headers) == {'Bearer fixture-generation-a'}
    assert tools._EMAIL_READ_BATCH_BINDING.get() is None
