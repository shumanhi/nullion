"""Whole read workflows through the real API/SQLite and native source handlers."""
import json
import os
import time
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def _configure_account_fixture(runtime, tmp_path, monkeypatch):
    from nullion import connections, connector_prompt_context
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, OPERATOR_PERMISSION_PRINCIPAL

    monkeypatch.setattr(connections, '_CONNECTIONS_PATH', tmp_path/'connections.json')
    connections.save_connection_registry({'connections':[{'connection_id':'fixture-mail',
        'workspace_id':'workspace_admin','provider_id':'fixture_connector_provider',
        'display_name':'Fixture account','active':True,'permission_mode':'read'}]})
    for key in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE'):
        monkeypatch.setattr(connector_prompt_context,key,None)
    for key in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT'):
        monkeypatch.setattr(connector_prompt_context,key,0.0)
    for kind in (BoundaryKind.ACCOUNT_ACCESS, BoundaryKind.OUTBOUND_NETWORK):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-'+kind.value,
            OPERATOR_PERMISSION_PRINCIPAL, kind, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))


@pytest.mark.parametrize('mode', ['off', 'read_only', 'on'])
@pytest.mark.parametrize('explicit', [False, True])
def test_email_and_calendar_share_the_selected_execution_layer(tmp_path, monkeypatch, mode, explicit, record_property):
    from fastapi.testclient import TestClient
    from nullion import code_mode
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, register_email_plugin, register_calendar_plugin
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    _configure_account_fixture(runtime, tmp_path, monkeypatch)
    native_calls = []
    def emails(query, limit):
        native_calls.append('email_search')
        return [{'id':'mail-37', 'subject':'Observatory invoice', 'snippet':'Paid 37 USD.', 'from':'billing@example.org'}]
    def calendar(start, end, maximum):
        native_calls.append('calendar_list')
        return [{'id':'event-47', 'summary':'Observatory visit', 'start':start, 'end':end}]
    registry = ToolRegistry()
    register_email_plugin(registry, email_searcher=emails)
    register_calendar_plugin(registry, calendar_lister=calendar)
    args = {'start':'2026-11-15T09:00:00Z', 'end':'2026-11-16T09:00:00Z'}
    class Model:
        phase = 0
        errors = []
        def create(self, **kwargs):
            try: return self.respond(**kwargs)
            except Exception as error:
                self.errors.append(repr(error)); raise
        def respond(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['connector'],
                    'tool_names':['email_search','calendar_list'], 'required_tool_names':['email_search','calendar_list'],
                    'source_user_requested':True, 'connector_app_ids':['google-mail','google-calendar'], 'source_evidence':self.prompt})
            if self.phase == 2:
                if explicit and mode != 'off':
                    return call(code_mode.TOOL_NAME, {'tool_names':['email_search','calendar_list'],
                        'code': "mail = call_tool('email_search', {'query':'newer_than:1d'})\n"
                            + "agenda = call_tool('calendar_list', " + repr(args) + ")\n[mail['output'], agenda['output']]"})
                result = call('email_search', {'query':'newer_than:1d'})
                second = call('calendar_list', args)['content'][0]; second['id'] = 'calendar-second'
                result['content'].append(second)
                return result
            assert self.phase == 3, 'Unexpected model call'
            evidence = json.dumps(kwargs['messages'])
            if 'Paid 37 USD.' not in evidence or 'Observatory visit' not in evidence:
                raise RuntimeError(json.dumps({'calls':native_calls, 'protocol':[b for m in kwargs['messages'] for b in m.get('content', []) if isinstance(b, dict) and b.get('type') == 'tool_result'][-3:]}))
            return text('Your observatory email reports a payment of 37 USD. Your calendar lists an observatory visit.')
    model = Model(); samples = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['メールとカレンダーを確認', 'Consulta mi correo y calendario', 'Consulte mes courriels et mon calendrier']):
            model.phase = 0; model.prompt = prompt
            conversation = 'web:code-read-'+str(index)
            started = time.monotonic()
            response = client.post('/api/chat', json={'text':prompt, 'conversation_id':conversation})
            samples.append(time.monotonic()-started)
            if model.errors: print('MODEL ERRORS', json.dumps(model.errors))
            assert not model.errors, model.errors
            assert response.status_code == 200 and '37 USD' in response.json()['text'], response.text
            results = [r for e in runtime.store.list_conversation_events(conversation)
                if e.get('event_type') == 'conversation.chat_turn' for r in e.get('tool_results', [])]
            composed = [r for r in results if r['tool_name'] == code_mode.TOOL_NAME]
            assert bool(composed) == (mode != 'off' and explicit), results
            if composed:
                assert composed[-1]['status'] == 'completed', composed
                assert {r['tool_name'] for r in composed[-1]['output']['reads']} == {'email_search','calendar_list'}
            assert all(r['status'] == 'completed' for r in results), results
    assert native_calls == ['email_search','calendar_list']*3
    assert sum(samples)/len(samples) < 3 and max(samples) < 5, samples
    record_property('mean_read_workflow_seconds', sum(samples)/len(samples))


@pytest.mark.parametrize('prompt, late_body', [
    ('メールに記載された期限、金額、カレンダー上の一致を比較して。読み取り専用。', False),
    ('Compara las fechas límite y los importes del correo con las citas guardadas. Solo lectura.', True),
])
def test_large_email_batch_preserves_deadline_comparison_through_chat(tmp_path, monkeypatch, prompt, late_body):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, register_email_plugin, register_calendar_plugin
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    _configure_account_fixture(runtime, tmp_path, monkeypatch)

    records = {f'mail-{i}': f'Evidence token {i}. ' + 'a' * (3100 if late_body and i == 9 else 700) + (
        ' Observatory permit: 2027-02-04, 42 USD.' if i == 8 else
        ' Easement renewal: 2027-02-07, 73 USD.' if i == 9 else '') + ' z' * 4500 for i in range(10)}
    calls = []

    def search(query, limit):
        calls.append('search')
        return [{'id': identifier, 'subject': f'Source {index}'} for index, identifier in enumerate(records)]

    def read(identifier):
        calls.append(identifier)
        return {'id': identifier, 'subject': 'Account notice', 'body': records[identifier]}

    def calendar(start, end, maximum):
        calls.append('calendar')
        return [{'id': 'event-1', 'summary': 'Observatory permit calendar entry', 'start': '2027-02-04'}]

    registry = ToolRegistry()
    register_email_plugin(registry, email_searcher=search, email_reader=read)
    register_calendar_plugin(registry, calendar_lister=calendar)

    class Model:
        phase = 0
        evidence_received = False
        evidence_checks = None

        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['connector'],
                    'tool_names': ['email_search', 'email_read', 'calendar_list'],
                    'required_tool_names': ['email_search', 'email_read', 'calendar_list'],
                    'source_user_requested': True, 'connector_app_ids': ['google-mail', 'google-calendar'],
                    'source_evidence': prompt})
            if self.phase == 2:
                return call('code_mode', {'tool_names': ['email_search', 'email_read', 'calendar_list'], 'code':
                    "hits = call_tool('email_search', {'query':'fixture', 'limit':10})['output']['results']\n"
                    "calendar = call_tool('calendar_list', {'start':'2027-02-01', 'end':'2027-02-28'})['output']\n"
                    "messages = [call_tool('email_read', {'id':hit['id']})['output'] for hit in hits]\n"
                    "{'calendar':calendar,'messages':messages}"})
            evidence = json.dumps(kwargs['messages'], ensure_ascii=False)
            if self.phase == 3 and late_body:
                assert 'Easement renewal: 2027-02-07, 73 USD.' not in evidence
                payloads = [json.loads(part['text']) for message in kwargs['messages']
                    for block in message.get('content', []) if block.get('type') == 'tool_result'
                    for part in block['content'] if part.get('type') == 'text' and part['text'].startswith('{')]
                observation = next(row for payload in payloads
                    for row in (payload.get('output') or {}).get('native_observations', [])
                    if (row.get('output') or {}).get('id') == 'mail-9')
                message = observation['output']['message']
                assert message['body_truncated'] and ['message', 'body'] in observation['omitted_paths']
                request = {**observation['inspection_request'], 'offset': len(message['body']), 'length': 4000}
                assert request['json_pointer'] == '/message/body'
                inspection = call('code_mode', {'tool_names': ['inspect_retained_evidence'],
                    'code': f"call_tool('inspect_retained_evidence', {{'requests': [{request!r}]}})['output']"})
                inspection['content'][0]['id'] = 'deadline-inspection-program'
                return inspection
            self.evidence_checks = {
                'phase': self.phase == (4 if late_body else 3),
                'ten_receipts': all(f'Evidence token {i}.' in evidence for i in range(10)),
                'matched_obligation': 'Observatory permit: 2027-02-04, 42 USD.' in evidence,
                'unmatched_obligation': 'Easement renewal: 2027-02-07, 73 USD.' in evidence,
                'calendar_evidence': 'Observatory permit calendar entry' in evidence,
            }
            self.evidence_received = all(self.evidence_checks.values())
            if not self.evidence_received:
                return text('The evidence needed to compare the account records was unavailable.')
            return text('Observatory permit, 2027-02-04, 42 USD: calendar match. '
                        'Easement renewal, 2027-02-07, 73 USD: no calendar match.')

    model = Model()
    conversation = 'web:deadline-account-comparison'
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})

    assert response.status_code == 200
    assert model.evidence_received, (model.evidence_checks, response.text)
    assert '2027-02-04, 42 USD: calendar match' in response.json()['text']
    assert '2027-02-07, 73 USD: no calendar match' in response.json()['text']
    assert not response.json()['artifacts']
    assert calls == ['search', 'calendar', *records]
    turn = next(event for event in runtime.store.list_conversation_events(conversation)
                if event.get('event_type') == 'conversation.chat_turn')
    program = next(result['output'] for result in turn['tool_results'] if result['tool_name'] == 'code_mode')
    assert program['value_limit']['omitted'] and program['value_limit']['original_bytes'] > 64_000
    native = [result for result in turn['tool_results'] if result['tool_name'] == 'email_read']
    assert len(native) == 10 and all(result['status'] == 'completed' for result in native)
    inspections = [result for result in turn['tool_results'] if result['tool_name'] == 'inspect_retained_evidence']
    assert len(inspections) == int(late_body)
    if inspections:
        assert inspections[0]['status'] == 'completed'
    assert turn['response_fulfilled'] is True


@pytest.mark.parametrize('execution_layer', ['off', 'read_only'])
def test_selected_mail_batch_completes_as_one_bounded_read(tmp_path, monkeypatch, record_property, execution_layer):
    import base64
    from threading import Lock
    from types import SimpleNamespace
    from fastapi.testclient import TestClient
    from nullion import tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    _configure_account_fixture(runtime, tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', execution_layer)
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES', '64000')
    monkeypatch.setenv('NULLION_EMAIL_SEARCH_DETAIL_FETCH_LIMIT', '0')
    monkeypatch.setattr(tools, '_default_email_connector_provider_id', lambda principal: 'fixture')
    monkeypatch.setattr(tools, '_default_calendar_connector_provider_id', lambda principal: 'fixture')
    monkeypatch.setattr(tools, '_connector_connection_for_invocation', lambda *args: SimpleNamespace())
    monkeypatch.setattr(tools, '_connector_request_headers', lambda *args: {'Authorization': 'Bearer fixture'})
    monkeypatch.setattr(tools, '_email_messages_endpoint_for_provider', lambda *args: 'https://provider.invalid/messages')
    monkeypatch.setattr(tools, '_calendar_events_endpoint_for_provider', lambda *args: 'https://provider.invalid/calendar')
    ids = [f'first-mail-{i}' for i in range(10)]
    calls, owners, starts, samples = [], [], [], []
    active = peak = 0
    mutex = Lock()

    def network(invocation, *, provider_id, url, params):
        nonlocal active, peak
        if url.endswith('/calendar'):
            return {'items': [{'id': 'entry-42', 'start': {'date': '2027-07-14'}, 'summary': 'Garden permit'}]}
        if url.endswith('/messages'):
            return {'messages': [{'id': key} for key in ids]}
        identifier = url.rsplit('/', 1)[-1]
        with mutex:
            if active == 0 and len(calls) % 10 == 0:
                starts.append(time.monotonic())
            calls.append(identifier)
            owners.append((tools.tool_execution_owner()['invocation_id'], invocation.invocation_id))
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(.34)
            prefix = 4100 if execution_layer == 'off' and identifier == ids[-1] else 120
            body = f'Checked message {identifier}. ' + 'a' * prefix + (
                ' Garden permit, 2027-07-14, 42 USD.' if identifier == ids[-1] else '') + 'z'*6500
            return {'id': identifier, 'payload': {'mimeType': 'text/plain', 'body': {
                'data': base64.urlsafe_b64encode(body.encode()).decode(),
            }}}
        finally:
            with mutex:
                active -= 1

    monkeypatch.setattr(tools, '_connector_json_request', network)
    registry = tools.ToolRegistry()
    tools.register_connector_plugin(registry)

    class Model:
        phase = 0

        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            self.phase += 1
            if self.phase == 1:
                names = ['email_search', 'email_read', 'calendar_list']
                return call('request_tool_scope', {'capabilities': ['connector'], 'tool_names': names,
                    'required_tool_names': names, 'connector_app_ids': ['google-mail', 'google-calendar'],
                    'source_user_requested': True, 'source_evidence': self.prompt})
            if self.phase == 2:
                if execution_layer == 'off':
                    searches = call('email_search', {'query': 'fixture', 'limit': 10})
                    calendar = call('calendar_list', {'start': '2027-07-01', 'end': '2027-08-01'})
                    searches['content'].extend(calendar['content'])
                    return searches
                return call('code_mode', {'tool_names': ['email_search', 'calendar_list'], 'code':
                    "hits = call_tool('email_search', {'query':'fixture','limit':10})['output']['results']\n"
                    "calendar = call_tool('calendar_list', {'start':'2027-07-01','end':'2027-08-01'})['output']\n"
                    "{'ids':[row['id'] for row in hits],'calendar':calendar}"})
            if self.phase == 3:
                if execution_layer == 'off':
                    evidence = json.dumps(kwargs['messages'], ensure_ascii=False)
                    assert all(identifier in evidence for identifier in ids)
                    return call('email_read', {'ids': ids, 'independent_messages': True})
                batch = call('code_mode', {'tool_names': ['email_read'], 'code':
                    "call_tool('email_read', {'ids':[row['id'] for row in hits],"
                    "'independent_messages':True}, on_error='return')['output']"})
                batch['content'][0]['id'] = 'independent-mail-program'
                return batch
            if self.phase == 4:
                samples.append(time.monotonic() - starts[-1])
            evidence = json.dumps(kwargs['messages'], ensure_ascii=False)
            if execution_layer == 'off' and self.phase == 4:
                assert 'inspect_retained_evidence' in {tool['name'] for tool in kwargs['tools']}
                assert 'Garden permit, 2027-07-14, 42 USD.' not in evidence
                payloads = [json.loads(part['text']) for message in kwargs['messages']
                    for block in message.get('content', []) if block.get('type') == 'tool_result'
                    for part in block['content'] if part.get('type') == 'text' and part['text'].startswith('{')]
                batch_view = next(payload['output'] for payload in payloads
                    if isinstance(payload.get('output'), dict) and payload['output'].get('result_kind') == 'email_message_batch')
                retained = batch_view['messages'][-1]['inspection_request']
                return call('inspect_retained_evidence', {'requests': [{**retained, 'offset': 4000, 'length': 500}]})
            assert self.phase == (5 if execution_layer == 'off' else 4)
            assert all(f'Checked message {identifier}.' in evidence for identifier in ids)
            assert 'Garden permit, 2027-07-14, 42 USD.' in evidence and 'entry-42' in evidence
            return text('Garden permit, 2027-07-14, 42 USD: calendar match.')

    model = Model()
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(('メールと予定を照合して。', 'Compara el correo con las citas.')):
            model.phase = 0
            model.prompt = prompt
            conversation = f'web:selected-mail-batch-{index}'
            result = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
            assert result.status_code == 200 and '42 USD: calendar match' in result.json()['text'], result.text
            events = runtime.store.list_conversation_events(conversation)
            turn = next(e for e in events if e['event_type'] == 'conversation.chat_turn')
            assert turn['response_fulfilled'] is True
            batch, = [r for r in turn['tool_results'] if r['tool_name'] == 'email_read']
            rows = batch['output']['messages']
            assert batch['status'] == 'completed' and batch['output']['complete'] is True
            assert [row['id'] for row in rows] == ids and all(row['status'] == 'completed' for row in rows)
            inspections = [r for r in turn['tool_results'] if r['tool_name'] == 'inspect_retained_evidence']
            assert len(inspections) == int(execution_layer == 'off')
            if inspections:
                assert inspections[0]['status'] == 'completed'
            else:
                programs = [r['output'] for r in turn['tool_results'] if r['tool_name'] == 'code_mode']
                assert programs[-1]['transfer_usage']['transferred_bytes'] < 64000
            assert [child for owner, child in sorted(owners[-10:], key=lambda p: int(p[1].rsplit(':', 1)[-1]))] == [
                f"{batch['invocation_id']}:message:{i}" for i in range(10)]
            assert {owner for owner, _ in owners[-10:]} == {batch['invocation_id']}
            budget = next(e for e in events if e['event_type'] == 'conversation.research_budget')
            assert budget['usage']['research_native_calls'] == 12
    assert len(calls) == 20 and peak == 3
    assert len(samples) == 2 and sum(samples) / 2 < 2.5 and max(samples) < 3, samples
    record_property('mean_selected_mail_batch_seconds', sum(samples) / len(samples))


def test_email_search_overlaps_metadata_reads_through_chat(tmp_path, monkeypatch, record_property):
    from contextlib import contextmanager
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import socket
    from threading import Lock, Thread
    from types import SimpleNamespace
    from urllib.parse import urlsplit
    from fastapi.testclient import TestClient
    from nullion import tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    _configure_account_fixture(runtime, tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'off')
    monkeypatch.setenv('NULLION_EMAIL_SEARCH_DETAIL_FETCH_LIMIT', '5')
    monkeypatch.setattr(tools, '_default_email_connector_provider_id', lambda principal: 'fixture')

    mutex = Lock()
    active = 0
    peak = 0
    received = []
    observed_owners = []
    started = []
    resolve_calls = []
    ordinary_resolver = socket.getaddrinfo

    class ProviderOrigin(BaseHTTPRequestHandler):
        def reply(self, payload, status=200):
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            nonlocal active, peak
            path = urlsplit(self.path).path
            if path == '/messages':
                started.append(time.monotonic())
                self.reply({'messages': [{'id': f'mail-{i}'} for i in range(6)]})
                return
            identifier = path.rsplit('/', 1)[-1]
            with mutex:
                received.append(identifier)
                observed_owners.append((self.headers.get('X-Fixture-Read-Owner'), self.headers.get('Host')))
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.38 if identifier == 'mail-0' else 0.34)
                if identifier == 'mail-2':
                    self.reply({'error': 'fixture metadata failure'}, status=503)
                else:
                    self.reply({'id': identifier, 'payload': {'headers': [
                        {'name': 'Subject', 'value': f'Subject {identifier}'},
                    ]}})
            finally:
                with mutex:
                    active -= 1

        def log_message(self, *_args):
            pass

    @contextmanager
    def gateway_server():
        with ThreadingHTTPServer(('127.0.0.1', 0), ProviderOrigin) as server:
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                yield server
            finally:
                server.shutdown()
                thread.join(timeout=2)

    registry = tools.ToolRegistry()
    tools.register_connector_plugin(registry)
    durations = []

    class Model:
        phase = 0

        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['connector'], 'tool_names': ['email_search'],
                    'required_tool_names': ['email_search'], 'connector_app_ids': ['google-mail'],
                    'source_user_requested': True, 'source_evidence': self.prompt})
            if self.phase == 2:
                return call('email_search', {'query': 'fixture', 'limit': 6})
            durations.append(time.monotonic() - started[-1])
            assert 'Subject mail-4' in json.dumps(kwargs['messages'])
            return text('Read six search results. Subject mail-4 was available.')

    model = Model()
    with gateway_server() as gateway, TestClient(
            create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        port = gateway.server_address[1]
        base = f'http://pinned-gateway.invalid:{port}'

        def resolve(raw_url):
            parsed = urlsplit(raw_url)
            assert socket.getaddrinfo is ordinary_resolver
            resolve_calls.append(parsed.path)
            assert parsed.hostname == 'pinned-gateway.invalid' and parsed.port == port
            return tools._WebFetchResolution(parsed.hostname, (
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('127.0.0.1', port)),
            ))

        def headers(_connection, _provider_id):
            return {'X-Fixture-Read-Owner': tools.tool_execution_owner()['invocation_id']}

        monkeypatch.setattr(tools, '_connector_connection_for_invocation', lambda *args: SimpleNamespace(provider_profile=base))
        monkeypatch.setattr(tools, '_connector_request_headers', headers)
        monkeypatch.setattr(tools, '_email_messages_endpoint_for_provider', lambda *args: base + '/messages')
        monkeypatch.setattr(tools, '_resolve_web_fetch_resolution', resolve)
        for index, prompt in enumerate(('メールを確認して。', 'Revisa el correo.', 'Vérifiez les messages.')):
            model.phase = 0
            model.prompt = prompt
            conversation = f'web:parallel-email-metadata-{index}'
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
            assert response.status_code == 200 and 'Subject mail-4' in response.json()['text'], response.text
            turn = next(event for event in runtime.store.list_conversation_events(conversation)
                        if event.get('event_type') == 'conversation.chat_turn')
            search = next(result for result in turn['tool_results'] if result['tool_name'] == 'email_search')
            records = search['output']['results']
            assert [record['id'] for record in records] == [f'mail-{i}' for i in range(6)]
            assert 'subject' not in records[2] and 'subject' not in records[5]
            assert search['status'] == 'completed' and turn['response_fulfilled'] is True
            assert set(observed_owners[-5:]) == {(search['invocation_id'], f'pinned-gateway.invalid:{port}')}
    assert len(durations) == 3 and sum(durations) / 3 < 1.4 and max(durations) < 1.6, durations
    assert peak == 3 and len(received) == 15
    assert len(resolve_calls) == 18
    record_property('mean_email_search_elapsed_seconds', sum(durations) / len(durations))


@pytest.mark.parametrize('tls', [False, True])
def test_pinned_opener_keeps_origin_through_redirects_and_tls(tls, tmp_path, monkeypatch):
    from datetime import timedelta
    from http.server import BaseHTTPRequestHandler
    import socket
    import ssl
    import urllib.error
    import urllib.request
    from urllib.parse import urlsplit
    from nullion import tools
    from tests.e2e.test_provider_tool_matrix_contracts import _MockToolWebServer

    host = 'pinned-redirect.invalid'
    seen, resolved, sni = [], [], []
    expired = False

    class Origin(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal expired
            seen.append((self.path, self.headers.get('Host')))
            if self.path != '/base/result':
                self.send_response(302)
                location = f'{scheme}://{host}:1/base/result' if self.path == '/base/wrong-port' else '/base/result'
                self.send_header('Location', location)
                if self.path == '/base/expired':
                    expired = True
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'pinned result')

        def log_message(self, *_args):
            pass

    server = _MockToolWebServer(Origin, 0)
    port = server.server.server_address[1]
    scheme = 'https' if tls else 'http'
    base = f'{scheme}://{host}:{port}/base'
    if tls:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, host)])
        now = datetime.now(UTC)
        certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(minutes=5))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(host)]), critical=False)
            .sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM))
        cert_path, key_path = tmp_path/'certificate.pem', tmp_path/'key.pem'
        cert_path.write_bytes(certificate)
        key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert_path, key_path)
        server_context.set_servername_callback(lambda _socket, name, _context: sni.append(name))
        server.server.socket = server_context.wrap_socket(server.server.socket, server_side=True)
        monkeypatch.setattr(tools, '_web_fetch_tls_context',
                            lambda: ssl.create_default_context(cadata=certificate.decode()))

    def resolve(raw_url):
        parsed = urlsplit(raw_url)
        resolved.append((parsed.hostname, parsed.port, parsed.path))
        if parsed.hostname == host and parsed.path in ('/base/start', '/base/wrong-port', '/base/expired'):
            return tools._WebFetchResolution(host, (
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('127.0.0.1', port)),
            ))
        return None

    monkeypatch.setattr(tools, '_resolve_web_fetch_resolution', resolve)
    monkeypatch.setattr(tools, 'tool_execution_remaining_seconds', lambda: 0 if expired else 2)
    with server:
        opener = tools._build_web_fetch_opener()
        request = urllib.request.Request(base + '/start')
        request._nullion_allowed_base_urls = (base,)
        with opener.open(request, timeout=2) as response:
            assert response.read() == b'pinned result'
        wrong_port = urllib.request.Request(base + '/wrong-port')
        wrong_port._nullion_allowed_base_urls = (base,)
        with pytest.raises(urllib.error.HTTPError) as redirect:
            opener.open(wrong_port, timeout=2)
        redirect.value.close()
        assert redirect.value.code == 302
        with pytest.raises(urllib.error.URLError, match='private network'):
            opener.open(f'{scheme}://127.0.0.1:{port}/base/result', timeout=2)
        past_deadline = urllib.request.Request(base + '/expired')
        past_deadline._nullion_allowed_base_urls = (base,)
        past_deadline._nullion_connector_read = True
        with pytest.raises(TimeoutError, match='Connector read budget expired'):
            opener.open(past_deadline, timeout=2)

    assert seen == [(path, f'{host}:{port}')
                    for path in ('/base/start', '/base/result', '/base/wrong-port', '/base/expired')]
    assert resolved == [(host, port, '/base/start'), (host, port, '/base/wrong-port'),
                        ('127.0.0.1', port, '/base/result'), (host, port, '/base/expired')]
    assert sni == ([host] * 4 if tls else [])


def test_gate_preserves_real_pending_read_approval_without_running_handler(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    from nullion.preferences import save_preferences
    save_preferences({'sentinel_mode':'ask_all'})
    called = []
    registry = ToolRegistry()
    registry.register(ToolSpec('protected_read', 'Read a protected source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, True, 5, input_schema={'type':'object'}),
        lambda invocation: called.append(invocation) or ToolResult(invocation.invocation_id,
            invocation.tool_name, 'completed', {'text':'protected value'}))
    class Model:
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            return protocol if protocol is not None else call('protected_read', {})
    result = AgentOrchestrator(model_client=Model()).run_turn(
        conversation_id='web:protected-read', principal_id='workspace_admin', user_message='確認',
        conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=2)
    assert result.suspended_for_approval and result.approval_id, [(r.tool_name,r.status,r.output) for r in result.tool_results]
    assert not called
    approval = runtime.store.get_approval_request(result.approval_id)
    assert approval is not None and approval.status.value == 'pending'


def test_primary_code_layer_reduces_dependent_read_roundtrips(tmp_path, monkeypatch, record_property):
    """A slow external model makes re-entering it per message measurably costly."""
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, register_email_plugin
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, OPERATOR_PERMISSION_PRINCIPAL
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    for kind in (BoundaryKind.ACCOUNT_ACCESS, BoundaryKind.OUTBOUND_NETWORK):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-'+kind.value,
            OPERATOR_PERMISSION_PRINCIPAL, kind, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    records = [{'id':'mail-'+str(i),'subject':'Record '+str(i),'from':'source@example.org'} for i in range(3)]
    native_calls = []
    def search(query, limit):
        native_calls.append('search'); return records
    def read(identifier):
        native_calls.append(identifier)
        return {'id':identifier,'subject':identifier,'body':'Verified amount 17 USD.'}
    registry = ToolRegistry()
    register_email_plugin(registry,email_searcher=search,email_reader=read)
    class Model:
        rounds = 0
        errors = []
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            time.sleep(.25)  # External provider boundary, not a Nullion mock.
            self.rounds += 1
            names = {t['name'] for t in kwargs.get('tools', [])}
            if self.rounds == 1:
                if 'code_mode' in names:
                    assert 'email_search' not in names and 'email_read' not in names
                    return call('code_mode',{'tool_names':['email_search','email_read'],
                        'code':"hits = call_tool('email_search', {'query':'newer_than:1d'})['output']['results']\n"
                            "bodies = [call_tool('email_read', {'id':hit['id']})['output']['message']['body'] for hit in hits]\n"
                            + ("" if getattr(self,"omit_final_expression",False) else "bodies")})
                return call('email_search',{'query':'newer_than:1d'})
            if 'code_mode' not in names and self.rounds <= 4:
                return call('email_read',{'id':records[self.rounds-2]['id']})
            assert 'Verified amount 17 USD.' in json.dumps(kwargs['messages'])
            return text('All three emails report 17 USD each.')
    timings = {}
    for mode in ['off','on']:
        monkeypatch.setenv('NULLION_CODE_MODE',mode)
        samples = []
        for index, prompt in enumerate(['メール本文を確認','Lee los mensajes','Lis les messages']):
            model = Model(); model.omit_final_expression = index==1; started = time.monotonic()
            result = AgentOrchestrator(model_client=model).run_turn(
                conversation_id=f'web:roundtrips-{mode}-{index}',principal_id='workspace_admin',
                user_message=prompt,conversation_history=[],tool_registry=registry,
                policy_store=runtime.store,approval_store=runtime.store)
            samples.append(time.monotonic()-started)
            assert model.rounds == (2 if mode != 'off' else 5)
            assert '17 USD' in result.final_text and not result.suspended_for_approval
            reads = [r for r in result.tool_results if r.tool_name in {'email_search','email_read'}]
            assert len(reads) == 4 and all(r.status == 'completed' for r in reads)
            if mode != 'off':
                programs = [r for r in result.tool_results if r.tool_name == 'code_mode']
                assert len(programs) == 1 and len(programs[0].output['reads']) == 4
        timings[mode] = sum(samples)/len(samples)
        record_property(mode+'_mean_seconds',timings[mode])
    assert native_calls == ['search','mail-0','mail-1','mail-2']*6
    assert timings['on'] < timings['off']*.85, timings
    assert timings['on'] < 2, timings
