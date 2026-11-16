"""Real chat/source receipts; the model and external account APIs are fixtures."""
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('mode', ['off', 'on'])
def test_source_summary_recovery_preserves_cross_source_facts_without_extra_account_audit(tmp_path, monkeypatch, mode):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, GLOBAL_PERMISSION_PRINCIPAL, PolicyMode
    from nullion.preferences import save_preferences
    from nullion.response_sanitizer import _email_read_summary
    from nullion.tools import ToolRegistry, register_email_plugin, register_calendar_plugin
    from nullion.web_app import create_app
    from datetime import datetime, UTC
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch); monkeypatch.setenv('NULLION_CODE_MODE', mode)
    from nullion import connections, connector_prompt_context
    monkeypatch.setattr(connections, '_CONNECTIONS_PATH', tmp_path/'connections.json')
    connections.save_connection_registry({'connections':[{'connection_id':'fixture-account',
        'workspace_id':'workspace_admin','provider_id':'fixture_connector_provider',
        'display_name':'Fixture account','active':True,'permission_mode':'read'}]})
    for key in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE'):
        monkeypatch.setattr(connector_prompt_context,key,None)
    for key in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT'):
        monkeypatch.setattr(connector_prompt_context,key,0.0)
    save_preferences({'outbound_request_mode': 'allow_all'})
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-'+kind.value,
            GLOBAL_PERMISSION_PRINCIPAL, kind, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    registry = ToolRegistry(); reads = []
    message = {'subject': 'Renewal deadline', 'from': 'Fixture service', 'date': '2026-11-15',
        'body': 'The subscription renews for USD 45 on 2026-11-21. Cancel before that date to avoid renewal.'}
    def mail_reader(identifier):
        reads.append('email_read')
        return message
    def calendar_reader(start, end, maximum):
        reads.append('calendar_list')
        return [{'summary': 'Renewal reminder', 'start': {'date': '2026-11-21'}, 'status': 'confirmed'}]
    register_email_plugin(registry, email_reader=mail_reader, email_searcher=lambda query, limit: [])
    register_calendar_plugin(registry, calendar_lister=calendar_reader)
    arguments = {'email_read': {'id': 'message-37'},
        'calendar_list': {'start': '2026-11-20T00:00:00Z', 'end': '2026-11-22T00:00:00Z'}}
    class Model:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['connector'], 'tool_names': ['email_read', 'calendar_list'],
                    'required_tool_names': ['email_read', 'calendar_list'], 'source_user_requested': True,
                    'connector_app_ids': ['google-mail','google-calendar'], 'source_evidence': self.prompt})
            if self.phase == 2:
                if mode == 'on':
                    return call('code_mode', {'tool_names': ['email_read', 'calendar_list'],
                        'code': "mail=call_tool('email_read', " + repr(arguments['email_read']) + ")\ncalendar=call_tool('calendar_list', "
                            + repr(arguments['calendar_list']) + ")\n{'mail':mail['output'],'calendar':calendar['output']}"})
                return {'stop_reason': 'tool_use', 'content': [call(n, arguments[n])['content'][0] for n in ('email_read', 'calendar_list')]}
            if self.phase == 3:
                return text(_email_read_summary({'message': message}))
            assert self.phase == 4, 'A source-summary rewrite must not start another account audit'
            wire = json.dumps(kwargs['messages'])
            assert 'source-based summary' in wire and 'all completed source types' in wire
            return text('The email states renewal is USD 45 on 2026-11-21; cancellation is due before that date. '
                'A matching calendar reminder, Renewal reminder, already exists. Actual posting is unconfirmed.')
    model = Model(); samples = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['予定とメールの期限を要約してください。', 'Resume los plazos del correo y el calendario.', 'لخص مواعيد البريد والتقويم.']):
            model.phase = 0; model.prompt = prompt; before = len(reads); cid = f'web:account-source-summary-{index}'
            start = time.monotonic()
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
            samples.append(time.monotonic()-start)
            assert response.status_code == 200 and 'USD 45 on 2026-11-21' in response.json()['text'], response.text
            assert 'matching calendar reminder' in response.json()['text']
            assert reads[before:] == ['email_read', 'calendar_list']
            turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True
    assert sum(samples)/len(samples) < 2 and max(samples) < 4, samples
