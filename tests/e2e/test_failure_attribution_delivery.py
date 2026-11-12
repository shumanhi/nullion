"""Actual browser/source, review, Web UI and Telegram HTTP delivery boundaries."""
import asyncio
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import urllib.error

import pytest

from tests.e2e.test_reviewed_failure_boundaries import isolated
from tests.e2e.test_universal_outcome_review_boundary import text
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')

COPIES = [
    ('Compara las opciones de esta fuente.', 'Opción A: 28; valoración 4,1 de 17 opiniones.',
     'La comparación con otras opciones sigue incompleta.'),
    ('この情報源の候補を比較してください。', '候補 A: 価格 28、評価 4.1、17 件。',
     '他の候補との比較はまだ完了していません。'),
]


class Provider:
    def __init__(self, calls, names, finding, limitation, *, variant):
        self.calls = list(calls)
        self.names = names
        self.finding = finding
        self.limitation = limitation
        self.variant = variant
        self.reviews = 0

    def create(self, **kw):
        auxiliary = workflow_provider_reply(kw, completion=False)
        if auxiliary is not None:
            return auxiliary
        system = str(kw.get('system') or '')
        if 'material-requirements planner' in system:
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            available = {row['name'] for row in packet['current_turn_tool_catalog']}
            return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                **fixture_source_plan(packet, scope='evidence_summary'),
                'material_requirements': [{'requirement': 'Compare the selected current source records',
                    'requirement_kind': 'fact', 'evidence_subject_id': 'selection',
                    'eligible_evidence_tool_names': [name for name in self.names if name in available]}]})
        if 'semantic completion reviewer' in system:
            self.reviews += 1
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            failures = [r for r in packet['tool_evidence'] if r['status'] == 'failed']
            assert failures
            positives = [r for r in packet['tool_evidence'] if r['status'] == 'completed'
                and r['tool_name'] in {'browser_extract_text', 'web_fetch'}]
            findings = []
            if positives:
                findings.append({'text': self.finding, 'evidence_invocation_ids': [positives[-1]['invocation_id']]})
            # Exercise the historical receipt failure without a limitation row:
            # it must not become a current operational cause by tool-name overlap.
            if self.variant == 'public_limit':
                findings.append({'text': self.limitation, 'kind': 'limitation',
                    'evidence_invocation_ids': [positives[-1]['invocation_id']]})
            bound = [failures[-1]['invocation_id']] if self.variant in {'bound_failure', 'entire_failure'} else []
            return text({'disposition': 'blocked' if bound else 'retry',
                **fixture_source_assessment(packet, conclusion='limited'),
                'all_material_requirements_enumerated': True,
                'unresolved_requirements': ['Internal correction: repair the comparison and confirm current state.'],
                'user_facing_failure': 'Private reviewer instructions; rewrite the draft.',
                'retry_tool_names': [], 'supported_findings': findings,
                'material_requirements': [{'requirement_id': r['requirement_id'], 'satisfied': False,
                    'draft_covered': False, 'evidence_invocation_ids': [],
                    **({'blocking_failure_invocation_ids': bound} if bound else {})}
                    for r in packet['material_requirement_contract']]})
        names = {r['name'] for r in kw.get('tools', [])}
        if 'select_attachment_format' in names:
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'format',
                'name': 'select_attachment_format', 'input': {'extension': None, 'confidence': 1}}]}
        if not names:
            return text({})
        if self.calls:
            name, arguments = self.calls.pop(0)
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use',
                'id': f'call-{len(self.calls)}', 'name': name, 'input': arguments}]}
        return text((self.finding + '\n\n' if self.variant != 'entire_failure' else '') +
            ('A required browser step failed.' if self.variant in {'bound_failure','entire_failure'} else self.limitation))


@pytest.mark.parametrize('family', ['browser', 'http'])
@pytest.mark.parametrize('surface', ['web_api', 'web_ui', 'telegram'])
@pytest.mark.parametrize('copy', COPIES)
def test_failed_attempt_does_not_replace_the_actual_remaining_gap(tmp_path, monkeypatch, family, surface, copy):
    run_case(tmp_path, monkeypatch, family=family, surface=surface, copy=copy, variant='coarse_gap')


@pytest.mark.parametrize('variant', ['bound_failure', 'entire_failure', 'missing_readback', 'public_limit'])
def test_remaining_failure_and_uncertain_state_stay_distinct(tmp_path, monkeypatch, variant):
    run_case(tmp_path, monkeypatch, family='browser', surface='web_api', copy=COPIES[1], variant=variant)


def run_case(tmp_path, monkeypatch, *, family, surface, copy, variant):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry, register_web_fetch_tool
    from nullion.web_app import create_app
    from nullion import tools, chat_store
    runtime, home = isolated(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED', '1' if family == 'http' else '0')
    prompt, finding, limitation = copy
    source_reads = []
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            source_reads.append(self.path)
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(('<html><body><section id="empty"></section><main id="observations">'
                + finding + ' Initial state: A. Only one candidate is displayed.</main>'
                '<button id="apply" onclick="this.dataset.state=\'B\'">Apply</button></body></html>').encode())
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{server.server_port}/'
    registry = ToolRegistry()
    if family == 'browser':
        register_browser_tools(registry)
        names = ['browser_navigate', 'browser_extract_detail', 'browser_extract_text']
        calls = [('browser_navigate', {'url': url}),
            ('browser_extract_detail', {'selector': '#empty'})]
        if variant != 'entire_failure':
            calls.append(('browser_extract_text', {'selector': '#observations'}))
        if variant == 'bound_failure':
            calls.append(('browser_extract_detail', {'selector': '#empty'}))
        if variant == 'missing_readback':
            names.append('browser_click_element')
            calls.append(('browser_click_element', {'selector': '#apply'}))
        required = ['browser_navigate']
        scope = {'capabilities': ['web'], 'tool_names': names, 'required_tool_names': required}
    else:
        names = ['web_fetch']
        calls = [('web_fetch', {'url': 'https://example.org/unavailable'}),
            ('web_fetch', {'url': 'https://example.org/records'})]
        def fetch(request_url, timeout):
            source_reads.append(request_url)
            if request_url.endswith('/unavailable'):
                raise urllib.error.URLError('Private transport diagnostic')
            return {'url': request_url, 'text': finding, 'status_code': 200}
        monkeypatch.setattr(tools, '_default_web_fetcher', fetch)
        register_web_fetch_tool(registry, web_fetcher=fetch)
        scope = {'capabilities': ['web'], 'tool_names': names}
    calls.insert(0, ('request_tool_scope', scope))
    provider = Provider(calls, names, finding, limitation, variant=variant)
    runtime.active_tool_registry = registry
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, OPERATOR_PERMISSION_PRINCIPAL
    from nullion.approvals import create_permission_grant
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('source', OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    for name in names:
        runtime.store.add_permission_grant(create_permission_grant(approval_id='allow-' + name,
            principal_id=OPERATOR_PERMISSION_PRINCIPAL, permission='tool:' + name,
            granted_by='fixture', granted_at=datetime.now(UTC)))
    runner = AgentOrchestrator(model_client=provider)
    request = prompt + ' ' + (url if family == 'browser' else 'https://example.org/records')
    cid = 'telegram:42' if surface == 'telegram' else 'web:attribution'
    try:
        if surface == 'telegram':
            from nullion.config import NullionSettings, TelegramSettings
            from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
            settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',
                operator_chat_id='42', chat_enabled=True))
            answer = run_platform_chat_request(runtime, PlatformChatRequest(platform='telegram',
                text=request, conversation_id=cid, chat_id='42', settings=settings,
                model_client=provider, agent_orchestrator=runner, append_activity_trace=False)).text
            from tests.e2e.test_health_action_acknowledgements import telegram_boundary
            from nullion.telegram_app import _deliver_reply
            from telegram import Bot, Message
            with telegram_boundary() as (base, messages):
                async def deliver():
                    async with Bot('123456:synthetic', base_url=base) as bot:
                        msg = Message.de_json({'message_id': 7, 'date': 1,
                            'chat': {'id': 42, 'type': 'private'}, 'text': request}, bot)
                        await _deliver_reply(msg, answer)
                asyncio.run(deliver())
            wire = [body['text'] for method, body in messages if method == 'sendMessage']
            assert len(wire) == 1 and finding in wire[0]
        elif surface == 'web_api':
            with TestClient(create_app(runtime, runner, registry)) as client:
                response = client.post('/api/chat', json={'text': request, 'conversation_id': cid})
                assert response.status_code == 200, response.text
                answer = response.json()['text']
        else:
            from playwright.sync_api import sync_playwright, expect
            from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port
            with _UvicornThread(create_app(runtime, runner, registry), _free_port()) as app:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.goto(app.base_url, wait_until='domcontentloaded')
                    page.locator('#user-input').fill(request)
                    with page.expect_response(lambda r: '/api/chat/history/' in r.url
                            and r.url.endswith('/message') and r.request.post_data_json.get('role') == 'bot',
                            timeout=30000) as saved:
                        page.locator('#user-input').press('Enter')
                    assert saved.value.ok
                    expect(page.locator('#messages')).to_contain_text(finding, timeout=10000)
                    answer = page.locator('#messages').inner_text()
                    page.reload(wait_until='domcontentloaded')
                    expect(page.locator('#messages')).to_contain_text(finding, timeout=10000)
                    browser.close()
        turns = [e for e in runtime.store.list_conversation_events() if e.get('event_type') == 'conversation.chat_turn'
            and e.get('user_message') == request]
        assert turns
        turn = turns[-1]
        assert finding in turn['assistant_reply'] if variant != 'entire_failure' else turn['assistant_reply'] in answer
        reviews = [e for e in runtime.store.list_conversation_events(turn['conversation_id'])
            if e.get('event_type') == 'conversation.completion_review']
        assert not reviews
        if variant != 'entire_failure':
            assert finding in answer and any(r['status'] == 'completed'
                and r['tool_name'] in {'browser_extract_text', 'web_fetch'} for r in turn['tool_results'])
        if variant == 'public_limit':
            assert limitation in answer
        assert all('Private' not in text and 'Internal correction' not in text and 'Rejected draft' not in text
            for text in (answer, turn['assistant_reply']))
        assert any(r['status'] == 'failed' and r['tool_name'] in {'browser_extract_detail', 'web_fetch'}
            for r in turn['tool_results'])
        if variant == 'missing_readback':
            assert turn['tool_results'][-1]['tool_name'] == 'browser_click_element'
            assert turn['tool_results'][-1]['status'] == 'completed'
        assert source_reads
    finally:
        registry.run_cleanup_hooks()
        server.shutdown()
        server.server_close()
