"""Correct composition in the existing review; real routing, store and delivery."""
import asyncio
import json
import os

import pytest

from tests.report_presentation_fixture import report_fixture
from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('surface', ['web_api', 'web_ui', 'telegram'])
@pytest.mark.parametrize('prompt', ['選択した記録を簡潔に報告してください。', 'Resume los registros de forma concisa.'])
@pytest.mark.parametrize('outcome', ['complete', 'partial'])
def test_reviewer_corrects_composition_once_without_reopening_completed_source_work(tmp_path, monkeypatch, surface, prompt, outcome, comparison=False):
    from nullion import web_app, preferences
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_text import REPORT_MESSAGE_FORMAT, render_report_message
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
    from nullion.config import NullionSettings, TelegramSettings
    runtime = setup_runtime(tmp_path, monkeypatch)
    for key in ['NULLION_MEMORY_ENABLED', 'NULLION_DOCTOR_ENABLED', 'NULLION_TASK_DECOMPOSITION_ENABLED',
                'NULLION_MULTI_AGENT_ENABLED', 'NULLION_CHAT_STARTUP_WARMUP']:
        monkeypatch.setenv(key, '0')
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'none.env'))
    monkeypatch.setattr(preferences, 'load_preferences', lambda: preferences.Preferences(markdown_style='plain',
        emoji_level='expressive' if outcome=='partial' or comparison else 'none'))
    if comparison:
        from tests.review_shopping_fixture import shopping_fixture
        message,verbose,source=shopping_fixture()
    else: message, verbose, source = report_fixture()
    source['value'] = 7
    expected = render_report_message(message)
    limitation = 'One requested value remains unconfirmed.'
    if outcome == 'partial': expected += '\n\n' + limitation
    registry = ToolRegistry(); reads = []
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', source)
    registry.register(ToolSpec('opaque_read', 'Read selected records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), read)
    runtime.active_tool_registry = registry
    class Provider:
        producer_calls = 0
        reviews = 0
        def create(self, **kwargs):
            system = kwargs.get('system') or ''
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                if 'semantic completion reviewer' in system:
                    self.reviews += 1
                    packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                    reply = json.loads(protocol['content'][0]['text'])
                    ids = [row['requirement_id'] for row in reply['material_requirements']]
                    refs = list(dict.fromkeys(ref for row in reply['material_requirements']
                                             for ref in row['evidence_invocation_ids']))
                    assert packet['draft_final_response'] != expected
                    assert 'corrected_findings' in system
                    reply.update(reply_target='corrected_findings',
                        source_presentation_valid=None if packet.get('source_answer_selection') is None else True,
                        supported_findings=[{'text': {'format': REPORT_MESSAGE_FORMAT, 'message': message},
                            'presentation': 'paragraph', 'requirement_ids': ids, 'evidence_invocation_ids': refs}])
                    if outcome == 'partial':
                        from jsonschema import validate, ValidationError
                        from copy import deepcopy
                        reply.update(disposition='blocked', reply_target='draft', source_presentation_valid=False,
                            unresolved_requirements=['The requested value is unavailable.'], user_facing_failure='Private diagnostic')
                        for row in reply['material_requirements']: row['satisfied'] = row['draft_covered'] = False
                        reply['supported_findings'].append({'kind':'limitation','text':limitation,'presentation':'paragraph',
                            'requirement_ids':ids,'evidence_invocation_ids':refs})
                        validate(reply, packet['review_response_schema'])
                        legacy = deepcopy(reply); legacy['supported_findings'][0]['text'] = verbose[:1900]
                        with pytest.raises(ValidationError): validate(legacy, packet['review_response_schema'])
                    return text_response(json.dumps(reply))
                return protocol
            names = {row['name'] for row in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            if not names: return text_response('{}')
            if 'opaque_read' not in names and 'request_tool_scope' in names:
                return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['opaque_read']})
            self.producer_calls += 1
            if not reads: return tool_response('opaque_read', {})
            if outcome == 'partial':
                from copy import deepcopy
                draft = deepcopy(message); draft['detail_level'] = 'detailed'
                return text_response(json.dumps({'format':REPORT_MESSAGE_FORMAT,'message':draft,'text':verbose}))
            return text_response(verbose)
    provider = Provider(); runner = AgentOrchestrator(model_client=provider)
    cid = 'telegram:42' if surface == 'telegram' else 'web:review-correction'
    if surface == 'telegram':
        settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic', operator_chat_id='42', chat_enabled=True))
        result = run_platform_chat_request(runtime, PlatformChatRequest(platform='telegram', text=prompt,
            conversation_id=cid, chat_id='42', settings=settings, model_client=provider,
            agent_orchestrator=runner, append_activity_trace=False))
        answer = result.text
        from telegram import Bot, Message
        from nullion.telegram_app import _deliver_reply
        from tests.e2e.test_health_action_acknowledgements import telegram_boundary
        with telegram_boundary() as (url, calls):
            async def deliver():
                async with Bot('123456:synthetic', base_url=url) as bot:
                    received = Message.de_json({'message_id': 1, 'date': 1,
                        'chat': {'id': 42, 'type': 'private'}, 'text': prompt}, bot)
                    await _deliver_reply(received, answer)
            asyncio.run(deliver())
        wire = [row['text'] for method, row in calls if method == 'sendMessage']
        assert len(wire) == 1 and all(value in wire[0] for value in (
            ('$9.98','$9.88','$5.97') if comparison else ('$720','$760')))
        assert '**' not in wire[0] and 'Historical detail' not in wire[0]
    elif surface == 'web_api':
        from fastapi.testclient import TestClient
        with TestClient(web_app.create_app(runtime, runner, registry)) as api:
            response = api.post('/api/chat', json={'text': '/chat '+prompt, 'conversation_id': cid})
            assert response.status_code == 200, response.text
            answer = response.json()['text']
    else:
        from playwright.sync_api import sync_playwright, expect
        from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port
        with _UvicornThread(web_app.create_app(runtime, runner, registry), _free_port()) as server, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True); page = browser.new_page()
            page.goto(server.base_url, wait_until='domcontentloaded')
            page.locator('#user-input').fill('/chat '+prompt)
            with page.expect_response(lambda response: '/api/chat/history/' in response.url
                    and response.url.endswith('/message') and response.request.post_data_json.get('role') == 'bot'):
                page.locator('#user-input').press('Enter')
            expect(page.locator('#messages')).to_contain_text('CedarBond' if comparison else 'Final posting is unconfirmed')
            answer = page.locator('#messages .msg.bot .bubble').last.inner_text()
            assert page.locator('#messages .msg.bot li').count() >= (3 if comparison else 10)
            page.reload(wait_until='domcontentloaded')
            expect(page.locator('#messages')).to_contain_text('$9.98' if comparison else '$760')
            assert 'Historical detail' not in page.locator('#messages').inner_text()
            browser.close()
    if comparison:
        assert all(value in answer for value in ('CedarBond','StoneEpoxy','QuickEpoxy','$9.98','$9.88','$5.97'))
        assert all(answer.count(name)==1 for name in ('CedarBond','StoneEpoxy','QuickEpoxy'))
        assert 'Optional specification catalog' not in answer and 'full-cure' not in answer
        assert len(answer.split())<160 and answer.count('📊')==1
    else: assert '$720' in answer and '$760' in answer and 'amount unavailable' in answer
    assert 'Historical detail' not in answer and 'Full subject:' not in answer
    if outcome == 'partial':
        assert answer.count('📊') == 1 and limitation in answer
    assert len(reads) == 1 and provider.reviews == 1 and provider.producer_calls == 2
    reviews = [event for event in runtime.store.list_conversation_events()
        if event.get('event_type') == 'conversation.completion_review']
    assert len(reviews) == 1 and reviews[0]['disposition'] == ('complete' if outcome=='complete' else 'blocked')
    if outcome == 'complete': assert reviews[0]['presentation_repaired'] is True
    else:
        assert reviews[0]['report_layout_required'] is True and reviews[0]['report_layout_fallback'] is False
        assert reviews[0]['supported_finding_formats'] == [REPORT_MESSAGE_FORMAT, 'legacy_text']
    turns = [event for event in runtime.store.list_conversation_events()
        if event.get('event_type') == 'conversation.chat_turn']
    assert turns[-1]['assistant_reply'] == expected and (turns[-1].get('response_fulfilled') is True if outcome=='complete' else turns[-1].get('response_fulfilled') is not True)
    runtime.checkpoint(force=True)
    from nullion.runtime import bootstrap_persistent_runtime
    restored = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert any(event.get('assistant_reply') == expected for event in restored.store.list_conversation_events())


def test_a_reviewer_rewrite_cannot_certify_an_unobserved_receipt(tmp_path, monkeypatch):
    from tests.e2e.test_universal_outcome_review_boundary import runtime_and_registry, ReasoningProvider
    from tests.test_reviewed_presentation_correction import correction_payload
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime, registry, reads = runtime_and_registry(tmp_path, monkeypatch, 'opaque_read')
    class Provider(ReasoningProvider):
        def create(self, **kwargs):
            if 'semantic completion reviewer' in str(kwargs.get('system') or ''):
                self.review_calls += 1
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                reply = correction_payload()
                identities = [row['requirement_id'] for row in packet['material_requirement_contract']]
                reply['material_requirements'] = [{'requirement_id': identity, 'satisfied': True,
                    'draft_covered': True, 'evidence_invocation_ids': ['unobserved']} for identity in identities]
                reply['supported_findings'][0].update(requirement_ids=identities, evidence_invocation_ids=['unobserved'])
                return text_response(json.dumps(reply))
            return super().create(**kwargs)
    provider = Provider('opaque_read')
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:invalid-correction',
        principal_id='web:invalid-correction', user_message='指定した値を報告。', conversation_history=[],
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=5)
    assert len(reads) == 1 and provider.review_calls == 2
    assert result.response_fulfilled is not True
    assert 'Three upcoming items' not in result.final_text and '$720' not in result.final_text
    reviews = runtime.store.list_recent_conversation_events('web:invalid-correction', event_type='conversation.completion_review')
    assert reviews and not reviews[-1].get('presentation_repaired')
