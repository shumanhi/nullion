"""Real Telegram ingress, stored parent state, graph, preflight and delivery."""
import asyncio
import json
import os
from types import SimpleNamespace

import pytest

from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('mode', ['profile', 'continue', 'persisted', 'unfinished', 'cancel', 'explain', 'override'])
@pytest.mark.parametrize('followup', ["Isn't my region already in the saved settings?", '登録済みの地域は分からないの？'])
def test_profile_and_selected_clarification_survive_until_actual_result(tmp_path, monkeypatch, mode, followup):
    from nullion import preferences, telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _remember_chat_turn
    from nullion.config import load_settings
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    runtime = isolate(tmp_path, monkeypatch)
    profile = tmp_path / 'profile.json'
    profile.write_text(json.dumps({'address': 'Example City, Region A'}))
    monkeypatch.setattr(preferences, '_PROFILE_PATH', profile)
    registry = ToolRegistry()
    reads = []
    def lookup(invocation):
        reads.append(invocation.arguments)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'option': 'Cedar', 'region': invocation.arguments['region']})
    registry.register(ToolSpec('browser_run_task', 'Inspect current regional options', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 3, capability_tags=('web',)), lookup)
    quote = 'Which region should I use?\n1. Region A\n2. Region B'
    if mode != 'profile':
        _remember_chat_turn(runtime, chat_id='123', conversation_id='telegram:123',
            conversation_turn_id='pending-parent', user_message='Find current options in my region.',
            assistant_reply=quote, response_fulfilled=False if mode == 'unfinished' else None,
            tool_results=[ToolResult('prior', 'browser_run_task', 'completed', {'option': 'Unverified'})]
            if mode == 'unfinished' else [] if mode == 'persisted' else [ToolResult('blocked', 'browser_run_task', 'failed',
                {'executed': False, 'reason': 'request_plan_conflict', 'review_decision': 'clarify'})],
            response_presentation={'pending_request': {'original_request': 'Find current options in my region.',
                'tool_names': ['browser_run_task']}} if mode == 'persisted' else None)
    prompt = ('Find current options in my region.' if mode == 'profile' else
        'Use Region B for this request.' if mode == 'override' else
        'Cancel that request.' if mode == 'cancel' else
        'Only explain why you asked; do not run the lookup.' if mode == 'explain' else followup)
    expected_region = 'B' if mode == 'override' else 'A'
    answers = {'cancel': 'Cancelled the pending request.', 'explain': 'The saved context was omitted from the check.'}
    packets = []
    class Model:
        agent_calls = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            names = {row['name'] for row in kw.get('tools', [])}
            if 'select_attachment_format' in names:
                return call('select_attachment_format', {'extension': None, 'confidence': 1})
            if 'relationship' in system and 'target_index' in system:
                return text(json.dumps({'relationship': 'follow_up', 'effect': 'continue', 'confidence': 1,
                    'target_index': 0, 'target_is_explicit': True}))
            if 'Workflow action preflight.' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text']); packets.append(packet)
                if 'Region A' not in json.dumps(packet.get('request_context')):
                    return text(json.dumps({'decision': 'clarify', 'issues': ['Region missing.'],
                        'question': 'Which region?', 'options': ['Region A', 'Region B']}))
                assert packet['proposed_action']['arguments']['region'] == expected_region
                return text(json.dumps({'decision': 'proceed', 'issues': []}))
            if 'material-requirements planner' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text']); packets.append(packet)
                if mode != 'profile':
                    assert packet['pending_request']['original_request'] == 'Find current options in my region.'
                return text(json.dumps({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Respect the current instruction and finish the selected request',
                        'requirement_kind': 'reasoned_answer' if mode in answers else 'fact',
                        'evidence_subject_id': 'selected', 'eligible_evidence_tool_names': [] if mode in answers else ['browser_run_task']}]}))
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text']); packets.append(packet)
                rows = [row for row in packet['tool_evidence'] if row['tool_name'] == 'browser_run_task' and row['status'] == 'completed']
                done = bool(rows) or mode in answers
                return text(json.dumps({'disposition': 'complete' if done else 'retry',
                    'unresolved_requirements': [] if done else ['The selected lookup still needs to execute.'],
                    'retry_tool_names': [] if done else ['browser_run_task'],
                    'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': done,
                        'draft_covered': done, 'evidence_invocation_ids': [rows[-1]['invocation_id']] if rows else []}
                        for row in packet['material_requirement_contract']]}))
            if not names:
                return text('{}')
            self.agent_calls += 1
            if mode in answers:
                return text(answers[mode])
            if 'browser_run_task' not in names:
                return call('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['browser_run_task']})
            if not reads:
                return call('browser_run_task', {'region': expected_region})
            return text('Cedar is available in Region ' + expected_region + '.')
    model = Model(); runtime.active_tool_registry = registry
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN': '123456:abcdefghijklmnopqrstuvwxyz',
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID': '123', 'NULLION_TELEGRAM_CHAT_ENABLED': 'true'})
    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
        operator_chat_id='123', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    sent = []
    class Message:
        message_id = 500
        reply_to_message = None if mode == 'profile' else SimpleNamespace(message_id=42, text=quote, caption=None,
            from_user=SimpleNamespace(is_bot=True, username='fixture'))
        chat = SimpleNamespace(id=123)
        async def reply_text(self, value, **kwargs):
            sent.append(value); return SimpleNamespace(message_id=501 + len(sent))
    Message.text = prompt
    Message.chat.send_action = lambda **kwargs: asyncio.sleep(0)
    asyncio.run(service.on_message(SimpleNamespace(update_id=500, message=Message()), SimpleNamespace(bot=None)))
    assert reads == ([] if mode in answers else [{'region': expected_region}]), sent
    expected = answers.get(mode, 'Cedar is available in Region ' + expected_region + '.')
    assert any(expected in message for message in sent), sent
    rows = runtime.store.list_recent_conversation_events('telegram:123')
    stored = [row for row in rows if row.get('event_type') == 'conversation.chat_turn'][-1]
    assert expected in stored['assistant_reply']
    assert bool(stored['tool_results']) is (mode not in answers)
    if mode != 'profile':
        assert stored['parent_turn_id'] == 'pending-parent'
    if mode not in answers:
        assert packets == []
