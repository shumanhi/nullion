"""Keep direct reads fast and uncertain writes safe across execution modes."""

import os

import pytest

from tests.e2e.test_quoted_reply_review_and_cleanup import call, isolate, text
from tests.workflow_provider_protocol import workflow_provider_reply


pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def test_single_scoped_read_is_direct_with_ambient_recall(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.web_app import create_app

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    registry = ToolRegistry()
    calls = []

    def read(invocation):
        calls.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'text': 'Lima 37'})

    registry.register(ToolSpec('browser_read_pages', 'Scoped read', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, capability_tags=('web',)), read)

    class Model:
        round = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            if not kwargs.get('tools'):
                return text('{}')
            self.round += 1
            names = {row['name'] for row in kwargs['tools']}
            if self.round == 1:
                assert 'chat_history_search' in names
                return call('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['browser_read_pages'], 'required_tool_names': ['browser_read_pages']})
            if self.round == 2:
                assert 'browser_read_pages' in names
                return call('browser_read_pages', {'pages': [{'url': 'https://example.test/'}]})
            return text('Lima 37')

    conversation = 'web:scoped-direct-read'
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=Model()), registry)) as client:
        response = client.post('/api/chat', json={'conversation_id': conversation,
            'text': 'Consult a public source for the current Lima fixture reading.'})
    assert response.status_code == 200 and 'Lima 37' in response.json()['text'], response.text
    turn = runtime.store.list_recent_conversation_events(conversation, event_type='conversation.chat_turn')[-1]
    assert [r['tool_name'] for r in turn['tool_results']] == ['request_tool_scope', 'browser_read_pages']
    assert turn['tool_results'][0]['output']['selected_tool_names'] == ['browser_read_pages']
    assert calls == [turn['tool_results'][-1]['invocation_id']]


@pytest.mark.parametrize('first_lane', ['code_mode', 'native'])
def test_uncertain_write_cannot_be_replayed_through_other_lane(tmp_path, monkeypatch, first_lane):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    registry = ToolRegistry()
    writes = []

    def write(invocation):
        writes.append(invocation.arguments['value'])
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', {'reason': 'response_lost'})

    registry.register(ToolSpec('append_record', 'Append a record', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 5,
        input_schema={'type': 'object', 'properties': {'value': {'type': 'integer'}}, 'required': ['value']}), write)

    class Model:
        round = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            self.round += 1
            if self.round <= 2:
                if (self.round == 1) == (first_lane == 'code_mode'):
                    return call('code_mode', {'tool_names': ['append_record'],
                        'code': "call_tool('append_record', {'value': 37})"})
                return call('append_record', {'value': 37})
            return text('The record may have been saved. Check it before trying again.')

    result = AgentOrchestrator(model_client=Model()).run_turn(conversation_id='web:write-once',
        principal_id='workspace_admin', user_message='37 を記録してください。', conversation_history=[],
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
    assert writes == [37]
    reasons = [receipt.output.get('reason') for receipt in result.tool_results]
    assert 'response_lost' in reasons and 'code_mode_unknown_write_outcome' in reasons, reasons
