"""Real shared runner and SQLite approval gate; only inference is scripted."""
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container runner E2E')


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('prompt', ['Comprueba el valor y solicita el permiso necesario.', '値を確認し、必要な許可を求めてください。'])
def test_native_pending_approval_replaces_working_progress_and_is_persisted(tmp_path, monkeypatch, surface, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.preferences import save_preferences
    from nullion.request_progress import RequestProgress
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'sentinel_mode': 'risk_based', 'emoji_level': 'none', 'markdown_style': 'plain'})
    registry = ToolRegistry()
    events = []
    progress = RequestProgress(events.append)
    executed = []
    def observe(invocation):
        visible = [event for event in events if event.get('status_kind') == 'request_progress']
        assert visible[-1]['phase_id'] == 'operation'
        assert not visible[-1]['text'].startswith('Planning')
        executed.append(invocation.tool_name)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'value': '37'})
    def forbidden(invocation):
        pytest.fail('A tool awaiting approval must not execute')
    for name, approval, handler in [('opaque_observe', False, observe), ('opaque_gate', True, forbidden)]:
        registry.register(ToolSpec(name, 'PRIVATE /internal/path',
            ToolRiskLevel.HIGH if approval else ToolRiskLevel.LOW, ToolSideEffectClass.READ,
            approval, 5, capability_tags=('web',), input_schema={
                'type': 'object', 'properties': {}, 'additionalProperties': False}), handler)
    class Model:
        step = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=False)
            if protocol is not None:
                return protocol
            names = {tool['name'] for tool in kwargs.get('tools', [])}
            if 'opaque_observe' not in names:
                return call('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['opaque_observe', 'opaque_gate'], 'required_tool_names': ['opaque_gate']})
            self.step += 1
            return call('opaque_observe' if self.step == 1 else 'opaque_gate', {})
    result = AgentOrchestrator(model_client=Model()).run_turn(
        conversation_id=surface + ':approval-progress', principal_id='operator', user_message=prompt,
        conversation_history=[], tool_registry=registry, policy_store=runtime.store,
        approval_store=runtime.store, foreground_event_callback=progress.emit)
    visible = [event for event in events if event.get('status_kind') == 'request_progress']
    assert executed == ['opaque_observe']
    assert visible[-1]['text'].startswith('Waiting for your approval')
    assert visible[-1]['phase_id'] == 'operation'
    assert 'PRIVATE' not in str(visible) and '/internal/path' not in str(visible)
    assert result.approval_id
    assert runtime.store.get_approval_request(result.approval_id).status.value == 'pending'
    assert runtime.store.get_suspended_turn(result.approval_id) is not None
