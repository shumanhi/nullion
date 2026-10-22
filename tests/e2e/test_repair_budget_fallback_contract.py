"""Tool-owned repair budgets must yield to safe alternatives without restarting the request."""
import json
import os

import pytest
from test_scheduled_report_completion import setup_runtime, text_response, tool_response
from test_source_and_status_delivery_contracts import source_presentation_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('prompt', ['Compare the displayed offer for my saved location.', '保存された地域で表示価格を確認して。', 'Compara la oferta con mi ubicación guardada.'])
@pytest.mark.parametrize('repair_kind', ['inspect_page'])
@pytest.mark.parametrize('exhausted', [True, False])
def test_chat_uses_alternate_read_after_repair_budget_without_losing_receipts(tmp_path, monkeypatch, prompt, repair_kind, exhausted):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_REPEATED_TOOL_FAILURE_LIMIT', '2')
    runtime = setup_runtime(tmp_path, monkeypatch); chat_store._store = None
    calls = []; waits = []
    def external(call):
        calls.append(call.tool_name)
        if call.tool_name == 'browser_wait_for':
            waits.append(call.invocation_id)
            if not exhausted and len(waits) == 2:
                return ToolResult(call.invocation_id, call.tool_name, 'completed', {'condition_met': True})
            reason = {'inspect_page': 'browser_condition_unmet', 'repair_precondition': 'tool_precondition_unmet', 'correct_arguments': 'invalid_tool_arguments'}[repair_kind]
            recovery = {'kind': repair_kind, 'retry_tool_name': call.tool_name}
            if repair_kind == 'correct_arguments': recovery['argument_constraints'] = {'timeout': {'minimum': 1}}
            else: recovery['required_tool_names'] = ['browser_snapshot']
            return ToolResult(call.invocation_id, call.tool_name, 'failed', {'reason': reason,
                'page_state_preserved': True, 'recovery': recovery}, 'Synthetic external condition not met')
        if call.tool_name == 'browser_extract_detail':
            return ToolResult(call.invocation_id, call.tool_name, 'completed', {
                'result_kind': 'source_observations', 'report_status': 'completed', 'limitations': [],
                'observations': [{'label': 'Offer A', 'value': '8.10 USD · 12 oz', 'source_url': 'https://example.com/offer'}]})
        return ToolResult(call.invocation_id, call.tool_name, 'completed', {'session_id': 'shared-session', 'page_revision': 1, 'observed_price': '8.10 USD'})
    registry = ToolRegistry()
    for name in ['browser_navigate', 'browser_wait_for', 'browser_snapshot', 'browser_extract_detail']:
        registry.register(ToolSpec(name, 'Read a remote page', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2,
            capability_tags=('web',)), external)
    class Model:
        steps = 0
        def create(self, **kw):
            system = kw.get('system', ''); names = {t['name'] for t in kw.get('tools', [])}
            if system.startswith('You are a material-requirements planner'):
                return text_response(json.dumps({'all_material_requirements_enumerated': True, 'material_requirements': [
                    {'requirement': 'Read the displayed offer', 'requirement_kind': 'fact', 'evidence_subject_id': 'offer', 'eligible_evidence_tool_names': ['browser_extract_detail']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                return text_response(json.dumps({'disposition': 'complete', 'unresolved_requirements': [], 'retry_tool_names': []}))
            if names == {'present_source_answer'}: return source_presentation_response(kw)
            if not names or 'browser_extract_detail' in calls: return text_response('Offer A: 8.10 USD · 12 oz')
            self.steps += 1
            if self.steps == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['browser_navigate','browser_wait_for','browser_snapshot','browser_extract_detail'], 'required_tool_names': ['browser_navigate']})
            if self.steps == 2: return tool_response('browser_navigate', {'url': 'https://example.com/offer'})
            if self.steps == 3: return tool_response('browser_wait_for', {'text': 'Expected condition', 'timeout': 1})
            if self.steps == 4:
                assert 'browser_wait_for' in names, 'One failed repair must not retire the tool'
                return tool_response('browser_snapshot', {})
            if self.steps == 5: return tool_response('browser_wait_for', {'text': 'Expected condition', 'timeout': 1})
            assert ('browser_wait_for' not in names) is exhausted, names
            assert '8.10 USD' in json.dumps(kw['messages']), 'Recovery dropped prior receipts'
            return tool_response('browser_extract_detail', {})
    model = Model(); runtime.model_client = model
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    conversation = 'web:repair-budget'
    with TestClient(app) as client:
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
        assert response.status_code == 200
        payload = response.json()
        assert '8.10 USD' in payload['text'] and '12 oz' in payload['text'], payload
        events = [e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type') == 'conversation.chat_turn']
        names = [t['tool_name'] for t in events[-1]['tool_results']]
        assert names.count('browser_navigate') == 1
        assert names.count('browser_wait_for') == 2
        assert names.count('browser_extract_detail') == 1
    assert calls == ['browser_navigate','browser_wait_for','browser_snapshot','browser_wait_for','browser_extract_detail']
