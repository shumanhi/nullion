"""A stopped optional method must not discard an accepted evidence-bound answer."""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker Web/store boundary')


@pytest.mark.parametrize('prompt', ['記録された値を報告してください。', 'Indica el valor registrado.'])
def test_terminal_failure_publishes_independently_verified_correction(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    runtime = setup_runtime(tmp_path, monkeypatch)
    for key, value in {'NULLION_ENV_FILE': str(tmp_path/'none.env'), 'NULLION_MEMORY_ENABLED': '0',
        'NULLION_TASK_PLANNER_FEED_MODE': 'off', 'NULLION_CHAT_STARTUP_WARMUP': '0'}.items():
        monkeypatch.setenv(key, value)
    calls = []; registry = ToolRegistry()
    def read(inv):
        calls.append(inv.tool_name)
        if inv.tool_name == 'source_read':
            return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
                {'record': {'id': '値', 'value': 37}, 'source_record_collections': [
                    {'path': ['record'], 'identity_fields': ['id'], 'cardinality': 'one'}]})
        return ToolResult(inv.invocation_id, inv.tool_name, 'failed',
            {'reason': 'source_not_ready', 'session_id': 'session-one', 'recovery': {
                'kind': 'correct_arguments', 'retry_tool_name': inv.tool_name,
                'argument_constraints': {'supported_parameters': ['ready_text']}}}, 'Observación no disponible')
    for name in ['source_read', 'browser_extract_text']:
        registry.register(ToolSpec(name, 'Read a public record', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2), read)
    class Model:
        phase = 0; reviews = 0
        def create(self, **kw):
            system = str(kw.get('system', '')); names = {row['name'] for row in kw.get('tools', [])}
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement': '記録された値', 'requirement_kind': 'fact',
                        'evidence_subject_id': 'value', 'eligible_evidence_tool_names': ['source_read']}]}))
            if 'semantic completion reviewer' in system:
                self.reviews += 1; packet = json.loads(kw['messages'][0]['content'][0]['text'])
                proof = next(row['invocation_id'] for row in packet['tool_evidence']
                    if row['tool_name'] == 'source_read' and (row.get('output') or {}).get('source_record_binding'))
                identities = [row['requirement_id'] for row in packet['material_requirement_contract']]
                return text_response(json.dumps({'disposition': 'complete', 'reply_target': 'corrected_findings',
                    'source_presentation_valid': True,
                    'execution_satisfied': True, 'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement_id': identity, 'satisfied': True,
                        'draft_covered': True, 'evidence_invocation_ids': [proof]} for identity in identities],
                    'unresolved_requirements': [], 'retry_tool_names': [],
                    'supported_findings': [{'text': {'format': 'nullion.report.v1',
                        'message': {'title': '', 'summary': 'El valor registrado es 37.', 'detail_level': 'compact',
                            'sections': [], 'sources': [], 'shared_details': [], 'footer': '', 'extra_details': []}}, 'presentation': 'paragraph',
                        'requirement_ids': identities, 'evidence_invocation_ids': [proof]}]}))
            reply = workflow_provider_reply(kw, completion=False)
            if reply is not None:
                return reply
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            if not names:
                return text_response('{}')
            self.phase += 1
            if self.phase == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['source_read', 'browser_extract_text']})
            if self.phase == 2:
                return tool_response('source_read', {})
            assert self.phase <= 4, 'Terminal correction reopened completed source work'
            return tool_response('browser_extract_text', {'ready_text': str(self.phase)})
    model = Model(); runtime.model_client = model
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
        cid = 'web:terminal-correction'
        result = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
        assert result.status_code == 200, result.text
        reviews = runtime.store.list_recent_conversation_events(cid, event_type='conversation.completion_review')
        assert reviews[-1]['disposition'] == 'complete' and reviews[-1]['presentation_repaired'] is True, json.dumps(reviews, ensure_ascii=False)
        assert result.json()['text'] == 'El valor registrado es 37.', result.json()
        turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
        assert turn['response_fulfilled'] is True and turn['assistant_reply'] == result.json()['text']
        assert calls == ['source_read', 'browser_extract_text', 'browser_extract_text'] and model.reviews == 1
        history = client.get('/api/chat/history/'+cid+'?sync_runtime=1').json()['messages']
        assert any(row['role'] == 'bot' and row['text'] == result.json()['text'] for row in history)
