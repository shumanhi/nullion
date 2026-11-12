"""Real chat, graph, SQLite and Telegram boundaries; external model/IO fakes only."""
import asyncio
import json
import os
import time
import statistics

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


def configure(tmp_path, monkeypatch):
    runtime = setup_runtime(tmp_path, monkeypatch)
    for key in ('NULLION_MEMORY_ENABLED', 'NULLION_DOCTOR_ENABLED', 'NULLION_TASK_DECOMPOSITION_ENABLED',
                'NULLION_MULTI_AGENT_ENABLED', 'NULLION_CHAT_STARTUP_WARMUP'):
        monkeypatch.setenv(key, '0')
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'none.env'))
    return runtime


def assert_reviewed_partial_reply(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_text import REPORT_MESSAGE_FORMAT
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
    runtime = configure(tmp_path, monkeypatch); registry = ToolRegistry(); reads = []
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', {'value': 73, 'source_url': 'https://record.example/item',
            'required_source_tool': 'opaque_followup'})
    registry.register(ToolSpec('opaque_read', 'Read the selected record', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), read)
    registry.register(ToolSpec('opaque_followup', 'Optional adapter continuation', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), lambda call: pytest.fail('Terminal partial recovery restarted'))
    class Model:
        reviews = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [{
                        'requirement': 'Report the requested values', 'requirement_kind': 'fact',
                        'evidence_subject_id': 'selected-values', 'eligible_evidence_tool_names': ['opaque_read']}]}))
            if 'semantic completion reviewer' in system:
                self.reviews += 1; packet = json.loads(kw['messages'][0]['content'][0]['text'])
                refs = [r['invocation_id'] for r in packet['citable_evidence']]
                ids = [r['requirement_id'] for r in packet['material_requirement_contract']]
                message = {'summary': 'Verified value: $73. The second requested value is unavailable.',
                    'detail_level': 'compact', 'sources': [{'id': 's', 'label': 'Checked record', 'url': 'https://record.example/item'}],
                    'shared_details': [], 'sections': []}
                return text_response(json.dumps({'disposition': 'blocked', 'source_presentation_valid': None,
                    'all_material_requirements_enumerated': True, 'retry_tool_names': [],
                    'unresolved_requirements': ['The second value remains unavailable.'],
                    'material_requirements': [{'requirement_id': i, 'satisfied': False, 'draft_covered': False,
                        'evidence_invocation_ids': refs} for i in ids],
                    'supported_findings': [{'text': {'format': REPORT_MESSAGE_FORMAT, 'message': message},
                        'evidence_invocation_ids': refs, 'requirement_ids': ids}]}))
            protocol = workflow_provider_reply(kw, completion=False)
            if protocol is not None: return protocol
            names = {r['name'] for r in kw.get('tools') or ()}
            if 'opaque_followup' in names and 'opaque_read' not in names:
                time.sleep(1)  # Reproduce the old slow model dependency at the external boundary.
                raise TimeoutError('The adapter reopened a reviewed partial result')
            if 'opaque_read' not in names and 'request_tool_scope' in names:
                return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['opaque_read']})
            if not names: return text_response('{}')
            return tool_response('opaque_read', {}) if not reads else text_response('A value was found; the other is unavailable.')
    model = Model()
    with TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=model), registry)) as api:
        started = time.perf_counter()
        reply = api.post('/api/chat', json={'text': '/chat '+prompt, 'conversation_id': 'web:reviewed-partial'}).json()['text']
        elapsed = time.perf_counter()-started
        assert '$73' in reply and 'second requested value is unavailable' in reply
        assert 'https://record.example/item' in reply and 'timed out' not in reply
        # Web clients persist the returned reply through this separate route.
        assert api.post('/api/chat/history/web:reviewed-partial/message',
            json={'role': 'bot', 'text': reply}).status_code == 200
        history = api.get('/api/chat/history/web%3Areviewed-partial').json()['messages']
        assert next(row['text'] for row in reversed(history) if row['role'] == 'bot') == reply
        event = next(r for r in reversed(runtime.store.list_conversation_events('web:reviewed-partial'))
            if r.get('event_type') == 'conversation.chat_turn')
    assert len(reads) == 1 and model.reviews > 0
    assert event.get('response_fulfilled') is not True and event['assistant_reply'] == reply
    wire = TelegramWire(); install_wire(monkeypatch, wire)
    assert asyncio.run(_send_operator_telegram_delivery(TOKEN, '77', reply, message_only=True))
    assert len(wire.texts) == 1 and '$73' in wire.texts[0] and 'href="https://record.example/item"' in wire.texts[0]
    return elapsed


def test_reviewed_partial_reply_is_delivered_without_a_fresh_adapter_retry(tmp_path, monkeypatch, record_property):
    durations = []
    for i, prompt in enumerate(['必要な情報を報告してください。', 'Resume la información solicitada.']):
        case = tmp_path/str(i); case.mkdir()
        with monkeypatch.context() as scoped:
            durations.append(assert_reviewed_partial_reply(case, scoped, prompt))
    record_property('request_seconds', json.dumps(durations))
    assert statistics.mean(durations) < 1.5 and max(durations) < 2.5, durations


def test_many_inspections_share_a_view_budget_and_exact_later_fields_remain_accessible(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.retained_work_context import INSPECT_TOOL
    runtime = configure(tmp_path, monkeypatch); registry = ToolRegistry(); reads = []; batches = []; originals = []
    body = 'x'*100000+'SELECTED_VALUE=73'
    def read(call):
        reads.append(call.invocation_id)
        result = ToolResult(call.invocation_id, call.tool_name, 'completed', {'value': 73, 'body': body})
        originals.append(result)
        return result
    registry.register(ToolSpec('opaque_read', 'Read the selected record', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), read)
    class Model:
        phase = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            names = {r['name'] for r in kw.get('tools') or ()}
            if 'opaque_read' not in names and 'request_tool_scope' in names:
                return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['opaque_read']})
            if not names: return text_response('{}')
            if not reads: return tool_response('opaque_read', {})
            if self.phase == 0:
                self.phase = 1
                assert INSPECT_TOOL in names
                return {'stop_reason': 'tool_use', 'content': [
                    {'type': 'tool_use', 'id': 'inspect-'+str(i), 'name': INSPECT_TOOL, 'input': {'requests': [
                        {'invocation_id': reads[0], 'json_pointer': '/body', 'offset': i*10000, 'length': 12000}]}}
                    for i in range(8)]}
            if self.phase == 1:
                self.phase = 2
                from nullion.workflow_reasoning import REVIEW_INSPECTION_CHAR_BUDGET
                for message in kw['messages']:
                    for block in message.get('content', ()) if isinstance(message.get('content'), list) else ():
                        if block.get('type') == 'tool_result' and str(block.get('tool_use_id')).startswith('inspect-'):
                            payload = json.loads(block['content'][0]['text']); batches.extend(payload['output']['windows'])
                assert sum(w['shown_chars'] for w in batches) == REVIEW_INSPECTION_CHAR_BUDGET
                assert any(w.get('inspection_status') == 'budget_exhausted' for w in batches)
                assert len(json.dumps(kw['messages'])) < 100000
                return tool_response(INSPECT_TOOL, {'requests': [{'invocation_id': reads[0], 'json_pointer': '/body',
                    'offset': 100000, 'length': len('SELECTED_VALUE=73')}]})
            assert 'SELECTED_VALUE=73' in json.dumps(kw['messages'])
            return text_response('SELECTED_VALUE=73')
    with TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=Model()), registry)) as api:
        response = api.post('/api/chat', json={'text': '/chat 選択値を確認。', 'conversation_id': 'web:inspection-budget'})
        assert response.status_code == 200 and 'SELECTED_VALUE=73' in response.json()['text']
        event = next(r for r in reversed(runtime.store.list_conversation_events('web:inspection-budget'))
            if r.get('event_type') == 'conversation.chat_turn')
    assert len(reads) == 1 and len(batches) == 8
    assert originals[0].output['body'] == body
    assert any(r['tool_name'] == 'opaque_read' for r in event['tool_results'])
