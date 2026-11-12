"""Captured production failure through real Web/Telegram chat and timeout paths."""
import copy
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, tool_response, text_response
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')
CAPTURE = json.loads((Path(__file__).parents[1]/'fixtures/ticket_context_only_observations.json').read_text())


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('mode', ['no_result', 'verified_result'])
@pytest.mark.parametrize('prompt', ['次の次の月曜の夜の上映を探して。', 'Busca una función para el lunes siguiente, por la noche.'])
def test_review_timeout_preserves_authored_answer_without_context_dump(tmp_path, monkeypatch, surface, mode, prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_operator, crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from datetime import UTC, datetime
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'absent.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: None)
    runtime = setup_runtime(tmp_path, monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-web', principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    calls = []
    capture = copy.deepcopy(CAPTURE)
    if mode == 'verified_result':
        capture['observations'].append({'semantic_role': 'entity', 'label': 'Requested screening',
            'value': 'Ramayana — Monday November 16, 8 PM', 'quote': 'Ramayana — Monday November 16, 8 PM\n89 seats available',
            'source_index': 0})
    registry = ToolRegistry()
    def read(inv):
        calls.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', copy.deepcopy(capture))
    registry.register(ToolSpec('browser_run_task', 'Read requested public information', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), read)
    summary = ('A Ramayana screening is available for Monday, November 16 at 8 PM, with 89 seats available.'
        if mode == 'verified_result' else 'I could not verify a Ramayana screening for Monday, November 16 after 7 PM.')
    class Provider:
        presentations = 0
        def create(self, **kw):
            system = kw.get('system') or ''
            names = {tool['name'] for tool in kw.get('tools') or ()}
            if 'material-requirements planner' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                return text_response(json.dumps({**fixture_source_plan(packet, scope='evidence_summary'),
                    'all_material_requirements_enumerated': True, 'material_requirements': [{
                        'requirement': 'Explain the requested date-specific screening result.', 'requirement_kind': 'fact',
                        'evidence_subject_id': 'requested-screening', 'eligible_evidence_tool_names': ['browser_run_task']}]}))
            if 'semantic completion reviewer' in system:
                raise TimeoutError('External reviewer unavailable')
            protocol = workflow_provider_reply(kw, completion=False)
            if protocol is not None: return protocol
            if names == {'present_source_answer'}:
                self.presentations += 1
                packet = kw['messages'][0]['content'][0]['text']
                context = json.loads(packet.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n')[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END)[0])
                points = []
                for row in context['evidence']:
                    if row['value'] == 'Ramayana\n 3hr 15m ':
                        points.append({'evidence_ids': [row['id']], 'detail_ids': [], 'semantic_role': 'entity',
                            'request_relation': 'supporting_context', 'summary': 'Film identity only.',
                            'relevance': 'Film identity only; not requested-date inventory.'})
                    if mode == 'verified_result' and row['value'].startswith('Ramayana — Monday'):
                        points.append({'evidence_ids': [row['id']], 'detail_ids': [], 'semantic_role': 'entity',
                            'request_relation': 'result', 'summary': '8 PM; 89 seats available.',
                            'relevance': 'The requested-date screening.'})
                limits = [context['limitations'][0]['id']]
                return tool_response('present_source_answer', {'intro': summary, 'focus_roles': ['entity'],
                    'answer_status': 'partial_results' if mode == 'verified_result' else 'no_verified_results',
                    'points': points, 'limitation_ids': limits,
                    'limitation_summaries': {limits[0]: 'The remaining ticket sources did not provide a requested-date result.'}})
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            if 'browser_run_task' not in names:
                return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['browser_run_task'],
                    'required_tool_names': ['browser_run_task']})
            return tool_response('browser_run_task', {'url': 'https://tickets.example/', 'task': prompt, 'max_steps': 6})
    provider = Provider()
    orchestrator = AgentOrchestrator(model_client=provider)
    runtime.active_tool_registry = registry
    runtime.model_client = provider
    if surface == 'web':
        app = web_app.create_app(runtime, orchestrator, registry)
        with TestClient(app) as client:
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': 'web:requested-ticket'})
            assert response.status_code == 200, response.text
            reply = response.json()['text']
    else:
        reply = chat_operator._render_chat_turn(runtime, message='/chat '+prompt,
            chat_id='requested-ticket', settings=load_settings(), model_client=provider, agent_orchestrator=orchestrator)
    assert summary in reply, reply
    assert 'Select Date' not in reply and 'Select a Theatre' not in reply and '3hr 15m' not in reply
    assert 'AMC 84th Street' not in reply and 'Here are the findings' not in reply
    assert 'remaining ticket sources' in reply
    assert len(calls) == 1 and provider.presentations == 1
    assert '8 PM; 89 seats available.' in reply if mode == 'verified_result' else '- **' not in reply
