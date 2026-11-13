"""A current source answer survives stale format guesses and real file failure."""
import asyncio
from datetime import datetime, UTC
import json
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


def text(value):
    return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': value if isinstance(value, str) else json.dumps(value)}]}


def call(name, arguments, call_id):
    return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': call_id, 'name': name, 'input': arguments}]}


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('prompt,wants_file', [
    ('Check my calendar.', False), ('予定を確認してください。', False),
    ('راجع التقويم وأنشئ تقريرًا بصيغة .pdf', True),
])
def test_current_reviewed_request_owns_delivery_and_keeps_verified_findings(
        tmp_path, monkeypatch, surface, prompt, wants_file, source_summary_model_reply, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_permission_grant
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.connections import ConnectionRegistry, ProviderConnection, save_connection_registry
    from nullion import connections, web_app
    from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL, BoundaryPolicyRule, BoundaryKind, PolicyMode
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, register_calendar_plugin

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', '0')
    monkeypatch.setenv('NULLION_DOCTOR_ENABLED', '0')
    monkeypatch.setattr(connections, '_CONNECTIONS_PATH', tmp_path/'home'/'connections.json')
    save_connection_registry(ConnectionRegistry(connections=[ProviderConnection(
        connection_id='fixture-calendar', workspace_id='workspace_admin', provider_id='fixture_connector_provider',
        display_name='Fixture calendar', provider_profile='https://calendar.example.test', permission_mode='read')]))
    reads, renders = [], []
    event = {'id': 'cedar-1', 'summary': 'Cedar appointment',
             'start': {'dateTime': '2026-11-01T19:30:00-05:00'},
             'end': {'dateTime': '2026-11-01T20:00:00-05:00'}}
    def calendar(start, end, limit):
        reads.append((start, end, limit))
        return [event]
    def unavailable_renderer(invocation):
        renders.append(invocation.arguments)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
            {'reason': 'renderer_unavailable', 'retryable': False}, error='Renderer unavailable')
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    register_calendar_plugin(registry, calendar_lister=calendar)
    registry.register(ToolSpec('file_write', 'Create the selected report file', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 3,
        input_schema={'type':'object','properties':{'path':{'type':'string'},'content':{'type':'string'}},
                      'required':['path','content']}, capability_tags=('local_files',)), unavailable_renderer)
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.active_tool_registry = registry
    for name in ('calendar_list', 'file_write'):
        runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-consent',
            principal_id=OPERATOR_PERMISSION_PRINCIPAL, permission='tool:'+name, granted_by='fixture', granted_at=datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-calendar', OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.ACCOUNT_ACCESS, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files', OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))

    trace = []
    class Model:
        calls = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            trace.append({'system':system[:95], 'tools':[r['name'] for r in kw.get('tools',[])], 'count':self.calls})
            names = {tool['name'] for tool in kw.get('tools', [])}
            if 'Workflow action preflight.' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                if not wants_file and packet['proposed_action']['tool_name'] == 'file_write':
                    return text({'decision':'revise','issues':['The current request only needs the calendar findings.']})
                return text({'decision':'proceed','issues':[]})
            if 'material-requirements planner' in system:
                requirements = [{'requirement':'Give the current calendar findings', 'requirement_kind':'fact',
                    'evidence_subject_id':'schedule', 'eligible_evidence_tool_names':['calendar_list']}]
                if wants_file:
                    requirements.append({'requirement':'Deliver the requested PDF', 'requirement_kind':'deliverable',
                        'evidence_subject_id':'report', 'eligible_evidence_tool_names':['file_write']})
                return text({'all_material_requirements_enumerated':True,
                    'requested_artifact_extensions':['.pdf'] if wants_file else [], 'material_requirements':requirements})
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                sources = [row for row in packet['tool_evidence'] if row['tool_name']=='calendar_list' and row['status']=='completed']
                refs = [sources[-1]['invocation_id']] if sources else []
                draft_has_findings = 'Cedar appointment' in packet['draft_final_response']
                rows = [{'requirement_id': row['requirement_id'], 'satisfied': row['requirement_kind']!='deliverable' and bool(refs),
                    'draft_covered': row['requirement_kind']!='deliverable' and draft_has_findings,
                    'evidence_invocation_ids':refs if row['requirement_kind']!='deliverable' else []}
                    for row in packet['material_requirement_contract']]
                return text({'disposition':'blocked' if wants_file else 'complete' if draft_has_findings else 'retry',
                    'all_material_requirements_enumerated':True,'material_requirements':rows,
                    'unresolved_requirements':['The PDF renderer was unavailable.'] if wants_file else [] if draft_has_findings else ['Present the verified calendar results.'],
                    'retry_tool_names':[], 'supported_findings':[{'text':'Cedar appointment is on November 1 at 7:30 PM.',
                        'evidence_invocation_ids':refs}, {'kind':'limitation',
                        'text':'The requested PDF could not be generated because the renderer is unavailable.',
                        'evidence_invocation_ids':[next(row['invocation_id'] for row in packet['tool_evidence']
                            if row['tool_name']=='file_write' and row['status']=='failed')]}] if wants_file and refs else [],
                    'user_facing_failure':'The requested PDF could not be generated because the renderer is unavailable.' if wants_file else None if draft_has_findings else 'The calendar was read but the draft did not present its results.'})
            if 'select_attachment_format' in names:
                return call('select_attachment_format', {'extension':'.pdf' if wants_file else None,'confidence':1}, 'format')
            presentation = source_summary_model_reply(kw)
            if presentation is not None:
                return presentation
            if not names:
                return text({'web_action':'none','scheduler_action':'none','requested_tool_names':['calendar_list',*(['file_write'] if wants_file else [])],
                    'required_tool_names':['calendar_list'], 'connector_source_user_requested':True,
                    'connector_source_evidence':prompt, 'confidence':1})
            self.calls += 1
            assert self.calls <= 12, 'Delivery disagreement restarted the completed source workflow'
            if self.calls == 1:
                return call('request_tool_scope', {'capabilities':['connector','local_files'],
                    'source_user_requested':True,'source_evidence':prompt,'tool_names':['calendar_list',*(['file_write'] if wants_file else [])],
                    'required_tool_names':['calendar_list'], 'artifact_extensions':['.pdf'] if wants_file else []}, 'scope')
            if self.calls == 2:
                return call('calendar_list', {'start':'2026-10-31T00:00:00-04:00','end':'2026-11-08T00:00:00-05:00','max':20}, 'read')
            if self.calls == 3 and wants_file:
                return call('file_write', {'path':str(tmp_path/'report.pdf'),'content':'Cedar appointment'}, 'render')
            return text('Cedar appointment is on November 1 at 7:30 PM.' + (' The requested PDF renderer is unavailable.' if wants_file else ''))

    model = Model()
    orchestrator = AgentOrchestrator(model_client=model)
    cid = 'web:current-delivery' if surface=='web' else 'telegram:42'
    if surface == 'web':
        app = web_app.create_app(runtime, orchestrator, registry)
        with TestClient(app) as client:
            response = client.post('/api/chat', json={'text':prompt,'conversation_id':cid})
            assert response.status_code == 200, response.text
            payload = response.json()
            answer = payload['text']
            (tmp_path/'delivery-diagnostic.json').write_text(json.dumps({'answer':answer,'trace':trace,'events':runtime.store.list_conversation_events(cid)},default=str))
            assert not payload.get('artifacts')
    else:
        settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',operator_chat_id='42',chat_enabled=True))
        response = run_platform_chat_request(runtime, PlatformChatRequest(platform='telegram',text=prompt,
            conversation_id=cid, chat_id='42', settings=settings, model_client=model, agent_orchestrator=orchestrator,
            append_activity_trace=False))
        answer = response.text
        assert not response.artifact_paths
        from telegram import Bot, Message
        from nullion.telegram_app import _deliver_reply
        from tests.e2e.test_health_action_acknowledgements import telegram_boundary
        with telegram_boundary() as (base_url, calls):
            async def deliver():
                async with Bot('123456:synthetic', base_url=base_url) as bot:
                    message = Message.de_json({'message_id':101,'date':1,'chat':{'id':42,'type':'private'},'text':prompt}, bot)
                    await _deliver_reply(message, answer)
            asyncio.run(deliver())
        sent = [row['text'] for name, row in calls if name=='sendMessage']
        assert sent and 'Cedar appointment' in '\n'.join(sent)
    record_property('native_diagnostic',json.dumps({'answer':answer,'trace':trace,'events':runtime.store.list_conversation_events(cid)},default=str))
    assert 'Cedar appointment' in answer, {'answer':answer,'trace':trace,'diagnostics':[{k:e.get(k) for k in ['event_type','status','validation_errors','error_type','review_error','disposition','reached_iteration_limit']} for e in runtime.store.list_conversation_events(cid) if e.get('event_type') in {'conversation.completion_requirement_contract','conversation.completion_review','conversation.agent_turn'}]}
    assert "I couldn't attach the requested file." != answer
    if wants_file:
        assert 'PDF' in answer and 'unavailable' in answer
    else:
        assert 'attach the requested file' not in answer
    assert len(reads) == 1
    assert len(renders) == (1 if wants_file else 0)
    events = [row for row in runtime.store.list_conversation_events(cid) if row.get('event_type')=='conversation.chat_turn']
    assert events and 'Cedar appointment' in events[-1]['assistant_reply']
