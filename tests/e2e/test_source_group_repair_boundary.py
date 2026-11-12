"""Real chat graph, SQLite and Telegram HTTP delivery; fake source/model edges."""
import asyncio
from copy import deepcopy
import json
import os

from fastapi.testclient import TestClient
import pytest

from tests.source_group_fixture import grouped_source_fixture
from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('mode', ['corrected', 'dropped_title', 'invalid_again', 'reviewer_corrects', 'planner_repairs'])
@pytest.mark.parametrize('prompt', ['提示された選択肢の価格と条件を比較して。', 'Compara los precios y las condiciones publicadas.'])
def test_single_capture_repair_retains_group_and_delivers_specific_limitations(tmp_path, monkeypatch, record_property, mode, prompt):
    from nullion import chat_store, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    runtime = setup_runtime(tmp_path, monkeypatch)
    for key, value in {'NULLION_ENV_FILE': str(tmp_path/'absent.env'), 'NULLION_MEMORY_ENABLED': 'false',
        'NULLION_TASK_PLANNER_FEED_MODE': 'off', 'NULLION_ACTIVITY_TRACE_ENABLED': 'false',
        'NULLION_CHAT_STARTUP_WARMUP': '0'}.items(): monkeypatch.setenv(key, value)
    monkeypatch.setattr(chat_store, '_store', None)
    results, original = grouped_source_fixture()
    registry = ToolRegistry(); reads = []
    def capture(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', deepcopy(results[0].output))
    registry.register(ToolSpec('opaque_capture', 'Read selected public records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), capture)
    class Provider:
        presentations = 0
        reviews = 0
        plans = 0
        review_packets = []
        def create(self, **kwargs):
            names = {row['name'] for row in kwargs.get('tools', [])}
            system = kwargs.get('system') or ''
            if 'present_source_answer' in names:
                self.presentations += 1
                raw = kwargs['messages'][0]['content'][0]['text']
                context = json.loads(raw.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n', 1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END, 1)[0])
                # IDs are request-local; compose from the real model evidence view.
                from nullion.source_observation_contract import source_answer_context
                old = source_answer_context(results)['evidence']
                remap = {a['id']: b['id'] for a,b in zip(old, context['evidence'])}
                remap.update({a['id']: b['id'] for first,second in zip(old, context['evidence'])
                    for a,b in zip(first['excerpts'], second['excerpts'])})
                def mapped(value):
                    if isinstance(value, str): return remap.get(value, value)
                    if isinstance(value, list): return [mapped(v) for v in value]
                    if isinstance(value, dict): return {k:mapped(v) for k,v in value.items()}
                    return value
                plan = mapped(deepcopy(original))
                plan.update(answer_status='partial_results', limitation_summaries={
                    row['id']: row['text'] for row in context['limitations']})
                for point, summary in zip(plan['points'], (
                        '$118.',
                        '$137, Unlimited miles, Pickup: 743 Sample Road.')):
                    point.update(request_relation='result', summary=summary)
                if self.presentations > 1:
                    assert 'compatible_title_roles' in raw and 'required_fields' in raw
                    if mode != 'invalid_again': plan['points'][1]['semantic_role'] = 'context'
                    if mode not in {'corrected', 'reviewer_corrects', 'planner_repairs'}: plan['points'][1].pop('title_ids')
                    if mode == 'invalid_again':
                        # The valid authored group survives an unrelated bad
                        # point; recovery must not reconstruct an invalid join.
                        plan['points'][1]['semantic_role'] = 'context'
                        plan['points'][1]['title_ids'] = mapped(original)['points'][1]['title_ids']
                        plan['points'].append({'semantic_role':'entity', 'request_relation':'result',
                            'summary':'Unsupported group.', 'relevance':'Invalid extra point',
                            'evidence_ids':['unowned'], 'detail_ids':[]})
                return tool_response('present_source_answer', plan)
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None:
                if 'material-requirements planner' in system and mode == 'planner_repairs':
                    self.plans += 1
                    value = json.loads(protocol['content'][0]['text'])
                    value['material_requirements'].append({'requirement': 'Follow the selected execution scope',
                        'requirement_kind': 'constraint', 'delivery_required': False,
                        'evidence_subject_id': 'execution',
                        'eligible_evidence_tool_names': [] if self.plans == 1 else ['opaque_capture']})
                    return text_response(json.dumps(value))
                if 'semantic completion reviewer' in system:
                    self.reviews += 1
                    self.review_packets.append(json.loads(kwargs['messages'][0]['content'][0]['text']))
                    value = json.loads(protocol['content'][0]['text'])
                    value['source_presentation_valid'] = True
                    if mode == 'reviewer_corrects':
                        from nullion.chat_text import REPORT_MESSAGE_FORMAT
                        from tests.report_presentation_fixture import report_fixture
                        message, _, _ = report_fixture()
                        item = message['sections'][0]['items'][0]
                        message.update(summary='Captured options', sources=[], shared_details=[],
                            footer=' '.join(results[0].output['unknowns']), sections=[{
                                'heading': 'Comparison', 'priority': 'primary', 'layout': 'list',
                                'items': [{**item, 'title': title, 'summary': summary,
                                    'required_details': details, 'extra_details': [],
                                    'source_ids': [], 'shared_detail_ids': []}
                                    for title, summary, details in [('Oak Compact', '$118', []),
                                        ('Trail SUV', '$137', ['Unlimited miles; 743 Sample Road'])]]}])
                        ids = [row['requirement_id'] for row in value['material_requirements']]
                        refs = list(dict.fromkeys(ref for row in value['material_requirements']
                            for ref in row['evidence_invocation_ids']))
                        value.update(reply_target='corrected_findings', supported_findings=[{
                            'text': {'format': REPORT_MESSAGE_FORMAT, 'message': message},
                            'presentation': 'paragraph', 'requirement_ids': ids,
                            'evidence_invocation_ids': refs}])
                    return text_response(json.dumps(value))
                return protocol
            if 'select_attachment_format' in names: return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            if 'request_tool_scope' in names and 'opaque_capture' not in names:
                return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['opaque_capture']})
            if not names: return text_response('{}')
            assert not reads, 'Completed source work was repeated'
            return tool_response('opaque_capture', {})
    model = Provider()
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as api:
        response = api.post('/api/chat', json={'text': '/chat '+prompt, 'conversation_id': 'web:group-repair'})
        assert response.status_code == 200, response.text
        body = response.json(); text = body['text']
        record_property('runtime_evidence',json.dumps(runtime.store.list_conversation_events('web:group-repair'),default=str))
        assert len(reads) == 1 and model.presentations == 2
        if mode == 'planner_repairs':
            assert model.plans == 2 and model.reviews == 1
        if mode == 'reviewer_corrects':
            assert model.reviews == 1, json.dumps([{key: row.get(key) for key in (
                'draft_final_response', 'review_protocol_correction', 'runtime_coverage_validation_errors',
                'request_context', 'material_requirement_contract')} for row in model.review_packets], ensure_ascii=False)
            assert text.startswith('Captured options') and 'Comparison' in text
        for value in ('Oak Compact', '$118', 'Trail SUV', '$137', 'Unlimited miles', '743 Sample Road'):
            assert value in text, (value, text)
        assert text.count('Trail SUV') == 1
        assert 'Selected offer information' not in text and 'Choose an option' not in text and 'Earlier listing' not in text
        assert 'safely combined or verified' not in text and 'could not turn it into a focused answer' not in text
        assert all(limit in text for limit in results[0].output['unknowns'])
        turns = [e for e in runtime.store.list_conversation_events('web:group-repair')
                 if e.get('event_type') == 'conversation.chat_turn']
        assert len(turns) == 1 and turns[0]['assistant_reply'] == text
        if mode == 'invalid_again':
            assert turns[0]['response_fulfilled'] is False
        else:
            assert turns[0].get('response_fulfilled') is not False
            reviews = [e for e in runtime.store.list_conversation_events('web:group-repair')
                       if e.get('event_type') == 'conversation.completion_review']
            assert reviews and reviews[-1]['disposition'] == 'complete'
        validation = [e for e in runtime.store.list_conversation_events('web:group-repair')
                      if e.get('event_type') == 'conversation.source_answer_validation']
        assert len(validation) == 2 and validation[0]['valid'] is False
        assert validation[-1]['valid'] is (mode != 'invalid_again')
    wire = TelegramWire(); install_wire(monkeypatch, wire)
    assert asyncio.run(_send_operator_telegram_delivery(TOKEN, '77', text, message_only=True))
    assert len(wire.texts) == 1 and '$137' in wire.texts[0] and 'Trail SUV' in wire.texts[0]
    assert 'safely combined or verified' not in wire.texts[0]
