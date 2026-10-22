"""Real chat, browser adapter, graph, SQLite and Telegram boundaries.

Only the external browser worker and model are deterministic providers.
"""
import asyncio
import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_scheduled_report_completion import setup_runtime, text_response, tool_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')
REPORT = json.loads((Path(__file__).parents[1] / 'fixtures/train_partial_source_report.json').read_text())
RETURN = '225\nNortheast Regional\nDEPARTS\n12:00\np\nARRIVES\n3:25\np\n3h 25m\nCoach\nfrom\n$\n89'


def presentation(kwargs, completed):
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    raw = kwargs['messages'][0]['content'][0]['text']
    context = json.loads(raw.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n', 1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END, 1)[0])
    points = []
    for row in context['evidence']:
        number = row['value'].splitlines()[0]
        if number not in {'1147', '225'}: continue
        ids = {e['text']: e['id'] for e in row['excerpts']}
        fields = (['DEPARTS', '12:50', 'p'], ['ARRIVES', '4:12', 'p'], ['3h 22m'], ['Coach', 'from', '$', '155']) if number == '1147' else (
            ['DEPARTS', '12:00', 'p'], ['ARRIVES', '3:25', 'p'], ['3h 25m'], ['Coach', 'from', '$', '89'])
        groups = [[ids[x] for x in field] for field in fields]
        points.append({'evidence_ids': [row['id']], 'title_ids': [ids['Northeast Regional'], ids[number]],
            'detail_ids': list(dict.fromkeys(i for g in groups for i in g)), 'detail_groups': groups,
            'relevance': 'An option for the requested comparison side'})
    return tool_response('present_source_answer', {'intro': 'NYC ↔ Washington: Thursday outbound, Sunday return.',
        'points': points, 'limitation_ids': [] if completed or not context['limitations'] else [context['limitations'][0]['id']]})


@pytest.mark.parametrize('prompt', [
    'Compare Thursday NYC–Washington trains around 1 PM and Sunday return trains around noon.',
    '木曜13時ごろNYからワシントンへ、日曜正午ごろ戻る列車を比較して。',
    'Compara trenes de Nueva York a Washington el jueves sobre las 13 y vuelta el domingo al mediodía.',
])
@pytest.mark.parametrize('mode', ['complete', 'complete_same_url', 'step_completed', 'combined', 'bound_combined', 'no_progress', 'budget'])
def test_partial_producer_continues_missing_side_without_restarting_or_losing_fields(tmp_path, monkeypatch, prompt, mode):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator, _COMPLETION_REVIEW_MAX_ATTEMPTS
    from nullion.plugins.browser_plugin import register_browser_tools, browser_use_adapter, browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.telegram_app import _deliver_reply
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0'); monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setattr(browser_use_adapter, 'browser_use_available', lambda: True)
    monkeypatch.setattr(browser_use_adapter, 'browser_use_python', lambda: '/external/worker')
    runtime = setup_runtime(tmp_path, monkeypatch); chat_store._store = None
    calls = []
    def external_browser(task, **kwargs):
        calls.append(task.task)
        if len(calls) > 1:
            if mode == 'combined': assert task.remaining_requirements
            else: assert task.remaining_requirements == ['Obtain Sunday return trains around noon, including arrival times and Coach fares']
        if mode in {'complete', 'complete_same_url', 'step_completed', 'combined'} and len(calls) == 2:
            return {'result_kind': 'source_observations', 'report_status': 'completed', 'next_action': 'report',
                'page_url': 'https://example.com/search', 'observations': [{'value': RETURN, 'quote': RETURN, 'source_url': 'https://example.com/return'}], 'unknowns': []}
        output = copy.deepcopy(REPORT)
        if mode == 'step_completed':
            output.update(report_status='completed', goal_status='answered', unknowns=[], blocker=None)
        if mode in {'combined', 'bound_combined'}:
            output.update(report_status='completed', goal_status='answered', unknowns=[], blocker=None)
            output['observations'].append({'value': RETURN, 'quote': RETURN, 'source_url': 'https://example.com/return'})
            if mode == 'bound_combined':
                for i,row in enumerate(output['observations']): row['captured_at'] = 2.0 if i == len(output['observations'])-1 else 1.0
        if mode == 'complete_same_url':
            output['page_url'] = 'https://example.com/search'
            output['continuation'] = {'kind': 'alternative_source', 'source_url': output['page_url'], 'tool_names': ['browser_run_task']}
        if mode == 'budget' and len(calls) > 1:
            value = f'Additional captured option {len(calls)}'
            output['observations'].append({'value': value, 'quote': value, 'source_url': 'https://example.com/outbound'})
        return output
    monkeypatch.setattr(browser_use_worker, 'run_isolated_browser_task', external_browser)
    class Model:
        steps = 0
        def create(self, **kwargs):
            names = {t['name'] for t in kwargs.get('tools', [])}; system = kwargs.get('system', '')
            if system.startswith('You are a material-requirements planner'):
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement': req, 'requirement_kind': 'comparison_side',
                        'evidence_subject_id': side, 'eligible_evidence_tool_names': ['browser_run_task']}
                        for side, req in [('outbound', 'Verify outbound trains, times and Coach fares'), ('return', 'Verify return trains, times and Coach fares')]]}))
            if system.startswith('You are a semantic completion reviewer'):
                data = json.loads(kwargs['messages'][0]['content'][0]['text'])
                evidence = [r for r in data['tool_evidence'] if r['tool_name'] == 'browser_run_task']
                done = mode in {'combined', 'bound_combined'} or mode in {'complete', 'complete_same_url', 'step_completed'} and len(calls) == 2
                if mode == 'bound_combined':
                    bound = [r for r in evidence if r['output'].get('source_record_binding')]
                    assert len(bound) == 2
                    assert len({r['output']['source_record_binding']['parent_invocation_id'] for r in bound}) == 1
                    evidence = bound
                rows = [{**{k: row[k] for k in ['requirement_id', 'requirement', 'requirement_kind']},
                    'satisfied': i == 0 or done, 'draft_covered': i == 0 or done,
                    'evidence_invocation_ids': [evidence[min(i, len(evidence)-1) if done else 0]['invocation_id']] if i == 0 or done else []}
                    for i, row in enumerate(data['material_requirement_contract'])]
                return text_response(json.dumps({'disposition': 'complete' if done else 'retry',
                    'unresolved_requirements': [] if done else ['Obtain Sunday return trains around noon, including arrival times and Coach fares'],
                    'retry_tool_names': [] if done else ['browser_run_task'],
                    'all_material_requirements_enumerated': True, 'material_requirements': rows}))
            if names == {'present_source_answer'}: return presentation(kwargs, mode in {'combined', 'bound_combined'} or mode in {'complete', 'complete_same_url', 'step_completed'} and len(calls) == 2)
            if not names: return text_response('{}')
            if 'select_attachment_format' in names: return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            self.steps += 1
            if self.steps == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['browser_run_task'], 'required_tool_names': ['browser_run_task']})
            if not calls:
                return tool_response('browser_run_task', {'url': 'https://example.com/outbound', 'task': 'Obtain outbound options'})
            assert any(marker in json.dumps(kwargs['messages']).lower() for marker in ['unresolved outcomes', 'alternative_source'])
            return tool_response('browser_run_task', {'url': 'https://example.com/return', 'task': 'Obtain only missing return options; preserve outbound findings'})
    model = Model(); runtime.model_client = model
    registry = ToolRegistry(); register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    conversation = 'web:partial-trip'
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
        assert response.status_code == 200, response.text
        payload = response.json(); text = payload['text']
        assert '**Northeast Regional 1147**' in text and 'DEPARTS 12:50 p' in text and 'Coach from $ 155' in text, payload
        if mode in {'complete', 'complete_same_url', 'step_completed', 'combined', 'bound_combined'}:
            assert '**Northeast Regional 225**' in text and 'DEPARTS 12:00 p' in text and 'Coach from $ 89' in text, payload
            assert 'Not verified' not in text and "couldn't" not in text, payload
        else: assert 'Not verified' in text and 'Live WAS to NYP' in text, payload
        assert not payload['artifacts']
        turn = [e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type') == 'conversation.chat_turn'][-1]
        assert turn['assistant_reply'] == text
        expected = 1 if mode == 'bound_combined' else 1 + _COMPLETION_REVIEW_MAX_ATTEMPTS if mode == 'budget' else 2
        assert len(calls) == expected
        assert len([r for r in turn['tool_results'] if r['tool_name'] == 'browser_run_task']) == expected
        assert all(r['output']['completion_scope'] == 'tool_step' for r in turn['tool_results'] if r['tool_name'] == 'browser_run_task')
        history = client.get('/api/chat/history/'+conversation, params={'turn_ids': json.dumps([turn['turn_id']])})
        assert history.json()['terminal_replies'][0]['text'] == text
    wire = []
    class Message:
        chat = SimpleNamespace(id=311); message_id = 1
        async def reply_text(self, text, **kwargs):
            wire.append(text); return SimpleNamespace(message_id=2)
    asyncio.run(_deliver_reply(Message(), text, principal_id='telegram:311'))
    assert len(wire) == 1 and 'Northeast Regional 1147' in wire[0] and 'Coach from $ 155' in wire[0]
