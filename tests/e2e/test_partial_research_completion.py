"""Real chat, browser adapter, graph, SQLite and Telegram boundaries.

Only the external browser worker and model are deterministic providers.
"""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
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


@pytest.mark.parametrize('prompt', [
    'Compare Thursday NYC–Washington trains around 1 PM and Sunday return trains around noon.',
    '木曜13時ごろNYからワシントンへ、日曜正午ごろ戻る列車を比較して。',
    'Compara trenes de Nueva York a Washington el jueves sobre las 13 y vuelta el domingo al mediodía.',
])
@pytest.mark.parametrize('mode', ['complete', 'complete_same_url', 'step_completed', 'combined', 'bound_combined', 'no_progress'])
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
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            names = {t['name'] for t in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            self.steps += 1
            if self.steps == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['browser_run_task'], 'required_tool_names': ['browser_run_task']})
            if not calls:
                return tool_response('browser_run_task', {'url': 'https://example.com/outbound', 'task': 'Obtain outbound options'})
            if len(calls) < 2 and mode not in {'combined', 'bound_combined'}:
                return tool_response('browser_run_task', {'url': 'https://example.com/return', 'task': 'Obtain missing return options; preserve outbound findings'})
            answer = '**Northeast Regional 1147** — DEPARTS 12:50 p; ARRIVES 4:12 p; 3h 22m; Coach from $ 155.'
            if mode != 'no_progress':
                answer += '\n\n**Northeast Regional 225** — DEPARTS 12:00 p; ARRIVES 3:25 p; 3h 25m; Coach from $ 89.'
            else:
                answer += '\n\nNot verified: Live WAS to NYP return options were not established.'
            return text_response(answer)
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
        assert len(calls) == (1 if mode in {'combined', 'bound_combined'} else 2)
        assert len([r for r in turn['tool_results'] if r['tool_name'] == 'browser_run_task']) == len(calls)
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
