"""Real SDK/Chromium/validator/chat/store; only the external model is scripted."""
import asyncio
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from test_browser_use_runtime_contract import response, source_presentation_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='container E2E only')


def page_feedback(messages):
    decoder = json.JSONDecoder()
    found = []
    for message in messages:
        content = message.get('content', '')
        texts = [content] if isinstance(content, str) else [p.get('text', '') for p in content if p.get('type') == 'text']
        for text in texts:
            for index, char in enumerate(text):
                if char != '{': continue
                try: obj, _ = decoder.raw_decode(text[index:])
                except ValueError: continue
                if isinstance(obj, dict) and 'text_lines' in obj: found.append(obj)
    assert found, 'The real browser feedback must expose referenceable captured text'
    return found[-1]


@pytest.mark.parametrize('isolated', [False, True])
@pytest.mark.parametrize('failure', ['quote', 'value', 'mixed', 'fields', 'absence', 'comparison', 'comparison_document'])
def test_invalid_done_is_repaired_in_same_real_browser_before_visible_delivery(
        tmp_path, monkeypatch, source_summary_model_reply, isolated, failure):
    from pathlib import Path
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import _deliver_reply
    from nullion.tools import ToolRegistry

    if not isolated: pytest.importorskip('browser_use')
    elif os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON'):
        monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON', os.environ['NULLION_BROWSER_USE_TEST_PYTHON'])
    else: pytest.skip('The isolated SDK dependency is not installed in this container')
    for key, value in {'NULLION_HOME': str(tmp_path / 'home'), 'NULLION_BROWSER_BLOCK_PRIVATE': 'false',
            'NULLION_BROWSER_HEADLESS': 'true', 'NULLION_BROWSER_USE_SANDBOX': 'false',
            'NULLION_BROWSER_BACKEND': 'playwright', 'NULLION_WORKSPACE_STORAGE_ROOT': str(tmp_path / 'workspaces'),
            'NULLION_MEMORY_ENABLED': '0', 'NULLION_TASK_PLANNER_FEED_MODE': 'off',
            'NULLION_COMPLETION_REVIEW_TIMEOUT_SECONDS': '20',
            'ANONYMIZED_TELEMETRY': 'false'}.items(): monkeypatch.setenv(key, value)
    monkeypatch.delenv('NULLION_BROWSER_USE_CDP_URL', raising=False)
    for name in ('_schedule_web_builder_reflection', '_schedule_web_no_tool_memory_capture'):
        monkeypatch.setattr(web_app, name, lambda *a, **kw: None)
    class Page(BaseHTTPRequestHandler):
        requests = 0
        def do_GET(self):
            type(self).requests += 1
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8'); self.end_headers()
            extra = '<h2>候補 B</h2><div>11:30</div><div>€</div><div>84</div><div>3h 05m</div>' if failure in {'comparison', 'comparison_document'} else ''
            self.wfile.write(('<html><body><h1>候補 A</h1><div>10:45</div><div>€</div><div>73</div><div>2h 17m</div>'+extra+'</body></html>').encode())
        def log_message(self, *_): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://localhost:{server.server_port}/results'
    class Model:
        model = 'external-provider-fixture'
        browser_calls = 0
        outer_calls = 0
        review_calls = 0
        def create(self, **kwargs):
            if failure in {'comparison', 'comparison_document'} and str(kwargs.get('system', '')).startswith('You are a material-requirements planner'):
                return response(json.dumps({'all_material_requirements_enumerated': True, 'material_requirements': [
                    {'requirement': f'Verify candidate {label} time, price, duration and read-only execution. ' + ('The literal current snapshot must jointly support all requested candidate fields. ' * 8), 'requirement_kind': 'comparison_side',
                        'evidence_subject_id': label, 'eligible_evidence_tool_names': ['browser_run_task']} for label in ['A', 'B']]}))
            if str(kwargs.get('system', '')).startswith('You are a semantic completion reviewer'):
                self.review_calls += 1
                if kwargs['timeout'] < 20:
                    raise TimeoutError('The configured provider cannot finish before its configured deadline')
                review = json.loads(kwargs['messages'][0]['content'][0]['text'])
                source = [row['output'] for row in review['tool_evidence'] if row['tool_name'] == 'browser_run_task']
                assert source and all(isinstance(row, dict) for row in source)
                proof = [record['quote'] for row in source for record in row.get('observations', [])] + [doc['text'] for doc in review['source_documents'].values()]
                assert any('73' in text for text in proof), 'The reviewer must receive the captured price, not only screenshot paths'
                assert any('2h 17m' in text for text in proof)
                if failure == 'absence':
                    assert all(not record.get('covers_main_document') for row in source for record in row.get('observations', []))
                    assert any('2h 17m' in doc['text'] for doc in review['source_documents'].values())
                    assert all(capture['source_document_id'] in review['source_documents'] for row in source for capture in row['sources'])
                assert all(row['execution_audit']['read_only_actions_only'] is True and row['execution_audit']['deliverable_file_count'] == 0 for row in source)
                if failure in {'comparison', 'comparison_document'}:
                    records = [row for row in review['tool_evidence'] if row['output'].get('source_record_binding')]
                    assert len(records) == 2 and len({r['output']['source_record_binding']['parent_invocation_id'] for r in records}) == 1
                    return response(json.dumps({'disposition': 'complete', 'unresolved_requirements': [], 'retry_tool_names': [],
                        'all_material_requirements_enumerated': True, 'material_requirements': [
                            {'requirement_id': req['requirement_id'], 'satisfied': True, 'draft_covered': True,
                                'evidence_invocation_ids': [records[i]['invocation_id']]} for i,req in enumerate(review['material_requirement_contract'])]}))
            schema_messages = [m for m in kwargs['messages'] if isinstance(m.get('content'), str) and m['content'].startswith('Return valid JSON matching this schema: ')]
            if schema_messages:
                self.browser_calls += 1
                if self.browser_calls == 1:
                    invalid = {'label': '比較', 'value': '€73' if failure != 'value' else '€9',
                        'quote': '候補 A 10:45 €73 2h 17m' if failure != 'value' else '候補 A\n10:45\n€\n73\n2h 17m'}
                    observations = ([{'label': '時間', 'value': '10:45', 'quote': '10:45'}] if failure == 'mixed' else []) + [invalid]
                    if failure in {'comparison', 'comparison_document'}:
                        observations = [{'label': '候補', 'value': value, 'quote': value} for value in [
                            '候補 A\n10:45\n€\n73\n2h 17m', '候補 B\n11:30\n€\n84\n3h 05m']]
                        if failure == 'comparison_document':
                            observations = [{'label': 'ページ全体', 'value': '候補 A', 'quote': '\n'.join(row['quote'] for row in observations)}]
                    if failure == 'absence':
                        observations = [{'label': str(i), 'value': value, 'quote': value}
                            for i, value in enumerate(['候補 A', '10:45', '€\n73', '2h 17m'])]
                    if failure == 'fields':
                        observations = [{'label': str(i), 'value': value, 'quote': value}
                            for i, value in enumerate(['候補 A', '10:45', '€\n73', '2h 17m'])]
                    action = {'done': {'data': {'goal_status': 'answered', 'next_action': 'report', 'observations': observations}, 'success': True}}
                elif self.browser_calls == 2:
                    assert 'needs_evidence_repair' in json.dumps(kwargs['messages'])
                    action = {'read_page_state': {}}
                else:
                    assert self.browser_calls == 3, 'Evidence repair must not restart navigation or loop'
                    page = page_feedback(kwargs['messages'])
                    lines = page['text_lines']; start = next(row['index'] for row in lines if row['text'] == '候補 A')
                    end = next(row['index'] for row in lines if row['text'] == '2h 17m')
                    action = {'done': {'data': {'goal_status': 'answered', 'next_action': 'report', 'observations': [
                        {'label': '比較', 'text_selection': {'source_id': page['source_id'], 'start_line': start, 'end_line': end}}]}, 'success': True}}
                schema = json.loads(schema_messages[-1]['content'].split(': ', 1)[1])
                payload = {'evaluation_previous_goal': 'Read current evidence', 'memory': 'Keep verified values', 'next_goal': 'Finish the same lookup', 'action': [action]}
                if 'thinking' in schema.get('properties', {}): payload['thinking'] = 'Select exact captured lines'
                for key in ('current_plan_item', 'plan_update'):
                    if key in schema.get('properties', {}): payload[key] = None
                return response(json.dumps(payload, ensure_ascii=False))
            reviewed = source_summary_model_reply(kwargs)
            if reviewed is not None: return reviewed
            names = {tool['name'] for tool in kwargs.get('tools', [])}
            if 'present_source_answer' in names:
                if failure in {'fields', 'absence', 'comparison_document'}:
                    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                    raw = kwargs['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n', 1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END, 1)[0]
                    rows = json.loads(raw)['evidence']
                    if failure == 'comparison_document':
                        row=rows[0];ids={e['text']:e['id'] for e in row['excerpts']};points=[]
                        for title,when,price,duration in [('候補 A','10:45','73','2h 17m'),('候補 B','11:30','84','3h 05m')]:
                            groups=[[ids[when]],[ids['€'],ids[price]],[ids[duration]]]
                            points.append({'evidence_ids':[row['id']],'title_ids':[ids[title]],'detail_ids':[i for g in groups for i in g],
                                'detail_groups':groups,'relevance':'A captured candidate'})
                        return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'present','name':'present_source_answer',
                            'input':{'intro':'','points':points,'limitation_ids':[]}}]}
                    if failure == 'absence':
                        context = json.loads(raw)
                        assert context['capture_documents'][rows[0]['capture_ids'][0]].endswith('2h 17m')
                        groups = [[excerpt['id'] for excerpt in row['excerpts']] for row in rows[1:]]
                        draft = {'intro': 'このページには配達予定日の表示がありません。', 'intro_evidence_ids': [rows[0]['id']],
                            'points': [{'evidence_ids': [r['id'] for r in rows], 'title_ids': [rows[0]['excerpts'][0]['id']],
                                'detail_ids': [i for g in groups for i in g], 'detail_groups': groups, 'relevance': 'Captured facts'}], 'limitation_ids': []}
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'present', 'name': 'present_source_answer', 'input': draft}]}
                    groups = [[excerpt['id'] for excerpt in row['excerpts']] for row in rows[1:]]
                    draft = {'intro': '', 'points': [{'evidence_ids': [row['id'] for row in rows],
                        'title_ids': [rows[0]['excerpts'][0]['id']], 'detail_ids': [i for g in groups for i in g],
                        'detail_groups': groups, 'relevance': 'The requested fields from the same captured candidate'}], 'limitation_ids': []}
                    return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'present', 'name': 'present_source_answer', 'input': draft}]}
                return source_presentation_response(kwargs)
            if 'select_attachment_format' in names:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'format', 'name': 'select_attachment_format', 'input': {'extension': None, 'confidence': 1}}]}
            self.outer_calls += 1
            name, args = ('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['browser_run_task'], 'required_tool_names': ['browser_run_task']}) if self.outer_calls == 1 else (
                'browser_run_task', {'task': '比較して。候補、時間、価格、所要時間を返して。', 'url': url, 'max_steps': 5})
            assert self.outer_calls <= 2, 'The outer graph must receive corrected evidence from the same browser invocation'
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': f'call-{self.outer_calls}', 'name': name, 'input': args}]}
    model = Model(); runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db'); runtime.model_client = model
    registry = ToolRegistry(); register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    conversation = 'web:different-workspace-repair'
    try:
        with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
            result = client.post('/api/chat', json={'text': ('Compara ambas opciones con hora, precio y duración. ' if failure in {'comparison', 'comparison_document'} else 'Compara la opción con hora, precio y duración. ')+url, 'conversation_id': conversation})
            assert result.status_code == 200, result.text
            text = result.json()['text']
            assert '候補 A' in text and '73' in text and '2h 17m' in text, result.json()
            assert 'Not verified' not in text and "couldn't complete" not in text and not result.json()['artifacts']
            turn = [e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type') == 'conversation.chat_turn'][-1]
            reports = [r['output'] for r in turn['tool_results'] if r['tool_name'] == 'browser_run_task']
            assert len(reports) == 1 and reports[0]['report_status'] == 'completed', turn
            assert reports[0]['rejection_counts'] == ({} if failure in {'fields', 'absence', 'comparison', 'comparison_document'} else {('value_not_in_quote' if failure == 'value' else 'quote_not_captured'): 1})
            if failure not in {'fields', 'absence', 'comparison', 'comparison_document'}: assert reports[0]['observations'][-1]['quote'] == '候補 A\n10:45\n€\n73\n2h 17m'
            assert model.browser_calls == (1 if failure in {'value', 'fields', 'absence', 'comparison', 'comparison_document'} else 3)
            if failure in {'comparison', 'comparison_document'}: assert model.review_calls >= 1 and '候補 B' in text and '84' in text and '3h 05m' in text
            if failure == 'absence': assert model.review_calls >= 1 and '配達予定日の表示がありません' in text
            if failure == 'fields': assert model.review_calls >= 1 and text.count('- **') == 1
            if failure == 'value':
                assert reports[0]['evidence_value_repair_count'] == 1
                assert model.review_calls >= 1, 'An exact but irrelevant quote must not bypass outcome review'
            history = client.get('/api/chat/history/'+conversation, params={'turn_ids': json.dumps([turn['turn_id']])})
            assert history.json()['terminal_replies'][0]['text'] == text
        wire = []
        class Message:
            chat = SimpleNamespace(id=809); message_id = 5
            async def reply_text(self, text, **kwargs): wire.append(text); return SimpleNamespace(message_id=6)
        asyncio.run(_deliver_reply(Message(), text, principal_id='telegram:809'))
        assert wire and '73' in wire[0] and '2h 17m' in wire[0]
        if failure in {'comparison', 'comparison_document'}: assert '候補 B' in wire[0] and '84' in wire[0]
    finally:
        server.shutdown(); server.server_close()


def test_rejection_at_the_real_sdk_step_limit_stays_bounded_without_parsing_feedback_as_a_report(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask
    from nullion.plugins.browser_plugin.browser_use_worker import run_isolated_browser_task
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    python = os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    if not python: pytest.skip('Isolated SDK dependency required')
    for key, value in {'NULLION_HOME': str(tmp_path / 'home'), 'NULLION_BROWSER_BLOCK_PRIVATE': 'false',
            'NULLION_BROWSER_HEADLESS': 'true', 'NULLION_BROWSER_USE_SANDBOX': 'false',
            'NULLION_WORKSPACE_STORAGE_ROOT': str(tmp_path / 'workspaces')}.items(): monkeypatch.setenv(key, value)
    monkeypatch.delenv('NULLION_BROWSER_USE_CDP_URL', raising=False)
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers(); self.wfile.write(b'<h1>Option A</h1><p>$73</p>')
        def log_message(self, *_): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    class Model:
        model = 'external-provider-fixture'
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            assert self.calls == 1, 'Step exhaustion must remain bounded'
            schema = json.loads(next(m['content'] for m in kwargs['messages']
                if isinstance(m.get('content'), str) and m['content'].startswith('Return valid JSON matching this schema: ')).split(': ', 1)[1])
            payload = {'evaluation_previous_goal': 'Read the page', 'memory': '', 'next_goal': 'Report', 'action': [
                {'done': {'data': {'goal_status': 'answered', 'next_action': 'report', 'observations': [
                    {'label': '価格', 'value': '$9', 'quote': 'invented quote'}]}, 'success': True}}]}
            if 'thinking' in schema.get('properties', {}): payload['thinking'] = 'Read only'
            for key in ('current_plan_item', 'plan_update'):
                if key in schema.get('properties', {}): payload[key] = None
            return response(json.dumps(payload))
    model = Model()
    try:
        output = run_isolated_browser_task(BrowserTask(task='価格を調べて',
            url=f'http://localhost:{server.server_port}/', max_steps=1), client=model,
            principal_id='different-workspace', policy=BrowserPolicy(), timeout=30, python=python)
        assert model.calls == 1
        assert output['report_status'] == 'blocked' and not output['observations']
        assert output['validation_errors'][0]['reason'] == 'quote_not_captured'
        assert 'step limit' in output['blocker'] and 'ValidationError' not in output['blocker']
        assert output['continuation']['kind'] == 'alternative_source'
    finally:
        server.shutdown(); server.server_close()
