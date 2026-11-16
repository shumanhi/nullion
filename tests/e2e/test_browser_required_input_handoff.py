"""Real Web/SQLite -> native browser read -> SDK -> shared user clarification."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from tests.browser_model_protocol import browser_schema_messages
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='container browser E2E')


@pytest.mark.parametrize('question,choices', [
    ('Which exact destination within the saved area?', ['Specify destination', 'Keep unchanged']),
    ('保存された地域内のどの場所ですか？', ['場所を指定', '変更しない']),
])
@pytest.mark.parametrize('observed_snapshot',[False,True])
def test_current_page_prerequisite_pauses_sdk_and_reaches_numbered_web_reply(tmp_path, monkeypatch, question, choices,observed_snapshot):
    worker = os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    if worker:
        monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON', worker)
    else:
        pytest.importorskip('browser_use')
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'<h1>Delivery destination</h1><label>Destination<input value="10024 Martense Ave"></label><p>Choose an exact destination</p>')
        def log_message(self, *_): pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://localhost:{server.server_port}/'
    for key,value in {'NULLION_BROWSER_BLOCK_PRIVATE':'false', 'NULLION_BROWSER_HEADLESS':'true',
                      'NULLION_BROWSER_BACKEND':'playwright', 'NULLION_BROWSER_USE_SANDBOX':'false',
                      'NULLION_BROWSER_USE_ISOLATED':'false', 'ANONYMIZED_TELEMETRY':'false',
                      'NULLION_WORKSPACE_STORAGE_ROOT':str(tmp_path/'workspaces')}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *a, **kw: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *a, **kw: None)

    class Model:
        model = 'external-controlled-provider'
        outer_calls = 0
        browser_calls = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            schemas = browser_schema_messages(kwargs)
            if schemas:
                self.browser_calls += 1
                assert self.browser_calls == 1, 'Required clarification must stop without another browser model pass'
                captured = next(m['content'] for m in kwargs['messages']
                    if isinstance(m.get('content'), str) and m['content'].startswith('Current captured page evidence'))
                assert '10024 Martense Ave' in captured and '10024' in json.dumps(kwargs['messages'])
                schema = json.loads(schemas[-1]['content'].split(': ', 1)[1])
                # Source ids are supplied by the actual page capture, never synthesized by a mocked tool.
                source = json.JSONDecoder().raw_decode(captured[captured.index('{'):])[0]
                report = {'goal_status':'blocked', 'next_action':'user_input', 'observations':[],
                    'blocker':'The suggested street does not establish the requested delivery area.',
                    'user_input':{'reason':'constraint_mismatch', 'source_id':source['source_id'],
                        'evidence_quote':'Choose an exact destination', 'question':question, 'options':choices}}
                value = {'evaluation_previous_goal':'Captured required selection', 'memory':'Keep saved area',
                         'next_goal':'Ask only for missing precision', 'action':[{'record_observations':report}]}
                if 'thinking' in schema.get('properties', {}): value['thinking'] = 'Use the current captured prerequisite'
                for key in ('current_plan_item','plan_update'):
                    if key in schema.get('properties', {}): value[key] = None
                return {'stop_reason':'tool_use', 'content':[{'type':'tool_use', 'id':'decision',
                    'name':'browser_decision', 'input':value}]}
            self.outer_calls += 1
            tools = {t['name'] for t in kwargs.get('tools', [])}
            if self.outer_calls == 1:
                assert 'request_tool_scope' in tools
                name, args = 'request_tool_scope', {'capabilities':['web'], 'tool_names':['browser_run_task',
                    *(['browser_open','browser_navigate','browser_snapshot'] if observed_snapshot else [])],
                    'required_tool_names':['browser_run_task'], 'artifact_extensions':[]}
            elif self.outer_calls==2 and observed_snapshot:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'observed-'+name,'name':name,'input':args}
                    for name,args in [('browser_open',{'session_id':'observed'}),
                        ('browser_navigate',{'session_id':'observed','url':url}),
                        ('browser_snapshot',{'session_id':'observed'})]]}
            else:
                assert self.outer_calls <= 3, 'Clarification must not trigger an answer rewrite or source-search loop'
                name, args = 'browser_run_task', {'task':'Use saved area 10024. Inspect the required destination control; retain known constraints.',
                                                'url':url, 'max_steps':4,
                    **({'execution_kind':'adaptive_interaction','interaction_reason':'The captured destination control requires an exact selection.'}
                       if self.outer_calls>2 else {})}
            return {'stop_reason':'tool_use', 'content':[{'type':'tool_use','id':f'call-{self.outer_calls}', 'name':name,'input':args}]}

    model = Model()
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.model_client = model
    registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: runtime.model_client)
    runtime.active_tool_registry = registry
    conversation = 'web:required-selection'
    try:
        start = time.monotonic()
        with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
            response = client.post('/api/chat', json={'text':f'{question} {url}', 'conversation_id':conversation})
        assert response.status_code == 200, response.text
        result = response.json()
        assert question in result['text'] and all(f'{i}. {label}' in result['text'] for i,label in enumerate(choices,1)), result
        assert not result['artifacts'] and model.browser_calls == 1 and model.outer_calls == 3
        turns = [e for e in runtime.store.list_conversation_events(conversation) if e['event_type']=='conversation.chat_turn']
        outputs = [t['output'] for t in turns[-1]['tool_results'] if t['tool_name']=='browser_run_task']
        assert len(outputs) == (1 if observed_snapshot else 2)
        if not observed_snapshot:assert outputs[0]['strategy']['kind']=='native_read_preflight'
        assert outputs[-1]['result_kind']=='user_choice_required' and outputs[-1]['requires_user_choice'] is True
        assert outputs[-1]['continuation'] is None and outputs[-1]['captured_documents']
        assert time.monotonic()-start < 30
    finally:
        server.shutdown()
        server.server_close()
