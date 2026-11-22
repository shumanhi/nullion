"""Planner visibility saves independently from activity through the real settings UI."""
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container UI E2E')


def test_planner_setting_round_trips_without_changing_activity(tmp_path, monkeypatch):
    from playwright.sync_api import expect
    from nullion import web_app, auth
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page

    runtime = isolate(tmp_path, monkeypatch)
    env_path = tmp_path / '.env'
    monkeypatch.setattr(web_app, '_find_env_path', lambda: env_path)
    monkeypatch.setattr(web_app, '_read_credentials_json', lambda: {})
    monkeypatch.setattr(web_app, '_write_credentials_json', lambda value: None)
    monkeypatch.setattr(auth, 'load_stored_credentials', lambda: {})
    monkeypatch.setenv('NULLION_ENV_FILE', str(env_path))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'task')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'false')
    with _UvicornThread(web_app.create_app(runtime, None, ToolRegistry()), _free_port()) as server:
        pw, browser, page, _ = _open_page(server.base_url)
        try:
            page.evaluate("openSettings(); showTab('execution');")
            planner = page.locator('#cfg-planner-enabled')
            verbose = page.locator('#cfg-verbose-enabled')
            expect(planner).to_be_checked()
            expect(verbose).not_to_be_checked()
            for enabled in (False, True):
                planner.locator('..').click()
                assert planner.is_checked() is enabled
                with page.expect_response(lambda response: response.url == server.base_url + '/api/config'
                        and response.request.method == 'POST') as saved:
                    page.evaluate('saveConfig()')
                assert saved.value.status == 200, saved.value.text()
                assert saved.value.json()['ok'] is True
                expect(page.locator('#save-feedback')).to_have_class('ok')
                config = page.request.get(server.base_url + '/api/config').json()
                assert config['task_planner_feed'] is enabled
                assert config['activity_trace'] is False
                mode = 'task' if enabled else 'off'
                assert f'NULLION_TASK_PLANNER_FEED_MODE="{mode}"' in env_path.read_text()
                page.evaluate('loadConfig({force: true})')
                assert planner.is_checked() is enabled
                expect(verbose).not_to_be_checked()
            invalid = page.request.post(server.base_url + '/api/config', data={'task_planner_feed': 'off'})
            assert invalid.status == 400
            assert page.request.get(server.base_url + '/api/config').json()['task_planner_feed'] is True
        finally:
            browser.close()
            pw.stop()


def test_websocket_native_dispatch_defaults_activity_from_chat_setting(tmp_path, monkeypatch):
    import json
    import threading
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import set_activity_trace_enabled_for_chat
    from nullion.task_decomposer import _DECOMPOSE_SYSTEM_PROMPT
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', 'true')
    monkeypatch.setenv('NULLION_MULTI_AGENT_ENABLED', 'true')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'task')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'true')
    conversation = 'web:planner-default-activity'
    set_activity_trace_enabled_for_chat(runtime, chat_id=conversation, enabled=False)
    registry = ToolRegistry()
    runtime.active_tool_registry = registry
    release = threading.Event()

    class Model:
        def create(self, **kwargs):
            if _DECOMPOSE_SYSTEM_PROMPT in str(kwargs.get('system', '')):
                return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': json.dumps({
                    'disposition': 'parallel_mission', 'needs_clarification': False,
                    'clarification_question': None, 'routing_evidence': ['model_structured_plan'],
                    'tasks': [{'title': title, 'description': title, 'tool_scope': [],
                        'priority': 'normal', 'dependencies': [], 'context_key_in': None,
                        'context_key_out': None, 'required_inputs': [], 'can_start': True}
                        for title in ['選択肢 A', '選択肢 B']]})}]}
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            assert release.wait(10)
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': '結果: A37 and B73.'}]}

    orchestrator = AgentOrchestrator(model_client=Model())
    try:
        with TestClient(create_app(runtime, orchestrator, registry)) as client, client.websocket_connect('/ws/chat') as socket:
            socket.send_json({'text': '/planner 二つの選択肢を比較してください。',
                'conversation_id': conversation, 'turn_id': 'turn:planner-default', 'stream': False})
            events = []
            try:
                while True:
                    event = socket.receive_json()
                    events.append(event)
                    if event.get('type') == 'done' and event.get('turn_id') == 'turn:planner-default':
                        break
                assert not [event for event in events if event.get('type') == 'error'], events
                summaries = [event for event in events if event.get('type') == 'task_status'
                    and event.get('status_kind') == 'task_summary'
                    and event.get('turn_id') == 'turn:planner-default']
                assert summaries, events
                summary = summaries[-1]
                assert summary['conversation_id'] == conversation
                assert summary['include_activity'] is False
                assert summary['group_id'] and '選択肢 A' in summary['text'] and '選択肢 B' in summary['text']
                runs = runtime.store.list_mini_agent_runs()
                assert len(runs) == 2
                assert any(str(getattr(run.status, 'value', run.status)) in {'pending', 'running'} for run in runs)
                turns = runtime.store.list_conversation_events(conversation)
                assert any(row.get('output', {}).get('task_group_id') == summary['group_id']
                    for turn in turns if turn.get('event_type') == 'conversation.chat_turn'
                    for row in turn.get('tool_results', []))
            finally:
                release.set()
    finally:
        release.set()
        orchestrator.shutdown_dispatcher_sync()
