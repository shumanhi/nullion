"""Real dispatch retains its running acknowledgement when planner cards are off."""
import json
import os
import threading

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container workflow E2E')


@pytest.mark.parametrize('prompt', ['Compara las dos opciones.', '二つの選択肢を比較してください。'])
def test_running_dispatch_without_cards_returns_shared_acknowledgement(tmp_path, monkeypatch, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
    from nullion.chat_text import WORKING_ACK_SUMMARY
    from nullion.preferences import save_preferences
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.task_decomposer import _DECOMPOSE_SYSTEM_PROMPT
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'emoji_level':'none', 'markdown_style':'plain'})
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', 'true')
    monkeypatch.setenv('NULLION_MULTI_AGENT_ENABLED', 'true')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    registry = ToolRegistry()
    runtime.active_tool_registry = registry
    release = threading.Event()
    class Model:
        def create(self, **kwargs):
            if _DECOMPOSE_SYSTEM_PROMPT in str(kwargs.get('system', '')):
                return {'stop_reason':'end_turn', 'content':[{'type':'text', 'text':json.dumps({
                    'disposition':'parallel_mission', 'needs_clarification':False,
                    'clarification_question':None, 'routing_evidence':['model_structured_plan'],
                    'tasks':[{'title':title, 'description':title, 'tool_scope':[],
                        'priority':'normal', 'dependencies':[], 'context_key_in':None,
                        'context_key_out':None, 'required_inputs':[], 'can_start':True}
                        for title in ['選択肢 A', '選択肢 B']]})}]}
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            assert release.wait(10)
            return {'stop_reason':'end_turn', 'content':[{'type':'text', 'text':'結果: A37 and B73.'}]}
    model = Model()
    orchestrator = AgentOrchestrator(model_client=model)
    delivered = []
    async def deliver(conversation_id, text, **kwargs):
        delivered.append((text, kwargs))
        return {'message_id':'fixture', 'status':'sent'}
    orchestrator.set_deliver_fn(deliver)
    try:
        reply = run_platform_chat_request(runtime, PlatformChatRequest(platform='telegram',
            text='/planner ' + prompt, chat_id='100001', conversation_id='telegram:100001',
            settings=NullionSettings(telegram=TelegramSettings(chat_enabled=True, operator_chat_id='100001')),
            model_client=model, agent_orchestrator=orchestrator,
            allow_mini_agents=True, append_activity_trace=False))
        assert reply.text == WORKING_ACK_SUMMARY
        assert getattr(reply, 'task_group_id', None)
        assert reply.mini_agent_dispatch_terminal is False
        runs = runtime.store.list_mini_agent_runs()
        assert len(runs) == 2
        assert any(str(getattr(run.status, 'value', run.status)) in {'pending','running'} for run in runs)
        events = runtime.store.list_conversation_events('telegram:100001')
        turn = next(row for row in reversed(events) if row.get('event_type') == 'conversation.chat_turn')
        assert any(row.get('output', {}).get('task_group_id') == reply.task_group_id
            for row in turn.get('tool_results', []))
    finally:
        release.set()
        orchestrator.shutdown_dispatcher_sync()
