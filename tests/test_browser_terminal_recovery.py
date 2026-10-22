from dataclasses import replace
from types import SimpleNamespace
import asyncio
import threading
import time

import pytest

from nullion import chat_operator
from nullion.response_fulfillment_contract import evaluate_response_fulfillment
from nullion.runtime_store import RuntimeStore
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec, _invoke_tool_handler_with_timeout


@pytest.mark.parametrize('message,reply', [('Details supplied.', 'I could not confirm the selected location.'), ('ข้อมูลเพิ่มเติม', 'ยังยืนยันตำแหน่งที่เลือกไม่ได้')])
def test_failed_read_after_page_action_does_not_force_optional_continuations(message, reply):
    registry = ToolRegistry()
    registry.register(ToolSpec(name='browser_extract_text', description='read', risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ, continuation_tools=('browser_type_id',), requires_approval=False, timeout_seconds=5), lambda _: None)
    initial = SimpleNamespace(final_text=reply, artifacts=[], suspended_for_approval=False, tool_results=[
        ToolResult('scope', 'request_tool_scope', 'completed', {'required_tool_names': ['browser_extract_detail']}),
        ToolResult('click', 'browser_click_id', 'completed', {}),
        ToolResult('detail', 'browser_extract_detail', 'failed', {}, error='No record'),
        ToolResult('read', 'browser_extract_text', 'completed', {'text': 'Partial page records'}),
    ])
    recovered, returned, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial, base_history=[], active_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message=message, conversation_result=None),
        run_turn=lambda *_: pytest.fail('Optional alternatives must not restart a terminal read'))
    assert recovered is initial and returned is registry and not retried


def test_optional_continuations_are_not_all_mandatory():
    registry = ToolRegistry()
    for name in ('browser_navigate', 'browser_extract_items', 'browser_type_field'):
        registry.register(ToolSpec(name=name, description=name, risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=5,
            continuation_tools=('browser_extract_items', 'browser_type_field') if name == 'browser_navigate' else ()), lambda _: None)
    initial = SimpleNamespace(turn_id='initial', final_text='', artifacts=[], suspended_for_approval=False,
        tool_results=[ToolResult('navigate', 'browser_navigate', 'completed', {'session_id': 'one'})])
    def retry(history, scoped):
        assert not scoped.turn_tool_scope_decision.required_tool_names
        assert {t['name'] for t in scoped.list_tool_definitions()} == {'browser_extract_items', 'browser_type_field'}
        return SimpleNamespace(turn_id='retry', final_text='Three current records.', artifacts=[], suspended_for_approval=False,
            tool_results=[ToolResult('read', 'browser_extract_items', 'completed', {'items': [{'title': 'A'}]})])
    recovered, _, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial, base_history=[], active_tool_registry=registry, evidence=None, run_turn=retry)
    assert retried and recovered.final_text == 'Three current records.'


@pytest.mark.parametrize('reason,expected', [('handler_timeout', 'timed out'), ('browser_action_failed', 'page interaction failed')])
def test_failed_required_action_reports_safe_cause(reason, expected):
    decision = evaluate_response_fulfillment(store=RuntimeStore(), conversation_id='case', user_message='続けて',
        reply='Done.', required_tool_names=['browser_type_id'], tool_results=[
            ToolResult('type', 'browser_type_id', 'failed', {'reason': reason, 'message': '/private/token'}, error='secret')])
    assert not decision.satisfied and expected in decision.reply
    assert 'still open' not in decision.reply and 'secret' not in decision.reply and '/private' not in decision.reply


def test_browser_timeout_cancels_coroutine_and_releases_session_lock():
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
    cancelled = threading.Event()
    class Backend(PlaywrightBackend):
        async def run_js(self, session_id, script):
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.set()
        async def extract_text(self, session_id, selector=None):
            return 'Recovered page'
    registry = ToolRegistry()
    register_browser_tools(registry)
    registry._handlers['browser_assert_page_state'].__self__._backend = Backend()
    spec = replace(registry.get_spec('browser_assert_page_state'), timeout_seconds=0.1)
    invocation = ToolInvocation('check', spec.name, 'member', {'required': ['Ready'], 'session_id': 'one'})
    start = time.monotonic()
    result = _invoke_tool_handler_with_timeout(spec, registry._handlers[spec.name], invocation)
    assert result.status == 'failed' and result.output['reason'] == 'handler_timeout'
    assert cancelled.wait(0.5), 'A timed-out browser coroutine must be cancelled'
    read = ToolInvocation('read', 'browser_extract_text', 'member', {'session_id': 'one'})
    recovered = _invoke_tool_handler_with_timeout(registry.get_spec(read.tool_name), registry._handlers[read.tool_name], read)
    assert recovered.status == 'completed' and recovered.output['text'] == 'Recovered page'
    assert time.monotonic() - start < 1
