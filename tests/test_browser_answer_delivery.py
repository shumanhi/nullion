"""Browser observations must not become a second answer-verification gate."""
import pytest

from nullion.agent_orchestrator import _completion_review_required
from nullion.response_sanitizer import sanitize_user_visible_reply
from nullion.tools import ToolResult


@pytest.mark.parametrize('reply', ['The page reports 18 available places.', 'La página indica 18 plazas disponibles.', 'ページには空きが18件あります。'])
@pytest.mark.parametrize('output', [
    {'text': '18 available places', 'session_id': 'workspace-a'},
    {'record': {'title': 'Places', 'availability_count': 18}, 'session_id': 'workspace-a'},
    {'items': [{'title': 'Places', 'availability_count': 18}], 'session_id': 'workspace-a'},
])
def test_browser_answer_does_not_require_assertion_bound_records(reply, output):
    results = [ToolResult('read', 'browser_extract_text', 'completed', output)]
    assert sanitize_user_visible_reply(user_message='ข้อมูลล่าสุด', reply=reply, tool_results=results) == reply
    assert not _completion_review_required({}, tool_results=results)


def test_failed_optional_assertion_does_not_discard_other_results():
    results = [ToolResult('read', 'browser_extract_text', 'completed', {'text': 'Service opens at 10:00.'}),
               ToolResult('check', 'browser_assert_page_state', 'completed', {'verified': False, 'result': {'ok': False}})]
    reply = 'Service opens at 10:00. I could not confirm availability.'
    assert sanitize_user_visible_reply(user_message=None, reply=reply, tool_results=results) == reply
    assert not _completion_review_required({}, tool_results=results)


def test_actual_browser_failure_remains_visible_without_verification_retry():
    results = [ToolResult('nav', 'browser_navigate', 'failed', {}, error='Connection refused')]
    reply = 'The page could not be opened: connection refused.'
    assert sanitize_user_visible_reply(user_message=None, reply=reply, tool_results=results) == reply
    assert not _completion_review_required({}, tool_results=results)


def test_successful_navigation_does_not_replace_a_mixed_source_summary():
    results = [ToolResult('nav', 'browser_navigate', 'completed', {'session_id': 'workspace-b', 'result': 'Opened page'}),
               ToolResult('forecast', 'weather_forecast', 'completed', {'current': {'temperature': 18}})]
    reply = 'It is 18°C. The requested page is open.'
    assert sanitize_user_visible_reply(user_message=None, reply=reply, tool_results=results) == reply


def test_browser_result_removal_preserves_raw_payload_and_path_protection():
    results = [ToolResult('read', 'browser_extract_text', 'completed', {'text': 'Page', 'session_id': 'private-session'})]
    assert 'private-session' not in sanitize_user_visible_reply(user_message=None, reply='{"session_id":"private-session","text":"Page"}', tool_results=results)
    assert '/Users/example/private.txt' not in sanitize_user_visible_reply(user_message=None, reply='Read /Users/example/private.txt.', tool_results=results)


@pytest.mark.parametrize('sessions', [('workspace-a',), ('workspace-a', 'workspace-b')])
def test_terminal_delivery_does_not_reopen_or_revalidate_browser_sessions(sessions):
    from nullion.agent_orchestrator import _complete_agent_turn
    from nullion.tools import ToolRegistry

    observations = []
    for session in sessions:
        observations.extend([
            ToolResult(f'{session}-read', 'browser_extract_detail', 'completed', {'session_id': session, 'record': {'title': 'Service', 'price': '18'}}),
            ToolResult(f'{session}-move', 'browser_navigate', 'completed', {'session_id': session, 'result': 'Navigation complete'}),
        ])
    registry = ToolRegistry()
    state = {'tool_results': observations, 'tool_registry': registry, 'artifacts': [], 'principal_id': 'member',
             'conversation_id': 'telegram:member', 'user_message': 'ข้อมูลล่าสุด'}
    update = _complete_agent_turn(state, final_text='The service costs 18.', response_fulfilled=True)
    assert update['result'].final_text == 'The service costs 18.'
    assert update['result'].response_fulfilled is True
    assert update['result'].tool_results == observations


@pytest.mark.parametrize("status", ["completed", "failed"])
def test_later_browser_work_is_not_overwritten_by_an_older_cron_receipt(status):
    from nullion.chat_operator import _completed_run_cron_receipt_reply

    cron = ToolResult("cron", "run_cron", "completed", {"message": "Scheduled result", "delivery_status": "sent"})
    browser = ToolResult("read", "browser_extract_text", status, {"text": "Current page"})
    assert _completed_run_cron_receipt_reply([cron, browser]) is None
    assert _completed_run_cron_receipt_reply([browser, cron]) == "Scheduled result"
    assert _completed_run_cron_receipt_reply([cron]) == "Scheduled result"


@pytest.mark.parametrize("tool_name,status,accepted", [
    ("browser_navigate", "failed", True), ("browser_extract_text", "failed", True),
    ("browser_navigate", "blocked", False), ("browser_navigate", "pending", False),
    ("browser_click_element", "failed", False), ("email_send", "failed", False),
])
def test_failed_read_observation_can_be_explained_without_success_gate(tmp_path, tool_name, status, accepted):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.response_fulfillment_contract import evaluate_response_fulfillment
    runtime = bootstrap_persistent_runtime(tmp_path / 'read-failure.db')
    reply = '接続できませんでした。'
    decision = evaluate_response_fulfillment(store=runtime.store, conversation_id='browser-case',
        user_message='สถานะ', reply=reply, required_tool_names=[tool_name],
        tool_results=[ToolResult('operation', tool_name, status, {}, error='Connection refused')])
    assert decision.satisfied is accepted
    if accepted:
        assert decision.reply == reply


@pytest.mark.parametrize('reply', ['18°C. Route 47 resumes at 10:30.', '18°C。47番路線は10:30に再開します。'])
@pytest.mark.parametrize('dictionary_results', [False, True])
def test_completed_scope_requirement_does_not_force_optional_browser_continuations(reply, dictionary_results):
    from dataclasses import asdict
    from types import SimpleNamespace
    from nullion import chat_operator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass

    registry = ToolRegistry()
    for name in ('weather_forecast', 'browser_extract_text', 'browser_snapshot'):
        registry.register(ToolSpec(name=name, description=name, risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=5,
            continuation_tools=('browser_snapshot',) if name == 'browser_extract_text' else ()),
            lambda invocation: pytest.fail('Completed reads must not execute another tool'))
    results = [
        ToolResult('scope', 'request_tool_scope', 'completed', {'required_tool_names': ['weather_forecast']}),
        ToolResult('weather', 'weather_forecast', 'completed', {'current': {'temperature_2m': 18}}),
        ToolResult('page', 'browser_extract_text', 'completed', {'text': 'Route 47 resumes at 10:30.'}),
    ]
    initial = SimpleNamespace(tool_results=[asdict(r) for r in results] if dictionary_results else results,
        artifacts=[], suspended_for_approval=False, final_text=reply)
    result, active, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial, base_history=[], active_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message='ข้อมูลล่าสุด', conversation_result=None),
        run_turn=lambda *_: pytest.fail('Optional browser verification must not restart the model'))
    assert result is initial
    assert active is registry
    assert not retried


@pytest.mark.parametrize('tool_name', ['browser_extract_items', 'browser_extract_detail', 'browser_run_js'])
def test_browser_observations_are_not_reduced_to_eight_item_repair_sample(tool_name):
    import json
    from nullion.agent_orchestrator import _tool_result_message_payload
    output = {'items': [{'title': f'Record {i}', 'url': 'https://example.test/' + ('x' * 550) + str(i),
                         'nested': {'value': i}} for i in range(20)], 'item_count': 20}
    result = ToolResult('collection', tool_name, 'completed', output)
    payload = json.loads(_tool_result_message_payload(result))
    assert payload['output'] == output
    assert payload['security']['untrusted_tool_output'] is True
