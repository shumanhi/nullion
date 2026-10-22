from __future__ import annotations
import asyncio
import threading
from types import SimpleNamespace
from pathlib import Path
import pytest
from nullion.telegram_app import _send_or_edit_telegram_status_message
from nullion.agent_orchestrator import _artifact_paths_from_tool_result
from nullion.plugins.browser_plugin.browser_use_adapter import browser_task_handler
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.source_observation_contract import compact_source_observations, pending_source_continuation
from nullion.tools import ToolInvocation, ToolResult

@pytest.mark.parametrize('text', ['◐ Running', '◐ 実行中', '◐ En curso'])
def test_status_creation_is_serialized_across_event_loops(text):
    messages, texts, locks = {}, {}, {}
    calls=[]; started=threading.Event(); errors=[]
    class Bot:
        async def send_message(self, chat_id, text, **kwargs):
            calls.append(('send', text)); started.set(); await asyncio.sleep(.08)
            return SimpleNamespace(message_id=7)
        async def edit_message_text(self, **kwargs): calls.append(('edit', kwargs['message_id']))
    async def deliver(value):
        await _send_or_edit_telegram_status_message(Bot(), messages, chat_id='different-user',
            group_id='same-run', text=value, status_texts=texts, status_locks=locks)
    def background():
        try: asyncio.run(deliver(text))
        except Exception as e: errors.append(e)
    thread=threading.Thread(target=background);thread.start();assert started.wait(1)
    asyncio.run(deliver(text));thread.join(2)
    asyncio.run(deliver('✓ Done'))
    assert not errors and not thread.is_alive()
    assert calls == [('send', text), ('edit', 7)]

@pytest.mark.parametrize('requested', [(), ('.pdf',), ('.png',)])
def test_browser_evidence_role_comes_from_typed_delivery_contract(tmp_path, monkeypatch, requested):
    from nullion.plugins.browser_plugin import browser_use_worker
    path=tmp_path/'arbitrary-name.png';path.write_bytes(b'png')
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python', lambda: '/worker/python')
    monkeypatch.setattr(browser_use_worker, 'run_isolated_browser_task', lambda *a,**kw: {
        'result_kind':'source_observations', 'report_status':'completed', 'artifact_paths':[str(path)],
        'observations':[], 'unknowns':[]})
    result=browser_task_handler(lambda: object(), BrowserPolicy())(ToolInvocation('i','browser_run_task','different-user',
        {'task':'調べて', 'url':'https://example.net'}, flow_context={'requested_artifact_extensions':requested}))
    assert result.status == 'completed'
    delivered=_artifact_paths_from_tool_result(result)
    if '.png' in requested:
        assert len(delivered)==1
        assert Path(delivered[0]).read_bytes()==path.read_bytes()
        assert Path(delivered[0]).parent.name=='artifacts'
        assert result.output['artifact_descriptors'][0]['role']=='deliverable'
    else:
        assert delivered==[]
        assert result.output['artifact_descriptors'][0]['role']=='source'
    compact=compact_source_observations(result.output)
    assert compact['artifact_descriptors'] == result.output['artifact_descriptors']


def test_typed_alternative_source_continuation_survives_compaction():
    first=ToolResult('a','browser_run_task','completed',{'result_kind':'source_observations','report_status':'partial',
        'observations':[], 'continuation':{'kind':'alternative_source','source_url':'https://first.example','tool_names':['browser_run_task']}})
    assert pending_source_continuation([first])
    assert compact_source_observations(first.output)['continuation'] == first.output['continuation']
    same=ToolResult('b','browser_run_task','completed',{'result_kind':'source_observations','page_url':'https://first.example'})
    assert pending_source_continuation([first,same])
    other=ToolResult('c','browser_run_task','completed',{'result_kind':'source_observations','page_url':'https://second.example'})
    assert pending_source_continuation([first,other]) is None


@pytest.mark.parametrize('status', ['partial', 'completed'])
def test_completed_source_report_is_not_retried_as_missing_history(status):
    from nullion.agent_orchestrator import TurnResult
    from nullion.chat_operator import _run_chat_turn_common_retries, _run_chat_turn_saved_history_retry
    from nullion.tools import ToolRegistry

    result = TurnResult(turn_id='report', final_text='Captured findings', tool_results=[],
                        response_fulfilled=False, response_presentation={'source_report_status': status})
    registry = ToolRegistry()

    def unexpected_retry(*args):
        pytest.fail('A delivered partial report must not restart source lookup')

    common = _run_chat_turn_common_retries(initial_result=result, base_history=[],
        active_tool_registry=registry, user_message='続けて', evidence=None, run_turn=unexpected_retry)
    assert common.result is result
    saved, _, retried = _run_chat_turn_saved_history_retry(initial_result=result,
        automatic_history_context='Earlier lookup', allow_saved_history_retry=True,
        build_history_context=lambda: 'Earlier lookup', base_history=[], active_tool_registry=registry,
        base_tool_registry=registry, turn_tool_evidence=None, run_turn=unexpected_retry)
    assert saved is result and not retried


def test_error_page_quotes_are_blocked_and_honor_explicit_terminal_report(tmp_path):
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserReport, PageEvidence
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        BrowserReport(observations=[])
    evidence = PageEvidence('reader', BrowserPolicy())
    screenshot = tmp_path / 'error.png'
    screenshot.write_bytes(b'png')
    evidence.pages.append({'url': 'https://first.example', 'text': 'アクセスできません',
                           'visible_text': '', 'path': str(screenshot), 'captured_at': 1, 'digest': 'error'})
    evidence.retain(BrowserReport(goal_status='blocked', next_action='report',
        observations=[{'label': '画面', 'value': 'アクセスできません', 'quote': 'アクセスできません',
                       'source_url': 'https://first.example'}], unknowns=['時間']))
    output = evidence.result(error=None, elapsed=1, steps=1, done=True)
    assert output['observations'] == []
    assert output['report_status'] == 'blocked'
    assert output['continuation'] is None
    assert output['blocker']


def test_recovered_report_does_not_present_error_page_as_findings():
    from nullion.source_observation_contract import partial_source_report_reply
    blocked = ToolResult('a', 'browser_run_task', 'completed', {
        'result_kind': 'source_observations', 'report_status': 'blocked',
        'goal_status': 'blocked', 'next_action': 'alternative_source',
        'observations': [{'value': 'アクセスできません', 'quote': 'アクセスできません', 'source_url': 'https://first.example'}]})
    recovered = ToolResult('b', 'browser_run_task', 'completed', {
        'result_kind': 'source_observations', 'report_status': 'partial',
        'observations': [{'value': '19:30', 'quote': '19:30', 'source_url': 'https://second.example'}],
        'unknowns': ['最終予約']})
    reply = partial_source_report_reply([blocked, recovered], draft={'intro':'','points':[{'evidence_ids':['e1.0'],'detail':'','relevance':'Requested time'}],'limitation_ids':['u0']})
    assert '19:30' in reply and 'https://second.example' in reply
    assert 'アクセスできません' not in reply and 'https://first.example' not in reply
    compact = compact_source_observations(blocked.output)
    assert compact['goal_status'] == 'blocked' and compact['next_action'] == 'alternative_source'


def test_long_current_page_url_does_not_evict_requested_facts():
    from nullion.source_observation_contract import partial_source_report_reply
    url = 'https://example.com/booking?state=' + 'x' * 6000
    output = {'result_kind': 'source_observations', 'report_status': 'partial', 'page_url': url,
        'observations': [{'label': '予約時間', 'value': '19:30', 'quote': '19:30', 'source_url': url}], 'unknowns': []}
    compact = compact_source_observations(output)
    assert compact['observations'][0]['value'] == '19:30'
    assert len(__import__('json').dumps(compact)) < 12000
    assert compact['sources'][0]['page_url_ref']
    reply = partial_source_report_reply([ToolResult('a', 'browser_run_task', 'completed', compact)], draft={'intro':'','points':[{'evidence_ids':['e0.0'],'detail':'','relevance':'Requested time'}],'limitation_ids':[]})
    assert '19:30' in reply and url in reply


@pytest.mark.parametrize('user_text', ['空き時間を調べて', 'Compara los precios', 'تحقق من المخزون'])
@pytest.mark.parametrize('delivery,quote,expected', [(False, '', False), (True, 'invented request', False), (True, None, True)])
def test_internal_capture_scope_requires_a_validated_delivery_decision(user_text, delivery, quote, expected):
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    registry = ToolRegistry()
    registry.register(ToolSpec('source_reader', 'Read structured records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, source_artifact_extensions=('.png',)), lambda invocation: None)
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(current_user_message=user_text))
    result, widened = scoped.apply_scope_request(ToolInvocation('scope', 'request_tool_scope', 'operator', {
        'tool_names': ['source_reader'], 'artifact_extensions': ['.png'],
        'source_artifact_delivery_requested': delivery,
        'source_artifact_request_evidence': user_text if quote is None else quote}))
    assert bool(result.output['artifact_extensions']) is expected, result.output
    assert bool(widened.turn_tool_scope_decision.requested_artifact_extensions) is expected


@pytest.mark.parametrize('role', ['source', 'intermediate', 'deliverable'])
def test_history_reconstruction_obeys_artifact_roles(tmp_path, role):
    from nullion.chat_store import _artifact_metadata_from_runtime_turn
    path = tmp_path / 'arbitrary.png'; path.write_bytes(b'png')
    turn = {'tool_results': [{'output': {'artifact_paths': [str(path)],
        'artifact_descriptors': [{'path': str(path), 'role': role}]}}]}
    metadata = _artifact_metadata_from_runtime_turn(turn)
    assert bool(metadata) is (role == 'deliverable')


def test_inherited_capture_requirement_cannot_reappear_after_scope_filter():
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    registry = ToolRegistry()
    registry.register(ToolSpec('source_reader', 'Read records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, source_artifact_extensions=('.png',)), lambda invocation: None)
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(current_user_message='値段を調べて'),
        tool_scope_decision=TurnToolScopeDecision(valid=True, requested_artifact_extensions=('.png',)))
    for number in range(3):
        result, scoped = scoped.apply_scope_request(ToolInvocation(str(number), 'request_tool_scope', 'operator', {
            'tool_names': ['source_reader'], 'artifact_extensions': ['.png']}))
        assert result.output['artifact_extensions'] == []
        assert scoped.turn_tool_scope_decision.requested_artifact_extensions == ()


def test_broad_capability_scope_filters_internal_capture_formats(monkeypatch):
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available', lambda: True)
    registry = ToolRegistry()
    register_browser_tools(registry, model_client_getter=lambda: None)
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(current_user_message='値段を調べて'),
        tool_scope_decision=TurnToolScopeDecision(valid=True, requested_artifact_extensions=('.png',)))
    result, widened = scoped.apply_scope_request(ToolInvocation('scope', 'request_tool_scope', 'operator', {
        'capabilities': ['web'], 'tool_names': ['browser_open'], 'artifact_extensions': ['.png']}))
    assert result.output['artifact_extensions'] == []
    assert widened.turn_tool_scope_decision.requested_artifact_extensions == ()


def test_capture_delivery_proof_survives_scope_widening_in_same_turn():
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    registry = ToolRegistry()
    registry.register(ToolSpec('source_reader', 'Read records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, source_artifact_extensions=('.png',)), lambda invocation: None)
    prompt = '画面を添付してください'
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(current_user_message=prompt))
    _, scoped = scoped.apply_scope_request(ToolInvocation('first', 'request_tool_scope', 'operator', {
        'tool_names': ['source_reader'], 'artifact_extensions': ['.png'],
        'source_artifact_delivery_requested': True, 'source_artifact_request_evidence': prompt}))
    result, scoped = scoped.apply_scope_request(ToolInvocation('next', 'request_tool_scope', 'operator', {
        'tool_names': ['source_reader']}))
    assert result.output['artifact_extensions'] == ['.png']
    assert scoped.turn_tool_scope_decision.source_artifact_request_evidence == prompt


def test_source_compaction_accepts_quoted_observation_without_optional_url():
    value={'result_kind':'source_observations','report_status':'completed',
        'observations':[{'value':'19:30','quote':'19:30'}],'unknowns':[]}
    compact=compact_source_observations(value)
    assert compact['sources']==[{}]
    assert compact['observations'][0]['value']=='19:30'
    assert compact['report_status']=='completed'
