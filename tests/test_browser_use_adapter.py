from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy, BrowserPolicyViolation
from nullion.plugins.browser_plugin.browser_use_adapter import BrowserReport, BrowserTask, PageEvidence, browser_task_handler
from nullion.tools import ToolInvocation, ToolRegistry


def test_structured_task_accepts_non_english_objectives_without_routing():
    for objective in ('比較して', 'Compara estos resultados', 'قارن النتائج'):
        assert BrowserTask(task=objective, url='https://example.com').task == objective
    with pytest.raises(ValidationError):
        BrowserTask(task='compare', url='https://example.com', max_steps=999)
    with pytest.raises(ValidationError):
        BrowserTask(task='compare', url='https://example.com', purchase=True)


def test_ungrounded_claims_are_rejected_and_partial_evidence_survives_timeout(tmp_path, monkeypatch):
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    evidence = PageEvidence('operator', BrowserPolicy())
    state = SimpleNamespace(url='https://example.com/', title='Listings',
        dom_state=SimpleNamespace(llm_representation=lambda: 'Sedan A\n$80\nTotal'),
        screenshot=base64.b64encode(b'\x89PNG\r\n\x1a\nfixture').decode())
    asyncio.run(evidence.capture(state, step=1))
    observation = {'label': 'Sedan A price', 'value': '$80', 'quote': 'Sedan A\n$80\nTotal', 'source_url': state.url}
    assert evidence.retain(BrowserReport(goal_status="partial", next_action="report", observations=[observation], unknowns=['Fuel type'])) == 1
    assert evidence.retain(BrowserReport(goal_status="partial", next_action="report", observations=[{**observation, 'value': '$20'}], unknowns=['Fuel type'])) == 0
    assert evidence.retain(BrowserReport(goal_status="partial", next_action="report", observations=[{**observation, 'source_url': 'https://other.com'}], unknowns=['Fuel type'])) == 0
    output = evidence.result(error='Browser task reached its time limit', elapsed=6, steps=2, done=False)
    assert output['report_status'] == 'partial'
    assert output['observations'][0]['value'] == '$80'
    assert output['unknowns'] == ['Fuel type']
    assert output['rejected_observations'] == 2
    assert Path(output['artifact_paths'][0]).read_bytes().startswith(b'\x89PNG')
    assert 'task is still open' not in str(output)


def test_empty_success_is_blocked_and_unused_captures_are_removed(tmp_path, monkeypatch):
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    evidence = PageEvidence('operator', BrowserPolicy())
    for step in range(3):
        state = SimpleNamespace(url='https://example.com/', title='Loading', dom_state=None,
            screenshot=base64.b64encode(b'\x89PNG\r\n\x1a\nfixture' + bytes([step])).decode())
        asyncio.run(evidence.capture(state, step=step))
    captures = [Path(p['path']) for p in evidence.pages]
    result = evidence.result(error=None, elapsed=5, steps=3, done=True)
    assert result['report_status'] == 'blocked'
    assert result['blocker'] == 'No source-backed observations were captured'
    assert [p.exists() for p in captures] == [False, False, True]


def test_private_url_fails_before_model_or_browser_access():
    def unexpected():
        pytest.fail('Blocked target must not load the model/browser')
    invocation = ToolInvocation('call', 'browser_run_task', 'operator', {'task': '比較', 'url': 'http://127.0.0.1/'})
    result = browser_task_handler(unexpected, BrowserPolicy())(invocation)
    assert result.status == 'failed'
    assert result.output['report_status'] == 'blocked'


def test_missing_dependency_keeps_existing_browser_tools(monkeypatch):
    from nullion.plugins.browser_plugin import register_browser_tools
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available', lambda: False)
    registry = ToolRegistry()
    register_browser_tools(registry, model_client_getter=lambda: None)
    assert registry.can_invoke_tool('browser_navigate')
    assert not registry.can_invoke_tool('browser_run_task')


def test_rendered_text_grounds_values_split_by_dom_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    state = SimpleNamespace(url='https://example.com/', title='Listings',
        dom_state=SimpleNamespace(llm_representation=lambda: '[8]<span /> $80 [9]<div /> Total'), screenshot=None)
    async def visible_text():
        return {'url': state.url, 'text': 'Sedan A\n$80\nTotal'}
    evidence = PageEvidence('operator', BrowserPolicy(), text_capture=visible_text)
    asyncio.run(evidence.capture(state))
    claim = {'label': '価格', 'value': '$80', 'quote': '$80 Total', 'source_url': state.url}
    assert evidence.retain(BrowserReport(goal_status="partial", next_action="report", observations=[claim])) == 1
    claim['value'] = '$20'
    assert evidence.retain(BrowserReport(goal_status="partial", next_action="report", observations=[claim])) == 0
    async def different_page():
        return {'url': 'https://other.com/', 'text': '$80 Total'}
    other = PageEvidence('operator', BrowserPolicy(), text_capture=different_page)
    asyncio.run(other.capture(state))
    claim['value'] = '$80'
    assert other.retain(BrowserReport(goal_status="partial", next_action="report", observations=[claim])) == 0


@pytest.mark.parametrize('label,value', [('予約時間', '19:30'), ('Precio', '$84'), ('المخزون', '12')])
def test_source_ids_and_control_values_survive_long_urls_and_rejection_feedback(label, value):
    url = 'https://example.com/options?state=' + 'x' * 6000
    state = SimpleNamespace(url=url, title='Options', dom_state=SimpleNamespace(llm_representation=lambda: value), screenshot=None)
    async def current_page():
        return {'url': url, 'text': value, 'controls': [{'label': label, 'value': value, 'text': '', 'disabled': False}]}
    evidence = PageEvidence('operator', BrowserPolicy(), text_capture=current_page)
    asyncio.run(evidence.capture(state))
    report = dict(goal_status='answered', next_action='report', observations=[{'label': label, 'value': value, 'quote': value, 'source_url': url[:300]}])
    assert evidence.retain(BrowserReport(**report)) == 0
    assert evidence.last_rejections[0]['reason'] == 'source_not_captured'
    report['observations'][0].pop('source_url')
    report['observations'][0].update(source_id=evidence.page_state()['source_id'], control_index=0)
    assert evidence.retain(BrowserReport(**report)) == 1
    assert evidence.observations[0]['source_url'] == url
    assert evidence.observations[0]['control_state']['value'] == value
    recovered = evidence.result(error=None, elapsed=1, steps=2, done=True)
    assert recovered['report_status'] == 'completed'
    assert recovered['rejected_observations'] == 1, 'Rejected attempts stay diagnostic after correction'
    partial = {**report, 'goal_status': 'partial'}
    evidence.retain(BrowserReport(**partial))
    assert evidence.result(error=None, elapsed=1, steps=3, done=True)['report_status'] == 'partial'
    report['observations'][0]['quote'] = 'unsupported'
    assert evidence.retain(BrowserReport(**report)) == 0
    assert evidence.last_rejections[0]['reason'] == 'quote_not_captured'
    assert evidence.rejection_counts == {'source_not_captured': 1, 'quote_not_captured': 1}


def test_control_quotes_cannot_move_to_another_page_or_invalid_source_id():
    evidence = PageEvidence('operator', BrowserPolicy())
    state = SimpleNamespace(url='https://example.com/', title='Options', dom_state=None, screenshot=None)
    asyncio.run(evidence.capture(state))
    for source_id in (999, 0):
        report = BrowserReport(goal_status='answered', next_action='report', observations=[{'label': '選択', 'value': '19:30', 'quote': '19:30', 'source_id': source_id, 'control_index': 0}])
        assert evidence.retain(report) == 0


def test_isolated_worker_cleanup_does_not_wait_for_abandoned_model_request(tmp_path, monkeypatch):
    import json
    import subprocess
    import sys
    import threading
    import time
    from nullion.plugins.browser_plugin import browser_use_worker as worker
    marker = tmp_path / 'model-started'
    release = threading.Event()
    request_started = threading.Event()
    class Client:
        model = 'external-boundary-fixture'
        def create(self, **kwargs):
            request_started.set()
            marker.touch()
            release.wait(5)
            return {'content': []}
    program = tmp_path / 'external_worker.py'
    program.write_text('''import json,os,sys,threading,time
from pathlib import Path
from urllib.request import Request,urlopen
job=json.loads(Path(sys.argv[1]).read_text())
def request():
    req=Request(job['bridge_url'], data=json.dumps({'messages': [], 'system': ''}).encode(), headers={'Authorization': 'Bearer '+job['token'], 'Content-Type': 'application/json'})
    try: urlopen(req, timeout=10).read()
    except Exception: pass
threading.Thread(target=request).start()
while not Path(sys.argv[3]).exists(): time.sleep(.01)
Path(sys.argv[2]).write_text(json.dumps({'report_status': 'partial', 'observations': [], 'blocker': 'External request cancelled'}))
os._exit(0)
''')
    original = subprocess.Popen
    def launch(args, **kwargs):
        return original([sys.executable, str(program), *args[-2:], str(marker)], **kwargs)
    monkeypatch.setattr(worker.subprocess, 'Popen', launch)
    monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON', sys.executable)
    results = []
    failures = []
    def run():
        try:
            results.append(worker.run_isolated_browser_task(BrowserTask(task='比較', url='https://example.com'), client=Client(), principal_id='operator', policy=BrowserPolicy(), timeout=3))
        except Exception as error:
            failures.append(error)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        assert request_started.wait(timeout=3), "External fixture request did not start"
        started = time.monotonic()
        thread.join(timeout=1.5)
        assert not thread.is_alive(), 'Bridge cleanup waited for the abandoned external request'
        assert not failures and results[0]['report_status'] == 'partial', failures
        assert time.monotonic() - started < 1.5
    finally:
        release.set()
        thread.join(timeout=5)


def test_large_source_report_preserves_typed_facts_and_shared_sources():
    import json
    from nullion.source_observation_contract import compact_source_observations
    report = {'result_kind': 'source_observations', 'report_status': 'partial', 'unknowns': ['燃料'],
        'observations': [{'label': f'項目 {index}', 'value': '$80', 'quote': '$80 Total', 'source_url': 'https://example.com/' + 'x' * 700, 'evidence_path': '/evidence.png', 'captured_at': 123.0} for index in range(50)]}
    assert len(json.dumps(report)) > 12_000
    compact = compact_source_observations(report)
    assert len(compact['observations']) == 50 and len(compact['sources']) == 1
    assert compact['sources'][0]['source_url'] == report['observations'][0]['source_url']
    assert compact['observations'][-1]['value'] == '$80' and compact['unknowns'] == ['燃料']
    assert len(json.dumps(compact, ensure_ascii=False)) < 12_000
    assert compact_source_observations(compact) == compact
    huge = {**report, 'observations': [{**report['observations'][0], 'quote': 'x' * 20_000}]}
    omitted = compact_source_observations(huge)
    assert omitted['omitted_observation_count'] == 1 and omitted['report_status'] == 'blocked'
    assert not omitted['observations']


def test_worker_cancellation_targets_only_its_owned_process_tree(monkeypatch):
    import signal
    from nullion.plugins.browser_plugin import browser_use_worker as worker
    calls = []
    process = SimpleNamespace(pid=4242)
    monkeypatch.setattr(worker.platform, 'system', lambda: 'Linux')
    monkeypatch.setattr(worker.os, 'killpg', lambda pid, value: calls.append((pid, value)))
    worker._signal_worker(process)
    worker._signal_worker(process, force=True)
    assert calls == [(4242, signal.SIGTERM), (4242, signal.SIGKILL)]
    monkeypatch.setattr(worker.platform, 'system', lambda: 'Windows')
    monkeypatch.setattr(worker.subprocess, 'run', lambda args, **kwargs: calls.append((args, kwargs)))
    worker._signal_worker(process, force=True)
    assert calls[-1][0] == ['taskkill', '/PID', '4242', '/T', '/F']
    assert calls[-1][1]['timeout'] == 5 and calls[-1][1]['check'] is False


def test_installer_environment_is_discovered_per_lane_without_model_calls(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin.browser_use_adapter import browser_use_available, browser_use_python
    from nullion.plugins.browser_plugin.browser_use_setup import worker_python
    monkeypatch.delenv('NULLION_BROWSER_USE_PYTHON', raising=False)
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    installed = worker_python(tmp_path)
    installed.parent.mkdir(parents=True)
    installed.write_text('installed interpreter')
    installed.chmod(0o700)
    assert browser_use_python() == str(installed)
    assert browser_use_available()
    monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON', str(tmp_path / 'missing'))
    assert not browser_use_available(), 'An explicit broken override must not silently change environments'


def test_setup_installs_only_worker_dependencies_and_validates_browser(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin import browser_use_setup as setup
    calls = []
    monkeypatch.setattr(setup.subprocess, 'run', lambda args, **kw: calls.append((args, kw)))
    result = setup.install_worker(tmp_path)
    assert result == setup.worker_python(tmp_path)
    assert calls[0][0] == [setup.sys.executable, '-m', 'venv', str(result.parent.parent)]
    assert all(call[0][0] == str(result) for call in calls[1:])
    assert any(call[0][2:4] == ['pip', 'check'] for call in calls)
    assert any(call[0][2:5] == ['playwright', 'install', 'chromium'] for call in calls)
    assert all(call[1]['check'] and call[1]['timeout'] <= 1200 for call in calls)
    result.parent.mkdir(parents=True, exist_ok=True)
    result.write_text('existing')
    calls.clear()
    assert setup.install_worker(tmp_path) == result
    assert all(call[0][0] == str(result) for call in calls), 'Existing worker must be reused'


def test_partial_source_delivery_requires_facts_unknowns_and_completed_scope(monkeypatch):
    from nullion import agent_orchestrator as agent
    from nullion.source_observation_contract import partial_source_report_disclosures, partial_source_report_reply
    from nullion.tools import ToolResult
    result = ToolResult('observed', 'source_reader', 'completed', {
        'result_kind': 'source_observations', 'report_status': 'partial',
        'observations': [{'value': '$80', 'quote': '$80 Total'}],
        'unknowns': ['表示されていない項目', 'Dato no mostrado']})
    scope = {'missing_callable_required_tool_names': [], 'unavailable_required_tool_names': [], 'connector_source_unavailable': False}
    monkeypatch.setattr(agent, '_completion_review_scope_contract', lambda *a: scope)
    monkeypatch.setattr(agent, '_scheduler_action_contract_missing', lambda **kw: None)
    monkeypatch.setattr(agent, '_turn_has_artifact_delivery_contract', lambda state: False)
    assert agent._partial_source_report_delivery_ready({}, [result])
    assert partial_source_report_disclosures([result]) == ['表示されていない項目', 'Dato no mostrado']
    assert '$80 Total' in partial_source_report_reply([result],draft={'intro':'','points':[{'evidence_ids':['e0.0'],'detail':'$80 Total','relevance':'Requested price'}],'limitation_ids':['u0']})
    scope['missing_callable_required_tool_names'] = ['send_receipt']
    assert not agent._partial_source_report_delivery_ready({}, [result])
    scope['missing_callable_required_tool_names'] = []
    monkeypatch.setattr(agent, '_turn_has_artifact_delivery_contract', lambda state: True)
    monkeypatch.setattr(agent, '_completed_required_artifact_paths_for_turn', lambda *args: [])
    assert not agent._partial_source_report_delivery_ready({}, [result]), 'Missing required attachment must still be repaired'
    result.output['observations'] = []
    assert not partial_source_report_disclosures([result]), 'Unknowns alone are not a usable result'
