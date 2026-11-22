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


@pytest.mark.parametrize('goal,next_action,valid,nonempty,finished',[
    ('answered','report',True,True,True),('partial','report',True,True,True),
    ('answered','alternative_source',True,True,False),('answered','report',False,True,False),
    ('answered','report',True,False,False)])
def test_validated_answered_record_is_terminal_without_another_model_decision(tmp_path,monkeypatch,
        goal,next_action,valid,nonempty,finished):
    from nullion.plugins.browser_plugin.browser_use_adapter import _record_observations_result
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    evidence=PageEvidence('operator',BrowserPolicy())
    state=SimpleNamespace(url='https://example.com/',title='記録',
        dom_state=SimpleNamespace(llm_representation=lambda:'Cedar 37'),screenshot=None)
    asyncio.run(evidence.capture(state,step=1))
    observation={'label':'記録','value':'37','quote':'Cedar 37' if valid else 'Invented 37',
        'source_url':state.url}
    report=BrowserReport(goal_status=goal,next_action=next_action,
        observations=[observation] if nonempty else [],unknowns=[])
    result=_record_observations_result(evidence,report,lambda **kw:SimpleNamespace(**kw))
    assert bool(getattr(result,'is_done',False)) is finished
    assert evidence.finished_report is finished
    if finished:assert result.success is (goal=='answered')
    if valid and nonempty:assert evidence.observations[0]['value']=='37'
    if not valid:
        assert evidence.last_rejections
        repaired=report.model_copy(update={'observations':[
            report.observations[0].model_copy(update={'quote':'Cedar 37'})]})
        result=_record_observations_result(evidence,repaired,lambda **kw:SimpleNamespace(**kw))
        assert result.is_done and evidence.finished_report
        assert len(evidence.pages)==1, 'Repair must reuse the captured source'


@pytest.mark.parametrize('invalid', [None, 'incomplete', 'effects', 'public_capture',
    'host_files', 'download', 'delivery', 'no_document', 'empty_document', 'no_source'])
def test_completed_browser_reads_need_native_audit_and_actual_captured_text(invalid):
    from nullion.plugins.browser_plugin.browser_use_adapter import _completed_browser_read_metadata
    output={'execution_audit':{'version':1,'complete':True,'read_only_actions_only':True,
        'captures_are_internal':True,'url_inventory_complete':True,'host_file_actions_enabled':False,
        'downloaded_file_count':0,'deliverable_file_count':0,'action_names':['navigate','read_page_state','done']},
        'captured_documents':[{'source_url':'https://example.test/','main_document_text':'測定値 37'}]}
    flags={'incomplete':'complete','effects':'read_only_actions_only','public_capture':'captures_are_internal'}
    if invalid in flags:output['execution_audit'][flags[invalid]]=False
    elif invalid=='host_files':output['execution_audit']['host_file_actions_enabled']=True
    elif invalid=='download':output['execution_audit']['downloaded_file_count']=1
    elif invalid=='delivery':output['execution_audit']['deliverable_file_count']=1
    elif invalid=='no_document':output['captured_documents']=[]
    elif invalid=='empty_document':output['captured_documents'][0]['main_document_text']=''
    elif invalid=='no_source':output['captured_documents'][0]['source_url']='about:blank'
    receipt=_completed_browser_read_metadata(output)
    if invalid is None:
        assert receipt=={'_native_completed_reads':{'tool_names':['browser_extract_text','browser_navigate'],'read_only':True}}
    else:assert receipt is None


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


def test_private_url_fails_before_model_or_browser_access(monkeypatch):
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_tools._principal_allows_private_host_navigation',lambda _:False)
    def unexpected():
        pytest.fail('Blocked target must not load the model/browser')
    invocation = ToolInvocation('call', 'browser_run_task', 'operator', {'task': '比較', 'url': 'http://127.0.0.1/'})
    result = browser_task_handler(unexpected, BrowserPolicy())(invocation)
    assert result.status == 'failed'
    assert result.output['report_status'] == 'blocked'


@pytest.mark.parametrize('isolated',[False,True])
def test_admin_private_host_permission_is_scoped_to_worker_without_mutating_policy(monkeypatch,isolated):
    from nullion.plugins.browser_plugin import browser_use_adapter as adapter
    from nullion.plugins.browser_plugin import browser_use_worker as worker
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_tools._principal_allows_private_host_navigation',lambda p:p=='admin-fixture')
    monkeypatch.setattr(adapter,'browser_use_python',lambda:'/test/python' if isolated else None)
    policy=BrowserPolicy();policy.block_private=True;policy.blocked_domains=frozenset({'denied.example.test'})
    seen=[]
    def received(task,**kwargs):
        active=kwargs['policy'];seen.append(active)
        assert active is not policy and active.block_private is False
        active.check_url('http://127.0.0.1/local')
        with pytest.raises(BrowserPolicyViolation):active.check_url('https://denied.example.test/')
        return {'artifact_paths':[],'report_status':'partial'}
    async def inline(task,**kwargs):return received(task,**kwargs)
    monkeypatch.setattr(adapter,'run_browser_task',inline)
    monkeypatch.setattr(worker,'run_isolated_browser_task',received)
    handle=browser_task_handler(lambda:object(),policy)
    result=handle(ToolInvocation('admin','browser_run_task','admin-fixture',{'task':'比較','url':'http://127.0.0.1/local'}))
    assert result.status=='completed' and len(seen)==1
    assert policy.block_private is True
    denied=handle(ToolInvocation('viewer','browser_run_task','viewer-fixture',{'task':'Comparar','url':'http://127.0.0.1/local'}))
    assert denied.status=='failed' and len(seen)==1
    assert policy.block_private is True


def test_missing_dependency_keeps_existing_browser_tools(monkeypatch):
    from nullion.plugins.browser_plugin import register_browser_tools
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available', lambda: False)
    registry = ToolRegistry()
    register_browser_tools(registry, model_client_getter=lambda: None)
    assert registry.can_invoke_tool('browser_navigate')
    assert not registry.can_invoke_tool('browser_run_task')


def test_worker_receives_runtime_clock_instead_of_model_provided_date(monkeypatch):
    from nullion.plugins.browser_plugin import browser_use_adapter as adapter
    from nullion.plugins.browser_plugin import browser_use_worker as worker
    monkeypatch.setattr(adapter,'browser_use_python',lambda:'/test/python')
    clock={'accepted_local_date':'2031-04-06','timezone':'America/New_York'}
    seen=[]
    def external(task,**kwargs):
        seen.append(task)
        return {'artifact_paths':[],'report_status':'partial'}
    monkeypatch.setattr(worker,'run_isolated_browser_task',external)
    result=browser_task_handler(lambda:object(),BrowserPolicy())(ToolInvocation('clock','browser_run_task','operator',
        {'task':'比較','url':'https://example.com/','request_time_context':{'accepted_local_date':'2099-01-01'}},
        flow_context={'user_request':'EWR to LAX, 2031-04-08 at 18:00','request_time_context':clock}))
    assert result.status=='completed' and len(seen)==1
    assert seen[0].request_time_context==clock
    objective=adapter._browser_task_objective(seen[0])
    assert '2031-04-06' in objective and '2031-04-08' in objective and '2099' not in objective


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
    assert evidence.result(error=None, elapsed=1, steps=1, done=False)["captured_documents"][0]["visible_text_verified"] is True
    assert other.result(error=None, elapsed=1, steps=1, done=False)["captured_documents"][0]["visible_text_verified"] is False


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
    monkeypatch.setattr(agent, '_completion_review_scope_contract', lambda *a, **kw: scope)
    monkeypatch.setattr(agent, '_scheduler_action_contract_missing', lambda **kw: None)
    monkeypatch.setattr(agent, '_turn_has_artifact_delivery_contract', lambda state: False)
    assert not agent._partial_source_report_delivery_ready({}, [result])
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


@pytest.mark.parametrize('label,detail', [('予約日','2027-01-04 休業'),('Fecha','2027-01-04 Cerrado')])
def test_blocked_report_retains_exact_disabled_control_evidence_without_inventory_success(label,detail):
    from nullion.source_observation_contract import compact_source_observations,source_report_status
    evidence=PageEvidence('operator',BrowserPolicy())
    state=SimpleNamespace(url='https://example.com/calendar',title='Calendar',dom_state=None,screenshot=None)
    async def capture():
        return {'url':state.url,'text':detail,'controls':[{'label':detail,'text':detail,'value':'2027-01-04','disabled':True}]}
    evidence.text_capture=capture
    asyncio.run(evidence.capture(state))
    claim={'label':label,'semantic_role':'interface_control','value':detail,'quote':detail,'source_id':0,'control_index':0}
    assert evidence.retain(BrowserReport(goal_status='blocked',next_action='report',blocker=detail,observations=[claim]))==1
    output=evidence.result(error=None,elapsed=1,steps=1,done=True)
    assert output['report_status']=='blocked'
    record=output['observations'][0]
    assert record['evidence_kind']=='source_blocker' and record['control_state']['disabled'] is True
    assert record['control_state']['value']=='2027-01-04'
    compact=compact_source_observations(output)
    assert compact['observations'][0]['evidence_kind']=='source_blocker'
    assert compact['observations'][0]['quote']==detail and compact['report_status']=='blocked'
    clipped=compact_source_observations({**output,'unknowns':['x'*1500]*5},max_chars=1800)
    assert clipped['omitted_unknown_count']>0 and clipped['report_status']=='blocked'
    result=__import__('nullion.tools',fromlist=['ToolResult']).ToolResult('read','browser_run_task','completed',compact)
    assert source_report_status([result])=='blocked'
    from nullion.source_observation_contract import source_answer_context
    context=source_answer_context([result])
    assert context['evidence'][0]['value']==detail
    assert context['evidence'][0]['evidence_kind']=='source_blocker'
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    assert _completion_review_evidence_result_is_positive(result), 'Captured closure is a fact, never an inventory result'
    from nullion.agent_orchestrator import _project_tool_output_for_repair
    assert _project_tool_output_for_repair('browser_run_task',compact)['observations'][0]['evidence_kind']=='source_blocker'
    assert not _completion_review_evidence_result_is_positive(type(result)('unquoted','browser_run_task','completed',
        {**compact,'observations':[]}))


def test_blocked_report_still_rejects_invented_quote_and_keeps_prior_facts():
    evidence=PageEvidence('operator',BrowserPolicy())
    state=SimpleNamespace(url='https://example.com/',title='Page',dom_state=SimpleNamespace(llm_representation=lambda:'A $80'),screenshot=None)
    asyncio.run(evidence.capture(state))
    evidence.retain(BrowserReport(goal_status='partial',next_action='alternative_source',observations=[{'label':'A','value':'$80','quote':'A $80'}]))
    assert evidence.retain(BrowserReport(goal_status='blocked',next_action='report',blocker='Unknown',observations=[{'label':'B','value':'$90','quote':'B $90'}]))==0
    assert evidence.last_rejections[0]['reason']=='quote_not_captured'
    output=evidence.result(error=None,elapsed=2,steps=2,done=True)
    assert output['report_status']=='partial' and output['observations'][0]['value']=='$80'


@pytest.mark.parametrize('mutation',['marker','quote','source','timestamp','index','url'])
def test_blocker_proof_requires_native_capture_provenance(mutation):
    from copy import deepcopy
    from nullion.source_observation_contract import has_captured_blocker_evidence
    output={'result_kind':'source_observations','report_status':'blocked','page_url':'https://example.com/',
        'sources':[{'source_url':'https://example.com/','captured_at':10.0}],
        'observations':[{'evidence_kind':'source_blocker','value':'休業','quote':'2027-01-04 休業','source_index':0}]}
    assert has_captured_blocker_evidence(output)
    changed=deepcopy(output)
    if mutation=='marker':changed['observations'][0].pop('evidence_kind')
    elif mutation=='quote':changed['observations'][0]['value']='Disponible'
    elif mutation=='source':changed['sources']=[]
    elif mutation=='timestamp':changed['sources'][0]['captured_at']=True
    elif mutation=='index':changed['observations'][0]['source_index']=99
    elif mutation=='url':changed['sources'][0]['source_url']='file:///private/source'
    assert not has_captured_blocker_evidence(changed)


def test_on_demand_visual_inspection_cannot_write_or_attach_an_extra_file():
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserVisualInspection, _register_visual_inspection
    with pytest.raises(ValidationError):
        BrowserVisualInspection(file_name='unrequested.png')
    actions = {}
    def action(description, param_model):
        def register(function):
            actions[function.__name__] = function
            return function
        return register
    controller = SimpleNamespace(registry=SimpleNamespace(action=action))
    _register_visual_inspection(controller, lambda **kwargs: kwargs)
    result = asyncio.run(actions['screenshot'](BrowserVisualInspection()))
    assert result['metadata'] == {'include_screenshot':True}
    assert 'attachments' not in result


@pytest.mark.parametrize('state',['completed','failed','choice','read'])
def test_explicit_interaction_continues_after_successful_native_preflight_only(monkeypatch,state):
    from nullion.plugins.browser_plugin import browser_use_adapter as adapter
    from nullion.plugins.browser_plugin import browser_use_worker as worker
    from nullion.tools import ToolResult
    monkeypatch.setattr(adapter,'browser_use_python',lambda:'/test/python')
    calls=[]
    def external(task,**kwargs):
        calls.append(task)
        assert task.prior_source_steps[-1]['output']['documents'][0]['text']=='Observed search controls'
        return {'artifact_paths':[],'report_status':'completed','goal_status':'answered','observations':[]}
    monkeypatch.setattr(worker,'run_isolated_browser_task',external)
    def read(inv):
        return ToolResult(inv.invocation_id,inv.tool_name,'failed' if state=='failed' else 'completed',{
            'result_kind':'browser_page_collection','completed_pages':0 if state=='failed' else 1,
            'documents':[{'url':'https://source.test/','text':'Observed search controls'}],
            'strategy':{'kind':'native_read_preflight','adaptive_browser_started':False},
            **({'requires_user_choice':True} if state=='choice' else {})})
    inv=ToolInvocation('preflight','browser_run_task','operator',{
        'task':'確認','url':'https://source.test/',
        'execution_kind':'read' if state=='read' else 'adaptive_interaction',
        'interaction_reason':'Use the observed form controls'})
    result=browser_task_handler(lambda:object(),BrowserPolicy(),read_preflight=read)(inv)
    assert len(calls)==(1 if state=='completed' else 0)
    if state=='completed':assert result.output['native_preflight']['adaptive_browser_started'] is True
    else:assert result.output['strategy']['adaptive_browser_started'] is False


def test_partial_report_does_not_finish_with_a_pending_required_control(tmp_path,monkeypatch):
    from nullion.plugins.browser_plugin.browser_use_adapter import _record_observations_result
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    async def read():
        return {'url':'https://example.com/','text':'Cedar 37','controls':[
            {'tag':'INPUT','value':'2031-04-09','label':'Date','visible':True,'disabled':False}]}
    evidence=PageEvidence('operator',BrowserPolicy(),text_capture=read)
    state=SimpleNamespace(url='https://example.com/',title='記録',screenshot=None,
        dom_state=SimpleNamespace(llm_representation=lambda:'Cedar 37'))
    asyncio.run(evidence.capture(state,step=1))
    report=BrowserReport(goal_status='partial',next_action='report',
        observations=[{'label':'記録','value':'37','quote':'Cedar 37','source_id':0}],
        pending_controls=[{'source_id':0,'control_index':0}])
    result=_record_observations_result(evidence,report,lambda **kw:SimpleNamespace(**kw))
    assert not getattr(result,'is_done',False) and not evidence.finished_report
    assert evidence.pending_controls and evidence.next_action=='alternative_source'


@pytest.mark.parametrize('label', ['選択状態', 'Estado seleccionado', 'الحالة المحددة'])
@pytest.mark.parametrize('checked', [False, True])
def test_captured_url_and_control_property_finish_without_page_text_repair(label, checked):
    from nullion.plugins.browser_plugin.browser_use_adapter import _record_observations_result
    from nullion.source_observation_contract import compact_source_observations
    url = 'https://example.com/results?date=2031-04-09&stops=0'
    evidence = PageEvidence('operator', BrowserPolicy())
    async def captured():
        return {'url':url, 'text':'Cedar 37', 'controls':[{'label':label, 'text':label,
            'value':'on', 'checked':checked, 'disabled':False, 'type':'checkbox'}]}
    evidence.text_capture = captured
    state = SimpleNamespace(url=url, title='Cedar', dom_state=None, screenshot=None)
    asyncio.run(evidence.capture(state))
    report = BrowserReport(goal_status='answered', next_action='report', observations=[
        {'label':'Cedar', 'semantic_role':'entity', 'value':'37', 'quote':'Cedar 37', 'source_id':0},
        {'label':'URL', 'semantic_role':'context', 'value':url, 'quote':url, 'source_id':0},
        {'label':label, 'semantic_role':'interface_control', 'control_index':0, 'source_id':0,
         'control_property':'checked'}])
    result = _record_observations_result(evidence, report, lambda **kw:SimpleNamespace(**kw))
    assert result.is_done is True and evidence.rejected == 0
    assert evidence.observations[1]['quote'] == url
    assert evidence.observations[1]['evidence_kind'] == 'captured_source_url'
    property_record = evidence.observations[2]
    assert property_record['value'] == ('true' if checked else 'false')
    assert property_record['control_state']['observed_property'] == 'checked'
    assert property_record['control_state']['checked'] is checked
    output = compact_source_observations(evidence.result(error=None, elapsed=1, steps=1, done=True))
    assert output['observations'][2]['control_state']['observed_property'] == 'checked'
    assert output['observations'][2]['value'] == property_record['value']


@pytest.mark.parametrize('mutation', ['wrong_value', 'wrong_quote', 'missing_property', 'missing_control',
    'wrong_url', 'entity_url', 'conflicting_selection', 'oversized'])
def test_native_property_evidence_rejects_mismatches_and_keeps_text_claims_strict(mutation):
    url = 'https://example.com/results'
    evidence = PageEvidence('operator', BrowserPolicy())
    async def captured():
        return {'url':url, 'text':'Cedar 37', 'controls':[{'label':'Filter', 'text':'Filter', 'checked':False,
            'value':'x'*2001 if mutation == 'oversized' else 'on'}]}
    evidence.text_capture = captured
    asyncio.run(evidence.capture(SimpleNamespace(url=url, title='Cedar', dom_state=None, screenshot=None)))
    observation = {'label':'Filter', 'semantic_role':'interface_control', 'source_id':0,
        'control_index':0, 'control_property':'checked'}
    if mutation == 'wrong_value': observation['value'] = 'true'
    if mutation == 'wrong_quote': observation['quote'] = 'Invented control'
    if mutation == 'missing_property': observation['control_property'] = 'expanded'
    if mutation == 'missing_control': observation.pop('control_index')
    if mutation == 'conflicting_selection': observation['text_selection'] = {'source_id':0, 'start_line':0, 'end_line':0}
    if mutation == 'oversized': observation['control_property'] = 'value'
    if mutation in {'wrong_url', 'entity_url'}:
        observation = {'label':'URL', 'semantic_role':'entity' if mutation == 'entity_url' else 'context',
            'source_id':0, 'value':url + ('?invented=1' if mutation == 'wrong_url' else '')}
    assert evidence.retain(BrowserReport(goal_status='answered', next_action='report', observations=[observation])) == 0
    assert evidence.last_rejections and not evidence.observations
