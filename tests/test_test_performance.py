import json
from pathlib import Path

from scripts import test_performance as performance


def sample(identifier,seconds,*,outcome='passed',lane='stage',revision='r1'):
    return dict(record_type='case',measurement_id=identifier,nodeid='test_case.py::test_read',
                recorded_at=identifier,call_seconds=seconds,outcome=outcome,lane=lane,
                kind='live_workflow',revision=revision,measurement_source='pytest_report')


def test_comparison_preserves_failures_and_separates_environment():
    rows=[sample('1',10),sample('2',12),sample('3',4,revision='r2'),sample('4',6,revision='r2'),
          sample('5',30,outcome='failed'),sample('6',.01,lane='local')]
    stage=next(r for r in performance.compare(rows,window=2) if r['environment']['lane']=='stage')
    assert stage['baseline_median_seconds']==11 and stage['recent_median_seconds']==5
    assert stage['change_percent']<0 and stage['failed_samples']==1
    assert stage['latest_outcome']=='failed' and stage['latest_seconds']==30
    assert stage['observed_p95_seconds']==12 and stage['successful_samples']==4


def test_one_measurement_does_not_invent_a_baseline():
    row=performance.compare([sample('1',2)])[0]
    assert row['change_percent'] is None and row['comparison_samples_per_window']==0


def test_legacy_import_is_idempotent_and_copied_rows_do_not_multiply_samples(tmp_path):
    source=tmp_path/'case-results.jsonl';target=tmp_path/'history'
    row=dict(nodeid='tests/e2e/test_live_stage_code_mode.py::test_live_stage_read_only_code_mode',
             phase='call',outcome='passed',duration_seconds=12.5,recorded_at=1000)
    source.write_text(json.dumps(row)+'\n')
    first=performance.import_live_log(source,target)
    assert first['imported']==1
    assert performance.import_live_log(source,target)['already_imported']
    source.write_text(json.dumps(row)+'\n'+json.dumps({**row,'recorded_at':1001,'duration_seconds':9})+'\n')
    performance.import_live_log(source,target)
    samples,malformed=performance.history(target)
    assert len(samples)==2 and not malformed
    assert all(r['revision'] is None and r['total_seconds'] is None for r in samples)
    assert all(r['measurement_source']=='legacy_call' for r in samples)


def test_partial_history_line_does_not_hide_other_measured_cases(tmp_path):
    path=tmp_path/'run.jsonl'
    performance.append(path,{'record_type':'run','revision':'r1'})
    performance.append(path,{**sample('1',3),'nodeid':'test_case.py::test_測定'})
    with path.open('ab') as f:f.write(b'{"incomplete":"\xe6')
    rows,malformed=performance.history(tmp_path)
    assert len(rows)==1 and malformed==1 and rows[0]['nodeid'].endswith('測定')


def test_catalogue_classification_uses_exact_structured_test_ids():
    api='tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[weather]'
    assert performance.classification(api)==dict(category='research',kind='live_workflow')
    assert performance.classification('tests/e2e/test_live_stage_transport.py::test_stage_transport_closes_when_connection_deadline_expires')['kind']=='local_harness'
    old='tests/e2e/test_live_stage_web_producer_completion.py::test_live_web_primary_keeps_native_account_source_consent'
    assert performance.classification(old)==dict(category='consent',kind='live_workflow')


def test_json_report_can_select_a_single_test_without_running_it(tmp_path,capsys):
    performance.append(tmp_path/'run.jsonl',{'record_type':'run','revision':'r1'})
    performance.append(tmp_path/'run.jsonl',sample('1',3))
    performance.main(['--history',str(tmp_path),'--test','test_case.py::test_read','--json'])
    report=json.loads(capsys.readouterr().out)
    assert report['samples']==1 and len(report['comparisons'])==1


def test_rerun_reports_keep_attempt_identity_and_never_become_passed_samples(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from scripts.pytest_test_timings import TimingRecorder
    monkeypatch.setenv('NULLION_TEST_PERFORMANCE_DIR',str(tmp_path/'history'))
    recorder=TimingRecorder(SimpleNamespace(rootpath=tmp_path))
    for call_outcome in ('rerun','passed'):
        for phase in ('setup','call','teardown'):
            recorder.pytest_runtest_logreport(SimpleNamespace(nodeid='test_case.py::test_retry',
                when=phase,duration=.01,start=1000,stop=1000.01,user_properties=[],
                outcome=call_outcome if phase=='call' else 'passed'))
    rows,_=performance.history(tmp_path/'history')
    assert [r['outcome'] for r in rows]==['rerun','passed']
    assert [r['attempt'] for r in rows]==[1,2]
    assert len({r['measurement_id'] for r in rows})==2


def test_parallel_controller_does_not_duplicate_worker_measurements(monkeypatch):
    from types import SimpleNamespace
    from scripts import pytest_test_timings as plugin
    calls=[]
    monkeypatch.setattr(plugin,'TimingRecorder',lambda config:'recorder')
    manager=SimpleNamespace(register=lambda *args:calls.append(args))
    controller=SimpleNamespace(option=SimpleNamespace(numprocesses=2,collectonly=False),pluginmanager=manager)
    plugin.pytest_configure(controller)
    assert calls==[]
    worker=SimpleNamespace(option=controller.option,pluginmanager=manager,workerinput={})
    plugin.pytest_configure(worker)
    assert calls==[('recorder','nullion-test-timings')]
