"""Real pytest processes prove durable measurements; no live model calls."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import test_performance as performance

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker developer-tool E2E')


def run_suite(suite,history,*,revision='r1',delay='.01',collect=False):
    suite.mkdir(exist_ok=True)
    (suite/'conftest.py').write_text("pytest_plugins=['scripts.pytest_test_timings']\n")
    env={**os.environ,'PYTHONPATH':str(performance.ROOT),'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1',
         'NULLION_TEST_PERFORMANCE_DIR':str(history),'NULLION_TEST_REVISION':revision,
         'NULLION_TEST_RUN_ID':'same-run-identity','NULLION_TEST_LANE':'local',
         'PYTEST_ADDOPTS':'','DELAY':delay}
    env.pop('NULLION_TEST_PERFORMANCE_DISABLE',None)
    args=[sys.executable,'-m','pytest','-q','-c','/dev/null','--rootdir',str(suite),str(suite)]
    if collect:args.append('--collect-only')
    return subprocess.run(args,env=env,capture_output=True,text=True,timeout=20)


def test_real_sessions_keep_every_phase_failure_and_rerun_without_overwriting(tmp_path):
    suite=tmp_path/'suite';suite.mkdir();history=tmp_path/'history'
    (suite/'test_cases.py').write_text('''
import os,time,pytest
@pytest.mark.parametrize('marker',['dato','数値'])
def test_measured(marker,record_property):
    delay=float(os.environ['DELAY']);time.sleep(delay)
    record_property('visible_seconds',delay)
    record_property('unrelated_payload','do-not-store-this-payload')
def test_failure():raise AssertionError('do-not-store-failure-text')
@pytest.mark.skip(reason='owned skip fixture')
def test_skipped():pass
@pytest.fixture
def bad_setup():raise ValueError('setup failure')
def test_setup_failure(bad_setup):pass
@pytest.fixture
def bad_teardown():
    yield
    raise ValueError('teardown failure')
def test_teardown_failure(bad_teardown):pass
@pytest.mark.xfail(reason='')
def test_expected_failure():raise ValueError('expected')
@pytest.mark.xfail(reason='')
def test_unexpected_success():pass
''')
    before=run_suite(suite,history,revision='before',delay='.15')
    after=run_suite(suite,history,revision='after',delay='.01')
    assert before.returncode==after.returncode==1,before.stdout+after.stdout
    assert len(list(history.glob('*.jsonl')))==2
    rows,malformed=performance.history(history)
    assert len(rows)==16 and malformed==0
    measured=[r for r in rows if '::test_measured[' in r['nodeid']]
    assert len(measured)==4 and {r['revision'] for r in measured}=={'before','after'}
    assert all(set(r['phase_seconds'])=={'setup','call','teardown'} for r in measured)
    assert all(r['total_seconds']>=r['call_seconds']>=r['metrics']['visible_seconds'] for r in measured)
    assert all(r['kind']=='unit' and r['lane']=='local' for r in rows)
    assert all(r['outcome']=='failed' and r['call_seconds'] is None for r in rows if r['nodeid'].endswith('::test_setup_failure'))
    assert all(r['outcome']=='failed' for r in rows if r['nodeid'].endswith('::test_teardown_failure'))
    assert all(r['outcome']=='xfailed' for r in rows if r['nodeid'].endswith('::test_expected_failure'))
    assert all(r['outcome']=='xpassed' for r in rows if r['nodeid'].endswith('::test_unexpected_success'))
    results=[r for r in performance.compare(rows,window=1) if '::test_measured[' in r['nodeid']]
    assert len(results)==2 and all(r['change_percent']<0 for r in results)
    text=''.join(p.read_text() for p in history.glob('*.jsonl'))
    assert 'do-not-store-this-payload' not in text and 'do-not-store-failure-text' not in text


def test_collect_only_never_creates_timing_samples(tmp_path):
    suite=tmp_path/'suite';suite.mkdir();history=tmp_path/'history'
    (suite/'test_case.py').write_text('def test_never_runs():raise AssertionError("collection ran a test")\n')
    result=run_suite(suite,history,collect=True)
    assert result.returncode==0,result.stdout+result.stderr
    assert not history.exists()


def test_interrupt_is_saved_as_incomplete_not_a_success(tmp_path):
    suite=tmp_path/'suite';suite.mkdir();history=tmp_path/'history'
    (suite/'test_case.py').write_text('def test_first():pass\ndef test_interrupted():raise KeyboardInterrupt()\n')
    result=run_suite(suite,history)
    assert result.returncode==2,result.stdout+result.stderr
    rows,_=performance.history(history)
    assert [r['outcome'] for r in rows]==['passed','incomplete']
    assert rows[-1]['call_seconds'] is None and rows[-1]['elapsed_seconds']>=0


def test_unwritable_timing_destination_cannot_change_test_success(tmp_path):
    suite=tmp_path/'suite';suite.mkdir();blocked=tmp_path/'not-a-directory';blocked.write_text('owned')
    (suite/'test_case.py').write_text('def test_passes():pass\n')
    result=run_suite(suite,blocked)
    assert result.returncode==0,result.stdout+result.stderr
    assert 'Timing history unavailable:' in result.stdout


@pytest.mark.skipif(os.geteuid()!=0,reason='Root container ownership boundary')
def test_container_export_keeps_private_evidence_readable_by_host_owner():
    import json,shlex,tempfile
    # Linux CI uploads as the host user after the root container has exited.
    script=(performance.ROOT/'scripts/run-container-tests.sh').read_text()
    function='export_results() {'+script.split('export_results() {',1)[1].split('\n}\n',1)[0]+'\n}'
    with tempfile.TemporaryDirectory(prefix='nullion-export-') as folder:
        root=Path(folder);root.chmod(0o755)
        output=root/'output';output.mkdir()
        source=root/'.e2e-artifacts'/'private';source.mkdir(parents=True,mode=0o700)
        evidence=source/'receipt.json';evidence.write_text(json.dumps({'completed':True}));evidence.chmod(0o600)
        history=output/'test-performance';history.mkdir(mode=0o700)
        performance.append(history/'case.jsonl',{'record_type':'case','outcome':'passed'})
        outside=root/'outside';outside.write_text('untouched');outside.chmod(0o600)
        (output/'link').symlink_to(outside)
        body=function.replace('/outputs',shlex.quote(str(output)))+'\nexport_results\n'
        result=subprocess.run(['bash','-euc',body],cwd=root,env={**os.environ,
            'NULLION_E2E_OUTPUT_UID':'65534','NULLION_E2E_OUTPUT_GID':'65534'},capture_output=True,text=True)
        assert result.returncode==0,result.stderr
        copied=output/'private'/'receipt.json'
        assert copied.stat().st_uid==65534 and copied.stat().st_mode&0o777==0o600
        assert history.stat().st_mode&0o777==0o700
        assert outside.stat().st_uid==0
        def host_identity():os.setgid(65534);os.setuid(65534)
        reader=subprocess.run([sys.executable,'-c',
            'import pathlib;print(pathlib.Path("private/receipt.json").read_text());print(pathlib.Path("test-performance/case.jsonl").read_text())'],
            cwd=output,preexec_fn=host_identity,capture_output=True,text=True)
        assert reader.returncode==0,reader.stderr
        assert '"completed": true' in reader.stdout and '"outcome": "passed"' in reader.stdout


def test_sigkill_keeps_completed_timings_and_the_active_native_test_identity(tmp_path):
    import json,signal,time
    suite=tmp_path/'suite';suite.mkdir();history=tmp_path/'history';ready=tmp_path/'ready'
    (suite/'conftest.py').write_text("pytest_plugins=['scripts.pytest_test_timings']\n")
    (suite/'test_case.py').write_text('from pathlib import Path\nimport time\ndef test_a():pass\n'
        +'def test_b():\n    Path('+repr(str(ready))+').touch()\n    time.sleep(60)\n')
    env={**os.environ,'PYTHONPATH':str(performance.ROOT),'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1',
        'PYTEST_ADDOPTS':'','NULLION_TEST_PERFORMANCE_DIR':str(history)}
    env.pop('NULLION_TEST_PERFORMANCE_DISABLE',None)
    process=subprocess.Popen([sys.executable,'-m','pytest','-q','-c','/dev/null','--rootdir',str(suite),str(suite)],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        deadline=time.monotonic()+10
        while not ready.exists() and time.monotonic()<deadline:time.sleep(.02)
        assert ready.exists()
        process.send_signal(signal.SIGKILL);process.wait(timeout=5)
    finally:
        if process.poll() is None:process.kill();process.wait()
    rows,_=performance.history(history)
    assert len(rows)==1 and rows[0]['nodeid'].endswith('::test_a') and rows[0]['outcome']=='passed'
    raw=[json.loads(line) for path in history.glob('*.jsonl') for line in path.read_text().splitlines()]
    assert any(r.get('record_type')=='case_started' and r['nodeid'].endswith('::test_b') for r in raw)
    assert not any(r.get('record_type')=='end' for r in raw)
