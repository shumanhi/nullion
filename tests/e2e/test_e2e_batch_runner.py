"""Real pytest subprocesses verify bounded CI execution and durable receipts."""
import json
import os
from pathlib import Path
import subprocess
import sys
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker developer-tool E2E')
ROOT=Path(__file__).resolve().parents[2]


def launch(suite,evidence,history):
    env={**os.environ,'PYTHONPATH':str(ROOT),'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTEST_ADDOPTS':'',
        'NULLION_E2E_BATCH_EVIDENCE':str(evidence),'NULLION_TEST_PERFORMANCE_DIR':str(history)}
    return subprocess.run([sys.executable,str(ROOT/'scripts/run_e2e_batches.py'),'--batch-files','1','--',
        str(suite),'-c','/dev/null','--rootdir',str(suite),'--maxfail=1','--tb=short'],
        env=env,cwd=ROOT,capture_output=True,text=True,timeout=30)


def test_real_batches_keep_every_case_once_and_release_their_processes_and_homes(tmp_path):
    suite=tmp_path/'suite';suite.mkdir();evidence=tmp_path/'evidence';history=tmp_path/'history'
    (suite/'conftest.py').write_text("pytest_plugins=['scripts.pytest_test_timings']\n")
    for name in ['a','b']:
        (suite/f'test_{name}.py').write_text('''
import json,os,subprocess,sys
from pathlib import Path
import pytest
@pytest.mark.parametrize('value',['数値','dato'])
def test_owned(value):
    home=Path(os.environ['HOME']);(home/'owned').write_text(value)
    worker=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])
    target=Path(OUTPUT)/(str(os.getpid())+'-'+value+'.json')
    target.write_text(json.dumps({'pid':os.getpid(),'worker':worker.pid,'home':str(home),'value':value}))
'''.replace('OUTPUT',repr(str(tmp_path))))
    result=launch(suite,evidence,history)
    assert result.returncode==0,result.stdout+result.stderr
    records=[json.loads(p.read_text()) for p in tmp_path.glob('*-*.json')]
    assert len(records)==4 and len({r['pid'] for r in records})==2
    assert len({r['home'] for r in records})==2
    for row in records:
        assert not Path(row['home']).exists()
        stat=Path('/proc',str(row['worker']),'stat')
        assert not stat.exists() or stat.read_text().split(') ',1)[1].startswith('Z ')
    run=next(evidence.iterdir());summary=json.loads((run/'results.json').read_text())
    assert summary['collected']==4 and len(summary['batches'])==2
    ids=[node for batch in summary['batches'] for node in batch['nodeids']]
    assert len(ids)==len(set(ids))==4
    from scripts.test_performance import history as read_history
    samples,_=read_history(history)
    assert len(samples)==4 and all(r['outcome']=='passed' for r in samples)
    assert len({r['session_id'] for r in samples})==2


def test_failed_batch_retains_its_identity_and_never_runs_the_next_batch(tmp_path):
    suite=tmp_path/'suite';suite.mkdir();evidence=tmp_path/'evidence';history=tmp_path/'history'
    marker=tmp_path/'must-not-run'
    (suite/'conftest.py').write_text("pytest_plugins=['scripts.pytest_test_timings']\n")
    (suite/'test_a.py').write_text('def test_bad():assert 37==73\n')
    (suite/'test_b.py').write_text('from pathlib import Path\ndef test_later():Path('+repr(str(marker))+').touch()\n')
    result=launch(suite,evidence,history)
    assert result.returncode==1,result.stdout+result.stderr
    assert not marker.exists()
    run=next(evidence.iterdir());summary=json.loads((run/'results.json').read_text())
    assert summary['collected']==2 and len(summary['batches'])==1
    assert summary['batches'][0]['exit_code']==1
    from scripts.test_performance import history as read_history
    samples,_=read_history(history)
    assert len(samples)==1 and samples[0]['outcome']=='failed'
