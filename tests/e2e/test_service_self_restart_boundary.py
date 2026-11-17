"""Real service-control subprocess with a deterministic external launchd boundary."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')

@pytest.mark.parametrize('name,label', [('telegram', 'ai.nullion.telegram'), ('web', 'com.nullion.web')])
def test_self_restart_keeps_job_registered_across_caller_termination(tmp_path, name, label):
    agents = tmp_path/'Library/LaunchAgents'; agents.mkdir(parents=True)
    (agents/(label+'.plist')).write_text('<plist/>')
    bin_dir = tmp_path/'bin'; bin_dir.mkdir()
    state = tmp_path/'service.json'; state.write_text(json.dumps({'registered': True, 'generation': 1}))
    launchctl = bin_dir/'launchctl'
    launchctl.write_text('#!'+sys.executable+'\n'+'''
import json,os,signal,sys
from pathlib import Path
p=Path(os.environ['SERVICE_STATE']); state=json.loads(p.read_text())
if sys.argv[1]=='bootout':
    state['registered']=False; p.write_text(json.dumps(state))
    os.kill(os.getppid(),signal.SIGTERM)
elif sys.argv[1:3]==['kickstart','-k']:
    assert state['registered']
    state['generation']+=1; p.write_text(json.dumps(state))
    os.kill(os.getppid(),signal.SIGTERM)
else: raise AssertionError(sys.argv)
''')
    launchctl.chmod(0o755)
    env = {**os.environ, 'HOME': str(tmp_path), 'PATH': str(bin_dir)+':'+os.environ['PATH'],
           'XPC_SERVICE_NAME': label, 'SERVICE_STATE': str(state)}
    process = subprocess.run([sys.executable, '-c',
        'from nullion.service_control import restart_managed_service; restart_managed_service('+repr(name)+',manager="launchd")'],
        env=env, timeout=20, capture_output=True, text=True)
    saved = json.loads(state.read_text())
    assert process.returncode < 0, process.stderr
    assert saved == {'registered': True, 'generation': 2}, saved
