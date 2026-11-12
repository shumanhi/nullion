"""Real runtime checkpoint and scheduler sidecar under a shared outer lock."""
import json
import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def test_nested_checkpoint_and_scheduler_write_finish_and_survive_reload(tmp_path):
    script = '''
import json,sys
from pathlib import Path
from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_persistence import runtime_store_file_lock, load_runtime_store
from nullion.cron_scheduler_state import write_limit,read_limit
from nullion.events import make_event
path=Path(sys.argv[1])
runtime=bootstrap_persistent_runtime(path)
with runtime_store_file_lock(path):
    with runtime_store_file_lock(path):
        runtime.store.add_event(make_event('checkpoint.regression','fixture',{'value':7}))
        runtime.checkpoint(force=True)
    write_limit(path,3)
restored=load_runtime_store(path)
events=[e for e in restored.list_events() if e.event_type=='checkpoint.regression']
assert len(events)==1 and events[0].payload=={'value':7}
assert read_limit(path)==3
print(json.dumps({'events':len(events),'limit':read_limit(path)}))
'''
    env = {**os.environ, 'NULLION_HOME': str(tmp_path / 'home'),
        'NULLION_DATA_DIR': str(tmp_path / 'data'), 'NULLION_MEMORY_ENABLED': '0'}
    result = subprocess.run([sys.executable, '-c', script, str(tmp_path / 'runtime.db')],
        env=env, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == {'events': 1, 'limit': 3}
