"""One paid stage API workflow per pytest case; never expand an empty selection."""
import os
from pathlib import Path
import pytest
from scripts import live_stage_chat_contract_probe as probe

SELECTED=tuple(dict.fromkeys(item.strip() for item in os.environ.get('NULLION_LIVE_SCENARIOS','').split(',') if item.strip()))
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Explicit stage provider quota only')

@pytest.mark.parametrize('scenario_name',SELECTED)
def test_live_stage_api_scenario(scenario_name):
    assert scenario_name in probe.SCENARIOS
    assert os.environ.get('NULLION_E2E_CONTAINER')=='1'
    assert probe.main(['--base-url',os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753'),
        '--runtime-db',os.environ.get('NULLION_LIVE_RUNTIME_DB','/stage/runtime.db'),
        '--repeat',os.environ.get('NULLION_LIVE_REPEAT','1'),'--fail-fast','--scenario',scenario_name])==0
