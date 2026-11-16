"""Real-provider status follow-ups preserve the selected request's action limits."""
import os
from dataclasses import replace
from pathlib import Path
import pytest
from scripts import live_stage_chat_contract_probe as probe

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in real stage provider')

@pytest.mark.parametrize('prompt',['もう一度、今の状態を確認してください','Haz una comprobación nueva','Refais une vérification à jour'])
def test_live_status_followup_after_enable_does_not_start_the_job(monkeypatch,prompt):
    name='scheduler_fresh_followup_after_bulk_enable'
    monkeypatch.setitem(probe.SCENARIOS,name,replace(probe.SCENARIOS[name],followups=(prompt,)))
    assert probe.main(['--base-url',os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753'),
        '--runtime-db',os.environ.get('NULLION_LIVE_RUNTIME_DB','/stage/runtime.db'),
        '--repeat','1','--fail-fast','--scenario',name])==0
