from pathlib import Path
import sys
import pytest
from scripts import live_stage_chat_contract_probe as probe


def test_container_probe_never_selects_a_host_macos_interpreter(monkeypatch):
    monkeypatch.setenv('NULLION_E2E_CONTAINER','1')
    monkeypatch.setenv('NULLION_LIVE_STAGE_PYTHON','/host/mac-only/python')
    assert probe._isolated_probe_python(probe.SCENARIOS['simple'])==Path(sys.executable)


@pytest.mark.parametrize('runner',['platform_code_fence_formatting','agent_turn_limit_doctor_card',
    'structured_tool_id_boundary_guards','stage_runtime_patch_regressions','account_answer_fulfillment','reply_anchor_context_guard'])
def test_isolated_boundary_probes_execute_in_container_without_provider_calls(monkeypatch,runner):
    monkeypatch.setenv('NULLION_E2E_CONTAINER','1')
    scenario=next(s for s in probe.SCENARIOS.values() if s.runner==runner)
    result=getattr(probe,'_run_'+runner)(scenario=scenario,timeout=45,index=0)
    assert result['verification_lane']=='candidate_container'
