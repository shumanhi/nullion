from copy import deepcopy
import pytest
from nullion.agent_orchestrator import _completion_preflight_recovery_limit
from nullion.tools import ToolResult


def state():
    return {'iterations':16,'max_iterations':20,'completion_recovery_iteration_limit':16,'tool_results':[
        ToolResult('preflight','terminal_exec','failed',{'reason':'artifact_deletion_alternative_available',
            'command_executed':False,'recovery':{'kind':'try_non_destructive_creation',
                'retry_tool_name':'terminal_exec','approval_bypass':False}})]}


def test_unexecuted_preflight_gets_one_bounded_correction_window():
    value=state();assert _completion_preflight_recovery_limit(value)==20
    value['preflight_recovery_budget_used']=True
    assert _completion_preflight_recovery_limit(value) is None


@pytest.mark.parametrize('change',['executed','bypass','other_tool','no_recovery','below_limit','hard_limit'])
def test_correction_budget_does_not_relax_execution_or_iteration_guards(change):
    value=state();out=value['tool_results'][0].output
    if change=='executed':out['command_executed']=True
    if change=='bypass':out['recovery']['approval_bypass']=True
    if change=='other_tool':out['recovery']['retry_tool_name']='file_write'
    if change=='no_recovery':value.pop('completion_recovery_iteration_limit')
    if change=='below_limit':value['iterations']=15
    if change=='hard_limit':value['max_iterations']=16
    assert _completion_preflight_recovery_limit(value) is None
