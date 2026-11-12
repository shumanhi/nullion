"""A non-executed creation preflight is repairable without bypassing consent."""
import pytest
from nullion.tools import ToolResult
from nullion.tool_repair_contract import tool_repair_kind

@pytest.mark.parametrize('changes',[
    {}, {'command_executed':True}, {'reason':'other'},
    {'recovery':{'kind':'try_non_destructive_creation','retry_tool_name':'terminal_exec','approval_bypass':True}},
    {'recovery':{'kind':'try_non_destructive_creation','retry_tool_name':'file_write','approval_bypass':False}},
    {'recovery':{'kind':'try_non_destructive_creation','approval_bypass':False}},
])
def test_preflight_repair_requires_exact_nonexecuted_consent_preserving_contract(changes):
    output={'reason':'artifact_deletion_alternative_available','command_executed':False,
        'recovery':{'kind':'try_non_destructive_creation','retry_tool_name':'terminal_exec','approval_bypass':False}}
    output.update(changes)
    result=ToolResult('repair','terminal_exec','failed',output,'No command ran.')
    assert tool_repair_kind(result)==('try_non_destructive_creation' if not changes else None)
