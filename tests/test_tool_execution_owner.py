"""Owner metadata remains tied to the actual invocation across timeout threads."""
import pytest
from nullion import tools


@pytest.mark.parametrize('effect',[tools.ToolSideEffectClass.READ,tools.ToolSideEffectClass.WRITE])
def test_owner_metadata_crosses_handler_thread_and_does_not_leak(effect):
    invocation=tools.ToolInvocation('invocation-1','inspect_owner','principal',{},capsule_id='scope-1',
        flow_context={'conversation_id':'thread-1','credentials':'private','user_message':'not ownership metadata'})
    spec=tools.ToolSpec('inspect_owner','Inspect current ownership',tools.ToolRiskLevel.LOW,effect,False,2)
    observed=[]
    def handler(call):
        observed.append(tools.tool_execution_owner())
        return tools.ToolResult(call.invocation_id,call.tool_name,'completed',{})
    token=tools._TOOL_EXECUTION_AUTHORITY.set((object(),invocation))
    try:
        result=tools._invoke_tool_handler_with_timeout(spec,handler,invocation)
        assert result.status=='completed'
    finally:tools._TOOL_EXECUTION_AUTHORITY.reset(token)
    assert observed==[{'conversation_id':'thread-1','invocation_id':'invocation-1','scope_id':'scope-1'}]
    assert tools.tool_execution_owner()=={}
    tools._invoke_tool_handler_with_timeout(spec,handler,invocation)
    assert observed[-1]=={}
