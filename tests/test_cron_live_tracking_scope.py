import pytest
from nullion.cron_execution_tools import CronExecutionToolRegistry
from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolInvocation

@pytest.mark.parametrize('name',['observe_temporarily','短期観察','observación_temporal'])
def test_scheduled_execution_cannot_create_another_tracker_through_an_alias(name):
    registry=ToolRegistry()
    registry.register(ToolSpec(name,'Temporary observation control',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,5,capability_tags=('live_tracking',)),
        lambda inv:pytest.fail('Scheduled report must not start another tracking service'))
    scoped=CronExecutionToolRegistry(registry)
    assert name not in {row['name'] for row in scoped.list_tool_definitions()}
    assert scoped.invoke(ToolInvocation('recursive',name,'web:scheduled',{})).status=='denied'
