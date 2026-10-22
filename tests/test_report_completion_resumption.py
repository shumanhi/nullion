from datetime import UTC, datetime

from nullion.agent_orchestrator import AgentOrchestrator, TurnResult, _tool_invocation_signature
from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult


def test_resumed_run_reuses_completed_action_but_requires_approval_for_new_arguments(tmp_path):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=ToolRegistry();writes=[]
    # The existing action receipt is execution state. It grants no permission
    # for a different account action in the continuation.
    def create(invocation):
        writes.append(invocation.arguments)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'id':invocation.arguments['id']})
    registry.register(ToolSpec('create_record','Create record',ToolRiskLevel.HIGH,
        ToolSideEffectClass.ACCOUNT_WRITE,True,2),create)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id=kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL,kind=kind,mode=PolicyMode.ALLOW,selector='*',
            created_by='fixture',created_at=datetime.now(UTC)))
    receipt=ToolResult('completed-a','create_record','completed',{'id':'A','action_receipt':{'type':'action_receipt','object_id':'A'}})
    previous=TurnResult('previous','Unfinished',tool_results=[receipt],completed_action_results={
        _tool_invocation_signature(tool_name='create_record',tool_input={'id':'A'}):receipt})
    class Model:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.calls),
                'name':'create_record','input':{'id':'A' if self.calls==1 else 'B'}}]}
    model=Model()
    result=AgentOrchestrator(model_client=model).resume_turn(conversation_id='web:resume',principal_id='web:resume',
        user_message='Finish both records',messages_snapshot=[{'role':'user','content':'Finish both records'}],
        previous_result=previous,tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
        tool_flow_context={'scheduled_task_run':True},max_iterations=4)
    assert writes==[] and model.calls==2
    assert result.suspended_for_approval and result.approval_id
    assert runtime.store.get_approval_request(result.approval_id).status.value=='pending'


def test_cancellation_stops_resumed_run_before_model_or_tool_call(tmp_path):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    class Model:
        def create(self,**kwargs):raise AssertionError('Cancelled continuation reached the model')
    result=AgentOrchestrator(model_client=Model()).resume_turn(conversation_id='web:stop',principal_id='web:stop',
        user_message='停止',messages_snapshot=[{'role':'user','content':'停止'}],tool_registry=ToolRegistry(),
        policy_store=runtime.store,approval_store=runtime.store,cancellation_checker=lambda:True,
        tool_flow_context={'scheduled_task_run':True})
    assert 'Stopped' in result.final_text and not result.tool_results


def test_unfinished_manual_run_waiting_for_approval_is_not_shown_as_failed():
    from types import SimpleNamespace
    from nullion.cron_delivery import manual_cron_terminal_status_text
    text=manual_cron_terminal_status_text(SimpleNamespace(name='承認'),result={
        'cron_delivery_status':'paused_for_approval','suspended_for_approval':True,'response_fulfilled':False,
        'approval_id':'approval-123'})
    assert 'Waiting:' in text and 'Failed:' not in text
