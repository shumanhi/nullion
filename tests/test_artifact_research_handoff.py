import pytest


@pytest.mark.parametrize('extension', ['.png','.pdf','.html','.xlsx'])
def test_generated_format_is_not_removed_by_incidental_source_capture(extension):
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolInvocation, ToolSideEffectClass, ToolRiskLevel
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence
    registry = ToolRegistry()
    for name, effect, kwargs in [('source_read',ToolSideEffectClass.READ,{'source_artifact_extensions':(extension,)}),
        ('render_image',ToolSideEffectClass.WRITE,{'produced_artifact_extensions':(extension,)})]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,10,**kwargs),
            lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{}))
    scoped = ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(current_user_message='図を作成'))
    result, widened = scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','member',{
        'capabilities':['web'],'tool_names':['source_read'],'required_tool_names':['source_read'],
        'artifact_extensions':[extension],'source_artifact_delivery_requested':False}))
    assert result.status == 'completed'
    assert widened.turn_tool_scope_decision.requested_artifact_extensions == (extension,)
    assert widened.can_invoke_tool('render_image')
    assert 'render_image' not in widened.turn_tool_scope_decision.required_tool_names


@pytest.mark.parametrize('with_read', [False, True])
def test_native_timeout_cause_survives_shared_missing_attachment_boundary(with_read):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.model_clients import ModelClientTimeoutError
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery, DeliveryContract
    from tests.workflow_provider_protocol import workflow_provider_reply
    registry = ToolRegistry()
    registry.register(ToolSpec('source_read','read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5),
        lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{'text':'Confirmed source value 17'}))
    class Model:
        calls = 0
        timeouts = []
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            self.calls += 1; self.timeouts.append(kwargs['timeout'])
            if with_read and self.calls == 1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':'source_read','input':{}}]}
            raise ModelClientTimeoutError('external provider timeout')
    model = Model()
    result = AgentOrchestrator(model_client=model).run_turn(conversation_id='telegram:42',principal_id='member',
        user_message='図を作成',conversation_history=[],tool_registry=registry,policy_store=None,approval_store=None,
        tool_flow_context={'requires_artifact_delivery':True,'required_artifact_extensions':['.png']})
    assert result.model_timed_out and all(timeout == 120 for timeout in model.timeouts)
    delivery = prepare_reply_for_platform_delivery(result.final_text,tool_results=result.tool_results,
        delivery_contract=DeliveryContract.attachment_required(source='request',required_attachment_extensions=('.png',)))
    assert 'timed out' in delivery.text.lower() and not delivery.attachments
    assert "couldn't attach" not in delivery.text
