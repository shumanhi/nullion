import json
import os
import pytest
from tests.e2e.test_universal_outcome_review_boundary import ReasoningProvider, runtime_and_registry, text
from tests.e2e.test_plan_revision_recovery_boundary import call

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('approval',[False,True])
@pytest.mark.parametrize('native_authority',[False,True,'availability'])
def test_partial_observations_can_make_progress_without_bypassing_approval_or_limiting_work(tmp_path,monkeypatch,approval,native_authority):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    runtime,registry,_=runtime_and_registry(tmp_path,monkeypatch,'resource_read');executed=[]
    def inspect(inv):
        executed.append(inv.arguments['sample'])
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':7,'sample':inv.arguments['sample']})
    registry.register(ToolSpec('opaque_action','Inspect selected resource',ToolRiskLevel.HIGH if approval else ToolRiskLevel.LOW,
        ToolSideEffectClass.DANGEROUS_EXEC,approval,2, action_review_context=(
            lambda inv, retained: {'native_contract_validation': {'validated': True,
                'advisory_concern_scopes': ['native_contract']}}) if native_authority is True else None),inspect)
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system','')
            if 'Workflow action preflight.' in system:
                if native_authority:
                    return text({'decision':'revise','issues':['Mechanical suggestion only.'],
                        'concern_scopes':['tool_registration' if native_authority=='availability' else 'native_contract']})
                return text({'decision':'proceed','issues':['Later observations are still needed.'],
                    'issue_kind':'outcome_coverage_gap','step_effect':'observation'})
            if 'material-requirements planner' in system or 'semantic completion reviewer' in system:
                return super().create(**kw)
            self.agent_calls+=1
            if len(executed)<5:return call('opaque_action',{'sample':len(executed)},self.agent_calls)
            return text('Observed value: 7.')
    provider=Provider('opaque_action');runner=AgentOrchestrator(model_client=provider)
    kwargs=dict(conversation_id='web:observations',principal_id='web:observations',user_message='Inspecciona el recurso.',
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,max_iterations=12)
    result=runner.run_turn(**kwargs)
    if approval:
        assert result.suspended_for_approval and not executed
        from nullion.approvals import approve,create_permission_grant
        for _ in range(5):
            if not result.suspended_for_approval:break
            request=runtime.store.get_approval_request(result.approval_id)
            runtime.store.add_approval_request(approve(request,decided_by='fixture'))
            runtime.store.add_permission_grant(create_permission_grant(approval_id=result.approval_id,
                principal_id='web:observations',permission='tool:opaque_action',granted_by='fixture'))
            result=runner.resume_turn(**{k:v for k,v in kwargs.items() if k!='conversation_history'},
                messages_snapshot=runtime.store.get_suspended_turn(result.approval_id).messages_snapshot,previous_result=result)
    assert result.response_fulfilled is True and executed==list(range(5)),result
    assert result.final_text=='Observed value: 7.' and provider.review_calls==0
