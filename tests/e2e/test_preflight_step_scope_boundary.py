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
    assert result.final_text=='Observed value: 7.' and provider.review_calls==1


@pytest.mark.parametrize('prompt',['対象を調べて。','Inspecciona el recurso.'])
def test_changed_unexecuted_proposals_cannot_reset_repair_forever(tmp_path,monkeypatch,prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass
    runtime,registry,_=runtime_and_registry(tmp_path,monkeypatch,'resource_read')
    registry.register(ToolSpec('opaque_action','Inspect selected resource',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),
        lambda inv:pytest.fail('Conflicting action executed'))
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system','')
            if 'Workflow action preflight.' in system:
                return text({'decision':'revise','issues':['対象が違います。'],
                    'issue_kind':'concrete_conflict','step_effect':'observation'})
            if 'material-requirements planner' in system:return super().create(**kw)
            if 'semantic completion reviewer' in system:
                self.review_calls+=1
                return text({'disposition':'retry','unresolved_requirements':['対象が違います。'],'retry_tool_names':['opaque_action']})
            self.agent_calls+=1
            return call('opaque_action',{'target':str(self.agent_calls)},self.agent_calls)
    provider=Provider('opaque_action');runner=AgentOrchestrator(model_client=provider)
    kwargs=dict(conversation_id='web:step-scope',principal_id='web:step-scope',user_message=prompt,
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    result=runner.run_turn(**kwargs,max_iterations=25)
    assert provider.agent_calls==3 and provider.review_calls==1
    assert result.response_fulfilled is False and 'still needed correction' in result.final_text
    assert result.outcome_review_receipt.disposition=='blocked'
    assert runner.review_turn_result(result,**kwargs) is result
    assert provider.review_calls==1


@pytest.mark.asyncio
async def test_delegated_revisions_stop_before_reopening_the_same_rejected_plan(tmp_path,monkeypatch):
    import asyncio
    from nullion.context_bus import ContextBus
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig
    from nullion.task_queue import TaskRecord,TaskStatus,TaskPriority
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass
    runtime,registry,_=runtime_and_registry(tmp_path,monkeypatch,'resource_read')
    registry.register(ToolSpec('opaque_action','Inspect selected resource',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2,
        input_schema={'type':'object','properties':{'target':{'type':'string'}},'required':['target']}),
        lambda inv:pytest.fail('Rejected delegated action executed'))
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system','')
            if 'Workflow action preflight.' in system:
                return text({'decision':'revise','issues':['Corrige el destino.'],
                    'issue_kind':'concrete_conflict','step_effect':'observation'})
            if 'material-requirements planner' in system or 'semantic completion reviewer' in system:
                pytest.fail('Exhausted delegated proposal restarted review')
            self.agent_calls+=1
            return call('opaque_action',{'target':str(self.agent_calls)},self.agent_calls)
    provider=Provider('opaque_action')
    task=TaskRecord(task_id='revisions',group_id='group',conversation_id='web:delegated',principal_id='web:delegated',
        title='Selected resource',description='Inspecciona el recurso.',status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,allowed_tools=['opaque_action'],dependencies=[])
    result=await DeepAgentMiniAgentRunner().run(MiniAgentConfig('inspector',task,timeout_s=30),anthropic_client=provider,
        tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,context_bus=ContextBus(),progress_queue=asyncio.Queue())
    assert provider.agent_calls==3 and result.status=='failure',result
    assert 'still needed correction' in result.error and 'timed out' not in result.error
