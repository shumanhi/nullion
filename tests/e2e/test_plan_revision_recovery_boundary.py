"""Real LangGraph executor, SQLite and review; external provider is scripted."""
import json
import os

import pytest

from tests.e2e.test_universal_outcome_review_boundary import ReasoningProvider, runtime_and_registry, text

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def call(name, arguments, ident):
    return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(ident),'name':name,'input':arguments}]}


@pytest.mark.parametrize('surface',['web','telegram'])
def test_numeric_height_followup_executes_corrected_browser_plan_and_delivers_heights(tmp_path,monkeypatch,surface):
    from tests.e2e.test_selected_request_objective_boundary import test_selected_goal_survives_restart_formatted_quote_and_wrong_roster as replay
    replay(tmp_path,monkeypatch,surface,True,'captured','出演者とプロの身長を調べて。','身長',
        ['Aoi: 173 cm','Ren: 183 cm'],revision_mode=True)


@pytest.mark.parametrize('act_after_nudge,terminal_verdict',[(True,'complete'),(False,'retry'),
    (False,'needs_user_input'),(False,'invalid')])
def test_repeated_scope_selection_gets_one_chance_to_do_work_and_cannot_restart_review(tmp_path,monkeypatch,act_after_nudge,terminal_verdict):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision
    runtime,delegate,reads=runtime_and_registry(tmp_path,monkeypatch,'resource_read')
    registry=ScopedTurnToolRegistry(delegate,evidence=TurnToolEvidence(),tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=('resource_read',),valid=True,confidence=1))
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system','')
            if 'material-requirements planner' in system:return super().create(**kw)
            if 'semantic completion reviewer' in system:
                if act_after_nudge:return super().create(**kw)
                self.review_calls+=1
                if terminal_verdict=='invalid':return text({})
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                refs=[r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']=='resource_read' and r['status']=='completed']
                return text({'disposition':terminal_verdict,'unresolved_requirements':['新しい値が必要です。'],
                    'retry_tool_names':['resource_read'] if terminal_verdict=='retry' else [],
                    'supported_findings':[{'text':'Observed value: 7.','evidence_invocation_ids':refs,'kind':'finding'}],
                    **({'clarification':{'question':'次はどちらを確認しますか？','options':['A','B']}}
                        if terminal_verdict=='needs_user_input' else {})})
            self.agent_calls+=1
            if self.agent_calls==1 or (self.agent_calls==4 and act_after_nudge):
                return call('resource_read',{'revision':self.agent_calls},self.agent_calls)
            if self.agent_calls==5 and act_after_nudge:return text('Observed value: 7.')
            return call('request_tool_scope',{'tool_names':['resource_read']},self.agent_calls)
    provider=Provider('resource_read');runner=AgentOrchestrator(model_client=provider)
    kwargs=dict(conversation_id='web:scope-recovery',principal_id='web:scope-recovery',user_message='対象の値を確認して。',
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    result=runner.run_turn(**kwargs,max_iterations=10)
    assert len(reads)==(2 if act_after_nudge else 1)
    assert provider.agent_calls==(5 if act_after_nudge else 4)
    expected_reviews=2 if terminal_verdict=='invalid' else 1  # Existing structured-protocol repair.
    assert provider.review_calls==expected_reviews
    assert result.response_fulfilled is act_after_nudge, result
    assert result.outcome_review_receipt.disposition==('complete' if act_after_nudge else 'blocked')
    if terminal_verdict!='invalid':assert 'Observed value: 7.' in result.final_text
    if terminal_verdict=='needs_user_input':assert '1. A\n2. B' not in result.final_text
    if not act_after_nudge:assert 'Tool selection' in result.final_text
    assert runner.review_turn_result(result,**kwargs) is result
    assert provider.review_calls==expected_reviews


@pytest.mark.parametrize('tool', ['browser_run_task','resource_write','opaque_action'])
@pytest.mark.parametrize('prompt,issue', [('対象を修正して実行してください。','指定された対象に修正してください。'),
    ('Corrige y ejecuta la acción.','Utiliza el destino seleccionado.')])
@pytest.mark.parametrize('approval',[False,True])
def test_preflight_revision_reuses_same_tool_and_resolves_rejection(tmp_path, monkeypatch, tool, prompt, issue, approval):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.workflow_recovery import recovered_failure_ids
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    writes = []
    def execute(inv):
        writes.append(inv.arguments['target'])
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'target':'selected','value':7})
    registry.register(ToolSpec(tool,'Perform selected action',ToolRiskLevel.HIGH if approval else ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,approval,2),execute)
    class Provider(ReasoningProvider):
        def __init__(self): super().__init__(tool); self.proposals=[]
        def create(self, **kw):
            system = kw.get('system','')
            if 'Workflow action preflight.' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                target = packet['proposed_action']['arguments']['target']; self.proposals.append(target)
                return text({'decision':'revise','issues':[issue]} if target=='other' else {'decision':'proceed','issues':[]})
            if 'material-requirements planner' in system or 'semantic completion reviewer' in system:
                return super().create(**kw)
            self.agent_calls += 1
            if self.agent_calls < 3:
                assert tool in {row['name'] for row in kw['tools']}
                return call(tool, {'target':'other' if self.agent_calls==1 else 'selected'}, self.agent_calls)
            return text('Observed value: 7.')
    provider = Provider(); runner = AgentOrchestrator(model_client=provider)
    kwargs = dict(conversation_id='web:revision',principal_id='web:revision',user_message=prompt,
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
        max_iterations=8)
    result = runner.run_turn(**kwargs)
    if approval:
        from nullion.approvals import approve,create_permission_grant
        assert result.suspended_for_approval and result.approval_id and not writes
        request=runtime.store.get_approval_request(result.approval_id)
        runtime.store.add_approval_request(approve(request,decided_by='fixture'))
        runtime.store.add_permission_grant(create_permission_grant(approval_id=result.approval_id,
            principal_id='web:revision',permission='tool:'+tool,granted_by='fixture'))
        result=runner.resume_turn(**{k:v for k,v in kwargs.items() if k!='conversation_history'},
            messages_snapshot=runtime.store.get_suspended_turn(result.approval_id).messages_snapshot,previous_result=result)
    assert result.response_fulfilled is True, result
    assert result.final_text == 'Observed value: 7.'
    assert writes == ['selected'] and provider.proposals[:2] == ['other','selected']
    rejected = next(row for row in result.tool_results if row.status=='failed')
    assert rejected.invocation_id in recovered_failure_ids(result.tool_results)
    reviews = provider.review_calls
    assert runner.review_turn_result(result, **{k:v for k,v in kwargs.items() if k!='max_iterations'}) is result
    assert provider.review_calls == reviews


@pytest.mark.parametrize('resumed',[False,True])
def test_unchanged_rejected_action_stops_without_reopening_review(tmp_path, monkeypatch,resumed):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass
    runtime,registry,_ = runtime_and_registry(tmp_path,monkeypatch,'resource_read')
    registry.register(ToolSpec('browser_run_task','Perform action',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),
        lambda inv: pytest.fail('Rejected action executed'))
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system','')
            if 'Workflow action preflight.' in system:
                return text({'decision':'revise','issues':['対象が違います。']})
            if 'material-requirements planner' in system: return super().create(**kw)
            if 'semantic completion reviewer' in system:
                self.review_calls += 1
                return text({'disposition':'retry','unresolved_requirements':['変更は未実行です。'],'retry_tool_names':['browser_run_task']})
            self.agent_calls += 1
            return call('browser_run_task',{'target':'wrong'},self.agent_calls)
    provider=Provider('browser_run_task');runner=AgentOrchestrator(model_client=provider)
    kwargs=dict(conversation_id='web:stalled',principal_id='web:stalled',user_message='選択した対象を更新して。',
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    result=(runner.resume_turn(**{k:v for k,v in kwargs.items() if k!='conversation_history'},max_iterations=8,
        messages_snapshot=[{'role':'user','content':[{'type':'text','text':kwargs['user_message']}]}]) if resumed else
        runner.run_turn(**kwargs,max_iterations=8))
    assert result.response_fulfilled is False
    assert provider.agent_calls == 2 and provider.review_calls == 1
    assert result.outcome_review_receipt.disposition == 'blocked'
    assert runner.review_turn_result(result,**kwargs) is result
    assert provider.review_calls == 1


def test_corrected_action_still_honors_cancellation_before_execution(tmp_path,monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass
    runtime,registry,_=runtime_and_registry(tmp_path,monkeypatch,'resource_read')
    registry.register(ToolSpec('browser_run_task','Perform action',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),
        lambda inv:pytest.fail('Cancelled action executed'))
    cancelled=[]
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system','')
            if 'Workflow action preflight.' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                if packet['proposed_action']['arguments']['target']=='other':
                    return text({'decision':'revise','issues':['Corrige el destino.']})
                cancelled.append(True)
                return text({'decision':'proceed','issues':[]})
            self.agent_calls+=1
            return call('browser_run_task',{'target':'other' if self.agent_calls==1 else 'selected'},self.agent_calls)
    provider=Provider('browser_run_task')
    result=AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:cancelled-revision',
        principal_id='web:cancelled-revision',user_message='Actualiza el destino.',conversation_history=[],tool_registry=registry,
        policy_store=runtime.store,approval_store=runtime.store,cancellation_checker=lambda:bool(cancelled))
    assert provider.agent_calls==2 and provider.review_calls==0
    assert result.response_fulfilled is False


@pytest.mark.asyncio
async def test_delegated_plan_revision_uses_shared_contract_and_keeps_corrected_receipts(tmp_path,monkeypatch):
    import asyncio
    from nullion.context_bus import ContextBus
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig
    from nullion.task_queue import TaskRecord,TaskStatus,TaskPriority
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult,ToolRegistry
    from nullion.workflow_recovery import recovered_failure_ids
    runtime,_,_=runtime_and_registry(tmp_path,monkeypatch,'resource_read')
    class AdapterRegistry(ToolRegistry):
        def list_tool_definitions(self,**kwargs):
            definitions=super().list_tool_definitions(**kwargs)
            return [{**row,'fallback_tools':['alternate_write']} if row['name']=='resource_write' else row for row in definitions]
    registry=AdapterRegistry()
    registry.register(ToolSpec('alternate_write','Alternate action',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),
        lambda inv:pytest.fail('Rejected arguments reached an alternate executor'))
    writes=[]
    def execute(inv):
        writes.append(inv.arguments['target'])
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':7})
    registry.register(ToolSpec('resource_write','Update the selected resource',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,2,input_schema={'type':'object','properties':{'target':{'type':'string'}},'required':['target']}),execute)
    preflight_tools=[]
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system','')
            if 'Workflow action preflight.' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                preflight_tools.append(packet['proposed_action']['tool_name'])
                target=packet['proposed_action']['arguments']['target']
                return text({'decision':'revise','issues':['Corrige el destino.']} if target=='other' else {'decision':'proceed','issues':[]})
            if 'material-requirements planner' in system or 'semantic completion reviewer' in system:
                return super().create(**kw)
            self.agent_calls+=1
            if self.agent_calls<3:return call('resource_write',{'target':'other' if self.agent_calls==1 else 'selected'},self.agent_calls)
            return text('Observed value: 7.')
    provider=Provider('resource_write')
    task=TaskRecord(task_id='revision',group_id='group',conversation_id='web:delegated',principal_id='web:delegated',
        title='Selected resource',description='Actualiza el destino seleccionado y devuelve su valor.',status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,allowed_tools=['resource_write'],dependencies=[],context_key_out='observed')
    result=await DeepAgentMiniAgentRunner().run(MiniAgentConfig('writer',task,timeout_s=30),anthropic_client=provider,
        tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,context_bus=ContextBus(),progress_queue=asyncio.Queue())
    assert result.status=='success' and writes==['selected'],result
    assert result.output=='Observed value: 7.' and provider.review_calls==1
    assert preflight_tools==['resource_write','resource_write']
    assert recovered_failure_ids(result.verified_turn_result.tool_results)
