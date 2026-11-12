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
    assert provider.review_calls == 0
    assert result.response_fulfilled is act_after_nudge, result
    assert result.outcome_review_receipt is None
    if act_after_nudge:assert 'Observed value: 7.' in result.final_text
    if terminal_verdict=='needs_user_input':assert '1. A\n2. B' not in result.final_text
    if not act_after_nudge:assert 'Tool selection' in result.final_text
    assert runner.review_turn_result(result,**kwargs) is result
    assert provider.review_calls == 0
