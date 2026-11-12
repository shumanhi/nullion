import json
import pytest
from nullion.tools import ToolInvocation, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
from nullion.workflow_reasoning import assess_action
from nullion.tool_repair_contract import (plan_revision_recovery, unresolved_plan_revision_count,
    exhausted_plan_revision_tool)


@pytest.mark.parametrize('case',['partial_observation','protocol_correction','real_conflict','mutation_conflict'])
def test_action_review_separates_admissible_steps_from_final_coverage(case):
    calls=[]
    class Provider:
        def create(self,**kw):
            calls.append(json.loads(kw['messages'][0]['content'][0]['text']))
            value={'decision':'proceed','issues':[], 'issue_kind':'outcome_coverage_gap','step_effect':'observation'}
            if case=='protocol_correction' and len(calls)==1:
                value.update(decision='revise',issues=['More evidence may be needed later.'])
            elif case in {'real_conflict','mutation_conflict'}:
                value.update(decision='revise',issue_kind='concrete_conflict',
                    step_effect='mutation' if case=='mutation_conflict' else 'observation',issues=['対象が違います。'])
            return {'content':[{'type':'text','text':json.dumps(value)}]}
    spec=ToolSpec('opaque_action','Inspect a selected resource',ToolRiskLevel.HIGH,ToolSideEffectClass.DANGEROUS_EXEC,True,3)
    result=assess_action(model_client=Provider(),request='選択した対象を調べて。',context={},
        invocation=ToolInvocation('i','opaque_action','p',{'target':'a'}),spec=spec,evidence=[],now='2026-11-03T00:00:00Z')
    assert result.decision==('revise' if case in {'real_conflict','mutation_conflict'} else 'proceed')
    assert len(calls)==(2 if case=='protocol_correction' else 1)
    assert result.error is None
    assert ('protocol_validation_errors' in calls[-1]) is (case=='protocol_correction')


def rejected(ident,tool='opaque_action'):
    return ToolResult(ident,tool,'failed',{'reason':'request_plan_conflict','executed':False,
        'review_decision':'revise','retryable':True,'recovery':plan_revision_recovery(tool)})


def test_changed_proposals_and_scope_calls_are_not_execution_progress():
    rows=[rejected('a'),ToolResult('scope','request_tool_scope','completed',{}),rejected('b'),rejected('c')]
    assert unresolved_plan_revision_count(rows,'opaque_action')==3
    assert exhausted_plan_revision_tool(rows)=='opaque_action'
    rows.append(ToolResult('real','opaque_action','completed',{'value':7}))
    assert unresolved_plan_revision_count(rows,'opaque_action')==0
    assert exhausted_plan_revision_tool(rows) is None
    rows.append(rejected('d'))
    assert unresolved_plan_revision_count(rows,'opaque_action')==1


def test_recovered_and_other_tool_proposals_do_not_consume_current_repair_allowance():
    rows=[rejected(str(i)) for i in range(3)]
    for row in rows:row.recovered_by=('real',)
    rows += [rejected('other','different'),rejected('current')]
    assert exhausted_plan_revision_tool(rows) is None
