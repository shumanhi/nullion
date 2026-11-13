import json
import pytest
from nullion.tools import ToolInvocation, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
from nullion.workflow_reasoning import assess_action
from nullion.tool_repair_contract import (plan_revision_recovery, unresolved_plan_revision_count,
    exhausted_plan_revision_tool)




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
