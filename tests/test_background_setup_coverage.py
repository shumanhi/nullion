from dataclasses import replace
import pytest
from nullion.agent_orchestrator import _CompletionRequirementContractRow as Row
from nullion.tools import ToolResult
from nullion.workflow_reasoning import accepted_setup_only_contract


def accepted():
    return ToolResult('start','opaque_start','completed',{
        'action_receipt':{'type':'action_receipt','object_id':'owned'},
        'deferred_execution':{'version':1,'phase':'accepted','handle':'owned','owner':'opaque_worker','configuration':{'duration_seconds':60}}})


def test_setup_and_preparation_are_not_a_future_search_outcome():
    rows=[Row('r','設定','action',('opaque_start',),'setup'),
          Row('p','内部準備','constraint',('source_read',),'prep',delivery_required=False),
          Row('c','頻度','constraint',('opaque_start',),'cadence'),
          Row('a','説明','reasoned_answer',(),'answer')]
    assert accepted_setup_only_contract(rows,[accepted()])
    assert not accepted_setup_only_contract(rows,[])
    assert not accepted_setup_only_contract(rows,[replace(accepted(),status='failed')])
    broken=accepted();broken.output['deferred_execution']['handle']='other'
    assert not accepted_setup_only_contract(rows,[broken])


@pytest.mark.parametrize('kind',['fact','comparison_side','deliverable','action','constraint'])
def test_independent_immediate_outcomes_still_require_source_coverage(kind):
    rows=[Row('r','設定','action',('opaque_start',),'setup'),Row('now','現在の情報',kind,('source_read',),'now')]
    assert not accepted_setup_only_contract(rows,[accepted()])
    assert not accepted_setup_only_contract(rows[1:],[accepted()])


def test_source_preparation_alone_is_not_accepted_setup():
    assert not accepted_setup_only_contract([Row('p','準備','constraint',('source_read',),'prep',delivery_required=False)],[accepted()])
    assert not accepted_setup_only_contract([Row('r','設定','action',('opaque_start','other_write'),'setup')],[accepted()])


def test_registered_read_actions_can_prepare_setup_but_do_not_waive_source_facts():
    rows=[Row('r','設定','action',('opaque_start','source_read'),'setup'),
          Row('p','準備','action',('source_read',),'prep')]
    assert accepted_setup_only_contract(rows,[accepted()],read_tool_names={'source_read'})
    assert not accepted_setup_only_contract(rows,[accepted()])
    assert not accepted_setup_only_contract(rows[1:],[accepted()],read_tool_names={'source_read'})
    for kind in ['fact','comparison_side','deliverable']:
        assert not accepted_setup_only_contract([*rows,Row('now','現在',kind,('source_read',),'now')],
            [accepted()],read_tool_names={'source_read'})
