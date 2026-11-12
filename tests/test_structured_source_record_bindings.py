import copy
import pytest
from nullion.tools import ToolResult
from nullion.source_observation_contract import source_capture_review_records

@pytest.mark.parametrize('rows',[
    [{'key':'uno','value':31},{'key':'二','value':22}],
    [{'key':'ثلاثة','value':0},{'key':'vier','value':None}],
])
def test_declared_batch_rows_remain_distinct_exact_and_do_not_change_parent(rows):
    output={'rows':rows,'source_record_collections':[{'path':['rows'],'identity_fields':['key']}]};before=copy.deepcopy(output)
    bound,fingerprints=source_capture_review_records([ToolResult('real-source','fixture_read','completed',output)])
    assert len(bound)==2 and len(fingerprints)==2
    assert [r.output['record'] for r in bound]==rows and output==before
    assert all(r.output['source_record_binding']['parent_invocation_id']=='real-source' for r in bound)
    assert len(source_capture_review_records([ToolResult('real-source','fixture_read','completed',output)],max_records=1)[0])==1
    assert not source_capture_review_records([ToolResult('real-source','fixture_read','completed',output)],max_chars=1)[0]

@pytest.mark.parametrize('output,status,error',[
    ({'rows':[{'key':'x'}]},'completed',None),
    ({'rows':[{'key':True}],'source_record_collections':[{'path':['rows'],'identity_fields':['key']}]},'completed',None),
    ({'rows':[{'key':'x'}],'source_record_collections':[{'path':'rows','identity_fields':['key']}]},'completed',None),
    ({'rows':[{'key':'x'}],'source_record_collections':[{'path':['rows'],'identity_fields':['key']}]},'failed',None),
    ({'rows':[{'key':'x'}],'source_record_collections':[{'path':['rows'],'identity_fields':['key']}]},'completed','source unavailable'),
])
def test_undeclared_invalid_or_failed_rows_cannot_become_independent_proof(output,status,error):
    assert source_capture_review_records([ToolResult('source','fixture_read',status,output,error)])==( [],{} )
