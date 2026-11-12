import pytest
from nullion.tools import ToolResult
from nullion.source_observation_contract import source_capture_review_records


def test_capture_records_preserve_literal_roles_controls_and_budget():
    output={'result_kind':'source_observations','report_status':'completed','goal_status':'answered','observations':[
        {'value':'Cedar 31','quote':'Cedar 31','semantic_role':'entity','source_url':'https://example.com/a','captured_at':1},
        {'value':'Maple 22','quote':'Maple 22','semantic_role':'entity','source_url':'https://example.com/b','captured_at':2},
        {'value':'Reserve','quote':'Reserve','semantic_role':'interface_control','source_url':'https://example.com/b','captured_at':2,'control_state':{'tag':'BUTTON','text':'Reserve'}},
        {'value':'Unsupported 99','quote':'Only 10','source_url':'https://example.com/c','captured_at':3},
        {'value':'Unbound 44','quote':'Unbound 44'}]}
    result=ToolResult('parent','browser_run_task','completed',output)
    records,fingerprints=source_capture_review_records([result]);assert len(records)==3 and len(set(fingerprints.values()))==3
    assert all(r.output['source_record_binding']['parent_invocation_id']=='parent' for r in records)
    assert records[-1].output['observations'][0]['control_state']['text']=='Reserve'
    assert records[-1].output['observations'][0]['semantic_role']=='interface_control'
    assert source_capture_review_records([result],max_chars=1)==([], {})
    assert source_capture_review_records([ToolResult('bad','browser_run_task','failed',output)])==([], {})
