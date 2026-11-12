"""Fair source budgets retain exact literals instead of clipping or hiding a side."""
import json
import pytest
from nullion.tools import ToolResult
from nullion.source_observation_contract import source_capture_review_records

@pytest.mark.parametrize('prefix',['Captured value','Valor capturado','取得した値'])
def test_source_budget_preserves_later_source_and_literal_fingerprints(prefix):
    rows=[]
    for i in range(3):
        for j in range(8 if i<2 else 1):
            value=f'{prefix} {i}/{j}: '+('x'*400 if i<2 else '17')
            rows.append({'value':value,'quote':value,'semantic_role':'entity','source_url':f'https://example.com/{i}','captured_at':i+1})
    parent=ToolResult('captured','fixture_read','completed',{'result_kind':'source_observations','report_status':'completed','observations':rows})
    diagnostic={};records,fingerprints=source_capture_review_records([parent],max_chars=6000,diagnostics=diagnostic)
    assert {r.output['sources'][0]['source_url'] for r in records}=={f'https://example.com/{i}' for i in range(3)}
    assert sum(len(json.dumps(r.output,ensure_ascii=False)) for r in records)<=6000
    assert diagnostic['budget_limited'] and diagnostic['represented_source_groups']==3
    assert set(fingerprints)=={r.invocation_id for r in records}
    for record in records:
        observation=record.output['observations'][0]
        assert observation['value']==observation['quote']
        assert observation['value'] in {row['value'] for row in rows}


def test_oversized_record_stays_whole_and_does_not_hide_shorter_valid_records():
    values=[('a','z'*20000),('a','31'),('b','17')]
    parent=ToolResult('capture','fixture_read','completed',{'result_kind':'source_observations','report_status':'completed','observations':[
        {'value':value,'quote':value,'source_url':'https://example.com/'+key,'captured_at':1} for key,value in values]})
    diagnostic={};records,_=source_capture_review_records([parent],max_chars=3000,diagnostics=diagnostic)
    assert {r.output['observations'][0]['value'] for r in records}=={'31','17'}
    assert diagnostic['omitted_records']==1 and diagnostic['budget_limited']
