from copy import deepcopy
import pytest
from nullion.search_coverage import discovery_ledger, assessment_result
from nullion.tools import ToolResult
from tests.test_bounded_comparison import fixture


@pytest.mark.parametrize('path',[['record'],['payload','entry']])
def test_declared_structured_detail_is_observed_without_a_text_field(path):
    url='https://source.example/item?filter=7#chosen'
    record={'key':'選択された対象','amount':7,'unit':'g'}
    output={'source_url':url,'source_record_collections':[{'path':path,'identity_fields':['key'],'cardinality':'one'}]}
    parent=output
    for key in path[:-1]:parent[key]={};parent=parent[key]
    parent[path[-1]]=record
    ledger=discovery_ledger([ToolResult('detail','opaque_adapter','completed',output)])
    assert ledger['candidates'][0]['url']==url
    assert ledger['candidates'][0]['observation_invocation_ids']==['detail']
    assert ledger['observed_sources'][0]['observation_invocation_ids']==['detail']


@pytest.mark.parametrize('case',['navigation','empty','bad_path','missing_identity','identity_inventory','failed','credentials'])
def test_declarations_never_turn_navigation_empty_or_failed_receipts_into_observations(case):
    url='https://source.example/item'
    output={'page_url':url,'record':{'key':'item','amount':7},
        'source_record_collections':[{'path':['record'],'identity_fields':['key'],'cardinality':'one'}]}
    status='completed'
    if case=='navigation':output.pop('source_record_collections')
    elif case=='empty':output['record']={}
    elif case=='bad_path':output['source_record_collections'][0]['path']=['absent']
    elif case=='missing_identity':output['source_record_collections'][0]['identity_fields']=['missing']
    elif case=='identity_inventory':output['result_kind']='source_identity_inventory'
    elif case=='credentials':output['page_url']='https://user:secret@source.example/item'
    else:status='failed'
    ledger=discovery_ledger([ToolResult('detail','opaque_adapter',status,output)])
    assert not ledger or not any(row['observation_invocation_ids'] for row in ledger['candidates'])


def test_collection_observes_its_document_not_each_linked_target():
    output={'page_url':'https://source.example/catalog','source_candidates':[{'url':'https://source.example/a'}],
        'entries':[{'key':'a','url':'https://source.example/a','amount':7}],
        'source_record_collections':[{'path':['entries'],'identity_fields':['key'],'cardinality':'many'}]}
    ledger=discovery_ledger([ToolResult('catalog','opaque_adapter','completed',output)])
    by_url={row['url']:row for row in ledger['candidates']}
    assert by_url['https://source.example/catalog']['observation_invocation_ids']==['catalog']
    assert by_url['https://source.example/a']['observation_invocation_ids']==[]


def test_runtime_bound_detail_record_aliases_resolve_to_observed_parent():
    context,value,positive=fixture()
    value=deepcopy(value)
    value['comparison_scope']['selected_evidence_invocation_ids']=['a-record','b-record']
    positive|={'a-record','b-record'}
    assert assessment_result(value,context=context,positive_ids=positive,
        evidence_parents={'a-record':'a','b-record':'b'})['errors']==[]
