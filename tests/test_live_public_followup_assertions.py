from copy import deepcopy
import pytest
from tests.e2e.live_public_followup_assertions import assert_public_followup_outcome


@pytest.mark.parametrize('mutation',['none','complete','false_fulfillment','fatal_review','unexplained_review','failed_read','navigation_only','missing_link','account_tool'])
def test_public_followup_requires_native_read_visible_link_and_nonfatal_review(mutation):
    url='https://public.example/記録'
    response={'text':'観測した情報 '+url}
    turns=[{'response_fulfilled':None,'tool_results':[{'invocation_id':'observed',
        'tool_name':'browser_extract_text','status':'completed','output':{'page_url':url,
        'record':{'text':'Contenido observado: 37'},'source_record_collections':[{'path':['record']}]}}]}]
    reviews=[{'disposition':'retry','review_error':'ModelClientTimeoutError','runtime_failure_code':None}]
    row=turns[0]['tool_results'][0]
    if mutation=='complete':reviews[0].update(disposition='complete',review_error=None);turns[0]['response_fulfilled']=True
    if mutation=='false_fulfillment':turns[0]['response_fulfilled']=False
    if mutation=='fatal_review':reviews[0]['runtime_failure_code']='unverified_requirements'
    if mutation=='unexplained_review':reviews[0]['review_error']=None
    if mutation=='failed_read':row['status']='failed'
    if mutation=='navigation_only':row['output'].pop('source_record_collections')
    if mutation=='missing_link':response['text']='Respuesta sin enlace'
    if mutation=='account_tool':turns[0]['tool_results'].append({'tool_name':'email_read','status':'failed'})
    before=deepcopy((response,turns,reviews))
    if mutation in {'none','complete'}:
        result=assert_public_followup_outcome(response=response,turns=turns,reviews=reviews)
        assert result['observed_sources'][0]['invocation_id']=='observed'
        assert result['request_completion_certified'] is (mutation=='complete')
    else:
        with pytest.raises(AssertionError):assert_public_followup_outcome(response=response,turns=turns,reviews=reviews)
    assert (response,turns,reviews)==before
