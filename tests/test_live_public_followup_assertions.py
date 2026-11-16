from copy import deepcopy
import pytest
from tests.e2e.live_public_followup_assertions import assert_public_followup_outcome


@pytest.mark.parametrize('mutation',['none','complete','native_complete','missing_completion','false_fulfillment','fatal_review','unexplained_review','failed_read','navigation_only','missing_link','account_tool',
    'batch','batch_empty','batch_unlinked','batch_failed','search_snippets'])
def test_public_followup_requires_native_read_visible_link_and_nonfatal_review(mutation):
    url='https://public.example/記録'
    response={'text':'観測した情報 '+url}
    turns=[{'response_fulfilled':None,'tool_results':[{'invocation_id':'observed',
        'tool_name':'browser_extract_text','status':'completed','output':{'page_url':url,
        'record':{'text':'Contenido observado: 37'},'source_record_collections':[{'path':['record']}]}}]}]
    reviews=[{'disposition':'retry','review_error':'ModelClientTimeoutError','runtime_failure_code':None}]
    row=turns[0]['tool_results'][0]
    if mutation=='complete':reviews[0].update(disposition='complete',review_error=None);turns[0]['response_fulfilled']=True
    if mutation=='native_complete':reviews=[];turns[0]['response_fulfilled']=True
    if mutation=='missing_completion':reviews=[]
    if mutation=='false_fulfillment':turns[0]['response_fulfilled']=False
    if mutation=='fatal_review':reviews[0]['runtime_failure_code']='unverified_requirements'
    if mutation=='unexplained_review':reviews[0]['review_error']=None
    if mutation=='failed_read':row['status']='failed'
    if mutation=='navigation_only':row['output'].pop('source_record_collections')
    if mutation=='missing_link':response['text']='Respuesta sin enlace'
    if mutation=='account_tool':turns[0]['tool_results'].append({'tool_name':'email_read','status':'failed'})
    if mutation.startswith('batch') or mutation=='search_snippets':
        row.update(tool_name='browser_read_pages',output={'result_kind':'browser_page_batch',
            'source_record_collections':[{'path':['details'],'cardinality':'many','identity_fields':['source_page_url']}],
            'details':[{'source_page_url':url,'record':{'details':'Precio observado: 37'}}]})
        if mutation=='batch_empty':row['output']['details'][0]['record']['details']=''
        if mutation=='batch_unlinked':row['output']['details'][0]['source_page_url']='https://public.example/other'
        if mutation=='batch_failed':row['status']='failed'
        if mutation=='search_snippets':row['output']['result_kind']='public_search_results'
    before=deepcopy((response,turns,reviews))
    if mutation in {'none','complete','native_complete','batch'}:
        result=assert_public_followup_outcome(response=response,turns=turns,reviews=reviews)
        assert result['observed_sources'][0]['invocation_id']=='observed'
        assert result['request_completion_certified'] is (mutation in {'complete','native_complete'})
    else:
        with pytest.raises(AssertionError):assert_public_followup_outcome(response=response,turns=turns,reviews=reviews)
    assert (response,turns,reviews)==before


@pytest.mark.parametrize('text,linked,status', [('Datos observados', True, 'completed'),
    ('確認した内容', True, 'completed'), ('', True, 'completed'),
    ('Datos observados', False, 'completed'), ('Datos observados', True, 'failed')])
def test_text_document_uses_its_declared_native_url(text, linked, status):
    url = 'https://public.example/production'
    turn = {'response_fulfilled': True, 'tool_results': [{'invocation_id': 'read-doc',
        'tool_name': 'browser_read_pages', 'status': status, 'output': {
            'source_record_collections': [{'path': ['documents'], 'identity_fields': ['url']}],
            'documents': [{'url': url, 'text': text}]}}]}
    response = {'text': 'Fuente ' + (url if linked else 'https://elsewhere.example/')}
    if text and linked and status == 'completed':
        assert assert_public_followup_outcome(response=response, turns=[turn], reviews=[])['observed_sources']
    else:
        with pytest.raises(AssertionError):
            assert_public_followup_outcome(response=response, turns=[turn], reviews=[])


@pytest.mark.parametrize('mutation',['none','missing_label','unobserved_label','wrong_link','discovery','identity_only'])
def test_native_listing_binds_its_observed_link_label_to_captured_text(mutation):
    url='https://public.example/product?opaque=A37&variant=blue'
    doc={'url':'https://public.example/list','text':'照明 37 — 価格 988',
        'links':[{'url':url,'text':'照明 37'}]}
    out={'result_kind':'browser_page_collection','documents':[doc],
        'source_record_collections':[{'path':['documents'],'identity_fields':['url']}]}
    if mutation=='missing_label':doc['links'][0]['text']=''
    if mutation=='unobserved_label':doc['links'][0]['text']='別の製品'
    if mutation=='wrong_link':doc['links'][0]['url']='https://other.example/'
    if mutation=='discovery':out['result_kind']='public_search_results'
    if mutation=='identity_only':out={'result_kind':'source_identity_inventory','source_identity_url_paths':[url]}
    turn={'response_fulfilled':True,'tool_results':[{'invocation_id':'native-read',
        'tool_name':'browser_read_pages','status':'completed','output':out}]}
    if mutation=='none':
        proof=assert_public_followup_outcome(response={'text':'確認した製品 '+url},turns=[turn],reviews=[])
        assert proof['observed_sources'][0]['evidence_kind']=='native_listing_link'
    else:
        with pytest.raises(AssertionError):
            assert_public_followup_outcome(response={'text':'確認した製品 '+url},turns=[turn],reviews=[])
