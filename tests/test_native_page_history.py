from copy import deepcopy
import json
import pytest


@pytest.mark.parametrize('surface',['chat','web','startup'])
@pytest.mark.parametrize('label',['照明 37','Lámpara 37'])
def test_native_page_history_keeps_facts_and_whole_links_with_explicit_omissions(surface,label):
    from nullion.chat_operator import _compact_tool_output_for_context
    from nullion.web_app import _compact_web_tool_output_for_context
    from nullion.startup_maintenance import _compact_tool_output
    url='https://source.example/item?opaque='+('A37'*60)+'&variant=blue'
    output={'result_kind':'browser_page_collection','completed_pages':1,'requested_pages':1,
        'documents':[{'url':'https://source.example/list','text':label+' 988\n'+'extra '*9000,
            'links':[{'url':url,'text':label},*[{'url':'https://source.example/item/'+str(i)+'?opaque='+'Q'*120,'text':'Item '+str(i)} for i in range(200)]]}],
        'source_record_collections':[{'path':['documents'],'identity_fields':['url'],'context_fields':['text','links']}]}
    before=deepcopy(output)
    compactors={'chat':_compact_tool_output_for_context,'web':_compact_web_tool_output_for_context,'startup':_compact_tool_output}
    compact=compactors[surface]('browser_read_pages',output)
    doc=compact['documents'][0]
    assert label+' 988' in doc['text'] and doc['links'][0]=={'url':url,'text':label}
    assert doc['history_projection']['text_truncated'] is True
    assert doc['history_projection']['omitted_link_count']==201-len(doc['links'])>0
    assert compact['history_projection']['omissions_are_not_absence'] is True
    assert len(json.dumps(compact,ensure_ascii=False))<=12000
    assert compactors[surface]('browser_read_pages',compact)==compact
    assert output==before


def test_native_history_projection_does_not_drop_mixed_structured_rows():
    from nullion.source_context_projection import compact_native_page_history
    assert compact_native_page_history({'result_kind':'browser_page_collection','items':[{'id':'opaque'}]},max_chars=100) is None


def test_partial_native_page_history_preserves_failure_and_exact_url():
    from nullion.source_context_projection import compact_native_page_history
    output={'result_kind':'browser_page_collection','completed_pages':1,'requested_pages':2,
        'documents':[{'url':'https://source.example/ok?opaque=A37','text':'観測 '*9000,'links':[]}],
        'pages':[{'status':'completed','page_url':'https://source.example/ok?opaque=A37'},
            {'status':'failed','requested_url':'https://source.example/missing?opaque=Q73',
                'failure_reason':'source_unavailable','requires_user_choice':True}],
        'source_record_collections':[{'path':['documents'],'identity_fields':['url']}]}
    compact=compact_native_page_history(output,max_chars=4000)
    assert compact['completed_pages']==1 and compact['requested_pages']==2
    assert compact['pages']==output['pages']
    assert compact['documents'][0]['url']==output['documents'][0]['url']
