from copy import deepcopy
import json
import pytest


def source(count=50):
    return {'result_kind':'browser_page_collection','completed_pages':1,'requested_pages':1,
        'pages':[{'status':'completed','page_url':'https://example.test/list'}],
        'items':[{'title':f'選択肢 {i} '+('詳細 ' * 200),'url':f'https://example.test/item/{i}?opaque='+('xyz'*180),
            'price_text':f'USD {i+1}.00','quantity':20 if i==0 else 4,
            'numeric_snippets':[str(i+1),'20 g' if i==0 else '4 g'], 'image_alt':'image '*100}
            for i in range(count)],'source_record_collections':[{'path':['items'],'cardinality':'many',
                'identity_fields':['title','url'],'context_fields':['price_text','numeric_snippets','quantity']}],
        'strategy': {'kind': 'native_read_preflight', 'adaptive_browser_started': False,
            'continuation': {'tool_name': 'browser_run_task', 'execution_kind': 'adaptive_interaction'}},
        'coverage':{'returned_count':count,'truncated':False,'sections':[{'heading':'text'*100}]*count}}


@pytest.mark.parametrize('tool',['browser_read_pages','browser_run_task','browser_extract_items'])
def test_large_source_view_keeps_all_rows_and_exact_original_evidence(tool):
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.tools import ToolResult
    from nullion.retained_work_context import inspect_retained
    original=source();before=deepcopy(original);result=ToolResult('owned-source',tool,'completed',original)
    payload=json.loads(_tool_result_message_payload(result));view=payload['output']
    assert original==before and len(json.dumps(payload,ensure_ascii=False))<18000
    table=view['collection_views'][0]
    assert table['record_count']==50 and len(table['rows'])==50
    assert view['retained_evidence']['invocation_id']=='owned-source'
    assert table['has_clipped_fields'] and view['page_outcomes'][0]['status']=='completed'
    assert view['coverage']['truncated'] is False
    assert view['strategy'] == original['strategy']
    inspection=inspect_retained({'requests':[{'invocation_id':'owned-source','json_pointer':'/items/49',
        'offset':0,'length':6000}]},[result],max_chars=12000)
    assert original['items'][49]['url'] in json.dumps(inspection,ensure_ascii=False)


def test_small_view_and_false_values_are_unchanged():
    from nullion.source_context_projection import compact_collections
    original={'source_record_collections':[{'path':['record'],'cardinality':'one','identity_fields':['id']}],
        'record':{'id':'α','available':False,'price':0}}
    assert compact_collections(original) is original


def test_wide_record_projection_has_explicit_omissions_instead_of_cut_json():
    from nullion.source_context_projection import compact_collections
    original=source(200)
    view=compact_collections(original)
    assert len(json.dumps(view,ensure_ascii=False))<14000
    assert view['retained_evidence']['complete'] is True
    assert view['collection_views'][0]['record_count']==200
    assert len(original['items'])==200


def test_nested_single_record_keeps_exact_pointer_and_ignores_invalid_columns():
    from nullion.source_context_projection import compact_collections
    from nullion.retained_work_context import inspect_retained
    from nullion.tools import ToolResult

    original = {
        'a/b': {'~detail': {'id': '項目', 'body': '詳細' * 12000, 'available': False, 'price': 0}},
        'source_record_collections': [{
            'path': ['a/b', '~detail'], 'cardinality': 'one',
            'identity_fields': ['id', {'invalid': 'column'}],
            'context_fields': ['body', 'available', 'price'],
        }],
    }
    view = compact_collections(original)
    table = view['collection_views'][0]
    assert table['json_pointer'] == '/a~1b/~0detail'
    assert table['cardinality'] == 'one' and table['record_count'] == 1
    assert table['rows'][0][-2:] == [False, 0]
    exact = inspect_retained({'requests': [{
        'invocation_id': 'owned', 'json_pointer': '/a~1b/~0detail/id', 'offset': 0, 'length': 100,
    }]}, [ToolResult('owned', 'browser_run_task', 'completed', original)])
    assert exact['windows'][0]['content'] == '項目'


def test_ambiguous_provider_ids_still_allow_exact_owned_source_inspection():
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.retained_work_context import producer_context, inspect_retained
    from nullion.tools import ToolResult

    results = [ToolResult(f'owned-{i}', 'browser_read_pages', 'completed', source()) for i in range(2)]
    messages = [{'role': 'user', 'content': [{
        'type': 'tool_result', 'tool_use_id': 'reused',
        'content': [{'type': 'text', 'text': _tool_result_message_payload(result)}],
    }]} for result in results]
    view, enabled = producer_context(messages, results, {
        result.invocation_id: {'tool_use_id': 'reused'} for result in results
    })
    assert enabled and view == messages
    exact = inspect_retained({'requests': [{
        'invocation_id': 'owned-1', 'json_pointer': '/items/49/url', 'offset': 0, 'length': 1000,
    }]}, results)
    assert exact['windows'][0]['content'] == results[1].output['items'][49]['url']


def test_large_captured_document_index_cannot_displace_accepted_source_findings(monkeypatch):
    monkeypatch.setenv('NULLION_MODEL_TOOL_RESULT_MAX_CHARS','12000')
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.tools import ToolResult
    original = {'result_kind':'source_observations', 'goal_status':'partial', 'next_action':'report',
        'report_status':'partial', 'page_url':'https://example.com/offers',
        'observations':[{'label':'Alpha total','value':'$80','quote':'$80 Total', 'source_url':'https://example.com/offers'},
                        {'label':'Beta total','value':'$88','quote':'$88 Total', 'source_url':'https://example.com/offers'}],
        'unknowns':['After-hours return'], 'captured_documents':[{'source_id':0,'main_document_text':'unrelated row\n'*3000}],
        'source_record_collections':[{'path':['captured_documents'],'identity_fields':['source_id']}], 'artifact_paths':[]}
    before=deepcopy(original)
    view=json.loads(_tool_result_message_payload(ToolResult('owned-prices','browser_run_task','completed',original)))['output']
    assert original==before
    assert view['report_status']=='partial' and [r['value'] for r in view['observations']]==['$80','$88']
    assert view['unknowns']==['After-hours return'] and view['captured_document_inspection']['tool_name']=='inspect_retained_evidence'
    assert len(json.dumps(view))<14000
