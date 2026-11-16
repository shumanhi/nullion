"""Facts survive the exact multi-source failure shape seen in production."""
from copy import deepcopy
import json

import pytest

from nullion.code_mode import compact_output
from nullion.source_observation_contract import compact_source_observations


def source_bundle(name, *, capture_chars=4000, count=4):
    url='https://source.test/'+name+'?selection='+('opaque-%7E'*35)
    findings=[{'label':f'{name}-{i}','value':f'{name}-verified-{i}',
        'quote':f'{name}-verified-{i}, 2031-04-08, 18:00, €73',
        'source_url':url,'captured_at':37,'evidence_path':'/owned/capture-'+name+'.png',
        'main_document_text':'optional page context '*180} for i in range(count)]
    return {'result_kind':'source_observations','report_status':'partial','goal_status':'partial',
        'next_action':'report','page_url':url,'observations':findings,'unknowns':['未確認の追加情報'],
        'execution_audit':{'version':1,'complete':True,'read_only_actions_only':True,
            'captures_are_internal':True,'host_file_actions_enabled':False,
            'downloaded_file_count':0,'deliverable_file_count':0,
            'visited_urls':['https://source.test/visit?'+str(i)+'x'*620 for i in range(3)],
            'url_inventory_complete':True,'action_names':['navigate']*30},
        'performance':{'model_calls':20,'input_tokens':500000,'model_ms':200000},
        'captured_documents':[{'source_id':0,'source_url':url,'main_document_text':'x'*capture_chars}]}


def test_small_source_budget_keeps_facts_ahead_of_navigation_diagnostics():
    source=source_bundle('記録');before=deepcopy(source)
    view=compact_source_observations(source,max_chars=2400)
    for _ in range(4):
        assert view['observations'] and view['observations'][0]['value']=='記録-verified-0'
        assert view['observations'][0]['quote']==source['observations'][0]['quote']
        assert len(json.dumps(view,ensure_ascii=False))<=2400
        assert view['page_url']==source['page_url']
        assert view['report_status']=='partial'
        assert not view['execution_audit'].get('url_inventory_complete') or 'visited_urls' in view['execution_audit']
        view=compact_source_observations(view,max_chars=2400)
    assert source==before


def program_output(*, arbitrary=False):
    sources=[source_bundle('vol'),source_bundle('宿泊')]
    value={'selected_a':sources[0],'selected_b':sources[1]}
    if arbitrary:value['other']='x'*100000
    native=[{'invocation_id':f'owned-{i}','tool_name':'source_tool','status':'completed',
        'output':s,'omitted_paths':[]} for i,s in enumerate(sources)]
    return {'reason':'code_mode_completed','value':value,'native_observations':native,
        'operations':[{'invocation_id':r['invocation_id'],'tool_name':r['tool_name'],'status':'completed'} for r in native],
        'automatic_replay':False}


@pytest.mark.parametrize('arbitrary',[False,True])
def test_nested_source_values_or_native_fallback_keep_both_sources_across_repeated_compaction(arbitrary):
    source=program_output(arbitrary=arbitrary);before=deepcopy(source)
    view=source
    for _ in range(4):
        view=compact_output(view,max_chars=48000)
        assert len(json.dumps(view,ensure_ascii=False))<=32000
        assert view['source_bundle_count']==2
        assert 'vol-verified-0' in json.dumps(view,ensure_ascii=False)
        assert '宿泊-verified-0' in json.dumps(view,ensure_ascii=False)
        assert view['operations']==source['operations']
        if arbitrary:
            assert {r['invocation_id'] for r in view['native_observations']}=={'owned-0','owned-1'}
        else:
            assert view['value']['selected_a']['observations']
            assert view['value']['selected_b']['observations']
            assert 'captured_documents' not in view['value']['selected_a']
    assert source==before


def test_model_boundary_and_history_use_the_same_finding_projection():
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.chat_operator import _compact_tool_output_for_context
    from nullion.tools import ToolResult
    source=program_output()
    message=json.loads(_tool_result_message_payload(ToolResult('program','code_mode','completed',source)))['output']
    history=_compact_tool_output_for_context('code_mode',source)
    assert message==history
    assert 'vol-verified-0' in json.dumps(message,ensure_ascii=False)
    assert '宿泊-verified-0' in json.dumps(message,ensure_ascii=False)


def test_return_allowance_scales_with_typed_source_results_and_has_a_hard_ceiling():
    from nullion.code_mode import _program_result_budget
    from nullion.tools import ToolResult
    assert _program_result_budget('ordinary value',[])==64000
    for count,expected in [(1,64000),(2,128000),(4,256000),(10,256000)]:
        sources=[source_bundle(str(i)) for i in range(count)]
        native=[ToolResult('owned-'+str(i),'read','completed',s) for i,s in enumerate(sources)]
        assert _program_result_budget(sources,native)==expected
        assert _program_result_budget({'records':sources},[])==expected
        assert _program_result_budget(None,native)==expected
    assert _program_result_budget({'untyped_records':[{'id':i} for i in range(1000)]},[])==64000


def test_declared_sources_decoded_from_files_get_the_same_bounded_view():
    source=program_output();source['native_observations']=[]
    source['operations']=[{'invocation_id':'owned-file','tool_name':'file_read','status':'completed'}]
    view=compact_output(source,max_chars=48000)
    assert view['source_bundle_count']==2
    assert len(json.dumps(view,ensure_ascii=False))<=32000
    assert all(name+'-verified-0' in json.dumps(view,ensure_ascii=False) for name in ('vol','宿泊'))
