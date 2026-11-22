"""Compacted model views retain request meaning and exact evidence access."""
from copy import deepcopy
import json

import pytest

from nullion.agent_orchestrator import _compact_focused_artifact_source_history, _compact_completed_artifact_producer_history
from nullion.retained_work_context import producer_context, inspect_retained
from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass


def registry():
    value=ToolRegistry()
    value.register(ToolSpec('file_write','Write HTML',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5),lambda invocation:None)
    return value


def state():
    return {'tool_flow_context':{'requires_artifact_delivery':True,'required_artifact_extensions':['.html']}}


@pytest.mark.parametrize('user_text', ['Use my saved location and keep the pending approval.',
                                   '保存済みの場所を使用し、承認を待ってください。'])
def test_focused_compaction_keeps_literal_request_and_restores_middle_facts(user_text):
    body='a'*12000+'EXACT_REQUIRED_VALUE=73\nhttps://example.test/exact?scope=original'+'z'*12000
    result=ToolResult('source-owned','browser_extract_text','completed',{'text':body})
    messages=[{'role':'system','content':'Saved profile location: 10024. Approval required before sending.'},
              {'role':'user','content':user_text},
              {'role':'assistant','content':[{'type':'tool_use','id':'provider-use','name':result.tool_name,'input':{}}]},
              {'role':'user','content':[{'type':'tool_result','tool_use_id':'provider-use','content':[{'type':'text','text':json.dumps(result.output)}]}]}]
    metadata={'source-owned':{'tool_use_id':'provider-use'}}
    before=deepcopy(messages);original=deepcopy(result.output)
    focused=_compact_focused_artifact_source_history(messages,state=state(),tool_results=[result],model_tool_registry=registry())
    assert {'role':'user','content':user_text} in focused
    for _ in range(3):
        focused, enabled=producer_context(focused,[result],metadata,include_unmapped=True,include_hidden=True)
        assert enabled and 'source-owned' in json.dumps(focused)
        assert user_text in [m.get('content') for m in focused]
    exact=inspect_retained({'requests':[{'invocation_id':'source-owned','json_pointer':'/text','offset':12000,'length':64}]},[result])
    assert 'EXACT_REQUIRED_VALUE=73' in exact['windows'][0]['content']
    assert messages==before and result.output==original


def test_completed_producer_compaction_keeps_failed_attempt_of_same_tool():
    results=[ToolResult('ok','file_write','completed',{'artifact_paths':['/tmp/output.html']}),
             ToolResult('bad','file_write','failed',{'executed':False,'reason':'invalid_tool_arguments'})]
    context=state();context['invocation_metadata']={'ok':{'tool_use_id':'ok-use'},'bad':{'tool_use_id':'bad-use'}}
    history=[]
    for use in ['ok-use','bad-use']:
        history.extend([{'role':'assistant','content':[{'type':'tool_use','id':use,'name':'file_write','input':{}}]},
                        {'role':'user','content':[{'type':'tool_result','tool_use_id':use,'content':[{'type':'text','text':use}]}]}])
    view=_compact_completed_artifact_producer_history(history,state=context,tool_results=results,model_tool_registry=registry())
    encoded=json.dumps(view)
    assert 'bad-use' in encoded and 'ok-use' not in encoded
    assert '"invocation_id": "ok"' in view[-1]['content'][0]['text']
    # Without verified protocol ownership, leave the input intact.
    assert _compact_completed_artifact_producer_history(history,state=state(),tool_results=results,model_tool_registry=registry())==history


def test_large_archive_collection_keeps_exact_inventory_and_ready_manifest():
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    root='/workspace/extracted-actual-37'
    manifest='/workspace/manifest.xlsx'
    output={'record':{'archive_path':'/uploads/original.zip'},
        'archive_path':'/uploads/original.zip', 'entry_count':111,
        'source_record_collections':[{'path':['record'],'cardinality':'one','identity_fields':['archive_path']}],
        'source_inventory':{'path':'/workspace/inventory.json','format':'json','rows_key':'rows',
            'columns_key':'columns','extracted_root':root,'record_count':111,'role':'source'},
        'manifest_xlsx_path':manifest,'artifact_paths':[manifest],
        'artifact_descriptors':[{'path':manifest,'role':'deliverable','extension':'.xlsx'}],
        'entries':[{'name':f'画像-{i}.png','path':root+'/'+str(i),'detail':'x'*300} for i in range(111)]}
    before=deepcopy(output)
    view=_compact_tool_output_for_model_context('archive_extract',output)
    assert view['source_inventory']==output['source_inventory']
    assert view['artifact_paths']==[manifest] and view['manifest_xlsx_path']==manifest
    assert view['deliverable_ready'] is True and 'rows_source' in view['inventory_guidance']
    assert len(json.dumps(view))<14000 and output==before


def test_focus_index_exposes_all_sources_beyond_preview_sample():
    results=[ToolResult(f'owned-{i}','browser_extract_text','completed',{'text':f'FACT-{i}'+'x'*1000}) for i in range(70)]
    metadata={r.invocation_id:{'tool_use_id':f'provider-{i}'} for i,r in enumerate(results)}
    messages=[{'role':'user','content':'作成してください'}]
    focused=_compact_focused_artifact_source_history(messages,state=state(),tool_results=results,model_tool_registry=registry())
    view,enabled=producer_context(focused,results,metadata,include_unmapped=True,include_hidden=True)
    packet=json.loads(view[-1]['content'][0]['text'].split('\n',1)[1])
    assert enabled and {r['invocation_id'] for r in packet['records']}=={r.invocation_id for r in results}
    assert inspect_retained({'requests':[{'invocation_id':'owned-37','json_pointer':'/text','offset':0,'length':7}]},results)['windows'][0]['content']=='FACT-37'


def test_delegated_fallback_exposes_omissions_without_cutting_json_or_sources():
    from nullion.deep_agent_runner import _tool_evidence_payload
    results=[ToolResult(f'owned-{i}','opaque_read','completed',{'body':'x'*4000+f'MIDDLE={i}'+'z'*4000,
        'url':'https://example.test/exact?scope='+str(i)}) for i in range(20)]
    before=deepcopy([r.output for r in results])
    view=_tool_evidence_payload(results,max_results=3,max_chars=800)
    assert view[0]['evidence_selection']=={'total_results':20,'shown_results':3,'omitted_results':17,'complete':False}
    for record in view[1:]:
        assert isinstance(record['output'],dict) and record['retained_evidence']['preview_only']
        assert record['retained_evidence']['omitted_paths'] and record['output']['url'].startswith('https://example.test/exact?scope=')
    assert before==[r.output for r in results]


def test_repeated_source_compaction_preserves_inspection_references_and_quotes():
    from nullion.source_observation_contract import compact_source_observations
    output={'result_kind':'source_observations','report_status':'partial', 'goal_status':'partial',
        'observations':[{'label':'Required fact','value':'73','quote':'Value 73','source_url':'https://example.test/a'}],
        'unknowns':['Remaining field'], 'captured_documents':[{'source_id':0,'source_url':'https://example.test/a',
            'main_document_text':'x'*30000}]}
    before=deepcopy(output)
    first=compact_source_observations(output,max_chars=3000)
    for _ in range(4):
        first=compact_source_observations(first,max_chars=3000)
        assert first['observations'][0]['value']=='73' and first['unknowns']==['Remaining field']
        assert first['captured_document_inspection']['json_pointer']=='/captured_documents'
        assert first['captured_document_index'][0]['json_pointer']=='/captured_documents/0'
    assert output==before


def test_select_catalogs_do_not_displace_later_enabled_actions():
    from nullion.source_observation_contract import compact_source_observations
    options=[{'label':str(index), 'value':str(index), 'selected':index==37} for index in range(250)]
    output={'result_kind':'source_observations','report_status':'partial',
        'observations':[{'label':'選択値','value':'37','quote':'37','source_url':'https://example.test',
            'control_state':{'tag':'SELECT','value':'37','disabled':False,'options':options,
                'text':' '.join(str(index) for index in range(250))}},
            {'label':'Acción disponible','value':'6:00 PM','quote':'Reserve for 2 at 6:00 PM',
                'source_url':'https://example.test','control_state':{'tag':'A','disabled':False,
                    'label':'Reserve for 2 at 6:00 PM'}}]}
    original=deepcopy(output)
    compact=compact_source_observations(output,max_chars=2000)
    for _ in range(3):
        assert len(compact['observations'])==2
        control=compact['observations'][0]['control_state']
        assert control['selected_options']==[options[37]] and control['value']=='37'
        assert control['option_count']==250
        assert control['options_inspection_pointer']=='/observations/0/control_state/options'
        assert compact['observations'][1]['control_state']==output['observations'][1]['control_state']
        compact=compact_source_observations(compact,max_chars=2000)
    assert output==original


def test_focused_compaction_preserves_visual_input_and_new_exact_inspection_batch():
    inspected=ToolResult('inspection','inspect_retained_evidence','completed',{'windows':[{'content':'EXACT=73'}]})
    context=state();context['invocation_metadata']={'inspection':{'tool_use_id':'use-inspect'}}
    visual={'type':'image','source':{'type':'base64','media_type':'image/png','data':'synthetic'}}
    messages=[{'role':'user','content':[visual]},
              {'role':'assistant','content':[{'type':'tool_use','id':'use-inspect','name':'inspect_retained_evidence','input':{}}]},
              {'role':'user','content':[{'type':'tool_result','tool_use_id':'use-inspect','content':[{'type':'text','text':json.dumps(inspected.output)}]}]}]
    view=_compact_focused_artifact_source_history(messages,state=context,tool_results=iter([inspected]),model_tool_registry=registry())
    assert visual in view[0]['content']
    assert any(b.get('type')=='tool_result' and 'EXACT=73' in json.dumps(b) for m in view for b in m.get('content',[]) if isinstance(b,dict))


@pytest.mark.parametrize('field',['task_id','approval_id','artifact_path','invocation_id','next_cursor','timezone'])
def test_opaque_context_handles_are_exact_or_explicitly_omitted(field):
    from nullion.workflow_reasoning import compact_evidence_value
    original={field:'opaque-'+'界'*600,'body':'x'*4000,'executed':False}
    compact,omitted=compact_evidence_value(original,max_chars=1200)
    assert compact[field]==original[field] and compact['executed'] is False
    assert omitted
    too_small,omitted=compact_evidence_value(original,max_chars=80)
    assert too_small is None and omitted==[[]]


@pytest.mark.parametrize('field',['artifact_paths','source_urls','task_ids'])
def test_handle_arrays_do_not_turn_shortened_strings_into_executable_values(field):
    from nullion.workflow_reasoning import compact_evidence_value
    value='opaque-'+'界'*600
    compact,omitted=compact_evidence_value({field:[value],'body':'x'*4000},max_chars=1200)
    assert compact[field]==[value] and omitted
    tiny,omitted=compact_evidence_value({field:[value],'body':'x'*4000},max_chars=80)
    assert omitted and (tiny is None or tiny[field]==[])


@pytest.mark.parametrize('forms,controls',[(0,0),(1,2)])
def test_large_page_projection_preserves_destination_control_facts(forms,controls):
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    output={'record':{'url':'https://source.test/record','details':'x'*30000},
        'source_record_collections':[{'path':['record'],'cardinality':'one','identity_fields':['url']}],
        'page_controls':{'form_count':forms,'editable_control_count':controls}}
    before=deepcopy(output)
    compact=_compact_tool_output_for_model_context('browser_extract_detail',output)
    assert compact['page_controls']==output['page_controls'] and output==before
    assert len(json.dumps(compact))<14000


def test_browser_snapshot_projection_keeps_destinations_and_control_state():
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    heading_links = [{"title": "商品", "url": "https://example.test/item"}]
    source = {'snapshot': {'url': 'https://example.test/start', 'element_count': 1,
        'heading_links': heading_links,
        'elements': [{'element_id': 'n-1', 'tag': 'a', 'role': 'link',
            'text': '連絡先', 'href': 'https://example.test/form'}]}}
    view = _compact_tool_output_for_model_context('browser_snapshot', source)
    assert view['url'] == 'https://example.test/start'
    assert view['elements'][0]['href'] == 'https://example.test/form'
    assert view['heading_links'] == heading_links
    assert view['captured_editable_control_count'] == 0
    source['snapshot']['elements'].append({'element_id': 'n-2', 'tag': 'input',
        'editable': True, 'name': 'mensaje', 'value': '', 'form_action': 'https://example.test/send'})
    view = _compact_tool_output_for_model_context('browser_snapshot', source)
    assert view['captured_editable_control_count'] == 1
    assert view['elements'][1]['form_action'] == 'https://example.test/send'


def test_navigation_projection_does_not_claim_loaded_controls_are_verified():
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    native = {'result_kind': 'browser_navigation', 'page_url': 'https://example.test/start',
        'session_id': 'owned', 'state_verification': {'navigation_completed': True, 'controls_verified': False},
        'next_step': 'Inspect actual controls'}
    assert _compact_tool_output_for_model_context('browser_navigate', native) == native
