import json

from nullion import code_mode
from nullion.tools import ToolResult
from tests.test_code_mode import registry, execute


def program(state, code, names, identity='program', executor=execute):
    code_mode.offer_code_mode(state['tool_registry'].list_tool_definitions(), state['tool_registry'], state=state)
    return code_mode.execute_if_selected(state, [{'type':'tool_use','name':'code_mode','id':identity,
        'input':{'tool_names':names,'code':code}}], execute=executor)


def test_resource_limits_are_configurable_operator_values_with_safe_defaults(monkeypatch):
    monkeypatch.delenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES',raising=False)
    monkeypatch.delenv('NULLION_CODE_MODE_MAX_MEMORY_BYTES',raising=False)
    assert code_mode.resource_limits()=={'transfer_bytes':4_000_000,'memory_bytes':64_000_000}
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES','8000000')
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_MEMORY_BYTES','128000000')
    assert code_mode.resource_limits()=={'transfer_bytes':8_000_000,'memory_bytes':128_000_000}
    for value in ('invalid','0','-1','9999999999999999'):
        monkeypatch.setenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES',value)
        monkeypatch.setenv('NULLION_CODE_MODE_MAX_MEMORY_BYTES',value)
        assert code_mode.resource_limits()=={'transfer_bytes':4_000_000,'memory_bytes':64_000_000}


def test_real_interpreter_accepts_structured_payload_over_old_limit(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    state={'tool_registry':registry(),'messages':[]}
    records=[{'id':f'item-{i:08d}','label':f'Example item {i:08d}',
        'url':f'https://example.invalid/item/{i:08d}','value':i} for i in range(28_571)]
    calls=[]
    def native(current, blocks):
        calls.append(blocks[0]['name'])
        return {'tool_results':[*current.get('tool_results',[]),
            ToolResult(blocks[0]['id'],'read','completed',{'records':records})]}
    result=program(state,"saved=call_tool('read', {})\nlen(saved['output']['records'])",['read'],executor=native)
    out=result['tool_results'][-1]
    assert out.status=='completed' and out.output['value']==28_571,json.dumps({
        k:out.output.get(k) for k in ('reason','execution_error','value_limit')})
    assert 3_000_000<out.output['transfer_usage']['transferred_bytes']<4_000_000
    assert calls==['read']
    continued=program(result,"len(saved['output']['records'])",[],identity='large-checkpoint-continuation',executor=native)
    assert continued['tool_results'][-1].output['value']==28_571
    assert calls==['read']


def test_large_captures_are_projected_before_vm_without_changing_native_receipt(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    original={'result_kind':'source_observations','report_status':'completed','goal_status':'answered',
        'page_url':'https://source.test','observations':[{'label':'Hotel','value':'Casa 37, $86',
            'quote':'Casa 37, $86','source_url':'https://source.test'}],
        'captured_documents':[{'source_url':'https://source.test','main_document_text':'x'*5_000_000}]}
    def native(current,blocks):
        return {'tool_results':[*current.get('tool_results',[]),ToolResult(blocks[0]['id'],'read','completed',original)]}
    result=program({'tool_registry':registry(),'messages':[]},
        "saved=call_tool('read', {})\n{'partial':saved['output_projection']['partial'],'value':saved['output']['observations'][0]['value']}",
        ['read'],executor=native)
    out=result['tool_results'][-1]
    assert out.status=='completed' and out.output['value']=={'partial':True,'value':'Casa 37, $86'},out.output
    assert result['tool_results'][0].output is original
    assert len(original['captured_documents'][0]['main_document_text'])==5_000_000
    assert out.output['transfer_usage']['transferred_bytes']<64_000


def test_existing_session_keeps_its_limits_when_operator_configuration_changes(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES','2000000')
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_MEMORY_BYTES','64000000')
    first=program({'tool_registry':registry(),'messages':[]},"saved=37\nsaved",[])
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES','8000000')
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_MEMORY_BYTES','128000000')
    second=program(first,'saved+1',[],identity='continue')
    assert second['tool_results'][-1].output['value']==38
    assert second['tool_results'][-1].output['transfer_usage']['limit_bytes']==2_000_000


def test_oversized_uncertain_write_still_stops_and_cannot_be_replayed(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES','64000')
    calls=[]
    def uncertain(current,blocks):
        calls.append(blocks[0]['name'])
        return {'tool_results':[*current.get('tool_results',[]),ToolResult(blocks[0]['id'],'write','failed',
            {'body':'x'*100000,'reason':'acknowledgement_unknown'})]}
    first=program({'tool_registry':registry(),'messages':[]},"call_tool('write', {})\ncall_tool('read', {})",
        ['write','read'],executor=uncertain)
    assert first['tool_results'][-1].output['reason']=='code_mode_operation_stopped'
    second=program(first,"call_tool('write', {})",['write'],identity='retry',executor=uncertain)
    assert second['tool_results'][-1].output['reason']=='code_mode_unknown_write_outcome'
    assert calls==['write']


def test_overflow_keeps_all_completed_source_views_and_saved_values_without_replay(monkeypatch):
    from nullion.agent_orchestrator import _tool_result_message_payload
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES','64000')
    calls=[]
    def native(current,blocks):
        b=blocks[0];kind=b['input']['kind'];calls.append(kind)
        if kind=='opaque':
            output={'body':'x'*100000,'records':[{'name':'verified-73','url':'https://source.test/73'}],
                'source_record_collections':[{'path':['records'],'identity_fields':['url'],'context_fields':['name']}]}
        else:
            output={'result_kind':'source_observations','report_status':'completed','goal_status':'answered',
                'observations':[{'label':kind,'value':kind+'-37','quote':kind+'-37','source_url':'https://source.test/'+kind}]}
        return {'tool_results':[*current.get('tool_results',[]),ToolResult(b['id'],'read','completed',output)]}
    code="f=call_tool('read', {'kind':'flight'})\nh=call_tool('read', {'kind':'hotel'})\nr=call_tool('read', {'kind':'opaque'})\ncall_tool('write', {})"
    result=program({'tool_registry':registry(),'messages':[]},code,['read','write'],executor=native)
    out=result['tool_results'][-1]
    assert out.status=='failed' and out.output['reason']=='code_mode_input_limit'
    assert out.output['transfer_overflow']['native_status']=='completed'
    assert calls==['flight','hotel','opaque']
    wire=json.loads(_tool_result_message_payload(out))['output']
    assert len(wire['native_observations'])==3,wire
    assert all(value in json.dumps(wire) for value in ('flight-37','hotel-37','verified-73'))
    followup=program(result,"[f['output']['observations'][0]['value'],h['output']['observations'][0]['value']]",[],
        identity='data-only',executor=native)
    assert followup['tool_results'][-1].output['value']==['flight-37','hotel-37']
    assert calls==['flight','hotel','opaque']


def test_browser_snapshot_stays_complete_in_python_and_typed_in_model_view():
    from copy import deepcopy
    url='https://store.test/item?record='+('opaque-'*700)
    rows=[{'element_id': str(i), 'tag': 'a', 'href': url+str(i), 'text': '商品',
           'rect': {'x':1,'y':2,'height':3,'width':4}, 'unused':'z'*700} for i in range(180)]
    original={'snapshot': {'element_count': 180, 'elements': rows}, 'session_id': 'exact-session'}
    result=ToolResult('large-controls', 'browser_snapshot', 'completed', original)
    before=deepcopy(original)
    payload,_=code_mode._bounded_native_payload(result, remaining=4_000_000, per_call=2_000_000)
    assert payload['output']==before and 'output_projection' not in payload
    view=code_mode._default_observations([result], max_chars=48_000)['observations'][0]
    snapshot=view['output']['snapshot']
    assert 0<len(snapshot['elements'])<80
    assert snapshot['elements_truncated']['shown']==len(snapshot['elements'])
    assert all(row['href']==url+row['element_id'] for row in snapshot['elements'])
    assert len(json.dumps(view,ensure_ascii=False))<48_000
    assert view['inspection_reference']['invocation_id']=='large-controls'
    assert result.output==before


def test_model_snapshot_index_does_not_fall_back_to_cut_json():
    result=ToolResult('native-snapshot','browser_snapshot','completed',{'session_id':'owned-session',
        'snapshot':{'element_count':180,'elements':[{'element_id':str(i),'tag':'a','role':'link',
            'text':f'品目 {i}','href':f'https://store.test/item/{i}?opaque='+('x'*350)} for i in range(180)]}})
    output={'value':result.output,'operations':[{'tool_name':result.tool_name,'invocation_id':result.invocation_id}],
        'native_observations':code_mode._default_observations([result])['observations']}
    view=code_mode.compact_output(output, max_chars=64_000)
    snapshots=[row['output']['snapshot'] for row in view['native_observations'] if 'snapshot' in row['output']]
    assert snapshots and snapshots[0]['elements'][0]['href']==result.output['snapshot']['elements'][0]['href']
    assert snapshots[0]['captured_element_count']==180
    assert 'preview' not in str(view.keys())
    assert len(json.dumps(view,ensure_ascii=False))<48_000


def test_snapshot_link_budget_keeps_complete_destinations_with_other_observations():
    links = [{"title":f"商品 {index}", "url":f"https://store.test/item/{index}?reference=" + "x" * 810}
             for index in range(6)]
    result = ToolResult('snapshot', 'browser_snapshot', 'completed', {'snapshot':{'heading_links':links,
        'elements':[{'tag':'input', 'editable':True, 'element_id':'n-1'}]}})
    operations = [ToolResult('open', 'browser_open', 'completed', {}),
                  ToolResult('navigate', 'browser_navigate', 'completed', {}), result]
    observations = code_mode._default_observations(operations, max_chars=16_000)['observations']
    snapshot = observations[-1]['output']['snapshot']
    assert snapshot['heading_links'][:2] == links[:2]
    assert snapshot['heading_links_truncated']['shown'] == len(snapshot['heading_links'])
    assert snapshot['heading_links_truncated']['total'] == 6
    assert snapshot['elements'][0]['element_id'] == 'n-1'
    assert len(result.output['snapshot']['heading_links']) == 6


def test_large_detail_batch_keeps_visible_prices_and_variants_in_model_view():
    details = []
    for index, fact in enumerate(('Paquete de 8, $17', '8 個入り、$21')):
        details.append({'source_page_url':f'https://store.test/item/{index}?id=' + 'x' * 780,
            'title':f'商品 {index}', 'observed_text':('x' * 600) + fact + ('z' * 800),
            'record':{'long_audit':'a' * 16000}})
    output = {'result_kind':'browser_page_collection', 'details':details,
              'pages':[{'status':'completed'}, {'status':'completed'}], 'completed_pages':2}
    native = ToolResult('details', 'browser_read_pages', 'completed', output)
    result = {'value':output, 'operations':[{'tool_name':native.tool_name, 'invocation_id':native.invocation_id}],
        'native_observations':code_mode._default_observations([native])['observations']}
    wire = code_mode.compact_output(result, max_chars=8_000)
    evidence = wire['native_observations'][0]['output']['detail_evidence']
    for source, row in zip(evidence, details, strict=True):
        assert source['source_url'] == row['source_page_url']
        assert ''.join(source['visible_text_segments']) == row['observed_text']
    assert len(json.dumps(wire, ensure_ascii=False)) < 8_000
