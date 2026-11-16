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
