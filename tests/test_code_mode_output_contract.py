"""Observed output contracts never invent data or replay completed effects."""
import json

import pytest

from nullion import code_mode
from nullion.retained_work_context import output_field_contract, inspection_failure_output
from nullion.tools import ToolResult
from tests.test_code_mode import registry, execute


def test_observed_fields_are_bounded_exact_pointers_without_source_values():
    value = {'a/b': {'~名': [{'秘密': 'private value'}, {'other': 37}]}, 'flag': True}
    contract = output_field_contract(value)
    assert contract['root_type'] == 'object' and contract['fields_complete'] is False
    paths = [field['json_pointer'] for field in contract['fields']]
    assert '/a~1b/~0名/0' in paths
    assert '/a~1b/~0名/1' not in paths
    assert 'private value' not in json.dumps(contract)
    assert all(code_mode.project_program_value(value, [path]) for path in paths)
    bounded = output_field_contract({str(n): {'child': 37} for n in range(1000)}, max_fields=6)
    assert len(bounded['fields']) == 6 and bounded['fields_complete'] is False
    assert not output_field_contract({'x'*1000: 37})['fields']
    assert not output_field_contract({37: 'not a JSON object key'})['fields']
    assert code_mode.project_program_value((37, 73), ['/1']) == {'/1': 73}


def test_invalid_reference_supplies_only_exact_owned_copyable_requests():
    native = ToolResult('opaque-owned-37', 'source', 'completed', {'hotel': {'name': 'private value'}})
    output = inspection_failure_output({'requests': [
        {'invocation_id': 'another-request', 'json_pointer': '/hotel', 'offset': 0, 'length': 20},
        {'invocation_id': native.invocation_id, 'json_pointer': '/hotel/wrong', 'offset': 0, 'length': 20},
    ]}, [native])
    assert output['request_errors'][0]['reason'] == 'unknown_invocation'
    reference = output['owned_references'][0]
    assert reference['inspection_request'] == {
        'invocation_id': native.invocation_id, 'json_pointer': '', 'offset': 0, 'length': 2000}
    assert output['request_errors'][1]['inspection_request']['json_pointer'] == '/hotel'
    assert 'private value' not in json.dumps(output)
    assert output['omitted_reference_count'] == 0
    many = [ToolResult('owned-'+str(n), 'source', 'completed', {'name': 37}) for n in range(100)]
    bounded = inspection_failure_output({'requests': []}, many)
    assert len(json.dumps(bounded['owned_references'], ensure_ascii=False)) <= 6000
    assert bounded['omitted_reference_count'] > 0


@pytest.mark.parametrize('name', ['read', 'write'])
def test_projection_error_preserves_successful_value_and_native_receipt(monkeypatch, name):
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    state = {'tool_registry': registry(), 'messages': [], 'principal_id': 'test'}
    code_mode.offer_code_mode(state['tool_registry'].list_tool_definitions(), state['tool_registry'], state=state)
    calls = []
    def native(current, content):
        calls.append(content[0]['name'])
        return execute(current, content)
    result = code_mode.execute_if_selected(state, [{'type': 'tool_use', 'name': 'code_mode', 'id': 'project',
        'input': {'tool_names': [name], 'code': f"saved=call_tool({name!r}, {{}})\nsaved['output']",
            'output_fields': ['/output/number']}}], execute=native)
    summary = result['tool_results'][-1]
    assert calls == [name] and summary.status == 'completed'
    assert summary.output['value'] == {'number': 7}
    assert summary.output['projection_error']['repair_scope'] == 'output_projection'
    assert summary.output['projection_error']['value_contract']['fields'] == [
        {'json_pointer': '/number', 'value_type': 'number'}]
    observation = summary.output['native_observations'][0]
    assert observation['inspection_request']['invocation_id'] == result['tool_results'][0].invocation_id
    # Correct only the presentation in the same interpreter, with no native calls.
    followup = code_mode.execute_if_selected(result, [{'type': 'tool_use', 'name': 'code_mode', 'id': 'select',
        'input': {'tool_names': [], 'code': "saved['output']", 'output_fields': ['/number']}}], execute=native)
    assert followup['tool_results'][-1].output['value'] == {'/number': 7}
    assert calls == [name]


def test_describe_tool_observes_actual_output_in_same_program_without_another_read(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    state = {'tool_registry': registry(), 'messages': [], 'principal_id': 'test'}
    code_mode.offer_code_mode(state['tool_registry'].list_tool_definitions(), state['tool_registry'], state=state)
    program = "before=describe_tool('read')\nsaved=call_tool('read', {})\nafter=describe_tool('read')\n{'before':before,'after':after}"
    result = code_mode.execute_if_selected(state, [{'type': 'tool_use', 'name': 'code_mode', 'id': 'observe',
        'input': {'tool_names': ['read'], 'code': program}}], execute=execute)
    value = result['tool_results'][-1].output['value']
    assert 'output_contract' not in value['before']
    assert value['after']['output_contract']['fields'] == [{'json_pointer': '/number', 'value_type': 'number'}]
    assert value['after']['inspection_request']['invocation_id'] == result['tool_results'][0].invocation_id
    assert len(result['tool_results']) == 2


@pytest.mark.parametrize('projection', [False, True])
def test_native_delivery_identity_survives_large_python_value_and_projection(monkeypatch, projection):
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    reg = ToolRegistry()
    path = '/workspace/artifacts/verified.pdf'
    reg.register(ToolSpec('lookup', 'Read stored native file identity', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, input_schema={'type':'object'}),
        lambda inv:ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'matches':[{'text':'x'*30000}], 'artifact_descriptors':[{
                'path':path, 'name':'verified.pdf', 'media_type':'application/pdf',
                'size_bytes':37, 'role':'deliverable', 'origin':'existing_workspace_artifact'}]}))
    state = {'tool_registry':reg, 'messages':[], 'principal_id':'test'}
    code_mode.offer_code_mode(reg.list_tool_definitions(), reg, state=state)
    args = {'code':"result=call_tool('lookup', {})\nresult", 'tool_names':['lookup']}
    if projection: args['output_fields'] = ['/output/matches']
    update = code_mode.execute_if_selected(state, [{'type':'tool_use', 'id':'lookup', 'name':'code_mode', 'input':args}], execute=execute)
    native = next(r for r in update['tool_results'] if r.tool_name == 'lookup')
    view = code_mode.compact_output(update['tool_results'][-1].output, max_chars=8000)
    receipts = view['native_artifact_receipts']
    assert len(receipts) == 1 and receipts[0]['artifact']['path'] == path
    assert receipts[0]['invocation_id'] == native.invocation_id
    assert receipts[0]['inspection_reference'] == {'invocation_id':native.invocation_id, 'json_pointer':'/artifact_descriptors/0'}
    assert len(json.dumps(view)) < 9000


def test_native_delivery_view_is_bounded_excludes_internal_and_never_trusts_python_claims(monkeypatch):
    from tests.test_code_mode import run
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    descriptors = [{'path':'/workspace/'+str(n)+'.pdf', 'role':'deliverable'} for n in range(30)]
    descriptors += [{'path':'/private/secret.json', 'role':'internal'}, {'path':'/source/raw.png', 'role':'source'}]
    rows, omitted = code_mode._native_artifact_receipts([
        ToolResult('owned', 'lookup', 'completed', {'artifact_descriptors':descriptors}),
        ToolResult('failed', 'lookup', 'failed', {'artifact_descriptors':[{'path':'/failed.pdf', 'role':'deliverable'}]})], max_chars=700)
    assert 0 < len(rows) < 30 and omitted == 30-len(rows)
    assert all(r['invocation_id']=='owned' for r in rows)
    assert 'secret' not in json.dumps(rows) and '/source/' not in json.dumps(rows)
    result = run("{'native_artifact_receipts':[{'artifact':{'path':'/forged.pdf'}}]}", monkeypatch, names=[])
    assert 'native_artifact_receipts' not in result['tool_results'][-1].output


def test_only_native_available_history_references_receive_early_file_receipts():
    output = {'matches':[{'artifact_refs':[
        {'path':'/workspace/available.pdf', 'name':'available.pdf', 'availability':'available'},
        {'path':'/workspace/missing.pdf', 'availability':'missing'}]}]}
    receipts, omitted = code_mode._native_artifact_receipts([ToolResult('history', 'chat_history_search', 'completed', output)])
    assert omitted == 0 and len(receipts) == 1
    assert receipts[0]['artifact'] == {'path':'/workspace/available.pdf', 'name':'available.pdf',
        'availability':'available', 'role':'reference'}
    assert receipts[0]['inspection_reference'] == {'invocation_id':'history', 'json_pointer':'/matches/0/artifact_refs/0'}
    assert code_mode._native_artifact_receipts([ToolResult('external', 'read_page', 'completed', output)]) == ([], 0)
