from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from nullion.tools import ToolResult


@pytest.mark.parametrize('label', ['候補', 'Opción', 'اختيار'])
def test_batch_discovery_and_observations_match_individual_reads(label):
    from nullion.search_coverage import discovery_ledger
    items = [{'title': label + str(i), 'url': f'https://source.test/title/{i}',
        'source_page_url': f'https://source.test/catalog/{i}'} for i in range(2)]
    batch = {'result_kind': 'browser_page_collection', 'items': items,
        'pages': [{'status': 'completed', 'read_kind': 'items', 'page_url': row['source_page_url']} for row in items]
            + [{'status': 'failed', 'read_kind': 'text', 'page_url': 'https://source.test/failed'}]}
    actual = discovery_ledger([ToolResult('batch', 'browser_read_pages', 'completed', batch)])
    separate = discovery_ledger([ToolResult(f'one-{i}', 'browser_extract_items', 'completed', {
        'result_kind': 'browser_item_collection', 'page_url': row['source_page_url'], 'items': [row]})
        for i, row in enumerate(items)])
    assert {r['candidate_id'] for r in actual['candidates']} == {r['candidate_id'] for r in separate['candidates']}
    assert {r['url'] for r in actual['observed_sources']} == {r['source_page_url'] for r in items}
    assert not any(r['observation_invocation_ids'] for r in actual['candidates'] if '/title/' in r['url'])
    assert not any('/failed' in r['url'] for r in actual['candidates'])
    assert actual['discovery_invocation_ids'] == ['batch']


def test_source_checkpoint_shares_receipts_losslessly_instead_of_repeating_them():
    from nullion.search_coverage import compact_context
    ids = ['orchestrator-' + str(i) * 32 for i in range(24)]
    context = {'truncated': True, 'candidates': [{'candidate_id': f's-{i}',
        'url': f'https://source.test/{i}?cursor=' + 'opaque' * 50, 'title': f'候補 {i}',
        'discovery_invocation_ids': ids, 'observation_invocation_ids': ids[:2]} for i in range(24)],
        'observed_sources': [{'url': 'https://source.test/current', 'observation_invocation_ids': ids}],
        'discovery_invocation_ids': ids}
    before = deepcopy(context)
    encoded = compact_context(context)
    decoded = json.loads(encoded)
    assert len(encoded) < len(json.dumps(context)) / 2
    for field in ('candidates', 'observed_sources'):
        for row in decoded[field]:
            for key in ('discovery_invocation_ids', 'observation_invocation_ids'):
                if key + '_ref' in row:
                    row[key] = decoded['receipt_sets'][row.pop(key + '_ref')]
    decoded.pop('receipt_sets'); decoded.pop('receipt_set_contract')
    assert decoded == context == before


@pytest.mark.parametrize('tools', [('browser_navigate',), ('browser_read_pages',),
    ('web_search',), ('web_fetch',), ('browser_navigate', 'web_search', 'web_fetch')])
def test_every_research_transport_gets_the_same_stopping_guidance(monkeypatch, tools):
    from nullion.web_research_policy import format_web_research_guidance
    from nullion.reasoning_guidance import RESEARCH_STOPPING_GUIDANCE
    registry = SimpleNamespace(list_specs=lambda: [SimpleNamespace(name=name) for name in tools])
    settings = SimpleNamespace(provider_bindings=[SimpleNamespace(capability='search_plugin', provider='brave_search_provider')])
    for api in (False, True):
        if api: monkeypatch.setenv('NULLION_BRAVE_SEARCH_API_KEY', 'test')
        else:
            monkeypatch.delenv('NULLION_BRAVE_SEARCH_API_KEY', raising=False)
            monkeypatch.delenv('BRAVE_SEARCH_API_KEY', raising=False)
        assert RESEARCH_STOPPING_GUIDANCE in format_web_research_guidance(tool_registry=registry, settings=settings)
    assert format_web_research_guidance(tool_registry=SimpleNamespace(list_specs=lambda: []), settings=settings) is None


def test_archiving_code_mode_keeps_the_typed_source_view_and_original_pointers():
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.code_mode import _default_observations
    from nullion.retained_work_context import producer_context, inspect_retained
    from tests.test_source_context_projection import source
    original = source()
    native = ToolResult('native-batch', 'browser_read_pages', 'completed', original)
    program = ToolResult('program', 'code_mode', 'completed', {'reason': 'code_mode_completed',
        'value': original, 'operations': [{'invocation_id': native.invocation_id,
            'tool_name': native.tool_name, 'status': 'completed'}],
        'native_observations': _default_observations([native], max_chars=8000)['observations']})
    before = deepcopy(program.output)
    fresh = _tool_result_message_payload(program)
    assert len(fresh) < 19000 and json.loads(fresh)['output']['value']['omitted'] is True
    messages = [{'role': 'assistant', 'content': [{'type': 'tool_use', 'id': 'use', 'name': 'code_mode', 'input': {}}]},
        {'role': 'user', 'content': [{'type': 'tool_result', 'tool_use_id': 'use', 'content': [{'type': 'text', 'text': fresh}]}]},
        {'role': 'assistant', 'content': [{'type': 'text', 'text': '次の判断'}]}]
    view, enabled = producer_context(messages, [native, program], {'program': {'tool_use_id': 'use'}})
    assert enabled and len(json.dumps(view)) < 23000
    assert program.output == before and native.output is original
    exact = inspect_retained({'requests': [{'invocation_id': 'native-batch', 'json_pointer': '/items/0/url',
        'offset': 0, 'length': 2000}]}, [native, program])
    assert exact['windows'][0]['content'] == original['items'][0]['url']


def test_failed_local_inspection_names_the_actual_original_keys_without_source_io():
    from nullion.retained_work_context import inspection_failure_output
    owned = ToolResult('owned', 'browser_read_pages', 'completed', {'items': [{'title': '秘密'}]})
    arguments = {'requests': [{'invocation_id': 'owned', 'json_pointer': '/collection_views/0/rows', 'offset': 0, 'length': 20},
        {'invocation_id': 'other-request', 'json_pointer': '', 'offset': 0, 'length': 20}]}
    output = inspection_failure_output(arguments, [owned])
    assert output['request_errors'][0]['available_keys'] == ['items']
    assert output['request_errors'][1]['owned_invocation_ids'] == ['owned']
    assert '秘密' not in json.dumps(output, ensure_ascii=False)
    assert output['recovery']['kind'] == 'correct_arguments' and output['executed'] is False


@pytest.mark.parametrize('operations', [[], [{'tool_name': 'inspect_retained_evidence', 'invocation_id': 'exact-window'}]])
def test_requested_exact_inspections_and_data_only_values_keep_their_existing_budget(operations):
    from nullion.code_mode import compact_output
    output = {'value': {'exact': 'α' * 24000}, 'operations': operations}
    assert compact_output(output, max_chars=80000) is output


@pytest.mark.parametrize('label,reply', [
    ('更新予定', '2026-11-21に45 USD。更新予定は登録済み。処理完了は未確認。'),
    ('Recordatorio de renovación', 'El aviso indica 45 USD el 2026-11-21; Recordatorio de renovación ya existe. Cargo sin confirmar.'),
    ('تذكير التجديد', 'القيمة 45 USD في 2026-11-21؛ تذكير التجديد موجود. تنفيذ الدفع غير مؤكد.'),
])
def test_mixed_source_paraphrases_are_not_replaced_by_the_first_tool_family(label, reply):
    from nullion.response_sanitizer import sanitize_user_visible_reply
    email = ToolResult('mail', 'email_read', 'completed', {'message': {
        'subject': 'Fixture billing notice 730', 'from': 'Fixture Billing <billing@source.test>',
        'body': 'Renewal is USD 45 on 2026-11-21.'}})
    calendar = ToolResult('calendar', 'calendar_list', 'completed', {'results': [
        {'summary': label, 'start': {'date': '2026-11-21'}}]})
    for rows in ([email, calendar], [calendar, email]):
        metadata = {}
        visible = sanitize_user_visible_reply(user_message='確認', reply=reply, tool_results=rows,
            presentation_metadata=metadata)
        assert reply in visible
        assert not metadata.get('fulfillment_satisfied') is False
