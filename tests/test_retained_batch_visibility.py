"""Exact newly requested evidence must reach the consumer before archiving."""
import json

from nullion.retained_work_context import INSPECT_TOOL, inspect_retained, producer_context
from nullion.tools import ToolResult
from nullion.workflow_reasoning import remember_review_inspections
from tests.test_retained_work_context import history


def add_batch(messages, results, metadata, additions):
    messages.append({'role': 'assistant', 'content': [
        {'type': 'tool_use', 'id': 'use-'+r.invocation_id, 'name': r.tool_name, 'input': {}}
        for r in additions]})
    messages.append({'role': 'user', 'content': [
        {'type': 'tool_result', 'tool_use_id': 'use-'+r.invocation_id, 'content': [
            {'type': 'text', 'text': json.dumps({'output': r.output, 'security': {'untrusted': True}})}]}
        for r in additions]})
    for r in additions:
        results.append(r); metadata[r.invocation_id] = {'tool_use_id': 'use-'+r.invocation_id}


def test_every_new_source_result_is_readable_before_it_is_archived():
    messages, results, metadata = history(30)
    batch = [ToolResult('new-'+str(i), 'opaque_read', 'completed',
        {'body': 'a'*5000+f'NEW_FACT_{i}'+'z'*5000}) for i in range(8)]
    add_batch(messages, results, metadata, batch)
    before = json.dumps(messages)
    view, enabled = producer_context(messages, results, metadata)
    assert enabled
    visible = json.dumps(view)
    assert all(f'NEW_FACT_{i}' in visible for i in range(8))
    assert 'EXACT=0' not in visible, 'Old history should still be compacted'
    assert json.dumps(messages) == before


def test_every_inspection_in_one_batch_keeps_its_exact_window():
    messages, results, metadata = history(30)
    batch = []
    for i in range(8):
        output = inspect_retained({'requests': [{'invocation_id': f'owned-{i}',
            'json_pointer': '/body', 'offset': 4500, 'length': 1200}]}, results)
        batch.append(ToolResult('inspection-'+str(i), INSPECT_TOOL, 'completed', output))
    add_batch(messages, results, metadata, batch)
    view, enabled = producer_context(messages, results, metadata)
    assert enabled
    actual = {b['tool_use_id']: json.loads(b['content'][0]['text'])['output']
        for m in view if isinstance(m.get('content'), list) for b in m['content']
        if b.get('type') == 'tool_result'}
    for result in batch:
        assert actual['use-'+result.invocation_id] == result.output
        assert result.output['windows'][0]['shown_chars'] == len(result.output['windows'][0]['content'])


def test_inspected_facts_survive_a_later_tool_batch_with_owned_source_validation():
    messages, results, metadata = history(30)
    cache = {}
    windows = []
    for i in range(8):
        windows += inspect_retained({'requests': [{'invocation_id': f'owned-{i}',
            'json_pointer': '/body', 'offset': 5000, 'length': len(f'EXACT={i}')}]}, results)['windows']
    remember_review_inspections(cache, windows, results=results, references={})
    add_batch(messages, results, metadata, [ToolResult('later', 'opaque_check', 'completed', {'checked': True})])
    view, _ = producer_context(messages, results, metadata, inspection_cache=cache)
    packet = json.loads(view[-1]['content'][0]['text'].split('\n', 1)[1])
    assert {w['content'] for w in packet['previously_inspected_evidence']} == {f'EXACT={i}' for i in range(8)}
    assert packet['inspection_security']['untrusted_tool_output'] is True
    # A descriptor from another request or a changed source cannot inject facts.
    changed = [ToolResult(r.invocation_id, r.tool_name, r.status, {'body': 'changed'}) for r in results]
    other_view, _ = producer_context(messages, changed, metadata, inspection_cache=cache)
    other_packet = json.loads(other_view[-1]['content'][0]['text'].split('\n', 1)[1])
    assert not other_packet.get('previously_inspected_evidence')


def test_fresh_inspection_windows_are_not_repeated_in_the_retained_packet():
    messages, results, metadata = history(20)
    output = inspect_retained({'requests': [{'invocation_id': 'owned-0', 'json_pointer': '/body',
        'offset': 5000, 'length': 7}]}, results)
    cache = {}
    remember_review_inspections(cache, output['windows'], results=results, references={})
    add_batch(messages, results, metadata, [ToolResult('fresh-inspection', INSPECT_TOOL, 'completed', output)])
    before = json.dumps(cache)
    view, _ = producer_context(messages, results, metadata, inspection_cache=cache)
    packet = json.loads(view[-1]['content'][0]['text'].split('\n', 1)[1])
    assert not packet.get('previously_inspected_evidence')
    assert 'EXACT=0' in json.dumps(view) and json.dumps(cache) == before


def test_archived_history_and_retained_windows_share_the_model_view_budget():
    messages, results, metadata = history(40)
    cache = {}
    for i in range(8):
        output = inspect_retained({'requests': [{'invocation_id': f'owned-{i}', 'json_pointer': '/body',
            'offset': 0, 'length': 10000}]}, results)
        remember_review_inspections(cache, output['windows'], results=results, references={})
    add_batch(messages, results, metadata, [ToolResult('latest', 'opaque_check', 'completed', {'ok': True})])
    view, _ = producer_context(messages, results, metadata, inspection_cache=cache)
    assert len(json.dumps(view)) < 100000
    # The original exact data is still inspectable after the bounded view.
    tail = inspect_retained({'requests': [{'invocation_id': 'owned-39', 'json_pointer': '/body',
        'offset': 5000, 'length': 8}]}, results)
    assert tail['windows'][0]['content'] == 'EXACT=39'
