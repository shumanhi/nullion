"""Retained windows are bounded descriptors, never cached proof or verdicts."""
from dataclasses import replace

import pytest

from nullion import workflow_reasoning as reasoning
from nullion.tools import ToolResult


def retain(result=None, requests=None):
    result = result or ToolResult('owned', 'opaque_read', 'completed', {'body': 'Selected value: 7.'})
    requests = requests or [{'invocation_id': 'old', 'json_pointer': '/body'}]
    windows = reasoning.inspect_review_evidence(requests, results=[result], references={'old': result.invocation_id}, visible_ids={'old'})
    cache = {}
    reasoning.remember_review_inspections(cache, windows, results=[result], references={'old': result.invocation_id})
    return cache, result


def test_descriptors_rebuild_owned_source_and_rebind_current_visible_alias():
    cache, result = retain()
    entry = cache['retained_inspection_windows'][0]
    assert set(entry) == {'invocation_id', 'json_pointer', 'offset', 'length', 'source_fingerprint'}
    assert 'Selected value' not in str(cache)
    # Even forged copied text/approval fields cannot become evidence.
    entry.update(content='Selected value: 999.', disposition='complete')
    rows = reasoning.retained_review_inspections(cache, results=[result], references={'new': 'owned', 'owned': 'owned'})
    assert rows[0]['content'] == 'Selected value: 7.'
    assert rows[0]['invocation_id'] == 'new'
    assert 'disposition' not in rows[0]
    reasoning.remember_review_inspections(cache, [], results=[result], references={})
    assert 'content' not in cache['retained_inspection_windows'][0]


@pytest.mark.parametrize('change', [
    {'output': {'body': 'Selected value: 8.'}}, {'status': 'failed'},
    {'error': 'Source changed'}, {'tool_name': 'other_read'}, {'invocation_id': 'other-request'},
])
def test_changed_source_cannot_reuse_a_window(change):
    cache, result = retain()
    assert reasoning.retained_review_inspections(cache, results=[replace(result, **change)]) == []


def test_missing_or_unexposed_result_cannot_reuse_a_window():
    cache, result = retain()
    assert reasoning.retained_review_inspections(cache, results=[]) == []
    assert reasoning.retained_review_inspections(cache, results=[result], references={'foreign': 'elsewhere'}) == []


@pytest.mark.parametrize('entry', [None, 'invalid', {}, {'offset': -1},
    {'invocation_id': 'owned', 'json_pointer': '/body', 'offset': True, 'length': 4, 'source_fingerprint': 'x'}])
def test_corrupt_cached_descriptors_are_discarded(entry):
    _, result = retain()
    assert reasoning.retained_review_inspections({'retained_inspection_windows': [entry]}, results=[result]) == []


@pytest.mark.parametrize('pointer,offset', [('/missing', 0), ('/body', 999)])
def test_corrupt_pointer_or_offset_cannot_interrupt_review(pointer, offset):
    cache, result = retain()
    cache['retained_inspection_windows'][0].update(json_pointer=pointer, offset=offset)
    assert reasoning.retained_review_inspections(cache, results=[result]) == []


def test_unchanged_sources_are_hashed_once_per_source_not_per_window(monkeypatch):
    result = ToolResult('owned', 'opaque_read', 'completed', {'body': 'x' * 1000})
    requests = [{'invocation_id': 'old', 'json_pointer': '/body', 'offset': i, 'length': 1} for i in range(128)]
    cache, result = retain(result, requests)
    original = reasoning._inspection_result_fingerprint
    hashes = []
    def fingerprint(row):
        hashes.append(row.invocation_id)
        return original(row)
    monkeypatch.setattr(reasoning, '_inspection_result_fingerprint', fingerprint)
    windows = reasoning.retained_review_inspections(cache, results=[result])
    assert len(windows) == 128 and hashes == ['owned']
    hashes.clear()
    reasoning.remember_review_inspections(cache, windows, results=[result], references={})
    assert hashes == ['owned'] and len(cache['retained_inspection_windows']) == 128


def test_retained_windows_have_total_budget_and_new_inspection_keeps_original_budget():
    result = ToolResult('owned', 'opaque_read', 'completed', {'body': 'x' * 100000})
    cache = {}
    # Separate reviews can inspect different portions; their combined carryover is bounded.
    for position in range(0, 72000, 12000):
        rows = reasoning.inspect_review_evidence([{'invocation_id': 'owned', 'json_pointer': '/body',
            'offset': position, 'length': 12000}], results=[result], references={}, visible_ids={'owned'})
        reasoning.remember_review_inspections(cache, rows, results=[result], references={})
    assert sum(row['shown_chars'] for row in reasoning.retained_review_inspections(cache, results=[result])) == 48000
    rows = reasoning.inspect_review_evidence([{'invocation_id': 'owned', 'json_pointer': '/body',
        'offset': i * 12000} for i in range(4)], results=[result], references={}, visible_ids={'owned'})
    assert sum(row['shown_chars'] for row in rows) == 24000


def test_empty_cache_stays_on_cheap_path_and_never_hashes_evidence(monkeypatch):
    monkeypatch.setattr(reasoning, '_inspection_result_fingerprint', lambda _: pytest.fail('Unneeded source hash'))
    assert reasoning.retained_review_inspections({}, results=[ToolResult('x', 'read', 'completed', {'body': 'x'})]) == []


def test_carryover_redacts_current_source_and_never_stores_secret_text():
    secret = 'sk-' + 'a' * 32
    cache, result = retain(ToolResult('owned', 'opaque_read', 'completed', {'body': 'API token: ' + secret}))
    rows = reasoning.retained_review_inspections(cache, results=[result])
    assert secret not in str(cache) and secret not in str(rows)
    assert '[redacted]' in rows[0]['content']


def test_descriptor_inventory_is_capped_and_latest_inspection_replaces_duplicate():
    result = ToolResult('owned', 'opaque_read', 'completed', {'body': 'x' * 1000})
    cache = {}
    for offset in range(130):
        windows = reasoning.inspect_review_evidence([{'invocation_id': 'owned', 'json_pointer': '/body',
            'offset': offset, 'length': 1}], results=[result], references={}, visible_ids={'owned'})
        reasoning.remember_review_inspections(cache, windows, results=[result], references={})
    assert len(cache['retained_inspection_windows']) == 128
    assert cache['retained_inspection_windows'][0]['offset'] == 129
    windows = reasoning.inspect_review_evidence([{'invocation_id': 'owned', 'json_pointer': '/body',
        'offset': 129, 'length': 2}], results=[result], references={}, visible_ids={'owned'})
    reasoning.remember_review_inspections(cache, windows, results=[result], references={})
    assert len(cache['retained_inspection_windows']) == 128
    assert cache['retained_inspection_windows'][0]['length'] == 2


def test_corrupt_cache_inventory_can_be_replaced_by_a_new_valid_inspection():
    cache, result = retain()
    windows = reasoning.retained_review_inspections(cache, results=[result])
    cache['retained_inspection_windows'] = 23
    assert reasoning.retained_review_inspections(cache, results=[result]) == []
    reasoning.remember_review_inspections(cache, windows, results=[result], references={})
    assert reasoning.retained_review_inspections(cache, results=[result])[0]['content'] == 'Selected value: 7.'


def test_smaller_carryover_budget_keeps_every_source_window_and_exact_scope():
    results = [ToolResult(str(i), 'opaque_read', 'completed',
        {'body': str(i) + ':' + 'x' * 5000}) for i in range(12)]
    cache = {}
    for result in results:
        windows = reasoning.inspect_review_evidence([{'invocation_id': result.invocation_id,
            'json_pointer': '/body', 'offset': 0, 'length': 4000}],
            results=[result], references={}, visible_ids={result.invocation_id})
        reasoning.remember_review_inspections(cache, windows, results=results, references={})
    before = str(cache)
    windows = reasoning.retained_review_inspections(cache, results=results, max_chars=9000)
    assert {row['invocation_id'] for row in windows} == {row.invocation_id for row in results}
    assert sum(row['shown_chars'] for row in windows) == 9000
    for window in windows:
        assert window['content'] == next(row.output['body'] for row in results
            if row.invocation_id == window['invocation_id'])[:750]
        assert window['complete'] is False and window['total_chars'] > window['shown_chars']
    assert str(cache) == before


def test_short_windows_leave_their_unused_share_for_longer_sources():
    results = [ToolResult(str(i), 'opaque_read', 'completed',
        {'body': 'x' * (5000 if i < 3 else 20)}) for i in range(12)]
    cache = {}
    for result in results:
        windows = reasoning.inspect_review_evidence([{'invocation_id': result.invocation_id,
            'json_pointer': '/body', 'length': 4000 if len(result.output['body']) > 20 else 20}],
            results=[result], references={}, visible_ids={result.invocation_id})
        reasoning.remember_review_inspections(cache, windows, results=results, references={})
    windows = reasoning.retained_review_inspections(cache, results=results, max_chars=9000)
    assert len(windows) == 12 and sum(row['shown_chars'] for row in windows) == 9000
    assert all(row['shown_chars'] == 20 and row['complete'] is True
               for row in windows if int(row['invocation_id']) >= 3)
    assert all(row['shown_chars'] >= 2940 for row in windows if int(row['invocation_id']) < 3)
