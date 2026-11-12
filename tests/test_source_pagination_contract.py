from dataclasses import asdict

import pytest

from nullion.tools import ToolResult
from nullion.workflow_reasoning import cursor_page_evidence, source_pagination_summary


def page(identity, cursor, following, *, scope='opaque-scope', tool='opaque_read', status='completed', count=1):
    return ToolResult(identity, tool, status, {'source_pagination': cursor_page_evidence(
        scope=scope, cursor=cursor, next_cursor=following, result_count=count)})


@pytest.mark.parametrize('scope', ['選択した範囲', 'النطاق المحدد'])
@pytest.mark.parametrize('tool', ['opaque_read', 'adapter_scan'])
def test_complete_page_chain_preserves_older_nonterminal_flags_without_repeating_pages(scope, tool):
    rows = [page('first', None, 'a', scope=scope, tool=tool),
            page('middle', 'a', 'b', scope=scope, tool=tool),
            page('last', 'b', None, scope=scope, tool=tool)]
    original = [asdict(row) for row in rows]
    result = source_pagination_summary(rows, references={'e0':'first','e1':'middle','e2':'last'})[0]
    assert result['complete'] is True and result['observed_pages'] == result['linked_pages'] == 3
    assert result['linked_result_count'] == 3 and not result['pending_after_invocation_ids']
    assert result['page_invocation_ids'] == ['e0', 'e1', 'e2']
    assert [asdict(row) for row in rows] == original
    assert source_pagination_summary([asdict(row) for row in rows])[0]['complete'] is True


@pytest.mark.parametrize('rows,reason', [
    ([page('first', None, 'a'), page('last', 'b', None)], 'missing_next_page'),
    ([page('last', 'b', None)], 'missing_initial_page'),
    ([page('first', None, 'a'), page('again', 'a', 'a')], 'cursor_cycle'),
    ([page('first', None, 'a'), page('changed', None, 'b')], 'conflicting_page_receipts'),
    ([page('first', None, None), page('unlinked', 'a', None)], 'unlinked_page_receipts'),
    ([page('first', None, 'a'), page('failed', 'a', None, status='failed')], 'missing_next_page'),
])
def test_terminal_page_or_failed_call_cannot_hide_missing_coverage(rows, reason):
    result = source_pagination_summary(rows)[0]
    assert result['complete'] is False and result['reason'] == reason
    if reason == 'missing_next_page': assert result['pending_after_invocation_ids'] == ['first']


def test_scopes_are_separate_and_repeated_same_page_does_not_inflate_coverage():
    rows = [page('one', None, 'a'), page('duplicate', None, 'a'), page('two', 'a', None),
            page('another', None, 'a', scope='another-scope')]
    result = source_pagination_summary(rows)
    assert result[0]['complete'] and result[0]['linked_result_count'] == 2
    assert result[0]['observed_pages'] == 2
    assert result[1]['complete'] is False
    assert all('cursor' not in row and 'next_cursor' not in row for row in result)


@pytest.mark.parametrize('value', [True, -1, '2'])
def test_adapter_page_count_requires_an_actual_nonnegative_integer(value):
    with pytest.raises(ValueError):
        cursor_page_evidence(scope='opaque', cursor=None, next_cursor=None, result_count=value)
