from nullion.agent_orchestrator import (_CompletionReviewDecision, _completion_review_warning_allowed,
    _completion_review_original_request, _outcome_review_binding_context)
from nullion.tools import ToolResult
from nullion.workflow_reasoning import mark_review_unavailable, review_warning_reply, outcome_binding


def test_warning_is_rendered_once_from_metadata_without_exposing_provider_error():
    metadata = {}
    mark_review_unavailable(metadata, _CompletionReviewDecision('retry', review_error='private-provider-payload', review_timed_out=True))
    reply = review_warning_reply('Selected value: 7.', metadata)
    assert reply == 'Selected value: 7.'
    assert metadata['outcome_review_role'] == 'helper'
    assert 'private-provider-payload' not in reply
    assert review_warning_reply(reply, metadata) == reply
    assert 'outcome_review_warning_offset' not in metadata
    changed = review_warning_reply('Revised finding: 9.', metadata)
    assert changed == 'Revised finding: 9.'
    assert review_warning_reply(changed, metadata) == changed


def test_unavailable_review_never_overrides_hard_failures_or_known_bad_drafts():
    review = _CompletionReviewDecision('retry', review_error='invalid_coverage')
    state = {'user_message': '選択した値を確認。', 'principal_id': 'web:one', 'conversation_id': 'web:one',
        'tool_results': [ToolResult('read', 'opaque_read', 'completed', {'value': 7})]}
    assert _completion_review_warning_allowed(state, review, 'Selected value: 7.')
    assert not _completion_review_warning_allowed(state, review, 'Selected value: 7.', hard_failure=True)
    assert not _completion_review_warning_allowed(state, review, '')
    binding = outcome_binding(_completion_review_original_request(state), _outcome_review_binding_context(state),
        'Selected value: 8.', state['tool_results'], ())
    state['outcome_review_cache'] = {'_rejected_drafts': {binding: _CompletionReviewDecision('retry')}}
    assert not _completion_review_warning_allowed(state, review, 'Selected value: 8.')
    assert _completion_review_warning_allowed(state, review, 'Selected value: 7.')
    for status in ('failed', 'denied'):
        state['tool_results'] = [ToolResult('read', 'opaque_read', status, {'value': 7})]
        assert not _completion_review_warning_allowed(state, review, 'Selected value: 7.')
    state['tool_results'] = [ToolResult('scope', 'request_tool_scope', 'completed', {'value': 7})]
    assert not _completion_review_warning_allowed(state, review, 'Selected value: 7.')


def test_successful_review_is_not_reclassified_as_a_warning():
    state = {'tool_results': [ToolResult('read', 'opaque_read', 'completed', {'value': 7})]}
    assert not _completion_review_warning_allowed(state, _CompletionReviewDecision('complete'), 'Selected value: 7.')
