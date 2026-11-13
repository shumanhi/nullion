"""Retire AI quality-review authority while retaining native execution facts."""
from collections.abc import Mapping
from dataclasses import replace


_RETIRED_STATE = {
    'outcome_review_cache', 'action_review_cache', 'completion_requirement_contract_cache',
    'completion_review_unresolved_requirements', 'completion_recovery_iteration_limit',
    'completion_review_progress_fingerprints', 'completion_review_recovery_tool_names',
    'completion_recovery_tool_names', 'completion_recovery_progress_signatures',
    'action_review_stop', 'outcome_assistance_producer', 'outcome_assistance_restored',
    'source_answer_repair_context',
    'source_answer_semantically_rejected', 'source_answer_semantics_required',
}


def _presentation(value):
    return {key: item for key, item in (value.items() if isinstance(value, Mapping) else ())
        if isinstance(key, str) and not key.startswith(('outcome_review_', 'review_'))
        and key not in {'scheduled_source_verification_required', 'scheduled_source_verification_satisfied'}}


def without_quality_review(state):
    """A persisted quality verdict cannot restart or block a resumed request."""
    result = {key: value for key, value in state.items() if key not in _RETIRED_STATE}
    result.update(outcome_review_cache={}, action_review_cache={}, completion_requirement_contract_cache={},
        completion_review_count=0, completion_recovery_pending=False, source_report_delivery_ready=False,
        response_presentation=_presentation(state.get('response_presentation')))
    if result.get('terminal_recovery_code') == 'preflight_revision_stalled':
        result.pop('terminal_recovery_code', None)
    if state.get('tool_results'):
        result['tool_results'] = without_review_receipts(state['tool_results'])
    return result


def clear_review_result(result):
    """Keep the actual producer output, files, pauses and execution failures."""
    result.outcome_review_receipt = None
    result.completion_review_handoff = None
    result.response_presentation = _presentation(getattr(result, 'response_presentation', None))
    result.tool_results = without_review_receipts(getattr(result, 'tool_results', ()))


def without_review_receipts(results):
    """Ignore obsolete model verdicts on restored turns; keep native metadata."""
    rows = []
    for row in results or ():
        metadata = row.get('response_requirements') if isinstance(row, Mapping) else getattr(row, 'response_requirements', None)
        if not isinstance(metadata, Mapping):
            rows.append(row)
            continue
        retired = {'execution_review', '_reviewed_action_reply'}
        if metadata.get('version') == 1 and 'request_digest' in metadata and 'evidence_binding' in metadata:
            retired.update({'version', 'request_digest', 'evidence_binding', 'required_attachment_extensions',
                'eligible_evidence_tool_names', 'partial_reply'})
        retained = {key: value for key, value in metadata.items() if key not in retired}
        if retained == metadata:
            rows.append(row)
        else:
            rows.append({**row, 'response_requirements': retained} if isinstance(row, Mapping)
                else replace(row, response_requirements=retained))
    return rows


def source_delivery_contract(state, results):
    """Project the producer's validated scope into source-file delivery metadata."""
    from nullion.browser_capture_contract import capture_only_scope
    from nullion.artifacts import ARTIFACT_ROLE_SOURCE, ARTIFACT_ROLE_DELIVERABLE, artifact_paths_from_output_descriptors
    decision = getattr(state.get('tool_registry'), 'turn_tool_scope_decision', None)
    capture = (capture_only_scope(decision, results)
        and '.png' in (getattr(decision, 'requested_artifact_extensions', ()) or ()))
    quote = ' '.join(str(getattr(decision, 'source_artifact_request_evidence', '') or '').split())
    requested = (getattr(decision, 'valid', False)
        and getattr(decision, 'source_artifact_delivery_requested', False)
        and quote and quote in ' '.join(str(state.get('user_message') or '').split()))
    if not capture and not requested:
        return (), ()
    formats = tuple(getattr(decision, 'requested_artifact_extensions', ()) or (('.png',) if capture else ()))
    selected = set(getattr(decision, 'requested_tool_names', ()) or ()) | set(getattr(decision, 'required_tool_names', ()) or ())
    def current_source_output(output):
        # Source-only tools can already mark an explicitly requested capture
        # as deliverable. Both native roles retain the same scoped selection;
        # demotion must not revoke it on the first or a subsequent pass.
        return bool(artifact_paths_from_output_descriptors(output,
            roles={ARTIFACT_ROLE_SOURCE, ARTIFACT_ROLE_DELIVERABLE}))
    producers = tuple(row.tool_name for row in results if row.status == 'completed' and row.tool_name in selected
        and current_source_output(row.output))
    contract = ({'requirement_kind': 'deliverable', 'delivery_required': True,
        'eligible_evidence_tool_names': producers},) if producers and formats else ()
    return contract, formats
