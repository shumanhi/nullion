"""Retire AI quality-review authority while retaining native execution facts."""
from collections.abc import Mapping
from dataclasses import replace


def reconcile_artifact_method(state, required_tool_names):
    """Apply an already executed replacement through the normal scope validator.

    A selected, current file from a later admitted producer is a structured
    method change. Preserve output quantities, source fidelity and all other
    obligations; neither prose nor a mere existing file can retire a method.
    """
    if not state.get('artifacts'):
        return state, False
    from pathlib import Path
    from uuid import uuid4
    from nullion.artifact_lifecycle import artifact_candidate_versions, identity, invalid_candidate_identities
    from nullion.tools import ToolInvocation, ToolSideEffectClass
    registry = state.get('tool_registry')
    apply_scope = getattr(registry, 'apply_scope_request', None)
    if not callable(apply_scope):
        return state, False
    results = list(state.get('tool_results') or ())
    required = set(required_tool_names)
    unavailable = {}
    for index, row in enumerate(results):
        metadata = row.response_requirements if isinstance(row.response_requirements, dict) else {}
        formats = metadata.get('_native_unavailable_file_producer_formats')
        facts = row.output if isinstance(row.output, dict) else {}
        if (row.tool_name in required and row.status == 'failed' and isinstance(formats, list)
                and facts.get('failure_scope') == 'tool' and facts.get('terminal_user_action_required') is True):
            try:
                failed_spec = registry.get_spec(row.tool_name)
            except (KeyError, AttributeError):
                failed_spec = None  # Native failure metadata survives discovery retirement.
            if failed_spec is not None and (failed_spec.side_effect_class is not ToolSideEffectClass.WRITE
                    or failed_spec.source_artifact_only):
                continue
            unavailable[row.tool_name] = (index, set(formats))
    if not unavailable:
        return state, False
    current = artifact_candidate_versions(results)
    selected = {identity(path) for path in state['artifacts']} - invalid_candidate_identities(results)
    replacements = []
    receipts = []
    formats = set()
    replaced = set()
    embedded_replacements = set()
    replacement_receipts = {}
    requested_formats = set(getattr(getattr(registry, 'turn_tool_scope_decision', None),
        'requested_artifact_extensions', ()) or ())
    for index, row in enumerate(results):
        if row.status != 'completed':
            continue
        try:
            spec = registry.get_spec(row.tool_name)
        except (KeyError, AttributeError):
            continue
        if (spec.side_effect_class is not ToolSideEffectClass.WRITE or spec.source_artifact_only
                or not spec.produced_artifact_extensions):
            continue
        paths = [path for path, producer in artifact_candidate_versions([row]).items()
            if path in selected and current.get(path) == producer and Path(path).is_file()]
        produced = {Path(path).suffix.lower() for path in paths}.intersection(spec.produced_artifact_extensions)
        matched = set()
        for name, (failed_index, failed_formats) in unavailable.items():
            if index <= failed_index:
                continue
            if produced.intersection(failed_formats):
                matched.add(name)
            elif (failed_formats and not failed_formats.intersection(requested_formats)
                    and produced.intersection(requested_formats)):
                # An image may be an intermediate dependency of a document,
                # rather than a requested image attachment. Verify the current
                # delivered bytes; model claims or preview metadata cannot
                # replace that dependency. Explicit image outputs stay required.
                from nullion.response_fulfillment_contract import IMAGE_ATTACHMENT_EXTENSIONS, artifact_file_embedded_media_count
                if (failed_formats.issubset(IMAGE_ATTACHMENT_EXTENSIONS)
                        and any(artifact_file_embedded_media_count(path) > 0 for path in paths)):
                    matched.add(name)
                    embedded_replacements.add(name)
        if matched:
            replaced.update(matched)
            for name in matched:
                replacement_receipts.setdefault(name, []).append(row.invocation_id)
            replacements.append(row.tool_name)
            receipts.append(row.invocation_id)
            formats.update(produced)
    if not replacements:
        return state, False
    selected_tools = sorted((required - replaced) | set(replacements))
    invocation = ToolInvocation('native-method-' + uuid4().hex, 'request_tool_scope',
        state.get('principal_id') or state.get('conversation_id') or 'operator', {
            'capabilities':['local_files'], 'tool_names':selected_tools,
            'required_tool_names':selected_tools, 'artifact_extensions':sorted(formats)},
        flow_context={'_runtime_unavailable_artifact_producers':list(unavailable),
            '_runtime_verified_embedded_media_replacements':sorted(embedded_replacements)})
    result, widened = apply_scope(invocation)
    if result.status != 'completed':
        return state, False
    # Failure handling may already have removed the method from the registry's
    # current scope while its earlier receipt still requires it. Record the
    # verified replacement against that receipt as well as the current scope.
    retired = replaced - set(result.output.get('required_tool_names') or ())
    if not retired:
        return state, False
    result.output['retired_unavailable_artifact_producers'] = sorted(retired)
    if not (required - set(result.output['retired_unavailable_artifact_producers'])).issubset(
            result.output.get('required_tool_names') or ()):
        return state, False
    for row in results:
        if row.status == 'failed' and row.tool_name in result.output['retired_unavailable_artifact_producers']:
            row.recovered_by = tuple(dict.fromkeys([*row.recovered_by, *replacement_receipts.get(row.tool_name, ())]))
    result.output.update(suppress_activity=True, native_artifact_method_reconciliation={
        'replacement_invocation_ids':receipts, 'selection_kind':'current_delivered_producer'})
    return {**state, 'tool_registry':widened, 'tool_results':[*results, result]}, True


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
    # Source-window descriptors share the old cache container, but are not
    # review verdicts. Dropping them on every model step erased exact evidence
    # carryover and made the producer repeatedly inspect the same source.
    from nullion.workflow_reasoning import _coalesce_inspection_windows, REVIEW_INSPECTION_MAX_WINDOWS
    cache = state.get('completion_requirement_contract_cache') or {}
    entries = cache.get('retained_inspection_windows') if isinstance(cache, Mapping) else None
    retained = _coalesce_inspection_windows(entries)[:REVIEW_INSPECTION_MAX_WINDOWS] if isinstance(entries, (list, tuple)) else []
    result.update(outcome_review_cache={}, action_review_cache={},
        completion_requirement_contract_cache={'retained_inspection_windows': retained} if retained else {},
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


def _authored_document_renderers(state, results, selected, formats):
    """A render of this turn's authored file is an output, not source research."""
    from pathlib import Path
    from urllib.parse import urlparse, unquote
    from nullion.artifact_lifecycle import artifact_candidate_versions, identity
    registry = state.get('tool_registry')
    authored = {}
    rendered = {}
    for row in results:
        if row.status != 'completed' or not isinstance(row.output, Mapping):
            continue
        try:
            spec = registry.get_spec(row.tool_name)
        except (AttributeError, KeyError):
            continue
        effect = getattr(spec.side_effect_class, 'value', spec.side_effect_class)
        if effect in {'write', 'dangerous_exec'} and not getattr(spec, 'source_artifact_only', False):
            for path, version in artifact_candidate_versions([row]).items():
                authored[path] = version
            continue
        if (row.tool_name not in selected or not getattr(spec, 'source_artifact_only', False)
                or not formats.intersection(getattr(spec, 'source_artifact_extensions', ()))):
            continue
        url = urlparse(str(row.output.get('page_url') or ''))
        if url.scheme != 'file' or url.netloc not in {'', 'localhost'}:
            continue
        source = Path(unquote(url.path))
        if source.suffix.lower() not in {'.html', '.htm'} or not source.is_file():
            continue
        key = identity(source)
        if key in authored:
            rendered[row.invocation_id] = (key, authored[key])
    return tuple(receipt for receipt, (path, version) in rendered.items() if authored.get(path) == version)


def source_delivery_contract(state, results):
    """Project the producer's validated scope into source-file delivery metadata."""
    from nullion.browser_capture_contract import capture_only_scope
    from pathlib import Path
    from nullion.artifacts import ARTIFACT_ROLE_SOURCE, ARTIFACT_ROLE_DELIVERABLE, artifact_paths_from_output_descriptors, normalize_artifact_extensions
    decision = getattr(state.get('tool_registry'), 'turn_tool_scope_decision', None)
    capture = (capture_only_scope(decision, results)
        and '.png' in (getattr(decision, 'requested_artifact_extensions', ()) or ()))
    quote = ' '.join(str(getattr(decision, 'source_artifact_request_evidence', '') or '').split())
    requested = (getattr(decision, 'valid', False)
        and getattr(decision, 'source_artifact_delivery_requested', False)
        and quote and quote in ' '.join(str(state.get('user_message') or '').split()))
    selected = set(getattr(decision, 'requested_tool_names', ()) or ()) | set(getattr(decision, 'required_tool_names', ()) or ())
    for row in reversed(results):
        if row.tool_name == 'request_tool_scope' and row.status == 'completed' and isinstance(row.output, Mapping):
            exact = row.output.get('explicit_tool_names') or ()
            if exact:
                selected = set(exact) | set(getattr(decision, 'required_tool_names', ()) or ())
            break
    rendered = (_authored_document_renderers(state, results, selected,
        set(getattr(decision, 'requested_artifact_extensions', ()) or ()))
        if getattr(decision, 'valid', False) and not getattr(decision, 'source_artifact_retrieval_only', False) else ())
    if not capture and not requested and not rendered:
        return (), ()
    def current_source_output(output):
        # Source-only tools can already mark an explicitly requested capture
        # as deliverable. Both native roles retain the same scoped selection;
        # demotion must not revoke it on the first or a subsequent pass.
        return bool(artifact_paths_from_output_descriptors(output,
            roles={ARTIFACT_ROLE_SOURCE, ARTIFACT_ROLE_DELIVERABLE}))
    eligible = selected if capture or requested else {row.tool_name for row in results if row.invocation_id in rendered}
    producers = tuple(row.tool_name for row in results if row.status == 'completed' and row.tool_name in eligible
        and (capture or requested or row.invocation_id in rendered)
        and current_source_output(row.output))
    formats = tuple(getattr(decision, 'requested_artifact_extensions', ()) or (('.png',) if capture else ()))
    if requested and producers and not formats:
        # A source file already has a native format. An explicit request to
        # deliver it must not depend on repeating its extension in the scope.
        excluded = set(getattr(decision, 'excluded_artifact_extensions', ()) or ())
        paths = [path for row in results if row.status == 'completed' and row.tool_name in producers
                 for path in artifact_paths_from_output_descriptors(row.output,
                     roles={ARTIFACT_ROLE_SOURCE, ARTIFACT_ROLE_DELIVERABLE})]
        formats = tuple(ext for ext in normalize_artifact_extensions(Path(path).suffix for path in paths)
                        if ext not in excluded)
    contract = ({'requirement_kind': 'deliverable', 'delivery_required': True,
        'eligible_evidence_tool_names': producers,
        **({'eligible_evidence_invocation_ids':rendered} if not capture and not requested else {})},) if producers and formats else ()
    return contract, formats
