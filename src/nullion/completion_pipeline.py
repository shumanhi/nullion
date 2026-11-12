"""Request-owned terminal state shared by execution and delivery.

The producer publishes a checkpoint after choosing its answer and files. A
transport must not reopen that exact result with a fresh recovery allowance.
This is lifecycle state, not a quality verdict or permission to run a tool.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping

from nullion.workflow_recovery import result_value


@dataclass(frozen=True, slots=True)
class CompletionDraft:
    """Content production only; no permissions, recovery or delivery verdict."""
    text: str | None
    artifacts: tuple[str, ...]
    tool_results: tuple[object, ...]
    prepared_answer: object = None
    presentation: Mapping[str, object] | None = None
    failure_code: str | None = None
    fulfillment_claim: bool | None = None
    content_receipt: object = None


class CompletionFileConflictError(ValueError):
    """An external change invalidated a content-only correction's file proof."""


def prepare_completion(state, text, *, review=None):
    """Run an optional content producer inside the common completion owner.

    The producer may retain evidence and append real artifact-production
    receipts. It cannot replace prior source/action receipts, grant tools or
    decide whether the original request has finished.
    """
    preparer = state.get('completion_preparer')
    if not callable(preparer):
        return state, text, False
    from nullion.workflow_reasoning import outcome_binding
    from copy import deepcopy
    previous = tuple(state.get('tool_results') or ())
    proof = outcome_binding('', {}, None, previous, ())
    previous_paths = list(state.get('artifacts') or ())
    files_before = outcome_binding('', {}, None, (), previous_paths)
    prepared = preparer(state={**state, 'tool_results': deepcopy(list(previous)),
        'artifacts': list(state.get('artifacts') or ()),
        'response_presentation': deepcopy(dict(state.get('response_presentation') or {}))},
        text=text, review=review)
    if prepared is None:
        return state, text, False
    if not isinstance(prepared, CompletionDraft):
        raise TypeError('Completion preparation must return a CompletionDraft')
    if ((prepared.text is not None and not isinstance(prepared.text, str))
            or not all(isinstance(path, str) for path in prepared.artifacts)
            or prepared.fulfillment_claim is not None and type(prepared.fulfillment_claim) is not bool):
        raise TypeError('Completion draft must contain text and selected paths')
    current = {result_value(row, 'invocation_id'): row for row in prepared.tool_results}
    retained = [current.get(result_value(row, 'invocation_id')) for row in previous]
    if any(row is None for row in retained) or outcome_binding('', {}, None, retained, ()) != proof:
        raise ValueError('Completion preparation cannot discard or change execution receipts')
    old_ids = {result_value(row, 'invocation_id') for row in previous}
    from nullion.artifact_validation import validate_artifact_paths
    for row in prepared.tool_results:
        if result_value(row, 'invocation_id') in old_ids:
            continue
        output = result_value(row, 'output')
        paths = output.get('artifact_paths') if isinstance(output, Mapping) else None
        if (result_value(row, 'status') != 'completed' or result_value(row, 'error')
                or not isinstance(output, Mapping) or output.get('structured_artifact_verified') is not True
                or not paths or not validate_artifact_paths(paths).ok):
            raise ValueError('Completion preparation may append only verified artifact receipts')
    if review is not None and files_before != outcome_binding('', {}, None, (), previous_paths):
        raise CompletionFileConflictError('Selected files changed during a content-only correction')
    failure = ({'code': prepared.failure_code,
        'binding': outcome_binding('', {}, None, prepared.tool_results, prepared.artifacts)}
        if prepared.failure_code else None)
    return {**state, 'tool_results': list(prepared.tool_results), 'artifacts': list(prepared.artifacts),
        'completion_prepared_answer': prepared.prepared_answer,
        'completion_preparation_claim': prepared.fulfillment_claim,
        'completion_content_receipt': deepcopy(prepared.content_receipt),
        'completion_preparation_failure': failure,
        # Source-preview markers describe the superseded draft. Keeping them
        # would let a later adapter reject the newly composed answer again.
        'response_presentation': {**{key: value for key, value in (state.get('response_presentation') or {}).items()
                if key not in {'kind', 'fulfillment_satisfied'}},
            **{key: value for key, value in (prepared.presentation or {}).items()
                if isinstance(key, str) and key.startswith('report_')}}}, prepared.text, True


def current_preparation_failure(state):
    from nullion.workflow_reasoning import outcome_binding
    failure = state.get('completion_preparation_failure')
    return failure if isinstance(failure, Mapping) and failure.get('binding') == outcome_binding(
        '', {}, None, state.get('tool_results') or (), state.get('artifacts') or ()) else None


def remaining_execution_calls(state):
    """An owned total allowance includes provider retries and model promotion.

    Absent a structured allowance this does not change ordinary turn limits.
    A resume carries cumulative usage and an explicitly approved new baseline.
    """
    budget = (state.get('tool_flow_context') or {}).get('execution_model_call_budget')
    if budget is None:
        return None
    if not isinstance(budget, Mapping):
        return 0
    limit, baseline = budget.get('limit'), budget.get('baseline')
    used = (state.get('work_usage') or {}).get('model_calls', 0)
    if any(type(value) is not int or value < 0 for value in (limit, baseline, used)) or baseline > used:
        return 0
    return max(0, limit - (used - baseline))


def receipt_versions(results):
    from nullion.workflow_reasoning import outcome_binding
    versions = {}
    for row in results:
        identity = result_value(row, 'invocation_id')
        if not isinstance(identity, str) or not identity or identity in versions:
            return {}
        versions[identity] = outcome_binding('', {}, None, [row], ())
    return versions


def retained_read_evidence_matches(previous, results, read_tools):
    """New reads may supplement findings; writes cannot preserve old totals."""
    rows = list(results or ())
    current = receipt_versions(rows)
    if not previous or not current or any(current.get(key) != value for key, value in previous.items()):
        return False
    for row in rows:
        if result_value(row, 'invocation_id') in previous:
            continue
        output = result_value(row, 'output')
        if (result_value(row, 'status') not in {'completed', 'failed'}
                or result_value(row, 'tool_name') not in read_tools
                or isinstance(output, Mapping) and isinstance(output.get('action_receipt'), Mapping)):
            return False
    return True


@dataclass(frozen=True, slots=True)
class RetainedCompletionAnswer:
    answer: object
    evidence: tuple[tuple[str, str], ...]
    files: str
    read_tools: frozenset[str]


def retain_completion_answer(state, request, text):
    from nullion.execution_outcome import prepare_answer
    from nullion.workflow_reasoning import outcome_binding
    rows, paths = list(state.get('tool_results') or ()), list(state.get('artifacts') or ())
    specs = getattr(state.get('tool_registry'), 'list_specs', None)
    return RetainedCompletionAnswer(prepare_answer(request, text, rows, paths),
        tuple(receipt_versions(rows).items()), outcome_binding('', {}, None, (), paths),
        frozenset(spec.name for spec in (specs() if callable(specs) else ())
            if getattr(spec.side_effect_class, 'value', spec.side_effect_class) == 'read'))


def retained_completion_answer(record, request, results, paths):
    from nullion.execution_outcome import PreparedAnswer
    from nullion.workflow_reasoning import outcome_binding
    if (isinstance(record, RetainedCompletionAnswer) and isinstance(record.answer, PreparedAnswer)
            and record.answer.outcome.requested_outcome == request
            and record.files == outcome_binding('', {}, None, (), paths)
            and retained_read_evidence_matches(dict(record.evidence), results, record.read_tools)):
        return record.answer
    return None


@dataclass(frozen=True, slots=True)
class CompletionCheckpoint:
    request: str
    binding: str
    reason: str
    conversation_id: str = ''
    principal_id: str = ''


@dataclass(frozen=True, slots=True)
class RecoveryCheckpoint:
    """The same producer's improvement allowance across internal handoffs."""
    identity: tuple[str, str, str]
    attempts: int
    progress: tuple[str, ...]
    tool_names: tuple[str, ...]
    compositions: int = 0


def capture_recovery(state, request):
    return RecoveryCheckpoint((state.get('conversation_id', ''), state.get('principal_id', ''), request),
        int(state.get('completion_review_count') or 0),
        tuple(state.get('completion_review_progress_fingerprints') or ()),
        tuple(state.get('completion_review_recovery_tool_names') or ()),
        int(state.get('completion_composition_count') or 0))


def restore_recovery(checkpoint, state, request):
    """Do not replenish a spent window or carry it to another user/request."""
    if (not isinstance(checkpoint, RecoveryCheckpoint)
            or checkpoint.identity != (state.get('conversation_id', ''), state.get('principal_id', ''), request)
            or type(checkpoint.attempts) is not int or checkpoint.attempts < 0
            or type(checkpoint.compositions) is not int or checkpoint.compositions < 0
            or not all(isinstance(name, str) for name in (*checkpoint.progress, *checkpoint.tool_names))):
        return {}
    return {'completion_review_count': checkpoint.attempts,
        'completion_composition_count': checkpoint.compositions,
        'completion_review_progress_fingerprints': list(checkpoint.progress),
        'completion_review_recovery_tool_names': list(checkpoint.tool_names)}


def _checkpoint(result):
    value = result_value(result, 'completion_checkpoint')
    if isinstance(value, CompletionCheckpoint):
        return value
    if (isinstance(value, Mapping) and set(value) == {
            'request', 'binding', 'reason', 'conversation_id', 'principal_id'}
            and all(isinstance(value[key], str) for key in value)
            and value['binding'] and value['reason']):
        return CompletionCheckpoint(**value)
    return None


def _control_state(result):
    state = {name: result_value(result, name) for name in (
        'response_fulfilled', 'suspended_for_approval', 'approval_id',
        'reached_iteration_limit', 'model_timed_out', 'raw_tool_payload_blocked',
        'artifact_delivery_required', 'artifact_delivery_satisfied',
    )}
    presentation = result_value(result, 'response_presentation', {}) or {}
    if isinstance(presentation, Mapping) and presentation.get('needs_input') is True:
        state['input_required'] = True
    return state


def _binding(result, request, conversation_id='', principal_id=''):
    from nullion.workflow_reasoning import outcome_binding
    from pathlib import Path
    artifacts = list(result_value(result, 'artifacts', ()) or ())
    paths = []
    for artifact in artifacts:
        path = artifact.get('path') if isinstance(artifact, Mapping) else artifact
        if isinstance(path, (str, Path)):
            paths.append(str(path))
    return outcome_binding(request, {**_control_state(result),
        'conversation_id': conversation_id, 'principal_id': principal_id,
        'artifact_descriptors': artifacts},
        result_value(result, 'final_text', result_value(result, 'text')),
        result_value(result, 'tool_results', ()) or (),
        paths)


def published_completion(result, request, *, conversation_id=None, principal_id=None):
    """Return only an exact current producer checkpoint, including file bytes."""
    checkpoint = _checkpoint(result)
    if (checkpoint is not None and checkpoint.request == request
            and (conversation_id is None or checkpoint.conversation_id == conversation_id)
            and (principal_id is None or checkpoint.principal_id == principal_id)
            and checkpoint.binding == _binding(result, request,
                checkpoint.conversation_id, checkpoint.principal_id)):
        return checkpoint
    return None


def _legacy_terminal_reason(result, request, *, pending_tool_names=()):
    """Translate existing typed producer contracts at the migration boundary.

    No assistant wording is interpreted. This remains necessary for persisted
    results written before producer checkpoints existed.
    """
    presentation = result_value(result, 'response_presentation', {}) or {}
    if not isinstance(presentation, Mapping):
        presentation = {}
    if result_value(result, 'suspended_for_approval', False):
        return 'awaiting_approval'
    if result_value(result, 'reached_iteration_limit', False):
        return 'budget_pause'
    if presentation.get('needs_input') is True:
        # A native producer question is a public terminal result for this
        # turn, even though its requested mutation has not executed. Shared
        # adapter recovery cannot answer the question on the user's behalf.
        return 'awaiting_input'
    if presentation.get('artifact_terminal') is True:
        return 'artifact_finalized'
    if presentation.get('recovery_exhausted') is True:
        return 'recovery_exhausted'
    if presentation.get('source_report_status') in {'partial', 'completed'}:
        return 'source_report_finalized'
    from nullion.execution_review_evidence import reviewed_execution_evidence
    results = result_value(result, 'tool_results', ()) or ()
    text = result_value(result, 'final_text', result_value(result, 'text'))
    review = reviewed_execution_evidence(results, request, text)
    fulfilled = result_value(result, 'response_fulfilled')
    if fulfilled is False and review is not None and review.partial_reply == text:
        return 'partial_answer_finalized'
    completed = {result_value(row, 'tool_name') for row in results
                 if result_value(row, 'status') == 'completed'}
    continuations_done = all(name in completed for name in pending_tool_names)
    if (fulfilled is True and review is not None and review.complete
            and not review.blocking_failure_ids and continuations_done):
        return 'answer_finalized'
    handoff = result_value(result, 'completion_review_handoff')
    assistance = getattr(handoff, 'latest_review', None)
    if (presentation.get('outcome_review_role') == 'helper' and assistance is not None
            and not assistance.review_error and assistance.supported_findings
            and assistance.disposition in {'blocked', 'retry'}):
        return 'assisted_partial_answer_finalized'
    substantive = bool(result_value(result, 'artifacts')) or any(
        result_value(row, 'tool_name') not in {'request_tool_scope', 'chat_history_search'}
        for row in results)
    if (presentation.get('outcome_review_role') == 'helper' and fulfilled is not False
            and (substantive or pending_tool_names) and continuations_done):
        return 'assisted_answer_finalized'
    return None


def recovery_closed(result, request=None, *, pending_tool_names=()):
    """One terminal-state decision for adapter recovery and history fallback.

    A changed request, draft, receipt, file selection or file version invalidates
    a checkpoint. Never silently fall back to its stale terminal flags.
    """
    checkpoint = _checkpoint(result)
    if checkpoint is not None:
        request = checkpoint.request if request is None else request
        if published_completion(result, request) is None:
            return False
        if checkpoint.reason in {'answer_finalized', 'assisted_answer_finalized'}:
            completed = {result_value(row, 'tool_name') for row in
                (result_value(result, 'tool_results', ()) or ())
                if result_value(row, 'status') == 'completed'}
            if any(name not in completed for name in pending_tool_names):
                return False
        return True
    return _legacy_terminal_reason(result, request or '', pending_tool_names=pending_tool_names) is not None


def publish_completion(result, request, *, reason=None, pending_tool_names=(),
        conversation_id='', principal_id=''):
    """Publish only a producer terminal decision; never certify fulfillment."""
    reason = reason or _legacy_terminal_reason(result, request, pending_tool_names=pending_tool_names)
    if reason is None:
        return None
    checkpoint = CompletionCheckpoint(request, _binding(result, request, conversation_id, principal_id),
        reason, conversation_id, principal_id)
    if isinstance(result, dict):
        result['completion_checkpoint'] = asdict(checkpoint)
    else:
        result.completion_checkpoint = checkpoint
    return checkpoint
