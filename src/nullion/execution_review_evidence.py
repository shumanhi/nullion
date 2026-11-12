"""Runtime-owned outcome evidence for legacy delivery/status boundaries.

Provider output cannot carry this authority. A receipt is tied to the exact
request, final text and current tool evidence; it grants neither consent nor
permission to bypass file, action, cancellation or delivery validation.
"""
from dataclasses import dataclass
from collections.abc import Mapping

from nullion.workflow_recovery import fingerprint, result_value


@dataclass(frozen=True, slots=True)
class ExecutionReviewEvidence:
    complete: bool
    blocking_failure_ids: tuple[str, ...]
    partial_reply: str | None = None


def is_failure_or_user_action(result):
    from nullion.tools import normalize_tool_status
    output = result_value(result, 'output')
    return (normalize_tool_status(result_value(result, 'status')) in {'failed', 'denied'}
        or isinstance(output, Mapping) and (output.get('terminal_user_action_required') is True
            or output.get('connection_state') == 'pending_or_failed'))


def _file_versions(paths):
    from nullion.workflow_reasoning import selected_file_versions
    versions = selected_file_versions(paths)
    return [list(row) for row in versions] if all(len(row) == 4 and row[3] for row in versions) else None


def _legacy_binding(results, request, reply, files):
    return fingerprint([request, reply, files, [
        [result_value(r, 'invocation_id'), result_value(r, 'tool_name'), result_value(r, 'status'),
         result_value(r, 'output'), result_value(r, 'error'), result_value(r, 'recovered_by', ())]
        for r in results]])


def _binding(results, request, reply, files):
    from nullion.workflow_reasoning import outcome_binding
    return outcome_binding(request, {'selected_versions': files}, reply, results, ())


def record_execution_review(results, request, reply, *, complete, blocking_failure_ids=(), partial_reply=None,
        artifact_paths=()):
    rows = list(results or ())
    if not rows or not isinstance(request, str) or not isinstance(reply, str):
        return False
    # Only the runtime's validated review owner calls this producer. Tool
    # output fields with this name have no authority at the reader.
    target = next((r for r in reversed(rows) if hasattr(r, 'response_requirements')), None)
    if target is None:
        return False
    failures = {result_value(r, 'invocation_id') for r in rows if is_failure_or_user_action(r)}
    blockers = tuple(dict.fromkeys(blocking_failure_ids))
    if any(not isinstance(i, str) or i not in failures for i in blockers):
        raise ValueError('Invalid current failure binding')
    files = _file_versions(artifact_paths)
    if files is None:
        return False
    record = dict(target.response_requirements or {})
    record['execution_review'] = {'version': 2, 'binding': _binding(rows, request, reply, files),
        'files': files,
        'complete': complete is True, 'blocking_failure_ids': list(blockers),
        # The text is already in the final reply and in its binding. Store
        # authority to retain that reply, not another full copy of a report.
        'preserve_reply': isinstance(partial_reply, str) and partial_reply == reply}
    target.response_requirements = record
    return True


def reviewed_execution_evidence(results, request, reply):
    rows = list(results or ())
    for result in reversed(rows):
        outer = result_value(result, 'response_requirements')
        record = outer.get('execution_review') if isinstance(outer, Mapping) else None
        if record is None:
            continue
        files = record.get('files') if isinstance(record, Mapping) else None
        legacy_inline = isinstance(record, Mapping) and record.get('version') == 1 and files == []
        if (not isinstance(record, Mapping) or (record.get('version') != 2 and not legacy_inline)
                or type(record.get('complete')) is not bool
                or type(record.get('preserve_reply')) is not bool
                or not isinstance(record.get('blocking_failure_ids'), list)
                or any(not isinstance(i, str) for i in record['blocking_failure_ids'])
                or not isinstance(files, list)
                or any(not isinstance(row, list) or len(row) != 4 or not isinstance(row[0], str) for row in files)
                or _file_versions([row[0] for row in files]) != files
                or record.get('binding') != (_legacy_binding if legacy_inline else _binding)(rows, request, reply, files)):
            return None
        failures = {result_value(r, 'invocation_id') for r in rows if is_failure_or_user_action(r)}
        if not set(record['blocking_failure_ids']) <= failures:
            return None
        return ExecutionReviewEvidence(record['complete'], tuple(record['blocking_failure_ids']),
            reply if record['preserve_reply'] else None)
    return None
