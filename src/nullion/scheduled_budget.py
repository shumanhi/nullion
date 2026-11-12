"""Durable, permission-preserving budget approvals for one scheduled execution."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json

from nullion.agent_turn_limits import (
    AGENT_TURN_LIMIT_EXTENSION_ACTION, AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
    chosen_multiplier_from_resume_token, max_iterations_from_resume_token,
)
from nullion.workflow_recovery import fingerprint


def job_binding(job):
    # next_run advances independently of this execution; last_run identifies
    # the execution being continued, so a newer run invalidates its approval.
    from nullion.crons import CronJob, _job_schedule_timezone, _timezone_display_name
    normalized = CronJob.from_dict(job.to_dict()).to_dict()
    normalized['schedule_timezone'] = _timezone_display_name(_job_schedule_timezone(job))
    return fingerprint({k: v for k, v in normalized.items() if k not in {'next_run', 'last_result'}})


def is_scheduled_budget_turn(turn):
    token = getattr(turn, 'resume_token', None)
    return isinstance(token, dict) and isinstance(token.get('scheduled_execution'), dict)


def _tool_rows(result):
    from nullion.agent_orchestrator import _serialize_pending_tool_calls
    return _serialize_pending_tool_calls(result.tool_results, result.invocation_metadata,
        workflow_checkpoint=result.workflow_checkpoint, work_usage=result.work_usage)


def _saved_result(result, artifacts=()):
    from nullion.tools import ToolResult
    fields = ('turn_id', 'final_text', 'artifacts', 'completed_invocation_signatures',
        'workflow_checkpoint', 'work_usage', 'invocation_metadata')
    value = {key: getattr(result, key) for key in fields}
    value['artifacts'] = list(dict.fromkeys([*value['artifacts'], *artifacts]))
    value['tool_results'] = _tool_rows(result)
    for key in ('completed_read_results', 'completed_action_results'):
        value[key] = {name: asdict(row) for name, row in getattr(result, key).items()
                      if isinstance(row, ToolResult)}
    from nullion.agent_orchestrator import _CompletionReviewHandoff
    from nullion.completion_pipeline import RecoveryCheckpoint
    handoff = getattr(result, 'completion_review_handoff', None)
    if isinstance(handoff, _CompletionReviewHandoff) and isinstance(handoff.recovery_checkpoint, RecoveryCheckpoint):
        value['recovery_checkpoint'] = asdict(handoff.recovery_checkpoint)
    from nullion.execution_outcome import PreparedAnswer, owned_prepared_answer
    answer = getattr(result, 'prepared_answer', None)
    request = answer.outcome.requested_outcome if isinstance(answer, PreparedAnswer) else ''
    if owned_prepared_answer(result, request) is not None:
        value['prepared_answer'] = {'request': request, 'text': answer.outcome.user_visible_message,
            'binding': answer.binding, 'complete': answer.outcome.status.value == 'succeeded',
            'remaining_work': list(answer.outcome.missing_requirements)}
    # Reject non-persistable receipts rather than publishing an unusable button.
    return json.loads(json.dumps(value, ensure_ascii=False))


def restore_result(value, snapshot):
    from nullion.agent_orchestrator import TurnResult
    from nullion.tools import ToolResult
    def tool(row):
        return ToolResult(row['invocation_id'], row['tool_name'], row['status'], row['output'],
            row.get('error'), recovered_by=tuple(row.get('recovered_by') or ()))
    result = TurnResult(value['turn_id'], value.get('final_text'), response_fulfilled=False,
        messages_snapshot=list(snapshot), artifacts=list(value['artifacts']),
        tool_results=[tool(row) for row in value['tool_results']],
        completed_invocation_signatures=list(value['completed_invocation_signatures']),
        workflow_checkpoint=dict(value['workflow_checkpoint']), work_usage=dict(value['work_usage']),
        invocation_metadata=dict(value['invocation_metadata']))
    for key in ('completed_read_results', 'completed_action_results'):
        setattr(result, key, {name: tool(row) for name, row in value[key].items()})
    recovery = value.get('recovery_checkpoint')
    if isinstance(recovery, dict):
        from nullion.agent_orchestrator import _CompletionReviewHandoff
        from nullion.completion_pipeline import RecoveryCheckpoint
        try:
            checkpoint = RecoveryCheckpoint(tuple(recovery['identity']), recovery['attempts'],
                tuple(recovery['progress']), tuple(recovery['tool_names']), recovery.get('compositions', 0))
            result.completion_review_handoff = _CompletionReviewHandoff({}, None, None, checkpoint)
        except (KeyError, TypeError, ValueError):
            pass
    saved_answer = value.get('prepared_answer')
    if isinstance(saved_answer, dict):
        from nullion.execution_outcome import prepare_answer
        try:
            answer = prepare_answer(saved_answer['request'], saved_answer['text'], result.tool_results,
                result.artifacts, complete=saved_answer['complete'] is True,
                remaining_work=tuple(saved_answer['remaining_work']))
            if answer.binding == saved_answer['binding'] and saved_answer['text'] == result.final_text:
                result.prepared_answer = answer
        except (KeyError, TypeError, ValueError):
            pass
    return result


def store_scheduled_budget_approval(runtime, *, job, original_job_binding, conversation_id,
        result, max_iterations, flow_context, connector_scope, tool_names, progress_text='', artifacts=(),
        agent_conversation_id=None):
    if not result.messages_snapshot or not getattr(runtime, 'checkpoint_path', None):
        return None
    checkpoint = {'version': 1, 'job': job.to_dict(), 'job_binding': original_job_binding,
        'conversation_id': conversation_id, 'flow_context': flow_context,
        'agent_conversation_id': agent_conversation_id or conversation_id,
        'connector_scope': asdict(connector_scope), 'tool_names': list(tool_names),
        'snapshot_binding': fingerprint(result.messages_snapshot),
        'result': _saved_result(result, artifacts)}
    checkpoint = json.loads(json.dumps(checkpoint, ensure_ascii=False))
    from nullion.agent_turn_limits import store_budget_approval
    return store_budget_approval(runtime, conversation_id=conversation_id,
        chat_id=conversation_id.partition(':')[2] if conversation_id.startswith('telegram:') else None,
        message=job.task, messages_snapshot=result.messages_snapshot, current_max_iterations=max_iterations,
        tool_result_count=len(result.tool_results), requested_extensions=tuple(result.required_artifact_extensions),
        context={'scheduled_checkpoint_binding': fingerprint(checkpoint), 'cron_id': job.id,
            'scheduled_task_name': job.name, 'scheduled_progress_text': progress_text, 'execution_scope': 'scheduled_run'},
        resume_state={'scheduled_execution': checkpoint})


@dataclass(frozen=True)
class ScheduledBudgetResume:
    checkpoint: dict
    snapshot: list
    max_iterations: int


def _current_job(runtime, cron_id):
    from nullion.sqlite_connections import sqlite_connection
    from nullion.crons import CronJob, _CRON_COLLECTION, _payload_is_deleted
    with sqlite_connection(str(runtime.checkpoint_path), timeout=10) as conn:
        row = conn.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',
                           (_CRON_COLLECTION, cron_id)).fetchone()
    if not row or _payload_is_deleted(row[0]):
        return None
    return CronJob.from_dict(json.loads(row[0]))


def resume_scheduled_budget_approval(runtime, suspended, *, background=False, return_result=False,
        cancellation_checker=None, _claim_owned=False):
    """Return an error reply, or None when the owning delivery flow handled it.

    Both Web and messaging approvals enter here before generic chat resume.
    The scheduled delivery graph owns the final report, not this adapter.
    """
    from nullion.approval_resume import claim_approval_resume, approval_resume_claimed
    from nullion.cron_scheduler_state import execution_lease, save_result, execution_result_label
    approval = runtime.store.get_approval_request(suspended.approval_id)
    if not _claim_owned and approval_resume_claimed(runtime, suspended.approval_id):
        return None
    token = suspended.resume_token or {}
    checkpoint = token.get('scheduled_execution')
    if (not approval or approval.status.value != 'approved'
            or approval.action != AGENT_TURN_LIMIT_EXTENSION_ACTION
            or approval.request_kind != AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND
            or not chosen_multiplier_from_resume_token(token)
            or not isinstance(checkpoint, dict)):
        return 'This scheduled continuation has not been approved.'
    if (approval.requested_by != suspended.conversation_id
            or checkpoint.get('conversation_id') != suspended.conversation_id
            or fingerprint(checkpoint) != approval.context.get('scheduled_checkpoint_binding')
            or fingerprint(suspended.messages_snapshot) != checkpoint.get('snapshot_binding')
            or max_iterations_from_resume_token(token) != approval.context.get('approved_max_iterations')
            or runtime.store.get_suspended_turn(suspended.approval_id) is None):
        return 'The saved continuation changed. Run the scheduled task again to create a new request.'
    runner = getattr(runtime, 'scheduled_budget_runner', None)
    if not callable(runner):
        return 'The saved continuation is unavailable in this runtime. Open its original app to resume it.'
    job = _current_job(runtime, checkpoint['job']['id'])
    if job is None or job_binding(job) != checkpoint['job_binding']:
        return 'This task changed or a newer run replaced it. The older continuation was not started.'
    if background:
        from nullion.cron_scheduler_state import leases
        if job.id in leases(runtime.checkpoint_path):
            return 'This scheduled task is already running. Its saved continuation was not started.'
        if not claim_approval_resume(runtime, approval.approval_id):
            return None
        def execute(cancellation_checker=None):
            result = resume_scheduled_budget_approval(runtime, suspended, return_result=True,
                cancellation_checker=cancellation_checker, _claim_owned=True)
            if isinstance(result, dict):
                return result
            return {'cron_report_failed': True, 'cron_delivery_status': 'failed',
                'cron_failure_notice': True, 'response_fulfilled': False,
                'reason': 'scheduled_continuation_not_started', 'text': result or
                    'This continuation was already handled.'}
        runner(job, ScheduledBudgetResume(checkpoint, suspended.messages_snapshot or [],
            max_iterations_from_resume_token(token)), execution_callback=execute)
        return None
    with execution_lease(runtime.checkpoint_path, job.id) as acquired:
        if not acquired:
            return 'This scheduled task is already running. Its saved continuation was not started.'
        current = _current_job(runtime, job.id)
        if current is None or job_binding(current) != checkpoint['job_binding']:
            return 'This task changed. Its saved continuation was not started.'
        if not _claim_owned and not claim_approval_resume(runtime, approval.approval_id):
            return None
        snapshot = list(suspended.messages_snapshot or [])
        snapshot.append({'role': 'user', 'content': [{'type': 'text', 'text':
            'Runtime continuation: the user approved more execution budget for this saved scheduled request. '
            'Continue its unfinished work from the retained evidence. Do not restart completed source checks '
            'or repeat successful actions. This approval changes no source, action, or delivery permissions.'}]})
        continuation = ScheduledBudgetResume(checkpoint, snapshot,
            max_iterations_from_resume_token(token))
        result = runner(job, continuation, **({'cancellation_checker': cancellation_checker} if cancellation_checker else {}))
        runtime.store.remove_suspended_turn(approval.approval_id)
        runtime.checkpoint()
        if not return_result:
            save_result(runtime.checkpoint_path, job, execution_result_label(result))
        if result.get('cron_delivery_failed') or result.get('cron_run_failed'):
            if return_result:
                return result
            return 'The scheduled continuation could not deliver its result. Its failure is recorded in the task history.'
    return result if return_result else None


def run_scheduled_continuation(job, continuation, *, callbacks, execution_callback=None, cancellation_checker=None):
    """One continuation dispatch path; adapters supply only delivery callbacks."""
    from nullion.cron_delivery import run_cron_delivery_workflow, start_manual_cron_background_delivery
    if execution_callback:
        def execute_saved(cron_job, *, label, callbacks, cancellation_checker=None):
            result = execution_callback(cancellation_checker=cancellation_checker)
            if isinstance(result, dict) and result.get('reason') == 'scheduled_continuation_not_started':
                # A task can change after enqueue. Deliver that terminal notice
                # through the same graph, instead of silently logging it.
                from dataclasses import replace
                notice_callbacks = replace(callbacks, run_agent_turn=lambda *a, **kw: result)
                return run_cron_delivery_workflow(cron_job, label=label, callbacks=notice_callbacks,
                    cancellation_checker=cancellation_checker)
            return result
        return start_manual_cron_background_delivery(job, label='Scheduled task', callbacks=callbacks,
            origin_conversation_id=continuation.checkpoint['conversation_id'],
            task_group_id='scheduled-budget-'+str(continuation.checkpoint['result']['turn_id']),
            workflow_runner=execute_saved, background_start_grace_seconds=0)
    return run_cron_delivery_workflow(job, label='Scheduled task', callbacks=callbacks,
        cancellation_checker=cancellation_checker)
