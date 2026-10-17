"""Doctor's scheduled-task policy, driven only by verified runtime facts."""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import json

STREAK_LIMIT = 12

@dataclass(frozen=True)
class CronNotice:
    kind: str
    episode: str
    message: str

# These are internal result-schema codes, never classifications of user prose.
ISSUES = {
    'model_timeout': 'The model/provider did not respond before the check timed out.',
    'source_unverified': 'The source evidence could not be verified.',
    'delivery_failed': 'The result could not be delivered.',
    'interrupted': 'The worker stopped before recording a completed result.',
    'incomplete': 'The check did not produce a verified, complete outcome. No more specific cause was recorded.',
}

def cron_notice(job, facts: dict, *, label: str, schedule: str, last_success: str, next_check: str) -> CronNotice | None:
    if not job.enabled or facts.get('state') in {'running','waiting_for_approval','cancelled'}:
        return None
    limit = facts.get('doctor_streak_limit',STREAK_LIMIT)
    failed = int(facts.get('consecutive_failures',0))
    empty = int(facts.get('consecutive_no_matches',0))
    next_label = 'Scheduled check (overdue)' if facts.get('overdue') else 'Next scheduled check'
    if failed >= limit:
        kind='failure';episode=facts.get('failure_since')
        text=(f'"{label}" could not complete its last {failed} checks, so its current result is unknown.\n\n'
              f'Latest recorded problem: {ISSUES.get(facts.get("last_issue"), ISSUES["incomplete"])}\n'
              f'Last successful check: {last_success}.\n'
              f'The task remains enabled. {next_label}: {next_check}. '
              'No repair has been confirmed. Review its source access, permissions, and task settings if the problem continues.')
    elif empty >= limit:
        kind='no_match';episode=facts.get('no_match_since')
        text=(f'I checked "{label}" {empty} consecutive times successfully. '
              'No match met its saved conditions.\n\n'
              f'The tracker is still active. Schedule: {schedule}.\n'
              f'Latest successful check: {last_success}. {next_label}: {next_check}.\n'
              'I will alert you when a verified match is found. This is one status notice for this streak.')
    else:
        return None
    if not episode:
        return None
    return CronNotice(kind,str(episode),text)


def sync_cron_doctor_action(runtime, job, facts: dict, notice: CronNotice | None, *, scope: str) -> None:
    """Persist one actionable Doctor incident; resolve silently on verified success."""
    from nullion.doctor_actions import create_doctor_action
    from dataclasses import asdict
    changed=False
    actions=runtime.store.list_doctor_actions()
    for action in actions:
        try:
            details=json.loads(action.get('source_reason') or '{}')
        except (ValueError,TypeError):
            details={}
        if not isinstance(details,dict):
            continue
        if (details.get('cron_id')==job.id and details.get('workspace_id')==job.workspace_id
                and action.get('status') in {'pending','in_progress'}):
            reason = ('The scheduled task or its Doctor monitoring was disabled.' if not job.enabled or not facts.get('notifications_enabled') else
                      'The scheduled task requirements or delivery route changed.' if details.get('cron_scope')!=scope else
                      'A later scheduled check completed successfully.' if facts.get('last_outcome') in {'match','no_match','succeeded'} else None)
            if reason:
                runtime.cancel_doctor_action(action['action_id'],reason=reason)
                changed=True
    if notice and notice.kind=='failure':
        identity='cron-'+hashlib.sha256((scope+notice.episode).encode()).hexdigest()[:24]
        if runtime.store.get_doctor_action(identity) is None:
            record=asdict(create_doctor_action(action_id=identity,recommendation_code='cron_verification_failed',
                summary=notice.message,severity='warning',owner='doctor',source_reason=json.dumps({'cron_id':job.id,'cron_scope':scope,'workspace_id':job.workspace_id,'episode':notice.episode})))
            record.update(action_type='review_scheduled_task',details={'cron_id':job.id,'cron_scope':scope,
                'workspace_id':job.workspace_id,'episode':notice.episode,'consecutive_failures':facts['consecutive_failures']})
            runtime.store.add_doctor_action(record)
            changed=True
    if changed:
        runtime.checkpoint()
