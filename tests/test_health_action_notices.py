import pytest

from nullion.health_action_notices import health_action_notice
from nullion.doctor_playbooks import execute_doctor_playbook_command
from tests.test_doctor_playbooks_workflow import RuntimeDouble, _action


@pytest.mark.parametrize('family,command,status', [
    ('doctor', 'complete', 'completed'), ('doctor', 'cancel', 'cancelled'),
    ('doctor', 'start', 'in_progress'), ('sentinel', 'resolve', 'resolved'),
    ('sentinel', 'acknowledge', 'acknowledged'),
])
def test_status_receipts_describe_alerts_without_certifying_repairs(family, command, status):
    notice = health_action_notice(family, command, status)
    assert notice.message.startswith(family.capitalize()+' alert')
    assert notice.message not in {'Action completed.', 'Action dismissed.'}
    assert notice.effect == 'alert_status'
    assert notice.repair_verified is False
    assert notice.as_payload()['repair_verified'] is False


@pytest.mark.parametrize('status', ['completed', 'cancelled', 'failed'])
@pytest.mark.parametrize('command', ['doctor:restart_bot', 'doctor:reconnect_telegram', 'doctor:restart_plugin'])
def test_stale_closed_playbook_buttons_do_not_replay_mutations(status, command):
    runtime = RuntimeDouble(_action(status=status))
    result = execute_doctor_playbook_command(runtime, action_id=runtime.store.action['action_id'],
        command=command, source_label='Web', restart_chat_services=lambda: pytest.fail('Restart replayed'))
    assert result.action['status'] == status
    assert result.effect == 'none'
    assert 'No new action was run' in result.message
    assert runtime.started == runtime.cancelled == []


@pytest.mark.parametrize('summary', ['Needs review', '要確認', 'تحتاج إلى مراجعة'])
@pytest.mark.parametrize('command', ['doctor:restart_plugin', 'doctor:disable_plugin', 'doctor:pause_chat', 'doctor:unknown_operation'])
def test_guidance_and_unsupported_actions_leave_health_alert_unchanged(summary, command):
    runtime = RuntimeDouble(_action(summary=summary))
    before = dict(runtime.store.action)
    result = execute_doctor_playbook_command(runtime, action_id=before['action_id'], command=command, source_label='Web')
    assert result.action == before == runtime.store.action
    assert runtime.started == runtime.cancelled == []
    assert result.repair_verified is False
    assert result.effect in {'guidance', 'unsupported'}


def test_notice_does_not_report_a_resolution_if_status_changed_before_receipt():
    notice = health_action_notice('doctor', 'complete', 'failed')
    assert 'already failed' in notice.message
    assert 'marked as resolved' not in notice.message


def test_status_notice_rejects_unknown_command():
    with pytest.raises(ValueError, match='Unsupported'):
        health_action_notice('doctor', 'invented-operation', 'pending')
