import base64
import json
from types import SimpleNamespace

import pytest
from nullion.cron_checkpoint_journal import CronCheckpointJournal, commit_delivered_checkpoints
from nullion.tools import ToolInvocation


def test_checkpoint_is_restored_until_successful_delivery(monkeypatch, tmp_path):
    monkeypatch.setattr('nullion.workspace_storage.workspace_storage_roots_for_principal', lambda *a, **k: SimpleNamespace(root=tmp_path))
    path = tmp_path/'checkpoint.json'; path.write_text('{"reported": []}')
    journal = CronCheckpointJournal('operator')
    call = SimpleNamespace(tool_name='file_write', arguments={'path':str(path)})
    tracked = journal.before(call); path.write_text('{"reported": ["new"]}')
    journal.after(tracked, SimpleNamespace(status='completed'))
    updates = journal.defer()
    assert json.loads(path.read_text()) == {'reported': []}
    result = {'cron_pending_file_updates': updates, 'response_fulfilled': True}
    commit_delivered_checkpoints(result)
    assert json.loads(path.read_text()) == {'reported': ['new']}
    assert 'cron_pending_file_updates' not in result


@pytest.mark.parametrize('failed', ['cron_report_failed', 'cron_run_failed'])
def test_failure_notice_never_acknowledges_a_checkpoint(failed, tmp_path):
    path=tmp_path/'state.json';path.write_text('old')
    result={failed:True,'cron_pending_file_updates':[{'path':str(path),'before':base64.b64encode(b'old').decode(),'after':base64.b64encode(b'new').decode()}]}
    commit_delivered_checkpoints(result)
    assert path.read_text()=='old'


def test_checkpoint_commit_preserves_concurrent_changes(tmp_path):
    path=tmp_path/'state.json';path.write_text('changed by another run')
    result={'cron_pending_file_updates':[{'path':str(path),'before':base64.b64encode(b'old').decode(),'after':base64.b64encode(b'new').decode()}]}
    commit_delivered_checkpoints(result)
    assert path.read_text()=='changed by another run'
    assert result['cron_checkpoint_commit_conflicts']==1


def test_partial_failed_write_is_also_restored(monkeypatch, tmp_path):
    monkeypatch.setattr('nullion.workspace_storage.workspace_storage_roots_for_principal', lambda *a, **k: SimpleNamespace(root=tmp_path))
    path=tmp_path/'state.json';path.write_text('original')
    journal=CronCheckpointJournal('operator')
    tracked=journal.before(SimpleNamespace(tool_name='file_write',arguments={'path':str(path)}))
    path.write_text('partial')
    journal.after(tracked,SimpleNamespace(status='failed'))
    journal.defer()
    assert path.read_text()=='original'


def test_alert_context_includes_actual_receipts_for_its_own_job_only(tmp_path):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.cron_delivery import _cron_delivery_history
    runtime=bootstrap_persistent_runtime(tmp_path/'history.db')
    for cron_id,report in [('own','Failure notice; report did not reach the user'),('other','Unrelated private report')]:
        runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'telegram:123',
            'cron_id':cron_id,'assistant_reply':report,'created_at':'2026-10-10T14:00:00+00:00'})
    job=SimpleNamespace(id='own',delivery_channel='telegram',delivery_target='123',artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'})
    text=json.dumps(_cron_delivery_history(job,runtime,'web:operator'))
    assert 'Failure notice; report did not reach the user' in text
    assert 'Unrelated private report' not in text
    assert 'checkpoint is not proof' in text


def test_regular_report_does_not_load_alert_delivery_history():
    from nullion.cron_delivery import _cron_delivery_history
    class Store:
        def list_recent_conversation_events(self,*a,**k):
            raise AssertionError('Unrelated turns must stay on the existing path')
    assert _cron_delivery_history(SimpleNamespace(artifact_delivery_options={}),SimpleNamespace(store=Store()),'web:operator')==[]


def test_interrupted_run_restores_unacknowledged_checkpoint(monkeypatch, tmp_path):
    monkeypatch.setattr('nullion.workspace_storage.workspace_storage_roots_for_principal', lambda *a, **k: SimpleNamespace(root=tmp_path))
    path=tmp_path/'state.json';path.write_text('original')
    journal=CronCheckpointJournal('operator')
    tracked=journal.before(SimpleNamespace(tool_name='file_write',arguments={'path':str(path),'content':'unacknowledged'}))
    path.write_text('unacknowledged');journal.after(tracked,None)
    data=json.loads(journal.journal_path.read_text());data['pid']=999999999
    journal.journal_path.write_text(json.dumps(data))
    CronCheckpointJournal('operator')
    assert path.read_text()=='original'
    assert not journal.journal_path.exists()


def test_checkpoint_io_failure_does_not_invalidate_successful_receipt(tmp_path, monkeypatch):
    path=tmp_path/'state.json';path.write_text('old')
    result={'response_fulfilled':True,'cron_pending_file_updates':[{'path':str(path),'before':base64.b64encode(b'old').decode(),'after':base64.b64encode(b'new').decode()}]}
    def fail(*a, **k):
        raise OSError('filesystem unavailable')
    monkeypatch.setattr(type(path), 'write_bytes', fail)
    commit_delivered_checkpoints(result)
    assert result['response_fulfilled'] is True
    assert result['cron_checkpoint_commit_conflicts']==1
    assert path.read_text()=='old'
