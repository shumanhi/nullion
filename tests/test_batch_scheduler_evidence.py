"""Core batch result declarations bind only successful object operations."""
import pytest
from nullion.source_observation_contract import source_capture_review_records
from nullion.tools import ToolInvocation,_build_run_cron_handler,_build_toggle_cron_handler

@pytest.mark.parametrize('tool',['run_cron','toggle_cron'])
@pytest.mark.parametrize('missing',[False,True])
def test_scheduler_batch_records_bind_successful_exact_ids(tmp_path,monkeypatch,tool,missing):
    from nullion import crons
    monkeypatch.setenv('NULLION_HOME',str(tmp_path))
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'runtime.db')
    monkeypatch.setattr('nullion.connections.workspace_id_for_principal',lambda _: 'workspace_admin')
    first=crons.add_cron('A','0 0 1 1 *','Return ready.',workspace_id='workspace_admin')
    second=crons.add_cron('B','0 0 1 1 *','Return ready.',workspace_id='workspace_admin')
    other='deadbeef' if missing else second.id
    handler=_build_run_cron_handler(lambda job:'ready') if tool=='run_cron' else _build_toggle_cron_handler()
    result=handler(ToolInvocation('batch',tool,'operator',{'ids':[first.id,other],**({'enabled':False} if tool=='toggle_cron' else {})}))
    records,fingerprints=source_capture_review_records([result])
    expected=({first.id} if missing else {first.id,second.id}) if result.status=='completed' else set()
    assert {r.output['record']['id'] for r in records}==expected
    assert len(records)==len(fingerprints)==len(expected)
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    assert all(_completion_review_evidence_result_is_positive(row) for row in records)
    assert all(r.output['source_record_binding']['parent_invocation_id']=='batch' for r in records)
    if tool=='run_cron':
        assert result.output['failed_count']==int(missing)
        assert all(row['status']=='completed' for row in result.output['started_results'])
    if missing and tool=='toggle_cron':
        assert result.status=='failed' and not records
