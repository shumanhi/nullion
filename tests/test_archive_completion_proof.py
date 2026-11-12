import zipfile

from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive, _compact_tool_output_for_repair
from nullion.tools import ToolInvocation, ToolResult, _build_archive_extract_handler


def test_completed_archive_extraction_keeps_proof_after_context_compaction(tmp_path):
    path=tmp_path/'input.zip'
    with zipfile.ZipFile(path,'w') as archive: archive.writestr('one.txt','Cedar')
    result=_build_archive_extract_handler(tmp_path,include_principal_workspace=False)(
        ToolInvocation('extract','archive_extract','operator',{'path':str(path)}))
    assert result.status=='completed',result
    from nullion import agent_orchestrator as agent
    predicates=['_completion_review_runtime_envelope_is_negative','_completion_review_counts_are_invalid',
        '_completion_review_absence_state_is_invalid','_completion_review_collection_counts_are_contradictory',
        '_completion_review_runtime_envelope_declares_invalid_or_empty_evidence']
    assert _completion_review_evidence_result_is_positive(result), {name:getattr(agent,name)(result.output) for name in predicates}
    compact=ToolResult(result.invocation_id,result.tool_name,result.status,_compact_tool_output_for_repair(result.tool_name,result.output))
    assert _completion_review_evidence_result_is_positive(compact),compact.output


def test_completed_extraction_has_bound_source_record_even_when_manifest_is_not_delivered(tmp_path):
    from nullion.agent_orchestrator import _selected_artifact_tool_results
    from nullion.source_observation_contract import source_capture_review_records
    path=tmp_path/'input.zip'
    with zipfile.ZipFile(path,'w') as archive: archive.writestr('one.txt','Cedar')
    result=_build_archive_extract_handler(tmp_path,include_principal_workspace=False)(ToolInvocation(
        'extract','archive_extract','operator',{'path':str(path),'manifest_output_path':str(tmp_path/'internal.xlsx')}))
    assert result.status=='completed',result
    projected=_selected_artifact_tool_results([result], [])
    assert projected[0].output['result_kind']=='superseded_producer_execution'
    records,_=source_capture_review_records([result])
    assert len(records)==1
    assert records[0].output['record']['archive_path']==str(path)
    assert records[0].output['record']['extraction_complete'] is True
    assert _completion_review_evidence_result_is_positive(records[0])


def test_live_artifact_acceptance_rejects_delivered_but_unfinished_review():
    from types import SimpleNamespace
    from scripts.live_stage_chat_contract_probe import _assert_fulfilled_artifact_turn
    import pytest
    for value in [None,False]:
        with pytest.raises(AssertionError): _assert_fulfilled_artifact_turn(SimpleNamespace(name='archive'),{'response_fulfilled':value,'artifact_paths':['report.xlsx']})
    _assert_fulfilled_artifact_turn(SimpleNamespace(name='archive'),{'response_fulfilled':True})
