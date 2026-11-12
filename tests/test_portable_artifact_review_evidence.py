import zipfile
import pytest
from nullion.artifact_evidence import artifact_structure_evidence
from nullion.agent_orchestrator import _compact_tool_output_for_repair,_completion_review_required

@pytest.mark.parametrize('broken',[False,True])
@pytest.mark.parametrize('extra',[False,True])
def test_portable_html_dependencies_are_inspected_without_extracting(tmp_path,broken,extra):
    p=tmp_path/'page.zip'
    with zipfile.ZipFile(p,'w') as archive:
        archive.writestr('page/index.html','<html><body><img src="../images/picture.svg"></body></html>')
        if not broken:archive.writestr('images/picture.svg','<svg xmlns="http://www.w3.org/2000/svg"></svg>')
        if extra:archive.writestr('unrelated.txt','not a dependency')
    evidence=artifact_structure_evidence(p)
    assert evidence['inspection_complete'] is True
    document=evidence['portable_html_documents'][0]
    assert document['entry_member']=='page/index.html'
    assert document['linked_asset_members']==['images/picture.svg']
    assert document['portable'] is (not broken)
    assert document['missing_asset_members']==(['images/picture.svg'] if broken else [])
    assert ('unrelated.txt' in evidence['archive_member_names']) is extra
    assert list(tmp_path.iterdir())==[p]


def test_creation_contract_flags_survive_compaction():
    output={'artifact_path':'/workspace/plan.xlsx','formulas_required':True,'rows':4,'columns':6,'sheet_names':['Comparison','Notes']}
    compact=_compact_tool_output_for_repair('spreadsheet_create',output)
    assert compact==output


def test_successful_repair_keeps_pending_outcome_review():
    from nullion.tools import ToolResult
    assert _completion_review_required({'completion_review_count':1},tool_results=[ToolResult('write','file_write','completed',{'artifact_path':'/workspace/report.html'})])
    assert not _completion_review_required({},tool_results=[])


def test_calendar_negative_retains_exact_query_window():
    output={'provider_id':'fixture','start':'2026-10-25T10:00:00Z','end':'2026-10-26T10:00:00Z','max':50,'result_count':0,'results':[]}
    compact=_compact_tool_output_for_repair('calendar_list',output)
    assert compact['start']==output['start'] and compact['end']==output['end']
    assert compact['result_count']==0 and compact['results']==[]


def test_weather_review_retains_complete_forecast_extent():
    output={'current':{'time':'2026-10-25T05:00'},'hourly':[{'time':'2026-10-25T00:00'},{'time':'2026-10-26T23:00'}]}
    assert _compact_tool_output_for_repair('weather_forecast',output)['forecast_coverage']=={'first_hour':'2026-10-25T00:00','last_hour':'2026-10-26T23:00','hour_count':2}


@pytest.mark.parametrize('hidden',[False,True])
def test_review_uses_current_producer_for_selected_path_only(hidden):
    from nullion.agent_orchestrator import _selected_artifact_tool_results
    from nullion.tools import ToolResult
    old=ToolResult('old','spreadsheet_create','completed',{'artifact_path':'/workspace/report.xlsx','formulas_required':False})
    if hidden:old.output['artifact_delivery_manifest']={'version':1,'selected_paths':[],'candidate_paths':['/workspace/report.xlsx']}
    new=ToolResult('new','spreadsheet_create','completed',{'artifact_path':'/workspace/report.xlsx','formulas_required':True})
    read=ToolResult('read','file_read','completed',{'path':'/workspace/report.xlsx'})
    selected=_selected_artifact_tool_results([old,new,read],['/workspace/report.xlsx'])
    assert selected[1:]==[new,read]
    assert selected[0].invocation_id=='old' and selected[0].status=='completed'
    assert selected[0].output['result_kind']=='superseded_producer_execution'
    assert not any(key in selected[0].output for key in ('artifact_path','path','formulas_required'))


@pytest.mark.parametrize('ready',[False,True])
def test_complete_inventory_reference_survives_all_context_projections(ready):
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    from nullion.chat_operator import _compact_tool_output_for_context
    from nullion.response_fulfillment_contract import artifact_paths_from_tool_results
    from nullion.tools import ToolResult
    output={'output_dir':'/workspace/current-extraction','manifest_json_path':'/workspace/inventory.json','manifest_xlsx_path':'/workspace/inventory.xlsx','manifest_row_count':111,'entry_count':111,'truncated':False,'entries':[{'name':str(i)} for i in range(111)],'artifact_paths':['/workspace/inventory.xlsx'] if ready else [],'artifact_descriptors':[{'path':'/workspace/inventory.xlsx','role':'deliverable' if ready else 'intermediate'}]}
    for project in [_compact_tool_output_for_model_context,_compact_tool_output_for_repair,_compact_tool_output_for_context]:
        compact=project('archive_extract',output)
        assert compact['source_inventory']=={'path':'/workspace/inventory.json','format':'json','rows_key':'rows','columns_key':'columns','extracted_root':'/workspace/current-extraction','record_count':111,'role':'source'}
        assert len(compact['entries_sample'])<111
        assert '/workspace/inventory.json' not in artifact_paths_from_tool_results([ToolResult('extract','archive_extract','completed',compact)])


@pytest.mark.parametrize('kind',['fact','deliverable','constraint'])
def test_superseded_execution_cannot_prove_current_outcomes(kind):
    from nullion import agent_orchestrator as a
    from nullion.tools import ToolResult
    record=ToolResult('past','pdf_create','completed',{'result_kind':'superseded_producer_execution','execution_succeeded':True,'summary':'Producer invocation completed.'})
    contract=(a._CompletionRequirementContractRow('r','Current outcome',kind,('pdf_create',),'outcome'),)
    decision=a._CompletionReviewDecision('complete',all_material_requirements_enumerated=True,
        material_requirements=(a._CompletionReviewMaterialRequirement('r','Current outcome',kind,True,True,('past',)),))
    assert not a._forced_completion_review_is_grounded(decision,tool_results=[record],requirement_contract=contract)


@pytest.mark.parametrize('extension',['.pdf','.docx','.xlsx','.pptx','.html','.csv','.svg','.png','.zip'])
@pytest.mark.parametrize('internal_kind',['constraint','action'])
@pytest.mark.parametrize('problem',[None,'external_fact','public_constraint','selected_file','wrong_producer','unbound','missing_parent','failed_parent'])
def test_current_internal_file_can_only_prove_an_eligible_execution_prerequisite(tmp_path,extension,problem,internal_kind):
    from nullion import agent_orchestrator as a
    from nullion.artifact_evidence import artifact_review_records
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    path=tmp_path/('working'+extension);path.write_bytes(b'Owned current artifact identity fixture.')
    parent=ToolResult('write','writer','completed',{'artifact_path':str(path)})
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('writer','Write artifact',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),lambda _:parent)
    records,fingerprints=artifact_review_records([path],[parent],registry=registry,allowed_roots=[tmp_path],include_unselected=True)
    assert len(records)==1
    record=records[0];record.output['artifact_record_binding']['delivery_role']='selected' if problem=='selected_file' else 'internal_not_selected'
    kind='fact' if problem=='external_fact' else 'constraint' if problem=='public_constraint' else internal_kind
    delivered=kind=='action' or problem in {'external_fact','public_constraint'}
    contract=(a._CompletionRequirementContractRow('r','内部作業ファイル',kind,
        ('other' if problem=='wrong_producer' else 'writer',),'working',delivery_required=delivered),)
    decision=a._CompletionReviewDecision('complete',all_material_requirements_enumerated=True,
        material_requirements=(a._CompletionReviewMaterialRequirement('r','内部作業ファイル',kind,True,
            delivered,(record.invocation_id,)),))
    evidence=[record] if problem=='missing_parent' else [ToolResult(parent.invocation_id,parent.tool_name,'failed',parent.output),record] if problem=='failed_parent' else [parent,record]
    assert a._forced_completion_review_is_grounded(decision,tool_results=evidence,requirement_contract=contract,
        source_record_fingerprints={} if problem=='unbound' else fingerprints) is (problem is None or (problem=='selected_file' and internal_kind=='action'))


@pytest.mark.parametrize('extension',['.pdf','.docx','.xlsx','.pptx','.html','.csv','.svg','.png','.zip'])
def test_supersession_tracks_producer_versions_not_pathnames(extension,tmp_path):
    from nullion.artifact_lifecycle import filter_delivery_paths
    from nullion.tools import ToolResult
    source=str(tmp_path/('source'+extension));replacement=str(tmp_path/('replacement'+extension))
    old=ToolResult('original','writer','completed',{'artifact_path':source})
    in_place=ToolResult('edit','writer','completed',{'artifact_path':source,'supersedes_artifact_paths':[source]})
    assert filter_delivery_paths([source],[old,in_place])==[source]
    different=ToolResult('replace','writer','completed',{'artifact_path':replacement,'supersedes_artifact_paths':[source]})
    assert filter_delivery_paths([source,replacement],[old,different])==[replacement]
    recreated=ToolResult('fresh','writer','completed',{'artifact_path':source})
    assert filter_delivery_paths([source,replacement],[old,different,recreated])==[source,replacement]
    read=ToolResult('read','reader','completed',{'path':replacement,'supersedes_artifact_paths':[source]})
    assert filter_delivery_paths([source],[old,read])==[source]
