import pytest
from nullion.artifact_lifecycle import filter_delivery_paths,select_delivery_paths,record_delivery_manifest
from nullion.tools import ToolResult

def test_repaired_excluded_candidate_requires_fresh_selection():
    a='/managed/final.html';b='/managed/final.csv'
    results=[ToolResult('html','file_write','completed',{'artifact_path':a}),ToolResult('csv-draft','file_write','completed',{'artifact_path':b})]
    record_delivery_manifest(results,[a],candidate_paths=[a,b])
    assert filter_delivery_paths([a,b],results)==[a]
    results.append(ToolResult('csv-repair','file_write','completed',{'artifact_path':b}))
    assert filter_delivery_paths([a,b],results)==[a,b]
    assert select_delivery_paths([a,b],results,'',required_extensions=['.html','.csv'])==([],True)
    record_delivery_manifest(results,[a,b],candidate_paths=[a,b])
    assert select_delivery_paths([a,b],results,'',required_extensions=['.html','.csv'])==([a,b],False)

def test_inspection_cannot_reopen_an_excluded_draft():
    path='/managed/old.pdf'
    result=ToolResult('draft','pdf_create','completed',{'artifact_path':path})
    results=[result];record_delivery_manifest(results,[],candidate_paths=[path])
    results.append(ToolResult('read','file_read','completed',{'path':path,'inspection':{'readable':True}}))
    assert filter_delivery_paths([path],results)==[]


@pytest.mark.parametrize('reply',['Listo.','完了。','Ready.'])
def test_current_typed_selection_keeps_requested_siblings_when_later_prose_lists_only_bundle(reply):
    paths=['/managed/report.html','/managed/report.zip','/managed/discarded.html']
    results=[ToolResult(str(i),'file_write','completed',{'artifact_path':path}) for i,path in enumerate(paths)]
    record_delivery_manifest(results,paths[:2],candidate_paths=paths)
    results.append(ToolResult('inspection','terminal_exec','completed',{'stdout':'37','exit_code':0}))
    assert select_delivery_paths(paths,results,reply+'\nMEDIA:'+paths[1],required_extensions=['.html','.zip'])==(paths[:2],False)
    assert select_delivery_paths(paths,results,'MEDIA:'+paths[2],required_extensions=['.html','.zip'])==(paths[:2],False)


@pytest.mark.parametrize('restored_id',['stored-0',''])
def test_stored_transport_receipts_do_not_reopen_rejected_artifacts(restored_id):
    from copy import deepcopy
    path='/managed/rejected.pdf'
    results=[ToolResult('actual-producer','pdf_create','completed',{'artifact_path':path})]
    record_delivery_manifest(results,[],candidate_paths=[path],failure_message='The selected version could not be confirmed.')
    restored=[ToolResult(restored_id,row.tool_name,row.status,deepcopy(row.output)) for row in results]
    assert filter_delivery_paths([path],restored)==[]
    assert select_delivery_paths([path],restored,'',required_extensions=['.pdf'])==([],False)


@pytest.mark.parametrize('reply',['確認できませんでした。','No se pudo revisar.'])
def test_unique_native_requested_formats_remain_selectable_without_model_caption(reply):
    paths=['/managed/book.docx','/managed/book.pdf','/managed/slides.pptx','/managed/book.html']
    results=[ToolResult(str(i),'producer','completed',{'artifact_path':p}) for i,p in enumerate(paths)]
    assert select_delivery_paths(paths,results,reply,required_extensions=['.docx','.pdf','.pptx','.html'])==(paths,False)
    duplicate='/managed/draft.docx';results.append(ToolResult('draft','producer','completed',{'artifact_path':duplicate}))
    assert select_delivery_paths([*paths,duplicate],results,reply,required_extensions=['.docx','.pdf','.pptx','.html'])==([],True)


def test_unrequested_or_unproduced_format_cannot_supply_native_pack_selection():
    paths=['/managed/a.pdf','/managed/b.docx']
    results=[ToolResult('pdf','producer','completed',{'artifact_path':paths[0]})]
    assert select_delivery_paths(paths,results,'',required_extensions=['.pdf','.docx'])==([],True)
    results.append(ToolResult('word','producer','completed',{'artifact_path':paths[1]}))
    record_delivery_manifest(results,[paths[0]],candidate_paths=paths,review_status='completed')
    assert select_delivery_paths(paths,results,'',required_extensions=['.pdf','.docx'])==([paths[0]],False)


@pytest.mark.parametrize('case',['ready','new_target','missing_format','already_reviewed','executed_failure','missing_file'])
@pytest.mark.parametrize('editor', ['file_write', 'file_patch', 'opaque_editor'])
def test_rejected_retained_overwrite_enters_review_only_with_a_current_native_package(tmp_path,case,editor):
    from nullion.agent_orchestrator import _retained_artifact_reproposal_paths
    from nullion.tool_repair_contract import plan_revision_recovery
    from nullion.tools import ToolInvocation, ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    paths=[str(tmp_path/'a.csv'),str(tmp_path/'b.html')]
    for path in paths:
        from pathlib import Path
        Path(path).write_text('Current requested content')
    rows=[ToolResult(str(i),'file_write','completed',{'artifact_path':path}) for i,path in enumerate(paths)]
    registry = ToolRegistry()
    registry.register(ToolSpec(editor, 'Edit an artifact', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2),
        lambda inv: pytest.fail('Rejected edit must not run'))
    state={'tool_registry':registry,'tool_flow_context':{'required_artifact_extensions':['.csv','.html']},'tool_results':rows}
    if case=='missing_format':rows.pop()
    if case=='already_reviewed':state['artifact_selection_nudged']=True
    if case=='missing_file':Path(paths[1]).unlink()
    result=ToolResult('rejected',editor,'failed',{'reason':'request_plan_conflict','executed':False,
        'review_decision':'revise','retryable':True,'recovery':plan_revision_recovery(editor)})
    if case=='executed_failure':result.output['executed']=True
    invocation=ToolInvocation('rejected',editor,'p',{'path':str(tmp_path/'new.csv') if case=='new_target' else paths[0]})
    assert _retained_artifact_reproposal_paths(state,invocation,result,rows)==(paths if case=='ready' else [])


@pytest.mark.parametrize('effect', ['read', 'account_write', 'dangerous_exec'])
def test_nonlocal_write_tools_cannot_enter_artifact_edit_recovery(tmp_path, effect):
    from nullion.agent_orchestrator import _retained_artifact_reproposal_paths
    from nullion.tool_repair_contract import plan_revision_recovery
    from nullion.tools import ToolInvocation, ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    path = tmp_path/'report.html'; path.write_text('<html>Existing report</html>')
    registry = ToolRegistry()
    registry.register(ToolSpec('opaque', 'Adapter', ToolRiskLevel.LOW, ToolSideEffectClass(effect), False, 2),
        lambda inv: pytest.fail('Discovery must not execute'))
    rows = [ToolResult('producer', 'file_write', 'completed', {'artifact_path':str(path)})]
    result = ToolResult('rejected', 'opaque', 'failed', {'reason':'request_plan_conflict','executed':False,
        'review_decision':'revise','retryable':True,'recovery':plan_revision_recovery('opaque')})
    state = {'tool_registry':registry,'tool_results':rows,'tool_flow_context':{'required_artifact_extensions':['.html']}}
    assert not _retained_artifact_reproposal_paths(state, ToolInvocation('rejected','opaque','p',{'path':str(path)}), result, rows)
