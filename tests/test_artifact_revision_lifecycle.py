from pathlib import Path
import pytest
from nullion.tools import ToolInvocation, ToolResult, _build_file_read_handler, _build_pdf_edit_handler
from nullion.artifact_lifecycle import select_delivery_paths, record_delivery_manifest, filter_delivery_paths
from nullion.response_fulfillment_contract import artifact_media_plain_replacement_guard_result, artifact_paths_from_tool_results


def receipt(path, key='one', **extra):
    return ToolResult(key, 'pdf_create', 'completed', {'path': str(path), 'artifact_path': str(path), 'artifact_paths': [str(path)], **extra})


@pytest.mark.parametrize('names', [('alpha.pdf', 'beta.pdf'), ('文書.pdf', '改訂.pdf'), ('one.docx', 'two.docx'), ('x.xlsx', 'y.xlsx'), ('x.pptx', 'y.pptx'), ('x.png', 'y.png'), ('x.zip', 'y.zip'), ('x.html', 'y.html'), ('x.csv', 'y.csv'), ('x.txt', 'y.txt'), ('x.stl','y.stl'), ('x.glb','y.glb'), ('x.mp4','y.mp4'), ('x.mp3','y.mp3'), ('x.bin','y.bin')])
def test_repeated_producers_need_explicit_selection_without_filename_guesses(tmp_path, names):
    paths = [str(tmp_path/n) for n in names]
    rs = [receipt(paths[0]), receipt(paths[1], 'two')]
    assert select_delivery_paths(paths, rs, 'Done')[1]
    assert select_delivery_paths(paths, rs, '添付\nMEDIA:'+paths[1]) == ([paths[1]], False)
    assert select_delivery_paths(paths, rs, '\n'.join('MEDIA:'+p for p in paths)) == (paths, False)


def test_one_batch_preserves_multiple_outputs(tmp_path):
    paths = [str(tmp_path/'a.pdf'), str(tmp_path/'b.pdf')]
    rs = [ToolResult('batch', 'pdf_create', 'completed', {'artifact_paths': paths})]
    assert select_delivery_paths(paths, rs, 'Finished') == ([], True)
    assert select_delivery_paths(paths, rs, '\n'.join('MEDIA:'+p for p in paths)) == (paths, False)


@pytest.mark.parametrize('selected', [[], ['b.pdf'], ['a.pdf','b.pdf']])
def test_terminal_manifest_cannot_be_undone_by_collectors(tmp_path, selected):
    from nullion.chat_operator import _artifact_paths_from_tool_results
    paths = [str(tmp_path/n) for n in ['a.pdf','b.pdf']]
    rs = [receipt(paths[0]), receipt(paths[1], 'two')]
    expected = [str(tmp_path/n) for n in selected]
    record_delivery_manifest(rs, expected)
    assert filter_delivery_paths(paths, rs) == expected
    assert artifact_paths_from_tool_results(rs) == expected
    assert _artifact_paths_from_tool_results(rs) == expected


def test_pdf_edit_inherits_media_and_reports_actual_page_count(tmp_path):
    from PIL import Image
    from pypdf import PdfReader
    source = tmp_path/'source.pdf'; image = tmp_path/'image.jpg'; output = tmp_path/'edited.pdf'
    Image.new('RGB', (50,50),'blue').save(image)
    with Image.open(image) as im: im.save(source, 'PDF', save_all=True, append_images=[im.copy()])
    prior = receipt(source, source_image_paths=[str(image)])
    call = ToolInvocation('edit', 'pdf_edit', 'user:independent', {'input_path': str(source), 'output_path': str(output), 'page_numbers': [1], 'expected_page_count': 1})
    assert artifact_media_plain_replacement_guard_result(call, [prior], required_embedded_media_extensions=['.pdf']) is None
    result = _build_pdf_edit_handler(allowed_roots=[tmp_path], include_principal_workspace=False)(call)
    assert result.status == 'completed', result.error
    assert len(PdfReader(output).pages) == 1
    assert result.output['inspection']['embedded_image_count'] > 0
    assert result.output['supersedes_artifact_paths'] == [str(source)]
    assert select_delivery_paths([str(source),str(output)], [prior,result], '') == ([str(output)], False)
    # Comparison deliveries can explicitly select both revisions.
    assert select_delivery_paths([str(source),str(output)], [prior,result], f'MEDIA:{source}\nMEDIA:{output}') == ([str(source),str(output)], False)


@pytest.mark.parametrize('expected', [1, True, 0])
def test_page_count_contract_rejects_wrong_pages_and_invalid_values(tmp_path, expected):
    from pypdf import PdfWriter
    source=tmp_path/'source.pdf'; target=tmp_path/'target.pdf'
    w=PdfWriter(); w.add_blank_page(width=100,height=100); w.add_blank_page(width=100,height=100); w.write(source)
    call=ToolInvocation('edit','pdf_edit','user:independent',{'input_path':str(source),'output_path':str(target),'expected_page_count':expected})
    result=_build_pdf_edit_handler(allowed_roots=[tmp_path],include_principal_workspace=False)(call)
    assert result.status=='failed'
    assert not artifact_paths_from_tool_results([result])
    if expected is not True and expected == 1: assert result.output['reason']=='artifact_page_count_mismatch'


def test_binary_image_read_returns_metadata_without_claiming_visual_content(tmp_path):
    from PIL import Image
    path=tmp_path/'画面.png';Image.new('RGB',(32,48)).save(path)
    call=ToolInvocation('read','file_read','user:independent',{'path':str(path)})
    result=_build_file_read_handler(allowed_roots=[tmp_path],include_principal_workspace=False)(call)
    assert result.status=='completed',result.error
    assert result.output['inspection_kind']=='image_metadata'
    assert result.output['width']==32 and result.output['visual_content_verified'] is False
    assert not artifact_paths_from_tool_results([result])


def test_filename_only_discovery_does_not_force_artifact_regeneration(tmp_path):
    from nullion.agent_orchestrator import _stale_artifact_extensions_after_new_evidence
    path=str(tmp_path/'report.pdf');r=receipt(path)
    discovery=ToolResult('find','file_search','completed',{'matches':[path],'search_contents':False})
    assert not _stale_artifact_extensions_after_new_evidence([r,discovery],required_extensions=['.pdf'])
    source=ToolResult('source','browser_extract_text','completed',{'text':'New verified source content'})
    assert _stale_artifact_extensions_after_new_evidence([r,source],required_extensions=['.pdf']) == {'.pdf'}


def test_html_image_markup_is_a_real_media_input(tmp_path):
    call=ToolInvocation('html','pdf_create','user:other',{'html_pages':[f'<img src="{tmp_path}/asset.png"><p>資料</p>']})
    assert artifact_media_plain_replacement_guard_result(call, [], required_embedded_media_extensions=['.pdf']) is None
    plain=ToolInvocation('html','pdf_create','user:other',{'html_pages':['<p>資料</p>']})
    assert artifact_media_plain_replacement_guard_result(plain, [], required_embedded_media_extensions=['.pdf']) is not None


def test_reading_a_previous_revision_is_not_new_source_evidence(tmp_path):
    from nullion.agent_orchestrator import _stale_artifact_extensions_after_new_evidence
    a,b=[str(tmp_path/n) for n in ['old.pdf','current.pdf']]
    rs=[receipt(a),receipt(b,'two'),ToolResult('inspect','file_read','completed',{'path':a,'content':'Old version'})]
    assert not _stale_artifact_extensions_after_new_evidence(rs,required_extensions=['.pdf'])


def test_failed_page_contract_cannot_be_reintroduced_by_search_or_media(tmp_path):
    p=str(tmp_path/'candidate.pdf')
    failed=ToolResult('bad','pdf_create','failed',{'path':p,'reason':'artifact_page_count_mismatch'})
    found=ToolResult('find','file_search','completed',{'matches':[p]})
    assert filter_delivery_paths([p],[failed,found])==[]
    assert select_delivery_paths([p],[failed,found],'MEDIA:'+p)==([],False)
    good=receipt(p,'repair')
    assert filter_delivery_paths([p],[failed,found,good])==[p]


def test_parallel_child_manifests_preserve_each_selected_deliverable(tmp_path):
    a,b,c,d=[str(tmp_path/n) for n in ['a.pdf','b.pdf','c.pdf','d.pdf']]
    left=[receipt(a),receipt(b,'two')];right=[receipt(c,'three'),receipt(d,'four')]
    record_delivery_manifest(left,[b]);record_delivery_manifest(right,[d])
    assert filter_delivery_paths([a,b,c,d],left+right)==[b,d]
    record_delivery_manifest(left+right,[b,d])
    assert filter_delivery_paths([a,b,c,d],left+right)==[b,d]


def test_failed_child_does_not_hide_a_siblings_selected_file(tmp_path):
    a,b=[str(tmp_path/n) for n in ['a.png','b.png']]
    left=[receipt(a)];right=[receipt(b,'two')]
    record_delivery_manifest(left,[a]);record_delivery_manifest(right,[])
    assert filter_delivery_paths([a,b],left+right)==[a]


def test_image_only_pdf_is_readable_without_claiming_text_or_visual_verification(tmp_path):
    from PIL import Image
    path=tmp_path/'visual.pdf';im=Image.new('RGB',(64,80),'red');im.save(path,'PDF')
    result=_build_file_read_handler(allowed_roots=[tmp_path],include_principal_workspace=False)(ToolInvocation('inspect','file_read','user:independent',{'path':str(path)}))
    assert result.status=='completed',result.error
    assert result.output['inspection_kind']=='pdf_metadata' and result.output['page_count']==1
    assert result.output['content_available'] is False and result.output['visual_content_verified'] is False


@pytest.mark.parametrize('tool,extension',[('pdf_create','.pdf'),('document_create','.docx'),('presentation_create','.pptx'),('spreadsheet_create','.xlsx'),('archive_create','.zip'),('image_generate','.png'),('terminal_exec','.png'),('terminal_exec','.stl')])
@pytest.mark.parametrize('prompt',['Create the requested output.','要求した出力を作成。'])
def test_a_planned_producer_keeps_its_formats_when_inspection_tools_are_scoped(tool,extension,prompt):
    from nullion.tools import ToolRegistry,ToolSpec,ToolSideEffectClass,ToolRiskLevel
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence
    registry=ToolRegistry()
    registry.register(ToolSpec(tool,'Generate output',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),lambda call:None)
    registry.register(ToolSpec('file_read','Inspect a file',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,source_artifact_extensions=(extension,)),lambda call:None)
    evidence=TurnToolEvidence(has_url_target=False,has_attachments=False,has_email_address_target=False,requested_extensions=(),context_linked=False,saved_history_available=False,slash_prefixed_literal=False,current_user_message=prompt)
    scoped=ScopedTurnToolRegistry(registry,evidence=evidence)
    result,widened=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','user:other',{'capabilities':['local_files','image_generation'] if tool=='image_generate' else ['local_files'],'tool_names':[tool,'file_read'],'required_tool_names':[tool,'file_read'],'artifact_extensions':[extension]}))
    assert result.status=='completed' and extension in result.output['artifact_extensions'],result.output
    assert widened.can_invoke_tool(tool) and widened.can_invoke_tool('file_read')


@pytest.mark.parametrize('suffix',['.pdf','.png','.docx','.xlsx','.pptx','.zip','.stl'])
def test_local_download_recovery_requires_current_turn_artifact_evidence(tmp_path,suffix):
    from nullion.artifact_lifecycle import local_artifact_download_recovery
    path=tmp_path/('output'+suffix);path.write_bytes(b'known local artifact')
    call=ToolInvocation('download','file_download','user:other',{'url':path.as_uri()})
    assert local_artifact_download_recovery(call,[]) is None
    result=local_artifact_download_recovery(call,[receipt(path)])
    assert result.output['reason']=='local_artifact_already_available'
    assert result.output['recovery']['arguments']=={'path':str(path)}
    remote=ToolInvocation('download','file_download','user:other',{'url':'https://example.com/output'+suffix})
    assert local_artifact_download_recovery(remote,[receipt(path)]) is None


def test_later_explicit_selection_can_restore_an_original_after_a_child_selection(tmp_path):
    a,b=[str(tmp_path/n) for n in ['original.pdf','edited.pdf']]
    rs=[receipt(a),receipt(b,'two')]
    record_delivery_manifest(rs,[b])
    rs.append(receipt(a,'parent'))
    record_delivery_manifest(rs,[a,b])
    assert filter_delivery_paths([a,b],rs)==[a,b]


def test_pdf_output_validation_catches_missing_raster_embedding(tmp_path):
    from pypdf import PdfWriter
    from nullion.tools import _validated_pdf_output_handler
    path=tmp_path/'bad.pdf';writer=PdfWriter();writer.add_blank_page(width=100,height=100);writer.write(path)
    handler=_validated_pdf_output_handler(lambda call:receipt(path,source_image_paths=[str(tmp_path/'photo.png')]))
    result=handler(ToolInvocation('id','pdf_create','user:other',{}))
    assert result.status=='failed' and result.output['reason']=='artifact_validation_failed'


def test_final_evidence_filters_custom_writers_but_keeps_downloaded_sources(tmp_path):
    from nullion.agent_orchestrator import _selected_artifact_tool_results
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    registry=ToolRegistry()
    registry.register(ToolSpec('custom_export','Export a file',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),lambda call:None)
    registry.register(ToolSpec('web_fetch','Read a source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),lambda call:None)
    draft,final,source=[str(tmp_path/n) for n in ['draft.stl','final.stl','source.pdf']]
    rows=[ToolResult('a','custom_export','completed',{'artifact_paths':[draft]}),
          ToolResult('b','custom_export','completed',{'artifact_paths':[final]}),
          ToolResult('c','web_fetch','completed',{'artifact_paths':[source]})]
    assert _selected_artifact_tool_results(rows,[final],registry=registry)==rows[1:]


def test_cross_format_replacements_require_selection_and_final_manifests_remain_authoritative(tmp_path):
    paths = [str(tmp_path/'original.pdf'), str(tmp_path/'converted.docx')]
    rows = [receipt(paths[0]), receipt(paths[1], 'conversion')]
    assert select_delivery_paths(paths, rows, 'Created') == ([], True)
    record_delivery_manifest(rows, paths)
    assert select_delivery_paths(paths, rows, 'Created') == (paths, False)
    third = str(tmp_path/'next.html')
    rows.append(receipt(third, 'new'))
    assert select_delivery_paths([*paths,third], rows, 'Created') == ([], True)


def test_inspected_existing_pdf_can_be_edited_without_new_image_arguments(tmp_path):
    from PIL import Image
    source=tmp_path/'existing.pdf';Image.new('RGB',(50,60),'blue').save(source,'PDF')
    inspected=_build_file_read_handler(allowed_roots=[tmp_path],include_principal_workspace=False)(
        ToolInvocation('inspect','file_read','user:existing',{'path':str(source)}))
    assert inspected.status=='completed'
    edit=ToolInvocation('edit','pdf_edit','user:existing',{'input_path':str(source),'output_path':str(tmp_path/'final.pdf'),'expected_page_count':1})
    assert artifact_media_plain_replacement_guard_result(edit,[],required_embedded_media_extensions=['.pdf']) is None
    assert artifact_media_plain_replacement_guard_result(edit,[inspected],required_embedded_media_extensions=['.pdf']) is None
    result=_build_pdf_edit_handler(allowed_roots=[tmp_path],include_principal_workspace=False)(edit)
    assert result.status=='completed' and result.output['inspection']['embedded_image_count']>0,result


def test_text_writer_cannot_turn_a_source_pdf_into_an_output_requirement():
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    from nullion.turn_context_policy import _planned_output_extensions
    registry=ToolRegistry()
    registry.register(ToolSpec('file_write','Write text',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5),lambda call:None)
    formats=_planned_output_extensions(registry,['file_write'],['.pdf','.html'])
    assert '.html' in formats and '.pdf' not in formats


def test_scratch_verification_adds_only_permitted_completed_write_receipt_paths(tmp_path):
    from nullion.agent_orchestrator import _artifact_verification_roots
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    allowed=tmp_path/'scratch';allowed.mkdir()
    registry=ToolRegistry(filesystem_allowed_roots=[allowed])
    for name,effect in [('custom_export',ToolSideEffectClass.WRITE),('file_read',ToolSideEffectClass.READ)]:
        registry.register(ToolSpec(name,'Fixture',ToolRiskLevel.LOW,effect,False,5),lambda call:None)
    good=str(allowed/'output.bin');source=str(allowed/'source.bin');outside=str(tmp_path/'outside.bin')
    rows=[ToolResult('one','custom_export','completed',{'artifact_paths':[good,outside]}),
          ToolResult('two','file_read','completed',{'artifact_paths':[source]}),
          ToolResult('three','custom_export','failed',{'artifact_paths':[str(allowed/'bad.bin')]})]
    roots=_artifact_verification_roots({'tool_registry':registry,'runtime_store':None,'principal_id':'fixture'},rows)
    assert Path(good) in roots and allowed not in roots
    assert Path(source) not in roots and Path(outside) not in roots and allowed/'bad.bin' not in roots


@pytest.mark.parametrize('prompt',['Create draft.docx, revise final.docx, deliver only the final document.','下書き draft.docx を作成し、final.docx に修正。最終版だけ添付。'])
@pytest.mark.parametrize('selected',[True,False])
def test_terminal_selection_outranks_ambiguous_prompt_filename_counts(tmp_path,monkeypatch,prompt,selected):
    from types import SimpleNamespace
    from docx import Document
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.chat_operator import _completed_requested_artifact_paths_for_turn_result
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');principal='web:final-roles'
    root=artifact_root_for_principal(principal);root.mkdir(parents=True,exist_ok=True)
    paths=[str(root/name) for name in ['draft.docx','final.docx']]
    rows=[]
    for index,path in enumerate(paths):
        document=Document();document.add_paragraph('Draft' if index==0 else 'Final');document.save(path)
        rows.append(ToolResult(str(index),'document_create','completed',{'artifact_path':path}))
    expected=[paths[-1]] if selected else []
    record_delivery_manifest(rows,expected)
    result=SimpleNamespace(tool_results=rows,artifacts=expected,final_text='Ready')
    assert _completed_requested_artifact_paths_for_turn_result(runtime,result=result,prompt=prompt,principal_id=principal,
        required_attachment_extensions=('.docx','.docx'),required_embedded_media_extensions=())==tuple(expected)
