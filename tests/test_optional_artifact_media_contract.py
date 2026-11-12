"""A failed optional illustration cannot rewrite the user's output contract."""
from pathlib import Path
import pytest
from nullion.tools import (ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolInvocation,
    ToolResult,_build_document_create_handler,_build_presentation_create_handler,
    _build_spreadsheet_create_handler,_build_pdf_create_handler)
from nullion.response_fulfillment_contract import (artifact_media_plain_replacement_guard_result,
    artifact_media_required_extensions,spreadsheet_media_embedding_was_required)

CASES=[('document_create','.docx',_build_document_create_handler,{'title':'Data','sections':[{'body':'Verified value 53'}]}),
       ('presentation_create','.pptx',_build_presentation_create_handler,{'title':'Data','slides':[{'title':'Data','bullets':['Verified value 53']}]}),
       ('spreadsheet_create','.xlsx',_build_spreadsheet_create_handler,{'columns':['Name'],'rows':[['Verified value 53']]}),
       ('pdf_create','.pdf',_build_pdf_create_handler,{'text_pages':['Verified value 53']})]

ALIASES=[('document_create',ext,_build_document_create_handler,{'title':'Data','sections':[{'body':'Verified value 53'}]}) for ext in ['.pdf','.html','.htm']]

@pytest.mark.parametrize('name,extension,builder,arguments',CASES+ALIASES)
@pytest.mark.parametrize('required',[False,True])
def test_native_media_failure_preserves_typed_request_contract(tmp_path,name,extension,builder,arguments,required):
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec(name,'Native artifact',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),builder(tmp_path,allowed_roots=[tmp_path]))
    path=tmp_path/('result'+extension)
    context={'required_embedded_media_extensions':(extension,) if required else ()}
    failed=registry.invoke(ToolInvocation('bad',name,'fixture:user',{
        **arguments,'output_path':str(path),'image_paths':[str(tmp_path/'missing.png')]},flow_context=context))
    assert failed.status=='failed',failed
    assert failed.response_requirements['_native_artifact_media']=={'version':1,'extension':'.html' if extension=='.htm' else extension,'required':required}
    plain=ToolInvocation('plain',name,'fixture:user',{**arguments,'output_path':str(path)},flow_context=context)
    guarded=artifact_media_plain_replacement_guard_result(plain,[failed],required_embedded_media_extensions=context['required_embedded_media_extensions'])
    assert (guarded is not None) is required,guarded
    assert bool(artifact_media_required_extensions([failed])) is required
    if name=='spreadsheet_create':assert spreadsheet_media_embedding_was_required([failed]) is required
    if not required:
        corrected=registry.invoke(plain)
        assert corrected.status=='completed' and path.is_file(),corrected
    else:
        from PIL import Image
        from nullion.response_fulfillment_contract import artifact_file_embedded_media_count,artifact_media_embedding_obligation_outstanding
        image=tmp_path/'real.png';Image.new('RGB',(160,160),'blue').save(image)
        visual=ToolInvocation('visual',name,'fixture:user',{**arguments,'output_path':str(path),
            'image_paths':[str(image)]},flow_context=context)
        assert artifact_media_plain_replacement_guard_result(visual,[failed],required_embedded_media_extensions=context['required_embedded_media_extensions']) is None
        repaired=registry.invoke(visual)
        assert repaired.status=='completed',repaired
        assert artifact_file_embedded_media_count(path)>0
        assert not artifact_media_embedding_obligation_outstanding([failed,repaired])


def test_provider_payload_cannot_make_a_media_failure_optional():
    failed=ToolResult('bad','spreadsheet_create','failed',{
        'reason':'spreadsheet_embed_paths_failed','skipped_images':['missing.png'],
        '_native_artifact_media':{'version':1,'extension':'.xlsx','required':False}})
    assert artifact_media_plain_replacement_guard_result(ToolInvocation('retry','spreadsheet_create','fixture:user',{
        'rows':[['data']]}),[failed]) is not None


def test_delivery_requirement_record_keeps_native_media_provenance():
    from nullion.request_delivery_requirements import record_request_delivery_requirements,reviewed_request_delivery_requirements
    metadata={'version':1,'extension':'.xlsx','required':False}
    result=ToolResult('done','spreadsheet_create','completed',{},response_requirements={'_native_artifact_media':metadata})
    assert record_request_delivery_requirements([result],'表を作成してください',['.xlsx'])
    assert result.response_requirements['_native_artifact_media']==metadata
    assert reviewed_request_delivery_requirements([result],'表を作成してください')['required_attachment_extensions']==['.xlsx']


def test_native_invocation_overwrites_handler_supplied_media_authority(tmp_path):
    registry=ToolRegistry()
    def external_handler(inv):
        return ToolResult(inv.invocation_id,inv.tool_name,'failed',{'reason':'spreadsheet_embed_paths_failed'},
            response_requirements={'_native_artifact_media':{'version':1,'extension':'.xlsx','required':False}})
    registry.register(ToolSpec('spreadsheet_create','Artifact',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5),external_handler)
    failed=registry.invoke(ToolInvocation('external','spreadsheet_create','fixture:user',{'rows':[['data']]},
        flow_context={'required_embedded_media_extensions':('.xlsx',)}))
    assert failed.response_requirements['_native_artifact_media']['required'] is True
    assert artifact_media_plain_replacement_guard_result(ToolInvocation('retry','spreadsheet_create','fixture:user',{'rows':[['data']]}),[failed]) is not None

@pytest.mark.parametrize('existing,requested',[('.htm','.html'),('.html','.htm'),('.HTM','.html')])
def test_html_alias_does_not_trigger_a_second_artifact(tmp_path,monkeypatch,existing,requested):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import _missing_required_attachment_extensions_for_repair,_materialize_last_mile_text_artifacts
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'data'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');principal='fixture:user'
    root=artifact_root_for_principal(principal);root.mkdir(parents=True,exist_ok=True)
    path=root/('complete'+existing);body=b'<html><body>Verified value 53</body></html>';path.write_bytes(body)
    producer=ToolResult('made','document_create','completed',{'path':str(path)})
    assert _missing_required_attachment_extensions_for_repair(runtime,principal_id=principal,
        prompt='Entrega el archivo.',required_attachment_extensions=(requested,),tool_results=[producer],artifact_paths=[str(path)])==()
    results,paths=_materialize_last_mile_text_artifacts(principal_id=principal,prompt='ファイルを添付してください。',
        reply='Ready',missing_extensions=(requested,),artifact_paths=[str(path)])
    assert results==[] and paths==[]
    assert path.read_bytes()==body and list(root.iterdir())==[path]


def test_html_named_directory_does_not_count_as_a_completed_file(tmp_path,monkeypatch):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import _existing_attachment_extensions_for_repair
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'data'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');root=artifact_root_for_principal('fixture:user');path=root/'folder.htm';path.mkdir(parents=True)
    assert not _existing_attachment_extensions_for_repair(runtime=runtime,principal_id='fixture:user',tool_results=[],artifact_paths=[str(path)])

@pytest.mark.parametrize('kind',['missing','directory'])
def test_stale_html_paths_cannot_mask_a_missing_file_or_be_listed_as_included(tmp_path,monkeypatch,kind):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import _materialize_last_mile_text_artifacts
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'data'))
    root=artifact_root_for_principal('fixture:user');root.mkdir(parents=True,exist_ok=True)
    ghost=root/'ghost.htm'
    if kind=='directory':ghost.mkdir()
    results,paths=_materialize_last_mile_text_artifacts(principal_id='fixture:user',prompt='Attach the prepared HTML.',
        reply='Verified value 53',missing_extensions=('.html',),artifact_paths=(str(p) for p in [ghost]))
    assert len(results)==len(paths)==1 and Path(paths[0]).is_file()
    assert 'ghost.htm' not in Path(paths[0]).read_text()
