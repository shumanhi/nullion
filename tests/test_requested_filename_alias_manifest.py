"""A naming-only copy retains the current selected producer and review authority."""
import pytest
from nullion.tools import ToolResult
from nullion.artifact_lifecycle import record_delivery_manifest,delivery_manifest,filter_delivery_paths,reviewed_delivery_extensions
from nullion import chat_operator

@pytest.mark.parametrize('extension',['.csv','.pdf','.docx','.pptx','.xlsx','.html','.png','.zip'])
@pytest.mark.parametrize('review_status',[None,'completed','unavailable'])
def test_alias_preserves_selection_and_review(monkeypatch,tmp_path,extension,review_status):
    source=tmp_path/('hashed-0123'+extension);source.write_bytes(b'original artifact bytes 37')
    sibling=tmp_path/'separate.txt';sibling.write_text('Value\n53\n')
    results=[ToolResult('producer','file_write','completed',{'artifact_path':str(source),'artifact_descriptors':[{'path':str(source),'role':'deliverable'}],'source_image_paths':['/verified/image.png'],'inspection':{'embedded_image_count':1}}),ToolResult('sibling','file_write','completed',{'artifact_path':str(sibling)})]
    record_delivery_manifest(results,[str(source),str(sibling)],review_status=review_status,required_extensions=[extension,'.txt'])
    monkeypatch.setattr(chat_operator,'artifact_path_for_generated_workspace_file',lambda **kw: tmp_path/(kw['stem']+kw['suffix']))
    new,paths=chat_operator._materialize_requested_filename_aliases(principal_id='web:alias',prompt='final'+extension,artifact_paths=[str(source),str(sibling)],tool_results=results)
    assert len(new)==len(paths)==1
    final=tmp_path/('final'+extension)
    assert final.read_bytes()==source.read_bytes()
    combined=results+new
    assert set(delivery_manifest(combined))=={str(final),str(sibling)}
    assert set(filter_delivery_paths([str(source),str(final),str(sibling)],combined))=={str(final),str(sibling)}
    manifests=[r.output['artifact_delivery_manifest'] for r in combined if 'artifact_delivery_manifest' in r.output]
    assert manifests[-1]['review_status']==review_status
    assert new[0].output['source_image_paths']==['/verified/image.png']
    assert reviewed_delivery_extensions(combined)==(tuple(dict.fromkeys([extension,'.txt'])) if review_status=='completed' else None)
    replacement=ToolResult('overwritten','file_write','completed',{'artifact_path':str(final)})
    assert reviewed_delivery_extensions(combined+[replacement]) is None


def test_alias_does_not_revive_unselected_draft(monkeypatch,tmp_path):
    source=tmp_path/'draft.csv';source.write_text('Value\n37\n')
    result=ToolResult('draft','file_write','completed',{'artifact_path':str(source),'artifact_descriptors':[{'path':str(source),'role':'deliverable'}]})
    record_delivery_manifest([result],[])
    monkeypatch.setattr(chat_operator,'artifact_path_for_generated_workspace_file',lambda **kw: tmp_path/(kw['stem']+kw['suffix']))
    new,paths=chat_operator._materialize_requested_filename_aliases(principal_id='web:alias',prompt='final.csv',artifact_paths=[str(source)],tool_results=[result])
    assert not new and not paths and not (tmp_path/'final.csv').exists()
