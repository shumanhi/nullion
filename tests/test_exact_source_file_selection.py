from types import SimpleNamespace
import pytest
from nullion.artifacts import register_selected_existing_source_artifacts
from nullion.tools import ToolResult

@pytest.mark.parametrize('prompt',['Adjunta cedar.pdf','cedar.pdf を添付','Joignez cedar.pdf'])
@pytest.mark.parametrize('condition',['valid','listing','ambiguous','outside','unquoted'])
def test_exact_lookup_only_delivers_the_uniquely_authorized_file(tmp_path,prompt,condition):
    root=tmp_path/'artifacts';root.mkdir();path=root/'cedar.pdf';path.write_bytes(b'%PDF-1.4 original bytes')
    paths=[str(path)]
    if condition=='ambiguous':
        other=root/'other';other.mkdir();(other/'cedar.pdf').write_bytes(b'%PDF-1.4 other bytes');paths.append(str(other/'cedar.pdf'))
    scope=SimpleNamespace(source_artifact_delivery_requested=condition!='listing',
        source_artifact_request_evidence='different request' if condition=='unquoted' else prompt,required_tool_names=())
    search=ToolResult('lookup','file_search','completed',{'matches':paths})
    selected=register_selected_existing_source_artifacts(reply='Found cedar.pdf',user_message=prompt,
        tool_results=[search],artifact_roots=[tmp_path/'unrelated'] if condition=='outside' else [root],source_scope_decision=scope)
    assert selected==([str(path)] if condition=='valid' else [])
    assert path.read_bytes()==b'%PDF-1.4 original bytes'
    if selected:assert search.output['artifact_descriptors'][0]['role']=='deliverable'


def test_latest_source_scope_can_decline_a_previously_planned_attachment(tmp_path):
    path=tmp_path/'cedar.pdf';path.write_bytes(b'%PDF-1.4 original')
    results=[ToolResult('first','request_tool_scope','completed',{'source_artifact_delivery_requested':True,
        'source_artifact_request_evidence':'Adjunta cedar.pdf'}),
        ToolResult('current','request_tool_scope','completed',{'source_artifact_delivery_requested':False}),
        ToolResult('lookup','file_search','completed',{'matches':[str(path)]})]
    assert register_selected_existing_source_artifacts(reply='MEDIA:'+str(path),user_message='Adjunta cedar.pdf',
        tool_results=results,artifact_roots=[tmp_path])==[]
