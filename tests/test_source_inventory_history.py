"""Native inventory totals survive compaction and historical reference lookup."""
import json
import zipfile
from pathlib import Path
import pytest
from nullion.tools import ToolInvocation,_build_archive_extract_handler
from nullion.agent_orchestrator import _compact_tool_output_for_model_context
from nullion.conversation_history_tools import _event_record,compact_history_search_output


def test_native_extraction_totals_cover_only_successfully_extracted_files(tmp_path):
    archive=tmp_path/'資料.zip'
    with zipfile.ZipFile(archive,'w') as bundle:
        for name in ['a.png','b.PNG','c.txt','記録','nested/d.csv']:
            bundle.writestr(name,'data')
        bundle.writestr('__MACOSX/._a.png','metadata')
        bundle.writestr('../unsafe.png','outside')
    result=_build_archive_extract_handler(tmp_path)(ToolInvocation('extract','archive_extract','fixture:user',{'path':str(archive)}))
    assert result.status=='completed',result
    expected={'<none>':1,'csv':1,'png':2,'txt':1}
    assert result.output['extension_counts']==expected
    assert result.output['record']['extension_counts']==expected
    source=result.output['source_inventory'];assert source['record_count']==5 and source['extension_counts']==expected
    assert _compact_tool_output_for_model_context('archive_extract',result.output)['source_inventory']==source
    assert len(json.loads(Path(source['path']).read_text())['rows'])==5


@pytest.mark.parametrize('tool_name',['archive_extract','解析ツール'])
@pytest.mark.parametrize('status',['completed','failed'])
def test_history_preserves_compact_source_reference_without_large_payload_or_new_authority(tool_name,status):
    source={'path':'/workspace/資料.json','format':'json','rows_key':'rows','columns_key':'columns',
            'role':'source','record_count':3,'extension_counts':{'png':2,'txt':1}}
    event={'conversation_id':'web:source','workspace_id':'workspace_admin','turn_id':'parent',
        'created_at':'2026-11-11T12:00:00+00:00','user_message':'資料をまとめて','assistant_reply':'Ready',
        'tool_results':[{'invocation_id':'source-call','tool_name':tool_name,'status':status,
          'output':{'source_inventory':source,'private_body':'PRIVATE-BODY-'*1000}}]}
    record=_event_record(event,index=1)
    output=compact_history_search_output({'matches':[record],'structured_matches':[],'match_count':1},max_chars=2000)
    proof=output['matches'][0]['tool_evidence'][0]
    if status=='completed':
        assert proof['source_inventory']==source
        assert proof['invocation_id']=='source-call' and proof['reference_only'] is True
    else:assert 'source_inventory' not in proof
    assert 'private_body' not in json.dumps(output)
    assert output['matches'][0]['reference_only'] is True


@pytest.mark.parametrize('change,invalid_reference',[
    ({'record_count':True},True),({'record_count':-1},True),({'format':'csv'},True),
    ({'role':'deliverable'},True),({'path':''},True),({'rows_key':[]},True),
    ({'extension_counts':{'png':4}},False),({'extension_counts':{'png':True,'txt':2}},False),
    ({'extension_counts':{'png':-1,'txt':4}},False),
])
def test_source_reference_validation_does_not_promote_invalid_totals(change,invalid_reference):
    from nullion.artifact_evidence import source_inventory_reference
    source={'path':'/workspace/data.json','format':'json','rows_key':'rows','columns_key':'columns',
        'role':'source','record_count':3,'extension_counts':{'png':2,'txt':1},**change}
    reference=source_inventory_reference({'source_inventory':source})
    if invalid_reference:assert reference is None
    else:assert reference is not None and 'extension_counts' not in reference


def test_later_tool_steps_do_not_erase_the_original_source_handle():
    source={'path':'/workspace/data.json','format':'json','rows_key':'rows','columns_key':'columns',
        'role':'source','record_count':3,'extension_counts':{'png':2,'txt':1}}
    event={'conversation_id':'web:source','turn_id':'parent','assistant_reply':'Ready','user_message':'資料',
        'tool_results':[{'tool_name':'解析','invocation_id':'source','status':'completed','output':{'source_inventory':source}},
            *[{'tool_name':'read-'+str(n),'status':'completed','output':{'data':'private'*1000}} for n in range(12)]]}
    output=compact_history_search_output({'matches':[_event_record(event,index=1)],'structured_matches':[]})
    evidence=output['matches'][0]['tool_evidence']
    assert len(evidence)==4
    assert next(row for row in evidence if row.get('invocation_id')=='source')['source_inventory']==source
    assert [row['tool_name'] for row in evidence[1:]]==['read-9','read-10','read-11']
    assert len(json.dumps(output))<2500
