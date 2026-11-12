import json
import pytest

@pytest.mark.parametrize('surface',['chat','web','startup'])
@pytest.mark.parametrize('label',['Registro','記録','Record'])
def test_late_source_identities_survive_large_history_compaction(surface,label):
    from nullion.chat_operator import _compact_tool_output_for_context
    from nullion.web_app import _compact_web_tool_output_for_context
    from nullion.startup_maintenance import _compact_tool_output
    output={'items':[{'title':label+' '+str(i),'url':'https://source.example/items/ID'+str(i).zfill(8)+'?session=opaque','body':'large evidence '*300} for i in range(50)],'source_record_collections':[{'path':['items'],'identity_fields':['title','url']}]}
    compact={'chat':_compact_tool_output_for_context,'web':_compact_web_tool_output_for_context,'startup':_compact_tool_output}[surface]('source_read',output)
    assert compact['result_kind']=='source_identity_inventory' and compact['truncated'] is True
    assert 'https://source.example/items/ID00000049' in compact['source_identity_url_paths']
    assert compact['source_identity_record_count']==50
    assert 'source_record_collections' not in compact and 'items' not in compact
    assert len(json.dumps(compact,ensure_ascii=False))<12500
