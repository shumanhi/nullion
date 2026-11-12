"""Source adapters retain their answer after discovery and content capture."""
import json
import pytest
from nullion.response_sanitizer import sanitize_user_visible_reply,is_raw_tool_payload_reply
from nullion.tools import ToolResult


def evidence():
    return [ToolResult('search','public_discover','completed',{'result_kind':'public_search_results','results':[
        {'title':'Discovery lead','url':'https://source.example/item','snippet':'A possible candidate listing.'}]}),
        ToolResult('read','opaque_read','completed',{'source_url':'https://source.example/item',
            'text':'Observed price 73, duration 2h 17m.'})]


@pytest.mark.parametrize('reply',['確認済み: 73、2時間17分。','Precio observado: 73; duración: 2 h 17 min.','Observed price: 73. Duration: 2h 17m.'])
def test_captured_source_answer_is_not_replaced_by_earlier_search_leads(reply):
    actual=sanitize_user_visible_reply(user_message='値と所要時間を確認してください。',reply=reply,tool_results=evidence())
    assert actual==reply
    assert 'Discovery lead' not in actual


def test_source_capture_does_not_bypass_raw_payload_protection():
    rows=evidence();raw=json.dumps(rows[-1].output)
    actual=sanitize_user_visible_reply(user_message='結果を説明してください。',reply=raw,tool_results=rows)
    assert actual and actual!=raw
    assert not is_raw_tool_payload_reply(reply=actual,tool_results=rows)


@pytest.mark.parametrize('tool,summary',[('create_live_tracker','Live tracker is active.'),
    ('設定を更新','観測の終了日時を更新しました。'),('configurar','Seguimiento actualizado.'),
    ('calendar_create','Calendar event created.')])
def test_action_acknowledgement_survives_repeated_presentation_after_source_preparation(tool,summary):
    rows=[ToolResult('baseline','opaque_read','completed',{'result_kind':'browser_item_collection',
        'items':[{'title':'Preparation only','url':'https://source.example/item'}]}),
        ToolResult('action',tool,'completed',{'action_receipt':{'type':'action_receipt',
            'action':'created','object_type':'observation','object_id':'one',
            'summary':summary,'delivery_required':True,'details':[]}})]
    reply='Configured.'
    for _ in range(3):
        reply=sanitize_user_visible_reply(user_message='設定してください。',reply=reply,tool_results=rows)
        assert summary in reply
        assert 'Preparation only' not in reply
        assert reply.count(summary)==1
