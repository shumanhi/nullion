import copy
from dataclasses import asdict
import pytest
from nullion.tools import ToolResult
from nullion.request_delivery_requirements import record_reviewed_action_reply,reviewed_action_reply_matches,record_request_delivery_requirements
from nullion.response_sanitizer import sanitize_user_visible_reply


def receipt():
    return ToolResult('native','u0','completed',{'action_receipt':{'type':'action_receipt',
        'action':'updated','object_type':'monitor','object_id':'owned','object_name':'Owned',
        'summary':'Native canonical acknowledgement.','delivery_required':True}})


@pytest.mark.parametrize('prompt,reply',[('条件を変えて','条件を保存しました。'),('Modifica la condición','La condición está guardada.')])
@pytest.mark.parametrize('serialized',[False,True])
def test_reviewed_action_copy_is_presented_once_without_losing_native_receipt(prompt,reply,serialized):
    item=receipt();facts=copy.deepcopy(item.output)
    assert record_reviewed_action_reply([item],prompt,reply)
    rows=[asdict(item) if serialized else item]
    assert reviewed_action_reply_matches(rows,prompt,reply)
    assert sanitize_user_visible_reply(user_message=prompt,reply=reply,tool_results=rows)==reply
    assert item.output==facts
    assert record_request_delivery_requirements([item],prompt,[])
    assert reviewed_action_reply_matches([item],prompt,reply)


@pytest.mark.parametrize('change',['copy','request','effect','status','identity','other_native_facts','new_evidence'])
def test_changed_reply_request_or_native_action_does_not_reuse_presentation_approval(change):
    item=receipt();rows=[item];prompt='条件を変えて';reply='条件を保存しました。'
    record_reviewed_action_reply(rows,prompt,reply)
    if change=='copy':reply+=' Additional claim.'
    elif change=='request':prompt+=' 別の条件'
    elif change=='effect':item.output['action_receipt']['object_id']='other'
    elif change=='status':item.status='failed'
    elif change=='identity':item.invocation_id='different'
    elif change=='other_native_facts':item.output['configuration']={'interval_seconds':60}
    else:rows.append(ToolResult('new','read','completed',{}))
    assert not reviewed_action_reply_matches(rows,prompt,reply)


def test_unreviewed_copy_and_provider_payload_keep_native_acknowledgement():
    item=receipt();item.output['_reviewed_action_reply']={'version':1}
    rendered=sanitize_user_visible_reply(user_message='変更',reply='Done',tool_results=[item])
    assert 'Native canonical acknowledgement.' in rendered


def test_pre_review_deferral_preserves_authored_prose_but_not_raw_payload():
    item=receipt()
    assert sanitize_user_visible_reply(user_message='変更',reply='The condition is saved.',tool_results=[item],defer_action_receipts=True)=='The condition is saved.'
    import json
    raw=json.dumps(item.output)
    assert sanitize_user_visible_reply(user_message='変更',reply=raw,tool_results=[item],defer_action_receipts=True)!=raw
