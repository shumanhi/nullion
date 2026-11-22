import pytest

from nullion.cron_delivery import _cron_result_leaked_internal_tool_output
from nullion.tools import ToolResult


@pytest.mark.parametrize('summary',['Report delivered to reader@example.test.','報告を配信しました。'])
def test_public_action_receipt_is_not_an_internal_output_leak(summary):
    state = {'tool_results':[ToolResult('sent','report_send','completed',{'action_receipt':{
        'type':'action_receipt','action':'send','object_type':'email','summary':summary,
        'details':['Accepted by the mail transport.']}})]}
    assert not _cron_result_leaked_internal_tool_output(state,summary+' Accepted by the mail transport.')


@pytest.mark.parametrize('tool,field',[('terminal_exec','stdout'),('request_tool_scope','message'),('skill_pack_read','text')])
def test_public_receipt_does_not_allow_raw_or_internal_source_copy(tool,field):
    raw = 'Private source output and connector configuration. '*4
    receipt = {'type':'action_receipt','action':'inspect','object_type':'source','summary':raw}
    state = {'tool_results':[ToolResult('source',tool,'completed',{field:raw,'action_receipt':receipt})]}
    assert _cron_result_leaked_internal_tool_output(state,raw)
    assert state['cron_internal_tool_output_match']['output_field']==field
