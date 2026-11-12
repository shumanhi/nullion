import pytest

from nullion.response_sanitizer import sanitize_user_visible_reply
from nullion.tools import ToolResult


@pytest.mark.parametrize('path', ['/home/example/runtime/bin/python', r'C:\Users\example\runtime\python.exe'])
@pytest.mark.parametrize('prompt', ['Give the exact executable path.', '実行ファイルの正確なパスを示して。'])
def test_requested_paths_require_typed_scope_and_completed_source(path, prompt):
    scope=ToolResult('scope','request_tool_scope','completed',{'include_local_path_details':True})
    source=ToolResult('read','terminal_exec','completed',{'stdout':path})
    reply='Runtime: `'+path+'`'
    render=lambda rows:sanitize_user_visible_reply(user_message=prompt,reply=reply,tool_results=rows)
    assert path in render([scope,source])
    assert path not in render([source])
    assert path not in render([scope,ToolResult('read','terminal_exec','failed',{'stdout':path})])
    assert path not in render([scope,ToolResult('read','terminal_exec','completed',{'stdout':'unrelated'})])
    assert path not in render([scope,source,ToolResult('revoke','request_tool_scope','completed',{})])
    # Context must not leak into another delivery or another request.
    assert path not in render([source])


def test_request_flag_does_not_expose_unrelated_generated_or_state_paths():
    scope=ToolResult('scope','request_tool_scope','completed',{'include_local_path_details':True})
    source=ToolResult('read','terminal_exec','completed',{'stdout':'/home/example/runtime/bin/python'})
    text='Runtime `/home/example/runtime/bin/python`; report `/home/example/private/report.pdf`.'
    rendered=sanitize_user_visible_reply(user_message='実行環境を確認。',reply=text,tool_results=[scope,source])
    assert '/home/example/runtime/bin/python' in rendered
    assert '/home/example/private/report.pdf' not in rendered


def test_transport_evidence_does_not_select_a_new_source_answer():
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    evidence=[ToolResult('inventory','list_crons','completed',{'jobs':[
        {'id':'one','name':'Selected schedule'}, {'id':'two','name':'Unrelated schedule'}]})]
    text='Selected schedule is disabled.'
    delivery=prepare_reply_for_platform_delivery(text,allow_attachments=False,tool_results=evidence)
    assert delivery.text==text
