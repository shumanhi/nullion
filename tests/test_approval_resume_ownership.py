from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC,datetime
from types import SimpleNamespace
import threading
import pytest
from nullion.approval_resume import claim_approval_resume,approval_resume_claimed
from nullion.approvals import create_approval_request
from nullion.runtime import bootstrap_persistent_runtime
from nullion.suspended_turns import SuspendedTurn
from nullion.chat_operator import resume_approved_telegram_request


def test_claim_survives_other_runtime_and_checkpoint(tmp_path):
    path=tmp_path/'runtime.db';first=bootstrap_persistent_runtime(path);first.checkpoint(force=True);second=bootstrap_persistent_runtime(path)
    with ThreadPoolExecutor(2) as pool:
        results=list(pool.map(lambda r:claim_approval_resume(r,'same-approved-action'),[first,second]))
    assert sorted(results)==[False,True]
    first.checkpoint(force=True);second.checkpoint(force=True)
    restored=bootstrap_persistent_runtime(path)
    assert approval_resume_claimed(restored,'same-approved-action')
    assert not claim_approval_resume(restored,'same-approved-action')
    assert claim_approval_resume(restored,'new-approved-action')


@pytest.mark.parametrize('prompt',['Restart the local helper','ローカルヘルパーを再起動して','Reinicia el proceso local'])
def test_concurrent_and_stale_resume_does_not_replay_action(tmp_path,prompt):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    approval=create_approval_request(requested_by='telegram_chat',action='allow_boundary',resource='/tmp/example.log',request_kind='boundary_policy',context={})
    runtime.store.add_approval_request(approval)
    runtime.approve_approval_request(approval.approval_id,principal_id='telegram_chat',permissions=(),reason='fixture')
    runtime.store.add_suspended_turn(SuspendedTurn(approval.approval_id,'telegram:another-user','another-user',prompt,None,None,datetime.now(UTC),
        messages_snapshot=[{'role':'user','content':[{'type':'text','text':prompt}]},
                           {'role':'assistant','content':[{'type':'text','text':'earlier progress'}]},
                           {'role':'user','content':[{'type':'text','text':'Runtime recovery: generate a text attachment.'}]}]))
    runtime.checkpoint()
    started=threading.Event();finish=threading.Event();calls=[]
    class Runner:
        def resume_turn(self,**kw):
            assert kw['user_message']==prompt
            calls.append(kw);started.set();assert finish.wait(3)
            return SimpleNamespace(suspended_for_approval=False,final_text='The operation completed.',artifacts=[],tool_results=[],reached_iteration_limit=False)
    runner=Runner()
    def resume():return resume_approved_telegram_request(runtime,approval_id=approval.approval_id,chat_id='another-user',agent_orchestrator=runner)
    with ThreadPoolExecutor(2) as pool:
        first=pool.submit(resume);assert started.wait(2)
        assert pool.submit(resume).result() is None
        finish.set();assert 'completed' in first.result()
    assert resume() is None
    assert len(calls)==1
    assert runtime.store.get_suspended_turn(approval.approval_id) is None
    restored=bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert resume_approved_telegram_request(restored,approval_id=approval.approval_id,chat_id='another-user',agent_orchestrator=runner) is None


@pytest.mark.parametrize('requested',[(),('.pdf',),('.txt',)])
def test_terminal_capture_is_unique_and_only_explicit_text_is_deliverable(tmp_path,monkeypatch,requested):
    from nullion.tools import create_core_tool_registry,ToolInvocation,TerminalBackendDescriptor,TerminalExecutionResult
    from nullion.agent_orchestrator import _artifact_paths_from_tool_result
    from nullion.chat_operator import _terminal_exec_outcome_reply
    from nullion.artifacts import filter_source_artifact_paths
    monkeypatch.setenv('NULLION_TERMINAL_ENABLED','1')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'storage'))
    class Backend:
        calls=0
        def describe(self):return TerminalBackendDescriptor(mode='subprocess',attested_capabilities=(),metadata={})
        def run(self,*args,**kwargs):
            self.calls+=1
            return TerminalExecutionResult(exit_code=0,stdout=('details '+str(self.calls)+'\n')*600,stderr='')
    backend=Backend();registry=create_core_tool_registry(workspace_root=tmp_path,terminal_executor_backend=backend)
    outputs=[]
    for i in range(2):
        result=registry.invoke(ToolInvocation('orchestrator-'+str(i),'terminal_exec','web:another-user',{'command':'printf details'},flow_context={'requested_artifact_extensions':requested}))
        outputs.append(result)
        path=result.output['terminal_output_path'];expected=[path] if '.txt' in requested else []
        assert _artifact_paths_from_tool_result(result)==expected
        assert filter_source_artifact_paths([path],[result])==expected
        reply=_terminal_exec_outcome_reply([result])
        assert ('MEDIA:' in reply)==('.txt' in requested)
        assert 'details' in reply or '.txt' in requested
    assert outputs[0].output['terminal_output_path']!=outputs[1].output['terminal_output_path']
    from pathlib import Path
    assert 'details 1' in Path(outputs[0].output['terminal_output_path']).read_text()


def test_file_boundary_is_not_checked_as_network_target(tmp_path, monkeypatch):
    from nullion.tools import create_core_tool_registry, ToolInvocation, TerminalBackendDescriptor, TerminalExecutionResult
    monkeypatch.setenv('NULLION_TERMINAL_ENABLED', '1')
    class Backend:
        calls = 0
        def describe(self):
            return TerminalBackendDescriptor(mode='subprocess', attested_capabilities=(), metadata={})
        def run(self, *args, **kwargs):
            self.calls += 1
            return TerminalExecutionResult(exit_code=0, stdout='recorded', stderr='')
    backend = Backend()
    registry = create_core_tool_registry(workspace_root=tmp_path, terminal_executor_backend=backend)
    local = registry.invoke(ToolInvocation('local', 'terminal_exec', 'web:user',
        {'command': f'printf fixture > {tmp_path}/fixture.log', 'network_mode': 'approved_only', 'approved_targets': []}))
    assert local.status == 'completed', local
    network = registry.invoke(ToolInvocation('remote', 'terminal_exec', 'web:user',
        {'command': 'curl https://example.org', 'network_mode': 'approved_only', 'approved_targets': []}))
    assert network.status == 'failed'
    assert network.output['reason'] == 'network_denied'
    assert backend.calls == 1


@pytest.mark.parametrize('number', ['1', '１', '١'])
def test_numeric_approval_selection_uses_trusted_callback_metadata(number):
    from nullion import telegram_app
    approval_id='approved-operation'
    data=telegram_app._build_callback_data(kind='approval',action='allow_once',record_id=approval_id)
    card=SimpleNamespace(from_user=SimpleNamespace(id=888,is_bot=True),
        reply_markup=SimpleNamespace(inline_keyboard=[[SimpleNamespace(callback_data=data)]]))
    message=SimpleNamespace(text=number,reply_to_message=card)
    assert telegram_app._approval_callback_for_numeric_reply(message,bot_id=888)==data
    assert telegram_app._approval_callback_for_numeric_reply(message,bot_id=999) is None
    assert telegram_app._approval_callback_for_numeric_reply(SimpleNamespace(text=number),bot_id=888) is None
    card.from_user.is_bot=False
    assert telegram_app._approval_callback_for_numeric_reply(message,bot_id=888) is None


def test_numeric_approval_selection_rejects_mixed_or_other_card_metadata():
    from nullion import telegram_app
    data=lambda kind,record:telegram_app._build_callback_data(kind=kind,action='allow_once',record_id=record)
    card=SimpleNamespace(from_user=SimpleNamespace(id=888,is_bot=True),
        reply_markup=SimpleNamespace(inline_keyboard=[[SimpleNamespace(callback_data=data('approval','one')),SimpleNamespace(callback_data=data('approval','two'))]]))
    message=SimpleNamespace(text='1',reply_to_message=card)
    assert telegram_app._approval_callback_for_numeric_reply(message,bot_id=888) is None
    card.reply_markup.inline_keyboard=[[SimpleNamespace(callback_data=data('proposal','one'))]]
    assert telegram_app._approval_callback_for_numeric_reply(message,bot_id=888) is None
