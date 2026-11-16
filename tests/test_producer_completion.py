"""Retired quality state cannot gain authority after resume."""
from types import SimpleNamespace
import pytest
from nullion.producer_completion import without_quality_review, clear_review_result, source_delivery_contract
from nullion.tools import ToolResult


def test_resume_discards_only_quality_authority_and_keeps_native_receipts():
    media = {'required_extensions':['.png'], 'mode':'embed'}
    row = ToolResult('read', 'file_read', 'completed', {'value':37}, response_requirements={
        'execution_review':{'complete':False}, '_reviewed_action_reply':{},
        'version':1, 'request_digest':'old', 'evidence_binding':'old',
        'required_attachment_extensions':['.pdf'], 'partial_reply':'obsolete',
        '_native_artifact_media':media, 'native_receipt':{'id':'owned'}})
    state = {'tool_results':[row], 'outcome_review_cache':{'_latest':'old'},
        'action_review_stop':{'decision':'revise'}, 'completion_recovery_iteration_limit':100,
        'response_presentation':{'review_error':'timeout', 'report_presentation':'structured'},
        'artifacts':['owned.html'], 'suspended_for_approval':True, 'approval_id':'native',
        'completion_review_unresolved_requirements':['obsolete']}
    current = without_quality_review(state)
    assert 'completion_recovery_iteration_limit' not in current and 'action_review_stop' not in current
    assert current['completion_review_count']==0 and current['outcome_review_cache']=={}
    assert current['response_presentation']=={'report_presentation':'structured'}
    assert current['suspended_for_approval'] and current['approval_id']=='native'
    assert current['artifacts']==['owned.html']
    assert current['tool_results'][0].response_requirements=={'_native_artifact_media':media, 'native_receipt':{'id':'owned'}}
    assert row.response_requirements['execution_review']=={'complete':False}  # No historical mutation.
    assert without_quality_review(current)==current


def test_removing_quality_state_preserves_only_owned_inspection_descriptors():
    from nullion.workflow_reasoning import remember_review_inspections, inspect_review_evidence, retained_review_inspections
    result = ToolResult('owned', 'opaque_read', 'completed', {'body':'Captured value 17.'})
    cache = {'rows':['obsolete verdict'], 'approved':True}
    windows = inspect_review_evidence([{'invocation_id':'owned','json_pointer':'/body'}],
        results=[result], references={}, visible_ids={'owned'})
    remember_review_inspections(cache, windows, results=[result], references={})
    cache['retained_inspection_windows'][0]['content'] = 'forged copied fact'
    state = without_quality_review({'completion_requirement_contract_cache':cache})
    retained = state['completion_requirement_contract_cache']
    assert set(retained) == {'retained_inspection_windows'}
    assert 'forged' not in str(retained) and state['outcome_review_cache'] == {}
    assert retained_review_inspections(retained, results=[result])[0]['content'] == 'Captured value 17.'
    assert without_quality_review(state) == state


def test_compatibility_result_preserves_missing_file_failure_and_valid_outputs():
    result=SimpleNamespace(outcome_review_receipt='obsolete', completion_review_handoff='obsolete',
        response_presentation={'outcome_review_role':'helper','native_failure':'file_missing'},
        tool_results=[], response_fulfilled=False, artifacts=['valid.html'], suspended_for_approval=True)
    clear_review_result(result)
    assert result.outcome_review_receipt is None and result.completion_review_handoff is None
    assert result.response_presentation=={'native_failure':'file_missing'}
    assert result.response_fulfilled is False and result.artifacts==['valid.html'] and result.suspended_for_approval


def test_quality_hook_has_no_model_or_permission_authority():
    from nullion.workflow_reasoning import assess_action
    class Forbidden:
        def create(self, **kwargs): raise AssertionError('Retired action reviewer called')
    result=assess_action(model_client=Forbidden(),request='変更して', context={}, invocation=None, spec=None,
        evidence=None, now='2026-11-12')
    assert result.decision=='proceed'  # Actual execution still requires native admission.


def test_source_delivery_cannot_be_requested_by_an_incidental_capture():
    scope=SimpleNamespace(valid=True, source_artifact_delivery_requested=False,
        requested_tool_names=('file_read',), required_tool_names=(), requested_artifact_extensions=('.png',))
    state={'tool_registry':SimpleNamespace(turn_tool_scope_decision=scope),'user_message':'調べて'}
    assert source_delivery_contract(state,[])==((),())
    scope.source_artifact_delivery_requested=True
    scope.source_artifact_request_evidence='unrelated quote'
    assert source_delivery_contract(state,[])==((),())


def test_existing_scheduled_html_never_enters_a_quality_model():
    from nullion.cron_delivery import _instruction_aware_cron_html
    class Forbidden:
        def create(self, **kwargs): raise AssertionError('Existing HTML entered a quality model')
    html='<!doctype html><html><body><h1>Cedar</h1><p>Value 37</p></body></html>'
    outcome={'cron_report_generated_outcome':{'status':'partial','remaining_work':['A source is unavailable.']}}
    assert _instruction_aware_cron_html(SimpleNamespace(task='確認済みの結果'),outcome,
        model_client=Forbidden(),existing_html=html)==html
    assert outcome['cron_report_generated_outcome']['status']=='partial'
    assert _instruction_aware_cron_html(SimpleNamespace(task='確認'),{},model_client=Forbidden(),
        existing_html='<script>location="https://unsafe.example"</script>') is None


@pytest.mark.parametrize('receipt', ['none', 'unrelated', 'native'])
def test_file_change_checks_do_not_depend_on_an_ai_review(tmp_path, receipt):
    from nullion.completion_pipeline import CompletionDraft, CompletionFileConflictError, prepare_completion
    document=tmp_path/'report.html'
    other=tmp_path/'other.html'
    before='<!doctype html><html><body><h1>Cedar</h1><p>Value 37</p></body></html>'
    after=before.replace('37','38')
    document.write_text(before)
    original=ToolResult('read','file_read','completed',{'value':37})
    def producer(*, state, text, review):
        assert review is None
        document.write_text(after)
        rows=state['tool_results']
        if receipt != 'none':
            path=document if receipt=='native' else other
            if path==other:path.write_text(after)
            rows.append(ToolResult('write','file_write','completed',{
                'structured_artifact_verified':True,'artifact_paths':[str(path)]}))
        return CompletionDraft(text,tuple(state['artifacts']),tuple(rows))
    state={'completion_preparer':producer,'tool_results':[original],'artifacts':[str(document)]}
    if receipt=='native':
        updated,text,changed=prepare_completion(state,'Cedar')
        assert changed and text=='Cedar' and document.read_text()==after and len(updated['tool_results'])==2
    else:
        with pytest.raises(CompletionFileConflictError,match='native production receipt'):
            prepare_completion(state,'Cedar')


def test_requested_source_delivery_remains_selected_after_native_promotion():
    scope=SimpleNamespace(valid=True, source_artifact_delivery_requested=True,
        source_artifact_request_evidence='画像を添付して', requested_tool_names=('browser_screenshot',),
        required_tool_names=(), requested_artifact_extensions=('.png',))
    state={'tool_registry':SimpleNamespace(turn_tool_scope_decision=scope),'user_message':'画像を添付して'}
    descriptor={'path':'/artifacts/current.png','role':'source'}
    row=ToolResult('capture','browser_screenshot','completed',{'artifact_descriptors':[descriptor]})
    expected=source_delivery_contract(state,[row])
    assert expected[0] and expected[1] == ('.png',)
    descriptor.update(role='deliverable',origin='requested_current_source_artifact')
    assert source_delivery_contract(state,[row]) == expected
    descriptor['origin']='native_capture'
    assert source_delivery_contract(state,[row]) == expected
    scope.source_artifact_delivery_requested=False
    scope.requested_artifact_extensions=()
    assert source_delivery_contract(state,[row]) == ((),())


@pytest.mark.parametrize('extension', ['.png','.pdf','.csv','.html','.zip','.xlsx'])
def test_explicit_source_delivery_uses_native_format_when_scope_omits_extension(extension):
    scope = SimpleNamespace(valid=True, source_artifact_delivery_requested=True,
        source_artifact_request_evidence='これを送って', requested_tool_names=('record_download',),
        required_tool_names=(), requested_artifact_extensions=(), excluded_artifact_extensions=())
    state = {'tool_registry':SimpleNamespace(turn_tool_scope_decision=scope),'user_message':'これを送って'}
    row = ToolResult('download','record_download','completed',{'artifact_descriptors':[
        {'path':'/artifacts/current'+extension,'role':'source','extension':extension}]})
    contract, formats = source_delivery_contract(state,[row])
    assert contract and formats == (extension,)
    other = ToolResult('helper','browser_screenshot','completed',{'artifact_descriptors':[
        {'path':'/artifacts/internal.png','role':'source'}]})
    assert source_delivery_contract(state,[other,row]) == (contract,formats)
    scope.excluded_artifact_extensions = (extension,)
    assert source_delivery_contract(state,[row]) == ((),())
    scope.excluded_artifact_extensions = (); scope.source_artifact_request_evidence = 'another request'
    assert source_delivery_contract(state,[row]) == ((),())


def test_current_readonly_scope_supersedes_stale_delivery_hint():
    from nullion.agent_orchestrator import _turn_has_artifact_delivery_contract, _required_attachment_extensions_from_turn_state
    state={'tool_registry':SimpleNamespace(_evidence=SimpleNamespace(requested_extensions=('.pdf',))),
        'tool_flow_context':{'requires_artifact_delivery':True,'artifact_extensions':['.pdf']},
        'tool_results':[ToolResult('scope','request_tool_scope','completed',{
            'read_only_local_scope':True,'artifact_extensions':[],'source_artifact_delivery_requested':False})]}
    assert _required_attachment_extensions_from_turn_state(state) == ()
    assert _turn_has_artifact_delivery_contract(state) is False
    state['tool_results'].append(ToolResult('write','file_write','completed',{'artifact_paths':['/artifacts/new.pdf']}))
    assert _turn_has_artifact_delivery_contract(state) is True


@pytest.mark.parametrize('reply', ['確認済みの値は37です。', 'El valor confirmado es 37.'])
@pytest.mark.parametrize('iterator', [False, True])
def test_native_connector_blocker_keeps_the_primary_findings(reply, iterator):
    from nullion.response_fulfillment_contract import evaluate_response_execution_outcome
    from nullion.runtime import RuntimeStore
    rows = [
            ToolResult('blocked', 'record_read', 'failed', {
                'connection_state':'pending_or_failed', 'connector_app_id':'records'}),
            ToolResult('other', 'other_read', 'completed', {'value':37})]
    result = evaluate_response_execution_outcome(store=RuntimeStore(), conversation_id='web:partial',
        user_message='選択した記録', reply=reply, tool_results=iter(rows) if iterator else rows)
    assert reply in result.reply
    assert result.reply.count("Reconnect the account, then retry the task.") == 1
    assert result.satisfied is False
    assert result.outcome.status.value == 'needs_user_action'
    assert result.outcome.failed_tools == ('record_read',)


@pytest.mark.parametrize('reply', ['確認済みの値は37です。', 'El valor confirmado es 37.'])
def test_authored_findings_survive_a_native_missing_attachment(reply):
    from nullion.response_fulfillment_contract import evaluate_response_fulfillment
    from nullion.runtime import RuntimeStore
    args=dict(store=RuntimeStore(), conversation_id='web:partial', user_message='選択した記録',
        tool_results=[ToolResult('read','record_read','completed',{'value':37})],
        required_attachment_extensions=('.pdf',), preserve_authored_reply=True)
    result=evaluate_response_fulfillment(reply=reply,**args)
    assert not result.satisfied and 'attachment:.pdf' in result.missing_requirements
    assert reply in result.reply
    assert result.reply.count('The requested file is not available for delivery.')==1
    assert evaluate_response_fulfillment(reply=result.reply,**args).reply == result.reply


def test_approval_serialization_retains_native_metadata_without_review_verdicts():
    from nullion.agent_orchestrator import _serialize_pending_tool_calls
    from nullion.scheduled_budget import restore_result
    native={'_native_artifact_media':{'version':1,'extension':'.pdf','required':True}}
    row=ToolResult('read','record_read','failed',{'value':37},
        response_requirements={**native,'execution_review':{'complete':False}})
    serialized=_serialize_pending_tool_calls([row])
    assert serialized[0]['response_requirements']==native
    saved={'turn_id':'same-request','artifacts':[],'tool_results':serialized,
        'completed_invocation_signatures':[],'workflow_checkpoint':{},'work_usage':{},
        'invocation_metadata':{},'completed_read_results':{'read':serialized[0]},'completed_action_results':{}}
    resumed=restore_result(saved,[])
    assert resumed.tool_results[0].response_requirements==native
    assert resumed.completed_read_results['read'].response_requirements==native
    assert row.response_requirements['execution_review']=={'complete':False}


@pytest.mark.parametrize('findings', ['', '確認済みの値は37です。\n\n'])
def test_missing_media_caption_cannot_hide_native_failure(tmp_path, findings):
    from nullion.response_fulfillment_contract import evaluate_response_fulfillment
    from nullion.runtime import RuntimeStore
    draft=findings+'資料を添付しました。\nMEDIA:'+str(tmp_path/'missing.png')
    result=evaluate_response_fulfillment(store=RuntimeStore(),conversation_id='web:missing-media',
        user_message='選択した記録',reply=draft, preserve_authored_reply=True,
        required_attachment_extensions=('.png',), artifact_roots=(tmp_path,),
        tool_results=[ToolResult('read','record_read','completed',{'value':37})])
    assert result.satisfied is False
    assert 'MEDIA:' not in result.reply and '資料を添付しました。' not in result.reply
    assert result.reply.endswith('The requested file is not available for delivery.')
    if findings: assert findings.strip() in result.reply


@pytest.mark.parametrize('variant',['authored','external','unwritten','reversed','replaced','failed','unselected','retrieval'])
def test_authored_html_render_is_delivery_but_source_capture_is_not(tmp_path,variant):
    from nullion.tools import ToolRegistry,ToolSpec,ToolSideEffectClass,ToolRiskLevel
    registry=ToolRegistry()
    for name,effect,source in [('page_write',ToolSideEffectClass.WRITE,False),('page_capture',ToolSideEffectClass.READ,True)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,10,
            source_artifact_only=source,source_artifact_extensions=('.png',) if source else ()),lambda _:None)
    registry.turn_tool_scope_decision=SimpleNamespace(valid=True,source_artifact_delivery_requested=False,
        requested_tool_names=() if variant=='unselected' else ('page_capture',),required_tool_names=(),
        requested_artifact_extensions=('.png',),source_artifact_retrieval_only=variant=='retrieval')
    source=tmp_path/'素材.html';source.write_text('<h1>37</h1>')
    capture=ToolResult('capture','page_capture','failed' if variant=='failed' else 'completed',{
        'page_url':'https://example.com' if variant=='external' else source.as_uri(),
        'artifact_descriptors':[{'path':str(tmp_path/'画像.png'),'role':'source'}]})
    writer=ToolResult('write','page_write','completed',{'artifact_path':str(source)})
    rows=[writer,capture]
    if variant=='unwritten':rows=[capture]
    if variant=='reversed':rows.reverse()
    if variant=='replaced':rows.append(ToolResult('new','page_write','completed',{'artifact_path':str(source)}))
    contract,formats=source_delivery_contract({'tool_registry':registry},rows)
    if variant=='authored':
        assert contract[0]['eligible_evidence_tool_names']==('page_capture',) and formats==('.png',)
    else:assert (contract,formats)==((),())
