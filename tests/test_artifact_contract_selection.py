from pathlib import Path
import pytest
from nullion.artifact_lifecycle import select_delivery_paths
from nullion.tools import ToolInvocation, _build_terminal_exec_handler

@pytest.mark.parametrize('extension',['.pdf','.html','.csv','.xlsx','.png'])
def test_typed_formats_exclude_supporting_verification_files(tmp_path,extension):
    final=str(tmp_path/('output'+extension));support=str(tmp_path/'verification.txt')
    assert select_delivery_paths([final,support],[], '',required_extensions=[extension])==([final],False)
    # Explicitly requesting the extra format preserves ambiguity/selection.
    assert select_delivery_paths([final,support],[], '',required_extensions=[extension,'.txt'])==([],True)
    draft=str(tmp_path/('draft'+extension))
    assert select_delivery_paths([final,draft],[], '',required_extensions=[extension])==([],True)
    assert select_delivery_paths([final,support],[], 'MEDIA:'+support,required_extensions=[extension])==([final],False)


def test_plain_terminal_preserves_configured_cwd(tmp_path,monkeypatch):
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    result=_build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path])(
        ToolInvocation('cwd','terminal_exec','web:plain',{'command':'pwd'}))
    assert result.status=='completed' and result.output['stdout'].strip()==str(tmp_path)


@pytest.mark.parametrize('prompt',['ZIP con dos archivos internos.','内部ファイルを含むZIP。','A ZIP with two internal files.'])
def test_explicit_support_writer_does_not_change_final_archive_formats(tmp_path,prompt):
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision
    registry=ToolRegistry()
    registry.register(ToolSpec('file_write','Write',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),lambda inv:None)
    evidence=TurnToolEvidence(current_user_message=prompt,requested_extensions=('.zip',))
    decision=TurnToolScopeDecision(valid=True,requested_artifact_extensions=('.zip',),
        requested_tool_names=('file_write',),required_tool_names=('file_write',))
    scoped=ScopedTurnToolRegistry(registry,evidence=evidence,tool_scope_decision=decision)
    assert scoped.get_spec('file_write').name=='file_write'
    assert scoped.turn_tool_scope_decision.requested_artifact_extensions==('.zip',)
    # A ZIP request alone does not expose unrelated write tools automatically.
    implicit=ScopedTurnToolRegistry(registry,evidence=evidence,
        tool_scope_decision=TurnToolScopeDecision(valid=True,requested_artifact_extensions=('.zip',)))
    assert not implicit.can_invoke_tool('file_write')


@pytest.mark.parametrize('extension',['.html','.xlsx','.png'])
@pytest.mark.parametrize('required_only',[False,True])
def test_explicit_shell_remains_selected_for_media_artifact_repairs(extension,required_only):
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision
    registry=ToolRegistry()
    for name,effect in [('terminal_exec',ToolSideEffectClass.DANGEROUS_EXEC),
                        ('browser_image_collect',ToolSideEffectClass.READ),
                        ('spreadsheet_create',ToolSideEffectClass.WRITE),('file_write',ToolSideEffectClass.WRITE),
                        ('browser_screenshot',ToolSideEffectClass.READ)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,10),lambda invocation:None)
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(requested_extensions=(extension,)),
        tool_scope_decision=TurnToolScopeDecision(valid=True,requested_artifact_extensions=(extension,)))
    result,widened=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:media',{
        'capabilities':['web','local_shell'],'tool_names':[] if required_only else ['terminal_exec'],
        'required_tool_names':['terminal_exec'],'artifact_extensions':[extension],
        'embedded_media_artifact_extensions':[extension]}))
    assert 'terminal_exec' in result.output['available_tools']
    assert 'terminal_exec' in result.output['required_tool_names']
    assert 'terminal_exec' in {row['name'] for row in widened.list_tool_definitions()}


def test_completion_recovery_budget_is_bounded_even_with_large_global_limit(monkeypatch):
    import nullion.agent_orchestrator as agent
    from nullion.tools import ToolRegistry
    monkeypatch.setattr(agent, '_review_risky_agent_completion', lambda *args, **kwargs:
        agent._CompletionReviewDecision('retry', ('Repair the selected workbook cell.',), ('spreadsheet_create',)))
    state={'user_message':'表を修正してください。','conversation_id':'web:budget','principal_id':'web:budget',
        'tool_registry':ToolRegistry(),'runtime_store':None,'tool_results':[], 'artifacts':[],
        'messages':[], 'content':[{'type':'text','text':'Prepared the files.'}],
        'iterations':7,'max_iterations':60,'completion_review_count':0,'tool_flow_context':{'required_artifact_extensions':['.xlsx']}}
    update=agent._agent_turn_finalize_node(state)
    assert update['completion_review_count']==1
    assert update['max_iterations']==60
    assert update['completion_recovery_iteration_limit']==7+agent._COMPLETION_REVIEW_RECOVERY_MAX_ITERATIONS
    assert update['completion_review_unresolved_requirements']==['Repair the selected workbook cell.']
    terminal=agent._agent_turn_model_node({**state,**update,
        'iterations':update['completion_recovery_iteration_limit']})
    assert terminal['result'].response_fulfilled is not True
    assert terminal['result'].reached_iteration_limit is False, 'Repair exhaustion must not request more global budget'
    assert terminal['result'].response_presentation['outcome_review_role'] == 'helper'
    assert terminal['result'].response_fulfilled is not True


def test_explicit_selection_can_reselect_valid_covered_candidate(tmp_path):
    from nullion.tools import ToolResult
    first=str(tmp_path/'first.pdf');second=str(tmp_path/'second.pdf')
    results=[ToolResult('choice','file_write','completed',{'artifact_delivery_manifest':{
        'version':1,'candidate_paths':[first,second],'selected_paths':[second]}})]
    assert select_delivery_paths([first,second],results,'MEDIA:'+first,required_extensions=['.pdf'])==([first],False)


def test_staged_source_alias_cannot_choose_between_revisions_or_add_unknown_outputs(tmp_path):
    from nullion.tools import ToolResult
    from nullion.artifact_lifecycle import local_artifact_download_recovery
    source=tmp_path/'scratch.pdf';first=tmp_path/'one.pdf';second=tmp_path/'two.pdf'
    for path in (source,first,second):path.write_bytes(b'current bytes')
    def receipt(name,path):
        return ToolResult(name,'terminal_exec','completed',{'artifact_paths':[str(path)],'artifact_descriptors':[
            {'path':str(path),'source_path':str(source),'role':'deliverable'}]})
    one=receipt('first',first);two=receipt('second',second)
    assert select_delivery_paths([str(first)],[one],'MEDIA:'+str(source),required_extensions=['.pdf'])==([str(first)],False)
    assert select_delivery_paths([str(first),str(second)],[one,two],'MEDIA:'+str(source),required_extensions=['.pdf'])==([],True)
    assert select_delivery_paths([str(first)],[two],'MEDIA:'+str(source),required_extensions=['.pdf'])==([str(first)],False)
    invocation=ToolInvocation('download','file_download','web:alias',{'url':source.as_uri()})
    guarded=local_artifact_download_recovery(invocation,[one,two])
    assert guarded.output['reason']=='local_artifact_revision_selection_required'
    assert set(guarded.output['recovery']['candidate_paths'])=={str(first),str(second)}


def test_terminal_read_cannot_promote_recent_unchanged_files(tmp_path,monkeypatch):
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    unrelated=tmp_path/'unrelated.csv';unrelated.write_text('private,value\nkeep,12\n')
    invocation=ToolInvocation('read','terminal_exec','web:owned',{'command':'pwd'},
        flow_context={'requested_artifact_extensions':['.csv']})
    result=_build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path])(invocation)
    assert result.status=='completed' and not result.output.get('artifact_paths')
    assert unrelated.read_text()=='private,value\nkeep,12\n'


def test_source_image_replacement_needs_a_successful_source_receipt_and_adds_no_plain_download_obligation(tmp_path):
    from nullion.response_fulfillment_contract import _collected_browser_image_path_identities,artifact_media_required_extensions
    from nullion.tools import ToolResult
    first=str(tmp_path/'earlier.png');replacement=str(tmp_path/'replacement.png')
    collected=ToolResult('collect','browser_image_collect','completed',{'image_paths':[first],'saved_count':1})
    downloaded=ToolResult('download','file_download','completed',{'artifact_descriptors':[
        {'path':replacement,'role':'source','extension':'.png'}]})
    assert replacement in _collected_browser_image_path_identities([collected,downloaded])
    failed=ToolResult('failed','file_download','failed',downloaded.output)
    assert replacement not in _collected_browser_image_path_identities([collected,failed])
    unrelated=ToolResult('file','file_write','completed',{'artifact_path':str(tmp_path/'text-only.docx')})
    assert artifact_media_required_extensions([downloaded,unrelated])==()


def test_artifact_creation_can_retry_without_deletion_but_never_bypasses_approval(tmp_path):
    from nullion.artifact_lifecycle import artifact_deletion_recovery
    from nullion.tools import ToolRegistry
    target=tmp_path/'personal.csv';target.write_text('keep')
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    invocation=ToolInvocation('delete','terminal_exec','web:one',{'command':'rm -f '+str(target)},
        flow_context={'requested_artifact_extensions':['.zip'],'requires_new_artifact_content':True})
    first=artifact_deletion_recovery(invocation,[],registry=registry)
    assert first.status=='failed' and first.output['command_executed'] is False
    assert first.output['recovery']['approval_bypass'] is False
    assert artifact_deletion_recovery(invocation,[first],registry=registry) is None
    assert target.read_text()=='keep'
    plain=ToolInvocation('plain','terminal_exec','web:one',invocation.arguments)
    assert artifact_deletion_recovery(plain,[],registry=registry) is None


def test_source_step_handoff_is_bounded_and_excludes_non_source_state_and_credentials():
    from nullion.agent_orchestrator import _prior_source_step_context
    from nullion.tools import ToolResult
    source=ToolResult('source','any_external_adapter','completed',{
        'result_kind':'source_observations','report_status':'blocked','goal_status':'blocked',
        'page_url':'https://example.org/read','blocker':'HTTP 403','unknowns':['情報なし'],
        'private_internal_path':'/private/user/credentials','token':'super-secret-value',
        'sources':[{'url':'https://example.org/?token=secret-auth-value'},{'url':'https://user:pass@example.org/'}]})
    other=ToolResult('mail','account_adapter','completed',{'token':'secret-mail-payload'})
    data=_prior_source_step_context([source,other]);assert len(data)==1
    assert data[0]['source_urls']==['https://example.org/read']
    assert data[0]['blocker']=='HTTP 403' and data[0]['unknowns']==['情報なし']
    payload=__import__('json').dumps(data);assert '/private/' not in payload and 'secret-' not in payload and 'user:pass' not in payload
    assert len(__import__('json').dumps(_prior_source_step_context([source]*100),ensure_ascii=False))<=12100


def test_source_handoff_retains_public_search_candidates_as_discovery_only():
    from nullion.agent_orchestrator import _prior_source_step_context
    from nullion.tools import ToolInvocation,_build_web_search_handler
    handler=_build_web_search_handler(lambda query,limit:[{'url':'https://example.org/park','title':'公園',
        'snippet':'A public park source with travel details.'}])
    result=handler(ToolInvocation('search','web_search','web:source',{'query':'駅 公園'}))
    assert result.output['result_kind']=='public_search_results'
    context=_prior_source_step_context([result]);assert context[0]['report_status']=='discovery'
    assert context[0]['observations']==[]
    assert context[0]['source_candidates'][0]['url']=='https://example.org/park'


@pytest.mark.parametrize("url",["https://[", "https://[invalid]", "http:///missing-host", "https://example.org:invalid/", "https://user:pass@example.org/"])
def test_malformed_external_source_urls_cannot_crash_or_leak_into_handoff(url):
    from nullion.agent_orchestrator import _prior_source_step_context
    from nullion.tools import ToolResult
    source=ToolResult("source","external","completed",{"result_kind":"source_observations",
        "page_url":url,"observations":{},"unknowns":{},"sources":[{"url":"https://example.org/valid"}]})
    search=ToolResult("search","external_search","completed",{"result_kind":"public_search_results",
        "results":[{"url":url,"title":"bad"},{"url":"https://example.org/good","title":"valid"}]})
    context=_prior_source_step_context([source,search])
    assert context[0]["source_urls"]==["https://example.org/valid"] and context[0]["observations"]==[]
    assert [r["url"] for r in context[1]["source_candidates"]]==["https://example.org/good"]
