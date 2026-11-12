from nullion.agent_orchestrator import (_completion_review_reference_packet,
    _resolve_completion_review_references,_CompletionReviewDecision,
    _CompletionReviewMaterialRequirement,_CompletionRequirementContractRow,
    _forced_completion_review_is_grounded)
from nullion.tools import ToolResult


def test_references_preserve_whole_runtime_id_and_do_not_mutate_evidence():
    ids=['orchestrator-'+str(i)*32+'/structured-record/'+str(i)*20 for i in range(3)]
    original=[{'invocation_id':v,'tool_name':'source','output':{'record':{'title':str(i)}}} for i,v in enumerate(ids)]
    packet,refs=_completion_review_reference_packet(original)
    assert [r['invocation_id'] for r in packet]==['e0001','e0002','e0003']
    assert [r['invocation_id'] for r in original]==ids
    decision=_CompletionReviewDecision('complete',all_material_requirements_enumerated=True,
        material_requirements=(_CompletionReviewMaterialRequirement('r1','fact','fact',True,True,('e0002',)),))
    resolved=_resolve_completion_review_references(decision,refs)
    assert resolved.material_requirements[0].evidence_invocation_ids==(ids[1],)
    assert _forced_completion_review_is_grounded(resolved,
        tool_results=[ToolResult(ids[1],'source','completed',{'text':'Cedar museum admission is $17.'})],
        requirement_contract=(_CompletionRequirementContractRow('r1','fact','fact',('source',),'cedar'),))


def test_unknown_or_spliced_reference_cannot_bypass_runtime_evidence_checks():
    real='orchestrator-'+'1'*32+'/structured-record/'+'2'*20
    _,refs=_completion_review_reference_packet([{'invocation_id':real}])
    contract=(_CompletionRequirementContractRow('r1','fact','fact',('source',),'cedar'),)
    for value,status in [('e9999','completed'),('e0001/structured-record/mixed','completed'),('e0001','failed')]:
        decision=_CompletionReviewDecision('complete',all_material_requirements_enumerated=True,
            material_requirements=(_CompletionReviewMaterialRequirement('r1','fact','fact',True,True,(value,)),))
        resolved=_resolve_completion_review_references(decision,refs)
        assert not _forced_completion_review_is_grounded(resolved,
            tool_results=[ToolResult(real,'source',status,{'text':'Cedar museum admission is $17.'})],requirement_contract=contract)


def test_short_existing_references_are_preserved():
    row={'invocation_id':'read-1','output':{}}
    assert _completion_review_reference_packet([row])==([row],{})


def test_generated_references_never_shadow_a_real_short_invocation_id():
    long_id='orchestrator-'+'3'*32+'/structured-record/'+'4'*20
    packet,refs=_completion_review_reference_packet([{'invocation_id':long_id},{'invocation_id':'e0001'}])
    assert packet[0]['invocation_id']=='e0002' and packet[1]['invocation_id']=='e0001'
    assert refs=={'e0002':long_id}
