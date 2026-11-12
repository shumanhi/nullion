import json
from dataclasses import replace
import pytest
from nullion.agent_orchestrator import (_CompletionReviewDecision as Review,
    _CompletionReviewSupportedFinding as Finding,_parse_completion_review_decision,
    _resolve_completion_review_references,_completion_review_open_task_reply)
from nullion.review_publication import render_supported_findings,finding_layout


def test_typed_layout_preserves_grouped_items_and_natural_paragraphs():
    rows=(Finding('短い概要。',('a',),'finding','paragraph'),
        Finding('Aoi + Ren\n173 cm / 183 cm',('a',),'finding','item','比較'),
        Finding('Luz + Sol\n164 cm / 未確認',('b',),'finding','item','比較'),
        Finding('Sol は未確認。',('b',),'limitation','paragraph','未確認'))
    reply=render_supported_findings(rows)
    assert reply=='短い概要。\n\n**比較**\n\n- Aoi + Ren\n  173 cm / 183 cm\n- Luz + Sol\n  164 cm / 未確認\n\n**未確認**\n\nSol は未確認。'
    review=Review('blocked',supported_findings=rows,runtime_failure_code='search_coverage_incomplete')
    assert _completion_review_open_task_reply(review)==reply
    assert review.disposition=='blocked'


@pytest.mark.parametrize('code',['deadline','network_error','action_denied','preflight_revision_stalled','scope_selection_stalled'])
def test_concrete_limitations_do_not_hide_runtime_failures(code):
    finding=Finding('A: 7.',('a',),'finding','item','Datos')
    limit=Finding('B no está confirmado.',('b',),'limitation','paragraph','Pendiente')
    review=Review('blocked',supported_findings=(finding,limit),runtime_failure_code=code)
    assert _completion_review_open_task_reply(review).startswith(render_supported_findings((finding,limit)))
    assert _completion_review_open_task_reply(review)!=render_supported_findings((finding,limit))


def test_legacy_and_explicit_prose_are_preserved_without_interpreting_punctuation():
    prose='One; two; three. 1. This is literal prose. 2. Keep it together.'
    assert render_supported_findings([Finding(prose,('a',))])==prose
    assert render_supported_findings([Finding(prose,('a',),'finding','paragraph')])==prose


@pytest.mark.parametrize('presentation,section',[(None,None),('wrong','x\ny'),([],{}),('item','x'*81)])
def test_malformed_layout_hints_cannot_discard_grounded_text(presentation,section):
    payload={'disposition':'blocked','unresolved_requirements':['private'],'retry_tool_names':[],
        'supported_findings':[{'text':'A: 7.','evidence_invocation_ids':['e001'],'presentation':presentation,'section':section}]}
    decision=_parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},registered_tool_names=set())
    assert decision is not None and decision.supported_findings[0].text=='A: 7.'
    resolved=_resolve_completion_review_references(decision,{'e001':'runtime-source'})
    assert resolved.supported_findings[0].evidence_invocation_ids==('runtime-source',)
    assert (resolved.supported_findings[0].presentation,resolved.supported_findings[0].section)==finding_layout(payload['supported_findings'][0])


def test_clarification_stays_numbered_after_structured_findings():
    review=Review('needs_user_input',supported_findings=(Finding('A: 7.',('a',),'finding','item','Datos'),),
        clarification_question='¿Cuál?',clarification_options=('Uno','Dos'))
    assert _completion_review_open_task_reply(review).endswith('¿Cuál?\n\n1. Uno\n2. Dos')


def test_unavailable_review_still_cannot_publish_layout_or_findings():
    review=Review('blocked',supported_findings=(Finding('Secret draft.',('a',),'finding','item','Heading'),),review_error='invalid_citations')
    reply=_completion_review_open_task_reply(review)
    assert 'Secret draft' not in reply and 'Heading' not in reply


@pytest.mark.parametrize('covered',[True,False])
def test_a_limitation_cannot_hide_a_different_missing_requirement(covered):
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement
    row=Finding('B remains unconfirmed.',('b',),'limitation','paragraph','Pending',('missing',) if covered else ('other',))
    review=Review('blocked',supported_findings=(row,),material_requirements=(Requirement('missing','Private diagnostic','fact',False,False),))
    result=_completion_review_open_task_reply(review)
    assert result.startswith('**Pending**\n\nB remains unconfirmed.')
    assert ('Some requested information remains unconfirmed.' in result) is (not covered)
    assert 'Private diagnostic' not in result


def test_public_limitations_bind_only_current_requirement_ids():
    from nullion.agent_orchestrator import _CompletionRequirementContractRow
    payload={'disposition':'blocked','all_material_requirements_enumerated':True,
        'unresolved_requirements':['Private diagnostic'],'retry_tool_names':[],
        'material_requirements':[{'requirement_id':'r','satisfied':False,'draft_covered':False,'evidence_invocation_ids':[]}],
        'supported_findings':[{'text':'The value remains unavailable.','kind':'limitation','presentation':'item',
            'evidence_invocation_ids':['e001'],'requirement_ids':['r','other',7]}]}
    decision=_parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},registered_tool_names=set(),
        requirement_contract=(_CompletionRequirementContractRow('r','Value','fact',('opaque_read',)),))
    assert decision is not None and decision.supported_findings[0].requirement_ids==('r',)
    resolved=_resolve_completion_review_references(decision,{'e001':'runtime-source'})
    assert resolved.supported_findings[0].requirement_ids==('r',)
    assert 'Some requested information' not in _completion_review_open_task_reply(resolved)
