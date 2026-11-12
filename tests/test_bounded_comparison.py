from copy import deepcopy
import pytest
from nullion.search_coverage import discovery_ledger,assessment_result,validate_plan,MAX_CANDIDATES
from nullion.tools import ToolResult


def fixture(extra=13,read_challenger=False):
    urls=['https://catalog.example/'+name for name in ['a','b','challenger',*[str(i) for i in range(extra-1)]]]
    results=[ToolResult('discover','opaque_discover','completed',{'source_candidates':[{'url':u} for u in urls]})]
    for ident,url in zip(['a','b','challenger'] if read_challenger else ['a','b'],urls):
        results.append(ToolResult(ident,'opaque_read','completed',{'source_url':url,'text':'Observed selected fields.'}))
    ledger=discovery_ledger(results)
    context={**ledger,'plan':{'scope':'comparison','comparison_mode':'bounded','constraints':['条件'],
        'restricted_source_ids':[]}}
    value={'conclusion':'comparison','coverage_sufficient':True,'scope_matches_request':True,
        'match_evidence_invocation_ids':['a','b'],'discovery_evidence_invocation_ids':['discover'],
        'constraints':[{'constraint':'条件','verified':True,'evidence_invocation_ids':['a','b']}],
        'comparison_scope':{'claim_extent':'verified_options','scope_disclosed':True,'ranking_complete':True,
            'selected_evidence_invocation_ids':['a','b'],'frontier_candidate_ids':[]},
        'candidates':[{'candidate_id':r['candidate_id'],'disposition':'checked' if r['observation_invocation_ids'] else 'pending',
            'reason':'観測済み' if r['observation_invocation_ids'] else '未確認',
            'evidence_invocation_ids':r['observation_invocation_ids'] or r['discovery_invocation_ids']} for r in ledger['candidates']]}
    return context,value,{'discover','a','b'}|({'challenger'} if read_challenger else set())


@pytest.mark.parametrize('extra',[1,13,MAX_CANDIDATES+3])
def test_scoped_observed_recommendation_does_not_certify_or_require_every_discovery(extra):
    context,value,positive=fixture(extra)
    before=deepcopy(value)
    assert validate_plan(context['plan'],context)
    assert assessment_result(value,context=context,positive_ids=positive)=={'errors':[],'diagnostics':[]}
    assert value==before and sum(r['disposition']=='pending' for r in value['candidates'])>0


@pytest.mark.parametrize('read',[False,True])
def test_declared_contender_must_be_resolved_before_ranking_is_complete(read):
    context,value,positive=fixture(read_challenger=read)
    value['comparison_scope']['frontier_candidate_ids']=[context['candidates'][2]['candidate_id']]
    errors=assessment_result(value,context=context,positive_ids=positive)['errors']
    assert ('comparison_frontier_unchecked' in errors) is (not read)


@pytest.mark.parametrize('case',['undisclosed','exhaustive_claim','unfinished_ranking','unknown_selection','discovery_selection',
    'unobserved_selection','wrong_constraint','missing_contract','wrong_scope','missing_discovery','requested_subject'])
def test_bounded_comparison_does_not_bypass_real_goal_or_proof_gaps(case):
    context,value,positive=fixture()
    summary=value['comparison_scope']
    if case=='undisclosed':summary['scope_disclosed']=False
    elif case=='exhaustive_claim':summary['claim_extent']='exhaustive'
    elif case=='unfinished_ranking':summary['ranking_complete']=False
    elif case=='unknown_selection':summary['selected_evidence_invocation_ids']=['foreign']
    elif case=='discovery_selection':summary['selected_evidence_invocation_ids']=['discover']
    elif case=='unobserved_selection':
        positive.add('unrelated');summary['selected_evidence_invocation_ids'].append('unrelated')
    elif case=='wrong_constraint':value['constraints'][0]['verified']=False
    elif case=='missing_contract':value.pop('comparison_scope')
    elif case=='wrong_scope':value['scope_matches_request']=False
    elif case=='missing_discovery':value['discovery_evidence_invocation_ids']=[]
    else:context['plan']['restricted_source_ids']=[context['candidates'][2]['candidate_id']]
    assert assessment_result(value,context=context,positive_ids=positive)['errors']


@pytest.mark.parametrize('scope,conclusion',[('comparison','negative'),('comprehensive','comparison'),('find_any','negative')])
def test_negative_and_exhaustive_conclusions_still_require_their_actual_coverage(scope,conclusion):
    context,value,positive=fixture();context['plan']['scope']=scope;value['conclusion']=conclusion
    assert 'relevant_alternatives_unchecked' in assessment_result(value,context=context,positive_ids=positive)['errors']
    if scope!='comparison':assert not validate_plan(context['plan'],context)


@pytest.mark.parametrize('frontier',[False,True])
def test_optional_discovery_alias_does_not_poison_verified_options_but_frontier_alias_must_bind(frontier):
    context,value,positive=fixture()
    row=value['candidates'][2];row.update(disposition='checked',evidence_invocation_ids=['a'])
    if frontier:value['comparison_scope']['frontier_candidate_ids']=[row['candidate_id']]
    result=assessment_result(value,context=context,positive_ids=positive)
    assert ('unobserved_candidate' in result['errors']) is frontier
    assert ('unobserved_candidate' in result['diagnostics']) is (not frontier)


def test_late_observed_winner_can_be_compared_after_discovery_inventory_fills():
    context,value,positive=fixture(MAX_CANDIDATES+3)
    context['observed_sources'].append({'url':'https://late.example/selected','observation_invocation_ids':['late']})
    positive.add('late');value['comparison_scope']['selected_evidence_invocation_ids']=['late','b']
    assert assessment_result(value,context=context,positive_ids=positive)['errors']==[]


@pytest.mark.parametrize('count',[1,2])
def test_public_gap_counts_shortlisted_unchecked_candidates_without_leaking_private_notes(count):
    from nullion.agent_orchestrator import _CompletionReviewDecision as Review, _CompletionReviewSupportedFinding as Finding
    from nullion.review_publication import render_incomplete_review
    context,value,_=fixture()
    value['comparison_scope']['frontier_candidate_ids']=[row['candidate_id'] for row in context['candidates'][2:2+count]]
    review=Review('blocked',runtime_failure_code='search_coverage_incomplete',
        search_coverage=value,search_coverage_errors=('comparison_frontier_unchecked',),
        unresolved_requirements=('Private recovery instructions',),
        supported_findings=(Finding('A: 9 cents/g.',('a',)),Finding('Only listed price fields were inspected.',('a',),'limitation')))
    reply=render_incomplete_review(review)
    assert 'A: 9 cents/g.' in reply and f'{count} shortlisted alternative' in reply
    assert 'Private recovery instructions' not in reply and 'alternative(s)' not in reply


def test_unresolved_ranking_does_not_invent_an_undiscovered_better_alternative():
    from nullion.agent_orchestrator import _CompletionReviewDecision as Review
    from nullion.review_publication import render_incomplete_review
    _,value,_=fixture()
    reply=render_incomplete_review(Review('blocked',runtime_failure_code='search_coverage_incomplete',
        search_coverage=value,search_coverage_errors=('comparison_ranking_unresolved',)))
    assert reply=='The ranking of the checked options is still unverified.'


@pytest.mark.parametrize('conclusion', ['comparison','positive','negative'])
def test_finite_positive_shortlist_can_omit_extra_provider_discovery_but_absence_cannot(conclusion):
    context,value,positive=fixture()
    context['plan']['source_discovery_required']=False
    context['discovery_invocation_ids']=[]
    value['discovery_evidence_invocation_ids']=[]
    value['conclusion']=conclusion
    errors=assessment_result(value,context=context,positive_ids=positive)['errors']
    assert ('broader_source_discovery_not_observed' in errors) is (conclusion=='negative')
    if conclusion!='negative': assert errors==[]
