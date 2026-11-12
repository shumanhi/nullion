from copy import deepcopy

import pytest

from nullion.search_coverage import discovery_ledger, validate_plan, assess, MAX_CANDIDATES
from nullion.tools import ToolResult


def results(second=False):
    rows = [ToolResult('discover', 'opaque_discovery', 'completed', {'source_candidates': [
        {'url': 'https://first.example/item', 'title': 'Selected inventory'},
        {'url': 'https://second.example/item', 'title': '別の販売元'}]}),
        ToolResult('first', 'opaque_read', 'completed', {'source_url': 'https://first.example/item',
            'text': 'Completed scoped check; no matches.', 'count': 0})]
    if second:
        rows.append(ToolResult('second', 'opaque_read', 'completed', {'source_url': 'https://second.example/item',
            'text': 'Completed scoped check.', 'count': 0}))
    return rows


def context(second=False, scope='find_any'):
    ledger = discovery_ledger(results(second))
    return {**ledger, 'plan': {'scope': scope, 'constraints': ['選択した日時と地域'],
        'restricted_source_ids': [ledger['candidates'][0]['candidate_id']] if scope == 'restricted' else []}}


def assessment(ctx, *, conclusion='negative', complete=True):
    return {'conclusion': conclusion, 'coverage_sufficient': complete, 'scope_matches_request': True,
        'discovery_evidence_invocation_ids': ctx.get('discovery_invocation_ids', []),
        'match_evidence_invocation_ids': ['first'] if conclusion == 'positive' else [],
        'constraints': [{'constraint': c, 'verified': True, 'evidence_invocation_ids': ['first']}
                        for c in ctx['plan']['constraints']],
        'candidates': [{'candidate_id': row['candidate_id'],
            'disposition': 'checked' if row['observation_invocation_ids'] else 'pending',
            'reason': '観測済み' if row['observation_invocation_ids'] else '未確認',
            'evidence_invocation_ids': row['observation_invocation_ids'] or row['discovery_invocation_ids']}
            for row in ctx['candidates']]}


@pytest.mark.parametrize('scope', ['find_any', 'comparison', 'comprehensive'])
def test_one_source_negative_does_not_cover_unvisited_relevant_alternatives(scope):
    ctx = context(scope=scope)
    assert 'relevant_alternatives_unchecked' in assess(assessment(ctx), context=ctx, positive_ids={'discover','first'})


@pytest.mark.parametrize('scope', ['find_any', 'comparison', 'comprehensive', 'restricted'])
def test_completed_scoped_sources_can_establish_negative_without_arbitrary_site_count(scope):
    ctx = context(second=scope != 'restricted', scope=scope)
    assert assess(assessment(ctx), context=ctx, positive_ids={'discover','first','second'}) == []


def test_one_positive_match_satisfies_find_any_without_searching_every_vendor():
    ctx = context()
    assert assess(assessment(ctx, conclusion='positive'), context=ctx, positive_ids={'first','discover'}) == []


@pytest.mark.parametrize('problem', ['constraint_false', 'constraint_missing', 'constraint_wrong', 'unknown_match',
    'fake_checked', 'unknown_exclusion', 'scope_mismatch', 'duplicate_decision', 'limited_complete', 'truncated'])
def test_coverage_claim_requires_the_actual_typed_scope_and_receipts(problem):
    ctx = context(True); value = assessment(ctx)
    if problem == 'constraint_false': value['constraints'][0]['verified'] = False
    if problem == 'constraint_missing': value['constraints'] = []
    if problem == 'constraint_wrong': value['constraints'][0]['constraint'] = '別の条件'
    if problem == 'unknown_match': value.update(conclusion='positive', match_evidence_invocation_ids=['invented'])
    if problem == 'fake_checked': value['candidates'][1]['evidence_invocation_ids'] = ['first']
    if problem == 'unknown_exclusion': value['candidates'][1].update(disposition='irrelevant', evidence_invocation_ids=['invented'])
    if problem == 'duplicate_decision': value['candidates'].append(deepcopy(value['candidates'][0]))
    if problem == 'limited_complete': value['conclusion'] = 'limited'
    if problem == 'truncated': ctx['truncated'] = True
    if problem == 'scope_mismatch': value['scope_matches_request'] = False
    assert assess(value, context=ctx, positive_ids={'first','second','discover'})


def test_navigation_is_not_inventory_observation():
    rows = results()[:1] + [ToolResult('opened', 'opaque_read', 'completed', {'source_url': 'https://first.example/item',
        'navigation_receipt': {'status': 200}})]
    assert not any(row['observation_invocation_ids'] for row in discovery_ledger(rows)['candidates'])


@pytest.mark.parametrize('identity,url_field',[('asset','downloaded_url'),('資産','取得先')])
def test_declared_binary_provenance_observes_only_the_actual_returned_resource(identity,url_field):
    row=ToolResult('download','opaque_adapter','completed',{'records':[
        {identity:'owned-file',url_field:'https://cdn.example/actual.png','page_url':'https://unobserved.example/page'}],
        'source_record_collections':[{'path':['records'],'identity_fields':[identity],'observed_url_fields':[url_field]}]})
    ledger=discovery_ledger([row])
    assert ledger and ledger['observed_sources']==[{'url':'https://cdn.example/actual.png','observation_invocation_ids':['download']}]
    assert not any(item['url']=='https://unobserved.example/page' for item in ledger['candidates'])
    row.output['source_record_collections'][0].pop('observed_url_fields')
    assert discovery_ledger([row]) is None


@pytest.mark.parametrize('explicit',[False,True])
@pytest.mark.parametrize('conclusion',['comparison','positive'])
def test_finite_subject_comparison_does_not_require_alternative_provider_discovery(explicit,conclusion):
    ctx=context(True,scope='comparison');ctx['discovery_invocation_ids']=[]
    for row in ctx['candidates']:row['discovery_invocation_ids']=[]
    if explicit:ctx['plan']['source_discovery_required']=False
    value=assessment(ctx,conclusion=conclusion)
    errors=assess(value,context=ctx,positive_ids={'first','second'})
    assert ('broader_source_discovery_not_observed' in errors) is (not explicit)


@pytest.mark.parametrize('missing',['candidate','constraint','negative'])
def test_finite_comparison_still_requires_each_subject_and_requested_fact(missing):
    ctx=context(True,scope='comparison');ctx['plan']['source_discovery_required']=False
    ctx['discovery_invocation_ids']=[]
    value=assessment(ctx,conclusion='comparison')
    if missing=='candidate':
        value['candidates'][1].update(disposition='pending',evidence_invocation_ids=[])
    elif missing=='constraint':value['constraints'][0]['verified']=False
    else:value['conclusion']='negative'
    assert assess(value,context=ctx,positive_ids={'first','second'})


def test_comprehensive_scope_cannot_disable_discovery():
    ctx=context(True,scope='comprehensive');ctx['plan']['source_discovery_required']=False
    assert not validate_plan(ctx['plan'],ctx)


@pytest.mark.parametrize('title', ['IMAX', '在庫', 'Pasajes', 'خيارات'])
def test_relevance_is_model_selected_not_inferred_from_title_words(title):
    rows = results(); rows[0].output['source_candidates'][1]['title'] = title
    ctx = context(); ctx.update(discovery_ledger(rows))
    value = assessment(ctx); value['candidates'][1].update(disposition='irrelevant', reason='Different requested subject',
        evidence_invocation_ids=['discover'])
    assert assess(value, context=ctx, positive_ids={'discover','first'}) == []


def test_request_checkpoint_retains_discovered_alternatives_but_not_old_observation_authority(tmp_path):
    from nullion.workflow_recovery import WorkflowCheckpoint, WorkflowCheckpointStore
    from nullion.tools import ToolRegistry
    cp = WorkflowCheckpoint('task', 'chat', 'goal'); cp.observe(results(), {}, ToolRegistry())
    restored = WorkflowCheckpoint.restore(cp.to_dict(), workflow_id='task', conversation_id='chat', goal_digest='goal')
    assert restored.discovered_sources == cp.discovered_sources
    ledger = discovery_ledger([], previous=restored.discovered_sources)
    assert len(ledger['candidates']) == 2 and not any(row['observation_invocation_ids'] for row in ledger['candidates'])
    assert not WorkflowCheckpoint.restore(cp.to_dict(), workflow_id='other', conversation_id='chat', goal_digest='goal').discovered_sources


def test_discovery_is_bounded_deduplicated_and_rejects_local_or_credential_urls():
    rows = results()[:1]
    rows[0].output['source_candidates'] *= 2
    rows[0].output['source_candidates'] += [{'url': u} for u in ['file:///private/state', 'https://user:pass@host/path']]
    assert len(discovery_ledger(rows)['candidates']) == 2
    rows[0].output['source_candidates'] += [{'url': f'https://site.example/{i}'} for i in range(MAX_CANDIDATES)]
    ledger = discovery_ledger(rows)
    assert len(ledger['candidates']) == MAX_CANDIDATES and ledger['truncated']


def test_coverage_plan_only_accepts_runtime_candidate_ids():
    ctx = context()
    assert validate_plan(ctx['plan'], ctx)
    assert not validate_plan({'scope': 'restricted', 'constraints': [], 'restricted_source_ids': ['invented']}, ctx)
    assert not validate_plan(None, ctx)


def test_native_browser_collection_preserves_format_specific_entries():
    row = ToolResult('links', 'browser_extract_items', 'completed', {'result_kind': 'browser_item_collection',
        'items': [{'canonical_url': 'https://tickets.example/movie', 'title': 'Movie'},
                  {'canonical_url': 'https://tickets.example/movie-special', 'title': '特別形式'}]})
    ledger = discovery_ledger([row])
    assert len(ledger['candidates']) == 2
    assert not any(r['observation_invocation_ids'] for r in ledger['candidates'])


def test_adapter_canonical_identity_does_not_certify_date_or_other_filters():
    rows = results()[:1]
    rows.append(ToolResult('query', 'opaque_read', 'completed', {
        'source_url': 'https://first.example/item?date=other',
        'canonical_source_url': 'https://first.example/item', 'text': 'Different requested date.'}))
    ledger = discovery_ledger(rows)
    assert ledger['candidates'][0]['observation_invocation_ids'] == ['query']
    ctx = {**ledger, 'plan': {'scope': 'restricted', 'constraints': ['requested date'],
        'restricted_source_ids': [ledger['candidates'][0]['candidate_id']]}}
    value = assessment(ctx); value['constraints'][0]['verified'] = False
    assert 'unverified_search_constraints' in assess(value, context=ctx, positive_ids={'query', 'discover'})


def test_query_and_fragment_scopes_are_never_silently_merged():
    row = ToolResult('links', 'opaque_discovery', 'completed', {'source_candidates': [
        {'url': 'https://inventory.example/search?id=1'}, {'url': 'https://inventory.example/search?id=2'},
        {'url': 'https://inventory.example/search#format-a'}, {'url': 'https://inventory.example/search#format-b'}]})
    assert len(discovery_ledger([row])['candidates']) == 4




def test_bounded_observation_report_does_not_need_every_incidental_link_read():
    ctx = context(); ctx['plan']['scope'] = 'evidence_summary'; ctx['truncated'] = True
    value = assessment(ctx, conclusion='limited'); value['match_evidence_invocation_ids'] = ['first']
    assert assess(value, context=ctx, positive_ids={'first', 'discover'}) == []
    value['conclusion'] = 'negative'
    assert 'unverified_or_overbroad_source_summary' in assess(value, context=ctx, positive_ids={'first', 'discover'})
    value.update(conclusion='limited', scope_matches_request=False)
    assert 'search_scope_mismatch' in assess(value, context=ctx, positive_ids={'first', 'discover'})


def test_direct_single_source_negative_needs_discovery_for_an_open_goal():
    ledger = discovery_ledger(results()[1:])
    ctx = {**ledger, 'plan': {'scope': 'find_any', 'constraints': [], 'restricted_source_ids': []}}
    assert len(ctx['candidates']) == 1 and not ctx['discovery_invocation_ids']
    value = assessment(ctx)
    assert 'broader_source_discovery_not_observed' in assess(value, context=ctx, positive_ids={'first'})
    ctx['plan'].update(scope='restricted', restricted_source_ids=[ctx['candidates'][0]['candidate_id']])
    assert assess(value, context=ctx, positive_ids={'first'}) == []


def test_explicit_comparison_sources_do_not_require_extra_catalog_discovery():
    ledger = discovery_ledger(results(True)[1:])
    ctx = {**ledger, 'plan': {'scope': 'comparison', 'constraints': [],
        'restricted_source_ids': [r['candidate_id'] for r in ledger['candidates']]}}
    value = assessment(ctx, conclusion='comparison')
    assert validate_plan(ctx['plan'], ctx)
    assert assess(value, context=ctx, positive_ids={'first','second'}) == []


def test_over_budget_discovered_url_is_explicitly_incomplete_not_silently_ignored():
    rows = results()
    rows[0].output['source_candidates'].append({'url': 'https://catalog.example/item?token='+'x'*1200})
    ledger = discovery_ledger(rows)
    assert ledger['truncated']
    ctx = {**ledger, 'plan': {'scope': 'find_any', 'constraints': [], 'restricted_source_ids': []}}
    assert 'discovery_inventory_truncated' in assess(assessment(ctx), context=ctx, positive_ids={'discover','first'})


def test_recovery_receives_typed_coverage_errors_and_candidate_decisions():
    from nullion.agent_orchestrator import _CompletionReviewDecision, _completion_review_retry_nudge
    from nullion.cron_delivery import _cron_outcome_review_feedback
    value = {'conclusion': 'negative', 'candidates': [{'candidate_id': 's-selected', 'disposition': 'pending',
        'reason': '未確認'}]}
    decision = _CompletionReviewDecision('retry', ('対象を確認',), search_coverage=value,
        search_coverage_errors=('unobserved_candidate',), runtime_failure_code='search_coverage_incomplete')
    assert 'unobserved_candidate' in _completion_review_retry_nudge(decision)
    assert 's-selected' in _completion_review_retry_nudge(decision)
    feedback = _cron_outcome_review_feedback(decision)
    assert feedback['search_coverage'] == value and feedback['search_coverage_errors'] == ['unobserved_candidate']


@pytest.mark.parametrize('description', ['La fecha y el formato solicitados.', '指定した日の対象形式。', 'الموعد والنوع المطلوبان'])
def test_constraint_identity_survives_paraphrased_or_translated_display_text(description):
    from nullion.search_coverage import constraint_definitions
    ctx = context(); value = assessment(ctx, conclusion='positive')
    value['constraints'][0].update(constraint_id=constraint_definitions(ctx['plan'])[0]['constraint_id'], constraint=description)
    assert assess(value, context=ctx, positive_ids={'first','discover'}) == []


@pytest.mark.parametrize('mutation', ['missing', 'duplicate', 'foreign', 'unverified', 'foreign_proof'])
def test_constraint_ids_do_not_remove_material_requirements(mutation):
    from nullion.search_coverage import constraint_definitions
    ctx=context(); value=assessment(ctx, conclusion='positive')
    row=value['constraints'][0]; row['constraint_id']=constraint_definitions(ctx['plan'])[0]['constraint_id']
    if mutation=='missing': value['constraints']=[]
    if mutation=='duplicate': value['constraints'].append(deepcopy(row))
    if mutation=='foreign': row['constraint_id']='c-unowned'
    if mutation=='unverified': row['verified']=False
    if mutation=='foreign_proof': row['evidence_invocation_ids']=['another-turn']
    assert assess(value, context=ctx, positive_ids={'first','discover'})


@pytest.mark.parametrize('scope', ['find_any','comparison','comprehensive','restricted'])
def test_observed_positive_variant_does_not_require_optional_discovery_alias_to_be_read(scope):
    from nullion.search_coverage import assessment_result
    rows=results()[:1]+[ToolResult('filtered', 'opaque_read', 'completed',
        {'source_url':'https://first.example/item?date=2030-01-01', 'text':'対象は7です。'})]
    ledger=discovery_ledger(rows)
    ctx={**ledger,'plan':{'scope':scope,'constraints':[],
        'restricted_source_ids':[ledger['candidates'][0]['candidate_id']] if scope=='restricted' else []}}
    value=assessment(ctx,conclusion='positive'); value['match_evidence_invocation_ids']=['filtered']
    value['candidates'][0].update(disposition='checked',evidence_invocation_ids=['filtered'])
    outcome=assessment_result(value,context=ctx,positive_ids={'discover','filtered'})
    if scope=='find_any':
        assert outcome['errors']==[] and outcome['diagnostics']==['unobserved_candidate']
    else: assert 'unobserved_candidate' in outcome['errors']


def test_positive_match_requires_a_source_read_not_just_an_unrelated_positive_receipt():
    ctx=context(); value=assessment(ctx,conclusion='positive')
    value['match_evidence_invocation_ids']=['discover']
    assert 'unobserved_positive_match' in assess(value,context=ctx,positive_ids={'first','discover'})


def test_structured_item_read_observes_only_its_container_including_empty_results():
    for items in [[], [{'url':'https://second.example/linked'}]]:
        ledger=discovery_ledger([ToolResult('items','opaque_read','completed',{
            'result_kind':'browser_item_collection','page':{'url':'https://first.example/catalog?date=2030'},'items':items})])
        rows={r['url']:r for r in ledger['candidates']}
        assert rows['https://first.example/catalog?date=2030']['observation_invocation_ids']==['items']
        if items: assert not rows['https://second.example/linked']['observation_invocation_ids']


def test_multi_page_observations_and_nested_record_bind_each_actual_source():
    ledger=discovery_ledger([ToolResult('multi','opaque_read','completed',{'result_kind':'source_observations',
        'page_url':'https://last.example/doc','observations':[
            {'source_url':'https://first.example/doc','quote':'確認7'},
            {'source_url':'https://last.example/doc','quote':'確認8'}]}),
        ToolResult('record','opaque_read','completed',{'record':{'url':'https://nested.example/doc','text':'確認9'}})])
    assert {row['url'] for row in ledger['candidates']}=={
        'https://first.example/doc','https://last.example/doc','https://nested.example/doc'}
    assert all(row['observation_invocation_ids'] for row in ledger['candidates'])


def test_empty_agent_observation_does_not_prove_a_page_was_read():
    assert discovery_ledger([ToolResult('empty','opaque_read','completed',{
        'result_kind':'source_observations','page_url':'https://source.example/doc','observations':[]})]) is None


def test_earlier_valid_read_is_not_lost_after_two_followup_reads():
    rows=results()+[ToolResult('later'+str(i),'opaque_read','completed',{
        'source_url':'https://first.example/item','text':'確認'+str(i)}) for i in range(3)]
    ledger=discovery_ledger(rows)
    assert 'first' in ledger['candidates'][0]['observation_invocation_ids']


def test_find_any_match_survives_bounded_discovery_inventory():
    rows=[ToolResult('discover','opaque_discovery','completed',{'source_candidates':[
        {'url':'https://source.example/'+str(i)} for i in range(MAX_CANDIDATES)]}),
        ToolResult('winner','opaque_read','completed',{'source_url':'https://winner.example/exact?filter=7','text':'確認7'})]
    ctx={**discovery_ledger(rows),'plan':{'scope':'find_any','constraints':[],'restricted_source_ids':[]}}
    value=assessment(ctx,conclusion='positive'); value['match_evidence_invocation_ids']=['winner']
    assert ctx['truncated'] and len(ctx['candidates'])==MAX_CANDIDATES
    assert assess(value,context=ctx,positive_ids={'discover','winner'})==[]
    ctx['plan']['scope']='comprehensive'
    assert 'discovery_inventory_truncated' in assess(value,context=ctx,positive_ids={'discover','winner'})


def test_observation_wrapper_does_not_certify_an_unobserved_last_page():
    ledger=discovery_ledger([ToolResult('multi','opaque_read','completed',{
        'result_kind':'source_observations','page_url':'https://footer.example/metadata',
        'observations':[{'source_url':'https://actual.example/inventory','quote':'確認7'}]})])
    assert [row['url'] for row in ledger['candidates']]==['https://actual.example/inventory']


@pytest.mark.parametrize('code,notice',[
    ('search_scope_mismatch','full requested scope'),
    ('unverified_search_constraints','search filters remain unverified'),
    ('unobserved_positive_match','proposed match could not be confirmed'),
    ('relevant_alternatives_unchecked','Some requested source checks remain unfinished'),
])
def test_real_search_gaps_publish_a_specific_typed_limitation_without_reviewer_prose(code,notice):
    from nullion.agent_orchestrator import _CompletionReviewDecision, _CompletionReviewSupportedFinding
    from nullion.review_publication import render_incomplete_review
    decision=_CompletionReviewDecision('retry',('private recovery instructions',),
        runtime_failure_code='search_coverage_incomplete',search_coverage_errors=(code,),
        supported_findings=(_CompletionReviewSupportedFinding('確認済み: 7。',('source',)),))
    rendered=render_incomplete_review(decision)
    assert '確認済み: 7。' in rendered and notice in rendered
    assert 'private recovery instructions' not in rendered and 'Some requested work remains incomplete' not in rendered


def test_compacted_multi_page_sources_keep_typed_observation_bindings():
    from nullion.source_observation_contract import compact_source_observations
    raw={'result_kind':'source_observations','report_status':'completed','page_url':'https://last.example/page',
        'observations':[{'source_url':url,'label':'対象','value':'確認7','quote':'確認7'}
            for url in ['https://first.example/page','https://last.example/page']]}
    compact=compact_source_observations(raw)
    assert all('source_index' in row for row in compact['observations'])
    ledger=discovery_ledger([ToolResult('multi','opaque_read','completed',compact)])
    assert {row['url'] for row in ledger['candidates']}=={'https://first.example/page','https://last.example/page'}
    assert all(row['observation_invocation_ids']==['multi'] for row in ledger['candidates'])


def test_typed_discovery_receipt_proves_discovery_without_claiming_inventory_facts():
    ctx=context(second=True);value=assessment(ctx,conclusion='negative')
    # A structured URL inventory is discovery proof even when it contains no
    # generic fact text/count. It cannot satisfy requested filters or a match.
    assert assess(value,context=ctx,positive_ids={'first','second'})==[]
    value['constraints'][0]['evidence_invocation_ids']=['discover']
    assert 'unverified_search_constraints' in assess(value,context=ctx,positive_ids={'first','second'})


@pytest.mark.parametrize('error',['constraint_set_mismatch','invalid_coverage_assessment','duplicate_candidate_decision'])
def test_legacy_review_binding_failures_do_not_claim_requested_work_is_missing(error):
    from nullion.agent_orchestrator import _CompletionReviewDecision, _CompletionReviewSupportedFinding
    from nullion.review_publication import render_incomplete_review
    decision=_CompletionReviewDecision('retry',('private review correction',),
        runtime_failure_code='search_coverage_incomplete',search_coverage_errors=(error,),
        supported_findings=(_CompletionReviewSupportedFinding('確認済み: 7。',('source',)),))
    rendered=render_incomplete_review(decision)
    assert '確認済み: 7。' in rendered and 'final check is unconfirmed' in rendered
    assert 'work remains incomplete' not in rendered and 'filters remain unverified' not in rendered


@pytest.mark.parametrize('scope',['find_any','restricted','comparison','comprehensive'])
def test_runtime_bound_record_citations_resolve_to_the_actual_observed_parent(scope):
    ctx=context(second=True,scope=scope);value=assessment(ctx,conclusion='positive')
    for row in value['candidates']:
        row['evidence_invocation_ids']=[ident+'-record' for ident in row['evidence_invocation_ids']]
    value['match_evidence_invocation_ids']=['first-record']
    value['constraints'][0]['evidence_invocation_ids']=['first-record']
    positive={'first','second','discover','first-record','second-record'}
    assert assess(value,context=ctx,positive_ids=positive,
        evidence_parents={'first-record':'first','second-record':'second'})==[]
    assert assess(value,context=ctx,positive_ids=positive)


def test_unknown_citation_cannot_borrow_a_valid_parent_relationship():
    ctx=context();value=assessment(ctx,conclusion='positive')
    value['match_evidence_invocation_ids']=['unowned-record']
    assert 'unverified_positive_match' in assess(value,context=ctx,positive_ids={'first','discover'},
        evidence_parents={'unowned-record':'first'})


def test_bounded_summary_keeps_optional_alias_diagnostics_separate_from_partial_outcomes():
    from nullion.search_coverage import assessment_result
    ctx=context();ctx['plan']['scope']='evidence_summary'
    value=assessment(ctx,conclusion='limited',complete=False)
    value['match_evidence_invocation_ids']=['first']
    value['candidates'][1].update(disposition='checked',evidence_invocation_ids=['first'])
    outcome=assessment_result(value,context=ctx,positive_ids={'first','discover'})
    assert outcome=={'errors':[],'diagnostics':['unobserved_candidate']}
    assert value['coverage_sufficient'] is False


def test_model_schema_prevents_checked_claim_for_unobserved_candidate():
    from jsonschema import validate,ValidationError
    from nullion.search_coverage import assessment_schema
    ctx=context(scope='restricted');value=assessment(ctx)
    validate(value,assessment_schema(ctx,model_contract=True))
    value['candidates'][1].update(disposition='checked',evidence_invocation_ids=['first'])
    with pytest.raises(ValidationError):validate(value,assessment_schema(ctx,model_contract=True))
    # Runtime diagnostics remain precise for a provider that ignores the schema.
    assert 'unobserved_candidate' in assess(value,context=ctx,positive_ids={'first','discover'})


@pytest.mark.parametrize('disposition',['pending','irrelevant','blocked'])
def test_model_schema_keeps_unobserved_source_recovery_choices(disposition):
    from jsonschema import validate
    from nullion.search_coverage import assessment_schema
    ctx=context();value=assessment(ctx,complete=False)
    value['candidates'][1]['disposition']=disposition
    validate(value,assessment_schema(ctx,model_contract=True))


def test_model_schema_keeps_checked_for_real_observations():
    from jsonschema import validate
    from nullion.search_coverage import assessment_schema
    ctx=context(True)
    validate(assessment(ctx),assessment_schema(ctx,model_contract=True))


def test_model_plan_schema_binds_selected_sources_to_current_inventory():
    from jsonschema import validate, ValidationError
    from nullion.search_coverage import plan_schema
    ctx = context(scope='restricted')
    validate(ctx['plan'], plan_schema(ctx))
    invalid = deepcopy(ctx['plan']); invalid['restricted_source_ids'] = ['unowned-source']
    with pytest.raises(ValidationError): validate(invalid, plan_schema(ctx))


@pytest.mark.parametrize('scope', ['restricted', 'comprehensive', 'comparison', 'find_any'])
def test_model_complete_negative_cannot_omit_its_required_candidate(scope):
    from jsonschema import validate, ValidationError
    from nullion.search_coverage import assessment_schema
    ctx = context(True, scope=scope); value = assessment(ctx)
    validate(value, assessment_schema(ctx, model_contract=True))
    value['candidates'] = []
    with pytest.raises(ValidationError): validate(value, assessment_schema(ctx, model_contract=True))
    # A partial review can accurately request missing source work.
    value['coverage_sufficient'] = False
    validate(value, assessment_schema(ctx, model_contract=True))


def test_positive_find_any_model_schema_does_not_require_incidental_candidates():
    from jsonschema import validate
    from nullion.search_coverage import assessment_schema
    ctx = context(); value = assessment(ctx, conclusion='positive')
    value['candidates'] = value['candidates'][:1]
    validate(value, assessment_schema(ctx, model_contract=True))
    assert assess(value, context=ctx, positive_ids={'first','discover'}) == []


@pytest.mark.parametrize('verified', [False, True])
def test_model_constraint_verification_requires_a_citation_when_verified(verified):
    from jsonschema import validate, ValidationError
    from nullion.search_coverage import assessment_schema
    ctx=context(); value=assessment(ctx, complete=False)
    value['constraints'][0].update(verified=verified, evidence_invocation_ids=[])
    if verified:
        with pytest.raises(ValidationError): validate(value, assessment_schema(ctx,model_contract=True))
    else:
        validate(value, assessment_schema(ctx,model_contract=True))


@pytest.mark.parametrize('mutation',['none','no_permission','foreign_proof','navigation_only','certifies_inventory','broad_absence','wrong_scope'])
def test_accepted_blocker_requires_owned_permission_and_current_observed_proof(mutation):
    ctx=context(scope='restricted');ctx['plan']['source_blocker_alternative']={'request_quote':'明確な問題があれば説明してください'}
    value=assessment(ctx,conclusion='limited',complete=False)
    value.update(outcome_kind='accepted_blocker',match_evidence_invocation_ids=['first'])
    if mutation=='no_permission':ctx['plan'].pop('source_blocker_alternative')
    if mutation=='foreign_proof':value['match_evidence_invocation_ids']=['foreign']
    if mutation=='navigation_only':value['match_evidence_invocation_ids']=['discover']
    if mutation=='certifies_inventory':value['coverage_sufficient']=True
    if mutation=='broad_absence':value['conclusion']='negative'
    if mutation=='wrong_scope':value['scope_matches_request']=False
    errors=assess(value,context=ctx,positive_ids={'first','discover'})
    assert bool(errors) is (mutation!='none')


def test_blocker_permission_quote_must_belong_to_original_request():
    ctx=context(scope='restricted');plan=ctx['plan']
    quote='explica el bloqueo concreto'
    plan['source_blocker_alternative']={'request_quote':quote}
    assert validate_plan(plan,ctx,request_text='Busca las opciones o '+quote+'.')
    assert not validate_plan(plan,ctx,request_text='Busca dos opciones.')
    assert not validate_plan(plan,ctx)


@pytest.mark.parametrize('kind',['current','selected_parent','linked_parent','unlinked_background','assistant_quote'])
def test_fallback_permission_can_follow_owned_ancestry_but_not_background_or_assistant_text(kind):
    from nullion.agent_orchestrator import _completion_review_owned_user_requests
    quote='明確な問題があれば説明してください'
    payload={'original_request':quote if kind=='current' else '3'}
    if kind=='selected_parent':payload['selected_request_context']={'parent_turn_id':'parent','turns':[{'user':quote}]}
    if kind=='linked_parent':payload['request_context']={'parent_turn_id':'parent','linked_turns':[{'user':quote}]}
    if kind=='unlinked_background':payload['request_context']={'reference_kind':'conversation_background','linked_turns':[{'user':quote}]}
    if kind=='assistant_quote':payload['selected_request_context']={'parent_turn_id':'parent','turns':[{'user':'探してください','assistant':quote}]}
    ctx=context(scope='restricted');ctx['plan']['source_blocker_alternative']={'request_quote':quote}
    assert validate_plan(ctx['plan'],ctx,request_text=_completion_review_owned_user_requests(payload)) is (kind in {'current','selected_parent','linked_parent'})


@pytest.mark.parametrize('status',['completed','failed'])
def test_native_public_search_is_discovery_only_and_failed_search_is_not_proof(status):
    row=ToolResult('search','web_search',status,{'result_kind':'public_search_results','query':'候補',
        'results':[{'url':'https://first.example/item','title':'候補 A','snippet':'A reported item'},
                   {'url':'https://second.example/item','title':'候補 B','snippet':'Another item'},
                   {'url':'file:///private/state'},{'url':'https://user:secret@private.example/'}]})
    ledger=discovery_ledger([row])
    if status=='failed':assert ledger is None;return
    assert ledger['discovery_invocation_ids']==['search']
    assert [r['url'] for r in ledger['candidates']]==['https://first.example/item','https://second.example/item']
    assert all(r['discovery_invocation_ids']==['search'] and not r['observation_invocation_ids'] for r in ledger['candidates'])
    assert ledger['observed_sources']==[]


def test_untyped_results_are_not_implicitly_search_discovery():
    assert discovery_ledger([ToolResult('opaque','opaque','completed',{'results':[{'url':'https://source.example/'}]})]) is None
