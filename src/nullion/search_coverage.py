"""Request-owned discovery evidence and typed search stopping decisions.

No vendor lists, prompt matching, URL rewrites, network requests or model calls.
The model decides relevance and goal scope; runtime binds its claims to receipts.
"""
from collections.abc import Mapping
import hashlib
import json
from urllib.parse import urlsplit

MAX_CANDIDATES = 24
MAX_SOURCE_REFERENCES = 32
COVERAGE_PROTOCOL_ERRORS = frozenset({'invalid_coverage_assessment', 'constraint_set_mismatch', 'duplicate_candidate_decision'})


def _url(value):
    if not isinstance(value, str) or len(value) > 1000:
        return None
    try:
        parsed = urlsplit(value)
        return value if parsed.scheme in {'http', 'https'} and parsed.hostname and not parsed.username and not parsed.password else None
    except ValueError:
        return None


def _declared_source_content(output, *, observed_urls=None):
    """Recognize captured adapter records without interpreting their fields."""
    if output.get('result_kind') == 'source_identity_inventory':
        return False
    declarations = output.get('source_record_collections')
    if not isinstance(declarations, list):
        return False
    captured = False
    for declaration in declarations[:10]:
        if not isinstance(declaration, Mapping):
            continue
        path, identities = declaration.get('path'), declaration.get('identity_fields')
        if (not isinstance(path, list) or not 1 <= len(path) <= 4
                or not all(isinstance(key, str) and key for key in path)
                or not isinstance(identities, list) or not identities
                or not all(isinstance(key, str) and key for key in identities)):
            continue
        rows = output
        for key in path:
            rows = rows.get(key) if isinstance(rows, Mapping) else None
        cardinality = declaration.get('cardinality', 'many')
        if cardinality == 'one' and isinstance(rows, Mapping):
            rows = [rows]
        if cardinality not in {'one', 'many'} or not isinstance(rows, list):
            continue
        valid = [row for row in rows[:100] if isinstance(row, Mapping) and all(
            isinstance(row.get(key), (str, int, float)) and not isinstance(row.get(key), bool)
            and str(row[key]).strip() for key in identities)]
        if valid:
            captured = True
            # Only adapter-declared provenance identifies observed resources.
            # Arbitrary URLs in records remain links, not observation receipts.
            fields = declaration.get('observed_url_fields')
            if observed_urls is not None and isinstance(fields, list):
                for row in valid:
                    for field in fields[:8]:
                        url = _url(row.get(field)) if isinstance(field, str) else None
                        if url:
                            observed_urls.add(url)
    return captured


def discovery_ledger(results, *, previous=None):
    """Reconstruct from full retained receipts, never compacted model messages."""
    candidates, observations = {}, {}
    discovery_ids = list(previous.get('discovery_invocation_ids') or ()) if isinstance(previous, Mapping) else []
    truncated = bool(previous.get('truncated')) if isinstance(previous, Mapping) else False
    for row in (previous.get('candidates') or ()) if isinstance(previous, Mapping) else ():
        if isinstance(row, Mapping) and _url(row.get('url')) and len(candidates) < MAX_CANDIDATES:
            url = row['url']
            candidates[url] = {'candidate_id': 's-' + hashlib.sha256(url.encode()).hexdigest()[:16],
                'url': url, 'title': str(row.get('title') or '')[:120],
                'discovery_invocation_ids': list(row.get('discovery_invocation_ids') or ())[-MAX_SOURCE_REFERENCES:],
                'observation_invocation_ids': []}
    for result in results:
        value = (lambda key, default=None: result.get(key, default)) if isinstance(result, Mapping) else (lambda key, default=None: getattr(result, key, default))
        output = value('output')
        ident = value('invocation_id')
        if value('status') != 'completed' or value('error') or not ident or not isinstance(output, Mapping):
            continue
        # These are adapter-owned collection schemas, not prose classification.
        rows = output.get('source_candidates')
        if output.get('result_kind') == 'browser_item_collection':
            rows = output.get('items')
        elif output.get('result_kind') == 'public_search_results':
            # Native search uses results, not source_candidates. These are
            # discovery receipts only; snippets do not observe linked pages.
            rows = output.get('results')
        if isinstance(rows, list):
            if (any(isinstance(row, Mapping) and _url(row.get('canonical_url') or row.get('url')) for row in rows)
                    or output.get('source_discovery_complete') is True):
                if ident not in discovery_ids: discovery_ids.append(ident)
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                raw_url = row.get('canonical_url') or row.get('url')
                url = _url(raw_url)
                if isinstance(raw_url, str) and len(raw_url) > 1000:
                    truncated = True
                if not url:
                    continue
                if url not in candidates:
                    if len(candidates) >= MAX_CANDIDATES:
                        truncated = True
                        continue
                    candidates[url] = {'candidate_id': 's-' + hashlib.sha256(url.encode()).hexdigest()[:16],
                        'url': url, 'title': str(row.get('title') or row.get('name') or '')[:120],
                        'discovery_invocation_ids': [], 'observation_invocation_ids': []}
                if ident not in candidates[url]['discovery_invocation_ids']:
                    candidates[url]['discovery_invocation_ids'].append(ident)
        # A navigation receipt alone is not an observation of inventory.
        page = output.get('page') if isinstance(output.get('page'), Mapping) else {}
        record = output.get('record') if isinstance(output.get('record'), Mapping) else {}
        url = _url(output.get('page_url') or output.get('source_url') or page.get('url') or record.get('url'))
        captured = output.get('observations') if output.get('result_kind') == 'source_observations' else None
        # A collection observes its containing page, not every linked item.
        if isinstance(captured, list):
            for observation in captured:
                if not isinstance(observation, Mapping):
                    continue
                sources = output.get('sources')
                index = observation.get('source_index')
                source = (sources[index] if isinstance(sources, list) and type(index) is int
                    and 0 <= index < len(sources) and isinstance(sources[index], Mapping) else {})
                observed_url = _url(observation.get('source_url') or source.get('source_url')
                    or (url if source.get('page_url_ref') is True else None))
                if observed_url:
                    observations.setdefault(observed_url, []).append(ident)
        observed_urls = set()
        declared_content = _declared_source_content(output, observed_urls=observed_urls)
        for observed_url in sorted(observed_urls):
            observations.setdefault(observed_url, []).append(ident)
        if url and (output.get('result_kind') == 'browser_item_collection' and isinstance(output.get('items'), list)
                    or declared_content
                    or isinstance(output.get('text'), str) and output['text'].strip()
                    or isinstance(record.get('text'), str) and record['text'].strip()
                    or isinstance(output.get('main_document_text'), str) and output['main_document_text'].strip()):
            observations.setdefault(url, []).append(ident)
            canonical = _url(output.get('canonical_source_url'))
            if canonical:
                # Explicit adapter metadata may identify a canonical document;
                # its actual query/filter evidence still needs separate review.
                observations.setdefault(canonical, []).append(ident)
    if observations:
        for url, identities in observations.items():
            if url not in candidates:
                if len(candidates) >= MAX_CANDIDATES:
                    truncated = True
                    continue
                candidates[url] = {'candidate_id': 's-' + hashlib.sha256(url.encode()).hexdigest()[:16],
                    'url': url, 'title': '', 'discovery_invocation_ids': identities[-MAX_SOURCE_REFERENCES:],
                    'observation_invocation_ids': []}
    for url, row in candidates.items():
        row['discovery_invocation_ids'] = row['discovery_invocation_ids'][-MAX_SOURCE_REFERENCES:]
        row['observation_invocation_ids'] = list(dict.fromkeys(observations.get(url, ())))[-MAX_SOURCE_REFERENCES:]
    return {'candidates': list(candidates.values()), 'truncated': truncated,
        # Discovery can fill its bounded inventory before the winning page is
        # read. Keep current observed sources independently; truncation still
        # prevents exhaustive conclusions, not a verified find-any result.
        'observed_sources': [{'url': url, 'observation_invocation_ids': list(dict.fromkeys(ids))[-MAX_SOURCE_REFERENCES:]}
            for url, ids in list(observations.items())[-MAX_CANDIDATES:]],
        'discovery_invocation_ids': discovery_ids[-32:]} if candidates else None


def plan_schema(ledger=None):
    schema = {'type': 'object', 'additionalProperties': False,
        'required': ['scope', 'constraints', 'restricted_source_ids'], 'properties': {
            'scope': {'enum': ['restricted', 'find_any', 'comparison', 'comprehensive', 'evidence_summary'],
                'description': 'Use comparison for research on a finite set of identified subjects and their requested properties, even without ranking. Comprehensive requires exhaustive discovery in the original goal. Contextual references do not add independent subjects or properties.'},
            'source_discovery_required': {'type':'boolean',
                'description': 'Whether this goal requires finding additional source providers. False is valid for a bounded positive shortlist within an observed inventory; requested subject count and fields still require proof. It never waives wider discovery for broad negative conclusions.'},
            'comparison_mode': {'enum': ['bounded', 'exhaustive']},
            'source_blocker_alternative': {'type':'object','additionalProperties':False,
                'required':['request_quote'],'properties':{'request_quote':{'type':'string','minLength':3,'maxLength':500}},
                'description':'Only when the original request explicitly accepts reporting a concrete live-source blocker instead of the requested records. Quote that permission exactly; a tool failure alone does not grant it.'},
            'constraints': {'type': 'array', 'maxItems': 16, 'uniqueItems': True,
                'description': 'External result constraints only. Execution restrictions and artifact-delivery rules belong to material requirements and the runtime execution audit, not source filters.',
                'items': {'type': 'string', 'minLength': 1, 'maxLength': 300}},
            'restricted_source_ids': {'type': 'array', 'maxItems': MAX_CANDIDATES,
                'uniqueItems': True, 'items': {'type': 'string'}}}}
    if ledger is not None:
        identities = [row['candidate_id'] for row in ledger['candidates']]
        schema['properties']['restricted_source_ids']['items'] = {'enum': identities} if identities else False
    return schema


def validate_plan(value, ledger, *, request_text=None):
    from jsonschema import validate, ValidationError
    try:
        validate(value, plan_schema())
        known = {row['candidate_id'] for row in ledger['candidates']}
        selected = set(value['restricted_source_ids'])
        alternative = value.get('source_blocker_alternative')
        texts = [request_text] if isinstance(request_text, str) else request_text
        if alternative is not None and (not isinstance(texts, (list, tuple))
                or not any(isinstance(text, str) and alternative['request_quote'] in text for text in texts)):
            return False
        if value.get('comparison_mode') is not None and value['scope'] != 'comparison':
            return False
        if value['scope'] == 'comprehensive' and value.get('source_discovery_required') is False:
            return False
        return bool(selected <= known and (bool(selected) if value['scope'] == 'restricted'
            else True if value['scope'] == 'comparison' else not selected))
    except (ValidationError, TypeError, KeyError):
        return False


def constraint_definitions(plan):
    """Opaque request-owned identities; descriptions are display text only."""
    return [{'constraint_id': 'c-' + hashlib.sha256(value.encode()).hexdigest()[:16],
             'description': value} for value in plan['constraints']]


def review_context(ledger, plan):
    return {**ledger, 'plan': plan, 'constraint_definitions': constraint_definitions(plan)}


def assessment_schema(context, *, model_contract=False):
    ids = [row['candidate_id'] for row in context['candidates']]
    refs = {'type': 'array', 'uniqueItems': True, 'maxItems': 32, 'items': {'type': 'string'}}
    schema = {'type': 'object', 'additionalProperties': False,
        'required': ['conclusion', 'coverage_sufficient', 'scope_matches_request', 'discovery_evidence_invocation_ids', 'match_evidence_invocation_ids', 'candidates', 'constraints'],
        'properties': {
            'outcome_kind': {'enum': ['results', 'accepted_blocker']
                if context['plan'].get('source_blocker_alternative') else ['results'],
                'description':'Use accepted_blocker only for the explicitly permitted fallback, with limited conclusion, coverage_sufficient=false and current observed blocker proof. Never claim inventory or a broad absence from this fallback.'},
            'conclusion': {'enum': ['positive', 'negative', 'comparison', 'limited']},
            'coverage_sufficient': {'type': 'boolean'}, 'scope_matches_request': {'type': 'boolean'},
            'match_evidence_invocation_ids': refs, 'discovery_evidence_invocation_ids': refs,
            'comparison_scope': {'type':'object','additionalProperties':False,
                'required':['claim_extent','scope_disclosed','ranking_complete','selected_evidence_invocation_ids','frontier_candidate_ids'],
                'properties':{
                    'claim_extent':{'enum':['verified_options','exhaustive']},
                    'scope_disclosed':{'type':'boolean'},
                    'ranking_complete':{'type':'boolean','description':'The verified shortlist has been ranked and any evidence-backed material contender resolved. Unknown prices in incidental unvisited listings alone do not prevent this.'},
                    'selected_evidence_invocation_ids':{**refs,'minItems':1},
                    'frontier_candidate_ids':{'type':'array','uniqueItems':True,'maxItems':MAX_CANDIDATES,
                        'description':'Only discovered contenders with positive evidence of a potential ranking advantage or material constraint conflict. Mere existence or unknown fields are not such evidence. Named required subjects remain mandatory independently.',
                        'items':{'enum':ids} if ids else {'type':'string'}}}},
            'candidates': {'type': 'array', 'maxItems': MAX_CANDIDATES,
                'description': 'For bounded comparison/find-any/evidence-summary conclusions, emit decisions needed for selected/restricted sources and material frontier contenders. Other unvisited candidates remain pending without a prose row. Exhaustive or negative conclusions must account for every required candidate. Keep reasons concise; do not restate biographies, findings or the answer here.',
                'items': {
                'type': 'object', 'additionalProperties': False,
                'required': ['candidate_id', 'disposition', 'reason', 'evidence_invocation_ids'],
                'properties': {'candidate_id': {'enum': ids},
                    'disposition': {'enum': ['checked', 'irrelevant', 'pending', 'blocked']},
                    'reason': {'type': 'string', 'minLength': 1, 'maxLength': 500},
                    'evidence_invocation_ids': refs}}},
            'constraints': {'type': 'array', 'maxItems': 16, 'items': {
                'type': 'object', 'additionalProperties': False,
                'required': ['verified', 'evidence_invocation_ids'],
                'anyOf': [{'required': ['constraint_id']}, {'required': ['constraint']}],
                'properties': {'constraint_id': {'enum': [row['constraint_id'] for row in constraint_definitions(context['plan'])]},
                    'constraint': {'type': 'string'}, 'verified': {'type': 'boolean'},
                    'evidence_invocation_ids': refs}}}}}
    if context['plan'].get('comparison_mode') == 'bounded':
        schema['allOf'] = [{'if':{'properties':{'conclusion':{'enum':['positive','comparison']}}},
            'then':{'required':['comparison_scope']}}]
    if model_contract:
        schema['properties']['constraints']['items']['allOf'] = [{
            'if': {'properties': {'verified': {'const': True}}},
            'then': {'properties': {'evidence_invocation_ids': {'minItems': 1}}}}]
        # Make impossible observation claims unavailable in the model's schema.
        # Runtime assessment retains its specific binding diagnostics and its
        # handling of ancillary mistakes without certifying those candidates.
        unobserved = [row['candidate_id'] for row in context['candidates']
            if not row['observation_invocation_ids']]
        if unobserved:
            schema['properties']['candidates']['items']['allOf'] = [{
                'if': {'properties': {'candidate_id': {'enum': unobserved}}},
                'then': {'properties': {'disposition': {'enum': ['irrelevant', 'pending', 'blocked']}}}}]
        required = context['plan']['restricted_source_ids'] or ids
        # A complete negative/exhaustive assessment must account for its own
        # selected inventory. Omitting rows is a protocol mistake, not proof
        # that another network read is needed. Positive find-any results and
        # bounded recommendations retain their existing smaller frontier.
        exhaustive = {'properties': {'coverage_sufficient': {'const': True}},
            'required': ['coverage_sufficient'], 'allOf': [
                {'properties': {'conclusion': {'enum': ['negative', 'comparison', 'positive']}}}]}
        if context['plan']['scope'] in {'find_any', 'evidence_summary'}:
            exhaustive['allOf'][0]['properties']['conclusion']['enum'] = ['negative']
        if context['plan'].get('comparison_mode') == 'bounded':
            exhaustive['allOf'][0]['properties']['conclusion']['enum'] = ['negative']
        if required:
            schema.setdefault('allOf', []).append({'if': exhaustive, 'then': {
                'properties': {'candidates': {'allOf': [
                    {'contains': {'properties': {'candidate_id': {'const': ident}},
                        'required': ['candidate_id']}} for ident in required]}}}})
    return schema


def assessment_result(value, *, context, positive_ids, references=None, evidence_parents=None):
    """Separate ancillary source diagnostics from material coverage gaps."""
    from jsonschema import validate, ValidationError
    references = references or {}
    try:
        validate(value, assessment_schema(context))
    except (ValidationError, TypeError):
        return {'errors': ['invalid_coverage_assessment'], 'diagnostics': []}
    def refs(row, key='evidence_invocation_ids'):
        return {references.get(ref, ref) for ref in row[key]}
    def observation_refs(identities):
        # Parent links are supplied only for runtime-created, fingerprint-bound
        # records. A model citation still has to be positive current proof.
        expanded = set(identities)
        pending = list(identities)
        while pending:
            parent = (evidence_parents or {}).get(pending.pop())
            if parent and parent not in expanded:
                expanded.add(parent)
                pending.append(parent)
        return expanded
    plan = context['plan']
    candidates = {row['candidate_id']: row for row in context['candidates']}
    discovery_ids = set(context.get('discovery_invocation_ids') or ())
    decisions = {row['candidate_id']: row for row in value['candidates']}
    problems = [] if value['scope_matches_request'] else ['search_scope_mismatch']
    diagnostics = []
    match_ids = refs(value, 'match_evidence_invocation_ids')
    observed_ids = {ident for row in candidates.values() for ident in row['observation_invocation_ids']}
    observed_ids.update(ident for row in context.get('observed_sources', ()) for ident in row['observation_invocation_ids'])
    if value.get('outcome_kind') == 'accepted_blocker':
        if (not context['plan'].get('source_blocker_alternative') or value['conclusion'] != 'limited'
                or value['coverage_sufficient'] or not match_ids or not match_ids <= positive_ids
                or not observation_refs(match_ids).intersection(observed_ids)):
            problems.append('unverified_accepted_source_blocker')
        return {'errors': problems, 'diagnostics': []}
    bounded_comparison = (plan['scope'] == 'comparison' and plan.get('comparison_mode') == 'bounded'
        and value['conclusion'] in {'comparison', 'positive'})
    comparison = value.get('comparison_scope') or {}
    selected_ids = refs(comparison, 'selected_evidence_invocation_ids') if comparison else set()
    frontier = set(comparison.get('frontier_candidate_ids', ()))
    selected_observed = bool(selected_ids and selected_ids <= positive_ids
        and all(observation_refs({identity}) & observed_ids for identity in selected_ids))
    bounded_proof = (bounded_comparison and value['scope_matches_request']
        and comparison.get('claim_extent') == 'verified_options' and selected_observed)
    if bounded_comparison:
        if not comparison:
            problems.append('comparison_scope_missing')
        else:
            if comparison['claim_extent'] != 'verified_options' or not comparison['scope_disclosed']:
                problems.append('comparison_scope_undisclosed')
            if not selected_observed:
                problems.append('unobserved_comparison_selection')
            if not comparison['ranking_complete']:
                problems.append('comparison_ranking_unresolved')
    bounded_match = (((plan['scope'] == 'find_any' and value['conclusion'] == 'positive' and value['coverage_sufficient'])
        or (plan['scope'] == 'evidence_summary' and value['conclusion'] == 'limited'))
        and value['scope_matches_request']
        and bool(observation_refs(match_ids) & observed_ids) and match_ids <= positive_ids) or bounded_proof
    if len(decisions) != len(value['candidates']):
        problems.append('duplicate_candidate_decision')
    checked = set()
    for ident, decision in decisions.items():
        evidence = refs(decision)
        candidate = candidates[ident]
        if decision['disposition'] == 'checked':
            if not evidence or not evidence <= positive_ids or not observation_refs(evidence).intersection(candidate['observation_invocation_ids']):
                (diagnostics if bounded_match and (not bounded_comparison or ident not in
                    frontier | set(plan['restricted_source_ids'])) else problems).append('unobserved_candidate')
            else:
                checked.add(ident)
        elif decision['disposition'] == 'irrelevant':
            if not evidence or not evidence <= positive_ids | discovery_ids or not observation_refs(evidence).intersection(candidate['discovery_invocation_ids']):
                (diagnostics if bounded_match and (not bounded_comparison or ident not in
                    frontier | set(plan['restricted_source_ids'])) else problems).append('ungrounded_candidate_exclusion')
    if bounded_comparison and any(ident not in checked and (ident not in decisions
            or decisions[ident]['disposition'] != 'irrelevant') for ident in frontier):
        problems.append('comparison_frontier_unchecked')
    definitions = constraint_definitions(plan)
    legacy_ids = {row['description']: row['constraint_id'] for row in definitions}
    # Old saved reviews may bind an unchanged description. New reviews bind ids;
    # paraphrases and translated display text never determine their identity.
    checks = {row.get('constraint_id') or legacy_ids.get(row.get('constraint')): row for row in value['constraints']}
    if len(checks) != len(value['constraints']) or set(checks) != {row['constraint_id'] for row in definitions}:
        problems.append('constraint_set_mismatch')
    if value['coverage_sufficient']:
        if any(not row['verified'] or not refs(row) or not refs(row) <= positive_ids for row in checks.values()):
            problems.append('unverified_search_constraints')
        if plan['scope'] == 'evidence_summary':
            # Source summaries are bounded by the requested observations, not
            # every link mentioned inside those sources. Material outcome rows
            # still prove every requested fact; no broad absence is certified.
            if value['conclusion'] != 'limited' or not refs(value, 'match_evidence_invocation_ids') or not refs(value, 'match_evidence_invocation_ids') <= positive_ids:
                problems.append('unverified_or_overbroad_source_summary')
        elif value['conclusion'] == 'positive' and plan['scope'] == 'find_any':
            if not match_ids or not match_ids <= positive_ids:
                problems.append('unverified_positive_match')
            elif not observation_refs(match_ids).intersection(observed_ids):
                problems.append('unobserved_positive_match')
        elif value['conclusion'] in {'negative', 'comparison', 'positive'}:
            required = (set(plan['restricted_source_ids']) | frontier if bounded_comparison
                else set(plan['restricted_source_ids']) or set(candidates))
            discovery_required = (not plan['restricted_source_ids'] and not (
                plan['scope'] == 'comparison' and value['conclusion'] in {'comparison','positive'}
                and plan.get('source_discovery_required') is False))
            if discovery_required:
                discovery = refs(value, 'discovery_evidence_invocation_ids')
                if not discovery or not discovery <= positive_ids | discovery_ids or not discovery.intersection(discovery_ids):
                    problems.append('broader_source_discovery_not_observed')
            if context['truncated'] and discovery_required and not bounded_comparison:
                problems.append('discovery_inventory_truncated')
            if any(ident not in checked and (ident not in decisions or decisions[ident]['disposition'] != 'irrelevant')
                   for ident in required):
                problems.append('relevant_alternatives_unchecked')
            if not checked and not (bounded_comparison and selected_observed):
                problems.append('no_observed_search_source')
        else:
            problems.append('limited_search_is_not_complete')
    return {'errors': list(dict.fromkeys(problems)), 'diagnostics': list(dict.fromkeys(diagnostics))}


def assess(value, *, context, positive_ids, references=None, evidence_parents=None):
    return assessment_result(value, context=context, positive_ids=positive_ids,
        references=references, evidence_parents=evidence_parents)['errors']


def compact_context(context):
    return json.dumps(context, ensure_ascii=False, separators=(',', ':'))


def state_ledger(state, results):
    checkpoint = state.get('workflow_checkpoint')
    previous = getattr(checkpoint, 'discovered_sources', None)
    snapshot = state.get('workflow_checkpoint_snapshot')
    if previous is None and isinstance(snapshot, Mapping):
        previous = snapshot.get('discovered_sources')
    return discovery_ledger(results, previous=previous)
