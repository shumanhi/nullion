"""Real graph and SQLite; fake only external discovery, inventory and model."""
import json
import os
import statistics
import time

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def text(value):
    return text_response(value if isinstance(value, str) else json.dumps(value, ensure_ascii=False))


@pytest.mark.parametrize('prompt', ['条件に合う在庫を探してください。', 'Busca opciones que cumplan las condiciones.'])
@pytest.mark.parametrize('mode', ['alternate_match', 'all_negative', 'first_positive', 'restricted', 'wrong_filter', 'bad_scope', 'blocked_alternate', 'direct_negative', 'binding_repair', 'binding_omission'])
def test_search_coverage_controls_actual_continuation_and_visible_answer(tmp_path, monkeypatch, prompt, mode, api_surface=False, cron_surface=False, native_search=False):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(); reads = []; discoveries = []
    sources = ['https://source-a.example/inventory', 'https://source-b.example/別形式']
    def discover(inv):
        discoveries.append(inv)
        if native_search:
            from nullion.tools import _build_web_search_handler
            return _build_web_search_handler(lambda query,limit:[
                {'url':url,'title':f'候補 {i}','snippet':'A discovered inventory source.'}
                for i,url in enumerate(sources)])(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'source_candidates': [
            {'url': sources[0], 'title': 'Primary catalog'}, {'url': sources[1], 'title': '別形式の在庫'}],
            'text': 'Discovered two candidate source catalogs.', 'count': 2})
    def read(inv):
        source = inv.arguments['url']; reads.append(source)
        match = mode in {'first_positive','expanded_scope'} and source == sources[0] or mode in {'alternate_match', 'bad_scope', 'direct_negative'} and source == sources[1]
        if mode == 'blocked_alternate' and source == sources[1]:
            return ToolResult(inv.invocation_id, inv.tool_name, 'failed', {'source_url': source}, 'Alternate catalog unavailable')
        wrong = mode == 'wrong_filter' and reads.count(source) == 1
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'source_url': source,
            'text': 'Matching item: 7.' if match else 'No matching records for the applied filter.',
            'value': 7 if match else 0, 'filter_verified': not wrong})
    registry.register(ToolSpec('opaque_discover', 'Discover relevant source catalogs', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), discover)
    registry.register(ToolSpec('opaque_read', 'Read one selected source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, input_schema={'type':'object','properties':{'url':{'type':'string'}},'required':['url']}, capability_tags=('web',)), read)
    class Provider:
        plans = 0
        reviews = 0
        interrupted = False
        def create(self, **kw):
            system = kw.get('system') or ''
            names = {r['name'] for r in kw.get('tools') or ()}
            if api_surface:
                if 'select_attachment_format' in names:
                    return tool_response('select_attachment_format', {'extension': None, 'confidence': 1, 'embedded_media_extensions': []})
                if 'relationship' in system and 'target_index' in system:
                    return text({'relationship': 'independent', 'effect': 'none', 'confidence': 1, 'target_index': None})
                if 'Return JSON only for a single already-selected read-only tool' in system:
                    return text({'use_direct': False, 'arguments': {}})
                if 'request_tool_scope' in names and 'opaque_discover' not in names:
                    return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['opaque_discover', 'opaque_read']})
                if not names and not ('material-requirements planner' in system or 'semantic completion reviewer' in system):
                    return text({})
            if 'material-requirements planner' in system:
                self.plans += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                ledger = packet['discovered_sources']
                if mode in {'bad_scope','expanded_scope'} and self.plans > 1:
                    assert packet['scope_correction']['rejected_plan']['scope'] == ('restricted' if mode=='bad_scope' else 'comprehensive')
                return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'search_coverage': {'scope': 'comprehensive' if mode=='expanded_scope' and self.plans==1 else 'restricted' if mode in {'restricted','binding_repair','binding_omission'} or mode == 'bad_scope' and self.plans == 1 else 'find_any',
                        'constraints': ['選択された日時・地域・形式',*(['Unrequested contextual metadata'] if mode=='expanded_scope' and self.plans==1 else [])],
                        'restricted_source_ids': [ledger['candidates'][0]['candidate_id']] if mode in {'restricted','binding_repair','binding_omission'} or mode == 'bad_scope' and self.plans == 1 else []},
                    'material_requirements': [{'requirement': 'Find a qualifying record or correctly report scoped absence',
                        'requirement_kind': 'fact', 'evidence_subject_id': 'selected_inventory',
                        'eligible_evidence_tool_names': ['opaque_read']},*([{'requirement':'Authenticate unrelated contextual metadata','requirement_kind':'fact','evidence_subject_id':'context_only','eligible_evidence_tool_names':['opaque_read']}] if mode=='expanded_scope' and self.plans==1 else [])]})
            if 'semantic completion reviewer' in system:
                self.reviews += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                ctx = packet['search_coverage_context']
                records = [r for r in packet['tool_evidence'] if r['tool_name'] == 'opaque_read' and r['status'] == 'completed']
                selected = records[-1]['invocation_id']
                positive = mode in {'first_positive','expanded_scope'} or mode in {'alternate_match', 'bad_scope', 'direct_negative'} and sources[1] in reads
                blocked = mode == 'blocked_alternate' and sources[1] in reads
                filter_ok = mode != 'wrong_filter' or reads.count(sources[0]) >= 2
                candidate_rows = [{'candidate_id': r['candidate_id'],
                    'disposition': 'checked' if r['observation_invocation_ids'] else 'pending',
                    'reason': 'Read exact source' if r['observation_invocation_ids'] else 'Relevant alternate not read',
                    'evidence_invocation_ids': r['observation_invocation_ids'] or r['discovery_invocation_ids']}
                    for r in ctx['candidates']]
                if mode=='binding_repair':
                    alternate=candidate_rows[1]
                    alternate.update(disposition='checked' if self.reviews==1 else 'irrelevant',
                        evidence_invocation_ids=[selected] if self.reviews==1 else ctx['candidates'][1]['discovery_invocation_ids'],
                        reason='Observed' if self.reviews==1 else 'Outside the explicitly selected source')
                    if self.reviews>1:
                        assert 'unobserved_candidate' in packet['search_coverage_binding_validation_errors']
                if mode=='binding_omission':
                    if self.reviews==1:
                        candidate_rows=[]
                    else:
                        assert 'candidate_decision_set_incomplete' in packet['search_coverage_binding_validation_errors']
                        assert ctx['plan']['restricted_source_ids'][0] in packet['search_coverage_missing_candidate_ids']
                # Deliberately repeat the original premature completion mistake.
                # Runtime must reject this negative despite the model claiming complete.
                return text({'disposition': 'retry' if mode=='expanded_scope' and ctx['plan']['scope']=='comprehensive' else 'blocked' if blocked else 'complete',
                    'user_facing_failure': 'Alternate catalog is unavailable.' if blocked else None,
                    'unresolved_requirements': ['The review plan expands beyond the requested subjects.'] if mode=='expanded_scope' and ctx['plan']['scope']=='comprehensive' else ['Alternate coverage remains unknown.'] if blocked else [], 'retry_tool_names': [],
                    'supported_findings': [{'text': 'The primary catalog had no qualifying records.',
                        'evidence_invocation_ids': [selected]}, {'kind': 'limitation',
                        'text': 'The alternate catalog could not be checked.',
                        'evidence_invocation_ids': [next(r['invocation_id'] for r in packet['tool_evidence'] if r['status'] == 'failed')]}] if blocked else [],
                    'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement_id': r['requirement_id'], 'satisfied': not blocked and r['evidence_subject_id']!='context_only',
                        'draft_covered': r['evidence_subject_id']!='context_only', 'evidence_invocation_ids': [] if r['evidence_subject_id']=='context_only' else [selected]}
                        for r in packet['material_requirement_contract']],
                    'search_coverage': {'conclusion': 'limited' if blocked else 'positive' if positive else 'negative',
                        'coverage_sufficient': not blocked and not (mode=='expanded_scope' and ctx['plan']['scope']=='comprehensive'), 'scope_matches_request': not (mode == 'bad_scope' and ctx['plan']['scope'] == 'restricted' or mode=='expanded_scope' and ctx['plan']['scope']=='comprehensive'), 'discovery_evidence_invocation_ids': ctx.get('discovery_invocation_ids', []), 'match_evidence_invocation_ids': [selected] if positive else [],
                        'candidates': candidate_rows, 'constraints': [{'constraint': c, 'verified': filter_ok,
                            'evidence_invocation_ids': [selected]} for c in ctx['plan']['constraints']]}})
            if cron_surface and system.startswith('You repair scheduled-task final answers'):
                payload = kw['messages'][0]['content'][0]['text']
                records = json.JSONDecoder().raw_decode(payload.split('Verified compact tool evidence from this run:\n', 1)[1])[0]
                return text({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [r['evidence_id'] for r in records if r['status'] == 'completed'][-1:],
                    'report': 'The first catalog had no qualifying records.'})
            if cron_surface and reads and not self.reviews:
                self.interrupted = True
                raise TimeoutError('Synthetic interruption after first source')
            if mode == 'direct_negative' and not reads: return tool_response('opaque_read', {'url': sources[0]})
            if not discoveries: return tool_response('opaque_discover', {'query':'候補','limit':len(sources)} if native_search else {})
            if not reads: return tool_response('opaque_read', {'url': sources[0]})
            if self.reviews:
                prompt_text = json.dumps(kw.get('messages'), ensure_ascii=False)
                assert 'Retained discovered sources' in prompt_text and sources[1] in prompt_text
                if mode == 'wrong_filter' and reads.count(sources[0]) < 2:
                    return tool_response('opaque_read', {'url': sources[0]})
                if mode not in {'first_positive', 'expanded_scope', 'restricted', 'binding_repair', 'binding_omission'} and sources[1] not in reads:
                    return tool_response('opaque_read', {'url': sources[1]})
            return text('A qualifying option is available; its verified value is 7.' if mode in {'first_positive','expanded_scope'} or mode in {'alternate_match', 'bad_scope', 'direct_negative'} and sources[1] in reads
                else 'No matching records in the requested checked sources.')
    provider = Provider()
    if api_surface:
        from fastapi.testclient import TestClient
        from types import SimpleNamespace
        from nullion import chat_store, web_app
        for key, value in {'NULLION_ENV_FILE': str(tmp_path/'none.env'), 'NULLION_TASK_PLANNER_FEED_MODE': 'off',
            'NULLION_CHAT_STARTUP_WARMUP': '0', 'NULLION_ACTIVITY_TRACE_ENABLED': '0'}.items(): monkeypatch.setenv(key, value)
        monkeypatch.setattr(chat_store, '_store', None)
        runtime.model_client = provider
        with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=provider), registry=registry)) as client:
            response = client.post('/api/chat', json={'text': '/chat '+prompt, 'conversation_id': 'web:coverage'})
            assert response.status_code == 200, response.text
            data = response.json()
        result = SimpleNamespace(final_text=data['text'], response_fulfilled=data.get('response_fulfilled', True))
    elif cron_surface:
        from nullion.crons import CronJob
        from tests.e2e.test_scheduled_report_completion import run_scheduled, telegram_boundary
        deliveries, send = telegram_boundary(monkeypatch)
        job = CronJob(id='source-coverage', name='確認', task=prompt, schedule='* * * * *',
            delivery_channel='telegram', delivery_target='123')
        payload = run_scheduled(job, provider, runtime, registry, send)
        # Recovery can finish inside the producer graph before a separate
        # scheduler continuation is needed. Require the interrupted execution
        # and its real final source/delivery proof below.
        assert provider.interrupted, 'The scheduled source interruption was not exercised'
        assert payload['cron_source_verification_satisfied'] is True, payload
        assert len(deliveries) == 1 and 'verified value is 7' in deliveries[0]['text']
        assert reads == sources and len(discoveries) == 1
        events = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.completion_review')
        assert events[-1]['disposition'] == 'complete' and provider.reviews >= 2
        return
    else:
        result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:coverage', principal_id='web:coverage',
        user_message=prompt, conversation_history=[], tool_registry=registry, policy_store=runtime.store,
        approval_store=runtime.store, max_iterations=9)
    if mode == 'blocked_alternate':
        assert result.response_fulfilled is not True and reads == sources
        assert 'No matching records in the requested checked sources.' in result.final_text
        assert 'alternate catalog could not be checked' in result.final_text
        return
    assert result.response_fulfilled is True, result
    assert provider.plans == (2 if mode in {'bad_scope','expanded_scope'} else 1) and len(discoveries) == 1, json.dumps({'plans':provider.plans,'reviews':provider.reviews,'events':runtime.store.list_recent_conversation_events('web:coverage',event_type='conversation.completion_review'),'contracts':runtime.store.list_recent_conversation_events('web:coverage',event_type='conversation.completion_requirement_contract')},ensure_ascii=False)
    if mode in {'alternate_match', 'all_negative', 'bad_scope', 'direct_negative'}: assert reads == sources
    if mode == 'wrong_filter': assert reads == [sources[0], sources[0], sources[1]]
    if mode in {'first_positive', 'expanded_scope', 'restricted','binding_repair','binding_omission'}: assert reads == sources[:1] and provider.reviews == (2 if mode in {'expanded_scope','binding_repair','binding_omission'} else 1)
    assert ('verified value is 7' in result.final_text) is (mode in {'alternate_match', 'first_positive', 'expanded_scope', 'bad_scope', 'direct_negative'})
    events = runtime.store.list_recent_conversation_events('web:coverage', event_type='conversation.completion_review')
    if mode not in {'first_positive', 'expanded_scope', 'restricted', 'binding_repair', 'binding_omission'}:
        assert any(row.get('runtime_failure_code') == 'search_coverage_incomplete' for row in events)
    assert events[-1]['disposition'] == 'complete' and not events[-1]['search_coverage_errors']
    if mode=='expanded_scope':
        assert events[-1]['requirement_count']==1
        assert any('search_scope_mismatch' in row.get('search_coverage_errors',[]) for row in events)
    if not api_surface: assert result.workflow_checkpoint['discovered_sources']['candidates'][1]['url'] == sources[1]
    else:
        turn = next(r for r in reversed(runtime.store.list_conversation_events('web:coverage')) if r.get('event_type') == 'conversation.chat_turn')
        assert ('verified value is 7' in turn['assistant_reply']) is (mode in {'alternate_match','first_positive','expanded_scope','bad_scope','direct_negative'})
        assert any(r['tool_name'] == 'opaque_read' and r['status'] == 'completed' for r in turn['tool_results'])


def test_plain_turns_do_not_load_discovery_or_add_a_model_call(tmp_path, monkeypatch):
    import nullion.search_coverage as coverage
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    runtime = setup_runtime(tmp_path, monkeypatch); calls = []
    def forbidden(*args, **kwargs):
        time.sleep(.2)
        raise AssertionError('Plain chat loaded a discovery ledger')
    monkeypatch.setattr(coverage, 'discovery_ledger', forbidden)
    class Provider:
        def create(self, **kw): calls.append(kw); return text('こんにちは。')
    runner = AgentOrchestrator(model_client=Provider()); elapsed = []
    for prompt in ['こんにちは', 'Hola', 'Bonjour', '안녕', 'Hello']:
        started = time.monotonic()
        result = runner.run_turn(conversation_id='web:plain-coverage', principal_id='web:plain-coverage',
            user_message=prompt, conversation_history=[], tool_registry=ToolRegistry(), policy_store=runtime.store,
            approval_store=runtime.store)
        elapsed.append(time.monotonic()-started)
        assert result.final_text == 'こんにちは。'
    assert len(calls) == 5 and statistics.mean(elapsed) < .1 and max(elapsed) < .2


def test_real_web_chat_route_publishes_the_alternate_match_with_runtime_proof(tmp_path, monkeypatch):
    test_search_coverage_controls_actual_continuation_and_visible_answer(tmp_path, monkeypatch,
        '条件に合う対象を確認してください。', 'alternate_match', api_surface=True)


def test_real_scheduled_run_resumes_missing_coverage_and_reviews_the_new_answer(tmp_path, monkeypatch):
    test_search_coverage_controls_actual_continuation_and_visible_answer(tmp_path, monkeypatch,
        '条件に合う対象を確認してください。', 'alternate_match', cron_surface=True)


def test_real_web_review_repairs_candidate_binding_without_repeating_sources(tmp_path,monkeypatch):
    test_search_coverage_controls_actual_continuation_and_visible_answer(tmp_path,monkeypatch,
        '指定された情報源の一致する記録だけを確認してください。','binding_repair',api_surface=True)


def test_real_web_review_repairs_omitted_candidate_decision_without_repeating_sources(tmp_path,monkeypatch):
    test_search_coverage_controls_actual_continuation_and_visible_answer(tmp_path,monkeypatch,
        'Consulta el resultado de la fuente seleccionada.', 'binding_omission', api_surface=True)


@pytest.mark.parametrize('mode',['all_negative','alternate_match'])
def test_real_web_native_search_receipt_supports_discovery_not_unvisited_inventory(tmp_path,monkeypatch,mode):
    test_search_coverage_controls_actual_continuation_and_visible_answer(
        tmp_path,monkeypatch,'条件に一致する記録を探してください。',mode,api_surface=True,native_search=True)


@pytest.mark.parametrize('prompt',['指定した対象の特徴を教えてください。','Describe las características del sujeto indicado.'])
def test_web_replans_an_expanded_review_without_new_source_work(tmp_path,monkeypatch,prompt):
    test_search_coverage_controls_actual_continuation_and_visible_answer(
        tmp_path,monkeypatch,prompt,'expanded_scope',api_surface=True)
