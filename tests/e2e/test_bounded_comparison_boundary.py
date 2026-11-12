"""Comparison policy through the real graph, SQLite and Web route.

Only the external model and source adapters are deterministic boundaries.
"""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def text(value):
    return text_response(value if isinstance(value, str) else json.dumps(value, ensure_ascii=False))


@pytest.mark.parametrize('prompt', ['条件に合う候補を比較してください。', 'Compara las opciones adecuadas.'])
@pytest.mark.parametrize('mode', ['bounded', 'frontier', 'wording', 'blocked', 'exhaustive', 'finite'])
def test_comparison_finishes_useful_scope_but_checks_material_challengers(tmp_path, monkeypatch, prompt, mode, web=False, telegram=False, structured=False, native_search=False):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult

    runtime = setup_runtime(tmp_path, monkeypatch)
    cid = 'telegram:42' if telegram else 'web:bounded'
    registry = ToolRegistry()
    reads = []
    discoveries = []
    sources = ['https://catalog.example/'+name for name in ['a', 'b', 'challenger', *map(str, range(12))]]

    def discover(inv):
        discoveries.append(inv.invocation_id)
        if native_search:
            from nullion.tools import _build_web_search_handler
            return _build_web_search_handler(lambda query,limit:[
                {'url':url,'title':f'候補 {i}','snippet':'An independent candidate listing.'}
                for i,url in enumerate(sources)])(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'source_candidates': [{'url': url} for url in sources], 'text': 'Discovered candidate listings.'})

    def read(inv):
        url = inv.arguments['url']
        reads.append(url)
        if mode == 'blocked' and url == sources[2]:
            return ToolResult(inv.invocation_id, inv.tool_name, 'failed', {'source_url': url}, 'Source unavailable')
        value = 9 if url == sources[0] else 75 if url == sources[1] else 7
        if structured:
            return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
                'source_url':url,'payload':{'entry':{'key':url,'amount':value,'unit':'cents/g'}},
                'source_record_collections':[{'path':['payload','entry'],'identity_fields':['key'],'cardinality':'one'}]})
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'source_url': url, 'text': f'Observed price per gram: {value} cents.', 'value': value, 'unit': 'cents/g'})

    registry.register(ToolSpec('opaque_discover', 'Discover source listings', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), discover)
    registry.register(ToolSpec('opaque_read', 'Inspect one source listing', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',),
        input_schema={'type': 'object', 'properties': {'url': {'type': 'string'}}, 'required': ['url']}), read)
    registry.register(ToolSpec('unrelated_read', 'Inspect unrelated local state', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), lambda inv: pytest.fail('Unrelated read entered comparison recovery'))

    class Provider:
        reviews = 0
        plans = 0

        def create(self, **kw):
            system = kw.get('system') or ''
            names = {row['name'] for row in kw.get('tools') or ()}
            if web or telegram:
                if 'select_attachment_format' in names:
                    return tool_response('select_attachment_format', {'extension': None, 'confidence': 1, 'embedded_media_extensions': []})
                if 'relationship' in system and 'target_index' in system:
                    return text({'relationship': 'independent', 'effect': 'none', 'confidence': 1, 'target_index': None})
                if 'Return JSON only for a single already-selected read-only tool' in system:
                    return text({'use_direct': False, 'arguments': {}})
                if 'request_tool_scope' in names and not names.intersection({'opaque_discover', 'opaque_read'}):
                    return tool_response('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['opaque_discover', 'opaque_read']})
                if not names and not ('material-requirements planner' in system or 'semantic completion reviewer' in system):
                    return text({})
            if 'material-requirements planner' in system:
                self.plans += 1
                return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'search_coverage': {'scope': 'comparison', 'comparison_mode': 'exhaustive' if mode == 'exhaustive' else 'bounded',
                        'constraints': ['比較条件'], 'restricted_source_ids': [],
                        **({'source_discovery_required':False} if mode=='finite' else {})},
                    'material_requirements': [{'requirement': label, 'requirement_kind': kind,
                        'evidence_subject_id': subject, 'eligible_evidence_tool_names': ['opaque_read']}
                        for label, kind, subject in [('Selected option price and unit', 'comparison_side', 'choice'),
                            ('Benchmark price and unit', 'comparison_side', 'benchmark'), ('Requested comparison conditions', 'constraint', 'conditions')]]})
            if 'semantic completion reviewer' in system:
                self.reviews += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                ctx = packet['search_coverage_context']
                observed = [row for row in packet['tool_evidence'] if row['tool_name'] == 'opaque_read' and row['status'] == 'completed']
                choice = observed[-1]['invocation_id'] if sources[2] in reads and mode == 'frontier' else observed[0]['invocation_id']
                benchmark = observed[1]['invocation_id']
                blocked = mode == 'blocked' and sources[2] in reads
                rows = [{'candidate_id': row['candidate_id'],
                    'disposition': 'checked' if row['observation_invocation_ids'] else 'pending',
                    'reason': '観測済み' if row['observation_invocation_ids'] else '未確認',
                    'evidence_invocation_ids': row['observation_invocation_ids'] or row['discovery_invocation_ids']}
                    for row in ctx['candidates']]
                assessment = {'conclusion': 'comparison', 'coverage_sufficient': True, 'scope_matches_request': True,
                    'discovery_evidence_invocation_ids': ctx['discovery_invocation_ids'],
                    'match_evidence_invocation_ids': [choice, benchmark], 'candidates': rows,
                    'constraints': [{'constraint': c, 'verified': True, 'evidence_invocation_ids': [choice, benchmark]}
                        for c in ctx['plan']['constraints']]}
                if mode != 'exhaustive':
                    assessment['comparison_scope'] = {'claim_extent': 'verified_options',
                        'scope_disclosed': mode != 'wording' or self.reviews > 1,
                        'ranking_complete': True, 'selected_evidence_invocation_ids': [choice, benchmark],
                        'frontier_candidate_ids': [ctx['candidates'][2]['candidate_id']] if mode in {'frontier', 'blocked'} else []}
                return text({'disposition': 'blocked' if blocked else 'complete',
                    'user_facing_failure': 'A shortlisted alternative is unavailable.' if blocked else None,
                    'unresolved_requirements': [], 'retry_tool_names': [],
                    'supported_findings': [{'text': 'Among the checked options: A costs 9 cents/g; benchmark B costs 75 cents/g.',
                        'evidence_invocation_ids': [choice, benchmark]}] if blocked else [],
                    'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': True, 'draft_covered': True,
                        'evidence_invocation_ids': [benchmark if row['evidence_subject_id'] == 'benchmark' else choice]}
                        for row in packet['material_requirement_contract']], 'search_coverage': assessment})
            if not discoveries and mode!='finite':
                return tool_response('opaque_discover', {'query':'候補','limit':len(sources)} if native_search else {})
            for url in sources[:2]:
                if url not in reads:
                    return tool_response('opaque_read', {'url': url})
            if mode in {'frontier', 'blocked'} and sources[2] not in reads:
                return tool_response('opaque_read', {'url': sources[2]})
            if mode == 'exhaustive':
                for url in sources[2:]:
                    if url not in reads:
                        return tool_response('opaque_read', {'url': url})
            return text('Among the checked options, '+('C is 7' if mode == 'frontier' and sources[2] in reads else 'A is 9')+
                ' cents/g; benchmark B is 75 cents/g.' + (' 1 shortlisted alternative still needs verification.' if mode == 'blocked' else ''))

    provider = Provider()
    runner = AgentOrchestrator(model_client=provider)
    if telegram:
        import asyncio
        from types import SimpleNamespace
        from nullion.config import NullionSettings, TelegramSettings
        from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
        from nullion.telegram_app import _deliver_reply
        from telegram import Bot, Message
        from tests.e2e.test_health_action_acknowledgements import telegram_boundary
        monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'none.env'))
        for key in ['NULLION_MEMORY_ENABLED', 'NULLION_DOCTOR_ENABLED', 'NULLION_TASK_DECOMPOSITION_ENABLED', 'NULLION_MULTI_AGENT_ENABLED']:
            monkeypatch.setenv(key, '0')
        monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
        runtime.active_tool_registry = registry
        settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic', operator_chat_id='42', chat_enabled=True))
        outcome = run_platform_chat_request(runtime, PlatformChatRequest(platform='telegram', text=prompt,
            conversation_id=cid, chat_id='42', settings=settings, model_client=provider,
            agent_orchestrator=runner, append_activity_trace=False))
        with telegram_boundary() as (url, calls):
            async def deliver():
                async with Bot('123456:synthetic', base_url=url) as bot:
                    message = Message.de_json({'message_id': 101, 'date': 1, 'chat': {'id': 42, 'type': 'private'}, 'text': prompt}, bot)
                    await _deliver_reply(message, outcome.text)
            asyncio.run(deliver())
        wire = [payload['text'] for method, payload in calls if method == 'sendMessage']
        assert len(wire) == 1 and '75 cents/g' in wire[0]
        assert 'Other relevant sources remain unchecked' not in wire[0]
        result = SimpleNamespace(final_text=outcome.text)
    elif web:
        from fastapi.testclient import TestClient
        from types import SimpleNamespace
        from nullion import chat_store, web_app
        for key, value in {'NULLION_ENV_FILE': str(tmp_path/'none.env'), 'NULLION_TASK_PLANNER_FEED_MODE': 'off',
            'NULLION_CHAT_STARTUP_WARMUP': '0', 'NULLION_ACTIVITY_TRACE_ENABLED': '0'}.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setattr(chat_store, '_store', None)
        runtime.model_client = provider
        with TestClient(web_app.create_app(runtime, orchestrator=runner, registry=registry)) as client:
            response = client.post('/api/chat', json={'text': '/chat '+prompt, 'conversation_id': 'web:bounded'})
            assert response.status_code == 200, response.text
            data = response.json()
        # The public message response need not expose the internal completion
        # verdict. Its persisted review, asserted below, remains authoritative.
        result = SimpleNamespace(final_text=data['text'], response_fulfilled=data.get('response_fulfilled'))
    else:
        result = runner.run_turn(conversation_id='web:bounded', principal_id='web:bounded', user_message=prompt,
            conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=25)
    assert len(discoveries) == (0 if mode == 'finite' else 1) and provider.plans == 0
    assert 'Other relevant sources remain unchecked' not in result.final_text
    assert '75 cents/g' in result.final_text
    events = runtime.store.list_recent_conversation_events(cid, event_type='conversation.completion_review')
    if mode == 'blocked':
        assert reads == sources[:3]
        assert '1 shortlisted alternative still needs verification' in result.final_text
    else:
        if not telegram and not web:
            assert result.response_fulfilled, result
        assert reads == (sources if mode == 'exhaustive' else sources[:3] if mode == 'frontier' else sources[:2])
        assert 'Among the checked options' in result.final_text
        if mode == 'wording':
            assert provider.reviews == 0 and 'best overall' not in result.final_text
        if mode == 'frontier':
            assert 'C is 7' in result.final_text
    assert not events and provider.reviews == 0
    if web or telegram:
        rows = [row for row in runtime.store.list_conversation_events(cid) if row.get('event_type') == 'conversation.chat_turn']
        assert rows[-1]['assistant_reply'] == result.final_text


def test_web_route_keeps_recommendation_with_receipts_and_no_blanket_warning(tmp_path, monkeypatch):
    test_comparison_finishes_useful_scope_but_checks_material_challengers(
        tmp_path, monkeypatch, 'Compara las opciones adecuadas.', 'bounded', web=True)


@pytest.mark.parametrize('mode', ['bounded', 'blocked'])
def test_telegram_delivers_one_scoped_answer_or_specific_remaining_gap(tmp_path, monkeypatch, mode):
    test_comparison_finishes_useful_scope_but_checks_material_challengers(
        tmp_path, monkeypatch, '条件に合う候補を比較してください。', mode, telegram=True)


@pytest.mark.parametrize('mode',['bounded','frontier','blocked'])
def test_structured_detail_adapter_keeps_source_proof_through_real_review_and_web_delivery(tmp_path,monkeypatch,mode):
    test_comparison_finishes_useful_scope_but_checks_material_challengers(
        tmp_path,monkeypatch,'Compare les options pertinentes.',mode,web=True,structured=True)


def test_web_finite_shortlist_does_not_invent_an_additional_provider_search(tmp_path,monkeypatch):
    test_comparison_finishes_useful_scope_but_checks_material_challengers(
        tmp_path,monkeypatch,'指定された候補の価格と単位を比較してください。','finite',web=True)


def test_web_bounded_recommendation_retains_native_search_discovery(tmp_path,monkeypatch):
    test_comparison_finishes_useful_scope_but_checks_material_challengers(
        tmp_path,monkeypatch,'Compara las opciones verificadas.','bounded',web=True,native_search=True)
