"""Captured text must survive model formatting without accepting inventions."""
import asyncio
from types import SimpleNamespace

import pytest

from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_use_adapter import BrowserReport, PageEvidence


def captured(text, url='https://results.example/compare'):
    async def read():
        return {'url': url, 'text': text}
    evidence = PageEvidence('different-workspace', BrowserPolicy(), text_capture=read)
    asyncio.run(evidence.capture(SimpleNamespace(url=url, title='結果', dom_state=None, screenshot=None)))
    return evidence


def report(observation, **kwargs):
    return BrowserReport(goal_status=kwargs.pop('goal_status', 'answered'), next_action='report',
        observations=[observation], **kwargs)


@pytest.mark.parametrize('label', ['詳細', 'Detalles', 'التفاصيل'])
def test_multiline_record_can_be_selected_without_regenerating_a_quote(label):
    text = 'Results\nA42\nRegional\nDEPARTS\n10:45\np\nARRIVES\n1:02\np\n2h 17m\nCoach\nfrom\n$\n73'
    evidence = captured(text)
    old = {'label': label, 'value': '$73', 'quote': 'Regional A42 departs 10:45 PM, Coach $73'}
    assert evidence.retain(report(old)) == 0
    assert evidence.last_rejections[0]['reason'] == 'quote_not_captured'
    selection = {'label': label, 'text_selection': {'source_id': 0, 'start_line': 1, 'end_line': 13}}
    assert evidence.retain(report(selection)) == 1
    assert evidence.observations[0]['value'] == '\n'.join(text.splitlines()[1:])
    result = evidence.result(error=None, elapsed=1, steps=2, done=True)
    assert result['report_status'] == 'completed'
    assert result['rejection_counts'] == {'quote_not_captured': 1}
    assert result['validation_errors'] == []


@pytest.mark.parametrize('change,reason', [
    ({'value': '$9'}, 'value_not_in_quote'),
    ({'quote': 'invented'}, 'quote_differs_from_selection'),
    ({'source_id': 1}, 'conflicting_evidence_references'),
    ({'control_index': 0}, 'conflicting_evidence_references'),
    ({'source_url': 'https://other.example/'}, 'source_not_captured'),
    ({'text_selection': {'source_id': 0, 'start_line': 2, 'end_line': 1}}, 'invalid_text_selection'),
    ({'text_selection': {'source_id': 0, 'start_line': 0, 'end_line': 100}}, 'invalid_text_selection'),
    ({'text_selection': {'source_id': 100, 'start_line': 0, 'end_line': 1}}, 'source_not_captured'),
])
def test_references_do_not_weaken_evidence_validation(change, reason):
    evidence = captured('Item\n$73\nIn stock')
    observation = {'label': '候補', 'text_selection': {'source_id': 0, 'start_line': 0, 'end_line': 2}, **change}
    assert evidence.retain(report(observation)) == 0
    assert evidence.last_rejections[0]['reason'] == reason
    assert not evidence.observations


def test_page_read_paginates_immutable_capture_without_losing_late_results():
    evidence = captured('\n'.join(str(i) for i in range(300)))
    first = evidence.page_state()
    assert first['next_start_line'] == 120
    second = evidence.page_state(source_id=0, start_line=first['next_start_line'])
    assert second['text_lines'][0] == {'index': 120, 'text': '120'}
    assert evidence.retain(report({'label': '比較', 'text_selection': {
        'source_id': 0, 'start_line': 298, 'end_line': 299}})) == 1
    assert evidence.observations[0]['value'] == '298\n299'
    assert evidence.page_state(source_id=900)['status'] == 'source_not_captured'


def test_later_blocker_does_not_discard_accepted_findings():
    evidence = captured('候補 A\n€84')
    assert evidence.retain(report({'label': '候補', 'value': '€84', 'quote': '候補 A\n€84'})) == 1
    evidence.retain(BrowserReport(goal_status='blocked', next_action='report',
        unknowns=['復路'], blocker='Source rejected the next lookup'))
    result = evidence.result(error=None, elapsed=1, steps=3, done=True)
    assert result['report_status'] == 'partial'
    assert result['observations'][0]['value'] == '€84'
    assert result['unknowns'] == ['復路']


@pytest.mark.parametrize('text', ['', 'x' * 2001])
def test_empty_and_oversized_selection_is_rejected(text):
    evidence = captured(text)
    assert evidence.retain(report({'label': '候補', 'text_selection': {
        'source_id': 0, 'start_line': 0, 'end_line': 0}})) == 0


def test_large_line_does_not_make_page_read_pagination_loop():
    evidence = captured('x' * 15000 + '\nResult\n€73')
    first = evidence.page_state()
    assert first['text_lines'][0]['truncated'] is True
    assert first['next_start_line'] == 1
    assert evidence.page_state(source_id=0, start_line=1)['text_lines'][0]['text'] == 'Result'


def test_compaction_preserves_diagnostics_without_dropping_facts_or_publishing_errors():
    from nullion.source_observation_contract import compact_source_observations, source_answer_context
    from nullion.tools import ToolResult
    evidence = captured('Option A\n€73')
    assert evidence.retain(report({'label': '料金', 'value': '€73', 'quote': '€73'})) == 1
    evidence.retain(report({'label': '料金', 'value': '€9', 'quote': 'unsupported'}))
    result = evidence.result(error=None, elapsed=1, steps=2, done=True)
    compact = compact_source_observations(result)
    assert compact['observations'][0]['value'] == '€73'
    assert compact['validation_errors'][0]['reason'] == 'quote_not_captured'
    context = source_answer_context([ToolResult('one', 'browser_run_task', 'completed', compact)])
    assert 'quote_not_captured' not in str(context)


@pytest.mark.parametrize('rewrite', ['22 copies', '22 ejemplares', '22冊'])
def test_exact_source_quote_survives_an_unsupported_value_rewrite(rewrite):
    quote = '在庫あり (22 available)'
    evidence = captured(quote)
    assert evidence.retain(report({'label': '在庫', 'value': rewrite, 'quote': quote})) == 1
    result = evidence.result(error=None, elapsed=1, steps=1, done=True)
    assert result['observations'][0]['value'] == quote
    assert result['observations'][0]['quote'] == quote
    assert result['evidence_value_repair_count'] == 1
    assert result['rejection_counts'] == {'value_not_in_quote': 1}
    assert result['validation_errors'] == []
    from nullion.source_observation_contract import compact_source_observations
    assert compact_source_observations(result)['evidence_value_repair_count'] == 1


@pytest.mark.parametrize('repaired', [False, True])
def test_only_a_structured_quote_repair_forces_semantic_completion_review(monkeypatch, repaired):
    from nullion import agent_orchestrator as agent
    from test_source_failure_preservation import source_state
    state = source_state()
    output = state['tool_results'][0].output
    output.update(report_status='completed', unknowns=[], evidence_value_repair_count=int(repaired))
    state['source_answer_checkpoint']['presentation']['limitation_ids'] = []
    state['content'] = [{'type': 'tool_use', 'name': 'present_source_answer',
                         'input': state['source_answer_checkpoint']['presentation']}]
    calls = []
    def review(*args, **kwargs):
        calls.append(kwargs)
        return agent._CompletionReviewDecision('complete')
    monkeypatch.setattr(agent, '_review_risky_agent_completion', review)
    result = agent._complete_partial_source_report(state, '')
    assert bool(calls) is repaired
    if repaired: assert calls[0]['force'] is True
    assert result['result'].response_fulfilled


def test_a_genuine_but_irrelevant_quote_cannot_complete_the_request(monkeypatch):
    from nullion import agent_orchestrator as agent
    from test_source_failure_preservation import source_state
    state = source_state()
    output = state['tool_results'][0].output
    output.update(report_status='completed', unknowns=[], evidence_value_repair_count=1)
    output['observations'] = [{'value': 'Welcome', 'quote': 'Welcome'}]
    presentation = state['source_answer_checkpoint']['presentation']
    presentation.update(intro='', limitation_ids=[])
    state['content'] = [{'type': 'tool_use', 'name': 'present_source_answer', 'input': presentation}]
    monkeypatch.setattr(agent, '_review_risky_agent_completion', lambda *a, **kw:
        agent._CompletionReviewDecision('blocked', ('The requested price was not established',), ()))
    result = agent._complete_partial_source_report(state, '')
    assert result['result'].response_fulfilled is False


def field_records():
    from nullion.tools import ToolResult
    return [ToolResult('capture', 'browser_run_task', 'completed', {
        'result_kind': 'source_observations', 'report_status': 'completed', 'observations': [
            {'label': str(i), 'value': value, 'quote': value, 'source_url': 'https://results.example/',
             'captured_at': 1.0} for i, value in enumerate(['候補 A', '€73', '在庫 22'])], 'unknowns': []})]


def grouped_presentation(results):
    from nullion.source_observation_contract import source_answer_context
    rows = source_answer_context(results)['evidence']
    fields = [row['excerpts'][0]['id'] for row in rows[1:]]
    return {'intro': '', 'points': [{'evidence_ids': [row['id'] for row in rows],
        'title_ids': [rows[0]['excerpts'][0]['id']], 'detail_ids': fields,
        'detail_groups': [[identity] for identity in fields], 'relevance': 'Same captured candidate'}], 'limitation_ids': []}


def test_separate_fields_from_one_capture_form_one_readable_candidate():
    from nullion.source_observation_contract import render_source_answer
    results = field_records()
    text = render_source_answer(results, grouped_presentation(results))
    assert text.count('- **') == 1
    assert '**候補 A** — €73 · 在庫 22' in text


@pytest.mark.parametrize('change', ['time', 'source', 'absent_capture'])
def test_distinct_or_missing_captures_cannot_be_combined(change):
    from nullion.source_observation_contract import render_source_answer
    results = field_records(); row = results[0].output['observations'][2]
    if change == 'time': row['captured_at'] = 2.0
    elif change == 'source': row['source_url'] = 'https://different.example/'
    else: row.pop('captured_at')
    with pytest.raises(ValueError, match='captured source'):
        render_source_answer(results, grouped_presentation(results))


def test_deduplication_cannot_move_an_excerpt_between_capture_contexts():
    from nullion.source_observation_contract import render_source_answer, source_answer_context
    results = field_records(); rows = results[0].output['observations']
    rows[1].update(value='73', quote='Price\n73')
    rows[2].update(value='73', quote='Stock\n73', captured_at=2.0)
    context = source_answer_context(results); item = context['evidence'][1]
    available = {row['text']: row['id'] for row in item['excerpts']}
    point = {'evidence_ids': [row['id'] for row in context['evidence']],
        'title_ids': [context['evidence'][0]['excerpts'][0]['id']],
        'detail_ids': [available['Stock'], available['73']],
        'detail_groups': [[available['Stock'], available['73']]], 'relevance': 'A selected field'}
    with pytest.raises(ValueError, match='same capture'):
        render_source_answer(results, {'intro': '', 'points': [point], 'limitation_ids': []})


def test_grouped_fields_force_relationship_review(monkeypatch):
    from nullion import agent_orchestrator as agent
    from test_source_failure_preservation import source_state
    state = source_state(); state['tool_results'] = field_records()
    state['content'] = [{'type': 'tool_use', 'name': 'present_source_answer',
                         'input': grouped_presentation(state['tool_results'])}]
    calls = []
    def review(*args, **kwargs):
        calls.append(kwargs)
        return agent._CompletionReviewDecision('blocked', ('Relationship not established',), ())
    monkeypatch.setattr(agent, '_review_risky_agent_completion', review)
    result = agent._complete_partial_source_report(state, '')
    assert calls and result['result'].response_fulfilled is False


@pytest.mark.parametrize('groups', [True, False])
def test_grouping_is_order_independent_when_candidates_share_a_price(groups):
    from nullion.source_observation_contract import render_source_answer, source_answer_context
    results = field_records(); rows = results[0].output['observations']
    rows.extend([{**rows[0], 'value': '候補 B', 'quote': '候補 B', 'captured_at': 2.0},
                 {**rows[1], 'captured_at': 2.0}])
    context = source_answer_context(results)
    title_a, price, _, title_b = context['evidence']
    price_id = price['excerpts'][0]['id']
    points = [{'evidence_ids': [price['id'], title['id']],
        'title_ids': [title['excerpts'][0]['id']], 'detail_ids': [price_id],
        'detail_groups': [[price_id]], 'relevance': 'A candidate from its own capture'} for title in [title_a, title_b]]
    if not groups:
        for point in points: point.pop('detail_groups')
    text = render_source_answer(results, {'intro': '', 'points': points, 'limitation_ids': []})
    assert '**候補 A**' in text and '**候補 B**' in text
    assert text.count('€73') == 2


def test_child_step_completion_always_requires_full_parent_review(monkeypatch):
    from nullion import agent_orchestrator as agent
    from test_source_failure_preservation import source_state
    state = source_state(); output = state['tool_results'][0].output
    output.update(report_status='completed', unknowns=[], completion_scope='tool_step')
    presentation = state['source_answer_checkpoint']['presentation']; presentation.update(intro='', limitation_ids=[])
    state['content'] = [{'type': 'tool_use', 'name': 'present_source_answer', 'input': presentation}]
    monkeypatch.setattr(agent, '_review_risky_agent_completion', lambda *a, **kw:
        agent._CompletionReviewDecision('blocked', ('The other comparison side is still missing',), ()))
    result = agent._complete_partial_source_report(state, '')
    assert result['result'].response_fulfilled is False


def test_browser_step_keeps_parent_constraints_without_restarting_other_steps():
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask, _browser_task_objective
    task = BrowserTask(task='Read only candidate B', user_objective='Compare A and B under the same constraints', url='https://results.example/b')
    objective = _browser_task_objective(task)
    assert task.task in objective and task.user_objective in objective
    assert 'Execute only the current structured browser step' in objective
    assert 'parent owns and reviews the combined full request' in objective


@pytest.mark.parametrize('timeout', [7, 31])
@pytest.mark.parametrize('row_count', [1, 12])
def test_forced_review_honors_configured_deadline_and_sizes_coverage_output(monkeypatch, timeout, row_count):
    from nullion import agent_orchestrator as agent
    from test_source_failure_preservation import source_state
    state = source_state(); calls = []
    rows = [SimpleNamespace(requirement_id=f'r{i}', requirement='各候補の必要な条件を確認する。' * 15,
        requirement_kind='fact', evidence_subject_id=f's{i}', eligible_evidence_tool_names=('source_reader',)) for i in range(row_count)]
    monkeypatch.setattr(agent, '_extract_completion_requirement_contract', lambda *a, **kw: rows)
    monkeypatch.setenv('NULLION_COMPLETION_REVIEW_TIMEOUT_SECONDS', str(timeout))
    class Model:
        def create(self, **kwargs):
            calls.append(kwargs)
            return {'content': [{'type': 'text', 'text': '{}'}]}
    state['orchestrator'] = SimpleNamespace(model_client=Model())
    agent._review_risky_agent_completion(state, final_text='結果', tool_results=state['tool_results'], artifacts=[], force=True)
    assert len(calls) == 1 and calls[0]['timeout'] == timeout
    assert calls[0]['max_tokens'] >= 1100
    if row_count == 12: assert calls[0]['max_tokens'] > 1100


@pytest.mark.parametrize('known', [True, False])
@pytest.mark.parametrize('length', [12, 301, 2000])
def test_compact_review_rows_resolve_only_authoritative_requirement_ids(known, length):
    import json
    from nullion import agent_orchestrator as agent
    contract = [SimpleNamespace(requirement_id='r1', requirement='価' * length, requirement_kind='fact')]
    payload = {'disposition': 'complete', 'unresolved_requirements': [], 'retry_tool_names': [],
        'all_material_requirements_enumerated': True, 'material_requirements': [{
            'requirement_id': 'r1' if known else 'invented', 'satisfied': True, 'draft_covered': True,
            'evidence_invocation_ids': ['observed']} ]}
    decision = agent._parse_completion_review_decision({'content': [{'type': 'text', 'text': json.dumps(payload)}]},
        registered_tool_names=set(), requirement_contract=contract)
    if not known: assert decision is None
    else:
        assert decision.material_requirements[0].requirement == contract[0].requirement
        assert decision.material_requirements[0].requirement_kind == 'fact'


def test_completion_review_receives_quoted_facts_instead_of_capture_paths():
    import json
    from nullion import agent_orchestrator as agent
    results = field_records()
    output = results[0].output
    output.update(artifact_paths=['/internal/source-capture.png'], artifact_descriptors=[{
        'path': '/internal/source-capture.png', 'role': 'source'}], execution_audit={
            'version': 1, 'complete': True, 'read_only_actions_only': True,
            'downloaded_file_count': 0, 'deliverable_file_count': 0, 'captures_are_internal': True})
    projected = json.loads(agent._compact_tool_evidence_for_repair(results, limit=2200))[0]['output']
    assert isinstance(projected, dict)
    assert [row['value'] for row in projected['observations']] == ['候補 A', '€73', '在庫 22']
    assert projected['execution_audit']['read_only_actions_only'] is True
    assert projected['execution_audit']['deliverable_file_count'] == 0
    assert '/internal/' not in str(projected)


@pytest.mark.parametrize('action,complete,read_only', [('read_page_state', True, True), ('click', True, False), ('read_page_state', False, False)])
def test_runtime_execution_audit_does_not_claim_unobserved_safety(action, complete, read_only):
    from nullion.plugins.browser_plugin.browser_use_adapter import _browser_execution_audit
    class Action:
        def model_dump(self, **kwargs): return {action: {}}
    agent = SimpleNamespace(history=SimpleNamespace(history=[SimpleNamespace(model_output=SimpleNamespace(action=[Action()]))]))
    evidence = captured('候補 A\n€73')
    audit = _browser_execution_audit(agent, evidence, complete=complete, download_count=1)
    assert audit['read_only_actions_only'] is read_only
    assert audit['downloaded_file_count'] == 1
    assert audit['captures_are_internal'] is True
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        BrowserReport(goal_status='answered', next_action='report', execution_audit=audit)


@pytest.mark.parametrize('observed', [None, '空きなし', 'Agotado'])
def test_empty_blocker_is_not_positive_evidence_but_verified_negative_is(observed):
    from nullion import agent_orchestrator as agent
    from nullion.tools import ToolResult
    output = {'result_kind': 'source_observations', 'report_status': 'completed' if observed else 'blocked',
        'next_action': 'report', 'observations': [{'value': observed, 'quote': observed}] if observed else [],
        'unknowns': [], 'blocker': None if observed else 'No captured evidence'}
    result = ToolResult('capture', 'browser_run_task', 'completed', output)
    assert agent._completion_review_evidence_result_is_positive(result, tool_results=[result]) is bool(observed)


def test_main_document_coverage_is_runtime_owned_and_survives_compaction():
    from pydantic import ValidationError
    from nullion.source_observation_contract import compact_source_observations, source_answer_context
    from nullion.tools import ToolResult
    evidence = captured('候補 A\n€73\nIn stock')
    evidence.retain(report({'label': 'Complete page', 'value': '候補 A', 'quote': '候補 A\n€73\nIn stock'}))
    evidence.retain(report({'label': 'Price', 'value': '€73', 'quote': '€73'}))
    assert [row.get('covers_main_document', False) for row in evidence.observations] == [True, False]
    with pytest.raises(ValidationError):
        report({'label': 'Forged', 'value': '€73', 'quote': '€73', 'covers_main_document': True})
    output = evidence.result(error=None, elapsed=1, steps=1, done=True)
    compact = compact_source_observations({'result_kind': 'source_observations', **output})
    rows = source_answer_context([ToolResult('capture', 'browser_run_task', 'completed', compact)])['evidence']
    assert rows[0]['whole_document_capture_ids'][0]
    assert rows[1]['whole_document_capture_ids'] == [[]]


@pytest.mark.parametrize('disposition', ['complete', 'blocked'])
@pytest.mark.parametrize('status', ['completed', 'partial'])
def test_interpreted_intro_requires_independent_review_and_never_leaks_on_failure(monkeypatch, disposition, status):
    from nullion import agent_orchestrator as agent
    from test_source_failure_preservation import source_state
    state = source_state(); output = state['tool_results'][0].output
    output.update(report_status=status, next_action='report', unknowns=['Producer could not interpret the missing field'] if status == 'partial' else [])
    presentation = {**state['source_answer_checkpoint']['presentation'], 'intro': 'No delivery date is shown on this page.',
        'intro_evidence_ids': ['e0.0'], 'limitation_ids': []}
    state['content'] = [{'type': 'tool_use', 'name': 'present_source_answer', 'input': presentation}]
    calls = []
    def review(*args, **kwargs):
        calls.append(kwargs)
        return agent._CompletionReviewDecision(disposition, () if disposition == 'complete' else ('Contradicted by the page',), ())
    monkeypatch.setattr(agent, '_review_risky_agent_completion', review)
    result = agent._complete_partial_source_report(state, '')['result']
    assert calls
    assert result.response_fulfilled is (disposition == 'complete')
    assert ('No delivery date is shown' in result.final_text) is (disposition == 'complete')
    assert '71 USD' in result.final_text


@pytest.mark.parametrize('ids', [['missing'], [], ['e0.0', 'e0.0']])
def test_interpreted_intro_rejects_invalid_binding(ids):
    from test_source_failure_preservation import source_state
    from nullion.source_observation_contract import render_source_answer
    state = source_state(); presentation = {**state['source_answer_checkpoint']['presentation'], 'intro_evidence_ids': ids}
    with pytest.raises(ValueError, match='introductory evidence'):
        render_source_answer(state['tool_results'], presentation)


def test_repeated_title_uses_latest_snapshot_without_losing_complete_quote():
    from nullion.source_observation_contract import compact_source_observations, source_answer_context
    from nullion.tools import ToolResult
    evidence = captured('候補 A\n€73')
    evidence.retain(report({'label': 'Title', 'value': '候補 A', 'quote': '候補 A', 'source_id': 0}))
    async def later(): return {'url': 'https://results.example/compare', 'text': '候補 A\n€84'}
    evidence.text_capture = later
    asyncio.run(evidence.capture(SimpleNamespace(url='https://results.example/compare', title='結果', dom_state=None, screenshot=None)))
    evidence.retain(BrowserReport(goal_status='answered', next_action='report', observations=[
        {'label': 'Title', 'value': '候補 A', 'quote': '候補 A', 'source_id': 1},
        {'label': 'Title', 'value': '候補 A', 'quote': '候補 A\n€84', 'source_id': 1}]))
    assert len(evidence.observations) == 2
    compact = compact_source_observations({'result_kind': 'source_observations', **evidence.result(error=None, elapsed=1, steps=2, done=True)})
    row = source_answer_context([ToolResult('capture', 'browser_run_task', 'completed', compact)])['evidence'][0]
    assert len(row['capture_ids']) == 1 and len(row['quotes']) == 2
    assert all(item['captured_at'] == evidence.pages[1]['captured_at'] for item in evidence.observations)
    assert row['whole_document_capture_ids'][0] == []
    assert len(row['whole_document_capture_ids'][1]) == 1


def test_short_fact_keeps_complete_document_context_without_claiming_its_quote_is_complete():
    from nullion.source_observation_contract import compact_source_observations, source_answer_context
    from nullion.tools import ToolResult
    evidence = captured('候補 A\n€73\nIn stock\nFooter')
    evidence.retain(report({'label': 'Price', 'value': '€73', 'quote': '€73'}))
    output = evidence.result(error=None, elapsed=1, steps=1, done=True)
    assert not output['observations'][0].get('covers_main_document')
    compact = compact_source_observations(output)
    again = compact_source_observations(compact)
    context = source_answer_context([ToolResult('capture', 'browser_run_task', 'completed', again)])
    cid = context['evidence'][0]['capture_ids'][0]
    assert context['capture_documents'][cid] == '候補 A\n€73\nIn stock\nFooter'
    assert context['evidence'][0]['whole_document_capture_ids'] == [[]]


@pytest.mark.parametrize('budget', [900, 1800])
def test_optional_document_never_displaces_facts_or_becomes_clipped_proof(budget):
    from nullion.source_observation_contract import compact_source_observations
    evidence = captured('候補 A\n€73\n' + 'x' * 3000)
    evidence.retain(report({'label': 'Price', 'value': '€73', 'quote': '€73'}))
    output = compact_source_observations(evidence.result(error=None, elapsed=1, steps=1, done=True), max_chars=budget)
    assert output['observations'][0]['value'] == '€73'
    assert output['report_status'] == 'completed'
    assert 'main_document_text' not in output['sources'][0]


def test_superseded_fact_capture_stays_available_for_explicit_image_delivery(tmp_path, monkeypatch):
    import base64, io
    from pathlib import Path
    from PIL import Image
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    evidence = captured('Item\n€73')
    for color in ['red', 'blue']:
        buffer = io.BytesIO(); Image.new('RGB', (10, 10), color).save(buffer, format='PNG')
        state = SimpleNamespace(url='https://results.example/compare', title='結果', dom_state=None,
            screenshot=base64.b64encode(buffer.getvalue()).decode())
        asyncio.run(evidence.capture(state))
        evidence.retain(report({'label': 'Price', 'value': '€73', 'quote': '€73'}))
    assert len(evidence.observations) == 1
    result = evidence.result(error=None, elapsed=1, steps=2, done=True)
    assert len(result['artifact_paths']) == 2
    assert all(Path(path).read_bytes().startswith(b'\x89PNG') for path in result['artifact_paths'])
    assert all(row['role'] == 'source' for row in result['artifact_descriptors'])


def test_labels_and_value_selections_do_not_duplicate_the_same_captured_quote():
    evidence = captured('Item\nIn stock (22 available)')
    evidence.retain(report({'label': '在庫', 'value': 'In stock (22 available)', 'quote': 'In stock (22 available)'}))
    evidence.retain(report({'label': 'Exact count', 'value': '22', 'quote': 'In stock (22 available)'}))
    assert len(evidence.observations) == 1
    assert evidence.observations[0]['value'] == 'In stock (22 available)'


def bound_comparison(shared_price=False):
    from nullion.tools import ToolResult
    from nullion.source_observation_contract import source_answer_context, source_answer_review_records
    price_b = '€73' if shared_price else '€84'
    evidence = captured('候補 A\n€73\n候補 B\n'+price_b)
    evidence.retain(BrowserReport(goal_status='answered', next_action='report', observations=[
        {'label': '一', 'value': '候補 A', 'quote': '候補 A\n€73'},
        {'label': '二', 'value': '候補 B', 'quote': '候補 B\n'+price_b}]))
    output = evidence.result(error=None, elapsed=1, steps=1, done=True)
    result = ToolResult('one-real-browser-call', 'browser_run_task', 'completed', output)
    context = source_answer_context([result])
    presentation = {'intro': '', 'points': [{'evidence_ids': [r['id']],
        'detail_ids': [r['excerpts'][1]['id']], 'relevance': 'A captured comparison item'} for r in context['evidence']], 'limitation_ids': []}
    bound, fingerprints, fully_bound = source_answer_review_records([result], presentation)
    assert fully_bound == {result.invocation_id}
    return result, presentation, bound, fingerprints


@pytest.mark.parametrize('also_cite_parent', [False, True])
@pytest.mark.parametrize('shared_price', [False, True])
def test_one_real_invocation_can_ground_two_distinct_capture_bound_entities(also_cite_parent, shared_price):
    from nullion import agent_orchestrator as a
    parent, _, records, fingerprints = bound_comparison(shared_price)
    assert len(records) == 2
    assert {r.output['source_record_binding']['parent_invocation_id'] for r in records} == {parent.invocation_id}
    contract = tuple(a._CompletionRequirementContractRow(f'r{i}', f'Outcome {i}', 'comparison_side',
        ('browser_run_task',), f's{i}') for i in range(2))
    decision = a._CompletionReviewDecision('complete', all_material_requirements_enumerated=True,
        material_requirements=tuple(a._CompletionReviewMaterialRequirement(row.requirement_id,
            row.requirement, row.requirement_kind, True, True,
            (records[i].invocation_id, *([parent.invocation_id] if also_cite_parent else []))) for i,row in enumerate(contract)))
    assert a._forced_completion_review_is_grounded(decision, tool_results=[parent,*records],
        requirement_contract=contract, source_record_fingerprints=fingerprints)
    assert not a._forced_completion_review_is_grounded(decision, tool_results=[parent], requirement_contract=contract)


@pytest.mark.parametrize('bad', ['unknown_ref', 'same_record', 'same_content'])
def test_record_references_cannot_inflate_comparison_coverage(bad):
    from nullion import agent_orchestrator as a
    parent, _, records, fingerprints = bound_comparison()
    ids = [r.invocation_id for r in records]
    if bad == 'unknown_ref': ids[1] = 'invented-record'
    if bad == 'same_record': ids[1] = ids[0]
    if bad == 'same_content': fingerprints[ids[1]] = fingerprints[ids[0]]
    contract = tuple(a._CompletionRequirementContractRow(f'r{i}', f'Outcome {i}', 'comparison_side',
        ('browser_run_task',), f's{i}') for i in range(2))
    decision = a._CompletionReviewDecision('complete', all_material_requirements_enumerated=True,
        material_requirements=tuple(a._CompletionReviewMaterialRequirement(r.requirement_id, r.requirement,
            r.requirement_kind, True, True, (ids[i],)) for i,r in enumerate(contract)))
    assert not a._forced_completion_review_is_grounded(decision, tool_results=[parent,*records],
        requirement_contract=contract, source_record_fingerprints=fingerprints)


def test_record_binding_rejects_unvalidated_or_unprovenanced_selections():
    from nullion.source_observation_contract import source_answer_review_records
    from nullion.tools import ToolResult
    parent, presentation, _, _ = bound_comparison()
    presentation['points'][0]['detail_ids'] = ['invented']
    assert source_answer_review_records([parent], presentation) == ([], {}, set())
    _, presentation, _, _ = bound_comparison()
    for row in parent.output['observations']: row.pop('captured_at', None)
    assert source_answer_review_records([parent], presentation) == ([], {}, set())


def test_one_complete_page_quote_can_supply_multiple_literal_entity_records():
    from nullion.tools import ToolResult
    from nullion.source_observation_contract import source_answer_context, source_answer_review_records, render_source_answer
    page = '候補 A\n€73\n候補 B\n€84'
    evidence = captured(page);evidence.retain(report({'label': 'Page', 'value': '候補 A', 'quote': page}))
    parent = ToolResult('one-page-one-call', 'browser_run_task', 'completed', evidence.result(error=None, elapsed=1, steps=1, done=True))
    row = source_answer_context([parent])['evidence'][0];ids={e['text']:e['id'] for e in row['excerpts']}
    presentation={'intro':'','points':[{'evidence_ids':[row['id']], 'title_ids':[ids[name]], 'detail_ids':[ids[price]],
        'detail_groups':[[ids[price]]], 'relevance':'A requested item'} for name,price in [('候補 A','€73'),('候補 B','€84')]],'limitation_ids':[]}
    text=render_source_answer([parent],presentation)
    assert '**候補 A**' in text and '**候補 B**' in text
    records,fingerprints,covered=source_answer_review_records([parent],presentation)
    assert len(records)==2 and len(set(fingerprints.values()))==2 and covered=={parent.invocation_id}


def test_review_shares_complete_documents_without_losing_unselected_evidence_or_mutating_history():
    import copy,json
    from nullion.source_observation_contract import source_review_documents
    from nullion import agent_orchestrator as a
    parent, _, records, _ = bound_comparison()
    before = copy.deepcopy(parent.output)
    projected, documents = source_review_documents([*records, parent])
    assert parent.output == before
    assert len(documents) == 1
    document = next(iter(documents.values()))
    assert document['text'] == '候補 A\n€73\n候補 B\n€84'
    assert len(projected) == 3 and projected[-1].invocation_id == parent.invocation_id
    evidence = json.loads(a._compact_tool_evidence_for_repair(projected, limit=5000))
    assert all(row['output']['sources'][0]['source_document_id'] in documents for row in evidence)
    assert all('main_document_text' not in row['output']['sources'][0] for row in evidence)
    omitted, none = source_review_documents([*records, parent], max_chars=1)
    assert none == {} and omitted[0].output['sources'][0]['main_document_text'] == document['text']
    assert parent.output == before


@pytest.mark.parametrize('echo', ['exact', 'changed', 'empty'])
def test_long_authoritative_requirement_echo_must_remain_exact(echo):
    import json
    from nullion import agent_orchestrator as a
    text='各候補の価格と制約' * 80
    contract=[a._CompletionRequirementContractRow('r1',text,'fact',('browser_run_task',),'one')]
    row={'requirement_id':'r1','requirement':text if echo=='exact' else (text+'変更' if echo=='changed' else ''),
        'satisfied':True,'draft_covered':True,'evidence_invocation_ids':['one']}
    response={'content':[{'type':'text','text':json.dumps({'disposition':'complete','material_requirements':[row],
        'all_material_requirements_enumerated':True})}]}
    decision=a._parse_completion_review_decision(response,registered_tool_names={'browser_run_task'},requirement_contract=contract)
    if echo=='exact': assert decision.material_requirements[0].requirement==text
    else: assert decision is None
