"""Page-bound prerequisites use the shared numbered clarification contract."""
import asyncio
from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import _native_tool_user_choice
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_use_adapter import BrowserReport, PageEvidence
from nullion.tools import ToolResult


def captured_evidence():
    state = SimpleNamespace(url='https://example.com/form', title='Selection', dom_state=None, screenshot=None)
    current = {'url':state.url, 'text':'Choose an exact destination',
               'controls':[{'label':'Destination', 'value':'10024 Martense Ave'}], 'frames':[]}
    async def capture(): return current
    evidence = PageEvidence('operator', BrowserPolicy(), text_capture=capture)
    asyncio.run(evidence.capture(state))
    return evidence, state, current


def report(question='Which exact destination?', options=None, **updates):
    fields = dict(goal_status='blocked', next_action='user_input', observations=[],
        blocker='The current selection conflicts with the supplied area.',
        user_input={'reason':'constraint_mismatch', 'source_id':0, 'control_index':0,
                    'evidence_quote':'10024 Martense Ave', 'question':question,
                    'options':options or ['Provide a destination', 'Leave the task unchanged']})
    fields.update(updates)
    return BrowserReport(**fields)


@pytest.mark.parametrize('question,options', [
    ('Which exact destination?', ['Provide a destination', 'Leave unchanged']),
    ('どの場所ですか？', ['場所を指定', '変更しない']),
    ('¿Qué destino?', ['Indicar destino', 'Dejar sin cambios']),
])
def test_grounded_prerequisite_returns_shared_choice_without_continuation(question, options):
    evidence, _, _ = captured_evidence()
    evidence.retain(report(question, options))
    for done in (False, True):
        output = evidence.result(error=None, elapsed=.1, steps=1, done=done)
        assert output['requires_user_choice'] is True and output['continuation'] is None
        result = ToolResult(invocation_id='required-input', tool_name='browser_run_task', status='completed', output=output)
        text = _native_tool_user_choice(result)
        assert question in text and all(f'{i}. {label}' in text for i,label in enumerate(options, 1))
        assert output['captured_documents'] and not output['observations']


@pytest.mark.parametrize('invalid', ['source', 'url', 'quote', 'control', 'answered', 'question', 'option'])
def test_invalid_or_stale_prerequisite_cannot_become_user_choice(invalid):
    evidence, state, current = captured_evidence()
    value = report().model_dump()
    if invalid == 'source': value['user_input']['source_id'] = 99
    if invalid == 'quote': value['user_input']['evidence_quote'] = 'invented evidence'
    if invalid == 'control': value['user_input']['control_index'] = 99
    if invalid == 'answered': value['goal_status'] = 'answered'
    if invalid == 'question': value['user_input']['question'] = '  '
    if invalid == 'option': value['user_input']['options'][0] = '  '
    if invalid == 'url':
        state.url = current['url'] = 'https://example.com/other'
        asyncio.run(evidence.capture(state))
    evidence.retain(BrowserReport(**value))
    output = evidence.result(error=None, elapsed=.1, steps=1, done=True)
    assert not output.get('requires_user_choice')
    assert {'reason':'user_input_not_grounded_in_current_page'} in evidence.last_rejections


def test_changed_control_cannot_be_grounded_in_old_capture_and_partial_facts_survive():
    evidence, state, current = captured_evidence()
    evidence.retain(BrowserReport(goal_status='partial', next_action='report', observations=[
        {'source_id':0, 'control_index':0, 'label':'Selected destination',
         'value':'10024 Martense Ave', 'quote':'10024 Martense Ave'}]))
    current['controls'] = [{'label':'Destination', 'value':'New destination'}]
    asyncio.run(evidence.capture(state))
    evidence.retain(report())
    assert not evidence.user_choice and evidence.observations
    assert not evidence.result(error=None, elapsed=.1, steps=2, done=True).get('requires_user_choice')
    fresh = report().model_dump()
    fresh['user_input'].update(source_id=1, evidence_quote='New destination')
    evidence.retain(BrowserReport(**fresh))
    result = evidence.result(error=None, elapsed=.1, steps=3, done=True)
    assert result['requires_user_choice'] and result['observations']
    assert not evidence.result(error='transport lost', elapsed=.1, steps=3, done=True).get('requires_user_choice')


def test_final_capture_revalidates_a_previously_grounded_choice():
    evidence, state, current = captured_evidence()
    evidence.retain(report())
    assert evidence.user_choice
    current['controls'] = [{'label':'Destination', 'value':'Changed after report'}]
    asyncio.run(evidence.capture(state))
    output = evidence.result(error=None, elapsed=.1, steps=1, done=True)
    assert not output.get('requires_user_choice')
    assert output['next_action'] == 'report'


def test_shared_sanitation_keeps_current_choice_ahead_of_earlier_source_inventory():
    from nullion.response_sanitizer import sanitize_user_visible_reply
    from nullion.chat_text import render_tool_user_choice
    evidence, _, _ = captured_evidence()
    evidence.retain(report())
    output = evidence.result(error=None, elapsed=.1, steps=1, done=True)
    results = [ToolResult('prior', 'personal_tasks_list', 'completed', {'items':[{'title':'Unrelated task','status':'pending'}]}),
               ToolResult('current', 'browser_run_task', 'completed', output)]
    metadata = {}
    text = sanitize_user_visible_reply(user_message='確認して', reply='earlier source inventory', tool_results=results,
                                      presentation_metadata=metadata)
    assert 'Which exact destination?' in text and '1.' in text and '2.' in text
    assert metadata['needs_input'] is True and 'Unrelated task' not in text
    assert render_tool_user_choice({**output,'recovery_options':[{'number':9,'label':'bad'}]}) is None
