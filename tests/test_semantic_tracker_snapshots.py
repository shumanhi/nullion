import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from nullion.live_tracking import (LiveTrackerEngine, LiveTrackerStore, Observation,
    evaluate_semantic, validate_prepared_observations, LiveTrackerPlanError, utcnow)
from nullion.tools import ToolInvocation, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass
from tests.test_live_tracking import fixture, create


@pytest.mark.parametrize('overview', ['概要', '', None])
def test_structured_alert_sections_render_without_parsing_finding_prose(overview):
    from nullion.chat_text import structured_user_message
    diagnostics = {}
    rendered = structured_user_message({'summary':overview, 'sections':[
        {'heading':'集計', 'items':['43 → 45', '31 → 34']},
        {'heading':'Cambios', 'items':['Resultado confirmado']} ]}, diagnostics=diagnostics, preferences=SimpleNamespace(markdown_style="plain", emoji_level="none"))
    assert '集計\n• 43 → 45\n• 31 → 34\n\nCambios\n• Resultado confirmado' in rendered
    assert diagnostics['presentation'] == 'sections' and diagnostics['section_count'] == 2
    assert not rendered.startswith('\n')


def test_presentation_error_preserves_the_verified_summary_instead_of_blocking_alert():
    from nullion.chat_text import structured_user_message
    diagnostics = {}
    assert structured_user_message({'summary':'Valid finding', 'sections':[{'heading':'Malformed','items':'wrong'}]},
        diagnostics=diagnostics) == 'Valid finding'
    assert diagnostics['presentation'] == 'summary_fallback'


def test_continuous_structured_condition_rearms_without_repeating_an_unchanged_match(fixture):
    f=fixture; job=create(f, stop_after_match=False, expires_at=utcnow()+timedelta(hours=1))
    outcomes=[Observation('matched','Value 23',match_keys=('23',)),
              Observation('matched','Value 23',match_keys=('23',)),
              Observation('no_match'), Observation('matched','Value 23',match_keys=('23',))]
    for outcome in outcomes:
        current,token=f.store.claim(4,now=utcnow()+timedelta(seconds=10))
        assert f.store.finish(current['id'],token,outcome)
        assert f.store.get(job['id'])['state']=='active'
    with f.store.connection() as connection:
        assert connection.execute("select count(*) from live_tracker_notifications where tracker_id=? and kind='matched'",(job['id'],)).fetchone()[0]==2
    for _ in range(3):
        current,token=f.store.claim(4,now=utcnow()+timedelta(seconds=1000))
        f.store.finish(current['id'],token,Observation('source_error',reason='source_timeout'))
    saved=f.store.get(job['id'])
    assert saved['state']=='blocked' and saved['consecutive_errors']==3
    f.store.stop(job['id'],'workspace_admin')
    with f.store.connection() as connection:
        assert not connection.execute("select 1 from live_tracker_notifications where tracker_id=? and state in ('pending','sending')",(job['id'],)).fetchone()


def test_continuous_record_monitor_keeps_durable_item_deduplication(fixture):
    f=fixture;job=create(f,stop_after_match=False,matcher={'kind':'semantic','instruction':'新しい記事'})
    for outcome in [Observation('matched','New record',match_keys=('record-a',)),Observation('no_match'),
                    Observation('matched','Old record',match_keys=('record-a',))]:
        current,token=f.store.claim(4,now=utcnow()+timedelta(seconds=10));f.store.finish(current['id'],token,outcome)
    with f.store.connection() as connection:
        assert connection.execute("select count(*) from live_tracker_notifications where tracker_id=? and kind='matched'",(job['id'],)).fetchone()[0]==1


@pytest.mark.parametrize('pointer', ['/text', '/observations/0/text'])
@pytest.mark.parametrize('label', ['Result', '結果', 'Resultado'])
def test_grounded_multi_change_alert_is_not_rejected_for_presentation_length(pointer, label):
    summary = '\n'.join(f'{label} {i}: total increased from {i} to {i+10}; the updated count is confirmed.'
                        for i in range(24))
    assert len(summary) > 700
    response = {'content': [{'type': 'text', 'text': json.dumps({'status':'matched', 'summary':summary,
        'evidence':[{'step':0, 'pointer':pointer, 'kind':'quote', 'value':summary}]})}]}
    outcome = evaluate_semantic({'kind':'semantic', 'history_mode':'snapshot', 'instruction':'Compare results'},
        [{'text':summary}], SimpleNamespace(create=lambda **kw:response), previous_observations=[{'text':'Earlier results'}])
    assert outcome.status == 'matched' and outcome.summary == summary
    assert outcome.match_evidence[0]['pointer'] == '/text'
    assert outcome.diagnostics['summary_chars'] == len(summary)


@pytest.mark.parametrize('pointer', ['/observations/1/text', '/snapshot_history/previous_observations/0/text', '/missing'])
def test_packet_pointer_must_resolve_to_the_declared_current_step(pointer):
    response = {'content':[{'type':'text','text':json.dumps({'status':'matched','summary':'Changed',
        'evidence':[{'step':0,'pointer':pointer,'kind':'quote','value':'only in another observation'}]})}]}
    outcome = evaluate_semantic({'kind':'semantic','instruction':'Compare'},
        [{'text':'Current first source'}, {'text':'only in another observation'}],
        SimpleNamespace(create=lambda **kw:response), previous_observations=[{'text':'only in another observation'}])
    assert outcome.reason == 'evidence_invalid'
    assert outcome.diagnostics['invalid_evidence'] == 'pointer_missing'


@pytest.mark.parametrize('condition', ['Notify when the reading changes, ignoring the clock.',
    '時刻の表示を除き、測定値が変化したら通知してください。', 'Avísame cuando cambie la lectura, no el reloj.'])
def test_snapshot_comparison_persists_across_worker_restart_and_preserves_last_good_read(fixture, condition):
    f = fixture
    readings = iter([10, 10, 999, 11]); packets = []
    def read(inv):
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'reading': next(readings), 'clock': len(packets)})
    f.registry.register(ToolSpec('opaque_read', 'Current reading', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Model:
        def create(self, **kw):
            p = json.loads(kw['messages'][0]['content'][0]['text']); packets.append(p)
            h = p['snapshot_history']; value = p['observations'][0]['reading']
            if value == 999:
                decision = {'status': 'unknown', 'reason': 'stale_data'}
            elif h['baseline_established'] and value != h['previous_observations'][0]['reading']:
                decision = {'status': 'matched', 'summary': 'The reading changed to 11.',
                    'evidence': [{'step': 0, 'pointer': '/reading', 'kind': 'value', 'value': value}]}
            else:
                decision = {'status': 'no_match'}
            return {'content': [{'type': 'text', 'text': json.dumps(decision)}]}
    job = create(f, probe=[{'tool_name': 'opaque_read', 'arguments': {}}],
        expires_at=utcnow() + timedelta(hours=1),
        matcher={'kind': 'semantic', 'history_mode': 'snapshot', 'instruction': condition})
    for index in range(4):
        store = LiveTrackerStore(f.store.path)
        engine = LiveTrackerEngine(store, registry=f.registry, runtime_store=f.runtime.store,
            model_client=Model(), deliver=lambda *a: True)
        current, token = store.claim(4, now=utcnow() + timedelta(seconds=1000))
        engine.check_once(current, token)
        saved = store.get(job['id'])
        if index < 3:
            assert saved['comparison_snapshot']['outputs'][0]['reading'] == 10
        else:
            assert saved['state'] == 'matched'
            assert saved['comparison_snapshot']['outputs'][0]['reading'] == 11
    assert not packets[0]['snapshot_history']['baseline_established']
    assert packets[-1]['snapshot_history']['previous_observations'][0]['reading'] == 10
    assert saved['checks'] == 4


def test_clipped_source_is_rejected_before_setup_and_never_spends_an_evaluator_call():
    output = {'elements': [{'text': 'partial'}], 'observation_coverage': {'complete': False}}
    matcher = {'kind': 'semantic', 'history_mode': 'snapshot', 'instruction': '観測'}
    step = {'tool_name': 'opaque_read', 'arguments': {}}
    with pytest.raises(LiveTrackerPlanError) as caught:
        validate_prepared_observations([step], matcher, {json.dumps(step, sort_keys=True): output})
    assert caught.value.validation['code'] == 'incomplete_observation'
    model = SimpleNamespace(create=lambda **kw: pytest.fail('Incomplete source invoked the evaluator'))
    assert evaluate_semantic(matcher, [output], model).reason == 'coverage_incomplete'


def test_first_snapshot_cannot_alert_even_if_evaluator_returns_a_present_value():
    model = SimpleNamespace(create=lambda **kw: {'content': [{'type': 'text', 'text': json.dumps({
        'status': 'matched', 'summary': 'Present value', 'evidence': [
            {'step': 0, 'pointer': '/reading', 'kind': 'value', 'value': 10}]})}]})
    out = evaluate_semantic({'kind': 'semantic', 'history_mode': 'snapshot', 'instruction': '変化'}, [{'reading': 10}], model)
    assert out.status == 'no_match' and not out.match_keys
    assert out.comparison_snapshot['outputs'] == [{'reading': 10}]


def test_unverified_render_readiness_cannot_register_a_polling_source():
    output = {'text': 'A page shell', 'observation_readiness': {'verified': False,
        'supported_parameters': ['ready_selector', 'ready_text']}}
    step = {'tool_name': 'opaque_read', 'arguments': {}}
    with pytest.raises(LiveTrackerPlanError) as error:
        validate_prepared_observations([step], {'kind':'semantic','instruction':'変化','history_mode':'snapshot'},
            {json.dumps(step, sort_keys=True): output})
    assert error.value.validation['code'] == 'source_readiness_unverified'


def test_prepared_success_seeds_the_same_comparison_used_by_the_first_background_check(fixture):
    from nullion.live_tracking_tools import register_live_tracker_tools
    f = fixture; packets = []
    class Model:
        def create(self, **kw):
            packets.append(json.loads(kw['messages'][0]['content'][0]['text']))
            return {'content': [{'type': 'text', 'text': '{"status":"no_match"}'}]}
    register_live_tracker_tools(f.registry, model_client_getter=lambda: Model())
    f.registry.live_tracker_store_path = f.store.path
    probe = [{'tool_name': 'sensor_read', 'arguments': {}}]
    result = f.registry.invoke(ToolInvocation('new', 'create_live_tracker', 'web:alpha', {
        'name': 'Cambios', 'duration_seconds': 3600, 'probe': probe,
        'matcher': {'kind': 'semantic', 'history_mode': 'snapshot', 'instruction': 'Avísame de cambios'}},
        flow_context={'conversation_id': 'web:alpha', 'prepared_observations': {
            json.dumps(probe[0], sort_keys=True): {'reading': 23}}}))
    assert result.status == 'completed', result
    job = f.store.get(result.output['id'])
    assert job['comparison_snapshot']['outputs'] == [{'reading': 23}]
    # Receipt-owned delivery gate is separate from checking comparison state.
    state = {'job': job, 'outputs': [{'reading': 23}]}
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store,
        model_client=Model(), deliver=lambda *a: True)
    assert engine._evaluate(state)['outcome'].status == 'no_match'
    assert packets[-1]['snapshot_history']['baseline_established'] is True


def test_changed_source_binding_does_not_compare_an_unrelated_previous_page(fixture):
    from nullion.live_tracking import snapshot_plan_binding
    f = fixture; probe = [{'tool_name': 'sensor_read', 'arguments': {}}]; packets = []
    job = create(f, matcher={'kind': 'semantic', 'history_mode': 'snapshot', 'instruction': '変化'},
        initial_comparison_snapshot={'outputs': [{'reading': 1}], 'plan_binding': snapshot_plan_binding(probe)})
    job['probe_contexts'] = [{'observation_setup': [{'tool_name': 'other_read', 'arguments': {}}]}]
    model = SimpleNamespace(create=lambda **kw: packets.append(json.loads(kw['messages'][0]['content'][0]['text']))
        or {'content': [{'type': 'text', 'text': '{"status":"no_match"}'}]})
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store,
        model_client=model, deliver=lambda *a: True)
    engine._evaluate({'job': job, 'outputs': [{'reading': 23}]})
    assert packets[0]['snapshot_history'] == {'baseline_established': False, 'previous_observations': None}


@pytest.mark.parametrize('mode', ['snapshot','records'])
def test_resume_comparison_does_not_discard_the_first_real_change(fixture, mode):
    from nullion.live_tracking import snapshot_plan_binding
    f = fixture; probe = [{'tool_name': 'sensor_read', 'arguments': {}}]
    job = create(f, matcher={'kind': 'semantic', 'history_mode': mode, 'instruction': '変化'},
        initial_comparison_snapshot={'outputs': [{'reading': 10}], 'plan_binding': snapshot_plan_binding(probe)})
    if mode == 'records':
        initial, initial_token = f.store.claim(4)
        f.store.finish(initial['id'], initial_token, Observation('no_match', observation_baseline_set=True))
    f.store.stop(job['id'], 'workspace_admin')
    resumed = f.store.resume(job['id'], 'workspace_admin')
    assert not resumed.get('baseline_on_next_match')
    current, token = f.store.claim(4)
    f.store.finish(current['id'], token, Observation('matched', summary='Changed to 23',
        match_keys=('23',), match_evidence=({'step': 0, 'pointer': '/reading', 'value': 23},)))
    assert f.store.get(job['id'])['state'] == 'matched'


def test_replayed_creation_cannot_claim_a_stopped_tracker_is_running(fixture):
    from nullion.live_tracking_tools import register_live_tracker_tools
    f = fixture; register_live_tracker_tools(f.registry)
    f.registry.live_tracker_store_path = f.store.path
    args = {'name':'観測','duration_seconds':60,'probe':[{'tool_name':'sensor_read','arguments':{}}],
        'matcher':{'kind':'structured','conditions':[{'step':0,'pointer':'/reading','op':'equals','value':100}], 'summary':'Ready'}}
    def invoke():return f.registry.invoke(ToolInvocation('same','create_live_tracker','web:alpha',args,
        flow_context={'conversation_id':'web:alpha'}))
    first = invoke(); assert first.status == 'completed'
    f.store.stop(first.output['id'], 'workspace_admin')
    replay = invoke()
    assert replay.status == 'completed' and replay.output['state'] == 'stopped'
    assert 'deferred_execution' not in replay.output
    assert replay.output['action_receipt']['action'] == 'unchanged'
    assert 'is stopped' in replay.output['message'] and 'Checking every' not in replay.output['message']


@pytest.mark.parametrize('parameter', ['ready_selector', 'ready_text'])
@pytest.mark.parametrize('timeout_kind', ['native', 'playwright'])
def test_native_readiness_timeout_uses_seconds_and_returns_repairable_source_error(parameter, timeout_kind):
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    calls = []
    class Backend:
        async def wait_for(self, session, selector, url, text, timeout):
            calls.append((selector, text, timeout))
            assert timeout == 5, 'Backend timeout units are seconds, not milliseconds'
            if timeout_kind == 'playwright':
                from playwright.async_api import TimeoutError as PlaywrightTimeoutError
                raise PlaywrightTimeoutError('No matching results landmark')
            raise TimeoutError('No matching results landmark')
    tools = BrowserTools(Backend(), BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_extract_text(ToolInvocation('read', 'browser_extract_text', 'web:test',
        {parameter: '#results' if parameter == 'ready_selector' else 'Observed label'}))
    assert len(calls) == 1
    assert result.status == 'failed' and result.output['reason'] == 'source_not_ready'
    assert result.output['recovery']['argument_constraints']['wait_seconds'] == 5
    from nullion.tool_repair_contract import tool_repair_kind
    assert tool_repair_kind(result)=='correct_arguments'


def test_three_readiness_failures_pause_with_a_specific_reason_and_repair_action(fixture):
    from datetime import datetime
    f = fixture
    f.registry._handlers['sensor_read'] = lambda inv: ToolResult(inv.invocation_id, inv.tool_name,
        'failed', {'failure_reason':'source_not_ready'}, 'Private endpoint detail')
    job = create(f, expires_at=utcnow()+timedelta(hours=1))
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store, deliver=lambda *a: True)
    now = utcnow()
    for _ in range(3):
        current, token = f.store.claim(4, now)
        outcome = engine._observe({'job':current})['outcome']
        assert outcome.reason == 'source_not_ready'
        f.store.finish(job['id'], token, outcome, now)
        now = datetime.fromisoformat(f.store.get(job['id'])['next_check_at'])
    assert f.store.get(job['id'])['state'] == 'blocked'
    notice = f.store.claim_notice(now)
    assert 'readiness landmark did not appear' in notice['text']
    assert 'repair its saved readiness landmark' in notice['text']
    assert 'Private endpoint detail' not in notice['text']


def test_verified_probe_repair_binds_the_saved_snapshot_to_the_replacement(fixture):
    from nullion.live_tracking import snapshot_plan_binding
    f=fixture
    original={'tool_name':'sensor_read','arguments':{'selector':'#old'}}
    replacement={'tool_name':'sensor_read','arguments':{'selector':'#current'}}
    job=create(f,probe=[original],matcher={'kind':'semantic','history_mode':'snapshot','instruction':'測定値の変化'},
        expires_at=utcnow()+timedelta(hours=1))
    current,token=f.store.claim(1)
    previous={'outputs':[{'reading':23}],'plan_binding':snapshot_plan_binding(job['probe'],job['probe_contexts'])}
    assert f.store.finish(job['id'],token,Observation('no_match',comparison_snapshot=previous),
        probe_repairs=[{'step':0,'original':original,'replacement':replacement}])
    saved=f.store.get(job['id'])
    assert saved['probe']==[replacement]
    assert saved['comparison_snapshot']['outputs']==[{'reading':23}]
    assert saved['comparison_snapshot']['plan_binding']==snapshot_plan_binding(saved['probe'],saved['probe_contexts'])
