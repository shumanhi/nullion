import time
from types import SimpleNamespace

import pytest

from nullion.research_budget import (ResearchBudget, research_context, current_budget,
    prepare_model, prepare_model_request, call_model, ensure_research_scope)
from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolInvocation, ToolRiskLevel, ToolSideEffectClass
from nullion.turn_context_policy import TurnToolScopeDecision


def registry(*, cached=False, batch=False):
    tools = ToolRegistry(); calls = []
    def read(inv):
        calls.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value':37, 'date':'2031-04-08', 'source':'https://source.test/a'})
    spec = ToolSpec('source_read', 'Read source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5,
        capability_tags=('web',), same_request_read_cache=cached,
        independent_batch=('records','independent') if batch else ())
    tools.register(spec, read)
    tools.register(ToolSpec('local_write', 'Write requested local result', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE,
        False, 5), lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'written':True}))
    return tools, calls


def invoke(tools, number, arguments=None, principal='member', name='source_read'):
    return tools.invoke(ToolInvocation(str(number), name, principal, arguments or {}))


def test_native_limits_cannot_be_reset_by_multiple_programs_or_scope_upgrades(monkeypatch):
    monkeypatch.setenv('NULLION_RESEARCH_TOOL_CALLS', '3')
    tools, calls = registry()
    with research_context() as budget:
        assert invoke(tools, 1).status == 'completed'
        with research_context() as nested:
            assert nested is budget
            assert invoke(tools, 2).status == 'completed'
        budget.configure(tools, TurnToolScopeDecision(valid=True, requested_tool_names=('source_read',), research_effort='exhaustive'))
        assert invoke(tools, 3).status == 'completed'
        denied = invoke(tools, 4)
        assert denied.output['executed'] is False and denied.output['next_phase'] == 'writing'
        assert denied.output['reason'] == 'research_budget_exhausted'
        assert budget.effort == 'standard' and len(calls) == 3
        assert invoke(tools, 5, name='local_write').status == 'completed'


def test_independent_batch_is_counted_before_any_of_its_items_execute(monkeypatch):
    monkeypatch.setenv('NULLION_RESEARCH_TOOL_CALLS', '3')
    tools, calls = registry(batch=True)
    with research_context() as budget:
        assert invoke(tools, 1, {'records':[1,2], 'independent':True}).status == 'completed'
        denied = invoke(tools, 2, {'records':[3,4], 'independent':True})
        assert denied.status == 'failed' and len(calls) == 1
        assert budget.native_calls == 2


@pytest.mark.parametrize('effort,limit', [('standard',2), ('extended',4), ('exhaustive',8)])
def test_effort_comes_from_validated_structured_scope(monkeypatch, effort, limit):
    monkeypatch.setenv('NULLION_RESEARCH_TOOL_CALLS', '2')
    tools, calls = registry()
    with research_context() as budget:
        budget.configure(tools, TurnToolScopeDecision(valid=True, requested_tool_names=('source_read',),
            research_effort=effort, research_questions=('必要な日付', 'Fuente oficial')))
        for n in range(limit): assert invoke(tools, n).status == 'completed'
        assert invoke(tools, 99).status == 'failed'
        assert len(calls) == limit and budget.questions == ('必要な日付', 'Fuente oficial')


def test_visibility_alone_does_not_activate_a_budget_or_scan_tools():
    class Registry:
        turn_tool_scope_decision = TurnToolScopeDecision(valid=True)
        def all_specs(self): raise AssertionError('Plain chat must not scan installed tools')
    with research_context():
        assert prepare_model({'tool_registry':Registry()}) is None
        options = {'system':'plain', 'tools':[], 'timeout':20}
        prepare_model_request(options)
        assert options == {'system':'plain', 'tools':[], 'timeout':20}


def test_fixed_workflow_scope_preserves_tools_permissions_and_accepts_structured_effort(monkeypatch):
    monkeypatch.setenv('NULLION_RESEARCH_TOOL_CALLS', '2')
    tools, calls = registry()
    with research_context() as budget:
        scoped = ensure_research_scope(tools)
        assert ensure_research_scope(scoped) is scoped
        assert getattr(scoped, 'turn_tool_scope_decision', None) is None
        assert {item['name'] for item in scoped.list_tool_definitions()} == {
            'request_tool_scope', 'source_read', 'local_write'}
        assert scoped.get_spec('source_read') is tools.get_spec('source_read')
        assert scoped.get_spec('local_write').side_effect_class == ToolSideEffectClass.WRITE
        unchanged, _ = scoped.apply_scope_request(ToolInvocation('recover', 'request_tool_scope', 'member', {
            'capabilities':['web']}))
        assert unchanged.status == 'failed' and not budget.active
        result, scoped = scoped.apply_scope_request(ToolInvocation('scope', 'request_tool_scope', 'member', {
            'capabilities':['web'], 'tool_names':['source_read'], 'research_effort':'exhaustive',
            'research_questions':['必要な日付']}))
        assert result.status == 'completed' and budget.limits()['native_calls'] == 8
        assert getattr(scoped, 'turn_tool_scope_decision', None) is None
        assert budget.questions == ('必要な日付',)
        denied, _ = scoped.apply_scope_request(ToolInvocation('widen', 'request_tool_scope', 'member', {
            'capabilities':['web'], 'tool_names':['unavailable_source'], 'research_effort':'standard'}))
        assert denied.status == 'failed' and not calls
        with pytest.raises(KeyError):
            scoped.get_spec('unavailable_source')


@pytest.mark.parametrize('selected', ['local_write', 'source_read'])
def test_artifact_generation_preserves_native_timeout_without_refunding_source_selection(monkeypatch, selected):
    clock = [100.0]
    monkeypatch.setattr('nullion.research_budget.time.monotonic', lambda: clock[0])
    tools, _ = registry()
    with research_context() as budget:
        invoke(tools, 1)
        started = budget.started
        options = {'tools':[], 'system':'', 'timeout':120}
        prepare_model_request(options, production=True)
        assert options['timeout'] == 120
        def provider(**kwargs):
            clock[0] += 10
            return {'content':[{'type':'tool_use', 'name':selected, 'input':{}}]}
        call_model(provider, options, production=True)
        assert budget.started == started + (10 if selected == 'local_write' else 0)


def test_model_attempts_transition_to_writing_with_a_reserved_timeout(monkeypatch):
    monkeypatch.setenv('NULLION_RESEARCH_MODEL_CALLS', '3')
    tools, _ = registry()
    with research_context() as budget:
        invoke(tools, 0)
        for _ in range(2): prepare_model_request({'tools':[], 'system':''})
        options = {'tools':[{'name':'source_read'}, {'name':'local_write'}], 'timeout':100, 'system':''}
        prepare_model_request(options)
        assert [t['name'] for t in options['tools']] == ['local_write']
        assert budget.writing() and budget.writing_calls == 1
        assert 0 < options['timeout'] <= 60
        assert 'unresolved' in options['system']
        assert invoke(tools, 2).status == 'failed'


def test_request_snapshots_preserve_values_and_invalidate_after_write_and_between_requests():
    tools, calls = registry(cached=True)
    with research_context() as budget:
        original = invoke(tools, 1)
        repeated = invoke(tools, 2)
        assert repeated.output == original.output and repeated.output['date'] == '2031-04-08'
        assert repeated.response_requirements['_native_reused_observation']['origin_invocation_id'] == '1'
        assert budget.cache_hits == 1 and len(calls) == 1
        invoke(tools, 3, principal='another-member')
        assert len(calls) == 2
        invoke(tools, 4, name='local_write')
        invoke(tools, 5)
        assert len(calls) == 3
    with research_context(): invoke(tools, 6)
    assert len(calls) == 4


def test_read_cache_declared_context_and_read_navigation_invalidation():
    tools, calls = registry(cached=True)
    spec = tools.get_spec('source_read'); spec.read_cache_context_keys = ('document_version',)
    with research_context():
        for n, version in enumerate(['a','a','b']):
            tools.invoke(ToolInvocation(str(n),'source_read','member',{},flow_context={'document_version':version}))
        assert len(calls) == 2
        spec.invalidates_read_cache = True
        invoke(tools, 8); invoke(tools, 9)
        assert len(calls) == 4


def test_failed_read_is_not_cached():
    tools, calls = registry(cached=True)
    tools._handlers['source_read'] = lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'failed', {'reason':'source_unavailable'})
    with research_context() as budget:
        invoke(tools, 1); invoke(tools, 2)
        assert budget.native_calls == 2 and not budget.reads


def test_remaining_source_time_clamps_the_actual_native_boundary():
    tools, _ = registry()
    tools._handlers['source_read'] = lambda inv: (time.sleep(.2) or ToolResult(inv.invocation_id, inv.tool_name,'completed',{}))
    with research_context() as budget:
        budget.activate(); budget.started -= 74.97
        started = time.monotonic(); result = invoke(tools, 1)
        assert result.status == 'failed' and time.monotonic()-started < .15
        assert budget.writing()


@pytest.mark.parametrize('production', [False, True])
def test_provider_attempt_timeout_preserves_remaining_research(production):
    import threading
    release = threading.Event()
    with research_context() as budget:
        budget.activate()
        try:
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                call_model(lambda **kw: release.wait(1), {'timeout':.02}, production=production)
            assert time.monotonic()-started < .2 and not budget.writing()
            assert budget.reason is None and budget.remaining() < 75
            assert call_model(lambda **kw: {'content':[]}, {'timeout':.02}, production=production) == {'content':[]}
        finally: release.set()


def test_provider_ignoring_timeout_cannot_extend_total_research():
    import threading
    release = threading.Event()
    with research_context() as budget:
        budget.activate(); budget.started -= 74.98
        try:
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                call_model(lambda **kw: release.wait(1), {'timeout':1})
            assert time.monotonic()-started < .2 and budget.writing()
            assert budget.remaining() == 0
        finally:
            release.set()


def test_resume_restores_spent_work_without_counting_human_wait(monkeypatch):
    monkeypatch.setenv('NULLION_RESEARCH_TOOL_CALLS', '2')
    tools, _ = registry()
    with research_context() as budget:
        invoke(tools, 1); invoke(tools, 2); usage = budget.usage()
    with research_context() as restored:
        prepare_model({'tool_registry':tools, 'work_usage':usage, 'research_resume':True})
        assert restored.native_calls == 2 and restored.writing()
        assert invoke(tools, 3).status == 'failed'
    with research_context() as fresh:
        prepare_model({'tool_registry':tools, 'work_usage':usage})
        assert fresh.native_calls == 0
        assert invoke(tools, 4).status == 'completed'


def test_scope_serialization_retains_effort_and_questions():
    from nullion.turn_context_policy import _tool_scope_decision_to_payload, _tool_scope_decision_from_payload
    decision = TurnToolScopeDecision(valid=True, research_effort='exhaustive', research_questions=('全件',))
    restored = _tool_scope_decision_from_payload(_tool_scope_decision_to_payload(decision))
    assert restored.research_effort == 'exhaustive' and restored.research_questions == ('全件',)


def test_code_mode_result_projection_is_exact_and_leaves_original_values_intact():
    from nullion.code_mode import project_program_value
    value = {'facts':[{'date':'2031-04-08','source':'https://source.test/a?opaque=123456789'}], 'a/b':{'~key':37}, 'unused':'x'*20000}
    projected = project_program_value(value, ['/facts', '/a~1b/~0key'])
    assert projected['/facts'] == value['facts'] and projected['/a~1b/~0key'] == 37
    assert 'unused' not in projected and len(value['unused']) == 20000
    with pytest.raises((ValueError, KeyError)): project_program_value(value, ['/facts/-1'])
    with pytest.raises(ValueError): project_program_value(value, ['/a~2b'])


def test_productive_artifact_steps_renew_only_composition(monkeypatch):
    monkeypatch.setenv('NULLION_RESEARCH_TOOL_CALLS', '1')
    tools, calls = registry()
    with research_context() as budget:
        invoke(tools, 1)
        assert budget.writing()
        budget.writing_calls = 7
        before = budget.native_calls
        assert invoke(tools, 2, name='local_write').status == 'completed'
        assert budget.writing_calls == 0 and budget.writing()
        assert budget.native_calls == before
        assert invoke(tools, 3).status == 'failed' and len(calls) == 1


def test_native_artifact_wait_does_not_spend_composition_allowance():
    tools, _ = registry()
    def write(inv):
        budget = current_budget()
        assert budget.paused_at is not None
        before = budget.writing_remaining()
        time.sleep(.02)
        assert abs(budget.writing_remaining()-before) < .005
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {})
    tools._handlers['local_write'] = write
    with research_context() as budget:
        budget.activate(); budget.finish_research('test')
        invoke(tools, 1, name='local_write')
        assert budget.writing_remaining() > 59.9


def test_cancelled_model_call_returns_without_waiting_for_provider():
    import threading
    from concurrent.futures import CancelledError
    release = threading.Event()
    with research_context() as budget:
        budget.activate()
        try:
            started = time.monotonic()
            with pytest.raises(CancelledError):
                call_model(lambda **kw: release.wait(1), {'timeout':1}, cancellation_checker=lambda:True)
            assert time.monotonic()-started < .2
        finally: release.set()


@pytest.mark.asyncio
async def test_delegated_writer_limit_cannot_be_reported_as_success(monkeypatch):
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.task_queue import TaskResult
    tools, _ = registry()
    runner = DeepAgentMiniAgentRunner()
    async def run(*args, **kwargs):
        budget = current_budget(); budget.activate(); budget.writer_forced_stop = True
        return TaskResult('task', 'success', output='The allowance was reached.')
    monkeypatch.setattr(runner, '_run', run)
    config = SimpleNamespace(task=SimpleNamespace(task_id='task', principal_id='member'))
    result = await runner.run(config, tool_registry=tools)
    assert result.status == 'failure'
    assert result.report_presentation['fulfillment_satisfied'] is False
