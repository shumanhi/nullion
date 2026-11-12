"""Saved tasks select capabilities and notification policy, never prompt keywords."""
from types import SimpleNamespace
import json

import pytest

from nullion.cron_execution_tools import build_cron_connector_scope_decision, CronExecutionToolRegistry
from nullion.cron_delivery import _cron_report_failure_notice, _cron_safe_remaining_work
from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolInvocation


def registry_with_account_actions():
    registry = ToolRegistry()
    calls = []
    for name, side_effect, approval in [('records_scan', ToolSideEffectClass.READ, False),
            ('schedule_add', ToolSideEffectClass.ACCOUNT_WRITE, True)]:
        def handler(invocation):
            calls.append(invocation.tool_name)
            return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {})
        registry.register(ToolSpec(name=name, description=name, risk_level=ToolRiskLevel.LOW,
            side_effect_class=side_effect, requires_approval=approval, timeout_seconds=2,
            capability_tags=('connector', 'records')), handler)
    return registry, calls


@pytest.mark.parametrize('saved_request', ['Scan obligations and add missing dated events.', '期限を確認し、未登録の予定を追加。', 'Revisa los plazos y añade las citas que falten.'])
def test_scope_selects_exact_writes_and_cache_invalidates_on_task_or_permissions(tmp_path, monkeypatch, saved_request):
    registry, calls = registry_with_account_actions()
    providers = [{'provider_id':'skill_pack_connector_records', 'permission_mode':'write'}]
    monkeypatch.setattr('nullion.cron_execution_tools._connected_connector_provider_summaries', lambda _:tuple(providers))
    class Model:
        count = 0
        def create(self, **kw):
            self.count += 1
            payload = json.loads(kw['messages'][0]['content'][0]['text'])
            assert payload['available_structured_write_tools'][0]['name'] == 'schedule_add'
            return {'content':[{'type':'text','text':json.dumps({'requires_account_connector':True,
                'provider_ids':['skill_pack_connector_records'], 'confidence':0.99,
                'write_tool_names':['schedule_add'], 'delivery_mode':'report_each_run'})}]}
    model = Model()
    kwargs = dict(model_client=model, user_message=saved_request, principal_id='web:fixture', registry=registry,
                  resolve_execution_contract=True, checkpoint_path=str(tmp_path/'runtime.db'))
    first = build_cron_connector_scope_decision(**kwargs)
    assert first.write_tool_names == ('schedule_add',) and first.delivery_mode == 'report_each_run'
    assert build_cron_connector_scope_decision(**kwargs) == first
    assert model.count == 1
    assert build_cron_connector_scope_decision(**{**kwargs, 'user_message':saved_request+'\n新しい条件'}).valid
    assert model.count == 2
    # A cached grant must not outlive the connection's permission metadata.
    providers[0]['permission_mode'] = 'read'
    assert not build_cron_connector_scope_decision(**kwargs).valid
    assert model.count == 3
    assert calls == []


@pytest.mark.parametrize('selected', [['not_registered'], ['records_scan'], ['schedule_add', 'not_registered']])
def test_execution_contract_cannot_grant_invented_or_wrong_effect_tools(tmp_path, monkeypatch, selected):
    registry, _ = registry_with_account_actions()
    monkeypatch.setattr('nullion.cron_execution_tools._connected_connector_provider_summaries', lambda _:(
        {'provider_id':'skill_pack_connector_records','permission_mode':'write'},))
    class Model:
        def create(self, **kw):
            return {'content':[{'type':'text','text':json.dumps({'requires_account_connector':True,
                'provider_ids':['skill_pack_connector_records'],'confidence':1,
                'write_tool_names':selected,'delivery_mode':'report_each_run'})}]}
    result = build_cron_connector_scope_decision(model_client=Model(),user_message='確認', principal_id='web:test',
        registry=registry, resolve_execution_contract=True, checkpoint_path=str(tmp_path/'runtime.db'))
    assert not result.valid and not result.write_tool_names


def test_selected_write_still_uses_delegate_approval_boundary(tmp_path):
    registry, calls = registry_with_account_actions()
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolExecutor
    from nullion.policy import SentinelPolicy, SentinelMode
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    class Guarded:
        def __getattr__(self, name): return getattr(registry, name)
        def invoke(self, invocation):
            return ToolExecutor(store=runtime.store,registry=registry,
                sentinel_policy=SentinelPolicy(mode=SentinelMode.ASK_ALL)).invoke(invocation)
    cron_registry = CronExecutionToolRegistry(Guarded(), allowed_structured_tool_names=['records_scan','schedule_add'])
    assert 'schedule_add' in {s.name for s in cron_registry.list_specs()}
    outcome = cron_registry.invoke(ToolInvocation('add', 'schedule_add', 'web:fixture', {}))
    assert outcome.status == 'denied'
    assert outcome.output['requires_approval'] is True
    assert calls == []


def test_failure_notice_keeps_private_remaining_steps_out_of_public_copy():
    result = {'cron_completion_required':True, 'reached_iteration_limit':True,
        'cron_remaining_work':['確認済みの記録と予定を照合', 'Read /Users/example/private/state.json token=abcdefghijklmno']}
    text = _cron_report_failure_notice(result)
    assert 'execution limit' in text and '確認済みの記録と予定を照合' not in text
    assert result['cron_remaining_work'][0] == '確認済みの記録と予定を照合'
    assert '/Users/example/private' not in text and 'abcdefghijklmno' not in text
    assert len(_cron_safe_remaining_work(['a']*4)) == 1
    assert not _cron_safe_remaining_work('untyped')


@pytest.mark.parametrize('enabled', [True, False, None])
@pytest.mark.parametrize('verification_failed', [True, False])
def test_failure_notice_promises_a_scheduled_retry_only_when_recurrence_is_enabled(enabled, verification_failed):
    result = {'cron_schedule_enabled':enabled, 'cron_verification_failed':verification_failed}
    notice = _cron_report_failure_notice(result)
    assert ('next scheduled run' in notice) is (enabled is True)
    if enabled is False:
        assert 'schedule is paused' in notice and 'manually' in notice


def test_missing_skill_reference_returns_exact_installed_paths_without_guessing(monkeypatch):
    from nullion.tools import _build_skill_pack_read_handler
    monkeypatch.setattr('nullion.skill_pack_installer.read_skill_pack_reference',
                        lambda *a: (_ for _ in ()).throw(FileNotFoundError('absent')))
    monkeypatch.setattr('nullion.skill_pack_installer.list_skill_pack_reference_paths',
                        lambda *a, **kw: ('SKILL.md', 'references/予定.json'))
    result = _build_skill_pack_read_handler()(ToolInvocation('lookup','skill_pack_read','web:fixture',
                        {'pack_id':'sample/records','path':'references/absent.md'}))
    assert result.status == 'failed'
    assert result.output['reason'] == 'skill_reference_not_found'
    assert result.output['available_reference_paths'] == ['SKILL.md','references/予定.json']


@pytest.mark.parametrize('completion', ['incomplete','failed','cancelled'])
def test_interrupted_scope_response_cannot_grant_actions(tmp_path,monkeypatch,completion):
    registry,_=registry_with_account_actions()
    monkeypatch.setattr('nullion.cron_execution_tools._connected_connector_provider_summaries',lambda _:(
        {'provider_id':'skill_pack_connector_records','permission_mode':'write'},))
    class Model:
        def create(self,**kw):
            return {'completion':{'status':completion},'content':[{'type':'text','text':json.dumps({
                'requires_account_connector':True,'provider_ids':['skill_pack_connector_records'],'confidence':1,
                'write_tool_names':['schedule_add'],'delivery_mode':'report_each_run'})}]}
    result=build_cron_connector_scope_decision(model_client=Model(),user_message='確認',principal_id='web:fixture',
        registry=registry,resolve_execution_contract=True,checkpoint_path=str(tmp_path/'runtime.db'))
    assert not result.valid and not result.write_tool_names


@pytest.mark.parametrize('scheduled', [False, True])
def test_source_scope_uses_real_json_schema_and_separate_saved_setup_budget(tmp_path, monkeypatch, scheduled):
    from jsonschema import Draft202012Validator
    registry, _ = registry_with_account_actions()
    monkeypatch.setattr('nullion.cron_execution_tools._connected_connector_provider_summaries', lambda _:(
        {'provider_id':'skill_pack_connector_records','permission_mode':'write'},))
    class Model:
        def create(self, **kw):
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            schema = packet['response_schema']
            Draft202012Validator.check_schema(schema)
            value = {'requires_account_connector':True,
                'provider_ids':['skill_pack_connector_records'], 'confidence':1}
            if scheduled:
                assert kw['timeout'] == 30
                value.update(write_tool_names=['schedule_add'], delivery_mode='report_each_run')
            else:
                assert kw['timeout'] == 5
            Draft202012Validator(schema).validate(value)
            return {'content':[{'type':'text','text':json.dumps(value)}]}
    result = build_cron_connector_scope_decision(model_client=Model(), user_message='記録の期限を確認。',
        principal_id='web:budget', registry=registry, resolve_execution_contract=scheduled,
        checkpoint_path=str(tmp_path/'runtime.db'))
    assert result.valid and not result.failure_reason


def test_source_scope_timeout_diagnostic_does_not_leak_exception_payload(tmp_path, monkeypatch, caplog):
    registry, _ = registry_with_account_actions()
    monkeypatch.setattr('nullion.cron_execution_tools._connected_connector_provider_summaries', lambda _:(
        {'provider_id':'skill_pack_connector_records','permission_mode':'write'},))
    class Model:
        def create(self, **kw):
            raise TimeoutError('private-provider-payload secret=must-not-appear')
    result = build_cron_connector_scope_decision(model_client=Model(), user_message='記録の期限を確認。',
        principal_id='web:budget', registry=registry, resolve_execution_contract=True,
        checkpoint_path=str(tmp_path/'runtime.db'))
    assert not result.valid and not result.allow_connector_tools and not result.write_tool_names
    assert result.failure_reason == 'provider_timeout' and result.error_type == 'TimeoutError'
    assert 'provider_timeout' in caplog.text
    assert 'private-provider-payload' not in caplog.text and 'must-not-appear' not in caplog.text


@pytest.mark.parametrize('label', ['Deferred report','期限の確認','Informe aplazado'])
def test_model_selected_exact_query_can_run_a_paused_task_without_sentence_parsing(label):
    from nullion.agent_orchestrator import _delegated_scheduler_run_arguments_from_list_result
    scope=SimpleNamespace(turn_tool_scope_decision=SimpleNamespace(scheduler_action='run',scheduler_selection_policy='user_selected'),
        can_invoke_tool=lambda name:name=='run_cron',
        scope_required_tool_names=('run_cron',))
    # The scope contract exposes required names through a completed typed scope result.
    scoped=ToolResult('scope','request_tool_scope','completed',{'required_tool_names':['run_cron']})
    listing=ToolResult('list','list_crons','completed',{'query':label,'crons':[
        {'id':'paused-a','name':label,'enabled':False,'has_task':True},
        {'id':'other','name':'別の項目','enabled':True}]})
    result=_delegated_scheduler_run_arguments_from_list_result(listing,state={'user_message':'أي صياغة للمستخدم'},
        tool_registry=scope,tool_results=[scoped,listing])
    assert result==({'id':'paused-a'},'user_selected')
    listing.output['crons'].append({'id':'duplicate','name':label,'enabled':True})
    assert _delegated_scheduler_run_arguments_from_list_result(listing,state={'user_message':'أي صياغة للمستخدم'},
        tool_registry=scope,tool_results=[scoped,listing]) is None
