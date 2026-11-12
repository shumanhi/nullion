"""Compact capability facts retain registered schema limits and cache ownership."""
import json
from dataclasses import replace
import pytest
from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult


def index(registry):
    from nullion.chat_operator import _compact_tool_scope_awareness_prompt
    return json.loads(_compact_tool_scope_awareness_prompt(registry).splitlines()[1])


def test_live_tracker_limits_are_available_before_scope_without_runtime_loads(monkeypatch):
    from nullion import chat_operator as operator
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.live_tracking import MIN_INTERVAL,MAX_INTERVAL,DEFAULT_INTERVAL
    registry=ToolRegistry();register_live_tracker_tools(registry)
    def forbidden(*args,**kw):raise AssertionError('Compact awareness loaded optional context')
    monkeypatch.setattr(operator,'_load_settings_if_available',forbidden)
    monkeypatch.setattr(operator,'active_connector_provider_context_snapshot',forbidden)
    monkeypatch.setattr(operator,'_chat_capability_inventory_prompt',forbidden)
    facts={row['tool_name']:row for row in index(registry)['tool_contracts']}
    creation=facts['create_live_tracker']
    assert creation['parameters']['interval_seconds']=={'type':'integer','minimum':MIN_INTERVAL,'maximum':MAX_INTERVAL,'default':DEFAULT_INTERVAL}
    assert creation['parameters']['duration_seconds']=={'type':'integer','minimum':1}
    assert creation['required_parameters']==registry.get_spec('create_live_tracker').input_schema['required']
    assert creation['capability_tags']==['live_tracking']
    assert 'probe' not in creation['parameters'] and 'matcher' not in creation['parameters']
    assert 'maximum' not in facts['resume_live_tracker']['parameters']['duration_seconds']
    assert 'expires_at' in facts['create_live_tracker']['parameters']
    assert facts['update_live_tracker']['capability_tags']==['live_tracking']
    original=registry.list_specs;calls=[]
    monkeypatch.setattr(registry,'list_specs',lambda:(calls.append(True) or original()))
    for _ in range(20):assert index(registry)['tool_contracts']==list(facts.values())
    assert not calls,'Warm awareness rebuilt metadata on the hot path'


@pytest.mark.parametrize('name',['observe_cycle','観測周期','intervalo_actual'])
def test_discovery_projects_opted_in_schema_without_tool_name_or_english_rules(name):
    from nullion.chat_operator import _chat_tool_registry_signature
    registry=ToolRegistry()
    schema={'type':'object','properties':{'period':{'type':'integer','minimum':7,'maximum':73,'default':17},
        'secret':{'type':'string','default':'PRIVATE_TOKEN'},'payload':{'type':'object'}},'required':['period']}
    spec=ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,1,input_schema=schema)
    handler=lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{})
    registry.register(spec,handler)
    before=_chat_tool_registry_signature(registry)
    assert index(registry)['tool_contracts']==[]
    registry.unregister(name);registry.register(replace(spec,discovery_parameters=('period',)),handler)
    after=_chat_tool_registry_signature(registry)
    assert before!=after,'Persistent capability cache ignores discovery metadata'
    row=index(registry)['tool_contracts'][0]
    assert row['tool_name']==name and row['parameters']=={'period':{'type':'integer','minimum':7,'maximum':73,'default':17}}
    assert 'PRIVATE_TOKEN' not in json.dumps(row)
    registry.unregister(name)
    schema['properties']['period']['minimum']=11
    registry.register(replace(spec,discovery_parameters=('period',)),handler)
    assert index(registry)['tool_contracts'][0]['parameters']['period']['minimum']==11


def test_full_context_keeps_contract_facts_when_task_tools_are_hidden(tmp_path,monkeypatch):
    from nullion import chat_operator as operator,builder_capabilities,system_context,web_research_policy,config
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.live_tracking import MAX_INTERVAL
    from nullion.turn_context_policy import PlainNoToolFastPathRegistry
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=ToolRegistry();register_live_tracker_tools(registry);runtime.active_tool_registry=registry
    hidden=PlainNoToolFastPathRegistry(registry)
    monkeypatch.setattr(operator,'_load_settings_if_available',lambda:None)
    monkeypatch.setattr(operator,'active_connector_provider_context_snapshot',lambda **kw:{})
    monkeypatch.setattr(system_context,'build_system_context_snapshot',lambda **kw:None)
    monkeypatch.setattr(system_context,'format_compact_system_context_for_prompt',lambda snapshot:'Registered runtime')
    monkeypatch.setattr(builder_capabilities,'format_installed_dependency_context',lambda runtime:'')
    monkeypatch.setattr(web_research_policy,'format_web_research_guidance',lambda **kw:'')
    prompt=operator._chat_capability_inventory_prompt(runtime,tool_registry=hidden)
    facts=json.loads(prompt.split('Registered tool contracts: ',1)[1].splitlines()[0])
    assert next(row for row in facts if row['tool_name']=='create_live_tracker')['parameters']['interval_seconds']['maximum']==MAX_INTERVAL
    assert hidden.list_specs()==[],'Discovery must not expose executable task tools'


def test_browser_delegation_is_discoverable_without_loading_or_launching_sdk(monkeypatch):
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin import browser_use_adapter
    from nullion.tools import ToolRegistry
    monkeypatch.setattr(browser_use_adapter,'browser_use_available',lambda:True)
    registry=ToolRegistry()
    calls=[]
    register_browser_tools(registry,model_client_getter=lambda:calls.append('model'))
    value=index(registry)
    rows=[row for row in value['tool_contracts'] if row['tool_name']=='browser_run_task']
    assert len(rows)==1
    assert rows[0]['parameters']=={'max_steps':{'type':'integer','minimum':1,'maximum':30}}
    assert 'multi-step forms' in rows[0]['description']
    assert 'lightweight readers' in rows[0]['description']
    assert calls==[]
    assert 'required_scope_only' in registry.get_spec('browser_run_task').capability_tags
    assert index(registry)==value and calls==[]
