"""A selected capture extent survives discovery, caching and native execution."""
import json
from dataclasses import replace

import pytest

from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
from nullion.turn_context_policy import (ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision,
    _parse_turn_tool_scope_decision, _tool_scope_decision_from_payload, _tool_scope_decision_to_payload,
    _validated_turn_tool_scope_decision)


def registry():
    base = ToolRegistry()
    for name in ('browser_navigate', 'browser_open', 'browser_wait_for', 'browser_screenshot'):
        base.register(ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            lambda i: ToolResult(i.invocation_id, i.tool_name, 'completed', dict(i.flow_context or {})))
    return base


@pytest.mark.parametrize('quote', ['Capture the entire page.', 'ページ全体を画像でください。', 'Captura toda la página.'])
def test_extent_roundtrip_validation_and_discovery_preserve_original_selection(quote):
    base = registry()
    evidence = TurnToolEvidence(has_url_target=True, current_user_message=quote)
    selected = _parse_turn_tool_scope_decision(json.dumps({'web_action':'browser_interaction',
        'requested_tool_names':['browser_navigate','browser_screenshot'],
        'required_tool_names':['browser_screenshot'], 'browser_capture_extent':'full_page',
        'browser_capture_request_evidence':quote}))
    selected = _validated_turn_tool_scope_decision(selected, evidence=evidence, registry=base, active_connector_providers=[])
    assert selected.browser_capture_extent == 'full_page'
    restored = _tool_scope_decision_from_payload(_tool_scope_decision_to_payload(selected))
    assert restored == selected
    scoped = ScopedTurnToolRegistry(base, evidence=evidence, tool_scope_decision=restored)
    result, widened = scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','test',
        {'capabilities':['web'],'tool_names':['browser_screenshot'],'required_tool_names':['browser_screenshot'],
         'browser_capture_extent':'auto','browser_capture_request_evidence':''}))
    assert 'browser_screenshot' in result.output['available_tools']
    assert not result.output['unavailable_required_tool_names']
    assert widened.turn_tool_scope_decision.browser_capture_extent == 'full_page'
    invocation = ToolInvocation('capture','browser_screenshot','test',{'mode':'full_page'},
        flow_context={'_runtime_browser_capture_extent':'viewport'})
    captured = widened.invoke(invocation)
    assert captured.output['_runtime_browser_capture_extent'] == 'full_page'
    assert invocation.flow_context['_runtime_browser_capture_extent'] == 'viewport'


@pytest.mark.parametrize('quote', ['', 'not in the request', 'x'*1201])
def test_full_page_scope_without_current_bounded_evidence_falls_back_to_auto(quote):
    selected = TurnToolScopeDecision(valid=True, browser_capture_extent='full_page', browser_capture_request_evidence=quote)
    selected = _validated_turn_tool_scope_decision(selected, evidence=TurnToolEvidence(current_user_message='現在の画面'),
        registry=registry(), active_connector_providers=[])
    assert selected.browser_capture_extent == 'auto'


def test_required_capture_without_suffix_exposes_only_the_selected_browser_family():
    scoped = ScopedTurnToolRegistry(registry(), evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=TurnToolScopeDecision())
    result, widened = scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','test',
        {'capabilities':['web'],'tool_names':['browser_navigate','browser_screenshot'],
         'required_tool_names':['browser_navigate','browser_screenshot']}))
    assert result.status == 'completed'
    assert 'browser_screenshot' in result.output['available_tools']
    assert 'browser_screenshot' not in result.output['unavailable_required_tool_names']
    assert 'terminal_exec' not in widened.turn_tool_scope_decision.required_tool_names


@pytest.mark.parametrize('extent,mode,size,accepted', [
    ('auto','viewport',(40,30),True), ('auto','full_page',(40,300),False),
    ('full_page','viewport',(40,30),False), ('full_page','full_page',(40,300),True),
    ('auto','viewport',(40,300),False)])
def test_capture_completion_verifies_actual_mode_and_pixels(tmp_path, extent, mode, size, accepted):
    from PIL import Image
    from nullion.browser_capture_contract import completed_capture_paths
    path = tmp_path/'capture.png'; Image.new('RGB',size,'blue').save(path)
    decision = TurnToolScopeDecision(valid=True, browser_capture_extent=extent,
        requested_tool_names=('browser_navigate','browser_screenshot'))
    results = [ToolResult('nav','browser_navigate','completed',{'session_id':'owned'}),
        ToolResult('capture','browser_screenshot','completed',{'session_id':'owned','page_url':'https://example.test',
            'path':str(path),'mode':mode,'viewport_width':40,'viewport_height':30})]
    assert bool(completed_capture_paths(decision,results)) is accepted


@pytest.mark.parametrize('extent', [[], {}, 1])
def test_malformed_capture_extent_does_not_crash_scope_parser(extent):
    selected = _parse_turn_tool_scope_decision(json.dumps({'browser_capture_extent':extent}))
    assert selected.browser_capture_extent == 'auto'


def test_unscoped_verifier_does_not_invent_viewport_for_explicit_native_full_page(tmp_path):
    from PIL import Image
    from nullion.browser_capture_contract import verified_capture_paths
    path=tmp_path/'full.png'; Image.new('RGB',(40,300),'blue').save(path)
    results=[ToolResult('nav','browser_navigate','completed',{'session_id':'owned'}),
        ToolResult('shot','browser_screenshot','completed',{'session_id':'owned','page_url':'https://example.test',
            'path':str(path),'mode':'full_page','capture_extent_contract':'full_page'})]
    assert verified_capture_paths(results)==(str(path),)
    results[-1].output['mode']='unknown'
    assert not verified_capture_paths(results)


def test_native_producer_refuses_mismatched_capture_before_writing_artifact(tmp_path, monkeypatch):
    import io
    from PIL import Image
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool, BrowserScreenshotResult
    data=io.BytesIO();Image.new('RGB',(40,300),'blue').save(data,format='PNG')
    class Backend:
        async def screenshot(self,session_id,mode):
            return BrowserScreenshotResult(data=data.getvalue(),mode='full_page',requested_mode=mode,
                page_url='https://example.test',viewport_width=40,viewport_height=30,document_width=40,document_height=300)
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path))
    result=BrowserTools(Backend(),BrowserSessionPool(),BrowserPolicy()).browser_screenshot(
        ToolInvocation('capture','browser_screenshot','test',{'mode':'full_page'},
            flow_context={'_runtime_browser_capture_extent':'auto'}))
    assert result.status=='failed' and result.output['reason']=='capture_extent_mismatch'
    assert not list(tmp_path.rglob('*.png'))


def test_direct_capture_uses_selected_full_page_extent(monkeypatch,tmp_path):
    from types import SimpleNamespace
    from PIL import Image
    from nullion import chat_operator
    path=tmp_path/'full.png';Image.new('RGB',(40,300),'blue').save(path)
    calls=[]
    def invoke(store,invocation,**kwargs):
        calls.append(invocation)
        output={'session_id':'owned'}
        if invocation.tool_name=='browser_screenshot':
            output.update(path=str(path),page_url='https://example.test',mode='full_page',
                capture_extent_contract='full_page',document_width=40,document_height=300)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',output)
    monkeypatch.setattr(chat_operator,'invoke_tool_with_boundary_policy',invoke)
    scoped=ScopedTurnToolRegistry(registry(),evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=TurnToolScopeDecision(valid=True,web_action='browser_interaction',
            browser_capture_extent='full_page',browser_capture_request_evidence='Entire page',
            requested_tool_names=('browser_navigate','browser_screenshot')))
    result=chat_operator._run_browser_screenshot_tool_directly(runtime=SimpleNamespace(store=object()),
        registry=scoped,principal_id='test',turn_id='owned-turn',user_message='https://example.test')
    assert result is not None
    assert calls[-1].arguments['mode']=='full_page'
    new_turn=ScopedTurnToolRegistry(registry(),evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=TurnToolScopeDecision(valid=True,web_action='browser_interaction',
            requested_tool_names=('browser_navigate','browser_screenshot')))
    calls.clear()
    chat_operator._run_browser_screenshot_tool_directly(runtime=SimpleNamespace(store=object()),
        registry=new_turn,principal_id='test',turn_id='next-turn',user_message='https://example.test')
    assert calls[-1].arguments['mode']=='viewport'
