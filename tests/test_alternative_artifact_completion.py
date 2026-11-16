"""Changing an artifact method does not erase independent execution obligations."""
from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import _completed_alternative_artifact_methods
from nullion.runtime_store import RuntimeStore
from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision


def state_for_output(tmp_path, monkeypatch, *, effect=ToolSideEffectClass.WRITE,
                     approval=False, source_only=False, old_formats=('.csv',)):
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path/'workspaces'))
    from PIL import Image
    path = tmp_path/'generated.png'
    Image.new('RGB', (120, 80), 'navy').save(path)
    base = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    base.register(ToolSpec('old_method', 'Earlier method', ToolRiskLevel.LOW, effect,
        approval, 5, produced_artifact_extensions=old_formats, source_artifact_only=source_only), lambda inv: None)
    base.register(ToolSpec('new_method', 'Current producer', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 5, produced_artifact_extensions=('.png',)), lambda inv: None)
    registry = ScopedTurnToolRegistry(base, evidence=TurnToolEvidence(requested_extensions=('.png',)),
        tool_scope_decision=TurnToolScopeDecision(valid=True, requested_artifact_extensions=('.png',),
            requested_tool_names=('new_method',), required_tool_names=('old_method',)))
    results = [ToolResult('scope', 'request_tool_scope', 'completed', {
        'artifact_extensions':['.png'], 'required_tool_names':[],
        'unavailable_required_tool_names':['old_method']}),
        ToolResult('producer', 'new_method', 'completed', {'artifact_paths':[str(path)],
            'artifact_descriptors':[{'path':str(path), 'extension':'.png', 'role':'deliverable'}]})]
    return {'tool_registry':registry, 'tool_results':results, 'artifacts':[str(path)],
        'runtime_store':RuntimeStore(), 'principal_id':'web:fixture', 'conversation_id':'web:fixture',
        'user_message':'選択した画像を渡してください。'}


def test_native_output_retires_only_incompatible_unavailable_local_method(tmp_path, monkeypatch):
    state = state_for_output(tmp_path, monkeypatch)
    assert _completed_alternative_artifact_methods(state, {'old_method'}, {'old_method'}) == {'old_method'}
    assert not state['tool_registry'].can_invoke_tool('old_method'), 'Metadata inspection must not expose a tool'


@pytest.mark.parametrize('case', ['missing', 'empty', 'read_only', 'failed', 'source', 'outside', 'no_format', 'extra_format', 'extra_count'])
def test_no_native_deliverable_cannot_retire_a_method(tmp_path, monkeypatch, case):
    from pathlib import Path
    from dataclasses import replace
    state = state_for_output(tmp_path, monkeypatch)
    path = Path(state['artifacts'][0])
    if case == 'missing': path.unlink()
    if case == 'empty': path.write_bytes(b'')
    if case == 'read_only': state['tool_results'][-1] = replace(state['tool_results'][-1], tool_name='file_read')
    if case == 'failed': state['tool_results'][-1] = replace(state['tool_results'][-1], status='failed')
    if case == 'source': state['tool_results'][-1].output['artifact_descriptors'][0]['role'] = 'source'
    if case == 'outside': state['artifacts'] = [str(tmp_path.parent/'unowned.png')]
    if case == 'no_format':
        state['tool_registry'].turn_tool_scope_decision = replace(state['tool_registry'].turn_tool_scope_decision,
            requested_artifact_extensions=())
        state['tool_results'][0].output['artifact_extensions'] = []
    if case in {'extra_format', 'extra_count'}:
        state['tool_flow_context'] = {'required_artifact_extensions':['.png', '.pdf' if case == 'extra_format' else '.png']}
    assert not _completed_alternative_artifact_methods(state, {'old_method'}, {'old_method'})


@pytest.mark.parametrize('kwargs', [
    {'effect':ToolSideEffectClass.ACCOUNT_WRITE}, {'approval':True}, {'source_only':True},
    {'old_formats':()}, {'old_formats':('.png',)},
])
def test_action_consent_source_and_compatible_methods_remain_required(tmp_path, monkeypatch, kwargs):
    state = state_for_output(tmp_path, monkeypatch, **kwargs)
    assert not _completed_alternative_artifact_methods(state, {'old_method'}, {'old_method'})


def test_other_missing_action_and_explicit_task_tool_obligation_remain_required(tmp_path, monkeypatch):
    from nullion.task_frames import TaskFrameStatus
    state = state_for_output(tmp_path, monkeypatch)
    assert not _completed_alternative_artifact_methods(state, {'old_method', 'account_action'}, {'old_method'})
    frame = SimpleNamespace(status=TaskFrameStatus.ACTIVE,
        finish=SimpleNamespace(required_tool_completion=('old_method',), requires_artifact_delivery=True,
            required_artifact_kind='png'), output=SimpleNamespace(artifact_kind='png'))
    state['runtime_store'] = SimpleNamespace(get_active_task_frame_id=lambda cid:'owned', get_task_frame=lambda fid:frame)
    assert not _completed_alternative_artifact_methods(state, {'old_method'}, {'old_method'})


def test_shared_artifact_guidance_uses_format_capabilities_and_rendered_layout(tmp_path):
    from datetime import UTC, datetime
    from nullion.chat_operator import _chat_delivery_contract_prompt
    from nullion.runtime import PersistentRuntime
    from nullion.reasoning_guidance import ARTIFACT_PRODUCER_SELECTION_GUIDANCE
    runtime = PersistentRuntime(store=RuntimeStore(), checkpoint_path=tmp_path/'runtime.db', started_at=datetime.now(UTC))
    prompt = _chat_delivery_contract_prompt(runtime, principal_id='web:fixture')
    assert 'write it with file_write' not in prompt
    assert ARTIFACT_PRODUCER_SELECTION_GUIDANCE in prompt
    assert 'measure and wrap text within each panel' in prompt
    assert 'actual final rendered pixels' in prompt
    assert 'metadata or dimensions alone are not visual inspection' in prompt
