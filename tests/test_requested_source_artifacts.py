from dataclasses import replace

import pytest

from nullion.agent_orchestrator import _CompletionRequirementContractRow
from nullion.artifacts import artifact_descriptor_for_path, requested_source_artifact_candidates
from nullion.artifact_lifecycle import select_delivery_paths
from nullion.tools import ToolResult


def fixture(tmp_path, *, role='source', status='completed', tool='browser_screenshot'):
    path = tmp_path/'capture.png'
    path.write_bytes(b'current captured image')
    descriptor = {**artifact_descriptor_for_path(path, artifact_root=tmp_path).to_dict(), 'role':role}
    result = ToolResult('capture', tool, status, {'artifact_descriptors':[descriptor]})
    row = _CompletionRequirementContractRow('r1', '現在の画面を添付', 'deliverable', (tool,), 'capture')
    return path, result, row


@pytest.mark.parametrize('tool', ['browser_screenshot', 'file_read', 'file_download', 'custom_capture'])
def test_requested_current_source_promotes_only_bound_candidate(tmp_path, tool):
    path, result, row = fixture(tmp_path, tool=tool)
    assert requested_source_artifact_candidates([result], artifact_roots=[tmp_path]) == [str(path)]
    assert result.output['artifact_descriptors'][0]['role'] == 'source'
    paths = requested_source_artifact_candidates([result], artifact_roots=[tmp_path],
        requirement_contract=[row], required_extensions=['.png'], promote=True)
    assert paths == [str(path)]
    assert select_delivery_paths(paths, [result], '画像を添付しました。', required_extensions=['.png']) == (paths, False)


@pytest.mark.parametrize('problem', ['fact', 'embedded', 'wrong_tool', 'wrong_format', 'no_contract',
    'internal', 'inspection', 'failed', 'outside', 'changed_size', 'changed_version', 'relative'])
def test_research_internal_unverified_or_unrequested_sources_stay_private(tmp_path, problem):
    path, result, row = fixture(tmp_path)
    roots = [tmp_path]; formats = ['.png']; rows = [row]
    if problem == 'fact': rows = [replace(row, requirement_kind='fact')]
    if problem == 'embedded': rows = [replace(row, delivery_required=False)]
    if problem == 'wrong_tool': rows = [replace(row, eligible_evidence_tool_names=('pdf_create',))]
    if problem == 'wrong_format': formats = ['.pdf']
    if problem == 'no_contract': rows = []
    if problem in {'internal', 'inspection'}: result.output['artifact_descriptors'][0]['role'] = problem
    if problem == 'failed': result.status = 'failed'
    if problem == 'outside': roots = [tmp_path/'other']
    if problem == 'changed_size': path.write_bytes(b'changed')
    if problem == 'changed_version': result.output['artifact_descriptors'][0]['artifact_id'] = 'stale'
    if problem == 'relative': result.output['artifact_descriptors'][0]['path'] = 'capture.png'
    assert requested_source_artifact_candidates([result], artifact_roots=roots,
        requirement_contract=rows, required_extensions=formats, promote=True) == []


def test_multiple_requested_sources_still_require_final_version_selection(tmp_path):
    path, result, row = fixture(tmp_path)
    other = tmp_path/'second.png'; other.write_bytes(b'other current image')
    result.output['artifact_descriptors'].append({**artifact_descriptor_for_path(other, artifact_root=tmp_path).to_dict(), 'role':'source'})
    paths = requested_source_artifact_candidates([result], artifact_roots=[tmp_path],
        requirement_contract=[row], required_extensions=['.png'], promote=True)
    assert select_delivery_paths(paths, [result], 'Ready', required_extensions=['.png']) == ([], True)


def test_terminal_receipt_cannot_claim_success_when_requested_attachment_is_unselected():
    from nullion.agent_orchestrator import _complete_agent_turn
    from nullion.tools import ToolRegistry
    state = {'tool_results': [], 'artifacts': [], 'tool_registry': ToolRegistry(),
        'principal_id':'member', 'conversation_id':'web:capture', 'user_message':'現在の画像を送って。',
        'completion_requirement_contract_cache':{'requested_artifact_extensions':['.png']}}
    result = _complete_agent_turn(state, final_text='Current cart: Cedar.', response_fulfilled=True)['result']
    assert result.response_fulfilled is False
    assert result.artifact_delivery_satisfied is False
    assert result.final_text == 'Current cart: Cedar.'


@pytest.mark.parametrize('requested', [True, False])
def test_stale_scope_cannot_publish_evidence_only_tool_output(tmp_path, requested):
    from nullion.artifacts import demote_unrequested_source_artifacts
    path, result, row = fixture(tmp_path, role='deliverable')
    paths = demote_unrequested_source_artifacts([result], source_only_tool_names={'browser_screenshot'},
        requirement_contract=[row if requested else replace(row, requirement_kind='fact')],
        required_extensions=['.png'] if requested else [])
    assert paths == ([] if requested else [str(path)])
    assert result.output['artifact_descriptors'][0]['role'] == ('deliverable' if requested else 'source')


@pytest.mark.parametrize('selected', ['none', 'other', 'capture'])
def test_verified_capture_also_requires_selected_delivery_identity(tmp_path, monkeypatch, selected):
    from nullion import agent_orchestrator as agent
    from nullion.tools import ToolRegistry
    path = tmp_path/'capture.png'; path.write_bytes(b'valid capture')
    other = tmp_path/'other.png'; other.write_bytes(b'unrelated')
    # A positive capture-verification dependency is insufficient by itself:
    # this boundary owns the final selection, not capture verification.
    monkeypatch.setattr(agent, '_completed_required_artifact_paths_for_turn', lambda *a, **kw: (str(path),))
    paths = [] if selected=='none' else [str(other if selected=='other' else path)]
    state = {'tool_results': [], 'artifacts': paths, 'tool_registry': ToolRegistry(),
        'principal_id':'member', 'conversation_id':'web:selection', 'user_message':'画像を送って。',
        'completion_requirement_contract_cache':{'requested_artifact_extensions':['.png']}}
    result = agent._complete_agent_turn(state, final_text='Current cart: Cedar.', response_fulfilled=True)['result']
    assert result.response_fulfilled is (selected=='capture')
    assert result.artifact_delivery_satisfied is (selected=='capture')
