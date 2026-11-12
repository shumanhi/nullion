import pytest

from nullion.artifact_lifecycle import artifact_read_reference_recovery
from nullion.tools import ToolInvocation, ToolResult


@pytest.mark.parametrize('extension', ['.png', '.pdf', '.xlsx', '.docx', '.html', '.csv'])
def test_unknown_artifact_path_returns_exact_choices_without_io_or_alias_guessing(tmp_path, extension):
    # Neither path exists. Recovery must use declared provenance, never probe
    # arbitrary filesystem contents or silently choose a similar filename.
    known = str(tmp_path / ('complete-hash-839f' + extension))
    invocation = ToolInvocation('read', 'file_read', 'member', {'path': '/outside/h839f' + extension})
    prior = ToolResult('source', 'producer', 'completed', {'artifact_paths': [known]})
    result = artifact_read_reference_recovery(invocation, [prior])
    from nullion.tool_repair_contract import tool_repair_kind
    from nullion.agent_orchestrator import _should_block_failed_tool_during_recovery
    assert tool_repair_kind(result) == 'select_retained_artifact'
    assert not _should_block_failed_tool_during_recovery(result)
    from nullion.langchain_adapters import _result_allows_fallback, _tool_result_text
    import json
    assert not _result_allows_fallback(result)
    assert json.loads(_tool_result_text(result))['output']['recovery']['candidate_paths'] == [known]
    assert result.status == 'failed' and result.output['executed'] is False
    assert result.output['recovery']['candidate_paths'] == [known]
    assert result.output['recovery']['approval_bypass'] is False
    assert invocation.arguments['path'] == '/outside/h839f' + extension
    assert artifact_read_reference_recovery(invocation, [prior, result]) is None
    exact = ToolInvocation('exact', 'file_read', 'member', {'path': known})
    assert artifact_read_reference_recovery(exact, [prior]) is None


@pytest.mark.parametrize('case', ['plain_read', 'other_extension', 'failed_producer', 'relative', 'mutation'])
def test_unrelated_reads_and_mutations_keep_normal_path(tmp_path, case):
    tool = 'file_write' if case == 'mutation' else 'file_read'
    path = 'relative.png' if case == 'relative' else str(tmp_path / 'unknown.png')
    prior = [] if case == 'plain_read' else [ToolResult('source', 'producer',
        'failed' if case == 'failed_producer' else 'completed',
        {'artifact_descriptors': [{'path': str(tmp_path / ('known.pdf' if case == 'other_extension' else 'known.png'))}]})]
    assert artifact_read_reference_recovery(ToolInvocation('read', tool, 'member', {'path': path}), prior) is None


def test_original_source_path_from_native_copy_provenance_is_already_known(tmp_path):
    source = str(tmp_path/'scratch'/'source.png')
    result = ToolResult('copy','producer','completed',{'artifact_descriptors':[
        {'path':str(tmp_path/'artifacts'/'copied.png'),'source_path':source}]})
    assert artifact_read_reference_recovery(ToolInvocation('read','file_read','member',{'path':source}),[result]) is None


@pytest.mark.parametrize('value', [42, '/not-a-list.png', {'path':'/not-a-list.png'}, None])
def test_malformed_optional_artifact_collections_cannot_break_a_read(value):
    prior=ToolResult('source','producer','completed',{'artifact_paths':value,'artifact_descriptors':value})
    assert artifact_read_reference_recovery(ToolInvocation('read','file_read','member',{'path':'/another.png'}),[prior]) is None


def test_other_file_inside_current_allowed_root_does_not_need_a_correction(tmp_path):
    from nullion.tools import ToolRegistry
    prior=ToolResult('source','producer','completed',{'artifact_paths':[str(tmp_path/'known.pdf')]})
    assert artifact_read_reference_recovery(ToolInvocation('read','file_read','member',
        {'path':str(tmp_path/'different.pdf')}),[prior],registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])) is None
