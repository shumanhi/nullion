"""Native target identity and fail-closed recovery after shell inspection."""
from pathlib import Path
import shlex

import pytest

from nullion.runtime_store import RuntimeStore
from nullion.tools import (
    ToolExecutor, ToolInvocation, ToolRegistry, ToolRiskLevel, ToolSideEffectClass,
    ToolSpec, _build_terminal_exec_handler, _terminal_delete_observations,
    _terminal_delete_receipt, _terminal_delete_targets_for_invocation,
)


def invocation(command):
    return ToolInvocation('shell-target', 'terminal_exec', 'workspace_admin', {'command': command})


def test_shell_preflight_adds_no_filesystem_resolution_for_other_tools(monkeypatch):
    import nullion.tools as tools
    def prohibited_resolution(*args, **kwargs):
        pytest.fail('Nonterminal tools must not resolve a shell working directory')
    monkeypatch.setattr(tools, '_terminal_execution_cwd_from_registry', prohibited_resolution)
    registry = ToolRegistry()
    registry.register(ToolSpec('local_read', 'Read local state', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 10), lambda selected: tools.ToolResult(
            selected.invocation_id, selected.tool_name, 'completed', {'value': 1}))
    result = ToolExecutor(store=RuntimeStore(), registry=registry).invoke(
        ToolInvocation('ordinary', 'local_read', 'workspace_admin', {}))
    assert result.status == 'completed'


@pytest.mark.parametrize('command', [
    'rm -rf "$HOME/cache" 2>/dev/null',
    'rm -rf ${CACHE}/item 2>/dev/null',
    'rm -rf ~/cache 2>/dev/null',
    'rm -rf "$(printf %s /tmp/cache)"/* 2>/dev/null || true',
    'find "$HOME/cache" -type f -delete 2>/dev/null',
    'find 2>/dev/null "${CACHE}" -exec rm -- {} \\;',
    'rm "unterminated',
    'rm /tmp/cache >',
])
def test_unresolved_shell_never_reaches_approval_backend_or_receipt(tmp_path, command):
    class ProhibitedBackend:
        def run(self, *args, **kwargs):
            pytest.fail('Unresolved syntax must not execute')

    handler = _build_terminal_exec_handler(tmp_path, allowed_roots=[tmp_path],
        terminal_executor_backend=ProhibitedBackend())
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('terminal_exec', 'Reviewed shell', ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC, True, 10), handler)
    store = RuntimeStore()
    denied = ToolExecutor(store=store, registry=registry).invoke(invocation(command))
    direct = handler(invocation(command))
    assert denied.status == 'denied' and direct.status == 'failed'
    for result in (denied, direct):
        assert result.output['executed'] is False
        assert result.output['command_executed'] is False
        assert result.output['requires_approval'] is False
        assert result.output['retryable'] is True
        assert result.output['recovery']['kind'] == 'correct_arguments'
        assert result.output['recovery']['approval_bypass'] is False
        assert not result.output.get('approval_id')
        assert not result.output.get('action_receipt')
    assert not store.list_approval_requests()
    assert not list(tmp_path.iterdir())


def test_symbolic_target_is_never_canonicalized_or_certified_absent(tmp_path, monkeypatch):
    selected = invocation('rm -rf "$HOME/cache" 2>/dev/null')
    targets = _terminal_delete_targets_for_invocation(selected, execution_cwd=tmp_path)
    assert len(targets) == 1
    assert targets[0]['target'] == '"$HOME/cache"'
    assert targets[0]['target_resolution'] == 'shell_expansion_required'

    def prohibited_probe(*args, **kwargs):
        pytest.fail('A symbolic target has no native filesystem identity')

    monkeypatch.setattr(Path, 'lstat', prohibited_probe)
    observations = _terminal_delete_observations(selected, execution_cwd=tmp_path)
    receipt = _terminal_delete_receipt(observations)
    assert receipt['changes'][0]['present_before'] is None
    assert receipt['changes'][0]['present_after'] is None
    assert receipt['changes'][0]['status'] == 'unconfirmed'


@pytest.mark.parametrize('filename', ['$HOME/cache', '~/cache', '*.txt', '>', '2', ' a '])
def test_quoted_literal_identity_and_missing_receipts(tmp_path, filename):
    target = tmp_path / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b'original literal entry')
    selected = invocation('rm -- ' + shlex.quote(filename) + ' 2>/dev/null')
    targets = _terminal_delete_targets_for_invocation(selected, execution_cwd=tmp_path)
    assert [row['target'] for row in targets] == [str(target)]
    observations = _terminal_delete_observations(selected, execution_cwd=tmp_path)
    assert observations[0]['size_bytes_before'] == len(b'original literal entry')
    target.unlink()
    assert _terminal_delete_receipt(observations)['changes'][0]['status'] == 'deleted'
    missing = _terminal_delete_observations(selected, execution_cwd=tmp_path)
    assert _terminal_delete_receipt(missing)['changes'][0]['status'] == 'already_absent'


@pytest.mark.parametrize('command', [
    'find 2>/dev/null {root} -type f -delete',
    'find -P {root} -type f -exec rm -- {{}} \\; 2>/dev/null',
])
def test_find_scope_uses_shared_words_without_redirection_targets(tmp_path, command):
    root = tmp_path / '資料'
    selected = invocation(command.format(root=shlex.quote(str(root))))
    targets = _terminal_delete_targets_for_invocation(selected, execution_cwd=tmp_path)
    assert len(targets) == 1
    assert targets[0]['target'] == str(root)
    assert targets[0]['target_kind'] == 'search_scope'
    assert not _terminal_delete_observations(selected, execution_cwd=tmp_path)


def test_script_string_tilde_is_not_shell_home_expansion(tmp_path):
    selected = invocation('python3 -c ' + shlex.quote("from pathlib import Path; Path('~/victim').unlink()"))
    targets = _terminal_delete_targets_for_invocation(selected, execution_cwd=tmp_path)
    assert [row['target'] for row in targets] == [str(tmp_path / '~' / 'victim')]


@pytest.mark.parametrize('body', ['rm -- {target}', "sh -c {script}"])
def test_find_literal_nested_delete_preserves_actual_target_not_search_root(tmp_path, body):
    root = tmp_path / 'search-only'
    target = tmp_path / 'different-victim'
    nested = body.format(target=shlex.quote(str(target)), script=shlex.quote('rm -- ' + shlex.quote(str(target))))
    selected = invocation('find ' + shlex.quote(str(root)) + ' -exec ' + nested + ' \\; 2>/dev/null')
    targets = _terminal_delete_targets_for_invocation(selected, execution_cwd=tmp_path)
    assert [row['target'] for row in targets] == [str(target)]
    assert all(row.get('target_kind') != 'search_scope' for row in targets)


@pytest.mark.parametrize('root', ["'$HOME'", "'${HOME}'", "'~'", r'\$HOME'])
def test_find_broad_traversal_guard_preserves_literal_shell_roots(root):
    from nullion.tools import _terminal_filesystem_safety_denial
    assert _terminal_filesystem_safety_denial('find ' + root + ' -maxdepth 0 -delete') is None


@pytest.mark.parametrize('root', ['/', '"/"', '/Users', '$HOME', '"$HOME"', '~', '"$HOME/Library"'])
def test_find_broad_traversal_guard_still_blocks_actual_system_or_home_roots(root):
    from nullion.tools import _terminal_filesystem_safety_denial
    result = _terminal_filesystem_safety_denial('find 2>/dev/null ' + root + ' -maxdepth 0')
    assert result['reason'] == 'filesystem_traversal_denied'
