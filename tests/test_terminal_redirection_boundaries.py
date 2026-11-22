"""Shell syntax remains distinct from literal filesystem operands."""
import pytest

from nullion.tool_boundaries import extract_boundary_facts, _shell_lexemes, _shell_segments, _shell_command_words
from nullion.tools import ToolInvocation


def facts(command):
    return extract_boundary_facts(ToolInvocation('syntax', 'terminal_exec', 'operator', {'command': command}))


def targets(command):
    return {(fact.operation, fact.target) for fact in facts(command)}


@pytest.mark.parametrize('redirect', ['2>/dev/null', '2> /dev/null', '2>&1', '2>&1 >/dev/null',
    '>/dev/null 2>&1', '3<&0 4>&- 5>&1-'])
def test_redirection_syntax_never_becomes_a_delete_operand(redirect):
    assert targets('rm -rf -- /tmp/削除対象 ' + redirect + ' || true') == {('delete', '/tmp/削除対象')}


@pytest.mark.parametrize('command,expected', [
    ('rm -- 2>/dev/null', set()),
    ('rm -- 2 >/dev/null', {('delete', '2')}),
    ("rm -- '2'>/dev/null", {('delete', '2')}),
    ('rm -- \\2>/dev/null', {('delete', '2')}),
    ("rm -- '>' /dev/null", {('delete', '>'), ('delete', '/dev/null')}),
    ('rm -- \\> /dev/null', {('delete', '>'), ('delete', '/dev/null')}),
    ("rm -- '2>/dev/null'", {('delete', '2>/dev/null')}),
    ('2>/dev/null rm -- /tmp/real', {('delete', '/tmp/real')}),
    ('rm -- /tmp/real 2> /tmp/log', {('delete', '/tmp/real'), ('write', '/tmp/log')}),
    ('rm -- /tmp/real < /tmp/input', {('delete', '/tmp/real'), ('read', '/tmp/input')}),
    ('rm -- /dev/null', {('delete', '/dev/null')}),
])
def test_fd_adjacency_quotes_and_redirect_targets_keep_distinct_meanings(command, expected):
    assert targets(command) == expected


@pytest.mark.parametrize('operator', ['>', '>>', '>|', '&>', '&>>', '<>'])
def test_real_output_redirects_preserve_file_write_boundaries(operator):
    assert targets('printf %s x ' + operator + ' "/tmp/registro > 2"') == {('write', '/tmp/registro > 2')}


def test_shared_command_words_exclude_redirects_before_option_termination():
    segments = _shell_segments(_shell_lexemes("2>/dev/null rm -- '>' 2 > /tmp/log"))
    words, redirects = _shell_command_words(segments[0])
    assert [word.value for word in words] == ['rm', '--', '>', '2']
    assert [(redirect.operator, redirect.target.value, redirect.descriptor) for redirect in redirects] == [
        ('>', '/dev/null', '2'), ('>', '/tmp/log', None)]
    assert words[2].quoted is True


def test_compound_commands_comments_continuations_and_literal_newlines():
    command = "(rm -- uno\\\n'dos' 2>/dev/null) &&\n# rm -- ignored\nrm -- '三\n四'; touch /tmp/final"
    assert targets(command) == {('delete', 'unodos'), ('delete', '三\n四'), ('write', '/tmp/final')}


@pytest.mark.parametrize('command', [
    "cat <<'EOF'\nrm -- /dev/null\nEOF\nrm -- /tmp/real",
    "cat <<EOF\nrm -- /dev/null\nEOF\nrm -- /tmp/real",
    "cat <<A <<-B\nrm -- /dev/null\nA\n\trm -- ignored\n\tB\nrm -- /tmp/real",
])
def test_here_document_data_is_not_an_executed_delete(command):
    assert targets(command) == {('delete', '/tmp/real')}


@pytest.mark.parametrize('command', ['rm "unterminated', 'rm /tmp/a >', 'cat <<EOF\nnot terminated',
    'for x in a; do rm "$x"; done', 'rm /tmp/a &&', '(rm /tmp/a', 'rm /tmp/a)',
    "printf $'extended quoting'", 'cat <<EOF\n$(rm /tmp/a)\nEOF'])
def test_unsupported_or_malformed_syntax_returns_explicit_nonexecutable_parse_fact(command):
    result = facts(command)
    assert len(result) == 1
    assert result[0].attributes['target_resolution'] == 'shell_parse_required'
    assert result[0].attributes['shell_expression'] == command


@pytest.mark.parametrize('expression', ['$HOME/cache', '${CACHE}/item', '"$(brew --cache)"/*', '~/cache'])
def test_dynamic_mutation_target_retains_exact_expression_without_evaluation(expression):
    result = [fact for fact in facts('rm -rf -- ' + expression + ' 2>/dev/null') if fact.operation == 'delete']
    assert len(result) == 1
    assert result[0].attributes['target_resolution'] == 'shell_expansion_required'
    assert result[0].attributes['shell_expression'] == expression


@pytest.mark.parametrize('expression,value', [("'$HOME/cache'", '$HOME/cache'), ("'~/cache'", '~/cache'),
    ("'*.txt'", '*.txt'), ('"a>b"', 'a>b')])
def test_quoted_literals_are_not_reexpanded(expression, value):
    result = facts('rm -- ' + expression)
    assert result[0].target == value
    assert result[0].attributes['shell_quoted_literal'] is True
    assert 'target_resolution' not in result[0].attributes


def test_substitution_and_literal_nested_shell_commands_use_the_same_inspection():
    assert targets('printf "%s" "$(rm -- /tmp/nested)"') == {('delete', '/tmp/nested')}
    assert targets("sh -c 'rm -- /dev/null'") == {('delete', '/dev/null')}
    assert targets('printf "%s" $((2 > 1))') == set()
    nested = facts("sh -c 'rm -- \"$1\"' sh /tmp/target")
    assert nested[0].attributes['target_resolution'] == 'shell_expansion_required'


def test_original_cleanup_syntax_has_no_phantom_delete_paths():
    result = facts('rm -rf "$(brew --cache)"/* 2>/dev/null || true')
    assert [(fact.operation, fact.target) for fact in result] == [('delete', '$(brew --cache)/*')]
    assert result[0].attributes['shell_expression'] == '"$(brew --cache)"/*'


@pytest.mark.parametrize('command', ['if rm /tmp/victim; then :; fi', 'time rm /tmp/victim',
    'exec rm /tmp/victim', "builtin eval 'rm /tmp/victim'", "printf '%s\\n' /tmp/victim | xargs rm",
    'printf "%s" "$(( $(rm /tmp/victim) + 1 ))"'])
def test_unresolved_modifiers_and_nested_arithmetic_fail_closed(command):
    assert any(fact.attributes.get('target_resolution') == 'shell_parse_required' for fact in facts(command))


@pytest.mark.parametrize('space', ['\u00a0', '\u3000'])
def test_unicode_spacing_is_a_literal_filename_character(space):
    assert targets('rm -- ' + space + 'archivo') == {('delete', space + 'archivo')}


def test_find_exec_literal_shell_body_keeps_its_own_delete_target():
    command = "find /tmp/search-root -exec sh -c 'rm -- /tmp/different-file' \\;"
    assert targets(command) == {('delete', '/tmp/different-file')}
    assert targets('find /tmp/search-root -exec rm -- /tmp/different-file \\;') == {('delete', '/tmp/different-file')}


@pytest.mark.parametrize('command', ['cd /tmp/other && rm relative', 'pushd /tmp/other; touch relative',
    'popd; printf x > relative', "sh -c 'cd /tmp/other; rm relative'",
    "cd /tmp/other; sh -c 'rm relative'"])
def test_directory_changes_with_relative_mutations_require_explicit_paths(command):
    assert any(fact.attributes.get('target_resolution') == 'shell_parse_required' for fact in facts(command))


def test_directory_changes_preserve_read_only_and_absolute_mutation_commands():
    assert targets('cd /tmp/other && ls') == set()
    assert targets('cd /tmp/other && rm -- /tmp/absolute') == {('delete', '/tmp/absolute')}


def test_find_directory_relative_mutations_need_absolute_paths_but_keep_native_placeholders():
    for operator in ('-execdir', '-okdir'):
        rejected = facts(f'find /tmp/root {operator} rm -- relative \\;')
        assert rejected[0].attributes['target_resolution'] == 'shell_parse_required'
        assert targets(f'find /tmp/root {operator} rm -- /tmp/absolute \\;') == {('delete', '/tmp/absolute')}
        assert targets(f"find /tmp/root {operator} rm -- '{{}}' \\;") == set()


def test_command_builtin_inspection_and_execution_modifiers_keep_their_native_meanings():
    for option in ('-v', '-V', '-pv', '-pV', '-v -p'):
        assert targets(f'command {option} rm /tmp/victim 2>/dev/null') == set()
    assert targets('command -v python3 2> /dev/null && python3 --version') == set()
    assert targets('command -V rm > /tmp/listing') == {('write', '/tmp/listing')}
    for option in ('-p', '--', '-p --'):
        assert targets(f'command {option} rm -- /tmp/victim') == {('delete', '/tmp/victim')}
    for option in ('-x', '-vx', '--unknown'):
        assert facts(f'command {option} rm /tmp/victim')[0].attributes['target_resolution'] == 'shell_parse_required'
