"""Boundary extraction helpers for Nullion tool invocations."""

from __future__ import annotations

from dataclasses import dataclass
from ipaddress import ip_address
from pathlib import Path
import re
import shlex
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse

from nullion.policy import BoundaryFact, BoundaryKind

if TYPE_CHECKING:
    from nullion.tools import ToolInvocation

_NETWORK_COMMAND_FAMILIES = {
    "curl",
    "curl.exe",
    "wget",
    "wget.exe",
    "invoke-webrequest",
    "invoke-restmethod",
    "iwr",
    "irm",
    "start-bitstransfer",
}
_SHELL_COMMAND_FAMILIES = {
    "powershell",
    "powershell.exe",
    "pwsh",
    "pwsh.exe",
    "cmd",
    "cmd.exe",
    "sh",
    "bash",
    "zsh",
}
_SHELL_CONTROL_TOKENS = {";", "&&", "||", "|", "|&", "&", "(", ")"}
_SHELL_WRAPPER_COMMANDS = {
    "command",
    "env",
    "sudo",
    "doas",
    "xargs",
}
_TERMINAL_FILESYSTEM_MUTATION_COMMANDS = {
    "cp": "write",
    "cp.exe": "write",
    "install": "write",
    "ln": "write",
    "ln.exe": "write",
    "mkdir": "write",
    "mkdir.exe": "write",
    "mv": "write",
    "mv.exe": "write",
    "rm": "delete",
    "rm.exe": "delete",
    "rmdir": "delete",
    "rmdir.exe": "delete",
    "touch": "write",
    "touch.exe": "write",
}
_TERMINAL_FILESYSTEM_METADATA_MUTATION_COMMANDS = {
    "chflags",
    "chgrp",
    "chmod",
    "chown",
}
_TERMINAL_SAFE_REDIRECT_TARGETS = {"/dev/null"}
_SHELL_OPERATORS = (';;&', '&>>', '<<<', '<<-', '&&', '||', '|&', ';;', ';&',
    '>>', '<<', '>&', '<&', '<>', '>|', '&>', ';', '&', '|', '(', ')', '<', '>')
_SHELL_REDIRECTS = frozenset({'<', '>', '>>', '>&', '<&', '<>', '>|', '&>', '&>>', '<<', '<<-', '<<<'})


class _ShellParseError(ValueError):
    """Syntax that cannot safely be represented without running the shell."""


@dataclass(frozen=True, slots=True)
class _ShellToken:
    value: str
    raw: str
    start: int
    end: int
    kind: str = 'word'
    quoted: bool = False
    dynamic: bool = False
    substitutions: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _ShellRedirect:
    operator: str
    target: _ShellToken
    descriptor: str | None = None
_HTTP_URL_RE = re.compile(r"https?://[^\s'\"<>()\[\]{}]+", re.I)
_WEB_SEARCH_TARGET = "https://www.bing.com/*"
_INTERNAL_LARGE_TOOL_RESULT_RE = re.compile(r"^/large_tool_results/call_[A-Za-z0-9_-]+$")

# Account-scoped tools: maps tool_name → (operation, account_type)
_ACCOUNT_SCOPED_TOOLS: dict[str, tuple[str, str]] = {
    "email_send":      ("send",  "email"),
    "email_search":    ("read",  "email"),
    "email_read":      ("read",  "email"),
    "email_attachment_read": ("read", "email"),
    "calendar_create": ("write", "calendar"),
    "calendar_update": ("write", "calendar"),
    "calendar_respond": ("write", "calendar"),
    "calendar_delete": ("write", "calendar"),
    "calendar_write":  ("write", "calendar"),
    "calendar_read":   ("read",  "calendar"),
    "contacts_read":   ("read",  "contacts"),
    "contacts_write":  ("write", "contacts"),
}

# Any tool whose name starts with this prefix is treated as account-scoped
_PLUGIN_TOOL_PREFIX = "plugin:"


def _extract_http_url(command: str) -> str | None:
    for match in _HTTP_URL_RE.finditer(command):
        candidate = match.group(0).rstrip(".,;")
        parsed = urlparse(candidate)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return candidate
    return None


def _command_tokens(command: str) -> list[str]:
    try:
        return shlex.split(command, posix=False)
    except ValueError:
        return command.split()


def _shell_statement_separators(command: str) -> str:
    # Keep quoted newlines as filename bytes and escaped newlines as shell
    # continuations. Only unquoted statement endings become separators.
    result: list[str] = []
    quote = None
    escaped = False
    comment = False
    boundary = True
    for char in command:
        if comment:
            if char == "\n":
                result.append(";")
                comment = False
                boundary = True
            continue
        if escaped:
            if char == "\n":
                result.pop()  # the preceding continuation backslash
            else:
                result.append(char)
            escaped = False
            continue
        if quote is not None:
            result.append(char)
            if char == quote:
                quote = None
            elif char == "\\" and quote == '"':
                escaped = True
            continue
        if char == "#" and boundary:
            comment = True
        elif char == "\n":
            result.append(";")
            boundary = True
        else:
            result.append(char)
            if char in {"'", '"'}:
                quote = char
            elif char == "\\":
                escaped = True
            boundary = char.isspace() or char in ";|&()<>"
    return "".join(result)


def _shell_group_end(command: str, start: int, opening: str, closing: str) -> int:
    depth, index, quote = 1, start + 1, None
    while index < len(command):
        char = command[index]
        if char == '\\' and quote != "'":
            index += 2
            continue
        if quote:
            if char == quote:
                quote = None
            elif quote == '"' and command.startswith('$(', index):
                index = _shell_group_end(command, index + 1, '(', ')')
                continue
        elif char in {"'", '"'}:
            quote = char
        elif char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if not depth:
                return index + 1
        index += 1
    raise _ShellParseError('Unterminated shell expansion or group')


def _shell_lexemes(command: str) -> list[_ShellToken]:
    """Lex shell words/operators without losing quoting or IO-number adjacency.

    This does not evaluate expansions. Unsupported grammar is rejected by the
    command reader instead of being reinterpreted as executable filenames.
    """
    if '\0' in command:
        raise _ShellParseError('A shell command cannot contain NUL bytes')
    tokens = []
    index = 0
    group_depth = 0
    heredocs = []
    heredoc_operator = None
    while index < len(command):
        char = command[index]
        if char == '\\' and command[index:index + 2] == '\\\n':
            index += 2
            continue
        if char in ' \t':
            index += 1
            continue
        if char == '#':
            newline = command.find('\n', index)
            index = len(command) if newline < 0 else newline
            continue
        if char == '\n':
            if not tokens or tokens[-1].value not in {'&&', '||', '|', '|&'} or tokens[-1].kind != 'operator':
                tokens.append(_ShellToken(';', '\n', index, index + 1, 'operator'))
            index += 1
            for delimiter, strip_tabs, quoted in heredocs:
                found = False
                while index < len(command):
                    end = command.find('\n', index)
                    end = len(command) if end < 0 else end
                    line = command[index:end]
                    compared = line.lstrip('\t') if strip_tabs else line
                    index = min(end + 1, len(command))
                    if compared == delimiter:
                        found = True
                        break
                    if not quoted and ('$' + '(' in line or '`' in line):
                        raise _ShellParseError('Substitution in an expandable here-document requires explicit commands')
                if not found:
                    raise _ShellParseError('Unterminated here-document')
            heredocs.clear()
            continue
        operator = next((op for op in _SHELL_OPERATORS if command.startswith(op, index)), None)
        # Process substitutions are words, not ordinary redirection operands.
        if operator and not command.startswith(('<(', '>('), index):
            if operator == '(':
                group_depth += 1
            elif operator == ')':
                group_depth -= 1
                if group_depth < 0:
                    raise _ShellParseError('Unmatched shell group')
            tokens.append(_ShellToken(operator, operator, index, index + len(operator), 'operator'))
            if operator in {'<<', '<<-'}:
                heredoc_operator = operator
            index += len(operator)
            continue
        start = index
        value = []
        quoted = dynamic = False
        substitutions = []
        quote = None
        while index < len(command):
            char = command[index]
            if quote == "'":
                if char == "'":
                    quote = None
                else:
                    value.append(char)
                index += 1
                continue
            if char == '\\':
                if index + 1 == len(command):
                    raise _ShellParseError('Trailing shell escape')
                following = command[index + 1]
                if following == '\n':
                    index += 2
                    continue
                if quote == '"' and following not in '$`"\\':
                    value.append('\\')
                    index += 1
                    continue
                quoted = True
                value.append(following)
                index += 2
                continue
            if char == '"':
                quoted = True
                quote = None if quote == '"' else '"'
                index += 1
                continue
            if not quote and char == "'":
                quoted = True
                quote = "'"
                index += 1
                continue
            if not quote and command.startswith(("$'", '$"'), index):
                raise _ShellParseError('Extended shell quoting requires explicit literal arguments')
            if command.startswith('$(', index) or (not quote and command.startswith(('<(', '>('), index)):
                end = _shell_group_end(command, index + 1, '(', ')')
                expression = command[index:end]
                value.append(expression)
                dynamic = True
                if not expression.startswith('$(('):
                    substitutions.append(expression[2:-1])
                elif '$(' in expression[3:-2] or '`' in expression[3:-2]:
                    raise _ShellParseError('Nested arithmetic substitutions require explicit commands')
                index = end
                continue
            if command.startswith('${', index):
                end = _shell_group_end(command, index + 1, '{', '}')
                expression = command[index:end]
                if '$(' in expression or '`' in expression:
                    raise _ShellParseError('Nested parameter substitutions require explicit commands')
                value.append(expression)
                dynamic = True
                index = end
                continue
            if char == '`':
                end = command.find('`', index + 1)
                if end < 0 or '\\' in command[index + 1:end]:
                    raise _ShellParseError('Unsupported or unterminated backtick substitution')
                substitutions.append(command[index + 1:end])
                value.append(command[index:end + 1])
                dynamic = True
                index = end + 1
                continue
            if not quote and (char in ' \t\n' or any(command.startswith(op, index) for op in _SHELL_OPERATORS)):
                break
            if char == '$' and index + 1 < len(command) and (
                    command[index + 1].isalnum() or command[index + 1] in '_?*!$@-#'):
                dynamic = True
            if char == '~' and index == start:
                dynamic = True
            value.append(char)
            index += 1
        if quote:
            raise _ShellParseError('Unterminated shell quote')
        token = _ShellToken(''.join(value), command[start:index], start, index,
            quoted=quoted, dynamic=dynamic, substitutions=tuple(substitutions))
        tokens.append(token)
        if heredoc_operator:
            heredocs.append((token.value, heredoc_operator == '<<-', token.quoted))
            heredoc_operator = None
    if group_depth:
        raise _ShellParseError('Unterminated shell group')
    if tokens and tokens[-1].kind == 'operator' and tokens[-1].value in {'&&', '||', '|', '|&'}:
        raise _ShellParseError('Incomplete shell control operator')
    if heredoc_operator or heredocs:
        raise _ShellParseError('Missing here-document body or delimiter')
    return tokens


def _shell_syntax_tokens(command: str) -> list[str]:
    return [token.value for token in _shell_lexemes(command)]


def _shell_segments(tokens):
    segments: list[list[str]] = []
    current: list[str] = []
    for token in tokens:
        value = token.value if isinstance(token, _ShellToken) else token
        operator = not isinstance(token, _ShellToken) or token.kind == 'operator'
        if operator and value in _SHELL_CONTROL_TOKENS:
            if current:
                segments.append(current)
                current = []
            continue
        current.append(token)
    if current:
        segments.append(current)
    return segments


def _shell_command_words(segment: list[_ShellToken]) -> tuple[list[_ShellToken], list[_ShellRedirect]]:
    words, redirects = [], []
    index = 0
    while index < len(segment):
        token = segment[index]
        if token.kind != 'operator':
            words.append(token)
            index += 1
            continue
        if token.value not in _SHELL_REDIRECTS or index + 1 >= len(segment):
            raise _ShellParseError('Unsupported shell operator or missing redirection target')
        target = segment[index + 1]
        if target.kind != 'word':
            raise _ShellParseError('A redirection requires one shell word')
        descriptor = None
        if words and words[-1].end == token.start and not words[-1].quoted:
            prefix = words[-1].raw.replace('\\\n', '')
            if prefix.isascii() and prefix.isdigit():
                descriptor = words.pop().value
            elif re.fullmatch(r'\{[A-Za-z_][A-Za-z_0-9]*\}', prefix):
                descriptor = words.pop().value
        redirects.append(_ShellRedirect(token.value, target, descriptor))
        index += 2
    return words, redirects


def _strip_shell_wrappers(segment):
    stripped = list(segment)
    while stripped:
        value = stripped[0].value if isinstance(stripped[0], _ShellToken) else stripped[0].strip("'\"")
        command = Path(value).name.lower()
        if command not in _SHELL_WRAPPER_COMMANDS:
            break
        if command == 'xargs' and isinstance(stripped[0], _ShellToken):
            raise _ShellParseError('Input-generated arguments require an explicit target selection')
        stripped = stripped[1:]
        if command == 'command':
            inspect_only = False
            while stripped:
                option = stripped[0].value if isinstance(stripped[0], _ShellToken) else stripped[0]
                if option == '--':
                    stripped = stripped[1:]
                    break
                if not option.startswith('-') or option == '-':
                    break
                if option.startswith('--') or any(flag not in 'pVv' for flag in option[1:]):
                    raise _ShellParseError('Unsupported command builtin option')
                inspect_only = inspect_only or any(flag in 'Vv' for flag in option[1:])
                stripped = stripped[1:]
            if inspect_only:
                return []  # Names are inspected; none of them is executed.
        if command == "env":
            while stripped:
                value = stripped[0].value if isinstance(stripped[0], _ShellToken) else stripped[0]
                if '=' not in value or value.startswith(('-', '/')):
                    break
                stripped = stripped[1:]
    return stripped


def _shell_operands(tokens):
    operands: list[str] = []
    skip_next = False
    options = True
    for token in tokens:
        if skip_next:
            skip_next = False
            continue
        value = token.value if isinstance(token, _ShellToken) else token
        if options and value == "--":
            options = False
            continue
        if options and value.startswith("-"):
            if value in {"-o", "-g", "-m", "-t", "-T"}:
                skip_next = True
            continue
        operands.append(token)
    return operands


def _filesystem_boundary_fact_from_terminal_target(
    *,
    tool_name: str,
    operation: str,
    raw_path: object,
    command_family: str,
) -> BoundaryFact | None:
    token = raw_path if isinstance(raw_path, _ShellToken) else None
    value = token.value if token else raw_path
    if token:
        if not value:
            return None
        # Shell quoting was already removed exactly by the lexer. Do not apply
        # another quote/tilde interpretation to a literal filename.
        fact = BoundaryFact(kind=BoundaryKind.FILESYSTEM_ACCESS, tool_name=tool_name,
            operation=operation, target=value, attributes={'path': value,
                **({'shell_quoted_literal': True, 'shell_expression': token.raw} if token.quoted else {}),
                **({'target_resolution': 'shell_expansion_required', 'shell_expression': token.raw}
                   if token.dynamic else {})})
    else:
        fact = _filesystem_boundary_fact(tool_name=tool_name, operation=operation, raw_path=value)
    if fact is None:
        return None
    return BoundaryFact(
        kind=fact.kind,
        tool_name=fact.tool_name,
        operation=fact.operation,
        target=fact.target,
        attributes={**fact.attributes, "command_family": command_family},
    )


def _is_safe_terminal_redirect_target(raw_path: object) -> bool:
    if isinstance(raw_path, _ShellToken):
        return not raw_path.dynamic and raw_path.value in _TERMINAL_SAFE_REDIRECT_TARGETS
    if not isinstance(raw_path, str) or not raw_path:
        return False
    target = raw_path.strip().strip("'\"")
    return target in _TERMINAL_SAFE_REDIRECT_TARGETS


def _shell_parse_failure_fact(command: str, *, tool_name: str) -> BoundaryFact:
    return BoundaryFact(kind=BoundaryKind.FILESYSTEM_ACCESS, tool_name=tool_name,
        operation='parse', target='<unresolved-shell-command>', attributes={
            'target_resolution': 'shell_parse_required', 'shell_expression': command,
            'failure_reason': 'shell_syntax_unresolved'})


def _terminal_filesystem_mutation_facts(command: str, *, tool_name: str, _depth=0) -> list[BoundaryFact]:
    try:
        return _parsed_terminal_filesystem_facts(command, tool_name=tool_name, _depth=_depth)
    except _ShellParseError:
        return [_shell_parse_failure_fact(command, tool_name=tool_name)]


def _parsed_terminal_filesystem_facts(command: str, *, tool_name: str, _depth: int) -> list[BoundaryFact]:
    if _depth > 12:
        raise _ShellParseError('Shell nesting exceeds the supported inspection depth')
    tokens = _shell_lexemes(command)
    facts: list[BoundaryFact] = []
    changes_directory = False
    for token in tokens:
        for nested in token.substitutions:
            facts.extend(_terminal_filesystem_mutation_facts(nested, tool_name=tool_name, _depth=_depth + 1))
    for segment in _shell_segments(tokens):
        words, redirects = _shell_command_words(segment)
        for redirect in redirects:
            operator, target = redirect.operator, redirect.target
            if operator in {'<<', '<<-', '<<<'}:
                continue  # Delimiters and here-string data are not file paths.
            if operator in {'>&', '<&'} and not target.dynamic:
                if re.fullmatch(r'(?:[0-9]+-?|-)', target.value):
                    continue  # Duplicate, move or close a file descriptor.
                if operator == '<&' or redirect.descriptor is not None:
                    raise _ShellParseError('Invalid file descriptor duplication')
            operation = 'read' if operator in {'<', '<&'} else 'write'
            if _is_safe_terminal_redirect_target(target):
                continue
            fact = _filesystem_boundary_fact_from_terminal_target(tool_name=tool_name,
                operation=operation, raw_path=target, command_family='redirect')
            if fact is not None:
                facts.append(fact)
        while words and not words[0].quoted and re.match(r'^[A-Za-z_][A-Za-z_0-9]*=', words[0].raw):
            words.pop(0)
        while words and not words[0].quoted and words[0].value in {'then', 'do', 'else', 'elif', '!', '{', '}'}:
            words.pop(0)
        stripped = _strip_shell_wrappers(words)
        if not stripped:
            continue
        if stripped[0].dynamic or stripped[0].value.startswith('-'):
            raise _ShellParseError('The executable must be an explicit shell word')
        command_family = Path(stripped[0].value).name.lower()
        if command_family in {'cd', 'pushd', 'popd'}:
            changes_directory = True
            continue
        if command_family in {'if', 'for', 'while', 'until', 'case', 'function', 'select', 'eval',
                'source', '.', '[[', 'time', 'exec', 'builtin'}:
            raise _ShellParseError('This shell construct requires explicit commands')
        if command_family in {'sh', 'bash', 'zsh', 'dash', 'ksh'}:
            command_index = next((index + 1 for index, token in enumerate(stripped[1:], 1)
                if token.value.startswith('-') and not token.value.startswith('--') and 'c' in token.value), None)
            if command_index is None or command_index >= len(stripped) or stripped[command_index].dynamic:
                raise _ShellParseError('Nested shells require an inspectable literal command')
            facts.extend(_terminal_filesystem_mutation_facts(stripped[command_index].value,
                tool_name=tool_name, _depth=_depth + 1))
            continue
        if command_family == 'find':
            for index, word in enumerate(stripped[1:], 1):
                if word.value not in {'-exec', '-execdir', '-ok', '-okdir'}:
                    continue
                end = next((offset for offset in range(index + 1, len(stripped))
                    if stripped[offset].value in {';', '+'}), None)
                if end is None or end == index + 1:
                    raise _ShellParseError('An executable find expression requires a complete command')
                nested_words = _strip_shell_wrappers(stripped[index + 1:end])
                if not nested_words:
                    raise _ShellParseError('An executable find expression requires a command')
                nested_family = Path(nested_words[0].value).name.lower()
                if any('{}' in item.value and item.value != '{}' for item in nested_words):
                    raise _ShellParseError('Embedded find substitutions require an explicit target selection')
                nested_command = command[stripped[index + 1].start:stripped[end - 1].end]
                nested_facts = _terminal_filesystem_mutation_facts(nested_command,
                    tool_name=tool_name, _depth=_depth + 1)
                # A standalone find placeholder is the stored search selection,
                # not a literal cwd/{} file. The shared find permission adapter
                # retains that native selection scope separately.
                nested_facts = [fact for fact in nested_facts if not (
                    fact.target == '{}' and nested_family in {'rm', 'rm.exe', 'rmdir', 'rmdir.exe'})]
                if word.value in {'-execdir', '-okdir'} and any(
                        fact.operation in {'delete', 'write', 'metadata_write'}
                        and not Path(fact.target).is_absolute() for fact in nested_facts):
                    raise _ShellParseError('Directory-relative find commands require absolute mutation targets')
                facts.extend(nested_facts)
        operands = _shell_operands(stripped[1:])
        if command_family in _TERMINAL_FILESYSTEM_MUTATION_COMMANDS:
            operation = _TERMINAL_FILESYSTEM_MUTATION_COMMANDS[command_family]
            targets = operands if operation == "delete" else operands[-1:]
            for target in targets:
                fact = _filesystem_boundary_fact_from_terminal_target(
                    tool_name=tool_name,
                    operation=operation,
                    raw_path=target,
                    command_family=command_family,
                )
                if fact is not None:
                    facts.append(fact)
        elif command_family in _TERMINAL_FILESYSTEM_METADATA_MUTATION_COMMANDS:
            for target in operands[1:] or operands[-1:]:
                fact = _filesystem_boundary_fact_from_terminal_target(
                    tool_name=tool_name,
                    operation="metadata_write",
                    raw_path=target,
                    command_family=command_family,
                )
                if fact is not None:
                    facts.append(fact)
    if changes_directory and any(fact.operation in {'delete', 'write', 'metadata_write'}
            and not Path(fact.target).is_absolute() for fact in facts):
        raise _ShellParseError('Directory-changing commands require absolute mutation targets')
    return facts


def _terminal_network_command_family(command: str) -> str | None:
    # This inspects structured command syntax, not the user's free-form prompt.
    for token in _command_tokens(command):
        normalized = Path(token.strip("'\"")).name.lower()
        if normalized in _NETWORK_COMMAND_FAMILIES:
            return normalized
        if normalized in _SHELL_COMMAND_FAMILIES:
            continue
    return None


def _address_class_for_host(host: str) -> str:
    lowered = host.strip().lower()
    if not lowered:
        return "unknown"
    if lowered == "localhost":
        return "localhost"
    try:
        parsed = ip_address(lowered)
    except ValueError:
        return "public"
    if parsed.is_loopback:
        return "loopback"
    if parsed.is_link_local:
        return "link_local"
    if parsed.is_unspecified:
        return "loopback"
    if parsed.is_reserved:
        return "reserved"
    if parsed.is_private:
        return "private"
    return "public"


def _network_boundary_fact(*, tool_name: str, operation: str, target: str, command_family: str | None = None) -> BoundaryFact:
    parsed = urlparse(target)
    host = parsed.hostname or ""
    attributes = {
        "scheme": parsed.scheme,
        "host": host,
        "address_class": _address_class_for_host(host),
    }
    if command_family is not None:
        attributes["command_family"] = command_family
    return BoundaryFact(
        kind=BoundaryKind.OUTBOUND_NETWORK,
        tool_name=tool_name,
        operation=operation,
        target=target,
        attributes=attributes,
    )


def _account_boundary_fact(
    *,
    tool_name: str,
    operation: str,
    account_type: str,
    target: str = "",
) -> BoundaryFact:
    """Build a BoundaryFact for an account-scoped tool invocation."""
    resolved_target = target or f"{account_type}:{operation}"
    return BoundaryFact(
        kind=BoundaryKind.ACCOUNT_ACCESS,
        tool_name=tool_name,
        operation=operation,
        target=resolved_target,
        attributes={"account_type": account_type},
    )


def _email_send_account_target(invocation: "ToolInvocation") -> str:
    provider_id = str(invocation.arguments.get("provider_id") or "").strip()
    if not provider_id:
        try:
            from nullion.connections import connection_for_principal, infer_email_plugin_provider

            native_provider_id = infer_email_plugin_provider(principal_id=invocation.principal_id) or ""
            native_connection = (
                connection_for_principal(invocation.principal_id, native_provider_id) if native_provider_id else None
            )
            permission_mode = (
                str(getattr(native_connection, "permission_mode", "read") or "read").strip().lower().replace("-", "_")
            )
            if permission_mode in {"write", "read_write", "readwrite", "rw", "read_and_write"}:
                provider_id = native_provider_id
        except Exception:
            provider_id = ""
    if not provider_id:
        try:
            from nullion.connections import default_email_connector_provider_id

            provider_id = default_email_connector_provider_id(invocation.principal_id) or ""
        except Exception:
            provider_id = ""
    return f"{provider_id}:send" if provider_id else "email:send"


def _email_read_account_target(invocation: "ToolInvocation") -> str:
    provider_id = str(invocation.arguments.get("provider_id") or "").strip()
    if not provider_id:
        try:
            from nullion.connections import infer_email_plugin_provider

            provider_id = infer_email_plugin_provider(principal_id=invocation.principal_id) or ""
        except Exception:
            provider_id = ""
    if not provider_id:
        try:
            from nullion.connections import default_email_connector_provider_id

            provider_id = default_email_connector_provider_id(invocation.principal_id) or ""
        except Exception:
            provider_id = ""
    return provider_id or "email:read"


def _calendar_account_target(invocation: "ToolInvocation", operation: str) -> str:
    provider_id = str(invocation.arguments.get("provider_id") or "").strip()
    calendar_id = str(invocation.arguments.get("calendar_id") or "primary").strip() or "primary"
    event_id = str(invocation.arguments.get("event_id") or "").strip()
    prefix = provider_id or "calendar"
    target = f"{prefix}:{calendar_id}:{operation}"
    return f"{target}:{event_id}" if event_id else target


def _filesystem_boundary_fact(*, tool_name: str, operation: str, raw_path: object) -> BoundaryFact | None:
    if not isinstance(raw_path, str) or not raw_path:
        return None
    raw_text = raw_path.strip()
    if operation == "read" and _INTERNAL_LARGE_TOOL_RESULT_RE.fullmatch(raw_text):
        return None
    path = str(Path(raw_path).expanduser())
    return BoundaryFact(
        kind=BoundaryKind.FILESYSTEM_ACCESS,
        tool_name=tool_name,
        operation=operation,
        target=path,
        attributes={"path": path},
    )


def extract_boundary_facts(invocation: ToolInvocation) -> list[BoundaryFact]:
    if invocation.tool_name == "terminal_exec":
        command = invocation.arguments.get("command")
        if not isinstance(command, str):
            return []
        stripped = command.strip()
        if not stripped:
            return []
        facts: list[BoundaryFact] = []
        command_family = _terminal_network_command_family(stripped)
        if command_family is not None:
            target = _extract_http_url(stripped)
            if target is not None:
                facts.append(
                    _network_boundary_fact(
                        tool_name=invocation.tool_name,
                        operation="http_get",
                        target=target,
                        command_family=command_family,
                    )
                )
        facts.extend(
            _terminal_filesystem_mutation_facts(
                stripped,
                tool_name=invocation.tool_name,
            )
        )
        return facts

    if invocation.tool_name in {"file_download", "web_fetch", "browser_screenshot"}:
        raw_url = invocation.arguments.get("pdf_url" if invocation.tool_name == "browser_screenshot" else "url")
        if not isinstance(raw_url, str) or not raw_url:
            return []
        return [
            _network_boundary_fact(
                tool_name=invocation.tool_name,
                operation="http_get",
                target=raw_url,
            )
        ]

    if invocation.tool_name == "browser_image_collect":
        facts = []
        raw_page_url = invocation.arguments.get("page_url")
        page_url = raw_page_url.strip() if isinstance(raw_page_url, str) else ""
        parsed_page = urlparse(page_url)
        if parsed_page.scheme in {"http", "https"} and parsed_page.netloc:
            facts.append(
                _network_boundary_fact(
                    tool_name=invocation.tool_name,
                    operation="http_get",
                    target=page_url,
                )
            )
        raw_image_urls = invocation.arguments.get("image_urls")
        if isinstance(raw_image_urls, str):
            raw_image_urls = [raw_image_urls]
        if isinstance(raw_image_urls, (list, tuple)):
            seen_targets = {fact.target for fact in facts}
            for raw_image_url in raw_image_urls:
                if not isinstance(raw_image_url, str) or not raw_image_url.strip():
                    continue
                image_url = urljoin(page_url, raw_image_url.strip()) if page_url else raw_image_url.strip()
                parsed_image = urlparse(image_url)
                if parsed_image.scheme not in {"http", "https"} or not parsed_image.netloc:
                    continue
                if image_url in seen_targets:
                    continue
                seen_targets.add(image_url)
                facts.append(
                    _network_boundary_fact(
                        tool_name=invocation.tool_name,
                        operation="http_get",
                        target=image_url,
                    )
                )
        return facts

    if invocation.tool_name == "connector_request":
        facts = []
        method = str(invocation.arguments.get("method") or "GET").strip().upper() or "GET"
        account_operation = "read" if method in {"GET", "HEAD"} else "write"
        raw_url = invocation.arguments.get("url")
        provider_id = str(invocation.arguments.get("provider_id") or "").strip()
        if not provider_id or not isinstance(raw_url, str) or not raw_url.strip():
            return []
        if isinstance(raw_url, str) and raw_url:
            facts.append(
                _network_boundary_fact(
                    tool_name=invocation.tool_name,
                    operation=f"http_{method.lower()}",
                    target=raw_url,
                )
            )
        facts.append(
            _account_boundary_fact(
                tool_name=invocation.tool_name,
                operation=account_operation,
                account_type=provider_id,
                target=provider_id if account_operation == "read" else f"{provider_id}:{method.lower()}",
            )
        )
        return facts

    if invocation.tool_name == "web_search":
        return [
            _network_boundary_fact(
                tool_name=invocation.tool_name,
                operation="search",
                target=_WEB_SEARCH_TARGET,
            )
        ]

    if invocation.tool_name == "file_read":
        fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="read",
            raw_path=invocation.arguments.get("path"),
        )
        return [] if fact is None else [fact]

    if invocation.tool_name == "file_write":
        fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="write",
            raw_path=invocation.arguments.get("path"),
        )
        return [] if fact is None else [fact]

    if invocation.tool_name == "file_download":
        fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="write",
            raw_path=invocation.arguments.get("output_path"),
        )
        return [] if fact is None else [fact]

    if invocation.tool_name == "archive_extract":
        facts = []
        read_fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="read",
            raw_path=invocation.arguments.get("path"),
        )
        if read_fact is not None:
            facts.append(read_fact)
        write_fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="write",
            raw_path=invocation.arguments.get("output_dir"),
        )
        if write_fact is not None:
            facts.append(write_fact)
        return facts

    if invocation.tool_name == "archive_create":
        facts = []
        output_fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="write",
            raw_path=invocation.arguments.get("output_path"),
        )
        if output_fact is not None:
            facts.append(output_fact)
        source_dir_fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="read",
            raw_path=invocation.arguments.get("source_dir"),
        )
        if source_dir_fact is not None:
            facts.append(source_dir_fact)
        raw_paths = invocation.arguments.get("source_paths")
        if isinstance(raw_paths, str):
            raw_paths = [raw_paths]
        if isinstance(raw_paths, (list, tuple)):
            for raw_path in raw_paths:
                source_fact = _filesystem_boundary_fact(
                    tool_name=invocation.tool_name,
                    operation="read",
                    raw_path=raw_path,
                )
                if source_fact is not None:
                    facts.append(source_fact)
        return facts

    if invocation.tool_name in {"audio_transcribe", "image_extract_text"}:
        fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="read",
            raw_path=invocation.arguments.get("path"),
        )
        return [] if fact is None else [fact]

    if invocation.tool_name == "image_generate":
        facts = []
        source_fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="read",
            raw_path=invocation.arguments.get("source_path"),
        )
        if source_fact is not None:
            facts.append(source_fact)
        output_fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="write",
            raw_path=invocation.arguments.get("output_path"),
        )
        if output_fact is not None:
            facts.append(output_fact)
        return facts

    if invocation.tool_name == "email_send":
        facts = [
            _account_boundary_fact(
                tool_name=invocation.tool_name,
                operation="send",
                account_type="email",
                target=_email_send_account_target(invocation),
            )
        ]
        raw_paths = invocation.arguments.get("attachment_paths")
        if isinstance(raw_paths, str):
            raw_paths = [raw_paths]
        html_path = invocation.arguments.get("html_path")
        if isinstance(html_path, str) and html_path.strip() and not str(invocation.arguments.get("html_body") or "").strip():
            raw_paths = [*(raw_paths if isinstance(raw_paths, (list, tuple)) else ()), html_path]
        if isinstance(raw_paths, (list, tuple)):
            for raw_path in raw_paths:
                fact = _filesystem_boundary_fact(
                    tool_name=invocation.tool_name,
                    operation="read",
                    raw_path=raw_path,
                )
                if fact is not None:
                    facts.append(fact)
        return facts

    # Named account-scoped tools (email, calendar, contacts)
    if invocation.tool_name in _ACCOUNT_SCOPED_TOOLS:
        operation, account_type = _ACCOUNT_SCOPED_TOOLS[invocation.tool_name]
        if invocation.tool_name in {"email_search", "email_read", "email_attachment_read"}:
            target = _email_read_account_target(invocation)
        elif invocation.tool_name.startswith("calendar_"):
            target = _calendar_account_target(invocation, operation)
        else:
            target = ""
        return [
            _account_boundary_fact(
                tool_name=invocation.tool_name,
                operation=operation,
                account_type=account_type,
                target=target,
            )
        ]

    # Generic plugin tools are account-scoped by convention
    if invocation.tool_name.startswith(_PLUGIN_TOOL_PREFIX):
        plugin_name = invocation.tool_name[len(_PLUGIN_TOOL_PREFIX):]
        return [
            _account_boundary_fact(
                tool_name=invocation.tool_name,
                operation="invoke",
                account_type=plugin_name,
            )
        ]

    return []
