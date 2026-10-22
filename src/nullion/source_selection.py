"""Render unresolved account-source decisions from typed scope results."""
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Iterable

from nullion.tools import normalize_tool_status

_ACCOUNT_READ_TOOLS = frozenset({'email_search', 'email_read', 'email_attachment_read', 'calendar_list', 'connector_request'})


@dataclass(frozen=True, slots=True)
class SourceSelection:
    options: tuple[str, ...]

    @property
    def reply(self) -> str:
        rows = '\n'.join(f'{index}. {label}' for index, label in enumerate(self.options, 1))
        return 'I can check, but please choose which authorized source you want me to read first.\n\n' + rows


def _names(value: object) -> set[str]:
    return {item for item in value if isinstance(item, str)} if isinstance(value, (list, tuple, set, frozenset)) else set()


def pending_source_selection(results: Iterable[object] | None) -> SourceSelection | None:
    pending = None
    for result in results or ():
        get = result.get if isinstance(result, Mapping) else lambda key, default=None: getattr(result, key, default)
        if get('tool_name') != 'request_tool_scope' or normalize_tool_status(get('status')) != 'completed':
            continue
        output = get('output')
        if not isinstance(output, Mapping):
            continue
        if output.get('source_selection_required') is True:
            pending = output
        elif pending is not None and (
            _names(output.get('available_tools')).intersection(_ACCOUNT_READ_TOOLS)
            or (_names(output.get('capabilities')).difference({'connector', 'conversation_history'})
                and _names(output.get('available_tools'))
                and not _names(output.get('unavailable_required_tool_names')))
        ):
            pending = None
    if pending is None:
        return None
    requested = _names(pending.get('explicit_tool_names')) | _names(pending.get('requested_required_tool_names'))
    requested &= _ACCOUNT_READ_TOOLS
    options = []
    for source in pending.get('available_sources') or ():
        if not isinstance(source, Mapping):
            continue
        names = _names(source.get('read_tools'))
        if requested:
            names &= requested
        labels = []
        if names.intersection({'email_search', 'email_read', 'email_attachment_read'}):
            labels.append('Email records')
        if 'calendar_list' in names:
            labels.append('Calendar records')
        if 'connector_request' in names:
            labels.append('Another connected account/source, if available')
        display = str(source.get('display_name') or '').strip()
        options.extend(f'{display}: {label}' if display and label != 'Another connected account/source, if available' else label for label in labels)
    options = list(dict.fromkeys(options))
    if not options:
        options.append('Choose a connected account/source')
    if len(options) == 1:
        options.append('Provide another source or details instead')
    return SourceSelection(tuple(options))
