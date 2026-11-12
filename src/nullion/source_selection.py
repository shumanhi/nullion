"""Render unresolved account-source decisions from typed scope results."""
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Iterable

from nullion.tools import normalize_tool_status

_ACCOUNT_READ_TOOLS = frozenset({'email_search', 'email_read', 'email_attachment_read', 'calendar_list', 'connector_request'})

NAMED_SOURCE_READ_GUIDANCE = (
    "A current request authorizing an account read selects its source by function; the user need not "
    "name an adapter, vendor or address. Use matching registered read-route metadata to request the exact "
    "scope with source_user_requested=true and source_evidence quoting that request, then perform the read. "
    "An unresolved adapter is metadata work, not a reason to ask for renewed consent. Do not offer a choice "
    "between the requested source and a hypothetical different account. Ask only when the source type is "
    "unclear, genuinely competing connected accounts cannot be resolved from the user's selection or configured "
    "default, or the user explicitly requires a choice before reading. A supported-service catalogue is not "
    "a list of connected accounts. Metadata discovery grants no account read; runtime permission and approval "
    "policy still applies. An independent request does not inherit consent from an unrelated prior topic."
)


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


def pending_source_resolution(results: Iterable[object] | None) -> Mapping[str, object] | None:
    """The latest runtime scope may require tool resolution, not a user choice."""
    latest = None
    for result in results or ():
        get = result.get if isinstance(result, Mapping) else lambda key, default=None: getattr(result, key, default)
        if get('tool_name') == 'request_tool_scope' and normalize_tool_status(get('status')) == 'completed':
            output = get('output')
            if isinstance(output, Mapping):
                latest = output
    if (latest is not None and latest.get('source_resolution_required') is True
            and latest.get('source_evidence_verified') is True
            and latest.get('source_selection_required') is not True):
        return latest
    return None


def compact_scope_evidence(output: Mapping[str, object], *, original_chars: int) -> dict[str, object]:
    """Keep scope state typed when optional connector catalogues exceed history limits."""
    keys = (
        'capabilities', 'available_tools', 'unavailable_tools', 'explicit_tool_names',
        'required_tool_names', 'requested_required_tool_names', 'unavailable_required_tool_names',
        'artifact_extensions', 'required_artifact_extensions', 'excluded_artifact_extensions',
        'embedded_media_artifact_extensions', 'connector_app_ids', 'scheduler_action',
        'scheduler_effect_actions', 'scheduler_target_scope', 'scheduler_selection_policy',
        'scheduler_toggle_enabled', 'tracking_mode', 'requested_outcome', 'scope_requested',
        'source_selection_required', 'source_resolution_required', 'source_evidence_verified',
        'connector_source_user_requested', 'connector_source_unavailable',
        'connector_source_evidence_required', 'missing_connector_app_scope',
        'source_artifact_delivery_requested', 'read_only_source_delivery', 'read_only_local_scope',
        'direct_read_complete_request', 'suppress_activity',
        'superseded_unselected_required_tool_names',
    )
    compact = {key: output[key] for key in keys if key in output}
    for key in ('connector_source_evidence', 'source_artifact_request_evidence'):
        value = output.get(key)
        if isinstance(value, str) and len(value) <= 2000:
            compact[key] = value
        elif value:
            # A partial quote must never masquerade as verified source proof.
            compact['source_evidence_verified'] = False
    sources = output.get('available_sources')
    if isinstance(sources, (list, tuple)):
        compact['available_sources'] = [
            {key: row[key] for key in ('provider_id', 'display_name', 'read_tools',
                'active_app_ids', 'permission_mode', 'app_inventory_kind') if key in row}
            for row in sources[:20] if isinstance(row, Mapping)]
        if len(sources) > 20:
            compact['available_sources_truncated'] = True
    # Resolving a user-selected source requires the exact adapter identities
    # and the metadata-only recovery contract. Omitting either can turn an
    # unresolved adapter into an unnecessary request for renewed consent.
    message=output.get('message')
    if isinstance(message,str) and len(message)<=2000:
        compact['message']=message
    compact.update(result_kind='tool_scope_evidence', truncated=True, original_chars=original_chars)
    return compact


def compact_source_identity_evidence(output, *, encoded: str, max_chars: int):
    """Retain declared identity anchors, never claim truncated rows as source facts."""
    import json
    from urllib.parse import urlsplit, urlunsplit
    declarations = output.get('source_record_collections') if isinstance(output, Mapping) else None
    if not isinstance(declarations, list):
        return None
    urls, values, records = [], [], 0
    for declaration in declarations[:10]:
        if not isinstance(declaration, Mapping):
            continue
        path, fields = declaration.get('path'), declaration.get('identity_fields')
        if not (isinstance(path, list) and 1 <= len(path) <= 4
                and all(isinstance(k, str) for k in path) and isinstance(fields, list)):
            continue
        rows = output
        for key in path:
            rows = rows.get(key) if isinstance(rows, Mapping) else None
        if declaration.get('cardinality') == 'one' and isinstance(rows, Mapping):
            rows = [rows]
        for row in rows[:100] if isinstance(rows, list) else ():
            if not isinstance(row, Mapping):
                continue
            records += 1
            for key in fields[:12]:
                value = row.get(key) if isinstance(key, str) else None
                if not isinstance(value, (str, int, float)) or isinstance(value, bool):
                    continue
                value = str(value).strip()
                try:
                    parsed = urlsplit(value)
                    if parsed.scheme in {'http', 'https'} and (parsed.username or parsed.password):
                        continue
                    public = parsed.scheme in {'http', 'https'} and parsed.hostname and not parsed.username and not parsed.password
                except ValueError:
                    public = False
                if public:
                    # These are identity paths, not substitutes for full query-
                    # dependent source URLs, quotations or price evidence.
                    urls.append(urlunsplit((parsed.scheme, parsed.netloc, parsed.path, '', '')))
                elif value and len(value) <= 256:
                    values.append(value)
    urls, values = list(dict.fromkeys(urls)), list(dict.fromkeys(values))
    if not urls and not values:
        return None
    result = {'result_kind': 'source_identity_inventory', 'truncated': True,
        'original_chars': len(encoded), 'source_identity_record_count': records,
        'source_identity_url_paths': [], 'source_identity_values': [], 'identity_anchors_omitted': 0,
        'preview': ''}
    for key, items in [('source_identity_url_paths', urls), ('source_identity_values', values)]:
        for item in items:
            result[key].append(item)
            if len(json.dumps(result, ensure_ascii=False)) > max_chars-100:
                result[key].pop();result['identity_anchors_omitted'] += 1
    room = max(0, max_chars-len(json.dumps(result, ensure_ascii=False))-100)
    result['preview'] = encoded[:min(3000, room//2)]
    return result


def current_read_only_local_scope(results):
    """An explicit current read plan supersedes old delivery-format guesses."""
    scope = None
    for result in results or ():
        name = result.get('tool_name') if isinstance(result, Mapping) else getattr(result, 'tool_name', None)
        status = result.get('status') if isinstance(result, Mapping) else getattr(result, 'status', None)
        output = result.get('output') if isinstance(result, Mapping) else getattr(result, 'output', None)
        if status != 'completed' or not isinstance(output, Mapping):
            continue
        if name == 'request_tool_scope':
            scope = output
        # A current generated file retains its independent delivery lifecycle,
        # including when a later read scope verifies that file.
        if output.get('artifact_path') or output.get('artifact_paths'):
            return False
    return bool(scope and scope.get('read_only_local_scope') is True
        and scope.get('artifact_extensions') == []
        and scope.get('source_artifact_delivery_requested') is False)
