"""Bounded, structured source evidence for model context and durable history."""
from __future__ import annotations

import json
from collections.abc import Mapping


def compact_source_observations(output: object, *, max_chars: int = 12_000) -> dict | None:
    if not isinstance(output, Mapping) or output.get('result_kind') != 'source_observations':
        return None
    observations = output.get('observations')
    if not isinstance(observations, list):
        return None
    compact = {key: output[key] for key in (
        'backend', 'result_kind', 'report_status', 'artifact_paths', 'artifact_descriptors', 'continuation', 'goal_status', 'next_action', 'format',
        'elapsed_seconds', 'steps', 'page_url', 'rejected_observations', 'rejection_counts', 'omitted_unknown_count',
    ) if key in output}
    compact['blocker'] = str(output['blocker'])[:1000] if output.get('blocker') else None
    compact['unknowns'] = []
    unknowns = output.get('unknowns') if isinstance(output.get('unknowns'), list) else []
    for value in unknowns[:30]:
        compact['unknowns'].append(str(value)[:1000])
        if len(json.dumps(compact, ensure_ascii=False, default=str)) > max_chars // 2:
            compact['unknowns'].pop()
    if len(compact['unknowns']) < len(unknowns):
        compact['omitted_unknown_count'] = int(compact.get('omitted_unknown_count') or 0) + len(unknowns) - len(compact['unknowns'])
        compact['report_status'] = 'partial'
    compact['sources'] = []
    compact['observations'] = []
    sources = output.get('sources') if isinstance(output.get('sources'), list) else []
    identities = {}
    omitted = int(output.get('omitted_observation_count') or 0)
    for item in observations:
        if not isinstance(item, Mapping):
            omitted += 1
            continue
        index = item.get('source_index')
        source = dict(sources[index]) if isinstance(index, int) and 0 <= index < len(sources) and isinstance(sources[index], Mapping) else {}
        source.update({key: item[key] for key in ('source_url', 'evidence_path', 'captured_at') if key in item})
        if source.get('source_url') == compact.get('page_url'):
            source.pop('source_url')
            source['page_url_ref'] = True
        identity = json.dumps(source, ensure_ascii=False, sort_keys=True, default=str)
        source_index = identities.get(identity, len(compact['sources']))
        new_source = identity not in identities
        record = {key: item[key] for key in ('label', 'value', 'quote', 'control_state') if key in item}
        record['source_index'] = source_index
        if new_source:
            compact['sources'].append(source)
        compact['observations'].append(record)
        # Reserve space for omission metadata. Never turn a clipped value into
        # an apparent fact or drop the typed result into an opaque text preview.
        if len(json.dumps(compact, ensure_ascii=False, default=str)) > max_chars - 160:
            compact['observations'].pop()
            if new_source:
                compact['sources'].pop()
            omitted += 1
        elif new_source:
            identities[identity] = source_index
    if omitted:
        compact['omitted_observation_count'] = omitted
        compact['report_status'] = 'partial' if compact['observations'] else 'blocked'
    return compact


def source_report_status(tool_results):
    """Usable producer reports carry a terminal status and grounded records."""
    from nullion.tools import normalize_tool_status
    reports = []
    for result in tool_results or ():
        output = getattr(result, 'output', None)
        if normalize_tool_status(getattr(result, 'status', None)) != 'completed' or not isinstance(output, Mapping):
            continue
        if output.get('result_kind') != 'source_observations' or output.get('report_status') not in {'completed', 'partial'}:
            continue
        if any(isinstance(row, Mapping) and isinstance(row.get('value'), str) and row['value'].strip()
               and isinstance(row.get('quote'), str) and ' '.join(row['value'].split()) in ' '.join(row['quote'].split())
               for row in output.get('observations') or ()):
            reports.append(output['report_status'])
    return reports[-1] if reports else None


def partial_source_report_disclosures(tool_results) -> list[str]:
    """Explicit unknowns from completed reports with actual quoted observations."""
    from nullion.tools import normalize_tool_status
    unknowns = []
    for result in tool_results:
        output = getattr(result, 'output', None)
        if normalize_tool_status(getattr(result, 'status', None)) != 'completed' or not isinstance(output, Mapping):
            continue
        if output.get('result_kind') != 'source_observations' or output.get('report_status') != 'partial':
            continue
        records = output.get('observations')
        if not isinstance(records, list) or not any(isinstance(row, Mapping) and isinstance(row.get('value'), str) and row['value'].strip() and isinstance(row.get('quote'), str) and ' '.join(row['value'].split()) in ' '.join(row['quote'].split()) for row in records):
            continue
        disclosures = output.get('unknowns')
        if not isinstance(disclosures, list):
            continue
        for value in disclosures:
            if isinstance(value, str) and value.strip() and value not in unknowns:
                unknowns.append(value[:1000])
    return unknowns


def partial_source_report_reply(tool_results, *, draft=None) -> str:
    """Deliver captured source quotes and typed unknowns without inventing facts."""
    results = list(tool_results)
    unknowns = partial_source_report_disclosures(results)
    # The source report already validates each quote against captured page text.
    # A later free-form draft must not replace those facts with an unsupported
    # failure claim or a guessed alternative link.
    quotes = []
    source_urls = []
    for result in results:
        output = getattr(result, 'output', None)
        if not isinstance(output, Mapping) or output.get('result_kind') != 'source_observations' or output.get('report_status') not in {'completed', 'partial'}:
            continue
        for row in output.get('observations') or []:
            source_index = row.get('source_index') if isinstance(row, Mapping) else None
            sources = output.get('sources') or []
            source = sources[source_index] if isinstance(source_index, int) and 0 <= source_index < len(sources) else row
            url = (output.get('page_url') if source.get('page_url_ref') else source.get('source_url')) if isinstance(source, Mapping) else None
            if isinstance(url, str) and url.startswith(('https://', 'http://')) and url not in source_urls:
                source_urls.append(url)
            value = str(row.get('value') or '').strip() if isinstance(row, Mapping) else ''
            label = str(row.get('label') or '').strip() if isinstance(row, Mapping) else ''
            fact = f'{label}: {value}' if label else str(row.get('quote') or value).strip()
            if value and fact not in quotes:
                quotes.append(fact[:2000])
    findings = '\n'.join('- ' + fact for fact in quotes[:20])
    references = '\n\n' + ' · '.join(f'[Source {index + 1}]({url})' for index, url in enumerate(source_urls[:10])) if source_urls else ''
    unresolved = '\n\nUnshown or unresolved:\n' + '\n'.join('- ' + value for value in unknowns) if unknowns else ''
    return 'Here are the available findings:\n\n' + findings + references + unresolved


def pending_source_continuation(tool_results):
    """Return a producer's typed continuation until another source is observed."""
    from nullion.tools import normalize_tool_status
    pending = None
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, 'status', None)) != 'completed':
            continue
        output = getattr(result, 'output', None)
        if not isinstance(output, Mapping) or output.get('result_kind') != 'source_observations':
            continue
        continuation = output.get('continuation')
        if isinstance(continuation, Mapping) and continuation.get('kind') == 'alternative_source':
            pending = dict(continuation)
        elif pending and output.get('page_url') and output.get('page_url') != pending.get('source_url'):
            pending = None
    return pending
