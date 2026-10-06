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
        'backend', 'result_kind', 'report_status', 'artifact_paths', 'format',
        'elapsed_seconds', 'steps', 'page_url', 'rejected_observations', 'omitted_unknown_count',
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
        identity = json.dumps(source, ensure_ascii=False, sort_keys=True, default=str)
        source_index = identities.get(identity, len(compact['sources']))
        new_source = identity not in identities
        record = {key: item[key] for key in ('label', 'value', 'quote') if key in item}
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
    """Preserve a summary attempt or exact source quotes plus typed unknowns."""
    results = list(tool_results)
    unknowns = partial_source_report_disclosures(results)
    if draft and str(draft).strip():
        findings = str(draft).strip()
    else:
        quotes = []
        for result in results:
            output = getattr(result, 'output', None)
            if not isinstance(output, Mapping) or output.get('result_kind') != 'source_observations':
                continue
            for row in output.get('observations') or []:
                quote = str(row.get('quote') or '').strip() if isinstance(row, Mapping) else ''
                if quote and quote not in quotes:
                    quotes.append(quote[:2000])
        findings = '\n'.join('> ' + quote.replace('\n', '\n> ') for quote in quotes[:20])
    return 'Here are the available findings:\n\n' + findings + '\n\nUnshown or unresolved:\n' + '\n'.join('- ' + value for value in unknowns)
