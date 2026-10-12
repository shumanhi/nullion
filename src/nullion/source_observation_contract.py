"""Bounded, structured source evidence for model context and durable history."""
from __future__ import annotations

import json
import unicodedata
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


def _readable_source_line(text: str) -> str:
    """Remove inaccessible font glyphs and empty separators from captured UI."""
    clean = ''.join(char for char in text if unicodedata.category(char) != 'Co')
    return ' · '.join(part.strip() for part in clean.split('·') if part.strip()).strip()


SOURCE_ANSWER_TOOL = 'present_source_answer'


def source_answer_context(tool_results) -> dict:
    """Verified, deduplicated evidence; relevance selection belongs to the model."""
    from nullion.tools import normalize_tool_status
    records, identities = [], {}
    for result_index, result in enumerate(tool_results or ()):
        output = getattr(result, 'output', None)
        if normalize_tool_status(getattr(result, 'status', None)) != 'completed' or not isinstance(output, Mapping):
            continue
        if output.get('result_kind') != 'source_observations' or output.get('report_status') not in {'completed', 'partial'}:
            continue
        for row_index, row in enumerate(output.get('observations') or ()):
            if not isinstance(row, Mapping) or not isinstance(row.get('value'), str) or not isinstance(row.get('quote'), str):
                continue
            value, quote = row['value'].strip(), row['quote'].strip()
            if not value or ' '.join(value.split()) not in ' '.join(quote.split()):
                continue
            identity = unicodedata.normalize('NFKC', ' '.join(value.split())).casefold()
            sources, index = output.get('sources') or [], row.get('source_index')
            source = sources[index] if isinstance(index, int) and not isinstance(index, bool) and 0 <= index < len(sources) else row
            url = (output.get('page_url') if source.get('page_url_ref') else source.get('source_url')) if isinstance(source, Mapping) else None
            if identity in identities:
                record = identities[identity]
                if quote not in record['quotes']:
                    record['quotes'].append(quote)
                if isinstance(url, str) and url.startswith(('https://', 'http://')) and url not in record['urls']:
                    record['urls'].append(url)
                continue
            record = {'id': f'e{result_index}.{row_index}', 'value': value,
                      'label': str(row.get('label') or ''), 'quotes': [quote],
                      'urls': [url] if isinstance(url, str) and url.startswith(('https://', 'http://')) else []}
            records.append(record)
            identities[identity] = record
    for record in records:
        excerpts, seen = [], set()
        for quote_index, quote in enumerate(record['quotes']):
            for line_index, text in enumerate(quote.splitlines()):
                text = text.strip()
                if text and text not in seen:
                    seen.add(text)
                    excerpts.append({'id': f"{record['id']}.q{quote_index}.l{line_index}", 'text': text})
        record['excerpts'] = excerpts
    return {'report_status': source_report_status(tool_results or ()),
            'evidence': records, 'limitations': [
        {'id': f'u{index}', 'text': text}
        for index, text in enumerate(partial_source_report_disclosures(tool_results or ()))
    ]}


def source_answer_tool_definition(context: dict) -> dict:
    ids = [record['id'] for record in context['evidence']]
    unknowns = [record['id'] for record in context['limitations']]
    excerpts = [excerpt['id'] for record in context['evidence'] for excerpt in record['excerpts']]
    return {'name': SOURCE_ANSWER_TOOL, 'description':
        'Compose a focused answer to the original request from the verified evidence. '
        'Select only relevant findings; combine supporting details into the same point. '
        'Do not enumerate the evidence inventory or repeat the same item under different labels. '
        'Prioritize the specific emphasis or constraint over broad category matches; do not add unrelated items as padding. '
        'Usually choose the smallest useful set of 1–3 points unless the user requested more. Each point names one evidence item; '
        'detail_ids must belong to that item, never a different product or entity. Explain useful distinctions with '
        'short supporting excerpts selected by their detail_ids; never rewrite the excerpt text or invent claims, rankings, or applicability. '
        'The intro is a short transition, not an uncited factual claim. Select only limitations that '
        'materially affect the answer; do not append every unavailable field. No further tool work is needed.',
        'input_schema': {'type': 'object', 'additionalProperties': False,
            'properties': {'intro': {'type': 'string', 'maxLength': 350},
                'points': {'type': 'array', 'minItems': 1, 'maxItems': 20, 'items': {
                    'type': 'object', 'additionalProperties': False,
                    'properties': {'evidence_ids': {'type': 'array', 'minItems': 1, 'maxItems': 1,
                                                  'uniqueItems': True, 'items': {'type': 'string', 'enum': ids}},
                        'detail_ids': {'type': 'array', 'maxItems': 3, 'uniqueItems': True,
                                       'items': {'type': 'string', 'enum': excerpts}},
                        'relevance': {'type': 'string', 'minLength': 1, 'maxLength': 250}},
                    'required': ['evidence_ids', 'detail_ids', 'relevance']}},
                'limitation_ids': {'type': 'array', 'maxItems': 3, 'uniqueItems': True,
                                   'items': {'type': 'string', **({'enum': unknowns} if unknowns else {})}}},
            'required': ['intro', 'points', 'limitation_ids']}}


def render_source_answer(tool_results, presentation: object) -> str:
    """Validate selected identities and literal supporting excerpts before delivery."""
    context = source_answer_context(tool_results)
    records = {row['id']: row for row in context['evidence']}
    unknowns = {row['id']: row['text'] for row in context['limitations']}
    if not isinstance(presentation, Mapping) or set(presentation) != {'intro', 'points', 'limitation_ids'}:
        raise ValueError('Expected a structured source answer')
    intro, points, limitations = (presentation[key] for key in ('intro', 'points', 'limitation_ids'))
    if not isinstance(intro, str) or len(intro) > 350 or not isinstance(points, list) or not 1 <= len(points) <= 20:
        raise ValueError('Invalid source answer length')
    if not isinstance(limitations, list) or len(limitations) > 3 or any(not isinstance(i, str) or i not in unknowns for i in limitations):
        raise ValueError('Unknown limitation identity')
    blocks, titles = [], set()
    for point in points:
        if not isinstance(point, Mapping) or set(point) not in ({'evidence_ids', 'detail', 'relevance'}, {'evidence_ids', 'detail_ids', 'relevance'}):
            raise ValueError('Invalid source answer point')
        ids, relevance = (point[key] for key in ('evidence_ids', 'relevance'))
        if not isinstance(ids, list) or len(ids) != 1 or any(not isinstance(i, str) or i not in records for i in ids):
            raise ValueError('Unknown evidence identity')
        if len(set(ids)) != len(ids) or not isinstance(relevance, str) or not relevance.strip() or len(relevance) > 250:
            raise ValueError('Missing relevance decision')
        if 'detail_ids' in point:
            selected = point['detail_ids']
            available = {excerpt['id']: excerpt['text'] for identity in ids for excerpt in records[identity]['excerpts']}
            if not isinstance(selected, list) or len(selected) > 3 or any(not isinstance(i, str) or i not in available for i in selected):
                raise ValueError('Unknown supporting excerpt identity')
            detail = ' '.join(available[i] for i in dict.fromkeys(selected)
                              if ' '.join(available[i].split()) != ' '.join(records[ids[0]]['value'].split()))
        else:
            detail = point['detail']
        if not isinstance(detail, str) or len(detail) > 650:
            raise ValueError('Invalid supporting excerpt')
        if 'detail_ids' not in point and detail.strip() and not any(' '.join(detail.split()) in ' '.join(quote.split()) for i in ids for quote in records[i]['quotes']):
            raise ValueError('Supporting excerpt is not verified')
        primary = records[ids[0]]
        if primary['id'] in titles:
            raise ValueError('Repeated answer item')
        titles.add(primary['id'])
        title = _readable_source_line(primary['value'].splitlines()[0])
        body = _readable_source_line(detail.strip())
        if body == title:
            body = ''
        urls = list(dict.fromkeys(url for i in ids for url in records[i]['urls']))
        citation = ' ' + ' · '.join(f'[Source {index + 1}]({url})' for index, url in enumerate(urls[:2])) if urls else ''
        blocks.append(f'- **{title}**' + (f' — {body}' if body else '') + citation)
    notes = list(dict.fromkeys(unknowns[i] for i in limitations))
    return (intro.strip() + '\n\n' if intro.strip() else '') + '\n\n'.join(blocks) + ('\n\n' + ' '.join(notes) if notes else '')


def partial_source_report_reply(tool_results, *, draft=None) -> str:
    """A source inventory is never a relevance-aware fallback answer."""
    try:
        return render_source_answer(tool_results, draft)
    except ValueError:
        return 'I found source information, but could not turn it into a focused answer. Please retry.'


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
