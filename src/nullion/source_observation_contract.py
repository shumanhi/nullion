"""Bounded, structured source evidence for model context and durable history."""
from __future__ import annotations

import json
import hashlib
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
        'elapsed_seconds', 'steps', 'page_url', 'rejected_observations', 'rejection_counts', 'omitted_unknown_count', 'evidence_value_repair_count', 'completion_scope', 'source_record_binding',
    ) if key in output}
    compact['blocker'] = str(output['blocker'])[:1000] if output.get('blocker') else None
    audit = output.get('execution_audit')
    if isinstance(audit, Mapping):
        compact['execution_audit'] = {key: audit[key] for key in ('version', 'complete', 'read_only_actions_only',
            'downloaded_file_count', 'host_file_actions_enabled', 'captures_are_internal', 'deliverable_file_count') if key in audit}
        if isinstance(audit.get('action_names'), list):
            compact['execution_audit']['action_names'] = audit['action_names'][:30]
        # Retain a bounded navigation inventory only when it is complete.
        urls = audit.get('visited_urls')
        if isinstance(urls, list) and len(json.dumps(urls, ensure_ascii=False)) < 2200:
            compact['execution_audit']['visited_urls'] = urls
            compact['execution_audit']['url_inventory_complete'] = audit.get('url_inventory_complete') is True
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
    documents = {}
    omitted = int(output.get('omitted_observation_count') or 0)
    for item in observations:
        if not isinstance(item, Mapping):
            omitted += 1
            continue
        index = item.get('source_index')
        source = dict(sources[index]) if isinstance(index, int) and 0 <= index < len(sources) and isinstance(sources[index], Mapping) else {}
        document = source.pop('main_document_text', item.get('main_document_text'))
        source.update({key: item[key] for key in ('source_url', 'evidence_path', 'captured_at') if key in item})
        if source.get('source_url') and source.get('source_url') == compact.get('page_url'):
            source.pop('source_url', None)
            source['page_url_ref'] = True
        identity = json.dumps(source, ensure_ascii=False, sort_keys=True, default=str)
        source_index = identities.get(identity, len(compact['sources']))
        new_source = identity not in identities
        record = {key: item[key] for key in ('label', 'value', 'quote', 'control_state', 'covers_main_document') if key in item}
        if record.get('covers_main_document') is True and record.get('value') == record.get('quote') and isinstance(record.get('quote'), str):
            record['value'] = next((line.strip() for line in record['quote'].splitlines() if line.strip()), record['value'])
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
        if identity in identities and isinstance(document, str) and 0 < len(document) <= 4000:
            documents[source_index] = document
    if omitted:
        compact['omitted_observation_count'] = omitted
        compact['report_status'] = 'partial' if compact['observations'] else 'blocked'
    # Complete document context is optional and never displaces quoted facts.
    # Keep exact text or omit it; clipped text must never imply page-wide coverage.
    def capture_time(index):
        value = compact['sources'][index].get('captured_at')
        return value if isinstance(value, (int, float)) and not isinstance(value, bool) else 0
    for index in sorted(documents, key=capture_time, reverse=True):
        compact['sources'][index]['main_document_text'] = documents[index]
        if len(json.dumps(compact, ensure_ascii=False, default=str)) > max_chars - 160:
            compact['sources'][index].pop('main_document_text')
    # Diagnostics never displace grounded facts or become answer evidence.
    # Keep enough rejected input to distinguish capture/formatting failures
    # from an external source blocker in a durable turn record.
    errors = output.get('validation_errors')
    if isinstance(errors, list):
        compact['validation_errors'] = []
        for error in errors[:3]:
            if not isinstance(error, Mapping):
                continue
            row = {key: str(error[key])[:500] for key in ('reason', 'label', 'value', 'quote')
                   if isinstance(error.get(key), str)}
            if isinstance(error.get('source_id'), int):
                row['source_id'] = error['source_id']
            compact['validation_errors'].append(row)
            if len(json.dumps(compact, ensure_ascii=False, default=str)) > max_chars:
                compact['validation_errors'].pop()
                break
    return compact


def source_report_status(tool_results):
    """Usable producer reports carry a terminal status and grounded records."""
    from nullion.tools import normalize_tool_status
    reports = []
    for result in tool_results or ():
        output = getattr(result, 'output', None)
        if normalize_tool_status(getattr(result, 'status', None)) != 'completed' or not isinstance(output, Mapping):
            continue
        if output.get('result_kind') != 'source_observations':
            continue
        if (output.get('report_status') == 'blocked' and output.get('next_action') == 'report'
                and not output.get('continuation') and output.get('blocker')):
            reports.append('blocked')
            continue
        if output.get('report_status') not in {'completed', 'partial'}:
            continue
        if any(isinstance(row, Mapping) and isinstance(row.get('value'), str) and row['value'].strip()
               and isinstance(row.get('quote'), str) and ' '.join(row['value'].split()) in ' '.join(row['quote'].split())
               for row in output.get('observations') or ()):
            reports.append(output['report_status'])
    return reports[-1] if reports else None


def terminal_source_report_tools(tool_results) -> frozenset[str]:
    """An explicit producer stop is not permission to repeat that producer.

    Other registered methods and explicit alternative-source continuations
    remain available. This contract is per turn and independent of page prose.
    """
    from nullion.tools import normalize_tool_status
    latest = {}
    for result in tool_results or ():
        output = getattr(result, 'output', None)
        if (normalize_tool_status(getattr(result, 'status', None)) == 'completed'
                and isinstance(output, Mapping) and output.get('result_kind') == 'source_observations'):
            name = getattr(result, 'tool_name', None)
            if isinstance(name, str) and name:
                latest[name] = output
    return frozenset(name for name, output in latest.items()
                     if output.get('report_status') in {'partial', 'blocked'}
                     and output.get('next_action') == 'report' and not output.get('continuation'))


def progressing_partial_source_tools(tool_results) -> frozenset[str]:
    """A producer stop is not exhaustion when material work is still missing.

    Only the existing structured completion reviewer may request continuation.
    Repeated identical evidence cannot reopen the same producer indefinitely.
    """
    from nullion.tools import normalize_tool_status
    reports = {}
    for result in tool_results or ():
        output = getattr(result, 'output', None)
        if (normalize_tool_status(getattr(result, 'status', None)) == 'completed'
                and isinstance(output, Mapping) and output.get('result_kind') == 'source_observations'):
            reports.setdefault(getattr(result, 'tool_name', None), []).append(output)
    allowed = set()
    for name, rows in reports.items():
        latest = rows[-1]
        if (not name or latest.get('report_status') != 'partial' or latest.get('blocker')
                or not latest.get('unknowns')):
            continue
        current = _source_observation_identities(latest)
        previous = frozenset().union(*(_source_observation_identities(older) for older in rows[:-1]))
        if not current or (previous and not current - previous):
            continue
        allowed.add(name)
    return frozenset(allowed)


def _source_observation_identities(output):
    """Grounded record identity; capture paths, timing and labels are incidental."""
    identities = set()
    for row in output.get('observations') or ():
        if not isinstance(row, Mapping) or not isinstance(row.get('value'), str) or not isinstance(row.get('quote'), str):
            continue
        value, quote = ' '.join(row['value'].split()), ' '.join(row['quote'].split())
        if not value or value not in quote:
            continue
        sources, index = output.get('sources') or [], row.get('source_index')
        source = sources[index] if isinstance(index, int) and not isinstance(index, bool) and 0 <= index < len(sources) and isinstance(sources[index], Mapping) else row
        url = output.get('page_url') if source.get('page_url_ref') else source.get('source_url')
        controls = json.dumps(row.get('control_state'), sort_keys=True, ensure_ascii=False, default=str)
        identities.add((value, quote, str(url or ''), controls))
    return frozenset(identities)


def partial_source_report_disclosures(tool_results) -> list[str]:
    """Explicit unknowns from incomplete reports; identity order stays stable."""
    from nullion.tools import normalize_tool_status
    unknowns = []
    for result in tool_results:
        output = getattr(result, 'output', None)
        if normalize_tool_status(getattr(result, 'status', None)) != 'completed' or not isinstance(output, Mapping):
            continue
        if output.get('result_kind') != 'source_observations' or output.get('report_status') not in {'partial', 'blocked'}:
            continue
        records = output.get('observations')
        terminal_blocked = (output.get('report_status') == 'blocked' and output.get('next_action') == 'report'
                            and not output.get('continuation') and output.get('blocker'))
        if not terminal_blocked and (not isinstance(records, list) or not any(isinstance(row, Mapping) and isinstance(row.get('value'), str) and row['value'].strip() and isinstance(row.get('quote'), str) and ' '.join(row['value'].split()) in ' '.join(row['quote'].split()) for row in records)):
            continue
        disclosures = output.get('unknowns')
        if not isinstance(disclosures, list):
            disclosures = []
        for value in disclosures:
            if isinstance(value, str) and value.strip() and value not in unknowns:
                unknowns.append(value[:1000])
    return unknowns


def source_report_blockers(tool_results) -> list[dict]:
    """Access/evidence causes have a separate identity namespace from unknowns."""
    blockers = []
    for index, result in enumerate(tool_results or ()):
        if source_report_status([result]) not in {'partial', 'blocked'}:
            continue
        value = result.output.get('blocker')
        if isinstance(value, str) and value.strip():
            blockers.append({'id': f'b{index}', 'text': value[:1000], 'kind': 'blocker'})
    return blockers


def _readable_source_line(text: str) -> str:
    """Remove inaccessible font glyphs and empty separators from captured UI."""
    clean = ''.join(char for char in text if unicodedata.category(char) != 'Co')
    return ' · '.join(part.strip() for part in clean.split('·') if part.strip()).strip()


SOURCE_ANSWER_TOOL = 'present_source_answer'


def source_answer_context(tool_results, *, include_bindings=False) -> dict:
    """Verified, deduplicated evidence; relevance selection belongs to the model."""
    from nullion.tools import normalize_tool_status
    records, identities, capture_times, capture_documents = [], {}, {}, {}
    capture_invocations, capture_sources, capture_controls = {}, {}, {}
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
            capture = None
            if (isinstance(source, Mapping) and isinstance(source.get('captured_at'), (int, float))
                    and not isinstance(source.get('captured_at'), bool) and isinstance(url, str)):
                capture = hashlib.sha256(json.dumps([url, source['captured_at'], source.get('evidence_path')],
                    ensure_ascii=False).encode()).hexdigest()[:20]
                capture_times[capture] = source['captured_at']
                invocation = str(getattr(result, 'invocation_id', '') or '')
                if invocation:
                    capture_invocations.setdefault(capture, set()).add(invocation)
                capture_sources[capture] = {**source, 'source_url': url}
                if include_bindings and isinstance(row.get('control_state'), Mapping):
                    controls = capture_controls.setdefault(capture, {}).setdefault(' '.join(quote.split()), [])
                    if row['control_state'] not in controls:
                        controls.append(row['control_state'])
                document = source.get('main_document_text')
                if isinstance(document, str) and 0 < len(document) <= 4000:
                    capture_documents[capture] = document
                elif row.get('covers_main_document') is True:
                    capture_documents[capture] = quote
            if identity in identities:
                record = identities[identity]
                if quote not in record['quotes']:
                    record['quotes'].append(quote)
                    record['quote_capture_ids'].append([])
                    record['whole_document_capture_ids'].append([])
                if isinstance(url, str) and url.startswith(('https://', 'http://')) and url not in record['urls']:
                    record['urls'].append(url)
                if capture and capture not in record['capture_ids']:
                    record['capture_ids'].append(capture)
                quote_captures = record['quote_capture_ids'][record['quotes'].index(quote)]
                if capture and capture not in quote_captures:
                    quote_captures.append(capture)
                if row.get('covers_main_document') is True and capture:
                    complete = record['whole_document_capture_ids'][record['quotes'].index(quote)]
                    if capture not in complete:
                        complete.append(capture)
                continue
            record = {'id': f'e{result_index}.{row_index}', 'value': value,
                      'label': str(row.get('label') or ''), 'quotes': [quote],
                      'capture_ids': [capture] if capture else [],
                      'quote_capture_ids': [[capture] if capture else []],
                      'whole_document_capture_ids': [[capture] if capture and row.get('covers_main_document') is True else []],
                      'urls': [url] if isinstance(url, str) and url.startswith(('https://', 'http://')) else []}
            records.append(record)
            identities[identity] = record
    for record in records:
        excerpts, seen = [], {}
        for quote_index, quote in enumerate(record['quotes']):
            for line_index, text in enumerate(quote.splitlines()):
                text = text.strip()
                captures = record['quote_capture_ids'][quote_index]
                if text and text not in seen:
                    excerpt = {'id': f"{record['id']}.q{quote_index}.l{line_index}", 'text': text,
                        'capture_ids': list(captures)}
                    excerpts.append(excerpt)
                    seen[text] = excerpt
                elif text:
                    seen[text]['capture_ids'] = list(dict.fromkeys(seen[text]['capture_ids'] + captures))
        record['excerpts'] = excerpts
    result = {'report_status': source_report_status(tool_results or ()),
            'evidence': records, 'capture_times': capture_times, 'capture_documents': capture_documents, 'limitations': [
        {'id': f'u{index}', 'text': text, 'kind': 'unknown'}
        for index, text in enumerate(partial_source_report_disclosures(tool_results or ()))
    ] + source_report_blockers(tool_results)}
    if include_bindings:
        result['_capture_invocations'] = capture_invocations
        result['_capture_sources'] = capture_sources
        result['_capture_controls'] = capture_controls
    return result


def source_answer_review_records(tool_results, presentation):
    """Runtime-owned record references for independently reviewed answer items.

    These are evidence references within real invocations, never extra tool calls.
    Only literal, capture-bound selections accepted by the renderer can create one.
    """
    from nullion.tools import ToolResult
    results = list(tool_results)
    try:
        render_source_answer(results, presentation)
    except (ValueError, TypeError):
        return [], {}, set()
    context = source_answer_context(results, include_bindings=True)
    records = {row['id']: row for row in context['evidence']}
    parents = {str(getattr(row, 'invocation_id', '')): row for row in results if getattr(row, 'invocation_id', None)}
    bound, fingerprints = [], {}
    for point in presentation['points']:
        selected = [records[i] for i in point['evidence_ids']]
        captures = set.intersection(*(set(row['capture_ids']) for row in selected))
        excerpts = {e['id']: e for row in selected for e in row['excerpts']}
        chosen = list(point.get('title_ids') or []) + list(point.get('detail_ids') or [])
        for identity in chosen:
            captures.intersection_update(excerpts[identity]['capture_ids'])
        detail = point.get('detail')
        if isinstance(detail, str) and detail.strip():
            supported = {cid for row in selected for index, quote in enumerate(row['quotes'])
                if ' '.join(detail.split()) in ' '.join(quote.split()) for cid in row['quote_capture_ids'][index]}
            captures.intersection_update(supported)
        captures = {cid for cid in captures if len(context['_capture_invocations'].get(cid, ())) == 1}
        if not captures:
            continue
        capture = max(captures, key=lambda cid: context['capture_times'][cid])
        parent_id = next(iter(context['_capture_invocations'][capture]))
        parent = parents.get(parent_id)
        if parent is None or not isinstance(parent.output, Mapping):
            continue
        title = ' '.join(excerpts[i]['text'] for i in point['title_ids']) if point.get('title_ids') else selected[0]['value'].splitlines()[0]
        groups = point.get('detail_groups')
        fields = ([' '.join(excerpts[i]['text'] for i in group) for group in groups] if groups is not None
            else [excerpts[i]['text'] for i in point.get('detail_ids') or ()])
        if isinstance(detail, str) and detail.strip():
            fields.append(detail)
        source = context['_capture_sources'][capture]
        canonical = json.dumps({'title': ' '.join(title.split()),
            'fields': [' '.join(value.split()) for value in fields], 'source_url': source['source_url']},
            ensure_ascii=False, sort_keys=True)
        identity = parent_id + '/source-record/' + hashlib.sha256((capture + canonical).encode()).hexdigest()[:20]
        observations = []
        for row in selected:
            index = next(i for i, caps in enumerate(row['quote_capture_ids']) if capture in caps)
            observation = {'value': row['value'], 'quote': row['quotes'][index], 'source_index': 0}
            if capture in row['whole_document_capture_ids'][index]:
                observation['covers_main_document'] = True
            controls = context['_capture_controls'].get(capture, {}).get(' '.join(observation['quote'].split()), ())
            if controls:
                observations.extend({**observation, 'control_state': control} for control in controls)
            else:
                observations.append(observation)
        proof_source = {'source_url': source['source_url'], 'captured_at': context['capture_times'][capture]}
        if capture in context['capture_documents']:
            proof_source['main_document_text'] = context['capture_documents'][capture]
        output = {key: parent.output[key] for key in ('result_kind', 'report_status', 'goal_status',
            'completion_scope', 'unknowns', 'blocker', 'execution_audit') if key in parent.output}
        output.update(observations=observations, sources=[proof_source], source_record_binding={
            'parent_invocation_id': parent_id, 'capture_id': capture, 'title': title, 'fields': fields})
        bound.append(ToolResult(identity, parent.tool_name, parent.status, output, parent.error))
        fingerprints[identity] = canonical
    expected, covered = {}, {}
    for capture, invocation_ids in context['_capture_invocations'].items():
        for invocation in invocation_ids:
            expected.setdefault(invocation, set()).add(capture)
    for record in bound:
        binding = record.output['source_record_binding']
        covered.setdefault(binding['parent_invocation_id'], set()).add(binding['capture_id'])
    fully_bound = {invocation for invocation, captures in expected.items()
                   if captures and captures <= covered.get(invocation, set())}
    return bound, fingerprints, fully_bound


def source_review_documents(tool_results, *, max_chars=5000):
    """Share complete captured documents within the review's bounded payload.

    Stored tool results remain unchanged. A document is moved only in full,
    with its exact source and snapshot; omitted documents keep their original
    quoted evidence and never become misleading page-wide coverage.
    """
    from copy import deepcopy
    from nullion.tools import ToolResult
    documents, projected = {}, []
    for result in tool_results:
        output = getattr(result, 'output', None)
        if not isinstance(output, Mapping) or output.get('result_kind') != 'source_observations':
            projected.append(result)
            continue
        output = compact_source_observations(deepcopy(output), max_chars=max(12_000,
            2 * len(json.dumps(output, ensure_ascii=False, default=str)) + 1000))
        sources = output.get('sources') or []
        observations = output.get('observations') or []
        for index, source in enumerate(sources):
            if not isinstance(source, dict):
                continue
            rows = [row for row in observations if isinstance(row, dict) and row.get('source_index') == index]
            text = source.get('main_document_text')
            if not isinstance(text, str) or not text:
                text = next((row.get('quote') for row in rows if row.get('covers_main_document') is True), None)
            url = source.get('source_url') or (output.get('page_url') if source.get('page_url_ref') else None)
            captured_at = source.get('captured_at')
            if not isinstance(text, str) or not text or not url or captured_at is None:
                continue
            identity = hashlib.sha256(json.dumps([url, captured_at, text], ensure_ascii=False).encode()).hexdigest()[:24]
            document = {'source_url': url, 'captured_at': captured_at, 'scope': 'main_document', 'text': text}
            if identity not in documents:
                if len(json.dumps({**documents, identity: document}, ensure_ascii=False)) > max_chars:
                    continue
                documents[identity] = document
            source.pop('main_document_text', None)
            source['source_document_id'] = identity
            for row in rows:
                if row.get('covers_main_document') is True and row.get('quote') == text:
                    value = row.get('value')
                    anchor = value if isinstance(value, str) and value and value != text and value in text else next((line.strip() for line in text.splitlines() if line.strip()), text)
                    row['value'] = anchor
                    row['quote'] = anchor
                    row.pop('covers_main_document', None)
        projected.append(ToolResult(result.invocation_id, result.tool_name, result.status, output, result.error))
    return projected, documents


def source_answer_tool_definition(context: dict) -> dict:
    ids = [record['id'] for record in context['evidence']]
    unknowns = [record['id'] for record in context['limitations']]
    excerpts = [excerpt['id'] for record in context['evidence'] for excerpt in record['excerpts']]
    return {'name': SOURCE_ANSWER_TOOL, 'description':
        'Compose a focused answer to the original request from the verified evidence. '
        'Select only relevant findings; combine supporting details into the same point. '
        'Do not enumerate the evidence inventory or repeat the same item under different labels. '
        'Prioritize the specific emphasis or constraint over broad category matches; do not add unrelated items as padding. '
        'Choose enough points to answer the requested scope and comparison sides. Each point names one entity. '
        'Combine its separate title, price or other fields by selecting evidence_ids with a common capture_id. '
        'Use explicit title_ids and detail_groups for such a group; every selected excerpt must share that capture. '
        'A shared page alone does not prove that different entities are the same: retain the actual subject/field relationship. '
        'Prefer the relevant latest capture for changing facts; never assemble fields from different snapshots. '
        'Explain useful distinctions with '
        'supporting excerpts selected by their detail_ids; never invent claims, rankings, or applicability. '
        'For multiline records, use title_ids to select a meaningful name and identifier instead of a bare number. '
        'Use detail_groups to keep each displayed label, value, unit, currency and time suffix together. '
        'Include the relevant complete fields rather than selecting three disconnected values. '
        'The intro may repeat user-supplied dates, routes or constraints. For an evidence-derived conclusion, select '
        'intro_evidence_ids naming every supporting record; that conclusion requires independent outcome review. '
        'A field not shown in a complete main-document quote can be reported as not shown in that captured page. '
        'capture_documents contains complete captured main-document text keyed by capture_id; '
        'whole_document_capture_ids also identifies complete quotes. Use only documents bound to the selected evidence capture_ids. '
        'A short excerpt or blocked page cannot establish absence. '
        'Do not extend a page-scoped absence to an entire website, hidden frames, availability, or future outcomes. '
        'Do not put these conclusions in limitations as if the check failed. Without intro_evidence_ids, the intro '
        'must not add factual claims. Select only limitations that '
        'materially affect the answer, including the concrete access or evidence blocker; do not append every unavailable field. '
        'Earlier unknowns may be resolved by later verified observations; omit resolved limitations rather than repeating stale warnings. '
        'When no finding answers the request, select zero points and at least one limitation. Metadata alone '
        'does not establish substantive claims. Do not pad an incomplete answer with irrelevant metadata. This composes a draft; '
        'it does not certify that required source checks or requested actions completed.',
        'input_schema': {'type': 'object', 'additionalProperties': False,
            'properties': {'intro': {'type': 'string', 'maxLength': 350},
                'intro_evidence_ids': {'type': 'array', 'minItems': 1, 'maxItems': 8, 'uniqueItems': True,
                                      'items': {'type': 'string', **({'enum': ids} if ids else {})}},
                'points': {'type': 'array', 'minItems': 0, 'maxItems': 20, 'items': {
                    'type': 'object', 'additionalProperties': False,
                    'properties': {'evidence_ids': {'type': 'array', 'minItems': 1, 'maxItems': 8,
                                                  'uniqueItems': True, 'items': {'type': 'string', **({'enum': ids} if ids else {})}},
                        'title_ids': {'type': 'array', 'minItems': 1, 'maxItems': 3, 'uniqueItems': True,
                                       'items': {'type': 'string'}},
                        'detail_ids': {'type': 'array', 'maxItems': 24, 'uniqueItems': True,
                                       'items': {'type': 'string', **({'enum': excerpts} if excerpts else {})}},
                        'detail_groups': {'type': 'array', 'maxItems': 8, 'items': {
                            'type': 'array', 'minItems': 1, 'maxItems': 8, 'uniqueItems': True,
                            'items': {'type': 'string'}}},
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
    if (not isinstance(presentation, Mapping) or not {'intro', 'points', 'limitation_ids'} <= set(presentation)
            or set(presentation) - {'intro', 'points', 'limitation_ids', 'intro_evidence_ids'}):
        raise ValueError('Expected a structured source answer')
    intro, points, limitations = (presentation[key] for key in ('intro', 'points', 'limitation_ids'))
    if not isinstance(intro, str) or len(intro) > 350 or not isinstance(points, list) or not 0 <= len(points) <= 20:
        raise ValueError('Invalid source answer length')
    intro_ids = presentation.get('intro_evidence_ids', [])
    if ('intro_evidence_ids' in presentation and (not isinstance(intro_ids, list) or not 1 <= len(intro_ids) <= 8
            or any(not isinstance(i, str) or i not in records for i in intro_ids)
            or len(set(intro_ids)) != len(intro_ids) or not intro.strip())):
        raise ValueError('Unknown introductory evidence identity')
    if not isinstance(limitations, list) or len(limitations) > 3 or any(not isinstance(i, str) or i not in unknowns for i in limitations):
        raise ValueError('Unknown limitation identity')
    if not points and not limitations and not intro_ids:
        raise ValueError('A source answer needs a finding or a specific limitation')
    if context['report_status'] != 'completed' and terminal_source_report_tools(tool_results) and unknowns and not limitations and not intro_ids:
        raise ValueError('A terminal incomplete report must explain its limitation')
    blocks, titles = [], set()
    for point in points:
        if (not isinstance(point, Mapping)
                or not {'evidence_ids', 'relevance'}.issubset(point)
                or set(point) - {'evidence_ids', 'detail', 'detail_ids', 'title_ids', 'detail_groups', 'relevance'}
                or ('detail' in point) == ('detail_ids' in point)):
            raise ValueError('Invalid source answer point')
        ids, relevance = (point[key] for key in ('evidence_ids', 'relevance'))
        if not isinstance(ids, list) or not 1 <= len(ids) <= 8 or any(not isinstance(i, str) or i not in records for i in ids):
            raise ValueError('Unknown evidence identity')
        if len(set(ids)) != len(ids) or not isinstance(relevance, str) or not relevance.strip() or len(relevance) > 250:
            raise ValueError('Missing relevance decision')
        common_capture = set.intersection(*(set(records[i]['capture_ids']) for i in ids))
        if len(ids) > 1 and (not common_capture or 'title_ids' not in point or 'detail_ids' not in point):
            raise ValueError('Grouped fields require one captured source and explicit title/field identities')
        if 'detail_ids' in point:
            selected = point['detail_ids']
            available = {excerpt['id']: excerpt['text'] for identity in ids for excerpt in records[identity]['excerpts']}
            if not isinstance(selected, list) or len(selected) > 24 or any(not isinstance(i, str) or i not in available for i in selected) or len(set(selected)) != len(selected):
                raise ValueError('Unknown supporting excerpt identity')
            if len(ids) > 1 or common_capture:
                title_ids = point.get('title_ids', [])
                if not isinstance(title_ids, list) or any(not isinstance(i, str) or i not in available for i in title_ids):
                    raise ValueError('Unknown title excerpt identity')
                excerpts = {excerpt['id']: excerpt for identity in ids for excerpt in records[identity]['excerpts']}
                for identity in selected + title_ids:
                    common_capture.intersection_update(excerpts[identity]['capture_ids'])
                if not common_capture:
                    raise ValueError('Grouped excerpts must come from the same capture')
            groups = point.get('detail_groups')
            if groups is not None:
                if (not isinstance(groups, list) or len(groups) > 8
                        or any(not isinstance(group, list) or not 1 <= len(group) <= 8
                               or any(not isinstance(i, str) or i not in available for i in group)
                               or len(set(group)) != len(group) for group in groups)):
                    raise ValueError('Invalid supporting excerpt groups')
                grouped = [i for group in groups for i in group]
                if len({tuple(group) for group in groups}) != len(groups) or set(grouped) != set(selected):
                    raise ValueError('Supporting groups must retain every selected field without duplicate groups')
                rendered_groups = [' '.join(available[i] for i in group) for group in groups]
                if any(not any(' '.join(group.split()) in ' '.join(quote.split())
                               and (len(ids) == 1 or common_capture.intersection(records[identity]['quote_capture_ids'][qindex]))
                               for identity in ids for qindex, quote in enumerate(records[identity]['quotes'])) for group in rendered_groups):
                    raise ValueError('Supporting field group is not a quoted relationship')
                detail = ' · '.join(rendered_groups)
            else:
                detail = ' '.join(available[i] for i in selected)
        else:
            if 'detail_groups' in point or 'title_ids' in point:
                raise ValueError('Excerpt layout requires excerpt identities')
            detail = point['detail']
        if not isinstance(detail, str) or len(detail) > 650:
            raise ValueError('Invalid supporting excerpt')
        if 'detail_ids' not in point and detail.strip() and not any(' '.join(detail.split()) in ' '.join(quote.split()) for i in ids for quote in records[i]['quotes']):
            raise ValueError('Supporting excerpt is not verified')
        primary = records[ids[0]]
        title = _readable_source_line(primary['value'].splitlines()[0])
        if 'title_ids' in point:
            title_ids = point['title_ids']
            if (not isinstance(title_ids, list) or not 1 <= len(title_ids) <= 3
                    or any(not isinstance(i, str) or i not in available for i in title_ids)
                    or len(set(title_ids)) != len(title_ids)):
                raise ValueError('Unknown title excerpt identity')
            title = _readable_source_line(' '.join(available[i] for i in title_ids))
        identity = (title, tuple(sorted(common_capture))) if 'title_ids' in point else primary['id']
        if identity in titles:
            raise ValueError('Repeated answer item')
        titles.add(identity)
        body = _readable_source_line(detail.strip())
        if body == title:
            body = ''
        urls = list(dict.fromkeys(url for i in ids for url in records[i]['urls']))
        citation = ' ' + ' · '.join(f'[Source {index + 1}]({url})' for index, url in enumerate(urls[:2])) if urls else ''
        blocks.append(f'- **{title}**' + (f' — {body}' if body else '') + citation)
    notes = list(dict.fromkeys(unknowns[i] for i in limitations))
    limitation_text = '\n\n**Not verified from this check:** ' + '; '.join(notes) if notes else ''
    intro_urls = list(dict.fromkeys(url for i in intro_ids for url in records[i]['urls']))
    intro_citations = ' ' + ' · '.join(f'[Source {index + 1}]({url})' for index, url in enumerate(intro_urls[:2])) if intro_urls else ''
    return (intro.strip() + intro_citations + '\n\n' if intro.strip() else '') + '\n\n'.join(blocks) + limitation_text


def partial_source_report_reply(tool_results, *, draft=None) -> str:
    """A source inventory is never a relevance-aware fallback answer."""
    try:
        return render_source_answer(tool_results, draft)
    except ValueError:
        context = source_answer_context(tool_results)
        if terminal_source_report_tools(tool_results) and any(row['kind'] == 'blocker' for row in context['limitations']):
            # This fallback selects no factual inventory. A broken synthesis
            # can still explain the producer's bounded access/evidence failure.
            limitations = sorted(context['limitations'], key=lambda row: row['kind'] != 'blocker')
            return render_source_answer(tool_results, {'intro': "I couldn't complete the requested checks.",
                'points': [], 'limitation_ids': [row['id'] for row in limitations[:2]]})
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
        elif (pending and output.get('report_status') == 'completed' and not output.get('blocker')
              and _source_observation_identities(output)):
            # Search forms can show different verified results at the same
            # canonical URL. A completed observed lookup resolves continuation;
            # navigation or an unchanged partial page alone still cannot.
            pending = None
        elif pending and output.get('page_url') and output.get('page_url') != pending.get('source_url'):
            pending = None
    return pending
