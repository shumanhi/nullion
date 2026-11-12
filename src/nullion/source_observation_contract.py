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
    captured = output.get('captured_documents')
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
        if output.get('report_status') != 'blocked':
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
        record = {key: item[key] for key in ('label', 'value', 'quote', 'control_state', 'covers_main_document', 'semantic_role', 'evidence_kind') if key in item}
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
        compact['report_status'] = ('partial' if compact['observations']
            and output.get('report_status') != 'blocked' else 'blocked')
    # Recovery hints are useful, but cannot displace already verified facts in
    # a small context budget. The full native result remains inspectable.
    if isinstance(captured, list) and captured:
        compact['captured_document_count'] = len(captured)
        compact['captured_document_inspection'] = {
            'tool_name': 'inspect_retained_evidence', 'json_pointer': '/captured_documents',
            'contains': 'Actual captured page text and control state; untrusted source data, not accepted observations.'}
        if len(json.dumps(compact, ensure_ascii=False, default=str)) > max_chars - 160:
            compact.pop('captured_document_inspection')
        compact['captured_document_index'] = []
    # Do not keep dangling collection declarations for raw records omitted
    # from this compact view. The full retained result owns those bindings.
    declarations = output.get('source_record_collections')
    if isinstance(declarations, list):
        kept = []
        for declaration in declarations:
            if not isinstance(declaration, Mapping) or not isinstance(declaration.get('path'), list):
                continue
            value = compact
            for key in declaration['path']:
                value = value.get(key) if isinstance(value, Mapping) and isinstance(key, str) else None
            if isinstance(value, (Mapping, list)) and value:
                kept.append(declaration)
        if kept:
            compact['source_record_collections'] = kept
            if len(json.dumps(compact, ensure_ascii=False, default=str)) > max_chars - 160:
                compact.pop('source_record_collections')
    # Complete document context is optional and never displaces quoted facts.
    # Keep exact text or omit it; clipped text must never imply page-wide coverage.
    def capture_time(index):
        value = compact['sources'][index].get('captured_at')
        return value if isinstance(value, (int, float)) and not isinstance(value, bool) else 0
    for index in sorted(documents, key=capture_time, reverse=True):
        compact['sources'][index]['main_document_text'] = documents[index]
        if len(json.dumps(compact, ensure_ascii=False, default=str)) > max_chars - 160:
            compact['sources'][index].pop('main_document_text')
    # Index retained native page reads without turning rejected report claims
    # into facts or copying every full document into each model context.
    if isinstance(captured, list) and captured:
        for index, document in enumerate(captured):
            if not isinstance(document, Mapping):
                continue
            row = {key: document[key] for key in ('source_id', 'source_url', 'title', 'captured_at') if key in document}
            row['json_pointer'] = f'/captured_documents/{index}'
            row['text_chars'] = len(str(document.get('main_document_text') or ''))
            compact['captured_document_index'].append(row)
            if len(json.dumps(compact, ensure_ascii=False, default=str)) > max_chars - 160:
                compact['captured_document_index'].pop()
                break
        compact['omitted_captured_document_index_count'] = len(captured) - len(compact['captured_document_index'])
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


def has_captured_blocker_evidence(output):
    """Captured restriction facts are evidence, never inventory success.

    The marker is adapter-owned and survives bounded review projections. A bare
    error string, navigation receipt, or uncaptured explanation cannot qualify.
    """
    if not isinstance(output, Mapping) or output.get('result_kind') != 'source_observations':
        return False
    sources = output.get('sources')
    sources = sources if isinstance(sources, (list, tuple)) else []
    observations = output.get('observations')
    if not isinstance(observations, (list, tuple)):
        return False
    for row in observations:
        if not isinstance(row, Mapping) or row.get('evidence_kind') != 'source_blocker':
            continue
        value, quote = row.get('value'), row.get('quote')
        if (not isinstance(value, str) or not value.strip() or not isinstance(quote, str)
                or ' '.join(value.split()) not in ' '.join(quote.split())):
            continue
        index = row.get('source_index')
        source = sources[index] if type(index) is int and 0 <= index < len(sources) else row
        if not isinstance(source, Mapping):
            continue
        url = output.get('page_url') if source.get('page_url_ref') else source.get('source_url')
        if (isinstance(url, str) and url.startswith(('https://', 'http://'))
                and type(source.get('captured_at')) in (int, float)):
            return True
    return False


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
SOURCE_ANSWER_MAX_DETAIL_CHARS = 650
SOURCE_SEMANTIC_ROLES = ('entity', 'attribute', 'interface_control', 'context')


def source_answer_context(tool_results, *, include_bindings=False) -> dict:
    """Verified, deduplicated evidence; relevance selection belongs to the model."""
    from nullion.tools import normalize_tool_status
    records, identities, capture_times, capture_documents = [], {}, {}, {}
    capture_invocations, capture_sources, capture_controls = {}, {}, {}
    for result_index, result in enumerate(tool_results or ()):
        output = getattr(result, 'output', None)
        if normalize_tool_status(getattr(result, 'status', None)) != 'completed' or not isinstance(output, Mapping):
            continue
        if output.get('result_kind') != 'source_observations' or output.get('report_status') not in {'completed', 'partial', 'blocked'}:
            continue
        for row_index, row in enumerate(output.get('observations') or ()):
            if not isinstance(row, Mapping) or not isinstance(row.get('value'), str) or not isinstance(row.get('quote'), str):
                continue
            if output.get('report_status') == 'blocked' and row.get('evidence_kind') != 'source_blocker':
                continue
            value, quote = row['value'].strip(), row['quote'].strip()
            if not value or ' '.join(value.split()) not in ' '.join(quote.split()):
                continue
            role = row.get('semantic_role') if row.get('semantic_role') in SOURCE_SEMANTIC_ROLES else None
            # The same visible words can name a real subject and a UI section.
            # Keep those typed observations separate rather than merging roles.
            identity = (unicodedata.normalize('NFKC', ' '.join(value.split())).casefold(), role, row.get('evidence_kind'))
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
            semantic_binding = {'role': row['semantic_role'], 'capture_id': capture} if row.get('semantic_role') in SOURCE_SEMANTIC_ROLES else None
            if identity in identities:
                record = identities[identity]
                if semantic_binding and semantic_binding not in record['semantic_bindings']:
                    record['semantic_bindings'].append(semantic_binding)
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
                      'semantic_bindings': [semantic_binding] if semantic_binding else [],
                      'capture_ids': [capture] if capture else [],
                      'quote_capture_ids': [[capture] if capture else []],
                      'whole_document_capture_ids': [[capture] if capture and row.get('covers_main_document') is True else []],
                      'urls': [url] if isinstance(url, str) and url.startswith(('https://', 'http://')) else []}
            if row.get('evidence_kind') == 'source_blocker':
                record['evidence_kind'] = 'source_blocker'
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
    result = {'rendering_limits': {'max_detail_chars_per_point': SOURCE_ANSWER_MAX_DETAIL_CHARS},
            'report_status': source_report_status(tool_results or ()),
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
    presentation = normalize_source_answer_selection(presentation)
    try:
        render_source_answer(results, presentation)
    except (ValueError, TypeError):
        return [], {}, set()
    context = source_answer_context(results, include_bindings=True)
    records = {row['id']: row for row in context['evidence']}
    parents = {str(getattr(row, 'invocation_id', '')): row for row in results if getattr(row, 'invocation_id', None)}
    bound, fingerprints = [], {}
    for point in presentation['points']:
        if point.get('request_relation') == 'supporting_context':
            continue
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
            if row.get('evidence_kind') == 'source_blocker':
                observation['evidence_kind'] = 'source_blocker'
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


def source_capture_review_records(tool_results, *, max_chars=30_000, max_records=50, diagnostics=None):
    """Expose literal capture records independently of a chat presentation.

    Generated reports need the same distinct-subject proof as narrative answers.
    These references bind exact facts to real captures; only the semantic reviewer
    may decide which requested subject a fact proves. No text intent is inferred.
    """
    from nullion.tools import ToolResult
    results = list(tool_results)
    context = source_answer_context(results, include_bindings=True)
    parents = {str(result.invocation_id): result for result in results if result.invocation_id}
    candidates = []
    for row in context['evidence']:
        captures = [capture for capture in row['capture_ids']
            if len(context['_capture_invocations'].get(capture, ())) == 1]
        if not captures:
            continue
        capture = max(captures, key=lambda key: context['capture_times'][key])
        parent_id = next(iter(context['_capture_invocations'][capture]))
        parent = parents.get(parent_id)
        source = context['_capture_sources'][capture]
        if parent is None or not isinstance(parent.output, Mapping):
            continue
        quote_index = next(index for index, captures in enumerate(row['quote_capture_ids']) if capture in captures)
        quote = row['quotes'][quote_index]
        canonical = json.dumps({'value': row['value'], 'quote': quote, 'source_url': source['source_url'],
            'controls': context['_capture_controls'].get(capture, {}).get(' '.join(quote.split()), ())},
            ensure_ascii=False, sort_keys=True)
        identity = parent_id + '/captured-record/' + hashlib.sha256((capture + canonical).encode()).hexdigest()[:20]
        observation = {'label': row['label'], 'value': row['value'], 'quote': quote, 'source_index': 0}
        if row.get('evidence_kind') == 'source_blocker':
            observation['evidence_kind'] = 'source_blocker'
        roles = {binding['role'] for binding in row['semantic_bindings'] if binding['capture_id'] == capture}
        if len(roles) == 1:
            observation['semantic_role'] = next(iter(roles))
        output = {key: parent.output[key] for key in ('result_kind', 'report_status', 'goal_status', 'completion_scope') if key in parent.output}
        controls = context['_capture_controls'].get(capture, {}).get(' '.join(quote.split()), ())
        observations = [{**observation, 'control_state': control} for control in controls] if controls else [observation]
        if capture in row['whole_document_capture_ids'][quote_index]:
            for item in observations:
                item['covers_main_document'] = True
        output.update(observations=observations, sources=[{'source_url': source['source_url'],
            'captured_at': context['capture_times'][capture]}], source_record_binding={
            'record_kind': 'source_observation', 'parent_invocation_id': parent_id,
            'capture_id': capture, 'title': row['value'].splitlines()[0], 'fields': [row['value']]})
        audit = parent.output.get('execution_audit')
        if isinstance(audit, Mapping):
            output['execution_audit'] = {key: audit[key] for key in ('version', 'complete', 'read_only_actions_only',
                'downloaded_file_count', 'host_file_actions_enabled', 'captures_are_internal', 'deliverable_file_count') if key in audit}
        record_size = len(json.dumps(output, ensure_ascii=False))
        candidates.append((ToolResult(identity, parent.tool_name, parent.status, output, parent.error),
            canonical, record_size, ("capture", source['source_url'])))
    # Adapters may declare typed row collections from their own output schema.
    # Bind exact rows, without interpreting field names or parsing result prose.
    for parent in results:
        if parent.status != 'completed' or parent.error or not isinstance(parent.output, Mapping):
            continue
        declarations = parent.output.get('source_record_collections')
        if not isinstance(declarations, list):
            continue
        for declaration in declarations[:10]:
            if not isinstance(declaration, Mapping):
                continue
            path = declaration.get('path')
            identities = declaration.get('identity_fields')
            if (not isinstance(path,list) or not 1 <= len(path) <= 4
                    or not all(isinstance(key,str) and key for key in path)
                    or not isinstance(identities,list) or not identities
                    or not all(isinstance(key,str) and key for key in identities)):
                continue
            rows = parent.output
            for key in path:
                rows = rows.get(key) if isinstance(rows,Mapping) else None
            cardinality = declaration.get('cardinality', 'many')
            if cardinality == 'one' and isinstance(rows, Mapping):
                rows = [rows]
            if cardinality not in {'one', 'many'} or not isinstance(rows,list):
                continue
            for index, record in enumerate(rows[:max_records]):
                if not isinstance(record,Mapping) or not all(
                        isinstance(record.get(key),(str,int,float)) and not isinstance(record.get(key),bool)
                        and str(record[key]).strip() for key in identities):
                    continue
                try:
                    canonical = json.dumps(dict(record),ensure_ascii=False,sort_keys=True,allow_nan=False)
                except (TypeError,ValueError):
                    continue
                identity = str(parent.invocation_id)+'/structured-record/'+hashlib.sha256(
                    (json.dumps(path)+canonical).encode()).hexdigest()[:20]
                output = {'result_kind':'structured_source_record','record':dict(record),
                    'source_record_binding':{'record_kind':'structured_source_record',
                        'parent_invocation_id':str(parent.invocation_id),'collection_path':path,'row_index':index}}
                record_size = len(json.dumps(output,ensure_ascii=False))
                candidates.append((ToolResult(identity,parent.tool_name,parent.status,output,parent.error),
                    canonical,record_size,("collection",str(parent.invocation_id),tuple(path))))
    # The file adapter declares machine-readable JSON, so bind literal document
    # records without another file read or inferring subjects from prose/keys.
    # Source data cannot declare its own proof metadata: wrappers are built here.
    for parent in results:
        output = parent.output
        if (parent.status != 'completed' or parent.error or not isinstance(output, Mapping)
                or output.get('source_document_format') != 'json' or output.get('truncated')
                or not isinstance(output.get('content'), str) or len(output['content']) > 1_000_000):
            continue
        try:
            document = json.loads(output['content'])
        except (ValueError, RecursionError):
            continue
        pending = [(document, '', 0)]
        visited = 0
        while pending and visited < max_records * 8:
            value, pointer, depth = pending.pop(0)
            visited += 1
            if isinstance(value, dict):
                try:
                    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
                except (TypeError, ValueError):
                    continue
                identity = str(parent.invocation_id) + '/document-record/' + hashlib.sha256(
                    (pointer + canonical).encode()).hexdigest()[:20]
                record_output = {'result_kind': 'structured_source_record', 'record': value,
                    'source_record_binding': {'record_kind': 'structured_source_record',
                        'parent_invocation_id': str(parent.invocation_id),
                        'collection_path': ['content'], 'json_pointer': pointer, 'document_format': 'json'}}
                candidates.append((ToolResult(identity, parent.tool_name, parent.status, record_output),
                    canonical, len(json.dumps(record_output, ensure_ascii=False)),
                    ('document', str(parent.invocation_id), pointer.rsplit('/', 1)[0])))
            if depth >= 4:
                continue
            children = value.items() if isinstance(value, dict) else enumerate(value) if isinstance(value, list) else ()
            from itertools import islice
            for key, child in islice(children, max_records):
                if isinstance(child, (dict, list)):
                    escaped = str(key).replace('~', '~0').replace('/', '~1')
                    pending.append((child, pointer + '/' + escaped, depth + 1))
    # Reserve a share for each independently captured source/typed collection.
    # First-invocation order must not erase every record of a later subject.
    # Keep quotations whole; unused capacity is then shared without enlarging
    # either the byte budget or the maximum record count.
    groups = {}
    for candidate in candidates:
        groups.setdefault(candidate[3], []).append(candidate)
    queues = {key:list(rows) for key,rows in groups.items()}
    used = dict.fromkeys(groups, 0)
    bound, fingerprints, size = [], {}, 0
    quota = max(0, max_chars) // max(1, len(groups))
    for reserved in (True, False):
        while len(bound) < max_records:
            progressed = False
            for key, queue in queues.items():
                allowance = min(max_chars - size, quota - used[key]) if reserved else max_chars - size
                index = next((i for i,row in enumerate(queue) if row[2] <= allowance), None)
                if index is None:
                    continue
                record, canonical, record_size, _ = queue.pop(index)
                bound.append(record);fingerprints[record.invocation_id] = canonical
                size += record_size;used[key] += record_size;progressed = True
                if len(bound) >= max_records:
                    break
            if not progressed:
                break
    if isinstance(diagnostics, dict):
        diagnostics.update(max_chars=max_chars, max_records=max_records, selected_chars=size,
            candidate_records=len(candidates), selected_records=len(bound), source_groups=len(groups),
            represented_source_groups=sum(value > 0 for value in used.values()),
            omitted_records=len(candidates)-len(bound), budget_limited=len(bound)<len(candidates))
    return bound, fingerprints


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


def source_answer_tool_definition(context: dict, *, require_semantics: bool = False) -> dict:
    ids = [record['id'] for record in context['evidence']]
    unknowns = [record['id'] for record in context['limitations']]
    excerpts = [excerpt['id'] for record in context['evidence'] for excerpt in record['excerpts']]
    definition = {'name': SOURCE_ANSWER_TOOL, 'description':
        'Compose a focused answer to the original request from the verified evidence. '
        'Select only relevant findings; combine supporting details into the same point. '
        'Declare answer_status from the original request, not whether any page loaded. '
        'Use no_verified_results when only background film/product titles, nearby venues or page controls were captured '
        'and no requested option was established. Classify those points as supporting_context; they are not results. '
        'Write each result summary in concise natural language using only its selected evidence. '
        'The final answer is a useful explanation, not a concatenation of page fragments or selector labels. '
        'Determine focus_roles from the original request: entity means a requested real-world subject, '
        'attribute means a requested property, interface_control means a requested page action/control, and context is background. '
        'Assign each point a semantic_role. A page section, input, or purchase action is not a separate real-world subject. '
        'Attach properties and controls as details of their subject when useful; list them separately only when the request asks about that role. '
        'Check semantic_bindings and complete captured documents before selecting a title. '
        'A name captured as an attribute can identify its entity when you also select the entity record '
        'containing that name at the same capture. Preserve that relationship instead of omitting an answered subject. '
        'When the intro fully answers a point, put its index in summary_point_indices to avoid repeating that point as a bullet. '
        'Do not enumerate the evidence inventory or repeat the same item under different labels. '
        'Prioritize the specific emphasis or constraint over broad category matches; do not add unrelated items as padding. '
        'Choose enough points to answer the requested scope and comparison sides. Each point names one requested subject. '
        'Combine its separate title, price or other fields by selecting evidence_ids with a common capture_id. '
        'Use explicit title_ids and detail_groups for such a group; every selected excerpt must share that capture. '
        'A shared page alone does not prove that different entities are the same: retain the actual subject/field relationship. '
        'Prefer the relevant latest capture for changing facts; never assemble fields from different snapshots. '
        'Explain useful distinctions with '
        'supporting excerpts selected by their detail_ids; never invent claims, rankings, or applicability. '
        'For multiline records, use title_ids to select a meaningful name and identifier instead of a bare number. '
        'Each detail_group must be contiguous quoted source text: split nonadjacent fields into separate groups. '
        'Every detail_id must appear in detail_groups when groups are provided. Groups select exact excerpt identities; '
        'the runtime derives their complete flat detail_ids index. Omit unrelated source fields from both. '
        'Use detail_groups to keep each displayed label, value, unit, currency and time suffix together. '
        'Include the relevant complete fields rather than selecting three disconnected values. '
        f'Aim for {SOURCE_ANSWER_MAX_DETAIL_CHARS} characters per point; this is presentation guidance, not an evidence-validity limit. Select concise complete fields instead of full source paragraphs. '
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
                'intro_evidence_ids': {'type': 'array', 'minItems': 0, 'maxItems': 8, 'uniqueItems': True,
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
                            'items': {'type': 'string', **({'enum': excerpts} if excerpts else {})}}},
                        'relevance': {'type': 'string', 'minLength': 1, 'maxLength': 250}},
                    'required': ['evidence_ids', 'detail_ids', 'relevance']}},
                'limitation_ids': {'type': 'array', 'maxItems': 3, 'uniqueItems': True,
                                   'items': {'type': 'string', **({'enum': unknowns} if unknowns else {})}}},
            'required': ['intro', 'points', 'limitation_ids']}}

    properties = definition['input_schema']['properties']
    properties['answer_status'] = {'type': 'string', 'enum': ['verified_results', 'partial_results', 'no_verified_results']}
    properties['limitation_summaries'] = {'type': 'object',
        'description': 'Concise user-facing explanations keyed by selected limitation id. Explain the actual gap; '
            'do not paste browser investigation notes or repeat the whole request.',
        'properties': {identity: {'type': 'string', 'minLength': 1, 'maxLength': 350} for identity in unknowns},
        'additionalProperties': False}
    properties['focus_roles'] = {'type': 'array', 'minItems': 1, 'maxItems': 4, 'uniqueItems': True,
        'items': {'type': 'string', 'enum': list(SOURCE_SEMANTIC_ROLES)}}
    properties['summary_point_indices'] = {'type': 'array', 'maxItems': 20, 'uniqueItems': True,
        'items': {'type': 'integer', 'minimum': 0, 'maximum': 19}}
    point_schema = properties['points']['items']
    point_schema['anyOf'] = [
        {'properties': {'evidence_ids': {'maxItems': 1}}},
        {'properties': {'evidence_ids': {'minItems': 2}}, 'required': ['title_ids', 'detail_ids']},
    ]
    point_schema['properties']['semantic_role'] = {'type': 'string', 'enum': list(SOURCE_SEMANTIC_ROLES)}
    point_schema['properties']['request_relation'] = {'type': 'string', 'enum': ['result', 'supporting_context']}
    point_schema['properties']['summary'] = {'type': 'string', 'maxLength': 600}
    if require_semantics:
        definition['input_schema']['required'].append('focus_roles')
        definition['input_schema']['required'].append('answer_status')
        point_schema['required'].append('semantic_role')
        point_schema['required'].append('request_relation')
        point_schema['required'].append('summary')
    return definition


def normalize_source_answer_selection(presentation, *, previous_selection=None):
    """Canonicalize the redundant flat index from explicitly selected groups.

    Groups already select exact excerpt identities. Expanding their index adds
    no source claims; literal text, capture, role and review checks still run.
    Missing declared selections remain a conflict requiring model repair.
    """
    if not isinstance(presentation, Mapping) or not isinstance(presentation.get('points'), list):
        return presentation
    points = []
    for point in presentation['points']:
        if not isinstance(point, Mapping):
            points.append(point)
            continue
        copied = dict(point)
        # Preserve an explicit earlier title when a cosmetic revision drops
        # just that field for the exact same evidence set. Ambiguous prior
        # subjects and explicit replacements are never inferred or overwritten.
        ids = point.get('evidence_ids')
        prior_points = previous_selection.get('points') if isinstance(previous_selection, Mapping) else None
        if ('title_ids' not in point and isinstance(ids, list) and len(ids) > 1
                and all(isinstance(identity, str) for identity in ids) and isinstance(prior_points, list)):
            matches = [old for old in prior_points if isinstance(old, Mapping)
                and isinstance(old.get('evidence_ids'), list)
                and all(isinstance(identity, str) for identity in old['evidence_ids'])
                and set(old['evidence_ids']) == set(ids)]
            if len(matches) == 1:
                titles = matches[0].get('title_ids')
                if (isinstance(titles, list) and 1 <= len(titles) <= 3
                        and all(isinstance(identity, str) for identity in titles)
                        and len(set(titles)) == len(titles)):
                    copied['title_ids'] = list(titles)
        groups, selected = point.get('detail_groups'), point.get('detail_ids')
        if (isinstance(groups, list) and len(groups) <= 8
                and all(isinstance(group, list) and 1 <= len(group) <= 8
                    and all(isinstance(identity, str) for identity in group) for group in groups)
                and isinstance(selected, list) and len(selected) <= 24
                and all(isinstance(identity, str) for identity in selected) and len(set(selected)) == len(selected)):
            grouped = list(dict.fromkeys(identity for group in groups for identity in group))
            if len(grouped) <= 24 and set(selected) <= set(grouped):
                copied['detail_ids'] = grouped
        points.append(copied)
    return {**presentation, 'points': points}


def render_source_answer(tool_results, presentation: object, *, require_semantics: bool = False) -> str:
    """Validate selected identities and literal supporting excerpts before delivery."""
    return _render_source_answer(tool_results, presentation, source_answer_context(tool_results, include_bindings=True),
        require_semantics=require_semantics, message_kind='report')


def _selected_source_urls(context, records, ids, captures):
    sources = context.get('_capture_sources') or {}
    if captures and all(capture in sources for capture in captures):
        # Match the actual record binding used by outcome review, rather than
        # every URL of a deduplicated value such as a shared price or policy.
        capture = max(captures, key=lambda value: context['capture_times'][value])
        return [sources[capture]['source_url']]
    return list(dict.fromkeys(url for identity in ids for url in records[identity]['urls']))


def _render_source_answer(tool_results, presentation, context, *, require_semantics=False, item_identities=None,
                          citation_numbers=None, message_kind=None):
    from nullion.chat_text import ReplyFinding, ReplySection, UserMessage, render_user_message
    presentation = normalize_source_answer_selection(presentation)
    records = {row['id']: row for row in context['evidence']}
    if citation_numbers is None:
        citation_numbers = {}
    unknowns = {row['id']: row['text'] for row in context['limitations']}
    if (not isinstance(presentation, Mapping) or not {'intro', 'points', 'limitation_ids'} <= set(presentation)
            or set(presentation) - {'intro', 'points', 'limitation_ids', 'intro_evidence_ids', 'focus_roles', 'summary_point_indices', 'answer_status', 'limitation_summaries'}):
        raise ValueError('Expected a structured source answer')
    intro, points, limitations = (presentation[key] for key in ('intro', 'points', 'limitation_ids'))
    answer_status = presentation.get('answer_status')
    if answer_status is not None and answer_status not in {'verified_results', 'partial_results', 'no_verified_results'}:
        raise ValueError('Invalid answer status')
    if not isinstance(intro, str) or len(intro) > 350 or not isinstance(points, list) or not 0 <= len(points) <= 20:
        raise ValueError('Invalid source answer length')
    intro_ids = presentation.get('intro_evidence_ids', [])
    if ('intro_evidence_ids' in presentation and (not isinstance(intro_ids, list) or len(intro_ids) > 8
            or any(not isinstance(i, str) or i not in records for i in intro_ids)
            or len(set(intro_ids)) != len(intro_ids) or (intro_ids and not intro.strip()))):
        raise ValueError('Unknown introductory evidence identity')
    if not isinstance(limitations, list) or len(limitations) > 3 or any(not isinstance(i, str) or i not in unknowns for i in limitations):
        raise ValueError('Unknown limitation identity')
    summaries = presentation.get('limitation_summaries', {})
    if (not isinstance(summaries, Mapping) or set(summaries) - set(limitations)
            or any(not isinstance(value, str) or not value.strip() or len(value) > 350 for value in summaries.values())):
        raise ValueError('Invalid authored limitation summary')
    if not points and not limitations and not intro_ids:
        raise ValueError('A source answer needs a finding or a specific limitation')
    if context['report_status'] != 'completed' and terminal_source_report_tools(tool_results) and unknowns and not limitations and not intro_ids and not presentation.get('focus_roles'):
        raise ValueError('A terminal incomplete report must explain its limitation')
    focus = presentation.get('focus_roles')
    if require_semantics or focus is not None:
        if (not isinstance(focus, list) or not focus or len(focus) > 4
                or any(not isinstance(role, str) or role not in SOURCE_SEMANTIC_ROLES for role in focus)
                or len(set(focus)) != len(focus)):
            raise ValueError('Declare the requested semantic roles before selecting answer items')
    summarized = presentation.get('summary_point_indices', [])
    if (not isinstance(summarized, list) or len(summarized) > len(points)
            or any(type(index) is not int or not 0 <= index < len(points) for index in summarized)
            or len(set(summarized)) != len(summarized)):
        raise ValueError('Unknown summarized answer point')
    blocks, rendered_records, selected_record_urls = [], {}, {}
    for point_index, point in enumerate(points):
        if (not isinstance(point, Mapping)
                or not {'evidence_ids', 'relevance'}.issubset(point)
                or set(point) - {'evidence_ids', 'detail', 'detail_ids', 'title_ids', 'detail_groups', 'relevance', 'semantic_role', 'request_relation', 'summary'}
                or ('detail' in point) == ('detail_ids' in point)):
            raise ValueError('Invalid source answer point')
        ids, relevance = (point[key] for key in ('evidence_ids', 'relevance'))
        if not isinstance(ids, list) or not 1 <= len(ids) <= 8 or any(not isinstance(i, str) or i not in records for i in ids):
            raise ValueError('Unknown evidence identity')
        if len(set(ids)) != len(ids) or not isinstance(relevance, str) or not relevance.strip() or len(relevance) > 250:
            raise ValueError('Missing relevance decision')
        role = point.get('semantic_role')
        relation = point.get('request_relation')
        summary = point.get('summary')
        if relation is not None and relation not in {'result', 'supporting_context'}:
            raise ValueError('Invalid request relation')
        if answer_status is not None and relation is None:
            raise ValueError('Declare whether each point answers the request or supplies background')
        if answer_status == 'no_verified_results' and relation == 'result':
            raise ValueError('A no-result answer cannot claim a verified candidate')
        if summary is not None and (not isinstance(summary, str) or len(summary) > 600):
            raise ValueError('Invalid authored finding summary')
        if require_semantics or focus is not None or role is not None:
            if role not in SOURCE_SEMANTIC_ROLES or not focus or role not in focus:
                raise ValueError('Standalone answer item must match a requested semantic role; merge supporting fields into their subject')
        if point_index in summarized and (not intro.strip() or not set(ids) <= set(intro_ids)):
            raise ValueError('The intro must cite every record of a summarized answer point: ' + json.dumps(
                {'point_index': point_index, 'missing_intro_evidence_ids': sorted(set(ids) - set(intro_ids))},
                separators=(',', ':')))
        common_capture = set.intersection(*(set(records[i]['capture_ids']) for i in ids))
        if len(ids) > 1 and (not common_capture or 'title_ids' not in point or 'detail_ids' not in point):
            raise ValueError('Grouped fields require one captured source and explicit title/field identities: '
                + json.dumps({'point_index': point_index, 'captures_by_record': {i: records[i]['capture_ids'] for i in ids},
                    'has_title_ids': 'title_ids' in point, 'has_detail_ids': 'detail_ids' in point}, separators=(',', ':'))
                + '. Use separate points for records without a common capture_id; do not combine different subjects.')
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
                    raise ValueError('Supporting groups must retain every selected field without duplicate groups: '
                        + json.dumps({'point_index': point_index, 'missing_detail_ids': sorted(set(selected)-set(grouped)),
                            'extra_detail_ids': sorted(set(grouped)-set(selected)), 'selected_detail_ids': selected,
                            'detail_groups': groups}, separators=(',', ':')))
                rendered_groups = [' '.join(available[i] for i in group) for group in groups]
                invalid_groups = [groups[index] for index, group in enumerate(rendered_groups)
                    if not any(' '.join(group.split()) in ' '.join(quote.split())
                        and (len(ids) == 1 or common_capture.intersection(records[identity]['quote_capture_ids'][qindex]))
                        for identity in ids for qindex, quote in enumerate(records[identity]['quotes']))]
                if invalid_groups:
                    raise ValueError('Supporting field group is not a quoted relationship: '
                        + json.dumps({'point_index': point_index, 'noncontiguous_groups': invalid_groups},
                                     separators=(',', ':'))
                        + '. Split nonadjacent fields into separate groups; keep every selected detail_id. '
                        'Do not combine different subjects merely because they share a page.')
                detail = ' · '.join(rendered_groups)
            else:
                detail = ' '.join(available[i] for i in selected)
        else:
            if 'detail_groups' in point or 'title_ids' in point:
                raise ValueError('Excerpt layout requires excerpt identities')
            detail = point['detail']
        if not isinstance(detail, str):
            raise ValueError('Invalid supporting excerpt: ' + json.dumps({'point_index': point_index,
                'max_detail_chars': SOURCE_ANSWER_MAX_DETAIL_CHARS, 'detail_chars': len(detail) if isinstance(detail, str) else None}, separators=(',', ':')))
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
        title_selection = point.get('title_ids')
        title_records = [records[identity] for identity in ids
            if (title_selection is None and identity == ids[0]) or (title_selection is not None
                and any(excerpt['id'] in title_selection for excerpt in records[identity]['excerpts']))]
        title_roles = {binding['role'] for record in title_records for binding in record['semantic_bindings']
            if not common_capture or binding['capture_id'] in common_capture}
        if title_roles and role is None:
            raise ValueError('Typed source observations require an explicit answer item role')
        # A name may be captured as a field of a listing. It can identify
        # that subject when the selected entity quote contains the same title
        # at the same capture; an unrelated property cannot stand in for it.
        named_subject = (role == 'entity' and title_roles == {'attribute'} and any(
            any(binding['role'] == 'entity' and binding['capture_id'] in common_capture
                for binding in records[identity]['semantic_bindings'])
            and any(' '.join(title.split()) in ' '.join(quote.split())
                and common_capture.intersection(records[identity]['quote_capture_ids'][index])
                for index, quote in enumerate(records[identity]['quotes'])) for identity in ids))
        # An observed property may be used as background context without
        # promoting it to an entity. Controls remain controls; independent
        # relevance/source review still checks the interpretation.
        background_attribute = role == 'context' and title_roles == {'attribute'}
        if title_roles and ((role not in title_roles and not named_subject and not background_attribute)
                or (role == 'entity' and 'interface_control' in title_roles)):
            raise ValueError('Answer title role contradicts its captured source role: ' + json.dumps(
                {'point_index': point_index, 'title': title, 'proposed_role': role,
                 'captured_title_roles': sorted(title_roles), 'evidence_ids': ids}, separators=(',', ':')))
        body = summary.strip() if summary is not None else _readable_source_line(detail.strip())
        if body == title:
            body = ''
        # A shared name and page do not identify the same record: separate
        # departures, showtimes or product variants can share both. Compare
        # the verified content actually presented, keeping exact duplicate
        # cards blocked even when their evidence comes from different captures.
        identity = (role, *(unicodedata.normalize('NFKC', ' '.join(value.split()))
                            for value in (title, body)))
        if identity in rendered_records:
            raise ValueError('Repeated answer item: ' + json.dumps({
                'point_index': point_index, 'previous_point_index': rendered_records[identity],
                'evidence_ids': ids}, separators=(',', ':')))
        rendered_records[identity] = point_index
        urls = _selected_source_urls(context, records, ids, common_capture)
        for identity in ids:
            selected_record_urls.setdefault(identity, set()).update(urls)
        for url in urls:
            citation_numbers.setdefault(url, len(citation_numbers) + 1)
        citation = ' ' + ' · '.join(f'[Source {citation_numbers[url]}]({url})' for url in urls[:2]) if urls else ''
        if point_index not in summarized and relation != 'supporting_context':
            blocks.append(ReplyFinding(title, body, citation))
    selected_limits = [{**row, 'text': summaries.get(identity, row['text'])}
        for identity in limitations for row in context['limitations'] if row['id'] == identity]
    blocker_notes = list(dict.fromkeys(row['text'] for row in selected_limits if row['kind'] == 'blocker'))
    unknown_notes = list(dict.fromkeys(row['text'] for row in selected_limits
        if row['kind'] != 'blocker' and row['text'] not in blocker_notes))
    intro_urls = list(dict.fromkeys(url for identity in intro_ids
        for url in sorted(selected_record_urls.get(identity, records[identity]['urls']))))
    for url in intro_urls:
        citation_numbers.setdefault(url, len(citation_numbers) + 1)
    intro_citations = ' ' + ' · '.join(f'[Source {citation_numbers[url]}]({url})' for url in intro_urls[:2]) if intro_urls else ''
    if item_identities is not None:
        item_identities.update(rendered_records)
    notes = tuple(ReplySection(heading, items=tuple(items)) for heading,items in (
        ('Source access', blocker_notes), ('Not verified from this check', unknown_notes)) if items)
    return render_user_message(UserMessage(summary=intro.strip() + intro_citations if intro.strip() else '',
        findings=tuple(blocks), sections=notes, kind=message_kind))



def source_answer_repair_context(tool_results, presentation, error):
    """Report independent draft defects together within the one repair budget.

    Rechecking each bounded point without summary suppression exposes title/role
    errors that an earlier intro-coverage error would otherwise hide. Nothing is
    selected, reclassified or published by this diagnostic pass.
    """
    issues = [{'scope': 'presentation', 'reason': str(error)[:1500]}]
    guidance = []
    context = source_answer_context(tool_results)
    records = {row['id']: row for row in context['evidence']}
    excerpts = {excerpt['id']:excerpt['text'] for row in context['evidence'] for excerpt in row['excerpts']}
    if isinstance(presentation, Mapping) and isinstance(presentation.get('points'), list):
        for index, point in enumerate(presentation['points'][:20]):
            # Report size independently of quote/capture failures, so repairing
            # one constraint does not merely reveal a previously hidden limit.
            if isinstance(point, Mapping):
                ids = point.get('evidence_ids')
                if (isinstance(ids, list) and ids
                        and all(isinstance(identity, str) and identity in records for identity in ids)):
                    captures = set.intersection(*(set(records[identity]['capture_ids']) for identity in ids))
                    title_ids = point.get('title_ids')
                    title_records = [records[identity] for identity in ids if (
                        title_ids is None and identity == ids[0]) or (
                        isinstance(title_ids, list) and any(excerpt['id'] in title_ids
                            for excerpt in records[identity]['excerpts']))]
                    roles = {binding['role'] for row in title_records for binding in row['semantic_bindings']
                             if binding['capture_id'] in captures}
                    compatible = set(roles)
                    if roles == {'attribute'}:
                        compatible.add('context')
                    title = ' '.join(excerpts[identity] for identity in title_ids
                        if identity in excerpts) if isinstance(title_ids, list) else ''
                    anchors = [row['id'] for row in records.values()
                        if title and any(binding['role'] == 'entity' and binding['capture_id'] in captures
                            for binding in row['semantic_bindings'])
                        and any(' '.join(title.split()) in ' '.join(quote.split())
                            and captures.intersection(row['quote_capture_ids'][q])
                            for q, quote in enumerate(row['quotes']))]
                    guidance.append({'scope': 'point', 'point_index': index, 'code': 'selection_contract',
                        'required_fields': ['title_ids', 'detail_ids'] if len(ids) > 1 else ['detail_ids'],
                        'captured_title_roles': sorted(roles), 'compatible_title_roles': sorted(compatible),
                        'same_capture_entity_evidence_ids': anchors[:8],
                        'instruction': 'Keep an explicit meaningful title. Use a requested compatible role, '
                            'or explicitly include a matching entity anchor to identify that subject. '
                            'Supporting context is not another independent entity. Never join different captures.'})
                groups = point.get('detail_groups')
                selected = point.get('detail_ids')
                detail = point.get('detail')
                if (isinstance(groups, list) and all(isinstance(group,list)
                        and all(isinstance(identity,str) and identity in excerpts for identity in group) for group in groups)):
                    detail = ' · '.join(' '.join(excerpts[identity] for identity in group) for group in groups)
                elif isinstance(selected, list) and all(isinstance(identity,str) and identity in excerpts for identity in selected):
                    detail = ' '.join(excerpts[identity] for identity in selected)
                if isinstance(detail,str) and len(detail) > SOURCE_ANSWER_MAX_DETAIL_CHARS:
                    issues.append({'scope':'point','point_index':index,'code':'supporting_excerpt_limit',
                        'max_detail_chars':SOURCE_ANSWER_MAX_DETAIL_CHARS,'detail_chars':len(detail)})
            probe = {key: value for key, value in presentation.items()
                     if key not in {'intro', 'intro_evidence_ids', 'summary_point_indices', 'points'}}
            probe.update(intro='', points=[point], limitation_ids=[])
            try:
                render_source_answer(tool_results, probe)
            except ValueError as exc:
                issues.append({'scope': 'point', 'point_index': index, 'reason': str(exc)[:1500]})
    return {'previous_selection': presentation, 'validation_issues': issues + guidance}


def recover_selected_source_excerpts(tool_results, presentation, *, authored_only=False):
    """Recover model-selected literal facts, never an arbitrary source inventory.

    Broken cross-capture groups may be separated, but not interpreted as a join.
    Every resulting point still passes the normal quote/capture/role validator.
    This is partial publication only; it cannot certify relevance or completion.
    """
    presentation = normalize_source_answer_selection(presentation)
    if not isinstance(presentation, Mapping) or not isinstance(presentation.get('points'), list):
        return None
    focus = presentation.get('focus_roles')
    if (not isinstance(focus, list) or not focus or len(focus) > 4
            or any(role not in SOURCE_SEMANTIC_ROLES for role in focus)):
        return None
    context = source_answer_context(tool_results, include_bindings=True)
    records = {row['id']: row for row in context['evidence']}
    blocks, supporting_blocks, seen, accepted = [], [], set(), []
    citation_numbers = {}

    def keep(point, *, supporting=False):
        probe = {'intro':'', 'focus_roles':focus, 'points':[point], 'limitation_ids':[]}
        identities = set()
        try:
            rendered = _render_source_answer(tool_results, probe, context, require_semantics=True,
                item_identities=identities, citation_numbers=citation_numbers)
        except (ValueError, TypeError):
            return False
        if not identities.intersection(seen):
            seen.update(identities)
            (supporting_blocks if supporting else blocks).append(rendered)
            accepted.append(probe)
        return True

    for point in presentation['points'][:20]:
        if not isinstance(point, Mapping):
            continue
        if authored_only and (point.get('request_relation') != 'result' or not point.get('summary')):
            continue
        if keep(point):
            continue
        if authored_only:
            # A failed authored point needs composition, not a reconstructed
            # list of fields or independent fragments with changed meaning.
            continue
        # Preserve a complete, quoted same-capture field group before splitting
        # it. The normal validator still checks roles, titles and relationships.
        # This is partial publication, never entity promotion or completion.
        ids = point.get('evidence_ids')
        selected_records = [records[identity] for identity in ids
            if isinstance(identity, str) and identity in records] if isinstance(ids, list) else []
        field_ids = [identity for key in ('title_ids', 'detail_ids')
            for identity in (point[key] if isinstance(point.get(key), list) else [])
            if isinstance(identity, str)]
        fields = [excerpt['text'] for row in selected_records for excerpt in row['excerpts']
            if excerpt['id'] in field_ids]
        # A common page is insufficient to join two subjects after review has
        # failed. Retain a group only when one non-document quote contains its
        # explicit title and all fields. Otherwise show independent fragments
        # as supporting details, never additional candidate entities.
        quoted_record = bool(fields and point.get('title_ids') and any(
            all(' '.join(field.split()) in ' '.join(quote.split()) for field in fields)
            and not row['whole_document_capture_ids'][q]
            for row in selected_records for q, quote in enumerate(row['quotes'])))
        if quoted_record and point.get('semantic_role') == 'entity' and 'attribute' in focus:
            if keep({**point, 'semantic_role': 'attribute'}):
                continue
        if not isinstance(ids, list) or len(ids) > 8:
            continue
        for identity in ids:
            if not isinstance(identity, str) or identity not in records:
                continue
            record = records[identity]
            excerpts = {row['id'] for row in record['excerpts']}
            candidate = {**point, 'evidence_ids':[identity]}
            if 'detail_ids' in candidate:
                if not isinstance(candidate['detail_ids'], list):
                    continue
                candidate['detail_ids'] = [ref for ref in candidate['detail_ids'] if isinstance(ref,str) and ref in excerpts]
                if 'title_ids' in candidate:
                    titles = candidate['title_ids']
                    if not isinstance(titles, list):
                        continue
                    candidate['title_ids'] = [ref for ref in titles if isinstance(ref,str) and ref in excerpts]
                    if not candidate['title_ids']:
                        candidate.pop('title_ids')
                if 'detail_groups' in candidate:
                    groups = candidate['detail_groups']
                    if not isinstance(groups, list):
                        continue
                    # Keep only whole quoted groups; stripping half of a label,
                    # unit or relationship would change what the source says.
                    groups = [group for group in groups if isinstance(group,list) and group
                        and all(isinstance(ref,str) and ref in excerpts for ref in group)]
                    candidate['detail_groups'] = groups
                    candidate['detail_ids'] = list(dict.fromkeys(ref for group in groups for ref in group))
            if keep(candidate):
                continue
            roles = {binding['role'] for binding in record['semantic_bindings']}
            allowed = roles.intersection(focus)
            if len(allowed) == 1:
                role = next(iter(allowed))
                # A rejected entity/control interpretation is never promoted.
                # An explicitly requested captured attribute may stand alone.
                if role == 'attribute' and point.get('semantic_role') in {'entity','attribute'}:
                    keep({**candidate, 'semantic_role':role}, supporting=True)
    if not blocks and not supporting_blocks:
        return None
    if supporting_blocks:
        from nullion.chat_text import ReplySection, UserMessage, render_user_message
        blocks.append(render_user_message(UserMessage(sections=(ReplySection(
            'Other captured details', paragraphs=tuple(supporting_blocks)),))))
    limitation_ids = presentation.get('limitation_ids')
    known_limitations = {row['id'] for row in context['limitations']}
    selected_limits = []
    if isinstance(limitation_ids, list):
        selected_limits = list(dict.fromkeys(ref for ref in limitation_ids[:3]
            if isinstance(ref, str) and ref in known_limitations))
        if authored_only:
            summaries = presentation.get('limitation_summaries') or {}
            selected_limits = [ref for ref in selected_limits if isinstance(summaries, dict)
                and isinstance(summaries.get(ref), str) and summaries[ref].strip()]
        if selected_limits:
            blocks.append(_render_source_answer(tool_results,
                {'intro':'', 'focus_roles':focus, 'points':[], 'limitation_ids':selected_limits,
                 'limitation_summaries': presentation.get('limitation_summaries') or {}},
                context, require_semantics=True))
    from nullion.chat_text import apply_presentation_preferences
    return {'text':apply_presentation_preferences('\n\n'.join(blocks), message_kind='report'),
            'selections':accepted, 'point_count':len(accepted),
            'limitation_count': len(selected_limits)}


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
