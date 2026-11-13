"""Shared reasoning instruction, independent of workflow, language and provider.

This is part of the existing model call, not an intent classifier or permission
grant. Tool workflows additionally receive independent outcome review.
"""
from dataclasses import dataclass
from pathlib import Path
from contextlib import contextmanager
from contextvars import ContextVar

_ACTION_REVIEWER = ContextVar("nullion_action_reviewer", default=None)
REVIEW_INSPECTION_MAX_WINDOWS = 128
REVIEW_INSPECTION_CHAR_BUDGET = 24_000
REVIEW_INSPECTION_MAX_WINDOW_CHARS = 12_000
RETAINED_INSPECTION_CHAR_BUDGET = 48_000


def mark_review_unavailable(presentation, review):
    """A failed quality reviewer is not an execution or permission verdict."""
    presentation.update(outcome_review_status="unavailable",
        outcome_review_role="helper",
        outcome_review_error=getattr(review, "review_error", None) or "review_unavailable",
        outcome_review_timeout=bool(getattr(review, "review_timed_out", False)))


def review_warning_reply(text, presentation):
    """Legacy handoff: reviewer availability is internal, not answer content."""
    presentation['outcome_review_role'] = 'helper'
    return text


def structured_check_model_client(client):
    """Use the same model with a bounded profile for tool-free JSON checks.

    Research and response generation retain the configured agent profile. A
    dataclass copy leaves credentials, model selection and the original client
    intact. Adapters without this capability retain their existing behavior.
    """
    import os
    from dataclasses import is_dataclass, replace
    effort = os.environ.get("NULLION_WORKFLOW_CHECK_REASONING_EFFORT", "low").strip().lower()
    if effort not in {"low", "medium", "high"} or not is_dataclass(client):
        return client
    if not hasattr(client, "reasoning_effort") or getattr(client, "reasoning_effort") == effort:
        return client
    try:
        return replace(client, reasoning_effort=effort)
    except (TypeError, ValueError):
        return client


@contextmanager
def action_reasoning_scope(reviewer):
    token = _ACTION_REVIEWER.set(reviewer)
    try:
        yield
    finally:
        _ACTION_REVIEWER.reset(token)


def active_action_review(spec, invocation):
    reviewer = _ACTION_REVIEWER.get()
    if reviewer is None or spec.side_effect_class.value == "read":
        return None
    return reviewer(spec, invocation)



def option_label(value):
    """Number structured choices once, regardless of provider punctuation."""
    import re
    text = str(value).strip()
    while True:
        normalized = re.sub(r"^\d+[.)]\s+", "", text).strip()
        if normalized == text:
            return text
        text = normalized

def selected_file_versions(artifacts):
    """Selected file identity includes bytes, even after a same-size rewrite."""
    import hashlib
    files = []
    for name in artifacts:
        try:
            source = Path(name)
            stat = source.stat()
            # Same-size rewrites can share a filesystem timestamp (or preserve
            # one deliberately). A completion receipt binds bytes, not merely
            # the path's metadata. Stream only selected files; plain turns do
            # no additional work.
            digest = None
            if source.is_file():
                hasher = hashlib.sha256()
                with source.open('rb') as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b''):
                        hasher.update(block)
                digest = hasher.hexdigest()
            files.append((str(name), stat.st_size, stat.st_mtime_ns, digest))
        except (OSError, ValueError, TypeError):
            files.append((str(name), None, None))
    return files


def outcome_binding(request, context, draft, results, artifacts):
    """Bind review to request-owned facts and current selected file versions."""
    from nullion.workflow_recovery import fingerprint, result_value
    files = selected_file_versions(artifacts)
    def evidence_row(result):
        output=result_value(result,'output')
        return (result_value(result,'invocation_id'), result_value(result,'tool_name'),
            result_value(result,'status'),
            {key:value for key,value in output.items()
                if key not in {'artifact_delivery_manifest','artifact_producer_receipt_id'}}
                if isinstance(output,dict) else output,
            result_value(result,'error'), tuple(result_value(result,'recovered_by',()) or ()))
    return fingerprint({"request": request, "context": context, "draft": draft,
        # These runtime delivery annotations do not change source facts. File
        # selection/version is bound separately above; changing any real tool
        # evidence still invalidates the receipt.
        "files": files, "results": [evidence_row(row) for row in results]})


def outcome_binding_components(request, context, draft, results, artifacts):
    from nullion.workflow_recovery import fingerprint
    return tuple((key, fingerprint(value)) for key, value in {
        'request': request, 'context': context, 'draft': draft,
        'evidence': outcome_binding('', {}, None, results, artifacts)}.items())


@dataclass(frozen=True)
class OutcomeReviewReceipt:
    binding: str
    disposition: str
    components: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class ActionReview:
    decision: str
    issues: tuple[str, ...] = ()
    question: str | None = None
    options: tuple[str, ...] = ()
    error: str | None = None
    protocol_errors: tuple[str, ...] = ()
    issue_kind: str | None = None
    step_effect: str | None = None
    concern_scopes: tuple[str, ...] = ()
    advisory: bool = False


def action_review_evidence(results, *, max_results=16, max_chars=24000):
    """Bound source examples without flattening away their contract envelope."""
    selected = list(results)[-max_results:]
    per_result = max(1000, max_chars // max(1, len(selected)))
    evidence = []
    for result in selected:
        remaining = per_result
        omitted = []
        def bounded(value, path=(), depth=0):
            nonlocal remaining
            if depth > 8 or remaining <= 0:
                omitted.append(list(path))
                return None
            if isinstance(value, dict):
                # Scalars and small contract objects survive large collections.
                keys = sorted(value, key=lambda key: (
                    3 if isinstance(value[key], (list, tuple)) else 2 if isinstance(value[key], dict)
                    else 1 if isinstance(value[key], str) else 0,
                    len(value[key]) if isinstance(value[key], (list, tuple, str)) else 0))
                output = {}
                for key in keys[:64]:
                    remaining -= len(str(key)) + 4
                    output[str(key)] = bounded(value[key], (*path, str(key)), depth + 1)
                if len(keys) > 64: omitted.append(list(path))
                return output
            if isinstance(value, (list, tuple)):
                rows = [bounded(item, (*path, index), depth + 1) for index, item in enumerate(value[:3])]
                if len(value) > 3: omitted.append(list(path))
                return rows
            if isinstance(value, str):
                cap = max(0, min(1200, remaining))
                if len(value) > cap: omitted.append(list(path))
                remaining -= min(len(value), cap)
                return value[:cap]
            if value is None or isinstance(value, (bool, int, float)):
                remaining -= 16
                return value
            return bounded(str(value), path, depth)
        output = bounded(result.output)
        evidence.append({'invocation_id': result.invocation_id, 'tool_name': result.tool_name,
            'status': result.status, 'error': result.error, 'output': output,
            'omitted_paths': omitted})
    return evidence


def compact_evidence_value(value, *, max_chars):
    """Bound a JSON evidence object without replacing its schema with prose.

    Omission paths describe the bounded view, never an absent source field.
    Prefer shortening long leaves before sampling collections. The caller keeps
    each invocation's identity/status separately even when its view cannot fit.
    """
    import json
    original = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if len(original) <= max_chars:
        return value, []
    for collection_cap in (64, 16, 8, 3, 1, 0):
        for text_cap in (500, 160, 64, 16, 0):
            omitted = []
            def bounded(item, path=(), depth=0):
                if depth > 12:
                    omitted.append(list(path))
                    return None
                if isinstance(item, str):
                    # These are opaque handles or typed source scopes. A
                    # shortened query can look malformed; a shortened date or
                    # cursor can select the wrong evidence. Omit the containing
                    # view if it cannot fit, never silently alter these values.
                    if path and path[-1] in {'id', 'query', 'start', 'end', 'url', 'source_url',
                            'provider_id', 'date', 'dateTime', 'next_page_token', 'inventory_version'}:
                        return item
                    if len(item) <= text_cap:
                        return item
                    omitted.append(list(path))
                    # Preserve both ends; leave exact values untouched when
                    # they fit. Omission metadata prevents absence inference.
                    head = text_cap // 2
                    tail = text_cap - head
                    return item[:head] + (item[-tail:] if tail else '')
                if isinstance(item, dict):
                    return {str(key): bounded(child, (*path, str(key)), depth + 1)
                            for key, child in item.items()}
                if isinstance(item, (list, tuple)):
                    if len(item) > collection_cap:
                        omitted.append(list(path))
                    return [bounded(child, (*path, index), depth + 1)
                            for index, child in enumerate(item[:collection_cap])]
                return item
            result = bounded(value)
            if len(json.dumps(result, ensure_ascii=False, sort_keys=True)) <= max_chars:
                return result, omitted
    return None, [[]]


def review_response_schema(schema, *, inspection_invocation_ids=()):
    """Describe both valid protocol responses while retained inspection is open."""
    identities = sorted(set(inspection_invocation_ids))
    if not identities:
        return schema
    inspection = {"type": "object", "additionalProperties": False,
        "required": ["disposition", "evidence_requests"],
        "properties": {"disposition": {"const": "inspect_evidence"},
            "evidence_requests": {"type": "array", "minItems": 1, "maxItems": REVIEW_INSPECTION_MAX_WINDOWS,
                "items": {"type": "object", "additionalProperties": False,
                    "required": ["invocation_id", "json_pointer", "offset", "length"],
                    "properties": {"invocation_id": {"enum": identities},
                        "json_pointer": {"type": "string", "maxLength": 1024, "pattern": "^(/|$)"},
                        "offset": {"type": "integer", "minimum": 0},
                        "length": {"type": "integer", "minimum": 1, "maximum": REVIEW_INSPECTION_MAX_WINDOW_CHARS}}}}}}
    return {"oneOf": [schema, inspection]}


def cursor_page_evidence(*, scope, cursor, next_cursor, result_count):
    """Adapter-owned receipt for one exact page, not a completeness claim."""
    from nullion.workflow_recovery import fingerprint
    def token(value):
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            raise ValueError("Invalid source continuation token")
        return value
    if not isinstance(result_count, int) or isinstance(result_count, bool) or result_count < 0:
        raise ValueError("Invalid source page count")
    return {"type": "cursor_page", "scope_id": fingerprint(scope), "cursor": token(cursor),
            "next_cursor": token(next_cursor), "result_count": result_count}


def source_pagination_summary(results, *, references=None):
    """Verify declared cursor chains from same-request completed receipts only.

    This proves listing coverage for that opaque adapter scope, never that its
    records are relevant, fully read, current forever, or acted on.
    """
    from collections import defaultdict
    from collections.abc import Mapping
    groups = defaultdict(list)
    aliases = {identity: reference for reference, identity in (references or {}).items()}
    for result in results:
        output = result.get('output') if isinstance(result, Mapping) else getattr(result, 'output', None)
        status = result.get('status') if isinstance(result, Mapping) else getattr(result, 'status', None)
        identity = result.get('invocation_id') if isinstance(result, Mapping) else getattr(result, 'invocation_id', None)
        page = output.get('source_pagination') if isinstance(output, Mapping) else None
        if status != 'completed' or not isinstance(identity, str) or not identity or not isinstance(page, Mapping):
            continue
        scope = page.get('scope_id'); count = page.get('result_count')
        if (page.get('type') != 'cursor_page' or not isinstance(scope, str) or not scope
                or not isinstance(count, int) or isinstance(count, bool) or count < 0
                or any(value is not None and (not isinstance(value, str) or not value)
                       for value in (page.get('cursor'), page.get('next_cursor')))
                or 'cursor' not in page or 'next_cursor' not in page):
            continue
        tool = result.get('tool_name', '') if isinstance(result, Mapping) else getattr(result, 'tool_name', '')
        if not isinstance(tool, str) or not tool:
            continue
        groups[(tool, scope)].append((page, aliases.get(identity, identity)))
    summaries = []
    for (tool, scope), pages in groups.items():
        by_cursor = defaultdict(list)
        for page, identity in pages:
            by_cursor[page['cursor']].append((page, identity))
        cursor = None; visited = set(); count = 0; reason = None; pending = []
        while True:
            if cursor in visited:
                reason = 'cursor_cycle'; break
            entries = by_cursor.get(cursor)
            if not entries:
                reason = 'missing_initial_page' if not visited else 'missing_next_page'; break
            if len({(page['next_cursor'], page['result_count']) for page, _ in entries}) != 1:
                reason = 'conflicting_page_receipts'; break
            page = entries[0][0]
            visited.add(cursor); count += page['result_count']
            if page['next_cursor'] is None:
                break
            if page['next_cursor'] not in by_cursor:
                pending = [identity for _, identity in entries]
            cursor = page['next_cursor']
        if reason is None and set(by_cursor) != visited:
            reason = 'unlinked_page_receipts'
        summaries.append({'tool_name': tool, 'scope_id': scope, 'complete': reason is None,
            'observed_pages': len(by_cursor), 'linked_pages': len(visited), 'linked_result_count': count,
            'page_invocation_ids': [identity for _, identity in pages],
            'pending_after_invocation_ids': pending, 'reason': reason})
    return summaries


def inspect_review_evidence(requests, *, results, references, visible_ids, _char_budget=REVIEW_INSPECTION_CHAR_BUDGET):
    """Resolve bounded, typed inspections of already-authorized turn results.

    This never invokes a tool, reads a file, accesses another request or changes
    state. JSON pointers and character windows refer only to retained output.
    """
    import json
    from jsonschema import validate
    from nullion.redaction import redact_value
    validate(requests, {'type':'array','minItems':1,'maxItems':REVIEW_INSPECTION_MAX_WINDOWS,'items':{
        'type':'object','additionalProperties':False,'required':['invocation_id','json_pointer'],
        'properties':{'invocation_id':{'type':'string','minLength':1,'maxLength':256},
            'json_pointer':{'type':'string','maxLength':1024,'pattern':'^(/|$)'},
            'offset':{'type':'integer','minimum':0},'length':{'type':'integer','minimum':1,'maximum':REVIEW_INSPECTION_MAX_WINDOW_CHARS}}}})
    by_id={result.invocation_id:result for result in results}
    remaining=min(RETAINED_INSPECTION_CHAR_BUDGET, _char_budget); inspections=[]
    for request in requests:
        reference=request['invocation_id']
        if reference not in visible_ids:
            raise ValueError('Inspection must reference visible current-turn evidence')
        identity=references.get(reference,reference)
        if identity not in by_id:
            raise ValueError('Inspection evidence is not retained by this request')
        value=redact_value(by_id[identity].output)
        pointer=request['json_pointer']
        for encoded in pointer.split('/')[1:] if pointer else ():
            key=encoded.replace('~1','/').replace('~0','~')
            if isinstance(value,dict) and key in value:
                value=value[key]
            elif (isinstance(value,list) and key.isascii() and key.isdecimal()
                    and (key=='0' or not key.startswith('0')) and int(key)<len(value)):
                value=value[int(key)]
            else:
                raise ValueError('Inspection pointer does not identify retained evidence')
        encoded=value if isinstance(value,str) else json.dumps(value,ensure_ascii=False,sort_keys=True)
        offset=request.get('offset',0); length=min(request.get('length',REVIEW_INSPECTION_MAX_WINDOW_CHARS),remaining)
        if offset>len(encoded):
            raise ValueError('Inspection window exceeds retained evidence budget')
        if length<=0:
            inspections.append({'invocation_id':reference,'json_pointer':pointer,'offset':offset,
                'total_chars':len(encoded),'shown_chars':0,'complete':False,
                'inspection_status':'budget_exhausted'})
            continue
        excerpt=encoded[offset:offset+length]
        inspections.append({'invocation_id':reference,'json_pointer':pointer,'offset':offset,
            'total_chars':len(encoded),'shown_chars':len(excerpt),
            'complete':offset==0 and len(excerpt)==len(encoded),'encoding':'text' if isinstance(value,str) else 'json',
            'content':excerpt})
        remaining-=len(excerpt)
    return inspections


def _inspection_result_fingerprint(result):
    import hashlib
    import json
    return hashlib.sha256(json.dumps({'tool_name': result.tool_name, 'status': result.status,
        'error': result.error, 'output': result.output}, ensure_ascii=False, sort_keys=True,
        default=str).encode('utf-8')).hexdigest()


def _valid_inspection_window(entry):
    """Validate internal descriptors too; a corrupt cache must not stop a run."""
    return (isinstance(entry, dict)
        and isinstance(entry.get('invocation_id'), str) and 0 < len(entry['invocation_id']) <= 256
        and isinstance(entry.get('json_pointer'), str) and len(entry['json_pointer']) <= 1024
        and (not entry['json_pointer'] or entry['json_pointer'].startswith('/'))
        and type(entry.get('offset')) is int and entry['offset'] >= 0
        and type(entry.get('length')) is int and 0 < entry['length'] <= REVIEW_INSPECTION_MAX_WINDOW_CHARS
        and isinstance(entry.get('source_fingerprint'), str))


def remember_review_inspections(cache, inspections, *, results, references):
    """Remember owned window descriptors, never model verdicts or copied facts."""
    by_id = {result.invocation_id: result for result in results}
    fingerprints = {}
    entries = []
    for window in inspections:
        identity = references.get(window.get('invocation_id'), window.get('invocation_id'))
        result = by_id.get(identity)
        if result is None or not window.get('shown_chars'):
            continue
        if identity not in fingerprints:
            fingerprints[identity] = _inspection_result_fingerprint(result)
        entries.append({'invocation_id': identity, 'json_pointer': window['json_pointer'],
            'offset': window['offset'], 'length': window['shown_chars'],
            'source_fingerprint': fingerprints[identity]})
    seen = set()
    selected = []
    previous = cache.get('retained_inspection_windows') or ()
    if not isinstance(previous, (list, tuple)):
        previous = ()
    for entry in [*entries, *previous]:
        if not _valid_inspection_window(entry):
            continue
        key = (entry['invocation_id'], entry['json_pointer'], entry['offset'], entry['source_fingerprint'])
        if key not in seen:
            seen.add(key)
            selected.append({key: entry[key] for key in (
                'invocation_id', 'json_pointer', 'offset', 'length', 'source_fingerprint')})
    cache['retained_inspection_windows'] = selected[:REVIEW_INSPECTION_MAX_WINDOWS]


def retained_review_inspections(cache, *, results, references=None, max_chars=RETAINED_INSPECTION_CHAR_BUDGET):
    """Re-read same-request windows only while their complete source is unchanged.

    Callers scope this cache to their existing request/contract identity. Stored
    content cannot forge proof: views are rebuilt from the current owned result.
    New drafts still need independent review; no earlier verdict is inherited.
    """
    by_id = {result.invocation_id: result for result in results}
    aliases = {}
    for alias, identity in (references or {}).items():
        # Prefer short references when both canonical and report ids are visible.
        if identity not in aliases or alias != identity:
            aliases[identity] = alias
    fingerprints = {}
    requests = []
    entries = cache.get('retained_inspection_windows') or ()
    if not isinstance(entries, (list, tuple)):
        return []
    for entry in entries[:REVIEW_INSPECTION_MAX_WINDOWS]:
        if not _valid_inspection_window(entry):
            continue
        identity = entry.get('invocation_id')
        result = by_id.get(identity)
        if result is None or (references is not None and identity not in aliases):
            continue
        if identity not in fingerprints:
            fingerprints[identity] = _inspection_result_fingerprint(result)
        if entry['source_fingerprint'] != fingerprints[identity]:
            continue
        requests.append({key: entry[key] for key in ('invocation_id', 'json_pointer', 'offset', 'length')})
    if not requests:
        return []
    budget = max(0, min(max_chars, RETAINED_INSPECTION_CHAR_BUDGET))
    if budget <= 0:
        return []
    if sum(row['length'] for row in requests) > budget:
        # Carryover is a preview, not another full source dump. Share its
        # allowance across every retained window so late sources remain visible.
        # The exact offsets and incomplete flags still expose the omitted scope.
        remaining = budget
        lengths = {}
        for position, index in enumerate(sorted(range(len(requests)), key=lambda i: requests[i]['length'])):
            share = remaining // (len(requests) - position)
            lengths[index] = min(requests[index]['length'], share)
            remaining -= lengths[index]
        requests = [{**row, 'length': lengths[index]} for index, row in enumerate(requests)
                    if lengths[index] > 0]
    try:
        windows = inspect_review_evidence(requests, results=results, references={}, visible_ids=set(by_id),
            _char_budget=budget)
    except ValueError:
        # Invalid pointers/offsets in persisted internal state are not source facts.
        return []
    return [{**window, 'invocation_id': aliases.get(window['invocation_id'], window['invocation_id'])}
        for window in windows if window.get('shown_chars')]


def assess_action(*, model_client, request, context, invocation, spec, evidence, now, retained_results=(), inspection_cache=None):
    """Retired compatibility hook; native tool admission remains authoritative."""
    return ActionReview("proceed", ())



WORKFLOW_REASONING_INSTRUCTION = (
    "Shared request reasoning contract: Before acting, establish the user's actual requested outcome "
    "using the current request, its selected references, amendments, known preferences and available evidence. "
    "For a practical recommendation, verify a useful shortlist and resolve contenders with actual evidence "
    "of a material advantage; do not inspect every incidental listing merely because its unknown fields "
    "could hypothetically change the ranking. Disclose the checked scope. Preserve explicitly requested "
    "exhaustive coverage, named subjects, and global-optimum claims as separate requirements. "
    "Check the premise, target, dates, timezone, scope and constraints for contradictions or stale assumptions. "
    "Do not blindly execute an instruction whose premise is contradicted by evidence. Explain the specific "
    "conflict respectfully and ask a numbered clarification when the intended correction is ambiguous; "
    "When a correction offers alternatives, present a concise question with numbered choices and allow "
    "a numeric reply; include a choice to supply a different value. Never silently replace a fixed "
    "target/date, expand account consent or blame the user without proof. "
    "While working, compare each result with the requested outcome. A successful tool call, navigation, "
    "discovery result or created file alone does not prove the request is fulfilled. Notice irrelevant "
    "results, missing constraints, superseded evidence and mistakes in your own plan; correct the approach "
    "using permitted capabilities and preserve completed actions rather than repeating them. "
    "Before answering, review the entire request against what the answer actually says and what will be "
    "delivered. Correct omissions, contradictions and unsupported claims. Reuse details already supplied "
    "instead of asking again. If necessary evidence or authority is missing, give the useful confirmed "
    "outcome and a specific limitation or numbered question. Keep this assessment internal; provide the "
    "answer, relevant correction and actionable next step, not your private reasoning."
)


def deferred_execution_receipts(results):
    """Accepted background work proves setup, never its pending future outcome."""
    receipts = []
    indexes = {}
    for result in results:
        output = result.output if isinstance(result.output, dict) else {}
        row = output.get('deferred_execution')
        action = output.get('action_receipt')
        if (result.status != 'completed' or result.error or not isinstance(row, dict)
                or not isinstance(action, dict) or action.get('type') != 'action_receipt'
                or row.get('version') != 1 or row.get('phase') != 'accepted'
                or not isinstance(row.get('handle'), str) or not row['handle']
                or row['handle'] != action.get('object_id')
                or not isinstance(row.get('owner'), str) or not row['owner']
                or not isinstance(row.get('configuration'), dict)):
            continue
        from nullion.redaction import redact_value
        current = redact_value({'invocation_id': result.invocation_id,
            'tool_name': result.tool_name, **row})
        identity = (row['owner'], row['handle'])
        if identity in indexes:
            receipts[indexes[identity]] = current
        else:
            indexes[identity] = len(receipts)
            receipts.append(current)
    return receipts


def accepted_setup_only_contract(requirements, results, *, read_tool_names=()):
    """Separate accepted setup from an independently requested source lookup.

    The existing model-produced material contract decides the requested
    outcomes. Only real accepted receipts can establish this narrower phase;
    source facts, comparisons and files always retain search coverage.
    """
    rows = tuple(requirements)
    accepted_tools = {row['tool_name'] for row in deferred_execution_receipts(results)}
    execution_tools = accepted_tools | set(read_tool_names)
    if not rows or not accepted_tools:
        return False
    setup_action = False
    for row in rows:
        eligible = set(row.eligible_evidence_tool_names)
        if row.requirement_kind == 'action':
            if not eligible or not eligible <= execution_tools:
                return False
            setup_action = setup_action or bool(eligible & accepted_tools)
        elif row.requirement_kind == 'constraint':
            if row.delivery_required and (not eligible or not eligible <= execution_tools):
                return False
        elif row.requirement_kind != 'reasoned_answer' or eligible:
            return False
    return setup_action



def durable_action_receipt(spec, result):
    if spec is None or (spec.side_effect_class.value != 'account_write'
                        and not getattr(spec, 'replay_safe_after_completion', False)):
        return False
    receipt = result.output.get('action_receipt') if isinstance(result.output, dict) else None
    if not isinstance(receipt, dict) or not receipt:
        return False
    if result.status == 'completed':
        return True
    return receipt.get('type') == 'action_receipt' and all(
        isinstance(receipt.get(key), str) and receipt[key].strip()
        for key in ('action', 'object_type', 'summary'))
