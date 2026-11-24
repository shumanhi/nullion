"""Bounded model views over retained source collections; no source is modified."""
from copy import deepcopy
import json


def email_body_field(message):
    if not isinstance(message, dict):
        return None
    payload = message.get('body_payload')
    if isinstance(payload, str) and (payload or message.get('body_truncated') is True):
        return 'body_payload'
    return 'body' if isinstance(message.get('body'), str) else None


def is_email_message_evidence(output):
    scope = output.get('evidence_scope') if isinstance(output, dict) else None
    return isinstance(scope, dict) and scope.get('kind') == 'email_message' and email_body_field(output.get('message')) is not None


def email_evidence_count(output):
    if isinstance(output, dict) and output.get('result_kind') == 'email_message_batch':
        messages = output.get('messages')
        return sum(isinstance(row, dict) and is_email_message_evidence(row.get('output'))
                   for row in messages) if isinstance(messages, list) else 0
    return int(is_email_message_evidence(output))


def compact_email_message(output, *, max_chars):
    """Keep the identity and message text ahead of optional mail metadata."""
    message = output['message']
    body_field = email_body_field(message)
    body = message.get(body_field)
    scope = output['evidence_scope']
    view = {'id': output.get('id'), 'evidence_scope': {key: scope[key] for key in (
        'kind', 'retrieved_at', 'message_sent_at', 'current_account_state_verified') if key in scope}, 'message': {
        key: message[key] for key in ('id', 'subject', 'date', 'from') if isinstance(message.get(key), str)}}
    projected = view['message']
    if message.get('body_is_preview') is True:
        projected['body_is_preview'] = True
    if isinstance(body, str):
        original_chars = message.get('original_body_chars')
        was_truncated = message.get('body_truncated') is True
        if type(original_chars) is not int or original_chars < len(body):
            original_chars = None if was_truncated else len(body)
        # Leave room for escaping and the projection metadata. Reduce only
        # when the actual serialized size exceeds the caller's share.
        length = min(len(body), max(0, max_chars - len(json.dumps(view, ensure_ascii=False)) - 100))
        while True:
            projected[body_field] = body[:length]
            if was_truncated or length < len(body) or (original_chars is not None and length < original_chars):
                if original_chars is not None:
                    projected['original_body_chars'] = original_chars
                projected['body_truncated'] = True
            if len(json.dumps(view, ensure_ascii=False)) <= max_chars:
                break
            if not length:
                return None, [[]]
            length //= 2
    elif len(json.dumps(view, ensure_ascii=False)) > max_chars:
        return None, [[]]
    omitted = [[key] for key in output if key not in view]
    omitted.extend(['evidence_scope', key] for key in scope if key not in view['evidence_scope'])
    omitted.extend(['message', key] for key in message if key not in projected)
    if projected.get('body_truncated'):
        omitted.append(['message', body_field])
    return view, omitted


def compact_email_batch(output, *, max_chars):
    """Keep ordered per-message outcomes and bounded, inspectable body text."""
    view = {key: output[key] for key in ('result_kind', 'provider_id', 'requested_messages',
        'completed_messages', 'complete', 'parallel_limit', 'reason') if key in output}
    messages = output.get('messages')
    sources = [row for row in messages if isinstance(row, dict)] if isinstance(messages, list) else []
    rows = [{key: row[key] for key in ('id', 'invocation_id', 'status', 'inspection_request') if key in row}
            for row in sources]
    view['messages'] = rows
    budget = max(0, max_chars - len(json.dumps(view, ensure_ascii=False)))
    share = max(0, budget // max(1, len(rows)) - 220)
    for original, row in zip(sources, rows):
        native = original.get('output')
        if is_email_message_evidence(native):
            projected, omitted = compact_email_message(native, max_chars=share)
            if projected is not None:
                row['output'] = projected
            if omitted:
                row['output_partial'] = True
        elif isinstance(native, dict):
            row['output'] = {key: native[key] for key in ('reason', 'executed') if key in native}
    return view


def compact_native_page_history(output, *, max_chars):
    """Keep native page text and its exact observed links together in history."""
    if (not isinstance(output,dict) or output.get('result_kind')!='browser_page_collection'
            or output.get('items') or output.get('details')):
        return None
    documents=output.get('documents')
    declarations=output.get('source_record_collections')
    if (not isinstance(documents,list) or not documents or len(documents)>6
            or not isinstance(declarations,list)
            or not any(isinstance(d,dict) and d.get('path')==['documents'] for d in declarations)
            or not all(isinstance(d,dict) and isinstance(d.get('url'),str)
                and isinstance(d.get('text'),str) and isinstance(d.get('links'),list) for d in documents)):
        return None
    original_chars=len(json.dumps(output,ensure_ascii=False,default=str))
    if original_chars<=max_chars:
        return None
    result={key:deepcopy(output[key]) for key in ('result_kind','session_id','completed_pages',
        'requested_pages','parallel_limit','reason','recovery','requires_user_choice','completion_scope') if key in output}
    if isinstance(strategy := output.get('strategy'), dict):
        result['strategy'] = {key: strategy[key] for key, kind in
            (('kind', str), ('adaptive_browser_started', bool)) if isinstance(strategy.get(key), kind)}
    if isinstance(output.get('pages'),list):
        result['pages']=[{key:deepcopy(page[key]) for key in ('status','page_url','requested_url',
            'failure_reason','reason','status_code','requires_user_choice') if key in page}
            for page in output['pages'] if isinstance(page,dict)]
    result.update(source_record_collections=deepcopy(declarations),documents=[],
        history_projection={'complete':False,'original_chars':original_chars,
            'document_count':len(documents),'omissions_are_not_absence':True})
    width=min(2000,max(32,max_chars//(4*len(documents))))
    while True:
        result['documents']=[{'url':d['url'],'text':d['text'][:width],'links':[],
            **{key:d[key] for key in ('observed_title','observed_price') if isinstance(d.get(key), str)},
            **({'observed_text':d['observed_text'][:width]} if isinstance(d.get('observed_text'), str) else {}),
            'history_projection':{'original_text_chars':len(d['text']),
                'text_truncated':len(d['text'])>width,'original_link_count':len(d['links']),
                'observed_text_truncated':isinstance(d.get('observed_text'),str) and len(d['observed_text'])>width,
                'omitted_link_count':len(d['links']),
                'omitted_fields':[k for k in d if k not in {'url','text','links','observed_title','observed_price','observed_text'}]}}
            for d in documents]
        room=max_chars-len(json.dumps(result,ensure_ascii=False))-128
        if room>=0:break
        if width<=32:return None
        width=max(32,width//2)
    # Round robin shares the bounded space across pages. URLs are never clipped.
    for index in range(max(len(d['links']) for d in documents)):
        for original,projected in zip(documents,result['documents']):
            if index>=len(original['links']):continue
            link=original['links'][index]
            if not isinstance(link,dict) or not isinstance(link.get('url'),str):continue
            retained={'url':link['url'],'text':str(link.get('text') or '')[:500]}
            if len(str(link.get('text') or ''))>500:retained['text_truncated']=True
            cost=len(json.dumps(retained,ensure_ascii=False))+2
            if cost>room:continue
            room-=cost
            projected['links'].append(retained)
            projected['history_projection']['omitted_link_count']-=1
    return result if len(json.dumps(result,ensure_ascii=False))<=max_chars else None


def _compact_page_details(output, budget):
    if output.get('result_kind') != 'browser_page_collection' or output.get('items') or output.get('documents'):
        return None
    details = output.get('details')
    if not isinstance(details, list) or not details or not all(
        isinstance(row, dict) and isinstance(row.get('source_page_url'), str)
        and isinstance(row.get('title'), str) and isinstance(row.get('observed_text'), str)
        and row['observed_text'] for row in details
    ):
        return None
    sources = []
    for row in details:
        text = row['observed_text']
        sources.append({'source_url': row['source_page_url'], 'title': row['title'],
            'visible_text_segments': [text[offset:offset + 400] for offset in range(0, len(text), 400)],
            **{key:row[key] for key in ('price', 'availability') if row.get(key)}})
    result = {key:output[key] for key in ('result_kind', 'completed_pages', 'requested_pages', 'parallel_limit') if key in output}
    result['detail_evidence'] = sources
    result['evidence_guidance'] = ('Text segments are consecutive excerpts from each bound detail page. '
        'Use these captured values to answer when they cover the request. Inspect retained evidence for a specific missing fact.')
    result['page_outcomes'] = [{key:page[key] for key in ('status', 'page_url', 'failure_reason') if key in page}
                               for page in output.get('pages', ()) if isinstance(page, dict)]
    return result if len(json.dumps(result, ensure_ascii=False)) <= budget else None


def compact_collections(output, *, budget=14000, preserve_identities=False):
    if not isinstance(output,dict) or len(json.dumps(output,ensure_ascii=False))<=budget:
        return output
    if detail_view := _compact_page_details(output, budget):
        return detail_view
    declarations=output.get('source_record_collections')
    if not isinstance(declarations,list):return output
    tables=[]
    for declaration in declarations[:12]:
        if not isinstance(declaration,dict):continue
        path=declaration.get('path')
        if not isinstance(path,list) or not path or not all(isinstance(key,str) for key in path):continue
        value=output
        for key in path:value=value.get(key) if isinstance(value,dict) else None
        rows=[value] if declaration.get('cardinality')=='one' and isinstance(value,dict) else value
        if not isinstance(rows,list) or not rows:continue
        identities=declaration.get('identity_fields');context_fields=declaration.get('context_fields')
        fields = list(dict.fromkeys(
            key for key in [*(identities if isinstance(identities, list) else []),
                            *(context_fields if isinstance(context_fields, list) else [])]
            if isinstance(key, str)
        ))
        if not fields:continue
        pointer=''.join('/'+key.replace('~','~0').replace('/','~1') for key in path)
        column_widths = declaration.get('context_widths')
        tables.append((pointer,rows,fields,declaration.get('cardinality')=='one',
                       {key for key in (identities or ()) if isinstance(key, str)},
                       column_widths if isinstance(column_widths, dict) else {}))
    if not tables:return output
    width=240;column_limit=1000
    while True:
        views=[]
        for pointer,rows,fields,one,identities,column_widths in tables:
            shown_fields=fields[:column_limit]
            values=[];clipped=False
            for row in rows:
                cells=[]
                for key in shown_fields:
                    value=row.get(key) if isinstance(row,dict) else None
                    if isinstance(value,(dict,list)):value=json.dumps(value,ensure_ascii=False,separators=(',',':'))
                    preferred_width = column_widths.get(key)
                    cell_width = min(2000, preferred_width) * width // 240 if type(preferred_width) is int and preferred_width > 240 else width
                    if isinstance(value,str) and len(value)>cell_width and not (preserve_identities and key in identities):
                        value={'preview':value[:cell_width],'truncated':True};clipped=True
                    cells.append(value)
                values.append(cells)
            views.append({'json_pointer':pointer,'cardinality':'one' if one else 'many',
                'record_count':len(rows),'columns':shown_fields,'rows':values,'has_clipped_fields':clipped,
                **({'omitted_columns':fields[column_limit:]} if len(fields)>column_limit else {})})
        result={'result_kind':output.get('result_kind'),'collection_views':views,
            'source_record_collections':deepcopy(declarations),
            'retained_evidence':{'complete':True,'original_chars':len(json.dumps(output,ensure_ascii=False)),
                'read_tool':'inspect_retained_evidence','instructions':
                    'Rows retain source order. For a many collection append /row_index/field to its json_pointer. '
                    'Read exact selected records before quoting clipped values or links. Previews are not usable URLs. '
                    'Full source records remain retained; no source facts have been deleted.'}}
        for key in ('session_id','completed_pages','requested_pages','parallel_limit','coverage','reason','recovery',
                    'status_code','strategy','completion_scope','requires_user_choice','page_controls'):
            if key in output:
                value=deepcopy(output[key])
                if key=='coverage' and isinstance(value,dict):
                    value={k:v for k,v in value.items() if not isinstance(v,(list,dict))}
                    result['retained_evidence']['coverage_pointer']='/coverage'
                if key=='page_controls' and isinstance(value,dict):
                    value={k:value[k] for k in ('form_count','editable_control_count')
                        if type(value.get(k)) is int and value[k]>=0}
                result[key]=value
        if 'pages' in output:
            result['page_outcomes']=[{'index':i,**{k:v for k,v in page.items()
                if k in ('status','failure_reason','requires_user_choice','status_code')}}
                for i,page in enumerate(output['pages']) if isinstance(page,dict)]
            result['retained_evidence']['page_details_pointer']='/pages'
        if len(json.dumps(result,ensure_ascii=False))<=budget:return result
        if width>24:width=max(24,width//2)
        elif column_limit>1:column_limit=max(1,min(column_limit//2,max(len(t[2]) for t in tables)-1))
        else:
            for view in views:
                view['omitted_record_count']=view['record_count'];view['rows']=[]
            return result
