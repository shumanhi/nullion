"""Bounded model views over retained source collections; no source is modified."""
from copy import deepcopy
import json


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
            'history_projection':{'original_text_chars':len(d['text']),
                'text_truncated':len(d['text'])>width,'original_link_count':len(d['links']),
                'omitted_link_count':len(d['links']),
                'omitted_fields':[k for k in d if k not in {'url','text','links'}]}}
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


def compact_collections(output, *, budget=14000, preserve_identities=False):
    if not isinstance(output,dict) or len(json.dumps(output,ensure_ascii=False))<=budget:
        return output
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
        tables.append((pointer,rows,fields,declaration.get('cardinality')=='one',
                       {key for key in (identities or ()) if isinstance(key, str)}))
    if not tables:return output
    width=240;column_limit=1000
    while True:
        views=[]
        for pointer,rows,fields,one,identities in tables:
            shown_fields=fields[:column_limit]
            values=[];clipped=False
            for row in rows:
                cells=[]
                for key in shown_fields:
                    value=row.get(key) if isinstance(row,dict) else None
                    if isinstance(value,(dict,list)):value=json.dumps(value,ensure_ascii=False,separators=(',',':'))
                    if isinstance(value,str) and len(value)>width and not (preserve_identities and key in identities):
                        value={'preview':value[:width],'truncated':True};clipped=True
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
