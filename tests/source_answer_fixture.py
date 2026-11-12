"""Structured fake composer for tests whose requested subjects are captured records."""
import json


def source_presentation_response(kwargs):
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    message = kwargs['messages'][0]['content'][0]['text']
    raw = message.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n', 1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END, 1)[0]
    context = json.loads(raw)
    points = []
    for row in context['evidence'][:7]:
        roles = {binding['role'] for binding in row.get('semantic_bindings', ())}
        if roles == {'interface_control'}:
            continue
        points.append({'semantic_role': 'entity', 'evidence_ids': [row['id']], 'detail_ids': [],
            'request_relation': 'result', 'summary': row['value'],
            'relevance': 'A requested captured comparison record'})
    limits = [row['id'] for row in context['limitations'][:3]]
    draft = {'intro': '', 'focus_roles': ['entity'], 'points': points,
        'answer_status': 'partial_results' if limits else 'verified_results', 'limitation_ids': limits}
    return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'present',
        'name': 'present_source_answer', 'input': draft}]}
