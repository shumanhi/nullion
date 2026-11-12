"""Auxiliary provider replies for execution-focused controller fixtures.

These tests script the external model, not Nullion's review or executor. Their
single-outcome fixture claims are still checked by the real runtime grounding
checks. Tests of review decisions supply their own explicit provider responses.
"""
import json
import re


def is_tool_scope_request(kwargs):
    """Identify the scope packet without depending on an output-token budget."""
    if kwargs.get('tools'):
        return False
    try:
        packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
    except (KeyError, IndexError, TypeError, ValueError):
        return False
    return (isinstance(packet, dict) and packet.get('surface') == 'ordinary_chat'
            and isinstance(packet.get('user_turn'), str)
            and isinstance(packet.get('registered_special_tool_names'), list)
            and isinstance(packet.get('available_special_tool_scopes'), list))


def fixture_action_acknowledgement(kwargs):
    """Script the fixture producer's confirmation from accepted tool receipts."""
    summaries = []
    for message in reversed(kwargs.get('messages', [])):
        content = message.get('content', [])
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict) or block.get('type') != 'tool_result':
                continue
            for row in block.get('content', []):
                if row.get('type') != 'text':
                    continue
                try:
                    result, _ = json.JSONDecoder().raw_decode(row['text'])
                except (ValueError, KeyError, TypeError):
                    continue
                output = result.get('output') or {}
                receipt = output.get('action_receipt') or {}
                if result.get('status') == 'completed' and receipt.get('type') == 'action_receipt':
                    summary = receipt.get('summary')
                    if isinstance(summary, str) and summary not in summaries:
                        summaries.append(summary)
    return '\n\n'.join(reversed(summaries))


def workflow_provider_reply(kwargs, *, completion=True, reasoned=False, action=False):
    system = str(kwargs.get('system') or '')
    def reply(value):
        return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': json.dumps(value)}]}
    if 'Workflow action preflight.' in system:
        return reply({'decision': 'proceed', 'issues': []})
    if not completion:
        return None
    if 'material-requirements planner' in system:
        packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
        tools = [row['name'] for row in packet['completed_current_turn_tools']]
        extensions = list(dict.fromkeys(re.findall(r'\.(?:pdf|docx|pptx|xlsx|csv|tsv|html|zip)\b', packet['original_request'], re.I)))
        coverage = {'search_coverage': {'scope': 'find_any', 'constraints': [], 'restricted_source_ids': []}} if packet.get('discovered_sources') else {}
        return reply({**coverage, 'all_material_requirements_enumerated': True,
            'requested_artifact_extensions': extensions,
            'material_requirements': [{'requirement': packet['original_request'] + (' (' + ext + ')' if ext else ''),
                'requirement_kind': 'deliverable' if ext else 'reasoned_answer' if reasoned else 'action' if action else 'other',
                'evidence_subject_id': ext or 'fixture-outcome', 'eligible_evidence_tool_names': [] if reasoned and not ext else tools}
                for ext in extensions or ['']]})
    if 'semantic completion reviewer' in system:
        packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
        evidence = packet['tool_evidence']
        def record_output(record):
            value = record.get('output')
            return value if isinstance(value, dict) else {}
        coverage = []
        for row in packet['material_requirement_contract']:
            eligible = [record for record in evidence if record['status'] == 'completed'
                        and record['tool_name'] in row['eligible_evidence_tool_names']]
            if row['requirement_kind'] == 'deliverable':
                bound = [record for record in evidence if record['status'] == 'completed'
                    and record_output(record).get('artifact_record_binding')
                    and str(record_output(record).get('artifact_path') or '').lower().endswith(row['evidence_subject_id'])]
                if bound:
                    eligible = bound
            if row['requirement_kind'] != 'deliverable':
                parents = [record for record in eligible if not record_output(record).get('source_record_binding')
                    and not record_output(record).get('artifact_record_binding')]
                if parents: eligible = parents
            if row['requirement_kind'] == 'reasoned_answer': eligible = []
            coverage.append({'requirement_id': row['requirement_id'], 'satisfied': bool(eligible) or row['requirement_kind'] == 'reasoned_answer',
                'draft_covered': True, 'evidence_invocation_ids': [record['invocation_id'] for record in eligible[-1:]]})
        search = {}
        context = packet.get('search_coverage_context')
        if context:
            # Execution fixtures return a positive finding. Dedicated coverage
            # contracts below independently script incomplete/negative searches.
            positive = [r['invocation_id'] for r in packet['citable_evidence']]
            search = {'search_coverage': {'conclusion': 'positive', 'coverage_sufficient': True, 'scope_matches_request': True,
                'discovery_evidence_invocation_ids': context.get('discovery_invocation_ids', []), 'match_evidence_invocation_ids': positive[-1:], 'constraints': [],
                'candidates': [{'candidate_id': r['candidate_id'], 'disposition': 'pending',
                    'reason': 'The fixture already found its requested positive result.',
                    'evidence_invocation_ids': []} for r in context['candidates']]}}
        return reply({**search, 'disposition': 'complete', 'unresolved_requirements': [], 'retry_tool_names': [],
            'all_material_requirements_enumerated': True, 'material_requirements': coverage})
    return None


def group_provider_reply(kwargs, raw_response):
    """Adapt an execution fixture's answer to the combined-review protocol."""
    if 'You are summarizing the results' not in str(kwargs.get('system') or ''):
        return None
    packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
    answer = ''.join(row.get('text', '') for row in raw_response.get('content', ()) if row.get('type') == 'text')
    return {'content': [{'type': 'text', 'text': json.dumps({'status': 'complete', 'answer': answer,
        'evaluated_task_ids': [row['task_id'] for row in packet['task_results']], 'missing_outcomes': []})}]}


def fixture_source_plan(packet, *, scope='find_any'):
    """Explicit source scope for a scripted external provider fixture."""
    ledger = packet.get('discovered_sources')
    if not ledger:
        return {}
    return {'search_coverage': {'scope': scope, 'constraints': [],
        'restricted_source_ids': [r['candidate_id'] for r in ledger['candidates']]
            if scope == 'restricted' else []}}


def fixture_source_assessment(packet, *, conclusion='positive', sufficient=True):
    context = packet.get('search_coverage_context')
    if not context:
        return {}
    refs = [r['invocation_id'] for r in packet['citable_evidence']]
    return {'search_coverage': {'conclusion': conclusion, 'coverage_sufficient': sufficient,
        'scope_matches_request': True, 'match_evidence_invocation_ids': refs[-1:]
            if conclusion == 'positive' or context['plan']['scope'] == 'evidence_summary' else [],
        'discovery_evidence_invocation_ids': context.get('discovery_invocation_ids', []),
        'constraints': [{'constraint': c, 'verified': sufficient, 'evidence_invocation_ids': refs[-1:]}
            for c in context['plan']['constraints']],
        'candidates': [{'candidate_id': r['candidate_id'],
            'disposition': 'checked' if r['observation_invocation_ids'] else 'pending',
            'reason': 'The scripted fixture observed this source.' if r['observation_invocation_ids'] else 'Not observed.',
            'evidence_invocation_ids': r['observation_invocation_ids'] or r['discovery_invocation_ids']}
            for r in context['candidates']]}}
