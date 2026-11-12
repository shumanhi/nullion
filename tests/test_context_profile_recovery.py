import json
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from nullion.retained_work_context import producer_context, inspect_retained, replace_runtime_context
from nullion.tools import ToolResult


def test_repeated_nonexecuted_failures_compact_below_size_ceiling_without_losing_facts():
    results, messages, metadata = [], [], {}
    for i in range(7):
        output = {'reason': 'invalid_tool_arguments', 'executed': False,
            'validation': {'code': 'invalid_json', 'argument_sha256': str(i), 'error_offset': i},
            'recovery': {'kind': 'correct_arguments', 'argument_constraints': {'type': 'object'},
                'instruction': 'Complete the registered schema. ' * 50}}
        result = ToolResult(str(i), 'opaque_tool', 'failed', output)
        results.append(result)
        metadata[str(i)] = {'tool_use_id': 'u'+str(i)}
        messages.extend([{'role': 'assistant', 'content': [{'type': 'tool_use', 'id': 'u'+str(i),
            'name': 'opaque_tool', 'input': {}}]}, {'role': 'user', 'content': [{'type': 'tool_result',
            'tool_use_id': 'u'+str(i), 'content': [{'type': 'text', 'text': json.dumps({'output': output})}]}]}])
    original = json.dumps(messages)
    assert len(original) < 64000
    view, available = producer_context(messages, results, metadata)
    assert available and len(json.dumps(view)) < len(original) * .65
    assert json.dumps(messages) == original
    assert view[-2] == messages[-1]  # fresh batch remains complete
    assert [b['content'][0]['tool_use_id'] for b in view[:-1] if b['role']=='user'] == ['u'+str(i) for i in range(7)]
    assert inspect_retained({'requests': [{'invocation_id':'0','json_pointer':'/validation/error_offset',
        'offset':0,'length':10}]}, results)['windows'][0]['content'] == '0'
    assert len({r.output['validation']['argument_sha256'] for r in results}) == 7


@pytest.mark.parametrize('variant', ['executed','constraint','reason','approval','success'])
def test_distinct_contracts_and_action_receipts_are_not_collapsed(variant):
    from copy import deepcopy
    output = {'reason':'invalid_tool_arguments','executed':False,'validation':{'code':'invalid_json'},
        'recovery':{'kind':'correct_arguments','argument_constraints':{'type':'object'}}}
    other = deepcopy(output)
    status='failed'
    if variant=='executed': other['executed']=True
    if variant=='constraint': other['recovery']['argument_constraints']={'required':['id']}
    if variant=='reason': other['reason']='permission_required'
    if variant=='approval': status='pending_approval'
    if variant=='success': status='completed'
    results=[ToolResult('a','opaque','failed',output),ToolResult('b','opaque',status,other)]
    messages=[{'role':'user','content':[{'type':'tool_result','tool_use_id':r.invocation_id,
        'content':[{'type':'text','text':json.dumps({'output':r.output})}]}]} for r in results]
    view,available=producer_context(messages,results,{r.invocation_id:{'tool_use_id':r.invocation_id} for r in results})
    assert view is messages and not available


def test_runtime_checkpoint_replacement_requires_position_and_exact_digest():
    original={'role':'user','content':'Runtime recovery checkpoint 私の依頼'}
    first={'role':'user','content':'runtime-owned-one'}
    second={'role':'user','content':'runtime-owned-two'}
    messages,receipt=replace_runtime_context([original],None,first)
    messages.append({'role':'assistant','content':'work'})
    view,new_receipt=replace_runtime_context(messages,receipt,second)
    assert original in view and first not in view and view[-1]==second
    assert len(view)==3 and len(messages)==3
    stale={**receipt,'sha256':'wrong'}
    view,_=replace_runtime_context(messages,stale,second)
    assert first in view and original in view
    assert new_receipt['index']==2


@dataclass
class BudgetClient:
    max_tokens: int | None = None
    model: str = 'fixture'
    reasoning_effort: str = 'high'
    def create(self, **kwargs):
        return {'stop_reason':'end_turn','content':[{'type':'text','text':'ok'}]}


@pytest.mark.parametrize('module_name',['chat_operator','web_app'])
def test_fast_profile_preserves_original_client_and_explicit_workflow_budget(monkeypatch,module_name):
    from nullion import chat_operator, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_MESSAGING_INTERACTIVE_FAST_MAX_TOKENS','768')
    monkeypatch.setenv('NULLION_WEB_INTERACTIVE_FAST_MAX_TOKENS','768')
    module=chat_operator if module_name=='chat_operator' else web_app
    client=BudgetClient(12000)
    original=AgentOrchestrator(model_client=client)
    fast=module._orchestrator_with_interactive_fast_profile(original,enabled=True,tool_registry=ToolRegistry())
    assert fast.model_client.max_tokens==768
    assert fast.workflow_model_client is client
    assert client.max_tokens==12000 and client.reasoning_effort=='high'
    again=module._orchestrator_with_interactive_fast_profile(fast,enabled=True,tool_registry=ToolRegistry())
    assert again.workflow_model_client is client


@pytest.mark.parametrize('prompt',['Continue','続けて','تابع'])
@pytest.mark.parametrize('first_truncated',[True,False])
def test_first_response_truncation_promotes_once_and_plain_chat_stays_fast(prompt,first_truncated):
    from nullion.agent_orchestrator import AgentOrchestrator, _agent_turn_model_node
    from nullion.tools import ToolRegistry
    calls=[]
    class Client:
        def __init__(self,budget): self.max_tokens=budget
        def create(self,**kwargs):
            calls.append(self.max_tokens)
            if self.max_tokens==768 and first_truncated:
                return {'stop_reason':'tool_use','completion':{'status':'incomplete','reason':'max_output_tokens'},
                    'content':[{'type':'tool_use','name':'opaque','id':'cut','input':{},'input_error':{'code':'invalid_json'}}]}
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'回答'}]}
    normal=Client(None)
    state={'orchestrator':AgentOrchestrator(model_client=Client(768),workflow_model_client=normal),
        'tool_registry':ToolRegistry(),'user_message':prompt,'messages':[{'role':'user','content':prompt}],
        'tool_results':[],'artifacts':[],'conversation_id':'test','principal_id':'test','max_iterations':10}
    update=_agent_turn_model_node(state)
    assert calls==([768,None] if first_truncated else [768])
    assert update['workflow_profile_required'] is first_truncated
    assert update['content']==[{'type':'text','text':'回答'}]
    if first_truncated:
        _agent_turn_model_node({**state,**update})
        assert calls==[768,None,None], 'Promotion must persist without another fast failure'


def test_large_consumed_sources_compact_early_but_new_evidence_and_exact_values_survive():
    from tests.test_retained_work_context import history
    messages,results,metadata=history(2)
    before=json.dumps(messages)
    view,available=producer_context(messages,results,metadata)
    assert len(before)<64000 and available
    assert len(json.dumps(view))<len(before)
    assert 'EXACT=1' in json.dumps(view)  # newest source has not been consumed
    assert inspect_retained({'requests':[{'invocation_id':'owned-0','json_pointer':'/body',
        'offset':5000,'length':7}]},results)['windows'][0]['content']=='EXACT=0'
    assert json.dumps(messages)==before


@pytest.mark.asyncio
@pytest.mark.parametrize('limit',[None,9000])
async def test_langchain_inherits_configured_budget_instead_of_adding_a_generation_ceiling(limit):
    from langchain_core.messages import HumanMessage
    from nullion.langchain_adapters import nullion_client_as_langchain_chat_model
    calls=[]
    class Client:
        max_tokens=12000
        def create(self,**kwargs):
            calls.append(kwargs)
            return {'content':[{'type':'text','text':'回答'}]}
    model=nullion_client_as_langchain_chat_model(Client(),default_max_tokens=limit)
    response=await model.ainvoke([HumanMessage(content='回答')])
    assert response.content=='回答'
    assert calls[0]['max_tokens']==limit


def test_shared_security_policies_reduce_small_context_without_shortening_source_or_new_batch():
    from nullion.agent_orchestrator import _tool_result_message_payload
    results=[ToolResult(str(i),'opaque_read','completed',{'value':i,'body':'根拠 '*120}) for i in range(3)]
    messages=[{'role':'user','content':[{'type':'tool_result','tool_use_id':r.invocation_id,
        'content':[{'type':'text','text':_tool_result_message_payload(r)}]}]} for r in results]
    original=json.dumps(messages)
    view,available=producer_context(messages,results,{r.invocation_id:{'tool_use_id':r.invocation_id} for r in results})
    assert not available  # no inspection needed: every fact stays inline
    assert len(json.dumps(view))<len(original)
    policies=json.loads(view[-1]['content'][0]['text'].split('\n',1)[1])['security_policies']
    for old,new in zip(messages,view):
        old_payload=json.loads(old['content'][0]['content'][0]['text'])
        payload=json.loads(new['content'][0]['content'][0]['text'])
        policy=policies[payload['security']['policy_ref']]
        restored={k:v for k,v in payload['security'].items() if k!='policy_ref'}
        restored.update(policy)
        assert restored==old_payload['security']
        assert payload['output']==old_payload['output']
        assert payload['untrusted_output_boundary']==old_payload['untrusted_output_boundary']
    assert json.dumps(messages)==original


@pytest.mark.parametrize('same_receipt',[False,True])
def test_ambiguous_protocol_ids_never_hide_all_copies_of_document_evidence(same_receipt):
    from tests.test_retained_work_context import history
    messages,results,metadata=history(12)  # force the broad archival path too
    references=[]
    for i in range(2):
        ident='document' if same_receipt else 'document-'+str(i)
        output={'artifact_refs':[{'path':'/opaque/旅程.pdf','turn_id':'original-document'}],
            'body':'older source '*1500}
        if not same_receipt or i==0:
            results.append(ToolResult(ident,'opaque_history','completed',output))
            metadata[ident]={'tool_use_id':'provider-reused-id'}
        block={'type':'tool_result','tool_use_id':'provider-reused-id',
            'content':[{'type':'text','text':json.dumps({'output':output})}]}
        references.append(block)
        messages.extend([{'role':'assistant','content':[{'type':'tool_use','id':'provider-reused-id',
            'name':'opaque_history','input':{'index':i}}]}, {'role':'user','content':[block]}])
    messages.append({'role':'assistant','content':[{'type':'text','text':'続けて'}]})
    view,available=producer_context(messages,results,metadata)
    assert available
    visible=[b for m in view if isinstance(m.get('content'),list) for b in m['content']
        if b.get('type')=='tool_result' and b.get('tool_use_id')=='provider-reused-id']
    assert visible==references
    assert all('original-document' in json.dumps(b) for b in visible)


def test_distinct_cached_protocol_ids_keep_one_complete_visible_receipt():
    output={'artifact_refs':[{'path':'/opaque/旅程.pdf','turn_id':'original-document'}]}
    result=ToolResult('owned','opaque_history','completed',output)
    messages=[]
    for use in ['first','second']:
        messages.extend([{'role':'assistant','content':[]},{'role':'user','content':[{
            'type':'tool_result','tool_use_id':use,'content':[{'type':'text','text':json.dumps({'output':output})}]}]}])
    messages.append({'role':'assistant','content':[]})
    view,available=producer_context(messages,[result],{'owned':{'tool_use_id':'first','reused_tool_use_ids':['second']}})
    assert available
    payloads=[json.loads(b['content'][0]['text']) for m in view if isinstance(m.get('content'),list)
        for b in m['content'] if b.get('type')=='tool_result']
    assert payloads[0]['model_context_compaction']['same_contract_as']['tool_use_id']=='second'
    assert payloads[1]['output']==output


def test_full_ambiguous_receipts_do_not_add_an_unneeded_inspection_tool_on_resume():
    output={'value':51.77,'next_tool':'opaque_lookup'}
    results=[ToolResult('a','opaque_read','completed',output)]
    metadata={'a':{'tool_use_id':'reused'}}
    block={'type':'tool_result','tool_use_id':'reused','content':[{'type':'text','text':json.dumps({'output':output})}]}
    messages=[{'role':'user','content':[block]},{'role':'assistant','content':[]},
        {'role':'user','content':[block]}]
    view,available=producer_context(messages,results,metadata,include_unmapped=True)
    assert view is messages and not available
    # Receipts without a materialized ownership binding still need inspection context.
    view,available=producer_context([{'role':'user','content':'続けて'}],results,{},include_unmapped=True)
    assert available and 'opaque_read' in json.dumps(view)


def test_materialized_frame_receipts_keep_followup_tool_scope_unchanged():
    result=ToolResult('owned','opaque_read','completed',{'value':51.77,'next_tool':'opaque_lookup'})
    messages=[{'role':'user','content':[{'type':'text','text':json.dumps({
        'retained_tool_results':[{'invocation_id':'owned','output':result.output}]})}]}]
    view,available=producer_context(messages,[result],{'owned':{'tool_use_id':'prior-read'}},include_unmapped=True)
    assert view is messages and not available
    assert '51.77' in json.dumps(view)
