"""Compaction keeps the selected target, not an arbitrary list prefix."""
import json
from types import SimpleNamespace

import pytest

from nullion.plugins.browser_plugin.browser_tools import BrowserTools
from nullion.tools import ToolInvocation, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass
from nullion.workflow_reasoning import action_review_evidence, assess_action


def adapter():
    return BrowserTools(backend=SimpleNamespace(BACKEND_NAME='playwright'),pool=None,policy=None)


def snapshot():
    rows=[{'element_id':f'n-{i}','role':'link','text':f'Navigation {i}'} for i in range(1,400)]
    rows.append({'element_id':'n-400','role':'link','tag':'a','text':'送信について',
        'href':'https://source.example/feedback','form_action':'','native_type':'','visible':True})
    return ToolResult('snapshot','browser_snapshot','completed',{'session_id':'one',
        'snapshot':{'url':'https://source.example/','elements':rows}})


@pytest.mark.parametrize('arguments',[{'element_id':'n-400'},{'role':'link','name':'送信について'}])
def test_exact_target_survives_action_preview_compaction_without_io(arguments):
    tools=adapter();rows=[snapshot()];inv=ToolInvocation('click','browser_click_id','web:one',arguments,
        flow_context={'browser_session_id':'one'})
    preview=action_review_evidence(rows)
    assert 'n-400' not in json.dumps(preview)
    spec=ToolSpec(inv.tool_name,'Interact with selected control',ToolRiskLevel.MEDIUM,
        ToolSideEffectClass.WRITE,False,10,action_review_context=tools.action_review_context)
    class Provider:
        def create(self,**kw):
            packet=json.loads(kw['messages'][0]['content'][0]['text'])
            context=packet['adapter_target_evidence']['context']
            assert context['source_invocation_id']=='snapshot' and context['match_count']==1
            assert context['target']['element_id']=='n-400'
            assert context['target']['href']=='https://source.example/feedback'
            return {'content':[{'type':'text','text':json.dumps({'decision':'proceed','issues':[],
                'issue_kind':'none','step_effect':'observation'})}]}
    review=assess_action(model_client=Provider(),request='確認用のフォームを開いて。',context={},
        invocation=inv,spec=spec,evidence=preview,retained_results=rows,now='2026-11-03T00:00:00Z')
    assert review.decision=='proceed' and not review.error


@pytest.mark.parametrize('kind',['changed_page','foreign_session','unknown_id','duplicate_id'])
def test_target_projection_never_substitutes_stale_foreign_or_ambiguous_controls(kind):
    tools=adapter();row=snapshot();rows=[row];ident='n-400'
    if kind=='changed_page':rows.append(ToolResult('nav','browser_navigate','completed',{'session_id':'one'}))
    if kind=='foreign_session':row.output['session_id']='other'
    if kind=='unknown_id':ident='not-observed'
    if kind=='duplicate_id':row.output['snapshot']['elements'].append(dict(row.output['snapshot']['elements'][-1]))
    context=tools.action_review_context(ToolInvocation('click','browser_click_id','web:one',{'element_id':ident},
        flow_context={'browser_session_id':'one'}),rows)
    assert context['status']!='observed' and not context.get('target')


@pytest.mark.parametrize('mutated',[False,True])
def test_prior_exact_inspections_reach_action_review_without_being_sampled_again(mutated):
    from nullion.workflow_reasoning import inspect_review_evidence, remember_review_inspections
    fact='A receipt was supplied: eleven sauces, order 05B33.'
    rows=[ToolResult('receipt','opaque_read','completed',{'body':'x'*4000+fact+'y'*4000})]
    windows=inspect_review_evidence([{'invocation_id':'receipt','json_pointer':'/body','offset':4000,'length':len(fact)}],
        results=rows,references={},visible_ids={'receipt'})
    cache={};remember_review_inspections(cache,windows,results=rows,references={})
    if mutated:rows[0].output['body']='Changed source'
    class Provider:
        def create(self,**kw):
            p=json.loads(kw['messages'][0]['content'][0]['text'])
            assert fact not in json.dumps(p['execution_evidence'])
            assert bool(p.get('previously_inspected_evidence')) is not mutated
            if not mutated:assert p['previously_inspected_evidence'][0]['content']==fact
            return {'content':[{'type':'text','text':json.dumps({'decision':'proceed','issues':[]})}]}
    result=assess_action(model_client=Provider(),request='Prepare the requested notice.',context={},
        invocation=ToolInvocation('prepare','opaque_action','web:one',{}),
        spec=ToolSpec('opaque_action','Prepare a notice',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),
        evidence=action_review_evidence(rows),retained_results=rows,inspection_cache=cache,now='2026-11-03T00:00:00Z')
    assert result.decision=='proceed' and not result.error
