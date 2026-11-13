"""Identity-linked choices preserve the requested property through real review.

Only the external model/source and platform send are controlled. The first
answer deliberately supplies a roster, so independent review must recover the
missing properties rather than approve that adjacent result.
"""
import asyncio
import json
import os
from types import SimpleNamespace

import httpx
import pytest

from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
from tests.workflow_provider_protocol import workflow_provider_reply,fixture_source_assessment

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('chain',[False,True])
@pytest.mark.parametrize('producer',['raw','captured'])
@pytest.mark.parametrize('original_request,property_name,values',[
    ('出演者とプロの身長を調べて。','身長',['Aoi: 173 cm','Ren: 183 cm']),
    ('Busca las alturas de los participantes y sus parejas profesionales.','alturas',['Aoi: 173 cm','Ren: 183 cm']),
    ('Compare les poids des deux produits.','poids',['Aoi: 250 g','Ren: 430 g']),
])
def test_selected_goal_survives_restart_formatted_quote_and_wrong_roster(
        tmp_path,monkeypatch,surface,chain,producer,original_request,property_name,values,revision_mode=False):
    from fastapi.testclient import TestClient
    from nullion import chat_store,telegram_app,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _remember_chat_turn
    from nullion.config import load_settings
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.messaging_adapters import PlatformDeliveryReceipt,record_platform_delivery_receipt
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,GLOBAL_PERMISSION_PRINCIPAL,PolicyMode
    from datetime import datetime,UTC
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'absent.env'))
    conversation='telegram:123' if surface=='telegram' else 'web:objective'
    question='**対象は？**\n1. Aoi\n2. Aoi + Ren'
    _remember_chat_turn(runtime,chat_id='123' if surface=='telegram' else None,conversation_id=conversation,
        conversation_turn_id='root-goal',user_message=original_request,assistant_reply=question)
    parent='root-goal'
    if chain:
        question='**単位は？**\n1. A\n2. B'
        _remember_chat_turn(runtime,chat_id='123' if surface=='telegram' else None,conversation_id=conversation,
            conversation_turn_id='second-choice',user_message='2',assistant_reply=question)
        turn=runtime.store.get_conversation_turn('second-choice')
        turn.disposition=ConversationTurnDisposition.CONTINUE
        turn.parent_turn_id='root-goal'
        parent='second-choice'
    runtime.checkpoint(force=True)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-network',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    chat_store._store=None
    quote=question.replace('**','')
    # The platform receipt supplies the actual turn id despite Markdown/plain
    # display differences. This is the boundary from the production failure.
    if surface=='telegram':
        assert record_platform_delivery_receipt(PlatformDeliveryReceipt('parent-receipt','telegram','123',
            'delivered',True,0,False,request_id=parent,message_id='42',delivered_text=quote))
        _remember_chat_turn(runtime,chat_id='123',conversation_id=conversation,
            conversation_turn_id='unrelated-report',user_message='Unrelated scheduled report',assistant_reply='Other work.')
    reads=[];packets=[]
    tool_name='browser_run_task' if revision_mode else 'opaque_read'
    def external_source(req):
        output={'source_url':str(req.url),'text':
            'Aoi — Ren' if req.url.path=='/roster' else '\n'.join(values)}
        if req.url.path=='/properties':
            output.update(records=[{'subject':name,'display':value} for name,value in zip(['Aoi','Ren'],values)],
                source_record_collections=[{'path':['records'],'identity_fields':['subject']}])
        if producer=='captured':
            complete=req.url.path=='/properties'
            output.update(result_kind='source_observations',report_status='completed' if complete else 'partial',
                goal_status='answered' if complete else 'partial',next_action='report',completion_scope='tool_step',
                observations=[{'value':v,'quote':v,'source_url':str(req.url),'captured_at':100 if complete else 50,
                    'semantic_role':'attribute' if complete else 'entity'} for v in (values if complete else ['Aoi — Ren'])],
                unknowns=[] if complete else [property_name+' for Aoi and Ren has not been read.'])
        return httpx.Response(200,json=output)
    network=httpx.Client(transport=httpx.MockTransport(external_source))
    registry=ToolRegistry()
    def read(inv):
        phase=inv.arguments['phase'];reads.append(phase)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',network.get('https://source.example/'+phase).json())
    registry.register(ToolSpec(tool_name,'Read a selected source',ToolRiskLevel.LOW,
        (ToolSideEffectClass.WRITE if revision_mode else ToolSideEffectClass.READ),False,3,capability_tags=('source_step_context','explicit_scope_only'),input_schema={'type':'object',
        'properties':{'phase':{'enum':['roster','properties']}},'required':['phase']}),read)
    class Model:
        drafts=0
        def __init__(self):self.trace=[];self.proposals=[]
        def create(self,**kw):
            system=kw.get('system') or '';names={r['name'] for r in kw.get('tools') or ()}
            self.trace.append({'system':system[:70],'tools':sorted(names),'last':str(kw.get('messages',[])[-1:])[-2200:]})
            if 'Workflow action preflight.' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                phase=packet['proposed_action']['arguments']['phase'];self.proposals.append(phase)
                return text(json.dumps({'decision':'revise','issues':['名簿だけでなく身長を調べてください。']}
                    if phase=='roster' else {'decision':'proceed','issues':[]}))
            if 'material-requirements planner' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text']);packets.append(packet)
                assert packet['selected_request_context']['original_request']==original_request
                assert original_request in packet['original_request']
                return text(json.dumps({'search_coverage':{'scope':'comparison','constraints':[property_name],
                    'restricted_source_ids':[],'source_discovery_required':False},
                    'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':name+' '+property_name,'requirement_kind':'fact',
                        'evidence_subject_id':name,'eligible_evidence_tool_names':[tool_name]}
                        for name in ['Aoi','Ren']]}))
            if 'semantic completion reviewer' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text']);packets.append(packet)
                assert packet['selected_request_context']['original_request']==original_request
                complete=all(value in packet['draft_final_response'] for value in values)
                proofs={name:[r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']==tool_name
                    and r.get('output',{}).get('source_record_binding')
                    and (r['output'].get('record',{}).get('subject')==name
                        or any(row.get('value')==value for row in r['output'].get('observations',[])))]
                    for name,value in zip(['Aoi','Ren'],values)}
                return text(json.dumps({**fixture_source_assessment(packet,conclusion='comparison',sufficient=complete),
                    'source_presentation_valid':True if packet.get('source_answer_selection') else None,
                    'disposition':'complete' if complete else 'retry','all_material_requirements_enumerated':True,
                    'unresolved_requirements':[] if complete else [property_name+' for Aoi and Ren is missing'],
                    'retry_tool_names':[] if complete else [tool_name],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':complete,
                        'draft_covered':complete,'evidence_invocation_ids':proofs.get(r['evidence_subject_id'],[])}
                        for r in packet['material_requirement_contract']]}))
            if 'present_source_answer' in names:
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                context=json.loads(raw);complete='properties' in reads
                selected=[r for r in context['evidence'] if r['value'] in (values if complete else ['Aoi — Ren'])]
                assert selected
                self.drafts=1
                return call('present_source_answer',{'intro':'','focus_roles':['entity','attribute'],
                    'points':[{'semantic_role':'attribute' if complete else 'entity','evidence_ids':[r['id']],
                        'detail_ids':[],'relevance':'Selected subjects and requested properties'} for r in selected],
                    'limitation_ids':[] if complete else [r['id'] for r in context['limitations'][:1]]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':None,'confidence':1})
            if 'relationship' in system:return text(json.dumps({'relationship':'follow_up','effect':'continue',
                'confidence':1,'target_index':0,'target_is_explicit':False}))
            if 'request_tool_scope' in names and tool_name not in names:
                return call('request_tool_scope',{'capabilities':['web'],'tool_names':[tool_name]})
            if not names:return text('{}')
            assert original_request in json.dumps(kw['messages'],ensure_ascii=False)
            if not reads:return call(tool_name,{'phase':'properties' if revision_mode else 'roster'})
            if revision_mode:return text('\n'.join(values))
            if len(reads)==1:return call(tool_name,{'phase':'properties'})
            return text('\n'.join(values))
    model=Model();orchestrator=AgentOrchestrator(model_client=model)
    runtime.active_tool_registry=registry
    if surface=='web':
        with TestClient(web_app.create_app(runtime,orchestrator=orchestrator,registry=registry)) as api:
            response=api.post('/api/chat',json={'text':'/chat 2','conversation_id':conversation})
            assert response.status_code==200,response.text
            reply=response.json()['text']
    else:
        settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
            'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
        service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,
            operator_chat_id='123',model_client=model,agent_orchestrator=orchestrator)
        sent=[]
        class Message:
            text='2';message_id=500
            reply_to_message=SimpleNamespace(message_id=42,text=quote,caption=None,
                from_user=SimpleNamespace(is_bot=True,username='fixture'))
            chat=SimpleNamespace(id=123)
            async def reply_text(self,value,**kwargs):
                sent.append(value);return SimpleNamespace(message_id=501+len(sent))
        Message.chat.send_action=lambda **kw:asyncio.sleep(0)
        asyncio.run(service.on_message(SimpleNamespace(update_id=500,message=Message()),SimpleNamespace(bot=None)))
        reply=next((value for value in sent if all(v in value for v in values)),str(sent))
    network.close()
    assert all(value in reply for value in values),json.dumps({'reply':reply,'reads':reads,'trace':model.trace[-5:],
        'tools':[r for e in runtime.store.list_recent_conversation_events(conversation,limit=100)
            if e.get('event_type')=='conversation.chat_turn' for r in (e.get('tool_results') or [])[-3:]],
        'approvals':[{'action':r.action,'resource':r.resource,'context':r.context}
            for r in runtime.store.list_approval_requests()]},ensure_ascii=False)
    assert 'Other relevant sources remain unchecked' not in reply and 'incomplete' not in reply
    assert reads==(['properties'] if revision_mode else ['roster','properties'])
    if revision_mode:assert model.proposals == []
    events=runtime.store.list_recent_conversation_events(conversation,limit=100)
    turn=next(r for r in reversed(events) if r.get('event_type')=='conversation.chat_turn')
    assert all(value in turn['assistant_reply'] for value in values)
    assert turn['parent_turn_id']==parent
    reviews=[r for r in events if r.get('event_type')=='conversation.completion_review']
    assert not reviews
