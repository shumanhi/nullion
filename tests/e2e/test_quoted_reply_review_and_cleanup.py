"""Real Telegram ingress/review and real cron terminal execution boundaries."""
import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
import time
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E only')

def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
def call(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name+'-id','name':name,'input':args}]}

def isolate(tmp_path,monkeypatch):
    from nullion import crons,chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    for key in ['NULLION_HOME','NULLION_DATA_DIR','NULLION_WORKSPACE_STORAGE_ROOT']:
        monkeypatch.setenv(key,str(tmp_path/key))
    for key in ['NULLION_MEMORY_ENABLED','NULLION_ACTIVITY_TRACE_ENABLED']:monkeypatch.setenv(key,'false')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'runtime.db');chat_store._store=None
    return bootstrap_persistent_runtime(tmp_path/'runtime.db')

@pytest.mark.parametrize('prompt,quote',[
    ('Find me amazon links','Recommended products: Alpha powder and Beta tablets.'),
    ('購入リンクを探して','おすすめはアルファ粉末とベータ錠剤です。'),
    ('Busca los enlaces','Recomiendo polvo Alfa y tabletas Beta.'),
])
@pytest.mark.parametrize('explicit',[True,False])
def test_telegram_quote_survives_scope_requirement_review_and_final_delivery(tmp_path,monkeypatch,prompt,quote,explicit):
    from nullion import telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from nullion.policy import BoundaryKind,GLOBAL_PERMISSION_PRINCIPAL,BoundaryPolicyRule,PolicyMode
    from datetime import datetime,UTC
    runtime=isolate(tmp_path,monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('network',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    registry.register(ToolSpec('web_fetch','Read listing',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,capability_tags=('web',)),
        lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{
            'url':'https://example.test/products','text':quote+' https://example.test/alpha https://example.test/beta',
            'result_kind':'source_observations','goal_status':'partial','report_status':'partial',
            'observations':[{'label':'Product links','value':quote,'quote':quote+' https://example.test/alpha https://example.test/beta','source_index':0}],
            'sources':[{'url':'https://example.test/products','text':quote}]}))
    from nullion.chat_operator import _remember_chat_turn
    _remember_chat_turn(runtime,chat_id='123',conversation_id='telegram:123',user_message='Recommend products',
        assistant_reply=quote,conversation_turn_id='quoted-parent')
    payloads=[]
    class Model:
        phase=0
        def create(self,**kw):
            system=str(kw.get('system') or '');names={tool['name'] for tool in kw.get('tools',[])}
            if ('allowed_target_indexes' in json.dumps(kw.get('messages',[]))
                    or ('relationship' in system and 'target_index' in system)):
                return text(json.dumps({'relationship':'follow_up','effect':'continue','confidence':1,'target_index':0,'target_is_explicit':False}))
            if 'material-requirements planner' in system:
                payload=json.loads(kw['messages'][0]['content'][0]['text']);payloads.append(payload)
                assert quote in json.dumps(payload['request_context'],ensure_ascii=False)
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Links for the referenced products','requirement_kind':'fact','evidence_subject_id':'products',
                     'eligible_evidence_tool_names':['web_fetch']}]}))
            if 'semantic completion reviewer' in system:
                payload=json.loads(kw['messages'][0]['content'][0]['text']);payloads.append(payload)
                assert quote in json.dumps(payload['request_context'],ensure_ascii=False)
                evidence=json.dumps(payload['tool_evidence'])
                import re
                # Copy the runtime-generated invocation id from its structured evidence.
                def refs(value):
                    if isinstance(value,dict):
                        if value.get('tool_name')=='web_fetch' and value.get('invocation_id'):return [value['invocation_id']]
                        return [r for v in value.values() for r in refs(v)]
                    if isinstance(value,list):return [r for v in value for r in refs(v)]
                    return []
                ids=refs(payload['tool_evidence']);assert ids,evidence
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,
                    'unresolved_requirements':[],'retry_tool_names':[], 'material_requirements':[
                    {**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids}
                    for row in payload['material_requirement_contract']]}))
            if 'present_source_answer' in names:
                assert 'request_context' in json.dumps(kw['messages'])
                tool=next(tool for tool in kw['tools'] if tool['name']=='present_source_answer')
                schema=tool['input_schema']['properties']['points']['items']['properties']
                identity=schema['evidence_ids']['items']['enum'][0]
                return call('present_source_answer',{'intro':'Links for the quoted products',
                    'points':[{'evidence_ids':[identity],'detail':'https://example.test/alpha https://example.test/beta',
                        'relevance':'These are the referenced product links.'}],'limitation_ids':[]})
            if not names:return text('{}')
            if 'web_fetch' not in names:return call('request_tool_scope',{'capabilities':['web'],'tool_names':['web_fetch']})
            self.phase+=1
            if self.phase==1:
                assert quote in json.dumps(kw['messages'],ensure_ascii=False)
                return call('web_fetch',{'url':'https://example.test/products'})
            return text('Alpha: https://example.test/alpha\nBeta: https://example.test/beta')
    model=Model();runtime.active_tool_registry=registry
    settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,
        operator_chat_id='123',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
    sent=[]
    class Message:
        text=prompt;message_id=500
        reply_to_message=SimpleNamespace(message_id=42,text=quote,caption=None,from_user=SimpleNamespace(is_bot=True,username='fixture')) if explicit else None
        chat=SimpleNamespace(id=123)
        async def reply_text(self,value,**kwargs):sent.append(value);return SimpleNamespace(message_id=501+len(sent))
    Message.chat.send_action=lambda **kwargs:asyncio.sleep(0)
    asyncio.run(service.on_message(SimpleNamespace(update_id=500,message=Message()),SimpleNamespace(bot=None)))
    assert any('https://example.test/alpha' in message and 'https://example.test/beta' in message for message in sent),sent
    assert len(payloads)>=2,'The previously failing completion-review boundary must execute'
    events=list(runtime.store.list_recent_conversation_events('telegram:123'))
    assert any('https://example.test/alpha' in str(row.get('assistant_reply')) for row in events)
    if explicit:
        assert any(row.get('reply_context',{}).get('reply_to_message_id')==42 for row in events)
        assert any(row.get('user_message')==prompt and row.get('parent_turn_id')=='quoted-parent' for row in events),events


@pytest.mark.parametrize('personal',[False,True])
def test_scheduled_terminal_source_cleanup_does_not_ask_or_delete(tmp_path,monkeypatch,personal):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_terminal_exec_handler
    from nullion.policy import BoundaryKind,GLOBAL_PERMISSION_PRINCIPAL,BoundaryPolicyRule,PolicyMode
    from datetime import datetime,UTC
    runtime=isolate(tmp_path,monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('files',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    registry.register(ToolSpec('terminal_exec','Run terminal',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,15),
        _build_terminal_exec_handler(workspace_root=tmp_path,allowed_roots=(tmp_path,)))
    class Model:
        phase=0
        def create(self,**kw):
            self.phase+=1
            if self.phase==1:return call('terminal_exec',{'command':"python -c 'print(\"x\" * 6000)'",'network_mode':'inherit'})
            if self.phase==2:
                content=kw['messages'][-1]['content']
                raw=next(block['content'] for block in content if block.get('type')=='tool_result')
                if isinstance(raw,list):raw=''.join(block.get('text','') for block in raw)
                result=json.loads(raw);result=result.get('output',result)
                self.path=next(item['path'] for item in result['artifact_descriptors'] if item['kind']=='terminal_output')
                target=str(tmp_path/'personal.txt') if personal else self.path
                return call('terminal_exec',{'command':'rm -f '+target,'network_mode':'inherit'})
            assert 'runtime_source_retained' in json.dumps(kw['messages']), json.dumps(kw['messages'][-1])
            return text('Completed the scheduled check.')
    (tmp_path/'personal.txt').write_text('keep')
    model=Model()
    from nullion.cron_delivery import run_cron_delivery_workflow,CronRunDeliveryCallbacks,cron_delivery_block_reason
    from nullion import crons
    job=crons.add_cron('確認','0 8 * * *','定期確認',enabled=False,delivery_channel='web',delivery_target='telegram:123')
    turns=[];delivered=[]
    def run_agent(job,conversation):
        result=AgentOrchestrator(model_client=model).run_turn(conversation_id=conversation,principal_id=conversation,
            user_message=job.task,conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
            tool_flow_context={'scheduled_task_run':True})
        turns.append(result)
        return {'text':result.final_text,'tool_results':result.tool_results,'artifacts':result.artifacts,
            'suspended_for_approval':result.suspended_for_approval,'response_fulfilled':result.response_fulfilled}
    def save(job,conversation,value,artifacts,result):delivered.append(value);return True
    receipt=run_cron_delivery_workflow(job,label='Scheduled task',callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda job:'web',delivery_target=lambda job,channel:'telegram:123',run_agent_turn=run_agent,
        record_event=lambda *args,**kwargs:None,block_reason=cron_delivery_block_reason,
        save_web_delivery=save,send_platform_delivery=lambda *args:True,health_store_path=tmp_path/'runtime.db'))
    result=turns[-1]
    assert bool(delivered) is not personal
    if not personal:assert 'Completed the scheduled check.' in delivered[0]
    assert Path(model.path).exists() and (tmp_path/'personal.txt').read_text()=='keep'
    if personal:
        assert result.suspended_for_approval
        assert runtime.store.list_approval_requests()
    else:
        assert not result.suspended_for_approval,result
        assert result.tool_results[-1].output['reason']=='runtime_source_retained'
        assert not runtime.store.list_approval_requests()
