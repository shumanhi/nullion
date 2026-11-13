"""A source fact and a composed in-chat question have different proof domains."""
import json,os
import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

@pytest.mark.parametrize('prompt,draft',[('天気を調べて、時間別の内訳が必要か質問して。','曇天です。時間別の内訳も必要ですか？'),
    ('Consulta el tiempo y pregunta si quiero el desglose horario.','Estará cubierto. ¿Quieres el desglose por hora?')])
def test_source_fact_and_followup_question_complete_with_separate_proof(tmp_path,monkeypatch,prompt,draft):
    from fastapi.testclient import TestClient
    from nullion.tools import ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    reads=[]
    def text(value):return {'content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}
    def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Model:
        reviews=0
        def create(self,**kw):
            system=kw.get('system','');names={t['name'] for t in kw.get('tools',[])}
            if 'material-requirements planner' in system:
                assert 'required in-chat communication' in system
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'現地の予報','requirement_kind':'fact','evidence_subject_id':'forecast','eligible_evidence_tool_names':['weather_forecast']},
                        {'requirement':'追加の内訳が必要か質問','requirement_kind':'reasoned_answer','evidence_subject_id':'question','eligible_evidence_tool_names':[]}]})
            if 'semantic completion reviewer' in system:
                self.reviews+=1;assert 'required in-chat communication' in system
                p=json.loads(kw['messages'][0]['content'][0]['text']);proof=next(t['invocation_id'] for t in p['tool_evidence'] if t['tool_name']=='weather_forecast')
                return text({'disposition':'complete','all_material_requirements_enumerated':True,'unresolved_requirements':[],'retry_tool_names':[],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,
                        'evidence_invocation_ids':[proof] if r['requirement_kind']=='fact' else []} for r in p['material_requirement_contract']]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if 'weather_forecast' not in names and 'request_tool_scope' in names:return tool('request_tool_scope',{'capabilities':['weather'],'tool_names':['weather_forecast']})
            if not reads:return tool('weather_forecast',{})
            return text(draft)
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    def weather(inv):reads.append(inv);return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':'overcast'})
    app.state.live_tracker_engine.registry.register(ToolSpec('weather_forecast','Read weather',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),weather)
    with TestClient(app) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:response-proof'})
        assert response.status_code==200 and draft in response.json()['text'],response.text
        events=runtime.store.list_recent_conversation_events('web:response-proof',event_type='conversation.chat_turn')
        assert events[-1]['response_fulfilled'] is True and model.reviews == 0 and (len(reads) == 1)
