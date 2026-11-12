"""A captured source link has identical safe wire and saved presentation."""
import json,os
import pytest
from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_tracker_status_delivery import setup
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker Web/store boundary')


@pytest.mark.parametrize('transport',['http','websocket'])
@pytest.mark.parametrize('prompt',['情報源の結果を表示して。','Muestra el resultado de la fuente.'])
def test_tool_source_secrets_never_reach_public_reply_or_break_saved_markdown(tmp_path,monkeypatch,transport,prompt):
    from fastapi.testclient import TestClient
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    url='https://example.test/source?token=synthetic-secret-value&language=es'
    body='Current 37 — [Fuente]('+url+')'
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            names={r['name'] for r in kw.get('tools',[])}
            def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':args}]}
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return {'content':[{'type':'text','text':'{}'}]}
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['opaque_read'],'required_tool_names':['opaque_read']})
            if self.phase==2:return tool('opaque_read',{})
            return {'stop_reason':'end_turn','content':[{'type':'text','text':body}]}
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    registry=app.state.live_tracker_engine.registry
    registry.register(ToolSpec('opaque_read','Read a public record',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,capability_tags=('web',)),
        lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':37,'source_url':url,'text':'Current 37'}))
    cid='web:safe-source';frames=[]
    with TestClient(app) as client:
        if transport=='http':
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});assert result.status_code==200,result.text
            data=result.json()
        else:
            with client.websocket_connect('/ws/chat') as socket:
                socket.send_json({'text':prompt,'conversation_id':cid,'turn_id':'safe-source-turn'})
                while True:
                    frame=socket.receive_json();frames.append(frame)
                    assert frame.get('type')!='error',frame
                    if frame.get('type')=='done':data=frame;break
        visible=data.get('text') or ''.join(str(f.get('text') or '') for f in frames if f.get('type')=='chunk')
        assert 'Current 37' in visible and 'synthetic-secret-value' not in visible,visible
        assert '?token=[redacted]&language=es)' in visible,visible
        assert all('synthetic-secret-value' not in str(f.get('text') or '') for f in frames)
        history=client.get('/api/chat/history/'+cid+'?sync_runtime=1').json()['messages']
        saved=[r['text'] for r in history if r['role']=='bot'][-1]
        assert saved==visible,(saved,visible)
        assert not data.get('artifacts') and model.phase==3
