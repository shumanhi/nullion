"""The real runner/SQLite must retain the linked file's identity and bytes."""
import hashlib
import json
import os

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('mode',['off','on'])
@pytest.mark.parametrize('prompt',['同じ資料を添付してください','Devuelve el mismo documento','Renvoyez le même fichier'])
def test_linked_artifact_lookup_does_not_select_another_conversations_similar_file(tmp_path,monkeypatch,mode,prompt):
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    from nullion.conversation_history_tools import with_conversation_history_tool
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision
    from nullion.chat_operator import _remember_chat_turn
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE',mode)
    root=workspace_storage_roots_for_principal('workspace_admin').artifacts
    root.mkdir(parents=True,exist_ok=True)
    old=root/'older-flight-comparison.pdf';current=root/'current-flight-comparison.pdf'
    Image.new('RGB',(40,40),'red').save(old)
    Image.new('RGB',(40,40),'blue').save(current)
    proof=hashlib.sha256(current.read_bytes()).hexdigest()
    _remember_chat_turn(runtime,chat_id='web:other',conversation_id='web:other',conversation_turn_id='old',
        user_message='Flight comparison two trips',assistant_reply='Two verified trips.\nMEDIA:'+str(old))
    _remember_chat_turn(runtime,chat_id='web:current',conversation_id='web:current',conversation_turn_id='parent',
        user_message='Flight comparison',assistant_reply='Current verified trip.\nMEDIA:'+str(current))
    registry=with_conversation_history_tool(ToolRegistry(filesystem_allowed_roots=(root,)),
        runtime=runtime,conversation_id='web:current',user_message=prompt)
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(context_linked=True,
        saved_history_available=True,current_user_message=prompt,requested_extensions=('.pdf',),
        existing_named_artifact_extensions=('.pdf',)),tool_scope_decision=TurnToolScopeDecision(
            valid=True,source_artifact_retrieval_only=True,source_artifact_delivery_requested=True,
            source_artifact_request_evidence=prompt,requested_artifact_extensions=('.pdf',),
            requested_tool_names=('chat_history_search',),required_tool_names=('chat_history_search',)))
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:
                args={'query':'Flight comparison two trips','artifact_extensions':['.pdf'],'limit':4}
                return (call('code_mode',{'tool_names':['chat_history_search'],
                    'code':"call_tool('chat_history_search', "+repr(args)+")"}) if mode=='on'
                    else call('chat_history_search',args))
            assert self.phase==2,'Retrieval must not become new research or production'
            wire=json.dumps(kwargs['messages'])
            assert str(current) in wire and str(old) not in wire
            return text('Selected current document.\nMEDIA:'+str(current))
    model=Model()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='web:current',principal_id='web:current',
        user_message=prompt,conversation_history=[],tool_registry=scoped,policy_store=runtime.store,approval_store=runtime.store)
    native=next(r for r in result.tool_results if r.tool_name=='chat_history_search')
    assert native.output['searched_scope']=='recent_conversation_after_reset'
    assert {m['conversation_id'] for m in native.output['matches']}=={'web:current'}
    assert result.artifacts==[str(current)],result
    assert hashlib.sha256(current.read_bytes()).hexdigest()==proof
    assert model.phase==2
